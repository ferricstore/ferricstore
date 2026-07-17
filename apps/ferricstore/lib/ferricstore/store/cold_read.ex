defmodule Ferricstore.Store.ColdRead do
  @moduledoc """
  Helpers for synchronous callers that need to wait on Tokio cold-read NIFs.

  The NIF sends `{:tokio_complete, corr_id, ...}` to the pid passed at submit
  time. If a caller waits directly and times out, a late completion can remain
  in that caller's mailbox. These helpers submit through a short-lived proxy
  process, so late completions are consumed or dropped away from the caller.
  """

  alias Ferricstore.Bitcask.{Async, NIF}

  @type submit_fun :: (pid(), pos_integer() -> :ok | {:error, term()})
  @type result :: {:ok, term()} | {:error, term()}

  @doc false
  @spec await_tokio(submit_fun(), timeout()) :: result()
  def await_tokio(submit_fun, timeout_ms) do
    Async.await(submit_fun, timeout_ms)
  end

  @spec pread_at(binary(), non_neg_integer(), timeout()) :: result()
  def pread_at(path, offset, timeout_ms) do
    await_tokio(
      fn proxy, corr_id ->
        NIF.v2_pread_at_async(proxy, corr_id, path, offset)
      end,
      timeout_ms
    )
    |> emit_pread_result_error(path)
  end

  @spec pread_at(binary(), non_neg_integer(), binary(), timeout()) :: result()
  def pread_at(path, offset, expected_key, timeout_ms) do
    pread_keyed(path, offset, expected_key, timeout_ms)
  end

  @spec pread_keyed(binary(), non_neg_integer(), binary(), timeout()) :: result()
  def pread_keyed(path, offset, expected_key, timeout_ms) do
    path
    |> do_pread_keyed(offset, expected_key, timeout_ms)
    |> maybe_read_compaction_backup(path, offset, expected_key, timeout_ms)
    |> emit_pread_result_error(path)
  end

  defp do_pread_keyed(path, offset, expected_key, timeout_ms) do
    await_tokio(
      fn proxy, corr_id ->
        NIF.v2_pread_at_key_async(proxy, corr_id, path, offset, expected_key)
      end,
      timeout_ms
    )
  end

  @spec pread_batch([{binary(), non_neg_integer()}], timeout()) :: result()
  def pread_batch([], _timeout_ms), do: {:ok, []}

  def pread_batch(locations, timeout_ms) do
    await_tokio(
      fn proxy, corr_id ->
        case pread_batch_submit_shape(locations) do
          {:single_path, path, offsets} ->
            NIF.v2_pread_batch_path_async(proxy, corr_id, path, offsets)

          {:grouped_paths, groups} ->
            NIF.v2_pread_batch_grouped_async(proxy, corr_id, groups)
        end
      end,
      timeout_ms
    )
  end

  @spec pread_batch_keyed([{binary(), non_neg_integer(), binary()}], timeout()) :: result()
  def pread_batch_keyed([], _timeout_ms), do: {:ok, []}

  def pread_batch_keyed(locations, timeout_ms) do
    locations
    |> do_pread_batch_keyed(timeout_ms)
    |> maybe_read_compaction_backup_batch(locations, timeout_ms)
    |> normalize_keyed_batch_result(length(locations))
  end

  @doc false
  def normalize_keyed_batch_result({:ok, values} = result, expected_count)
      when is_list(values) and length(values) == expected_count,
      do: result

  def normalize_keyed_batch_result({:ok, values}, expected_count) when is_list(values) do
    {:error, {:batch_result_length_mismatch, expected_count, length(values)}}
  end

  def normalize_keyed_batch_result({:error, _reason} = error, _expected_count), do: error

  def normalize_keyed_batch_result(invalid, _expected_count) do
    {:error, {:invalid_batch_result, invalid}}
  end

  @type keyed_read_token :: term()
  @type current_keyed_read :: {binary(), non_neg_integer(), binary(), keyed_read_token()}

  @type current_keyed_resolution ::
          {:cold, binary(), non_neg_integer(), keyed_read_token()}
          | {:hot, binary(), keyed_read_token()}
          | :missing
          | {:error, term()}

  @type current_keyed_result ::
          {:value, binary(), keyed_read_token()} | :missing | {:error, term()}

  @doc false
  @spec pread_batch_keyed_current(
          [current_keyed_read()],
          (binary(), keyed_read_token() -> current_keyed_resolution()),
          timeout()
        ) :: {:ok, [current_keyed_result()]}
  def pread_batch_keyed_current([], _resolve_current, _timeout_ms), do: {:ok, []}

  def pread_batch_keyed_current(reads, resolve_current, timeout_ms)
      when is_function(resolve_current, 2) do
    locations = Enum.map(reads, fn {path, offset, key, _token} -> {path, offset, key} end)
    deadline_ms = cold_read_deadline(timeout_ms)

    values =
      locations
      |> pread_batch_keyed(timeout_ms)
      |> normalize_current_batch_values(length(reads))

    results =
      reads
      |> Enum.zip(values)
      |> Enum.map(fn
        {{path, offset, key, token}, value} when is_binary(value) ->
          validate_successful_current_keyed_read(
            path,
            offset,
            key,
            token,
            value,
            resolve_current,
            deadline_ms
          )

        {{path, offset, key, token}, failed_value} ->
          retry_current_keyed_read(
            path,
            offset,
            key,
            token,
            current_batch_failure(failed_value),
            resolve_current,
            deadline_ms
          )
      end)

    {:ok, results}
  end

  defp validate_successful_current_keyed_read(
         path,
         offset,
         key,
         token,
         value,
         resolve_current,
         deadline_ms
       ) do
    case call_current_keyed_resolver(resolve_current, key, token) do
      {:hot, current_value, current_token} when is_binary(current_value) ->
        {:value, current_value, current_token}

      {:cold, ^path, ^offset, current_token} ->
        {:value, value, current_token}

      {:cold, current_path, current_offset, current_token}
      when is_binary(current_path) and is_integer(current_offset) and current_offset >= 0 ->
        retry_current_keyed_location(
          current_path,
          current_offset,
          key,
          current_token,
          resolve_current,
          deadline_ms
        )

      :missing ->
        :missing

      {:error, reason} ->
        {:error, reason}

      invalid ->
        {:error, {:invalid_current_location, invalid}}
    end
  end

  defp normalize_current_batch_values({:ok, values}, expected_count)
       when is_list(values) and length(values) == expected_count,
       do: values

  defp normalize_current_batch_values({:ok, _values}, expected_count),
    do: List.duplicate({:error, :batch_result_length_mismatch}, expected_count)

  defp normalize_current_batch_values({:error, reason}, expected_count),
    do: List.duplicate({:error, reason}, expected_count)

  defp normalize_current_batch_values(invalid, expected_count),
    do: List.duplicate({:error, {:invalid_batch_result, invalid}}, expected_count)

  defp current_batch_failure({:error, reason}), do: reason
  defp current_batch_failure(nil), do: :not_found
  defp current_batch_failure(other), do: {:unexpected_pread_result, other}

  defp retry_current_keyed_read(
         path,
         offset,
         key,
         token,
         previous_reason,
         resolve_current,
         deadline_ms
       ) do
    case call_current_keyed_resolver(resolve_current, key, token) do
      {:hot, value, current_token} when is_binary(value) ->
        {:value, value, current_token}

      {:cold, current_path, current_offset, current_token}
      when is_binary(current_path) and is_integer(current_offset) and current_offset >= 0 ->
        if current_path == path and current_offset == offset do
          {:error, previous_reason}
        else
          retry_current_keyed_location(
            current_path,
            current_offset,
            key,
            current_token,
            resolve_current,
            deadline_ms
          )
        end

      :missing ->
        :missing

      {:error, reason} ->
        {:error, reason}

      invalid ->
        {:error, {:invalid_current_location, invalid}}
    end
  end

  defp retry_current_keyed_location(
         path,
         offset,
         key,
         token,
         resolve_current,
         deadline_ms
       ) do
    case cold_read_remaining(deadline_ms) do
      0 ->
        {:error, :timeout}

      timeout_ms ->
        case pread_keyed(path, offset, key, timeout_ms) do
          {:ok, value} when is_binary(value) ->
            validate_successful_current_keyed_read(
              path,
              offset,
              key,
              token,
              value,
              resolve_current,
              deadline_ms
            )

          {:error, reason} ->
            retry_current_keyed_read(
              path,
              offset,
              key,
              token,
              reason,
              resolve_current,
              deadline_ms
            )

          invalid ->
            retry_current_keyed_read(
              path,
              offset,
              key,
              token,
              {:unexpected_pread_result, invalid},
              resolve_current,
              deadline_ms
            )
        end
    end
  end

  defp call_current_keyed_resolver(resolve_current, key, token) do
    resolve_current.(key, token)
  rescue
    error -> {:error, {:current_location_resolver_failed, error.__struct__}}
  catch
    kind, _reason -> {:error, {:current_location_resolver_failed, kind}}
  end

  defp cold_read_deadline(:infinity), do: :infinity

  defp cold_read_deadline(timeout_ms) when is_integer(timeout_ms) and timeout_ms >= 0,
    do: System.monotonic_time(:millisecond) + timeout_ms

  defp cold_read_remaining(:infinity), do: :infinity

  defp cold_read_remaining(deadline_ms),
    do: max(deadline_ms - System.monotonic_time(:millisecond), 0)

  defp do_pread_batch_keyed(locations, timeout_ms) do
    await_tokio(
      fn proxy, corr_id ->
        case pread_batch_keyed_submit_shape(locations) do
          {:single_path, path, reads} ->
            NIF.v2_pread_batch_path_key_async(proxy, corr_id, path, reads)

          {:grouped_paths, groups} ->
            NIF.v2_pread_batch_grouped_key_async(proxy, corr_id, groups)
        end
      end,
      timeout_ms
    )
  end

  defp maybe_read_compaction_backup(primary, path, offset, key, timeout_ms)
       when not (is_tuple(primary) and tuple_size(primary) == 2 and elem(primary, 0) == :ok and
                   is_binary(elem(primary, 1))) do
    case available_compaction_backup(path) do
      nil -> primary
      backup -> prefer_success(do_pread_keyed(backup, offset, key, timeout_ms), primary)
    end
  end

  defp maybe_read_compaction_backup(primary, _path, _offset, _key, _timeout_ms), do: primary

  defp maybe_read_compaction_backup_batch(
         {:ok, values} = primary,
         locations,
         timeout_ms
       )
       when is_list(values) and length(values) == length(locations) do
    {backup_reads, _path_cache} =
      locations
      |> Enum.zip(values)
      |> Enum.with_index()
      |> Enum.reduce({[], %{}}, fn
        {{_location, value}, _index}, acc when is_binary(value) ->
          acc

        {{{path, offset, key}, _value}, index}, {reads, path_cache} ->
          {backup, path_cache} = cached_compaction_backup(path, path_cache)

          case backup do
            nil -> {reads, path_cache}
            backup_path -> {[{index, {backup_path, offset, key}} | reads], path_cache}
          end
      end)

    case Enum.reverse(backup_reads) do
      [] ->
        primary

      indexed_reads ->
        locations = Enum.map(indexed_reads, &elem(&1, 1))

        case do_pread_batch_keyed(locations, timeout_ms) do
          {:ok, backup_values} when length(backup_values) == length(indexed_reads) ->
            replacements =
              indexed_reads
              |> Enum.zip(backup_values)
              |> Map.new(fn {{index, _location}, value} -> {index, value} end)

            merged =
              values
              |> Enum.with_index()
              |> Enum.map(fn {value, index} ->
                case Map.get(replacements, index) do
                  backup_value when is_binary(backup_value) -> backup_value
                  _ -> value
                end
              end)

            {:ok, merged}

          _error ->
            primary
        end
    end
  end

  defp maybe_read_compaction_backup_batch(primary, _locations, _timeout_ms), do: primary

  defp prefer_success({:ok, value} = backup, _primary) when is_binary(value), do: backup
  defp prefer_success(_backup, primary), do: primary

  defp cached_compaction_backup(path, cache) do
    case Map.fetch(cache, path) do
      {:ok, backup} ->
        {backup, cache}

      :error ->
        backup = available_compaction_backup(path)
        {backup, Map.put(cache, path, backup)}
    end
  end

  @doc false
  @spec compaction_backup_path(binary()) :: binary() | nil
  def compaction_backup_path(path) when is_binary(path) do
    basename = Path.basename(path)

    with ".log" <- Path.extname(basename),
         stem <- Path.rootname(basename),
         {fid, ""} <- Integer.parse(stem),
         true <- fid >= 0 do
      Path.join(Path.dirname(path), "compaction_backup_#{fid}.log")
    else
      _ -> nil
    end
  end

  defp available_compaction_backup(path) do
    case compaction_backup_path(path) do
      nil ->
        nil

      backup ->
        case File.lstat(backup) do
          {:ok, %File.Stat{type: :regular}} -> backup
          _missing_or_unsafe -> nil
        end
    end
  end

  @doc false
  @spec emit_pread_error(binary(), term(), pos_integer()) :: :ok
  def emit_pread_error(path, raw_reason, count \\ 1) when is_binary(path) and count > 0 do
    :telemetry.execute(
      [:ferricstore, :bitcask, :pread_corrupt],
      %{count: count},
      %{path: path, reason: classify_pread_error(raw_reason), raw_reason: raw_reason}
    )
  end

  @doc false
  @spec pread_batch_submit_shape([{binary(), non_neg_integer()}]) ::
          {:single_path, binary(), [non_neg_integer()]}
          | {:grouped_paths, [{binary(), [{non_neg_integer(), non_neg_integer()}]}]}
  def pread_batch_submit_shape([{path, offset} | rest] = locations) do
    same_path_offsets(rest, path, [offset], locations)
  end

  def pread_batch_submit_shape([]), do: {:grouped_paths, []}

  defp same_path_offsets([{path, offset} | rest], path, offsets, locations) do
    same_path_offsets(rest, path, [offset | offsets], locations)
  end

  defp same_path_offsets([], path, offsets, _locations) do
    {:single_path, path, Enum.reverse(offsets)}
  end

  defp same_path_offsets(_rest, _path, _offsets, locations),
    do: {:grouped_paths, group_paths(locations)}

  @doc false
  @spec pread_batch_keyed_submit_shape([{binary(), non_neg_integer(), binary()}]) ::
          {:single_path, binary(), [{non_neg_integer(), binary()}]}
          | {:grouped_paths,
             [
               {binary(), [{non_neg_integer(), non_neg_integer(), binary()}]}
             ]}
  def pread_batch_keyed_submit_shape([{path, offset, key} | rest] = locations) do
    same_path_keyed_offsets(rest, path, [{offset, key}], locations)
  end

  def pread_batch_keyed_submit_shape([]), do: {:grouped_paths, []}

  defp same_path_keyed_offsets([{path, offset, key} | rest], path, reads, locations) do
    same_path_keyed_offsets(rest, path, [{offset, key} | reads], locations)
  end

  defp same_path_keyed_offsets([], path, reads, _locations) do
    {:single_path, path, Enum.reverse(reads)}
  end

  defp same_path_keyed_offsets(_rest, _path, _reads, locations),
    do: {:grouped_paths, group_keyed_paths(locations)}

  defp group_paths(locations) do
    {groups, order} =
      locations
      |> Enum.with_index()
      |> Enum.reduce({%{}, []}, fn {{path, offset}, index}, {groups, order} ->
        order = if Map.has_key?(groups, path), do: order, else: [path | order]
        groups = Map.update(groups, path, [{index, offset}], &[{index, offset} | &1])
        {groups, order}
      end)

    order
    |> Enum.reverse()
    |> Enum.map(fn path -> {path, groups |> Map.fetch!(path) |> Enum.reverse()} end)
  end

  defp group_keyed_paths(locations) do
    {groups, order} =
      locations
      |> Enum.with_index()
      |> Enum.reduce({%{}, []}, fn {{path, offset, key}, index}, {groups, order} ->
        order = if Map.has_key?(groups, path), do: order, else: [path | order]
        groups = Map.update(groups, path, [{index, offset, key}], &[{index, offset, key} | &1])
        {groups, order}
      end)

    order
    |> Enum.reverse()
    |> Enum.map(fn path -> {path, groups |> Map.fetch!(path) |> Enum.reverse()} end)
  end

  defp emit_pread_result_error({:error, reason} = result, path) do
    emit_pread_error(path, reason)

    result
  end

  defp emit_pread_result_error(result, _path), do: result

  defp classify_pread_error(:timeout), do: :timeout
  defp classify_pread_error(reason) when reason in [:missing_file, :enoent], do: :missing_file

  defp classify_pread_error(reason) when is_binary(reason) do
    downcased = String.downcase(reason)

    if String.contains?(downcased, "missing_file") or
         String.contains?(downcased, "no such file") do
      :missing_file
    else
      :corrupt_record
    end
  end

  defp classify_pread_error(_reason), do: :corrupt_record
end
