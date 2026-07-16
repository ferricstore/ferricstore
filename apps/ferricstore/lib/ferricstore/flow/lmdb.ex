defmodule Ferricstore.Flow.LMDB do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.LMDB.Access
  alias Ferricstore.Flow.LMDB.ValueLocator
  alias Ferricstore.TermCodec

  @default_map_size 16 * 1024 * 1024 * 1024
  @release_retry_interval_ms 5
  @max_u64 18_446_744_073_709_551_615

  def enabled?, do: true
  def projection_enabled?, do: true
  def mode, do: :lagged
  def refresh_config!, do: :lagged
  def normalize_mode(_value), do: :lagged

  def map_size do
    case Application.get_env(:ferricstore, :flow_lmdb_map_size, @default_map_size) do
      value when is_integer(value) and value > 0 and value <= @max_u64 -> value
      _invalid -> @default_map_size
    end
  end

  def path(shard_data_path), do: Path.join(shard_data_path, "flow_lmdb")

  def ensure_shard_dirs(data_dir, shard_count)
      when is_binary(data_dir) and is_integer(shard_count) and shard_count >= 0 do
    Enum.reduce_while(0..max(shard_count - 1, -1)//1, :ok, fn shard_index, :ok ->
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> path()
      |> Ferricstore.FS.mkdir_p()
      |> case do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  def ensure_shard_dirs(_data_dir, _shard_count), do: {:error, :badarg}

  # LMDB stores Flow state as a wrapper around the already-versioned Flow record
  # bytes: {expire_at_ms, encoded_record}. The wrapper owns TTL semantics only.
  # Do not decode user payload here; the Flow schema gate remains centralized.
  def encode_value(value, expire_at_ms)
      when is_binary(value) and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             expire_at_ms <= @max_u64,
      do: TermCodec.encode({expire_at_ms, value})

  def encode_value(_value, _expire_at_ms),
    do: raise(ArgumentError, "LMDB value fields are invalid")

  # Returns the wrapped encoded Flow record when it is still live. Callers must
  # pass the returned value through Ferricstore.Flow.decode_record/1 so the Flow
  # schema version gate stays centralized in one place.
  def decode_value(blob, now_ms)
      when is_binary(blob) and is_integer(now_ms) and now_ms >= 0 and now_ms <= @max_u64 do
    case TermCodec.decode(blob) do
      {:ok, {expire_at_ms, value}}
      when is_integer(expire_at_ms) and expire_at_ms > 0 and expire_at_ms <= @max_u64 and
             is_binary(value) ->
        if expire_at_ms <= now_ms, do: :expired, else: {:ok, value}

      {:ok, {0, value}} when is_binary(value) ->
        {:ok, value}

      _invalid ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_value(blob, _now_ms) when is_binary(blob), do: :error

  def get(path, key) when is_binary(path) and is_binary(key) do
    if Ferricstore.FS.dir?(path), do: NIF.lmdb_get(path, key, map_size()), else: :not_found
  end

  def encode_value_locator(expire_at_ms, file_id, offset, value_size),
    do: ValueLocator.encode(expire_at_ms, file_id, offset, value_size)

  def decode_value_locator(blob, now_ms)
      when is_binary(blob) and is_integer(now_ms) and now_ms >= 0 and now_ms <= @max_u64 do
    case TermCodec.decode(blob) do
      {:ok, {:flow_value_locator, 1, expire_at_ms, file_id, offset, value_size}}
      when is_integer(expire_at_ms) and expire_at_ms > 0 and expire_at_ms <= @max_u64 ->
        if expire_at_ms <= now_ms do
          :expired
        else
          decode_live_value_locator(file_id, offset, value_size)
        end

      {:ok, {:flow_value_locator, 1, 0, file_id, offset, value_size}} ->
        decode_live_value_locator(file_id, offset, value_size)

      {:ok, {:flow_value_locator, 1, _expire_at_ms, _file_id, _offset, _value_size}} ->
        :error

      _other ->
        :not_locator
    end
  rescue
    _ -> :error
  end

  def decode_value_locator(blob, _now_ms) when is_binary(blob), do: :error

  defp decode_live_value_locator(file_id, offset, value_size)
       when is_integer(offset) and offset >= 0 and offset <= @max_u64 and
              is_integer(value_size) and value_size >= 0 and value_size <= @max_u64 do
    if ValueLocator.valid_file_id?(file_id),
      do: {:ok, {file_id, offset, value_size}},
      else: :error
  end

  defp decode_live_value_locator(_file_id, _offset, _value_size), do: :error

  def cold_park_key(flow_id) when is_binary(flow_id),
    do: Ferricstore.Flow.LMDB.Cold.park_key(flow_id)

  def cold_park_key_for_state_key(state_key) when is_binary(state_key),
    do: Ferricstore.Flow.LMDB.Cold.park_key_for_state_key(state_key)

  def cold_due_bucket_ms(due_at_ms, bucket_ms \\ 60_000)

  def cold_due_bucket_ms(due_at_ms, bucket_ms),
    do: Ferricstore.Flow.LMDB.Cold.due_bucket_ms(due_at_ms, bucket_ms)

  def cold_due_key(attrs), do: Ferricstore.Flow.LMDB.Cold.due_key(attrs)

  def cold_due_bucket_prefix(bucket_ms),
    do: Ferricstore.Flow.LMDB.Cold.due_bucket_prefix(bucket_ms)

  def cold_due_type_bucket_prefix(bucket_ms, type),
    do: Ferricstore.Flow.LMDB.Cold.due_type_bucket_prefix(bucket_ms, type)

  def cold_due_state_bucket_prefix(bucket_ms, type, state),
    do: Ferricstore.Flow.LMDB.Cold.due_state_bucket_prefix(bucket_ms, type, state)

  def cold_due_claim_prefix(attrs), do: Ferricstore.Flow.LMDB.Cold.due_claim_prefix(attrs)

  def cold_by_segment_key(locator), do: Ferricstore.Flow.LMDB.Cold.by_segment_key(locator)

  def cold_by_segment_key(file_id, offset),
    do: Ferricstore.Flow.LMDB.Cold.by_segment_key(file_id, offset)

  def cold_by_segment_prefix(file_id), do: Ferricstore.Flow.LMDB.Cold.by_segment_prefix(file_id)

  def encode_cold_park(locator, attrs),
    do: Ferricstore.Flow.LMDB.Cold.encode_park(locator, attrs)

  def decode_cold_park(blob), do: Ferricstore.Flow.LMDB.Cold.decode_park(blob)

  def encode_cold_value_locator(value_ref, owner_flow_id, owner_version, locator, attrs \\ []),
    do:
      Ferricstore.Flow.LMDB.Cold.encode_value_locator(
        value_ref,
        owner_flow_id,
        owner_version,
        locator,
        attrs
      )

  def decode_cold_value_locator(blob), do: Ferricstore.Flow.LMDB.Cold.decode_value_locator(blob)

  def get_many(_path, []), do: {:ok, []}

  def get_many(path, keys) when is_binary(path) and is_list(keys), do: Access.get_many(path, keys)

  def warm(path) when is_binary(path) do
    case NIF.lmdb_get(path, <<0>>, map_size()) do
      {:ok, _value} -> :ok
      :not_found -> :ok
      {:error, _reason} = error -> error
    end
  end

  def write_batch(_path, []), do: :ok

  def write_batch(path, ops) when is_binary(path) and is_list(ops) do
    NIF.lmdb_write_batch(path, ops, map_size())
  end

  def clear_all(data_dir, shard_count)
      when is_binary(data_dir) and is_integer(shard_count) and shard_count >= 0 do
    Enum.reduce_while(0..max(shard_count - 1, -1)//1, :ok, fn shard_index, :ok ->
      path =
        data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> path()

      case clear(path) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  def clear(path) when is_binary(path) do
    if Ferricstore.FS.dir?(path) do
      with :ok <- release_detached_env(path) do
        NIF.lmdb_clear(path, map_size())
      end
    else
      :ok
    end
  end

  defp release_detached_env(path) do
    if env_present?(path) do
      :ok
    else
      release(path)
    end
  end

  @spec release(binary()) :: :ok | {:error, term()}
  def release(path) when is_binary(path) do
    case NIF.lmdb_release(path) do
      {:ok, _released} -> :ok
      {:busy, count} -> {:error, {:lmdb_env_busy, count}}
      {:error, _reason} = error -> error
    end
  end

  @spec release(binary(), non_neg_integer()) :: :ok | {:error, term()}
  def release(path, timeout_ms)
      when is_binary(path) and is_integer(timeout_ms) and timeout_ms >= 0 do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms

    case release_until(fn -> NIF.lmdb_release(path) end, deadline_ms) do
      {:ok, _released} -> :ok
      {:busy, count} -> {:error, {:lmdb_env_busy, count}}
      {:error, _reason} = error -> error
    end
  end

  def release_all do
    NIF.lmdb_release_all()
  end

  def release_all(timeout_ms) when is_integer(timeout_ms) and timeout_ms >= 0 do
    release_until(&NIF.lmdb_release_all/0, System.monotonic_time(:millisecond) + timeout_ms)
  end

  defp release_until(release_fun, deadline_ms) do
    case release_fun.() do
      {:busy, _count} = busy ->
        now_ms = System.monotonic_time(:millisecond)

        if now_ms < deadline_ms do
          Process.sleep(min(@release_retry_interval_ms, deadline_ms - now_ms))
          release_until(release_fun, deadline_ms)
        else
          busy
        end

      result ->
        result
    end
  end

  def flush_in_progress_key, do: "flow-lmdb-flush-in-progress"

  def flush_in_progress?(path) when is_binary(path) do
    if env_present?(path) do
      path
      |> get(flush_in_progress_key())
      |> normalize_flush_marker_read()
    else
      false
    end
  end

  @doc false
  def __normalize_flush_marker_read_for_test__(result), do: normalize_flush_marker_read(result)

  defp normalize_flush_marker_read({:ok, _value}), do: true
  defp normalize_flush_marker_read(:not_found), do: false
  defp normalize_flush_marker_read(_storage_failure_or_invalid), do: true

  def env_present?(path) when is_binary(path) do
    case File.lstat(path) do
      {:ok, %{type: :directory}} ->
        regular_file_nofollow?(Path.join(path, "data.mdb")) or
          regular_file_nofollow?(Path.join(path, "lock.mdb"))

      _missing_or_unsafe ->
        false
    end
  end

  def env_present?(_path), do: false

  defp regular_file_nofollow?(path) do
    match?({:ok, %{type: :regular}}, File.lstat(path))
  end

  def flush_in_progress_put_op, do: {:put, flush_in_progress_key(), <<1>>}
  def flush_in_progress_delete_op, do: {:delete, flush_in_progress_key()}

  def write_batch_with_originals(_path, []), do: {:ok, []}

  def write_batch_with_originals(path, ops) when is_binary(path) and is_list(ops) do
    NIF.lmdb_write_batch_with_originals(path, ops, map_size())
  end

  def prefix_entries(path, prefix, limit)
      when is_binary(path) and is_binary(prefix) and is_integer(limit) and limit >= 0 do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_prefix_entries(path, prefix, limit, map_size()),
      else: {:ok, []}
  end

  def prefix_entries_after(path, prefix, after_key, limit)
      when is_binary(path) and is_binary(prefix) and is_binary(after_key) and
             is_integer(limit) and limit >= 0 do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_prefix_entries_after(path, prefix, after_key, limit, map_size()),
      else: {:ok, []}
  end

  def reduce_prefix_entries(path, prefix, page_size, acc, reduce_fun)
      when is_binary(path) and is_binary(prefix) and is_integer(page_size) and page_size > 0 and
             is_function(reduce_fun, 2) do
    scan_fun = fn
      nil -> prefix_entries(path, prefix, page_size)
      after_key -> prefix_entries_after(path, prefix, after_key, page_size)
    end

    reduce_prefix_pages(prefix, page_size, acc, scan_fun, reduce_fun, nil)
  end

  @doc false
  def __reduce_prefix_pages_for_test__(prefix, page_size, acc, scan_fun, reduce_fun)
      when is_binary(prefix) and is_integer(page_size) and page_size > 0 and
             is_function(scan_fun, 1) and is_function(reduce_fun, 2) do
    reduce_prefix_pages(prefix, page_size, acc, scan_fun, reduce_fun, nil)
  end

  defp reduce_prefix_pages(prefix, page_size, acc, scan_fun, reduce_fun, cursor) do
    case scan_fun.(cursor) do
      {:ok, []} ->
        {:ok, acc}

      {:ok, entries} when is_list(entries) ->
        with :ok <- validate_prefix_page(entries, prefix, cursor, page_size),
             {:ok, next_acc} <- normalize_prefix_page_reduce(reduce_fun.(entries, acc)) do
          if length(entries) < page_size do
            {:ok, next_acc}
          else
            {next_cursor, _value} = List.last(entries)

            reduce_prefix_pages(
              prefix,
              page_size,
              next_acc,
              scan_fun,
              reduce_fun,
              next_cursor
            )
          end
        end

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_prefix_page}
    end
  end

  defp validate_prefix_page(entries, prefix, cursor, page_size)
       when length(entries) <= page_size do
    entries
    |> Enum.reduce_while({:ok, cursor}, fn
      {key, value}, {:ok, previous_key}
      when is_binary(key) and is_binary(value) ->
        cond do
          not String.starts_with?(key, prefix) ->
            {:halt, {:error, :invalid_prefix_page}}

          is_binary(previous_key) and key <= previous_key ->
            {:halt, {:error, :non_monotonic_prefix_page}}

          true ->
            {:cont, {:ok, key}}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_prefix_page}}
    end)
    |> case do
      {:ok, _last_key} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp validate_prefix_page(_entries, _prefix, _cursor, _page_size),
    do: {:error, :invalid_prefix_page}

  defp normalize_prefix_page_reduce({:ok, _acc} = result), do: result
  defp normalize_prefix_page_reduce({:error, _reason} = error), do: error

  defp normalize_prefix_page_reduce(_invalid),
    do: {:error, :invalid_prefix_page_reducer_result}

  def prefix_entries_after_bounded(path, prefix, after_key, max_items, max_bytes)
      when is_binary(path) and is_binary(prefix) and is_binary(after_key) and
             is_integer(max_items) and max_items >= 0 and is_integer(max_bytes) and
             max_bytes >= 0 do
    if Ferricstore.FS.dir?(path) do
      NIF.lmdb_prefix_entries_after_bounded(
        path,
        prefix,
        after_key,
        max_items,
        max_bytes,
        map_size()
      )
    else
      {:ok, []}
    end
  end

  def prefix_entries(path, prefix, limit, false), do: prefix_entries(path, prefix, limit)

  def prefix_entries(path, prefix, limit, true)
      when is_binary(path) and is_binary(prefix) and is_integer(limit) and limit >= 0 do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_prefix_entries_reverse(path, prefix, limit, map_size()),
      else: {:ok, []}
  end

  def prefix_entries_reverse_before(path, prefix, before_key, limit)
      when is_binary(path) and is_binary(prefix) and is_binary(before_key) and
             is_integer(limit) and limit >= 0 do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_prefix_entries_reverse_before(path, prefix, before_key, limit, map_size()),
      else: {:ok, []}
  end

  def prefix_count(path, prefix) when is_binary(path) and is_binary(prefix) do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_prefix_count(path, prefix, map_size()),
      else: {:ok, 0}
  end

  def terminal_state?(state) when state in ["completed", "failed", "cancelled"], do: true
  def terminal_state?(_state), do: false

  def terminal_index_prefix(state_index_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_index_prefix(state_index_key)

  def terminal_index_global_prefix,
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_index_global_prefix()

  def terminal_index_key(state_index_key, id, updated_at_ms),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_index_key(state_index_key, id, updated_at_ms)

  def terminal_index_entry_key?(key, id, updated_at_ms),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_index_entry_key?(key, id, updated_at_ms)

  def terminal_count_key(state_index_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_count_key(state_index_key)

  def terminal_count_prefix, do: Ferricstore.Flow.LMDB.IndexCodec.terminal_count_prefix()

  def terminal_expire_prefix, do: Ferricstore.Flow.LMDB.IndexCodec.terminal_expire_prefix()

  def terminal_expire_key(expire_at_ms, terminal_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_expire_key(expire_at_ms, terminal_key)

  def terminal_by_state_key_key(state_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_by_state_key_key(state_key)

  def terminal_by_state_global_prefix,
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_by_state_global_prefix()

  def active_index_global_prefix,
    do: Ferricstore.Flow.LMDB.IndexCodec.active_index_global_prefix()

  def active_index_prefix(index_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.active_index_prefix(index_key)

  def active_index_key(index_key, id, score),
    do: Ferricstore.Flow.LMDB.IndexCodec.active_index_key(index_key, id, score)

  def active_index_entry_key?(key, index_key, id, score),
    do: Ferricstore.Flow.LMDB.IndexCodec.active_index_entry_key?(key, index_key, id, score)

  def active_by_state_key_key(state_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.active_by_state_key_key(state_key)

  def active_by_state_global_prefix,
    do: Ferricstore.Flow.LMDB.IndexCodec.active_by_state_global_prefix()

  def active_index_put_ops(state_key, record, expire_at_ms)
      when is_binary(state_key) and is_map(record) and is_integer(expire_at_ms) do
    {ops, _reverse_value} = active_index_put_ops_with_reverse(state_key, record, expire_at_ms)
    ops
  end

  def active_index_put_ops_with_reverse(state_key, record, expire_at_ms)
      when is_binary(state_key) and is_map(record) and is_integer(expire_at_ms) do
    entries = active_flow_index_entries(record)

    active_ops =
      Enum.map(entries, fn {index_key, id, score} ->
        active_key = active_index_key(index_key, id, score)
        value = encode_active_index_value(index_key, id, score, expire_at_ms, state_key)
        {:put, active_key, value}
      end)

    active_keys = Enum.map(active_ops, fn {:put, key, _value} -> key end)
    reverse_value = encode_active_index_reverse_value(active_keys)

    {
      [{:put, active_by_state_key_key(state_key), reverse_value} | active_ops],
      reverse_value
    }
  end

  def active_timeout_index_put_ops(state_key, record, expire_at_ms)
      when is_binary(state_key) and is_map(record) and is_integer(expire_at_ms) do
    cleanup_ops = record |> active_non_timeout_index_delete_ops() |> Enum.uniq()

    case active_timeout_deadline(record) do
      {:ok, deadline_ms} ->
        index_key = Ferricstore.Flow.Keys.active_timeout_index_key()
        active_key = active_index_key(index_key, state_key, deadline_ms)

        value =
          encode_active_index_value(
            index_key,
            state_key,
            deadline_ms,
            expire_at_ms,
            state_key
          )

        reverse_value = encode_active_index_reverse_value([active_key])

        cleanup_ops ++
          [
            {:put, active_by_state_key_key(state_key), reverse_value},
            {:put, active_key, value}
          ]

      :none ->
        cleanup_ops ++ [{:delete, active_by_state_key_key(state_key)}]
    end
  end

  defp active_non_timeout_index_delete_ops(record) do
    timeout_index_key = Ferricstore.Flow.Keys.active_timeout_index_key()

    record
    |> active_flow_index_entries()
    |> Enum.reject(fn {index_key, _id, _score} -> index_key == timeout_index_key end)
    |> Enum.map(fn {index_key, id, score} ->
      {:delete, active_index_key(index_key, id, score)}
    end)
  end

  def active_index_delete_ops_from_reverse_result(state_key, reverse_value)
      when is_binary(state_key) and is_binary(reverse_value),
      do: active_index_delete_ops_from_read_result({:ok, reverse_value}, state_key)

  def active_index_delete_ops_from_reverse_result(_state_key, invalid),
    do: {:error, {:invalid_active_index_reverse_read, invalid}}

  def active_index_delete_ops_result(path, state_key)
      when is_binary(path) and is_binary(state_key) do
    case get(path, active_by_state_key_key(state_key)) do
      {:ok, reverse_value} when is_binary(reverse_value) ->
        with {:ok, active_keys} <- decode_active_index_reverse_value(reverse_value),
             {:ok, active_compare_ops} <-
               validate_active_index_reverse_rows(path, state_key, active_keys) do
          reverse_key = active_by_state_key_key(state_key)

          {:ok,
           [{:compare, reverse_key, reverse_value} | active_compare_ops] ++
             [{:delete, reverse_key} | Enum.map(active_keys, &{:delete, &1})]}
        else
          :error -> {:error, :invalid_active_index_reverse}
          {:error, _reason} = error -> error
        end

      result ->
        active_index_delete_ops_from_read_result(result, state_key)
    end
  end

  @doc false
  def __active_index_delete_ops_result_for_test__(state_key, result),
    do: active_index_delete_ops_from_read_result(result, state_key)

  defp active_index_delete_ops_from_read_result(:not_found, state_key),
    do: {:ok, [{:compare_missing, active_by_state_key_key(state_key)}]}

  defp active_index_delete_ops_from_read_result({:ok, reverse_value}, state_key)
       when is_binary(reverse_value) do
    reverse_key = active_by_state_key_key(state_key)

    case decode_active_index_reverse_value(reverse_value) do
      {:ok, active_keys} ->
        {:ok,
         [
           {:compare, reverse_key, reverse_value},
           {:delete, reverse_key}
           | Enum.map(active_keys, &{:delete, &1})
         ]}

      :error ->
        {:error, :invalid_active_index_reverse}
    end
  end

  defp active_index_delete_ops_from_read_result({:error, _reason} = error, _state_key),
    do: error

  defp active_index_delete_ops_from_read_result(invalid, _state_key),
    do: {:error, {:invalid_active_index_reverse_read, invalid}}

  defp validate_active_index_reverse_rows(path, state_key, active_keys) do
    case Access.get_many(path, active_keys) do
      {:ok, results} ->
        active_keys
        |> Enum.zip(results)
        |> Enum.reduce_while({:ok, []}, fn
          {active_key, :not_found}, {:ok, compare_ops} ->
            {:cont, {:ok, [{:compare_missing, active_key} | compare_ops]}}

          {active_key, {:ok, active_value}}, {:ok, compare_ops} when is_binary(active_value) ->
            case decode_active_index_value(active_value) do
              {:ok, {index_key, id, score, _expire_at_ms, ^state_key}} ->
                if active_index_entry_key?(active_key, index_key, id, score) do
                  {:cont, {:ok, [{:compare, active_key, active_value} | compare_ops]}}
                else
                  {:halt, {:error, {:invalid_active_index_value, active_key}}}
                end

              {:ok, {_index_key, _id, _score, _expire_at_ms, _foreign_state_key}} ->
                {:halt, {:error, {:active_index_reverse_state_mismatch, active_key}}}

              :error ->
                {:halt, {:error, {:invalid_active_index_value, active_key}}}
            end

          {active_key, invalid}, {:ok, _compare_ops} ->
            {:halt, {:error, {:invalid_active_index_read, active_key, invalid}}}
        end)
        |> case do
          {:ok, reversed_compare_ops} -> {:ok, Enum.reverse(reversed_compare_ops)}
          {:error, _reason} = error -> error
        end

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_active_index_batch_read, invalid}}
    end
  end

  def query_index_prefix(index_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.query_index_prefix(index_key)

  def query_index_raw_prefix(index_key_prefix),
    do: Ferricstore.Flow.LMDB.IndexCodec.query_index_raw_prefix(index_key_prefix)

  def query_index_key(index_key, id, updated_at_ms),
    do: Ferricstore.Flow.LMDB.IndexCodec.query_index_key(index_key, id, updated_at_ms)

  def query_index_entry_key?(key, id, updated_at_ms),
    do: Ferricstore.Flow.LMDB.IndexCodec.query_index_entry_key?(key, id, updated_at_ms)

  def history_index_prefix(history_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.history_index_prefix(history_key)

  def history_index_key(history_key, event_id, event_ms),
    do: Ferricstore.Flow.LMDB.IndexCodec.history_index_key(history_key, event_id, event_ms)

  def history_index_entry_key?(key, event_id, event_ms),
    do: Ferricstore.Flow.LMDB.IndexCodec.history_index_entry_key?(key, event_id, event_ms)

  def history_expire_prefix, do: Ferricstore.Flow.LMDB.IndexCodec.history_expire_prefix()

  def history_flow_expire_prefix,
    do: Ferricstore.Flow.LMDB.IndexCodec.history_flow_expire_prefix()

  def segment_value_pin_prefix, do: Ferricstore.Flow.LMDB.SegmentPins.prefix()

  def segment_value_pin_prefix(tag) when tag in [:waraft_segment, :waraft_apply_projection],
    do: Ferricstore.Flow.LMDB.SegmentPins.prefix(tag)

  def encode_segment_value_pin_batch(file_id, entries),
    do: Ferricstore.Flow.LMDB.SegmentPins.encode_batch(file_id, entries)

  def segment_value_pin_batch_put_ops(entries),
    do: Ferricstore.Flow.LMDB.SegmentPins.batch_put_ops(entries)

  def segment_value_pin_entries_before(path, trim_index, limit),
    do: Ferricstore.Flow.LMDB.SegmentPins.entries_before(path, trim_index, limit)

  def segment_value_pin_entries_before_page(path, trim_index, after_key, limit),
    do: Ferricstore.Flow.LMDB.SegmentPins.entries_before_page(path, trim_index, after_key, limit)

  def history_expire_key(expire_at_ms, history_index_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.history_expire_key(expire_at_ms, history_index_key)

  def history_flow_expire_key(expire_at_ms, history_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.history_flow_expire_key(expire_at_ms, history_key)

  def encode_history_index_value(event_id, event_ms, compound_key, expire_at_ms \\ 0),
    do:
      Ferricstore.Flow.LMDB.IndexCodec.encode_history_index_value(
        event_id,
        event_ms,
        compound_key,
        expire_at_ms
      )

  def encode_history_index_value(
        event_id,
        event_ms,
        compound_key,
        expire_at_ms,
        file_id,
        offset,
        value_size
      ),
      do:
        Ferricstore.Flow.LMDB.IndexCodec.encode_history_index_value(
          event_id,
          event_ms,
          compound_key,
          expire_at_ms,
          file_id,
          offset,
          value_size
        )

  def encode_history_expire_value(history_index_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.encode_history_expire_value(history_index_key)

  def encode_history_flow_expire_value(history_key, expire_at_ms),
    do:
      Ferricstore.Flow.LMDB.IndexCodec.encode_history_flow_expire_value(history_key, expire_at_ms)

  def decode_history_expire_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_history_expire_value(blob)

  def decode_history_flow_expire_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_history_flow_expire_value(blob)

  def encode_query_index_value(id, updated_at_ms, expire_at_ms \\ 0, state_key \\ nil),
    do:
      Ferricstore.Flow.LMDB.IndexCodec.encode_query_index_value(
        id,
        updated_at_ms,
        expire_at_ms,
        state_key
      )

  def encode_active_index_value(index_key, id, score, expire_at_ms, state_key),
    do:
      Ferricstore.Flow.LMDB.IndexCodec.encode_active_index_value(
        index_key,
        id,
        score,
        expire_at_ms,
        state_key
      )

  def decode_active_index_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_active_index_value(blob)

  def encode_active_index_reverse_value(active_keys),
    do: Ferricstore.Flow.LMDB.IndexCodec.encode_active_index_reverse_value(active_keys)

  def decode_active_index_reverse_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_active_index_reverse_value(blob)

  def delete_state_artifacts(path, state_key) when is_binary(path) and is_binary(state_key) do
    reverse_key = terminal_by_state_key_key(state_key)

    with {:ok, active_ops} <- active_index_delete_ops_result(path, state_key),
         {:ok, terminal_key} <- path |> get(reverse_key) |> terminal_reverse_read() do
      case terminal_key do
        nil ->
          write_batch(path, [{:delete, state_key}, {:delete, reverse_key} | active_ops])

        terminal_key ->
          with {:ok, terminal_ops} <-
                 strict_terminal_index_delete_ops_result(path, terminal_key, state_key) do
            write_batch(path, terminal_ops ++ active_ops)
          end
      end
    end
  end

  @doc false
  def __terminal_reverse_read_for_test__(result), do: terminal_reverse_read(result)

  defp terminal_reverse_read(:not_found), do: {:ok, nil}

  defp terminal_reverse_read({:ok, terminal_key}) when is_binary(terminal_key),
    do: {:ok, terminal_key}

  defp terminal_reverse_read({:error, _reason} = error), do: error
  defp terminal_reverse_read(_invalid), do: {:error, :invalid_terminal_reverse_read}

  def delete_terminal_index_entry(path, terminal_key, state_key)
      when is_binary(path) and is_binary(terminal_key) do
    with {:ok, ops} <- strict_terminal_index_delete_ops_result(path, terminal_key, state_key),
         :ok <- write_batch(path, ops) do
      :ok
    end
  end

  def delete_history_index_entry(path, history_index_key)
      when is_binary(path) and is_binary(history_index_key) do
    with {:ok, ops} <- history_index_delete_ops_result(path, history_index_key) do
      write_batch(path, ops)
    end
  end

  def terminal_index_delete_ops_result(path, terminal_key, state_key)
      when is_binary(path) and is_binary(terminal_key) do
    terminal_result = get(path, terminal_key)

    terminal_index_delete_ops_from_results(
      terminal_key,
      state_key,
      terminal_result,
      fn count_key -> get(path, count_key) end
    )
  end

  defp strict_terminal_index_delete_ops_result(path, terminal_key, state_key) do
    case get(path, terminal_key) do
      :not_found ->
        {:error, :missing_terminal_index}

      terminal_result ->
        terminal_index_delete_ops_from_results(
          terminal_key,
          state_key,
          terminal_result,
          fn count_key -> get(path, count_key) end
        )
    end
  end

  @doc false
  def __terminal_index_delete_ops_result_for_test__(
        terminal_key,
        state_key,
        terminal_result,
        count_result
      ) do
    terminal_index_delete_ops_from_results(
      terminal_key,
      state_key,
      terminal_result,
      fn _count_key -> count_result end
    )
  end

  defp terminal_index_delete_ops_from_results(
         terminal_key,
         _state_key,
         :not_found,
         _count_reader
       ) do
    {:ok, [{:compare_missing, terminal_key}]}
  end

  defp terminal_index_delete_ops_from_results(
         _terminal_key,
         _state_key,
         {:error, _reason} = error,
         _count_reader
       ),
       do: error

  defp terminal_index_delete_ops_from_results(
         terminal_key,
         state_key,
         {:ok, terminal_value},
         count_reader
       )
       when is_binary(terminal_value) do
    with {:ok, {id, updated_at_ms, expire_at_ms, stored_state_key}} <-
           decode_terminal_index_value(terminal_value),
         true <- terminal_index_entry_key?(terminal_key, id, updated_at_ms),
         :ok <- validate_terminal_delete_state_key(state_key, stored_state_key),
         {:ok, count_key} <- terminal_index_count_key(terminal_value),
         {:ok, count, count_value} <- terminal_count_from_read_result(count_reader.(count_key)) do
      ops =
        terminal_key
        |> terminal_index_base_delete_ops(state_key)
        |> prepend_terminal_count_update(count_key, count)
        |> prepend_terminal_expire_delete(
          terminal_key,
          expire_at_ms,
          stored_state_key,
          count_key
        )

      {:ok,
       [
         {:compare, terminal_key, terminal_value},
         {:compare, count_key, count_value}
         | ops
       ]}
    else
      false -> {:error, :invalid_terminal_index_value}
      :missing -> {:error, :invalid_terminal_index_value}
      :error -> {:error, :invalid_terminal_index_value}
      {:error, _reason} = error -> error
    end
  end

  defp terminal_index_delete_ops_from_results(
         _terminal_key,
         _state_key,
         _invalid,
         _count_reader
       ),
       do: {:error, :invalid_terminal_index_read}

  defp terminal_index_base_delete_ops(terminal_key, state_key) do
    ops = [{:delete, terminal_key}]

    if is_binary(state_key) do
      reverse_key = terminal_by_state_key_key(state_key)

      [
        {:compare, reverse_key, terminal_key},
        {:delete, state_key},
        {:delete, reverse_key}
        | ops
      ]
    else
      ops
    end
  end

  defp validate_terminal_delete_state_key(nil, _stored_state_key), do: :ok
  defp validate_terminal_delete_state_key(state_key, state_key), do: :ok

  defp validate_terminal_delete_state_key(_state_key, _stored_state_key),
    do: {:error, :terminal_state_key_mismatch}

  defp terminal_count_from_read_result({:ok, blob}) when is_binary(blob) do
    case decode_count(blob) do
      {:ok, count} -> {:ok, count, blob}
      :error -> {:error, :invalid_terminal_count_value}
    end
  end

  defp terminal_count_from_read_result(:not_found), do: {:error, :terminal_count_missing}
  defp terminal_count_from_read_result({:error, _reason} = error), do: error

  defp terminal_count_from_read_result(_invalid),
    do: {:error, :invalid_terminal_count_read}

  defp prepend_terminal_count_update(ops, count_key, count),
    do: [{:put, count_key, encode_count(max(count - 1, 0))} | ops]

  defp prepend_terminal_expire_delete(
         ops,
         terminal_key,
         expire_at_ms,
         state_key,
         count_key
       )
       when is_integer(expire_at_ms) and expire_at_ms > 0 do
    expire_key = terminal_expire_key(expire_at_ms, terminal_key)
    expire_value = encode_terminal_expire_value(terminal_key, state_key, count_key)
    [{:compare, expire_key, expire_value}, {:delete, expire_key} | ops]
  end

  defp prepend_terminal_expire_delete(
         ops,
         _terminal_key,
         _expire_at_ms,
         _state_key,
         _count_key
       ),
       do: ops

  def encode_terminal_index_value(id, updated_at_ms, expire_at_ms, state_key, count_key),
    do:
      Ferricstore.Flow.LMDB.IndexCodec.encode_terminal_index_value(
        id,
        updated_at_ms,
        expire_at_ms,
        state_key,
        count_key
      )

  def encode_terminal_expire_value(terminal_key, state_key, count_key),
    do:
      Ferricstore.Flow.LMDB.IndexCodec.encode_terminal_expire_value(
        terminal_key,
        state_key,
        count_key
      )

  def decode_terminal_expire_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_terminal_expire_value(blob)

  def encode_count(count), do: Ferricstore.Flow.LMDB.IndexCodec.encode_count(count)

  def decode_count(blob), do: Ferricstore.Flow.LMDB.IndexCodec.decode_count(blob)

  def terminal_count(path, state_index_key),
    do: Ferricstore.Flow.LMDB.TerminalCounts.terminal_count(path, state_index_key)

  def terminal_counts(path, state_index_keys),
    do: Ferricstore.Flow.LMDB.TerminalCounts.terminal_counts(path, state_index_keys)

  def put_terminal_count(path, state_index_key, count),
    do: Ferricstore.Flow.LMDB.TerminalCounts.put_terminal_count(path, state_index_key, count)

  def sweep_expired_terminal(path, now_ms, limit),
    do: Ferricstore.Flow.LMDB.Retention.sweep_expired_terminal(path, now_ms, limit)

  def expired_terminal_state_keys(path, now_ms, limit),
    do: Ferricstore.Flow.LMDB.Retention.expired_terminal_state_keys(path, now_ms, limit)

  def expired_active_timeout_state_keys(path, now_ms, limit),
    do: Ferricstore.Flow.LMDB.Retention.expired_active_timeout_state_keys(path, now_ms, limit)

  def sweep_expired_history(path, now_ms, limit),
    do: Ferricstore.Flow.LMDB.Retention.sweep_expired_history(path, now_ms, limit)

  def decode_terminal_index_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_terminal_index_value(blob)

  def decode_query_index_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_query_index_value(blob)

  def decode_history_index_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_history_index_value(blob)

  def decode_history_index_location(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_history_index_location(blob)

  defp active_flow_index_entries(record) do
    id = Map.fetch!(record, :id)
    type = Map.fetch!(record, :type)
    state = Map.fetch!(record, :state)
    partition_key = Map.get(record, :partition_key)
    updated_score = normalize_ms(Map.get(record, :updated_at_ms))
    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, state, partition_key)

    [{state_index_key, id, updated_score}]
    |> maybe_add_due_active_entry(record, partition_key)
    |> maybe_add_running_active_entries(record, partition_key)
    |> maybe_add_active_timeout_entry(record)
  end

  defp maybe_add_active_timeout_entry(entries, record) do
    case active_timeout_deadline(record) do
      {:ok, deadline_ms} ->
        state_key =
          Ferricstore.Flow.Keys.state_key(record.id, Map.get(record, :partition_key))

        [
          {Ferricstore.Flow.Keys.active_timeout_index_key(), state_key, deadline_ms}
          | entries
        ]

      :none ->
        entries
    end
  end

  defp active_timeout_deadline(record) do
    case {Map.get(record, :state), Map.get(record, :created_at_ms),
          Map.get(record, :max_active_ms)} do
      {flow_state, created_at_ms, max_active_ms}
      when is_integer(created_at_ms) and is_integer(max_active_ms) and max_active_ms > 0 ->
        if terminal_state?(flow_state), do: :none, else: {:ok, created_at_ms + max_active_ms}

      _other ->
        :none
    end
  end

  defp maybe_add_due_active_entry(
         entries,
         %{next_run_at_ms: next_run_at_ms} = record,
         partition_key
       )
       when is_integer(next_run_at_ms) do
    priority = Map.get(record, :priority, 0)
    due_key = Ferricstore.Flow.Keys.due_key(record.type, record.state, priority, partition_key)
    [{due_key, record.id, normalize_ms(next_run_at_ms)} | entries]
  end

  defp maybe_add_due_active_entry(entries, _record, _partition_key), do: entries

  defp maybe_add_running_active_entries(
         entries,
         %{state: "running", lease_deadline_ms: lease_deadline_ms} = record,
         partition_key
       )
       when is_integer(lease_deadline_ms) do
    score = normalize_ms(lease_deadline_ms)
    inflight_key = Ferricstore.Flow.Keys.inflight_index_key(record.type, partition_key)

    worker_key =
      Ferricstore.Flow.Keys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key)

    [
      {worker_key, record.id, score},
      {inflight_key, record.id, score}
      | entries
    ]
  end

  defp maybe_add_running_active_entries(entries, _record, _partition_key), do: entries

  def terminal_index_count_key(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_index_count_key(blob)

  def history_index_delete_ops_result(path, history_index_key)
      when is_binary(path) and is_binary(history_index_key) do
    path
    |> get(history_index_key)
    |> history_index_delete_ops_from_read_result(history_index_key)
  end

  @doc false
  def __history_index_delete_ops_result_for_test__(history_index_key, result),
    do: history_index_delete_ops_from_read_result(result, history_index_key)

  defp history_index_delete_ops_from_read_result(:not_found, history_index_key),
    do: {:ok, [{:compare_missing, history_index_key}]}

  defp history_index_delete_ops_from_read_result({:error, _reason} = error, _history_index_key),
    do: error

  defp history_index_delete_ops_from_read_result({:ok, history_value}, history_index_key)
       when is_binary(history_value) do
    case decode_history_index_value(history_value) do
      {:ok, {event_id, event_ms, expire_at_ms, _compound_key}} ->
        if history_index_entry_key?(history_index_key, event_id, event_ms) do
          ops = [{:delete, history_index_key}]

          ops =
            if expire_at_ms > 0 do
              expire_key = history_expire_key(expire_at_ms, history_index_key)
              expire_value = encode_history_expire_value(history_index_key)
              [{:compare, expire_key, expire_value}, {:delete, expire_key} | ops]
            else
              ops
            end

          {:ok, [{:compare, history_index_key, history_value} | ops]}
        else
          {:error, :invalid_history_index_value}
        end

      :error ->
        {:error, :invalid_history_index_value}
    end
  end

  defp history_index_delete_ops_from_read_result(_invalid, _history_index_key),
    do: {:error, :invalid_history_index_read}

  defp normalize_ms(value)
       when is_integer(value) and value >= 0 and value <= @max_u64,
       do: value

  defp normalize_ms(_value),
    do: raise(ArgumentError, "active index score must be an unsigned 64-bit integer")
end
