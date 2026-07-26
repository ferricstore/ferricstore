defmodule Ferricstore.Raft.WARaftStorage.ApplyProjectionRetention do
  @moduledoc false

  alias Ferricstore.Flow.LMDB

  @entry_prefix <<0, "fapr:1:">>
  @format_version 1
  @max_u64 0xFFFF_FFFF_FFFF_FFFF
  @max_key_bytes 65_535
  @max_value_bytes 1_073_741_824
  @max_page_items 100_000
  @max_page_bytes 64 * 1_024 * 1_024
  @max_atomic_page_bytes 1_073_741_824
  @default_memory_entries 4_096
  @default_memory_bytes 16 * 1_024 * 1_024
  @default_flush_items 256
  @default_flush_bytes 8 * 1_024 * 1_024
  @default_max_atomic_page_bytes @max_atomic_page_bytes
  @release_timeout_ms 5_000

  @enforce_keys [
    :path,
    :trim_index,
    :max_memory_entries,
    :max_memory_bytes,
    :flush_items,
    :flush_bytes,
    :max_atomic_page_bytes
  ]
  defstruct [
    :path,
    :trim_index,
    :max_memory_entries,
    :max_memory_bytes,
    :flush_items,
    :flush_bytes,
    :max_atomic_page_bytes,
    mode: :memory,
    entries: %{},
    entry_bytes: 0,
    buffer: %{},
    buffer_bytes: 0,
    finished?: false
  ]

  @type ref :: {pos_integer(), binary()}
  @type entry :: {binary(), binary(), non_neg_integer()}
  @type t :: %__MODULE__{}

  @spec open(binary(), pos_integer(), keyword()) :: {:ok, t()} | {:error, term()}
  def open(root, trim_index, opts \\ [])

  def open(root, trim_index, opts)
      when is_binary(root) and root != "" and is_integer(trim_index) and trim_index > 0 and
             is_list(opts) do
    with {:ok, limits} <- limits(opts),
         path = Path.join(root, ".apply_projection_retention"),
         :ok <- prepare_path(root, path) do
      {:ok,
       struct!(__MODULE__,
         path: path,
         trim_index: trim_index,
         max_memory_entries: limits.max_memory_entries,
         max_memory_bytes: limits.max_memory_bytes,
         flush_items: limits.flush_items,
         flush_bytes: limits.flush_bytes,
         max_atomic_page_bytes: limits.max_atomic_page_bytes
       )}
    end
  rescue
    error -> {:error, {:open_apply_projection_retention_failed, error}}
  end

  def open(_root, _trim_index, _opts), do: {:error, :invalid_apply_projection_retention}

  @spec put(t(), {ref(), entry()}) :: {:ok, t()} | {:error, term()}
  def put(%__MODULE__{finished?: false} = retention, candidate) do
    with :ok <- validate_candidate(candidate, retention.trim_index) do
      do_put(retention, candidate)
    end
  end

  def put(%__MODULE__{finished?: true}, _candidate),
    do: {:error, :apply_projection_retention_finished}

  def put(_retention, _candidate), do: {:error, :invalid_apply_projection_retention_entry}

  @spec put_many(t(), [{ref(), entry()}]) :: {:ok, t()} | {:error, term()}
  def put_many(%__MODULE__{} = retention, candidates) when is_list(candidates) do
    Enum.reduce_while(candidates, {:ok, retention}, fn candidate, {:ok, current} ->
      case put(current, candidate) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  def put_many(_retention, _candidates), do: {:error, :invalid_apply_projection_retention_entry}

  @spec finish(t()) :: {:ok, t()} | {:error, term()}
  def finish(%__MODULE__{finished?: false, mode: :memory} = retention),
    do: {:ok, %{retention | finished?: true}}

  def finish(%__MODULE__{finished?: false, mode: :disk} = retention) do
    with {:ok, flushed} <- flush(retention) do
      {:ok, %{flushed | finished?: true}}
    end
  end

  def finish(%__MODULE__{finished?: true} = retention), do: {:ok, retention}
  def finish(_retention), do: {:error, :invalid_apply_projection_retention}

  @spec mode(t()) :: :memory | :disk
  def mode(%__MODULE__{mode: mode}) when mode in [:memory, :disk], do: mode

  @spec path(t()) :: binary()
  def path(%__MODULE__{path: path}), do: path

  @doc false
  @spec memory_entry_count(t()) :: non_neg_integer()
  def memory_entry_count(%__MODULE__{entries: entries, buffer: buffer}),
    do: map_size(entries) + map_size(buffer)

  @spec memory_batches(t()) :: {:ok, list()} | {:error, term()}
  def memory_batches(%__MODULE__{mode: :memory, finished?: true, entries: entries}) do
    {:ok, entries |> Map.to_list() |> batches()}
  end

  def memory_batches(_retention), do: {:error, :apply_projection_retention_not_in_memory}

  @spec page(t(), binary(), pos_integer(), pos_integer()) ::
          {:ok, list(), binary(), boolean()} | {:error, term()}
  def page(
        %__MODULE__{
          mode: :disk,
          finished?: true,
          path: path,
          max_atomic_page_bytes: max_atomic_page_bytes
        },
        cursor,
        max_items,
        max_bytes
      )
      when is_binary(cursor) and is_integer(max_items) and max_items > 0 and
             max_items <= @max_page_items and is_integer(max_bytes) and max_bytes > 0 and
             max_bytes <= @max_page_bytes do
    with :ok <- validate_cursor(cursor),
         true <- max_bytes <= max_atomic_page_bytes,
         {:ok, rows, exhausted, bytes} <-
           read_page_rows(
             path,
             @entry_prefix,
             cursor,
             max_items,
             max_bytes,
             max_atomic_page_bytes
           ),
         false <- rows == [] and not exhausted,
         :ok <- validate_page_start(cursor, rows),
         {:ok, page_rows, next_cursor, done?} <-
           complete_page(
             path,
             rows,
             exhausted,
             bytes,
             max_items,
             max_atomic_page_bytes,
             cursor
           ),
         {:ok, entries} <- decode_rows(page_rows) do
      {:ok, batches(entries), next_cursor, done?}
    else
      true -> {:error, :apply_projection_retention_page_made_no_progress}
      false -> {:error, :invalid_apply_projection_retention_page}
      {:error, :range_entry_too_large} -> {:error, :apply_projection_retention_page_too_large}
      {:error, _reason} = error -> error
    end
  end

  def page(_retention, _cursor, _max_items, _max_bytes),
    do: {:error, :invalid_apply_projection_retention_page}

  @spec cleanup(t()) :: :ok | {:error, term()}
  def cleanup(%__MODULE__{path: path}) do
    with :ok <- release_if_present(path),
         {:ok, _removed} <- File.rm_rf(path) do
      :ok
    else
      {:error, reason} -> {:error, {:cleanup_apply_projection_retention_failed, reason}}
    end
  end

  def cleanup(_retention), do: {:error, :invalid_apply_projection_retention}

  defp do_put(%__MODULE__{mode: :memory} = retention, {ref, entry} = candidate) do
    case Map.fetch(retention.entries, ref) do
      {:ok, ^entry} ->
        {:ok, retention}

      {:ok, existing} ->
        conflict(ref, existing, entry)

      :error ->
        bytes = candidate_bytes(candidate)

        if map_size(retention.entries) + 1 <= retention.max_memory_entries and
             retention.entry_bytes + bytes <= retention.max_memory_bytes do
          {:ok,
           %{
             retention
             | entries: Map.put(retention.entries, ref, entry),
               entry_bytes: retention.entry_bytes + bytes
           }}
        else
          with {:ok, spilled} <- spill(retention),
               {:ok, stored} <- do_put(spilled, candidate) do
            {:ok, stored}
          end
        end
    end
  end

  defp do_put(%__MODULE__{mode: :disk} = retention, {ref, entry} = candidate) do
    case Map.fetch(retention.buffer, ref) do
      {:ok, ^entry} ->
        {:ok, retention}

      {:ok, existing} ->
        conflict(ref, existing, entry)

      :error ->
        bytes = candidate_bytes(candidate)

        buffered = %{
          retention
          | buffer: Map.put(retention.buffer, ref, entry),
            buffer_bytes: retention.buffer_bytes + bytes
        }

        if map_size(buffered.buffer) >= buffered.flush_items or
             buffered.buffer_bytes >= buffered.flush_bytes,
           do: flush(buffered),
           else: {:ok, buffered}
    end
  end

  defp spill(%__MODULE__{entries: entries} = retention) do
    disk = %{
      retention
      | mode: :disk,
        entries: %{},
        entry_bytes: 0,
        buffer: entries,
        buffer_bytes:
          Enum.reduce(entries, 0, fn candidate, total -> total + candidate_bytes(candidate) end)
    }

    flush(disk)
  end

  defp flush(%__MODULE__{mode: :disk, buffer: buffer} = retention) when map_size(buffer) == 0,
    do: {:ok, retention}

  defp flush(%__MODULE__{mode: :disk, path: path, buffer: buffer} = retention) do
    expected =
      Map.new(buffer, fn {{index, key}, entry} ->
        {storage_key(index, key), {{index, key}, entry}}
      end)

    ops =
      Enum.map(expected, fn {key, {_ref, entry}} ->
        {:put_new, key, encode_entry(entry)}
      end)

    with {:ok, originals} <- LMDB.write_batch_with_originals(path, ops),
         :ok <- validate_originals(originals, expected) do
      {:ok, %{retention | buffer: %{}, buffer_bytes: 0}}
    end
  end

  defp validate_originals(originals, expected) when length(originals) == map_size(expected) do
    Enum.reduce_while(originals, :ok, fn
      {storage_key, :missing}, :ok when is_binary(storage_key) ->
        if Map.has_key?(expected, storage_key), do: {:cont, :ok}, else: {:halt, corrupt()}

      {storage_key, {:value, encoded}}, :ok
      when is_binary(storage_key) and is_binary(encoded) ->
        with {:ok, {ref, existing}} <- decode_storage_row(storage_key, encoded),
             {^ref, candidate} <- Map.fetch!(expected, storage_key) do
          if existing == candidate,
            do: {:cont, :ok},
            else: {:halt, conflict_error(ref, existing, candidate)}
        else
          _invalid -> {:halt, corrupt()}
        end

      _invalid, :ok ->
        {:halt, corrupt()}
    end)
  end

  defp validate_originals(_originals, _expected), do: corrupt()

  defp decode_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn
      {storage_key, encoded}, {:ok, acc}
      when is_binary(storage_key) and is_binary(encoded) ->
        case decode_storage_row(storage_key, encoded) do
          {:ok, candidate} -> {:cont, {:ok, [candidate | acc]}}
          :error -> {:halt, corrupt()}
        end

      _invalid, _acc ->
        {:halt, corrupt()}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_storage_row(
         <<@entry_prefix::binary, index::unsigned-big-64, digest::binary-size(32)>> = storage_key,
         encoded
       ) do
    with {:ok, entry = {key, _value, _expire_at_ms}} <- decode_entry(encoded),
         true <- storage_key == storage_key(index, key),
         true <- digest == :crypto.hash(:sha256, key) do
      {:ok, {{index, key}, entry}}
    else
      _invalid -> :error
    end
  end

  defp decode_storage_row(_storage_key, _encoded), do: :error

  defp read_page_rows(path, prefix, cursor, max_items, max_bytes, max_atomic_page_bytes) do
    case LMDB.range_entries_bounded(path, prefix, cursor, "", max_items, max_bytes) do
      {:error, :range_entry_too_large} ->
        LMDB.range_entries_bounded_atomic(
          path,
          prefix,
          cursor,
          "",
          1,
          max_atomic_page_bytes
        )

      result ->
        result
    end
  end

  defp complete_page(_path, [], true, _bytes, _max_items, _max_atomic_bytes, cursor),
    do: {:ok, [], cursor, true}

  defp complete_page(_path, rows, true, _bytes, _max_items, _max_atomic_bytes, _cursor),
    do: {:ok, rows, rows |> List.last() |> elem(0), true}

  defp complete_page(path, rows, false, bytes, max_items, max_atomic_bytes, _cursor) do
    {complete, trailing, trailing_index} = split_trailing_index(rows)

    case complete do
      [] ->
        complete_index_page(
          path,
          trailing_index,
          trailing,
          bytes,
          max_items,
          max_atomic_bytes
        )

      _rows ->
        {:ok, complete, complete |> List.last() |> elem(0), false}
    end
  end

  defp split_trailing_index(rows) do
    trailing_index = rows |> List.last() |> elem(0) |> storage_index!()

    {complete, trailing} =
      Enum.split_while(rows, fn {storage_key, _encoded} ->
        storage_index!(storage_key) != trailing_index
      end)

    {complete, trailing, trailing_index}
  end

  defp complete_index_page(path, index, rows, bytes, max_items, max_atomic_bytes) do
    do_complete_index_page(
      path,
      index,
      [rows],
      rows |> List.last() |> elem(0),
      bytes,
      max_items,
      max_atomic_bytes
    )
  end

  defp do_complete_index_page(
         _path,
         _index,
         _chunks,
         _after_key,
         bytes,
         _max_items,
         max_atomic_bytes
       )
       when bytes >= max_atomic_bytes,
       do: {:error, :apply_projection_retention_page_too_large}

  defp do_complete_index_page(
         path,
         index,
         chunks,
         after_key,
         bytes,
         max_items,
         max_atomic_bytes
       ) do
    case LMDB.range_entries_bounded_atomic(
           path,
           index_prefix(index),
           after_key,
           "",
           max_items,
           max_atomic_bytes - bytes
         ) do
      {:ok, [], true, _chunk_bytes} ->
        rows = chunks |> Enum.reverse() |> List.flatten()
        {:ok, rows, after_key, false}

      {:ok, [], false, _chunk_bytes} ->
        {:error, :apply_projection_retention_page_made_no_progress}

      {:ok, rows, exhausted, chunk_bytes} ->
        if Enum.all?(rows, fn {storage_key, _encoded} -> storage_index!(storage_key) == index end) do
          next_after_key = rows |> List.last() |> elem(0)
          next_chunks = [rows | chunks]

          if exhausted do
            completed = next_chunks |> Enum.reverse() |> List.flatten()
            {:ok, completed, next_after_key, false}
          else
            do_complete_index_page(
              path,
              index,
              next_chunks,
              next_after_key,
              bytes + chunk_bytes,
              max_items,
              max_atomic_bytes
            )
          end
        else
          corrupt()
        end

      {:error, :range_entry_too_large} ->
        {:error, :apply_projection_retention_page_too_large}

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_page_start("", _rows), do: :ok
  defp validate_page_start(_cursor, []), do: :ok

  defp validate_page_start(cursor, [{first_key, _encoded} | _rows]) do
    if storage_index!(cursor) == storage_index!(first_key),
      do: {:error, :invalid_apply_projection_retention_cursor},
      else: :ok
  end

  defp encode_entry({key, value, expire_at_ms}) do
    <<@format_version, byte_size(key)::unsigned-big-16, key::binary,
      expire_at_ms::unsigned-big-64, byte_size(value)::unsigned-big-32, value::binary>>
  end

  defp decode_entry(<<@format_version, key_bytes::unsigned-big-16, rest::binary>>) do
    with true <- key_bytes > 0 and key_bytes <= @max_key_bytes,
         <<key::binary-size(key_bytes), expire_at_ms::unsigned-big-64,
           value_bytes::unsigned-big-32, value::binary>> <- rest,
         true <- value_bytes <= @max_value_bytes,
         true <- byte_size(value) == value_bytes do
      {:ok, {:binary.copy(key), :binary.copy(value), expire_at_ms}}
    else
      _invalid -> :error
    end
  end

  defp decode_entry(_encoded), do: :error

  defp batches(entries) do
    entries
    |> Enum.group_by(fn {{index, _key}, _entry} -> index end)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(fn {index, indexed} ->
      retained = indexed |> Enum.map(&elem(&1, 1)) |> Enum.sort_by(&elem(&1, 0))
      {{:raft_log_pos, index, 0}, retained}
    end)
  end

  defp validate_candidate(
         {{index, key}, {entry_key, value, expire_at_ms}},
         trim_index
       )
       when is_integer(index) and index > 0 and index < trim_index and is_binary(key) and
              key != "" and
              byte_size(key) <= @max_key_bytes and entry_key == key and is_binary(value) and
              byte_size(value) <= @max_value_bytes and
              is_integer(expire_at_ms) and expire_at_ms >= 0 and expire_at_ms <= @max_u64,
       do: :ok

  defp validate_candidate(_candidate, _trim_index),
    do: {:error, :invalid_apply_projection_retention_entry}

  defp candidate_bytes({{_index, key}, {_entry_key, value, _expire_at_ms}}),
    do: byte_size(key) + byte_size(value) + 24

  defp storage_key(index, key),
    do: <<@entry_prefix::binary, index::unsigned-big-64, :crypto.hash(:sha256, key)::binary>>

  defp index_prefix(index), do: <<@entry_prefix::binary, index::unsigned-big-64>>

  defp storage_index!(
         <<@entry_prefix::binary, index::unsigned-big-64, _digest::binary-size(32)>>
       ),
       do: index

  defp conflict(ref, existing, candidate),
    do: {:error, {:conflicting_apply_projection_retention, ref, existing, candidate}}

  defp conflict_error(ref, existing, candidate),
    do: {:error, {:conflicting_apply_projection_retention, ref, existing, candidate}}

  defp corrupt, do: {:error, :corrupt_apply_projection_retention}

  defp validate_cursor(""), do: :ok

  defp validate_cursor(
         <<@entry_prefix::binary, _index::unsigned-big-64, _digest::binary-size(32)>>
       ),
       do: :ok

  defp validate_cursor(_cursor), do: {:error, :invalid_apply_projection_retention_cursor}

  defp limits(opts) do
    values = %{
      max_memory_entries: Keyword.get(opts, :max_memory_entries, @default_memory_entries),
      max_memory_bytes: Keyword.get(opts, :max_memory_bytes, @default_memory_bytes),
      flush_items: Keyword.get(opts, :flush_items, @default_flush_items),
      flush_bytes: Keyword.get(opts, :flush_bytes, @default_flush_bytes),
      max_atomic_page_bytes:
        Keyword.get(opts, :max_atomic_page_bytes, @default_max_atomic_page_bytes)
    }

    if is_integer(values.max_memory_entries) and values.max_memory_entries >= 0 and
         is_integer(values.max_memory_bytes) and values.max_memory_bytes >= 0 and
         is_integer(values.flush_items) and values.flush_items > 0 and
         is_integer(values.flush_bytes) and values.flush_bytes > 0 and
         is_integer(values.max_atomic_page_bytes) and values.max_atomic_page_bytes > 0 and
         values.max_atomic_page_bytes <= @max_atomic_page_bytes,
       do: {:ok, values},
       else: {:error, :invalid_apply_projection_retention_limits}
  end

  defp prepare_path(root, path) do
    with :ok <- File.mkdir_p(root),
         :ok <- release_if_present(path),
         {:ok, _removed} <- File.rm_rf(path) do
      :ok
    else
      {:error, reason} -> {:error, {:prepare_apply_projection_retention_failed, reason}}
    end
  end

  defp release_if_present(path) do
    if File.exists?(path), do: LMDB.release(path, @release_timeout_ms), else: :ok
  end
end
