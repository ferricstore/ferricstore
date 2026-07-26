defmodule Ferricstore.Raft.WARaftSegmentReader do
  alias Ferricstore.Raft.WARaftSegmentReader.CommandValues
  alias Ferricstore.Raft.WARaftSegmentReader.DiskReader
  @moduledoc false

  @table_prefix "raft_log_ferricstore_waraft_backend_"
  @storage_root "ferricstore_waraft_backend"
  @projection_dir "segment_projection_log"
  @apply_projection_dir "apply_projection_log"
  @apply_projection_table :ferricstore_waraft_apply_projection_cache
  @apply_projection_count_tag :apply_projection_count
  @apply_projection_bytes_tag :apply_projection_bytes
  @apply_projection_lock_tag :apply_projection_lock
  @apply_projection_disk_lock_tag :apply_projection_disk_lock
  @apply_projection_disk_reader_tag :apply_projection_disk_reader
  @apply_projection_select_page_size 512
  @apply_projection_lock_retry_min_ms 1
  @apply_projection_lock_retry_max_ms 32
  @maximum_location_batch_groups 4_096
  @maximum_location_batch_keys 4_096
  @maximum_physical_read_bytes 1 * 1_024 * 1_024 * 1_024
  @held_apply_projection_locks_key :ferricstore_waraft_apply_projection_held_locks
  @held_apply_projection_disk_locks_key :ferricstore_waraft_apply_projection_held_disk_locks
  @held_apply_projection_disk_read_locks_key :ferricstore_waraft_apply_projection_held_disk_read_locks

  @spec put_apply_projection(binary(), non_neg_integer(), pos_integer(), [
          {binary(), binary(), non_neg_integer()}
        ]) ::
          :ok
  def put_apply_projection(data_dir, shard_index, index, entries)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(index) and index > 0 and is_list(entries) do
    table = ensure_apply_projection_table!()
    root = storage_root(%{data_dir: data_dir}, shard_index)

    with_apply_projection_lock(root, fn ->
      :ok = ensure_apply_projection_counters(table, root)

      {inserted, byte_delta} =
        Enum.reduce(entries, {0, 0}, fn
          {key, value, expire_at_ms}, {inserted, byte_delta}
          when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) ->
            cache_key = {root, index, key}
            entry = {cache_key, value, expire_at_ms}

            case upsert_apply_projection_entry(table, cache_key, entry) do
              {:inserted, value_bytes} ->
                {inserted + 1, byte_delta + value_bytes}

              {:replaced, previous_bytes, value_bytes} ->
                {inserted, byte_delta + value_bytes - previous_bytes}
            end

          _invalid, acc ->
            acc
        end)

      maybe_run_apply_projection_cache_mutation_hook(:after_upsert, %{
        root: root,
        index: index,
        inserted: inserted,
        byte_delta: byte_delta
      })

      increment_apply_projection_count(table, root, inserted)
      adjust_apply_projection_bytes(table, root, byte_delta)
      :ok
    end)
  end

  @spec apply_projection_cache_count(binary(), non_neg_integer()) :: non_neg_integer()
  def apply_projection_cache_count(data_dir, shard_index)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        0

      table ->
        root = storage_root(%{data_dir: data_dir}, shard_index)
        read_apply_projection_count(table, root)
    end
  rescue
    ArgumentError -> 0
  end

  def apply_projection_cache_count(_data_dir, _shard_index), do: 0

  @doc false
  @spec apply_projection_cache_bytes(binary(), non_neg_integer()) :: non_neg_integer()
  def apply_projection_cache_bytes(data_dir, shard_index)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        0

      table ->
        root = storage_root(%{data_dir: data_dir}, shard_index)
        read_apply_projection_bytes(table, root)
    end
  rescue
    ArgumentError -> 0
  end

  def apply_projection_cache_bytes(_data_dir, _shard_index), do: 0

  @doc false
  @spec with_apply_projection_disk_lock(binary(), non_neg_integer(), (-> result)) :: result
        when result: term()
  def with_apply_projection_disk_lock(data_dir, shard_index, fun)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_function(fun, 0) do
    root = storage_root(%{data_dir: data_dir}, shard_index)
    with_apply_projection_disk_lock_root(root, fun)
  end

  @spec apply_projection_dependency_ready?(binary(), non_neg_integer(), pos_integer()) ::
          boolean()
  def apply_projection_dependency_ready?(data_dir, shard_index, index)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(index) and index > 0 do
    root = storage_root(%{data_dir: data_dir}, shard_index)

    case read_apply_projection_latest_entries_from_disk(root, index) do
      {:ok, _entries} -> true
      :not_found -> not apply_projection_cache_entries_present?(root, index)
      {:error, _reason} -> false
    end
  rescue
    ArgumentError -> false
  end

  def apply_projection_dependency_ready?(_data_dir, _shard_index, _index), do: false

  @spec spill_apply_projection_cache(binary(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def spill_apply_projection_cache(data_dir, shard_index)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    spill_apply_projection_cache(data_dir, shard_index, :all)
  end

  def spill_apply_projection_cache(_data_dir, _shard_index), do: {:ok, 0}

  @spec spill_apply_projection_cache(binary(), non_neg_integer(), :all | non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def spill_apply_projection_cache(data_dir, shard_index, max_entries)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    root = storage_root(%{data_dir: data_dir}, shard_index)

    with_apply_projection_disk_lock_root(root, fn ->
      case :ets.whereis(@apply_projection_table) do
        :undefined ->
          {:ok, 0}

        table ->
          projection_root = Path.join(root, @apply_projection_dir)

          table
          |> apply_projection_cache_entries(root, max_entries)
          |> Enum.group_by(fn {index, _key, _value, _expire_at_ms} -> index end)
          |> Enum.sort_by(fn {index, _entries} -> index end)
          |> spill_apply_projection_groups(data_dir, shard_index, projection_root)
      end
    end)
  rescue
    error -> {:error, {:spill_apply_projection_cache_failed, error}}
  end

  def spill_apply_projection_cache(_data_dir, _shard_index, _max_entries), do: {:ok, 0}

  @doc false
  @spec spill_apply_projection_cache(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def spill_apply_projection_cache(data_dir, shard_index, min_entries, min_bytes)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(min_entries) and min_entries >= 0 and is_integer(min_bytes) and
             min_bytes >= 0 do
    root = storage_root(%{data_dir: data_dir}, shard_index)

    with_apply_projection_disk_lock_root(root, fn ->
      case :ets.whereis(@apply_projection_table) do
        :undefined ->
          {:ok, 0}

        table ->
          projection_root = Path.join(root, @apply_projection_dir)

          table
          |> apply_projection_cache_entries_for_limits(root, min_entries, min_bytes)
          |> Enum.group_by(fn {index, _key, _value, _expire_at_ms} -> index end)
          |> Enum.sort_by(fn {index, _entries} -> index end)
          |> spill_apply_projection_groups(data_dir, shard_index, projection_root)
      end
    end)
  rescue
    error -> {:error, {:spill_apply_projection_cache_failed, error}}
  end

  def spill_apply_projection_cache(_data_dir, _shard_index, _min_entries, _min_bytes),
    do: {:ok, 0}

  @doc false
  @spec ensure_apply_projection_entries_durable(binary(), non_neg_integer(), [
          {pos_integer(), binary()}
        ]) :: {:ok, non_neg_integer()} | {:error, term()}
  def ensure_apply_projection_entries_durable(data_dir, shard_index, refs)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_list(refs) do
    with {:ok, refs} <- normalize_apply_projection_refs(refs) do
      root = storage_root(%{data_dir: data_dir}, shard_index)

      with_apply_projection_disk_lock_root(root, fn ->
        indexes = refs |> Enum.map(&elem(&1, 0)) |> MapSet.new()
        projection_root = Path.join(root, @apply_projection_dir)

        groups =
          case :ets.whereis(@apply_projection_table) do
            :undefined ->
              []

            table ->
              table
              |> apply_projection_cache_entries_for_indexes(root, indexes)
              |> Enum.group_by(fn {index, _key, _value, _expire_at_ms} -> index end)
              |> Enum.sort_by(fn {index, _entries} -> index end)
          end

        with {:ok, removed} <-
               spill_apply_projection_groups(groups, data_dir, shard_index, projection_root),
             :ok <- verify_apply_projection_refs_from_disk(root, refs) do
          {:ok, removed}
        end
      end)
    end
  rescue
    error -> {:error, {:ensure_apply_projection_entries_durable_failed, error}}
  catch
    kind, reason -> {:error, {:ensure_apply_projection_entries_durable_failed, {kind, reason}}}
  end

  def ensure_apply_projection_entries_durable(_data_dir, _shard_index, _refs),
    do: {:error, :invalid_apply_projection_refs}

  @doc false
  @spec physical_location(FerricStore.Instance.t(), non_neg_integer(), term()) ::
          {:ok, {non_neg_integer(), non_neg_integer(), pos_integer()}} | {:error, term()}
  def physical_location(ctx, shard_index, {kind, index})
      when is_map(ctx) and is_integer(shard_index) and shard_index >= 0 and
             kind in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
             is_integer(index) and index > 0 do
    root = storage_root(ctx, shard_index)

    locate = fn ->
      segment_root =
        case kind do
          :waraft_segment -> root
          :waraft_projection -> Path.join(root, @projection_dir)
          :waraft_apply_projection -> Path.join(root, @apply_projection_dir)
        end

      locate_physical_frame(segment_root, index)
    end

    if kind == :waraft_apply_projection,
      do: with_apply_projection_disk_read_lock_root(root, locate),
      else: locate.()
  rescue
    error -> {:error, {:physical_location_failed, error}}
  catch
    kind, reason -> {:error, {:physical_location_failed, {kind, reason}}}
  end

  def physical_location(_ctx, _shard_index, _file_id),
    do: {:error, :invalid_waraft_location}

  @doc false
  @spec read_physical_values(
          FerricStore.Instance.t(),
          non_neg_integer(),
          [map()],
          pos_integer(),
          :live | :include_expired
        ) :: {:ok, %{binary() => binary()}} | {:error, term()}
  def read_physical_values(ctx, shard_index, requests, timeout_ms, read_mode)
      when is_map(ctx) and is_integer(shard_index) and shard_index >= 0 and is_list(requests) and
             is_integer(timeout_ms) and timeout_ms > 0 and
             read_mode in [:live, :include_expired] do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms

    with :ok <- validate_physical_requests(requests),
         false <- waraft_read_expired?(deadline_ms),
         {:ok, values} <-
           do_read_physical_values(ctx, shard_index, requests, read_mode, deadline_ms),
         false <- waraft_read_expired?(deadline_ms) do
      {:ok, values}
    else
      true -> {:error, :deadline_exceeded}
      {:error, _reason} = error -> error
    end
  rescue
    error -> {:error, {:physical_read_failed, error}}
  catch
    kind, reason -> {:error, {:physical_read_failed, {kind, reason}}}
  end

  def read_physical_values(_ctx, _shard_index, _requests, _timeout_ms, _read_mode),
    do: {:error, :invalid_physical_read}

  defp validate_physical_requests(requests) do
    requests
    |> Enum.reduce_while({0, 0, MapSet.new(), MapSet.new()}, fn
      %{
        file_id: {kind, index},
        ordinal: ordinal,
        offset: offset,
        frame_size: frame_size,
        key: key
      },
      {count, bytes, seen_keys, seen_frames}
      when kind in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
             is_integer(index) and index > 0 and is_integer(ordinal) and ordinal >= 0 and
             is_integer(offset) and offset >= 0 and is_integer(frame_size) and frame_size >= 8 and
             is_binary(key) and key != "" and count < @maximum_location_batch_keys ->
        frame = {{kind, index}, ordinal, offset, frame_size}

        cond do
          MapSet.member?(seen_keys, key) ->
            {:halt, :invalid}

          MapSet.member?(seen_frames, frame) ->
            {:cont, {count + 1, bytes, MapSet.put(seen_keys, key), seen_frames}}

          bytes + frame_size > @maximum_physical_read_bytes ->
            {:halt, :byte_budget_exceeded}

          true ->
            {:cont,
             {count + 1, bytes + frame_size, MapSet.put(seen_keys, key),
              MapSet.put(seen_frames, frame)}}
        end

      _invalid, _acc ->
        {:halt, :invalid}
    end)
    |> case do
      :invalid -> {:error, :invalid_physical_read}
      :byte_budget_exceeded -> {:error, :physical_read_byte_budget_exceeded}
      {_count, _bytes, _seen_keys, _seen_frames} -> :ok
    end
  end

  defp do_read_physical_values(ctx, shard_index, requests, read_mode, deadline_ms) do
    requests
    |> Enum.group_by(fn %{file_id: {kind, _index}} -> kind end)
    |> Enum.reduce_while({:ok, %{}}, fn {kind, grouped}, {:ok, values} ->
      case read_physical_kind(ctx, shard_index, kind, grouped, read_mode, deadline_ms) do
        {:ok, kind_values} -> {:cont, {:ok, Map.merge(values, kind_values)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp read_physical_kind(
         ctx,
         shard_index,
         :waraft_apply_projection,
         requests,
         read_mode,
         deadline_ms
       ) do
    root = storage_root(ctx, shard_index)
    log_root = Path.join(root, @apply_projection_dir)

    with_apply_projection_disk_read_lock_root(root, deadline_ms, fn ->
      read_physical_frames(root, log_root, requests, read_mode, deadline_ms)
    end)
  end

  defp read_physical_kind(ctx, shard_index, kind, requests, read_mode, deadline_ms)
       when kind in [:waraft_segment, :waraft_projection] do
    root = storage_root(ctx, shard_index)

    log_root =
      case kind do
        :waraft_segment -> root
        :waraft_projection -> Path.join(root, @projection_dir)
      end

    read_physical_frames(root, log_root, requests, read_mode, deadline_ms)
  end

  defp read_physical_frames(root, log_root, requests, read_mode, deadline_ms) do
    frames = unique_physical_frames(requests)

    locations =
      Enum.map(frames, fn %{file_id: {_kind, index}} = frame ->
        {index, {frame.ordinal, frame.offset, frame.frame_size}}
      end)

    with {:ok, timeout_ms} <- remaining_waraft_read_timeout(deadline_ms) do
      case DiskReader.read_many_at(root, log_root, locations, timeout_ms) do
        {:ok, entries} when length(entries) == length(frames) ->
          decode_physical_frames(frames, entries, read_mode, %{})

        {:ok, _entries} ->
          {:error, :physical_read_count_mismatch}

        {:error, {:segment_reader_unavailable, _reason}} ->
          read_physical_frames_fallback_bounded(log_root, requests, read_mode, deadline_ms)

        {:error, {:segment_reader_timeout, _reason}} ->
          {:error, :deadline_exceeded}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp read_physical_frames_fallback_bounded(log_root, requests, read_mode, deadline_ms) do
    with {:ok, timeout_ms} <- remaining_waraft_read_timeout(deadline_ms) do
      task = Task.async(fn -> read_physical_frames_fallback(log_root, requests, read_mode) end)

      case Task.yield(task, timeout_ms) || Task.shutdown(task, :brutal_kill) do
        {:ok, result} -> result
        nil -> {:error, :deadline_exceeded}
        {:exit, reason} -> {:error, {:physical_read_fallback_failed, reason}}
      end
    end
  end

  defp read_physical_frames_fallback(log_root, requests, read_mode) do
    frames = unique_physical_frames(requests)

    with {:ok, entries} <- read_physical_frame_groups(log_root, frames) do
      decode_physical_frames(frames, entries, read_mode, %{})
    end
  end

  defp read_physical_frame_groups(_log_root, []), do: {:ok, []}

  defp read_physical_frame_groups(log_root, frames) do
    groups =
      frames
      |> Enum.with_index()
      |> Enum.group_by(fn {frame, _position} -> frame.ordinal end)

    groups
    |> Enum.reduce_while({:ok, %{}}, fn
      {_ordinal, group}, {:ok, entries} ->
        case read_physical_frame_group(log_root, group) do
          {:ok, group_entries} ->
            positioned =
              group
              |> Enum.zip(group_entries)
              |> Enum.reduce(entries, fn {{_frame, position}, entry}, acc ->
                Map.put(acc, position, entry)
              end)

            {:cont, {:ok, positioned}}

          {:error, _reason} = error ->
            {:halt, error}
        end
    end)
    |> case do
      {:ok, entries} ->
        {:ok, Enum.map(0..(length(frames) - 1), &Map.fetch!(entries, &1))}

      {:error, _reason} = error ->
        error
    end
  end

  defp read_physical_frame_group(log_root, [{%{ordinal: ordinal} = first, _position} | _] = group) do
    %{file_id: {_kind, first_index}} = first
    root_chars = to_charlist(log_root)

    case :ferricstore_waraft_spike_segment_log.open_disk_reader(
           root_chars,
           first_index,
           ordinal
         ) do
      {:ok, reader} ->
        try do
          reads =
            Enum.map(group, fn {%{file_id: {_kind, index}} = frame, _position} ->
              {index, frame.offset, frame.frame_size}
            end)

          case :ferricstore_waraft_spike_segment_log.read_disk_reader_many(reader, reads) do
            {:ok, entries} when length(entries) == length(group) -> {:ok, entries}
            {:ok, _entries} -> {:error, :physical_read_count_mismatch}
            :not_found -> {:error, :physical_segment_entry_not_found}
            {:error, reason} -> {:error, reason}
            invalid -> {:error, {:invalid_physical_segment_read, invalid}}
          end
        after
          _ = :ferricstore_waraft_spike_segment_log.close_disk_reader(reader)
        end

      :not_found ->
        {:error, :physical_segment_entry_not_found}

      {:error, reason} ->
        {:error, reason}

      invalid ->
        {:error, {:invalid_physical_segment_reader, invalid}}
    end
  end

  defp unique_physical_frames(requests) do
    requests
    |> Enum.group_by(fn request ->
      {request.file_id, request.ordinal, request.offset, request.frame_size}
    end)
    |> Enum.map(fn {_location, grouped} ->
      first = hd(grouped)
      Map.put(first, :keys, Enum.map(grouped, & &1.key))
    end)
    |> Enum.sort_by(&{&1.ordinal, &1.offset, &1.file_id})
  end

  defp decode_physical_frames([], [], _read_mode, values), do: {:ok, values}

  defp decode_physical_frames([frame | frames], [entry | entries], read_mode, values) do
    case decode_physical_frame(frame, entry, read_mode, values) do
      {:ok, values} -> decode_physical_frames(frames, entries, read_mode, values)
      {:error, _reason} = error -> error
    end
  end

  defp decode_physical_frames(_frames, _entries, _read_mode, _values),
    do: {:error, :physical_read_count_mismatch}

  defp decode_physical_frame(
         %{file_id: {:waraft_apply_projection, _index}, keys: keys},
         entry,
         read_mode,
         values
       ) do
    projection_mode =
      if read_mode == :live, do: {:live, CommandValues.now_ms()}, else: :include_expired

    case decode_apply_projection_entry(entry) do
      {:ok, entries} ->
        {:ok, collect_projection_entry_values(entries, keys, values, projection_mode)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_physical_frame(
         %{file_id: {:waraft_projection, _index}, keys: keys},
         entry,
         read_mode,
         values
       ) do
    case entry do
      {0, {:ferricstore_segment_projection_entry, key, value, expire_at_ms}}
      when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) ->
        visible? = read_mode == :include_expired or CommandValues.live_expire_at?(expire_at_ms)

        if visible? and key in keys,
          do: {:ok, Map.put(values, key, value)},
          else: {:ok, values}

      _invalid ->
        {:error, :bad_segment_projection_entry}
    end
  end

  defp decode_physical_frame(
         %{file_id: {:waraft_segment, _index}, keys: keys},
         entry,
         _read_mode,
         values
       ) do
    {:ok, Map.merge(values, CommandValues.values_from_entry(entry, keys))}
  end

  defp locate_physical_frame(root, index) do
    case :ferricstore_waraft_spike_segment_log.location_for_index(to_charlist(root), index) do
      {:ok, {ordinal, offset, frame_size}}
      when is_integer(ordinal) and ordinal >= 0 and is_integer(offset) and offset >= 0 and
             is_integer(frame_size) and frame_size >= 8 ->
        {:ok, {ordinal, offset, frame_size}}

      :not_found ->
        {:error, :segment_entry_not_found}

      {:error, :enoent} ->
        {:error, :segment_entry_not_found}

      {:error, reason} ->
        {:error, reason}

      _invalid ->
        {:error, :invalid_segment_location}
    end
  end

  @spec apply_projection_refs_before(binary(), non_neg_integer(), pos_integer()) :: [
          {pos_integer(), binary()}
        ]
  def apply_projection_refs_before(data_dir, shard_index, before_index)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(before_index) and before_index > 0 do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        []

      table ->
        root = storage_root(%{data_dir: data_dir}, shard_index)

        :ets.select(table, [
          {{{root, :"$1", :"$2"}, :_, :_}, [{:<, :"$1", before_index}], [{{:"$1", :"$2"}}]}
        ])
    end
  rescue
    ArgumentError -> []
  end

  def apply_projection_refs_before(_data_dir, _shard_index, _before_index), do: []

  @spec delete_apply_projection_entries(binary(), non_neg_integer(), [
          {pos_integer(), binary()}
        ]) :: non_neg_integer()
  def delete_apply_projection_entries(data_dir, shard_index, refs)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_list(refs) do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        0

      table ->
        root = storage_root(%{data_dir: data_dir}, shard_index)

        with_apply_projection_lock(root, fn ->
          :ok = ensure_apply_projection_counters(table, root)

          {removed, removed_bytes} =
            Enum.reduce(refs, {0, 0}, fn
              {index, key}, {removed, removed_bytes}
              when is_integer(index) and index > 0 and is_binary(key) ->
                case :ets.take(table, {root, index, key}) do
                  [] ->
                    {removed, removed_bytes}

                  [{{^root, ^index, ^key}, value, _expire_at_ms}] when is_binary(value) ->
                    {removed + 1, removed_bytes + byte_size(value)}
                end

              _invalid, acc ->
                acc
            end)

          decrement_apply_projection_count(table, root, removed)
          adjust_apply_projection_bytes(table, root, -removed_bytes)
          removed
        end)
    end
  rescue
    ArgumentError -> 0
  end

  def delete_apply_projection_entries(_data_dir, _shard_index, _refs), do: 0

  @spec clear_apply_projection_cache(binary(), non_neg_integer()) :: non_neg_integer()
  def clear_apply_projection_cache(data_dir, shard_index)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        0

      table ->
        root = storage_root(%{data_dir: data_dir}, shard_index)

        with_apply_projection_lock(root, fn ->
          removed =
            :ets.select_delete(table, [
              {{{root, :_, :_}, :_, :_}, [], [true]}
            ])

          :ets.delete(table, apply_projection_count_key(root))
          :ets.delete(table, apply_projection_bytes_key(root))
          removed
        end)
    end
  rescue
    ArgumentError -> 0
  end

  def clear_apply_projection_cache(_data_dir, _shard_index), do: 0

  @spec read_value(FerricStore.Instance.t(), non_neg_integer(), non_neg_integer(), binary()) ::
          {:ok, binary()} | :not_found | {:error, term()}
  def read_value(ctx, shard_index, index, key)
      when is_integer(shard_index) and shard_index >= 0 and is_integer(index) and index > 0 and
             is_binary(key) do
    case read_main_log_value(ctx, shard_index, index, key) do
      {:error, :segment_entry_not_found} ->
        {:error, :segment_entry_not_found}

      {:error, :key_not_in_segment_entry} ->
        :not_found

      other ->
        other
    end
  end

  def read_value(_ctx, _shard_index, _index, _key), do: {:error, :bad_segment_location}

  @spec read_value_from_location(FerricStore.Instance.t(), non_neg_integer(), term(), binary()) ::
          {:ok, binary()} | :not_found | {:error, term()}
  def read_value_from_location(ctx, shard_index, {:waraft_segment, index}, key),
    do: read_value(ctx, shard_index, index, key)

  def read_value_from_location(ctx, shard_index, {:waraft_projection, index}, key),
    do: read_projection_value_at(ctx, shard_index, index, key)

  def read_value_from_location(ctx, shard_index, {:waraft_apply_projection, index}, key),
    do: read_apply_projection_value_at(ctx, shard_index, index, key)

  def read_value_from_location(_ctx, _shard_index, _file_id, _key),
    do: {:error, :not_waraft_segment_location}

  @doc false
  @spec read_value_from_location_including_expired(
          FerricStore.Instance.t(),
          non_neg_integer(),
          term(),
          binary()
        ) ::
          {:ok, binary()} | :not_found | {:error, term()}
  def read_value_from_location_including_expired(
        ctx,
        shard_index,
        {:waraft_apply_projection, index},
        key
      ) do
    read_apply_projection_value_at(ctx, shard_index, index, key, :include_expired)
  end

  def read_value_from_location_including_expired(ctx, shard_index, file_id, key) do
    read_value_from_location(ctx, shard_index, file_id, key)
  end

  @doc false
  @spec read_values_from_location_including_expired(
          FerricStore.Instance.t(),
          non_neg_integer(),
          term(),
          [binary()],
          non_neg_integer()
        ) :: {:ok, %{binary() => binary()}} | {:error, term()}
  def read_values_from_location_including_expired(
        ctx,
        shard_index,
        file_id,
        keys,
        timeout_ms
      )
      when is_list(keys) and is_integer(timeout_ms) and timeout_ms >= 0 do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms

    if waraft_read_expired?(deadline_ms) do
      {:error, :deadline_exceeded}
    else
      result = read_values_from_location_including_expired(ctx, shard_index, file_id, keys)

      if waraft_read_expired?(deadline_ms),
        do: {:error, :deadline_exceeded},
        else: result
    end
  end

  def read_values_from_location_including_expired(
        _ctx,
        _shard_index,
        _file_id,
        _keys,
        _timeout_ms
      ),
      do: {:error, :bad_segment_location}

  @spec read_values_from_location(FerricStore.Instance.t(), non_neg_integer(), term(), [
          binary()
        ]) ::
          {:ok, %{binary() => binary()}} | {:error, term()}

  @spec read_values_from_location(
          FerricStore.Instance.t(),
          non_neg_integer(),
          term(),
          [binary()],
          non_neg_integer()
        ) :: {:ok, %{binary() => binary()}} | {:error, term()}
  def read_values_from_location(ctx, shard_index, file_id, keys, timeout_ms)
      when is_list(keys) and is_integer(timeout_ms) and timeout_ms >= 0 do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms

    if waraft_read_expired?(deadline_ms) do
      {:error, :deadline_exceeded}
    else
      result = read_values_from_location(ctx, shard_index, file_id, keys)

      if waraft_read_expired?(deadline_ms),
        do: {:error, :deadline_exceeded},
        else: result
    end
  end

  def read_values_from_location(ctx, shard_index, {:waraft_segment, index}, keys)
      when is_integer(index) and index > 0 and is_list(keys) do
    case read_main_log_entry(ctx, shard_index, index) do
      {:ok, entry} ->
        {:ok, CommandValues.values_from_entry(entry, keys)}

      {:error, :segment_entry_not_found} ->
        {:error, :segment_entry_not_found}

      {:error, :key_not_in_segment_entry} ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def read_values_from_location(ctx, shard_index, {:waraft_projection, index}, keys)
      when is_integer(index) and index > 0 and is_list(keys) do
    projection_root = Path.join(storage_root(ctx, shard_index), @projection_dir)
    root_chars = to_charlist(projection_root)
    keyset = MapSet.new(keys)

    case read_projection_entry_at(root_chars, index) do
      {:ok, entry} ->
        case entry do
          {0, {:ferricstore_segment_projection_entry, key, value, _expire_at_ms}}
          when is_binary(key) and is_binary(value) ->
            if MapSet.member?(keyset, key), do: {:ok, %{key => value}}, else: {:ok, %{}}

          _other ->
            {:error, :bad_segment_projection_entry}
        end

      :not_found ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def read_values_from_location(ctx, shard_index, {:waraft_apply_projection, index}, keys)
      when is_integer(index) and index > 0 and is_list(keys) do
    root = storage_root(ctx, shard_index)
    read_apply_projection_values_at(root, index, keys, {:live, CommandValues.now_ms()})
  end

  def read_values_from_location(ctx, shard_index, file_id, keys) when is_list(keys) do
    Enum.reduce_while(keys, {:ok, %{}}, fn key, {:ok, acc} ->
      case read_value_from_location(ctx, shard_index, file_id, key) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, key, value)}}
        :not_found -> {:cont, {:ok, acc}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @doc false
  @spec read_values_from_locations(
          FerricStore.Instance.t(),
          non_neg_integer(),
          [{term(), [binary()]}],
          non_neg_integer()
        ) :: {:ok, %{binary() => binary()}} | {:error, term()}
  def read_values_from_locations(ctx, shard_index, groups, timeout_ms)
      when is_list(groups) and is_integer(timeout_ms) and timeout_ms >= 0 do
    read_values_from_locations_with_mode(ctx, shard_index, groups, timeout_ms, :live)
  end

  @doc false
  @spec read_values_from_locations_including_expired(
          FerricStore.Instance.t(),
          non_neg_integer(),
          [{term(), [binary()]}],
          non_neg_integer()
        ) :: {:ok, %{binary() => binary()}} | {:error, term()}
  def read_values_from_locations_including_expired(ctx, shard_index, groups, timeout_ms)
      when is_list(groups) and is_integer(timeout_ms) and timeout_ms >= 0 do
    read_values_from_locations_with_mode(
      ctx,
      shard_index,
      groups,
      timeout_ms,
      :include_expired
    )
  end

  defp read_values_from_locations_with_mode(ctx, shard_index, groups, timeout_ms, read_mode) do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms

    with :ok <- validate_location_groups(groups),
         false <- waraft_read_expired?(deadline_ms),
         {:ok, values} <- do_read_values_from_locations(ctx, shard_index, groups, read_mode),
         false <- waraft_read_expired?(deadline_ms) do
      {:ok, values}
    else
      true -> {:error, :deadline_exceeded}
      {:error, _reason} = error -> error
    end
  end

  defp do_read_values_from_locations(ctx, shard_index, groups, read_mode) do
    case Enum.all?(groups, fn
           {{:waraft_apply_projection, index}, keys} ->
             is_integer(index) and index > 0 and is_list(keys)

           _other ->
             false
         end) do
      true ->
        projection_mode =
          if read_mode == :live,
            do: {:live, CommandValues.now_ms()},
            else: :include_expired

        read_apply_projection_values_many(ctx, shard_index, groups, projection_mode)

      false ->
        read_location_groups_sequential(ctx, shard_index, groups, read_mode)
    end
  end

  defp read_location_groups_sequential(ctx, shard_index, groups, read_mode) do
    Enum.reduce_while(groups, {:ok, %{}}, fn {file_id, keys}, {:ok, values} ->
      result =
        case read_mode do
          :include_expired ->
            read_values_from_location_including_expired(ctx, shard_index, file_id, keys)

          :live ->
            read_values_from_location(ctx, shard_index, file_id, keys)
        end

      case result do
        {:ok, group_values} when is_map(group_values) ->
          {:cont, {:ok, Map.merge(values, group_values)}}

        {:error, _reason} = error ->
          {:halt, error}

        _invalid ->
          {:halt, {:error, :invalid_location_batch_read}}
      end
    end)
  end

  defp validate_location_groups(groups),
    do: validate_location_groups(groups, 0, 0, MapSet.new())

  defp validate_location_groups([], _group_count, _key_count, _seen), do: :ok

  defp validate_location_groups(
         [{file_id, [_ | _] = keys} | groups],
         group_count,
         key_count,
         seen
       )
       when group_count < @maximum_location_batch_groups do
    with true <- valid_location_file_id?(file_id),
         {:ok, next_key_count, seen} <- validate_location_keys(keys, key_count, seen) do
      validate_location_groups(groups, group_count + 1, next_key_count, seen)
    else
      _invalid -> {:error, :invalid_location_batch}
    end
  end

  defp validate_location_groups(_groups, _group_count, _key_count, _seen),
    do: {:error, :invalid_location_batch}

  defp validate_location_keys([], key_count, seen), do: {:ok, key_count, seen}

  defp validate_location_keys([key | keys], key_count, seen)
       when is_binary(key) and byte_size(key) > 0 and
              key_count < @maximum_location_batch_keys do
    if MapSet.member?(seen, key) do
      {:error, :duplicate_location_key}
    else
      validate_location_keys(keys, key_count + 1, MapSet.put(seen, key))
    end
  end

  defp validate_location_keys(_keys, _key_count, _seen),
    do: {:error, :invalid_location_key}

  defp valid_location_file_id?({kind, index})
       when kind in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
              is_integer(index) and index > 0,
       do: true

  defp valid_location_file_id?(_file_id), do: false

  defp read_values_from_location_including_expired(
         ctx,
         shard_index,
         {:waraft_apply_projection, index},
         keys
       )
       when is_integer(index) and index > 0 and is_list(keys) do
    root = storage_root(ctx, shard_index)
    read_apply_projection_values_at(root, index, keys, :include_expired)
  end

  defp read_values_from_location_including_expired(ctx, shard_index, file_id, keys),
    do: read_values_from_location(ctx, shard_index, file_id, keys)

  defp read_apply_projection_values_at(root, index, keys, read_mode) do
    {found, missing} =
      keys
      |> Enum.uniq()
      |> Enum.reduce({%{}, []}, fn key, {found, missing} ->
        case read_apply_projection_cache(root, index, key, read_mode) do
          {:ok, value} -> {Map.put(found, key, value), missing}
          :expired -> {found, missing}
          :not_found -> {found, [key | missing]}
        end
      end)

    case missing do
      [] ->
        {:ok, found}

      [_ | _] ->
        read_apply_projection_values_from_disk(root, index, missing, found, read_mode)
    end
  end

  defp read_apply_projection_values_many(ctx, shard_index, groups, read_mode) do
    root = storage_root(ctx, shard_index)

    {found, missing_groups} =
      Enum.reduce(groups, {%{}, []}, fn {{:waraft_apply_projection, index}, keys},
                                        {found, missing_groups} ->
        {found, missing} =
          keys
          |> Enum.uniq()
          |> Enum.reduce({found, []}, fn key, {found, missing} ->
            case read_apply_projection_cache(root, index, key, read_mode) do
              {:ok, value} -> {Map.put(found, key, value), missing}
              :expired -> {found, missing}
              :not_found -> {found, [key | missing]}
            end
          end)

        case missing do
          [] -> {found, missing_groups}
          [_ | _] -> {found, [{index, Enum.reverse(missing)} | missing_groups]}
        end
      end)

    case missing_groups do
      [] ->
        {:ok, found}

      [_ | _] ->
        missing_groups = Enum.reverse(missing_groups)

        with {:ok, latest_by_index} <-
               read_apply_projection_latest_entries_many_from_disk(root, missing_groups),
             {:ok, found} <-
               collect_apply_projection_batch_values(
                 root,
                 missing_groups,
                 latest_by_index,
                 found,
                 read_mode
               ) do
          {:ok, found}
        end
    end
  end

  defp read_apply_projection_latest_entries_many_from_disk(root, groups) do
    with_apply_projection_disk_read_lock_root(root, fn ->
      projection_root = Path.join(root, @apply_projection_dir)
      root_chars = to_charlist(projection_root)

      Enum.each(groups, fn {index, _keys} ->
        maybe_run_apply_projection_disk_read_hook(root, index, :latest)
      end)

      with {:ok, locations} <- locate_apply_projection_batch(root_chars, groups, []),
           {:ok, entries} <- read_apply_projection_disk_batch(root, root_chars, locations),
           {:ok, decoded} <- decode_apply_projection_disk_batch(locations, entries, %{}) do
        {:ok, decoded}
      end
    end)
  end

  defp locate_apply_projection_batch(_root_chars, [], locations),
    do: {:ok, Enum.reverse(locations)}

  defp locate_apply_projection_batch(root_chars, [{index, _keys} | groups], locations) do
    case :ferricstore_waraft_spike_segment_log.location_for_index(root_chars, index) do
      {:ok, location} ->
        locate_apply_projection_batch(root_chars, groups, [{index, location} | locations])

      :not_found ->
        {:error, :apply_projection_entry_missing_at_recorded_location}

      {:error, :enoent} ->
        {:error, :apply_projection_entry_missing_at_recorded_location}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_apply_projection_disk_batch(_root, _root_chars, []), do: {:ok, []}

  defp read_apply_projection_disk_batch(root, root_chars, locations) do
    case DiskReader.read_many(root, locations) do
      {:ok, entries} when length(entries) == length(locations) ->
        {:ok, entries}

      {:ok, _entries} ->
        {:error, :apply_projection_batch_count_mismatch}

      {:error, {:segment_reader_unavailable, _reason}} ->
        read_apply_projection_disk_batch_fallback(root_chars, locations, [])

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_apply_projection_disk_batch_fallback(_root_chars, [], entries),
    do: {:ok, Enum.reverse(entries)}

  defp read_apply_projection_disk_batch_fallback(
         root_chars,
         [{index, {_ordinal, offset, encoded_size}} | locations],
         entries
       ) do
    case :ferricstore_waraft_spike_segment_log.read_disk_at(
           root_chars,
           index,
           offset,
           encoded_size
         ) do
      {:ok, entry} ->
        read_apply_projection_disk_batch_fallback(root_chars, locations, [entry | entries])

      :not_found ->
        {:error, :apply_projection_entry_missing_at_recorded_location}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_apply_projection_disk_batch([], [], decoded), do: {:ok, decoded}

  defp decode_apply_projection_disk_batch(
         [{index, _location} | locations],
         [entry | entries],
         decoded
       ) do
    case decode_apply_projection_entry(entry) do
      {:ok, projection_entries} ->
        decode_apply_projection_disk_batch(
          locations,
          entries,
          Map.put(decoded, index, projection_entries)
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_apply_projection_disk_batch(_locations, _entries, _decoded),
    do: {:error, :apply_projection_batch_count_mismatch}

  defp collect_apply_projection_batch_values(
         root,
         groups,
         latest_by_index,
         found,
         read_mode
       ) do
    Enum.reduce_while(groups, {:ok, found}, fn {index, keys}, {:ok, found} ->
      found =
        case Map.fetch(latest_by_index, index) do
          {:ok, entries} -> collect_projection_entry_values(entries, keys, found, read_mode)
          :error -> found
        end

      case reject_found_keys(keys, found) do
        [] ->
          {:cont, {:ok, found}}

        still_missing ->
          case read_apply_projection_missing_from_merged_disk(
                 root,
                 index,
                 still_missing,
                 found,
                 read_mode
               ) do
            {:ok, next} -> {:cont, {:ok, next}}
            {:error, _reason} = error -> {:halt, error}
          end
      end
    end)
  end

  defp read_apply_projection_values_from_disk(root, index, keys, found, read_mode) do
    case read_apply_projection_latest_entries_from_disk(root, index) do
      {:ok, entries} ->
        found = collect_projection_entry_values(entries, keys, found, read_mode)
        still_missing = reject_found_keys(keys, found)

        if still_missing == [] do
          {:ok, found}
        else
          read_apply_projection_missing_from_merged_disk(
            root,
            index,
            still_missing,
            found,
            read_mode
          )
        end

      :not_found ->
        {:ok, found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp waraft_read_expired?(deadline_ms),
    do: System.monotonic_time(:millisecond) >= deadline_ms

  defp remaining_waraft_read_timeout(deadline_ms) when is_integer(deadline_ms) do
    remaining_ms = deadline_ms - System.monotonic_time(:millisecond)

    if remaining_ms > 0,
      do: {:ok, remaining_ms},
      else: {:error, :deadline_exceeded}
  end

  defp read_apply_projection_missing_from_merged_disk(root, index, missing, found, read_mode) do
    case read_apply_projection_entries_from_disk(root, index) do
      {:ok, entries} ->
        {:ok, collect_projection_entry_values(entries, missing, found, read_mode)}

      :not_found ->
        {:ok, found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp reject_found_keys(keys, found) when is_map(found) do
    Enum.reject(keys, &Map.has_key?(found, &1))
  end

  defp reject_found_keys(keys, _found), do: keys

  defp read_main_log_value(ctx, shard_index, index, key) do
    with {:ok, entry} <- read_main_log_entry(ctx, shard_index, index) do
      case CommandValues.value_from_entry(entry, key) do
        {:ok, value} -> {:ok, value}
        :deleted -> :not_found
        :not_found -> {:error, :key_not_in_segment_entry}
        {:error, _reason} = error -> error
      end
    end
  end

  defp read_main_log_entry(ctx, shard_index, index) do
    table = log_table(shard_index)

    case ets_log_lookup(table, index) do
      {:ok, entry} ->
        {:ok, entry}

      :not_found ->
        read_main_log_entry_from_disk(ctx, shard_index, index)
    end
  end

  defp ets_log_lookup(table, index) do
    case :ets.info(table) do
      :undefined ->
        :not_found

      _info ->
        case :ets.lookup(table, index) do
          [{^index, entry}] -> {:ok, entry}
          [] -> :not_found
        end
    end
  rescue
    ArgumentError -> :not_found
  end

  defp read_main_log_entry_from_disk(ctx, shard_index, wanted_index) do
    root = storage_root(ctx, shard_index)

    root_chars = to_charlist(root)

    case :ferricstore_waraft_spike_segment_log.location_for_index(root_chars, wanted_index) do
      {:ok, {_ordinal, offset, encoded_size}} ->
        read_main_log_entry_from_disk_at(root_chars, wanted_index, offset, encoded_size)

      :not_found ->
        {:error, :segment_entry_not_found}

      {:error, _reason} = error ->
        error
    end
  end

  defp read_main_log_entry_from_disk_at(root, wanted_index, offset, encoded_size) do
    case :ferricstore_waraft_spike_segment_log.read_disk_at(
           root,
           wanted_index,
           offset,
           encoded_size
         ) do
      {:ok, entry} -> {:ok, entry}
      :not_found -> {:error, :segment_entry_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp read_projection_value_at(ctx, shard_index, index, key)
       when is_integer(index) and index > 0 and is_binary(key) do
    projection_root = Path.join(storage_root(ctx, shard_index), @projection_dir)
    root_chars = to_charlist(projection_root)

    case read_projection_entry_at(root_chars, index) do
      {:ok, entry} ->
        case entry do
          {0, {:ferricstore_segment_projection_entry, ^key, value, _expire_at_ms}}
          when is_binary(value) ->
            {:ok, value}

          {0, {:ferricstore_segment_projection_entry, _other_key, _value, _expire_at_ms}} ->
            :not_found

          _other ->
            {:error, :bad_segment_projection_entry}
        end

      :not_found ->
        :not_found

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_projection_value_at(_ctx, _shard_index, _index, _key),
    do: {:error, :bad_segment_projection_location}

  defp read_projection_entry_at(root_chars, index) do
    case :ferricstore_waraft_spike_segment_log.location_for_index(root_chars, index) do
      {:ok, {_ordinal, offset, encoded_size}} ->
        case :ferricstore_waraft_spike_segment_log.read_disk_at(
               root_chars,
               index,
               offset,
               encoded_size
             ) do
          {:ok, entry} -> {:ok, entry}
          :not_found -> {:error, :projection_entry_missing_at_recorded_location}
          {:error, reason} -> {:error, reason}
        end

      :not_found ->
        :not_found

      {:error, :enoent} ->
        :not_found

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_apply_projection_value_at(ctx, shard_index, index, key)
       when is_integer(index) and index > 0 and is_binary(key) do
    root = storage_root(ctx, shard_index)
    read_mode = {:live, CommandValues.now_ms()}

    case read_apply_projection_cache(root, index, key, read_mode) do
      {:ok, value} ->
        {:ok, value}

      :expired ->
        :not_found

      :not_found ->
        read_apply_projection_value_from_disk(root, index, key, read_mode)
    end
  end

  defp read_apply_projection_value_at(_ctx, _shard_index, _index, _key),
    do: {:error, :bad_segment_apply_projection_location}

  defp read_apply_projection_value_at(ctx, shard_index, index, key, :include_expired)
       when is_integer(index) and index > 0 and is_binary(key) do
    root = storage_root(ctx, shard_index)

    case read_apply_projection_cache(root, index, key, :include_expired) do
      {:ok, value} ->
        {:ok, value}

      :not_found ->
        read_apply_projection_value_from_disk(root, index, key, :include_expired)
    end
  end

  defp read_apply_projection_value_at(_ctx, _shard_index, _index, _key, :include_expired),
    do: {:error, :bad_segment_apply_projection_location}

  defp read_apply_projection_cache(root, index, key, {:live, now_ms}) do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        :not_found

      table ->
        case :ets.lookup(table, {root, index, key}) do
          [{{^root, ^index, ^key}, value, expire_at_ms}] ->
            if CommandValues.live_expire_at?(expire_at_ms, now_ms),
              do: {:ok, value},
              else: :expired

          [] ->
            :not_found
        end
    end
  rescue
    ArgumentError -> :not_found
  end

  defp read_apply_projection_cache(root, index, key, :include_expired) do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        :not_found

      table ->
        case :ets.lookup(table, {root, index, key}) do
          [{{^root, ^index, ^key}, value, _expire_at_ms}] when is_binary(value) -> {:ok, value}
          _ -> :not_found
        end
    end
  rescue
    ArgumentError -> :not_found
  end

  defp read_apply_projection_value_from_disk(root, index, key, read_mode) do
    case read_apply_projection_latest_entries_from_disk(root, index) do
      {:ok, entries} ->
        case value_from_projection_entries(entries, key, read_mode) do
          {:ok, _value} = ok -> ok
          :not_found -> read_apply_projection_value_from_merged_disk(root, index, key, read_mode)
        end

      :not_found ->
        :not_found

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_apply_projection_value_from_merged_disk(root, index, key, read_mode) do
    case read_apply_projection_entries_from_disk(root, index) do
      {:ok, entries} -> value_from_projection_entries(entries, key, read_mode)
      :not_found -> :not_found
      {:error, reason} -> {:error, reason}
    end
  end

  defp read_apply_projection_latest_entries_from_disk(root, index) do
    with_apply_projection_disk_read_lock_root(root, fn ->
      projection_root = Path.join(root, @apply_projection_dir)
      root_chars = to_charlist(projection_root)
      maybe_run_apply_projection_disk_read_hook(root, index, :latest)

      case :ferricstore_waraft_spike_segment_log.location_for_index(root_chars, index) do
        {:ok, {ordinal, offset, encoded_size}} ->
          case read_apply_projection_disk_entry(
                 root,
                 root_chars,
                 index,
                 ordinal,
                 offset,
                 encoded_size
               ) do
            {:ok, entry} -> decode_apply_projection_entry(entry)
            :not_found -> {:error, :apply_projection_entry_missing_at_recorded_location}
            {:error, reason} -> {:error, reason}
          end

        :not_found ->
          :not_found

        {:error, :enoent} ->
          :not_found

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  defp read_apply_projection_disk_entry(
         root,
         root_chars,
         index,
         ordinal,
         offset,
         encoded_size
       ) do
    case DiskReader.read(root, index, {ordinal, offset, encoded_size}) do
      {:error, {:segment_reader_unavailable, _reason}} ->
        :ferricstore_waraft_spike_segment_log.read_disk_at(
          root_chars,
          index,
          offset,
          encoded_size
        )

      result ->
        result
    end
  end

  defp decode_apply_projection_entry(
         {0, {:ferricstore_segment_apply_projection_batch, _position, entries}}
       )
       when is_list(entries),
       do: {:ok, entries}

  defp decode_apply_projection_entry(_entry),
    do: {:error, :bad_segment_apply_projection_entry}

  defp read_apply_projection_entries_from_disk(root, index) do
    with_apply_projection_disk_read_lock_root(root, fn ->
      projection_root = Path.join(root, @apply_projection_dir)
      root_chars = to_charlist(projection_root)
      maybe_run_apply_projection_disk_read_hook(root, index, :merged)

      with {:ok, entry} <-
             :ferricstore_waraft_spike_segment_log.read_disk(root_chars, index) do
        case entry do
          {0, {:ferricstore_segment_apply_projection_batch, _position, entries}}
          when is_list(entries) ->
            {:ok, entries}

          _other ->
            {:error, :bad_segment_apply_projection_entry}
        end
      else
        :not_found -> :not_found
        {:error, :enoent} -> :not_found
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  defp apply_projection_cache_entries_present?(root, index) do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        false

      table ->
        :ets.select_count(table, [
          {{{root, index, :_}, :_, :_}, [], [true]}
        ]) > 0
    end
  rescue
    ArgumentError -> false
  end

  defp maybe_run_apply_projection_disk_read_hook(root, index, source) do
    case Process.get(:ferricstore_waraft_apply_projection_disk_read_hook) do
      fun when is_function(fun, 3) -> fun.(root, index, source)
      _other -> :ok
    end
  end

  defp value_from_projection_entries(entries, key, read_mode) do
    Enum.reduce(entries, :not_found, fn
      {^key, value, expire_at_ms}, _acc when is_binary(value) ->
        if projection_entry_live?(expire_at_ms, read_mode), do: {:ok, value}, else: :not_found

      _entry, acc ->
        acc
    end)
  end

  defp collect_projection_entry_values(entries, keys, acc, read_mode) do
    keyset = MapSet.new(keys)

    Enum.reduce(entries, acc, fn
      {key, value, expire_at_ms}, values
      when is_binary(key) and is_binary(value) ->
        if MapSet.member?(keyset, key) do
          if projection_entry_live?(expire_at_ms, read_mode),
            do: Map.put(values, key, value),
            else: Map.delete(values, key)
        else
          values
        end

      _entry, values ->
        values
    end)
  end

  defp projection_entry_live?(_expire_at_ms, :include_expired), do: true

  defp projection_entry_live?(expire_at_ms, {:live, now_ms}),
    do: CommandValues.live_expire_at?(expire_at_ms, now_ms)

  defp ensure_apply_projection_table! do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        case Ferricstore.Raft.WARaftSegmentReader.TableOwner.ensure_table() do
          :ok ->
            case :ets.whereis(@apply_projection_table) do
              :undefined -> raise "WARaft apply-projection cache table is unavailable"
              table -> table
            end

          {:error, reason} ->
            raise "WARaft apply-projection cache owner is unavailable: #{inspect(reason)}"
        end

      table ->
        table
    end
  end

  defp upsert_apply_projection_entry(table, cache_key, entry) do
    case :ets.lookup(table, cache_key) do
      [] ->
        if :ets.insert_new(table, entry) do
          {:inserted, apply_projection_entry_value_bytes(entry)}
        else
          upsert_apply_projection_entry(table, cache_key, entry)
        end

      [{^cache_key, _previous_value, _previous_expire_at_ms} = previous] ->
        case :ets.select_replace(table, [{previous, [], [{:const, entry}]}]) do
          1 ->
            {:replaced, apply_projection_entry_value_bytes(previous),
             apply_projection_entry_value_bytes(entry)}

          0 ->
            upsert_apply_projection_entry(table, cache_key, entry)
        end
    end
  end

  defp apply_projection_count_key(root), do: {@apply_projection_count_tag, root}
  defp apply_projection_bytes_key(root), do: {@apply_projection_bytes_tag, root}

  defp read_apply_projection_count(table, root) do
    key = apply_projection_count_key(root)

    case :ets.lookup(table, key) do
      [{^key, count}] when is_integer(count) and count >= 0 ->
        count

      _missing_or_stale ->
        with_apply_projection_lock(root, fn ->
          read_or_rebuild_apply_projection_count(table, root)
        end)
    end
  end

  defp read_apply_projection_bytes(table, root) do
    key = apply_projection_bytes_key(root)

    case :ets.lookup(table, key) do
      [{^key, bytes}] when is_integer(bytes) and bytes >= 0 ->
        bytes

      _missing_or_stale ->
        with_apply_projection_lock(root, fn ->
          read_or_rebuild_apply_projection_bytes(table, root)
        end)
    end
  end

  defp read_or_rebuild_apply_projection_count(table, root) do
    key = apply_projection_count_key(root)

    case :ets.lookup(table, key) do
      [{^key, count}] when is_integer(count) and count >= 0 ->
        count

      _missing_or_stale ->
        count = count_apply_projection_rows(table, root)

        maybe_run_apply_projection_cache_mutation_hook(:before_counter_rebuild, %{
          kind: :count,
          root: root,
          value: count
        })

        :ets.insert(table, {key, count})
        count
    end
  end

  defp read_or_rebuild_apply_projection_bytes(table, root) do
    key = apply_projection_bytes_key(root)

    case :ets.lookup(table, key) do
      [{^key, bytes}] when is_integer(bytes) and bytes >= 0 ->
        bytes

      _missing_or_stale ->
        bytes = count_apply_projection_value_bytes(table, root)

        maybe_run_apply_projection_cache_mutation_hook(:before_counter_rebuild, %{
          kind: :bytes,
          root: root,
          value: bytes
        })

        :ets.insert(table, {key, bytes})
        bytes
    end
  end

  defp ensure_apply_projection_counters(table, root) do
    _count = read_or_rebuild_apply_projection_count(table, root)
    _bytes = read_or_rebuild_apply_projection_bytes(table, root)
    :ok
  end

  defp increment_apply_projection_count(_table, _root, 0), do: :ok

  defp increment_apply_projection_count(table, root, count) when count > 0 do
    key = apply_projection_count_key(root)
    :ets.update_counter(table, key, {2, count}, {key, 0})

    :ok
  end

  defp decrement_apply_projection_count(_table, _root, 0), do: :ok

  defp decrement_apply_projection_count(table, root, count) when count > 0 do
    key = apply_projection_count_key(root)
    :ets.update_counter(table, key, {2, -count, 0, 0}, {key, 0})
    :ok
  end

  defp adjust_apply_projection_bytes(_table, _root, 0), do: :ok

  defp adjust_apply_projection_bytes(table, root, delta) when is_integer(delta) do
    key = apply_projection_bytes_key(root)

    if delta > 0 do
      :ets.update_counter(table, key, {2, delta}, {key, 0})
    else
      :ets.update_counter(table, key, {2, delta, 0, 0}, {key, 0})
    end

    :ok
  end

  defp count_apply_projection_rows(table, root) do
    :ets.select_count(table, [
      {{{root, :_, :_}, :_, :_}, [], [true]}
    ])
  end

  defp count_apply_projection_value_bytes(table, root) do
    table
    |> :ets.select([
      {{{root, :_, :_}, :"$1", :_}, [{:is_binary, :"$1"}], [{:byte_size, :"$1"}]}
    ])
    |> Enum.sum()
  end

  defp apply_projection_entry_value_bytes({_cache_key, value, _expire_at_ms})
       when is_binary(value),
       do: byte_size(value)

  defp apply_projection_cache_entries(table, root, :all) do
    :ets.select(table, [
      {{{root, :"$1", :"$2"}, :"$3", :"$4"}, [], [{{:"$1", :"$2", :"$3", :"$4"}}]}
    ])
  end

  defp apply_projection_cache_entries(_table, _root, max_entries)
       when is_integer(max_entries) and max_entries <= 0,
       do: []

  defp apply_projection_cache_entries(table, root, max_entries) when is_integer(max_entries) do
    selected =
      case :ets.select(
             table,
             [
               {{{root, :"$1", :"$2"}, :"$3", :"$4"}, [], [{{:"$1", :"$2", :"$3", :"$4"}}]}
             ],
             max_entries
           ) do
        {entries, _continuation} -> entries
        :"$end_of_table" -> []
      end

    # Keep each Raft index in one spill record. Splitting an index would make
    # misses fall back to a full apply-projection log fold to merge duplicates.
    selected
    |> Enum.map(fn {index, _key, _value, _expire_at_ms} -> index end)
    |> MapSet.new()
    |> Enum.flat_map(fn index ->
      :ets.select(table, [
        {{{root, index, :"$1"}, :"$2", :"$3"}, [], [{{index, :"$1", :"$2", :"$3"}}]}
      ])
    end)
  end

  defp apply_projection_cache_entries_for_limits(_table, _root, 0, 0), do: []

  defp apply_projection_cache_entries_for_limits(table, root, min_entries, min_bytes) do
    # Byte/count targets choose indexes, then spill every row for those indexes
    # so the common latest-record read stays O(1).
    table
    |> select_apply_projection_indexes(root, min_entries, min_bytes)
    |> Enum.flat_map(fn index ->
      :ets.select(table, [
        {{{root, index, :"$1"}, :"$2", :"$3"}, [], [{{index, :"$1", :"$2", :"$3"}}]}
      ])
    end)
  end

  defp apply_projection_cache_entries_for_indexes(table, root, indexes) do
    if MapSet.size(indexes) == 0 do
      []
    else
      Enum.flat_map(indexes, fn index ->
        :ets.select(table, [
          {{{root, index, :"$1"}, :"$2", :"$3"}, [], [{{index, :"$1", :"$2", :"$3"}}]}
        ])
      end)
    end
  end

  defp normalize_apply_projection_refs(refs) do
    Enum.reduce_while(refs, {:ok, MapSet.new()}, fn
      {index, key}, {:ok, acc}
      when is_integer(index) and index > 0 and is_binary(key) and key != "" ->
        {:cont, {:ok, MapSet.put(acc, {index, key})}}

      _invalid, _acc ->
        {:halt, {:error, :invalid_apply_projection_refs}}
    end)
    |> case do
      {:ok, refs} -> {:ok, refs |> MapSet.to_list() |> Enum.sort()}
      {:error, _reason} = error -> error
    end
  end

  defp verify_apply_projection_refs_from_disk(root, refs) do
    refs
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce_while(:ok, fn {index, keys}, :ok ->
      case read_apply_projection_values_from_disk(root, index, keys, %{}, :include_expired) do
        {:ok, values} ->
          case Enum.find(keys, &(not Map.has_key?(values, &1))) do
            nil -> {:cont, :ok}
            key -> {:halt, {:error, {:apply_projection_entry_not_durable, index, key}}}
          end

        {:error, reason} ->
          key = hd(keys)
          {:halt, {:error, {:apply_projection_durability_check_failed, index, key, reason}}}
      end
    end)
  end

  defp select_apply_projection_indexes(table, root, min_entries, min_bytes) do
    match_spec = [
      {{{root, :"$1", :_}, :"$2", :_}, [{:is_binary, :"$2"}], [{{:"$1", {:byte_size, :"$2"}}}]}
    ]

    case :ets.select(table, match_spec, @apply_projection_select_page_size) do
      {rows, continuation} ->
        consume_apply_projection_index_page(
          rows,
          continuation,
          MapSet.new(),
          0,
          0,
          min_entries,
          min_bytes
        )

      :"$end_of_table" ->
        MapSet.new()
    end
  end

  defp consume_apply_projection_index_page(
         rows,
         continuation,
         indexes,
         selected_entries,
         selected_bytes,
         min_entries,
         min_bytes
       ) do
    {indexes, selected_entries, selected_bytes, complete?} =
      Enum.reduce_while(
        rows,
        {indexes, selected_entries, selected_bytes, false},
        fn {index, value_bytes}, {indexes, entry_count, byte_count, _complete?}
           when is_integer(index) and index > 0 and is_integer(value_bytes) and
                  value_bytes >= 0 ->
          indexes = MapSet.put(indexes, index)
          entry_count = entry_count + 1
          byte_count = byte_count + value_bytes
          complete? = entry_count >= min_entries and byte_count >= min_bytes
          result = {indexes, entry_count, byte_count, complete?}

          if complete?, do: {:halt, result}, else: {:cont, result}
        end
      )

    cond do
      complete? ->
        indexes

      continuation == :"$end_of_table" ->
        indexes

      true ->
        case :ets.select(continuation) do
          {next_rows, next_continuation} ->
            consume_apply_projection_index_page(
              next_rows,
              next_continuation,
              indexes,
              selected_entries,
              selected_bytes,
              min_entries,
              min_bytes
            )

          :"$end_of_table" ->
            indexes
        end
    end
  end

  defp spill_apply_projection_groups(groups, data_dir, shard_index, projection_root) do
    {batches, cached_entries} =
      Enum.map_reduce(groups, [], fn {index, entries}, ref_acc ->
        batch =
          Enum.map(entries, fn {_index, key, value, expire_at_ms} ->
            {key, value, expire_at_ms}
          end)

        cached_entries = Enum.reduce(entries, ref_acc, fn entry, acc -> [entry | acc] end)

        {{{:raft_log_pos, index, 0}, batch}, cached_entries}
      end)

    case write_apply_projection_spill(projection_root, batches) do
      :ok ->
        {:ok, delete_spilled_apply_projection_entries(data_dir, shard_index, cached_entries)}

      {:error, _reason} = error ->
        error
    end
  end

  defp delete_spilled_apply_projection_entries(data_dir, shard_index, cached_entries) do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        0

      table ->
        root = storage_root(%{data_dir: data_dir}, shard_index)

        maybe_run_apply_projection_cache_mutation_hook(:before_spill_delete_lock, %{
          root: root,
          cached_entries: length(cached_entries)
        })

        with_apply_projection_lock(root, fn ->
          :ok = ensure_apply_projection_counters(table, root)

          {removed, removed_bytes} =
            Enum.reduce(cached_entries, {0, 0}, fn
              {index, key, value, expire_at_ms}, {removed, removed_bytes}
              when is_integer(index) and index > 0 and is_binary(key) and is_binary(value) and
                     is_integer(expire_at_ms) ->
                cached_entry = {{root, index, key}, value, expire_at_ms}

                case :ets.select_delete(table, [{cached_entry, [], [true]}]) do
                  1 -> {removed + 1, removed_bytes + byte_size(value)}
                  0 -> {removed, removed_bytes}
                end

              _invalid, acc ->
                acc
            end)

          decrement_apply_projection_count(table, root, removed)
          adjust_apply_projection_bytes(table, root, -removed_bytes)
          removed
        end)
    end
  rescue
    ArgumentError -> 0
  end

  defp write_apply_projection_spill(_projection_root, []), do: :ok

  defp write_apply_projection_spill(projection_root, batches) do
    with :ok <- maybe_run_apply_projection_spill_hook(batches) do
      case :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
             to_charlist(projection_root),
             batches
           ) do
        :ok -> :ok
        {:error, reason} -> {:error, {:write_apply_projection_spill_failed, reason}}
        other -> {:error, {:write_apply_projection_spill_failed, other}}
      end
    end
  end

  defp maybe_run_apply_projection_spill_hook(batches) do
    case Application.get_env(:ferricstore, :waraft_apply_projection_spill_hook) do
      fun when is_function(fun, 1) ->
        case fun.(batches) do
          :ok -> :ok
          nil -> :ok
          {:error, _reason} = error -> error
          other -> {:error, {:apply_projection_spill_hook, other}}
        end

      _other ->
        :ok
    end
  end

  defp with_apply_projection_lock(root, fun) when is_binary(root) and is_function(fun, 0) do
    with_apply_projection_named_lock(
      root,
      @apply_projection_lock_tag,
      @held_apply_projection_locks_key,
      fun
    )
  end

  defp with_apply_projection_disk_lock_root(root, fun)
       when is_binary(root) and is_function(fun, 0) do
    if apply_projection_named_lock_held?(@held_apply_projection_disk_read_locks_key, root) and
         not apply_projection_named_lock_held?(@held_apply_projection_disk_locks_key, root) do
      raise ArgumentError, "cannot upgrade an apply-projection disk read latch"
    end

    with_apply_projection_named_lock(
      root,
      @apply_projection_disk_lock_tag,
      @held_apply_projection_disk_locks_key,
      fn ->
        :ok = wait_for_apply_projection_disk_readers(root)
        :ok = DiskReader.invalidate(root)

        try do
          fun.()
        after
          :ok = DiskReader.invalidate(root)
        end
      end
    )
  end

  defp with_apply_projection_disk_read_lock_root(root, fun)
       when is_binary(root) and is_function(fun, 0) do
    if apply_projection_named_lock_held?(@held_apply_projection_disk_locks_key, root) do
      fun.()
    else
      with_apply_projection_disk_read_lock(root, fun)
    end
  end

  defp with_apply_projection_disk_read_lock_root(root, deadline_ms, fun)
       when is_binary(root) and is_integer(deadline_ms) and is_function(fun, 0) do
    if apply_projection_named_lock_held?(@held_apply_projection_disk_locks_key, root) do
      case remaining_waraft_read_timeout(deadline_ms) do
        {:ok, _remaining_ms} -> fun.()
        {:error, _reason} = error -> error
      end
    else
      with_apply_projection_disk_read_lock(root, deadline_ms, fun)
    end
  end

  defp with_apply_projection_disk_read_lock(root, fun) do
    held = Process.get(@held_apply_projection_disk_read_locks_key, %{})

    case Map.get(held, root) do
      nil ->
        :ok = acquire_apply_projection_disk_read_lock(root)
        Process.put(@held_apply_projection_disk_read_locks_key, Map.put(held, root, 1))

        try do
          fun.()
        after
          release_apply_projection_disk_read_lock(root)
        end

      count when is_integer(count) and count > 0 ->
        Process.put(
          @held_apply_projection_disk_read_locks_key,
          Map.put(held, root, count + 1)
        )

        try do
          fun.()
        after
          release_apply_projection_disk_read_lock(root)
        end
    end
  end

  defp with_apply_projection_disk_read_lock(root, deadline_ms, fun) do
    held = Process.get(@held_apply_projection_disk_read_locks_key, %{})

    case Map.get(held, root) do
      nil ->
        case acquire_apply_projection_disk_read_lock(root, deadline_ms) do
          :ok ->
            Process.put(@held_apply_projection_disk_read_locks_key, Map.put(held, root, 1))

            try do
              fun.()
            after
              release_apply_projection_disk_read_lock(root)
            end

          {:error, _reason} = error ->
            error
        end

      count when is_integer(count) and count > 0 ->
        case remaining_waraft_read_timeout(deadline_ms) do
          {:ok, _remaining_ms} ->
            Process.put(
              @held_apply_projection_disk_read_locks_key,
              Map.put(held, root, count + 1)
            )

            try do
              fun.()
            after
              release_apply_projection_disk_read_lock(root)
            end

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp with_apply_projection_named_lock(root, lock_tag, held_locks_key, fun) do
    held = Process.get(held_locks_key, %{})

    case Map.get(held, root) do
      nil ->
        :ok = acquire_apply_projection_lock(root, lock_tag)
        Process.put(held_locks_key, Map.put(held, root, 1))

        try do
          fun.()
        after
          release_apply_projection_lock(root, lock_tag, held_locks_key)
        end

      count when is_integer(count) and count > 0 ->
        Process.put(held_locks_key, Map.put(held, root, count + 1))

        try do
          fun.()
        after
          release_apply_projection_lock(root, lock_tag, held_locks_key)
        end
    end
  end

  defp acquire_apply_projection_lock(root, lock_tag),
    do: acquire_apply_projection_lock(root, lock_tag, @apply_projection_lock_retry_min_ms)

  defp acquire_apply_projection_lock(root, lock_tag, wait_ms) do
    table = ensure_apply_projection_table!()
    key = apply_projection_lock_key(lock_tag, root)

    case :ets.insert_new(table, {key, self()}) do
      true ->
        :ok

      false ->
        wait_for_apply_projection_lock(table, key, root, lock_tag, wait_ms)
    end
  rescue
    ArgumentError ->
      apply_projection_lock_backoff(wait_ms)

      acquire_apply_projection_lock(
        root,
        lock_tag,
        next_apply_projection_lock_backoff(wait_ms)
      )
  end

  defp wait_for_apply_projection_lock(table, key, root, lock_tag, wait_ms) do
    next_wait_ms =
      case :ets.lookup(table, key) do
        [{^key, holder}] when is_pid(holder) ->
          if Process.alive?(holder) do
            apply_projection_lock_backoff(wait_ms)
            next_apply_projection_lock_backoff(wait_ms)
          else
            :ets.select_delete(table, [{{key, holder}, [], [true]}])
            @apply_projection_lock_retry_min_ms
          end

        _missing_or_invalid ->
          :ets.delete(table, key)
          @apply_projection_lock_retry_min_ms
      end

    acquire_apply_projection_lock(root, lock_tag, next_wait_ms)
  rescue
    ArgumentError ->
      apply_projection_lock_backoff(wait_ms)

      acquire_apply_projection_lock(
        root,
        lock_tag,
        next_apply_projection_lock_backoff(wait_ms)
      )
  end

  defp acquire_apply_projection_disk_read_lock(root),
    do:
      acquire_apply_projection_disk_read_lock(
        root,
        :infinity,
        @apply_projection_lock_retry_min_ms
      )

  defp acquire_apply_projection_disk_read_lock(root, deadline_ms)
       when is_integer(deadline_ms),
       do:
         acquire_apply_projection_disk_read_lock(
           root,
           deadline_ms,
           @apply_projection_lock_retry_min_ms
         )

  defp acquire_apply_projection_disk_read_lock(root, deadline_ms, wait_ms) do
    with :ok <- validate_apply_projection_read_deadline(deadline_ms) do
      table = ensure_apply_projection_table!()
      writer_key = apply_projection_lock_key(@apply_projection_disk_lock_tag, root)
      reader_key = apply_projection_disk_reader_key(root, self())

      case :ets.lookup(table, writer_key) do
        [] ->
          acquire_apply_projection_disk_read_lock_without_writer(
            table,
            writer_key,
            reader_key,
            root,
            deadline_ms,
            wait_ms
          )

        [{^writer_key, holder}] when is_pid(holder) ->
          if Process.alive?(holder) do
            with :ok <- apply_projection_read_lock_backoff(deadline_ms, wait_ms) do
              acquire_apply_projection_disk_read_lock(
                root,
                deadline_ms,
                next_apply_projection_lock_backoff(wait_ms)
              )
            end
          else
            :ets.select_delete(table, [{{writer_key, holder}, [], [true]}])
            acquire_apply_projection_disk_read_lock(root, deadline_ms, wait_ms)
          end

        _missing_or_invalid ->
          :ets.delete(table, writer_key)
          acquire_apply_projection_disk_read_lock(root, deadline_ms, wait_ms)
      end
    end
  rescue
    ArgumentError ->
      with :ok <- apply_projection_read_lock_backoff(deadline_ms, wait_ms) do
        acquire_apply_projection_disk_read_lock(
          root,
          deadline_ms,
          next_apply_projection_lock_backoff(wait_ms)
        )
      end
  end

  defp acquire_apply_projection_disk_read_lock_without_writer(
         table,
         writer_key,
         reader_key,
         root,
         deadline_ms,
         wait_ms
       ) do
    case :ets.insert_new(table, {reader_key, self()}) do
      true ->
        case :ets.lookup(table, writer_key) do
          [] ->
            :ok

          [{^writer_key, holder}] when is_pid(holder) ->
            :ets.select_delete(table, [{{reader_key, self()}, [], [true]}])

            if Process.alive?(holder) do
              with :ok <- apply_projection_read_lock_backoff(deadline_ms, wait_ms) do
                acquire_apply_projection_disk_read_lock(
                  root,
                  deadline_ms,
                  next_apply_projection_lock_backoff(wait_ms)
                )
              end
            else
              :ets.select_delete(table, [{{writer_key, holder}, [], [true]}])
              acquire_apply_projection_disk_read_lock(root, deadline_ms, wait_ms)
            end

          _missing_or_invalid ->
            :ets.select_delete(table, [{{reader_key, self()}, [], [true]}])
            :ets.delete(table, writer_key)
            acquire_apply_projection_disk_read_lock(root, deadline_ms, wait_ms)
        end

      false ->
        :ets.select_delete(table, [{{reader_key, self()}, [], [true]}])
        acquire_apply_projection_disk_read_lock(root, deadline_ms, wait_ms)
    end
  end

  defp wait_for_apply_projection_disk_readers(root),
    do: wait_for_apply_projection_disk_readers(root, @apply_projection_lock_retry_min_ms)

  defp wait_for_apply_projection_disk_readers(root, wait_ms) do
    table = ensure_apply_projection_table!()

    live_reader? =
      table
      |> :ets.select([
        {{{@apply_projection_disk_reader_tag, root, :"$1"}, :"$2"}, [], [{{:"$1", :"$2"}}]}
      ])
      |> Enum.reduce(false, fn {reader_pid, holder}, live_reader? ->
        if is_pid(reader_pid) and holder == reader_pid and Process.alive?(reader_pid) do
          true
        else
          reader_key = apply_projection_disk_reader_key(root, reader_pid)
          :ets.select_delete(table, [{{reader_key, holder}, [], [true]}])
          live_reader?
        end
      end)

    if live_reader? do
      apply_projection_lock_backoff(wait_ms)

      wait_for_apply_projection_disk_readers(
        root,
        next_apply_projection_lock_backoff(wait_ms)
      )
    else
      :ok
    end
  rescue
    ArgumentError ->
      apply_projection_lock_backoff(wait_ms)

      wait_for_apply_projection_disk_readers(
        root,
        next_apply_projection_lock_backoff(wait_ms)
      )
  end

  defp release_apply_projection_lock(root, lock_tag, held_locks_key) do
    held = Process.get(held_locks_key, %{})

    case Map.get(held, root) do
      count when is_integer(count) and count > 1 ->
        Process.put(held_locks_key, Map.put(held, root, count - 1))

      1 ->
        next = Map.delete(held, root)

        if map_size(next) == 0 do
          Process.delete(held_locks_key)
        else
          Process.put(held_locks_key, next)
        end

        table = ensure_apply_projection_table!()
        key = apply_projection_lock_key(lock_tag, root)
        :ets.select_delete(table, [{{key, self()}, [], [true]}])

      _not_held ->
        :ok
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp release_apply_projection_disk_read_lock(root) do
    held = Process.get(@held_apply_projection_disk_read_locks_key, %{})

    case Map.get(held, root) do
      count when is_integer(count) and count > 1 ->
        Process.put(
          @held_apply_projection_disk_read_locks_key,
          Map.put(held, root, count - 1)
        )

      1 ->
        next = Map.delete(held, root)

        if map_size(next) == 0 do
          Process.delete(@held_apply_projection_disk_read_locks_key)
        else
          Process.put(@held_apply_projection_disk_read_locks_key, next)
        end

        table = ensure_apply_projection_table!()
        reader_key = apply_projection_disk_reader_key(root, self())
        :ets.select_delete(table, [{{reader_key, self()}, [], [true]}])

      _not_held ->
        :ok
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp apply_projection_named_lock_held?(held_locks_key, root) do
    case Process.get(held_locks_key, %{}) do
      %{^root => count} when is_integer(count) and count > 0 -> true
      _not_held -> false
    end
  end

  defp apply_projection_lock_key(lock_tag, root), do: {lock_tag, root}

  defp apply_projection_disk_reader_key(root, reader_pid),
    do: {@apply_projection_disk_reader_tag, root, reader_pid}

  defp apply_projection_lock_backoff(wait_ms) do
    maybe_run_apply_projection_lock_backoff_hook(wait_ms)

    receive do
    after
      wait_ms -> :ok
    end
  end

  defp validate_apply_projection_read_deadline(:infinity), do: :ok

  defp validate_apply_projection_read_deadline(deadline_ms) when is_integer(deadline_ms) do
    case remaining_waraft_read_timeout(deadline_ms) do
      {:ok, _remaining_ms} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp apply_projection_read_lock_backoff(:infinity, wait_ms),
    do: apply_projection_lock_backoff(wait_ms)

  defp apply_projection_read_lock_backoff(deadline_ms, wait_ms)
       when is_integer(deadline_ms) do
    case remaining_waraft_read_timeout(deadline_ms) do
      {:ok, remaining_ms} ->
        bounded_wait_ms = min(wait_ms, remaining_ms)
        maybe_run_apply_projection_lock_backoff_hook(bounded_wait_ms)

        receive do
        after
          bounded_wait_ms -> :ok
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp next_apply_projection_lock_backoff(wait_ms),
    do: min(wait_ms * 2, @apply_projection_lock_retry_max_ms)

  if Mix.env() == :test do
    defp maybe_run_apply_projection_cache_mutation_hook(phase, metadata) do
      case Application.get_env(:ferricstore, :waraft_apply_projection_cache_mutation_hook) do
        hook when is_function(hook, 2) -> hook.(phase, metadata)
        _other -> :ok
      end
    end

    defp maybe_run_apply_projection_lock_backoff_hook(wait_ms) do
      case Process.get(:ferricstore_waraft_apply_projection_lock_backoff_hook) do
        hook when is_function(hook, 1) -> hook.(wait_ms)
        _other -> :ok
      end
    end
  else
    defp maybe_run_apply_projection_cache_mutation_hook(_phase, _metadata), do: :ok
    defp maybe_run_apply_projection_lock_backoff_hook(_wait_ms), do: :ok
  end

  defp storage_root(%{data_dir: data_dir}, shard_index) do
    Path.join([data_dir, "waraft", "#{@storage_root}.#{shard_index + 1}"])
  end

  defp log_table(shard_index) do
    String.to_existing_atom("#{@table_prefix}#{shard_index + 1}")
  rescue
    ArgumentError -> nil
  end
end
