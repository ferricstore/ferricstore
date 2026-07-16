defmodule Ferricstore.Raft.WARaftSegmentReader do
  alias Ferricstore.Raft.WARaftSegmentReader.CommandValues
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
    read_mode = {:live, CommandValues.now_ms()}

    {found, missing} =
      keys
      |> Enum.uniq()
      |> Enum.reduce({%{}, []}, fn key, {found, missing} ->
        case read_apply_projection_cache(root, index, key, read_mode) do
          {:ok, value} -> {Map.put(found, key, value), missing}
          :not_found -> {found, [key | missing]}
        end
      end)

    case missing do
      [] ->
        {:ok, found}

      [_ | _] ->
        case read_apply_projection_latest_entries_from_disk(root, index) do
          {:ok, entries} ->
            found = collect_projection_entry_values(entries, missing, found, read_mode)
            still_missing = reject_found_keys(missing, found)

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

  defp waraft_read_expired?(deadline_ms),
    do: System.monotonic_time(:millisecond) >= deadline_ms

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
              else: :not_found

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
        {:ok, {_ordinal, offset, encoded_size}} ->
          case :ferricstore_waraft_spike_segment_log.read_disk_at(
                 root_chars,
                 index,
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
        fun.()
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
    do: acquire_apply_projection_disk_read_lock(root, @apply_projection_lock_retry_min_ms)

  defp acquire_apply_projection_disk_read_lock(root, wait_ms) do
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
          wait_ms
        )

      [{^writer_key, holder}] when is_pid(holder) ->
        if Process.alive?(holder) do
          apply_projection_lock_backoff(wait_ms)

          acquire_apply_projection_disk_read_lock(
            root,
            next_apply_projection_lock_backoff(wait_ms)
          )
        else
          :ets.select_delete(table, [{{writer_key, holder}, [], [true]}])
          acquire_apply_projection_disk_read_lock(root)
        end

      _missing_or_invalid ->
        :ets.delete(table, writer_key)
        acquire_apply_projection_disk_read_lock(root)
    end
  rescue
    ArgumentError ->
      apply_projection_lock_backoff(wait_ms)

      acquire_apply_projection_disk_read_lock(
        root,
        next_apply_projection_lock_backoff(wait_ms)
      )
  end

  defp acquire_apply_projection_disk_read_lock_without_writer(
         table,
         writer_key,
         reader_key,
         root,
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
              apply_projection_lock_backoff(wait_ms)

              acquire_apply_projection_disk_read_lock(
                root,
                next_apply_projection_lock_backoff(wait_ms)
              )
            else
              :ets.select_delete(table, [{{writer_key, holder}, [], [true]}])
              acquire_apply_projection_disk_read_lock(root)
            end

          _missing_or_invalid ->
            :ets.select_delete(table, [{{reader_key, self()}, [], [true]}])
            :ets.delete(table, writer_key)
            acquire_apply_projection_disk_read_lock(root)
        end

      false ->
        :ets.select_delete(table, [{{reader_key, self()}, [], [true]}])
        acquire_apply_projection_disk_read_lock(root)
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
