defmodule Ferricstore.Raft.WARaftSegmentReader do
  alias Ferricstore.Raft.WARaftSegmentReader.CommandValues
  @moduledoc false

  @table_prefix "raft_log_ferricstore_waraft_backend_"
  @storage_root "ferricstore_waraft_backend"
  @projection_dir "segment_projection_log"
  @apply_projection_dir "apply_projection_log"
  @apply_projection_table :ferricstore_waraft_apply_projection_cache
  @apply_projection_count_tag :apply_projection_count

  @spec put_apply_projection(binary(), non_neg_integer(), pos_integer(), [
          {binary(), binary(), non_neg_integer()}
        ]) ::
          :ok
  def put_apply_projection(data_dir, shard_index, index, entries)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(index) and index > 0 and is_list(entries) do
    table = ensure_apply_projection_table!()
    root = storage_root(%{data_dir: data_dir}, shard_index)

    inserted =
      Enum.reduce(entries, 0, fn
        {key, value, expire_at_ms}, acc
        when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) ->
          cache_key = {root, index, key}
          entry = {cache_key, value, expire_at_ms}

          if :ets.insert_new(table, entry) do
            acc + 1
          else
            :ets.insert(table, entry)
            acc
          end

        _invalid, acc ->
          acc
      end)

    increment_apply_projection_count(table, root, inserted)

    :ok
  end

  @spec apply_projection_cache_count(binary(), non_neg_integer()) :: non_neg_integer()
  def apply_projection_cache_count(data_dir, shard_index)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        0

      table ->
        root = storage_root(%{data_dir: data_dir}, shard_index)

        case :ets.lookup(table, apply_projection_count_key(root)) do
          [{_key, count}] when is_integer(count) and count >= 0 ->
            count

          _missing_or_stale ->
            count_apply_projection_rows(table, root)
        end
    end
  rescue
    ArgumentError -> 0
  end

  def apply_projection_cache_count(_data_dir, _shard_index), do: 0

  @spec apply_projection_dependency_ready?(binary(), non_neg_integer(), pos_integer()) ::
          boolean()
  def apply_projection_dependency_ready?(data_dir, shard_index, index)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(index) and index > 0 do
    root = storage_root(%{data_dir: data_dir}, shard_index)

    apply_projection_index_on_disk?(root, index) or
      not apply_projection_cache_entries_present?(root, index)
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
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        {:ok, 0}

      table ->
        root = storage_root(%{data_dir: data_dir}, shard_index)
        projection_root = Path.join(root, @apply_projection_dir)

        table
        |> apply_projection_cache_entries(root, max_entries)
        |> Enum.group_by(fn {index, _key, _value, _expire_at_ms} -> index end)
        |> Enum.sort_by(fn {index, _entries} -> index end)
        |> spill_apply_projection_groups(data_dir, shard_index, projection_root)
    end
  rescue
    error -> {:error, {:spill_apply_projection_cache_failed, error}}
  end

  def spill_apply_projection_cache(_data_dir, _shard_index, _max_entries), do: {:ok, 0}

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

        removed =
          Enum.reduce(refs, 0, fn
            {index, key}, acc when is_integer(index) and index > 0 and is_binary(key) ->
              case :ets.take(table, {root, index, key}) do
                [] -> acc
                [_entry] -> acc + 1
              end

            _invalid, acc ->
              acc
          end)

        decrement_apply_projection_count(table, root, removed)
        removed
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

        removed =
          :ets.select_delete(table, [
            {{{root, :_, :_}, :_, :_}, [], [true]}
          ])

        :ets.delete(table, apply_projection_count_key(root))
        removed
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

    with {:ok, {_ordinal, offset, encoded_size}} <-
           :ferricstore_waraft_spike_segment_log.location_for_index(root_chars, index),
         {:ok, entry} <-
           :ferricstore_waraft_spike_segment_log.read_disk_at(
             root_chars,
             index,
             offset,
             encoded_size
           ) do
      case entry do
        {0, {:ferricstore_segment_projection_entry, key, value, _expire_at_ms}}
        when is_binary(key) and is_binary(value) ->
          if MapSet.member?(keyset, key), do: {:ok, %{key => value}}, else: {:ok, %{}}

        _other ->
          {:error, :bad_segment_projection_entry}
      end
    else
      :not_found -> {:ok, %{}}
      {:error, :enoent} -> {:ok, %{}}
      {:error, reason} -> {:error, reason}
    end
  end

  def read_values_from_location(ctx, shard_index, {:waraft_apply_projection, index}, keys)
      when is_integer(index) and index > 0 and is_list(keys) do
    root = storage_root(ctx, shard_index)

    {found, missing} =
      keys
      |> Enum.uniq()
      |> Enum.reduce({%{}, []}, fn key, {found, missing} ->
        case read_apply_projection_cache(root, index, key) do
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
            found = collect_projection_entry_values(entries, missing, found)
            still_missing = reject_found_keys(missing, found)

            if still_missing == [] do
              {:ok, found}
            else
              read_apply_projection_missing_from_merged_disk(root, index, still_missing, found)
            end

          :not_found ->
            {:ok, found}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  def read_values_from_location(ctx, shard_index, file_id, keys) when is_list(keys) do
    values =
      Enum.reduce(keys, %{}, fn key, acc ->
        case read_value_from_location(ctx, shard_index, file_id, key) do
          {:ok, value} -> Map.put(acc, key, value)
          _missing_or_error -> acc
        end
      end)

    {:ok, values}
  end

  defp read_apply_projection_missing_from_merged_disk(root, index, missing, found) do
    case read_apply_projection_entries_from_disk(root, index) do
      {:ok, entries} ->
        {:ok, collect_projection_entry_values(entries, missing, found)}

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

    with {:ok, {_ordinal, offset, encoded_size}} <-
           :ferricstore_waraft_spike_segment_log.location_for_index(root_chars, index),
         {:ok, entry} <-
           :ferricstore_waraft_spike_segment_log.read_disk_at(
             root_chars,
             index,
             offset,
             encoded_size
           ) do
      case entry do
        {0, {:ferricstore_segment_projection_entry, ^key, value, _expire_at_ms}}
        when is_binary(value) ->
          {:ok, value}

        {0, {:ferricstore_segment_projection_entry, _other_key, _value, _expire_at_ms}} ->
          :not_found

        _other ->
          {:error, :bad_segment_projection_entry}
      end
    else
      :not_found -> :not_found
      {:error, :enoent} -> :not_found
      {:error, reason} -> {:error, reason}
    end
  end

  defp read_projection_value_at(_ctx, _shard_index, _index, _key),
    do: {:error, :bad_segment_projection_location}

  defp read_apply_projection_value_at(ctx, shard_index, index, key)
       when is_integer(index) and index > 0 and is_binary(key) do
    root = storage_root(ctx, shard_index)

    case read_apply_projection_cache(root, index, key) do
      {:ok, value} ->
        {:ok, value}

      :not_found ->
        read_apply_projection_value_from_disk(root, index, key)
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
        read_apply_projection_value_from_disk(root, index, key)
    end
  end

  defp read_apply_projection_value_at(_ctx, _shard_index, _index, _key, :include_expired),
    do: {:error, :bad_segment_apply_projection_location}

  defp read_apply_projection_cache(root, index, key) do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        :not_found

      table ->
        case :ets.lookup(table, {root, index, key}) do
          [{{^root, ^index, ^key}, value, expire_at_ms}] ->
            if CommandValues.live_expire_at?(expire_at_ms), do: {:ok, value}, else: :not_found

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

  defp read_apply_projection_value_from_disk(root, index, key) do
    case read_apply_projection_latest_entries_from_disk(root, index) do
      {:ok, entries} ->
        case value_from_projection_entries(entries, key) do
          {:ok, _value} = ok -> ok
          :not_found -> read_apply_projection_value_from_merged_disk(root, index, key)
        end

      :not_found ->
        :not_found

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_apply_projection_value_from_merged_disk(root, index, key) do
    case read_apply_projection_entries_from_disk(root, index) do
      {:ok, entries} -> value_from_projection_entries(entries, key)
      :not_found -> :not_found
      {:error, reason} -> {:error, reason}
    end
  end

  defp read_apply_projection_latest_entries_from_disk(root, index) do
    projection_root = Path.join(root, @apply_projection_dir)
    root_chars = to_charlist(projection_root)
    maybe_run_apply_projection_disk_read_hook(root, index, :latest)

    with {:ok, {_ordinal, offset, encoded_size}} <-
           :ferricstore_waraft_spike_segment_log.location_for_index(root_chars, index),
         {:ok, entry} <-
           :ferricstore_waraft_spike_segment_log.read_disk_at(
             root_chars,
             index,
             offset,
             encoded_size
           ) do
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
  end

  defp read_apply_projection_entries_from_disk(root, index) do
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
  end

  defp apply_projection_index_on_disk?(root, index) do
    case read_apply_projection_latest_entries_from_disk(root, index) do
      {:ok, _entries} -> true
      _missing_or_error -> false
    end
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

  defp value_from_projection_entries(entries, key) do
    Enum.reduce(entries, :not_found, fn
      {^key, value, _expire_at_ms}, _acc when is_binary(value) -> {:ok, value}
      _entry, acc -> acc
    end)
  end

  defp collect_projection_entry_values(entries, keys, acc) do
    keyset = MapSet.new(keys)

    Enum.reduce(entries, acc, fn
      {key, value, _expire_at_ms}, values
      when is_binary(key) and is_binary(value) ->
        if MapSet.member?(keyset, key), do: Map.put(values, key, value), else: values

      _entry, values ->
        values
    end)
  end

  defp ensure_apply_projection_table! do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        try do
          :ets.new(@apply_projection_table, [
            :set,
            :public,
            :named_table,
            {:read_concurrency, true},
            {:write_concurrency, true}
          ])
        rescue
          ArgumentError -> @apply_projection_table
        end

      table ->
        table
    end
  end

  defp apply_projection_count_key(root), do: {@apply_projection_count_tag, root}

  defp increment_apply_projection_count(_table, _root, 0), do: :ok

  defp increment_apply_projection_count(table, root, count) when count > 0 do
    :ets.update_counter(
      table,
      apply_projection_count_key(root),
      {2, count},
      {apply_projection_count_key(root), 0}
    )

    :ok
  end

  defp decrement_apply_projection_count(_table, _root, 0), do: :ok

  defp decrement_apply_projection_count(table, root, count) when count > 0 do
    key = apply_projection_count_key(root)

    current =
      case :ets.lookup(table, key) do
        [{^key, value}] when is_integer(value) and value > 0 -> value
        _missing_or_stale -> count_apply_projection_rows(table, root) + count
      end

    :ets.insert(table, {key, max(current - count, 0)})
    :ok
  end

  defp count_apply_projection_rows(table, root) do
    :ets.select_count(table, [
      {{{root, :_, :_}, :_, :_}, [], [true]}
    ])
  end

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

    selected
    |> Enum.map(fn {index, _key, _value, _expire_at_ms} -> index end)
    |> MapSet.new()
    |> Enum.flat_map(fn index ->
      :ets.select(table, [
        {{{root, index, :"$1"}, :"$2", :"$3"}, [], [{{index, :"$1", :"$2", :"$3"}}]}
      ])
    end)
  end

  defp spill_apply_projection_groups(groups, data_dir, shard_index, projection_root) do
    {batches, refs} =
      Enum.map_reduce(groups, [], fn {index, entries}, ref_acc ->
        batch =
          Enum.map(entries, fn {_index, key, value, expire_at_ms} ->
            {key, value, expire_at_ms}
          end)

        refs =
          Enum.reduce(entries, ref_acc, fn {_index, key, _value, _expire_at_ms}, acc ->
            [{index, key} | acc]
          end)

        {{{:raft_log_pos, index, 0}, batch}, refs}
      end)

    case write_apply_projection_spill(projection_root, batches) do
      :ok -> {:ok, delete_apply_projection_entries(data_dir, shard_index, refs)}
      {:error, _reason} = error -> error
    end
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

  defp storage_root(%{data_dir: data_dir}, shard_index) do
    Path.join([data_dir, "waraft", "#{@storage_root}.#{shard_index + 1}"])
  end

  defp log_table(shard_index), do: String.to_atom("#{@table_prefix}#{shard_index + 1}")
end
