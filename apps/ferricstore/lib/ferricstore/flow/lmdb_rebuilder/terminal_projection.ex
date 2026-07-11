defmodule Ferricstore.Flow.LMDBRebuilder.TerminalProjection do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.LMDB

  def persist_terminal_counts(%{terminal_counts: counts} = stats, lmdb_path) do
    if map_size(counts) == 0 and not Ferricstore.FS.dir?(lmdb_path) do
      stats
    else
      do_persist_terminal_counts(stats, counts, lmdb_path)
    end
  end

  def cleanup_stale_terminal_ops(lmdb_path, state_key, record) do
    reverse_key = LMDB.terminal_by_state_key_key(state_key)

    reverse_ops =
      case LMDB.get(lmdb_path, reverse_key) do
        {:ok, terminal_key} when is_binary(terminal_key) ->
          [{:delete, reverse_key}, {:delete, terminal_key}]

        _ ->
          []
      end

    reverse_ops ++ cleanup_stale_terminal_ops_by_id(lmdb_path, state_key, record)
  end

  def cleanup_stale_terminal_reverse_ops(lmdb_path, keydir, decode_entry_fun)
      when is_function(decode_entry_fun, 1) do
    case LMDB.prefix_entries(
           lmdb_path,
           LMDB.terminal_by_state_global_prefix(),
           terminal_projection_scan_limit()
         ) do
      {:ok, entries} ->
        Enum.flat_map(entries, fn {reverse_key, terminal_key} ->
          with {:ok, state_key} <- terminal_state_key_from_reverse_key(reverse_key),
               false <- terminal_state_key?(keydir, state_key, decode_entry_fun),
               true <- is_binary(terminal_key) do
            [
              {:delete, reverse_key}
              | LMDB.terminal_index_delete_ops(lmdb_path, terminal_key, nil)
            ]
          else
            _ -> []
          end
        end)

      _ ->
        []
    end
  end

  def query_metadata_index_ops(record, expire_at_ms) do
    partition_key = Map.get(record, :partition_key)
    score = Map.get(record, :updated_at_ms, 0)

    metadata_ops =
      metadata_index_entries(record)
      |> Enum.map(fn {kind, value} ->
        key =
          case kind do
            :parent -> Flow.Keys.parent_index_key(value, partition_key)
            :root -> Flow.Keys.root_index_key(value, partition_key)
            :correlation -> Flow.Keys.correlation_index_key(value, partition_key)
          end

        query_key = LMDB.query_index_key(key, record.id, score)
        state_key = Flow.Keys.state_key(record.id, partition_key)
        value = LMDB.encode_query_index_value(record.id, score, expire_at_ms, state_key)
        {:put, query_key, value}
      end)

    attribute_ops =
      (Ferricstore.Flow.Attributes.index_entries(record) ++
         Ferricstore.Flow.StateMeta.index_entries(record))
      |> Enum.map(fn {key, _id, _score} ->
        query_key = LMDB.query_index_key(key, record.id, score)
        state_key = Flow.Keys.state_key(record.id, partition_key)
        value = LMDB.encode_query_index_value(record.id, score, expire_at_ms, state_key)
        {:put, query_key, value}
      end)

    metadata_ops ++ attribute_ops
  end

  defp do_persist_terminal_counts(stats, counts, lmdb_path) do
    count_keys =
      lmdb_path
      |> existing_terminal_count_keys()
      |> MapSet.union(MapSet.new(Map.keys(counts)))

    ops =
      Enum.map(count_keys, fn count_key ->
        {:put, count_key, LMDB.encode_count(Map.get(counts, count_key, 0))}
      end)

    case LMDB.write_batch(lmdb_path, ops) do
      :ok ->
        Enum.each(count_keys, fn count_key ->
          LMDB.put_cached_terminal_count_key(lmdb_path, count_key, Map.get(counts, count_key, 0))
        end)

        stats

      {:error, _reason} ->
        %{stats | lmdb_errors: stats.lmdb_errors + 1}
    end
  end

  defp existing_terminal_count_keys(lmdb_path) do
    limit = Application.get_env(:ferricstore, :flow_lmdb_rebuild_count_key_scan_limit, 1_000_000)

    case LMDB.prefix_entries(lmdb_path, LMDB.terminal_count_prefix(), limit) do
      {:ok, entries} -> MapSet.new(entries, fn {key, _value} -> key end)
      {:error, _reason} -> MapSet.new()
    end
  end

  defp cleanup_stale_terminal_ops_by_id(lmdb_path, state_key, %{id: id, type: type} = record)
       when is_binary(id) and is_binary(type) do
    partition_key = Map.get(record, :partition_key)

    specific_ops =
      ["completed", "failed", "cancelled"]
      |> Enum.flat_map(fn terminal_state ->
        index_key = Flow.Keys.state_index_key(type, terminal_state, partition_key)

        cleanup_stale_terminal_ops_under_prefix(
          lmdb_path,
          LMDB.terminal_index_prefix(index_key),
          id,
          state_key,
          false
        )
      end)

    if specific_ops == [] do
      cleanup_stale_terminal_ops_under_prefix(
        lmdb_path,
        LMDB.terminal_index_global_prefix(),
        id,
        state_key,
        true
      )
    else
      specific_ops
    end
  end

  defp cleanup_stale_terminal_ops_by_id(_lmdb_path, _state_key, _record), do: []

  defp terminal_state_key_from_reverse_key(<<"flow-terminal-by-state:", state_key::binary>>)
       when byte_size(state_key) > 0,
       do: {:ok, state_key}

  defp terminal_state_key_from_reverse_key(_reverse_key), do: :error

  defp terminal_state_key?(keydir, state_key, decode_entry_fun) when is_binary(state_key) do
    case :ets.lookup(keydir, state_key) do
      [entry] ->
        case decode_entry_fun.(entry) do
          [{_key, _value, _expire_at_ms, record}] ->
            LMDB.terminal_state?(Map.get(record, :state))

          _ ->
            false
        end

      _ ->
        false
    end
  end

  defp cleanup_stale_terminal_ops_under_prefix(
         lmdb_path,
         prefix,
         id,
         current_state_key,
         nil_state_key_only?
       ) do
    case LMDB.prefix_entries(lmdb_path, prefix, terminal_projection_scan_limit()) do
      {:ok, entries} ->
        Enum.flat_map(entries, fn {terminal_key, value} ->
          case LMDB.decode_terminal_index_value(value) do
            {:ok, {^id, _updated_at_ms, _expire_at_ms, nil}} ->
              LMDB.terminal_index_delete_ops(lmdb_path, terminal_key, nil)

            {:ok, {^id, _updated_at_ms, _expire_at_ms, ^current_state_key}}
            when is_binary(current_state_key) and not nil_state_key_only? ->
              LMDB.terminal_index_delete_ops(lmdb_path, terminal_key, current_state_key)

            _ ->
              []
          end
        end)

      _ ->
        []
    end
  end

  defp metadata_index_entries(record) do
    [
      {:parent, Map.get(record, :parent_flow_id)},
      {:root, non_default_root_flow_id(record)},
      {:correlation, Map.get(record, :correlation_id)}
    ]
    |> Enum.filter(fn {_kind, value} -> is_binary(value) and value != "" end)
  end

  defp non_default_root_flow_id(record) do
    id = Map.get(record, :id)

    case Map.get(record, :root_flow_id) do
      root_flow_id when root_flow_id in [nil, "", id] -> nil
      root_flow_id -> root_flow_id
    end
  end

  defp terminal_projection_scan_limit do
    Application.get_env(:ferricstore, :flow_lmdb_terminal_rebuild_scan_limit, 1_000_000)
  end
end
