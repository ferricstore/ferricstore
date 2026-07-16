defmodule Ferricstore.Flow.LMDBRebuilder.TerminalProjection do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.LMDB

  @default_scan_page_size 4_096
  @max_scan_page_size 65_536

  def persist_terminal_counts(%{terminal_counts: counts} = stats, lmdb_path) do
    if map_size(counts) == 0 and not Ferricstore.FS.dir?(lmdb_path) do
      stats
    else
      do_persist_terminal_counts(stats, counts, lmdb_path)
    end
  end

  def cleanup_stale_terminal_ops(lmdb_path, state_key, record) do
    reverse_key = LMDB.terminal_by_state_key_key(state_key)

    with {:ok, reverse_ops} <-
           stale_terminal_reverse_delete_ops(lmdb_path, reverse_key),
         {:ok, id_ops} <- cleanup_stale_terminal_ops_by_id(lmdb_path, state_key, record) do
      {:ok, Enum.uniq(reverse_ops ++ id_ops)}
    end
  end

  defp stale_terminal_reverse_delete_ops(lmdb_path, reverse_key) do
    case LMDB.get(lmdb_path, reverse_key) do
      :not_found ->
        {:ok, []}

      {:ok, terminal_key} when is_binary(terminal_key) ->
        case LMDB.terminal_index_delete_ops_result(lmdb_path, terminal_key, nil) do
          {:ok, terminal_ops} -> {:ok, [{:delete, reverse_key} | terminal_ops]}
          {:error, _reason} = error -> error
        end

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_terminal_reverse_read}
    end
  end

  def cleanup_stale_terminal_reverse_ops(lmdb_path, keydir, decode_entry_fun)
      when is_function(decode_entry_fun, 1) do
    LMDB.reduce_prefix_entries(
      lmdb_path,
      LMDB.terminal_by_state_global_prefix(),
      terminal_projection_page_size(),
      [],
      fn entries, reversed_ops ->
        case cleanup_stale_terminal_reverse_scan_result(
               {:ok, entries},
               keydir,
               decode_entry_fun,
               fn terminal_key, state_key ->
                 LMDB.terminal_index_delete_ops_result(lmdb_path, terminal_key, state_key)
               end
             ) do
          {:ok, ops} -> {:ok, :lists.reverse(ops, reversed_ops)}
          {:error, _reason} = error -> error
        end
      end
    )
    |> reverse_ops_result()
  end

  @doc false
  def __cleanup_stale_terminal_reverse_scan_result_for_test__(result, keydir, decode_entry_fun),
    do:
      cleanup_stale_terminal_reverse_scan_result(
        result,
        keydir,
        decode_entry_fun,
        fn _terminal_key, _state_key -> {:ok, []} end
      )

  @doc false
  def __cleanup_stale_terminal_reverse_scan_with_delete_for_test__(
        result,
        keydir,
        decode_entry_fun,
        delete_fun
      ),
      do:
        cleanup_stale_terminal_reverse_scan_result(
          result,
          keydir,
          decode_entry_fun,
          delete_fun
        )

  defp cleanup_stale_terminal_reverse_scan_result(
         {:ok, entries},
         keydir,
         decode_entry_fun,
         delete_fun
       )
       when is_list(entries) do
    entries
    |> Enum.reduce_while({:ok, []}, fn
      {reverse_key, terminal_key}, {:ok, reversed_ops}
      when is_binary(reverse_key) and is_binary(terminal_key) ->
        case terminal_state_key_from_reverse_key(reverse_key) do
          {:ok, state_key} ->
            if terminal_state_key?(keydir, state_key, decode_entry_fun) do
              {:cont, {:ok, reversed_ops}}
            else
              case delete_fun.(terminal_key, nil) do
                {:ok, terminal_ops} when is_list(terminal_ops) ->
                  entry_ops = [{:delete, reverse_key} | terminal_ops]
                  {:cont, {:ok, :lists.reverse(entry_ops, reversed_ops)}}

                {:error, _reason} = error ->
                  {:halt, error}

                invalid ->
                  {:halt, {:error, {:invalid_terminal_delete_plan, invalid}}}
              end
            end

          :error ->
            {:halt, {:error, :invalid_terminal_reverse_key}}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_terminal_reverse_entry, invalid}}}
    end)
    |> case do
      {:ok, reversed_ops} -> {:ok, Enum.reverse(reversed_ops)}
      {:error, _reason} = error -> error
    end
  end

  defp cleanup_stale_terminal_reverse_scan_result(
         {:error, _reason} = error,
         _keydir,
         _decode_fun,
         _delete_fun
       ),
       do: error

  defp cleanup_stale_terminal_reverse_scan_result(invalid, _keydir, _decode_fun, _delete_fun),
    do: {:error, {:invalid_terminal_reverse_scan, invalid}}

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
    case existing_terminal_count_keys(lmdb_path) do
      {:ok, existing_count_keys} ->
        count_keys = MapSet.union(existing_count_keys, MapSet.new(Map.keys(counts)))

        ops =
          Enum.map(count_keys, fn count_key ->
            {:put, count_key, LMDB.encode_count(Map.get(counts, count_key, 0))}
          end)

        case LMDB.write_batch(lmdb_path, ops) do
          :ok ->
            Enum.each(count_keys, fn count_key ->
              LMDB.put_cached_terminal_count_key(
                lmdb_path,
                count_key,
                Map.get(counts, count_key, 0)
              )
            end)

            stats

          {:error, _reason} ->
            %{stats | lmdb_errors: stats.lmdb_errors + 1}
        end

      {:error, _reason} ->
        %{stats | lmdb_errors: stats.lmdb_errors + 1}
    end
  end

  defp existing_terminal_count_keys(lmdb_path) do
    page_size =
      :ferricstore
      |> Application.get_env(
        :flow_lmdb_rebuild_count_key_page_size,
        @default_scan_page_size
      )
      |> normalize_scan_page_size()

    LMDB.reduce_prefix_entries(
      lmdb_path,
      LMDB.terminal_count_prefix(),
      page_size,
      MapSet.new(),
      fn entries, keys ->
        case existing_terminal_count_keys_result({:ok, entries}) do
          {:ok, page_keys} -> {:ok, MapSet.union(keys, page_keys)}
          {:error, _reason} = error -> error
        end
      end
    )
  end

  @doc false
  def __existing_terminal_count_keys_result_for_test__(result),
    do: existing_terminal_count_keys_result(result)

  defp existing_terminal_count_keys_result({:ok, entries}) when is_list(entries) do
    if Enum.all?(entries, fn
         {key, value} when is_binary(key) and is_binary(value) -> true
         _invalid -> false
       end) do
      {:ok, MapSet.new(entries, fn {key, _value} -> key end)}
    else
      {:error, :invalid_terminal_count_scan_entry}
    end
  end

  defp existing_terminal_count_keys_result({:error, _reason} = error), do: error

  defp existing_terminal_count_keys_result(invalid),
    do: {:error, {:invalid_terminal_count_scan, invalid}}

  defp cleanup_stale_terminal_ops_by_id(lmdb_path, state_key, %{id: id, type: type} = record)
       when is_binary(id) and is_binary(type) do
    partition_key = Map.get(record, :partition_key)

    specific_result =
      Enum.reduce_while(["completed", "failed", "cancelled"], {:ok, []}, fn
        terminal_state, {:ok, reversed_ops} ->
          index_key = Flow.Keys.state_index_key(type, terminal_state, partition_key)

          case cleanup_stale_terminal_ops_under_prefix(
                 lmdb_path,
                 LMDB.terminal_index_prefix(index_key),
                 id,
                 state_key,
                 false
               ) do
            {:ok, ops} -> {:cont, {:ok, :lists.reverse(ops, reversed_ops)}}
            {:error, _reason} = error -> {:halt, error}
          end
      end)

    case specific_result do
      {:ok, reversed_ops} -> {:ok, Enum.reverse(reversed_ops)}
      {:error, _reason} = error -> error
    end
  end

  defp cleanup_stale_terminal_ops_by_id(_lmdb_path, _state_key, _record), do: {:ok, []}

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
    LMDB.reduce_prefix_entries(
      lmdb_path,
      prefix,
      terminal_projection_page_size(),
      [],
      fn entries, reversed_ops ->
        case cleanup_stale_terminal_entries_result(
               {:ok, entries},
               id,
               current_state_key,
               nil_state_key_only?,
               fn terminal_key, delete_state_key ->
                 LMDB.terminal_index_delete_ops_result(
                   lmdb_path,
                   terminal_key,
                   delete_state_key
                 )
               end
             ) do
          {:ok, ops} -> {:ok, :lists.reverse(ops, reversed_ops)}
          {:error, _reason} = error -> error
        end
      end
    )
    |> reverse_ops_result()
  end

  @doc false
  def __cleanup_stale_terminal_entries_for_test__(
        result,
        id,
        current_state_key,
        nil_state_key_only?,
        delete_fun
      ),
      do:
        cleanup_stale_terminal_entries_result(
          result,
          id,
          current_state_key,
          nil_state_key_only?,
          delete_fun
        )

  defp cleanup_stale_terminal_entries_result(
         {:ok, entries},
         id,
         current_state_key,
         nil_state_key_only?,
         delete_fun
       )
       when is_list(entries) do
    entries
    |> Enum.reduce_while({:ok, []}, fn
      {terminal_key, value}, {:ok, reversed_ops}
      when is_binary(terminal_key) and is_binary(value) ->
        case LMDB.decode_terminal_index_value(value) do
          {:ok, {^id, _updated_at_ms, _expire_at_ms, nil}} ->
            append_stale_terminal_delete(
              terminal_key,
              nil,
              nil,
              delete_fun,
              reversed_ops
            )

          {:ok, {^id, _updated_at_ms, _expire_at_ms, ^current_state_key}}
          when is_binary(current_state_key) and not nil_state_key_only? ->
            append_stale_terminal_delete(
              terminal_key,
              nil,
              current_state_key,
              delete_fun,
              reversed_ops
            )

          {:ok, {_other_id, _updated_at_ms, _expire_at_ms, _stored_state_key}} ->
            {:cont, {:ok, reversed_ops}}

          :error ->
            {:halt, {:error, {:invalid_terminal_index_value, terminal_key}}}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_terminal_index_entry, invalid}}}
    end)
    |> case do
      {:ok, reversed_ops} -> {:ok, Enum.reverse(reversed_ops)}
      {:error, _reason} = error -> error
    end
  end

  defp cleanup_stale_terminal_entries_result(
         {:error, _reason} = error,
         _id,
         _current_state_key,
         _nil_state_key_only?,
         _delete_fun
       ),
       do: error

  defp cleanup_stale_terminal_entries_result(
         invalid,
         _id,
         _current_state_key,
         _nil_state_key_only?,
         _delete_fun
       ),
       do: {:error, {:invalid_terminal_index_scan, invalid}}

  defp append_stale_terminal_delete(
         terminal_key,
         delete_state_key,
         reverse_state_key,
         delete_fun,
         reversed_ops
       ) do
    case delete_fun.(terminal_key, delete_state_key) do
      {:ok, terminal_ops} when is_list(terminal_ops) ->
        ops =
          if is_binary(reverse_state_key) do
            [{:delete, LMDB.terminal_by_state_key_key(reverse_state_key)} | terminal_ops]
          else
            terminal_ops
          end

        {:cont, {:ok, :lists.reverse(ops, reversed_ops)}}

      {:error, _reason} = error ->
        {:halt, error}

      invalid ->
        {:halt, {:error, {:invalid_terminal_delete_plan, invalid}}}
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

  defp terminal_projection_page_size do
    :ferricstore
    |> Application.get_env(
      :flow_lmdb_terminal_rebuild_page_size,
      @default_scan_page_size
    )
    |> normalize_scan_page_size()
  end

  @doc false
  def __normalize_scan_limit_for_test__(value), do: normalize_scan_page_size(value)

  defp normalize_scan_page_size(value) when is_integer(value) and value > 0,
    do: min(value, @max_scan_page_size)

  defp normalize_scan_page_size(_invalid), do: @default_scan_page_size

  defp reverse_ops_result({:ok, reversed_ops}), do: {:ok, Enum.reverse(reversed_ops)}
  defp reverse_ops_result({:error, _reason} = error), do: error
end
