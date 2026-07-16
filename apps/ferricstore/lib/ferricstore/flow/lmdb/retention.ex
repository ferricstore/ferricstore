defmodule Ferricstore.Flow.LMDB.Retention do
  @moduledoc false

  alias Ferricstore.Flow.LMDB.Access
  alias Ferricstore.Flow.LMDB.IndexCodec

  @max_u64 18_446_744_073_709_551_615

  def sweep_expired_terminal(path, now_ms, limit)
      when is_binary(path) and is_integer(now_ms) and is_integer(limit) and limit > 0 do
    with {:ok, entries} <-
           Access.prefix_entries(
             path,
             IndexCodec.terminal_expire_prefix(),
             limit
           ),
         {:ok, write_ops, counts, swept} <-
           terminal_sweep_write_plan(path, entries, now_ms) do
      case Access.write_batch(path, write_ops) do
        :ok ->
          Enum.each(counts, fn {count_key, {count, _original_value}} ->
            Ferricstore.Flow.LMDB.TerminalCounts.put_cached_count_key(path, count_key, count)
          end)

          {:ok, swept}

        {:error, _reason} = error ->
          error
      end
    end
  end

  def sweep_expired_terminal(_path, _now_ms, _limit), do: {:ok, 0}

  @doc false
  def __terminal_sweep_write_plan_for_test__(path, entries, now_ms) do
    with {:ok, write_ops, _counts, swept} <- terminal_sweep_write_plan(path, entries, now_ms) do
      {:ok, write_ops, swept}
    end
  end

  defp terminal_sweep_write_plan(path, entries, now_ms) do
    with {:ok, ops, counts, swept} <- expired_terminal_sweep_ops(path, entries, now_ms) do
      {:ok, terminal_count_write_ops(counts) ++ ops, counts, swept}
    end
  end

  defp terminal_count_write_ops(counts) do
    Enum.flat_map(counts, fn {count_key, {count, original_value}} ->
      [
        {:compare, count_key, original_value},
        {:put, count_key, IndexCodec.encode_count(count)}
      ]
    end)
  end

  def expired_terminal_state_keys(path, now_ms, limit)
      when is_binary(path) and is_integer(now_ms) and is_integer(limit) and limit > 0 do
    with {:ok, entries} <-
           Access.prefix_entries(
             path,
             IndexCodec.terminal_expire_prefix(),
             limit
           ) do
      entries
      |> Enum.reduce_while({:ok, []}, fn
        {expire_key, expire_value}, {:ok, acc}
        when is_binary(expire_key) and is_binary(expire_value) ->
          case terminal_expire_key_time(expire_key) do
            {:ok, expire_at_ms} when expire_at_ms > now_ms ->
              {:halt, {:ok, acc}}

            {:ok, expire_at_ms} ->
              case expired_terminal_state_key(
                     path,
                     expire_key,
                     expire_at_ms,
                     expire_value,
                     now_ms
                   ) do
                {:ok, state_key} when is_binary(state_key) ->
                  {:cont, {:ok, [state_key | acc]}}

                {:ok, nil} ->
                  {:cont, {:ok, acc}}

                {:error, _reason} = error ->
                  {:halt, error}
              end

            :error ->
              {:halt, {:error, {:invalid_terminal_expire_key, expire_key}}}
          end

        invalid, _acc ->
          {:halt, {:error, {:invalid_terminal_expire_entry, invalid}}}
      end)
      |> case do
        {:ok, keys} -> {:ok, keys |> Enum.reverse() |> Enum.uniq()}
        {:error, _reason} = error -> error
      end
    end
  end

  def expired_terminal_state_keys(_path, _now_ms, _limit), do: {:ok, []}

  def expired_active_timeout_state_keys(path, now_ms, limit)
      when is_binary(path) and is_integer(now_ms) and is_integer(limit) and limit > 0 do
    index_key = Ferricstore.Flow.Keys.active_timeout_index_key()

    with {:ok, entries} <-
           Access.prefix_entries(path, IndexCodec.active_index_prefix(index_key), limit) do
      entries
      |> Enum.reduce_while({:ok, []}, fn
        {key, value}, {:ok, acc} when is_binary(key) and is_binary(value) ->
          case IndexCodec.decode_active_index_value(value) do
            {:ok, {^index_key, member, deadline_ms, _expire_at_ms, state_key}} ->
              cond do
                not IndexCodec.active_index_entry_key?(key, index_key, member, deadline_ms) ->
                  {:halt, {:error, {:invalid_active_timeout_index_value, key}}}

                deadline_ms <= now_ms ->
                  {:cont, {:ok, [state_key | acc]}}

                true ->
                  {:halt, {:ok, acc}}
              end

            _invalid ->
              {:halt, {:error, {:invalid_active_timeout_index_value, key}}}
          end

        invalid, _acc ->
          {:halt, {:error, {:invalid_active_timeout_index_entry, invalid}}}
      end)
      |> case do
        {:ok, state_keys} -> {:ok, Enum.reverse(state_keys)}
        {:error, _reason} = error -> error
      end
    end
  end

  def expired_active_timeout_state_keys(_path, _now_ms, _limit), do: {:ok, []}

  def sweep_expired_history(path, now_ms, limit)
      when is_binary(path) and is_integer(now_ms) and is_integer(limit) and limit > 0 do
    with {:ok, entries} <-
           Access.prefix_entries(
             path,
             IndexCodec.history_expire_prefix(),
             limit
           ),
         {:ok, ops, swept} <- expired_history_sweep_ops(path, entries, now_ms),
         {:ok, flow_ops, flow_swept} <-
           expired_history_flow_sweep(path, now_ms, limit, max(limit - swept, 0)) do
      case Access.write_batch(path, flow_ops ++ ops) do
        :ok -> {:ok, swept + flow_swept}
        {:error, _reason} = error -> error
      end
    end
  end

  def sweep_expired_history(_path, _now_ms, _limit), do: {:ok, 0}

  defp expired_terminal_sweep_ops(path, entries, now_ms) do
    Enum.reduce_while(entries, {:ok, [], %{}, 0}, fn
      {expire_key, expire_value}, {:ok, ops, counts, swept}
      when is_binary(expire_key) and is_binary(expire_value) ->
        case terminal_expire_key_time(expire_key) do
          {:ok, expire_at_ms} when expire_at_ms > now_ms ->
            {:halt, {:ok, ops, counts, swept}}

          {:ok, expire_at_ms} ->
            case expired_terminal_entry_ops(
                   path,
                   expire_key,
                   expire_at_ms,
                   expire_value,
                   counts,
                   now_ms
                 ) do
              {:ok, entry_ops, next_counts, entry_swept} ->
                {:cont, {:ok, entry_ops ++ ops, next_counts, swept + entry_swept}}

              {:error, _reason} = error ->
                {:halt, error}
            end

          :error ->
            {:halt, {:error, {:invalid_terminal_expire_key, expire_key}}}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_terminal_expire_entry, invalid}}}
    end)
  end

  defp expired_history_sweep_ops(path, entries, now_ms) do
    Enum.reduce_while(entries, {:ok, [], 0}, fn
      {expire_key, expire_value}, {:ok, ops, swept}
      when is_binary(expire_key) and is_binary(expire_value) ->
        case expire_key_time(expire_key, IndexCodec.history_expire_prefix()) do
          {:ok, expire_at_ms} when expire_at_ms > now_ms ->
            {:halt, {:ok, ops, swept}}

          {:ok, expire_at_ms} ->
            case expired_history_entry_ops(
                   path,
                   expire_key,
                   expire_at_ms,
                   expire_value,
                   now_ms
                 ) do
              {:ok, entry_ops, entry_swept} ->
                {:cont, {:ok, entry_ops ++ ops, swept + entry_swept}}

              {:error, _reason} = error ->
                {:halt, error}
            end

          :error ->
            {:halt, {:error, {:invalid_history_expire_key, expire_key}}}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_history_expire_entry, invalid}}}
    end)
  end

  @doc false
  def __history_sweep_write_plan_for_test__(path, entries, now_ms),
    do: expired_history_sweep_ops(path, entries, now_ms)

  defp expired_history_flow_sweep(_path, _now_ms, _marker_limit, 0), do: {:ok, [], 0}

  defp expired_history_flow_sweep(path, now_ms, marker_limit, event_budget) do
    with {:ok, flow_entries} <-
           Access.prefix_entries(
             path,
             IndexCodec.history_flow_expire_prefix(),
             marker_limit
           ) do
      expired_history_flow_sweep_ops(path, flow_entries, now_ms, event_budget)
    end
  end

  defp expired_history_flow_sweep_ops(path, entries, now_ms, event_budget) do
    Enum.reduce_while(entries, {:ok, [], 0, event_budget}, fn
      _entry, {:ok, ops, swept, 0} ->
        {:halt, {:ok, ops, swept, 0}}

      {expire_key, expire_value}, {:ok, ops, swept, remaining}
      when is_binary(expire_key) and is_binary(expire_value) ->
        case expire_key_time(expire_key, IndexCodec.history_flow_expire_prefix()) do
          {:ok, expire_at_ms} when expire_at_ms > now_ms ->
            {:halt, {:ok, ops, swept, remaining}}

          {:ok, expire_at_ms} ->
            case expired_history_flow_entry_ops(
                   path,
                   expire_key,
                   expire_value,
                   expire_at_ms,
                   remaining
                 ) do
              {:ok, entry_ops, entry_swept} ->
                {:cont, {:ok, entry_ops ++ ops, swept + entry_swept, remaining - entry_swept}}

              {:error, _reason} = error ->
                {:halt, error}
            end

          :error ->
            {:halt, {:error, {:invalid_history_flow_expire_key, expire_key}}}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_history_flow_expire_entry, invalid}}}
    end)
    |> case do
      {:ok, ops, swept, _remaining} -> {:ok, ops, swept}
      {:error, _reason} = error -> error
    end
  end

  defp expired_history_flow_entry_ops(
         path,
         expire_key,
         expire_value,
         marker_expire_at_ms,
         limit
       ) do
    case IndexCodec.decode_history_flow_expire_value(expire_value) do
      {:ok, {history_key, history_cutoff_ms}}
      when history_cutoff_ms == marker_expire_at_ms ->
        expected_expire_key =
          IndexCodec.history_flow_expire_key(marker_expire_at_ms, history_key)

        if expire_key != expected_expire_key do
          {:error, {:invalid_history_flow_expire_value, expire_key}}
        else
          expired_history_flow_entries_ops(
            path,
            expire_key,
            history_key,
            history_cutoff_ms,
            limit
          )
        end

      _invalid ->
        {:error, {:invalid_history_flow_expire_value, expire_key}}
    end
  end

  defp expired_history_flow_entries_ops(
         path,
         expire_key,
         history_key,
         history_cutoff_ms,
         limit
       ) do
    prefix = IndexCodec.history_index_prefix(history_key)
    read_limit = max(limit, 1) + 1

    case Access.prefix_entries(path, prefix, read_limit) do
      {:ok, entries} ->
        {entries, keep_marker?} =
          if length(entries) > limit do
            {Enum.take(entries, limit), true}
          else
            {entries, false}
          end

        marker_value =
          IndexCodec.encode_history_flow_expire_value(history_key, history_cutoff_ms)

        base_ops =
          if keep_marker? do
            [{:compare, expire_key, marker_value}]
          else
            [{:compare, expire_key, marker_value}, {:delete, expire_key}]
          end

        entries
        |> Enum.reduce_while({:ok, base_ops, 0}, fn
          {history_index_key, history_value}, {:ok, ops_acc, swept_acc}
          when is_binary(history_index_key) and is_binary(history_value) ->
            case IndexCodec.decode_history_index_value(history_value) do
              {:ok, {event_id, event_ms, _expire_at_ms, _compound_key}} ->
                cond do
                  not IndexCodec.history_index_entry_key?(
                    history_index_key,
                    event_id,
                    event_ms
                  ) ->
                    {:halt, {:error, {:invalid_history_index_value, history_index_key}}}

                  history_event_before_cutoff?(event_ms, history_cutoff_ms) ->
                    {:cont,
                     {:ok,
                      history_index_delete_ops_from_value(
                        [
                          {:compare, history_index_key, history_value},
                          {:delete, history_index_key}
                          | ops_acc
                        ],
                        history_index_key,
                        history_value
                      ), swept_acc + 1}}

                  true ->
                    {:cont, {:ok, ops_acc, swept_acc}}
                end

              :error ->
                {:halt, {:error, {:invalid_history_index_value, history_index_key}}}
            end

          invalid, _acc ->
            {:halt, {:error, {:invalid_history_index_entry, invalid}}}
        end)

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_history_index_scan, invalid}}
    end
  end

  defp history_event_before_cutoff?(event_ms, :infinity) when is_integer(event_ms), do: true

  defp history_event_before_cutoff?(event_ms, cutoff_ms)
       when is_integer(event_ms) and is_integer(cutoff_ms),
       do: event_ms <= cutoff_ms

  defp history_event_before_cutoff?(_event_ms, _cutoff_ms), do: false

  defp expired_history_entry_ops(path, expire_key, marker_expire_at_ms, expire_value, now_ms) do
    case IndexCodec.decode_history_expire_value(expire_value) do
      {:ok, history_index_key} ->
        if expire_key == IndexCodec.history_expire_key(marker_expire_at_ms, history_index_key) do
          case Access.get(path, history_index_key) do
            {:ok, history_value} ->
              with {:ok, ops, swept} <-
                     expired_live_history_ops(
                       expire_key,
                       history_index_key,
                       history_value,
                       now_ms
                     ) do
                {:ok, [{:compare, expire_key, expire_value} | ops], swept}
              end

            :not_found ->
              {:ok,
               [
                 {:compare, expire_key, expire_value},
                 {:compare_missing, history_index_key},
                 {:delete, expire_key}
               ], 0}

            {:error, _reason} = error ->
              error

            invalid ->
              {:error, {:invalid_history_index_read, history_index_key, invalid}}
          end
        else
          {:error, {:invalid_history_expire_value, expire_key}}
        end

      :error ->
        {:error, {:invalid_history_expire_value, expire_key}}
    end
  end

  defp expired_live_history_ops(expire_key, history_index_key, history_value, now_ms) do
    case IndexCodec.decode_history_index_value(history_value) do
      {:ok, {event_id, event_ms, expire_at_ms, _compound_key}} ->
        cond do
          not IndexCodec.history_index_entry_key?(history_index_key, event_id, event_ms) ->
            {:error, {:invalid_history_index_value, history_index_key}}

          expire_at_ms > 0 and expire_at_ms <= now_ms ->
            {:ok,
             [
               {:compare, history_index_key, history_value},
               {:delete, expire_key},
               {:delete, history_index_key}
             ], 1}

          true ->
            {:ok, [{:compare, history_index_key, history_value}, {:delete, expire_key}], 0}
        end

      :error ->
        {:error, {:invalid_history_index_value, history_index_key}}
    end
  end

  defp expired_terminal_entry_ops(
         path,
         expire_key,
         marker_expire_at_ms,
         expire_value,
         counts,
         now_ms
       ) do
    case IndexCodec.decode_terminal_expire_value(expire_value) do
      {:ok, {terminal_key, state_key, count_key}} ->
        if expire_key == IndexCodec.terminal_expire_key(marker_expire_at_ms, terminal_key) do
          case Access.get(path, terminal_key) do
            {:ok, terminal_value} ->
              with {:ok, ops, next_counts, swept} <-
                     expired_live_terminal_ops(
                       path,
                       expire_key,
                       terminal_key,
                       terminal_value,
                       state_key,
                       count_key,
                       counts,
                       now_ms
                     ) do
                {:ok, [{:compare, expire_key, expire_value} | ops], next_counts, swept}
              end

            :not_found ->
              with {:ok, ops, next_counts, swept} <-
                     expired_missing_terminal_ops(
                       path,
                       expire_key,
                       terminal_key,
                       state_key,
                       counts
                     ) do
                {:ok, [{:compare, expire_key, expire_value} | ops], next_counts, swept}
              end

            {:error, _reason} = error ->
              error

            invalid ->
              {:error, {:invalid_terminal_index_read, terminal_key, invalid}}
          end
        else
          {:error, {:invalid_terminal_expire_value, expire_key}}
        end

      :error ->
        {:error, {:invalid_terminal_expire_value, expire_key}}
    end
  end

  defp expired_terminal_state_key(
         path,
         expire_key,
         marker_expire_at_ms,
         expire_value,
         now_ms
       ) do
    case IndexCodec.decode_terminal_expire_value(expire_value) do
      {:ok, {terminal_key, state_key, count_key}} ->
        if expire_key == IndexCodec.terminal_expire_key(marker_expire_at_ms, terminal_key) do
          case Access.get(path, terminal_key) do
            {:ok, terminal_value} ->
              case IndexCodec.decode_terminal_index_value(terminal_value) do
                {:ok, {id, updated_at_ms, expire_at_ms, decoded_state_key}} ->
                  cond do
                    not IndexCodec.terminal_index_entry_key?(terminal_key, id, updated_at_ms) ->
                      {:error, {:invalid_terminal_index_value, terminal_key}}

                    decoded_state_key != state_key or
                        IndexCodec.terminal_index_count_key(terminal_value) != {:ok, count_key} ->
                      {:error, :invalid_terminal_expire_value}

                    expire_at_ms > 0 and expire_at_ms <= now_ms ->
                      {:ok, decoded_state_key || state_key}

                    true ->
                      {:ok, nil}
                  end

                :error ->
                  {:error, {:invalid_terminal_index_value, terminal_key}}
              end

            :not_found ->
              {:ok, state_key}

            {:error, _reason} = error ->
              error

            invalid ->
              {:error, {:invalid_terminal_index_read, terminal_key, invalid}}
          end
        else
          {:error, :invalid_terminal_expire_value}
        end

      :error ->
        {:error, :invalid_terminal_expire_value}
    end
  end

  defp expired_live_terminal_ops(
         path,
         expire_key,
         terminal_key,
         terminal_value,
         state_key,
         count_key,
         counts,
         now_ms
       ) do
    case IndexCodec.decode_terminal_index_value(terminal_value) do
      {:ok, {id, updated_at_ms, expire_at_ms, decoded_state_key}} ->
        cond do
          not IndexCodec.terminal_index_entry_key?(terminal_key, id, updated_at_ms) ->
            {:error, {:invalid_terminal_index_value, terminal_key}}

          decoded_state_key != state_key or
              IndexCodec.terminal_index_count_key(terminal_value) != {:ok, count_key} ->
            {:error, {:invalid_terminal_expire_value, expire_key}}

          expire_at_ms > 0 and expire_at_ms <= now_ms ->
            reverse_key =
              if is_binary(state_key),
                do: IndexCodec.terminal_by_state_key_key(state_key),
                else: nil

            with {:ok, current_count, original_count_value} <-
                   terminal_count_for_sweep(path, count_key, counts),
                 {:ok, ops} <-
                   [{:delete, terminal_key}]
                   |> maybe_delete_terminal_reverse(reverse_key, terminal_key)
                   |> maybe_delete_expire_key(path, expire_key, state_key) do
              count = max(current_count - 1, 0)

              {:ok, [{:compare, terminal_key, terminal_value} | ops],
               Map.put(counts, count_key, {count, original_count_value}), 1}
            end

          true ->
            {:ok, [{:compare, terminal_key, terminal_value}, {:delete, expire_key}], counts, 0}
        end

      :error ->
        {:error, {:invalid_terminal_index_value, terminal_key}}
    end
  end

  defp terminal_count_for_sweep(path, count_key, counts) do
    case Map.fetch(counts, count_key) do
      {:ok, {count, original_value}} ->
        {:ok, count, original_value}

      :error ->
        case Access.get(path, count_key) do
          {:ok, value} when is_binary(value) ->
            case IndexCodec.decode_count(value) do
              {:ok, count} -> {:ok, count, value}
              :error -> {:error, :invalid_terminal_count_value}
            end

          :not_found ->
            {:error, :missing_terminal_count_value}

          {:error, _reason} = error ->
            error

          _invalid ->
            {:error, :invalid_terminal_count_read}
        end
    end
  end

  defp expired_missing_terminal_ops(_path, expire_key, terminal_key, state_key, counts)
       when not is_binary(state_key) do
    {:ok, [{:compare_missing, terminal_key}, {:delete, expire_key}], counts, 0}
  end

  defp expired_missing_terminal_ops(path, expire_key, terminal_key, state_key, counts) do
    case Access.get(path, state_key) do
      {:ok, state_value} when is_binary(state_value) ->
        {:ok,
         [
           {:compare_missing, terminal_key},
           {:compare, state_key, state_value}
         ], counts, 0}

      :not_found ->
        {:ok,
         [
           {:compare_missing, terminal_key},
           {:compare_missing, state_key},
           {:delete, expire_key}
         ], counts, 0}

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_terminal_state_read, state_key, invalid}}
    end
  end

  defp terminal_expire_key_time(key) do
    expire_key_time(key, IndexCodec.terminal_expire_prefix())
  end

  defp maybe_delete_expire_key(ops, path, expire_key, state_key)
       when is_binary(expire_key) and is_binary(state_key) do
    case Access.get(path, state_key) do
      :not_found ->
        {:ok, [{:compare_missing, state_key}, {:delete, expire_key} | ops]}

      {:ok, state_value} when is_binary(state_value) ->
        {:ok, [{:compare, state_key, state_value} | ops]}

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_terminal_state_read, state_key, invalid}}
    end
  end

  defp maybe_delete_expire_key(ops, _path, expire_key, _state_key) when is_binary(expire_key),
    do: {:ok, [{:delete, expire_key} | ops]}

  defp maybe_delete_expire_key(ops, _path, _expire_key, _state_key), do: {:ok, ops}

  defp expire_key_time(key, prefix) do
    size = byte_size(prefix)

    with true <- byte_size(key) > size + 21,
         ^prefix <- binary_part(key, 0, size),
         digits <- binary_part(key, size, 20),
         <<0>> <- binary_part(key, size + 20, 1),
         {value, ""} <- Integer.parse(digits),
         true <- value >= 0 and value <= @max_u64,
         true <- String.pad_leading(Integer.to_string(value), 20, "0") == digits do
      {:ok, value}
    else
      _invalid -> :error
    end
  end

  defp maybe_delete_terminal_reverse(ops, reverse_key, terminal_key)
       when is_binary(reverse_key) do
    [{:compare, reverse_key, terminal_key}, {:delete, reverse_key} | ops]
  end

  defp maybe_delete_terminal_reverse(ops, _reverse_key, _terminal_key), do: ops

  defp history_index_delete_ops_from_value(ops, history_index_key, history_value) do
    maybe_delete_history_expire_key(ops, history_index_key, history_value)
  end

  defp maybe_delete_history_expire_key(ops, history_index_key, history_value) do
    case IndexCodec.decode_history_index_value(history_value) do
      {:ok, {_event_id, _event_ms, expire_at_ms, _compound_key}} when expire_at_ms > 0 ->
        expire_key = IndexCodec.history_expire_key(expire_at_ms, history_index_key)
        expire_value = IndexCodec.encode_history_expire_value(history_index_key)
        [{:compare, expire_key, expire_value}, {:delete, expire_key} | ops]

      _ ->
        ops
    end
  end
end
