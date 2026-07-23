defmodule Ferricstore.Flow.LMDBRebuilder.TerminalState do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.LMDB

  def statuses(lmdb_path, keydir, state_keys, decode_entry_fun)
      when is_binary(lmdb_path) and is_list(state_keys) and is_function(decode_entry_fun, 1) do
    statuses_with_reader(keydir, state_keys, decode_entry_fun, fn keys ->
      LMDB.get_many(lmdb_path, keys)
    end)
  end

  @doc false
  def statuses_with_reader(keydir, state_keys, decode_entry_fun, read_many_fun)
      when is_list(state_keys) and is_function(decode_entry_fun, 1) and
             is_function(read_many_fun, 1) do
    with {:ok, hot_statuses, missing_state_keys} <-
           collect_hot_statuses(state_keys, keydir, decode_entry_fun, %{}, []),
         {:ok, durable_results} <- read_many_fun.(missing_state_keys),
         {:ok, statuses} <-
           merge_durable_statuses(
             missing_state_keys,
             durable_results,
             keydir,
             hot_statuses
           ) do
      {:ok, statuses}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_durable_flow_state_batch_read}
    end
  rescue
    ArgumentError -> {:error, :authoritative_flow_state_unavailable}
  end

  defp collect_hot_statuses([], _keydir, _decode_entry_fun, statuses, missing) do
    {:ok, statuses, Enum.reverse(missing)}
  end

  defp collect_hot_statuses(
         [state_key | state_keys],
         keydir,
         decode_entry_fun,
         statuses,
         missing
       )
       when is_binary(state_key) do
    case :ets.lookup(keydir, state_key) do
      [entry] ->
        with [{_key, _value, _expire_at_ms, record}] <- decode_entry_fun.(entry),
             {:ok, status} <- record_status(record, state_key) do
          collect_hot_statuses(
            state_keys,
            keydir,
            decode_entry_fun,
            Map.put(statuses, state_key, status),
            missing
          )
        else
          {:error, _reason} = error -> error
          _invalid -> {:error, :invalid_authoritative_flow_state}
        end

      [] ->
        if registry_owner?(keydir, state_key) do
          collect_hot_statuses(
            state_keys,
            keydir,
            decode_entry_fun,
            statuses,
            [state_key | missing]
          )
        else
          collect_hot_statuses(
            state_keys,
            keydir,
            decode_entry_fun,
            Map.put(statuses, state_key, :missing),
            missing
          )
        end

      _invalid ->
        {:error, :invalid_authoritative_flow_state_entry}
    end
  end

  defp collect_hot_statuses(_invalid, _keydir, _decode_entry_fun, _statuses, _missing),
    do: {:error, :invalid_authoritative_flow_state_keys}

  defp merge_durable_statuses([], [], _keydir, statuses), do: {:ok, statuses}

  defp merge_durable_statuses(
         [state_key | state_keys],
         [result | results],
         keydir,
         statuses
       ) do
    case durable_result_status(result, keydir, state_key) do
      {:ok, status} ->
        merge_durable_statuses(
          state_keys,
          results,
          keydir,
          Map.put(statuses, state_key, status)
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp merge_durable_statuses(_state_keys, _results, _keydir, _statuses),
    do: {:error, :invalid_durable_flow_state_batch_result}

  defp durable_result_status({:ok, blob}, keydir, state_key) when is_binary(blob) do
    if registry_owner?(keydir, state_key) do
      case LMDB.decode_value(blob, 0) do
        {:ok, value} when is_binary(value) ->
          value
          |> Flow.decode_record()
          |> record_status(state_key)

        _invalid ->
          {:error, :invalid_durable_flow_state}
      end
    else
      {:ok, :missing}
    end
  rescue
    _error -> {:error, :invalid_durable_flow_state}
  end

  defp durable_result_status(:not_found, keydir, state_key) do
    status = if registry_owner?(keydir, state_key), do: :retained_terminal, else: :missing
    {:ok, status}
  end

  defp durable_result_status({:error, _reason} = error, _keydir, _state_key), do: error

  defp durable_result_status(_invalid, _keydir, _state_key),
    do: {:error, :invalid_durable_flow_state_read}

  defp record_status(%{id: id, state: state} = record, state_key)
       when is_binary(id) and is_binary(state) do
    if Flow.Keys.state_key(id, Map.get(record, :partition_key)) == state_key do
      {:ok, if(LMDB.terminal_state?(state), do: :terminal, else: :active)}
    else
      {:error, :mismatched_authoritative_flow_state}
    end
  end

  defp record_status(_record, _state_key), do: {:error, :invalid_authoritative_flow_state}

  defp registry_owner?(keydir, state_key) do
    case Flow.Keys.registry_key_from_state_key(state_key) do
      {:ok, registry_key} -> :ets.member(keydir, registry_key)
      :error -> false
    end
  end
end
