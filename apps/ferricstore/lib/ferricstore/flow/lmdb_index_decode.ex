defmodule Ferricstore.Flow.LMDBIndexDecode do
  @moduledoc false

  alias Ferricstore.Flow.LMDB

  def terminal_entries(entries, path, now_ms)
      when is_list(entries) and is_binary(path) and is_integer(now_ms) and now_ms >= 0 do
    reduce_entries(entries, fn
      key, value, acc ->
        case LMDB.decode_terminal_index_value(value) do
          {:ok, {id, updated_at_ms, expire_at_ms, state_key}} ->
            cond do
              not LMDB.terminal_index_entry_key?(key, id, updated_at_ms) ->
                {:error, {:invalid_terminal_index_value, key}}

              expire_at_ms <= 0 or expire_at_ms > now_ms ->
                {:ok, [{id, updated_at_ms} | acc]}

              true ->
                case LMDB.delete_terminal_index_entry(path, key, state_key) do
                  :ok -> {:ok, acc}
                  {:error, _reason} = error -> error
                end
            end

          :error ->
            {:error, {:invalid_terminal_index_value, key}}
        end
    end)
  end

  def terminal_entries(_entries, _path, _now_ms),
    do: {:error, :invalid_terminal_index_entries}

  def query_entries(entries, path, now_ms)
      when is_list(entries) and is_binary(path) and is_integer(now_ms) and now_ms >= 0 do
    reduce_entries(entries, fn
      key, value, acc ->
        case decode_query_entry(key, value) do
          {:ok, id, updated_at_ms, expire_at_ms, state_key}
          when expire_at_ms <= 0 or expire_at_ms > now_ms ->
            {:ok, [{id, updated_at_ms, state_key} | acc]}

          {:ok, _id, _updated_at_ms, _expire_at_ms, _state_key} ->
            case LMDB.write_batch(path, [{:delete, key}]) do
              :ok -> {:ok, acc}
              {:error, _reason} = error -> error
            end

          {:error, _reason} = error ->
            error
        end
    end)
  end

  def query_entries(_entries, _path, _now_ms),
    do: {:error, :invalid_query_index_entries}

  def query_entries_readonly(entries, now_ms)
      when is_list(entries) and is_integer(now_ms) and now_ms >= 0 do
    reduce_entries(entries, fn key, value, acc ->
      case decode_query_entry(key, value) do
        {:ok, id, updated_at_ms, expire_at_ms, state_key}
        when expire_at_ms <= 0 or expire_at_ms > now_ms ->
          {:ok, [{id, updated_at_ms, state_key} | acc]}

        {:ok, _id, _updated_at_ms, _expire_at_ms, _state_key} ->
          {:ok, acc}

        {:error, _reason} = error ->
          error
      end
    end)
  end

  def query_entries_readonly(_entries, _now_ms),
    do: {:error, :invalid_query_index_entries}

  def history_entries(entries, path, now_ms)
      when is_list(entries) and is_binary(path) and is_integer(now_ms) and now_ms >= 0 do
    reduce_entries(entries, fn
      key, value, acc ->
        case decode_history_entry(key, value) do
          {:ok, event_id, event_ms, expire_at_ms} ->
            cond do
              expire_at_ms <= 0 or expire_at_ms > now_ms ->
                {:ok, [{event_id, event_ms} | acc]}

              true ->
                case LMDB.delete_history_index_entry(path, key) do
                  :ok -> {:ok, acc}
                  {:error, _reason} = error -> error
                end
            end

          {:error, _reason} = error ->
            error
        end
    end)
  end

  def history_entries(_entries, _path, _now_ms),
    do: {:error, :invalid_history_index_entries}

  def history_query_entries(entries, now_ms)
      when is_list(entries) and is_integer(now_ms) and now_ms >= 0 do
    reduce_entries(entries, fn key, value, acc ->
      case decode_history_entry(key, value) do
        {:ok, event_id, event_ms, expire_at_ms}
        when expire_at_ms <= 0 or expire_at_ms > now_ms ->
          {:ok, [{event_id, event_ms} | acc]}

        {:ok, _event_id, _event_ms, _expire_at_ms} ->
          {:ok, acc}

        {:error, _reason} = error ->
          error
      end
    end)
  end

  def history_query_entries(_entries, _now_ms),
    do: {:error, :invalid_history_index_entries}

  defp decode_history_entry(key, value) do
    case LMDB.decode_history_index_value(value) do
      {:ok, {event_id, event_ms, expire_at_ms, _compound_key}} ->
        if LMDB.history_index_entry_key?(key, event_id, event_ms),
          do: {:ok, event_id, event_ms, expire_at_ms},
          else: {:error, {:invalid_history_index_value, key}}

      :error ->
        {:error, {:invalid_history_index_value, key}}
    end
  end

  defp decode_query_entry(key, value) do
    case LMDB.decode_query_index_value(value) do
      {:ok,
       {family_digest, index_digest, _discovery_component, id, updated_at_ms, expire_at_ms,
        state_key}} ->
        if LMDB.query_index_entry_key?(
             key,
             family_digest,
             index_digest,
             id,
             updated_at_ms
           ) do
          {:ok, id, updated_at_ms, expire_at_ms, state_key}
        else
          {:error, {:invalid_query_index_value, key}}
        end

      :error ->
        {:error, {:invalid_query_index_value, key}}
    end
  end

  defp reduce_entries(entries, decode_fun) do
    entries
    |> Enum.reduce_while({:ok, []}, fn
      {key, value}, {:ok, acc} when is_binary(key) and is_binary(value) ->
        case decode_fun.(key, value, acc) do
          {:ok, next_acc} -> {:cont, {:ok, next_acc}}
          {:error, _reason} = error -> {:halt, error}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_lmdb_index_entry, invalid}}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end
end
