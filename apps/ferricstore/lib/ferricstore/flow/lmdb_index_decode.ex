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
        case LMDB.decode_query_index_value(value) do
          {:ok, {id, updated_at_ms, expire_at_ms, state_key}} ->
            cond do
              not LMDB.query_index_entry_key?(key, id, updated_at_ms) ->
                {:error, {:invalid_query_index_value, key}}

              expire_at_ms <= 0 or expire_at_ms > now_ms ->
                {:ok, [{id, updated_at_ms, state_key} | acc]}

              true ->
                case LMDB.write_batch(path, [{:delete, key}]) do
                  :ok -> {:ok, acc}
                  {:error, _reason} = error -> error
                end
            end

          :error ->
            {:error, {:invalid_query_index_value, key}}
        end
    end)
  end

  def query_entries(_entries, _path, _now_ms),
    do: {:error, :invalid_query_index_entries}

  def history_entries(entries, path, now_ms)
      when is_list(entries) and is_binary(path) and is_integer(now_ms) and now_ms >= 0 do
    reduce_entries(entries, fn
      key, value, acc ->
        case LMDB.decode_history_index_value(value) do
          {:ok, {event_id, event_ms, expire_at_ms, _compound_key}} ->
            cond do
              not LMDB.history_index_entry_key?(key, event_id, event_ms) ->
                {:error, {:invalid_history_index_value, key}}

              expire_at_ms <= 0 or expire_at_ms > now_ms ->
                {:ok, [{event_id, event_ms} | acc]}

              true ->
                case LMDB.delete_history_index_entry(path, key) do
                  :ok -> {:ok, acc}
                  {:error, _reason} = error -> error
                end
            end

          :error ->
            {:error, {:invalid_history_index_value, key}}
        end
    end)
  end

  def history_entries(_entries, _path, _now_ms),
    do: {:error, :invalid_history_index_entries}

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
