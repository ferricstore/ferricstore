defmodule Ferricstore.Flow.LMDBIndexDecode do
  @moduledoc false

  alias Ferricstore.Flow.LMDB

  def terminal_entries(entries, path, now_ms) do
    Enum.flat_map(entries, fn {key, value} ->
      case LMDB.decode_terminal_index_value(value) do
        {:ok, {id, updated_at_ms, expire_at_ms, _state_key}}
        when expire_at_ms <= 0 or expire_at_ms > now_ms ->
          [{id, updated_at_ms}]

        {:ok, {_id, _updated_at_ms, _expire_at_ms, state_key}} ->
          LMDB.delete_terminal_index_entry(path, key, state_key)
          []

        :error ->
          []
      end
    end)
  end

  def query_entries(entries, path, now_ms) do
    Enum.flat_map(entries, fn {key, value} ->
      case LMDB.decode_query_index_value(value) do
        {:ok, {id, updated_at_ms, expire_at_ms, state_key}}
        when expire_at_ms <= 0 or expire_at_ms > now_ms ->
          [{id, updated_at_ms, state_key}]

        {:ok, {_id, _updated_at_ms, _expire_at_ms, _state_key}} ->
          LMDB.write_batch(path, [{:delete, key}])
          []

        :error ->
          []
      end
    end)
  end

  def history_entries(entries, path, now_ms) do
    Enum.flat_map(entries, fn {key, value} ->
      case LMDB.decode_history_index_value(value) do
        {:ok, {event_id, event_ms, expire_at_ms, _compound_key}}
        when expire_at_ms <= 0 or expire_at_ms > now_ms ->
          [{event_id, event_ms}]

        {:ok, {_event_id, _event_ms, _expire_at_ms, _compound_key}} ->
          LMDB.delete_history_index_entry(path, key)
          []

        :error ->
          []
      end
    end)
  end
end
