defmodule Ferricstore.Commands.Stream.Waiters do
  @moduledoc false

  @stream_waiters_table :ferricstore_stream_waiters

  @spec register(binary(), pid(), binary()) :: :ok
  def register(stream_key, pid, last_seen_id) do
    ensure_table()
    Ferricstore.Waiters.Monitor.track(pid)
    registered_at = System.monotonic_time(:microsecond)
    :ets.insert(@stream_waiters_table, {stream_key, pid, last_seen_id, registered_at})
    :ok
  end

  @spec unregister(binary(), pid()) :: :ok
  def unregister(stream_key, pid) do
    if :ets.whereis(@stream_waiters_table) != :undefined do
      :ets.match_delete(@stream_waiters_table, {stream_key, pid, :_, :_})
    end

    :ok
  end

  @spec cleanup(pid()) :: :ok
  def cleanup(pid) do
    if :ets.whereis(@stream_waiters_table) != :undefined do
      :ets.match_delete(@stream_waiters_table, {:_, pid, :_, :_})
    end

    :ok
  end

  @spec count(binary()) :: non_neg_integer()
  def count(stream_key) do
    ensure_table()
    :ets.match(@stream_waiters_table, {stream_key, :_, :_, :_}) |> length()
  end

  @spec notify(binary()) :: :ok
  def notify(stream_key) do
    case :ets.whereis(@stream_waiters_table) do
      :undefined ->
        :ok

      _ref ->
        entries = :ets.lookup(@stream_waiters_table, stream_key)

        Enum.each(entries, fn {_key, pid, _last_id, _reg_at} ->
          send(pid, {:stream_waiter_notify, stream_key})
        end)

        :ets.match_delete(@stream_waiters_table, {stream_key, :_, :_, :_})
        :ok
    end
  end

  defp ensure_table do
    case :ets.whereis(@stream_waiters_table) do
      :undefined ->
        try do
          :ets.new(@stream_waiters_table, [:duplicate_bag, :public, :named_table])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end
  end
end
