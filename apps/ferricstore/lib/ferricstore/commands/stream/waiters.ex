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
    Ferricstore.Commands.Stream.WaiterCleanup.cleanup(pid)
  end

  @spec count(binary()) :: non_neg_integer()
  def count(stream_key) do
    ensure_table()
    :ets.match(@stream_waiters_table, {stream_key, :_, :_, :_}) |> length()
  end

  @spec snapshot(non_neg_integer()) :: [map()]
  def snapshot(limit \\ 100) do
    ensure_table()

    @stream_waiters_table
    |> :ets.tab2list()
    |> Enum.group_by(fn {stream_key, _pid, _last_seen_id, _registered_at} -> stream_key end)
    |> Enum.map(fn {stream_key, entries} ->
      %{
        key: stream_key,
        waiters: length(entries),
        oldest_wait_us:
          entries
          |> Enum.map(fn {_key, _pid, _last_seen_id, registered_at} ->
            System.monotonic_time(:microsecond) - registered_at
          end)
          |> Enum.max(fn -> 0 end),
        last_seen_id:
          entries
          |> List.first()
          |> case do
            {_key, _pid, last_seen_id, _registered_at} -> last_seen_id
            _other -> nil
          end
      }
    end)
    |> Enum.sort_by(& &1.waiters, :desc)
    |> Enum.take(max(limit, 0))
  rescue
    _ -> []
  catch
    :exit, _ -> []
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
