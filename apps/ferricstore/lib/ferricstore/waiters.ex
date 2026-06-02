defmodule Ferricstore.Waiters do
  @moduledoc """
  BEAM waiter registry for blocking list commands (BLPOP, BRPOP, BLMOVE, BLMPOP).

  Uses an ETS table (`:ferricstore_waiters`) with entries
  `{key, client_pid, deadline_ms, registered_at}`. When a list is empty, the
  client BEAM process registers itself and enters a `receive` block with an
  `after` clause matching the timeout. When another client pushes to that key,
  the push path calls `notify_push/1` which wakes the oldest waiter (FIFO by
  registration timestamp).

  The BEAM runtime handles the timeout with zero polling -- no busy wait, no
  periodic check loop. Multiple waiters on the same key are served FIFO by
  registration timestamp.

  ## ETS table

  The table is a `:duplicate_bag` keyed by the watched key. Each entry is:

      {key, pid, deadline_ms, registered_at_mono}

  - `key` -- the Redis key being watched
  - `pid` -- the client connection process
  - `deadline_ms` -- absolute monotonic deadline (0 = infinite)
  - `registered_at_mono` -- `System.monotonic_time(:microsecond)` for FIFO ordering
  """

  @table :ferricstore_waiters

  @doc """
  Initializes the waiter ETS table. Must be called once at application start.
  """
  @spec init() :: :ok
  def init do
    :ets.new(@table, [
      :duplicate_bag,
      :public,
      :named_table,
      {:read_concurrency, true},
      {:write_concurrency, :auto},
      {:decentralized_counters, true}
    ])

    Ferricstore.Waiters.Monitor.ensure_table()

    :ok
  end

  @doc """
  Registers the calling process as a waiter for `key`.

  ## Parameters

    - `key` -- the Redis key to wait on
    - `pid` -- the client connection process pid
    - `deadline_ms` -- absolute monotonic deadline in ms (0 = block forever)

  ## Returns

  `:ok`
  """
  @spec register(binary(), pid(), non_neg_integer()) :: :ok
  def register(key, pid, deadline_ms) do
    Ferricstore.Waiters.Monitor.track(pid)
    registered_at = System.monotonic_time(:microsecond)
    :ets.insert(@table, {key, pid, deadline_ms, registered_at})
    :ok
  end

  @doc """
  Unregisters a specific waiter for `key`.

  ## Parameters

    - `key` -- the Redis key
    - `pid` -- the client connection process pid
  """
  @spec unregister(binary(), pid()) :: :ok
  def unregister(key, pid) do
    # Match and delete all entries for this key+pid combination
    :ets.match_delete(@table, {key, pid, :_, :_})
    :ok
  end

  @doc """
  Notifies the oldest waiter for `key` that a value has been pushed.

  Called from the push path (LPUSH/RPUSH) when data is added to a key.
  Wakes the oldest registered waiter (FIFO) by sending `{:waiter_notify, key}`
  to its pid. Returns the notified pid, or `nil` if no waiters exist.

  ## Parameters

    - `key` -- the Redis key that received a push
  """
  @spec notify_push(binary()) :: pid() | nil
  def notify_push(key) do
    case :ets.lookup(@table, key) do
      [] ->
        nil

      entries ->
        entries
        |> Enum.sort_by(fn {_k, _p, _d, reg_at} -> reg_at end)
        |> notify_oldest_live(key)
    end
  end

  @doc """
  Notifies up to `count` oldest waiters for `key`.

  Multi-element pushes can make more than one blocked list command runnable.
  Waking at most the pushed element count avoids a thundering herd while still
  allowing each woken client to re-run the real pop atomically.
  """
  @spec notify_push(binary(), non_neg_integer()) :: [pid()]
  def notify_push(_key, count) when not is_integer(count) or count <= 0, do: []

  def notify_push(key, count) do
    case :ets.lookup(@table, key) do
      [] ->
        []

      entries ->
        entries
        |> Enum.sort_by(fn {_k, _p, _d, reg_at} -> reg_at end)
        |> notify_oldest_live_many(key, count, [])
        |> Enum.reverse()
    end
  end

  @doc """
  Removes all waiters registered by `pid`.

  Called when a client disconnects to prevent stale entries.

  ## Parameters

    - `pid` -- the disconnected client's pid
  """
  @spec cleanup(pid()) :: :ok
  def cleanup(pid) do
    :ets.match_delete(@table, {:_, pid, :_, :_})
    :ok
  end

  @doc """
  Returns the number of waiters registered for `key`.

  Useful for testing and monitoring.
  """
  @spec count(binary()) :: non_neg_integer()
  def count(key) do
    :ets.match(@table, {key, :_, :_, :_}) |> length()
  end

  @doc """
  Returns the total number of waiters across all keys.

  Useful for testing and monitoring.
  """
  @spec total_count() :: non_neg_integer()
  def total_count do
    :ets.info(@table, :size)
  end

  defp notify_oldest_live([], _key), do: nil

  defp notify_oldest_live([{key, pid, _deadline, _reg_at} | rest], key) do
    if Process.alive?(pid) do
      # Remove this waiter before notifying (prevent double-wake).
      :ets.match_delete(@table, {key, pid, :_, :_})
      send(pid, {:waiter_notify, key})
      pid
    else
      cleanup(pid)
      notify_oldest_live(rest, key)
    end
  end

  defp notify_oldest_live_many(_entries, _key, 0, acc), do: acc
  defp notify_oldest_live_many([], _key, _remaining, acc), do: acc

  defp notify_oldest_live_many([{key, pid, _deadline, _reg_at} | rest], key, remaining, acc) do
    if Process.alive?(pid) do
      :ets.match_delete(@table, {key, pid, :_, :_})
      send(pid, {:waiter_notify, key})
      notify_oldest_live_many(rest, key, remaining - 1, [pid | acc])
    else
      cleanup(pid)
      notify_oldest_live_many(rest, key, remaining, acc)
    end
  end
end
