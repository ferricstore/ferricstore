defmodule Ferricstore.FetchOrCompute do
  @moduledoc """
  GenServer managing compute locks for cache-aside with stampede protection.

  When a cache miss occurs, only one client across the cluster is allowed to
  compute the value (the "computer"). All other clients requesting the same
  key block as "waiters" until the computer delivers the result or an error.

  ## Cluster-wide stampede protection

  Stampede protection uses a Raft-replicated distributed lock keyed on
  `"__foc_lock__:" <> key`. Because `Router.lock/4` is forced through the
  quorum path (see `Router.always_quorum?/1`), exactly one caller across the
  whole cluster wins the lock and receives `{:compute, hint}`. All other
  callers — local or remote — fall into a poll-wait loop on `Router.get/2`
  until the computer publishes the value (also Raft-replicated) or the
  compute_timeout expires.

  ## Local fast-path waiter coordination

  Within a single node we still maintain a local ETS table of waiters. When
  a caller on the same node sees an existing local lock, it parks as a
  waiter and is woken directly via `GenServer.reply/2` when the computer
  calls `fetch_or_compute_result/3` on this node. This avoids the polling
  overhead for same-node waiters; cross-node waiters poll.

  ## Timeout handling

  If the computer does not publish a result within `compute_timeout_ms`
  (default 30 seconds), waiters' polling loops give up with
  `{:error, :timeout}`. The Raft lock has its own TTL (also
  `compute_timeout_ms`) so it auto-expires and the next caller can retry.
  """

  use GenServer

  alias Ferricstore.HLC
  alias Ferricstore.Store.Router

  require Logger

  @table :ferricstore_compute_locks
  @default_compute_timeout_ms 30_000
  @poll_interval_ms 50

  defp lock_key(key), do: "__foc_lock__:" <> key

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Attempts to fetch a key, or initiates a cluster-wide compute lock.

  Returns:
    * `{:hit, value}` — cache hit
    * `{:compute, hint}` — caller won the lock and must compute the value,
      then call `fetch_or_compute_result/3`
    * `{:ok, value}` — caller waited and received the result from another node
    * `{:error, reason}` — compute failed or timed out
  """
  @spec fetch_or_compute(binary(), pos_integer(), binary()) ::
          {:hit, binary()} | {:compute, binary()} | {:ok, binary()} | {:error, term()}
  def fetch_or_compute(key, ttl_ms, hint) do
    GenServer.call(__MODULE__, {:fetch_or_compute, key, ttl_ms, hint, self()}, :infinity)
  end

  @doc """
  Delivers the computed result for a key.

  Stores the value via Router (Raft-replicated to all nodes), releases the
  distributed lock, and wakes all local waiters.
  """
  @spec fetch_or_compute_result(binary(), binary(), non_neg_integer()) :: :ok | {:error, term()}
  def fetch_or_compute_result(key, value, ttl_ms) do
    GenServer.call(__MODULE__, {:fetch_or_compute_result, key, value, ttl_ms})
  end

  @doc """
  Reports a compute error. Releases the lock, wakes local waiters with the error.
  """
  @spec fetch_or_compute_error(binary(), binary()) :: :ok
  def fetch_or_compute_error(key, error_msg) do
    GenServer.call(__MODULE__, {:fetch_or_compute_error, key, error_msg})
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(opts) do
    timeout_ms = Keyword.get(opts, :compute_timeout_ms, @default_compute_timeout_ms)
    _table = :ets.new(@table, [:set, :named_table, :protected])
    schedule_sweep(timeout_ms)
    {:ok, %{compute_timeout_ms: timeout_ms}}
  end

  @impl true
  def handle_call({:fetch_or_compute, key, _ttl_ms, hint, caller_pid}, from, state) do
    ctx = FerricStore.Instance.get(:default)

    case Router.get(ctx, key) do
      nil ->
        # Cache miss. Determine if there is already a local computer on this
        # node — if so, just park as a waiter and let the local computer wake
        # us up when it publishes.
        case :ets.lookup(@table, key) do
          [{^key, computer_pid, waiters, started_at, lock_hint, compute_id}] ->
            new_waiters = waiters ++ [{from, caller_pid}]

            :ets.insert(
              @table,
              {key, computer_pid, new_waiters, started_at, lock_hint, compute_id}
            )

            {:noreply, state}

          [] ->
            # No local computer. Try to acquire the cluster-wide Raft lock.
            compute_id = generate_compute_id()
            timeout_ms = state.compute_timeout_ms

            case Router.lock(ctx, lock_key(key), compute_id, timeout_ms) do
              :ok ->
                # We won the lock. Record locally and tell caller to compute.
                now = System.monotonic_time(:millisecond)
                :ets.insert(@table, {key, caller_pid, [], now, hint, compute_id})
                {:reply, {:compute, hint}, state}

              {:error, _held_by_other} ->
                # Another node owns the lock. Spawn a poller that will reply
                # to this caller when the value appears (or on timeout).
                spawn_poller(from, key, timeout_ms)
                {:noreply, state}
            end
        end

      value ->
        {:reply, {:hit, value}, state}
    end
  end

  def handle_call({:fetch_or_compute_result, key, value, ttl_ms}, _from, state) do
    expire_at_ms =
      if ttl_ms > 0 do
        HLC.now_ms() + ttl_ms
      else
        0
      end

    ctx = FerricStore.Instance.get(:default)
    write_result = Router.put(ctx, key, value, expire_at_ms)

    # Wake local waiters and release the cluster-wide lock.
    case :ets.lookup(@table, key) do
      [{^key, _computer_pid, waiters, _started, _hint, compute_id}] ->
        reply =
          case write_result do
            :ok -> {:ok, value}
            {:error, _} = error -> error
          end

        Enum.each(waiters, fn {waiter_from, _pid} -> GenServer.reply(waiter_from, reply) end)

        _ = Router.unlock(ctx, lock_key(key), compute_id)
        :ets.delete(@table, key)

      [] ->
        # No local lock — likely the computer is on another node and we are
        # being called as a courtesy. Nothing local to clean up.
        :ok
    end

    {:reply, write_result, state}
  end

  def handle_call({:fetch_or_compute_error, key, error_msg}, _from, state) do
    ctx = FerricStore.Instance.get(:default)

    case :ets.lookup(@table, key) do
      [{^key, _computer_pid, waiters, _started, _hint, compute_id}] ->
        Enum.each(waiters, fn {waiter_from, _pid} ->
          GenServer.reply(waiter_from, {:error, error_msg})
        end)

        _ = Router.unlock(ctx, lock_key(key), compute_id)
        :ets.delete(@table, key)

      [] ->
        :ok
    end

    {:reply, :ok, state}
  end

  # -------------------------------------------------------------------
  # Timeout sweep — clears stale local lock entries whose computer never
  # delivered. The Raft lock has its own TTL so it will auto-expire.
  # -------------------------------------------------------------------

  @impl true
  def handle_info(:sweep_timeouts, state) do
    now = System.monotonic_time(:millisecond)
    timeout_ms = state.compute_timeout_ms

    :ets.tab2list(@table)
    |> Enum.each(fn {key, _computer_pid, waiters, started_at, hint, _compute_id} ->
      if now - started_at > timeout_ms, do: promote_or_clear(key, waiters, hint)
    end)

    schedule_sweep(timeout_ms)
    {:noreply, state}
  end

  # -------------------------------------------------------------------
  # Private
  # -------------------------------------------------------------------

  defp generate_compute_id do
    rand = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    "#{node()}:#{rand}"
  end

  # Poll Router.get/2 in a separate process so the GenServer is not blocked.
  # Replies to `from` with {:ok, value} when the value appears, or
  # {:error, :timeout} on expiry.
  defp spawn_poller(from, key, timeout_ms) do
    spawn(fn -> poll_loop(from, key, timeout_ms) end)
  end

  defp poll_loop(from, _key, remaining_ms) when remaining_ms <= 0 do
    GenServer.reply(from, {:error, :timeout})
  end

  defp poll_loop(from, key, remaining_ms) do
    ctx = FerricStore.Instance.get(:default)

    case Router.get(ctx, key) do
      nil ->
        Process.sleep(@poll_interval_ms)
        poll_loop(from, key, remaining_ms - @poll_interval_ms)

      value ->
        GenServer.reply(from, {:ok, value})
    end
  end

  defp promote_or_clear(key, [{next_from, next_pid} | rest_waiters], hint) do
    new_started = System.monotonic_time(:millisecond)
    compute_id = generate_compute_id()
    :ets.insert(@table, {key, next_pid, rest_waiters, new_started, hint, compute_id})
    GenServer.reply(next_from, {:compute, hint})
  end

  defp promote_or_clear(key, [], _hint), do: :ets.delete(@table, key)

  defp schedule_sweep(timeout_ms) do
    interval = max(div(timeout_ms, 2), 500)
    Process.send_after(self(), :sweep_timeouts, interval)
  end
end
