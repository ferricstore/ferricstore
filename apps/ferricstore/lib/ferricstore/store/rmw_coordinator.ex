defmodule Ferricstore.Store.RmwCoordinator do
  @moduledoc """
  Per-shard fallback for async read-modify-write (RMW) commands under
  contention.

  See `docs/async-rmw-design.md` for the full design. In short:

  - `Router.async_rmw/4` tries `:ets.insert_new(latch_tab, {key, self()})`.
    If it wins the latch, it runs the RMW inline in the caller's process
    (~15μs p50). Fast path.
  - If the latch is already held, `async_rmw` falls through here and
    sends `{:rmw, ctx, cmd}` to the shard worker. The caller context is
    part of the message because the coordinator name is global per shard;
    without it the worker would silently operate on the default instance.
    The worker processes RMW commands serially from its mailbox (FIFO).
    This is the slow path under heavy same-key contention, but it never
    loses updates and callers sleep on `receive` while queued (zero CPU).

  The coordinator keeps a FIFO queue per key and starts at most one waiter
  task per key. That keeps same-key RMW serialized without letting a latch
  wait for key A block unrelated key B on the same shard.

  Periodic latch sweep (every 5s) removes entries whose holder pid is
  dead — recovery path for a caller that crashed between `insert_new`
  and `ets.take`.
  """

  use GenServer

  require Logger

  @sweep_interval_ms 5_000

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the coordinator for the given shard index.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    idx = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, idx, name: name(idx))
  end

  @doc """
  Registered process name for the coordinator at the given shard index.
  """
  @spec name(non_neg_integer()) :: atom()
  def name(idx), do: :"Ferricstore.Store.RmwCoordinator.#{idx}"

  @doc """
  Execute an RMW command via the worker (fallback path).

  Callers only reach this when they lost the latch CAS in their fast
  path. Returns the command's natural result (e.g. `{:ok, integer}` for
  INCR, `old_value_or_nil` for GETSET/GETDEL, the push's new length for
  LPUSH, etc.).

  Accepted command shapes:
    - Plain RMW: `{:incr, k, d}`, `{:incr_float, k, d}`, `{:append, k, s}`,
      `{:getset, k, v}`, `{:getdel, k}`, `{:getex, k, e}`, `{:setrange, k, o, v}`.
    - List ops: `{:list_op, k, operation}`, `{:list_op_lmove, src, dst, from, to}`.

  Timeouts and worker crashes propagate as `:exit` to the caller;
  the caller's `async_*` function catches them and returns `{:error, msg}`.
  """
  @spec execute(non_neg_integer(), tuple()) :: term()
  def execute(idx, cmd), do: GenServer.call(name(idx), {:rmw, cmd}, 10_000)

  @spec execute(non_neg_integer(), FerricStore.Instance.t(), tuple()) :: term()
  def execute(idx, %FerricStore.Instance{} = ctx, cmd),
    do: GenServer.call(name(idx), {:rmw, ctx, cmd}, 10_000)

  @doc """
  Force a sweep of stale latches for this shard. Intended for tests.
  """
  @spec sweep_latches(non_neg_integer()) :: :ok
  def sweep_latches(idx), do: GenServer.call(name(idx), :sweep_latches_now, 5_000)

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(idx) do
    # The instance context may not be populated yet at application start
    # order. Defer lookup until first use, but remember the shard index.
    Process.send_after(self(), :sweep_latches, @sweep_interval_ms)
    {:ok, %{idx: idx, queues: %{}, running: MapSet.new()}}
  end

  @impl true
  def handle_call({:rmw, cmd}, from, state) do
    ctx = FerricStore.Instance.get(:default)

    case ctx do
      nil ->
        {:reply, {:error, "ERR instance not initialized"}, state}

      _ ->
        {:noreply, enqueue_rmw(state, from, ctx, cmd)}
    end
  end

  def handle_call({:rmw, %FerricStore.Instance{} = ctx, cmd}, from, state) do
    {:noreply, enqueue_rmw(state, from, ctx, cmd)}
  end

  def handle_call(:sweep_latches_now, _from, state) do
    case FerricStore.Instance.get(:default) do
      nil -> :ok
      ctx -> do_sweep(elem(ctx.latch_refs, state.idx))
    end

    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:sweep_latches, state) do
    case FerricStore.Instance.get(:default) do
      nil -> :ok
      ctx -> do_sweep(elem(ctx.latch_refs, state.idx))
    end

    Process.send_after(self(), :sweep_latches, @sweep_interval_ms)
    {:noreply, state}
  end

  def handle_info({:rmw_finished, queue_key, from, result}, state) do
    GenServer.reply(from, result)

    state =
      %{state | running: MapSet.delete(state.running, queue_key)}
      |> maybe_start_next(queue_key)

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp dispatch_inline(ctx, idx, {:list_op, _key, _op} = cmd),
    do: Ferricstore.Store.Router.execute_list_op_inline(ctx, idx, cmd)

  defp dispatch_inline(ctx, idx, {:list_op_lmove, _src, _dst, _from, _to} = cmd),
    do: Ferricstore.Store.Router.execute_list_op_inline(ctx, idx, cmd)

  defp dispatch_inline(ctx, idx, cmd),
    do: Ferricstore.Store.Router.execute_rmw_inline(ctx, idx, cmd)

  defp enqueue_rmw(state, from, ctx, cmd) do
    queue_key = queue_key(ctx, cmd)
    queue = Map.get(state.queues, queue_key, :queue.new())
    queues = Map.put(state.queues, queue_key, :queue.in({from, ctx, cmd}, queue))

    %{state | queues: queues}
    |> maybe_start_next(queue_key)
  end

  defp maybe_start_next(state, queue_key) do
    if MapSet.member?(state.running, queue_key) do
      state
    else
      case Map.fetch(state.queues, queue_key) do
        {:ok, queue} ->
          case :queue.out(queue) do
            {{:value, {from, ctx, cmd}}, rest} ->
              queues =
                if :queue.is_empty(rest),
                  do: Map.delete(state.queues, queue_key),
                  else: Map.put(state.queues, queue_key, rest)

              start_key_worker(self(), state.idx, queue_key, from, ctx, cmd)
              %{state | queues: queues, running: MapSet.put(state.running, queue_key)}

            {:empty, _} ->
              %{state | queues: Map.delete(state.queues, queue_key)}
          end

        :error ->
          state
      end
    end
  end

  defp start_key_worker(parent, idx, queue_key, from, ctx, cmd) do
    Task.start(fn ->
      send(parent, {:rmw_finished, queue_key, from, run_rmw(ctx, idx, cmd)})
    end)
  end

  defp run_rmw(ctx, idx, cmd) do
    latch_tab = elem(ctx.latch_refs, idx)
    keys = latch_keys_of(cmd)

    try do
      Enum.each(keys, fn key -> wait_for_latch(latch_tab, key) end)

      try do
        result = dispatch_inline(ctx, idx, cmd)
        :telemetry.execute([:ferricstore, :rmw, :worker], %{}, %{shard_index: idx})
        result
      after
        release_latches(latch_tab, keys)
      end
    catch
      kind, reason ->
        Logger.error("RmwCoordinator: worker task failed: #{inspect({kind, reason})}")
        {:error, "ERR RMW worker crashed"}
    end
  end

  # Scheduling must include the instance name. The coordinator process is
  # global per shard index, but each instance has its own ETS/keydir/latch
  # tables. Serializing by raw user key would make unrelated tenants with
  # the same key block each other under RMW contention.
  defp queue_key(%FerricStore.Instance{name: name}, {:list_op_lmove, src, dst, _from, _to}) do
    {name, {:multi_key, sorted_unique_keys([src, dst])}}
  end

  defp queue_key(%FerricStore.Instance{name: name}, cmd), do: {name, key_of(cmd)}

  # Acquire the per-key latch. The coordinator starts at most one waiter task
  # per key, so same-key contention has no thundering herd.
  #
  # If the current holder is dead (crashed mid-RMW), take over the latch
  # immediately instead of waiting for the periodic sweeper. This handles
  # the "caller crashed before `:ets.take`" recovery path without a 5s
  # stall on the first RMW to the orphaned key.
  defp wait_for_latch(tab, key) do
    case :ets.insert_new(tab, {key, self()}) do
      true ->
        :ok

      false ->
        case :ets.lookup(tab, key) do
          [{^key, holder}] when is_pid(holder) ->
            if Process.alive?(holder) do
              :erlang.yield()
              wait_for_latch(tab, key)
            else
              # Orphaned latch — take over, but only delete THIS specific
              # dead holder's entry. Using `:ets.delete/2` unconditionally
              # here would race with a fresh legitimate acquirer and corrupt
              # the latch. select_delete matches on holder atomically.
              :ets.select_delete(tab, [{{key, holder}, [], [true]}])
              wait_for_latch(tab, key)
            end

          _ ->
            # Race: holder released between our insert_new and our lookup.
            :erlang.yield()
            wait_for_latch(tab, key)
        end
    end
  end

  defp release_latches(tab, keys) do
    Enum.each(keys, fn key ->
      :ets.select_delete(tab, [{{key, self()}, [], [true]}])
    end)
  end

  # Remove latch entries whose holder pid is dead. Called on a timer and
  # via the `:sweep_latches_now` test hook.
  defp do_sweep(tab) do
    dead =
      :ets.foldl(
        fn {key, pid}, acc ->
          if Process.alive?(pid), do: acc, else: [{key, pid} | acc]
        end,
        [],
        tab
      )

    Enum.each(dead, fn {key, pid} ->
      :ets.select_delete(tab, [{{key, pid}, [], [true]}])
    end)

    if dead != [] do
      Logger.debug("RmwCoordinator: swept #{length(dead)} stale latch entries")
    end

    :ok
  end

  defp key_of({:incr, k, _}), do: k
  defp key_of({:incr_float, k, _}), do: k
  defp key_of({:append, k, _}), do: k
  defp key_of({:getset, k, _}), do: k
  defp key_of({:getdel, k}), do: k
  defp key_of({:getex, k, _}), do: k
  defp key_of({:setrange, k, _, _}), do: k
  defp key_of({:list_op, k, _}), do: k
  defp key_of({:list_op_lmove, src_k, _dst, _from, _to}), do: src_k

  defp latch_keys_of({:list_op_lmove, src_k, dst_k, _from, _to}),
    do: sorted_unique_keys([src_k, dst_k])

  defp latch_keys_of(cmd), do: [key_of(cmd)]

  defp sorted_unique_keys(keys), do: keys |> Enum.uniq() |> Enum.sort()
end
