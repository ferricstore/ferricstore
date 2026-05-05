defmodule FerricStore.BatchWorker do
  @moduledoc """
  Persistent process for erpc batch operations.

  Instead of spawning a new process per `erpc.call`, clients start a
  long-lived worker on the remote node and send batches via
  `GenServer.call/2` over Erlang distribution. This eliminates
  per-call process spawn overhead and distribution codec setup.

  No Ranch, no TCP, no RESP parsing -- just distribution to Router.

  ## Usage (from remote node)

      # Start a worker on the FerricStore node (once per client process)
      {:ok, pid} = :erpc.call(node, FerricStore.BatchWorker, :start, [])

      # Send batches -- reuses the same process
      values = GenServer.call(pid, {:batch_get, keys})
      results = GenServer.call(pid, {:batch_set, kv_pairs})
      results = GenServer.call(pid, {:batch_mixed, pipe_commands})

      # When done
      GenServer.stop(pid)

  """

  use GenServer

  @spec start() :: GenServer.on_start()
  def start do
    GenServer.start(__MODULE__, [])
  end

  @impl true
  def init([]) do
    case :persistent_term.get({FerricStore.Instance, :default}, nil) do
      nil -> {:stop, :instance_not_initialized}
      ctx -> {:ok, %{ctx: ctx}}
    end
  end

  @impl true
  def handle_call({:batch_get, keys}, _from, state) do
    result = Ferricstore.Store.Router.batch_get(state.ctx, keys)
    {:reply, result, state}
  end

  def handle_call({:batch_set, kv_pairs}, _from, state) do
    result = do_batch_set(state.ctx, kv_pairs)
    {:reply, result, state}
  end

  def handle_call({:batch_mixed, commands}, _from, state) do
    pipe = %FerricStore.Pipe{commands: Enum.reverse(commands)}
    result = FerricStore.Pipe.execute(pipe)
    {:reply, result, state}
  end

  defp do_batch_set(ctx, kv_pairs) do
    Ferricstore.Store.Router.batch_quorum_put(ctx, kv_pairs)
  end
end
