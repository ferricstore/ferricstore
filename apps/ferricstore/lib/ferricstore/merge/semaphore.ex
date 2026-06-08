defmodule Ferricstore.Merge.Semaphore do
  @moduledoc """
  Node-level semaphore that limits concurrent merge operations.

  Only one shard merge may run at a time across the entire node. This prevents
  merge I/O from multiple shards competing for the same NVMe bandwidth and
  PCIe bus, which would cause both merges to run slower than a single
  sequential merge.

  The semaphore uses a simple acquire/release model backed by a GenServer.
  The caller must release the semaphore after the merge completes (or fails).
  To guard against callers that crash without releasing, the semaphore monitors
  the acquiring process and auto-releases on DOWN.

  ## Usage

      case Ferricstore.Merge.Semaphore.acquire(shard_index) do
        :ok ->
          try do
            do_merge(...)
          after
            Ferricstore.Merge.Semaphore.release(shard_index)
          end
        {:busy, holder_shard} ->
          # Another shard is merging, try later.
          :ok
      end
  """

  use GenServer

  require Logger

  @type state :: %{
          holder: {non_neg_integer(), pid()} | nil,
          monitor_ref: reference() | nil
        }

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts the merge semaphore GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, [], name: name)
  end

  @doc """
  Attempts to acquire the merge semaphore for the given shard.

  Returns `:ok` if the semaphore was acquired, or `{:busy, holder_shard_index}`
  if another shard currently holds it.
  """
  @spec acquire(non_neg_integer(), GenServer.server()) :: :ok | {:busy, non_neg_integer()}
  def acquire(shard_index, server \\ __MODULE__) do
    GenServer.call(server, {:acquire, shard_index})
  end

  @doc """
  Releases the merge semaphore. Must be called by the same shard that acquired it.

  Returns `:ok` on success, or `{:error, :not_holder}` if the caller is not
  the current holder.
  """
  @spec release(non_neg_integer(), GenServer.server()) :: :ok | {:error, :not_holder}
  def release(shard_index, server \\ __MODULE__) do
    GenServer.call(server, {:release, shard_index})
  end

  @doc """
  Returns the current state of the semaphore for observability.

  Returns `{:held, shard_index}` if a merge is in progress, or `:free`.
  """
  @spec status(GenServer.server()) :: :free | {:held, non_neg_integer()}
  def status(server \\ __MODULE__) do
    GenServer.call(server, :status)
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(_opts) do
    {:ok, %{holder: nil, monitor_ref: nil}}
  end

  @impl true
  def handle_call({:acquire, shard_index}, {caller_pid, _tag}, %{holder: nil} = state) do
    ref = Process.monitor(caller_pid)

    {:reply, :ok, %{state | holder: {shard_index, caller_pid}, monitor_ref: ref}}
  end

  def handle_call({:acquire, _shard_index}, _from, %{holder: {held_shard, _pid}} = state) do
    {:reply, {:busy, held_shard}, state}
  end

  def handle_call({:release, shard_index}, _from, %{holder: {shard_index, _pid}} = state) do
    if state.monitor_ref, do: Process.demonitor(state.monitor_ref, [:flush])
    {:reply, :ok, %{state | holder: nil, monitor_ref: nil}}
  end

  def handle_call({:release, _shard_index}, _from, state) do
    {:reply, {:error, :not_holder}, state}
  end

  def handle_call(:status, _from, %{holder: nil} = state) do
    {:reply, :free, state}
  end

  def handle_call(:status, _from, %{holder: {shard_index, _pid}} = state) do
    {:reply, {:held, shard_index}, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{monitor_ref: ref} = state) do
    Logger.warning("Merge semaphore: holder process crashed, auto-releasing semaphore")

    {:noreply, %{state | holder: nil, monitor_ref: nil}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end
end
