defmodule Ferricstore.Store.BitcaskCheckpointer do
  @moduledoc """
  Per-shard background fsync for Bitcask data files.

  Replaces the per-apply `v2_fsync` in `StateMachine.flush_pending_writes` and
  the old shard-level `fsync_needed` deferred fsync timer. One shared
  mechanism, one shared flag (atomics on the Instance), covering all write
  paths (Raft state machine + async BitcaskWriter).

  ## Correctness

  Ra WAL is the source of truth for client-visible durability. Writes hit
  Bitcask data files via `v2_append_batch_nosync` (page cache only). On a
  crash, the Ra log replays any post-checkpoint entries and rebuilds the
  Bitcask state exactly — no acknowledged data is lost.

  The checkpointer's job is to move data from page cache to disk on a
  predictable cadence, bounding replay time after kernel panic.

  ## Algorithm

      every checkpoint_interval_ms:
        if :atomics.get(checkpoint_flags, idx+1) == 1:
          {_fid, active_path, _sp} = ActiveFile.get(idx)
          :atomics.put(checkpoint_in_flight, idx+1, 1)
          :atomics.put(checkpoint_flags, idx+1, 0)
          NIF.v2_fsync_async(self(), corr_id, active_path)
        else: skip (idle shard — no syscalls)

  The in-flight marker is set before clearing the dirty flag. That avoids
  a false-clean window where Raft could release log entries while Bitcask
  bytes are still only in page cache. A writer that arrives during fsync
  re-sets the dirty flag, so the next tick picks it up. The current fsync
  may miss bytes from that concurrent write, which is fine because Ra WAL
  is authoritative.

  On fsync error (disk full, I/O error), we re-set the flag so the next
  tick retries, and raise DiskPressure to shed writes.

  ## Configuration

    * `:checkpoint_interval_ms` (default 10_000 = 10s) — how often to
      check the flag. Ra WAL is fdatasync'd per batch and is the source
      of truth for acknowledged writes, so a large interval is safe:
      on kernel panic we replay up to one interval's worth of Ra log
      entries and rebuild Bitcask exactly. Short intervals mean more
      fsync syscalls per shard for no durability gain.
  """
  use GenServer

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.ActiveFile
  alias Ferricstore.Store.DiskPressure

  require Logger

  @default_interval_ms 10_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    index = Keyword.fetch!(opts, :index)
    name = Keyword.get(opts, :name, process_name(index, Keyword.get(opts, :instance_ctx)))
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Canonical process name for the checkpointer of a given shard."
  @spec process_name(non_neg_integer(), map() | nil) :: atom()
  def process_name(index, nil), do: :"ferricstore_checkpointer_#{index}"
  def process_name(index, %{name: inst}), do: :"ferricstore_checkpointer_#{inst}_#{index}"

  @doc """
  Forces a synchronous fsync of the shard's active file right now.
  Used by graceful shutdown (see design doc §shutdown ordering) and by
  tests. Bypasses the async path and clears the dirty flag on success.
  """
  @spec sync_now(pid() | atom()) :: :ok | {:error, term()}
  def sync_now(server) do
    GenServer.call(server, :sync_now, 30_000)
  end

  @impl true
  def init(opts) do
    index = Keyword.fetch!(opts, :index)
    ctx = Keyword.get(opts, :instance_ctx)

    interval_ms =
      Keyword.get(opts, :checkpoint_interval_ms) ||
        Application.get_env(:ferricstore, :checkpoint_interval_ms, @default_interval_ms)

    # Trap exits so `terminate/2` runs on graceful shutdown and we can
    # synchronously fsync the active file before the supervisor returns.
    Process.flag(:trap_exit, true)

    state = %{
      index: index,
      instance_ctx: ctx,
      interval_ms: interval_ms,
      in_flight?: false,
      next_corr_id: 1,
      current_corr_id: nil
    }

    schedule_tick(state)
    {:ok, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Graceful shutdown: if the shard is dirty, synchronously fsync the
    # active file so no post-checkpoint writes are lost from page cache.
    # Emits a `:shutdown_sync` telemetry event so tests (and operators)
    # can observe that the shutdown barrier actually fired.
    ctx = state.instance_ctx
    flag_idx = state.index + 1

    dirty? = ctx && :atomics.get(ctx.checkpoint_flags, flag_idx) == 1
    in_flight? = state.in_flight? or checkpoint_in_flight?(ctx, flag_idx)
    checkpoint_needed? = dirty? or in_flight?

    result =
      cond do
        in_flight? ->
          if ctx do
            :atomics.put(ctx.checkpoint_flags, flag_idx, 1)
            mark_checkpoint_in_flight(ctx, state.index, 0)
          end

          :in_flight_retry

        dirty? ->
          case ActiveFile.get(ctx, state.index) do
            {_fid, active_path, _sp} ->
              r = NIF.v2_fsync(active_path)
              if r == :ok, do: :atomics.put(ctx.checkpoint_flags, flag_idx, 0)
              r
          end

        true ->
          :clean
      end

    :telemetry.execute(
      [:ferricstore, :bitcask, :checkpoint_shutdown],
      %{shard_index: state.index},
      %{dirty?: checkpoint_needed?, result: result}
    )

    :ok
  rescue
    # ActiveFile entry may be missing if the shard is gone. Not fatal —
    # Ra WAL replay covers us. Emit telemetry so observers see the skip.
    exception ->
      :telemetry.execute(
        [:ferricstore, :bitcask, :checkpoint_shutdown],
        %{shard_index: state.index},
        %{dirty?: false, result: {:error, Exception.message(exception)}}
      )

      :ok
  end

  @impl true
  def handle_info(:tick, state) do
    state =
      if state.in_flight? do
        # Previous fsync still pending — skip this tick. The flag remains
        # set (or may have been re-set by a writer). Next tick re-checks
        # after the in-flight completes.
        state
      else
        maybe_fire_fsync(state)
      end

    schedule_tick(state)
    {:noreply, state}
  end

  # Tokio async-fsync completion.
  def handle_info({:tokio_complete, corr_id, :ok, _}, %{current_corr_id: corr_id} = state) do
    mark_checkpoint_in_flight(state.instance_ctx, state.index, 0)

    :telemetry.execute(
      [:ferricstore, :bitcask, :checkpoint],
      %{shard_index: state.index},
      %{status: :ok}
    )

    {:noreply, %{state | in_flight?: false, current_corr_id: nil}}
  end

  def handle_info({:tokio_complete, corr_id, :error, reason}, %{current_corr_id: corr_id} = state) do
    mark_checkpoint_in_flight(state.instance_ctx, state.index, 0)

    # "No such file or directory" = the active file was wiped (test
    # cleanup, shard shutdown, rotation race). Don't raise disk pressure
    # — the next tick will see the new ActiveFile entry. Just drop the
    # tick silently.
    enoent? =
      is_binary(reason) and String.contains?(reason, "No such file or directory")

    unless enoent? do
      if state.instance_ctx do
        :atomics.put(state.instance_ctx.checkpoint_flags, state.index + 1, 1)
        DiskPressure.set(state.instance_ctx, state.index)
      end

      :telemetry.execute(
        [:ferricstore, :bitcask, :checkpoint],
        %{shard_index: state.index},
        %{status: :error, reason: reason}
      )

      Logger.error("BitcaskCheckpointer shard=#{state.index}: fsync failed: #{inspect(reason)}")
    end

    {:noreply, %{state | in_flight?: false, current_corr_id: nil}}
  end

  # Ignore tokio_complete messages for stale correlation ids (e.g. from a
  # previous run or a sync_now). The state machine branch above keys on
  # `current_corr_id`; anything else falls through here.
  def handle_info({:tokio_complete, _corr, _status, _}, state), do: {:noreply, state}

  # We trap_exit so terminate/2 runs the shutdown fsync on graceful stop.
  # As a side effect we must explicitly shut down when a linker process
  # dies — otherwise the checkpointer lingers after the test process
  # that called `start_link` exits.
  def handle_info({:EXIT, _from, reason}, state) do
    {:stop, reason, state}
  end

  @impl true
  def handle_call(:sync_now, _from, state) do
    ctx = state.instance_ctx
    flag_idx = state.index + 1

    reply =
      case ActiveFile.get(ctx, state.index) do
        {_fid, active_path, _sp} ->
          mark_checkpoint_in_flight(ctx, state.index, 1)
          if ctx, do: :atomics.put(ctx.checkpoint_flags, flag_idx, 0)

          try do
            case NIF.v2_fsync(active_path) do
              :ok ->
                :ok

              {:error, reason} = err ->
                if ctx, do: :atomics.put(ctx.checkpoint_flags, flag_idx, 1)

                Logger.error(
                  "BitcaskCheckpointer shard=#{state.index}: sync_now failed: #{inspect(reason)}"
                )

                err
            end
          after
            mark_checkpoint_in_flight(ctx, state.index, 0)
          end
      end

    {:reply, reply, state}
  rescue
    _ -> {:reply, {:error, :not_initialized}, state}
  end

  # -------------------------------------------------------------------
  # Internals
  # -------------------------------------------------------------------

  defp schedule_tick(%{interval_ms: ms}) do
    Process.send_after(self(), :tick, ms)
  end

  defp maybe_fire_fsync(%{instance_ctx: nil} = state), do: state

  defp maybe_fire_fsync(state) do
    flag_idx = state.index + 1

    if :atomics.get(state.instance_ctx.checkpoint_flags, flag_idx) == 1 do
      case ActiveFile.get(state.instance_ctx, state.index) do
        {_fid, active_path, _sp} ->
          corr_id = state.next_corr_id
          mark_checkpoint_in_flight(state.instance_ctx, state.index, 1)
          :atomics.put(state.instance_ctx.checkpoint_flags, flag_idx, 0)

          case fsync_async(state.instance_ctx, self(), corr_id, active_path) do
            :ok ->
              %{state | in_flight?: true, next_corr_id: corr_id + 1, current_corr_id: corr_id}

            {:error, reason} ->
              checkpoint_submit_failed(state, reason)
          end
      end
    else
      state
    end
  rescue
    exception ->
      :atomics.put(state.instance_ctx.checkpoint_flags, state.index + 1, 1)
      mark_checkpoint_in_flight(state.instance_ctx, state.index, 0)

      :telemetry.execute(
        [:ferricstore, :bitcask, :checkpoint],
        %{shard_index: state.index},
        %{status: :error, reason: Exception.message(exception)}
      )

      state
  end

  defp fsync_async(ctx, caller, corr_id, active_path) do
    case Map.get(ctx, :fsync_async) do
      fun when is_function(fun, 3) -> fun.(caller, corr_id, active_path)
      _ -> NIF.v2_fsync_async(caller, corr_id, active_path)
    end
  end

  defp checkpoint_submit_failed(state, reason) do
    :atomics.put(state.instance_ctx.checkpoint_flags, state.index + 1, 1)
    mark_checkpoint_in_flight(state.instance_ctx, state.index, 0)
    DiskPressure.set(state.instance_ctx, state.index)

    :telemetry.execute(
      [:ferricstore, :bitcask, :checkpoint],
      %{shard_index: state.index},
      %{status: :error, reason: reason}
    )

    Logger.error(
      "BitcaskCheckpointer shard=#{state.index}: fsync submit failed: #{inspect(reason)}"
    )

    state
  end

  defp mark_checkpoint_in_flight(nil, _index, _value), do: :ok

  defp mark_checkpoint_in_flight(ctx, index, value) do
    case Map.get(ctx, :checkpoint_in_flight) do
      nil -> :ok
      checkpoint_in_flight -> :atomics.put(checkpoint_in_flight, index + 1, value)
    end
  end

  defp checkpoint_in_flight?(nil, _flag_idx), do: false

  defp checkpoint_in_flight?(ctx, flag_idx) do
    case Map.get(ctx, :checkpoint_in_flight) do
      nil ->
        false

      checkpoint_in_flight ->
        flag_idx <= :atomics.info(checkpoint_in_flight).size and
          :atomics.get(checkpoint_in_flight, flag_idx) == 1
    end
  rescue
    _ -> false
  end
end
