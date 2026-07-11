defmodule Ferricstore.Flow.LMDBWriter do
  @moduledoc """
  Async Flow projection writer for LMDB.

  This module is a boundary between durable Flow truth and cold/query indexes.
  Raft + Bitcask state/history records are the source of truth. `LMDBWriter`
  consumes queued projection ops after the hot apply path and batches them into
  LMDB so history, terminal, lineage, and value-ref queries can catch up without
  blocking normal Flow writes.

  ## Performance boundary

  This is still a hot-adjacent module: enqueue cost is paid by Flow create,
  transition, terminal, retention, and history paths. Keep enqueue functions
  allocation-light and avoid extra processes/calls in request paths. Refactors
  here need DBOS/Flow benchmark comparison.
  """

  alias Ferricstore.Flow.LMDBWriter.AfterFlush
  alias Ferricstore.Flow.LMDBWriter.Control
  alias Ferricstore.Flow.LMDBWriter.EnqueueControl
  alias Ferricstore.Flow.LMDBWriter.Outbox
  alias Ferricstore.Flow.LMDBWriter.ProjectionOps

  use GenServer

  alias Ferricstore.Flow.LMDBFlushCoordinator
  alias Ferricstore.Flow.LMDBReplaySafeIndex
  alias Ferricstore.Flow.LMDBWriter.Config

  require Logger

  @doc false
  def __timer_flush_decision_for_test__(state, now)
      when is_map(state) and is_integer(now) do
    timer_flush_decision(state, now)
  end

  def start_link(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    instance_name = Config.instance_name_from_opts(opts)
    GenServer.start_link(__MODULE__, opts, name: name(instance_name, shard_index))
  end

  def enqueue(shard_index, ops) when is_integer(shard_index) and is_list(ops) do
    enqueue(:default, shard_index, ops, [])
  end

  def enqueue(shard_index, ops, after_flush)
      when is_integer(shard_index) and is_list(ops) and is_list(after_flush) do
    enqueue(:default, shard_index, ops, after_flush)
  end

  def enqueue(instance_name, shard_index, ops)
      when is_atom(instance_name) and is_integer(shard_index) and is_list(ops) do
    enqueue(instance_name, shard_index, ops, [])
  end

  def enqueue(instance_name, shard_index, ops, after_flush)
      when is_atom(instance_name) and is_integer(shard_index) and is_list(ops) and
             is_list(after_flush) do
    cond do
      ops == [] and after_flush == [] ->
        :ok

      is_pid(pid = Process.whereis(name(instance_name, shard_index))) ->
        op_count = length(ops)

        case EnqueueControl.enqueue_guard(pid, op_count) do
          :ok ->
            try do
              case GenServer.call(pid, {:enqueue, ops, after_flush}, 5_000) do
                :ok -> :ok
                {:error, _reason} = error -> error
              end
            catch
              :exit, reason ->
                writer_unavailable(:enqueue, instance_name, shard_index, reason, op_count)
            end

          {:error, reason} ->
            writer_unavailable(:enqueue, instance_name, shard_index, reason, op_count)
        end

      true ->
        writer_unavailable(:enqueue, instance_name, shard_index, :writer_not_started, length(ops))
    end
  end

  def enqueue_async(shard_index, ops) when is_integer(shard_index) and is_list(ops) do
    enqueue_async(:default, shard_index, ops, [])
  end

  def enqueue_async(shard_index, ops, after_flush)
      when is_integer(shard_index) and is_list(ops) and is_list(after_flush) do
    enqueue_async(:default, shard_index, ops, after_flush)
  end

  def enqueue_async(instance_name, shard_index, ops)
      when is_atom(instance_name) and is_integer(shard_index) and is_list(ops) do
    enqueue_async(instance_name, shard_index, ops, [])
  end

  def enqueue_async(instance_name, shard_index, ops, after_flush)
      when is_atom(instance_name) and is_integer(shard_index) and is_list(ops) and
             is_list(after_flush) do
    op_count = length(ops)

    cond do
      ops == [] and after_flush == [] ->
        :ok

      instance_suspended?(instance_name) ->
        writer_unavailable(:enqueue, instance_name, shard_index, :writer_suspended, op_count)

      is_pid(pid = Process.whereis(name(instance_name, shard_index))) ->
        case EnqueueControl.enqueue_async_guard(instance_name, shard_index, op_count) do
          :ok ->
            seq = EnqueueControl.reserve_enqueue_seq(instance_name, shard_index)
            GenServer.cast(pid, {:enqueue, seq, ops, after_flush})
            :ok

          {:error, reason} ->
            writer_unavailable(:enqueue, instance_name, shard_index, reason, op_count)
        end

      true ->
        writer_unavailable(:enqueue, instance_name, shard_index, :writer_not_started, op_count)
    end
  end

  def enqueue_projection_outbox(instance_name, shard_index, entries)
      when is_atom(instance_name) and is_integer(shard_index) and is_list(entries) do
    entries = normalize_projection_outbox_entries(entries)
    entry_count = length(entries)

    cond do
      entries == [] ->
        :ok

      instance_suspended?(instance_name) ->
        writer_unavailable(
          :projection_outbox_enqueue,
          instance_name,
          shard_index,
          :writer_suspended,
          entry_count
        )

      pid = Process.whereis(name(instance_name, shard_index)) ->
        case :ets.whereis(projection_outbox_name(instance_name, shard_index)) do
          :undefined ->
            writer_unavailable(
              :projection_outbox_enqueue,
              instance_name,
              shard_index,
              :projection_outbox_not_started,
              entry_count
            )

          tid ->
            :ets.insert(tid, projection_outbox_rows(entries))
            GenServer.cast(pid, :projection_outbox_available)
            :ok
        end

      true ->
        writer_unavailable(
          :projection_outbox_enqueue,
          instance_name,
          shard_index,
          :writer_not_started,
          entry_count
        )
    end
  end

  def mark_projection_dirty(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) do
    cond do
      instance_suspended?(instance_name) ->
        writer_unavailable(
          :projection_dirty,
          instance_name,
          shard_index,
          :writer_suspended,
          1
        )

      pid = Process.whereis(name(instance_name, shard_index)) ->
        case :ets.whereis(projection_outbox_name(instance_name, shard_index)) do
          :undefined ->
            writer_unavailable(
              :projection_dirty,
              instance_name,
              shard_index,
              :projection_outbox_not_started,
              1
            )

          tid ->
            if :ets.insert_new(tid, {:dirty, true}) do
              GenServer.cast(pid, :projection_dirty)
            end

            :ok
        end

      true ->
        writer_unavailable(
          :projection_dirty,
          instance_name,
          shard_index,
          :writer_not_started,
          1
        )
    end
  end

  def durable?(instance_ctx, shard_index, shard_data_path, index) do
    durable_index(instance_ctx, shard_index, shard_data_path) >= index
  end

  def durable_index(
        %{flow_lmdb_replay_safe_index: replay_safe_index},
        shard_index,
        _shard_data_path
      )
      when is_reference(replay_safe_index) do
    if shard_index < :atomics.info(replay_safe_index).size do
      :atomics.get(replay_safe_index, shard_index + 1)
    else
      0
    end
  rescue
    _ -> 0
  end

  def durable_index(_instance_ctx, _shard_index, shard_data_path) do
    LMDBReplaySafeIndex.read(shard_data_path)
  end

  def request(instance_ctx, shard_index, shard_data_path, index)
      when is_integer(index) and index >= 0 do
    instance_name = Config.instance_name_from_ctx(instance_ctx)
    publish_requested(instance_ctx, shard_index, index)

    cond do
      mirror_degraded?(instance_ctx, shard_index) ->
        {:error, :mirror_degraded}

      durable?(instance_ctx, shard_index, shard_data_path, index) ->
        :durable

      is_pid(writer_pid = Process.whereis(name(instance_name, shard_index))) ->
        GenServer.cast(writer_pid, {:persist_replay_safe, index})
        :requested

      true ->
        {:error, :writer_not_started}
    end
  catch
    :exit, reason -> {:error, reason}
  end

  defp mirror_degraded?(%{flow_lmdb_mirror_degraded: degraded}, shard_index)
       when is_reference(degraded) do
    shard_index < :atomics.info(degraded).size and
      :atomics.get(degraded, shard_index + 1) > 0
  rescue
    _ -> false
  end

  defp mirror_degraded?(_instance_ctx, _shard_index), do: false

  defdelegate flush_all(shard_count), to: Control
  defdelegate flush_all(shard_count, timeout), to: Control
  defdelegate flush_all(instance_name, shard_count, timeout), to: Control
  defdelegate flush(shard_index), to: Control
  defdelegate flush(shard_index_or_instance_name, timeout_or_shard_index), to: Control
  defdelegate flush(instance_name, shard_index, timeout), to: Control
  defdelegate suspend_all(shard_count), to: Control
  defdelegate suspend_all(shard_count, opts), to: Control
  defdelegate suspend_all(instance_name, shard_count, opts), to: Control
  defdelegate suspend(instance_name, shard_index), to: Control
  defdelegate suspend_without_flush(instance_name, shard_index), to: Control
  defdelegate resume_all(shard_count), to: Control
  defdelegate resume_all(instance_name, shard_count), to: Control
  defdelegate discard_all(shard_count), to: Control
  defdelegate discard_all(instance_name, shard_count), to: Control
  defdelegate discard(instance_name, shard_index), to: Control
  defdelegate prepare_snapshot_install(instance_name, shard_index), to: Control
  defdelegate resume_after_snapshot_install(instance_name, shard_index), to: Control

  def name(shard_index), do: Ferricstore.Flow.LMDBWriter.Registry.name(shard_index)

  def name(:default, shard_index),
    do: Ferricstore.Flow.LMDBWriter.Registry.name(:default, shard_index)

  def name(instance_name, shard_index) do
    Ferricstore.Flow.LMDBWriter.Registry.name(instance_name, shard_index)
  end

  def projection_outbox_name(instance_name, shard_index) do
    Ferricstore.Flow.LMDBWriter.Registry.projection_outbox_name(instance_name, shard_index)
  end

  defp ensure_projection_outbox!(instance_name, shard_index) do
    Ferricstore.Flow.LMDBWriter.Registry.ensure_projection_outbox!(instance_name, shard_index)
  end

  defp normalize_projection_outbox_entries(entries) do
    Ferricstore.Flow.LMDBWriter.Registry.normalize_projection_outbox_entries(entries)
  end

  defp projection_outbox_rows(entries) do
    Ferricstore.Flow.LMDBWriter.Registry.projection_outbox_rows(entries)
  end

  defp clear_instance_suspended(instance_name) when is_atom(instance_name) do
    Ferricstore.Flow.LMDBWriter.Registry.clear_instance_suspended(instance_name)
  end

  defp instance_suspended?(instance_name) when is_atom(instance_name),
    do: Ferricstore.Flow.LMDBWriter.Registry.instance_suspended?(instance_name)

  @impl true
  def init(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    data_dir = Keyword.fetch!(opts, :data_dir)
    instance_name = Config.instance_name_from_opts(opts)
    _outbox = ensure_projection_outbox!(instance_name, shard_index)

    if shard_index == 0 do
      clear_instance_suspended(instance_name)
    end

    lost_enqueue = EnqueueControl.lost_unprocessed_enqueue(instance_name, shard_index)
    enqueue_seq = :atomics.new(2, signed: false)
    EnqueueControl.publish_enqueue_seq(instance_name, shard_index, enqueue_seq)

    state = Config.initial_state(opts, instance_name, shard_index, data_dir, enqueue_seq)

    durable_index = LMDBReplaySafeIndex.read(state.shard_data_path)
    publish_durable(state.instance_ctx, shard_index, durable_index)

    state = %{state | durable_index: durable_index, requested_index: durable_index}

    EnqueueControl.maybe_mark_lost_enqueue(state.instance_ctx, shard_index, lost_enqueue)

    {:ok, state}
  end

  def enqueue_ops_capacity(op_count), do: EnqueueControl.enqueue_ops_capacity(op_count)

  @impl true
  def handle_cast({:enqueue, seq, ops, after_flush}, state)
      when is_integer(seq) and seq >= 0 and is_list(ops) and is_list(after_flush) do
    state =
      if state.suspended? do
        mark_mirror_degraded(state.instance_ctx, state.shard_index, :enqueue_after_suspend)
        state
      else
        enqueue_and_maybe_flush(ops, after_flush, state)
      end

    state =
      state
      |> EnqueueControl.mark_enqueue_processed(seq)
      |> EnqueueControl.maybe_reply_flush_waiters()

    {:noreply, state}
  end

  def handle_cast({:enqueue, ops, after_flush}, state) do
    if state.suspended? do
      {:noreply, state}
    else
      handle_enqueue(ops, after_flush, state)
    end
  end

  def handle_cast(:projection_outbox_available, state) do
    cond do
      writer_suspended?(state) ->
        {:noreply, state}

      true ->
        {:noreply, ensure_projection_outbox_timer(state)}
    end
  end

  def handle_cast(:projection_dirty, state) do
    if writer_suspended?(state) do
      {:noreply, state}
    else
      {:noreply, ensure_projection_outbox_timer(%{state | projection_dirty?: true})}
    end
  end

  def handle_cast({:persist_replay_safe, index}, state) when is_integer(index) and index >= 0 do
    requested_index = max(state.requested_index, index)
    publish_requested(state.instance_ctx, state.shard_index, requested_index)

    {:noreply, maybe_flush_replay_safe_request(%{state | requested_index: requested_index})}
  end

  def handle_cast(:suspend_without_flush, state) do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
    {:noreply, %{state | timer_ref: nil, suspended?: true}}
  end

  def handle_cast(:resume, state) do
    {:noreply, %{state | suspended?: false}}
  end

  @impl true
  def handle_call({:enqueue, ops, after_flush}, _from, state) do
    if writer_suspended?(state) do
      {:reply, {:error, :writer_suspended}, state}
    else
      {:reply, :ok, enqueue_without_flush(ops, after_flush, state)}
    end
  end

  def handle_call(:flush, from, state) do
    cond do
      writer_suspended?(state) ->
        {:reply, {:error, :writer_suspended}, state}

      EnqueueControl.enqueue_seq_target(state) > state.processed_enqueue_seq ->
        {:noreply, EnqueueControl.queue_flush_waiter(state, from)}

      true ->
        {state, reply} = flush_pending_with_reply(state)
        {:reply, reply, state}
    end
  end

  def handle_call(:prepare_snapshot_install, _from, state) do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
    {state, reply} = flush_pending_with_reply(%{state | timer_ref: nil})

    case reply do
      :ok -> {:reply, :ok, reset_for_snapshot_install(state, true)}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  def handle_call(:resume_after_snapshot_install, _from, state) do
    durable_index = LMDBReplaySafeIndex.read(state.shard_data_path)

    Ferricstore.Flow.LMDBWriter.Telemetry.reset_replay_safe(
      state.instance_ctx,
      state.shard_index,
      durable_index
    )

    state =
      state
      |> reset_for_snapshot_install(false)
      |> Map.put(:durable_index, durable_index)
      |> Map.put(:requested_index, durable_index)

    {:reply, :ok, state}
  end

  def handle_call(:suspend, _from, state) do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
    {state, _reply} = flush_pending_with_reply(%{state | timer_ref: nil})
    {:reply, :ok, %{state | timer_ref: nil, suspended?: true}}
  end

  def handle_call(:discard, _from, state) do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
    Outbox.clear_projection_outbox(state)

    processed = EnqueueControl.enqueue_seq_target(state)
    EnqueueControl.publish_processed_enqueue_seq(state.enqueue_seq, processed)
    Enum.each(state.flush_waiters, fn {from, _target} -> GenServer.reply(from, :ok) end)

    state = %{
      state
      | pending: [],
        pending_after_flush: [],
        count: 0,
        first_pending_at: nil,
        last_enqueue_at: nil,
        timer_ref: nil,
        projection_dirty?: false,
        processed_enqueue_seq: processed,
        processed_enqueue_gaps: MapSet.new(),
        flush_waiters: []
    }

    publish_backlog(state, 0)

    {:reply, :ok, state}
  end

  defp reset_for_snapshot_install(state, suspended?) do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
    Outbox.clear_projection_outbox(state)

    processed = EnqueueControl.enqueue_seq_target(state)
    EnqueueControl.publish_processed_enqueue_seq(state.enqueue_seq, processed)
    Enum.each(state.flush_waiters, fn {from, _target} -> GenServer.reply(from, :ok) end)

    state = %{
      state
      | pending: [],
        pending_after_flush: [],
        count: 0,
        first_pending_at: nil,
        last_enqueue_at: nil,
        timer_ref: nil,
        terminal_count_inits: MapSet.new(),
        terminal_count_cache: ProjectionOps.empty_terminal_count_cache(),
        lmdb_ready: false,
        suspended?: suspended?,
        projection_dirty?: false,
        processed_enqueue_seq: processed,
        processed_enqueue_gaps: MapSet.new(),
        flush_waiters: []
    }

    publish_backlog(state, 0)
    state
  end

  defp handle_enqueue(ops, after_flush, state) do
    {:noreply, enqueue_and_maybe_flush(ops, after_flush, state)}
  end

  def enqueue_and_maybe_flush(ops, after_flush, state) do
    now = System.monotonic_time()

    state = enqueue_ops(ops, after_flush, state, now)
    emit_backlog(state, now)

    if flush_on_max_ops?(state) do
      flush_pending(state)
    else
      state
    end
  end

  defp enqueue_without_flush(ops, after_flush, state) do
    now = System.monotonic_time()
    state = enqueue_ops(ops, after_flush, state, now)
    emit_backlog(state, now)

    if flush_on_max_ops?(state) do
      send(self(), :flush)
    end

    state
  end

  def enqueue_ops(ops, after_flush, state, now) do
    %{
      state
      | pending: prepend_reverse(ops, state.pending),
        pending_after_flush: prepend_reverse(after_flush, state.pending_after_flush),
        count: state.count + length(ops),
        first_pending_at: state.first_pending_at || now,
        last_enqueue_at: now
    }
    |> ensure_enqueue_timer()
  end

  @impl true
  def handle_info(:flush, %{suspended?: true} = state) do
    {:noreply, %{state | timer_ref: nil}}
  end

  def handle_info(:flush, state) do
    if instance_suspended?(state.instance_name) do
      {state, _reply} = flush_pending_with_reply(%{state | timer_ref: nil})
      {:noreply, %{state | timer_ref: nil, suspended?: true}}
    else
      state = %{state | timer_ref: nil}

      case maybe_defer_timer_flush(state) do
        {:defer, state} -> {:noreply, state}
        :flush -> {:noreply, flush_pending(state)}
      end
    end
  end

  def handle_info({:apply_after_flush, action}, state) do
    apply_after_flush(action)
    {:noreply, state}
  end

  defp writer_suspended?(state) do
    state.suspended? or instance_suspended?(state.instance_name)
  end

  defp ensure_timer(%{timer_ref: nil, flush_interval_ms: interval} = state) do
    Ferricstore.Flow.LMDBWriter.Timer.ensure_timer(%{state | flush_interval_ms: interval})
  end

  defp ensure_timer(state), do: Ferricstore.Flow.LMDBWriter.Timer.ensure_timer(state)

  defp ensure_projection_outbox_timer(%{timer_ref: nil} = state) do
    Ferricstore.Flow.LMDBWriter.Timer.ensure_projection_outbox_timer(state)
  end

  defp ensure_projection_outbox_timer(state),
    do: Ferricstore.Flow.LMDBWriter.Timer.ensure_projection_outbox_timer(state)

  defp ensure_enqueue_timer(state), do: ensure_timer(state)

  defp flush_on_max_ops?(state), do: Ferricstore.Flow.LMDBWriter.Timer.flush_on_max_ops?(state)

  defp maybe_defer_timer_flush(state),
    do: Ferricstore.Flow.LMDBWriter.Timer.maybe_defer_timer_flush(state)

  defp maybe_flush_replay_safe_request(state) do
    case timer_flush_decision(state) do
      :flush ->
        flush_pending(state)

      {:defer, delay_ms} ->
        ensure_timer_with_delay(state, delay_ms)
    end
  end

  defp timer_flush_decision(state),
    do: Ferricstore.Flow.LMDBWriter.Timer.timer_flush_decision(state)

  defp timer_flush_decision(state, now),
    do: Ferricstore.Flow.LMDBWriter.Timer.timer_flush_decision(state, now)

  defp ensure_timer_with_delay(%{timer_ref: nil} = state, delay_ms)
       when is_integer(delay_ms) and delay_ms > 0 do
    Ferricstore.Flow.LMDBWriter.Timer.ensure_timer_with_delay(state, delay_ms)
  end

  defp ensure_timer_with_delay(state, delay_ms),
    do: Ferricstore.Flow.LMDBWriter.Timer.ensure_timer_with_delay(state, delay_ms)

  defp flush_pending(state) do
    {state, _reply} = flush_pending_with_reply(state)
    state
  end

  def flush_pending_with_reply(state) do
    state = Outbox.drain_projection_outbox(state, :no_flush)
    {state, reply} = do_flush_pending_with_reply(state)
    Outbox.maybe_reconcile_dirty_projection_with_reply(state, reply)
  end

  defp do_flush_pending_with_reply(
         %{
           pending: [],
           pending_after_flush: [],
           requested_index: requested,
           durable_index: durable
         } = state
       )
       when requested <= durable do
    {%{
       state
       | count: 0,
         pending_after_flush: [],
         first_pending_at: nil,
         last_enqueue_at: nil,
         timer_ref: nil
     }, :ok}
  end

  defp do_flush_pending_with_reply(state) do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)

    ops = Enum.reverse(state.pending)
    after_flush = Enum.reverse(state.pending_after_flush)
    started_at = System.monotonic_time()
    op_count = length(ops)
    pending_age_us = pending_age_us(state, started_at)

    case flush_ops_and_marker(state, ops, started_at) do
      {:ok, state, expanded_op_count} ->
        Enum.each(after_flush, &apply_after_flush/1)
        emit_flush(:ok, state, started_at, op_count, expanded_op_count, pending_age_us)

        state = %{
          state
          | pending: [],
            pending_after_flush: [],
            count: 0,
            first_pending_at: nil,
            last_enqueue_at: nil,
            timer_ref: nil
        }

        publish_backlog(state, 0)

        {state, :ok}

      {:error, reason, state} ->
        if instance_suspended?(state.instance_name) do
          {%{state | timer_ref: nil, suspended?: true}, :ok}
        else
          record_persist_failure(state.instance_ctx, state.shard_index)
          record_flush_failure(state.instance_ctx, state.shard_index)
          mark_mirror_degraded(state.instance_ctx, state.shard_index, reason)
          publish_backlog(state, pending_age_us)
          emit_persist({:error, reason}, state, state.requested_index, started_at)
          emit_flush({:error, reason}, state, started_at, op_count, 0, pending_age_us)

          Logger.warning(
            "Flow LMDB writer shard #{state.shard_index} flush failed: #{inspect(reason)}"
          )

          {ensure_timer(%{state | timer_ref: nil}), {:error, reason}}
        end
    end
  end

  defp flush_ops_and_marker(state, ops, started_at) do
    LMDBFlushCoordinator.with_permit(state.instance_name, fn ->
      flush_ops_and_marker_with_permit(state, ops, started_at)
    end)
  end

  defp flush_ops_and_marker_with_permit(state, ops, started_at) do
    try do
      with {:ok, state} <- maybe_ensure_lmdb_ready(state, ops),
           {:ok, ops, state} <- ProjectionOps.expand_ops(state, ops),
           :ok <-
             Ferricstore.FaultInjection.maybe_pause(:before_flow_lmdb_flush_write, %{
               instance_name: state.instance_name,
               shard_index: state.shard_index,
               op_count: length(ops),
               requested_index: state.requested_index
             }),
           :ok <- write_ops_chunked(state, ops),
           :ok <- ProjectionOps.cache_terminal_counts(state.path, state.terminal_count_cache),
           {:ok, state} <- persist_requested(state, started_at) do
        {:ok, state, length(ops)}
      else
        {:error, reason} -> {:error, reason, state}
      end
    catch
      kind, reason -> {:error, {kind, reason}, state}
    end
  end

  defp maybe_ensure_lmdb_ready(state, []), do: {:ok, state}
  defp maybe_ensure_lmdb_ready(state, _ops), do: ensure_lmdb_ready(state)

  defp ensure_lmdb_ready(%{lmdb_ready: true} = state), do: {:ok, state}

  defp ensure_lmdb_ready(state) do
    case Ferricstore.Flow.LMDB.warm(state.path) do
      :ok -> {:ok, %{state | lmdb_ready: true}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp write_ops(_path, []), do: :ok
  defp write_ops(path, ops), do: Ferricstore.Flow.LMDB.write_batch(path, ops)

  defp write_ops_chunked(_state, []), do: :ok

  defp write_ops_chunked(state, ops) do
    chunk_ops = flush_chunk_ops(state)

    cond do
      length(ops) <= chunk_ops ->
        write_ops(state.path, ops)

      not terminal_count_cache_empty?(Map.get(state, :terminal_count_cache)) ->
        # Terminal count cache updates are the public consistency boundary for
        # terminal queries. Keep those flushes in one LMDB transaction until the
        # count cache can move into the same expanded op batch.
        write_ops(state.path, ops)

      true ->
        with :ok <- write_ops(state.path, [Ferricstore.Flow.LMDB.flush_in_progress_put_op()]),
             :ok <- write_op_chunks(state, ops, chunk_ops),
             :ok <- write_ops(state.path, [Ferricstore.Flow.LMDB.flush_in_progress_delete_op()]) do
          :ok
        end
    end
  end

  defp flush_chunk_ops(%{flush_chunk_ops: chunk_ops})
       when is_integer(chunk_ops) and chunk_ops > 0,
       do: chunk_ops

  defp flush_chunk_ops(_state), do: Config.default_flush_chunk_ops()

  defp terminal_count_cache_empty?(%{puts: puts, refresh: refresh}) do
    map_size(puts) == 0 and MapSet.size(refresh) == 0
  end

  defp terminal_count_cache_empty?(_cache), do: true

  defp write_op_chunks(state, ops, chunk_ops) do
    ops
    |> Enum.chunk_every(chunk_ops)
    |> Enum.reduce_while(:ok, fn chunk, :ok ->
      case write_ops(state.path, chunk) do
        :ok ->
          maybe_pause_between_flush_chunks(state)
          {:cont, :ok}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp maybe_pause_between_flush_chunks(%{flush_chunk_pause_ms: pause_ms})
       when is_integer(pause_ms) and pause_ms > 0 do
    Process.sleep(pause_ms)
  end

  defp maybe_pause_between_flush_chunks(_state), do: :ok

  defp persist_requested(
         %{requested_index: requested, durable_index: durable} = state,
         _started_at
       )
       when requested <= durable do
    {:ok, state}
  end

  defp persist_requested(state, started_at) do
    index = state.requested_index

    case LMDBReplaySafeIndex.persist(state.shard_data_path, index) do
      :ok ->
        publish_durable(state.instance_ctx, state.shard_index, index)
        emit_persist(:ok, state, index, started_at)
        poke_release_cursor(state, index)
        {:ok, %{state | durable_index: index}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp prepend_reverse([], acc), do: acc
  defp prepend_reverse([head | rest], acc), do: prepend_reverse(rest, [head | acc])

  def terminal_hot_ttl_ms, do: AfterFlush.terminal_hot_ttl_ms()

  defp apply_after_flush(action), do: AfterFlush.apply_after_flush(action)

  def delete_apply_projection_cache_for_row(data_dir, shard_index, row) do
    AfterFlush.delete_apply_projection_cache_for_row(data_dir, shard_index, row)
  end

  defp poke_release_cursor(state, index) do
    if Application.get_env(:ferricstore, :flow_lmdb_release_cursor_poke_enabled, false) == true do
      _ =
        Task.start(fn ->
          _ = index
          Ferricstore.Raft.WARaftBackend.write(state.shard_index, :noop)
        end)
    end

    :ok
  catch
    :exit, _reason -> :ok
  end

  defp publish_durable(instance_ctx, shard_index, index),
    do: Ferricstore.Flow.LMDBWriter.Telemetry.publish_durable(instance_ctx, shard_index, index)

  defp publish_requested(instance_ctx, shard_index, index),
    do: Ferricstore.Flow.LMDBWriter.Telemetry.publish_requested(instance_ctx, shard_index, index)

  defp record_persist_failure(instance_ctx, shard_index),
    do: Ferricstore.Flow.LMDBWriter.Telemetry.record_persist_failure(instance_ctx, shard_index)

  def record_flush_failure(instance_ctx, shard_index),
    do: Ferricstore.Flow.LMDBWriter.Telemetry.record_flush_failure(instance_ctx, shard_index)

  def mark_mirror_degraded(instance_ctx, shard_index, reason),
    do:
      Ferricstore.Flow.LMDBWriter.Telemetry.mark_mirror_degraded(
        instance_ctx,
        shard_index,
        reason
      )

  def emit_backlog(state, now), do: Ferricstore.Flow.LMDBWriter.Telemetry.emit_backlog(state, now)

  defp publish_backlog(state, pending_age_us),
    do: Ferricstore.Flow.LMDBWriter.Telemetry.publish_backlog(state, pending_age_us)

  defp emit_flush(status, state, started_at, op_count, expanded_op_count, pending_age_us),
    do:
      Ferricstore.Flow.LMDBWriter.Telemetry.emit_flush(
        status,
        state,
        started_at,
        op_count,
        expanded_op_count,
        pending_age_us
      )

  defp emit_persist(status, state, index, started_at),
    do: Ferricstore.Flow.LMDBWriter.Telemetry.emit_persist(status, state, index, started_at)

  defp writer_unavailable(operation, instance_name, shard_index, reason, op_count),
    do:
      Ferricstore.Flow.LMDBWriter.Telemetry.writer_unavailable(
        operation,
        instance_name,
        shard_index,
        reason,
        op_count
      )

  defp pending_age_us(state, now),
    do: Ferricstore.Flow.LMDBWriter.Telemetry.pending_age_us(state, now)
end
