defmodule Ferricstore.Flow.LMDBWriter do
  @moduledoc false

  use GenServer

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.Hibernation
  alias Ferricstore.Flow.LMDBFlushCoordinator
  alias Ferricstore.Flow.LMDBReplaySafeIndex
  alias Ferricstore.Flow.Locator
  alias Ferricstore.Raft.WARaftSegmentReader

  require Logger

  @default_lagged_flush_interval_ms 500
  @default_lagged_flush_jitter_ms 250
  @default_lagged_flush_quiet_ms 250
  @default_lagged_flush_max_lag_ms 30_000
  @default_lagged_max_ops 25_000
  @default_flush_chunk_ops 5_000
  @default_lagged_flush_chunk_pause_ms 1
  @default_source_pending_retries 100
  @default_source_pending_sleep_ms 1
  @default_max_mailbox_messages 50_000
  @default_max_enqueue_ops 100_000
  @terminal_states ["completed", "failed", "cancelled"]
  @enqueue_seq_queued 1
  @enqueue_seq_processed 2

  def start_link(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    instance_name = instance_name_from_opts(opts)
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

        case enqueue_guard(pid, op_count) do
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
        case enqueue_async_guard(instance_name, shard_index, op_count) do
          :ok ->
            seq = reserve_enqueue_seq(instance_name, shard_index)
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
    instance_name = instance_name_from_ctx(instance_ctx)
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

  def flush_all(shard_count) when is_integer(shard_count) and shard_count >= 0 do
    flush_all(:default, shard_count, 30_000)
  end

  def flush_all(shard_count, timeout)
      when is_integer(shard_count) and shard_count >= 0 and is_integer(timeout) do
    flush_all(:default, shard_count, timeout)
  end

  def flush_all(instance_name, shard_count)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 do
    flush_all(instance_name, shard_count, 30_000)
  end

  def flush_all(instance_name, shard_count, timeout)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 and
             is_integer(timeout) do
    shard_count
    |> shard_indexes()
    |> Task.async_stream(
      fn shard_index -> {shard_index, flush(instance_name, shard_index, timeout)} end,
      max_concurrency: flush_all_concurrency(shard_count),
      on_timeout: :kill_task,
      ordered: false,
      timeout: flush_all_task_timeout(timeout)
    )
    |> Enum.reduce(:ok, &merge_flush_all_result/2)
  end

  def flush(shard_index) when is_integer(shard_index) and shard_index >= 0 do
    flush(:default, shard_index, 30_000)
  end

  def flush(shard_index, timeout)
      when is_integer(shard_index) and shard_index >= 0 and is_integer(timeout) do
    flush(:default, shard_index, timeout)
  end

  def flush(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    flush(instance_name, shard_index, 30_000)
  end

  def flush(instance_name, shard_index, timeout)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(timeout) do
    case Process.whereis(name(instance_name, shard_index)) do
      pid when is_pid(pid) ->
        try do
          case GenServer.call(pid, :flush, timeout) do
            :ok -> :ok
            {:error, _reason} = error -> error
          end
        catch
          :exit, reason ->
            writer_unavailable(:flush, instance_name, shard_index, reason, 0)
        end

      nil ->
        writer_unavailable(:flush, instance_name, shard_index, :writer_not_started, 0)
    end
  end

  def suspend_all(shard_count) when is_integer(shard_count) and shard_count >= 0 do
    suspend_all(:default, shard_count)
  end

  def suspend_all(shard_count, opts)
      when is_integer(shard_count) and shard_count >= 0 and is_list(opts) do
    suspend_all(:default, shard_count, opts)
  end

  def suspend_all(instance_name, shard_count)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 do
    suspend_all(instance_name, shard_count, flush: true)
  end

  def suspend_all(instance_name, shard_count, opts)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 and
             is_list(opts) do
    mark_instance_suspended(instance_name)
    flush? = Keyword.get(opts, :flush, true)

    Enum.each(shard_indexes(shard_count), fn shard_index ->
      _ =
        if flush? do
          suspend(instance_name, shard_index)
        else
          suspend_without_flush(instance_name, shard_index)
        end
    end)

    :ok
  end

  def suspend(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    mark_instance_suspended(instance_name)

    case Process.whereis(name(instance_name, shard_index)) do
      pid when is_pid(pid) ->
        GenServer.call(pid, :suspend, 5_000)

      nil ->
        :ok
    end
  catch
    :exit, _reason -> :ok
  end

  def suspend_without_flush(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    mark_instance_suspended(instance_name)

    case Process.whereis(name(instance_name, shard_index)) do
      pid when is_pid(pid) ->
        GenServer.cast(pid, :suspend_without_flush)
        :ok

      nil ->
        :ok
    end
  end

  def resume_all(shard_count) when is_integer(shard_count) and shard_count >= 0 do
    resume_all(:default, shard_count)
  end

  def resume_all(instance_name, shard_count)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 do
    clear_instance_suspended(instance_name)

    Enum.each(shard_indexes(shard_count), fn shard_index ->
      case Process.whereis(name(instance_name, shard_index)) do
        pid when is_pid(pid) -> GenServer.cast(pid, :resume)
        nil -> :ok
      end
    end)

    :ok
  end

  def name(shard_index), do: :"Ferricstore.Flow.LMDBWriter.#{shard_index}"

  def name(:default, shard_index), do: name(shard_index)

  def name(instance_name, shard_index) do
    :"Ferricstore.Flow.LMDBWriter.#{instance_name}.#{shard_index}"
  end

  defp projection_outbox_name(instance_name, shard_index) do
    :"Ferricstore.Flow.LMDBWriter.ProjectionOutbox.#{instance_name}.#{shard_index}"
  end

  defp ensure_projection_outbox!(instance_name, shard_index) do
    table = projection_outbox_name(instance_name, shard_index)

    case :ets.whereis(table) do
      :undefined ->
        :ets.new(table, [
          :ordered_set,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

      tid ->
        tid
    end
  end

  defp normalize_projection_outbox_entries(entries) do
    Enum.flat_map(entries, fn
      {state_key, version} when is_binary(state_key) and is_integer(version) -> [{state_key, version}]
      _other -> []
    end)
  end

  defp projection_outbox_rows(entries) do
    Enum.map(entries, fn {state_key, version} ->
      {System.unique_integer([:monotonic, :positive]), state_key, version}
    end)
  end

  defp mark_instance_suspended(instance_name) when is_atom(instance_name) do
    :persistent_term.put(suspend_key(instance_name), true)
    :ok
  end

  defp clear_instance_suspended(instance_name) when is_atom(instance_name) do
    :persistent_term.erase(suspend_key(instance_name))
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp instance_suspended?(instance_name) when is_atom(instance_name),
    do: :persistent_term.get(suspend_key(instance_name), false)

  defp suspend_key(instance_name), do: {__MODULE__, :suspended, instance_name}

  defp enqueue_seq_key(instance_name, shard_index),
    do: {__MODULE__, :enqueue_seq, instance_name, shard_index}

  defp publish_enqueue_seq(instance_name, shard_index, ref) when is_reference(ref) do
    :persistent_term.put(enqueue_seq_key(instance_name, shard_index), ref)
  end

  defp reserve_enqueue_seq(instance_name, shard_index) do
    case :persistent_term.get(enqueue_seq_key(instance_name, shard_index), nil) do
      ref when is_reference(ref) -> :atomics.add_get(ref, @enqueue_seq_queued, 1)
      _ -> 0
    end
  rescue
    _ -> 0
  end

  @impl true
  def init(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    data_dir = Keyword.fetch!(opts, :data_dir)
    instance_name = instance_name_from_opts(opts)
    _outbox = ensure_projection_outbox!(instance_name, shard_index)

    if shard_index == 0 do
      clear_instance_suspended(instance_name)
    end

    lost_enqueue = lost_unprocessed_enqueue(instance_name, shard_index)
    enqueue_seq = :atomics.new(2, signed: false)
    publish_enqueue_seq(instance_name, shard_index, enqueue_seq)

    state = %{
      instance_name: instance_name,
      mode: Ferricstore.Flow.LMDB.mode(),
      shard_index: shard_index,
      data_dir: data_dir,
      shard_data_path: Ferricstore.DataDir.shard_data_path(data_dir, shard_index),
      path:
        data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> Ferricstore.Flow.LMDB.path(),
      instance_ctx: Keyword.get(opts, :instance_ctx),
      pending: [],
      pending_after_flush: [],
      count: 0,
      first_pending_at: nil,
      last_enqueue_at: nil,
      timer_ref: nil,
      durable_index: 0,
      requested_index: 0,
      terminal_count_inits: MapSet.new(),
      lmdb_ready: false,
      suspended?: false,
      projection_dirty?: false,
      enqueue_seq: enqueue_seq,
      processed_enqueue_seq: 0,
      processed_enqueue_gaps: MapSet.new(),
      flush_waiters: [],
      flush_interval_ms:
        Application.get_env(
          :ferricstore,
          :flow_lmdb_flush_interval_ms,
          default_flush_interval_ms()
        ),
      flush_jitter_ms:
        Application.get_env(
          :ferricstore,
          :flow_lmdb_flush_jitter_ms,
          default_flush_jitter_ms()
        ),
      flush_quiet_ms:
        Application.get_env(
          :ferricstore,
          :flow_lmdb_flush_quiet_ms,
          @default_lagged_flush_quiet_ms
        ),
      flush_max_lag_ms:
        Application.get_env(
          :ferricstore,
          :flow_lmdb_flush_max_lag_ms,
          @default_lagged_flush_max_lag_ms
        ),
      max_ops: Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops, default_max_ops()),
      flush_on_max_ops?:
        Application.get_env(
          :ferricstore,
          :flow_lmdb_flush_on_max_ops,
          default_flush_on_max_ops(Ferricstore.Flow.LMDB.mode())
        ),
      flush_chunk_ops:
        Application.get_env(:ferricstore, :flow_lmdb_flush_chunk_ops, @default_flush_chunk_ops),
      flush_chunk_pause_ms:
        Application.get_env(
          :ferricstore,
          :flow_lmdb_flush_chunk_pause_ms,
          default_flush_chunk_pause_ms()
        )
    }

    durable_index = LMDBReplaySafeIndex.read(state.shard_data_path)
    publish_durable(state.instance_ctx, shard_index, durable_index)

    state = %{state | durable_index: durable_index, requested_index: durable_index}

    # Do not create the LMDB directory here. The default supervisor starts these
    # writers before WARaft publishes initial storage metadata, and even an empty
    # directory makes WARaft treat fresh storage as non-empty. Application start
    # creates idle LMDB dirs after WARaft is ready; real writes also create/open
    # LMDB lazily through the NIF path.
    maybe_mark_lost_enqueue(state.instance_ctx, shard_index, lost_enqueue)

    {:ok, state}
  end

  defp lost_unprocessed_enqueue(instance_name, shard_index) do
    case :persistent_term.get(enqueue_seq_key(instance_name, shard_index), nil) do
      ref when is_reference(ref) ->
        queued = :atomics.get(ref, @enqueue_seq_queued)
        processed = :atomics.get(ref, @enqueue_seq_processed)

        if queued > processed do
          {:lost_async_enqueue, queued, processed}
        else
          :none
        end

      _other ->
        :none
    end
  rescue
    _ -> :none
  end

  defp maybe_mark_lost_enqueue(_instance_ctx, _shard_index, :none), do: :ok

  defp maybe_mark_lost_enqueue(instance_ctx, shard_index, reason) do
    mark_mirror_degraded(instance_ctx, shard_index, reason)
  end

  defp instance_name_from_opts(opts) do
    case {Keyword.get(opts, :instance_name), Keyword.get(opts, :instance_ctx)} do
      {name, _ctx} when is_atom(name) and not is_nil(name) -> name
      {_name, %{name: name}} when is_atom(name) and not is_nil(name) -> name
      _ -> :default
    end
  end

  defp default_flush_interval_ms do
    @default_lagged_flush_interval_ms
  end

  defp default_max_ops do
    @default_lagged_max_ops
  end

  defp default_flush_on_max_ops(_mode), do: false

  defp default_flush_jitter_ms do
    @default_lagged_flush_jitter_ms
  end

  defp default_flush_chunk_pause_ms do
    @default_lagged_flush_chunk_pause_ms
  end

  defp enqueue_guard(pid, op_count) do
    with :ok <- enqueue_ops_capacity(op_count) do
      enqueue_mailbox_capacity(pid)
    end
  end

  defp enqueue_async_guard(instance_name, shard_index, op_count) do
    with :ok <- enqueue_ops_capacity(op_count) do
      enqueue_async_mailbox_capacity(instance_name, shard_index)
    end
  end

  defp enqueue_ops_capacity(op_count) do
    case Ferricstore.MemoryBudget.limit(
           :flow_lmdb_writer_max_enqueue_ops,
           @default_max_enqueue_ops
         ) do
      :infinity -> :ok
      max_ops when is_integer(max_ops) and op_count <= max_ops -> :ok
      _max_ops -> {:error, :queue_full}
    end
  end

  defp enqueue_mailbox_capacity(pid) do
    case Ferricstore.MemoryBudget.limit(
           :flow_lmdb_writer_max_mailbox_messages,
           @default_max_mailbox_messages
         ) do
      :infinity ->
        :ok

      max_messages when is_integer(max_messages) ->
        case Process.info(pid, :message_queue_len) do
          {:message_queue_len, len} when len < max_messages -> :ok
          {:message_queue_len, _len} -> {:error, :queue_full}
          nil -> {:error, :writer_not_started}
        end
    end
  end

  defp enqueue_async_mailbox_capacity(instance_name, shard_index) do
    case Ferricstore.MemoryBudget.limit(
           :flow_lmdb_writer_max_mailbox_messages,
           @default_max_mailbox_messages
         ) do
      :infinity ->
        :ok

      max_messages when is_integer(max_messages) ->
        case :persistent_term.get(enqueue_seq_key(instance_name, shard_index), nil) do
          ref when is_reference(ref) ->
            queued = :atomics.get(ref, @enqueue_seq_queued)
            processed = :atomics.get(ref, @enqueue_seq_processed)

            if queued - processed < max_messages do
              :ok
            else
              {:error, :queue_full}
            end

          _missing ->
            :ok
        end
    end
  rescue
    _ -> :ok
  end

  defp instance_name_from_ctx(%{name: name}) when is_atom(name) and not is_nil(name), do: name
  defp instance_name_from_ctx(_ctx), do: :default

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
      |> mark_enqueue_processed(seq)
      |> maybe_reply_flush_waiters()

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

      enqueue_seq_target(state) > state.processed_enqueue_seq ->
        {:noreply, queue_flush_waiter(state, from)}

      true ->
        {state, reply} = flush_pending_with_reply(state)
        {:reply, reply, state}
    end
  end

  def handle_call(:suspend, _from, state) do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
    {state, _reply} = flush_pending_with_reply(%{state | timer_ref: nil})
    {:reply, :ok, %{state | timer_ref: nil, suspended?: true}}
  end

  defp handle_enqueue(ops, after_flush, state) do
    {:noreply, enqueue_and_maybe_flush(ops, after_flush, state)}
  end

  defp enqueue_and_maybe_flush(ops, after_flush, state) do
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

  defp enqueue_ops(ops, after_flush, state, now) do
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
    delay = interval + timer_jitter_ms(Map.get(state, :flush_jitter_ms, 0))
    %{state | timer_ref: Process.send_after(self(), :flush, delay)}
  end

  defp ensure_timer(state), do: state

  defp ensure_projection_outbox_timer(%{timer_ref: nil} = state) do
    delay =
      state
      |> Map.get(:flush_max_lag_ms, @default_lagged_flush_max_lag_ms)
      |> max(1)

    %{state | timer_ref: Process.send_after(self(), :flush, delay)}
  end

  defp ensure_projection_outbox_timer(state), do: state

  defp ensure_enqueue_timer(state), do: ensure_timer(state)

  defp flush_on_max_ops?(%{flush_on_max_ops?: true} = state), do: state.count >= state.max_ops
  defp flush_on_max_ops?(_state), do: false

  defp maybe_defer_timer_flush(%{pending: [], pending_after_flush: []}), do: :flush

  defp maybe_defer_timer_flush(%{flush_on_max_ops?: true, count: count, max_ops: max_ops})
       when is_integer(count) and is_integer(max_ops) and count >= max_ops,
       do: :flush

  defp maybe_defer_timer_flush(%{last_enqueue_at: nil}), do: :flush
  defp maybe_defer_timer_flush(%{first_pending_at: nil}), do: :flush

  defp maybe_defer_timer_flush(state) do
    case timer_flush_decision(state) do
      {:defer, delay_ms} ->
        {:defer, %{state | timer_ref: Process.send_after(self(), :flush, delay_ms)}}

      :flush ->
        :flush
    end
  end

  defp maybe_flush_replay_safe_request(state) do
    case timer_flush_decision(state) do
      :flush ->
        flush_pending(state)

      {:defer, delay_ms} ->
        ensure_timer_with_delay(state, delay_ms)
    end
  end

  defp timer_flush_decision(%{pending: [], pending_after_flush: []}), do: :flush

  defp timer_flush_decision(%{flush_on_max_ops?: true, count: count, max_ops: max_ops})
       when is_integer(count) and is_integer(max_ops) and count >= max_ops,
       do: :flush

  defp timer_flush_decision(%{last_enqueue_at: nil}), do: :flush
  defp timer_flush_decision(%{first_pending_at: nil}), do: :flush

  defp timer_flush_decision(state) do
    quiet_ms =
      normalize_non_negative_integer(state.flush_quiet_ms, @default_lagged_flush_quiet_ms)

    max_lag_ms =
      normalize_non_negative_integer(state.flush_max_lag_ms, @default_lagged_flush_max_lag_ms)

    cond do
      quiet_ms == 0 or max_lag_ms == 0 ->
        :flush

      true ->
        now = System.monotonic_time()
        idle_ms = elapsed_ms(state.last_enqueue_at, now)
        pending_age_ms = elapsed_ms(state.first_pending_at, now)

        if idle_ms < quiet_ms and pending_age_ms < max_lag_ms do
          {:defer, max(1, min(quiet_ms - idle_ms, max_lag_ms - pending_age_ms))}
        else
          :flush
        end
    end
  end

  defp ensure_timer_with_delay(%{timer_ref: nil} = state, delay_ms)
       when is_integer(delay_ms) and delay_ms > 0 do
    %{state | timer_ref: Process.send_after(self(), :flush, delay_ms)}
  end

  defp ensure_timer_with_delay(state, _delay_ms), do: state

  defp elapsed_ms(started_at, now) when is_integer(started_at) and is_integer(now) do
    max(System.convert_time_unit(now - started_at, :native, :millisecond), 0)
  end

  defp enqueue_seq_target(%{enqueue_seq: ref}) when is_reference(ref) do
    :atomics.get(ref, @enqueue_seq_queued)
  rescue
    _ -> 0
  end

  defp enqueue_seq_target(_state), do: 0

  defp queue_flush_waiter(state, from) do
    target = enqueue_seq_target(state)
    %{state | flush_waiters: [{from, target} | state.flush_waiters]}
  end

  defp mark_enqueue_processed(state, seq) when is_integer(seq) and seq > 0 do
    {processed, gaps} =
      advance_processed_enqueue_seq(
        state.processed_enqueue_seq,
        state.processed_enqueue_gaps,
        seq
      )

    publish_processed_enqueue_seq(state.enqueue_seq, processed)
    %{state | processed_enqueue_seq: processed, processed_enqueue_gaps: gaps}
  end

  defp mark_enqueue_processed(state, _seq), do: state

  defp advance_processed_enqueue_seq(current, gaps, seq) when seq <= current do
    {current, gaps}
  end

  defp advance_processed_enqueue_seq(current, gaps, seq) when seq == current + 1 do
    consume_processed_enqueue_gaps(seq, gaps)
  end

  defp advance_processed_enqueue_seq(current, gaps, seq) do
    {current, MapSet.put(gaps, seq)}
  end

  defp consume_processed_enqueue_gaps(current, gaps) do
    next = current + 1

    if MapSet.member?(gaps, next) do
      consume_processed_enqueue_gaps(next, MapSet.delete(gaps, next))
    else
      {current, gaps}
    end
  end

  defp publish_processed_enqueue_seq(ref, processed) when is_reference(ref) do
    :atomics.put(ref, @enqueue_seq_processed, processed)
  rescue
    _ -> :ok
  end

  defp publish_processed_enqueue_seq(_ref, _processed), do: :ok

  defp maybe_reply_flush_waiters(%{flush_waiters: []} = state), do: state

  defp maybe_reply_flush_waiters(state) do
    {ready, waiting} =
      state.flush_waiters
      |> Enum.reverse()
      |> Enum.split_with(fn {_from, target} -> state.processed_enqueue_seq >= target end)

    state = %{state | flush_waiters: Enum.reverse(waiting)}

    case ready do
      [] ->
        state

      _ ->
        {state, reply} = flush_pending_with_reply(state)
        Enum.each(ready, fn {from, _target} -> GenServer.reply(from, reply) end)
        state
    end
  end

  defp flush_pending(state) do
    {state, _reply} = flush_pending_with_reply(state)
    state
  end

  defp flush_pending_with_reply(state) do
    state = drain_projection_outbox(state, :no_flush)
    {state, reply} = do_flush_pending_with_reply(state)
    maybe_reconcile_dirty_projection_with_reply(state, reply)
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
           {:ok, ops, state} <- expand_ops(state, ops),
           :ok <-
             Ferricstore.FaultInjection.maybe_pause(:before_flow_lmdb_flush_write, %{
               instance_name: state.instance_name,
               shard_index: state.shard_index,
               op_count: length(ops),
               requested_index: state.requested_index
             }),
           :ok <- write_ops_chunked(state, ops),
           :ok <- cache_terminal_counts(state.path, state.terminal_count_cache),
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

  defp flush_chunk_ops(_state), do: @default_flush_chunk_ops

  defp terminal_count_cache_empty?(%{puts: puts, refresh: refresh}) do
    map_size(puts) == 0 and MapSet.size(refresh) == 0
  end

  defp terminal_count_cache_empty?(_cache), do: true

  defp drain_projection_outbox(state, mode) when mode in [:maybe_flush, :no_flush] do
    case take_projection_outbox_entries(state) do
      [] ->
        state

      entries ->
        {ops, after_flush} = projection_outbox_items(state, entries)

        cond do
          ops == [] and after_flush == [] ->
            state

          mode == :maybe_flush ->
            enqueue_and_maybe_flush(ops, after_flush, state)

          true ->
            now = System.monotonic_time()
            state = enqueue_ops(ops, after_flush, state, now)

            emit_backlog(state, now)
            state
        end
    end
  end

  defp take_projection_outbox_entries(state) do
    table = projection_outbox_name(state.instance_name, state.shard_index)

    case :ets.whereis(table) do
      :undefined ->
        []

      tid ->
        tid
        |> :ets.tab2list()
        |> Enum.flat_map(fn
          {seq, _state_key, _version} ->
          case :ets.take(tid, seq) do
            [entry] -> [entry]
            _ -> []
          end

          _marker ->
            []
        end)
        |> Enum.sort_by(fn {seq, _state_key, _version} -> seq end)
    end
  rescue
    ArgumentError -> []
  end

  defp projection_outbox_items(state, entries) do
    entries
    |> Enum.reduce({[], []}, fn {_seq, state_key, version}, {ops, after_flush} ->
      action = projection_outbox_after_flush(state, state_key, version)

      after_flush =
        case action do
          nil -> after_flush
          action -> [action | after_flush]
        end

      {[{:project_flow_state_from_source, state_key} | ops], after_flush}
    end)
    |> then(fn {ops, after_flush} -> {Enum.reverse(ops), Enum.reverse(after_flush)} end)
  end

  defp projection_outbox_after_flush(state, state_key, version) do
    case source_keydir(state) do
      nil ->
        nil

      ets ->
        {zset_index, zset_lookup} =
          Ferricstore.Store.Shard.ZSetIndex.table_names(state.instance_name, state.shard_index)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.NativeOrderedIndex.table_names(state.instance_name, state.shard_index)

        {:defer_after_flush, terminal_hot_ttl_ms(),
         {:prune_terminal_flow_from_source_v1, state.data_dir, state.shard_index, ets, zset_index,
          zset_lookup, flow_index, flow_lookup, state_key, version}}
    end
  end

  defp maybe_reconcile_dirty_projection_with_reply(state, :ok) do
    case reconcile_dirty_projection(state) do
      :ok ->
        clear_projection_dirty_marker(state)
        {%{state | projection_dirty?: false}, :ok}

      {:error, reason} ->
        record_flush_failure(state.instance_ctx, state.shard_index)
        mark_mirror_degraded(state.instance_ctx, state.shard_index, reason)
        {%{state | projection_dirty?: true}, {:error, reason}}
    end
  end

  defp maybe_reconcile_dirty_projection_with_reply(state, reply), do: {state, reply}

  defp reconcile_dirty_projection(%{projection_dirty?: true, mode: :lagged} = state) do
    case source_keydir(state) do
      nil ->
        {:error, :source_keydir_unavailable}

      keydir ->
        {zset_index, zset_lookup} =
          Ferricstore.Store.Shard.ZSetIndex.table_names(state.instance_name, state.shard_index)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.NativeOrderedIndex.table_names(state.instance_name, state.shard_index)

        Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
          state.shard_data_path,
          keydir,
          state.shard_index,
          state.instance_ctx,
          zset_index,
          zset_lookup,
          flow_index,
          flow_lookup
        )
    end
  rescue
    error -> {:error, {:lagged_projection_reconcile_failed, error}}
  catch
    kind, reason -> {:error, {:lagged_projection_reconcile_failed, {kind, reason}}}
  end

  defp reconcile_dirty_projection(_state), do: :ok

  defp clear_projection_dirty_marker(state) do
    case :ets.whereis(projection_outbox_name(state.instance_name, state.shard_index)) do
      :undefined -> :ok
      tid -> :ets.delete(tid, :dirty)
    end
  rescue
    ArgumentError -> :ok
  end

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

  defp expand_ops(state, []) do
    {:ok, [], Map.put(state, :terminal_count_cache, empty_terminal_count_cache())}
  end

  defp expand_ops(state, ops) do
    initial = %{
      ops: [],
      counts: %{},
      terminal_values: %{},
      active_reverse_values: %{},
      terminal_count_inits: state.terminal_count_inits,
      terminal_count_cache: empty_terminal_count_cache()
    }

    Enum.reduce_while(ops, {:ok, initial}, fn op, {:ok, acc} ->
      case expand_op(state, op, acc) do
        {:ok, acc} -> {:cont, {:ok, acc}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok,
       %{
         ops: expanded,
         terminal_count_inits: terminal_count_inits,
         terminal_count_cache: terminal_count_cache
       }} ->
        state =
          state
          |> Map.put(:terminal_count_inits, terminal_count_inits)
          |> Map.put(:terminal_count_cache, terminal_count_cache)

        {:ok, Enum.reverse(expanded), state}

      {:error, _reason} = error ->
        error
    end
  end

  defp empty_terminal_count_cache, do: %{puts: %{}, refresh: MapSet.new()}

  defp expand_op(state, {:project_kv_from_source, key}, acc) when is_binary(key) do
    case read_source_value(state, key) do
      {:ok, value, expire_at_ms} ->
        expand_path_op(
          state.path,
          {:put, key, Ferricstore.Flow.LMDB.encode_value(value, expire_at_ms)},
          acc
        )

      :not_found ->
        {:ok, prepend_ops(acc, [{:delete, key}])}

      {:error, _reason} = error ->
        error
    end
  end

  defp expand_op(state, {:project_flow_state_from_source, key}, acc) when is_binary(key) do
    case read_source_value(state, key) do
      {:ok, value, expire_at_ms} ->
        expand_flow_state_value(state.path, key, value, expire_at_ms, acc)

      :not_found ->
        expand_missing_flow_state_projection(state.path, key, acc)

      {:error, _reason} = error ->
        error
    end
  end

  defp expand_op(state, {:project_flow_state, key, value, expire_at_ms}, acc)
       when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) do
    expand_flow_state_value(state.path, key, value, expire_at_ms, acc)
  end

  defp expand_op(%{path: path}, op, acc), do: expand_path_op(path, op, acc)

  defp expand_flow_state_value(path, key, value, expire_at_ms, acc) do
    wrapper = Ferricstore.Flow.LMDB.encode_value(value, expire_at_ms)

    with {:ok, acc} <- expand_path_op(path, {:put, key, wrapper}, acc) do
      case decode_flow_record_value(wrapper) do
        {:ok, record} ->
          expand_flow_state_projection(path, key, expire_at_ms, record, acc)

        :error ->
          {:ok, acc}
      end
    end
  end

  defp expand_flow_state_projection(path, state_key, expire_at_ms, record, acc) do
    if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
      with {:ok, acc} <- maybe_expand_stale_active_delete(path, state_key, acc) do
        expand_path_op(
          path,
          {:terminal_project, Map.fetch!(record, :id), Map.fetch!(record, :type),
           Map.fetch!(record, :state), Map.get(record, :partition_key),
           Map.get(record, :updated_at_ms, 0), state_key, expire_at_ms,
           Map.get(record, :parent_flow_id), Map.get(record, :root_flow_id),
           Map.get(record, :correlation_id)},
          acc
        )
      end
    else
      with {:ok, acc} <- maybe_expand_stale_terminal_delete(path, state_key, acc),
           {:ok, acc} <- maybe_expand_stale_active_delete(path, state_key, acc) do
        {active_ops, reverse_value} =
          Ferricstore.Flow.LMDB.active_index_put_ops_with_reverse(
            state_key,
            record,
            expire_at_ms
          )

        acc =
          acc
          |> put_in([:active_reverse_values, state_key], reverse_value)
          |> prepend_ops(active_ops)

        {:ok, acc}
      end
    end
  end

  defp expand_missing_flow_state_projection(path, state_key, acc) do
    with {:ok, acc} <- maybe_expand_stale_terminal_delete(path, state_key, acc),
         {:ok, acc} <- maybe_expand_stale_active_delete(path, state_key, acc) do
      {:ok, prepend_ops(acc, [{:delete, state_key}])}
    end
  end

  defp maybe_expand_stale_terminal_delete(path, state_key, acc) do
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

    with {:ok, terminal_key} when is_binary(terminal_key) <-
           Ferricstore.Flow.LMDB.get(path, reverse_key),
         {:ok, terminal_value} <- Ferricstore.Flow.LMDB.get(path, terminal_key),
         {:ok, count_key} <- Ferricstore.Flow.LMDB.terminal_index_count_key(terminal_value) do
      expand_path_op(path, {:terminal_delete, terminal_key, state_key, count_key}, acc)
    else
      _ -> {:ok, acc}
    end
  end

  defp maybe_expand_stale_active_delete(path, state_key, acc) do
    with {:ok, reverse_value, acc} <- active_reverse_value(path, state_key, acc),
         true <- is_binary(reverse_value) do
      acc =
        acc
        |> put_in([:active_reverse_values, state_key], nil)
        |> prepend_ops(
          Ferricstore.Flow.LMDB.active_index_delete_ops_from_reverse(state_key, reverse_value)
        )

      {:ok, acc}
    else
      false -> {:ok, acc}
      :not_found -> {:ok, acc}
      {:error, _reason} = error -> error
      _ -> {:ok, acc}
    end
  end

  defp read_source_value(state, key) do
    read_source_value(state, key, source_pending_retries())
  end

  defp read_source_value(state, key, pending_retries) do
    with keydir when not is_nil(keydir) <- source_keydir(state),
         [{^key, cached_value, expire_at_ms, _lfu, file_id, offset, _value_size}] <-
           :ets.lookup(keydir, key) do
      case file_id do
        :pending when pending_retries > 0 ->
          Process.sleep(source_pending_sleep_ms())
          read_source_value(state, key, pending_retries - 1)

        :pending ->
          {:error, {:source_pending, key}}

        :deleted ->
          :not_found

        file_id when is_integer(file_id) and file_id >= 0 ->
          read_source_location(state, key, cached_value, expire_at_ms, file_id, offset)

        {:waraft_segment, _index} = file_id ->
          read_source_location(state, key, cached_value, expire_at_ms, file_id, offset)

        {kind, _index} = file_id when kind in [:waraft_projection, :waraft_apply_projection] ->
          read_source_location(state, key, cached_value, expire_at_ms, file_id, offset)

        _ when is_binary(cached_value) ->
          source_read_result({:ok, cached_value}, expire_at_ms)

        _ ->
          read_source_location(state, key, cached_value, expire_at_ms, file_id, offset)
      end
    else
      [] -> :not_found
      nil -> {:error, :source_keydir_unavailable}
      _ -> {:error, :source_keydir_bad_entry}
    end
  rescue
    error -> {:error, {:source_read_failed, error}}
  end

  defp source_pending_retries do
    :ferricstore
    |> Application.get_env(:flow_lmdb_source_pending_retries, @default_source_pending_retries)
    |> normalize_non_negative_integer(@default_source_pending_retries)
  end

  defp source_pending_sleep_ms do
    :ferricstore
    |> Application.get_env(:flow_lmdb_source_pending_sleep_ms, @default_source_pending_sleep_ms)
    |> normalize_non_negative_integer(@default_source_pending_sleep_ms)
  end

  defp source_keydir(%{instance_ctx: ctx, shard_index: shard_index})
       when is_map(ctx) and is_integer(shard_index) do
    case Map.get(ctx, :keydir_refs) do
      refs when is_tuple(refs) and shard_index >= 0 and shard_index < tuple_size(refs) ->
        elem(refs, shard_index)

      _ ->
        nil
    end
  end

  defp source_keydir(_state), do: nil

  defp read_source_location(state, _key, _cached_value, expire_at_ms, file_id, offset)
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 do
    state.shard_data_path
    |> bitcask_file_path(file_id)
    |> NIF.v2_pread_at(offset)
    |> source_read_result(expire_at_ms)
  end

  defp read_source_location(_state, _key, _cached_value, _expire_at_ms, :deleted, _offset),
    do: :not_found

  defp read_source_location(
         %{instance_ctx: ctx, shard_index: shard_index},
         key,
         _cached_value,
         expire_at_ms,
         {:waraft_segment, _index} = file_id,
         _offset
       ) do
    ctx
    |> WARaftSegmentReader.read_value_from_location(shard_index, file_id, key)
    |> source_segment_read_result(expire_at_ms)
  end

  defp read_source_location(
         %{instance_ctx: ctx, shard_index: shard_index},
         key,
         _cached_value,
         expire_at_ms,
         {kind, _index} = file_id,
         _offset
       )
       when kind in [:waraft_projection, :waraft_apply_projection] do
    ctx
    |> WARaftSegmentReader.read_value_from_location(shard_index, file_id, key)
    |> source_segment_read_result(expire_at_ms)
  end

  defp read_source_location(_state, _key, _cached_value, _expire_at_ms, file_id, _offset),
    do: {:error, {:source_location_unavailable, file_id}}

  defp source_read_result({:ok, value}, expire_at_ms) when is_binary(value),
    do: {:ok, value, expire_at_ms}

  defp source_read_result({:error, reason}, _expire_at_ms), do: {:error, reason}
  defp source_read_result(other, _expire_at_ms), do: {:error, {:bad_source_read, other}}

  defp source_segment_read_result({:ok, value}, expire_at_ms) when is_binary(value),
    do: {:ok, value, expire_at_ms}

  defp source_segment_read_result(:not_found, _expire_at_ms), do: :not_found
  defp source_segment_read_result({:error, reason}, _expire_at_ms), do: {:error, reason}
  defp source_segment_read_result(other, _expire_at_ms), do: {:error, {:bad_source_read, other}}

  defp bitcask_file_path(shard_data_path, file_id) do
    Path.join(shard_data_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  defp expand_path_op(path, {:terminal_put, terminal_key, value, state_key, count_key}, acc)
       when is_binary(terminal_key) and is_binary(value) and is_binary(state_key) and
              is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         {:ok, count, acc} <- terminal_count(path, count_key, acc) do
      existed? = is_binary(old_value)
      {:ok, count, acc} = repair_terminal_count_if_missing(path, count_key, count, existed?, acc)
      count = if existed?, do: count, else: count + 1
      reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

      ops =
        [
          {:put, terminal_key, value},
          {:put, reverse_key, terminal_key},
          {:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}
        ]
        |> maybe_put_expire_key(terminal_key, value, state_key, count_key)
        |> maybe_delete_old_expire_key(terminal_key, old_value)

      acc =
        acc
        |> put_in([:counts, count_key], count)
        |> put_in([:terminal_values, terminal_key], value)
        |> put_in([:terminal_count_cache, :puts, count_key], count)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  defp expand_path_op(path, {:terminal_put, terminal_key, value, nil, count_key}, acc)
       when is_binary(terminal_key) and is_binary(value) and is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         {:ok, count, acc} <- terminal_count(path, count_key, acc) do
      existed? = is_binary(old_value)
      {:ok, count, acc} = repair_terminal_count_if_missing(path, count_key, count, existed?, acc)
      count = if existed?, do: count, else: count + 1

      ops =
        [
          {:put, terminal_key, value},
          {:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}
        ]
        |> maybe_put_expire_key(terminal_key, value, nil, count_key)
        |> maybe_delete_old_expire_key(terminal_key, old_value)

      acc =
        acc
        |> put_in([:counts, count_key], count)
        |> put_in([:terminal_values, terminal_key], value)
        |> put_in([:terminal_count_cache, :puts, count_key], count)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  defp expand_path_op(
         path,
         {:terminal_project, id, type, terminal_state, partition_key, updated_at_ms, state_key,
          expire_at_ms, parent_flow_id, root_flow_id, correlation_id},
         acc
       )
       when is_binary(id) and is_binary(type) and is_binary(terminal_state) and
              is_integer(updated_at_ms) and is_binary(state_key) and is_integer(expire_at_ms) do
    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)
    terminal_key = Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, id, updated_at_ms)
    count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

    value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value(
        id,
        updated_at_ms,
        expire_at_ms,
        state_key,
        count_key
      )

    with {:ok, acc} <-
           expand_path_op(path, {:terminal_put, terminal_key, value, state_key, count_key}, acc) do
      {:ok,
       prepend_ops(
         acc,
         terminal_project_metadata_ops(
           id,
           partition_key,
           updated_at_ms,
           expire_at_ms,
           state_key,
           parent_flow_id,
           root_flow_id,
           correlation_id
         )
       )}
    end
  end

  defp expand_path_op(_path, {:query_put, query_key, value}, acc)
       when is_binary(query_key) and is_binary(value) do
    {:ok, prepend_ops(acc, [{:put, query_key, value}])}
  end

  defp expand_path_op(
         path,
         {:history_project_from_index, flow_index, flow_lookup, id, partition_key, history_key,
          expire_at_ms},
         acc
       )
       when is_binary(id) and is_binary(history_key) and is_integer(expire_at_ms) do
    entries = history_project_from_index_entries(flow_index, flow_lookup, history_key)

    {:ok,
     prepend_ops(
       acc,
       history_put_many_ops(path, id, partition_key, history_key, expire_at_ms, entries)
     )}
  end

  defp expand_path_op(
         path,
         {:history_put_many, id, partition_key, history_key, expire_at_ms, entries},
         acc
       )
       when is_binary(id) and is_binary(history_key) and is_integer(expire_at_ms) and
              is_list(entries) do
    {:ok,
     prepend_ops(
       acc,
       history_put_many_ops(path, id, partition_key, history_key, expire_at_ms, entries)
     )}
  end

  defp expand_path_op(_path, {:query_delete, query_key}, acc) when is_binary(query_key) do
    {:ok, prepend_ops(acc, [{:delete, query_key}])}
  end

  defp expand_path_op(path, {:history_delete, history_index_key}, acc)
       when is_binary(history_index_key) do
    {:ok,
     prepend_ops(acc, Ferricstore.Flow.LMDB.history_index_delete_ops(path, history_index_key))}
  end

  defp expand_path_op(_path, {put_mode, key, value} = op, acc)
       when put_mode in [:put, :put_new] and is_binary(key) and is_binary(value) do
    acc = maybe_init_terminal_counts_for_active_record(value, acc)
    {:ok, prepend_ops(acc, [op])}
  end

  defp expand_path_op(path, {:terminal_delete, terminal_key, state_key, count_key}, acc)
       when is_binary(terminal_key) and is_binary(state_key) and is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         {:ok, count, acc} <- terminal_count(path, count_key, acc) do
      existed? = is_binary(old_value)
      {:ok, count, acc} = repair_terminal_count_if_missing(path, count_key, count, existed?, acc)

      reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

      {count, count_ops} =
        if existed? do
          count = max(count - 1, 0)
          {count, [{:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}]}
        else
          {count, []}
        end

      ops =
        [{:delete, terminal_key}, {:delete, reverse_key} | count_ops]
        |> maybe_delete_old_expire_key(terminal_key, old_value)

      acc =
        acc
        |> put_in([:counts, count_key], count)
        |> put_in([:terminal_values, terminal_key], nil)
        |> put_in([:terminal_count_cache, :puts, count_key], count)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  defp expand_path_op(path, {:terminal_delete, terminal_key, nil, count_key}, acc)
       when is_binary(terminal_key) and is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         {:ok, count, acc} <- terminal_count(path, count_key, acc) do
      existed? = is_binary(old_value)
      {:ok, count, acc} = repair_terminal_count_if_missing(path, count_key, count, existed?, acc)

      {count, count_ops} =
        if existed? do
          count = max(count - 1, 0)
          {count, [{:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}]}
        else
          {count, []}
        end

      ops =
        [{:delete, terminal_key} | count_ops]
        |> maybe_delete_old_expire_key(terminal_key, old_value)

      acc =
        acc
        |> put_in([:counts, count_key], count)
        |> put_in([:terminal_values, terminal_key], nil)
        |> put_in([:terminal_count_cache, :puts, count_key], count)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  defp expand_path_op(_path, op, acc), do: {:ok, prepend_ops(acc, [op])}

  defp maybe_init_terminal_counts_for_active_record(value, acc) do
    with {:ok, record} <- decode_flow_record_value(value),
         state when is_binary(state) <- Map.get(record, :state),
         false <- Ferricstore.Flow.LMDB.terminal_state?(state),
         type when is_binary(type) <- Map.get(record, :type) do
      partition_key = Map.get(record, :partition_key)
      init_key = {type, partition_key}

      if MapSet.member?(acc.terminal_count_inits, init_key) do
        acc
      else
        count_ops =
          Enum.map(@terminal_states, fn terminal_state ->
            state_index_key =
              Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)

            count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

            {:put_new, count_key, Ferricstore.Flow.LMDB.encode_count(0)}
          end)

        acc
        |> Map.update!(:terminal_count_inits, &MapSet.put(&1, init_key))
        |> update_in([:terminal_count_cache, :refresh], fn refresh ->
          Enum.reduce(count_ops, refresh, fn {:put_new, count_key, _value}, refresh ->
            MapSet.put(refresh, count_key)
          end)
        end)
        |> prepend_ops(count_ops)
      end
    else
      _ -> acc
    end
  end

  defp decode_flow_record_value(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {_expire_at_ms, encoded_record} when is_binary(encoded_record) ->
        {:ok, flow_call(:decode_record, [encoded_record])}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  defp flow_call(function, args) do
    apply(Ferricstore.Flow, function, args)
  end

  defp terminal_value(path, terminal_key, acc) do
    case Map.fetch(acc.terminal_values, terminal_key) do
      {:ok, value} ->
        {:ok, value, acc}

      :error ->
        case Ferricstore.Flow.LMDB.get(path, terminal_key) do
          {:ok, value} -> {:ok, value, put_in(acc, [:terminal_values, terminal_key], value)}
          :not_found -> {:ok, nil, put_in(acc, [:terminal_values, terminal_key], nil)}
          {:error, _reason} = error -> error
        end
    end
  end

  defp terminal_count(path, count_key, acc) do
    case Map.fetch(acc.counts, count_key) do
      {:ok, count} ->
        {:ok, count, acc}

      :error ->
        case Ferricstore.Flow.LMDB.get(path, count_key) do
          {:ok, value} ->
            case Ferricstore.Flow.LMDB.decode_count(value) do
              {:ok, count} -> {:ok, count, put_in(acc, [:counts, count_key], count)}
              :error -> {:ok, 0, put_in(acc, [:counts, count_key], 0)}
            end

          :not_found ->
            {:ok, 0, put_in(acc, [:counts, count_key], 0)}

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp repair_terminal_count_if_missing(path, count_key, 0, true, acc) do
    case terminal_state_index_key_from_count_key(count_key) do
      {:ok, state_index_key} ->
        prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(state_index_key)

        case Ferricstore.Flow.LMDB.prefix_count(path, prefix) do
          {:ok, count} when count > 0 ->
            {:ok, count, put_in(acc, [:counts, count_key], count)}

          _missing_or_error ->
            {:ok, 1, put_in(acc, [:counts, count_key], 1)}
        end

      :error ->
        {:ok, 1, put_in(acc, [:counts, count_key], 1)}
    end
  end

  defp repair_terminal_count_if_missing(_path, _count_key, count, _existed?, acc),
    do: {:ok, count, acc}

  defp terminal_state_index_key_from_count_key(count_key) when is_binary(count_key) do
    prefix = Ferricstore.Flow.LMDB.terminal_count_prefix()

    if String.starts_with?(count_key, prefix) and byte_size(count_key) > byte_size(prefix) do
      state_index_key =
        binary_part(count_key, byte_size(prefix), byte_size(count_key) - byte_size(prefix))

      case state_index_key do
        <<>> -> :error
        _ -> {:ok, state_index_key}
      end
    else
      :error
    end
  end

  defp terminal_state_index_key_from_count_key(_count_key), do: :error

  defp active_reverse_value(path, state_key, acc) do
    case Map.fetch(acc.active_reverse_values, state_key) do
      {:ok, value} ->
        {:ok, value, acc}

      :error ->
        reverse_key = Ferricstore.Flow.LMDB.active_by_state_key_key(state_key)

        case Ferricstore.Flow.LMDB.get(path, reverse_key) do
          {:ok, value} -> {:ok, value, put_in(acc, [:active_reverse_values, state_key], value)}
          :not_found -> {:ok, nil, put_in(acc, [:active_reverse_values, state_key], nil)}
          {:error, _reason} = error -> error
        end
    end
  end

  defp cache_terminal_counts(path, %{puts: puts, refresh: refresh}) do
    Enum.each(puts, fn {count_key, count} ->
      Ferricstore.Flow.LMDB.put_cached_terminal_count_key(path, count_key, count)
    end)

    Enum.each(refresh, fn count_key ->
      case Map.has_key?(puts, count_key) do
        true -> :ok
        false -> Ferricstore.Flow.LMDB.refresh_terminal_count_key(path, count_key)
      end
    end)

    :ok
  end

  defp prepend_ops(acc, ops), do: %{acc | ops: :lists.reverse(ops, acc.ops)}

  defp history_project_from_index_entries(nil, _flow_lookup, _history_key), do: []
  defp history_project_from_index_entries(_flow_index, nil, _history_key), do: []

  defp history_project_from_index_entries(flow_index, flow_lookup, history_key) do
    case Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup) do
      nil -> []
      native -> history_project_from_native_entries(native, history_key)
    end
  rescue
    ArgumentError -> []
  end

  defp history_project_from_native_entries(native, history_key) do
    native
    |> Ferricstore.Flow.NativeOrderedIndex.range_slice(
      history_key,
      :neg_inf,
      :inf,
      false,
      0,
      :all
    )
    |> history_project_normalize_entries()
  rescue
    ArgumentError -> []
  end

  defp history_project_normalize_entries(entries) do
    Enum.map(entries, fn {event_id, event_ms} -> {event_id, trunc(event_ms)} end)
  end

  defp terminal_project_metadata_ops(
         id,
         partition_key,
         score,
         expire_at_ms,
         state_key,
         parent_flow_id,
         root_flow_id,
         correlation_id
       ) do
    []
    |> terminal_project_metadata_op(
      :parent,
      parent_flow_id,
      partition_key,
      id,
      score,
      expire_at_ms,
      state_key
    )
    |> terminal_project_metadata_op(
      :root,
      root_flow_id,
      partition_key,
      id,
      score,
      expire_at_ms,
      state_key
    )
    |> terminal_project_metadata_op(
      :correlation,
      correlation_id,
      partition_key,
      id,
      score,
      expire_at_ms,
      state_key
    )
  end

  defp terminal_project_metadata_op(
         ops,
         :root,
         nil,
         _partition_key,
         _id,
         _score,
         _expire_at_ms,
         _state_key
       ),
       do: ops

  defp terminal_project_metadata_op(
         ops,
         :root,
         "",
         _partition_key,
         _id,
         _score,
         _expire_at_ms,
         _state_key
       ),
       do: ops

  defp terminal_project_metadata_op(
         ops,
         :root,
         id,
         _partition_key,
         id,
         _score,
         _expire_at_ms,
         _state_key
       ),
       do: ops

  defp terminal_project_metadata_op(
         ops,
         kind,
         value,
         partition_key,
         id,
         score,
         expire_at_ms,
         state_key
       )
       when is_binary(value) and value != "" do
    index_key =
      case kind do
        :parent -> Ferricstore.Flow.Keys.parent_index_key(value, partition_key)
        :root -> Ferricstore.Flow.Keys.root_index_key(value, partition_key)
        :correlation -> Ferricstore.Flow.Keys.correlation_index_key(value, partition_key)
      end

    query_key = Ferricstore.Flow.LMDB.query_index_key(index_key, id, score)
    value = Ferricstore.Flow.LMDB.encode_query_index_value(id, score, expire_at_ms, state_key)

    [{:put, query_key, value} | ops]
  end

  defp terminal_project_metadata_op(
         ops,
         _kind,
         _value,
         _partition_key,
         _id,
         _score,
         _expire_at_ms,
         _state_key
       ),
       do: ops

  defp terminal_project_metadata_index_keys(
         id,
         partition_key,
         parent_flow_id,
         root_flow_id,
         correlation_id
       ) do
    []
    |> terminal_project_metadata_index_key(:parent, parent_flow_id, partition_key, id)
    |> terminal_project_metadata_index_key(:root, root_flow_id, partition_key, id)
    |> terminal_project_metadata_index_key(:correlation, correlation_id, partition_key, id)
  end

  defp terminal_project_metadata_index_key(keys, :root, nil, _partition_key, _id), do: keys
  defp terminal_project_metadata_index_key(keys, :root, "", _partition_key, _id), do: keys
  defp terminal_project_metadata_index_key(keys, :root, id, _partition_key, id), do: keys

  defp terminal_project_metadata_index_key(keys, kind, value, partition_key, _id)
       when is_binary(value) and value != "" do
    key =
      case kind do
        :parent -> Ferricstore.Flow.Keys.parent_index_key(value, partition_key)
        :root -> Ferricstore.Flow.Keys.root_index_key(value, partition_key)
        :correlation -> Ferricstore.Flow.Keys.correlation_index_key(value, partition_key)
      end

    [key | keys]
  end

  defp terminal_project_metadata_index_key(keys, _kind, _value, _partition_key, _id), do: keys

  defp history_put_many_ops(path, id, partition_key, history_key, expire_at_ms, entries) do
    entries
    |> Enum.flat_map(fn {event_id, event_ms} ->
      compound_key = Ferricstore.Flow.Keys.stream_entry_key(id, event_id, partition_key)
      history_index_key = Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, event_ms)

      value =
        history_index_value(
          path,
          history_index_key,
          event_id,
          event_ms,
          compound_key,
          expire_at_ms
        )

      [
        {:put, history_index_key, value}
      ]
      |> maybe_history_expire_put(expire_at_ms, history_index_key)
      |> Enum.reverse()
    end)
    |> maybe_history_flow_expire_put(expire_at_ms, history_key)
  end

  defp history_index_value(
         path,
         history_index_key,
         event_id,
         event_ms,
         compound_key,
         expire_at_ms
       ) do
    case existing_history_index_location(path, history_index_key) do
      {{:flow_history, _file_id} = file_ref, offset, value_size} ->
        Ferricstore.Flow.LMDB.encode_history_index_value(
          event_id,
          event_ms,
          compound_key,
          expire_at_ms,
          file_ref,
          offset,
          value_size
        )

      nil ->
        Ferricstore.Flow.LMDB.encode_history_index_value(
          event_id,
          event_ms,
          compound_key,
          expire_at_ms
        )
    end
  end

  defp existing_history_index_location(path, history_index_key) do
    with {:ok, value} <- Ferricstore.Flow.LMDB.get(path, history_index_key),
         {:ok,
          {_event_id, _event_ms, _expire_at_ms, _compound_key,
           {:flow_history, _file_id} = file_ref, offset, value_size}} <-
           Ferricstore.Flow.LMDB.decode_history_index_location(value),
         true <- is_integer(offset) and offset >= 0,
         true <- is_integer(value_size) and value_size >= 0 do
      {file_ref, offset, value_size}
    else
      _ -> nil
    end
  rescue
    _ -> nil
  end

  defp maybe_history_expire_put(ops, expire_at_ms, history_index_key) do
    case Ferricstore.Flow.LMDB.history_expire_key(expire_at_ms, history_index_key) do
      nil ->
        ops

      expire_key ->
        [
          {:put, expire_key, Ferricstore.Flow.LMDB.encode_history_expire_value(history_index_key)}
          | ops
        ]
    end
  end

  defp maybe_history_flow_expire_put(ops, expire_at_ms, history_key) do
    case Ferricstore.Flow.LMDB.history_flow_expire_key(expire_at_ms, history_key) do
      nil ->
        ops

      expire_key ->
        ops ++
          [
            {:put, expire_key,
             Ferricstore.Flow.LMDB.encode_history_flow_expire_value(history_key, expire_at_ms)}
          ]
    end
  end

  defp maybe_put_expire_key(ops, terminal_key, value, state_key, count_key) do
    case Ferricstore.Flow.LMDB.decode_terminal_index_value(value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, _state_key}} when expire_at_ms > 0 ->
        expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)

        expire_value =
          Ferricstore.Flow.LMDB.encode_terminal_expire_value(terminal_key, state_key, count_key)

        [{:put, expire_key, expire_value} | ops]

      _ ->
        ops
    end
  end

  defp maybe_delete_old_expire_key(ops, _terminal_key, nil), do: ops

  defp maybe_delete_old_expire_key(ops, terminal_key, old_value) do
    case Ferricstore.Flow.LMDB.decode_terminal_index_value(old_value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, _state_key}} when expire_at_ms > 0 ->
        expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)
        [{:delete, expire_key} | ops]

      _ ->
        ops
    end
  end

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

  defp apply_after_flush(
         {:prune_terminal_flow, ets, zset_index, zset_lookup, state_key, state_index_key, id,
          version}
       ) do
    prune_terminal_state_key(ets, state_key, version)

    safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id)

    :ok
  end

  defp apply_after_flush(
         {:prune_terminal_flow, ets, zset_index, zset_lookup, flow_index, flow_lookup, state_key,
          state_index_key, id, version}
       ) do
    prune_terminal_state_key(ets, state_key, version)

    safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id)
    safe_flow_index_delete_member(flow_index, flow_lookup, state_index_key, id)

    :ok
  end

  defp apply_after_flush(
         {:prune_terminal_flow, ets, zset_index, zset_lookup, flow_index, flow_lookup, state_key,
          state_index_key, metadata_index_keys, id, version}
       ) do
    prune_terminal_state_key(ets, state_key, version)

    safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id)
    safe_flow_index_delete_member(flow_index, flow_lookup, state_index_key, id)

    Enum.each(metadata_index_keys, fn index_key ->
      safe_flow_index_delete_member(flow_index, flow_lookup, index_key, id)
    end)

    :ok
  end

  defp apply_after_flush(
         {:prune_terminal_flow_v2, ets, zset_index, zset_lookup, flow_index, flow_lookup,
          state_key, type, terminal_state, partition_key, parent_flow_id, root_flow_id,
          correlation_id, id, version}
       ) do
    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)

    metadata_index_keys =
      terminal_project_metadata_index_keys(
        id,
        partition_key,
        parent_flow_id,
        root_flow_id,
        correlation_id
      )

    prune_terminal_state_key(ets, state_key, version)

    safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id)
    safe_flow_index_delete_member(flow_index, flow_lookup, state_index_key, id)

    Enum.each(metadata_index_keys, fn index_key ->
      safe_flow_index_delete_member(flow_index, flow_lookup, index_key, id)
    end)

    :ok
  end

  defp apply_after_flush(
         {:prune_terminal_flow_v3, data_dir, shard_index, ets, zset_index, zset_lookup,
          flow_index, flow_lookup, state_key, type, terminal_state, partition_key, parent_flow_id,
          root_flow_id, correlation_id, id, version}
       ) do
    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)

    metadata_index_keys =
      terminal_project_metadata_index_keys(
        id,
        partition_key,
        parent_flow_id,
        root_flow_id,
        correlation_id
      )

    prune_terminal_state_key(data_dir, shard_index, ets, state_key, version)

    safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id)
    safe_flow_index_delete_member(flow_index, flow_lookup, state_index_key, id)

    Enum.each(metadata_index_keys, fn index_key ->
      safe_flow_index_delete_member(flow_index, flow_lookup, index_key, id)
    end)

    :ok
  end

  defp apply_after_flush(
         {:prune_terminal_flow_from_source_v1, data_dir, shard_index, ets, zset_index,
          zset_lookup, flow_index, flow_lookup, state_key, version}
       ) do
    with {:ok, record} <- hot_flow_record_from_ets(ets, state_key),
         ^version <- Map.get(record, :version),
         terminal_state when is_binary(terminal_state) <- Map.get(record, :state),
         true <- Ferricstore.Flow.LMDB.terminal_state?(terminal_state),
         type when is_binary(type) <- Map.get(record, :type),
         id when is_binary(id) <- Map.get(record, :id) do
      partition_key = Map.get(record, :partition_key)
      state_index_key = Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)

      metadata_index_keys =
        terminal_project_metadata_index_keys(
          id,
          partition_key,
          Map.get(record, :parent_flow_id),
          Map.get(record, :root_flow_id),
          Map.get(record, :correlation_id)
        )

      prune_terminal_state_key(data_dir, shard_index, ets, state_key, version)

      safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id)
      safe_flow_index_delete_member(flow_index, flow_lookup, state_index_key, id)

      Enum.each(metadata_index_keys, fn index_key ->
        safe_flow_index_delete_member(flow_index, flow_lookup, index_key, id)
      end)
    end

    :ok
  end

  defp apply_after_flush(
         {:hibernate_flow_evict_hot_v1,
          %{
            data_dir: data_dir,
            shard_index: shard_index,
            ets: ets,
            flow_index: flow_index,
            flow_lookup: flow_lookup,
            state_key: state_key,
            record: record,
            locator: %Locator{} = locator
          } = attrs}
       ) do
    evicted? = hibernate_delete_hot_state_key(data_dir, shard_index, ets, state_key, locator)

    if evicted? do
      zset_index = Map.get(attrs, :zset_index)
      zset_lookup = Map.get(attrs, :zset_lookup)
      id = Map.fetch!(record, :id)

      record
      |> Hibernation.hot_index_keys(due_any?: true)
      |> Enum.each(fn index_key ->
        safe_zset_delete_member(zset_index, zset_lookup, index_key, id)
        safe_flow_index_delete_member(flow_index, flow_lookup, index_key, id)
      end)

      Hibernation.maybe_schedule_claim_waiter(record)
    end

    :telemetry.execute(
      [:ferricstore, :flow, :hibernation, :evict_hot],
      %{count: 1},
      %{result: if(evicted?, do: :evicted, else: :stale), shard_index: shard_index}
    )

    :ok
  end

  defp apply_after_flush({:defer_after_flush, delay_ms, action}) do
    delay_ms = normalize_delay_ms(delay_ms)

    if delay_ms > 0 do
      Process.send_after(self(), {:apply_after_flush, action}, delay_ms)
      :ok
    else
      apply_after_flush(action)
    end
  end

  defp apply_after_flush({:delete_flow_tombstone, ets, key}) do
    case :ets.lookup(ets, key) do
      [{^key, nil, 0, :flow_state_deleted, :deleted, 0, 0}] -> :ets.delete(ets, key)
      _ -> :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp apply_after_flush(_action), do: :ok

  defp hot_flow_record_from_ets(ets, state_key) do
    case :ets.lookup(ets, state_key) do
      [{^state_key, value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}]
      when is_binary(value) ->
        decode_raw_flow_record(value)

      _ ->
        :not_found
    end
  rescue
    ArgumentError -> :not_found
  end

  defp decode_raw_flow_record(value) when is_binary(value) do
    {:ok, flow_call(:decode_record, [value])}
  rescue
    _ -> :error
  end

  defp hibernate_delete_hot_state_key(data_dir, shard_index, ets, state_key, %Locator{} = locator) do
    case :ets.lookup(ets, state_key) do
      [{^state_key, _value, _expire_at_ms, _lfu, file_id, offset, value_size} = row] ->
        if file_id == locator.file_id and offset == locator.offset and
             value_size == locator.value_size do
          delete_apply_projection_cache_for_row(data_dir, shard_index, row)
          :ets.delete(ets, state_key)
          true
        else
          false
        end

      _ ->
        false
    end
  rescue
    ArgumentError -> false
  end

  defp normalize_delay_ms(delay_ms) when is_integer(delay_ms) and delay_ms >= 0, do: delay_ms
  defp normalize_delay_ms(_delay_ms), do: 0

  defp shard_indexes(0), do: []
  defp shard_indexes(shard_count), do: 0..(shard_count - 1)

  defp flush_all_concurrency(0), do: 1

  defp flush_all_concurrency(shard_count) do
    min(shard_count, max(1, min(System.schedulers_online(), 16)))
  end

  defp flush_all_task_timeout(timeout), do: timeout + 1_000

  defp merge_flush_all_result({:ok, {_shard_index, :ok}}, acc), do: acc

  defp merge_flush_all_result({:ok, {_shard_index, {:error, _reason} = error}}, :ok),
    do: error

  defp merge_flush_all_result({:ok, {_shard_index, {:error, _reason}}}, acc), do: acc

  defp merge_flush_all_result({:exit, reason}, :ok), do: {:error, {:flush_task_exit, reason}}

  defp merge_flush_all_result({:exit, _reason}, acc), do: acc

  defp timer_jitter_ms(jitter_ms) do
    jitter_ms = normalize_non_negative_integer(jitter_ms, 0)

    if jitter_ms == 0 do
      0
    else
      :erlang.phash2({self(), System.unique_integer([:monotonic])}, jitter_ms + 1)
    end
  end

  defp normalize_non_negative_integer(value, _default) when is_integer(value) and value >= 0,
    do: value

  defp normalize_non_negative_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= 0 -> parsed
      _ -> default
    end
  end

  defp normalize_non_negative_integer(_value, default), do: default

  defp terminal_hot_ttl_ms do
    :ferricstore
    |> Application.get_env(:flow_terminal_hot_ttl_ms, 0)
    |> normalize_non_negative_integer(0)
  end

  defp safe_zset_delete_member(nil, _zset_lookup, _state_index_key, _id), do: :ok
  defp safe_zset_delete_member(_zset_index, nil, _state_index_key, _id), do: :ok

  defp safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id) do
    Ferricstore.Store.Shard.ZSetIndex.delete_member(
      zset_index,
      zset_lookup,
      state_index_key,
      id
    )
  rescue
    ArgumentError -> :ok
  end

  defp safe_flow_index_delete_member(nil, _flow_lookup, _state_index_key, _id), do: :ok
  defp safe_flow_index_delete_member(_flow_index, nil, _state_index_key, _id), do: :ok

  defp safe_flow_index_delete_member(flow_index, flow_lookup, state_index_key, id) do
    case Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup) do
      nil -> :ok
      native -> Ferricstore.Flow.NativeOrderedIndex.delete_member(native, state_index_key, id)
    end
  rescue
    ArgumentError -> :ok
  end

  defp prune_terminal_state_key(ets, state_key, version) do
    case :ets.lookup(ets, state_key) do
      [
        {^state_key, _value, _expire_at_ms, {:flow_state_version, ^version, _lfu}, _fid, _off,
         _vsize}
      ] ->
        :ets.delete(ets, state_key)

      _ ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp prune_terminal_state_key(data_dir, shard_index, ets, state_key, version) do
    case :ets.lookup(ets, state_key) do
      [
        {^state_key, _value, _expire_at_ms, {:flow_state_version, ^version, _lfu}, _fid, _off,
         _vsize} = row
      ] ->
        delete_apply_projection_cache_for_row(data_dir, shard_index, row)
        :ets.delete(ets, state_key)

      _ ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp delete_apply_projection_cache_for_row(
         data_dir,
         shard_index,
         {key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, index}, _offset,
          _value_size}
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(key) and is_integer(index) and index > 0 do
    Ferricstore.Raft.WARaftSegmentReader.delete_apply_projection_entries(data_dir, shard_index, [
      {index, key}
    ])

    :ok
  rescue
    _ -> :ok
  end

  defp delete_apply_projection_cache_for_row(_data_dir, _shard_index, _row), do: :ok

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

  defp publish_durable(%{flow_lmdb_replay_safe_index: replay_safe_index}, shard_index, index)
       when is_reference(replay_safe_index) do
    if shard_index < :atomics.info(replay_safe_index).size do
      :atomics.put(replay_safe_index, shard_index + 1, index)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp publish_durable(_instance_ctx, _shard_index, _index), do: :ok

  defp publish_requested(
         %{flow_lmdb_replay_safe_requested_index: requested_index},
         shard_index,
         index
       )
       when is_reference(requested_index) do
    put_atomic_max(requested_index, shard_index, index)
  rescue
    _ -> :ok
  end

  defp publish_requested(_instance_ctx, _shard_index, _index), do: :ok

  defp record_persist_failure(%{flow_lmdb_replay_safe_persist_failures: failures}, shard_index)
       when is_reference(failures) do
    if shard_index < :atomics.info(failures).size do
      :atomics.add(failures, shard_index + 1, 1)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp record_persist_failure(_instance_ctx, _shard_index), do: :ok

  defp record_flush_failure(%{flow_lmdb_writer_flush_failures: failures}, shard_index)
       when is_reference(failures) do
    if shard_index < :atomics.info(failures).size do
      :atomics.add(failures, shard_index + 1, 1)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp record_flush_failure(_instance_ctx, _shard_index), do: :ok

  defp mark_mirror_degraded(
         %{flow_lmdb_mirror_degraded: degraded},
         shard_index,
         reason
       )
       when is_reference(degraded) do
    if shard_index < :atomics.info(degraded).size do
      :atomics.put(degraded, shard_index + 1, 1)
    end

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_mirror, :degraded],
      %{count: 1},
      %{shard_index: shard_index, reason: reason, source: :flush}
    )

    :ok
  rescue
    _ -> :ok
  end

  defp mark_mirror_degraded(_instance_ctx, _shard_index, _reason), do: :ok

  defp put_atomic_max(ref, shard_index, value) do
    if shard_index < :atomics.info(ref).size do
      position = shard_index + 1
      current = :atomics.get(ref, position)

      if value > current do
        :atomics.put(ref, position, value)
      end
    end

    :ok
  end

  defp emit_backlog(state, now) do
    pending_age_us = pending_age_us(state, now)
    publish_backlog(state, pending_age_us)

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_writer, :backlog],
      %{
        pending_ops: state.count,
        pending_after_flush: length(state.pending_after_flush),
        oldest_pending_age_us: pending_age_us,
        requested_index: state.requested_index,
        durable_index: state.durable_index,
        replay_safe_lag: replay_safe_lag(state)
      },
      writer_metadata(state)
    )
  end

  defp publish_backlog(state, pending_age_us) do
    publish_atomic(
      state.instance_ctx,
      :flow_lmdb_writer_pending_ops,
      state.shard_index,
      state.count
    )

    publish_atomic(
      state.instance_ctx,
      :flow_lmdb_writer_oldest_pending_age_us,
      state.shard_index,
      pending_age_us
    )
  end

  defp publish_atomic(ctx, field, shard_index, value) when is_map(ctx) do
    case Map.get(ctx, field) do
      ref when is_reference(ref) ->
        if shard_index < :atomics.info(ref).size do
          :atomics.put(ref, shard_index + 1, max(value, 0))
        end

        :ok

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  end

  defp publish_atomic(_ctx, _field, _shard_index, _value), do: :ok

  defp emit_flush(status, state, started_at, op_count, expanded_op_count, pending_age_us) do
    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_writer, :flush],
      %{
        duration_us: duration_us(started_at),
        op_count: op_count,
        expanded_op_count: expanded_op_count,
        pending_age_us: pending_age_us,
        requested_index: state.requested_index,
        durable_index: state.durable_index,
        replay_safe_lag: replay_safe_lag(state)
      },
      state
      |> writer_metadata()
      |> Map.put(:status, persist_status(status))
      |> Map.put(:reason, persist_reason(status))
    )
  end

  defp emit_persist(status, state, index, started_at) do
    requested_index = max(state.requested_index, index)
    durable_index = if status == :ok, do: index, else: state.durable_index

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_replay_safe_index, :persist],
      %{
        duration_us: duration_us(started_at),
        index: index,
        requested_index: requested_index,
        durable_index: durable_index,
        lag: max(requested_index - durable_index, 0)
      },
      %{
        status: persist_status(status),
        shard_index: state.shard_index,
        reason: persist_reason(status)
      }
    )
  end

  defp writer_unavailable(operation, instance_name, shard_index, reason, op_count) do
    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_writer, :unavailable],
      %{op_count: op_count},
      %{
        operation: operation,
        instance_name: instance_name,
        shard_index: shard_index,
        reason: reason
      }
    )

    {:error, reason}
  end

  defp persist_status(:ok), do: :ok
  defp persist_status({:error, _}), do: :error

  defp persist_reason(:ok), do: :none
  defp persist_reason({:error, reason}), do: reason

  defp replay_safe_lag(state), do: max(state.requested_index - state.durable_index, 0)

  defp writer_metadata(state) do
    %{
      shard_index: state.shard_index,
      instance_name: state.instance_name
    }
  end

  defp pending_age_us(%{first_pending_at: nil}, _now), do: 0

  defp pending_age_us(%{first_pending_at: first_pending_at}, now) do
    now
    |> Kernel.-(first_pending_at)
    |> System.convert_time_unit(:native, :microsecond)
    |> max(0)
  end

  defp duration_us(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :microsecond)
  end
end
