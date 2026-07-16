defmodule Ferricstore.Flow.HistoryProjector do
  @moduledoc false

  use GenServer
  require Logger
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.HistoryProjector.Config
  alias Ferricstore.Flow.HistoryProjector.Keydir
  alias Ferricstore.Flow.HistoryProjector.Pending
  alias Ferricstore.Flow.HistoryProjector.Recovery
  alias Ferricstore.Flow.HistoryProjector.Storage
  alias Ferricstore.Flow.HistoryProjector.Telemetry
  alias Ferricstore.Flow.HistoryProjector.Trim
  alias Ferricstore.Flow.HistoryProjector.ValueProjection
  alias Ferricstore.Flow.HistoryProjectedIndex
  alias Ferricstore.Store.{DiskPressure, WriteVersion}

  @retry_interval_ms 50
  @flush_drain_limit 16

  @type entry :: %{
          required(:key) => binary(),
          required(:expire_at_ms) => non_neg_integer(),
          required(:history_key) => binary(),
          required(:event_id) => binary(),
          required(:event_ms) => non_neg_integer(),
          required(:version) => non_neg_integer(),
          optional(:value) => binary(),
          optional(:record) => map(),
          optional(:event) => binary(),
          optional(:now_ms) => non_neg_integer(),
          optional(:meta) => map(),
          optional(:terminal?) => boolean(),
          optional(:history_max_events) => pos_integer(),
          optional(:ra_index) => non_neg_integer() | nil
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    instance_ctx = Keyword.get(opts, :instance_ctx)
    GenServer.start_link(__MODULE__, opts, name: name(instance_ctx, shard_index))
  end

  @spec recover(map() | nil, non_neg_integer(), binary(), :ets.tid() | atom() | nil) ::
          :ok | {:error, term()}
  def recover(instance_ctx, shard_index, shard_data_path, keydir_override \\ nil) do
    case ensure_history_file(shard_data_path) do
      :ok ->
        projected = HistoryProjectedIndex.read(shard_data_path)
        publish_projected_index(instance_ctx, shard_index, shard_data_path, projected)

        if Recovery.skip_history_log_recover?(shard_data_path, projected) do
          :ok
        else
          case Recovery.recover_history_log(
                 instance_ctx,
                 shard_index,
                 shard_data_path,
                 keydir_override
               ) do
            :ok ->
              :ok

            {:error, reason} = error ->
              emit_recover_error(instance_ctx, shard_index, reason)
              error
          end
        end

      {:error, reason} = error ->
        emit_recover_error(instance_ctx, shard_index, {:ensure_history_file_failed, reason})
        error

      other ->
        reason = {:ensure_history_file_failed, other}
        emit_recover_error(instance_ctx, shard_index, reason)
        {:error, reason}
    end
  rescue
    error ->
      reason = {:history_projector_recover_failed, error}
      emit_recover_error(instance_ctx, shard_index, reason)
      {:error, reason}
  end

  @spec name(map() | nil, non_neg_integer()) :: atom()
  def name(nil, shard_index), do: :"ferricstore_flow_history_projector_#{shard_index}"
  def name(%{name: :default}, shard_index), do: name(nil, shard_index)
  def name(%{name: name}, shard_index), do: :"#{name}_flow_history_projector_#{shard_index}"
  def name(%{}, shard_index), do: name(nil, shard_index)

  @spec enqueue(map() | nil, non_neg_integer(), [entry()], non_neg_integer() | nil) ::
          :ok | {:error, :not_started}
  def enqueue(_instance_ctx, _shard_index, [], _ra_index), do: :ok

  def enqueue(instance_ctx, shard_index, entries, ra_index) when is_list(entries) do
    projector = name(instance_ctx, shard_index)

    case Process.whereis(projector) do
      nil ->
        {:error, :not_started}

      _pid ->
        GenServer.call(projector, {:enqueue, stamp_ra_index(entries, ra_index)}, 5_000)
    end
  catch
    :exit, _reason -> {:error, :not_started}
  end

  @spec enqueue_async(map() | nil, non_neg_integer(), [entry()], non_neg_integer() | nil) ::
          :ok | {:error, :not_started}
  def enqueue_async(_instance_ctx, _shard_index, [], _ra_index), do: :ok

  def enqueue_async(instance_ctx, shard_index, entries, ra_index) when is_list(entries) do
    projector = name(instance_ctx, shard_index)
    entries = stamp_ra_index(entries, ra_index)

    case Process.whereis(projector) do
      nil ->
        enqueue_overflow(instance_ctx, shard_index, projector, nil, entries)

      pid when is_pid(pid) ->
        case Pending.reserve_pending(projector, length(entries)) do
          :ok ->
            case Pending.reserve_replay_range(projector, entries) do
              :ok ->
                # This is used from Raft apply. It must never wait behind cold history
                # projection or LMDB work; release-cursor gating handles durability.
                GenServer.cast(pid, {:enqueue_reserved, entries})
                :ok

              {:error, reason} ->
                _ = Pending.release_pending(projector, length(entries))
                {:error, {:replay_reservation_failed, reason}}
            end

          {:error, :queue_full, pending_entries, max_pending_entries} ->
            mark_queue_full(instance_ctx, shard_index)

            emit_queue_full(
              instance_ctx,
              shard_index,
              pending_entries,
              length(entries),
              max_pending_entries
            )

            enqueue_overflow(instance_ctx, shard_index, projector, pid, entries)

          {:error, :not_registered} ->
            enqueue_overflow(instance_ctx, shard_index, projector, pid, entries)
        end
    end
  catch
    :exit, _reason -> {:error, :not_started}
  end

  defp enqueue_overflow(instance_ctx, shard_index, projector, pid, entries) do
    case Pending.append_overflow(projector, entries) do
      {:ok, sequence} ->
        case Pending.reserve_replay_range(projector, entries) do
          :ok ->
            case Pending.commit_overflow(projector, sequence) do
              :ok ->
                _ = pid
                :ok

              {:error, reason} ->
                mark_queue_full(instance_ctx, shard_index)
                {:error, {:overflow_commit_failed, reason}}
            end

          {:error, reason} ->
            _ = Pending.delete_overflow(projector, sequence)
            mark_queue_full(instance_ctx, shard_index)
            {:error, {:replay_reservation_failed, reason}}
        end

      {:error, reason} ->
        mark_queue_full(instance_ctx, shard_index)
        {:error, {:overflow_failed, reason}}
    end
  end

  @spec flush(map() | nil, non_neg_integer(), timeout()) :: :ok | {:error, term()}
  def flush(instance_ctx, shard_index, timeout \\ 10_000) do
    projector = name(instance_ctx, shard_index)

    case Process.whereis(projector) do
      nil -> :ok
      _pid -> GenServer.call(projector, :flush, timeout)
    end
  catch
    :exit, reason -> {:error, {:flush_exit, reason}}
  end

  @spec discard(map() | nil, non_neg_integer(), timeout()) :: :ok | {:error, term()}
  def discard(instance_ctx, shard_index, timeout \\ 5_000) do
    projector = name(instance_ctx, shard_index)

    case Process.whereis(projector) do
      nil -> :ok
      _pid -> GenServer.call(projector, :discard, timeout)
    end
  catch
    :exit, reason -> {:error, {:discard_exit, reason}}
  end

  @spec reset_after_flush(map() | nil, non_neg_integer(), non_neg_integer(), timeout()) ::
          :ok | {:error, term()}
  def reset_after_flush(instance_ctx, shard_index, index, timeout \\ 5_000)
      when is_integer(shard_index) and shard_index >= 0 and is_integer(index) and index >= 0 do
    projector = name(instance_ctx, shard_index)

    case Process.whereis(projector) do
      nil -> Pending.discard(projector)
      _pid -> GenServer.call(projector, {:reset_after_flush, index}, timeout)
    end
  catch
    :exit, reason -> {:error, {:reset_after_flush_exit, reason}}
  end

  @spec reset_projected_index(map() | nil, non_neg_integer(), binary(), non_neg_integer()) ::
          :ok | {:error, term()}
  def reset_projected_index(instance_ctx, shard_index, shard_data_path, index)
      when is_integer(shard_index) and shard_index >= 0 and is_binary(shard_data_path) and
             is_integer(index) and index >= 0 do
    with :ok <- HistoryProjectedIndex.reset(shard_data_path, index) do
      case instance_ctx do
        %{flow_history_projected_index: ref} when is_reference(ref) ->
          size = :atomics.info(ref).size
          if shard_index < size, do: :atomics.put(ref, shard_index + 1, index)

        _ ->
          :ok
      end
    end
  rescue
    error -> {:error, {:history_projected_index_reset_failed, error}}
  end

  @spec pending_count(map() | nil, non_neg_integer(), timeout()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def pending_count(instance_ctx, shard_index, timeout \\ 1_000) do
    projector = name(instance_ctx, shard_index)

    case Process.whereis(projector) do
      nil -> {:error, :not_started}
      _pid -> GenServer.call(projector, :pending_count, timeout)
    end
  catch
    :exit, reason -> {:error, {:pending_count_exit, reason}}
  end

  @spec request(map() | nil, non_neg_integer(), binary(), non_neg_integer()) ::
          :durable | :requested
  def request(instance_ctx, shard_index, shard_data_path, index) do
    publish_requested_index(instance_ctx, shard_index, index)

    if durable?(instance_ctx, shard_index, shard_data_path, index) do
      :durable
    else
      projector = name(instance_ctx, shard_index)

      case Process.whereis(projector) do
        nil ->
          :requested

        _pid ->
          GenServer.cast(projector, {:project_to, index})
          :requested
      end
    end
  end

  @spec durable?(map() | nil, non_neg_integer(), binary(), non_neg_integer()) :: boolean()
  def durable?(_instance_ctx, _shard_index, _shard_data_path, index)
      when not is_integer(index) or index <= 0,
      do: true

  def durable?(instance_ctx, shard_index, shard_data_path, index) do
    projected_index(instance_ctx, shard_index, shard_data_path) >= index
  end

  @doc false
  def __trim_cap_requirements_for_test__(entries, load_cap_fun)
      when is_list(entries) and is_function(load_cap_fun, 1) do
    Trim.history_cap_requirements(entries, load_cap_fun)
  end

  @doc false
  def __direct_hot_history_evict_items_for_test__(entries) when is_list(entries) do
    Trim.direct_hot_history_evict_items(entries)
  end

  @doc false
  def __history_hot_rank_entries_for_test__(entries) when is_list(entries) do
    Trim.history_hot_rank_entries(entries)
  end

  @doc false
  def __skip_history_log_recover_for_test__(shard_data_path, projected) do
    Recovery.skip_history_log_recover?(shard_data_path, projected)
  end

  @spec write_entries_sync(
          map() | nil,
          non_neg_integer(),
          binary(),
          [entry()],
          non_neg_integer() | nil,
          keyword()
        ) :: :ok | {:error, term()}
  def write_entries_sync(
        instance_ctx,
        shard_index,
        shard_data_path,
        entries,
        requested_index \\ nil,
        opts \\ []
      ) do
    do_project(
      instance_ctx,
      shard_index,
      shard_data_path,
      entries,
      requested_index,
      Keyword.get(opts, :keydir)
    )
  end

  @impl true
  def init(opts) do
    Process.flag(:priority, :low)

    shard_index = Keyword.fetch!(opts, :shard_index)
    shard_data_path = Keyword.fetch!(opts, :shard_data_path)
    instance_ctx = Keyword.get(opts, :instance_ctx)
    projector_name = name(instance_ctx, shard_index)

    :ok =
      if Keyword.get(opts, :recover_on_init, true) do
        recover(instance_ctx, shard_index, shard_data_path)
      else
        Recovery.prepare_recovered_history_projector(instance_ctx, shard_index, shard_data_path)
      end

    max_pending_entries =
      Ferricstore.MemoryBudget.limit(
        :flow_history_projector_max_pending_entries,
        Config.default_max_pending_entries()
      )

    pending_counter = :atomics.new(1, signed: true)
    Pending.register_pending_counter(projector_name, pending_counter, max_pending_entries)
    projected_index = projected_index(instance_ctx, shard_index, shard_data_path)
    flushed_index = max(projected_index, Pending.replay_reservation_flushed_index(projector_name))

    state =
      Config.initial_state(
        projector_name,
        shard_index,
        shard_data_path,
        instance_ctx,
        pending_counter,
        max_pending_entries,
        flushed_index
      )

    Process.send_after(self(), :drain_overflow, 0)
    {:ok, state}
  end

  @impl true
  def handle_cast(:drain_overflow, state) do
    {:noreply, drain_overflow(state)}
  end

  def handle_cast({:enqueue_reserved, entries}, state) do
    {:noreply, enqueue_entries(state, entries)}
  end

  def handle_cast({:enqueue, entries}, state) do
    case Pending.reserve_pending(state, length(entries)) do
      :ok ->
        {:noreply, enqueue_entries(state, entries)}

      {:error, :queue_full, pending_entries, max_pending_entries} ->
        mark_queue_full(state.instance_ctx, state.shard_index)

        emit_queue_full(
          state.instance_ctx,
          state.shard_index,
          pending_entries,
          length(entries),
          max_pending_entries
        )

        {:noreply, state}
    end
  end

  def handle_cast({:project_to, index}, state) do
    requested_index = max_index(state.requested_index, index)
    {:noreply, flush_pending(%{state | requested_index: requested_index})}
  end

  @impl true
  def handle_call(:flush, _from, state) do
    state = flush_until_idle(state)

    result =
      if state.pending_count == 0 and state.requested_index == nil,
        do: :ok,
        else: {:error, :flush_failed}

    {:reply, result, state}
  end

  def handle_call(:discard, _from, state) do
    state = cancel_flush(state)

    case Pending.discard(state.projector_name) do
      :ok ->
        :ok = Pending.release_pending(state, state.pending_count)

        state =
          state
          |> Map.merge(%{
            pending: [],
            pending_count: 0,
            first_pending_at: nil,
            requested_index: nil
          })
          |> publish_backlog_state()

        {:reply, :ok, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:reset_after_flush, index}, _from, state)
      when is_integer(index) and index >= 0 do
    state = cancel_flush(state)

    case Pending.discard(state.projector_name) do
      :ok ->
        :ok = Pending.release_pending(state, state.pending_count)

        state =
          state
          |> Map.merge(%{
            pending: [],
            pending_count: 0,
            first_pending_at: nil,
            requested_index: nil,
            flushed_index: index
          })
          |> publish_backlog_state()

        {:reply, :ok, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(:pending_count, _from, state) do
    {:reply, {:ok, state.pending_count}, state}
  end

  def handle_call({:enqueue, entries}, _from, state) do
    case Pending.reserve_pending(state, length(entries)) do
      :ok ->
        {:reply, :ok, enqueue_entries(state, entries)}

      {:error, :queue_full, pending_entries, max_pending_entries} ->
        mark_queue_full(state.instance_ctx, state.shard_index)

        emit_queue_full(
          state.instance_ctx,
          state.shard_index,
          pending_entries,
          length(entries),
          max_pending_entries
        )

        {:reply, {:error, :queue_full}, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    Pending.unregister_pending_counter(Map.get(state, :projector_name))
    :ok
  end

  @impl true
  def handle_info(:flush_timer, state) do
    {:noreply, flush_pending(%{state | flush_timer: nil}, :chunk)}
  end

  def handle_info(:drain_overflow, state) do
    {:noreply, drain_overflow(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp flush_until_idle(state, attempts \\ @flush_drain_limit)

  defp flush_until_idle(state, attempts) when attempts <= 0, do: state

  defp flush_until_idle(state, attempts) do
    drained_state = drain_overflow(state)
    next_state = flush_pending(drained_state)

    cond do
      not flush_progress?(drained_state, next_state) ->
        next_state

      next_state.pending_count == 0 and next_state.requested_index == nil ->
        case drain_overflow(next_state) do
          %{pending_count: 0, requested_index: nil} = idle_state -> idle_state
          active_state -> flush_until_idle(active_state, attempts - 1)
        end

      true ->
        flush_until_idle(next_state, attempts - 1)
    end
  end

  defp flush_progress?(before, after_state) do
    after_state.pending_count < before.pending_count or
      after_state.requested_index != before.requested_index or
      after_state.flushed_index != before.flushed_index
  end

  defp drain_overflow(state) do
    case overflow_drain_limit(state) do
      0 ->
        schedule_overflow_drain(state)

      limit ->
        drain_overflow_batch(state, limit)
    end
  end

  defp drain_overflow_batch(state, limit) do
    case Pending.reserve_pending(state, limit) do
      :ok ->
        case Pending.take_overflow(state.projector_name, limit) do
          {:ok, []} ->
            :ok = Pending.release_pending(state, limit)
            state

          {:ok, entries} ->
            :ok = Pending.release_pending(state, limit - length(entries))
            enqueue_entries(state, entries)

          {:error, reason} ->
            :ok = Pending.release_pending(state, limit)
            Logger.error("Flow history projector overflow drain failed: #{inspect(reason)}")
            schedule_overflow_drain(state)
        end

      {:error, :queue_full, pending_entries, max_pending_entries} ->
        mark_queue_full(state.instance_ctx, state.shard_index)

        emit_queue_full(
          state.instance_ctx,
          state.shard_index,
          pending_entries,
          limit,
          max_pending_entries
        )

        schedule_overflow_drain(state)
    end
  end

  defp overflow_drain_limit(%{max_pending_entries: :infinity, batch_size: batch_size}),
    do: batch_size

  defp overflow_drain_limit(%{
         pending_counter: counter,
         max_pending_entries: max_pending_entries,
         batch_size: batch_size
       }) do
    current = max(:atomics.get(counter, 1), 0)
    min(max(max_pending_entries - current, 0), batch_size)
  rescue
    _ -> 0
  end

  defp schedule_overflow_drain(state) do
    Process.send_after(self(), :drain_overflow, @retry_interval_ms)
    state
  end

  defp enqueue_entries(state, entries) do
    pending = Enum.reverse(entries, state.pending)
    count = state.pending_count + length(entries)
    first_pending_at = state.first_pending_at || System.monotonic_time(:microsecond)

    state =
      %{state | pending: pending, pending_count: count, first_pending_at: first_pending_at}
      |> publish_backlog_state()

    if count >= state.batch_size do
      schedule_flush_soon(state)
    else
      schedule_flush(state)
    end
  end

  defp flush_pending(state, mode \\ :all)

  defp flush_pending(%{pending_count: 0, requested_index: nil} = state, _mode),
    do: cancel_flush(state)

  defp flush_pending(%{requested_index: nil} = state, :chunk) do
    state = cancel_flush(state)
    entries = Enum.reverse(state.pending)
    {chunk, rest} = Enum.split(entries, state.batch_size)

    case do_project(
           state.instance_ctx,
           state.shard_index,
           state.shard_data_path,
           chunk,
           nil,
           nil
         ) do
      :ok ->
        Pending.release_pending(state, length(chunk))
        flushed_index = max_index(state.flushed_index, max_projected_index(chunk, nil))
        Pending.mark_replay_range_flushed(state.projector_name, flushed_index)

        first_pending_at = if rest == [], do: nil, else: state.first_pending_at

        next_state =
          state
          |> Map.merge(%{
            pending: Enum.reverse(rest),
            pending_count: length(rest),
            first_pending_at: first_pending_at,
            flushed_index: flushed_index
          })
          |> reset_retry_backoff()
          |> publish_backlog_state()

        next_state
        |> maybe_schedule_next_chunk()
        |> drain_overflow()

      {:error, reason} ->
        mark_flush_failure(state.instance_ctx, state.shard_index)

        Logger.error(
          "Flow history projector shard_#{state.shard_index}: flush failed: #{inspect(reason)}"
        )

        schedule_retry(state)
    end
  end

  defp flush_pending(%{pending_count: 0, requested_index: requested_index} = state, _mode)
       when is_integer(requested_index) and requested_index >= 0 do
    state = cancel_flush(state)

    if state.flushed_index >= requested_index do
      case publish_projected_index(
             state.instance_ctx,
             state.shard_index,
             state.shard_data_path,
             requested_index
           ) do
        :ok ->
          Pending.trim_replay_reservation(state.projector_name, requested_index)

          %{
            state
            | requested_index: nil,
              flushed_index: max(state.flushed_index, requested_index)
          }
          |> reset_retry_backoff()
          |> publish_backlog_state()

        {:error, reason} ->
          mark_flush_failure(state.instance_ctx, state.shard_index)

          Logger.error(
            "Flow history projector shard_#{state.shard_index}: projected index publish failed: #{inspect(reason)}"
          )

          schedule_retry(state)
      end
    else
      state
    end
  end

  defp flush_pending(state, _mode) do
    state = cancel_flush(state)
    entries = Enum.reverse(state.pending)
    projected_index = max_projected_index(entries, state.requested_index)

    case do_project(
           state.instance_ctx,
           state.shard_index,
           state.shard_data_path,
           entries,
           state.requested_index,
           nil
         ) do
      :ok ->
        Pending.release_pending(state, length(entries))
        Pending.mark_replay_range_flushed(state.projector_name, projected_index)
        Pending.trim_replay_reservation(state.projector_name, projected_index)

        %{
          state
          | pending: [],
            pending_count: 0,
            first_pending_at: nil,
            requested_index: nil,
            flushed_index: max_index(state.flushed_index, projected_index)
        }
        |> reset_retry_backoff()
        |> publish_backlog_state()
        |> drain_overflow()

      {:error, reason} ->
        mark_flush_failure(state.instance_ctx, state.shard_index)

        Logger.error(
          "Flow history projector shard_#{state.shard_index}: flush failed: #{inspect(reason)}"
        )

        schedule_retry(state)
    end
  end

  defp do_project(
         instance_ctx,
         shard_index,
         shard_data_path,
         [],
         requested_index,
         _keydir_override
       ) do
    case requested_index do
      index when is_integer(index) and index >= 0 ->
        with {:ok, _file_id, file_path} <- Storage.history_active_file(shard_data_path),
             :ok <- NIF.v2_fsync(file_path) do
          publish_projected_index(instance_ctx, shard_index, shard_data_path, index)
        end

      _ ->
        :ok
    end
  rescue
    error ->
      set_disk_pressure(instance_ctx, shard_index)
      {:error, {:history_projection_exception, error}}
  end

  defp do_project(
         instance_ctx,
         shard_index,
         shard_data_path,
         entries,
         requested_index,
         keydir_override
       ) do
    with {:ok, file_id, file_path} <- Storage.history_active_file(shard_data_path),
         keydir <- keydir_override || keydir(instance_ctx, shard_index),
         expanded_entries <- Storage.expand_entries(entries),
         encoded_entries <- Enum.map(expanded_entries, &Storage.encode_entry/1),
         :ok <- Storage.validate_entries(encoded_entries),
         batch <- Enum.map(encoded_entries, &{&1.key, &1.value, &1.expire_at_ms}),
         {:ok, locations} <- Storage.append_batch(file_path, batch),
         :ok <- Storage.validate_locations(encoded_entries, locations),
         :ok <- Storage.sync_history_log_before_publish(file_path) do
      clear_disk_pressure(instance_ctx, shard_index)

      with :ok <-
             Storage.publish_lmdb_history_locations(
               shard_data_path,
               file_id,
               encoded_entries,
               locations
             ),
           :ok <-
             Storage.publish_keydir_entries(
               instance_ctx,
               shard_index,
               keydir,
               file_id,
               encoded_entries,
               locations
             ),
           :ok <- Storage.publish_history_index(instance_ctx, shard_index, encoded_entries),
           :ok <-
             compact_projected_flow_values(
               instance_ctx,
               shard_index,
               shard_data_path,
               keydir,
               file_path,
               file_id,
               encoded_entries
             ),
           :ok <-
             trim_history_caps(
               instance_ctx,
               shard_index,
               shard_data_path,
               keydir,
               file_path,
               encoded_entries
             ),
           :ok <- trim_history_hot_cache(instance_ctx, shard_index, keydir, encoded_entries) do
        bump_write_version(instance_ctx, shard_index)

        Storage.maybe_persist_projected_index(
          instance_ctx,
          shard_index,
          shard_data_path,
          file_path,
          max_projected_index(entries, requested_index),
          requested_index
        )
      end
    else
      {:error, _reason} = error ->
        set_disk_pressure(instance_ctx, shard_index)
        error

      other ->
        set_disk_pressure(instance_ctx, shard_index)
        {:error, other}
    end
  rescue
    error ->
      set_disk_pressure(instance_ctx, shard_index)
      {:error, {:history_projection_exception, error}}
  end

  defdelegate history_dir(shard_data_path), to: Storage
  defdelegate history_file_path(shard_data_path, file_id), to: Storage

  defdelegate read_value(shard_data_path, file_id, offset),
    to: Ferricstore.Flow.HistoryProjector.Log

  defdelegate scan_event_value(shard_data_path, target_key),
    to: Ferricstore.Flow.HistoryProjector.Log

  def ensure_history_file(shard_data_path), do: Storage.ensure_history_file(shard_data_path)
  def append_batch(file_path, batch), do: Storage.append_batch(file_path, batch)

  def sync_history_log_before_publish(file_path),
    do: Storage.sync_history_log_before_publish(file_path)

  def write_lmdb_ops(shard_data_path, ops), do: Storage.write_lmdb_ops(shard_data_path, ops)
  def flow_call(function, args), do: Storage.flow_call(function, args)

  def publish_keydir_entries(instance_ctx, shard_index, keydir, file_id, entries, locations) do
    Storage.publish_keydir_entries(instance_ctx, shard_index, keydir, file_id, entries, locations)
  end

  def publish_history_index(instance_ctx, shard_index, entries),
    do: Storage.publish_history_index(instance_ctx, shard_index, entries)

  def publish_lmdb_history_locations(shard_data_path, file_id, entries, locations) do
    Storage.publish_lmdb_history_locations(shard_data_path, file_id, entries, locations)
  end

  @doc false
  def __history_index_entries_for_test__(entries), do: Storage.history_index_entries(entries)

  defp compact_projected_flow_values(
         instance_ctx,
         shard_index,
         shard_data_path,
         keydir,
         file_path,
         file_id,
         entries
       ) do
    ValueProjection.compact_projected_flow_values(
      instance_ctx,
      shard_index,
      shard_data_path,
      keydir,
      file_path,
      file_id,
      entries
    )
  end

  @doc false
  def __projected_flow_value_keydir_refs_for_test__(keydir, refs) do
    ValueProjection.projected_flow_value_keydir_refs(keydir, refs)
  end

  @doc false
  def __projected_flow_value_refs_for_test__(entries) do
    ValueProjection.projected_flow_value_refs(entries)
  end

  defp trim_history_caps(instance_ctx, shard_index, shard_data_path, keydir, file_path, entries) do
    Trim.trim_history_caps(
      instance_ctx,
      shard_index,
      shard_data_path,
      keydir,
      file_path,
      entries,
      history_trim_callbacks()
    )
  end

  def trim_history_hot_cache(instance_ctx, shard_index, keydir, entries) do
    Trim.trim_history_hot_cache(
      instance_ctx,
      shard_index,
      keydir,
      entries,
      history_trim_callbacks()
    )
  end

  defp history_trim_callbacks do
    %{
      delete_keydir_row: &delete_keydir_row/4,
      load_history_max_cap: &load_history_max_cap/3,
      sync_history_log_before_publish: &Storage.sync_history_log_before_publish/1
    }
  end

  defp load_history_max_cap(history_key, keydir, shard_data_path) do
    with {:ok, state_key} <- Recovery.history_state_key(history_key),
         {:ok, %{history_max_events: max_events}} <-
           Recovery.load_history_state_record(state_key, keydir, shard_data_path),
         true <- is_integer(max_events) and max_events > 0 do
      max_events
    else
      _ -> nil
    end
  end

  defp emit_recover_error(instance_ctx, shard_index, reason) do
    Telemetry.emit_recover_error(instance_ctx, shard_index, reason)
  end

  defp emit_queue_full(
         instance_ctx,
         shard_index,
         pending_entries,
         incoming_entries,
         max_pending_entries
       ) do
    Telemetry.emit_queue_full(
      instance_ctx,
      shard_index,
      pending_entries,
      incoming_entries,
      max_pending_entries
    )
  end

  defp publish_requested_index(instance_ctx, shard_index, index)
       when is_integer(index) and index >= 0 do
    Telemetry.publish_requested_index(instance_ctx, shard_index, index)
  end

  defp publish_requested_index(_instance_ctx, _shard_index, _index), do: :ok

  defp publish_backlog_state(%{pending_count: pending_count} = state) do
    publish_atomic(
      state.instance_ctx,
      :flow_history_projector_pending_entries,
      state.shard_index,
      max(pending_count, 0)
    )

    publish_atomic(
      state.instance_ctx,
      :flow_history_projector_oldest_pending_age_us,
      state.shard_index,
      history_pending_age_us(state)
    )

    state
  end

  defp history_pending_age_us(%{pending_count: count}) when count <= 0, do: 0

  defp history_pending_age_us(%{first_pending_at: first_pending_at})
       when is_integer(first_pending_at) do
    max(System.monotonic_time(:microsecond) - first_pending_at, 0)
  end

  defp history_pending_age_us(_state), do: 0

  defp mark_queue_full(instance_ctx, shard_index) do
    Telemetry.mark_queue_full(instance_ctx, shard_index)
  end

  defp publish_atomic(instance_ctx, field, shard_index, value)
       when is_integer(shard_index) and shard_index >= 0 and is_integer(value) and value >= 0 do
    case Map.get(instance_ctx || %{}, field) do
      ref when is_reference(ref) ->
        size = :atomics.info(ref).size
        if shard_index < size, do: :atomics.put(ref, shard_index + 1, value)

      _ ->
        :ok
    end

    :ok
  rescue
    _ -> :ok
  end

  defp projected_index(%{flow_history_projected_index: ref}, shard_index, _shard_data_path)
       when is_reference(ref) do
    size = :atomics.info(ref).size
    if shard_index < size, do: :atomics.get(ref, shard_index + 1), else: 0
  rescue
    _ -> 0
  end

  defp projected_index(_instance_ctx, _shard_index, shard_data_path) do
    HistoryProjectedIndex.read(shard_data_path)
  end

  def publish_projected_index(_instance_ctx, _shard_index, _shard_data_path, nil), do: :ok

  def publish_projected_index(instance_ctx, shard_index, shard_data_path, index)
      when is_integer(index) and index >= 0 do
    index = max(index, projected_index(instance_ctx, shard_index, shard_data_path))

    with :ok <- HistoryProjectedIndex.persist(shard_data_path, index) do
      case instance_ctx do
        %{flow_history_projected_index: ref} when is_reference(ref) ->
          size = :atomics.info(ref).size
          if shard_index < size, do: :atomics.put(ref, shard_index + 1, index)

        _ ->
          :ok
      end
    end
  end

  defp max_projected_index(entries, requested_index) do
    Enum.reduce(entries, requested_index, fn entry, acc ->
      max_index(acc, Map.get(entry, :ra_index))
    end)
  end

  defp max_index(nil, nil), do: nil
  defp max_index(nil, idx) when is_integer(idx), do: idx
  defp max_index(idx, nil) when is_integer(idx), do: idx
  defp max_index(a, b) when is_integer(a) and is_integer(b), do: max(a, b)
  defp max_index(a, _b), do: a
  def keydir(%{keydir_refs: refs}, shard_index) when is_tuple(refs), do: elem(refs, shard_index)
  def keydir(_instance_ctx, shard_index), do: :"keydir_#{shard_index}"

  def instance_name(%{name: name}), do: name
  def instance_name(_instance_ctx), do: :default

  defp set_disk_pressure(nil, shard_index), do: DiskPressure.set(shard_index)

  defp set_disk_pressure(instance_ctx, shard_index),
    do: DiskPressure.set(instance_ctx, shard_index)

  defp clear_disk_pressure(nil, shard_index), do: DiskPressure.clear(shard_index)

  defp clear_disk_pressure(instance_ctx, shard_index),
    do: DiskPressure.clear(instance_ctx, shard_index)

  defp bump_write_version(nil, shard_index), do: WriteVersion.increment(shard_index)

  defp bump_write_version(instance_ctx, shard_index),
    do: WriteVersion.increment(instance_ctx, shard_index)

  defp mark_flush_failure(%{flow_history_projector_flush_failures: ref}, shard_index)
       when is_reference(ref) do
    size = :atomics.info(ref).size
    if shard_index < size, do: :atomics.add(ref, shard_index + 1, 1)
    :ok
  rescue
    _ -> :ok
  end

  defp mark_flush_failure(_instance_ctx, _shard_index), do: :ok

  def track_keydir_binary_delta(instance_ctx, keydir, shard_index, key, new_value),
    do: Keydir.track_keydir_binary_delta(instance_ctx, keydir, shard_index, key, new_value)

  def track_keydir_binary_remove_row(instance_ctx, shard_index, row),
    do: Keydir.track_keydir_binary_remove_row(instance_ctx, shard_index, row)

  defp delete_keydir_row(instance_ctx, keydir, shard_index, key),
    do: Keydir.delete_keydir_row(instance_ctx, keydir, shard_index, key)

  def delete_apply_projection_cache_for_row(instance_ctx, shard_index, row),
    do: Keydir.delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)

  def keydir_row_binary_bytes(row), do: Keydir.keydir_row_binary_bytes(row)
  def binary_bytes(value), do: Keydir.binary_bytes(value)
  def safe_ets_lookup(table, key), do: Keydir.safe_ets_lookup(table, key)
  def safe_ets_insert(table, row), do: Keydir.safe_ets_insert(table, row)
  def safe_ets_delete(table, key), do: Keydir.safe_ets_delete(table, key)

  defp stamp_ra_index(entries, nil), do: entries

  defp stamp_ra_index(entries, ra_index) when is_integer(ra_index) do
    Enum.map(entries, &Map.put_new(&1, :ra_index, ra_index))
  end

  defp schedule_flush(%{flush_timer: nil} = state) do
    %{state | flush_timer: Process.send_after(self(), :flush_timer, state.flush_interval_ms)}
  end

  defp schedule_flush(state), do: state

  defp schedule_flush_soon(state) do
    state = cancel_flush(state)
    %{state | flush_timer: Process.send_after(self(), :flush_timer, 0)}
  end

  defp maybe_schedule_next_chunk(%{pending_count: count} = state) when count > 0 do
    state = cancel_flush(state)
    %{state | flush_timer: Process.send_after(self(), :flush_timer, state.chunk_interval_ms)}
  end

  defp maybe_schedule_next_chunk(state), do: state

  defp schedule_retry(state) do
    state = cancel_flush(state)
    delay = Map.get(state, :retry_interval_ms, @retry_interval_ms)
    max_delay = Map.get(state, :retry_max_interval_ms, delay)

    %{
      state
      | flush_timer: Process.send_after(self(), :flush_timer, delay),
        retry_interval_ms: min(delay * 2, max_delay)
    }
  end

  defp reset_retry_backoff(state), do: %{state | retry_interval_ms: @retry_interval_ms}

  defp cancel_flush(%{flush_timer: nil} = state), do: state

  defp cancel_flush(%{flush_timer: timer} = state) do
    Process.cancel_timer(timer)

    receive do
      :flush_timer -> :ok
    after
      0 -> :ok
    end

    %{state | flush_timer: nil}
  end
end
