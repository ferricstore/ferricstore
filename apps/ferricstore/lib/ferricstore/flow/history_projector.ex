defmodule Ferricstore.Flow.HistoryProjector do
  @moduledoc false

  use GenServer

  require Logger

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.HistoryProjector.Config
  alias Ferricstore.Flow.HistoryProjectedIndex
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Store.{BlobRef, DiskPressure, LFU, WriteVersion}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @retry_interval_ms 50
  @pending_registry :ferricstore_flow_history_projector_pending_registry
  @replay_reservation_registry :ferricstore_flow_history_projector_replay_reservations

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

        if skip_history_log_recover?(shard_data_path, projected) do
          :ok
        else
          case recover_history_log(instance_ctx, shard_index, shard_data_path, keydir_override) do
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

    case Process.whereis(projector) do
      nil ->
        {:error, :not_started}

      pid when is_pid(pid) ->
        entries = stamp_ra_index(entries, ra_index)

        case reserve_pending(projector, length(entries)) do
          :ok ->
            reserve_replay_range(projector, entries)
            # This is used from Raft apply. It must never wait behind cold history
            # projection or LMDB work; release-cursor gating handles durability.
            GenServer.cast(pid, {:enqueue_reserved, entries})
            :ok

          {:error, :queue_full, pending_entries, max_pending_entries} ->
            mark_queue_full(instance_ctx, shard_index)

            emit_queue_full(
              instance_ctx,
              shard_index,
              pending_entries,
              length(entries),
              max_pending_entries
            )

            {:error, :queue_full}
        end
    end
  catch
    :exit, _reason -> {:error, :not_started}
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
    history_cap_requirements(entries, load_cap_fun)
  end

  @doc false
  def __direct_hot_history_evict_items_for_test__(entries) when is_list(entries) do
    direct_hot_history_evict_items(entries)
  end

  @doc false
  def __history_hot_rank_entries_for_test__(entries) when is_list(entries) do
    history_hot_rank_entries(entries)
  end

  @doc false
  def __skip_history_log_recover_for_test__(shard_data_path, projected) do
    skip_history_log_recover?(shard_data_path, projected)
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
        prepare_recovered_history_projector(instance_ctx, shard_index, shard_data_path)
      end

    max_pending_entries =
      Ferricstore.MemoryBudget.limit(
        :flow_history_projector_max_pending_entries,
        Config.default_max_pending_entries()
      )

    pending_counter = :atomics.new(1, signed: true)
    register_pending_counter(projector_name, pending_counter, max_pending_entries)
    projected_index = projected_index(instance_ctx, shard_index, shard_data_path)
    flushed_index = max(projected_index, replay_reservation_flushed_index(projector_name))

    {:ok,
     Config.initial_state(
       projector_name,
       shard_index,
       shard_data_path,
       instance_ctx,
       pending_counter,
       max_pending_entries,
       flushed_index
     )}
  end

  @impl true
  def handle_cast({:enqueue_reserved, entries}, state) do
    {:noreply, enqueue_entries(state, entries)}
  end

  def handle_cast({:enqueue, entries}, state) do
    case reserve_pending(state, length(entries)) do
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
    state = flush_pending(state)

    result =
      if state.pending_count == 0 and state.requested_index == nil,
        do: :ok,
        else: {:error, :flush_failed}

    {:reply, result, state}
  end

  def handle_call(:discard, _from, state) do
    state = cancel_flush(state)
    :ok = release_pending(state, state.pending_count)

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
  end

  def handle_call(:pending_count, _from, state) do
    {:reply, {:ok, state.pending_count}, state}
  end

  def handle_call({:enqueue, entries}, _from, state) do
    case reserve_pending(state, length(entries)) do
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
    unregister_pending_counter(Map.get(state, :projector_name))
    :ok
  end

  @impl true
  def handle_info(:flush_timer, state) do
    {:noreply, flush_pending(%{state | flush_timer: nil}, :chunk)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

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

  defp reserve_pending(%{pending_counter: counter, max_pending_entries: max_pending}, count),
    do: reserve_pending_counter(counter, count, max_pending)

  defp reserve_pending(projector, count) when is_atom(projector) do
    case lookup_pending_counter(projector) do
      {:ok, counter, max_pending} -> reserve_pending_counter(counter, count, max_pending)
      :error -> :ok
    end
  end

  defp reserve_pending_counter(_counter, count, _max_pending) when count <= 0, do: :ok

  defp reserve_pending_counter(counter, count, :infinity) do
    :atomics.add(counter, 1, count)
    :ok
  end

  defp reserve_pending_counter(counter, count, max_pending)
       when is_integer(max_pending) and max_pending >= 0 do
    pending_entries = :atomics.add_get(counter, 1, count)

    if pending_entries <= max_pending do
      :ok
    else
      _ = :atomics.add_get(counter, 1, -count)
      {:error, :queue_full, max(pending_entries - count, 0), max_pending}
    end
  end

  defp release_pending(_state, count) when count <= 0, do: :ok

  defp release_pending(%{pending_counter: counter}, count) do
    pending_entries = :atomics.add_get(counter, 1, -count)

    if pending_entries < 0 do
      :atomics.put(counter, 1, 0)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp register_pending_counter(projector, counter, max_pending) when is_atom(projector) do
    @pending_registry
    |> ensure_pending_registry()
    |> :ets.insert({projector, counter, max_pending})

    :ok
  end

  defp unregister_pending_counter(nil), do: :ok

  defp unregister_pending_counter(projector) when is_atom(projector) do
    case :ets.whereis(@pending_registry) do
      :undefined -> :ok
      table -> :ets.delete(table, projector)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp lookup_pending_counter(projector) when is_atom(projector) do
    table = ensure_pending_registry(@pending_registry)

    case :ets.lookup(table, projector) do
      [{^projector, counter, max_pending}] -> {:ok, counter, max_pending}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp ensure_pending_registry(name) do
    ensure_registry(
      name,
      [:named_table, :public, :set, read_concurrency: true],
      :pending_registry_unavailable
    )
  end

  defp ensure_replay_reservation_registry do
    ensure_registry(
      @replay_reservation_registry,
      [
        :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ],
      :replay_reservation_registry_unavailable
    )
  end

  defp ensure_registry(name, opts, unavailable_reason) do
    case :ets.whereis(name) do
      :undefined ->
        ensure_registry_slow(name, opts, unavailable_reason)

      table ->
        table
    end
  end

  defp ensure_registry_slow(name, opts, unavailable_reason) do
    Ferricstore.Flow.HistoryProjector.TableOwner.ensure_tables()

    case :ets.whereis(name) do
      :undefined ->
        try do
          :ets.new(name, opts)
        rescue
          ArgumentError ->
            case :ets.whereis(name) do
              :undefined -> :erlang.error(unavailable_reason)
              table -> table
            end
        end

      table ->
        table
    end
  end

  defp reserve_replay_range(projector, entries) do
    case entry_index_range(entries) do
      nil ->
        :ok

      {min_index, max_index} ->
        table = ensure_replay_reservation_registry()

        case :ets.lookup(table, projector) do
          [{^projector, old_min, old_max, flushed_index}] ->
            :ets.insert(
              table,
              {projector, min(old_min, min_index), max(old_max, max_index), flushed_index}
            )

          _missing ->
            :ets.insert(table, {projector, min_index, max_index, 0})
        end

        :ok
    end
  rescue
    _ -> :ok
  end

  defp mark_replay_range_flushed(_projector, nil), do: :ok

  defp mark_replay_range_flushed(projector, index) when is_integer(index) and index >= 0 do
    table = ensure_replay_reservation_registry()

    case :ets.lookup(table, projector) do
      [{^projector, min_index, max_index, flushed_index}] ->
        :ets.insert(table, {projector, min_index, max_index, max(flushed_index, index)})

      _missing ->
        :ok
    end

    :ok
  rescue
    _ -> :ok
  end

  defp trim_replay_reservation(_projector, nil), do: :ok

  defp trim_replay_reservation(projector, index) when is_integer(index) and index >= 0 do
    table = ensure_replay_reservation_registry()

    case :ets.lookup(table, projector) do
      [{^projector, _min_index, max_index, _flushed_index}] when index >= max_index ->
        :ets.delete(table, projector)

      [{^projector, min_index, max_index, flushed_index}] when index >= min_index ->
        :ets.insert(table, {projector, index + 1, max_index, flushed_index})

      _other ->
        :ok
    end

    :ok
  rescue
    _ -> :ok
  end

  defp replay_reservation_flushed_index(projector) do
    table = ensure_replay_reservation_registry()

    case :ets.lookup(table, projector) do
      [{^projector, _min_index, _max_index, flushed_index}] when is_integer(flushed_index) ->
        flushed_index

      _missing ->
        0
    end
  rescue
    _ -> 0
  end

  defp entry_index_range(entries) do
    Enum.reduce(entries, nil, fn entry, acc ->
      case Map.get(entry, :ra_index) do
        index when is_integer(index) and index >= 0 ->
          case acc do
            nil -> {index, index}
            {min_index, max_index} -> {min(min_index, index), max(max_index, index)}
          end

        _other ->
          acc
      end
    end)
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
        release_pending(state, length(chunk))
        flushed_index = max_index(state.flushed_index, max_projected_index(chunk, nil))
        mark_replay_range_flushed(state.projector_name, flushed_index)

        first_pending_at = if rest == [], do: nil, else: state.first_pending_at

        next_state =
          state
          |> Map.merge(%{
          pending: Enum.reverse(rest),
          pending_count: length(rest),
          first_pending_at: first_pending_at,
          flushed_index: flushed_index
          })
          |> publish_backlog_state()

        maybe_schedule_next_chunk(next_state)

      {:error, reason} ->
        mark_flush_failure(state.instance_ctx, state.shard_index)

        Logger.error(
          "Flow history projector shard_#{state.shard_index}: flush failed: #{inspect(reason)}"
        )

        retry = Process.send_after(self(), :flush_timer, @retry_interval_ms)
        %{state | flush_timer: retry}
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
          trim_replay_reservation(state.projector_name, requested_index)

          %{state | requested_index: nil, flushed_index: max(state.flushed_index, requested_index)}
          |> publish_backlog_state()

        {:error, reason} ->
          mark_flush_failure(state.instance_ctx, state.shard_index)

          Logger.error(
            "Flow history projector shard_#{state.shard_index}: projected index publish failed: #{inspect(reason)}"
          )

          state
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
        release_pending(state, length(entries))
        mark_replay_range_flushed(state.projector_name, projected_index)
        trim_replay_reservation(state.projector_name, projected_index)

        %{
          state
          | pending: [],
            pending_count: 0,
            first_pending_at: nil,
            requested_index: nil,
            flushed_index: max_index(state.flushed_index, projected_index)
        }
        |> publish_backlog_state()

      {:error, reason} ->
        mark_flush_failure(state.instance_ctx, state.shard_index)

        Logger.error(
          "Flow history projector shard_#{state.shard_index}: flush failed: #{inspect(reason)}"
        )

        retry = Process.send_after(self(), :flush_timer, @retry_interval_ms)
        %{state | flush_timer: retry}
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
        with {:ok, _file_id, file_path} <- history_active_file(shard_data_path),
             :ok <- NIF.v2_fsync(file_path) do
          publish_projected_index(instance_ctx, shard_index, shard_data_path, index)
        end

      _ ->
        :ok
    end
  end

  defp do_project(
         instance_ctx,
         shard_index,
         shard_data_path,
         entries,
         requested_index,
         keydir_override
       ) do
    with {:ok, file_id, file_path} <- history_active_file(shard_data_path),
         keydir <- keydir_override || keydir(instance_ctx, shard_index),
         encoded_entries <- Enum.map(entries, &encode_entry/1),
         batch <- Enum.map(encoded_entries, &{&1.key, &1.value, &1.expire_at_ms}),
         {:ok, locations} <- append_batch(file_path, batch),
         :ok <- validate_locations(encoded_entries, locations),
         :ok <- sync_history_log_before_publish(file_path) do
      clear_disk_pressure(instance_ctx, shard_index)

      with :ok <-
             publish_lmdb_history_locations(shard_data_path, file_id, encoded_entries, locations),
           :ok <-
             publish_keydir_entries(
               instance_ctx,
               shard_index,
               keydir,
               file_id,
               encoded_entries,
               locations
             ),
           :ok <- publish_history_index(instance_ctx, shard_index, encoded_entries),
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

        maybe_persist_projected_index(
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
  end

  @spec history_dir(binary()) :: binary()
  def history_dir(shard_data_path), do: Path.join(shard_data_path, "history")

  @spec history_file_path(binary(), non_neg_integer()) :: binary()
  def history_file_path(shard_data_path, file_id) do
    Path.join(
      history_dir(shard_data_path),
      "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log"
    )
  end

  @spec read_value(binary(), {:flow_history, non_neg_integer()}, non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read_value(shard_data_path, {:flow_history, file_id}, offset) do
    shard_data_path
    |> history_file_path(file_id)
    |> NIF.v2_pread_at(offset)
  end

  def read_value(_shard_data_path, _file_id, _offset), do: {:error, :not_flow_history}

  @spec scan_event_value(binary(), binary()) :: {:ok, binary()} | :miss | {:error, term()}
  def scan_event_value(shard_data_path, target_key) do
    file_path = history_file_path(shard_data_path, 0)

    with {:ok, records} <- NIF.v2_scan_file(file_path) do
      case latest_scanned_event_location(records, target_key) do
        {:live, offset} ->
          NIF.v2_pread_at(file_path, offset)

        :miss ->
          :miss
      end
    end
  end

  defp latest_scanned_event_location(records, target_key) do
    Enum.reduce(records, :miss, fn
      {^target_key, _offset, _value_size, _expire_at_ms, true}, _acc ->
        :miss

      {^target_key, offset, _value_size, _expire_at_ms, false}, _acc ->
        {:live, offset}

      _record, acc ->
        acc
    end)
  end

  defp history_active_file(shard_data_path) do
    :ok = ensure_history_file(shard_data_path)
    {:ok, 0, history_file_path(shard_data_path, 0)}
  rescue
    error -> {:error, {:history_file_unavailable, error}}
  end

  defp ensure_history_file(shard_data_path) do
    dir = history_dir(shard_data_path)
    path = history_file_path(shard_data_path, 0)

    with :ok <- Ferricstore.FS.mkdir_p(dir),
         :ok <- touch_if_missing(path) do
      :ok
    end
  end

  defp touch_if_missing(path) do
    if Ferricstore.FS.exists?(path), do: :ok, else: Ferricstore.FS.touch(path)
  end

  defp append_batch(file_path, batch), do: NIF.v2_append_batch_nosync(file_path, batch)

  defp sync_history_log_before_publish(file_path) do
    with :ok <- NIF.v2_fsync(file_path),
         :ok <-
           Ferricstore.FaultInjection.maybe_pause(:after_flow_history_fsync, %{
             file_path: file_path
           }) do
      maybe_history_projector_fsync_hook(file_path)
    end
  end

  defp maybe_history_projector_fsync_hook(file_path) do
    case Application.get_env(:ferricstore, :flow_history_projector_fsync_hook) do
      fun when is_function(fun, 1) -> fun.(file_path)
      _other -> :ok
    end
  end

  defp maybe_persist_projected_index(
         _instance_ctx,
         _shard_index,
         _shard_data_path,
         _file_path,
         _index,
         nil
       ),
       do: :ok

  defp maybe_persist_projected_index(
         instance_ctx,
         shard_index,
         shard_data_path,
         _file_path,
         index,
         requested_index
       )
       when is_integer(index) and is_integer(requested_index) do
    publish_projected_index(instance_ctx, shard_index, shard_data_path, index)
  end

  defp encode_entry(%{value: value} = entry) when is_binary(value), do: entry

  defp encode_entry(%{snapshot: snapshot} = entry) do
    Map.put(entry, :value, flow_call(:encode_history_snapshot, [snapshot]))
  end

  defp encode_entry(%{record: record, event: event, now_ms: now_ms} = entry) do
    value = flow_call(:encode_history_fields, [record, event, now_ms, Map.get(entry, :meta, %{})])
    Map.put(entry, :value, value)
  end

  defp flow_call(function, args) do
    apply(Ferricstore.Flow, function, args)
  end

  defp validate_locations(entries, locations) when length(entries) == length(locations), do: :ok

  defp validate_locations(entries, locations),
    do: {:error, {:location_count_mismatch, length(entries), length(locations)}}

  defp publish_keydir_entries(instance_ctx, shard_index, keydir, file_id, entries, locations) do
    initial_lfu = LFU.initial()

    entries
    |> Enum.zip(locations)
    |> Enum.each(fn {entry, {offset, value_size}} ->
      case safe_ets_lookup(keydir, entry.key) do
        [row] -> delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)
        _missing -> :ok
      end

      track_keydir_binary_delta(instance_ctx, keydir, shard_index, entry.key, nil)

      safe_ets_insert(
        keydir,
        {entry.key, nil, entry.expire_at_ms, initial_lfu, {:flow_history, file_id}, offset,
         value_size}
      )
    end)
  end

  defp publish_history_index(instance_ctx, shard_index, entries) do
    {flow_index, flow_lookup} =
      NativeFlowIndex.table_names(instance_name(instance_ctx), shard_index)

    native = NativeFlowIndex.get(flow_index, flow_lookup)
    {new_entries, update_entries} = history_index_entries(entries)

    if native && new_entries != [], do: NativeFlowIndex.put_new_entries(native, new_entries)
    if native && update_entries != [], do: NativeFlowIndex.put_entries(native, update_entries)

    :ok
  end

  @doc false
  def __history_index_entries_for_test__(entries), do: history_index_entries(entries)

  defp history_index_entries(entries) do
    {new_entries, update_entries} =
      Enum.reduce(entries, {[], []}, fn entry, {new_acc, update_acc} ->
        index_entry = {entry.history_key, entry.event_id, entry.event_ms}

        if entry.version == 1 do
          {[index_entry | new_acc], update_acc}
        else
          {new_acc, [index_entry | update_acc]}
        end
      end)

    {Enum.reverse(new_entries), Enum.reverse(update_entries)}
  end

  defp publish_lmdb_history_locations(shard_data_path, file_id, entries, locations) do
    with :ok <- maybe_history_projector_lmdb_publish_hook(shard_data_path, file_id, entries) do
      write_lmdb_ops(shard_data_path, lmdb_history_location_ops(file_id, entries, locations))
    end
  end

  defp maybe_history_projector_lmdb_publish_hook(shard_data_path, file_id, entries) do
    case Application.get_env(:ferricstore, :flow_history_projector_lmdb_publish_hook) do
      fun when is_function(fun, 3) -> fun.(shard_data_path, file_id, entries)
      _other -> :ok
    end
  end

  defp lmdb_history_location_ops(file_id, entries, locations) do
    entries
    |> Enum.zip(locations)
    |> Enum.flat_map(fn {entry, {offset, value_size}} ->
      history_index_key =
        Ferricstore.Flow.LMDB.history_index_key(
          entry.history_key,
          entry.event_id,
          entry.event_ms
        )

      [
        {:put, history_index_key,
         Ferricstore.Flow.LMDB.encode_history_index_value(
           entry.event_id,
           entry.event_ms,
           entry.key,
           entry.expire_at_ms,
           {:flow_history, file_id},
           offset,
           value_size
         )}
      ]
      |> maybe_history_expire_put(entry.expire_at_ms, history_index_key)
      |> Enum.reverse()
    end)
    |> maybe_history_flow_expire_puts(entries)
  end

  defp write_lmdb_ops(_shard_data_path, []), do: :ok

  defp write_lmdb_ops(shard_data_path, ops) do
    shard_data_path
    |> Ferricstore.Flow.LMDB.path()
    |> Ferricstore.Flow.LMDB.write_batch(ops)
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

  defp maybe_history_flow_expire_puts(ops, entries) do
    entries
    |> Enum.reduce(%{}, fn entry, acc ->
      if is_integer(entry.expire_at_ms) and entry.expire_at_ms > 0 do
        Map.put(acc, entry.history_key, entry.expire_at_ms)
      else
        acc
      end
    end)
    |> Enum.reduce(ops, fn {history_key, expire_at_ms}, acc ->
      case Ferricstore.Flow.LMDB.history_flow_expire_key(expire_at_ms, history_key) do
        nil ->
          acc

        expire_key ->
          [
            {:put, expire_key,
             Ferricstore.Flow.LMDB.encode_history_flow_expire_value(history_key, expire_at_ms)}
            | acc
          ]
      end
    end)
  end

  defp compact_projected_flow_values(
         instance_ctx,
         shard_index,
         shard_data_path,
         keydir,
         file_path,
         file_id,
         entries
       ) do
    refs = projected_flow_value_refs(entries)

    keydir_items = projected_flow_value_keydir_items(keydir, refs)

    {direct_segment_items, lmdb_checked_items} =
      Enum.split_with(keydir_items, fn {_ref, file_id} ->
        direct_segment_value_file_id?(file_id)
      end)

    direct_segment_refs = Enum.map(direct_segment_items, &elem(&1, 0))
    lmdb_checked_refs = Enum.map(lmdb_checked_items, &elem(&1, 0))

    # Do not flush the async LMDB writer here. The projector writes value
    # locators itself before removing keydir refs, so waiting behind unrelated
    # state/history LMDB work would turn cold projection into an apply-adjacent
    # latency source.
    {projected_refs, pending_refs} =
      split_projected_flow_value_refs(shard_data_path, lmdb_checked_refs)

    delete_projected_flow_value_keydir_refs(instance_ctx, shard_index, keydir, projected_refs)

    case collect_projected_flow_values(
           instance_ctx,
           shard_index,
           shard_data_path,
           keydir,
           direct_segment_refs ++ pending_refs
         ) do
      [] ->
        :ok

      value_entries ->
        {direct_entries, remaining_entries} =
          Enum.split_with(value_entries, &direct_segment_value_entry?/1)

        {direct_value_entries, copied_entries} =
          Enum.split_with(remaining_entries, &direct_lmdb_value_entry?/1)

        with :ok <- publish_lmdb_direct_value_locations(shard_data_path, direct_entries),
             :ok <-
               delete_projected_flow_value_keydir_rows(
                 instance_ctx,
                 shard_index,
                 keydir,
                 direct_entries,
                 delete_apply_projection_cache?: false
               ),
             :ok <- publish_lmdb_direct_values(shard_data_path, direct_value_entries),
             :ok <-
               delete_projected_flow_value_keydir_rows(
                 instance_ctx,
                 shard_index,
                 keydir,
                 direct_value_entries
               ),
             :ok <-
               copy_projected_flow_values(
                 instance_ctx,
                 shard_index,
                 shard_data_path,
                 keydir,
                 file_path,
                 file_id,
                 copied_entries
               ) do
          :ok
        end
    end
  end

  @doc false
  def __projected_flow_value_keydir_refs_for_test__(keydir, refs) do
    projected_flow_value_keydir_refs(keydir, refs)
  end

  defp projected_flow_value_keydir_refs(keydir, refs) do
    keydir
    |> projected_flow_value_keydir_items(refs)
    |> Enum.map(&elem(&1, 0))
  end

  defp projected_flow_value_keydir_items(keydir, refs) do
    refs
    |> Enum.reduce([], fn ref, acc ->
      case safe_ets_lookup(keydir, ref) do
        [{^ref, _value, _expire_at_ms, _lfu, file_id, offset, value_size}] ->
          if readable_value_locator?(file_id, offset, value_size),
            do: [{ref, file_id} | acc],
            else: acc

        _missing_or_unreadable ->
          acc
      end
    end)
    |> Enum.reverse()
  end

  defp direct_segment_value_file_id?({:waraft_segment, index})
       when is_integer(index) and index > 0,
       do: true

  defp direct_segment_value_file_id?(_file_id), do: false

  defp split_projected_flow_value_refs(shard_data_path, refs) do
    refs = Enum.to_list(refs)

    case refs do
      [] ->
        {[], []}

      [_ | _] ->
        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        case Ferricstore.Flow.LMDB.get_many(path, refs) do
          {:ok, results} when length(results) == length(refs) ->
            now_ms = System.system_time(:millisecond)

            Enum.zip(refs, results)
            |> Enum.reduce({[], []}, fn
              {ref, result}, {projected, pending} ->
                if projected_flow_value_lmdb_live?(result, now_ms) do
                  {[ref | projected], pending}
                else
                  {projected, [ref | pending]}
                end
            end)
            |> then(fn {projected, pending} ->
              {Enum.reverse(projected), Enum.reverse(pending)}
            end)

          _other ->
            {[], refs}
        end
    end
  end

  defp projected_flow_value_lmdb_live?({:ok, blob}, now_ms) when is_binary(blob) do
    case Ferricstore.Flow.LMDB.decode_value_locator(blob, now_ms) do
      {:ok, {{:waraft_apply_projection, _index}, _offset, _value_size}} ->
        false

      {:ok, _locator} ->
        true

      :not_locator ->
        match?({:ok, _value}, Ferricstore.Flow.LMDB.decode_value(blob, now_ms))

      _expired_or_error ->
        false
    end
  end

  defp projected_flow_value_lmdb_live?(_result, _now_ms), do: false

  defp collect_projected_flow_values(instance_ctx, shard_index, shard_data_path, keydir, refs) do
    refs
    |> Enum.map(
      &projected_flow_value_source(instance_ctx, shard_index, shard_data_path, keydir, &1)
    )
    |> materialize_projected_flow_value_sources(instance_ctx, shard_index)
  end

  defp projected_flow_value_source(instance_ctx, shard_index, shard_data_path, keydir, key) do
    case safe_ets_lookup(keydir, key) do
      [{^key, value, expire_at_ms, lfu, file_id, offset, value_size} = row] ->
        if readable_value_locator?(file_id, offset, value_size) do
          case {value, file_id, value_size} do
            {nil, {tag, _index}, 0} when tag in [:waraft_segment, :waraft_apply_projection] ->
              {:entry,
               projected_flow_value_entry_from_row(
                 key,
                 <<>>,
                 expire_at_ms,
                 lfu,
                 file_id,
                 offset,
                 value_size,
                 row
               )}

            {nil, {:waraft_segment, _index}, _value_size} ->
              {:entry,
               direct_projected_flow_value_entry_from_row(
                 key,
                 expire_at_ms,
                 lfu,
                 file_id,
                 offset,
                 value_size,
                 row
               )}

            _other ->
              case projected_flow_value_bytes(
                     instance_ctx,
                     shard_index,
                     shard_data_path,
                     key,
                     value,
                     file_id,
                     offset
                   ) do
                {:ok, bytes} ->
                  {:entry,
                   projected_flow_value_entry_from_row(
                     key,
                     bytes,
                     expire_at_ms,
                     lfu,
                     file_id,
                     offset,
                     value_size,
                     row
                   )}

                _error ->
                  :skip
              end
          end
        else
          :skip
        end

      _other ->
        :skip
    end
  end

  defp materialize_projected_flow_value_sources(sources, _instance_ctx, _shard_index) do
    sources
    |> Enum.reduce([], fn
      {:entry, entry}, acc ->
        [entry | acc]

      _skip, acc ->
        acc
    end)
    |> Enum.reverse()
  end

  defp projected_flow_value_entry_from_row(
         key,
         bytes,
         expire_at_ms,
         lfu,
         file_id,
         offset,
         value_size,
         row
       ) do
    %{
      key: key,
      value: bytes,
      expire_at_ms: expire_at_ms,
      lfu: lfu,
      source_file_id: file_id,
      source_offset: offset,
      source_value_size: value_size,
      source_row: row
    }
  end

  defp direct_projected_flow_value_entry_from_row(
         key,
         expire_at_ms,
         lfu,
         file_id,
         offset,
         value_size,
         row
       ) do
    %{
      key: key,
      value: nil,
      expire_at_ms: expire_at_ms,
      lfu: lfu,
      source_file_id: file_id,
      source_offset: offset,
      source_value_size: value_size,
      source_row: row
    }
  end

  defp projected_flow_value_bytes(
         _instance_ctx,
         _shard_index,
         _shard_data_path,
         _key,
         value,
         _file_id,
         _offset
       )
       when is_binary(value),
       do: {:ok, value}

  defp projected_flow_value_bytes(
         _instance_ctx,
         _shard_index,
         shard_data_path,
         key,
         nil,
         file_id,
         offset
       )
       when is_integer(file_id) and file_id >= 0 do
    shard_data_path
    |> ShardETS.file_path(file_id)
    |> Ferricstore.Store.ColdRead.pread_keyed(offset, key, 10_000)
  end

  defp projected_flow_value_bytes(
         _instance_ctx,
         _shard_index,
         shard_data_path,
         _key,
         nil,
         {:flow_history, _file_id} = file_id,
         offset
       ) do
    read_value(shard_data_path, file_id, offset)
  end

  defp projected_flow_value_bytes(
         instance_ctx,
         shard_index,
         _shard_data_path,
         key,
         nil,
         file_id,
         _offset
       )
       when is_tuple(file_id) do
    Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
      instance_ctx,
      shard_index,
      file_id,
      key
    )
  end

  defp projected_flow_value_bytes(
         _instance_ctx,
         _shard_index,
         _shard_data_path,
         _key,
         _value,
         _file_id,
         _offset
       ),
       do: :error

  defp validate_projected_value_locations(entries, locations)
       when length(entries) == length(locations) do
    if Enum.all?(locations, &valid_projected_value_location?/1) do
      :ok
    else
      {:error, {:flow_value_projection_location_mismatch, length(entries), locations}}
    end
  end

  defp validate_projected_value_locations(entries, locations),
    do: {:error, {:flow_value_projection_location_mismatch, length(entries), locations}}

  defp valid_projected_value_location?({offset, value_size})
       when is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0,
       do: true

  defp valid_projected_value_location?(_location), do: false

  defp direct_segment_value_entry?(%{
         source_file_id: {tag, index},
         source_offset: offset,
         source_value_size: value_size
       })
       when tag == :waraft_segment and is_integer(index) and index > 0 and
              is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size > 0,
       do: true

  defp direct_segment_value_entry?(_entry), do: false

  defp direct_lmdb_value_entry?(%{value: value, source_value_size: 0}) when is_binary(value),
    do: true

  defp direct_lmdb_value_entry?(%{value: value}) when is_binary(value), do: BlobRef.ref?(value)
  defp direct_lmdb_value_entry?(_entry), do: false

  defp publish_lmdb_direct_value_locations(_shard_data_path, []), do: :ok

  defp publish_lmdb_direct_value_locations(shard_data_path, entries) do
    write_lmdb_ops(
      shard_data_path,
      Ferricstore.Flow.LMDB.segment_value_pin_batch_put_ops(entries)
    )
  end

  defp publish_lmdb_direct_values(_shard_data_path, []), do: :ok

  defp publish_lmdb_direct_values(shard_data_path, entries) do
    write_lmdb_ops(shard_data_path, direct_lmdb_value_put_ops(entries))
  end

  defp direct_lmdb_value_put_ops(entries) do
    Enum.map(entries, fn %{key: key, value: value, expire_at_ms: expire_at_ms} ->
      {:put, key, Ferricstore.Flow.LMDB.encode_value(value, expire_at_ms)}
    end)
  end

  defp copy_projected_flow_values(
         _instance_ctx,
         _shard_index,
         _shard_data_path,
         _keydir,
         _file_path,
         _file_id,
         []
       ),
       do: :ok

  defp copy_projected_flow_values(
         instance_ctx,
         shard_index,
         shard_data_path,
         keydir,
         file_path,
         file_id,
         value_entries
       ) do
    batch =
      Enum.map(value_entries, fn %{key: key, value: value, expire_at_ms: expire_at_ms} ->
        {key, value, expire_at_ms}
      end)

    with {:ok, locations} <- append_batch(file_path, batch),
         :ok <- validate_projected_value_locations(value_entries, locations),
         :ok <- sync_history_log_before_publish(file_path),
         :ok <-
           publish_lmdb_value_locations(
             shard_data_path,
             file_id,
             value_entries,
             locations
           ) do
      delete_projected_flow_value_keydir_rows(
        instance_ctx,
        shard_index,
        keydir,
        value_entries
      )
    end
  end

  defp publish_lmdb_value_locations(shard_data_path, file_id, entries, locations) do
    write_lmdb_ops(shard_data_path, lmdb_value_location_ops(file_id, entries, locations))
  end

  defp lmdb_value_location_ops(file_id, entries, locations) do
    entries
    |> Enum.zip(locations)
    |> Enum.map(fn {%{key: key, expire_at_ms: expire_at_ms}, {offset, value_size}} ->
      {:put, key,
       Ferricstore.Flow.LMDB.encode_value_locator(
         expire_at_ms,
         {:flow_history, file_id},
         offset,
         value_size
       )}
    end)
  end

  defp delete_projected_flow_value_keydir_rows(instance_ctx, shard_index, keydir, entries) do
    delete_projected_flow_value_keydir_rows(instance_ctx, shard_index, keydir, entries,
      delete_apply_projection_cache?: true
    )
  end

  defp delete_projected_flow_value_keydir_rows(instance_ctx, shard_index, keydir, entries, opts) do
    delete_apply_projection_cache? = Keyword.get(opts, :delete_apply_projection_cache?, true)

    {count, bytes} =
      Enum.reduce(entries, {0, 0}, fn %{key: key, value: value, source_row: row},
                                      {count_acc, bytes_acc} ->
        case safe_ets_lookup(keydir, key) do
          [^row] ->
            bytes = binary_bytes(value)
            track_keydir_binary_remove_row(instance_ctx, shard_index, row)

            if delete_apply_projection_cache? do
              delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)
            end

            safe_ets_delete(keydir, key)
            {count_acc + 1, bytes_acc + bytes}

          _changed ->
            {count_acc, bytes_acc}
        end
      end)

    emit_value_dematerialize(instance_ctx, shard_index, count, bytes)
    :ok
  end

  defp delete_projected_flow_value_keydir_refs(_instance_ctx, _shard_index, _keydir, []),
    do: :ok

  defp delete_projected_flow_value_keydir_refs(instance_ctx, shard_index, keydir, refs) do
    {count, bytes} =
      Enum.reduce(refs, {0, 0}, fn key, {count_acc, bytes_acc} ->
        case safe_ets_lookup(keydir, key) do
          [{^key, value, _expire_at_ms, _lfu, _file_id, _offset, _value_size} = row] ->
            bytes = binary_bytes(value)
            track_keydir_binary_remove_row(instance_ctx, shard_index, row)
            delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)
            safe_ets_delete(keydir, key)
            {count_acc + 1, bytes_acc + bytes}

          _missing_or_changed ->
            {count_acc, bytes_acc}
        end
      end)

    emit_value_dematerialize(instance_ctx, shard_index, count, bytes)
    :ok
  end

  defp projected_flow_value_refs(entries) do
    entries
    |> Enum.reduce(MapSet.new(), fn entry, acc ->
      entry
      |> entry_flow_value_refs()
      |> Enum.reduce(acc, fn ref, refs ->
        if generated_flow_value_ref?(ref), do: MapSet.put(refs, ref), else: refs
      end)
    end)
  end

  @doc false
  def __projected_flow_value_refs_for_test__(entries), do: projected_flow_value_refs(entries)

  defp entry_flow_value_refs(%{value_refs: refs}), do: entry_value_refs(refs)

  defp entry_flow_value_refs(%{record: record}) when is_map(record),
    do: record_flow_value_refs(record)

  defp entry_flow_value_refs(%{snapshot: snapshot}) do
    :encode_history_snapshot
    |> flow_call([snapshot])
    |> history_value_refs_from_encoded()
  rescue
    _ -> []
  end

  defp entry_flow_value_refs(%{value: value}) when is_binary(value),
    do: history_value_refs_from_encoded(value)

  defp entry_flow_value_refs(_entry), do: []

  defp entry_value_refs(refs) when is_list(refs) do
    Enum.filter(refs, &(is_binary(&1) and &1 != ""))
  end

  defp entry_value_refs(%{} = refs), do: named_flow_value_refs(refs)

  defp entry_value_refs(refs) when is_binary(refs), do: named_flow_value_refs(refs)

  defp entry_value_refs(_refs), do: []

  defp record_flow_value_refs(record) when is_map(record) do
    [
      Map.get(record, :payload_ref),
      Map.get(record, :result_ref),
      Map.get(record, :error_ref)
      | named_flow_value_refs(Map.get(record, :value_refs))
    ]
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
  end

  defp history_value_refs_from_encoded(value) when is_binary(value) do
    :decode_history_fields
    |> flow_call([value])
    |> history_fields_to_map()
    |> history_fields_value_refs()
  rescue
    _ -> []
  end

  defp history_fields_to_map(fields) when is_list(fields) do
    fields
    |> Enum.chunk_every(2)
    |> Enum.reduce(%{}, fn
      [key, value], acc when is_binary(key) -> Map.put(acc, key, value)
      _field, acc -> acc
    end)
  end

  defp history_fields_to_map(_fields), do: %{}

  defp history_fields_value_refs(fields) when is_map(fields) do
    [
      Map.get(fields, "payload_ref"),
      Map.get(fields, "result_ref"),
      Map.get(fields, "error_ref")
      | named_flow_value_refs(Map.get(fields, "value_refs"))
    ]
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
  end

  defp named_flow_value_refs(%{} = refs) do
    Enum.flat_map(refs, fn
      {_name, %{ref: ref}} when is_binary(ref) -> [ref]
      {_name, %{"ref" => ref}} when is_binary(ref) -> [ref]
      {_name, ref} when is_binary(ref) -> [ref]
      _entry -> []
    end)
  end

  defp named_flow_value_refs(value) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, refs} -> named_flow_value_refs(refs)
      _error -> []
    end
  end

  defp named_flow_value_refs(_refs), do: []

  defp generated_flow_value_ref?("f:" <> _rest = ref) do
    case :binary.split(ref, ":v:") do
      ["f:" <> tag, <<kind, ?:, rest::binary>>]
      when byte_size(tag) > 0 and kind in [?p, ?r, ?e, ?s] and byte_size(rest) > 0 ->
        true

      _other ->
        false
    end
  end

  defp generated_flow_value_ref?(_ref), do: false

  defp readable_value_locator?(file_id, offset, value_size)
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
              is_integer(value_size) and value_size >= 0,
       do: true

  defp readable_value_locator?({tag, index}, offset, value_size)
       when tag in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
              is_integer(index) and index > 0 and is_integer(offset) and offset >= 0 and
              is_integer(value_size) and value_size >= 0,
       do: true

  defp readable_value_locator?(_file_id, _offset, _value_size), do: false

  defp emit_value_dematerialize(_instance_ctx, _shard_index, 0, _bytes), do: :ok

  defp emit_value_dematerialize(instance_ctx, shard_index, count, bytes) do
    :telemetry.execute(
      [:ferricstore, :flow, :value_dematerialize],
      %{count: count, bytes: bytes},
      %{instance: instance_name(instance_ctx), shard_index: shard_index}
    )
  rescue
    _ -> :ok
  end

  defp trim_history_caps(instance_ctx, shard_index, shard_data_path, keydir, file_path, entries) do
    cap_requirements =
      history_cap_requirements(entries, fn history_key ->
        load_history_max_cap(history_key, keydir, shard_data_path)
      end)

    if map_size(cap_requirements) == 0 do
      :ok
    else
      {flow_index, flow_lookup} =
        NativeFlowIndex.table_names(instance_name(instance_ctx), shard_index)

      native = NativeFlowIndex.get(flow_index, flow_lookup)

      trim_items =
        cap_requirements
        |> Enum.flat_map(fn
          {history_key, {max_events, true}} ->
            history_key
            |> lmdb_history_over_cap_items(shard_data_path, max_events)
            |> Enum.map(fn {event_id, key, history_index_key} ->
              {history_key, event_id, key, history_index_key}
            end)

          {_history_key, {_max_events, false}} ->
            []
        end)

      case trim_items do
        [] ->
          :ok

        [_ | _] ->
          tombstone_keys =
            Enum.map(trim_items, fn {_history_key, _event_id, key, _history_index_key} -> key end)

          with :ok <- append_tombstones(file_path, tombstone_keys),
               :ok <- delete_lmdb_history_entries(shard_data_path, trim_items) do
            if native do
              NativeFlowIndex.delete_entries(native, history_delete_entries(trim_items))
            end

            Enum.each(tombstone_keys, fn key ->
              delete_keydir_row(instance_ctx, keydir, shard_index, key)
            end)

            :ok
          end
      end
    end
  end

  defp history_cap_requirements(entries, load_cap_fun) do
    entries
    |> Enum.reduce(%{}, &put_history_cap_requirement/2)
    |> Enum.reduce(%{}, fn {history_key, state}, acc ->
      cap = state.cap || load_cap_fun.(history_key)

      case cap do
        max_events when is_integer(max_events) and max_events > 0 ->
          Map.put(acc, history_key, {max_events, history_cap_required?(state, max_events)})

        _ ->
          acc
      end
    end)
  end

  defp put_history_cap_requirement(%{history_key: history_key} = entry, acc)
       when is_binary(history_key) do
    state =
      Map.get(acc, history_key, %{
        cap: nil,
        max_version: nil,
        unknown_version?: false
      })

    state =
      entry
      |> history_cap_entry_state()
      |> merge_history_cap_entry_state(state)

    Map.put(acc, history_key, state)
  end

  defp put_history_cap_requirement(_entry, acc), do: acc

  defp history_cap_entry_state(entry) do
    %{
      cap: history_cap_from_entry(entry),
      version: Map.get(entry, :version, :missing)
    }
  end

  defp history_cap_from_entry(%{history_max_events: max_events})
       when is_integer(max_events) and max_events > 0,
       do: max_events

  defp history_cap_from_entry(_entry), do: nil

  defp merge_history_cap_entry_state(%{cap: cap, version: version}, state) do
    state
    |> maybe_put_history_cap(cap)
    |> put_history_cap_version(version)
  end

  defp maybe_put_history_cap(state, cap) when is_integer(cap) and cap > 0,
    do: %{state | cap: cap}

  defp maybe_put_history_cap(state, _cap), do: state

  defp put_history_cap_version(state, version) when is_integer(version) and version >= 0 do
    max_version =
      case state.max_version do
        existing when is_integer(existing) -> max(existing, version)
        _ -> version
      end

    %{state | max_version: max_version}
  end

  defp put_history_cap_version(state, _version), do: %{state | unknown_version?: true}

  defp history_cap_required?(%{unknown_version?: true}, _max_events), do: true

  defp history_cap_required?(%{max_version: version}, max_events)
       when is_integer(version) and version > max_events,
       do: true

  defp history_cap_required?(_state, _max_events), do: false

  defp load_history_max_cap(history_key, keydir, shard_data_path) do
    with {:ok, state_key} <- history_state_key(history_key),
         {:ok, %{history_max_events: max_events}} <-
           load_history_state_record(state_key, keydir, shard_data_path),
         true <- is_integer(max_events) and max_events > 0 do
      max_events
    else
      _ -> nil
    end
  end

  defp lmdb_history_over_cap_items(history_key, shard_data_path, max_events) do
    lmdb_path = Ferricstore.Flow.LMDB.path(shard_data_path)
    prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)

    with {:ok, count} <- Ferricstore.Flow.LMDB.prefix_count(lmdb_path, prefix),
         true <- count > max_events,
         {:ok, entries} <-
           Ferricstore.Flow.LMDB.prefix_entries(lmdb_path, prefix, count - max_events) do
      Enum.flat_map(entries, &decode_lmdb_history_trim_item/1)
    else
      _ -> []
    end
  end

  defp decode_lmdb_history_trim_item({history_index_key, value}) do
    case Ferricstore.Flow.LMDB.decode_history_index_value(value) do
      {:ok, {event_id, _event_ms, _expire_at_ms, compound_key}} ->
        [{event_id, compound_key, history_index_key}]

      _ ->
        []
    end
  end

  defp trim_history_hot_cache(instance_ctx, shard_index, keydir, entries) do
    direct_items = direct_hot_history_evict_items(entries)
    direct_native = history_native_index(instance_ctx, shard_index, direct_items)

    with :ok <-
           evict_hot_history_items(
             direct_items,
             instance_ctx,
             shard_index,
             keydir,
             direct_native
           ) do
      trim_history_hot_cache_by_rank(
        instance_ctx,
        shard_index,
        keydir,
        history_hot_rank_entries(entries)
      )
    end
  end

  defp trim_history_hot_cache_by_rank(_instance_ctx, _shard_index, _keydir, []), do: :ok

  defp trim_history_hot_cache_by_rank(instance_ctx, shard_index, keydir, entries) do
    caps = history_hot_caps(entries)

    if map_size(caps) == 0 do
      :ok
    else
      {flow_index, flow_lookup} =
        NativeFlowIndex.table_names(instance_name(instance_ctx), shard_index)

      native = NativeFlowIndex.get(flow_index, flow_lookup)

      caps
      |> Enum.flat_map(fn {history_key, max_events} ->
        if native do
          count = NativeFlowIndex.count_all(native, history_key)

          if count > max_events do
            native
            |> NativeFlowIndex.rank_range(history_key, 0, count - max_events - 1, false)
            |> Enum.map(fn {event_id, _score} ->
              {history_key, event_id, history_entry_key(history_key, event_id)}
            end)
          else
            []
          end
        else
          []
        end
      end)
      |> evict_hot_history_items(
        instance_ctx,
        shard_index,
        keydir,
        native
      )
    end
  end

  defp history_native_index(_instance_ctx, _shard_index, []), do: nil

  defp history_native_index(instance_ctx, shard_index, _items) do
    {flow_index, flow_lookup} =
      NativeFlowIndex.table_names(instance_name(instance_ctx), shard_index)

    NativeFlowIndex.get(flow_index, flow_lookup)
  end

  defp direct_hot_history_evict_items(entries) do
    entries
    |> Enum.flat_map(fn entry ->
      entry
      |> direct_hot_history_evict_event_ids()
      |> Enum.map(fn event_id ->
        {entry.history_key, event_id, history_entry_key(entry.history_key, event_id)}
      end)
    end)
    |> Enum.uniq()
  end

  defp direct_hot_history_evict_event_ids(%{history_key: history_key, terminal?: true} = entry)
       when is_binary(history_key) do
    entry
    |> Map.get(:hot_evict_event_ids, [])
    |> List.wrap()
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
  end

  defp direct_hot_history_evict_event_ids(
         %{history_key: history_key, history_hot_max_events: 0, event_id: event_id} = entry
       )
       when is_binary(history_key) and is_binary(event_id) and event_id != "" do
    entry
    |> Map.get(:hot_evict_event_ids, [])
    |> List.wrap()
    |> then(&[event_id | &1])
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.uniq()
  end

  defp direct_hot_history_evict_event_ids(%{history_key: history_key} = entry)
       when is_binary(history_key) do
    entry
    |> Map.get(:hot_evict_event_ids, [])
    |> List.wrap()
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
  end

  defp direct_hot_history_evict_event_ids(_entry), do: []

  defp history_hot_rank_entries(entries) do
    Enum.reject(entries, &history_hot_direct_or_under_cap?/1)
  end

  defp history_hot_direct_or_under_cap?(%{terminal?: true}), do: false

  defp history_hot_direct_or_under_cap?(%{history_hot_max_events: 0}), do: true

  defp history_hot_direct_or_under_cap?(%{
         history_hot_max_events: 1,
         version: version,
         hot_evict_event_ids: [_ | _]
       })
       when is_integer(version) and version > 1,
       do: true

  defp history_hot_direct_or_under_cap?(%{history_hot_max_events: max_events, version: version})
       when is_integer(max_events) and max_events > 0 and is_integer(version) and
              version <= max_events,
       do: true

  defp history_hot_direct_or_under_cap?(_entry), do: false

  defp history_hot_caps(entries) do
    Enum.reduce(entries, %{}, fn
      %{history_key: history_key, terminal?: true}, acc when is_binary(history_key) ->
        put_history_hot_cap(acc, history_key, 0)

      %{history_key: history_key, history_hot_max_events: max_events}, acc
      when is_binary(history_key) and is_integer(max_events) and max_events >= 0 ->
        put_history_hot_cap(acc, history_key, max_events)

      _entry, acc ->
        acc
    end)
  end

  defp put_history_hot_cap(caps, history_key, max_events) do
    Map.update(caps, history_key, max_events, &min(&1, max_events))
  end

  defp evict_hot_history_items(
         [],
         _instance_ctx,
         _shard_index,
         _keydir,
         _native
       ),
       do: :ok

  defp evict_hot_history_items(
         items,
         instance_ctx,
         shard_index,
         keydir,
         native
       ) do
    if native do
      NativeFlowIndex.delete_entries(native, history_delete_entries(items))
    end

    Enum.each(items, fn {_history_key, _event_id, key} ->
      delete_keydir_row(instance_ctx, keydir, shard_index, key)
    end)

    :ok
  end

  defp history_delete_entries(items) do
    Enum.map(items, fn
      {history_key, event_id, _key} ->
        {history_key, event_id}

      {history_key, event_id, _key, _history_index_key} ->
        {history_key, event_id}
    end)
  end

  defp append_tombstones(_file_path, []), do: :ok

  defp append_tombstones(file_path, keys) do
    ops = Enum.map(keys, &{:delete, &1})

    with {:ok, locations} <- NIF.v2_append_ops_batch_nosync(file_path, ops),
         :ok <- validate_tombstone_locations(locations, length(keys)),
         :ok <- sync_history_log_before_publish(file_path) do
      :ok
    end
  end

  defp validate_tombstone_locations(locations, expected_count)
       when is_list(locations) and length(locations) == expected_count do
    if Enum.all?(locations, &valid_tombstone_location?/1) do
      :ok
    else
      {:error, {:history_tombstone_batch_result_mismatch, expected_count, locations}}
    end
  end

  defp validate_tombstone_locations(locations, expected_count),
    do: {:error, {:history_tombstone_batch_result_mismatch, expected_count, locations}}

  defp valid_tombstone_location?({:delete, offset, record_size})
       when is_integer(offset) and offset >= 0 and is_integer(record_size) and record_size > 0,
       do: true

  defp valid_tombstone_location?(_location), do: false

  defp delete_lmdb_history_entries(_shard_data_path, []), do: :ok

  defp delete_lmdb_history_entries(shard_data_path, items) do
    path = Ferricstore.Flow.LMDB.path(shard_data_path)

    ops =
      Enum.flat_map(items, fn
        {_history_key, _event_id, _key, history_index_key} when is_binary(history_index_key) ->
          Ferricstore.Flow.LMDB.history_index_delete_ops(path, history_index_key)

        {history_key, event_id, _key} ->
          case parse_event_ms(event_id) do
            {:ok, event_ms} ->
              history_index_key =
                Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, event_ms)

              Ferricstore.Flow.LMDB.history_index_delete_ops(path, history_index_key)

            :error ->
              []
          end
      end)

    Ferricstore.Flow.LMDB.write_batch(path, ops)
  end

  defp recover_history_log(instance_ctx, shard_index, shard_data_path, keydir_override) do
    file_path = history_file_path(shard_data_path, 0)

    case NIF.v2_scan_file(file_path) do
      {:ok, records} ->
        keydir = keydir_override || keydir(instance_ctx, shard_index)
        live_records = live_history_records(records)
        {entries, locations} = recovered_history_entries(live_records, keydir, shard_data_path)

        with :ok <- publish_lmdb_history_locations(shard_data_path, 0, entries, locations),
             :ok <-
               publish_keydir_entries(instance_ctx, shard_index, keydir, 0, entries, locations),
             :ok <- publish_history_index(instance_ctx, shard_index, entries),
             :ok <- trim_history_hot_cache(instance_ctx, shard_index, keydir, entries) do
          :ok
        end

      {:error, reason} ->
        {:error, {:history_scan_failed, reason}}

      other ->
        {:error, {:history_scan_unexpected, other}}
    end
  rescue
    error -> {:error, {:history_recover_exception, error}}
  end

  defp live_history_records(records) do
    Enum.reduce(records, %{}, fn
      {key, _offset, _value_size, _expire_at_ms, true}, acc ->
        Map.delete(acc, key)

      {key, offset, value_size, expire_at_ms, false}, acc ->
        Map.put(acc, key, {offset, value_size, expire_at_ms})
    end)
  end

  defp recovered_history_entries(live_records, keydir, shard_data_path) do
    {entries, locations, _caps} =
      Enum.reduce(live_records, {[], [], %{}}, fn {key, {offset, value_size, expire_at_ms}},
                                                  {entries, locations, caps} ->
        case parse_history_entry_key(key) do
          {:ok, history_key, event_id, event_ms} ->
            {history_hot_max_events, caps} =
              recovered_history_hot_cap(history_key, keydir, shard_data_path, caps)

            version = recovered_history_event_version(event_id)

            entry = %{
              key: key,
              expire_at_ms: expire_at_ms,
              history_key: history_key,
              event_id: event_id,
              event_ms: event_ms,
              version: version,
              history_hot_max_events: history_hot_max_events
            }

            {[entry | entries], [{offset, value_size} | locations], caps}

          :error ->
            {entries, locations, caps}
        end
      end)

    {Enum.reverse(entries), Enum.reverse(locations)}
  end

  defp recovered_history_hot_cap(history_key, keydir, shard_data_path, caps) do
    case Map.fetch(caps, history_key) do
      {:ok, max_events} ->
        {max_events, caps}

      :error ->
        max_events = load_history_hot_cap(history_key, keydir, shard_data_path)
        {max_events, Map.put(caps, history_key, max_events)}
    end
  end

  defp recovered_history_event_version(event_id) do
    case parse_event_version(event_id) do
      {:ok, version} -> version
      :error -> 1
    end
  end

  defp load_history_hot_cap(history_key, keydir, shard_data_path) do
    with {:ok, state_key} <- history_state_key(history_key),
         {:ok, %{history_hot_max_events: max_events}} <-
           load_history_state_record(state_key, keydir, shard_data_path),
         true <- is_integer(max_events) and max_events >= 0 do
      max_events
    else
      _ -> default_history_hot_max_events()
    end
  end

  defp load_history_state_record(state_key, keydir, shard_data_path) do
    case safe_ets_lookup(keydir, state_key) do
      [{^state_key, _value, _expire_at_ms, _lfu, _file_id, _offset, _value_size} = row] ->
        with {:ok, value} <- keydir_row_value(shard_data_path, row) do
          decode_flow_record(value)
        end

      _ ->
        load_lmdb_history_state_record(state_key, shard_data_path)
    end
  end

  defp load_lmdb_history_state_record(state_key, shard_data_path) do
    path = Ferricstore.Flow.LMDB.path(shard_data_path)

    with {:ok, blob} <- Ferricstore.Flow.LMDB.get(path, state_key),
         {:ok, value} <-
           Ferricstore.Flow.LMDB.decode_value(blob, System.system_time(:millisecond)) do
      decode_flow_record(value)
    end
  end

  defp history_state_key(history_key) when is_binary(history_key) do
    case :binary.split(history_key, ":h:") do
      [prefix, id] when byte_size(prefix) > 0 and byte_size(id) > 0 ->
        {:ok, prefix <> ":s:" <> id}

      _ ->
        :error
    end
  end

  defp keydir_row_value(
         _shard_data_path,
         {_key, value, _expire_at_ms, _lfu, _file_id, _offset, _size}
       )
       when is_binary(value),
       do: {:ok, value}

  defp keydir_row_value(shard_data_path, {_key, nil, _expire_at_ms, _lfu, file_id, offset, _size})
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 do
    shard_data_path
    |> ShardETS.file_path(file_id)
    |> NIF.v2_pread_at(offset)
  end

  defp keydir_row_value(_shard_data_path, _row), do: :error

  defp decode_flow_record(value) when is_binary(value) do
    {:ok, flow_call(:decode_record, [value])}
  rescue
    _ -> :error
  end

  defp default_history_hot_max_events do
    Ferricstore.Flow.RetryPolicy.default_retention().history_hot_max_events
  rescue
    _ -> 1
  end

  defp skip_history_log_recover?(shard_data_path, projected)
       when is_integer(projected) and projected >= 0 do
    default_history_hot_max_events() == 0 and lmdb_projection_present?(shard_data_path) and
      history_log_safe_to_skip?(shard_data_path)
  end

  defp skip_history_log_recover?(_shard_data_path, _projected), do: false

  defp history_log_safe_to_skip?(shard_data_path) do
    shard_data_path
    |> history_file_path(0)
    |> File.stat()
    |> case do
      {:ok, %{type: :regular, size: 0}} -> true
      {:ok, %{type: :directory}} -> true
      {:error, :enoent} -> true
      _ -> false
    end
  end

  defp lmdb_projection_present?(shard_data_path) do
    shard_data_path
    |> Ferricstore.Flow.LMDB.path()
    |> Path.join("data.mdb")
    |> File.stat()
    |> case do
      {:ok, %{type: :regular, size: size}} when size > 0 -> true
      _ -> false
    end
  end

  defp prepare_recovered_history_projector(instance_ctx, shard_index, shard_data_path) do
    with :ok <- ensure_history_file(shard_data_path) do
      projected = HistoryProjectedIndex.read(shard_data_path)
      publish_projected_index(instance_ctx, shard_index, shard_data_path, projected)
    end
  rescue
    error -> {:error, {:history_projector_prepare_failed, error}}
  end

  defp emit_recover_error(instance_ctx, shard_index, reason) do
    :telemetry.execute(
      [:ferricstore, :flow, :history_projector, :recover],
      %{errors: 1},
      %{
        instance: instance_name(instance_ctx),
        shard_index: shard_index,
        reason: reason
      }
    )
  rescue
    _ -> :ok
  end

  defp emit_queue_full(
         instance_ctx,
         shard_index,
         pending_entries,
         incoming_entries,
         max_pending_entries
       ) do
    :telemetry.execute(
      [:ferricstore, :flow, :history_projector, :queue_full],
      %{
        count: 1,
        pending_entries: pending_entries,
        incoming_entries: incoming_entries,
        max_pending_entries: max_pending_entries
      },
      %{
        instance: instance_name(instance_ctx),
        shard_index: shard_index
      }
    )
  rescue
    _ -> :ok
  end

  defp publish_requested_index(instance_ctx, shard_index, index)
       when is_integer(index) and index >= 0 do
    put_atomic_max(instance_ctx, :flow_history_requested_index, shard_index, index)
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
    add_atomic(instance_ctx, :flow_history_projector_queue_full, shard_index, 1)
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

  defp put_atomic_max(instance_ctx, field, shard_index, value)
       when is_integer(shard_index) and shard_index >= 0 and is_integer(value) and value >= 0 do
    case Map.get(instance_ctx || %{}, field) do
      ref when is_reference(ref) ->
        size = :atomics.info(ref).size

        if shard_index < size do
          index = shard_index + 1
          current = :atomics.get(ref, index)
          if value > current, do: :atomics.put(ref, index, value)
        end

      _ ->
        :ok
    end

    :ok
  rescue
    _ -> :ok
  end

  defp add_atomic(instance_ctx, field, shard_index, increment)
       when is_integer(shard_index) and shard_index >= 0 and is_integer(increment) do
    case Map.get(instance_ctx || %{}, field) do
      ref when is_reference(ref) ->
        size = :atomics.info(ref).size
        if shard_index < size, do: :atomics.add(ref, shard_index + 1, increment)

      _ ->
        :ok
    end

    :ok
  rescue
    _ -> :ok
  end

  defp parse_history_entry_key("X:" <> rest) do
    case :binary.split(rest, <<0>>) do
      [history_key, event_id] ->
        case parse_event_ms(event_id) do
          {:ok, event_ms} -> {:ok, history_key, event_id, event_ms}
          :error -> :error
        end

      _ ->
        :error
    end
  end

  defp parse_history_entry_key(_key), do: :error

  defp history_entry_key(history_key, event_id), do: "X:" <> history_key <> <<0>> <> event_id

  defp parse_event_ms(event_id) do
    case :binary.split(event_id, "-") do
      [ms, _version] ->
        case Integer.parse(ms) do
          {event_ms, ""} -> {:ok, event_ms}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp parse_event_version(event_id) do
    case :binary.split(event_id, "-") do
      [_ms, version] ->
        case Integer.parse(version) do
          {parsed, ""} -> {:ok, parsed}
          _ -> :error
        end

      _ ->
        :error
    end
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

  defp publish_projected_index(_instance_ctx, _shard_index, _shard_data_path, nil), do: :ok

  defp publish_projected_index(instance_ctx, shard_index, shard_data_path, index)
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

  defp keydir(%{keydir_refs: refs}, shard_index) when is_tuple(refs), do: elem(refs, shard_index)
  defp keydir(_instance_ctx, shard_index), do: :"keydir_#{shard_index}"

  defp instance_name(%{name: name}), do: name
  defp instance_name(_instance_ctx), do: :default

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

  defp track_keydir_binary_delta(%{keydir_binary_bytes: ref}, keydir, shard_index, key, new_value)
       when is_reference(ref) do
    old_bytes =
      case safe_ets_lookup(keydir, key) do
        [{^key, old_value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}] ->
          binary_bytes(key) + binary_bytes(old_value)

        _ ->
          0
      end

    new_bytes = binary_bytes(key) + binary_bytes(new_value)
    delta = new_bytes - old_bytes
    if delta != 0, do: :atomics.add(ref, shard_index + 1, delta)
    :ok
  rescue
    _ -> :ok
  end

  defp track_keydir_binary_delta(_instance_ctx, _keydir, _shard_index, _key, _new_value), do: :ok

  defp track_keydir_binary_remove_row(%{keydir_binary_bytes: ref}, shard_index, row)
       when is_reference(ref) do
    bytes = keydir_row_binary_bytes(row)
    if bytes > 0, do: :atomics.sub(ref, shard_index + 1, bytes)
    :ok
  rescue
    _ -> :ok
  end

  defp track_keydir_binary_remove_row(_instance_ctx, _shard_index, _row), do: :ok

  defp delete_keydir_row(instance_ctx, keydir, shard_index, key) do
    case safe_ets_lookup(keydir, key) do
      [row] ->
        track_keydir_binary_remove_row(instance_ctx, shard_index, row)
        delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)
        safe_ets_delete(keydir, key)

      _missing ->
        :ok
    end
  end

  defp delete_apply_projection_cache_for_row(
         %{data_dir: data_dir},
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
  end

  defp delete_apply_projection_cache_for_row(_instance_ctx, _shard_index, _row), do: :ok

  defp keydir_row_binary_bytes(
         {key, old_value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}
       ),
       do: binary_bytes(key) + binary_bytes(old_value)

  defp keydir_row_binary_bytes(_row), do: 0

  defp binary_bytes(value) when is_binary(value) and byte_size(value) > 64, do: byte_size(value)
  defp binary_bytes(_value), do: 0

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

  defp safe_ets_lookup(table, key) do
    :ets.lookup(table, key)
  rescue
    ArgumentError -> []
  end

  defp safe_ets_insert(table, row) do
    :ets.insert(table, row)
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp safe_ets_delete(table, key) do
    :ets.delete(table, key)
    :ok
  rescue
    ArgumentError -> :ok
  end

end
