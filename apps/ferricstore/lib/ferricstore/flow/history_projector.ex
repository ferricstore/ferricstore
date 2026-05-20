defmodule Ferricstore.Flow.HistoryProjector do
  @moduledoc false

  use GenServer

  require Logger

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Flow.HistoryProjectedIndex
  alias Ferricstore.Flow.OrderedIndex, as: FlowIndex
  alias Ferricstore.Store.{DiskPressure, LFU, WriteVersion}

  @default_batch_size 16_384
  @default_flush_interval_ms 250
  @retry_interval_ms 50

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
        case recover_history_log(instance_ctx, shard_index, shard_data_path, keydir_override) do
          :ok ->
            projected = HistoryProjectedIndex.read(shard_data_path)
            publish_projected_index(instance_ctx, shard_index, shard_data_path, projected)

          {:error, reason} = error ->
            emit_recover_error(instance_ctx, shard_index, reason)
            error
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

  @spec request(map() | nil, non_neg_integer(), binary(), non_neg_integer()) ::
          :durable | :requested
  def request(instance_ctx, shard_index, shard_data_path, index) do
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

  @spec write_entries_sync(
          map() | nil,
          non_neg_integer(),
          binary(),
          [entry()],
          non_neg_integer() | nil
        ) :: :ok | {:error, term()}
  def write_entries_sync(
        instance_ctx,
        shard_index,
        shard_data_path,
        entries,
        requested_index \\ nil
      ) do
    do_project(instance_ctx, shard_index, shard_data_path, entries, requested_index)
  end

  @impl true
  def init(opts) do
    Process.flag(:priority, :low)

    shard_index = Keyword.fetch!(opts, :shard_index)
    shard_data_path = Keyword.fetch!(opts, :shard_data_path)
    instance_ctx = Keyword.get(opts, :instance_ctx)

    :ok =
      if Keyword.get(opts, :recover_on_init, true) do
        recover(instance_ctx, shard_index, shard_data_path)
      else
        prepare_recovered_history_projector(instance_ctx, shard_index, shard_data_path)
      end

    {:ok,
     %{
       shard_index: shard_index,
       shard_data_path: shard_data_path,
       instance_ctx: instance_ctx,
       pending: [],
       pending_count: 0,
       flush_timer: nil,
       requested_index: nil,
       batch_size: app_env(:flow_history_projector_batch_size, @default_batch_size),
       flush_interval_ms:
         app_env(:flow_history_projector_flush_interval_ms, @default_flush_interval_ms)
     }}
  end

  @impl true
  def handle_cast({:enqueue, entries}, state) do
    {:noreply, enqueue_entries(state, entries)}
  end

  def handle_cast({:project_to, index}, state) do
    requested_index = max_index(state.requested_index, index)
    {:noreply, flush_pending(%{state | requested_index: requested_index})}
  end

  @impl true
  def handle_call(:flush, _from, state) do
    state = flush_pending(state)
    result = if state.pending_count == 0, do: :ok, else: {:error, :flush_failed}
    {:reply, result, state}
  end

  def handle_call({:enqueue, entries}, _from, state) do
    {:reply, :ok, enqueue_entries(state, entries)}
  end

  @impl true
  def handle_info(:flush_timer, state) do
    {:noreply, flush_pending(%{state | flush_timer: nil})}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp enqueue_entries(state, entries) do
    pending = Enum.reverse(entries, state.pending)
    count = state.pending_count + length(entries)
    state = %{state | pending: pending, pending_count: count}

    if count >= state.batch_size do
      schedule_flush_soon(state)
    else
      schedule_flush(state)
    end
  end

  defp flush_pending(%{pending_count: 0, requested_index: nil} = state), do: cancel_flush(state)

  defp flush_pending(state) do
    state = cancel_flush(state)
    entries = Enum.reverse(state.pending)

    case do_project(
           state.instance_ctx,
           state.shard_index,
           state.shard_data_path,
           entries,
           state.requested_index
         ) do
      :ok ->
        %{state | pending: [], pending_count: 0, requested_index: nil}

      {:error, reason} ->
        mark_flush_failure(state.instance_ctx, state.shard_index)

        Logger.error(
          "Flow history projector shard_#{state.shard_index}: flush failed: #{inspect(reason)}"
        )

        retry = Process.send_after(self(), :flush_timer, @retry_interval_ms)
        %{state | flush_timer: retry}
    end
  end

  defp do_project(instance_ctx, shard_index, shard_data_path, [], requested_index) do
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

  defp do_project(instance_ctx, shard_index, shard_data_path, entries, requested_index) do
    with {:ok, file_id, file_path} <- history_active_file(shard_data_path),
         keydir <- keydir(instance_ctx, shard_index),
         encoded_entries <- Enum.map(entries, &encode_entry/1),
         batch <- Enum.map(encoded_entries, &{&1.key, &1.value, &1.expire_at_ms}),
         {:ok, locations} <- append_batch(file_path, batch),
         :ok <- validate_locations(encoded_entries, locations) do
      clear_disk_pressure(instance_ctx, shard_index)

      publish_keydir_entries(
        instance_ctx,
        shard_index,
        keydir,
        file_id,
        encoded_entries,
        locations
      )

      with :ok <- publish_history_index(instance_ctx, shard_index, encoded_entries),
           :ok <-
             trim_history_caps(
               instance_ctx,
               shard_index,
               keydir,
               file_path,
               encoded_entries
             ) do
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
      case Enum.find(records, fn
             {^target_key, _offset, _value_size, _expire_at_ms, false} -> true
             _ -> false
           end) do
        {^target_key, offset, _value_size, _expire_at_ms, false} ->
          NIF.v2_pread_at(file_path, offset)

        nil ->
          :miss
      end
    end
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

    with :ok <- File.mkdir_p(dir),
         :ok <- touch_if_missing(path) do
      :ok
    end
  end

  defp touch_if_missing(path) do
    if File.exists?(path), do: :ok, else: File.touch(path)
  end

  defp append_batch(file_path, batch), do: NIF.v2_append_batch_nosync(file_path, batch)

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
         file_path,
         index,
         requested_index
       )
       when is_integer(index) and is_integer(requested_index) do
    with :ok <- NIF.v2_fsync(file_path) do
      publish_projected_index(instance_ctx, shard_index, shard_data_path, index)
    end
  end

  defp encode_entry(%{value: value} = entry) when is_binary(value), do: entry

  defp encode_entry(%{snapshot: snapshot} = entry) do
    Map.put(entry, :value, Flow.encode_history_snapshot(snapshot))
  end

  defp encode_entry(%{record: record, event: event, now_ms: now_ms} = entry) do
    value = Flow.encode_history_fields(record, event, now_ms, Map.get(entry, :meta, %{}))
    Map.put(entry, :value, value)
  end

  defp validate_locations(entries, locations) when length(entries) == length(locations), do: :ok

  defp validate_locations(entries, locations),
    do: {:error, {:location_count_mismatch, length(entries), length(locations)}}

  defp publish_keydir_entries(instance_ctx, shard_index, keydir, file_id, entries, locations) do
    entries
    |> Enum.zip(locations)
    |> Enum.each(fn {entry, {offset, value_size}} ->
      track_keydir_binary_delta(instance_ctx, keydir, shard_index, entry.key, nil)

      :ets.insert(
        keydir,
        {entry.key, nil, entry.expire_at_ms, LFU.initial(), {:flow_history, file_id}, offset,
         value_size}
      )
    end)
  end

  defp publish_history_index(instance_ctx, shard_index, entries) do
    {flow_index, flow_lookup} = FlowIndex.table_names(instance_name(instance_ctx), shard_index)
    native = NativeFlowIndex.get(flow_index, flow_lookup)

    {new_entries, update_entries} =
      entries
      |> Enum.map(&{&1.history_key, &1.event_id, &1.event_ms, &1.version})
      |> Enum.split_with(fn {_key, _event_id, _event_ms, version} -> version == 1 end)

    new_entries =
      Enum.map(new_entries, fn {key, event_id, event_ms, _version} ->
        {key, event_id, event_ms}
      end)

    update_entries =
      Enum.map(update_entries, fn {key, event_id, event_ms, _version} ->
        {key, event_id, event_ms}
      end)

    if new_entries != [], do: FlowIndex.put_new_entries(flow_index, flow_lookup, new_entries)
    if update_entries != [], do: FlowIndex.put_entries(flow_index, flow_lookup, update_entries)
    if native && new_entries != [], do: NativeFlowIndex.put_new_entries(native, new_entries)
    if native && update_entries != [], do: NativeFlowIndex.put_entries(native, update_entries)

    :ok
  end

  defp trim_history_caps(instance_ctx, shard_index, keydir, file_path, entries) do
    caps = history_caps(entries)

    if map_size(caps) == 0 do
      :ok
    else
      {flow_index, flow_lookup} = FlowIndex.table_names(instance_name(instance_ctx), shard_index)
      native = NativeFlowIndex.get(flow_index, flow_lookup)

      trim_items =
        caps
        |> Enum.flat_map(fn {history_key, max_events} ->
          count = FlowIndex.count_all(flow_lookup, history_key)

          if count > max_events do
            flow_index
            |> FlowIndex.rank_range(history_key, 0, count - max_events - 1, false)
            |> Enum.map(fn {event_id, _score} ->
              {history_key, event_id, history_entry_key(history_key, event_id)}
            end)
          else
            []
          end
        end)

      case trim_items do
        [] ->
          :ok

        [_ | _] ->
          tombstone_keys = Enum.map(trim_items, fn {_history_key, _event_id, key} -> key end)

          with :ok <- append_tombstones(file_path, tombstone_keys) do
            trim_items
            |> Enum.group_by(fn {history_key, _event_id, _key} -> history_key end)
            |> Enum.each(fn {history_key, items} ->
              event_ids = Enum.map(items, fn {_history_key, event_id, _key} -> event_id end)
              FlowIndex.delete_members(flow_index, flow_lookup, history_key, event_ids)
              if native, do: NativeFlowIndex.delete_members(native, history_key, event_ids)
            end)

            Enum.each(tombstone_keys, fn key ->
              track_keydir_binary_remove(instance_ctx, keydir, shard_index, key)
              :ets.delete(keydir, key)
            end)

            :ok
          end
      end
    end
  end

  defp history_caps(entries) do
    Enum.reduce(entries, %{}, fn
      %{history_key: history_key, history_max_events: max_events}, acc
      when is_binary(history_key) and is_integer(max_events) and max_events > 0 ->
        Map.put(acc, history_key, max_events)

      _entry, acc ->
        acc
    end)
  end

  defp append_tombstones(_file_path, []), do: :ok

  defp append_tombstones(file_path, keys) do
    ops = Enum.map(keys, &{:delete, &1})

    case NIF.v2_append_ops_batch_nosync(file_path, ops) do
      {:ok, locations} -> validate_tombstone_locations(locations, length(keys))
      {:error, _reason} = error -> error
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

  defp recover_history_log(instance_ctx, shard_index, shard_data_path, keydir_override) do
    file_path = history_file_path(shard_data_path, 0)

    case NIF.v2_scan_file(file_path) do
      {:ok, records} ->
        keydir = keydir_override || keydir(instance_ctx, shard_index)
        {flow_index, flow_lookup} = FlowIndex.table_names(instance_name(instance_ctx), shard_index)
        native = NativeFlowIndex.get(flow_index, flow_lookup)

        Enum.each(records, fn
          {key, _offset, _value_size, _expire_at_ms, true} ->
            track_keydir_binary_remove(instance_ctx, keydir, shard_index, key)
            :ets.delete(keydir, key)

            case parse_history_entry_key(key) do
              {:ok, history_key, event_id, _event_ms} ->
                FlowIndex.delete_member(flow_index, flow_lookup, history_key, event_id)
                if native, do: NativeFlowIndex.delete_member(native, history_key, event_id)

              :error ->
                :ok
            end

          {key, offset, value_size, expire_at_ms, false} ->
            track_keydir_binary_delta(instance_ctx, keydir, shard_index, key, nil)

            :ets.insert(
              keydir,
              {key, nil, expire_at_ms, LFU.initial(), {:flow_history, 0}, offset, value_size}
            )

            case parse_history_entry_key(key) do
              {:ok, history_key, event_id, event_ms} ->
                FlowIndex.put_member(flow_index, flow_lookup, history_key, event_id, event_ms)
                if native, do: NativeFlowIndex.put_member(native, history_key, event_id, event_ms)

              :error ->
                :ok
            end
          end)

        :ok

      {:error, reason} ->
        {:error, {:history_scan_failed, reason}}

      other ->
        {:error, {:history_scan_unexpected, other}}
    end
  rescue
    error -> {:error, {:history_recover_exception, error}}
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

    case instance_ctx do
      %{flow_history_projected_index: ref} when is_reference(ref) ->
        size = :atomics.info(ref).size
        if shard_index < size, do: :atomics.put(ref, shard_index + 1, index)

      _ ->
        :ok
    end

    HistoryProjectedIndex.persist(shard_data_path, index)
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
      case :ets.lookup(keydir, key) do
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

  defp track_keydir_binary_remove(%{keydir_binary_bytes: ref}, keydir, shard_index, key)
       when is_reference(ref) do
    bytes =
      case :ets.lookup(keydir, key) do
        [{^key, old_value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}] ->
          binary_bytes(key) + binary_bytes(old_value)

        _ ->
          0
      end

    if bytes > 0, do: :atomics.sub(ref, shard_index + 1, bytes)
    :ok
  rescue
    _ -> :ok
  end

  defp track_keydir_binary_remove(_instance_ctx, _keydir, _shard_index, _key), do: :ok

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

  defp app_env(key, default) do
    case Application.get_env(:ferricstore, key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end
end
