defmodule Ferricstore.Flow.HistoryProjector do
  @moduledoc false

  use GenServer

  require Logger

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Flow.HistoryProjectedIndex
  alias Ferricstore.Flow.OrderedIndex, as: FlowIndex
  alias Ferricstore.Store.{DiskPressure, LFU, WriteVersion}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @default_batch_size 16_384
  @default_flush_interval_ms 250
  @default_max_pending_entries 100_000
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
        # This is used from Raft apply. It must never wait behind cold history
        # projection or LMDB work; release-cursor gating handles durability.
        GenServer.cast(pid, {:enqueue, stamp_ra_index(entries, ra_index)})
        :ok
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
       max_pending_entries:
         Ferricstore.MemoryBudget.limit(
           :flow_history_projector_max_pending_entries,
           @default_max_pending_entries
         ),
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
    case pending_capacity(state, length(entries)) do
      :ok ->
        {:reply, :ok, enqueue_entries(state, entries)}

      {:error, :queue_full} = error ->
        emit_queue_full(state, length(entries))
        {:reply, error, state}
    end
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

  defp pending_capacity(%{max_pending_entries: :infinity}, _new_count), do: :ok

  defp pending_capacity(%{pending_count: count, max_pending_entries: max_pending}, new_count)
       when is_integer(max_pending) do
    if count + new_count <= max_pending, do: :ok, else: {:error, :queue_full}
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
           state.requested_index,
           nil
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
             publish_lmdb_history_locations(shard_data_path, file_id, encoded_entries, locations),
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
    entries
    |> Enum.zip(locations)
    |> Enum.each(fn {entry, {offset, value_size}} ->
      case :ets.lookup(keydir, entry.key) do
        [row] -> delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)
        _missing -> :ok
      end

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

  defp publish_lmdb_history_locations(shard_data_path, file_id, entries, locations) do
    ops =
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

    case collect_projected_flow_values(instance_ctx, shard_index, shard_data_path, keydir, refs) do
      [] ->
        :ok

      value_entries ->
        batch =
          Enum.map(value_entries, fn %{key: key, value: value, expire_at_ms: expire_at_ms} ->
            {key, value, expire_at_ms}
          end)

        with {:ok, locations} <- append_batch(file_path, batch),
             :ok <- validate_projected_value_locations(value_entries, locations),
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
  end

  defp collect_projected_flow_values(instance_ctx, shard_index, shard_data_path, keydir, refs) do
    refs
    |> Enum.reduce([], fn ref, acc ->
      case projected_flow_value_entry(instance_ctx, shard_index, shard_data_path, keydir, ref) do
        {:ok, entry} -> [entry | acc]
        :skip -> acc
      end
    end)
    |> Enum.reverse()
  end

  defp projected_flow_value_entry(instance_ctx, shard_index, shard_data_path, keydir, key) do
    case :ets.lookup(keydir, key) do
      [{^key, value, expire_at_ms, lfu, file_id, offset, value_size} = row] ->
        if readable_value_locator?(file_id, offset, value_size) do
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
              {:ok,
               %{
                 key: key,
                 value: bytes,
                 expire_at_ms: expire_at_ms,
                 lfu: lfu,
                 source_file_id: file_id,
                 source_offset: offset,
                 source_value_size: value_size,
                 source_row: row
               }}

            _error ->
              :skip
          end
        else
          :skip
        end

      _other ->
        :skip
    end
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
    |> Ferricstore.Store.ColdRead.pread_at(offset, key, 10_000)
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

  defp publish_lmdb_value_locations(shard_data_path, file_id, entries, locations) do
    ops =
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

    shard_data_path
    |> Ferricstore.Flow.LMDB.path()
    |> Ferricstore.Flow.LMDB.write_batch(ops)
  end

  defp delete_projected_flow_value_keydir_rows(instance_ctx, shard_index, keydir, entries) do
    {count, bytes} =
      Enum.reduce(entries, {0, 0}, fn %{key: key, value: value, source_row: row},
                                      {count_acc, bytes_acc} ->
        case :ets.lookup(keydir, key) do
          [^row] ->
            bytes = binary_bytes(value)
            track_keydir_binary_remove(instance_ctx, keydir, shard_index, key)
            delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)
            :ets.delete(keydir, key)
            {count_acc + 1, bytes_acc + bytes}

          _changed ->
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
      when byte_size(tag) > 0 and kind in [?p, ?r, ?e] and byte_size(rest) > 0 ->
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
    caps = history_caps(entries, keydir, shard_data_path)

    if map_size(caps) == 0 do
      :ok
    else
      {flow_index, flow_lookup} = FlowIndex.table_names(instance_name(instance_ctx), shard_index)
      native = NativeFlowIndex.get(flow_index, flow_lookup)

      trim_items =
        caps
        |> Enum.flat_map(fn {history_key, max_events} ->
          if history_cap_check_required?(entries, history_key, max_events) do
            history_key
            |> lmdb_history_over_cap_items(shard_data_path, max_events)
            |> Enum.map(fn {event_id, key, history_index_key} ->
              {history_key, event_id, key, history_index_key}
            end)
          else
            []
          end
        end)

      case trim_items do
        [] ->
          :ok

        [_ | _] ->
          tombstone_keys =
            Enum.map(trim_items, fn {_history_key, _event_id, key, _history_index_key} -> key end)

          with :ok <- append_tombstones(file_path, tombstone_keys),
               :ok <- delete_lmdb_history_entries(shard_data_path, trim_items) do
            trim_items
            |> Enum.group_by(fn {history_key, _event_id, _key, _history_index_key} ->
              history_key
            end)
            |> Enum.each(fn {history_key, items} ->
              event_ids =
                Enum.map(items, fn {_history_key, event_id, _key, _history_index_key} ->
                  event_id
                end)

              FlowIndex.delete_members(flow_index, flow_lookup, history_key, event_ids)
              if native, do: NativeFlowIndex.delete_members(native, history_key, event_ids)
            end)

            Enum.each(tombstone_keys, fn key ->
              delete_keydir_row(instance_ctx, keydir, shard_index, key)
            end)

            :ok
          end
      end
    end
  end

  defp history_caps(entries, keydir, shard_data_path) do
    {caps, history_keys} =
      Enum.reduce(entries, {%{}, MapSet.new()}, fn
        %{history_key: history_key, history_max_events: max_events}, {caps, keys}
        when is_binary(history_key) and is_integer(max_events) and max_events > 0 ->
          {Map.put(caps, history_key, max_events), MapSet.put(keys, history_key)}

        %{history_key: history_key}, {caps, keys} when is_binary(history_key) ->
          {caps, MapSet.put(keys, history_key)}

        _entry, acc ->
          acc
      end)

    Enum.reduce(history_keys, caps, fn history_key, acc ->
      if Map.has_key?(acc, history_key) do
        acc
      else
        case load_history_max_cap(history_key, keydir, shard_data_path) do
          max_events when is_integer(max_events) and max_events > 0 ->
            Map.put(acc, history_key, max_events)

          _ ->
            acc
        end
      end
    end)
  end

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

  defp history_cap_check_required?(entries, history_key, max_events) do
    Enum.any?(entries, fn
      %{history_key: ^history_key, version: version}
      when is_integer(version) and version > max_events ->
        true

      %{history_key: ^history_key, version: version} when not is_integer(version) ->
        true

      %{history_key: ^history_key} = entry ->
        not Map.has_key?(entry, :version)

      _entry ->
        false
    end)
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
    caps = history_hot_caps(entries)

    if map_size(caps) == 0 do
      :ok
    else
      {flow_index, flow_lookup} = FlowIndex.table_names(instance_name(instance_ctx), shard_index)
      native = NativeFlowIndex.get(flow_index, flow_lookup)

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
      |> evict_hot_history_items(
        instance_ctx,
        shard_index,
        keydir,
        flow_index,
        flow_lookup,
        native
      )
    end
  end

  defp history_hot_caps(entries) do
    Enum.reduce(entries, %{}, fn
      %{history_key: history_key, terminal?: true}, acc when is_binary(history_key) ->
        put_history_hot_cap(acc, history_key, 0)

      %{history_key: history_key, history_hot_max_events: max_events}, acc
      when is_binary(history_key) and is_integer(max_events) and max_events > 0 ->
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
         _flow_index,
         _flow_lookup,
         _native
       ),
       do: :ok

  defp evict_hot_history_items(
         items,
         instance_ctx,
         shard_index,
         keydir,
         flow_index,
         flow_lookup,
         native
       ) do
    items
    |> Enum.group_by(fn {history_key, _event_id, _key} -> history_key end)
    |> Enum.each(fn {history_key, group} ->
      event_ids = Enum.map(group, fn {_history_key, event_id, _key} -> event_id end)
      FlowIndex.delete_members(flow_index, flow_lookup, history_key, event_ids)
      if native, do: NativeFlowIndex.delete_members(native, history_key, event_ids)
    end)

    Enum.each(items, fn {_history_key, _event_id, key} ->
      delete_keydir_row(instance_ctx, keydir, shard_index, key)
    end)

    :ok
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

        publish_keydir_entries(instance_ctx, shard_index, keydir, 0, entries, locations)

        with :ok <- publish_history_index(instance_ctx, shard_index, entries),
             :ok <- publish_lmdb_history_locations(shard_data_path, 0, entries, locations),
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

            entry = %{
              key: key,
              expire_at_ms: expire_at_ms,
              history_key: history_key,
              event_id: event_id,
              event_ms: event_ms,
              version: 2,
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

  defp load_history_hot_cap(history_key, keydir, shard_data_path) do
    with {:ok, state_key} <- history_state_key(history_key),
         {:ok, %{history_hot_max_events: max_events}} <-
           load_history_state_record(state_key, keydir, shard_data_path),
         true <- is_integer(max_events) and max_events > 0 do
      max_events
    else
      _ -> default_history_hot_max_events()
    end
  end

  defp load_history_state_record(state_key, keydir, shard_data_path) do
    case :ets.lookup(keydir, state_key) do
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

  defp emit_queue_full(state, incoming_entries) do
    :telemetry.execute(
      [:ferricstore, :flow, :history_projector, :queue_full],
      %{
        count: 1,
        pending_entries: state.pending_count,
        incoming_entries: incoming_entries,
        max_pending_entries: state.max_pending_entries
      },
      %{
        instance: instance_name(state.instance_ctx),
        shard_index: state.shard_index
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

  defp delete_keydir_row(instance_ctx, keydir, shard_index, key) do
    case :ets.lookup(keydir, key) do
      [row] ->
        track_keydir_binary_remove(instance_ctx, keydir, shard_index, key)
        delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)
        :ets.delete(keydir, key)

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
