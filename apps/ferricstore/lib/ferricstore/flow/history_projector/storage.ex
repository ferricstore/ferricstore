defmodule Ferricstore.Flow.HistoryProjector.Storage do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.HistoryProjector.KeyCodec
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Store.LFU

  @max_exact_integer 9_007_199_254_740_991

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
  defdelegate read_value(shard_data_path, file_id, offset),
    to: Ferricstore.Flow.HistoryProjector.Log

  @spec scan_event_value(binary(), binary()) :: {:ok, binary()} | :miss | {:error, term()}
  defdelegate scan_event_value(shard_data_path, target_key),
    to: Ferricstore.Flow.HistoryProjector.Log

  def history_active_file(shard_data_path) do
    with :ok <- ensure_history_file(shard_data_path) do
      {:ok, 0, history_file_path(shard_data_path, 0)}
    end
  rescue
    error -> {:error, {:history_file_unavailable, error}}
  end

  def ensure_history_file(shard_data_path) do
    dir = history_dir(shard_data_path)
    path = history_file_path(shard_data_path, 0)

    with :ok <- Ferricstore.FS.mkdir_p(dir),
         :ok <- touch_if_missing(path),
         :ok <- validate_history_file(path) do
      :ok
    end
  end

  def touch_if_missing(path) do
    if Ferricstore.FS.exists?(path), do: :ok, else: Ferricstore.FS.touch(path)
  end

  defp validate_history_file(path) do
    case File.lstat(path) do
      {:ok, %{type: :regular}} -> :ok
      {:ok, %{type: type}} -> {:error, {:invalid_history_file_type, type}}
      {:error, reason} -> {:error, {:history_file_stat_failed, reason}}
    end
  end

  def append_batch(file_path, batch), do: NIF.v2_append_batch_nosync(file_path, batch)

  def sync_history_log_before_publish(file_path) do
    with :ok <- NIF.v2_fsync(file_path) do
      after_history_log_sync(file_path)
    end
  end

  def after_history_log_sync(file_path) do
    with :ok <-
           Ferricstore.FaultInjection.maybe_pause(:after_flow_history_fsync, %{
             file_path: file_path
           }) do
      maybe_history_projector_fsync_hook(file_path)
    end
  end

  def maybe_history_projector_fsync_hook(file_path) do
    case Application.get_env(:ferricstore, :flow_history_projector_fsync_hook) do
      fun when is_function(fun, 1) -> fun.(file_path)
      _other -> :ok
    end
  end

  def expand_entries(entries) when is_list(entries), do: Enum.flat_map(entries, &expand_entry/1)

  def expand_entry(
        %{history_chain: {created, final, step_states}, history_key: history_key} = entry
      ) do
    expand_history_chain(history_key, created, final, step_states, Map.get(entry, :ra_index))
  end

  def expand_entry(entry), do: [entry]

  defp expand_history_chain(history_key, created, final, step_states, ra_index) do
    now_ms = Map.fetch!(created, :updated_at_ms)

    {created_entry, previous_ms} =
      chain_history_entry(created, history_key, "created", now_ms, nil, ra_index)

    {step_entries, previous_ms} =
      step_states
      |> Enum.drop(1)
      |> Enum.with_index(2)
      |> Enum.reduce({[], previous_ms}, fn {run_state, version}, {entries, previous_history_ms} ->
        event_ms = now_ms + version - 1

        record = %{
          created
          | version: version,
            updated_at_ms: event_ms,
            run_state: run_state,
            fencing_token: version
        }

        {step_entry, next_previous_ms} =
          chain_history_entry(
            record,
            history_key,
            "step_continued",
            event_ms,
            previous_history_ms,
            ra_index
          )

        {[step_entry | entries], next_previous_ms}
      end)

    {completed_entry, _event_ms} =
      chain_history_entry(
        final,
        history_key,
        "completed",
        Map.fetch!(final, :updated_at_ms),
        previous_ms,
        ra_index
      )

    [created_entry | Enum.reverse(step_entries)] ++ [completed_entry]
  end

  defp chain_history_entry(record, history_key, event, now_ms, previous_history_ms, ra_index) do
    version = Map.fetch!(record, :version)
    {event_id, event_ms} = chain_history_next_event(now_ms, version, previous_history_ms)

    entry =
      %{
        key: Ferricstore.Flow.Keys.stream_entry_key_from_history_key(history_key, event_id),
        expire_at_ms: 0,
        history_key: history_key,
        event_id: event_id,
        event_ms: event_ms,
        version: version,
        history_hot_max_events: Map.get(record, :history_hot_max_events),
        history_max_events: Map.get(record, :history_max_events),
        terminal?: Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)),
        value: {:flow_history_fields, record, event, now_ms, %{}}
      }
      |> maybe_put_ra_index(ra_index)
      |> maybe_put_hot_evict_event_ids(
        chain_history_hot_evict_event_ids(record, event_id, version, previous_history_ms)
      )

    {entry, entry.event_ms}
  end

  defp chain_history_next_event(now_ms, 1, _previous_history_ms) do
    {Integer.to_string(trunc(now_ms)) <> "-1", trunc(now_ms)}
  end

  defp chain_history_next_event(now_ms, version, previous_history_ms)
       when is_integer(previous_history_ms) do
    ms = max(trunc(now_ms), previous_history_ms)
    {Integer.to_string(ms) <> "-" <> Integer.to_string(version), ms}
  end

  defp chain_history_next_event(now_ms, version, _previous_history_ms) do
    {Integer.to_string(trunc(now_ms)) <> "-" <> Integer.to_string(version), trunc(now_ms)}
  end

  defp chain_history_hot_evict_event_ids(record, event_id, version, previous_history_ms) do
    []
    |> maybe_add_terminal_hot_evict_event_id(record, event_id)
    |> maybe_add_previous_hot_evict_event_id(record, version, previous_history_ms)
    |> Enum.uniq()
  end

  defp maybe_add_terminal_hot_evict_event_id(ids, record, event_id) do
    if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) and is_binary(event_id) and
         event_id != "" do
      [event_id | ids]
    else
      ids
    end
  end

  defp maybe_add_previous_hot_evict_event_id(ids, record, version, previous_history_ms) do
    if Map.get(record, :history_hot_max_events) == 1 and is_integer(version) and version > 1 and
         is_integer(previous_history_ms) do
      previous_id =
        Integer.to_string(previous_history_ms) <> "-" <> Integer.to_string(version - 1)

      [previous_id | ids]
    else
      ids
    end
  end

  defp maybe_put_hot_evict_event_ids(entry, []), do: entry
  defp maybe_put_hot_evict_event_ids(entry, ids), do: Map.put(entry, :hot_evict_event_ids, ids)

  defp maybe_put_ra_index(entry, nil), do: entry

  defp maybe_put_ra_index(entry, ra_index) when is_integer(ra_index),
    do: Map.put(entry, :ra_index, ra_index)

  def maybe_persist_projected_index(
        _instance_ctx,
        _shard_index,
        _shard_data_path,
        _file_path,
        _index,
        nil
      ),
      do: :ok

  def maybe_persist_projected_index(
        instance_ctx,
        shard_index,
        shard_data_path,
        _file_path,
        index,
        requested_index
      )
      when is_integer(index) and is_integer(requested_index) do
    HistoryProjector.publish_projected_index(instance_ctx, shard_index, shard_data_path, index)
  end

  def encode_entry(%{value: value} = entry) when is_binary(value), do: entry

  def encode_entry(%{value: {:flow_history_fields, record, event, now_ms, meta}} = entry) do
    value = flow_call(:encode_history_fields, [record, event, now_ms, meta])
    Map.put(entry, :value, value)
  end

  def encode_entry(%{snapshot: snapshot} = entry) do
    Map.put(entry, :value, flow_call(:encode_history_snapshot, [snapshot]))
  end

  def encode_entry(%{record: record, event: event, now_ms: now_ms} = entry) do
    value = flow_call(:encode_history_fields, [record, event, now_ms, Map.get(entry, :meta, %{})])
    Map.put(entry, :value, value)
  end

  def flow_call(function, args) do
    apply(Ferricstore.Flow, function, args)
  end

  def validate_entries(entries) when is_list(entries) do
    entries
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {entry, index}, :ok ->
      case validate_entry(entry) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:invalid_history_entry, index, reason}}}
      end
    end)
  end

  def validate_entries(_entries),
    do: {:error, {:invalid_history_entry_batch, :not_a_list}}

  defp validate_entry(%{
         key: key,
         history_key: history_key,
         event_id: event_id,
         event_ms: event_ms,
         version: version,
         expire_at_ms: expire_at_ms,
         value: value
       })
       when is_binary(key) and is_binary(history_key) and is_binary(event_id) and
              is_integer(event_ms) and is_integer(version) and is_integer(expire_at_ms) and
              is_binary(value) do
    cond do
      not Keys.history_key?(history_key) ->
        {:error, :invalid_history_key}

      key != KeyCodec.history_entry_key(history_key, event_id) ->
        {:error, :physical_key_mismatch}

      KeyCodec.parse_event_ms(event_id) != {:ok, event_ms} or
          KeyCodec.parse_event_version(event_id) != {:ok, version} ->
        {:error, :event_identity_mismatch}

      expire_at_ms < 0 or expire_at_ms > @max_exact_integer ->
        {:error, :invalid_expiration}

      true ->
        :ok
    end
  end

  defp validate_entry(_entry), do: {:error, :invalid_shape}

  def validate_locations(entries, locations) when is_list(entries) and is_list(locations) do
    cond do
      length(entries) != length(locations) ->
        {:error, {:location_count_mismatch, length(entries), length(locations)}}

      Enum.all?(locations, &valid_location?/1) ->
        :ok

      true ->
        {:error, {:invalid_history_locations, locations}}
    end
  end

  def validate_locations(_entries, locations),
    do: {:error, {:invalid_history_locations, locations}}

  defp valid_location?({offset, value_size})
       when is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0,
       do: true

  defp valid_location?(_location), do: false

  def publish_keydir_entries(instance_ctx, shard_index, keydir, file_id, entries, locations) do
    initial_lfu = LFU.initial()

    entries
    |> Enum.zip(locations)
    |> Enum.each(fn {entry, {offset, value_size}} ->
      case HistoryProjector.safe_ets_lookup(keydir, entry.key) do
        [row] ->
          HistoryProjector.delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)

        _missing ->
          :ok
      end

      HistoryProjector.track_keydir_binary_delta(
        instance_ctx,
        keydir,
        shard_index,
        entry.key,
        nil
      )

      HistoryProjector.safe_ets_insert(
        keydir,
        {entry.key, nil, entry.expire_at_ms, initial_lfu, {:flow_history, file_id}, offset,
         value_size}
      )
    end)
  end

  def publish_history_index(instance_ctx, shard_index, entries) do
    {flow_index, flow_lookup} =
      NativeFlowIndex.table_names(HistoryProjector.instance_name(instance_ctx), shard_index)

    native = NativeFlowIndex.get(flow_index, flow_lookup)
    {new_entries, update_entries} = history_index_entries(entries)

    if native && new_entries != [], do: NativeFlowIndex.put_new_entries(native, new_entries)
    if native && update_entries != [], do: NativeFlowIndex.put_entries(native, update_entries)

    :ok
  end

  @doc false
  def __history_index_entries_for_test__(entries), do: history_index_entries(entries)

  def history_index_entries(entries) do
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

  def publish_lmdb_history_locations(shard_data_path, file_id, entries, locations) do
    with :ok <- maybe_history_projector_lmdb_publish_hook(shard_data_path, file_id, entries) do
      write_lmdb_ops(shard_data_path, lmdb_history_location_ops(file_id, entries, locations))
    end
  end

  def maybe_history_projector_lmdb_publish_hook(shard_data_path, file_id, entries) do
    case Application.get_env(:ferricstore, :flow_history_projector_lmdb_publish_hook) do
      fun when is_function(fun, 3) -> fun.(shard_data_path, file_id, entries)
      _other -> :ok
    end
  end

  def lmdb_history_location_ops(file_id, entries, locations) do
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

  def write_lmdb_ops(_shard_data_path, []), do: :ok

  def write_lmdb_ops(shard_data_path, ops) do
    shard_data_path
    |> Ferricstore.Flow.LMDB.path()
    |> Ferricstore.Flow.LMDB.write_batch(ops)
  end

  def maybe_history_expire_put(ops, expire_at_ms, history_index_key) do
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

  def maybe_history_flow_expire_puts(ops, entries) do
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
end
