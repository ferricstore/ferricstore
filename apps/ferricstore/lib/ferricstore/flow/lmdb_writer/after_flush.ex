defmodule Ferricstore.Flow.LMDBWriter.AfterFlush do
  @moduledoc false

  alias Ferricstore.Flow.Hibernation
  alias Ferricstore.Flow.LMDBWriter.ProjectionOps
  alias Ferricstore.Flow.Locator

  def apply_after_flush(
         {:prune_terminal_flow, ets, zset_index, zset_lookup, state_key, state_index_key, id,
          version}
       ) do
    prune_terminal_state_key(ets, state_key, version)

    safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id)

    :ok
  end

  def apply_after_flush(
         {:prune_terminal_flow, ets, zset_index, zset_lookup, flow_index, flow_lookup, state_key,
          state_index_key, id, version}
       ) do
    prune_terminal_state_key(ets, state_key, version)

    safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id)
    safe_flow_index_delete_member(flow_index, flow_lookup, state_index_key, id)

    :ok
  end

  def apply_after_flush(
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

  def apply_after_flush(
         {:prune_terminal_flow_v2, ets, zset_index, zset_lookup, flow_index, flow_lookup,
          state_key, type, terminal_state, partition_key, parent_flow_id, root_flow_id,
          correlation_id, id, version}
       ) do
    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)

    metadata_index_keys =
      ProjectionOps.terminal_project_metadata_index_keys(
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

  def apply_after_flush(
         {:prune_terminal_flow_v3, data_dir, shard_index, ets, zset_index, zset_lookup,
          flow_index, flow_lookup, state_key, type, terminal_state, partition_key, parent_flow_id,
          root_flow_id, correlation_id, id, version}
       ) do
    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)

    metadata_index_keys =
      ProjectionOps.terminal_project_metadata_index_keys(
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

  def apply_after_flush(
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
        ProjectionOps.terminal_project_metadata_index_keys(
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

  def apply_after_flush(
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

  def apply_after_flush({:defer_after_flush, delay_ms, action}) do
    delay_ms = normalize_delay_ms(delay_ms)

    if delay_ms > 0 do
      Process.send_after(self(), {:apply_after_flush, action}, delay_ms)
      :ok
    else
      apply_after_flush(action)
    end
  end

  def apply_after_flush({:delete_flow_tombstone, ets, key}) do
    case :ets.lookup(ets, key) do
      [{^key, nil, 0, :flow_state_deleted, :deleted, 0, 0}] -> :ets.delete(ets, key)
      _ -> :ok
    end
  rescue
    ArgumentError -> :ok
  end

  def apply_after_flush(_action), do: :ok

  def hot_flow_record_from_ets(ets, state_key) do
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

  def decode_raw_flow_record(value) when is_binary(value) do
    {:ok, ProjectionOps.flow_call(:decode_record, [value])}
  rescue
    _ -> :error
  end

  def hibernate_delete_hot_state_key(data_dir, shard_index, ets, state_key, %Locator{} = locator) do
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

  def normalize_delay_ms(delay_ms) when is_integer(delay_ms) and delay_ms >= 0, do: delay_ms
  def normalize_delay_ms(_delay_ms), do: 0

  def normalize_non_negative_integer(value, _default) when is_integer(value) and value >= 0,
    do: value

  def normalize_non_negative_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= 0 -> parsed
      _ -> default
    end
  end

  def normalize_non_negative_integer(_value, default), do: default

  def terminal_hot_ttl_ms do
    :ferricstore
    |> Application.get_env(:flow_terminal_hot_ttl_ms, 0)
    |> normalize_non_negative_integer(0)
  end

  def safe_zset_delete_member(nil, _zset_lookup, _state_index_key, _id), do: :ok
  def safe_zset_delete_member(_zset_index, nil, _state_index_key, _id), do: :ok

  def safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id) do
    Ferricstore.Store.Shard.ZSetIndex.delete_member(
      zset_index,
      zset_lookup,
      state_index_key,
      id
    )
  rescue
    ArgumentError -> :ok
  end

  def safe_flow_index_delete_member(nil, _flow_lookup, _state_index_key, _id), do: :ok
  def safe_flow_index_delete_member(_flow_index, nil, _state_index_key, _id), do: :ok

  def safe_flow_index_delete_member(flow_index, flow_lookup, state_index_key, id) do
    case Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup) do
      nil -> :ok
      native -> Ferricstore.Flow.NativeOrderedIndex.delete_member(native, state_index_key, id)
    end
  rescue
    ArgumentError -> :ok
  end

  def prune_terminal_state_key(ets, state_key, version) do
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

  def prune_terminal_state_key(data_dir, shard_index, ets, state_key, version) do
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

  def delete_apply_projection_cache_for_row(
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

  def delete_apply_projection_cache_for_row(_data_dir, _shard_index, _row), do: :ok
end
