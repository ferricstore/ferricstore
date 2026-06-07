defmodule Ferricstore.Raft.StateMachine.Sections.FlowValues do
  @moduledoc false

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote do
      import Kernel, except: [apply: 3]
      import Bitwise

      require Logger

      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.CommandTime
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Commands.HyperLogLog
      alias Ferricstore.Commands.Json
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Flow
      alias Ferricstore.Flow.Hibernation
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.RetryPolicy
      alias Ferricstore.HLC

      alias Ferricstore.Store.{
        BitcaskWriter,
        BlobRef,
        BlobStore,
        BlobValue,
        ColdRead,
        CompoundKey,
        ExpiryTracker,
        LFU,
        ListOps,
        Promotion,
        Router,
        ValueCodec
      }

      alias Ferricstore.Store.Shard.ZSetIndex
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

  defp flow_after_history_fast_record?(lmdb_mirror?, record) do
    flow_history_trim_skippable?(record) and
      (not lmdb_mirror? or not Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)))
  end

  defp flow_after_history_put(state, record) do
    with :ok <- flow_history_trim(state, record) do
      maybe_queue_terminal_lmdb_history_indexes(state, record)
    end
  end

  defp flow_history_trim_oldest(state, record, id, partition_key, history_key, delete_count) do
    events = flow_index_rank_range(state, history_key, 0, delete_count - 1, false)

    with :ok <-
           flow_history_delete_oldest_events(
             state,
             record,
             id,
             partition_key,
             history_key,
             events
           ) do
      events
      |> Enum.map(fn {event_id, _event_ms} -> event_id end)
      |> then(&flow_index_delete_members(state, history_key, &1))
    end
  end

  defp flow_history_delete_oldest_events(_state, _record, _id, _partition_key, _history_key, []),
    do: :ok

  defp flow_history_delete_oldest_events(state, record, id, partition_key, history_key, events) do
    Enum.reduce_while(events, :ok, fn {event_id, event_ms}, :ok ->
      compound_key = FlowKeys.stream_entry_key(id, event_id, partition_key)

      if flow_lmdb_projection_enabled?(state) do
        with_lmdb_mirror_shard(state, fn ->
          queue_lmdb_history_index_delete(record, history_key, event_id, trunc(event_ms))
        end)
      end

      case do_delete(state, compound_key) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp maybe_queue_terminal_lmdb_history_indexes(state, record) do
    if flow_lmdb_projection_enabled?(state) and
         Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
      id = Map.fetch!(record, :id)
      partition_key = Map.get(record, :partition_key)
      history_key = FlowKeys.history_key(id, partition_key)

      with_lmdb_mirror_shard(state, fn ->
        queue_lmdb_history_indexes_project_from_index(state, record, history_key)
      end)
    end

    :ok
  end

  defp flow_record_expire_at(%{terminal_retention_until_ms: expire_at_ms})
       when is_integer(expire_at_ms) and expire_at_ms > 0,
       do: expire_at_ms

  defp flow_record_expire_at(_record), do: 0

  defp flow_state_record_expire_at(_record), do: 0

  defp flow_encode(record), do: Flow.encode_record(record)

  defp do_put(state, key, value, expire_at_ms) do
    maybe_clear_compound_data_structure_for_string_put(state, key)

    case maybe_externalize_apply_value(state, value) do
      {:ok, :value, value} ->
        raw_put(state, key, value, expire_at_ms)

      {:ok, :blob_ref, encoded_ref, materialized_value} ->
        raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value)

      {:error, _reason} = error ->
        error
    end
  end

  defp prepare_apply_blob_command(state, command) do
    ctx = blob_apply_ctx(state)

    if BlobCommand.side_channel_candidate?(ctx, command) do
      case BlobCommand.prepare(ctx, state.shard_index, command, single_member?: true) do
        {:ok, prepared_command} -> {:ok, prepared_command}
        {:error, reason} -> {:error, {:blob_externalize_failed, reason}}
      end
    else
      {:ok, command}
    end
  end

  defp blob_apply_ctx(%{instance_ctx: %{data_dir: data_dir} = ctx}) when is_binary(data_dir),
    do: ctx

  defp blob_apply_ctx(%{data_dir: data_dir, blob_side_channel_threshold_bytes: threshold})
       when is_binary(data_dir) do
    %{data_dir: data_dir, blob_side_channel_threshold_bytes: threshold}
  end

  defp blob_apply_ctx(_state), do: %{blob_side_channel_threshold_bytes: 0}

  defp maybe_externalize_apply_value(state, value) when is_binary(value) do
    ctx = blob_apply_ctx(state)
    threshold = BlobValue.threshold(ctx)

    if flow_inline_blob_value?(threshold, value) do
      {:ok, :value, value}
    else
      case BlobValue.maybe_externalize(
             Map.get(ctx, :data_dir),
             state.shard_index,
             threshold,
             value
           ) do
        {:ok, ^value} ->
          {:ok, :value, value}

        {:ok, encoded_ref} ->
          {:ok, :blob_ref, encoded_ref, value}

        {:error, reason} ->
          {:error, {:blob_externalize_failed, reason}}
      end
    end
  end

  defp maybe_externalize_apply_value(_state, value), do: {:ok, :value, value}

  defp maybe_externalize_cross_shard_value(anchor_state, ctx, value) when is_binary(value) do
    instance_ctx = Map.get(ctx, :instance_ctx) || Map.get(anchor_state, :instance_ctx)
    threshold = BlobValue.threshold(instance_ctx)

    if flow_inline_blob_value?(threshold, value) do
      {:ok, value_for_ets(value, hot_cache_threshold(anchor_state)), to_disk_binary(value), value}
    else
      case BlobValue.maybe_externalize(ctx.data_dir, ctx.index, threshold, value) do
        {:ok, ^value} ->
          {:ok, value_for_ets(value, hot_cache_threshold(anchor_state)), to_disk_binary(value),
           value}

        {:ok, encoded_ref} ->
          {:ok, nil, to_disk_binary(encoded_ref), value}

        {:error, reason} ->
          {:error, {:blob_externalize_failed, reason}}
      end
    end
  end

  defp maybe_externalize_cross_shard_value(anchor_state, _ctx, value) do
    {:ok, value_for_ets(value, hot_cache_threshold(anchor_state)), to_disk_binary(value), value}
  end

  defp flow_inline_blob_value?(threshold, value) when is_binary(value) do
    size = byte_size(value)
    threshold <= 0 or (size < threshold and not BlobRef.encoded_size?(size))
  end

  defp flow_put_record_values(state, record, attrs) do
    do_flow_put_record_values(state, record, attrs)
  end

  defp do_flow_put_record_values(state, record, attrs) do
    with :ok <- flow_maybe_put_record_value(state, record, attrs, :payload),
         :ok <- flow_maybe_put_record_value(state, record, attrs, :result),
         :ok <- flow_maybe_put_record_value(state, record, attrs, :error) do
      flow_put_named_record_values(state, record, attrs)
    end
  end

  defp flow_maybe_put_record_value(state, record, attrs, kind) do
    if Map.has_key?(attrs, kind) do
      key = Map.fetch!(record, flow_value_ref_field(kind))
      value = Map.fetch!(attrs, kind)

      case BlobCommand.flow_blob_value_ref(value) do
        {:ok, encoded_ref} ->
          flow_put_record_blob_value(state, record, key, encoded_ref)

        :error ->
          with :ok <- flow_validate_key_size(key) do
            raw_put_cold(
              state,
              key,
              Flow.encode_value(value),
              flow_record_expire_at(record)
            )
          end
      end
    else
      :ok
    end
  end

  defp flow_put_named_record_values(state, record, attrs) do
    values = flow_named_values(Map.get(attrs, :values))

    if map_size(values) == 0 do
      :ok
    else
      refs = flow_record_value_refs(record)

      Enum.reduce_while(values, :ok, fn {name, value}, :ok ->
        case Map.get(refs, name) do
          %{ref: key} when is_binary(key) and key != "" ->
            link_key = flow_shared_value_link_key(record, name, Map.get(refs, name))

            with :ok <- flow_put_named_record_value(state, record, key, value),
                 :ok <- flow_maybe_put_shared_value_link(state, link_key, key, record) do
              {:cont, :ok}
            else
              {:error, _reason} = error -> {:halt, error}
            end

          _missing ->
            {:halt, {:error, "ERR flow value #{name} missing ref"}}
        end
      end)
    end
  end

  defp flow_put_named_record_value(state, record, key, value) do
    case BlobCommand.flow_blob_value_ref(value) do
      {:ok, encoded_ref} ->
        flow_put_record_blob_value(state, record, key, encoded_ref)

      :error ->
        with :ok <- flow_validate_key_size(key) do
          raw_put_cold(
            state,
            key,
            Flow.encode_value(value),
            flow_record_expire_at(record)
          )
        end
    end
  end

  defp flow_shared_value_link_key(record, name, %{version: version})
       when is_binary(name) and is_integer(version) do
    Map.fetch!(record, :id)
    |> FlowKeys.shared_value_link_prefix(Map.get(record, :partition_key))
    |> Kernel.<>(name)
    |> Kernel.<>(":")
    |> Kernel.<>(Integer.to_string(version))
  end

  defp flow_shared_value_link_key(_record, _name, _entry), do: nil

  defp flow_maybe_put_shared_value_link(_state, nil, _ref, _record), do: :ok

  defp flow_maybe_put_shared_value_link(state, link_key, ref, record)
       when is_binary(link_key) and is_binary(ref) do
    with :ok <- flow_validate_key_size(link_key) do
      raw_put_cold(state, link_key, ref, flow_record_expire_at(record))
    end
  end

  defp flow_refresh_terminal_value_expirations(state, record, attrs) do
    flow_refresh_terminal_value_expirations_without_materializing(state, record, attrs)
  end

  defp flow_refresh_terminal_value_expirations_without_materializing(_state, _record, _attrs) do
    # Payload/result/error bytes are separate value/blob records. Terminal state
    # writes must not read those bytes just to refresh TTL: large payloads would
    # turn a metadata transition into a hidden cold-read/materialize path. Newly
    # supplied values are already written above with the terminal record expiry;
    # existing refs keep their original value-retention policy.
    :ok
  end

  defp flow_refresh_record_value_expirations(state, record, attrs) do
    refs =
      [:payload, :result, :error]
      |> Enum.reject(&Map.has_key?(attrs, &1))
      |> Enum.map(fn kind -> Map.get(record, flow_value_ref_field(kind)) end)
      |> Enum.filter(&flow_owned_value_ref?/1)

    values = sm_store_batch_get(state, refs, &sm_file_path/2)
    expire_at_ms = flow_record_expire_at(record)

    refs
    |> Enum.zip(values)
    |> Enum.reduce_while(:ok, fn
      {_key, nil}, :ok ->
        {:cont, :ok}

      {key, value}, :ok when is_binary(value) ->
        with :ok <- flow_validate_key_size(key),
             :ok <- raw_put_cold(state, key, value, expire_at_ms) do
          {:cont, :ok}
        else
          {:error, _reason} = error -> {:halt, error}
        end

      {_key, _value}, :ok ->
        {:cont, :ok}
    end)
  end

  defp flow_create_put_record_values(state, plans) do
    if flow_create_plans_have_record_values?(plans) do
      Enum.reduce_while(plans, :ok, fn {record, attrs}, :ok ->
        case flow_put_record_values(state, record, attrs) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    else
      :ok
    end
  end

  defp flow_many_put_record_values(state, plans) do
    flow_many_put_record_values(state, plans, :unknown)
  end

  defp flow_many_put_record_values(_state, _plans, false), do: :ok

  defp flow_many_put_record_values(state, plans, true) do
    flow_many_put_record_values_nonempty(state, plans)
  end

  defp flow_many_put_record_values(state, plans, :unknown) do
    if flow_many_plans_have_record_values?(plans) do
      flow_many_put_record_values_nonempty(state, plans)
    else
      :ok
    end
  end

  defp flow_many_put_record_values_nonempty(state, plans) do
    Enum.reduce_while(plans, :ok, fn
      {_record, next, attrs}, :ok ->
        case flow_put_record_values(state, next, attrs) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end

      {_record, next, _history_meta, attrs}, :ok ->
        case flow_put_record_values(state, next, attrs) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
    end)
  end

  defp flow_create_plans_have_record_values?(plans) do
    Enum.any?(plans, fn {_record, attrs} -> flow_attrs_have_record_values?(attrs) end)
  end

  defp flow_many_plans_have_record_values?(plans) do
    Enum.any?(plans, fn
      {_record, _next, attrs} -> flow_attrs_have_record_values?(attrs)
      {_record, _next, _history_meta, attrs} -> flow_attrs_have_record_values?(attrs)
    end)
  end

  defp flow_attrs_have_record_values?(attrs) do
    Map.has_key?(attrs, :payload) or Map.has_key?(attrs, :result) or Map.has_key?(attrs, :error) or
      map_size(flow_named_values(Map.get(attrs, :values))) > 0
  end

  defp flow_attrs_record_value_mode(attrs) do
    has_payload? = Map.has_key?(attrs, :payload)
    has_result? = Map.has_key?(attrs, :result)
    has_error? = Map.has_key?(attrs, :error)
    has_named? = map_size(flow_named_values(Map.get(attrs, :values))) > 0

    cond do
      has_payload? and not has_result? and not has_error? and not has_named? -> :payload_only
      has_payload? or has_result? or has_error? or has_named? -> :mixed
      true -> :none
    end
  end

  defp flow_merge_record_value_mode(:mixed, _mode), do: :mixed
  defp flow_merge_record_value_mode(_mode, :mixed), do: :mixed
  defp flow_merge_record_value_mode(:empty, mode), do: mode
  defp flow_merge_record_value_mode(:none, :none), do: :none
  defp flow_merge_record_value_mode(:none, :payload_only), do: :mixed
  defp flow_merge_record_value_mode(:payload_only, :none), do: :mixed
  defp flow_merge_record_value_mode(:none, mode), do: mode
  defp flow_merge_record_value_mode(mode, :none), do: mode
  defp flow_merge_record_value_mode(:payload_only, :payload_only), do: :payload_only

  defp flow_finalize_record_value_mode(:empty), do: :none
  defp flow_finalize_record_value_mode(mode), do: mode

  defp flow_put_state_record(state, key, record) when is_map(record) do
    flow_put_state_record_encoded(
      state,
      key,
      flow_encode(record),
      flow_state_record_expire_at(record),
      record
    )
  end

  defp flow_put_new_state_record(state, key, record) when is_map(record) do
    flow_put_state_record_encoded(
      state,
      key,
      flow_encode(record),
      flow_state_record_expire_at(record),
      record
    )
  end

  defp flow_put_state_record_encoded(state, key, value, expire_at_ms, record) do
    cond do
      flow_lmdb_projection_enabled?(state) ->
        flow_mirror_put_state_record(state, key, value, expire_at_ms, record)
        maybe_queue_lmdb_indexes_for_state_record(state, key, value, expire_at_ms, record)
        maybe_queue_flow_hibernation_candidate(state, key, record, value)

      Ferricstore.Flow.LMDB.mode() == :lagged and
          Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) ->
        with :ok <- flow_put_hot(state, key, value, expire_at_ms) do
          queue_pending_lmdb_projection_dirty()
        end

      true ->
        with :ok <- flow_put_hot(state, key, value, expire_at_ms) do
          maybe_queue_flow_hibernation_candidate(state, key, record, value)
        end
    end
  end

  defp flow_mirror_put_state_record(state, key, value, expire_at_ms, record) do
    if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
      raw_put_cold(state, key, value, expire_at_ms, flow_record_lfu(record, value))
    else
      flow_put_hot(state, key, value, expire_at_ms)
    end
  end

  defp flow_put_hot(state, key, value, expire_at_ms) do
    case maybe_externalize_apply_value(state, value) do
      {:ok, :value, value} ->
        flow_put_hot_value(state, key, value, expire_at_ms)

      {:ok, :blob_ref, encoded_ref, materialized_value} ->
        raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value)

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_put_hot_value(state, key, value, expire_at_ms) do
    disk_val = to_disk_binary(value)

    if cross_shard_pending_active?() do
      cross_shard_raw_put(state, key, value, disk_val, expire_at_ms, LFU.initial())
      maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
      :ok
    else
      record_pending_original(state, key)

      unless standalone_staged_apply?() do
        track_keydir_binary_delta(state, key, value, expire_at_ms)

        safe_ets_insert(
          state.ets,
          {key, value, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_val)}
        )
      end

      queue_pending_put(key, disk_val, expire_at_ms)
      Process.put(:sm_pending_fast_staged_put_batch, true)
      maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)

      :ok
    end
  end

  defp raw_put_cold(state, key, value, expire_at_ms) do
    raw_put_cold(state, key, value, expire_at_ms, flow_cold_lfu(value))
  end

  defp raw_put_cold(state, key, value, expire_at_ms, lfu) do
    case maybe_externalize_apply_value(state, value) do
      {:ok, :value, value} ->
        raw_put_cold_value(state, key, value, expire_at_ms, lfu)

      {:ok, :blob_ref, encoded_ref, materialized_value} ->
        raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value, lfu)

      {:error, _reason} = error ->
        error
    end
  end

  defp raw_put_cold_value(state, key, value, expire_at_ms, lfu) do
    disk_val = to_disk_binary(value)

    if cross_shard_pending_active?() do
      ets_val = nil
      cross_shard_raw_put(state, key, ets_val, disk_val, expire_at_ms, lfu)
    else
      record_pending_original(state, key)

      unless standalone_staged_apply?() do
        track_keydir_binary_delta(state, key, nil, expire_at_ms)

        safe_ets_insert(
          state.ets,
          {key, nil, expire_at_ms, lfu, :pending, 0, byte_size(disk_val)}
        )
      end

      queue_pending_put_cold(key, disk_val, expire_at_ms, lfu)
      Process.put(:sm_pending_fast_staged_put_batch, true)
      :ok
    end
  end

  defp cross_shard_raw_put(state, key, ets_val, disk_val, expire_at_ms, lfu) do
    ctx = cross_shard_pending_ctx(state)
    record_cross_shard_pending_original(ctx, key)

    track_keydir_binary_delta_for_keydir(state, ctx.keydir, ctx.index, key, ets_val, expire_at_ms)

    :ets.insert(
      ctx.keydir,
      {key, ets_val, expire_at_ms, lfu, :pending, 0, byte_size(disk_val)}
    )

    queue_cross_shard_pending_put(ctx, key, disk_val, expire_at_ms, ets_val)
    :ok
  end

  defp cross_shard_pending_ctx(state) do
    %{
      keydir: state.ets,
      index: state.shard_index,
      active_file_path: state.active_file_path,
      active_file_id: state.active_file_id
    }
  end

  defp cross_shard_pending_active? do
    is_list(Process.get(:sm_cross_shard_pending_writes, :undefined))
  end

  defp track_keydir_binary_delta_for_keydir(
         state,
         keydir,
         shard_index,
         key,
         new_value,
         new_expire_at_ms
       ) do
    ref = keydir_binary_ref(state)
    previous = :ets.lookup(keydir, key)

    ExpiryTracker.adjust(
      expiry_instance_ctx(state),
      shard_index,
      ExpiryTracker.entry_expire_at(previous),
      new_expire_at_ms
    )

    if ref do
      new_bytes = binary_byte_size(key) + binary_byte_size(new_value)

      old_bytes =
        case previous do
          [{^key, old_val, _, _, _, _, _}] -> binary_byte_size(key) + binary_byte_size(old_val)
          _ -> 0
        end

      delta = new_bytes - old_bytes
      if delta != 0, do: :atomics.add(ref, shard_index + 1, delta)
    end
  end

  defp flow_record_lfu(%{version: version}, _value) when is_integer(version) do
    {:flow_state_version, version, LFU.initial()}
  end

  defp flow_record_lfu(_record, value), do: flow_cold_lfu(value)

  defp flow_cold_lfu(value) when is_binary(value) do
    if Flow.record_blob?(value) do
      case flow_decode_record_blob(value) do
        {:ok, %{version: version}} when is_integer(version) ->
          {:flow_state_version, version, LFU.initial()}

        _ ->
          LFU.initial()
      end
    else
      LFU.initial()
    end
  end

  defp flow_cold_lfu(_value), do: LFU.initial()

  defp raw_put(state, key, value, expire_at_ms) do
    ets_val = value_for_ets(value, hot_cache_threshold(state))
    disk_val = to_disk_binary(value)

    if cross_shard_pending_active?() do
      cross_shard_raw_put(state, key, ets_val, disk_val, expire_at_ms, LFU.initial())
      maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
      :ok
    else
      record_pending_original(state, key)

      unless standalone_staged_apply?() do
        # Track binary memory: subtract old entry's bytes, add new entry's bytes.
        # This gives MemoryGuard accurate off-heap binary accounting.
        track_keydir_binary_delta(state, key, ets_val, expire_at_ms)

        # Insert into ETS immediately so subsequent read-modify-write commands
        # (INCR, APPEND, etc.) in the same batch see the correct value.
        # The file_id is :pending — flush_pending_writes will update it with
        # the real offset after the batch NIF call.
        :ets.insert(
          state.ets,
          {key, ets_val, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_val)}
        )
      end

      # Accumulate for one storage append, then publish real locations before
      # the replicated apply returns.
      queue_pending_put(key, disk_val, expire_at_ms)
      maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)

      :ok
    end
  end

  defp do_set(state, key, value, expire_at_ms, opts) do
    compound_data_structure? = compound_data_structure_key?(state, key)
    get? = Map.get(opts, :get, false)
    current = set_current_meta(state, key, get?)
    exists? = current != nil or compound_data_structure?

    {old_value, old_expire_at_ms} =
      case current do
        nil -> {nil, expire_at_ms}
        {old_value, old_expire_at_ms} -> {old_value, old_expire_at_ms}
      end

    skip? =
      cond do
        Map.get(opts, :nx, false) and exists? -> true
        Map.get(opts, :xx, false) and not exists? -> true
        true -> false
      end

    cond do
      compound_data_structure? and get? ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

      skip? and get? ->
        old_value

      skip? ->
        nil

      true ->
        effective_expire_at_ms =
          if Map.get(opts, :keepttl, false) and exists? do
            old_expire_at_ms
          else
            expire_at_ms
          end

        do_put(state, key, value, effective_expire_at_ms)
        if get?, do: old_value, else: :ok
    end
  end

  defp set_current_meta(state, key, true), do: do_get_meta(state, key)

  defp set_current_meta(state, key, false) do
    case plain_expire_at_ms(state, key) do
      nil -> nil
      expire_at_ms -> {nil, expire_at_ms}
    end
  end

  defp plain_expire_at_ms(state, key) do
    now = apply_now_ms()

    case :ets.lookup(state.ets, key) do
      [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
        0

      [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
        0

      [{^key, nil, 0, _lfu, fid, off, vsize}]
      when valid_waraft_segment_location(fid, off, vsize) ->
        0

      [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        exp

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when exp > now and valid_cold_location(fid, off, vsize) ->
        exp

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
        exp

      [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
        track_keydir_binary_remove_known(state, key, value)
        :ets.delete(state.ets, key)
        nil

      [] ->
        nil
    end
  end

  defp do_checked_put_blob_ref(state, key, encoded_ref, expire_at_ms) do
    redis_key = CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_put_blob_ref(state, key, encoded_ref, expire_at_ms)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp do_checked_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms) do
    redis_key = CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp do_put_blob_ref(state, key, encoded_ref, expire_at_ms) do
    with {:ok, materialized} <- materialize_blob_ref(state, encoded_ref) do
      maybe_clear_compound_data_structure_for_string_put(state, key)
      raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized)
    end
  end

  defp do_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms) do
    with {:ok, ref} <- decode_blob_ref(encoded_ref),
         :ok <- verify_blob_refs_for_apply(state, [ref]) do
      do_put_blob_ref_ref_only_validated(state, key, encoded_ref, expire_at_ms)
    end
  end

  defp do_checked_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts) do
    redis_key = CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp do_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts) when is_map(opts) do
    compound_data_structure? = compound_data_structure_key?(state, key)
    get? = Map.get(opts, :get, false)
    current = set_current_meta(state, key, get?)
    exists? = current != nil or compound_data_structure?

    {old_value, old_expire_at_ms} =
      case current do
        nil -> {nil, expire_at_ms}
        {old_value, old_expire_at_ms} -> {old_value, old_expire_at_ms}
      end

    skip? =
      cond do
        Map.get(opts, :nx, false) and exists? -> true
        Map.get(opts, :xx, false) and not exists? -> true
        true -> false
      end

    cond do
      compound_data_structure? and get? ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

      skip? and get? ->
        old_value

      skip? ->
        nil

      true ->
        effective_expire_at_ms =
          if Map.get(opts, :keepttl, false) and exists? do
            old_expire_at_ms
          else
            expire_at_ms
          end

        case do_put_blob_ref(state, key, encoded_ref, effective_expire_at_ms) do
          :ok -> if get?, do: old_value, else: :ok
          {:error, _reason} = error -> error
        end
    end
  end

  defp do_getset_blob_ref(state, key, encoded_ref) do
    with :ok <- ensure_string_key(state, key),
         {:ok, materialized} <- materialize_blob_ref(state, encoded_ref) do
      old = do_get(state, key)
      raw_put_blob_ref(state, key, encoded_ref, 0, materialized)
      old
    end
  end

  defp do_append_blob_ref(state, key, encoded_ref) do
    with {:ok, suffix} <- materialize_blob_ref(state, encoded_ref) do
      do_append(state, key, suffix)
    end
  end

  defp do_setrange_blob_ref(state, key, offset, encoded_ref) do
    with {:ok, value} <- materialize_blob_ref(state, encoded_ref) do
      do_setrange(state, key, offset, value)
    end
  end

  defp do_cas_blob_ref(state, key, expected, encoded_ref, expire_at_ms) do
    case ets_lookup(state, key) do
      {:hit, ^expected, old_exp} ->
        with {:ok, materialized} <- materialize_blob_ref(state, encoded_ref) do
          expire = if expire_at_ms, do: expire_at_ms, else: old_exp
          raw_put_blob_ref(state, key, encoded_ref, expire, materialized)
          1
        end

      {:hit, _other, _exp} ->
        0

      :expired ->
        nil

      :miss ->
        nil
    end
  end

  defp do_locked_put_blob_ref(state, key, encoded_ref, expire_at_ms, owner_ref) do
    redis_key = CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, owner_ref) do
      :ok -> do_put_blob_ref(state, key, encoded_ref, expire_at_ms)
      {:error, _reason} = error -> error
    end
  end

  defp do_checked_compound_put_blob_ref(state, compound_key, encoded_ref, expire_at_ms) do
    redis_key = CompoundKey.extract_redis_key(compound_key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_compound_put_blob_ref(state, redis_key, compound_key, encoded_ref, expire_at_ms)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

    end
  end
end
