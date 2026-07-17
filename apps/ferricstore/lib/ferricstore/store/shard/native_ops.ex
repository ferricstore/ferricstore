defmodule Ferricstore.Store.Shard.NativeOps do
  @moduledoc "Shard-level CAS, distributed lock, rate-limit, and list operation handlers with Raft and direct-write paths."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.ErrorReasons
  alias Ferricstore.Raft.ReplyAwaiter

  alias Ferricstore.Store.{
    BlobValue,
    CompoundCommand,
    RateLimit,
    ReadResult,
    TypeRegistry,
    ValueCodec
  }

  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Reads, as: ShardReads

  require Logger

  @record_header_size 26
  @direct_dead_bytes_key {__MODULE__, :direct_dead_bytes}

  # -------------------------------------------------------------------
  # CAS / LOCK / UNLOCK / EXTEND / RATELIMIT / TYPE CLAIM / LIST handlers
  # -------------------------------------------------------------------

  @spec handle_cas(binary(), term(), binary(), non_neg_integer() | nil, map()) ::
          {:reply, term(), map()}
  @doc false
  def handle_cas(key, expected, new_value, expire_at_ms, state) do
    if state.raft? do
      handle_cas_raft(key, expected, new_value, expire_at_ms, state)
    else
      handle_cas_direct(key, expected, new_value, expire_at_ms, state)
    end
  end

  defp handle_cas_raft(key, expected, new_value, expire_at_ms, state) do
    # expire_at_ms is already absolute (converted by Router.cas).
    # Use the forced-quorum path so the result is the state machine's
    # actual reply (1/0/nil). CAS must not use fire-and-forget submission
    # because callers need the state machine's linearizable decision.
    result = forced_quorum_call(state.index, {:cas, key, expected, new_value, expire_at_ms})

    case result do
      r when r in [1, 0, nil] ->
        new_version = if r == 1, do: state.write_version + 1, else: state.write_version
        {:reply, r, %{state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  # Synchronous wrapper around Batcher.write_async_quorum: enqueues into the
  # quorum slot regardless of namespace, then receives the reply for this
  # specific command. The alias-backed waiter drops late replies after timeout
  # so connection/shard mailboxes do not retain stale Raft results.
  defp forced_quorum_call(shard_index, command) do
    {from, token} = ReplyAwaiter.new()

    from =
      case Process.get(:ferricstore_forward_origin) do
        nil -> from
        origin_node -> Ferricstore.Raft.Batcher.remote_origin_from(origin_node, from)
      end

    Ferricstore.Raft.Batcher.write_async_quorum(shard_index, command, from)
    ReplyAwaiter.await(token, 10_000, {:error, "ERR forced-quorum write timeout"})
  end

  @spec handle_type_claim(binary(), atom(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_type_claim(key, type, state)
      when is_binary(key) and type in [:hash, :list, :set, :zset, :stream] do
    if state.raft? do
      result = forced_quorum_call(state.index, CompoundCommand.type_claim(key, type))
      new_state = maybe_advance_type_claim_version(state, result, true)
      {:reply, result, new_state}
    else
      handle_type_claim_direct(key, type, state)
    end
  end

  def handle_type_claim(_key, _type, state),
    do: {:reply, {:error, "ERR invalid compound type claim"}, state}

  defp handle_type_claim_direct(key, type, state) do
    state = ShardFlush.await_in_flight(state)
    state = ShardFlush.flush_pending_sync(state)
    store = key |> build_list_compound_store_direct(state) |> type_check_store(state)

    reset_direct_dead_bytes()

    try do
      result = TypeRegistry.serialized_claim_status(key, type, store)
      mutated? = result == {:ok, :created} or direct_dead_bytes_recorded?()

      new_state =
        state
        |> apply_direct_dead_bytes()
        |> refresh_direct_file_accounting()
        |> maybe_advance_type_claim_version(result, mutated?)

      {:reply, result, new_state}
    after
      reset_direct_dead_bytes()
    end
  end

  defp maybe_advance_type_claim_version(state, {:ok, :created}, true),
    do: %{state | write_version: state.write_version + 1}

  defp maybe_advance_type_claim_version(state, _result, true),
    do: %{state | write_version: state.write_version + 1}

  defp maybe_advance_type_claim_version(state, _result, false), do: state

  defp direct_dead_bytes_recorded? do
    @direct_dead_bytes_key
    |> Process.get(%{})
    |> map_size()
    |> Kernel.>(0)
  end

  defp handle_cas_direct(key, expected, new_value, expire_at_ms, state) do
    case resolve_for_native(state, key) do
      {{:hit, ^expected, old_exp}, state} ->
        expire = expire_at_ms || old_exp

        case persist_direct_value(state, key, new_value, expire) do
          {:ok, new_state} -> {:reply, 1, new_state}
          {:error, reason, rolled_back_state} -> {:reply, {:error, reason}, rolled_back_state}
        end

      {{:hit, _other, _exp}, state} ->
        {:reply, 0, state}

      {:expired, state} ->
        {:reply, nil, state}

      {:missing, state} ->
        {:reply, nil, state}

      {{:error, {:storage_read_failed, _reason}} = failure, state} ->
        {:reply, failure, state}

      {:error, state} ->
        {:reply, {:error, "ERR cold read failed"}, state}
    end
  end

  @spec handle_lock(binary(), binary(), non_neg_integer(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_lock(key, owner, expire_at_ms, state) do
    if state.raft? do
      handle_lock_raft(key, owner, expire_at_ms, state)
    else
      handle_lock_direct(key, owner, expire_at_ms, state)
    end
  end

  defp handle_lock_raft(key, owner, expire_at_ms, state) do
    # expire_at_ms is already absolute (converted by Router.lock)
    result = forced_quorum_call(state.index, {:lock, key, owner, expire_at_ms})

    case result do
      :ok ->
        {:reply, :ok, %{state | write_version: state.write_version + 1}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_lock_direct(key, owner, expire_at_ms, state) do
    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        case persist_direct_value(state, key, owner, expire_at_ms) do
          {:ok, new_state} -> {:reply, :ok, new_state}
          {:error, reason, rolled_back_state} -> {:reply, {:error, reason}, rolled_back_state}
        end

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK lock is held by another owner"}, state}

      {{:error, {:storage_read_failed, _reason}} = failure, state} ->
        {:reply, failure, state}

      {:error, state} ->
        {:reply, {:error, "ERR cold read failed"}, state}

      {_, state} ->
        case persist_direct_value(state, key, owner, expire_at_ms) do
          {:ok, new_state} -> {:reply, :ok, new_state}
          {:error, reason, rolled_back_state} -> {:reply, {:error, reason}, rolled_back_state}
        end
    end
  end

  @spec handle_unlock(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_unlock(key, owner, state) do
    if state.raft? do
      handle_unlock_raft(key, owner, state)
    else
      handle_unlock_direct(key, owner, state)
    end
  end

  defp handle_unlock_raft(key, owner, state) do
    result = forced_quorum_call(state.index, {:unlock, key, owner})

    case result do
      1 ->
        {:reply, 1, %{state | write_version: state.write_version + 1}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_unlock_direct(key, owner, state) do
    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case NIF.v2_append_tombstone(state.active_file_path, key) do
          {:ok, _} ->
            state = ShardFlush.track_delete_dead_bytes(state, key)
            ShardETS.ets_delete_key(state, key)
            {:reply, 1, %{state | write_version: state.write_version + 1}}

          {:error, reason} ->
            Logger.error(
              "Shard #{state.index}: tombstone write failed for UNLOCK: #{inspect(reason)}"
            )

            {:reply, {:error, reason}, state}
        end

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK caller is not the lock owner"}, state}

      {{:error, {:storage_read_failed, _reason}} = failure, state} ->
        {:reply, failure, state}

      {:error, state} ->
        {:reply, {:error, "ERR cold read failed"}, state}

      {_, state} ->
        {:reply, 1, state}
    end
  end

  @spec handle_extend(binary(), binary(), non_neg_integer(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_extend(key, owner, expire_at_ms, state) do
    if state.raft? do
      handle_extend_raft(key, owner, expire_at_ms, state)
    else
      handle_extend_direct(key, owner, expire_at_ms, state)
    end
  end

  defp handle_extend_raft(key, owner, expire_at_ms, state) do
    # expire_at_ms is already absolute (converted by Router.extend)
    result = forced_quorum_call(state.index, {:extend, key, owner, expire_at_ms})

    case result do
      1 ->
        {:reply, 1, %{state | write_version: state.write_version + 1}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_extend_direct(key, owner, expire_at_ms, state) do
    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        case persist_direct_value(state, key, owner, expire_at_ms) do
          {:ok, new_state} -> {:reply, 1, new_state}
          {:error, reason, rolled_back_state} -> {:reply, {:error, reason}, rolled_back_state}
        end

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK caller is not the lock owner"}, state}

      {{:error, {:storage_read_failed, _reason}} = failure, state} ->
        {:reply, failure, state}

      {:error, state} ->
        {:reply, {:error, "ERR cold read failed"}, state}

      {_, state} ->
        {:reply, {:error, "DISTLOCK lock does not exist or has expired"}, state}
    end
  end

  @spec handle_ratelimit_add(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          map()
        ) :: {:reply, term(), map()}
  @doc false
  def handle_ratelimit_add(key, window_ms, max, count, state) do
    if state.raft? do
      handle_ratelimit_add_raft(key, window_ms, max, count, state)
    else
      handle_ratelimit_add_direct(key, window_ms, max, count, state)
    end
  end

  defp handle_ratelimit_add_raft(key, window_ms, max, count, state) do
    # Force-quorum path: callers need ratelimit's state-machine reply, not an
    # enqueue acknowledgement.
    result = forced_quorum_call(state.index, {:ratelimit_add, key, window_ms, max, count})

    case result do
      [_status, _count, _remaining, _ttl] = reply ->
        new_version = state.write_version + 1
        {:reply, reply, %{state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  @spec handle_ratelimit_add_direct(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          map()
        ) :: {:reply, [term()] | {:error, term()}, map()}
  @doc false
  def handle_ratelimit_add_direct(key, window_ms, max, count, state) do
    expiry_context = Ferricstore.ExpiryContext.capture()
    now = Ferricstore.ExpiryContext.now_ms(expiry_context)

    case ShardETS.ets_lookup_warm_result(state, key, expiry_context) do
      {:error, :cold_read_failed} ->
        {:reply, {:error, "ERR cold read failed"}, state}

      {:error, {:storage_read_failed, _reason}} = failure ->
        {:reply, failure, state}

      {:hit, value, expire_at_ms} ->
        value
        |> decode_ratelimit(now)
        |> apply_ratelimit_add_direct(
          key,
          window_ms,
          max,
          count,
          now,
          state,
          {:existing, value, expire_at_ms}
        )

      _missing_or_expired ->
        apply_ratelimit_add_direct(
          {0, now, 0},
          key,
          window_ms,
          max,
          count,
          now,
          state,
          :missing
        )
    end
  end

  defp apply_ratelimit_add_direct(
         {cur_count, cur_start, prv_count},
         key,
         window_ms,
         max,
         count,
         now,
         state,
         original
       ) do
    # Rotate windows
    {cur_count, cur_start, prv_count} =
      cond do
        now - cur_start >= window_ms * 2 -> {0, now, 0}
        now - cur_start >= window_ms -> {0, now, cur_count}
        true -> {cur_count, cur_start, prv_count}
      end

    elapsed = now - cur_start
    effective = RateLimit.effective_count(cur_count, prv_count, elapsed, window_ms)
    expire_at_ms = cur_start + window_ms * 2

    {status, final_count, remaining, value} =
      if effective + count > max do
        value = encode_ratelimit(cur_count, cur_start, prv_count)
        {"denied", effective, max(0, max - effective), value}
      else
        new_cur = cur_count + count
        new_eff = effective + count
        value = encode_ratelimit(new_cur, cur_start, prv_count)
        {"allowed", new_eff, max(0, max - new_eff), value}
      end

    ms_until_reset = max(0, cur_start + window_ms - now)

    reply = [status, final_count, remaining, ms_until_reset]

    if status == "denied" and rate_limit_state_unchanged?(original, value, expire_at_ms) do
      {:reply, reply, state}
    else
      case persist_direct_value(state, key, value, expire_at_ms) do
        {:ok, new_state} ->
          {:reply, reply, new_state}

        {:error, reason, rolled_back_state} ->
          {:reply, {:error, reason}, rolled_back_state}
      end
    end
  end

  defp rate_limit_state_unchanged?(
         {:existing, value, expire_at_ms},
         value,
         expire_at_ms
       ),
       do: true

  defp rate_limit_state_unchanged?(_original, _value, _expire_at_ms), do: false

  defp persist_direct_value(state, key, value, expire_at_ms) do
    previous_entry = :ets.lookup(state.keydir, key)
    previous_pending = state.pending
    previous_pending_count = Map.get(state, :pending_count, length(previous_pending))
    previous_write_version = state.write_version

    ShardETS.ets_insert(state, key, value, expire_at_ms, previous_entry)

    new_state = %{
      state
      | pending: [{key, value, expire_at_ms} | previous_pending],
        pending_count: previous_pending_count + 1,
        write_version: previous_write_version + 1
    }

    if state.flush_in_flight == nil do
      flushed_state = ShardFlush.flush_pending(new_state)

      case Map.get(flushed_state, :last_flush_error) do
        nil ->
          {:ok, flushed_state}

        reason ->
          rolled_back_state =
            rollback_direct_value(
              flushed_state,
              key,
              previous_entry,
              previous_pending,
              previous_pending_count,
              previous_write_version
            )

          {:error, reason, rolled_back_state}
      end
    else
      {:ok, new_state}
    end
  end

  defp rollback_direct_value(
         state,
         key,
         previous_entry,
         previous_pending,
         previous_pending_count,
         previous_write_version
       ) do
    ShardETS.ets_delete_key(state, key)
    restore_direct_entry(state, previous_entry)

    %{
      state
      | pending: previous_pending,
        pending_count: previous_pending_count,
        write_version: previous_write_version
    }
  end

  defp restore_direct_entry(_state, []), do: :ok

  defp restore_direct_entry(
         state,
         [{key, value, expire_at_ms, _lfu, file_id, offset, value_size}]
       ) do
    ShardETS.ets_insert_with_location(
      state,
      key,
      value,
      expire_at_ms,
      file_id,
      offset,
      value_size,
      []
    )
  end

  # -------------------------------------------------------------------
  # List operations
  # -------------------------------------------------------------------

  @spec handle_list_op(binary(), term(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_list_op(key, operation, state) do
    if Ferricstore.Store.ListOps.read_operation?(operation) do
      handle_list_read(key, operation, state)
    else
      if state.raft? do
        handle_list_op_raft(key, operation, state)
      else
        handle_list_op_direct(key, operation, state)
      end
    end
  end

  @spec handle_list_read(binary(), term(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_list_read(key, operation, state) do
    store =
      if state.raft? do
        build_list_compound_store_raft(key, state)
      else
        build_list_compound_store_direct(key, state)
      end

    case ensure_list_type_for_operation(key, operation, type_check_store(store, state)) do
      :ok -> {:reply, Ferricstore.Store.ListOps.execute(key, store, operation), state}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp handle_list_op_raft(key, operation, state) do
    store = build_list_compound_store_raft(key, state)
    type_store = type_check_store(store, state)

    case ensure_list_type_for_operation(key, operation, type_store) do
      :ok ->
        result = Ferricstore.Store.ListOps.execute(key, store, operation)
        new_version = state.write_version + 1
        {:reply, result, %{state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_list_op_direct(key, operation, state) do
    state = ShardFlush.await_in_flight(state)
    state = ShardFlush.flush_pending_sync(state)
    store = build_list_compound_store_direct(key, state)

    reset_direct_dead_bytes()

    try do
      type_store = type_check_store(store, state)

      case ensure_list_type_for_operation(key, operation, type_store) do
        :ok ->
          result = Ferricstore.Store.ListOps.execute(key, store, operation)

          new_state =
            state
            |> apply_direct_dead_bytes()
            |> refresh_direct_file_accounting()

          new_state =
            maybe_advance_list_write_version(
              new_state,
              result,
              direct_list_storage_changed?(state, new_state)
            )

          {:reply, result, new_state}

        {:error, _} = err ->
          {:reply, err, state}
      end
    after
      reset_direct_dead_bytes()
    end
  end

  defp maybe_advance_list_write_version(state, {:error, _reason}, _mutated?), do: state

  defp maybe_advance_list_write_version(state, _successful_result, true),
    do: %{state | write_version: state.write_version + 1}

  defp maybe_advance_list_write_version(state, _successful_result, false), do: state

  defp direct_list_storage_changed?(before, after_state) do
    before.active_file_id != after_state.active_file_id or
      before.active_file_size != after_state.active_file_size
  end

  # Builds a store suitable for TypeRegistry.check_type by adding exists?
  # to the compound store. The compound store lacks exists? which TypeRegistry
  # needs to detect string keys masquerading as lists.
  defp type_check_store(compound_store, state) do
    Map.put(compound_store, :exists?, fn key ->
      # Raw ETS presence is not enough: an unswept expired string must not
      # block a list write with WRONGTYPE.
      case ShardETS.ets_lookup(state, key) do
        {:hit, _value, _exp} -> true
        {:cold, _fid, _off, _vsize, _exp} -> true
        {:error, :invalid_keydir_entry} -> true
        {:error, {:storage_read_failed, _reason}} -> true
        _ -> false
      end
    end)
  end

  defp ensure_list_type_for_operation(key, operation, store)

  defp ensure_list_type_for_operation(key, {:lpush, _elements}, store),
    do: Ferricstore.Store.TypeRegistry.check_or_set(key, :list, store)

  defp ensure_list_type_for_operation(key, {:rpush, _elements}, store),
    do: Ferricstore.Store.TypeRegistry.check_or_set(key, :list, store)

  defp ensure_list_type_for_operation(key, _operation, store),
    do: Ferricstore.Store.TypeRegistry.check_type(key, :list, store)

  @spec handle_list_op_lmove(binary(), binary(), atom(), atom(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_list_op_lmove(src_key, dst_key, from_dir, to_dir, state) do
    if state.raft? do
      handle_list_op_lmove_raft(src_key, dst_key, from_dir, to_dir, state)
    else
      handle_list_op_lmove_direct(src_key, dst_key, from_dir, to_dir, state)
    end
  end

  defp handle_list_op_lmove_raft(src_key, dst_key, from_dir, to_dir, state) do
    result =
      forced_quorum_call(state.index, {:list_op_lmove, src_key, dst_key, from_dir, to_dir})

    case result do
      {:error, _} ->
        {:reply, result, state}

      _ ->
        new_version = state.write_version + 1
        {:reply, result, %{state | write_version: new_version}}
    end
  end

  defp handle_list_op_lmove_direct(src_key, dst_key, from_dir, to_dir, state) do
    state = ShardFlush.await_in_flight(state)
    state = ShardFlush.flush_pending_sync(state)
    store = build_list_compound_store_direct(src_key, state)

    reset_direct_dead_bytes()

    try do
      result =
        checked_lmove(src_key, dst_key, store, type_check_store(store, state), from_dir, to_dir)

      case result do
        {:error, _} ->
          {:reply, result, state}

        _ ->
          new_state =
            state
            |> apply_direct_dead_bytes()
            |> refresh_direct_file_accounting()

          new_state =
            maybe_advance_list_write_version(
              new_state,
              result,
              direct_list_storage_changed?(state, new_state)
            )

          {:reply, result, new_state}
      end
    after
      reset_direct_dead_bytes()
    end
  end

  defp checked_lmove(src_key, dst_key, store, type_store, from_dir, to_dir) do
    with :ok <- Ferricstore.Store.TypeRegistry.check_type(src_key, :list, type_store) do
      case Ferricstore.Store.ListOps.read_meta(src_key, store) do
        nil ->
          nil

        {0, _, _} ->
          nil

        {:error, _reason} = error ->
          error

        _meta ->
          with :ok <- Ferricstore.Store.TypeRegistry.check_or_set(dst_key, :list, type_store) do
            Ferricstore.Store.ListOps.execute_lmove(src_key, dst_key, store, from_dir, to_dir)
          end
      end
    end
  end

  @spec build_list_compound_store_raft(binary(), map()) :: map()
  @doc false
  def build_list_compound_store_raft(_key, state) do
    %{
      compound_get: fn _redis_key, compound_key ->
        do_compound_get(state, compound_key)
      end,
      # In cluster mode the local node may not be the leader for this shard.
      # Batcher.write will reply :not_leader if so. We forward to the leader's
      # Shard via the same forward path Router.quorum_write uses.
      # NOTE: this runs INSIDE the local Shard GenServer (handle_list_op_raft),
      # so we must not re-call our own pid; route only to the LEADER's shard
      # process directly.
      compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
        cluster_safe_compound_write(state, {:put, compound_key, value, expire_at_ms})
      end,
      compound_batch_put: fn
        _redis_key, [] ->
          :ok

        _redis_key, entries ->
          commands =
            Enum.map(entries, fn {compound_key, value, expire_at_ms} ->
              {:put, compound_key, value, expire_at_ms}
            end)

          state
          |> cluster_safe_compound_write({:batch, commands})
          |> normalize_batch_write_result(length(commands))
      end,
      compound_delete: fn _redis_key, compound_key ->
        cluster_safe_compound_write(state, {:delete, compound_key})
      end,
      compound_batch_delete: fn
        _redis_key, [] ->
          :ok

        _redis_key, compound_keys ->
          commands = Enum.map(compound_keys, &{:delete, &1})

          state
          |> cluster_safe_compound_write({:batch, commands})
          |> normalize_batch_write_result(length(commands))
      end,
      compound_batch_mutate: fn _redis_key, compound_keys, entries ->
        commands =
          Enum.map(compound_keys, &{:delete, &1}) ++
            Enum.map(entries, fn {compound_key, value, expire_at_ms} ->
              {:put, compound_key, value, expire_at_ms}
            end)

        state
        |> cluster_safe_compound_write({:batch, commands})
        |> normalize_batch_write_result(length(commands))
      end,
      compound_scan: fn _redis_key, prefix ->
        results = ShardETS.prefix_scan_entries(state, prefix, state.shard_data_path)
        ReadResult.map_success(results, &Enum.sort_by(&1, fn {field, _value} -> field end))
      end,
      compound_scan_slice: fn _redis_key, prefix, start, count, total ->
        ShardETS.prefix_scan_entries_slice(
          state,
          prefix,
          state.shard_data_path,
          start,
          count,
          total
        )
      end
    }
  end

  # Writes from inside a Shard GenServer can't go through Router (would
  # GenServer.call ourselves). Submit via Batcher.write; if rejected as
  # not-leader, do a remote GenServer.call to the leader's shard. The
  # leader's Batcher will tag the reply with the ra_index so we can wait
  # for our own local apply before returning to the caller.
  defp cluster_safe_compound_write(state, command) do
    case Ferricstore.Raft.Batcher.write(state.index, command) do
      {:error, {:not_leader, {_shard_name, leader_node}}} when is_atom(leader_node) ->
        forward_compound_to_leader(state, leader_node, command)

      {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
        forward_compound_to_leader(state, leader_node, command)

      other ->
        other
    end
  end

  defp forward_compound_to_leader(_state, leader_node, _command) when leader_node == node() do
    {:error, "ERR not leader, election in progress"}
  end

  defp forward_compound_to_leader(state, leader_node, command) do
    try do
      remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)
      shard = elem(remote_ctx.shard_names, state.index)

      result =
        :erpc.call(
          leader_node,
          GenServer,
          :call,
          [shard, {:forwarded_quorum, node(), command}, 10_000],
          10_000
        )

      barrier_forwarded_result(state.index, result, 5_000)
    catch
      _, _ -> {:error, "ERR leader unavailable"}
    end
  end

  @doc false
  def __barrier_forwarded_result__(idx, result, timeout_ms \\ 5_000),
    do: barrier_forwarded_result(idx, result, timeout_ms)

  defp barrier_forwarded_result(idx, {:remote_applied_at, ra_index, real_result}, timeout_ms) do
    case Ferricstore.Raft.Batcher.await_local_applied(idx, ra_index, timeout_ms) do
      :ok -> real_result
      {:error, _reason} -> Ferricstore.ErrorReasons.write_timeout_unknown()
    end
  end

  defp barrier_forwarded_result(_idx, other, _timeout_ms), do: other

  @spec build_list_compound_store_direct(binary(), map()) :: map()
  @doc false
  def build_list_compound_store_direct(_key, state) do
    %{
      compound_get: fn _redis_key, compound_key ->
        do_compound_get(state, compound_key)
      end,
      compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
        case persisted_disk_value(state, value) do
          {:ok, persisted_value} ->
            case NIF.v2_append_batch(state.active_file_path, [
                   {compound_key, persisted_value, expire_at_ms}
                 ]) do
              {:ok, [{offset, _value_size}]} ->
                record_direct_dead_bytes(state, compound_key)

                ShardETS.ets_insert_with_location(
                  state,
                  compound_key,
                  value,
                  expire_at_ms,
                  state.active_file_id,
                  offset,
                  byte_size(ShardETS.to_disk_binary(persisted_value))
                )

                :ok

              {:error, reason} ->
                Logger.error(
                  "Shard #{state.index}: append failed for list compound_put: #{inspect(reason)}"
                )

                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end
      end,
      compound_batch_put: fn
        _redis_key, [] ->
          :ok

        _redis_key, entries ->
          case persisted_disk_entries(state, entries) do
            {:ok, persisted_entries} ->
              case NIF.v2_append_batch(state.active_file_path, persisted_entries) do
                {:ok, locations} when length(locations) == length(entries) ->
                  entries
                  |> Enum.zip(persisted_entries)
                  |> Enum.zip(locations)
                  |> Enum.each(fn {{{compound_key, value, expire_at_ms},
                                    {_compound_key, persisted_value, _expire_at_ms}},
                                   {offset, _value_size}} ->
                    record_direct_dead_bytes(state, compound_key)

                    ShardETS.ets_insert_with_location(
                      state,
                      compound_key,
                      value,
                      expire_at_ms,
                      state.active_file_id,
                      offset,
                      byte_size(ShardETS.to_disk_binary(persisted_value))
                    )
                  end)

                  :ok

                {:ok, locations} ->
                  reason = {:location_count_mismatch, length(entries), length(locations)}

                  Logger.error(
                    "Shard #{state.index}: append failed for list compound_batch_put: #{inspect(reason)}"
                  )

                  {:error, reason}

                {:error, reason} ->
                  Logger.error(
                    "Shard #{state.index}: append failed for list compound_batch_put: #{inspect(reason)}"
                  )

                  {:error, reason}
              end

            {:error, reason} ->
              {:error, reason}
          end
      end,
      compound_delete: fn _redis_key, compound_key ->
        case NIF.v2_append_tombstone(state.active_file_path, compound_key) do
          {:ok, _} ->
            record_direct_dead_bytes(state, compound_key)
            ShardETS.ets_delete_key(state, compound_key)
            :ok

          {:error, reason} ->
            Logger.error(
              "Shard #{state.index}: tombstone write failed for list compound_delete: #{inspect(reason)}"
            )

            {:error, reason}
        end
      end,
      compound_batch_delete: fn _redis_key, compound_keys ->
        case append_tombstone_batch_sync(state.active_file_path, compound_keys) do
          {:ok, _locations} ->
            Enum.each(compound_keys, fn compound_key ->
              record_direct_dead_bytes(state, compound_key)
              ShardETS.ets_delete_key(state, compound_key)
            end)

            :ok

          {:error, reason} ->
            Logger.error(
              "Shard #{state.index}: tombstone write failed for list compound_batch_delete: #{inspect(reason)}"
            )

            {:error, reason}
        end
      end,
      compound_batch_mutate: fn _redis_key, compound_keys, entries ->
        compound_batch_mutate_direct(state, compound_keys, entries)
      end,
      compound_scan: fn _redis_key, prefix ->
        results = ShardETS.prefix_scan_entries(state, prefix, state.shard_data_path)
        ReadResult.map_success(results, &Enum.sort_by(&1, fn {field, _value} -> field end))
      end,
      compound_scan_slice: fn _redis_key, prefix, start, count, total ->
        ShardETS.prefix_scan_entries_slice(
          state,
          prefix,
          state.shard_data_path,
          start,
          count,
          total
        )
      end
    }
  end

  defp compound_batch_mutate_direct(_state, [], []), do: :ok

  defp compound_batch_mutate_direct(state, compound_keys, entries) do
    with {:ok, persisted_entries} <- persisted_disk_entries(state, entries) do
      ops =
        Enum.map(compound_keys, &{:delete, &1}) ++
          Enum.map(persisted_entries, fn {compound_key, value, expire_at_ms} ->
            {:put, compound_key, value, expire_at_ms}
          end)

      case NIF.v2_append_ops_batch(state.active_file_path, ops) do
        {:ok, locations} ->
          {delete_locations, put_locations} = Enum.split(locations, length(compound_keys))

          with :ok <- validate_tombstone_locations(delete_locations, length(compound_keys)),
               :ok <- validate_put_locations(put_locations, length(entries)) do
            Enum.each(compound_keys, fn compound_key ->
              record_direct_dead_bytes(state, compound_key)
              ShardETS.ets_delete_key(state, compound_key)
            end)

            entries
            |> Enum.zip(persisted_entries)
            |> Enum.zip(put_locations)
            |> Enum.each(fn {{{compound_key, value, expire_at_ms},
                              {_compound_key, persisted_value, _expire_at_ms}},
                             {:put, offset, _value_size}} ->
              record_direct_dead_bytes(state, compound_key)

              ShardETS.ets_insert_with_location(
                state,
                compound_key,
                value,
                expire_at_ms,
                state.active_file_id,
                offset,
                byte_size(ShardETS.to_disk_binary(persisted_value))
              )
            end)

            :ok
          end

        {:error, reason} ->
          Logger.error(
            "Shard #{state.index}: append failed for list compound_batch_mutate: #{inspect(reason)}"
          )

          {:error, reason}
      end
    end
  end

  defp validate_put_locations(locations, expected_count)
       when length(locations) == expected_count do
    if Enum.all?(locations, fn
         {:put, offset, value_size}
         when is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0 ->
           true

         _other ->
           false
       end) do
      :ok
    else
      {:error, {:put_batch_result_mismatch, expected_count, locations}}
    end
  end

  defp validate_put_locations(locations, expected_count),
    do: {:error, {:put_batch_result_mismatch, expected_count, locations}}

  @doc false
  def __normalize_batch_write_result_for_test__(result, expected_count),
    do: normalize_batch_write_result(result, expected_count)

  defp normalize_batch_write_result({:ok, results}, expected_count)
       when is_list(results) and is_integer(expected_count) and expected_count >= 0 do
    normalize_exact_batch_results(results, expected_count, nil)
  end

  defp normalize_batch_write_result(:ok, 0), do: :ok

  defp normalize_batch_write_result(:ok, expected_count)
       when is_integer(expected_count) and expected_count > 0,
       do: ErrorReasons.write_timeout_unknown()

  defp normalize_batch_write_result({:error, _} = error, _expected_count), do: error
  defp normalize_batch_write_result(other, _expected_count), do: {:error, other}

  defp normalize_exact_batch_results([], 0, nil), do: :ok
  defp normalize_exact_batch_results([], 0, {:error, _} = error), do: error

  defp normalize_exact_batch_results([result | results], remaining, first_error)
       when remaining > 0 do
    first_error =
      case {first_error, result} do
        {nil, {:error, _} = error} -> error
        _ -> first_error
      end

    normalize_exact_batch_results(results, remaining - 1, first_error)
  end

  defp normalize_exact_batch_results(_results, _remaining, _first_error),
    do: ErrorReasons.write_timeout_unknown()

  defp persisted_disk_entries(state, entries) do
    {prepared_reversed, disk_values_reversed} =
      Enum.reduce(entries, {[], []}, fn {compound_key, value, expire_at_ms},
                                        {prepared_acc, disk_acc} ->
        disk_value = ShardETS.to_disk_binary(value)

        {
          [{compound_key, expire_at_ms} | prepared_acc],
          [disk_value | disk_acc]
        }
      end)

    with {:ok, persisted_values} <-
           BlobValue.maybe_externalize_many(
             Map.get(state, :data_dir),
             Map.get(state, :index, 0),
             blob_side_channel_threshold(state),
             Enum.reverse(disk_values_reversed)
           ),
         {:ok, persisted_entries} <-
           attach_persisted_disk_entries(Enum.reverse(prepared_reversed), persisted_values) do
      {:ok, persisted_entries}
    end
  end

  defp attach_persisted_disk_entries(prepared, persisted_values),
    do: attach_persisted_disk_entries(prepared, persisted_values, [])

  defp attach_persisted_disk_entries(
         [{compound_key, expire_at_ms} | prepared],
         [persisted_value | persisted_values],
         acc
       ) do
    attach_persisted_disk_entries(prepared, persisted_values, [
      {compound_key, persisted_value, expire_at_ms} | acc
    ])
  end

  defp attach_persisted_disk_entries([], [], acc), do: {:ok, Enum.reverse(acc)}

  defp attach_persisted_disk_entries(_prepared, _persisted_values, _acc),
    do: {:error, :blob_externalize_result_mismatch}

  defp persisted_disk_value(state, value) do
    disk_value = ShardETS.to_disk_binary(value)

    BlobValue.maybe_externalize(
      Map.get(state, :data_dir),
      Map.get(state, :index, 0),
      blob_side_channel_threshold(state),
      disk_value
    )
  end

  defp blob_side_channel_threshold(%{instance_ctx: ctx}), do: BlobValue.threshold(ctx)
  defp blob_side_channel_threshold(_state), do: 0

  defp append_tombstone_batch_sync(_path, []), do: {:ok, []}

  defp append_tombstone_batch_sync(path, compound_keys) do
    ops = Enum.map(compound_keys, &{:delete, &1})

    case NIF.v2_append_ops_batch(path, ops) do
      {:ok, locations} ->
        with :ok <- validate_tombstone_locations(locations, length(compound_keys)) do
          {:ok, locations}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_tombstone_locations(locations, expected_count)
       when length(locations) == expected_count do
    if Enum.all?(locations, &valid_tombstone_location?/1) do
      :ok
    else
      {:error, {:tombstone_batch_result_mismatch, expected_count, locations}}
    end
  end

  defp validate_tombstone_locations(locations, expected_count),
    do: {:error, {:tombstone_batch_result_mismatch, expected_count, locations}}

  defp valid_tombstone_location?({:delete, offset, record_size})
       when is_integer(offset) and offset >= 0 and is_integer(record_size) and record_size >= 0,
       do: true

  defp valid_tombstone_location?(_location), do: false

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  @spec resolve_for_native(map(), binary()) ::
          {{:hit, term(), non_neg_integer()}, map()}
          | {:expired, map()}
          | {:missing, map()}
          | {ReadResult.failure(), map()}
  @doc false
  def resolve_for_native(state, key) do
    case ShardETS.ets_lookup_warm_result(state, key) do
      {:hit, value, exp} ->
        {{:hit, value, exp}, state}

      :expired ->
        {:expired, state}

      :miss ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case ShardReads.do_get_meta(state, key) do
          nil -> {:missing, state}
          {:error, {:storage_read_failed, _reason}} = failure -> {failure, state}
          {value, exp} -> {{:hit, value, exp}, state}
        end

      {:error, :cold_read_failed} ->
        {:error, state}

      {:error, {:storage_read_failed, _reason}} = failure ->
        {failure, state}
    end
  end

  @spec encode_ratelimit(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: binary()
  @doc false
  def encode_ratelimit(cur, start, prev), do: ValueCodec.encode_ratelimit(cur, start, prev)

  @spec decode_ratelimit(binary(), non_neg_integer()) ::
          {non_neg_integer(), non_neg_integer(), non_neg_integer()}
  @doc false
  def decode_ratelimit(value, fallback_start_ms),
    do: ValueCodec.decode_ratelimit(value, fallback_start_ms)

  # Alias for compound key reads — same logic as do_get since compound keys
  # are stored as regular ETS/Bitcask entries.
  defp do_compound_get(state, compound_key), do: ShardReads.do_get(state, compound_key)

  defp refresh_direct_file_accounting(state) do
    case File.lstat(state.active_file_path) do
      {:ok, %File.Stat{type: :regular, size: current_size}} ->
        written = max(current_size - state.active_file_size, 0)

        {total, dead} =
          Map.get(state.file_stats, state.active_file_id, {state.active_file_size, 0})

        state
        |> Map.put(:active_file_size, current_size)
        |> Map.put(
          :file_stats,
          Map.put(state.file_stats, state.active_file_id, {total + written, dead})
        )
        |> ShardFlush.maybe_rotate_file()

      _invalid_or_missing ->
        state
    end
  end

  defp reset_direct_dead_bytes, do: Process.put(@direct_dead_bytes_key, %{})

  defp record_direct_dead_bytes(state, key) do
    case :ets.lookup(state.keydir, key) do
      [{^key, _value, _exp, _lfu, old_fid, _off, old_vsize}]
      when is_integer(old_fid) and old_fid >= 0 and is_integer(old_vsize) and old_vsize >= 0 ->
        dead_increment = old_vsize + @record_header_size + byte_size(key)

        @direct_dead_bytes_key
        |> Process.get(%{})
        |> Map.update(old_fid, dead_increment, &(&1 + dead_increment))
        |> then(&Process.put(@direct_dead_bytes_key, &1))

      _ ->
        :ok
    end
  end

  defp apply_direct_dead_bytes(state) do
    @direct_dead_bytes_key
    |> Process.get(%{})
    |> Enum.reduce(state, fn {fid, dead_increment}, acc ->
      {total, dead} = Map.get(acc.file_stats, fid, {0, 0})
      %{acc | file_stats: Map.put(acc.file_stats, fid, {total, dead + dead_increment})}
    end)
  end
end
