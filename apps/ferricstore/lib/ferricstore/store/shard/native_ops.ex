defmodule Ferricstore.Store.Shard.NativeOps do
  @moduledoc "Shard-level CAS, distributed lock, rate-limit, and list operation handlers with Raft and direct-write paths."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.ReplyAwaiter
  alias Ferricstore.Store.{ValueCodec}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Reads, as: ShardReads

  require Logger

  @record_header_size 26
  @direct_dead_bytes_key {__MODULE__, :direct_dead_bytes}

  # -------------------------------------------------------------------
  # CAS / LOCK / UNLOCK / EXTEND / RATELIMIT / LIST handlers
  # -------------------------------------------------------------------

  @spec handle_cas(binary(), term(), binary(), non_neg_integer() | nil, map()) ::
          {:reply, term(), map()}
  @doc false
  def handle_cas(key, expected, new_value, ttl_ms, state) do
    if state.raft? do
      handle_cas_raft(key, expected, new_value, ttl_ms, state)
    else
      handle_cas_direct(key, expected, new_value, ttl_ms, state)
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

  defp handle_cas_direct(key, expected, new_value, ttl_ms, state) do
    case resolve_for_native(state, key) do
      {{:hit, ^expected, old_exp}, state} ->
        expire = if ttl_ms, do: Ferricstore.HLC.now_ms() + ttl_ms, else: old_exp
        ShardETS.ets_insert(state, key, new_value, expire)
        new_pending = [{key, new_value, expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, 1, new_state}

      {{:hit, _other, _exp}, state} ->
        {:reply, 0, state}

      {:expired, state} ->
        {:reply, nil, state}

      {:missing, state} ->
        {:reply, nil, state}
    end
  end

  @spec handle_lock(binary(), binary(), non_neg_integer(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_lock(key, owner, ttl_ms, state) do
    if state.raft? do
      handle_lock_raft(key, owner, ttl_ms, state)
    else
      handle_lock_direct(key, owner, ttl_ms, state)
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

  defp handle_lock_direct(key, owner, ttl_ms, state) do
    expire = Ferricstore.HLC.now_ms() + ttl_ms

    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        ShardETS.ets_insert(state, key, owner, expire)
        new_pending = [{key, owner, expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, :ok, new_state}

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK lock is held by another owner"}, state}

      {_, state} ->
        ShardETS.ets_insert(state, key, owner, expire)
        new_pending = [{key, owner, expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, :ok, new_state}
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
        state = ShardFlush.track_delete_dead_bytes(state, key)

        case NIF.v2_append_tombstone(state.active_file_path, key) do
          {:ok, _} ->
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

      {_, state} ->
        {:reply, 1, state}
    end
  end

  @spec handle_extend(binary(), binary(), non_neg_integer(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_extend(key, owner, ttl_ms, state) do
    if state.raft? do
      handle_extend_raft(key, owner, ttl_ms, state)
    else
      handle_extend_direct(key, owner, ttl_ms, state)
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

  defp handle_extend_direct(key, owner, ttl_ms, state) do
    new_expire = Ferricstore.HLC.now_ms() + ttl_ms

    case resolve_for_native(state, key) do
      {{:hit, ^owner, _exp}, state} ->
        ShardETS.ets_insert(state, key, owner, new_expire)
        new_pending = [{key, owner, new_expire} | state.pending]
        new_state = %{state | pending: new_pending, write_version: state.write_version + 1}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, 1, new_state}

      {{:hit, _other, _exp}, state} ->
        {:reply, {:error, "DISTLOCK caller is not the lock owner"}, state}

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
        ) :: {:reply, [term()], map()}
  @doc false
  def handle_ratelimit_add_direct(key, window_ms, max, count, state) do
    now = Ferricstore.HLC.now_ms()

    {cur_count, cur_start, prv_count} =
      case ShardETS.ets_lookup_warm(state, key) do
        {:hit, value, _exp} -> decode_ratelimit(value, now)
        _ -> {0, now, 0}
      end

    # Rotate windows
    {cur_count, cur_start, prv_count} =
      cond do
        now - cur_start >= window_ms * 2 -> {0, now, 0}
        now - cur_start >= window_ms -> {0, now, cur_count}
        true -> {cur_count, cur_start, prv_count}
      end

    # Compute effective count with sliding window approximation
    elapsed = now - cur_start
    weight = max(0.0, 1.0 - elapsed / window_ms)
    effective = cur_count + trunc(Float.round(prv_count * weight))
    expire_at_ms = cur_start + window_ms * 2

    {status, final_count, remaining, state} =
      if effective + count > max do
        value = encode_ratelimit(cur_count, cur_start, prv_count)
        ShardETS.ets_insert(state, key, value, expire_at_ms)
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {"denied", effective, max(0, max - effective), new_state}
      else
        new_cur = cur_count + count
        new_eff = effective + count
        value = encode_ratelimit(new_cur, cur_start, prv_count)
        ShardETS.ets_insert(state, key, value, expire_at_ms)
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {"allowed", new_eff, max(0, max - new_eff), new_state}
      end

    ms_until_reset = max(0, cur_start + window_ms - now)
    {:reply, [status, final_count, remaining, ms_until_reset], state}
  end

  # -------------------------------------------------------------------
  # List operations
  # -------------------------------------------------------------------

  @spec handle_list_op(binary(), term(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_list_op(key, operation, state) do
    if state.raft? do
      handle_list_op_raft(key, operation, state)
    else
      handle_list_op_direct(key, operation, state)
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
          state = state |> apply_direct_dead_bytes() |> refresh_direct_file_accounting()
          {:reply, result, state}

        {:error, _} = err ->
          {:reply, err, state}
      end
    after
      reset_direct_dead_bytes()
    end
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
    store = build_list_compound_store_raft(src_key, state)

    result =
      checked_lmove(src_key, dst_key, store, type_check_store(store, state), from_dir, to_dir)

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
          state = state |> apply_direct_dead_bytes() |> refresh_direct_file_accounting()
          {:reply, result, state}
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
      compound_delete: fn _redis_key, compound_key ->
        cluster_safe_compound_write(state, {:delete, compound_key})
      end,
      compound_scan: fn _redis_key, prefix ->
        results = ShardETS.prefix_scan_entries(state, prefix, state.shard_data_path)
        Enum.sort_by(results, fn {field, _} -> field end)
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
        case NIF.v2_append_batch(state.active_file_path, [{compound_key, value, expire_at_ms}]) do
          {:ok, [{offset, _value_size}]} ->
            record_direct_dead_bytes(state, compound_key)

            ShardETS.ets_insert_with_location(
              state,
              compound_key,
              value,
              expire_at_ms,
              state.active_file_id,
              offset,
              byte_size(value)
            )

            :ok

          {:error, reason} ->
            Logger.error(
              "Shard #{state.index}: append failed for list compound_put: #{inspect(reason)}"
            )

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
      compound_scan: fn _redis_key, prefix ->
        results = ShardETS.prefix_scan_entries(state, prefix, state.shard_data_path)
        Enum.sort_by(results, fn {field, _} -> field end)
      end
    }
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  @spec resolve_for_native(map(), binary()) ::
          {{:hit, term(), non_neg_integer()}, map()} | {:expired, map()} | {:missing, map()}
  @doc false
  def resolve_for_native(state, key) do
    case ShardETS.ets_lookup_warm(state, key) do
      {:hit, value, exp} ->
        {{:hit, value, exp}, state}

      :expired ->
        {:expired, state}

      :miss ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case ShardReads.do_get_meta(state, key) do
          nil -> {:missing, state}
          {value, exp} -> {{:hit, value, exp}, state}
        end
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
    case File.stat(state.active_file_path) do
      {:ok, %{size: current_size}} ->
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

      {:error, _} ->
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
