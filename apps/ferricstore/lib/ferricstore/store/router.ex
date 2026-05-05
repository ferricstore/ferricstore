defmodule Ferricstore.Store.Router do
  @moduledoc """
  Routes keys to shard GenServers using the shared `Ferricstore.Store.SlotMap`
  hashing implementation.

  This is a pure module with no process state. It provides two categories of
  functions:

  1. **Routing helpers** -- `shard_for/2` and `shard_name/2` map a key to its
     owning shard index and registered process name respectively. Supports
     Redis hash tags: keys containing `{tag}` are hashed on the tag content,
     allowing related keys to co-locate on the same shard.

  2. **Convenience accessors** -- `get/2`, `put/4`, `delete/2`, `exists?/2`,
     `keys/1`, and `dbsize/1` dispatch to the correct shard GenServer
     transparently.

  All public functions take a `ctx` (`FerricStore.Instance.t()`) as the first
  argument, replacing all persistent_term lookups with instance-local state.
  """

  alias Ferricstore.HLC
  alias Ferricstore.HyperLogLog, as: HLL
  alias Ferricstore.ErrorReasons
  alias Ferricstore.Raft.ReplyAwaiter
  alias Ferricstore.Stats
  alias Ferricstore.Store.{CompoundKey, LFU, ListOps, SlotMap, TypeRegistry, ValueCodec}

  @cold_batch_read_timeout_ms 10_000
  @cold_location_retry_attempts 8
  @cold_location_retry_sleep_ms 1
  @default_async_key_latch_timeout_ms 30_000
  @async_list_rollback_key :ferricstore_async_list_originals

  defguardp valid_cold_file_ref(file_id, value_size)
            when is_integer(file_id) and file_id >= 0 and is_integer(value_size) and
                   value_size >= 0

  defguardp valid_cold_location(file_id, offset, value_size)
            when valid_cold_file_ref(file_id, value_size) and is_integer(offset) and offset >= 0

  defguardp valid_pending_value_size(value_size)
            when is_integer(value_size) and value_size >= 0

  # ---------------------------------------------------------------------------
  # Shard resolution helpers
  # ---------------------------------------------------------------------------

  @spec resolve_shard(FerricStore.Instance.t(), non_neg_integer()) :: atom()
  @doc false
  def resolve_shard(ctx, idx), do: elem(ctx.shard_names, idx)

  @doc false
  def safe_read_call(ctx, idx, request) do
    {:ok, GenServer.call(resolve_shard(ctx, idx), request)}
  catch
    :exit, {:noproc, _} ->
      emit_shard_unavailable(ctx, idx, request, :noproc)
      :unavailable

    :exit, {:timeout, _} ->
      emit_shard_unavailable(ctx, idx, request, :timeout)
      :unavailable
  end

  defp safe_write_call(ctx, idx, request) do
    GenServer.call(resolve_shard(ctx, idx), request)
  catch
    :exit, {:noproc, _} ->
      emit_shard_unavailable(ctx, idx, request, :noproc)
      {:error, "ERR shard not available"}

    :exit, {:timeout, _} ->
      emit_shard_unavailable(ctx, idx, request, :timeout)
      ErrorReasons.write_timeout_unknown()

    :exit, _reason ->
      emit_shard_unavailable(ctx, idx, request, :exit)
      {:error, "ERR shard not available"}
  end

  @doc false
  @spec with_key_latch(FerricStore.Instance.t(), binary(), (-> term())) :: term()
  def with_key_latch(ctx, key, fun) when is_binary(key) and is_function(fun, 0) do
    idx = shard_for(ctx, key)
    with_async_key_latch(ctx, idx, key, fun)
  end

  defp emit_shard_unavailable(ctx, idx, request, reason) do
    :telemetry.execute(
      [:ferricstore, :store, :shard_unavailable],
      %{count: 1},
      %{
        instance: ctx.name,
        shard_index: idx,
        request: request_name(request),
        reason: reason
      }
    )
  end

  defp request_name(request) when is_tuple(request), do: elem(request, 0)
  defp request_name(request), do: request

  @spec resolve_keydir(FerricStore.Instance.t(), non_neg_integer()) :: atom() | reference()
  @doc false
  def resolve_keydir(ctx, idx), do: elem(ctx.keydir_refs, idx)
  @spec effective_shard_count(FerricStore.Instance.t()) :: non_neg_integer()
  @doc false
  def effective_shard_count(ctx), do: ctx.shard_count

  # ---------------------------------------------------------------------------
  # Write-path dispatch: all durable writes go through the quorum/Raft path.
  # ---------------------------------------------------------------------------

  @spec quorum_write(FerricStore.Instance.t(), non_neg_integer(), tuple()) :: term()
  defp quorum_write(ctx, idx, command) do
    result =
      try do
        GenServer.call(elem(ctx.shard_names, idx), command, 10_000)
      catch
        :exit, {:timeout, _} ->
          ErrorReasons.write_timeout_unknown()

        :exit, {:noproc, _} ->
          {:error, "ERR shard not available"}
      end

    case result do
      {:error, {:not_leader, {_shard, leader_node}}} when is_atom(leader_node) ->
        forward_to_leader(ctx, leader_node, idx, command)

      {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
        forward_to_leader(ctx, leader_node, idx, command)

      {:error, _} ->
        result

      _ ->
        size = :counters.info(ctx.write_version).size
        if idx < size, do: :counters.add(ctx.write_version, idx + 1, 1)
        result
    end
  end

  defp forward_to_leader(ctx, leader_node, idx, command) do
    if leader_node == node() do
      {:error, "ERR not leader, election in progress"}
    else
      forward_via_shard_call(ctx, leader_node, idx, command)
    end
  end

  defp forward_via_shard_call(ctx, leader_node, idx, command) do
    try do
      remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)

      result =
        :erpc.call(
          leader_node,
          GenServer,
          :call,
          [elem(remote_ctx.shard_names, idx), {:forwarded_quorum, node(), command}, 10_000],
          10_000
        )

      case result do
        # Leader's batcher detected a cross-node caller and tagged the reply
        # with the ra_index. Barrier on local apply so reads on this node
        # see the just-written value (read-your-write across redirects).
        {:remote_applied_at, _ra_index, _real_result} ->
          barrier_forwarded_result(idx, result, 5_000)

        other ->
          barrier_forwarded_result(idx, other, 5_000)
      end
    catch
      _, reason ->
        if forward_timeout?(reason) do
          ErrorReasons.write_timeout_unknown()
        else
          try do
            GenServer.call(elem(ctx.shard_names, idx), command, 10_000)
          catch
            _, _ -> {:error, "ERR leader unavailable"}
          end
        end
    end
  end

  @doc false
  def __barrier_forwarded_result__(idx, result, timeout_ms \\ 5_000),
    do: barrier_forwarded_result(idx, result, timeout_ms)

  @doc false
  @spec __forward_batch_failure_results__(term(), non_neg_integer()) :: [
          {:error, binary() | {:timeout, :unknown_outcome}}
        ]
  def __forward_batch_failure_results__(reason, count) when is_integer(count) and count >= 0 do
    List.duplicate(forward_failure_result(reason), count)
  end

  @spec forward_failure_result(term()) :: {:error, binary() | {:timeout, :unknown_outcome}}
  defp forward_failure_result(reason) do
    if forward_timeout?(reason) do
      ErrorReasons.write_timeout_unknown()
    else
      {:error, "ERR leader unavailable"}
    end
  end

  defp forward_timeout?({:erpc, :timeout}), do: true
  defp forward_timeout?(:timeout), do: true
  defp forward_timeout?({:timeout, _}), do: true
  defp forward_timeout?(_reason), do: false

  defp barrier_forwarded_result(idx, {:remote_applied_at, ra_index, real_result}, timeout_ms) do
    case Ferricstore.Raft.Batcher.await_local_applied(idx, ra_index, timeout_ms) do
      :ok -> real_result
      {:error, _reason} -> ErrorReasons.write_timeout_unknown()
    end
  end

  defp barrier_forwarded_result(_idx, other, _timeout_ms), do: other

  # Dispatches default-instance writes through quorum. Internal async
  # IO/WAL/read machinery remains separate from client acknowledgement policy.
  @spec raft_write(FerricStore.Instance.t(), non_neg_integer(), binary(), tuple()) :: term()
  defp raft_write(ctx, idx, _key, command) do
    if ctx.name == :default do
      quorum_write(ctx, idx, command)
    else
      # Custom embedded instances are local/direct. The default application
      # instance owns Raft; there is no public switch that can disable it.
      GenServer.call(elem(ctx.shard_names, idx), command)
    end
  end

  defp shard_under_disk_pressure?(ctx, idx) do
    size = :atomics.info(ctx.disk_pressure).size
    idx < size and :atomics.get(ctx.disk_pressure, idx + 1) == 1
  end

  defp forced_quorum_write(ctx, idx, command, origin_node) do
    {from, token} = ReplyAwaiter.new()

    if origin_node == node() do
      Ferricstore.Raft.Batcher.write_async_quorum(idx, command, from)
    else
      Ferricstore.Raft.Batcher.write_async_quorum_forwarded(idx, command, from, origin_node)
    end

    result = ReplyAwaiter.await(token, 10_000, ErrorReasons.write_timeout_unknown())

    case result do
      # Local node isn't the leader for this shard. Forward via the same
      # path quorum_write uses so callers get a real result, not the
      # internal :not_leader error.
      {:error, {:not_leader, {_shard, leader_node}}} when is_atom(leader_node) ->
        forward_to_leader(ctx, leader_node, idx, command)

      {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
        forward_to_leader(ctx, leader_node, idx, command)

      {:error, _} ->
        result

      _ ->
        bump_write_version(ctx, idx)
        result
    end
  end

  defp bump_write_version(%{write_version: write_version}, idx) do
    size = :counters.info(write_version).size
    if idx < size, do: :counters.add(write_version, idx + 1, 1)
    :ok
  end

  defp bump_write_version(_ctx, _idx), do: :ok

  @doc """
  Executes an RMW command inline against local ETS + BitcaskWriter and submits
  the delta to Raft via the ordered async submitter.

  **Called with the per-key latch held.** The latch guarantees exclusive
  access to `key`'s ETS row among local direct RMW paths. Namespace async
  durability and its coordinator fallback are retired; default-instance RMW
  goes through the Raft state machine.

  Returns the command's natural result shape (e.g. `{:ok, new_int}` for
  INCR, `old_value_or_nil` for GETSET/GETDEL).
  """
  @spec execute_rmw_inline(FerricStore.Instance.t(), non_neg_integer(), tuple()) :: term()
  def execute_rmw_inline(ctx, idx, cmd) do
    size = :atomics.info(ctx.disk_pressure).size
    under_pressure = idx < size and :atomics.get(ctx.disk_pressure, idx + 1) == 1

    if under_pressure do
      {:error, "ERR disk pressure on shard #{idx}, rejecting write"}
    else
      with :ok <- ensure_string_rmw_key(ctx, idx, cmd) do
        do_rmw_inline(ctx, idx, cmd)
      end
    end
  end

  defp ensure_string_rmw_key(ctx, idx, {_op, key, _arg}) when is_binary(key) do
    ensure_string_rmw_key_name(ctx, idx, key)
  end

  defp ensure_string_rmw_key(ctx, idx, {_op, key}) when is_binary(key) do
    ensure_string_rmw_key_name(ctx, idx, key)
  end

  defp ensure_string_rmw_key(ctx, idx, {_op, key, _arg1, _arg2}) when is_binary(key) do
    ensure_string_rmw_key_name(ctx, idx, key)
  end

  defp ensure_string_rmw_key(_ctx, _idx, _cmd), do: :ok

  defp ensure_string_rmw_key_name(ctx, idx, key) do
    if compound_marker_present?(ctx, idx, key) do
      case TypeRegistry.get_type(key, ctx) do
        type when type in ["none", "string"] ->
          :ok

        _compound_type ->
          {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
      end
    else
      :ok
    end
  end

  defp compound_marker_present?(ctx, idx, key) do
    keydir = elem(ctx.keydir_refs, idx)

    :ets.lookup(keydir, CompoundKey.type_key(key)) != []
  end

  # Per-command RMW implementations. Mirror state_machine.ex do_incr et al.,
  # but operate on origin-local ETS + cast BitcaskWriter + submit a DELTA
  # command to the Batcher for replication.

  defp do_rmw_inline(ctx, idx, {:incr, key, delta}) do
    case read_live(ctx, idx, key) do
      :missing ->
        if delta > 9_223_372_036_854_775_807 or delta < -9_223_372_036_854_775_808 do
          {:error, "ERR increment or decrement would overflow"}
        else
          install_rmw_and_submit(
            ctx,
            idx,
            key,
            delta,
            0,
            {:incr, key, delta},
            nil,
            0,
            {:ok, delta}
          )
        end

      {:hit, value, expire_at_ms} ->
        case coerce_integer(value) do
          {:ok, int_val} ->
            new_val = int_val + delta

            if new_val > 9_223_372_036_854_775_807 or new_val < -9_223_372_036_854_775_808 do
              {:error, "ERR increment or decrement would overflow"}
            else
              install_rmw_and_submit(
                ctx,
                idx,
                key,
                new_val,
                expire_at_ms,
                {:incr, key, delta},
                value,
                expire_at_ms,
                {:ok, new_val}
              )
            end

          :error ->
            {:error, "ERR value is not an integer or out of range"}
        end
    end
  end

  defp do_rmw_inline(ctx, idx, {:incr_float, key, delta}) do
    case read_live(ctx, idx, key) do
      :missing ->
        new_val = delta * 1.0

        install_rmw_and_submit(
          ctx,
          idx,
          key,
          new_val,
          0,
          {:incr_float, key, delta},
          nil,
          0,
          {:ok, new_val}
        )

      {:hit, value, expire_at_ms} ->
        case coerce_float(value) do
          {:ok, float_val} ->
            new_val = float_val + delta

            install_rmw_and_submit(
              ctx,
              idx,
              key,
              new_val,
              expire_at_ms,
              {:incr_float, key, delta},
              value,
              expire_at_ms,
              {:ok, new_val}
            )

          :error ->
            {:error, "ERR value is not a valid float"}
        end
    end
  end

  defp do_rmw_inline(ctx, idx, {:append, key, suffix}) do
    {old_val, before_value, expire_at_ms} =
      case read_live(ctx, idx, key) do
        :missing -> {"", nil, 0}
        {:hit, v, exp} -> {to_disk_binary(v), v, exp}
      end

    new_val = old_val <> suffix

    install_rmw_and_submit(
      ctx,
      idx,
      key,
      new_val,
      expire_at_ms,
      {:append, key, suffix},
      before_value,
      expire_at_ms,
      {:ok, byte_size(new_val)}
    )
  end

  defp do_rmw_inline(ctx, idx, {:getset, key, new_value}) do
    {old, old_expire_at_ms} =
      case read_live(ctx, idx, key) do
        :missing -> {nil, 0}
        {:hit, v, exp} -> {v, exp}
      end

    install_rmw_and_submit(
      ctx,
      idx,
      key,
      new_value,
      0,
      {:getset, key, new_value},
      old,
      old_expire_at_ms,
      old
    )
  end

  defp do_rmw_inline(ctx, idx, {:getdel, key}) do
    case read_live(ctx, idx, key) do
      :missing ->
        nil

      {:hit, v, exp} ->
        previous = {:value, v, exp}
        raft_cmd = origin_checked_command(key, {:getdel, key}, previous, nil, 0)

        with :ok <- async_submit_to_raft(idx, raft_cmd) do
          keydir = elem(ctx.keydir_refs, idx)
          flush_pending_writer_for_key(ctx, idx, keydir, key)
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)

          wv_size = :counters.info(ctx.write_version).size
          if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)

          v
        end
    end
  end

  defp do_rmw_inline(ctx, idx, {:getex, key, new_expire_at_ms}) do
    case read_live(ctx, idx, key) do
      :missing ->
        nil

      {:hit, v, old_expire_at_ms} ->
        install_rmw_and_submit(
          ctx,
          idx,
          key,
          v,
          new_expire_at_ms,
          {:getex, key, new_expire_at_ms},
          v,
          old_expire_at_ms,
          v
        )
    end
  end

  defp do_rmw_inline(ctx, idx, {:setrange, key, offset, value}) do
    {old_val, before_value, expire_at_ms} =
      case read_live(ctx, idx, key) do
        :missing -> {"", nil, 0}
        {:hit, v, exp} -> {to_disk_binary(v), v, exp}
      end

    new_val = apply_setrange(old_val, offset, value)

    install_rmw_and_submit(
      ctx,
      idx,
      key,
      new_val,
      expire_at_ms,
      {:setrange, key, offset, value},
      before_value,
      expire_at_ms,
      {:ok, byte_size(new_val)}
    )
  end

  defp install_rmw_and_submit(
         ctx,
         idx,
         key,
         value,
         expire_at_ms,
         raft_cmd,
         before_value,
         before_expire_at_ms,
         success
       ) do
    checked_cmd =
      {:origin_checked, key, raft_cmd, origin_check_value(before_value), before_expire_at_ms,
       origin_check_value(value), expire_at_ms}

    if large_value_for_hot_cache?(ctx, value) do
      install_large_rmw_and_submit(ctx, idx, key, value, expire_at_ms, checked_cmd, success)
    else
      with :ok <- async_enqueue_to_raft(idx, checked_cmd),
           :ok <- install_rmw_value(ctx, idx, key, value, expire_at_ms) do
        success
      end
    end
  end

  defp install_large_rmw_and_submit(ctx, idx, key, value, expire_at_ms, checked_cmd, success) do
    keydir = elem(ctx.keydir_refs, idx)
    previous = snapshot_live_value(ctx, idx, key)
    disk_value = to_disk_binary(value)

    case nif_append_batch_with_file(ctx, idx, [{key, disk_value, expire_at_ms}]) do
      {:ok, file_id, [{offset, _record_size}]} ->
        case async_submit_to_raft(idx, checked_cmd) do
          :ok ->
            track_keydir_binary_insert(ctx, idx, keydir, key, nil)

            :ets.insert(
              keydir,
              {key, nil, expire_at_ms, LFU.initial(), file_id, offset, byte_size(disk_value)}
            )

            wv_size = :counters.info(ctx.write_version).size
            if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)
            success

          {:error, _} = err ->
            rollback_unaccepted_large_put(ctx, idx, key, previous)
            err
        end

      {:error, reason} ->
        {:error, "ERR disk write failed: #{inspect(reason)}"}
    end
  end

  defp large_value_for_hot_cache?(ctx, value) when is_binary(value),
    do: byte_size(value) > ctx.hot_cache_max_value_size

  defp large_value_for_hot_cache?(_ctx, _value), do: false

  defp origin_check_value(value) when is_integer(value), do: Integer.to_string(value)
  defp origin_check_value(value) when is_float(value), do: Float.to_string(value)
  defp origin_check_value(value), do: value

  # Read the live value for a key (treating expired TTL as missing).
  defp read_live(ctx, idx, key) do
    keydir = elem(ctx.keydir_refs, idx)
    now = HLC.now_ms()

    case :ets.lookup(keydir, key) do
      [{^key, value, exp, _, _, _, _}]
      when value != nil and (exp == 0 or exp > now) ->
        {:hit, value, exp}

      [{^key, nil, exp, _, file_id, offset, value_size}]
      when (exp == 0 or exp > now) and valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)
        original_location = {file_id, offset, value_size}

        case read_cold_async(path, offset, key) do
          {:ok, value} when is_binary(value) ->
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            {:hit, value, exp}

          _ ->
            case retry_changed_cold_meta(ctx, idx, keydir, key, original_location, now) do
              {:hot, value, retry_expire_at_ms} ->
                {:hit, value, retry_expire_at_ms}

              {:cold, value, retry_expire_at_ms, retry_file_id, retry_offset} ->
                warm_ets_after_cold_read(
                  ctx,
                  idx,
                  keydir,
                  key,
                  value,
                  retry_file_id,
                  retry_offset
                )

                {:hit, value, retry_expire_at_ms}

              :miss ->
                :missing
            end
        end

      [{^key, _value, _exp, _, _, _, _}] ->
        track_keydir_binary_delete(ctx, idx, keydir, key)
        :ets.delete(keydir, key)
        :missing

      _ ->
        :missing
    end
  end

  # Write the new RMW value into ETS, cast BitcaskWriter for disk, bump
  # write_version. Matches the direct local-update shape used by the retired
  # async SET path.
  defp install_rmw_value(ctx, idx, key, value, expire_at_ms) do
    keydir = elem(ctx.keydir_refs, idx)

    value_for_ets =
      case value do
        v when is_integer(v) ->
          Integer.to_string(v)

        v when is_float(v) ->
          Float.to_string(v)

        v when is_binary(v) ->
          if byte_size(v) > ctx.hot_cache_max_value_size, do: nil, else: v
      end

    disk_value = to_disk_binary(value)

    result =
      if value_for_ets == nil do
        # Large — sync NIF write then ETS with real offset.
        case nif_append_batch_with_file(ctx, idx, [{key, disk_value, expire_at_ms}]) do
          {:ok, file_id, [{offset, _record_size}]} ->
            track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)

            :ets.insert(
              keydir,
              {key, nil, expire_at_ms, LFU.initial(), file_id, offset, byte_size(disk_value)}
            )

            :ok

          {:error, reason} ->
            {:error, "ERR disk write failed: #{inspect(reason)}"}
        end
      else
        {file_id, file_path, _} = Ferricstore.Store.ActiveFile.get(ctx, idx)
        track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)
        :ets.insert(keydir, {key, value_for_ets, expire_at_ms, LFU.initial(), :pending, 0, 0})

        Ferricstore.Store.BitcaskWriter.write(
          ctx,
          idx,
          file_path,
          file_id,
          keydir,
          key,
          disk_value,
          expire_at_ms
        )

        :ok
      end

    if result == :ok do
      wv_size = :counters.info(ctx.write_version).size
      if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)
      :ok
    else
      result
    end
  end

  defp flush_pending_writer_for_key(ctx, idx, keydir, key) do
    case :ets.lookup(keydir, key) do
      [{^key, _value, _expire_at_ms, _lfu, :pending, _offset, _value_size}] ->
        Ferricstore.Store.BitcaskWriter.flush(ctx, idx)

      _ ->
        :ok
    end
  end

  # Coerce a stored value (integer, float, binary-digits) to an integer.
  defp coerce_integer(v) when is_integer(v), do: {:ok, v}
  defp coerce_integer(v) when is_float(v), do: :error

  defp coerce_integer(v) when is_binary(v) do
    case Integer.parse(v) do
      {n, ""} -> {:ok, n}
      _ -> :error
    end
  end

  # Coerce a stored value to a float (ints upcast).
  defp coerce_float(v) when is_float(v), do: {:ok, v}
  defp coerce_float(v) when is_integer(v), do: {:ok, v * 1.0}

  defp coerce_float(v) when is_binary(v) do
    ValueCodec.parse_float(v)
  end

  # SETRANGE helper: overwrite bytes at offset, zero-padding if needed.
  defp apply_setrange(old, offset, value) do
    old_len = byte_size(old)
    val_len = byte_size(value)

    cond do
      val_len == 0 ->
        if offset > old_len,
          do: old <> :binary.copy(<<0>>, offset - old_len),
          else: old

      offset > old_len ->
        old <> :binary.copy(<<0>>, offset - old_len) <> value

      offset + val_len >= old_len ->
        binary_part(old, 0, offset) <> value

      true ->
        binary_part(old, 0, offset) <>
          value <>
          binary_part(old, offset + val_len, old_len - offset - val_len)
    end
  end

  defp snapshot_live_value(ctx, idx, key) do
    case read_live(ctx, idx, key) do
      {:hit, value, expire_at_ms} -> {:value, value, expire_at_ms}
      :missing -> :missing
    end
  end

  defp origin_checked_command(key, inner_cmd, previous, expected_value, expire_at_ms) do
    {before_value, before_expire_at_ms} =
      case previous do
        {:value, value, exp} -> {origin_check_value(value), exp}
        :missing -> {nil, 0}
      end

    {:origin_checked, key, inner_cmd, before_value, before_expire_at_ms,
     origin_check_value(expected_value), expire_at_ms}
  end

  defp rollback_unaccepted_large_put(ctx, idx, key, {:value, previous_value, expire_at_ms}) do
    disk_value = to_disk_binary(previous_value)
    _ = nif_append_batch_with_file(ctx, idx, [{key, disk_value, expire_at_ms}])
    :ok
  end

  defp rollback_unaccepted_large_put(ctx, idx, key, :missing) do
    _ = append_delete_tombstone_nosync(ctx, idx, key)
    :ok
  end

  defp with_async_key_latch(ctx, idx, key, fun) do
    case acquire_async_key_latches(ctx, [{idx, key}]) do
      {:ok, [{latch_tab, ^key}]} ->
        try do
          fun.()
        after
          release_async_key_latches([{latch_tab, key}])
        end

      {:error, {:timeout, wait_ms}} ->
        async_key_latch_timeout_error(wait_ms)
    end
  end

  defp acquire_async_key_latches(ctx, locks) do
    result =
      locks
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.reduce_while([], fn {idx, key}, held ->
        latch_tab = elem(ctx.latch_refs, idx)

        case wait_for_async_key_latch(latch_tab, idx, key) do
          :ok ->
            {:cont, [{latch_tab, key} | held]}

          {:error, reason} ->
            release_async_key_latches(held)
            {:halt, {:error, reason}}
        end
      end)

    case result do
      {:error, _reason} = error -> error
      held -> {:ok, Enum.reverse(held)}
    end
  end

  defp release_async_key_latches(held_latches) do
    Enum.each(held_latches, fn {latch_tab, key} ->
      :ets.select_delete(latch_tab, [{{key, self()}, [], [true]}])
    end)
  end

  defp wait_for_async_key_latch(latch_tab, idx, key) do
    case :ets.insert_new(latch_tab, {key, self()}) do
      true ->
        :ok

      false ->
        emit_async_key_latch_event(:blocked, idx, key, 0)

        wait_for_async_key_latch(
          latch_tab,
          idx,
          key,
          System.monotonic_time(:millisecond),
          async_key_latch_timeout_ms()
        )
    end
  end

  defp wait_for_async_key_latch(latch_tab, idx, key, started_ms, timeout_ms) do
    case :ets.insert_new(latch_tab, {key, self()}) do
      true ->
        :ok

      false ->
        wait_ms = max(System.monotonic_time(:millisecond) - started_ms, 0)

        if wait_ms >= timeout_ms do
          emit_async_key_latch_event(:timeout, idx, key, wait_ms)
          {:error, {:timeout, wait_ms}}
        else
          wait_for_async_key_latch_holder(latch_tab, idx, key, started_ms, timeout_ms)
        end
    end
  end

  defp wait_for_async_key_latch_holder(latch_tab, idx, key, started_ms, timeout_ms) do
    case :ets.lookup(latch_tab, key) do
      [{^key, holder}] when is_pid(holder) ->
        if Process.alive?(holder) do
          latch_retry_backoff()
        else
          :ets.select_delete(latch_tab, [{{key, holder}, [], [true]}])
        end

      _ ->
        latch_retry_backoff()
    end

    wait_for_async_key_latch(latch_tab, idx, key, started_ms, timeout_ms)
  end

  defp async_key_latch_timeout_ms do
    Application.get_env(
      :ferricstore,
      :router_async_key_latch_timeout_ms,
      @default_async_key_latch_timeout_ms
    )
  end

  defp async_key_latch_timeout_error(wait_ms) do
    {:error, async_key_latch_timeout_error_message(wait_ms)}
  end

  defp async_key_latch_timeout_error_message(wait_ms),
    do: "ERR write key latch timeout after #{wait_ms}ms"

  defp emit_async_key_latch_event(status, idx, key, wait_ms) do
    :telemetry.execute(
      [:ferricstore, :store, :async_key_latch],
      %{count: 1, wait_ms: wait_ms},
      %{status: status, shard_index: idx, redis_key_hash: :erlang.phash2(key)}
    )
  end

  defp latch_retry_backoff do
    receive do
    after
      1 -> :ok
    end
  end

  defp to_disk_binary(v) when is_integer(v), do: Integer.to_string(v)
  defp to_disk_binary(v) when is_float(v), do: Float.to_string(v)
  defp to_disk_binary(v) when is_binary(v), do: v

  defp clear_compound_data_structure_for_string_put(ctx, idx, keydir, key) do
    if CompoundKey.internal_key?(key) do
      :ok
    else
      case compound_marker_for_string_put(ctx, idx, keydir, key) do
        :none ->
          :ok

        {:type, type_key, type} ->
          {_, file_path, _} = Ferricstore.Store.ActiveFile.get(ctx, idx)
          clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, type)
          delete_local_key(ctx, idx, keydir, file_path, type_key)
      end
    end
  end

  defp compound_marker_for_string_put(ctx, idx, keydir, key) do
    type_key = CompoundKey.type_key(key)

    case live_compound_marker(ctx, idx, keydir, type_key) do
      {:ok, type} ->
        {:type, type_key, type}

      :none ->
        :none
    end
  end

  # Keep ordinary string SET on the cheap path: a missing marker needs only
  # direct ETS lookup, while a present marker still uses origin_compound_get/4
  # so stale/expired/cold markers are handled correctly before clearing.
  defp live_compound_marker(ctx, idx, keydir, marker_key) do
    case :ets.lookup(keydir, marker_key) do
      [] ->
        :none

      _ ->
        case origin_compound_get(ctx, idx, keydir, marker_key) do
          nil -> :none
          marker -> {:ok, marker}
        end
    end
  end

  defp clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, "hash"),
    do: delete_local_prefix(ctx, idx, keydir, file_path, CompoundKey.hash_prefix(key))

  defp clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, "list") do
    delete_local_prefix(ctx, idx, keydir, file_path, CompoundKey.list_prefix(key))
    delete_local_key(ctx, idx, keydir, file_path, CompoundKey.list_meta_key(key))
  end

  defp clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, "set"),
    do: delete_local_prefix(ctx, idx, keydir, file_path, CompoundKey.set_prefix(key))

  defp clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, "zset"),
    do: delete_local_prefix(ctx, idx, keydir, file_path, CompoundKey.zset_prefix(key))

  defp clear_compound_prefix_for_string_put(_ctx, _idx, _keydir, _file_path, _key, _type),
    do: :ok

  defp delete_local_prefix(ctx, idx, keydir, file_path, prefix) do
    keydir
    |> Ferricstore.Store.Shard.ETS.prefix_collect_keys(prefix)
    |> Enum.each(fn compound_key ->
      delete_local_key(ctx, idx, keydir, file_path, compound_key)
    end)
  end

  defp delete_local_key(ctx, idx, keydir, file_path, key) do
    track_keydir_binary_delete(ctx, idx, keydir, key)
    :ets.delete(keydir, key)
    Ferricstore.Store.BitcaskWriter.delete(ctx, idx, file_path, key)
  end

  # NIF batch write with retry on stale active file (ENOENT after rotation).
  # Returns {:ok, file_id, locations} or {:error, reason}.
  defp nif_append_batch_with_file(ctx, idx, batch) do
    {file_id, file_path, _} = Ferricstore.Store.ActiveFile.get(ctx, idx)

    case Ferricstore.Bitcask.NIF.v2_append_batch_nosync(file_path, batch) do
      {:ok, locations} ->
        mark_checkpoint_dirty(ctx, idx)
        {:ok, file_id, locations}

      {:error, reason} when is_binary(reason) ->
        if String.contains?(reason, "No such file") do
          {fresh_id, fresh_path, _} = Ferricstore.Store.ActiveFile.get(ctx, idx)

          case Ferricstore.Bitcask.NIF.v2_append_batch_nosync(fresh_path, batch) do
            {:ok, locations} ->
              mark_checkpoint_dirty(ctx, idx)
              {:ok, fresh_id, locations}

            {:error, _} = err ->
              err
          end
        else
          {:error, reason}
        end

      {:error, _} = err ->
        err
    end
  end

  defp mark_checkpoint_dirty(%{checkpoint_flags: flags}, idx) when is_integer(idx) do
    flag_idx = idx + 1

    if flag_idx <= :atomics.info(flags).size do
      :atomics.put(flags, flag_idx, 1)
    end
  rescue
    _ -> :ok
  end

  defp mark_checkpoint_dirty(_ctx, _idx), do: :ok

  defp append_delete_tombstone_nosync(ctx, idx, key) do
    {_, file_path, _} = Ferricstore.Store.ActiveFile.get(ctx, idx)

    case Ferricstore.Bitcask.NIF.v2_append_ops_batch_nosync(file_path, [{:delete, key}]) do
      {:ok, _locations} = ok ->
        mark_checkpoint_dirty(ctx, idx)
        ok

      other ->
        other
    end
  end

  # -- Keydir binary memory tracking --
  # Only counts binaries > 64 bytes (refc binaries, off-heap).
  # Smaller binaries are inlined in the ETS tuple and counted by :ets.info(:memory).

  defp track_keydir_binary_insert(ctx, idx, keydir, key, new_val) do
    new_bytes = offheap_size(key) + offheap_size(new_val)

    old_bytes =
      case :ets.lookup(keydir, key) do
        [{^key, old_val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(old_val)
        _ -> 0
      end

    delta = new_bytes - old_bytes
    if delta != 0, do: :atomics.add(ctx.keydir_binary_bytes, idx + 1, delta)
  end

  defp track_keydir_binary_delete(ctx, idx, keydir, key) do
    bytes =
      case :ets.lookup(keydir, key) do
        [{^key, val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(val)
        _ -> 0
      end

    if bytes > 0, do: :atomics.sub(ctx.keydir_binary_bytes, idx + 1, bytes)
  end

  defp track_keydir_binary_delete_known(ctx, idx, key, value) do
    bytes = offheap_size(key) + offheap_size(value)
    if bytes > 0, do: :atomics.sub(ctx.keydir_binary_bytes, idx + 1, bytes)
  end

  defp offheap_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
  defp offheap_size(_), do: 0

  defp stored_value_size(value) when is_binary(value), do: byte_size(value)
  defp stored_value_size(value) when is_integer(value), do: byte_size(Integer.to_string(value))
  defp stored_value_size(value) when is_float(value), do: byte_size(Float.to_string(value))
  defp stored_value_size(value), do: value |> to_string() |> byte_size()

  # Stronger Raft boundary for flows that need submission before publishing a
  # local effect.
  defp async_submit_to_raft(idx, command) do
    case Ferricstore.Raft.Batcher.async_submit_ordered(idx, command) do
      :ok -> :ok
      {:error, :overloaded} -> {:error, "ERR raft replication overloaded"}
      {:error, reason} -> {:error, "ERR raft replication failed: #{inspect(reason)}"}
    end
  end

  # Latency-critical local-origin RMW flows wait only for local Batcher
  # acceptance while still surfacing local overload/down errors.
  defp async_enqueue_to_raft(idx, command) do
    case Ferricstore.Raft.Batcher.async_enqueue_ordered(idx, command) do
      :ok -> :ok
      {:error, :overloaded} -> {:error, "ERR raft replication overloaded"}
      {:error, reason} -> {:error, "ERR raft replication failed: #{inspect(reason)}"}
    end
  end

  # -------------------------------------------------------------------
  # Routing helpers
  # -------------------------------------------------------------------

  @doc """
  Returns the slot (0-1023) for a key, respecting hash tags.
  """
  @spec slot_for(FerricStore.Instance.t(), binary()) :: non_neg_integer()
  def slot_for(_ctx, key) do
    SlotMap.slot_for_key(key)
  end

  @doc """
  Returns the shard index (0-based) that owns `key`.

  Routes through the 1,024-slot indirection layer:
  `key -> SlotMap.slot_for_key/1 -> slot_map[slot] -> shard_index`

  Supports Redis hash tags: if the key contains `{tag}` (non-empty content
  between the first `{` and the next `}`), the tag is used for hashing
  instead of the full key.
  """
  @spec shard_for(FerricStore.Instance.t(), binary()) :: non_neg_integer()
  def shard_for(ctx, key) do
    slot = slot_for(ctx, key)
    elem(ctx.slot_map, slot)
  end

  @doc """
  Extracts the hash tag from a key, following Redis hash tag semantics.

  If the key contains a substring enclosed in `{...}` where the content
  between the first `{` and the next `}` is non-empty, that substring is
  used for hashing instead of the full key. This allows related keys to
  be routed to the same shard.

  ## Examples

      iex> Ferricstore.Store.Router.extract_hash_tag("{user:42}:session")
      "user:42"

      iex> Ferricstore.Store.Router.extract_hash_tag("no_tag")
      nil

      iex> Ferricstore.Store.Router.extract_hash_tag("{}empty")
      nil

  """
  @spec extract_hash_tag(binary()) :: binary() | nil
  def extract_hash_tag(key) do
    case :binary.match(key, "{") do
      {start, 1} ->
        rest_start = start + 1
        rest_len = byte_size(key) - rest_start

        case :binary.match(key, "}", [{:scope, {rest_start, rest_len}}]) do
          {end_pos, 1} when end_pos > rest_start ->
            binary_part(key, rest_start, end_pos - rest_start)

          _ ->
            nil
        end

      :nomatch ->
        nil
    end
  end

  @doc """
  Returns the registered process name for the shard at `index`.

  Uses the pre-computed tuple from the instance context for O(1) lookup.
  """
  @spec shard_name(FerricStore.Instance.t(), non_neg_integer()) :: atom()
  def shard_name(ctx, index), do: elem(ctx.shard_names, index)

  @doc """
  Returns the keydir ETS table ref for the shard at `index`.

  Uses the pre-computed tuple from the instance context for O(1) lookup.
  """
  @spec keydir_name(FerricStore.Instance.t(), non_neg_integer()) :: atom() | reference()
  def keydir_name(ctx, index), do: elem(ctx.keydir_refs, index)

  # -------------------------------------------------------------------
  # Convenience accessors (dispatch to correct shard)
  # -------------------------------------------------------------------

  @doc """
  Returns the on-disk file reference for a key's value, or `nil`.

  Used by the sendfile optimisation in standalone TCP mode. Returns
  `{file_path, value_byte_offset, value_size}` for cold (on-disk) keys.
  Returns `nil` for hot keys (ETS), expired keys, or missing keys --
  the caller should fall back to the normal read path.

  Only cold keys benefit from sendfile: hot keys are already in BEAM memory
  and would need a normal `get` + `transport.send`.
  """
  @spec get_file_ref(FerricStore.Instance.t(), binary()) ::
          {binary(), non_neg_integer(), non_neg_integer()} | nil
  def get_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, _value, _lfu} ->
        # Hot key — value is in ETS, sendfile not applicable.
        nil

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)

        case validated_file_ref(path, offset, key, value_size) do
          {^path, value_offset, ^value_size} ->
            {path, value_offset, value_size}

          nil ->
            case retry_changed_file_ref(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
              {:cold_ref, retry_path, value_offset, retry_size} ->
                {retry_path, value_offset, retry_size}

              _ ->
                nil
            end
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Invalid file ref — fall back to GenServer.
        case safe_read_call(ctx, idx, {:get_file_ref, key}) do
          {:ok, result} -> result
          :unavailable -> nil
        end

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :miss ->
        # Key doesn't exist. No GenServer needed.
        nil

      :no_table ->
        nil
    end
  end

  @doc """
  Unified GET that returns everything from a single ETS lookup.

  Returns:
    - `{:hot, value}` — value is in ETS, ready to return
    - `{:cold_ref, path, offset, size}` — value is on disk, file ref for sendfile
    - `{:cold_value, value}` — value was on disk, GenServer fetched it
    - `:miss` — key doesn't exist
  """
  @spec get_with_file_ref(FerricStore.Instance.t(), binary()) ::
          {:hot, binary()}
          | {:cold_ref, binary(), non_neg_integer(), non_neg_integer()}
          | {:cold_value, binary()}
          | :miss
  def get_with_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value}

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)

        case validated_file_ref(path, offset, key, value_size) do
          {^path, value_offset, ^value_size} ->
            Stats.record_cold_read(ctx, key)
            {:cold_ref, path, value_offset, value_size}

          nil ->
            case retry_changed_file_ref(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
              {:cold_ref, retry_path, value_offset, retry_size} ->
                Stats.record_cold_read(ctx, key)
                {:cold_ref, retry_path, value_offset, retry_size}

              {:hot, value} ->
                {:hot, value}

              :miss ->
                Stats.incr_keyspace_misses(ctx)
                :miss
            end
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Cold entry but no valid file ref. Ask the shard to flush pending
        # writes and return a file ref before falling back to materialization.
        shard_file_ref_or_value(ctx, idx, key)

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        :miss

      :miss ->
        # Key not in ETS = doesn't exist. No GenServer needed.
        Stats.incr_keyspace_misses(ctx)
        :miss

      :no_table ->
        # ETS table unavailable (shard restarting). Fall back to GenServer.
        shard_file_ref_or_value(ctx, idx, key)
    end
  end

  defp shard_file_ref_or_value(ctx, idx, key) do
    case safe_read_call(ctx, idx, {:get_file_ref, key}) do
      {:ok, {path, value_offset, value_size}}
      when is_binary(path) and is_integer(value_offset) and is_integer(value_size) ->
        Stats.record_cold_read(ctx, key)
        {:cold_ref, path, value_offset, value_size}

      _ ->
        result =
          case safe_read_call(ctx, idx, {:get, key}) do
            {:ok, value} -> value
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
          {:cold_value, result}
        else
          Stats.incr_keyspace_misses(ctx)
          :miss
        end
    end
  end

  defp validated_file_ref(path, record_offset, key, value_size) do
    case Ferricstore.Bitcask.NIF.v2_validate_value_ref(path, record_offset, key, value_size) do
      {:ok, {value_offset, ^value_size}} ->
        {path, value_offset, value_size}

      _ ->
        maybe_run_validate_file_ref_miss_hook()
        nil
    end
  end

  defp retry_changed_file_ref(ctx, idx, keydir, key, original_location, now) do
    case retry_changed_file_ref_once(ctx, idx, keydir, key, original_location, now) do
      :unchanged_cold ->
        retry_after_unchanged_cold_location(
          fn ->
            retry_changed_file_ref_once(ctx, idx, keydir, key, original_location, now)
          end,
          cold_retry_metadata(ctx, idx, key, :file_ref)
        )

      result ->
        result
    end
  end

  defp retry_changed_file_ref_once(ctx, idx, keydir, key, original_location, now) do
    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value}

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} != original_location ->
        path = cold_file_path(ctx, idx, file_id)

        case validated_file_ref(path, offset, key, value_size) do
          {^path, value_offset, ^value_size} -> {:cold_ref, path, value_offset, value_size}
          nil -> :miss
        end

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} == original_location ->
        :unchanged_cold

      _ ->
        :miss
    end
  end

  defp cold_file_path(ctx, idx, file_id) do
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
    Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  defp maybe_run_validate_file_ref_miss_hook do
    case Process.get(:ferricstore_router_validate_file_ref_miss_hook) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  # Like ets_get but returns file ref info for cold entries and LFU counter for hits.
  # Single lookup provides everything needed — no second ETS read for bookkeeping.
  defp ets_get_full(ctx, idx, keydir, key, now) do
    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, lfu, _fid, _off, _vsize}] when value != nil ->
          {:hit, value, lfu}

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize}

        [{^key, nil, 0, _lfu, :pending, off, vsize}] ->
          {:cold, :pending, off, vsize}

        [{^key, value, exp, lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          {:hit, value, lfu}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize}

        [{^key, nil, exp, _lfu, :pending, off, vsize}] when exp > now ->
          {:cold, :pending, off, vsize}

        [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete_known(ctx, idx, key, value)
          :ets.delete(keydir, key)
          :expired

        [] ->
          :miss
      end
    rescue
      ArgumentError -> :no_table
    end
  end

  defp ets_get_meta_full(ctx, idx, keydir, key, now) do
    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, exp, lfu, _fid, _off, _vsize}]
        when value != nil and (exp == 0 or exp > now) ->
          {:hit, value, exp, lfu}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when (exp == 0 or exp > now) and valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize, exp}

        [{^key, nil, exp, _lfu, :pending, off, vsize}] when exp == 0 or exp > now ->
          {:cold, :pending, off, vsize, exp}

        [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete_known(ctx, idx, key, value)
          :ets.delete(keydir, key)
          :expired

        [] ->
          :miss
      end
    rescue
      ArgumentError -> :no_table
    end
  end

  @doc """
  Retrieves the value for `key`, or `nil` if the key does not exist or is
  expired.

  Hot path: reads directly from ETS (no GenServer roundtrip for cached keys).
  Falls back to a GenServer call for cache misses or when the ETS table is
  temporarily unavailable (e.g. during a shard restart).

  Each successful read is recorded as either *hot* (ETS hit) or *cold*
  (Bitcask fallback) in `Ferricstore.Stats` for the `FERRICSTORE.HOTNESS`
  command and the `INFO stats` hot/cold fields.
  """
  @spec get(FerricStore.Instance.t(), binary()) :: binary() | nil
  def get(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        value

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        # Cold key — value evicted from ETS but disk location known.
        # Read directly from Bitcask via NIF, bypassing the Shard GenServer.
        # The ETS entry has valid file_id/offset from when the write committed,
        # so pread works without flushing pending writes.
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_async(path, offset, key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, key)
            # Warm ETS: promote back to hot if value fits in cache
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            value

          _ ->
            case retry_changed_cold_value(
                   ctx,
                   idx,
                   keydir,
                   key,
                   {file_id, offset, value_size},
                   now
                 ) do
              {:cold, value, retry_file_id, retry_offset} ->
                Stats.record_cold_read(ctx, key)

                warm_ets_after_cold_read(
                  ctx,
                  idx,
                  keydir,
                  key,
                  value,
                  retry_file_id,
                  retry_offset
                )

                value

              {:hot, value} ->
                value

              :miss ->
                Stats.incr_keyspace_misses(ctx)
                nil
            end
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Cold entry but invalid file ref — ask GenServer.
        result =
          case safe_read_call(ctx, idx, {:get, key}) do
            {:ok, value} -> value
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          Stats.incr_keyspace_misses(ctx)
        end

        result

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :miss ->
        # Key not in ETS at all — doesn't exist. No GenServer needed.
        Stats.incr_keyspace_misses(ctx)
        nil

      :no_table ->
        # ETS table unavailable (shard restarting). Fall back to GenServer.
        result =
          case safe_read_call(ctx, idx, {:get, key}) do
            {:ok, value} -> value
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          Stats.incr_keyspace_misses(ctx)
        end

        result
    end
  end

  @spec batch_get(FerricStore.Instance.t(), [binary()]) :: [binary() | nil]
  def batch_get(ctx, keys) do
    now = HLC.now_ms()

    {results, {cold_entries, _cold_count}} =
      Enum.map_reduce(keys, {[], 0}, fn key, {cold_entries, cold_count} ->
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)

        case ets_get_full(ctx, idx, keydir, key, now) do
          {:hit, value, lfu} ->
            sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
            {{:value, value}, {cold_entries, cold_count}}

          {:cold, file_id, offset, value_size}
          when valid_cold_location(file_id, offset, value_size) ->
            path = cold_file_path(ctx, idx, file_id)

            entry = {ctx, idx, keydir, key, path, file_id, offset, value_size}
            {{:cold, cold_count}, {[entry | cold_entries], cold_count + 1}}

          {:cold, _file_id, _offset, _value_size} ->
            result =
              case safe_read_call(ctx, idx, {:get, key}) do
                {:ok, value} -> value
                :unavailable -> nil
              end

            if result != nil do
              Stats.record_cold_read(ctx, key)
            else
              Stats.incr_keyspace_misses(ctx)
            end

            {{:value, result}, {cold_entries, cold_count}}

          :expired ->
            Stats.incr_keyspace_misses(ctx)
            {{:value, nil}, {cold_entries, cold_count}}

          :miss ->
            Stats.incr_keyspace_misses(ctx)
            {{:value, nil}, {cold_entries, cold_count}}

          :no_table ->
            result =
              case safe_read_call(ctx, idx, {:get, key}) do
                {:ok, value} -> value
                :unavailable -> nil
              end

            if result != nil do
              Stats.record_cold_read(ctx, key)
            else
              Stats.incr_keyspace_misses(ctx)
            end

            {{:value, result}, {cold_entries, cold_count}}
        end
      end)

    cold_values =
      cold_entries
      |> Enum.reverse()
      |> read_cold_batch_async(now)
      |> List.to_tuple()

    Enum.map(results, fn
      {:value, value} -> value
      {:cold, index} -> elem(cold_values, index)
    end)
  end

  @doc """
  Batch GET variant for TCP large-value streaming.

  It performs the same single ETS pass as `batch_get/2`, but cold entries whose
  value size is at least `min_file_ref_size` are returned as validated
  `{:file_ref, path, value_offset, size}` tuples instead of being materialized
  into BEAM binaries. Stale or invalid refs fall back to the normal batched cold
  pread path.
  """
  @spec batch_get_with_file_refs(FerricStore.Instance.t(), [binary()], non_neg_integer()) :: [
          binary() | nil | {:file_ref, binary(), non_neg_integer(), non_neg_integer()}
        ]
  def batch_get_with_file_refs(ctx, keys, min_file_ref_size) do
    now = HLC.now_ms()

    {results, {cold_entries, _cold_count}} =
      Enum.map_reduce(keys, {[], 0}, fn key, {cold_entries, cold_count} ->
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)

        case ets_get_full(ctx, idx, keydir, key, now) do
          {:hit, value, lfu} ->
            sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
            {{:value, value}, {cold_entries, cold_count}}

          {:cold, file_id, offset, value_size}
          when valid_cold_location(file_id, offset, value_size) ->
            path = cold_file_path(ctx, idx, file_id)

            maybe_file_ref_or_cold_entry(
              ctx,
              idx,
              keydir,
              key,
              path,
              file_id,
              offset,
              value_size,
              min_file_ref_size,
              cold_entries,
              cold_count,
              now
            )

          {:cold, _file_id, _offset, _value_size} ->
            result =
              case safe_read_call(ctx, idx, {:get, key}) do
                {:ok, value} -> value
                :unavailable -> nil
              end

            if result != nil do
              Stats.record_cold_read(ctx, key)
            else
              Stats.incr_keyspace_misses(ctx)
            end

            {{:value, result}, {cold_entries, cold_count}}

          :expired ->
            Stats.incr_keyspace_misses(ctx)
            {{:value, nil}, {cold_entries, cold_count}}

          :miss ->
            Stats.incr_keyspace_misses(ctx)
            {{:value, nil}, {cold_entries, cold_count}}

          :no_table ->
            result =
              case safe_read_call(ctx, idx, {:get, key}) do
                {:ok, value} -> value
                :unavailable -> nil
              end

            if result != nil do
              Stats.record_cold_read(ctx, key)
            else
              Stats.incr_keyspace_misses(ctx)
            end

            {{:value, result}, {cold_entries, cold_count}}
        end
      end)

    cold_values =
      cold_entries
      |> Enum.reverse()
      |> read_cold_batch_async(now)
      |> List.to_tuple()

    Enum.map(results, fn
      {:value, value} -> value
      {:file_ref, path, offset, size} -> {:file_ref, path, offset, size}
      {:cold, index} -> elem(cold_values, index)
    end)
  end

  defp maybe_file_ref_or_cold_entry(
         ctx,
         idx,
         keydir,
         key,
         path,
         file_id,
         offset,
         value_size,
         min_file_ref_size,
         cold_entries,
         cold_count,
         now
       )
       when value_size >= min_file_ref_size do
    case validated_file_ref(path, offset, key, value_size) do
      {^path, value_offset, ^value_size} ->
        Stats.record_cold_read(ctx, key)
        {{:file_ref, path, value_offset, value_size}, {cold_entries, cold_count}}

      nil ->
        case retry_changed_file_ref(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
          {:cold_ref, retry_path, value_offset, retry_size} ->
            Stats.record_cold_read(ctx, key)
            {{:file_ref, retry_path, value_offset, retry_size}, {cold_entries, cold_count}}

          {:hot, value} ->
            {{:value, value}, {cold_entries, cold_count}}

          :miss ->
            Stats.incr_keyspace_misses(ctx)
            {{:value, nil}, {cold_entries, cold_count}}
        end
    end
  end

  defp maybe_file_ref_or_cold_entry(
         ctx,
         idx,
         keydir,
         key,
         path,
         file_id,
         offset,
         value_size,
         _min_file_ref_size,
         cold_entries,
         cold_count,
         _now
       ) do
    entry = {ctx, idx, keydir, key, path, file_id, offset, value_size}
    {{:cold, cold_count}, {[entry | cold_entries], cold_count + 1}}
  end

  defp read_cold_batch_async([], _now), do: []

  defp read_cold_batch_async(entries, now) do
    {unique_entries, value_indexes} = dedupe_cold_batch_entries(entries)
    unique_values = read_unique_cold_batch_async(unique_entries, now) |> List.to_tuple()

    Enum.map(value_indexes, fn index -> elem(unique_values, index) end)
  end

  defp dedupe_cold_batch_entries(entries) do
    {unique_entries, _index_by_location, value_indexes} =
      Enum.reduce(entries, {[], %{}, []}, fn entry, {unique_acc, index_acc, value_index_acc} ->
        location = cold_batch_entry_location(entry)

        case Map.fetch(index_acc, location) do
          {:ok, index} ->
            {unique_acc, index_acc, [index | value_index_acc]}

          :error ->
            index = map_size(index_acc)
            {[entry | unique_acc], Map.put(index_acc, location, index), [index | value_index_acc]}
        end
      end)

    {Enum.reverse(unique_entries), Enum.reverse(value_indexes)}
  end

  defp cold_batch_entry_location({_ctx, _idx, _keydir, key, path, _file_id, offset, _value_size}) do
    {path, offset, key}
  end

  defp read_unique_cold_batch_async(entries, now) do
    locations =
      Enum.map(entries, fn {_ctx, _idx, _keydir, key, path, _file_id, offset, _value_size} ->
        {path, offset, key}
      end)

    values =
      case router_pread_batch_keyed(locations, @cold_batch_read_timeout_ms) do
        {:ok, values} when is_list(values) ->
          if length(values) == length(entries) do
            values
          else
            List.duplicate({:error, :batch_result_length_mismatch}, length(entries))
          end

        {:error, reason} ->
          List.duplicate({:error, reason}, length(entries))
      end

    entry_values = Enum.zip(entries, values)

    corrupt_by_path =
      Enum.reduce(entry_values, %{}, fn
        {{_ctx, _idx, _keydir, _key, _path, _file_id, _offset, _value_size}, value},
        corrupt_by_path
        when is_binary(value) ->
          corrupt_by_path

        {{_ctx, _idx, _keydir, _key, path, _file_id, _offset, _value_size}, value},
        corrupt_by_path ->
          reason = cold_batch_read_error_reason(value)
          Map.update(corrupt_by_path, {path, reason}, 1, &(&1 + 1))
      end)

    emit_batch_cold_read_corruption(corrupt_by_path)

    Enum.map(entry_values, fn
      {{ctx, idx, keydir, key, _path, file_id, offset, _value_size}, value}
      when is_binary(value) ->
        Stats.record_cold_read(ctx, key)
        warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
        value

      {{ctx, idx, keydir, key, _path, file_id, offset, value_size}, _value} ->
        case retry_changed_cold_value(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
          {:cold, value, retry_file_id, retry_offset} ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, retry_file_id, retry_offset)
            value

          {:hot, value} ->
            value

          :miss ->
            Stats.incr_keyspace_misses(ctx)
            nil
        end
    end)
  end

  defp router_pread_batch_keyed(locations, timeout_ms) do
    case Process.get(:ferricstore_router_pread_batch_keyed_result) do
      nil -> Ferricstore.Store.ColdRead.pread_batch_keyed(locations, timeout_ms)
      forced_result -> forced_result
    end
  end

  defp emit_batch_cold_read_corruption(corrupt_by_path) when map_size(corrupt_by_path) == 0,
    do: :ok

  defp emit_batch_cold_read_corruption(corrupt_by_path) do
    Enum.each(corrupt_by_path, fn {{path, reason}, count} ->
      :telemetry.execute(
        [:ferricstore, :bitcask, :pread_corrupt],
        %{count: count},
        %{path: path, reason: reason}
      )
    end)
  end

  defp cold_batch_read_error_reason({:error, reason}) when is_binary(reason) do
    if String.contains?(reason, "missing_file"), do: :missing_file, else: :corrupt_record
  end

  defp cold_batch_read_error_reason({:error, reason}) when reason in [:missing_file, :enoent],
    do: :missing_file

  defp cold_batch_read_error_reason({:error, :timeout}), do: :timeout

  defp cold_batch_read_error_reason({:error, :batch_result_length_mismatch}),
    do: :batch_result_length_mismatch

  defp cold_batch_read_error_reason({:error, _reason}), do: :corrupt_record

  defp cold_batch_read_error_reason(_value), do: :nil_from_cold_location

  defp read_cold_async(path, offset, expected_key) do
    Ferricstore.Store.ColdRead.pread_at(path, offset, expected_key, @cold_batch_read_timeout_ms)
  end

  defp retry_changed_cold_value(ctx, idx, keydir, key, original_location, now) do
    case retry_changed_cold_value_once(ctx, idx, keydir, key, original_location, now) do
      :unchanged_cold ->
        retry_after_unchanged_cold_location(
          fn ->
            retry_changed_cold_value_once(ctx, idx, keydir, key, original_location, now)
          end,
          cold_retry_metadata(ctx, idx, key, :value)
        )

      result ->
        result
    end
  end

  defp retry_changed_cold_value_once(ctx, idx, keydir, key, original_location, now) do
    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value}

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} != original_location ->
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_async(path, offset, key) do
          {:ok, value} when is_binary(value) -> {:cold, value, file_id, offset}
          _ -> :miss
        end

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} == original_location ->
        :unchanged_cold

      _ ->
        :miss
    end
  end

  defp retry_changed_cold_meta(ctx, idx, keydir, key, original_location, now) do
    case retry_changed_cold_meta_once(ctx, idx, keydir, key, original_location, now) do
      :unchanged_cold ->
        retry_after_unchanged_cold_location(
          fn ->
            retry_changed_cold_meta_once(ctx, idx, keydir, key, original_location, now)
          end,
          cold_retry_metadata(ctx, idx, key, :meta)
        )

      result ->
        result
    end
  end

  defp retry_changed_cold_meta_once(ctx, idx, keydir, key, original_location, now) do
    case ets_get_meta_full(ctx, idx, keydir, key, now) do
      {:hit, value, expire_at_ms, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value, expire_at_ms}

      {:cold, file_id, offset, value_size, expire_at_ms}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} != original_location ->
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_async(path, offset, key) do
          {:ok, value} when is_binary(value) -> {:cold, value, expire_at_ms, file_id, offset}
          _ -> :miss
        end

      {:cold, file_id, offset, value_size, _expire_at_ms}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} == original_location ->
        :unchanged_cold

      _ ->
        :miss
    end
  end

  defp retry_after_unchanged_cold_location(retry_fun, metadata) when is_function(retry_fun, 0) do
    retry_after_unchanged_cold_location(retry_fun, metadata, @cold_location_retry_attempts)
  end

  defp retry_after_unchanged_cold_location(_retry_fun, metadata, 0) do
    emit_cold_retry_exhausted(metadata)
    :miss
  end

  defp retry_after_unchanged_cold_location(retry_fun, metadata, attempts_left) do
    maybe_run_cold_location_miss_hook()
    Process.sleep(@cold_location_retry_sleep_ms)

    case retry_fun.() do
      :unchanged_cold ->
        retry_after_unchanged_cold_location(retry_fun, metadata, attempts_left - 1)

      result ->
        result
    end
  end

  defp cold_retry_metadata(ctx, idx, key, operation) do
    %{
      instance: ctx.name,
      shard_index: idx,
      operation: operation,
      reason: :unchanged_cold_location,
      redis_key_hash: :erlang.phash2(key)
    }
  end

  defp emit_cold_retry_exhausted(nil), do: :ok

  defp emit_cold_retry_exhausted(metadata) do
    :telemetry.execute(
      [:ferricstore, :store, :cold_read_retry_exhausted],
      %{count: 1, attempts: @cold_location_retry_attempts},
      metadata
    )
  end

  defp maybe_run_cold_location_miss_hook do
    case Process.get(:ferricstore_router_cold_location_miss_hook) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  @doc """
  Returns `{value, expire_at_ms}` for a live key, or `nil` if the key does
  not exist or is expired.

  Hot path: reads directly from ETS for cached keys. Each read is recorded
  as hot or cold in `Ferricstore.Stats`.
  """
  @spec get_meta(FerricStore.Instance.t(), binary()) :: {binary(), non_neg_integer()} | nil
  def get_meta(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_meta_full(ctx, idx, keydir, key, now) do
      {:hit, value, expire_at_ms, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {value, expire_at_ms}

      {:cold, file_id, offset, value_size, expire_at_ms}
      when valid_cold_location(file_id, offset, value_size) ->
        # Cold key — read value from disk directly, return with expire_at_ms.
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_async(path, offset, key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            {value, expire_at_ms}

          _ ->
            case retry_changed_cold_meta(
                   ctx,
                   idx,
                   keydir,
                   key,
                   {file_id, offset, value_size},
                   now
                 ) do
              {:cold, value, retry_expire_at_ms, retry_file_id, retry_offset} ->
                Stats.record_cold_read(ctx, key)

                warm_ets_after_cold_read(
                  ctx,
                  idx,
                  keydir,
                  key,
                  value,
                  retry_file_id,
                  retry_offset
                )

                {value, retry_expire_at_ms}

              {:hot, value, retry_expire_at_ms} ->
                {value, retry_expire_at_ms}

              :miss ->
                Stats.incr_keyspace_misses(ctx)
                nil
            end
        end

      {:cold, _file_id, _offset, _value_size, _expire_at_ms} ->
        # Invalid file ref — ask GenServer.
        result =
          case safe_read_call(ctx, idx, {:get_meta, key}) do
            {:ok, result} -> result
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          Stats.incr_keyspace_misses(ctx)
        end

        result

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :miss ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :no_table ->
        result =
          case safe_read_call(ctx, idx, {:get_meta, key}) do
            {:ok, result} -> result
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          Stats.incr_keyspace_misses(ctx)
        end

        result
    end
  end

  @doc """
  Returns the expiry timestamp for a live plain key without reading its value.

  This is used by expiry-time commands so cold large values do not pay a
  Bitcask pread just to report TTL metadata.
  """
  @spec expire_at_ms(FerricStore.Instance.t(), binary()) :: non_neg_integer() | nil
  def expire_at_ms(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          0

        [{^key, nil, 0, _lfu, fid, off, vsize}]
        when valid_cold_location(fid, off, vsize) ->
          0

        [{^key, nil, 0, _lfu, :pending, _off, vsize}]
        when valid_pending_value_size(vsize) ->
          0

        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          exp

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          exp

        [{^key, nil, exp, _lfu, :pending, _off, vsize}]
        when exp > now and valid_pending_value_size(vsize) ->
          exp

        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          nil

        [] ->
          nil
      end
    rescue
      ArgumentError -> keydir_unavailable(ctx, idx, :expire_at_ms, nil)
    end
  end

  @doc """
  Returns the live plain key value size without reading a cold value.

  Hot entries use the in-memory value size; cold entries use the keydir
  `value_size` field populated by Bitcask append/recovery.
  """
  @spec value_size(FerricStore.Instance.t(), binary()) :: non_neg_integer() | nil
  def value_size(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          stored_value_size(value)

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          vsize

        [{^key, nil, 0, _lfu, :pending, _off, vsize}]
        when valid_pending_value_size(vsize) ->
          vsize

        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          stored_value_size(value)

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          vsize

        [{^key, nil, exp, _lfu, :pending, _off, vsize}]
        when exp > now and valid_pending_value_size(vsize) ->
          vsize

        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          nil

        [] ->
          nil
      end
    rescue
      ArgumentError -> keydir_unavailable(ctx, idx, :value_size, nil)
    end
  end

  @doc false
  @spec object_lfu(FerricStore.Instance.t(), binary()) :: non_neg_integer() | nil
  def object_lfu(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, lfu, _fid, _off, _vsize}] when value != nil ->
          lfu

        [{^key, nil, 0, lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          lfu

        [{^key, nil, 0, lfu, :pending, _off, vsize}]
        when valid_pending_value_size(vsize) ->
          lfu

        [{^key, value, exp, lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          lfu

        [{^key, nil, exp, lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          lfu

        [{^key, nil, exp, lfu, :pending, _off, vsize}]
        when exp > now and valid_pending_value_size(vsize) ->
          lfu

        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          nil

        [] ->
          nil
      end
    rescue
      ArgumentError -> keydir_unavailable(ctx, idx, :object_lfu, nil)
    end
  end

  @doc """
  Returns a byte range for a live plain key without reading the full cold value.

  Hot entries slice the in-memory value. Cold entries validate the Bitcask
  location once, then read only the requested value bytes from the data file.
  Missing or expired keys return `nil`, matching `get/2`.
  """
  @spec getrange(FerricStore.Instance.t(), binary(), integer(), integer()) :: binary() | nil
  def getrange(ctx, key, start_idx, end_idx) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        range_from_value(value, start_idx, end_idx)

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        cold_range_from_location(
          ctx,
          idx,
          keydir,
          key,
          file_id,
          offset,
          value_size,
          start_idx,
          end_idx,
          now
        )

      {:cold, _file_id, _offset, _value_size} ->
        fallback_getrange(ctx, idx, key, start_idx, end_idx)

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :miss ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :no_table ->
        fallback_getrange(ctx, idx, key, start_idx, end_idx)
    end
  end

  defp cold_range_from_location(
         ctx,
         idx,
         keydir,
         key,
         file_id,
         offset,
         value_size,
         start_idx,
         end_idx,
         now
       ) do
    case normalize_byte_range(value_size, start_idx, end_idx) do
      :empty ->
        ""

      {relative_offset, count} ->
        path = cold_file_path(ctx, idx, file_id)

        case validated_file_ref(path, offset, key, value_size) do
          {^path, value_offset, ^value_size} ->
            case read_validated_value_range(ctx, key, path, value_offset + relative_offset, count) do
              {:ok, value} ->
                value

              :error ->
                retry_getrange_after_ref_miss(
                  ctx,
                  idx,
                  keydir,
                  key,
                  {file_id, offset, value_size},
                  start_idx,
                  end_idx,
                  now
                )
            end

          nil ->
            retry_getrange_after_ref_miss(
              ctx,
              idx,
              keydir,
              key,
              {file_id, offset, value_size},
              start_idx,
              end_idx,
              now
            )
        end
    end
  end

  defp retry_getrange_after_ref_miss(
         ctx,
         idx,
         keydir,
         key,
         original_location,
         start_idx,
         end_idx,
         now
       ) do
    case retry_changed_file_ref(ctx, idx, keydir, key, original_location, now) do
      {:cold_ref, path, value_offset, value_size} ->
        case normalize_byte_range(value_size, start_idx, end_idx) do
          :empty ->
            ""

          {relative_offset, count} ->
            case read_validated_value_range(ctx, key, path, value_offset + relative_offset, count) do
              {:ok, value} ->
                value

              :error ->
                Stats.incr_keyspace_misses(ctx)
                nil
            end
        end

      {:hot, value} ->
        range_from_value(value, start_idx, end_idx)

      :miss ->
        Stats.incr_keyspace_misses(ctx)
        nil
    end
  end

  defp read_validated_value_range(ctx, key, path, offset, count) do
    maybe_run_cold_range_pread_miss_hook()

    case pread_file_range(path, offset, count) do
      {:ok, value} ->
        Stats.record_cold_read(ctx, key)
        {:ok, value}

      :error ->
        :error
    end
  end

  defp maybe_run_cold_range_pread_miss_hook do
    case Process.get(:ferricstore_router_cold_range_pread_miss_hook) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  defp fallback_getrange(ctx, idx, key, start_idx, end_idx) do
    result =
      case safe_read_call(ctx, idx, {:get, key}) do
        {:ok, value} -> value
        :unavailable -> nil
      end

    if result != nil do
      Stats.record_cold_read(ctx, key)
      range_from_value(result, start_idx, end_idx)
    else
      Stats.incr_keyspace_misses(ctx)
      nil
    end
  end

  defp pread_file_range(_path, _offset, 0), do: {:ok, ""}

  defp pread_file_range(path, offset, count) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          case :file.pread(fd, offset, count) do
            {:ok, value} when is_binary(value) and byte_size(value) == count -> {:ok, value}
            _ -> :error
          end
        after
          :file.close(fd)
        end

      {:error, _reason} ->
        :error
    end
  end

  defp range_from_value(value, start_idx, end_idx) when is_binary(value),
    do: slice_binary_range(value, start_idx, end_idx)

  defp range_from_value(value, start_idx, end_idx) when is_integer(value),
    do: value |> Integer.to_string() |> slice_binary_range(start_idx, end_idx)

  defp range_from_value(value, start_idx, end_idx) when is_float(value),
    do: value |> Float.to_string() |> slice_binary_range(start_idx, end_idx)

  defp range_from_value(value, start_idx, end_idx),
    do: value |> to_string() |> slice_binary_range(start_idx, end_idx)

  defp slice_binary_range(value, start_idx, end_idx) do
    case normalize_byte_range(byte_size(value), start_idx, end_idx) do
      :empty -> ""
      {offset, count} -> binary_part(value, offset, count)
    end
  end

  defp normalize_byte_range(0, _start_idx, _end_idx), do: :empty

  defp normalize_byte_range(size, start_idx, end_idx) when size > 0 do
    start_norm = if start_idx < 0, do: max(size + start_idx, 0), else: start_idx
    end_norm = if end_idx < 0, do: size + end_idx, else: end_idx

    start_clamped = min(start_norm, size)
    end_clamped = min(end_norm, size - 1)

    if start_clamped > end_clamped do
      :empty
    else
      {start_clamped, end_clamped - start_clamped + 1}
    end
  end

  # Sampling rate for read-side bookkeeping (LFU touch + hot/cold stats).
  # 1 in N reads performs the ETS writes. Reduces write contention at high
  # concurrency with negligible impact on LFU accuracy (logarithmic counter)
  # and stats precision (ratio stays the same).
  # Default 100 = sample 1 in 100 reads. Set to 1 to disable sampling.

  # LFU counter already available from the initial ets_get_full lookup.
  # Eliminates the second ETS lookup that sampled_read_bookkeeping does.
  defp sampled_read_bookkeeping_fast(ctx, keydir, key, lfu) do
    rate = ctx.read_sample_rate

    if rate <= 1 or :rand.uniform(rate) == 1 do
      Stats.incr_keyspace_hits(ctx)
      LFU.touch(ctx, keydir, key, lfu)
      Stats.record_hot_read(ctx, key)
    end
  end

  # After a cold read, promote the value back to ETS (hot) if it fits
  # under the hot cache max value size threshold. ETS is :public with
  # write_concurrency so this is safe from any process.
  @doc false
  def warm_ets_after_cold_read(ctx, keydir, key, value, file_id, offset) do
    warm_ets_after_cold_read(ctx, keydir_index(ctx, keydir), keydir, key, value, file_id, offset)
  end

  @doc false
  def warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset) do
    # Skip promotion when under memory pressure — prevents evict/re-promote
    # thrashing where MemoryGuard evicts values and cold reads immediately
    # re-cache them. skip_promotion? is set at :pressure level (85%+).
    skip_promotion = :atomics.get(ctx.pressure_flags, 3) == 1

    if byte_size(value) <= ctx.hot_cache_max_value_size and not skip_promotion do
      lfu = LFU.initial()

      try do
        replaced =
          :ets.select_replace(keydir, [
            {
              {key, nil, :"$1", :"$2", file_id, offset, :"$3"},
              [],
              [{{key, value, :"$1", lfu, file_id, offset, :"$3"}}]
            }
          ])

        if replaced > 0 do
          track_keydir_binary_warm(ctx, idx, value)
        end

        :ok
      rescue
        ArgumentError -> :ok
      end
    end
  end

  defp keydir_index(%{keydir_refs: refs, shard_count: count}, keydir)
       when is_tuple(refs) and is_integer(count) and count > 0 do
    Enum.find(0..(count - 1), fn idx -> elem(refs, idx) == keydir end)
  end

  defp keydir_index(_ctx, _keydir), do: nil

  defp track_keydir_binary_warm(%{keydir_binary_bytes: ref}, idx, value)
       when is_integer(idx) do
    bytes = offheap_size(value)

    if bytes > 0 and idx >= 0 and idx < :atomics.info(ref).size do
      :atomics.add(ref, idx + 1, bytes)
    end
  end

  defp track_keydir_binary_warm(_ctx, _idx, _value), do: :ok

  @max_key_size 65_535
  @max_value_size 512 * 1024 * 1024

  @spec max_key_size() :: pos_integer()
  @doc "Returns the maximum allowed key size in bytes."
  def max_key_size, do: @max_key_size

  @spec max_value_size() :: pos_integer()
  @doc "Returns the maximum allowed value size in bytes."
  def max_value_size, do: @max_value_size

  @doc """
  Batch PUT API with `:ok | {:error, _}` result shape.

  The default application instance submits through quorum. Embedded/custom
  instances write locally because the Raft batchers are owned by the default
  application instance.
  """
  @spec batch_put(FerricStore.Instance.t(), [{binary(), binary()}]) ::
          :ok | {:error, binary()}
  def batch_put(%{name: name} = ctx, kv_pairs) when name != :default do
    case pressured_batch_shard(ctx, kv_pairs) do
      nil -> batch_local_put(ctx, kv_pairs)
      idx -> {:error, "ERR disk pressure on shard #{idx}, rejecting write"}
    end
  end

  def batch_put(ctx, kv_pairs) do
    case batch_quorum_put(ctx, kv_pairs) do
      results when is_list(results) ->
        Enum.find(results, &match?({:error, _}, &1)) || :ok

      other ->
        other
    end
  end

  @doc false
  def __install_batch_entries_for_test__(ctx, idx, entries, shard_locations) do
    keydir = elem(ctx.keydir_refs, idx)
    install_batch_entries(ctx, idx, keydir, entries, shard_locations, LFU.initial())
  end

  defp install_batch_entries(ctx, idx, keydir, entries, shard_locations, lfu_val) do
    Enum.each(entries, fn {key, value, value_for_ets} ->
      clear_compound_data_structure_for_string_put(ctx, idx, keydir, key)

      case Map.get(shard_locations, key) do
        {file_id, offset, value_size} ->
          track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)
          :ets.insert(keydir, {key, value_for_ets, 0, lfu_val, file_id, offset, value_size})

        nil ->
          install_batch_pending_entry(ctx, idx, keydir, key, value, value_for_ets, lfu_val)
      end
    end)
  end

  defp install_batch_pending_entry(ctx, idx, keydir, key, value, value_for_ets, lfu_val) do
    case :ets.lookup(keydir, key) do
      [{^key, ^value_for_ets, 0, _lfu, fid, off, value_size}]
      when fid != :pending and valid_cold_location(fid, off, value_size) ->
        :ok

      _ ->
        track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)
        :ets.insert(keydir, {key, value_for_ets, 0, lfu_val, :pending, 0, byte_size(value)})
    end
  end

  defp batch_local_put(ctx, kv_pairs) do
    # Embedded/custom instances are local/direct. They must not consult the
    # default instance's Raft batchers; those global names are owned by the
    # application instance and can be under unrelated backpressure.
    Enum.reduce_while(kv_pairs, :ok, fn {key, value}, :ok ->
      case put(ctx, key, value, 0) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
        other -> {:halt, {:error, inspect(other)}}
      end
    end)
  end

  defp pressured_batch_shard(ctx, kv_pairs) do
    Enum.find_value(kv_pairs, fn {key, _value} ->
      idx = shard_for(ctx, key)
      if shard_under_disk_pressure?(ctx, idx), do: idx, else: nil
    end)
  end

  @doc """
  Batch quorum PUT for pipelined SET commands.

  Groups commands by shard, submits each group as a single batch to its
  Batcher with ONE reply ref per shard, then waits for all shard replies.
  Returns a list of results in input order.

  Uses one ref per shard (not per command) so the connection process's
  selective receive scans at most shard_count refs instead of N*pipeline
  refs — critical for high concurrency where TCP messages flood the
  mailbox.
  """
  @spec batch_quorum_put(FerricStore.Instance.t(), [{binary(), binary()}]) :: [
          :ok | {:error, binary() | {:timeout, :unknown_outcome}}
        ]
  def batch_quorum_put(ctx, kv_pairs) do
    batch_quorum_put(ctx, kv_pairs, nil)
  end

  defp batch_quorum_commands(ctx, keyed_commands) do
    batch_quorum_commands(ctx, keyed_commands, nil)
  end

  defp batch_quorum_commands(_ctx, [], _origin_node), do: []

  defp batch_quorum_commands(ctx, keyed_commands, origin_node) do
    wv_size = :counters.info(ctx.write_version).size

    {by_shard, count, by_shard_commands} =
      keyed_commands
      |> Enum.reduce({%{}, 0, %{}}, fn {key, command}, {shards, i, commands_map} ->
        idx = shard_for(ctx, key)
        {cmds, indices} = Map.get(shards, idx, {[], []})

        {
          Map.put(shards, idx, {[command | cmds], [i | indices]}),
          i + 1,
          Map.update(commands_map, idx, [{key, command}], fn acc -> [{key, command} | acc] end)
        }
      end)

    shard_refs =
      Enum.map(by_shard, fn {shard_idx, {cmds, indices}} ->
        {from, token} = ReplyAwaiter.new()
        cmds = Enum.reverse(cmds)

        if origin_node == nil do
          Ferricstore.Raft.Batcher.write_batch(shard_idx, cmds, from)
        else
          Ferricstore.Raft.Batcher.write_batch_forwarded(shard_idx, cmds, from, origin_node)
        end

        {token, shard_idx, Enum.reverse(indices)}
      end)

    results =
      collect_shard_replies(shard_refs, wv_size, ctx, %{}, System.monotonic_time(:millisecond))

    results =
      Enum.reduce(by_shard_commands, results, fn {shard_idx, commands}, acc ->
        {_cmds, indices} = Map.fetch!(by_shard, shard_idx)
        first_index = List.last(indices)

        case Map.get(acc, first_index) do
          {:error, {:not_leader, {_shard_name, leader_node}}} when is_atom(leader_node) ->
            merge_forwarded_commands(acc, by_shard, shard_idx, commands, leader_node, ctx)

          {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
            merge_forwarded_commands(acc, by_shard, shard_idx, commands, leader_node, ctx)

          _ ->
            acc
        end
      end)

    0..(count - 1)
    |> Enum.map(fn i -> Map.get(results, i, ErrorReasons.write_timeout_unknown()) end)
  end

  defp batch_quorum_put(_ctx, [], _origin_node), do: []

  defp batch_quorum_put(ctx, kv_pairs, origin_node) do
    wv_size = :counters.info(ctx.write_version).size

    # Single pass: group by shard, build cmds + indices lists simultaneously,
    # also remember the kv_pairs subset per shard so we can re-issue the
    # batch via erpc on :not_leader replies.
    {by_shard, count, by_shard_kvs} =
      kv_pairs
      |> Enum.reduce({%{}, 0, %{}}, fn {key, value}, {shards, i, kvs_map} ->
        idx = shard_for(ctx, key)
        cmd = {:put, key, value, 0}
        entry = Map.get(shards, idx, {[], []})
        {cmds, indices} = entry
        shards = Map.put(shards, idx, {[cmd | cmds], [i | indices]})
        kvs_map = Map.update(kvs_map, idx, [{key, value}], fn acc -> [{key, value} | acc] end)
        {shards, i + 1, kvs_map}
      end)

    shard_refs =
      Enum.map(by_shard, fn {shard_idx, {cmds, indices}} ->
        {from, token} = ReplyAwaiter.new()
        cmds = Enum.reverse(cmds)

        if origin_node == nil do
          Ferricstore.Raft.Batcher.write_batch(shard_idx, cmds, from)
        else
          Ferricstore.Raft.Batcher.write_batch_forwarded(shard_idx, cmds, from, origin_node)
        end

        {token, shard_idx, Enum.reverse(indices)}
      end)

    results =
      collect_shard_replies(shard_refs, wv_size, ctx, %{}, System.monotonic_time(:millisecond))

    # Per-shard not_leader → forward that shard's slice to its hinted leader.
    # Each shard reports independently; we re-issue just the failing shard.
    results =
      Enum.reduce(by_shard_kvs, results, fn {shard_idx, kvs}, acc ->
        # All indices for this shard share the same shard-level reply
        first_index = Map.get(by_shard, shard_idx) |> elem(1) |> List.last()

        case Map.get(acc, first_index) do
          {:error, {:not_leader, {_shard_name, leader_node}}} when is_atom(leader_node) ->
            merge_forwarded(acc, by_shard, shard_idx, kvs, leader_node, ctx)

          {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
            merge_forwarded(acc, by_shard, shard_idx, kvs, leader_node, ctx)

          _ ->
            acc
        end
      end)

    0..(count - 1)
    |> Enum.map(fn i -> Map.get(results, i, ErrorReasons.write_timeout_unknown()) end)
  end

  defp merge_forwarded(acc, by_shard, shard_idx, kvs, leader_node, ctx) do
    {_, indices} = Map.fetch!(by_shard, shard_idx)
    indices = Enum.reverse(indices)
    new_results = forward_batch_to_leader(ctx, leader_node, shard_idx, Enum.reverse(kvs))

    Enum.zip(indices, new_results)
    |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
  end

  defp merge_forwarded_commands(acc, by_shard, shard_idx, commands, leader_node, ctx) do
    {_, indices} = Map.fetch!(by_shard, shard_idx)
    indices = Enum.reverse(indices)

    new_results =
      forward_batch_commands_to_leader(ctx, leader_node, shard_idx, Enum.reverse(commands))

    Enum.zip(indices, new_results)
    |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
  end

  defp collect_shard_replies([], _wv_size, _ctx, acc, _start), do: acc

  defp collect_shard_replies(remaining_refs, wv_size, ctx, acc, start) do
    elapsed = System.monotonic_time(:millisecond) - start
    timeout = max(10_000 - elapsed, 0)

    refs_by_token =
      Map.new(remaining_refs, fn {token, shard_idx, indices} ->
        {token, {shard_idx, indices}}
      end)

    {_status, replies, _unresolved} =
      remaining_refs
      |> Enum.map(fn {token, _shard_idx, _indices} -> token end)
      |> ReplyAwaiter.collect(timeout)

    Enum.reduce(replies, acc, fn {token, result}, next_acc ->
      {shard_idx, indices} = Map.fetch!(refs_by_token, token)
      apply_shard_results(result, indices, shard_idx, wv_size, ctx, next_acc)
    end)
  end

  defp apply_shard_results({:ok, results}, indices, shard_idx, wv_size, ctx, acc)
       when is_list(results) do
    ok_count = Enum.count(results, fn r -> r == :ok or not match?({:error, _}, r) end)

    if ok_count > 0 and shard_idx < wv_size do
      :counters.add(ctx.write_version, shard_idx + 1, ok_count)
    end

    Enum.zip(indices, results)
    |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
  end

  defp apply_shard_results(result, indices, shard_idx, wv_size, ctx, acc) do
    case result do
      {:error, _} ->
        Enum.reduce(indices, acc, fn i, a -> Map.put(a, i, result) end)

      _ ->
        if shard_idx < wv_size,
          do: :counters.add(ctx.write_version, shard_idx + 1, length(indices))

        Enum.reduce(indices, acc, fn i, a -> Map.put(a, i, result) end)
    end
  end

  # Forward a batch to the leader's node. Used by batch_quorum_put when
  # the local Batcher rejects with :not_leader. Issues an erpc to run
  # batch_quorum_put on the leader, then returns its results.
  defp forward_batch_to_leader(_ctx, leader_node, _shard_idx, kv_pairs)
       when leader_node == node() do
    Enum.map(kv_pairs, fn _ -> {:error, "ERR not leader, election in progress"} end)
  end

  defp forward_batch_to_leader(_ctx, leader_node, shard_idx, kv_pairs) do
    try do
      remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)

      leader_results =
        :erpc.call(
          leader_node,
          __MODULE__,
          :__forwarded_batch_quorum_put__,
          [remote_ctx, kv_pairs, node()],
          10_000
        )

      unwrap_forwarded_batch_results(shard_idx, leader_results)
    catch
      _, reason ->
        require Logger
        Logger.warning("batch forward to #{inspect(leader_node)} failed: #{inspect(reason)}")
        __forward_batch_failure_results__(reason, length(kv_pairs))
    end
  end

  defp forward_batch_commands_to_leader(_ctx, leader_node, _shard_idx, keyed_commands)
       when leader_node == node() do
    Enum.map(keyed_commands, fn _ -> {:error, "ERR not leader, election in progress"} end)
  end

  defp forward_batch_commands_to_leader(_ctx, leader_node, shard_idx, keyed_commands) do
    try do
      remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)

      leader_results =
        :erpc.call(
          leader_node,
          __MODULE__,
          :__forwarded_batch_quorum_commands__,
          [remote_ctx, keyed_commands, node()],
          10_000
        )

      unwrap_forwarded_batch_results(shard_idx, leader_results)
    catch
      _, reason ->
        require Logger

        Logger.warning(
          "batch command forward to #{inspect(leader_node)} failed: #{inspect(reason)}"
        )

        __forward_batch_failure_results__(reason, length(keyed_commands))
    end
  end

  @doc false
  def __forwarded_batch_quorum_put__(ctx, kv_pairs, origin_node) do
    batch_quorum_put(ctx, kv_pairs, origin_node)
  end

  @doc false
  def __forwarded_batch_quorum_commands__(ctx, keyed_commands, origin_node) do
    batch_quorum_commands(ctx, keyed_commands, origin_node)
  end

  defp unwrap_forwarded_batch_results(shard_idx, results) when is_list(results) do
    Enum.map(results, &barrier_forwarded_result(shard_idx, &1, 5_000))
  end

  defp unwrap_forwarded_batch_results(_shard_idx, other), do: other

  @doc """
  Stores `key` with `value`. `expire_at_ms` is an absolute Unix-epoch
  timestamp in milliseconds; pass `0` for no expiry.
  """
  @spec put(FerricStore.Instance.t(), binary(), binary(), non_neg_integer()) ::
          :ok | {:error, binary()}
  def put(ctx, key, value, expire_at_ms \\ 0) do
    cond do
      byte_size(key) > @max_key_size ->
        {:error, "ERR key too large (max #{@max_key_size} bytes)"}

      is_binary(value) and byte_size(value) >= @max_value_size ->
        {:error, "ERR value too large (max #{@max_value_size} bytes)"}

      true ->
        case check_keydir_full(ctx, key) do
          :ok ->
            idx = shard_for(ctx, key)
            raft_write(ctx, idx, key, {:put, key, value, expire_at_ms})

          {:error, _} = err ->
            err
        end
    end
  end

  @doc false
  def flow_get(ctx, id, partition_key) when is_binary(id) do
    key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    cond do
      Ferricstore.Flow.LMDB.write_through?() ->
        flow_get_lmdb(ctx, key)

      Ferricstore.Flow.LMDB.mirror?() ->
        case get(ctx, key) do
          nil -> flow_get_lmdb(ctx, key)
          value -> value
        end

      true ->
        get(ctx, key)
    end
  end

  defp flow_get_lmdb(ctx, key) do
    idx = shard_for(ctx, key)

    path =
      ctx.data_dir |> Ferricstore.DataDir.shard_data_path(idx) |> Ferricstore.Flow.LMDB.path()

    case Ferricstore.Flow.LMDB.get(path, key) do
      {:ok, blob} ->
        case Ferricstore.Flow.LMDB.decode_value(blob, System.os_time(:millisecond)) do
          {:ok, value} ->
            value

          :expired ->
            Ferricstore.Flow.LMDB.delete_state_artifacts(path, key)
            nil

          :error ->
            nil
        end

      :not_found ->
        nil

      {:error, _reason} ->
        nil
    end
  end

  @doc false
  def flow_create(ctx, %{id: id} = attrs) when is_binary(id) do
    key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

    if byte_size(key) > @max_key_size do
      {:error, "ERR key too large (max #{@max_key_size} bytes)"}
    else
      idx = shard_for(ctx, key)
      raft_write(ctx, idx, key, {:flow_create, key, attrs})
    end
  end

  @doc false
  def flow_create_batch(_ctx, []), do: []

  def flow_create_batch(ctx, attrs_list) when is_list(attrs_list) do
    if ctx.name == :default do
      {valid, indexed_results} =
        attrs_list
        |> Enum.with_index()
        |> Enum.reduce({[], %{}}, fn
          {%{id: id} = attrs, idx}, {valid_acc, result_acc} when is_binary(id) ->
            key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

            if byte_size(key) > @max_key_size do
              {valid_acc,
               Map.put(
                 result_acc,
                 idx,
                 {:error, "ERR key too large (max #{@max_key_size} bytes)"}
               )}
            else
              {[{idx, key, {:flow_create, key, attrs}} | valid_acc], result_acc}
            end

          {_attrs, idx}, {valid_acc, result_acc} ->
            {valid_acc,
             Map.put(result_acc, idx, {:error, "ERR flow id must be a non-empty string"})}
        end)

      valid = Enum.reverse(valid)

      valid_results =
        batch_quorum_commands(ctx, Enum.map(valid, fn {_idx, key, cmd} -> {key, cmd} end))

      indexed_results =
        valid
        |> Enum.map(fn {idx, _key, _cmd} -> idx end)
        |> Enum.zip(valid_results)
        |> Enum.reduce(indexed_results, fn {idx, result}, acc -> Map.put(acc, idx, result) end)

      for idx <- 0..(length(attrs_list) - 1), do: Map.fetch!(indexed_results, idx)
    else
      Enum.map(attrs_list, &flow_create(ctx, &1))
    end
  end

  @doc false
  def flow_create_many(ctx, partition_key, attrs_list)
      when is_binary(partition_key) and is_list(attrs_list) do
    key = Ferricstore.Flow.Keys.state_key("__batch__", partition_key)

    if byte_size(key) > @max_key_size do
      {:error, "ERR key too large (max #{@max_key_size} bytes)"}
    else
      idx = shard_for(ctx, key)
      raft_write(ctx, idx, key, {:flow_create_many, key, %{records: attrs_list}})
    end
  end

  def flow_create_many(ctx, nil, attrs_list) when is_list(attrs_list) do
    flow_many_by_shard(ctx, attrs_list, :flow_create_many, "__batch__")
  end

  defp flow_many_by_shard(ctx, attrs_list, command, batch_id)
       when command in [:flow_create_many, :flow_transition_many] do
    indexed =
      attrs_list
      |> Enum.with_index()
      |> Enum.map(fn {%{id: id, partition_key: partition_key} = attrs, idx}
                     when is_binary(id) and is_binary(partition_key) ->
        key = Ferricstore.Flow.Keys.state_key(id, partition_key)
        shard_idx = shard_for(ctx, key)
        {idx, shard_idx, attrs}
      end)

    groups =
      indexed
      |> Enum.group_by(fn {_idx, shard_idx, _attrs} -> shard_idx end)
      |> Enum.map(fn {shard_idx, group} ->
        attrs = Enum.map(group, fn {_idx, _shard_idx, attrs} -> attrs end)
        partition_key = attrs |> hd() |> Map.fetch!(:partition_key)
        key = Ferricstore.Flow.Keys.state_key(batch_id, partition_key)
        original_indices = Enum.map(group, fn {idx, _shard_idx, _attrs} -> idx end)
        {shard_idx, key, original_indices, {command, key, %{records: attrs}}}
      end)

    case Enum.find(groups, fn {_shard_idx, key, _indices, _cmd} ->
           byte_size(key) > @max_key_size
         end) do
      {_shard_idx, _key, indices, _cmd} ->
        error = {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        {:ok, flow_many_error_results(length(indexed), indices, error)}

      nil ->
        keyed_commands = Enum.map(groups, fn {_shard_idx, key, _indices, cmd} -> {key, cmd} end)
        group_results = batch_quorum_commands(ctx, keyed_commands)
        expand_flow_many_results(length(indexed), groups, group_results)
    end
  end

  defp flow_many_error_results(count, error_indices, error) do
    error_set = MapSet.new(error_indices)

    0..(count - 1)
    |> Enum.map(fn idx ->
      if MapSet.member?(error_set, idx), do: error, else: ErrorReasons.write_timeout_unknown()
    end)
  end

  defp expand_flow_many_results(count, groups, group_results) do
    expanded =
      group_results
      |> Enum.zip(groups)
      |> Enum.reduce(%{}, fn
        {{:ok, records}, {_shard_idx, _key, indices, _cmd}}, acc when is_list(records) ->
          indices
          |> Enum.zip(records)
          |> Enum.reduce(acc, fn {idx, record}, next -> Map.put(next, idx, record) end)

        {{:error, _reason} = error, {_shard_idx, _key, indices, _cmd}}, acc ->
          Enum.reduce(indices, acc, fn idx, next -> Map.put(next, idx, error) end)

        {other, {_shard_idx, _key, indices, _cmd}}, acc ->
          Enum.reduce(indices, acc, fn idx, next -> Map.put(next, idx, other) end)
      end)

    results =
      0..(count - 1)
      |> Enum.map(fn idx -> Map.get(expanded, idx, ErrorReasons.write_timeout_unknown()) end)

    {:ok, results}
  end

  @doc false
  def flow_claim_due(ctx, %{type: type, state: state, priority: priority} = attrs)
      when is_binary(type) and is_binary(state) and (is_integer(priority) or is_nil(priority)) do
    key =
      Ferricstore.Flow.Keys.due_key(type, state, priority || 0, Map.get(attrs, :partition_key))

    if byte_size(key) > @max_key_size do
      {:error, "ERR key too large (max #{@max_key_size} bytes)"}
    else
      idx = shard_for(ctx, key)
      raft_write(ctx, idx, key, {:flow_claim_due, key, attrs})
    end
  end

  @doc false
  def flow_complete(ctx, %{id: id} = attrs) when is_binary(id) do
    key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

    if byte_size(key) > @max_key_size do
      {:error, "ERR key too large (max #{@max_key_size} bytes)"}
    else
      idx = shard_for(ctx, key)
      raft_write(ctx, idx, key, {:flow_complete, key, attrs})
    end
  end

  @doc false
  def flow_transition(ctx, %{id: id} = attrs) when is_binary(id) do
    key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

    if byte_size(key) > @max_key_size do
      {:error, "ERR key too large (max #{@max_key_size} bytes)"}
    else
      idx = shard_for(ctx, key)
      raft_write(ctx, idx, key, {:flow_transition, key, attrs})
    end
  end

  @doc false
  def flow_transition_batch(_ctx, []), do: []

  def flow_transition_batch(ctx, attrs_list) when is_list(attrs_list) do
    if ctx.name == :default do
      {valid, indexed_results} =
        attrs_list
        |> Enum.with_index()
        |> Enum.reduce({[], %{}}, fn
          {%{id: id} = attrs, idx}, {valid_acc, result_acc} when is_binary(id) ->
            key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

            if byte_size(key) > @max_key_size do
              {valid_acc,
               Map.put(
                 result_acc,
                 idx,
                 {:error, "ERR key too large (max #{@max_key_size} bytes)"}
               )}
            else
              {[{idx, key, {:flow_transition, key, attrs}} | valid_acc], result_acc}
            end

          {_attrs, idx}, {valid_acc, result_acc} ->
            {valid_acc,
             Map.put(result_acc, idx, {:error, "ERR flow id must be a non-empty string"})}
        end)

      valid = Enum.reverse(valid)

      valid_results =
        batch_quorum_commands(ctx, Enum.map(valid, fn {_idx, key, cmd} -> {key, cmd} end))

      indexed_results =
        valid
        |> Enum.map(fn {idx, _key, _cmd} -> idx end)
        |> Enum.zip(valid_results)
        |> Enum.reduce(indexed_results, fn {idx, result}, acc -> Map.put(acc, idx, result) end)

      for idx <- 0..(length(attrs_list) - 1), do: Map.fetch!(indexed_results, idx)
    else
      Enum.map(attrs_list, &flow_transition(ctx, &1))
    end
  end

  @doc false
  def flow_transition_many(ctx, partition_key, attrs_list)
      when is_binary(partition_key) and is_list(attrs_list) do
    key = Ferricstore.Flow.Keys.state_key("__transition_batch__", partition_key)

    if byte_size(key) > @max_key_size do
      {:error, "ERR key too large (max #{@max_key_size} bytes)"}
    else
      idx = shard_for(ctx, key)
      raft_write(ctx, idx, key, {:flow_transition_many, key, %{records: attrs_list}})
    end
  end

  def flow_transition_many(ctx, nil, attrs_list) when is_list(attrs_list) do
    flow_many_by_shard(ctx, attrs_list, :flow_transition_many, "__transition_batch__")
  end

  @doc false
  def flow_retry(ctx, %{id: id} = attrs) when is_binary(id) do
    key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

    if byte_size(key) > @max_key_size do
      {:error, "ERR key too large (max #{@max_key_size} bytes)"}
    else
      idx = shard_for(ctx, key)
      raft_write(ctx, idx, key, {:flow_retry, key, attrs})
    end
  end

  @doc false
  def flow_fail(ctx, %{id: id} = attrs) when is_binary(id) do
    key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

    if byte_size(key) > @max_key_size do
      {:error, "ERR key too large (max #{@max_key_size} bytes)"}
    else
      idx = shard_for(ctx, key)
      raft_write(ctx, idx, key, {:flow_fail, key, attrs})
    end
  end

  @doc false
  def flow_cancel(ctx, %{id: id} = attrs) when is_binary(id) do
    key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

    if byte_size(key) > @max_key_size do
      {:error, "ERR key too large (max #{@max_key_size} bytes)"}
    else
      idx = shard_for(ctx, key)
      raft_write(ctx, idx, key, {:flow_cancel, key, attrs})
    end
  end

  @doc false
  def flow_rewind(ctx, %{id: id} = attrs) when is_binary(id) do
    key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

    if byte_size(key) > @max_key_size do
      {:error, "ERR key too large (max #{@max_key_size} bytes)"}
    else
      idx = shard_for(ctx, key)
      raft_write(ctx, idx, key, {:flow_rewind, key, attrs})
    end
  end

  @doc false
  def pfadd(ctx, key, elements) when is_binary(key) and is_list(elements) do
    forced_single_key_quorum(ctx, key, {:pfadd, key, elements})
  end

  @doc false
  def pfmerge(ctx, dest_key, source_keys)
      when is_binary(dest_key) and is_list(source_keys) and source_keys != [] do
    with {:ok, [_dest_sketch | source_sketches]} <-
           hll_read_sketches(ctx, [dest_key | source_keys]) do
      forced_single_key_quorum(ctx, dest_key, {:pfmerge, dest_key, source_sketches})
    end
  end

  def pfmerge(_ctx, _dest_key, _source_keys),
    do: {:error, "ERR wrong number of arguments for 'pfmerge' command"}

  @doc false
  def spop(ctx, key, count) when is_binary(key) and (is_nil(count) or is_integer(count)) do
    forced_single_key_quorum(ctx, key, {:spop, key, count})
  end

  @doc false
  def zpopmin(ctx, key, count) when is_binary(key) and is_integer(count) do
    forced_single_key_quorum(ctx, key, {:zpop, key, count, :min})
  end

  @doc false
  def zpopmax(ctx, key, count) when is_binary(key) and is_integer(count) do
    forced_single_key_quorum(ctx, key, {:zpop, key, count, :max})
  end

  @doc false
  def json_set(ctx, key, path, value, flags)
      when is_binary(key) and (is_binary(path) or is_list(path)) and is_binary(value) and
             is_list(flags) do
    forced_single_key_quorum(ctx, key, {:json_set, key, path, value, flags})
  end

  @doc false
  def json_del(ctx, key, path) when is_binary(key) and (is_binary(path) or is_list(path)) do
    forced_single_key_quorum(ctx, key, {:json_del, key, path})
  end

  @doc false
  def json_numincrby(ctx, key, path, increment)
      when is_binary(key) and (is_binary(path) or is_list(path)) and is_number(increment) do
    forced_single_key_quorum(ctx, key, {:json_numincrby, key, path, increment})
  end

  @doc false
  def json_arrappend(ctx, key, path, values)
      when is_binary(key) and (is_binary(path) or is_list(path)) and is_list(values) do
    forced_single_key_quorum(ctx, key, {:json_arrappend, key, path, values})
  end

  @doc false
  def json_toggle(ctx, key, path) when is_binary(key) and (is_binary(path) or is_list(path)) do
    forced_single_key_quorum(ctx, key, {:json_toggle, key, path})
  end

  @doc false
  def json_clear(ctx, key, path) when is_binary(key) and (is_binary(path) or is_list(path)) do
    forced_single_key_quorum(ctx, key, {:json_clear, key, path})
  end

  defp forced_single_key_quorum(ctx, key, command) do
    idx = shard_for(ctx, key)
    forced_quorum_write(ctx, idx, command, node())
  end

  @hll_wrongtype_error {:error,
                        "WRONGTYPE Operation against a key holding the wrong kind of value"}

  defp hll_read_sketches(ctx, keys) do
    with :ok <- hll_ensure_string_keys(ctx, keys) do
      ctx
      |> batch_get(keys)
      |> Enum.map(fn
        nil -> HLL.new()
        value -> value
      end)
      |> hll_validate_sketches()
    end
  end

  defp hll_ensure_string_keys(ctx, keys) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      if hll_compound_data_structure_key?(ctx, key) do
        {:halt, @hll_wrongtype_error}
      else
        {:cont, :ok}
      end
    end)
  end

  defp hll_compound_data_structure_key?(ctx, key) do
    compound_get(ctx, key, CompoundKey.type_key(key)) != nil and
      TypeRegistry.get_type(key, ctx) != "none"
  end

  defp hll_validate_sketches(sketches) do
    Enum.reduce_while(sketches, {:ok, []}, fn sketch, {:ok, acc} ->
      if HLL.valid_sketch?(sketch) do
        {:cont, {:ok, [sketch | acc]}}
      else
        {:halt, @hll_wrongtype_error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      {:error, _} = err -> err
    end
  end

  @doc """
  Atomically applies Redis SET options in Raft order.

  Unlike `put/4`, this keeps NX/XX/GET/KEEPTTL checks inside the state
  machine so concurrent conditional SETs serialize correctly.
  """
  @spec set(FerricStore.Instance.t(), binary(), binary(), map()) :: term()
  def set(ctx, key, value, opts) do
    cond do
      byte_size(key) > @max_key_size ->
        {:error, "ERR key too large (max #{@max_key_size} bytes)"}

      is_binary(value) and byte_size(value) >= @max_value_size ->
        {:error, "ERR value too large (max #{@max_value_size} bytes)"}

      true ->
        case check_keydir_full_for_set(ctx, key, opts) do
          :ok ->
            idx = shard_for(ctx, key)
            raft_write(ctx, idx, key, {:set, key, value, opts.expire_at_ms, opts})

          {:error, _} = err ->
            err
        end
    end
  end

  # Checks if the keydir is full. If so, only allows writes to existing keys.
  # Checks both `keydir_full?` (ETS-level memory guard) and `reject_writes?`
  # (noeviction policy with reject-level pressure). The Shard GenServer has its
  # own `reject_writes?` check in `handle_call({:put, ...})`, but when the
  # quorum bypass path is used, the Shard is skipped, so we must check here.
  # Reads from ctx.pressure_flags atomics instead of persistent_term.
  defp check_keydir_full(ctx, key) do
    keydir_full = :atomics.get(ctx.pressure_flags, 1) == 1
    reject_writes = :atomics.get(ctx.pressure_flags, 2) == 1

    if keydir_full or reject_writes do
      # Allow updates to existing keys — use ETS direct check
      if exists_fast?(ctx, key) do
        :ok
      else
        # Nudge MemoryGuard to run eviction immediately (async, non-blocking).
        # Without this, the next eviction cycle is up to 100ms away.
        Ferricstore.MemoryGuard.nudge()
        {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}
      end
    else
      :ok
    end
  end

  defp check_keydir_full_for_set(ctx, key, opts) do
    keydir_full = :atomics.get(ctx.pressure_flags, 1) == 1
    reject_writes = :atomics.get(ctx.pressure_flags, 2) == 1

    if keydir_full or reject_writes do
      existing? = exists_fast?(ctx, key)

      cond do
        existing? ->
          :ok

        opts.xx ->
          :ok

        true ->
          Ferricstore.MemoryGuard.nudge()
          {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}
      end
    else
      :ok
    end
  end

  @doc "Deletes `key`. Returns `:ok` whether or not the key existed."
  @spec delete(FerricStore.Instance.t(), binary()) :: :ok
  def delete(ctx, key) do
    idx = shard_for(ctx, key)

    raft_write(ctx, idx, key, {:delete, key})
  end

  @doc """
  Submits a server command through Raft for replication to all nodes.

  Server commands are opaque to the library — the state machine dispatches
  them via the `raft_apply_hook` callback on the Instance struct. Routed
  through shard 0 for consistent ordering.
  """
  @spec server_command(FerricStore.Instance.t(), term()) :: term()
  def server_command(ctx, command) do
    raft_write(ctx, 0, "__server__", {:server_command, command})
  end

  @doc """
  Routes a probabilistic data structure write command through Raft.
  """
  @spec prob_write(FerricStore.Instance.t(), tuple()) :: term()
  def prob_write(ctx, command) do
    key = extract_prob_key(command)
    idx = shard_for(ctx, key)
    raft_write(ctx, idx, key, command)
  end

  defp extract_prob_key({:bloom_create, key, _, _, _}), do: key
  defp extract_prob_key({:bloom_add, key, _, _}), do: key
  defp extract_prob_key({:bloom_madd, key, _, _}), do: key
  defp extract_prob_key({:cms_create, key, _, _}), do: key
  defp extract_prob_key({:cms_incrby, key, _}), do: key
  defp extract_prob_key({:cms_merge, dst_key, _, _, _}), do: dst_key
  defp extract_prob_key({:cuckoo_create, key, _, _}), do: key
  defp extract_prob_key({:cuckoo_add, key, _, _}), do: key
  defp extract_prob_key({:cuckoo_addnx, key, _, _}), do: key
  defp extract_prob_key({:cuckoo_del, key, _}), do: key
  defp extract_prob_key({:topk_create, key, _, _, _, _}), do: key
  defp extract_prob_key({:topk_add, key, _}), do: key
  defp extract_prob_key({:topk_incrby, key, _}), do: key

  @doc """
  Returns `true` if `key` exists and is not expired.

  Uses direct ETS lookup (no GenServer roundtrip) for hot and cold keys.
  A key is considered existing if it is in the keydir and not expired,
  regardless of whether its value is hot (in ETS) or cold (on disk only).
  """
  @spec exists?(FerricStore.Instance.t(), binary()) :: boolean()
  def exists?(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, val, 0, _lfu, _fid, _off, _vsize}] when val != nil ->
          true

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          true

        [{^key, val, exp, _lfu, _fid, _off, _vsize}] when exp > now and val != nil ->
          true

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          true

        [{^key, _val, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          false

        [] ->
          false
      end
    rescue
      ArgumentError -> keydir_unavailable(ctx, idx, :exists, false)
    end
  end

  @doc """
  Fast ETS-direct existence check for a key.

  Returns `true` if the key exists in ETS and is not expired, `false` otherwise.
  This bypasses the GenServer entirely, saving ~1-3us per call. Used in the
  hot write path (`check_keydir_full/2`) where we only need a boolean answer
  and can tolerate the fact that cold keys (value=nil but still in keydir)
  are correctly detected as existing.
  """
  @spec exists_fast?(FerricStore.Instance.t(), binary()) :: boolean()
  def exists_fast?(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, val, 0, _lfu, _fid, _off, _vsize}] when val != nil ->
          true

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          true

        [{^key, val, exp, _lfu, _fid, _off, _vsize}] when exp > now and val != nil ->
          true

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          true

        [{^key, _val, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          false

        [] ->
          false
      end
    rescue
      ArgumentError -> false
    end
  end

  @doc """
  Atomically increments the integer value of `key` by `delta`.

  If the key does not exist, it is set to `delta`. Returns `{:ok, new_integer}`
  on success or `{:error, reason}` if the value is not a valid integer.
  """
  @spec incr(FerricStore.Instance.t(), binary(), integer()) ::
          {:ok, integer()} | {:error, binary()}
  def incr(ctx, key, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:incr, key, delta})
  end

  @doc """
  Atomically increments the float value of `key` by `delta`.

  If the key does not exist, it is set to `delta`. Returns `{:ok, new_float_string}`
  on success or `{:error, reason}` if the value is not a valid float.
  """
  @spec incr_float(FerricStore.Instance.t(), binary(), float()) ::
          {:ok, binary()} | {:error, binary()}
  def incr_float(ctx, key, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:incr_float, key, delta})
  end

  @doc """
  Atomically appends `suffix` to the value of `key`.

  If the key does not exist, it is created with value `suffix`.
  Returns `{:ok, new_byte_length}`.
  """
  @spec append(FerricStore.Instance.t(), binary(), binary()) :: {:ok, non_neg_integer()}
  def append(ctx, key, suffix) do
    raft_write(ctx, shard_for(ctx, key), key, {:append, key, suffix})
  end

  @doc """
  Atomically gets the old value and sets a new value for `key`.

  Returns the old value, or `nil` if the key did not exist.
  """
  @spec getset(FerricStore.Instance.t(), binary(), binary()) :: binary() | nil
  def getset(ctx, key, value) do
    raft_write(ctx, shard_for(ctx, key), key, {:getset, key, value})
  end

  @doc """
  Atomically gets and deletes `key`.

  Returns the value, or `nil` if the key did not exist.
  """
  @spec getdel(FerricStore.Instance.t(), binary()) :: binary() | nil
  def getdel(ctx, key) do
    raft_write(ctx, shard_for(ctx, key), key, {:getdel, key})
  end

  @doc """
  Atomically gets the value and updates the expiry of `key`.

  `expire_at_ms` is an absolute Unix-epoch timestamp in milliseconds;
  pass `0` to persist (remove expiry). Returns the value, or `nil` if
  the key did not exist.
  """
  @spec getex(FerricStore.Instance.t(), binary(), non_neg_integer()) :: binary() | nil
  def getex(ctx, key, expire_at_ms) do
    raft_write(ctx, shard_for(ctx, key), key, {:getex, key, expire_at_ms})
  end

  @doc """
  Atomically overwrites part of the string at `key` starting at `offset`.

  Zero-pads if the key doesn't exist or the string is shorter than offset.
  Returns `{:ok, new_byte_length}`.
  """
  @spec setrange(FerricStore.Instance.t(), binary(), non_neg_integer(), binary()) ::
          {:ok, non_neg_integer()}
  def setrange(ctx, key, offset, value) do
    raft_write(ctx, shard_for(ctx, key), key, {:setrange, key, offset, value})
  end

  @doc """
  Atomically sets the bit at `offset` to `bit_val` (0 or 1). Returns the
  previous bit value (0 or 1). Extends the bitmap with zero bytes if
  necessary. Goes through Raft so concurrent SETBITs on the same key
  never lose updates — the state machine is the sole mutator.
  """
  @spec setbit(FerricStore.Instance.t(), binary(), non_neg_integer(), 0 | 1) :: 0 | 1
  def setbit(ctx, key, offset, bit_val) do
    raft_write(ctx, shard_for(ctx, key), key, {:setbit, key, offset, bit_val})
  end

  @doc """
  Atomically increments the integer value of hash field `field` in `key` by
  `delta`. Returns `{:ok, new_int}` or `{:error, reason}`. Shares ordering
  with the parent hash's shard (routes by the hash's redis_key).
  """
  @spec hincrby(FerricStore.Instance.t(), binary(), binary(), integer()) ::
          integer() | {:error, binary()}
  def hincrby(ctx, key, field, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:hincrby, key, field, delta})
  end

  @doc """
  Atomically increments the float value of hash field `field` in `key` by
  `delta`. Returns the new value as a string, or `{:error, reason}`.
  """
  @spec hincrbyfloat(FerricStore.Instance.t(), binary(), binary(), float()) ::
          binary() | {:error, binary()}
  def hincrbyfloat(ctx, key, field, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:hincrbyfloat, key, field, delta})
  end

  @doc """
  Atomically increments the score of `member` in the sorted set at `key` by
  `increment`. Returns the new score as a string.
  """
  @spec zincrby(FerricStore.Instance.t(), binary(), number(), binary()) ::
          binary() | {:error, binary()}
  def zincrby(ctx, key, increment, member) do
    raft_write(ctx, shard_for(ctx, key), key, {:zincrby, key, increment, member})
  end

  @doc "Returns all live (non-expired, non-deleted) keys across every shard."
  @spec keys(FerricStore.Instance.t()) :: [binary()]
  def keys(ctx) do
    sc = ctx.shard_count

    Enum.flat_map(0..(sc - 1), fn i ->
      case safe_read_call(ctx, i, :keys) do
        {:ok, keys} -> keys
        :unavailable -> []
      end
    end)
  end

  @doc "Returns the count of all live keys across every shard."
  @spec dbsize(FerricStore.Instance.t()) :: non_neg_integer()
  def dbsize(ctx) do
    sc = ctx.shard_count
    now = HLC.now_ms()

    Enum.reduce(0..(sc - 1), 0, fn i, acc ->
      acc + live_keydir_size(ctx, i, resolve_keydir(ctx, i), now)
    end)
  end

  defp live_keydir_size(ctx, idx, keydir, now) do
    {count, expired_keys} =
      :ets.foldl(
        fn
          {_key, _value, 0, _lfu, _fid, _off, _vsize}, {count, expired_keys} ->
            {count + 1, expired_keys}

          {_key, _value, exp, _lfu, _fid, _off, _vsize}, {count, expired_keys} when exp > now ->
            {count + 1, expired_keys}

          {key, _value, _exp, _lfu, _fid, _off, _vsize}, {count, expired_keys} ->
            {count, [key | expired_keys]}
        end,
        {0, []},
        keydir
      )

    Enum.each(expired_keys, fn key ->
      track_keydir_binary_delete(ctx, idx, keydir, key)
      :ets.delete(keydir, key)
    end)

    count
  rescue
    ArgumentError ->
      keydir_unavailable(ctx, idx, :dbsize, 0)
  end

  defp keydir_unavailable(ctx, idx, request, fallback) do
    emit_shard_unavailable(ctx, idx, request, :keydir_unavailable)
    fallback
  end

  @doc """
  Returns the current write version of the shard that owns `key`.

  Used by the WATCH/EXEC transaction mechanism to detect concurrent modifications.
  """
  @spec get_version(FerricStore.Instance.t(), binary()) :: non_neg_integer()
  def get_version(ctx, key) do
    idx = shard_for(ctx, key)

    case safe_read_call(ctx, idx, {:get_version, key}) do
      {:ok, version} -> version
      :unavailable -> shared_write_version(ctx, idx)
    end
  end

  defp shared_write_version(%{write_version: write_version}, idx) do
    size = :counters.info(write_version).size
    if idx < size, do: :counters.get(write_version, idx + 1), else: 0
  rescue
    _ -> 0
  end

  @doc """
  Returns a lightweight WATCH token for `key`.

  Hot keys use the value hash. Cold keys use their live keydir location and
  expiry, avoiding a large Bitcask read just to snapshot WATCH state.
  """
  @spec watch_token(FerricStore.Instance.t(), binary()) :: term()
  def watch_token(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          {:value, :erlang.phash2(value)}

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize, 0}

        [{^key, nil, 0, _lfu, :pending, _off, _vsize}] ->
          {:version, get_version(ctx, key)}

        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          {:value, :erlang.phash2(value), exp}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize, exp}

        [{^key, nil, exp, _lfu, :pending, _off, _vsize}] when exp > now ->
          {:version, get_version(ctx, key)}

        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          :missing

        [] ->
          :missing
      end
    rescue
      ArgumentError -> {:version, get_version(ctx, key)}
    end
  end

  @doc """
  Returns the keydir disk location for a key, or `:miss`.

  Reads the `{file_id, offset, value_size}` fields directly from the keydir
  ETS table without a GenServer roundtrip. Returns `{:ok, {fid, off, vsize}}`
  for live keys, or `:miss` if the key is not in the keydir or is expired.

  Used by sendfile zero-copy and STRLEN on cold keys.
  """
  @spec get_keydir_file_ref(FerricStore.Instance.t(), binary()) ::
          {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}} | :miss
  def get_keydir_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{_, _, 0, _, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          {:ok, {fid, off, vsize}}

        [{^key, nil, 0, _, fid, _off, _vsize}] when is_integer(fid) ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          :miss

        [{_, _, 0, _, _fid, _off, _vsize}] ->
          :miss

        [{_, _, exp, _, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          {:ok, {fid, off, vsize}}

        [{^key, nil, exp, _, fid, _off, _vsize}] when exp > now and is_integer(fid) ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          :miss

        [{_, _, exp, _, _fid, _off, _vsize}] when exp > now ->
          :miss

        [{^key, _, _exp, _, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          :miss

        [] ->
          :miss
      end
    rescue
      ArgumentError -> :miss
    end
  end

  # -------------------------------------------------------------------
  # Native command accessors
  # -------------------------------------------------------------------

  @spec cas(FerricStore.Instance.t(), binary(), binary(), binary(), non_neg_integer() | nil) ::
          1 | 0 | nil
  def cas(ctx, key, expected, new_value, ttl_ms) do
    expire_at_ms = if ttl_ms, do: HLC.now_ms() + ttl_ms, else: nil
    raft_write(ctx, shard_for(ctx, key), key, {:cas, key, expected, new_value, expire_at_ms})
  end

  @spec lock(FerricStore.Instance.t(), binary(), binary(), pos_integer()) ::
          :ok | {:error, binary()}
  def lock(ctx, key, owner, ttl_ms) do
    expire_at_ms = HLC.now_ms() + ttl_ms
    raft_write(ctx, shard_for(ctx, key), key, {:lock, key, owner, expire_at_ms})
  end

  @spec unlock(FerricStore.Instance.t(), binary(), binary()) :: 1 | {:error, binary()}
  def unlock(ctx, key, owner) do
    raft_write(ctx, shard_for(ctx, key), key, {:unlock, key, owner})
  end

  @spec extend(FerricStore.Instance.t(), binary(), binary(), pos_integer()) ::
          1 | {:error, binary()}
  def extend(ctx, key, owner, ttl_ms) do
    expire_at_ms = HLC.now_ms() + ttl_ms
    raft_write(ctx, shard_for(ctx, key), key, {:extend, key, owner, expire_at_ms})
  end

  @spec ratelimit_add(
          FerricStore.Instance.t(),
          binary(),
          pos_integer(),
          pos_integer(),
          pos_integer()
        ) :: [term()]
  def ratelimit_add(ctx, key, window_ms, max, count) do
    raft_write(
      ctx,
      shard_for(ctx, key),
      key,
      {:ratelimit_add, key, window_ms, max, count}
    )
  end

  # -------------------------------------------------------------------
  # Compound key operations
  # -------------------------------------------------------------------

  @spec compound_get(FerricStore.Instance.t(), binary(), binary()) :: binary() | nil
  def compound_get(ctx, redis_key, compound_key) do
    idx = shard_for(ctx, redis_key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, compound_key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, compound_key, lfu)
        value

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_async(path, offset, compound_key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, compound_key)
            warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
            value

          _ ->
            case safe_read_call(ctx, idx, {:compound_get, redis_key, compound_key}) do
              {:ok, value} -> value
              :unavailable -> nil
            end
        end

      _ ->
        case safe_read_call(ctx, idx, {:compound_get, redis_key, compound_key}) do
          {:ok, value} -> value
          :unavailable -> nil
        end
    end
  end

  @spec compound_batch_get(FerricStore.Instance.t(), binary(), [binary()]) :: [binary() | nil]
  def compound_batch_get(ctx, redis_key, compound_keys) do
    idx = shard_for(ctx, redis_key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    {results, fallback_keys} =
      Enum.map_reduce(compound_keys, [], fn compound_key, fallback_keys ->
        case ets_get_full(ctx, idx, keydir, compound_key, now) do
          {:hit, value, lfu} ->
            sampled_read_bookkeeping_fast(ctx, keydir, compound_key, lfu)
            {{:value, value}, fallback_keys}

          _ ->
            {:fallback, [compound_key | fallback_keys]}
        end
      end)

    fallback_values =
      case fallback_keys do
        [] ->
          []

        keys ->
          pending_keys = Enum.reverse(keys)

          case safe_read_call(ctx, idx, {:compound_batch_get, redis_key, pending_keys}) do
            {:ok, values} -> values
            :unavailable -> List.duplicate(nil, length(pending_keys))
          end
      end

    {values, []} =
      Enum.map_reduce(results, fallback_values, fn
        {:value, value}, remaining -> {value, remaining}
        :fallback, [value | remaining] -> {value, remaining}
      end)

    values
  end

  @spec compound_get_meta(FerricStore.Instance.t(), binary(), binary()) ::
          {binary(), non_neg_integer()} | nil
  def compound_get_meta(ctx, redis_key, compound_key) do
    idx = shard_for(ctx, redis_key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_meta_full(ctx, idx, keydir, compound_key, now) do
      {:hit, value, expire_at_ms, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, compound_key, lfu)
        {value, expire_at_ms}

      {:cold, file_id, offset, value_size, expire_at_ms}
      when valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_async(path, offset, compound_key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, compound_key)
            warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
            {value, expire_at_ms}

          _ ->
            case safe_read_call(ctx, idx, {:compound_get_meta, redis_key, compound_key}) do
              {:ok, meta} -> meta
              :unavailable -> nil
            end
        end

      _ ->
        case safe_read_call(ctx, idx, {:compound_get_meta, redis_key, compound_key}) do
          {:ok, meta} -> meta
          :unavailable -> nil
        end
    end
  end

  @spec compound_batch_get_meta(FerricStore.Instance.t(), binary(), [binary()]) ::
          [{binary(), non_neg_integer()} | nil]
  def compound_batch_get_meta(ctx, redis_key, compound_keys) do
    idx = shard_for(ctx, redis_key)

    case safe_read_call(ctx, idx, {:compound_batch_get_meta, redis_key, compound_keys}) do
      {:ok, metas} -> metas
      :unavailable -> List.duplicate(nil, length(compound_keys))
    end
  end

  @spec compound_put(FerricStore.Instance.t(), binary(), binary(), binary(), non_neg_integer()) ::
          :ok | {:error, term()}
  def compound_put(ctx, redis_key, compound_key, value, expire_at_ms) do
    idx = shard_for(ctx, redis_key)

    if ctx.name == :default do
      quorum_write(ctx, idx, {:compound_put, redis_key, compound_key, value, expire_at_ms})
    else
      safe_write_call(ctx, idx, {:compound_put, redis_key, compound_key, value, expire_at_ms})
    end
  end

  @spec compound_batch_put(
          FerricStore.Instance.t(),
          binary(),
          [{binary(), binary(), non_neg_integer()}]
        ) :: :ok | {:error, term()}
  def compound_batch_put(_ctx, _redis_key, []), do: :ok

  def compound_batch_put(ctx, redis_key, entries) do
    idx = shard_for(ctx, redis_key)

    if ctx.name == :default do
      quorum_write(ctx, idx, {:compound_batch_put, redis_key, entries})
    else
      safe_write_call(ctx, idx, {:compound_batch_put, redis_key, entries})
    end
  end

  @spec compound_delete(FerricStore.Instance.t(), binary(), binary()) :: :ok | {:error, term()}
  def compound_delete(ctx, redis_key, compound_key) do
    idx = shard_for(ctx, redis_key)

    if ctx.name == :default do
      quorum_write(ctx, idx, {:compound_delete, redis_key, compound_key})
    else
      safe_write_call(ctx, idx, {:compound_delete, redis_key, compound_key})
    end
  end

  @doc """
  Executes a list_op inline under a held latch. Called from local-origin
  helper paths.
  The latch guarantees exclusive access to the list's compound keys.

  Reserves Batcher capacity before touching origin-local compound state.
  That prevents overloaded Raft replication from exposing a local-only list
  mutation as successful.
  """
  @spec execute_list_op_inline(FerricStore.Instance.t(), non_neg_integer(), tuple()) :: term()
  def execute_list_op_inline(ctx, idx, {:list_op, key, operation} = cmd) do
    :telemetry.execute([:ferricstore, :rmw, :worker_list_op], %{}, %{shard_index: idx})

    if list_operation_mutating?(operation) do
      execute_mutating_list_op_inline(ctx, idx, cmd, fn ->
        do_execute_list_op_inline(ctx, idx, key, operation)
      end)
    else
      do_execute_list_op_inline(ctx, idx, key, operation)
    end
  end

  def execute_list_op_inline(ctx, idx, {:list_op_lmove, src_key, dst_key, from_dir, to_dir} = cmd) do
    execute_mutating_list_op_inline(ctx, idx, cmd, fn ->
      store = build_origin_compound_store(ctx, idx)

      checked_lmove(src_key, dst_key, store, from_dir, to_dir)
    end)
  end

  defp do_execute_list_op_inline(ctx, idx, key, operation) do
    store = build_origin_compound_store(ctx, idx)

    case ensure_list_type_for_operation(key, operation, store) do
      :ok ->
        # Resolve the module at runtime to avoid the compile-time cycle
        # ListOps → Ops → Router → ListOps. `list_ops_mod/0` returns an atom
        # that xref cannot trace through.
        :erlang.apply(list_ops_mod(), :execute, [key, store, operation])

      {:error, _} = err ->
        err
    end
  end

  defp execute_mutating_list_op_inline(ctx, idx, cmd, fun) do
    previous_rollback = Process.get(@async_list_rollback_key)
    Process.put(@async_list_rollback_key, %{ctx: ctx, idx: idx, originals: %{}})

    # Do not enqueue the async Raft command until the origin-local list write
    # succeeds. Large list elements can fail during Bitcask append; accepting
    # Raft first would let a client-visible error apply later via replication.
    try do
      result = fun.()

      cond do
        match?({:error, _}, result) ->
          rollback_async_list_originals(ctx, idx)
          result

        true ->
          case async_enqueue_to_raft(idx, cmd) do
            :ok ->
              bump_write_version(ctx, idx)
              result

            {:error, _} = error ->
              rollback_async_list_originals(ctx, idx)
              error
          end
      end
    after
      case previous_rollback do
        nil -> Process.delete(@async_list_rollback_key)
        previous -> Process.put(@async_list_rollback_key, previous)
      end
    end
  end

  defp list_operation_mutating?({:lrange, _, _}), do: false
  defp list_operation_mutating?(:llen), do: false
  defp list_operation_mutating?({:lindex, _}), do: false
  defp list_operation_mutating?({:lpos, _, _, _, _}), do: false
  defp list_operation_mutating?(_), do: true

  defp ensure_list_type_for_operation(key, operation, store)

  defp ensure_list_type_for_operation(key, {:lpush, _elements}, store),
    do: TypeRegistry.check_or_set(key, :list, store)

  defp ensure_list_type_for_operation(key, {:rpush, _elements}, store),
    do: TypeRegistry.check_or_set(key, :list, store)

  defp ensure_list_type_for_operation(key, _operation, store),
    do: TypeRegistry.check_type(key, :list, store)

  # The module atom is constructed at runtime from a string so it doesn't
  # appear as a BEAM atom-literal reference to the target module. This
  # breaks the compile-time dependency edge Router -> ListOps while keeping
  # the call semantically identical.
  @compile {:inline, list_ops_mod: 0}
  defp list_ops_mod, do: String.to_atom("Elixir.Ferricstore.Store.ListOps")

  # Build an origin-local compound store that ListOps.execute can drive.
  # Each put closes over the current active file (file_id, path); a file
  # rotation between put and submit is rare but harmless — the submission
  # carries the raw command, replicas apply against their own active file.
  defp build_origin_compound_store(ctx, idx) do
    keydir = elem(ctx.keydir_refs, idx)
    {_file_id, file_path, shard_data_path} = Ferricstore.Store.ActiveFile.get(ctx, idx)

    %{
      compound_get: fn _redis_key, compound_key ->
        origin_compound_get(ctx, idx, keydir, compound_key)
      end,
      compound_put: fn _redis_key, compound_key, value, exp ->
        record_async_list_original(ctx, idx, compound_key)
        install_rmw_value(ctx, idx, compound_key, value, exp)
      end,
      compound_delete: fn _redis_key, compound_key ->
        record_async_list_original(ctx, idx, compound_key)
        track_keydir_binary_delete(ctx, idx, keydir, compound_key)
        :ets.delete(keydir, compound_key)
        Ferricstore.Store.BitcaskWriter.delete(ctx, idx, file_path, compound_key)
        :ok
      end,
      compound_scan: fn _redis_key, prefix ->
        state = %{keydir: keydir, index: idx, instance_ctx: ctx}

        Ferricstore.Store.Shard.ETS.prefix_scan_entries(state, prefix, shard_data_path)
        |> Enum.sort_by(fn {field, _} -> field end)
      end,
      compound_count: fn _redis_key, prefix ->
        state = %{keydir: keydir, index: idx, instance_ctx: ctx}
        Ferricstore.Store.Shard.ETS.prefix_count_entries(state, prefix)
      end,
      exists?: fn k ->
        origin_key_exists?(ctx, idx, keydir, k)
      end
    }
  end

  defp record_async_list_original(ctx, idx, compound_key) do
    case Process.get(@async_list_rollback_key) do
      %{ctx: ^ctx, idx: ^idx, originals: originals} = rollback ->
        unless Map.has_key?(originals, compound_key) do
          Process.put(@async_list_rollback_key, %{
            rollback
            | originals:
                Map.put(originals, compound_key, snapshot_live_value(ctx, idx, compound_key))
          })
        end

      _ ->
        :ok
    end
  end

  defp rollback_async_list_originals(ctx, idx) do
    originals =
      case Process.get(@async_list_rollback_key) do
        %{ctx: ^ctx, idx: ^idx, originals: originals} -> originals
        _ -> %{}
      end

    Enum.each(originals, fn
      {compound_key, :missing} ->
        rollback_async_list_key_to_missing(ctx, idx, compound_key)

      {compound_key, {:value, value, expire_at_ms}} ->
        rollback_async_list_key_to_value(ctx, idx, compound_key, value, expire_at_ms)
    end)
  end

  defp rollback_async_list_key_to_missing(ctx, idx, compound_key) do
    keydir = elem(ctx.keydir_refs, idx)

    _ = append_delete_tombstone_nosync(ctx, idx, compound_key)
    Ferricstore.Store.BitcaskWriter.discard_pending(ctx, idx, compound_key)
    track_keydir_binary_delete(ctx, idx, keydir, compound_key)
    :ets.delete(keydir, compound_key)
  end

  defp rollback_async_list_key_to_value(ctx, idx, compound_key, value, expire_at_ms) do
    keydir = elem(ctx.keydir_refs, idx)
    disk_value = to_disk_binary(value)

    case nif_append_batch_with_file(ctx, idx, [{compound_key, disk_value, expire_at_ms}]) do
      {:ok, file_id, [{offset, _record_size}]} ->
        ets_value =
          case value do
            v when is_integer(v) ->
              Integer.to_string(v)

            v when is_float(v) ->
              Float.to_string(v)

            v when is_binary(v) ->
              if byte_size(v) > ctx.hot_cache_max_value_size, do: nil, else: v
          end

        track_keydir_binary_insert(ctx, idx, keydir, compound_key, ets_value)

        :ets.insert(
          keydir,
          {compound_key, ets_value, expire_at_ms, LFU.initial(), file_id, offset,
           byte_size(disk_value)}
        )

      {:error, _reason} ->
        :ok
    end
  end

  defp origin_compound_get(ctx, idx, keydir, compound_key) do
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, compound_key, now) do
      {:hit, value, _lfu} ->
        value

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)

        path =
          Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")

        case read_cold_async(path, offset, compound_key) do
          {:ok, value} when is_binary(value) ->
            warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
            value

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  defp origin_key_exists?(ctx, idx, keydir, key) do
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, _value, _lfu} ->
        true

      {:cold, file_id, _offset, value_size}
      when valid_cold_file_ref(file_id, value_size) ->
        true

      _ ->
        false
    end
  end

  @spec compound_scan(FerricStore.Instance.t(), binary(), binary()) :: [{binary(), binary()}]
  def compound_scan(ctx, redis_key, prefix) do
    idx = shard_for(ctx, redis_key)

    case safe_read_call(ctx, idx, {:compound_scan, redis_key, prefix}) do
      {:ok, results} -> results
      :unavailable -> []
    end
  end

  @spec compound_count(FerricStore.Instance.t(), binary(), binary()) :: non_neg_integer()
  def compound_count(ctx, redis_key, prefix) do
    idx = shard_for(ctx, redis_key)

    case safe_read_call(ctx, idx, {:compound_count, redis_key, prefix}) do
      {:ok, count} -> count
      :unavailable -> 0
    end
  end

  @spec zset_score_range(FerricStore.Instance.t(), binary(), term(), term(), boolean()) ::
          {:ok, [{binary(), float()}]} | :unavailable
  def zset_score_range(ctx, redis_key, min_bound, max_bound, reverse?) do
    idx = shard_for(ctx, redis_key)

    ctx
    |> safe_read_call(idx, {:zset_score_range, redis_key, min_bound, max_bound, reverse?})
    |> unwrap_zset_index_reply()
  end

  @spec zset_score_range_slice(
          FerricStore.Instance.t(),
          binary(),
          term(),
          term(),
          boolean(),
          non_neg_integer(),
          non_neg_integer() | :all
        ) ::
          {:ok, [{binary(), float()}]} | :unavailable
  def zset_score_range_slice(ctx, redis_key, min_bound, max_bound, reverse?, offset, count) do
    idx = shard_for(ctx, redis_key)

    ctx
    |> safe_read_call(
      idx,
      {:zset_score_range_slice, redis_key, min_bound, max_bound, reverse?, offset, count}
    )
    |> unwrap_zset_index_reply()
  end

  @spec zset_score_count(FerricStore.Instance.t(), binary(), term(), term()) ::
          {:ok, non_neg_integer()} | :unavailable
  def zset_score_count(ctx, redis_key, min_bound, max_bound) do
    idx = shard_for(ctx, redis_key)

    ctx
    |> safe_read_call(idx, {:zset_score_count, redis_key, min_bound, max_bound})
    |> unwrap_zset_index_reply()
  end

  @spec zset_rank_range(
          FerricStore.Instance.t(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          boolean()
        ) ::
          {:ok, [{binary(), float()}]} | :unavailable
  def zset_rank_range(ctx, redis_key, start_idx, stop_idx, reverse?) do
    idx = shard_for(ctx, redis_key)

    ctx
    |> safe_read_call(idx, {:zset_rank_range, redis_key, start_idx, stop_idx, reverse?})
    |> unwrap_zset_index_reply()
  end

  @spec zset_member_rank(FerricStore.Instance.t(), binary(), binary(), boolean()) ::
          {:ok, non_neg_integer() | nil} | :unavailable
  def zset_member_rank(ctx, redis_key, member, reverse?) do
    idx = shard_for(ctx, redis_key)

    ctx
    |> safe_read_call(idx, {:zset_member_rank, redis_key, member, reverse?})
    |> unwrap_zset_index_reply()
  end

  defp unwrap_zset_index_reply({:ok, {:ok, result}}), do: {:ok, result}
  defp unwrap_zset_index_reply(other), do: other

  @spec compound_delete_prefix(FerricStore.Instance.t(), binary(), binary()) :: :ok
  def compound_delete_prefix(ctx, redis_key, prefix) do
    idx = shard_for(ctx, redis_key)
    quorum_write(ctx, idx, {:compound_delete_prefix, redis_key, prefix})
  end

  # -------------------------------------------------------------------
  # List operations
  # -------------------------------------------------------------------

  @spec list_op(FerricStore.Instance.t(), binary(), term()) :: term()
  def list_op(ctx, key, {:lmove, destination, from_dir, to_dir}) do
    source_idx = shard_for(ctx, key)

    if source_idx == shard_for(ctx, destination) do
      raft_write(ctx, source_idx, key, {:list_op_lmove, key, destination, from_dir, to_dir})
    else
      Ferricstore.CrossShardOp.execute(
        [{key, :read_write}, {destination, :write}],
        fn unified_store ->
          checked_lmove(key, destination, unified_store, from_dir, to_dir)
        end,
        intent: %{command: :lmove, keys: %{source: key, dest: destination}},
        instance: ctx
      )
    end
  end

  def list_op(ctx, key, operation) do
    idx = shard_for(ctx, key)
    raft_write(ctx, idx, key, {:list_op, key, operation})
  end

  defp checked_lmove(source, destination, store, from_dir, to_dir) do
    with :ok <- TypeRegistry.check_type(source, :list, store) do
      case ListOps.read_meta(source, store) do
        nil ->
          nil

        {0, _, _} ->
          nil

        _meta ->
          with :ok <- TypeRegistry.check_or_set(destination, :list, store) do
            ListOps.execute_lmove(source, destination, store, from_dir, to_dir)
          end
      end
    end
  end
end
