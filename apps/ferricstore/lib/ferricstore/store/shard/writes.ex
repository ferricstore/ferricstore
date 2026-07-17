defmodule Ferricstore.Store.Shard.Writes do
  @moduledoc """
  Shard-local write handlers.

  These handlers remain for custom/direct instances and for the staged migration
  away from using `Shard` as a pass-through default quorum write proxy. For the
  default application instance, write ingress should go through
  `Ferricstore.Store.Router` or `Ferricstore.Raft.Backend` and be serialized by
  the selected Raft state machine.

  Do not add new default-instance write paths here. If a command is durable and
  user-visible, route it to the Raft backend so read-modify-write behavior is
  decided in apply order.
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.ValueCodec
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  require Logger

  # Maximum pending entries before triggering a synchronous flush.
  @max_pending_size 10_000
  @int64_max 9_223_372_036_854_775_807
  @int64_min -9_223_372_036_854_775_808

  # -------------------------------------------------------------------
  # WRITE-PATH handlers (return {:reply, result, state} or {:noreply, state})
  # -------------------------------------------------------------------

  @spec handle_put(binary(), term(), non_neg_integer(), GenServer.from(), map()) ::
          {:reply, term(), map()} | {:noreply, map()}
  @doc false
  def handle_put(key, value, expire_at_ms, from, state) do
    # Reject new-key writes when the keydir is at capacity (spec 2.4).
    # Updates to existing keys are always allowed regardless of memory pressure.
    existing = :ets.lookup(state.keydir, key)

    is_new =
      case existing do
        [] -> true
        _ -> false
      end

    if is_new and Ferricstore.MemoryGuard.reject_writes?() do
      Ferricstore.MemoryGuard.nudge()
      {:reply, {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}, state}
    else
      if state.raft? do
        Ferricstore.Raft.Batcher.write_async(state.index, {:put, key, value, expire_at_ms}, from)
        new_version = state.write_version + 1
        {:noreply, %{state | write_version: new_version}}
      else
        case persist_direct_value(state, key, value, expire_at_ms, existing) do
          {:ok, new_state} ->
            {:reply, :ok, new_state}

          {:error, reason, rolled_back_state} ->
            {:reply, {:error, reason}, rolled_back_state}
        end
      end
    end
  end

  @spec handle_delete(binary(), GenServer.from(), map()) ::
          {:reply, :ok, map()} | {:noreply, map()}
  @doc false
  def handle_delete(key, from, state) do
    if state.raft? do
      Ferricstore.Raft.Batcher.write_async(state.index, {:delete, key}, from)
      new_version = state.write_version + 1
      {:noreply, %{state | write_version: new_version}}
    else
      state = ShardFlush.await_in_flight(state)
      state = ShardFlush.flush_pending_sync(state)

      case NIF.v2_append_tombstone(state.active_file_path, key) do
        {:ok, _} ->
          state = ShardFlush.track_delete_dead_bytes(state, key)
          ShardETS.ets_delete_key(state, key)

          new_pending =
            case state.pending do
              [] -> []
              pending -> Enum.reject(pending, fn {k, _, _} -> k == key end)
            end

          new_version = state.write_version + 1

          {:reply, :ok,
           %{
             state
             | pending: new_pending,
               pending_count: length(new_pending),
               write_version: new_version
           }}

        {:error, reason} ->
          # Do NOT delete from ETS if the tombstone write failed —
          # the key would resurrect on restart (no tombstone on disk).
          Logger.error(
            "Shard #{state.index}: tombstone write failed for DELETE: #{inspect(reason)}"
          )

          {:reply, {:error, "ERR disk write failed: #{inspect(reason)}"}, state}
      end
    end
  end

  @spec handle_incr(binary(), integer(), GenServer.from(), map()) ::
          {:reply, term(), map()} | {:noreply, map()}
  @doc false
  def handle_incr(key, delta, from, state) do
    if state.raft? do
      handle_incr_raft(key, delta, from, state)
    else
      handle_incr_direct(key, delta, state)
    end
  end

  defp handle_incr_raft(key, delta, from, state) do
    # RMW must return the state-machine result. The serial apply/3 guarantees
    # atomicity and materializes the computed value.
    Ferricstore.Raft.Batcher.write_async_quorum(state.index, {:incr, key, delta}, from)
    {:noreply, %{state | write_version: state.write_version + 1}}
  end

  defp handle_incr_direct(key, delta, state) do
    case resolve_direct_rmw(state, key) do
      {:error, state} ->
        {:reply, {:error, "ERR cold read failed"}, state}

      {:ok, value, expire_at_ms, state} ->
        with {:ok, int_val} <- direct_integer_value(value),
             new_val = int_val + delta,
             true <- int64?(new_val),
             {:ok, new_state} <- persist_direct_value(state, key, new_val, expire_at_ms) do
          {:reply, {:ok, new_val}, new_state}
        else
          :error ->
            {:reply, {:error, "ERR value is not an integer or out of range"}, state}

          false ->
            {:reply, {:error, "ERR increment or decrement would overflow"}, state}

          {:error, reason, rolled_back_state} ->
            {:reply, {:error, reason}, rolled_back_state}
        end
    end
  end

  defp direct_integer_value(nil), do: {:ok, 0}
  defp direct_integer_value(value), do: ShardETS.coerce_integer(value)

  defp int64?(value), do: value >= @int64_min and value <= @int64_max

  @spec handle_incr_float(binary(), float(), GenServer.from(), map()) ::
          {:reply, term(), map()} | {:noreply, map()}
  @doc false
  def handle_incr_float(key, delta, from, state) do
    if state.raft? do
      handle_incr_float_raft(key, delta, from, state)
    else
      handle_incr_float_direct(key, delta, state)
    end
  end

  defp handle_incr_float_raft(key, delta, from, state) do
    Ferricstore.Raft.Batcher.write_async_quorum(state.index, {:incr_float, key, delta}, from)
    {:noreply, %{state | write_version: state.write_version + 1}}
  end

  defp handle_incr_float_direct(key, delta, state) do
    case resolve_direct_rmw(state, key) do
      {:error, state} ->
        {:reply, {:error, "ERR cold read failed"}, state}

      {:ok, value, expire_at_ms, state} ->
        with {:ok, float_val} <- direct_float_value(value),
             {:ok, new_val} <- ValueCodec.checked_float_add(float_val, delta),
             {:ok, new_state} <- persist_direct_value(state, key, new_val, expire_at_ms) do
          {:reply, {:ok, new_val}, new_state}
        else
          :overflow ->
            {:reply, {:error, "ERR increment would produce NaN or Infinity"}, state}

          :error ->
            {:reply, {:error, "ERR value is not a valid float"}, state}

          {:error, reason, rolled_back_state} ->
            {:reply, {:error, reason}, rolled_back_state}
        end
    end
  end

  defp direct_float_value(nil), do: {:ok, 0.0}
  defp direct_float_value(value), do: ShardETS.coerce_float(value)

  @spec handle_append(binary(), binary(), GenServer.from(), map()) ::
          {:reply, term(), map()} | {:noreply, map()}
  @doc false
  def handle_append(key, suffix, from, state) do
    if state.raft? do
      handle_append_raft(key, suffix, from, state)
    else
      handle_append_direct(key, suffix, state)
    end
  end

  defp handle_append_raft(key, suffix, from, state) do
    Ferricstore.Raft.Batcher.write_async_quorum(state.index, {:append, key, suffix}, from)
    {:noreply, %{state | write_version: state.write_version + 1}}
  end

  defp handle_append_direct(key, suffix, state) do
    case resolve_direct_rmw(state, key) do
      {:error, state} ->
        {:reply, {:error, "ERR cold read failed"}, state}

      {:ok, value, expire_at_ms, state} ->
        old_val = if is_nil(value), do: "", else: ShardETS.to_disk_binary(value)
        new_val = old_val <> suffix

        case persist_direct_value(state, key, new_val, expire_at_ms) do
          {:ok, new_state} ->
            {:reply, {:ok, byte_size(new_val)}, new_state}

          {:error, reason, rolled_back_state} ->
            {:reply, {:error, reason}, rolled_back_state}
        end
    end
  end

  @spec handle_getset(binary(), binary(), GenServer.from(), map()) ::
          {:reply, term(), map()} | {:noreply, map()}
  @doc false
  def handle_getset(key, new_value, from, state) do
    if state.raft? do
      handle_getset_raft(key, new_value, from, state)
    else
      handle_getset_direct(key, new_value, state)
    end
  end

  defp handle_getset_raft(key, new_value, from, state) do
    Ferricstore.Raft.Batcher.write_async_quorum(state.index, {:getset, key, new_value}, from)
    {:noreply, %{state | write_version: state.write_version + 1}}
  end

  defp handle_getset_direct(key, new_value, state) do
    case resolve_direct_rmw(state, key) do
      {:error, state} ->
        {:reply, {:error, "ERR cold read failed"}, state}

      {:ok, old, _expire_at_ms, state} ->
        case persist_direct_value(state, key, new_value, 0) do
          {:ok, new_state} ->
            {:reply, old, new_state}

          {:error, reason, rolled_back_state} ->
            {:reply, {:error, reason}, rolled_back_state}
        end
    end
  end

  @spec handle_getdel(binary(), GenServer.from(), map()) ::
          {:reply, term(), map()} | {:noreply, map()}
  @doc false
  def handle_getdel(key, from, state) do
    if state.raft? do
      handle_getdel_raft(key, from, state)
    else
      handle_getdel_direct(key, state)
    end
  end

  defp handle_getdel_raft(key, from, state) do
    Ferricstore.Raft.Batcher.write_async_quorum(state.index, {:getdel, key}, from)
    {:noreply, %{state | write_version: state.write_version + 1}}
  end

  defp handle_getdel_direct(key, state) do
    case resolve_direct_rmw(state, key) do
      {:error, state} ->
        {:reply, {:error, "ERR cold read failed"}, state}

      {:ok, nil, _expire_at_ms, state} ->
        {:reply, nil, state}

      {:ok, old, _expire_at_ms, state} ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case NIF.v2_append_tombstone(state.active_file_path, key) do
          {:ok, _} ->
            state = ShardFlush.track_delete_dead_bytes(state, key)
            ShardETS.ets_delete_key(state, key)

            new_pending =
              case state.pending do
                [] -> []
                pending -> Enum.reject(pending, fn {k, _, _} -> k == key end)
              end

            {:reply, old,
             %{
               state
               | pending: new_pending,
                 pending_count: length(new_pending),
                 write_version: state.write_version + 1
             }}

          {:error, reason} ->
            Logger.error(
              "Shard #{state.index}: tombstone write failed for GETDEL: #{inspect(reason)}"
            )

            {:reply, {:error, reason}, state}
        end
    end
  end

  @spec handle_getex(binary(), non_neg_integer(), GenServer.from(), map()) ::
          {:reply, term(), map()} | {:noreply, map()}
  @doc false
  def handle_getex(key, expire_at_ms, from, state) do
    if state.raft? do
      handle_getex_raft(key, expire_at_ms, from, state)
    else
      handle_getex_direct(key, expire_at_ms, state)
    end
  end

  defp handle_getex_raft(key, expire_at_ms, from, state) do
    Ferricstore.Raft.Batcher.write_async_quorum(state.index, {:getex, key, expire_at_ms}, from)
    {:noreply, %{state | write_version: state.write_version + 1}}
  end

  defp handle_getex_direct(key, expire_at_ms, state) do
    case resolve_direct_rmw(state, key) do
      {:error, state} ->
        {:reply, {:error, "ERR cold read failed"}, state}

      {:ok, value, _old_exp, state} when value != nil ->
        case persist_direct_value(state, key, value, expire_at_ms) do
          {:ok, new_state} ->
            {:reply, value, new_state}

          {:error, reason, rolled_back_state} ->
            {:reply, {:error, reason}, rolled_back_state}
        end

      {:ok, nil, _old_exp, state} ->
        {:reply, nil, state}
    end
  end

  @spec handle_setrange(binary(), non_neg_integer(), binary(), GenServer.from(), map()) ::
          {:reply, term(), map()} | {:noreply, map()}
  @doc false
  def handle_setrange(key, offset, value, from, state) do
    if state.raft? do
      handle_setrange_raft(key, offset, value, from, state)
    else
      handle_setrange_direct(key, offset, value, state)
    end
  end

  defp handle_setrange_raft(key, offset, value, from, state) do
    Ferricstore.Raft.Batcher.write_async_quorum(
      state.index,
      {:setrange, key, offset, value},
      from
    )

    {:noreply, %{state | write_version: state.write_version + 1}}
  end

  defp handle_setrange_direct(key, offset, value, state) do
    case resolve_direct_rmw(state, key) do
      {:error, state} ->
        {:reply, {:error, "ERR cold read failed"}, state}

      {:ok, old, expire_at_ms, state} ->
        old_val = if old == nil, do: "", else: ShardETS.to_disk_binary(old)
        new_val = apply_setrange(old_val, offset, value)

        case persist_direct_value(state, key, new_val, expire_at_ms) do
          {:ok, new_state} ->
            {:reply, {:ok, byte_size(new_val)}, new_state}

          {:error, reason, rolled_back_state} ->
            {:reply, {:error, reason}, rolled_back_state}
        end
    end
  end

  @spec handle_delete_prefix(binary(), map()) :: {:reply, :ok, map()}
  @doc false
  def handle_delete_prefix(prefix, state) do
    if state.raft? do
      :ok =
        ShardETS.prefix_each_key(state.keydir, prefix, fn key ->
          Ferricstore.Raft.Batcher.write(state.index, {:delete, key})
        end)

      new_version = state.write_version + 1
      {:reply, :ok, %{state | write_version: new_version}}
    else
      keys_to_delete = ShardETS.prefix_collect_keys(state.keydir, prefix)
      state = ShardFlush.await_in_flight(state)
      state = ShardFlush.flush_pending_sync(state)

      case tombstone_and_delete_keys(state, keys_to_delete) do
        {:ok, new_state} ->
          {:reply, :ok, %{new_state | write_version: new_state.write_version + 1}}

        {{:error, reason}, new_state} ->
          Logger.error("Shard #{state.index}: delete_prefix tombstone failed: #{inspect(reason)}")
          {:reply, {:error, reason}, new_state}
      end
    end
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  defp persist_direct_value(state, key, value, expire_at_ms) do
    persist_direct_value(state, key, value, expire_at_ms, :ets.lookup(state.keydir, key))
  end

  defp persist_direct_value(state, key, value, expire_at_ms, previous_entry) do
    previous_pending = state.pending
    previous_pending_count = Map.get(state, :pending_count, length(previous_pending))
    previous_write_version = state.write_version

    ShardETS.ets_insert(state, key, value, expire_at_ms, previous_entry)

    staged_state = %{
      state
      | pending: [{key, value, expire_at_ms} | previous_pending],
        pending_count: previous_pending_count + 1,
        write_version: previous_write_version + 1
    }

    {flush_attempted?, flushed_state} =
      cond do
        staged_state.pending_count > @max_pending_size ->
          flushed_state =
            staged_state
            |> ShardFlush.await_in_flight()
            |> ShardFlush.flush_pending_sync()

          {true, flushed_state}

        state.flush_in_flight == nil ->
          {true, ShardFlush.flush_pending(staged_state)}

        true ->
          {false, staged_state}
      end

    if flush_attempted? do
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
      {:ok, flushed_state}
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
    restore_direct_entry(state, key, previous_entry)

    state
    |> Map.put(:pending, previous_pending)
    |> Map.put(:pending_count, previous_pending_count)
    |> Map.put(:write_version, previous_write_version)
    |> Map.delete(:last_flush_error)
  end

  defp restore_direct_entry(state, key, []) do
    ShardETS.ets_delete_key(state, key)
  end

  defp restore_direct_entry(
         state,
         key,
         [{key, value, expire_at_ms, _lfu, file_id, offset, value_size}]
       )
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
              is_integer(value_size) and value_size >= 0 do
    ShardETS.ets_insert_with_location(
      state,
      key,
      value,
      expire_at_ms,
      file_id,
      offset,
      value_size
    )
  end

  defp restore_direct_entry(
         state,
         key,
         [{key, value, expire_at_ms, lfu, :pending, old_file_id, old_value_size}]
       ) do
    ShardETS.ets_insert(state, key, value, expire_at_ms)

    :ets.insert(
      state.keydir,
      {key, value, expire_at_ms, lfu, :pending, old_file_id, old_value_size}
    )
  end

  defp restore_direct_entry(state, key, [entry]) do
    ShardETS.ets_delete_key(state, key)
    :ets.insert(state.keydir, entry)
  end

  defp resolve_direct_rmw(state, key) do
    case ShardETS.ets_lookup_warm_result(state, key) do
      {:hit, value, expire_at_ms} ->
        {:ok, value, expire_at_ms, state}

      :expired ->
        {:ok, nil, 0, state}

      :miss ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case Ferricstore.Store.Shard.Reads.do_get_meta(state, key) do
          {:error, {:storage_read_failed, _reason}} -> {:error, state}
          {value, expire_at_ms} -> {:ok, value, expire_at_ms, state}
          nil -> {:ok, nil, 0, state}
        end

      {:error, :cold_read_failed} ->
        {:error, state}

      {:error, {:storage_read_failed, _reason}} ->
        {:error, state}
    end
  end

  defp tombstone_and_delete_keys(state, []), do: {:ok, state}

  defp tombstone_and_delete_keys(state, keys) do
    case append_tombstone_batch_sync(state.active_file_path, keys) do
      {:ok, _locations} ->
        next_state =
          Enum.reduce(keys, state, fn key, acc_state ->
            ShardFlush.track_delete_dead_bytes(acc_state, key)
          end)

        Enum.each(keys, fn key -> ShardETS.ets_delete_key(next_state, key) end)
        {:ok, next_state}

      {:error, reason} ->
        {{:error, reason}, state}
    end
  end

  defp append_tombstone_batch_sync(path, keys) do
    ops = Enum.map(keys, &{:delete, &1})

    case NIF.v2_append_ops_batch(path, ops) do
      {:ok, locations} ->
        with :ok <- validate_tombstone_locations(locations, length(keys)) do
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

  @spec apply_setrange(binary(), non_neg_integer(), binary()) :: binary()
  @doc false
  def apply_setrange(old, offset, value) do
    old_len = byte_size(old)
    val_len = byte_size(value)

    cond do
      val_len == 0 ->
        if offset > old_len do
          old <> :binary.copy(<<0>>, offset - old_len)
        else
          old
        end

      offset >= old_len ->
        padding = :binary.copy(<<0>>, offset - old_len)
        old <> padding <> value

      offset + val_len >= old_len ->
        binary_part(old, 0, offset) <> value

      true ->
        binary_part(old, 0, offset) <>
          value <>
          binary_part(old, offset + val_len, old_len - offset - val_len)
    end
  end
end
