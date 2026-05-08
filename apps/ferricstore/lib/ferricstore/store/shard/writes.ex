defmodule Ferricstore.Store.Shard.Writes do
  @moduledoc "Shard write-path handlers: put, delete, incr, append, getset, getdel, getex, and setrange with async flush and Raft support."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  require Logger

  # Maximum pending entries before triggering a synchronous flush.
  @max_pending_size 10_000

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
        ShardETS.ets_insert(state, key, value, expire_at_ms, existing)
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_count = state.pending_count + 1
        new_version = state.write_version + 1

        state =
          if new_count > @max_pending_size do
            s = %{state | pending: new_pending, pending_count: new_count}
            s = ShardFlush.await_in_flight(s)
            ShardFlush.flush_pending_sync(s)
          else
            %{state | pending: new_pending, pending_count: new_count}
          end

        new_state = %{state | write_version: new_version}

        if state.flush_in_flight == nil do
          flushed_state = ShardFlush.flush_pending(new_state)

          case Map.get(flushed_state, :last_flush_error) do
            nil ->
              {:reply, :ok, flushed_state}

            reason ->
              rolled_back = rollback_failed_direct_put(flushed_state, key, existing)
              {:reply, {:error, reason}, rolled_back}
          end
        else
          {:reply, :ok, new_state}
        end
      end
    end
  end

  defp rollback_failed_direct_put(state, key, existing) do
    ShardETS.ets_delete_key(state, key)

    case existing do
      [{^key, old_value, old_exp, _old_lfu, old_fid, old_off, old_vsize}]
      when is_integer(old_fid) and old_fid >= 0 and is_integer(old_off) and old_off >= 0 and
             is_integer(old_vsize) and old_vsize >= 0 ->
        ShardETS.ets_insert_with_location(
          state,
          key,
          old_value,
          old_exp,
          old_fid,
          old_off,
          old_vsize
        )

      [{^key, old_value, old_exp, _old_lfu, :pending, old_fid, old_vsize}] ->
        :ets.insert(
          state.keydir,
          {key, old_value, old_exp, LFU.initial(), :pending, old_fid, old_vsize}
        )

      _ ->
        :ok
    end

    new_pending =
      Enum.reject(state.pending, fn {pending_key, _value, _exp} -> pending_key == key end)

    state
    |> Map.put(:pending, new_pending)
    |> Map.put(:pending_count, length(new_pending))
    |> Map.delete(:last_flush_error)
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
      state = ShardFlush.track_delete_dead_bytes(state, key)

      case NIF.v2_append_tombstone(state.active_file_path, key) do
        {:ok, _} ->
          ShardETS.ets_delete_key(state, key)

          new_pending =
            case state.pending do
              [] -> []
              pending -> Enum.reject(pending, fn {k, _, _} -> k == key end)
            end

          new_version = state.write_version + 1
          {:reply, :ok, %{state | pending: new_pending, write_version: new_version}}

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
    case ShardETS.ets_lookup_warm(state, key) do
      {:hit, value, expire_at_ms} ->
        case ShardETS.coerce_integer(value) do
          {:ok, int_val} ->
            new_val = int_val + delta
            ShardETS.ets_insert(state, key, new_val, expire_at_ms)
            new_pending = [{key, new_val, expire_at_ms} | state.pending]
            new_version = state.write_version + 1
            new_state = %{state | pending: new_pending, write_version: new_version}

            new_state =
              if state.flush_in_flight == nil,
                do: ShardFlush.flush_pending(new_state),
                else: new_state

            {:reply, {:ok, new_val}, new_state}

          :error ->
            {:reply, {:error, "ERR value is not an integer or out of range"}, state}
        end

      :expired ->
        ShardETS.ets_insert(state, key, delta, 0)
        new_pending = [{key, delta, 0} | state.pending]
        new_version = state.write_version + 1
        new_state = %{state | pending: new_pending, write_version: new_version}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, {:ok, delta}, new_state}

      :miss ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case Ferricstore.Store.Shard.Reads.do_get(state, key) do
          nil ->
            ShardETS.ets_insert(state, key, delta, 0)
            new_pending = [{key, delta, 0} | state.pending]
            new_version = state.write_version + 1
            new_state = %{state | pending: new_pending, write_version: new_version}

            new_state =
              if state.flush_in_flight == nil,
                do: ShardFlush.flush_pending(new_state),
                else: new_state

            {:reply, {:ok, delta}, new_state}

          value ->
            expire_at_ms =
              case Ferricstore.Store.Shard.Reads.do_get_meta(state, key) do
                {_, exp} -> exp
                nil -> 0
              end

            case ShardETS.coerce_integer(value) do
              {:ok, int_val} ->
                new_val = int_val + delta
                ShardETS.ets_insert(state, key, new_val, expire_at_ms)
                new_pending = [{key, new_val, expire_at_ms} | state.pending]
                new_version = state.write_version + 1
                new_state = %{state | pending: new_pending, write_version: new_version}

                new_state =
                  if state.flush_in_flight == nil,
                    do: ShardFlush.flush_pending(new_state),
                    else: new_state

                {:reply, {:ok, new_val}, new_state}

              :error ->
                {:reply, {:error, "ERR value is not an integer or out of range"}, state}
            end
        end
    end
  end

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
    case ShardETS.ets_lookup_warm(state, key) do
      {:hit, value, expire_at_ms} ->
        case ShardETS.coerce_float(value) do
          {:ok, float_val} ->
            new_val = float_val + delta
            ShardETS.ets_insert(state, key, new_val, expire_at_ms)
            new_pending = [{key, new_val, expire_at_ms} | state.pending]
            new_state = %{state | pending: new_pending}

            new_state =
              if state.flush_in_flight == nil,
                do: ShardFlush.flush_pending(new_state),
                else: new_state

            {:reply, {:ok, new_val}, new_state}

          :error ->
            {:reply, {:error, "ERR value is not a valid float"}, state}
        end

      :expired ->
        new_val = delta * 1.0
        ShardETS.ets_insert(state, key, new_val, 0)
        new_pending = [{key, new_val, 0} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, {:ok, new_val}, new_state}

      :miss ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case Ferricstore.Store.Shard.Reads.do_get(state, key) do
          nil ->
            new_val = delta * 1.0
            ShardETS.ets_insert(state, key, new_val, 0)
            new_pending = [{key, new_val, 0} | state.pending]
            new_state = %{state | pending: new_pending}

            new_state =
              if state.flush_in_flight == nil,
                do: ShardFlush.flush_pending(new_state),
                else: new_state

            {:reply, {:ok, new_val}, new_state}

          value ->
            expire_at_ms =
              case Ferricstore.Store.Shard.Reads.do_get_meta(state, key) do
                {_, exp} -> exp
                nil -> 0
              end

            case ShardETS.coerce_float(value) do
              {:ok, float_val} ->
                new_val = float_val + delta
                ShardETS.ets_insert(state, key, new_val, expire_at_ms)
                new_pending = [{key, new_val, expire_at_ms} | state.pending]
                new_state = %{state | pending: new_pending}

                new_state =
                  if state.flush_in_flight == nil,
                    do: ShardFlush.flush_pending(new_state),
                    else: new_state

                {:reply, {:ok, new_val}, new_state}

              :error ->
                {:reply, {:error, "ERR value is not a valid float"}, state}
            end
        end
    end
  end

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
    case ShardETS.ets_lookup_warm(state, key) do
      {:hit, value, expire_at_ms} ->
        new_val = ShardETS.to_disk_binary(value) <> suffix
        ShardETS.ets_insert(state, key, new_val, expire_at_ms)
        new_pending = [{key, new_val, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, {:ok, byte_size(new_val)}, new_state}

      :expired ->
        ShardETS.ets_insert(state, key, suffix, 0)
        new_pending = [{key, suffix, 0} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, {:ok, byte_size(suffix)}, new_state}

      :miss ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        {old_val, expire_at_ms} =
          case Ferricstore.Store.Shard.Reads.do_get_meta(state, key) do
            {v, exp} -> {ShardETS.to_disk_binary(v), exp}
            nil -> {"", 0}
          end

        new_val = old_val <> suffix
        ShardETS.ets_insert(state, key, new_val, expire_at_ms)
        new_pending = [{key, new_val, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, {:ok, byte_size(new_val)}, new_state}
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
    {old, state} =
      case ShardETS.ets_lookup_warm(state, key) do
        {:hit, value, _expire_at_ms} ->
          {value, state}

        :expired ->
          {nil, state}

        :miss ->
          state = ShardFlush.await_in_flight(state)
          state = ShardFlush.flush_pending_sync(state)
          {Ferricstore.Store.Shard.Reads.do_get(state, key), state}
      end

    ShardETS.ets_insert(state, key, new_value, 0)
    new_pending = [{key, new_value, 0} | state.pending]
    new_state = %{state | pending: new_pending}

    new_state =
      if state.flush_in_flight == nil,
        do: ShardFlush.flush_pending(new_state),
        else: new_state

    {:reply, old, new_state}
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
    {old, state} =
      case ShardETS.ets_lookup_warm(state, key) do
        {:hit, value, _expire_at_ms} ->
          {value, state}

        :expired ->
          {nil, state}

        :miss ->
          state = ShardFlush.await_in_flight(state)
          state = ShardFlush.flush_pending_sync(state)
          {Ferricstore.Store.Shard.Reads.do_get(state, key), state}
      end

    if old != nil do
      state = ShardFlush.await_in_flight(state)
      state = ShardFlush.flush_pending_sync(state)
      state = ShardFlush.track_delete_dead_bytes(state, key)

      case NIF.v2_append_tombstone(state.active_file_path, key) do
        {:ok, _} ->
          ShardETS.ets_delete_key(state, key)

          new_pending =
            case state.pending do
              [] -> []
              pending -> Enum.reject(pending, fn {k, _, _} -> k == key end)
            end

          {:reply, old, %{state | pending: new_pending}}

        {:error, reason} ->
          Logger.error(
            "Shard #{state.index}: tombstone write failed for GETDEL: #{inspect(reason)}"
          )

          {:reply, {:error, reason}, state}
      end
    else
      {:reply, nil, state}
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
    case ShardETS.ets_lookup_warm(state, key) do
      {:hit, value, _old_exp} ->
        ShardETS.ets_insert(state, key, value, expire_at_ms)
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_state = %{state | pending: new_pending}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        {:reply, value, new_state}

      :expired ->
        {:reply, nil, state}

      :miss ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case Ferricstore.Store.Shard.Reads.do_get(state, key) do
          nil ->
            {:reply, nil, state}

          value ->
            ShardETS.ets_insert(state, key, value, expire_at_ms)
            new_pending = [{key, value, expire_at_ms} | state.pending]
            new_state = %{state | pending: new_pending}

            new_state =
              if state.flush_in_flight == nil,
                do: ShardFlush.flush_pending(new_state),
                else: new_state

            {:reply, value, new_state}
        end
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
    {old_val, expire_at_ms} =
      case ShardETS.ets_lookup_warm(state, key) do
        {:hit, v, exp} ->
          {ShardETS.to_disk_binary(v), exp}

        :expired ->
          {"", 0}

        :miss ->
          state = ShardFlush.await_in_flight(state)
          state = ShardFlush.flush_pending_sync(state)

          case Ferricstore.Store.Shard.Reads.do_get_meta(state, key) do
            {v, exp} -> {ShardETS.to_disk_binary(v), exp}
            nil -> {"", 0}
          end
      end

    new_val = apply_setrange(old_val, offset, value)
    ShardETS.ets_insert(state, key, new_val, expire_at_ms)
    new_pending = [{key, new_val, expire_at_ms} | state.pending]
    new_state = %{state | pending: new_pending}

    new_state =
      if state.flush_in_flight == nil,
        do: ShardFlush.flush_pending(new_state),
        else: new_state

    {:reply, {:ok, byte_size(new_val)}, new_state}
  end

  @spec handle_delete_prefix(binary(), map()) :: {:reply, :ok, map()}
  @doc false
  def handle_delete_prefix(prefix, state) do
    keys_to_delete = ShardETS.prefix_collect_keys(state.keydir, prefix)

    if state.raft? do
      Enum.each(keys_to_delete, fn key ->
        Ferricstore.Raft.Batcher.write(state.index, {:delete, key})
      end)

      new_version = state.write_version + 1
      {:reply, :ok, %{state | write_version: new_version}}
    else
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

  defp tombstone_and_delete_keys(state, []), do: {:ok, state}

  defp tombstone_and_delete_keys(state, keys) do
    next_state =
      Enum.reduce(keys, state, fn key, acc_state ->
        ShardFlush.track_delete_dead_bytes(acc_state, key)
      end)

    case append_tombstone_batch_sync(next_state.active_file_path, keys) do
      {:ok, _locations} ->
        Enum.each(keys, fn key -> ShardETS.ets_delete_key(next_state, key) end)
        {:ok, next_state}

      {:error, reason} ->
        {{:error, reason}, next_state}
    end
  end

  defp append_tombstone_batch_sync(path, keys) do
    ops = Enum.map(keys, &{:delete, &1})

    case NIF.v2_append_ops_batch_nosync(path, ops) do
      {:ok, locations} ->
        with :ok <- validate_tombstone_locations(locations, length(keys)),
             :ok <- NIF.v2_fsync(path) do
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
