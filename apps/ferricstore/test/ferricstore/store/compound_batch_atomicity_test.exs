defmodule Ferricstore.Store.CompoundBatchAtomicityTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{CompoundKey, LFU, Promotion}
  alias Ferricstore.Store.Shard.Compound.Ops
  alias Ferricstore.Store.Shard.Compound.Promoted
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    shard = elem(ctx.shard_names, 0)
    state = :sys.get_state(shard)
    redis_key = "atomic:promoted"
    type_key = CompoundKey.type_key(redis_key)
    field_key = CompoundKey.hash_field(redis_key, "field")
    marker_key = Promotion.marker_key(redis_key)

    invalid_dedicated = Path.join(ctx.data_dir, "invalid-dedicated")
    File.mkdir_p!(Path.join(invalid_dedicated, "00000.log"))

    :ets.insert(state.ets, {marker_key, "hash", 0, LFU.initial(), 0, 0, 4})

    promoted = %{
      path: invalid_dedicated,
      writes: 0,
      total_bytes: 0,
      dead_bytes: 0,
      last_compacted_at: nil
    }

    state = %{state | promoted_instances: %{redis_key => promoted}}

    {:ok, state: state, redis_key: redis_key, type_key: type_key, field_key: field_key}
  end

  test "failed promoted batch put does not publish shared metadata", ctx do
    :ets.insert(ctx.state.ets, {ctx.type_key, "hash", 0, LFU.initial(), 0, 0, 4})
    state = %{ctx.state | flush_in_flight: make_ref()}

    assert {:reply, {:error, _reason}, _state} =
             Ops.handle_compound_batch_put(
               ctx.redis_key,
               [{ctx.type_key, "set", 0}, {ctx.field_key, "new", 0}],
               state
             )

    assert [{_, "hash", _, _, _, _, _}] = :ets.lookup(state.ets, ctx.type_key)
    assert [] == :ets.lookup(state.ets, ctx.field_key)
  end

  test "failed promoted batch delete does not remove shared metadata", ctx do
    :ets.insert(ctx.state.ets, {ctx.type_key, "hash", 0, LFU.initial(), 0, 0, 4})
    :ets.insert(ctx.state.ets, {ctx.field_key, "value", 0, LFU.initial(), 0, 0, 5})

    assert {:reply, {:error, _reason}, failed_state} =
             Ops.handle_compound_batch_delete(
               ctx.redis_key,
               [ctx.type_key, ctx.field_key],
               ctx.state
             )

    assert [{_, "hash", _, _, _, _, _}] = :ets.lookup(ctx.state.ets, ctx.type_key)
    assert [{_, "value", _, _, _, _, _}] = :ets.lookup(ctx.state.ets, ctx.field_key)
    assert failed_state.promoted_instances == ctx.state.promoted_instances
  end

  test "failed shared tombstone batch does not account uncommitted dead bytes", ctx do
    :ets.insert(ctx.state.ets, {ctx.field_key, "value", 0, LFU.initial(), 0, 0, 5})
    invalid_log = Path.join(ctx.state.data_dir, "invalid-shared.log")
    File.mkdir_p!(invalid_log)
    state = %{ctx.state | active_file_path: invalid_log}

    assert {{:error, _reason}, failed_state} =
             Promoted.tombstone_and_delete_keys(state, [ctx.field_key])

    assert failed_state.file_stats == state.file_stats
    assert [{_, "value", _, _, _, _, _}] = :ets.lookup(state.ets, ctx.field_key)
  end

  test "failed shared single delete does not account uncommitted dead bytes", ctx do
    redis_key = "atomic:shared"
    compound_key = CompoundKey.hash_field(redis_key, "field")
    :ets.insert(ctx.state.ets, {compound_key, "value", 0, LFU.initial(), 0, 0, 5})
    invalid_log = Path.join(ctx.state.data_dir, "invalid-shared-single.log")
    File.mkdir_p!(invalid_log)

    state = %{
      ctx.state
      | active_file_path: invalid_log,
        promoted_instances: %{}
    }

    assert {:reply, {:error, _reason}, failed_state} =
             Ops.handle_compound_delete(redis_key, compound_key, state)

    assert failed_state.file_stats == state.file_stats
    assert [{_, "value", _, _, _, _, _}] = :ets.lookup(state.ets, compound_key)
  end

  test "successful promoted batch put enqueues one compaction check", ctx do
    redis_key = ctx.redis_key
    dedicated = valid_dedicated_path(ctx.state, ctx.redis_key, "put")

    state = %{
      ctx.state
      | promoted_instances: %{
          ctx.redis_key => promoted_info(dedicated)
        }
    }

    second_field = CompoundKey.hash_field(ctx.redis_key, "second")

    assert {:reply, :ok, new_state} =
             Ops.handle_compound_batch_put(
               ctx.redis_key,
               [{ctx.field_key, "first", 0}, {second_field, "second", 0}],
               state
             )

    assert new_state.write_version == state.write_version + 2
    assert_receive {:maybe_compact_promoted, key}
    assert key == ctx.redis_key
    refute_receive {:maybe_compact_promoted, ^redis_key}
  end

  test "successful promoted batch delete enqueues one compaction check", ctx do
    redis_key = ctx.redis_key
    dedicated = valid_dedicated_path(ctx.state, ctx.redis_key, "delete")
    second_field = CompoundKey.hash_field(ctx.redis_key, "second-delete")
    entries = [{ctx.field_key, "first", 0}, {second_field, "second", 0}]
    {:ok, locations} = NIF.v2_append_batch(Path.join(dedicated, "00000.log"), entries)

    Enum.zip(entries, locations)
    |> Enum.each(fn {{key, value, expire_at_ms}, {offset, value_size}} ->
      :ets.insert(ctx.state.ets, {key, value, expire_at_ms, LFU.initial(), 0, offset, value_size})
    end)

    state = %{
      ctx.state
      | promoted_instances: %{
          ctx.redis_key => promoted_info(dedicated)
        }
    }

    assert {:reply, :ok, new_state} =
             Ops.handle_compound_batch_delete(ctx.redis_key, [ctx.field_key, second_field], state)

    assert new_state.write_version == state.write_version + 2
    assert_receive {:maybe_compact_promoted, key}
    assert key == ctx.redis_key
    refute_receive {:maybe_compact_promoted, ^redis_key}
  end

  test "shared compound puts maintain the pending count while a flush is in flight", ctx do
    redis_key = "atomic:pending"
    first = CompoundKey.hash_field(redis_key, "first")
    second = CompoundKey.hash_field(redis_key, "second")

    state = %{
      ctx.state
      | flush_in_flight: make_ref(),
        promoted_instances: %{}
    }

    assert {:reply, :ok, one_state} =
             Ops.handle_compound_put(redis_key, first, "one", 0, state)

    assert one_state.pending_count == state.pending_count + 1

    assert {:reply, :ok, two_state} =
             Ops.handle_compound_batch_put(
               redis_key,
               [{second, "two", 0}, {CompoundKey.type_key(redis_key), "hash", 0}],
               one_state
             )

    assert two_state.pending_count == state.pending_count + 3
  end

  defp valid_dedicated_path(state, redis_key, suffix) do
    path = Path.join(state.data_dir, "valid-dedicated-#{redis_key}-#{suffix}")
    File.mkdir_p!(path)
    File.touch!(Path.join(path, "00000.log"))
    path
  end

  defp promoted_info(path) do
    %{
      path: path,
      writes: 0,
      total_bytes: 2_200_000,
      dead_bytes: 1_200_000,
      last_compacted_at: nil
    }
  end
end
