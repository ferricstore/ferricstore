Code.require_file(
  "promotion_test/sections/promotion_batches_shared_tombstones_after_dedicated_copy.exs",
  __DIR__
)

Code.require_file("promotion_test/sections/small_hash_stays_in_shared_bitcask.exs", __DIR__)
Code.require_file("promotion_test/sections/small_set_stays_in_shared_bitcask.exs", __DIR__)
Code.require_file("promotion_test/sections/small_sorted_set_stays_in_shared_bitcask.exs", __DIR__)

defmodule Ferricstore.Store.PromotionTest do
  @moduledoc false

  use ExUnit.Case, async: false
  @moduletag :global_state
  import ExUnit.CaptureLog

  alias Ferricstore.Commands.{Hash, List, PreparedCommand, Set, SortedSet, Strings}
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.HLC
  alias Ferricstore.Store.{CompoundKey, Promotion, Router}
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Transaction.ExecutionEntry

  # Use a very low threshold so we can trigger promotion in tests
  # without inserting hundreds of fields.
  @test_threshold 5

  setup_all do
    ShardHelpers.wait_shards_alive()

    apply_context_snapshot =
      ShardHelpers.replace_default_apply_context(promotion_threshold: @test_threshold)

    on_exit(fn ->
      ShardHelpers.restore_default_apply_context(apply_context_snapshot)
      ShardHelpers.wait_shards_alive()
    end)

    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
    :ok
  end

  use Ferricstore.Store.PromotionTest.Sections.PromotionBatchesSharedTombstonesAfterDedicatedCopy

  defp real_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      flush: fn -> :ok end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end,
      compound_get: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
      end
    }
  end

  defp ukey(base), do: "#{base}_#{:rand.uniform(9_999_999)}"

  defp prepared_tx_entry(command, args) do
    {:ok, prepared} = PreparedCommand.prepare(command, args)
    {:ok, entry} = ExecutionEntry.from_prepared(prepared)
    entry
  end

  # Inserts `n` fields into a hash and returns the key.
  defp populate_hash(store, key, n) do
    pairs =
      Enum.flat_map(1..n, fn i ->
        ["field_#{i}", "value_#{i}"]
      end)

    Hash.handle("HSET", [key | pairs], store)
    key
  end

  # Returns true if the given redis_key is promoted in its shard.
  defp promoted?(redis_key) do
    shard =
      Router.shard_name(
        FerricStore.Instance.get(:default),
        Router.shard_for(FerricStore.Instance.get(:default), redis_key)
      )

    GenServer.call(shard, {:promoted?, redis_key})
  end

  defp assert_promoted(redis_key) do
    ShardHelpers.eventually(
      fn -> promoted?(redis_key) end,
      "expected #{inspect(redis_key)} to finish promotion"
    )
  end

  defp promoted_state(shard, redis_key, attempts \\ 50)

  defp promoted_state(shard, redis_key, attempts) when attempts > 0 do
    state = :sys.get_state(shard)

    case state.promoted_instances[redis_key] || ShardCompound.promoted_store(state, redis_key) do
      nil ->
        Process.sleep(20)
        promoted_state(shard, redis_key, attempts - 1)

      %{path: _path} = promoted_instance ->
        {state, promoted_instance}

      path when is_binary(path) ->
        {state, %{path: path}}
    end
  end

  defp promoted_state(shard, redis_key, 0) do
    state = :sys.get_state(shard)

    flunk(
      "expected promoted instance for #{inspect(redis_key)}, got #{inspect(state.promoted_instances)}"
    )
  end

  defp state_machine_for_promoted_key(redis_key) do
    ctx = FerricStore.Instance.get(:default)
    idx = Router.shard_for(ctx, redis_key)
    shard = Router.shard_name(ctx, idx)
    shard_state = :sys.get_state(shard)

    state_machine =
      Ferricstore.Raft.StateMachine.init(%{
        shard_index: idx,
        data_dir: shard_state.data_dir,
        shard_data_path: shard_state.shard_data_path,
        active_file_id: shard_state.active_file_id,
        active_file_path: shard_state.active_file_path,
        active_file_size: shard_state.active_file_size,
        file_stats: shard_state.file_stats,
        ets: shard_state.ets,
        instance_ctx: ctx,
        instance_name: ctx.name,
        promoted_instances: shard_state.promoted_instances,
        apply_context: shard_state.apply_context,
        compound_member_index_name: shard_state.compound_member_index,
        zset_score_index_name: shard_state.zset_score_index,
        zset_score_lookup_name: shard_state.zset_score_lookup,
        logical_key_index_name: shard_state.logical_key_index,
        logical_key_slots_name: shard_state.logical_key_slots
      })

    {state_machine, ctx, shard}
  end

  # ---------------------------------------------------------------------------
  # Small hash stays in shared Bitcask (under threshold)
  # ---------------------------------------------------------------------------

  use Ferricstore.Store.PromotionTest.Sections.SmallHashStaysInSharedBitcask

  defp populate_set(store, key, n) do
    members = Enum.map(1..n, fn i -> "member_#{i}" end)
    Set.handle("SADD", [key | members], store)
    key
  end

  # ---------------------------------------------------------------------------
  # Small set stays in shared Bitcask (under threshold)
  # ---------------------------------------------------------------------------

  use Ferricstore.Store.PromotionTest.Sections.SmallSetStaysInSharedBitcask

  defp populate_zset(store, key, n) do
    pairs =
      Enum.flat_map(1..n, fn i ->
        [Integer.to_string(i), "member_#{i}"]
      end)

    SortedSet.handle("ZADD", [key | pairs], store)
    key
  end

  # ---------------------------------------------------------------------------
  # Small sorted set stays in shared Bitcask (under threshold)
  # ---------------------------------------------------------------------------

  use Ferricstore.Store.PromotionTest.Sections.SmallSortedSetStaysInSharedBitcask
end
