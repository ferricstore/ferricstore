defmodule Ferricstore.Review.H3PromotedExpiryTest do
  @moduledoc """
  Proves that compound_get_meta hardcodes expiry=0 when recovering a promoted
  hash field from disk after an ETS miss (shard restart).

  Bug location: store/shard.ex, compound_get_meta promoted path.
  When the ETS cache misses, `promoted_read/3` returns only the value and the
  result is inserted with `ets_insert(state, compound_key, value, 0)` — always
  expiry 0. The actual expire_at_ms written to the Bitcask record is lost.

  Consequence: HTTL returns -1 (no expiry) after shard restart for fields that
  had a per-field TTL before the restart.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  @moduletag :shard_kill

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @test_threshold 5
  @ttl_seconds 300

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    apply_context_snapshot =
      ShardHelpers.replace_default_apply_context(promotion_threshold: @test_threshold)

    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.restore_default_apply_context(apply_context_snapshot)
      ShardHelpers.wait_shards_alive()
    end)
  end

  defp real_store, do: ShardHelpers.router_store()

  defp ukey(base), do: "h3_#{base}_#{:rand.uniform(9_999_999)}"

  defp populate_hash(store, key, n) do
    pairs = Enum.flat_map(1..n, fn i -> ["field_#{i}", "value_#{i}"] end)
    Hash.handle("HSET", [key | pairs], store)
    key
  end

  defp promoted?(redis_key) do
    shard =
      Router.shard_name(
        FerricStore.Instance.get(:default),
        Router.shard_for(FerricStore.Instance.get(:default), redis_key)
      )

    GenServer.call(shard, {:promoted?, redis_key})
  end

  describe "promoted hash field expiry survives shard restart" do
    test "HTTL returns remaining TTL (not -1) after shard kill + restart" do
      store = real_store()
      key = ukey("expiry_promoted")

      # 1. Create a promoted hash (> threshold fields).
      populate_hash(store, key, @test_threshold + 1)

      ShardHelpers.eventually(
        fn -> promoted?(key) end,
        "hash should be promoted before setting field expiry"
      )

      # 2. Set a per-field TTL on field_1 via HEXPIRE.
      ttl_str = Integer.to_string(@ttl_seconds)

      assert [1] =
               Hash.handle(
                 "HEXPIRE",
                 [key, ttl_str, "FIELDS", "1", "field_1"],
                 store
               )

      # Sanity: HTTL should return a positive TTL before any restart.
      [ttl_before] =
        Hash.handle("HTTL", [key, "FIELDS", "1", "field_1"], store)

      assert ttl_before > 0,
             "TTL should be positive before restart, got #{ttl_before}"

      # 3. Flush to disk and kill the owning shard.
      ShardHelpers.flush_all_shards()
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      ShardHelpers.kill_shard_safely(shard_idx)
      ShardHelpers.wait_shards_alive(30_000)

      # 4. The field value should still be readable after restart.
      ShardHelpers.eventually(
        fn -> Hash.handle("HGET", [key, "field_1"], store) == "value_1" end,
        "field_1 value should survive shard restart",
        200,
        200
      )

      # 5. HTTL should return a positive TTL, not -1.
      #    BUG: compound_get_meta returns {value, 0} for promoted keys after
      #    ETS miss, so HTTL sees expire_at_ms=0 and returns -1.
      [ttl_after] =
        Hash.handle("HTTL", [key, "FIELDS", "1", "field_1"], store)

      assert ttl_after > 0,
             "BUG: HTTL returned #{ttl_after} after shard restart " <>
               "(expected positive TTL, got -1 means expiry was lost)"
    end
  end
end
