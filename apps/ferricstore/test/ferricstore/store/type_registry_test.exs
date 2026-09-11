defmodule Ferricstore.Store.TypeRegistryTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.{CompoundKey, LocalTxStore, Promotion, ReadResult, TypeRegistry}
  alias Ferricstore.Store.Shard.CompoundMemberIndex

  test "check_or_set propagates type marker write errors" do
    store = %{
      exists?: fn "hash" -> false end,
      compound_get: fn "hash", _compound_key -> nil end,
      compound_put: fn "hash", _compound_key, "hash", 0 -> {:error, :disk_full} end
    }

    assert {:error, :disk_full} == TypeRegistry.check_or_set("hash", :hash, store)
  end

  test "check_or_set uses the store's atomic first-claim contract" do
    parent = self()

    store = %{
      compound_type_claim: fn "shared", type ->
        send(parent, {:claimed, type})
        {:ok, :created}
      end,
      compound_get: fn _redis_key, _compound_key ->
        flunk("an atomic type claim must not perform a separate marker read")
      end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms ->
        flunk("an atomic type claim must not perform a separate marker write")
      end
    }

    assert {:ok, :created} = TypeRegistry.check_or_set_status("shared", :hash, store)
    assert_receive {:claimed, :hash}
  end

  test "probabilistic type checks accept replay-stamped type markers" do
    marker = CompoundKey.encode_prob_type(:bloom, 42)

    store = %{
      compound_get: fn "filter", compound_key ->
        assert compound_key == CompoundKey.type_key("filter")
        marker
      end
    }

    assert :ok = TypeRegistry.check_or_set_status("filter", :bloom, store)
    assert :ok = TypeRegistry.serialized_claim_status("filter", :bloom, store)
    assert :ok = TypeRegistry.check_type("filter", :bloom, store)
  end

  test "rolling back a freshly created type claim does not read promotion state" do
    parent = self()
    type_key = CompoundKey.type_key("hash")

    store = %{
      compound_get: fn _redis_key, _compound_key ->
        flunk("a fresh type-claim rollback must not inspect promotion state")
      end,
      compound_delete: fn "hash", ^type_key ->
        send(parent, :type_deleted)
        :ok
      end
    }

    assert :ok = TypeRegistry.rollback_created_type("hash", store)
    assert_receive :type_deleted
  end

  test "promoted deletion fails closed without an owner-aware store" do
    keydir = :ets.new(:promoted_type_registry_keydir, [:set, :public])
    compound_index = :ets.new(:promoted_type_registry_compound_index, [:ordered_set, :public])
    :ok = CompoundMemberIndex.reset(compound_index)

    redis_key = "promoted-hash"
    marker_key = Promotion.marker_key(redis_key)
    type_key = CompoundKey.type_key(redis_key)
    member_key = CompoundKey.hash_field(redis_key, "field")

    marker = Promotion.encode_marker(:hash, :promoted, Promotion.new_generation())

    :ets.insert(keydir, [
      {marker_key, marker, 0, 0, 0, 0, byte_size(marker)},
      {type_key, "hash", 0, 0, 0, 0, 4},
      {member_key, "value", 0, 0, 0, 0, 5}
    ])

    tx = %LocalTxStore{
      instance_ctx: nil,
      shard_index: 0,
      shard_state: %{
        instance_ctx: nil,
        keydir: keydir,
        index: 0,
        data_dir: System.tmp_dir!(),
        shard_data_path: System.tmp_dir!(),
        promoted_instances: %{},
        compound_member_index: compound_index,
        zset_score_index: nil,
        zset_score_lookup: nil
      }
    }

    try do
      assert ReadResult.failure(:promoted_cleanup_requires_owner_context) ==
               TypeRegistry.delete_type(redis_key, tx)

      assert [{^marker_key, ^marker, 0, _lfu, 0, 0, _}] = :ets.lookup(keydir, marker_key)
      assert [{^type_key, "hash", 0, _lfu, 0, 0, 4}] = :ets.lookup(keydir, type_key)
      assert [{^member_key, "value", 0, _lfu, 0, 0, 5}] = :ets.lookup(keydir, member_key)
    after
      :ets.delete(compound_index)
      :ets.delete(keydir)
    end
  end
end
