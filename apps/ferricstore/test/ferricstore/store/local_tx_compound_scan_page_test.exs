defmodule Ferricstore.Store.LocalTxCompoundScanPageTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.{CompoundKey, LocalTxStore, Ops}
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  test "transaction-local collection paging bounds inspected catalog members" do
    keydir = :ets.new(:local_tx_scan_keydir, [:set, :public])
    index = :ets.new(:local_tx_scan_members, [:ordered_set, :public])
    CompoundMemberIndex.reset(index)

    state = %{
      keydir: keydir,
      compound_member_index: index,
      shard_data_path: nil,
      data_dir: System.tmp_dir!(),
      index: 0,
      promoted_instances: %{}
    }

    tx = %LocalTxStore{instance_ctx: nil, shard_index: 0, shard_state: state}
    key = "hash"
    prefix = CompoundKey.hash_prefix(key)

    Enum.each(["a", "b-one", "b-two"], fn field ->
      assert true =
               ShardETS.ets_insert(
                 state,
                 CompoundKey.hash_field(key, field),
                 "value-#{field}",
                 0
               )
    end)

    assert {:ok, {{:after, "a"}, []}} =
             Ops.compound_scan_page(tx, key, prefix, 0, 1, "b*", false)

    assert {:ok, {{:after, "b-one"}, [{"b-one", "value-b-one"}]}} =
             Ops.compound_scan_page(tx, key, prefix, {:after, "a"}, 1, "b*", false)
  end

  test "transaction-local collection count uses the authoritative member catalog" do
    keydir = :ets.new(:local_tx_count_keydir, [:set, :public])
    index = :ets.new(:local_tx_count_members, [:ordered_set, :public])
    CompoundMemberIndex.reset(index)

    state = %{
      keydir: keydir,
      compound_member_index: index,
      shard_data_path: nil,
      data_dir: System.tmp_dir!(),
      index: 0,
      promoted_instances: %{}
    }

    key = "hash"
    compound_key = CompoundKey.hash_field(key, "unindexed")
    :ets.insert(keydir, {compound_key, "value", 0, 0, 0, 0, 5})

    tx = %LocalTxStore{instance_ctx: nil, shard_index: 0, shard_state: state}

    assert 0 = Ops.compound_count(tx, key, CompoundKey.hash_prefix(key))
  end
end
