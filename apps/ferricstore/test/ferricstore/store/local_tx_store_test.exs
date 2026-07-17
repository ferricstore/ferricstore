defmodule Ferricstore.Store.LocalTxStoreTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.{LocalTxStore, Ops}
  alias Ferricstore.Store.Shard.ZSetIndex

  test "new preserves Raft zset index table names for transaction-local reads" do
    keydir = :ets.new(:local_tx_store_keydir, [:set, :public])
    suffix = System.unique_integer([:positive])
    index = :"local_tx_store_zset_index_#{suffix}"
    lookup = :"local_tx_store_zset_lookup_#{suffix}"
    :ets.new(index, [:ordered_set, :public, :named_table])
    :ets.new(lookup, [:set, :public, :named_table])

    state = %{
      shard_index: 0,
      ets: keydir,
      instance_ctx: nil,
      shard_data_path: "/tmp/local-tx-store",
      data_dir: "/tmp",
      zset_score_index_name: index,
      zset_score_lookup_name: lookup
    }

    try do
      tx = LocalTxStore.new(state)
      :ok = ZSetIndex.mark_ready_empty(index, lookup, "zs")
      :ok = ZSetIndex.put_member(index, lookup, "zs", "member", "1")

      assert tx.shard_state.zset_score_index == index
      assert tx.shard_state.zset_score_lookup == lookup
      assert {:ok, [{"member", 1.0}]} = Ops.zset_rank_range(tx, "zs", 0, 0, false)
    after
      :ets.delete(lookup)
      :ets.delete(index)
      :ets.delete(keydir)
    end
  end
end
