defmodule Ferricstore.Store.LocalTxStoreTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.{LocalTxStore, Ops}
  alias Ferricstore.Store.Shard.{CompoundMemberIndex, Transaction, ZSetIndex}

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

  @tag :local_tx_atomicity
  test "the removed mutable LocalTx execution path rejects before writing" do
    keydir = :ets.new(:local_tx_atomicity_keydir, [:set, :public])
    compound_index = :ets.new(:local_tx_atomicity_compound_index, [:ordered_set, :public])
    CompoundMemberIndex.reset(compound_index)

    read_key = "local-tx-budget-read"
    staged_key = "local-tx-budget-staged"
    :ets.insert(keydir, {read_key, "1234", 0, 0, 0, 0, 4})

    store = %LocalTxStore{
      instance_ctx: nil,
      shard_index: 0,
      shard_state: %{
        instance_ctx: nil,
        keydir: keydir,
        index: 0,
        data_dir: "/tmp",
        shard_data_path: "/tmp",
        promoted_instances: %{},
        compound_member_index: compound_index,
        zset_score_index: nil,
        zset_score_lookup: nil
      }
    }

    entries =
      Enum.map(
        [
          {"SET", [staged_key, "staged"]},
          {"GET", [read_key]}
        ],
        fn {command, args} ->
          {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
          {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
          entry
        end
      )

    assert {:error, :local_tx_store_not_supported} = Transaction.execute(entries, nil, store)

    assert [] = :ets.lookup(keydir, staged_key)
  end

  @tag :transaction_result_byte_budget
  test "result byte admission stops before an oversized improper tail" do
    assert {:error, :transaction_result_byte_budget_exceeded} =
             Transaction.__admit_result_bytes_for_test__(
               ["already-too-large" | :must_not_be_visited],
               4,
               0,
               0
             )
  end

  @tag :transaction_prepared_compound_work
  test "prepared compound fanout is rejected before dispatcher allocation or callbacks" do
    {:ok, prepared} =
      Ferricstore.Commands.PreparedCommand.prepare(
        "HSET",
        ["hash", "field-a", "one", "field-b", "two"]
      )

    {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)

    validation_only_store = %{
      transaction_command_budget: 1,
      transaction_key_apply_budget: 1,
      compound_member_apply_budget: 1
    }

    assert {:error, :transaction_compound_read_budget_exceeded} =
             Transaction.execute([entry], nil, validation_only_store)
  end
end
