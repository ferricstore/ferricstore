defmodule Ferricstore.Store.CompoundCommandTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.CompoundCommand

  @cross_shard_path Path.expand(
                      "../../../lib/ferricstore/cross_shard_op.ex",
                      __DIR__
                    )
  test "builds the single default Raft compound write contract" do
    redis_key = "hash"
    compound_key = "H:" <> redis_key <> <<0>> <> "field"
    entries = [{compound_key, "value", 123}]

    assert CompoundCommand.put(compound_key, "value", 123) ==
             {:compound_put, compound_key, "value", 123}

    assert CompoundCommand.batch_put(redis_key, entries) ==
             {:compound_batch_put, redis_key, entries}

    assert CompoundCommand.delete(compound_key) == {:compound_delete, compound_key}

    assert CompoundCommand.batch_delete(redis_key, [compound_key]) ==
             {:compound_batch_delete, redis_key, [compound_key]}

    assert CompoundCommand.delete_prefix("H:" <> redis_key <> <<0>>) ==
             {:compound_delete_prefix, "H:" <> redis_key <> <<0>>}
  end

  test "normalizes compound batch Raft replies consistently" do
    assert CompoundCommand.normalize_batch_reply(:ok) == :ok
    assert CompoundCommand.normalize_batch_reply({:ok, [:ok, :ok]}) == :ok

    assert CompoundCommand.normalize_batch_reply({:ok, [:ok, {:error, :disk_full}]}) ==
             {:error, :disk_full}

    assert CompoundCommand.normalize_batch_reply({:error, :unavailable}) ==
             {:error, :unavailable}

    assert CompoundCommand.normalize_batch_reply(:unexpected) == {:error, :unexpected}
  end

  test "cross-shard LocalTxStore routes compound prefix delete through Router" do
    source = File.read!(@cross_shard_path)

    refute source =~ "GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix}",
           "compound prefix delete must use Router so default Raft routing keeps the lean command contract"

    assert source =~ "Router.compound_delete_prefix(ctx, redis_key, prefix)"
  end

  test "Router default compound writes use the central command builder" do
    source = Ferricstore.Test.SourceFiles.router_source()

    assert source =~ "CompoundCommand.put(compound_key, value, expire_at_ms)"
    assert source =~ "CompoundCommand.batch_put(redis_key, entries)"
    assert source =~ "CompoundCommand.delete(compound_key)"
    assert source =~ "CompoundCommand.batch_delete(redis_key, compound_keys)"
    assert source =~ "CompoundCommand.delete_prefix(prefix)"

    refute source =~ "quorum_write(ctx, idx, {:compound_put"
    refute source =~ "|> quorum_write(idx, {:compound_batch_put"
    refute source =~ "quorum_write(ctx, idx, {:compound_delete,"
    refute source =~ "|> quorum_write(idx, {:compound_batch_delete"
    refute source =~ "quorum_write(ctx, idx, {:compound_delete_prefix"
  end
end
