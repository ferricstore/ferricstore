defmodule Ferricstore.Commands.ListCrossShardTest do
  @moduledoc false

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @crossslot {:error, "CROSSSLOT Keys in request don't hash to the same slot"}

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  describe "durable cross-group LMOVE" do
    test "rejects independent Raft groups without moving an element" do
      [source, destination] = ShardHelpers.keys_on_different_shards(2)
      ctx = FerricStore.Instance.get(:default)

      assert Router.shard_for(ctx, source) != Router.shard_for(ctx, destination)

      assert {:ok, 2} = FerricStore.rpush(source, ["first", "second"])
      assert {:ok, 1} = FerricStore.rpush(destination, ["existing"])

      assert @crossslot = FerricStore.lmove(source, destination, :left, :right)
      assert {:ok, ["first", "second"]} = FerricStore.lrange(source, 0, -1)
      assert {:ok, ["existing"]} = FerricStore.lrange(destination, 0, -1)
    end

    test "rejects topology before inspecting a wrong-type destination" do
      [source, destination] = ShardHelpers.keys_on_different_shards(2)
      ctx = FerricStore.Instance.get(:default)

      assert Router.shard_for(ctx, source) != Router.shard_for(ctx, destination)

      assert {:ok, 2} = FerricStore.rpush(source, ["first", "second"])
      assert :ok = FerricStore.set(destination, "not-a-list")

      assert @crossslot = FerricStore.lmove(source, destination, :left, :right)

      assert {:ok, ["first", "second"]} = FerricStore.lrange(source, 0, -1)
      assert {:ok, "not-a-list"} = FerricStore.get(destination)
    end

    test "direct Router.list_op rejects topology before destination type checks" do
      [source, destination] = ShardHelpers.keys_on_different_shards(2)
      ctx = FerricStore.Instance.get(:default)

      assert Router.shard_for(ctx, source) != Router.shard_for(ctx, destination)

      assert {:ok, 2} = FerricStore.rpush(source, ["first", "second"])
      assert :ok = FerricStore.set(destination, "not-a-list")

      assert @crossslot = Router.list_op(ctx, source, {:lmove, destination, :left, :right})

      assert {:ok, ["first", "second"]} = FerricStore.lrange(source, 0, -1)
      assert {:ok, "not-a-list"} = FerricStore.get(destination)
    end

    test "direct Router.list_op same-shard LMOVE wrong-type destination does not pop source" do
      {source, destination} = ShardHelpers.keys_on_same_shard()
      ctx = FerricStore.Instance.get(:default)

      assert Router.shard_for(ctx, source) == Router.shard_for(ctx, destination)

      assert {:ok, 2} = FerricStore.rpush(source, ["first", "second"])
      assert :ok = FerricStore.set(destination, "not-a-list")

      assert {:error, message} = Router.list_op(ctx, source, {:lmove, destination, :left, :right})
      assert message =~ "WRONGTYPE"

      assert {:ok, ["first", "second"]} = FerricStore.lrange(source, 0, -1)
      assert {:ok, "not-a-list"} = FerricStore.get(destination)
    end

    test "rejects topology even when the source is currently missing" do
      [source, destination] = ShardHelpers.keys_on_different_shards(2)
      ctx = FerricStore.Instance.get(:default)

      assert Router.shard_for(ctx, source) != Router.shard_for(ctx, destination)

      assert :ok = FerricStore.set(destination, "not-a-list")

      assert @crossslot = FerricStore.lmove(source, destination, :left, :right)
      assert {:ok, []} = FerricStore.lrange(source, 0, -1)
      assert {:ok, "not-a-list"} = FerricStore.get(destination)
    end
  end
end
