defmodule Ferricstore.Commands.ListCrossShardTest do
  @moduledoc false

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  describe "embedded cross-shard LMOVE" do
    test "moves one element between lists on different shards" do
      [source, destination] = ShardHelpers.keys_on_different_shards(2)
      ctx = FerricStore.Instance.get(:default)

      assert Router.shard_for(ctx, source) != Router.shard_for(ctx, destination)

      assert {:ok, 2} = FerricStore.rpush(source, ["first", "second"])
      assert {:ok, 1} = FerricStore.rpush(destination, ["existing"])

      assert {:ok, "first"} = FerricStore.lmove(source, destination, :left, :right)
      assert {:ok, ["second"]} = FerricStore.lrange(source, 0, -1)
      assert {:ok, ["existing", "first"]} = FerricStore.lrange(destination, 0, -1)
    end

    test "wrong-type destination does not pop the source list" do
      [source, destination] = ShardHelpers.keys_on_different_shards(2)
      ctx = FerricStore.Instance.get(:default)

      assert Router.shard_for(ctx, source) != Router.shard_for(ctx, destination)

      assert {:ok, 2} = FerricStore.rpush(source, ["first", "second"])
      assert :ok = FerricStore.set(destination, "not-a-list")

      assert {:error, message} = FerricStore.lmove(source, destination, :left, :right)
      assert message =~ "WRONGTYPE"

      assert {:ok, ["first", "second"]} = FerricStore.lrange(source, 0, -1)
      assert {:ok, "not-a-list"} = FerricStore.get(destination)
    end

    test "direct Router.list_op LMOVE wrong-type destination does not pop source" do
      [source, destination] = ShardHelpers.keys_on_different_shards(2)
      ctx = FerricStore.Instance.get(:default)

      assert Router.shard_for(ctx, source) != Router.shard_for(ctx, destination)

      assert {:ok, 2} = FerricStore.rpush(source, ["first", "second"])
      assert :ok = FerricStore.set(destination, "not-a-list")

      assert {:error, message} = Router.list_op(ctx, source, {:lmove, destination, :left, :right})
      assert message =~ "WRONGTYPE"

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

    test "missing source returns nil without touching a wrong-type destination" do
      [source, destination] = ShardHelpers.keys_on_different_shards(2)
      ctx = FerricStore.Instance.get(:default)

      assert Router.shard_for(ctx, source) != Router.shard_for(ctx, destination)

      assert :ok = FerricStore.set(destination, "not-a-list")

      assert {:ok, nil} = FerricStore.lmove(source, destination, :left, :right)
      assert {:ok, []} = FerricStore.lrange(source, 0, -1)
      assert {:ok, "not-a-list"} = FerricStore.get(destination)
    end
  end
end
