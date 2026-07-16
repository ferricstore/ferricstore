defmodule Ferricstore.Transaction.CrossShardIsolationTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.PreparedTransactionCoordinator, as: Coordinator
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    [first, second] = ShardHelpers.keys_on_different_shards(2)
    {:ok, first: first, second: second}
  end

  test "fixture keys belong to independent Raft groups", %{first: first, second: second} do
    ctx = FerricStore.Instance.get(:default)
    refute Router.shard_for(ctx, first) == Router.shard_for(ctx, second)
  end

  test "cross-shard SET is rejected before either write", %{first: first, second: second} do
    assert_crossslot(
      Coordinator.execute(
        [{"SET", [first, "first"]}, {"SET", [second, "second"]}],
        %{},
        nil
      )
    )

    ctx = FerricStore.Instance.get(:default)
    assert Router.get(ctx, first) == nil
    assert Router.get(ctx, second) == nil
  end

  test "cross-shard read-modify-write leaves existing values unchanged", %{
    first: first,
    second: second
  } do
    ctx = FerricStore.Instance.get(:default)
    :ok = Router.put(ctx, first, "10", 0)
    :ok = Router.put(ctx, second, "20", 0)

    assert_crossslot(Coordinator.execute([{"INCR", [first]}, {"DEL", [second]}], %{}, nil))

    assert Router.get(ctx, first) == "10"
    assert Router.get(ctx, second) == "20"
  end

  test "hash tags retain single-shard atomic transactions" do
    assert [:ok, :ok, "Alice"] =
             Coordinator.execute(
               [
                 {"SET", ["{user:42}:name", "Alice"]},
                 {"SET", ["{user:42}:email", "alice@example.com"]},
                 {"GET", ["{user:42}:name"]}
               ],
               %{},
               nil
             )
  end

  defp assert_crossslot(result) do
    assert result == {:error, "CROSSSLOT Keys in request don't hash to the same slot"}
  end
end
