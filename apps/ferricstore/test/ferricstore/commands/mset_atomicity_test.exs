defmodule Ferricstore.Commands.MSetAtomicityTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.Commands.Strings
  alias Ferricstore.Store.PublicationEpoch
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @crossslot {:error, "CROSSSLOT Keys in request don't hash to the same slot"}

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "MSET rejects cross-slot command and typed requests before either write" do
    ctx = FerricStore.Instance.get(:default)
    [first, second] = ShardHelpers.keys_on_different_shards(2)

    :ok = Router.put(ctx, first, "old-first", 0)
    :ok = Router.put(ctx, second, "old-second", 0)

    assert @crossslot == Strings.handle("MSET", [first, "new-first", second, "new-second"], ctx)
    assert "old-first" == Router.get(ctx, first)
    assert "old-second" == Router.get(ctx, second)

    assert @crossslot ==
             FerricStore.Impl.mset(ctx, [{first, "typed-first"}, {second, "typed-second"}])

    assert "old-first" == Router.get(ctx, first)
    assert "old-second" == Router.get(ctx, second)
  end

  test "public MSET rejects cross-slot maps before either write" do
    ctx = FerricStore.Instance.get(:default)
    [first, second] = ShardHelpers.keys_on_different_shards(2)

    assert @crossslot == FerricStore.mset(%{first => "first", second => "second"})
    assert nil == Router.get(ctx, first)
    assert nil == Router.get(ctx, second)
  end

  test "MSET commits same-slot pairs in one atomic batch" do
    ctx = FerricStore.Instance.get(:default)
    first = "mset:{account-42}:first"
    second = "mset:{account-42}:second"

    assert Router.shard_for(ctx, first) == Router.shard_for(ctx, second)
    assert :ok == Strings.handle("MSET", [first, "one", second, "two"], ctx)
    assert "one" == Router.get(ctx, first)
    assert "two" == Router.get(ctx, second)
  end

  test "MSETNX rejects cross-slot pairs before either write" do
    ctx = FerricStore.Instance.get(:default)
    [first, second] = ShardHelpers.keys_on_different_shards(2)

    assert @crossslot == Strings.handle("MSETNX", [first, "one", second, "two"], ctx)
    assert nil == Router.get(ctx, first)
    assert nil == Router.get(ctx, second)
  end

  test "MSET family rejects different slots that currently share one shard" do
    ctx = FerricStore.Instance.get(:default)
    {first, second} = same_shard_different_slot_keys(ctx)

    assert Router.shard_for(ctx, first) == Router.shard_for(ctx, second)
    refute Router.slot_for(ctx, first) == Router.slot_for(ctx, second)

    assert :ok = Router.put(ctx, first, "old-first", 0)
    assert :ok = Router.put(ctx, second, "old-second", 0)

    assert @crossslot == Strings.handle("MSET", [first, "new-first", second, "new-second"], ctx)
    assert "old-first" == Router.get(ctx, first)
    assert "old-second" == Router.get(ctx, second)

    assert :ok = Router.delete(ctx, first)
    assert :ok = Router.delete(ctx, second)

    assert @crossslot == Strings.handle("MSETNX", [first, "one", second, "two"], ctx)
    assert nil == Router.get(ctx, first)
    assert nil == Router.get(ctx, second)
  end

  test "concurrent same-slot MSETNX requests have exactly one winner" do
    ctx = FerricStore.Instance.get(:default)
    first = "msetnx:{account-99}:first"
    second = "msetnx:{account-99}:second"

    results =
      1..24
      |> Task.async_stream(
        fn attempt ->
          Strings.handle(
            "MSETNX",
            [first, "first-#{attempt}", second, "second-#{attempt}"],
            ctx
          )
        end,
        max_concurrency: 24,
        ordered: false,
        timeout: 10_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert 1 == Enum.count(results, &(&1 == 1))
    assert 23 == Enum.count(results, &(&1 == 0))

    assert String.replace_prefix(Router.get(ctx, first), "first-", "") ==
             String.replace_prefix(Router.get(ctx, second), "second-", "")
  end

  test "batch reads wait for a same-shard publication to become complete" do
    ctx = FerricStore.Instance.get(:default)
    first = "mset-publication:{account-100}:first"
    second = "mset-publication:{account-100}:second"
    shard_index = Router.shard_for(ctx, first)
    keydir = elem(ctx.keydir_refs, shard_index)

    assert :ok = Router.put(ctx, first, "old-first", 0)
    assert :ok = Router.put(ctx, second, "old-second", 0)

    token = PublicationEpoch.begin_write(ctx, shard_index)

    try do
      [{^first, _value, exp, lfu, file_id, offset, value_size}] = :ets.lookup(keydir, first)
      true = :ets.insert(keydir, {first, "new-first", exp, lfu, file_id, offset, value_size})

      reader = Task.async(fn -> Router.batch_get(ctx, [first, second]) end)
      assert Task.yield(reader, 50) == nil

      [{^second, _value, exp, lfu, file_id, offset, value_size}] = :ets.lookup(keydir, second)
      true = :ets.insert(keydir, {second, "new-second", exp, lfu, file_id, offset, value_size})
      :ok = PublicationEpoch.end_write(token)

      assert ["new-first", "new-second"] == Task.await(reader, 1_000)
    after
      PublicationEpoch.end_write_if_open(token)
      Router.delete(ctx, first)
      Router.delete(ctx, second)
    end
  end

  defp same_shard_different_slot_keys(ctx) do
    first = "mset:slot-collision:0"
    first_shard = Router.shard_for(ctx, first)
    first_slot = Router.slot_for(ctx, first)

    second =
      Enum.find_value(1..10_000, fn suffix ->
        candidate = "mset:slot-collision:#{suffix}"

        if Router.shard_for(ctx, candidate) == first_shard and
             Router.slot_for(ctx, candidate) != first_slot do
          candidate
        end
      end)

    {first, second}
  end
end
