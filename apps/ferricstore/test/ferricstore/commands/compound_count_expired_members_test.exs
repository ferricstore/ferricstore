defmodule Ferricstore.Commands.CompoundCountExpiredMembersTest do
  @moduledoc """
  A collection's member count drops members whose own TTL passed. The member
  index cleans up a bounded number of them per count; a count with more of
  them due must still answer, not fail as a storage read.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.{CompoundKey, Router}
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @fields 5_000
  @ttl_ms 300

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{expired-members:#{System.unique_integer([:positive])}}"
    }
  end

  test "HLEN counts a hash whose expired fields exceed one cleanup pass", %{ctx: ctx, key: key} do
    names = Enum.map(1..@fields, &"f#{&1}")
    assert @fields == Dispatcher.dispatch("HSET", [key | Enum.flat_map(names, &[&1, "v"])], ctx)
    assert :ok = expire_fields(ctx, key, Enum.drop(names, 1))

    Process.sleep(@ttl_ms * 2)

    assert 1 == Dispatcher.dispatch("HLEN", [key], ctx)
    assert ["f1", "v"] == Dispatcher.dispatch("HGETALL", [key], ctx)
  end

  test "below feature level 5 the state machine's count keeps failing past one pass", %{
    ctx: ctx,
    key: key
  } do
    names = Enum.map(1..@fields, &"f#{&1}")
    assert @fields == Dispatcher.dispatch("HSET", [key | Enum.flat_map(names, &[&1, "v"])], ctx)
    assert :ok = expire_fields(ctx, key, Enum.drop(names, 1))

    Process.sleep(@ttl_ms * 2)

    idx = Router.shard_for(ctx, key)

    shard_state = %{
      keydir: elem(ctx.keydir_refs, idx),
      index: idx,
      instance_ctx: ctx,
      compound_member_index: CompoundMemberIndex.table_name(ctx.name, idx)
    }

    prefix = CompoundKey.hash_prefix(key)

    assert {:error, {:storage_read_failed, {:compound_count_failed, :limit_exceeded}}} =
             ShardETS.bounded_prefix_count_entries(shard_state, prefix)

    assert 1 == ShardETS.prefix_count_entries(shard_state, prefix)
  end

  # One HPEXPIRE per 1,000 fields stays inside a single entry's member budget.
  defp expire_fields(ctx, key, names) do
    names
    |> Enum.chunk_every(1_000)
    |> Enum.each(fn chunk ->
      replies =
        Dispatcher.dispatch(
          "HPEXPIRE",
          [key, Integer.to_string(@ttl_ms), "FIELDS", Integer.to_string(length(chunk)) | chunk],
          ctx
        )

      assert Enum.all?(replies, &(&1 == 1))
    end)
  end
end
