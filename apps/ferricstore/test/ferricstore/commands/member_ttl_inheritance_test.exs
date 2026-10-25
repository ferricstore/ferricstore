defmodule Ferricstore.Commands.MemberTtlInheritanceTest do
  @moduledoc """
  A key's TTL is stored on each of a collection's members. A member written
  after EXPIRE was stored without one, so when the TTL passed the key was gone
  (EXISTS 0) while reads still returned the late members, the sweep never
  removed them, and a later write to the key brought them back. From cluster
  feature level 6 a member written into a collection that has a TTL takes the
  key's TTL.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    assert {:ok, %{active: 6}} = FerricStore.raise_cluster_feature_level(6)
    :ok
  end

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{member-ttl:#{System.unique_integer([:positive])}}"
    }
  end

  defp cmd(ctx, name, args), do: Dispatcher.dispatch(name, args, ctx)

  # Sets a short TTL, runs `add`, then checks the late member left with the key.
  defp assert_late_member_expires(ctx, key, add, read, empty) do
    assert 1 == cmd(ctx, "PEXPIRE", [key, "300"])
    add.()
    assert cmd(ctx, "PTTL", [key]) > 0
    Process.sleep(450)

    assert 0 == cmd(ctx, "EXISTS", [key])
    assert empty == read.()
  end

  test "a hash field set after EXPIRE expires with the key", %{ctx: ctx, key: key} do
    assert 1 == cmd(ctx, "HSET", [key, "a", "1"])

    assert_late_member_expires(
      ctx,
      key,
      fn -> assert 1 == cmd(ctx, "HSET", [key, "b", "2"]) end,
      fn -> cmd(ctx, "HGETALL", [key]) end,
      []
    )

    assert 1 == cmd(ctx, "HSET", [key, "c", "3"])
    assert ["c", "3"] == cmd(ctx, "HGETALL", [key])
  end

  test "a hash field HINCRBY creates after EXPIRE expires with the key", %{ctx: ctx, key: key} do
    assert 1 == cmd(ctx, "HSET", [key, "a", "1"])

    assert_late_member_expires(
      ctx,
      key,
      fn -> assert 5 == cmd(ctx, "HINCRBY", [key, "n", "5"]) end,
      fn -> cmd(ctx, "HGETALL", [key]) end,
      []
    )
  end

  test "a set member added after EXPIRE expires with the key", %{ctx: ctx, key: key} do
    assert 1 == cmd(ctx, "SADD", [key, "a"])

    assert_late_member_expires(
      ctx,
      key,
      fn -> assert 1 == cmd(ctx, "SADD", [key, "b"]) end,
      fn -> cmd(ctx, "SMEMBERS", [key]) end,
      []
    )
  end

  test "a sorted set member added after EXPIRE expires with the key", %{ctx: ctx, key: key} do
    assert 1 == cmd(ctx, "ZADD", [key, "1", "a"])

    assert_late_member_expires(
      ctx,
      key,
      fn -> assert 1 == cmd(ctx, "ZADD", [key, "2", "b"]) end,
      fn -> cmd(ctx, "ZRANGE", [key, "0", "-1"]) end,
      []
    )
  end

  test "a sorted set member ZINCRBY creates after EXPIRE expires with the key",
       %{ctx: ctx, key: key} do
    assert 1 == cmd(ctx, "ZADD", [key, "1", "a"])

    assert_late_member_expires(
      ctx,
      key,
      fn -> assert "3" == cmd(ctx, "ZINCRBY", [key, "3", "b"]) end,
      fn -> cmd(ctx, "ZRANGE", [key, "0", "-1"]) end,
      []
    )
  end

  test "a list element pushed after EXPIRE expires with the key", %{ctx: ctx, key: key} do
    assert 1 == cmd(ctx, "RPUSH", [key, "a"])

    assert_late_member_expires(
      ctx,
      key,
      fn ->
        assert 2 == cmd(ctx, "RPUSH", [key, "b"])
        assert 3 == cmd(ctx, "LPUSH", [key, "c"])
      end,
      fn -> cmd(ctx, "LRANGE", [key, "0", "-1"]) end,
      []
    )
  end

  test "a stream entry added after EXPIRE expires with the key", %{ctx: ctx, key: key} do
    assert is_binary(cmd(ctx, "XADD", [key, "*", "f", "1"]))

    assert_late_member_expires(
      ctx,
      key,
      fn -> assert is_binary(cmd(ctx, "XADD", [key, "*", "f", "2"])) end,
      fn -> cmd(ctx, "XLEN", [key]) end,
      0
    )
  end

  test "a field HSETNX sets after EXPIRE expires with the key", %{ctx: ctx, key: key} do
    assert 1 == cmd(ctx, "HSET", [key, "a", "1"])

    assert_late_member_expires(
      ctx,
      key,
      fn -> assert 1 == cmd(ctx, "HSETNX", [key, "b", "2"]) end,
      fn -> cmd(ctx, "HGETALL", [key]) end,
      []
    )
  end

  test "a blob-sized hash field set after EXPIRE expires with the key", %{ctx: ctx, key: key} do
    blob = :binary.copy("b", ctx.blob_side_channel_threshold_bytes + 1)
    assert 1 == cmd(ctx, "HSET", [key, "a", "1"])

    assert_late_member_expires(
      ctx,
      key,
      fn -> assert 1 == cmd(ctx, "HSET", [key, "b", blob]) end,
      fn -> cmd(ctx, "HKEYS", [key]) end,
      []
    )
  end

  test "a member ZADD NX adds after EXPIRE expires with the key", %{ctx: ctx, key: key} do
    assert 1 == cmd(ctx, "ZADD", [key, "1", "a"])

    assert_late_member_expires(
      ctx,
      key,
      fn -> assert 1 == cmd(ctx, "ZADD", [key, "NX", "2", "b"]) end,
      fn -> cmd(ctx, "ZRANGE", [key, "0", "-1"]) end,
      []
    )
  end

  test "a member GEOADD NX adds after EXPIRE expires with the key", %{ctx: ctx, key: key} do
    assert 1 == cmd(ctx, "GEOADD", [key, "13.36", "38.11", "a"])

    assert_late_member_expires(
      ctx,
      key,
      fn -> assert 1 == cmd(ctx, "GEOADD", [key, "NX", "15.08", "37.50", "b"]) end,
      fn -> cmd(ctx, "ZRANGE", [key, "0", "-1"]) end,
      []
    )
  end

  test "a member SMOVE adds to a destination with a TTL expires with it", %{ctx: ctx, key: key} do
    source = key <> ":src"
    assert 1 == cmd(ctx, "SADD", [source, "m"])
    assert 1 == cmd(ctx, "SADD", [key, "a"])

    assert_late_member_expires(
      ctx,
      key,
      fn -> assert 1 == cmd(ctx, "SMOVE", [source, key, "m"]) end,
      fn -> cmd(ctx, "SMEMBERS", [key]) end,
      []
    )
  end

  test "members a transaction writes after EXPIRE expire with their keys", %{ctx: ctx, key: key} do
    keys = for t <- ~w(h z s l), do: "#{key}:#{t}"
    [h, z, s, l] = keys
    assert 1 == cmd(ctx, "HSET", [h, "a", "1"])
    assert 1 == cmd(ctx, "ZADD", [z, "1", "a"])
    assert 1 == cmd(ctx, "SADD", [s, "a"])
    assert 1 == cmd(ctx, "RPUSH", [l, "a"])
    for k <- keys, do: assert(1 == cmd(ctx, "PEXPIRE", [k, "300"]))

    assert {:ok, _results} =
             FerricStore.multi(fn tx ->
               tx
               |> FerricStore.Tx.hset(h, %{"b" => "2"})
               |> FerricStore.Tx.zadd(z, [{2, "b"}])
               |> FerricStore.Tx.sadd(s, ["b"])
               |> FerricStore.Tx.rpush(l, ["b"])
             end)

    Process.sleep(450)

    assert [] == cmd(ctx, "HGETALL", [h])
    assert [] == cmd(ctx, "ZRANGE", [z, "0", "-1"])
    assert [] == cmd(ctx, "SMEMBERS", [s])
    assert [] == cmd(ctx, "LRANGE", [l, "0", "-1"])
  end
end
