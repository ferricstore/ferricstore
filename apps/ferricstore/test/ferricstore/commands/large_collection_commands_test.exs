defmodule Ferricstore.Commands.LargeCollectionCommandsTest do
  @moduledoc """
  Commands whose members come from stored data, not from the request (RENAME,
  COPY, the STORE forms, ZREMRANGEBY*, EXPIRE on a collection), work on
  collections past the per-command member bound once feature level 11 is
  active, as one replicated entry.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ShardHelpers

  @size 10_000

  # Levels never go down, so the raise happens in a data dir of its own.
  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    assert {:ok, %{active: 11}} = FerricStore.raise_cluster_feature_level(11)
    ctx = FerricStore.Instance.get(:default)
    assert @size > ctx.apply_context.compound_member_apply_budget

    tag = "large:{#{System.unique_integer([:positive])}}"
    keys = ~w(h l s z s2 d1 d2 d3 d4 g)
    on_exit(fn -> for suffix <- keys, do: FerricStore.del(tag <> suffix) end)
    %{ctx: ctx, k: &(tag <> &1)}
  end

  defp d(ctx, command, args), do: Dispatcher.dispatch(command, args, ctx)

  defp fill(ctx, k) do
    for chunk <- Enum.chunk_every(1..@size, 2_000) do
      for {command, args} <- [
            {"HSET", [k.("h") | Enum.flat_map(chunk, &["f#{&1}", "v#{&1}"])]},
            {"RPUSH", [k.("l") | Enum.map(chunk, &"e#{&1}")]},
            {"SADD", [k.("s") | Enum.map(chunk, &"m#{&1}")]},
            {"ZADD", [k.("z") | Enum.flat_map(chunk, &["#{&1}", "m#{&1}"])]}
          ] do
        reply = d(ctx, command, args)
        assert is_integer(reply), "#{command}: #{inspect(reply)}"
      end
    end

    :ok
  end

  test "RENAME moves a hash, list, set and sorted set past the member bound", %{ctx: ctx, k: k} do
    fill(ctx, k)

    for {from, to, length_command} <- [
          {"h", "d1", "HLEN"},
          {"l", "d2", "LLEN"},
          {"s", "d3", "SCARD"},
          {"z", "d4", "ZCARD"}
        ] do
      reply = d(ctx, "RENAME", [k.(from), k.(to)])
      assert :ok == reply, "RENAME #{from}: #{inspect(reply)}"
      assert @size == d(ctx, length_command, [k.(to)])
      assert 0 == d(ctx, "EXISTS", [k.(from)])
    end

    assert "v#{@size}" == d(ctx, "HGET", [k.("d1"), "f#{@size}"])
    assert "e#{@size}" == d(ctx, "LINDEX", [k.("d2"), "-1"])
    assert "#{@size}" == d(ctx, "ZSCORE", [k.("d4"), "m#{@size}"])
  end

  test "COPY copies a large hash and keeps the source", %{ctx: ctx, k: k} do
    fill(ctx, k)

    assert 1 == d(ctx, "COPY", [k.("h"), k.("d1")])
    assert @size == d(ctx, "HLEN", [k.("d1")])
    assert @size == d(ctx, "HLEN", [k.("h")])
    assert "v1" == d(ctx, "HGET", [k.("d1"), "f1"])
  end

  test "the set and sorted-set STORE forms write large results", %{ctx: ctx, k: k} do
    fill(ctx, k)
    # Half of the set again, so INTER and DIFF have large results too.
    for chunk <- Enum.chunk_every(1..div(@size, 2), 2_000),
        do: d(ctx, "SADD", [k.("s2") | Enum.map(chunk, &"m#{&1}")])

    assert @size == d(ctx, "SUNIONSTORE", [k.("d1"), k.("s"), k.("s2")])
    assert div(@size, 2) == d(ctx, "SINTERSTORE", [k.("d2"), k.("s"), k.("s2")])
    assert div(@size, 2) == d(ctx, "SDIFFSTORE", [k.("d3"), k.("s"), k.("s2")])
    assert @size == d(ctx, "SCARD", [k.("d1")])

    assert @size == d(ctx, "ZUNIONSTORE", [k.("d4"), "1", k.("z")])
    assert @size == d(ctx, "ZCARD", [k.("d4")])
    assert div(@size, 2) == d(ctx, "ZINTERSTORE", [k.("d1"), "2", k.("z"), k.("s2")])
    assert div(@size, 2) == d(ctx, "ZDIFFSTORE", [k.("d2"), "2", k.("z"), k.("s2")])
    assert @size == d(ctx, "ZRANGESTORE", [k.("d3"), k.("z"), "0", "-1"])
  end

  test "ZREMRANGEBYRANK removes more members than the bound", %{ctx: ctx, k: k} do
    fill(ctx, k)

    assert @size - 10 == d(ctx, "ZREMRANGEBYRANK", [k.("z"), "0", "#{@size - 11}"])
    assert 10 == d(ctx, "ZCARD", [k.("z")])
  end

  test "EXPIRE and PERSIST reach every member of a large collection", %{ctx: ctx, k: k} do
    fill(ctx, k)

    for key <- [k.("h"), k.("s"), k.("z"), k.("l")] do
      reply = d(ctx, "EXPIRE", [key, "1000"])
      assert 1 == reply, "EXPIRE #{key}: #{inspect(reply)}"
      assert d(ctx, "TTL", [key]) in 990..1000
      reply = d(ctx, "PERSIST", [key])
      assert 1 == reply, "PERSIST #{key}: #{inspect(reply)}"
      assert -1 == d(ctx, "TTL", [key])
    end
  end

  test "SET over a large collection replaces it", %{ctx: ctx, k: k} do
    fill(ctx, k)

    for {key, length_command} <- [{"h", "HLEN"}, {"l", "LLEN"}, {"s", "SCARD"}, {"z", "ZCARD"}] do
      reply = d(ctx, "SET", [k.(key), "plain"])
      assert :ok == reply, "SET over #{key}: #{inspect(reply)}"
      assert "plain" == d(ctx, "GET", [k.(key)])
      assert {:error, "WRONGTYPE" <> _} = d(ctx, length_command, [k.(key)])
    end
  end

  test "past the bulk bound RENAME is refused whole and DEL still removes the key", %{
    ctx: ctx,
    k: k
  } do
    over = 100_001

    for chunk <- Enum.chunk_every(1..over, 2_000),
        do: d(ctx, "HSET", [k.("h") | Enum.flat_map(chunk, &["f#{&1}", "v"])])

    assert {:error, "ERR too many members in one command (the limit is 100000)"} ==
             d(ctx, "RENAME", [k.("h"), k.("d1")])

    assert over == d(ctx, "HLEN", [k.("h")])
    assert 0 == d(ctx, "EXISTS", [k.("d1")])

    assert 1 == d(ctx, "DEL", [k.("h")])
    assert 0 == d(ctx, "HLEN", [k.("h")])
  end

  test "DEL and UNLINK remove large collections", %{ctx: ctx, k: k} do
    fill(ctx, k)

    assert 2 == d(ctx, "DEL", [k.("h"), k.("l")])
    assert 2 == d(ctx, "UNLINK", [k.("s"), k.("z")])
    assert 0 == d(ctx, "EXISTS", [k.("h"), k.("l"), k.("s"), k.("z")])
    assert 0 == d(ctx, "HLEN", [k.("h")])
  end
end
