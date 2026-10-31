defmodule Ferricstore.Commands.CollectionExpireTest do
  @moduledoc """
  From feature level 12, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT and PERSIST on a
  collection, and every EXPIRE with a flag, change the TTL in one compound
  command entry instead of a one-command transaction, whose journal cost
  20-40 ms a write.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Transaction.Coordinator

  @large 5_000

  # Levels never go down, so the raise happens in a data dir of its own.
  setup_all do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, %{active: 12}} = FerricStore.raise_cluster_feature_level(12)
    :ok
  end

  setup do
    ctx = FerricStore.Instance.get(:default)
    tag = "cexp:{#{System.unique_integer([:positive])}}"
    %{d: fn command, args -> Dispatcher.dispatch(command, args, ctx) end, k: &(tag <> &1)}
  end

  defp transactions(fun) do
    mfa = {Coordinator, :execute, 3}
    :erlang.trace_pattern(mfa, true, [:call_count])

    try do
      result = fun.()
      {:call_count, count} = :erlang.trace_info(mfa, :call_count)
      {count, result}
    after
      :erlang.trace_pattern(mfa, false, [:call_count])
    end
  end

  test "expiry on each collection type runs without a transaction", %{d: d, k: k} do
    1 = d.("HSET", [k.("h"), "f", "v"])
    1 = d.("RPUSH", [k.("l"), "e"])
    1 = d.("SADD", [k.("s"), "m"])
    1 = d.("ZADD", [k.("z"), "1", "m"])
    at_s = Integer.to_string(div(System.os_time(:millisecond), 1_000) + 500)
    at_ms = Integer.to_string(System.os_time(:millisecond) + 450_000)

    {count, replies} =
      transactions(fn ->
        for key <- ~w(h l s z) do
          {key,
           [
             d.("EXPIRE", [k.(key), "100"]),
             d.("PEXPIRE", [k.(key), "200000"]),
             d.("EXPIRE", [k.(key), "300", "GT"]),
             d.("EXPIRE", [k.(key), "50", "GT"]),
             d.("EXPIREAT", [k.(key), at_s]),
             d.("PEXPIREAT", [k.(key), at_ms, "LT"]),
             d.("PERSIST", [k.(key)]),
             d.("EXPIRE", [k.(key), "400", "NX"]),
             d.("EXPIRE", [k.(key), "500", "NX"])
           ]}
        end
      end)

    for {key, reply} <- replies, do: assert({key, [1, 1, 1, 0, 1, 1, 1, 1, 0]} == {key, reply})
    assert count == 0, "#{count} one-command transactions"

    for key <- ~w(h l s z) do
      ttl = d.("TTL", [k.(key)])
      assert ttl in 399..400, "#{key}: #{ttl}"
    end
  end

  test "EXPIRE with a flag on a string runs without a transaction", %{d: d, k: k} do
    :ok = d.("SET", [k.("str"), "v"])

    {count, replies} =
      transactions(fn ->
        [d.("EXPIRE", [k.("str"), "100", "NX"]), d.("EXPIRE", [k.("str"), "50", "GT"])]
      end)

    assert [1, 0] == replies
    assert count == 0
    assert "v" == d.("GET", [k.("str")])
    assert d.("TTL", [k.("str")]) in 99..100
  end

  test "a past time deletes the collection and a missing key answers 0", %{d: d, k: k} do
    2 = d.("HSET", [k.("h"), "a", "1", "b", "2"])
    assert 1 == d.("PEXPIREAT", [k.("h"), "1"])
    assert 0 == d.("EXISTS", [k.("h")])
    assert 0 == d.("EXPIRE", [k.("missing"), "100"])
    assert 0 == d.("PERSIST", [k.("missing")])
  end

  defp fill(d, k, size) do
    for chunk <- Enum.chunk_every(1..size, 500),
        {command, args} <- [
          {"HSET", [k.("h") | Enum.flat_map(chunk, &["f#{&1}", "v#{&1}"])]},
          {"RPUSH", [k.("l") | Enum.map(chunk, &"e#{&1}")]},
          {"SADD", [k.("s") | Enum.map(chunk, &"m#{&1}")]},
          {"ZADD", [k.("z") | Enum.flat_map(chunk, &["#{&1}", "m#{&1}"])]}
        ] do
      reply = d.(command, args)
      assert is_integer(reply), "#{command}: #{inspect(reply)}"
    end
  end

  test "collections past the member bound take and drop a TTL", %{d: d, k: k} do
    fill(d, k, @large)

    for {key, length_command} <- [{"h", "HLEN"}, {"l", "LLEN"}, {"s", "SCARD"}, {"z", "ZCARD"}] do
      assert 1 == d.("EXPIRE", [k.(key), "100"]), key
      assert d.("TTL", [k.(key)]) in 99..100
      assert 1 == d.("EXPIRE", [k.(key), "200", "GT"]), key
      assert 1 == d.("PERSIST", [k.(key)]), key
      assert -1 == d.("TTL", [k.(key)])
      assert @large == d.(length_command, [k.(key)])
    end
  end

  test "a TTL that runs out removes the whole collection", %{d: d, k: k} do
    fill(d, k, 2_000)
    for key <- ~w(h l s z), do: assert(1 == d.("PEXPIRE", [k.(key), "300", "NX"]), key)
    Process.sleep(600)

    for key <- ~w(h l s z) do
      assert 0 == d.("EXISTS", [k.(key)]), key
      # Nothing of the old collection is left to clash with a new type.
      assert 1 == d.("RPUSH", [k.(key), "fresh"]), key
    end
  end

  test "a stream takes and drops a TTL with its groups", %{d: d, k: k} do
    "1-0" = d.("XADD", [k.("x"), "1-0", "f", "v"])
    :ok = d.("XGROUP", ["CREATE", k.("x"), "g", "0"])
    [[_, [_]]] = d.("XREADGROUP", ~w(GROUP g c STREAMS) ++ [k.("x"), ">"])

    assert 1 == d.("EXPIRE", [k.("x"), "100"])
    assert d.("TTL", [k.("x")]) in 99..100
    assert 1 == d.("PERSIST", [k.("x")])
    assert -1 == d.("TTL", [k.("x")])
    assert [1, "1-0", "1-0", [["c", "1"]]] == d.("XPENDING", [k.("x"), "g"])
  end

  test "a probabilistic key still refuses an expiry", %{d: d, k: k} do
    1 = d.("BF.ADD", [k.("bf"), "item"])

    assert {:error, "ERR expiry is not supported on " <> _} =
             d.("EXPIRE", [k.("bf"), "100", "NX"])

    assert -1 == d.("TTL", [k.("bf")])
  end

  test "past the bulk bound EXPIRE is refused and the TTL is kept", %{d: d, k: k} do
    over = 100_001

    for chunk <- Enum.chunk_every(1..over, 1_000),
        do: d.("HSET", [k.("h") | Enum.flat_map(chunk, &["f#{&1}", "v"])])

    assert {:error, "ERR too many members in one command (the limit is 100000)"} ==
             d.("EXPIRE", [k.("h"), "100"])

    assert -1 == d.("TTL", [k.("h")])
    assert over == d.("HLEN", [k.("h")])
    assert 1 == d.("DEL", [k.("h")])
  end
end

defmodule Ferricstore.Commands.CollectionExpireBelowLevel12Test do
  @moduledoc "Below feature level 12 a compound expiry entry is refused, as older releases refuse it."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, %{active: 11}} = FerricStore.raise_cluster_feature_level(11)
    :ok
  end

  test "the entry changes nothing and EXPIRE runs as a transaction" do
    ctx = FerricStore.Instance.get(:default)
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end
    key = "cexp11:{#{System.unique_integer([:positive])}}h"
    1 = d.("HSET", [key, "f", "v"])
    at = System.os_time(:millisecond) + 100_000

    for ast <- [{:pexpireat, key, at}, {:pexpireat, key, at, :nx}, {:persist, key}] do
      assert {:error, "ERR this command requires cluster feature level 12"} ==
               Batcher.write(Router.shard_for(ctx, key), {:compound_command, key, ast})
    end

    assert -1 == d.("TTL", [key])
    assert 1 == d.("EXPIRE", [key, "100"])
    assert d.("TTL", [key]) in 99..100
  end
end
