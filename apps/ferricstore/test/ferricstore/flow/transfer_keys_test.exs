defmodule Ferricstore.Flow.TransferKeysTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Flow.Transfer
  alias Ferricstore.Test.IsolatedInstance

  @moduletag :flow

  setup do
    source = IsolatedInstance.checkout()
    target = IsolatedInstance.checkout()

    path = Path.join(System.tmp_dir!(), "keys_transfer_#{System.unique_integer([:positive])}.ffx")

    on_exit(fn ->
      IsolatedInstance.checkin(source)
      IsolatedInstance.checkin(target)
      File.rm(path)
    end)

    {:ok, source: source, target: target, path: path}
  end

  defp run(store, command, args) do
    case Dispatcher.dispatch(command, args, store) do
      {:error, reason} -> flunk("#{command} #{inspect(args)} failed: #{inspect(reason)}")
      result -> result
    end
  end

  # One key of every kind a plain-key export carries: strings (and the types
  # stored as strings), every collection, a stream with a consumer group,
  # pending entries and a consumer, expiries, and the invocation catalog.
  defp build_keys!(store) do
    expire_at_ms = System.system_time(:millisecond) + 3_600_000

    run(store, "SET", ["k:string", "value"])
    run(store, "SET", ["k:expiring", "soon", "PXAT", Integer.to_string(expire_at_ms)])
    run(store, "SET", ["k:binary", <<0, 255, 1, 254>>])
    run(store, "SETBIT", ["k:bits", "13", "1"])
    run(store, "PFADD", ["k:hll", "a", "b", "c"])
    run(store, "TDIGEST.CREATE", ["k:tdigest"])
    run(store, "TDIGEST.ADD", ["k:tdigest", "1.5", "2.5", "9"])
    run(store, "HSET", ["k:hash", "f1", "v1", "f2", "v2"])
    run(store, "PEXPIREAT", ["k:hash", Integer.to_string(expire_at_ms)])
    run(store, "RPUSH", ["k:list", "a", "b", "c"])
    run(store, "LPUSH", ["k:list", "z"])
    run(store, "SADD", ["k:set", "m1", "m2", "m3"])
    run(store, "ZADD", ["k:zset", "1", "a", "2.5", "b", "-3", "c"])
    run(store, "GEOADD", ["k:geo", "13.361389", "38.115556", "Palermo"])
    first = run(store, "XADD", ["k:stream", "*", "f", "1"])
    run(store, "XADD", ["k:stream", "*", "f", "2"])
    run(store, "XGROUP", ["CREATE", "k:stream", "workers", "0"])
    run(store, "XREADGROUP", ["GROUP", "workers", "w1", "COUNT", "1", "STREAMS", "k:stream", ">"])
    run(store, "XACK", ["k:stream", "workers", first])
    run(store, "XREADGROUP", ["GROUP", "workers", "w1", "COUNT", "1", "STREAMS", "k:stream", ">"])

    definition = %{"name" => "billing", "flow" => %{"type" => "bill", "state" => "queued"}}
    "OK" = run(store, "INVOCATION.DEFINITION.PUT", [Jason.encode!(definition)])
    :ok
  end

  @user_keys ~w(k:string k:expiring k:binary k:bits k:hll k:tdigest k:hash k:list k:set
                k:zset k:geo k:stream)

  # Everything a client can read back about those keys.
  defp snapshot(store) do
    keys =
      Map.new(@user_keys, fn key ->
        type = type(store, key)

        {key,
         %{
           type: type,
           expire: run(store, "PEXPIRETIME", [key]),
           content: content(store, key, type)
         }}
      end)

    %{
      keys: keys,
      dbsize: run(store, "DBSIZE", []),
      invocations: run(store, "INVOCATION.DEFINITION.LIST", [])
    }
  end

  defp type(store, key) do
    case run(store, "TYPE", [key]) do
      {:simple, type} -> type
      type -> type
    end
  end

  defp content(store, key, "string") do
    case key do
      "k:hll" -> run(store, "PFCOUNT", [key])
      "k:tdigest" -> run(store, "TDIGEST.QUANTILE", [key, "0.5"])
      _other -> run(store, "GET", [key])
    end
  end

  defp content(store, key, "hash"), do: Enum.sort(run(store, "HGETALL", [key]))
  defp content(store, key, "list"), do: run(store, "LRANGE", [key, "0", "-1"])
  defp content(store, key, "set"), do: Enum.sort(run(store, "SMEMBERS", [key]))

  defp content(store, "k:geo" = key, "zset"),
    do: {run(store, "GEOPOS", [key, "Palermo"]), run(store, "ZRANGE", [key, "0", "-1"])}

  defp content(store, key, "zset"),
    do: run(store, "ZRANGE", [key, "0", "-1", "WITHSCORES"])

  defp content(store, key, "stream") do
    %{
      entries: run(store, "XRANGE", [key, "-", "+"]),
      info: run(store, "XINFO", ["STREAM", key]),
      # The consumer's pending entries (ID 0 reads its history, not new entries).
      pending: run(store, "XREADGROUP", ["GROUP", "workers", "w1", "STREAMS", key, "0"])
    }
  end

  test "plain keys of every type move with their content and expiry", ctx do
    build_keys!(ctx.source)
    before = snapshot(ctx.source)

    assert {:ok, %{keys: keys}} = Transfer.export(ctx.source, ctx.path)
    assert keys == length(@user_keys) + 1
    assert {:ok, %{keys: ^keys}} = Transfer.restore(ctx.target, ctx.path)

    assert snapshot(ctx.target) == before

    # The restored group resumes after the entries it already delivered.
    new_id = run(ctx.target, "XADD", ["k:stream", "*", "f", "3"])

    assert [["k:stream", [[^new_id, "f", "3"]]]] =
             run(ctx.target, "XREADGROUP", [
               "GROUP",
               "workers",
               "w2",
               "COUNT",
               "5",
               "STREAMS",
               "k:stream",
               ">"
             ])
  end

  test "export names probabilistic keys it cannot carry yet", ctx do
    run(ctx.source, "BF.ADD", ["k:bloom", "item"])

    assert {:error, {:unsupported_keys, ["k:bloom"]}} = Transfer.export(ctx.source, ctx.path)
    refute File.exists?(ctx.path)
  end

  test "restore refuses a store that already holds keys", ctx do
    build_keys!(ctx.source)
    {:ok, _counts} = Transfer.export(ctx.source, ctx.path)
    run(ctx.target, "SET", ["already-here", "1"])

    assert {:error, {:target_not_empty, "already-here"}} = Transfer.restore(ctx.target, ctx.path)
  end
end
