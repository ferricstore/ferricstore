defmodule Ferricstore.ImplStringOptionsTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    {:ok, ctx: ctx}
  end

  test "getex without options reads without removing expiry", %{ctx: ctx} do
    assert :ok = FerricStore.Impl.psetex(ctx, "getex-ttl", 60_000, "value")
    assert {:ok, before_ms} = FerricStore.Impl.pttl(ctx, "getex-ttl")
    assert before_ms > 0

    assert {:ok, "value"} = FerricStore.Impl.getex(ctx, "getex-ttl", [])
    assert {:ok, after_ms} = FerricStore.Impl.pttl(ctx, "getex-ttl")
    assert after_ms > 0
  end

  test "getex rejects malformed, duplicate, and conflicting options before storage", %{ctx: ctx} do
    assert {:error, "ERR syntax error"} =
             FerricStore.Impl.getex(ctx, "key", ttl: 1, persist: true)

    assert {:error, "ERR syntax error"} =
             FerricStore.Impl.getex(ctx, "key", ttl: 1, ttl: 2)

    assert {:error, "ERR syntax error"} = FerricStore.Impl.getex(ctx, "key", unknown: true)

    assert {:error, "ERR invalid expire time in 'getex' command"} =
             FerricStore.Impl.getex(ctx, "key", ttl: 0)
  end

  test "setex and psetex reject non-positive and non-integer TTLs", %{ctx: ctx} do
    assert {:error, "ERR invalid expire time in 'setex' command"} =
             FerricStore.Impl.setex(ctx, "key", 0, "value")

    assert {:error, "ERR invalid expire time in 'setex' command"} =
             FerricStore.Impl.setex(ctx, "key", "1", "value")

    assert {:error, "ERR invalid expire time in 'psetex' command"} =
             FerricStore.Impl.psetex(ctx, "key", -1, "value")

    assert {:error, "ERR invalid expire time in 'psetex' command"} =
             FerricStore.Impl.psetex(ctx, "key", 1.5, "value")
  end

  test "set rejects conflicting or unknown options and oversized values", %{ctx: ctx} do
    assert {:error, "ERR XX and NX options at the same time are not compatible"} =
             FerricStore.Impl.set(ctx, "key", "value", nx: true, xx: true)

    assert {:error, "ERR syntax error"} =
             FerricStore.Impl.set(ctx, "key", "value", unknown: true)

    limited_ctx = %{ctx | max_value_size: 1}

    assert {:error, "ERR value too large (2 bytes, max 1 bytes)"} =
             FerricStore.Impl.set(limited_ctx, "key", "xx")
  end

  test "setnx is one atomic conditional write under concurrency", %{ctx: ctx} do
    key = "setnx-race"
    parent = self()

    tasks =
      for index <- 1..32 do
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :go -> FerricStore.Impl.setnx(ctx, key, Integer.to_string(index))
          end
        end)
      end

    pids =
      for _ <- tasks do
        assert_receive {:ready, pid}
        pid
      end

    Enum.each(pids, &send(&1, :go))
    results = Enum.map(tasks, &Task.await(&1, 5_000))

    assert Enum.count(results, &(&1 == {:ok, true})) == 1
    assert Enum.count(results, &(&1 == {:ok, false})) == 31
  end

  test "del accepts an empty key list without inspecting instance state", %{ctx: ctx} do
    assert {:ok, 0} = FerricStore.Impl.del(ctx, [])
  end

  test "mset accepts the map contract exposed by use FerricStore", %{ctx: ctx} do
    assert :ok =
             FerricStore.Impl.mset(ctx, %{
               "mset:{batch}:a" => "one",
               "mset:{batch}:b" => "two"
             })

    assert {:ok, ["one", "two"]} =
             FerricStore.Impl.mget(ctx, ["mset:{batch}:a", "mset:{batch}:b"])
  end

  test "string-only operations reject compound keys without mutating them", %{ctx: ctx} do
    key = "impl:typed-hash"
    assert {:ok, 1} = FerricStore.Impl.hset(ctx, key, %{"field" => "value"})

    for result <- [
          FerricStore.Impl.get(ctx, key),
          FerricStore.Impl.strlen(ctx, key),
          FerricStore.Impl.getrange(ctx, key, 0, -1),
          FerricStore.Impl.append(ctx, key, "suffix"),
          FerricStore.Impl.incr(ctx, key, 1),
          FerricStore.Impl.incr_float(ctx, key, 1.0),
          FerricStore.Impl.setrange(ctx, key, 0, "x"),
          FerricStore.Impl.getset(ctx, key, "replacement"),
          FerricStore.Impl.getdel(ctx, key),
          FerricStore.Impl.getex(ctx, key, ttl: 60_000)
        ] do
      assert {:error, "WRONGTYPE" <> _} = result
    end

    assert {:ok, "value"} = FerricStore.Impl.hget(ctx, key, "field")
  end

  test "optimized string reads reject stream keys", %{ctx: ctx} do
    key = "impl:typed-stream"

    assert "1-0" =
             Ferricstore.Commands.Stream.handle_ast(
               {:xadd, key, {{:explicit, 1, 0}, ["field", "value"], nil, false}},
               ctx
             )

    assert {:error, "WRONGTYPE" <> _} = FerricStore.Impl.get(ctx, key)
    assert {:error, "WRONGTYPE" <> _} = FerricStore.Impl.strlen(ctx, key)
  end

  test "set variants replace compound values while setnx leaves them intact", %{ctx: ctx} do
    assert {:ok, 1} = FerricStore.Impl.hset(ctx, "set:typed", %{"field" => "value"})
    assert {:ok, false} = FerricStore.Impl.setnx(ctx, "set:typed", "ignored")
    assert {:ok, "value"} = FerricStore.Impl.hget(ctx, "set:typed", "field")

    assert :ok = FerricStore.Impl.set(ctx, "set:typed", "replacement")
    assert {:ok, "replacement"} = FerricStore.Impl.get(ctx, "set:typed")
    assert {:error, "WRONGTYPE" <> _} = FerricStore.Impl.hget(ctx, "set:typed", "field")

    assert {:ok, 1} = FerricStore.Impl.hset(ctx, "setex:typed", %{"field" => "value"})
    assert :ok = FerricStore.Impl.setex(ctx, "setex:typed", 60, "replacement")
    assert {:ok, "replacement"} = FerricStore.Impl.get(ctx, "setex:typed")
  end

  test "getset propagates write admission failures", %{ctx: ctx} do
    limited_ctx = %{ctx | max_value_size: 4}

    assert {:error, message} = FerricStore.Impl.getset(limited_ctx, "getset:too-large", "12345")
    assert message =~ "value too large"
  end

  test "ttl propagates storage failures instead of wrapping them as values", %{ctx: ctx} do
    IsolatedInstance.checkin(ctx)

    assert {:error, "ERR storage read failed"} = FerricStore.Impl.ttl(ctx, "unavailable")
    assert {:error, "ERR storage read failed"} = FerricStore.Impl.pttl(ctx, "unavailable")
  end
end
