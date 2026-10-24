defmodule Ferricstore.Commands.ReadDecideWriteLatchTest do
  @moduledoc """
  Commands that read a key and decide their write from it, through the command
  path the TCP and HTTP gateways use: one of many concurrent clients wins.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  setup do
    tag = "{latched:#{System.unique_integer([:positive])}}"
    %{ctx: FerricStore.Instance.get(:default), key: tag}
  end

  test "concurrent HGETDEL hands the field to one client", %{ctx: ctx, key: key} do
    assert 1 == Dispatcher.dispatch("HSET", [key, "f", "v"], ctx)

    results =
      concurrently(fn _ -> Dispatcher.dispatch("HGETDEL", [key, "FIELDS", "1", "f"], ctx) end)

    assert 1 == Enum.count(results, &(&1 == ["v"]))
  end

  test "concurrent SMOVE of one member has one winner", %{ctx: ctx, key: key} do
    assert 1 == Dispatcher.dispatch("SADD", [key <> ":from", "only"], ctx)

    results =
      concurrently(fn _ ->
        Dispatcher.dispatch("SMOVE", [key <> ":from", key <> ":to", "only"], ctx)
      end)

    assert 1 == Enum.count(results, &(&1 == 1))
    assert 1 == Dispatcher.dispatch("SCARD", [key <> ":to"], ctx)
  end

  # SMOVE latches its source, but SPOP and SREM do not take that latch: a
  # member SPOP took between SMOVE's read and its write still reached the
  # destination, and a later SPOP there handed it out a second time.
  test "concurrent SMOVE and SPOP hand out each member once", %{ctx: ctx, key: key} do
    for round <- 1..3 do
      from = "#{key}:from#{round}"
      to = "#{key}:to#{round}"
      members = for i <- 1..100, do: "m#{i}"
      assert 100 == Dispatcher.dispatch("SADD", [from | members], ctx)

      popped =
        1..32
        |> Task.async_stream(
          fn client ->
            Enum.flat_map(1..10, fn i ->
              if rem(client + i, 2) == 0 do
                [Dispatcher.dispatch("SPOP", [from], ctx), Dispatcher.dispatch("SPOP", [to], ctx)]
              else
                _ = Dispatcher.dispatch("SMOVE", [from, to, Enum.random(members)], ctx)
                []
              end
            end)
          end,
          max_concurrency: 32,
          timeout: 60_000
        )
        |> Enum.flat_map(fn {:ok, result} -> result end)
        |> Enum.reject(&is_nil/1)

      left =
        Dispatcher.dispatch("SMEMBERS", [from], ctx) ++ Dispatcher.dispatch("SMEMBERS", [to], ctx)

      all = popped ++ left

      assert [] == all -- Enum.uniq(all), "handed out twice in round #{round}"
      assert Enum.sort(members) == Enum.sort(all)
    end
  end

  # SETBIT read the whole value, changed one bit and wrote the value back, so
  # concurrent clients setting different bits overwrote each other's.
  test "concurrent SETBIT of different bits keeps every bit", %{ctx: ctx, key: key} do
    assert :ok = Dispatcher.dispatch("SET", [key, "", "EX", "100"], ctx)

    1..16
    |> Task.async_stream(
      fn client ->
        for i <- 1..40, do: Dispatcher.dispatch("SETBIT", [key, "#{client * 40 + i}", "1"], ctx)
      end,
      max_concurrency: 16,
      timeout: 60_000
    )
    |> Stream.run()

    assert 640 == Dispatcher.dispatch("BITCOUNT", [key], ctx)
    assert Dispatcher.dispatch("TTL", [key], ctx) in 95..100
  end

  # EXPIRE and PERSIST of a string read the value and wrote it back with the
  # new expiry, so a write in between (INCR here) was undone.
  for {name, expiry} <- [
        {"EXPIRE", ["EXPIRE", "100"]},
        {"PEXPIRE", ["PEXPIRE", "100000"]},
        {"PERSIST", ["PERSIST"]}
      ] do
    @expiry expiry
    test "INCR racing #{name} loses no increment", %{ctx: ctx, key: key} do
      [command | args] = @expiry
      assert :ok = Dispatcher.dispatch("SET", [key, "0", "EX", "1000"], ctx)

      expirer =
        Task.async(fn ->
          for _ <- 1..200, do: Dispatcher.dispatch(command, [key | args], ctx)
        end)

      1..8
      |> Task.async_stream(
        fn _ -> for _ <- 1..50, do: Dispatcher.dispatch("INCR", [key], ctx) end,
        max_concurrency: 8,
        timeout: 60_000
      )
      |> Stream.run()

      Task.await(expirer, 60_000)
      assert "400" == Dispatcher.dispatch("GET", [key], ctx)
    end
  end

  # A collection's expiry was set by reading every member and writing them all
  # back with it, so members changed or added in between were undone.
  for {name, init, write, read} <- [
        {"HINCRBY", ["HSET", "f", "0"], ["HINCRBY", "f", "1"], ["HGET", "f"]},
        {"ZINCRBY", ["ZADD", "0", "m"], ["ZINCRBY", "1", "m"], ["ZSCORE", "m"]},
        {"RPUSH", ["RPUSH", "seed"], ["RPUSH", "e"], ["LLEN"]}
      ] do
    @init init
    @write write
    @read read
    @expected if name == "RPUSH", do: 401, else: "400"

    for expiry <- [["EXPIRE", "1000"], ["PEXPIREAT", "99999999999999"], ["PERSIST"]] do
      @expiry expiry
      test "#{name} racing #{hd(expiry)} on a collection loses nothing", %{ctx: ctx, key: key} do
        [init | init_args] = @init
        [write | write_args] = @write
        [read | read_args] = @read
        [expiry | expiry_args] = @expiry
        Dispatcher.dispatch(init, [key | init_args], ctx)

        expirer =
          Task.async(fn ->
            for i <- 1..100 do
              # PERSIST only changes a key with a TTL.
              if expiry == "PERSIST" and rem(i, 2) == 0,
                do: Dispatcher.dispatch("EXPIRE", [key, "1000"], ctx),
                else: Dispatcher.dispatch(expiry, [key | expiry_args], ctx)
            end
          end)

        1..8
        |> Task.async_stream(
          fn _ -> for _ <- 1..50, do: Dispatcher.dispatch(write, [key | write_args], ctx) end,
          max_concurrency: 8,
          timeout: 60_000
        )
        |> Stream.run()

        Task.await(expirer, 60_000)
        assert @expected == Dispatcher.dispatch(read, [key | read_args], ctx)
      end
    end
  end

  # A hash field's expiry was set by reading the field and writing it back.
  for {name, getex} <- [{"HGETEX EX", ["EX", "1000"]}, {"HGETEX PERSIST", ["PERSIST"]}] do
    @getex getex
    test "HINCRBY racing #{name} on the field loses no increment", %{ctx: ctx, key: key} do
      assert 1 == Dispatcher.dispatch("HSET", [key, "f", "0"], ctx)

      expirer =
        Task.async(fn ->
          for _ <- 1..100,
              do: Dispatcher.dispatch("HGETEX", [key | @getex] ++ ["FIELDS", "1", "f"], ctx)
        end)

      1..8
      |> Task.async_stream(
        fn _ -> for _ <- 1..50, do: Dispatcher.dispatch("HINCRBY", [key, "f", "1"], ctx) end,
        max_concurrency: 8,
        timeout: 60_000
      )
      |> Stream.run()

      Task.await(expirer, 60_000)
      assert "400" == Dispatcher.dispatch("HGET", [key, "f"], ctx)
    end
  end

  for expiry <- [["HEXPIRE", "1000"], ["HPEXPIRE", "1000000"], ["HPERSIST"]] do
    @expiry expiry
    test "HINCRBY racing #{hd(expiry)} on the field loses no increment", %{ctx: ctx, key: key} do
      [expiry | ttl] = @expiry
      assert 1 == Dispatcher.dispatch("HSET", [key, "f", "0"], ctx)

      expirer =
        Task.async(fn ->
          for i <- 1..100 do
            # HPERSIST only changes a field with a TTL.
            if expiry == "HPERSIST" and rem(i, 2) == 0,
              do: Dispatcher.dispatch("HEXPIRE", [key, "1000", "FIELDS", "1", "f"], ctx),
              else: Dispatcher.dispatch(expiry, [key | ttl] ++ ["FIELDS", "1", "f"], ctx)
          end
        end)

      1..8
      |> Task.async_stream(
        fn _ -> for _ <- 1..50, do: Dispatcher.dispatch("HINCRBY", [key, "f", "1"], ctx) end,
        max_concurrency: 8,
        timeout: 60_000
      )
      |> Stream.run()

      Task.await(expirer, 60_000)
      assert "400" == Dispatcher.dispatch("HGET", [key, "f"], ctx)
    end
  end

  test "embedded expire and persist racing incr lose no increment", %{key: key} do
    assert :ok = FerricStore.set(key, "0")

    expirer =
      Task.async(fn ->
        for i <- 1..200 do
          if rem(i, 2) == 0, do: FerricStore.expire(key, 100_000), else: FerricStore.persist(key)
        end
      end)

    1..8
    |> Task.async_stream(fn _ -> for _ <- 1..50, do: FerricStore.incr(key) end,
      max_concurrency: 8,
      timeout: 60_000
    )
    |> Stream.run()

    Task.await(expirer, 60_000)
    assert {:ok, "400"} == FerricStore.get(key)
  end

  # PFMERGE read the destination's sketch and wrote it back merged, undoing
  # PFADDs to the destination in between.
  test "PFADD to the destination racing PFMERGE keeps every element", %{ctx: ctx, key: key} do
    dest = key <> ":dest"
    assert 1 == Dispatcher.dispatch("PFADD", [key <> ":src", "s"], ctx)

    merger =
      Task.async(fn ->
        for _ <- 1..200, do: Dispatcher.dispatch("PFMERGE", [dest, key <> ":src"], ctx)
      end)

    1..8
    |> Task.async_stream(
      fn client ->
        for i <- 1..100, do: Dispatcher.dispatch("PFADD", [dest, "#{client}-#{i}"], ctx)
      end,
      max_concurrency: 8,
      timeout: 60_000
    )
    |> Stream.run()

    Task.await(merger, 60_000)
    # 801 distinct elements; HyperLogLog counts within a few percent.
    assert Dispatcher.dispatch("PFCOUNT", [dest], ctx) in 770..830
  end

  test "concurrent EXPIRE NX has one winner", %{ctx: ctx, key: key} do
    assert :ok = Dispatcher.dispatch("SET", [key, "v"], ctx)

    results =
      concurrently(fn client ->
        Dispatcher.dispatch("EXPIRE", [key, "#{100 + client}", "NX"], ctx)
      end)

    assert 1 == Enum.count(results, &(&1 == 1))
  end

  test "concurrent GEOADD NX has one winner", %{ctx: ctx, key: key} do
    results =
      concurrently(fn client ->
        Dispatcher.dispatch("GEOADD", [key, "NX", "13.#{client}", "38.1", "m"], ctx)
      end)

    assert 1 == Enum.count(results, &(&1 == 1))
  end

  defp concurrently(fun) do
    1..20
    |> Task.async_stream(fun, max_concurrency: 20, timeout: 60_000)
    |> Enum.map(fn {:ok, result} -> result end)
  end
end
