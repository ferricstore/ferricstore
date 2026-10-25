defmodule Ferricstore.Commands.ReadDecideWriteLevel5Test do
  @moduledoc """
  Commands whose handlers write differently below feature level 5 inside a
  transaction (BITOP, PFMERGE, HGETEX, GEOSEARCHSTORE) run as one transaction
  entry only from that level, so their races are checked there.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    assert {:ok, %{active: 5}} = FerricStore.raise_cluster_feature_level(5)
    :ok
  end

  setup do
    tag = "{level5-rmw:#{System.unique_integer([:positive])}}"
    %{ctx: FerricStore.Instance.get(:default), key: tag}
  end

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

  # GEOSEARCHSTORE into its own source read the source and wrote the matches
  # back over it, undoing a GEOADD in between.
  test "GEOADD racing GEOSEARCHSTORE into its own source keeps every member",
       %{ctx: ctx, key: key} do
    assert 1 == Dispatcher.dispatch("GEOADD", [key, "0", "0", "origin"], ctx)

    search = ["FROMLONLAT", "0", "0", "BYRADIUS", "500", "km"]

    searcher =
      Task.async(fn ->
        for _ <- 1..200, do: Dispatcher.dispatch("GEOSEARCHSTORE", [key, key | search], ctx)
      end)

    1..8
    |> Task.async_stream(
      fn client ->
        for i <- 1..50 do
          lng = Float.to_string(client * 0.1)
          lat = Float.to_string(i * 0.01)
          Dispatcher.dispatch("GEOADD", [key, lng, lat, "#{client}-#{i}"], ctx)
        end
      end,
      max_concurrency: 8,
      timeout: 60_000
    )
    |> Stream.run()

    Task.await(searcher, 60_000)
    assert 401 == Dispatcher.dispatch("ZCARD", [key], ctx)
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

  test "embedded pfadd to the destination racing pfmerge keeps every element", %{key: key} do
    dest = key <> ":edest"
    assert {:ok, true} = FerricStore.pfadd(key <> ":esrc", ["s"])

    merger =
      Task.async(fn -> for _ <- 1..200, do: FerricStore.pfmerge(dest, [key <> ":esrc"]) end)

    1..8
    |> Task.async_stream(
      fn client -> for i <- 1..100, do: FerricStore.pfadd(dest, ["#{client}-#{i}"]) end,
      max_concurrency: 8,
      timeout: 60_000
    )
    |> Stream.run()

    Task.await(merger, 60_000)
    assert {:ok, count} = FerricStore.pfcount([dest])
    assert count in 770..830
  end

  test "SETBIT racing BITOP into one of its sources keeps every bit", %{ctx: ctx, key: key} do
    assert 0 == Dispatcher.dispatch("SETBIT", [key <> ":o", "0", "1"], ctx)

    merger =
      Task.async(fn ->
        for _ <- 1..200,
            do: Dispatcher.dispatch("BITOP", ["OR", key <> ":b", key <> ":b", key <> ":o"], ctx)
      end)

    1..8
    |> Task.async_stream(
      fn client ->
        for i <- 1..50,
            do: Dispatcher.dispatch("SETBIT", [key <> ":b", "#{client * 50 + i}", "1"], ctx)
      end,
      max_concurrency: 8,
      timeout: 60_000
    )
    |> Stream.run()

    Task.await(merger, 60_000)
    assert 401 == Dispatcher.dispatch("BITCOUNT", [key <> ":b"], ctx)
  end

  test "embedded bitop into one of its sources keeps every bit", %{ctx: ctx, key: key} do
    assert 0 == Dispatcher.dispatch("SETBIT", [key <> ":eo", "0", "1"], ctx)

    merger =
      Task.async(fn ->
        for _ <- 1..200, do: FerricStore.bitop(:or, key <> ":eb", [key <> ":eb", key <> ":eo"])
      end)

    1..8
    |> Task.async_stream(
      fn client ->
        for i <- 1..50,
            do: Dispatcher.dispatch("SETBIT", [key <> ":eb", "#{client * 50 + i}", "1"], ctx)
      end,
      max_concurrency: 8,
      timeout: 60_000
    )
    |> Stream.run()

    Task.await(merger, 60_000)
    assert 401 == Dispatcher.dispatch("BITCOUNT", [key <> ":eb"], ctx)
  end
end
