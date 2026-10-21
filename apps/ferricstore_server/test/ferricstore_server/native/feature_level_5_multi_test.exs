defmodule FerricstoreServer.Native.FeatureLevel5MultiTest do
  @moduledoc """
  Cluster feature level 5 inside MULTI: a transaction may queue ZRANK and
  ZREVRANK WITHSCORE, a sourceless PFMERGE, HEXPIRE/HPEXPIRE with NX, XX, GT
  or LT or a TTL of 0, HSETEX's Redis 8 form and HGETEX with a TTL of 0, BITOP
  with an empty result
  deletes its destination, and GEOSEARCHSTORE of a missing source empties its
  destination.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Native.Session

  setup_all do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    assert {:ok, %{active: 5}} = FerricStore.raise_cluster_feature_level(5)
    :ok
  end

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{level5:#{System.unique_integer([:positive])}}"
    }
  end

  test "MULTI queues ZRANK and ZREVRANK WITHSCORE", %{ctx: ctx, key: key} do
    assert 2 == Dispatcher.dispatch("ZADD", [key, "1", "a", "2.5", "b"], ctx)

    state = multi()
    state = queue(state, "ZRANK", [key, "b", "WITHSCORE"])
    state = queue(state, "ZREVRANK", [key, "b", "WITHSCORE"])

    assert {:ok, [[1, "2.5"], [0, "2.5"]], _state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, state)
  end

  test "BITOP in MULTI deletes the destination for an empty result", %{ctx: ctx, key: key} do
    destination = key <> ":dst"
    :ok = Dispatcher.dispatch("SET", [destination, "x"], ctx)

    state = multi()
    state = queue(state, "BITOP", ["AND", destination, key <> ":m1", key <> ":m2"])

    assert {:ok, [0], _state} = Session.execute(%{"command" => "EXEC", "args" => []}, state)
    assert nil == Router.get(ctx, destination)
    assert 0 == Dispatcher.dispatch("DEL", [destination], ctx)
  end

  test "MULTI queues PFMERGE with only a destination", %{ctx: ctx, key: key} do
    state = queue(multi(), "PFMERGE", [key])
    assert {:ok, ["OK"], _state} = Session.execute(%{"command" => "EXEC", "args" => []}, state)
    assert 0 == Dispatcher.dispatch("PFCOUNT", [key], ctx)
  end

  test "GEOSEARCHSTORE in MULTI empties the destination for a missing source", %{
    ctx: ctx,
    key: key
  } do
    destination = key <> ":dst"
    assert 1 == Dispatcher.dispatch("GEOADD", [destination, "13.36", "38.11", "palermo"], ctx)

    state =
      queue(multi(), "GEOSEARCHSTORE", [
        destination,
        key,
        "FROMMEMBER",
        "m",
        "BYRADIUS",
        "10",
        "km"
      ])

    assert {:ok, [0], _state} = Session.execute(%{"command" => "EXEC", "args" => []}, state)
    assert 0 == Dispatcher.dispatch("EXISTS", [destination], ctx)
  end

  test "MULTI queues HEXPIRE with a condition or a TTL of 0", %{ctx: ctx, key: key} do
    assert 2 == Dispatcher.dispatch("HSET", [key, "a", "1", "b", "2"], ctx)

    state = multi()
    state = queue(state, "HEXPIRE", [key, "100", "NX", "FIELDS", "2", "a", "a"])
    state = queue(state, "HPEXPIRE", [key, "0", "FIELDS", "1", "b"])

    assert {:ok, [[1, 0], [2]], _state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, state)

    assert ["a", "1"] == Dispatcher.dispatch("HGETALL", [key], ctx)
  end

  test "MULTI queues HSETEX's Redis 8 form and HGETEX with a TTL of 0", %{ctx: ctx, key: key} do
    state = multi()
    state = queue(state, "HSETEX", [key, "FNX", "EX", "100", "FIELDS", "2", "a", "1", "b", "2"])
    state = queue(state, "HSETEX", [key, "FNX", "FIELDS", "1", "a", "9"])
    state = queue(state, "HGETEX", [key, "EX", "0", "FIELDS", "2", "a", "a"])

    assert {:ok, [1, 0, ["1", nil]], _state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, state)

    assert ["b", "2"] == Dispatcher.dispatch("HGETALL", [key], ctx)
  end

  # At level 5 HSETEX reads, decides and writes as one replicated entry.
  test "concurrent HSETEX FNX on one field sets it once", %{ctx: ctx, key: key} do
    results =
      1..20
      |> Task.async_stream(
        fn i -> Dispatcher.dispatch("HSETEX", [key, "FNX", "FIELDS", "1", "f", "#{i}"], ctx) end,
        max_concurrency: 20,
        ordered: false
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.sort(results) == List.duplicate(0, 19) ++ [1]
  end

  test "GEOSEARCHSTORE in MULTI uses Redis's Earth radius", %{ctx: ctx, key: key} do
    # b is 200.04 km from (0, 0) with Redis's radius (199.98 km with the old).
    assert 2 == Dispatcher.dispatch("GEOADD", [key, "0", "0", "a", "1.79846", "0", "b"], ctx)

    state =
      queue(multi(), "GEOSEARCHSTORE", [
        key <> ":dst",
        key,
        "FROMLONLAT",
        "0",
        "0",
        "BYRADIUS",
        "200",
        "km"
      ])

    assert {:ok, [1], _state} = Session.execute(%{"command" => "EXEC", "args" => []}, state)
  end

  test "MULTI queues SET IFEQ and IFNE", %{ctx: ctx, key: key} do
    :ok = Dispatcher.dispatch("SET", [key, "a"], ctx)
    state = multi()
    state = queue(state, "SET", [key, "b", "IFEQ", "a"])
    state = queue(state, "SET", [key, "c", "IFEQ", "a"])
    state = queue(state, "SET", [key, "d", "IFNE", "zz", "GET"])

    assert {:ok, ["OK", nil, "b"], _state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, state)

    assert "d" == Dispatcher.dispatch("GET", [key], ctx)
  end

  test "concurrent SET IFEQ against one value lets one writer win", %{ctx: ctx, key: key} do
    :ok = Dispatcher.dispatch("SET", [key, "v0"], ctx)

    results =
      1..20
      |> Task.async_stream(
        fn i -> Dispatcher.dispatch("SET", [key, "v#{i}", "IFEQ", "v0"], ctx) end,
        max_concurrency: 20,
        ordered: false
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &(&1 == :ok)) == 1
    assert Enum.count(results, &is_nil/1) == 19
  end

  test "DECRBY of the smallest integer in MULTI answers Redis's error", %{ctx: ctx, key: key} do
    :ok = Dispatcher.dispatch("SET", [key, "-1"], ctx)
    state = queue(multi(), "DECRBY", [key, "-9223372036854775808"])

    assert {:ok, [%{"error" => "ERR decrement would overflow"}], _state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, state)

    assert "-1" == Dispatcher.dispatch("GET", [key], ctx)
  end

  test "MULTI queues ZRANGE BYLEX", %{ctx: ctx, key: key} do
    assert 3 == Dispatcher.dispatch("ZADD", [key, "0", "a", "0", "b", "0", "c"], ctx)
    state = queue(multi(), "ZRANGE", [key, "[b", "+", "BYLEX"])

    assert {:ok, [["b", "c"]], _state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, state)
  end

  test "MULTI queues ZADD INCR", %{ctx: ctx, key: key} do
    state = multi()
    state = queue(state, "ZADD", [key, "INCR", "2", "m"])
    state = queue(state, "ZADD", [key, "NX", "INCR", "1", "m"])
    state = queue(state, "ZADD", [key, "GT", "INCR", "1.5", "m"])

    assert {:ok, ["2", nil, "3.5"], _state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, state)

    assert "3.5" == Dispatcher.dispatch("ZSCORE", [key, "m"], ctx)
  end

  # At level 5 a conditional ZADD INCR reads, decides and writes as one
  # replicated entry; concurrent NX writers must see one winner.
  test "concurrent ZADD NX INCR on one member adds it once", %{ctx: ctx, key: key} do
    results =
      1..20
      |> Task.async_stream(
        fn _i -> Dispatcher.dispatch("ZADD", [key, "NX", "INCR", "1", "m"], ctx) end,
        max_concurrency: 20,
        ordered: false
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &(&1 == "1")) == 1
    assert Enum.count(results, &is_nil/1) == 19
  end

  # PFADD and PFMERGE change a sketch in place, as Redis does, so it keeps its
  # TTL; the state machine applies them, keeping it only at level 5.
  test "PFADD and PFMERGE keep the sketch's TTL", %{ctx: ctx, key: key} do
    assert 1 == Dispatcher.dispatch("PFADD", [key, "a"], ctx)
    assert 1 == Dispatcher.dispatch("EXPIRE", [key, "100"], ctx)
    assert 1 == Dispatcher.dispatch("PFADD", [key, "b"], ctx)
    assert Dispatcher.dispatch("TTL", [key], ctx) in 99..100
    assert 1 == Dispatcher.dispatch("PFADD", [key <> ":src", "x"], ctx)
    assert :ok = Dispatcher.dispatch("PFMERGE", [key, key <> ":src"], ctx)
    assert Dispatcher.dispatch("TTL", [key], ctx) in 99..100

    state = queue(multi(), "PFADD", [key, "c"])
    assert {:ok, [1], _state} = Session.execute(%{"command" => "EXEC", "args" => []}, state)
    assert Dispatcher.dispatch("TTL", [key], ctx) in 99..100
  end

  defp multi do
    {:ok, "OK", state} = Session.execute(%{"command" => "MULTI", "args" => []}, session_state())
    state
  end

  defp queue(state, command, args) do
    {:ok, "QUEUED", state} = Session.execute(%{"command" => command, "args" => args}, state)
    state
  end

  defp session_state do
    ctx = FerricStore.Instance.get(:default)

    %{
      client_id: System.unique_integer([:positive, :monotonic]),
      client_name: nil,
      username: "default",
      authenticated: true,
      require_auth: false,
      acl_cache: ConnAuth.build_acl_cache("default"),
      peer: {{127, 0, 0, 1}, 12_345},
      created_at: System.monotonic_time(:millisecond),
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      compression: :none,
      compact_flow_responses: false,
      compact_response_codecs: MapSet.new(),
      subscribed_events: MapSet.new(),
      flow_wake_subscriptions: MapSet.new(),
      multi_state: :none,
      multi_queue: [],
      multi_queue_count: 0,
      multi_queue_bytes: 0,
      multi_error: false,
      watched_keys: %{},
      watched_key_bytes: 0,
      pubsub_channels: nil,
      pubsub_patterns: nil
    }
  end
end
