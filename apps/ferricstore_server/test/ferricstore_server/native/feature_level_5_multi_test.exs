defmodule FerricstoreServer.Native.FeatureLevel5MultiTest do
  @moduledoc """
  Cluster feature level 5 inside MULTI: a transaction may queue ZRANK and
  ZREVRANK WITHSCORE, a sourceless PFMERGE and HEXPIRE/HPEXPIRE with NX, XX,
  GT or LT or a TTL of 0, BITOP with an empty result
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

  test "MULTI queues HEXPIREAT, HPEXPIREAT and HPEXPIRETIME", %{ctx: ctx, key: key} do
    assert 2 == Dispatcher.dispatch("HSET", [key, "a", "1", "b", "2"], ctx)

    state = multi()
    state = queue(state, "HEXPIREAT", [key, "1", "FIELDS", "1", "a"])
    state = queue(state, "HPEXPIREAT", [key, "1800000001400", "NX", "FIELDS", "1", "b"])
    state = queue(state, "HPEXPIRETIME", [key, "FIELDS", "2", "a", "b"])

    assert {:ok, [[2], [1], [-2, 1_800_000_001_400]], _state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, state)
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
