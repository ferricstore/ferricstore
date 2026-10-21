defmodule FerricstoreServer.Native.FeatureLevel5MultiTest do
  @moduledoc """
  Cluster feature level 5 inside MULTI: a transaction may queue ZRANK and
  ZREVRANK WITHSCORE, and BITOP with an empty result deletes its destination.
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
