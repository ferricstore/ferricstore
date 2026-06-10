defmodule FerricstoreServer.Native.CommandsTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Config
  alias Ferricstore.Flow.ClaimWaiters
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Native.Commands

  @op_auth 0x0002
  @op_ping 0x0003
  @op_options 0x000B
  @op_startup 0x000C
  @op_batch 0x000E
  @op_subscribe_events 0x0011
  @op_unsubscribe_events 0x0012
  @op_get 0x0101
  @op_cas 0x0106
  @op_cluster_keyslot 0x0303
  @op_flow_create 0x0201
  @op_flow_claim_due 0x0203
  @op_flow_value_mget 0x020C
  @op_flow_create_many 0x020F

  setup do
    ConnRegistry.init_table()
    FerricstoreServer.Acl.reset!()
    old_max_batch_commands = Application.get_env(:ferricstore, :native_max_batch_commands)

    old_request_compression_enabled =
      Application.get_env(:ferricstore, :native_request_compression_enabled)

    on_exit(fn ->
      Config.set("requirepass", "")
      restore_env(:native_max_batch_commands, old_max_batch_commands)
      restore_env(:native_request_compression_enabled, old_request_compression_enabled)
      FerricstoreServer.Acl.reset!()
    end)

    :ok
  end

  test "OPTIONS advertises multiplexing, flow control, and event support" do
    {status, payload, _state} = Commands.execute(@op_options, %{}, state())

    assert status == :ok
    assert payload.protocol_versions == [1]
    assert payload.multiplexing.lane_id == true
    assert payload.multiplexing.ordered_per_lane == true
    assert payload.flow_control.window_update == true
    assert payload.flow_control.enforced == true
    assert payload.compression == ["none"]
    assert payload.chunking.request_reassembly == true
    assert payload.chunking.response_chunks == true
    assert payload.response_codecs.typed_value == true
    assert "flow_claim_jobs_v1" in payload.response_codecs.supported
    assert "ok_list_v1" in payload.response_codecs.supported
    assert payload.schemas["FLOW.CREATE"]["required"] == ["id"]
    assert "type" in payload.schemas["FLOW.CREATE"]["fields"]
    assert "return" in payload.schemas["FLOW.CLAIM_DUE"]["fields"]
    assert "partition_keys" in payload.schemas["FLOW.CLAIM_DUE"]["fields"]
    assert "block_ms" in payload.schemas["FLOW.CLAIM_DUE"]["fields"]
    assert "reclaim_expired" in payload.schemas["FLOW.CLAIM_DUE"]["fields"]
    assert "reclaim_ratio" in payload.schemas["FLOW.CLAIM_DUE"]["fields"]
    assert "AUTH_INVALIDATED" in payload.events
    assert Enum.any?(payload.opcodes, &(&1["name"] == "BATCH"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "ROUTE_BATCH"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.CREATE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.CLAIM_DUE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.COMPLETE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.SIGNAL"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "CAS"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "LOCK"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "RATELIMIT.ADD"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "CLUSTER.STATUS"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FERRICSTORE.KEY_INFO"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FERRICSTORE.CONFIG"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FERRICSTORE.METRICS"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.VALUE.PUT"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.VALUE.MGET"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.POLICY.SET"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.POLICY.GET"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.SPAWN_CHILDREN"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.RETENTION_CLEANUP"))

    assert payload.schemas["FLOW.VALUE.MGET"]["fields"] == [
             "refs",
             "max_bytes",
             "payload_max_bytes",
             "value_max_bytes",
             "deadline_ms"
           ]

    assert "owner_flow_id" in payload.schemas["FLOW.VALUE.PUT"]["fields"]
    assert "retention_ttl_ms" in payload.schemas["FLOW.CREATE"]["fields"]
  end

  test "OPTIONS advertises zlib request compression only when enabled" do
    Application.put_env(:ferricstore, :native_request_compression_enabled, true)

    {status, payload, _state} = Commands.execute(@op_options, %{}, state())

    assert status == :ok
    assert "zlib" in payload.compression
  end

  test "native validates custom command fields before dispatch" do
    {status, reason, _state} =
      Commands.execute(
        @op_cas,
        %{"key" => "k", "expected" => "old", "value" => "new", "ttl" => "bad"},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :bad_request
    assert reason =~ "ttl"
  end

  test "native VALUE.MGET accepts max byte options" do
    {status, reason, _state} =
      Commands.execute(
        @op_flow_value_mget,
        %{"refs" => [], "max_bytes" => "bad"},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :error
    assert reason =~ "max_bytes"
  end

  test "native FLOW.CREATE rejects obsolete terminal_ttl_ms option" do
    {status, reason, _state} =
      Commands.execute(
        @op_flow_create,
        %{"id" => "flow-1", "type" => "email", "terminal_ttl_ms" => 1000},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :bad_request
    assert reason =~ "terminal_ttl_ms"
  end

  test "native FLOW.CREATE_MANY accepts decoded item maps" do
    id = "native-create-many-#{System.unique_integer([:positive])}"
    type = "native-create-many-#{System.unique_integer([:positive])}"
    now_ms = System.system_time(:millisecond)

    {status, _reply, _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "type" => type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "items" => [%{"id" => id, "payload" => "payload"}]
        },
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
  end

  test "native FLOW.CREATE_MANY accepts compact item arrays" do
    id = "native-create-many-compact-#{System.unique_integer([:positive])}"
    type = "native-create-many-compact-#{System.unique_integer([:positive])}"
    now_ms = System.system_time(:millisecond)

    {status, _reply, _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "type" => type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "items" => [[id, "payload"]]
        },
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
  end

  test "BATCH same_shard validates compact FLOW.CREATE_MANY item arrays" do
    ctx = FerricStore.Instance.get(:default)
    {id1, id2} = different_shard_ids(ctx, "native-create-many-same-shard")
    now_ms = System.system_time(:millisecond)

    {status, reason, _state} =
      Commands.execute(
        @op_batch,
        %{
          "atomicity" => "same_shard",
          "commands" => [
            %{
              "opcode" => @op_flow_create_many,
              "lane_id" => 1,
              "request_id" => 501,
              "body" => %{
                "type" => "native-create-many-same-shard",
                "state" => "queued",
                "now_ms" => now_ms,
                "run_at_ms" => now_ms,
                "independent" => true,
                "items" => [[id1, "payload"], [id2, "payload"]]
              }
            }
          ]
        },
        state(instance_ctx: ctx)
      )

    assert status == :bad_request
    assert reason =~ "same_shard"
  end

  test "native FLOW.CREATE_MANY can return OK on all-success independent create" do
    id = "native-create-many-ok-#{System.unique_integer([:positive])}"
    type = "native-create-many-ok-#{System.unique_integer([:positive])}"
    now_ms = System.system_time(:millisecond)

    {status, reply, _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "type" => type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "return" => "ok_on_success",
          "items" => [[id, "payload"]]
        },
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
    assert reply == "OK"
  end

  test "native FLOW.CLAIM_DUE accepts reclaim options" do
    {status, reply, _state} =
      Commands.execute(
        @op_flow_claim_due,
        %{
          "type" => "native-claim-reclaim",
          "state" => "queued",
          "worker" => "w1",
          "limit" => 1,
          "lease_ms" => 1000,
          "reclaim_expired" => false,
          "reclaim_ratio" => 0
        },
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
    assert reply == []
  end

  test "SUBSCRIBE_EVENTS registers persistent Flow wake filters through claim waiters" do
    try do
      ClaimWaiters.cleanup(self())
      type = "native-wake-#{System.unique_integer([:positive])}"

      {status, reply, new_state} =
        Commands.execute(
          @op_subscribe_events,
          %{
            "events" => ["FLOW_WAKE"],
            "flow_wake" => %{
              "type" => type,
              "state" => "queued",
              "priority" => 0,
              "partition_keys" => ["bucket-0", "bucket-1"],
              "limit" => 500
            }
          },
          state()
        )

      assert status == :ok
      assert "FLOW_WAKE" in reply.subscribed
      assert new_state.event_subscriptions == MapSet.new(["FLOW_WAKE"])
      assert %{type: ^type, limit: 500, keys: keys} = new_state.flow_wake_subscription
      assert length(keys) == 2
      assert ClaimWaiters.total_count() == 2

      assert Commands.flow_wake_event_payload(new_state) == %{
               type: type,
               credit: 500,
               reason: "ready"
             }

      assert Commands.refresh_flow_wake_subscription(new_state) == new_state
      assert ClaimWaiters.total_count() == 2
    after
      ClaimWaiters.cleanup(self())
    end
  end

  test "UNSUBSCRIBE_EVENTS removes Flow wake waiter registrations" do
    try do
      ClaimWaiters.cleanup(self())
      type = "native-wake-unsub-#{System.unique_integer([:positive])}"

      {:ok, _reply, new_state} =
        Commands.execute(
          @op_subscribe_events,
          %{
            "events" => ["FLOW_WAKE"],
            "flow_wake" => %{"type" => type, "state" => "queued", "limit" => 10}
          },
          state()
        )

      assert ClaimWaiters.total_count() > 0

      {status, reply, unsubscribed_state} =
        Commands.execute(
          @op_unsubscribe_events,
          %{"events" => ["FLOW_WAKE"]},
          new_state
        )

      assert status == :ok
      assert reply.subscribed == []
      assert unsubscribed_state.flow_wake_subscription == nil
      assert ClaimWaiters.total_count() == 0
    after
      ClaimWaiters.cleanup(self())
    end
  end

  test "native admin bridge dispatches cluster keyslot" do
    {status, slot, _state} =
      Commands.execute(
        @op_cluster_keyslot,
        %{"key" => "user:1", "args" => ["user:1"]},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
    assert is_integer(slot)
  end

  test "SUBSCRIBE_EVENTS rejects unknown event names" do
    {status, reason, _state} =
      Commands.execute(@op_subscribe_events, %{"events" => ["unknown"]}, state())

    assert status == :bad_request
    assert reason =~ "known event names"
  end

  test "SUBSCRIBE_EVENTS stores normalized event subscriptions" do
    {status, payload, new_state} =
      Commands.execute(@op_subscribe_events, %{"events" => ["auth_invalidated"]}, state())

    assert status == :ok
    assert payload.subscribed == ["AUTH_INVALIDATED"]
    assert MapSet.member?(new_state.event_subscriptions, "AUTH_INVALIDATED")
  end

  test "UNSUBSCRIBE_EVENTS removes normalized event subscriptions" do
    state = state(event_subscriptions: MapSet.new(["AUTH_INVALIDATED", "FLOW_WAKE"]))

    {status, payload, new_state} =
      Commands.execute(@op_unsubscribe_events, %{"events" => ["auth_invalidated"]}, state)

    assert status == :ok
    assert payload.subscribed == ["FLOW_WAKE"]
    refute MapSet.member?(new_state.event_subscriptions, "AUTH_INVALIDATED")
    assert MapSet.member?(new_state.event_subscriptions, "FLOW_WAKE")
  end

  test "STARTUP sets client name and subscribes requested events" do
    {status, payload, new_state} =
      Commands.execute(
        @op_startup,
        %{"client_name" => "native-sdk", "events" => ["flow_wake"]},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
    assert payload.protocol == "ferricstore-native"
    assert new_state.client_name == "native-sdk"
    assert MapSet.member?(new_state.event_subscriptions, "FLOW_WAKE")
  end

  test "STARTUP enables compact Flow responses when requested" do
    {status, payload, new_state} =
      Commands.execute(
        @op_startup,
        %{"compact_flow_responses" => true},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
    assert payload.capabilities.response_codecs.compact_flow_responses == true
    assert new_state.compact_flow_responses == true
  end

  test "STARTUP rejects unsupported compression negotiation" do
    {status, reason, new_state} =
      Commands.execute(
        @op_startup,
        %{"compression" => "brotli"},
        state(instance_ctx: FerricStore.Instance.get(:default), compression: :none)
      )

    assert status == :bad_request
    assert reason =~ "unsupported compression"
    assert new_state.compression == :none
  end

  test "BATCH rejects control commands" do
    {status, reason, _state} =
      Commands.execute(
        @op_batch,
        %{"commands" => [%{"opcode" => @op_ping, "body" => %{}}]},
        state()
      )

    assert status == :bad_request
    assert reason =~ "control commands"
  end

  test "BATCH enforces configured max command count before execution" do
    Application.put_env(:ferricstore, :native_max_batch_commands, 1)

    {status, reason, _state} =
      Commands.execute(
        @op_batch,
        %{
          "commands" => [
            %{"opcode" => @op_get, "body" => %{"key" => "a"}},
            %{"opcode" => @op_get, "body" => %{"key" => "b"}}
          ]
        },
        state()
      )

    assert status == :bad_request
    assert reason =~ "max commands"
  end

  test "AUTH does not bypass requirepass through passwordless default ACL user" do
    Config.set("requirepass", "secret")

    {status, reason, new_state} =
      Commands.execute(
        @op_auth,
        %{"username" => "default", "password" => "wrong"},
        state(require_auth: true)
      )

    assert status == :auth
    assert reason =~ "WRONGPASS"
    assert new_state.authenticated == false
  end

  defp state(overrides \\ []) do
    Map.merge(
      %{
        client_id: System.unique_integer([:positive]),
        client_name: nil,
        username: "default",
        authenticated: false,
        require_auth: false,
        peer: nil,
        created_at: 0,
        instance_ctx: nil,
        stats_counter: nil,
        acl_cache: ConnAuth.build_acl_cache("default"),
        max_frame_bytes: 16 * 1024 * 1024,
        max_lanes: 1024,
        lane_max_queue: 1024,
        max_inflight_per_connection: 4096,
        max_inflight_per_lane: 1024,
        compression: :none,
        event_subscriptions: MapSet.new(),
        flow_wake_subscription: nil,
        compact_flow_responses: false,
        close_after_reply: false
      },
      Map.new(overrides)
    )
  end

  defp different_shard_ids(ctx, prefix) do
    id1 = "#{prefix}-#{System.unique_integer([:positive])}-a"
    shard1 = Router.shard_for(ctx, id1)

    id2 =
      1..10_000
      |> Stream.map(&"#{prefix}-#{System.unique_integer([:positive])}-#{&1}")
      |> Enum.find(&(Router.shard_for(ctx, &1) != shard1))

    {id1, id2}
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
