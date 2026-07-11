defmodule FerricstoreServer.Native.CommandsTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Acl
  alias FerricstoreServer.AuthRateLimiter
  alias Ferricstore.Commands.PreparedCommand
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Native.Commands
  alias FerricstoreServer.Native.Session

  @op_hello 0x0001
  @op_auth 0x0002
  @op_route 0x0006
  @op_route_batch 0x000F
  @op_shards 0x0007
  @op_options 0x000B
  @op_pipeline 0x000E
  @op_command_exec 0x0100
  @op_set 0x0102
  @op_hset 0x0110
  @op_flow_create 0x0201
  @op_flow_get 0x0202
  @op_flow_claim_due 0x0203
  @op_flow_value_put 0x020B
  @op_flow_list 0x020E
  @op_flow_create_many 0x020F
  @op_flow_complete_many 0x0210
  @op_flow_reclaim 0x0215
  @op_flow_by_parent 0x0219
  @op_flow_by_root 0x021A
  @op_flow_by_correlation 0x021B
  @op_flow_policy_set 0x021E
  @op_flow_spawn_children 0x0220
  @op_flow_start_and_claim 0x0223
  @op_flow_run_steps_many 0x0224
  @op_flow_schedule_create 0x0225
  @op_flow_schedule_delete 0x0227
  @op_flow_schedule_fire 0x022A
  @op_flow_stats 0x022D
  @op_flow_search 0x0230
  @op_flow_approval_request 0x0246
  @op_flow_approval_get 0x0249
  @op_flow_circuit_open 0x024A
  @op_flow_circuit_get 0x024C
  @op_flow_budget_reserve 0x024D
  @op_flow_limit_lease 0x024F

  defmodule TestExtension do
    @behaviour Ferricstore.Commands.Extension

    @impl true
    def commands do
      [
        %{
          name: "EXT.READ",
          arity: 2,
          flags: ["readonly"],
          first_key: 1,
          last_key: 1,
          step: 1,
          access: :read,
          summary: "Test extension read"
        },
        %{
          name: "EXT.WRITE",
          arity: 2,
          flags: ["write"],
          first_key: 1,
          last_key: 1,
          step: 1,
          access: :write,
          summary: "Test extension write"
        },
        %{
          name: "EXT.CONTEXT",
          arity: 1,
          flags: ["readonly"],
          first_key: 0,
          last_key: 0,
          step: 0,
          access: :read,
          summary: "Test extension request context"
        }
      ]
    end

    @impl true
    def handle("EXT.READ", [key], _store), do: {:ok, ["read", key]}
    def handle("EXT.WRITE", [key], _store), do: {:ok, ["write", key]}

    def handle("EXT.CONTEXT", [], store),
      do: {:ok, Ferricstore.Commands.Extension.request_context(store)}
  end

  defmodule CountingKeyExtension do
    @behaviour Ferricstore.Commands.Extension

    @impl true
    def commands do
      [
        %{
          name: "EXT.COUNTED",
          arity: 2,
          flags: ["readonly"],
          first_key: 1,
          last_key: 1,
          step: 1,
          access: :read,
          summary: "Counts key discovery calls"
        }
      ]
    end

    @impl true
    def keys("EXT.COUNTED", [key]) do
      counter_key = {__MODULE__, :key_discovery_calls}
      Process.put(counter_key, Process.get(counter_key, 0) + 1)
      {:ok, [key]}
    end

    @impl true
    def handle("EXT.COUNTED", [key], _store), do: {:ok, key}
  end

  setup do
    previous_extensions = Application.get_env(:ferricstore, :command_extensions)

    previous_trusted_request_context_users =
      Application.get_env(:ferricstore, :native_trusted_request_context_users)

    previous_acl_management = Application.get_env(:ferricstore, FerricStore.Management.ACL)

    Application.delete_env(:ferricstore, :command_extensions)
    Application.delete_env(:ferricstore, :native_trusted_request_context_users)

    Application.put_env(
      :ferricstore,
      FerricStore.Management.ACL,
      FerricstoreServer.Management.ACL
    )

    ConnRegistry.init_table()
    FerricstoreServer.Acl.reset!()
    {:ok, _} = Application.ensure_all_started(:ferricstore)

    on_exit(fn ->
      FerricstoreServer.Acl.reset!()

      case previous_extensions do
        nil -> Application.delete_env(:ferricstore, :command_extensions)
        value -> Application.put_env(:ferricstore, :command_extensions, value)
      end

      case previous_trusted_request_context_users do
        nil -> Application.delete_env(:ferricstore, :native_trusted_request_context_users)
        value -> Application.put_env(:ferricstore, :native_trusted_request_context_users, value)
      end

      case previous_acl_management do
        nil -> Application.delete_env(:ferricstore, FerricStore.Management.ACL)
        value -> Application.put_env(:ferricstore, FerricStore.Management.ACL, value)
      end
    end)

    :ok
  end

  test "OPTIONS advertises native protocol capabilities and command coverage" do
    {status, payload, _state} = Commands.execute(@op_options, %{}, state())

    assert status == :ok
    assert payload.protocol_versions == [1]
    assert payload.multiplexing.request_id == true
    assert payload.multiplexing.concurrent_lanes == true
    assert payload.response_codecs.typed_value == true
    assert "FLOW.CREATE" in schema_names(payload)
    assert "FLOW.CLAIM_DUE" in schema_names(payload)
    assert "FLOW.COMPLETE" in schema_names(payload)
    assert "FLOW.RUN_STEPS_MANY" in schema_names(payload)
    assert "FLOW.SCHEDULE.CREATE" in schema_names(payload)
    assert "FLOW.ATTRIBUTES" in schema_names(payload)
    assert "FLOW.SEARCH" in opcode_names(payload)
    assert "FLOW.BUDGET.RESERVE" in opcode_names(payload)
    assert "GET" in opcode_names(payload)
    assert "SET" in opcode_names(payload)
    refute "GET.COMPACT" in opcode_names(payload)

    for command <- [
          "FLOW.CREATE",
          "FLOW.CREATE_MANY",
          "FLOW.POLICY.SET",
          "FLOW.SPAWN_CHILDREN",
          "FLOW.START_AND_CLAIM"
        ] do
      assert "max_active_ms" in payload.schemas[command]["fields"]
    end
  end

  test "HELLO returns native route metadata only" do
    {status, payload, new_state} =
      Commands.execute(@op_hello, %{"client_name" => "sdk-a"}, state())

    assert status == :ok
    assert payload.protocol == "ferricstore-native"
    assert is_binary(payload.route.host)
    assert is_integer(payload.route.native_port)
    assert payload.route.endpoint.host == payload.route.host
    assert payload.route.endpoint.native_port == payload.route.native_port
    refute Map.has_key?(payload.route, String.to_atom("resp" <> "_port"))
    assert new_state.client_name == "sdk-a"
  end

  test "HELLO redacts native endpoints before authentication is complete" do
    {status, payload, _state} =
      Commands.execute(
        @op_hello,
        %{"client_name" => "sdk-a"},
        state(%{require_auth: true, authenticated: false, acl_cache: nil})
      )

    assert status == :ok
    assert payload.auth_required == true
    assert payload.route.slots == 1024
    assert payload.route.shard_count >= 1
    refute Map.has_key?(payload.route, :host)
    refute Map.has_key?(payload.route, :native_host)
    refute Map.has_key?(payload.route, :native_port)
    refute Map.has_key?(payload.route, :endpoint)
  end

  test "native AUTH does not reveal whether an ACL username exists" do
    assert :ok = Acl.set_user("known_auth_user", ["on", ">secret"])

    assert {:auth, known_error, _state} =
             Commands.execute(
               @op_auth,
               %{"username" => "known_auth_user", "password" => "wrong"},
               state()
             )

    assert {:auth, missing_error, _state} =
             Commands.execute(
               @op_auth,
               %{"username" => "missing_auth_user", "password" => "wrong"},
               state()
             )

    assert known_error == missing_error
    assert known_error =~ "WRONGPASS"
  end

  test "native AUTH rate limits repeated password verification by peer IP" do
    previous_max_attempts = Application.get_env(:ferricstore, :auth_rate_limit_max_attempts)
    previous_window_ms = Application.get_env(:ferricstore, :auth_rate_limit_window_ms)

    Application.put_env(:ferricstore, :auth_rate_limit_max_attempts, 1)
    Application.put_env(:ferricstore, :auth_rate_limit_window_ms, 60_000)
    :ok = AuthRateLimiter.reset()

    on_exit(fn ->
      restore_env(:auth_rate_limit_max_attempts, previous_max_attempts)
      restore_env(:auth_rate_limit_window_ms, previous_window_ms)
      AuthRateLimiter.reset()
    end)

    assert :ok = Acl.set_user("rate_limited_auth_user", ["on", ">secret"])
    state = state(%{peer: {{10, 20, 30, 40}, 12_345}})

    assert {:auth, first_error, _state} =
             Commands.execute(
               @op_auth,
               %{"username" => "rate_limited_auth_user", "password" => "wrong"},
               state
             )

    assert first_error =~ "WRONGPASS"

    assert {:auth, limited_error, _state} =
             Commands.execute(
               @op_auth,
               %{"username" => "rate_limited_auth_user", "password" => "wrong"},
               %{state | peer: {{10, 20, 30, 40}, 54_321}}
             )

    assert limited_error =~ "too many authentication attempts"
  end

  test "successful native AUTH does not consume the failure budget" do
    previous_max_attempts = Application.get_env(:ferricstore, :auth_rate_limit_max_attempts)
    username = "successful_rate_limited_auth_user"

    Application.put_env(:ferricstore, :auth_rate_limit_max_attempts, 1)
    :ok = AuthRateLimiter.reset()
    assert :ok = Acl.set_user(username, ["on", ">secret"])

    on_exit(fn ->
      Acl.del_user(username)
      restore_env(:auth_rate_limit_max_attempts, previous_max_attempts)
      AuthRateLimiter.reset()
    end)

    state = state(%{peer: {{10, 20, 31, 40}, 12_345}})

    for port <- [12_345, 54_321] do
      assert {:ok, "OK", _state} =
               Commands.execute(
                 @op_auth,
                 %{"username" => username, "password" => "secret"},
                 %{state | peer: {{10, 20, 31, 40}, port}}
               )
    end
  end

  test "native AUTH rejects oversized credentials before authentication" do
    assert {:auth, username_error, _state} =
             Commands.execute(
               @op_auth,
               %{"username" => :binary.copy("u", 1_025), "password" => "password"},
               state()
             )

    assert username_error =~ "username exceeds 1024 bytes"
    refute username_error =~ "too many authentication attempts"

    assert {:auth, password_error, _state} =
             Commands.execute(
               @op_auth,
               %{"username" => "default", "password" => :binary.copy("p", 4_097)},
               state()
             )

    assert password_error =~ "password exceeds 4096 bytes"
    refute password_error =~ "WRONGPASS"
  end

  test "ROUTE returns leader-aware native endpoint metadata" do
    {status, payload, _state} =
      Commands.execute(@op_route, %{"key" => "{sdk-route}:a"}, state())

    assert status == :ok
    assert payload.slot in 0..1023
    assert payload.lane_id == payload.shard + 1
    assert is_binary(payload.owner_node)
    assert is_binary(payload.leader_node)
    assert payload.owner_node == payload.leader_node
    assert is_binary(payload.native_host)
    assert is_integer(payload.native_port)
    assert payload.endpoint.node == payload.leader_node
    assert payload.endpoint.host == payload.native_host
    assert payload.endpoint.native_port == payload.native_port
    assert payload.hint in ["leader", "remote_leader", "local"]
  end

  test "SHARDS returns leader-aware endpoint metadata per slot range" do
    {status, payload, _state} = Commands.execute(@op_shards, %{}, state())

    assert status == :ok
    assert payload.slots == 1024
    assert is_list(payload.ranges)
    assert [range | _] = payload.ranges
    assert range.first_slot <= range.last_slot
    assert range.lane_id == range.shard + 1
    assert range.owner_node == range.leader_node
    assert range.endpoint.node == range.leader_node
    assert range.endpoint.host == range.native_host
    assert range.endpoint.native_port == range.native_port
    assert range.hint in ["leader", "remote_leader", "local"]
  end

  test "COMMAND_EXEC delegates through native AST parser" do
    {status, payload, _state} =
      Commands.execute(@op_command_exec, %{"command" => "PING", "args" => []}, state())

    assert status == :ok
    assert payload == "PONG"
  end

  test "FLOW.POLICY.SET accepts indexed attributes through native opcode" do
    type = "native-policy-indexes-#{System.unique_integer([:positive, :monotonic])}"

    {status, payload, _state} =
      Commands.execute(
        @op_flow_policy_set,
        %{
          "type" => type,
          "indexed_attributes" => ["tenant", "region"],
          "indexed_state_meta" => "version",
          "retry" => %{"max_retries" => 5}
        },
        state()
      )

    assert status == :ok
    assert payload.indexed_attributes == ["tenant", "region"]
    assert payload.indexed_state_meta == "version"
    assert payload.retry.max_retries == 5
  end

  test "FLOW.POLICY.SET accepts max_active_ms through native opcode" do
    type = "native-policy-active-limit-#{System.unique_integer([:positive, :monotonic])}"

    assert {:ok, %{max_active_ms: 30_000}, _state} =
             Commands.execute(
               @op_flow_policy_set,
               %{"type" => type, "max_active_ms" => 30_000},
               state()
             )
  end

  test "native Flow creation opcodes accept max_active_ms" do
    suffix = System.unique_integer([:positive, :monotonic])
    create_id = "native-active-create-#{suffix}"
    many_id = "native-active-create-many-#{suffix}"
    start_id = "native-active-start-#{suffix}"
    partition = "native-active-partition-#{suffix}"

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create,
               %{
                 "id" => create_id,
                 "type" => "native-active",
                 "state" => "queued",
                 "now_ms" => 1_000,
                 "max_active_ms" => 10_000
               },
               state()
             )

    assert {:ok, created} = FerricStore.flow_get(create_id)
    assert created.max_active_ms == 10_000

    assert {:ok, _payload, _state} =
             Commands.execute(
               @op_flow_create_many,
               %{
                 "items" => [%{"id" => many_id, "max_active_ms" => 20_000}],
                 "partition_key" => partition,
                 "type" => "native-active",
                 "state" => "queued",
                 "now_ms" => 1_000
               },
               state()
             )

    assert {:ok, created_many} =
             FerricStore.flow_get(many_id, partition_key: partition)

    assert created_many.max_active_ms == 20_000

    assert {:ok, started, _state} =
             Commands.execute(
               @op_flow_start_and_claim,
               %{
                 "id" => start_id,
                 "type" => "native-active",
                 "initial_state" => "queued",
                 "worker" => "native-worker",
                 "now_ms" => 1_000,
                 "max_active_ms" => 30_000
               },
               state()
             )

    assert started.max_active_ms == 30_000
  end

  test "FLOW.SPAWN_CHILDREN accepts max_active_ms in child payloads" do
    suffix = System.unique_integer([:positive, :monotonic])
    parent_id = "native-active-parent-#{suffix}"
    child_id = "native-active-child-#{suffix}"
    partition = "native-active-family-#{suffix}"

    assert :ok =
             FerricStore.flow_create(parent_id,
               type: "native-parent",
               state: "dispatch",
               partition_key: partition,
               now_ms: 1_000
             )

    assert {:ok, parent} = FerricStore.flow_get(parent_id, partition_key: partition)

    assert {:ok, _payload, _state} =
             Commands.execute(
               @op_flow_spawn_children,
               %{
                 "id" => parent_id,
                 "children" => [
                   %{
                     "id" => child_id,
                     "type" => "native-child",
                     "max_active_ms" => 40_000
                   }
                 ],
                 "partition_key" => partition,
                 "group_id" => "native-group",
                 "wait" => "none",
                 "success" => "dispatched",
                 "failure" => "dispatch_failed",
                 "from_state" => "dispatch",
                 "fencing_token" => parent.fencing_token,
                 "now_ms" => 1_010
               },
               state()
             )

    assert {:ok, child} = FerricStore.flow_get(child_id, partition_key: partition)
    assert child.max_active_ms == 40_000
  end

  test "native Flow opcodes enforce FIFO lane claims" do
    suffix = System.unique_integer([:positive, :monotonic])
    type = "native-fifo-#{suffix}"
    partition = "native:fifo:#{suffix}:partition"
    first_id = "z-native-fifo-first:#{suffix}"
    second_id = "a-native-fifo-second:#{suffix}"

    {status, policy, _state} =
      Commands.execute(
        @op_flow_policy_set,
        %{
          "type" => type,
          "states" => %{"queued" => %{"mode" => "fifo"}}
        },
        state()
      )

    assert status == :ok
    assert policy.states["queued"].mode == :fifo

    for {id, now_ms} <- [{first_id, 1_000}, {second_id, 1_000}] do
      {status, payload, _state} =
        Commands.execute(
          @op_flow_create,
          %{
            "id" => id,
            "type" => type,
            "state" => "queued",
            "partition_key" => partition,
            "payload" => id,
            "now_ms" => now_ms,
            "run_at_ms" => 2_000
          },
          state()
        )

      assert status == :ok
      assert payload == "OK"
    end

    {status, claimed, _state} =
      Commands.execute(
        @op_flow_claim_due,
        %{
          "type" => type,
          "state" => "queued",
          "partition_key" => partition,
          "worker" => "native-fifo-worker",
          "limit" => 10,
          "now_ms" => 2_000
        },
        state()
      )

    assert status == :ok
    assert [%{id: ^first_id}] = claimed

    {status, claimed, _state} =
      Commands.execute(
        @op_flow_claim_due,
        %{
          "type" => type,
          "state" => "queued",
          "partition_key" => partition,
          "worker" => "native-fifo-worker",
          "limit" => 10,
          "now_ms" => 2_001
        },
        state()
      )

    assert status == :ok
    assert claimed == []
  end

  test "FLOW.SEARCH returns indexed records through COMMAND_EXEC and native opcode" do
    suffix = System.unique_integer([:positive, :monotonic])
    type = "native-search-#{suffix}"
    id = "native:search:#{suffix}"
    partition = "native:search:#{suffix}:partition"
    marker = "marker-#{suffix}"
    now = System.system_time(:millisecond)

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               indexed_attributes: ["search_marker"],
               indexed_state_meta: "version"
             )

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "searchable",
               partition_key: partition,
               attributes: %{"search_marker" => marker},
               state_meta: %{"version" => "1"},
               idempotent: true,
               run_at_ms: now,
               now_ms: now
             )

    command_exec_payload = %{
      "command" => "FLOW.SEARCH",
      "args" => [
        "TYPE",
        type,
        "STATE",
        "searchable",
        "ATTRIBUTE",
        "search_marker",
        marker,
        "STATE_META",
        "searchable",
        "version",
        "1",
        "PARTITION",
        partition,
        "COUNT",
        "10",
        "CONSISTENT_PROJECTION",
        "true"
      ]
    }

    assert {:ok, command_records, _state} =
             Commands.execute(@op_command_exec, command_exec_payload, state())

    assert id in flow_record_ids(command_records)

    native_payload = %{
      "type" => type,
      "state" => "searchable",
      "partition_key" => partition,
      "attributes" => %{"search_marker" => marker},
      "state_meta" => %{"searchable" => %{"version" => "1"}},
      "count" => 10,
      "consistent_projection" => true
    }

    assert {:ok, native_records, _state} =
             Commands.execute(@op_flow_search, native_payload, state())

    assert id in flow_record_ids(native_records)
  end

  test "COMMAND_EXEC delegates configured extension commands" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "ext.read", "args" => ["tenant:1"]},
        state()
      )

    assert status == :ok
    assert payload == ["read", "tenant:1"]
  end

  test "COMMAND_EXEC ignores request context unless the native user is trusted" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{
          "command" => "EXT.CONTEXT",
          "args" => [],
          "request_context" => %{"subject" => "client-1"}
        },
        state()
      )

    assert status == :ok
    assert payload == %{}
  end

  test "COMMAND_EXEC attaches trusted request context to extension store" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])
    Application.put_env(:ferricstore, :native_trusted_request_context_users, ["default"])

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{
          "command" => "EXT.CONTEXT",
          "args" => [],
          "request_context" => %{
            "subject" => "client-1",
            "tenant" => "t1",
            "scopes" => ["tenant:t1:write", nil]
          }
        },
        state()
      )

    assert status == :ok

    assert payload == %{
             "subject" => "client-1",
             "tenant" => "t1",
             "scopes" => ["tenant:t1:write"]
           }
  end

  test "PIPELINE attaches top-level trusted request context to extension commands" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])
    Application.put_env(:ferricstore, :native_trusted_request_context_users, ["default"])

    {status, payload, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "request_context" => %{
            "subject" => "client-1",
            "tenant" => "t1",
            "scopes" => "tenant:t1:write invocation:create:*"
          },
          "commands" => [
            %{
              "opcode" => @op_command_exec,
              "lane_id" => 1,
              "request_id" => 7,
              "body" => %{"command" => "EXT.CONTEXT", "args" => []}
            }
          ]
        },
        state()
      )

    assert status == :ok

    assert [
             %{
               "request_id" => 7,
               "status" => "ok",
               "value" => %{
                 "subject" => "client-1",
                 "tenant" => "t1",
                 "scopes" => ["tenant:t1:write", "invocation:create:*"]
               }
             }
           ] = payload
  end

  test "COMMAND_EXEC authorizes extension command and key metadata" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])

    assert :ok =
             Acl.set_user("ext-reader", [
               "on",
               "nopass",
               "-@all",
               "+ext.read",
               "~tenant:*"
             ])

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "EXT.READ", "args" => ["tenant:1"]},
        state_as("ext-reader")
      )

    assert status == :ok
    assert payload == ["read", "tenant:1"]

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "EXT.READ", "args" => ["other:1"]},
        state_as("ext-reader")
      )

    assert status == :noperm
    assert payload =~ "keys mentioned"

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "EXT.WRITE", "args" => ["tenant:1"]},
        state_as("ext-reader")
      )

    assert status == :noperm
    assert payload =~ "ext.write"
  end

  test "COMMAND_EXEC prepares extension key metadata once" do
    Application.put_env(:ferricstore, :command_extensions, [CountingKeyExtension])
    counter_key = {CountingKeyExtension, :key_discovery_calls}
    Process.delete(counter_key)

    assert {:ok, "tenant:one", _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "EXT.COUNTED", "args" => ["tenant:one"]},
               state()
             )

    assert Process.get(counter_key) == 1
  after
    Process.delete({CountingKeyExtension, :key_discovery_calls})
  end

  test "same_shard pipeline validates COMMAND_EXEC routing keys" do
    ctx = FerricStore.Instance.get(:default)
    first = "prepared:pipeline:one"
    first_shard = Ferricstore.Store.Router.shard_for(ctx, first)

    second =
      Enum.find_value(2..1_000, fn suffix ->
        candidate = "prepared:pipeline:#{suffix}"

        if Ferricstore.Store.Router.shard_for(ctx, candidate) != first_shard,
          do: candidate
      end)

    assert is_binary(second)

    assert {:bad_request, message, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "atomicity" => "same_shard",
                 "commands" => [
                   %{
                     "opcode" => @op_command_exec,
                     "request_id" => 1,
                     "body" => %{"command" => "MGET", "args" => [first, second]}
                   }
                 ]
               },
               state()
             )

    assert message =~ "multiple shards"
  end

  @tag :prepared_flow_routing
  test "same_shard pipeline rejects coordinated COMMAND_EXEC Flow routing" do
    type = "prepared-flow-routing-#{System.unique_integer([:positive, :monotonic])}"

    assert {:bad_request, message, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "atomicity" => "same_shard",
                 "commands" => [
                   %{
                     "opcode" => @op_command_exec,
                     "request_id" => 1,
                     "body" => %{
                       "command" => "FLOW.POLICY.GET",
                       "args" => [type]
                     }
                   }
                 ]
               },
               state()
             )

    assert message =~ "coordinated"

    assert {:bad_request, native_message, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "atomicity" => "same_shard",
                 "commands" => [
                   %{
                     "opcode" => @op_flow_policy_set,
                     "request_id" => 2,
                     "body" => %{
                       "type" => type,
                       "indexed_attributes" => ["tenant"]
                     }
                   }
                 ]
               },
               state()
             )

    assert native_message =~ "coordinated"
  end

  @tag :prepared_multi_routing
  test "same_shard pipeline rejects global data mutations and keyspace reads" do
    for {command, args} <- [
          {"FLUSHDB", []},
          {"DBSIZE", []},
          {"KEYS", ["*"]},
          {"RANDOMKEY", []},
          {"SCAN", ["0"]}
        ] do
      assert {:bad_request, message, _state} =
               Commands.execute(
                 @op_pipeline,
                 %{
                   "atomicity" => "same_shard",
                   "commands" => [
                     %{
                       "opcode" => @op_command_exec,
                       "request_id" => 1,
                       "body" => %{"command" => command, "args" => args}
                     }
                   ]
                 },
                 state()
               )

      assert message =~ "coordinated"
    end
  end

  test "COMMAND_EXEC authorizes ACL subcommands before dispatch" do
    assert :ok = Acl.set_user("operator", ["on", "nopass", "+@all", "-acl|setuser", "~*"])

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "ACL", "args" => ["SETUSER", "target", "on"]},
        state_as("operator")
      )

    assert status == :noperm
    assert payload =~ "acl.setuser"
  end

  @tag :acl_command_exec_replication
  test "COMMAND_EXEC dispatches replicated ACL mutations and invalidates cached sessions" do
    join_acl_invalidation_group()

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{
          "command" => "ACL",
          "args" => ["SETUSER", "native-target", "on", "nopass", "-@all", "+GET", "~tenant:*"]
        },
        state()
      )

    assert status == :ok
    assert payload == "OK"
    assert_receive {:acl_invalidate, "native-target"}

    target_state = state_as("native-target")
    assert_native_get_ok("tenant:key", target_state)

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "ACL", "args" => ["GETUSER", "native-target"]},
        state()
      )

    assert status == :ok
    assert "flags" in payload

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "ACL", "args" => ["DELUSER", "native-target"]},
        state()
      )

    assert status == :ok
    assert payload == 1
    assert_receive {:acl_invalidate, "native-target"}

    target_state = ConnAuth.maybe_refresh_acl_cache(target_state, "native-target")
    assert_native_get_denied("tenant:key", target_state)
  end

  test "COMMAND_EXEC enforces scoped keys for management commands" do
    assert :ok =
             Acl.set_user("tenant-a-manager", [
               "on",
               "nopass",
               "+ferricstore.quota",
               "~tenant:a:*"
             ])

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "FERRICSTORE.QUOTA", "args" => ["GET", "tenant:b"]},
        state_as("tenant-a-manager")
      )

    assert status == :noperm
    assert payload =~ "keys mentioned"

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "FERRICSTORE.QUOTA", "args" => ["GET", "tenant:a"]},
        state_as("tenant-a-manager")
      )

    assert status == :error
    assert payload == "ERR unsupported management command"
  end

  test "COMMAND_EXEC enforces admin and dangerous categories for scoped credentials" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    denied_commands = [
      {"ACL", ["SETUSER", "target", "on"], "acl.setuser"},
      {"CONFIG", ["GET", "*"], "config"},
      {"FLUSHDB", [], "flushdb"},
      {"FERRICSTORE.METRICS", [], "ferricstore.metrics"},
      {"FERRICSTORE.NAMESPACE", ["LIST"], "ferricstore.namespace"},
      {"FERRICSTORE.QUOTA", ["GET", "tenant:a"], "ferricstore.quota"},
      {"FLOW.RETENTION_CLEANUP", [], "flow.retention_cleanup"}
    ]

    for {command, args, expected} <- denied_commands do
      {status, payload, _state} =
        Commands.execute(
          @op_command_exec,
          %{"command" => command, "args" => args},
          state
        )

      assert status == :noperm
      assert payload =~ expected
    end
  end

  test "COMMAND_EXEC enforces key scope for scoped credentials" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "SET", "args" => ["tenant:a:key", "value"]},
               state
             )

    assert {:noperm, payload, _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "SET", "args" => ["tenant:b:key", "value"]},
               state
             )

    assert payload =~ "keys mentioned"
  end

  test "typed command ACLs preserve read-modify-write and routing access" do
    assert :ok =
             Acl.set_user("native-write-only", [
               "on",
               "nopass",
               "+@all",
               "%W~secret:*"
             ])

    write_only = state_as("native-write-only")

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_set,
               %{"key" => "secret:key", "value" => "initial"},
               write_only
             )

    assert {:noperm, _message, _state} =
             Commands.execute(
               @op_set,
               %{"key" => "secret:key", "value" => "replacement", "get" => true},
               write_only
             )

    assert {:noperm, _message, _state} =
             Commands.execute(
               @op_hset,
               %{"key" => "secret:hash", "fields" => %{"field" => "value"}},
               write_only
             )

    assert :ok =
             Acl.set_user("native-route-reader", [
               "on",
               "nopass",
               "+@all",
               "%R~route:*"
             ])

    route_reader = state_as("native-route-reader")

    assert {status, _payload, _state} =
             Commands.execute(
               @op_route_batch,
               %{"keys" => ["route:one", "route:two"]},
               route_reader
             )

    refute status == :noperm
  end

  test "FLOW.SEARCH enforces scoped key boundaries" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_command_exec,
               %{
                 "command" => "FLOW.SEARCH",
                 "args" => [
                   "TYPE",
                   "checkout",
                   "ATTRIBUTE",
                   "tenant",
                   "acme",
                   "PARTITION",
                   "tenant:b"
                 ]
               },
               state
             )

    assert message =~ "keys mentioned"

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_search,
               %{
                 "type" => "checkout",
                 "attributes" => %{"tenant" => "acme"},
                 "partition_key" => "tenant:b"
               },
               state
             )

    assert message =~ "keys mentioned"
  end

  test "typed FLOW commands authorize the effective partition instead of the flow id" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    for {opcode, payload} <- [
          {@op_flow_get, %{"id" => "tenant:a:flow", "partition_key" => "tenant:b:partition"}},
          {@op_flow_claim_due,
           %{"type" => "tenant:a:type", "partition_key" => "tenant:b:partition"}},
          {@op_flow_list,
           %{"type" => "tenant:a:type", "opts" => %{"partition_key" => "tenant:b:partition"}}}
        ] do
      assert {:noperm, message, _state} = Commands.execute(opcode, payload, state)
      assert message =~ "keys mentioned"
    end
  end

  test "typed FLOW commands authorize nested options that override direct partitions" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    for {opcode, payload} <- [
          {@op_flow_get,
           %{
             "id" => "tenant:a:flow",
             "partition_key" => "tenant:a:direct",
             "opts" => %{"partition_key" => "tenant:b:nested"}
           }},
          {@op_flow_claim_due,
           %{
             "type" => "tenant:a:type",
             "worker" => "worker",
             "partition_key" => "tenant:a:direct",
             "opts" => %{"partition_key" => "tenant:b:nested"}
           }},
          {@op_flow_list,
           %{
             "type" => "tenant:a:type",
             "partition_key" => "tenant:a:direct",
             "opts" => %{"partition_key" => "tenant:b:nested"}
           }},
          {@op_flow_search,
           %{
             "type" => "checkout",
             "partition_key" => "tenant:a:direct",
             "opts" => %{"partition_key" => "tenant:b:nested"}
           }}
        ] do
      assert {:noperm, message, _state} = Commands.execute(opcode, payload, state)
      assert message =~ "keys mentioned"
    end
  end

  test "typed FLOW.CLAIM_DUE authorizes every requested partition" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_claim_due,
               %{
                 "type" => "tenant:a:type",
                 "partition_keys" => ["tenant:a:partition", "tenant:b:partition"]
               },
               state
             )

    assert message =~ "keys mentioned"
  end

  test "typed FLOW partition-wide commands require unrestricted scope without a partition" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    for {opcode, payload} <- [
          {@op_flow_claim_due, %{"type" => "tenant:a:type", "worker" => "worker"}},
          {@op_flow_list, %{"type" => "tenant:a:type"}}
        ] do
      assert {:noperm, message, _state} = Commands.execute(opcode, payload, state)
      assert message =~ "keys mentioned"
    end
  end

  test "typed FLOW relationship queries require unrestricted scope without a partition" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    for {opcode, field} <- [
          {@op_flow_by_parent, "parent_id"},
          {@op_flow_by_root, "root_id"},
          {@op_flow_by_correlation, "correlation_id"}
        ] do
      assert {:noperm, message, _state} =
               Commands.execute(opcode, %{field => "tenant:a:selector"}, state)

      assert message =~ "keys mentioned"
    end
  end

  test "claim selectors AUTO ANY and GLOBAL require unrestricted key scope" do
    assert :ok =
             Acl.set_user("claim_selector_guard", [
               "on",
               "nopass",
               "-@all",
               "+FLOW.CLAIM_DUE",
               "+FLOW.RECLAIM",
               "~AUTO",
               "~ANY",
               "~GLOBAL"
             ])

    state = state_as("claim_selector_guard")

    for selector <- ["AUTO", "ANY", "GLOBAL"] do
      for {opcode, payload} <- [
            {@op_flow_claim_due,
             %{"type" => "claim-selector", "worker" => "worker", "partition_key" => selector}},
            {@op_flow_reclaim, %{"type" => "claim-selector", "partition_key" => selector}}
          ] do
        assert {:noperm, message, _state} = Commands.execute(opcode, payload, state)
        assert message =~ "keys mentioned"
      end
    end

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_command_exec,
               %{
                 "command" => "FLOW.CLAIM_DUE",
                 "args" => [
                   "claim-selector",
                   "WORKER",
                   "worker",
                   "PARTITION",
                   "AUTO"
                 ]
               },
               state
             )

    assert message =~ "keys mentioned"
  end

  test "raw FLOW ACL extraction cannot confuse option values with partition options" do
    assert :ok =
             Acl.set_user("raw_flow_option_guard", [
               "on",
               "nopass",
               "-@all",
               "+FLOW.CLAIM_DUE",
               "~LEASE_MS"
             ])

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_command_exec,
               %{
                 "command" => "FLOW.CLAIM_DUE",
                 "args" => [
                   "tenant:a:type",
                   "WORKER",
                   "PARTITION",
                   "LEASE_MS",
                   "30000"
                 ]
               },
               state_as("raw_flow_option_guard")
             )

    assert message =~ "keys mentioned"
  end

  test "typed FLOW batch commands authorize shared and item partitions" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")
    suffix = System.unique_integer([:positive, :monotonic])

    for payload <- [
          %{
            "partition_key" => "tenant:b:partition",
            "items" => [%{"id" => "tenant:a:shared", "payload" => %{}}]
          },
          %{
            "items" => [
              %{
                "id" => "tenant:a:mixed",
                "partition_key" => "tenant:b:partition",
                "payload" => %{}
              }
            ]
          },
          %{
            "type" => "batch-list",
            "state" => "queued",
            "items" => [["tenant:a:list:#{suffix}", "tenant:b:partition", %{}]]
          },
          %{
            "type" => "batch-nested-options",
            "state" => "queued",
            "opts" => %{"partition_key" => "tenant:a:ignored"},
            "items" => [
              %{
                "id" => "tenant:a:nested:#{suffix}",
                "partition_key" => "tenant:b:partition",
                "payload" => %{}
              }
            ]
          }
        ] do
      assert {:noperm, message, _state} =
               Commands.execute(@op_flow_create_many, payload, state)

      assert message =~ "keys mentioned"
    end
  end

  test "compact FLOW batches authorize auto-partitioned normalized item ids" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    payload = %{
      "items" => [
        {:id, "tenant:b:compact", :lease_token, "lease", :fencing_token, 1}
      ],
      __wire_flow_items_normalized__: true,
      __wire_flow_opts__: []
    }

    assert {:noperm, message, _state} =
             Commands.execute(@op_flow_complete_many, payload, state)

    assert message =~ "keys mentioned"
  end

  test "FLOW.RUN_STEPS_MANY authorizes each effective item partition" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_run_steps_many,
               %{
                 "partition_key" => "tenant:a:decoy",
                 "type" => "checkout",
                 "worker" => "worker-1",
                 "states" => ["queued", "done"],
                 "items" => [
                   %{"id" => "tenant:b:flow", "partition_key" => "tenant:b:partition"}
                 ]
               },
               state
             )

    assert message =~ "keys mentioned"

    {status, _payload, _state} =
      Commands.execute(
        @op_flow_run_steps_many,
        %{
          "type" => "checkout",
          "worker" => "worker-1",
          "states" => ["queued", "done"],
          "items" => [%{"id" => "tenant:a:auto"}]
        },
        state
      )

    refute status == :noperm
  end

  test "typed FLOW global queries require unrestricted key scope" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, message, _state} =
             Commands.execute(@op_flow_stats, %{"type" => "tenant:a:type"}, state)

    assert message =~ "keys mentioned"

    {status, _payload, _state} =
      Commands.execute(
        @op_flow_stats,
        %{"type" => "tenant:a:type", "partition_key" => "tenant:a:partition"},
        state
      )

    refute status == :noperm
  end

  test "anonymous FLOW.VALUE.PUT requires unrestricted scope in typed and raw paths" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, message, _state} =
             Commands.execute(@op_flow_value_put, %{"value" => "secret"}, state)

    assert message =~ "keys mentioned"

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "FLOW.VALUE.PUT", "args" => ["secret"]},
               state
             )

    assert message =~ "keys mentioned"
  end

  test "schedule ACLs ignore unsupported decoy partitions and protect target partitions" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    for opcode <- [@op_flow_schedule_fire, @op_flow_schedule_delete] do
      assert {:noperm, message, _state} =
               Commands.execute(
                 opcode,
                 %{"id" => "tenant:b:schedule", "partition_key" => "tenant:a:decoy"},
                 state
               )

      assert message =~ "keys mentioned"
    end

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_schedule_create,
               %{
                 "id" => "tenant:a:schedule",
                 "kind" => "delay",
                 "delay_ms" => 1_000,
                 "target" => %{
                   "id" => "tenant:b:target",
                   "type" => "scheduled",
                   "partition_key" => "tenant:b:partition"
                 }
               },
               state
             )

    assert message =~ "keys mentioned"
  end

  test "approval ACLs ignore decoy partitions and protect requested flow scope" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_approval_get,
               %{"id" => "tenant:b:approval", "partition_key" => "tenant:a:decoy"},
               state
             )

    assert message =~ "keys mentioned"

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_approval_request,
               %{
                 "id" => "tenant:a:approval",
                 "flow_id" => "tenant:b:flow",
                 "scope" => "tenant:b:scope"
               },
               state
             )

    assert message =~ "keys mentioned"
  end

  test "PIPELINE preserves COMMAND_EXEC and typed command ACL checks" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    {status, payload, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "commands" => [
            %{
              "opcode" => @op_command_exec,
              "request_id" => 1,
              "body" => %{"command" => "PING", "args" => []}
            },
            %{
              "opcode" => @op_command_exec,
              "request_id" => 2,
              "body" => %{"command" => "FERRICSTORE.METRICS", "args" => []}
            },
            %{
              "opcode" => @op_set,
              "request_id" => 3,
              "body" => %{"key" => "tenant:b:key", "value" => "value"}
            }
          ]
        },
        state
      )

    assert status == :ok
    assert Enum.map(payload, & &1["status"]) == ["ok", "noperm", "noperm"]
    assert Enum.at(payload, 1)["value"] =~ "ferricstore.metrics"
    assert Enum.at(payload, 2)["value"] =~ "keys mentioned"
  end

  test "compact PIPELINE fallback preserves key ACL checks" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    {status, payload, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "compact_pipeline" => {1, [{"tenant:a:key", "ok"}, {"tenant:b:key", "denied"}]}
        },
        state
      )

    assert status == :ok
    assert [["ok", "OK"], ["noperm", message]] = payload
    assert message =~ "keys mentioned"
  end

  test "native transactions deny scoped credential boundary escapes before queueing" do
    put_platform_scoped_user("platform_scoped")
    state = session_state_as("platform_scoped")

    assert {:ok, "OK", state} =
             Session.execute(%{"command" => "MULTI", "args" => []}, state)

    assert {:noperm, admin_payload, admin_state} =
             Session.execute(%{"command" => "FLUSHDB", "args" => []}, state)

    assert admin_payload =~ "flushdb"
    assert admin_state.multi_queue == []

    assert {:noperm, key_payload, key_state} =
             Session.execute(
               %{"command" => "SET", "args" => ["tenant:b:key", "value"]},
               state
             )

    assert key_payload =~ "keys mentioned"
    assert key_state.multi_queue == []

    assert {:ok, "QUEUED", queued_state} =
             Session.execute(
               %{"command" => "SET", "args" => ["tenant:a:key", "value"]},
               state
             )

    assert queued_state.multi_queue_count == 1

    assert [%PreparedCommand{command: "SET", write_keys: ["tenant:a:key"]}] =
             queued_state.multi_queue

    assert {:ok, ["OK"], executed_state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, queued_state)

    assert executed_state.multi_state == :none
    assert executed_state.multi_queue == []
  end

  @tag :prepared_multi_routing
  test "native MULTI retains prepared multi-key routing through EXEC" do
    ctx = FerricStore.Instance.get(:default)
    first = "native-multi-routing:#{System.unique_integer([:positive, :monotonic])}:one"
    first_idx = Ferricstore.Store.Router.shard_for(ctx, first)

    second =
      Enum.find_value(2..1_000, fn suffix ->
        candidate = "#{first}:#{suffix}"

        if Ferricstore.Store.Router.shard_for(ctx, candidate) != first_idx,
          do: candidate
      end)

    assert is_binary(second)
    second_idx = Ferricstore.Store.Router.shard_for(ctx, second)
    first_before = Ferricstore.Store.WriteVersion.get(first_idx)
    second_before = Ferricstore.Store.WriteVersion.get(second_idx)

    assert {:ok, "OK", multi_state} =
             Session.execute(%{"command" => "MULTI", "args" => []}, session_state_as("default"))

    assert {:ok, "QUEUED", queued_state} =
             Session.execute(
               %{"command" => "MSET", "args" => [first, "one", second, "two"]},
               multi_state
             )

    assert [%PreparedCommand{routing_keys: [^first, ^second]}] = queued_state.multi_queue

    assert {:ok, ["OK"], _state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, queued_state)

    assert Ferricstore.Store.Router.get(ctx, first) == "one"
    assert Ferricstore.Store.Router.get(ctx, second) == "two"
    assert Ferricstore.Store.WriteVersion.get(first_idx) == first_before + 1
    assert Ferricstore.Store.WriteVersion.get(second_idx) == second_before + 1
  end

  @tag :prepared_multi_routing
  test "native MULTI rejects coordinated prepared commands before queueing" do
    assert {:ok, "OK", multi_state} =
             Session.execute(%{"command" => "MULTI", "args" => []}, session_state_as("default"))

    assert {:error, message, rejected_state} =
             Session.execute(
               %{"command" => "FLOW.POLICY.GET", "args" => ["coordinated-policy"]},
               multi_state
             )

    assert message =~ "coordinated"
    assert rejected_state.multi_error
    assert rejected_state.multi_queue == []
    assert rejected_state.multi_queue_count == 0

    assert {:error, abort_message, final_state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, rejected_state)

    assert abort_message =~ "EXECABORT"
    assert final_state.multi_state == :none
  end

  @tag :prepared_multi_routing
  test "native MULTI rejects global data mutations before queueing" do
    assert {:ok, "OK", multi_state} =
             Session.execute(%{"command" => "MULTI", "args" => []}, session_state_as("default"))

    assert {:error, message, rejected_state} =
             Session.execute(%{"command" => "FLUSHDB", "args" => []}, multi_state)

    assert message =~ "coordinated"
    assert rejected_state.multi_error
    assert rejected_state.multi_queue == []
  end

  test "replicated ACL disable denies rotated-out native service credential sessions only" do
    join_acl_invalidation_group()

    assert :ok =
             Acl.set_user("platform_worker_old", [
               "on",
               "nopass",
               "-@all",
               "+get",
               "~tenant:a:*"
             ])

    assert :ok =
             Acl.set_user("platform_worker_new", [
               "on",
               "nopass",
               "-@all",
               "+get",
               "~tenant:a:*"
             ])

    old_state = state_as("platform_worker_old")
    new_state = state_as("platform_worker_new")

    assert_native_get_ok("tenant:a:key", old_state)
    assert_native_get_ok("tenant:a:key", new_state)

    assert :ok = apply_raft_acl({:acl_setuser, "platform_worker_old", ["off"]})
    assert_receive {:acl_invalidate, "platform_worker_old"}

    old_state = ConnAuth.maybe_refresh_acl_cache(old_state, "platform_worker_old")
    new_state = ConnAuth.maybe_refresh_acl_cache(new_state, "platform_worker_old")

    assert_native_get_denied("tenant:a:key", old_state)
    assert_native_get_ok("tenant:a:key", new_state)
  end

  @tag :acl_direct_invalidation
  test "direct ACL mutations invalidate cached native permissions" do
    join_acl_invalidation_group()

    assert :ok =
             Acl.set_user("direct-revoke", [
               "on",
               "nopass",
               "-@all",
               "+get",
               "~tenant:direct:*"
             ])

    assert_receive {:acl_invalidate, "direct-revoke"}

    state = state_as("direct-revoke")
    assert_native_get_ok("tenant:direct:key", state)

    assert :ok = Acl.set_user("direct-revoke", ["off"])
    assert_receive {:acl_invalidate, "direct-revoke"}

    state = ConnAuth.maybe_refresh_acl_cache(state, "direct-revoke")
    assert_native_get_denied("tenant:direct:key", state)
  end

  test "replicated ACL delete denies active native service credential sessions" do
    join_acl_invalidation_group()

    assert :ok =
             Acl.set_user("platform_revoke_abcd", [
               "on",
               "nopass",
               "-@all",
               "+get",
               "~tenant:revoke:*"
             ])

    state = state_as("platform_revoke_abcd")
    assert_native_get_ok("tenant:revoke:key", state)

    assert :ok = apply_raft_acl({:acl_deluser, "platform_revoke_abcd"})
    assert_receive {:acl_invalidate, "platform_revoke_abcd"}

    state = ConnAuth.maybe_refresh_acl_cache(state, "platform_revoke_abcd")
    assert_native_get_denied("tenant:revoke:key", state)
  end

  test "replicated ACL delete of missing credential does not invalidate other sessions" do
    join_acl_invalidation_group()

    assert :ok =
             Acl.set_user("platform_other_abcd", [
               "on",
               "nopass",
               "-@all",
               "+get",
               "~tenant:other:*"
             ])

    state = state_as("platform_other_abcd")
    assert_native_get_ok("tenant:other:key", state)

    assert_receive {:acl_invalidate, "platform_other_abcd"}

    assert {:error, "ERR User 'platform_missing_abcd' does not exist"} =
             apply_raft_acl({:acl_deluser, "platform_missing_abcd"})

    refute_receive {:acl_invalidate, _username}, 100

    state = ConnAuth.maybe_refresh_acl_cache(state, "platform_missing_abcd")
    assert_native_get_ok("tenant:other:key", state)
  end

  test "native scope-based governance commands enforce key ACLs" do
    assert :ok =
             Acl.set_user("scope_guard", [
               "on",
               "nopass",
               "-@all",
               "+FLOW.CIRCUIT.OPEN",
               "+FLOW.CIRCUIT.GET",
               "+FLOW.BUDGET.RESERVE",
               "+FLOW.LIMIT.LEASE",
               "~tenant:a:*"
             ])

    state = state_as("scope_guard")

    for {opcode, payload} <- [
          {@op_flow_circuit_open,
           %{"scope" => "tenant:b:effect", "failure_threshold" => 1, "open_ms" => 1_000}},
          {@op_flow_circuit_get, %{"scope" => "tenant:b:effect"}},
          {@op_flow_budget_reserve, %{"scope" => "tenant:b:budget", "amount" => 10}},
          {@op_flow_limit_lease, %{"scope" => "tenant:b:limit", "limit" => 10}}
        ] do
      assert {:noperm, message, _state} = Commands.execute(opcode, payload, state)
      assert message =~ "NOPERM"
    end

    {status, _payload, _state} =
      Commands.execute(
        @op_flow_circuit_open,
        %{"scope" => "tenant:a:effect", "failure_threshold" => 1, "open_ms" => 1_000},
        state
      )

    refute status == :noperm
  end

  test "CLIENT TRACKING is explicitly rejected after text protocol removal" do
    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "CLIENT", "args" => ["TRACKING", "ON"]},
        state()
      )

    assert status == :error
    assert payload =~ "CLIENT TRACKING is not supported"
  end

  defp state(overrides \\ %{}) do
    %{
      client_id: System.unique_integer([:positive, :monotonic]),
      client_name: nil,
      username: "default",
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access,
      peer: {{127, 0, 0, 1}, 12_345},
      created_at: System.monotonic_time(:millisecond),
      instance_ctx: FerricStore.Instance.get(:default),
      compression: :none,
      compact_flow_responses: false,
      subscribed_events: MapSet.new(),
      flow_wake_subscriptions: MapSet.new()
    }
    |> Map.merge(overrides)
  end

  defp state_as(username) do
    state(%{
      username: username,
      acl_cache: ConnAuth.build_acl_cache(username),
      require_auth: ConnAuth.user_requires_auth?(username)
    })
  end

  defp session_state_as(username) do
    username
    |> state_as()
    |> Map.merge(%{
      multi_state: :none,
      multi_queue: [],
      multi_queue_count: 0,
      multi_error: false,
      watched_keys: %{},
      pubsub_channels: nil,
      pubsub_patterns: nil
    })
  end

  defp join_acl_invalidation_group do
    group = ConnAuth.acl_pg_group()
    :ok = :pg.join(group, group, self())

    on_exit(fn ->
      try do
        :pg.leave(group, group, self())
      catch
        :error, _reason -> :ok
      end
    end)
  end

  defp apply_raft_acl(command) do
    Task.async(fn -> Acl.handle_raft_command(command) end)
    |> Task.await()
  end

  defp put_platform_scoped_user(username) do
    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "-@all",
               "+PING",
               "+@read",
               "+@write",
               "+MULTI",
               "+EXEC",
               "+DISCARD",
               "-@dangerous",
               "-@admin",
               "~tenant:a:*",
               "&tenant:a:*"
             ])
  end

  defp assert_native_get_ok(key, state) do
    assert {:ok, _payload, _state} =
             Commands.execute(@op_command_exec, %{"command" => "GET", "args" => [key]}, state)
  end

  defp assert_native_get_denied(key, state) do
    assert {:noperm, message, _state} =
             Commands.execute(@op_command_exec, %{"command" => "GET", "args" => [key]}, state)

    assert message =~ "NOPERM"
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp flow_record_ids(records) when is_list(records) do
    Enum.map(records, fn record ->
      Map.get(record, :id) || Map.get(record, "id")
    end)
  end

  defp schema_names(payload), do: Map.keys(payload.schemas)
  defp opcode_names(payload), do: Enum.map(payload.opcodes, & &1["name"])
end
