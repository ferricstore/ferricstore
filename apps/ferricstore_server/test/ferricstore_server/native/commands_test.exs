defmodule FerricstoreServer.Native.CommandsTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Native.Commands
  alias FerricstoreServer.Native.Session

  @op_hello 0x0001
  @op_options 0x000B
  @op_pipeline 0x000E
  @op_command_exec 0x0100
  @op_set 0x0102

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
    assert "FLOW.BUDGET.RESERVE" in opcode_names(payload)
    assert "GET" in opcode_names(payload)
    assert "SET" in opcode_names(payload)
    refute "GET.COMPACT" in opcode_names(payload)
  end

  test "HELLO returns native route metadata only" do
    {status, payload, new_state} =
      Commands.execute(@op_hello, %{"client_name" => "sdk-a"}, state())

    assert status == :ok
    assert payload.protocol == "ferricstore-native"
    assert payload.route.native_port == Application.get_env(:ferricstore, :native_port, 6388)
    refute Map.has_key?(payload.route, String.to_atom("resp" <> "_port"))
    assert new_state.client_name == "sdk-a"
  end

  test "COMMAND_EXEC delegates through native AST parser" do
    {status, payload, _state} =
      Commands.execute(@op_command_exec, %{"command" => "PING", "args" => []}, state())

    assert status == :ok
    assert payload == "PONG"
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

  test "COMMAND_EXEC dispatches ACL subcommands through server management adapter" do
    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{
          "command" => "ACL",
          "args" => ["SETUSER", "native-target", "on", "nopass", "+PING", "~*"]
        },
        state()
      )

    assert status == :ok
    assert payload == "OK"

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

    assert {:error, "ERR User 'platform_missing_abcd' does not exist"} =
             apply_raft_acl({:acl_deluser, "platform_missing_abcd"})

    refute_receive {:acl_invalidate, _username}, 100

    state = ConnAuth.maybe_refresh_acl_cache(state, "platform_missing_abcd")
    assert_native_get_ok("tenant:other:key", state)
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

  defp schema_names(payload), do: Map.keys(payload.schemas)
  defp opcode_names(payload), do: Enum.map(payload.opcodes, & &1["name"])
end
