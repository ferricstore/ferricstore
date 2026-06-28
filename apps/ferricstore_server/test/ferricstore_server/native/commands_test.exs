defmodule FerricstoreServer.Native.CommandsTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Native.Commands

  @op_hello 0x0001
  @op_options 0x000B
  @op_pipeline 0x000E
  @op_command_exec 0x0100

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

    Application.delete_env(:ferricstore, :command_extensions)
    Application.delete_env(:ferricstore, :native_trusted_request_context_users)

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

  defp schema_names(payload), do: Map.keys(payload.schemas)
  defp opcode_names(payload), do: Enum.map(payload.opcodes, & &1["name"])
end
