defmodule FerricstoreServer.Native.CommandsTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Native.Commands

  @op_hello 0x0001
  @op_options 0x000B
  @op_command_exec 0x0100

  setup do
    ConnRegistry.init_table()
    FerricstoreServer.Acl.reset!()
    {:ok, _} = Application.ensure_all_started(:ferricstore)

    on_exit(fn ->
      FerricstoreServer.Acl.reset!()
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
      peer: {{127, 0, 0, 1}, 12345},
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
