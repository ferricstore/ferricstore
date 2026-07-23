defmodule Ferricstore.Flow.Query.CommandsTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Flow.Query.Commands

  test "declares one administrative, read-only, keyless management command" do
    assert [entry] = Commands.commands()
    assert entry.name == "FLOW.QUERY.INDEXES"
    assert entry.arity == -1
    assert entry.flags == ["readonly"]
    assert entry.access == :read
    assert entry.acl_categories == [:admin]
    assert entry.first_key == 0
    assert entry.last_key == 0
    assert entry.step == 0
    assert {:ok, []} = Commands.keys(entry.name, [])
    assert {:ok, []} = Commands.keys(entry.name, ["index-id"])
  end

  test "returns actionable usage and availability errors" do
    ctx = %{name: :missing_query_index_services}

    assert {:error, usage} = Commands.handle("FLOW.QUERY.INDEXES", ["one", "two"], ctx)
    assert usage =~ "FLOW.QUERY.INDEXES [index-id]"

    assert {:error, unavailable} = Commands.handle("FLOW.QUERY.INDEXES", [], ctx)
    assert unavailable =~ "registry unavailable"
    assert unavailable =~ "query services"

    assert {:error, invalid_id} =
             Commands.handle("FLOW.QUERY.INDEXES", ["invalid/index"], ctx)

    assert invalid_id =~ "invalid query index id"
    assert invalid_id =~ "FLOW.QUERY.INDEXES [index-id]"
  end

  test "the default OSS command prepares as an extension instead of a static native AST" do
    previous = Application.get_env(:ferricstore, :command_extensions)
    Application.delete_env(:ferricstore, :command_extensions)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :command_extensions)
        configured -> Application.put_env(:ferricstore, :command_extensions, configured)
      end
    end)

    assert {:ok,
            %PreparedCommand{
              command: "FLOW.QUERY.INDEXES",
              ast: {:extension_command, Commands, "FLOW.QUERY.INDEXES", [], :read},
              acl_keys: [],
              routing_scope: :coordinated
            }} = Dispatcher.prepare_raw("FLOW.QUERY.INDEXES", [])
  end

  test "does not claim unrelated extension commands" do
    assert :not_found = Commands.handle("GET", [], %{})
    assert :error = Commands.keys("GET", [])
  end
end
