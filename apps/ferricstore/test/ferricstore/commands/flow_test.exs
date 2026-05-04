defmodule Ferricstore.Commands.FlowTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.{MockStore, ShardHelpers}

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  defp uid(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"

  test "dispatches Flow create/get/list/history through Rust AST" do
    id = uid("flow-command")

    assert %{"id" => ^id, "type" => "checkout", "state" => "queued", "priority" => 2} =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [
                 id,
                 "TYPE",
                 "checkout",
                 "STATE",
                 "queued",
                 "RUN_AT",
                 "1000",
                 "PRIORITY",
                 "2",
                 "HISTORY_MAX_EVENTS",
                 "5"
               ],
               MockStore.make()
             )

    assert %{"id" => ^id, "type" => "checkout"} =
             Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())

    assert [%{"id" => ^id}] =
             Dispatcher.dispatch("FLOW.LIST", ["checkout", "COUNT", "10"], MockStore.make())

    assert [[_event_id, %{"event" => "created", "version" => "1"}]] =
             Dispatcher.dispatch("FLOW.HISTORY", [id, "COUNT", "10"], MockStore.make())
  end

  test "dispatches Flow claim and complete through Rust AST" do
    id = uid("flow-command-complete")

    assert %{"id" => ^id} =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [id, "TYPE", "email", "RUN_AT", "1000"],
               MockStore.make()
             )

    assert [
             %{
               "id" => ^id,
               "state" => "running",
               "lease_token" => lease_token,
               "fencing_token" => fencing_token
             }
           ] =
             Dispatcher.dispatch(
               "FLOW.CLAIM_DUE",
               ["email", "WORKER", "worker-a", "LEASE_MS", "30000", "LIMIT", "1", "NOW", "1000"],
               MockStore.make()
             )

    assert %{"id" => ^id, "state" => "completed", "result_ref" => "result:1"} =
             Dispatcher.dispatch(
               "FLOW.COMPLETE",
               [
                 id,
                 lease_token,
                 "FENCING",
                 Integer.to_string(fencing_token),
                 "RESULT_REF",
                 "result:1"
               ],
               MockStore.make()
             )
  end

  test "dispatches Flow AST parse errors without calling embedded API" do
    assert {:error, "ERR flow limit must be a positive integer"} =
             Dispatcher.dispatch_ast(
               {:flow_claim_due, "checkout",
                {:error, "ERR flow limit must be a positive integer"}},
               MockStore.make()
             )

    assert {:error, "ERR wrong number of arguments for 'flow.complete' command"} =
             Dispatcher.dispatch_ast(
               {:flow_complete,
                {:error, "ERR wrong number of arguments for 'flow.complete' command"}},
               MockStore.make()
             )
  end

  test "Flow commands are visible in COMMAND catalog" do
    names = Dispatcher.dispatch("COMMAND", ["LIST"], MockStore.make())
    assert "flow.create" in names
    assert "flow.claim_due" in names
    assert "flow.complete" in names

    assert [["flow.create", _arity, flags, 1, 1, 1]] =
             Dispatcher.dispatch("COMMAND", ["INFO", "flow.create"], MockStore.make())

    assert "write" in flags
    assert {:ok, ["flow-id"]} = Ferricstore.Commands.Catalog.get_keys("flow.create", ["flow-id"])
  end
end
