defmodule Ferricstore.Commands.FlowTest.Sections.DispatchesFlowStateMetaThroughRustAst do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Test.MockStore

      test "dispatches Flow state_meta and indexed_state_meta through Rust AST" do
        partition = uid("tenant-state-meta-command")
        type = uid("flow-command-state-meta")
        id = uid("flow-command-state-meta-id")

        assert %{"type" => ^type, "indexed_state_meta" => "version"} =
                 Dispatcher.dispatch(
                   "FLOW.POLICY.SET",
                   [type, "INDEXED_STATE_META", "version"],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [
                     id,
                     "TYPE",
                     type,
                     "STATE",
                     "accept",
                     "PARTITION",
                     partition,
                     "STATE_META",
                     "version",
                     "1",
                     "RUN_AT",
                     "1000",
                     "NOW",
                     "1000"
                   ],
                   MockStore.make()
                 )

        assert %{
                 "id" => ^id,
                 "state_meta" => %{"accept" => %{"version" => "1"}}
               } =
                 Dispatcher.dispatch("FLOW.GET", [id, "PARTITION", partition], MockStore.make())

        assert [
                 %{
                   "id" => ^id,
                   "lease_token" => lease_token,
                   "fencing_token" => fencing_token
                 }
               ] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   [
                     type,
                     "STATE",
                     "accept",
                     "WORKER",
                     "worker-state-meta",
                     "LIMIT",
                     "1",
                     "PARTITION",
                     partition,
                     "NOW",
                     "1001"
                   ],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.COMPLETE",
                   [
                     id,
                     lease_token,
                     "FENCING",
                     Integer.to_string(fencing_token),
                     "PARTITION",
                     partition,
                     "STATE_META",
                     "version",
                     "3",
                     "NOW",
                     "1002"
                   ],
                   MockStore.make()
                 )

        assert %{
                 "id" => ^id,
                 "state_meta" => %{
                   "accept" => %{"version" => "1"},
                   "completed" => %{"version" => "3"}
                 }
               } =
                 Dispatcher.dispatch("FLOW.GET", [id, "PARTITION", partition], MockStore.make())

        assert {:ok, [%{id: ^id}]} =
                 FerricStore.flow_search(
                   type: type,
                   partition_key: partition,
                   state_meta: %{"accept" => %{"version" => "1"}},
                   consistent_projection: true,
                   count: 10
                 )

        assert {:ok, [%{id: ^id}]} =
                 FerricStore.flow_search(
                   type: type,
                   partition_key: partition,
                   state_meta: %{"completed" => %{"version" => "3"}},
                   consistent_projection: true,
                   count: 10
                 )

        assert {:error, "ERR flow indexed_state_meta is type-level only"} =
                 Dispatcher.dispatch(
                   "FLOW.POLICY.SET",
                   [type, "STATE", "accept", "INDEXED_STATE_META", "version"],
                   MockStore.make()
                 )

        assert {:error, "ERR flow indexed_attributes is type-level only"} =
                 Dispatcher.dispatch(
                   "FLOW.POLICY.SET",
                   [type, "STATE", "accept", "INDEXED_ATTRIBUTES", "tenant"],
                   MockStore.make()
                 )
      end
    end
  end
end
