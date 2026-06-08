defmodule Ferricstore.Commands.FlowTest.Sections.DispatchesFlowRewindThroughRustAst do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Test.{MockStore, ShardHelpers}

      test "dispatches Flow rewind through Rust AST" do
        id = uid("flow-command-rewind")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [id, "TYPE", "email", "RUN_AT", "1000", "NOW", "1000"],
                   MockStore.make()
                 )

        assert [[created_event_id, _fields]] =
                 Dispatcher.dispatch("FLOW.HISTORY", [id, "COUNT", "10"], MockStore.make())

        assert [%{"lease_token" => lease_token, "fencing_token" => fencing_token}] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   [
                     "email",
                     "WORKER",
                     "worker-a",
                     "LEASE_MS",
                     "30000",
                     "LIMIT",
                     "1",
                     "NOW",
                     "1000"
                   ],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.COMPLETE",
                   [id, lease_token, "FENCING", Integer.to_string(fencing_token), "NOW", "2000"],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.REWIND",
                   [
                     id,
                     "TO_EVENT",
                     created_event_id,
                     "RUN_AT",
                     "5000",
                     "EXPECT_STATE",
                     "completed",
                     "NOW",
                     "3000"
                   ],
                   MockStore.make()
                 )

        assert %{"id" => ^id, "state" => "queued", "next_run_at_ms" => 5000} =
                 Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())
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

        assert {:error, "ERR flow to_event is required"} =
                 Dispatcher.dispatch_ast(
                   {:flow_rewind, "flow", {:error, "ERR flow to_event is required"}},
                   MockStore.make()
                 )
      end

      test "dispatches Flow policy AST through embedded API" do
        type = uid("flow-policy-ast")

        assert %{"type" => ^type, "retry" => %{"max_retries" => 5}} =
                 Dispatcher.dispatch_ast(
                   {:flow_policy_set, type,
                    [
                      retention: [ttl_ms: 60_000, history_max_events: 320],
                      retry: [
                        max_retries: 5,
                        backoff: [kind: :fixed, base_ms: 1_000, max_ms: 5_000, jitter_pct: 0],
                        exhausted_to: "failed"
                      ],
                      states: %{
                        "charge_card" => [
                          retry: [max_retries: 1, exhausted_to: "payment_failed"],
                          retention: [
                            ttl_ms: 30_000,
                            history_max_events: 160
                          ]
                        ]
                      }
                    ]},
                   MockStore.make()
                 )

        assert %{
                 "retention" => %{
                   "ttl_ms" => 60_000,
                   "history_max_events" => 320
                 }
               } =
                 Dispatcher.dispatch_ast({:flow_policy_get, type, []}, MockStore.make())

        refute Map.has_key?(
                 Dispatcher.dispatch_ast({:flow_policy_get, type, []}, MockStore.make())[
                   "retention"
                 ],
                 "history_hot_max_events"
               )

        assert %{
                 "type" => ^type,
                 "state" => "charge_card",
                 "retry" => %{"max_retries" => 1, "exhausted_to" => "payment_failed"},
                 "retention" => %{
                   "ttl_ms" => 30_000,
                   "history_max_events" => 160
                 }
               } =
                 Dispatcher.dispatch_ast(
                   {:flow_policy_get, type, [state: "charge_card"]},
                   MockStore.make()
                 )
      end

      test "dispatches Flow policy commands through Rust AST" do
        type = uid("flow-policy-rust")

        assert %{
                 "type" => ^type,
                 "retry" => %{
                   "max_retries" => 5,
                   "backoff" => %{
                     "kind" => :fixed,
                     "base_ms" => 1000,
                     "max_ms" => 5000,
                     "jitter_pct" => 0
                   },
                   "exhausted_to" => "failed"
                 }
               } =
                 Dispatcher.dispatch(
                   "FLOW.POLICY.SET",
                   [
                     type,
                     "MAX_RETRIES",
                     "5",
                     "BACKOFF",
                     "FIXED",
                     "BASE_MS",
                     "1000",
                     "MAX_MS",
                     "5000",
                     "JITTER_PCT",
                     "0",
                     "RETENTION_TTL",
                     "60000",
                     "HISTORY_MAX_EVENTS",
                     "320",
                     "EXHAUSTED_TO",
                     "failed",
                     "STATE",
                     "charge_card",
                     "MAX_RETRIES",
                     "1",
                     "EXHAUSTED_TO",
                     "payment_failed",
                     "RETENTION_TTL",
                     "30000",
                     "HISTORY_MAX_EVENTS",
                     "160"
                   ],
                   MockStore.make()
                 )

        assert %{
                 "retention" => %{
                   "ttl_ms" => 60_000,
                   "history_max_events" => 320
                 }
               } =
                 Dispatcher.dispatch("FLOW.POLICY.GET", [type], MockStore.make())

        refute Map.has_key?(
                 Dispatcher.dispatch("FLOW.POLICY.GET", [type], MockStore.make())["retention"],
                 "history_hot_max_events"
               )

        assert %{
                 "type" => ^type,
                 "state" => "charge_card",
                 "retry" => %{"max_retries" => 1, "exhausted_to" => "payment_failed"},
                 "retention" => %{
                   "ttl_ms" => 30_000,
                   "history_max_events" => 160
                 }
               } =
                 Dispatcher.dispatch(
                   "FLOW.POLICY.GET",
                   [type, "STATE", "charge_card"],
                   MockStore.make()
                 )

        assert {:error, "ERR syntax error"} =
                 Dispatcher.dispatch(
                   "FLOW.POLICY.SET",
                   [type, "STATE", "queued", "MAX_RETRIES"],
                   MockStore.make()
                 )

        assert {:error, "ERR flow retention history_hot_max_events is internal"} =
                 Dispatcher.dispatch(
                   "FLOW.POLICY.SET",
                   [type, "HISTORY_HOT_MAX_EVENTS", "1"],
                   MockStore.make()
                 )
      end

      test "Flow commands are visible in COMMAND catalog" do
        names = Dispatcher.dispatch("COMMAND", ["LIST"], MockStore.make())
        assert "flow.create" in names
        assert "flow.create_many" in names
        assert "flow.spawn_children" in names
        assert "flow.policy.set" in names
        assert "flow.policy.get" in names
        assert "flow.retention_cleanup" in names
        assert "flow.claim_due" in names
        assert "flow.failures" in names
        assert "flow.terminals" in names
        assert "flow.complete" in names
        assert "flow.rewind" in names

        assert [["flow.create", _arity, flags, 1, 1, 1]] =
                 Dispatcher.dispatch("COMMAND", ["INFO", "flow.create"], MockStore.make())

        assert "write" in flags

        assert {:ok, ["flow-id"]} =
                 Ferricstore.Commands.Catalog.get_keys("flow.create", ["flow-id"])

        assert {:ok, ["checkout"]} =
                 Ferricstore.Commands.Catalog.get_keys("flow.policy.set", ["checkout"])

        assert [["flow.retention_cleanup", _arity, cleanup_flags, 0, 0, 0]] =
                 Dispatcher.dispatch(
                   "COMMAND",
                   ["INFO", "flow.retention_cleanup"],
                   MockStore.make()
                 )

        assert "admin" in cleanup_flags
      end
    end
  end
end
