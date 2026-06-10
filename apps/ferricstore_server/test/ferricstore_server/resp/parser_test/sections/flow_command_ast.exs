defmodule FerricstoreServer.Resp.ParserTest.Sections.FlowCommandAst do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.Parser

      describe "Flow command AST" do
        test "parses Flow write commands into typed Rust AST" do
          assert {:ok,
                  [
                    {:command, "FLOW.CREATE",
                     [
                       "flow-1",
                       "TYPE",
                       "checkout",
                       "STATE",
                       "queued",
                       "RUN_AT",
                       "1000",
                       "PRIORITY",
                       "2",
                       "PARTITION",
                       "tenant-a",
                       "RETENTION_TTL",
                       "5000",
                       "HISTORY_MAX_EVENTS",
                       "10",
                       "IDEMPOTENT",
                       "true"
                     ],
                     {:flow_create, "flow-1",
                      [
                        type: "checkout",
                        state: "queued",
                        run_at_ms: 1000,
                        priority: 2,
                        partition_key: "tenant-a",
                        retention_ttl_ms: 5000,
                        history_max_events: 10,
                        idempotent: true
                      ]}, ["tenant-a"]},
                    {:command, "FLOW.VALUE.PUT",
                     ["shared-payload", "PARTITION", "tenant-a", "OWNER_FLOW_ID", "flow-1"],
                     {:flow_value_put, "shared-payload",
                      [partition_key: "tenant-a", owner_flow_id: "flow-1"]}, ["tenant-a"]},
                    {:command, "FLOW.CLAIM_DUE",
                     [
                       "checkout",
                       "WORKER",
                       "worker-a",
                       "LEASE_MS",
                       "30000",
                       "LIMIT",
                       "100",
                       "NOW",
                       "1000"
                     ],
                     {:flow_claim_due, "checkout",
                      [worker: "worker-a", lease_ms: 30000, limit: 100, now_ms: 1000]},
                     ["checkout"]},
                    {:command, "FLOW.RECLAIM",
                     [
                       "checkout",
                       "WORKER",
                       "worker-b",
                       "LEASE_MS",
                       "30000",
                       "LIMIT",
                       "10",
                       "NOW",
                       "2000"
                     ],
                     {:flow_reclaim, "checkout",
                      [worker: "worker-b", lease_ms: 30000, limit: 10, now_ms: 2000]},
                     ["checkout"]},
                    {:command, "FLOW.COMPLETE",
                     ["flow-1", "lease-1", "FENCING", "1", "RESULT", "result-1"],
                     {:flow_complete, "flow-1", "lease-1",
                      [fencing_token: 1, result: "result-1"]}, ["flow-1"]},
                    {:command, "FLOW.TRANSITION",
                     ["flow-1", "queued", "running", "FENCING", "1", "LEASE_TOKEN", "lease-1"],
                     {:flow_transition, "flow-1", "queued", "running",
                      [fencing_token: 1, lease_token: "lease-1"]}, ["flow-1"]},
                    {:command, "FLOW.TRANSITION_MANY",
                     [
                       "tenant-a",
                       "queued",
                       "running",
                       "PAYLOAD_REF",
                       "payload-ref",
                       "RUN_AT",
                       "2000",
                       "NOW",
                       "1000",
                       "ITEMS",
                       "flow-1",
                       "1",
                       "-",
                       "flow-2",
                       "2",
                       "lease-2"
                     ],
                     {:flow_transition_many, "tenant-a", "queued", "running",
                      [
                        {:id, "flow-1", :fencing_token, 1, :lease_token, nil},
                        {:id, "flow-2", :fencing_token, 2, :lease_token, "lease-2"}
                      ], [payload_ref: "payload-ref", run_at_ms: 2000, now_ms: 1000]},
                     ["tenant-a"]},
                    {:command, "FLOW.RETRY",
                     ["flow-1", "lease-1", "FENCING", "1", "RUN_AT", "2000"],
                     {:flow_retry, "flow-1", "lease-1", [fencing_token: 1, run_at_ms: 2000]},
                     ["flow-1"]},
                    {:command, "FLOW.FAIL",
                     ["flow-1", "lease-1", "FENCING", "1", "ERROR", "err-1"],
                     {:flow_fail, "flow-1", "lease-1", [fencing_token: 1, error: "err-1"]},
                     ["flow-1"]},
                    {:command, "FLOW.CANCEL",
                     ["flow-1", "FENCING", "1", "REASON_REF", "reason-1"],
                     {:flow_cancel, "flow-1", [fencing_token: 1, reason_ref: "reason-1"]},
                     ["flow-1"]},
                    {:command, "FLOW.REWIND",
                     [
                       "flow-1",
                       "TO_EVENT",
                       "1000-1",
                       "RUN_AT",
                       "5000",
                       "EXPECT_STATE",
                       "completed",
                       "REASON_REF",
                       "manual"
                     ],
                     {:flow_rewind, "flow-1",
                      [
                        to_event: "1000-1",
                        run_at_ms: 5000,
                        expect_state: "completed",
                        reason_ref: "manual"
                      ]}, ["flow-1"]}
                  ], ""} =
                   Parser.parse_commands(
                     "flow.create flow-1 TYPE checkout STATE queued RUN_AT 1000 PRIORITY 2 PARTITION tenant-a RETENTION_TTL 5000 HISTORY_MAX_EVENTS 10 IDEMPOTENT true\r\n" <>
                       "flow.value.put shared-payload PARTITION tenant-a OWNER_FLOW_ID flow-1\r\n" <>
                       "flow.claim_due checkout WORKER worker-a LEASE_MS 30000 LIMIT 100 NOW 1000\r\n" <>
                       "flow.reclaim checkout WORKER worker-b LEASE_MS 30000 LIMIT 10 NOW 2000\r\n" <>
                       "flow.complete flow-1 lease-1 FENCING 1 RESULT result-1\r\n" <>
                       "flow.transition flow-1 queued running FENCING 1 LEASE_TOKEN lease-1\r\n" <>
                       "flow.transition_many tenant-a queued running PAYLOAD_REF payload-ref RUN_AT 2000 NOW 1000 ITEMS flow-1 1 - flow-2 2 lease-2\r\n" <>
                       "flow.retry flow-1 lease-1 FENCING 1 RUN_AT 2000\r\n" <>
                       "flow.fail flow-1 lease-1 FENCING 1 ERROR err-1\r\n" <>
                       "flow.cancel flow-1 FENCING 1 REASON_REF reason-1\r\n" <>
                       "flow.rewind flow-1 TO_EVENT 1000-1 RUN_AT 5000 EXPECT_STATE completed REASON_REF manual\r\n"
                   )
        end

        test "parses Flow read commands into typed Rust AST" do
          assert {:ok,
                  [
                    {:command, "FLOW.GET", ["flow-1", "PARTITION", "GLOBAL"],
                     {:flow_get, "flow-1", []}, ["GLOBAL"]},
                    {:command, "FLOW.GET", ["flow-1", "NOPAYLOAD"],
                     {:flow_get, "flow-1", [payload: false]}, ["flow-1"]},
                    {:command, "FLOW.GET", ["flow-1", "PAYLOAD", "MAXBYTES", "4096"],
                     {:flow_get, "flow-1", [payload: true, payload_max_bytes: 4096]}, ["flow-1"]},
                    {:command, "FLOW.LIST", ["checkout", "STATE", "queued", "COUNT", "25"],
                     {:flow_list, "checkout", [state: "queued", count: 25]}, ["checkout"]},
                    {:command, "FLOW.CLAIM_DUE",
                     [
                       "checkout",
                       "WORKER",
                       "worker-a",
                       "LIMIT",
                       "10",
                       "PAYLOAD",
                       "MAXBYTES",
                       "2048"
                     ],
                     {:flow_claim_due, "checkout",
                      [worker: "worker-a", limit: 10, payload: true, payload_max_bytes: 2048]},
                     ["checkout"]},
                    {:command, "FLOW.INFO", ["checkout", "PARTITION", "tenant-a"],
                     {:flow_info, "checkout", [partition_key: "tenant-a"]}, ["tenant-a"]},
                    {:command, "FLOW.STUCK", ["checkout", "OLDER_THAN", "1000", "COUNT", "10"],
                     {:flow_stuck, "checkout", [older_than_ms: 1000, count: 10]}, ["checkout"]},
                    {:command, "FLOW.HISTORY",
                     [
                       "flow-1",
                       "COUNT",
                       "10",
                       "FROM_MS",
                       "1000",
                       "TO_MS",
                       "2000",
                       "FROM_VERSION",
                       "2",
                       "TO_VERSION",
                       "4",
                       "EVENT",
                       "claimed",
                       "WORKER",
                       "worker-a",
                       "REV",
                       "true"
                     ],
                     {:flow_history, "flow-1",
                      [
                        count: 10,
                        from_ms: 1000,
                        to_ms: 2000,
                        from_version: 2,
                        to_version: 4,
                        event: "claimed",
                        worker: "worker-a",
                        rev: true
                      ]}, ["flow-1"]},
                    {:command, "FLOW.FAILURES", ["checkout", "FROM_MS", "1000", "TO_MS", "2000"],
                     {:flow_failures, "checkout", [from_ms: 1000, to_ms: 2000]}, ["checkout"]},
                    {:command, "FLOW.TERMINALS",
                     [
                       "checkout",
                       "STATE",
                       "any",
                       "FROM_MS",
                       "1000",
                       "TO_MS",
                       "2000",
                       "REV",
                       "true"
                     ],
                     {:flow_terminals, "checkout",
                      [state: "any", from_ms: 1000, to_ms: 2000, rev: true]}, ["checkout"]},
                    {:command, "FLOW.BY_PARENT",
                     [
                       "parent-1",
                       "FROM_MS",
                       "1000",
                       "TO_MS",
                       "2000",
                       "REV",
                       "true",
                       "STATE",
                       "failed",
                       "TERMINAL_ONLY",
                       "true"
                     ],
                     {:flow_by_parent, "parent-1",
                      [
                        from_ms: 1000,
                        to_ms: 2000,
                        rev: true,
                        state: "failed",
                        terminal_only: true
                      ]}, ["parent-1"]}
                  ], ""} =
                   Parser.parse_commands(
                     "flow.get flow-1 PARTITION GLOBAL\r\n" <>
                       "flow.get flow-1 NOPAYLOAD\r\n" <>
                       "flow.get flow-1 PAYLOAD MAXBYTES 4096\r\n" <>
                       "flow.list checkout STATE queued COUNT 25\r\n" <>
                       "flow.claim_due checkout WORKER worker-a LIMIT 10 PAYLOAD MAXBYTES 2048\r\n" <>
                       "flow.info checkout PARTITION tenant-a\r\n" <>
                       "flow.stuck checkout OLDER_THAN 1000 COUNT 10\r\n" <>
                       "flow.history flow-1 COUNT 10 FROM_MS 1000 TO_MS 2000 FROM_VERSION 2 TO_VERSION 4 EVENT claimed WORKER worker-a REV true\r\n" <>
                       "flow.failures checkout FROM_MS 1000 TO_MS 2000\r\n" <>
                       "flow.terminals checkout STATE any FROM_MS 1000 TO_MS 2000 REV true\r\n" <>
                       "flow.by_parent parent-1 FROM_MS 1000 TO_MS 2000 REV true STATE failed TERMINAL_ONLY true\r\n"
                   )
        end

        test "parses mixed-partition Flow many commands into typed Rust AST" do
          assert {:ok,
                  [
                    {:command, "FLOW.CREATE_MANY",
                     ["tenant-a", "ITEMS", "flow-min", "payload-min"],
                     {:flow_create_many, "tenant-a", [{:id, "flow-min", :payload, "payload-min"}],
                      []}, ["tenant-a"]},
                    {:command, "FLOW.COMPLETE_MANY",
                     ["tenant-a", "INDEPENDENT", "true", "ITEMS", "flow-1", "lease-1", "1"],
                     {:flow_complete_many, "tenant-a",
                      [{:id, "flow-1", :lease_token, "lease-1", :fencing_token, 1}],
                      [independent: true]}, ["tenant-a"]},
                    {:command, "FLOW.RETRY_MANY",
                     ["tenant-a", "INDEPENDENT", "true", "ITEMS", "flow-1", "lease-1", "1"],
                     {:flow_retry_many, "tenant-a",
                      [{:id, "flow-1", :lease_token, "lease-1", :fencing_token, 1}],
                      [independent: true]}, ["tenant-a"]},
                    {:command, "FLOW.FAIL_MANY",
                     ["tenant-a", "INDEPENDENT", "true", "ITEMS", "flow-1", "lease-1", "1"],
                     {:flow_fail_many, "tenant-a",
                      [{:id, "flow-1", :lease_token, "lease-1", :fencing_token, 1}],
                      [independent: true]}, ["tenant-a"]},
                    {:command, "FLOW.CANCEL_MANY",
                     ["tenant-a", "INDEPENDENT", "true", "ITEMS", "flow-1", "1"],
                     {:flow_cancel_many, "tenant-a", [{:id, "flow-1", :fencing_token, 1}],
                      [independent: true]}, ["tenant-a"]},
                    {:command, "FLOW.CREATE_MANY",
                     [
                       "MIXED",
                       "TYPE",
                       "iot",
                       "RUN_AT",
                       "1000",
                       "IDEMPOTENT",
                       "true",
                       "INDEPENDENT",
                       "true",
                       "ITEMS",
                       "flow-1",
                       "device-a",
                       "payload-1",
                       "flow-2",
                       "device-b",
                       "payload-2"
                     ],
                     {:flow_create_many, nil,
                      [
                        {"flow-1", [partition_key: "device-a", payload: "payload-1"]},
                        {"flow-2", [partition_key: "device-b", payload: "payload-2"]}
                      ], [type: "iot", run_at_ms: 1000, idempotent: true, independent: true]},
                     ["device-a", "device-b"]},
                    {:command, "FLOW.CREATE_MANY",
                     [
                       "AUTO",
                       "TYPE",
                       "iot",
                       "INDEPENDENT",
                       "true",
                       "RETURN",
                       "OK_ON_SUCCESS",
                       "ITEMS",
                       "flow-auto-1",
                       "payload-1",
                       "flow-auto-2",
                       "payload-2"
                     ],
                     {:flow_create_many, nil,
                      [
                        {:id, "flow-auto-1", :payload, "payload-1"},
                        {:id, "flow-auto-2", :payload, "payload-2"}
                      ], [type: "iot", independent: true, return: "OK_ON_SUCCESS"]}, ["AUTO"]},
                    {:command, "FLOW.TRANSITION_MANY",
                     [
                       "MIXED",
                       "queued",
                       "ready",
                       "RUN_AT",
                       "2000",
                       "INDEPENDENT",
                       "true",
                       "ITEMS",
                       "flow-1",
                       "device-a",
                       "1",
                       "-",
                       "flow-2",
                       "device-b",
                       "2",
                       "lease-2"
                     ],
                     {:flow_transition_many, nil, "queued", "ready",
                      [
                        {"flow-1", [partition_key: "device-a", fencing_token: 1]},
                        {"flow-2",
                         [partition_key: "device-b", fencing_token: 2, lease_token: "lease-2"]}
                      ], [run_at_ms: 2000, independent: true]}, ["device-a", "device-b"]}
                  ], ""} =
                   Parser.parse_commands(
                     "flow.create_many tenant-a ITEMS flow-min payload-min\r\n" <>
                       "flow.complete_many tenant-a INDEPENDENT true ITEMS flow-1 lease-1 1\r\n" <>
                       "flow.retry_many tenant-a INDEPENDENT true ITEMS flow-1 lease-1 1\r\n" <>
                       "flow.fail_many tenant-a INDEPENDENT true ITEMS flow-1 lease-1 1\r\n" <>
                       "flow.cancel_many tenant-a INDEPENDENT true ITEMS flow-1 1\r\n" <>
                       "flow.create_many MIXED TYPE iot RUN_AT 1000 IDEMPOTENT true INDEPENDENT true ITEMS flow-1 device-a payload-1 flow-2 device-b payload-2\r\n" <>
                 "flow.create_many AUTO TYPE iot INDEPENDENT true RETURN OK_ON_SUCCESS ITEMS flow-auto-1 payload-1 flow-auto-2 payload-2\r\n" <>
                       "flow.transition_many MIXED queued ready RUN_AT 2000 INDEPENDENT true ITEMS flow-1 device-a 1 - flow-2 device-b 2 lease-2\r\n"
                   )
        end

        test "parses mixed-partition Flow spawn_children into typed Rust AST" do
          assert {:ok,
                  [
                    {:command, "FLOW.SPAWN_CHILDREN",
                     [
                       "parent-1",
                       "GROUP",
                       "fanout",
                       "PARTITION",
                       "parent-p",
                       "FENCING",
                       "1",
                       "ITEMS",
                       "MIXED",
                       "child-a",
                       "device-a",
                       "child",
                       "payload-a",
                       "child-b",
                       "device-b",
                       "child",
                       "payload-b"
                     ],
                     {:flow_spawn_children, "parent-1",
                      [
                        {"child-a",
                         [partition_key: "device-a", type: "child", payload: "payload-a"]},
                        {"child-b",
                         [partition_key: "device-b", type: "child", payload: "payload-b"]}
                      ], [group_id: "fanout", partition_key: "parent-p", fencing_token: 1]},
                     ["parent-p", "device-a", "device-b"]}
                  ], ""} =
                   Parser.parse_commands(
                     "flow.spawn_children parent-1 GROUP fanout PARTITION parent-p FENCING 1 ITEMS MIXED child-a device-a child payload-a child-b device-b child payload-b\r\n"
                   )
        end

        test "keeps Flow option parse errors inside AST" do
          huge_ref = String.duplicate("p", 4_097)

          assert {:ok,
                  [
                    {:command, "FLOW.CREATE", ["f", "TYPE", "t", "PRIORITY", "x"],
                     {:flow_create, "f", {:error, "ERR value is not an integer or out of range"}},
                     ["f"]},
                    {:command, "FLOW.CREATE", ["f", "TYPE", "t", "PAYLOAD_REF", ^huge_ref],
                     {:flow_create, "f",
                      {:error, "ERR flow payload_ref too large (max 4096 bytes)"}}, ["f"]},
                    {:command, "FLOW.CLAIM_DUE", ["t", "WORKER", "w", "LIMIT", "0"],
                     {:flow_claim_due, "t",
                      {:error, "ERR flow limit must be a positive integer"}}, ["t"]},
                    {:command, "FLOW.COMPLETE", ["f", "l"],
                     {:flow_complete,
                      {:error, "ERR wrong number of arguments for 'flow.complete' command"}},
                     ["f"]},
                    {:command, "FLOW.REWIND", ["f", "RUN_AT", "1"],
                     {:flow_rewind, "f", {:error, "ERR flow to_event is required"}}, ["f"]}
                  ], ""} =
                   Parser.parse_commands(
                     "flow.create f TYPE t PRIORITY x\r\n" <>
                       "flow.create f TYPE t PAYLOAD_REF #{huge_ref}\r\n" <>
                       "flow.claim_due t WORKER w LIMIT 0\r\n" <>
                       "flow.complete f l\r\n" <>
                       "flow.rewind f RUN_AT 1\r\n"
                   )
        end
      end

      describe "cluster command AST parsing" do
        test "removed CLUSTER.ENABLE is parsed as unknown" do
          assert {:ok,
                  [
                    {:command, "CLUSTER.ENABLE", [], {:unknown, "CLUSTER.ENABLE", []}, []},
                    {:command, "CLUSTER.ENABLE", ["dryrun"],
                     {:unknown, "CLUSTER.ENABLE", ["dryrun"]}, []}
                  ], ""} =
                   Parser.parse_commands("cluster.enable\r\ncluster.enable dryrun\r\n")
        end
      end

      describe "boolean edge cases" do
        test "invalid boolean value returns error" do
          assert {:error, {:invalid_boolean, "1"}} = Parser.parse("#1\r\n")
        end

        test "empty boolean returns error" do
          assert {:error, {:invalid_boolean, ""}} = Parser.parse("#\r\n")
        end
      end
    end
  end
end
