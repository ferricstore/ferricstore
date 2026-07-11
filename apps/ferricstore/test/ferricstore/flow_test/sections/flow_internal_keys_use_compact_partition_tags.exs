defmodule Ferricstore.FlowTest.Sections.FlowInternalKeysUseCompactPartitionTags do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

      test "flow internal keys use compact partition tags" do
        partition_key = "tenant:device:" <> String.duplicate("abcdef0123456789", 4)

        state_key = Ferricstore.Flow.Keys.state_key("flow-a", partition_key)
        history_key = Ferricstore.Flow.Keys.history_key("flow-a", partition_key)
        due_key = Ferricstore.Flow.Keys.due_key("checkout", "queued", 0, partition_key)

        assert state_key =~ ~r/^f:\{f:[A-Za-z0-9_-]{43}\}:s:flow-a$/
        assert history_key =~ ~r/^f:\{f:[A-Za-z0-9_-]{43}\}:h:flow-a$/
        assert due_key =~ ~r/^f:\{f:[A-Za-z0-9_-]{43}\}:d:checkout:queued:p0$/
        assert Ferricstore.Flow.Keys.state_key?(state_key)
        refute Ferricstore.Flow.Keys.state_key?(history_key)

        assert Ferricstore.Flow.Keys.stream_entry_key_from_history_key(history_key, "1-1") ==
                 Ferricstore.Flow.Keys.stream_entry_key("flow-a", "1-1", partition_key)

        old_state_key = "flow:{flow:" <> String.duplicate("0", 64) <> "}:state:flow-a"
        assert byte_size(state_key) < byte_size(old_state_key)
      end

      test "flow state record encoding is compact" do
        record = %{
          id: "flow-1",
          type: "checkout",
          state: "queued",
          version: 3,
          attempts: 2,
          fencing_token: 9,
          created_at_ms: 1_000,
          updated_at_ms: 1_100,
          next_run_at_ms: 1_200,
          priority: 1,
          ttl_ms: nil,
          retention_ttl_ms: 60_000,
          terminal_retention_until_ms: nil,
          history_hot_max_events: 100,
          history_max_events: 100_000,
          partition_key: "tenant-a",
          payload_ref: "payload:1",
          parent_flow_id: "parent-1",
          parent_partition_key: nil,
          root_flow_id: "root-1",
          correlation_id: "order-1",
          result_ref: "result:1",
          error_ref: nil,
          lease_owner: "worker-1",
          lease_token: "lease-1",
          lease_deadline_ms: 2_000,
          run_state: "charge_card",
          rewound_to_event_id: "1000-1",
          child_groups: %{}
        }

        compact = Ferricstore.Flow.encode_record(record)

        normal_record = Map.delete(record, :rewound_to_event_id)
        term_record = :erlang.term_to_binary(record)

        assert "FSF5" <> _ = compact
        assert Ferricstore.Flow.decode_record(compact) == record

        assert Ferricstore.Flow.decode_record(Ferricstore.Flow.encode_record(normal_record)) ==
                 normal_record

        assert Ferricstore.Flow.encode_record(Map.delete(record, :child_groups)) == compact

        assert byte_size(compact) < byte_size(term_record)

        large_record = %{
          record
          | created_at_ms: 1_777_777_777_777,
            updated_at_ms: 1_777_777_777_888,
            next_run_at_ms: 1_777_777_777_999,
            lease_deadline_ms: 1_777_777_778_111
        }

        assert Ferricstore.Flow.decode_record(Ferricstore.Flow.encode_record(large_record)) ==
                 large_record

        assert_raise ArgumentError, fn ->
          Ferricstore.Flow.decode_record(
            "FSF9" <> binary_part(compact, 4, byte_size(compact) - 4)
          )
        end

        assert_raise ArgumentError, fn ->
          Ferricstore.Flow.decode_record(binary_part(compact, 0, byte_size(compact) - 1))
        end
      end

      test "flow state record encoding preserves zeroes nils and empty binaries" do
        record = %{
          id: "flow-zero",
          type: "checkout",
          state: "queued",
          version: 0,
          attempts: 0,
          fencing_token: 0,
          created_at_ms: 0,
          updated_at_ms: 0,
          next_run_at_ms: 0,
          priority: 0,
          ttl_ms: 0,
          retention_ttl_ms: 0,
          terminal_retention_until_ms: 0,
          history_hot_max_events: 0,
          history_max_events: 0,
          partition_key: "",
          payload_ref: nil,
          parent_flow_id: "",
          parent_partition_key: nil,
          root_flow_id: "",
          correlation_id: "",
          result_ref: nil,
          error_ref: "",
          lease_owner: "",
          lease_token: nil,
          lease_deadline_ms: 0,
          run_state: "",
          rewound_to_event_id: nil,
          child_groups: %{}
        }

        assert Ferricstore.Flow.decode_record(Ferricstore.Flow.encode_record(record)) ==
                 Map.delete(record, :rewound_to_event_id)
      end

      test "flow history encoding is compact and includes metadata" do
        record = %{
          id: "flow-1",
          type: "checkout",
          state: "queued",
          version: 2,
          attempts: 1,
          fencing_token: 4,
          created_at_ms: 1_000,
          updated_at_ms: 1_100,
          next_run_at_ms: 1_200,
          priority: 1,
          lease_deadline_ms: 2_000,
          lease_owner: "worker-1",
          payload_ref: "payload:1",
          parent_flow_id: "parent-1",
          root_flow_id: "root-1",
          correlation_id: "order-1",
          result_ref: nil,
          error_ref: "error:1",
          rewound_to_event_id: nil
        }

        fields = [
          "event",
          "retry",
          "version",
          "2",
          "at",
          "1100",
          "id",
          "flow-1",
          "type",
          "checkout",
          "state",
          "queued",
          "priority",
          "1",
          "attempts",
          "1",
          "fencing_token",
          "4",
          "created_at_ms",
          "1000",
          "updated_at_ms",
          "1100",
          "next_run_at_ms",
          "1200",
          "lease_deadline_ms",
          "2000",
          "lease_owner",
          "worker-1",
          "payload_ref",
          "payload:1",
          "parent_flow_id",
          "parent-1",
          "root_flow_id",
          "root-1",
          "correlation_id",
          "order-1",
          "result_ref",
          "",
          "error_ref",
          "error:1",
          "rewound_to_event_id",
          ""
        ]

        compact =
          Ferricstore.Flow.encode_history_fields(record, "retry", 1_100, %{
            "retry_decision" => "scheduled",
            "retry_next_run_at_ms" => nil
          })

        assert compact ==
                 record
                 |> Ferricstore.Flow.history_snapshot("retry", 1_100, %{
                   "retry_decision" => "scheduled",
                   "retry_next_run_at_ms" => nil
                 })
                 |> Ferricstore.Flow.encode_history_snapshot()

        assert "FSH2" <> _ = compact

        assert Ferricstore.Flow.decode_history_fields(compact, record) ==
                 fields ++
                   [
                     "retry_decision",
                     "scheduled",
                     "retry_next_run_at_ms",
                     ""
                   ]

        assert Ferricstore.Flow.decode_history_fields(
                 "BAD!" <> binary_part(compact, 4, byte_size(compact) - 4)
               ) == []

        assert Ferricstore.Flow.decode_history_fields(:erlang.term_to_binary(fields)) == []

        assert Ferricstore.Flow.decode_history_fields(
                 binary_part(compact, 0, byte_size(compact) - 1)
               ) ==
                 []
      end

      test "flow_create stores state and prevents duplicate ids" do
        id = uid("flow-create")

        assert {:ok, flow} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "queued",
                   payload: "payload:" <> id,
                   run_at_ms: 1_000
                 )

        assert flow.id == id
        assert flow.type == "checkout"
        assert flow.state == "queued"
        assert flow.version == 1
        assert flow.fencing_token == 0
        assert is_binary(flow.payload_ref)
        assert flow.payload_ref != "payload:" <> id

        assert {:ok, fetched} = FerricStore.flow_get(id)
        assert fetched.id == id
        assert fetched.state == "queued"

        assert {:error, "ERR flow already exists"} =
                 flow_create_and_get(id, type: "checkout", state: "queued")
      end

      test "flow_create and flow_transition accept payload_ref for shared payloads" do
        id = uid("flow-shared-payload-ref")
        initial_ref = "shared:payload:initial"
        transition_ref = "shared:payload:transition"

        assert {:ok, flow} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "queued",
                   payload_ref: initial_ref,
                   run_at_ms: 1_000,
                   now_ms: 1_000
                 )

        assert flow.payload_ref == initial_ref

        assert {:ok, transitioned} =
                 flow_transition_and_get(id, "queued", "ready",
                   fencing_token: 0,
                   payload_ref: transition_ref,
                   run_at_ms: 2_000,
                   now_ms: 1_100
                 )

        assert transitioned.state == "ready"
        assert transitioned.payload_ref == transition_ref
      end

      test "flow_transition without value options preserves existing named value refs" do
        id = uid("flow-preserve-named-values")

        assert {:ok, flow} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "queued",
                   values: %{"doc" => "v1"},
                   run_at_ms: 1_000,
                   now_ms: 1_000
                 )

        assert is_map(flow.value_refs)
        assert %{} = flow.value_refs["doc"]

        assert {:ok, transitioned} =
                 flow_transition_and_get(id, "queued", "ready",
                   fencing_token: 0,
                   run_at_ms: 2_000,
                   now_ms: 1_100
                 )

        assert transitioned.state == "ready"
        assert transitioned.value_refs == flow.value_refs

        assert {:ok, hydrated} = FerricStore.flow_get(id, values: true)
        assert hydrated.values == %{"doc" => "v1"}
      end

      test "flow_get hydrates payload refs only when full or payload is requested" do
        id = uid("flow-get-payload")
        payload = "payload-body"

        assert {:ok, _flow} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "queued",
                   payload: payload,
                   run_at_ms: 1_000
                 )

        assert {:ok, ref_only} = FerricStore.flow_get(id)
        refute Map.has_key?(ref_only, :payload)
        refute Map.has_key?(ref_only, :payload_omitted)

        assert {:ok, fetched} = FerricStore.flow_get(id, full: true)
        assert fetched.payload == payload
        assert fetched.payload_size == encoded_value_size(payload)
        refute Map.has_key?(fetched, :payload_omitted)

        assert {:ok, no_payload} = FerricStore.flow_get(id, payload: false)
        refute Map.has_key?(no_payload, :payload)
        refute Map.has_key?(no_payload, :payload_omitted)

        assert {:ok, capped} = FerricStore.flow_get(id, full: true, payload_max_bytes: 4)
        assert capped.payload_omitted == true
        assert capped.payload_size == encoded_value_size(payload)
        refute Map.has_key?(capped, :payload)
      end

      test "flow_claim_due returns payload inline up to cap without rolling back missing payloads" do
        type = uid("claim-payload")
        id = uid("claim-payload-flow")
        payload = "worker-input"

        assert {:ok, _flow} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   payload: payload,
                   run_at_ms: 1_000
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-a",
                   limit: 1,
                   now_ms: 1_000,
                   payload_max_bytes: 64
                 )

        assert claimed.id == id
        assert claimed.payload == payload
        assert claimed.payload_size == encoded_value_size(payload)

        missing_id = uid("claim-missing-payload-flow")

        assert {:ok, missing_flow} =
                 flow_create_and_get(missing_id,
                   type: type,
                   state: "queued",
                   payload: "missing-worker-input",
                   run_at_ms: 2_000
                 )

        assert {:ok, 1} = internal_del(missing_flow.payload_ref)

        assert {:ok, [missing]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-b",
                   limit: 1,
                   now_ms: 2_000
                 )

        assert missing.id == missing_id
        assert missing.payload == nil
        assert missing.payload_missing == true
        assert {:ok, %{state: "running"}} = FerricStore.flow_get(missing_id)
      end

      test "pipeline_read_batch preserves order across get, missing, and history reads" do
        ctx = FerricStore.Instance.get(:default)
        {partition_a, partition_b} = different_partition_keys()
        id_a = uid("flow-pipeline-read-a")
        id_b = uid("flow-pipeline-read-b")

        attach_flow_telemetry([[:ferricstore, :flow, :pipeline_read_batch]])

        assert {:ok, _} =
                 flow_create_and_get(id_a,
                   type: "pipeline-read",
                   state: "queued",
                   partition_key: partition_a,
                   now_ms: 1,
                   run_at_ms: 1
                 )

        assert {:ok, _} =
                 flow_create_and_get(id_b,
                   type: "pipeline-read",
                   state: "queued",
                   partition_key: partition_b,
                   now_ms: 2,
                   run_at_ms: 2
                 )

        assert [
                 {:ok, %{id: ^id_a, partition_key: ^partition_a}},
                 {:ok, nil},
                 {:ok, %{id: ^id_b, partition_key: ^partition_b}},
                 {:ok, [{_event_id, %{"event" => "created", "state" => "queued"}}]}
               ] =
                 Ferricstore.Flow.pipeline_read_batch(ctx, [
                   {:get, id_a, [partition_key: partition_a]},
                   {:get, "missing-pipeline-read", [partition_key: partition_a]},
                   {:get, id_b, [partition_key: partition_b]},
                   {:history, id_a, [partition_key: partition_a, count: 10]}
                 ])

        assert_receive {:flow_telemetry, [:ferricstore, :flow, :pipeline_read_batch],
                        %{count: 4, gets: 3, histories: 1}, %{source: :pipeline}}
      end

      test "pipeline_read_batch accepts Rust Flow AST reads directly" do
        ctx = FerricStore.Instance.get(:default)
        partition_key = uid("pipeline-read-rust-ast")
        id = uid("pipeline-read-rust-ast")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: "pipeline-read-rust-ast",
                   state: "queued",
                   partition_key: partition_key,
                   now_ms: 1,
                   run_at_ms: 1
                 )

        assert [
                 {:ok, %{id: ^id, partition_key: ^partition_key}},
                 {:ok, [{_event_id, %{"event" => "created", "state" => "queued"}}]}
               ] =
                 Ferricstore.Flow.pipeline_read_batch(ctx, [
                   {:flow_get, id, [partition_key: partition_key]},
                   {:flow_history, id, [partition_key: partition_key, count: 10]}
                 ])
      end

      test "pipeline_read_batch accepts Flow terminal query AST reads directly" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("pipeline-read-terminal-rust-ast")

        assert [
                 {:ok, []},
                 {:ok, []}
               ] =
                 Ferricstore.Flow.pipeline_read_batch(ctx, [
                   {:flow_terminals, type, [count: 10]},
                   {:flow_failures, type, [count: 10]}
                 ])
      end

      test "pipeline_write_batch_independent works in raft mode" do
        ctx = FerricStore.Instance.get(:default)
        partition_key = uid("flow-pipeline-write-standalone-partition")
        id_a = uid("flow-pipeline-write-standalone-a")
        id_b = uid("flow-pipeline-write-standalone-b")
        attach_flow_telemetry([[:ferricstore, :flow, :pipeline_write, :stop]])

        assert [:ok, :ok] =
                 Ferricstore.Flow.pipeline_write_batch_independent(ctx, [
                   {:create, id_a,
                    [
                      type: "pipeline-write-standalone",
                      state: "queued",
                      run_at_ms: 1,
                      now_ms: 1,
                      partition_key: partition_key
                    ]},
                   {:create, id_b,
                    [
                      type: "pipeline-write-standalone",
                      state: "queued",
                      run_at_ms: 1,
                      now_ms: 1,
                      partition_key: partition_key
                    ]}
                 ])

        assert {:ok, %{id: ^id_a}} = FerricStore.flow_get(id_a, partition_key: partition_key)
        assert {:ok, %{id: ^id_b}} = FerricStore.flow_get(id_b, partition_key: partition_key)

        assert_receive {:flow_telemetry, [:ferricstore, :flow, :pipeline_write, :stop],
                        %{count: 2}, %{result: :ok, reason: nil}}
      end

      test "pipeline_write_batch_independent accepts Rust Flow AST writes directly" do
        ctx = FerricStore.Instance.get(:default)
        partition_key = uid("flow-pipeline-rust-ast-partition")
        id = uid("flow-pipeline-rust-ast")

        assert [:ok, :ok] =
                 Ferricstore.Flow.pipeline_write_batch_independent(ctx, [
                   {:flow_create, id,
                    [
                      type: "pipeline-rust-ast",
                      state: "queued",
                      run_at_ms: 1,
                      now_ms: 1,
                      partition_key: partition_key
                    ]},
                   {:flow_transition, id, "queued", "ready",
                    [
                      fencing_token: 0,
                      now_ms: 2,
                      partition_key: partition_key
                    ]}
                 ])

        assert {:ok, %{state: "ready"}} = FerricStore.flow_get(id, partition_key: partition_key)
      end

      test "pipeline_write_batch_independent terminal commands preserve per-command success" do
        ctx = FerricStore.Instance.get(:default)
        partition_key = uid("flow-pipeline-terminal-partition")
        type = uid("flow-pipeline-terminal")
        now_ms = 1_000
        ids = Enum.map(1..3, &uid("flow-pipeline-terminal-#{&1}"))

        Enum.each(ids, fn id ->
          assert {:ok, _} =
                   flow_create_and_get(id,
                     type: type,
                     state: "queued",
                     partition_key: partition_key,
                     now_ms: now_ms,
                     run_at_ms: now_ms
                   )
        end)

        assert {:ok, claims} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition_key,
                   worker: "pipeline-terminal",
                   limit: 3,
                   now_ms: now_ms,
                   lease_ms: 30_000
                 )

        claims_by_id = Map.new(claims, fn claim -> {claim.id, claim} end)
        [id_a, id_b, id_c] = ids
        claim_a = Map.fetch!(claims_by_id, id_a)
        claim_b = Map.fetch!(claims_by_id, id_b)
        claim_c = Map.fetch!(claims_by_id, id_c)

        assert [:ok, {:error, _reason}, :ok] =
                 Ferricstore.Flow.pipeline_write_batch_independent(ctx, [
                   {:complete, id_a, claim_a.lease_token,
                    [
                      partition_key: partition_key,
                      fencing_token: claim_a.fencing_token,
                      now_ms: now_ms + 1
                    ]},
                   {:complete, id_b, "wrong-token",
                    [
                      partition_key: partition_key,
                      fencing_token: claim_b.fencing_token,
                      now_ms: now_ms + 1
                    ]},
                   {:complete, id_c, claim_c.lease_token,
                    [
                      partition_key: partition_key,
                      fencing_token: claim_c.fencing_token,
                      now_ms: now_ms + 1
                    ]}
                 ])

        assert {:ok, %{state: "completed"}} =
                 FerricStore.flow_get(id_a, partition_key: partition_key)

        assert {:ok, %{state: "running"}} =
                 FerricStore.flow_get(id_b, partition_key: partition_key)

        assert {:ok, %{state: "completed"}} =
                 FerricStore.flow_get(id_c, partition_key: partition_key)
      end

      test "pipeline_write_batch_independent terminal commands skip cross-terminal pre-read" do
        ctx = FerricStore.Instance.get(:default)
        partition_key = uid("flow-pipeline-terminal-fast-partition")
        type = uid("flow-pipeline-terminal-fast")
        now_ms = 1_000
        id = uid("flow-pipeline-terminal-fast")
        owner = self()

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   now_ms: now_ms,
                   run_at_ms: now_ms
                 )

        assert {:ok, [claim]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition_key,
                   worker: "pipeline-terminal-fast",
                   limit: 1,
                   now_ms: now_ms,
                   lease_ms: 30_000
                 )

        Process.put(:ferricstore_flow_terminal_many_values_hook, fn keys ->
          send(owner, {:terminal_pre_read, keys})
        end)

        try do
          assert [:ok] =
                   Ferricstore.Flow.pipeline_write_batch_independent(ctx, [
                     {:complete, id, claim.lease_token,
                      [
                        partition_key: partition_key,
                        fencing_token: claim.fencing_token,
                        now_ms: now_ms + 1
                      ]}
                   ])

          refute_received {:terminal_pre_read, _keys}
        after
          Process.delete(:ferricstore_flow_terminal_many_values_hook)
        end
      end

      test "pipeline_write_batch_independent terminal commands preserve duplicate flow order" do
        ctx = FerricStore.Instance.get(:default)
        partition_key = uid("flow-pipeline-terminal-dup-partition")
        type = uid("flow-pipeline-terminal-dup")
        now_ms = 1_000
        id = uid("flow-pipeline-terminal-dup")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   now_ms: now_ms,
                   run_at_ms: now_ms
                 )

        assert {:ok, [claim]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition_key,
                   worker: "pipeline-terminal-dup",
                   limit: 1,
                   now_ms: now_ms,
                   lease_ms: 30_000
                 )

        command =
          {:complete, id, claim.lease_token,
           [
             partition_key: partition_key,
             fencing_token: claim.fencing_token,
             now_ms: now_ms + 1
           ]}

        assert [:ok, :ok] =
                 Ferricstore.Flow.pipeline_write_batch_independent(ctx, [command, command])

        assert {:ok, %{state: "completed"}} =
                 FerricStore.flow_get(id, partition_key: partition_key)

        assert {:ok, history} = FerricStore.flow_history(id, partition_key: partition_key)

        assert Enum.count(history, fn {_event_id, fields} ->
                 Map.get(fields, "event") == "completed" or Map.get(fields, :event) == "completed"
               end) == 1
      end

      test "pipeline_write_batch_independent transitions preserve duplicate flow order" do
        ctx = FerricStore.Instance.get(:default)
        partition_key = uid("flow-pipeline-transition-dup-partition")
        type = uid("flow-pipeline-transition-dup")
        now_ms = 1_000
        id = uid("flow-pipeline-transition-dup")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   now_ms: now_ms,
                   run_at_ms: now_ms
                 )

        assert [:ok, :ok] =
                 Ferricstore.Flow.pipeline_write_batch_independent(ctx, [
                   {:transition, id, "queued", "ready",
                    [
                      partition_key: partition_key,
                      fencing_token: 0,
                      now_ms: now_ms + 1
                    ]},
                   {:transition, id, "ready", "processing",
                    [
                      partition_key: partition_key,
                      fencing_token: 0,
                      now_ms: now_ms + 2
                    ]}
                 ])

        assert {:ok, %{state: "processing", version: 3}} =
                 FerricStore.flow_get(id, partition_key: partition_key)
      end

      test "pipeline_claim_due_batch coalesces compatible claims and preserves worker boundaries" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("pipeline-claim")
        partition_key = uid("tenant")
        ids = Enum.map(1..3, &uid("pipeline-claim-#{&1}"))

        Enum.each(ids, fn id ->
          assert {:ok, %{id: ^id}} =
                   flow_create_and_get(id,
                     type: type,
                     partition_key: partition_key,
                     now_ms: 1,
                     run_at_ms: 1
                   )
        end)

        attach_flow_telemetry([[:ferricstore, :flow, :pipeline_claim_due_batch]])

        assert [
                 {:ok, [%{lease_owner: "worker-a"} = first]},
                 {:ok, [%{lease_owner: "worker-a"} = second]},
                 {:ok, [%{lease_owner: "worker-b"} = third]}
               ] =
                 Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
                   {:claim_due, type,
                    [worker: "worker-a", partition_key: partition_key, limit: 1, now_ms: 2]},
                   {:claim_due, type,
                    [worker: "worker-a", partition_key: partition_key, limit: 1, now_ms: 2]},
                   {:claim_due, type,
                    [worker: "worker-b", partition_key: partition_key, limit: 1, now_ms: 2]}
                 ])

        assert MapSet.new([first.id, second.id, third.id]) == MapSet.new(ids)

        assert_receive {:flow_telemetry, [:ferricstore, :flow, :pipeline_claim_due_batch],
                        %{commands: 3, groups: 2, coalesced_calls: 1}, %{source: :resp_pipeline}}
      end

      test "pipeline_claim_due_batch coalesces job-only claim responses" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("pipeline-claim-jobs")
        partition_key = uid("tenant")
        ids = Enum.map(1..2, &uid("pipeline-claim-jobs-#{&1}"))

        Enum.each(ids, fn id ->
          assert {:ok, %{id: ^id}} =
                   flow_create_and_get(id,
                     type: type,
                     partition_key: partition_key,
                     now_ms: 1,
                     run_at_ms: 1
                   )
        end)

        attach_flow_telemetry([[:ferricstore, :flow, :pipeline_claim_due_batch]])

        assert [
                 {:ok,
                  [%{id: first_id, lease_token: first_lease, fencing_token: first_fence} = first]},
                 {:ok,
                  [
                    %{id: second_id, lease_token: second_lease, fencing_token: second_fence} =
                      second
                  ]}
               ] =
                 Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
                   {:claim_due, type,
                    [
                      worker: "worker-a",
                      partition_key: partition_key,
                      limit: 1,
                      now_ms: 2,
                      return: :jobs
                    ]},
                   {:claim_due, type,
                    [
                      worker: "worker-a",
                      partition_key: partition_key,
                      limit: 1,
                      now_ms: 2,
                      return: :jobs
                    ]}
                 ])

        assert MapSet.new([first_id, second_id]) == MapSet.new(ids)
        assert is_binary(first_lease)
        assert is_binary(second_lease)
        assert is_integer(first_fence)
        assert is_integer(second_fence)
        refute Map.has_key?(first, :version)
        refute Map.has_key?(second, :version)

        assert_receive {:flow_telemetry, [:ferricstore, :flow, :pipeline_claim_due_batch],
                        %{commands: 2, groups: 1, coalesced_calls: 1}, %{source: :resp_pipeline}}
      end

      test "claim_due supports compact job responses" do
        type = uid("claim-jobs-compact")
        partition_key = uid("tenant")
        id = uid("claim-jobs-compact")

        assert {:ok, %{id: ^id}} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition_key,
                   now_ms: 1,
                   run_at_ms: 1
                 )

        assert {:ok, [[^id, ^partition_key, lease_token, fencing_token]]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-compact",
                   partition_key: partition_key,
                   limit: 1,
                   now_ms: 2,
                   return: :jobs_compact
                 )

        assert is_binary(lease_token)
        assert is_integer(fencing_token)
      end

      test "claim_due compact state response includes the claimed run state" do
        type = uid("claim-jobs-compact-state")
        partition_key = uid("tenant")
        id = uid("claim-jobs-compact-state")

        assert {:ok, %{id: ^id}} =
                 flow_create_and_get(id,
                   type: type,
                   state: "ready",
                   partition_key: partition_key,
                   now_ms: 1,
                   run_at_ms: 1
                 )

        assert {:ok, [[^id, ^partition_key, lease_token, fencing_token, "ready"]]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-compact-state",
                   partition_key: partition_key,
                   limit: 1,
                   now_ms: 2,
                   return: :jobs_compact_state
                 )

        assert is_binary(lease_token)
        assert is_integer(fencing_token)
      end

      test "claim_due multi-state precheck does not miss work after an empty route state" do
        type = uid("claim-multi-state-precheck")
        partition_key = uid("tenant")
        id = uid("claim-multi-state-precheck")

        assert {:ok, %{id: ^id}} =
                 flow_create_and_get(id,
                   type: type,
                   state: "ready",
                   partition_key: partition_key,
                   now_ms: 1,
                   run_at_ms: 1
                 )

        assert {:ok, [[^id, ^partition_key, lease_token, fencing_token, "ready"]]} =
                 FerricStore.flow_claim_due(type,
                   states: ["queued", "ready"],
                   worker: "worker-multi-state-precheck",
                   partition_key: partition_key,
                   limit: 1,
                   now_ms: 2,
                   return: :jobs_compact_state
                 )

        assert is_binary(lease_token)
        assert is_integer(fencing_token)
      end
    end
  end
end
