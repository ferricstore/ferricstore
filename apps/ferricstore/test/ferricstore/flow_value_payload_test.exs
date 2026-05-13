defmodule Ferricstore.FlowValuePayloadTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "shared payload refs can be reused by create, create_many, transition, and transition_many" do
    assert {:ok, %{ref: shared_ref}} =
             FerricStore.flow_value_put(%{message: "same payload"}, partition_key: "tenant-a")

    assert {:ok, created} =
             FerricStore.flow_create(unique_id("flow-value-shared-create"),
               type: "value-shared",
               partition_key: "tenant-a",
               payload_ref: shared_ref,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert created.payload_ref == shared_ref

    assert {:ok, fetched} =
             FerricStore.flow_get(created.id, partition_key: "tenant-a", full: true)

    assert fetched.payload == %{message: "same payload"}

    id_a = unique_id("flow-value-shared-many-a")
    id_b = unique_id("flow-value-shared-many-b")

    assert {:ok, many} =
             FerricStore.flow_create_many(
               nil,
               [
                 {id_a, partition_key: "tenant-a", payload_ref: shared_ref},
                 {id_b, partition_key: "tenant-b", payload_ref: shared_ref}
               ],
               type: "value-shared-many",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert Enum.map(many, & &1.payload_ref) == [shared_ref, shared_ref]

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-shared",
               partition_key: "tenant-a",
               worker: "worker-1",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, transitioned} =
             FerricStore.flow_transition(created.id, "running", "waiting",
               partition_key: "tenant-a",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               payload_ref: shared_ref,
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert transitioned.payload_ref == shared_ref

    id_many_a = unique_id("flow-value-shared-transition-many-a")
    id_many_b = unique_id("flow-value-shared-transition-many-b")

    assert {:ok, _created_many} =
             FerricStore.flow_create_many(
               "tenant-a",
               [
                 {id_many_a, [payload_ref: shared_ref]},
                 {id_many_b, [payload_ref: shared_ref]}
               ],
               type: "value-shared-transition-many",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claim_many_a, claim_many_b]} =
             FerricStore.flow_claim_due("value-shared-transition-many",
               partition_key: "tenant-a",
               worker: "worker-many",
               limit: 2,
               now_ms: 1_000
             )

    assert {:ok, transitioned_many} =
             FerricStore.flow_transition_many(
               "tenant-a",
               "running",
               "waiting",
               [
                 {claim_many_a.id,
                  [
                    fencing_token: claim_many_a.fencing_token,
                    lease_token: claim_many_a.lease_token,
                    payload_ref: shared_ref
                  ]},
                 %{
                   id: claim_many_b.id,
                   fencing_token: claim_many_b.fencing_token,
                   lease_token: claim_many_b.lease_token,
                   payload_ref: shared_ref
                 }
               ],
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert Enum.map(transitioned_many, & &1.payload_ref) == [shared_ref, shared_ref]
  end

  test "payload refs can point at user-managed string keys" do
    external_ref = unique_id("flow-value-external-ref")
    assert :ok = FerricStore.set(external_ref, Ferricstore.Flow.encode_value(%{external: true}))

    id = unique_id("flow-value-external-create")

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "value-external",
               partition_key: "tenant-external",
               payload_ref: external_ref,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert created.payload_ref == external_ref

    assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: "tenant-external", full: true)
    assert fetched.payload == %{external: true}

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-external",
               partition_key: "tenant-external",
               worker: "worker-external",
               limit: 1,
               now_ms: 1_000
             )

    next_ref = unique_id("flow-value-external-transition-many-ref")
    assert :ok = FerricStore.set(next_ref, "plain-user-managed-value")

    assert {:ok, [transitioned]} =
             FerricStore.flow_transition_many(
               "tenant-external",
               "running",
               "waiting",
               [
                 %{
                   id: claimed.id,
                   fencing_token: claimed.fencing_token,
                   lease_token: claimed.lease_token,
                   payload_ref: next_ref
                 }
               ],
               now_ms: 1_100
             )

    assert transitioned.payload_ref == next_ref

    assert {:ok, fetched_after} =
             FerricStore.flow_get(id, partition_key: "tenant-external", full: true)

    assert fetched_after.payload == "plain-user-managed-value"
  end

  test "payload refs can point at user-managed compound keys" do
    compound_ref = unique_id("flow-value-external-hash-ref")
    assert :ok = FerricStore.hset(compound_ref, %{"field" => "value"})

    id = unique_id("flow-value-external-hash-create")

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "value-external-compound",
               partition_key: "tenant-external",
               payload_ref: compound_ref,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert created.payload_ref == compound_ref

    assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: "tenant-external")
    assert fetched.payload_ref == compound_ref

    assert {:ok, %{"field" => "value"}} = FerricStore.hgetall(compound_ref)
  end

  test "payload refs must point at an existing key" do
    assert {:error, "ERR flow payload_ref does not exist"} =
             FerricStore.flow_create(unique_id("flow-value-invalid-ref"),
               type: "value-shared",
               payload_ref: "payload:external"
             )

    assert {:error, "ERR flow payload_ref must be a non-empty string"} =
             FerricStore.flow_create(unique_id("flow-value-empty-ref"),
               type: "value-shared",
               payload_ref: ""
             )

    assert {:error, "ERR flow payload_ref must be a non-empty string"} =
             FerricStore.flow_create(unique_id("flow-value-non-binary-ref"),
               type: "value-shared",
               payload_ref: 123
             )

    missing_ref = Ferricstore.Flow.Keys.shared_value_key("missing", "tenant-a")

    assert {:error, "ERR flow payload_ref does not exist"} =
             FerricStore.flow_create(unique_id("flow-value-missing-ref"),
               type: "value-shared",
               partition_key: "tenant-a",
               payload_ref: missing_ref
             )
  end

  test "spawn_children validates child payload refs and includes them in idempotency" do
    parent = unique_id("flow-value-spawn-parent")
    child = unique_id("flow-value-spawn-child")
    other_child = unique_id("flow-value-spawn-other-child")
    partition = "tenant-spawn-ref"

    assert {:ok, created_parent} =
             FerricStore.flow_create(parent,
               type: "spawn-parent",
               state: "dispatch",
               partition_key: partition,
               now_ms: 1_000
             )

    opts = [
      group_id: "fanout-ref",
      wait: :all,
      wait_state: "waiting_children",
      on_child_failed: :ignore,
      on_parent_closed: :abandon_children,
      exhaust_to: %{success: "children_done", failure: "children_failed"},
      partition_key: partition,
      from_state: "dispatch",
      fencing_token: created_parent.fencing_token,
      now_ms: 1_010
    ]

    assert {:error, "ERR flow payload_ref does not exist"} =
             FerricStore.flow_spawn_children(
               parent,
               [%{id: other_child, type: "spawn-child", payload_ref: "missing-spawn-ref"}],
               opts
             )

    assert {:ok, %{ref: ref_a}} =
             FerricStore.flow_value_put("spawn-shared-a", partition_key: partition)

    assert {:ok, %{ref: ref_b}} =
             FerricStore.flow_value_put("spawn-shared-b", partition_key: partition)

    assert {:ok, first} =
             FerricStore.flow_spawn_children(
               parent,
               [%{id: child, type: "spawn-child", payload_ref: ref_a}],
               opts
             )

    assert {:ok, same} =
             FerricStore.flow_spawn_children(
               parent,
               [%{id: child, type: "spawn-child", payload_ref: ref_a}],
               opts
             )

    assert same.version == first.version

    assert {:error, "ERR flow child group idempotency conflict"} =
             FerricStore.flow_spawn_children(
               parent,
               [%{id: child, type: "spawn-child", payload_ref: ref_b}],
               opts
             )
  end

  test "terminal retention does not expire shared payload refs" do
    id = unique_id("flow-value-shared-retention")

    assert {:ok, %{ref: shared_ref}} =
             FerricStore.flow_value_put("shared-retention", partition_key: "tenant-retention")

    assert {:ok, _created} =
             FerricStore.flow_create(id,
               type: "value-shared-retention",
               partition_key: "tenant-retention",
               payload_ref: shared_ref,
               run_at_ms: 1,
               now_ms: 1
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-shared-retention",
               partition_key: "tenant-retention",
               worker: "worker-1",
               limit: 1,
               now_ms: 1
             )

    assert {:ok, _completed} =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token,
               ttl_ms: 1,
               now_ms: 1
             )

    assert {:ok, value_blob} = FerricStore.get(shared_ref)
    assert Ferricstore.Flow.decode_value(value_blob) == "shared-retention"
  end

  test "owner-linked shared payload refs are deleted with owner retention cleanup" do
    owner_id = unique_id("flow-value-owner")

    assert {:ok, _owner} =
             FerricStore.flow_create(owner_id,
               type: "value-owner-retention",
               partition_key: "tenant-retention",
               run_at_ms: 1,
               now_ms: 1
             )

    assert {:ok, %{ref: shared_ref, owner_flow_id: ^owner_id}} =
             FerricStore.flow_value_put("owned-shared",
               partition_key: "tenant-retention",
               owner_flow_id: owner_id
             )

    link_key =
      Ferricstore.Flow.Keys.shared_value_link_key(owner_id, shared_ref, "tenant-retention")

    assert {:ok, ^shared_ref} = FerricStore.get(link_key)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-owner-retention",
               partition_key: "tenant-retention",
               worker: "worker-1",
               limit: 1,
               now_ms: 1
             )

    assert {:ok, _completed} =
             FerricStore.flow_complete(owner_id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token,
               ttl_ms: 1,
               now_ms: 1
             )

    cleanup_now = System.system_time(:millisecond) + 10_000

    assert {:ok, %{flows: 1, values: 1}} =
             FerricStore.flow_retention_cleanup(limit: 10, now_ms: cleanup_now)

    assert {:ok, nil} = FerricStore.get(shared_ref)
    assert {:ok, nil} = FerricStore.get(link_key)
  end

  test "create stores a full payload value and claim/get hydrate it from internal storage" do
    id = unique_id("flow-value-create")
    payload = %{order_id: 123, items: ["book", "pen"]}

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "value-payload",
               partition_key: "tenant-a",
               payload: payload,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert is_binary(created.payload_ref)
    assert created.payload_ref != ""

    assert {:ok, fetched_ref_only} = FerricStore.flow_get(id, partition_key: "tenant-a")
    refute Map.has_key?(fetched_ref_only, :payload)
    assert fetched_ref_only.payload_ref == created.payload_ref

    assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: "tenant-a", full: true)
    assert fetched.payload == payload

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-payload",
               partition_key: "tenant-a",
               worker: "worker-1",
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.payload == payload
  end

  test "transition can replace payload and retry error preserves current payload" do
    id = unique_id("flow-value-transition")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "value-transition",
               partition_key: "tenant-a",
               payload: "initial",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-transition",
               partition_key: "tenant-a",
               worker: "worker-1",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, transitioned} =
             FerricStore.flow_transition(id, "running", "waiting",
               partition_key: "tenant-a",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               payload: %{step: "waiting"},
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert is_binary(transitioned.payload_ref)
    assert transitioned.payload_ref != claimed.payload_ref

    assert {:ok, [reclaimed]} =
             FerricStore.flow_claim_due("value-transition",
               partition_key: "tenant-a",
               state: "waiting",
               worker: "worker-2",
               limit: 1,
               now_ms: 2_000
             )

    assert reclaimed.payload == %{step: "waiting"}

    assert {:ok, retried} =
             FerricStore.flow_retry(id, reclaimed.lease_token,
               partition_key: "tenant-a",
               fencing_token: reclaimed.fencing_token,
               error: %{reason: "temporary"},
               run_at_ms: 3_000,
               now_ms: 2_100
             )

    assert retried.payload_ref == reclaimed.payload_ref
    assert is_binary(retried.error_ref)

    assert {:ok, fetched_ref_only} = FerricStore.flow_get(id, partition_key: "tenant-a")
    refute Map.has_key?(fetched_ref_only, :payload)
    refute Map.has_key?(fetched_ref_only, :error)

    assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: "tenant-a", full: true)
    assert fetched.payload == %{step: "waiting"}
    assert fetched.error == %{reason: "temporary"}
  end

  test "complete and fail store result/error values without requiring public refs" do
    complete_id = unique_id("flow-value-complete")
    fail_id = unique_id("flow-value-fail")

    assert {:ok, _} =
             FerricStore.flow_create(complete_id,
               type: "value-terminal",
               partition_key: "tenant-a",
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_create(fail_id,
               type: "value-terminal",
               partition_key: "tenant-a",
               run_at_ms: 1_000
             )

    assert {:ok, [complete_claim, fail_claim]} =
             FerricStore.flow_claim_due("value-terminal",
               partition_key: "tenant-a",
               worker: "worker-1",
               limit: 2,
               now_ms: 1_000
             )

    assert {:ok, completed} =
             FerricStore.flow_complete(complete_claim.id, complete_claim.lease_token,
               partition_key: "tenant-a",
               fencing_token: complete_claim.fencing_token,
               result: %{status: "sent"},
               now_ms: 1_100
             )

    assert {:ok, failed} =
             FerricStore.flow_fail(fail_claim.id, fail_claim.lease_token,
               partition_key: "tenant-a",
               fencing_token: fail_claim.fencing_token,
               error: %{code: "bad_input"},
               now_ms: 1_100
             )

    assert is_binary(completed.result_ref)
    assert is_binary(failed.error_ref)

    assert {:ok, fetched_completed} =
             FerricStore.flow_get(complete_claim.id, partition_key: "tenant-a", full: true)

    assert {:ok, fetched_failed} =
             FerricStore.flow_get(fail_claim.id, partition_key: "tenant-a", full: true)

    assert fetched_completed.result == %{status: "sent"}
    assert fetched_failed.error == %{code: "bad_input"}
  end

  test "terminal retention expires generated payload value refs" do
    id = unique_id("flow-value-retention")

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "value-retention",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("x", 256)},
               retention_ttl_ms: 20,
               run_at_ms: 1_000
             )

    assert {:ok, value_blob} = FerricStore.get(created.payload_ref)
    assert is_binary(value_blob)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-retention",
               partition_key: "tenant-retention",
               worker: "worker-retention",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _completed} =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token
             )

    Process.sleep(40)

    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    assert {:ok, nil} = FerricStore.get(created.payload_ref)
  end

  test "cancel terminal retention expires generated payload value refs" do
    id = unique_id("flow-value-cancel-retention")

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "value-cancel-retention",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("x", 256)},
               retention_ttl_ms: 100,
               run_at_ms: 1_000
             )

    assert {:ok, value_blob} = FerricStore.get(created.payload_ref)
    assert is_binary(value_blob)

    assert {:ok, _cancelled} =
             FerricStore.flow_cancel(id,
               partition_key: "tenant-retention",
               fencing_token: created.fencing_token
             )

    Process.sleep(150)

    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    assert {:ok, nil} = FerricStore.get(created.payload_ref)
  end

  test "cancel rejects caller-owned reason_ref input" do
    id = unique_id("flow-value-cancel-reason-ref")
    reason_key = unique_id("flow-value-cancel-reason-user-key")

    assert :ok = FerricStore.set(reason_key, "keep-me")

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "value-cancel-reason-ref",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("x", 256)},
               retention_ttl_ms: 100,
               run_at_ms: 1_000
             )

    assert {:error, "ERR flow reason_ref input is not supported; use reason"} =
             FerricStore.flow_cancel(id,
               partition_key: "tenant-retention",
               fencing_token: created.fencing_token,
               reason_ref: reason_key
             )

    assert {:ok, %{state: "queued"}} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    assert {:ok, "keep-me"} = FerricStore.get(reason_key)
  end

  test "cancel stores inline reason payload as an owned terminal value" do
    id = unique_id("flow-value-cancel-reason")
    reason = %{code: "user_cancelled", details: String.duplicate("x", 256)}

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "value-cancel-reason",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("p", 256)},
               retention_ttl_ms: 100,
               run_at_ms: 1_000
             )

    assert {:ok, cancelled} =
             FerricStore.flow_cancel(id,
               partition_key: "tenant-retention",
               fencing_token: created.fencing_token,
               reason: reason
             )

    assert is_binary(cancelled.error_ref)
    assert cancelled.error_ref != ""

    assert {:ok, fetched} =
             FerricStore.flow_get(id, partition_key: "tenant-retention", full: true)

    assert fetched.error == reason

    Process.sleep(150)

    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    assert {:ok, nil} = FerricStore.get(cancelled.error_ref)
  end

  test "retention cleanup removes expired terminal state, history, and owned values" do
    id = unique_id("flow-value-retention-cleanup")

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "value-retention-cleanup",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("p", 256)},
               retention_ttl_ms: 100,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-retention-cleanup",
               partition_key: "tenant-retention",
               worker: "worker-retention-cleanup",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, completed} =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token,
               result: %{ok: true},
               now_ms: 1_100
             )

    assert :ok =
             Ferricstore.Flow.LMDBWriter.flush_all(FerricStore.Instance.get(:default).shard_count)

    assert {:ok, [_ | _]} =
             FerricStore.flow_history(id,
               partition_key: "tenant-retention",
               include_cold: true,
               consistent_projection: false,
               count: 10
             )

    Process.sleep(150)

    assert {:ok, cleaned} = FerricStore.flow_retention_cleanup(limit: 10)
    assert cleaned.flows >= 1
    assert cleaned.history >= 1
    assert cleaned.values >= 2

    assert :ok =
             Ferricstore.Flow.LMDBWriter.flush_all(FerricStore.Instance.get(:default).shard_count)

    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    assert {:ok, []} =
             FerricStore.flow_history(id,
               partition_key: "tenant-retention",
               include_cold: true,
               consistent_projection: false,
               count: 10
             )

    assert {:ok, nil} = FerricStore.get(created.payload_ref)
    assert {:ok, nil} = FerricStore.get(completed.result_ref)
  end

  test "retention sweeper runs cleanup through Flow command path" do
    id = unique_id("flow-value-retention-sweeper")
    parent = self()
    handler_id = "flow-retention-sweeper-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:ferricstore, :flow, :retention_sweeper, :sweep],
      fn event, measurements, metadata, test_pid ->
        send(test_pid, {:retention_sweeper_event, event, measurements, metadata})
      end,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "value-retention-sweeper",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("s", 256)},
               retention_ttl_ms: 100,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-retention-sweeper",
               partition_key: "tenant-retention",
               worker: "worker-retention-sweeper",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, completed} =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token,
               result: %{ok: true},
               now_ms: 1_100
             )

    Process.sleep(150)

    assert pid = Process.whereis(Ferricstore.Flow.RetentionSweeper)
    send(pid, :sweep)

    assert_receive {:retention_sweeper_event, [:ferricstore, :flow, :retention_sweeper, :sweep],
                    %{flows: flows, history: history, values: values}, %{status: :ok}},
                   5_000

    assert flows >= 1
    assert history >= 1
    assert values >= 2
    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    assert {:ok, nil} = FerricStore.get(created.payload_ref)
    assert {:ok, nil} = FerricStore.get(completed.result_ref)
  end

  test "flow_info exposes retention sweeper operational state" do
    assert Process.whereis(Ferricstore.Flow.RetentionSweeper)

    assert {:ok, info} =
             FerricStore.flow_info("value-retention-info", partition_key: "tenant-retention")

    assert %{
             enabled: true,
             interval_ms: interval_ms,
             limit: limit,
             last_sweep: last_sweep
           } = info.retention_sweeper

    assert is_integer(interval_ms) and interval_ms > 0
    assert is_integer(limit) and limit > 0
    assert is_nil(last_sweep) or is_map(last_sweep)
  end

  test "retention sweeper schedules catch-up pass when cleanup hits limit" do
    parent = self()
    name = :"retention_sweeper_catchup_#{System.unique_integer([:positive])}"

    {:ok, counter} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(counter), do: Agent.stop(counter) end)

    handler_id = {:retention_sweeper_catchup, parent, make_ref()}
    backlog_handler_id = {:retention_sweeper_backlog, parent, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :flow, :retention_sweeper, :sweep],
      fn _event, measurements, metadata, test_pid ->
        send(test_pid, {:retention_sweeper_sweep, measurements, metadata})
      end,
      parent
    )

    :telemetry.attach(
      backlog_handler_id,
      [:ferricstore, :flow, :retention_sweeper, :backlog],
      fn _event, measurements, metadata, test_pid ->
        send(test_pid, {:retention_sweeper_backlog, measurements, metadata})
      end,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    on_exit(fn -> :telemetry.detach(backlog_handler_id) end)

    cleanup_fun = fn opts ->
      run = Agent.get_and_update(counter, fn value -> {value + 1, value + 1} end)
      send(parent, {:retention_cleanup_run, run, opts})

      flows =
        case run do
          1 -> 2
          _ -> 0
        end

      {:ok, %{flows: flows, history: 0, values: 0}}
    end

    pid =
      start_supervised!(
        {Ferricstore.Flow.RetentionSweeper,
         name: name,
         initial_delay_ms: 60_000,
         interval_ms: 60_000,
         catchup_delay_ms: 1,
         limit: 2,
         cleanup_fun: cleanup_fun}
      )

    send(pid, :sweep)

    assert_receive {:retention_cleanup_run, 1, [limit: 2]}, 1_000

    assert_receive {:retention_sweeper_sweep, %{flows: 2, limit: 2},
                    %{status: :ok, limit_hit?: true}},
                   1_000

    assert_receive {:retention_sweeper_backlog, %{flows: 2, limit: 2},
                    %{consecutive_limit_hits: 1}},
                   1_000

    assert_receive {:retention_cleanup_run, 2, [limit: 2]}, 1_000

    assert_receive {:retention_sweeper_sweep, %{flows: 0, limit: 2},
                    %{status: :ok, limit_hit?: false}},
                   1_000
  end

  test "retention sweeper emits dedicated error telemetry" do
    parent = self()
    name = :"retention_sweeper_error_#{System.unique_integer([:positive])}"
    handler_id = {:retention_sweeper_error, parent, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :flow, :retention_sweeper, :error],
      fn _event, measurements, metadata, test_pid ->
        send(test_pid, {:retention_sweeper_error, measurements, metadata})
      end,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    cleanup_fun = fn [limit: 3] -> {:error, "ERR cleanup failed"} end

    pid =
      start_supervised!(
        {Ferricstore.Flow.RetentionSweeper,
         name: name,
         initial_delay_ms: 60_000,
         interval_ms: 60_000,
         limit: 3,
         cleanup_fun: cleanup_fun}
      )

    send(pid, :sweep)

    assert_receive {:retention_sweeper_error, %{count: 1},
                    %{reason: "ERR cleanup failed", limit: 3}},
                   1_000
  end

  test "rewind from terminal back to active clears value ref expiration" do
    id = unique_id("flow-value-rewind-retention")

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "value-rewind-retention",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("x", 256)},
               retention_ttl_ms: 100,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [{created_event_id, _fields}]} =
             FerricStore.flow_history(id, partition_key: "tenant-retention", count: 10)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-rewind-retention",
               partition_key: "tenant-retention",
               worker: "worker-retention",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _completed} =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token
             )

    assert {:ok, rewound} =
             FerricStore.flow_rewind(id,
               partition_key: "tenant-retention",
               to_event: created_event_id
             )

    assert rewound.state == created.state
    assert rewound.payload_ref == created.payload_ref

    Process.sleep(150)

    assert {:ok, fetched} =
             FerricStore.flow_get(id, partition_key: "tenant-retention", full: true)

    assert fetched.state == created.state
    assert fetched.payload == %{large: String.duplicate("x", 256)}
    assert {:ok, value_blob} = FerricStore.get(created.payload_ref)
    assert is_binary(value_blob)
  end

  test "batch APIs also persist full value fields" do
    partition = "tenant-b"
    type = "value-batch"
    complete_id = unique_id("flow-value-batch-complete")
    retry_id = unique_id("flow-value-batch-retry")
    fail_id = unique_id("flow-value-batch-fail")

    assert {:ok, created} =
             FerricStore.flow_create_many(
               partition,
               [
                 %{id: complete_id, payload: %{kind: "complete"}},
                 %{id: retry_id, payload: %{kind: "retry"}},
                 %{id: fail_id, payload: %{kind: "fail"}}
               ],
               type: type,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert Enum.all?(created, &is_binary(&1.payload_ref))

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-1",
               limit: 3,
               now_ms: 1_000
             )

    claimed_by_id = Map.new(claimed, &{&1.id, &1})

    complete_claim = Map.fetch!(claimed_by_id, complete_id)
    retry_claim = Map.fetch!(claimed_by_id, retry_id)
    fail_claim = Map.fetch!(claimed_by_id, fail_id)

    assert {:ok, [completed]} =
             FerricStore.flow_complete_many(
               partition,
               [
                 %{
                   id: complete_id,
                   lease_token: complete_claim.lease_token,
                   fencing_token: complete_claim.fencing_token,
                   result: ["done"]
                 }
               ],
               now_ms: 1_100
             )

    assert {:ok, [retried]} =
             FerricStore.flow_retry_many(
               partition,
               [
                 %{
                   id: retry_id,
                   lease_token: retry_claim.lease_token,
                   fencing_token: retry_claim.fencing_token,
                   error: %{retry: true},
                   payload: %{kind: "retry-updated"}
                 }
               ],
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert {:ok, [failed]} =
             FerricStore.flow_fail_many(
               partition,
               [
                 %{
                   id: fail_id,
                   lease_token: fail_claim.lease_token,
                   fencing_token: fail_claim.fencing_token,
                   error: {:bad, :input}
                 }
               ],
               now_ms: 1_100
             )

    assert is_binary(completed.result_ref)
    assert is_binary(retried.error_ref)
    assert retried.payload_ref != retry_claim.payload_ref
    assert is_binary(failed.error_ref)

    assert {:ok, fetched_completed} =
             FerricStore.flow_get(complete_id, partition_key: partition, full: true)

    assert {:ok, fetched_retried} =
             FerricStore.flow_get(retry_id, partition_key: partition, full: true)

    assert {:ok, fetched_failed} =
             FerricStore.flow_get(fail_id, partition_key: partition, full: true)

    assert fetched_completed.result == ["done"]
    assert fetched_retried.payload == %{kind: "retry-updated"}
    assert fetched_retried.error == %{retry: true}
    assert fetched_failed.payload == %{kind: "fail"}
    assert fetched_failed.error == {:bad, :input}
  end

  test "history only hydrates stored values when values option is requested" do
    id = unique_id("flow-value-history")
    partition = "tenant-a"

    assert {:ok, _created} =
             FerricStore.flow_create(id,
               type: "value-history",
               partition_key: partition,
               payload: %{input: 1},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-history",
               partition_key: partition,
               worker: "worker-1",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _completed} =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: partition,
               fencing_token: claimed.fencing_token,
               result: %{output: 2},
               now_ms: 1_100
             )

    assert {:ok, ref_history} = FerricStore.flow_history(id, partition_key: partition, count: 10)

    refute Enum.any?(ref_history, fn {_event_id, fields} ->
             Map.has_key?(fields, "payload") or Map.has_key?(fields, "result")
           end)

    assert {:ok, value_history} =
             FerricStore.flow_history(id, partition_key: partition, count: 10, values: true)

    value_events = Map.new(value_history, fn {_event_id, fields} -> {fields["event"], fields} end)

    assert value_events["created"]["payload"] == %{input: 1}
    assert value_events["claimed"]["payload"] == %{input: 1}
    assert value_events["completed"]["payload"] == %{input: 1}
    assert value_events["completed"]["result"] == %{output: 2}
  end

  defp unique_id(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"
end
