defmodule Ferricstore.FlowValuePayloadTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Store.Router

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "create stores a full payload value and claim/get hydrate it from internal storage" do
    id = unique_id("flow-value-create")
    payload = %{order_id: 123, items: ["book", "pen"]}

    assert :ok =
             FerricStore.flow_create(id,
               type: "value-payload",
               partition_key: "tenant-a",
               payload: payload,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, created} = FerricStore.flow_get(id, partition_key: "tenant-a")

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

    assert :ok =
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

    assert :ok =
             FerricStore.flow_transition(id, "running", "waiting",
               partition_key: "tenant-a",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               payload: %{step: "waiting"},
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert {:ok, transitioned} = FerricStore.flow_get(id, partition_key: "tenant-a")

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

    assert :ok =
             FerricStore.flow_retry(id, reclaimed.lease_token,
               partition_key: "tenant-a",
               fencing_token: reclaimed.fencing_token,
               error: %{reason: "temporary"},
               run_at_ms: 3_000,
               now_ms: 2_100
             )

    assert {:ok, retried} = FerricStore.flow_get(id, partition_key: "tenant-a")

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

    assert :ok =
             FerricStore.flow_create(complete_id,
               type: "value-terminal",
               partition_key: "tenant-a",
               run_at_ms: 1_000
             )

    assert :ok =
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

    assert :ok =
             FerricStore.flow_complete(complete_claim.id, complete_claim.lease_token,
               partition_key: "tenant-a",
               fencing_token: complete_claim.fencing_token,
               result: %{status: "sent"},
               now_ms: 1_100
             )

    assert :ok =
             FerricStore.flow_fail(fail_claim.id, fail_claim.lease_token,
               partition_key: "tenant-a",
               fencing_token: fail_claim.fencing_token,
               error: %{code: "bad_input"},
               now_ms: 1_100
             )

    assert {:ok, completed} = FerricStore.flow_get(complete_claim.id, partition_key: "tenant-a")
    assert {:ok, failed} = FerricStore.flow_get(fail_claim.id, partition_key: "tenant-a")

    assert is_binary(completed.result_ref)
    assert is_binary(failed.error_ref)

    assert {:ok, fetched_completed} =
             FerricStore.flow_get(complete_claim.id, partition_key: "tenant-a", full: true)

    assert {:ok, fetched_failed} =
             FerricStore.flow_get(fail_claim.id, partition_key: "tenant-a", full: true)

    assert fetched_completed.result == %{status: "sent"}
    assert fetched_failed.error == %{code: "bad_input"}
  end

  test "terminal retention does not materialize existing generated payload value refs" do
    id = unique_id("flow-value-retention")

    assert :ok =
             FerricStore.flow_create(id,
               type: "value-retention",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("x", 256)},
               retention_ttl_ms: 20,
               run_at_ms: 1_000
             )

    assert {:ok, created} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    assert {:ok, value_blob} = internal_get(created.payload_ref)
    assert is_binary(value_blob)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-retention",
               partition_key: "tenant-retention",
               worker: "worker-retention",
               limit: 1,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token
             )

    wait_terminal_removed!(id, "tenant-retention")

    assert {:ok, [%{large: large_blob_after_terminal_expiry}]} =
             FerricStore.flow_value_mget([created.payload_ref])

    assert large_blob_after_terminal_expiry == String.duplicate("x", 256)
  end

  test "value_mget can omit values larger than max_bytes without decoding them" do
    payload = %{large: String.duplicate("x", 512)}

    assert {:ok, %{ref: ref}} =
             FerricStore.flow_value_put(payload, partition_key: "tenant-a")

    assert {:ok, [%{ref: ^ref, value_omitted: true, value_size: size}]} =
             FerricStore.flow_value_mget([ref], max_bytes: 1)

    assert is_integer(size)
    assert size > 1

    assert {:ok, [^payload]} = FerricStore.flow_value_mget([ref], max_bytes: 64 * 1024)
    assert {:ok, [^payload]} = FerricStore.flow_value_mget([ref], value_max_bytes: 64 * 1024)
    assert {:ok, [^payload]} = FerricStore.flow_value_mget([ref], payload_max_bytes: 64 * 1024)
  end

  test "shared value TTL uses command time when now_ms is omitted" do
    assert {:ok, %{ref: ref}} =
             FerricStore.flow_value_put("expires", partition_key: "tenant-a", ttl_ms: 60_000)

    assert {:ok, ["expires"]} = FerricStore.flow_value_mget([ref])
  end

  test "shared value expiry stays in the exact integer domain" do
    max_exact = 9_007_199_254_740_991

    assert {:error, "ERR flow now_ms must be an exact non-negative integer"} =
             FerricStore.flow_value_put("invalid", now_ms: max_exact + 1)

    assert {:error, "ERR flow ttl_ms must be an exact positive integer"} =
             FerricStore.flow_value_put("invalid", ttl_ms: max_exact + 1)

    assert {:error, "ERR flow expiry exceeds the exact integer limit"} =
             FerricStore.flow_value_put("invalid", now_ms: max_exact, ttl_ms: 1)
  end

  test "value_mget returns hot values and leaves ordinary missing refs as nil" do
    assert {:ok, %{ref: ref}} =
             FerricStore.flow_value_put("hot-value", partition_key: "tenant-a")

    missing_ref = unique_id("ordinary-missing-flow-value-ref")

    assert {:ok, ["hot-value", nil]} = FerricStore.flow_value_mget([ref, missing_ref])
  end

  test "cancel terminal retention does not materialize existing generated payload value refs" do
    id = unique_id("flow-value-cancel-retention")

    assert :ok =
             FerricStore.flow_create(id,
               type: "value-cancel-retention",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("x", 256)},
               retention_ttl_ms: 100,
               run_at_ms: 1_000
             )

    assert {:ok, created} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    assert {:ok, value_blob} = internal_get(created.payload_ref)
    assert is_binary(value_blob)

    assert :ok =
             FerricStore.flow_cancel(id,
               partition_key: "tenant-retention",
               fencing_token: created.fencing_token
             )

    wait_terminal_removed!(id, "tenant-retention")

    assert {:ok, [%{large: large_blob_after_terminal_expiry}]} =
             FerricStore.flow_value_mget([created.payload_ref])

    assert large_blob_after_terminal_expiry == String.duplicate("x", 256)
  end

  test "cancel rejects public reason_ref input without touching external value" do
    id = unique_id("flow-value-cancel-reason-ref")
    reason_key = unique_id("flow-value-cancel-reason-user-key")

    assert :ok = FerricStore.set(reason_key, "keep-me")

    assert :ok =
             FerricStore.flow_create(id,
               type: "value-cancel-reason-ref",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("x", 256)},
               retention_ttl_ms: 100,
               run_at_ms: 1_000
             )

    assert {:ok, created} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    assert {:error, "ERR flow reason_ref input is not supported; use reason"} =
             FerricStore.flow_cancel(id,
               partition_key: "tenant-retention",
               fencing_token: created.fencing_token,
               reason_ref: reason_key
             )

    Process.sleep(150)

    assert {:ok, active} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    assert active.state == "queued"
    assert {:ok, "keep-me"} = FerricStore.get(reason_key)
  end

  test "cancel stores inline reason payload as an owned terminal value" do
    id = unique_id("flow-value-cancel-reason")
    reason = %{code: "user_cancelled", details: String.duplicate("x", 256)}
    now_ms = System.os_time(:millisecond)

    assert :ok =
             FerricStore.flow_create(id,
               type: "value-cancel-reason",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("p", 256)},
               retention_ttl_ms: 2_000,
               run_at_ms: now_ms,
               now_ms: now_ms
             )

    assert {:ok, created} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    assert :ok =
             FerricStore.flow_cancel(id,
               partition_key: "tenant-retention",
               fencing_token: created.fencing_token,
               reason: reason,
               now_ms: now_ms + 1
             )

    assert {:ok, cancelled} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    assert is_binary(cancelled.error_ref)
    assert cancelled.error_ref != ""

    assert {:ok, fetched} =
             FerricStore.flow_get(id, partition_key: "tenant-retention", full: true)

    assert fetched.error == reason

    ShardHelpers.eventually(
      fn -> Ferricstore.HLC.now_ms() >= cancelled.terminal_retention_until_ms + 1 end,
      "cancelled terminal retention deadline should elapse",
      1_000,
      5
    )

    cleaned =
      cleanup_until_flow_removed!(
        id,
        "tenant-retention",
        cancelled.terminal_retention_until_ms + 1
      )

    assert cleaned.flows >= 1
    assert cleaned.values >= 1
    assert {:ok, nil} = internal_get(cancelled.error_ref)
  end

  test "retention cleanup removes expired terminal state, history, and owned values" do
    id = unique_id("flow-value-retention-cleanup")

    assert :ok =
             FerricStore.flow_create(id,
               type: "value-retention-cleanup",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("p", 256)},
               retention_ttl_ms: 60_000,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, created} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-retention-cleanup",
               partition_key: "tenant-retention",
               worker: "worker-retention-cleanup",
               limit: 1,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token,
               result: %{ok: true},
               now_ms: 1_100
             )

    assert {:ok, completed} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    cleanup_now_ms = completed.terminal_retention_until_ms + 1

    cleaned =
      cleanup_until_flow_removed!(id, "tenant-retention", cleanup_now_ms)

    assert cleaned.flows >= 1
    assert cleaned.history >= 1
    assert cleaned.values >= 2

    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    assert {:ok, []} =
             FerricStore.flow_history(id,
               partition_key: "tenant-retention",
               include_cold: true,
               consistent_projection: false,
               count: 10
             )

    assert {:ok, nil} = internal_get(created.payload_ref)
    assert {:ok, nil} = internal_get(completed.result_ref)
  end

  test "retention cleanup returns an exact continuation without a final empty pass" do
    partition = "tenant-retention-continuation"
    ids = [unique_id("retention-continuation-a"), unique_id("retention-continuation-b")]

    Enum.each(ids, fn id ->
      assert :ok =
               FerricStore.flow_create(id,
                 type: "retention-continuation",
                 partition_key: partition,
                 retention_ttl_ms: 60_000,
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )
    end)

    assert {:ok, claims} =
             FerricStore.flow_claim_due("retention-continuation",
               partition_key: partition,
               worker: "retention-continuation-worker",
               limit: 2,
               now_ms: 1_000
             )

    assert length(claims) == 2

    Enum.each(claims, fn claim ->
      assert :ok =
               FerricStore.flow_complete(claim.id, claim.lease_token,
                 partition_key: partition,
                 fencing_token: claim.fencing_token,
                 now_ms: 1_100
               )
    end)

    cleanup_now_ms =
      ids
      |> Enum.map(fn id ->
        assert {:ok, completed} = FerricStore.flow_get(id, partition_key: partition)
        completed.terminal_retention_until_ms + 1
      end)
      |> Enum.max()

    assert {:ok, first} =
             FerricStore.flow_retention_cleanup(limit: 1, now_ms: cleanup_now_ms)

    assert first.flows == 1
    assert first.more? == true
    assert is_binary(first.continuation)

    assert {:ok, second} =
             FerricStore.flow_retention_cleanup(
               limit: 1,
               now_ms: cleanup_now_ms,
               continuation: first.continuation
             )

    assert second.flows == 1
    assert second.more? == false
    assert second.continuation == nil

    ctx = FerricStore.Instance.get(:default)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    assert Enum.all?(ids, fn id ->
             FerricStore.flow_get(id, partition_key: partition) == {:ok, nil}
           end)

    assert {:error, "ERR invalid flow retention continuation"} =
             FerricStore.flow_retention_cleanup(
               limit: 1,
               now_ms: cleanup_now_ms,
               continuation: <<1, 9, 0::unsigned-big-32>>
             )

    assert {:error, "ERR flow continuation must be a binary token"} =
             FerricStore.flow_retention_cleanup(
               limit: 1,
               now_ms: cleanup_now_ms,
               continuation: %{shard: 0}
             )
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

    assert :ok =
             FerricStore.flow_create(id,
               type: "value-retention-sweeper",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("s", 256)},
               retention_ttl_ms: 100,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, created} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-retention-sweeper",
               partition_key: "tenant-retention",
               worker: "worker-retention-sweeper",
               limit: 1,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token,
               result: %{ok: true},
               now_ms: 1_100
             )

    assert {:ok, completed} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    ShardHelpers.eventually(
      fn -> Ferricstore.HLC.now_ms() > completed.terminal_retention_until_ms end,
      "retention deadline did not elapse",
      1_000,
      10
    )

    assert pid = Process.whereis(Ferricstore.Flow.RetentionSweeper)
    send(pid, :sweep)

    cleaned = await_retention_sweeper_cleanup!(5_000)

    assert cleaned.flows >= 1
    assert cleaned.history >= 1
    assert cleaned.values >= 2
    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    assert {:ok, nil} = internal_get(created.payload_ref)
    assert {:ok, nil} = internal_get(completed.result_ref)
  end

  test "rewind from terminal back to active clears value ref expiration" do
    id = unique_id("flow-value-rewind-retention")

    assert :ok =
             FerricStore.flow_create(id,
               type: "value-rewind-retention",
               partition_key: "tenant-retention",
               payload: %{large: String.duplicate("x", 256)},
               retention_ttl_ms: 100,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, created} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    assert {:ok, [{created_event_id, _fields}]} =
             FerricStore.flow_history(id, partition_key: "tenant-retention", count: 10)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-rewind-retention",
               partition_key: "tenant-retention",
               worker: "worker-retention",
               limit: 1,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token
             )

    assert :ok =
             FerricStore.flow_rewind(id,
               partition_key: "tenant-retention",
               to_event: created_event_id
             )

    assert {:ok, rewound} = FerricStore.flow_get(id, partition_key: "tenant-retention")

    assert rewound.state == created.state
    assert rewound.payload_ref == created.payload_ref

    Process.sleep(150)

    assert {:ok, fetched} =
             FerricStore.flow_get(id, partition_key: "tenant-retention", full: true)

    assert fetched.state == created.state
    assert fetched.payload == %{large: String.duplicate("x", 256)}
    assert {:ok, [%{large: large_blob}]} = FerricStore.flow_value_mget([created.payload_ref])
    assert large_blob == String.duplicate("x", 256)
  end

  test "batch APIs also persist full value fields" do
    partition = "tenant-b"
    type = "value-batch"
    complete_id = unique_id("flow-value-batch-complete")
    retry_id = unique_id("flow-value-batch-retry")
    fail_id = unique_id("flow-value-batch-fail")

    assert :ok =
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

    assert {:ok, created_complete} = FerricStore.flow_get(complete_id, partition_key: partition)
    assert {:ok, created_retry} = FerricStore.flow_get(retry_id, partition_key: partition)
    assert {:ok, created_fail} = FerricStore.flow_get(fail_id, partition_key: partition)
    created = [created_complete, created_retry, created_fail]

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

    assert :ok =
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

    assert {:ok, completed} = FerricStore.flow_get(complete_id, partition_key: partition)

    assert :ok =
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

    assert {:ok, retried} = FerricStore.flow_get(retry_id, partition_key: partition)

    assert :ok =
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

    assert {:ok, failed} = FerricStore.flow_get(fail_id, partition_key: partition)

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

    assert :ok =
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

    assert :ok =
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

  defp wait_terminal_removed!(id, partition_key) do
    cleanup_now_ms =
      case FerricStore.flow_get(id, partition_key: partition_key) do
        {:ok, %{terminal_retention_until_ms: deadline}} when is_integer(deadline) ->
          cleanup_now_ms = deadline + 1

          Ferricstore.Test.ShardHelpers.eventually(
            fn -> Ferricstore.HLC.now_ms() >= cleanup_now_ms end,
            "terminal retention deadline should elapse",
            1_000,
            5
          )

          cleanup_now_ms

        {:ok, nil} ->
          Ferricstore.HLC.now_ms()
      end

    assert {:ok, cleaned} =
             FerricStore.flow_retention_cleanup(limit: 10, now_ms: cleanup_now_ms)

    assert cleaned.flows >= 0

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> match?({:ok, nil}, FerricStore.flow_get(id, partition_key: partition_key)) end,
      "terminal flow #{inspect(id)} should be removed by retention",
      1_000,
      10
    )
  end

  defp cleanup_until_flow_removed!(id, partition_key, now_ms) do
    do_cleanup_until_flow_removed!(
      id,
      partition_key,
      now_ms,
      %{flows: 0, history: 0, values: 0, active_timeouts: 0},
      100
    )
  end

  defp do_cleanup_until_flow_removed!(id, partition_key, now_ms, totals, attempts) do
    assert {:ok, cleaned} =
             FerricStore.flow_retention_cleanup(limit: 10, now_ms: now_ms)

    totals = merge_cleanup_counts(totals, cleaned)
    ctx = FerricStore.Instance.get(:default)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    case FerricStore.flow_get(id, partition_key: partition_key) do
      {:ok, nil} ->
        totals

      {:ok, _record} when attempts > 1 ->
        do_cleanup_until_flow_removed!(id, partition_key, now_ms, totals, attempts - 1)

      {:ok, _record} ->
        flunk("retention cleanup did not remove flow #{inspect(id)}")
    end
  end

  defp await_retention_sweeper_cleanup!(timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    do_await_retention_sweeper_cleanup!(
      deadline,
      %{flows: 0, history: 0, values: 0, active_timeouts: 0}
    )
  end

  defp do_await_retention_sweeper_cleanup!(deadline, totals) do
    timeout_ms = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:retention_sweeper_event, [:ferricstore, :flow, :retention_sweeper, :sweep], cleaned,
       %{status: :ok, more?: more?}} ->
        totals = merge_cleanup_counts(totals, cleaned)

        if totals.flows >= 1 and totals.history >= 1 and totals.values >= 2 and not more? do
          totals
        else
          do_await_retention_sweeper_cleanup!(deadline, totals)
        end
    after
      timeout_ms -> flunk("retention sweeper did not finish bounded cleanup passes")
    end
  end

  defp merge_cleanup_counts(left, right) do
    Map.new([:flows, :history, :values, :active_timeouts], fn key ->
      {key, Map.get(left, key, 0) + Map.get(right, key, 0)}
    end)
  end

  defp internal_get(key), do: {:ok, Router.get(FerricStore.Instance.get(:default), key)}

  defp unique_id(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"
end
