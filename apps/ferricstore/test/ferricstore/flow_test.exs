defmodule Ferricstore.FlowTest do
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

  defp uid(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"

  defp shard_for(key) do
    Ferricstore.Store.Router.shard_for(FerricStore.Instance.get(:default), key)
  end

  defp different_partition_keys do
    base = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))

    first =
      1..64
      |> Enum.map(&"#{base}:#{&1}")
      |> Enum.find(fn key ->
        shard_for(Ferricstore.Flow.Keys.state_key("probe", key)) !=
          shard_for(Ferricstore.Flow.Keys.state_key("probe", nil))
      end)

    second =
      1..64
      |> Enum.map(&"#{base}:other:#{&1}")
      |> Enum.find(fn key ->
        shard_for(Ferricstore.Flow.Keys.state_key("probe", key)) !=
          shard_for(Ferricstore.Flow.Keys.state_key("probe", first))
      end)

    {first, second}
  end

  test "flow_create stores state and prevents duplicate ids" do
    id = uid("flow-create")

    assert {:ok, flow} =
             FerricStore.flow_create(id,
               type: "checkout",
               state: "queued",
               payload_ref: "payload:" <> id,
               run_at_ms: 1_000
             )

    assert flow.id == id
    assert flow.type == "checkout"
    assert flow.state == "queued"
    assert flow.version == 1
    assert flow.fencing_token == 0
    assert flow.payload_ref == "payload:" <> id

    assert {:ok, fetched} = FerricStore.flow_get(id)
    assert fetched.id == id
    assert fetched.state == "queued"

    assert {:error, "ERR flow already exists"} =
             FerricStore.flow_create(id, type: "checkout", state: "queued")
  end

  test "flow APIs reject malformed inputs before raft apply" do
    assert {:error, "ERR flow id must be a non-empty string"} =
             FerricStore.flow_create("", type: "checkout")

    assert {:error, "ERR flow opts must be a keyword list"} =
             FerricStore.flow_create("bad-opts", ["checkout"])

    assert {:error, "ERR flow type is required"} =
             FerricStore.flow_create("missing-type", state: "queued")

    assert {:error, "ERR flow type must be a non-empty string"} =
             FerricStore.flow_create("empty-type", type: "")

    assert {:error, "ERR flow now_ms must be a non-negative integer"} =
             FerricStore.flow_create("bad-now", type: "checkout", now_ms: -1)

    assert {:error, "ERR flow run_at_ms must be a non-negative integer"} =
             FerricStore.flow_create("bad-run-at", type: "checkout", run_at_ms: -1)

    assert {:error, "ERR flow partition_key must be a non-empty string or :global"} =
             FerricStore.flow_create("bad-partition", type: "checkout", partition_key: "")

    assert {:error, "ERR flow opts must be a keyword list"} =
             FerricStore.flow_get("bad-get", ["bad"])

    assert {:error, "ERR flow type must be a non-empty string"} =
             FerricStore.flow_claim_due("", worker: "worker-a")

    assert {:error, "ERR flow worker is required"} =
             FerricStore.flow_claim_due("email", [])

    assert {:error, "ERR flow lease_ms must be a positive integer"} =
             FerricStore.flow_claim_due("email", worker: "worker-a", lease_ms: 0)

    assert {:error, "ERR flow limit must be a positive integer"} =
             FerricStore.flow_claim_due("email", worker: "worker-a", limit: 0)

    assert {:error, "ERR flow lease_token must be a non-empty string"} =
             FerricStore.flow_complete("flow", "")

    assert {:error, "ERR flow fencing_token is required"} =
             FerricStore.flow_complete("flow", "token")

    assert {:error, "ERR flow now_ms must be a non-negative integer"} =
             FerricStore.flow_retry("flow", "token", fencing_token: 0, now_ms: -1)

    assert {:error, "ERR flow id must be a non-empty string"} =
             FerricStore.flow_history("")

    assert {:error, "ERR flow opts must be a keyword list"} =
             FerricStore.flow_history("flow", ["bad"])

    assert {:error, "ERR flow count must be a positive integer"} =
             FerricStore.flow_history("flow", count: 0)

    assert {:error, "ERR flow id must be a non-empty string"} =
             FerricStore.flow_transition("", "queued", "done")

    assert {:error, "ERR flow from must be a non-empty string"} =
             FerricStore.flow_transition("flow", "", "done")

    assert {:error, "ERR flow to must be a non-empty string"} =
             FerricStore.flow_transition("flow", "queued", "")

    assert {:error, "ERR flow opts must be a keyword list"} =
             FerricStore.flow_transition("flow", "queued", "done", ["bad"])

    assert {:error, "ERR flow lease_token must be a non-empty string"} =
             FerricStore.flow_transition("flow", "queued", "done", lease_token: "")

    assert {:error, "ERR flow fencing_token is required"} =
             FerricStore.flow_transition("flow", "queued", "done")

    assert {:error, "ERR flow lease_token must be a non-empty string"} =
             FerricStore.flow_fail("flow", "")

    assert {:error, "ERR flow fencing_token is required"} =
             FerricStore.flow_fail("flow", "token")

    assert {:error, "ERR flow now_ms must be a non-negative integer"} =
             FerricStore.flow_fail("flow", "token", fencing_token: 0, now_ms: -1)

    assert {:error, "ERR flow id must be a non-empty string"} =
             FerricStore.flow_cancel("")

    assert {:error, "ERR flow opts must be a keyword list"} =
             FerricStore.flow_cancel("flow", ["bad"])

    assert {:error, "ERR flow fencing_token is required"} =
             FerricStore.flow_cancel("flow")

    assert {:error, "ERR flow partition_key must be a non-empty string or :global"} =
             FerricStore.flow_claim_due("email", worker: "worker-a", partition_key: "")

    large_id = String.duplicate("x", 65_536)

    assert {:error, "ERR key too large" <> _} =
             FerricStore.flow_create(large_id, type: "checkout")
  end

  test "flow_claim_due atomically leases due flows and removes them from due set" do
    id = uid("flow-claim")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "email",
               state: "queued",
               payload_ref: "payload:" <> id,
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("email",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert claimed.id == id
    assert claimed.state == "running"
    assert claimed.lease_owner == "worker-a"
    assert is_binary(claimed.lease_token)
    assert claimed.fencing_token == 1
    assert claimed.version == 2

    assert {:ok, []} =
             FerricStore.flow_claim_due("email",
               state: "queued",
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )
  end

  test "partition_key keeps related flow keys on one shard and can spread partitions" do
    {partition_a, partition_b} = different_partition_keys()
    id = uid("flow-partition-keys")

    state_a = Ferricstore.Flow.Keys.state_key(id, partition_a)
    history_a = Ferricstore.Flow.Keys.history_key(id, partition_a)
    due_a = Ferricstore.Flow.Keys.due_key("email", "queued", 0, partition_a)
    state_b = Ferricstore.Flow.Keys.state_key(id, partition_b)

    assert shard_for(state_a) == shard_for(history_a)
    assert shard_for(state_a) == shard_for(due_a)
    assert shard_for(state_a) != shard_for(state_b)
  end

  test "partition_key scopes claim, complete, retry, get, and history" do
    partition = uid("tenant")
    id = uid("flow-partition")

    assert {:ok, flow} =
             FerricStore.flow_create(id,
               type: "email",
               partition_key: partition,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 999
             )

    assert flow.partition_key == partition

    assert {:ok, nil} = FerricStore.flow_get(id)
    assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: partition)
    assert fetched.id == id

    assert {:ok, []} =
             FerricStore.flow_claim_due("email",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("email",
               partition_key: partition,
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.partition_key == partition

    assert {:error, "ERR flow not found"} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token
             )

    assert {:ok, completed} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: partition
             )

    assert completed.state == "completed"

    assert {:ok, events} = FerricStore.flow_history(id, partition_key: partition)

    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]
  end

  test "flow_claim_due only scans the selected partition" do
    partition_a = uid("tenant-a")
    partition_b = uid("tenant-b")
    id_a = uid("flow-partition-claim-a")
    id_b = uid("flow-partition-claim-b")

    assert {:ok, _} =
             FerricStore.flow_create(id_a,
               type: "email",
               partition_key: partition_a,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_create(id_b,
               type: "email",
               partition_key: partition_b,
               run_at_ms: 1_000
             )

    assert {:ok, [claimed_a]} =
             FerricStore.flow_claim_due("email",
               partition_key: partition_a,
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert claimed_a.id == id_a
    assert claimed_a.partition_key == partition_a

    assert {:ok, []} =
             FerricStore.flow_claim_due("email",
               partition_key: partition_a,
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert {:ok, [claimed_b]} =
             FerricStore.flow_claim_due("email",
               partition_key: partition_b,
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_000
             )

    assert claimed_b.id == id_b
    assert claimed_b.partition_key == partition_b
  end

  test "flow_claim_due skips stale due index members without starving live work" do
    stale_id = "a-" <> uid("flow-stale-due")
    live_id = "z-" <> uid("flow-live-due")

    assert {:ok, _} =
             FerricStore.flow_create(stale_id, type: "stale-scan", run_at_ms: 1_000)

    assert {:ok, _} =
             FerricStore.flow_create(live_id, type: "stale-scan", run_at_ms: 1_000)

    assert {:ok, 1} = FerricStore.del(Ferricstore.Flow.Keys.state_key(stale_id))

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("stale-scan",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == live_id
  end

  test "flow_complete enforces lease token guard and writes terminal state" do
    id = uid("flow-complete")

    assert {:ok, _} =
             FerricStore.flow_create(id, type: "image", state: "queued", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("image",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_complete(id, "wrong-token",
               fencing_token: claimed.fencing_token,
               result_ref: "result:" <> id
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               result_ref: "result:" <> id
             )

    assert {:ok, completed} =
             FerricStore.flow_complete(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               result_ref: "result:" <> id
             )

    assert completed.state == "completed"
    assert completed.result_ref == "result:" <> id
    assert completed.lease_token == nil
    assert completed.version == 3
  end

  test "flow_retry clears lease and reschedules flow" do
    id = uid("flow-retry")

    assert {:ok, _} =
             FerricStore.flow_create(id, type: "webhook", state: "queued", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("webhook",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_retry(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               error_ref: "error:" <> id,
               run_at_ms: 2_000
             )

    assert {:ok, retried} =
             FerricStore.flow_retry(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               error_ref: "error:" <> id,
               run_at_ms: 2_000
             )

    assert retried.state == "queued"
    assert retried.attempts == 1
    assert retried.error_ref == "error:" <> id
    assert retried.lease_token == nil

    assert {:ok, [reclaimed]} =
             FerricStore.flow_claim_due("webhook",
               state: "queued",
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2_000
             )

    assert reclaimed.id == id
    assert reclaimed.lease_owner == "worker-b"
  end

  test "expired running lease can be reclaimed" do
    id = uid("flow-reclaim")

    assert {:ok, _} =
             FerricStore.flow_create(id, type: "lease", state: "queued", run_at_ms: 1_000)

    assert {:ok, [first]} =
             FerricStore.flow_claim_due("lease",
               state: "queued",
               worker: "worker-a",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_000
             )

    assert first.lease_deadline_ms == 1_050

    assert {:ok, []} =
             FerricStore.flow_claim_due("lease",
               state: "running",
               worker: "worker-b",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_049
             )

    assert {:ok, [second]} =
             FerricStore.flow_claim_due("lease",
               state: "running",
               worker: "worker-b",
               lease_ms: 50,
               limit: 1,
               now_ms: 1_050
             )

    assert second.id == id
    assert second.lease_owner == "worker-b"
    assert second.version == 3
    assert second.lease_token != first.lease_token
  end

  test "flow_transition atomically moves state, due index, and history" do
    id = uid("flow-transition")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "checkout",
               state: "payment_pending",
               run_at_ms: 1_000,
               now_ms: 900
             )

    assert {:ok, transitioned} =
             FerricStore.flow_transition(id, "payment_pending", "email_pending",
               fencing_token: 0,
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert transitioned.state == "email_pending"
    assert transitioned.next_run_at_ms == 2_000
    assert transitioned.version == 2

    assert {:ok, []} =
             FerricStore.flow_claim_due("checkout",
               state: "payment_pending",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("checkout",
               state: "email_pending",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2_000
             )

    assert claimed.id == id

    assert {:ok, events} = FerricStore.flow_history(id)

    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == [
             "created",
             "transitioned",
             "claimed"
           ]
  end

  test "flow_transition enforces expected state and running lease guard" do
    id = uid("flow-transition-guard")

    assert {:ok, _} =
             FerricStore.flow_create(id, type: "checkout", state: "queued", run_at_ms: 1_000)

    assert {:error, "ERR flow wrong state"} =
             FerricStore.flow_transition(id, "running", "completed",
               fencing_token: 0,
               run_at_ms: 1_000
             )

    assert {:ok, fetched} = FerricStore.flow_get(id)
    assert fetched.state == "queued"
    assert fetched.version == 1

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("checkout",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_transition(id, "running", "next",
               fencing_token: claimed.fencing_token,
               run_at_ms: 2_000
             )

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_transition(id, "running", "next",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               run_at_ms: 2_000
             )

    assert {:ok, transitioned} =
             FerricStore.flow_transition(id, "running", "next",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               run_at_ms: 2_000
             )

    assert transitioned.state == "next"
    assert transitioned.lease_token == nil
  end

  test "flow_transition rolls back index changes when derived keys are invalid" do
    id = uid("flow-transition-rollback")
    huge_state = String.duplicate("x", 65_536)

    assert {:ok, _} =
             FerricStore.flow_create(id, type: "audit", state: "queued", run_at_ms: 1_000)

    assert {:error, "ERR key too large" <> _} =
             FerricStore.flow_transition(id, "queued", huge_state,
               fencing_token: 0,
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert {:ok, fetched} = FerricStore.flow_get(id)
    assert fetched.state == "queued"
    assert fetched.version == 1

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("audit",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == id
  end

  test "flow_fail and flow_cancel write terminal states and remove due work" do
    fail_id = uid("flow-fail")
    cancel_id = uid("flow-cancel")
    fail_type = "jobs-fail"
    cancel_type = "jobs-cancel"

    assert {:ok, _} = FerricStore.flow_create(fail_id, type: fail_type, run_at_ms: 1_000)
    assert {:ok, _} = FerricStore.flow_create(cancel_id, type: cancel_type, run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(fail_type,
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == fail_id

    assert {:error, "ERR stale flow lease"} =
             FerricStore.flow_fail(fail_id, claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               error_ref: "error:" <> fail_id,
               now_ms: 1_500
             )

    assert {:ok, failed} =
             FerricStore.flow_fail(fail_id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               error_ref: "error:" <> fail_id,
               now_ms: 1_500
             )

    assert failed.state == "failed"
    assert failed.error_ref == "error:" <> fail_id
    assert failed.lease_token == nil
    assert failed.next_run_at_ms == nil

    assert {:ok, cancelled} =
             FerricStore.flow_cancel(cancel_id,
               fencing_token: 0,
               reason_ref: "reason:" <> cancel_id,
               now_ms: 1_500
             )

    assert cancelled.state == "cancelled"
    assert cancelled.error_ref == "reason:" <> cancel_id
    assert cancelled.next_run_at_ms == nil

    assert {:ok, []} =
             FerricStore.flow_claim_due(fail_type,
               state: "queued",
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_500
             )

    assert {:ok, []} =
             FerricStore.flow_claim_due(cancel_type,
               state: "queued",
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 10,
               now_ms: 1_500
             )
  end

  test "flow_history returns transition events" do
    id = uid("flow-history")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "audit",
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 999
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("audit",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token
             )

    assert {:ok, events} = FerricStore.flow_history(id, count: 10)

    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]
  end
end
