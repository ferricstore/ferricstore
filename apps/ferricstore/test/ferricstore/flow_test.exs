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

    assert {:error, "ERR flow now_ms must be a non-negative integer"} =
             FerricStore.flow_retry("flow", "token", now_ms: -1)

    assert {:error, "ERR flow id must be a non-empty string"} =
             FerricStore.flow_history("")

    assert {:error, "ERR flow opts must be a keyword list"} =
             FerricStore.flow_history("flow", ["bad"])

    assert {:error, "ERR flow count must be a positive integer"} =
             FerricStore.flow_history("flow", count: 0)

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
             FerricStore.flow_complete(id, "wrong-token", result_ref: "result:" <> id)

    assert {:ok, completed} =
             FerricStore.flow_complete(claimed.id, claimed.lease_token,
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

    assert {:ok, retried} =
             FerricStore.flow_retry(claimed.id, claimed.lease_token,
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

    assert {:ok, _} = FerricStore.flow_complete(id, claimed.lease_token)
    assert {:ok, events} = FerricStore.flow_history(id, count: 10)

    assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]
  end
end
