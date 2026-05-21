defmodule Ferricstore.FlowSignalTest do
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

  test "signal attaches named values and history records the signal" do
    id = uid("signal-values")

    assert :ok =
             FerricStore.flow_create(id,
               type: "signal-values",
               partition_key: "tenant-a",
               values: %{"order" => "order-bytes"},
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_signal(id,
               partition_key: "tenant-a",
               signal: "payment_received",
               values: %{"payment_event" => "payment-bytes"},
               now_ms: 1_100
             )

    assert {:ok, fetched} =
             FerricStore.flow_get(id,
               partition_key: "tenant-a",
               values: ["payment_event"]
             )

    assert fetched.state == "queued"
    assert fetched.values == %{"payment_event" => "payment-bytes"}
    assert Map.has_key?(fetched.value_refs, "order")

    assert {:ok, history} =
             FerricStore.flow_history(id,
               partition_key: "tenant-a",
               count: 10
             )

    assert Enum.any?(history, fn {_event_id, fields} ->
             fields["event"] == "signaled" and fields["signal"] == "payment_received"
           end)
  end

  test "signal can atomically attach values and guarded-transition state" do
    id = uid("signal-transition")

    assert :ok =
             FerricStore.flow_create(id,
               type: "signal-transition",
               partition_key: "tenant-a",
               state: "waiting_payment",
               run_at_ms: 10_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_signal(id,
               partition_key: "tenant-a",
               signal: "payment_received",
               if_state: "waiting_payment",
               transition_to: "verify_payment",
               values: %{"payment_event" => "payment-v1"},
               run_at_ms: 1_250,
               now_ms: 1_100
             )

    assert {:ok, fetched} =
             FerricStore.flow_get(id,
               partition_key: "tenant-a",
               values: ["payment_event"]
             )

    assert fetched.state == "verify_payment"
    assert fetched.next_run_at_ms == 1_250
    assert fetched.values == %{"payment_event" => "payment-v1"}

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("signal-transition",
               partition_key: "tenant-a",
               state: "verify_payment",
               worker: "worker-a",
               payload: false,
               values: ["payment_event"],
               now_ms: 1_250
             )

    assert claimed.id == id
    assert claimed.values == %{"payment_event" => "payment-v1"}
  end

  test "signal transition requires an if_state guard" do
    id = uid("signal-transition-guard-required")

    assert :ok =
             FerricStore.flow_create(id,
               type: "signal-transition-guard-required",
               partition_key: "tenant-a",
               state: "waiting_payment",
               run_at_ms: 10_000,
               now_ms: 1_000
             )

    assert {:error, "ERR flow signal transition requires if_state"} =
             FerricStore.flow_signal(id,
               partition_key: "tenant-a",
               signal: "payment_received",
               transition_to: "verify_payment",
               run_at_ms: 1_250,
               now_ms: 1_100
             )

    assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: "tenant-a")
    assert fetched.state == "waiting_payment"
    assert fetched.next_run_at_ms == 10_000
  end

  test "signal cannot transition a leased running flow without matching running guard" do
    id = uid("signal-running-transition-guard")

    assert :ok =
             FerricStore.flow_create(id,
               type: "signal-running-transition-guard",
               partition_key: "tenant-a",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("signal-running-transition-guard",
               partition_key: "tenant-a",
               worker: "worker-a",
               lease_ms: 10_000,
               payload: false,
               now_ms: 1_000
             )

    assert {:error, "ERR flow state mismatch"} =
             FerricStore.flow_signal(id,
               partition_key: "tenant-a",
               signal: "external_interrupt",
               if_state: "waiting",
               transition_to: "manual_review",
               run_at_ms: 1_200,
               now_ms: 1_100
             )

    assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: "tenant-a")
    assert fetched.state == "running"
    assert fetched.lease_token == claimed.lease_token
    assert fetched.fencing_token == claimed.fencing_token
  end

  test "signal cannot transition an already terminal flow" do
    id = uid("signal-terminal-source")

    assert :ok =
             FerricStore.flow_create(id,
               type: "signal-terminal-source",
               partition_key: "tenant-a",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("signal-terminal-source",
               partition_key: "tenant-a",
               worker: "worker-a",
               lease_ms: 10_000,
               payload: false,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-a",
               fencing_token: claimed.fencing_token,
               now_ms: 1_100
             )

    assert {:error, "ERR flow is terminal; use FLOW.REWIND"} =
             FerricStore.flow_signal(id,
               partition_key: "tenant-a",
               signal: "external_event",
               if_state: "completed",
               transition_to: "queued",
               now_ms: 1_200
             )

    assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: "tenant-a")
    assert fetched.state == "completed"
  end

  test "signal if_state guard rejects stale state without value writes" do
    id = uid("signal-guard")

    assert :ok =
             FerricStore.flow_create(id,
               type: "signal-guard",
               partition_key: "tenant-a",
               state: "waiting_inventory",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:error, "ERR flow state mismatch"} =
             FerricStore.flow_signal(id,
               partition_key: "tenant-a",
               signal: "payment_received",
               if_state: "waiting_payment",
               transition_to: "verify_payment",
               values: %{"payment_event" => "payment-v1"},
               now_ms: 1_100
             )

    assert {:ok, fetched} =
             FerricStore.flow_get(id,
               partition_key: "tenant-a",
               values: ["payment_event"]
             )

    assert fetched.state == "waiting_inventory"
    refute Map.has_key?(fetched, :values)
    refute Map.has_key?(Map.get(fetched, :value_refs, %{}), "payment_event")
  end

  test "signal idempotency key replays same digest and rejects conflicting digest" do
    id = uid("signal-idem")

    assert :ok =
             FerricStore.flow_create(id,
               type: "signal-idem",
               partition_key: "tenant-a",
               state: "waiting_payment",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    opts = [
      partition_key: "tenant-a",
      signal: "payment_received",
      idempotency_key: "stripe_evt_1",
      values: %{"payment_event" => "payment-v1"},
      now_ms: 1_100
    ]

    assert :ok = FerricStore.flow_signal(id, opts)
    assert :ok = FerricStore.flow_signal(id, opts)

    assert {:error, "ERR flow signal idempotency conflict"} =
             FerricStore.flow_signal(id,
               partition_key: "tenant-a",
               signal: "payment_received",
               idempotency_key: "stripe_evt_1",
               values: %{"payment_event" => "payment-v2"},
               override_values: ["payment_event"],
               now_ms: 1_200
             )

    assert {:ok, fetched} =
             FerricStore.flow_get(id,
               partition_key: "tenant-a",
               values: ["payment_event"]
             )

    assert fetched.values == %{"payment_event" => "payment-v1"}
  end

  test "signal idempotency key rejects conflicting transition semantics" do
    id = uid("signal-idem-transition")

    assert :ok =
             FerricStore.flow_create(id,
               type: "signal-idem-transition",
               partition_key: "tenant-a",
               state: "waiting_payment",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_signal(id,
               partition_key: "tenant-a",
               signal: "payment_received",
               idempotency_key: "stripe_evt_2",
               if_state: ["waiting_payment", "waiting_capture"],
               transition_to: "verify_payment",
               run_at_ms: 1_200,
               now_ms: 1_100
             )

    assert {:error, "ERR flow signal idempotency conflict"} =
             FerricStore.flow_signal(id,
               partition_key: "tenant-a",
               signal: "payment_received",
               idempotency_key: "stripe_evt_2",
               if_state: ["waiting_payment", "waiting_capture"],
               transition_to: "manual_review",
               run_at_ms: 1_200,
               now_ms: 1_100
             )

    assert {:error, "ERR flow signal idempotency conflict"} =
             FerricStore.flow_signal(id,
               partition_key: "tenant-a",
               signal: "payment_received",
               idempotency_key: "stripe_evt_2",
               if_state: ["waiting_payment", "waiting_capture"],
               transition_to: "verify_payment",
               run_at_ms: 1_300,
               now_ms: 1_100
             )
  end

  test "passive signal preserves active lease and fencing token" do
    id = uid("signal-lease")

    assert :ok =
             FerricStore.flow_create(id,
               type: "signal-lease",
               partition_key: "tenant-a",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("signal-lease",
               partition_key: "tenant-a",
               worker: "worker-a",
               lease_ms: 10_000,
               payload: false,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_signal(id,
               partition_key: "tenant-a",
               signal: "external_note",
               values: %{"note" => "ok"},
               now_ms: 1_100
             )

    assert {:ok, fetched} =
             FerricStore.flow_get(id,
               partition_key: "tenant-a",
               values: ["note"]
             )

    assert fetched.state == "running"
    assert fetched.lease_owner == "worker-a"
    assert fetched.lease_token == claimed.lease_token
    assert fetched.fencing_token == claimed.fencing_token
    assert fetched.lease_deadline_ms == 11_000
    assert fetched.values == %{"note" => "ok"}

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-a",
               fencing_token: claimed.fencing_token,
               now_ms: 1_200
             )
  end

  test "retention cleanup removes signal-owned named value blobs" do
    id = uid("signal-retention")

    assert :ok =
             FerricStore.flow_create(id,
               type: "signal-retention",
               partition_key: "tenant-retention",
               retention_ttl_ms: 100,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_signal(id,
               partition_key: "tenant-retention",
               signal: "artifact_ready",
               values: %{"artifact" => "artifact-bytes"},
               now_ms: 1_050
             )

    assert {:ok, signaled} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    artifact_ref = get_in(signaled.value_refs, ["artifact", :ref])
    assert is_binary(artifact_ref)
    assert {:ok, _blob} = FerricStore.get(artifact_ref)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("signal-retention",
               partition_key: "tenant-retention",
               worker: "worker-retention",
               limit: 1,
               now_ms: 1_000,
               payload: false
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: "tenant-retention",
               fencing_token: claimed.fencing_token,
               now_ms: 1_100
             )

    Process.sleep(150)

    assert {:ok, cleaned} = FerricStore.flow_retention_cleanup(limit: 10)
    assert cleaned.values >= 1

    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: "tenant-retention")
    assert {:ok, nil} = FerricStore.get(artifact_ref)
  end
end
