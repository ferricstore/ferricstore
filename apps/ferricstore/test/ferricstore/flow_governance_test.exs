defmodule Ferricstore.FlowGovernanceTest do
  use Ferricstore.Test.FlowCase

  @partition "tenant-governance"

  test "flow policy stores governance rules and exposes them" do
    type = unique_flow_id("gov-policy-type")

    assert {:ok, policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 mode: "ledger",
                 effects: %{
                   allowed: ["email.send"],
                   denied: ["stripe.refund"],
                   require_idempotency_key: true
                 },
                 audit: %{effect_events: true, denials: "all"}
               }
             )

    assert policy.governance.mode == :ledger
    assert policy.governance.effects.allowed == ["email.send"]
    assert policy.governance.effects.denied == ["stripe.refund"]
    assert policy.governance.audit.denials == :all

    assert {:ok, fetched} = FerricStore.flow_policy_get(type)
    assert fetched.governance == policy.governance
  end

  test "flow policy version is returned and carried by governance decisions" do
    type = unique_flow_id("gov-policy-version-type")
    id = unique_flow_id("gov-policy-version")

    assert {:ok, policy} =
             FerricStore.flow_policy_set(type,
               version: "policy-v1",
               governance: %{effects: %{denied: ["stripe.refund"]}}
             )

    assert policy.version == "policy-v1"

    claimed = create_and_claim!(id, type)

    assert {:error, denial} =
             FerricStore.flow_effect_reserve(id, "refund", "stripe.refund",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "refund-digest",
               idempotency_key: "refund-1",
               now_ms: 1_002
             )

    assert denial.policy_version == "policy-v1"
    assert denial.policy_hash

    assert {:ok, [event]} = FerricStore.flow_governance_ledger(id, partition_key: @partition)
    assert event.policy_version == "policy-v1"
    assert event.policy_hash == denial.policy_hash
  end

  test "effect reserve is idempotent for same digest and conflicts for different digest" do
    type = unique_flow_id("gov-effect-type")
    id = unique_flow_id("gov-effect")

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 effects: %{allowed: ["email.send"], require_idempotency_key: true}
               }
             )

    claimed = create_and_claim!(id, type)

    opts = [
      partition_key: @partition,
      lease_token: claimed.lease_token,
      fencing_token: claimed.fencing_token,
      operation_digest: "digest-1",
      idempotency_key: "email-send-1",
      now_ms: 1_002
    ]

    assert {:ok, reserved} =
             FerricStore.flow_effect_reserve(id, "send-email", "email.send", opts)

    assert reserved.status == :reserved
    assert reserved.decision == :reserved
    assert reserved.policy_hash

    assert {:ok, replay} =
             FerricStore.flow_effect_reserve(id, "send-email", "email.send", opts)

    assert replay.status == :reserved
    assert replay.decision == :already_reserved

    assert {:error, denial} =
             FerricStore.flow_effect_reserve(
               id,
               "send-email",
               "email.send",
               Keyword.put(opts, :operation_digest, "digest-2")
             )

    assert denial.code == "GOVERNANCE_CONFLICT"
    assert denial.policy == "effect_idempotency"
  end

  test "effect confirm updates the durable effect record" do
    type = unique_flow_id("gov-effect-confirm-type")
    id = unique_flow_id("gov-effect-confirm")

    claimed = create_and_claim!(id, type)

    reserve_opts = [
      partition_key: @partition,
      lease_token: claimed.lease_token,
      fencing_token: claimed.fencing_token,
      operation_digest: "digest-confirm",
      idempotency_key: "confirm-1",
      now_ms: 1_002
    ]

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(id, "charge", "stripe.charge", reserve_opts)

    assert {:ok, confirmed} =
             FerricStore.flow_effect_confirm(id, "charge",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               external_id: "ch_123",
               now_ms: 1_003
             )

    assert confirmed.status == :confirmed
    assert confirmed.external_id == "ch_123"

    assert {:ok, fetched} = FerricStore.flow_effect_get(id, "charge", partition_key: @partition)
    assert fetched.status == :confirmed
    assert fetched.external_id == "ch_123"

    assert {:ok, ledger} = FerricStore.flow_governance_ledger(id, partition_key: @partition)
    assert Enum.map(ledger, & &1.kind) == [:effect_reserved, :effect_confirmed]
    assert Enum.map(ledger, & &1.effect_key) == ["charge", "charge"]

    assert {:ok, [latest]} =
             FerricStore.flow_governance_ledger(id,
               partition_key: @partition,
               rev: true,
               limit: 1,
               from_ms: 1_003
             )

    assert latest.kind == :effect_confirmed
  end

  test "policy denial returns structured governance error" do
    type = unique_flow_id("gov-effect-denied-type")
    id = unique_flow_id("gov-effect-denied")

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{effects: %{denied: ["stripe.refund"]}}
             )

    claimed = create_and_claim!(id, type)

    assert {:error, denial} =
             FerricStore.flow_effect_reserve(id, "refund", "stripe.refund",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "refund-digest",
               idempotency_key: "refund-1",
               now_ms: 1_002
             )

    assert denial.code == "GOVERNANCE_EFFECT_DENIED"
    assert denial.reason == "effect_denied"
    assert denial.scope == @partition
    assert denial.type == type
    assert denial.state == "running"
    assert denial.effect_type == "stripe.refund"
    assert denial.decision_id

    assert {:ok, [event]} = FerricStore.flow_governance_ledger(id, partition_key: @partition)
    assert event.kind == :effect_denied
    assert event.code == "GOVERNANCE_EFFECT_DENIED"
  end

  test "approval policy returns structured approval-required denial" do
    type = unique_flow_id("gov-effect-approval-type")
    id = unique_flow_id("gov-effect-approval")

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{approvals: %{effects: ["stripe.refund"]}}
             )

    claimed = create_and_claim!(id, type)

    assert {:error, denial} =
             FerricStore.flow_effect_reserve(id, "refund", "stripe.refund",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "refund-digest",
               idempotency_key: "refund-1",
               now_ms: 1_002
             )

    assert denial.code == "GOVERNANCE_APPROVAL_REQUIRED"
    assert denial.scope == @partition
    assert denial.effect_type == "stripe.refund"

    assert {:ok, [event]} = FerricStore.flow_governance_ledger(id, partition_key: @partition)
    assert event.kind == :approval_required
    assert event.code == "GOVERNANCE_APPROVAL_REQUIRED"
  end

  test "approval requests are durable and accept one terminal decision" do
    id = unique_flow_id("gov-approval")

    assert {:ok, approval} =
             FerricStore.flow_approval_request(id,
               flow_id: "flow-1",
               scope: @partition,
               reason: "refund approval",
               requested_by: "worker-1",
               now_ms: 1_000
             )

    assert approval.status == :pending

    assert {:error, conflict} =
             FerricStore.flow_approval_request(id,
               flow_id: "flow-1",
               scope: @partition,
               now_ms: 1_001
             )

    assert conflict.code == "GOVERNANCE_CONFLICT"

    assert {:ok, approved} =
             FerricStore.flow_approval_approve(id,
               approver: "admin",
               reason: "looks good",
               now_ms: 1_002
             )

    assert approved.status == :approved
    assert approved.decided_by == "admin"

    assert {:ok, fetched} = FerricStore.flow_approval_get(id)
    assert fetched.status == :approved

    assert {:error, terminal_conflict} =
             FerricStore.flow_approval_reject(id, approver: "admin-2", now_ms: 1_003)

    assert terminal_conflict.code == "GOVERNANCE_CONFLICT"
  end

  test "approval request validates optional metadata" do
    id = unique_flow_id("gov-approval-validation")

    assert {:error, "ERR flow approval assignees must be a list of non-empty strings"} =
             FerricStore.flow_approval_request(id,
               flow_id: "flow-1",
               scope: @partition,
               assignees: "finance",
               now_ms: 1_000
             )

    assert {:error, "ERR flow approval timeout_ms must be a positive integer"} =
             FerricStore.flow_approval_request(id,
               flow_id: "flow-1",
               scope: @partition,
               timeout_ms: -1,
               now_ms: 1_000
             )

    assert {:error, "ERR flow approval expires_at_ms must be a non-negative integer"} =
             FerricStore.flow_approval_request(id,
               flow_id: "flow-1",
               scope: @partition,
               expires_at_ms: -1,
               now_ms: 1_000
             )
  end

  test "approval list filters pending requests and governance overview summarizes records" do
    approval_id = unique_flow_id("gov-approval-list")
    budget_scope = unique_flow_id("gov-overview-budget")
    limit_scope = unique_flow_id("gov-overview-limit")

    assert {:ok, approval} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: "flow-list-1",
               scope: @partition,
               reason: "manual approval",
               requested_by: "worker-1",
               assignees: ["finance", "ops"],
               policy_hash: "hash-1",
               policy_version: "policy-v2",
               timeout_ms: 10_000,
               now_ms: 1_000
             )

    assert approval.status == :pending
    assert approval.assignees == ["finance", "ops"]
    assert approval.policy_version == "policy-v2"
    assert approval.expires_at_ms == 11_000

    assert {:ok, _budget} =
             FerricStore.flow_budget_reserve(budget_scope, 1,
               limit: 10,
               window_ms: 60_000,
               now_ms: 1_000
             )

    assert {:ok, _limit} =
             FerricStore.flow_limit_lease(limit_scope,
               shard_id: 0,
               amount: 1,
               limit: 2,
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    assert {:ok, approvals} = FerricStore.flow_approval_list(status: :pending, scope: @partition)
    assert Enum.any?(approvals, &(&1.id == approval_id))

    assert {:ok, overview} = FerricStore.flow_governance_overview(limit: 200)
    assert overview.counts.pending_approvals >= 1
    assert overview.counts.budgets >= 1
    assert overview.counts.limits >= 1
    assert Enum.any?(overview.approvals, &(&1.id == approval_id))
    assert Enum.any?(overview.budgets, &(&1.scope == budget_scope))
    assert Enum.any?(overview.limits, &(&1.scope == limit_scope))
  end

  test "governance overview and list APIs honor partition scope filters" do
    visible_partition = unique_flow_id("gov-visible-partition")
    hidden_partition = unique_flow_id("gov-hidden-partition")
    visible_prefixed_scope = "partition:" <> visible_partition

    assert {:ok, _approval} =
             FerricStore.flow_approval_request(unique_flow_id("gov-visible-approval"),
               flow_id: "flow-visible",
               scope: visible_prefixed_scope,
               now_ms: 1_000
             )

    assert {:ok, _approval} =
             FerricStore.flow_approval_request(unique_flow_id("gov-hidden-approval"),
               flow_id: "flow-hidden",
               scope: "partition:" <> hidden_partition,
               now_ms: 1_000
             )

    assert {:ok, _budget} =
             FerricStore.flow_budget_reserve(visible_prefixed_scope, 1,
               limit: 10,
               window_ms: 60_000,
               now_ms: 1_000
             )

    assert {:ok, _budget} =
             FerricStore.flow_budget_reserve("partition:" <> hidden_partition, 1,
               limit: 10,
               window_ms: 60_000,
               now_ms: 1_000
             )

    assert {:ok, _limit} =
             FerricStore.flow_limit_lease(visible_prefixed_scope,
               shard_id: 0,
               amount: 1,
               limit: 2,
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    assert {:ok, _limit} =
             FerricStore.flow_limit_lease("partition:" <> hidden_partition,
               shard_id: 0,
               amount: 1,
               limit: 2,
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    assert {:ok, approvals} = FerricStore.flow_approval_list(partition_key: visible_partition)
    assert Enum.all?(approvals, &(&1.scope in [visible_partition, visible_prefixed_scope]))

    assert {:ok, overview} =
             FerricStore.flow_governance_overview(partition_key: visible_partition, limit: 200)

    assert Enum.all?(
             overview.approvals,
             &(&1.scope in [visible_partition, visible_prefixed_scope])
           )

    assert Enum.all?(overview.budgets, &(&1.scope in [visible_partition, visible_prefixed_scope]))
    assert Enum.all?(overview.limits, &(&1.scope in [visible_partition, visible_prefixed_scope]))
  end

  test "open circuit blocks governed effect reserve until closed" do
    type = unique_flow_id("gov-circuit-type")
    id = unique_flow_id("gov-circuit")

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 circuits: %{"effect:email.send" => %{failure_threshold: 1, open_ms: 30_000}}
               }
             )

    claimed = create_and_claim!(id, type)

    assert {:ok, open} =
             FerricStore.flow_circuit_open("effect:email.send", now_ms: 1_001, open_ms: 30_000)

    assert open.status == :open

    opts = [
      partition_key: @partition,
      lease_token: claimed.lease_token,
      fencing_token: claimed.fencing_token,
      operation_digest: "email-digest",
      idempotency_key: "email-1",
      now_ms: 1_002
    ]

    assert {:error, denial} =
             FerricStore.flow_effect_reserve(id, "send-email", "email.send", opts)

    assert denial.code == "GOVERNANCE_CIRCUIT_OPEN"

    assert {:ok, [event]} = FerricStore.flow_governance_ledger(id, partition_key: @partition)
    assert event.kind == :circuit_open
    assert event.code == "GOVERNANCE_CIRCUIT_OPEN"

    assert {:ok, closed} = FerricStore.flow_circuit_close("effect:email.send", now_ms: 1_003)
    assert closed.status == :closed

    assert {:ok, reserved} =
             FerricStore.flow_effect_reserve(id, "send-email", "email.send", opts)

    assert reserved.status == :reserved
  end

  test "circuit list filters by scope and status for dashboard use" do
    open_scope = "effect:" <> unique_flow_id("gov-circuit-list-open")
    closed_scope = "effect:" <> unique_flow_id("gov-circuit-list-closed")

    assert {:ok, open} = FerricStore.flow_circuit_open(open_scope, now_ms: 1_001)
    assert {:ok, _} = FerricStore.flow_circuit_open(closed_scope, now_ms: 1_001)
    assert {:ok, closed} = FerricStore.flow_circuit_close(closed_scope, now_ms: 1_002)

    assert open.status == :open
    assert closed.status == :closed

    assert {:ok, open_circuits} =
             FerricStore.flow_circuit_list(scope: open_scope, circuit_status: :open)

    assert Enum.map(open_circuits, & &1.scope) == [open_scope]

    assert {:ok, closed_circuits} =
             FerricStore.flow_circuit_list(scope: closed_scope, circuit_status: "closed")

    assert Enum.map(closed_circuits, & &1.status) == [:closed]
  end

  test "effect failures open governed circuit and later success resets it" do
    type = unique_flow_id("gov-circuit-auto-type")
    effect_type = unique_flow_id("email-send")
    scope = "effect:" <> effect_type

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 circuits: %{scope => %{failure_threshold: 2, open_ms: 1_000}}
               }
             )

    claimed1 = create_and_claim!(unique_flow_id("gov-circuit-auto-1"), type)

    reserve_opts1 = [
      partition_key: @partition,
      lease_token: claimed1.lease_token,
      fencing_token: claimed1.fencing_token,
      operation_digest: "email-digest-1",
      idempotency_key: "email-1",
      now_ms: 1_000
    ]

    assert {:ok, reserved} =
             FerricStore.flow_effect_reserve(
               claimed1.id,
               "send-email",
               effect_type,
               reserve_opts1
             )

    assert reserved.status == :reserved

    fail_opts1 = [
      partition_key: @partition,
      lease_token: claimed1.lease_token,
      fencing_token: claimed1.fencing_token,
      error: "smtp down",
      now_ms: 1_001
    ]

    assert {:ok, failed} = FerricStore.flow_effect_fail(claimed1.id, "send-email", fail_opts1)
    assert failed.status == :failed

    assert {:ok, circuit} = FerricStore.flow_circuit_get(scope)
    assert circuit.status == :closed
    assert circuit.failures == 1

    assert {:ok, already_failed} =
             FerricStore.flow_effect_fail(
               claimed1.id,
               "send-email",
               Keyword.put(fail_opts1, :now_ms, 1_002)
             )

    assert already_failed.decision == :already_applied
    assert {:ok, circuit} = FerricStore.flow_circuit_get(scope)
    assert circuit.failures == 1

    claimed2 = create_and_claim!(unique_flow_id("gov-circuit-auto-2"), type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(claimed2.id, "send-email", effect_type,
               partition_key: @partition,
               lease_token: claimed2.lease_token,
               fencing_token: claimed2.fencing_token,
               operation_digest: "email-digest-2",
               idempotency_key: "email-2",
               now_ms: 1_100
             )

    assert {:ok, _failed} =
             FerricStore.flow_effect_fail(claimed2.id, "send-email",
               partition_key: @partition,
               lease_token: claimed2.lease_token,
               fencing_token: claimed2.fencing_token,
               error: "smtp still down",
               now_ms: 1_101
             )

    assert {:ok, circuit} = FerricStore.flow_circuit_get(scope)
    assert circuit.status == :open
    assert circuit.failures == 2
    assert circuit.opened_at_ms == 1_101

    claimed3 = create_and_claim!(unique_flow_id("gov-circuit-auto-3"), type)

    assert {:error, denial} =
             FerricStore.flow_effect_reserve(claimed3.id, "send-email", effect_type,
               partition_key: @partition,
               lease_token: claimed3.lease_token,
               fencing_token: claimed3.fencing_token,
               operation_digest: "email-digest-3",
               idempotency_key: "email-3",
               now_ms: 1_200
             )

    assert denial.code == "GOVERNANCE_CIRCUIT_OPEN"
    assert denial.retry_after_ms == 901

    assert {:ok, reserved} =
             FerricStore.flow_effect_reserve(claimed3.id, "send-email", effect_type,
               partition_key: @partition,
               lease_token: claimed3.lease_token,
               fencing_token: claimed3.fencing_token,
               operation_digest: "email-digest-3",
               idempotency_key: "email-3",
               now_ms: 2_200
             )

    assert reserved.status == :reserved
    assert {:ok, circuit} = FerricStore.flow_circuit_get(scope)
    assert circuit.status == :half_open

    claimed4 = create_and_claim!(unique_flow_id("gov-circuit-auto-4"), type)

    assert {:error, denial} =
             FerricStore.flow_effect_reserve(claimed4.id, "send-email", effect_type,
               partition_key: @partition,
               lease_token: claimed4.lease_token,
               fencing_token: claimed4.fencing_token,
               operation_digest: "email-digest-4",
               idempotency_key: "email-4",
               now_ms: 2_201
             )

    assert denial.code == "GOVERNANCE_CIRCUIT_OPEN"
    assert denial.status == :half_open

    assert {:ok, confirmed} =
             FerricStore.flow_effect_confirm(claimed3.id, "send-email",
               partition_key: @partition,
               lease_token: claimed3.lease_token,
               fencing_token: claimed3.fencing_token,
               external_id: "mail-3",
               now_ms: 2_201
             )

    assert confirmed.status == :confirmed
    assert {:ok, circuit} = FerricStore.flow_circuit_get(scope)
    assert circuit.status == :closed
    assert circuit.failures == 0
    assert circuit.opened_at_ms == nil
  end

  test "default circuit policy does not write circuit state on clean successful effect" do
    type = unique_flow_id("gov-circuit-clean-type")
    effect_type = unique_flow_id("clean-effect")
    scope = "effect:" <> effect_type

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 circuits: %{
                   scope => %{
                     failure_threshold: 2,
                     open_ms: 1_000
                   }
                 }
               }
             )

    claimed = create_and_claim!(unique_flow_id("gov-circuit-clean-success"), type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(claimed.id, "clean", effect_type,
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "clean-success",
               idempotency_key: "clean-success",
               now_ms: 1_000
             )

    assert {:ok, _confirmed} =
             FerricStore.flow_effect_confirm(claimed.id, "clean",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 1_001
             )

    assert {:ok, nil} = FerricStore.flow_circuit_get(scope)
  end

  test "latency circuit does not write circuit state for fast clean success" do
    type = unique_flow_id("gov-circuit-fast-type")
    effect_type = unique_flow_id("fast-effect")
    scope = "effect:" <> effect_type

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 circuits: %{
                   scope => %{
                     failure_threshold: 1,
                     open_ms: 1_000,
                     latency_threshold_ms: 2_000
                   }
                 }
               }
             )

    claimed = create_and_claim!(unique_flow_id("gov-circuit-fast-success"), type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(claimed.id, "fast", effect_type,
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "fast-success",
               idempotency_key: "fast-success",
               now_ms: 1_000
             )

    assert {:ok, _confirmed} =
             FerricStore.flow_effect_confirm(claimed.id, "fast",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               latency_ms: 25,
               now_ms: 1_001
             )

    assert {:ok, nil} = FerricStore.flow_circuit_get(scope)
  end

  test "latency circuit opens when the first recorded effect is a slow success" do
    type = unique_flow_id("gov-circuit-first-slow-type")
    effect_type = unique_flow_id("first-slow-effect")
    scope = "effect:" <> effect_type

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 circuits: %{
                   scope => %{
                     failure_threshold: 1,
                     open_ms: 1_000,
                     latency_threshold_ms: 2_000
                   }
                 }
               }
             )

    claimed = create_and_claim!(unique_flow_id("gov-circuit-first-slow"), type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(claimed.id, "slow", effect_type,
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "slow-success",
               idempotency_key: "slow-success",
               now_ms: 1_000
             )

    assert {:ok, _confirmed} =
             FerricStore.flow_effect_confirm(claimed.id, "slow",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               latency_ms: 2_500,
               now_ms: 1_001
             )

    assert {:ok, circuit} = FerricStore.flow_circuit_get(scope)
    assert circuit.status == :open
    assert circuit.failure_count == 1
    assert Enum.any?(circuit.events, &(&1.kind == :slow_call))
  end

  test "failure-rate circuit records first clean successes for exact rate math" do
    type = unique_flow_id("gov-circuit-first-rate-type")
    effect_type = unique_flow_id("first-rate-effect")
    scope = "effect:" <> effect_type

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 circuits: %{
                   scope => %{
                     failure_threshold: 100,
                     open_ms: 1_000,
                     window_ms: 60_000,
                     min_calls: 4,
                     failure_rate_pct: 50
                   }
                 }
               }
             )

    for i <- 1..2 do
      claimed = create_and_claim!(unique_flow_id("gov-circuit-first-rate-#{i}"), type)

      assert {:ok, _reserved} =
               FerricStore.flow_effect_reserve(claimed.id, "rate", effect_type,
                 partition_key: @partition,
                 lease_token: claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 operation_digest: "rate-success-#{i}",
                 idempotency_key: "rate-success-#{i}",
                 now_ms: 1_000 + i
               )

      assert {:ok, _confirmed} =
               FerricStore.flow_effect_confirm(claimed.id, "rate",
                 partition_key: @partition,
                 lease_token: claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 now_ms: 1_010 + i
               )
    end

    assert {:ok, circuit} = FerricStore.flow_circuit_get(scope)
    assert circuit.status == :closed
    assert Enum.count(circuit.events, &(&1.kind == :success)) == 2
  end

  test "manual circuit open rejects impossible tracking windows" do
    scope = unique_flow_id("gov-circuit-invalid")

    assert {:error, reason} =
             FerricStore.flow_circuit_open(scope,
               failure_threshold: 65,
               open_ms: 30_000,
               now_ms: 1_000
             )

    assert reason =~ "failure_threshold"
    assert reason =~ "64"
  end

  test "advanced circuit policy opens on latency and filtered failure rate" do
    type = unique_flow_id("gov-circuit-v2-type")
    effect_type = unique_flow_id("llm-call")
    scope = "effect:" <> effect_type

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 circuits: %{
                   scope => %{
                     failure_threshold: 100,
                     open_ms: 1_000,
                     window_ms: 60_000,
                     min_calls: 4,
                     failure_rate_pct: 50,
                     latency_threshold_ms: 2_000,
                     error_classes: ["TimeoutError"],
                     half_open_max_probes: 2,
                     half_open_success_threshold: 2
                   }
                 }
               }
             )

    ignored = create_and_claim!(unique_flow_id("gov-circuit-v2-ignored"), type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(ignored.id, "llm", effect_type,
               partition_key: @partition,
               lease_token: ignored.lease_token,
               fencing_token: ignored.fencing_token,
               operation_digest: "ignored",
               idempotency_key: "ignored",
               now_ms: 1_000
             )

    assert {:ok, _failed} =
             FerricStore.flow_effect_fail(ignored.id, "llm",
               partition_key: @partition,
               lease_token: ignored.lease_token,
               fencing_token: ignored.fencing_token,
               error: "bad input",
               reason: "ValidationError",
               now_ms: 1_001
             )

    assert {:ok, circuit} = FerricStore.flow_circuit_get(scope)
    assert circuit.status == :closed
    assert circuit.failures == 0

    slow = create_and_claim!(unique_flow_id("gov-circuit-v2-slow"), type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(slow.id, "llm", effect_type,
               partition_key: @partition,
               lease_token: slow.lease_token,
               fencing_token: slow.fencing_token,
               operation_digest: "slow",
               idempotency_key: "slow",
               now_ms: 1_002
             )

    assert {:ok, _confirmed} =
             FerricStore.flow_effect_confirm(slow.id, "llm",
               partition_key: @partition,
               lease_token: slow.lease_token,
               fencing_token: slow.fencing_token,
               latency_ms: 2_500,
               now_ms: 1_003
             )

    success1 = create_and_claim!(unique_flow_id("gov-circuit-v2-success-1"), type)
    success2 = create_and_claim!(unique_flow_id("gov-circuit-v2-success-2"), type)
    timeout = create_and_claim!(unique_flow_id("gov-circuit-v2-timeout"), type)

    for {claimed, digest, now_ms} <- [{success1, "s1", 1_004}, {success2, "s2", 1_006}] do
      assert {:ok, _reserved} =
               FerricStore.flow_effect_reserve(claimed.id, "llm", effect_type,
                 partition_key: @partition,
                 lease_token: claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 operation_digest: digest,
                 idempotency_key: digest,
                 now_ms: now_ms
               )

      assert {:ok, _confirmed} =
               FerricStore.flow_effect_confirm(claimed.id, "llm",
                 partition_key: @partition,
                 lease_token: claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 now_ms: now_ms + 1
               )
    end

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(timeout.id, "llm", effect_type,
               partition_key: @partition,
               lease_token: timeout.lease_token,
               fencing_token: timeout.fencing_token,
               operation_digest: "timeout",
               idempotency_key: "timeout",
               now_ms: 1_008
             )

    assert {:ok, _failed} =
             FerricStore.flow_effect_fail(timeout.id, "llm",
               partition_key: @partition,
               lease_token: timeout.lease_token,
               fencing_token: timeout.fencing_token,
               error: "upstream timeout",
               reason: "TimeoutError",
               now_ms: 1_009
             )

    assert {:ok, circuit} = FerricStore.flow_circuit_get(scope)
    assert circuit.status == :open
    assert circuit.failure_count == 2
    assert Enum.any?(circuit.events, &(&1.kind == :slow_call))
    assert Enum.any?(circuit.events, &(&1.kind == :ignored_failure))
  end

  test "half-open circuit allows configured probe budget before closing" do
    type = unique_flow_id("gov-circuit-probe-type")
    effect_type = unique_flow_id("smtp-send")
    scope = "effect:" <> effect_type

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 circuits: %{
                   scope => %{
                     failure_threshold: 1,
                     open_ms: 10,
                     half_open_max_probes: 2,
                     half_open_success_threshold: 2
                   }
                 }
               }
             )

    failed = create_and_claim!(unique_flow_id("gov-circuit-probe-failed"), type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(failed.id, "send", effect_type,
               partition_key: @partition,
               lease_token: failed.lease_token,
               fencing_token: failed.fencing_token,
               operation_digest: "failed",
               idempotency_key: "failed",
               now_ms: 1_000
             )

    assert {:ok, _failed} =
             FerricStore.flow_effect_fail(failed.id, "send",
               partition_key: @partition,
               lease_token: failed.lease_token,
               fencing_token: failed.fencing_token,
               error: "smtp down",
               reason: "TimeoutError",
               now_ms: 1_001
             )

    probe1 = create_and_claim!(unique_flow_id("gov-circuit-probe-1"), type)
    probe2 = create_and_claim!(unique_flow_id("gov-circuit-probe-2"), type)
    probe3 = create_and_claim!(unique_flow_id("gov-circuit-probe-3"), type)

    for {claimed, digest, now_ms} <- [{probe1, "p1", 1_012}, {probe2, "p2", 1_013}] do
      assert {:ok, reserved} =
               FerricStore.flow_effect_reserve(claimed.id, "send", effect_type,
                 partition_key: @partition,
                 lease_token: claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 operation_digest: digest,
                 idempotency_key: digest,
                 now_ms: now_ms
               )

      assert reserved.status == :reserved
    end

    assert {:error, denial} =
             FerricStore.flow_effect_reserve(probe3.id, "send", effect_type,
               partition_key: @partition,
               lease_token: probe3.lease_token,
               fencing_token: probe3.fencing_token,
               operation_digest: "p3",
               idempotency_key: "p3",
               now_ms: 1_014
             )

    assert denial.code == "GOVERNANCE_CIRCUIT_OPEN"

    for {claimed, now_ms} <- [{probe1, 1_020}, {probe2, 1_021}] do
      assert {:ok, _confirmed} =
               FerricStore.flow_effect_confirm(claimed.id, "send",
                 partition_key: @partition,
                 lease_token: claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 now_ms: now_ms
               )
    end

    assert {:ok, circuit} = FerricStore.flow_circuit_get(scope)
    assert circuit.status == :closed
    assert circuit.half_open_successes == 0
  end

  test "budget reserve enforces fixed window and resets after window" do
    scope = unique_flow_id("gov-budget")

    assert {:ok, budget} =
             FerricStore.flow_budget_reserve(scope, 40,
               limit: 100,
               window_ms: 60_000,
               now_ms: 1_000
             )

    assert budget.used == 40

    assert {:ok, budget} = FerricStore.flow_budget_reserve(scope, 60, now_ms: 2_000)
    assert budget.used == 100

    assert {:error, denial} = FerricStore.flow_budget_reserve(scope, 1, now_ms: 3_000)
    assert denial.code == "GOVERNANCE_BUDGET_EXHAUSTED"
    assert denial.reason == "budget_exhausted"

    assert {:ok, budget} = FerricStore.flow_budget_reserve(scope, 1, now_ms: 61_000)
    assert budget.used == 1

    assert {:ok, fetched} = FerricStore.flow_budget_get(scope)
    assert fetched.used == 1
  end

  test "budget commit and release settle reservations with actual usage" do
    scope = unique_flow_id("gov-budget-settle")

    assert {:ok, reserved} =
             FerricStore.flow_budget_reserve(scope, 80,
               limit: 100,
               window_ms: 60_000,
               reservation_id: "llm-step",
               now_ms: 1_000
             )

    assert reserved.used == 80
    assert reserved.reservation_id == "llm-step"
    assert reserved.status == :reserved

    assert {:ok, committed} =
             FerricStore.flow_budget_commit(scope, "llm-step", 55,
               usage: %{model: "gpt", tokens: 55},
               now_ms: 2_000
             )

    assert committed.used == 55
    assert committed.actual_amount == 55
    assert committed.status == :committed
    assert committed.usage == %{model: "gpt", tokens: 55}

    assert {:ok, released} =
             FerricStore.flow_budget_reserve(scope, 30,
               reservation_id: "unused-step",
               now_ms: 2_100
             )

    assert released.used == 85

    assert {:ok, released} =
             FerricStore.flow_budget_release(scope, "unused-step", now_ms: 2_200)

    assert released.used == 55
    assert released.status == :released
    assert released.actual_amount == 0
  end

  test "governance telemetry emits stable budget and approval metadata" do
    parent = self()
    handler_id = :"flow-governance-telemetry-#{System.unique_integer([:positive])}"

    events = [
      [:ferricstore, :flow, :governance, :budget_reserve],
      [:ferricstore, :flow, :governance, :budget_commit],
      [:ferricstore, :flow, :governance, :budget_release],
      [:ferricstore, :flow, :governance, :approval_approve],
      [:ferricstore, :flow, :governance, :circuit_open],
      [:ferricstore, :flow, :governance, :circuit_close]
    ]

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, measurements, metadata, _config ->
          send(parent, {:governance_telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    scope = unique_flow_id("gov-telemetry-budget")

    assert {:ok, _reserved} =
             FerricStore.flow_budget_reserve(scope, 10,
               limit: 100,
               window_ms: 60_000,
               reservation_id: "llm-step",
               now_ms: 1_000
             )

    assert_receive {:governance_telemetry, [:ferricstore, :flow, :governance, :budget_reserve],
                    %{count: 1}, reserve_metadata}

    assert reserve_metadata.status == :ok
    assert reserve_metadata.scope == scope
    assert reserve_metadata.amount == 10

    assert {:ok, _committed} =
             FerricStore.flow_budget_commit(scope, "llm-step", 7, now_ms: 1_100)

    assert_receive {:governance_telemetry, [:ferricstore, :flow, :governance, :budget_commit],
                    %{count: 1}, commit_metadata}

    assert commit_metadata.status == :ok
    assert commit_metadata.scope == scope
    assert commit_metadata.reservation_id == "llm-step"
    assert commit_metadata.actual_amount == 7

    assert {:ok, _reserved} =
             FerricStore.flow_budget_reserve(scope, 5,
               reservation_id: "unused-step",
               now_ms: 1_200
             )

    assert_receive {:governance_telemetry, [:ferricstore, :flow, :governance, :budget_reserve],
                    %{count: 1}, _reserve_metadata}

    assert {:ok, _released} =
             FerricStore.flow_budget_release(scope, "unused-step", now_ms: 1_300)

    assert_receive {:governance_telemetry, [:ferricstore, :flow, :governance, :budget_release],
                    %{count: 1}, release_metadata}

    assert release_metadata.status == :ok
    assert release_metadata.scope == scope
    assert release_metadata.reservation_id == "unused-step"

    approval_id = unique_flow_id("gov-telemetry-approval")

    assert {:ok, _approval} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: "flow-telemetry-1",
               scope: @partition,
               reason: "manual review",
               now_ms: 2_000
             )

    assert {:ok, _approval} =
             FerricStore.flow_approval_approve(approval_id,
               approver: "ops",
               reason: "ok",
               now_ms: 2_500
             )

    assert_receive {:governance_telemetry, [:ferricstore, :flow, :governance, :approval_approve],
                    %{count: 1}, approval_metadata}

    assert approval_metadata.status == :ok
    assert approval_metadata.approval_id == approval_id
    assert approval_metadata.flow_id == "flow-telemetry-1"
    assert approval_metadata.scope == @partition
    assert approval_metadata.wait_ms == 500

    circuit_scope = unique_flow_id("gov-telemetry-circuit")

    assert {:ok, _open} =
             FerricStore.flow_circuit_open(circuit_scope,
               open_ms: 30_000,
               failure_threshold: 5,
               window_ms: 60_000,
               failure_rate_pct: 50,
               latency_threshold_ms: 2_000,
               now_ms: 3_000
             )

    assert_receive {:governance_telemetry, [:ferricstore, :flow, :governance, :circuit_open],
                    %{count: 1}, circuit_open_metadata}

    assert circuit_open_metadata.status == :ok
    assert circuit_open_metadata.scope == circuit_scope
    assert circuit_open_metadata.circuit_status == :open
    assert circuit_open_metadata.failure_threshold == 5
    assert circuit_open_metadata.window_ms == 60_000
    assert circuit_open_metadata.failure_rate_pct == 50
    assert circuit_open_metadata.latency_threshold_ms == 2_000

    assert {:ok, _closed} = FerricStore.flow_circuit_close(circuit_scope, now_ms: 3_100)

    assert_receive {:governance_telemetry, [:ferricstore, :flow, :governance, :circuit_close],
                    %{count: 1}, circuit_close_metadata}

    assert circuit_close_metadata.status == :ok
    assert circuit_close_metadata.scope == circuit_scope
    assert circuit_close_metadata.circuit_status == :closed
  end

  test "budget supports repeated same-scope reserve and commit operations" do
    scope = unique_flow_id("gov-budget-same-scope")

    Enum.each(1..300, fn i ->
      reservation_id = "llm-step-#{i}"

      assert {:ok, reserved} =
               FerricStore.flow_budget_reserve(scope, 1,
                 limit: 1_000,
                 window_ms: 60_000,
                 reservation_id: reservation_id,
                 now_ms: 1_000 + i
               )

      assert reserved.status == :reserved

      assert {:ok, committed} =
               FerricStore.flow_budget_commit(scope, reservation_id, 1,
                 usage: %{tokens: 1},
                 now_ms: 2_000 + i
               )

      assert committed.status == :committed
    end)

    assert {:ok, budget} = FerricStore.flow_budget_get(scope)
    assert budget.used == 300
    assert budget.reservations_count <= 128

    assert {:ok, committed} =
             FerricStore.flow_budget_commit(scope, "llm-step-300", 1,
               usage: %{tokens: 1},
               now_ms: 2_400
             )

    assert committed.status == :committed
  end

  test "limit leases grant, spend, release, and reclaim durable credits" do
    scope = unique_flow_id("gov-limit")

    assert {:ok, %{owner: owner, lease: lease}} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 1,
               amount: 6,
               limit: 10,
               ttl_ms: 1_000,
               now_ms: 1_000
             )

    assert owner.free == 4
    assert lease.available == 6

    assert {:ok, %{owner: owner, lease: lease, reservation_ids: reservation_ids}} =
             FerricStore.flow_limit_spend(scope, shard_id: 1, amount: 4, now_ms: 1_001)

    assert owner.free == 4
    assert lease.available == 2
    assert lease.in_use == 4

    assert {:ok, %{owner: owner, lease: lease}} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 2,
               amount: 10,
               ttl_ms: 1_000,
               now_ms: 1_002
             )

    assert owner.free == 0
    assert lease.available == 4

    assert {:error, denial} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 3,
               amount: 1,
               ttl_ms: 1_000,
               now_ms: 1_003
             )

    assert denial.code == "GOVERNANCE_LIMIT_EXCEEDED"

    assert {:ok, owner} =
             FerricStore.flow_limit_release(scope,
               shard_id: 1,
               reservation_ids: reservation_ids
             )

    assert owner.leases[1].available == 6
    assert owner.leases[1].in_use == 0

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 2_003)
    assert owner.free == 10
    refute Map.has_key?(owner.leases, 1)
    refute Map.has_key?(owner.leases, 2)
  end

  test "expired in-use limit leases are reclaimed to avoid pinned capacity" do
    scope = unique_flow_id("gov-limit-expired-in-use")

    assert {:ok, _lease} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 1,
               amount: 1,
               limit: 1,
               ttl_ms: 100,
               now_ms: 1_000
             )

    assert {:ok, _spent} =
             FerricStore.flow_limit_spend(scope, shard_id: 1, amount: 1, now_ms: 1_001)

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_101)
    assert owner.free == 1
    refute Map.has_key?(owner.leases, 1)
  end

  test "claim_due can spend leased running-limit credits and terminal release frees them" do
    type = unique_flow_id("gov-claim-limit-type")
    limit_scope = unique_flow_id("gov-claim-limit")

    assert {:ok, _lease} =
             FerricStore.flow_limit_lease(limit_scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_create(unique_flow_id("gov-claim-limit-flow-1"),
               type: type,
               state: "queued",
               partition_key: @partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_create(unique_flow_id("gov-claim-limit-flow-2"),
               type: type,
               state: "queued",
               partition_key: @partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    claim_opts = [
      states: ["queued"],
      partition_key: @partition,
      worker: "worker-1",
      limit: 1,
      now_ms: 1_001,
      governance_limit_scope: limit_scope,
      governance_shard_id: 0
    ]

    assert {:ok, [claimed]} = FerricStore.flow_claim_due(type, claim_opts)

    assert {:error, denial} =
             FerricStore.flow_claim_due(type, Keyword.put(claim_opts, :now_ms, 1_002))

    assert denial.code == "GOVERNANCE_LIMIT_EXCEEDED"

    assert :ok =
             FerricStore.flow_complete(claimed.id, claimed.lease_token,
               partition_key: @partition,
               fencing_token: claimed.fencing_token,
               now_ms: 1_003,
               governance_limit_scope: limit_scope,
               governance_shard_id: 0
             )

    assert {:ok, [_second]} =
             FerricStore.flow_claim_due(type, Keyword.put(claim_opts, :now_ms, 1_004))
  end

  test "governed claim uses pre-spent local limit credits across batches" do
    type = unique_flow_id("gov-claim-cache-type")
    limit_scope = unique_flow_id("gov-claim-cache-limit")

    assert {:ok, _lease} =
             FerricStore.flow_limit_lease(limit_scope,
               shard_id: 0,
               amount: 4,
               limit: 4,
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    for i <- 1..2 do
      assert :ok =
               FerricStore.flow_create(unique_flow_id("gov-claim-cache-flow-#{i}"),
                 type: type,
                 state: "queued",
                 partition_key: @partition,
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )
    end

    claim_opts = [
      states: ["queued"],
      partition_key: @partition,
      worker: "worker-cache",
      limit: 1,
      now_ms: 1_001,
      governance_limit_scope: limit_scope,
      governance_shard_id: 0
    ]

    assert {:ok, [_first]} = FerricStore.flow_claim_due(type, claim_opts)
    assert {:ok, owner_after_first} = FerricStore.flow_limit_get(limit_scope, now_ms: 1_002)
    assert owner_after_first.leases[0].available == 0
    assert owner_after_first.leases[0].in_use == 4

    assert {:ok, [_second]} =
             FerricStore.flow_claim_due(type, Keyword.put(claim_opts, :now_ms, 1_003))

    assert {:ok, owner_after_second} = FerricStore.flow_limit_get(limit_scope, now_ms: 1_004)
    assert owner_after_second.leases[0].available == 0
    assert owner_after_second.leases[0].in_use == 4
  end

  test "terminal release returns its named pre-spent credit durably" do
    type = unique_flow_id("gov-release-cache-type")
    limit_scope = unique_flow_id("gov-release-cache-limit")

    assert {:ok, _lease} =
             FerricStore.flow_limit_lease(limit_scope,
               shard_id: 0,
               amount: 4,
               limit: 4,
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    ids =
      for i <- 1..2 do
        id = unique_flow_id("gov-release-cache-flow-#{i}")

        assert :ok =
                 FerricStore.flow_create(id,
                   type: type,
                   state: "queued",
                   partition_key: @partition,
                   run_at_ms: 1_000,
                   now_ms: 1_000
                 )

        id
      end

    claim_opts = [
      states: ["queued"],
      partition_key: @partition,
      worker: "worker-release-cache",
      limit: 1,
      now_ms: 1_001,
      governance_limit_scope: limit_scope,
      governance_shard_id: 0
    ]

    assert {:ok, [first]} = FerricStore.flow_claim_due(type, claim_opts)
    assert first.id in ids

    assert :ok =
             FerricStore.flow_complete(first.id, first.lease_token,
               partition_key: @partition,
               fencing_token: first.fencing_token,
               now_ms: 1_002,
               governance_limit_scope: limit_scope,
               governance_shard_id: 0
             )

    assert {:ok, owner_after_complete} = FerricStore.flow_limit_get(limit_scope, now_ms: 1_003)
    assert owner_after_complete.leases[0].available == 1
    assert owner_after_complete.leases[0].in_use == 3

    assert {:ok, [second]} =
             FerricStore.flow_claim_due(type, Keyword.put(claim_opts, :now_ms, 1_004))

    assert second.id in ids
    assert second.id != first.id
  end

  test "empty governed claim skips limit spend when no due work exists" do
    type = unique_flow_id("gov-empty-claim-type")
    limit_scope = unique_flow_id("gov-empty-claim-limit")

    assert {:ok, _lease} =
             FerricStore.flow_limit_lease(limit_scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    assert {:ok, _spent} =
             FerricStore.flow_limit_spend(limit_scope,
               shard_id: 0,
               amount: 1,
               now_ms: 1_001
             )

    assert :ok =
             FerricStore.flow_create(unique_flow_id("gov-empty-claim-future"),
               type: type,
               state: "queued",
               partition_key: @partition,
               run_at_ms: 60_000,
               now_ms: 1_000
             )

    ctx = FerricStore.Instance.get(:default)

    assert_eventually(fn ->
      assert Ferricstore.Store.Router.flow_claim_due_presence(ctx, %{
               type: type,
               state: "queued",
               priority: nil,
               partition_key: @partition,
               limit: 1,
               now_ms: 1_002
             }) == :empty
    end)

    assert {:ok, []} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: @partition,
               worker: "worker-1",
               limit: 1,
               now_ms: 1_002,
               governance_limit_scope: limit_scope,
               governance_shard_id: 0
             )

    assert {:ok, owner} = FerricStore.flow_limit_get(limit_scope, now_ms: 1_003)
    assert owner.leases[0].available == 0
    assert owner.leases[0].in_use == 1
  end

  test "effect reserve requires the current flow lease and fencing token" do
    type = unique_flow_id("gov-effect-lease-type")
    id = unique_flow_id("gov-effect-lease")

    _claimed = create_and_claim!(id, type)

    assert {:error, "ERR flow lease_token must be a non-empty string"} =
             FerricStore.flow_effect_reserve(id, "effect", "email.send",
               partition_key: @partition,
               operation_digest: "digest",
               idempotency_key: "idem",
               now_ms: 1_002
             )
  end

  test "ledger list treats its exact durable index as the source of truth" do
    type = unique_flow_id("gov-ledger-repair-type")
    id = unique_flow_id("gov-ledger-repair")
    claimed = create_and_claim!(id, type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(id, "repair", "email.send",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "repair-digest",
               idempotency_key: "repair-1",
               now_ms: 1_002
             )

    ctx = FerricStore.Instance.get(:default)

    assert :ok =
             Ferricstore.Store.Router.delete(
               ctx,
               Ferricstore.Flow.Keys.governance_ledger_index_key(id, @partition)
             )

    assert {:ok, []} = FerricStore.flow_governance_ledger(id, partition_key: @partition)
  end

  test "terminal retention removes effect and ledger governance records" do
    type = unique_flow_id("gov-retention-type")
    id = unique_flow_id("gov-retention")
    now = System.system_time(:millisecond)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: @partition,
               run_at_ms: now,
               now_ms: now,
               retention_ttl_ms: 60_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: @partition,
               worker: "worker-1",
               limit: 1,
               now_ms: now + 1
             )

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(id, "cleanup", "email.send",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "cleanup-digest",
               idempotency_key: "cleanup-1",
               now_ms: now + 2
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: @partition,
               fencing_token: claimed.fencing_token,
               now_ms: now + 3
             )

    assert {:ok, completed} = FerricStore.flow_get(id, partition_key: @partition)
    cleanup_now_ms = completed.terminal_retention_until_ms + 1

    Ferricstore.Test.ShardHelpers.eventually(
      fn ->
        case FerricStore.flow_retention_cleanup(limit: 10, now_ms: cleanup_now_ms) do
          {:ok, cleaned} -> cleaned.flows >= 1
          _other -> false
        end
      end,
      "governance retention cleanup did not delete flow",
      50,
      20
    )

    assert {:ok, nil} = FerricStore.flow_effect_get(id, "cleanup", partition_key: @partition)
    assert {:ok, []} = FerricStore.flow_governance_ledger(id, partition_key: @partition)
  end

  test "terminal retention serializes with racing effect and ledger owner writes" do
    ctx = FerricStore.Instance.get(:default)
    parent = self()
    old_write_hook = Application.get_env(:ferricstore, :flow_retention_owned_write_hook)
    old_plan_hook = Application.get_env(:ferricstore, :flow_retention_after_plan_hook)

    on_exit(fn ->
      restore_env(:flow_retention_owned_write_hook, old_write_hook)
      restore_env(:flow_retention_after_plan_hook, old_plan_hook)
    end)

    for kind <- [:effect, :ledger] do
      type = unique_flow_id("gov-retention-race-#{kind}-type")
      id = unique_flow_id("gov-retention-race-#{kind}")
      now = System.system_time(:millisecond)

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 partition_key: @partition,
                 run_at_ms: now,
                 now_ms: now,
                 retention_ttl_ms: 500
               )

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 states: ["queued"],
                 partition_key: @partition,
                 worker: "worker-retention-race",
                 limit: 1,
                 now_ms: now + 1
               )

      assert :ok =
               FerricStore.flow_complete(id, claimed.lease_token,
                 partition_key: @partition,
                 fencing_token: claimed.fencing_token,
                 now_ms: now + 2
               )

      state_blob = Ferricstore.Store.Router.flow_get(ctx, id, @partition)
      record = Ferricstore.Flow.Codec.decode_record(state_blob)

      key =
        case kind do
          :effect -> Ferricstore.Flow.Keys.governance_effect_key(id, "racing", @partition)
          :ledger -> Ferricstore.Flow.Keys.governance_ledger_key(id, "racing", @partition)
        end

      owner = %{
        id: id,
        partition_key: @partition,
        state_key: Ferricstore.Flow.Keys.state_key(id, @partition),
        expected_guard: Ferricstore.Flow.RetentionGuard.encode(record)
      }

      release_ref = make_ref()

      Application.put_env(:ferricstore, :flow_retention_owned_write_hook, fn
        %{id: ^id}, ^key ->
          send(parent, {:governance_owner_write_paused, key, self(), release_ref})

          receive do
            {:release_governance_owner_write, ^release_ref} -> :ok
          after
            10_000 -> {:error, :governance_owner_write_hook_timeout}
          end

        _owner, _key ->
          :ok
      end)

      Application.put_env(:ferricstore, :flow_retention_after_plan_hook, fn
        :terminal, candidates when is_list(candidates) ->
          if Enum.any?(candidates, &(&1.record.id == id)) do
            send(parent, {:governance_cleanup_planned, key})
          end

          :ok

        _kind, _candidates ->
          :ok
      end)

      set_opts = %{
        expire_at_ms: 0,
        nx: false,
        xx: false,
        get: false,
        keepttl: false,
        flow_retention_owner: owner
      }

      write_task =
        Task.async(fn -> Ferricstore.Store.Router.set(ctx, key, "governance", set_opts) end)

      assert_receive {:governance_owner_write_paused, ^key, apply_pid, ^release_ref}, 5_000

      cleanup_task =
        Task.async(fn ->
          FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 1_000)
        end)

      assert_receive {:governance_cleanup_planned, ^key}, 5_000
      assert Task.yield(cleanup_task, 500) == nil
      send(apply_pid, {:release_governance_owner_write, release_ref})

      assert :ok = Task.await(write_task, 10_000)
      assert {:ok, %{flows: 0}} = Task.await(cleanup_task, 10_000)
      assert Ferricstore.Store.Router.get(ctx, key) == "governance"

      assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

      assert {:ok, %{flows: 1}} =
               FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 1_001)

      assert Ferricstore.Store.Router.get(ctx, key) == nil
    end
  end

  defp create_and_claim!(id, type) do
    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: @partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: @partition,
               worker: "worker-1",
               limit: 1,
               now_ms: 1_001
             )

    claimed
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
