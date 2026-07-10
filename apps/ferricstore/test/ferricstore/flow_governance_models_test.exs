defmodule Ferricstore.FlowGovernanceModelsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Governance.Approval
  alias Ferricstore.Flow.Governance.Budget
  alias Ferricstore.Flow.Governance.Circuit
  alias Ferricstore.Flow.Governance.CreditLease
  alias Ferricstore.Flow.Governance.Policy
  alias Ferricstore.Flow.Governance.Scope

  test "policy normalization keeps limits, budgets, circuits, and approvals" do
    assert {:ok, policy} =
             Policy.normalize(%{
               mode: "full",
               limits: %{
                 "running" => %{limit: 100, enforcement: "strict_global", lease_size: 20}
               },
               budgets: %{
                 "llm:gpt" => %{limit: 10_000, window_ms: 60_000, unit: "tokens"}
               },
               circuits: %{
                 "effect:email.send" => %{
                   failure_threshold: 5,
                   open_ms: 30_000,
                   window_ms: 60_000,
                   min_calls: 10,
                   failure_rate_pct: 50,
                   latency_threshold_ms: 2_000,
                   error_classes: ["TimeoutError"],
                   half_open_max_probes: 3,
                   half_open_success_threshold: 2
                 }
               },
               approvals: %{
                 effects: ["stripe.refund"],
                 states: ["payment_review"],
                 assignees: ["finance"]
               }
             })

    assert policy.mode == :full
    assert policy.limits["running"].enforcement == :strict_global
    assert policy.limits["running"].lease_size == 20
    assert policy.budgets["llm:gpt"].window_ms == 60_000
    assert policy.circuits["effect:email.send"].failure_threshold == 5
    assert policy.circuits["effect:email.send"].window_ms == 60_000
    assert policy.circuits["effect:email.send"].min_calls == 10
    assert policy.circuits["effect:email.send"].failure_rate_pct == 50
    assert policy.circuits["effect:email.send"].latency_threshold_ms == 2_000
    assert policy.circuits["effect:email.send"].error_classes == ["TimeoutError"]
    assert policy.circuits["effect:email.send"].half_open_max_probes == 3
    assert policy.circuits["effect:email.send"].half_open_success_threshold == 2
    assert policy.approvals.effects == ["stripe.refund"]
    assert policy.approvals.states == ["payment_review"]
  end

  test "policy normalization rejects unenforced limit modes without crashing" do
    for enforcement <- [:strict_local, :shard_local, :async_audit, "strict_local"] do
      assert {:error, "ERR invalid flow governance enforcement mode"} =
               Policy.normalize(%{
                 limits: %{
                   "running" => %{limit: 10, enforcement: enforcement}
                 }
               })
    end
  end

  test "policy normalization returns errors for every malformed enum value" do
    for mode <- [:unknown, 42, %{}] do
      assert {:error, "ERR invalid flow governance mode"} = Policy.normalize(%{mode: mode})
    end

    for denials <- [:unknown, 42, %{}] do
      assert {:error, "ERR invalid flow governance denials audit mode"} =
               Policy.normalize(%{audit: %{denials: denials}})
    end
  end

  test "policy normalization rejects circuit windows larger than tracked samples" do
    assert {:error, reason} =
             Policy.normalize(%{
               circuits: %{
                 "effect:email.send" => %{failure_threshold: 65, open_ms: 30_000}
               }
             })

    assert reason =~ "failure_threshold"
    assert reason =~ "64"

    assert {:error, reason} =
             Policy.normalize(%{
               circuits: %{
                 "effect:llm.call" => %{
                   failure_threshold: 10,
                   open_ms: 30_000,
                   window_ms: 60_000,
                   min_calls: 65,
                   failure_rate_pct: 50
                 }
               }
             })

    assert reason =~ "min_calls"
    assert reason =~ "64"
  end

  test "resolved governance policy carries stable version metadata" do
    flow_policy = %{
      version: "policy-v7",
      governance: %{
        mode: "full",
        effects: %{denied: ["stripe.refund"]}
      }
    }

    resolved = Policy.resolve(flow_policy, "queued", "stripe.refund")

    assert resolved.policy_version == "policy-v7"
    assert is_binary(resolved.policy_hash)
    refute resolved.policy_hash == ""
  end

  test "scope resolution prefers explicit governance scope over partition key" do
    assert {:ok, scope} =
             Scope.resolve(%{
               id: "flow-1",
               type: "email",
               state: "queued",
               partition_key: "tenant-a",
               governance_scope: "tenant:acme"
             })

    assert scope.kind == :tenant
    assert scope.name == "acme"
    assert scope.key == "tenant:acme"
  end

  test "scope resolution falls back to partition key and then auto bucket" do
    assert {:ok, partition_scope} =
             Scope.resolve(%{id: "flow-1", type: "email", partition_key: "tenant-a"})

    assert partition_scope.kind == :partition
    assert partition_scope.key == "partition:tenant-a"

    assert {:ok, auto_scope} = Scope.resolve(%{id: "flow-2", type: "email"})
    assert auto_scope.kind == :partition
    assert String.starts_with?(auto_scope.key, "partition:")
  end

  test "credit owner grants leases, spends credits, and refuses over-limit requests" do
    owner = CreditLease.owner("tenant:acme", 10)

    assert {:ok, owner, lease} = CreditLease.grant(owner, 1, 6, now_ms: 1_000, ttl_ms: 5_000)
    assert lease.available == 6
    assert owner.free == 4

    assert {:ok, owner, lease} = CreditLease.spend(owner, 1, 4, now_ms: 1_001)
    assert lease.available == 2
    assert lease.in_use == 4

    assert {:ok, owner, lease} = CreditLease.grant(owner, 2, 5, now_ms: 1_002, ttl_ms: 5_000)
    assert lease.available == 4
    assert owner.free == 0

    assert {:error, denial, owner} = CreditLease.grant(owner, 3, 1, now_ms: 1_003, ttl_ms: 5_000)
    assert denial.code == "GOVERNANCE_LIMIT_EXCEEDED"
    assert owner.free == 0
  end

  test "credit owner reclaims expired lease credits" do
    owner = CreditLease.owner("tenant:acme", 10)

    assert {:ok, owner, _lease} = CreditLease.grant(owner, 1, 6, now_ms: 1_000, ttl_ms: 5)
    assert owner.free == 4

    owner = CreditLease.reclaim_expired(owner, 1_006)

    assert owner.free == 10
    refute Map.has_key?(owner.leases, 1)
  end

  test "credit release by reservation id is idempotent and cannot free a sibling" do
    owner = CreditLease.owner("global", 2)
    assert {:ok, owner, _lease} = CreditLease.grant(owner, 1, 2, now_ms: 1_000, ttl_ms: 1_000)

    assert {:ok, owner, _lease} =
             CreditLease.spend(owner, 1, 2,
               now_ms: 1_001,
               reservation_ids: ["reservation-a", "reservation-b"]
             )

    owner = CreditLease.release(owner, 1, 1, reservation_ids: ["reservation-a"])
    owner = CreditLease.release(owner, 1, 1, reservation_ids: ["reservation-a"])

    assert owner.leases[1].in_use == 1
    assert owner.leases[1].available == 1

    assert owner.leases[1].reservations == %{"reservation-b" => 1}
  end

  test "credit release accepts legacy reservation maps after upgrade" do
    owner = CreditLease.owner("global", 1)

    lease = %CreditLease.Lease{
      shard_id: 1,
      epoch: 1,
      expires_at_ms: 2_000,
      in_use: 1,
      reservations: %{
        "legacy-reservation" => %{reservation_id: "legacy-reservation", amount: 1}
      }
    }

    owner = %{owner | free: 0, epoch: 1, leases: %{1 => lease}}
    owner = CreditLease.release(owner, 1, 1, reservation_ids: ["legacy-reservation"])

    assert owner.leases[1].in_use == 0
    assert owner.leases[1].available == 1
    assert owner.leases[1].reservations == %{}
  end

  test "credit owner applies monotonic config versions and drains capacity on decreases" do
    owner =
      CreditLease.owner("global", 10,
        config_version: 1,
        policy_version: "policy-v1"
      )

    assert {:ok, owner, _lease} =
             CreditLease.grant(owner, 2, 5, now_ms: 1_000, ttl_ms: 10_000)

    assert {:ok, owner, _lease} =
             CreditLease.spend(owner, 2, 4,
               now_ms: 1_001,
               reservation_ids: ["shard-2-a", "shard-2-b", "shard-2-c", "shard-2-d"]
             )

    assert {:ok, owner, _lease} =
             CreditLease.grant(owner, 1, 5, now_ms: 1_002, ttl_ms: 10_000)

    assert {:ok, owner, _lease} =
             CreditLease.spend(owner, 1, 4,
               now_ms: 1_003,
               reservation_ids: ["shard-1-a", "shard-1-b", "shard-1-c", "shard-1-d"]
             )

    assert {:ok, owner} = CreditLease.reconfigure(owner, 6, 2, "policy-v2")
    assert owner.limit == 6
    assert owner.config_version == 2
    assert owner.policy_version == CreditLease.policy_version_fingerprint("policy-v2")
    assert owner.free == 0
    assert owner.leases[1].available == 0
    assert owner.leases[2].available == 0
    assert owner.leases[1].in_use + owner.leases[2].in_use == 8

    reclaimed = CreditLease.reclaim_expired(owner, 11_001)
    assert reclaimed.free == 2
    assert reclaimed.leases[1].in_use == 4
    refute Map.has_key?(reclaimed.leases, 2)

    assert {:ok, ^owner} = CreditLease.reconfigure(owner, 20, 1, "stale-policy")

    assert {:error, "ERR flow limit config_version conflict", ^owner} =
             CreditLease.reconfigure(owner, 7, 2, "conflicting-policy")

    owner = CreditLease.release(owner, 1, 1, reservation_ids: ["shard-1-a"])
    assert owner.leases[1].in_use == 3
    assert owner.leases[1].available == 0

    owner = CreditLease.release(owner, 1, 1, reservation_ids: ["shard-1-b"])
    assert owner.leases[1].in_use == 2
    assert owner.leases[1].available == 0

    owner = CreditLease.release(owner, 1, 1, reservation_ids: ["shard-1-c"])
    assert owner.leases[1].in_use == 1
    assert owner.leases[1].available == 1

    assert {:ok, owner} = CreditLease.reconfigure(owner, 12, 3, "policy-v3")
    assert owner.limit == 12
    assert owner.config_version == 3
    assert owner.policy_version == CreditLease.policy_version_fingerprint("policy-v3")
    assert owner.free == 6
  end

  test "credit owner marks reclaim when global credits exist on another shard" do
    owner = CreditLease.owner("tenant:acme", 10)

    assert {:ok, owner, _lease} = CreditLease.grant(owner, 1, 10, now_ms: 1_000, ttl_ms: 5_000)
    assert {:error, denial, owner} = CreditLease.grant(owner, 2, 1, now_ms: 1_001, ttl_ms: 5_000)

    assert denial.code == "GOVERNANCE_LIMIT_EXCEEDED"
    assert owner.leases[1].pending_reclaim == 1
  end

  test "budget fixed window resets and returns structured exhaustion" do
    budget = Budget.fixed_window("llm:gpt", 100, 60_000, now_ms: 0)

    assert {:ok, budget, first} =
             Budget.reserve(budget, 40, now_ms: 1_000, reservation_id: "res-1")

    assert first.id == "res-1"

    assert {:ok, budget, _second} =
             Budget.reserve(budget, 60, now_ms: 2_000, reservation_id: "res-2")

    assert {:error, denial, ^budget} =
             Budget.reserve(budget, 1, now_ms: 3_000, reservation_id: "res-denied")

    assert denial.code == "GOVERNANCE_BUDGET_EXHAUSTED"

    assert {:ok, reset_budget, _reservation} =
             Budget.reserve(budget, 1, now_ms: 61_000, reservation_id: "res-3")

    assert reset_budget.used == 1
  end

  test "budget commit settles actual usage and records overage" do
    budget = Budget.fixed_window("llm:gpt", 100, 60_000, now_ms: 0)

    assert {:ok, budget, _reservation} =
             Budget.reserve(budget, 80, now_ms: 1_000, reservation_id: "llm-step")

    assert budget.used == 80

    assert {:ok, budget, committed} =
             Budget.commit(budget, "llm-step", 120,
               now_ms: 2_000,
               usage: %{model: "gpt", tokens: 120}
             )

    assert budget.used == 120
    assert committed.status == :committed
    assert committed.actual_amount == 120
    assert committed.overage_amount == 20
    assert committed.usage == %{model: "gpt", tokens: 120}

    assert {:error, denial, ^budget} =
             Budget.reserve(budget, 1, now_ms: 3_000, reservation_id: "denied-after-overage")

    assert denial.code == "GOVERNANCE_BUDGET_EXHAUSTED"
  end

  test "budget release refunds unused reservation" do
    budget = Budget.fixed_window("llm:gpt", 100, 60_000, now_ms: 0)

    assert {:ok, budget, _reservation} =
             Budget.reserve(budget, 80, now_ms: 1_000, reservation_id: "unused")

    assert {:ok, budget, released} = Budget.release(budget, "unused", now_ms: 2_000)

    assert budget.used == 0
    assert released.status == :released
    assert released.actual_amount == 0

    assert {:ok, ^budget, ^released} = Budget.release(budget, "unused", now_ms: 2_001)
  end

  test "circuit opens after threshold and half-opens after cooldown" do
    circuit = Circuit.new("effect:email.send", failure_threshold: 2, open_ms: 1_000)

    assert :allow = Circuit.allow?(circuit, 1_000)
    circuit = Circuit.record_failure(circuit, 1_000)
    circuit = Circuit.record_failure(circuit, 1_001)

    assert {:deny, denial} = Circuit.allow?(circuit, 1_500)
    assert denial.code == "GOVERNANCE_CIRCUIT_OPEN"

    assert :allow = Circuit.allow?(circuit, 2_002)
    circuit = Circuit.claim_probe(circuit, 2_002)
    assert %{status: :closed, failures: 0} = Circuit.record_success(circuit, 2_003)
  end

  test "default circuit success is no-op until a prior failure needs reset" do
    circuit = Circuit.new("effect:email.send", failure_threshold: 2, open_ms: 1_000)

    assert Circuit.record_success(circuit, 1_000) == circuit

    circuit = Circuit.record_failure(circuit, 1_001)
    assert circuit.status == :closed
    assert circuit.failures == 1

    circuit = Circuit.record_success(circuit, 1_002)
    assert circuit.status == :closed
    assert circuit.failures == 0

    circuit = Circuit.record_failure(circuit, 1_003)
    assert circuit.status == :closed
    assert circuit.failures == 1
  end

  test "failure-rate circuit still records closed successes for exact rate math" do
    circuit =
      Circuit.new("effect:llm.call",
        failure_threshold: 100,
        window_ms: 60_000,
        min_calls: 4,
        failure_rate_pct: 50
      )

    circuit = Circuit.record_success(circuit, 1_000)
    circuit = Circuit.record_success(circuit, 1_001)

    assert Enum.count(circuit.events, &(&1.kind == :success)) == 2
  end

  test "circuit supports sliding failure rate latency filters and slow-start probes" do
    circuit =
      Circuit.new("effect:llm.call",
        failure_threshold: 100,
        open_ms: 1_000,
        window_ms: 60_000,
        min_calls: 4,
        failure_rate_pct: 50,
        latency_threshold_ms: 2_000,
        error_classes: ["TimeoutError"],
        half_open_max_probes: 2,
        half_open_success_threshold: 2
      )

    circuit = Circuit.record_failure(circuit, 1_000, error_class: "ValidationError")
    assert circuit.status == :closed
    assert circuit.failures == 0

    circuit = Circuit.record_success(circuit, 1_001, latency_ms: 2_500)
    circuit = Circuit.record_success(circuit, 1_002)
    circuit = Circuit.record_failure(circuit, 1_003, error_class: "TimeoutError")
    circuit = Circuit.record_success(circuit, 1_004)

    assert circuit.status == :open
    assert circuit.failures == 2
    assert Enum.any?(circuit.events, &(&1.kind == :slow_call))

    assert Circuit.probe_due?(circuit, 2_004)
    circuit = Circuit.claim_probe(circuit, 2_004)
    assert circuit.status == :half_open
    assert circuit.half_open_in_flight == 1

    circuit = Circuit.claim_probe(circuit, 2_005)
    assert circuit.half_open_in_flight == 2
    refute Circuit.probe_available?(circuit, 2_006)

    circuit = Circuit.record_success(circuit, 2_010)
    assert circuit.status == :half_open

    circuit = Circuit.record_success(circuit, 2_011)
    assert circuit.status == :closed
    assert circuit.failures == 0
  end

  test "circuit error filters ignore missing classes and sequential half-open probes can close" do
    circuit =
      Circuit.new("effect:gateway.call",
        failure_threshold: 1,
        open_ms: 1_000,
        error_classes: ["TimeoutError"],
        half_open_max_probes: 1,
        half_open_success_threshold: 2
      )

    circuit = Circuit.record_failure(circuit, 1_000)
    assert circuit.status == :closed
    assert circuit.failures == 0

    circuit = Circuit.record_failure(circuit, 1_001, error_class: "TimeoutError")
    assert circuit.status == :open

    circuit = Circuit.claim_probe(circuit, 2_001)
    circuit = Circuit.record_success(circuit, 2_002)
    assert circuit.status == :half_open
    assert circuit.half_open_successes == 1
    assert circuit.half_open_in_flight == 0

    circuit = Circuit.claim_probe(circuit, 2_003)
    circuit = Circuit.record_success(circuit, 2_004)
    assert circuit.status == :closed
  end

  test "approval request accepts exactly one terminal decision" do
    approval =
      Approval.request("approval-1",
        flow_id: "flow-1",
        scope: "tenant:acme",
        reason: "manual refund",
        requested_by: "worker-1",
        now_ms: 1_000
      )

    assert approval.status == :pending
    assert {:ok, approved} = Approval.approve(approval, approver: "admin", now_ms: 2_000)
    assert approved.status == :approved
    assert approved.decided_by == "admin"

    assert {:error, denial} = Approval.reject(approved, approver: "admin-2", now_ms: 2_001)
    assert denial.code == "GOVERNANCE_CONFLICT"
  end
end
