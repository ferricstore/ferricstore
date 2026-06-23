# FerricFlow Governance Design

This document defines the FerricStore-side governance architecture for FerricFlow.

The goal is to make workflow work governable without slowing the normal Flow hot
path.

## Goals

- Enforce side-effect, approval, budget, circuit, and fairness policy.
- Keep current Flow command performance when governance is disabled.
- Support strict global limits without a cross-shard read/write on every command.
- Keep governance truth in FerricStore, not an external database.
- Make every denial explainable with structured errors.
- Attach flow-specific audit to the Flow timeline while also supporting global
  governance views.

## Non-goals

- Do not mirror workflow governance into Platform Postgres.
- Do not make LMDB governance projection authoritative.
- Do not add synchronous global coordination to every `claim_due` candidate.
- Do not read payload bytes for governance checks.
- Do not make all audit events mandatory for every workflow.

## Naming

Product name:

```text
FlowGuard
```

Code namespace:

```elixir
Ferricstore.Flow.Governance
```

Command namespace:

```text
FLOW.EFFECT.*
FLOW.APPROVAL.*
FLOW.BUDGET.*
FLOW.CIRCUIT.*
FLOW.LEDGER
FLOW.GOVERNANCE.*
```

## Implemented V1 Surface

Implemented server-side surfaces:

```text
FLOW.EFFECT.RESERVE
FLOW.EFFECT.CONFIRM
FLOW.EFFECT.FAIL
FLOW.EFFECT.COMPENSATE
FLOW.EFFECT.GET
FLOW.GOVERNANCE.LEDGER
FLOW.APPROVAL.REQUEST
FLOW.APPROVAL.APPROVE
FLOW.APPROVAL.REJECT
FLOW.APPROVAL.GET
FLOW.CIRCUIT.OPEN
FLOW.CIRCUIT.CLOSE
FLOW.CIRCUIT.GET
FLOW.BUDGET.RESERVE
FLOW.BUDGET.GET
FLOW.LIMIT.LEASE
FLOW.LIMIT.SPEND
FLOW.LIMIT.RELEASE
FLOW.LIMIT.GET
```

Implemented policy fields:

```text
governance.mode
governance.effects
governance.approvals
governance.circuits
governance.budgets
governance.limits
governance.audit
```

Implemented enforcement:

```text
effect allow/deny policy
effect idempotency/fencing/lease validation
approval-required denial for governed effects/states
circuit-open denial for governed effects
fixed-window budget reserve/get
durable strict-limit credit lease/grant/spend/release model
opt-in claim_due admission using leased running-limit credits
opt-in complete/fail/cancel/retry release using the same limit scope
per-flow governance ledger
```

The implemented hot-path enforcement is explicitly opt-in. `claim_due` enforces
strict running capacity only when the caller passes `governance_limit_scope` and
`governance_shard_id`; terminal and retry commands release capacity only when
the same scope/shard options are passed. This keeps governance-disabled Flow
performance unchanged until default policy-driven enforcement is enabled.

Governance limit TTL is a recovery boundary. Set the credit lease TTL at least
as long as the Flow lease or handler window that should count against capacity.
When a governance lease expires, FerricStore reclaims both idle and in-use
credits so a crashed worker cannot pin capacity forever.

Not yet inserted into the normal Flow create/claim hot path:

```text
default strict global active/running limit enforcement
automatic local shard credit spending during claim/create without explicit opts
async governance dashboard projections
```

## Truth Model

```text
WARaft + Bitcask = durable governance truth
Hot governance caches = rebuildable enforcement state
LMDB = async query/dashboard projection
```

Current Flow state remains authoritative. Governance records are durable
Flow-adjacent records.

## Audit Model

Flow-specific audit is represented as Flow timeline events, not as a separate
workflow state machine.

Each governance decision that belongs to a specific flow may write a compact
ledger event tied to that flow:

```text
flow/gov/ledger/{partition}/{flow_id}/{event_id}
```

This makes the Flow detail page show:

```text
created
claimed
effect reserved
approval requested
approval granted
effect confirmed
completed
```

Global awareness is provided by async projections:

```text
denials by scope
effects by status
approvals by assignee
budget windows
circuits by state/effect
top blocked tenants/types/states
```

So:

```text
per-flow audit = durable timeline
global audit/dashboard = async LMDB projection + compact counters
```

## Governance Scope

Strict governance must have a deterministic owner.

Scope examples:

```text
partition:tenant-123
tenant:acme
type:email
type_state:email:queued
effect:stripe.charge
budget:llm:gpt-5
```

Scope key:

```text
flow/gov/scope/{scope}
```

The scope key hashes to one governance owner shard.

Default scope:

```text
partition_key if provided
hidden auto bucket if no partition_key
```

For strict tenant-level governance, users should use:

```text
partition_key = tenant_id
```

or explicit:

```text
governance_scope = "tenant:acme"
```

The server must not trust arbitrary tenant scope if authentication/ACL binds a
user to a tenant. In managed mode, scope resolution must be derived from the
authenticated session or validated against it.

## Policy Composition

Policy levels:

```text
type
state
effect/tool
scope
```

Composition order:

```text
default
type
state
effect/tool
command override if allowed
```

Conflict rules:

```text
deny wins over allow
approval_required wins over allow
budget_exhausted wins over approval
circuit_open wins over approval
admin override wins only if explicitly authorized and audited
```

Every effective policy gets a deterministic hash:

```text
policy_hash = hash(canonical_effective_policy)
```

Governance records store the policy hash used for the decision.

## Enforcement Modes

Each policy rule declares enforcement:

```text
strict_local
shard_local
strict_global
approximate_global
async_audit
```

Meaning:

```text
strict_local: exact on the Flow shard/scope shard
shard_local: exact per shard, global projection async
strict_global: exact through governance owner + leased credits
approximate_global: soft aggregate, no hard guarantee
async_audit: observe only
```

If a policy asks for strict global enforcement, it must use the governance
owner/credit lease protocol.

## Global Credit Lease Model

Global limits are enforced by governance owner shards.

Flow shards do not ask the governance shard for every command. They lease
credits in batches and spend local in-memory credits on the hot path.

Credit types:

```text
running credits
claim rate credits
budget credits
effect rate credits
```

Governance owner tracks:

```elixir
%Scope{
  scope: binary(),
  limit: integer(),
  free: integer(),
  leases: %{
    shard_id => %Lease{
      epoch: integer(),
      available: integer(),
      in_use: integer(),
      pending_reclaim: integer(),
      expires_at_ms: integer(),
      drain_rate: float(),
      last_spend_at_ms: integer()
    }
  }
}
```

Flow shard cache:

```elixir
%LocalLease{
  scope: binary(),
  epoch: integer(),
  available: integer(),
  in_use: integer(),
  expires_at_ms: integer(),
  low_watermark: integer()
}
```

Hot path rule:

```text
spend local unexpired credits only
remote refill only when low/empty
```

## Dynamic Rebalancing

Static per-shard slices are not enough. If one shard reaches zero while other
shards hold idle credits, the governance owner should rebalance.

Reclaim order:

```text
1. grant from free pool
2. reclaim expired leases
3. accept voluntary returns from idle shards
4. hard reclaim from idle shards under pressure
```

Strict correctness rule:

```text
pending_reclaim is not reusable until donor ack or lease expiry
```

This prevents over-allocation.

Never steal:

```text
in_use credits
```

Stealable:

```text
available credits above shard floor
expired credits
voluntarily returned credits
```

Reclaim candidate order:

```text
lowest drain_rate
highest available
oldest last_spend_at_ms
not in cooldown
```

Anti-hoarding:

```text
short TTL
adaptive grant size
voluntary return
low watermark refill
EWMA drain rate
cooldown after reclaim
```

Recommended defaults:

```text
lease_ttl_ms: 2000-5000
target_window_ms: 250-1000
refill_threshold: 25%
min_grant: 32
max_grant: policy-dependent
reclaim_cooldown_ms: 250
```

## Failure Behavior

Strict policy:

```text
continue using current unexpired local credits
do not grant/refill when governance owner cannot durably write
stop governed work after credits/lease expire
fail closed
```

Soft policy:

```text
may use bounded emergency credits
record debt marker
reconcile later
degrade open only if explicitly configured
```

If governance owner disk/Raft append fails:

```text
no new strict credit grant
return GOVERNANCE_UNAVAILABLE or retry_after
```

If Flow shard disk/Raft append fails:

```text
Flow command fails anyway
```

If governance owner leader changes:

```text
existing unexpired local credits remain valid
refill pauses during election
new owner rebuilds scope leases from durable records
```

## Side-Effect Registry

Commands:

```text
FLOW.EFFECT.RESERVE
FLOW.EFFECT.CONFIRM
FLOW.EFFECT.FAIL
FLOW.EFFECT.COMPENSATE
FLOW.EFFECT.GET
FLOW.EFFECT.LIST
```

Effect truth belongs to the Flow shard because it needs current Flow state,
lease, and fencing.

Effect reserve:

```text
1. load current Flow state
2. validate lease/fencing
3. read compiled policy cache
4. check effect allowed/denied/approval/budget/circuit
5. spend local governance credits or refill
6. write effect record
7. write compact flow ledger event if policy requires
8. return structured decision
```

Record:

```elixir
%{
  flow_id: binary(),
  partition_key: binary(),
  effect_key: binary(),
  effect_type: binary(),
  status: :reserved | :sending | :confirmed | :failed | :compensated | :unknown,
  operation_digest: binary(),
  idempotency_key: binary() | nil,
  external_id: binary() | nil,
  policy_hash: binary(),
  cost: map(),
  created_at_ms: integer(),
  updated_at_ms: integer()
}
```

Correctness:

```text
same effect_key + same operation_digest -> idempotent existing decision
same effect_key + different operation_digest -> conflict
non-idempotent effect denied unless policy allows approval path
```

## Approvals

Commands:

```text
FLOW.APPROVAL.REQUEST
FLOW.APPROVAL.APPROVE
FLOW.APPROVAL.REJECT
FLOW.APPROVAL.ASSIGN
FLOW.APPROVAL.COMMENT
FLOW.APPROVAL.EXPIRE
FLOW.APPROVAL.GET
FLOW.APPROVAL.LIST
```

Approval record belongs to the Flow shard.

Record:

```elixir
%{
  approval_id: binary(),
  flow_id: binary(),
  partition_key: binary(),
  effect_key: binary() | nil,
  status: :pending | :approved | :rejected | :expired,
  assignee: binary(),
  requested_by: binary(),
  approved_by: binary() | nil,
  reason: binary() | nil,
  visible_value_refs: [binary()],
  policy_hash: binary(),
  created_at_ms: integer(),
  expires_at_ms: integer() | nil
}
```

Approval decisions are durable flow ledger events.

## Budgets

Budget model:

```text
per-flow budget: exact and local
per-scope budget: exact through governance owner
global budget: exact only via governance owner credit lease
```

Budget commands:

```text
FLOW.BUDGET.GET
FLOW.BUDGET.RESERVE
FLOW.BUDGET.COMMIT
FLOW.BUDGET.RELEASE
```

Budget reserve:

```text
reserve estimated amount before effect
commit actual amount after effect
release if effect not sent
```

Budget credits are durable leases from the governance owner. Local spending must
be replayable from Flow/effect records.

## Circuits

Circuit policies protect retry loops and downstream failures.

Commands:

```text
FLOW.CIRCUIT.OPEN
FLOW.CIRCUIT.CLOSE
FLOW.CIRCUIT.GET
FLOW.CIRCUIT.LIST
```

Circuit record:

```elixir
%{
  scope: binary(),
  status: :open | :half_open | :closed,
  reason: binary(),
  error_fingerprint: binary(),
  opened_at_ms: integer(),
  retry_after_ms: integer(),
  sample_flow_id: binary() | nil
}
```

Claim/effect paths use cached circuit state. Circuit truth is durable and
rebuildable.

Default circuit enforcement is failure-driven: clean successful effects do not
write circuit records while the circuit is closed. Circuit writes happen on
failures, slow calls, half-open probe claims/results, manual operator changes,
or when a success resets prior failure state. Exact `failure_rate_pct` policies
are the advanced mode and intentionally track successes as well as failures.

## Structured Denials

Governance denial must be structured.

Native error body:

```elixir
%{
  code: "GOVERNANCE_LIMIT_EXCEEDED",
  message: "tenant acme exceeded max running workflows for email:queued",
  policy: "max_running",
  scope: "tenant:acme",
  type: "email",
  state: "queued",
  current: 10_000,
  limit: 10_000,
  requested: 100,
  available: 0,
  retry_after_ms: 250,
  enforcement: "strict_global",
  decision_id: "govdec_..."
}
```

Common codes:

```text
GOVERNANCE_LIMIT_EXCEEDED
GOVERNANCE_BUDGET_EXHAUSTED
GOVERNANCE_APPROVAL_REQUIRED
GOVERNANCE_EFFECT_DENIED
GOVERNANCE_CIRCUIT_OPEN
GOVERNANCE_UNAVAILABLE
GOVERNANCE_CONFLICT
```

Denials should answer:

```text
what blocked me
which scope owns it
current usage
limit
retry_after
decision_id
operator hint
```

High-frequency denials must aggregate instead of writing one event per denial.

Denial aggregation key:

```text
gov/denial_counter/{scope}/{policy}/{reason}/{bucket_ms}
```

Strict denials for effects, approvals, budgets, and admin overrides should be
durably audited. High-rate capacity denials should be sampled/aggregated.

## Claim Due Performance

`claim_due` must avoid remote governance checks per candidate.

Before scanning:

```text
load compiled type/state policy
read local unexpired governance credits
derive max claim capacity
```

During candidate loop:

```text
cheap in-memory credit checks only
```

After selected batch:

```text
consume local credits
write Flow claim state
enqueue governance projection/metrics
```

If local governance capacity is zero:

```text
trigger refill
return structured governance capacity response or retry_after
do not scan endlessly
```

## Projections

Governance queries use async LMDB projections.

Indexes:

```text
effect_status(scope,status,updated_at,effect_key)
effect_type(scope,effect_type,status,updated_at,effect_key)
approval_assignee(assignee,status,due_at,approval_id)
approval_flow(flow_id,status,approval_id)
budget_scope(scope,window_start_ms)
ledger_flow(flow_id,event_ms,event_id)
denial_scope(scope,policy,bucket_ms)
circuit_scope(scope,status,updated_at)
```

Projection lag affects dashboard freshness, not command correctness.

## Retention

Governance retention is policy-controlled.

Defaults:

```text
effect records: same as Flow terminal retention
approval records: same as Flow terminal retention
budget windows: 7-30 days
ledger events: same as history retention unless audit policy extends
denial counters: short TTL
```

Retention cleanup must remove:

```text
truth records
hot indexes
LMDB projections
ledger events
denial counters
```

No payload reads during retention cleanup.

## ACL And Security

New ACL categories:

```text
FLOW.GOV.READ
FLOW.GOV.WRITE
FLOW.EFFECT.READ
FLOW.EFFECT.WRITE
FLOW.APPROVAL.READ
FLOW.APPROVAL.WRITE
FLOW.BUDGET.READ
FLOW.BUDGET.WRITE
FLOW.CIRCUIT.READ
FLOW.CIRCUIT.WRITE
```

Admin-only:

```text
FLOW.GOVERNANCE.POLICY.SET
FLOW.CIRCUIT.CLOSE
FLOW.REPAIR.APPLY
FLOW.BUDGET.GRANT
FLOW.GOVERNANCE.OVERRIDE
```

Approval mutation stores actor identity from authenticated connection metadata.

Managed multi-tenant mode must bind allowed scopes to authenticated identity.

## Reconciliation

Background reconciler handles partial failures:

```text
budget reserved but effect not written
effect reserved but never confirmed/failed
effect confirmed but budget commit missing
approval approved but flow retained/deleted
token granted but flow shard crashed
pending_reclaim waiting for donor ack
```

Reconciler must be idempotent and emit compact repair ledger events.

## Testing Requirements

Correctness tests:

```text
effect reserve idempotency
effect conflict on different operation_digest
effect reserve survives restart
approval required blocks effect
approval approve allows effect
budget exhausted blocks reserve
two flow shards cannot exceed global max_running
tokens expire and capacity returns after shard crash
governance owner restart preserves lease state
disk append failure denies strict grants
soft policy degrades only when configured
retention removes governance records/indexes
projection rebuild restores governance queries
structured denial includes scope/current/limit/retry_after
high-rate denials aggregate
```

Performance tests:

```text
governance disabled: DBOS/native-KV baselines unchanged
warm local credits: claim_due close to baseline
refill path: bounded p99
denial storm: no write/log storm
projection lag: no command correctness impact
```

## Implementation Order

1. Policy schema and compiled policy cache.
2. Structured governance decision/error type.
3. Side-effect registry records and commands.
4. Flow ledger event storage for governance timeline.
5. Approval records and commands.
6. Governance scope owner and local credit lease data model.
7. Strict global credit request/return/expire.
8. Dynamic rebalancing and pending reclaim.
9. Budget reserve/commit/release.
10. Circuit records and claim/effect blocking.
11. LMDB projections and dashboard/query commands.
12. Reconciler and repair commands.

Each step must keep governance-disabled performance unchanged.
