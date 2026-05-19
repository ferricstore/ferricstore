# FerricStore Flow Design

## Summary

FerricStore Flow is a durable operational state-machine layer built on top of
FerricStore primitives. It is meant for app-owned execution: the application
runs business logic and side effects, while FerricStore owns durable state,
atomic transitions, timers, leases, retries, history, and backpressure.

Flow is not a generic queue and it is not a full durable-execution runtime like
Temporal. It is the middle layer many applications currently build with Redis
locks, sorted sets, streams, status keys, and polling.

The core model is:

```text
Application:
  workers
  business logic
  external side effects
  Phoenix/LiveView status UI

FerricStore:
  current flow state
  due index
  claim leases
  fencing tokens
  retry schedule
  idempotency/result state
  history stream
  wakeup notifications
```

The product promise is:

```text
temporary does not mean disposable
```

Flow state may live for minutes, hours, or days, but until its TTL/lifecycle
expires it should not disappear because RAM is full, a node restarts, or a
consumer crashes.

## Why Flow

Many production workflows are not naturally "just a queue". They have identity,
state, retries, status, cancellation, idempotency, and user-visible progress.

Examples:

```text
checkout:123
webhook:stripe:event_456
import:file_789
approval:expense_111
notification:user_42:email_abc
```

With a queue-first design, applications usually add extra state around the
message:

```text
queue message
status key
lock key
attempt counter
retry sorted set
idempotency key
history stream
dead-letter key
progress record
```

Flow makes the named operation the source of truth:

```text
flow record = current truth
due index = schedulable truth
history stream = durable event trail
pubsub = fast wakeup only
```

This gives a cleaner producer/consumer model:

```text
producer creates or schedules a flow
workers claim due flows only when they have capacity
workers transition flows atomically
users and services query flow status directly
```

## Relationship To Existing FerricStore Features

Flow reuses existing FerricStore primitives.

| Need | Primitive |
| --- | --- |
| Current state | KV, hash, or JSON value |
| Durable payload | Bitcask cold value |
| Fast metadata/status | ETS/keydir and small hot values |
| Timers and retries | Sorted set score = `next_run_at_ms` |
| History/audit | Stream |
| Wake workers/UI | PubSub |
| Claim ownership | Lock/lease |
| Stale-owner protection | Fencing token |
| Lifecycle cleanup | TTL |
| Ordering and atomicity | Raft state-machine command |
| Backpressure | `claim_due` `limit` plus leases |
| Cross-language access | RESP commands |

Flow should expose these as a first-class API instead of requiring users to
compose the primitives manually.

## Fit With The Current FerricStore Engine

Flow should be designed around how FerricStore works today, not as an abstract
database design.

Current FerricStore has these important properties:

```text
Raft:
  orders quorum writes per shard
  state-machine apply is the serialization point for correctness

Bitcask:
  append-only durable storage
  excellent for key -> latest value point lookups
  old versions are reclaimed later by merge/compaction

ETS/keydir:
  in-memory index from key to current value or file offset
  hot values can live in ETS
  cold values are read from Bitcask by offset

Compound keys:
  hashes, lists, sets, sorted sets, streams are stored as internal keys
  they are durable because mutations go through the same Raft/Bitcask path

PubSub/blocking waiters:
  useful for wakeups
  not a durable source of truth
```

That means Flow's natural durable storage is:

```text
current flow record -> Bitcask key/value, hot metadata in ETS
due ordering        -> sorted-set/compound-key data
history             -> stream/compound-key data
wakeups             -> PubSub / waiter notification
atomicity           -> one native Raft command per Flow operation
```

### Bitcask Fit

Bitcask is a good match for Flow current records because Flow mostly needs
latest-state point lookups:

```text
FLOW.GET id
FLOW.TRANSITION id from -> to
FLOW.COMPLETE id
```

Each update appends a new version of the flow record. The keydir points at the
latest version. Old versions are merge/compaction garbage.

This is the right model for Flow:

```text
flow id -> latest state
```

It is not the right model for long analytical range scans, which is why time
series has a separate columnar design. Flow range needs should be served by
indexes such as due sorted sets and state indexes, not by scanning Bitcask.

### Hot And Cold Values

FlowStore should usually keep only small state/metadata hot.

Recommended:

```text
small flow record:
  state
  next_run_at_ms
  lease_until_ms
  fencing token
  attempts
  data_ref/result_ref

large cold values:
  payload
  result body
  large error body
  large history event body
```

The current engine already supports this model through the hot-cache value-size
threshold. A FlowStore can set a small `hot_cache_max_value_size` so large
payloads do not stay in application RAM while remaining durable on NVMe.

### Compound Key Fit

FerricStore already implements Redis-like structures through compound keys. Flow
can use the same representation internally:

```text
flow_due:{type}:{state}:p{priority}
  sorted set per priority level
  score = next_run_at_ms
  member = flow id

flow_history:{id}
  stream
  event id = HLC/stream id
  fields = event metadata/body reference
```

The important rule is that Flow should not call public Redis-style commands as
a sequence from the client or dispatcher and hope the sequence is atomic.

Do not implement this as:

```text
ZREM flow_due checkout:123
GET flow:checkout:123
SET flow:checkout:123 ...
XADD flow_history:checkout:123 ...
PUBLISH ...
```

Instead, the state-machine apply for a Flow command should update the needed
internal keys in one apply step:

```text
apply FLOW.CLAIM_DUE:
  inspect due sorted-set data
  update flow record
  update due index
  append history event
  emit wakeup metadata
```

This keeps FerricStore's atomicity promise aligned with its current Raft design.

### Stream Fit

Streams are a good fit for Flow history.

Flow history does not initially require stream consumer groups. It mostly needs:

```text
append event
read history by range/count
replay events to reconstruct state for FLOW.REWIND/FLOW.VERIFY
trim by maxlen or TTL
```

That avoids depending on durable consumer-group state for the Flow MVP.

If Flow later uses streams as worker queues, then consumer-group persistence
must be handled carefully. The queue design already notes that consumer-group
state cannot remain ETS-only if it is part of correctness.

### Sorted Set Fit

Sorted sets are the right primitive for timers and retries:

```text
score = next_run_at_ms
member = flow id
```

Priority should not be encoded by subtracting from `next_run_at_ms`, because
that can make future work claimable too early. Keep eligibility and priority
separate:

```text
next_run_at_ms = when work becomes eligible
priority = ordering among eligible work
```

Use one due sorted set per state and priority level:

```text
flow_due:{type}:{state}:p2
flow_due:{type}:{state}:p1
flow_due:{type}:{state}:p0
```

This lets workers claim only states they know how to handle without scanning and
skipping unrelated due flows.

All sets still use:

```text
score = next_run_at_ms
member = flow id
```

`FLOW.CLAIM_DUE` drains due items from highest priority to lowest priority:

```text
for priority in high..low:
  ZRANGEBYSCORE flow_due:{type}:{state}:p{priority} -inf now_ms LIMIT remaining
  claim candidates atomically
  stop when limit is reached
```

This preserves timer correctness while giving priority under backlog.

`FLOW.CLAIM_DUE` should use sorted-set ordering internally, but it must also
validate and mutate the flow record in the same atomic operation. A plain
`ZRANGEBYSCORE` followed by app-side `SET` is not enough.

### PubSub Fit

PubSub should be used for:

```text
worker wakeup
LiveView refresh
flow changed notification
new due work available
```

PubSub should not be used as durable truth.

The durable truth remains:

```text
flow record
due sorted set
history stream
```

### Instance Awareness

Current FerricStore has a module-based instance API where each `use FerricStore`
module owns its own context. Flow should be built for that model from day one.

Correct:

```elixir
MyApp.FlowStore.Flow.claim_due(...)
```

or internally:

```elixir
FerricStore.Flow.claim_due(ctx, ...)
```

Avoid:

```elixir
FerricStore.Instance.get(:default)
```

Flow must work for multiple embedded instances in one application:

```text
KVStore
SessionStore
FlowStore
```

### Engine-Level Command Shape

The cleanest implementation is to add native Flow command terms to the Raft
state machine rather than building Flow out of external command dispatch.

Examples:

```elixir
{:flow_create, id, attrs}
{:flow_claim_due, type, worker_id, lease_ms, limit, now_ms}
{:flow_transition, id, expected_state, next_state, patch, opts}
{:flow_retry, id, error_ref, next_run_at_ms}
{:flow_complete, id, result_ref, ttl_ms}
```

Each command should return a deterministic result from the state machine:

```text
created
exists
claimed records
wrong_state
leased
stale_token
missing
completed
```

This avoids partial updates and keeps the behavior testable under leader
failover, retries, and duplicate client requests.

## Flow vs Queues

FerricStore Queues (`FQ.*`) are message-broker style:

```text
producer -> exchange/queue/stream -> consumer group -> ack/nack/reject
```

Queues are best for:

```text
fire-and-forget messages
routing and fanout
consumer groups
priority queues
dead-letter queues
message-oriented workloads
```

Flow is operation-state style:

```text
producer -> named operation -> claim due -> transition state -> status/history
```

Flow is best for:

```text
idempotent business operations
checkout/payment lifecycles
webhook processing state
imports and uploads
approval flows
scheduled retries
LiveView progress/status screens
operations that can be cancelled/resumed/queried
```

The distinction:

```text
Queue message = work item
Flow record = source of truth
```

FQ can still be useful for high-volume message routing. Flow should be used
when the application cares about the lifecycle of a named operation.

## Flow vs Time Series

Time series and Flow solve different problems.

Time series:

```text
append-only measurements
range scans
compression
retention
aggregation
columnar files and metadata indexes
```

Flow:

```text
mutable operational state
atomic transitions
leases
retries
timers
status reads
history
```

Flow should use FerricStore KV/Bitcask/ZSET/Stream primitives. It does not need
the columnar time-series subsystem because Flow reads are mostly point lookups
and due-index scans, not long analytical range scans.

Flow can emit metrics into TS:

```text
flow transition count
claim latency
retry count
completion latency
dead-letter count
```

## Storage Model

The recommended Flow storage layout has four durable pieces.

```text
flow:{type}:{id}
  Current flow record.

flow_due:{type}:{state}:p{priority}
  Sorted set of due flow ids for one priority level.
  score = next_run_at_ms
  member = flow id

flow_history:{type}:{id}
  Stream of flow events.

flow_by_state:{type}:{state}
  Required secondary index for state/status queries.
  score = updated_at_ms
  member = flow id
```

The state index is part of the MVP, not a later observability add-on. Operators
must be able to list pending/running/failed flows during incidents without
scanning Bitcask.

The current flow record should be small enough to update atomically, but it can
refer to larger payload/result keys if needed.

Example flow record:

```elixir
%{
  id: "checkout:123",
  type: "checkout",
  state: "payment_pending",
  data_ref: "flow_payload:checkout:123",
  result_ref: nil,
  priority: 1,
  next_run_at_ms: 1_714_000_000_000,
  lease_owner: nil,
  lease_until_ms: 0,
  fencing_token: 12,
  attempts: 0,
  max_retries: 10,
  last_error: nil,
  created_at_ms: 1_714_000_000_000,
  updated_at_ms: 1_714_000_000_000,
  expires_at_ms: 1_714_604_800_000
}
```

For FlowStore, large values should usually stay cold on NVMe:

```text
hot:
  id
  type
  state
  next_run_at_ms
  lease_until_ms
  fencing token
  attempt count

cold:
  full payload
  large result
  error body
  large history event body
```

This protects application RAM while keeping point lookup and claim metadata
fast.

## Single-Shard MVP

The simplest MVP should support a one-shard FlowStore instance.

Benefits:

```text
all flow metadata is in one ordered state machine
claim_due is simple
multi-key atomicity is straightforward
no cross-shard due scan
easier testing and debugging
```

Example:

```elixir
defmodule MyApp.FlowStore do
  use FerricStore,
    data_dir: "/data/ferric/flows",
    shard_count: 1,
    eviction_policy: :noeviction,
    hot_cache_max_value_size: 512
end
```

One shard is not a toy mode. For many applications, a single durable ordered
FlowStore can handle enough lightweight flow transitions. It also gives the
cleanest correctness model for v1.

This is independent from node count. A one-shard FlowStore can still run on a
multi-node FerricStore cluster:

```text
3 FerricStore nodes
1 FlowStore shard
1 Raft leader for that shard
2 replicated followers
```

All flow data is replicated across the nodes. The shard leader orders writes
and claims; followers replicate the log and can serve reads if the selected
read-consistency mode allows it.

## Multi-Node Model

Flow must work as a replicated FerricStore workload, not as local node state.

The core rule:

```text
Replication is for availability and read locality.
Sharding is for write/claim throughput.
```

These are separate dimensions.

### One Shard, Many Nodes

With one FlowStore shard on three nodes:

```text
client -> any node -> FLOW.CLAIM_DUE
                     -> routed/forwarded to the shard leader
                     -> Raft orders the claim
                     -> replicated to followers
                     -> reply returns claimed flows
```

Only the Raft leader for that shard may commit state changes:

```text
FLOW.CREATE
FLOW.CLAIM_DUE
FLOW.TRANSITION
FLOW.RETRY
FLOW.COMPLETE
FLOW.CANCEL
```

The caller should not need to know which node is leader. Any node can accept the
command and route it through FerricStore's normal leader/forwarding path.

This gives:

```text
strong atomicity
simple due index
simple claim semantics
replicated durability
leader failover
follower reads where allowed
```

The tradeoff is that all Flow writes are ordered by one shard leader. That is
often acceptable for a first version because the operations are small and can be
batched.

### Many Shards, Many Nodes

With multiple FlowStore shards:

```text
flow id A -> shard 0 -> replicated across nodes
flow id B -> shard 3 -> replicated across nodes
flow id C -> shard 7 -> replicated across nodes
```

Each shard has its own Raft leader and replicated followers. This increases
write/claim throughput because independent flows can be claimed and transitioned
in parallel on different shard leaders.

The application still sees one logical FlowStore:

```text
FLOW.GET <id>
FLOW.TRANSITION <id> ...
FLOW.CLAIM_DUE <type> ...
```

The implementation decides which shard owns each flow. Users should not be
forced to model tenants, partitions, or routing domains unless they want to.

### Point Operations vs Type Operations

In a multi-shard FlowStore, operations fall into two categories.

Point operations route to exactly one shard by flow id:

```text
FLOW.GET <id>
FLOW.TRANSITION <id> ...
FLOW.COMPLETE <id> ...
FLOW.FAIL <id> ...
FLOW.CANCEL <id> ...
FLOW.REWIND <id> ...
FLOW.HISTORY <id> ...
```

These are simple:

```text
shard = hash(flow_id)
route command to shard
execute atomically on that shard
```

Type/state operations are distributed because flows of the same type and state
can live on many shards:

```text
FLOW.CLAIM_DUE <type> STATE <state> ...
FLOW.LIST <type> STATE <state> ...
FLOW.INFO <type>
FLOW.DUE <type>
FLOW.STUCK <type>
FLOW.INFLIGHT <type>
```

Example:

```text
checkout:1 -> shard 0
checkout:2 -> shard 5
checkout:3 -> shard 9

all have:
  type = checkout
  state = payment_pending
```

Therefore each shard owns a shard-local due/state slice:

```text
shard 0: flow_due:checkout:payment_pending:p1
shard 5: flow_due:checkout:payment_pending:p1
shard 9: flow_due:checkout:payment_pending:p1
```

This is intentional. Sharding by flow id keeps many flows of the same type/state
parallelizable. Sharding by type/state would make `CLAIM_DUE` simpler but would
turn a hot type/state into a single-shard bottleneck.

The design choice:

```text
flow-id ownership:
  better write/claim scaling
  type/state reads need aggregation

type/state ownership:
  simpler listing/claiming
  hot workflow type can bottleneck
```

Flow should use flow-id ownership by default.

### What Partitioning Means

Partitioning is only an internal scaling strategy. It is not a product concept
the user must adopt.

Examples such as "tenant" or "type" are optional ways to choose keys when an
application wants predictable locality. They are not required semantics.

From the user's point of view:

```text
id is just an id
type is just an application label
data is just data
FerricStore decides where it lives
```

The important implementation rule is that all internal keys needed to mutate a
single flow must be owned by the same shard:

```text
flow record
due index entry
lease/fencing metadata
history append metadata needed for the transition
```

If a command touches only one flow, it should be single-shard and atomic.

### Due Index In A Multi-Shard Cluster

For one shard, there is one due index:

```text
flow_due:{type}:{state}:p{priority}
```

For many shards, each shard should maintain its own shard-local due indexes for
the flows it owns:

```text
shard 0: flow_due:{type}:{state}:p2, flow_due:{type}:{state}:p1, flow_due:{type}:{state}:p0
shard 1: flow_due:{type}:{state}:p2, flow_due:{type}:{state}:p1, flow_due:{type}:{state}:p0
shard 2: flow_due:{type}:{state}:p2, flow_due:{type}:{state}:p1, flow_due:{type}:{state}:p0
```

`FLOW.CLAIM_DUE <type> ... <limit>` can be implemented in stages:

```text
v1:
  one shard
  claim from the single due index

v2:
  router fans out to all shards for due work by default
  each shard atomically claims from its own due index
  results are merged up to limit

v3:
  add bounded/assigned shard claim strategies for very large shard counts
```

Claim atomicity remains shard-local. There is no global lock across all shards.
That is acceptable because each flow belongs to exactly one shard.

`FLOW.CLAIM_DUE` is logically a type/state operation. For ease of use, the
default behavior should fan out to all shards and return due work from anywhere.

Default simple model:

```text
FLOW.CLAIM_DUE checkout worker-a 30000 100 STATE payment_pending

router:
  asks every shard concurrently
  each shard claims a small local batch atomically
  router merges claimed flows
  returns up to global limit
```

This gives the clean product behavior:

```text
worker asks for work
FerricStore finds due work anywhere
worker does not care about shards
```

Per-shard claim limits should be derived from the requested global limit:

```text
local_limit = ceil(global_limit / shard_count) + slack
```

This avoids severe overclaiming while still allowing uneven shard backlog.

All-shard fan-out is acceptable for modest shard counts:

```text
4, 8, 16, maybe 32 shards
```

especially when shard calls run concurrently and each shard only returns a small
local batch.

Advanced scale model:

```text
workers are assigned shard subsets
each worker claims only from its assigned shards
rebalance assignment when worker set changes
```

Example:

```text
worker-a -> shards 0,1,2,3
worker-b -> shards 4,5,6,7
worker-c -> shards 8,9,10,11
worker-d -> shards 12,13,14,15
```

Then:

```text
FLOW.CLAIM_DUE checkout worker-a 30000 100 STATE payment_pending
```

checks only:

```text
shards 0,1,2,3
```

and claims atomically within those shard-local due indexes.

This is an optimization for large shard counts or very high claim QPS. It should
not be required for the default user experience.

Configurable claim strategies:

```text
claim_strategy: :all_shards
  default, simplest behavior

claim_strategy: :bounded
  router checks max_shards_per_claim per call using a rotating cursor

claim_strategy: :assigned
  workers are assigned shard ranges
```

Multi-shard claim fan-out must be bounded. A worker should not synchronously ask
every shard on every claim when shard count is large and claim QPS is high,
because claim latency and load grow with shard count. This is a scale-mode
concern, not the default MVP behavior.

Acceptable strategies:

```text
assigned shard ranges:
  each worker owns a small shard subset
  FLOW.CLAIM_DUE only checks assigned shards

rotating shard cursor:
  each worker checks K shards per call
  cursor advances between calls
  K is bounded, for example 2-8 shards

coordinator per type:
  a lightweight scheduler tracks shards/types with due work
  workers ask the coordinator for the next shard subset

pubsub hint + bounded scan:
  due wakeups include shard/type hints
  worker first checks hinted shards
  periodic bounded scan catches missed hints
```

The API can remain:

```text
FLOW.CLAIM_DUE <type> <worker_id> <lease_ms> <limit>
```

but implementation should include internal options such as:

```text
max_shards_per_claim
worker_shard_assignment
claim_cursor
```

For v1, this is avoided by recommending one shard. For multi-shard FlowStore,
bounded claim fan-out is required before calling the design production-ready.

### Timers And PubSub In Multi-Node Mode

Timer notifications must not depend on one local process on one node.

Correctness still comes from the durable due index:

```text
missed notification -> periodic claim_due finds the work
node restart -> due index is recovered
leader failover -> new leader can serve claim_due
```

PubSub is only a wakeup accelerator.

Timer watchers can be implemented safely in either of these ways:

```text
leader-only watcher:
  only the shard leader publishes due wakeups

all-node watcher:
  any node may publish wakeups
  duplicate wakeups are harmless
  FLOW.CLAIM_DUE ensures only one worker claims
```

Leader-only reduces duplicate notifications. All-node is simpler if the code is
careful to treat PubSub as lossy and duplicate-prone.

### Reads From Followers

Flow reads have different consistency needs.

Strong reads:

```text
FLOW.GET for a worker before acting
FLOW.HISTORY for audit after a transition
```

These should read from the leader or use a read barrier/read-index equivalent.

Follower/eventual reads:

```text
LiveView progress screen
status dashboard
metrics view
```

These can often tolerate small replication lag, especially when the UI refreshes
again after PubSub/state-change notifications.

Writes and claims should not be served by followers unless they are forwarded to
the shard leader.

Later scaling options:

```text
one FlowStore per domain:
  CheckoutFlowStore
  WebhookFlowStore
  EmailFlowStore

optional key locality chosen by the application:
  flow:{some-user-chosen-id}
  flow_due:{some-user-chosen-type}

multi-shard FlowStore:
  claim_due scans each shard/type partition
```

## API

Flow should have both Elixir APIs and RESP commands. The Elixir API can be
ergonomic; the RESP API should be plain arrays/maps so any Redis client can use
it.

### Elixir API

```elixir
MyApp.FlowStore.Flow.create("checkout:123",
  type: "checkout",
  state: "payment_pending",
  data: payload,
  priority: :normal,
  next_run_at: System.system_time(:millisecond),
  ttl: :timer.days(7),
  idempotency: true
)

MyApp.FlowStore.Flow.claim_due("checkout",
  worker: "node-a:worker-1",
  lease: 30_000,
  limit: 100
)

MyApp.FlowStore.Flow.transition("checkout:123",
  from: "payment_pending",
  to: "email_pending",
  patch: %{payment_id: "pi_123"},
  next_run_at: System.system_time(:millisecond)
)

MyApp.FlowStore.Flow.retry("checkout:123",
  error: "stripe timeout",
  next_run_at: System.system_time(:millisecond) + 60_000
)

MyApp.FlowStore.Flow.complete("checkout:123",
  result: %{order_id: 123},
  ttl: :timer.days(30)
)

MyApp.FlowStore.Flow.cancel("checkout:123",
  reason: "user_cancelled"
)
```

### RESP API

```text
FLOW.CREATE <id> TYPE <type> STATE <state> DATA <bytes> TTL <ms> [NX] [RUN_AT <ms>] [PRIORITY <n|low|normal|high>]
FLOW.GET <id>
FLOW.CLAIM_DUE <type> <worker_id> <lease_ms> <limit> [STATE <state> | STATES <state,...>] [PRIORITY <n|low|normal|high> | MINPRIORITY <n|low|normal|high>] [WORKER_MAX_INFLIGHT <n>] [TYPE_MAX_INFLIGHT <n>] [RATE <n> PER <ms>] [SCOPE <scope> SCOPE_MAX_INFLIGHT <n>]
FLOW.TRANSITION <id> FROM <expected_state> TO <next_state> [PATCH <bytes>] [RUN_AT <ms>]
FLOW.RETRY <id> ERROR <bytes> RUN_AT <ms>
FLOW.COMPLETE <id> RESULT <bytes> TTL <ms>
FLOW.FAIL <id> ERROR <bytes> TTL <ms>
FLOW.CANCEL <id> REASON <bytes>
FLOW.REWIND <id> TO_EVENT <event_id> [RUN_AT <ms>] [REASON <bytes>] [EXPECT_STATE <state>] [DRYRUN]
FLOW.HISTORY <id> [COUNT <n>] [START <stream_id>] [END <stream_id>] [REV] [FULL]
FLOW.EXPLAIN <id> [COUNT <n>] [FULL]
FLOW.INFO <type>
```

Example response shapes:

```text
["ok", "created", id]
["ok", "exists", id, state]
["ok", "claimed", id, state, data, fencing_token]
["err", "wrong_state", current_state]
["err", "leased", lease_owner, lease_until_ms]
["err", "missing"]
```

## Atomicity Model

Flow correctness depends on making each public command a single Raft
state-machine operation or an equivalent same-shard atomic operation.

Do not implement Flow as app-side multi-command glue:

```text
ZREM due
GET flow
SET lease
XADD history
PUBLISH wakeup
```

That is the Redis failure mode. If the process crashes halfway, the durable
state can become inconsistent.

Instead, implement each Flow command as an atomic command inside FerricStore:

```text
FLOW.CLAIM_DUE
  reads due index
  validates flow state
  validates lease
  increments fencing token
  sets lease owner/until
  removes or updates due index
  appends history event
  returns claimed records
```

All-or-nothing within the shard.

### Atomic Commands

#### FLOW.CREATE

Atomic responsibilities:

```text
validate id does not exist if NX
write flow record
write payload/result refs if included
add due index for the selected state and priority if next_run_at_ms is set
append history event "created"
set TTL/lifecycle
publish wakeup if immediately due
```

Idempotent create should return the existing state/result instead of creating a
duplicate.

#### FLOW.CLAIM_DUE

Atomic responsibilities:

```text
for each requested state:
  for priority from highest to lowest within the requested priority filter:
    select due ids where score <= now from flow_due:{type}:{state}:p{priority}
  for each candidate:
    load flow record
    skip missing/expired/completed/cancelled
    skip if state no longer matches the requested state/index
    skip if priority no longer matches the flow record
    skip if lease_until_ms > now
    set lease_owner = worker_id
    set lease_until_ms = now + lease_ms
    increment fencing_token
    remove due index entry or move it to lease expiry
    append history event "claimed"
  stop when limit is reached
return up to limit claimed records
```

This command is the backpressure boundary. Workers only claim what they can
handle.

#### FLOW.TRANSITION

Atomic responsibilities:

```text
load flow record
verify expected state
verify lease owner/token when required
apply patch
set next state
update next_run_at_ms
update priority if the command includes a priority change
update due index:
  add to flow_due:{type}:{state}:p{priority} if next_run_at_ms exists
  remove if no next run
  move between state/priority due indexes if state or priority changed
clear or retain lease depending on next state
append history event "transitioned"
publish state-change event
```

The important contract:

```text
transition only succeeds if current state is exactly expected
```

That gives business operations compare-and-set semantics.

#### FLOW.SIGNAL

Atomic responsibilities:

```text
load flow record
verify IF_STATE guard if present
verify idempotency marker if IDEMPOTENCY/IDEMPOTENCY_KEY is present
attach VALUE / VALUE_REF / DROP_VALUE / OVERRIDE_VALUE changes
optionally transition to a non-terminal state
optionally set run_at_ms and priority
require IF_STATE when transitioning, so stale signals cannot advance wrong state
preserve active lease when the signal does not transition state
clear active lease when the signal transitions state
append history event "signaled"
persist idempotency marker until flow retention cleanup
```

Command shape:

```text
FLOW.SIGNAL <id>
  SIGNAL <name>
  [PARTITION <partition_key>]
  [IDEMPOTENCY <key> | IDEMPOTENCY_KEY <key>]
  [IF_STATE <state>]...
  [TRANSITION_TO <state>]
  [RUN_AT <ms>]
  [PRIORITY <n|low|normal|high>]
  [VALUE <name> <bytes>]...
  [VALUE_REF <name> <ref>]...
  [DROP_VALUE <name>]...
  [OVERRIDE_VALUE <name>]...
```

Signal is for external facts arriving while a Flow is active: webhook received,
human approval, child summary, agent tool output, IoT event, or fraud decision.
Large bodies should use value refs, not inline metadata. Signal history stores
compact metadata and refs; workers hydrate only requested values.

Signal does not create terminal states. Terminal transitions must use
`FLOW.COMPLETE`, `FLOW.FAIL`, or `FLOW.CANCEL`, so lifecycle semantics stay
explicit and index cleanup remains simple.

#### FLOW.RETRY

Atomic responsibilities:

```text
increment attempts
store last_error
if attempts >= max_retries:
  move to failed/dead state
  remove due index
else:
  set state/retry state
  set next_run_at_ms
  add/update due index
clear lease
append history event "retried"
```

#### FLOW.COMPLETE

Atomic responsibilities:

```text
verify expected state/lease if required
store result
set state = completed
remove due index
clear lease
set completion TTL
append history event "completed"
publish state-change event
```

Completion must be idempotent. A duplicate completion should return the existing
completed result when possible.

#### FLOW.CANCEL

Atomic responsibilities:

```text
verify flow is cancellable
set state = cancelled
remove due index
clear lease
append history event "cancelled"
publish state-change event
```

#### FLOW.REWIND

`FLOW.REWIND` replays stored Flow history to an earlier event and makes that
reconstructed state the new current state.

This is not Temporal-style code replay. It does not run application code and it
does not undo external side effects.

Command shape:

```text
FLOW.REWIND <id> TO_EVENT <event_id> [RUN_AT <ms>] [REASON <bytes>] [EXPECT_STATE <state>] [DRYRUN]
```

Concept:

```text
history:
  created -> payment_pending
  transitioned -> payment_charged
  transitioned -> email_pending
  failed

FLOW.REWIND checkout:123 TO_EVENT <payment_charged_event>

new history:
  created -> payment_pending
  transitioned -> payment_charged
  transitioned -> email_pending
  failed
  rewound to <payment_charged_event>, current state = payment_charged
```

The flow continues from the rewound state as a new forward branch in history.
History is not deleted or rewritten.

Atomic responsibilities:

```text
load flow record
verify flow is rewindable:
  terminal, paused, or unleased by default
verify EXPECT_STATE if provided
load/replay history up to target event
derive reconstructed metadata:
  state
  data refs / working data refs
  next_run_at_ms
  priority
  attempts, according to policy
clear lease owner/until
increment fencing token
remove stale due/inflight/worker/scope index entries
write reconstructed current metadata
add due index entry using RUN_AT or reconstructed next_run_at_ms
update state index
append history event "rewound"
publish state-change event
```

`DRYRUN` returns the reconstructed state without applying it:

```text
["ok", "rewind_preview", id, "state", state, "event", event_id]
```

Applied rewind returns:

```text
["ok", "rewound", id, "state", state, "event", event_id]
```

Safety rules:

```text
rewind does not undo external side effects
rewind should require REASON for operator use
rewind should be disabled for actively leased flows unless forced by an explicit future option
rewind should increment fencing token so stale workers cannot complete old claims
rewind should append a history event rather than rewriting history
```

If a prior state involved external side effects, the application must decide how
to continue safely:

```text
idempotency key
compensation state
external operation ref
manual operator decision
```

## Fencing Tokens

Leases alone are not enough when workers perform external side effects.

Example failure:

```text
worker A claims flow with lease until t=30
worker A stalls
lease expires
worker B claims flow and continues
worker A wakes up and tries to write stale result
```

Every claim should increment and return a fencing token:

```text
claim 1 -> token 41
claim 2 -> token 42
```

Then transition/complete can require the token:

```text
FLOW.COMPLETE checkout:123 TOKEN 42 ...
```

If worker A submits token 41 after worker B has token 42, FerricStore rejects
it:

```text
["err", "stale_token", 42]
```

Applications can also pass fencing tokens to downstream systems that support
monotonic versions.

## Timers

Timers are represented by a durable due index:

```text
flow_due:{type}:{state}:p{priority}
  member = flow id
  score = next_run_at_ms
```

PubSub can wake workers quickly:

```text
flow_due:{type}:{state}:p{priority}
flow_changed:{id}
```

But PubSub is not the source of truth.

Correctness rule:

```text
PubSub can be lossy.
The due index cannot be lossy.
```

Workers should use both:

```text
fast path:
  subscribe to due/type notifications
  claim immediately when notified

correctness path:
  periodic FLOW.CLAIM_DUE polling
  catches missed notifications and node restarts
```

A timer watcher can watch the nearest due timestamp per type/shard and publish
wakeup events when due, but claim correctness must still come from
`FLOW.CLAIM_DUE`.

## Backpressure

Flow should handle producer/consumer imbalance by making claiming explicit and
bounded.

```text
producer can create 1M flows
workers claim only N at a time
unclaimed flows remain durable
```

Controls:

```text
limit per claim
lease duration
max in-flight per worker
max in-flight per type
priority
next_run_at scheduling
retry backoff
application-defined quotas
pause/resume per type or application-defined scope
dead-letter state
```

This is different from a naive queue where consumers can be flooded with work.
Flow makes the durable store absorb the gap.

## Idempotency

Flow should treat idempotency as a first-class feature.

For the MVP, idempotency is based on the flow id. The flow id is the
idempotency key.

Common API pattern:

```text
FLOW.CREATE checkout:123 ... NX
```

`NX` means "create only if this flow id does not exist". If the id already
exists, FerricStore must not modify the record, due index, history, or payload.

Return shapes:

```text
created:
  ["ok", "created", id]

already active:
  ["ok", "exists", id, state, "active"]

already completed:
  ["ok", "exists", id, "completed", result_ref]

already failed:
  ["ok", "exists", id, "failed", error_ref]
```

This is intentionally not a conflict by default. For idempotent APIs, a duplicate
request should be able to discover the current/result state and return the same
business answer.

If a client wants strict create semantics, add:

```text
FLOW.CREATE <id> ... STRICT_NX
```

Then an existing id returns:

```text
["err", "exists", id, current_state]
```

Future versions may add a separate idempotency key distinct from flow id:

```text
FLOW.CREATE <id> IDEMPOTENCY_KEY <key> ...
```

but that is not MVP. The MVP rule is simple:

```text
one flow id = one idempotent operation
```

This is valuable for:

```text
payment retries
webhook duplicate delivery
API client retries
background job retries
exactly-once-enough business operations
```

## Payload And State Data

Flow should be explicit about how data moves between states. Payload handling has
large effects on throughput, disk usage, replay/debuggability, and schema
evolution.

The recommended model has three data zones:

```text
input payload:
  immutable original request/data

working data:
  mutable state data changed by transitions

result/error:
  terminal output or failure details
```

Flow metadata should stay compact and refer to large bodies by key/ref:

```text
flow_meta:
  state
  schema_version
  data_version
  payload_ref
  working_data_ref
  result_ref
  error_ref
```

### Immutable Input Payload

The original input payload should be immutable.

```text
flow_payload:{id}
  original create payload
```

Do not rewrite it on every transition. This preserves idempotency/debug value:

```text
what did the producer ask for?
```

If the payload is small, it can be stored inline in `flow_meta`. If it is large,
store it as a cold Bitcask body or Flow body ref.

### Payload Return Contract

Flow should feel like one logical work item to clients, similar to queues or
workflow engines: a worker should not have to guess whether payload data is part
of the Flow response. The default read/claim contract should therefore try to
return payload data with the Flow record, but only up to a configured safety cap.

Recommended default:

```text
flow_payload_return_max_bytes = 64 KiB
```

This cap protects both the client and server from accidental giant responses.
For example, without a cap:

```text
FLOW.CLAIM_DUE type WORKER w1 LIMIT 100
100 claimed flows * 2 MiB payload = 200 MiB response
```

That can destroy p99 latency, memory, and TCP buffering. The cap makes the
default ergonomic for normal work items while preventing a client from hurting
itself by claiming too much large work at once.

Response semantics:

```text
payload_ref exists and payload_size <= max_bytes:
  return payload inline

payload_ref exists and payload_size > max_bytes:
  return payload_ref
  return payload_omitted = true
  return payload_size when known

payload_ref exists but value is missing:
  return payload_ref
  return payload = nil
  return payload_missing = true

payload read fails:
  return payload_ref
  return payload_error
  do not roll back a successful claim
```

Command-level overrides:

```text
FLOW.GET id PAYLOAD MAXBYTES 2097152
FLOW.GET id NOPAYLOAD

FLOW.CLAIM_DUE type WORKER w1 LIMIT 100 PAYLOAD MAXBYTES 2097152
FLOW.CLAIM_DUE type WORKER w1 LIMIT 100 NOPAYLOAD
```

The global default should be safe, but applications can opt into Temporal-style
larger payloads explicitly. Operators can tune the cap per deployment:

```text
config :ferricstore,
  flow_payload_return_max_bytes: 64 * 1024
```

### Mutable Working Data

Working data is the data that evolves across states:

```text
payment_id
approval_id
external_status
validated_fields
progress
intermediate refs
```

Small working data can live inline in `flow_meta`. Large working data should be
stored by ref:

```text
flow_data:{id}:{data_version}
```

Avoid rewriting large blobs on every transition. Prefer:

```text
small metadata patch
new data ref only when needed
history event with compact patch/ref
```

### Result And Error Data

Terminal result and large errors should be separate refs:

```text
flow_result:{id}
flow_error:{id}:{attempt}
```

This keeps terminal metadata cheap:

```text
state=completed
result_ref=flow_result:...
```

`FLOW.GET id` should return the payload by default when it is under the payload
return cap. Larger bodies remain available through `payload_ref` or explicit
`PAYLOAD MAXBYTES`.

### Patch vs Replace

Transition data changes must have explicit semantics.

Recommended command shapes:

```text
FLOW.TRANSITION <id> FROM <state> TO <state> PATCH <bytes>
FLOW.TRANSITION <id> FROM <state> TO <state> REPLACE_DATA <bytes>
FLOW.TRANSITION <id> FROM <state> TO <state> DATA_REF <ref>
```

MVP should avoid magical deep merge. Use one of:

```text
REPLACE_DATA:
  replace working data with supplied bytes/ref

PATCH:
  shallow merge for structured map/JSON only, or app-defined patch stored as ref

DATA_REF:
  point metadata to a new large working-data body
```

Whatever is chosen must be deterministic inside the state-machine apply.

### Data Version Guards

Flow should support optional app-owned version guards:

```text
schema_version
data_version
```

Commands can include:

```text
EXPECT_DATA_VERSION <n>
SET_DATA_VERSION <n>
```

If the stored version does not match:

```text
["err", "wrong_data_version", current_version]
```

This prevents stale workers from overwriting newer working data.

The application owns the meaning of these versions. FerricStore only enforces
atomic compare/set.

### Claim Payload Hydration

`FLOW.CLAIM_DUE` should return enough for the worker to start work without an
extra fetch in the common case. By default, claimed records should include
payload data up to `flow_payload_return_max_bytes`.

Large payloads are not an error. They are deliberately omitted from the claim
response and returned as refs:

```text
payload_ref = "flow_payload:checkout:123"
payload_omitted = true
payload_size = 7340032
```

The worker can then fetch those large payloads with normal KV `GET`/`MGET` when
it is ready to process them. This keeps `CLAIM_DUE LIMIT 100` safe while keeping
the client model simple.

### History Stores Facts And Refs

History events should not duplicate full payloads by default.

Good:

```text
event=transitioned
from=payment_pending
to=paid
patch_ref=flow_event_body:...
result_ref=flow_result:...
```

Bad:

```text
event=transitioned
full_payload=500KB
full_result=500KB
```

History should store compact facts and refs. Old history can then be compressed
or archived without duplicating large bodies many times.

### Data Ref Cleanup

Transitions that create new data refs must have retention rules.

Options:

```text
keep_latest:
  delete older working data refs after successful transition

keep_last_n:
  retain last N working data versions for debugging

archive_with_history:
  move old data refs into history archive retention

keep_until_terminal:
  retain refs while active, clean up after completion TTL
```

Cleanup must be tied to history/current-record retention so refs do not leak.

### Comparison With Temporal

Temporal stores workflow execution history. Inputs, signals, activity results,
timers, retries, and markers become part of the durable history used to replay
workflow code.

FerricStore Flow should not replay application code. It stores explicit current
state plus refs:

```text
current compact metadata
immutable payload ref
mutable working-data ref/version
terminal result/error refs
compact history events
```

That usually keeps the hot/current payload smaller and more controllable than a
workflow-code replay history. The tradeoff is that the application owns state
transition logic and schema evolution.

## Terminal Archive Storage

When a flow reaches a terminal state, Flow should keep a compact terminal record
searchable in FlowMetaStore and move cold/heavy data into archive storage.

Terminal states:

```text
completed
failed
cancelled
deadlettered
```

Terminal transition must atomically update the live Flow metadata:

```text
set terminal state
store result/error refs
remove due index entry
remove inflight/worker/scope indexes
release lease/quota counters
append terminal history event
set archive_status = pending
set archive_due_at_ms
set terminal retention/expiry metadata
```

Heavy archive work should happen asynchronously:

```text
payload
working data versions
large errors
old history events
history bodies
```

Do not compress/archive these inside the terminal transition hot path.

### Separate Archive Bitcask

Archive bodies should use their own Bitcask directory/namespace, separate from
active Flow bodies.

Suggested layout:

```text
data/
  flow/
    meta/
      shard_0.lmdb
      shard_1.lmdb

    bodies/
      shard_0/
      shard_1/

    archives/
      shard_0/
      shard_1/
```

Reasons:

```text
archive blobs are cold
archive blobs are usually compressed and larger
archive retention differs from active body retention
archive compaction can be throttled separately
archive disk accounting is easier
archive storage can later move to cheaper disk/object storage
```

FlowMetaStore stores searchable archive metadata:

```text
archive_status
archive_ref
archive_codec
archive_size_bytes
archive_checksum
archive_created_at_ms
archive_expires_at_ms
```

Archive Bitcask stores opaque compressed bodies. Archives should be compressed
by default:

```text
codec: zstd
encoding: etf_v1 or msgpack_v1
```

Archive body contents can include:

```text
old history events
large payload
working data versions
result/error bodies
debug metadata
```

FlowMetaStore should record both compressed and uncompressed sizes:

```text
archive_size_bytes
archive_uncompressed_bytes
```

Archive Bitcask key shape:

```text
flow_archive_body:{internal_flow_id}:{archive_seq}
```

The archive ref should be logical enough to support future storage backends:

```elixir
%{
  store: :flow_archive_bitcask,
  shard: 3,
  key: "flow_archive_body:123:1",
  codec: :zstd,
  encoding: :etf_v1,
  bytes: 92_300,
  uncompressed_bytes: 860_000,
  checksum: "..."
}
```

Later backends can use the same Flow API:

```text
store: :cold_disk
store: :s3
store: :object_storage
```

### Archive Pointer Ordering

Archive body writes and FlowMetaStore pointer updates must follow this order:

```text
1. write compressed archive body to archive Bitcask
2. fsync/commit according to archive durability policy
3. update FlowMetaStore archive_ref/archive_status metadata
4. remove old active refs if policy allows
```

Do not update FlowMetaStore before the archive body exists.

Safe failure behavior:

```text
crash before FlowMetaStore pointer update:
  archive body may be orphaned
  flow still points to old active refs
  archiver can retry
  orphan sweeper can delete unreferenced archive bodies

crash after FlowMetaStore pointer update:
  archive_ref is durable
  archive body is readable
  old active refs can be cleaned according to policy
```

The rule:

```text
orphan archive body is acceptable
dangling archive pointer is not acceptable
```

### Archive Commit Command

The archiver should use a native atomic command to publish the archive pointer:

```text
{:flow_archive_commit, id, archive_ref, archive_meta, old_refs_to_release}
```

This command verifies:

```text
flow is still terminal
archive_status is pending or retryable
archive generation/sequence is expected
archive body exists or has been verified by the archiver
```

Then atomically:

```text
sets archive_status = archived
sets archive_ref
updates archive metadata
updates archive expiry index
marks old refs releasable/deleted according to policy
appends history event "archived"
```

### Archive Reads

Compact reads do not load archive bodies:

```text
FLOW.GET id
  reads FlowMetaStore
  returns terminal state + archive_ref/result summary
  does not read or decompress archive body
```

Full reads resolve archive refs:

```text
FLOW.GET id FULL
FLOW.HISTORY id FULL
```

Read path:

```text
1. lookup flow/archive metadata in FlowMetaStore
2. read archive body from archive Bitcask
3. verify checksum if configured
4. decompress
5. return requested payload/result/history range
```

### Archive Cleanup

Archive cleanup is driven by FlowMetaStore indexes, not Bitcask key scans.

```text
flow_archive_expiry
  score = archive_expires_at_ms
  member = internal_flow_id
```

Sweeper:

```text
1. read expired archive refs from FlowMetaStore
2. delete archive Bitcask body
3. delete/update FlowMetaStore archive metadata
4. remove expiry index entry
```

Orphan sweeper:

```text
1. scan archive Bitcask bodies by local directory/index
2. verify whether FlowMetaStore references them
3. delete unreferenced bodies older than safety window
```

The FlowMetaStore remains the source of searchable truth. Archive Bitcask is
only opaque body storage.

## History

Every state-changing command should append a compact durable event.

Example event stream:

```text
created
claimed
transitioned payment_pending -> payment_charged
transitioned payment_charged -> email_pending
retried email_pending
completed
```

History supports:

```text
debugging
audit
LiveView progress
operational inspection
replay of status views
dead-letter analysis
```

History should be stored as a FerricStore stream per flow:

```text
flow_history:{id}
  stream id -> compact event fields
```

Example:

```text
flow_history:checkout:123
  1714660000000-0 -> event=created state=payment_pending
  1714660005000-0 -> event=claimed worker=worker-1 token=12
  1714660010000-0 -> event=transitioned from=payment_pending to=paid
  1714660020000-0 -> event=completed result_ref=flow_result:checkout:123
```

The history event must be appended inside the same native Flow command that
changes state.

Bad:

```text
FLOW.TRANSITION checkout:123 ...
XADD flow_history:checkout:123 ...
```

Good:

```text
FLOW.TRANSITION checkout:123 payment_pending paid
  state-machine apply:
    verify state
    update current flow record
    update due index
    append history event
    publish wakeup metadata
```

This guarantees the current state and the history trail do not diverge.

### History Inspection Commands

Users need first-class commands to investigate flow history. They should not
need to know the internal stream key.

```text
FLOW.HISTORY <id> [COUNT <n>] [START <stream_id>] [END <stream_id>] [REV] [FULL]
```

Examples:

```text
FLOW.HISTORY checkout:123
FLOW.HISTORY checkout:123 COUNT 50
FLOW.HISTORY checkout:123 REV COUNT 20
FLOW.HISTORY checkout:123 START 1714660000000-0 END +
FLOW.HISTORY checkout:123 FULL
```

Default `FLOW.HISTORY` returns compact events:

```text
[
  ["1714660000000-0", "event", "created", "state", "payment_pending"],
  ["1714660005000-0", "event", "claimed", "worker", "worker-1", "token", "12"],
  ["1714660010000-0", "event", "transitioned", "from", "payment_pending", "to", "paid"],
  ["1714660020000-0", "event", "completed", "result_ref", "flow_result:checkout:123"]
]
```

Large values should be stored by reference:

```text
payload_ref
result_ref
error_ref
event_body_ref
```

`FULL` resolves those references and returns the large bodies. Without `FULL`,
history inspection stays cheap and does not pull large cold values from NVMe.

```text
FLOW.EXPLAIN <id> [COUNT <n>] [FULL]
```

`FLOW.EXPLAIN` is a developer/operator convenience command. It returns the
current flow record plus recent history:

```text
[
  "current", [
    "id", "checkout:123",
    "type", "checkout",
    "state", "email_pending",
    "attempts", "2",
    "next_run_at_ms", "1714660900000",
    "lease_owner", "",
    "fencing_token", "13",
    "updated_at_ms", "1714660020000"
  ],
  "history", [
    ["1714660000000-0", "event", "created", "state", "payment_pending"],
    ["1714660005000-0", "event", "claimed", "worker", "worker-1"],
    ["1714660010000-0", "event", "transitioned", "from", "payment_pending", "to", "paid"]
  ]
]
```

History retention should be configurable separately from the flow current
record. For example:

```text
active flow: no eviction
completed result: 30 day TTL
history: 7 day TTL or max length
large event body: cold value ref
```

### History Storage And Retention Logic

Flow history should use FerricStore Streams as an append-only event log, but the
Flow layer must define the retention policy. Raw stream behavior is not enough
because flow history has lifecycle semantics.

Internal keys:

```text
flow:{id}
  current flow record

flow_history:{id}
  stream entries for the flow

flow_history_meta:{id}
  optional compact metadata:
    first_event_id
    last_event_id
    event_count
    retention_policy
    expires_at_ms

flow_event_body:{id}:{event_id}
  optional cold body for large payload/result/error details
```

Stream entry fields should stay compact:

```text
event          created | claimed | transitioned | retried | completed | failed | cancelled
state          current state after event
from           previous state, for transitions
to             next state, for transitions
worker         lease owner, for claims
token          fencing token, for claims/transitions
attempt        attempt count
run_at_ms      next due time, if scheduled
error_ref      cold body ref, if large
result_ref     cold body ref, if large
body_ref       generic cold body ref, if needed
at_ms          wall/HLC timestamp
```

History event IDs should be monotonic and deterministic inside the shard. The
implementation can use existing stream IDs/HLC-derived IDs, but the state
machine must generate them as part of the same Flow apply that changes the
current record.

Retention should support three modes:

```text
history: :none
  do not keep history except maybe last_error/last_transition in flow record

history: {:maxlen, n}
  keep last N events using stream trimming semantics

history: {:ttl, ms}
  keep events until event timestamp + ttl

history: {:maxlen_and_ttl, n, ms}
  keep last N events and also expire old events

history: :forever
  keep until the whole flow is explicitly deleted
```

Recommended defaults:

```text
active flow:
  keep history

completed flow:
  keep current result for completion_ttl
  keep history for history_ttl, usually same or longer

failed/dead flow:
  keep history longer than normal completion for debugging

cancelled flow:
  keep history for normal completion_ttl
```

Example:

```text
FLOW.CREATE checkout:123 ... TTL 7d HISTORY_TTL 30d HISTORY_MAXLEN 500
```

This means:

```text
active current record lives up to 7 days unless completed/cancelled/failed
completed result may get its own TTL
history entries are kept up to 30 days
only the latest 500 events are retained
large event bodies follow the history TTL unless separately configured
```

Important: history TTL should not accidentally delete active-flow history.

Rules:

```text
if flow is active:
  do not trim by age below the minimum needed to explain the active lifecycle
  maxlen trimming is allowed if configured

if flow reaches terminal state:
  set history expiration based on terminal retention policy
```

Deletion/trim implementation options:

```text
maxlen trim:
  use stream trim semantics during the same Flow apply when appending an event

ttl trim:
  maintain a cleanup task that scans terminal histories by expiration time
  or add history-expiry entries to a sorted set:
    flow_history_expiry score=expires_at_ms member=id
```

For exact TTL cleanup, use a sorted set:

```text
flow_history_expiry
  score = history_expires_at_ms
  member = flow id
```

Cleanup command/process:

```text
FLOW.HISTORY.SWEEP now limit
  find expired history ids from flow_history_expiry
  delete/trim flow_history:{id}
  delete flow_event_body:{id}:*
  update/remove flow_history_meta:{id}
```

This sweep does not affect current active state unless the current flow record is
also expired by its own lifecycle.

Large body refs must be retained consistently:

```text
if stream event is trimmed:
  delete its flow_event_body ref

if history expires:
  delete all event bodies for that flow

if FULL is requested after body expiry:
  return the compact event with body_missing=true
```

The compact history stream should remain useful even when large bodies are gone.
For example, a `failed` event should keep:

```text
event=failed
state=failed
attempt=5
error_class=Timeout
error_ref=flow_event_body:...
```

If the body expires, operators still see the failure type and lifecycle.

### History Compression And Archiving

Recent history should remain uncompressed stream entries. Old history can be
sealed into compressed archive blocks.

Do not compress on every transition. The hot path should stay:

```text
update current record
update due index
append compact stream event
return
```

Compression should be a background maintenance task for cold history.

Recommended layout:

```text
flow_history:{id}
  recent uncompressed stream entries
  plus optional archive marker entries

flow_history_archive:{id}:{archive_seq}
  compressed block containing old stream events

flow_history_meta:{id}
  first_event_id
  last_event_id
  event_count
  recent_event_count
  archive_count
  oldest_archive_id
  newest_archive_id
  archive_policy
  history_expires_at_ms
```

Archive block metadata:

```elixir
%{
  id: "flow_history_archive:checkout:123:000001",
  flow_id: "checkout:123",
  archive_seq: 1,
  from_event_id: "1714660000000-0",
  to_event_id: "1714660100000-0",
  event_count: 500,
  codec: "zstd",
  encoding: "etf_v1",
  uncompressed_bytes: 86_000,
  compressed_bytes: 9_200,
  created_at_ms: 1_714_660_200_000
}
```

Archive block value:

```text
magic/version
archive metadata header
encoded event list
checksum
```

The first version can use:

```text
encoding = ETF or MessagePack
codec = zstd
```

Later, if history volume is high, use a custom binary layout:

```text
event_ids column
event_type enum column
state/from/to string table
worker/token/attempt fields
refs table
```

That is optional. ETF + zstd is good enough for v1.

#### Archive Policy

Archive policy should be configurable per FlowStore/type:

```text
archive_after_count: 100
archive_after_age_ms: 86_400_000
archive_min_block_events: 100
archive_max_block_events: 1000
archive_codec: zstd
```

Meaning:

```text
keep the newest 100 events as normal stream entries
or keep events newer than 24h
archive older events in blocks of 100-1000 events
```

Recent operational history stays cheap:

```text
FLOW.EXPLAIN id
FLOW.HISTORY id COUNT 50
```

Old audit/debug history is still available:

```text
FLOW.HISTORY id START old_id END old_id FULL
```

#### Archive Marker

When old stream entries are archived, leave a marker in the stream:

```text
event=history_archived
archive_ref=flow_history_archive:checkout:123:000001
from_event_id=1714660000000-0
to_event_id=1714660100000-0
event_count=500
codec=zstd
encoding=etf_v1
```

The marker lets `FLOW.HISTORY` show that older events exist without reading the
archive body. With `FULL`, FerricStore can resolve the marker and return the
decompressed events.

#### Archiver Flow

A background archiver should do the expensive work outside the write hot path.

Candidate selection:

```text
flow_history_archive_due
  sorted set
  score = next_archive_at_ms
  member = flow id
```

When a Flow command appends history and crosses a threshold, it should update
archive metadata/due state cheaply:

```text
if recent_event_count > archive_after_count:
  ZADD flow_history_archive_due now flow_id
```

The background archiver:

```text
1. claim archive work from flow_history_archive_due
2. read old stream entries outside the recent window
3. encode and compress an archive block
4. write flow_history_archive:{id}:{seq}
5. atomically replace archived stream entries with an archive marker
6. update flow_history_meta:{id}
7. delete large event bodies that were moved into the archive, if embedded
```

Step 5 must be atomic. The archive value can be written first, but the stream
trim/marker/meta update must happen as one native command:

```text
{:flow_history_archive_commit, id, archive_meta, marker_event, trim_range}
```

This command verifies:

```text
the source stream still has the expected from/to event ids
the archive sequence is still next
the archive ref exists
```

Then it:

```text
appends archive marker
trims/deletes archived stream entries
updates flow_history_meta
removes/reschedules archive_due entry
```

If the process crashes before commit:

```text
archive block may be orphaned
stream still has original events
safe to retry
orphan cleanup can delete archive blocks not referenced by meta/markers
```

If it crashes after commit:

```text
stream marker points to archive block
meta is updated
history remains readable
```

#### Reading Archived History

Default reads should avoid archive decompression:

```text
FLOW.HISTORY id COUNT 50
  reads recent stream entries only
```

Range reads that cross an archive marker:

```text
without FULL:
  return archive marker summary

with FULL:
  read archive_ref
  decompress block
  filter events by requested START/END
  merge with recent stream events
```

`FLOW.EXPLAIN` should not decompress archives by default. It is meant for recent
debugging.

#### Retention With Archives

History expiry must delete both stream entries and archive blocks.

```text
if history expires:
  delete flow_history:{id}
  delete flow_history_meta:{id}
  delete all flow_history_archive:{id}:*
  delete remaining flow_event_body:{id}:*
  remove flow_history_expiry entry
  remove flow_history_archive_due entry
```

If only maxlen trimming applies:

```text
keep newest N recent entries
archive or delete older entries depending on archive policy
```

For terminal flows, compression can run soon after completion:

```text
completed/failed/cancelled:
  archive old history immediately or within a short delay
  keep only last N recent events uncompressed
  rely on history_ttl for final deletion
```

For active flows, be conservative:

```text
keep recent window uncompressed
archive only events safely outside the recent/debug window
never remove all explanatory history unless history policy says :none
```

## LiveView Integration

Flow is a strong match for Phoenix/LiveView.

LiveView can query:

```text
FLOW.GET checkout:123
FLOW.HISTORY checkout:123 COUNT 50
```

and subscribe to:

```text
flow_changed:checkout:123
```

On PubSub notification, LiveView reads the current state from FerricStore and
rerenders status/progress.

The UI remains application code. FerricStore only provides durable status and
notifications.

## Observability

Flow needs first-class observability. If users compare it to workflow systems,
they will expect to answer these questions quickly:

```text
what is running?
what is stuck?
what failed?
why did it fail?
who owns it?
when will it retry?
how much backlog exists?
which flow types are unhealthy?
```

Observability should have four layers:

```text
durable visibility:
  query current flow state and history from FerricStore

telemetry:
  emit metrics/events for dashboards and alerts

tracing:
  connect app execution spans to flow ids and fencing tokens

operator commands:
  inspect, list, explain, pause, resume, retry, cancel
```

### Durable Visibility

Temporal's strongest observability feature is that workflow history is durable
and queryable. Flow should provide the same kind of practical visibility through
current records, history streams, and lightweight indexes.

Required commands:

```text
FLOW.GET <id>
FLOW.HISTORY <id> ...
FLOW.EXPLAIN <id> ...
FLOW.INFO <type>
```

Additional operator commands should be considered:

```text
FLOW.LIST <type> [STATE <state>] [COUNT <n>] [CURSOR <cursor>]
FLOW.STUCK <type> [OLDER_THAN <ms>] [COUNT <n>]
FLOW.DUE <type> [COUNT <n>]
FLOW.INFLIGHT <type> [COUNT <n>]
FLOW.FAILURES <type> [COUNT <n>]
FLOW.WORKERS <type>
```

These commands need indexes. Do not implement them by scanning Bitcask.

Suggested indexes:

```text
flow_by_state:{type}:{state}
  sorted set
  score = updated_at_ms
  member = flow id

flow_due:{type}:{state}:p{priority}
  priority-aware due sorted sets
  score remains next_run_at_ms
  claim can target specific states and drains high priority before normal before low

flow_inflight:{type}
  sorted set
  score = lease_until_ms
  member = flow id

flow_failures:{type}
  sorted set
  score = failed_at_ms
  member = flow id

flow_by_worker:{type}:{worker_id}
  sorted set
  score = lease_until_ms
  member = flow id
```

Index updates must happen inside the same native Flow command as the state
change. For example, `FLOW.CLAIM_DUE` should update:

```text
flow record
due index
inflight index
worker index
history
```

atomically in one state-machine apply.

For multi-shard FlowStore, visibility commands should distinguish counters from
lists.

Counter-style reads:

```text
FLOW.INFO <type>
FLOW.DUE <type>
FLOW.INFLIGHT <type>
FLOW.FAILURES <type>
```

should use shard-local counters and aggregate them:

```text
read counters from each shard
sum results
avoid scanning flow records
```

List-style reads:

```text
FLOW.LIST <type> STATE <state> COUNT <n>
FLOW.STUCK <type> ...
```

should fan out to shard-local indexes and merge results. The cursor must carry
per-shard positions:

```text
cursor = {
  shard_0: "...",
  shard_1: "...",
  shard_2: "..."
}
```

Large operator listings are not on the hot claim path, so fan-out + merge is
acceptable there. `FLOW.CLAIM_DUE` should use worker shard assignment or bounded
fan-out instead.

### Telemetry Events

FerricStore should emit `:telemetry` events for all important Flow operations.

Recommended event names:

```elixir
[:ferricstore, :flow, :create, :stop]
[:ferricstore, :flow, :claim_due, :stop]
[:ferricstore, :flow, :transition, :stop]
[:ferricstore, :flow, :retry, :stop]
[:ferricstore, :flow, :complete, :stop]
[:ferricstore, :flow, :fail, :stop]
[:ferricstore, :flow, :cancel, :stop]
[:ferricstore, :flow, :lease, :expired]
[:ferricstore, :flow, :claim, :stale_token]
[:ferricstore, :flow, :claim, :wrong_state]
[:ferricstore, :flow, :history, :archive, :stop]
[:ferricstore, :flow, :sweep, :stop]
```

Measurements:

```elixir
%{
  duration_ms: non_neg_integer(),
  count: non_neg_integer(),
  claimed: non_neg_integer(),
  skipped: non_neg_integer(),
  bytes: non_neg_integer(),
  backlog: non_neg_integer()
}
```

Metadata:

```elixir
%{
  instance: MyApp.FlowStore,
  flow_type: "checkout",
  flow_id: "checkout:123",
  from_state: "payment_pending",
  to_state: "paid",
  worker_id: "node-a:worker-1",
  result: :ok | :error,
  reason: :wrong_state | :leased | :stale_token | :missing | nil,
  fencing_token: 42
}
```

Be careful with metric cardinality:

```text
flow_type is usually safe
state is usually safe
worker_id can be high cardinality depending on deployment
flow_id should be tracing/log metadata, not a Prometheus label
```

### Metrics

Core metrics:

```text
flow_created_total{type}
flow_claimed_total{type}
flow_completed_total{type}
flow_failed_total{type}
flow_retried_total{type}
flow_cancelled_total{type}

flow_command_duration_ms{type,command}
flow_transition_duration_ms{type,from,to}
flow_backlog{type}
flow_due_count{type}
flow_inflight_count{type}
flow_failed_count{type}
flow_stuck_count{type}

flow_claim_batch_size{type}
flow_claim_skipped_total{type,reason}
flow_lease_expired_total{type}
flow_stale_token_total{type}
flow_wrong_state_total{type}

flow_history_events_total{type}
flow_history_archive_blocks_total{type}
flow_history_archive_bytes{type}
flow_history_trimmed_events_total{type}
```

Latency metrics:

```text
flow_age_ms:
  now - created_at_ms

flow_time_in_state_ms:
  now - state_entered_at_ms

flow_schedule_lag_ms:
  claim_time_ms - next_run_at_ms

flow_execution_lag_ms:
  transition_time_ms - claim_time_ms

flow_end_to_end_ms:
  completed_at_ms - created_at_ms
```

`flow_schedule_lag_ms` is especially important. It tells whether the system is
falling behind even if consumers are still working.

### Tracing

Flow commands should make OpenTelemetry integration easy.

Every claimed flow should carry correlation metadata:

```text
flow_id
flow_type
state
worker_id
fencing_token
attempt
claim_id
```

Workers can attach this to spans:

```elixir
OpenTelemetry.Tracer.with_span "checkout.charge_card" do
  OpenTelemetry.Span.set_attributes([
    {"ferricstore.flow_id", flow.id},
    {"ferricstore.flow_type", flow.type},
    {"ferricstore.flow_state", flow.state},
    {"ferricstore.fencing_token", flow.fencing_token},
    {"ferricstore.attempt", flow.attempts}
  ])
end
```

FerricStore itself should trace command execution around:

```text
claim_due
transition
history archive
sweep
```

but should avoid adding large payload/result bodies to spans.

### Logs

Structured logs should be emitted for unusual events, not every successful
transition by default.

Good log events:

```text
stale token rejected
wrong state transition rejected
lease expired
flow moved to failed/dead state
history archive failed
due sweep lag above threshold
```

Every log should include:

```text
instance
flow_id
flow_type
state
worker_id
fencing_token
attempt
reason
```

### Operator Commands

To approach workflow-system ergonomics, Flow needs operator controls, not only
metrics.

Suggested commands:

```text
FLOW.PAUSE <type>
FLOW.RESUME <type>
FLOW.RETRY_NOW <id>
FLOW.RELEASE <id> [TOKEN <token>]
FLOW.CANCEL <id> REASON <bytes>
FLOW.DEADLETTER <id> REASON <bytes>
```

`FLOW.PAUSE` should prevent new claims for that type but not block reads or
manual inspection.

MVP pause semantics are drain-by-default:

```text
FLOW.PAUSE <type>
  prevents new FLOW.CLAIM_DUE claims for that type
  does not revoke existing leases
  does not block FLOW.TRANSITION / COMPLETE / RETRY for already claimed flows
  does not change due indexes
  does not stop reads, history, explain, or manual cancel
```

This is the least surprising operational behavior. A pause stops taking new
work but lets in-flight work finish or retry normally.

During pause:

```text
FLOW.CLAIM_DUE <type> ...
  returns ["ok", "paused", []]

lease expiry still works:
  if a worker crashes during pause, the flow lease expires
  the flow remains due/inflight-expired
  it is not reclaimed until FLOW.RESUME or manual intervention
```

Manual controls remain allowed:

```text
FLOW.RELEASE <id> [TOKEN <token>]
FLOW.RETRY_NOW <id>
FLOW.CANCEL <id> REASON <bytes>
FLOW.DEADLETTER <id> REASON <bytes>
```

`FLOW.RESUME <type>` re-enables normal claims. Any due flows whose leases expired
during the pause become claimable again.

Future options can add stronger behavior, but they are explicit non-MVP
extensions:

```text
FLOW.PAUSE <type> DRAIN
  same as MVP/default

FLOW.PAUSE <type> RELEASE_INFLIGHT
  release current leases and make work claimable after resume

FLOW.PAUSE <type> CANCEL_INFLIGHT
  terminally cancel matching active flows
```

Only `DRAIN` is required for MVP.

### Dashboards

A useful built-in dashboard or LiveView admin page should be possible from the
same primitives:

```text
type overview:
  backlog
  due count
  inflight count
  failed count
  schedule lag p50/p95/p99
  completion rate
  retry rate

flow detail:
  current state
  lease owner/token
  next retry time
  attempts
  recent history
  archived history markers
  payload/result refs
```

This should not require a separate workflow server. It can be built as a Phoenix
admin UI over `FLOW.INFO`, `FLOW.LIST`, `FLOW.EXPLAIN`, and telemetry metrics.

## Cross-Language Workers

Because Flow commands can be exposed over RESP, non-Elixir workers can
participate.

Examples:

```text
Python worker:
  FLOW.CLAIM_DUE image worker-py-1 30000 10
  process image
  FLOW.COMPLETE image:123 RESULT ...

Go worker:
  FLOW.CLAIM_DUE webhook worker-go-7 30000 100
  call external API
  FLOW.RETRY webhook:event_456 ERROR ... RUN_AT ...
```

The application owns serialization and business logic. FerricStore owns the
atomic state changes.

## Failure Handling

### Worker Crash After Claim

```text
flow is leased
worker crashes
lease expires
another worker claims
fencing token increments
stale worker results are rejected
```

### Crash After Side Effect Before Transition

This cannot be solved purely by storage. The application should use idempotent
external operations where possible.

FerricStore helps by storing:

```text
idempotency key
attempt count
fencing token
last known state
retry schedule
history
```

### Missed PubSub Wakeup

```text
worker misses wakeup
periodic claim_due scan finds due flow
```

### Node Restart

```text
flow records recover from Bitcask/Raft
due index recovers from durable sorted set data
history recovers from streams
leases expire naturally
workers resume claim_due
```

## Memory Policy

FlowStore should not behave like a hot cache by default.

Recommended policy:

```text
metadata hot
large payload/result cold
no eviction for active flow state
TTL cleanup for completed/cancelled/failed flows
small hot_cache_max_value_size
```

This allows applications to keep large amounts of operational state for days
without turning application RAM into the storage budget.

## Scale And Disk-Backed Active Flows

Flow must be designed for many active flows without requiring every flow body,
result, and history event to stay in RAM.

The target model is:

```text
RAM:
  compact routing/claim metadata
  keydir offsets
  due/state/inflight index entries
  small recent status fields

NVMe/Bitcask:
  full flow records
  payloads
  results
  large errors
  history archives
  cold history bodies
```

Do not design Flow as:

```text
1M active flows -> 1M large Elixir maps in ETS
```

Design it as:

```text
1M active flows -> compact metadata/indexes in ETS + bodies on disk
```

Important: with FerricStore's current Bitcask/keydir model, disk size is not the
only limit. Cold values move bytes out of RAM, but every live key still needs
metadata in the keydir, and Flow indexes also need metadata.

So the limits are:

```text
disk:
  payloads
  results
  large errors
  archived history
  old Bitcask versions before merge

RAM:
  keydir entry per live key
  due/state/inflight index entries
  hot metadata needed for claim/status
  process/ETS overhead
```

To make disk the dominant limit, Flow needs compact key/index design and,
eventually, native or disk-backed indexes.

### What Must Be Hot

Only data needed for routing, claiming, and basic observability should be hot.

Per active flow, the hot metadata should be compact:

```text
id/key hash
type id or small type string
state id or small state string
priority
next_run_at_ms
lease_until_ms
fencing_token
attempts
updated_at_ms
Bitcask offset/ref for full record
```

This metadata allows:

```text
claim due work
list by state
detect stuck/inflight work
show compact status
load full record only when needed
```

The full record can be cold:

```text
data
result
error body
large labels
large app metadata
```

### Index Memory Budget

Each active flow may appear in multiple indexes:

```text
flow record keydir entry
one due priority index, if scheduled
one state index
one inflight/worker index, if claimed
history metadata, if history enabled
```

This means memory grows with:

```text
active flow count
number of secondary indexes
key/id size
metadata tuple size
history retention settings
```

The FlowStore should expose explicit limits:

```text
max_active_flows
max_due_index_entries
max_state_index_entries
max_inflight_entries
max_hot_flow_metadata_bytes
max_recent_history_events_per_flow
```

When limits are approached, Flow should return backpressure errors instead of
silently exhausting VM memory:

```text
["err", "flow_capacity_exceeded", limit, current]
["err", "flow_index_pressure", index, limit, current]
```

### Compact Internal Representation

For high scale, avoid storing large repeated strings in every hot tuple.

Recommended:

```text
type registry:
  "checkout" -> small integer id

state registry per type:
  "payment_pending" -> small integer id

priority:
  small integer

flow id:
  full id stored in Bitcask record
  hot indexes may store hash + short ref where possible
```

The RESP/API still returns strings. The internal representation can be compact.

### Native Flow Indexes vs Generic Compound Entries

The MVP can use existing compound sorted sets and streams. That is simple and
consistent with FerricStore primitives.

At high active-flow counts, generic compound entries may create more ETS/keydir
overhead than a purpose-built Flow index. The design should allow a later native
index without changing the public API.

Possible native hot indexes:

```text
FlowDueIndex:
  per shard/type/priority ordered by next_run_at_ms

FlowStateIndex:
  per shard/type/state ordered by updated_at_ms

FlowInflightIndex:
  per shard/type ordered by lease_until_ms
```

Durability remains in Bitcask/Raft. The native indexes are rebuilt from durable
flow records/index records on startup if needed.

### Reducing Keydir Pressure

Because Bitcask keeps one keydir entry per live key, Flow should avoid exploding
one logical flow into too many durable keys.

Prefer:

```text
one compact flow_meta key
one payload key only when payload is large
one result key only when result is large
recent history stream with bounded length
archive blocks for old history
```

Avoid:

```text
many tiny keys per flow field
one key per small history attribute
unbounded per-flow secondary index entries
large numbers of abandoned terminal-flow keys
```

At higher scale, these are the likely optimizations:

```text
packed flow record:
  store metadata fields together under one key

integer dictionaries:
  encode type/state strings as small ids internally

short internal ids:
  map external flow id -> compact internal id

native indexes:
  keep due/state/inflight indexes in compact ordered structures

disk-backed indexes:
  move cold/large index pages to LMDB or another ordered on-disk structure
```

Until disk-backed indexes exist, Flow capacity is bounded by RAM metadata as
well as disk bytes.

### Recommended Large-Scale Architecture

If the goal is to make disk, not RAM, the practical limit for active flows, Flow
should become a Flow-specific storage subsystem rather than only a collection of
generic Bitcask keys.

Recommended large-scale split:

```text
Raft:
  ordering and replication of Flow commands

FlowMetaStore:
  disk-backed ordered metadata/index store per shard
  stores compact flow metadata and indexes

Bitcask:
  large payload/result/error/history bodies

ETS:
  bounded cache only
  nearest due heads
  hot flow metadata
  counters/pressure state
```

The FlowMetaStore can use an embedded ordered engine such as LMDB/heed or
another B-tree/LSM-style local index. This is similar in spirit to the time
series design using LMDB for metadata: keep compact ordered metadata in a
disk-backed index instead of requiring every index entry to be an ETS/keydir
object.

FlowMetaStore should own:

```text
flow_meta table:
  internal_flow_id -> compact metadata record

external_id table:
  external flow id -> internal_flow_id

due index:
  {type_id, priority, next_run_at_ms, internal_flow_id} -> nil

state index:
  {type_id, state_id, updated_at_ms, internal_flow_id} -> nil

inflight index:
  {type_id, lease_until_ms, internal_flow_id} -> nil

worker index:
  {type_id, worker_id, lease_until_ms, internal_flow_id} -> nil

history_meta table:
  internal_flow_id -> compact history metadata
```

Bitcask should own only larger bodies:

```text
flow_payload:{internal_flow_id}
flow_result:{internal_flow_id}
flow_error:{internal_flow_id}:{attempt}
flow_history_archive:{internal_flow_id}:{archive_seq}
```

This reduces keydir pressure because the number of Bitcask keys becomes
proportional to large bodies, not every metadata/index entry.

### FlowMetaStore Atomicity

FlowMetaStore updates must still be driven by Raft. The state-machine apply for
a Flow command should update the FlowMetaStore and body refs as one logical
operation.

Example `FLOW.CLAIM_DUE` apply:

```text
1. read due candidates from FlowMetaStore due index
2. validate metadata records
3. update lease owner/until and fencing token
4. remove due index entries
5. add inflight/worker index entries
6. append compact history metadata/event
7. commit FlowMetaStore transaction
8. return claimed records
```

For implementation safety, the durable source of truth must be unambiguous. Two
reasonable designs:

```text
Option A: synchronous materialization
  Raft apply commits FlowMetaStore transaction before replying.
  Simpler recovery and query semantics.
  Slightly higher apply latency.

Option B: Raft log as source, async materialization
  Raft apply updates in-memory state and enqueues FlowMetaStore writes.
  Higher throughput.
  More complex recovery because indexes must catch up before serving claims.
```

For MVP correctness, prefer Option A for Flow metadata:

```text
Raft orders the command.
FlowMetaStore transaction materializes it.
Reply after metadata/index transaction succeeds.
```

Large body writes can still be optimized separately, but metadata/index
correctness should be synchronous at first.

### Batched FlowMetaStore Writes

LMDB-style ordered stores usually allow many readers but only one concurrent
writer per environment. This fits FerricStore only if the FlowMetaStore is
shard-local and writes are batched.

Required design rules:

```text
one FlowMetaStore environment per Flow shard
one metadata writer stream per shard
no global FlowMetaStore writer
no one-transaction-per-command implementation
batch metadata/index mutations before commit
```

FerricStore already orders writes per Raft shard. Flow should use that ordering
to commit multiple Flow mutations in one FlowMetaStore transaction.

Example:

```text
Raft apply batch:
  100 FLOW.CREATE
  200 FLOW.TRANSITION
  50 FLOW.RETRY

FlowMetaStore transaction:
  put/update 350 flow_meta records
  update due indexes
  update state indexes
  update inflight indexes
  append compact history metadata
  commit once
```

This amortizes:

```text
write transaction setup
B-tree page work
fsync/msync cost
writer lock cost
```

Batching policy:

```text
commit when Raft apply batch ends
or when mutation count >= flow_meta_batch_size
or when batch age >= flow_meta_batch_window_ms
or before replying to commands that require synchronous visibility
```

Default target:

```text
flow_meta_batch_window_ms: 0-1ms
flow_meta_batch_size: 100-1000 mutations
```

The exact values should be benchmarked.

Index mutations should be grouped and written in sorted key order where
possible:

```text
flow_meta puts sorted by internal_flow_id
due index deletes/inserts sorted by {type, state, priority, next_run_at_ms, id}
state index deletes/inserts sorted by {type, state, updated_at_ms, id}
inflight index deletes/inserts sorted by {type, lease_until_ms, id}
```

This improves locality in B-tree-like engines.

The reply rule for MVP:

```text
do not reply success until the FlowMetaStore transaction containing that command
has committed.
```

Later optimization can separate Raft commit from FlowMetaStore materialization,
but that requires careful recovery and should not be part of the first
correctness-focused implementation.

### Bounded RAM Cache

With FlowMetaStore, ETS becomes a cache, not the source of scale.

ETS can hold:

```text
recently claimed flow metadata
nearest due entries per type/priority/shard
small type/state dictionaries
metrics counters
lease hot set
```

The cache should be explicitly bounded:

```text
flow_metadata_cache_max_entries
flow_metadata_cache_max_bytes
due_head_cache_per_type
```

On cache miss, read from FlowMetaStore. This makes active flow count primarily a
disk/index-size problem rather than an ETS/keydir-size problem.

### Due Head Cache

To avoid scanning disk for every claim, each shard can keep a small due-head
cache:

```text
for each {type, priority}:
  keep next K due entries in ETS
```

`FLOW.CLAIM_DUE`:

```text
1. check due-head cache
2. if empty/stale, refill from FlowMetaStore ordered due index
3. claim candidates atomically through FlowMetaStore transaction
```

The due-head cache is an optimization. If it is lost, the shard refills it from
the disk-backed due index.

### Hybrid Hot/Cold Indexes

Flow does not need every index to have the same storage policy.

Recommended split:

```text
hot / RAM optimized:
  near-due claim index
  inflight leases
  worker ownership for active claims
  small type/state dictionaries

cold / disk-backed:
  full flow metadata
  far-future due entries
  state listing indexes
  old terminal flows
  history metadata/archive indexes
```

This keeps the claim path fast without making all active flows consume RAM.

The important distinction:

```text
claim-critical set:
  flows due soon or currently leased

long-tail set:
  flows scheduled far in the future, waiting in state, terminal, or rarely read
```

The claim-critical set can be much smaller than total active flow count.

Example:

```text
10M active flows total
50K due in the next minute
20K inflight
```

RAM only needs to optimize the 50K near-due + 20K inflight working set. The
other 9.93M flows can live in disk-backed metadata/indexes.

### Native Hot Due Index

The existing FerricStore sorted set gives the correct semantics, but generic
compound-key storage may be too heavy for very large Flow due sets.

For Flow, implement a native hot due index optimized for the near-due window:

```text
FlowHotDueIndex
  key: {type_id, state_id, priority, next_run_at_ms, internal_flow_id}
  value: compact pointer/ref
  storage: ETS or native NIF structure
  bounded by max_hot_due_entries / max_hot_due_window_ms
```

This index should contain only entries that are close enough to claim soon:

```text
next_run_at_ms <= now + hot_due_window_ms
```

Far-future entries stay only in FlowMetaStore's disk-backed due index until they
enter the hot window.

Promotion loop:

```text
1. read next entries from disk-backed due index for each {type, state, priority}
2. insert entries whose next_run_at_ms <= now + hot_due_window_ms into FlowHotDueIndex
3. remember cursor/head per {type, state, priority}
4. repeat periodically or when hot index falls below low-water mark
```

`FLOW.CLAIM_DUE`:

```text
1. read due entries from FlowHotDueIndex for requested states, high priority first
2. validate/update metadata in FlowMetaStore transaction
3. remove claimed entries from FlowHotDueIndex
4. refill from FlowMetaStore if hot index is below low-water mark
```

If the process restarts:

```text
FlowHotDueIndex is empty
refill it from FlowMetaStore
no correctness loss
```

### Claim Quotas And Rate Limits

Flow backpressure should be enforced at claim time, not create time.

Reason:

```text
FLOW.CREATE:
  producer side
  durable intake
  should be allowed to build backlog

FLOW.CLAIM_DUE:
  consumer side
  worker capacity boundary
  should decide how much work leaves backlog
```

Claim command shape:

```text
FLOW.CLAIM_DUE <type> <worker_id> <lease_ms> <limit>
  [STATE <state> | STATES <state,...>]
  [PRIORITY <n|low|normal|high> | MINPRIORITY <n|low|normal|high>]
  [WORKER_MAX_INFLIGHT <n>]
  [TYPE_MAX_INFLIGHT <n>]
  [RATE <n> PER <ms>]
  [SCOPE <scope> SCOPE_MAX_INFLIGHT <n>]
```

Examples:

```text
FLOW.CLAIM_DUE checkout worker-1 30000 100 WORKER_MAX_INFLIGHT 500

FLOW.CLAIM_DUE checkout worker-1 30000 100 STATE payment_pending

FLOW.CLAIM_DUE checkout worker-1 30000 100 STATES payment_pending,email_pending

FLOW.CLAIM_DUE checkout worker-1 30000 100 STATE payment_pending PRIORITY high

FLOW.CLAIM_DUE checkout worker-1 30000 100 STATE payment_pending MINPRIORITY normal

FLOW.CLAIM_DUE checkout worker-1 30000 100 TYPE_MAX_INFLIGHT 5000

FLOW.CLAIM_DUE webhook worker-7 30000 50 RATE 1000 PER 1000

FLOW.CLAIM_DUE email worker-2 30000 20 SCOPE tenant:42 SCOPE_MAX_INFLIGHT 100
```

These limits must be checked and updated atomically with the claim.

`FLOW.CLAIM_DUE` apply order:

```text
1. check pause state
2. check worker inflight count
3. check type inflight count
4. check optional scope inflight count
5. check claim-rate bucket/window
6. select due candidates for requested states and priority filter
7. claim up to remaining capacity
8. update flow leases/fencing tokens
9. update inflight/worker/scope indexes
10. update quota counters/buckets
11. append history events
12. commit FlowMetaStore transaction
```

If limits are reached:

```text
worker max inflight reached:
  ["ok", "capacity_full", []]

type max inflight reached:
  ["ok", "type_capacity_full", []]

scope max inflight reached:
  ["ok", "scope_capacity_full", []]

rate limit reached:
  ["ok", "rate_limited", [], "retry_after_ms", ms]
```

This is better than doing a separate generic rate-limit command before claim:

```text
RATE.LIMIT ...
FLOW.CLAIM_DUE ...
```

because the separate sequence has a race and an extra round trip. Flow quotas
are part of the atomic claim operation.

Internal quota metadata can live in FlowMetaStore:

```text
flow_quota_worker:
  {type_id, worker_id} -> inflight_count

flow_quota_type:
  {type_id} -> inflight_count

flow_quota_scope:
  {type_id, scope_id} -> inflight_count

flow_rate_bucket:
  {type_id, bucket_start_ms} -> claim_count
```

Inflight counts should also be derivable from indexes for repair:

```text
flow_inflight:{type}
flow_by_worker:{type}:{worker_id}
optional flow_by_scope:{type}:{scope}
```

`FLOW.COMPLETE`, `FLOW.FAIL`, `FLOW.CANCEL`, and terminal transitions must
decrement/release the relevant inflight counters in the same atomic operation
that clears the lease.

`FLOW.RETRY` should usually clear the lease and decrement inflight, then add the
flow back to the due index for `next_run_at_ms`.

Producer-side rate limiting can exist as a separate generic FerricStore feature,
but it is not the primary Flow backpressure mechanism.

### RAM-Only Sorted Set Is Not Enough

Optimizing sorted set RAM representation helps, but it does not remove the RAM
limit if all due entries remain resident.

Generic BEAM/ETS entries may cost hundreds of bytes each. A native compact
structure could be much smaller, but the memory still scales with entry count:

```text
10M entries * 32 bytes raw = 320MB before overhead
10M entries * 100 bytes = 1GB
10M entries * 300 bytes = 3GB
```

That can be acceptable for one hot index, but Flow may also need state,
inflight, worker, history, and key metadata. Keeping all indexes fully in RAM
still makes RAM a primary capacity limit.

The better design is:

```text
RAM:
  compact hot working set

disk:
  full durable metadata and long-tail indexes
```

### State Index Policy

State indexes are important for operators, but they do not need to be fully hot.

Recommended:

```text
active/problem states:
  optionally keep compact hot heads/counts

full state listing:
  served from FlowMetaStore disk-backed state index

metrics:
  maintained as counters during state transitions
```

So `FLOW.LIST type STATE failed COUNT 100` can page from disk, while dashboard
counters come from cheap counters.

### Inflight Index Policy

Inflight entries are usually bounded by worker capacity:

```text
worker_count * max_inflight_per_worker
```

That makes inflight indexes good candidates for RAM:

```text
FlowInflightHotIndex:
  ordered by lease_until_ms
  used for lease expiry/stuck detection
```

The durable copy still lives in FlowMetaStore so restart/failover can rebuild
the hot inflight index.

### Migration Path

The public API does not need to change.

Implementation can evolve in phases:

```text
Phase 1:
  generic Bitcask/compound-key Flow MVP
  good for lower active-flow counts
  fastest to implement

Phase 2:
  packed flow_meta record and fewer Bitcask keys
  bounded recent history and archive blocks
  reduced keydir pressure

Phase 3:
  native in-memory FlowDueIndex/FlowStateIndex
  lower ETS overhead than generic compound entries

Phase 4:
  FlowMetaStore disk-backed metadata/indexes
  bounded ETS cache
  disk becomes the practical active-flow limit
```

If the product requirement from day one is millions to tens of millions of
active flows, skip directly to Phase 4 for Flow metadata instead of building the
MVP only on generic compound keys.

### Disk-First Full Record

Flow records should be written in a way that allows compact status reads without
loading large bodies.

Recommended split:

```text
flow_meta:{id}
  compact current state metadata

flow_payload:{id}
  application data, possibly large

flow_result:{id}
  terminal result, possibly large

flow_error:{id}:{attempt}
  large error/debug body, optional
```

`FLOW.GET` can have modes:

```text
FLOW.GET id
  returns metadata and payload if payload_size <= flow_payload_return_max_bytes

FLOW.GET id NOPAYLOAD
  returns metadata + refs only

FLOW.GET id PAYLOAD MAXBYTES <n>
  resolves payload up to an explicit per-command cap
```

`FLOW.CLAIM_DUE` should return enough for the worker to execute in the common
case. It should hydrate payloads by default only up to a safe cap:

```text
FLOW.CLAIM_DUE ... LIMIT 100
  returns claimed metadata and payloads up to flow_payload_return_max_bytes

FLOW.CLAIM_DUE ... LIMIT 100 NOPAYLOAD
  returns claimed metadata + refs only

FLOW.CLAIM_DUE ... LIMIT 10 PAYLOAD MAXBYTES 2097152
  explicitly allows larger payload hydration
```

If a payload is larger than the active cap, the response should be explicit:

```text
payload_ref = "flow_payload:{id}"
payload_omitted = true
payload_size = <bytes if known>
```

This keeps the queue/workflow-style default where the work item includes its
payload, but prevents accidental huge responses such as `LIMIT 100 * 2 MiB`.
Workers can fetch omitted large payloads through normal KV `GET`/`MGET` when
ready.

### Completed And Cold Flows

Terminal flows should leave hot indexes quickly.

On completion/failure/cancel:

```text
remove from due index
remove from inflight index
move state index entry to terminal state with TTL/retention
set current record/result TTL
archive/compress old history
```

After terminal retention expires:

```text
delete compact metadata
delete payload/result/error refs
delete history stream/archive
delete state index entry
```

This keeps active-flow memory proportional to flows that can still do work or
need immediate operator visibility.

### Many FlowStore Instances

For very high scale, multiple FlowStore instances can isolate memory and disk
budgets:

```text
CheckoutFlowStore
WebhookFlowStore
ImportFlowStore
NotificationFlowStore
```

Each instance can have its own:

```text
data directory
shard count
memory limits
retention policy
history policy
priority policy
```

This is often simpler than one giant shared FlowStore.

### Practical Scale Expectation

One million active flows should be achievable only if the per-flow hot footprint
is kept small and history/payloads are cold.

Design target:

```text
large data stays on NVMe
recent operational metadata stays in RAM
secondary indexes are compact and bounded
terminal flows leave hot indexes quickly
operators can see pressure before rejection
```

If a workload needs tens or hundreds of millions of active scheduled flows, the
Flow layer should evolve toward native compact indexes and/or disk-backed index
pages rather than relying only on generic compound-key structures.

## Multi-Shard Design

The v1 design can recommend one shard for strict simplicity. Multi-shard Flow
can come later.

Options:

### Application-Chosen Locality

```text
flow_due:{application-chosen-label}
flow:{application-chosen-id}
```

Workers can claim by application label:

```text
FLOW.CLAIM_DUE checkout worker-1 30000 100
```

### Hash Tags

Related internal keys can share a hash tag:

```text
flow:{checkout:123}
flow_due:{checkout:123}
flow_history:{checkout:123}
```

This keeps one flow's record/history/due metadata on the same shard.

### Shard-Local Claim

Each shard maintains its own due index. Workers claim from all shards or from
assigned shard partitions.

```text
worker 1 -> shards 0,1
worker 2 -> shards 2,3
```

The atomicity rule remains the same: a single flow transition must be owned by
one shard.

## MVP Scope

Recommended MVP:

```text
FLOW.CREATE
FLOW.GET
FLOW.CLAIM_DUE
FLOW.TRANSITION
FLOW.RETRY
FLOW.COMPLETE
FLOW.FAIL
FLOW.CANCEL
FLOW.REWIND
FLOW.HISTORY
FLOW.INFO
FLOW.LIST
```

Storage:

```text
one-shard FlowStore recommended
current flow record
sorted-set due index
stream history
state index
pubsub wakeups
lease/fencing token
TTL cleanup
```

Defer:

```text
multi-shard global claim_due
rich search/visibility beyond state/due/inflight indexes
workflow SDKs
graph/state-machine validation DSL
management UI
large history archival
complex human-task abstractions
```

## Non-Goals

Flow should not initially provide:

```text
deterministic workflow replay
SDK-owned workflow code execution
activity worker protocol
code versioning/replay semantics
full BPMN/process modeling
management UI
cross-language SDK abstractions
```

Those are workflow-platform features. FerricStore Flow should stay focused on
durable operational state and atomic coordination.

## Positioning

Possible positioning:

```text
FerricStore Flow gives apps durable workflow state without moving execution out
of the application.
```

Or:

```text
Temporal-like durable coordination for app-owned workflows.
```

The stronger technical framing:

```text
app-owned execution + store-owned correctness
```

FerricStore owns the hard state parts:

```text
atomic claim
atomic transition
durable timer
lease expiry
fencing token
retry schedule
history
status
backpressure
```

The app owns the business logic.
