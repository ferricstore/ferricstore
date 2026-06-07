# Flow Production Readiness

This guide describes the operational model for FerricFlow in production: durability, overload behavior, leases, retry exhaustion, projection lag, retention, and metrics.

## Production Mental Model

FerricFlow stores current workflow state through FerricStore's own WARaft-backed storage path. A command succeeds only after the state change is accepted through the quorum path and written to disk. Hot keydir/native indexes serve current state. LMDB/history projections are query surfaces and may lag briefly.

```text
WARaft segment/apply projection = durable Flow write path
Hot keydir/native Flow indexes = authoritative serving state
LMDB/history = lagged query projection
Payload/value bytes = Flow values or blob side-channel files
```

## Durability Boundary

- `FLOW.CREATE`, transition, retry, complete, fail, cancel, and signal commands wait for quorum-backed disk durability before success.
- Current Flow state wins over query projections.
- On restart, hot indexes and projections rebuild or resume from FerricStore-managed WARaft segment/apply-projection storage.
- History/projection queries can be made consistent by waiting for projection catch-up when the command supports it.

## Failure Model

- `FLOW.CLAIM_DUE` grants a lease token and fencing token.
- Mutating a claimed Flow requires the current lease token.
- A stale worker cannot overwrite a newer claim.
- If a worker crashes, lease expiry or reclaim makes the Flow claimable again.
- Handlers are normal application code and are not replayed by FerricFlow.
- Side effects should be idempotent or guarded by application-level idempotency keys.

## Overload And Backpressure

FerricStore protects memory and disk instead of accepting unbounded writes.

Operators should expect these states:

| State | Meaning |
| --- | --- |
| normal | Writes accepted normally. |
| pressure | Server starts protecting resources and may slow or reject some write paths. |
| reject | New Flow creates/writes can fail cleanly until pressure drops. |

SDKs should treat write rejection as overload and back off. Production workloads should monitor RSS, disk growth, apply queue pressure, and projection lag.

## Leases And Reclaim

Use leases to fence workers:

```text
FLOW.CLAIM_DUE order STATE charge WORKER worker-1 LIMIT 100
FLOW.COMPLETE order-1 <lease-token> FENCING <fencing-token> RESULT "ok"
```

If a worker dies, the Flow remains durable and can be reclaimed after the lease expires. Reclaim should be treated as normal recovery, not data loss.

## Retry And Terminal States

Retry policy controls whether failed work is retried later or exhausted into another state.

- `MAX_RETRIES` counts retries after the first attempt.
- Retry can use fixed or exponential backoff.
- Exhaustion can move to `failed`, `cancelled`, `completed`, or a manual/active state.
- Terminal state changes are recorded in history and terminal query projections.

See [Flow Retry Policy](flow-retry-policy.md).

## Signals, Fanout, And Value Refs

Signals are durable external events. They can be idempotent, state-conditional, visible in history, and can optionally transition the Flow.

Fanout creates child Flows with their own state, leases, retries, history, and terminal status. Parent/child links are queryable. Retention should account for both parent and child histories.

Value refs keep large or optional bytes separate from hot Flow metadata. Workers should request only the values they need. Large values may require blob cleanup and retention tuning.

## Projection Lag

LMDB/history projections are lagged query surfaces. Command success does not require every cold query index to be flushed first.

Monitor:

- `ferricstore_flow_lmdb_projection_pending_entries`
- `ferricstore_flow_lmdb_projection_oldest_pending_age_us`
- `ferricstore_flow_history_projector_pending_entries`
- `ferricstore_flow_history_projector_oldest_pending_age_us`
- projection flush failure counters

Operational rule: current Flow state is authoritative; projection lag affects query freshness, not command correctness.

## Retention And Cleanup

Retention deletes logical Flow records after terminal TTLs and history/value policies allow it. Disk space may not be reclaimed immediately because append-only storage still needs compaction/release to catch up.

Use retention to bound:

- terminal Flow records;
- history rows;
- value refs and blob manifests;
- parent/child query projections.

## Metrics And Alerts

Alert on sustained growth in:

- RSS or allocator memory;
- disk used by data directory;
- apply queue pressure/rejections;
- LMDB/history projection lag;
- blob protection/hardened value counts;
- Raft apply/release cursor gaps;
- terminal retention backlog.

## Starting Points

Recommended starting points for queued workflow workloads:

- server shard count: auto for small nodes, explicit shard sweep for benchmark/large nodes;
- queue/workflow batch size: `100` to `500` depending on latency target;
- claim leases: long enough for normal handler time plus jitter;
- payload hydration: request only needed values;
- terminal retention: set based on audit/debug requirements.

Validate changes with the 1M Flow benchmark and a soak test that includes create, claim, complete, retry, fail, signals, value refs, history, retention, and restart.
