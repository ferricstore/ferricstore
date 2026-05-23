# Flow Production Readiness Notes

Flow command correctness is based on Ra + Bitcask Flow state/history records and
the hot ETS Flow indexes. LMDB is a cold projection for terminal, lineage, and
deep-history queries. Hot commands must not wait for LMDB projection flush unless
the caller explicitly asks for a consistent projection read.

## LMDB Projection Visibility

Projection lag and backpressure are exposed through telemetry and public metrics.

Telemetry:

- `[:ferricstore, :flow, :lmdb_writer, :backlog]`
- `[:ferricstore, :flow, :lmdb_writer, :flush]`
- `[:ferricstore, :flow, :lmdb_writer, :unavailable]`
- `[:ferricstore, :flow, :lmdb_replay_safe_index, :persist]`
- `[:ferricstore, :flow, :lmdb_mirror, :degraded]`

Prometheus / `FERRICSTORE.METRICS`:

- `ferricstore_flow_lmdb_replay_safe_index`
- `ferricstore_flow_lmdb_replay_safe_requested_index`
- `ferricstore_flow_lmdb_replay_safe_lag`
- `ferricstore_flow_lmdb_replay_safe_persist_failures_total`
- `ferricstore_flow_lmdb_mirror_enqueue_failures_total`
- `ferricstore_flow_lmdb_mirror_degraded`

`INFO bitcask` exposes the same shard fields with `shard_N_flow_lmdb_*` names.
Alert on non-zero degraded flags, increasing enqueue/persist failures, or a
replay-safe lag that grows instead of draining.

## Retention And Cleanup

Terminal flows stay in hot indexes for `:flow_terminal_hot_ttl_ms` after LMDB
flush, then the writer prunes hot terminal/lineage index entries. The default is
`0`, so terminal rows leave hot indexes as soon as the LMDB projection is flushed.
Terminal history is also cold-only after LMDB projection: it remains queryable
from LMDB/history storage, but no terminal history rows are kept in the hot Flow
index by default. The Flow state record remains durable until normal Flow TTL
expiry.

LMDB terminal and history projections have explicit expire indexes:

- terminal sweep: `Ferricstore.Flow.LMDB.sweep_expired_terminal/3`
- history sweep: `Ferricstore.Flow.LMDB.sweep_expired_history/3`

Cold query paths run bounded sweeps before reading projection rows. This keeps
expired terminal/history projection rows from growing forever even if no exact
record is read.

## Lease Reclaim

Expired running leases are reclaimed through `FLOW.RECLAIM` or
`FerricStore.flow_reclaim/2`. Reclaim is partition scoped when `PARTITION` /
`:partition_key` is provided. It creates a new lease/fencing token and keeps the
same Flow atomicity rules as `FLOW.CLAIM_DUE`.

`FLOW.CLAIM_DUE` includes expired lease reclaim by default:

- `reclaim_expired`: default `true`
- `reclaim_ratio`: default `25`, valid `0..100`
- claim order: reclaim a small expired-running slice, claim normal due work, then
  reclaim more expired-running flows if the response is still below `LIMIT`

Use `STATE running` or `FLOW.RECLAIM` when a worker only wants expired running
leases. Use `reclaim_expired: false` when a worker must claim only fresh due
work. Reclaimed work is still at-least-once; handlers must be idempotent.

## Retry And Terminal Policy

Retry policy can be set once per Flow type and overridden per state with
`FLOW.POLICY.SET` / `FerricStore.flow_policy_set/2`. Command-level retry policy
still wins for the single command that carries it.

Defaults and guards:

- `max_retries`: default `3`, hard max `1000`
- backoff: default exponential, `base_ms: 1000`, `max_ms: 30000`,
  `jitter_pct: 20`
- `max_ms` accepts long schedules up to the configured duration cap, currently
  one month
- `exhausted_to`: any state, but only `completed`, `failed`, and `cancelled` are
  terminal states

Terminal state changes are centralized through `FLOW.COMPLETE`, `FLOW.FAIL`, and
`FLOW.CANCEL`. `FLOW.TRANSITION` rejects transitions into terminal states so
parent/child hooks, summaries, cross-shard updates, and retention stamping stay
on one path. `FLOW.RETRY` and `FLOW.RETRY_MANY` use the same terminal hook path
when retry exhaustion moves a Flow into `completed`, `failed`, or `cancelled`.

## History Caps

History has two separate caps:

- `history_hot_max_events`: default `1`, hard max `10000`
- `history_max_events`: default `100000`, hard max `1000000`

The hot cap bounds the recent history kept in memory/indexed for quick reads.
The total cap bounds durable history growth for one Flow. `history_max_events`
must be greater than or equal to `history_hot_max_events`.
Terminal flows override the hot cap to zero after LMDB projection, because
terminal history is not on the worker hot path.

## Fairness

Flow fairness is explicit, not global magic:

- `partition_key` isolates tenant/device ordering and lets callers scale workers
  across shards.
- `priority` splits due queues into fixed priority bands.
- `FLOW.CLAIM_DUE ... PARTITION ...` gives FIFO inside the selected partition and
  priority band.
- `PARTITION ANY` / no partition uses a per-type rotating shard cursor for
  wildcard claims so small limits do not always start from the same shard.
- `STATE ANY` scans due states for the selected type; use explicit states when a
  worker owns a narrow step.

Under skew, run workers per hot partition or partition group when strict tenant
fairness matters. The wildcard cursor is bounded and fair enough for general
workers, but it is not a replacement for explicit tenant/device worker pools.

## Query Scalability Bench

Use the lineage bench for large terminal/lineage query surfaces:

LMDB projection is mandatory for Flow. Use `FLOW_LMDB_MODE=mirror` only when the
benchmark intentionally measures synchronous projection; production defaults to
lagged projection.

```sh
MIX_ENV=bench FERRICSTORE_BUILD=1 FLOW_LMDB_MODE=mirror \
FLOW_LINEAGE_BACKLOG=1000000 FLOW_LINEAGE_TERMINAL=1000000 \
FLOW_LINEAGE_ITER=200 FLOW_LINEAGE_QUERY_COUNT=100 \
mix run --no-start bench/flow_lineage_bench.exs
```

For 10M projection scale, run the same bench with:

```sh
FLOW_LINEAGE_BACKLOG=10000000 FLOW_LINEAGE_TERMINAL=10000000
```

Track p50/p95/p99 for `flow.by_root`, `flow.by_correlation`, and terminal LMDB
queries, plus BEAM memory delta and LMDB replay-safe lag.

## Audit Query Surfaces

`FLOW.HISTORY <id>` is a per-flow query. It stays on the owning shard and uses
the existing hot history refs, Bitcask history records, and optional LMDB cold
projection. Supported filters are `COUNT`, `FROM_EVENT`, `TO_EVENT`, `FROM_MS`,
`TO_MS`, `FROM_VERSION`, `TO_VERSION`, `REV`, `EVENT`, `WORKER`, `INCLUDE_COLD`, and
`CONSISTENT_PROJECTION`.

`FLOW.TERMINALS <type>` lists terminal records from existing terminal state
indexes. `STATE` accepts `failed`, `completed`, `cancelled`, or `any`, with
optional `COUNT`, `PARTITION`, `FROM_MS`, `TO_MS`, `REV`, `INCLUDE_COLD`, and
`CONSISTENT_PROJECTION`. `FLOW.FAILURES <type>` is a convenience alias for
`FLOW.TERMINALS <type> STATE failed`.

These audit queries add no hot-path write indexes. They reuse Flow state/history
truth plus existing terminal projections, so create/transition/claim latency is
not affected by the richer read shape.

`FLOW.BY_PARENT`, `FLOW.BY_ROOT`, and `FLOW.BY_CORRELATION` also reuse the
existing LMDB lineage indexes. They support `FROM_MS`, `TO_MS`, `REV`, `STATE`,
and `TERMINAL_ONLY` as read-side filters; no new LMDB rows are written for
these query shapes.

## Flow Schema And Compact Codec

Before public release, Flow uses one compact current schema:

- Flow record magic: `FSF5`
- Flow history magic: `FSH2`
- Flow value magic: `FSV2`

User payload/result/error bytes are raw refs and are not decoded by FerricStore.
Only Flow metadata is schema-owned by FerricStore.

`FSF5` stores required mutable state fields inline and uses a flag word for
optional/default fields. Nil values, default counters, default priority,
missing leases, empty sidecars, and the common `root_flow_id == id` case are not
written repeatedly. The Elixir codec and Rust NIF codec must stay byte-compatible.

`FSH2` stores per-event history fields only. It intentionally omits immutable
workflow metadata such as id, type, parent/root, partition, and correlation id.
User-facing history decode must pass the current/snapshot Flow record as context
through `Ferricstore.Flow.decode_history_fields/2`; no-context decode is only
for low-level projection/ref extraction.

Generated payload/result/error refs can be dematerialized from the hot keydir
after LMDB/history projection confirms the history event. Public Flow value
reads must go through Flow value helpers so they can resolve hot keydir rows,
LMDB locators, history-projector files, and WARaft segment locations.

Before public release, old Flow record magic may be rejected cleanly because no
external user data depends on it yet. After public release, incompatible
metadata field-order or type changes must add a new magic version and keep old
decoders until all persisted data can migrate. LMDB stores wrapped Flow record
bytes only; schema migration belongs in `Ferricstore.Flow.encode_record/1` and
`decode_record/1`, not in LMDB. User payload/result/error bytes stay raw and
are not part of the Flow metadata schema.
