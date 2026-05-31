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
- `[:ferricstore, :flow, :history_projector, :queue_full]`
- `[:ferricstore, :flow, :history_projector, :recover]`

Prometheus / `FERRICSTORE.METRICS`:

- `ferricstore_flow_lmdb_replay_safe_index`
- `ferricstore_flow_lmdb_replay_safe_requested_index`
- `ferricstore_flow_lmdb_replay_safe_lag`
- `ferricstore_flow_lmdb_replay_safe_persist_failures_total`
- `ferricstore_flow_lmdb_mirror_enqueue_failures_total`
- `ferricstore_flow_lmdb_mirror_degraded`
- `ferricstore_flow_history_projected_index`
- `ferricstore_flow_history_requested_index`
- `ferricstore_flow_history_projection_lag`
- `ferricstore_flow_history_projector_pending_entries`
- `ferricstore_flow_history_projector_oldest_pending_age_us`
- `ferricstore_flow_history_projector_flush_failures_total`
- `ferricstore_flow_history_projector_queue_full_total`

`INFO bitcask` exposes the same shard fields with `shard_N_flow_lmdb_*` and
`shard_N_flow_history_*` names. Alert on non-zero degraded flags, increasing
enqueue/persist/flush failures, or a replay-safe/projection lag that grows
instead of draining.

Durability boundary:

- Flow command success waits for Ra commit/apply and Bitcask truth writes.
- LMDB terminal/lineage projection is async.
- History projection is async, but every requested replay-safe marker is
  monotonic and never moves backward.
- `CONSISTENT_PROJECTION`/projection requests may wait for the async projectors;
  normal hot commands must not.

Failure mode:

- If LMDB/history projection fails, the command remains durable because state and
  history truth are in Bitcask.
- Projection lag metrics rise until the projector catches up or an operator
  fixes the underlying disk/LMDB issue.
- On restart, projectors replay from durable Flow state/history records and the
  persisted projected-index markers.
- The optional LMDB release-cursor poke is disabled by default through
  `:flow_lmdb_release_cursor_poke_enabled`. Enable it only after validating that
  the deployment benefits from faster cursor nudges without adding noisy Ra
  traffic.

Important tuning note: the hot-path throughput cliff during large terminal Flow
bursts is usually not fixed by making LMDB flush more aggressively. The critical
budget is `:waraft_apply_projection_cache_max_entries` /
`FERRICSTORE_WARAFT_APPLY_PROJECTION_CACHE_MAX_ENTRIES`, which buffers applied
Flow state/value rows until lagged LMDB/history projection consumes them. If it
is too small, WARaft has to spill/compact that cache synchronously and the
DBOS-style 1M Flow benchmark can fall from the ~58K/s range to the ~35K/s range.
Treat LMDB flush interval/batch knobs as lag controls; treat the apply-projection
cache as the burst-throughput guardrail.

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

## Blob Protection Cleanup

Blob writes prepared before Ra commit are protected from GC until the command
outcome is known. If the caller loses the commit outcome after submit, the blob
protection is intentionally hardened instead of deleted. This is safe: GC keeps
the blob until the system can prove all older Ra/apply state is replay-safe.

The cleanup path is background-only:

- Hardened protections are tracked in ETS with creation time and metadata.
- Blob GC snapshots a bounded set of hardened protection IDs per shard.
- Blob GC commits a no-op Ra barrier for that shard.
- Blob GC waits until apply/release metrics prove the shard is replay-safe.
- Under the blob GC shard lock, it releases only the snapshotted hardened IDs and
  sweeps normally against the current live-ref set.

Telemetry:

- `[:ferricstore, :blob, :protection, :hardened]`
- `[:ferricstore, :blob, :protection, :reconcile]`
- `[:ferricstore, :blob, :protection, :reconcile, :failed]`

Prometheus / `FERRICSTORE.METRICS`:

- `ferricstore_blob_hardened_protections`
- `ferricstore_blob_hardened_oldest_age_ms`

`FERRICSTORE.BLOBGC` also returns:

- `hardened_protections_seen`
- `hardened_protections_released`
- `hardened_protections_blocked`

Runbook:

- If `ferricstore_blob_hardened_protections` rises briefly and drains, no action
  is needed.
- If `ferricstore_blob_hardened_oldest_age_ms` keeps rising, check Ra health,
  replay-safe cursor lag, disk pressure, and blob GC errors.
- `:blob_protection_reconcile_enabled` can disable automatic release if an
  operator needs manual inspection.
- `:blob_protection_reconcile_max_records` bounds one GC pass, default `1000`.
- `:blob_protection_reconcile_barrier_timeout_ms` bounds the Ra/replay wait,
  default `30000`.

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

History has one user-facing policy cap and one internal hot cache cap:

- `history_max_events`: default `100000`, hard max `1000000`
- internal hot history cache: default `0`

The internal hot cache keeps history rows only until LMDB projection catches up.
It is intentionally not exposed through `FLOW.POLICY.SET`; policies should only
control terminal retention TTL and total durable history growth for one Flow.
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

LMDB projection is mandatory for Flow and always runs as a lagged cold
projection. There is no synchronous/write-through mode in production.

```sh
MIX_ENV=bench FLOW_LINEAGE_BACKLOG=1000000 \
FLOW_LINEAGE_TERMINAL=1000000 FLOW_LINEAGE_ITER=200 FLOW_LINEAGE_QUERY_COUNT=100 \
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
