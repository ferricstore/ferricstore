# Flow snapshot reconciliation and history request bursts

Continuation: [apply-projection cache scaling experiment](projection-cache-scaling-followup.md)
tests and archives the cache/group candidate after its lifecycle gate fails,
verifies restoration, and identifies repeated disk-offset scans as the next
diagnostic target. The retained corrections and checkpoint below remain valid.

Date: 2026-10-05. Continues the
[source-wait investigation](flow-shutdown-source-wait-followup.md).

## Result

Two supported corrections are retained in the uncommitted worktree:

1. State reconciliation pages honor the existing composite projection admission
   limit. A valid large source shard no longer becomes unhealthy simply because
   a page exceeds the reverse-row prefetch bound.
2. Already queued history projection requests share a prompt flush timer, instead
   of forcing a durable marker update for every cast. Explicit flush barriers and
   replay-cut ordering remain enforced.

All three larger matched fixed-work controls now complete **all four storage
snapshots**, stopping in **12.96–13.88 seconds**. The legacy controls stop in
27.59–29.08 seconds but fail snapshot health. The longer sixty-second-per-scenario
workload still exposes LMDB writer handoff timeouts; this is not a complete
large-workload shutdown fix.

## 1. Valid reconciliation pages exceeded a downstream bound

The bounded diagnostic now records errors from reconciliation preparation and
write results. The exact error was `:composite_reverse_prefetch_too_large`.
`LMDBRebuilder` selected up to 512 source records, while
`CompositeProjection.prefetch_reverse_values/3` accepts at most
`Limits.max_projection_page_records()` (256). Failed pages accumulated
`lmdb_errors`, left the projection marked unhealthy and prevented snapshots.

Both batch-source-wait and sequential-source-wait controls reproduce this error:
`flow-reconcile-{batch,sequential}-trace-2.json`. It therefore predates the
source-wait correction. Their traced stop times, 24.60 and 66.14 seconds, are
diagnostics rather than acceptance measurements.

The state scan now uses `min(512, Limits.max_projection_page_records())`.
Source validation, physical locators, composite CAS operations, exact counts,
terminal cleanup and final health-marker publication remain enforced. The
downstream admission limit is not raised. Other non-state scan/write limits retain
their separate purposes.

A failing-first regression builds **529 physically persisted active and terminal
records**. Before the correction, reconciliation returns an unhealthy mirror.
Afterward it verifies every query row, composite entry and reverse row, exact
per-state counters, a cleared flush marker and healthy mirror. A second pass
verifies idempotence and counts across page boundaries. The rebuild/cold-source/
prune-race/count checks initially pass **43 cases** together.

## 2. History request bursts amplified durable marker work

The earlier long diagnostic had history-projector queues near 10,000 messages and
thousands of durable history-marker replacements. `handle_cast({:project_to,
index}, state)` flushed immediately for each request. This serialized small
history batches and marker syncs even when the mailbox already contained the
rest of a burst.

The handler now remembers the maximum **handled** requested index and schedules
one prompt timer. It does not consult the requested-index atomics as a replay
cut: those atomics can describe history messages the actor has not received.
The existing projection/sync/publication path executes the combined work;
`HistoryProjector.flush/3` still blocks until the handled work is durably drained.
Pending admission, overflow and lost-enqueue recovery logic are retained.

The new request tests establish:

- A queued hundred-entry/request burst paid **100 syncs** before the change. It
  now completes in at most two shared syncs. While the first sync hook is blocked,
  the durable marker is zero and the flush barrier cannot complete.
- Requests without flushed history cannot publish their target watermark.
- A failed shared sync cannot authorize a requested watermark.
- An unseen higher requested atomic cannot advance the handled replay cut.

Existing lost-enqueue, overflow, chunked flush, recovery, failed-marker and
source-validation checks pass. New fixtures use process/random directory suffixes
and refuse existing roots, preventing stale markers from an earlier failed
fixture or another verification VM from being mistaken for new progress.

## Matched larger-work controls

`bench/flow_snapshot_controls.py` runs three serial alternating legacy/bounded
pairs. Each trial completes **1,280 native SET/GET cycles and 1,280 public Flow
cycles**, eight clients, wall-clock timestamps and zero warmup. All use the
retained batch-source-wait writer, default-off automatic HSET coalescing and the
same durability/compaction/snapshot policy.

Workload measurement is unprofiled. Afterward, the observer traces only the
snapshot call/return functions; it does not sample actor stacks or trace native
I/O/source reads. Success requires four storage snapshots returning `:ok` plus
completed application stop. The exact legacy sources are compiled from HEAD into
private fresh-VM BEAMs; shared source/loaded identities and changed module hashes
are validated across trials. Workspace/dependency binaries are not overwritten.

Median of per-trial results (not pooled percentiles):

| Metric | Legacy pages / immediate requests | Bounded pages / coalesced requests |
| --- | ---: | ---: |
| Native cycles/s | 607.6 | 688.0 |
| Native p99 | 33.22 ms | 26.46 ms |
| Native p99.9 | 34.77 ms | 35.86 ms |
| Flow cycles/s | 141.2 | 141.4 |
| Flow p50 | 50.65 ms | 54.10 ms |
| Flow p99 | 122.91 ms | 106.95 ms |
| Flow p99.9 | 229.70 ms | 121.25 ms |
| Stop elapsed range | 27.59–29.08 s | 12.96–13.88 s |
| Four storage snapshots successful | 0/3 trials | **3/3 trials** |

Native p99.9 and Flow p50 regress in this cohort. Individual outliers and ranges
vary; some candidate trial percentiles also exceed their paired legacy values.
The shared host ran affected-suite verification concurrently. These results
support the specific boundedness and durable-work corrections, not universal
service-latency or throughput improvement.

Artifact: `flow-snapshot-matched-summary.json` with six workload/stop/log triples.
The first exploratory fixed run also succeeds on four snapshots in 13.46 seconds
(`flow-snapshot-fixed-{control,stop}-1.json`).

## Longer workload: remaining writer drain bottleneck

A sixty-second native scenario followed by sixty seconds of Flow, with five
seconds warmup each, completes **48,284 native cycles and 5,312 Flow cycles** with
zero operation errors. Native p99 is 19.64 ms; Flow p99 is 170.49 ms. Its strict
snapshot-result observation fails: some writers exceed their existing
30-second handoff/flush budgets despite application stop finishing after
166.83 seconds. The failed fixture is preserved.

A following diagnostic fails all four writer handoffs and completes stop after
150.16 seconds. It records:

- No composite reconciliation errors; the page-bound failure is absent.
- History queues drain and projected/requested counters agree at shutdown start.
  All four history actors remain idle during the sampled shutdown.
- LMDB writers queue behind the serialized runtime permit. Observed dirty
  projection reconciliation calls, including their permit waits, take roughly
  53–61 seconds.
- Stacks repeatedly enter apply-projection cache selection and projection locks.
  `apply_projection_cache_entries_for_indexes/3` performs a partial-key ETS scan
  for each index, and cold-source preparation invokes durability work per group.
  This identifies a remaining scaling investigation; no cache-index or batching
  change is included in this pass.
- Source retries and background policy-fence commits still occur. Their counts
  are diagnostics, not proof that either is the sole remaining cause.

Artifacts: `flow-snapshot-long-fixed-{control,stop}-1.json` and
`flow-snapshot-long-fixed-{control,trace}-2.json`, with logs. The strict control
exits nonzero for failed snapshots even when application stop returns `:ok`.

## Verification scope

- **2,535 affected tests pass**, 37 excluded, in 1,218.9 seconds:
  `mix test apps/ferricstore/test/ferricstore/flow
  apps/ferricstore/test/ferricstore/flow_lmdb_test.exs
  apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs
  apps/ferricstore/test/ferricstore/application_test.exs --seed 873483
  --max-failures 1 --timeout 180000`.
- The latest focused reconciliation/request run passes **22 tests**, including
  the added unseen-atomic case and fresh fixture preparation; this overlaps the
  affected suite rather than being added to its total.
- **15 three-node checks** and **two performance guards** pass on current source.
- Formatting, warnings-as-errors compilation, both specified Credo warning
  profiles, whitespace checks and unchanged dependency lock checks pass.

The earlier **14,291**-test whole-application checkpoint predates these two
production changes. It is not relabeled as a new full-suite result. The current
pass verifies the affected Flow/query/recovery/snapshot/lifecycle surface and
cluster/guard checks; broad non-macOS/Jepsen/crash/SDK lanes remain separate.

Branch: `codex/oss-full-tdd-review-0.11.23`. Changes remain uncommitted; dependency
publication/integration remains paused. General durable-write tails and the
longer LMDB writer drain remain open.
