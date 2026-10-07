# Sparse projection offset fallback scans

Date: 2026-10-05. Continues the
[cache-scaling investigation](projection-cache-scaling-followup.md).

## Supported cause and scoped correction

The fallback calls in the observed sparse apply-projection log are primarily
**negative lookups**, not rejected valid sidecar entries. A read-only audit of
the preserved `router-control-data-33305` fixture validates every frame CRC and
every existing derived slot against the latest frame for its index. Its four
projection segments contain 146, 144, 37 and 15 unique indexes respectively, with
the same number of valid slots, no rejected slots and no missing slots for
existing records.

A fresh live diagnostic records 899 calls to `locate_offset_on_disk/2`, all
returning `not_found`. The derived-index path has 1,280 successful frame matches,
zero rejected matches and no observed untrusted-sidecar state. Gaps in the sparse
projection index legitimately have no frame; the provider traverses the segment
to establish that absence.

Those fallback descriptors previously used raw sequential reads without read-
ahead. Each record header and payload caused small raw read operations. The
retained change adds **256-KiB bounded read-ahead** to the verified descriptor
open in `locate_disk_record_offset/3`. It executes the same complete scan, CRC
checks, ordinal validation, record-size guards and latest repeated-index
selection. No negative-location cache, trusted-index policy or replay-cut rule
is introduced. File synchronization and acknowledgment semantics are preserved.

Artifacts: `offset-fallback-preserved-audit-1.json` and
`offset-fallback-live-{control,trace}-1.json`.

## Component and boundary checks

An isolated projection log with 2,000 records and 128 absent-index queries takes
**1.342 seconds** through the original scan, versus **224 ms** with read-ahead.
Both return `not_found` for every query. These are exploratory component results,
not database throughput or a universal timing bound.

New runtime offset tests force fallback across read windows, select the latest
merged projection frame and reject a later corrupt frame even after finding the
wanted index. Existing tests cover derived-slot corruption, absent/unsafe
sidecars, offset windows, descriptor identity, native frame reads and recovery.
The combined provider/security/runtime scan checks pass **80 tests**.

Artifacts: `offset-scan-{before,buffered}-1.json`. The first pre-change measurement
successfully writes its report but its teardown invokes an inapplicable log-
handle cleanup API and fails; that artifact is component timing only. The
corrected harness removes only its owned fixture after measurement.

## Matched fixed-work lifecycle controls

Three fresh-VM pairs alternate raw and buffered scans. Each completes **1,280
native SET/GET cycles and 1,280 Flow cycles**, eight clients, wall-clock timestamps
and zero warmup. Other source-wait, history-request, page-bound, compaction,
snapshot and durability policies are shared. Both variants succeed on all four
storage snapshots in every trial.

Workload measurement is unprofiled. Shutdown observes only snapshot call/returns,
plus the existing commit telemetry; it does not trace native I/O or sample actor
stacks. The raw control compiles the exact archived pre-change scan section with
the current shared provider sections into a private temporary BEAM. Changed
source/loaded MD5 and shared identities are validated per trial. Workspace and
dependency binaries are not overwritten.

Median of the three per-trial results, rather than pooled percentiles:

| Metric | Raw fallback reads | Bounded read-ahead |
| --- | ---: | ---: |
| Native cycles/s | 641.2 | 658.9 |
| Native p99 | 27.30 ms | 30.15 ms |
| Native p99.9 | 32.17 ms | 35.39 ms |
| Flow cycles/s | 156.4 | 157.8 |
| Flow p50 | 47.54 ms | 45.68 ms |
| Flow p99 | 101.29 ms | 96.95 ms |
| Flow p99.9 | 134.55 ms | 107.93 ms |
| Stop elapsed range | 13.38–15.96 s | **6.47–6.85 s** |
| Successful four-shard snapshots | 3/3 trials | 3/3 trials |

Native tail percentiles regress in this cohort, including a 108.8-ms candidate
p99 outlier. Individual maximums and paired results vary. The shared host runs
full application verification concurrently. The reproducible component and
snapshot-completion improvement support a scoped fallback-read correction;
these results are not a universal service-performance/no-regression claim.

Artifact: `offset-buffered-matched-summary.json` and six workload/stop/log triples.
`BENCH_GATE_OFFSET=1` selects this cohort in `bench/flow_snapshot_controls.py`.

## Larger lifecycle acceptance remains open

The sixty-second-per-scenario buffered control completes **60,374 native cycles
and 5,125 Flow cycles** with zero operation errors. Native p99 is 10.34 ms and
Flow p99 is 158.89 ms. All four writer handoffs still exceed their existing
30-second budgets; application stop finishes after 136.72 seconds. The strict
control fails and preserves its fixture.

Two diagnostic alternatives also fail the four-snapshot gate:

- The **archived cache-batching candidate plus buffering** completes 59,324
  native and 5,551 Flow cycles, then fails three writer handoffs and stops after
  131.48 seconds. It remains experimental and archived.
- A benchmark-only **write quiescence lease** uses the existing public sync-pause
  barriers before stop. It completes 59,248 native and 5,306 Flow cycles, then
  fails three handoffs and stops after 130.05 seconds. This reduces ongoing
  producer activity but does not establish it as the sole remaining cause. No
  production stop/admission policy is changed by that diagnostic.

These time-based workloads finish different operation counts and cannot be
treated as equal-work speedup pairs. They show that buffering, combined cache
batching and producer quiescence have not completed the larger lifecycle goal.
Client success and eventual application stop are not relabeled as successful
snapshots.

Artifacts: `offset-buffered-long-{control,stop}-1.json`,
`offset-cache-combined-long-{control,stop}-1.json`, and
`offset-quiesced-long-{control,stop}-1.json`, with logs.

## Final-source verification

Formatting, warnings-as-errors compilation, both specified Credo warning
profiles and whitespace checks pass. **15 three-node tests** and **two performance
guards** pass. The focused provider/runtime/security suite passes 80 tests.

The first full-core attempt stops after **8,467 passes** (2 skipped, 152 excluded)
on a 120-second Bloom-filter stress-test timeout in `Bitcask.Async.await/2`, not a
failed memory-growth assertion. The focused probabilistic module reports **41
passes** in 97.3 seconds, but the command later exceeds its outer tool budget
during teardown; it is not a successful complete lifecycle check. The first
server attempt reports **1,646 passes**, then a blocking-list gateway operation
exceeds its two-second command deadline. Its isolated case passes with the same
seed. Neither broad failure is established as unrelated to the current source.
The failed logs/results are retained.

The no-benchmark serial rerun (`--max-cases 1`, unchanged seed `873483`,
`--max-failures 1` and `--timeout 180000`) also fails. It reports **305/306 passes**
before the durable-index marker concurrency test exceeds its existing five-
second task-stream budget. The following narrow marker command exceeds its
outer tool budget without producing a successful result. A separate serial
server run stops after **1/2 passes** because a worker that immediately raises
does not complete within the dashboard test's existing 100-ms budget. HTTP is
not reached by the chained commands.

Individual assertions, module deadlines and internal concurrency were not
relaxed. Logs are `offset-buffered-serial-verification.log` and
`offset-buffered-server-serial-verification.log`. These are explicit failed
whole-suite attempts, not clean new-source totals. Multiple runtime timing
failures prevent completing broad acceptance in this continuation; their
relationship to the current source is not established. The earlier 14,291
whole-application checkpoint is not used as a new-source total.

The buffering change remains in the uncommitted worktree with successful focused,
cluster, guard and matched fixed-work evidence. **Full-suite acceptance remains
blocked**, and the longer writer drain remains unresolved. The next
diagnostic must separate source-retry preparation, replay-safe request flushes,
queued enqueue barriers and dirty reconciliation time under the shared permit,
using actual completed work and successful snapshot returns as the acceptance
criteria.

Branch: `codex/oss-full-tdd-review-0.11.23`. Dependency publication/integration
remains paused; cache-batching production sources remain restored.
