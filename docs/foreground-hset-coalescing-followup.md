# Foreground HSET group-commit follow-up

Date: 2026-10-03. Continues the [durable-stall investigation](durable-stall-followup.md).

The subsequent [startup/storage follow-up](startup-storage-stall-followup.md)
fixes the captured 60-second bootstrap cap and a separate LMDB lock-file copy
hang. Its final retained source passes 14,285 application tests, two performance
guards and 15 cluster checks. Native/Flow controls now complete, with substantial
remaining latency variability. The snapshot-sync optimization was rejected and
automatic HSET coalescing still defaults off. Counts and blocked attempts below
describe this earlier foreground-group checkpoint.

## Decision

Bounded promoted HSET group append is retained for eligible explicit batches.
Automatic synchronous HSET coalescing is **experimental and opt-in**, with
`waraft_single_hset_coalescing: false` by default. The runtime opt-in is
`FERRICSTORE_WARAFT_SINGLE_HSET_COALESCING=true`.

The saturated workload benefits substantially, but the cross-workload gate does
not justify enabling the policy by default. Paced median latency worsened and
maximum write latency did not improve consistently. This is not a general fix
for the remaining full-durability stalls, nor a claim that nothing can regress
when the opt-in is enabled. Ordinary synchronous HSETs retain direct submission.

The final source passes **14,278 application tests** in three serial fresh-VM
runs, both performance guards, and **15** installed-dependency cluster checks.
The actual grouped-publication probe observes **11,969,412** sequential field
pairs across 1,000 updates without backwards versions. Full native/Flow
performance acceptance remains blocked by bootstrap timeouts and highly variable
measurements on the scratch image. Failed attempts are retained below.

## Implementation and invariants

- Consecutive same-key `hset_single` commands in a generic apply batch share one
  validated durable dedicated-log append. Groups contain at most **128 commands**
  and **1 MiB estimated payload/metadata**. A non-HSET or different key is an
  ordering barrier. Duplicate fields preserve ordered insertion counts.
- Grouping requires a live nonexpiring hash type, valid cached ordinary field
  locators or missing fields, valid inline values, the promotion latch, and no
  outstanding shared or cross-shard pending writes. Cold, expiring, invalid,
  blob-shaped and side-channel candidates retain sequential handling.
- The existing publication scope spans the append groups and later commands.
  Appends are durable before publication; malformed locations cannot publish a
  valid prefix. Existing revision, index, accounting and maintenance updates run.
- The opt-in synchronous batcher preserves scalar caller replies. With the
  default zero window it queues behind an in-flight commit, without imposing an
  additional fixed batching timer. Explicit configured windows still apply.
  Pending single submissions check a byte bound, configured commit-byte limit,
  count limit and replicated command/member/visit budgets. The existing backend
  commit-byte, quorum and writing-node local-apply admission remains in force.
- A batcher death or call timeout after handoff returns an unknown-outcome error;
  the request is never retried as a direct write. Invalid values stay on the
  original command-level validation path.
- The opt-in rate heuristic uses 100-ms per-shard windows and a threshold of five
  requests, plus already-in-flight work when byte accounting is enabled. It is
  an optimization heuristic, not a correctness condition. Negative monotonic
  origins, delayed timestamps, saturated counters and long-lived VMs are covered.
  A delayed timestamp cannot move the shared window backwards. Backend start and
  stop initialize/clear its state.

No native synchronization primitive, WAL retention policy, replay cut or
dependency pin was changed. WARaft remains at installed **0.1.0**; dependency
publication/integration remains paused.

## Three-pair acceptance experiment

`hset-group-verified` contains 18 serial fresh-VM runs: three alternating pairs
for saturated (120 seconds, 16 clients), paced (60 seconds, 16 clients, 100-ms
cycles), and low-concurrency (30 seconds, four clients) workloads. Four promoted
hashes have 4,096 seed fields and 4-KiB values. Request tracing and diagnostic
metrics are disabled. Source and compiled-module identities are validated.

The direct control removes the automatic-routing heuristic in a temporary BEAM;
the candidate uses the corrected cadence implementation. These measurements
precede the final opt-in configuration wrapper and are kept separately from its
confirmation. Earlier always-coalesced, byte-only, weighted-cadence and window
pilots are diagnostic cohorts, not pooled acceptance evidence.

| Median of three trial metrics | Direct | Automatic candidate |
| --- | ---: | ---: |
| Saturated mixed throughput | 879.7 ops/s | 1,394.2 ops/s |
| Saturated write p50 | 67.8 ms | 42.2 ms |
| Saturated write p95 | 91.9 ms | 54.0 ms |
| Saturated write p99 | 118 ms | 78.8 ms |
| Saturated write p99.9 | 850 ms | 714 ms |
| Saturated HGET p99 | 26 µs | 22 µs |
| Paced write p50 | 26.6 ms | **34.4 ms** |
| Paced write p99 | 86.1 ms | 76.5 ms |
| Low-concurrency write p50 | 20.9 ms | 20.8 ms |
| Low-concurrency write p99.9 | 312 ms | **345 ms** |

Saturated throughput improves about **58.5%** and write p99 about **33.2%**, but
the maximum write across saturated trials rises **1.027→1.357 seconds**. One
paced candidate trial drops throughput to 569.3 ops/s and reaches 190-ms p99.
This fails a universal no-regression criterion despite the strong saturated gain.

Saturated direct runs complete four compactions each; candidate runs complete
eight each. No compactions complete during quiet time and no active, pending or
retry maintenance remains afterward. The throughput improvement is not obtained
by performing less maintenance. Paced/low-concurrency runs perform no measured
compactions, so they are not substitutes for the saturated maintenance control.

## Final-source opt-in confirmation

`hset-group-optin-final` is a separate six-run confirmation on the final source.
Both variants use the same production BEAMs and differ only in the opt-in flag;
reports record and validate the actual enabled setting.

| Final-source pair | Default/direct | Opt-in |
| --- | ---: | ---: |
| Saturated mixed throughput | 998.2 ops/s | 1,406.4 ops/s |
| Saturated write p99 | 91.3 ms | 79.0 ms |
| Saturated write p99.9 | 725 ms | 644 ms |
| Saturated maximum write | 813 ms | **938 ms** |
| Paced write p99 | 71.7 ms | **90.6 ms** |
| Low-concurrency write p99 | 27.1 ms | **36.8 ms** |

Direct/opt-in saturated runs complete six/eight compactions respectively, with
no deferred or outstanding maintenance. This confirmation reinforces the opt-in
decision; it is not pooled with the three-pair cohort or treated as a confidence
bound for other workloads.

## Correctness and verification

Added regressions exercise the actual WARaft segment-apply callback, rather than
only a helper: ordered duplicate/new-field replies, invalid append/cold-read
failure, intervening HINCRBY, 128-command and 1-MiB splitting, TTL-aware sequential
fallback, durable recovery into a fresh keydir, and protected reader waiting
after the durable append. Admission tests cover queued-byte overflow and death
after handoff without retry. A public opt-in test observes real coalesced commits
and correct scalar insertion counts under contention. Cluster tests verify
grouped counts and immediate writing-node reads on all three nodes.

Final application suites each run in a fresh VM, at seed 873483:

```text
mise exec -- mix test apps/ferricstore/test --seed 873483 --max-failures 1 --timeout 180000
core:   11951 passed, 3 skipped, 282 excluded (3195.7 seconds)
mise exec -- mix test apps/ferricstore_server/test --seed 873483 --max-failures 1 --timeout 180000
server:  2183 passed, 1 skipped,  61 excluded (278.3 seconds)
mise exec -- mix test apps/ferricstore_http/test --seed 873483 --max-failures 1 --timeout 180000
HTTP:     144 passed,             5 excluded (5.6 seconds)
total:  14278 passed, 4 skipped, 348 excluded
```

This is serial per-application verification, not one clean same-VM umbrella run.
Its full output is archived as `tool_0ffb19856001UGplshaUeiHbY7`. Final formatting,
warnings-as-errors compilation, both CI-equivalent Credo warning profiles,
whitespace checks, two performance guards and 15 cluster tests pass.

The grouped atomicity probe applies batches of 512 HSETs, exercising multiple
128-command append groups inside one logical publication scope. Its result is
`bench/results/hset-group-atomicity-final.json`: 1,000 updates, 11,969,412 pairs,
no backwards version. This does not assert that two independent HGET requests
form a snapshot; it checks the prohibited newer-first/older-last observation.

### Failed attempts and remaining limits

- The first umbrella attempt failed the existing active-benchmark documentation
  guard because a source-boundary anchor contained a prohibited API name. The
  benchmark control was corrected without weakening the guard. Output:
  `tool_0ff168f4a001cJi1UuRHkmpYOF`.
- A later umbrella attempt hit FLUSHDB unknown-outcome and dashboard cursor
  timeouts; both affected suites pass in isolation without source changes.
  Output: `tool_0ff281863001MhmSsNARK0mqxQ`.
- Another attempt passed all 11,950 then-current core tests, but the same VM's
  server suite hit an unhealthy LMDB reconciliation fixture. Its fresh-VM server
  suite passed all 2,183 tests. Output: `tool_0ff401448001bkToKaYfZhQWBt`.
  These failures are not claimed fixed by retries or by the batching flag.
- Native TCP SET/GET and public Flow create/claim/complete/get controls used the
  owned healthy-capacity APFS scratch image with admission enabled. Two runs
  timed out on a **60-second WARaft bootstrap call**, before measurement.
  Completed runs also varied from milliseconds to seconds. The requested
  three-pair performance-control envelope did not complete; no no-regression
  claim is made from those samples. Logs/results are preserved as
  `hset-group-controls-*` and `hset-group-controls-retry-*`. The image was detached.
- Hardware/full-flush outliers remain observable, including in the earlier
  matching `F_FULLFSYNC` probe outside FerricStore. This work reduces flush
  amplification for one workload; it does not remove that lower-level cost.

All FerricStore changes remain uncommitted. The remaining acceptance work is a
stable, controlled native/Flow and storage/full-flush comparison, followed by a
policy that improves stalled writes without the measured paced/max-latency costs.

## Reproduction

- `BENCH_GATE_PREFIX=<fresh-name> python3 bench/hset_group_gate.py`: three pairs.
- Add `--final-source` for a separate single pair per workload.
- `BENCH_GATE_PREFIX=<fresh-name> python3 bench/hset_group_controls.py`: mount the
  owned healthy-capacity scratch volume first; failed startup logs remain errors.
- `MIX_ENV=test ERL_FLAGS='+S 8:8' BENCH_PROMOTED_READ=protected BENCH_WRITER_SCOPE=grouped BENCH_UPDATES=1000 BENCH_OUTPUT=bench/results/<fresh-name>.json mise exec -- mix run --no-start bench/regressions/promoted_cached_read_atomicity.exs`.
