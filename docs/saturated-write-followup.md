# Saturated-write follow-up: single-field HSET

The subsequent [durable-stall follow-up](durable-stall-followup.md) reproduces
hundreds-of-milliseconds full flushes outside BEAM, rejects ineffective
compaction synchronization/admission experiments, and fixes a locator race with
read-side LFU/cache updates. The latest combined tree passes **14,261 tests**,
both performance guards and 14 cluster checks. The HSET measurements below
retain their original stage-specific provenance.

Date: 2026-10-02. Continues the
[protected promoted-read investigation](promoted-publication-followup.md).

## Outcome

Retained a targeted single-field HSET improvement for selected WARaft contexts.
The instance-oriented facade and default public API now submit one atomic
`hset_single` command for eligible inline values. Type checking, field existence,
the insertion count, and the write execute together in Raft order. Blob candidates
retain the existing ref-aware preparation path; other adapters use their existing
compound operations. The default public API still returns `:ok`, while the
instance-oriented facade returns the insertion count.

This substantially removes the saturated-write penalty seen after enabling fast
promoted reads. It does **not** resolve the underlying long durable-I/O stalls.
No fsync, quorum, local-apply barrier, recovery validation, or admission limit was
relaxed. Installed WARaft remains 0.1.0; dependency publication/integration is
still paused. All accumulated FerricStore changes remain uncommitted.

## Diagnosis and correctness fixes

The old public single-field path performed a durable `compound_type_claim`, then
a client-side field read, then a durable `compound_batch_put`. A telemetry
regression observed both commit shapes for one HSET. The existing state-machine
`hset_single` operation can perform the complete mutation in one durable command.

A separate failing-first concurrency test found that all **16** concurrent
writers to the same missing field reported one insertion. The new route computes
existence during serialized apply: exactly one caller reports one, and the other
15 report zero.

Reviewing that atomic command also exposed a pre-existing failure-path defect:
an invalid cold-field read was treated as an existing field and overwritten
before the outer apply returned its recorded read error. A failing-first test
observed the new row and appended bytes despite the error. The command now
propagates type/field read failures before writing; the regression verifies both
the original row and dedicated-log size remain unchanged.

Diagnostic request spans and WARaft service-time histograms support the queueing
diagnosis. In one 30-second pair, the old serialized/cached paths had approximately
the same number of commit requests, but log append batches increased from 4,667
to 10,100 with cached reads (counts include warmup). This indicates worse grouping
when the client cadence changes. Most request time was in the commit wait, not
preparation or admission. These instrumented runs are diagnostic evidence and
are not pooled with the unprofiled performance gate. The exact device/OS cause
of the occasional long stalls was not established.

## Three-pair performance matrix

Fresh VMs ran serially in alternating variant order, with 16 clients, four
4,096-field promoted hashes, 4-KiB values, eight schedulers, a 1-GiB configured
cache, and automatic compaction. Saturated runs measured 120 seconds after
10 seconds warmup. Paced runs measured 60 seconds, limiting each client to one
four-operation cycle per 100 ms. Values were checked during execution and at
completion. Full-cycle latency is captured separately from individual operations.

The variants are:

- `legacy_serialized`: serialized promoted reads and the old two-command HSET.
- `legacy_cached`: protected cached reads and the old two-command HSET.
- `atomic_cached`: protected cached reads and the new one-command HSET.

Numbers are medians of per-trial metrics across three trials, not pooled request
percentiles. All completed reports have zero operation and compaction errors.

| Metric | Legacy serialized | Legacy cached | Atomic cached |
| --- | ---: | ---: | ---: |
| Saturated mixed throughput | 874.3 ops/s | 854.8 ops/s | **1,009.9 ops/s** |
| Saturated write p50 | 62.2 ms | 68.9 ms | **59.7 ms** |
| Saturated write p95 | 77.1 ms | 94.7 ms | **77.3 ms** |
| Saturated write p99 | 89.4 ms | 119 ms | **90.3 ms** |
| Saturated HGET p99 | 29.6 ms | 22 µs | **22 µs** |
| Saturated full-cycle p99 | 96 ms | 117 ms | **90.4 ms** |
| Paced write p50 | 32.9 ms | 27.7 ms | **19.4 ms** |
| Paced write p95 | 56.1 ms | 44.9 ms | **31.6 ms** |
| Paced write p99 | 70.2 ms | 65.7 ms | **70.1 ms** |

Against the protected-read path with the old writer, saturated throughput improves
about **18.1%** and write p99 improves about **24.1%**. Write p99 is approximately
back to the original serialized baseline, while the read-tail improvement is
preserved. Saturated write p99 improved in every paired trial: 128→114,
110→88.7, and 119→90.3 ms.

The legacy variants each completed four compactions per saturated trial. The
atomic variant completed **4, 8, and 7**: it performed more writes and maintenance
in the latter two runs, rather than obtaining its advantage by postponing
compaction. Every paced trial completed zero compactions. Paced p99 has overlapping
ranges and no consistent improvement; the table deliberately retains that result.
The maximum atomic write across these trials was **1.332 seconds**, so long
durable writes remain an unresolved concern.

The three-pair matrix preceded the final default-public-API callback wiring; its
workload uses the instance facade. The final source was then reconfirmed with a
separate complete matrix, described next. Source and compiled-module identities
are captured and validated within each cohort.

## Final-source confirmation

After wiring the default API's callback, one separate fresh-VM confirmation per
variant/workload completed successfully. It is kept separate from the three-pair
medians; the host was slower and the legacy cached path had particularly large
tails in this later cohort.

| Final-source metric | Legacy serialized | Legacy cached | Atomic cached |
| --- | ---: | ---: | ---: |
| Saturated mixed throughput | 796.0 ops/s | 765.5 ops/s | **937.8 ops/s** |
| Saturated write p99 | 101 ms | 212 ms | **98.6 ms** |
| Saturated HGET p99 | 31.8 ms | 19 µs | **20 µs** |
| Paced write p99 | 87.6 ms | 217 ms | **80.2 ms** |
| Paced HGET p99 | 27.3 ms | 20 µs | **19 µs** |

Each saturated confirmation completed four compactions; paced confirmations
completed zero. All operation and compaction errors were zero. These results
confirm the performance direction on final source; they are not a precise
general tail-latency bound or proof that storage stalls are fixed.

## Verification and failure provenance

The final complete retry passed:

```text
mise exec -- mix test --seed 873483 --max-failures 1 --timeout 180000
core:   11932 passed, 3 skipped, 281 excluded (2908.9 seconds)
server:  2183 passed, 1 skipped,  61 excluded (267.2 seconds)
HTTP:     144 passed,             5 excluded (5.9 seconds)
total:  14259 passed, 4 skipped, 347 excluded
```

Both performance guards pass. The installed-dependency three-node suite passes
**14 tests**, including concurrent public/instance HSET insertion counts and
immediate read-your-write on each writing node. The focused checks include
WRONGTYPE preservation, expiration and TTL clearing, oversized-value rollback,
blob side-channel behavior, promoted publication, and invalid cold-read failure.
Formatting, warnings-as-errors compilation, both CI-equivalent Credo warning
profiles, and whitespace checks pass. No Rust source changed.

Failures are preserved rather than reclassified as successful runs:

- The first legacy benchmark override failed atom preloading because its compiled
  module had no real BEAM filename. Overrides now write/load temporary BEAMs;
  benchmark cleanup no longer masks a startup failure with telemetry teardown.
- One atomic benchmark attempt timed out during initial client-field setup,
  before measurement. Its original log is retained; a fresh pilot and all later
  completed matrix runs succeeded. The cause of that isolated timeout is not
  established, and it is not included in passing operation counts.
- The initial cluster test incorrectly expected insertion counts from the default
  facade, whose documented contract is `:ok`. Counts are now checked through the
  instance facade, while default-facade writes/read-your-write retain their public
  contract. The final 14-test cluster run passed.
- A pre-default-callback umbrella run passed 14,259 tests. The subsequent
  final-source attempt failed a Flow LMDB reconciliation test and later server/
  HTTP checks reported shard unavailability. The entire 155-test LMDB suite passed
  separately, and the complete fresh retry above passed without source changes.
  The failure is not conclusively attributed to the new route or to the host.

Artifacts/reproduction:

- `bench/saturated_write_gate.py`: three-variant serial matrix; `--resume`
  preserves completed reports and failed-attempt logs, `--summarize-only`
  validates/aggregates them, and `--final-source` / `--summarize-final` operate
  on the separate final-source cohort.
- `bench/results/saturated-write-gate-summary.json` and
  `saturated-write-gate-final-summary.json`: complete metrics, maintenance counts,
  source hashes, and compiled-module checksums.
- `bench/results/saturated-write-*-diagnostic-1.json` and
  `bench/saturated_write_summary.py`: request spans and service-time diagnostics.
- `bench/results/saturated-write-saturated-atomic_cached-1.log`: initial setup
  timeout; its `.attempt-1.log` contains the successful retry.
- OpenCode outputs: `tool_0fc6346d50017jx9eTJGkl5zs7` (earlier umbrella pass and
  initial cluster assertion failure), `tool_0fca0ca50001Wg89yYnemhk0uP`
  (final-source matrix and failed umbrella attempt), and
  `tool_0fcbaa2880017IoSK6rhFCpnKL` (final full retry and performance guards).

This is the tenth retained workload-specific steady-state optimization. The
extra single-field HSET round trip and its concurrent-count defect are fixed;
the general WAL/device latency investigation remains open.
