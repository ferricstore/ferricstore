# Five-hour performance investigation goal

Requested work window: **2026-10-01 21:09:18 UTC → 2026-10-02 02:09:18 UTC**.

Goal: find measurable FerricStore performance issues, reproduce them, implement
justified fixes, and verify both correctness and performance before retaining
changes. Record accepted fixes, rejected experiments, and actual blockers.

## Priorities

1. Complete promoted-writer publication coverage and rerun the actual
   batch-update consistency counterexample before reconsidering cached reads.
2. Measure the correctness-protected read candidate under sustained and paced
   load, including cold reads, TTL, transactions, compaction, and failures.
3. Investigate remaining durable-write/apply queueing and other measured
   request-path/background scaling costs.
4. Run affected suites, performance controls, and final combined verification
   appropriate to the changes actually retained.

## Working constraints

- Preserve durability, quorum, local apply barriers, publication consistency,
  validated recovery, and resource admission.
- Use fresh isolated fixtures and matched variants; distinguish profiled
  diagnostics from unprofiled acceptance measurements.
- Preserve unrelated services, original data, and existing worktree changes.
- Dependency publication remains paused. FerricStore changes are uncommitted.

Progress and measured decisions will be recorded in the relevant review reports.

## Completed window

Five-hour work completed; checkpoint time **2026-10-02 02:12:21 UTC** (five hours
and three minutes from the recorded start). Results are documented in
[`promoted-publication-followup.md`](promoted-publication-followup.md).

- Protected the observed promoted-writer publication boundary on direct/default
  paths and added failed/killed-publisher fencing.
- Verified the actual batch-update counterexample, including 1,000-update probes:
  5,192,672 direct-instance and 20,061,341 default-path read pairs without backwards
  field versions.
- Found and fixed busy-spinning publication readers and queued publishers, with
  failing-first reduction-budget regressions and paired component measurements.
- Final combined default suite: **14,239 passed**; both performance guards and
  12 installed-dependency cluster checks passed. Static checks passed.
- Protected cached reads remain benchmark-only. Mixed performance evidence and
  lifecycle acceptance are incomplete; long durable-write stalls remain.
- Preserved rejected broader scope hooks and failed/adjusted verification
  provenance in the report. No dependency publication or FerricStore commit.

## Authorized continuation completed (2026-10-02)

- Completed selected-WARaft promoted hot reads with lifecycle-generation,
  blocked/failed replacement, returned-error, and transaction-failure protection.
- Final-source 1,000-update/512-field probe: **11,935,210** successive field-read
  pairs without backwards versions.
- Completed three alternating sustained/paced pairs and a separate final-source
  confirmation. HGET p99 fell from 31.1 ms to 19 µs in the confirmation;
  saturated write p99 rose 90→112 ms. Paced write p99 was 69→67.5 ms.
- Public native TCP and Flow lifecycle controls completed on an isolated
  healthy-capacity APFS volume, with admission enabled and zero operation errors.
- Final default umbrella: **14,253 passed**; two performance guards, **13**
  installed-dependency cluster checks, and both static-warning profiles passed.
- The protected-read acceptance task is complete; long durable-write stalls
  remain separately documented. FerricStore changes remain uncommitted and
  dependency publication/integration stays paused.

## Saturated-write continuation

The [single-field HSET follow-up](saturated-write-followup.md) removes a redundant
durable round trip and performs type/existence/count/write together in Raft order.
It repairs a concurrent insertion-count race and invalid-cold-read publication.
Three matched pairs lower the prior cached-read writer's saturated p99 from
119 to 90.3 ms, with about 18% more mixed throughput and preserved fast reads.
The final-source confirmation is recorded separately. The latest combined retry
passes **14,259 default tests**, two performance guards, and **14** cluster checks.
General WAL/device stalls remain unresolved; publication/integration stays paused
and FerricStore changes remain uncommitted.

## Durable-flush continuation

The [latest investigation](durable-stall-followup.md) reproduces up to 436-ms
full-durability flushes without BEAM, distinguishes them from weaker POSIX fsync
controls, and evaluates descriptor reuse and compaction policies. Unsupported
policy experiments were reverted. A compaction locator race under LFU updates
and cache eviction was fixed and verified with **14,261 passing default tests**,
two performance guards and 14 cluster checks. General full-flush latency remains
open; durability, admission, existing services/data and the publication pause
are preserved.
