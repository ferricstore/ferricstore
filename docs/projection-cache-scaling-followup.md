# Apply-projection cache scaling experiment

Continuation: [sparse offset fallback scan correction](offset-fallback-scan-followup.md)
audits preserved sidecars, identifies negative lookups, and retains bounded
read-ahead with matched fixed-work and CRC/latest-frame verification. The cache
candidate below remains archived; the larger lifecycle goal remains open.

Date: 2026-10-05. Continues the
[snapshot reconciliation follow-up](flow-snapshot-reconciliation-followup.md).

## Decision

The cache-selection and bounded durability-batching candidate improves its
component diagnostic and passes targeted correctness checks, but **does not pass
the required long snapshot lifecycle control**. It is archived and removed from
production. The previously retained source-wait, reconciliation-page and history-
request corrections remain the production worktree checkpoint.

`cold_state.ex` and `waraft_segment_reader.ex` are verified exactly restored to
their pre-experiment sources. The candidate remains available only through an
explicit fresh-VM benchmark loader and archived regression harness. No overall
shutdown improvement or service-latency acceptance is claimed from this pass.

## Supported component cause

The shared apply-projection cache is an ETS `:set`. Selection for a partially
bound `{root, index, key}` key traverses the table. Repeating that selection once
per requested index multiplies the scan cost by the index count. Cold-state
preparation also performs durability work separately for each WARaft group.

`bench/apply_projection_cache_scaling.exs` compares equivalent selections, checks
their complete results, and prepares 128 real cached Flow source records with
normal durable projection spilling. This is a component diagnostic, not a
database throughput or lifecycle gate.

| Unrelated cache rows | Ten per-index selections | Ten single-pass selections | Source preparation before | Candidate preparation |
| --- | ---: | ---: | ---: | ---: |
| 0 | 9.98 ms | 0.364 ms | 248.32 ms | 113.04 ms |
| 10,000 | 299.00 ms | 2.571 ms | 287.50 ms | 101.25 ms |
| 50,000 | 1,999.68 ms | 14.664 ms | 680.26 ms | 91.35 ms |

Artifacts: `projection-cache-scaling-{before,after}-1.json`. These are single
exploratory before/after measurements. Their source hashes identify the measured
versions; the later single-group error-path refinement is separately captured in
the lifecycle source archive.

## Candidate and correctness checks

The candidate selects all requested complete indexes with one membership-map
match specification. Count/byte-limited spills use the same selection helper;
group ordering, full-index spill records, CRC verification and compare-before-
delete cache accounting remain enforced.

Hot and cold source preparation collect apply-projection references into batches
of at most `Limits.max_projection_page_records()` (256). Successful shared checks
precede physical locator preparation and source reads. A failed shared check
falls back to per-group handling, retaining changed-source versus stable-failure
classification. A lone group keeps its original path to avoid duplicate error-
path attempts.

New regressions cover complete sibling records, unrelated indexes/shards, exact
cache count/byte accounting, unchanged-source failure and source replacement
during a failed batch. The unchanged source pays **64 spill calls** and fails the
batching regression; the candidate passes. An existing churn-budget assertion
initially caught duplicate single-group attempts (six instead of three); the
single-group refinement restores the original attempt count.

The candidate plus existing cold-source/security/retention checks pass **48
tests** together. The archived candidate's three new regressions also pass in an
explicit private-BEAM VM after production restoration. They are not part of the
default application test load.

## Lifecycle acceptance failed

A sixty-second native scenario followed by sixty seconds of Flow completes
33,613 native cycles and 704 Flow cycles with zero operation errors, but **all
four storage snapshots fail the existing 30-second writer handoff budget**.
Application stop eventually completes in 151.03 seconds. The strict control
fails and preserves the fixture (`router-control-data-27431`).

The observed Flow p99 is 1.565 seconds, substantially worse than the preceding
cohort. An unchanged legacy-source control also slows severely: its equal-work
1,280-cycle Flow scenario takes 155.25 seconds (8.24 cycles/s, p99 1.813 seconds),
versus roughly 141 cycles/s in the earlier cohort. Its outer 360-second command
budget expires before a shutdown report is captured. Another attempted legacy
control expires during compilation. These failures prevent attributing the
workload slowdown solely to the candidate or pooling the older and newer cohorts.
They do not convert the candidate's failed lifecycle gate into a pass.

Artifacts: `flow-cache-long-batched-{control,stop}-1.json`,
`flow-cache-fixedwork-legacy-control-{1,2}.log` and the second workload JSON.

An outside-BEAM five-second `F_FULLFSYNC` probe succeeds without errors, with
13-ms median, 22-ms p99 and 24.56-ms maximum synchronization time
(`projection-cache-host-fsync-1.json`). Host load/capacity and compiler/workload
behavior also differ from the preceding cohort. This probe is a component
control; it does not establish the cause of the database's longer stalls.

## Latest diagnostic and next target

A smaller fresh diagnostic completes 640 native and 640 Flow cycles, then exceeds
its sixty-second shutdown budget. It has no recorded reconciliation failures and
only 31 exhausted source-retry observations. Writers spend substantial sampled
time in `locate_disk_record_offset_fd/9` and `file:read/2`, holding or waiting for
the serialized projection permit. A completed reconciliation source-read call
takes 10.58 seconds. History actors are idle.

Artifact: `flow-cache-current-stall-{control,trace}-1.json`; the fixture
`router-control-data-33305` is preserved. This workload's absolute timings are
diagnostic rather than an accepted matched result.

The next investigation should distinguish **why offset lookup falls back**:
missing/out-of-window registry rows, missing or rejected derived index slots,
untrusted sidecars and legitimate repeated-index projection frames. It must
measure and retain CRC/ordinal/latest-record validation, including malformed or
stale sidecars. Caching a location or changing scan behavior without that evidence
would not establish a safe fix. No offset-index policy change is included here.

## Restoration and verification

- Both experimental production files exactly match their pre-experiment HEAD
  sources after restoration (`git diff --exit-code` for those paths).
- **45 retained-source cold-state/security/retention tests pass** afterward;
  log: `projection-cache-restored-verification.log`.
- **Three archived candidate regressions pass** through
  `MIX_ENV=test ERL_FLAGS='+S 4:4' mise exec -- mix run --no-start --no-compile
  bench/regressions/flow_cache_batching.exs`;
  log: `projection-cache-archived-no-compile-regressions.log`.
- Formatting, warnings-as-errors compilation, both specified Credo warning
  profiles, whitespace and unchanged dependency-lock checks pass.

The exact archived sources are in the `cache_sources` field of
`flow-cache-current-stall-control-1.json`. The loader validates their hashes
before compiling private BEAMs. Ordinary runs use `BENCH_FLOW_CACHE=retained`;
`batched`, `select_only` and `batch_only` are explicit experimental modes.

The prior 2,535 affected-suite / 15 cluster / two-guard checkpoint still describes
the retained production source. The earlier 14,291 whole-application checkpoint
predates the retained history/page changes and is not relabeled as a new result.
No new accepted production optimization results from this experiment.

Branch: `codex/oss-full-tdd-review-0.11.23`; changes remain uncommitted. Dependency
publication/integration remains paused. Long snapshot lifecycle acceptance remains
open.
