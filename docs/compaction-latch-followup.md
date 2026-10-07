# Compaction-latch investigation: yielding policies rejected

Date: 2026-10-05. Continues the
[write metadata queue fix](write-metadata-queue-followup.md).

The subsequent [separate-output experiment](separate-output-compaction-followup.md)
tests a private copy plus a higher foreground tail. Its latency gate also fails,
but the recovery review exposes and fixes deleted-value resurrection during
partial cleanup of the existing compactor's old logs.

## Decision

The long compaction latch is reproduced, but **neither tested yielding policy is
retained in production**. Both let a write proceed before an entire compaction
finishes, while worsening saturated write p99. Production has been restored to
the pre-experiment compactor and worker sources, including the earlier validated
locator-publication fix. The accepted metadata-queue correction remains.

This pass adds a controlled reproduction, archived candidate implementations,
concurrency/recovery regression probes, and matched performance artifacts. It
does not add an accepted compaction speedup or resolve general durable-write tails.

## Protected work and reproduction

The existing worker acquires the collection's compaction latch before spawning
and transfers ownership before allowing the worker to start. The latch covers:

1. Syncing the outgoing active log and creating/durably registering its successor.
2. Copying type metadata and scanning the member catalog in 256-entry pages.
3. Validating/materializing cold values, durably appending each page, and publishing
   relocated source locators while preserving cache residency and LFU.
4. Removing superseded logs and synchronizing the directory.

A disposable fixture with 4,096 fields, 4-KiB values and an injected 20-ms delay
per copied member page reproduces the blocking relationship:

| Controlled diagnostic | Whole-compaction latch | Per-page yield | Four-page yield |
| --- | ---: | ---: | ---: |
| Total compaction time | 438.5 ms | 468.7 ms | 448.6 ms |
| Concurrent HSET time | 433.7 ms | 32.5 ms | 111.9 ms |
| Write finishes before compaction | No | Yes | Yes |

Durable appends and synchronization remained enabled. These injected-delay
measurements identify the protected interval; they are not end-to-end acceptance
results. Artifacts: `compaction-latch-whole-probe-1.json`,
`compaction-latch-pages-probe-1.json` and `compaction-latch-four-page-probe-1.json`.

## Candidate boundaries and recovery checks

The candidates released the latch only after a durable page append and locator
publication, then gave existing 1-ms pollers a chance to acquire it. They
reacquired the latch before the next page's scan/read/append/publication. Versioned
marker identity, keydir table identity and active-file identity were rechecked
after the handoff. Legacy unversioned markers retained whole-compaction locking.
Worker cleanup removed only its own latch ownership.

Four behavioral cases exercise actual durable recovery into a fresh keydir:

- Replacement, deletion and insertion on both sides of the catalog cursor during
  the released interval; newer values/tombstones survive source cleanup and replay.
- Killing the compactor between pages; old sources remain, acknowledged writes
  recover, and a subsequent owner's latch is preserved.
- Deleting and recreating the collection; the prior generation aborts without
  modifying or removing the replacement collection.
- A competing compaction rotates the active file; the interrupted job aborts
  without stranding locators or removing the newer target.

The initial per-page candidate passed **110 targeted tests**, including existing
compaction, latch, accounting, recovery, deletion and flush-enumeration coverage.
The four-page candidate passed the four concurrency/recovery cases. The cases
and candidate loader remain under `bench/regressions/` and `bench/support/` for
explicit isolated runs; they are not enabled by the default application suite.

## Unprofiled saturated controls

Two separate source cohorts each have a fresh-VM control/candidate pair. They
must not be pooled as repeats of the same implementation. Each run uses 16
clients, four promoted 4,096-field hashes, 4-KiB values, 120 measured seconds,
10 seconds warmup and 10 seconds quiet observation. HSET coalescing is off.

| Cohort | Policy | Mixed ops/s | HSET p99 | HSET p99.9 | Maximum HSET |
| --- | --- | ---: | ---: | ---: | ---: |
| Per-page | Whole latch | 1,087.8 | 79.0 ms | 765 ms | 1,144.1 ms |
| Per-page | Yield each page | 1,079.9 | 110.0 ms | 702 ms | 780.6 ms |
| Four-page | Whole latch | 1,078.5 | 84.1 ms | 695 ms | 752.8 ms |
| Four-page | Yield each four pages | 1,071.8 | 131.0 ms | 665 ms | 756.2 ms |

Every run completed **eight compactions**, with no compaction failures, operation
errors, quiet-period compactions, active workers, pending work or retry timers at
final observation. Improvements therefore were not obtained by deferring
maintenance, and the candidate's worse p99 cannot be dismissed as less measured
compaction work. Read p99 remained approximately flat at 19–20 microseconds for
HGET and 13 microseconds for KV reads.

Per-page handoff improves this pair's maximum but increases HSET p99 by about
39%. Four-page handoff increases p99 by about 56%, with essentially unchanged
maximum and slightly lower throughput. Neither satisfies the intended broad
latency gate, so further long acceptance matrices were not run for these policies.

## Fixed offered-load pilot

The per-page cohort also has a separate 200-cycle/s, 60-second offered-load pair,
with five seconds warmup, 16 workers, bounded eight-item worker queues and
1,024-field hashes. Each cycle contains one HSET and three reads.

| Metric | Whole latch | Per-page yield |
| --- | ---: | ---: |
| Offers | 12,000 | 12,000 |
| Client-side drops | 4 | 0 |
| In-window completions | 11,993 | 11,997 |
| Write service p99 | 89.3 ms | 74.1 ms |
| Arrival-to-write-reply p99 | 590 ms | 561 ms |
| Write service p99.9 | 551 ms | 560 ms |
| Maximum write service time | 654.4 ms | 711.0 ms |
| Completed compactions | 8 | 8 |

All accepted operations completed without errors and neither variant deferred
compaction to quiet time. The offered-load improvements do not override the
saturated p99 regression, and service p99.9/max also worsen in this pilot.

## Restoration and retained artifacts

`bench/compaction_latch_summary.py` validates per-cohort shared source/runtime
identity, completed maintenance and histogram counts. It verifies that both
`store/shard/compound/promoted.ex` and `store/shard/info.ex` exactly match the
pre-experiment sources recorded by the first reproduction. The Compound facade
also has no remaining diff from its pre-experiment implementation.

After restoration, **122 targeted application tests passed**, with eight excluded
special-lane cases. The explicit archived four-page candidate harness passes
its four concurrency/recovery tests in a fresh VM. The first archive-harness
attempt encountered the test helper's generated-directory guard; the harness
now owns its private fixture cleanup explicitly and initializes ExUnit directly.

The prior complete production verification remains the
[14,286-test metadata-queue checkpoint](write-metadata-queue-followup.md#final-verification),
including 15 cluster checks and two performance guards. It was not rerun in full
for a production experiment that was completely reverted. Final formatting,
warnings-as-errors compilation, both CI-equivalent Credo warning profiles and
whitespace checks pass.

Main artifacts under `bench/results/`:

- `compaction-latch-summary.json`
- `compaction-latch-pilot-saturated-{whole,pages}-1.json`
- `compaction-latch-pilot-offered-{whole,pages}-1.json`
- `compaction-latch-four-page-saturated-{whole,pages}-1.json`

Ordinary benchmarks default to production `BENCH_COMPACTION_LATCH=whole`.
Explicit `pages` loads the rejected four-page source; `single_page` loads the
earlier rejected per-page source into temporary BEAMs. The regression entrypoint
is `MIX_ENV=test ERL_FLAGS='+S 4:4' mise exec -- mix run --no-start
bench/regressions/compaction_page_yield.exs`.

## Remaining direction

Shortening ownership alone redistributes protected work across more requests;
it does not reduce the native synchronization work. Copying stale values outside
the latch into the current append target would also need a recovery-order proof:
an in-memory locator CAS cannot prevent an older copied record from overriding
a newer acknowledged write during physical replay.

A genuinely nonblocking copy design would require an isolated compaction output,
a separately ordered foreground tail, source retention, collection-generation
checks, and snapshot/crash-safe publication. That design is not implemented or
claimed safe by these yielding probes. The remaining compaction/full-sync tail
problem stays open.

Branch: `codex/oss-full-tdd-review-0.11.23`. Changes remain uncommitted; dependency
integration remains paused.
