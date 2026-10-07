# Startup replay metadata experiment — rejected

Date: 2026-09-28.

**Decision: do not retain the production change.** A component benchmark
improved, but the initial small startup gain did not survive follow-up
measurements. Recovery-state snapshots also varied and did not provide a clean
equivalence check. The candidate and its candidate-specific tests were reverted.
The earlier correctness fixes and tested async-helper cleanup are retained.

## Candidate

The empty cold-due proof performs two LMDB flush-marker checks per command.
Each check discovers the environment using non-following file metadata reads.
`File.lstat/1` routes those reads through the shared Erlang file server.

The experiment added a **recovery-only** marker probe using the documented
`:file.read_link_info(path, [:raw, {:time, :posix}])` API. It kept the directory
and regular-file checks, marker normalization, transaction-ID comparisons,
range bounds, and fallback scan. It did not cache results, skip WAL records,
change checkpoint boundaries, or modify the normal claim path.

The exact rejected implementation and regression tests are archived in
[`bench/experiments/replay_raw_metadata.patch`](../bench/experiments/replay_raw_metadata.patch).
It is an experiment artifact, not an applied source change.

## Component results

The component benchmark compares original metadata discovery, POSIX timestamps
only, and raw local metadata reads, with one and four callers. It uses actual
LMDB environments, marker reads, a bounded empty-range read, and transaction-ID
checks. It does not include the complete Raft replay path or cursor calculation.

On macOS, the four-caller empty-proof sequence took a median **1,599.8 ms** for
20,000 operations with the original implementation versus **1,195.5 ms** with
raw metadata reads: about **25% less elapsed time**. One-caller timings were
variable, and POSIX timestamps alone did not establish a benefit.

Script: `bench/replay_lmdb_metadata_bench.exs`.
Full inputs/results: `bench/results/replay-lmdb-metadata.json`.

## Full startup experiment

- Linux/arm64 release images on Docker Desktop, Apple M4 Max host.
- 16 shards, 16 online schedulers, 6 GiB container limit, four adaptive preopen
  workers. Runtime budgets and recovery validation remained enabled.
- Baseline includes the accumulated post-0.11.23 review fixes and async cleanup;
  candidate adds only the archived replay-metadata change.
- A fresh copy of the isolated `ferricstore-offset-consistent-20260923` fixture
  was used for every run. Its allocated size was 33,348,436 KiB, approximately
  31.8 GiB. Copying and filesystem sync completed before timing began.
- The source fixture was mounted read-only; containers had no external network.
  The persisted node identity was preserved inside each isolated container.
- Timing covers application startup through `FerricStore.await_ready/1`.
  Snapshot analysis and request measurements are outside that interval.
- Primary run order: baseline, candidate, candidate, baseline, baseline,
  candidate. Every benchmark container and scratch volume was removed.

### Initial three matched trials

| Trial | Baseline startup | Candidate startup |
| --- | ---: | ---: |
| 1 | 96.523 s | 95.385 s |
| 2 | 97.030 s | 93.266 s |
| 3 | 96.069 s | 95.458 s |
| **Median** | **96.523 s** | **95.385 s** |

The initial median gain was only **1.138 seconds / 1.2%**, not the component's
25%. Summed storage replay time across shards decreased from 144.140 to
135.113 seconds; these are overlapping worker sums, not startup wall time.

Median startup cgroup memory peaks were 2.774 GiB baseline and 2.862 GiB
candidate; maxima were 3.182 and 3.320 GiB respectively. Both stayed below the
6 GiB limit. These kernel peaks include charged page cache and are not directly
comparable to earlier sampled Docker working-set figures.

### Follow-up diagnostics changed the decision

| Follow-up | Baseline startup | Candidate startup |
| --- | ---: | ---: |
| Expiry-filtered snapshot diagnostic | 103.749 s | 100.369 s |
| Key-directory export diagnostic | 99.668 s | 110.274 s |
| Quiescent diagnostic | 100.660 s | 107.056 s |

The snapshot/export work happens after the recorded ready time. The quiescent
diagnostic additionally disabled Flow scheduling, retention sweeping, and policy
migration workers in both instances, then drained projections before snapshot
comparison. Its timings must not be pooled with default-config timings.

The later slower candidate runs contradict a reliable end-to-end improvement.
In the quiescent diagnostic, summed replay itself was also slower: 181.711
seconds versus 150.316 seconds. The result is not explained away as an unrelated
history-scan delay.

## Recovery-state comparison limits

Post-ready key/expiry fingerprints differed slightly even between baseline
runs: approximately 742,822–742,834 rows in the initial cohort. Filtering expired
keys did not eliminate the differences, so this was **not established to be a
simple TTL-expiry effect**.

A separate post-ready key-directory export found:

- Six history-entry keys present only in the baseline snapshot.
- 137 retention-cleanup-member keys, six history-entry keys, and one payload
  value key present only in the candidate snapshot.
- No changed expiry among keys present in both exports.

Scheduling and derived/retention bookkeeping make raw live keydir snapshots an
imperfect recovery-equivalence oracle. Differences still appeared in the
quiescent diagnostic (742,822 versus 742,958 rows). This investigation did not
fully isolate their cause, prove candidate-induced data loss, or prove identical
recovered logical state. It is an unresolved validation limitation, not evidence
to dismiss. The candidate was not retained.

Raw key exports are local, ignored diagnostics under `bench/output/`; curated
results contain aggregate fingerprints and key-family counts, not user values.

## Normal request measurements

The large fixture's baseline hit `KEYDIR_FULL` when admitting new benchmark
keys at 6 GiB. The benchmark did not raise admission limits. Request comparisons
instead used fresh, empty 16-shard instances with the same image/memory settings.
They measure in-process router write/read pairs and the empty Flow-claim API,
including its absent-type path, rather than TCP or a fully populated Flow queue.

Initial five-second measurement medians:

| Workload | Clients | Baseline ops/s | Candidate ops/s | Baseline p99 | Candidate p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Put + get | 1 | 98.23 | 96.25 | 14.060 ms | 13.988 ms |
| Empty Flow claim | 1 | 2,647 | 2,653 | 0.652 ms | 0.660 ms |
| Put + get | 16 | 1,707 | 1,716 | 12.904 ms | 12.805 ms |
| Empty Flow claim | 16 | 5,021 | 4,898 | 4.516 ms | 4.604 ms |

The short concurrent Flow result was followed by three **30-second** trials per
variant, in alternating order. Median throughput was 4,896 versus 4,920 ops/s;
median p95 was 4.157 versus 4.139 ms, and median p99 4.585 versus 4.618 ms.
Ranges overlapped. The short apparent throughput regression was not sustained,
but this does not establish performance equivalence for every workload.

## Correctness and checks

The file-server contention regression failed on the baseline: a warmed recovery
proof remained blocked while the shared file server was paused. It passed with
the candidate. Marker presence/removal, path replacement, symlink rejection,
missing environments, existing due rows, cursor equivalence, and concurrent
LMDB-commit fallback were also exercised.

With the candidate applied, **1,143 tests passed, 34 excluded** across the full
state-machine and WARaft backend suites, LMDB/hibernation suites, architecture
and filesystem guards, and both performance guards. Formatting, warnings-as-
errors compilation, and both CI-equivalent Credo profiles passed.

After reverting it, the four affected production/test files have no diff from
their pre-experiment state. The original replay-proof tests passed (3), as did
the LMDB unit, async-helper, and performance-guard tests (42). The archived patch
passes `git apply --check`. The umbrella suite was not rerun for this rejected
experiment.

## Artifacts and reproduction

Primary results:

- `bench/results/startup-replay-comparison.json`
- `bench/results/startup-replay-live-comparison.json`
- `bench/results/startup-replay-live-confirm.json`
- `bench/results/startup-replay-summary.json`

Diagnostic results:

- `bench/results/startup-replay-fingerprint-check.json`
- `bench/results/startup-replay-keydir-diagnostic.json`
- `bench/results/startup-replay-keydir-diff.json`
- `bench/results/startup-replay-quiescent-check.json`

Local image tags and immutable IDs are recorded in the result files. The
comparison scripts expect those images and the isolated fixture to exist:

```sh
python3 bench/startup_replay_compare.py
BENCH_MODE=live python3 bench/startup_replay_compare.py
BENCH_MODE=live BENCH_FLOW_CONFIRM=1 \
  BENCH_OUTPUT=bench/results/startup-replay-live-confirm.json \
  python3 bench/startup_replay_compare.py
python3 bench/startup_replay_summary.py
```

The runner skips completed trials and rejects mismatched image IDs. Building
the candidate again requires the archived patch in a separate experimental
worktree. Future startup work should establish a stable logical-state comparison
and control background/clock-dependent work before accepting a small timing win.
