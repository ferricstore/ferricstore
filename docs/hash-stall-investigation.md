# Promoted-hash stall investigation

The latest [durable-flush continuation](durable-stall-followup.md) isolates
hundreds-of-milliseconds full flushes outside FerricStore, rejects unsupported
descriptor/compaction policy changes, and fixes locator publication under LFU
updates and eviction. The retained tree passes **14,261 tests**, both performance
guards and 14 cluster checks. General durable-flush delays remain unresolved.

The latest [single-field HSET follow-up](saturated-write-followup.md) removes a
redundant durable type-claim round trip, lowering cached-path saturated write
p99 from 119 to 90.3 ms while retaining fast reads. Its final combined tree passes
14,259 tests, two performance guards, and 14 cluster checks. The original rejected
prototype below and the later read-only acceptance are separate historical stages.

The [publication follow-up](promoted-publication-followup.md) now protects the
identified writer boundary and passes the original actual-writer counterexample
on direct/default paths. It also bounds publication wait CPU cost. Application
cached reads were experimental at its 14,239-test checkpoint. Its continuation
now enables the protected selected-WARaft path, with lifecycle/transaction
coverage, 14,253 passing default tests, and 13 cluster checks. Final-source HGET
p99 is 31.1 ms→19 µs, with a saturated write-p99 tradeoff of 90→112 ms; the linked
report preserves the complete matched results and public Flow controls.
The rejected prototype and historical counts below retain their provenance.

## Historical prototype result

**Decision: the cached-read prototype is rejected and reverted.** It reduced
experimental HGET p99 **31.7 ms → 19 µs**, but an actual concurrent batch-update
probe exposed a consistency regression: successive reads returned version 1
for the first field followed by version 0 for the last field during one batch
update. The serialized baseline did not do that. No new production optimization
is retained from this follow-up.

**Remaining concern:** occasional long durable-write calls and storage-apply
queueing still produce hash-write stalls. Their cost was not removed or hidden
by weakening durability. Restored production sources match the earlier
**14,232-test** umbrella-verified state. The prototype's 14,235 passing tests
were insufficient to establish its concurrent publication correctness.
All FerricStore review changes remain uncommitted; WARaft publication is paused.

## Why the prototype was withdrawn

Checking `PublicationEpoch.read/3` is only sufficient if every relevant writer
marks its cache-publication phase. Some promoted batch writers update individual
keydir rows under the promoted-write/compaction latch without marking that
phase in the epoch read by this prototype. The normal shard read path respects
that serialization; the direct-cache prototype did not.

The probe performs actual 512-field batch updates, with one writer increasing
every field's version together and one reader repeatedly reading the first and
last fields in order. A newer first-field read followed by an older last-field
read cannot be explained by one atomic batch linearization. The prototype
reproduced **[1, 0]** in two runs; the serialized baseline completed both probes
without that counterexample. In the saved rerun, the baseline checked 29,476
pairs, while the prototype found the violation after 4,495 pairs.

`bench/regressions/promoted_cached_read_atomicity.exs` preserves this behavioral
probe. `promoted-cached-atomicity-{cached,baseline}.json` stores the results.
The prototype source is preserved in its measurement artifacts and loaded only
when a fresh benchmark explicitly selects `BENCH_PROMOTED_READ=cached`.
Normal benchmark runs default to the restored baseline. Router source and its
test file have **no retained diff** from this investigation.
The probe asserts monotonic field versions independently of the selected
variant. Before the subsequent writer-boundary fix, selecting the archived
prototype produced a failing check; the follow-up records the repaired outcome.

A future read fast path needs actual writer-publication coverage across promoted
batch puts, deletes, mutations, transactions, and direct/default instance paths.
Tests that manually hold an epoch do not prove actual writers participate in it.
This is a prerequisite for reconsidering the shortcut.

## What the profiles show

The original four-shard workload has 16 clients, four promoted hashes with 4,096
seed fields each, 4 KiB values, and one GiB cache. Each client cycles through one
hash overwrite, one hot KV read, and two validated hash reads. Its earlier runs
showed approximately one-second write/read stalls with both automatic and
deferred promoted compaction.

Two new 120-second diagnostic runs reproduced that behavior:

- Deferred compaction: zero compactions, longest retained write capture **966 ms**.
- Automatic compaction: four successful compactions, longest retained capture
  **818 ms**.
- Long request traces spend most of their time in the acceptor/apply wait. The
  storage actor is sampled in promoted batch apply, with up to three messages
  waiting behind it. Raft actors are frequently idle during these intervals.
- Timed append calls reach roughly **300–330 ms**, and timed `file:datasync/1`
  calls exceed **150–200 ms**. These are call wall times, including scheduling;
  they are not isolated kernel-device latency measurements.
- The shard actor is sampled waiting in `Promotion.wait_for_latch_owner/7`.
  That latch also protects promoted writes, so its name does not establish that
  a compaction is responsible. Deferred-compaction stalls confirm that distinction.

Slow operations coincide across shards and durable-write boundaries. The evidence
narrows the long write tails to durable I/O and serial apply queueing on this
shared, disk-pressured host; it does not identify the underlying device/OS cause
or prove that compaction has no cost. Nested spans must not be added together.

`bench/support/hash_stall_profiler.exs` is opt-in and runs only in a fresh benchmark
VM. It retains 200 actor snapshots sampled at 10 ms, the 24 longest operations
over 100 ms, up to 500 diagnostic events, and latency histograms. Profiling
enables request trace wrappers and function tracing, so these captures are not
pooled with the unprofiled acceptance measurements.

## Experimental attempt to avoid read queueing

`Router.compound_get/3` currently sends every promoted data-field read through the
shard, including an already cached binary. Those reads could wait behind a shard
callback waiting for an unrelated in-progress apply/write, even though no disk
locator or I/O was needed to return the cached value.

The prototype in `apps/ferricstore/lib/ferricstore/store/router/part_09.ex` attempted to:

1. Use the existing promotion classification.
2. Check cached binary rows inside `PublicationEpoch.read/3`.
3. Sample TTL **after** any publication wait, avoiding an expired value being
   accepted using the caller's earlier clock sample.
4. Perform normal read bookkeeping only after the stable result is obtained.
5. Keep the existing serialized fallback for all other row shapes and states.

The prototype's failing-first busy-shard regression suspended a promoted hash's
shard and checked that a cached HGET still completed. A second test held an odd
publication epoch and verified the read waited for the final published value. A third
test expired a row during that wait; it exposed a stale-clock bug in the initial
candidate, which was fixed before the final prototype measurements. Those checks
and existing suites passed, but the later actual-writer probe invalidated the
candidate. The three prototype-only tests were removed with its source change.

## Final prototype measurements — not a retained optimization

Three alternating fresh-VM pairs per workload, same dependencies/NIFs/fixtures,
Apple M4 Max, Elixir 1.20.4 / OTP 29, eight online schedulers. Each paired variant
captures its Router source and BEAM checksum. Medians below aggregate trial
metrics, not pooled request percentiles.

### Closed-loop, 120 measured seconds and 10 seconds warmup

| Metric | Serialized baseline | Cached reads |
| --- | ---: | ---: |
| Mixed throughput | 882.9 ops/s | 889.1 ops/s |
| Hash-read p50 | 90 µs | 5 µs |
| Hash-read p99 (trial range) | 31.7 ms (31.3–31.7) | 19 µs (19–20) |
| Maximum hash read across trials | 720.1 ms | 4.6 ms |
| Hash-write p95 | 74.3 ms | 89.5 ms |
| Hash-write p99 | 87.1 ms | 111.0 ms |
| Maximum hash write across trials | 1.065 s | 1.330 s |
| Hot KV-read p99 | 14 µs | 14 µs |

Each trial completes four compactions with zero failures; all reads and final
written versions are verified. Aggregate throughput is approximately flat
(+0.7%). Read queueing disappears, but write p99 rises about 27% in this
closed-loop mix: clients can reach their next write sooner rather than spending
that time queued on reads. This is a latency-distribution tradeoff, not a claim
that durable hash writes became faster or that the one-second write stalls are
resolved.

### Paced control, 60 measured seconds

Each of 16 clients starts at staggered phases and permits one four-operation
cycle per 100 ms, with no catch-up burst after a delayed cycle. This caps intended
write arrival at 160/s; observed rates are roughly 154–155/s. Pacing waits are
excluded from operation latency. These runs complete no promoted compactions.

| Metric | Serialized baseline | Cached reads |
| --- | ---: | ---: |
| Mixed throughput | 615.7 ops/s | 619.6 ops/s |
| Hash-read p99 | 29.6 ms | 16 µs |
| Hash-write p50 | 33.8 ms | 27.8 ms |
| Hash-write p95 | 57.9 ms | 42.7 ms |
| Hash-write p99 (trial range) | 68.7 ms (67.1–76.4) | 66.5 ms (55.9–66.7) |
| Maximum hash write across trials | 912.9 ms | 665.6 ms |

The near-matched arrival-rate control shows no write-tail regression here and
supports its performance direction, but cannot override the consistency failure.
Shared-host variation, sampling, and small trial counts still prevent a strict
tail-latency bound. The pressure guard
records pressure but no general write rejection in these hash fixtures.

### Protocol and Flow controls

Actual native TCP SET/GET cycles use eight clients, one-second warmup, five
measured seconds, and validated request IDs/values. Across three pairs, cycle p99
is **9.447 ms** baseline (8.682–10.074) versus **9.000 ms** cached (8.628–9.336).
Throughput varies substantially: 1,302–2,207 versus 1,340–2,251 cycles/s. The
string SET/GET path does not exercise the new promoted branch; this is a control,
not an attributed speedup. Data requests correctly use native lane 1.

The public Flow create/claim/complete performance control was attempted but could
not start: the host's disk-pressure admission guard returned
`BUSY ... new Flow creates paused ... reason=disk_pressure`. A measured Flow
comparison remains blocked until a healthy-capacity fixture is available.
Guardrails were not overridden. Flow functional/recovery tests pass within the
full suite, but that is separate from the missing performance control.

## Verification provenance

The experimental candidate passed this complete default run before the
additional concurrency review found the regression:

```text
mise exec -- mix test --seed 873483 --max-failures 1 --timeout 180000
core:   11908 passed, 3 skipped, 279 excluded (2595.2 seconds)
server:  2183 passed, 1 skipped,  61 excluded (260.3 seconds)
HTTP:     144 passed,             5 excluded (6.0 seconds)
total:  14235 passed, 4 skipped, 345 excluded
```

Both performance guards pass separately. Formatting, warnings-as-errors compile,
both CI-equivalent Credo warning profiles, and whitespace checks pass. The
nine-test prototype Router suite overlaps that full run. The earlier 112 focused
promotion/compaction/publication checks preceded the final TTL regression.
Rust and WARaft dependency source were not changed in this follow-up.

After reverting the prototype, the existing promoted-read/promotion/compaction/
publication suites pass **110 tests**, four excluded. The archived-prototype
concurrency probe reproduces [1, 0], while the restored baseline reports none.
Formatting, warnings-as-errors compilation, both CI-equivalent Credo profiles,
and whitespace checks pass again. The restored production source matches the
earlier 14,232-test full-suite state; the full suite was not repeated after
reversion. No passing prototype count is presented as acceptance evidence.

## Reproduce and artifacts

- Diagnostic captures: `hash-stall-{deferred,auto}-profile-1.json`.
- Final gate: `hash-read-final-gate-{baseline,cached}-{1,2,3}.json` and its summary.
- Final paced control: `hash-read-final-paced-{baseline,cached}-{1,2,3}.json` and
  its summary.
- Native controls: `promoted-read-controls-{baseline,cached}-{1,2,3}.json`.
- Earlier `hash-read-gate-*` / `hash-read-paced-*` captures describe the candidate
  before its TTL refinement and are preserved separately, not pooled with final data.

```sh
ERL_FLAGS='+S 8:8' BENCH_PROFILE=1 BENCH_COMPACTION=deferred BENCH_SECONDS=120 \
  BENCH_OUTPUT=bench/results/hash-stall-deferred-profile-1.json \
  mise exec -- mix run --no-start bench/sustained_mixed_perf.exs
python3 bench/hash_stall_summary.py bench/results/hash-stall-deferred-profile-1.json
```

For unprofiled pairs omit `BENCH_PROFILE`, use `BENCH_PROMOTED_READ=baseline` or
`cached`, and alternate order across three trial IDs. Use `BENCH_SECONDS=120`
with `BENCH_CYCLE_MS=0` for the closed loop, or 60 seconds/100 ms for the paced
control. Write separate final-cohort filenames, then run:

```sh
python3 bench/hash_read_summary.py gate hash-read-final-gate
python3 bench/hash_read_summary.py paced hash-read-final-paced
BENCH_CONTROL_CASES=native_set_get ERL_FLAGS='+S 8:8' BENCH_PROMOTED_READ=cached \
  mise exec -- mix run --no-start bench/promoted_read_controls.exs
```

For the recorded comparisons, the baseline macro and Router caller were
recompiled only in a fresh VM and
temporary BEAM directory. Installed/workspace source is not overwritten. The
next write-path experiment needs to isolate scheduler/device queueing and test
safe apply batching with durability/publication boundaries explicitly preserved.

The variant loader now uses current workspace code for `baseline` and compiles
only the archived, rejected prototype for `cached`. To check the consistency
counterexample without applying it to the workspace:

```sh
MIX_ENV=test ERL_FLAGS='+S 8:8' BENCH_PROMOTED_READ=cached \
  mise exec -- mix run --no-start bench/regressions/promoted_cached_read_atomicity.exs
MIX_ENV=test ERL_FLAGS='+S 8:8' BENCH_PROMOTED_READ=baseline \
  mise exec -- mix run --no-start bench/regressions/promoted_cached_read_atomicity.exs
```
