# Post-0.11.23 review: steady-state performance comparison

Date: 2026-09-28. Release source: `e5f59ba7959710773729812343ae9bcd62d8d15f`.

For the later, broader review of ordinary request paths, see
[Overall steady-state performance review](overall-performance-review-0.11.23.md).
It adds measured embedded-dispatch, HTTP cache, native cleanup, and frame
accounting improvements beyond the narrow async/list comparison below.

The subsequent [startup replay metadata experiment](startup-replay-metadata-experiment.md)
was rejected and reverted after its initial timing gain failed to hold up in
follow-up runs. Its artifacts are preserved separately from this steady-state
comparison.

The initial comparison below predates the small async-cleanup optimization.
Its separate before/after measurements and validation are recorded in the
follow-up section near the end of this report.

## Findings

- **The async reply wrapper has measurable CPU overhead.** Its no-I/O control
  lost 36% single-caller throughput and 15% throughput with 16 callers. This is
  the cost of the reviewed lifecycle-safe path, including its process monitors;
  it is not a measurement of disk throughput. Single-caller median latency rose
  from 0.958 to 1.250 microseconds.
- **Actual cached async reads were much closer:** median throughput changed
  -1.1% at one caller and +0.4% at 16. At 16 callers, median trial p99 increased
  from 977 to 1,028 microseconds, with overlapping trial ranges.
- **Append plus fsync showed no consistent regression.** Throughput and latency
  ranges varied between trials; the median throughput differences were +6.8%
  and +1.3% at one and 16 callers respectively. These are not established gains.
- **Single-client list timings were nearly unchanged.** A push plus ready
  blocking pop remained about 31 operations/second; a register/push/wakeup/pop
  cycle remained about 25 operations/second with the default durable backend.
- **Concurrent list capacity is inconclusive.** Ready-list throughput varied by
  more than 3x within each version. Reported median differences below must not
  be treated as a reliable capacity regression or improvement. The cause of
  the variation under the default adaptive batching configuration was not
  isolated in this experiment.
- **The fixed batch-consumer case completes:** every release trial served only
  one of three waiting consumers after one three-value push. Every review trial
  served all three in FIFO order. This is a correctness result, not a comparable
  throughput number.

## Method

- Apple M4 Max, `Mac16,5`, 128 GiB RAM, macOS 26.6.2.
- Elixir 1.20.4, Erlang/OTP 29 / ERTS 17.0.5.
- Eight online BEAM schedulers (`ERL_FLAGS='+S 8:8'`), ten dirty-I/O schedulers,
  four storage shards, WARaft, normal development durability/batching settings.
- Fresh VM and unique temporary data directory for each trial. List keys and
  storage fixtures are identical across variants. List workers use the server's
  resource budget and authorization path. List producers use core command
  dispatch in both variants, so the old embedded-notification bug does not
  invalidate the timing comparison.
- Source-level differential comparison: before starting applications, the
  harness compiles every changed production module from either the release Git
  blobs or the review working tree. It also recompiles `WARaftStorage`, the
  caller of the modified checkpoint macro. Dependencies and unchanged BEAM/NIF
  artifacts are shared. Temporary BEAM files support recovery atom preloading;
  working-tree source and build artifacts are not overwritten.
- Three trials per variant, sequential order **release, review, review,
  release, release, review**. No simultaneous benchmark VMs. The first shell
  batch timed out after four complete result files; the remaining two trials
  were run separately with the same parameters. Smoke runs are excluded.
- One-second warmup for each scenario; five-second async measurements and
  twenty-second list measurements. Concurrency is one or 16 closed-loop client
  processes. Throughput counts all completed operations through the last
  completion; latency covers the entire operation described below.
- Proxy latency samples every 100th operation, cached reads every tenth, and
  durable writes/lists every operation, throughout the measurement interval.
  Single-client list trials have approximately 500–630 samples each, so their
  p99 estimates represent only a handful of observations.
- Tables show **medians of trial-level statistics**, not pooled percentiles or
  confidence intervals. Complete measurements, source hashes, sample counts,
  and per-metric ranges are in
  [`bench/results/oss-review-perf-0.11.23.json`](../bench/results/oss-review-perf-0.11.23.json).

### Workloads

1. `proxy_only`: `Async.await/2` with an immediate synthetic completion. Isolates
   BEAM proxy/alias/monitor/message overhead; performs no native or disk I/O.
2. `pread_4k`: checked-key Tokio async reads from a 256-record, 4 KiB/value
   fixture. **OS-page-cache-warm**, not a cold-device or full router GET test.
3. `append_fsync_256b`: one 256-byte value appended through the async NIF, then
   an awaited async fsync. Each client owns its file. One operation includes
   both completions; this is not a full replicated write.
4. `list_ready`: durable command push followed by a native `BLPOP` worker that
   finds the value immediately. Includes push, worker creation, result delivery,
   and normal worker exit.
5. `list_blocked`: create native `BLPOP` worker, wait for its registration,
   durably push one value, receive the result, and await worker exit. Each client
   owns a separate key; the producer's registration barrier is included.
6. Separate batch probe: register three FIFO consumers, push three values once,
   and count correct completions within one second. Uncompleted release workers
   are explicitly terminated afterward.

## Throughput

Operations/second; brackets show minimum–maximum trial results.

| Workload | Clients | Release median [range] | Review median [range] | Median difference |
| --- | ---: | ---: | ---: | ---: |
| Proxy only | 1 | 776,820 [740,099–855,248] | 497,365 [434,867–531,374] | -36.0% |
| Cached 4 KiB read | 1 | 16,614 [16,575–16,640] | 16,435 [16,132–16,681] | -1.1% |
| Append + fsync | 1 | 205.9 [191.7–222.3] | 219.9 [212.1–227.0] | +6.8% |
| Ready list cycle | 1 | 31.40 [31.32–31.42] | 31.30 [31.12–31.33] | -0.3% |
| Blocked list cycle | 1 | 25.17 [24.69–25.19] | 25.17 [25.15–25.27] | ~0.0% |
| Proxy only | 16 | 3,039,185 [2,985,856–3,095,956] | 2,576,600 [2,540,922–2,608,085] | -15.2% |
| Cached 4 KiB read | 16 | 25,812 [25,674–26,094] | 25,928 [25,744–26,008] | +0.4% |
| Append + fsync | 16 | 431.0 [420.8–455.8] | 436.7 [413.5–437.3] | +1.3% |
| Ready list cycle | 16 | 1,341 [518–1,952] | 509 [496–1,729] | -62.1%; unstable |
| Blocked list cycle | 16 | 566 [538–757] | 1,015 [841–1,021] | +79.3%; variable |

## Tail latency

All values in **microseconds**, again medians of trial-level percentiles.

| Workload | Clients | Release p95 | Review p95 | Release p99 | Review p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Proxy only | 1 | 1.21 | 13.08 | 14.33 | 15.42 |
| Cached 4 KiB read | 1 | 74.96 | 73.71 | 93.58 | 90.83 |
| Append + fsync | 1 | 8,400 | 8,065 | 10,104 | 8,727 |
| Ready list cycle | 1 | 32,447 | 32,543 | 34,776 | 35,958 |
| Blocked list cycle | 1 | 40,452 | 40,458 | 45,840 | 44,979 |
| Proxy only | 16 | 9.58 | 13.96 | 21.67 | 23.83 |
| Cached 4 KiB read | 16 | 799.17 | 812.71 | 977.33 | 1,027.67 |
| Append + fsync | 16 | 63,939 | 62,902 | 83,976 | 81,089 |
| Ready list cycle | 16 | 32,374 | 32,950 | 37,763 | 36,565 |
| Blocked list cycle | 16 | 43,326 | 27,713 | 54,174 | 45,257 |

The single-caller review proxy p95 ranged from 1.96 to 14.38 microseconds;
the scheduling tail is variable even though its throughput loss is consistent.
The review's 16-client blocked-list p99 ranged from 44.3 to 96.1 milliseconds,
versus 46.5 to 93.8 milliseconds for the release. The median alone hides that
tail variability.

## Interpretation and next performance work

The most defensible new issue is CPU overhead in the lifecycle-safe async reply
wrapper. Investigate reducing per-call monitor/proxy bookkeeping if profiling
shows this path consumes meaningful application CPU. Preserve proxy-death and
caller-death handling; removing those protections would reintroduce hangs and
leaks. The measured microbenchmark loss did not translate into a comparable
loss in the actual storage operations tested here.

The concurrent list measurements require a separate steady-state/profiled run,
including per-interval throughput and batching behavior, before changing queue
or batching policy. These results do not establish production capacity or a
production p99 bound. They exclude TCP, TLS, HTTP authentication, Linux
io_uring, cold device reads, multi-node replication, and large waiter registries.
They also do not quantify the checkpoint-cadence fix on a large keydir.

## Follow-up: remove redundant per-proxy cleanup

The retained optimization removes the proxy's explicit
`Process.demonitor(caller_monitor, [:flush])` and its surrounding `try/after`.
This is a one-shot process: after replying or receiving cancellation/caller
death, it exits. BEAM automatically removes its monitors on process exit,
including exceptional exits. The caller still monitors proxy death, and the
proxy still monitors caller death. Timeout cancellation, alias deactivation,
late-reply flushing, and caller-side monitor cleanup remain in place.

Before changing production code, a focused same-VM benchmark compared:

1. The exact lifecycle-safe review helper from the initial comparison.
2. The helper with implicit proxy-monitor cleanup (**retained**).
3. The same change plus earlier caller-side demonitoring (not retained; no
   compelling additional benefit).

Each variant was compiled under a separate module name and called through the
same function-capture interface. Five trials per scenario used rotated/reversed
variant order, 500 ms warmup, three-second measurements, and one or 16 callers.
The fixture, native binaries, and VM were shared; no FerricStore application or
listener was started. This isolates the helper comparison and differs from the
fresh-VM whole-review setup above; do not combine their percentages.

| Workload | Clients | Before ops/s | Retained candidate ops/s | Change |
| --- | ---: | ---: | ---: | ---: |
| Proxy only | 1 | 764,833 | 796,296 | +4.1% |
| Proxy only | 16 | 2,521,684 | 2,638,659 | +4.6% |
| Cached 4 KiB read | 1 | 16,828 | 16,750 | -0.5% |
| Cached 4 KiB read | 16 | 25,892 | 25,864 | -0.1% |

Proxy-only single-caller median latency changed from 1.250 to 1.167 microseconds;
median trial p95 changed from 1.542 to 1.459 microseconds, and p99 from 1.667 to
1.584 microseconds. Concurrent proxy results overlap between trials, so the
4.6% figure is an observed median improvement, not a guaranteed speedup.

The initial three-second, 16-writer append/fsync measurements showed substantial
outliers and a lower candidate median. This was investigated with **five matched
20-second trials per variant**, alternating baseline/candidate order:

- Before: median **393.48 ops/s**, range 379.01–399.05.
- Candidate: median **394.15 ops/s**, range 376.32–402.08.
- Median trial p95: 69.09 ms before, 71.04 ms candidate.
- Median trial p99: 94.00 ms before, 99.11 ms candidate. Trial p99 ranges overlap:
  93.15–111.04 ms before and 91.10–108.97 ms candidate.

The longer measurements did not establish a consistent write-throughput
regression or gain; latency remains variable. The retained change is a small
reduction in helper bookkeeping, not a claim of faster disk or database writes.

After applying it, **176 targeted tests passed**, covering async lifecycle and
cancellation, late replies, cold reads, shard async I/O and failure paths,
filesystem/NIF operations, architecture/scheduler guards, and both performance
guards. Formatting, warnings-as-errors compilation, both CI-equivalent Credo
profiles, and `git diff --check` passed. The earlier 14,223-test full-suite result
predates this cleanup-only optimization; it was not rerun for this follow-up.

The focused benchmark stores the exact baseline and candidate sources, hashes,
and every trial in:

- [`bench/results/async-cleanup-perf.json`](../bench/results/async-cleanup-perf.json)
- [`bench/results/async-cleanup-fsync-confirm.json`](../bench/results/async-cleanup-fsync-confirm.json)

Reproduce it without reverting the applied optimization:

```sh
ERL_FLAGS='+S 8:8' mise exec -- mix run --no-start bench/async_cleanup_perf.exs
ERL_FLAGS='+S 8:8' BENCH_FSYNC_CONFIRM=1 \
  mise exec -- mix run --no-start bench/async_cleanup_perf.exs
```

## Reproduce the whole-review comparison

Run from the uncommitted review worktree based on the exact release commit:

```sh
for spec in release:1 review:1 review:2 release:2 release:3 review:3; do
  ERL_FLAGS='+S 8:8' \
    BENCH_VARIANT="${spec%%:*}" BENCH_TRIAL="${spec##*:}" \
    mise exec -- mix run --no-start bench/oss_review_perf.exs || exit
done

mise exec -- mix run --no-start bench/oss_review_perf_summary.exs
```

Allow sufficient wall-clock time for compilation, startup, shutdown, and
fixture cleanup in addition to the timed measurement intervals. Generated
per-trial files go under ignored `bench/output/oss-review/`; the summary script
validates matching configurations/source hashes and preserves all six trials
in the curated results file.

Rerunning this whole-review comparison now measures the current working tree,
including the cleanup optimization, and generates new results. The initial
results and their source hashes remain preserved in the curated file until
explicitly regenerated.
