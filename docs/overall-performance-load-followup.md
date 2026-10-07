# Overall performance review: sustained load, waiter churn, and multi-node follow-up

The latest [single-field HSET follow-up](saturated-write-followup.md) addresses
the saturated-write tradeoff introduced by faster promoted reads: cached-path
write p99 falls 119→90.3 ms and mixed throughput rises about 18% across three
pairs. It preserves durability and fast reads, fixes insertion-count/read-error
defects, and passes **14,259 default tests** plus **14** cluster checks. Long
durable-I/O stalls remain separately unresolved.

The [subsequent cluster-write investigation](cluster-write-latency-investigation.md)
reproduces a commit-batch timer defect and evaluates an isolated WARaft fix.
Its compiler-aligned gate lowers local three-node median write p99 from 340.8 ms
to 226.5 ms. The dependency source is pushed, but Hex publication and FerricStore
integration are paused. Commit-propagation experiments remain unaccepted.

The [latest memory-accounting follow-up](overall-performance-review-0.11.23.md#5-native-inbound-admission-omitted-retained-metadata)
also fixes native fragment and decoded/queued metadata admission. Its final
affected suites pass with 2,183 server tests and 144 HTTP tests; the measurements
and counts below describe the earlier sustained-load/waiter-cleanup stage.
The complete default umbrella suite has since passed against the combined tree:
11,903 core + 2,183 server + 144 HTTP = **14,230 passed**, with four skipped and
345 excluded, at seed 873483. See the linked review for the exact command and scope.
After the later heartbeat term-fold fix, the final complete retry passes
**14,232 tests**; the installed-dependency cluster suite passes 12 checks and the
deadline-candidate suite passes 13. The cluster report adds the lookup bottleneck
and matched measurements to the earlier latency diagnosis below.

The subsequent [hash-stall investigation](hash-stall-investigation.md) reproduces
long durable-write/queueing stalls. Its cached-read prototype improved HGET tails
but failed an actual batch-publication consistency probe and was reverted. That
report preserves the measurements, counterexample, and blocked Flow control.
The restored production sources match the earlier **14,232-test** verified state.

The later [publication follow-up](promoted-publication-followup.md) repairs the
observed writer boundary and passed 14,239 tests at its five-hour checkpoint. It
retains bounded wait CPU cost. Its subsequent continuation completes the narrower
WARaft promoted-hot-read path with lifecycle/transaction protection, **14,253**
passing default tests and **13** cluster checks. Final-source HGET p99 falls
31.1 ms→19 µs; saturated write p99 rises 90→112 ms, while paced write p99 is
69→67.5 ms. The linked report preserves that tradeoff and the matched public
Flow controls on an isolated healthy-capacity volume.

## Main findings

1. **Retained fix:** native list workers no longer scan all unrelated waiter keys
   when completing a request. In the large-population benchmark, worker CPU work
   fell from about 34,474 to 3,002 BEAM reductions. FIFO handoff, re-registration,
   cancellation, and multi-key cleanup remain covered by regressions.
2. **Unresolved concern:** local three-node WARaft writes had substantially higher
   latency than single-node writes. Forwarded writes can spend roughly 100–130 ms
   waiting for local apply; leader-targeted writes also had large tails. No
   consensus/durability guarantee or default heartbeat policy was changed.
3. **Compaction:** automatic and deferred-compaction controls had similar overall
   throughput and p99 in this particular mixed workload. Both showed occasional
   long hash-operation stalls, so compaction alone does not explain all latency.
4. **Memory:** the ten-minute run completed 32 compactions without errors. ETS
   memory reached about 25 MiB and stabilized, while RSS rose more slowly and
   ended above its initial level. This does not establish a memory leak, nor
   prove long-term memory stability for other workloads.

This follows the [broader performance review](overall-performance-review-0.11.23.md).
All measurements use isolated data and the current uncommitted review tree.

## Sustained mixed read/write workload

`bench/sustained_mixed_perf.exs` creates four promoted hashes, one per shard,
each with 4,096 seed fields holding 4 KiB values. Sixteen closed-loop clients
cycle through:

- 25% acknowledged hash overwrites of each client's own field;
- 25% hot KV reads;
- 25% reads of the last overwritten hash field, checked against the expected
  version;
- 25% reads of unchanged seed fields, checked against their original values.

The fixture has fixed logical cardinality. It uses normal durable WARaft writes,
one GiB cache budget, eight online schedulers, and real automatic promoted-hash
compaction. The diagnostic control defers only promoted compaction by adjusting
its cooldown timestamp in the isolated fixture; it is not a proposed production
setting. Ten seconds of warmup precedes each measured interval.

### Two matched two-minute trials per mode

Run order: deferred, automatic, automatic, deferred. Units below are separate
operation latencies, not full four-operation cycles.

| Mode / trial | Total ops/s | Hash write p99 | Hash read p99 | Hot KV read p99 | Compactions |
| --- | ---: | ---: | ---: | ---: | ---: |
| Automatic / 1 | 866 | 88.4 ms | 27.7 ms | 13 µs | 4 |
| Automatic / 2 | 869 | 86.2 ms | 28.1 ms | 13 µs | 4 |
| Deferred / 1 | 889 | 87.6 ms | 27.8 ms | 13 µs | 0 |
| Deferred / 2 | 867 | 86.2 ms | 28.0 ms | 13 µs | 0 |

No read mismatches, rejected benchmark operations, or compaction failures were
observed. Automatic-compaction maximum hash-write latency approached 0.99 s;
the deferred control reached 1.17 s. Worst ten-second-window hash-write p99 was
239–360 ms automatic and 188–258 ms deferred. Aggregate p99 hides these short
stalls; the result does not justify claiming that compaction has no latency cost.

### Ten-minute automatic-compaction run

- 527,274 measured operations, approximately **878 ops/s**.
- **32 successful compactions, zero failures**, plus validated reads and final
  written values.
- Hash-write p95/p99: **76.5 / 88.9 ms**; maximum **1.061 s**.
- Hash-read p95/p99: **18.3 / 29.3 ms**; maximum **0.992 s**.
- Hot KV read p95/p99: **7 / 14 µs**; maximum **2.350 ms**.
- Worst ten-second-window hash-write p99: **437 ms**.

Latencies are accumulated in bounded histograms: exact microsecond bins below
1 ms, 10-µs bins below 10 ms, 100-µs bins below 100 ms, and 1-ms bins thereafter.
Reported histogram percentiles are upper bucket boundaries. Raw per-window
counts and histograms are preserved; latency samples are not accumulated into
an unbounded list during the sustained run.

### Memory observations

RSS began at **296.1 MiB**, peaked at a sampled **375.5 MiB**, and was **359.2 MiB**
after ten seconds without the workload. Minute medians illustrate the trend:

| Minute | RSS median | ETS median |
| ---: | ---: | ---: |
| 0 | 312.7 MiB | 17.2 MiB |
| 1 | 346.2 MiB | 24.6 MiB |
| 2 | 354.6 MiB | 25.1 MiB |
| 5 | 360.8 MiB | 25.1 MiB |
| 9 | 369.0 MiB | 25.1 MiB |

Worker processes disappeared after completion: process count went from 287
during the early workload to 272 after quiescence, close to 271 before it.
Binary memory was about 132 MiB initially and 134 MiB after quiescence. The
benchmark's own histograms, telemetry, and memory samples contribute to VM
memory, so RSS growth cannot be assigned entirely to the database.

The host filesystem was approximately 82% occupied. The long run recorded
operational pressure in every sampled interval, but no write-rejection flag.
The earlier short runs predated recording those flags; their missing flags
must not be read as proof of no pressure. Neither guardrails nor retained-WAL
defaults were reduced. These are shared-host observations with pressure and
other local workloads, not isolated production capacity limits.

## Large waiter populations and completion churn

`bench/waiter_churn_perf.exs` parks 0, 512, or 4,096 real BEAM processes on distinct
waiter keys, then starts and completes actual native `BLPOP` workers. Results
are checked, and an outbound lease briefly keeps each completed worker alive so
its reduction count can be measured. There are three alternating paired trials,
30 worker completions per trial.

### Confirmed cost and retained fix

The worker originally called `Waiters.cleanup(self())` both after successful
consumption and in its `after` cleanup. That matches by PID over the global
key-indexed table. The worker already knows exactly which list keys it registered.
The retained helper binds each watched key using `Waiters.unregister/2` instead.
The successful-pop helper clears all watched keys before handing the wake-up
to the next FIFO consumer. The outer cleanup repeats that idempotently for
timeouts/errors. General owner-death cleanup retains its existing global scan.

| Unrelated parked processes | Before worker reductions | After worker reductions |
| ---: | ---: | ---: |
| 0 | 2,992 | 3,002 |
| 512 | 4,017 | 3,005 |
| 4,096 | **34,474** | **3,002** |

The no-waiter case adds ten reductions in this fixture. At 4,096 unrelated keys,
worker work falls about 91%. Push-to-result median latency remains around 32 ms:
durable write/dispatch costs dominate, so this is a CPU/scaling improvement,
not an order-of-magnitude end-to-end latency gain. The actual benchmark used
server workers in-process, not 4,096 TCP connections.

The failing-first reduction regression reproduced 1,906 reductions with a small
table versus 33,318 with unrelated rows before the fix. It passes afterward.
A separate regression verifies cleanup of duplicate/multiple watched keys while
the worker remains alive awaiting outbound-capacity release.

### Owner-death storm still has a cost

Killing all parked owners drained the registry in approximately **5.9 ms** for
512 processes and **206.6 ms** for 4,096, with zero remaining registrations.
This is one diagnostic sample per population. The new request-completion fix
does not change monitor-driven owner-death cleanup or establish constant-time
disconnect processing. A reverse PID index would require careful registration,
notification, restart, and race testing before being justified.

## Multi-node measurements

`bench/multinode_mixed_perf.exs` uses the existing test helper to create actual
one-node and three-node WARaft groups with four shards. Eight clients run a
closed-loop mix of one 256-byte overwrite followed by three reads. Writes are
distributed across nodes; reads target the relevant shard's current leader to
avoid conflating eventual follower-read freshness with operation latency.
Every read is validated, and final versions converge on every replica.

Two 30-second trials per shape, alternating order:

| Shape | Total ops/s | Write p50 | Write p95 | Write p99 | Read p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| One node / 1 | 4,342 | 7.71 ms | 8.64 ms | 9.70 ms | 235 µs |
| One node / 2 | 4,951 | 7.60 ms | 8.48 ms | 9.58 ms | 226 µs |
| Three nodes, distributed writes / 1 | **231** | **139.5 ms** | **327.6 ms** | **417.8 ms** | 196 µs |
| Three nodes, distributed writes / 2 | **258** | **98.7 ms** | **286.6 ms** | **380.4 ms** | 201 µs |

The major observed concern is replicated write latency, not hot reads. Two
leader-targeted three-node trials remained slow: approximately **285–287 ops/s**,
write p50 **91.5–111.2 ms**, p99 **290.4–291.2 ms**. Forwarding contributes, but
does not explain the entire difference.

### Trace and heartbeat diagnostic

A separate traced, sequential-write probe across four shards found follower-
targeted calls spending **27.7–131.1 ms** in
`server_waraft_local_apply_wait_us`; many samples were around 108–122 ms.
Typical leader-targeted sequential commits were roughly 10–22 ms, with occasional
124–142 ms outliers. This narrows the concern to commit/replication/application
scheduling and local apply acknowledgement, rather than option parsing or
in-flight admission. Trace spans nest and must not be summed as independent time.

The backend deliberately waits for the writing node's local storage position
after a redirected commit. That protects read-after-write behavior. Removing
the barrier would weaken correctness and is not an acceptable optimization.

The effective default heartbeat interval is 120 ms. A **diagnostic-only 10-ms
override** on disposable peers increased leader-targeted throughput to roughly
368–370 ops/s and reduced p99 to 236–249 ms, but substantial latency remained.
It also increases heartbeat frequency approximately 12x. CPU, network traffic,
idle cost, wide-area behavior, and failure cases were not measured for that
override, so it was **not made a production default or retained tuning change**.

All peer nodes share the same host/filesystem and run test configuration with
four schedulers per VM and one GiB configured memory budget. The host was under
disk pressure and unrelated workloads continued to run. Results expose an
important local performance concern, not a production cluster-throughput
guarantee or proof of a single root cause. Separate-disk deployment profiling
and queue/fsync/commit-propagation measurements are the next steps for cluster
optimization.

## Verification and retained scope

Only the native blocking-worker key-scoped cleanup was added to production in
this follow-up. The earlier four steady-state optimizations and correctness
fixes remain. Consensus, heartbeat defaults, admission budgets, and compaction
policy remain as before these load experiments.

- Native blocking/waiter/resource-budget focused tests: **40 passed** across
  core/server (overlap with the complete suites below).
- Complete protocol-server suite at the waiter-cleanup stage: **2,182 passed**, 1 skipped,
  61 excluded; includes frame accounting and the new cleanup tests.
- Complete HTTP suite: **144 passed**, 5 excluded.
- Three-node replication/failover suite: **12 passed** with `--include cluster`.
- Compaction-plan/worker/dedicated and performance-guard checks: **34 passed**,
  4 excluded.
- Formatting, warnings-as-errors compilation, both CI-equivalent Credo warning
  profiles, and `git diff --check` passed.

The full default core suite and broader distributed/Jepsen, Linux io_uring, and
external SDK lanes were not rerun. All temporary benchmark data and started peer
nodes were cleaned up. Changes remain uncommitted.

## Reproduce and artifacts

```sh
for spec in deferred:1 auto:1 auto:2 deferred:2; do
  ERL_FLAGS='+S 8:8' BENCH_COMPACTION="${spec%%:*}" BENCH_TRIAL="${spec##*:}" \
    mise exec -- mix run --no-start bench/sustained_mixed_perf.exs || exit
done
ERL_FLAGS='+S 8:8' BENCH_SECONDS=600 BENCH_COMPACTION=auto BENCH_TRIAL=long \
  mise exec -- mix run --no-start bench/sustained_mixed_perf.exs
python3 bench/sustained_mixed_summary.py

ERL_FLAGS='+S 8:8' mise exec -- mix run --no-start bench/waiter_churn_perf.exs
MIX_ENV=test ERL_FLAGS='+S 4:4' \
  mise exec -- mix run --no-start bench/multinode_mixed_perf.exs
MIX_ENV=test ERL_FLAGS='+S 4:4' BENCH_LEADER_WRITES=1 \
  mise exec -- mix run --no-start bench/multinode_mixed_perf.exs
MIX_ENV=test ERL_FLAGS='+S 4:4' BENCH_TRACE=1 \
  mise exec -- mix run --no-start bench/multinode_mixed_perf.exs
```

The optional `BENCH_HEARTBEAT_MS=10` flag is for the diagnostic experiment only.
Per-run histograms, memory samples, captured waiter baseline/candidate source,
multi-node measurements, and trace samples are preserved under `bench/results/`:

- `sustained-mixed-{auto,deferred}-{1,2}.json`, `sustained-mixed-auto-long.json`
- `sustained-mixed-summary.json`
- `waiter-churn-perf.json`
- `multinode-mixed-perf.json`, `multinode-mixed-leader-perf.json`
- `multinode-write-trace.json`, `multinode-heartbeat-10.json`
