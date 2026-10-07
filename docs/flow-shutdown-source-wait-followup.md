# Flow shutdown: bounded batch source waits

Continuation: [snapshot reconciliation and history request bursts](flow-snapshot-reconciliation-followup.md)
isolates the larger workload's reconciliation error on both writer controls,
retains bounded pages/coalesced handled requests, and records current affected
verification plus remaining long-workload writer drain failures. Results below
describe the preceding source-wait checkpoint.

Date: 2026-10-05. Continues the shutdown diagnostics recorded in the
[separate-output compaction follow-up](separate-output-compaction-followup.md).

## Supported cause and retained change

The reproducible snapshot/shutdown stall occurs while LMDB writers prepare
versioned Flow state and query projections. A missing source used to consume its
own complete retry window inside the shared runtime LMDB flush permit. A batch
of many missing sources therefore waited approximately once per key while every
other shard's flush queued behind it.

The writer now observes every versioned source once per batch attempt and returns
the permit before waiting. Sources in the batch share the configured retry
window. Preparation, source-reference durability checks, LMDB writes, compare-
conflict retries and replay-marker advancement remain inside the existing
serialized permit boundary. No prepared record map is retained while sleeping.

The same bounded retry/sleep configuration and combined maximum wait budget are
used. A source that remains pending or too old at the final attempt still fails
with its existing typed error. Missing sources use the existing final cleanup
semantics. CRC/read/context failures remain errors, and unresolved sources cannot
authorize a replay-safe watermark.

The direct two-argument `ProjectionOps.expand_ops/2` path retains its original
waiting contract. `LMDBWriter` uses the new probe/final preparation path for
batched versioned sources. Non-versioned projection paths retain their existing
handling. No concurrency limit or synchronization primitive is weakened.

## Attribution

Bounded fresh-VM probes record history pending/requested/projected counters,
coordinator holders/queues, actor stacks, source-retry outcomes and commit shapes.

- History projector queues drain during the failing shutdown probes.
- WARaft storage then waits in `LMDBWriter.prepare_snapshot_install/2` for the
  writer's flush.
- The single runtime permit remains held by a writer sleeping in
  `retry_versioned_source_read/6`; writers on other shards queue for that permit.
- A short probe records 63 exhausted source reads, all `:not_found`, with expected
  versions 2 or 3 and absent source rows. This excludes a hypothesis of one long
  hardware synchronization call or one unchanged-marker rewrite loop.
- Background policy-fence commits can continue during the attempted stop. They
  are recorded but were not the wait mechanism changed here.

The earlier historical-time control (`now_ms=1000`) exercises immediate retention
pressure. Wall-clock controls reproduce the stall too, so the diagnosis does not
rest solely on synthetic old timestamps. `HistoryProjectedIndex.persist/2`
already avoids rewriting an unchanged durable marker; its durability logic was
not altered.

The first exhausted-retry trace used call-only instrumentation. Its retry sample
durations are not live-call durations and must not be interpreted as such. The
diagnostic collector now stores exhausted-retry examples/counts separately and
does not leave those calls in its active-call table.

Artifacts: `flow-shutdown-{progress,coordinator,wall,work,source}-baseline-1.json`.

## Behavioral checks

New `source_wait_test.exs` cases exercise public writers with isolated instance
fixtures:

1. Sixty-four missing versioned sources complete within one bounded batch wait
   instead of paying a full retry window for each key.
2. An independent shard's flush proceeds while another writer waits for its
   source; runtime LMDB serialization remains enabled.
3. A source published during the window is validated and projected with its
   actual durable locator.
4. An unresolved source cannot advance a requested replay-safe watermark.

The writer/projection/control/restart tests and coordinator checks pass together:
**106 tests**. Existing source-version, replacement-before-durability, deleted-
source cleanup, retained terminal row, malformed-input and marker-ordering checks
are included. Formatting, warnings-as-errors compilation, both configured Credo
warning profiles and whitespace checks also pass at the frozen source checkpoint.

The same four new cases are also run in fresh private-writer VMs using
`bench/regressions/flow_source_wait.exs`. The sequential writer fails both the
shared-window and cross-shard progress checks; its delayed-source and watermark
cases pass. The batch writer passes all four. These results distinguish the wait
policies rather than merely exercising the new code. Logs are
`flow-source-wait-{sequential,batch}-regressions.log`.

## Controlled lifecycle results

A wall-clock short control previously exceeded the 30-second shutdown budget.
After the change, the first corresponding diagnostic shuts down successfully in
**4.95 seconds**, including its normal durable snapshot work.

The matched fixed-work control uses eight clients, eighty cycles per client,
640 total Flow create/claim/complete/get cycles, wall time, zero warmup cycles and
the same snapshot/HSET settings. Only the source-wait writer variant differs:

| Metric | Sequential source waits | Batch source waits |
| --- | ---: | ---: |
| Completed cycles / errors | 640 / 0 | 640 / 0 |
| Measurement elapsed | 2.194 s | 2.285 s |
| Cycle p99 | 64.5 ms | 51.0 ms |
| Traced shutdown | Exceeds 20 s | 4.48 s |

These snapshot/shutdown traces are diagnostics. A separate unprofiled fixed-work
batch run completes 640 cycles without errors and shuts down in **3.58 seconds**
(`flow-shutdown-unprofiled-batch-1.json`). The modest measurement-time difference
is retained rather than labeled a universal throughput improvement: source
projection work and remaining native I/O costs are workload-dependent.

The sequential control compiles the release-baseline `LMDBWriter` into a private
fresh-VM BEAM; all other source/reference/durability modules remain shared. The
loaded writer MD5 and exact writer source are recorded in each control report.
Installed dependency files and compiled workspace artifacts are not overwritten.

### Larger alternating equal-work controls

`bench/flow_source_wait_controls.py` runs three serial, alternating pairs in fresh
VMs. Each trial completes **1,280 native SET/GET cycles and 1,280 public Flow
cycles**, with eight clients, wall-clock timestamps, zero warmup, default-off
automatic HSET coalescing and the same compactor/snapshot policy. It validates
shared source and loaded BEAM identities, the distinct writer identities and
exact completed-work counts. Percentiles are computed per trial; the table gives
the median of those trial results, not pooled percentiles.

| Metric | Sequential source waits | Batch source waits |
| --- | ---: | ---: |
| Native cycles/s | 702.1 | 744.2 |
| Native cycle p99 | 26.24 ms | 27.01 ms |
| Native cycle p99.9 | 31.68 ms | 28.36 ms |
| Flow cycles/s | 153.8 | 159.2 |
| Flow cycle p50 | 43.73 ms | 46.30 ms |
| Flow cycle p99 | 114.68 ms | 108.21 ms |
| Flow cycle p99.9 | 146.97 ms | 119.80 ms |
| Application-stop completion | Exceeds 60 s in all three | 28.32–30.15 s in all three |

Native p99 and Flow p50 regress modestly in this cohort; individual trials and
maximums vary. These measurements support the bounded source-wait correction,
not a universal throughput or latency improvement. They ran on the shared host
while the separate full application verification was active.

The initial larger cohort used a **20-second** stop budget. Both variants exceed
that budget in all three trials. Those results remain in
`flow-shutdown-matched-summary.json`. A following diagnostic completes in
27.46 seconds and attributes substantial remaining work to history projection:
938 atomic replacements, with history-marker publication consuming much of the
shutdown interval. The next matched cohort uses an explicitly recorded
60-second budget; the earlier failures are not converted into passes.

Artifacts: `flow-shutdown-matched-60s-summary.json` and its six workload/stop/log
triples; `flow-shutdown-mixed-trace-batch-1.json` is diagnostic only.

### Application stop is not proof of successful snapshots

`WARaftBackend.stop/0` currently ignores snapshot results. Consequently, an
application-stop return of `:ok` can coexist with failed snapshot preparation.
The unprofiled timings above measure **operation replies and application-stop
completion**; their snapshot results were not observed.

A separate sixty-second-per-scenario unprofiled control completes **47,072 native
cycles and 5,137 Flow cycles**, with zero operation errors. Native p99 is 20.50 ms;
Flow p99 is 157.78 ms and maximum is 1.02 seconds. Application stop returns after
**150.35 seconds**. This is a long remaining lifecycle cost, not an acceptance
claim for successful snapshot flushing.

The corresponding longer diagnostic completes application stop in 176.63 seconds
but records history-flush timeouts during snapshot preparation, LMDB reconcile
health errors, history-projector queues near **10,000 messages**, and 9,349 atomic
replacements. The original source-wait amplification is reduced, but history
request/marker amplification and reconciliation remain open.

The bounded tracer now records snapshot return values and waits for trace
delivery before collecting its report. Traced controls require all four actual
storage snapshots to return `:ok`; otherwise they exit nonzero and preserve the
fixture, even if application stop succeeded. The first tracer revision retained
only the latest twenty snapshot returns, which were displaced by repeated
backend-unavailable probes after teardown. That older long artifact still
contains the real failed returns in `slow_calls`; the corrected collector retains
the first 128 returns.

A current larger snapshot diagnostic stops in **27.03 seconds**, but only three
storage snapshots succeed; the fourth returns
`{:flow_lmdb_reconcile_unhealthy, %{lmdb_errors: 2, ...}}`. The stricter control
correctly fails and preserves `router-control-data-2851`. This failure has not
been isolated as a regression of the batch-wait change versus a pre-existing
reconciliation issue. It prevents declaring the broader snapshot/lifecycle goal
complete.

A following small Flow-only control completes **640 cycles** and shuts down in
**6.01 seconds**, with all four storage snapshots explicitly returning `:ok`.
Its report distinguishes `application_stop_completed`,
`snapshot_results_verified`, and `storage_snapshots_succeeded`. This validates the
stricter diagnostic on a successful case as well as its rejection of the larger
failed case; it does not erase that larger workload's failure.

Artifacts:

- `flow-shutdown-long-retained-{controls,stop}-1.json` and workload log;
- `flow-shutdown-long-snapshots-{control,trace}-1.json` and diagnostic log;
- `flow-shutdown-mixed-snapshots-{control,trace}-1.json` and diagnostic log.
- `flow-shutdown-small-verified-{control,trace}-1.json` and diagnostic log.

## Broader verification status

The first whole-core attempt stopped after 7,001 passes because an HSET fixture
reported `ERR shard not available` before its initial mutation. The narrowed
Flow/HSET sequence passed all 162 cases. The HSET fixture now uses the existing
strict default-pipeline readiness probe before capturing its context; all seven
HSET cases pass with that preparation.

The initial failed run remains in `flow-shutdown-retained-verification.log`.
Frozen-source serial fresh-VM application verification completes successfully in
`flow-shutdown-retained-retry-verification.log`, with seed `873483`,
`--max-failures 1` and `--timeout 180000`:

| Application | Passed | Skipped | Excluded | Elapsed |
| --- | ---: | ---: | ---: | ---: |
| Core | 11,964 | 3 | 282 | 3,549.6 s |
| Server | 2,183 | 1 | 61 | 256.4 s |
| HTTP | 144 | 0 | 5 | 5.5 s |
| Total | **14,291** | **4** | **348** | |

Formatting, warnings-as-errors compilation, both specified Credo warning
profiles and `git diff --check` pass. The dependency lock remains unchanged.

The current-source three-node run initially reports **13/15 passes**: a promoted
replica remains on version 1 when version 2 is expected, followed by compaction-
latch timeouts and a later apply test timeout. With the same seed (`395667`) and
unchanged source, the isolated replication test passes, then the whole three-node
suite passes **15/15**. The first failure remains unexplained; the passing rerun
does not prove it was harmless or unrelated. Both explicit performance guards
also pass. No test assertion or cluster timeout was relaxed.

The previous completed production checkpoint was 14,287 application tests,
fifteen three-node checks and two performance guards. Broad non-macOS, Jepsen,
SDK and large-allocation lanes are not all covered by this local continuation.

## Decision and next unresolved work

The source-wait change has distinct before/after regression coverage and removes
the measured per-key retry-window/permit amplification. It is retained as a
scoped correction in the uncommitted worktree. No general write-tail or complete
snapshot/lifecycle acceptance is claimed.

The next investigation must isolate the larger workload's reconciliation error
against a matched sequential-source control, and address the history
request/marker backlog without advancing replay cuts ahead of unprojected
history. Snapshot health, explicit completed maintenance and larger-workload
durable lifecycle checks remain the acceptance gates. The preserved failed
fixture and strict snapshot-result collector support that continuation.

Branch: `codex/oss-full-tdd-review-0.11.23`; changes remain uncommitted. WARaft
publication/integration remains paused at the user's request.
