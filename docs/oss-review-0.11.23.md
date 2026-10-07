# OSS correctness and performance review after 0.11.23

Base: `e5f59ba7959710773729812343ae9bcd62d8d15f` (release 0.11.23).

The subsequent [overall steady-state performance review](overall-performance-review-0.11.23.md)
covers KV/collections, Flow/query, native/HTTP, and background/memory paths. It
retains four additional targeted optimizations with separate benchmark and
verification evidence; the test totals below describe the earlier correctness
passes rather than that later working-tree state.

Three review passes confirmed and fixed 13 production defects, with failing
behavioral regressions before the corresponding fixes, plus one shared-test
fixture cleanup. These are post-release working-tree changes.

## Scope

Risk-oriented review across the OSS application boundaries: Raft startup and
recovery, checkpoint cadence, storage publication and cold reads, compaction
plans, Flow-history checkpoints and LMDB access, query admission/cursors,
native protocol buffering/resource budgets, HTTP admission/authentication,
and native asynchronous I/O admission. This is not a proof that every source
line or failure interleaving is defect-free.

## Confirmed defects and regression tests

### Checkpoint interval bypass (performance)

`WARaftStorage.Sections.ApplyResult` interpreted a last-start monotonic timestamp
at or below zero as "not started". BEAM monotonic timestamps can be negative.
After a successful checkpoint, sufficiently frequent writes could therefore
start another full keydir scan before the configured interval elapsed.

The behavioral test writes through Raft, waits for the first background
checkpoint, then writes again within the minimum interval. Before the fix it
observed another scan; afterward it does not. Missing timestamps now use `nil`,
and elapsed time is calculated for every integer timestamp.

### Orphaned prepared storage workers (resource lifecycle)

Startup preopen workers monitored by the caller did not monitor that caller in
return. A caller killed before adopting prepared storage could leave workers
waiting indefinitely with ETS tables, storage handles, and registry entries.

The regression pauses one worker, waits for a different worker to publish its
prepared handle, and kills the startup caller. Before the fix the prepared
worker remained alive. Workers now monitor the caller and release their state
on its `DOWN` message. Recovery already in progress reaches the handoff receive
before processing this message; this does not forcibly interrupt native I/O.

### HTTP admission leak on stream initialization failure (availability)

HTTP admission acquired a slot before invoking the next Cowboy stream handler.
If that handler raised, threw, or exited, initialization returned no state and
Cowboy could not invoke its termination callback. The slot remained occupied,
eventually rejecting healthy requests after enough failures.

The regression verifies all three failure classes plus a subsequent successful
request. It failed with one permanently occupied slot before the fix. Failed
initialization now releases the slot and re-raises the original failure with
its stacktrace.

### Authentication work retained after caller death (availability/memory)

The HTTP authentication cache cancelled work when callers timed out normally,
but did not observe killed request processes. A slow or stuck authentication
task could retain a pending slot after all its callers had disappeared.

Two regressions failed before the fix: orphan work did not terminate, and a dead
coalesced waiter remained registered. Pending callers are now monitored. Death
uses the same cancellation path as timeout, and the shared task is cancelled
only after its last waiter disappears. Successful completion removes caller
monitors. Cached-hit reads do not acquire these monitors.

### Additional lifecycle defects (second pass)

The second pass also confirmed the following lifecycle defects:

- **Infinite wait after async reply-proxy death:** `Bitcask.Async.await/2`
  previously waited only for a reply. If its proxy died, completion-bound
  (`:infinity`) callers could remain blocked forever. It now monitors the proxy
  and returns an explicit `{:error, {:proxy_exit, reason}}` rather than implying
  successful I/O. The regression kills the proxy after submission.
- **Orphan async reply proxy:** a submitted proxy waiting for completion did
  not monitor its caller. It now exits when the caller dies. This cleans up
  BEAM bookkeeping; it does not claim to cancel already submitted native I/O.
  A regression kills an infinite-wait caller and observes proxy termination.
- **Dead LMDB admission waiters retained:** the flush coordinator monitored
  holders only. Dead queued processes remained in the queue until capacity
  became available, retaining memory and increasing queue-scan work while a
  long-running holder kept its permit. Waiters are now monitored on enqueue;
  the same monitor is reused on grant. A regression verifies removal before
  the holder releases, without admitting another writer early.

### Stale publication token releases a newer writer's latch (read consistency)

Closing the same publication token twice could delete a newer write's latch
when both tokens belonged to the same PID. A reader then treated the odd epoch
as abandoned, repaired it, and observed partially published rows.

The regression closes an old token, starts a new write, publishes only its first
row, and closes the old token again. It previously returned `{:new, :old}`.
Latch release now requires successfully advancing that token's own epoch with
compare-and-exchange. The reader waits and returns `{:new, :new}`.

### Waiter cleanup stops after monitor restart (resource lifecycle)

The waiter PID registry survives a restart of `Waiters.Monitor`, but the previous
process's monitor references cannot deliver `DOWN` to its replacement. Dead
waiters could remain registered indefinitely after such a restart.

The monitor now rearms surviving registry entries during initialization. The
regression registers a waiter, restarts the supervised monitor, kills the
waiter, and verifies automatic removal without requiring a subsequent push.

### Lost native list wake-up after a competing consumer wins (availability)

`Waiters.notify_push/1` consumes a registration. If the awakened native worker
found that another consumer had already removed the value, it returned to
waiting without registering again. An infinite blocking request could never
observe another push.

Workers now register and recheck after unsuccessful wake-ups, keeping the
original absolute deadline. The regression consumes a notification on an empty
list, verifies re-registration, and then pushes a real value. It covers `BLPOP`,
`BRPOP`, `BLMPOP`, and `BLMOVE`. The existing queued-notification deadline test
also passes.

### Missing native FIFO handoff strands remaining list consumers (availability)

List pushes intentionally wake only the oldest waiter. Native blocking workers
did not hand off that wake-up after successfully consuming a value, so a single
multi-value push could leave later consumers blocked while values remained.

Four regressions register three workers in order, issue one three-value push,
and verify the values assigned to each worker. All four command variants failed
before the fix. Successful native pops/moves now remove their own registrations,
check the remaining source-list length, and notify the next waiter if nonempty.
This adds one metadata read on successful native blocking consumption and keeps
the single-waiter FIFO notification policy.

An initial multi-wakeup proposal was discarded when the broader list suite
exposed the explicit single-waiter contract. The retained regression verifies
end-to-end FIFO consumption rather than requiring a particular notification
count from one push.

### Embedded list pushes omit notifications (availability)

The default embedded list API writes directly through the router; its pushes
did not notify waiters. The instance implementation uses a store adapter that
also lacked the push callback. Either path could successfully insert a value
without waking a blocked client.

Default pushes now invoke the existing notification hook after successful
mutation, and the instance adapter supplies that hook. Four red-to-green tests
cover both directions (`lpush`, `rpush`) and both embedded entry points.

### Conditional list pushes omit notifications (availability)

`LPUSHX` and `RPUSHX` returned successful writes without starting the waiter
wake-up chain. Both string and AST dispatch now share the conditional push
helper, which notifies after a positive result. Separate regressions for both
directions verify notification after pushing to an existing list.

## Validation

The first-pass partitioned checks were:

| Check | Passed | Skipped/excluded |
| --- | ---: | --- |
| Core CI partition 1 | 3,767 | 92 excluded |
| Core CI partition 2 | 3,900 | 3 skipped, 86 excluded |
| Core CI partition 3 | 3,863 | 69 excluded |
| Separate WARaft backend suite | 362 | 32 excluded |
| Protocol-server suite | 2,173 | 1 skipped, 61 excluded |
| HTTP suite | 142 | 5 excluded |
| Bitcask Rust suite | 532 | 0 |
| WAL Rust suite | 111 | 0 |
| Native protocol Rust suite | 24 | 0 |
| Performance guards | 2 | non-guard tests excluded |

After the second-pass fixes and fixture cleanup below, the complete default
umbrella suite passed in one invocation:

```text
mise exec -- mix test --seed 873483 --max-failures 1 --timeout 180000
core:   11895 passed, 3 skipped, 279 excluded
server:  2173 passed, 1 skipped, 61 excluded
HTTP:     142 passed, 5 excluded
```

The core total includes the WARaft backend suite. These totals should not be
added to the earlier partitioned counts as independent tests.

After the third-pass publication, waiter, and list fixes, the complete default
umbrella suite passed again with the same command:

| Application | Passed | Skipped | Excluded |
| --- | ---: | ---: | ---: |
| Core, including WARaft | 11,903 | 3 | 279 |
| Protocol server | 2,178 | 1 | 61 |
| HTTP | 142 | 0 | 5 |
| **Total** | **14,223** | **4** | **345** |

An earlier attempt at this third-pass run hit the shell's 40-minute wall-clock
limit during WARaft tests without a reported assertion failure. The completed
retry used a longer shell limit; per-test timeouts remained 180 seconds. Core
took 2,582.2 seconds, server 266.8 seconds, and HTTP 5.9 seconds.

Both performance guards were rerun and passed. Core, server, and HTTP
architecture tests passed within the full suite. Formatting,
warnings-as-errors compilation, `git diff --check`, and both CI-equivalent
Credo warning profiles passed:

```text
mix credo suggest --only warning --min-priority high --files-excluded 'apps/*/test/**/*'
mix credo suggest --only warning --min-priority high --ignore-checks UnusedListOperation,ExpensiveEmptyEnumCheck
```

The broader, unrestricted `mix credo --min-priority high` reports existing
refactoring and test-warning findings; it is not a clean check. The Rust counts
above are from the first pass; no Rust source changed in later passes.

The fixes keep storage in the core application and HTTP request bookkeeping
inside the HTTP application. Native list workers use the existing core list
commands and waiter registry for their wake-up handoff.

## Resolved shared-fixture failure

The initial unpartitioned `mix test --timeout 180000` run did **not** pass. With
seed `873483`, the first failure was the test that deliberately fails Erlang
distribution startup and restores the default application. Its restoration
failed with `flow_lmdb_reconcile_unhealthy` / `cold_read_errors: 1`, followed by
cascading missing-default-instance/ETS failures. The run hit the 40-minute
command timeout.

Temporary diagnostics identified the source as the intentionally malformed
`corrupt-flow-record:*` state written by `FlowFacadeCorrectnessTest` into the
default durable instance. It had no teardown delete. Recovery was correctly
failing closed on that record, rather than introducing corruption.

The issue reproduced with just `flow_facade_correctness_test.exs` followed by
`test_support/shard_helpers_test.exs` at seed `873483`. A durable delete in the
corruption test's `after` block fixes the contamination. The reduced run then
passed all 17 tests, and the complete default suite subsequently passed.
All temporary diagnostic instrumentation has been removed.

## Verification limits

The subsequent [steady-state performance comparison](oss-review-performance-0.11.23.md)
measures the released source against the review changes. It finds measurable
async-wrapper CPU overhead, much smaller differences in real cached storage
I/O, and substantial variability in concurrent list throughput. Raw matched
trials and reproduction scripts accompany that report.

A follow-up removed redundant monitor cleanup from the one-shot async proxy,
relying on BEAM's automatic cleanup at process exit. Focused comparisons showed
approximately 4–5% higher helper-only median throughput. Longer write trials
had essentially equal throughput with overlapping latency ranges. All 176
targeted follow-up checks passed, including lifecycle/cancellation regressions
and performance guards. The full-suite counts above predate this small
optimization; see the performance report for exact measurement and test scope.

The subsequent [overall performance review](overall-performance-review-0.11.23.md)
and [load follow-up](overall-performance-load-followup.md) retain further measured
changes, including native metadata admission. The final affected server/HTTP run
passes 2,183 and 144 tests respectively. A subsequent complete default umbrella
run against all combined changes, using the same seed/command above, also passes:
11,903 core, 2,183 server, and 144 HTTP tests (**14,230 passed**, four skipped,
345 excluded). This replaces the earlier 14,223 total for that stage.
The later heartbeat term-fold optimization adds two regressions; after repairing
cross-VM temporary-directory isolation, the final complete retry passes
11,905 core + 2,183 server + 144 HTTP = **14,232 tests**, four skipped and
345 excluded. The [cluster report](cluster-write-latency-investigation.md)
records the failed setup attempt, fix, final verification, and 12/13 cluster checks.
The [hash-stall follow-up](hash-stall-investigation.md) evaluated three additional
cached-read checks and passed 14,235 default tests on its prototype. An actual
concurrent batch-update probe then exposed a consistency regression. The prototype
and its added default tests were reverted; the restored production sources match
the earlier **14,232-test** verified state and pass 110 affected checks afterward.
The prototype's larger passing count is not acceptance evidence.

The subsequent [publication follow-up](promoted-publication-followup.md) protects
the actual promoted-writer boundary, adds failed/killed-publisher fencing, and
bounds reader/publisher wait CPU cost. Its final combined run passes **14,239
tests**: 11,912 core, 2,183 server, 144 HTTP, four skipped/345 excluded. Application
cached-read enablement was still experimental at that checkpoint.
The load follow-up also runs 12 actual three-node
replication/failover checks, with the separate dependency investigation adding
its timer regression against an isolated candidate.

The continued publication/read work adds lifecycle and transaction-failure
barriers, and enables protected binary promoted reads for selected WARaft
contexts. The final default suite passes **14,253 tests** (11,926 core, 2,183
server, 144 HTTP), plus two performance guards and **13** installed-dependency
cluster checks, including an actual replicated promoted-read case. The
[acceptance report](promoted-publication-followup.md#continuation-protected-application-reads)
records final-source matched read/write timings, saturated-write tradeoffs,
11,935,210 actual batch-update read pairs without a counterexample, and public
Flow controls with admission enabled on an isolated healthy-capacity volume.

Linux-only io_uring, broad Jepsen, explicitly tagged crash-kill, external SDK
integration, and large-allocation lanes were not rerun in this local macOS
review. PR CI remains necessary before merging these fixes.

Latest [single-field HSET continuation](saturated-write-followup.md): one atomic
WARaft command replaces two durable round trips for eligible inline values;
concurrent insertion counts and invalid-cold-read failure handling are repaired.
Three-pair saturated write p99 improves 119→90.3 ms with roughly 18% greater
mixed throughput and preserved fast reads. The final complete retry passes
**14,259 tests** (11,932 core, 2,183 server, 144 HTTP), four skipped/347 excluded,
plus two performance guards and **14** installed-dependency cluster checks.
The report preserves the initial benchmark timeout, cluster harness correction,
failed LMDB umbrella attempt, standalone LMDB pass, and successful full retry.

The [durable-stall continuation](durable-stall-followup.md) additionally repairs
compaction locator publication under LFU/cache changes. It reproduces full-flush
outliers outside the database and rejects compaction policies without reliable
performance evidence. Final verification: **14,261 tests** (11,934 core, 2,183
server, 144 HTTP), four skipped/347 excluded, two performance guards, and 14
installed-dependency cluster checks. No weaker durability primitive is retained.
