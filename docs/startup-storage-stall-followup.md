# Startup storage deadlines and LMDB snapshot stalls

Date: 2026-10-03. Continues the [foreground HSET investigation](foreground-hset-coalescing-followup.md).

Subsequent [write metadata queue isolation](write-metadata-queue-followup.md)
removes shared-file-server waiting from local WAL metadata queries and promoted
log discovery. That retained checkpoint passes 14,286 application tests, two
guards and 15 cluster checks. Counts and measurements below describe this earlier
startup/snapshot checkpoint.

## Retained result

Two availability defects are fixed:

1. WARaft bootstrap and nested storage calls can now use **at least the configured
   FerricStore startup wait budget**, instead of failing at the dependency's
   smaller ordinary storage-call timeout. The previous ordinary setting is
   restored after success, failure, or caller death.
2. Data snapshots omit the canonical **`flow_lmdb/lock.mdb` runtime coordination
   file**. Copying this file through a shared-file-lock primitive could wait
   indefinitely during restart. Durable `data.mdb` contents, locators and pins
   remain in the snapshot; ordinary lock-named payloads are retained.

The final tree passes **14,285 application tests**, two performance guards and
**15** installed-dependency cluster checks. The snapshot synchronization-policy
experiment was reverted because its final matched measurements did not establish
a reliable benefit. Automatic synchronous HSET coalescing remains experimental,
opt-in, and disabled by default.

These fixes address premature startup failure and an actual snapshot-copy hang.
Long hardware/full-flush write outliers remain observable; this report does not
claim uniform write latency or a universal performance no-regression bound.

## Capturing the 60-second bootstrap failure

Four initial fresh-data traces on the owned APFS scratch image completed in
5.27–5.58 seconds. Each recorded 85 atomic metadata replacements and 282 explicit
directory synchronizations. Snapshot installation dominated bootstrap time.

A later trace captured the actual failure:

- The bootstrap caller timed out at **60 seconds**.
- The longest storage snapshot-install callback completed in **77.43 seconds**.
- The longest observed atomic metadata replacement was **2.88 seconds**; an
  explicit directory synchronization reached **1.12 seconds**.
- Shard and LMDB actors were generally idle while native calls were pending.
- Startup plus failure cleanup consumed **219.8 seconds**. That total is not the
  duration of one disk flush or one bootstrap call.

Artifact: `bench/results/snapshot-copy-controls-long-traced-baseline-1-bootstrap.json`.
Earlier timeout logs and failed control matrices remain archived.

Installed WARaft 0.1.0 uses the configurable global option
`raft_storage_call_timeout`, defaulting to 60,000 ms, for both bootstrap and its
nested snapshot calls. FerricStore's `waraft_start_wait_timeout_ms` defaults to
300,000 ms. Its polling budget therefore did not prevent the inner call from
aborting a healthy but slower bootstrap.

### Startup-scoped timeout ownership

`WARaftBackend.start/2` validates its startup budget before stopping an existing
backend. While the startup write fence is held, storage calls use the maximum of
the ordinary storage-call limit and the startup budget. Larger or infinite
ordinary limits are preserved. The prior setting is restored if the temporary
value is still installed. A monitor-backed guard also restores it when an
untrappable caller exit bypasses `try/after`.

This scopes the extension to startup. The regular storage configuration is
restored before successful startup returns; normal acknowledged-write timeouts
retain their existing behavior. The startup budget remains a per-call/polling
budget, not a new absolute deadline for the complete application startup.

A real stressed startup after the correction completed successfully in
**109.7 seconds**, including a **65.4-second bootstrap call** that exceeds the old
cap. The trace records the temporary 300,000-ms setting and confirms that the
previous absent override was restored afterward:
`bench/results/bootstrap-stall-startup-budget-fixed-1.json`.

Behavioral regressions also cover a 250-ms bootstrap pause with a 50-ms ordinary
storage-call limit and a 1,000-ms startup budget, failed-start restoration,
invalid-budget validation preserving an already-running backend, and killed
startup-caller cleanup. Caller death does not cancel native/snapshot work; the
fixture resumes and drains its owned callback before deleting context tables.

## The separate LMDB lock-file hang

Full verification then reproduced a restart hang in the application lifecycle
suite. A fresh-VM diagnostic reproduced it independently. Path-aware tracing
identified an indefinitely pending `fs_copy_sync_nofollow/2` call reading:

```text
<owned fixture>/data/shard_0/flow_lmdb/lock.mdb
```

Other shard/storage actors were idle, and the sampled run queues were empty.
The native copy helper opens its source under a shared advisory file lock. An
LMDB coordination file can be locked by environment initialization/lifecycle
work; copying it also transfers process-local reader/mutex state into a snapshot.

Artifacts: `application-restart-guard-probe-1.json` and
`application-restart-guard-probe-7.json`. The first trace identified a pending
copy; the path-aware trace pinned the exact coordination file. Both diagnostics
preserve their timeout outcomes and private fixtures.

### Scoped omission with durable data retained

`copy_payload_dir/3` computes the canonical lock-file path only for a `:data`
payload root and carries it through recursive copying. Only that regular file is
omitted. The existing nofollow/type checks still reject unsafe payload paths.
Storage payload roots and nested ordinary paths such as
`nested/flow_lmdb/lock.mdb` retain normal copying.

LMDB recreates coordination state when opening the copied `data.mdb` environment.
A regression inspects the snapshot immediately after its data directory is
copied: the runtime lock is absent, the database file is present, an ordinary
lock-named payload is intact, and a sentinel value is readable from the copied
database. Existing snapshot-retention, copied locator-view, rollback, cold-value,
and recovery tests pass in the final core suite.

The full 25-test application lifecycle suite passes after the correction. Six
fresh restart probes completed in **5.48–6.66 seconds** without the prior hang:
`bench/results/application-restart-lockfix-{1..6}.json`.

## Rejected snapshot-sync optimization

A prototype synchronized each copied child once, relying on the existing
durable native copy before synchronizing its parent. It removed repeated subtree
walks at every ancestor. Durability and failure/retry checks passed, and an early
three-pair 1-MiB/16-file nested-copy component gate showed a 355.1→288.1-ms median.

The later matched gate includes the LMDB-lock correction in both variants:

| Median of three fresh-VM trial times | Existing tree sync | Single-pass prototype |
| --- | ---: | ---: |
| Nested snapshot copy | 379.8 ms | 382.0 ms |
| Maximum trial time | 397.1 ms | 451.5 ms |

That gate does not reproduce the early benefit. The prototype was reverted;
production keeps its original synchronization walk. Its exact source remains
archived in `snapshot-copy-lockfix-retry-single_pass-1.json` and is loaded only by
the explicit benchmark variant. The regular benchmark default is production
`baseline`.

No weaker synchronization primitive, WAL retention policy, or dependency version
was introduced. Native Rust sources remain unchanged. Both installed Rust Apple
`sync_all` and `sync_data` implementations use `F_FULLFSYNC`.

## Native/Flow controls and remaining latency

The earlier six-run, five-second control matrix completed, but a later longer
matrix reproduced bootstrap failure. With the startup-budget correction, all
six 60-second native TCP SET/GET and public Flow lifecycle runs completed, with
admission enabled and zero operation errors. Their throughput and percentile
ranges remained very large; the single-pass prototype's later confirmation also
worsened several metrics. These are preserved observations, not a general
speedup or a tight no-regression envelope.

The final retained source completed an additional unprofiled 60-second control
per workload after five seconds warmup:

- Native SET/GET: 4,776 cycles, 79.1 cycles/s, p99 **1.221 seconds**.
- Public Flow create/claim/complete/get: 1,287 cycles, 21.4 cycles/s, p99
  **2.894 seconds** for the complete multi-command cycle.
- Zero operation errors; ordinary HSET coalescing disabled; production snapshot
  synchronization; admission enabled.

Artifact: `startup-storage-controls-retained-final.json`. It verifies successful
execution on the retained source, not improved steady-state tail latency.

A matching Python `F_FULLFSYNC` probe on the image, outside FerricStore/BEAM,
observed a 34-ms sync median and **642-ms maximum** with persistent descriptors
(`fsync-stage-image-stress-1.json`). Startup metadata replacements exhibited
larger combined costs. The filesystem/storage source of that variability remains
open. The owned scratch image was detached after the controls.

## Final verification

Final serial fresh-VM suites, seed 873483:

```text
core:   11958 passed, 3 skipped, 282 excluded (3271.3 seconds)
server:  2183 passed, 1 skipped,  61 excluded (259.1 seconds)
HTTP:     144 passed,             5 excluded (5.5 seconds)
total:  14285 passed, 4 skipped, 348 excluded
```

Commands are `mise exec -- mix test apps/<app>/test --seed 873483 --max-failures 1
--timeout 180000` for each of the three application directories. Full output:
`tool_1018d8c9c001qkJV87o4usrN52`. Final formatting, warnings-as-errors compilation,
both CI-equivalent Credo warning profiles, whitespace checks, two performance
guards and all 15 installed-dependency cluster checks pass.

Failed lifecycle runs (`tool_1013879c4001KAOBv1iKYprUhB`), the original streaming
source guard's stale two-argument match, benchmark setup failures, and the early
fixture teardown error are preserved. The guard now recognizes the scoped
three-argument copy helper while retaining the nofollow-streaming assertions.
The final complete suite passes after the lock-file correction and sync-policy
reversion; failures are not hidden by increased test deadlines.

All changes remain uncommitted. WARaft stays at installed 0.1.0; dependency
publication/integration remains paused. The remaining work is write-latency
mitigation under full-durability storage stalls, not the corrected startup cap
or LMDB coordination-file copying.
