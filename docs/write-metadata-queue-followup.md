# Durable writes: shared metadata queue isolation

Date: 2026-10-04. Continues the
[startup/storage stall follow-up](startup-storage-stall-followup.md).

The subsequent [compaction-latch investigation](compaction-latch-followup.md)
reproduces the protected copy interval and rejects two yielding policies after
saturated p99 regressions. Its production experiments are fully reverted; the
metadata-queue correction and verification recorded here remain retained.

## Retained result

Durable WAL operations and promoted-log discovery now issue local metadata
queries directly, avoiding Erlang's shared `file_server_2` queue. The same
file-type, symlink, descriptor-identity, corruption and error checks remain in
place. An existing-directory fast path avoids a serialized `filelib:ensure_dir`
query; missing or unsuitable parents retain its ordinary creation/error path.

A controlled 300-ms suspension of the shared file server blocked the old HSET
route for **311.9 ms**. The corrected route completed a durable promoted HSET in
**12.6 ms**, while that server remained suspended. The production regression
requires the write to complete during the suspension, then checks the value.
It resumes the server and drains the write before fixture cleanup even on failure.

The retained tree passes **14,286 application tests**, **15** installed-dependency
three-node checks, and **2** performance guards. Final native TCP and public Flow
controls also complete with zero operation errors.

This removes a demonstrated application-level source of write stalls. It does
not establish a uniform full-durability latency bound: disk synchronization,
compaction latches, and overload queueing still produce outliers. Automatic HSET
coalescing remains opt-in and disabled by default.

## Per-write attribution

The new diagnostic joins client HSET, submission, WAL append and state-machine
apply using the existing command reference and benchmark key/field/version. It
does not wrap or change replicated commands. It reports admission, WAL queue,
WAL wall, apply queue, apply wall, acknowledgement tail, native-call and latch
intervals. Nested intervals are unioned rather than added twice. These are
instrumented diagnostics, separate from the unprofiled acceptance controls.

The initial 120-second saturated trace linked **all 32,897 writes**. Median WAL
wall time was **22.2 ms**, median native apply **13.3 ms**, and some **1.103-second**
writes spent **1.026 seconds** inside WAL append. A separate apply-latch wait
reached **1.000 second**. That first capture capped its global I/O history and
records the dropped events; its per-write stage joins were complete.

A later 200-cycle/s diagnostic linked **all 25,899 writes**, including warmup,
with no dropped trace records. It distinguished combined WAL/apply/latch delays
from client queueing. A deeper diagnostic then identified substantial serialized
metadata work: one WAL append took **570.9 ms** with only **3.5 ms** in its traced
sync; another took **579.1 ms** with a **1.0-ms** sync. Repeated `read_link_info`,
`ensure_dir` and offset-index metadata calls occupied the remaining time, and
samples showed the shared file server handling those requests.

Artifacts in `bench/results/`:

- `write-timeline-saturated-direct-1.json`
- `write-timeline-offered-200-direct-1.json`
- `offered-mixed-smoke-backlog-1.json`
- `write-timeline-wal-overhead-smoke-1.json`
- `file-server-contention-{server,raw}-1.json`

The `apply_backlog` HSET-routing experiment remains benchmark-only. Its short
diagnostic was affected by large non-sync delays; it is not an accepted batching
policy. The metadata prototype's short run also timed out during cleanup after
writing its result; that artifact is preserved and is not a successful gate.

## Production change and correctness boundaries

- Segmented WAL metadata calls use documented `file:read_link_info(Path, [raw])`
  and `file:read_file_info(Fd, [raw])` APIs. Record framing, CRC verification,
  descriptor-versus-path device/inode comparison, fail-closed recovery, offset
  fallback and append rollback retain their existing logic.
- `metadata_ensure_dir/1` returns immediately only after a direct query identifies
  the existing parent as a directory. Other results use `filelib:ensure_dir/1`.
- Promoted active-log discovery uses `File.lstat(path, [:raw])` and still requires
  canonical segment names and regular files.
- Existing append and file/directory synchronization remain. Native Rust sources,
  macOS `F_FULLFSYNC`, quorum and writing-node apply barriers are unchanged.
- Installed WARaft remains **0.1.0**. Dependency publication/integration remains
  paused, and `mix.lock` is unchanged.

These queries concern local storage paths. Direct metadata queries do not gain
the file server's serialization with concurrent `write_file_info` operations;
storage validity is still enforced by the existing type and opened-descriptor
checks, and the change adds no cache of potentially stale metadata.

## Fixed offered-load controls

The new harness schedules arrivals independently of request completion. Each
cycle contains one HSET, one KV read and two hash reads. Sixteen workers share
four promoted 4,096-field hashes, with 4-KiB values. Each worker has a bounded
eight-item client queue; offers beyond that bound are explicitly counted as
**client-side drops**, not database errors. Accepted work drains after the
measurement deadline. Service time and scheduled-arrival-to-reply time are
reported separately, with p99.9, maxima, drain time and completed maintenance.

The final matrix uses three alternating fresh-VM pairs at each of 100, 200 and
300 cycles/s, 120 measured seconds, 10 seconds warmup and 10 seconds quiet
observation. Coalescing is off in both variants. `server` loads the archived
pre-change segment-log and Promotion modules into temporary BEAMs; `raw` uses
the compiled production implementation. Reports validate shared source/runtime
identity and stable per-variant source and loaded BEAM identities.

### Median of three trials

| Offered cycles/s | Write service p99, old → corrected | Arrival-to-write-reply p99, old → corrected | Client drops, old → corrected |
| ---: | ---: | ---: | ---: |
| 100 | 717 → 353 ms | 2,210 → 1,009 ms | 662 → 0 |
| 200 | 507 → 338 ms | 2,666 → 1,799 ms | 10,829 → 7,610 |
| 300 | 530 → 350 ms | 2,496 → 1,883 ms | 20,474 → 19,190 |

The large overload counts are part of the result; percentiles cover accepted
cycles, not dropped offers. At 100 cycles/s, median in-window completions improve
11,267→11,993 of 12,000 offers. At 200 and 300 cycles/s, substantial overload
remains in both variants.

### Remaining regressions and variability

These medians do not imply every metric or pair improved:

- At 200 cycles/s, median service p99.9 increased **753→789 ms** and median
  maximum service time **909→919 ms**.
- At 300 cycles/s, median arrival-to-reply p50 increased **893→965 ms**.
- The third 300-cycle/s pair had approximately flat service p99 (**82.1→80.9 ms**)
  but worse arrival-to-reply p99 (**1.175→1.380 seconds**), maximum service time
  (**856→1,002 ms**) and slightly more drops (**4,112→4,188**).
- Trial ranges are wide. At 200 cycles/s, old arrival-to-reply p99 spans
  **588–3,275 ms**, corrected **788–1,962 ms**. This shared host's variability
  prevents a tight universal no-regression claim.

Maintenance was not disabled. At 200 cycles/s, the corrected route completed
four compactions during each trial; the old route completed zero, zero and four.
At 300 cycles/s, corrected trials completed four, four and eight; old trials
completed zero, zero and eight during measurement, with four additional quiet
compactions in the first old-route trial. Neither route compacted at 100 cycles/s.
Final status shows no active, pending or retry compactions. Throughput differences
therefore cannot be described as equal completed maintenance, but the corrected
route did not obtain its observed gains by deferring compaction to quiet time.

Artifact: `bench/results/metadata-route-matched-summary.json`, with all 18
individual JSON reports and logs retained. Earlier `metadata-route-pilot-*` and
`offered-mixed-pilot-*` cohorts are separate exploratory results, not pooled into
this final matrix. The earlier pilot's worse tails remain recorded.

The decision to retain the change rests on the deterministic queue-isolation
regression plus preserved storage validation, supported by matched-load results.
It is not a claim that the general durable-write latency investigation is solved.

## Final native TCP and Flow controls

Current production, unprofiled, eight clients, 60 seconds per workload after five
seconds warmup, admission enabled:

| Full cycle | Completed cycles | Cycles/s | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native TCP SET/GET | 59,476 | 991.0 | 8.01 ms | 8.53 ms | 9.35 ms |
| Public Flow create/claim/complete/get | 6,735 | 112.2 | 67.98 ms | 95.58 ms | 116.68 ms |

Zero operation errors; HSET coalescing off; original production snapshot
synchronization. Artifact: `metadata-queue-controls-retained-final.json`.
This is a retained-source execution check, not a matched speedup comparison with
earlier scratch-image controls.

## Final verification

Serial per-application fresh VMs, seed 873483:

```text
core:   11959 passed, 3 skipped, 282 excluded (2913.3 seconds)
server:  2183 passed, 1 skipped,  61 excluded (255.1 seconds)
HTTP:     144 passed,             5 excluded (5.5 seconds)
total:  14286 passed, 4 skipped, 348 excluded
```

Commands: `mise exec -- mix test apps/<app>/test --seed 873483 --max-failures 1
--timeout 180000`. Saved output: `tool_1040ec14e001zbcVtPmhp2GxL5`.

An initial full-suite attempt stopped at a source guard that expected the old
one-argument metadata spelling. Updating its assertions to the direct-query
signatures preserved descriptor-identity checks; all 12 source guards and the
subsequent full suite pass. The earlier targeted command/segment/reader-security
run passed 85 tests, including symlink and offset-index fallback cases.

Also passed:

- Formatting and warnings-as-errors compilation.
- Both CI-equivalent Credo warning profiles and `git diff --check`.
- Two performance guards.
- Fifteen installed-dependency three-node checks, including writing-node reads.

Broad Jepsen/partition, Linux io_uring, explicit crash-kill, SDK and large-allocation
lanes were not all rerun in this local macOS continuation. Existing recovery,
snapshot, compaction, corruption and failure-injection coverage is included in
the complete core suite.

Branch: `codex/oss-full-tdd-review-0.11.23`; worktree:
`/Users/yoavgea/repos/ferricstore-runtime-incident-001`. Changes remain uncommitted.
