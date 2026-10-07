# Durable-write stall and compaction follow-up

Date: 2026-10-02. Continues the [single-field HSET work](saturated-write-followup.md).

The later [write metadata queue investigation](write-metadata-queue-followup.md)
also identifies and removes a software source of stalls: local metadata queries
waiting through Erlang's shared file server. It retains full synchronization and
storage validation; general write-tail variability remains open.

Subsequent [foreground HSET group-commit work](foreground-hset-coalescing-followup.md)
adds bounded explicit-batch append and an experimental opt-in synchronous policy.
The saturated gain does not pass the paced/max-latency no-regression gate, so the
automatic policy defaults off. That final source passes 14,278 application tests
in fresh VMs, two performance guards and 15 cluster checks; broad stall resolution
and stable native/Flow performance controls remain open. Counts below describe
this earlier locator-only checkpoint.

## Result

The retained change is a **compaction locator correctness fix**. Read-side LFU
updates or cache eviction could make the compactor skip a row's locator update,
then remove the old file still referenced by that row. Locator publication now
atomically matches the immutable source version (key, expiry, file, offset and
size), while preserving current cache residency and LFU. Changed/deleted disk
versions do not match. Cold entries stay cold; compaction does not undo eviction
or require a new binary-accounting charge.

A failing-first regression reproduced the unchanged old file ID after compaction.
The retained tests verify both an LFU change and eviction during collection, the
relocated offset, and successful validated reads after source cleanup.

**The remaining long write stalls are not fixed.** This pass isolates substantial
cost in the OS/full-durability flush primitive and demonstrates how compaction
and serial apply queues amplify it. Descriptor reuse, serialized background
compaction and grouped compaction synchronization did not establish a reliable
stall improvement, so no new default I/O/batching policy is retained.

The latest combined tree passes **14,261 default tests**, both performance guards,
and **14** installed-dependency cluster tests. The ten earlier workload-specific
steady-state optimizations remain; this pass does not claim an eleventh speedup.
All FerricStore changes remain uncommitted and dependency integration stays paused.

## Profiling the atomic writer

Two fresh 120-second diagnostic runs used the current atomic HSET path, protected
reads, 16 clients, four promoted 4,096-field hashes and 4-KiB values. One deferred
promoted compaction; the other used automatic compaction. The bounded profiler
now observes `v2_append_record/4`, file synchronization, actor stacks/mailboxes,
and normal/dirty scheduler queues. These instrumented results are not pooled
with unprofiled performance acceptance trials.

- Deferred compaction still produced roughly 770-ms slow writes. During those
  outliers, sampled normal and dirty run queues were usually zero. Record append
  calls overlapped 150–300-ms WAL synchronization/append calls across shards.
- Automatic compaction produced a **1.440-second** write. Its samples show both
  native record append and waits for the promoted compaction latch. Other writes
  behind it spent most time in commit wait despite short individual apply spans.
- In the automatic diagnostic cohort, observed maxima were approximately
  **413 ms** for `file:datasync/1`, **484 ms** for native record append and **675 ms**
  for the segment provider append. Compaction batch append reached **580 ms**.

This rules out persistent dirty-scheduler queue saturation as the main explanation
for these sampled outliers. It does not prove that scheduling never contributes,
nor identify a particular firmware, kernel, or unrelated-service cause.

## Matching the actual durability primitive

The first isolated Python probe used POSIX `fsync` and returned very small times.
That is **not the same durability primitive** as Rust `File::sync_data()` on this
macOS host. The installed Rust 1.98 standard-library source maps Apple data sync
to `fcntl(F_FULLFSYNC)`. The probe was corrected to use that primitive; the weaker
POSIX control is kept separately and is not proposed as an application change.

Four Python threads wrote two private append-only files each, with 4-KiB records
and a nominal 20-ms cycle interval. The probe checks appended lengths, closes all
owned descriptors and removes only its fresh scratch directory. It runs without
FerricStore/BEAM and does not modify existing data or services.

| 60-second full-sync probe | Reopen descriptors | Persistent descriptors |
| --- | ---: | ---: |
| Open/metadata p50 | 29 µs | 11 µs |
| Open/metadata p99 | 99 µs | 54 µs |
| Full synchronization p50 | 14 ms | 14 ms |
| Full synchronization p99 | 21 ms | 21 ms |
| Maximum full synchronization | **325.3 ms** | **436.3 ms** |
| Maximum two-file cycle | 713.1 ms | 733.7 ms |

The expensive flush and hundreds-of-milliseconds outliers therefore reproduce
outside the database, including with persistent descriptors. File-open reuse
alone cannot remove that cost. The exact underlying OS/filesystem/storage cause
is still open; changing to the weaker POSIX control would not preserve the tested
durability contract.

Installed-source evidence is in
`~/.rustup/toolchains/1.98.0-aarch64-apple-darwin/share/doc/rust/html/src/std/sys/fs/unix.rs.html`,
whose Apple `os_datasync` implementation invokes `F_FULLFSYNC`.

## Rejected experiments

### Serial background compaction admission

A fresh-VM-only macro prototype reused the existing merge semaphore before
acquiring a collection latch. Two alternating pairs measured 120 seconds each.
Write p99 appeared lower (103 ms versus 112–179 ms), but the serialized variant
completed only 2–3 compactions during measurement, while parallel runs completed
four. One additional compaction completed during quiet time, and a retry remained
after ten seconds quiet. Serialized maxima were **1.760–1.871 seconds**, versus
0.962–1.625 seconds in the parallel pair. This is not a maintenance-matched,
reliably improved result; the prototype remains experimental. No production
semaphore ownership/admission change was applied.

### Bounded grouped compaction synchronization

A prototype staged source-version/new-locator plans without retaining page
values, and synchronized before publishing each bounded group. It retained the
metadata-first durability boundary and old files after later-group failure.
Prototype tests verified no early locator publication and durable earlier groups
after a later sync failure. Those prototype-only tests were removed on reversion;
the retained LFU/eviction regressions remain.

Three alternating unprofiled 120-second pairs all completed four compactions,
with zero operation errors, zero compaction failures, and no outstanding
maintenance after quiet time. Source and compiled-module identities were checked
within each variant. A page-synchronized control used the same corrected locator
publication logic, isolating the synchronization policy.

| Median of three trial metrics | Page sync | Grouped sync |
| --- | ---: | ---: |
| Mixed throughput | 921.1 ops/s | 925.2 ops/s |
| Write p99 | 101 ms | 101 ms |
| Maximum write across trials | 1.100 s | **1.269 s** |

One grouped trial worsened write p99 to 177 ms; there is no consistent end-to-end
improvement. A separate 16-MiB manual-copy component comparison across three
fresh-VM pairs produced **110.8 ms** page-sync versus **121.1 ms** grouped-sync
medians. Although the grouped prototype reduced full file-sync operations from
18 to six for a 4,096-field copy, it did not improve overall copy time on this
host. Grouped synchronization was reverted rather than retained on syscall-count
evidence alone. Its exact source is archived in the result JSON and can be loaded
only through the explicit benchmark override.

The temporary page-control compilation emitted an unreachable-clause warning
because its sync-failure branch was removed from the benchmark override. This
warning belongs to that synthetic control; normal production compilation passes
warnings-as-errors. The earlier grouped implementation initially tripped an
existing metadata-retention assertion; it was adjusted to preserve the metadata
publication boundary before its measurements, then ultimately rejected for
performance. No failing attempt is presented as passing.

## Verification

After reverting grouping and retaining only the locator fix:

```text
mise exec -- mix test --seed 873483 --max-failures 1 --timeout 180000
core:   11934 passed, 3 skipped, 281 excluded (2896.9 seconds)
server:  2183 passed, 1 skipped,  61 excluded (279.5 seconds)
HTTP:     144 passed,             5 excluded (6.0 seconds)
total:  14261 passed, 4 skipped, 347 excluded
```

Both performance guards and all 14 installed-dependency three-node checks pass.
Formatting, warnings-as-errors compile, both CI-equivalent Credo warning profiles
and whitespace checks pass. No Rust source, native durability primitive or
dependency pin changed. The full output is archived as
`tool_0fd9920fc0014j04Gx0AwB3mxA` in the OpenCode tool-output directory.

## Reproduction and artifacts

- `bench/results/durable-stall-atomic-{deferred,auto}-profile-1.json` and
  `bench/hash_stall_summary.py`: bounded diagnostic captures and scheduler queues.
- `bench/fsync_stage_probe.py`: use `BENCH_SYNC=full` on macOS, optionally
  `BENCH_FD_MODE=reopen|persistent`; results are component diagnostics.
- `bench/results/fsync-stage-full-{reopen,persistent}-1.json`: full-sync probes;
  the earlier `fsync-stage-{reopen,persistent}-1.json` are weaker POSIX controls.
- `bench/results/compaction-admission-{parallel,serialized}-{1,2}.json` and
  `bench/support/compaction_variant.exs`: rejected admission prototype.
- `bench/results/compaction-sync-{page,grouped}-{1,2,3}.json` and
  `compaction-copy-{page,grouped}-{1,2,3}.json`: full trial/copy measurements.
- `bench/support/compaction_sync_variant.exs`: defaults to production page sync;
  `BENCH_COMPACTION_SYNC=grouped` loads archived rejected source in a fresh VM.
- `bench/durable_stall_results.py` and `bench/results/durable-stall-summary.json`:
  validated, separately aggregated experiment cohorts.

The next latency investigation should focus on controlled storage/full-flush
behavior and genuinely maintenance-matched group commit, using the same durable
publication and replay guarantees. Current hardware-level flush delays remain
visible rather than hidden by weaker acknowledgments.
