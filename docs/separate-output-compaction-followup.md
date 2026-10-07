# Separate-output compaction and durable cleanup ordering

Date: 2026-10-05. Continues the
[compaction-latch investigation](compaction-latch-followup.md).

The subsequent [Flow shutdown investigation](flow-shutdown-source-wait-followup.md)
attributes the recorded teardown stall to versioned source waits holding the
shared LMDB permit and implements bounded batch attempts with waits outside that
permit. The measurements and verification below describe this earlier cleanup
correctness checkpoint.

## Retained result

This pass retains a **deleted-value resurrection fix** in the existing promoted
compactor. Old logs are removed in numeric file-ID order, and each removed prefix
is directory-synced before the next file can be removed. A failed cleanup can no
longer remove a newer tombstone while retaining its older value log.

The separate-output latency candidate was implemented and tested, then removed
from production after its matched performance gate failed. Its source bundle,
regression cases and measurement artifacts remain available through explicit
fresh-VM benchmark loaders. Ordinary production keeps whole-compaction locking.
The accepted metadata-queue fix is retained; HSET coalescing still defaults off.

## Actual recovery counterexample

The failing-first regression creates:

- `00000.log`: a promoted hash containing `field_1 = "value_1"`.
- `00001.log`: a later durable tombstone for `field_1`.
- `00002.log`: the compaction output containing the other live fields.

It supplies a legitimate unsorted directory listing with file 1 before file 0,
then injects a removal failure for file 0. The previous implementation removes
file 1, fails to remove file 0, and returns a compaction error. Recovery into a
fresh keydir then restores **`field_1 = "value_1"`** despite its acknowledged
deletion. The test failed with that exact recovered value.

With numeric cleanup ordering, removal of file 0 fails first and file 1 remains.
Fresh-keydir recovery correctly leaves `field_1` absent. Each successfully deleted
prefix is synchronized before a newer file is removed: ascending unlink calls
alone would not establish power-loss persistence ordering. The existing final
directory synchronization is also retained.

Regression: `dedicated_compaction_test.exs`, test
`a failed old-value removal must retain the newer tombstone file for recovery`.
The removal hook is process-local and used only for fault injection; normal
removal still uses the existing filesystem wrapper.

This fix adds synchronization work where cleanup previously had an unsafe
ordering. It is a recovery-correctness repair, not a latency optimization.

## Separate-output design explored

The candidate reserved a foreground tail with a file ID above a private compacted
output. It copied existing live source records into a nonce-named work directory
outside snapshot payload roots, using the existing bounded-buffer native copier.
New writes continued into the higher tail while copying proceeded.

Before installation, the candidate validated the complete CRC-framed relocation
manifest against the copied output, including keys, expiry, sizes and offsets.
Generation, keydir-table, directory and source-file identities were rechecked.
Installation was synchronized before immutable-version locator CAS operations
and ascending, durably ordered source removal. A guard coordinated installation
with snapshot copying. The foreground tail always replayed after copied records.

Copy batches targeted one MiB and 256 records. Large existing records were copied
individually through native streaming buffers. Carrying partial batches across
catalog pages avoided small extra sync batches. The candidate admitted at most
128 source files and retained fallback for unsupported legacy contexts.

An early candidate completed only four compactions during measurement, with four
more finishing in quiet time. That failed the maintenance gate. A bounded
publication-turn reservation prevented new writers from indefinitely overtaking
installation; final-source runs completed all eight compactions during traffic.
Controlled tests cover reservation handoff and cleanup after owner death.

Eleven candidate regressions passed, including concurrent replacement/insertion/
deletion, cold-cache/LFU preservation, interrupted installation, interrupted
source cleanup, collection replacement, valid-record-boundary manifest truncation,
wrong-key locators, a two-MiB streamed value and snapshot exclusion/coordination.
These are controlled process/failure cuts, not a complete physical power-loss
or Jepsen qualification.

The default test fixture also exposed a history-projection watermark that did not
drain in a hash-only snapshot control. The snapshot-coordination case includes
a real Flow creation and verifies its history flush before snapshot copying.
The hash-only flush failure was not fixed by this compaction experiment and is
not claimed resolved by the successful mixed control.

## Measurements and rejection

The injected-delay component probe used 4,096 fields, four-KiB values and a 20-ms
delay at each of sixteen member pages. Durable I/O remained enabled:

- Previous whole-compaction latch: write **433.7 ms**, compaction **438.5 ms**.
- Separate output: write **12.8 ms**, compaction **575.1 ms**; the write completed
  while copying continued.

That verifies the nonblocking mechanism, not general steady-state performance.

The frozen unprofiled saturated pair used sixteen clients, four promoted
4,096-field hashes, four-KiB values, 120 measured seconds, ten seconds warmup and
ten seconds quiet observation:

| Metric | Whole-compaction control | Separate-output candidate |
| --- | ---: | ---: |
| Write p50 | 57.2 ms | 59.2 ms |
| Write p99 | 86.7 ms | 90.6 ms |
| Maximum write | 856.1 ms | 744.5 ms |
| Completed compactions during traffic | 8 | 8 |
| Quiet-period compactions | 0 | 0 |
| Operation/compaction errors | 0 | 0 |

A separate fixed offered-load pair used 200 cycles/s, sixteen workers with bounded
eight-item queues, 1,024-field hashes, 60 measured seconds and five seconds warmup:

| Metric | Whole-compaction control | Separate-output candidate |
| --- | ---: | ---: |
| Offers / completed cycles | 12,000 / 12,000 | 12,000 / 12,000 |
| Client-side drops | 0 | 0 |
| Write service p99 | 85.4 ms | 87.5 ms |
| Arrival-to-write-reply p99 | 574 ms | 617 ms |
| Maximum arrival-to-reply | 657.8 ms | 703.2 ms |
| Completed compactions | 8 | 8 |
| Quiet-period compactions / errors | 0 / 0 | 0 / 0 |

The candidate therefore failed the no-regression gate even after maintenance
completed during traffic. Lower lock-held work did not eliminate device/native
I/O contention. A preceding instrumented stage measured copy/validation spans of
3.0–19.7 seconds under traffic, with final publication waits of microseconds to
23.9 ms. Earlier pilot, batch-packing and publication-turn source cohorts remain
separate; they are not pooled as repeated final-source acceptance trials.

## Archived candidate and artifacts

The separate-output module, promotion-turn checks, experimental plan variant,
startup staging cleanup and snapshot guard are all removed from production.
The retained change is the existing compactor's safe cleanup ordering and its
behavioral regression.

`bench/results/separate-output-candidate-source.json` preserves all candidate
module/macro sources with SHA-256 identities. The archive includes the subsequent
cleanup correctness repair in shared modules; it must not be mislabeled as the
exact earlier measured shared source. The original individual reports retain
their measured source identities and candidate body.

Explicit isolated regression command:

```text
MIX_ENV=test ERL_FLAGS='+S 4:4' mise exec -- mix run --no-start bench/regressions/separated_compaction.exs
```

Normal benchmarks default to `BENCH_COMPACTION_LATCH=whole`. Explicit `separate`
loads the archived candidate into temporary BEAMs and verifies its source hashes.
Installed modules and dependency sources are not overwritten.

Main artifacts in `bench/results/`:

- `separate-output-turn-probe-1.json`
- `separate-output-frozen-saturated-{whole,separate}-1.json`
- `separate-output-frozen-offered-{whole,separate}-1.json`
- `separate-output-{pilot,packed,turn}-saturated-*.json`
- `separate-output-retained-verification.log`
- `separate-output-summary.json`

## Final retained-source verification

The failing-first recovery regression now passes. Eleven candidate checks passed
in both the implementation checkout and the explicit archived-loader VM.
Retained-source serial fresh-VM application verification, seed 873483:

```text
core:   11960 passed, 3 skipped, 282 excluded (3066.3 seconds)
server:  2183 passed, 1 skipped,  61 excluded (258.1 seconds)
HTTP:     144 passed,             5 excluded (5.4 seconds)
total:  14287 passed, 4 skipped, 348 excluded
```

Commands: `mise exec -- mix test apps/<app>/test --seed 873483 --max-failures 1
--timeout 180000`, recorded in `bench/results/separate-output-retained-verification.log`.
Fifteen installed-dependency three-node checks, two performance guards, formatting,
warnings-as-errors compilation, both configured Credo profiles and whitespace
checks also pass. Broad Jepsen/partition, Linux io_uring, SDK, explicit crash-kill
and large-allocation lanes were not all rerun in this continuation.

## Control measurements and remaining shutdown defect

The final retained-source, unprofiled 60-second native TCP and public Flow
measurement phases completed after five seconds warmup, with eight clients and
zero operation errors:

| Full cycle | Cycles | Cycles/s | p99 |
| --- | ---: | ---: | ---: |
| Native SET/GET | 59,096 | 984.7 | 9.67 ms |
| Flow create/claim/complete/get | 6,717 | 111.8 | 105.64 ms |

Artifact: `separate-output-retained-controls-final.json`. **The subsequent
benchmark teardown exceeded the 300-second command deadline**, so this is not a
successful whole-lifecycle control or evidence that shutdown is fixed.

A fresh five-second control with a bounded 30-second shutdown trace reproduced
the teardown failure. It captured 802 atomic replacements and 1,072 explicit file
syncs during shutdown; repeated paths include `flow_history_projected.index`, and
samples show Flow LMDB policy reconciliation as well as history projection work.
This is active flush/reconciliation amplification, not evidence of one 30-second
flush. Artifact: `separate-output-shutdown-trace-1.json`.

The matching isolated pre-repair compactor control also failed the 30-second
shutdown budget, with 848 atomic replacements and 1,127 file syncs. Its loaded
legacy source/BEAM identity is recorded separately in
`separate-output-shutdown-legacy-controls-1.json`; trace:
`separate-output-shutdown-legacy-trace-1.json`. The cleanup correctness repair
therefore does not explain this reproduced teardown defect. It remains open;
failed fixtures and diagnostics are preserved, and no benchmark VM remains.

Branch: `codex/oss-full-tdd-review-0.11.23`. Changes remain uncommitted, and WARaft
dependency publication/integration remains paused.
