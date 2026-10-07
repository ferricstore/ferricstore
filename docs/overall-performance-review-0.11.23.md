# Overall steady-state performance review

The retained corrections are being prepared for **0.11.24**. Current PR, CI and
approval status is tracked in [release preparation](release-0.11.24.md);
the reports below preserve each investigation's historical verification scope.

Latest [temporary rewrite-index cleanup follow-up](rewrite-offset-index-cleanup-followup.md),
2026-10-06: failed rewrites remove their staging directories but the committed
baseline retains 8,194 RAM offset rows per attempt. Memory-only removal/rename
retirement and conservative 30-second maintenance now pass 104 targeted tests,
including active-writer, rollback trust and cross-process reader regressions.
Eleven selected snapshot/recovery checks, 43 application/retention/guard checks
(including both performance guards), fifteen cluster checks and static checks
also pass: 173 tests/checks in the completed scoped runs.
Ten full-cap failures keep ETS memory flat; reclamation removes exactly 188,462
seeded abandoned rows and about 30.2 MB of ETS allocation. A broader lifecycle
run is incomplete at its 600-second outer limit; this is not a new full-suite
or long-shutdown acceptance checkpoint. Deployment awaits release preparation.
Earlier checkpoints follow.

Latest [sparse offset fallback scan follow-up](offset-fallback-scan-followup.md),
2026-10-05: valid sidecars are not the observed failure; most fallback scans
confirm sparse absent indexes. Bounded 256-KiB read-ahead retains the full CRC/
latest-frame scan and reduces the component probe from 1.34 s to 224 ms. Three
equal-work pairs succeed on all four snapshots in 6.5–6.8 s versus 13.4–16.0 s,
while retaining native-tail tradeoffs. Eighty targeted tests, fifteen cluster
checks, both guards and static checks pass; full application verification is
tracked in that report. Larger writer handoffs still time out, including combined
cache and quiescence diagnostics. Earlier checkpoints follow.

Latest [apply-projection cache scaling follow-up](projection-cache-scaling-followup.md),
2026-10-05: single-pass selection and bounded group preparation improve the
component probe and pass correctness checks, but fail long snapshot acceptance.
The candidate is archived, production cache/source paths are restored exactly,
and 45 retained-source tests pass afterward. A smaller current diagnostic points
to repeated CRC-verified disk-offset scans; the unchanged legacy control also
slows severely in the later cohort. No new production optimization is accepted,
and the preceding history/page corrections remain retained.

Latest [Flow snapshot reconciliation follow-up](flow-snapshot-reconciliation-followup.md),
2026-10-05: large valid state pages exceeded composite prefetch admission, and
history request bursts forced a sync per cast. Bounded state pages and coalesced
handled requests now pass failing-first durability/count regressions. Three
matched larger controls succeed on all four snapshots in 13.0–13.9 seconds;
legacy controls stop in 27.6–29.1 seconds but fail snapshot health. The current
source passes 2,535 affected tests, 22 overlapping focused checks, fifteen cluster
checks and two guards. Longer workloads still fail LMDB writer handoff budgets;
cache/durability-group scaling remains open. Earlier whole-application checkpoints
and measured tradeoffs follow.

Latest [Flow shutdown source-wait follow-up](flow-shutdown-source-wait-followup.md),
2026-10-05: missing versioned sources consumed individual retry windows while
holding the shared LMDB flush permit. Batch attempts now return the permit before
waiting and preserve validation/marker ordering. The short fixed-work diagnostic
improves from exceeding 20 seconds to 4.48 seconds. Three larger alternating
equal-work pairs complete application stop in 28.3–30.2 seconds versus exceeding
60 seconds with sequential waits, while retaining native p99/Flow p50 tradeoffs.
Snapshot-result tracing exposes remaining history-flush and LMDB reconciliation
failures despite successful application-stop returns; broader lifecycle acceptance
remains open. The current source passes **14,291 application tests**, fifteen
three-node checks on the unchanged-source rerun, and two guards. The unexplained
first cluster failure and remaining snapshot blockers are tracked in that report;
earlier checkpoints follow.

Latest [separate-output compaction follow-up](separate-output-compaction-followup.md),
2026-10-05: copying outside the latch is implemented and checked, but the saturated
and offered-load gates regress, so the candidate is archived and removed from
production. A real deleted-value resurrection case during partial old-log cleanup
is reproduced and fixed with numeric, durably ordered prefix removal. General
write-tail improvement remains open. The retained tree passes **14,287 application
tests**, fifteen cluster checks and two guards. Native/Flow measurement phases
complete with zero errors, but shutdown amplification reproduces on both retained
and isolated pre-repair compactor controls. Earlier stage measurements follow.

Latest [compaction-latch investigation](compaction-latch-followup.md), 2026-10-05:
the long protected copy interval is reproduced. Per-page and four-page yielding
policies pass targeted concurrency/recovery checks but worsen saturated HSET p99,
so both are reverted. Production compactor/worker sources are verified restored;
122 targeted tests pass afterward. The prior accepted metadata-queue fix and its
14,286-test production verification remain the retained checkpoint.

Latest [write metadata queue follow-up](write-metadata-queue-followup.md),
2026-10-04: local WAL metadata queries and promoted-log discovery avoid the shared
file-server queue while retaining storage validation. A suspended-file-server
probe completes a durable HSET in 12.6 ms with the correction, versus 311.9 ms on
the old route. Three matched offered-load pairs at each rate support the scoped
fix, while preserving p99.9/max regressions, overload drops and maintenance counts.
The retained source passes **14,286 application tests**, two guards and **15**
cluster checks; final native TCP/Flow controls complete with zero errors. Broad
full-durability write latency remains open. Earlier checkpoints follow.

Latest [startup/storage stall follow-up](startup-storage-stall-followup.md):
startup storage calls honor the configured startup budget, and snapshots omit
LMDB's process-local coordination lock while retaining durable data and pins.
The final source passes **14,285 application tests**, two guards and **15** cluster
checks. The single-pass snapshot-sync experiment was reverted after its benefit
failed to reproduce. Native/Flow controls complete, but long full-flush write
outliers remain open. Earlier source checkpoints are recorded below.

Latest [foreground HSET group-commit follow-up](foreground-hset-coalescing-followup.md),
2026-10-03: bounded explicit-batch append is retained; automatic synchronous
coalescing is experimental and defaults off. It improves saturated throughput
but fails the paced/max-latency no-regression gate. Final fresh-VM application
suites pass **14,278 tests**, two performance guards and **15** cluster checks.
Stable native/Flow performance controls and broad durable-stall resolution remain
open. Earlier stage counts and acceptance decisions are preserved below.

Latest [durable-flush investigation](durable-stall-followup.md): full-durability
flush outliers reproduce outside the database; descriptor reuse and tested
compaction policy changes do not provide a reliable latency fix. A real locator
race with LFU/cache updates during compaction is repaired. The final tree passes
**14,261 default tests**, two performance guards and **14** cluster checks. Ten
measured optimizations remain; no new speedup is claimed from the rejected
experiments, and long durable-flush stalls remain open.

Latest continuation: [single-field HSET write path](saturated-write-followup.md),
2026-10-02. One atomic committed operation replaces a type-claim round trip plus
a field-write round trip for eligible WARaft single-field HSETs. Saturated write
p99 returns approximately to the original serialized baseline (119→90.3 ms
against the prior cached-read writer), throughput improves about 18%, and fast
reads remain. Concurrent insertion counts and invalid-cold-read publication are
also fixed. The final combined tree passes **14,259 tests**, two performance
guards, and **14** cluster checks. This adds a tenth scoped optimization; long
durable-I/O stalls remain unresolved.

Initial pass: 2026-09-28; latest promoted-read follow-up: 2026-10-02.
Working tree based on release 0.11.23, including the earlier correctness review
and async-helper cleanup.

## Result

The [sustained-load and multi-node follow-up](overall-performance-load-followup.md)
now adds two-minute paired mixed-load runs, a ten-minute memory/compaction run,
large waiter-population tests, and actual three-node measurements. It retains
one additional blocking-worker cleanup improvement and identifies replicated
write latency as the main unresolved concern. The original pass below describes
its four earlier optimizations and verification scope.

The latest follow-up also fixes **native inbound metadata under-accounting**:
retained fragments and decoded/queued frames now include a constant-time metadata
allowance. Section 5 records its socket regression, matched measurements, and
the complete affected-suite verification. The earlier four improvements plus
waiter cleanup and metadata admission make six retained steady-state changes.

The [latest cluster investigation](cluster-write-latency-investigation.md#ferricstore-defect-heartbeat-term-folds-scanned-past-the-durable-tail)
adds a seventh improvement: bound follower heartbeat term folds at the durable
tail, avoiding repeated full-segment probes for new indexes. The final umbrella
retry passes **14,232 tests**. On installed WARaft, three-node mixed throughput
rises 3.01x and median write p99 falls 356.7→247.8 ms; with the isolated timer
fix, p99 falls 264.0→33.7 ms. The cluster report records the matched cohorts,
read-latency tradeoff, regressions, and test-directory isolation repair.

The [promoted-hash stall investigation](hash-stall-investigation.md) profiles
durable append/apply queueing and evaluates a cached-read prototype. Despite
lower HGET tails, an actual batch-update probe exposed a consistency regression;
the prototype was reverted. This remains **seven retained improvements**. The
report preserves the counterexample, tradeoffs, and blocked Flow control. The
restored production sources match the earlier **14,232-test** verified state;
the prototype's 14,235 passing tests did not establish concurrent correctness.

The [publication follow-up](promoted-publication-followup.md) subsequently repairs
the observed promoted writer boundary and retains bounded publication waiting as
an eighth measured scaling improvement. Actual direct/default batch-read probes
passed while application cached reads were experimental. That checkpoint's combined
tree passes **14,239 tests** (11,912 core, 2,183 server, 144 HTTP), plus both
performance guards and 12 actual cluster checks.

The continued [protected-read acceptance](promoted-publication-followup.md#continuation-protected-application-reads)
adds lifecycle/transaction failure fencing and enables cached binary promoted
reads for selected WARaft contexts. The final-source confirmation lowers HGET
p99 from 31.1 ms to 19 µs, at approximately flat saturated throughput, with a
hash-write p99 tradeoff of 90→112 ms. At paced load, HGET p99 falls 23.2 ms→18 µs
and write p99 is 69→67.5 ms. This is a ninth scoped improvement, not a uniform
latency reduction. The final default suite passes **14,253 tests**, plus two
performance guards and **13** installed-dependency cluster checks. Public Flow
controls now pass on an isolated healthy-capacity volume with admission enabled.

**Four targeted improvements are retained:** embedded dispatch, HTTP auth-cache
maintenance, scoped-lease cleanup, and fragmented native-frame accounting.
They address recurring request-path work and resource scaling, rather than
startup alone. Measurements below are workload-specific, not a claim that the
entire database is faster by any one percentage.

## Review coverage

| Area | Paths examined | Outcome |
| --- | --- | --- |
| KV and collections | Embedded instance dispatch, typed command handlers, hot/cold router reads, batch reads | Remove per-call callback-map construction for concrete instances |
| Durable writes | WARaft direct/batched dispatch, admission, commit boundaries | Preserve durability/batching policy; related API/write-error tests and performance guards pass |
| Flow and queries | Planner/executor limits, ordered/top-k selection, projections, record hydration, admission | Profile all five launch indexes; preserve bounded reads and projected-row path |
| Native transport | Fragment accumulation/accounting, decode budgets, execution/scoped leases, owner cleanup | Fix quadratic fragment counting, metadata under-accounting, and global scoped-table scan on owner death |
| HTTP | Auth hit/miss/coalescing paths, expiration/LRU maintenance, request admission, keep-alive transport | Remove session copies from expiration and eviction scans |
| Background/memory | LMDB permit queue, waiter cleanup, scoped sweep, operational guard, compaction scheduling | Identify remaining scaling costs; preserve safety and admission budgets |

This is a risk-oriented review of the listed paths, not an exhaustive proof of
every workload or source line. Startup experiments are documented separately in
[`startup-replay-metadata-experiment.md`](startup-replay-metadata-experiment.md).

## 1. Embedded collection operations built a large adapter on every call

`FerricStore.Impl.build_store/1` constructed dozens of closures and a map for
operations whose command handlers already accept `%FerricStore.Instance{}`.
That fixed setup cost dominated several otherwise cheap ETS-backed reads.

A concrete instance now passes directly to the existing `Store.Ops` clauses.
Other context shapes retain the callback adapter. No validation, read-failure
propagation, instance isolation, or list notification is removed.

Five paired, alternating trials of actual hot embedded operations, one caller,
four shards, eight online schedulers:

| Operation | Before ops/s | After ops/s | Relative throughput |
| --- | ---: | ---: | ---: |
| String GET hit (control) | 2,502,411 | 2,481,264 | 0.99x |
| GET miss with type lookup | 398,011 | 951,784 | **2.39x** |
| HGET | 334,087 | 655,706 | **1.96x** |
| SCARD | 310,662 | 570,274 | **1.84x** |
| LLEN | 268,560 | 481,623 | **1.79x** |

These results apply to the instance-oriented embedded facade. They are not
native TCP throughput or cold-storage results. The ordinary string-hit path
does not build this adapter and is approximately unchanged in the control.

## 2. HTTP cache maintenance copied session payloads unnecessarily

Expiration used `:ets.tab2list/1`, copying every session into the cache actor
before inspecting timestamps. Capacity eviction also copied session terms
although it only needed the key, expiry, and recency. Large opaque sessions made
miss handling at the default 10,000-entry capacity particularly expensive.

The retained implementation:

- deletes expired entries with an ETS match specification;
- selects only metadata for LRU eviction;
- preserves the existing timestamp/key ordering, TTL boundary, capacity,
  coalescing, caller-death cleanup, and opaque session values.

Three paired trials, 100 distinct successful misses at capacity per trial, with
an immediate synthetic authenticator. Numbers are medians of per-trial median
latencies:

| Cached entries | Session shape | Before cache-miss latency | After |
| ---: | --- | ---: | ---: |
| 1,000 | Small session | 244.5 µs | 94.7 µs |
| 10,000 | Small session | **1.880 ms** | **0.891 ms** |
| 1,000 | Session with 32 nested groups | 4.014 ms | 0.107 ms |
| 10,000 | Session with 32 nested groups | **47.404 ms** | **0.829 ms** |

For 10,000 large sessions, active-entry sweep median latency fell from 34.961 ms
to 0.222 ms. Cache-actor memory observed after ten sweeps was approximately
258.5 MiB before versus 11.6 KiB after. This measures the **actor's post-workload
memory**, not peak RSS or the ETS table holding the sessions. The table's
necessary session storage remains; garbage-collection timing affects individual
post-workload heap observations.

Large-session hit controls showed some variation (about 0.552M versus 0.521M
hits/s at 10,000 entries); the hit implementation itself is unchanged. A separate
HTTP/1.1 keep-alive control used 64 clients, 500 measured requests/client, and
20 warmups/client. Across three fresh VMs per variant, median throughput was
85,215 versus 84,534 requests/s, approximately -0.8%. Runs were short and varied
from 84,604–86,407 before and 79,888–84,596 after, so this is a sanity check rather
than a tight no-regression guarantee. Its mock PONG backend excludes storage and
external authentication costs.

The capacity-miss improvements are cache-layer results. They must not be
presented as a 50x improvement to the entire HTTP service.

## 3. Owner cleanup scanned unrelated scoped leases

The native resource budget stores scoped amounts by `{owner_pid, resource}`.
Cleanup searched by the partial owner key, scanning the global table on every
tracked-owner death, even if that owner had no scoped leases.

Cleanup now uses exact indexed keys for the fixed nine resource kinds, with an
empty-table fast path. Atomic take/release and wake-up behavior are preserved.
This bounds the **scoped-lease portion** of cleanup; it does not make every part
of connection teardown constant-time.

Three paired component trials of the real `DOWN` callback:

| Unrelated live scopes | Before median | After median |
| ---: | ---: | ---: |
| 0 | 1.000 µs | 0.625 µs |
| 16 | 1.416 µs | 1.083 µs |
| 128 | **2.917 µs** | **1.166 µs** |
| 4,096 | **72.542 µs** | **1.000 µs** |

The 4,096-row case is a scaling stress fixture, not the default execution limit.
Production scoped execution admission normally caps concurrency at eight times
the online scheduler count. The benchmark directly seeds valid-shaped scope
rows and invokes the callback to isolate table-search cost; it is not a network
disconnect throughput measurement.

## 4. Native fragment accounting had quadratic work

`Connection.put_inbound_buffer/2` calls `FrameBuffer.stats/1` after each receive.
Although fragments were accumulated efficiently in a list, `stats/1` called
`length/1` on the entire list each time. A frame delivered in N fragments thus
incurred O(N²) fragment-counting work.

The buffer now maintains a count as fragments arrive. Statistics are O(1),
including empty appends and newly initialized buffers. Wire framing, byte
limits, deadlines, and materialization are preserved.

The new scaling regression failed before the fix: 1,000 statistics calls cost
11,507 reductions for a small buffer versus 1,031,507 for a heavily fragmented
buffer. It passes with the maintained counter and uses reductions rather than
a machine-dependent wall-clock threshold.

Five paired component trials assembling a 16 KiB body, querying statistics after
every chunk, and validating the materialized frame:

| Chunk size | Fragments | Before per frame | After per frame |
| ---: | ---: | ---: | ---: |
| 1 byte (stress) | 16,408 | **132.282 ms** | **1.619 ms** |
| 64 bytes | 257 | 50.30 µs | 24.46 µs |
| 4,096 bytes | 5 | 1.99 µs | 2.04 µs |
| Whole frame | 1 | 0.57 µs | 0.56 µs |

The stress improvement is not an 82x improvement to ordinary TCP traffic. It
removes a bad scaling case; normal-sized receive chunks are approximately flat.

## 5. Native inbound admission omitted retained metadata

The payload-only charge omitted the heap cost of each retained fragment and
decoded/queued frame. Many tiny receives could therefore retain substantially
more memory than their inbound admission charge indicated, even after fixing
the quadratic fragment-counting work.

`FrameBuffer.retained_bytes/1` now adds **128 estimated metadata bytes per
nonempty retained fragment**, using the cached count. Decoded and queued requests
use the same allowance per frame, in addition to their existing wire-byte charge.
This is an admission estimate, not an exact heap or RSS measurement. Matching the
single-fragment and decoded-frame charges also avoids an extra lease resize at
the receive/decode boundary for ordinary complete-frame receives. Wire-size
validation still uses the existing wire-size helper.

The failing-first socket regression uses a 256-byte inbound budget, sends a
24-byte incomplete header, then sends individual bytes with accounting barriers
between sends to prevent TCP coalescing. Payload-only admission leaves the
connection open; metadata admission closes it and releases its capacity. A fresh
connection then successfully completes an ordinary PING using that capacity.

### Matched component measurements

Five alternating paired trials assemble and verify a 16 KiB body. The baseline
already has the O(1) fragment counter; this comparison isolates the additional
metadata charge, rather than repeating the quadratic-work comparison above.

| Receive chunk size | Fragments | Payload-only time/frame | With metadata | Final estimated charge |
| ---: | ---: | ---: | ---: | ---: |
| 1 byte (stress) | 16,408 | 1.466 ms | 1.521 ms | 2,116,632 bytes |
| 64 bytes | 257 | 21.52 µs | 22.68 µs | 49,304 bytes |
| 4,096 bytes | 5 | 1.311 µs | 1.322 µs | 17,048 bytes |
| Whole frame | 1 | 0.538 µs | 0.538 µs | 16,536 bytes |

Payload-only admission charged 16,408 bytes in every row. The new estimate scales
with the retained fragment population. The extra arithmetic adds approximately
3.8–5.4% component time in the tiny-chunk fixtures; normal-sized chunks are nearly
flat. BEAM reduction counts are essentially unchanged; the charge remains O(1)
per receive.

### Ordinary socket-traffic control

Three alternating paired fresh-VM trials use actual native PING sockets, one
second of warmup, five measured seconds, and a latency sample every tenth request.
Each cell is a median of trial metrics with its trial range, not a pooled p99.

| Clients | Payload-only ops/s | With metadata ops/s | Payload-only p99 | With metadata p99 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 15,472 (14,996–17,809) | 16,933 (14,650–18,111) | 130 µs (123–156) | 149 µs (115–160) |
| 16 | 63,691 (62,794–65,021) | 63,596 (62,602–63,613) | 419 µs (403–425) | 423 µs (411–443) |

At 16 clients, median throughput changes -0.15% and p99 +0.95%. Single-client
median throughput is higher but median p99 is also higher (+14.6%); both ranges
overlap. These short shared-host controls support retaining the bounded-cost
accounting fix, but do not establish a strict tail-latency no-regression bound.

An intermediate 64-byte fragment-only estimate incurred an unnecessary
receive/decode resize. Its `fragment-socket-*.json` and
`fragment-memory-perf.json` artifacts are preserved as exploratory results.
The final 128-byte aligned version is recorded separately in
`fragment-socket-aligned-*.json`, `fragment-memory-aligned-perf.json`, and
`fragment-memory-summary.json`. The summary validates trial identities, socket
source hashes, component source, and expected charges before aggregation.

### Final affected-suite verification

```text
mise exec -- mix test apps/ferricstore_server/test apps/ferricstore_http/test --timeout 180000 --max-failures 1
seed: 89672
server: 2183 passed, 1 skipped, 61 excluded
HTTP:    144 passed, 5 excluded
```

The 42-test frame/decode-budget suite also passed separately, overlapping the
complete server suite. Formatting, warnings-as-errors compilation, both
CI-equivalent Credo warning profiles, and `git diff --check` passed. These checks
cover the final fragment/decoded/queued accounting, earlier waiter cleanup, and
HTTP changes.

The complete default umbrella suite subsequently passed against the combined
correctness, async, steady-state, waiter-cleanup, and final memory-accounting changes:

```text
mise exec -- mix test --seed 873483 --max-failures 1 --timeout 180000
core:   11903 passed, 3 skipped, 279 excluded (2592.2 seconds)
server:  2183 passed, 1 skipped,  61 excluded (264.6 seconds)
HTTP:     144 passed,             5 excluded (6.0 seconds)
total:  14230 passed, 4 skipped, 345 excluded
```

These are replacement totals, not additional independent coverage. This run
uses the installed WARaft 0.1.0 dependency; the isolated dependency candidate
has separate cluster validation. Excluded Linux, SDK, Jepsen, and tagged stress
lanes are outside this default run.

These 14,230 totals describe the memory-accounting stage. After the heartbeat
term-fold fix and two regressions, the final complete retry passes **14,232 tests**
(11,905 core, 2,183 server, 144 HTTP), four skipped/345 excluded. The cluster
report records the intervening fixture failure, isolation repair, final timings,
and separate 12/13 cluster checks.

## Flow/query findings

The existing launch-index benchmark exercised five indexes over 1,000 records,
with five warmups and 30 measured queries per index. Projected-row results were
checked against authoritative hydration results. Prepared indexed-query p50 was
331–398 µs; p95 was 481–536 µs. Projected-row p50 was 1.52–1.67x faster than
the authoritative hydration fallback in this fixture. These are descriptive
current-path measurements, not a new query optimization or production SLA;
30 samples are insufficient for a strong p99 estimate.

The profile also exposes a real tradeoff: index backfill requires **3–7 logical
operations per record per tested index**, with approximately 4.5–8.7x logical
index-write bytes relative to the fixture's source bytes. Multiple indexes add
write and storage work. Removing those operations would change index/counter
semantics, so the review does not treat that cost as redundant work.

Ordered-query selection repeatedly checks list length, but the public result
limit is 100 records. That bounded cost was not prioritized over the larger
measured costs above. Query pagination, hydration-byte limits, deadlines,
scope validation, and memory accounting were preserved.

## Remaining performance questions

These are follow-ups, not measured fixes in this pass:

- General list-waiter owner-death cleanup still scans the waiter table by PID.
  The follow-up fixes avoidable scans during normal native worker completion
  using known keys and measures a 4,096-owner death storm. A reverse index and
  its registration/race-handling overhead still need separate justification.
- Auth LRU eviction is still O(cache size), now over small metadata rather than
  session payloads. Extremely high credential churn can still saturate its actor.
- Compaction/retention and cold I/O can dominate disk latency under pressure.
  Their existing serialization, bounded batches, and retries were reviewed;
  this pass does not claim saturation or Linux io_uring tail-latency coverage.

## Original-pass verification

| Check | Result |
| --- | ---: |
| Embedded/instance/API/error-path suites | 511 passed |
| Flow query, hydration, LMDB coordinator, core architecture and performance guards | 916 passed, 2 excluded |
| Complete protocol-server suite, before the final frame-counter change | 2,179 passed, 1 skipped, 61 excluded |
| Frame/decode-budget suite after the counter change | 41 passed |
| Complete HTTP suite | 144 passed, 5 excluded |

The frame suite overlaps the server suite and must not be added as entirely new
coverage. The new native owner test exercises aggregated leases across resource
kinds while preserving another owner's capacity. New auth tests cover negative
monotonic timestamps, exact TTL boundaries, tie-breaking, and opaque sessions.

Formatting, warnings-as-errors compilation, CI-equivalent Credo warning checks,
and `git diff --check` passed. The full umbrella suite and excluded external SDK,
distributed, Linux-specific, and stress lanes were not rerun for this pass.

## Reproducibility

The original paired comparisons use the pre-performance-pass review sources
captured in the result artifacts, not an unmodified release image. The metadata
follow-up baseline already includes the earlier performance fixes and removes
only the new metadata charge. Candidate modules use the same dependencies,
NIFs, fixtures, and VM settings. Host: Apple M4 Max, Elixir
1.20.4 / OTP 29, normally `ERL_FLAGS='+S 8:8'` for measurements. These local
measurements do not establish multi-node production throughput.

Scripts:

- `bench/embedded_dispatch_perf.exs`
- `bench/auth_cache_maintenance_perf.exs`
- `bench/native_cleanup_perf.exs`
- `bench/frame_accounting_perf.exs`
- `bench/fragment_memory_perf.exs`
- `bench/fragment_socket_perf.exs`
- `bench/fragment_memory_summary.py`
- `bench/http/overall_keepalive_compare.exs`
- Existing `bench/flow_query_index_bench.exs`
- `bench/overall_perf_summary.py`

Curated trials and the consolidated summary are under `bench/results/`, including
`overall-performance-summary.json`. The scripts retain baseline/candidate source
text where needed for a matched rerun. All changes remain uncommitted.

To reproduce the final metadata-accounting comparison:

```sh
BENCH_OUTPUT=bench/results/fragment-memory-aligned-perf.json ERL_FLAGS='+S 8:8' \
  mise exec -- mix run --no-start bench/fragment_memory_perf.exs
for spec in baseline:1 current:1 current:2 baseline:2 baseline:3 current:3; do
  BENCH_VARIANT="${spec%%:*}" BENCH_TRIAL="${spec##*:}" \
    BENCH_REPORT_PREFIX=fragment-socket-aligned ERL_FLAGS='+S 8:8' \
    mise exec -- mix run --no-start bench/fragment_socket_perf.exs || exit
done
python3 bench/fragment_memory_summary.py
```
