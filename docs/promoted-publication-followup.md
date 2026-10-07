# Promoted publication and wait-cost follow-up

The subsequent [single-field HSET write follow-up](saturated-write-followup.md)
removes a redundant durable round trip, repairs concurrent insertion counts,
and rejects invalid cold reads before publication. Its three-pair matrix lowers
the cached-read path's saturated write p99 from 119 to 90.3 ms, with about 18%
more mixed throughput and preserved fast reads. The latest combined tree passes
**14,259 default tests**, two performance guards, and **14** cluster checks.
The read-acceptance measurements and their earlier write tradeoff below retain
their stage-specific provenance.

Work window: 2026-10-01 21:09 UTC through 2026-10-02 02:09 UTC.

**Current status (continued 2026-10-02):** the protected hot promoted-read path is
enabled for selected WARaft contexts. The continuation below records lifecycle
and transaction hardening, final-source measurements, **14,253 passing default
tests**, two performance guards, and 13 installed-dependency cluster checks.
The earlier five-hour checkpoint is preserved separately.

## Retained changes

### Promoted writer publication boundaries

The [earlier cached-read experiment](hash-stall-investigation.md) exposed a real
boundary gap: promoted batch writers updated cached rows individually under a
collection latch without participating in the epoch checked by a cache reader.

`Ferricstore.Store.PromotedPublication` now provides a logical mutation scope:

- The epoch starts at the cache-publication phase after the validated durable
  append result, rather than before that first I/O operation.
- Later promoted/shared publications in the same logical mutation share the
  epoch through completion/rollback. Nested scopes cannot close the outer epoch.
- An already owned epoch is borrowed without closing it. A later publication
  acquires its own token if the original owner has closed that epoch.
- Failed or killed publishers leave a fence. The protected read helper falls
  back instead of treating repaired epoch parity as proof of complete cache
  publication. Subsequent publications cannot erase a pre-existing failure fence.
- Recovery reset clears that fence; the normal serialized read path remains
  the fallback.

The scope is wired into state-machine pending-write execution and the promoted
single/batch put, delete, blob, and prefix-related publication paths. Direct
instance compound writers also use it. Ordinary shared publication retains its
short-lived epoch unless it is part of an already active promoted mutation.

The actual 512-field/100-update counterexample now passes with the protected
experimental reader. The final probes checked **528,761 direct-instance pairs**
and **2,161,074 default WARaft pairs**, with no backwards field versions. Earlier
protected probes also passed 647,902 and 2,240,462 pairs respectively. These are
bounded stress results, not an exhaustive proof of every interleaving.

The end-of-window extended probes performed **1,000 full 512-field updates** on
each path. They checked **5,192,672 direct-instance** and **20,061,341 default
WARaft** successive read pairs without a backwards-version counterexample.
Results are in `promoted-publication-{direct,default}-1000.json`.

At the five-hour checkpoint, application Router cached reads were **not enabled**. Broader cache-reader
acceptance, including all lifecycle/transaction combinations and a complete
matched performance gate, is a separate step. This follow-up retains the writer
boundary protection and explicit failure-fallback mechanism without presenting
experimental HGET timings as shipped behavior.

### Bounded publication waiting

The existing epoch reader repeatedly yielded while a live writer kept an odd
epoch. A failing-first regression measured **9,970,045 reductions in 51 ms**.
A queued publisher similarly used **1,231,541 reductions in 54 ms** while another
writer owned the latch. Yielding handed scheduling to another process but did
not bound the total work when the wait persisted.

Readers and queued publishers now yield briefly and then wait one millisecond
between retries. Epoch/latch checks, orphan repair, reentrancy rejection, and
stable-read validation are retained. This trades a small wake-up delay for a
large reduction in scheduler work under contention.

Three paired component trials, eight schedulers, approximately 100-ms holds:

| Case | Before reductions | After reductions | Before wake-up | After wake-up |
| --- | ---: | ---: | ---: | ---: |
| One blocked reader | 19.58 million median | 1,707 | 21 µs median | 882 µs median |
| Sixteen blocked readers, aggregate | 16.84 million median | 27,312 | 47 µs median | 779 µs median |
| One queued publisher | 2.26 million median | 427 | 2.770 ms median | 881 µs median |

The uncontended component control was 0.0500 versus 0.0509 µs/read in these short
trials. That tiny difference is not an end-to-end request-latency claim. The
blocked-reader CPU improvement is likewise not a database throughput multiplier.
Source snapshots and per-trial results are in `publication-wait-perf.json`.

## Experimental read measurements

Two 120-second pairs compared current serialized Router reads with the protected
experimental cache reader. Read p99 was **32.3–35.7 ms** versus **19–23 µs**.
However, one protected trial performed no compactions and only 432 mixed ops/s,
while the other completed four compactions at 821 ops/s; baseline trials completed
four compactions at 767–816 ops/s. The loop reached its shell timeout during the
overall run despite writing all four reports. These results do not establish a
clean performance acceptance gate and were not used to enable application cache
reads. The complete per-trial tradeoff is preserved, not filtered to the favorable
trial.

At that checkpoint the public Flow lifecycle performance control was blocked by host disk-pressure
admission. Native TCP SET/GET still validates IDs and values; its final control
completed 17,148 cycles with p99 9.533 ms and no operation errors. That is a current
sanity control rather than a matched attributed speedup. An `erl_child_setup`
message appeared after the completed report and is retained in the tool output.

## Verification

The five-hour checkpoint's combined tree passed:

```text
mise exec -- mix test --seed 873483 --max-failures 1 --timeout 180000
core:   11912 passed, 3 skipped, 279 excluded (2784.3 seconds)
server:  2183 passed, 1 skipped,  61 excluded (280.5 seconds)
HTTP:     144 passed,             5 excluded (5.9 seconds)
total:  14239 passed, 4 skipped, 345 excluded
```

Both performance guards pass separately, and the actual installed-dependency
three-node replication/failover suite passes **12 tests**. Formatting,
warnings-as-errors compile, both CI-equivalent Credo warning profiles, and
whitespace checks pass. No Rust or dependency-pin change was made in this window.

Five new promoted-publication tests cover nested phases, borrowing, exception
fences, killed publishers, and actual direct-writer blocking. Two failing-first
wait-cost regressions cover readers and queued publishers. Focused state-machine,
ops, rollback, compaction, and publication suites also passed; their counts
overlap the combined run.

## Rejected/adjusted attempts and verification history

- An initial wrapper extraction tripped the existing source-level compact Stream
  publication guard. The execution remains inline in `with_pending_writes/2`,
  preserving the guard and explicit publication threading.
- A broader experiment hooked generic shard ETS mutations and transaction context
  into the scope. A combined run later encountered FLUSHDB and dashboard timeouts
  amid very slow I/O. Those broader hooks were removed; the current narrowed
  source subsequently passed the full core and final combined suites. The failures
  are not called passing, nor conclusively attributed to one cause.
- At that checkpoint the store/read files retained no cached-read or generic
  ETS/transaction hook diff from those rejected attempts. The protected reader
  was loaded from an archived macro inside explicit fresh benchmark VMs. The
  narrower, subsequently verified application path is described below.

## Artifacts and next work

- `bench/regressions/promoted_cached_read_atomicity.exs`: actual writer/read probe;
  `BENCH_WRITER_SCOPE=direct` or `default`, `BENCH_PROMOTED_READ=protected`.
- `bench/results/promoted-publication-{direct,default}-final.json`: final pair counts.
- `bench/results/promoted-publication-{direct,default}-1000.json`: extended probes.
- `bench/results/hash-publication-sustained-{baseline,protected}-{1,2}.json` and
  summary: full, mixed experimental performance outcome.
- `bench/publication_wait_perf.exs`, `bench/results/publication-wait-perf.json`:
  paired wait-cost and uncontended controls with captured sources.

At the checkpoint, application cached-read enablement still needed additional
actual-writer/lifecycle coverage and a matched gate. Existing seven earlier
steady-state improvements remained; bounded publication waiting added an eighth
measured scaling improvement. The continuation completes that narrower read path.

## Continuation: protected application reads

### Scope and correctness boundaries

`Router.compound_get/3` now uses the protected cache path for selected WARaft
contexts and cached binary promoted rows. Missing, cold, expired, and unsupported
rows retain the serialized fallback. Non-WARaft adapters retain that fallback.
Positive TTLs are sampled again after any publication wait; read bookkeeping
runs only after obtaining a stable result.

Additional publication/lifecycle coverage:

- Returned mutation errors after an earlier publication retain the publisher
  fence, as do inner failures rescued by an outer mutation.
- Cross-shard transaction publication registers failure fences while borrowing
  its existing ordered shard epochs. A failed/killed apply cannot authorize a
  partial hot-cache read merely by repairing the epoch.
- Shard startup, WARaft storage opening, snapshot replacement, and replicated
  shard flush hold a lifecycle barrier. Failure or a blocked/paused handle keeps
  that barrier. Successful replacement clears publisher fences and its barrier.
- A lifecycle generation check catches replacement that starts and finishes
  inside a read. Failed/dead-owner barriers are replaced atomically, without a
  transient clear interval.
- Dedicated compaction preserves the logical value/TTL and publishes only when
  the observed old row still matches. The shortcut consumes the cached value,
  not the rewritten disk locator; cold reads keep their validated serialized path.

Actual apply tests block between separate promoted append/publication phases,
inject a later append failure, and fail a promoted transaction after its group
has published. Other regressions cover lifecycle replacement, repeated failed
replacement, blocked handles, expired-during-wait TTLs, cold fallback, and flush
failure/replay. The three-node test verifies replicated promoted fields and hot
reads while each node's shard callback is suspended.

The **final-source** 1,000-update/512-field default WARaft probe checked
**11,935,210 successive field-read pairs**, with no backwards versions. This is
bounded stress evidence alongside the deterministic tests, not an exhaustive
interleaving proof. Earlier direct/default counts above belong to their earlier
source cohorts.

### Matched measurements and decision

**Retained as a scoped read-tail optimization.** The path removes promoted hot
reads from the shard callback queue. Saturated write latency has a reproducible
tradeoff, so this is not an across-the-board latency or throughput improvement.
Long durable-write stalls remain visible.

Three fresh-VM alternating pairs used 16 clients, four 4,096-field promoted
hashes, 4-KiB values, eight schedulers, installed WARaft 0.1.0, and automatic
compaction. The 120-second closed-loop trials all completed four compactions;
the 60-second paced controls all completed zero. Pacing was one four-operation
cycle per client per 100 ms, approximately the same offered workload. Values
were asserted during execution and at completion; all operation/compaction
error counts were zero.

| Three-pair median | Serialized baseline | Protected |
| --- | ---: | ---: |
| Closed-loop mixed throughput | 888.5 ops/s | 877.5 ops/s |
| Closed-loop HGET p99 | 31.4 ms | 27 µs |
| Closed-loop hash-write p99 | 85.8 ms | 117 ms |
| Paced mixed throughput | 613.5 ops/s | 617.3 ops/s |
| Paced HGET p99 | 33.0 ms | 21 µs |
| Paced hash-write p99 | 72.5 ms | 63.2 ms |

The old unmatched no-compaction/432-ops/s protected outcome did not recur in
these matched trials. The write-tail difference under saturation is retained
in the results rather than hidden by quoting only HGET.

After the final failed-lifecycle retry hardening, a separate **final-source
confirmation pair** reran both workloads and both public controls. Source hashes
and compiled-module identities are captured and checked within each cohort;
the confirmation is not pooled into the earlier three-pair medians.

| Final-source confirmation | Serialized baseline | Protected |
| --- | ---: | ---: |
| Closed-loop mixed throughput | 881.4 ops/s | 888.0 ops/s |
| Closed-loop HGET p99 | 31.1 ms | 19 µs |
| Closed-loop hash-write p99 | 90.0 ms | 112 ms |
| Paced mixed throughput | 613.7 ops/s | 622.6 ops/s |
| Paced HGET p99 | 23.2 ms | 18 µs |
| Paced hash-write p99 | 69.0 ms | 67.5 ms |

Both saturated confirmation runs again completed four compactions with zero
failures. Both paced runs completed zero; all operation errors were zero.
Subsecond durable-write maxima remain (protected saturated maximum 925 ms).

### Public controls and healthy-capacity fixture

The original host-volume Flow control was rejected by disk-pressure admission.
The continuation used a new isolated 8-GiB APFS sparse-image volume for native
TCP SET/GET and public Flow create/claim/complete/get controls. Admission remained
enabled; existing data and services were preserved. The image's I/O behavior
differs from the host filesystem, so compare only matched variants on that
volume, not absolute rates from earlier host-volume controls.
The owned scratch volume was detached after the final gates.

The harness was corrected to the current public mutation return contract (`:ok`)
and to pass the claimed `fencing_token` on completion. Values, IDs, and completed
state are verified. The failed pilots are not passing measurements.

Three-pair native cycle throughput medians were 283.1 versus 267.5/s, with
overlapping ranges and one protected tail outlier (435.5-ms p99). Flow medians
were 48.85 versus 47.04 cycles/s; baseline Flow had large tail variation. These
short controls do not establish a precise no-regression bound or attributed
speedup. The final-source confirmation completed native cycles at 257.5 versus
272.0/s (p99 88.0 versus 59.2 ms) and Flow cycles at 50.66 versus 50.82/s
(p99 320.2 versus 235.8 ms), all with zero errors.

The first serial gate reached its shell timeout during the final control's
teardown after all reports had been written. The owned process subsequently
exited, and summaries were generated from the complete reports. The separate
final-source gate exited successfully for every subprocess.

### Final verification

```text
mise exec -- mix test --seed 873483 --max-failures 1 --timeout 180000
core:   11926 passed, 3 skipped, 279 excluded (2651.1 seconds)
server:  2183 passed, 1 skipped,  61 excluded (269.1 seconds)
HTTP:     144 passed,             5 excluded (6.2 seconds)
total:  14253 passed, 4 skipped, 345 excluded
```

Two performance guards pass separately. The installed-dependency three-node
replication/failover suite passes **13 tests**, including the subsequently added
promoted hot-read replication/suspended-shard case. That added cluster-only case
was verified separately after the default umbrella run. Formatting,
warnings-as-errors compilation, both CI-equivalent Credo warning profiles, and
`git diff --check` pass. The umbrella output is archived at
`tool_0fbbbe4970015mHtlDs1SifQbP` in the OpenCode tool-output directory.

Reproduction/artifacts:

- `bench/promoted_live_gate.py`: serial alternating gate; `--final-source` runs
  the separate confirmation; `--summarize-only` / `--summarize-final` validate
  and aggregate their respective reports. Set `BENCH_DATA_PARENT` to an already
  mounted healthy-capacity scratch volume.
- `bench/results/promoted-live-{protected,protected-paced}-summary.json`:
  three-pair results and source/compiled-module identities.
- `bench/results/promoted-live-{protected,protected-paced}-final-summary.json`:
  final-source confirmation, kept separate.
- `bench/results/promoted-live-controls-{summary,final-summary}.json`:
  public control results and identities.
- `bench/results/promoted-live-atomicity-default-final.json`: final-source probe.

This adds a ninth retained, workload-specific steady-state optimization. The
protected promoted-read acceptance work is complete for the selected WARaft
path, with the saturated-write tradeoff recorded. Device/OS causes of long
durable writes remain a separate investigation. FerricStore changes are
uncommitted; dependency publication/integration stays paused.
