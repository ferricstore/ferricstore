# Three-node write-latency investigation

## Decision and current integration status

**The latest investigation retains a FerricStore-side term-fold fix.** Follower
heartbeat checks were repeatedly scanning the segment for new indexes beyond
their durable log tail. Three paired unprofiled trials lower median three-node
write p99 from **356.7 to 247.8 ms** on the installed dependency and from
**264.0 to 33.7 ms** with the isolated deadline fix. The full current umbrella
suite passes **14,232 tests**. FerricStore changes remain uncommitted, and
dependency publication stays paused.

The later [hash-stall follow-up](hash-stall-investigation.md) evaluated cached
promoted reads but reverted the prototype after a concurrent publication failure.
The restored production sources match this investigation's 14,232-test verified
state; the later report records its separate prototype and reversion checks.

**The commit-batch deadline fix is a supported first improvement.** A real
three-node regression fails on the published WARaft dependency and passes with
the isolated fix. The final compiler-aligned gate lowered median three-node
distributed-write p99 from **340.8 ms to 226.5 ms**, with single-node throughput
and p99 approximately flat. High replicated-write latency remains.

The code change is in an **isolated WARaft checkout**, based on its published
`v0.1.0` source commit `149ad68adee9dc7e753a4914f4ab429d2669360e`:

```text
/var/folders/n3/4p13zj5n0kjbs8xk2qb4jq080000gn/T/opencode/waraft-commit-latency-20261001
```

The authorized dependency source update was committed and pushed to WARaft main:
[`5dc5ce3a9ab13f600788e084f4b8584f39a1335d`](https://github.com/ferricstore/waraft/commit/5dc5ce3a9ab13f600788e084f4b8584f39a1335d),
"Fix leader commit batch deadlines for 0.1.1". It includes the version/changelog
and EUnit CI step. [Upstream CI](https://github.com/ferricstore/waraft/actions/runs/36852643622)
passed on OTP 27 and 28. The original WARaft checkout contained unrelated work
and was left untouched.

The 0.1.1 package built locally, but Hex publication failed at the local credential
unlock step. **Publication and FerricStore integration are paused at the user's
request.** FerricStore's installed dependency and `mix.lock` remain on 0.1.0;
the fix is not yet a normally loaded FerricStore change. No FerricStore pin,
commit, push, or package release was completed for this integration. Its source
patch and integration regression tests remain in this review tree.

Immediate and coalesced commit-index notification candidates pass their
correctness regression, but their loaded tails do not establish a consistent
benefit. They are archived experiments and are **not included in the pushed
deadline-only source**.

## Confirmed defect: a heartbeat reply overwrites the batch deadline

WARaft uses `gen_statem` state timeouts both to flush client commit batches and
to send periodic heartbeats. A client command arms the short batch window, but
an intervening follower `AppendEntriesResponse` returns a new heartbeat timeout.
That replaces the pending batch timer. With more acknowledgements, the delayed
flush can be postponed again, even though the batching window has elapsed.

This explains why a small configured batch window can turn into heartbeat-scale
write stalls. It is not fixed by removing fsync or the quorum requirement.

### Regression

The test starts a real three-node, one-shard group, sets a **100-ms batching
window** and a **2,000-ms heartbeat**, waits until a write is buffered, and sends
a valid acknowledgement for the already committed prefix. The buffered write
must retain its short deadline and complete within 400 ms. The test also checks
that it does not flush immediately before the batching window.

Published dependency: writer remains pending beyond the 400-ms assertion.
Candidate: write completes and is readable. The regression uses the actual
Raft callbacks/transport/storage rather than a mock timer implementation.

### Fix

On the first buffered command, capture the batch's **absolute monotonic
deadline**. When an acknowledgement or another leader event schedules its next
timeout, select the earlier of that remaining deadline and the normal heartbeat
interval. New commands in the same batch retain the original deadline.

An empty pending queue ignores the old deadline. Handover and in-flight async
append states keep their heartbeat timer, avoiding zero-timeout spinning while
an append or transfer cannot make progress. The configured batching interval,
heartbeat interval, commit-size limit, durable append, quorum calculation,
state-machine application, and redirected local-apply barrier are preserved.

Patch: `bench/experiments/waraft_commit_deadline.patch`.

## FerricStore defect: heartbeat term folds scanned past the durable tail

WARaft checks incoming heartbeats by folding local terms through the incoming
batch's end, including **new entries that the follower has not appended yet**.
The provider attempted `get/2` for every index. Each nonexistent-index cache miss
opened the segment and scanned its CRC-framed records from the beginning.
Several new entries multiplied those scans on the follower's Raft process before
it could acknowledge the append.

### Profile and retained fix

The opt-in benchmark profiler now records append wall time, heartbeat handler
and lookup time, request-local spans, and 10-ms actor-mailbox samples, resetting
histograms after warmup. It runs only in fresh benchmark peers. Timings nest and
must not be added as independent costs.

Sampled follower lookup p99 was **29.9–30.0 ms**, with heartbeat-handler p99
**93.8–97.4 ms**. After bounding the fold, node lookup p99 is **8–9 µs** in the
diagnostic sample. Storage apply p99 stays below about 1.1 ms in these samples.
Append latency still spikes; this does not establish that disk latency is absent.
Profiler mailboxes remained small. The payload-fsync telemetry histogram has no
samples and provides no evidence about its cost.

`fold_terms/5` in
`apps/ferricstore/src/ferricstore_waraft_spike_segment_log/sections/part_01.hrl`
now bounds its end once at the provider's existing durable last-index boundary.
An empty log returns an empty fold. It uses **durable bounds, not ETS residency**:
demoted entries still take the validated disk path. Existing term comparisons,
corruption propagation, fsync-before-ack, quorum, and local-apply barriers remain.

The failing-first regression traces the disk fallback for a fold extending past
32 cached durable entries. The original probes disk for nonexistent indexes;
the fix returns the same terms without those probes. A second regression caps
ETS residency, verifies all 16 durable terms are returned, corrupts a demoted
record's CRC, and verifies fail-closed behavior. The segment-log suite passes
**59 tests**.

### Matched unprofiled gate

Three alternating paired trials per dependency/node count: four shards, eight
clients, three seconds warmup, 15 measured seconds, 25% distributed acknowledged
writes and 75% checked leader reads. Every peer's server/provider checksum is
checked. Both dependency cohorts report compiler `10.0.3` and are summarized
separately. The baseline provider is compiled in a temporary directory with only
the term-fold bound reversed; installed/workspace source is not overwritten.

| Dependency / nodes / terms | Mixed ops/s | Write p50 | Write p95 | Write p99 (trial range) |
| --- | ---: | ---: | ---: | ---: |
| Installed 0.1.0 / 3 / baseline | 230.9 | 149.8 ms | 301.3 ms | 356.7 ms (338.8–378.5) |
| Installed 0.1.0 / 3 / bounded | 694.4 | 16.8 ms | 134.8 ms | 247.8 ms (247.6–247.9) |
| Deadline candidate / 3 / baseline | 360.9 | 74.3 ms | 192.6 ms | 264.0 ms (259.6–267.7) |
| Deadline candidate / 3 / bounded | 2,065.1 | 16.8 ms | 21.6 ms | 33.7 ms (28.6–54.8) |
| Installed 0.1.0 / 1 / baseline | 3,694.3 | 7.859 ms | 9.998 ms | 14.039 ms (13.226–14.398) |
| Installed 0.1.0 / 1 / bounded | 3,899.1 | 7.793 ms | 9.446 ms | 12.614 ms (12.197–12.788) |
| Deadline candidate / 1 / baseline | 3,590.1 | 7.884 ms | 10.251 ms | 22.262 ms (14.634–28.890) |
| Deadline candidate / 1 / bounded | 4,067.3 | 7.762 ms | 10.048 ms | 14.543 ms (12.090–15.142) |

Installed-dependency throughput rises **3.01x** and write p99 falls **30.5%**.
Read p99 increases 202→248 µs while the closed-loop workload performs about
three times as many operations; this is not an equal-offered-load read control.
With the deadline candidate, throughput rises **5.72x**, write p99 falls **87.2%**,
and read p99 is 197 versus 192 µs. Single-node controls show no median throughput
or write-tail regression here. Small trial counts, shared-host variation, and
filesystem pressure limit attribution; these are not production capacity or
strict tail bounds. All 24 scenarios validate reads, report zero errors, and
verify final values on every replica.

### Final correctness verification

```text
mise exec -- mix test --seed 873483 --max-failures 1 --timeout 180000
core:   11905 passed, 3 skipped, 279 excluded (2578.7 seconds)
server:  2183 passed, 1 skipped,  61 excluded (264.8 seconds)
HTTP:     144 passed,             5 excluded (6.1 seconds)
total:  14232 passed, 4 skipped, 345 excluded
```

Actual replication/failover checks pass **12 tests** on the installed dependency
and **13 checks**, one propagation experiment excluded, with the deadline
candidate. Both performance guards, formatting, warnings-as-errors compilation,
both CI-equivalent Credo warning profiles, and whitespace checks pass.

The first follow-up full run failed during isolated-instance setup with
`query_index_metadata_schema_mismatch`. Its directory used only
`unique_integer/1`, which can recur across BEAM lifetimes and reuse leftover
metadata. The helper now adds a random directory suffix; the complete retry
passes without relaxing validation or deleting the old root. The timer
regression also needed to drain startup batches before changing the interval
and measure operation elapsed time instead of asserting it remained pending
after a remote probe. The corrected test passes on the candidate and still fails
the 400-ms assertion on installed 0.1.0.

Artifacts: `term-fold-gate-{installed,deadline}-{baseline,bounded}-{1,2,3}.json`,
`term-fold-summary.json`, `multinode-deadline-detailed-profile.json`,
`multinode-deadline-protocol-profile.json`, and
`multinode-bounded-terms-protocol-profile.json` under `bench/results/`.
`bench/term_fold_summary.py` validates scenario counts/identities, compiler and
source checksums, settings, errors, and replica verification before aggregation.

Reproduce using `bench/multinode_mixed_perf.exs` with `BENCH_SEGMENT_TERMS=baseline`
or `bounded`, `BENCH_NODES=1,3`, `BENCH_TRIALS=1`, `BENCH_SECONDS=15`, and
`BENCH_WARMUP_SECONDS=3`. Alternate variants over `BENCH_TRIAL_ID=1`, `2`, `3`;
write separate `term-fold-gate-{dependency}-{terms}-{trial}.json` outputs.
Installed runs set `BENCH_VARIANT=installed` and omit `BENCH_WARAFT_EBIN`;
deadline runs set `BENCH_VARIANT=deadline` and select the explicit OTP-29 build.
After all 24 scenarios, run `python3 bench/term_fold_summary.py`. Use
`BENCH_PROFILE=1` and separate filenames for diagnostics; do not pool those
timings with the unprofiled gate.

## Measurements

Same benchmark shape as the prior load follow-up: local peer VMs, four shards,
eight clients, 256-byte values, 25% acknowledged writes / 75% validated
leader reads. Three-node writes are distributed across nodes and include leader
forwarding plus the required local-apply acknowledgement. The host remained
shared and under filesystem pressure, so these are relative diagnostics, not
production latency guarantees or confidence intervals.

The runner checks that every peer loads the selected server BEAM checksum.
This matters because repeated `-pa` arguments reverse code-path precedence on
the peers. Early candidate regression attempts inadvertently loaded the baseline
there; those attempts were excluded from the candidate's validation and timing.

### Earlier deadline-only gate: both dependency variants compiled on OTP 29

The standalone WARaft directory's default toolchain selected OTP 28, whereas
FerricStore used OTP 29. Earlier exploratory candidate/control measurements mixed
those compiler cohorts. The final gate explicitly builds both the v0.1.0 control
and deadline-only candidate with `mise exec erlang@29.0.5 -- rebar3 compile` and
checks the selected module checksum on every peer. All retained gate trials report
the same Erlang compiler version, `10.0.3`.

Five alternating single-node pairs and three alternating three-node pairs use
fresh fixtures, three seconds of warmup, and 15 measured seconds per trial.
Numbers below are medians of trial metrics; p99 ranges are trial ranges.

| Shape / dependency | Total mixed ops/s | Write p50 | Write p95 | Write p99 (range) |
| --- | ---: | ---: | ---: | ---: |
| One node / control | 3,938.8 | 7.711 ms | 8.431 ms | 9.255 ms (8.931–9.727) |
| One node / deadline | 3,946.1 | 7.701 ms | 8.342 ms | 9.301 ms (8.876–9.455) |
| Three nodes / control | 217.1 | 153.1 ms | 305.8 ms | 340.8 ms (331.2–367.8) |
| Three nodes / deadline | 360.7 | 76.7 ms | 176.8 ms | 226.5 ms (216.7–285.7) |

Single-node median throughput changes **+0.19%** and write p99 **+0.50%**, with
overlapping ranges. Three-node median throughput increases **66.1%** and write
p99 falls **33.5%**. Every gate trial reports zero errors, validates reads, and
verifies final values on all replicas. These short matched controls support the
deadline fix; they are not a production capacity estimate or strict tail bound.

Artifacts:

- `bench/results/commit-otp29-{1,3}-{control,deadline}-*.json`
- `bench/results/commit-release-gate-summary.json`
- `bench/commit_gate_summary.py` (checks trial counts, compiler identity, errors,
  and replica verification)

### Earlier exploratory measurements, superseded

The original two 30-second three-node trials gave control p99 of 389–404 ms
versus deadline-only 283–296 ms. Their single-node controls were noisier and
showed worse candidate tails. Those results prompted the toolchain audit and
are preserved for provenance, but the compiler-aligned gate above replaces them
for the acceptance decision:

- `bench/results/multinode-commit-deadline-control.json`
- `bench/results/multinode-commit-deadline-candidate.json`
- `bench/results/multinode-commit-summary.json`

## What other projects do

The comparison uses their documented mechanisms, not their advertised throughput
as an apples-to-apples benchmark:

- **etcd/Raft:** its `maybeCommit()` documentation explicitly calls for
  `bcastAppend()` when the commit index advances. Appends can convey a new commit
  index even with no new entries. Periodic heartbeats maintain leadership;
  they are not the only trigger for replication/commit progress.
  [Source](https://github.com/etcd-io/raft/blob/main/raft.go).
- **TiKV raft-rs:** `propose` and `step` drive event processing; `Ready` and
  `LightReady` expose outgoing messages and committed entries separately from
  the timer tick. Its async-storage documentation retains the requirement that
  persisted messages wait for the corresponding durable writes. Client
  callbacks are fulfilled after application, preserving the distinction between
  replication, persistence, and application.
  [Documentation](https://docs.rs/raft/0.7.0/raft/).
- **Dragonboat:** documents fully pipelined, multi-group operation, batching,
  and independent log storage/transport interfaces. It notes that active Raft
  group count affects batchability. This supports measuring batching and I/O
  scheduling rather than importing a benchmark-specific heartbeat constant.
  [Project documentation](https://github.com/lni/dragonboat#features).

These comparisons support the direction: preserve durable acknowledgement and
application semantics, while ensuring events advance the protocol without
unnecessary timer delays. They do not justify weakening consistency or replacing
the engine wholesale.

## Second experiment: immediate commit-index propagation

The prior trace identified approximately 100–130 ms of redirected local-apply
wait. Even after fixing the batching timer, a follower may persist the new entry
before learning that it is now committed. The published leader response path
normally leaves that notification to a later heartbeat when none is otherwise due.

Following the event-driven approach seen in etcd/Raft, the experiment sends an
existing-format AppendEntries heartbeat when a quorum acknowledgement actually
advances the commit index. It does not flush newly pending client batches and
does not send again for an unchanged index. A handover keeps its existing path.

A separate real-cluster regression sets the heartbeat to two seconds and writes
through a follower. The original and deadline-only paths fail its 400-ms
completion assertion; the combined candidate passes, and the value is immediately
readable on the writing follower. The local-apply barrier remains in place.

However, two loaded distributed-write trials with the combined change had p99
**463 and 704 ms**, despite improved medians and roughly 339–354 mixed ops/s.
Leader-targeted trials had p99 **227–238 ms**, below the earlier 290-ms results,
but that does not override the inconsistent distributed tails. Extra replication
messages, queueing, storage contention, or host variation need isolation before
acceptance. The regression exposes a real idle propagation delay; it does not
prove that this particular loaded implementation is the best performance fix.

The second change was removed from the isolated recommended candidate. Patch:
`bench/experiments/waraft_commit_propagation.patch`, applicable after the deadline
patch, **experimental only**. Its regression is enabled with `BENCH_PROPAGATION=1`.

A subsequent coalesced-notification prototype also passed the propagation
regression but did not show a consistent loaded-tail improvement. Its patch is
`bench/experiments/waraft_coalesced_notification.patch`. Neither notification
prototype is included in the pushed source. Their exploratory compiler cohorts
must not be pooled with the final deadline-only gate.

Artifacts: `multinode-commit-propagation-candidate.json` and
`multinode-commit-propagation-leader.json` under `bench/results/`.

## Verification

- Recommended deadline-only candidate: **five EUnit tests passed**, covering
  decreasing/elapsed deadlines, idle queues, in-flight append, and handover.
- Recommended candidate: **13 cluster checks passed, one propagation experiment
  excluded**. Includes the new deadline regression and the existing 12 replication,
  quorum, restart, and failover checks.
- Combined experimental candidate: **14 cluster checks passed**, including both
  new regressions; performance results still prevented recommending it.
- Dependency compile, Xref, five EUnit tests, and Dialyzer passed locally on
  OTP 28 and explicit OTP 29. Upstream CI passed on OTP 27/28 after the source push.
  Patch application/reversal checks and whitespace checks passed.

The experimental regressions live under `bench/regressions/` so the current
published dependency's normal CI lane is not made permanently failing before
the fix is integrated. The same tests should enter upstream/integration CI with
the dependency change. Full umbrella validation with the newly pinned dependency,
broad Jepsen/partition, Linux-only I/O, and release-artifact checks are still
required for shipping that dependency integration.

## Integration status and remaining optimization work

1. Hex publication and FerricStore dependency-pin integration are paused. If
   resumed, publish the pushed 0.1.1 source and run normal FerricStore integration
   and release CI. The compiler-aligned local gate and regressions are recorded.
2. Profile the remaining commit-to-local-apply delay with per-node queue depth,
   append/fsync latency, and commit-notification traffic. Test targeted/coalesced
   event-driven propagation against the regression and loaded p95/p99.
3. Repeat on separate nodes/disks with controlled CPU/network load before
   changing cluster defaults. A 120-ms periodic heartbeat should not impose a
   120-ms wait on healthy commit progress, but blindly increasing heartbeat rate
   adds idle traffic and does not fix all observed leader stalls.

### Run the recommended candidate checks

Build each isolated WARaft variant with explicit OTP 29:

```sh
mise exec erlang@29.0.5 -- rebar3 compile
```

Then, from FerricStore's review worktree:

```sh
MIX_ENV=test ERL_FLAGS='+S 4:4' BENCH_CLUSTER_CHECKS=1 \
  BENCH_WARAFT_EBIN=/path/to/waraft/_build/default/lib/wa_raft/ebin \
  mise exec -- mix run --no-start bench/waraft_timer_regression.exs

MIX_ENV=test ERL_FLAGS='+S 4:4' \
  BENCH_WARAFT_EBIN=/path/to/waraft/_build/default/lib/wa_raft/ebin \
  BENCH_OUTPUT=bench/results/multinode-commit-deadline-candidate.json \
  mise exec -- mix run --no-start bench/multinode_mixed_perf.exs
python3 bench/multinode_commit_summary.py
```

The EUnit source is also preserved as
`bench/regressions/wa_raft_server_commit_deadline_test.erl` for copying into the
dependency's test directory. Baseline runs omit `BENCH_WARAFT_EBIN`. Candidate
loading is confined to fresh test/benchmark VMs; no installed dependency files
or other running services are overwritten.

For the compiler-aligned gate, point `BENCH_WARAFT_EBIN` at an explicitly OTP-29
built control or candidate, set `BENCH_VARIANT=control` or `deadline`, and use
`BENCH_NODES=1` or `3`, `BENCH_TRIALS=1`, `BENCH_SECONDS=15`, and
`BENCH_WARMUP_SECONDS=3`; label each run with `BENCH_TRIAL_ID`. Write separate
`commit-otp29-{nodes}-{variant}-{trial}.json` outputs, alternating variants across
five single-node pairs and three three-node pairs. Run
`python3 bench/commit_gate_summary.py` to validate and summarize those artifacts.
