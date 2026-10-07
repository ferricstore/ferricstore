# WARaft offset index and startup

FerricStore keeps the authoritative Raft/WARaft records on disk. A bounded ETS
tail caches recent physical positions; older positions are read from a derived
`<segment-ordinal>.idx` sidecar next to each `.seg` file. The sidecar has one
fixed-width, CRC-checked slot per record index, keyed by the segment's configured
records-per-segment value. It is an index, **not another WAL** and not a source of
truth. A missing, incomplete, or malformed slot falls back to a validated scan
of the original segment. Even a checksum-valid sidecar slot is checked against
the WAL frame's index and length before use; reading the actual record also
checks its WAL CRC.

The hot ETS cache holds at most 8,192 positions per segment-log directory by
default, independently of the length of the retained WAL. The derived sidecars
are rebuilt while validating the WAL on startup, and after a directory rewrite.
Neither replay nor a read-only fold retains historical offsets in ETS. Logical
trim removes fully trimmed sidecars; an incomplete sidecar build is disposable
and can be reconstructed from the authoritative WAL. This avoids a 20-million-
row ETS offset registry while keeping historical lookup near constant time.
Successful rewrite-directory removal/replacement also retires its RAM offsets
and path-keyed caches without removing replacement sidecars. The operational
guard reclaims already-abandoned, missing temporary rewrite indexes on its first
check and every 30 seconds; live writers, existing/unsafe paths and canonical
directories are retained. See the [rewrite cleanup follow-up](rewrite-offset-index-cleanup-followup.md)
for the full-cap reproduction, explicit reclamation API and verification scope.
If a sidecar update fails after a WAL append has committed, the append keeps its
durable outcome and that directory's sidecar is marked untrusted. Older reads
then use verified segment scans until a complete rebuild; a stale sidecar must
never select an earlier version of a repeated projection index.

Recovery must still validate the WAL and rebuild state not covered by a durable
checkpoint before the server admits writes. Disk-backed offsets make this
bounded in RAM and remove repeated historical segment scans; they do not make
an uncheckpointed multi-gigabyte WAL disappear. On nodes with at least 4 GiB
of configured memory budget and more than one scheduler, two independent LMDB
shard rebuilds may proceed concurrently during startup. At 4.5 GiB of
configured memory and at least three schedulers, the startup limit rises to
three; smaller nodes keep one permit. Normal post-startup flushes return to one
permit to retain the previous write-path I/O budget. The existing
`FERRICSTORE_FLOW_LMDB_MAX_CONCURRENT_FLUSHES` override still controls both
budgets when explicitly set. With three preopen workers on the isolated
16-shard volume, three startup LMDB permits measured 121.8–134.5 seconds
end-to-end versus 136.8–139.5 seconds in same-image two-permit controls. The
range reflects substantial run-to-run variation; no per-restart saving is
guaranteed.
Independent Flow-history shard recovery uses the same memory-aware startup
budget: two at a time from 4 GiB, or three from 4.5 GiB with at least three
schedulers, with the largest retained logs first. Startup waits for every
history shard to succeed before starting Raft; smaller nodes remain serial.
On the isolated 16-shard volume, three-way history recovery measured 109.7
and 113.2 seconds end-to-end versus 119.7–120.3 seconds for adjacent two-way
controls. This affects only the startup barrier, not post-startup workers.
Initial WAL scans use bounded 1 MiB read-ahead per active reader to reduce
per-record disk calls. This does not bypass CRC, record, or projection checks,
and ordinary runtime reads retain their previous file-opening behavior.
Partition creation under the WARaft supervisor is synchronous, so concurrent
`start_partition` requests alone still recover shards one at a time. On nodes
with a configured memory budget of at least 4 GiB and more than one scheduler,
startup prepares two independent shard storage handles concurrently before
starting their Raft children. At 4.5 GiB and at least three schedulers, both
the preopen work queue and startup LMDB coordinator use three slots. When the
detected node limit is at least 6 GiB, the existing internal startup budget
already allows three slots, and four schedulers are available, only the
preopen work queue increases to four. Startup Flow-history recovery and LMDB
reconciliation remain capped at three. Storage recovery uses the existing
validated WAL path; once prepared, any ETS tables created by a worker are
handed to its real storage process before the worker exits. A failed
preparation aborts startup, and a storage handoff failure fails closed.
Smaller nodes and single-shard instances retain normal serial opening. Set
`FERRICSTORE_WARAFT_START_PREOPEN_CONCURRENCY=0` to disable preparation, or
explicitly set `2`, `3`, or `4` to override the adaptive work-queue limit
(capped at four). Explicit `4` on a smaller node requires sufficient startup
memory headroom. The slots form a bounded work queue: when a shard finishes
recovery, the next shard can start without waiting for the other slot to finish.
There are no preparation workers or additional per-write checks after startup.
On an isolated 16-shard workload, startup took 143.1–143.3 seconds with the
work queue, versus 158.3–161.3 seconds with fixed pairs and 191.4 seconds
without preopening. These are isolated comparisons, not latency guarantees
for other workloads.
In a 6 GiB isolated container, three workers took
138.0–138.7 seconds versus 146.8 seconds for a same-image two-worker control.
Peak sampled container memory was 2.27 GiB; other data sets may need more
headroom, so the extra slot requires a larger detected memory budget.
On the same isolated 6 GiB volume, an explicit four-worker trial started in
109.1–112.7 seconds versus 116.6 seconds for a same-image three-worker
control. On the subsequently buffered-history build, two four-worker restarts
took 94.1 and 93.8 seconds versus 105.9 seconds for an adjacent three-worker
control, with sampled peak container memory of 2.25 GiB and no OOM. These
workload-specific measurements motivate the 6 GiB/four-scheduler threshold;
other data sets may use more startup memory.
After a durable logical trim, startup scans and disk folds start at the first
segment that can contain an untrimmed record. Fully trimmed physical segment
files are storage debt, not replayable log entries; their stale bytes cannot
delay recovery or become visible again. A partial first segment still receives
normal CRC and index validation.
When a durable segment projection has been validated, state-machine replay
starts its disk fold at the first record *after* that projection's Raft index.
The first tail record must pass the normal record checks; if its preflight is
missing or invalid, recovery uses the original full fold instead of assuming a
contiguous tail (including the existing torn-final-record recovery behavior).
Corrupt replayed segments still fail recovery. Reads of a partial first
segment still validate its earlier frames, while fully covered segments are
left to the independent Raft-log open validation. Without a projection,
startup retains the full fold. This optimization adds no recurring work to
live writes.

During startup replay, pre-serialized TTB commands are now decoded once before
checking whether a record can use the direct projection path. If it needs the
state machine, replay passes the *validated stamped command with its original
wrappers* through the normal apply path instead of decoding the same binary
again. Invalid TTB payloads still take the original error path; WAL integrity
and dependency validation are unchanged. This work runs only during recovery.
One isolated 16-shard restart took 190.3 seconds after this change, compared
with 203.8 seconds in the preceding baseline run. A second comparison on the
same isolated volume measured 191.4 seconds versus 193.6 seconds, respectively.
These differences include run-to-run variability and are not guaranteed
per-restart savings.

Background segment-projection checkpoints are triggered by a Raft position gap
and minimum interval, rather than a periodic full-keydir scan. A node-wide
nonblocking slot lets only one shard serialize a checkpoint at a time. Other
shards defer until a later qualifying write; a deferred attempt does not advance
the checkpoint position. After scanning, the worker asks the owning storage
process to confirm that its Raft position has not advanced since the snapshot
began. A changed position discards the candidate before it is published; writes
after this confirmation do not change the already collected rows. A discarded
cut waits five minutes before another attempt, avoiding repeated full scans on
a busy shard. This adds no per-write work. On an isolated legacy 16-shard
workload, forcing
simultaneous checkpoints previously reached about 636% CPU. With the slot, a
193,706-entry shard checkpoint took about 20 seconds, used about 300 MiB of
additional memory at the observed sample, and a concurrent write completed in
about 9 ms; the observed CPU sample was about 129%. These measurements are
diagnostic, not a write-latency guarantee.

The background checkpoint is currently used for trim preparation, **not as a
crash-recovery replay boundary**. Its worker scans a live ETS table while Raft
may continue to apply writes. The position check rejects one kind of torn
snapshot, but a matching Raft index and checkpoint header alone do not prove
that every dependency and any mutation outside Raft has the same cut. Before
recovery can consume a checkpoint, it needs provenance for the complete state,
crash-safe publication of that state and its dependencies, and a validated
fallback for missing, stale, or damaged candidates. On that same legacy workload,
roughly 215,000 historical
`flow_claim_due` commands accounted for about 56 seconds of direct replay work,
making a proven boundary more valuable than skipping inexpensive read commands.

During WAL replay only, an empty Flow cold-due bucket window can now be proved
with one bounded LMDB range read. Recovery checks the LMDB transaction ID
before and after the proof and again before applying the result. It advances
the same recent/backfill cursor as the existing bucket scanner, calculating
the empty-window cursor directly rather than iterating up to 80 empty pages.
A row in the window, a changed transaction, incomplete LMDB flush, bad range,
or failed read takes the original per-bucket scan; proofs are never cached across
commands. Normal Flow claims keep their existing scan and do not perform the
extra LMDB range read. On the isolated 16-shard volume, two runs took 116.9
and 119.9 seconds, versus 130.3 seconds for an adjacent old-image control.
These are workload-specific measurements, not a guarantee for other data.
After the direct cursor calculation, two isolated restarts took 112.3 and
113.3 seconds versus 118.3 seconds in an adjacent pre-change control. Earlier
pre-change runs were faster, so this is a modest measured gain, not a stable
per-restart guarantee.
Temporary per-command profiling on the buffered, four-preopen-worker build
found that replay work, rather than WAL reading and CRC validation, dominates
the largest shard folds. Across 16 shards, about 225,000 `flow_claim_due`
commands spent approximately 53 seconds summed across workers checking
228,000 empty cold-due windows. On shard 12 the proof alone took about
9.6 seconds summed. These are instrumented per-shard sums, not elapsed startup
time. Because earlier cross-command proof caching and a combined native probe
did not produce a reliable improvement, recovery still checks each proof
against its own LMDB transaction and retains the original scan fallback.

A subsequent recovery-only attempt to bypass the shared Erlang file server for
the proof's no-follow metadata reads was also **reverted**. Although a four-worker
component benchmark improved by about 25%, the initial full-startup median moved
only from 96.5 to 95.4 seconds, and later paired runs included slower candidates.
Recovery-state snapshot differences were not fully isolated. The benchmark
gate therefore did not accept the change; see
[the rejected metadata experiment](startup-replay-metadata-experiment.md) for
all measurements and validation limits.

The subsequent apply-projection locator pass still validates every retained
projection log frame and every generated Flow value reference. During startup
it now writes consecutive value-pin operations to LMDB in bounded batches
(at most 256 operations and 2 MiB per batch), preserving WAL order and the
last locator for a repeated key. Oversized individual records keep their
original standalone write, and any LMDB error fails recovery closed. This
changes no hot apply or normal-runtime write path. On the isolated volume,
summed locator recovery fell from 47.5–50.8 to 4.8–4.9 seconds; adjacent
end-to-end startups measured 117.0 seconds before versus 107.3 seconds after.

On a completed shared-ref migration, startup still checks every cleanup member
against its owner and removes stale members. Rebuilding the ephemeral native
cleanup index now inserts validated members in batches of at most 128, instead
of making a native call for each member. The same isolated 16-shard workload
measured 13.6 seconds summed across cleanup rebuilds before batching and 12.1
seconds after; end-to-end startup varied from 206 to 197 seconds across those
runs. No validation, fail-closed ownership check, or live write path is skipped.
For completed migrations, a single cleanup-index page may reuse a decoded hot
owner record only while its exact ETS-encoded value remains unchanged. The
page-local cache is bounded to 64 owners, 2 MiB total, and 64 KiB per owner;
cold, LMDB-only, and encoded blob-reference owners keep their original read
path. Every member still passes its ownership check, and a changed owner is
re-decoded. On an isolated 16-shard restart, summed post-replay LMDB
reconciliation fell from 70.4 seconds to 51.7–53.6 seconds; adjacent startup
runs measured 123.0 seconds before versus 109.7–113.8 seconds after. This
cache is used only while rebuilding after recovery, never on normal writes.

Flow history records an optional recovery boundary inside the same durable LMDB
environment that holds its history index. After a successful full recovery
with no hot-history rows, the boundary binds the projected Raft index, the
physical end of the validated history log, and its SHA-256 digest. On the next
restart, startup checks every covered frame's CRC and the prefix digest in
bounded native memory, then replays only later history records and tombstones.
This avoids republishing old LMDB rows without skipping integrity validation.
A missing boundary uses the original full recovery; a committed boundary that
disagrees with the log fails closed rather than exposing stale LMDB history.
Configurations with hot history or synchronous history retain full recovery.
An incomplete final record remains eligible for the existing tolerant recovery
path, without publishing a checkpoint for that incomplete suffix.

Startup Flow-history metadata pages now use a bounded 256 KiB read buffer
while validating each record's CRC, so scanning the uncheckpointed tail does
not issue separate file reads for every small header and key. Live scans keep
their original unbuffered memory footprint. Page boundaries, truncated-tail
handling, and integrity failures are unchanged. On adjacent
isolated 16-shard restarts, shard 15's tail scan took 30.4 seconds without
buffering versus 15.6 seconds with it; end-to-end startup took 111.3 versus
106.2 seconds. These timings depend on file size and concurrent shard work.
For the buffered-scanner build, increasing only the pre-Raft Flow-history
recovery queue from three to four workers produced 108.3 and 106.0 seconds
on two isolated 6 GiB restarts; an adjacent three-worker control took 105.7
seconds. Both modes recovered all 16 shards without OOM, with sampled peak
container memory around 2.2–2.3 GiB. Because four workers showed no
repeatable startup gain, the memory-aware three-worker default remains.

The operational guard logs RSS/disk pressure transitions with byte budgets,
the offset registry footprint, and the Flow admission state. It reports only
state changes, rather than logging every one-second guard sample. Flow policy
catalog projection gaps retain their fail-closed health status; identical
retries are logged once until the condition recovers or changes. A shard with
the persistent `:policy_catalog_state_projection_pending` warning uses a
bounded per-shard retry delay (5–60 seconds), including when other shards are
catching up; a successful retry clears the delay. Explicit attribute-repair
requests wake the worker immediately.
