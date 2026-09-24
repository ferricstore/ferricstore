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
If a sidecar update fails after a WAL append has committed, the append keeps its
durable outcome and that directory's sidecar is marked untrusted. Older reads
then use verified segment scans until a complete rebuild; a stale sidecar must
never select an earlier version of a repeated projection index.

Recovery must still validate the WAL and rebuild state not covered by a durable
checkpoint before the server admits writes. Disk-backed offsets make this
bounded in RAM and remove repeated historical segment scans; they do not make
an uncheckpointed multi-gigabyte WAL disappear. On nodes with at least 4 GiB
of configured memory budget and more than one scheduler, two independent LMDB
shard rebuilds may proceed concurrently during startup; smaller nodes keep one
permit. Normal post-startup flushes return to one permit to retain the previous
write-path I/O budget. Set `FERRICSTORE_FLOW_LMDB_MAX_CONCURRENT_FLUSHES` to
explicitly override both budgets.
Independent Flow-history shard recovery also runs two at a time on nodes with
at least 4 GiB, with the largest retained logs first. Startup waits for every
history shard to succeed before starting Raft; smaller nodes remain serial.
Initial WAL scans use bounded 1 MiB read-ahead per active reader to reduce
per-record disk calls. This does not bypass CRC, record, or projection checks,
and ordinary runtime reads retain their previous file-opening behavior.
After a durable logical trim, startup scans and disk folds start at the first
segment that can contain an untrimmed record. Fully trimmed physical segment
files are storage debt, not replayable log entries; their stale bytes cannot
delay recovery or become visible again. A partial first segment still receives
normal CRC and index validation.

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

The operational guard logs RSS/disk pressure transitions with byte budgets,
the offset registry footprint, and the Flow admission state. It reports only
state changes, rather than logging every one-second guard sample. Flow policy
catalog projection gaps retain their fail-closed health status; identical
retries are logged once until the condition recovers or changes.
