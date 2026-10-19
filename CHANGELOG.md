# Changelog

All notable changes to FerricStore will be documented here.

## Unreleased

- Keep the dashboard's run pages working when a run or schedule time is past
  year 9999. The store accepts any integer run time, and the new compact rows
  and schedule times raised on such a value, so one such run broke the Due
  and Runs pages for every viewer; they now show "-".
- Order journal events from the same millisecond by sequence number. Event
  IDs were compared as text, so with ten or more events in one millisecond
  the journal listed them out of order with wrong "from" states, and the
  Lifecycle strip could repeat a state.
- Reject a negative or non-integer offset in the embedded `FerricStore.setrange/3`
  with "ERR offset is out of range". The call skipped the offset check that
  the SETRANGE command has, so the bad offset reached the replicated log and
  raised while the shard applied it; the shard's storage process crashed
  until the node shut down, and replaying the entry crashed it again. The
  state machine now also returns an error for a bad SETRANGE or SETBIT offset
  instead of raising.

- Redesign the dashboard shell. The sidebar has 18 destinations in
  always-open sections instead of about 30 links in collapsible groups; lower-use
  pages are tabs of their destination. Every page shares one header with its
  section, title, purpose, instance and live status. A single palette, Geist
  type, sentence-case labels and calmer tables, buttons and links replace the
  older mixed styles.
- Redesign the dashboard overview: what needs attention (or one Healthy line),
  four headline cards with live commands per second, workflow states as one bar
  with the busiest types, shards as a grid, and small storage, keydir, client
  and slow-command cards. The live overview payload no longer has a `top_bar`
  component.
- Redesign the dashboard Failures page. Expired leases are grouped by type and
  partition, largest first; each group opens to its runs and a confirmed
  reclaim limited to its own type and partition. The general reclaim form is
  folded under "Reclaim by type and partition", and failed or out-of-retry runs
  use compact rows.
- Compact the dashboard's recent run rows to two short lines, and style the run
  status badges, which rendered as plain text.
- Redesign the dashboard Consensus and Doctor pages. Consensus leads with one
  status line (shards without a leader, unreadable or more than 100 entries
  behind are named), summary cards and a tile per shard, with the full table
  folded away. Doctor leads with one status line and named check rows, and
  folds the CLI reference away.
- Redesign the dashboard's workflow detail, Schedules and Keys pages. A run's
  page is titled by its ID and shows a Lifecycle strip of the states it
  entered, each linking to its journal entry, with an expired lease as the
  current step. Schedules read as cadence and relative next and last runs,
  with Run now and Pause on the row and Edit and Delete under More. The two
  key searches are one row each, with a prompt instead of an empty table.
- Say that the overview's workflow numbers and the Failures badge come from
  the overview's small sample.
- Finish the dashboard pass: workflow pages share one scope row (run lookup and
  scope, with the repeated route links removed and the header tabs carrying the
  scope), running and due rows are compact, and every page gets 14 px body
  text, smaller disclosure titles and summary cards that show words such as
  Unavailable as words. Table scroll boxes now contain positioned labels, which
  made Workers scroll sideways on phones.
- Redesign the remaining workflow pages. Runs, Due and Workers open with a
  status line saying what needs attention (each kind of problem counted
  separately, never summed) with a link to Failures. Runs gets a state split
  bar and a six-column states table. Due rows lead with how overdue a run is.
  Worker rows show leases as bars with the expired share. Signals offers its
  opt-in history scan as one click, Lineage offers recent runs to start
  from, and empty sections across these pages are short prompts instead of
  empty tables.
- Show signed-in users the query prompt (for example "Enter a partition
  key") instead of "0 visible row(s)" before a query has run.
- Redesign the Settings pages. Policies states a shared policy once and
  trims its table to nine columns, with an Edit column that no longer
  clips Max active. Governance leads with open circuits and waiting
  approvals over a compact count strip. Retention says whether the sample
  has anything to clean up, with every cleanup safeguard unchanged. The
  shared row filter control is laid out as one aligned row.
- Redesign the Data pages. Storage, Merges, Prefixes, Read path, Commands,
  Streams and Pub/Sub each open with a status line, and summary counts
  become one compact connected strip. Shard sizes, prefix shares and cold
  read shares are drawn as bars. When nothing has merged, the per-shard
  merge table folds away, as does the command reference. Empty bounded
  tables are one-line prompts, and their row filter appears with their rows.
- Redesign the Cluster and Admin pages. Clients warns about blocked
  connections, Slow log names its worst command, and Security says whether
  access is protected, with the accounts list first, account creation
  folded below it and the route reference folded away. Config states how
  many parameters there are and trims its table to four columns with a row
  filter, and Capabilities names what is unsupported.
- Make the dashboard's status lines say no more than their data supports.
  - A failed Governance load reads as unavailable, not as unused.
  - Partial Runs results never show a healthy line.
  - Lists capped when collected (Due's scheduled runs, Prefixes, Slow log)
    report their full counts or say what they show.
  - Clients counts blocked clients instead of the keys they wait on.
  - Policies compares state overrides before calling a policy shared.
  - Access-filtered empty states say "visible".
  - Sampled hot reads are labelled estimates.

## 0.12.6 - 2026-10-19

- Start when a Flow's LMDB row points at a segment-projection frame a rewrite
  has since repacked. 0.12.4 let startup go on when such an owner read back as
  `:query_storage_inconsistent` (its old frame now holds another key); the old
  offset can also land inside a frame or past its end, which read as
  `:query_storage_unavailable` and still refused to start. A graceful restart
  of the Inspeactor data could do this. Since 0.12.5 a Flow whose record sits
  on the projection stays in the keydir, so an owner missing from it whose row
  names a projection frame has lost its record whatever its read fails with:
  its cleanup members are kept but not registered. Rows pointing anywhere else
  still fail closed, and the startup error now names the owner.
- Finish the Flow policy catalog backfill when a registered Flow has lost its
  state record. The backfill read "registry present, state in neither the
  keydir nor LMDB" as a projection still on its way and retried it every minute
  for good, so a shard holding such a Flow never finished; it now flushes the
  shard's LMDB writer, reads again, and skips the Flow, counting it in
  `[:ferricstore, :flow, :policy_migration, :orphaned_states]` and in one warning
  per finished run. A Flow missing from the keydir whose LMDB row names a
  segment-projection frame that no longer reads back counts as having lost its
  record too.
- Remove the policy backfill staging of earlier runs when a new run starts. A run
  that never finished kept its staged copy of the keydir in LMDB, and each new run
  staged another.
- `FerricStore.flow_purge_orphans/1` lists Flows whose state record is gone and,
  with `dry_run: false`, removes what they left (registry marker, retention guard,
  catalog entry, values, history, query index rows, and retention cleanup members
  startup left unregistered), releasing their shared value references. Retention
  starts from the state record, so it never reached them.
  Purging needs cluster feature level 3 (new; raise it with
  `CLUSTER.FEATURES.RAISE 3` once every node runs this release).

## 0.12.5 - 2026-10-19

- Stop losing the record of a Flow whose state sits on the segment projection.
  The projection's frame numbers are positions that every rewrite renumbers,
  and a rewrite keeps only keydir rows. Terminal prune, hibernation eviction
  and the LMDB rebuilder's terminal prune could drop such a Flow's keydir row
  and leave LMDB pointing at a frame number, so the next rewrite (a checkpoint,
  or the rebuild at a trim or shutdown) discarded the record. This happened
  after a restart, when the LMDB writer replays Flows whose rows were loaded
  from the projection; it is how the 0.12.4 startup failure came about. Such a
  Flow now stays in the keydir and is not hibernated.
- Repair a QueryRow left on a renumbered projection frame. A read of such a
  Flow returned `:query_storage_inconsistent`; the repair now takes the
  record's current frame from the keydir, checks that the frame holds it, and
  moves the row there.

## 0.12.4 - 2026-10-18

- Start when a Flow's stored state record can no longer be read back. A
  segment-projection rewrite (checkpoint adoption, or the rebuild at a trim or
  shutdown) keeps only keydir-backed values, so a finished Flow whose state
  lived only in LMDB could lose its record, and the startup rebuild of the
  retention cleanup index then refused to start the whole store with
  `:query_storage_inconsistent`. Members of such an owner are now kept but not
  registered, so nothing is released on its behalf, and reported with a
  warning and `[:ferricstore, :flow, :shared_ref_backfill,
  :cleanup_owner_unreadable]`. Reads of that Flow still fail closed.
- Stop the QueryRow locator repair from moving a row onto another key's
  record. Segment-projection frame numbers are positions in the current log
  and every rewrite renumbers them; the repair now uses a projection frame
  only after reading it and finding the row's key and checksum there.

## 0.12.3 - 2026-10-18

- Give schedules held back by overlap the short history too. A schedule whose
  previous target never finishes (`overlap_policy: :queue_after_previous` or
  `:skip`) does not fire again: each due time it records the overlap and
  rewrites its record, about every 30 seconds with the default retry. Only a
  fire carried the 100-event history window, so a schedule created with a
  longer one kept it and kept every version, and its keydir grew without bound
  even at feature level 2. Overlap, skip and manual fires now carry the window
  as well, and the background worker then releases the values and members the
  trimmed history held. The change needs no new feature level: on a cluster
  already at level 2 it takes effect as each node is upgraded.

## 0.12.2 - 2026-10-18

- Stop Flows that never finish, such as schedules, from growing the keydir
  without bound. Every change to a Flow's payload, result or error writes the
  value under a new version, with a retention cleanup member (a hot keydir row)
  for it. Members were removed only when a finished Flow was pruned, so each
  schedule fire added one for good, until the memory guard refused new Flows.
  - At cluster feature level 2 (raise it with `CLUSTER.FEATURES.RAISE 2` once
    every node runs this release), a background worker deletes the values that
    a running Flow's remaining history and its record no longer refer to,
    together with their members. Flows keeping more than 1,000 history events
    keep their values.
  - At level 2, a rewind to an event whose values were deleted fails with
    `ERR flow rewind target event not found`, as for a trimmed event.
  - A schedule's own Flow keeps 100 history events
    (`:flow_schedule_history_max_events`). At level 2, a schedule created with
    a longer history takes the shorter one on its next fire, and the worker
    then deletes the values and members the trimmed history held.
- Fix a rewind to an event recorded while the Flow was claimed crashing the
  shard's storage process. The rewound record was running with no lease owner,
  and building its worker index key raised inside Raft apply. Such a rewind now
  fails with `ERR flow running state is only entered by FLOW.CLAIM_DUE`, and a
  running record without a lease owner fails validation instead of raising.

## 0.12.1 - 2026-10-17

- Stop WARaft log trims from stalling writes. Every 50k entries a trim rewrote
  each live key into the segment projection inside the Raft commit path, so
  write pauses grew with the dataset (several seconds at a few hundred thousand
  keys per shard, past the 10 s write timeout around a million keys).
  - Background checkpoints now cut a consistent snapshot by copying the keydir
    while storage briefly holds its applies. Before, a checkpoint was discarded
    whenever a write landed during its scan, so it never succeeded under
    sustained writes.
  - A finished checkpoint adopts itself in the background: it is renamed into
    place as the projection and the keydir rows below its position are
    repointed at it. A trim then only drops the log behind it, so the work left
    in the Raft commit path (value pins and apply-projection compaction) no
    longer grows with the number of keys.
  - Without a usable checkpoint a trim is deferred (the log keeps its entries)
    and asks for one. A trim may also adopt a checkpoint that covers only part
    of the request. Past
    `:waraft_segment_projection_trim_max_deferred_entries` entries behind the
    log floor (default 1,000,000; 0 never defers) a trim rebuilds the
    projection synchronously as before.
  - A checkpoint fills its location table from the verify pass of its own
    write instead of reading its projection back a third time.
  - Checkpoints that find every node-wide slot taken get one in the order they
    asked, so no shard is starved. The number of slots is
    `:waraft_segment_projection_checkpoint_concurrency` (default: a quarter of
    the schedulers, at least 1).
  - In a 256-writer unique-key run on 4 shards that reached 4.3M keys, the worst
    write pause dropped from more than 10 s to under 0.4 s, with no timeouts.
- Fix lost writes during WARaft log trims. Repointing a keydir row at the
  projection read the row and wrote it back in two steps while storage kept
  applying writes, so a write to the same key in between was overwritten until
  restart. The compare and the replace are now one atomic step.
- Warm cold reads of WARaft-backed keys. Warming built a match spec that ETS
  rejects for WARaft locations, so these keys were read from disk every time.

## 0.12.0 - 2026-10-16

- Upgrading from 0.11.x: upgrade nodes one at a time. Existing data
  directories stay in data format 1 and keep working. A new, empty data
  directory is created in format 2 unless `config :ferricstore, :data_format, 1`
  is set, and a node in another format cannot join a cluster, so set it on
  nodes you add to an existing format 1 cluster. To move a cluster to format 2,
  export it and restore it into a new cluster (see "Moving Data to Format 2" in
  the deployment guide). Once every node runs 0.12.0, raise the cluster feature
  level to 1 to turn on feature-gated behavior.
- Update the WARaft dependency (`ferricstore_waraft`) to 0.1.1. A replica now
  grants a vote only after its term and vote are durably on disk (fsynced
  before the rename, and the directory after it), so a crash cannot let it
  vote twice in one term; a disabled replica keeps retrying to persist its
  disable reason instead of coming back enabled after a restart; and a leader's
  commit batch deadline survives heartbeat rescheduling.
- New data directories use 128-bit Flow key digests (22 characters instead of
  43), about 1.7 KB less disk and 360 B less keydir memory per flow. They also
  store each Flow record's sidecar (child groups, attributes, value refs,
  sequence numbers) as a compact Erlang term instead of JSON: about a third
  smaller and 3.5 times faster to decode, which takes about 9% off a claim's
  CPU. Their Raft log entries replace repeated atoms and the default apply
  context with short dictionary references: Flow log entries are about 40%
  smaller, which shrinks the in-memory log window, replication traffic and log
  disk. Existing
  data directories keep full digests (data format 1) and run unchanged; every
  member of a cluster must use the same data format.
- `FerricStore.export/2` and `FerricStore.restore/2` copy a store's keys (every
  type, with expiry) and its Flow data (runs
  with values and history, shared values, policies and governance state:
  circuits, budgets, limits, approvals, effects, ledgers) through a file into an
  empty store, writing the target's data format; this is how a format 1 cluster
  moves to format 2.
- Custom (embedded, non-Raft) instances now apply every write to a Bloom or
  cuckoo filter, count-min sketch or top-k. Every write after the first on a key
  reused the first one's mutation token and was answered from its receipt
  without being applied, and alternating BF.ADD and BF.MADD paused the shard's
  writes. The default (Raft) instance was not affected.
- A shard whose Flow LMDB flush failed leaves the degraded state on its own once
  a retry succeeds and a reconcile finds it healthy. Before, it stayed degraded
  (and refused Flow queries and history reads) until a doctor repair, restart
  or FLUSHDB.
- A corrupt Flow policy row no longer stops a node from starting. Startup
  recovery logs it, emits `[:ferricstore, :flow, :lmdb_rebuild,
  :corrupt_policy_skipped]` and drops its LMDB copy; reads and commands for that
  type still fail with `ERR flow policy is corrupt`.
- Add cluster feature levels. New replicated behavior stays off until
  `FerricStore.raise_cluster_feature_level/1` raises the level after a rolling
  upgrade; the raise requires every member to support the level, levels never
  go down, and nodes below the active level cannot join. Level 1 is the first
  level; see the deployment guide. `CLUSTER.FEATURES` and
  `CLUSTER.FEATURES.RAISE <level>` (native opcodes 0x0311 and 0x0312) do the
  same from a client.
- At feature level 1, `flow_policy_set(type, replace: true, ...)` repairs a corrupt
  Flow policy row (unreadable or stored under another type) on every shard,
  at a generation above any the type used before.
- Node-local locks (standalone cross-shard transaction journal, segment and
  index markers, Flow promotion, segment projection) retry a busy lock within
  1-8 ms instead of `:global`'s random 0.25-8 s backoff. 48 concurrent journal
  operations took 1.1-2.3 s against 0.25 s run one by one; they now take about
  0.26-0.29 s.
- LFU counters now increment like Redis's `LFULogIncr`: the odds shrink with the
  counter above its initial value of 5, so the first read of a new key always
  counts. Previously a new key needed about 51 counted reads (about 5,100 reads
  at the default 1-in-100 read sampling) to rank above keys never read, so
  eviction could not tell moderately read keys from unread ones. `OBJECT FREQ`
  values grow faster as a result.
- Fix a Flow LMDB writer stall. When a Flow due more than the hibernation hot
  window ahead was parked and then changed state (completed, cancelled, fired)
  before the writer flushed, the batch compared the Flow's active-index reverse
  row against LMDB instead of the value the batch had just written. Every retry
  failed with `compare_failed`, so that shard's Flow LMDB projection stopped
  advancing (degraded mirror; schedule lists and Flow queries reported the
  catalog unavailable) until restart.
- A failed Flow LMDB clear (FLUSHDB) now names the step that failed (opening
  the environment, beginning the write transaction, clearing, or committing)
  instead of only the OS error, such as `Invalid argument (os error 22)`.
- One Flow whose projection cannot be written because of its own data (an
  undecodable record, a corrupt or conflicting LMDB row of its own) no longer
  stops every later Flow LMDB write on its shard. The writer narrows the failed
  batch down to that Flow, holds its projection and its hot-record cleanup, and
  writes everything else. The held projection is retried with backoff and
  released once it writes, for example after the record is repaired or
  deleted. While anything is held the shard's mirror stays degraded (queries
  that need a complete mirror still refuse), the replay-safe index does not
  advance, and LMDB writer flushes return
  `{:error, {:flow_projection_quarantined, count}}`. Failures that would hit
  every Flow alike (I/O, a full or busy LMDB, a source not yet readable) still
  hold the whole batch as before.

## 0.11.24 - 2026-10-06

- Update Mint to 1.10.2 to address the newly published HTTP response-framing and
  HTTP/2 memory-limit advisories, and retain strict native checks on Rust 1.99.
- Retire temporary WARaft rewrite offsets and metadata when directories are
  removed or replaced, and periodically reclaim indexes for already-abandoned
  rewrite directories. Protect live writers, preserve rollback sidecar trust,
  and invalidate stale readers without deleting authoritative records.
- Fence promoted reads across durable publication, failed mutations, transactions
  and restart. Use one atomic WARaft operation for eligible single-field HSETs
  with exact insertion counts, and bound explicit HSET append groups. Automatic
  synchronous coalescing is opt-in and remains disabled by default.
- Release the shared LMDB flush permit while waiting for missing versioned Flow
  sources, bound reconciliation pages to query admission limits, and coalesce
  handled history requests while retaining sync-before-watermark barriers.
- Preserve immutable value locators across read-side LFU/cache updates during
  compaction, and remove old log files in durably synced numeric order so partial
  cleanup cannot resurrect deleted fields.
- Clean up orphan authentication, asynchronous await, LMDB permit and blocking
  list waiters on caller death/restart. Preserve FIFO list wake-up chains and
  absolute deadlines, and charge retained native fragment/frame metadata to
  inbound admission.
- Respect validated startup storage-call budgets, release prepared storage when
  its caller exits, and omit only LMDB's runtime coordination lock from durable
  snapshot payloads. Keep data files and recovery validation intact.
- Reduce unrelated file-server queueing with raw local metadata reads, bound
  heartbeat term folds at the durable log tail, and buffer CRC-verified sparse
  offset fallback scans. Recover lost WAL memory-boundary metadata without
  shared file-server directory queueing, preserving ordinal/type/CRC checks.
  Retain workload-specific latency tradeoffs and existing
  durability policy; this release does not claim to resolve every long snapshot
  handoff timeout.

## 0.11.23 - 2026-09-27

- Shorten crash restart by buffering validated Flow-history tail scans only at
  startup, preopening independent Raft storage shards within memory-aware
  limits, and admitting up to three concurrent startup LMDB rebuilds. Normal
  LMDB flush admission returns to one worker after startup.
- Batch recovery-only Flow value-pin writes and shared-ref cleanup index inserts;
  reuse only page-local, exactly matching hot owner records. WAL frames,
  references, and derived history boundaries remain validated.
- Prove empty cold-due windows during WAL replay with bounded LMDB reads and
  transaction checks, falling back to the original scan on uncertainty.
  Serialize and validate background projection checkpoints without treating
  them as a crash-recovery shortcut.

## 0.11.22 - 2026-09-24

- Bound WARaft offset memory with derived, validated disk indexes and bounded
  read-ahead; replay Flow history after a verified durable checkpoint and fold
  only the segment-log tail following a validated projection when possible.
- Prevent orphaned or unrelated cold-due rows from producing empty replicated
  schedule claims. Repeated unchanged schedule overlaps back off to 30 seconds,
  preserve the queued due occurrence, and wake early on target completion.
- Keep the legacy policy-catalog projection warning visible while backing off
  repeated unsuccessful shard retries, and arm Bitcask shard drain timers only
  when direct writes actually remain pending.

## 0.11.21 - 2026-09-22

- Hardened standalone transaction recovery around partial or corrupt journals,
  append and fsync failures, and restart ordering. Ambiguous recovery now fails
  closed and retains a durable write fence until recovery completes.
- Serialized standalone shard commits through one FIFO sequence across direct
  and conditional write paths, preserving conflicting-write order while
  allowing unrelated keys to continue. Cross-shard compensation failures remain
  fenced until the owning recovery completes.

## 0.11.20 - 2026-09-20

- Harden Flow value-pin trimming after the runtime incident: stale relocated
  pins are removed per entry with atomic current-locator guards, preserving
  unrelated entries in the same pin batch. True missing value sources and
  malformed locators remain fail-closed. No corruption or data loss was
  demonstrated, and the warning should not be treated as the cause of every
  timeout.

## 0.11.19 - 2026-09-19

- Schedule resolved nonterminal child joins at their resolution time so parents
  can be claimed again, including custom successor states and cross-shard joins.
  Terminal successors remain outside the due index; retry attempts are unchanged.
- Preserve full record and job-map responses when native compact claim codecs
  are negotiated. Explicit compact job responses retain their existing format
  and fast path; full records no longer lose metadata or selected values.
- Accept `FLOW.REWIND REASON` through the generic command parser and persist the
  supplied reason with its history reference, including large binary reasons.
  Unsupported external `REASON_REF` inputs remain rejected.
- Existing parents already stalled with a missing due time are not automatically
  repaired. This patch does not add a startup scan or modify historical records.
- Route compound-key expiry through the logical parent shard and expire opaque
  fetch-or-compute outcomes on their supplied owner shard. Stale snapshots no
  longer reject unrelated expiry entries; renewed generations remain protected.
- Retry bounded Flow projection batches when their source changes during a
  durability read, including newer records and deletion markers. Missing current
  sources and exhausted retries still fail closed instead of reporting healthy.
- Compare terminal keydir rows exactly before pruning, preserving concurrent
  replacements and accounting only for the bytes actually removed.
- Serialize terminal and hibernation hot-index cleanup with the existing
  per-shard publication epoch. Disk reads remain outside the critical section,
  and delayed cleanup rechecks its source before deleting rows or indexes.
- Match delayed terminal cleanup to the Flow incarnation, not just its version,
  and require source-based cleanup to match its durable query projection. Reusing
  an expired Flow ID no longer lets old cleanup remove the replacement.
- Ignore read-side LFU counter changes during validated terminal and hibernation
  cleanup while retaining all source identity checks, so reads cannot prevent
  eligible hot records and indexes from being removed.

## 0.11.18 - 2026-09-13

- Hardened restart recovery for active Bitcask and blob segments: repair only
  unambiguous incomplete tails, preserve corrupt or ambiguous data, reject torn
  sealed segments, and invalidate stale append offsets after writer failure.
- Reconciled durable Flow state and history deletions with their derived LMDB
  indexes, rebuilt policy mirrors after projection failure, and recovered
  promoted collection metadata from WARaft-backed locations.
- Removed per-append global-term replacement from the Raft configuration miss
  cache and skipped provably empty scheduler claims without Raft writes. The
  empty check is bounded, single-node only, and falls back on uncertain state.
- Replaced recurring disk-capacity subprocesses with native sampling and avoided
  unnecessary host-memory detection when explicit limits are configured. Memory
  protection thresholds, polling intervals, and query admission are unchanged.
- Isolated development worktrees with checkout-local storage and OS-assigned
  listener ports; local launchers now print their actual endpoints.
- No client wire-format or SDK API changes are required for this patch.

## 0.11.17 - 2026-09-12

- Fixed WARaft segment recovery so replayed Flow commands cannot observe LMDB,
  keydir, value, retention, or hibernation state written by the same or a later
  Raft command. This preserves strict log ordering when the latest QueryRow is
  expired or its payload has already been compacted.
- Kept live reads unchanged, filtered recovery metadata before hydration or
  disk I/O, and used compact QueryRow references for recovery presence checks.
- Classified Flow query admission saturation as retryable busy work with a
  bounded retry hint, allowing clients to back off without raising concurrency
  or memory limits.

## 0.11.16 - 2026-09-12

- Fixed WARaft restart recovery when a live Flow QueryRow still points at a
  stale physical frame for a later version while an earlier command is being
  replayed. Recovery now compare-and-swap repairs the locator and retries the
  authoritative read within the existing deadline and memory bounds.
- Kept ordinary query reads unchanged, limited repair work for partial misses
  to only the missing rows, preserved validated expired-row recovery, and kept
  unresolved live rows fail-closed.

## 0.11.15 - 2026-09-11

- Redesigned the OSS dashboard around operational workflow investigation,
  including scoped query controls, projections and exports, schedule and
  governance actions, lineage, recovery evidence, freshness indicators, and
  protected-mode ACL enforcement.
- Fixed restart recovery for expired Flow QueryRows whose authoritative WAL
  source has already retired, while keeping live rows fail-closed, preserving
  scoped retention identity, and serializing startup reconciliation through the
  per-shard LMDB permit.
- Made apply-projection compaction use one expiry cutoff across retention and
  relocation, and made every shard and WARaft startup caller fail closed when
  LMDB reconciliation cannot complete.
- Hardened workflow mutation, schedule, circuit, stream waiter, Pub/Sub, and
  observability paths used by the dashboard, with expanded integration and
  browser-level regression coverage.
- Corrected retention-sweeper timing and compaction-start failure handling so
  short terminal TTLs remain deterministic and cleanup retries stay bounded.
- Serialized promoted compound routing through durable publication, rechecked
  deferred candidates against the exact indexed cardinality, and made exact
  prefix, batch, and collection deletion retire promotion metadata and storage.
  Contended post-commit retirement now defers through the bounded retry queue
  instead of blocking or terminating the shard.

## 0.11.14 - 2026-08-29

- Made the core Hex package dependency graph publishable while retaining
  `libcluster` 3.5.0 by moving the upstream Consul adapter into a release-only
  umbrella support application.
- Corrected Consul and etcd discovery to use each adapter's actual strategy
  module and option contract, including healthy Consul service lookup and
  normalized etcd endpoints and key directories.
- Added a required CI gate that builds and unpacks the core Hex package before
  a release tag can be merged.

## 0.11.13 - 2026-08-28

- Upgraded the supported runtime baseline to Elixir 1.20.4, Erlang/OTP 29.0.5,
  and Rust 1.98.0 across local tooling, CI, release images, and deployment
  examples.
- Updated Hex and Cargo dependencies, including Rustler 0.38 and
  RustlerPrecompiled 0.9, while retaining strict compiler, security, and
  allocation gates.
- Fixed strict-compiler findings and cross-test runtime-state restoration
  without weakening correctness assertions, performance thresholds, retry
  budgets, or timeouts.

## 0.11.12 - 2026-08-26

- Accelerated eligible homogeneous command-gateway GET batches and ordered
  same-shard SET batches while retaining ACL, internal-key, resource-limit,
  response-limit, deadline, and positional error semantics.
- Reduced FerricFlow query and write-path allocation and LMDB lookup overhead
  without allowing read-side storage creation.
- Added Java SDK compatibility coverage for native TCP/TLS and authenticated
  HTTP/HTTPS on Java 17 and Java 21.

## 0.11.11 - 2026-08-23

- Added the HTTP/HTTPS command and invocation application to the OSS umbrella
  and release, disabled by default and backed directly by the shared
  authentication and command gateways.
- Added TLS 1.2/1.3 listener configuration, bounded protocol controls,
  invocation target protections, architecture enforcement, real ACL gateway
  tests, and Python SDK TLS integration coverage.

## 0.11.10 - 2026-08-23

- Added caller-owned, deadline-bounded blocking list and Stream execution to
  the transport-neutral command gateway. Ordered pipelines may contain
  blocking commands, while independent requests are never coalesced across a
  blocking wait; cancellation releases waiters and resource-budget leases.
- Reconciled transient WARaft participant/member configuration timeouts during
  node joins instead of rolling back already-converged shards, preventing
  partial three-node membership after a slow cluster start.
- Contained malformed and invalid UTF-8 gateway command names as typed request
  errors before command planning or execution.

## 0.11.9 - 2026-08-22

- Added structured native descriptors to the transport-neutral command gateway
  so in-process HTTP transports can execute the complete stateless Flow command
  surface through canonical parsing, ACL, routing, deadline, and resource paths.
- Validated the HTTP-enabled Go, Python, Elixir, and TypeScript SDK release
  lines while retaining FerricStore 0.11.4 as the native compatibility floor.

## 0.11.8 - 2026-08-22

- Preserved durable terminal QueryRows when late query-only projection work
  observes a hot source that was intentionally pruned after projection. The
  atomic terminal reverse marker now distinguishes retained terminal state
  from a real deletion, while normal source-deletion cleanup remains intact.
- Made fixed-index FQL terminal reads synchronize pending cold projection work
  before answering, eliminating read-after-write omissions across completed,
  failed, and cancelled records, including equal-timestamp transitions.

## 0.11.7 - 2026-08-22

- Added transport-neutral authentication and command gateways so trusted
  in-process protocol adapters can reuse the same ACL, command preparation,
  resource-budget, deadline, and ordered batch-execution contracts as the
  native TCP server without a loopback network hop.
- Revalidated prepared command batches immediately before execution, retained
  bounded resource accounting across shared gateway calls, and added direct
  gateway coverage for structured native commands and ACL-sensitive keys.
- Allowed structured `FLOW.QUERY` requests through the shared gateway while
  preserving command discovery, authorization, bounded planning, diagnostics,
  and native execution semantics for HTTP and future transports.

## 0.11.6 - 2026-08-19

- Added a single-task AWS ECS/Fargate OSS deployment profile with private
  networking, a native TCP load balancer, encrypted task-local ephemeral
  storage, isolated readiness checks, serialized task replacement, CloudWatch
  Logs, ECS Exec, and
  explicit data-loss and security guidance. The profile has no S3, DynamoDB,
  EFS, EBS, clustering, or autoscaling dependencies.
- Added a three-node AWS ECS/Fargate cluster profile that emulates stable
  StatefulSet slots with three desired-count-one services, Cloud Map identities,
  fixed and private Erlang distribution ports, periodic DNS reconnects,
  three-AZ placement, task-local Raft replicas, liveness-safe ECS health, and a
  one-node-at-a-time rollout guarded by local recovery position. A blank-disk
  replacement integration test covers task IP/disk replacement. The support
  contract permits one replica loss and explicitly rejects simultaneous
  multi-replica replacement without an external durable recovery source.
- Release node identity now selects long-name Erlang distribution for Fargate
  DNS names and short-name distribution for single-label Docker/Compose hosts,
  preserving cluster discovery compatibility in both environments.

## 0.11.5 - 2026-08-03

- Added compact native Pub/Sub publish pipeline mode 35 and negotiated
  `pubsub_batch_v1` delivery. Eligible homogeneous publish batches reuse
  exact-channel routing, bounded pattern matches, subscriber admission, and
  shared response encoding while preserving publish order, per-item subscriber
  counts, ACL/resource policy, response-size limits, and the legacy one-event
  framing for clients that do not negotiate batching.
- Hardened Pub/Sub activity-log generation resets, capability renegotiation for
  existing subscriptions, bounded outbound admission, slow-consumer eviction,
  and fast-consumer isolation. The retained fanout-8 TCP gate measured 413,811
  publishes/s with one publisher and 430,637 publishes/s with four publishers,
  corresponding to 3.31M and 3.45M validated deliveries/s.
- Made cold Flow reads and derived-state flushes isolation-safe, and saturated
  terminal schedule-retention deadlines at the maximum exact timestamp so
  valid high timestamps cannot corrupt the durable history log.

- Added deterministic atomic Stream append plans, same-stream and grouped
  multi-topic `XADD_MANY`, compact native producer mode 34, post-WAL bulk
  catalog/cache publication, and reproducible single-topic, partitioned,
  same-shard multi-topic, and concurrent TCP benchmarks. The optimized path
  retains Redis Stream ordering/type semantics and publishes no derived state
  before the durable Raft projection succeeds.
- Reduced Stream fixed costs across both write and range paths: grouped topics
  stage their validated projections in one pending-write update; new topics
  validate an already-observed type marker once; physical Stream keys are
  escaped once per plan; idle legacy-index and blocking-waiter tables are
  skipped; and range catalogs combine ordered traversal with lookup using
  prepared physical prefixes. The mixed-topic producer now routes and groups in
  one pass, native compact Stream producers use that same path, generated IDs
  share the owning durable-key binary, and exact common range rows are assembled
  directly. Paired local gates improved 64-topic staging by about 27%, reduced a
  64-entry append plan's median by about 25%, and improved `XRANGE COUNT 10`
  request throughput by about 80% from the initial baseline while retaining the
  same WAL, persisted ETF, reply order, promotion fallback, and cache publication
  contracts.

## 0.11.4 - 2026-07-28

- Added the complete durable schedule recurrence contract, including canonical
  creation time, interval period, cron expression, timezone, overlap policy,
  and optional queue-after-previous retry configuration in create, get, list,
  fire, and scheduler responses.
- Made schedule catalog maintenance and due execution bounded, restart-safe,
  and deterministic while preserving interval catch-up, overlap, and terminal
  lifecycle correctness.
- Added protected-mode HTTP bootstrap, login, signed revocable sessions, and
  ACL-scoped account management to the OSS dashboard. Browser forms retain
  strict origin and CSRF validation, login throttling, audit records, and
  immediate session revocation after credential, state, rule, or user changes.
- Hardened prepared Flow command discovery and mutation metadata so parsing,
  ACL scope, routing, and apply-time footprints remain consistent without
  adding work to query or ordinary KV hot paths.

## 0.11.3 - 2026-07-27

- Centralized the compact FQL1 result quality vocabulary and made response
  emission fail closed for non-canonical cursor, quality, usage, count, or page
  relationships. Valid result bytes and the negotiated wire contract remain
  unchanged, with no added work on the query execution hot path.

## 0.11.2 - 2026-07-26

- Fixed `EXPLAIN ANALYZE` for clients that negotiate compact FQL results by
  reading execution usage directly from the already-validated prepared
  response without copying or re-encoding its payload.

## 0.11.1 - 2026-07-26

- Reduced native-order QueryRow page latency by reading composite entries and their metadata rows in one scope-bound LMDB snapshot. The operator keeps independent scan/value byte ceilings, conservative covered-ID and QueryRow-reference admission, exact memory accounting, a shared versioned compact-key contract, and a bounded two-stage fallback when a fused page cannot be reserved; final paired storage runs improved p50 by 6.0-48.5%, while the end-to-end 25-row query improved about 2% beyond the prepared-prefix path.
- Reduced QueryRow and compact-result codec work without changing persisted or wire bytes. QueryRows parse their state identity once, construct the final row once, and validate skipped metadata without materializing it; prepared query decode plans retain only dynamic fields used by indexes, filters, ordering, or projection. Negotiated compact results are encoded and sized once before native framing. Metadata-heavy 100-row selective decode improved about 50% with about 38% less allocation, and exact-field decode remained about 19% faster with about 11% less allocation than materializing the full attribute section.
- Kept failed local transactional read-modify-write operations from touching LFU metadata and skipped the redundant LFU update before successful replacement writes.
- Made a history projector that restarts with unresolved Raft work rescan its durable fsynced log before serving reads, without adding recovery scans to normal startup or work to the query hot path.
- Retried compact query projection when a concurrent Raft update replaces its prepared source before durability, while continuing to fail closed when the live keydir still references a genuinely missing source frame.
- Batched page-bounded approval-catalog repair removals instead of issuing up to 128 single-member writes per step, and revalidated removed memberships so concurrent approval creation or a transient validation error cannot leave a valid record uncataloged.

## 0.11.0 - 2026-07-26

- Replaced the beta Flow LMDB full-record projection with one deterministic compact QueryRow per current state. QueryRows contain bounded query-visible metadata and a checked physical locator; the append-only WARaft or Bitcask log remains the only authoritative full record, and composite indexes never duplicate payloads or physical offsets.
- Added metadata-row and composite-covering execution for built-in fields plus bounded `attribute.*` and `state_meta.*` values. Eligible projections, counts, and bare run collection records perform no authoritative-log reads. Point, fixed, history, lineage, and internal payload-dependent paths retain byte-, memory-, concurrency-, and deadline-bounded grouped reads with identity, version, expiry, size, and checksum validation.
- Centralized physical relocation in QueryRow with compare-and-swap journal recovery, including live and snapshot catalogs, stale-locator repair, and one bounded retry for a compaction race. Apply-projection retention now spills large rewrite sets to a durable temporary catalog instead of retaining unbounded record values in memory.
- Replaced redundant projection discovery with an exact shared SourceCatalog and resumable rebuild/backfill paths. Projection writes publish locators only after the source frame is durable, and index migration, validation, retirement, expiry, restart, and pressure handling retain bounded replay-safe behavior.
- Expanded EXPLAIN and index status with projection coverage source, hydration requirements and estimated bytes, residual predicates, dynamic covering fields, codec formats, build/validation/retirement progress, and statistics freshness.
- Cut over the beta derived-state formats to catalog version `4`, built-in generation `2`, registry snapshot version `2`, and cold-park version `2`. There is no old-format decoder or dual write; operators must perform the documented offline derived-query rebuild while preserving authoritative WARaft/Bitcask logs.
- Added pooled vector reads for all WARaft record sources, strict caller deadlines while compaction owns the storage latch, corruption and resource ceilings at every codec/read boundary, and Linux io_uring parity coverage with the portable grouped-pread fallback.
- Added result-equivalence, restart, compaction-race, malformed-input, memory-pressure, soak, and storage-layout benchmarks. At 10,000 records the compact projection reduced physical LMDB bytes by about 23.0% for sparse, 35.5% for typical, and 6.0% for metadata-heavy records; covered 100-row projections measured about 5-10x faster than authoritative hydration; a paired 25-row state/time bare-page benchmark measured 1.43x higher p50 throughput than authoritative fallback; and spilled retention reduced peak memory by 62.9% in the 10,000-record fixture.

## 0.10.3 - 2026-07-24

- Added bounded, source-specific FQL1 result fields after `RETURN RECORD` and `RETURN RECORDS`, including whole or leaf metadata maps, sparse binary result rows, canonical cursor binding, and `plan.projection` EXPLAIN output. Projection is applied after authoritative validation and reduces result memory/encoding/transfer work without weakening scan, hydration, ACL, or budget checks.
- Added the explicitly negotiated `flow_query_result_v1` native response codec for `FLOW.QUERY` record pages and counts, using fixed metadata, row-presence bitmaps, and typed field values while retaining lossless typed-value fallback for EXPLAIN, errors, and unknown result shapes. Clients name supported compact codecs in `HELLO` or `STARTUP`, preventing broad compact-response opt-in from enabling unknown future tags.
- Removed the full-record intermediate from explicit fixed-index projections after a benchmark showed about 4x higher projection throughput and 2.9x lower allocation, while retaining authoritative ordering, cursor, integrity, and memory-budget checks.
- Added a byte-for-byte compact-result golden corpus for server and SDK conformance, made the native NIF ABI derive the release version from the umbrella project, and allowed clients to ignore well-formed unknown future response codecs without weakening duplicate-opcode validation.
- Kept native responses uncompressed when an established connection state omits optional compact-codec negotiation, while retaining fail-closed selection for malformed or unsupported codec declarations.
- Made completed query-index validation restart-safe before atomic activation, allowing the lifecycle worker to resume a durably validated build without exposing it early or rejecting the registry snapshot.

## 0.10.2 - 2026-07-23

- Made the native topology and event-subscription control commands individually grantable through ACLs and the connection category, enabling least-privilege topology-aware SDK sessions without broad command permissions.

## 0.10.1 - 2026-07-23

- Fixed the standalone container build to package the OSS query-index catalog required during startup.
- Fixed LMDB reconciliation to preserve valid cold terminal projections, remove stale reverse rows, rebuild exact terminal counts in bounded pages, and serialize writers with reconciliation per shard without blocking independent shards.
- Added native-port startup smoke tests for every CI image and each release architecture before publishing a multi-arch manifest.

## 0.10.0 - 2026-07-23

- Added the complete cost-aware `FLOW.QUERY` planner, composite index lifecycle, bounded executor, statistics, actionable diagnostics, index management, and `EXPLAIN`/`EXPLAIN ANALYZE` UX to OSS; Enterprise consumes the same implementation and contracts.
- Removed the beta wire commands `FLOW.LIST`, `FLOW.SEARCH`, `FLOW.TERMINALS`, `FLOW.FAILURES`, `FLOW.STUCK`, `FLOW.BY_PARENT`, `FLOW.BY_ROOT`, and `FLOW.BY_CORRELATION`; clients must use `FLOW.QUERY` and negotiate its advertised shapes.
- Unified point, history, lineage, fixed-index, composite, and count plans under `ferric.flow.query.result/v1` and `ferric.flow.explain/v1`, with one edition-neutral capability manifest containing every executable shape.
- Added memory-weighted query admission, pressure-aware index backfill, aggregate native merge limits, batched selected-plan statistics probes, exact one-pass response accounting, and resumable bounded-prefix LMDB hydration.
- Bounded fixed-index fallback candidate hydration to one maximum result page plus look-ahead, and kept namespace quota accounting distinct from Flow secondary indexes.
- Isolated query-only LMDB projection from scheduler and terminal lifecycle indexes, and bound MemoryGuard pressure publication to the supervised default guard so auxiliary guards cannot alter production admission state.
- Added lossless unsigned 64-bit native values for query catalog versions, registry epochs, and lifecycle counters while retaining the existing signed integer tag for ordinary command values.
- Added the `ferric.flow.query.indexes/v1` index-status contract to query capability negotiation so clients reject incompatible management responses during `HELLO` instead of after a query.

## 0.9.1 - 2026-07-19

- Advertised and accepted `expected_generation` and `replace` in typed-native `FLOW.POLICY.SET`, aligning native SDK policy CAS and replacement with the embedded and textual command paths.

## 0.9.0 - 2026-07-19

- Added durable per-state FIFO/parallel Flow execution, with exact partition-lane ordering across hibernation and restart, plus `mode :fifo | :parallel` declarations in the Elixir workflow DSL.
- Made direct Flow policy updates merge atomically by default with replicated generation compare-and-swap; set/get responses now expose `generation`, while workflow `install_policy/1` replaces its declaration snapshot by default.
- Changed workflow-derived composite partition keys to collision-free length-prefixed encoding. Explicit partition keys are unchanged.
- Fixed LMDB startup recovery to fall back to synchronous keydir reconciliation when an active Flow projection is invalid, and to rebuild the durable active projection before serving claims.

## 0.8.0 - 2026-07-18

- Default governance effect lookups to the Flow ID auto-partition when no explicit partition is provided.
- Added type-level and per-Flow `max_active_ms` limits with durable timeout failure history, cold-record enforcement, and parent/child coordination.
- Exposed maximum active runtime through embedded, native TCP, workflow, and dashboard policy surfaces, including an `infinity` override.
- Made Flow timeout sweeping instance-scoped and surfaced active timeout candidates and counts in retention maintenance views.
- Hardened native and health-server request handling with bounded decode/output work, HTTP framing and deadlines, CSRF/session checks, login throttling, and dashboard ACL redaction.
- Reserved internal Flow storage keys across public command, pipeline, transaction, embedded, and dashboard paths, and made shared-reference/retention backfills bounded, resumable, and fail-closed across destructive resets.
- Made Flow policy fan-out failure-atomic with bounded deterministic hot-record reindexing, repaired WARaft cold-row compensation, and routed ACL mutations through replicated invalidating paths with bounded protected-mode and load checks.
- Added monotonic internal Flow policy generations and command-captured policy snapshots so policy-sensitive work remains deterministic across independently ordered Raft groups without replacing public policy versions.
- Replaced synchronous Flow policy reindex scans with a durable exact-type catalog, bounded resumable catalog backfill, and replicated migration plans whose explicit candidates are safe to replay on every replica.
- Added a shared prepared-command contract for parsing, ACL keys, routing keys, and read/write footprints, and moved Flow apply limits into a compact replicated context persisted with WARaft recovery metadata.
- Canonicalized the beta-only Flow command, projection outbox, and dirty-marker contracts and removed obsolete compatibility paths.
- Fenced direct LMDB enqueues, outbox rows, and reconciliation markers by writer generation so destructive resets and snapshots cannot publish stale projection work.
- Made WARaft projection recovery authoritative, fenced client writes during restart recovery, hardened promotion recovery and compaction resource release, and prevented deleted projected values from being resurrected.
- Made expiry, scheduling, history, replay, and maintenance decisions deterministic from replicated command time and HLC state.
- Bounded batch, transaction, frame, response, allocation, fan-out, reindex, and cleanup work while preserving targeted hot paths and projection batching.
- Hardened ACL, protected-mode, TLS, path, corruption, admission, rate-governance, and global-concurrency enforcement to fail closed.

## 0.7.5 - 2026-07-08

- Added dashboard views for Flow details, capabilities, messaging, streams, Pub/Sub, and security state.
- Added bounded stream and Pub/Sub activity logs with ACL-filtered dashboard live payloads.
- Expanded health endpoint routing, dashboard access checks, and observability coverage.

## 0.7.4 - 2026-07-07

- Fixed lagged LMDB terminal Flow cleanup so stale cold keydir rows are pruned only after durable projection while startup rebuild remains non-destructive.
- Preserved LFU Flow state version wrappers during hotness updates for cold Flow rows.
- Added FIFO Flow coverage for partition-wide claims, explicit partition lists, batched claims, expired lease reclaim, state transitions, restart replay ordering, RESP command dispatch, and native opcode execution.

## 0.7.3 - 2026-07-07

- Added native `FLOW.POLICY.SET` support for indexed Flow attributes.
- Added native and `COMMAND_EXEC` coverage for `FLOW.SEARCH` attribute and state metadata queries.
- Rejected state-scoped indexed Flow policy options that are type-level only.
- Updated native protocol and command guides for newer Flow opcodes and `FLOW.SEARCH`.

## 0.7.2 - 2026-07-06

- Removed the embedded Elixir SDK app from this repository; the Elixir SDK now lives in the standalone SDK repository.
- Added `FLOW.SEARCH` support to the native `COMMAND_EXEC` parser and dispatcher.
- Hardened namespace-scope enforcement for `FLOW.SEARCH` over raw and typed native commands.

## 0.7.1 - 2026-07-05

- Added native `FLOW.SEARCH` support for indexed per-state metadata queries.
- Exposed Flow `state_meta`, `indexed_state_meta`, and `FLOW.SEARCH` through the embedded Elixir SDK.
- Added SDK and server coverage for state metadata indexing/search and HA topology refresh after node restart/rejoin.

## 0.7.0 - 2026-07-04

- Added HA-aware native route metadata for `HELLO`, `ROUTE`, `ROUTE_BATCH`, and `SHARDS`, including advertised native endpoints and leader hints.
- Added the embedded Elixir native SDK with topology-aware routing, keyed KV helpers, Flow helpers, and admin/governance helpers.
- Hardened SDK rerouting so automatic replay is limited to connection-open/send failures and explicit native `REROUTE`; post-send close/timeout results are surfaced to callers as unknown outcomes.
- Added SDK durability coverage for routed leader loss, no-quorum failures, and topology refresh recovery.
- Exposed state metadata in the governance dashboard and Flow detail views using bounded indexed `state_meta` queries.
- Hardened native route/governance ACL checks and expanded CI coverage for SDK and durability routing paths.

## 0.6.0 - 2026-07-01

- Added bounded per-state Flow metadata with durable record/history encoding.
- Added one policy-controlled indexed state metadata key per Flow type for indexed search.
- Added automatic backfill and stale index cleanup when a Flow type changes its indexed state metadata key.
- Added native command support for `STATE_META` mutation options and `INDEXED_STATE_META` Flow policy configuration.
- Added LMDB projection, rebuild, and retention cleanup coverage for indexed state metadata rows.
- Exposed Flow policy and retention cleanup functions through embedded `use FerricStore` instances.

## 0.5.6 - 2026-06-29

- Added trusted native request context for extension command execution.
- Gated native `request_context` acceptance behind trusted native users so arbitrary clients cannot spoof control-plane authority.
- Propagated trusted request context from typed pipelines into nested `COMMAND_EXEC` extension commands.
- Added `FERRICSTORE_NATIVE_TRUSTED_REQUEST_CONTEXT_USERS` for production configuration.

## 0.5.4 - 2026-06-27

- Added the command extension interface for optional command providers.
- Exposed extension commands through native command execution, catalog metadata, and ACL categories.
- Hardened built-in command precedence so extension metadata cannot shadow core routing, key metadata, or ACL access classes.
- Stabilized embedded ACL regression tests by explicitly waiting for the shared default write path.

## 0.5.3 - 2026-06-23

- Updated public docs for the Ferric native TCP protocol architecture after RESP removal.
- Removed stale Redis/RESP protocol wording from public docs and dashboard copy.
- Replaced obsolete RESP benchmark helpers with native-protocol benchmark guidance.
- Fixed `INFO server` fields to report FerricStore/native protocol names without the legacy `tcp_port` fallback.
- Aligned the shard active-file fallback default with the 8 GiB runtime default.
