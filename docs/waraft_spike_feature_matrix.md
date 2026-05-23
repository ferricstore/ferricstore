# WARaft Spike Feature Matrix

This spike is only allowed to inform a migration decision. It must not replace
the current Ra path until both performance and feature contracts are proven.

## Required Features

| Feature | WARaft Status | FerricStore Work Needed | Migration Risk |
| --- | --- | --- | --- |
| Custom segmented WAL | Supported by `log_module` in the partition spec and `wa_raft_log` callbacks. Spike provider `ferricstore_waraft_spike_segment_log` now proves CRC-framed grouped append, datasync-before-ack, keep-size preallocation hooks, recovery scan, persisted trim/rotation, crash-atomic trim rewrite, corruption telemetry, append/fsync telemetry, restart, multi-partition commit, and 3-node commit. | Replace the separate WAL + Bitcask payload files with the final unified segment format, then add byte backpressure and production tuning on real Azure runs. | High |
| Snapshot install | Supported through storage snapshot callbacks and transport snapshot delivery. Spike test installs a snapshot into a stalled member and verifies state is readable. Backend tests now copy real Bitcask/blob/prob payload dirs, tolerate only payload dirs recorded as empty at snapshot creation, reject missing non-empty payload dirs, and cover partial install recovery. Segment-backed WARaft compound data is carried by the WARaft segment/projection log, not promoted dedicated dirs. | Map this into FerricStore cluster commands and add kill-during-transfer/load chaos tests. | High |
| Membership changes | Supported by `wa_raft_server:adjust_membership/3,4` and participant/member/witness transitions. Backend tests now cover dynamic member removal and staged member add with participant config, snapshot transfer, readiness wait, promotion, and post-add writes. | Wrap with FerricStore cluster commands plus replace/reject rules for existing standalone data. | High |
| Backpressure | Supported by commit/apply/read queue limits and queue inspection helpers. Spike test verifies tagged async `commit_queue_full` replies. The FerricStore backend maps WARaft queue/full-byte rejections to the existing `:overloaded` write error and has an optional per-shard in-flight command byte gate with telemetry on rejection. Current per-shard in-flight bytes are exposed in `INFO raft`, `DEBUG BATCHER-STATS`, and Prometheus metrics. | Tune byte limits for large values before replacement. | Medium |
| Command correlation/replies | Supported by `wa_raft_acceptor:commit_async/4`; reply is sent after storage apply completes. Spike tests verify tagged replies and storage visibility after `:ok`. | Preserve FerricStore request ids, timeout/unknown handling, and protocol error mapping. | Medium |
| Local apply notification | Storage replies occur after apply. Spike proves reply-after-apply is enough for single-shard commands. | Decide whether multi-shard commands need an explicit all-shard local-apply aggregator. | Medium |
| Per-shard multi-Raft | Supported by `{table, partition}` specs and generated per-partition process names. Spike starts multiple partitions under one supervisor, verifies isolation, and has a multi-partition load benchmark. | Route FerricStore shards through one WARaft partition per shard and benchmark through the real RESP/router path. | Medium |
| Deterministic recovery | Proven for a lagging storage position with durable segment log: restart replays log entries after storage position. | Durable production state machine must recover exactly to last acknowledged position, including torn write and partial snapshot tests. | High |

## Current Spike Coverage

Covered:

- One-shard SET/GET adapter.
- Batched SET as one Raft command.
- Async pipelined replies.
- Reply after local storage apply.
- Restart semantic test using a toy persistent storage provider.
- Three local peer nodes with quorum commit.
- Three peer backend nodes committing through the real FerricStore state
  machine with the durable segment-log provider.
- Durable segmented log provider with restart and lagging-storage replay tests.
- Durable segmented log corruption emits telemetry and fails closed on CRC
  mismatch instead of silently replaying partial/corrupt records.
- WARaft storage metadata keeps a previous durable metadata file plus a framed
  append-only metadata journal. Startup can recover from torn/undecodable
  current and previous metadata terms by using the last valid journal record,
  while valid but semantically bad metadata still fails closed. Journal recovery
  streams framed records with `File.open/2` and `:file.read/2` instead of
  materializing the full journal in BEAM memory, and oversized journal records
  are rejected before payload read.
- Durable segmented log appends are tested across segment boundaries: if a
  multi-record append writes one segment file then fails on the next, recovery
  does not expose any part of the unacknowledged append.
- If rollback of a partially written split append itself fails, the segment log
  writes a durable `segment_log.append_failed.term` poison marker and fails
  closed on the next open instead of replaying bytes from an unacknowledged Raft
  append. The hot path only checks for the marker once per append batch, and
  restart size-checks the marker before reading it so a corrupt marker cannot
  allocate an unbounded metadata binary.
- Durable segmented log appends fsync the parent directory when the append
  creates a new segment file. Existing segment appends avoid an extra directory
  fsync and keep the hot path at file fsync only.
- Durable segmented log appends now have explicit file-sync test hooks. Tests
  prove the segment file fsync happens before append returns, failed file fsync
  rolls back unacknowledged bytes, and a same-segment multi-record append uses
  one file fsync for the group rather than one fsync per record.
- Durable segmented log appends use one production I/O path: Erlang
  `file:write` followed by `file:datasync` before the append is acknowledged.
  Earlier WAL-NIF and alternate writer experiments are no longer user-facing
  modes because the extra selectors made benchmarks and deploys ambiguous.
- Segment-log append atomicity invariant: a Raft entry is not inserted into the
  in-memory log table, applied to FerricStore storage, or acknowledged to the
  client until the segment append has written the bytes and the segment file
  sync has completed. A blocked-sync test keeps the fsync suspended and proves
  the write remains invisible and the caller remains waiting until the sync is
  released.
- New segment preallocation is wired through the WAL NIF's keep-size fallocate
  helper instead of Erlang `file:allocate/3`, because extending the logical file
  size would leave zero trailer bytes that break deterministic segment recovery.
  Tests cover successful preallocation, failure-before-append, and the keep-size
  Rust helper.
- Durable segmented log appends emit
  `[:ferricstore, :waraft, :segment_log, :append]` telemetry with record count,
  encoded bytes, duration, segment path, new-segment flag, and success/error
  reason. Tests cover both successful appends and fsync failures.
- The spike segment log now streams recovery with `file:read/2` instead of
  materializing whole segment files in BEAM memory, and tests guard that shape.
  Records per segment are configurable through
  `:waraft_segment_log_records_per_segment`, with the value read once per append
  group so the hot path avoids per-record config lookups. The chosen segment
  size is persisted in `segment_config.term` per log directory; changing the
  application setting later cannot make new appends go back into older segment
  ordinals and break deterministic replay. The marker is validated at log open,
  so corrupted segment sizing metadata, or a missing marker beside existing
  segment files, fails closed before traffic is served. Trim/truncate rewrites
  preserve the live log's pinned segment size when building staging files, so a
  later application config change cannot silently alter the log's layout during
  compaction. Those rewrites stream kept ETS records into staging one segment
  group at a time instead of materializing the full retained log in memory.
  Config lookup walks backward from the newest in-memory segment index entry instead of
  folding the full in-memory log, so membership/config reads avoid O(log length)
  CPU in the common case. Append grouping is linear over monotonic Raft batches
  and avoids map allocation plus sorting on the commit hot path; out-of-order
  groups fail closed instead of being silently reordered. Close erases the
  per-path segment-size cache, so path reuse in one BEAM does not leak or reuse
  stale sizing after a log is stopped.
- Segment-log trim rewrite stages records before swapping live files and uses a
  marker plus backup directory so in-process failures and restart with an
  interrupted marker preserve replayability for acknowledged writes.
- Segment-log trim rewrite treats backup cleanup and its parent-dir fsync as
  part of the rewrite contract. A failed cleanup sync returns an error instead
  of silently accepting uncertain cleanup durability.
- Snapshot install into a stalled member.
- Snapshot install now fails closed across final cleanup races. If snapshot
  metadata is already durable but the staging/backup cleanup or install-marker
  removal fails, the installed handle is returned blocked so no later write can
  advance over an ambiguous marker that restart recovery might otherwise roll
  back.
- Dynamic membership removal in a 3-node cluster.
- Staged dynamic member add in a 3-node cluster: add as participant, transfer a
  real storage snapshot, wait for peer readiness, promote to voting member, and
  verify new and old writes on all peers.
- Dynamic member add restart durability: a fourth member added through staged
  snapshot catch-up remains in membership and keeps pre/post-add writes after a
  full cluster restart.
- Dynamic member add covers blob-backed large values, so snapshot catch-up is
  not limited to inline Bitcask payloads.
- Backpressure rejection through tagged async replies.
- Two local WARaft partitions under one supervisor.
- Multi-partition load driver and benchmark script.
- Mixed GET/SET load driver using local applied storage reads for hot GET.
- Local restart recovery timing script for segment-log replay.
- Local benchmarks for one-node and three-node durable paths.
- Guard test for the WARaft APIs and callback hooks this spike depends on.

Current replacement-gate coverage:

- `Ferricstore.Raft.WARaftBackend` starts one WARaft partition per FerricStore
  shard behind a selectable backend boundary.
- The backend rejects WARaft's volatile ETS log. WARaft evaluation now has a
  single supported shape: the durable segment/keydir provider, so local
  benchmarks cannot accidentally measure a non-durable in-memory log.
- Raft backend selection fails closed for invalid config values. A typo like
  `:warft` now raises during backend selection instead of silently falling back
  to legacy Ra and invalidating replacement benchmarks.
- The backend runs the real FerricStore state machine and Bitcask apply path,
  not the toy spike map storage.
- Router can select WARaft for `SET`, `DEL`, generic command batches,
  `put_batch`, `delete_batch`, forced-quorum commands, and compound writes.
- Generic command batches flatten nested hot batch terms before state-machine
  dispatch. This keeps `{:batch, [{:put_batch, ...}]}` and similar shapes from
  crashing apply if parser/router coalescing changes batch boundaries.
- Router/command coverage now includes representative JSON, bitmap,
  native/rate-limit, Flow create/claim/transition, Flow create_many/
  transition_many, cross-shard Flow child fanout, file-backed Bloom/CMS/Cuckoo/
  TopK, TDigest, Stream XADD/XDEL/XTRIM restart durability, and stream
  consumer-group create/read-pending/ack restart durability.
- Default compound writes use `Ferricstore.Store.CompoundCommand` compact terms
  before entering WARaft. This avoids carrying Shard-only `redis_key` metadata
  through the default replicated write path and keeps the command shape compact.
- Hash command coverage proves field TTL and advanced read-modify-write
  mutations work through WARaft direct keydir/cold-file recovery, including
  `HSETEX`, `HPTTL`, `HMGET`, `HGETEX`, `HINCRBY`, `HINCRBYFLOAT`, `HSETNX`,
  `HGETDEL`, `HPERSIST`, `HEXPIRETIME`, and `HSCAN` after restart.
- List, Set, Geo, and sorted-set helper coverage proves WARaft direct keydir
  scans materialize recovered cold compound values without a Shard GenServer
  fallback. The representative restart matrix now covers list rewrites
  (`LSET`, `LINSERT`, `LREM`, `LTRIM`, `LPUSHX`, `RPUSHX`), set store/pop/scan
  paths (`SINTERSTORE`, `SDIFFSTORE`, `SPOP`, `SINTERCARD`, `SSCAN`), and
  sorted-set pop/range/scan paths (`ZPOPMIN`, `ZPOPMAX`, `ZRANGEBYSCORE`,
  `ZREVRANGEBYSCORE`, `ZMSCORE`, `ZSCAN`).
- The obsolete projected/dual-Bitcask shortcuts are rejected at startup.
  Replacement benchmarking uses the unified segment/keydir path: projectable
  commands install keydir locations pointing at the WARaft segment itself.
- Application boot always uses WARaft: legacy Ra batchers are not started,
  legacy Ra elections are skipped, and the WARaft backend starts behind the
  default instance.
- Ordered per-command replies are preserved for hot batch terms.
- Backend queue limits are configurable and tested to surface WARaft
  backpressure as FerricStore-compatible `{:error, :overloaded}` without
  applying the rejected write. Coverage includes both the synchronous single
  write path and the asynchronous `write_many/1` submission path used for
  multi-shard batches.
- Backend in-flight command byte limits are configurable and tested to reject
  oversized submissions before apply, emit telemetry, return the same
  `{:error, :overloaded}` contract, and release accounting after commit replies.
  Admission happens before blob side-channel preparation so an overloaded write
  does not create orphan blob bytes before being rejected. Async submit now
  wraps `commit_async` after byte admission; if WARaft rejects, returns an
  unexpected error, or exits before a reply is registered, the reservation is
  released and the result is normalized instead of crashing the caller with
  bytes still accounted as in flight. Timed-out async waits also unalias and
  drain any already-delivered reply for that exact ref, avoiding mailbox buildup
  during timeout storms.
- WARaft in-flight commit bytes are now visible through `DEBUG BATCHER-STATS`,
  `INFO raft`, and `ferricstore_waraft_inflight_commit_bytes`, giving operators
  a direct signal for byte-admission pressure before replacement.
- WARaft public API boundaries now fail closed for invalid shard indices instead
  of leaking `FunctionClauseError`/`:atomics` crashes. Negative indices are
  rejected before registered-name construction, and positive out-of-range
  indices are rejected inside the optional in-flight byte gate without adding a
  shard-count lookup to the normal write hot path.
- WARaft membership mutation boundaries reject unknown actions, malformed
  redirect counters, and malformed node atoms before config append or remote
  redirect work. This keeps cluster-management errors deterministic without
  adding checks to the commit hot path.
- WARaft batch write boundaries reject malformed `write_many`, generic batch,
  put-batch, and delete-batch payload shapes instead of crashing the caller.
  Valid batch entries still use the async submit path and preserve ordered
  replies.
- WARaft local read boundaries reject invalid shard indices and malformed keys
  explicitly, so routing bugs do not get hidden as ordinary cache misses.
- WARaft snapshot install boundaries reject malformed paths and Raft positions
  before calling WARaft internals, keeping cluster orchestration failures
  structured.
- WARaft public/control boundaries (`status`, `membership`, storage position,
  snapshot/election install, peer readiness, membership mutations, local reads,
  and bootstrap) return
  `{:error, :backend_unavailable}` when the WARaft processes are stopped
  instead of leaking `gen_statem` exits.
- WARaft startup bootstrap errors are returned through the normal failed-start
  cleanup path instead of using a bang match after backend context publication.
  A forced bootstrap storage failure now proves the backend context and
  partition processes are removed before callers can route traffic to a partial
  WARaft start.
- WARaft membership participant/member polling wraps storage config reads, so a
  storage process exit during add/promote waits is handled as a retry or
  timeout result instead of leaking an exit from cluster-management code.
- WARaft startup promotion wraps server calls and returns structured errors
  before waiting for leadership. A restart-time server exit now follows the
  same failed-start cleanup contract as bootstrap failures.
- WARaft snapshot transfer during add-member/add-participant wraps transport
  calls, so transfer-process exits surface as structured cluster-management
  errors instead of caller exits.
- WARaft startup status polling and storage durable-position polling wrap
  WARaft status calls. Transient server/storage exits now follow the bounded
  retry/timeout path instead of relying on raw catch blocks.
- WARaft transfer-leadership wraps `handover/2` and preserves the existing
  stopped-shard mapping to `ERR shard not available`.
- WARaft sync commits now mirror the async commit boundary: if the local
  acceptor exits after byte admission, the call is normalized into the existing
  unknown-outcome path, while an acceptor missing before submit remains a
  definite `ERR shard not available`.
- A real RESP/router benchmark gate now exists at
  `bench/waraft_resp_router_bench.exs`. It runs through the FerricStore server,
  current key hashing, Router, WARaft, and the segment/keydir apply path with
  selectable workload shape (`BENCH_MODE=set|get|mixed`) plus `CONCURRENCY`,
  `PIPELINE`, `DATA_SIZE`, `SHARDS`, and `KEY_COUNT`.
- Missing local WARaft acceptors are normalized to `ERR shard not available`
  instead of leaking WARaft's internal `:unreachable` atom.
- Multi-shard Router batches route each key to the matching WARaft partition and
  keep partition state isolated.
- Multi-shard Router batches now submit WARaft shard groups concurrently while
  preserving ordered replies. The single-shard path stays synchronous to avoid
  adding Task overhead to the common one-partition case.
- Router WARaft routing is now instance-scoped. Production default-instance
  traffic uses WARaft, and isolated WARaft backend tests can route through the
  exact context registered with `WARaftBackend.start/2`; ordinary custom
  embedded instances stay local/direct.
- Restart recovery reopens the durable storage position and rebuilds keydir
  state from the WARaft segment/projection checkpoint.
- WARaft storage now treats Bitcask/blob/projection apply failures as
  infrastructure failures: it returns the error but does not advance the local
  storage replay position, so restart recovery replays the committed entry.
  Deterministic command errors still advance the position to avoid replaying
  bad commands forever.
- WARaft commit normalization maps storage infrastructure failures to the
  existing `{:timeout, :unknown_outcome}` write contract. Those entries may
  already be committed in the Raft log and can materialize after restart, so
  client-facing APIs must not report them as definite "write failed" errors.
- WARaft commit normalization also distinguishes a missing local acceptor from
  a mid-commit disappearance. If no acceptor existed before submit, writes still
  return `"ERR shard not available"`; if the acceptor existed and disappears
  while the command is in flight, the backend returns
  `{:timeout, :unknown_outcome}` because the log append or apply outcome is now
  ambiguous.
- WARaft storage uses the unified "Raft WAL == Bitcask segment" path for
  projectable hot commands: keydir rows point at `{:waraft_segment, index}`,
  cold reads load from the WARaft segment, and trim writes a projection
  checkpoint before compacting away segment records. `status/1` reports both
  `:applied_position` and `:durable_position`, plus `:payload_dirty?`, so
  operators can distinguish volatile apply progress from replay-safe durable
  progress.
- Replay-safe no-sync metadata boundaries are tested: graceful close and
  membership/config metadata commits fsync dirty payload before publishing the
  newer storage metadata position. Frontier fsync emits
  `[:ferricstore, :waraft, :storage, :payload_fsync]` telemetry with shard,
  position, duration, result, and failure reason.
- Replay-safe no-sync avoids unnecessary payload frontier work for deterministic
  command no-ops that did not mutate Bitcask state. The covered cases are CAS
  mismatch/missing and conditional SET/SET_BLOB_REF skips (`NX` on existing key
  and `XX` on missing key); broader no-op classification should only be added
  with command-specific tests.
- WARaft membership/config commits now wait until the FerricStore storage handle
  reports the committed config position as durable. WARaft can return
  `{:ok, position}` from config submission before the storage callback has
  persisted its metadata; the backend now polls the storage handle and maps
  storage blocks/timeouts to the same `{:timeout, :unknown_outcome}` contract as
  data writes.
- After a WARaft storage infrastructure failure, the storage callback latches a
  blocked state until restart. Later log positions return `:storage_blocked`
  instead of applying and persisting a newer replay position that would skip the
  failed committed entry. The blocked path emits
  `[:ferricstore, :waraft, :storage_blocked]` telemetry for the first
  infrastructure failure and for later rejected applies/config/snapshot
  attempts.
- WARaft storage refuses snapshot and witness-snapshot creation while blocked.
  The underlying WARaft storage process can have a volatile apply pointer ahead
  of FerricStore's durable storage handle after an apply failure; exporting a
  snapshot in that state could report the newer volatile position for older
  Bitcask/blob contents.
- WARaft storage metadata temp files are fsynced before rename, then the parent
  dir is fsynced. Tests cover the happy path, forced metadata file fsync
  failure, and forced parent-dir fsync failure; failures return an error and
  leave the replay position pinned for restart replay. The hot metadata journal
  reader streams records at startup, so a large long-lived journal does not
  become one large restart-time binary. Individual journal records are capped
  because storage metadata should stay small; a corrupt or bloated size header
  fails closed instead of asking the VM to read an unbounded payload. Current
  and previous storage metadata files are capped with the same bound before
  read, so a bloated local metadata term cannot become the restart path.
- WARaft storage metadata now fails closed before opening shard payload when a
  persisted metadata file is absent while live shard payload exists, malformed,
  missing the required replay position, has an invalid replay position, or
  carries malformed persisted config. This prevents bad or missing metadata
  from defaulting to position zero and letting bootstrap/snapshot setup reset
  live Bitcask/blob payload. The empty Flow history projection marker
  (`flow_history_projected.index` with value `0`) is explicitly treated as
  startup metadata, not user payload, so fresh WARaft application boot remains
  possible while non-empty shard data still fails closed.
- WARaft partition specs can now enable a WARaft `label_module`, and the
  backend preserves storage labels through the storage callback contract.
  Successful applies persist label and replay position atomically before
  graceful close; metadata fsync failure leaves both the old replay position and
  old label in storage state.
- Snapshot creation and install copy real shard-owned Bitcask/blob/prob
  directories into a stalled backend member and rebuild the keydir from disk.
  Segment-backed compound records are copied through the WARaft
  projection log instead of promoted dedicated directories.
- Snapshot install uses staged payload dirs plus an install marker. Tests now
  prove incomplete snapshot payloads fail before wiping live data, copy failure
  leaves live data readable after restart, and startup rolls back an interrupted
  directory swap when storage metadata was not advanced to the snapshot position.
- Snapshot-install recovery fails closed when storage metadata is unreadable or
  malformed. Startup keeps the install marker and backup dirs intact instead of
  guessing whether to finalize or roll back live shard data.
- WARaft local storage metadata, snapshot metadata, and pending
  snapshot-install markers are decoded as trusted local persisted terms because
  durable cluster configs can contain peer node atoms that a restarted VM has
  not interned yet. The decoded shape is still validated before use,
  payload-dir declarations are checked before opening a snapshot, and pending
  install marker paths outside the WARaft storage directory are rejected.
  Recovery rolls back a position mismatch only when the marker also has a
  complete local backup payload for every shard storage directory; otherwise it
  fails closed and leaves the marker intact for inspection.
- Snapshot creation fails closed if copied payload dirs cannot be scanned while
  building metadata, instead of crashing the WARaft storage process on a bang
  filesystem call.
- Snapshot metadata records which payload dirs were empty at creation time.
  Missing empty dirs are recreated after WARaft transport, but missing non-empty
  dirs fail closed before live shard data is touched. Install also re-stats each
  payload dir immediately before staging, so a required dir that disappears
  between metadata verification and copy cannot be promoted as an empty live
  shard directory.
- Snapshot install restart recovery also finalizes an interrupted directory
  swap when storage metadata was already advanced to the snapshot position, so
  cleanup crashes do not roll back a completed install.
- Snapshot install finalization treats staging/backup cleanup as part of the
  recovery contract. If cleanup fails, startup fails closed and preserves the
  install marker instead of deleting the marker and leaking old payload dirs.
- Snapshot creation is tested against a concurrent write submitted while the
  storage snapshot callback is paused; the installed snapshot includes the
  pre-snapshot key and excludes the later write.
- Replay-safe no-sync snapshot creation is tested while the source shard has
  dirty payload. The snapshot copies and fsyncs its own payload tree without
  first fsyncing the live source Bitcask tree, and the copied snapshot installs
  into a new member with the key readable after recovery.
- A tagged 3-node backend test now starts peer BEAM nodes, bootstraps one
  WARaft partition per peer through the backend API, commits through the real
  FerricStore state machine on the elected leader, and verifies replicated
  local reads on all peers.
- A tagged 3-node backend test also proves a write submitted to a follower is
  redirected to the known WARaft leader and still applies on all peers. This
  keeps the backend compatible with client traffic landing on any node.
- WARaft remote redirect failure classification preserves the existing
  unknown-outcome contract. If an `:erpc` redirect times out after the call may
  have reached the leader, data writes normalize to write-timeout-unknown and
  membership redirects return the same unknown-outcome error. A definite
  no-connection remains `leader_unavailable`.
- WARaft redirect peer normalization rejects boolean/nil pseudo-nodes before
  any `:erpc` redirect path. This matters because booleans are atoms on the
  BEAM; malformed status or notify-redirect metadata must not become a remote
  node target.
- A tagged 3-node backend test now starts multiple FerricStore shards per peer
  and proves independent WARaft partitions replicate through the same backend
  cluster.
- A tagged 3-node backend restart test stops all peer backends after an
  acknowledged write, rebuilds the instance contexts on the same data
  directories, re-elects a leader, verifies the acknowledged write on all peers,
  and commits another write after restart.
- A tagged 3-node backend membership test removes a peer through
  `Ferricstore.Raft.WARaftBackend.adjust_membership/3`, verifies the WARaft
  membership no longer includes that peer, then commits and reads a real
  FerricStore state-machine write on the remaining members.
- A focused backend test forces membership metadata fsync failure and proves the
  caller gets `{:timeout, :unknown_outcome}`, later writes stay blocked, and no
  new value becomes visible until restart recovery can replay from the durable
  storage position.
- A focused backend test now also forces segment file fsync failure and proves
  the public write path returns the same unknown-outcome contract while the
  unacknowledged value stays invisible after restart.
- A tagged shard-kill backend test now kills the single-shard WARaft server
  during active write load, restarts from the durable segment log, and verifies
  every write that had already returned `:ok` remains readable.
- Tagged shard-kill backend tests now also cover multi-shard local load and a
  three-peer cluster leader-process kill during active writes. The cluster test
  uses WARaft election-timeout options added to the backend so failover timing
  is deterministic without changing the production default.
- Tagged three-peer chaos tests now cover full follower and leader peer-node
  crashes during active writes. They verify acknowledged writes remain readable
  on the surviving quorum, crashed peers can restart from their existing data
  directory, and restarted peers catch up both pre-crash and post-restart
  writes.
- A tagged three-peer chaos test now hard-kills the leader BEAM OS process
  during active writes with `kill -9`, verifies acknowledged writes survive on
  the remaining quorum, restarts the killed peer, and verifies catch-up from the
  durable segment log.
- A tagged three-peer chaos test now also hard-kills a follower BEAM OS process
  during active writes, verifies the live quorum keeps acknowledged writes
  readable, restarts the killed peer from its existing data directory, and
  verifies it catches up.
- A tagged three-peer chaos test now hard-kills both followers, verifies the
  surviving leader cannot acknowledge writes without quorum, restarts one peer
  to restore quorum, commits again, restarts the final peer, and verifies all
  acknowledged values catch up while the no-quorum value remains absent.
- A tagged single-peer kill test covers replay-safe no-sync recovery directly:
  with the payload frontier disabled, an acknowledged write survives a BEAM OS
  process kill because the WARaft log replays entries newer than the durable
  storage position on restart.
- Local leaders now reject submissions before append when current Erlang
  distribution connectivity cannot reach voter quorum. This prevents a
  timeout/unknown no-quorum write from staying in the leader log and committing
  later after quorum returns. Voter nodes are cached from durable config apply,
  snapshot/open recovery, and startup bootstrap so the normal write path does
  not call `wa_raft_server:status/1` per command.
- A tagged three-peer chaos test now isolates a follower with a peer-to-peer
  network partition, verifies the minority cannot acknowledge writes, commits
  on the majority side, heals the partition, and verifies the isolated peer
  catches up.
- A tagged three-peer chaos test now isolates the current leader from quorum,
  verifies the old leader cannot acknowledge a minority write, commits on the
  new majority leader, heals the partition, and verifies the old leader catches
  up without preserving the uncommitted minority value.
- A tagged three-peer chaos test now repeats partition/heal cycles across both
  leader and follower isolation cases. Each cycle rejects the isolated-side
  write, commits on the majority, heals, and verifies all majority writes
  remain readable while minority writes remain absent.
- A tagged backend member-add test starts a fourth peer after data already
  exists, stages it as a participant, transfers a real snapshot through WARaft's
  transport, waits until it is ready, promotes it to membership, verifies the
  pre-existing key on the new peer, then commits and reads a post-add key on all
  four peers.
- Tagged backend member-add tests now also prove retry from a failed staged add,
  full-cluster restart after member add, and catch-up of large blob-backed
  values.
- One-node WARaft clusters disable the blob side-channel while a second node is
  staged as a participant. Participants can receive Raft entries before they are
  voters, so ref-only blob commands are only allowed when the config has exactly
  one participant and one member.
- Backend `membership/1` normalizes WARaft `#raft_identity{}` records into
  `{server_name, node}` peer tuples so FerricStore callers do not depend on
  WARaft record internals.
- Shared cluster APIs are now backend guarded. `start_system/1` is a no-op
  under WARaft, `stop_system/0` is a no-op under WARaft because application
  shutdown stops the WARaft backend directly, `start_shard_server/6` and
  `join_shard_server/7` fail closed instead of accidentally starting legacy Ra, and
  `trigger_shard_elections_parallel/2` delegates to WARaft election handling.
- `Ferricstore.Raft.Cluster.members/1`, `add_member/3`, `remove_member/2`, and
  `transfer_leadership/2` honor the backend selector. WARaft `:voter` joins
  stage a snapshot and then promote, while `:promotable`/`:non_voter` joins
  remain participant-only with snapshot transfer. Existing WARaft voters can be
  demoted back to participant-only through the same shared API.
- Shared cluster APIs are leader-location tolerant under WARaft. `members/1`
  reports the same leader peer from both leaders and followers.
  `add_member/3` redirects the full staged snapshot/promote workflow from a
  follower, including promotable participant joins, voter demotion, and member
  removal through follower-origin public calls.
- WARaft backend shutdown cleanup now derives registered partition names from
  the configured shard count instead of a fixed 64-partition range, so clusters
  with more than 64 shards do not leave partition processes behind during stop
  or failed-start cleanup.
- `INFO raft`, `INFO ferricstore`, and `CLUSTER.HEALTH` now read membership and
  progress through shared backend APIs instead of directly calling legacy Ra.
  WARaft reports leader/role through cached/bounded membership probes and
  derives progress/term from storage position, so `INFO raft` does not block on
  live WARaft status RPCs.
- Startup health checks are backend-aware. Readiness uses the shared
  `Ferricstore.Raft.Cluster.members/2` facade with the existing 1s probe
  timeout instead of calling `:ra.members/2` directly, so a WARaft-only boot can
  become healthy without any legacy Ra server while legacy health probes remain
  bounded.
- WARaft bounded membership probes now honor the caller's timeout path without
  waiting on `wa_raft_server:status/1`. The bounded path reads cached voter
  nodes plus WARaft's local leader cache first and only uses a killed bounded
  fallback if the cache is cold, keeping health readiness probes from inheriting
  WARaft's longer default RPC timeout.
- Cluster operational status now has a bounded `ClusterManager.node_status/1`
  path, and `CLUSTER.STATUS`/`CLUSTER.HEALTH` use bounded membership probes so a
  slow backend probe cannot make the commands inherit WARaft's longer default
  RPC timeout. `CLUSTER.ROLE` now reports the configured Raft role from the
  replication mode instead of the old standalone/topology mode, matching the
  removal of standalone durability.
- Production operational Ra probes now have a source guard. Membership,
  leadership, restart/delete, overview, and remote `:erpc.call(node, :ra, ...)`
  calls must stay behind `Ferricstore.Raft.Cluster`, so WARaft cannot be
  bypassed by health, DataSync, or ClusterManager support paths.
- Data sync index introspection uses WARaft storage position when the WARaft
  backend is selected, keeping sync logging and join diagnostics away from
  legacy Ra counters.
- Data sync resync checks fail closed to a full resync under WARaft rather than
  trying to inspect legacy Ra WAL entries.
- The legacy DataSync copy path now fails closed under WARaft. That path can
  only pause legacy batchers/shards; WARaft membership uses snapshot transfer
  instead, so calling the legacy copy path under WARaft returns
  `:unsupported_waraft_data_sync` rather than racing live WARaft writes.
- ClusterManager's remote join path is backend-aware. WARaft joins skip legacy
  data sync and target Ra stop/start, add membership through the shared WARaft
  API, persist target markers from WARaft storage positions, and roll back
  newly-added membership if marker persistence fails.
- WARaft join rollback now removes every newly-added target participant/voter
  from all shards that reached membership mutation, including the shard that
  surfaced the later failure. Explicit target cleanup removes shard Bitcask
  data plus blob payload dirs, legacy dedicated payload dirs if present,
  WARaft/Ra backend roots, and mode marker files; the target-data probe
  intentionally ignores freshly booted backend roots and mode markers so a
  clean node is not rejected as dirty.
- `CommandClock.process_command/2,3` and `pipeline_command/4` now dispatch
  through WARaft when selected while preserving the legacy `{:applied_at, ...}`
  reply/event shape used by cross-shard operations and transactions.
- Default application `MULTI`/`EXEC` now has WARaft coverage through
  `CommandClock.pipeline_command/4`, including WATCH conflict aborts after a
  concurrent WARaft write.
- Generic `COPY`, `RENAME`, and `RENAMENX` now have application-level WARaft
  restart coverage for both plain string values and cross-shard hash compound
  values. The compound case verifies metadata/member records move through the
  selected backend and survive durable segment-log restart.
- Generic cross-shard `COPY`/`RENAME` restart coverage now also includes
  blob-backed values, proving the large-value side channel survives key movement
  and durable WARaft restart without being downgraded to inline payloads.
- Flow cross-shard terminal propagation now has WARaft coverage for both
  single commands and terminal-many commands. The matrix covers child
  `complete`, retry exhaustion, `fail`, parent `cancel`, `complete_many`,
  `retry_many`, `fail_many`, and `cancel_many`, including parent child-group
  summary updates through the selected backend.
- Flow retention cleanup now has WARaft multi-shard coverage. The test creates
  expired terminal records on two shards, runs bounded cleanup through WARaft,
  and verifies state/history/value cleanup accounting plus state deletion.
- Forwarded Router batches no longer fall back to the legacy Batcher under
  WARaft when `origin_node` is set. Forwarded generic, put, delete, and
  forced-quorum writes stay on `WARaftBackend`.
- Key-level expiry commands have restart coverage under WARaft. The test sets a
  plain key, applies `PEXPIRE`, restarts from the durable segment log, verifies
  the TTL/value remain live, applies `PERSIST`, restarts again, and verifies the
  key remains persistent.
- File-backed probabilistic commands have restart coverage under WARaft. Bloom,
  CMS, Cuckoo, TopK, and TDigest commands are written through WARaft, recovered
  from the durable segment log, and verified through their public read commands.
- Default-instance legacy single-write ingress now goes through the shared
  Router/Batcher/backend write boundary instead of calling the Shard GenServer
  write handler directly. The Shard GenServer remains responsible for
  ownership/recovery/read and custom-instance paths.
- If a stale default-instance caller still reaches the Shard GenServer while
  WARaft is selected, write handlers route through `Ferricstore.Raft.Backend`
  instead of mutating local Bitcask/ETS directly. Boot coverage asserts a direct
  shard `PUT` advances WARaft storage position.
- Replay-safe index writers for Bitcask and Flow LMDB persist their marker
  files under both backends, but skip the legacy `release_cursor_poke` Batcher
  command when WARaft is selected. WARaft owns replay position through its
  storage/segment-log path.
- `SAVE`/persistence barriers skip legacy Batcher flushes under WARaft while
  still flushing Bitcask writers and active-file checkpointers.
- `DEBUG BATCHER-STATS` is backend-aware. Legacy mode still reports
  `B*/WAL/R*`; WARaft mode reports per-shard WARaft
  server/acceptor/queue/storage process stats and does not present missing
  legacy processes as failures.
- `FERRICSTORE.CONFIG SET <prefix> window_ms <n>` is supported under WARaft.
  Default-window writes stay on the direct acceptor path; explicitly configured
  larger namespace windows go through a small per-shard WARaft namespace batcher
  and emit `[:ferricstore, :waraft, :batcher, :slot_flush]` telemetry.
  Namespace and hot-batch timers carry per-window tokens so a stale timer
  message cannot flush a newer window early and destroy batching under pipeline
  churn.
- The application-level WARaft gate now includes a real RESP/TCP `SET` + `GET`
  path through `ferricstore_server`, the Rust RESP parser, Router, selected
  WARaft backend, durable segment log, Bitcask apply, shutdown, restart, and a
  second TCP `GET` verifying the value survived.

Not covered yet:

- Azure side-by-side against current Ra with the same RESP/router benchmark
  script and client protocol.
- Broader crash/restart chaos tests: longer-running mixed chaos matrices that
  combine OS kills, partitions, membership changes, and active acknowledged
  write load in the same scenario.
- Production segment WAL integration with Bitcask/blob checkpoints and
  production-ready group-fsync policy beyond the spike provider.
- Full command surface audit remains open for broad public command sweeps.
  Hash, List, Set, Geo, sorted-set helpers, streams, probabilistic commands,
  generic cross-shard key movement, and advanced Flow terminal/retention paths
  now have representative WARaft coverage, but this is still not a
  replacement-grade exhaustive command matrix.

## Local Benchmark Snapshot

These are local development-machine numbers. The real RESP/router rows include
RESP parsing, routing, WARaft, durable storage, and the production state
machine. The 3-node row still uses the isolated WARaft cluster load driver and
should not be compared directly to the RESP rows.

| Case | Path | Log | Result |
| --- | --- | --- | --- |
| SET 256B, c=200/p=50 | Current Ra, real RESP server | current Ra WAL | ~473K ops/s, p99 batch ~27.0ms |
| SET 256B, c=200/p=50 | WARaft, real RESP server | segment/keydir, file datasync | ~622K ops/s, p99 batch ~19.5ms |
| SET 1KB, c=200/p=50 | Current Ra, real RESP server | current Ra WAL | ~415K ops/s, ~406MB/s, p99 batch ~30.0ms |
| SET 1KB, c=200/p=50 | WARaft, real RESP server | segment/keydir, file datasync | ~583K ops/s, ~570MB/s, p99 batch ~21.5ms |
| Mixed GET/SET 256B, c=200/p=50 | Current Ra, real RESP server | current Ra WAL | ~342K ops/s, p99 batch ~44.6ms |
| Mixed GET/SET 256B, c=200/p=50 | WARaft, real RESP server | segment/keydir, file datasync | ~371K ops/s, p99 batch ~38.7ms |
| SET 256B, c=100/p=50 | WARaft 3 local peer nodes | spike segment log, file datasync | ~42K ops/s |
| SET 256B, c=200/p=50 | WARaft, real RESP server | segment/keydir, WAL-NIF sync delay 0 | ~217K ops/s, p99 batch ~65.0ms |
| Restart replay, 100K keys | Current Ra recovery task | current Ra WAL | ~591ms recovery |
| Restart replay, 100K keys | WARaft spike recovery script | spike segment log + toy storage | ~107ms recovery |

Important interpretation:

- WARaft core still needs deeper work on the real durable path. We no longer
  count in-memory-log numbers as decision data.
- The segment log groups all records in one same-segment Raft append behind one
  data sync before returning. Cross-append group fsync is still delegated to
  WARaft's commit batch window; the benchmark path now derives the default
  WARaft window from the same `wal_commit_delay_us` knob used by the patched Ra
  WAL, while preserving an explicit `:waraft_commit_batch_interval_ms` override.
  WARaft only exposes millisecond state timeouts, so the Ra adaptive floor maps
  to `0ms` when disabled or `1ms` when enabled unless the WARaft-specific knob is
  set.
- Replacement RESP/router benchmarks use the production segment I/O mode
  (`file:write` + `file:datasync`). On the local c=200/p=50 256B SET run, that
  path reached ~622K ops/s with p99 batch latency ~19.5ms, versus current Ra at
  ~473K ops/s. On the same local harness, 1KB SET reached ~583K ops/s
  (~570MB/s) versus Ra at ~415K ops/s, and mixed 256B GET/SET reached ~371K
  ops/s versus Ra at ~342K ops/s. The raw WAL-NIF segment path reached only
  ~217K ops/s in experiments, so it is not exposed as a benchmark or runtime
  mode.
- The current WARaft replacement benchmark path uses segment/keydir storage:
  projectable hot-path records install ETS/keydir locations that point directly
  at the WARaft segment (`{:waraft_segment, index}`), so the normal Bitcask
  payload file stays empty for covered commands and cold large values are read
  back from the Raft segment. Commands that are not projectable still need
  projector coverage before this can replace the full Ra path.
- Segment-log recovery now treats rewrite markers as untrusted local metadata:
  staging/backup paths must be direct children of the log parent with the
  expected rewrite prefixes. Corrupt record headers with impossible lengths emit
  corruption telemetry and fail closed when the declared full payload exists.
  If the impossible length points past EOF, recovery treats it as a torn
  uncommitted tail and truncates before reopening, so a crash during append
  cannot brick an otherwise valid segment. Duplicate recovered Raft indexes also
  emit corruption telemetry and fail closed instead of allowing ETS insertion
  order to pick a winner. Gaps between recovered indexes now fail closed before
  WARaft can skip a committed entry during replay. Segment files are sorted by
  numeric segment ordinal, not lexicographic filename, so recovery past `10.seg`
  does not reject a valid contiguous log. Malformed `*.seg` filenames now emit
  the same segment corruption telemetry before failing closed. If a later
  segment is corrupt, the provider clears any records loaded from earlier
  segments before returning the open error so callers cannot observe a partial
  recovered log. Segment append now syncs the directory entry only when writing
  the first record to a segment file, closing the new-file crash window without
  adding directory fsyncs to normal appends. If that new-segment directory sync
  fails, append returns an error and the unacknowledged record is not exposed
  through the in-memory log view.
- Segment-log local metadata is capped before `binary_to_term/2`. Oversized
  `segment_config.term` and pending rewrite markers now fail closed during log
  open instead of being accepted because they contain the required keys plus a
  large ignored field. Metadata reads use link-aware file info and only accept
  regular files, so a symlinked segment config or rewrite marker fails closed
  before deserialization or path use.
- Segment-log data files also use link-aware validation before recovery,
  append rollback, and tail truncation. Symlinked `*.seg` files fail closed
  rather than replaying or appending outside the WARaft data root. Recovery
  also validates that every record index belongs to the numeric ordinal of the
  segment file containing it, so a renamed segment cannot be replayed under the
  wrong append slot. Duplicate numeric segment ordinals such as `0.seg` and
  `00.seg` are rejected before recovery, even if the duplicate file is empty,
  and non-canonical ordinal filenames such as `01.seg` are rejected so future
  appends cannot recreate the canonical `1.seg` beside them.
  The `segment_log` directory itself is also `lstat`-validated before rewrite
  recovery, after directory creation, and on append/rewrite entry points, so a
  symlinked log root cannot redirect recovery or live appends outside the WARaft
  partition root.
- Segment-log rewrite recovery treats rewrite directories as real directories,
  not paths that merely resolve through symlinks. A pending rewrite marker with
  a symlink backup now fails closed instead of replacing `segment_log` with a
  symlink during rollback.
- Segment-log recovery streams records from each segment with `file:read/2`
  instead of materializing the whole segment through `file:read_file/1`. The
  test suite has a source guard for this because production WAL segments must be
  allowed to grow without restart allocating one binary per segment file.
- The spike segment-log sizing knob is treated as a creation-time log property.
  A durable `segment_config.term` marker pins the first configured
  records-per-segment value, and a regression test changes the application env
  between restarts before appending more records and replaying again. Corrupted
  segment config metadata is checked at log open and blocks startup. Missing
  segment config metadata also blocks startup when segment files already exist,
  because reopening with a different default could map new Raft indexes to the
  wrong segment ordinal. Trim/truncate rewrites copy the pinned segment sizing
  into the staging directory instead of reading the current app env again, and
  stream kept records into staging in segment-sized groups so rewrite memory is
  bounded by the configured segment size rather than retained log length. The
  per-path sizing cache is cleared on log close to avoid stale path state in
  tests and dynamic replacement runs.
- Snapshot install rollback only replaces live payload dirs that have a matching
  backup dir. If install fails after the marker is written but before all live
  dirs are backed up, untouched payload dirs are left in place instead of being
  deleted during rollback. Pending-install recovery also validates backup dirs
  with `lstat`, so a stale marker cannot treat a backup symlink as a complete
  rollback payload.
- Snapshot payload copy uses `lstat` and a verified copier instead of
  `File.cp_r`, so snapshot creation/install only accepts regular files and
  directories. Symlinks and other special files fail closed before install can
  publish a malformed payload tree or fsync a path outside the shard root.
- WARaft local metadata reads are bounded before decoding terms. Current,
  previous, and journaled storage metadata, snapshot metadata, and pending
  snapshot-install markers all reject records above the metadata cap before
  allocating the payload binary. Oversized metadata fails closed and keeps the
  marker/files in place for operator inspection instead of silently accepting a
  huge local term. The storage metadata write path also enforces the same cap
  before appending to the journal or compacting the metadata file, and reuses the
  encoded term so successful writes do not pay double serialization. Snapshot
  creation uses the same write-side cap before publishing snapshot metadata, so
  a bad label/config provider cannot create a huge local metadata term. Bounded
  local metadata reads and hot metadata journal append/recovery checks use
  `lstat` and only accept regular files, so metadata symlinks fail closed
  instead of trusting or appending to files outside the shard root. Missing
  metadata only bootstraps as empty when shard payload dirs contain directories
  or regular empty marker files; zero-size special files now fail closed and
  preserve the unsafe path reason.
- WARaft default replication settings are not suitable for FerricStore's
  pipeline depth. `raft_max_log_entries_per_heartbeat` was the biggest knob:
  the default is 15, and the spike needs 1024+ for high-throughput batches.
  The backend now exposes the heartbeat/apply batch knobs through start options
  and FerricStore app env so Azure runs can tune them without code edits. Those
  knobs fail closed unless configured as positive integers, so a bad benchmark
  env cannot silently boot with pathological WARaft limits.
- WARaft election timeout tuning now validates `timeout_max >= timeout` before
  any backend app env is published. This keeps failover/chaos benchmarks from
  accidentally booting with inverted timing bounds.
- WARaft queue and commit tuning now fail closed before any backend app env or
  persistent-term state is published. Pending queue limits and commit delay are
  non-negative integers, `raft_commit_batch_max` is positive, and in-flight byte
  caps must be non-negative integers or `:infinity`. This keeps benchmark/prod
  config mistakes from silently booting with pathological backpressure behavior.
- WARaft log and label provider modules are now validated during the same
  preflight config pass. Missing modules or missing required callbacks fail
  before the backend starts/stops partitions or publishes replacement context.
- WARaft startup config validation is intentionally ordered before replacing a
  running backend. A bad restart attempt must leave the live backend, context,
  and committed state intact so config rollout mistakes do not turn into
  availability incidents.
- WARaft redirect failure handling keeps timeout semantics explicit across
  writes, membership changes, and leadership transfer. A remote transfer timeout
  is an unknown outcome, not merely "leader unavailable", because the handoff may
  still complete after the caller times out.
- WARaft cluster bootstrap now validates the requested member list before any
  shard snapshot is written. Empty clusters, non-node terms, and duplicate nodes
  fail closed so the backend cannot publish a zero-voter or malformed baseline
  config.
- Repeated `bootstrap_cluster/1` is only idempotent when the requested voters
  match the already-persisted voters. A conflicting bootstrap now returns
  `{:already_bootstrapped, actual_nodes}` and caches the actual config, not the
  caller's stale requested config.
- WARaft membership add paths have regression coverage for invalid timeout
  values: bad timeouts must return an error without adding the target as a
  participant/member.
- WARaft membership inputs are preflighted before config append. `nil`/boolean
  atoms are rejected as invalid node names, and negative/non-integer membership
  timeouts are rejected before an add-member/add-participant entry can enter the
  log.
- WARaft cached voter extraction applies the same node validation to recovered
  or externally supplied config metadata. Malformed `nil`/boolean peers are
  ignored instead of poisoning quorum checks with unreachable fake voters.
- WARaft backend context is now published only after startup config validation
  succeeds. Bad config no longer leaves a half-started context visible to
  Router/storage code after `start/2` raises.
- WARaft info cleanup is now idempotent after the shared app ETS table is
  already gone. This avoids noisy `badarg` shutdown crash reports when peer
  nodes or replacement tests tear down the WARaft application before every
  server process has reached `terminate/3`.
- Single-member WARaft leaders now honor the same commit batch window as
  multi-member leaders. Before this fork patch, the server applied single-node
  commits before checking `raft_commit_batch_interval_ms`, so concurrent
  one-node durable writes could still produce one segment fsync per write.
- Single-member WARaft leaders use async segment append by default. Local
  segment append+fsync runs on a
  background process while the server keeps accepting more commands into the
  next batch. The server does not advance `log_view`, apply storage, or reply to
  callers until the durable append completion message returns. Tests block the
  segment fsync and prove the leader remains responsive, the blocked write is
  invisible, and commands arriving during that fsync are grouped into the next
  durable append. This remains single-member only; multi-node leaders still use
  the synchronous append path until leader-stepdown and follower-replication
  safety is designed.
- FerricStore hot GET should keep using local applied state. WARaft strong reads
  use a separate read queue and saturated under the c=200/p=50 mixed load, so
  the migration shape should not put normal GET behind `wa_raft_acceptor:read/3`.

## Decision Rule

WARaft is worth deeper work if a durable prototype beats current Ra by at least
5% on the same Azure topology, or materially improves p99/p99.9 without a
correctness gap.

The current local benchmark is promising enough to continue, but it is not a
migration decision because the spike still skips the real RESP/router/Bitcask/blob
path and its segment WAL is not production-grade.

## Next Gate

The next useful gate is a durable FerricStore-compatible WARaft path:

- current key hashing and local hot-read path,
- production-style WAL grouping or a direct integration with FerricStore's WAL,
- restart and snapshot-install chaos around acknowledged writes,
- Azure three-node backend cluster with production baseline copy/snapshot
  catchup and member add/remove,
- the same memtier c=200/p=50 runs on Azure.
