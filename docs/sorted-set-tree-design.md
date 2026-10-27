# Sorted Set Tree Design

From feature level 9, a sorted set past the promotion threshold lives in its own
file: a counted, copy-on-write B+tree, read and written by stateless NIF calls.
Every sorted-set read and write is O(log N), including ZRANK and ZRANGE by
index, and the set needs no RAM between calls and no rebuild after a restart.

Engine: `native/ferricstore_bitcask/src/zset_tree/`. Elixir wrapper:
`Ferricstore.Store.ZSetTree`. Apply side:
`Ferricstore.Raft.StateMachine.Sections.ZSetTreeApply`. File upkeep:
`Ferricstore.Raft.WARaftStorage.Sections.ZSetTreeMaintenance`.

## Why

A sorted set's members are durable today, but its read structures live only in
RAM:

- Every member is a compound key (`Z:key\0member`) in the shard's ETS keydir,
  including after promotion to a dedicated Bitcask instance (promotion moves the
  values, and the entries stay in ETS).
- The score index (`Ferricstore.Store.Shard.ZSetIndex`) is a second in-RAM copy:
  an ETS `ordered_set` of `{key, tag, score, member}` plus a member lookup
  table. After a restart it is rebuilt per sorted set, on that set's first read,
  by scanning every member.
- An ETS `ordered_set` can seek, but it can't count positions. `member_rank/5`
  and `rank_range/5` walk from the set's first member with `:ets.next/2`, about
  0.6 µs per member.

Measured on a local prod build with 7,200 members (Inspeactor's event index),
inside the VM:

| Operation | Time |
|---|---|
| ZCARD | 0.02 ms |
| ZRANGE 0 99 | 0.07 ms |
| ZRANGE at the last 100 | 4.2 ms |
| ZRANK of the last member | 4.3 ms |

A client that pages a set by position (ZRANK, then ZRANGE from that offset) pays
O(N) per page and O(N²) per full read: 0.73 s at 7,200 members. Redis does both
in O(log N) because each skiplist link records how many elements it spans.

## Goals

- O(log N) for ZRANK, ZREVRANK and ZRANGE / ZREVRANGE by index, and for every
  other sorted-set read and write.
- Sorted sets larger than RAM: no per-member RAM in the keydir or in an index.
- No rebuild after a restart: the structure on disk is the index.
- NIF memory lives only for one call. Hot pages stay in the kernel page cache,
  which the kernel evicts under pressure. This is the model the probabilistic
  structures already use (see "Stateless pread/pwrite Structures" in
  `guides/architecture.md`).
- Durability no weaker than today: every acknowledged write survives a crash,
  through the Raft log and replay.
- Writes stay deterministic under Raft apply, and replay is idempotent.

## Non-goals

- Small sorted sets keep today's compound-key layout. Below the promotion
  threshold (100 members by default) a walk costs at most about 60 µs, and a
  file per tiny set would cost more than it saves.
- Flow's internal score indexes (`Flow.LMDBRebuilder.ActiveIndexes`) keep the
  ETS index. They are bounded and rebuilt from LMDB.
- Hashes and sets are out of scope. The same file format without counts could
  carry them later.

## Why not an existing B+tree

| Option | Position lookups | RAM between calls | Verdict |
|---|---|---|---|
| ETS `ordered_set` (today) | O(N) walk | All members | Not disk-native |
| Rust `BTreeMap` in a NIF resource | O(N) walk (no counts) | All members | Same problem in Rust |
| LMDB via `heed` | O(N) walk (no counts) | Open environment, reader table | No counts; long-lived handle |
| `redb` | O(N) walk (no counts) | Database handle with its own page cache | Fork needed for counts; long-lived handle |
| `sled` | O(N) walk | Large in-process cache | Beta, no counts |
| Fork of `redb` with counted branches | O(log N) | Long-lived handle | Possible, but we would own a fork of a general database |
| Purpose-built counted tree (this doc) | O(log N) | One call's pages | Recommended |

The counts are the core requirement, and no embedded engine keeps them. Forking
`redb` would give us crash safety we'd otherwise write, but we would carry a
general transactional database (multiple tables, savepoints, its own cache and
allocator) to use one access pattern, and still need a long-lived handle.
Disk-based Redis-compatible stores built on RocksDB (Kvrocks, Pika) have O(N)
ZRANK for the same reason. Dragonfly has a counted B+tree, but in RAM only.

A purpose-built tree only needs what a sorted set does: two orderings, counts,
append-only commits and compaction. Its crash model is the one FerricStore
already runs for promoted collections (no fsync per apply, sync before the
replay position moves, replay from the Raft log).

## File

One directory per sorted set, under the shard's dedicated directory:
`dedicated/shard_N/zset_tree:<sha256 of key>/`. It holds:

- `<generation>.tree`, the tree. The generation is the Raft position of the
  entry that converted the set (`index * 2^20 + ordinal`), so every replica and
  every replay name the same file, and a set deleted and created again never
  shares one.
- `<generation>.removed`, written once no set names that generation, holding the
  Raft index from which nothing reads it.
- `key`, the set's key, so the sweep can find the set's marker.

The promotion marker (type `:zset_tree`, tag 3) names the generation. The set
lives while its type key does, and the marker takes the type key's TTL.

```text
[ header region: slot A | slot B | padding ][ frame ][ frame ] ...
frame = [ header 40 B | pages, 4 KiB each, CRC each | logged changes | trailer 72 B ]
```

- Two trees per file: by score (key: sortable score bytes, -0 folded into 0,
  then the member; value: the original score bits) and by member (member to
  score bits). Branch entries carry their children's counts, so a rank or a
  position costs one descent.
- Members longer than 512 bytes keep a 64-byte prefix in the node and the rest
  in overflow pages.
- A frame's trailer names both roots, the member count and the Raft token
  (index, ordinal) the frame was written for. Any valid trailer describes a
  whole tree.
- The header slots name the latest frame and the last one known to be on disk.
  Recovery trusts frames, not slots.
- Each logged change records what the member had before: `Set(member, score,
  prev)` or `Remove(member, prev)`.
- The first frame of a compacted file is a base frame. Its logged-changes region
  is an undo log instead: a floor index, then the changes after it, one group
  per folded frame.

## Writes

A write is one frame: copy the path to each changed leaf, append the dirty pages,
the logged changes and the trailer, then rewrite a header slot. Writers hold the
file's `flock`. A batch that changes nothing writes no frame.

Tree writes run only inside a WARaft apply, which gives them the entry's Raft
index; the ordinal counts the entry's tree writes. Outside one they fail.

## Replays and rewinds

A replayed entry must see the set as it did the first time, but after a crash
the file can hold frames the previous run wrote for that entry and later ones.
So the first time an entry touches a tree, the file is cut back to just before
the entry's position:

- Frames after the base are cut off the end.
- Below the base, the undo log is undone back to the position, and the result is
  written to a new file that replaces the old one whole. This only happens while
  a restart replays.
- At or below the undo log's floor the cut fails: Raft never replays there, and
  an entry that asks is a bug, so apply stops rather than run it on a later
  state.

MULTI writes trees as it goes. A transaction that fails cuts each tree it wrote
back to before its first write.

Readers take no lock. After a query, a reader checks that the frame it read
still ends the same way, and runs again if not: a cut can drop frames and a
later frame can reuse their bytes.

## Durability

The model is the one promoted collections use. A tree write does not fsync;
`PromotedFileSync` tracks the file, and the storage syncs it before it persists
a replay position or writes a checkpoint past the write. Recovery checks each
frame after the last synced one (CRCs, decodable changes) and cuts the file
after the last whole one; Raft replay restores the rest.

## Compaction and removal

The replay floor is the segment projection checkpoint: recovery replays only
the entries after it. Checkpoints are rare (every million entries or more), so
compaction cannot wait for them. It folds every frame into a base of the latest
tree and keeps the changes after the floor as the base's undo log, so a hot set
stays near the size of its data plus a few dozen bytes per change since the
last checkpoint.

The storage process runs a pass at most once a second, after an apply:

- It sweeps a few tree directories. A generation the set's marker no longer
  names (deleted, expired and swept, or replaced by a later conversion) gets a
  `.removed` record at the applied position. One removed at or below the floor
  is deleted, as are temp files a stopped compaction or rewind left.
- It compacts, in a task, the files written since the last pass that have grown
  past twice their base and past 1 MiB. Compaction builds the new base without
  the lock, then takes it to copy the frames written meanwhile and rename the
  file in. Snapshot creation, snapshot install and close wait for the task.

Settings: `:zset_tree_maintenance_interval_ms` (1000),
`:zset_tree_dirs_per_pass` (64), `:zset_tree_compact_min_bytes` (1 MiB),
`:zset_tree_compact_growth` (2).

## Integration

- Conversion happens in apply, at level 9, before a write that would take a
  keydir set past the promotion threshold. It builds the tree from the members
  (including the entry's staged writes), stages the marker with the type key's
  TTL, and removes the members from the keydir.
- The apply store, MULTI's transaction store and the router route a tree set's
  member reads and writes to the tree; other keys keep their paths. A store
  routed across shards refuses tree sets.
- The segment projector sends any key with a tree marker to full apply, whatever
  the marker's TTL, so apply decides expiry by its own clock.
- A set promoted to a dedicated Bitcask before level 9 converts on its first
  write after the raise, whatever its size. The post-commit cleanup then finds a
  tree marker and removes the dedicated directory.
- Snapshots copy the dedicated directory, trees included.

## Testing

- Engine (`zset_tree/tests.rs`): a model check through writes, compactions and
  syncs; replays after compactions at random floors against the recorded
  history and outcomes; recovery of torn frames and slots; rewinds; writes
  during compaction (fails without the lock); reads beside rewinds; overflow
  keys; -0.
- `zset_tree_integration_test.exs`: random command runs against sets promoted
  before level 9, plain and in MULTI; conversion in MULTI; conversion of sets
  promoted before level 9, plain and in MULTI; MULTI rollback; key
  commands, GEO, expiry; WATCH, SCAN TYPE, FLUSHDB; restart; flat ZRANK as the
  set grows.
- `zset_tree_maintenance_test.exs`: removal after DEL and after expiry; small
  files under steady writes; a storage kill whose replay rewinds through the
  undo log (fails with a no-op revert).
- `zset_tree_snapshot_test.exs`: a snapshot installed on another node.
