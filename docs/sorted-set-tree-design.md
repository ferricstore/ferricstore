# Sorted Set Tree Design

This document proposes a disk-native storage structure for large sorted sets:
a counted, copy-on-write B+tree in one append-only file per sorted set, read
and written by stateless NIF calls.

Status: proposal, not implemented.

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

One file per promoted sorted set, in the directory promotion already gives the
key: `dedicated/shard_N/zset:{sha256_of_key}/tree`. WARaft snapshots already
ship `dedicated/` (`snapshot_payload_kinds/0`), so snapshots, follower installs
and backups carry the tree without new payload code.

The file is append-only. Every commit appends the pages it changed, then a
commit trailer. Committed bytes are never rewritten. Space goes back through
compaction (below), as with Bitcask files.

```text
[ header slot A | header slot B | commit 1 pages | trailer 1 | commit 2 pages | trailer 2 | ... ]
```

### Pages

Fixed-size pages (4 KiB; the size is recorded in the header). Every page carries
a type, its byte length and a CRC32C.

- **Score tree:** a B+tree ordered by `(score, member)`.
  - Leaf entries are `(score, member)`.
  - Branch entries are `(separator, child offset, child count)`, where
    `child count` is the number of members under that child.
- **Member tree:** a B+tree ordered by `member`, with leaf entries
  `(member, score)` and no counts. ZSCORE, ZMSCORE, and the old-score lookup in
  ZADD, ZINCRBY and ZREM use it.
- **Overflow pages:** a member longer than the inline limit (512 bytes) keeps a
  64-byte prefix inline plus a pointer to overflow pages holding the rest.
  Comparisons use the prefix and read the overflow only on a tie.

Scores are stored as an order-preserving 64-bit encoding of the IEEE double:
flip all bits of a negative, the sign bit of a positive. So byte order is score
order, and -0.0 and 0.0 compare as Redis compares them (equal, normalized to
0.0 on write).

### Commit trailer

```text
magic | version | generation | raft token (index, ordinal)
score root offset | member root offset | member count
pages start offset | pages end offset | CRC32C of the pages
previous trailer offset | logical ops (length-prefixed) | CRC32C of the trailer
```

The logical ops are the commit's member changes (put score, delete). Compaction
uses them to catch up (below), and they make a commit checkable on its own.

### Header slots

Two fixed slots at the start of the file. Each holds:
- a generation;
- the latest trailer offset;
- the **synced trailer offset**: the newest trailer known to be on disk;
- a CRC32C.

A commit writes the slot with the older generation. A reader reads both and uses
the valid one with the higher generation. A torn slot fails its CRC, so the
reader falls back to the other slot, which points at an older, still valid
commit. Pages are immutable, so any trailer a slot names describes a consistent
tree.

## Operations

All NIF calls are stateless:
- They open the file, read the header, follow page offsets with `pread`, and
  append with `pwrite`.
- They return a result and close.
- No cache, handle or tree survives the call.

### Reads

- Every read descends from the root of the latest commit, with a stack cursor:
  copy-on-write trees can't keep sibling links.
- **ZRANK:** look up the member's score in the member tree, then descend the
  score tree to `(score, member)`. The rank is the sum of the counts of the
  children left of the path, plus the position inside the leaf.
- **ZRANGE by index:** descend by cumulative counts to entry `start`, then walk
  forward `stop - start + 1` entries. Reverse ranks use `count - 1 - i`.
- **ZRANGE BYSCORE / BYLEX and ZCOUNT:** seek to the bound. ZCOUNT is
  `rank(max bound) - rank(min bound)`, also O(log N).
- **ZCARD:** the trailer's member count. **ZSCORE / ZMSCORE:** member tree
  lookups. **ZRANDMEMBER:** draw a random index, then a ZRANGE by index.
- Expected cost: with 4 KiB pages, leaves hold about 50 members of 64 bytes and
  branches fan out about 50 ways. Ten million members is height 4: four or five
  page reads from the page cache per lookup, about 5–20 µs.

### Writes

- **One commit per apply call.** Raft apply calls one NIF with the ordered ops
  for one sorted set and its Raft token. The NIF:
  1. applies the ops to private copies of the pages they touch;
  2. writes each touched page once;
  3. appends the trailer;
  4. updates a header slot.

  A batch of 1,000 ZADDs that land in the same leaves writes those leaves once.
  Commands inside MULTI/EXEC or a script commit one by one, so a read later in
  the same transaction sees the earlier write.
- **Idempotent replay.** Before applying, the NIF compares the call's Raft token
  with the latest trailer's token. An older or equal token is a replayed entry,
  and the NIF returns the result recorded for it (as `prob_txn.rs` does for the
  probabilistic structures). A newer token applies.
- **Results.** The NIF returns the per-op results Redis needs: added count,
  changed count, new score for ZINCRBY, popped members for ZPOPMIN/MAX. Apply
  turns them into replies, so the apply logic stays in Elixir.

### Durability

- **No fsync per commit.** The file joins the existing
  `Ferricstore.Raft.PromotedFileSync` tracking. The WARaft storage syncs every
  tracked file before it persists a replay position, so a commit that may not be
  on disk always lies after the position replay starts from.
- **Moving the synced point.** After that sync, the storage records the new
  synced trailer offset in a header slot. The header is a hint: the trailers are
  the source of truth.
- **Recovery** runs on the first open after a node start:
  1. Start from the synced trailer offset in the newest valid header slot.
  2. Walk forward one trailer at a time, checking each trailer's CRC and the CRC
     of its pages.
  3. Stop at the first commit that fails. Because pages from an unsynced commit
     can reach disk out of order, every commit after the synced point is
     checked, not just the last one.
  4. Truncate the file after the last valid trailer and rewrite both header
     slots.
  5. Raft replay then reapplies anything after that trailer's token. Tokens make
     the overlap harmless.
- **The `dedicated_unsynced` marker.** A storage that opens with it present (the
  existing marker) syncs every tracked file before replay, as it already does for
  dedicated Bitcask files.

### Compaction

The file grows with every commit, since a commit copies its path. A per-shard
compactor rewrites a tree once its file is more than twice its live page bytes:
1. Read the tree at trailer T. Its pages are immutable, so concurrent commits
   don't disturb this.
2. Bulk-load a packed tree into `tree.compacting`, a sequential write with leaves
   100% full.
3. Catch up: replay the logical ops of every trailer after T into the new file,
   in order, with their tokens.
4. Swap inside the shard's apply process, between apply batches:
   - copy the ops of any trailers written since the catch-up began;
   - fsync the new file;
   - rename it over `tree` and fsync the directory.
   Readers that opened the old file keep reading its unlinked inode, which stays
   consistent.

The swap runs where writes run, so no write can be lost between the last
catch-up and the rename. A crash before the rename leaves `tree.compacting`
behind, and the next start deletes it.

## Integration

- **Promotion:** at the threshold, apply builds the tree from the set's members
  (a bulk load), deletes the members' compound keys from the keydir and the
  shared Bitcask, and keeps the `PM:` marker. The marker records the encoding,
  `zset-tree-v1`, so type and lifecycle checks keep working.
- **Commands:**
  - `Commands.SortedSet` and `SortedSet.Reads` route a promoted set to the tree
    NIFs, and a small set to today's path. The reply formats are shared.
  - ZSCAN on a tree walks the member tree with a cursor of
    `{member, generation}`. A changed generation means the cursor restarts from
    the member, which Redis's SCAN guarantees allow.
- **Key lifecycle:** each of these acts on the marker and the file:
  - DEL and UNLINK delete the file;
  - EXPIRE stays on the marker;
  - RENAME and COPY rename or copy the file;
  - TYPE reports `zset`, and OBJECT ENCODING reports `skiplist` (the encoding
    name Redis clients expect for large sets);
  - MEMORY USAGE reports the marker plus the file size.
- **Reads off the apply path** call the read NIFs from the caller's process,
  like probabilistic reads today. No GenServer hop is added.
- **Promotion recovery** (`Promotion.recover_promoted/4`) no longer loads a
  tree-encoded set's members into ETS. It runs the tree's recovery instead.

## Rollout

New apply semantics need a feature level: a replayed entry must apply the same
way on every release, so apply may only change behavior behind a level every
node has accepted. Level 9 gates the new encoding:

1. **Engine.** The tree file, NIFs and recovery, with no command routed to them
   yet. Released when the tests below pass.
2. **Level 9, new promotions.** From level 9, a promotion builds a tree. Sets
   promoted earlier keep their dedicated Bitcask until they are next written.
3. **Conversion.** A write to a set promoted before level 9 converts it in apply
   first: a bulk load from its members, deterministic on every replica. An admin
   command converts the remaining sets in batches through Raft, so an idle large
   set doesn't keep its RAM footprint.
4. **Cleanup.** Once a shard has no dedicated-Bitcask zsets left, the old
   promoted-zset paths stop being reachable for it. They stay in the code for
   nodes below level 9.

Rollback: before level 9 is raised, nothing on disk changes. After it, an older
release can't read tree-encoded sets, as with every feature level.

## Testing

- **Model tests (Rust):** random operation sequences against a `BTreeMap`
  reference. Check every reply, the count invariants on every branch, the tree
  heights and the page CRCs.
- **Crash tests (Rust):**
  - drop any suffix of unsynced bytes;
  - reorder unsynced page writes;
  - tear a header slot or a trailer.

  Recovery must land on a commit-boundary prefix, and replaying the remaining
  ops by token must reach the model state.
- **Compaction tests:** compaction interleaved with commits and reads, and
  crashes at each step of the swap.
- **Elixir:**
  - the existing sorted-set command suite, run against promoted sets;
  - restart and replay idempotence;
  - snapshot install on a follower;
  - the level-9 conversion of an existing dedicated set;
  - DEL, UNLINK, RENAME, COPY and EXPIRE on tree-encoded sets.
- **Performance (regression tests):**
  - ZRANK and ZRANGE by index at 10,000 and 1,000,000 members within 3x of each
    other;
  - a paged full read of 7,200 members under 20 ms (0.73 s today);
  - ZADD apply cost per member within 2x of today's promoted path.

## Open questions

- **Page size:** 4 KiB matches the page cache. Larger leaves cut tree height but
  copy more bytes per commit. To be decided by the performance tests.
- **Promotion threshold for trees:** today's 100 may suit a tree as well, or a
  higher one may.
- **Hashes and sets:** whether they follow later with the uncounted variant.
