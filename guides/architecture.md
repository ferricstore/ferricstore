# Architecture

FerricStore is built around three core design principles: (1) all mutable state lives in Elixir where it is observable and debuggable, (2) Rust NIFs are pure stateless functions for I/O and CPU-intensive work, and (3) data is stored in the tier best suited to its access pattern.

## Overview

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         Client Layer                                       │
│  redis-cli / Redix / any Redis client          FerricStore Elixir API     │
│  (RESP3 over TCP/TLS)                          (direct function calls)    │
└──────────────────┬──────────────────────────────────┬─────────────────────┘
                   │                                  │
                   ▼                                  │
┌──────────────────────────────────┐                  │
│  FerricStore Server (standalone) │                  │
│  Ranch TCP/TLS → Connection     │                  │
│  → RESP3 Parser → ACL Check     │                  │
│  → Command Dispatcher           │                  │
│  → CLIENT TRACKING              │                  │
└──────────────────┬───────────────┘                  │
                   │                                  │
                   ▼                                  ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                         FerricStore Core                                   │
│                                                                            │
│  ┌────────────────────────────────────────────────┐                       │
│  │                    Router                       │                       │
│  │  key → phash2(key) band 0x3FF → slot            │                       │
│  │  slot → slot_map[slot] → idx                   │                       │
│  │  idx → :"Ferricstore.Store.Shard.N"            │                       │
│  └────────────────┬────────────────┬──────────────┘                       │
│                   │                │                                        │
│    ┌──────────────┴──┐  ┌─────────┴───────────┐                           │
│    │   Read Path      │  │    Write Path        │                          │
│    │   ETS direct     │  │    GenServer call    │                          │
│    │   (no GenServer) │  │    → ETS immediate   │                          │
│    │                  │  │    → Raft Batcher    │                          │
│    └──────┬───────────┘  │    → ra consensus    │                          │
│           │              │    → Bitcask append  │                          │
│           │              └────────────┬─────────┘                          │
│           │                           │                                    │
│  ┌────────┴───────────────────────────┴──────────────────────────┐        │
│  │                    Storage Tiers                                │        │
│  │                                                                 │        │
│  │  ┌─────────────────────────────────────────────────────────┐   │        │
│  │  │  Tier 1: ETS keydir (hot data)                          │   │        │
│  │  │  {key, value|nil, expire, lfu, file_id, offset, vsize}  │   │        │
│  │  │  One table per shard: keydir_0, keydir_1, ...            │   │        │
│  │  │  read_concurrency: true, write_concurrency: true         │   │        │
│  │  └────────────────────────────┬────────────────────────────┘   │        │
│  │                               │ cold read (value=nil)           │        │
│  │  ┌────────────────────────────▼────────────────────────────┐   │        │
│  │  │  Tier 2: Bitcask data files (cold data)                  │   │        │
│  │  │  Append-only log files, 256 MB rotation                  │   │        │
│  │  │  Rust NIF: v2_pread_at(path, offset)                     │   │        │
│  │  │  Background merge/compaction                             │   │        │
│  │  └─────────────────────────────────────────────────────────┘   │        │
│  │                                                                 │        │
│  │  ┌─────────────────────────────────────────────────────────┐   │        │
│  │  │  Tier 3: pread/pwrite files (probabilistic)               │   │        │
│  │  │  Bloom (.bloom), Cuckoo (.cuckoo), CMS (.cms)            │   │        │
│  │  │  TopK (.topk), TDigest (.tdig)                           │   │        │
│  │  │  OS page cache manages RAM, stateless NIF reads           │   │        │
│  │  └─────────────────────────────────────────────────────────┘   │        │
│  └─────────────────────────────────────────────────────────────────┘        │
│                                                                            │
│  ┌────────────────────┐  ┌──────────────┐  ┌──────────────────────────┐   │
│  │  Raft Consensus     │  │  MemoryGuard  │  │  Merge/Compaction        │   │
│  │  ra library         │  │  LFU eviction │  │  Background scheduler   │   │
│  │  per-shard group    │  │  per shard    │  │  Semaphore-gated        │   │
│  └────────────────────┘  └──────────────┘  └──────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
```

## Umbrella Structure

FerricStore is an Elixir umbrella application with two apps:

| App | Purpose | Key Modules |
|-----|---------|-------------|
| `ferricstore` | Core engine: shards, ETS, Bitcask, Raft, Rust NIFs, LFU, MemoryGuard | `Ferricstore.Store.{Shard, Router, LFU, Promotion}`, `Ferricstore.Raft.{Batcher, StateMachine, Cluster}`, `Ferricstore.Bitcask.{NIF, Async}`, `Ferricstore.MemoryGuard` |
| `ferricstore_server` | TCP/TLS server, RESP3 protocol, ACL, health HTTP | `FerricstoreServer.Connection`, `FerricstoreServer.Health` |

In **embedded mode**, only the `ferricstore` app starts. In **standalone mode**, `ferricstore_server` also starts Ranch TCP/TLS listeners and an HTTP health endpoint.

## Supervision Tree

```
Ferricstore.Supervisor (:one_for_one)
├── [Cluster.Supervisor]                  # libcluster node discovery (optional)
├── Ferricstore.Stats                     # Global counters (ETS :atomics), uptime
├── Ferricstore.SlowLog                   # Slow command ring buffer
├── Ferricstore.AuditLog                  # Security audit trail
├── Ferricstore.Config                    # Runtime CONFIG GET/SET (ETS-backed)
├── Ferricstore.NamespaceConfig           # Per-namespace commit window
├── Ferricstore.Acl                       # Access control lists (PBKDF2 passwords)
├── Ferricstore.HLC                       # Hybrid Logical Clock (for Raft timestamps)
├── Ferricstore.Raft.Batcher.0            # Group-commit batcher (shard 0)
├── Ferricstore.Raft.Batcher.1            # Group-commit batcher (shard 1)
├── ...                                   # (one Batcher per shard)
├── Ferricstore.Store.BitcaskWriter.0     # Background Bitcask writer (shard 0)
├── Ferricstore.Store.BitcaskWriter.1     # Background Bitcask writer (shard 1)
├── ...                                   # (one BitcaskWriter per shard)
├── Ferricstore.Store.ShardSupervisor     # Supervises N Shard GenServers
│   ├── Ferricstore.Store.Shard.0
│   ├── Ferricstore.Store.Shard.1
│   ├── ...
│   └── Ferricstore.Store.Shard.N-1   # N = System.schedulers_online()
├── Ferricstore.Merge.Supervisor          # Merge subsystem
│   ├── Ferricstore.Merge.Semaphore       # Node-level concurrency gate (capacity 1)
│   ├── Ferricstore.Merge.Scheduler.0     # Per-shard compaction scheduler
│   ├── Ferricstore.Merge.Scheduler.1
│   └── ...
├── Ferricstore.PubSub                    # Pub/Sub message routing
├── Ferricstore.FetchOrCompute            # Cache-aside stampede protection
└── Ferricstore.MemoryGuard               # Memory pressure monitor (100ms interval)

FerricstoreServer.Supervisor (:one_for_one)  [standalone mode only]
├── :pg (FerricstoreServer.PG)            # ACL invalidation process groups
├── Ranch TCP Listener                    # RESP3 connections
├── Ranch TLS Listener                    # Optional encrypted connections
└── Health HTTP Endpoint                  # /health/ready + /metrics + dashboard
```

### Startup Sequence

Before the supervision tree starts, `Application.start/2` performs critical initialization:

1. `DataDir.ensure_layout!(data_dir, shard_count)` -- creates the on-disk directory structure
2. `LFU.init_config_cache()` -- caches `lfu_decay_time` and `lfu_log_factor` in `persistent_term` (~5ns reads)
3. `persistent_term` initialization -- `hot_cache_max_value_size`, `keydir_full`, `reject_writes`, `shard_count`, `promotion_threshold`
4. `Waiters.init()`, `ClientTracking.init_tables()`, `Stream.init_tables()` -- ETS tables for blocking commands, client tracking, and streams
5. `install_patched_wal()` -- loads the patched `ra_log_wal` module with async fdatasync (pre-compiled `.beam` in release mode, runtime-compiled in dev)
6. `Raft.Cluster.start_system(data_dir)` -- starts the ra system (WAL directory under `data_dir/ra`)
7. Supervision tree starts: Stats -> SlowLog -> AuditLog -> Config -> NamespaceConfig -> HLC -> Batchers -> BitcaskWriters -> Flow LMDB writers -> ShardSupervisor -> Merge.Supervisor -> PubSub -> FetchOrCompute -> MemoryGuard

Stats starts first so counters are available before any connection. The ShardSupervisor must start before the Ranch listener (in the server app) so the key-value store is ready before any client arrives. MemoryGuard starts last because it reads from shard ETS tables.

## Shard Routing

Every key is mapped to a shard via a 1,024-slot indirection layer: `phash2(key) band 0x3FF` maps the key to one of 1,024 slots, then a `persistent_term` slot-map tuple (`slot_map[slot]`) maps the slot to a shard index. This is a pure, deterministic function -- no coordinator. The shard count defaults to `System.schedulers_online()` and is set at startup (determines maximum write parallelism).

Each shard has:
- An ETS table (`keydir_N`) for hot data
- A Bitcask data directory (`data_dir/shard_N/`) for persistent storage
- A Raft group (ra server `ferricstore_shard_N`) for consensus
- A group-commit batcher for write batching
- A prefix index ETS table for efficient SCAN/KEYS by prefix
- A merge scheduler for background compaction

The Router module (`Ferricstore.Store.Router`) pre-computes shard name atoms at startup via `Router.init_shard_names(shard_count)` for O(1) dispatch (~5ns via `elem/2` on a tuple vs ~300ns for string interpolation).

```elixir
# Router dispatches to the correct shard
def get(key) do
  idx = shard_for(key)              # phash2(key) band 0x3FF -> slot -> slot_map[slot] -> idx
  keydir = resolve_keydir(idx)      # pre-computed atom from persistent_term

  case ets_get(keydir, key, now) do
    {:hit, value, _exp} ->
      sampled_read_bookkeeping(keydir, key)  # LFU touch + hot stats, sampled
      value                                   # Hot path: no GenServer

    :miss ->
      Stats.record_cold_read(key)
      GenServer.call(resolve_shard(idx), {:get, key})  # Cold path: pread from disk
  end
end
```

## ETS Keydir

The ETS keydir is the single source of truth for all key-value data in RAM. Each entry is a 7-tuple:

```
{key, value | nil, expire_at_ms, lfu_counter, file_id, offset, value_size}
  |      |            |              |            |        |         |
  |      |            |              |            └────────┴─────────┘
  |      |            |              |            Disk location for cold reads
  |      |            |              |            (enables v2_pread_at without scanning)
  |      |            |              |
  |      |            |              └── LFU frequency counter
  |      |            |                  Packed u24: upper 16 bits = ldt minutes,
  |      |            |                              lower 8 bits  = log counter (0-255)
  |      |            |                  Probabilistic increment, time-based decay
  |      |            |
  |      |            └── Unix epoch ms, 0 = never expires
  |      |                Lazy eviction on read
  |      |
  |      └── Binary value (hot) or nil (cold/evicted)
  |          Values > hot_cache_max_value_size (default 64KB) stored as nil
  |          to avoid ETS binary copy overhead on every :ets.lookup
  |
  └── Binary key
```

**Hot vs Cold**: When `value` is a binary, the key is "hot" -- reads return directly from ETS with no GenServer roundtrip (~1-5us). When `value` is `nil`, the key is "cold" -- the `file_id`/`offset`/`value_size` fields tell the system exactly where to read from Bitcask via `NIF.v2_pread_at(path, offset)` (~50-200us on NVMe).

**ETS Table Options**: Each keydir table is created with `[:set, :public, :named_table, {:read_concurrency, true}, {:write_concurrency, true}]`. `:public` allows the Router to read directly from any process without going through the Shard GenServer. `:read_concurrency` enables lock-free concurrent readers on multi-core systems.

## Write Path

```
Client                Router              Shard GenServer         Raft Batcher
  |                     |                      |                       |
  |-- put(key, val) -->|                      |                       |
  |                     |-- GenServer.call -->|                       |
  |                     |                      |                       |
  |                     |  [Raft path]         |                       |
  |                     |                      |-- Batcher.write() -->|
  |                     |                      |                       |-- extract namespace
  |                     |                      |                       |   prefix from key
  |                     |                      |                       |-- lookup window_ms
  |                     |                      |                       |-- append to slot buffer
  |                     |                      |                       |-- start timer (1st write)
  |                     |                      |                       |
  |                     |                      |   (timer fires)       |
  |                     |                      |                       |
  |                     |                      |                       |-- :ra.process_command
  |                     |                      |                       |   (blocks until quorum)
  |                     |                      |                       |
  |                     |                      |  StateMachine.apply:  |
  |                     |                      |   NIF.v2_append_batch |
  |                     |                      |   :ets.insert(keydir) |
  |                     |                      |                       |
  |<-- :ok ------------|<-- :ok --------------|<-- result ------------|
  |                     |                      |                       |
```

### Write Path Details

**Raft write path (always active)**:

1. `Router.put/3` validates key/value size limits (max key: 64KB, max value: 512MB) and checks `keydir_full?()` via `persistent_term` (~5ns).
2. `Shard.handle_call({:put, key, value, expire_at_ms})` calls `Batcher.write(shard_index, {:put, key, value, expire_at_ms})`.
3. The Batcher extracts the namespace prefix from the key (text before the first `:`; keys without `:` go to `"_root"`). It looks up the namespace config for `window_ms` (commit window).
4. The command and caller are appended to the namespace's slot buffer. On the first write to an empty slot, a timer is started with `window_ms` (default: 1ms). If the slot reaches `max_batch_size` (default: 1000), it flushes immediately.
5. When the timer fires or the slot is full, the batch is submitted through Raft via `:ra.pipeline_command/4`. `StateMachine.apply/3` runs after the Raft entry is committed: it calls `NIF.v2_append_batch(active_file_path, records)` to write to Bitcask with fsync, then `:ets.insert(keydir, 7-tuple)` to update the keydir. Each caller receives their individual result after the applied notification arrives.

**Direct write path (embedded custom instances and sandbox test shards)**:

Custom embedded instances and sandbox test shards bypass the default Raft-backed application instance and write through their own Shard GenServers directly. This is not a standalone-server durability mode.

1. The Shard writes to ETS immediately via `:ets.insert` so reads see the value at once.
2. The entry is prepended to an in-memory `pending` list in the GenServer state.
3. If no flush is in-flight, `flush_pending/1` is called immediately. It calls `NIF.v2_append_batch_nosync(active_file_path, batch)` which writes to the OS page cache without fsync, then updates ETS entries with their disk locations (file_id, offset, value_size) via `:ets.update_element`.
4. A recurring timer fires every 1ms (`:flush_interval_ms`). When it fires, any accumulated pending entries are flushed, then `NIF.v2_fsync_async` is called to durably sync to disk. This amortizes fsync cost across all writes in the 1ms window.
5. If the pending list exceeds `@max_pending_size` (10,000 entries), a synchronous flush with fsync is forced to bound heap memory growth.

### File Rotation

When the active log file exceeds `@max_active_file_size` (8 GiB by default), `maybe_rotate_file/1`:
1. Writes a hint file for the current active file (for fast recovery)
2. Increments the file ID
3. Creates a new empty log file (e.g., `00001.log`)
4. Resets `active_file_size` to 0

Log file names are zero-padded to 5 digits: `00000.log`, `00001.log`, etc.

## Read Path

```
Client                Router              ETS keydir          Shard GenServer
  |                     |                     |                     |
  |-- get(key) ------->|                     |                     |
  |                     |-- :ets.lookup ---->|                     |
  |                     |                     |                     |
  |                     |  ┌──────────────────┘                     |
  |                     |  |                                        |
  |                     |  |  value != nil (HOT)                    |
  |                     |  |  sampled_read_bookkeeping(keydir, key) |
  |                     |  |  (1 persistent_term + 1 rand, sampled) |
  |<-- value ----------|<─┘  ~1-5us, no GenServer                 |
  |                     |                                           |
  |                     |  |  value == nil (COLD)                   |
  |                     |  |  Stats.record_cold_read(key)           |
  |                     |  |-- GenServer.call ─────────────────────>|
  |                     |  |                                        |-- NIF.v2_pread_at
  |                     |  |                                        |   (path, offset)
  |                     |  |                                        |-- warm ETS entry
  |<-- value ----------|<─┘<-- value ─────────────────────────────|
  |                     |       ~50-200us (NVMe)                    |
  |                     |                                           |
  |                     |  |  expired (TTL elapsed)                 |
  |                     |  |  :ets.delete(keydir, key)              |
  |<-- nil ------------|<─┘  lazy eviction on read                 |
```

### Read Path Details

`Router.get/1` reads directly from the ETS table without going through the Shard GenServer for hot keys. The function `ets_get/3`:

1. Calls `:ets.lookup(keydir, key)` -- lock-free with `read_concurrency: true`.
2. Pattern-matches the 7-tuple:
   - **Hot, no TTL**: `{key, value, 0, lfu, _, _, _}` when `value != nil` -- returns `{:hit, value, 0}`. The caller then invokes `sampled_read_bookkeeping/2` which performs a single `persistent_term` read + `rand` call, and only on the sampled fraction (default 1-in-100) does it call `LFU.touch/3` and `Stats.record_hot_read/1`.
   - **Hot, valid TTL**: `{key, value, exp, lfu, _, _, _}` when `exp > now` and `value != nil` -- returns `{:hit, value, exp}` (same sampled bookkeeping as above).
   - **Cold**: `{key, nil, ...}` -- returns `:miss`. The Router then calls `GenServer.call(shard, {:get, key})`, which performs `NIF.v2_pread_at(file_path, offset)` using the location from the ETS tuple. After reading, the value is warmed back into ETS (subject to `hot_cache_max_value_size`).
   - **Expired**: `{key, _, exp, _, _, _, _}` when `exp <= now` -- deletes the entry from ETS and returns `:expired`. This is lazy eviction: expired keys are cleaned up on access.
   - **Missing**: `[]` -- returns `:miss`.

3. Every read is recorded as hot or cold in `Ferricstore.Stats` for the `FERRICSTORE.HOTNESS` command and `INFO stats`.

### Sendfile Zero-Copy (Standalone Mode)

For cold keys over plain TCP (not TLS), `Router.get_file_ref/1` returns `{file_path, value_offset, value_size}` when the value exceeds the sendfile threshold (default: 64KB). The connection handler uses `:file.sendfile/5` to transfer data directly from disk to the TCP socket without copying through BEAM memory. The value offset is computed as `record_offset + 26 (header) + key_length`.

## Raft Consensus

FerricStore uses the [ra](https://hex.pm/packages/ra) library for Raft consensus. Each shard has its own independent Raft group with its own leader.

### Cluster Topology

- **Single-node mode** (development, testing): Each shard's Raft group has one member -- self quorum. Writes are durable after local WAL append + fsync. No network round trip.
- **Three-node cluster**: Each shard's Raft group has three members. Writes require quorum (2 of 3) acknowledgement before commit.

The ra system is named `:ferricstore_raft` and stores its WAL and segment files under `data_dir/ra`. Each shard's ra server is identified as `{:"ferricstore_shard_N", node()}`.

### Group Commit Batcher

`Ferricstore.Raft.Batcher` is a per-shard GenServer that accumulates write commands into per-namespace buffers:

1. Client calls `Batcher.write(shard_index, command)` -- synchronous `GenServer.call`.
2. The key's namespace prefix is extracted: `"session"` from `"session:abc123"`, `"_root"` for keys without a colon.
3. Namespace config is looked up from the `ns_cache` process-state map (populated lazily from the `:ferricstore_ns_config` ETS table managed by `NamespaceConfig`). Returns `window_ms`.
4. Commands are buffered in a slot keyed by prefix. A timer is started on the first write to each slot.
5. When the timer fires (or `max_batch_size` of 1000 is reached):
   - Single command -> `:ra.pipeline_command(shard_id, command, corr, :low)`.
   - Homogeneous hot batch -> `:ra.pipeline_command(shard_id, {:put_batch, entries}, corr, :low)` or `{:delete_batch, keys}` when the router can build the final shape directly.
   - Mixed batch -> `:ra.pipeline_command(shard_id, {:batch, commands}, corr, :low)`.
6. When `NamespaceConfig` changes (via `FERRICSTORE.CONFIG SET`), it broadcasts `:ns_config_changed` to all Batchers, which clear their `ns_cache`.

### Specialized Ra Command Terms

FerricStore uses compact Ra command terms for hot homogeneous write paths. The current examples are `{:put_batch, entries}` and `{:delete_batch, keys}`. They reduce allocation, Ra term size, serialization work, and per-command pattern matching compared with sending `{:batch, [{:put, ...}, ...]}` for the same request.

The rule for adding the next specialized term is strict:

1. Only specialize homogeneous write-only commands, or a deterministic bulk operation with identical semantics to the generic path.
2. Preserve the logical command count separately from the compact term so callers still receive one reply per input command.
3. Batch Bitcask work once and verify NIF result length/order before publishing public state.
4. For pure writes, stage disk records only, then publish ETS once after append succeeds.
5. Do not create temporary pending ETS rows or fill pending read maps unless later commands inside the same Ra entry must read intermediate state.
6. On append failure, no new public ETS state should remain visible. Tests must cover rollback of overwritten keys and absence of new keys.
7. Keep a runtime perf toggle while proving a new term, and benchmark it against the generic `{:batch, commands}` path plus any dedicated fast apply path disabled.

The `put_batch` profiling lesson was that the compact wire term was good, but the first apply implementation did too much extra pending-state work. The matching `delete_batch` fast path follows the same rule for tombstones: stage the Bitcask delete records first, then remove ETS rows only after append success. Future terms need both halves: compact Ra shape and compact state-machine apply.

### State Machine

`Ferricstore.Raft.StateMachine` implements the `:ra_machine` behaviour. Key callbacks:

- **`init/1`**: Receives shard config (paths, ETS table name, active file info). Stores `release_cursor_interval` (default: 1000) in machine state for deterministic cursor emission.
- **`apply/3`**: Deterministic command application. Supports `:put`, `:put_batch`, `:delete`, `:delete_batch`, `:batch`, `:list_op`, `:compound_put`, `:compound_delete`, `:compound_delete_prefix`, `:incr_float`, `:append`, `:getset`, `:getdel`, `:getex`, `:setrange`, `:cas`, `:lock`, `:unlock`, `:extend`, `:ratelimit_add`. Writes are appended to Bitcask before public ETS state is updated. Values exceeding `hot_cache_max_value_size` are stored as `nil` in ETS.
- **`state_enter/2`**: On becoming leader, calls `HLC.now()` to advance the local clock.
- **`overview/1`**: Returns debugging info: shard index, keydir size, applied count, cursor interval.

**Log Compaction**: Every `release_cursor_interval` applied commands, `apply/3` emits a `{:release_cursor, ra_index, state}` effect. This tells ra that all log entries up to that index are reflected in the snapshot and can be truncated.

**HLC Piggybacking**: Commands can be wrapped as `{inner_command, %{hlc_ts: {physical_ms, logical}}}`. When `apply/3` processes a wrapped command, it calls `HLC.update/1` to merge the leader's clock into the local node's HLC, keeping followers causally synchronized.

## LFU Eviction

FerricStore implements Redis-compatible LFU (Least Frequently Used) eviction with time-based decay.

### Packed Format

The LFU field in each keydir 7-tuple is a single integer packing two values into 24 bits:

```
[  16-bit ldt (last decrement time)  |  8-bit counter (0-255)  ]
     upper 16 bits                        lower 8 bits
```

- **ldt**: Minutes since epoch, masked to 16 bits (wraps every ~45 days). Used to compute elapsed time for decay.
- **counter**: Logarithmic frequency counter. New keys start at 5.

### Access Algorithm

On sampled key accesses (`LFU.touch/3`, called from `Router.sampled_read_bookkeeping/2` -- not every access, sampled at 1-in-N where N defaults to 100):

1. **Decay**: `elapsed = now_minutes - ldt`. Reduce counter by `elapsed / lfu_decay_time` (default: 1 minute per step, 0 disables decay).
2. **Probabilistic increment**: With probability `1 / (decayed_counter * lfu_log_factor + 1)` (default log_factor: 10), increment the counter by 1, capped at 255.
3. **Update ldt** to current minutes.
4. Write the new packed LFU value to ETS position 4 via `:ets.update_element/3`.

Config values are cached in `persistent_term` (~5ns) instead of `Application.get_env` (~200-250ns), saving ~400ns per hot GET.

### Effective Counter (for Eviction)

`LFU.effective_counter/1` computes the decayed counter without updating the stored value. Used by MemoryGuard eviction sorting and `OBJECT FREQ`.

### Eviction

When MemoryGuard reaches `:pressure` or `:reject` level and the policy is not `:noeviction`, it samples up to 10 hot entries (value != nil) per shard, sorts by effective counter (for LFU policies) or TTL (for `volatile_ttl`), and evicts the bottom 5 by setting their ETS value to `nil` via `:ets.update_element`. The key stays in the keydir with its disk location intact -- the next GET falls through to Bitcask and re-warms the entry.

## Memory Guard

`Ferricstore.MemoryGuard` is a GenServer that checks memory pressure every 100ms (configurable via `:memory_guard_interval_ms`).

### Pressure Levels

| Level | Threshold | Action |
|-------|-----------|--------|
| `:ok` | < 70% | Normal operation |
| `:warning` | 70-85% | Log warning |
| `:pressure` | 85-95% | Log error, begin eviction, emit telemetry |
| `:reject` | >= 95% | Log critical, evict aggressively, reject new key writes (with `:noeviction` policy) |

### Lock-Free Hot Path

MemoryGuard publishes two boolean flags to `persistent_term` on every check:
- `:ferricstore_keydir_full` -- true when keydir memory >= 95% of `keydir_max_ram`
- `:ferricstore_reject_writes` -- true when total memory >= 95% AND policy is `:noeviction`

`Router.put/3` and `Shard.handle_call({:put, ...})` read these flags via `persistent_term.get/1` (~5ns) instead of `GenServer.call` to MemoryGuard (~1-5us). This eliminates MemoryGuard as a contention point. The 100ms staleness window is acceptable since memory pressure changes slowly.

### Hot Cache Budget

MemoryGuard dynamically adjusts the hot cache budget based on pressure:
- `:ok` -> 50% of max_memory
- `:warn` -> 30%
- `:pressure` -> 15%
- `:full` -> 5%

Budget changes emit `[:ferricstore, :hot_cache, :limit_reduced]` and `[:ferricstore, :hot_cache, :limit_restored]` telemetry events.

## Collection Promotion

When a compound-key collection (hash, set, sorted set) exceeds the `promotion_threshold` (default: 100 entries), it is promoted from the shared shard Bitcask to a dedicated per-key Bitcask instance.

### Compound Key Encoding

Small collections store each field as a compound key in the shared shard:
- Hash fields: `H:redis_key\0field`
- Set members: `S:redis_key\0member`
- Sorted set members: `Z:redis_key\0member`
- List elements: `L:redis_key\0<position>`

### Promotion Process

1. `Promotion.promote_collection!/6` scans ETS for all compound keys matching the prefix.
2. Opens (or creates) a dedicated Bitcask directory at `data_dir/dedicated/shard_N/{type}:{sha256_of_key}/`.
3. Writes all entries to the dedicated Bitcask via `NIF.v2_append_batch`.
4. Writes tombstones to the shared Bitcask for the migrated keys.
5. Writes a marker entry `PM:redis_key` to the shared Bitcask (value = type string).
6. Entries **stay** in ETS so compound operations continue to work immediately.

### Recovery

On shard startup, `Promotion.recover_promoted/4` scans ETS for `PM:` marker keys (populated by `recover_keydir`), re-opens dedicated Bitcask directories, and scans their log files to recover entries into ETS.

### List Compound Keys

Lists use compound keys like other collection types:
- List elements: `L:redis_key\0<position>`

Each element is a separate Bitcask entry, making LPUSH/RPUSH O(1) per element instead of O(N). Position values encode the element's location in the list.

Lists are not promoted to dedicated Bitcask instances because the position-based compound key scheme already provides efficient per-element access.

## Merge / Compaction

Bitcask files are append-only and accumulate dead entries (overwritten or deleted keys). The merge subsystem compacts data files in the background.

### Architecture

```
Ferricstore.Merge.Supervisor (:one_for_one)
├── Ferricstore.Merge.Semaphore       # Node-level gate (capacity 1)
├── Ferricstore.Merge.Scheduler.0     # Per-shard, periodic check
├── Ferricstore.Merge.Scheduler.1
├── Ferricstore.Merge.Scheduler.2
└── Ferricstore.Merge.Scheduler.3
```

### Compaction Process

1. **Check**: Each scheduler periodically scans its shard's data directory for fragmentation (dead/total byte ratio).
2. **Acquire**: The scheduler acquires the node-level Semaphore to limit concurrent I/O.
3. **Select**: Identifies files exceeding the fragmentation threshold.
4. **Compact**: Collects live key offsets from ETS (`fid == target_file`), then calls `NIF.v2_copy_records(source, dest, offsets)` to copy live entries to a new file.
5. **Switch**: Renames the compacted file over the original (`File.rename!/2`).
6. **Release**: Releases the Semaphore.

### Hint Files

Each log file can have a corresponding `.hint` file (e.g., `00000.hint`). Hint files contain `{key, file_id, offset, value_size, expire_at_ms}` tuples written via `NIF.v2_write_hint_file/2`. On startup, the shard reads hint files first for fast keydir recovery (no value data to parse), then scans only unhinted log files.

## Recovery

On shard startup, `Shard.init/1` rebuilds the in-memory keydir:

1. **Discover active file**: Scans the shard data directory for `.log` files, finds the highest file ID and its size.
2. **Torn write recovery**: The active log file is truncated to the last valid CRC-checked record. Any partially-written bytes after a crash are discarded, ensuring that subsequent appends start from a clean boundary and no data is silently lost.
3. **Create ETS table**: `keydir_N` with `:set`, `:public`, `:named_table`, `read_concurrency`, `write_concurrency`.
4. **Recover keydir** (`recover_keydir/4`):
   - If `.hint` files exist: read them via `NIF.v2_read_hint_file/2` and populate ETS with cold entries (value=nil, disk location known). Then scan only unhinted log files.
   - If no hint files: full scan of all log files via `NIF.v2_scan_file/2`. For each record, insert or delete from ETS. Last-writer-wins (higher file_id + higher offset wins).
   - Entries recovered from hints/logs are inserted as cold: `{key, nil, expire_at_ms, LFU.initial(), fid, offset, value_size}`.
5. **Recover promoted collections**: Scans ETS for `PM:` marker keys, re-opens dedicated Bitcask directories, scans their logs to recover entries.
6. **Migrate prob files**: Scans prob directory for existing `.bloom`/`.cms`/`.cuckoo`/`.topk` files, writes metadata markers to ETS for any files without corresponding keydir entries.
7. **Start Raft server**: `Raft.Cluster.start_shard_server/5` starts the ra server for this shard. On supervisor restart after a shard crash, the existing Raft server is stopped and restarted with the same UID, preserving all committed WAL data (previous versions used `force_delete_server` which destroyed the WAL).
9. **Schedule flush timer and expiry sweep**.

## Rust NIF Design

Rust NIFs are **pure, stateless functions** -- no HashMap, no Mutex, no internal state for the v2 API. Elixir owns all mutable state (ETS keydir, GenServer). For mmap-backed structures, Rust NIFs use NIF resources (reference-counted by the BEAM GC).

### v2 Pure Stateless File I/O

All v2 functions take a file **path** (not a Store resource) as their first argument:

```
v2_append_record(path, key, value, expire_at_ms) -> {:ok, {offset, record_size}}
v2_append_tombstone(path, key) -> {:ok, {offset, record_size}}
v2_append_batch(path, records) -> {:ok, [{offset, size}, ...]}    # write + fsync
v2_append_batch_nosync(path, records) -> {:ok, [{offset, size}, ...]}  # page cache only
v2_pread_at(path, offset) -> {:ok, value | nil}
v2_pread_batch(path, locations) -> {:ok, [value | nil, ...]}
v2_fsync(path) -> :ok
v2_scan_file(path) -> {:ok, [{key, offset, value_size, expire_at_ms, is_tombstone}, ...]}
v2_write_hint_file(path, entries) -> :ok
v2_read_hint_file(path) -> {:ok, [{key, file_id, offset, value_size, expire_at_ms}, ...]}
v2_copy_records(source_path, dest_path, offsets) -> {:ok, [{old_offset, new_offset}, ...]}
```

### Tokio Async I/O

For non-blocking operations, v2 async NIFs submit work to a Tokio runtime thread pool and send results back as messages with correlation IDs:

```
v2_pread_at_async(caller_pid, corr_id, path, offset) -> :ok
  # sends {:tokio_complete, corr_id, :ok | :error, result}

v2_pread_batch_async(caller_pid, corr_id, locations) -> :ok
v2_fsync_async(caller_pid, corr_id, path) -> :ok
v2_append_batch_async(caller_pid, corr_id, path, records) -> :ok
```

Correlation IDs (monotonically increasing integers from `System.unique_integer/1`) prevent LIFO ordering bugs when multiple async operations are in flight. The `Bitcask.Async` module wraps these in `receive` blocks with a 5-second timeout.

### On-Disk Record Format

```
[ crc32: u32 | timestamp_ms: u64 | expire_at_ms: u64 | key_size: u16 | value_size: u32 | key: [u8] | value: [u8] ]
  4 bytes      8 bytes             8 bytes              2 bytes         4 bytes           variable    variable
```

Header size: 26 bytes. CRC32 covers everything after the checksum field. Tombstone records have `value_size = u32::MAX` (0xFFFFFFFF) and no value bytes. A `value_size` of 0 indicates a valid empty string value (`SET key ""`). All integers are little-endian. The I/O backend is selected at startup: `io_uring` on Linux kernel >= 5.1, `BufWriter<File>` otherwise.

### Stateless pread/pwrite Structures

Probabilistic structures use stateless file-based NIFs. Each NIF opens the file, reads/writes specific bytes via pread/pwrite, and closes on return. No mmap, no ResourceArc, no Mutex. Memory stays in kernel page cache (managed by OS).

| Structure | File Extension | NIFs |
|-----------|---------------|------|
| Bloom Filter | `.bloom` | `bloom_file_create`, `bloom_file_add`, `bloom_file_madd`, `bloom_file_exists`, `bloom_file_mexists`, `bloom_file_card`, `bloom_file_info` |
| Cuckoo Filter | `.cuckoo` | `cuckoo_file_create`, `cuckoo_file_add`, `cuckoo_file_addnx`, `cuckoo_file_del`, `cuckoo_file_exists`, `cuckoo_file_count`, `cuckoo_file_info` |
| Count-Min Sketch | `.cms` | `cms_file_create`, `cms_file_incrby`, `cms_file_query`, `cms_file_info`, `cms_file_merge` |
| TopK | `.topk` | `topk_file_create_v2`, `topk_file_add_v2`, `topk_file_incrby_v2`, `topk_file_query_v2`, `topk_file_list_v2`, `topk_file_count_v2`, `topk_file_info_v2` |
| TDigest | `.tdig` | (in-memory ResourceArc — pending migration to stateless) |

Write commands route through Raft for replication. Read commands use stateless pread NIFs directly on the local file. Files live at `shard_data_path/prob/BASE64_KEY.ext`.

### The "Should This Be in Rust?" Test

1. Is it CPU-intensive? (hash, fingerprint, CMS counters) -- **Rust**
2. Is it a syscall wrapper? (pread, pwrite, fsync) -- **Rust**
3. Is it file I/O on binary layouts? (bloom bits, CMS counters, cuckoo buckets) -- **Rust stateless NIF**
4. Does it have application state? (keydir, routing, scheduling) -- **Elixir**
5. Does it make decisions? (eviction, batching, consensus) -- **Elixir**
6. Does it need debugging in production? -- **Elixir**

### NIF Scheduling

NIFs run on the Normal BEAM scheduler with cooperative yielding via `enif_schedule_nif`. Large operations (batch writes, file scans, hint file I/O) use the `enif_schedule_nif` pattern to yield back to the BEAM scheduler between chunks, preventing scheduler starvation without consuming dirty scheduler threads. On NVMe, individual I/O operations complete in ~50-200us -- fast enough that the normal scheduler handles them without jitter.

## Connection Handling (Standalone Mode)

Each TCP connection is a Ranch protocol handler (`FerricstoreServer.Connection`) that:

1. Performs the Ranch handshake (`ranch.handshake/1`)
2. Enforces `require_tls` -- rejects plaintext connections if configured
3. Checks protected mode -- rejects non-localhost when no ACL passwords are set
4. Enters an event-driven receive loop (configurable via `:socket_active_mode`, default `active: true`)

### Sliding Window Pipeline

Connections support pipelined commands with concurrent dispatch:

- All "pure" commands (those that don't mutate connection state) in a pipeline batch are dispatched concurrently as `Task`s.
- Responses are sent in-order: response N is sent as soon as responses 0..N are all complete. Fast commands before a slow command get delivered immediately.
- Stateful commands (MULTI, AUTH, SUBSCRIBE, blocking ops) act as barriers: all prior concurrent tasks are awaited and flushed before the stateful command executes synchronously.

### RESP Pipeline Write Batching

Phase 1 batches only consecutive RESP pipeline runs of safe single-key writes that already have direct Raft state-machine commands. The connection keeps response order intact, preserves read boundaries, and falls back to the existing command path for anything outside the allowlist.

Included in phase 1:

- Plain `SET`
- `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`
- `APPEND`, `SETRANGE`, `SETBIT`
- `HINCRBY`, `HINCRBYFLOAT`, `ZINCRBY`
- `PFADD`
- JSON mutations: `JSON.SET`, `JSON.DEL`, `JSON.NUMINCRBY`, `JSON.ARRAPPEND`, `JSON.TOGGLE`, `JSON.CLEAR`

Explicitly excluded:

- `GETSET`, `GETDEL`, `GETEX`, and any other write command whose reply depends on returning prior state.
- Multi-key commands and cross-shard commands.
- Compound writes without a direct Raft command conversion, including `HSET`, `SADD`, `ZADD`, list/stream mutations, `MSET`, and `DEL`.
- Commands inside `MULTI` or commands requiring per-command ACL checks.

Phase 2 can expand the allowlist when needed. Good candidates are same-shard `MSET`, same-shard single-entry compound writes (`HSET`, `SADD`, `ZADD`) after exact type/error semantics are audited, and `DEL`/`UNLINK` only if the batched path can preserve Redis integer-delete replies for plain and compound keys.

### Per-Connection State

Each connection maintains:
- **Multi/transaction state**: `:none` or `:queuing` mode, queued commands, watched keys with shard write versions.
- **ACL context**: Cached user permissions (commands, key patterns, enabled flag).
- **Pub/Sub subscriptions**: Channel and pattern subscription sets.
- **Client tracking config**: For cache invalidation broadcasts.

### Transaction Support (MULTI/EXEC/DISCARD/WATCH)

When `MULTI` is issued, the connection enters `:queuing` mode. Subsequent commands (except EXEC, DISCARD, MULTI, WATCH, UNWATCH) are queued with `+QUEUED` responses. `EXEC` executes all queued commands sequentially. If `WATCH` was used and any watched key's shard write-version changed, `EXEC` returns nil (transaction aborted). `DISCARD` clears the queue.

### Input Validation

Protocol-level limits prevent resource exhaustion from malicious or malformed input:

| Input | Limit | Enforced At |
|-------|-------|-------------|
| RESP array elements | 1,000,000 | RESP3 parser |
| Inline command size | 1 MB | RESP3 parser |
| INCR/DECRBY values | i64 range (-2^63 to 2^63-1) | Command handler |
| SETRANGE offset | 512 MB | Command handler |
| SUBSCRIBE/PSUBSCRIBE per connection | 100,000 | Connection state |
| SETBIT offset | 2^32 - 1 | Command handler |
| Glob patterns (KEYS, SCAN) | 1,024 bytes | Command handler (CVE-2022-36021 mitigation) |

These limits apply in both standalone and embedded mode. The RESP parser limits are checked before command dispatch, so oversized payloads are rejected without allocating memory for the full payload.

## Three-Tier Storage

### Tier 1: ETS (Hot Data)

Key-value data (strings, hashes, lists, sets, sorted sets) lives in ETS when frequently accessed:

- Lock-free concurrent reads (`read_concurrency: true`)
- Atomic `update_element` for LFU counter and disk location updates
- Lazy expiry on read (expired entries deleted when accessed)
- Active expiry sweep every 1 second per shard (configurable)
- ~1-5us read latency

### Tier 2: Bitcask (Cold Data)

Bitcask is an append-only log-structured storage engine. When values are evicted from ETS (LFU eviction) or exceed `hot_cache_max_value_size`, they are stored on disk and the ETS entry holds `nil` for the value but retains `{file_id, offset, value_size}` for direct pread:

- Append-only writes (fast, sequential)
- Point reads via pread at known offset -- no scanning
- Background merge/compaction removes dead entries
- Hint files for fast startup recovery
- File rotation at 256 MB

### Tier 3: Stateless pread/pwrite (Probabilistic)

Probabilistic data structures use stateless file-based NIFs with pread/pwrite. Each NIF opens the file, operates, and closes on return. Data stays in OS page cache:

| Structure | File Extension | Access Pattern |
|-----------|---------------|----------------|
| Bloom Filter | `.bloom` | Random bit set/check |
| Cuckoo Filter | `.cuckoo` | Bucket array, fingerprint ops |
| Count-Min Sketch | `.cms` | Counter matrix, hash-indexed increment |
| TopK | `.topk` | CMS + min-heap |
| TDigest | `.tdig` | Sorted centroid array |

Write commands replicate through Raft. Read commands bypass Raft and use stateless pread NIFs on local files. Zero process memory — the OS page cache handles caching.

## Telemetry Events

FerricStore emits telemetry events for observability:

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:ferricstore, :node, :startup_complete]` | `duration_ms` | `shard_count`, `port`, `mode` |
| `[:ferricstore, :node, :shutdown_started]` | `uptime_ms` | -- |
| `[:ferricstore, :memory, :check]` | `total_bytes` | `pressure_level`, `ratio`, `max_bytes` |
| `[:ferricstore, :memory, :pressure]` | `total_bytes`, `max_bytes`, `ratio` | `level` (:ok, :warn, :pressure, :full) |
| `[:ferricstore, :memory, :recovered]` | `total_bytes`, `max_bytes`, `ratio` | `previous_level` |
| `[:ferricstore, :memory, :keydir_pressure]` | `keydir_bytes`, `keydir_max_ram`, `keydir_ratio` | `keydir_pressure_level` |
| `[:ferricstore, :hot_cache, :limit_reduced]` | `new_budget_bytes`, `old_budget_bytes` | `level`, `shard_count` |
| `[:ferricstore, :hot_cache, :limit_restored]` | `new_budget_bytes`, `old_budget_bytes` | `level`, `shard_count` |
| `[:ferricstore, :config, :changed]` | -- | `param`, `value`, `old_value` |
| `[:ferricstore, :embedded, :large_values_detected]` | `count`, `largest_size` | `largest_key` |
| `[:ferricstore, :shard, :shutdown]` | `flush_duration_us`, `hint_duration_us`, `total_duration_us` | `shard_index` |
| `[:ferricstore, :async_apply, :batch]` | `duration_us`, `batch_size` | `shard_index` |

## Graceful Shutdown

When the application stops:

1. `prep_stop/1` marks the node as not ready (`Health.set_ready(false)`) so Kubernetes stops routing traffic.
2. Emits `[:ferricstore, :node, :shutdown_started]` telemetry.
3. Supervisor stops children in reverse start order.
4. Each shard's `terminate/2`:
   - Awaits any in-flight async fsync
   - Flushes all pending writes synchronously to disk
   - Writes a hint file for the active log file
   - Calls `NIF.v2_fsync(active_file_path)` for final durability
   - Emits `[:ferricstore, :shard, :shutdown]` telemetry with timing
