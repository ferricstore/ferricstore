# Architecture

This guide explains how FerricStore works internally. Read it after [Getting Started](getting-started.md) if you want to understand routing, durability, storage, recovery, memory behavior, and the native TCP server.

Quick model:

```text
client command -> shard router -> WARaft segment log -> state/apply projection -> hot keydir/indexes
```

FerricStore owns the storage path itself; workflow state is not written through an external Postgres, Cassandra, or FerricStore database. The committed WARaft segment log is the durable command boundary. Committed commands are projected into per-shard ETS keydirs and native Flow indexes for the hot serving path. LMDB/history are query projections that can lag briefly.

For FerricFlow, command correctness uses the current Flow state in the hot keydir/native indexes. That state is recoverable from FerricStore-managed WARaft segment/apply-projection storage. LMDB/history projection freshness affects query surfaces, not whether a Flow command committed.

FerricStore is built around three boundaries: (1) the BEAM owns routing, sessions, ACL, Raft orchestration, ETS keydir state, and operational decisions, (2) Rust NIFs handle bounded binary protocol work, file I/O, and CPU-heavy primitives, with selected native resources for specialized indexes, and (3) data is stored in the tier best suited to its access pattern.

## Overview

```text
Clients
  Native SDKs / native clients
    -> native binary TCP/TLS
    -> Ranch listener
    -> FerricstoreServer.Native.Connection
       - Native.Codec + native-protocol Rust NIF frame scan
       - protected mode, maxclients, ACL
       - control lane, data lanes, backpressure, chunking
       - Native.Commands / NativeAstParser

  Embedded Elixir callers
    -> FerricStore API

Core engine
  Command handlers / Router
    -> shard_for(key): phash2(key) band 0x3FF -> slot_map[slot] -> shard idx
    -> hot reads: ETS keydir direct lookup
    -> writes: shard GenServer -> WARaft batcher -> committed segment/apply projection

Storage
  Tier 0: WARaft segment/apply projection storage
    durable command boundary and restart/replay source

  Tier 1: ETS keydir
    {key, value | nil, expire_at_ms, lfu, file_id, offset, value_size}

  Tier 2: Bitcask/blob files
    append-only cold values, large values, dedicated collections, merge/compaction

  Tier 3: stateless pread/pwrite files
    Bloom, Cuckoo, CMS, TopK, TDigest query structures backed by OS page cache

Operations
  Raft consensus, MemoryGuard, LFU eviction, merge scheduling, Pub/Sub, Flow events
```

Standalone and embedded mode converge at the command handler and router boundary. The native TCP protocol owns transport, framing, multiplexing, ACL enforcement, and response shaping; command correctness and storage semantics stay in the core engine.

## Umbrella Structure

FerricStore is an Elixir umbrella application with two apps:

| App | Purpose | Key Modules |
|-----|---------|-------------|
| `ferricstore` | Core engine: shards, ETS, Bitcask, Raft, Rust NIFs, LFU, MemoryGuard | `Ferricstore.Store.{Shard, Router, LFU, Promotion}`, `Ferricstore.Raft.{Batcher, StateMachine, Cluster}`, `Ferricstore.Bitcask.{NIF, Async}`, `Ferricstore.MemoryGuard` |
| `ferricstore_server` | Native TCP/TLS server, ACL, client registry, health HTTP | `FerricstoreServer.Native.Connection`, `FerricstoreServer.Native.Codec`, `FerricstoreServer.Health` |

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
├── FerricstoreServer.Acl                 # Server-side ACL state and invalidation hooks
├── Ranch TCP Listener                    # Native protocol connections
├── Ranch TLS Listener                    # Optional encrypted native connections
├── Dashboard/Metrics HTTP Endpoint       # /dashboard + /metrics + legacy health routes
└── Isolated Health Probe Endpoint        # /health/live + /health/ready
```

### Startup Sequence

Before the supervision tree starts, `Application.start/2` performs critical initialization:

1. `DataDir.ensure_layout!(data_dir, shard_count)` -- creates the on-disk directory structure
2. `LFU.init_config_cache()` -- caches `lfu_decay_time` and `lfu_log_factor` in `persistent_term` (~5ns reads)
3. `persistent_term` initialization -- `hot_cache_max_value_size`, `keydir_full`, `reject_writes`, `shard_count`, `promotion_threshold`
4. `Waiters.init()` and `Stream.init_tables()` -- ETS tables for blocking commands and streams
5. `WARaftBackend.start(default_ctx)` -- starts WARaft partitions with the segment log under `data_dir/waraft`
6. Supervision tree starts: Stats -> SlowLog -> AuditLog -> Config -> NamespaceConfig -> HLC -> WARaft namespace batchers -> BitcaskWriters -> Flow LMDB writers -> ShardSupervisor -> Merge.Supervisor -> PubSub -> FetchOrCompute -> MemoryGuard

Stats starts first so counters are available before any connection. The ShardSupervisor must start before the Ranch listener (in the server app) so the key-value store is ready before any client arrives. In standalone mode, the server app starts ACL invalidation groups before accepting native TCP/TLS clients. MemoryGuard starts last because it reads from shard ETS tables.

## Shard Routing

Every key is mapped to a shard via a 1,024-slot indirection layer: `phash2(key) band 0x3FF` maps the key to one of 1,024 slots, then a `persistent_term` slot-map tuple (`slot_map[slot]`) maps the slot to a shard index. This is a pure, deterministic function -- no coordinator. The shard count defaults to `System.schedulers_online()` and is set at startup (determines maximum write parallelism).

Each shard has:
- An ETS table (`keydir_N`) for hot data
- A Bitcask data directory (`data_dir/shard_N/`) for persistent storage
- A WARaft partition (`raft_server_ferricstore_waraft_backend_N`) for consensus
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

The write path below starts after a standalone native frame has been decoded,
authorized, and dispatched, or after an embedded caller has invoked the Elixir
API directly.

```
Client                Router              Shard GenServer         WARaft Batcher
  |                     |                      |                       |
  |-- put(key, val) -->|                      |                       |
  |                     |-- GenServer.call -->|                       |
  |                     |                      |                       |
  |                     |  [WARaft path]       |                       |
  |                     |                      |-- Batcher.write() -->|
  |                     |                      |                       |-- extract namespace
  |                     |                      |                       |   prefix from key
  |                     |                      |                       |-- lookup window_ms
  |                     |                      |                       |-- append to slot buffer
  |                     |                      |                       |-- start timer (1st write)
  |                     |                      |                       |
  |                     |                      |   (timer fires)       |
  |                     |                      |                       |
  |                     |                      |                       |-- WARaftBackend.write
  |                     |                      |                       |   (blocks until quorum)
  |                     |                      |                       |
  |                     |                      |  StateMachine.apply:  |
  |                     |                      |   segment/apply       |
  |                     |                      |   projection          |
  |                     |                      |   keydir/index publish|
  |                     |                      |                       |
  |<-- :ok ------------|<-- :ok --------------|<-- result ------------|
  |                     |                      |                       |
```

### Write Path Details

**WARaft write path (always active)**:

1. `Router.put/3` validates key/value size limits (max key: 64KB, max value: 512MB) and checks `keydir_full?()` via `persistent_term` (~5ns).
2. `Shard.handle_call({:put, key, value, expire_at_ms})` calls `Batcher.write(shard_index, {:put, key, value, expire_at_ms})`.
3. The Batcher extracts the namespace prefix from the key (text before the first `:`; keys without `:` go to `"_root"`). It looks up the namespace config for `window_ms` (commit window).
4. The command and caller are appended to the namespace's slot buffer. On the first write to an empty slot, a timer is started with `window_ms` (default: 1ms). If the slot reaches `max_batch_size` (default: 1000), it flushes immediately.
5. When the timer fires or the slot is full, the batch is submitted through `WARaftBackend`. `StateMachine.apply/3` runs after the WARaft entry is committed. Fast segment-projectable commands use the committed WARaft segment as the stored value location and publish ETS rows with `{:waraft_segment, index}` locations. Full state-machine commands stage an apply-projection batch and publish rows with `{:waraft_apply_projection, index}` locations; large values may go through the blob side channel. Each caller receives its individual result after the applied notification arrives.

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

### Native Response Framing

Standalone responses always return through the native frame encoder. Hot values
come from ETS. Cold values are read from the Bitcask/blob location stored in
the ETS keydir, then encoded as native response frames. The native connection
can coalesce adjacent responses and can split large responses into chunks using
`native_response_coalesce_max`, `native_response_coalesce_bytes`, and
`native_response_chunk_bytes`. Response chunking is always capped by the
connection's advertised maximum frame size; a configured value of `0` selects
that frame-size cap automatically.

Large cold values still return through native frames, so request-id
correlation, lane ordering, TLS parity, compact response payloads, and bounded
chunk memory all use the same response machinery.

## WARaft Consensus

FerricStore uses WARaft for Raft consensus. Each shard has its own independent WARaft partition with its own leader.

### Cluster Topology

- **Single-node mode** (development, testing): Each shard's WARaft partition has one member -- self quorum. Writes are durable after local segment append + fsync. No network round trip.
- **Three-node cluster**: Each shard's WARaft partition has three members. Writes require quorum (2 of 3) acknowledgement before commit.

The WARaft system is named `:ferricstore_waraft_backend` and stores segment files under `data_dir/waraft`. Each shard's server is identified as `{:"raft_server_ferricstore_waraft_backend_N", node()}`.

### Group Commit Batcher

`Ferricstore.Raft.WARaftBackend.Batcher` is a per-shard GenServer that accumulates write commands into per-namespace buffers:

1. Client calls `Batcher.write(shard_index, command)` -- synchronous `GenServer.call`.
2. The key's namespace prefix is extracted: `"session"` from `"session:abc123"`, `"_root"` for keys without a colon.
3. Namespace config is looked up from the `ns_cache` process-state map (populated lazily from the `:ferricstore_ns_config` ETS table managed by `NamespaceConfig`). Returns `window_ms`.
4. Commands are buffered in a slot keyed by prefix. A timer is started on the first write to each slot.
5. When the timer fires (or `max_batch_size` of 1000 is reached):
   - Single command -> `WARaftBackend.write(shard_index, command)`.
   - Homogeneous hot batch -> `WARaftBackend.write_put_batch/2` or `write_delete_batch/2` when the router can build the final shape directly.
   - Mixed batch -> `WARaftBackend.write_batch(shard_index, commands)`.
6. When `NamespaceConfig` changes (via `FERRICSTORE.CONFIG SET`), it broadcasts `:ns_config_changed` to all Batchers, which clear their `ns_cache`.

### Specialized WARaft Command Terms

FerricStore uses compact WARaft command terms for hot homogeneous write paths. The current examples are `{:put_batch, entries}` and `{:delete_batch, keys}`. They reduce allocation, log term size, serialization work, and per-command pattern matching compared with sending `{:batch, [{:put, ...}, ...]}` for the same request.

The rule for adding the next specialized term is strict:

1. Only specialize homogeneous write-only commands, or a deterministic bulk operation with identical semantics to the generic path.
2. Preserve the logical command count separately from the compact term so callers still receive one reply per input command.
3. Batch storage/projection work once and verify result length/order before publishing public state.
4. For pure writes, stage segment/apply-projection records only, then publish ETS once after the durable projection succeeds.
5. Do not create temporary pending ETS rows or fill pending read maps unless later commands inside the same WARaft entry must read intermediate state.
6. On projection failure, no new public ETS state should remain visible. Tests must cover rollback of overwritten keys and absence of new keys.
7. Keep a runtime perf toggle while proving a new term, and benchmark it against the generic `{:batch, commands}` path plus any dedicated fast apply path disabled.

The `put_batch` profiling lesson was that the compact wire term was good, but the first apply implementation did too much extra pending-state work. The matching `delete_batch` fast path follows the same rule for tombstones: stage the durable projection first, then remove ETS rows only after projection success. Future terms need both halves: compact log shape and compact state-machine apply.

### Future KV Native-Batch Work

The highest-value KV native optimization is durable batched SET, not hot GET. Hot GET already routes through `Router.get/2` to a direct ETS lookup for resident values, and a per-command NIF call would likely give back much of the gain. Durable SET still pays per-entry BEAM costs in routing, compact term handling, state-machine staging, segment/apply-projection validation, and ETS/keydir publishing.

The next native experiment should keep the current Elixir-owned ETS keydir model and move only the batch apply/publish work behind a single Rust NIF boundary:

1. Accept the already-routed `{:put_batch, entries}` shape for one shard.
2. Convert values to the durable segment/apply-projection representation and persist the whole batch once.
3. Validate projection result count, order, offsets, and value sizes before publishing.
4. Return enough compact location data for the state machine to publish final ETS rows with no visible partial writes, or publish through a carefully audited native helper that preserves the same ETS tuple contract.
5. Keep Flow policy/value mirror hooks, blob externalization, large-value hot-cache thresholds, and rollback semantics equivalent to the current path.

A Rust-owned KV keydir should be treated as a larger architecture change, not a first optimization step. It must prove that it beats ETS for concurrent hot reads, avoids one mutex bottleneck per shard, preserves lazy expiry and LFU bookkeeping semantics, and does not weaken crash recovery. Benchmarks should compare the current path, the native batch path, and any Rust-keydir prototype under a native-protocol SDK benchmark shape before replacing the ETS-backed design.

### State Machine

`Ferricstore.Raft.StateMachine` is the deterministic WARaft apply module. Key callbacks:

- **`init/1`**: Receives shard config (paths, ETS table name, active file info). Stores `release_cursor_interval` (default: 1000) in machine state for deterministic cursor emission.
- **`apply/3`**: Deterministic command application. Supports `:put`, `:put_batch`, `:delete`, `:delete_batch`, `:batch`, `:list_op`, `:compound_put`, `:compound_delete`, `:compound_delete_prefix`, `:incr_float`, `:append`, `:getset`, `:getdel`, `:getex`, `:setrange`, `:cas`, `:lock`, `:unlock`, `:extend`, `:ratelimit_add`. In the standalone WARaft path, segment-projected commands use WARaft segment locations and full apply commands use apply-projection locations before public ETS state is updated. Direct embedded/test paths can still append through Bitcask before publishing ETS. Values exceeding `hot_cache_max_value_size` are stored as `nil` in ETS.
- **`state_enter/2`**: On becoming leader, calls `HLC.now()` to advance the local clock.
- **`overview/1`**: Returns debugging info: shard index, keydir size, applied count, cursor interval.

**Log Compaction**: Every `release_cursor_interval` applied commands, `apply/3` emits a `{:release_cursor, ra_index, state}` effect. This tells WARaft that all log entries up to that index are reflected in the snapshot and can be truncated.

**HLC Piggybacking**: Commands can be wrapped as `{inner_command, %{hlc_ts: {physical_ms, logical}}}`. When `apply/3` processes a wrapped command, it calls `HLC.update/1` to merge the leader's clock into the local node's HLC, keeping followers causally synchronized.

**Replicated Apply Context**: Policy-sensitive Flow commands carry a compact, versioned `ApplyContext` captured from the immutable instance configuration before Raft submission. Apply reads retention, cleanup, and hibernation limits from that command context rather than node-local application or process configuration. The latest applied context is stored in machine state and WARaft recovery/snapshot metadata. Ordinary KV commands keep their existing command shape and allocation path.

### Flow Policy Ordering And Migration

Flow policy storage has an internal monotonic generation that is separate from the public `version` field. Policy-sensitive Flow commands capture the generation and normalized policy they observed before entering a shard's Raft log. Batch commands deduplicate those snapshots by Flow type. Apply uses the captured policy for lifecycle and retry decisions, so replicas converge even when the policy Raft group and a Flow shard group are applied in different relative orders.

Generation allocation is always enabled. Policy fan-out uses one cross-shard command and one generation-bearing persisted envelope; there is no rollout mode or alternate write path.

Policy-derived index projection uses a generation high-water in the durable per-Flow type catalog. This makes projection updates commutative: a delayed command may retain its captured lifecycle semantics, but it cannot replace projection state installed by a newer policy generation. Catalog entries retain exact type ownership rather than relying on an in-memory prefix scan.

Policy changes enqueue resumable migration work instead of synchronously scanning a shard keydir. A bounded worker first stages authoritative primary keydir membership, including registry-derived cold states, and then plans bounded catalog-member batches outside Raft apply. Each replicated migration command contains explicit, size-bounded record candidates, guards, and the policy generation; apply validates that plan without reading replica-local LMDB or selecting work from a native index. Catalog membership remains non-expiring until explicit state cleanup. Durable cursor/source metadata lets ordinary restarts resume and forces a safe restart after destructive source replacement.

### Flow Limit Reservation Storage

Limit owners persist fixed-size counters; exact reservation IDs live in detached
pages of 256 entries. IDs carry the lease epoch used by their spend command.
When all reservations in an epoch are released, apply advances the epoch and
queues the old pages for bounded cleanup, so an old command remains fenced even
after its tombstones are deleted. A scope may retain at most 256 pages in one
active epoch and 256 pending cleanup tasks; new spends receive backpressure at
those bounds instead of growing storage without limit.

## LFU Eviction

FerricStore implements LFU (Least Frequently Used) eviction with time-based decay.

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
- Hash fields: `H:user_key\0field`
- Set members: `S:user_key\0member`
- Sorted set members: `Z:user_key\0member`
- List elements: `L:user_key\0<position>`

### Promotion Process

1. `Promotion.promote_collection!/6` scans ETS for all compound keys matching the prefix.
2. Opens (or creates) a dedicated Bitcask directory at `data_dir/dedicated/shard_N/{type}:{sha256_of_key}/`.
3. Writes all entries to the dedicated Bitcask via `NIF.v2_append_batch`.
4. Writes tombstones to the shared Bitcask for the migrated keys.
5. Writes a marker entry `PM:user_key` to the shared Bitcask (value = type string).
6. Entries **stay** in ETS so compound operations continue to work immediately.

### Recovery

On shard startup, `Promotion.recover_promoted/4` scans ETS for `PM:` marker keys (populated by `recover_keydir`), re-opens dedicated Bitcask directories, and scans their log files to recover entries into ETS.

### List Compound Keys

Lists use compound keys like other collection types:
- List elements: `L:user_key\0<position>`

Each element is a separate Bitcask entry, making LPUSH/RPUSH O(1) per element instead of O(N). Position values encode the element's location in the list.

Lists are not promoted to dedicated Bitcask instances because the position-based compound key scheme already provides efficient per-element access.

## Merge / Compaction

Bitcask files are append-only and accumulate dead entries (overwritten or deleted keys). The merge subsystem compacts data files in the background.

### Merge Supervision

This section explains the merge subsystem that keeps append-only storage compact over time.

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
7. **Start quorum runtime**: application startup owns WARaft partition startup. A shard GenServer restart reuses the same keydir and active-file state while WARaft keeps the committed segment log durable.
8. **Schedule flush timer and expiry sweep**.

## Rust NIF Design

The main storage path keeps routing, keydir ownership, scheduling, consensus,
and operational decisions in Elixir. Rust NIFs are used where they give a clear
boundary advantage: native frame scanning/encoding, file I/O, CRC/layout work,
and CPU-heavy data-structure primitives. The v2 file I/O API is stateless; for
specialized indexes or mmap-backed structures, Rust can expose NIF resources
that are reference-counted by the BEAM GC.

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

Most probabilistic structures use stateless file-based NIFs. Each stateless NIF opens the file, reads/writes specific bytes via pread/pwrite, and closes on return. Memory stays in kernel page cache. TDigest still uses an in-memory native resource and is tracked separately from the stateless file-backed structures.

| Structure | File Extension | NIFs |
|-----------|---------------|------|
| Bloom Filter | `.bloom` | `bloom_file_create`, `bloom_file_add`, `bloom_file_madd`, `bloom_file_exists`, `bloom_file_mexists`, `bloom_file_card`, `bloom_file_info` |
| Cuckoo Filter | `.cuckoo` | `cuckoo_file_create`, `cuckoo_file_add`, `cuckoo_file_addnx`, `cuckoo_file_del`, `cuckoo_file_exists`, `cuckoo_file_count`, `cuckoo_file_info` |
| Count-Min Sketch | `.cms` | `cms_file_create`, `cms_file_incrby`, `cms_file_query`, `cms_file_info`, `cms_file_merge` |
| TopK | `.topk` | `topk_file_create_v2`, `topk_file_add_v2`, `topk_file_incrby_v2`, `topk_file_query_v2`, `topk_file_list_v2`, `topk_file_count_v2`, `topk_file_info_v2` |
| TDigest | `.tdig` | In-memory native resource, pending migration to stateless file-backed storage |

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

Each standalone client connection is a Ranch protocol handler
(`FerricstoreServer.Native.Connection`). The only standalone data plane is the
Ferric native binary protocol.

Connection startup:

1. Performs the Ranch handshake (`ranch.handshake/1`).
2. Enforces `require_tls` when configured.
3. Checks protected mode and `maxclients`.
4. Creates a connection state with client id, ACL cache, socket settings,
   frame limits, lane limits, chunk limits, and idle timeout.
5. Registers the client in `FerricstoreServer.Connection.Registry`.
6. Enters an event-driven receive loop.

Native frame shape:

```text
magic(4) version(1) flags(1) lane_id(4) opcode(2) request_id(8) body_len(4) body(N)
```

The Rust native-protocol NIF scans frames and emits frame headers/bodies. It
does not own sessions, ACL state, storage state, or command semantics. Elixir
owns socket/TLS handling, authorization, lane scheduling, command dispatch, and
response coalescing.

### Lanes And Multiplexing

Native clients can pipeline requests with stable `request_id` correlation.
Ordering is preserved within one lane; different lanes can execute
concurrently.

```text
lane 0   control requests and server events
lane N   ordered data stream, usually mapped to shard_id + 1
```

The server bounds per-connection inflight requests, per-lane inflight requests,
lane queue size, pending chunks, and pending chunk bytes. A closed window or
full queue returns `busy` instead of letting one client consume unbounded
memory.

### Command Forms

Dedicated native opcodes exist for hot KV, Flow, cluster, and admin commands.
The protocol also has `COMMAND_EXEC` for generic command execution:

```text
{"command": "SET", "args": ["key", "value"]}
{"command": "FLOW.CLAIM_DUE", "args": ["email", "STATE", "queued", "WORKER", "w1"]}
```

`Ferricstore.Commands.NativeAstParser` converts generic native command bodies
into the same internal AST shape used by embedded command handlers. The native
protocol separates command name and arguments; it does not parse inline text
frames.

`Ferricstore.Commands.PreparedCommand` is the shared result of that parse. It
contains the normalized AST plus ACL keys, an explicit routing scope, routing
keys, and separate read/write mutation footprints. Native dispatch, same-shard
pipeline validation, blocking and session handling, transaction
reauthorization, and `MULTI` shard planning reuse the same immutable value so
no stage can reinterpret a command's keys differently. Coordinated Flow and
global control commands are rejected by execution modes that promise
single-shard semantics. Legacy parser callers receive a compatibility tuple
derived from the prepared value.

### Per-Connection State

Each native connection maintains:

- **Multi/transaction state**: `:none` or `:queuing`, queued commands, and watched keys.
- **ACL context**: cached command, key, and channel permissions.
- **Pub/Sub subscriptions**: channel and pattern subscription sets.
- **Flow wake/event subscriptions**: optional server events on lane 0.
- **Backpressure state**: inflight counters, lane queues, chunk buffers, and close-after-reply flags.

### Transaction And Blocking Support

`MULTI`/`EXEC`/`DISCARD`/`WATCH` are session commands carried by
`COMMAND_EXEC`. During a transaction, the server requires `COMMAND_EXEC` frames
until `EXEC` or `DISCARD` so queued commands have a single session-level shape.

Blocking list/stream commands are handled by the native blocking path and keep
the request correlated to its original lane and request id. Embedded mode does
not expose blocking waits because there is no persistent client socket to hold.

### Input Validation

Protocol-level and command-level limits prevent resource exhaustion from
malicious or malformed input:

| Input | Limit | Enforced At |
|-------|-------|-------------|
| Native frame body | `native_max_frame_bytes` (max 128 MiB minus header) | Native frame decoder |
| Connection receive buffer | 128 MiB incomplete input, plus at most 64 KiB after a complete first frame | `Native.Connection.FrameBuffer` |
| Pending request chunks | `native_max_pending_chunks` | `Native.Connection` |
| Pending chunk bytes | `native_max_pending_chunk_bytes` | `Native.Connection` |
| Lanes per connection | `native_max_lanes_per_connection` | `Native.Connection` |
| Lane queue length | `native_lane_max_queue` | Native lane scheduler |
| Value size | `max_value_size` | Native body validation and embedded API |
| INCR/DECRBY values | i64 range (-2^63 to 2^63-1) | Command handler |
| SETRANGE offset | 512 MB | Command handler |
| SUBSCRIBE/PSUBSCRIBE per connection | 100,000 | Native session state |
| SETBIT offset | 2^32 - 1 | Command handler |
| Glob patterns (KEYS, SCAN) | 1,024 bytes | Command handler |

These command limits apply in both standalone and embedded mode. Native frame
limits are checked before command dispatch, so oversized payloads are rejected
without executing a command.

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
- File rotation at `@max_active_file_size` (8 GiB by default)

### Tier 3: Stateless pread/pwrite (Probabilistic)

Probabilistic data structures use stateless file-based NIFs with pread/pwrite. Each NIF opens the file, operates, and closes on return. Data stays in OS page cache:

| Structure | File Extension | Access Pattern |
|-----------|---------------|----------------|
| Bloom Filter | `.bloom` | Random bit set/check |
| Cuckoo Filter | `.cuckoo` | Bucket array, fingerprint ops |
| Count-Min Sketch | `.cms` | Counter matrix, hash-indexed increment |
| TopK | `.topk` | CMS + min-heap |
| TDigest | `.tdig` | Sorted centroid array |

Write commands replicate through Raft. Read commands bypass Raft and use stateless pread NIFs on local files. The OS page cache handles caching instead of process heap storage.

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
