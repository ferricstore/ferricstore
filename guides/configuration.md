# Configuration Reference

FerricStore is configured through Elixir's standard `config/config.exs` (compile-time) and `config/runtime.exs` (runtime). Some parameters can also be changed at runtime via the `CONFIG SET` Redis command.

> **Mode applicability.** Each option is tagged with its scope:
>
> - **Both** — applies to standalone and embedded mode
> - **Standalone only** — ignored in embedded mode (no TCP listener, no connections)
> - **Embedded only** — only relevant when running as an in-process library

## Mode

| Option | Type | Default | Applies to | Description |
|--------|------|---------|------------|-------------|
| `:mode` | `:standalone \| :embedded` | `:standalone` | Both | Operational mode. `:standalone` starts TCP/TLS listeners and HTTP health endpoint. `:embedded` starts only the core engine. |

```elixir
config :ferricstore, :mode, :embedded
```

## Network

*Standalone only — these options are ignored in embedded mode.*

| Option | Type | Default | Applies to | Description |
|--------|------|---------|------------|-------------|
| `:port` | `integer` | `6379` | Standalone only | TCP port for the RESP3 listener. Use `0` for an OS-assigned ephemeral port (useful in tests). |
| `:health_port` | `integer` | `4000` | Standalone only | HTTP port for health checks and Prometheus metrics. Use `0` for ephemeral. |
| `:socket_active_mode` | `:once \| true \| integer` | `true` | Standalone only | TCP socket active mode. `:once` reads one message at a time (back-pressure friendly). `true` pushes all data without flow control (highest throughput). An integer N reads N messages then switches to passive. |

The TCP listener also sets the following socket options (not configurable at runtime):

| Socket Option | Value | Description |
|---------------|-------|-------------|
| `nodelay` | `true` | Disable Nagle's algorithm for low-latency responses |
| `recbuf` | `65_536` | Receive buffer size (64 KB) |
| `sndbuf` | `65_536` | Send buffer size (64 KB) |
| `backlog` | `1024` | TCP listen backlog |
| `keepalive` | `true` | Enable TCP keepalive to detect dead connections |

```elixir
config :ferricstore, :port, 6379
config :ferricstore, :health_port, 4000
```

## Storage

> See [Architecture Guide — Three-Tier Storage](architecture.md#three-tier-storage)
> for how ETS, Bitcask, and mmap work together.

| Option | Type | Default | Applies to | Description |
|--------|------|---------|------------|-------------|
| `:data_dir` | `string` | `"data"` | Both | Root directory for Bitcask data files, WARaft segment logs, mmap structures, and hint files. Each shard gets a subdirectory. |
| `:shard_count` | `integer` | `0` (auto) | Both | Number of shards. `0` = auto-detect via `System.schedulers_online()`. Each shard is a separate ETS table, Bitcask directory, and WARaft partition. More shards = more write parallelism but more file descriptors. Set at startup. |
| `:hot_cache_max_value_size` | `integer` | `65_536` | Both | Maximum value size (bytes) stored in ETS hot cache. Values larger than this are stored as `nil` in ETS and read from Bitcask on access. Prevents large binaries from being copied on every ETS lookup. |
| `:blob_side_channel_threshold_bytes` | `integer` | `262_144` | Both | Values at or above this size are stored in per-shard append blob segments with a small Bitcask reference. `0` disables the blob side channel. |
| `:blob_gc_sweeper_enabled` | `boolean` | `true` | Both | Enables automatic conservative cleanup of stale tmp and legacy side-channel blob files. Append-segment record compaction is a separate maintenance path. |
| `:blob_gc_sweeper_initial_delay_ms` | `integer` | `60_000` | Both | Delay before the first automatic blob GC sweep. |
| `:blob_gc_sweeper_interval_ms` | `integer` | `600_000` | Both | Interval between automatic blob GC sweeps. Each tick first checks blob storage stats and skips the keydir scan when no reclaimable legacy blob files or tmp files exist. |
| `:max_active_file_size` | `integer` | `8_589_934_592` | Both | Maximum Bitcask active file size before rotation. Larger defaults reduce high-throughput rollover tail latency; lower this if reclaim granularity is more important than p99 write latency. |

```elixir
config :ferricstore, :data_dir, "data"
config :ferricstore, :shard_count, 0  # 0 = auto-detect from CPU cores
config :ferricstore, :hot_cache_max_value_size, 65_536
config :ferricstore, :blob_side_channel_threshold_bytes, 256 * 1024
config :ferricstore, :blob_gc_sweeper_enabled, true
config :ferricstore, :blob_gc_sweeper_interval_ms, 600_000
config :ferricstore, :max_active_file_size, 8 * 1024 * 1024 * 1024
```

## Supervisor

| Option | Type | Default | Applies to | Description |
|--------|------|---------|------------|-------------|
| `:supervisor_max_restarts` | `{integer, integer}` | `{20, 10}` | Both | `{max_restarts, max_seconds}` for the shard supervisor. Controls how many shard restarts are allowed within the time window before the supervisor itself shuts down. Increase for test suites that deliberately kill shards. |

```elixir
# Production default: 20 restarts in 10 seconds
config :ferricstore, :supervisor_max_restarts, {20, 10}

# Test config: generous budget for shard-kill tests
config :ferricstore, :supervisor_max_restarts, {1000, 60}
```

## Memory Management

| Option | Type | Default | Applies to | Description |
|--------|------|---------|------------|-------------|
| `:max_memory_bytes` | `integer` | `nil` | Both | Maximum total ETS memory budget in bytes. When unset, no memory limit is enforced. |
| `:eviction_policy` | `atom` | `:volatile_lru` | Both | Eviction policy when memory limit is reached. See policies below. |
| `:memory_guard_interval_ms` | `integer` | `100` | Both | How often (ms) MemoryGuard checks memory pressure. |
| `:keydir_max_ram` | `integer` | `268_435_456` | Both | Maximum RAM for ETS keydir metadata in bytes (default: 256 MB). |
| `:hot_cache_max_ram` | `integer` | derived | Both | Maximum RAM for hot cache values. Default: `max_memory_bytes - keydir_max_ram`, minimum 64 MB. |
| `:hot_cache_min_ram` | `integer` | `67_108_864` | Both | Minimum RAM for hot cache values in bytes (default: 64 MB). |
| `:maxclients` | `integer` | `10_000` | Standalone only | Maximum simultaneous client connections. Ignored in embedded mode (no TCP connections). |
| `:promotion_threshold` | `integer` | `100` | Both | Field/member count threshold for promoting collections (hash/set/zset) to dedicated Bitcask instances. |
| `:acl_auto_save` | `boolean` | `false` | Both | Auto-save ACL changes to disk when modified via `ACL SETUSER` or `ACL DELUSER`. |
| `:read_sample_rate` | `integer` | `100` | Both | LFU/stats sampling rate. `1` = update counters on every read, `100` = sample 1% of reads. Higher values reduce per-read overhead at the cost of LFU accuracy. |

### Eviction: How It Works

> See [Architecture Guide — LFU Eviction](architecture.md#lfu-eviction) and
> [Architecture Guide — Memory Guard](architecture.md#memory-guard) for
> implementation details.

> **Defaults out of the box:**
>
> | Setting | Default | Meaning |
> |---------|---------|---------|
> | `:max_memory_bytes` | `nil` (no limit) | Eviction is **disabled** until you set this. Without a limit, ETS grows unbounded. |
> | `:eviction_policy` | `:volatile_lru` | When eviction triggers, only keys with a TTL are candidates. Least-frequently-accessed go first. |
> | `:lfu_decay_time` | `1` minute | Counter decays by 1 every minute — keys that stop being accessed lose priority within minutes. |
> | `:lfu_log_factor` | `10` | Moderate increment curve. Matches Redis default. |
> | `:memory_guard_interval_ms` | `100` ms | Memory checked 10 times per second. |
>
> **If you set nothing**, eviction never happens and RAM grows until the OS kills the process.
> At minimum, set `:max_memory_bytes` in production.

FerricStore has a three-tier storage model:

```
                 ┌─────────────────────────┐
  Tier 1 (HOT)  │  ETS (RAM)              │  ~1-5 us reads
                 │  key → value in memory  │
                 └────────────┬────────────┘
                              │ eviction moves value
                              │ out of RAM (key stays)
                 ┌────────────▼────────────┐
  Tier 2 (COLD) │  ETS (RAM) metadata     │  ~50-200 us reads
                 │  key → nil (disk loc)   │  (pread from disk)
                 └────────────┬────────────┘
                              │ always persisted
                 ┌────────────▼────────────┐
  Tier 3 (DISK) │  Bitcask (append-only)  │  source of truth
                 │  all keys, all values   │
                 └─────────────────────────┘
```

**Eviction does NOT delete data.** When a key is evicted, its value is removed
from RAM (Tier 1 → Tier 2) but it stays on disk. The ETS entry keeps the key
and disk location (`file_id`, `offset`) so the next read can fetch it. The key
goes from "hot" (~1-5 us) to "cold" (~50-200 us) — slower but still accessible.

**Expiration DOES delete data.** When a key's TTL expires, the key is
permanently removed from both ETS and Bitcask (a tombstone is written to disk).
The key is gone — `GET` returns nil, `EXISTS` returns 0. This happens two ways:
lazy deletion on the next read, and active sweep every 100ms (configurable).
This is the same behavior as Redis `EXPIRE`.

When RAM usage reaches `max_memory_bytes`, MemoryGuard must choose which hot
keys to make cold. This involves two things:

1. **LFU scoring** — a counter on each key that tracks how often it's accessed
2. **Eviction policy** — a filter that decides which keys are *eligible*

They are **not separate systems** — they work together:

```
  RAM full → MemoryGuard triggers
         │
         ▼
  ┌──────────────────────────┐
  │  1. FILTER by policy     │  ← eviction policy controls this
  │     volatile_lru: only   │
  │       keys with TTL      │
  │     allkeys_lru: all     │
  │       keys               │
  │     volatile_ttl: only   │
  │       keys with TTL      │
  │     noeviction: nobody   │
  │       (reject writes)    │
  └────────────┬─────────────┘
               ▼
  ┌──────────────────────────┐
  │  2. SORT by score        │  ← LFU counter controls this
  │     volatile_lru: lowest │    (except volatile_ttl which
  │       LFU score first    │     sorts by shortest TTL)
  │     allkeys_lru: lowest  │
  │       LFU score first    │
  │     volatile_ttl: shortest│
  │       remaining TTL first│
  └────────────┬─────────────┘
               ▼
  ┌──────────────────────────┐
  │  3. EVICT                │
  │     Set value to nil in  │
  │     ETS. Key stays, disk │
  │     location preserved.  │
  └──────────────────────────┘
```

### Eviction Policies

| Policy | Which keys can be evicted? | How are victims chosen? | Use when... |
|--------|---------------------------|------------------------|-------------|
| `:volatile_lru` | Only keys **with a TTL** | Lowest LFU score (least accessed) | You have two types of data: permanent (no TTL) that must stay hot, and cache entries (with TTL) that can go cold. **Default. Best for most apps.** |
| `:allkeys_lru` | **Any key** | Lowest LFU score (least accessed) | All keys are cache entries, or you don't care which keys go cold — let the access pattern decide. Best RAM utilization. |
| `:volatile_ttl` | Only keys **with a TTL** | Shortest remaining TTL (expiring soonest) | TTL duration = importance. A 5-min cache entry should be evicted before a 24-hour session. Does **not** use LFU scores. |
| `:noeviction` | **None** | N/A — writes are rejected with OOM error | Your dataset fits in RAM and you'd rather fail writes than serve cold reads. |

> **Quick decision guide:**
>
> - "I have cache data (with TTL) and permanent data (no TTL)" → `:volatile_lru`
> - "Everything is cache data" or "I don't use TTLs" → `:allkeys_lru`
> - "Short-lived keys should go first" → `:volatile_ttl`
> - "My data fits in RAM, never evict" → `:noeviction`
>
> **If you're unsure, use `:volatile_lru`** (the default). Set a TTL on cache
> entries and leave permanent data without TTL.

### LFU Scoring (the counter behind `volatile_lru` and `allkeys_lru`)

Every key in ETS has an LFU counter — a packed 24-bit integer stored in the
7-tuple alongside the key and value. This counter answers: "how important is
this key based on recent access patterns?"

```
  ┌─────────────────────────────────────┐
  │  24-bit LFU counter                 │
  │  ┌──────────────┬──────────────┐    │
  │  │ ldt (16 bit) │ counter (8b) │    │
  │  │ last decay   │ log access   │    │
  │  │ timestamp    │ frequency    │    │
  │  └──────────────┴──────────────┘    │
  └─────────────────────────────────────┘
```

**How it works:**

1. **On every read** (GET, HGET, etc.), the counter is **probabilistically
   incremented**. "Probabilistically" means: the higher the counter already is,
   the harder it is to increment. A key accessed 1 million times won't have a
   much higher counter than one accessed 10,000 times — this prevents popular
   keys from monopolizing RAM forever.

2. **Over time, the counter decays.** Every `:lfu_decay_time` minutes (default: 1),
   the counter drops by 1. A key that was hot an hour ago but hasn't been
   accessed since will have a low score — even if it had millions of historical
   accesses. This is what makes it *adaptive*: access patterns change, and the
   counter follows.

3. **On eviction**, MemoryGuard compares the **effective counter** (after
   applying time decay) across candidate keys. The key with the lowest score
   gets evicted first.

**The two LFU config knobs:**

| Option | Default | What it controls | Tuning guidance |
|--------|---------|-----------------|-----------------|
| `:lfu_decay_time` | `1` | Minutes between decay steps. Lower = faster decay = more responsive to changing access patterns. `0` disables decay entirely. | Leave at `1` unless your workload has very slow access cycles (e.g., keys accessed once per hour — use `10` or higher). |
| `:lfu_log_factor` | `10` | How hard it is to increment the counter. Higher = harder to saturate = better differentiation between moderately and extremely hot keys. | Leave at `10` for most workloads. Use `100` if you have extreme hotspot keys (millions of reads/sec on a few keys) and want to distinguish them from merely popular keys. |

```elixir
config :ferricstore, :lfu_decay_time, 1
config :ferricstore, :lfu_log_factor, 10
```

> These match Redis's `lfu-decay-time` and `lfu-log-factor` exactly — same
> algorithm, same defaults, same behavior.

### Memory Pressure Thresholds

MemoryGuard monitors ETS memory at configurable intervals and takes action at
three pressure levels. All thresholds are relative to `max_memory_bytes`:

| Level | Threshold | What happens |
|-------|-----------|--------------|
| Normal | < 70% | Nothing. Writes proceed normally. |
| Warning | 70% | Logs a warning. No action taken — this is an early heads-up. |
| Pressure | 85% | Begins eviction. MemoryGuard samples keys from ETS and evicts the lowest-scored ones according to the eviction policy. Emits `:ferricstore.memory_pressure` telemetry event. |
| Reject | 95% | With `:noeviction` policy: new writes are rejected with `-OOM` error. With other policies: eviction continues aggressively. |

If `max_memory_bytes` is not set, MemoryGuard is disabled and no eviction or
rejection occurs. This is fine for development but **not recommended for
production** — an unbounded ETS can eventually exhaust system RAM.

```elixir
config :ferricstore, :max_memory_bytes, 4_294_967_296  # 4 GB
config :ferricstore, :eviction_policy, :allkeys_lru
config :ferricstore, :memory_guard_interval_ms, 100
```

## Raft Consensus & Durability

> See [Architecture Guide — Raft Consensus](architecture.md#raft-consensus) for
> the full write path, state machine internals, and batcher design.

Raft controls **write durability**: client writes are acknowledged only after
the Raft entry is committed and applied through the durable write path. The
old per-namespace `durability` switch was removed; namespaces now tune only
the group-commit window.

### What Raft does

Every write follows this path:

```
  Client writes SET key value
         │
         ▼
  Batcher accumulates writes     ← groups writes for throughput
         │
         ▼
  ra (Raft library) commits      ← blocks until quorum confirms
         │                          (single node = self-quorum)
         ▼
  v2_append_batch + fsync        ← persisted to disk
         │
         ▼
  ETS updated                    ← reads see committed value
         │
         ▼
  Client gets OK                 ← crash-safe at this point
```

The default namespace commit window is 1ms. You can tune that window per
namespace with `FERRICSTORE.CONFIG SET <prefix> window_ms <n>`.

### How Raft log replication works (multi-node)

In a multi-node cluster, one node is the **leader** and the others are
**followers**. All writes go to the leader.

```
  Client: SET key value
         │
         ▼
  Leader node
    1. Appends write to its local Raft log
    2. Sends AppendEntries RPC to all followers (in parallel)
    3. Followers write to their local Raft log and acknowledge
    4. Once majority (2 of 3) have acknowledged → committed
    5. Leader replies OK to client
    6. Leader applies the write (ETS + Bitcask)
    7. Followers apply the write on their side
```

Replication happens **continuously** as writes arrive — the leader streams new
log entries to followers, piggybacked on heartbeats (every ~100-500ms). There
is no separate "replication phase."

**If a follower is down or slow**, it misses some entries. When it comes back
online, the leader detects the gap and sends all missing entries in bulk. The
follower replays them in order and catches up. This is automatic — no manual
intervention needed.

**Single-node mode** (the common case): the leader is its own quorum. Writes
are committed as soon as they're fsynced locally. No network round-trips.

Configure per-namespace at runtime:

```bash
# Session data: 1ms batch window (default)
redis-cli FERRICSTORE.CONFIG SET session window_ms 1

# Analytics counters: 5ms batch window
redis-cli FERRICSTORE.CONFIG SET analytics window_ms 5

# Reset to defaults
redis-cli FERRICSTORE.CONFIG RESET analytics
```

Or in Elixir config:

```elixir
config :ferricstore, :namespace_config, %{
  "session" => %{window_ms: 1},
  "analytics" => %{window_ms: 5}
}
```

## Connection Limits

Safety limits that prevent a single connection from consuming unbounded memory.
These protect against oversized payloads, slow-drip buffer attacks, and
runaway transactions or pipelines.

| Option | Type | Default | Hard Cap | Applies to | Description |
|--------|------|---------|----------|------------|-------------|
| `:max_value_size` | `integer` | `1_048_576` (1 MB) | `67_108_864` (64 MB) | Both | Maximum bulk string (value) size in bytes. Enforced at RESP3 parse time and in embedded `FerricStore.set/3`. The hard cap of 64 MB is non-configurable. |
| Buffer limit | compile-time | `134_217_728` (128 MB) | — | Standalone only | Maximum bytes accumulated in a connection's receive buffer before the connection is closed. Not configurable at runtime (module attribute in `Connection`). |
| MULTI queue limit | compile-time | `100_000` | — | Standalone only | Maximum commands queued inside a `MULTI` transaction. When exceeded, the transaction is auto-discarded and an error is returned. |
| Pipeline batch limit | compile-time | `100_000` | — | Standalone only | Maximum commands in a single pipeline batch. When exceeded, the entire batch is rejected with an error. |

### Max value size

Applied at two levels:

1. **RESP3 parser** — when a bulk string header `$N\r\n` declares a length exceeding the limit, the parser returns an error immediately without reading the body. The connection receives `-ERR value too large (N bytes, max M bytes)` and is closed.
2. **Embedded API** — `FerricStore.set/3` checks `byte_size(value)` before writing and returns `{:error, "ERR value too large ..."}` if the value exceeds the limit.

```elixir
# Allow values up to 10 MB (capped at 64 MB hard limit)
config :ferricstore, :max_value_size, 10_485_760
```

### Buffer, MULTI queue, and pipeline limits

These are compile-time module attributes in `FerricstoreServer.Connection` and
cannot be changed at runtime. The defaults are generous enough for any
legitimate workload while preventing pathological resource exhaustion:

- **Buffer overflow** — a client sending incomplete RESP frames faster than
  they can be parsed will accumulate data in the receive buffer. At 128 MB the
  connection is closed with `-ERR connection buffer overflow`.
- **MULTI queue overflow** — a client queueing more than 100K commands inside
  `MULTI` triggers an automatic `DISCARD` with `-ERR MULTI queue overflow`.
- **Pipeline overflow** — a pipeline batch with more than 100K commands is
  rejected with `-ERR pipeline batch too large`.

## Sendfile Zero-Copy

*Standalone only — embedded mode returns values directly via ETS or `v2_pread_at`, with no socket involved.*

| Option | Type | Default | Applies to | Description |
|--------|------|---------|------------|-------------|
| `:sendfile_threshold` | `integer` | `65_536` | Standalone only | Minimum value size (bytes) for sendfile zero-copy. Values larger than this are served via `:file.sendfile/5` instead of copying into BEAM memory. Only applies to cold (on-disk) keys over plain TCP. |

```elixir
config :ferricstore_server, :sendfile_threshold, 65_536
```

## TLS

*Standalone only — embedded mode has no network listener so TLS does not apply.*

> See [Security Guide — TLS](security.md#tls) for certificate setup and mutual
> TLS configuration.

| Option | Type | Default | Applies to | Description |
|--------|------|---------|------------|-------------|
| `:tls_port` | `integer` | `nil` | Standalone only | TLS port to bind. Not started unless configured. |
| `:tls_cert_file` | `string` | `nil` | Standalone only | Path to PEM certificate file. |
| `:tls_key_file` | `string` | `nil` | Standalone only | Path to PEM private key file. |
| `:tls_ca_cert_file` | `string` | `nil` | Standalone only | Path to CA certificate bundle (optional, for mutual TLS). |
| `:require_tls` | `boolean` | `false` | Standalone only | When `true`, reject plaintext TCP connections. |

```elixir
config :ferricstore, :tls_port, 6380
config :ferricstore, :tls_cert_file, "/etc/ssl/ferricstore/cert.pem"
config :ferricstore, :tls_key_file, "/etc/ssl/ferricstore/key.pem"
config :ferricstore, :tls_ca_cert_file, "/etc/ssl/ferricstore/ca.pem"
config :ferricstore, :require_tls, true
```

## Merge / Compaction

> See [Architecture Guide — Merge / Compaction](architecture.md#merge--compaction)
> for the compaction lifecycle, semaphore, and file selection.

Bitcask files are append-only and accumulate dead entries over time. The merge scheduler periodically compacts data files by copying live entries to new files and discarding the old ones.

| Option | Type | Default | Applies to | Description |
|--------|------|---------|------------|-------------|
| `:merge.check_interval_ms` | `integer` | varies | Both | How often the merge scheduler checks for fragmentation. |
| `:merge.fragmentation_threshold` | `float` | `0.5` | Both | Fragmentation ratio (dead/total) above which a merge is triggered. |

```elixir
config :ferricstore, :merge,
  check_interval_ms: 60_000,
  fragmentation_threshold: 0.5
```

## Expiry Sweep

| Option | Type | Default | Applies to | Description |
|--------|------|---------|------------|-------------|
| `:expiry_sweep_interval_ms` | `integer` | `1_000` | Both | How often each shard scans for expired keys. See below. |

When you set a TTL on a key (`SET key value EX 60` or `FerricStore.set("key", "value", ttl: 60_000)`),
the key doesn't disappear the instant it expires. FerricStore uses two complementary strategies to
clean up expired keys — just like Redis:

**1. Lazy expiry (on read):** When any command reads a key, it checks
`expire_at_ms`. If the key is past its TTL, it's deleted on the spot and the
command returns as if the key doesn't exist. This is free — it piggybacks on
the read that was happening anyway. Hot keys are always cleaned up instantly.

**2. Active expiry sweep (background):** Every `:expiry_sweep_interval_ms`
(default: 1 second), each shard samples keys from ETS and deletes any that
have expired. This catches keys that nobody is reading — without the sweep,
a key set with a 5-minute TTL that nobody ever reads again would sit in ETS
and on disk forever, wasting RAM.

```
  Lazy expiry (on read):              Active sweep (background):

  GET key                             Every 1 second per shard:
    │                                   │
    ▼                                   ▼
  Check expire_at_ms                  Sample keys from ETS
    │                                   │
    ├─ not expired → return value       ├─ expired → delete from ETS
    │                                   │             + write tombstone to disk
    └─ expired → delete + return nil    └─ not expired → skip
```

**Tuning guidance:**

| Value | Effect |
|-------|--------|
| `1_000` (default) | Good balance. Expired keys linger at most ~1 second after their TTL. |
| `100` | Aggressive cleanup. More CPU per shard but tighter TTL precision. Use if you have many short-TTL keys (sub-second) and need them gone fast. |
| `10_000` | Lazy cleanup. Expired keys may linger up to 10 seconds. Saves CPU if you have few expiring keys or don't care about precise cleanup. |
| `600_000` | Effectively disables active sweep (10 minutes). Only lazy expiry on read. Good for tests or when you don't use TTLs at all. |

The sweep also writes tombstones to Bitcask for expired keys, so they're
cleaned from disk on the next compaction cycle.

> See [Architecture Guide — Recovery](architecture.md#recovery) for how expired
> keys are handled during shard restart.

```elixir
config :ferricstore, :expiry_sweep_interval_ms, 1_000
```

## Sync Flush

| Option | Type | Default | Applies to | Description |
|--------|------|---------|------------|-------------|
| `:sync_flush_timeout_ms` | `integer` | `5_000` | Both | Maximum time to wait for an in-flight disk flush to complete. See below. |

FerricStore has two disk write modes that work together:

**Async flush (normal writes):** During regular operation, writes accumulate in
a pending list and are flushed to disk periodically via `v2_append_batch_nosync`
(page cache only, ~1us). A separate async fsync is then submitted to Tokio
to make the data durable. This async fsync runs in the background — the shard
GenServer continues processing commands while the disk syncs.

**Sync flush (durability-critical operations):** Certain operations need to
guarantee that ALL pending data is on disk before they can proceed. These
include: DEL (tombstone must be durable), KEYS/SCAN (must reflect latest
state), shutdown (must not lose data), and any explicit flush request. For
these, the shard calls `flush_pending_sync` which:

1. Writes all pending entries to disk via `v2_append_batch` (includes fsync)
2. If an async fsync is already in-flight from a previous batch, **waits** for
   it to complete before proceeding

The `sync_flush_timeout_ms` controls step 2 — how long the shard GenServer
will block waiting for an in-flight async fsync to finish. If the timeout
expires (disk is very slow, I/O stall), the shard logs an error and proceeds
anyway to avoid permanently blocking the GenServer.

```
  Normal write path (async):

  pending list → v2_append_batch_nosync → page cache
                                            │
                                            └─ v2_fsync_async → Tokio → disk
                                                (background, non-blocking)

  Durability-critical path (sync):

  flush_pending_sync
    │
    ├─ 1. Write pending → v2_append_batch (includes fsync)
    │
    └─ 2. If async fsync in-flight from previous batch:
           wait up to sync_flush_timeout_ms for it to complete
           │
           ├─ completed → proceed (data is durable)
           └─ timeout → log error, proceed anyway (avoid GenServer deadlock)
```

**Tuning guidance:**

| Value | When to use |
|-------|-------------|
| `5_000` (default) | Good for most deployments. NVMe/SSD fsync completes in 1-10ms. The 5s timeout is a safety net for edge cases (I/O queue depth spikes, slow cloud disks). |
| `1_000` | Fast-fail on slow I/O. If fsync takes >1s something is seriously wrong — better to log and move on than block the shard for 5s. Good for NVMe-only deployments. |
| `10_000` | Tolerant of slow I/O. Use on network-attached storage (EBS, GCE PD) or spinning disks where fsync can occasionally take seconds under load. |
| `30_000` | Very tolerant. Only for extremely slow storage or debugging I/O issues. |

> **If this timeout fires in production**, it means your disk can't keep up with
> write throughput. Check: I/O utilization (`iostat`), disk queue depth, whether
> the filesystem journal is creating write amplification. Consider fewer shards
> or faster storage.

> See [Architecture Guide — Write Path](architecture.md#write-path) for the full
> write pipeline and how async/sync flushes interact with Raft group commit.

```elixir
config :ferricstore, :sync_flush_timeout_ms, 5_000
```

## Large Value Warning

| Option | Type | Default | Applies to | Description |
|--------|------|---------|------------|-------------|
| `:embedded_large_value_warning_bytes` | `integer` | `524_288` | Embedded only | At startup, scan ETS for values exceeding this threshold and log a warning. Helps catch accidental large-value storage in embedded mode. In standalone mode, large values are served via sendfile zero-copy instead. |

```elixir
config :ferricstore, :embedded_large_value_warning_bytes, 512 * 1024
```

## Node Discovery (libcluster)

FerricStore uses [libcluster](https://hexdocs.pm/libcluster/) for automatic node discovery in multi-node deployments.

```elixir
# Local development: Gossip (multicast)
config :libcluster,
  topologies: [
    ferricstore: [
      strategy: Cluster.Strategy.Gossip,
      config: [
        port: 45892,
        if_addr: "0.0.0.0",
        multicast_addr: "230.1.1.251"
      ]
    ]
  ]

# Kubernetes: DNS
config :libcluster,
  topologies: [
    ferricstore: [
      strategy: Cluster.Strategy.Kubernetes.DNS,
      config: [
        service: "ferricstore-headless",
        application_name: "ferricstore"
      ]
    ]
  ]

# Disable (tests, single-node)
config :libcluster, topologies: :disabled
```

## Example: Development Configuration

```elixir
# config/dev.exs
config :ferricstore, :port, 6379
config :ferricstore, :data_dir, "data"
config :ferricstore, :max_memory_bytes, 1_073_741_824  # 1 GB
config :ferricstore, :eviction_policy, :volatile_lru

config :libcluster,
  topologies: [
    ferricstore: [
      strategy: Cluster.Strategy.Gossip
    ]
  ]
```

## Example: Test Configuration

```elixir
# config/test.exs
config :ferricstore, :port, 0
config :ferricstore, :health_port, 0
config :ferricstore, :data_dir, System.tmp_dir!() <> "/ferricstore_test_#{:os.getpid()}"
config :ferricstore, :shard_count, 4
config :ferricstore, :sync_flush_timeout_ms, 1_000
config :ferricstore, :max_memory_bytes, 1_073_741_824
config :ferricstore, :eviction_policy, :volatile_lru
config :ferricstore, :memory_guard_interval_ms, 5_000
config :ferricstore, :merge, check_interval_ms: 600_000, fragmentation_threshold: 0.99
config :ferricstore, :expiry_sweep_interval_ms, 600_000
config :ferricstore, :read_sample_rate, 1

# Generous supervisor budget for shard-kill tests
config :ferricstore, :supervisor_max_restarts, {1000, 60}

config :libcluster, topologies: :disabled
```

## Example: Production Configuration

```elixir
# config/runtime.exs
config :ferricstore, :port, String.to_integer(System.get_env("FERRICSTORE_PORT", "6379"))
config :ferricstore, :data_dir, System.get_env("FERRICSTORE_DATA_DIR", "/var/lib/ferricstore")
config :ferricstore, :max_memory_bytes, 8_589_934_592  # 8 GB
config :ferricstore, :eviction_policy, :allkeys_lru

config :ferricstore, :tls_port, 6380
config :ferricstore, :tls_cert_file, System.get_env("TLS_CERT_FILE")
config :ferricstore, :tls_key_file, System.get_env("TLS_KEY_FILE")
config :ferricstore, :require_tls, true

config :libcluster,
  topologies: [
    ferricstore: [
      strategy: Cluster.Strategy.Kubernetes.DNS,
      config: [
        service: "ferricstore-headless",
        application_name: "ferricstore"
      ]
    ]
  ]
```

## Runtime Environment Variables

These environment variables are read from `config/runtime.exs` in production (`MIX_ENV=prod`). They configure FerricStore when running as a release or Docker container.

### Core

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_PORT` | `6379` | TCP port for RESP3 listener |
| `FERRICSTORE_HEALTH_PORT` | `6380` | HTTP health/metrics port |
| `FERRICSTORE_DATA_DIR` | `/data` | Root data directory (Bitcask, WARaft segments, mmap) |
| `FERRICSTORE_SHARD_COUNT` | `0` (auto) | Number of shards. `0` = `System.schedulers_online()` |

### Memory & Eviction

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_MAX_MEMORY` | `0` (unlimited) | Max memory bytes. 0 = no limit |
| `FERRICSTORE_EVICTION_POLICY` | `volatile_lru` | Eviction policy when memory limit reached |
| `FERRICSTORE_MAX_VALUE_SIZE` | `1048576` (1MB) | Max value size in bytes |
| `FERRICSTORE_HOT_CACHE_MAX_VALUE_SIZE` | `65536` (64KB) | Values larger than this are stored cold (disk only) |
| `FERRICSTORE_BLOB_SIDE_CHANNEL_THRESHOLD_BYTES` | `262144` (256KB) | Values at or above this size use append-segment blob side-channel storage. `0` disables it. |
| `FERRICSTORE_BLOB_GC_SWEEPER_ENABLED` | `true` | Enables automatic conservative cleanup of stale tmp and legacy blob files. |
| `FERRICSTORE_BLOB_GC_SWEEPER_INITIAL_DELAY_MS` | `60000` | Delay before the first automatic blob GC sweep. |
| `FERRICSTORE_BLOB_GC_SWEEPER_INTERVAL_MS` | `600000` | Interval between automatic blob GC sweeps. |
| `FERRICSTORE_MEMORY_GUARD_INTERVAL_MS` | `5000` | Memory pressure check interval |

### LFU Scoring

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_LFU_DECAY_TIME` | `1` | Minutes between LFU counter decay |
| `FERRICSTORE_LFU_LOG_FACTOR` | `10` | Logarithmic factor for LFU increment probability |
| `FERRICSTORE_READ_SAMPLE_RATE` | `100` | 1-in-N reads trigger LFU/stats update (1 = every read) |

### Expiry

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_EXPIRY_SWEEP_INTERVAL_MS` | `100` | Active expiry sweep interval |
| `FERRICSTORE_EXPIRY_MAX_KEYS_PER_SWEEP` | `20` | Max keys to expire per sweep cycle |

### Slow Log

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_SLOWLOG_SLOWER_THAN_US` | `10000` (10ms) | Log commands slower than this |
| `FERRICSTORE_SLOWLOG_MAX_LEN` | `128` | Max slow log entries (ring buffer) |

### Security

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_PROTECTED_MODE` | `true` | Reject non-localhost connections without auth |
| `FERRICSTORE_MAXCLIENTS` | `10000` | Max concurrent TCP connections |
| `FERRICSTORE_AUDIT_LOG` | `false` | Enable audit logging |
| `FERRICSTORE_ACL_AUTO_SAVE` | `false` | Auto-save ACL changes to disk |

### TLS

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_TLS_PORT` | `6380` | TLS listener port (only if cert configured) |
| `FERRICSTORE_TLS_CERT_FILE` | unset | Path to TLS certificate. Setting this enables TLS |
| `FERRICSTORE_TLS_KEY_FILE` | unset | Path to TLS private key |
| `FERRICSTORE_TLS_CA_CERT_FILE` | unset | Path to CA certificate for client verification |
| `FERRICSTORE_REQUIRE_TLS` | `false` | Reject non-TLS connections when TLS is configured |

### Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_SOCKET_ACTIVE_MODE` | `true` | TCP active mode: `true`, `once`, or integer N |

### WARaft Internals

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_RELEASE_CURSOR_INTERVAL` | `200000` | Applied-entry interval for emitting release-cursor effects. |
| `FERRICSTORE_WAL_COMMIT_DELAY_US` | `6000` | Max adaptive WARaft segment group-commit delay in microseconds. |
| `FERRICSTORE_RAFT_BATCHER_MAX_PENDING` | `256` | Max pending calls per shard Batcher before backpressure. |
| `FERRICSTORE_RAFT_BATCHER_MAX_BATCH_BYTES` | `4194304` (4 MiB) | Max estimated payload bytes per Batcher slot before an early WARaft submit. Keeps large SET pipelines from forming very large log terms while preserving high small-value batching. |
| `FERRICSTORE_RAFT_BATCHER_MAX_PENDING_BYTES` | `0` (disabled) | Max serialized payload bytes in flight per shard Batcher before overload backpressure. Set a positive value, for example `268435456` for 256 MiB, to bound large-payload bursts while allowing one oversized write through on an empty queue. |
| `FERRICSTORE_WARAFT_LOG_ROTATION_INTERVAL` | `50000` | WARaft segment rotation interval in entries. |
| `FERRICSTORE_WARAFT_LOG_ROTATION_KEEP` | `100000` | Entries to keep around rotation/compaction boundaries. |
| `FERRICSTORE_WARAFT_MAX_RETAINED_ENTRIES` | `100000` | Max retained entries after release-cursor trimming. |
| `FERRICSTORE_WARAFT_APPLY_PROJECTION_CACHE_MAX_ENTRIES` | adaptive | Per-shard cap for WARaft-applied Flow rows waiting for lagged LMDB/history projection. Too low can force synchronous spill/compaction during terminal Flow bursts; validate changes with the DBOS-style 1M Flow benchmark. |
| `FERRICSTORE_RAFT_DIRECT_BATCH_COMMANDS` | `true` | Submit homogeneous SET/DEL pipeline runs as final compact Ra terms instead of generic per-command batches. |
| `FERRICSTORE_RAFT_COMPACT_HOT_BATCHES` | `true` | Compact homogeneous generic Ra batches into `put_batch` / `delete_batch` terms inside the batcher. |
| `FERRICSTORE_RAFT_PUT_BATCH_APPLY_FAST_PATH` | `true` | Use the state-machine SET batch fast path that stages Bitcask records and publishes ETS after append success. |
| `FERRICSTORE_RAFT_DELETE_BATCH_APPLY_FAST_PATH` | `true` | Use the state-machine DEL batch fast path that stages tombstones and deletes ETS after append success. |
| `FERRICSTORE_MAX_ACTIVE_FILE_SIZE` | `8589934592` (8 GiB) | Max active Bitcask file size before rollover. Larger values reduce active-file rotation tail spikes on high-throughput write workloads. |
| `FERRICSTORE_PROMOTION_THRESHOLD` | `100` | Field count to promote collection to dedicated Bitcask |

### Clustering

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_NODE_NAME` | unset | Erlang node name. Setting this enables clustering |
| `FERRICSTORE_COOKIE` | `ferricstore` | Erlang distribution cookie |
| `FERRICSTORE_CLUSTER_NODES` | unset | Comma-separated peer node names |

### Supervisor (advanced)

| Variable | Default | Description |
|----------|---------|-------------|
| `FERRICSTORE_MAX_RESTARTS` | `20` | Max child restarts before supervisor gives up |
| `FERRICSTORE_MAX_RESTARTS_SECONDS` | `10` | Time window for max restarts |

## Runtime Configuration (CONFIG SET)

Some parameters can be changed at runtime **without restarting FerricStore**.
This works in both standalone and embedded mode — same parameters, same
behavior, different API.

### Standalone mode (Redis protocol)

```bash
redis-cli CONFIG SET maxmemory-policy allkeys-lru
redis-cli CONFIG GET maxmemory
redis-cli CONFIG GET *             # get all parameters
```

### Embedded mode (Elixir API)

```elixir
# Set a parameter
:ok = Ferricstore.Config.set("maxmemory-policy", "allkeys-lru")

# Get a single parameter (fast ETS read, ~100ns)
"allkeys-lru" = Ferricstore.Config.get_value("maxmemory-policy")

# Get parameters matching a pattern (returns list of {key, value} pairs)
pairs = Ferricstore.Config.get("*")            # all parameters
pairs = Ferricstore.Config.get("maxmemory*")   # matching pattern

# Read-only parameter
{:error, "ERR Unsupported CONFIG parameter: maxmemory (read-only)"} =
  Ferricstore.Config.set("maxmemory", "999")
```

Changes take effect immediately. A `[:ferricstore, :config, :changed]`
telemetry event is emitted on every successful `CONFIG SET`.

### Read-Write Parameters

| Parameter | Description | Applies to |
|-----------|-------------|------------|
| `maxmemory-policy` | Eviction policy (`volatile-lru`, `allkeys-lru`, `volatile-ttl`, `noeviction`) | Both |
| `notify-keyspace-events` | Keyspace notification flag string | Standalone only |
| `slowlog-log-slower-than` | Slowlog threshold in microseconds | Both |
| `slowlog-max-len` | Maximum slowlog entries | Both |
| `hz` | Server tick frequency (1-500) | Standalone only |
| `keydir-max-ram` | Maximum RAM for ETS keydir in bytes (default: 256 MB) | Both |
| `hot-cache-max-ram` | Maximum RAM for hot cache values in bytes | Both |
| `hot-cache-min-ram` | Minimum RAM for hot cache values in bytes (default: 64 MB) | Both |
| `hot-cache-max-value-size` | Maximum value size stored hot in ETS (default: 65536). Values above this are stored cold. | Both |

### Read-Only Parameters (CONFIG GET only)

These reflect the startup configuration and cannot be changed at runtime.

| Parameter | Description | Applies to |
|-----------|-------------|------------|
| `maxmemory` | Max memory budget in bytes | Both |
| `maxclients` | Maximum simultaneous client connections | Standalone only |
| `tcp-port` | TCP port | Standalone only |
| `data-dir` | Bitcask data directory | Both |
| `tls-port` | TLS port | Standalone only |
| `tls-cert-file` | PEM certificate path | Standalone only |
| `tls-key-file` | PEM key path | Standalone only |
| `tls-ca-cert-file` | CA certificate bundle path | Standalone only |
| `require-tls` | Whether plaintext is rejected | Standalone only |

### Namespace-Specific Configuration

FerricStore supports per-namespace tuning for group commit windows. The
namespace is derived from the key prefix (everything before the first `:`).

**Standalone mode:**

```bash
# Session keys: 1ms batch window
redis-cli FERRICSTORE.CONFIG SET session window_ms 1

# Analytics counters: 5ms batch window
redis-cli FERRICSTORE.CONFIG SET analytics window_ms 5

# Reset to defaults
redis-cli FERRICSTORE.CONFIG RESET analytics
```

**Embedded mode:**

```elixir
# Same thing via Elixir — call the Config command handler directly
alias Ferricstore.Commands.Server

# Set namespace config (builds a store map for the handler)
Server.handle("FERRICSTORE.CONFIG", ["SET", "session", "window_ms", "1"], %{})
Server.handle("FERRICSTORE.CONFIG", ["SET", "analytics", "window_ms", "5"], %{})

# Reset
Server.handle("FERRICSTORE.CONFIG", ["RESET", "analytics"], %{})
```

> See [Raft Consensus & Durability](#raft-consensus--durability) and the
> [Architecture Guide](architecture.md) for how namespace-aware group commit works.
