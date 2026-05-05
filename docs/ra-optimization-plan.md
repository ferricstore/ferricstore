# RA Optimization Plan for FerricStore

## Current State

FerricStore uses `ra ~> 3.1` (RabbitMQ's Raft library) with one ra system (`:ferricstore_raft`) shared by all shards. Each shard is an independent Raft group, but all shards share a **single `ra_log_wal` process** — the throughput ceiling.

We already have:
- Patched `ra_log_wal` with async fdatasync (amortizes fsync across batches)
- Group commit via Batcher (1ms window, up to 1000 commands)
- Pipeline commands (`ra:pipeline_command/4`) instead of blocking writes
- ETS hot cache for reads (bypasses Raft entirely)
- `segment_max_entries: 32_768`

## Architecture: Where Time Goes

```
Client write
  → Batcher (1ms window, batch up to 1000 cmds)
    → ra:pipeline_command({:batch, cmds})
      → ra_server_proc (gen_statem per shard)
        → ra_log_wal (SINGLE process, ALL shards)
          → file:write (kernel buffer)        ~0.01ms
          → file:datasync (BLOCKING)          ~0.1-20ms  ← BOTTLENECK
        → AppendEntries RPC to followers      ~0.5-5ms network
        → ra_server apply/3 (state machine)   ~0.01-0.1ms per entry
          → Bitcask NIF + ETS update
```

**The WAL is the single serialization point.** All shards queue behind one fsync. More shards = better batch amortization, but still bounded by fsync latency.

Measured: 4 shards → 254 writes/s, 32 shards → 435 writes/s (macOS, ~20ms fdatasync).

---

## Optimization 0: Pre-serialize Before WAL (HIGHEST IMPACT, NO FORK)

**What:** Move `term_to_binary` from the WAL process to the caller (shard/batcher), send pre-serialized refc binaries via `{ttb, Bin}`.

**Why:** The single `ra_log_wal` process currently does ALL serialization — `term_to_iovec` for every entry from every shard, sequentially on one core. This is the hidden CPU bottleneck *before* we even hit fdatasync. By pre-serializing in the caller, we parallelize the expensive work across all BEAM schedulers and reduce the WAL process to a thin write+fsync coordinator.

### How Erlang binaries make this work

```
Small binary (≤64 bytes):  lives on process heap, COPIED on send
Refc binary  (>64 bytes):  lives in shared heap, only a POINTER is copied on send
```

A `term_to_binary({:batch, [cmd1, cmd2, ...]})` produces a refc binary (>64 bytes). Sending it to the WAL process copies ~8 bytes (the ProcBin pointer), not the payload. The WAL process receives a pointer, not data.

### Current flow (bad — serialization on one core)

```
Shard A:  sends raw term {put, k, v}       ──deep copy──→  WAL mailbox
Shard B:  sends raw term {delete, k}        ──deep copy──→  WAL mailbox
Shard C:  sends raw term {batch, [...]}     ──deep copy──→  WAL mailbox
                                                                ↓
                                                    WAL process (1 core):
                                                      term_to_iovec(A)  ← serialize
                                                      term_to_iovec(B)  ← serialize
                                                      term_to_iovec(C)  ← serialize
                                                      file:write(all)
                                                      fdatasync()
```

Raw Erlang terms are **always deep-copied** into the receiver's heap. A `{:put, <<"user:123">>, <<big_value>>}` tuple is deconstructed and reconstructed in the WAL process heap. Then the WAL process serializes each one, sequentially, on one core.

### New flow (good — serialization parallel across cores)

```
Shard A (core 1):  Bin_A = term_to_binary(cmd)  ──ptr──→  WAL mailbox
Shard B (core 2):  Bin_B = term_to_binary(cmd)  ──ptr──→  WAL mailbox
Shard C (core 3):  Bin_C = term_to_binary(cmd)  ──ptr──→  WAL mailbox
                   ↑ parallel, N cores                        ↓
                                                    WAL process (1 core):
                                                      IoList = [Bin_A, Bin_B, Bin_C]
                                                      file:write(Fd, IoList)  ← scatter-gather
                                                      fdatasync()
```

Three things change:

1. **Serialization moves to shards.** Each shard calls `term_to_binary` on its own scheduler, on its own core, in parallel with all other shards.
2. **Message is a pointer, not a copy.** The binary is >64 bytes → refc binary. Crossing the mailbox copies ~8 bytes (pointer), not the payload.
3. **WAL process does almost nothing.** Receives pointers, passes them as an iolist to `file:write` (kernel scatter-gather reads from each binary's memory location), then one `fdatasync`.

### ra already supports this

`ra_log_wal.erl` line ~509:

```erlang
case Data of
    {ttb, Bin} -> Bin;          %% pre-serialized — use as-is, skip term_to_iovec
    Term -> term_to_iovec(Term) %% raw term — serialize here (current path)
end
```

The `{ttb, Binary}` path exists but isn't used by default. We just need to use it.

### Implementation in FerricStore (no ra fork needed)

In the Batcher, before calling `ra:pipeline_command`:

```elixir
# Before (current):
command = {:batch, commands}
:ra.pipeline_command(shard_id, command, corr, :normal)

# After:
command = {:batch, commands}
serialized = :erlang.term_to_binary(command)
:ra.pipeline_command(shard_id, {:ttb, serialized}, corr, :normal)
```

Same change in Router for direct `ra:process_command` calls:

```elixir
# Before:
:ra.process_command(shard_id, command, opts)

# After:
serialized = :erlang.term_to_binary(command)
:ra.process_command(shard_id, {:ttb, serialized}, opts)
```

### Why this beats the Rust shared-buffer NIF

| | Pre-serialize (Erlang) | Rust shared-buffer NIF |
|--|---|---|
| Serialization | Parallel (BEAM schedulers) | Parallel (Rust threads) |
| Data to WAL | Refc binary pointer (~8 bytes) | Direct mmap write |
| WAL process CPU | ~50µs per batch (iolist + write) | Eliminated entirely |
| fdatasync | Same | Same |
| Complexity | 5-line change, no fork | NIF, mmap lifecycle, unsafe |
| Gain over current | **~10-30x** | **~10-35x** (marginal improvement) |

The Rust NIF saves ~50µs per batch by eliminating the WAL process entirely. But fdatasync dominates at 0.1-20ms. The 50µs is noise. Do this first — the Rust NIF is only worth revisiting if profiling shows the WAL process is *still* the bottleneck.

### Estimated impact

- **Single-node:** 10-30x throughput improvement (eliminates CPU serialization bottleneck, fdatasync becomes the only ceiling)
- **Cluster:** Same improvement on leader node, plus followers process AppendEntries faster (entries arrive pre-serialized)

---

## Optimization 1: Parallel Leader Disk Write (HIGH IMPACT)

**What:** Leader sends AppendEntries to followers *before* its own WAL write completes.

**Why:** In standard Raft, the leader writes to WAL, then sends to followers. Commitment requires a majority — the leader's local write is just one of those votes. By sending AppendEntries in parallel with the local WAL write, we overlap disk latency with network latency.

**Safety:** Raft thesis Section 10.2.1 proves this is safe. TiKV, etcd, and CockroachDB all implement it.

**Where in ra:**

`ra_server.erl` — `handle_leader/2` for `command` messages:
1. Currently: append to log → generate pipelined RPCs → return effects
2. Change: generate RPCs with entries *before* WAL confirms → append to log in parallel

The tricky part: ra's WAL batching means the entry isn't yet durable when `handle_leader` returns. The RPCs are generated as effects that `ra_server_proc` sends — we need to ensure RPCs go out before waiting on WAL notification.

**Estimated impact:** In cluster mode, overlaps ~1-20ms disk with ~0.5-5ms network. Reduces commit latency by min(disk, network). Single-node: no impact (self-quorum, must fsync locally).

**Complexity:** Medium. Requires careful ordering in `ra_server_proc.erl` effect handling.

---

## Optimization 2: io_uring for WAL fsync (HIGH IMPACT, Linux only)

**What:** Replace `file:datasync/1` in `ra_log_wal` with a Rust NIF using `io_uring` for async fsync.

**Why:** `io_uring` with `IORING_OP_FSYNC` submits the fsync to the kernel's submission queue and returns immediately. The kernel processes it asynchronously. On NVMe with datacenter SSDs, this can take <50µs vs 0.1-2ms for synchronous fdatasync.

**Implementation:**

```rust
// NIF: submit fsync to io_uring ring, return immediately
fn wal_fsync_submit(fd: RawFd) -> NifResult<()>

// NIF: poll for fsync completion (non-blocking)
fn wal_fsync_poll() -> NifResult<Option<i32>>
```

Integrate into patched `ra_log_wal.erl`:
- Replace `spawn(fun() -> file:datasync(Fd), self() ! {wal_async_sync_complete} end)` with:
  - `wal_fsync_submit(Fd)` after `file:write`
  - Poll for completion in `handle_batch` or via a port message

**Important:** The NIF must be fast (submit only, no blocking). The BEAM scheduler is not blocked. Completion is delivered as a message.

**Estimated impact:** 10-100x reduction in fsync latency on Linux NVMe. Eliminates the WAL bottleneck.

**Complexity:** Medium. Rust NIF already exists in the project (Bitcask). Need to handle the fd lifecycle (WAL opens files via Erlang, need to pass raw fd to NIF).

**Limitation:** macOS has no io_uring. Fallback to current async fdatasync.

---

## Optimization 3: Increase Hardcoded Constants (LOW EFFORT, MEDIUM IMPACT)

Several ra constants are too conservative for a high-throughput KV store:

### `?FLUSH_COMMANDS_SIZE = 16` → 128

**File:** `ra_server.hrl:12`

Low-priority commands are flushed in batches of 16. With high write rates, this causes many small flushes. Increasing to 128 reduces message-passing overhead.

### `?AER_CHUNK_SIZE = 128` → 512

**File:** `ra_server.hrl:7`

AppendEntries RPCs batch at most 128 entries. For a KV store doing thousands of entries/sec, followers fall behind. 512 improves replication throughput with minimal memory overhead.

### `?WAL_DEFAULT_MAX_BATCH_SIZE = 8192` → 32768

**File:** `ra.hrl:214`

WAL batches up to 8192 entries before flushing. With 256 shards each submitting batches, this limit is hit. 32768 allows better amortization per fsync.

### `?SEGMENT_MAX_ENTRIES = 4096` → 32768

**File:** `ra.hrl:224`

Already overridden in FerricStore's `start_system` config. Confirm it's being applied.

### How to apply without forking

These can be set at runtime via the ra system config map:
```elixir
config = %{
  name: :ferricstore_raft,
  wal_max_batch_size: 32_768,           # was 8192
  segment_max_entries: 32_768,          # already set
  wal_compute_checksums: false,         # skip CRC (validated at Bitcask layer)
  wal_pre_allocate: true,               # avoid extent allocation stalls on Linux
  # These require fork:
  # max_append_entries_rpc_batch_size and flush_commands_size are not in system config
}
```

The `AER_CHUNK_SIZE` and `FLUSH_COMMANDS_SIZE` are compile-time defines — they require a fork or patched beam file.

---

## Optimization 4: Disable WAL Checksums (LOW EFFORT, LOW-MEDIUM IMPACT)

**What:** Set `wal_compute_checksums: false` in ra system config.

**Why:** FerricStore validates data integrity at the Bitcask layer (CRC per entry). The WAL checksum (Adler32 per entry) is redundant. On high write rates, this saves CPU.

**Where:** `ra_log_wal.erl:524` — `erlang:adler32(IoData)` called per entry if enabled.

**How:**
```elixir
config = %{
  ...
  wal_compute_checksums: false
}
```

**Risk:** If WAL corruption occurs (bit flip in transit between ra_server and WAL process — extremely unlikely on same node), it won't be detected until state machine apply. Acceptable since Bitcask has its own CRC.

---

## Optimization 5: WAL Pre-allocation (LOW EFFORT, LOW IMPACT on Linux)

**What:** Set `wal_pre_allocate: true`.

**Why:** On ext4/XFS, file writes that extend the file trigger extent allocation, which can cause latency spikes (1-10ms). Pre-allocating the full WAL file (256MB) avoids this.

**Where:** `ra_log_wal.erl` — `open_wal/1` calls `file:allocate(Fd, 0, MaxSize)` if enabled.

**How:**
```elixir
config = %{
  ...
  wal_pre_allocate: true
}
```

**Note:** No effect on macOS APFS (doesn't support `fallocate`).

---

## Optimization 6: Multiple WAL Processes (HIGH IMPACT, HIGH COMPLEXITY)

**What:** Run multiple ra systems, each with its own WAL process, partitioned by shard groups.

**Why:** The single WAL is the serialization point. Two WAL processes on two NVMe drives doubles throughput.

**How:**
```elixir
# Instead of one system:
:ra_system.start(%{name: :ferricstore_raft, ...})

# Run N systems, one per NVMe or per shard group:
:ra_system.start(%{name: :ferricstore_raft_0, wal_data_dir: '/mnt/nvme0/ra', ...})
:ra_system.start(%{name: :ferricstore_raft_1, wal_data_dir: '/mnt/nvme1/ra', ...})
```

Shards 0-127 → system 0, shards 128-255 → system 1.

**Trade-off:** Smaller batches per WAL (less amortization), but parallel fsyncs. Net positive only with separate physical drives.

**Complexity:** High. Cluster management, metrics, and shutdown logic all assume one system.

---

## Optimization 7: Async Apply (MEDIUM IMPACT, MEDIUM COMPLEXITY)

**What:** Decouple state machine `apply/3` from the Raft commit path.

**Why:** Currently, `ra_server_proc` applies entries synchronously after they're committed. If apply is slow (Bitcask write + ETS update), it blocks the next Raft round. Async apply lets the Raft thread move on immediately.

**How (in ra fork):**

In `ra_server_proc.erl`, after entries are committed:
1. Instead of calling `ra_server:handle_effects` with applied results inline...
2. Send committed entries to a separate apply process
3. The apply process calls `apply/3` and updates ETS
4. Raft thread continues accepting new commands

**Complication:** Linearizability. A client that writes then reads might see stale data if the apply hasn't happened yet. FerricStore's ETS reads would need to check if the read index has been applied.

**TiKV/CockroachDB approach:** They handle this by tracking `applied_index` per replica and stalling reads that depend on unapplied entries.

**For FerricStore:** Since reads go through ETS (not Raft), and ETS is updated during apply, this would require a "read barrier" — check that `applied_index >= last_written_index` before returning to client. The Batcher already tracks correlation, so this is feasible.

---

## Optimization 8: Batched term_to_binary with Atom Cache (MEDIUM IMPACT, HIGH COMPLEXITY)

**What:** Replace `term_to_iovec/1` in WAL writes with a custom serializer that caches repeated atoms.

**Why:** ra Issue #199. FerricStore commands like `{:put, key, value, expire_at_ms}` repeat atoms (`:put`, `:delete`, `:batch`) thousands of times. Each `term_to_binary` encodes the atom as a full UTF-8 string. An atom cache replaces repeated atoms with 1-byte indices.

**Estimated savings:** 10-30% reduction in WAL write size for small values. Negligible for large values.

**Complexity:** High. Need a pure-Erlang `term_to_binary` replacement that maintains an atom table. Must be bug-free and faster than the BIF — hard to beat C.

**Alternative:** Pre-serialize commands as binaries before submitting to ra. The WAL already supports `{ttb, Bin}` tuples for pre-serialized data (`ra_log_wal.erl:509`). If we `term_to_binary` the command in the Batcher (once per batch, not per entry), we avoid redundant serialization.

---

## Optimization 9: Separate WAL and Segment Directories (LOW EFFORT, MEDIUM IMPACT)

**What:** Put WAL on the fastest drive, segments on a separate drive.

**Why:** WAL writes are latency-critical (fsync path). Segment writes happen in background (`ra_log_segment_writer`). If they share the same drive, segment flushes compete with WAL fsyncs for disk bandwidth.

**How:**
```elixir
config = %{
  ...
  data_dir: '/mnt/ssd/ra',           # segments, snapshots
  wal_data_dir: '/mnt/nvme-fast/ra',  # WAL only (fastest drive)
}
```

---

## Optimization 10: Checkpoint Tuning (LOW EFFORT, LOW IMPACT)

**What:** Use ra checkpoints (merged Jan 2024) instead of full snapshots for faster recovery.

**Why:** Full snapshots serialize entire state + CRC. Checkpoints are lighter — captured state without triggering log truncation. On restart, ra replays from the most recent checkpoint instead of the most recent snapshot.

**How:** Ra 3.x supports checkpoints automatically. Tune `min_checkpoint_interval`:
```elixir
# In server config log_init_args:
log_init_args: %{
  uid: shard_uid(shard_index),
  min_checkpoint_interval: 8192   # default 16384
}
```

FerricStore already sets `min_recovery_checkpoint_interval: 1` in server config — this is for shutdown checkpoints only. Consider lowering `min_checkpoint_interval` for runtime checkpoints too.

---

## Priority Matrix

| # | Optimization | Impact | Effort | Requires Fork | Single-Node Benefit |
|---|-------------|--------|--------|---------------|---------------------|
| **0** | **Pre-serialize before WAL** | **Highest** | **Lowest** | **No** | **Yes** |
| 3 | Increase constants | Medium | Low | Partial (AER/flush) | Yes |
| 4 | Disable checksums | Low-Med | Low | No | Yes |
| 5 | WAL pre-allocate | Low | Low | No | Yes (Linux) |
| 9 | Separate WAL dir | Medium | Low | No | Yes |
| 10 | Checkpoint tuning | Low | Low | No | Yes |
| 1 | Parallel leader write | High | Medium | Yes | No (cluster only) |
| 2 | io_uring fsync | High | Medium | Yes (patched WAL) | Yes |
| 7 | Async apply | Medium | Medium | Yes | Yes |
| 6 | Multiple WAL procs | High | High | No (config) | Yes (multi-drive) |
| 8 | Atom cache | Medium | High | Yes | Yes (subsumed by #0) |

## Recommended Order

**Phase 0 — Pre-serialize commands (do first, no fork):**
- In Batcher: `term_to_binary(command)` before `ra:pipeline_command`, wrap as `{:ttb, bin}`
- In Router: same for direct `ra:process_command` calls
- Benchmark before/after to measure actual gain

**Phase 1 — Config tuning (do now, no fork):**
- Add `wal_compute_checksums: false` to system config
- Add `wal_pre_allocate: true` to system config
- Separate `wal_data_dir` from `data_dir` in production
- Verify `segment_max_entries: 32_768` is applied

**Phase 2 — Fork with constant changes:**
- Fork ra to `~/repos/ra`, point mix.exs at local path
- Increase `FLUSH_COMMANDS_SIZE` 16 → 128
- Increase `AER_CHUNK_SIZE` 128 → 512
- Increase `WAL_DEFAULT_MAX_BATCH_SIZE` 8192 → 32768

**Phase 3 — io_uring NIF (Linux production):**
- Add `io_uring` fsync NIF to ferricstore_nif
- Integrate into patched `ra_log_wal.erl`
- Benchmark: target <100µs fsync on datacenter NVMe

**Phase 4 — Parallel leader write (cluster mode):**
- Modify `ra_server.erl` to send AppendEntries before WAL confirmation
- Modify `ra_server_proc.erl` effect ordering
- Requires thorough Jepsen testing

**Phase 5 — Async apply (if apply is measured as bottleneck):**
- Only if profiling shows `apply/3` time > WAL time
- Requires read barrier mechanism

## Fork Strategy

Fork at `~/repos/ra`. In FerricStore's `mix.exs`:
```elixir
{:ra, path: "../ra"}  # development
{:ra, github: "yoavgea/ra", branch: "ferricstore"}  # production
```

Keep the fork minimal — only touch constants and the WAL module. Rebase regularly on upstream `rabbitmq/ra` main.

## References

- [Raft thesis Section 10.2.1](https://raft.github.io/raft.pdf) — Parallel leader disk write proof
- [Optimizing Raft in TiKV](https://www.pingcap.com/blog/optimizing-raft-in-tikv/) — Parallel append, async apply, batch+pipeline
- [CockroachDB async apply](https://github.com/cockroachdb/cockroach/issues/17500)
- [ra Issue #199](https://github.com/rabbitmq/ra/issues/199) — Serialization overhead
- [ra Issue #141](https://github.com/rabbitmq/ra/issues/141) — Checkpoints
- [WhatsApp/waraft](https://github.com/WhatsApp/waraft) — Alternative Erlang Raft, 200K/s
- [FLEET (VLDB 2025)](https://dl.acm.org/doi/10.14778/3718057.3718077) — Scattered log entries, 10x over etcd
