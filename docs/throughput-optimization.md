# FerricStore Throughput Optimization Guide (v0.2.0)

Each section has: current default, what to benchmark, expected tradeoff.

## Write Path Architecture (v0.2.0)

Understanding the write path is essential before tuning:

```
Client SET key value
  → Router.put()
    → check_keydir_full() (atomics, ~5ns)
    → ra:process_command(reply_from: :local)
      → WAL write (page cache, async fdatasync)
      → Raft commit (single-node: immediate)
      → state_machine apply/3
        → v2_append_batch_nosync() (Bitcask, page cache)
        → v2_fsync() (one fsync per batch)
        → ETS insert
      → reply :ok to caller
```

**Two fsyncs per write path:**
1. **WAL fdatasync** — async, batched by our patched ra_log_wal
2. **Bitcask fsync** — sync, one per Raft batch (group commit)

The Bitcask fsync is the new bottleneck (added in v0.2.0 for crash
durability). It's amortized: the Raft batcher groups N writes into one
`:batch` command → one `v2_append_batch_nosync` → one `v2_fsync`.

**Measured:** 20 writes batched = 4ms (1 fsync). 20 individual = 84ms
(20 fsyncs). **20x speedup from batching.**

### Quorum-only write path

FerricStore no longer exposes a weaker namespace write mode. All client
writes use the quorum path: the Batcher groups commands, submits them through
Raft, waits for local apply, and only then replies to the caller. Bitcask data
files are checkpointed in the background, while Ra WAL durability protects
acknowledged commands until the state-machine cursor can be released.

---

## Tunable Parameters (need benchmarking)

### 1. Raft Batcher Window

**Current:** Configurable per namespace via `window_ms` (default varies).

The batcher collects writes over a time window, then submits them as a
single `{:batch, commands}` to Raft. The state machine applies all
commands and does ONE fsync for the entire batch.

**This is the most important tunable for quorum write throughput.**

```
window_ms=1   → batch of ~10-50 writes  → 1 fsync  → low latency, lower throughput
window_ms=5   → batch of ~50-500 writes → 1 fsync  → balanced
window_ms=10  → batch of ~500+ writes   → 1 fsync  → high throughput, higher tail latency
```

**What to benchmark:**
- Throughput at window_ms 1, 2, 5, 10, 20
- p50/p95/p99 latency at each window
- Optimal: highest throughput where p99 < your SLA

**Tradeoff:** Longer window = more writes per fsync = higher throughput,
but p99 latency increases by ~window_ms.

### 2. WAL fdatasync (automatic, self-tuning)

**Current:** Async fdatasync in patched ra_log_wal. While fdatasync is
in flight (~100μs NVMe, ~1-5ms SSD), new writes accumulate. When it
completes, all accumulated writers are notified, then another fdatasync
starts.

The "batch window" IS the fdatasync latency. No explicit tunable — the
faster the disk, the smaller the batch. NVMe is ~10x faster than SSD
here.

**What to benchmark:**
- NVMe vs SSD throughput difference (expected: 3-10x)
- How many writes accumulate per fdatasync cycle

**Tradeoff:** Self-tuning. Storage medium is the only variable.

### 3. Shard Count

**Current:** `4` (default), configurable via `FERRICSTORE_SHARD_COUNT`.
If set to `0`, defaults to `System.schedulers_online()`.

Each shard has an independent Raft group + WAL + Bitcask file + fsync.
More shards = more PARALLEL fsyncs = higher aggregate write throughput.

**This is the second most important tunable for write throughput.**

```
4 shards on 4 cores   → 4 parallel fsyncs  → ~40K writes/sec (NVMe)
8 shards on 4 cores   → 8 parallel fsyncs  → ~70K writes/sec (NVMe)
16 shards on 4 cores  → 16 parallel fsyncs → ~100K writes/sec (NVMe)
32 shards on 4 cores  → diminishing returns, more memory overhead
```

**What to benchmark:**
- `schedulers_online()` (1:1 with cores)
- `schedulers_online() * 2` (2:1, more parallelism for NVMe)
- `schedulers_online() * 4` (4:1, diminishing returns?)
- Measure write AND read throughput (reads may degrade with many shards)

**Tradeoff:** More shards = more write parallelism but:
- More ETS tables = less CPU cache locality for reads
- MGET across shards hits multiple tables
- More file descriptors and memory overhead per shard
- Each shard's Raft state machine is a separate process

The sweet spot depends on read/write ratio. Write-heavy: more shards.
Read-heavy: fewer shards.

### 4. Socket Active Mode

**Current:** `active: 100` — kernel delivers up to 100 TCP messages
before going passive. Re-arms on `{:tcp_passive, _}`.

**What to benchmark:**
- `active: true` — no back-pressure, highest throughput, risk of mailbox flood
- `active: 50` — less kernel overhead than 100
- `active: 200` — more batching per passive cycle
- `active: :once` — lowest throughput, best back-pressure

**Tradeoff:** Higher N = fewer `setopts` syscalls = higher throughput,
but larger mailbox under slow consumers.

### 5. Hot Cache Max Value Size

**Current:** `65_536` (64KB). Values larger than this are stored as
`nil` in ETS (cold). Reads go to Bitcask NIF pread.

**What to benchmark:**
- Lower (16KB, 32KB) — less ETS memory, more cold reads
- Higher (128KB, 256KB) — more ETS memory, fewer cold reads
- Measure with your actual value size distribution

**Tradeoff:** Larger threshold = more RAM used by ETS hot cache, but
fewer disk reads. Off-heap binary tracking makes MemoryGuard accurate
regardless.

### 6. Max Active File Size (Bitcask)

**Current:** `256MB`. When active file exceeds this, it rolls over.

**What to benchmark:**
- Smaller (64MB, 128MB) — more frequent rollovers, more compaction
- Larger (512MB, 1GB) — fewer rollovers, larger sequential writes

**Tradeoff:** Smaller files = more compaction I/O but less wasted space.
Larger files = less overhead but slower recovery on restart.

### 7. Release Cursor Interval (Raft Snapshots)

**Current:** `100` — every 100 applied commands, ra may snapshot.

**What to benchmark:**
- Lower (50) — more frequent snapshots, faster recovery, more I/O
- Higher (500, 1000) — less snapshot overhead, slower recovery

**Tradeoff:** More snapshots = faster restart but more CPU/IO during
normal operation.

---

## Fixed Settings (already optimized, just enable)

### BEAM VM Flags

```bash
# vm.args — add all of these
+sbt db           # scheduler pinning → 10-20% throughput gain
+sbwt very_short  # ~1µs wakeup vs ~100µs
+swt very_low     # wake idle schedulers immediately
+K true           # epoll/kqueue
+P 5000000        # max processes
+Q 65536          # max ports
+A 128            # async threads for disk reads
+MHas aoffcbf     # heap allocator strategy
+MBas aoffcbf     # binary allocator strategy
```

### OS Tuning

```bash
# Huge pages — critical for large ETS
echo 'vm.nr_hugepages = 1024' >> /etc/sysctl.conf

# Never swap
echo 'vm.swappiness = 1' >> /etc/sysctl.conf

# Disable THP (causes latency spikes)
echo never > /sys/kernel/mm/transparent_hugepage/enabled

# NVMe — bypass block layer
echo none > /sys/block/nvme0n1/queue/scheduler
echo 1024 > /sys/block/nvme0n1/queue/nr_requests

# File descriptors
echo '* soft nofile 1048576' >> /etc/security/limits.conf

# CPU governor
cpupower frequency-set -g performance
```

### NUMA (multi-socket)

```bash
# Pin BEAM to one NUMA node
numactl --cpubind=0 --membind=0 ./ferricstore start
```

---

## Application-Level Optimizations

### Embedded mode vs TCP

```
embedded:   ETS lookup → ~1-5µs per read
TCP/RESP3:  syscall + parse + ETS → ~50-300µs per read
```

If your app runs on the same BEAM node, use the embedded API.

### Pipelining (TCP clients)

```
# Without pipelining: 3 round-trips
SET k1 v1 → OK → SET k2 v2 → OK → SET k3 v3 → OK

# With pipelining: 1 round-trip, commands batch into one Raft batch
SET k1 v1 \r\n SET k2 v2 \r\n SET k3 v3 → OK OK OK
```

Pipelined commands arrive in the same TCP read, get dispatched together,
and land in the same Raft batcher slot → one batch → one fsync.

### MULTI/EXEC

```
MULTI → SET k1 v1 → SET k2 v2 → SET k3 v3 → EXEC
= 1 Raft round-trip + 1 fsync instead of 3
```

### Data modeling — fewer keys

```
3 keys (3 keydir entries):  SET user:42:name, SET user:42:email, SET user:42:country
1 key (1 keydir entry):     HSET user:42 name email country
```

Fewer keys = smaller keydir = better TLB hit rate = faster everything.

### Raw binary storage (embedded mode)

```elixir
# Skip JSON — store Erlang terms directly
FerricStore.set("key", :erlang.term_to_binary(term))
term = :erlang.binary_to_term(FerricStore.get("key"))
```

---

## Expected Numbers (to verify with benchmarks)

Based on the architecture, expected single-node throughput on NVMe:

| Workload | Shards | Expected ops/sec | Bottleneck |
|----------|--------|-----------------|------------|
| Read-only (ETS hot) | 4 | 500K-1M+ | CPU/scheduler |
| Read-only (cold, pread) | 4 | 100-200K | NVMe IOPS |
| Write quorum (batched) | 4 | 30-50K | fsync latency |
| Write quorum (batched) | 16 | 80-120K | fsync parallelism |
| Write async | 4 | 200-500K | CPU/ETS |
| Mixed 80/20 read/write | 8 | 100-200K | balanced |

Redis 7 single-threaded: ~100-160K SET/sec. Dragonfly multi-threaded:
~1M+ SET/sec. Our target: competitive with Redis on quorum writes,
faster on reads (ETS hot cache).

---

## Benchmarking Methodology

For each tunable, run:

```bash
# Baseline (4 shards, default batcher)
memtier_benchmark -s 127.0.0.1 -p 6379 --ratio=1:0 -n 100000 -c 50 -t 4

# Change one variable
FERRICSTORE_SHARD_COUNT=16 mix run --no-halt &
memtier_benchmark -s 127.0.0.1 -p 6379 --ratio=1:0 -n 100000 -c 50 -t 4
```

Measure:
- **Throughput**: ops/sec (writes and reads separately)
- **Latency**: p50, p95, p99 in microseconds
- **Memory**: RSS, ETS memory, binary bytes tracked
- **Disk I/O**: IOPS, bandwidth (iostat -x 1)

Compare against baseline. Change one variable at a time.
Run each config 3 times and take the median.
