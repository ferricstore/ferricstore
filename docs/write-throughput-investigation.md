# Write Throughput Investigation

## Observed

On Azure L4as_v4 (4 vCPU, NVMe @ 57K IOPS):

| Workload | Ops/sec | Avg Latency |
|----------|---------|-------------|
| Write quorum | 7,655 | 26ms |
| Write async | 7,571 | 26ms |
| Read | 96,112 | 2ms |

**Key finding:** async writes are the same speed as quorum writes. This means fsync is NOT the bottleneck — something else is limiting both paths equally.

The disk can do 57,600 IOPS. We're doing 7,600. We're using **13%** of disk capacity.

## What both paths share

Quorum and async writes go through different durability paths (Raft vs fire-and-forget), but they share:

1. **TCP accept + RESP3 parsing** — Ranch connection handler, binary parsing
2. **Router dispatch** — key hashing, shard lookup
3. **Shard GenServer** — all writes funnel through `GenServer.call` to the shard
4. **Batcher** — both paths go through `Raft.Batcher.write/2` which extracts namespace, looks up config, buffers in slot
5. **ETS insert** — both update the keydir

If quorum was slower than async, we'd know fsync is the bottleneck. Since they're equal, the bottleneck is upstream of the durability divergence point.

## Likely bottlenecks (in order of probability)

### 1. No pipelining in the benchmark

memtier ran with `--pipeline=1` (default). Each of the 200 connections (50 clients × 4 threads) sends one request, waits for the full round-trip response, then sends the next.

With 26ms average latency per write:
- Per connection: 1000ms / 26ms = ~38 ops/sec
- 200 connections × 38 = ~7,600 ops/sec ← **matches exactly**

This is TCP round-trip bound, not server bound. The server might be able to handle 50K+ ops/sec but we're only feeding it 7.6K because each connection is idle 99% of the time.

**Test:** re-run with `--pipeline=10` and `--pipeline=100`. If throughput scales linearly, the server is not the bottleneck.

### 2. Shard GenServer serialization

All writes to a shard go through `GenServer.call(shard, {:put, ...})`. With 4 shards and 200 connections, each shard handles ~50 connections. If the GenServer `handle_call` takes >1ms (Batcher.write is synchronous for quorum), the shard becomes a serialization bottleneck.

For quorum path: `Batcher.write` → `ra:process_command` blocks until Raft applies. The caller is blocked in GenServer.call for the entire Raft round-trip (~5-10ms). During this time, no other write to that shard can proceed.

For async path: `Batcher.write` → `AsyncApplyWorker.apply_batch` is a cast (fire-and-forget), so the GenServer should return immediately. If async is still slow, the GenServer itself might be doing work before the cast.

**Test:** check if the shard GenServer is the bottleneck by looking at its message queue length during the benchmark. If queues build up, the GenServer is too slow.

### 3. Batcher window and batching overhead

The Batcher collects writes into per-namespace slots with a timer window (default 1ms). Even for async writes, the flow is:
1. Client → Shard GenServer.call
2. Shard → Batcher.write (synchronous GenServer.call)
3. Batcher buffers, starts timer
4. Timer fires → submit batch

Two GenServer hops (Shard + Batcher) before any write reaches Raft or AsyncApplyWorker. Each hop adds ~1-5us of scheduling overhead, but more importantly, the Batcher.write is a synchronous call — the Shard GenServer blocks waiting for the Batcher to accept the write.

**Test:** bypass the Batcher for a raw ETS insert benchmark to see if the overhead matters.

### 4. Connection handler overhead

Each TCP connection is a Ranch process. The RESP3 parser, ACL check, command dispatcher, and response encoder all run per-command. With `active: true` socket mode, messages arrive as fast as the client sends them, but each command is processed sequentially within the connection process.

**Test:** compare with `redis-benchmark` or a simpler client to rule out memtier-specific overhead.

### 5. RESP3 protocol overhead

FerricStore requires RESP3 (`HELLO 3`). memtier's RESP3 support may add overhead vs RESP2. Each SET command is:
```
*3\r\n$3\r\nSET\r\n$N\r\nkey\r\n$M\r\nvalue\r\n
```
Parsing is O(n) in value size but for 256B values this should be negligible.

## What to measure

1. **Pipeline scaling test** — run the benchmark with pipeline 1, 10, 50, 100. If ops/sec scales ~linearly with pipeline depth, the server has headroom and the bottleneck is TCP round-trips.

2. **GenServer queue depth** — during the benchmark, check `:erlang.process_info(shard_pid, :message_queue_len)` for each shard. If queues are 0, the server is idle waiting for requests. If queues are >100, the GenServer is saturated.

3. **Scheduler utilization** — `:scheduler.utilization(1000)` during the benchmark. If schedulers are <50% utilized, the server has CPU headroom.

4. **Batcher bypass** — benchmark direct ETS insert (embedded mode, no TCP, no Raft) to establish the ceiling.

5. **Flame graph** — `:eflame` or `:fprof` during a write workload to see where time is spent.

## Pipeline Test Results

| Pipeline | Write | Read | Mixed |
|----------|-------|------|-------|
| 1 | 7,604 | 96,821 | 26,124 |
| 5 | 7,660 | 106,440 | 18,233 |
| 10 | 7,597 | 113,044 | 17,912 |
| 50 | 7,571 | 120,698 | 24,511 |
| 100 | 7,570 | 112,202 | - |

**Pipeline does NOT increase write throughput.** Writes are flat at ~7,600 regardless of pipeline depth. Latency increases linearly (26ms → 2,674ms at pipeline=100) — requests are just queuing.

**Reads scale to ~120K at pipeline=50**, then drop at 100 (ETS contention or scheduler saturation).

## Root Cause Analysis

### The write path (confirmed by code)
1. Connection process sends `GenServer.call(shard, {:put, key, value, exp})`
2. Shard `handle_call` → `ShardWrites.handle_put` → `Batcher.write/3`
3. Batcher buffers the command in a Raft batch
5. After `window_ms` (default 1ms), Batcher flushes:
   - `ra:pipeline_command` → waits for Raft acceptance and local apply → replies to caller

### Why throughput is flat across pipeline depth
The Shard GenServer processes one message at a time. Even though `handle_put` returns `{:noreply, ...}` quickly (~microseconds), the GenServer mailbox is the serialization point. With 200 connections sending to 4 shards, each shard processes ~50 connections sequentially.

The shard's `handle_put` does:
- `MemoryGuard.reject_writes?()` check
- `Batcher.write_async` (GenServer.cast — fast)
- `write_version + 1`

This is microseconds of work per message. The actual throughput should be much higher than 7.6K. The bottleneck is likely NOT the shard GenServer itself.

### Remaining suspects

1. **Batcher window_ms=1** — the batcher collects writes for 1ms, then flushes. For quorum, `ra:pipeline_command` goes to the ra server which does WAL fdatasync + Bitcask fsync. The ra server is also a GenServer — one batch at a time per shard. With 4 shards and 1ms windows, theoretical max = 4 × 1000 batches/sec. If each batch has ~2 commands (low because window is only 1ms), that's ~8,000 ops/sec. **This matches our 7,600 observation.**

2. **For async**: the Batcher still uses the same 1ms timer window before calling `submit_async`. Even though the actual apply is non-blocking, the 1ms batching window + GenServer scheduling adds latency. Callers block in `GenServer.call` to the shard, which returned `{:noreply}`, so they're waiting for `GenServer.reply` from the Batcher. The Batcher only replies after the 1ms flush timer fires for async too.

3. **Connection process blocking** — the connection process does `GenServer.call(shard, {:put, ...})` and waits for the reply. For quorum, the reply comes after Raft. For async, the reply comes after the Batcher's 1ms window + `submit_async` + `GenServer.reply`. The connection cannot process the next pipelined command until the reply arrives.

### The real bottleneck: Batcher 1ms window × 4 shards

With 4 shards and 1ms flush windows:
- Best case: 4 shards × 1000 flushes/sec = 4000 flushes/sec
- Each flush handles all commands buffered in that 1ms
- At 7,600 ops/sec across 4 shards = 1,900 ops/shard/sec
- 1,900 ops / 1000 flushes = ~2 ops per batch

The batch size is tiny because the window is small and the write rate is modest. Increasing the window would increase batch size but also latency.

## Live Instrumentation Results (pipeline=50, during load)

Sampled process queues 3 times during a 7K ops/sec write workload:

```
Sample 1:
  ferricstore_shard_2: q=68  {StateMachine, :flush_pending_writes, 1}  ← BLOCKED
  ferricstore_shard_3: q=13  {Bitcask.NIF, :v2_fsync, 1}              ← BLOCKED on fsync
  ferricstore_shard_1: q=3   {:lists, :reverse, 1}

Sample 2:
  ferricstore_shard_1: q=0   {Bitcask.NIF, :v2_fsync, 1}              ← BLOCKED on fsync
  ra_ferricstore_raft_log_wal: q=50  {:erts_internal, :dirty_nif_finalizer, 1}  ← WAL BLOCKED

Sample 3:
  ferricstore_shard_3: q=67  {:ra_seq, :drop_prefix, 2}               ← BLOCKED on ra cleanup
```

CPU was at 70% during this test.

## Root Cause: v2_fsync blocks Ra state machine

The Bitcask `v2_fsync` NIF runs **synchronously inside the Ra state machine's `apply/3` callback**. While fsync is running on the normal scheduler (~100-200us on NVMe), the entire Ra server process for that shard is blocked — no other Raft commands can be applied.

The call chain:
1. Ra applies a batch command
2. `StateMachine.apply/3` calls `v2_append_batch` (fast, page cache)
3. `StateMachine.apply/3` calls `v2_fsync` (blocks 100-200us on NVMe)
4. During fsync, the Ra server queues up incoming commands (q=13-68)
5. WAL also gets blocked (`q=50`, stuck on `dirty_nif_finalizer`)

With 4 shards, each doing ~1000 fsyncs/sec (1ms batcher window), the Ra servers spend most of their time in fsync. The 70% CPU is split between actual work and scheduler overhead from blocking NIFs.

## Proposed fixes (in order of impact)

### 1. Move fsync out of Ra state machine apply/3
The state machine should NOT fsync during apply. Instead:
- `apply/3` does `v2_append_batch_nosync` (page cache only, microseconds)
- `apply/3` returns an effect that triggers async fsync
- A separate process (or the BitcaskWriter) does `v2_fsync` and notifies completion
- Callers are replied to AFTER fsync completes (for quorum durability)

This unblocks the Ra server to keep applying commands while fsync runs in parallel.

### 2. Batch fsync across multiple apply calls
Currently each Raft batch triggers one fsync. If the Ra server applied 5 batches before fsyncing, the amortization would be 5x.

### 3. Use v2_fsync_async (Tokio) instead of v2_fsync
The NIF already has `v2_fsync_async` that submits to Tokio and sends a message on completion. The state machine could use this to avoid blocking the Ra process.

### 4. Increase release_cursor_interval
Current: 10,000 commands. At high throughput, Ra snapshots fire constantly. Each snapshot + ETS cleanup takes 25-50ms. Increase to 100,000 or make time-based (~60s).

## Failed Fix: Moving fsync to BitcaskWriter

Attempted: Remove `v2_fsync` from `StateMachine.apply/3`, add `BitcaskWriter.request_fsync` (cast).

Results:
- Quorum pipeline=50: 7,570 → 6,420 ops/sec (**15% worse**)
- Async pipeline=50: 7,580 → 7,769 ops/sec (no change)

Why it failed: Moving fsync to a separate process doesn't help because:
1. The BitcaskWriter still does fsync synchronously (one at a time per shard)
2. Extra process hop adds overhead without reducing total fsync count
3. The real issue is that **callers block until Raft reply**, and Raft reply comes from the Ra server which was blocked on fsync. Moving fsync elsewhere means the Ra server replies faster, but the Batcher still needs to wait for the ra_event callback before replying to callers — and the ra_event only fires after apply/3 returns.

The actual constraint: Ra's `apply/3` is synchronous by design. The return value of `apply/3` IS the result that gets sent back via ra_event. You can't return from `apply/3` and do work after — it's too late.

## Correct approach

The real question is: **can we increase the number of commands per fsync?**

Currently: 1ms batcher window → ~8 commands per batch → 1 fsync per ~8 commands.
If we could get 100 commands per fsync: 100x better amortization.

Two ways:
1. **Increase batcher window** to 5-10ms — more commands accumulate per batch
2. **Use ra:pipeline_command** more aggressively — submit multiple batches before waiting for apply, and do one fsync for all of them

But approach #2 conflicts with how Ra works — each `apply/3` call is independent.

The most promising: **increase batcher window + increase shard count** to get more parallelism AND more commands per fsync.

### 5. For async writes: bypass Ra entirely
Async writes don't need Raft consensus. They could write directly to ETS + Bitcask (nosync) from the Batcher, skipping the Ra server completely. The current path goes: Batcher → AsyncApplyWorker → `v2_append_batch` (still with fsync). The AsyncApplyWorker should use `_nosync` variant.
