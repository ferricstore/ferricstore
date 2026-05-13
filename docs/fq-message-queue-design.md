# FerricStore Message Queue (FQ.*) — Design Plan

## Context

FerricStore already has Redis Streams (XADD/XREADGROUP/XACK), Lists (BLPOP/BRPOP/LMOVE), and PubSub — but no RabbitMQ-style routing layer (exchanges, bindings, DLQ, TTL, prefetch). This adds the best parts of RabbitMQ on top of the existing Raft-replicated infrastructure.

**Key insight: this is NOT hard.** The hard distributed systems problems (Raft consensus, cross-shard atomics, blocking waiters, HLC ordering, compound key storage) are already solved. What's missing is a routing + state management layer — pure application logic riding on existing primitives.

**Estimated effort: ~2-3 weeks total. MVP (queues + direct publish + consume + ack) in under a week.**

## Performance: FerricStore FQ vs RabbitMQ

FerricStore already achieves **200K ops/sec with quorum** (3 shards, WAL batch-then-fsync, Bitcask background fsync). RabbitMQ persistent mirrored queues do ~30-50K msg/sec. FQ writes go through the same Raft path, so FQ inherits this throughput.

| Dimension | RabbitMQ (persistent, mirrored) | FerricStore FQ |
|-----------|------|------|
| Publish throughput | ~30-50K msg/sec | ~200K msg/sec (quorum, 3 shards) |
| Consume throughput | ~30-50K msg/sec | 300K+ (ETS reads, no WAL) |
| Replication | Mirror queues (async, can lose) | Raft quorum (stronger) |
| Durability | Journal + mirror | WAL batch fsync + Bitcask bg fsync |

FQ adds minimal overhead on top — exchange routing is an ETS lookup, message storage reuses Streams (same WAL path). The only new write is consumer group persistence (one extra compound key per consume/ack batch, coalesced by Batcher).

### Where FerricStore FQ Wins

- **Stronger durability** — Raft quorum > RabbitMQ mirror queues
- **Single system** — no separate RabbitMQ cluster to manage. Queue + cache + data store in one
- **Any Redis client works** — no AMQP library needed, just RESP3
- **Reads are free** — ETS hot cache, consumers don't touch disk
- **4-6x faster** publish throughput with persistent + replicated messages

### Where RabbitMQ Wins

- **Ecosystem** — AMQP protocol, management UI, plugins, Shovel, Federation
- **Priority queues** — native support (FQ defers this)
- **Headers exchange** — FQ doesn't implement this
- **Management UI** — FQ has no dashboard

## Feature Comparison: FerricStore FQ vs RabbitMQ

| Feature | RabbitMQ | FerricStore FQ | Same? |
|---------|----------|---------------|-------|
| Exchanges (direct, fanout, topic) | Yes | Yes | Yes |
| Durable queues | Yes | Yes (Raft-replicated) | Better |
| Consumer groups + ACK | Yes | Yes (XREADGROUP) | Yes |
| Dead-letter queues | Yes | Yes | Yes |
| Message TTL | Yes | Yes | Yes |
| Prefetch / QoS | Yes | Yes | Yes |
| NACK + redelivery | Yes | Yes | Yes |
| Blocking consumers | Yes | Yes (BEAM receive) | Yes |
| Priority queues | Yes | Yes (multi-stream) | Yes |
| Headers exchange | Yes | No | Missing |
| AMQP protocol | Yes | No — RESP3 only | Different |
| Shovel / Federation | Yes | No | Missing |
| Management UI | Yes | No | Missing |
| Delayed messages | Plugin | No | Could add |

## What RabbitMQ Features to Borrow

| Feature | Value | Reuses Existing |
|---------|-------|--------|
| Exchange routing (direct, fanout, topic) | Core | PubSub `glob_to_regex/1` for topic matching |
| Durable consumer ack + redelivery | Core | Stream consumer groups (XREADGROUP/XACK) |
| Priority queues | Core | Multiple backing streams per queue, priority-ordered consume |
| Dead-letter queues | High | Exchange routing + retry counter |
| Message TTL | High | HLC timestamps + XTRIM |
| Prefetch/QoS | Medium | Per-consumer pending count |

## Existing Infrastructure Reuse Map

| Queue Concept | Backed By | Already Exists? |
|---|---|---|
| Queue messages | Streams (`X:` compound keys in Bitcask via Raft) | Yes |
| Blocking consumers | Waiters ETS + BEAM `receive after` | Yes |
| Message ordering | HLC timestamps (lock-free monotonic) | Yes |
| Durability + replication | Raft consensus per shard | Yes |
| Cross-queue atomic moves | CrossShardOp (Lock->Intent->Execute->Unlock) | Yes |
| Topic pattern matching | `PubSub.glob_to_regex/1` | Yes |
| Consumer groups | Stream consumer groups (ETS) | Partially — persistence missing (Phase 1) |

**Genuinely new code:**
- `FQ` command module + dispatcher wiring (~follows Bloom/Cuckoo pattern)
- Exchange metadata storage + routing logic
- DLQ routing (retry counter + re-route)
- Message TTL expiry (per-message + periodic sweep)
- Prefetch tracking (per-consumer unacked count gating)

**No new:** storage engines, NIF code, Raft primitives, or ETS table patterns.

## How It Works — Producer and Consumer

### Setup (one-time)

```bash
# Declare a simple queue (no priorities)
redis-cli FQ.QUEUE.DECLARE orders

# Declare a priority queue (3 levels: 0=low, 1=normal, 2=high)
redis-cli FQ.QUEUE.DECLARE orders PRIORITIES 3

# Optionally: declare an exchange + bind the queue
redis-cli FQ.EXCHANGE.DECLARE notifications fanout
redis-cli FQ.BIND notifications orders "*"
redis-cli FQ.BIND notifications audit "*"
```

### Producer (publishes messages)

**Direct to queue (simplest):**
```bash
# Publish a message directly to a named queue
redis-cli FQ.PUBLISH "" orders '{"order_id": 123, "status": "new"}'
```

**Via exchange (decoupled routing):**
```bash
# Publish to exchange — routes to all bound queues based on type
redis-cli FQ.PUBLISH notifications order.created '{"order_id": 123}'

# Direct exchange: only queues bound with matching routing key get it
# Fanout exchange: ALL bound queues get it
# Topic exchange: queues with glob pattern match get it (e.g. "order.*")
```

The producer doesn't need to know which queues exist. It publishes to an exchange with a routing key. The exchange routes to the right queues based on bindings.

### Consumer (receives + acknowledges messages)

```bash
# Consume messages (returns immediately if available)
redis-cli FQ.CONSUME orders mygroup worker-1 COUNT 10

# Consume with blocking (waits up to 5000ms for new messages)
redis-cli FQ.CONSUME orders mygroup worker-1 COUNT 10 BLOCK 5000

# Returns: array of [message_id, field, value, field, value, ...]
# Example: [["1714000000000-0", "body", "{\"order_id\": 123}", "_rk", "order.created"]]
```

**After processing, acknowledge:**
```bash
# ACK — message processed successfully, remove from pending
redis-cli FQ.ACK orders mygroup 1714000000000-0

# NACK — processing failed, put back for redelivery
redis-cli FQ.NACK orders mygroup 1714000000000-0

# REJECT — permanent failure, send to dead-letter queue (if configured)
redis-cli FQ.REJECT orders mygroup 1714000000000-0
```

### Consumer Groups (multiple workers)

Multiple workers in the same group share the load — each message delivered to exactly one worker:

```bash
# Worker 1
redis-cli FQ.CONSUME orders mygroup worker-1 COUNT 10 BLOCK 5000

# Worker 2 (same group — gets DIFFERENT messages)
redis-cli FQ.CONSUME orders mygroup worker-2 COUNT 10 BLOCK 5000

# Worker 3 in a DIFFERENT group — gets ALL messages (independent cursor)
redis-cli FQ.CONSUME orders analytics-group worker-3 COUNT 10 BLOCK 5000
```

### Dead-Letter Queue (failed message handling)

```bash
# Declare queue with DLQ settings
redis-cli FQ.QUEUE.DECLARE orders MAXRETRIES 3 DLX dead-letters DLX.RK failed

# Declare the dead-letter exchange + queue
redis-cli FQ.EXCHANGE.DECLARE dead-letters direct
redis-cli FQ.QUEUE.DECLARE failed-orders
redis-cli FQ.BIND dead-letters failed-orders failed

# Flow: publish -> consume -> REJECT -> (after 3 rejects) -> appears in failed-orders
```

### Priority Queues

```bash
# Declare queue with 3 priority levels
redis-cli FQ.QUEUE.DECLARE orders PRIORITIES 3

# Publish with different priorities (default is 0 = lowest)
redis-cli FQ.PUBLISH "" orders '{"type": "refund"}'    PRIORITY 2   # high
redis-cli FQ.PUBLISH "" orders '{"type": "standard"}'                # default 0, low
redis-cli FQ.PUBLISH "" orders '{"type": "update"}'     PRIORITY 1   # normal

# Consumer gets highest priority first — no change to consume command
redis-cli FQ.CONSUME orders mygroup worker-1 COUNT 10 BLOCK 5000
# Returns: [refund_msg, update_msg, standard_msg]  (priority 2, 1, 0)
```

The client doesn't need to know about priority internals. FQ.CONSUME always drains highest priority first. Internally, each priority level is a separate backing stream (`_fq:orders:p0`, `_fq:orders:p1`, `_fq:orders:p2`). FQ.CONSUME reads from p2 first, then p1, then p0, filling up to COUNT.

ACK/NACK/REJECT work transparently — the pending map tracks which priority level each message came from, so the ack routes to the correct backing stream.

**Filtering by priority on consume:**
```bash
# Only consume high-priority messages (priority >= 2)
redis-cli FQ.CONSUME orders mygroup urgent-worker COUNT 10 BLOCK 5000 MINPRIORITY 2

# Dedicated worker for all priorities (default — no filter)
redis-cli FQ.CONSUME orders mygroup general-worker COUNT 10 BLOCK 5000
```

This lets you have dedicated workers for urgent messages while general workers handle everything. `MINPRIORITY n` skips all backing streams below level n.

### Message TTL

```bash
# Queue-level TTL: all messages expire after 60 seconds
redis-cli FQ.QUEUE.DECLARE temp-queue TTL 60000

# Per-message TTL (set on publish via metadata fields)
redis-cli FQ.PUBLISH "" orders '{"data": "urgent"}' _ttl 5000
```

### Prefetch / Backpressure

```bash
# Limit to 5 unacked messages per consumer — won't get more until ACKed
redis-cli FQ.CONSUME orders mygroup worker-1 COUNT 5 PREFETCH 5 BLOCK 5000
```

### Introspection

```bash
redis-cli FQ.QUEUE.INFO orders        # length, consumers, pending count
redis-cli FQ.EXCHANGE.INFO notifications  # type, bindings
redis-cli FQ.PENDING orders mygroup    # list pending (unacked) messages
```

## Architecture

```
Client  ->  RESP3  ->  Dispatcher  ->  Ferricstore.Commands.FQ.handle/3
                                         |
  FQ.PUBLISH --> exchange lookup -> binding scan -> XADD per matched queue
  FQ.CONSUME --> XREADGROUP on backing stream _fq:{queue}
  FQ.ACK     --> XACK + update persisted consumer group
  FQ.NACK    --> reset pending entry for redelivery
  FQ.REJECT  --> increment retries, route to DLQ if max exceeded
```

Each queue is backed by a Stream under the hood — stored as `X:_fq:{queue_name}\0{ms}-{seq}`. This means XRANGE/XTRIM/XLEN all work on queue storage with zero new code.

### Command Flow Detail

**FQ.PUBLISH `<exchange> <routing_key> <body> [field value ...]`:**
1. Read exchange entry `EX:{exchange}` via `Ops.get` (ETS hot cache hit)
2. Deserialize to get `%{type, bindings}`
3. Filter bindings by routing key:
   - Direct: `binding_rk == publish_rk`
   - Fanout: all bindings
   - Topic: `PubSub.glob_to_regex(binding_rk)` matches `publish_rk`
4. For each matched queue: XADD to `_fq:{queue}` with body as field-value pairs plus `_rk`, `_ex` metadata
5. Notify stream waiters for each queue (wakes blocked FQ.CONSUME)
6. Return message ID(s)

**FQ.CONSUME `<queue> <group> <consumer> [COUNT n] [BLOCK ms] [PREFETCH n]`:**
1. If prefetch set: count pending entries for this consumer, return empty if >= limit
2. Read queue metadata `QD:{queue}` to get priority count
3. Priority-ordered consume (highest first):
   - Try XREADGROUP on `_fq:{queue}:p{max}` — got messages? Add to result.
   - If result < COUNT, try `_fq:{queue}:p{max-1}`, etc. down to `p0`.
   - Stop when COUNT reached or all levels exhausted.
4. If no messages from any level and BLOCK specified:
   - Register stream waiter on ALL priority streams (`_fq:{queue}:p0` through `p{max}`)
   - Enter `generic_wait_loop` (already supports multi-stream wakeup)
   - On wake from any stream, re-run priority-ordered consume from step 3
5. For each delivered message: record `{msg_id, consumer, timestamp, attempt_count, priority_level}` in unified pending map
6. Persist consumer group state via Raft
7. Return entries (consumer sees flat list, unaware of priority levels)

**FQ.ACK / FQ.NACK / FQ.REJECT:**
- ACK: look up `priority_level` from pending map, XACK on correct backing stream `_fq:{queue}:p{level}`, remove from pending, persist group state
- NACK: reset deliver_ts to 0 (immediate redelivery on next consume at same priority level)
- REJECT: increment attempt_count; if >= max_retries, route to DLQ exchange; else requeue at same priority level

## Storage Format (Compound Keys)

| Prefix | Key Format | Value |
|--------|-----------|-------|
| `EX:` | `EX:{exchange_name}` | `term_to_binary(%{type, durable, bindings: [{queue, rk, args}, ...]})` |
| `QD:` | `QD:{queue_name}` | `term_to_binary(%{priorities, max_len, msg_ttl_ms, dlx, dlx_rk, max_retries})` |
| `X:` | `X:_fq:{queue}:p{N}\0{ms}-{seq}` | Message body per priority level (reuses Streams) |
| `CG:` | `CG:_fq:{queue}\0{group}` | Persisted consumer group state (unified across priorities) |

### Priority Queue Storage

A queue with `PRIORITIES 3` has 3 backing streams: `_fq:orders:p0`, `_fq:orders:p1`, `_fq:orders:p2`. A queue without `PRIORITIES` (or `PRIORITIES 1`) has a single stream `_fq:orders:p0` — zero overhead, same as before.

The consumer group state is **unified** across all priority streams. The pending map stores `{msg_id => {consumer, timestamp, attempt_count, priority_level}}`. FQ.ACK looks up the priority level from the pending entry to route the ack to the correct backing stream.

Bindings are embedded inside the exchange entry (not separate keys) to avoid cross-shard scan issues — see Review Issue 1.

Add to `internal_key?/1` in `compound_key.ex`: `EX:`, `QD:`, `CG:` prefixes.

## API (Redis RESP3 Commands)

### Topology
```
FQ.EXCHANGE.DECLARE <name> <type:direct|fanout|topic> [DURABLE]
FQ.EXCHANGE.DELETE <name>
FQ.QUEUE.DECLARE <name> [PRIORITIES n] [MAXLEN n] [TTL ms] [DLX exchange] [DLX.RK key] [MAXRETRIES n]
FQ.QUEUE.DELETE <name>
FQ.BIND <exchange> <queue> <routing_key>
FQ.UNBIND <exchange> <queue> <routing_key>
```

### Publish / Consume
```
FQ.PUBLISH <exchange> <routing_key> <body> [PRIORITY n] [field value ...]
FQ.CONSUME <queue> <group> <consumer> [COUNT n] [BLOCK ms] [PREFETCH n] [MINPRIORITY n]
FQ.ACK <queue> <group> <id> [id ...]
FQ.NACK <queue> <group> <id> [id ...]
FQ.REJECT <queue> <group> <id> [id ...]
```

### Introspection
```
FQ.QUEUE.INFO <queue>
FQ.EXCHANGE.INFO <exchange>
FQ.PENDING <queue> <group> [consumer] [COUNT n]
```

## Implementation Phases

### Phase 1: Persist Consumer Group State (1-2 days)

**Fixes a critical bug that affects existing Streams too — consumer group state is currently ETS-only and lost on node restart.**

Currently `Ferricstore.Stream.Groups` ETS table holds all consumer group state (pending map, last delivered ID, consumer registrations). It's created empty on startup in `init_tables/0` with no hydration from Bitcask. A node restart or leader failover means:
- Consumers lose their position (re-read from beginning or miss messages)
- Pending messages (delivered but not ACKed) are forgotten — no redelivery
- At-most-once delivery instead of at-least-once

**Fix:**
- On XREADGROUP/XACK, write group state to `CG:{stream}\0{group}` compound key via Raft
- On shard startup, prefix-scan `CG:*` from Bitcask, hydrate `@groups_table` ETS
- Batch group state updates through Batcher (1ms coalescing window deduplicates same-group updates)

**Performance concern:** Adds a Raft write per XREADGROUP/XACK. Mitigated by Batcher coalescing — same-group updates within 1ms window = 1 Raft write. Needs load testing to confirm acceptable throughput impact.

**Files:**
- `apps/ferricstore/lib/ferricstore/commands/stream.ex` — add Raft writes after group mutations (lines 807-898)
- `apps/ferricstore/lib/ferricstore/store/compound_key.ex` — add `CG:` to `internal_key?/1`
- `apps/ferricstore/lib/ferricstore/store/shard.ex` — hydrate groups on startup

### Phase 2: Core Queue Commands (3-4 days)
- Create `apps/ferricstore/lib/ferricstore/commands/fq.ex` — `handle/3` following Bloom/Cuckoo pattern
- Wire `FQ.*` commands into `dispatcher.ex` — add `@fq_cmds`, `:fq` tag to `@cmd_dispatch_map`
- `FQ.QUEUE.DECLARE/DELETE` — write/delete `QD:` compound key (includes `priorities` count)
- `FQ.PUBLISH` (direct-to-queue, no exchange yet) — route to `_fq:{queue}:p{priority}` + `notify_stream_waiters`
- `FQ.CONSUME` — priority-ordered XREADGROUP across `_fq:{queue}:p{max}` down to `p0`, unified pending map
- `FQ.CONSUME` with `MINPRIORITY n` — only consume from priority levels >= n (skip lower levels)
- `FQ.ACK` — look up priority from pending map, XACK correct backing stream, persist group state
- Wire `FQ.CONSUME BLOCK` into `connection/blocking.ex`:
  1. Try immediate priority-ordered consume
  2. If empty, register stream waiter on ALL priority streams for this queue
  3. Enter `generic_wait_loop` with `:stream_waiter_notify` message
  4. On wake from any priority stream, re-run priority-ordered consume

**Files:**
- `apps/ferricstore/lib/ferricstore/commands/fq.ex` (new)
- `apps/ferricstore/lib/ferricstore/commands/dispatcher.ex` — add FQ routing
- `apps/ferricstore_server/lib/ferricstore_server/connection/blocking.ex` — add FQ.CONSUME BLOCK

### Phase 3: Exchange Routing (2-3 days)
- `FQ.EXCHANGE.DECLARE/DELETE` — write/delete `EX:` compound key (includes bindings list)
- `FQ.BIND/UNBIND` — read-modify-write the `EX:` key's bindings list (single Raft write)
- Direct exchange: `binding_rk == publish_rk`
- Fanout exchange: all bound queues
- Topic exchange: Redis-style glob matching — reuse `PubSub.glob_to_regex/1` directly (no new module)
- FQ.PUBLISH reads exchange entry (single `Ops.get`), iterates bindings, XADDs per matched queue

**Files:**
- `apps/ferricstore/lib/ferricstore/commands/fq.ex` — exchange/bind commands
- `apps/ferricstore/lib/ferricstore/store/compound_key.ex` — add `EX:`, `QD:` prefixes

### Phase 4: Reliability Features (2-3 days)
- `FQ.NACK` — reset pending entry deliver_ts for immediate redelivery
- `FQ.REJECT` — increment attempt_count; if >= max_retries, look up queue's DLX from `QD:` metadata, publish to DLX exchange, ACK original
- Message TTL — `_exp` metadata field on publish (absolute timestamp via `HLC.now_ms() + ttl_ms`), skip expired on consume + auto-ACK, periodic XTRIM sweep via GenServer timer
- Prefetch/QoS — count pending per consumer in group state, gate FQ.CONSUME when count >= prefetch limit, wake blocked consumers on FQ.ACK

### Phase 5: Tests (2-3 days)
- Publish -> consume -> ack cycle (basic round-trip)
- Consumer group persistence (restart node, verify position preserved)
- DLQ flow (publish -> consume -> reject x max_retries -> appears in DLQ)
- Topic routing patterns (glob matching: `orders.*` matches `orders.new`)
- TTL expiry (publish with TTL, wait, verify expired on consume)
- Prefetch backpressure (consume with PREFETCH 1, verify second consume blocks)
- Cluster: consumer group state survives leader failover

## Key Design Decisions

1. **Queues = Streams under the hood** — `_fq:` prefix namespace avoids collision with user streams. All existing Stream internals (XADD, XTRIM, compound key storage, waiter notifications) reused directly.

2. **Priority queues = multiple backing streams** — `_fq:orders:p0` through `_fq:orders:pN`. FQ.CONSUME drains highest first. FQ.PUBLISH routes to the right stream by priority. No PRIORITIES option = single stream, zero overhead. Unified consumer group pending map tracks priority level per message for transparent ACK routing.

3. **MINPRIORITY on consume** — Lets dedicated workers handle only high-priority messages while general workers handle all. Simply skips lower-priority backing streams during the priority-ordered read loop.

4. **Bindings embedded in exchange entry** — Avoids cross-shard scan issues. One `EX:` key per exchange contains the full bindings list. FQ.BIND does a read-modify-write. FQ.PUBLISH reads one key to get all routing info.

5. **Topic matching = Redis-style globs** — Reuse `PubSub.glob_to_regex/1` directly for topic exchange routing (`*`/`?` patterns). No new matcher module needed.

6. **Consumer group persistence** — Adds Raft write per XREADGROUP/XACK. Batcher coalescing mitigates (same-group updates within 1ms = 1 write). This is the one item that needs load testing.

7. **No ETS caching for MVP** — Exchange/binding reads go through normal `Ops.get` path (hits ETS hot cache via shard keydir). Avoids the cross-node cache invalidation problem entirely. Add dedicated caching later if profiling shows it's needed.

## Review Findings — Issues to Address

### Issue 1: Exchange/Binding Metadata Must Not Be Sharded by Compound Key (FIXED)

**Problem:** Compound keys are sharded by `phash2(full_key)` in `Router.shard_for/2` (router.ex:652). If bindings were stored as separate `QB:` keys, `QB:orders\0queue_a\0rk1` and `QB:orders\0queue_b\0rk2` would land on different shards. A prefix scan `QB:orders\0*` via `compound_scan` only hits one shard — silently missing bindings on other shards.

**Fix applied:** Bindings are embedded inside the `EX:` exchange entry. One key = one shard = complete binding list. No cross-shard scan needed.

### Issue 2: Stream Entries Scatter Across Shards (Existing Limitation)

**Problem:** `Ops.put(store, "X:_fq:myqueue\0123-0", ...)` hashes the full compound key, so different message IDs for the same stream land on different shards. Streams work today because `stream_keys_for` (stream.ex:1037) calls `Ops.keys(store)` which returns ALL keys globally, then filters by prefix — an O(n) scan over the entire store.

**Impact:** Not new to FQ. Same performance as existing Streams. Acceptable for MVP.

**Future optimization:** Use hash tags: `X:{_fq:myqueue}\0123-0` so all entries land on one shard. Replace global `Ops.keys` scan with single-shard `compound_scan`. This benefits all Streams, not just FQ.

### Issue 3: Consumer Group State Lost on Restart (FIXED in Phase 1)

**Problem:** `Ferricstore.Stream.Groups` ETS table is global, initialized empty on startup (`init_tables/0`, stream.ex:274), no hydration from Bitcask. Consumer group state (pending map, last_delivered_id) lost on any node restart.

**Fix:** Phase 1 persists group state to `CG:` compound keys via Raft, hydrates from Bitcask on shard startup. Benefits both existing Redis Streams and FQ.

### Issue 4: Fan-out Publish is Not Atomic (Accepted)

**Problem:** FQ.PUBLISH to a fanout exchange XADDs to N queues sequentially. Crash mid-publish = partial delivery.

**Accepted:** Matches RabbitMQ behavior — fanout is not transactional across queues. Per-queue delivery is durable (Raft quorum). Document as at-most-once fan-out, at-least-once per-queue.

**Future fix:** Write fan-out intent record via Raft before delivering. On crash recovery, replay incomplete intents. Similar to existing `CrossShardOp` intent pattern.

### Issue 5: ETS Cache Invalidation in Cluster (Deferred)

**Problem:** No cross-node ETS invalidation mechanism. Node A declares exchange, node B doesn't know.

**MVP solution:** Don't cache — read exchange metadata via normal `Ops.get` path (hits shard ETS keydir, fast). Skip the problem entirely.

**Future fix:** Internal PubSub channel for metadata invalidation events.

### Issue 6: Queue Deletion Cleanup (Solvable)

**Problem:** `FQ.QUEUE.DELETE` must delete messages, metadata, bindings, and consumer groups scattered across shards.

**Solution:**
- Bindings: update each exchange's binding list (single-key approach from Issue 1)
- Messages: `Ops.keys(store)` filtered by `X:_fq:myqueue\0` prefix, delete each (same O(n) scan Streams uses for XTRIM)
- Consumer groups: same scan+delete for `CG:_fq:myqueue\0*`
- Queue metadata: single `Ops.delete` for `QD:myqueue`
- Run cleanup in background task so DELETE returns immediately

## Verification

1. `mix test` — existing tests pass (no regressions)
2. New test file `test/ferricstore/commands/fq_test.exs` covering all phases
3. Manual redis-cli test: `FQ.QUEUE.DECLARE myq` -> `FQ.PUBLISH` -> `FQ.CONSUME` -> `FQ.ACK`
4. Cluster test: kill leader during consume, verify consumer group state recovered on new leader
5. Load test: measure FQ.PUBLISH throughput vs direct XADD to confirm minimal overhead
