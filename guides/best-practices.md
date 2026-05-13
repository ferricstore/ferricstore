# Best Practices

## Use Hash Tags for Related Keys

FerricStore shards data across multiple independent Raft groups. Each shard has its own WAL, its own fsync, and its own ETS table. When related keys land on the same shard, writes are batched into a single Raft command and a single fsync. When they land on different shards, each shard does its own fsync independently.

### How hash tags work

FerricStore supports Redis hash tags: if a key contains `{tag}`, only the content between the first `{` and the next `}` is used for shard routing. Everything outside the braces is ignored for routing purposes.

```
{user:42}:session   → hashes on "user:42"
{user:42}:profile   → hashes on "user:42"  ← same shard
{user:42}:cart      → hashes on "user:42"  ← same shard
user:42:session     → hashes on full key   ← could be any shard
```

### Why this matters for writes

Every quorum write goes through Raft consensus and fsync. The group-commit batcher collects writes within a time window and submits them as a single batch to Raft — one fsync for the entire batch. This only works when the writes are on the same shard.

**Without hash tags** — MSET of 5 related keys might hit 5 different shards, causing 5 separate Raft commits and 5 separate fsyncs.

**With hash tags** — MSET of 5 related keys hits one shard, causing 1 Raft commit and 1 fsync.

### When to use hash tags

**User data** — session, profile, preferences, cart:
```
{user:42}:session
{user:42}:profile
{user:42}:preferences
```

**Entity with multiple fields as separate keys:**
```
{order:1001}:status
{order:1001}:items
{order:1001}:total
```

**Rate limiting with related counters:**
```
{api:client-xyz}:minute
{api:client-xyz}:hour
{api:client-xyz}:day
```

**Transactions (MULTI/EXEC with WATCH):**
```redis
WATCH {account:A}:balance {account:A}:history
MULTI
SET {account:A}:balance 950
LPUSH {account:A}:history "withdraw:50"
EXEC
```

All watched and written keys must be on the same shard for the transaction to work atomically. Hash tags guarantee this.

### When NOT to use hash tags

**High-cardinality independent keys** — if keys are unrelated and accessed independently, hash tags create hot shards. Let them spread naturally:

```
cache:product:1     ← no tag, spread across shards
cache:product:2
cache:product:3
```

**Cross-entity operations** — if you need to atomically update keys belonging to different entities, hash tags won't help (they'd force all entities to one shard). FerricStore handles this with cross-shard operations, but it's slower. Consider whether you truly need atomicity across entities.

### Cross-shard operations

When keys span multiple shards, FerricStore uses a mini-percolator protocol: lock keys in shard order, write intent, execute, unlock. This is correct but slower than single-shard operations because it requires multiple Raft round-trips.

Some Redis-compatible multi-key commands require all keys to live on one shard. If keys span shards for one of those commands, FerricStore returns a `CROSSSLOT` error with guidance:

```
CROSSSLOT Keys in request don't hash to the same slot.
Use hash tags {tag} to colocate keys.
```

### Hash tag rules

- `{tag}` — only the first `{...}` pair is used
- `{}` — empty tag is ignored, full key is used for routing
- `{` without `}` — no tag, full key is used
- Nested `{{tag}}` — outer braces are the tag, content is `{tag`

## Tune Commit Windows per Namespace

All standalone-server writes use the Raft durability path. Namespace prefixes can still tune the group-commit window:

```redis
# Latency-sensitive state: short commit window
FERRICSTORE.CONFIG SET "session:" window_ms 1
FERRICSTORE.CONFIG SET "order:" window_ms 1

# Write-heavy data: larger batches
FERRICSTORE.CONFIG SET "cache:" window_ms 5
FERRICSTORE.CONFIG SET "ratelimit:" window_ms 5
```

Design your key naming scheme around this:
```
session:{user:42}:token     → namespace "session"
cache:{product:99}:details  → namespace "cache"
```

## Prefer Compound Operations Over Multiple Round-Trips

Instead of multiple GET/SET commands, use the built-in compound operations:

| Instead of | Use |
|-----------|-----|
| GET + conditional SET | `CAS key expected new_value` |
| SETNX + manual expiry | `SET key value NX EX 300` |
| GET + SET (cache miss) | `FETCH_OR_COMPUTE key ttl_ms` |
| SETNX + GET (lock) | `LOCK key owner ttl_ms` |
| Multiple INCR + check | `RATELIMIT.ADD key window_ms max_count` |

Each round-trip is a Raft commit. Fewer round-trips = fewer fsyncs = higher throughput.

## Pipeline Commands

When sending multiple independent commands, pipeline them. Pipelines allow the group-commit batcher to collect more writes per batch, reducing the number of fsyncs:

```redis
# Without pipelining: 3 separate Raft batches, 3 fsyncs
SET key1 val1
SET key2 val2
SET key3 val3

# With pipelining: potentially 1 Raft batch, 1 fsync
PIPELINE
SET key1 val1
SET key2 val2
SET key3 val3
```

Most Redis client libraries support pipelining natively (e.g., `Redix.pipeline/2` in Elixir).

## Size Your Values for the Hot Cache

FerricStore keeps values in ETS (hot cache) only if they're smaller than `hot_cache_max_value_size` (default: 64KB). Larger values are stored cold — reads go to disk via Bitcask. Values at or above `blob_side_channel_threshold_bytes` (default: 256KB) are stored as content-addressed blob files with a small Bitcask reference.

Expired or overwritten large values become eligible for blob cleanup when their
live Bitcask reference disappears. The automatic blob GC sweeper is enabled by
default and runs conservatively: it first checks whether blob files exist, then
builds the live reference set from the shard keydir before deleting unreferenced
blob files. `FERRICSTORE.BLOBGC` can be used to force the same cleanup manually.

If your values are consistently larger than this threshold, reads always hit disk. Consider:
- Splitting large values into smaller fields (use Hash commands)
- Increasing `FERRICSTORE_HOT_CACHE_MAX_VALUE_SIZE` if you have the RAM
- Accepting cold reads for large values (still much faster than a database query)
