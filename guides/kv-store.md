# Key-Value Store

FerricStore includes a durable key-value and data-structure store. FerricFlow is
built on the same storage engine, but the KV store is useful on its own for
sessions, profiles, counters, cache-aside values, locks, rate limits, indexes,
sets, sorted sets, streams, and probabilistic structures.

Use KV when you need current state for a key. Use FerricFlow when you need a
durable lifecycle with claims, leases, retries, history, signals, or fanout.

## Mental Model

```text
client command
  -> native TCP/TLS server or embedded Elixir API
  -> command dispatcher
  -> shard router
  -> per-shard Raft write path for mutations
  -> ETS keydir and disk-backed storage
```

Standalone clients speak the Ferric native binary protocol on port `6388` by
default. Embedded Elixir callers use `FerricStore.*` functions directly. Both
paths execute the same command handlers and use the same storage semantics.

FerricStore is not a memory-only cache. In the normal server path, a write
returns success only after it has passed through the shard durability path and
the current state has been published for reads.

## Basic Operations

Logical command shape:

```text
SET user:42:name alice
GET user:42:name
DEL user:42:name

HSET user:42 profile_name alice
HGET user:42 profile_name

INCR counter:emails_sent
EXPIRE session:abc 3600
```

Embedded Elixir shape:

```elixir
:ok = FerricStore.set("user:42:name", "alice", ttl: :timer.hours(1))
{:ok, "alice"} = FerricStore.get("user:42:name")
{:ok, true} = FerricStore.exists("user:42:name")
:ok = FerricStore.del("user:42:name")
```

The command examples are logical command names and arguments. SDKs encode them
over the native protocol rather than sending a text protocol.

## How Writes Work

For a write such as `SET session:abc value PX 60000`:

1. The command is decoded, authorized, and normalized by the native server or
   embedded API.
2. The router maps the key to a shard. FerricStore uses a 1,024-slot routing
   layer; each slot maps to a shard.
3. The shard batches writes for a short namespace window. The default commit
   window is 1 ms and can be tuned per namespace.
4. The batch is committed through the shard Raft path.
5. The state machine applies the command and publishes the new keydir entry.
6. The caller receives the command result.

Each command is applied atomically: either the command is reflected in current
state or it is not. Multi-key operations are fastest when the keys land on the
same shard. Use hash tags such as `{user:42}:profile` and `{user:42}:cart` when
related keys must be colocated.

## How Reads Work

For `GET session:abc`, FerricStore routes the key to the owning shard and reads
from the shard keydir:

```text
hot value in ETS
  -> return directly

cold value in disk-backed storage
  -> read by stored file/offset
  -> optionally warm back into ETS
  -> return value

expired value
  -> delete keydir entry
  -> return nil/null
```

The keydir stores each key with:

```text
key, hot value or nil, expire_at_ms, LFU counter, file id, offset, value size
```

Small hot values can be returned directly from ETS. Larger or evicted values
keep their disk location in the keydir, so cold reads do not scan files; they
read the exact stored location.

## Hot Cache And Large Values

FerricStore keeps values in the hot ETS keydir only when they are below
`hot_cache_max_value_size` (default: 64 KiB). Larger values remain durable but
are served from disk-backed storage on read.

Values at or above `blob_side_channel_threshold_bytes` (default: 256 KiB) use a
blob side-channel path with a small reference in the main store. This keeps the
hot keydir and normal append path efficient for common small values.

Memory pressure does not delete durable data. The MemoryGuard can evict hot
values from ETS by setting the hot value field to `nil`; the key still has its
disk location and can be read back later.

## Expiry

TTL is stored as an absolute millisecond timestamp.

Common forms:

```text
SET session:abc token PX 60000
EXPIRE session:abc 3600
PEXPIRE session:abc 60000
TTL session:abc
PTTL session:abc
PERSIST session:abc
```

Expired keys are removed lazily when touched and by background sweeps. A key
with no expiry stores `expire_at_ms = 0`.

## Data Structures

FerricStore supports more than plain string keys:

| Family | Use for |
| --- | --- |
| Strings | Raw bytes, JSON blobs, counters, sessions, cache values |
| Hashes | Per-field updates without rewriting one large object |
| Lists | Queues, recent items, ordered append/pop workloads |
| Sets | Membership, tags, uniqueness |
| Sorted sets | Scores, rankings, due-time indexes |
| Streams | Append-only event streams and consumer groups |
| Bitmaps | Dense boolean flags |
| HyperLogLog | Approximate cardinality |
| Bloom/Cuckoo/CMS/TopK/TDigest | Built-in probabilistic structures |
| GEO | Location indexing through sorted-set style geospatial data |

Compound structures are stored under internal keys plus type metadata. For
example, a hash field can be updated independently without reading and
rewriting the full hash value. Commands on the wrong data type return a type
error.

## Streams

Streams are the durable messaging structure in the KV store. Use them when
messages must survive restarts, readers may be offline, or consumers need to
resume from an ID.

Common command shape:

```text
XADD events:orders * order_id 1001 status paid
XLEN events:orders
XRANGE events:orders - + COUNT 10
XREAD COUNT 10 STREAMS events:orders 0
```

Consumer group shape:

```text
XGROUP CREATE events:orders workers 0
XREADGROUP GROUP workers worker-1 COUNT 10 STREAMS events:orders >
XACK events:orders workers 1700000000000-0
```

How FerricStore stores streams:

- Each stream entry is stored as a compound key under the stream key.
- Entry IDs use the Hybrid Logical Clock path for monotonic IDs.
- Stream metadata such as length, first ID, last ID, and sequence counters is
  tracked in ETS for fast reads.
- `XREAD BLOCK`, `XGROUP`, `XREADGROUP`, and `XACK` require native TCP mode
  because they depend on connection/session wait state.

Use streams for durable event logs, inboxes, handoff queues, audit trails, and
consumer-group fanout where replay matters. For durable workflow jobs with
leases, retries, state transitions, and history, use FerricFlow instead of
building that lifecycle manually on streams.

## Pub/Sub

Pub/Sub is live connection fanout. It is not a durable data structure and does
not store messages. Use it for notifications where missing a message is
acceptable or where subscribers can recover by reading current state from KV,
Streams, or Flow.

Common command shape:

```text
SUBSCRIBE events:orders
PUBLISH events:orders "order:1001:paid"
UNSUBSCRIBE events:orders

PUBSUB CHANNELS events:*
PUBSUB NUMSUB events:orders
PUBSUB NUMPAT
```

Pub/Sub behavior:

- Messages are delivered only to subscribers connected at publish time.
- A publish returns the number of subscribers that received the message.
- Pattern subscriptions can match multiple channels.
- Pub/Sub state belongs to active native server sessions, not to Bitcask or
  Raft storage.

Use Pub/Sub to wake workers, invalidate local caches, notify dashboards, or
broadcast that a durable key/stream/flow changed. Use Streams when the message
itself must be retained and replayed.

| Need | Use |
| --- | --- |
| Replayable events | Streams |
| Consumer groups and acknowledgements | Streams |
| Live notification only | Pub/Sub |
| Durable job lifecycle with retries and leases | FerricFlow |

## FerricStore-Native KV Helpers

FerricStore also provides native helper commands that are useful in applications:

| Command | Use |
| --- | --- |
| `CAS` | Compare-and-swap for optimistic updates |
| `LOCK`, `UNLOCK`, `EXTEND` | Lease-style distributed lock primitives |
| `RATELIMIT.ADD` | Atomic rate limit counters |
| `FETCH_OR_COMPUTE` | Cache-aside stampede protection |
| `FERRICSTORE.KEY_INFO` | Inspect type, size, TTL, and hot/cold status |

These are part of the FerricStore command layer, not separate services.

## Key Design

Prefer predictable namespaces:

```text
session:{user:42}:token
profile:{user:42}:settings
ratelimit:{client:abc}:minute
cache:product:99
```

The namespace before the first `:` is used for namespace commit-window tuning.
The `{...}` hash tag, when present, controls shard routing. Use hash tags for
related keys that must be updated together; avoid one global tag that pushes all
traffic to one shard.

## Operational Notes

- `INFO keyspace`, `INFO bitcask`, and `FERRICSTORE.HOTNESS` help inspect store
  behavior.
- `FERRICSTORE.KEY_INFO key` shows whether a value is hot or cold.
- `SCAN` and `KEYS` filter internal compound keys from normal user output.
- `FLUSHDB` and `FLUSHALL` are destructive and should be protected by ACL.
- There is one logical database; `SELECT` is not used for namespacing.

For command syntax, see [Commands Reference](commands.md). For the deeper
write/read path and storage internals, see [Architecture](architecture.md).
