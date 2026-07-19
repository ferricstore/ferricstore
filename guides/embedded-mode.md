# Embedded Mode

Embedded mode runs FerricStore inside your Elixir application. There is no TCP listener and no wire serialization; your code calls the public `FerricStore` module directly.

Use this guide after [Getting Started](getting-started.md). It covers setup, first commands, behavior differences from native TCP mode, and the embedded API reference.

## When to Use Embedded Mode

- Your application and cache run on the same BEAM node
- You want microsecond read latency (~1-5us for hot keys) with zero-copy binaries
- You want direct Elixir API access without native TCP serialization overhead
- You don't need external SDK clients from another app to connect
- You want the full FerricStore feature set (Raft durability, LFU eviction, probabilistic structures) as a library dependency

## Setup

### 1. Add the Dependency

Only add `ferricstore` -- you do not need `ferricstore_server`:

```elixir
# mix.exs
def deps do
  [
    {:ferricstore, "~> 0.9.1"}
  ]
end
```

### 2. Configure

```elixir
# config/config.exs
config :ferricstore, :data_dir, "priv/ferricstore_data"
config :ferricstore, :shard_count, 0  # 0 = auto-detect from CPU cores
config :ferricstore, :max_memory_bytes, 1_073_741_824  # 1 GB
config :ferricstore, :eviction_policy, :volatile_lru
```

In embedded mode, these options are **not used** and can be omitted:
- `:native_port` (no TCP listener)
- `:health_port`, `:health_probe_port` (no HTTP endpoints)
- `:native_tls_port`, `:native_tls_cert_file`, `:native_tls_key_file` (no TLS)
- `:native_response_chunk_bytes` and response coalescing settings (no TCP sends)

### 3. Start Using It

FerricStore starts automatically with your application. The public API module is `FerricStore`; internal supervision modules use the internal `Ferricstore.*` namespace. Once started, call functions on `FerricStore`:

```elixir
:ok = FerricStore.set("session:abc", session_data, ttl: :timer.minutes(30))
{:ok, data} = FerricStore.get("session:abc")
```


## Quick Examples

Store a short-lived cache value:

```elixir
:ok = FerricStore.set("cache:user:42", :erlang.term_to_binary(user), ttl: :timer.minutes(10))
{:ok, encoded} = FerricStore.get("cache:user:42")
user = :erlang.binary_to_term(encoded)
```

Use an atomic counter:

```elixir
{:ok, 1} = FerricStore.incr("rate:user:42")
:ok = FerricStore.expire("rate:user:42", 60_000)
```

Use a hash for structured fields:

```elixir
:ok = FerricStore.hset("profile:42", %{"name" => "alice", "plan" => "pro"})
{:ok, "pro"} = FerricStore.hget("profile:42", "plan")
```

Use FerricFlow directly from embedded mode:

```elixir
:ok = FerricStore.flow_create("email-1", type: "email", state: "queued", payload: "welcome:user-1")
{:ok, [job]} = FerricStore.flow_claim_due("email", state: "queued", worker: "worker-1", limit: 1)
{:ok, _} = FerricStore.flow_complete(job.id, job.lease_token,
  fencing_token: job.fencing_token,
  result: "sent"
)
```

## Behavior Differences from Native TCP Mode

The embedded API (`FerricStore` module) and the native TCP mode execute the same command handlers, but there are differences to be aware of:

| Aspect | Native TCP mode | Embedded mode |
|--------|---------------|---------------|
| **Return values** | Native typed values (`nil`, binary, integer, list, map) | Elixir-idiomatic types (`{:ok, value}`, `:ok`, etc.) |
| **Blocking commands** | `BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `XREAD BLOCK` supported | Not available -- return immediately |
| **Set algebra** | `SINTER`/`SUNION`/`SDIFF` scan the single shard owning the key | Same behavior -- set members are co-located per key |
| **Transactions** | `MULTI`/`EXEC`/`WATCH` at connection level | `FerricStore.multi/1` with function-based API |
| **ACL** | Per-connection ACL enforcement | No ACL checks |
| **Client state** | Client registry, lanes, sessions, Pub/Sub, blocking waits | Not applicable |

## API Reference

### Strings

```elixir
# SET -- store a value with optional TTL
:ok = FerricStore.set("key", "value")
:ok = FerricStore.set("key", "value", ttl: :timer.hours(1))

# GET -- retrieve a value (nil for missing/expired keys)
{:ok, "value"} = FerricStore.get("key")
{:ok, nil} = FerricStore.get("nonexistent")

# DEL -- delete a key (works for any type: string, hash, list, set, zset)
:ok = FerricStore.del("key")

# EXISTS -- check if a key exists
true = FerricStore.exists("key")
false = FerricStore.exists("nonexistent")

# INCR / DECR -- atomic integer operations
# Key initialized to 0 if missing
{:ok, 1} = FerricStore.incr("counter")
{:ok, 2} = FerricStore.incr("counter")
{:ok, 1} = FerricStore.decr("counter")

# INCRBY / DECRBY -- increment/decrement by N
{:ok, 11} = FerricStore.incr_by("counter", 10)
{:ok, 1} = FerricStore.decr_by("counter", 10)

# INCRBYFLOAT -- atomic float increment
{:ok, "3.14"} = FerricStore.incr_by_float("pi", 3.14)

# MGET / MSET -- multi-key operations
:ok = FerricStore.mset(%{"k1" => "v1", "k2" => "v2", "k3" => "v3"})
{:ok, ["v1", nil, "v3"]} = FerricStore.mget(["k1", "missing", "k3"])

# MSETNX -- set multiple only if NONE exist
{:ok, true} = FerricStore.msetnx(%{"a" => "1", "b" => "2"})
{:ok, false} = FerricStore.msetnx(%{"a" => "1", "c" => "3"})  # "a" exists

# SETNX -- set only if key does not exist
{:ok, true} = FerricStore.setnx("new_key", "value")
{:ok, false} = FerricStore.setnx("new_key", "other")  # already exists

# SETEX / PSETEX -- set with expiry
:ok = FerricStore.setex("key", 60, "value")        # 60 seconds
:ok = FerricStore.psetex("key", 60_000, "value")    # 60,000 milliseconds

# GETSET -- atomically set and return old value
:ok = FerricStore.set("key", "old")
{:ok, "old"} = FerricStore.getset("key", "new")

# GETDEL -- atomically get and delete
{:ok, "new"} = FerricStore.getdel("key")
{:ok, nil} = FerricStore.get("key")  # gone

# GETEX -- get and update TTL
:ok = FerricStore.set("key", "value")
{:ok, "value"} = FerricStore.getex("key", ttl: 60_000)

# APPEND -- append to string value
:ok = FerricStore.set("greeting", "Hello")
{:ok, 11} = FerricStore.append("greeting", " World")

# STRLEN -- byte length of value
{:ok, 11} = FerricStore.strlen("greeting")

# GETRANGE -- substring by byte range
{:ok, "World"} = FerricStore.getrange("greeting", 6, 10)

# SETRANGE -- overwrite at offset
{:ok, 11} = FerricStore.setrange("greeting", 6, "World")

# KEYS -- find keys by glob pattern
{:ok, keys} = FerricStore.keys("user:*")
{:ok, all} = FerricStore.keys()

# DBSIZE / FLUSHDB
{:ok, count} = FerricStore.dbsize()
:ok = FerricStore.flushdb()
```

### Hash

Hash fields are stored as individual compound keys, enabling O(1) per-field access.

```elixir
# HSET -- set one or more fields (pass a map)
:ok = FerricStore.hset("user:42", %{"name" => "alice", "email" => "a@b.com", "age" => "30"})

# HGET -- get a single field
{:ok, "alice"} = FerricStore.hget("user:42", "name")
{:ok, nil} = FerricStore.hget("user:42", "missing")

# HGETALL -- get all fields as a map
{:ok, %{"name" => "alice", "email" => "a@b.com", "age" => "30"}} = FerricStore.hgetall("user:42")

# HDEL -- delete fields
{:ok, 2} = FerricStore.hdel("user:42", ["email", "missing"])

# HEXISTS -- check if field exists
true = FerricStore.hexists("user:42", "name")
false = FerricStore.hexists("user:42", "email")

# HLEN -- field count
{:ok, 2} = FerricStore.hlen("user:42")

# HKEYS / HVALS -- field names / values
{:ok, ["name", "age"]} = FerricStore.hkeys("user:42")
{:ok, ["alice", "30"]} = FerricStore.hvals("user:42")

# HMGET -- multiple fields at once
{:ok, ["alice", nil, "30"]} = FerricStore.hmget("user:42", ["name", "missing", "age"])

# HINCRBY / HINCRBYFLOAT -- atomic field increment
{:ok, 31} = FerricStore.hincrby("user:42", "age", 1)
{:ok, "31.5"} = FerricStore.hincrbyfloat("user:42", "age", 0.5)

# HSETNX -- set field only if it does not exist
{:ok, true} = FerricStore.hsetnx("user:42", "country", "US")
{:ok, false} = FerricStore.hsetnx("user:42", "country", "UK")  # already exists

# HRANDFIELD -- random field
{:ok, field} = FerricStore.hrandfield("user:42")

# HSTRLEN -- byte length of field value
{:ok, 5} = FerricStore.hstrlen("user:42", "name")
```

### Lists

```elixir
# LPUSH / RPUSH -- push elements to head/tail
{:ok, 3} = FerricStore.lpush("queue", ["a", "b", "c"])  # head: c, b, a
{:ok, 4} = FerricStore.rpush("queue", ["d"])              # head: c, b, a, d

# LPOP / RPOP -- pop from head/tail
{:ok, "c"} = FerricStore.lpop("queue")
{:ok, "d"} = FerricStore.rpop("queue")

# LRANGE -- get range of elements (supports negative indices)
{:ok, ["b", "a"]} = FerricStore.lrange("queue", 0, -1)

# LLEN -- list length
{:ok, 2} = FerricStore.llen("queue")

# LINDEX -- get element by index
{:ok, "b"} = FerricStore.lindex("queue", 0)
{:ok, "a"} = FerricStore.lindex("queue", -1)

# LSET -- set element at index
:ok = FerricStore.lset("queue", 0, "X")

# LREM -- remove occurrences of element
# count > 0: from head, count < 0: from tail, count = 0: all
{:ok, 1} = FerricStore.lrem("queue", 0, "X")

# LINSERT -- insert relative to pivot
{:ok, 2} = FerricStore.rpush("list", ["a", "c"])
{:ok, 3} = FerricStore.linsert("list", :before, "c", "b")
# list is now: a, b, c

# LMOVE -- atomically move between lists
{:ok, "a"} = FerricStore.lmove("list", "other", :left, :right)

# LPOS -- find position(s) of element
{:ok, 0} = FerricStore.lpos("list", "b")
{:ok, [0, 1]} = FerricStore.lpos("list", "b", count: 0)  # all positions

# LTRIM -- trim list to range
:ok = FerricStore.rpush("nums", ["1", "2", "3", "4", "5"])
:ok = FerricStore.ltrim("nums", 1, 3)
# nums is now: 2, 3, 4
```

> **Blocking list commands (`BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`) are not
> available in embedded mode.** These commands require a persistent TCP
> connection to block on. In embedded mode, use `lpop`/`rpop` with a polling
> loop or your application's own notification mechanism.

### Sets

```elixir
# SADD -- add members
{:ok, 3} = FerricStore.sadd("tags", ["elixir", "rust", "storage"])

# SMEMBERS -- get all members (unordered)
{:ok, members} = FerricStore.smembers("tags")  # ["elixir", "rust", "storage"] in any order

# SISMEMBER -- check membership
true = FerricStore.sismember("tags", "elixir")
false = FerricStore.sismember("tags", "python")

# SMISMEMBER -- check multiple memberships
{:ok, [1, 0, 1]} = FerricStore.smismember("tags", ["elixir", "python", "rust"])

# SCARD -- set size
{:ok, 3} = FerricStore.scard("tags")

# SREM -- remove members
{:ok, 1} = FerricStore.srem("tags", ["storage"])

# SRANDMEMBER -- random member without removal
{:ok, member} = FerricStore.srandmember("tags")

# SPOP -- random member with removal
{:ok, member} = FerricStore.spop("tags")

# SDIFF / SINTER / SUNION -- set algebra
FerricStore.sadd("set1", ["a", "b", "c"])
FerricStore.sadd("set2", ["b", "c", "d"])
{:ok, diff} = FerricStore.sdiff(["set1", "set2"])    # ["a"]
{:ok, inter} = FerricStore.sinter(["set1", "set2"])   # ["b", "c"]
{:ok, union} = FerricStore.sunion(["set1", "set2"])    # ["a", "b", "c", "d"]
```

### Sorted Sets

```elixir
# ZADD -- add members with scores
{:ok, 3} = FerricStore.zadd("scores", [{100.0, "alice"}, {200.0, "bob"}, {150.0, "carol"}])

# ZRANGE -- range by index
{:ok, ["alice", "carol", "bob"]} = FerricStore.zrange("scores", 0, -1)

# ZRANGE with scores -- returns list of {member, score} tuples
{:ok, [{"alice", 100.0}, {"carol", 150.0}, {"bob", 200.0}]} =
  FerricStore.zrange("scores", 0, -1, withscores: true)

# ZSCORE -- get member's score
{:ok, 100.0} = FerricStore.zscore("scores", "alice")
{:ok, nil} = FerricStore.zscore("scores", "unknown")

# ZCARD -- cardinality
{:ok, 3} = FerricStore.zcard("scores")

# ZRANK / ZREVRANK -- rank (0-based)
{:ok, 0} = FerricStore.zrank("scores", "alice")    # lowest score
{:ok, 2} = FerricStore.zrevrank("scores", "alice")  # highest rank reversed

# ZRANGEBYSCORE -- range by score (supports -inf, +inf, exclusive prefix)
{:ok, ["carol", "bob"]} = FerricStore.zrangebyscore("scores", "150", "+inf")

# ZCOUNT -- count in score range
{:ok, 2} = FerricStore.zcount("scores", "100", "150")

# ZINCRBY -- increment score
{:ok, "115.0"} = FerricStore.zincrby("scores", 15.0, "alice")

# ZREM -- remove members
{:ok, 1} = FerricStore.zrem("scores", ["alice"])

# ZPOPMIN / ZPOPMAX -- pop lowest/highest scored
{:ok, [{"carol", 150.0}]} = FerricStore.zpopmin("scores", 1)
{:ok, [{"bob", 200.0}]} = FerricStore.zpopmax("scores", 1)

# ZMSCORE -- multiple scores at once
{:ok, scores} = FerricStore.zmscore("scores", ["alice", "unknown"])

# ZRANDMEMBER -- random member
{:ok, member} = FerricStore.zrandmember("scores")
```

### TTL / Expiry

```elixir
# Set TTL via SET option
:ok = FerricStore.set("session", "data", ttl: :timer.minutes(30))

# EXPIRE / PEXPIRE -- set TTL on existing key
{:ok, true} = FerricStore.expire("key", :timer.hours(1))
{:ok, true} = FerricStore.pexpire("key", 30_000)

# EXPIREAT / PEXPIREAT -- set absolute expiry
{:ok, true} = FerricStore.expireat("key", 1700000000)
{:ok, true} = FerricStore.pexpireat("key", 1700000000000)

# TTL / PTTL -- remaining time
{:ok, seconds} = FerricStore.ttl("session")     # seconds remaining
{:ok, ms} = FerricStore.pttl("session")          # milliseconds remaining
{:ok, nil} = FerricStore.ttl("no_ttl_key")       # nil = no expiry
# Returns native integer TTL codes over TCP, nil in embedded for missing keys

# EXPIRETIME / PEXPIRETIME -- absolute expiry
{:ok, unix_seconds} = FerricStore.expiretime("key")
{:ok, unix_ms} = FerricStore.pexpiretime("key")

# PERSIST -- remove expiry
{:ok, true} = FerricStore.persist("key")
```

### Streams

```elixir
# XADD -- add entry (auto-generates ID with * by default)
{:ok, id} = FerricStore.xadd("events", ["type", "click", "page", "/home"])
# id => "1679000000000-0"

# XLEN -- stream length
{:ok, 1} = FerricStore.xlen("events")

# XRANGE / XREVRANGE -- range queries
{:ok, entries} = FerricStore.xrange("events", "-", "+")
{:ok, entries} = FerricStore.xrange("events", "-", "+", count: 10)
{:ok, entries} = FerricStore.xrevrange("events", "+", "-")
# Each entry: [id, field1, value1, field2, value2, ...]

# XTRIM -- trim by MAXLEN or MINID
{:ok, trimmed} = FerricStore.xtrim("events", maxlen: 1000)
```

**Note:** `XREAD BLOCK`, `XGROUP`, `XREADGROUP`, and `XACK` are only available in native TCP mode. The embedded API does not support blocking reads or consumer groups.

### Bitmap

```elixir
# SETBIT / GETBIT -- individual bit operations
{:ok, 0} = FerricStore.setbit("bitmap", 7, 1)   # returns old bit value
{:ok, 1} = FerricStore.getbit("bitmap", 7)

# BITCOUNT -- count set bits
{:ok, count} = FerricStore.bitcount("bitmap")
{:ok, count} = FerricStore.bitcount("bitmap", 0, 0)       # first byte only
{:ok, count} = FerricStore.bitcount("bitmap", 0, 7, :bit)  # first 8 bits

# BITPOS -- find first 0 or 1 bit
{:ok, pos} = FerricStore.bitpos("bitmap", 1)
{:ok, pos} = FerricStore.bitpos("bitmap", 0, 1)  # starting from byte 1

# BITOP -- bitwise operations
{:ok, len} = FerricStore.bitop(:and, "dest", ["bitmap1", "bitmap2"])
{:ok, len} = FerricStore.bitop(:or, "dest", ["bitmap1", "bitmap2"])
{:ok, len} = FerricStore.bitop(:xor, "dest", ["bitmap1", "bitmap2"])
{:ok, len} = FerricStore.bitop(:not, "dest", ["bitmap1"])
```

### HyperLogLog

```elixir
# PFADD -- add elements to HLL sketch
{:ok, true} = FerricStore.pfadd("hll", ["a", "b", "c"])
{:ok, false} = FerricStore.pfadd("hll", ["a"])  # no modification

# PFCOUNT -- estimated cardinality
{:ok, 3} = FerricStore.pfcount(["hll"])

# PFMERGE -- merge multiple HLLs
FerricStore.pfadd("hll2", ["d", "e"])
:ok = FerricStore.pfmerge("merged", ["hll", "hll2"])
{:ok, 5} = FerricStore.pfcount(["merged"])
```

### Bloom Filter

Bloom filters are backed by mmap files. Auto-created with defaults (error_rate=0.01, capacity=100) on first `bf_add`.

```elixir
# BF.RESERVE -- create with specific parameters
:ok = FerricStore.bf_reserve("filter", 0.01, 10_000)

# BF.ADD / BF.MADD -- add elements
{:ok, 1} = FerricStore.bf_add("filter", "hello")
{:ok, results} = FerricStore.bf_madd("filter", ["a", "b", "c"])
# results => [1, 1, 1]  (1 = newly added)

# BF.EXISTS / BF.MEXISTS -- check membership
{:ok, 1} = FerricStore.bf_exists("filter", "hello")    # may exist
{:ok, 0} = FerricStore.bf_exists("filter", "unknown")   # definitely not
{:ok, [1, 0]} = FerricStore.bf_mexists("filter", ["hello", "unknown"])

# BF.CARD -- number of items added
{:ok, 4} = FerricStore.bf_card("filter")

# BF.INFO -- filter metadata
{:ok, info} = FerricStore.bf_info("filter")
# info includes: Capacity, Size, Number of filters, items, error rate, hash functions, bits
```

### Cuckoo Filter

Cuckoo filters support deletion (unlike Bloom). Auto-created with capacity 1024 on first `cf_add`.

```elixir
# CF.RESERVE
:ok = FerricStore.cf_reserve("cuckoo", 10_000)

# CF.ADD / CF.ADDNX
{:ok, 1} = FerricStore.cf_add("cuckoo", "hello")
{:ok, 1} = FerricStore.cf_addnx("cuckoo", "world")   # add only if not present
{:ok, 0} = FerricStore.cf_addnx("cuckoo", "world")   # already present

# CF.EXISTS / CF.MEXISTS
{:ok, 1} = FerricStore.cf_exists("cuckoo", "hello")
{:ok, [1, 0]} = FerricStore.cf_mexists("cuckoo", ["hello", "missing"])

# CF.DEL -- delete one occurrence
{:ok, 1} = FerricStore.cf_del("cuckoo", "hello")

# CF.COUNT -- approximate count of fingerprint occurrences
{:ok, 1} = FerricStore.cf_count("cuckoo", "world")

# CF.INFO
{:ok, info} = FerricStore.cf_info("cuckoo")
```

### Count-Min Sketch

```elixir
# CMS.INITBYDIM -- create by dimensions
:ok = FerricStore.cms_initbydim("sketch", 1000, 5)

# CMS.INITBYPROB -- create by target accuracy
:ok = FerricStore.cms_initbyprob("sketch2", 0.001, 0.01)

# CMS.INCRBY -- increment counts
{:ok, counts} = FerricStore.cms_incrby("sketch", [{"page:/home", 3}, {"page:/about", 1}])
# counts => [3, 1]  (estimated minimum counts)

# CMS.QUERY -- query counts
{:ok, counts} = FerricStore.cms_query("sketch", ["page:/home", "page:/about"])
# counts => [3, 1]

# CMS.INFO
{:ok, info} = FerricStore.cms_info("sketch")
# info => [width, depth, count]
```

### TopK

```elixir
# TOPK.RESERVE -- create tracker
:ok = FerricStore.topk_reserve("top10", 10)
# Optional: FerricStore.topk_reserve("top10", 10)

# TOPK.ADD -- add items (returns evicted items or nil)
{:ok, evicted} = FerricStore.topk_add("top10", ["item1", "item2", "item3"])

# TOPK.QUERY -- check if items are in top-K
{:ok, results} = FerricStore.topk_query("top10", ["item1", "unknown"])
# results => [1, 0]

# TOPK.LIST -- list current top-K
{:ok, items} = FerricStore.topk_list("top10")

# TOPK.INFO
{:ok, info} = FerricStore.topk_info("top10")
```

### TDigest

T-digests provide accurate quantile estimation with bounded memory, especially at the tails (P99, P99.9).

```elixir
# TDIGEST.CREATE
:ok = FerricStore.tdigest_create("latency")
# Optional: FerricStore.tdigest_create("latency", compression: 200)

# TDIGEST.ADD
:ok = FerricStore.tdigest_add("latency", [1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 50.0, 100.0])

# TDIGEST.QUANTILE -- estimate values at quantile positions
{:ok, quantiles} = FerricStore.tdigest_quantile("latency", [0.5, 0.95, 0.99])

# TDIGEST.CDF -- cumulative distribution function
{:ok, cdfs} = FerricStore.tdigest_cdf("latency", [2.5, 50.0])

# TDIGEST.RANK / TDIGEST.REVRANK -- rank estimation
{:ok, ranks} = FerricStore.tdigest_rank("latency", [3.0])
{:ok, revranks} = FerricStore.tdigest_revrank("latency", [3.0])

# TDIGEST.BYRANK / TDIGEST.BYREVRANK -- value at rank
{:ok, values} = FerricStore.tdigest_byrank("latency", [0, 4])
{:ok, values} = FerricStore.tdigest_byrevrank("latency", [0])

# TDIGEST.TRIMMED_MEAN -- mean excluding tails
{:ok, mean} = FerricStore.tdigest_trimmed_mean("latency", 0.1, 0.9)

# TDIGEST.MIN / TDIGEST.MAX
{:ok, min} = FerricStore.tdigest_min("latency")
{:ok, max} = FerricStore.tdigest_max("latency")

# TDIGEST.INFO
{:ok, info} = FerricStore.tdigest_info("latency")

# TDIGEST.RESET -- clear data, preserve compression
:ok = FerricStore.tdigest_reset("latency")
```

### Geo

Geo is implemented on top of Sorted Sets with geohash-encoded scores.

```elixir
# GEOADD -- add geo positions
{:ok, 2} = FerricStore.geoadd("places", [
  {13.361389, 38.115556, "Palermo"},
  {15.087269, 37.502669, "Catania"}
])

# GEODIST -- distance between members
{:ok, dist} = FerricStore.geodist("places", "Palermo", "Catania", "km")

# GEOHASH -- geohash strings
{:ok, hashes} = FerricStore.geohash("places", ["Palermo"])

# GEOPOS -- coordinates
{:ok, positions} = FerricStore.geopos("places", ["Palermo", "Catania"])
# positions => [[13.361389, 38.115556], [15.087269, 37.502669]]
```

### Compare-and-Swap

Atomic compare-and-swap routed directly through `Router.cas/4`.

```elixir
FerricStore.set("version", "1")
{:ok, true} = FerricStore.cas("version", "1", "2")       # swap succeeded
{:ok, false} = FerricStore.cas("version", "1", "3")      # expected "1" but found "2"
{:ok, nil} = FerricStore.cas("nonexistent", "1", "2")    # key does not exist
```

### Distributed Lock

```elixir
# Acquire lock for 5 seconds
:ok = FerricStore.lock("resource:1", "worker_a", 5_000)

# Extend TTL while holding lock
{:ok, 1} = FerricStore.extend("resource:1", "worker_a", 10_000)

# Release lock (must be same owner)
{:ok, 1} = FerricStore.unlock("resource:1", "worker_a")
```

### Rate Limiting

Sliding window rate limiter.

```elixir
# 100 requests per 60-second window
{:ok, [allowed, current_count, remaining, retry_after]} =
  FerricStore.ratelimit_add("api:user42", 60_000, 100)

# allowed: 1 (allowed) or 0 (rejected)
# current_count: current count in window
# remaining: requests left before limit
# retry_after: ms to wait if rejected (0 if allowed)
```

### Cache-Aside with Stampede Protection

`FETCH_OR_COMPUTE` ensures only one caller computes a missing cache value. All other concurrent callers block until the value is available.

```elixir
case FerricStore.fetch_or_compute("expensive:key", ttl: 60_000) do
  {:ok, {:hit, value}} ->
    # Cache hit -- return immediately
    value

  {:ok, {:compute, _hint, token}} ->
    # Cache miss -- this process is the designated computer
    value = expensive_computation()
    FerricStore.fetch_or_compute_result("expensive:key", value,
      token: token,
      ttl: 60_000
    )
    value
end
```

To report a computation error:

```elixir
FerricStore.fetch_or_compute_error("expensive:key", "computation failed", token: token)
```

### Multi/Transaction

```elixir
{:ok, results} = FerricStore.multi(fn tx ->
  tx
  |> FerricStore.Tx.set("k1", "v1")
  |> FerricStore.Tx.set("k2", "v2")
  |> FerricStore.Tx.get("k1")
  |> FerricStore.Tx.incr("counter")
end)
# results => [:ok, :ok, {:ok, "v1"}, {:ok, 1}]
```

### Pipelines

Batch multiple operations for efficiency:

```elixir
{:ok, results} = FerricStore.pipeline(fn pipe ->
  pipe
  |> FerricStore.Pipe.set("key1", "val1")
  |> FerricStore.Pipe.set("key2", "val2")
  |> FerricStore.Pipe.get("key1")
  |> FerricStore.Pipe.incr("counter")
  |> FerricStore.Pipe.hset("user:1", %{"name" => "alice"})
  |> FerricStore.Pipe.zadd("scores", [{100.0, "alice"}])
end)
```

### Key Operations

```elixir
# TYPE -- get key type
{:ok, "string"} = FerricStore.type("key")
{:ok, "hash"} = FerricStore.type("user:42")
{:ok, "none"} = FerricStore.type("missing")

# COPY
{:ok, true} = FerricStore.copy("src", "dst")

# RENAME / RENAMENX
:ok = FerricStore.rename("old", "new")
{:ok, true} = FerricStore.renamenx("old", "new")

# RANDOMKEY
{:ok, "some_key"} = FerricStore.randomkey()
```

### Server

```elixir
{:ok, "PONG"} = FerricStore.ping()
{:ok, "hello"} = FerricStore.echo("hello")
:ok = FerricStore.flushall()
```

### Named Caches

Direct operations to different cache instances:

```elixir
:ok = FerricStore.set("session:abc", data, cache: :sessions)
{:ok, data} = FerricStore.get("session:abc", cache: :sessions)
```

## Performance Characteristics

| Operation | Latency | Notes |
|-----------|---------|-------|
| Hot read (ETS) | ~1-5 us | No GenServer roundtrip, lock-free concurrent reads |
| Cold read (Bitcask) | ~50-200 us | pread via Rust NIF, NVMe |
| Write | ~10-50 us | ETS immediate, Raft group-commit async |
| INCR | ~10-20 us | Read-modify-write in shard GenServer |
| Pipeline (N ops) | ~N * 10 us | Commands execute sequentially |

### Large Value Handling

Values larger than `hot_cache_max_value_size` (default: 64 KB) are stored as `nil` in ETS to avoid binary copy overhead on every read. They are read from Bitcask on access. At startup, FerricStore scans for large values and logs a warning:

```
[warning] Embedded large value check: 3 value(s) exceed threshold;
largest key="big:blob" (2097152 bytes)
```

If you routinely store values larger than 64 KB, consider:
- Raising `hot_cache_max_value_size` (if you have the RAM)
- Chunking values into smaller pieces
- Using standalone native mode with response chunking and connection backpressure

## Testing

With the instance-based architecture, each test gets its own fully isolated FerricStore instance -- separate shards, ETS tables, WARaft segments, and data directory. No shared state, supporting `async: true`.

```elixir
defmodule MyApp.CacheTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, _} = TestCache.start_link(data_dir: make_temp_dir(), shard_count: 2)
    on_exit(fn -> TestCache.stop() end)
    :ok
  end

  test "set and get" do
    TestCache.set("key", "value")
    assert {:ok, "value"} = TestCache.get("key")
  end

  test "isolated from other tests" do
    # Each test has its own instance -- no shared state
    assert {:ok, nil} = TestCache.get("key")
  end
end
```

## Multiple Instances

By default, FerricStore starts a single `:default` instance that the
`FerricStore` module uses. For applications that need isolated cache
domains (separate data directories, memory limits, or eviction policies),
define named instances with `use FerricStore`:

```elixir
defmodule MyApp.Sessions do
  use FerricStore,
    data_dir: "/data/sessions",
    shard_count: 2,
    max_memory_bytes: 512_000_000,
    eviction_policy: :volatile_lfu,
    flow_retention_sweeper: [interval_ms: 60_000, limit: 500]
end

defmodule MyApp.PageCache do
  use FerricStore,
    data_dir: "/data/page_cache",
    shard_count: 4,
    max_memory_bytes: 2_000_000_000,
    eviction_policy: :allkeys_lfu
end
```

### Supervision Tree

Add each instance as a child in your application supervisor. Each instance
starts its own shards, ETS tables, and Raft system independently:

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      MyApp.Sessions,
      MyApp.PageCache
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

### Usage

Each module gets the full FerricStore API. Instances are completely isolated --
keys in one instance are invisible to another:

```elixir
MyApp.Sessions.set("sess:abc", session_data, ttl: :timer.minutes(30))
{:ok, data} = MyApp.Sessions.get("sess:abc")

MyApp.PageCache.set("page:/home", html, ttl: :timer.hours(1))
{:ok, nil} = MyApp.PageCache.get("sess:abc")  # not found here
```

Each named instance starts its own Flow retention sweeper. Configure it with
`:flow_retention_sweeper` when the default interval, batch limit, or bounded
catch-up burst and pause settings are not appropriate. The sweeper enforces
`max_active_ms` and terminal retention only
against that instance's shards.

### When to Use Multiple Instances

- **Separate data directories** -- session data on fast NVMe, page cache on larger disk
- **Different eviction policies** -- `:volatile_lfu` for sessions (TTL-based), `:allkeys_lfu` for a general cache
- **Independent memory budgets** -- prevent one workload from evicting another's hot keys
- **Isolation in tests** -- each test module can use its own instance with `ExUnit.Callbacks.tmp_dir`

A single instance with namespace prefixes (`session:`, `cache:`) is simpler
and sufficient when you don't need separate eviction pools or data directories.

## Integration with Phoenix

A common pattern is to use FerricStore as an application cache in a Phoenix application:

```elixir
# lib/my_app/cache.ex
defmodule MyApp.Cache do
  def get(key) do
    case FerricStore.get(key) do
      {:ok, nil} -> :miss
      {:ok, value} -> {:ok, :erlang.binary_to_term(value)}
    end
  end

  def put(key, value, ttl \\ :timer.hours(1)) do
    FerricStore.set(key, :erlang.term_to_binary(value), ttl: ttl)
  end

  def delete(key) do
    FerricStore.del(key)
  end

  def fetch_or_compute(key, ttl, fun) do
    case FerricStore.fetch_or_compute(key, ttl: ttl) do
      {:ok, {:hit, bin}} -> :erlang.binary_to_term(bin)
      {:ok, {:compute, _, token}} ->
        value = fun.()
        bin = :erlang.term_to_binary(value)
        FerricStore.fetch_or_compute_result(key, bin, token: token, ttl: ttl)
        value
    end
  end
end
```

## Health Checks

Even in embedded mode, you can check FerricStore's health programmatically:

```elixir
Ferricstore.Health.ready?()
# => true

Ferricstore.Health.check()
# => %{status: :ok, shard_count: 4, shards: [...], uptime_seconds: 120}
```
