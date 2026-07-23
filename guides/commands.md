# Commands Reference

This is the command reference for the native TCP server, embedded API, and FerricFlow.

Start here if you need command syntax, logical return values, embedded API equivalents, or native protocol mapping notes. For a first walkthrough, use [Getting Started](getting-started.md). For the KV store model and storage path, use [Key-Value Store](kv-store.md).

Native TCP clients normally use dedicated opcodes with typed map payloads. For
commands outside the compact opcode set, native clients use `COMMAND_EXEC` with
`{"command": "...", "args": [...]}`. The command examples below show the
logical command name and arguments, not a text wire protocol.

FerricFlow commands use the `FLOW.*` prefix and model durable workflow state: create, claim due work, transition, retry, complete, fail, cancel, signal, value refs, and fanout.

## Command Surface Summary

### Implemented Command Families

FerricStore implements these command names and argument shapes across native TCP
mode and the embedded API:

GET, SET (EX/PX/EXAT/PXAT/NX/XX/GET/KEEPTTL), DEL, EXISTS, MGET, MSET, MSETNX, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT,
APPEND, STRLEN, GETSET, GETDEL, GETEX, SETNX, SETEX, PSETEX, GETRANGE, SETRANGE,
HSET, HGET, HDEL, HMGET, HGETALL, HEXISTS, HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT,
HSETNX, HSTRLEN, HRANDFIELD, HSCAN, HEXPIRE, HTTL, HPERSIST, HPEXPIRE, HPTTL, HEXPIRETIME, HGETDEL, HGETEX, HSETEX,
LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET, LREM, LTRIM, LPOS, LINSERT, LMOVE, RPOPLPUSH, LPUSHX, RPUSHX,
SADD, SREM, SMEMBERS, SISMEMBER, SMISMEMBER, SCARD, SRANDMEMBER, SPOP, SDIFF, SINTER, SUNION,
SDIFFSTORE, SINTERSTORE, SUNIONSTORE, SINTERCARD, SMOVE, SSCAN,
ZADD (NX/XX/GT/LT/CH), ZSCORE, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE, ZCARD, ZREM, ZINCRBY,
ZCOUNT, ZPOPMIN, ZPOPMAX, ZRANDMEMBER, ZMSCORE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZSCAN,
XADD, XLEN, XRANGE, XREVRANGE, XREAD (including BLOCK), XTRIM, XDEL, XINFO STREAM,
XGROUP CREATE, XREADGROUP, XACK,
EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, EXPIRETIME, PEXPIRETIME,
SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP,
PFADD, PFCOUNT, PFMERGE,
GEOADD, GEOPOS, GEODIST, GEOHASH, GEOSEARCH, GEOSEARCHSTORE,
PING, ECHO, DBSIZE, KEYS, FLUSHDB, FLUSHALL, INFO, TYPE, UNLINK, RENAME, RENAMENX, COPY, RANDOMKEY,
OBJECT HELP/REFCOUNT, SCAN, CONFIG GET/SET/RESETSTAT/REWRITE,
SLOWLOG GET/LEN/RESET, COMMAND/COMMAND COUNT/COMMAND LIST/COMMAND INFO/COMMAND DOCS/COMMAND GETKEYS,
MULTI, EXEC, DISCARD, WATCH, UNWATCH, SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH,
CLIENT ID/SETNAME/GETNAME/INFO/LIST/TRACKING/CACHING/TRACKINGINFO/GETREDIR, HELLO, AUTH, QUIT, RESET

### FerricStore-Native Flow Commands

Flow commands are FerricStore-native workflow commands, not FerricStore
data-structure commands. They are exposed through native TCP mode and the embedded API:

`FLOW.CREATE`, `FLOW.CREATE_MANY`, `FLOW.VALUE.PUT`, `FLOW.SIGNAL`,
`FLOW.SPAWN_CHILDREN`, `FLOW.GET`, `FLOW.CLAIM_DUE`, `FLOW.RECLAIM`,
`FLOW.EXTEND_LEASE`, `FLOW.COMPLETE`, `FLOW.COMPLETE_MANY`, `FLOW.RETRY`,
`FLOW.RETRY_MANY`, `FLOW.FAIL`, `FLOW.FAIL_MANY`, `FLOW.CANCEL`,
`FLOW.CANCEL_MANY`, `FLOW.TRANSITION`, `FLOW.TRANSITION_MANY`,
`FLOW.REWIND`, `FLOW.STATS`, `FLOW.INFO`, `FLOW.HISTORY`,
`FLOW.POLICY.SET`, `FLOW.POLICY.GET`, `FLOW.ATTRIBUTES`,
`FLOW.ATTRIBUTE_VALUES`, `FLOW.QUERY`, and `FLOW.RETENTION_CLEANUP`.

Flow attributes are small indexed metadata fields for query and dashboard
filters. They are separate from payload and named value refs:

```text
FLOW.CREATE order-1 TYPE order STATE queued ATTRIBUTE tenant acme ATTRIBUTE region us
FLOW.TRANSITION order-1 queued charged LEASE_TOKEN <token> FENCING 1 ATTRIBUTE_MERGE phase charge
FLOW.STATS order STATE queued ATTRIBUTE tenant acme
FLOW.QUERY FQL1 "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state AND attribute.tenant = @tenant ORDER BY updated_at_ms ASC LIMIT 100 RETURN RECORDS" partition tenant-a type order state queued tenant acme
```

Use `FLOW.QUERY` as the versioned read/query envelope. The OSS default provider
supports authoritative point/history reads and every bounded shape advertised
by the capability manifest:

```text
FLOW.QUERY FQL1 "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'order-1' RETURN RECORD"
FLOW.QUERY FQL1 "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD" partition tenant-a flow_id order-1
FLOW.QUERY FQL1 "EXPLAIN FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD" partition tenant-a flow_id order-1
FLOW.QUERY FQL1 "FROM runs WHERE partition_key = @partition AND parent_flow_id = @parent ORDER BY updated_at_ms DESC LIMIT 50 RETURN RECORDS" partition tenant-a parent checkout-root
```

Collection reads require one `partition_key` equality. A point read that omits
it addresses only the run ID's deterministic auto-partition; explicitly
partitioned runs require the predicate. The OSS default includes bounded
composite collections, exact `RETURN COUNT`, cursors, statistics, index status,
and full explain analysis. Enterprise uses the same provider and adds metadata
scope and governance integration without changing the query contracts.

```text
FLOW.QUERY FQL1 "FROM runs WHERE partition_key = @partition AND state = 'failed' ORDER BY updated_at_ms DESC LIMIT 50 RETURN RECORDS" partition tenant-a
FLOW.QUERY FQL1 "FROM runs WHERE partition_key = @partition AND type = 'order' RETURN COUNT" partition tenant-a
```

All editions reject unadvertised shapes and predicate or ordering combinations
that have no bounded plan. See [Flow Query Architecture](../docs/flow-query.md)
for the exact capability contract and mandatory bounds.

The returned record is a structural query projection including attributes and
state metadata. It excludes payload/result/error refs, named values, child
bookkeeping, and worker lease/fencing credentials.

Bound the total non-terminal lifetime of a Flow with a type policy or a
per-create override:

```text
FLOW.POLICY.SET order MAX_ACTIVE_MS 300000
FLOW.CREATE order-1 TYPE order STATE queued MAX_ACTIVE_MS 60000
FLOW.CREATE order-no-deadline TYPE order STATE queued MAX_ACTIVE_MS INFINITY
```

The limit is measured from Flow creation, is type-level rather than state-level,
and includes queued, scheduled, running, and retry time. An overdue Flow fails
with history reason `max_active_ms`.

Configure execution ordering independently for each state in the type policy:

```text
FLOW.POLICY.SET order STATE queued MODE FIFO STATE review MODE PARALLEL
```

States are parallel unless configured otherwise. FIFO is a property of the
type/state/partition lane, so it cannot be overridden by an individual create,
transition, or claim command. FIFO states require a partition key and do not
support priority ordering.

Policy updates patch the existing snapshot by default, including nested state
settings. Use `REPLACE TRUE` for an intentional full replacement. Both
`FLOW.POLICY.SET` and `FLOW.POLICY.GET` return the replicated monotonic
`generation`. A caller that read generation 7 can require compare-and-swap:

```text
FLOW.POLICY.SET order EXPECTED_GENERATION 7 STATE queued MODE FIFO
```

A mismatched generation returns `ERR stale flow policy generation` without
changing the policy. Enabling FIFO on a populated state uses the existing
durable state-entry order. Already-issued parallel leases are not revoked; new
claims remain blocked behind the earliest active lane entry while those leases
drain.

Use attributes for values you want to filter or count by, such as tenant,
region, campaign, device group, or model. Use value refs for large bytes or
state-specific data. Attribute query projection is asynchronous, so use
`CONSISTENT_PROJECTION true` when an admin/debug read must wait for projection
catch-up.

State metadata is per logical Flow state. It is retained with the record/history
for that state and does not overwrite metadata stored for earlier states:

```text
FLOW.POLICY.SET order INDEXED_STATE_META version
FLOW.CREATE order-1 TYPE order STATE accept STATE_META version 1
FLOW.COMPLETE order-1 <lease-token> FENCING <fencing-token> STATE_META version 3
FLOW.QUERY FQL1 "FROM runs WHERE partition_key = @partition AND type = @type AND state_meta.accept.version = @version ORDER BY updated_at_ms ASC LIMIT 100 RETURN RECORDS" partition tenant-a type order version 1
```

Only one state metadata key can be indexed per Flow type. Non-indexed state
metadata is still returned by `FLOW.GET`, but broad `state_meta` search requires
the type policy to index the searched key. Changing the indexed state metadata
key backfills existing Flow records of that type and removes stale query rows
for the previous key.

Production semantics, retry policy, history caps, LMDB cold projection, and
operator metrics are documented in `docs/flow-production-readiness.md` and
`docs/flow-retry-policy.md`. The Elixir workflow SDK for the embedded API is
documented in `guides/flow-elixir-sdk.md`.

### Command-Specific Differences

| Command | Difference |
|---------|-----------|
| `ZRANGE` | Unified `BYSCORE`/`BYLEX`/`REV`/`LIMIT` syntax is not yet supported -- use `ZRANGEBYSCORE`/`ZREVRANGEBYSCORE` instead |
| `SCAN` | Cursor is key-based (alphabetic position), not an opaque integer |
| `HSCAN`/`SSCAN`/`ZSCAN` | Cursor is an integer offset into the scanned list |
| `FLUSHDB`/`FLUSHALL` | `ASYNC`/`SYNC` accepted but both execute synchronously; true async reclaim happens during Bitcask merge |
| `UNLINK` | Semantically identical to `DEL` -- async reclaim is deferred to Bitcask merge |
| `OBJECT ENCODING` | Returns type-specific logical encodings (`"embstr"`, `"raw"`, `"hashtable"`, `"quicklist"`, `"skiplist"`, `"stream"`) rather than exposing internal storage layouts |
| `OBJECT FREQ` | Returns the LFU counter from keydir |
| `OBJECT IDLETIME` | Returns idle seconds derived from LFU last-decrement-time |
| `SELECT` | Returns error -- FerricStore is single-database |
| `INFO` | Returns FerricStore-specific sections (`raft`, `bitcask`, `ferricstore`, `keydir_analysis`, `namespace_config`) |
| `WAIT` | Always returns `0` immediately (no replica acknowledgement) |
| `BLPOP`/`BRPOP`/`BLMOVE`/`BLMPOP` | Supported in TCP mode only, not in embedded mode |
| `XREAD BLOCK` | Supported in TCP mode via stream waiters; not available in embedded mode |

### FerricStore-Only Commands

These are FerricStore-native commands:

`CAS`, `LOCK`, `UNLOCK`, `EXTEND`, `RATELIMIT.ADD`, `FETCH_OR_COMPUTE`,
`FETCH_OR_COMPUTE_RESULT`, `FETCH_OR_COMPUTE_ERROR`, `FERRICSTORE.CONFIG`,
`FERRICSTORE.METRICS`, `FERRICSTORE.HOTNESS`, `FERRICSTORE.KEY_INFO`, `FERRICSTORE.DOCTOR`,
`CLUSTER.HEALTH`, `CLUSTER.STATS`, `CLUSTER.KEYSLOT`, `CLUSTER.SLOTS`

### Command Names Not Yet Supported

`EVAL`, `EVALSHA`, `EVALSHA_RO`, `EVAL_RO` (Lua scripting),
`LMPOP`, `ZMPOP`, `BZMPOP` (multi-key pop),
`ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE` (sorted set store operations),
`ZRANGESTORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZLEXCOUNT`,
`SORT`, `SORT_RO`,
`OBJECT` extended subcommands (`OBJECT PERSIST`, `OBJECT COPY`),
`CLUSTER` (full cluster command family),
`DUMP`, `RESTORE`, `MIGRATE`, `MOVE`,
`CLIENT KILL`, `CLIENT NO-EVICT`, `CLIENT PAUSE`, `CLIENT UNPAUSE`,
`DEBUG` (most subcommands)

---

## String Commands

String commands operate on simple key-value pairs. Values are stored as raw byte strings in Bitcask. All writes go through Raft group-commit.

### GET

Retrieves the value of a key. Returns a `WRONGTYPE` error if the key holds a non-string data structure (hash, list, set, zset). FerricStore detects data structure types by peeking at ETF header bytes without deserializing the entire value.

| | |
|---|---|
| **Protocol command** | `GET key` |
| **Embedded API** | `FerricStore.get(key)` |
| **Return** | Bulk string, or `_` (null) if key does not exist |
| **Elixir return** | `{:ok, binary()}` or `{:ok, nil}` |
| **Status** | Supported |

### SET

Sets a string value with optional expiry and conditional flags.

| | |
|---|---|
| **Protocol command** | `SET key value [EX seconds \| PX milliseconds \| EXAT unix-sec \| PXAT unix-ms] [NX\|XX] [GET] [KEEPTTL]` |
| **Embedded API** | `FerricStore.set(key, value, ttl: ms)` |
| **Return** | `+OK` on success, `_` (null) when NX/XX condition fails. With `GET`: returns old value or null. |
| **Elixir return** | `:ok` on success, `{:ok, nil}` when condition fails |

**Options:**
- `EX seconds` -- set expiry in seconds (must be > 0)
- `PX milliseconds` -- set expiry in milliseconds (must be > 0)
- `EXAT unix-sec` -- set absolute expiry as Unix timestamp in seconds
- `PXAT unix-ms` -- set absolute expiry as Unix timestamp in milliseconds
- `NX` -- only set if key does not exist
- `XX` -- only set if key already exists
- `GET` -- return the old value stored at key (or null if key didn't exist)
- `KEEPTTL` -- retain the existing TTL on the key (cannot combine with EX/PX/EXAT/PXAT)

**Status:** Supported -- all SET options supported.

**FerricStore behavior:** Expiry is stored as an absolute HLC timestamp (`expire_at_ms`). Writes go through Raft group-commit -- the ETS keydir is updated immediately (sub-microsecond read visibility) while Bitcask persistence is batched.

### DEL

Deletes one or more keys. Handles both plain string keys and compound data structure keys (hash, list, set, zset) by cleaning up all sub-keys and type metadata.

| | |
|---|---|
| **Protocol command** | `DEL key [key ...]` |
| **Embedded API** | `FerricStore.del(key)` |
| **Return** | Integer -- number of keys deleted |
| **Elixir return** | `:ok` |
| **Status** | Supported |

### EXISTS

Returns the count of keys that exist. Checks both plain keys and compound data structure type metadata.

| | |
|---|---|
| **Protocol command** | `EXISTS key [key ...]` |
| **Embedded API** | `FerricStore.exists(key)` |
| **Return** | Integer -- count of existing keys (a key is counted once for each time it appears in the argument list) |
| **Elixir return** | `true` or `false` (single key) |
| **Status** | Supported |

### MGET

Returns values for multiple keys. Returns `nil` for keys that do not exist.

| | |
|---|---|
| **Protocol command** | `MGET key [key ...]` |
| **Embedded API** | `FerricStore.mget(keys)` |
| **Return** | Array of bulk strings / nulls |
| **Elixir return** | `{:ok, [binary() \| nil]}` |
| **Status** | Supported |

### MSET

Sets multiple key-value pairs atomically. Never fails (always overwrites).

| | |
|---|---|
| **Protocol command** | `MSET key value [key value ...]` |
| **Embedded API** | `FerricStore.mset(map)` |
| **Return** | `+OK` |
| **Elixir return** | `:ok` |
| **Status** | Supported |

**Validation:** Rejects empty keys and keys larger than 65,535 bytes.

**Atomicity:** Every key must hash to the same slot. A cross-slot request returns
`CROSSSLOT` before any mutation; an accepted request is applied as one replicated
state-machine command.

### MSETNX

Sets multiple keys only if NONE of the keys exist. Returns 0 if any key already exists (none are set).

| | |
|---|---|
| **Protocol command** | `MSETNX key value [key value ...]` |
| **Embedded API** | `FerricStore.msetnx(map)` |
| **Return** | Integer -- `1` (all set) or `0` (none set) |
| **Elixir return** | `{:ok, true}` or `{:ok, false}` |
| **Status** | Supported |

**Atomicity:** Every key must hash to the same slot. A cross-slot request returns
`CROSSSLOT` before checking or writing any key. The existence check and all
writes run in one replicated state-machine command.

### INCR / DECR / INCRBY / DECRBY

Atomically increment or decrement integer values. If the key does not exist, it is initialized to `0` before the operation.

| | |
|---|---|
| **Protocol command** | `INCR key`, `DECR key`, `INCRBY key increment`, `DECRBY key decrement` |
| **Embedded API** | `FerricStore.incr(key)`, `FerricStore.decr(key)`, `FerricStore.incr_by(key, n)`, `FerricStore.decr_by(key, n)` |
| **Return** | Integer -- the new value |
| **Elixir return** | `{:ok, integer()}` |
| **Status** | Supported |

**Error:** Returns `ERR value is not an integer or out of range` if the value is not a valid integer.

### INCRBYFLOAT

Atomically increment a value by a floating point amount. If the key does not exist, it is initialized to `0.0`. Rejects `inf` and `NaN`.

| | |
|---|---|
| **Protocol command** | `INCRBYFLOAT key increment` |
| **Embedded API** | `FerricStore.incr_by_float(key, delta)` |
| **Return** | Bulk string -- the new value as a string |
| **Elixir return** | `{:ok, binary()}` |
| **Status** | Supported |

### APPEND

Appends a value to an existing string. If the key does not exist, it is created with the given value.

| | |
|---|---|
| **Protocol command** | `APPEND key value` |
| **Embedded API** | `FerricStore.append(key, value)` |
| **Return** | Integer -- the new length in bytes |
| **Elixir return** | `{:ok, integer()}` |
| **Status** | Supported |

### STRLEN

Returns the byte length of the string stored at key. Returns `0` if the key does not exist.

| | |
|---|---|
| **Protocol command** | `STRLEN key` |
| **Embedded API** | `FerricStore.strlen(key)` |
| **Return** | Integer |
| **Elixir return** | `{:ok, integer()}` |
| **Status** | Supported |

### GETSET

Atomically sets a key and returns the old value. Prefer `SET ... GET` for new clients, but `GETSET` is still supported.

| | |
|---|---|
| **Protocol command** | `GETSET key value` |
| **Embedded API** | `FerricStore.getset(key, value)` |
| **Return** | Bulk string (old value) or null |
| **Elixir return** | `{:ok, binary() \| nil}` |
| **Status** | Supported |

### GETDEL

Atomically gets and deletes a key.

| | |
|---|---|
| **Protocol command** | `GETDEL key` |
| **Embedded API** | `FerricStore.getdel(key)` |
| **Return** | Bulk string or null |
| **Elixir return** | `{:ok, binary() \| nil}` |
| **Status** | Supported |

### GETEX

Gets a key and optionally updates its TTL.

| | |
|---|---|
| **Protocol command** | `GETEX key [EX seconds \| PX ms \| EXAT ts \| PXAT ms_ts \| PERSIST]` |
| **Embedded API** | `FerricStore.getex(key, ttl: ms)` |
| **Return** | Bulk string or null |
| **Elixir return** | `{:ok, binary() \| nil}` |
| **Status** | Supported -- all five TTL options supported |

### SETNX

Sets a key only if it does not already exist.

| | |
|---|---|
| **Protocol command** | `SETNX key value` |
| **Embedded API** | `FerricStore.setnx(key, value)` |
| **Return** | Integer -- `1` (set) or `0` (not set) |
| **Elixir return** | `{:ok, true}` or `{:ok, false}` |
| **Status** | Supported |

### SETEX / PSETEX

Sets a key with an expiry.

| | |
|---|---|
| **Protocol command** | `SETEX key seconds value`, `PSETEX key milliseconds value` |
| **Embedded API** | `FerricStore.setex(key, seconds, value)`, `FerricStore.psetex(key, ms, value)` |
| **Return** | `+OK` |
| **Elixir return** | `:ok` |
| **Status** | Supported. TTL must be > 0. |

### GETRANGE

Returns a substring of the string value by byte range. Supports negative indices (from end).

| | |
|---|---|
| **Protocol command** | `GETRANGE key start end` |
| **Embedded API** | `FerricStore.getrange(key, start, stop)` |
| **Return** | Bulk string (empty if key missing or range invalid) |
| **Elixir return** | `{:ok, binary()}` |
| **Status** | Supported |

### SETRANGE

Overwrites part of a string starting at the given byte offset. If the key does not exist, creates a zero-padded string.

| | |
|---|---|
| **Protocol command** | `SETRANGE key offset value` |
| **Embedded API** | `FerricStore.setrange(key, offset, value)` |
| **Return** | Integer -- the new string length |
| **Elixir return** | `{:ok, integer()}` |
| **Status** | Supported |

---

## Hash Commands

Each hash field is stored as an individual compound key in the shared shard Bitcask: `H:user_key\0field_name -> value`. This allows individual field access without reading the entire hash. Type metadata is maintained by `TypeRegistry` -- using a hash command on a key that holds a different type returns `WRONGTYPE`.

### HSET

Sets one or more field-value pairs. Returns the number of NEW fields added (not updated).

| | |
|---|---|
| **Protocol command** | `HSET key field value [field value ...]` |
| **Embedded API** | `FerricStore.hset(key, map)` |
| **Return** | Integer -- count of new fields added |
| **Elixir return** | `:ok` |
| **Status** | Supported |

### HGET

Returns the value of a single field.

| | |
|---|---|
| **Protocol command** | `HGET key field` |
| **Embedded API** | `FerricStore.hget(key, field)` |
| **Return** | Bulk string or null |
| **Elixir return** | `{:ok, binary() \| nil}` |
| **Status** | Supported |

### HDEL

Deletes one or more fields. Cleans up type metadata if the hash becomes empty.

| | |
|---|---|
| **Protocol command** | `HDEL key field [field ...]` |
| **Embedded API** | `FerricStore.hdel(key, fields)` |
| **Return** | Integer -- count of fields deleted |
| **Elixir return** | `{:ok, integer()}` |
| **Status** | Supported |

### HMGET

Returns values for multiple fields. Missing fields return null.

| | |
|---|---|
| **Protocol command** | `HMGET key field [field ...]` |
| **Embedded API** | `FerricStore.hmget(key, fields)` |
| **Return** | Array of bulk strings / nulls |
| **Elixir return** | `{:ok, [binary() \| nil]}` |
| **Status** | Supported |

### HGETALL

Returns all fields and values as a flat list: `[field1, value1, field2, value2, ...]`.

| | |
|---|---|
| **Protocol command** | `HGETALL key` |
| **Embedded API** | `FerricStore.hgetall(key)` |
| **Return** | Array (flat interleaved) or Map in native TCP mode |
| **Elixir return** | `{:ok, map()}` |
| **Status** | Supported |

### HEXISTS / HLEN / HKEYS / HVALS

| Command | Syntax | Return |
|---------|--------|--------|
| `HEXISTS` | `HEXISTS key field` | `1` if exists, `0` if not |
| `HLEN` | `HLEN key` | Integer -- field count |
| `HKEYS` | `HKEYS key` | Array of field names |
| `HVALS` | `HVALS key` | Array of values |

All return empty results (0, []) for non-existent keys. native TCP mode.

### HINCRBY / HINCRBYFLOAT

Atomically increment hash field values. If the field does not exist, it is initialized to `0`.

| | |
|---|---|
| **Protocol command** | `HINCRBY key field increment`, `HINCRBYFLOAT key field increment` |
| **Embedded API** | `FerricStore.hincrby(key, field, n)`, `FerricStore.hincrbyfloat(key, field, delta)` |
| **Return** | Integer (HINCRBY) or bulk string (HINCRBYFLOAT) |
| **Status** | Supported |

### HSETNX

Sets a field only if it does not exist.

| | |
|---|---|
| **Protocol command** | `HSETNX key field value` |
| **Return** | `1` (set) or `0` (not set) |
| **Status** | Supported |

### HSTRLEN

Returns the string length of a hash field value. Returns `0` for missing fields.

| | |
|---|---|
| **Protocol command** | `HSTRLEN key field` |
| **Return** | Integer |
| **Status** | Supported |

### HRANDFIELD

Returns random field(s). Negative count allows duplicates.

| | |
|---|---|
| **Protocol command** | `HRANDFIELD key [count [WITHVALUES]]` |
| **Return** | Bulk string (single), array (multiple) |
| **Status** | Supported. Negative count behavior allows repeated fields. |

### HSCAN

Cursor-based iteration over hash fields with optional pattern matching.

| | |
|---|---|
| **Protocol command** | `HSCAN key cursor [MATCH pattern] [COUNT count]` |
| **Return** | `[next_cursor, [field, value, ...]]` |
| **Status** | Cursor is an integer offset into the scanned field list. Default COUNT is 10. |

### Hash Field TTL

FerricStore supports per-field expiry on hash fields:

| Command | Syntax | Return |
|---------|--------|--------|
| `HEXPIRE` | `HEXPIRE key seconds FIELDS count field [field ...]` | List of `1` (set) / `-2` (field missing) |
| `HTTL` | `HTTL key FIELDS count field [field ...]` | List of TTL seconds / `-1` (no expiry) / `-2` (missing) |
| `HPERSIST` | `HPERSIST key FIELDS count field [field ...]` | List of `1` (removed) / `-1` (no expiry) / `-2` (missing) |
| `HPEXPIRE` | `HPEXPIRE key ms FIELDS count field [field ...]` | Same as HEXPIRE but milliseconds |
| `HPTTL` | `HPTTL key FIELDS count field [field ...]` | Same as HTTL but milliseconds |
| `HEXPIRETIME` | `HEXPIRETIME key FIELDS count field [field ...]` | Absolute Unix timestamp (seconds) |
| `HGETDEL` | `HGETDEL key FIELDS count field [field ...]` | List of values (nil for missing) |
| `HGETEX` | `HGETEX key [EX sec\|PX ms\|EXAT ts\|PXAT ms\|PERSIST] FIELDS count field [...]` | List of values |
| `HSETEX` | `HSETEX key seconds field value [field value ...]` | Count of new fields |

**Status:** Supported for native TCP and embedded command handlers.

---

## List Commands

Lists are stored via `ListOps` using compound keys. Each element is individually addressable. Push operations notify any blocking waiters (`BLPOP`/`BRPOP`).

### LPUSH / RPUSH

Push one or more elements to the head or tail. Returns the new list length.

| | |
|---|---|
| **Protocol command** | `LPUSH key element [element ...]`, `RPUSH key element [element ...]` |
| **Embedded API** | `FerricStore.lpush(key, elements)`, `FerricStore.rpush(key, elements)` |
| **Return** | Integer -- new length |
| **Elixir return** | `{:ok, integer()}` |
| **Status** | Supported |

### LPOP / RPOP

Pop one or more elements from head or tail.

| | |
|---|---|
| **Protocol command** | `LPOP key [count]`, `RPOP key [count]` |
| **Embedded API** | `FerricStore.lpop(key)`, `FerricStore.rpop(key)` |
| **Return** | Bulk string (single pop), Array (counted pop), null (empty/missing) |
| **Elixir return** | `{:ok, binary() \| nil}` |
| **Status** | Supported. Count=0 returns empty list if key exists, nil if not. |

### LRANGE

Returns elements in the specified range. Supports negative indices.

| | |
|---|---|
| **Protocol command** | `LRANGE key start stop` |
| **Embedded API** | `FerricStore.lrange(key, start, stop)` |
| **Return** | Array of bulk strings |
| **Elixir return** | `{:ok, [binary()]}` |
| **Status** | Supported |

### LLEN / LINDEX / LSET / LREM / LTRIM / LPOS / LINSERT

| Command | Syntax | Return | Notes |
|---------|--------|--------|-------|
| `LLEN` | `LLEN key` | Integer | native TCP mode |
| `LINDEX` | `LINDEX key index` | Bulk string / null | Supports negative indices |
| `LSET` | `LSET key index element` | `+OK` or error | native TCP mode |
| `LREM` | `LREM key count element` | Integer (removed count) | count>0: head-to-tail, count<0: tail-to-head, count=0: all |
| `LTRIM` | `LTRIM key start stop` | `+OK` | native TCP mode |
| `LPOS` | `LPOS key element [RANK r] [COUNT c] [MAXLEN m]` | Integer / Array / null | RANK 0 is invalid |
| `LINSERT` | `LINSERT key BEFORE\|AFTER pivot element` | Integer (new length) / `-1` (pivot not found) | native TCP mode |

### LMOVE / RPOPLPUSH

Atomically pops from one list and pushes to another.

| | |
|---|---|
| **Protocol command** | `LMOVE source destination LEFT\|RIGHT LEFT\|RIGHT` |
| **Embedded API** | `FerricStore.lmove(src, dst, from, to)` |
| **Status** | Supported. `RPOPLPUSH` is an alias for `LMOVE source dest RIGHT LEFT`. |

### LPUSHX / RPUSHX

Push only if the list already exists. Returns 0 if the key does not exist.

| | |
|---|---|
| **Protocol command** | `LPUSHX key element [element ...]`, `RPUSHX key element [element ...]` |
| **Status** | Supported |

### BLPOP / BRPOP / BLMOVE / BLMPOP

Blocking variants of pop/move. These are only available in native TCP mode -- not in embedded mode. When the list is empty, the connection blocks until an element is pushed or the timeout expires.

---

## Set Commands

Each set member is stored as a compound key `S:user_key\0member -> "1"`. This allows O(1) membership testing.

### SADD / SREM

| | |
|---|---|
| **Protocol command** | `SADD key member [member ...]`, `SREM key member [member ...]` |
| **Embedded API** | `FerricStore.sadd(key, members)`, `FerricStore.srem(key, members)` |
| **Return** | Integer -- count of members added/removed |
| **Elixir return** | `{:ok, integer()}` |
| **Status** | Supported. Type metadata cleaned up when set becomes empty. |

### SMEMBERS / SISMEMBER / SCARD

| Command | Syntax | Return |
|---------|--------|--------|
| `SMEMBERS` | `SMEMBERS key` | Array of members |
| `SISMEMBER` | `SISMEMBER key member` | `1` or `0` |
| `SCARD` | `SCARD key` | Integer -- set size |

All native TCP mode. Non-existent keys return empty/0.

### SRANDMEMBER / SPOP

| | |
|---|---|
| **Protocol command** | `SRANDMEMBER key [count]`, `SPOP key [count]` |
| **Status** | Supported. Negative count for `SRANDMEMBER` allows duplicates. `SPOP` removes the selected members. |

### SDIFF / SINTER / SUNION

Set algebra operations across multiple keys.

| | |
|---|---|
| **Protocol command** | `SDIFF key [key ...]`, `SINTER key [key ...]`, `SUNION key [key ...]` |
| **Embedded API** | `FerricStore.sdiff(keys)`, `FerricStore.sinter(keys)`, `FerricStore.sunion(keys)` |
| **Return** | Array of members |
| **Status** | Supported. All keys are loaded into `MapSet` for computation. |

### SDIFFSTORE / SINTERSTORE / SUNIONSTORE

Store operations that compute set algebra and write the result to a destination key.

| | |
|---|---|
| **Protocol command** | `SDIFFSTORE dest key [key ...]`, `SINTERSTORE dest key [key ...]`, `SUNIONSTORE dest key [key ...]` |
| **Return** | Integer -- cardinality of the resulting set |
| **Status** | Supported. Destination is cleared and re-created. |

### SINTERCARD

Returns the cardinality of the intersection without creating a new set.

| | |
|---|---|
| **Protocol command** | `SINTERCARD numkeys key [key ...] [LIMIT limit]` |
| **Return** | Integer -- intersection cardinality (capped by LIMIT if provided) |
| **Status** | Supported |

### SMISMEMBER

Returns whether each member is a member of the set.

| | |
|---|---|
| **Protocol command** | `SMISMEMBER key member [member ...]` |
| **Return** | Array of `1` / `0` |
| **Status** | Supported |

### SMOVE

Atomically moves a member from source to destination set.

| | |
|---|---|
| **Protocol command** | `SMOVE source destination member` |
| **Return** | `1` (moved) or `0` (member not in source) |
| **Status** | Supported |

### SSCAN

Cursor-based iteration with optional MATCH and COUNT.

| | |
|---|---|
| **Protocol command** | `SSCAN key cursor [MATCH pattern] [COUNT count]` |
| **Status** | Cursor is offset-based. Default COUNT is 10. |

---

## Sorted Set Commands

Each sorted set member is stored as `Z:user_key\0member -> score_string`. Scores are float64 strings. For range queries, all members are loaded and sorted in memory -- adequate for typical cache workloads.

### ZADD

Adds members with scores. Supports all FerricStore modifier flags.

| | |
|---|---|
| **Protocol command** | `ZADD key [NX\|XX] [GT\|LT] [CH] score member [score member ...]` |
| **Embedded API** | `FerricStore.zadd(key, [{score, member}, ...])` |
| **Return** | Integer -- count of elements added (or added+changed with CH) |
| **Elixir return** | `{:ok, integer()}` |

**Options:**
- `NX` -- only add new elements, don't update existing
- `XX` -- only update existing elements, don't add new
- `GT` -- only update when new score > current score
- `LT` -- only update when new score < current score
- `CH` -- return count of added + changed (instead of just added)

**Status:** Supported.

### ZSCORE / ZMSCORE

| | |
|---|---|
| **Protocol command** | `ZSCORE key member`, `ZMSCORE key member [member ...]` |
| **Return** | Bulk string (score) or null |
| **Status** | Supported |

### ZRANK / ZREVRANK

Returns zero-based rank of a member.

| | |
|---|---|
| **Protocol command** | `ZRANK key member`, `ZREVRANK key member` |
| **Return** | Integer or null (member not found) |
| **Status** | Supported |

### ZRANGE / ZREVRANGE

Range query by index with optional WITHSCORES.

| | |
|---|---|
| **Protocol command** | `ZRANGE key start stop [WITHSCORES]`, `ZREVRANGE key start stop [WITHSCORES]` |
| **Embedded API** | `FerricStore.zrange(key, start, stop, withscores: bool)` |
| **Return** | Array of members, or interleaved `[member, score, ...]` with WITHSCORES |
| **Status** | Index-based syntax is supported. Unified `ZRANGE` syntax (`BYSCORE`/`BYLEX`/`REV`/`LIMIT`) is not yet supported. Use `ZRANGEBYSCORE`/`ZREVRANGEBYSCORE` for score ranges. |

### ZRANGEBYSCORE / ZREVRANGEBYSCORE

Range by score with optional WITHSCORES and LIMIT.

| | |
|---|---|
| **Protocol command** | `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` |
| **Supported bounds** | Numeric, `-inf`, `+inf`, `(exclusive` prefix |
| **Status** | Supported. Negative LIMIT count means "all remaining". |

### ZCOUNT

Count members with scores in the given range.

| | |
|---|---|
| **Protocol command** | `ZCOUNT key min max` |
| **Status** | Supported. Supports `-inf`, `+inf`, and `(exclusive`. |

### ZINCRBY

Increment the score of a member. Creates the member if it does not exist.

| | |
|---|---|
| **Protocol command** | `ZINCRBY key increment member` |
| **Return** | Bulk string -- the new score |
| **Status** | Supported |

### ZPOPMIN / ZPOPMAX

Pop the lowest/highest scored members.

| | |
|---|---|
| **Protocol command** | `ZPOPMIN key [count]`, `ZPOPMAX key [count]` |
| **Return** | Array of `[member, score, ...]` |
| **Status** | Supported. Cleans up type metadata when empty. |

### ZRANDMEMBER / ZSCAN / ZCARD / ZREM

| Command | Status |
|---------|-------------|
| `ZRANDMEMBER key [count [WITHSCORES]]` | Supported. Negative count allows duplicates. |
| `ZSCAN key cursor [MATCH pattern] [COUNT count]` | Offset-based cursor |
| `ZCARD key` | Supported |
| `ZREM key member [member ...]` | Supported |

---

## Stream Commands

Stream entries are stored as compound keys `X:{stream_key}\0{ms}-{seq}` with field-value pairs serialized as ETF. Stream metadata (length, first/last ID, sequence counters) is tracked in an ETS table for fast access. Stream IDs use a Hybrid Logical Clock (HLC) for monotonicity, even when the wall clock jumps backward.

### XADD

Adds an entry to a stream with optional trimming and NOMKSTREAM.

| | |
|---|---|
| **Protocol command** | `XADD key [NOMKSTREAM] [MAXLEN\|MINID [=\|~] threshold] *\|ID field value [field value ...]` |
| **Embedded API** | `FerricStore.xadd(key, fields)` |
| **Return** | Bulk string -- the generated entry ID |
| **Elixir return** | `{:ok, binary()}` |

**ID generation:** `*` auto-generates using HLC. Explicit IDs must be strictly greater than the last entry. Partial IDs (just milliseconds) auto-assign the sequence.

**Status:** Supported, including NOMKSTREAM and trim options.

### XLEN / XRANGE / XREVRANGE

| Command | Syntax | Return | Notes |
|---------|--------|--------|-------|
| `XLEN` | `XLEN key` | Integer | From ETS metadata, O(1) |
| `XRANGE` | `XRANGE key start end [COUNT count]` | Array of entries | `-` = min, `+` = max |
| `XREVRANGE` | `XREVRANGE key end start [COUNT count]` | Array (reversed) | native TCP mode |

### XREAD

Reads entries from one or more streams. Supports BLOCK for waiting on new data.

| | |
|---|---|
| **Protocol command** | `XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]` |
| **Special IDs** | `$` = only new entries from now on; `0` = all entries |
| **BLOCK behavior** | In TCP mode, the connection registers as a stream waiter and is notified by XADD. In embedded mode, BLOCK is not supported. |
| **Status** | Supported in TCP mode. BLOCK 0 = infinite wait. |

### XTRIM / XDEL

| Command | Syntax | Return | Notes |
|---------|--------|--------|-------|
| `XTRIM` | `XTRIM key MAXLEN\|MINID [=\|~] threshold` | Integer (entries deleted) | `~` is accepted but exact trim is always applied |
| `XDEL` | `XDEL key id [id ...]` | Integer (entries deleted) | Metadata rebuilt after deletion |

### XINFO STREAM

Returns stream metadata as a map.

| | |
|---|---|
| **Protocol command** | `XINFO STREAM key` |
| **Return** | Map with `length`, `first-entry`, `last-entry`, `last-generated-id`, `groups` |
| **Status** | Subset of FerricStore XINFO. FULL option not yet supported. |

### XGROUP CREATE / XREADGROUP / XACK

Consumer group support:

| Command | Syntax | Notes |
|---------|--------|-------|
| `XGROUP CREATE` | `XGROUP CREATE key group id [MKSTREAM]` | `$` for new-only, `0` for all |
| `XREADGROUP` | `XREADGROUP GROUP group consumer [COUNT count] STREAMS key [key ...] id [id ...]` | `>` for new messages, `0` for pending |
| `XACK` | `XACK key group id [id ...]` | Returns count acknowledged |

Consumer group state (pending entries, consumers, last-delivered-id) is tracked in ETS. XGROUP DESTROY, DELCONSUMER, and SETID are not yet implemented.

---

## Key/Generic Commands

### TYPE

Returns the type of a key as a simple string.

| | |
|---|---|
| **Protocol command** | `TYPE key` |
| **Return** | Simple string: `string`, `hash`, `list`, `set`, `zset`, `stream`, or `none` |
| **Status** | Supported |

### RENAME / RENAMENX / COPY

| Command | Syntax | Return | Notes |
|---------|--------|--------|-------|
| `RENAME` | `RENAME key newkey` | `+OK` or error | Copies value+TTL, deletes old |
| `RENAMENX` | `RENAMENX key newkey` | `1` (renamed) or `0` (dest exists) | Same key returns 0 |
| `COPY` | `COPY source dest [REPLACE]` | `1` (success) or error | REPLACE overwrites existing dest |

**Note:** These operate on plain string keys only. Renaming compound data structures (hash, list, set, zset) is not supported -- only the raw value is copied.

### SCAN

Cursor-based key iteration with optional MATCH pattern, COUNT hint, and TYPE filter.

| | |
|---|---|
| **Protocol command** | `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` |
| **Return** | `[next_cursor, [key, ...]]` |

**FerricStore behavior:** Cursor is the last key seen (alphabetic). `"0"` starts from the beginning. The prefix index is used for `prefix:*` patterns for O(matching) performance. Internal compound keys (H:, S:, Z:, T:, VM:, V:) are filtered out.

### RANDOMKEY / DBSIZE / KEYS

| Command | Syntax | Return |
|---------|--------|--------|
| `RANDOMKEY` | `RANDOMKEY` | Random key or null |
| `DBSIZE` | `DBSIZE` | Integer -- key count (excludes internal keys) |
| `KEYS` | `KEYS pattern` | Array of matching keys. Uses prefix index for `prefix:*` patterns. |

### EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT / TTL / PTTL / PERSIST

| Command | Syntax | Return |
|---------|--------|--------|
| `EXPIRE` | `EXPIRE key seconds` | `1` (set) or `0` (key missing) |
| `PEXPIRE` | `PEXPIRE key ms` | `1` or `0` |
| `EXPIREAT` | `EXPIREAT key unix-ts` | `1` or `0` |
| `PEXPIREAT` | `PEXPIREAT key unix-ts-ms` | `1` or `0` |
| `TTL` | `TTL key` | Seconds remaining, `-1` (no expiry), `-2` (missing) |
| `PTTL` | `PTTL key` | Milliseconds remaining, `-1`, `-2` |
| `PERSIST` | `PERSIST key` | `1` (removed), `0` (no expiry or missing) |
| `EXPIRETIME` | `EXPIRETIME key` | Absolute Unix timestamp (seconds), `-1`, `-2` |
| `PEXPIRETIME` | `PEXPIRETIME key` | Absolute Unix timestamp (ms), `-1`, `-2` |

All native TCP mode. Expiry uses HLC timestamps internally.

### OBJECT

| Subcommand | Return | Notes |
|------------|--------|-------|
| `OBJECT ENCODING key` | Type-specific encoding | Returns `"embstr"` (strings <= 44 bytes), `"raw"` (longer strings), `"hashtable"` (hashes), `"quicklist"` (lists), `"skiplist"` (sorted sets), `"stream"` (streams) |
| `OBJECT HELP` | Array of help strings | native TCP mode format |
| `OBJECT FREQ key` | Integer (LFU counter) | Uses keydir LFU, not FerricStore logarithmic frequency |
| `OBJECT IDLETIME key` | Integer (idle seconds) | Derived from LFU last-decrement-time. Returns elapsed seconds since last access. |
| `OBJECT REFCOUNT key` | `1` | Always 1 |

### WAIT

| | |
|---|---|
| **Protocol command** | `WAIT numreplicas timeout` |
| **Return** | `0` (always) |
| **Status** | Stub -- no replica acknowledgement. Always returns immediately. |

---

## Bitmap Commands

Bitmap operations work at the bit level on string values. Bits are numbered MSB-first: bit 0 is the MSB of byte 0 (value 128). Write operations (SETBIT, BITOP) perform a read-modify-write cycle.

| Command | Syntax | Return | Status |
|---------|--------|--------|-------------|
| `SETBIT` | `SETBIT key offset value` | Integer (old bit value) | Supported |
| `GETBIT` | `GETBIT key offset` | Integer (0 or 1) | Supported |
| `BITCOUNT` | `BITCOUNT key [start end [BYTE\|BIT]]` | Integer (count of set bits) | Supported including BYTE/BIT mode |
| `BITPOS` | `BITPOS key bit [start [end [BYTE\|BIT]]]` | Integer (position or -1) | Supported |
| `BITOP` | `BITOP AND\|OR\|XOR\|NOT destkey key [key ...]` | Integer (dest string length) | Supported |

---

## HyperLogLog Commands

HyperLogLog sketches are stored as 16,384-byte binary values (plain strings in Bitcask). No special type metadata.

| Command | Syntax | Return | Status |
|---------|--------|--------|-------------|
| `PFADD` | `PFADD key element [element ...]` | `1` (modified) or `0` | Supported |
| `PFCOUNT` | `PFCOUNT key [key ...]` | Integer (estimated cardinality) | Multi-key merges in memory without writing |
| `PFMERGE` | `PFMERGE destkey sourcekey [sourcekey ...]` | `+OK` | Supported. Takes max across registers. |

---

## Bloom Filter Commands

Backed by mmap NIF resources. Each filter is a memory-mapped file at `data_dir/prob/shard_N/KEY.bloom`. Handles are cached in per-shard ETS tables.

| Command | Syntax | Return | Notes |
|---------|--------|--------|-------|
| `BF.RESERVE` | `BF.RESERVE key error_rate capacity` | `+OK` or error | error_rate: (0,1), capacity: positive int |
| `BF.ADD` | `BF.ADD key element` | `1` (added) or `0` | Auto-creates with defaults (0.01, 100) |
| `BF.MADD` | `BF.MADD key element [element ...]` | Array of 1/0 | Auto-creates |
| `BF.EXISTS` | `BF.EXISTS key element` | `1` (may exist) or `0` | Returns 0 for non-existent keys |
| `BF.MEXISTS` | `BF.MEXISTS key element [element ...]` | Array of 1/0 | Returns all 0s for non-existent keys |
| `BF.CARD` | `BF.CARD key` | Integer | Items added count |
| `BF.INFO` | `BF.INFO key` | Array: Capacity, Size, filters, items, expansion, error rate, hashes, bits | |

**Status:** Uses FerricStoreBloom module syntax. Optimal sizing uses `m = -n*ln(p) / (ln(2))^2`. No scaling/expansion support (single filter).

---

## Cuckoo Filter Commands

Backed by mmap NIF resources at `data_dir/prob/shard_N/KEY.cuckoo`. Supports deletion (unlike Bloom).

| Command | Syntax | Return | Notes |
|---------|--------|--------|-------|
| `CF.RESERVE` | `CF.RESERVE key capacity` | `+OK` or error | Bucket size: 4 |
| `CF.ADD` | `CF.ADD key element` | `1` or error (filter full) | Auto-creates with capacity 1024 |
| `CF.ADDNX` | `CF.ADDNX key element` | `1` (added), `0` (already exists), or error | |
| `CF.DEL` | `CF.DEL key element` | `1` (deleted) or `0` (not found) | Deletes one occurrence |
| `CF.EXISTS` | `CF.EXISTS key element` | `1` or `0` | |
| `CF.MEXISTS` | `CF.MEXISTS key element [element ...]` | Array of 1/0 | |
| `CF.COUNT` | `CF.COUNT key element` | Integer (approximate count) | Fingerprint occurrences |
| `CF.INFO` | `CF.INFO key` | Array: Size, buckets, filters, items, deletes, bucket_size, fingerprint_size, max_kicks, expansion | |

**Status:** Uses FerricStoreBloom/Cuckoo module syntax.

---

## Count-Min Sketch Commands

Backed by mmap NIF resources at `data_dir/prob/shard_N/KEY.cms`.

| Command | Syntax | Return | Notes |
|---------|--------|--------|-------|
| `CMS.INITBYDIM` | `CMS.INITBYDIM key width depth` | `+OK` | width and depth must be > 0 |
| `CMS.INITBYPROB` | `CMS.INITBYPROB key error probability` | `+OK` | width = ceil(e/error), depth = ceil(ln(1/prob)) |
| `CMS.INCRBY` | `CMS.INCRBY key item count [item count ...]` | Array of counts | Each count >= 1 |
| `CMS.QUERY` | `CMS.QUERY key item [item ...]` | Array of estimated counts | |
| `CMS.MERGE` | `CMS.MERGE dst numkeys key [key ...] [WEIGHTS w ...]` | `+OK` | All sources must have same width/depth. Creates dst if missing. |
| `CMS.INFO` | `CMS.INFO key` | `[width, W, depth, D, count, C]` | |

**Status:** Uses FerricStoreBloom CMS module syntax.

---

## TopK Commands

Backed by mmap NIF resources at `prob/shard_N/KEY.topk`. Uses Count-Min Sketch internally with a Heavy Keeper algorithm.

| Command | Syntax | Return | Notes |
|---------|--------|--------|-------|
| `TOPK.RESERVE` | `TOPK.RESERVE key k [width depth]` | `+OK` | Defaults: width=8, depth=7 |
| `TOPK.ADD` | `TOPK.ADD key element [element ...]` | Array (evicted items or nil) | |
| `TOPK.INCRBY` | `TOPK.INCRBY key element count [element count ...]` | Array (evicted items or nil) | |
| `TOPK.QUERY` | `TOPK.QUERY key element [element ...]` | Array of 1/0 | |
| `TOPK.LIST` | `TOPK.LIST key [WITHCOUNT]` | Array of items (or interleaved items+counts) | |
| `TOPK.INFO` | `TOPK.INFO key` | `[k, K, width, W, depth, D]` | |

**Status:** Uses FerricStoreBloom TopK module syntax.

---

## TDigest Commands

T-digests provide accurate rank-based statistics (quantiles, CDF, trimmed means) with bounded memory and high accuracy at the tails (P99, P99.9). Stored as tagged tuples `{:tdigest, centroids, metadata}` in Bitcask.

| Command | Syntax | Return | Notes |
|---------|--------|--------|-------|
| `TDIGEST.CREATE` | `TDIGEST.CREATE key [COMPRESSION c]` | `+OK` | Default compression: 100 |
| `TDIGEST.ADD` | `TDIGEST.ADD key value [value ...]` | `+OK` | Accepts floats and integers |
| `TDIGEST.RESET` | `TDIGEST.RESET key` | `+OK` | Clears data, preserves compression |
| `TDIGEST.QUANTILE` | `TDIGEST.QUANTILE key q [q ...]` | Array of float strings | q must be in [0, 1] |
| `TDIGEST.CDF` | `TDIGEST.CDF key value [value ...]` | Array of float strings | CDF at each value |
| `TDIGEST.RANK` | `TDIGEST.RANK key value [value ...]` | Array of integers | Estimated rank |
| `TDIGEST.REVRANK` | `TDIGEST.REVRANK key value [value ...]` | Array of integers | Reverse rank |
| `TDIGEST.BYRANK` | `TDIGEST.BYRANK key rank [rank ...]` | Array of float strings | Value at rank |
| `TDIGEST.BYREVRANK` | `TDIGEST.BYREVRANK key rank [rank ...]` | Array of float strings | Value at reverse rank |
| `TDIGEST.TRIMMED_MEAN` | `TDIGEST.TRIMMED_MEAN key lo hi` | Float string | lo must be < hi |
| `TDIGEST.MIN` | `TDIGEST.MIN key` | Float string or `"nan"` | |
| `TDIGEST.MAX` | `TDIGEST.MAX key` | Float string or `"nan"` | |
| `TDIGEST.INFO` | `TDIGEST.INFO key` | Array: Compression, Capacity, Merged/Unmerged nodes, weights, total_compressions, Memory usage | |
| `TDIGEST.MERGE` | `TDIGEST.MERGE dest numkeys key [key ...] [COMPRESSION c] [OVERRIDE]` | `+OK` | OVERRIDE replaces dest; without it, merges into existing |

**Status:** Uses FerricStoreBloom TDigest module syntax.

---

## Geo Commands

Geo is implemented on top of Sorted Sets. Members are stored with 52-bit interleaved geohash scores (26 bits per axis, ~0.6mm precision), matching FerricStore's encoding. No new data structure is needed.

| Command | Syntax | Return | Notes |
|---------|--------|--------|-------|
| `GEOADD` | `GEOADD key [NX\|XX] [CH] lon lat member [...]` | Integer (added) | Same flags as ZADD |
| `GEOPOS` | `GEOPOS key member [member ...]` | Array of `[lon, lat]` or null | |
| `GEODIST` | `GEODIST key member1 member2 [M\|KM\|FT\|MI]` | Bulk string (distance) or null | Default unit: meters |
| `GEOHASH` | `GEOHASH key member [member ...]` | Array of 11-char base32 strings | Standard geohash alphabet |
| `GEOSEARCH` | `GEOSEARCH key FROMLONLAT lon lat\|FROMMEMBER member BYRADIUS radius unit\|BYBOX w h unit [ASC\|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]` | Array | Full FerricStore GEOSEARCH syntax |
| `GEOSEARCHSTORE` | `GEOSEARCHSTORE dest source [GEOSEARCH opts] [STOREDIST]` | Integer (stored count) | |

**Status:** Supported including all GEOSEARCH options.

---

## FerricStore-Native Commands

These commands extend beyond the FerricStore command set with operations not available in standard FerricStore.

### CAS (Compare-and-Swap)

Atomically sets a key only if its current value matches the expected value. Routed directly through `Router.cas/4`.

| | |
|---|---|
| **Protocol command** | `CAS key expected new_value [EX seconds]` |
| **Embedded API** | `FerricStore.cas(key, expected, new_value)` |
| **Return** | `1` (swapped), `0` (value mismatch), null (key missing) |
| **Elixir return** | `{:ok, true}`, `{:ok, false}`, or `{:ok, nil}` |

### LOCK / UNLOCK / EXTEND

Distributed lock with owner identity and TTL. Routed through `Router.lock/3`, `Router.unlock/2`, `Router.extend/3`.

| Command | Syntax | Return |
|---------|--------|--------|
| `LOCK` | `LOCK key owner ttl_ms` | `+OK` (acquired) or `ERR` (already held) |
| `UNLOCK` | `UNLOCK key owner` | `1` (released) or `ERR` (wrong owner / not held) |
| `EXTEND` | `EXTEND key owner ttl_ms` | `1` (extended) or `ERR` (wrong owner / not held) |

### RATELIMIT.ADD

Sliding window rate limiter. Routed through `Router.ratelimit_add/4`.

| | |
|---|---|
| **Protocol command** | `RATELIMIT.ADD key window_ms max_count [count]` |
| **Embedded API** | `FerricStore.ratelimit_add(key, window_ms, max)` |
| **Return** | Array: `[allowed (0\|1), current_count, remaining, retry_after_ms]` |
| **Default count** | 1 |

### FETCH_OR_COMPUTE

Cache-aside with stampede protection. The first caller to a missing key is designated the "computer" -- all concurrent callers block until the value is available.

| Command | Syntax | Return |
|---------|--------|--------|
| `FETCH_OR_COMPUTE` | `FETCH_OR_COMPUTE key ttl_ms [hint]` | `["hit", value]` or `["compute", hint, token]` |
| `FETCH_OR_COMPUTE_RESULT` | `FETCH_OR_COMPUTE_RESULT key token value ttl_ms` | `+OK` |
| `FETCH_OR_COMPUTE_ERROR` | `FETCH_OR_COMPUTE_ERROR key token message` | `+OK` |

### FERRICSTORE.KEY_INFO

Returns diagnostic metadata about a key.

| | |
|---|---|
| **Protocol command** | `FERRICSTORE.KEY_INFO key` |
| **Return** | Array: `[type, T, value_size, N, ttl_ms, N, hot_cache_status, hot\|cold, last_write_shard, N]` |

### FERRICSTORE.DOCTOR

Runs bounded operator diagnostics and safe background repair jobs. This is an
admin command intended for production debugging and dashboard actions. Inline
`CHECK` reads bounded metadata only; expensive checks or repairs should be run
with `START` so the client connection is not held.

| | |
|---|---|
| **Protocol command** | `FERRICSTORE.DOCTOR <subcommand> [args...]` |
| **Embedded API** | Internal command surface only |
| **Return** | Map with `status`, `checks`, `job_id`, or `jobs` depending on subcommand |
| **ACL** | `@admin`; repair jobs are also `@dangerous` |
| **Dashboard** | `/dashboard/doctor` |

Supported scopes:

| Scope | What It Checks |
|-------|----------------|
| `BITCASK` | Keydir availability, keydir binary bytes, data file count, and data bytes per shard |
| `BLOB_REFS` | Large-value blob segment metadata and protected blob refs |
| `FLOW_LMDB` | Flow LMDB projection health, pending ops, oldest pending age, replay-safe lag, and degraded shards |
| `ALL` | All supported scopes |

Subcommands:

| Command | Purpose |
|---------|---------|
| `FERRICSTORE.DOCTOR CHECK [SCOPE scope]` | Run a bounded inline check. Omitting `SCOPE` checks all scopes. |
| `FERRICSTORE.DOCTOR CHECK SCOPES n scope...` | Run a bounded inline check for specific scopes. |
| `FERRICSTORE.DOCTOR START CHECK [SCOPE scope]` | Start the same check as a background job and return `job_id`. |
| `FERRICSTORE.DOCTOR START REPAIR PROJECTIONS SCOPE FLOW_LMDB` | Flush and reconcile the Flow LMDB cold projection from durable Flow records. |
| `FERRICSTORE.DOCTOR STATUS job_id` | Return one background job. |
| `FERRICSTORE.DOCTOR LIST` | Return known doctor jobs, newest first. |
| `FERRICSTORE.DOCTOR CANCEL job_id` | Cancel a running background job. |

Examples:

```bash
FERRICSTORE.DOCTOR CHECK
FERRICSTORE.DOCTOR CHECK SCOPE FLOW_LMDB
FERRICSTORE.DOCTOR START CHECK SCOPE BITCASK
FERRICSTORE.DOCTOR START REPAIR PROJECTIONS SCOPE FLOW_LMDB
FERRICSTORE.DOCTOR STATUS doctor-1-123
FERRICSTORE.DOCTOR LIST
```

Repair notes:

- `START REPAIR PROJECTIONS SCOPE FLOW_LMDB` repairs the cold/query projection
  only. It does not mutate hot Flow indexes or rewrite user state.
- Flow command durability does not depend on LMDB projection being current; the
  durable source of truth remains FerricStore-managed Raft segment/apply-projection
  storage.
- Use this repair when `FLOW_LMDB` reports degraded shards or projection lag that
  does not drain after the underlying disk/LMDB issue is fixed.

---

## Server Commands

### PING / ECHO

| Command | Syntax | Return |
|---------|--------|--------|
| `PING` | `PING [message]` | `+PONG` (no args), or bulk string (with message) |
| `ECHO` | `ECHO message` | Bulk string |

### INFO

Returns server information. Supports sections: `server`, `clients`, `memory`, `keyspace`, `stats`, `persistence`, `replication`, `cpu`, `namespace_config`, `raft`, `bitcask`, `ferricstore`, `keydir_analysis`. Use `all` or `everything` for all sections.

| | |
|---|---|
| **Protocol command** | `INFO [section]` |
| **FerricStore sections** | `raft` (per-shard role/term/commit), `bitcask` (per-shard file counts/sizes), `ferricstore` (raft committed, hot cache evictions), `keydir_analysis` (per-prefix key breakdown), `namespace_config` (group-commit settings) |

The `server` section reports FerricStore version and native protocol metadata.

### CONFIG

| Subcommand | Syntax | Notes |
|------------|--------|-------|
| `CONFIG GET` | `CONFIG GET pattern` | Glob pattern matching |
| `CONFIG SET` | `CONFIG SET key value` | Changes logged to audit log |
| `CONFIG SET LOCAL` | `CONFIG SET LOCAL key value` | Node-local config override |
| `CONFIG GET LOCAL` | `CONFIG GET LOCAL key` | Read node-local config |
| `CONFIG RESETSTAT` | `CONFIG RESETSTAT` | Resets stats + slowlog |
| `CONFIG REWRITE` | `CONFIG REWRITE` | Persists config changes |

### SLOWLOG

| Subcommand | Syntax | Return |
|------------|--------|--------|
| `SLOWLOG GET` | `SLOWLOG GET [count]` | Array of `[id, timestamp_us, duration_us, command]` |
| `SLOWLOG LEN` | `SLOWLOG LEN` | Integer |
| `SLOWLOG RESET` | `SLOWLOG RESET` | `+OK` |

### COMMAND

| Subcommand | Syntax | Return |
|------------|--------|--------|
| `COMMAND` | `COMMAND` | Array of command info tuples |
| `COMMAND COUNT` | `COMMAND COUNT` | Integer |
| `COMMAND LIST` | `COMMAND LIST` | Array of command names |
| `COMMAND INFO` | `COMMAND INFO name [name ...]` | Array of info tuples (null for unknown) |
| `COMMAND DOCS` | `COMMAND DOCS name [name ...]` | Interleaved `[name, [summary]]` |
| `COMMAND GETKEYS` | `COMMAND GETKEYS cmd [args ...]` | Array of key arguments |

### CLIENT

Handled via `dispatch_client/3` with per-connection state:

| Subcommand | Syntax | Return |
|------------|--------|--------|
| `CLIENT ID` | `CLIENT ID` | Integer (connection ID) |
| `CLIENT SETNAME` | `CLIENT SETNAME name` | `+OK` |
| `CLIENT GETNAME` | `CLIENT GETNAME` | Bulk string or null |
| `CLIENT INFO` | `CLIENT INFO` | Info string for current connection |
| `CLIENT LIST` | `CLIENT LIST [TYPE type]` | Info string for all connections |
| `CLIENT TRACKING` | `CLIENT TRACKING ON\|OFF [REDIRECT id] [PREFIX ...] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]` | `+OK` |
| `CLIENT CACHING` | `CLIENT CACHING YES\|NO` | `+OK` |
| `CLIENT TRACKINGINFO` | `CLIENT TRACKINGINFO` | Tracking configuration |
| `CLIENT GETREDIR` | `CLIENT GETREDIR` | Integer (redirect target or 0) |

### Other Server Commands

| Command | Syntax | Return | Notes |
|---------|--------|--------|-------|
| `FLUSHDB` | `FLUSHDB [ASYNC\|SYNC]` | `+OK` | Both modes execute synchronously |
| `FLUSHALL` | `FLUSHALL [ASYNC\|SYNC]` | `+OK` | Alias for FLUSHDB |
| `SELECT` | `SELECT db` | Error | Single-database only |
| `SAVE` | `SAVE` | `+OK` | No-op (Bitcask is always persisted) |
| `BGSAVE` | `BGSAVE` | `+Background saving started` | No-op |
| `LASTSAVE` | `LASTSAVE` | Integer (current timestamp) | |
| `LOLWUT` | `LOLWUT [VERSION v]` | ASCII art | FerricStore branding |
| `DEBUG SLEEP` | `DEBUG SLEEP seconds` | `+OK` | Testing only. Logged to audit log. |
| `MODULE LIST` | `MODULE LIST` | Empty array | Modules not supported |
| `WAITAOF` | `WAITAOF numlocal numreplicas timeout` | `[0, 0]` | Stub |
| `MEMORY USAGE` | `MEMORY USAGE key` | Integer (estimated bytes) | |

---

## Transaction Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `MULTI` | `MULTI` | Start a transaction. Subsequent commands are queued (return `+QUEUED`). |
| `EXEC` | `EXEC` | Execute all queued commands atomically. Returns array of results. Returns null if WATCH detected a change. |
| `DISCARD` | `DISCARD` | Discard queued commands, exit MULTI state. |
| `WATCH` | `WATCH key [key ...]` | Watch keys for changes. If any watched key is modified before EXEC, the transaction is aborted. |
| `UNWATCH` | `UNWATCH` | Stop watching all keys. |

Transactions work at the connection level. WATCH implements optimistic locking -- if a watched key is modified by another connection between WATCH and EXEC, EXEC returns null (transaction aborted).

---

## Pub/Sub Commands

| Command | Syntax | Return |
|---------|--------|--------|
| `SUBSCRIBE` | `SUBSCRIBE channel [channel ...]` | Push messages: `[subscribe, channel, count]` |
| `UNSUBSCRIBE` | `UNSUBSCRIBE [channel ...]` | Push messages: `[unsubscribe, channel, count]` |
| `PSUBSCRIBE` | `PSUBSCRIBE pattern [pattern ...]` | Push messages: `[psubscribe, pattern, count]` |
| `PUNSUBSCRIBE` | `PUNSUBSCRIBE [pattern ...]` | Push messages: `[punsubscribe, pattern, count]` |
| `PUBLISH` | `PUBLISH channel message` | Integer (subscribers that received) |
| `PUBSUB CHANNELS` | `PUBSUB CHANNELS [pattern]` | Array of active channels |
| `PUBSUB NUMSUB` | `PUBSUB NUMSUB [channel ...]` | Array of `[channel, count, ...]` |
| `PUBSUB NUMPAT` | `PUBSUB NUMPAT` | Integer (pattern subscriptions) |

---

## ACL Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `ACL SETUSER` | `ACL SETUSER username [rule ...]` | Create/update user |
| `ACL DELUSER` | `ACL DELUSER username [username ...]` | Delete user(s) |
| `ACL GETUSER` | `ACL GETUSER username` | Get user info |
| `ACL LIST` | `ACL LIST` | List all users |
| `ACL WHOAMI` | `ACL WHOAMI` | Current user |
| `ACL SAVE` | `ACL SAVE` | Persist ACL to file |
| `ACL LOAD` | `ACL LOAD` | Load ACL from file |
| `AUTH` | `AUTH [username] password` | Authenticate connection |

---

## FerricStore Command Notes

1. **Single database** -- `SELECT` returns an error. FerricStore is single-database.
2. **Native TCP mode** -- clients start with `HELLO`/`STARTUP` on the native control lane.
3. **No Lua scripting** -- `EVAL`/`EVALSHA` are not implemented. Use CAS, LOCK, and FETCH_OR_COMPUTE for atomic operations.
4. **No blocking commands in embedded mode** -- `BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `XREAD BLOCK` require a TCP connection.
5. **Probabilistic structures are built-in** -- available without an external module. BF, CF, CMS, TopK, TDigest are all native.
6. **CAS is a native command** -- available as a direct compare-and-swap command. WATCH/MULTI/EXEC is also supported.
8. **FETCH_OR_COMPUTE** -- built-in cache stampede protection.
9. **Group commit** -- writes are batched for higher throughput. Individual write latency includes the batch window (default 1ms). Use hash tags `{tag}` to colocate related keys on the same shard for maximum batching -- see [Best Practices](best-practices.md).
10. **HLC timestamps** -- expiry uses Hybrid Logical Clock timestamps instead of wall-clock time. Monotonic even during clock skew.
11. **Compound key storage** -- hash fields, set members, and zset members are stored as individual Bitcask entries with structured key prefixes, enabling O(1) field-level access without deserializing the entire data structure.
12. **SCAN cursor** -- uses alphabetic key position, a key-position cursor rather than an opaque hash-table cursor. Functionally equivalent but cursor values differ.
13. **OBJECT ENCODING** -- returns type-specific encodings (`"embstr"`, `"raw"`, `"hashtable"`, `"quicklist"`, `"skiplist"`, `"stream"`) and does not expose implementation-specific internal encodings such as `ziplist`, `listpack`, or `intset`.
14. **INFO sections** -- includes FerricStore-specific sections: `raft`, `bitcask`, `ferricstore`, `keydir_analysis`, `namespace_config`.
