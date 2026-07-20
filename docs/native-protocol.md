# FerricStore Native TCP Protocol

FerricStore native protocol is the standalone binary SDK data plane. It
delegates command semantics to the same FerricStore engine path used by the
embedded Elixir API.

The hot frame scanner/emitter is implemented as a pure Rust NIF:

```text
Rust NIF:
  frame scan
  frame header/body emit

Elixir:
  TLS/socket ownership
  protected mode / ACL
  client registry
  lane supervision
  command dispatch
  storage/Flow correctness path
```

The Rust boundary must remain side-effect free.

## Frame

```text
magic      4 bytes   "FSNP"
version    1 byte    low 7 bits version, high bit response direction
flags      1 byte    trace/custom/warning/compressed/no-reply/chunk flags
lane_id    4 bytes   0 control/events, >0 ordered data lane
opcode     2 bytes   unsigned big-endian
request_id 8 bytes   unsigned big-endian
body_len   4 bytes   unsigned big-endian
body       N bytes
```

Requests may be pipelined. Responses carry the same `lane_id` and
`request_id`. Responses may arrive out of order across lanes, but order is
preserved within one lane.

Version follows Cassandra's useful direction-bit pattern:

```text
0x01 request for protocol v1
0x81 response for protocol v1
```

Flags:

```text
0x01 trace requested/present
0x02 custom payload present
0x04 warnings present
0x08 compressed body
0x10 no reply requested
0x20 more chunks follow
```

Requests accept `0x10` by default. `0x01 trace requested` is accepted only when
the server enables native tracing. `0x08 compressed body` is accepted only after
the server enables request compression and the connection negotiates
`compression: "zlib"` through `HELLO` or `STARTUP`. `0x20 more chunks follow` is
accepted for request reassembly. Warning and custom-payload request flags are
reserved. Unknown request flags are rejected before command dispatch.

Client command requests must use a non-zero `request_id`. `request_id=0` is
reserved for server-initiated management frames such as `EVENT` and `GOAWAY`.

`0x10 no reply requested` executes the command but suppresses the normal
response frame. Use it only for idempotent or application-tolerant writes where
the client does not need the server result.

Most command bodies may include `deadline_ms`, an absolute server Unix
millisecond deadline. If the deadline is already expired when dispatch begins,
the command returns `deadline_exceeded` and is not executed. The field must be
a non-negative integer; `0` disables the deadline for that request.

Chunking:

```text
request chunks:  same lane_id/opcode/request_id, 0x20 on all non-final chunks
response chunks: same lane_id/opcode/request_id, full response body split
```

The server never emits a response frame body larger than the
`max_frame_bytes` advertised for the connection. A configured response chunk
size above that limit is clamped, and `0` selects the frame limit.

Compressed chunked payloads are compressed as one logical body, then split.
Receivers reassemble chunks first, then decompress if `0x08` is present on the
final logical response. The server bounds incomplete request chunk streams per
connection with `FERRICSTORE_NATIVE_MAX_PENDING_CHUNKS` and
`FERRICSTORE_NATIVE_MAX_PENDING_CHUNK_BYTES`. Compressed request bodies are
rejected if decompressed bytes exceed `max_frame_bytes`.

## Typed values

```text
0 nil
1 true
2 false
3 signed i64
4 binary: u32 length + bytes
5 array:  u32 count + values
6 map:    u32 count + repeated u32 key_len + key bytes + value
7 f64
```

Response body starts with a `u16` status code followed by one typed value.

```text
0 ok
1 error
2 auth
3 noperm
4 busy/backpressure
5 reroute
6 bad_request
```

## Control opcodes

```text
0x0001 HELLO
0x0002 AUTH
0x0003 PING
0x0004 CLIENT.SETNAME
0x0005 CLIENT.INFO
0x0006 ROUTE
0x0007 SHARDS
0x0008 BACKPRESSURE
0x0009 QUIT
0x000A GOAWAY          server-initiated, request_id=0
0x000B OPTIONS
0x000C STARTUP
0x000D WINDOW_UPDATE
0x000E PIPELINE
0x000F ROUTE_BATCH
0x0010 EVENT           server-initiated, request_id=0
0x0011 SUBSCRIBE_EVENTS
0x0012 UNSUBSCRIBE_EVENTS
```

`OPTIONS` returns supported versions/features without changing session state.
`STARTUP`/`HELLO` returns protocol version, client id, auth requirement,
backpressure, limits, and route metadata. `ROUTE` returns slot/shard/leader
native endpoint hints for a key. `ROUTE_BATCH` returns route hints for many
keys. `SHARDS` returns current slot ranges, `route_epoch`, and the native
endpoint for the current leader of each shard when known. If leader lookup is
temporarily unavailable, the server returns a local fallback route and still
keeps server-side write redirection active.

`GOAWAY` is sent by the server with `request_id=0` during graceful shutdown or
drain. Clients should stop writing new requests on that connection, finish
already correlated responses if possible, then reconnect with jitter.

`EVENT` is server-initiated on lane `0` with `request_id=0`.

Supported events:

```text
GOAWAY
TOPOLOGY_CHANGED
BACKPRESSURE_CHANGED
AUTH_INVALIDATED
FLOW_WAKE
```

`AUTH_INVALIDATED` is security-relevant. Native multiplexing can have queued
data-lane commands, so affected native sessions fail closed: the server sends
the event when subscribed and closes the connection. Clients must reconnect and
authenticate again.

`WINDOW_UPDATE` updates per-connection and per-lane inflight request limits when
fields are present:

```text
max_inflight_per_connection
max_inflight_per_lane
```

The server clamps requested windows to configured server maxima and enforces
those windows in addition to bounded lane queues and frame limits. A closed
window returns `busy` with `flow_control_window_exhausted`.

## Heartbeats and idle close

Native clients should send application-level heartbeats on idle connections,
similar to Cassandra drivers sending protocol heartbeats over the native
transport. FerricStore uses `PING` on the control lane for this.

Recommended defaults:

```text
client heartbeat interval: 30s
client heartbeat timeout: 30s
server idle timeout: 90s
```

Any inbound frame resets the server idle timer. If no frame arrives before
`FERRICSTORE_NATIVE_IDLE_TIMEOUT_MS`, the server closes the connection and
cleans up lanes, subscriptions, and client registry state. Set
`FERRICSTORE_NATIVE_IDLE_TIMEOUT_MS=0` to disable server idle close.

SDKs should close and replace a socket if a heartbeat response is not received
within the heartbeat timeout. Do not rely only on TCP keepalive; OS keepalive is
too slow for application-level failure detection in many deployments.

`STARTUP` may include:

```text
client_name
driver_name
events
compression: "none" | "zlib"
```

`compression: "zlib"` is an opt-in server feature. The default advertised and
accepted compression is `"none"`.

## KV opcodes

```text
0x0101 GET
0x0102 SET
0x0103 DEL
0x0104 MGET
0x0105 MSET
0x0106 CAS
0x0107 LOCK
0x0108 UNLOCK
0x0109 EXTEND
0x010A RATELIMIT.ADD
0x010B FETCH_OR_COMPUTE
0x010C FETCH_OR_COMPUTE_RESULT
0x010D FETCH_OR_COMPUTE_ERROR
0x0110 HSET
0x0111 HGET
0x0112 HMGET
0x0113 HGETALL
0x0120 LPUSH
0x0121 RPUSH
0x0122 LPOP
0x0123 RPOP
0x0124 LRANGE
0x0130 SADD
0x0131 SREM
0x0132 SMEMBERS
0x0133 SISMEMBER
0x0140 ZADD
0x0141 ZREM
0x0142 ZRANGE
0x0143 ZSCORE
```

Bodies are maps. Example:

```text
SET:  {"key": "k", "value": <bytes>, "ttl": 1000}
MSET: {"pairs": [{"key": "k1", "value": "v1"}, {"key": "k2", "value": "v2"}]}
CAS:  {"key": "k", "expected": <bytes>, "value": <bytes>, "ttl": 1000}
HSET: {"key": "h", "fields": {"field": "value"}}
HMGET: {"key": "h", "fields": ["field", "missing"]}
LPUSH: {"key": "l", "values": ["a", "b"]}
LRANGE: {"key": "l", "start": 0, "stop": -1}
SADD: {"key": "s", "members": ["a", "b"]}
ZADD: {"key": "z", "items": [[1.0, "a"], [2.0, "b"]]}
ZRANGE: {"key": "z", "start": 0, "stop": -1, "withscores": true}
```

All MSET keys must hash to one slot. Cross-slot payloads fail before mutation;
accepted payloads are committed atomically as one state-machine command.

Hash/list/set/sorted-set opcodes use the same store semantics as the embedded
command handlers. Native requests use typed map payloads; compact binary fast
paths can be added for commands that show up as real bottlenecks.

## Admin/observability opcodes

```text
0x0301 CLUSTER.HEALTH
0x0302 CLUSTER.STATS
0x0303 CLUSTER.KEYSLOT
0x0304 CLUSTER.SLOTS
0x0305 CLUSTER.STATUS
0x0306 CLUSTER.JOIN
0x0307 CLUSTER.LEAVE
0x0308 CLUSTER.FAILOVER
0x0309 CLUSTER.PROMOTE
0x030A CLUSTER.DEMOTE
0x030B CLUSTER.ROLE
0x030C FERRICSTORE.KEY_INFO
0x030D FERRICSTORE.CONFIG
0x030E FERRICSTORE.HOTNESS
0x030F FERRICSTORE.METRICS
0x0310 FERRICSTORE.BLOBGC
```

Most admin bodies use an `args` list so the native protocol can delegate to the
existing command handlers:

```text
CLUSTER.JOIN:       {"args": ["node@host", "REPLACE"]}
CLUSTER.FAILOVER:   {"args": ["0", "node@host"]}
FERRICSTORE.CONFIG: {"args": ["GET", "prefix"]}
```

Key-addressed admin commands also include `key` for ACL/key routing:

```text
CLUSTER.KEYSLOT:     {"key": "k", "args": ["k"]}
FERRICSTORE.KEY_INFO: {"key": "k", "args": ["k"]}
```

## Flow opcodes

```text
0x0201 FLOW.CREATE
0x0202 FLOW.GET
0x0203 FLOW.CLAIM_DUE
0x0204 FLOW.COMPLETE
0x0205 FLOW.TRANSITION
0x0206 FLOW.RETRY
0x0207 FLOW.FAIL
0x0208 FLOW.CANCEL
0x0209 FLOW.EXTEND_LEASE
0x020A FLOW.HISTORY
0x020B FLOW.VALUE.PUT
0x020C FLOW.VALUE.MGET
0x020D FLOW.SIGNAL
0x020E FLOW.LIST
0x020F FLOW.CREATE_MANY
0x0210 FLOW.COMPLETE_MANY
0x0211 FLOW.TRANSITION_MANY
0x0212 FLOW.RETRY_MANY
0x0213 FLOW.FAIL_MANY
0x0214 FLOW.CANCEL_MANY
0x0215 FLOW.RECLAIM
0x0216 FLOW.REWIND
0x0217 FLOW.TERMINALS
0x0218 FLOW.FAILURES
0x0219 FLOW.BY_PARENT
0x021A FLOW.BY_ROOT
0x021B FLOW.BY_CORRELATION
0x021C FLOW.INFO
0x021D FLOW.STUCK
0x021E FLOW.POLICY.SET
0x021F FLOW.POLICY.GET
0x0220 FLOW.SPAWN_CHILDREN
0x0221 FLOW.RETENTION_CLEANUP
0x0222 FLOW.STEP_CONTINUE
0x0223 FLOW.START_AND_CLAIM
0x0224 FLOW.RUN_STEPS_MANY
0x0225 FLOW.SCHEDULE.CREATE
0x0226 FLOW.SCHEDULE.GET
0x0227 FLOW.SCHEDULE.DELETE
0x0228 FLOW.SCHEDULE.FIRE_DUE
0x0229 FLOW.SCHEDULE.LIST
0x022A FLOW.SCHEDULE.FIRE
0x022B FLOW.SCHEDULE.PAUSE
0x022C FLOW.SCHEDULE.RESUME
0x022D FLOW.STATS
0x022E FLOW.ATTRIBUTES
0x022F FLOW.ATTRIBUTE_VALUES
0x0230 FLOW.SEARCH
0x0231 FLOW.QUERY
0x0240 FLOW.EFFECT.RESERVE
0x0241 FLOW.EFFECT.CONFIRM
0x0242 FLOW.EFFECT.FAIL
0x0243 FLOW.EFFECT.COMPENSATE
0x0244 FLOW.EFFECT.GET
0x0245 FLOW.GOVERNANCE.LEDGER
0x0246 FLOW.APPROVAL.REQUEST
0x0247 FLOW.APPROVAL.APPROVE
0x0248 FLOW.APPROVAL.REJECT
0x0249 FLOW.APPROVAL.GET
0x024A FLOW.CIRCUIT.OPEN
0x024B FLOW.CIRCUIT.CLOSE
0x024C FLOW.CIRCUIT.GET
0x024D FLOW.BUDGET.RESERVE
0x024E FLOW.BUDGET.GET
0x024F FLOW.LIMIT.LEASE
0x0250 FLOW.LIMIT.SPEND
0x0251 FLOW.LIMIT.RELEASE
0x0252 FLOW.LIMIT.GET
0x0253 FLOW.APPROVAL.LIST
0x0254 FLOW.GOVERNANCE.OVERVIEW
0x0255 FLOW.BUDGET.LIST
0x0256 FLOW.LIMIT.LIST
0x0257 FLOW.BUDGET.COMMIT
0x0258 FLOW.BUDGET.RELEASE
```

Flow bodies are maps with command fields plus options. For example:

```text
FLOW.CREATE:
{"id": "flow-1", "type": "email", "state": "queued", "payload": <typed value>,
 "max_active_ms": 60000}

FLOW.POLICY.SET:
{"type": "email", "expected_generation": 7, "replace": false,
 "max_active_ms": 300000}

FLOW.CLAIM_DUE:
{"type": "email", "state": "queued", "worker": "w1", "limit": 100, "lease_ms": 30000}

FLOW.COMPLETE:
{"id": "flow-1", "lease_token": "...", "fencing_token": 1, "result": <typed value>}

FLOW.VALUE.MGET:
{"refs": ["ref-a", "ref-b"], "max_bytes": 65536}

FLOW.STATS:
{"type": "email", "state": "queued", "attributes": {"tenant": "acme"}}

FLOW.SEARCH:
{"type": "email", "state": "queued", "attributes": {"tenant": "acme"},
 "state_meta": {"queued": {"version": "1"}}, "consistent_projection": true}

FLOW.QUERY:
{"version": "FQL1",
 "query": "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD",
 "params": {"partition": "tenant-a", "flow_id": "flow-1"}}
```

Typed `payload`, `result`, and `error` values stay binary-safe and structured at
the protocol layer. Storage behavior is unchanged: commands still use current
FerricFlow value/ref rules.

`FLOW.POLICY.SET` patches the current policy by default. Set `replace` to `true`
to replace the complete snapshot, and pass a non-negative `expected_generation`
for compare-and-swap. Successful policy reads and writes include the monotonic
`generation`; a mismatch returns `ERR stale flow policy generation`.

Flow `attributes` are small indexed metadata values for list/stats/dashboard
filters. They are not payload bytes and are projected asynchronously for query
use.

Flow `state_meta` values are small per-state metadata maps. Mutating commands
accept `STATE_META <key> <value>` options, and `FLOW.POLICY.SET <type>
INDEXED_STATE_META <key>` enables one indexed state metadata key for broad
search on that Flow type. Metadata for one state does not replace metadata
stored for another state. Changing or removing `INDEXED_STATE_META` rewrites
existing Flow records for that type so LMDB query rows are backfilled or deleted
for the affected key.

### FLOW.QUERY and FQL1

`FLOW.QUERY` is the versioned Flow read envelope. The OSS default query provider
is deliberately limited to an exact authoritative record lookup:

```text
[EXPLAIN] FROM runs
WHERE partition_key = <value> AND run_id = <value>
RETURN RECORD[;]
```

The two predicates may appear in either order. A value is either a single-quoted
keyword literal or a named parameter such as `@flow_id`; doubled single quotes
escape a quote inside a literal. Named parameter values must be non-empty
binaries. Query text is limited to 16 KiB and 32 lexical tokens. Bound
partition keys are limited to 65,535 bytes, and bound run IDs are limited so
the resulting physical point key remains within the store key-size ceiling.

The OSS provider rejects every other source, field, predicate, return shape, and
FQL version before storage access. In particular, a run-id-only query is not
accepted because records with explicit partition keys require the partition for
an exact physical lookup. This path delegates directly to the existing
`FLOW.GET` point read and never performs a scan or materializes a candidate set.

Enterprise installs a separate capability-negotiated provider for bounded
record collections, event history, lineage reads, and exact `RETURN COUNT`.
Clients must check the advertised query shapes rather than infer Enterprise
support from the `FQL1` language version.

`RETURN RECORD` returns a structural allowlist, not the complete internal Flow
record. It includes identity, type/state/version, priority, partition,
timestamps, attempts/run state, maximum active time, and parent/root/correlation
identifiers. It excludes payload/result/error and named-value references,
attributes, state metadata, child bookkeeping, worker/lease/fencing tokens and
owners, parent partition keys, retention controls, and unknown future fields.

Prefixing the query with `EXPLAIN` returns
`ferric.flow.query.point-explain/v1`. The plan is deterministic and redacts
literal and bound parameter values. It reports only the structural point-read
bounds that this operator enforces. The OSS provider does not advertise the broader
`ferric.flow.query/v1` or `ferric.flow.explain/v1` Platform contracts until it
implements tenant scope, workload budgets, cursors, quality evidence, and the
approved collection plans. Enterprise advertises those contracts when its
provider is installed. Query failures use fixed, value-free error codes and
messages. A point-read storage outage returns `query_storage_unavailable` and is
marked retryable and safe to retry; an execution-provider defect returns the
non-retryable `query_engine_failure` error. A decoded primary record that does
not match the bound partition and run ID returns the non-retryable
`query_storage_inconsistent` error without exposing the record.

Trusted native proxies may attach `request_context` with `subject`, `tenant`,
and `scopes`. Trust is configured by
`FERRICSTORE_NATIVE_TRUSTED_REQUEST_CONTEXT_USERS` and frozen when a connection
is accepted, so configuration changes apply to new connections. Subject and
tenant values are limited to 4 KiB. Scopes are limited to 64 entries of at most
1 KiB each, with duplicates removed. Oversized trusted contexts fail before
query-provider dispatch; untrusted connection contexts are ignored. Query text
and parameters are fully redacted from the slow log.

`FLOW.QUERY` is parsed and bound once through the shared prepared-command
contract before authorization or routing. The mandatory partition becomes the
ACL resource and shard-routing key; execution consumes the same prepared AST so
authorization cannot diverge from the query that reaches storage.

## Client management and reroute behavior

Native clients should:

1. Connect and send `HELLO`.
2. If `auth_required` is true, send `AUTH`.
3. Fetch `SHARDS`.
4. Route key/flow-id commands by slot -> shard -> leader endpoint.
5. Refresh route metadata when connection errors, topology changes, or
   `BACKPRESSURE` indicates sustained pressure.

The server keeps the old safety behavior: a request accepted on a follower can
still be redirected internally to the relevant shard leader. Leader-aware SDKs
should treat that as a fallback path, not the primary data path.

## Multiplexing model

Native multiplexing uses bounded server-side lanes:

```text
lane 0:
  control requests and server events

lane N:
  ordered data stream
  one lightweight server lane process per active lane
  bounded queue
```

Recommended lane mapping:

```text
lane_id = shard_id + 1
```

This gives:

```text
same shard/order-sensitive work -> same lane, ordered
different shards                -> different lanes, concurrent
few TCP connections             -> many logical streams
```

Data commands sent on lane `0` are rejected.

## Multi-shard commands and batch policy

`PIPELINE` accepts a list of native command bodies and an explicit atomicity policy:

```text
none       independent command results
per_shard  caller accepts per-shard semantics
same_shard server validates all keys route to one shard
```

Unsupported/global atomicity must be rejected rather than faked.

For peak performance, clients should still split multi-shard independent work
into shard-local lanes. Server-side `PIPELINE` exists for protocol completeness
and ease-of-use, not as the highest-throughput coordinator path.

## Security model

Native protocol uses the same protected-mode and ACL model as the embedded
command path:

```text
protected mode -> rejects non-localhost clients unless a passworded user exists
AUTH           -> supports ACL users and requirepass-compatible default auth
ACL checks     -> command and key checks before dispatch
require_tls    -> plaintext native connections are rejected when enabled
maxclients     -> connection limit across native TCP/TLS listeners
frame caps     -> native_max_frame_bytes (max 134,217,704 body bytes), 128 MiB incomplete buffer,
                  and at most 64 KiB coalesced continuation after a complete first frame
lane caps      -> native_max_lanes_per_connection and native_lane_max_queue
```

Recommended production setup:

```text
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_NATIVE_TLS_PORT=6389
FERRICSTORE_NATIVE_TLS_CERT_FILE=/etc/ferricstore/tls.crt
FERRICSTORE_NATIVE_TLS_KEY_FILE=/etc/ferricstore/tls.key
FERRICSTORE_REQUIRE_TLS=true
FERRICSTORE_NATIVE_ADVERTISE_HOST=ferricstore-0.ferricstore-headless.default.svc.cluster.local
FERRICSTORE_NATIVE_ADVERTISE_TLS_PORT=6389
```

For mTLS, set a CA file:

```text
FERRICSTORE_NATIVE_TLS_CA_CERT_FILE=/etc/ferricstore/ca.crt
```

Do not expose the plaintext native port publicly unless it is behind a trusted
private network or a terminating proxy.

## Multiplexing and pooling

The protocol supports true multiplexing through `lane_id` and `request_id`.

Client SDK best practice:

```text
1. Keep one small control connection for HELLO/SHARDS/BACKPRESSURE.
2. Keep a small data TCP connection pool per advertised node, usually 1-4.
3. Route key/flow-id commands by slot -> shard -> leader endpoint -> lane.
4. Split independent multi-key commands by shard/leader and merge results client-side.
5. Keep lane-local pipelines bounded, for example 32-256 in flight.
6. Use more TCP connections only when one socket/TLS process saturates.
7. Do not pipeline dependent Flow operations that need the previous response.
8. Treat request_id=0 frames as server management events, not command replies.
```

Operator tuning:

```text
FERRICSTORE_NATIVE_MAX_FRAME_BYTES
FERRICSTORE_NATIVE_UNAUTHENTICATED_MAX_FRAME_BYTES
FERRICSTORE_NATIVE_FRAME_ASSEMBLY_TIMEOUT_MS
FERRICSTORE_NATIVE_SEND_TIMEOUT_MS
FERRICSTORE_NATIVE_MAX_VALUE_ITEMS
FERRICSTORE_NATIVE_MAX_VALUE_DEPTH
FERRICSTORE_NATIVE_MAX_LANES_PER_CONNECTION
FERRICSTORE_NATIVE_LANE_MAX_QUEUE
FERRICSTORE_NATIVE_MAX_PIPELINE_COMMANDS
FERRICSTORE_NATIVE_MAX_INFLIGHT_PER_CONNECTION
FERRICSTORE_NATIVE_MAX_INFLIGHT_PER_LANE
FERRICSTORE_NATIVE_MAX_RESPONSE_BYTES
FERRICSTORE_NATIVE_MAX_OUTBOUND_BYTES_PER_CONNECTION
FERRICSTORE_NATIVE_MAX_GLOBAL_EXECUTIONS
FERRICSTORE_NATIVE_MAX_GLOBAL_LANES
FERRICSTORE_NATIVE_MAX_GLOBAL_BLOCKING_REQUESTS
FERRICSTORE_NATIVE_MAX_GLOBAL_INBOUND_BUFFER_BYTES
FERRICSTORE_NATIVE_MAX_GLOBAL_SESSION_BYTES
FERRICSTORE_NATIVE_MAX_GLOBAL_OUTBOUND_BYTES
```

`request_id` is still required because clients may have many independent
requests in flight and need stable response correlation.

## Client-side backpressure

Native clients should treat status `4` as a server overload response. The
payload is an error string for command failures or a map from `BACKPRESSURE`.

Recommended client behavior:

```text
BUSY/OOM/status 4:
  pause producers globally for retry_after_ms or exponential backoff
  keep workers draining existing claimed work
  do not retry immediately in a tight loop

connection close:
  refresh SHARDS
  reconnect with jitter
  replay only naturally idempotent commands or commands protected by Flow fencing/state

auth/noperm:
  do not retry automatically
```

For Flow:

```text
FLOW.CREATE should use stable flow ids.
FLOW.COMPLETE/FAIL/RETRY should keep lease_token and fencing_token from claim_due.
FLOW.CLAIM_DUE should be bounded by worker capacity, not a fixed huge batch.
```

## Elixir SDK

The topology-aware Elixir native client lives in the standalone
[`ferricstore-elixir`](https://github.com/ferricstore/ferricstore-elixir)
repository and is published as the Hex package `ferricstore_sdk`. It bootstraps
from seed nodes, performs `HELLO`/`AUTH`, fetches `SHARDS`, builds the slot
table, opens one connection per advertised endpoint, routes keyed commands to
shard leaders, and refreshes topology once on stale endpoints/reroute responses.

```elixir
{:ok, client} = FerricStore.SDK.start_link(seeds: [{"127.0.0.1", 6388}])

:ok = FerricStore.SDK.set(client, "{tenant:1}:k", "value")
{:ok, "value"} = FerricStore.SDK.get(client, "{tenant:1}:k")

:ok = FerricStore.SDK.mset(client, %{"{a}:1" => "one", "{b}:2" => "two"})
{:ok, ["one", "two"]} = FerricStore.SDK.mget(client, ["{a}:1", "{b}:2"])

FerricStore.SDK.Flow.create(client, %{id: "flow-1", type: "email", state: "queued"})
FerricStore.SDK.Admin.cluster_keyslot(client, %{key: "{a}:1", args: ["{a}:1"]})
```

Advanced callers can use raw opcodes without hard-coded integers:

```elixir
FerricStore.SDK.request_by_key(client, :get, "k", %{key: "k"})
FerricStore.SDK.command_exec(client, "PING", [])
```

## Other SDK surfaces expected later

The Python/TypeScript SDKs should expose native protocol without making users
manage frames directly:

```python
client = FerricStoreClient(
    host="...",
    native=True,
    tls=True,
    username="worker",
    password=os.environ["FERRICSTORE_PASSWORD"],
)

queue = client.queue("email", state="queued", concurrency=500)
workflow = client.workflow("orders")
```

Internally the SDK should own:

```text
HELLO/AUTH handshake
route table refresh
connection pools
request id allocation
bounded in-flight pipelines
backpressure sleep/retry
safe replay rules
```
