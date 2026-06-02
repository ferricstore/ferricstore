# Standalone Mode

Standalone mode runs FerricStore as a full Redis-compatible server. It starts a TCP listener (and optionally a TLS listener) that speaks the RESP3 protocol, an HTTP health endpoint, Prometheus metrics, and all the core engine components. Connect with `redis-cli`, Redix, or any Redis client in any language.

## When to Use Standalone Mode

- You need external clients to connect (redis-cli, applications in other languages)
- You want to run FerricStore as a separate service
- You need TLS encryption for client connections
- You want HTTP health endpoints for Kubernetes
- You want Prometheus metrics scraping

## Setup

### From Source

```bash
git clone https://github.com/YoavGivati/ferricstore.git
cd ferricstore
mix deps.get
mix run --no-halt
```

FerricStore starts on port 6379 by default.

### As a Dependency

If you want standalone mode inside an umbrella or host application, add both packages:

```elixir
# mix.exs
def deps do
  [
    {:ferricstore, "~> 0.3.5"},
    {:ferricstore_server, "~> 0.3.5"}
  ]
end
```

### Configuration

```elixir
# config/config.exs
config :ferricstore, :port, 6379
config :ferricstore, :data_dir, "/var/lib/ferricstore"
config :ferricstore, :shard_count, 0  # 0 = auto-detect from CPU cores
config :ferricstore, :max_memory_bytes, 8_589_934_592  # 8 GB
config :ferricstore, :eviction_policy, :allkeys_lru
```

The server's `runtime.exs` also sets `protected_mode: true`, which prevents destructive operations (like `FLUSHALL`) from being executed without authentication. This defaults to `false` in the library and is only enabled by the server:

```elixir
# config/runtime.exs (in ferricstore_server)
config :ferricstore, :protected_mode, true
```

## Connecting

### redis-cli

```bash
redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET greeting "hello world"
OK
127.0.0.1:6379> GET greeting
"hello world"
```

### Redix (Elixir)

```elixir
{:ok, conn} = Redix.start_link(host: "localhost", port: 6379)
{:ok, "OK"} = Redix.command(conn, ["SET", "key", "value"])
{:ok, "value"} = Redix.command(conn, ["GET", "key"])
```

### Any Redis Client

FerricStore speaks RESP3, so any Redis client library works:

```python
# Python (redis-py)
import redis
r = redis.Redis(host='localhost', port=6379)
r.set('key', 'value')
r.get('key')  # b'value'
```

```javascript
// Node.js (ioredis)
const Redis = require('ioredis');
const redis = new Redis(6379);
await redis.set('key', 'value');
await redis.get('key');  // 'value'
```

```go
// Go (go-redis)
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
rdb.Set(ctx, "key", "value", 0)
rdb.Get(ctx, "key")  // "value"
```

## TLS Configuration

FerricStore supports encrypted connections via a separate TLS listener.

### Generate Certificates (Development)

```bash
# Self-signed CA + server cert for local testing
openssl req -x509 -newkey rsa:4096 -days 365 -nodes \
  -keyout ca-key.pem -out ca-cert.pem \
  -subj "/CN=FerricStore CA"

openssl req -newkey rsa:4096 -nodes \
  -keyout server-key.pem -out server-req.pem \
  -subj "/CN=localhost"

openssl x509 -req -in server-req.pem -days 365 \
  -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
  -out server-cert.pem
```

### Configure TLS

```elixir
# config/config.exs (or runtime.exs)
config :ferricstore, :tls_port, 6380
config :ferricstore, :tls_cert_file, "/etc/ssl/ferricstore/server-cert.pem"
config :ferricstore, :tls_key_file, "/etc/ssl/ferricstore/server-key.pem"

# Optional: CA cert for mutual TLS (client certificate authentication)
config :ferricstore, :tls_ca_cert_file, "/etc/ssl/ferricstore/ca-cert.pem"

# Optional: reject plaintext connections entirely
config :ferricstore, :require_tls, true
```

### Connect via TLS

```bash
redis-cli -p 6380 --tls \
  --cert /path/to/client-cert.pem \
  --key /path/to/client-key.pem \
  --cacert /path/to/ca-cert.pem
```

```elixir
# Redix with TLS
{:ok, conn} = Redix.start_link(
  host: "localhost",
  port: 6380,
  ssl: true,
  socket_opts: [
    certfile: "/path/to/client-cert.pem",
    keyfile: "/path/to/client-key.pem",
    cacertfile: "/path/to/ca-cert.pem"
  ]
)
```

## Health Endpoints

FerricStore exposes an HTTP health endpoint (default port 4000) with:

### Readiness Probe

```
GET /health/ready
```

Returns `200 OK` with a JSON body when the node is ready:

```json
{
  "status": "ok",
  "shard_count": 4,
  "shards": [
    {"index": 0, "status": "ok", "keys": 1234},
    {"index": 1, "status": "ok", "keys": 1189},
    {"index": 2, "status": "ok", "keys": 1201},
    {"index": 3, "status": "ok", "keys": 1156}
  ],
  "uptime_seconds": 3600
}
```

Returns `503 Service Unavailable` during startup.

### Kubernetes Configuration

```yaml
readinessProbe:
  httpGet:
    path: /health/ready
    port: 4000
  initialDelaySeconds: 2
  periodSeconds: 5

livenessProbe:
  httpGet:
    path: /health/live
    port: 4000
  initialDelaySeconds: 10
  periodSeconds: 10
```

### HTML Dashboard

A human-friendly health dashboard is available at:

```
GET /dashboard
```

The dashboard has sidebar navigation with the following pages:

- **Overview** (`/dashboard`) -- top bar, cache perf, shards, memory, connections. Auto-refreshes every 2s.
- **Slow Log** (`/dashboard/slowlog`) -- full slow log table. Refreshes 5s.
- **Merge Status** (`/dashboard/merge`) -- per-shard merge/compaction. Refreshes 10s.
- **Storage** (`/dashboard/storage`) -- per-shard on-disk storage details.
- **Raft Consensus** (`/dashboard/raft`) -- per-shard Raft health, leader, term, applied/commit index. Refreshes 5s.
- **Config** (`/dashboard/config`) -- namespace config overrides.
- **Doctor** (`/dashboard/doctor`) -- bounded storage/projection diagnostics and safe background repair jobs.
- **Clients** (`/dashboard/clients`) -- active client connections with IP, age, idle time. Refreshes 5s.
- **Key Prefixes** (`/dashboard/prefixes`) -- key prefix distribution.

## Prometheus Metrics

FerricStore exposes Prometheus-compatible metrics with no external dependencies. The metrics text is available via:

### Redis Command

```bash
redis-cli FERRICSTORE.METRICS
```

### HTTP Endpoint

The health HTTP endpoint also serves metrics at `/metrics`.

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `ferricstore_connected_clients` | gauge | Current number of TCP connections |
| `ferricstore_total_connections_received` | counter | Total connections since startup |
| `ferricstore_total_commands_processed` | counter | Total commands executed |
| `ferricstore_hot_reads_total` | counter | Reads served from ETS (hot) |
| `ferricstore_cold_reads_total` | counter | Reads served from Bitcask (cold) |
| `ferricstore_used_memory_bytes` | gauge | Total BEAM memory usage |
| `ferricstore_keydir_used_bytes` | gauge | ETS keydir memory |
| `ferricstore_uptime_seconds` | gauge | Seconds since server start |
| `ferricstore_blocked_clients` | gauge | Clients blocked on BLPOP etc. |
| `ferricstore_tracking_clients` | gauge | Clients with CLIENT TRACKING enabled |
| `ferricstore_slowlog_entries` | gauge | Current slowlog size |

### Prometheus Scrape Config

```yaml
scrape_configs:
  - job_name: ferricstore
    static_configs:
      - targets: ['localhost:4000']
    metrics_path: /metrics
```

## Connection Handling

Each TCP connection is handled by a Ranch protocol handler with:

- **RESP3 parser** -- full RESP3 protocol support (HELLO 3 only)
- **ACL check** -- every command is checked against the authenticated user's permissions
- **Command dispatch** -- O(1) lookup via compile-time dispatch map
- **Configurable socket mode** -- `socket_active_mode` (default: `true`). Supports `true`, `:once`, or an integer N for `active: N` flow control.
- **Sliding window pipeline** -- concurrent dispatch of pure commands within a pipeline batch; responses sent in-order as they complete
- **Socket tuning** -- `nodelay: true`, `recbuf: 65536`, `sndbuf: 65536`, `backlog: 1024`, `keepalive: true`

### Sendfile Zero-Copy

For large cold values (>64 KB by default), FerricStore uses `:file.sendfile/5` to transfer data directly from disk to the TCP socket without copying through BEAM memory. This only applies to cold keys over plain TCP -- hot keys and TLS connections use the normal path.

```elixir
config :ferricstore_server, :sendfile_threshold, 65_536
```

## Server Commands

### INFO

Returns server statistics in Redis INFO format:

```bash
redis-cli INFO
redis-cli INFO server
redis-cli INFO memory
redis-cli INFO stats
redis-cli INFO keyspace
```

### CONFIG

```bash
redis-cli CONFIG GET maxmemory
redis-cli CONFIG GET "max*"
redis-cli CONFIG SET maxmemory-policy allkeys-lru
```

### SLOWLOG

```bash
redis-cli SLOWLOG GET 10    # Last 10 slow commands
redis-cli SLOWLOG LEN       # Number of entries
redis-cli SLOWLOG RESET     # Clear the log
```

### CLIENT

```bash
redis-cli CLIENT LIST        # List all connections
redis-cli CLIENT SETNAME myapp
redis-cli CLIENT GETNAME
redis-cli CLIENT ID
redis-cli CLIENT INFO
```

### COMMAND

```bash
redis-cli COMMAND             # List all supported commands
redis-cli COMMAND COUNT       # Number of supported commands
redis-cli COMMAND INFO GET    # Info about specific command
```

## Transactions (MULTI/EXEC)

FerricStore supports Redis-compatible optimistic transactions:

```bash
redis-cli WATCH mykey
redis-cli MULTI
redis-cli SET mykey "new_value"
redis-cli INCR counter
redis-cli EXEC
# Returns array of results, or nil if WATCH detected a conflict
```

## Pub/Sub

```bash
# Terminal 1: Subscribe
redis-cli SUBSCRIBE mychannel

# Terminal 2: Publish
redis-cli PUBLISH mychannel "hello world"
```

Pattern subscriptions are also supported:

```bash
redis-cli PSUBSCRIBE "user:*"
```

## FerricStore-Specific Commands

### Cluster Health

```bash
redis-cli CLUSTER.HEALTH     # Node health + shard status
redis-cli CLUSTER.STATS      # Cluster-wide statistics
```

### Hot/Cold Ratio

```bash
redis-cli FERRICSTORE.HOTNESS   # Hot vs cold read ratio per key
```

### Namespace Configuration

```bash
# Set session namespace to a 5ms commit window
redis-cli FERRICSTORE.CONFIG SET session window_ms 5

# Reset a namespace to defaults
redis-cli FERRICSTORE.CONFIG RESET session
```

### Doctor Diagnostics

```bash
redis-cli FERRICSTORE.DOCTOR CHECK
redis-cli FERRICSTORE.DOCTOR CHECK SCOPE FLOW_LMDB
redis-cli FERRICSTORE.DOCTOR START CHECK SCOPE BITCASK
redis-cli FERRICSTORE.DOCTOR START REPAIR PROJECTIONS SCOPE FLOW_LMDB
redis-cli FERRICSTORE.DOCTOR LIST
```

`FERRICSTORE.DOCTOR` is the Redis command behind the dashboard Doctor page. It
checks Bitcask/keydir metadata, large-value blob refs, and the Flow LMDB cold
projection. Inline `CHECK` is bounded; repair runs as a background job and is
currently limited to rebuilding the Flow LMDB projection from durable Flow
records.

### Key Diagnostics

```bash
redis-cli FERRICSTORE.KEY_INFO mykey   # Shard, ETS entry, disk location, TTL
```

## Graceful Shutdown

FerricStore handles graceful shutdown:

1. Marks node as not ready (Kubernetes stops routing traffic)
2. Emits `[:ferricstore, :node, :shutdown_started]` telemetry
3. Flushes pending writes to disk
4. Closes TCP connections
5. Stops Raft groups
6. Supervisor stops all children in reverse order
