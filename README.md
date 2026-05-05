# FerricStore

[![Hex.pm](https://img.shields.io/hexpm/v/ferricstore.svg)](https://hex.pm/packages/ferricstore)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/ferricstore)
[![CI](https://github.com/yoavgeva/ferricstore/actions/workflows/test.yml/badge.svg)](https://github.com/yoavgeva/ferricstore/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**The Redis-compatible store where every write is durable by default.**

FerricStore is a distributed, crash-safe key-value store with Redis wire protocol (RESP3). Every write goes through Raft consensus and is fsync'd to disk before the client gets OK. No "enable persistence" checkbox. No "hope the AOF rewrite finishes before the crash." Your data survives kill -9, node failures, and power loss — automatically.

## Why FerricStore?

Redis, Dragonfly, and Memcached treat persistence as optional. By default, your data lives only in RAM. Enable AOF? You still lose the last second. Enable RDB snapshots? You lose minutes. Even with replication, failover is manual or requires Sentinel — a separate system to babysit your cache.

You shouldn't have to choose between speed and safety.

Every write in FerricStore is:

| Property | How |
|---|---|
| **Atomic** | Each command is a single Raft log entry — applied or not, never partial |
| **Consistent** | Raft linearizability — every read sees the latest committed write |
| **Isolated** | Single-threaded state machine per shard — commands never interleave |
| **Durable** | WAL + fdatasync + Raft quorum — majority of nodes must persist before ack |

FerricStore has one write contract today: quorum durability. Every write goes
through Raft and disk-backed storage before success is reported.

## Beyond Cachex and Nebulex

If you're using Cachex or Nebulex today, FerricStore gives you:

- **Crash survival** — your cache survives restarts, deploys, and kill -9. ETS doesn't.
- **Disk-backed eviction** — when RAM fills up, evicted keys are read from disk instead of hitting your database again. Less load on your database, fewer cache stampedes.
- **No cold start** — restart your app, all data is already on disk. No thundering herd while the cache warms up.
- **Redis protocol** — non-Elixir services can share the same store. Use any Redis client library.
- **Data structures beyond key-value** — Hash, List, Set, SortedSet, Stream, Geo, JSON, Bitmap, HyperLogLog, Bloom, Cuckoo, Count-Min Sketch, TopK, TDigest.
- **Automatic failover** — Raft consensus with leader election. No Sentinel, no manual intervention.
- **Quorum-first writes** — every write uses Raft consensus and disk-backed durability.

## Quick Start

### Embedded (inside your Elixir app)

```elixir
# mix.exs
{:ferricstore, "~> 0.3.5"}
```

```elixir
# Durable by default — this write survives kill -9
:ok = FerricStore.set("user:42:name", "alice", ttl: :timer.hours(1))
{:ok, "alice"} = FerricStore.get("user:42:name")

# Atomic compare-and-swap
{:ok, true} = FerricStore.cas("counter", "1", "2")

# Distributed lock
{:ok, true} = FerricStore.lock("resource:42", "owner-id", ttl: 30_000)
```

No sidecar. No Docker. No ops. Your Phoenix app starts, FerricStore starts with it.

### Standalone (Redis-compatible server)

```bash
docker run -p 6379:6379 -e FERRICSTORE_PROTECTED_MODE=false -v ferricstore_data:/data yoavgeva/ferricstore

# Connect with any Redis client
redis-cli -p 6379
127.0.0.1:6379> SET user:42:name alice
OK
```

Drop-in Redis replacement. 250+ commands. Use your existing Redis client libraries.

## Guides

- [Getting Started](guides/getting-started.md) — installation, configuration, first commands
- [Best Practices](guides/best-practices.md) — hash tags, pipelining, key design
- [Architecture](guides/architecture.md) — write path, read path, three-tier storage, Raft consensus
- [Commands Reference](guides/commands.md) — all 250+ commands with syntax and compatibility notes
- [Embedded Mode](guides/embedded-mode.md) — using FerricStore inside your Elixir app
- [Standalone Mode](guides/standalone-mode.md) — Redis-compatible server, dashboard, Prometheus
- [Configuration](guides/configuration.md) — all config options
- [Deployment](guides/deployment.md) — Docker, Kubernetes, bare metal, clustering
- [Security](guides/security.md) — ACL, TLS, protected mode
- [Extensions](guides/extensions.md) — Plug sessions, Ecto L2 cache
- [Comparison](guides/comparison.md) — vs Redis, Valkey, Dragonfly, Garnet, Kvrocks, Cachex, Nebulex

## Requirements

- Elixir >= 1.19
- Erlang/OTP 28+
- Rust toolchain (for NIF compilation, or use precompiled binaries)

## License

MIT
