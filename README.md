# FerricStore

[![Hex.pm](https://img.shields.io/hexpm/v/ferricstore.svg)](https://hex.pm/packages/ferricstore)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/ferricstore)
[![CI](https://github.com/ferricstore/ferricstore/actions/workflows/test.yml/badge.svg)](https://github.com/ferricstore/ferricstore/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**FerricFlow durable execution, built on a Redis-compatible durable store.**

FerricStore is the server behind FerricFlow: durable queues, explicit state
machines, workflow history, value refs, fanout, retries, leases, and terminal
retention. It speaks the Redis wire protocol, so applications can use normal
Redis clients while getting a purpose-built durable workflow layer.

The core model is intentionally explicit:

```text
FLOW.CREATE -> FLOW.CLAIM_DUE -> handler -> FLOW.TRANSITION / COMPLETE / FAIL / RETRY
```

FerricFlow uses explicit durable states and worker outcomes. State, leases,
history, payload refs, and transitions are stored as durable server state.

## Why FerricFlow?

Most durable execution systems make you choose between two extremes:

- a workflow runtime when your application wants code-driven orchestration
- a queue plus app code, where you rebuild leases, retries, state, history, and fanout yourself

FerricFlow sits in the middle: server-side durable workflow primitives that are
simple enough to use through Redis protocol, but strong enough for production
work queues and state machines.

| Capability | What FerricFlow provides |
| --- | --- |
| Durable create | Flow state is written through Raft and disk-backed storage before success. |
| Claim leases | Workers claim due work with leases and fencing. |
| Explicit state machine | Transitions move a flow from one state to the next. |
| Terminal commands | Complete, fail, cancel, retry, rewind, and reclaim are first-class. |
| History | State changes are recorded for audit/debugging. |
| Value refs | Large or optional values can be stored separately and hydrated only when needed. |
| Fanout | Parent flows can spawn many child flows without making users build routing logic. |
| Backpressure | Server overload returns clean rejections instead of silently eating memory. |

## What makes it different?

FerricFlow stores Flow state, due indexes, leases, history, value refs, and
retention as part of the server contract.

That gives you:

- **one durable truth** — Raft log plus Bitcask Flow records
- **fast hot indexes** — native ordered indexes for due/running/state lookups
- **payload-safe workflows** — payload/value bytes are separate from hot Flow metadata
- **Redis-compatible access** — use RESP clients, pipelines, ACLs, and familiar deployment patterns
- **explicit workflow state** — handlers return durable outcomes such as transition, retry, complete, or fail

## Quick start with Python SDK

Install the Python SDK:

```bash
pip install ferricstore
```

Run FerricStore locally:

```bash
docker run -p 6379:6379 \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -v ferricstore_data:/data \
  ferricstore/ferricstore
```

Create a durable queue item and process it:

```python
from ferricstore import QueueClient

client = QueueClient.from_url("redis://127.0.0.1:6379/0")
emails = client.queue(type="email")

emails.enqueue("email-1", payload=b"welcome:user-1", idempotent=True)


def send_email(job):
    print(job.id, job.payload)
    return b"sent"


emails.worker(concurrency=10, batch_size=100).run(send_email)
```

Create an explicit workflow/state machine:

```python
from ferricstore import WorkflowClient, complete, transition

client = WorkflowClient.from_url("redis://127.0.0.1:6379/0")
order = client.workflow(type="order", initial_state="created")


@order.state("created")
def created(job):
    charge_card(job.payload)
    return transition("charged")


@order.state("charged")
def charged(job):
    send_receipt(job.id)
    return complete(result=b"ok")


order.start("order-1", payload=b"order payload", idempotent=True)
order.worker(states=["created", "charged"]).run()
```

Python SDK:

- Package: <https://pypi.org/project/ferricstore/>
- Repository: <https://github.com/ferricstore/ferricstore-python>

## Quick start with RESP commands

FerricFlow commands are available over the Redis-compatible protocol.

```text
FLOW.CREATE email email-1 STATE queued PAYLOAD "welcome:user-1"
FLOW.CLAIM_DUE email STATE queued WORKER worker-1 LIMIT 100
FLOW.COMPLETE email-1 WORKER worker-1 LEASE <lease-token> RESULT "sent"
```

Because this is RESP, clients can pipeline normal Redis commands and Flow
commands on the same connection.

## Durable store underneath

FerricStore still includes a Redis-compatible durable key-value/data-structure
store. Normal commands are durable by default:

```text
SET user:42:name alice
GET user:42:name
HSET order:1 status paid
ZADD due 1700000000000 flow-1
```

Every write goes through Raft consensus and disk-backed storage before success is
reported. There is no separate “turn persistence on” mode.

| Property | How |
| --- | --- |
| Atomic | Each command is a single Raft log entry, applied or not. |
| Consistent | Raft linearizability for committed writes. |
| Isolated | Single-threaded state machine per shard. |
| Durable | WAL, disk-backed storage, and Raft quorum before ack. |

## Embedded Elixir

FerricStore can also run inside an Elixir application.

```elixir
# mix.exs
{:ferricstore, "~> 0.3.7"}
```

```elixir
:ok = FerricStore.set("user:42:name", "alice", ttl: :timer.hours(1))
{:ok, "alice"} = FerricStore.get("user:42:name")
```

## Use cases

- durable work queues
- explicit state-machine workflows
- queued workflow execution
- AI orchestration and tool execution
- IoT fanout and command tracking
- saga steps with retry/fail/compensation state
- human approval workflows
- durable cache/data-structure storage

## Flow docs

- [Flow production readiness](docs/flow-production-readiness.md) — operational model, lagged projections, retention, reclaim, and production tuning.
- [Flow retry policy](docs/flow-retry-policy.md) — type/state retry policies and retry exhaustion behavior.
- [Elixir Flow SDK](guides/flow-elixir-sdk.md) — high-level embedded workflow/state-machine API over core Flow commands.
- [Workflow usage examples](docs/flow-vs-temporal-usage.md) — code-shape examples for queues, workflows, retries, fanout, signals, and value refs.
- [Flow command reference](guides/commands.md) — `FLOW.*` command syntax alongside Redis-compatible commands.
- [Benchmarks](docs/benchmarks.md) — latest Azure FerricFlow and KV SET/GET results.

## Guides

- [Getting Started](guides/getting-started.md) — installation, configuration, first commands
- [Architecture](guides/architecture.md) — write path, read path, storage, Raft consensus
- [Commands Reference](guides/commands.md) — Redis-compatible and FerricFlow command syntax
- [Configuration](guides/configuration.md) — server config and production defaults
- [Deployment](guides/deployment.md) — Docker, Kubernetes, bare metal, clustering
- [Security](guides/security.md) — ACL, TLS, protected mode
- [Best Practices](guides/best-practices.md) — pipelining, key design, partitioning
- [Benchmarks](docs/benchmarks.md) — latest Azure FerricFlow and KV SET/GET results

## Requirements

- Elixir >= 1.19
- Erlang/OTP 28+
- Rust toolchain for NIF compilation, or precompiled binaries when available

## License

Apache-2.0
