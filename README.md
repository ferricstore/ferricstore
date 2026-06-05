# FerricStore

[![Hex.pm](https://img.shields.io/hexpm/v/ferricstore.svg)](https://hex.pm/packages/ferricstore)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/ferricstore)
[![CI](https://github.com/ferricstore/ferricstore/actions/workflows/test.yml/badge.svg)](https://github.com/ferricstore/ferricstore/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**FerricStore is a Redis-compatible durable server with FerricFlow durable execution built in.**

FerricStore gives applications a durable key-value/data-structure store and a workflow layer for queues, explicit state machines, retries, leases, history, value refs, signals, and fanout.

FerricFlow is the durable execution layer inside FerricStore. It stores workflow state as server-owned records instead of asking application code to rebuild leases, due indexes, retry state, history, and terminal records around a generic queue.

## Beta Status

FerricStore is currently a `0.x` beta release. The core durability path, Flow
commands, precompiled NIFs, Docker image, and SDKs are published and usable, but
public APIs, command details, operational defaults, and storage/projection
internals may still change before `1.0`.

Use it today for development, benchmarks, pilots, and controlled production
experiments. For critical production workloads, pin exact versions, test
upgrades on your data model, and expect compatibility guarantees to harden with
the `1.0` release line.

## What Is A Flow?

A Flow is one durable execution record:

| Field | Meaning |
| --- | --- |
| `type` | Workflow or queue type, such as `email` or `order`. |
| `id` | Application-defined Flow id. |
| `state` | Current durable state, such as `queued`, `created`, or `charged`. |
| `payload` / value refs | Small routing payload plus optional named values stored separately. |
| lease | Worker claim ownership with fencing. |
| history | State changes, signals, retries, and terminal events. |
| terminal status | Completed, failed, cancelled, or still active. |

The core loop is explicit:

```text
FLOW.CREATE -> FLOW.CLAIM_DUE -> handler -> FLOW.TRANSITION / COMPLETE / FAIL / RETRY
```

Queue workers usually process one state and complete/fail/retry. Workflow workers process multiple named states and return explicit transitions.

## Run Locally

```bash
docker run -p 6379:6379 \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -v ferricstore_data:/data \
  ghcr.io/ferricstore/ferricstore:0.4.1
```

`FERRICSTORE_PROTECTED_MODE=false` is for local development only. Use ACL/TLS/protected-mode settings for real deployments.

The published container image is hosted on GitHub Container Registry:

```bash
docker pull ghcr.io/ferricstore/ferricstore:0.4.1
```

Current release images are published for `linux/amd64`. Native `linux/arm64`
container images will be added in a later release; Apple Silicon can still run
the amd64 image through Docker emulation for local development.

## First Flow Over RESP

FerricFlow commands are available over the Redis-compatible protocol, so normal RESP clients can use pipelines, ACLs, TLS, and existing connection pools.

Durable queue item:

```text
FLOW.CREATE email-1 TYPE email STATE queued PAYLOAD "welcome:user-1"
FLOW.CLAIM_DUE email STATE queued WORKER worker-1 LIMIT 100
FLOW.COMPLETE email-1 <lease-token> FENCING <fencing-token> RESULT "sent"
```

Explicit state transition:

```text
FLOW.CREATE order-1 TYPE order STATE created PAYLOAD "order payload"
FLOW.CLAIM_DUE order STATE created WORKER worker-1 LIMIT 1
FLOW.TRANSITION order-1 running charged LEASE_TOKEN <lease-token> FENCING <fencing-token>
FLOW.CLAIM_DUE order STATE charged WORKER worker-1 LIMIT 1
FLOW.COMPLETE order-1 <lease-token> FENCING <fencing-token> RESULT "ok"
```

Because this is RESP, Flow commands and normal Redis-compatible commands can be pipelined on the same connection.

## Python SDK

Install:

```bash
pip install ferricstore
```

Durable queue:

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

Explicit state-machine workflow:

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

The SDK handles claim leases and fencing. Handlers return durable outcomes such as `transition(...)`, `complete(...)`, `retry(...)`, or `fail(...)`.

Python SDK links:

- Package: <https://pypi.org/project/ferricstore/>
- Repository: <https://github.com/ferricstore/ferricstore-python>

## Core FerricFlow Primitives

### Signals

Signals record external events durably and can optionally move a Flow to another state.

```python
from ferricstore import WorkflowClient

client = WorkflowClient.from_url("redis://127.0.0.1:6379/0")
approval = client.workflow(type="approval", initial_state="waiting")
approval.start("approval-1", payload=b"invoice:123", idempotent=True)

approval.signal(
    "approval-1",
    signal="approved",
    if_state="waiting",
    transition_to="approved",
    idempotency_key="approve-approval-1",
)
```

### Value Refs

Named values let a Flow store large or optional bytes separately from hot state. Workers hydrate only the values they ask for.

```python
from ferricstore import QueueClient

client = QueueClient.from_url("redis://127.0.0.1:6379/0")
orders = client.queue(type="order")

orders.enqueue(
    "order-1",
    payload=b"small routing bytes",
    values={"invoice": invoice_pdf_bytes, "customer": customer_snapshot_bytes},
)

orders.worker(claim_values=["customer"]).run(handle_customer_step)
```

### Fanout

A parent Flow can spawn child Flows. Children run independently with their own state, retries, leases, history, and terminal status; parent/child links are queryable later.

```python
from ferricstore import ChildSpec, WorkflowClient, transition

client = WorkflowClient.from_url("redis://127.0.0.1:6379/0")
campaign = client.workflow(type="campaign", initial_state="dispatch")


@campaign.state("dispatch")
def dispatch(job):
    job.flow.spawn_children(
        [
            ChildSpec(
                id=f"device:{device_id}:cmd:{job.id}",
                type="device-command",
                payload=device_id.encode(),
            )
            for device_id in device_ids
        ],
        wait_state="done",
    )
    return transition("waiting_for_children")
```

## Failure Model

- Flow state is durable before `FLOW.CREATE`, transition, retry, complete, fail, or cancel returns success.
- `FLOW.CLAIM_DUE` grants a lease token and fencing token to a worker.
- Terminal or transition commands must present the current lease/fencing data, so stale workers cannot overwrite newer claims.
- If a worker crashes after claiming, the Flow becomes claimable again after the lease expires or is reclaimed.
- Handlers are normal application code. FerricFlow does not replay handler code to recover state.
- History and cold query projections may lag briefly, but current Flow state is the source of truth.

## Durable Store Underneath

FerricStore also exposes a Redis-compatible durable key-value/data-structure store:

```text
SET user:42:name alice
GET user:42:name
HSET order:1 status paid
ZADD due 1700000000000 flow-1
```

Writes go through Raft consensus and disk-backed storage before success is reported. There is no separate mode to turn persistence on.

| Property | How |
| --- | --- |
| Atomic | Each command is one Raft log entry, applied or not. |
| Consistent | Raft linearizability for committed writes. |
| Isolated | Single-threaded state machine per shard. |
| Durable | WAL, disk-backed storage, and Raft quorum before ack. |

## Embedded Elixir

FerricStore can also run inside an Elixir application.

```elixir
# mix.exs
{:ferricstore, "~> 0.4.1"}
```

```elixir
:ok = FerricStore.set("user:42:name", "alice", ttl: :timer.hours(1))
{:ok, "alice"} = FerricStore.get("user:42:name")
```

FerricFlow is also available through embedded `FerricStore.flow_*` functions and the high-level Elixir Flow SDK.

## Documentation

Start here:

- [Getting Started](guides/getting-started.md) — installation, configuration, first commands.
- [Workflow usage examples](docs/flow-vs-temporal-usage.md) — queues, workflows, retries, fanout, signals, and value refs.
- [Benchmarks](docs/benchmarks.md) — latest Azure FerricFlow and KV SET/GET results.

FerricFlow:

- [Flow command reference](guides/commands.md) — `FLOW.*` command syntax alongside Redis-compatible commands.
- [Flow retry policy](docs/flow-retry-policy.md) — type/state retry policies and retry exhaustion behavior.
- [Flow production readiness](docs/flow-production-readiness.md) — operational model, lagged projections, retention, reclaim, and production tuning.
- [Elixir Flow SDK](guides/flow-elixir-sdk.md) — high-level embedded workflow/state-machine API over core Flow commands.

Operations and reference:

- [Architecture](guides/architecture.md) — write path, read path, storage, Raft consensus.
- [Commands Reference](guides/commands.md) — Redis-compatible and FerricFlow command syntax.
- [Configuration](guides/configuration.md) — server config and production defaults.
- [Deployment](guides/deployment.md) — Docker, Kubernetes, bare metal, clustering.
- [Security](guides/security.md) — ACL, TLS, protected mode.
- [Best Practices](guides/best-practices.md) — pipelining, key design, partitioning.

## Development

Source builds require:

- Elixir >= 1.19
- Erlang/OTP 28+
- Rust stable toolchain

```bash
mix deps.get
mix compile
mix test
```

Run from source:

```bash
MIX_ENV=prod FERRICSTORE_DATA_DIR=/tmp/ferricstore mix run --no-halt
```

Build a release:

```bash
MIX_ENV=prod mix release ferricstore
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). For security issues, see [SECURITY.md](SECURITY.md).

## License

Apache-2.0
