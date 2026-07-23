# Getting Started

This guide gets you to a first successful FerricStore command quickly, then points you to the deeper guides.

FerricStore has two common ways to run:

| Mode | Use when |
| --- | --- |
| Docker/server | You want a Ferric native TCP server for Python, Go, Node, Java, Elixir, or SDK clients. |
| Embedded Elixir | You want FerricStore inside an Elixir application with direct `FerricStore.*` calls. |

FerricFlow is available in both modes. It keeps workflow or job state and history in one durable place for queues, state-machine workflows, retries, leases, signals, value refs, observability, and fanout.

> **Beta:** FerricStore is currently a `0.x` beta. It is published and usable,
> but APIs, command details, and operational defaults may change before `1.0`.
> Pin exact versions for pilots and production experiments.

## 1. Run The Server With Docker

```bash
docker run -p 6388:6388 \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -v ferricstore_data:/data \
  ghcr.io/ferricstore/ferricstore:0.10.0
```

`FERRICSTORE_PROTECTED_MODE=false` is for local development only. Use ACL/TLS/protected-mode settings for real deployments.

The official container image is `ghcr.io/ferricstore/ferricstore`. The current
release tag is `0.10.0` and is published for `linux/amd64` and `linux/arm64`.

Smoke test with the Python SDK:

```bash
python - <<PY
from ferricstore import FlowClient
client = FlowClient.from_url("ferric://127.0.0.1:6388")
print(client.ping())
client.close()
PY
```

## 2. Create Your First Flow

A Flow is one durable workflow record with a `type`, `id`, `state`, payload/value refs, lease, history, and terminal status. Flow commands return success only after the state change is accepted through the quorum path and written to disk.

```text
FLOW.CREATE email-1 TYPE email STATE queued PAYLOAD "welcome:user-1"
FLOW.CLAIM_DUE email STATE queued WORKER worker-1 LIMIT 1
FLOW.COMPLETE email-1 <lease-token> FENCING <fencing-token> RESULT "sent"
```

For a state machine, return a transition instead of completing immediately:

```text
FLOW.CREATE order-1 TYPE order STATE created PAYLOAD "order payload"
FLOW.CLAIM_DUE order STATE created WORKER worker-1 LIMIT 1
FLOW.TRANSITION order-1 running charged LEASE_TOKEN <lease-token> FENCING <fencing-token>
```

## 3. Use The Python SDK

```bash
pip install ferricstore
```

Queue worker:

```python
from ferricstore import QueueClient

client = QueueClient.from_url("ferric://127.0.0.1:6388")
emails = client.queue(type="email")

emails.enqueue("email-1", payload=b"welcome:user-1", idempotent=True)


def send_email(job):
    print(job.id, job.payload)
    return b"sent"


emails.worker(concurrency=10, batch_size=100).run(send_email)
```

Workflow worker:

```python
from ferricstore import WorkflowClient, complete, transition

client = WorkflowClient.from_url("ferric://127.0.0.1:6388")
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

The SDK handles claim leases and fencing. Your handler returns a durable outcome such as `transition(...)`, `complete(...)`, `retry(...)`, or `fail(...)`.

## 4. Use Embedded Elixir

Add the dependency:

```elixir
# mix.exs
def deps do
  [
    {:ferricstore, "~> 0.10.0"}
  ]
end
```

Start an IEx shell and verify the embedded API:

```bash
iex -S mix
```

```elixir
:ok = FerricStore.set("hello", "world")
{:ok, "world"} = FerricStore.get("hello")
```

FerricStore starts with the OTP application. If your application uses a custom embedded instance, see [Embedded Mode](embedded-mode.md).

## 5. Build From Source

Source builds require Elixir, Erlang/OTP, and Rust.

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
_build/prod/rel/ferricstore/bin/ferricstore start
```

## 6. Next Steps

- [Key-Value Store](kv-store.md) -- how the durable KV/data-structure store works.
- [Workflow usage examples](../docs/flow-vs-temporal-usage.md) -- queues, state machines, signals, retries, fanout, and value refs.
- [Commands Reference](commands.md) -- command syntax and native protocol mapping.
- [Embedded Mode](embedded-mode.md) -- direct Elixir API.
- [Configuration](configuration.md) -- production defaults and runtime configuration.
- [Deployment](deployment.md) -- Docker, releases, Kubernetes, and clustering.
- [Security](security.md) -- ACL, TLS, protected mode.
- [Benchmarks](../docs/benchmarks.md) -- latest Azure Flow and KV benchmark results.
