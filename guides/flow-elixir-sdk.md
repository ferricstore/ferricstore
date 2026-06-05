# Elixir Flow SDK

FerricStore Flow already exposes low-level embedded commands such as
`FerricStore.flow_create/2`, `FerricStore.flow_claim_due/2`, and
`FerricStore.flow_complete/3`.

The Elixir Flow SDK is the high-level embedded API for durable queues and
state-machine workflows. It builds on `FerricStore.flow_*` commands. It does not
add a replay engine, does not use RESP, and does not change Flow durability or
atomicity: successful Flow writes are accepted through the quorum path and
written to disk.


## First Workflow

Start with one state that completes or retries:

```elixir
defmodule EmailFlow do
  use FerricStore.Flow.Workflow,
    type: "email",
    initial_state: :queued

  state :queued do
    lease_ms 30_000
    on_ok complete()
    on_error retry_or: :failed
  end

  state :failed do
    on_ok complete()
  end
end
```

Create and run one unit of work:

```elixir
{:ok, _flow} =
  EmailFlow.create(%{
    id: "email-1",
    payload: "welcome:user-1"
  })

EmailFlow.run_once(:queued, worker: "worker-1", handler: fn job ->
  send_email(job.payload)
  {:ok, "sent"}
end)

{:ok, history} = EmailFlow.history("email-1")
```

The SDK handles command construction. Flow core still handles leases, fencing, durable state, and history.

## How It Works Internally

```text
Workflow SDK
  -> embedded FerricStore.flow_* functions
  -> Flow core
  -> Ra + Bitcask durable records
  -> hot Flow indexes
  -> async LMDB cold projections
```

Truth stays in Flow core. SDK modules are command builders and worker helpers.

## Define A Workflow

```elixir
defmodule BillingFlow do
  use FerricStore.Flow.Workflow,
    type: "billing",
    partition_by: [:tenant_id, :invoice_id],
    initial_state: :created

  state :created do
    lease_ms 60_000
    claim_payload true, max_bytes: 64_000

    retry max_retries: 8,
          backoff: [kind: :exponential, base_ms: 1_000, max_ms: :timer.hours(1)]

    on_ok :charged
    on_error retry_or: :failed
  end

  state :charged do
    lease_ms 30_000
    on_ok complete()
    on_error fail()
  end
end
```

This creates a normal Elixir module with functions such as:

```elixir
BillingFlow.create(attrs)
BillingFlow.child(attrs)
BillingFlow.fanout(parent_job, children, opts)
BillingFlow.claim_due(:created, worker: "w1", limit: 100)
BillingFlow.run_once(:created, worker: "w1", handler: &handle/1)
BillingFlow.ok(job, result)
BillingFlow.error(job, reason)
BillingFlow.history(id)
```

## Workflow Options

`type` is required.

```elixir
use FerricStore.Flow.Workflow, type: "billing"
```

Every generated command uses this Flow type.

`store` selects the embedded API module. Default is `FerricStore`. Most applications should omit this option.

```elixir
use FerricStore.Flow.Workflow,
  type: "billing",
  store: MyApp.Store
```

Use `store` when your app defines its own embedded instance:

```elixir
defmodule MyApp.Store do
  use FerricStore, data_dir: "/data/ferric"
end
```

Then the SDK calls `MyApp.Store.flow_create/2`,
`MyApp.Store.flow_claim_due/2`, etc.

`partition_by` builds `partition_key` from create attrs:

```elixir
partition_by: [:tenant_id, :invoice_id]
```

Attrs:

```elixir
%{tenant_id: "t1", invoice_id: "i9"}
```

become:

```elixir
partition_key: "t1:i9"
```

Same partition goes to the same shard and keeps ordered Flow behavior.

`initial_state` sets the state used by `create/2`. If omitted, first declared
state is used.

## State DSL

`state name do ... end` declares SDK defaults and action rules.

### `lease_ms`

Default lease for claims in this state:

```elixir
state :created do
  lease_ms 60_000
end
```

Generated claim:

```elixir
BillingFlow.claim_due(:created, worker: "w1")
```

calls:

```elixir
FerricStore.flow_claim_due("billing",
  state: "created",
  worker: "w1",
  lease_ms: 60_000
)
```

### `claim_payload`

Default payload hydration for claims:

```elixir
claim_payload true, max_bytes: 64_000
```

This makes workers receive payload bytes by default up to the cap. If payload is
larger than the cap, Flow returns payload reference/omitted metadata instead of
materializing the large value.

Payload rule:

```text
metadata/index/transition paths do not read payload bytes
payload read happens only when command requests payload hydration
```

### `retry`

Per-state retry policy:

```elixir
retry max_retries: 8,
      backoff: [kind: :fixed, base_ms: 1_000, max_ms: 30_000],
      exhausted_to: "failed"
```

This maps to the existing Flow retry policy. Limits and validation are enforced
by Flow core.

### `on_ok`

Success action for `BillingFlow.ok(job, result)`.

Transition to another state:

```elixir
on_ok :charged
```

Complete terminally:

```elixir
on_ok complete()
```

### `on_error`

Error action for `BillingFlow.error(job, reason)`.

Retry, then move to a state when retry budget is exhausted:

```elixir
on_error retry_or: :failed
```

Fail terminally:

```elixir
on_error fail()
```

## Creating Flows

```elixir
{:ok, flow} =
  BillingFlow.create(%{
    id: "invoice-123",
    tenant_id: "tenant-a",
    invoice_id: "invoice-123",
    payload: %{amount: 4200},
    correlation_id: "order-9"
  })
```

This calls `flow_create/2` with:

```elixir
type: "billing",
state: "created",
partition_key: "tenant-a:invoice-123"
```

`create_many/2` accepts a list of attrs:

```elixir
BillingFlow.create_many([
  %{id: "invoice-1", tenant_id: "t1", invoice_id: "1", payload: p1},
  %{id: "invoice-2", tenant_id: "t2", invoice_id: "2", payload: p2}
])
```

The SDK passes per-item partition keys and lets Flow core group by shard. Each
shard group keeps Flow batch atomicity.

## Children and Fanout

Flow supports parent/child workflows through `flow_spawn_children/3`. The SDK
adds child builders and fanout helpers so users do not have to hand-write the
parent metadata and guard fields.

Build child specs from workflow modules:

```elixir
email =
  EmailFlow.child(%{
    id: "email-123",
    tenant_id: "tenant-a",
    invoice_id: "invoice-123",
    payload: %{template: "paid"}
  })

audit =
  AuditFlow.child(%{
    id: "audit-123",
    tenant_id: "tenant-a",
    invoice_id: "invoice-123",
    payload: %{event: "invoice_paid"}
  })
```

`child/2` adds the child workflow `type`, initial `state`, and derived
`partition_key`. The parent link is still added by Flow core during
`spawn_children`.

Fan out from a claimed parent job:

```elixir
BillingFlow.fanout(parent_job, [email, audit],
  group_id: "notify-and-audit",
  wait: :all,
  on_all_ok: :children_done,
  on_any_error: :children_failed
)
```

This calls:

```elixir
FerricStore.flow_spawn_children(parent_id, children,
  group_id: "notify-and-audit",
  wait: :all,
  wait_state: "waiting_children",
  success: "children_done",
  failure: "children_failed",
  partition_key: parent_job.partition_key,
  from_state: parent_job.state,
  lease_token: parent_job.lease_token,
  fencing_token: parent_job.fencing_token
)
```

`fanout/3` is an alias for `spawn_children/3`. Both accept the same options.

Important options:

* `:group_id` - idempotency key for this child group. Default is `"fanout"`.
  Use a specific value when a parent can spawn more than one group.
* `:wait` - `:all`, `:any`, or `:none`. Default is `:all`.
* `:wait_state` - parent state while waiting. Default is `"waiting_children"`
  for `:all` and `:any`.
* `:on_all_ok` / `:on_success` / `:success` - parent state when child group
  succeeds.
* `:on_any_error` / `:on_failure` / `:failure` - parent state when child group
  fails.
* `:child_failure_policy` / `:on_child_failed` - `:fail_parent` or `:ignore`.
  Default is `:fail_parent`.
* `:on_parent_closed` - `:cancel_children` or `:abandon_children`. Default is
  `:cancel_children`.

Query children:

```elixir
BillingFlow.children(parent_job, count: 100)
BillingFlow.waiting_children(parent_job, count: 100)
```

`children/2` uses the parent lineage index. `waiting_children/2` filters out
terminal child states (`completed`, `failed`, `cancelled`) client-side.

Fanout correctness:

```text
parent update and same-shard children are atomic in one Flow command
cross-shard children use Flow cross-shard coordination
group_id makes duplicate fanout calls idempotent
child completion updates parent summary through Flow core
```

## Claiming Work

```elixir
{:ok, jobs} =
  BillingFlow.claim_due(:created,
    worker: "payment-1",
    limit: 100
  )
```

Returned items are `%FerricStore.Flow.Job{}` structs. The raw Flow record is
available as `job.record`.

Important fields:

```elixir
job.id
job.state
job.partition_key
job.lease_token
job.fencing_token
job.payload
job.payload_ref
```

You can also claim any state:

```elixir
BillingFlow.claim_due(:any, worker: "worker-1", limit: 100)
```

or selected states:

```elixir
BillingFlow.claim_due([:created, :charged], worker: "worker-1")
```

## Handling Jobs

Manual handling:

```elixir
for job <- jobs do
  case charge(job.payload) do
    {:ok, receipt} ->
      BillingFlow.ok(job, receipt)

    {:error, reason} ->
      BillingFlow.error(job, reason)
  end
end
```

`ok/3` and `error/3` carry `partition_key`, `lease_token`, and `fencing_token`
from the job. Core Flow still enforces lease/fencing correctness.

For one-shot polling without supervising a worker:

```elixir
BillingFlow.run_once(:created,
  worker: "payment-1",
  limit: 100,
  handler: fn job ->
    case charge(job.payload) do
      {:ok, receipt} -> {:ok, receipt}
      {:error, reason} -> {:error, reason}
    end
  end
)
```

For applying handler result to an already claimed job:

```elixir
BillingFlow.handle(job, fn job ->
  do_work(job)
end)
```

Handler return contract:

```text
{:ok, result}    -> workflow.ok(job, result)
{:error, reason} -> workflow.error(job, reason)
:noreply         -> handler owns final Flow command
other            -> workflow.error(job, {:unexpected_worker_result, other})
exception        -> workflow.error(job, exception)
```

Explicit commands are also available:

```elixir
BillingFlow.transition(job, :charged, payload)
BillingFlow.complete(job, result)
BillingFlow.retry(job, reason)
BillingFlow.fail(job, reason)
BillingFlow.extend_lease(job, lease_ms: 60_000)
```

## Reads and Queries

SDK read helpers map directly to Flow read APIs:

```elixir
BillingFlow.get(id)
BillingFlow.get(id, payload: true)
BillingFlow.history(id)
BillingFlow.history(id, include_cold: true)
BillingFlow.list(:created, count: 100)
BillingFlow.by_parent(parent_id, count: 100)
BillingFlow.by_root(root_id, count: 100)
BillingFlow.by_correlation(correlation_id, count: 100)
BillingFlow.info()
BillingFlow.stuck()
BillingFlow.children(parent_job)
BillingFlow.waiting_children(parent_job)
```

By default, `get/2` does not hydrate payload. Ask for payload explicitly:

```elixir
BillingFlow.get(id, payload: true, payload_max_bytes: 64_000)
```

## Installing Policy

The DSL can write retry policy defaults into Flow:

```elixir
BillingFlow.install_policy()
```

This calls:

```elixir
FerricStore.flow_policy_set("billing", states: ...)
```

Command-local retry policy still wins over stored policy. This is useful when
you want production defaults set once at boot.

## Optional Worker

You may use your own cron, Oban, Broadway, GenServer loop, or Kubernetes job.
The SDK worker is only a convenience poller.

```elixir
children = [
  {FerricStore.Flow.Worker,
   workflow: BillingFlow,
   state: :created,
   worker: "payment-#{node()}",
   limit: 100,
   interval_ms: 250,
   handler: &MyApp.PaymentWorker.handle/1}
]
```

Handler contract:

```elixir
def handle(job) do
  case charge(job.payload) do
    {:ok, receipt} -> {:ok, receipt}
    {:error, reason} -> {:error, reason}
  end
end
```

Worker behavior:

```text
{:ok, result}    -> workflow.ok(job, result)
{:error, reason} -> workflow.error(job, reason)
:noreply         -> handler owns final Flow command
exception        -> workflow.error(job, exception)
```

## Reclaim

Expired running leases can be reclaimed through normal Flow claim/reclaim
semantics:

```elixir
BillingFlow.reclaim_once(:running,
  worker: "recovery-1",
  limit: 100
)
```

`reclaim_once/2` returns `%FerricStore.Flow.Job{}` structs, same as
`claim_due/2`. Use it for explicit recovery loops. Normal `claim_due/2` can also
mix due work with expired lease reclaim based on core Flow options.

## Atomicity Boundary

Flow command atomicity stays the same as core Flow:

```text
one Flow command = atomic through Ra/Bitcask
same-shard many command = atomic for that shard group
cross-shard many command = atomic per shard group
```

Handler-side reads/writes are not automatically part of the Flow transition.

Good:

```elixir
customer = MyApp.Store.get!("customer:#{job.payload.customer_id}")
BillingFlow.ok(job, receipt)
```

If you need Flow transition plus other writes to be atomic, use an explicit
transaction/cross-op API when available. Do not hide that inside SDK helpers.

## Payload Guidance

Small payloads can be carried inline.

Large payloads should use Flow value refs or app-owned blob refs. The SDK does
not put payload bytes into query indexes. Claim/get/history only hydrate payload
when requested and bounded by `payload_max_bytes`.

Recommended defaults:

```elixir
claim_payload true, max_bytes: 64_000
```

For larger worker inputs:

```elixir
BillingFlow.claim_due(:created,
  worker: "w1",
  payload: true,
  payload_max_bytes: 2_000_000
)
```

Use large caps carefully. They increase response bytes and client memory use.

## Testing

Use `store:` to inject a fake embedded API module:

```elixir
defmodule TestBillingFlow do
  use FerricStore.Flow.Workflow,
    type: "billing",
    store: MyFakeStore,
    partition_by: [:tenant_id]

  state :created do
    on_ok complete()
    on_error retry_or: :failed
  end
end
```

This keeps SDK unit tests fast and separate from Ra/Bitcask integration tests.

## What SDK Does Not Do

The SDK does not:

* use a replay-driven workflow engine
* hide Flow transitions inside normal Elixir code
* make arbitrary KV writes atomic with Flow transitions
* require RESP or an external client
* make LMDB part of command correctness
* read payload bytes unless the command asks for payload hydration
