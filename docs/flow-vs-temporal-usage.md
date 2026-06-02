# Workflow Usage Examples

This is not a feature checklist. It shows code-shape examples for common
workflow patterns.

The short version:

```text
Workflow-code examples: workflow code schedules activities
FerricFlow examples: durable states, workers, signals, and explicit outcomes
```

## 1. Durable queue item

### Workflow-code shape

A queue-like workload can be modeled as a workflow or activity task:

```python
# conceptual Temporal shape
@workflow.defn
class EmailWorkflow:
    @workflow.run
    async def run(self, user_id: str) -> None:
        await workflow.execute_activity(
            send_email,
            user_id,
            start_to_close_timeout=timedelta(seconds=30),
        )
```

The worker runs SDK workflow code, and workflow history is used to resume the
workflow model correctly.

### FerricFlow shape

FerricFlow models it as durable work in a state:

```python
from ferricstore import QueueClient

client = QueueClient.from_url("redis://127.0.0.1:6379/0")
emails = client.queue(type="email")

emails.enqueue("email-1", payload=b"user-1", idempotent=True)


def send_email(job):
    send(job.payload)
    return b"sent"


emails.worker(state="queued", concurrency=100, batch_size=500).run(send_email)
```

The server owns the due index, lease, state mutation, and history. The handler is
normal application code; it does not replay.

## 2. Multi-step workflow

### Workflow-code shape

A code-driven workflow runtime often expresses steps as workflow code:

```python
# conceptual Temporal shape
@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> str:
        charge_id = await workflow.execute_activity(charge_card, order_id, ...)
        await workflow.execute_activity(send_receipt, charge_id, ...)
        return "ok"
```

The workflow code represents the orchestration model.

### FerricFlow shape

FerricFlow expresses steps as durable states:

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

The state record is the orchestration source of truth. Returning
`transition(...)`, `complete(...)`, `retry(...)`, or `fail(...)` from the handler
asks FerricFlow to mutate state atomically with lease/fencing validation.

## 3. Retry after failure

### Workflow-code shape

A workflow runtime commonly configures retry policy on activities or workflows:

```python
# conceptual Temporal shape
await workflow.execute_activity(
    charge_card,
    order_id,
    retry_policy=RetryPolicy(maximum_attempts=5),
    start_to_close_timeout=timedelta(seconds=30),
)
```

### FerricFlow shape

FerricFlow retry can be a worker default, state policy, or explicit outcome:

```python
from ferricstore import ExceptionPolicy, RetryPolicy, retry, transition

order = client.workflow(
    type="order",
    initial_state="charge",
    retry_policy=RetryPolicy(max_retries=5, backoff="exponential"),
)


@order.state("charge", exception_policy=ExceptionPolicy.RETRY)
def charge(job):
    result = charge_card(job.payload)
    if result.rate_limited:
        return retry(error=b"rate limited")
    return transition("ship")
```

`RetryPolicy` controls the durable retry schedule. `ExceptionPolicy.RETRY`
handles unhandled handler exceptions. Explicit `retry(...)` is used when
application code knows the current attempt should be retried. The retry decision
becomes durable Flow state and history, and workers later claim the flow again
when it is due.

## 4. Fanout

### Workflow-code shape

A workflow runtime can fan out by starting many activities or child workflows from
workflow code:

```python
# conceptual Temporal shape
children = [
    workflow.start_child_workflow(DeviceWorkflow.run, device_id)
    for device_id in device_ids
]
```

The parent workflow code owns the fanout model in this shape.

### FerricFlow shape

FerricFlow fanout uses first-class child flows. The parent state handler creates
children through the Flow context, so parent/child links are stored by the
server:

```python
from ferricstore import ChildSpec, transition

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

The parent/child links are queryable later. Child flows are normal Flow records
with their own state, lease, retry, terminal status, and history.

If many children need the same large bytes, store those bytes as a Flow value and
attach value refs instead of duplicating payload bytes in every child.

## 5. Large optional payloads

### Workflow-code shape

Workflow applications often keep large data outside workflow history and pass
references, or use payload codecs/converters carefully.

```python
# conceptual Temporal shape
await workflow.execute_activity(process_invoice, invoice_blob_ref, ...)
```

### FerricFlow shape

FerricFlow has first-class value refs tied to a flow:

```python
orders.enqueue(
    "order-1",
    payload=b"small routing bytes",
    values={
        "invoice": invoice_pdf_bytes,
        "customer": customer_snapshot_bytes,
    },
)

orders.worker(claim_values=["customer"]).run(handle_customer_step)
```

Only requested values are hydrated for the worker. Other values stay stored as
Flow value refs and follow Flow retention policy.

## 6. Investigating what happened

### Workflow-code shape

Workflow-runtime visibility is usually workflow-history centered: events,
activity attempts, timers, signals, and search attributes.

### FerricFlow shape

FerricFlow history is state-transition and signal centered:

```python
record = orders.get("order-1")
history = orders.history("order-1")
children = orders.children("order-1")
```

You inspect the current state, terminal result/error, retry attempts, parent/root
links, value refs, and history events.


## 7. Signals / external events

### Workflow-code shape

In a workflow-code shape, signals are methods on workflow code. A running
workflow receives the signal and updates workflow state.

```python
# conceptual Temporal shape
@workflow.defn
class ApprovalWorkflow:
    def __init__(self) -> None:
        self.approved = False

    @workflow.signal
    async def approve(self, user_id: str) -> None:
        self.approved = True

    @workflow.run
    async def run(self, request_id: str) -> str:
        await workflow.wait_condition(lambda: self.approved)
        return "approved"
```

### FerricFlow shape

FerricFlow has an explicit signal command. A signal records the external event
durably, can be idempotent, can be guarded by current state, and can optionally
move the flow to another state.

```python
approval = client.workflow(type="approval", initial_state="waiting")

approval.start("request-1", payload=b"approval request", idempotent=True)

# Later, from an API handler or webhook:
approval.signal(
    "request-1",
    signal="approved",
    if_state="waiting",
    transition_to="approved",
    values={"approval": b"approved by user-42"},
    idempotency_key="approval-request-1-user-42",
)
```

The signal is visible in Flow history. If `transition_to` is provided, it also
advances the durable state. No workflow code needs to be kept suspended in
memory.

## 8. Query current workflow state

### Workflow-code shape

In a workflow-code shape, queries are methods that read workflow state.

```python
# conceptual Temporal shape
@workflow.query
async def status(self) -> str:
    return self.current_status
```

### FerricFlow shape

FerricFlow queries read durable server state and indexes.

```python
record = approval.get("request-1")
history = approval.history("request-1")
children = approval.children("request-1")
```

For operational queries, use Flow list/index APIs through the SDK:

```python
waiting = approval.list(state="waiting", count=100)
failed = approval.terminals(state="failed", count=100)
```

## 9. Timers / delayed work

### Workflow-code shape

In a workflow-code shape, timers are expressed inside workflow code:

```python
# conceptual Temporal shape
await workflow.sleep(timedelta(hours=1))
await workflow.execute_activity(send_reminder, request_id, ...)
```

### FerricFlow shape

FerricFlow stores the next due time in the Flow record/index. Workers claim the
flow only when it becomes due.

```python
from ferricstore import retry, transition


@approval.state("waiting")
def waiting(job):
    if not is_approved(job.id):
        return retry(error=b"still waiting", run_at_ms=one_hour_from_now_ms())
    return transition("approved")
```

You can also create work directly for a future due time:

```python
reminders.enqueue(
    "reminder-1",
    payload=b"request-1",
    run_at_ms=one_hour_from_now_ms(),
    idempotent=True,
)
```

## 10. Cancellation

### Workflow-code shape

In a workflow-code shape, cancellation is delivered to workflow/activity
execution and handled by the SDK runtime.

```python
# conceptual Temporal shape
await client.get_workflow_handle("request-1").cancel()
```

### FerricFlow shape

FerricFlow cancellation is a terminal state mutation.

```python
approval.cancel("request-1", error=b"cancelled by user")
```

After cancellation, the flow is terminal and no longer claimable as normal due
work. The cancellation appears in history and terminal queries.

## 11. Human approval

### Workflow-code shape

A workflow-code shape often models human approval as workflow code waiting for a
signal.

```python
# conceptual Temporal shape
await workflow.wait_condition(lambda: self.approved or self.rejected)
```

### FerricFlow shape

FerricFlow keeps the request in a durable state. The web/API layer sends a
signal when the human acts.

```python
approval.start("approval-1", payload=b"invoice:123", idempotent=True)

# User clicks approve:
approval.signal(
    "approval-1",
    signal="approved",
    if_state="waiting",
    transition_to="approved",
    idempotency_key="approve-approval-1",
)

# User clicks reject:
approval.signal(
    "approval-1",
    signal="rejected",
    if_state="waiting",
    transition_to="rejected",
    idempotency_key="reject-approval-1",
)
```

Workers can process the next state normally:

```python
@approval.state("approved")
def approved(job):
    release_payment(job.payload)
    return complete(result=b"released")
```

## 12. Continue-as-new / long histories

### Workflow-code shape

Workflow-code runtimes often use continue-as-new to keep workflow histories
bounded.

```python
# conceptual Temporal shape
return workflow.continue_as_new(next_input)
```

### FerricFlow shape

FerricFlow stores current state separately from history and uses retention/cold
projection for older history. Long-running processes normally stay as the same
flow id unless the application wants to create a new generation explicitly.

```python
record = workflow.get("flow-1")
history = workflow.history("flow-1")
```

If the application wants a new generation, create a new flow and link it through
correlation/root metadata:

```python
workflow.start(
    "flow-1:generation-2",
    payload=record.payload,
    correlation_id="flow-1",
    idempotent=True,
)
```
