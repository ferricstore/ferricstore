# Workflow Runtime Shapes And FerricFlow Examples

This document shows common workflow patterns in two shapes:

| Shape | Description |
| --- | --- |
| Workflow-code runtime | Application code models workflow control flow and activities. Examples are conceptual. |
| FerricFlow | FerricStore stores durable Flow records; workers claim states and return explicit outcomes. |

FerricFlow’s model is the same across the examples:

```text
Flow record: type + id + state + payload/value refs + lease + history + terminal status
Worker:      claim due state -> run handler -> transition/complete/fail/retry
```

Handlers are normal application code. FerricFlow does not replay handler code to recover state. A Flow command returns success only after the state change is accepted through the quorum path and written to disk.

## Durable Queue Item

Workflow-code shape:

```python
# conceptual shape
@workflow.defn
class EmailWorkflow:
    @workflow.run
    async def run(self, user_id: str) -> None:
        await workflow.execute_activity(send_email, user_id)
```

FerricFlow shape:

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

Queue workers usually process one state and complete, fail, or retry.

## State-Machine Workflow

Workflow-code shape:

```python
# conceptual shape
@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> str:
        charge_id = await workflow.execute_activity(charge_card, order_id)
        await workflow.execute_activity(send_receipt, charge_id)
        return "ok"
```

FerricFlow shape:

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

Workflow workers process multiple named states and return explicit transitions.

## Signals And External Events

Workflow-code shape:

```python
# conceptual shape
@workflow.signal
async def approve(self, user_id: str) -> None:
    self.approved = True
```

FerricFlow shape:

```python
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

Signals are durable history events. They can be idempotent, state-guarded, and can optionally move the Flow to another state.

## Retry After Failure

Workflow-code shape:

```python
# conceptual shape
await workflow.execute_activity(
    charge_card,
    order_id,
    retry_policy=RetryPolicy(maximum_attempts=5),
)
```

FerricFlow shape:

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

Retry state is durable. Workers claim the Flow again when its next due time arrives.

## Fanout

Workflow-code shape:

```python
# conceptual shape
children = [
    workflow.start_child_workflow(DeviceWorkflow.run, device_id)
    for device_id in device_ids
]
```

FerricFlow shape:

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

Child Flows have their own state, retry, lease, history, and terminal status. Parent/child links are queryable later.

## Value Refs For Large Optional Data

Workflow-code shape:

```python
# conceptual shape
await workflow.execute_activity(process_invoice, invoice_blob_ref)
```

FerricFlow shape:

```python
orders = client.queue(type="order")

orders.enqueue(
    "order-1",
    payload=b"small routing bytes",
    values={"invoice": invoice_pdf_bytes, "customer": customer_snapshot_bytes},
)

orders.worker(claim_values=["customer"]).run(handle_customer_step)
```

Only requested values are hydrated for the worker. Other values stay stored as Flow value refs and follow Flow retention policy.

## Failure Model

- Claiming work grants a lease and lease token.
- Transition, retry, complete, fail, and cancel validate the current lease token.
- If a worker crashes, the Flow becomes claimable again after lease expiry or reclaim.
- Handlers can run more than once after crashes or retries; side effects should be idempotent or guarded by application keys.
- Current Flow state is authoritative. History and cold projections may lag briefly.

## Inspecting History And State

```python
record = order.get("order-1")
history = order.history("order-1")
children = order.children("order-1")
failed = order.terminals(state="failed", count=100)
```

Use current state for decisions. Use history for debugging and audit.
