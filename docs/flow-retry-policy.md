# Flow Retry Policy

Retry policy controls what happens when a leased Flow should be tried again.

A retry can:

- put the Flow back into a due state later;
- apply fixed or exponential backoff;
- stop retrying after a maximum retry count;
- move the Flow to a terminal or manual state when exhausted.

Retry is per Flow. Child Flows, fanout Flows, and ordinary queue items each track their own retry state.

## Mental Model

```text
claim due work
-> handler fails or returns retry(...)
-> FerricFlow validates the lease token
-> retry count increases
-> next due time is scheduled
-> worker can claim again when due
```

If retry budget is exhausted, FerricFlow applies the exhaustion rule from the effective policy.

## Attempts vs Retries

`MAX_RETRIES` is the number of retry operations after the initial attempt.

```text
MAX_RETRIES 0  -> one attempt only, no retries
MAX_RETRIES 1  -> initial attempt + one retry
MAX_RETRIES 5  -> initial attempt + five retries
```

This keeps the policy aligned with normal application language: retries are extra attempts after the first failure.

## Policy Precedence

FerricFlow resolves retry policy in this order:

| Level | Meaning |
| --- | --- |
| State policy | Most specific. Applies to one Flow type and state. |
| Type policy | Applies to every state of the Flow type. |
| Command outcome | `retry(...)` can provide per-attempt error/result data. |
| Default | Safe default behavior if no policy is installed. |

## Maximum Active Runtime

`max_active_ms` bounds the total non-terminal lifetime of a Flow, measured from
`created_at_ms`. Queue wait, scheduled delay, running work, and retry backoff all
count toward the same deadline.

The value is type-level and must be between `1` and `31_536_000_000`
milliseconds. It cannot be configured inside a state policy. A create command
can override the type policy for one Flow, and `infinity` opts that Flow out.

```text
FLOW.POLICY.SET order MAX_ACTIVE_MS 300000
FLOW.CREATE order-1 TYPE order STATE queued MAX_ACTIVE_MS 60000
FLOW.CREATE order-no-deadline TYPE order STATE queued MAX_ACTIVE_MS INFINITY
```

```elixir
FerricStore.flow_policy_set("order", max_active_ms: :timer.minutes(5))

FerricStore.flow_create("order-1",
  type: "order",
  state: "queued",
  max_active_ms: :timer.minutes(1)
)
```

At or after the deadline, FerricFlow changes a non-terminal record to `failed`,
clears its lease and due time, starts terminal retention, and records a `failed`
history event with `reason: "max_active_ms"`. Claim and completion paths recheck
the deadline, and the instance retention sweeper handles inactive and
cold records. `FLOW.RETENTION_CLEANUP` reports these transitions as
`active_timeouts` before using the remaining batch budget for terminal deletion.

## Python SDK

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

Unhandled handler exceptions follow `exception_policy`. Explicit `retry(...)` is for application-known retry decisions.

## Native TCP commands

Install or update policy for a Flow type:

```text
FLOW.POLICY.SET order MAX_RETRIES 5 BACKOFF exponential BASE_MS 1000 MAX_MS 60000
```

Install state-specific policy:

```text
FLOW.POLICY.SET order STATE charge MAX_RETRIES 5 BACKOFF exponential BASE_MS 1000 MAX_MS 60000
```

Retry a leased Flow:

```text
FLOW.RETRY order-1 <lease-token> FENCING <fencing-token> ERROR "rate limited"
```

The `lease_token` and `FENCING` token come from `FLOW.CLAIM_DUE`. Stale workers cannot retry or complete a Flow after another worker has claimed it.

## Embedded Elixir

```elixir
FerricStore.flow_policy_set("order",
  retry: [
    max_retries: 5,
    backoff: [kind: :exponential, base_ms: 1_000, max_ms: 60_000],
    exhausted_to: "failed"
  ],
  states: %{
    "charge" => [
      retry: [
        max_retries: 5,
        backoff: [kind: :exponential, base_ms: 1_000, max_ms: 60_000],
        exhausted_to: "failed"
      ]
    ]
  }
)
```

Retry from a leased claim:

```elixir
FerricStore.flow_retry(claim.id, claim.lease_token,
  fencing_token: claim.fencing_token,
  error: "rate limited"
)
```

## Exhaustion Behavior

When retry budget is exhausted, the effective policy decides the next state:

| Exhaustion target | Behavior |
| --- | --- |
| `failed` | Flow becomes terminal failed. |
| `cancelled` | Flow becomes terminal cancelled. |
| `completed` | Flow becomes terminal completed. |
| active/manual state | Flow moves to that state and is no longer retried by this policy. |

Terminal exhaustion runs normal terminal hooks: history is recorded, terminal indexes/projections are updated asynchronously, and parent/child group hooks can observe child terminal status.

## History And Debugging

Retry events are visible in Flow history with retry count, error/result metadata, and due time. Use history to answer:

- why the Flow retried;
- how many retries were used;
- when the next attempt was scheduled;
- whether retry exhausted into a terminal or manual state.
