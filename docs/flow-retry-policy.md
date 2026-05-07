# Flow Retry Policy

Flow retry policy controls what `FLOW.RETRY` and `FLOW.RETRY_MANY` do after a leased
worker returns work to the scheduler.

Policy precedence is:

1. Built-in default.
2. Type policy from `FLOW.POLICY.SET`.
3. State policy from `FLOW.POLICY.SET ... STATE <state> ...`.
4. Command-local retry policy on `FLOW.RETRY` or `FLOW.RETRY_MANY`.

The effective policy is evaluated inside the Flow apply path. The durable source
of truth is Ra/Bitcask Flow state plus the stored policy key. LMDB receives the
policy asynchronously as a cold projection only; Flow command correctness does not
wait for LMDB.

## Defaults And Guards

Default retry policy:

```text
MAX_ATTEMPTS 3
BACKOFF EXPONENTIAL
BASE_MS 1000
MAX_MS 30000
JITTER_PCT 20
EXHAUSTED_TO failed
```

Guards:

- `MAX_ATTEMPTS` must be `0..1000`.
- `BASE_MS` and `MAX_MS` must be `0..2592000000` (30 days).
- `JITTER_PCT` must be `0..100`.
- `BACKOFF` must be `NONE`, `FIXED`, `LINEAR`, or `EXPONENTIAL`.
- `EXHAUSTED_TO` can be any non-empty state except `running`.
- Terminal states are fixed: `completed`, `failed`, `cancelled`.

## Redis Commands

Set a type default:

```text
FLOW.POLICY.SET checkout MAX_ATTEMPTS 5 BACKOFF EXPONENTIAL BASE_MS 1000 MAX_MS 60000 JITTER_PCT 10 EXHAUSTED_TO failed
```

Set per-state overrides in the same command:

```text
FLOW.POLICY.SET checkout \
  MAX_ATTEMPTS 5 EXHAUSTED_TO failed \
  STATE charge_card MAX_ATTEMPTS 2 BACKOFF FIXED BASE_MS 10000 MAX_MS 10000 JITTER_PCT 0 EXHAUSTED_TO payment_failed
```

Read effective policy:

```text
FLOW.POLICY.GET checkout
FLOW.POLICY.GET checkout STATE charge_card
```

Command-local override:

```text
FLOW.RETRY flow-1 lease-token FENCING 7 MAX_ATTEMPTS 1 EXHAUSTED_TO payment_failed
```

## Embedded API

```elixir
FerricStore.flow_policy_set("checkout",
  retry: [
    max_attempts: 5,
    backoff: [kind: :exponential, base_ms: 1_000, max_ms: 60_000, jitter_pct: 10],
    exhausted_to: "failed"
  ],
  states: %{
    "charge_card" => [
      retry: [max_attempts: 2, exhausted_to: "payment_failed"]
    ]
  }
)

FerricStore.flow_policy_get("checkout", state: "charge_card")
```

## History Metadata

Retry history events include the effective retry decision and policy values used:

- `retry_decision`: `scheduled` or `exhausted`.
- `retry_run_state`: state to return to after retry.
- `retry_next_run_at_ms`: computed or explicit next run time.
- `retry_max_attempts`.
- `retry_backoff_kind`.
- `retry_backoff_base_ms`.
- `retry_backoff_max_ms`.
- `retry_jitter_pct`.
- `retry_exhausted_to`.

This keeps debugging self-contained in `FLOW.HISTORY` without requiring the
caller to reconstruct which policy was active at the time.
