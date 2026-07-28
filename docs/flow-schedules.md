# FerricFlow Schedules

FerricFlow schedules durably create Flow targets at a configured time. Schedule
records use the same shard-owned Raft, lease, and fencing path as other Flow
records, so only one claimant can advance a due schedule after restart or
leader failover.

## Schedule Kinds

| Kind | Required timing | Behavior |
| --- | --- | --- |
| `one_shot` | None | Creates one target at `at_ms`, an absolute Unix timestamp in milliseconds; omitted `at_ms` means now. |
| `delay` | `delay_ms` | Creates one target after a delay from creation. |
| `interval` | `every_ms` | Creates recurring targets; `start_at_ms` is optional. |
| `cron` | `cron` | Creates recurring targets on minute-granularity wall-clock matches. |

Cron defaults to `Etc/UTC`. Set `timezone` to an IANA timezone for wall-clock
matching with daylight-saving transitions. Planning uses an immutable
eight-year Gregorian bound, which covers the longest valid leap-day gap across
a non-leap century. The planner advances by calendar day and only evaluates
allowed times on matching dates; it does not scan every elapsed minute.

Recurring targets cannot use a fixed `id`. Set `id_prefix` to choose the
generated ID prefix; when it is omitted, the schedule ID is used. One-shot and
delay targets may use a fixed `id`, or use the same generated-ID form.
Generated IDs contain the logical due time and actual fire count:

```text
<id_prefix>:<due_at_ms>:<fire_count>
```

## Bounded Interval Catch-Up

Interval schedules use `catchup_policy: :fire_once`. This is the default and
currently the only accepted interval catch-up policy.

When the scheduler observes an interval less than one full period late, it
fires the recorded occurrence and preserves the existing cadence. When it is
one or more full periods late, it:

1. Creates exactly one target for the recorded due occurrence.
2. Counts the additional elapsed occurrences as coalesced.
3. Sets the next run to `recovery_time + every_ms`.

The count uses integer arithmetic and is O(1), even after millions of missed
periods. There is no per-period loop and no recovery burst. `fire_count` counts
targets actually created, not coalesced occurrences.

For example, an interval due at `1,000` with `every_ms: 100` recovered at
`2,050` creates one target, reports `coalesced: 10`, and schedules its next run
at `2,150`. A second fire-due call at `2,050` creates nothing.

Catch-up applies after downtime, pause/resume, delayed overlap queues, process
restart, and Raft leader failover. It does not change manual schedule fires,
cron matching, or Flow retry timing.

Cron recovery is intentionally different. An overdue cron schedule advances
one matching occurrence per successful automatic fire; `fire_once` does not
coalesce cron matches. Use an interval schedule when recovery must have O(1)
work and create no burst after extended downtime.

## Catch-Up And Overlap

Catch-up and overlap solve different problems:

| Policy | Question answered |
| --- | --- |
| `catchup_policy` | How should elapsed interval occurrences be handled after scheduler delay? |
| `overlap_policy` | What should happen when the previous target is still active? |

Recurring schedules support these overlap policies:

| Value | Behavior |
| --- | --- |
| `allow` | Create the new target even while the previous target is active. |
| `skip` | Skip the current occurrence and advance the schedule. |
| `queue_after_previous` | Retain one due occurrence and retry after the previous target finishes. |
| `fail_schedule` | Mark the schedule failed. |

A queued occurrence keeps its original logical due time. If it is released
after additional intervals elapsed, that occurrence fires once and the later
elapsed periods are coalesced.

## Create An Interval Schedule

Embedded Elixir:

```elixir
{:ok, schedule} =
  FerricStore.flow_schedule_create("billing-sweep",
    kind: :interval,
    every_ms: 60_000,
    start_at_ms: System.system_time(:millisecond),
    catchup_policy: :fire_once,
    overlap_policy: :queue_after_previous,
    overlap_retry_ms: 1_000,
    max_fires: 1_000,
    target: [
      id_prefix: "billing-sweep",
      type: "billing",
      state: "queued"
    ]
  )
```

Native request body:

```json
{
  "id": "billing-sweep",
  "kind": "interval",
  "every_ms": 60000,
  "catchup_policy": "fire_once",
  "overlap_policy": "queue_after_previous",
  "target": {
    "id_prefix": "billing-sweep",
    "type": "billing",
    "state": "queued"
  }
}
```

Clients should discover `FLOW.SCHEDULE.CREATE` fields through `OPTIONS` before
sending them.

Schedule creation rejects unknown or duplicate options and target fields. A
target accepts the same bounded create data used by Flow scheduling:
`type`, `state`, `id` or `id_prefix`, partition and lineage identifiers,
`run_at_ms`, `priority`, `payload` or `payload_ref`, and `values` or
`value_refs`. Prefer refs for larger data. Definitions are limited to 32 KiB
by default, inline target `payload` and `values` are each limited to 8 KiB by
default, and admission also reserves enough encoded space for later runtime
bookkeeping. A schedule accepted at creation therefore remains readable after
fires, overlap handling, and catch-up updates.

Internal schedule hydration uses an immutable 64 KiB ceiling. It is independent
of the public Flow payload-return setting, so lowering that setting after
creation cannot make a valid schedule unreadable or unfireable.

## Inspection And Operations

`FLOW.SCHEDULE.GET` and `FLOW.SCHEDULE.LIST` expose:

| Field | Meaning |
| --- | --- |
| `catchup_policy` | Interval catch-up policy, or null for other kinds. |
| `coalesced_count` | Cumulative number of elapsed occurrences not replayed. |
| `last_coalesced_count` | Number coalesced by the latest recovery fire. |
| `last_catchup_at_ms` | Actual scheduler time of the latest catch-up. |
| `fire_count` | Targets actually created. |
| `next_run_at_ms` | Next durable due time, or null after completion. |
| `last_planning_error` | Actionable recurrence error when `end_reason` is `planning_failed`; otherwise null. |

Schedule states are `active`, `paused`, `running`, `completed`, `failed`, and
`cancelled`. `running` is a transient durable claim state while a scheduler
owns a fenced lease; normal clients should observe it but must not treat it as
a new user-controlled lifecycle state. List operations default to `active`;
pass `state: :all` (or the native string `"all"`) to inspect every state.
State filters outside this set and inverted `from_ms`/`to_ms` ranges are
rejected before storage access. Listing uses a replicated partition catalog and
a single 1,000-record candidate budget across every requested state and
partition; it never scans all 256 schedule buckets when none are occupied.

`FLOW.SCHEDULE.FIRE_DUE` returns batch counts for `claimed`, `fired`, `skipped`,
and `coalesced`, plus bounded per-schedule `errors`. Every claimed schedule has
exactly one fired, skipped, or error outcome. If completed work is followed by
a failure to claim a later wave, the response preserves those completed
outcomes and adds `claim_error`; the batch failure is not counted as a claimed
schedule. The field is omitted when every claim wave succeeds.

The built-in scheduler executes this command continuously; applications
normally inspect schedules rather than call it directly. Call it only for
tests, administrative recovery, or a deployment that deliberately disables
the built-in runner and provides one custom scheduler. Raft claims and fencing
prevent duplicate advancement, but an unnecessary second runner still adds
contention and operational ambiguity.
One pass claims at most 16 schedules per wave and executes at most 8 fires
concurrently. A later wave is not leased until the previous wave finishes, so
the configured pass limit cannot turn into a large set of idle leases.

Pausing removes the schedule from due claiming without changing its recorded
due time. Resuming makes it active again; if it is overdue, the next automatic
fire uses bounded catch-up. `end_at_ms` and `max_fires` remain terminal bounds.
Coalesced occurrences do not consume `max_fires` because they did not create
targets.

A manual `FLOW.SCHEDULE.FIRE` creates or skips one occurrence immediately; it
does not replay elapsed interval occurrences. After that outcome, the same
`end_at_ms`, `max_fires`, and timestamp-range checks used by the automatic
runner are applied. Terminal records always expose `next_run_at_ms: null`.

Completed schedules report one of these `end_reason` values:

| Value | Meaning |
| --- | --- |
| `one_shot_fired` | A one-shot or delay schedule created its target. |
| `max_fires` | The configured target-creation limit was reached. |
| `end_at_ms` | The schedule reached its configured inclusive time bound. |
| `timestamp_limit` | No later due time can be represented safely. |
| `planning_failed` | Persisted recurrence data could not be parsed or planned; inspect `last_planning_error`. |

`overlap_policy: fail_schedule` produces state `failed` with
`end_reason: overlap_failed`. Cancellation produces state `cancelled` and also
clears the visible next due time. Skip outcomes still apply terminal bounds,
but do not consume `max_fires` because no target was created.
Planning happens before target creation, including overlap-skip advancement, so
a malformed recurrence fails the schedule without leaving an orphan target.

The health dashboard's Flow schedules page displays the catch-up policy,
cumulative count, latest count, and latest catch-up time.

## Runtime Configuration

The automatic runner is controlled by the `:flow_scheduler_enabled`,
`:flow_scheduler_limit`, `:flow_scheduler_initial_delay_ms`, and
`:flow_scheduler_error_sleep_ms` application settings. The claim limit is
capped by `:flow_max_claim_limit`. These affect how due work is claimed, not
the persisted schedule definition or catch-up arithmetic.
See [Configuration Reference](../guides/configuration.md#flow-scheduler).

Release and Docker deployments can set the equivalent
`FERRICSTORE_FLOW_SCHEDULER_ENABLED`, `FERRICSTORE_FLOW_SCHEDULER_LIMIT`,
`FERRICSTORE_FLOW_SCHEDULER_INITIAL_DELAY_MS`, and
`FERRICSTORE_FLOW_SCHEDULER_ERROR_SLEEP_MS` environment variables. Runner
settings are validated and captured once when the scheduler starts; changing
process environment or application settings later does not mutate a running
worker.
