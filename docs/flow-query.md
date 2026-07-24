# Flow Query Guide And Reference

`FLOW.QUERY` is FerricStore's versioned, bounded read surface for Flow runs and
history. Its query language, FQL1, is deliberately smaller than SQL: callers
describe the records they need, while the planner must prove that an
authorized index and immutable execution budget bound the work. FerricStore
does not fall back to a full run scan.

This document is the user, SDK, operator, and architecture reference for the
OSS query planner. FerricStore Enterprise uses the same FQL1 parser, planner,
index lifecycle, result contracts, and limits; extensions may add trusted
scope and governance without changing these contracts.

In OSS, `partition_key` is the logical data, routing, and ACL scope. There is
no separate tenant argument or tenant data model. Examples use `tenant-a` only
as an ordinary partition value.

## Quick Start

Use named parameters for values. This point query addresses a run in its
deterministic automatic partition:

```fql
FROM runs WHERE run_id = @run_id RETURN RECORD
```

Explicitly partitioned runs require the partition in the query:

```fql
FROM runs
WHERE partition_key = @partition AND run_id = @run_id
RETURN RECORD
```

A collection query always has one exact partition, an explicit order, and a
bounded limit:

```fql
FROM runs
WHERE partition_key = @partition
  AND type = @type
  AND state = @state
ORDER BY updated_at_ms DESC
LIMIT 50
RETURN RECORDS
```

Append a result-field list when the caller needs only part of each record:

```fql
FROM runs
WHERE partition_key = @partition
  AND type = @type
  AND state = @state
ORDER BY updated_at_ms DESC
LIMIT 50
RETURN RECORDS (run_id, state, updated_at_ms, attribute['customer'])
```

Embedded Elixir accepts the query and a typed parameter map. A generated
instance module from `use FerricStore` exposes the same `flow_query/2` call:

```elixir
query = """
FROM runs
WHERE partition_key = @partition AND type = @type AND state = @state
ORDER BY updated_at_ms DESC
LIMIT 50
RETURN RECORDS
"""

{:ok, page} =
  FerricStore.flow_query(query, %{
    "partition" => "tenant-a",
    "type" => "invoice",
    "state" => "failed"
  })
```

Native clients should prefer the typed `0x0231` request. Its body separates the
FQL program from values:

```json
{
  "version": "FQL1",
  "query": "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state ORDER BY updated_at_ms DESC LIMIT 50 RETURN RECORDS",
  "params": {
    "partition": "tenant-a",
    "type": "invoice",
    "state": "failed"
  }
}
```

The textual `COMMAND_EXEC` form takes the version and query followed by
parameter name/value pairs. Its values are strings:

```text
FLOW.QUERY FQL1 "FROM runs WHERE partition_key = @partition AND state = @state ORDER BY updated_at_ms DESC LIMIT 50 RETURN RECORDS" partition tenant-a state failed
```

Before putting a new query into production, prefix it with `EXPLAIN` and check
these fields in order:

1. `status` is `planned`, not `rejected`.
2. `plan.path` is the expected access path and `plan.index` identifies the
   generation that will be used.
3. `plan.projection.fields` matches the requested result fields and
   `plan.projection.index_only` is `false`.
4. `plan.residual_predicates` is empty or intentionally small.
5. `plan.order` is `native` when the index should satisfy the requested order;
   `bounded_top_k` means a bounded in-memory sort is required.
6. `pressure.hard_limiting_resource` has sufficient headroom.
7. `stats.state` is `fresh` or `current`; stale or unavailable statistics make
   estimates conservative, not correctness weaker.

Then use `EXPLAIN ANALYZE` against representative data to compare estimates
with actual bounded usage. It executes the read but returns no records or count
value.

## FQL1 Grammar

Keywords and built-in field names are ASCII case-insensitive. Metadata names
retain their case. Whitespace may be spaces, tabs, carriage returns, or
newlines. One optional semicolon may terminate a query.

```ebnf
query            = [ "EXPLAIN" [ "ANALYZE" ] ],
                   "FROM", source, "WHERE", predicate,
                   { "AND", predicate }, tail, [ ";" ] ;

source           = "runs" | "events" ;

tail             = point_tail | count_tail | collection_tail ;
point_tail        = "RETURN", "RECORD", [ return_projection ] ;
count_tail        = "RETURN", "COUNT" ;
collection_tail   = "ORDER", "BY", order, [ ",", order ],
                   "LIMIT", positive_integer,
                   [ "CURSOR", parameter ],
                   "RETURN", "RECORDS", [ return_projection ] ;
order             = order_field, ( "ASC" | "DESC" ) ;
return_projection = "(", result_field, { ",", result_field }, ")" ;

predicate        = field, "=", value
                 | field, "IN", "(", value, { ",", value }, ")"
                 | field, "BETWEEN", value, "AND", value
                 | time_field, "FROM", value, "TO", value
                 | field, "IS", "NULL"
                 | field, "IS", "MISSING" ;

value             = string_literal | signed_integer | parameter ;
parameter         = "@", parameter_name ;
parameter_name    = identifier_character, { identifier_character } ;
string_literal    = "'", { character | "''" }, "'" ;

field             = built_in_field | attribute_field | state_meta_field ;
built_in_field    = "partition_key" | "run_id" | "event_id"
                  | "type" | "state" | "run_state"
                  | integer_field
                  | "parent_flow_id" | "root_flow_id" | "correlation_id" ;
integer_field     = "version" | "priority" | "created_at_ms"
                  | "updated_at_ms" | "next_run_at_ms"
                  | "lease_deadline_ms" | "attempts" | "max_active_ms" ;
order_field       = integer_field | "event_id" ;
time_field        = "created_at_ms" | "updated_at_ms"
                  | "next_run_at_ms" | "lease_deadline_ms" ;
attribute_field   = "attribute.", unquoted_segment
                  | "attribute[", string_literal, "]" ;
state_meta_field  = "state_meta.", unquoted_segment, ".", unquoted_segment
                  | "state_meta[", string_literal, "][", string_literal, "]" ;
result_field      = built_in_field | "attributes" | "state_meta"
                  | attribute_field | state_meta_field
                  | "fields"
                  | "fields[", string_literal, "]" ;
```

An `identifier_character` is an ASCII letter, digit, underscore, hyphen, or
dot. An unquoted metadata segment uses ASCII letters, digits, underscores, and
hyphens; the validation rules below additionally reject reserved names.
`event_id` is an order field only for `events`; run collections accept only the
integer order fields.

FQL1 has no SQL `SELECT` clause, `OR`, `NOT`, joins, subqueries, grouping,
mutation, comments, wildcard projection, expression evaluation, planner hints,
or user-supplied index name. Result fields are selected only in the bounded
`RETURN RECORD(S) (...)` clause. These omissions are part of the bounded
execution contract, not incomplete SQL syntax.

### Parse And Request Limits

| Item | Limit |
| --- | ---: |
| query text | 16 KiB |
| lexical tokens | 256 |
| predicates | 12 |
| values in one `IN` | 20, with no duplicates after binding |
| generated physical ranges | 32 |
| `ORDER BY` fields | 2, distinct, non-metadata integer fields |
| return projection fields | 32, distinct after canonicalization |
| named parameters | 64 distinct names, each at most 128 bytes |
| returned records | 1 to 100 per page |
| cursor token | 16 to 4,096 bytes after binding |
| ordinary string or metadata value | 1,024 bytes |
| metadata field segment | 1 to 64 UTF-8 bytes |

The partition and run-ID limits are derived from the store's physical key
limit. They are validated after binding rather than represented by a smaller
FQL-specific constant.

## Query Forms

Syntax validity does not guarantee a physical plan. A query must also match an
advertised shape and have an active bounded access path.

### Point Runs

`RETURN RECORD` is available only for `runs` and accepts exactly one `run_id`
equality, optionally paired with one `partition_key` equality. Predicate order
does not matter. It may include a run result-field projection. A missing run
returns an empty `records` array, not an error.

Omitting `partition_key` derives the run ID's automatic partition. It does not
search every partition and cannot find a run created with an explicit
partition.

### Run Collections

`RETURN RECORDS` requires exactly one `partition_key = value`, one or two
integer ordering fields, and `LIMIT 1..100`. The shipped planner supports fixed
paths for common state, type, metadata, lease, and lineage workloads plus
general active composite indexes. It may include a run result-field projection.

A state and half-open update-time query is:

```fql
FROM runs
WHERE partition_key = @partition
  AND state IN ('failed', 'completed')
  AND updated_at_ms FROM @from_ms TO @until_ms
ORDER BY updated_at_ms DESC
LIMIT 100
RETURN RECORDS
```

Two-field ordering is legal, but it needs a matching bounded index and may use
bounded top-K sorting when the index does not provide the complete order:

```fql
FROM runs
WHERE partition_key = @partition AND type = @type
ORDER BY priority DESC, updated_at_ms DESC
LIMIT 25
RETURN RECORDS
```

### Exact Counts

`RETURN COUNT` is available only for partition-scoped `runs`. It rejects
`ORDER BY`, `LIMIT`, `CURSOR`, and result fields because the result is one scalar. The planner
uses a transactional counter only when a declared count prefix represents all
predicates exactly and without overlap. Otherwise it may use an exact bounded
count scan. It never returns an estimate as a count.

```fql
FROM runs
WHERE partition_key = @partition
  AND type = @type
  AND state = @state
RETURN COUNT
```

### Event History

The `events` source is a specialized authoritative history read. It requires
exactly one `run_id` equality, allows an optional exact `partition_key`, orders
only by `event_id`, and returns records. As with point reads, omitting the
partition addresses only the automatic partition.

```fql
FROM events
WHERE run_id = @run_id
ORDER BY event_id ASC
LIMIT 100
RETURN RECORDS
```

```fql
FROM events
WHERE partition_key = @partition AND run_id = @run_id
ORDER BY event_id DESC
LIMIT 50
RETURN RECORDS
```

History records have the shape `{event_id, fields}`. Their optional projection
is event-specific: select `event_id`, the complete `fields` map, or individual
entries such as `fields['event']`.

### Lineage

Parent, root, and correlation reads use authoritative specialized access paths.
They require an exact partition and exactly one lineage equality, ordered by
`updated_at_ms`.

```fql
FROM runs
WHERE partition_key = @partition AND parent_flow_id = @parent_id
ORDER BY updated_at_ms DESC
LIMIT 50
RETURN RECORDS
```

```fql
FROM runs
WHERE partition_key = @partition AND root_flow_id = @root_id
ORDER BY updated_at_ms ASC
LIMIT 50
RETURN RECORDS
```

```fql
FROM runs
WHERE partition_key = @partition AND correlation_id = @correlation_id
ORDER BY updated_at_ms DESC
LIMIT 50
RETURN RECORDS
```

### Metadata

Simple metadata names use dotted fields. Broad state-metadata search also
requires the relevant Flow type policy to project that metadata key.

```fql
FROM runs
WHERE partition_key = @partition
  AND type = @type
  AND attribute.region = @region
ORDER BY updated_at_ms DESC
LIMIT 50
RETURN RECORDS
```

```fql
FROM runs
WHERE partition_key = @partition
  AND type = @type
  AND state_meta.review.risk_tier = @risk_tier
ORDER BY updated_at_ms DESC
LIMIT 50
RETURN RECORDS
```

Names containing dots, whitespace, quotes, or other UTF-8 characters use
bracket notation. Double a single quote inside a bracket segment:

```fql
FROM runs
WHERE partition_key = @partition
  AND attribute['customer''s.region'] = @region
ORDER BY updated_at_ms DESC
LIMIT 50
RETURN RECORDS
```

```fql
FROM runs
WHERE partition_key = @partition
  AND type = @type
  AND state_meta['review.v2']['risk tier'] = @risk_tier
ORDER BY updated_at_ms DESC
LIMIT 50
RETURN RECORDS
```

Metadata key segments beginning with `__` are reserved and cannot be queried.
A bracket-quoted state segment in `state_meta` may begin with `__`, but its
metadata-key segment may not. Unquoted segments beginning with `__` are
rejected.

### Lease Deadline

The shipped stuck-run path requires exact partition and type, state `running`,
a closed lease-deadline range, and lease-deadline order:

```fql
FROM runs
WHERE partition_key = @partition
  AND type = @type
  AND state = 'running'
  AND lease_deadline_ms BETWEEN @from_ms AND @now_ms
ORDER BY lease_deadline_ms ASC
LIMIT 100
RETURN RECORDS
```

## Fields And Types

FQL values are typed. Built-in keyword fields compare with strings; integer
fields compare with signed 64-bit integers. Metadata fields are dynamic and may
hold strings, signed integers, finite floats, or booleans when supplied by a
typed transport.

| Field | Type | Primary role |
| --- | --- | --- |
| `attempts` | integer | Run predicate; optional run ordering with a matching index. |
| `correlation_id` | keyword | Exact partition-scoped lineage lookup. |
| `created_at_ms` | integer | Run predicate, closed range or half-open time window, and ordering. |
| `event_id` | keyword | The sole `events` ordering field; not a run collection order field. |
| `lease_deadline_ms` | integer | Run predicate/range and stuck-run ordering. |
| `max_active_ms` | integer | Run predicate and optional ordering with a matching index. |
| `next_run_at_ms` | integer | Run predicate, closed range or half-open time window, and ordering. |
| `parent_flow_id` | keyword | Exact partition-scoped lineage lookup. |
| `partition_key` | keyword | Mandatory exact scope for run collections/counts; optional for point/history. |
| `priority` | integer | Run predicate and optional ordering with a matching index. |
| `root_flow_id` | keyword | Exact partition-scoped lineage lookup. |
| `run_id` | keyword | Run point identity and event-history identity. |
| `run_state` | keyword | Run predicate with a matching index. |
| `state` | keyword | Run state predicate; common fixed and composite dimension. |
| `type` | keyword | Run type predicate; common fixed and composite dimension. |
| `updated_at_ms` | integer | Default run collection/lineage order and time filter. |
| `version` | integer | Run predicate and optional ordering with a matching index. |
| `attribute.<name>` | dynamic | Flow attribute; equality, `IN`, null/missing, or a residual predicate. |
| `state_meta.<state>.<name>` | dynamic | Per-state metadata; equality, `IN`, null/missing, or a residual predicate. |

`id` in a returned run record corresponds to the query field `run_id`.
Payload, result, error, and named-value data are intentionally not query fields.

## Field Projection

Bare `RETURN RECORD` and `RETURN RECORDS` return every allowlisted field for
their source. Add a parenthesized list to return only selected fields:

```fql
FROM runs
WHERE partition_key = @partition AND run_id = @run_id
RETURN RECORD (run_id, state, version, attribute['customer'], state_meta['review']['owner'])
```

Run projections accept the run fields in the table above, including complete
`attributes` and `state_meta` maps or individual `attribute[...]` and
`state_meta[...][...]` leaves. The wire/result key for `run_id` remains `id` so
the versioned result shape does not change.

```fql
FROM events
WHERE run_id = @run_id
ORDER BY event_id ASC
LIMIT 50
RETURN RECORDS (event_id, fields['event'], fields['worker'])
```

Event projections accept `event_id`, the complete `fields` map, or individual
`fields[...]` leaves. Run fields are rejected for `events`; event-only fields
are rejected for `runs`. Operational fields such as payload references, lease
tokens, fencing state, and retention controls are never valid selectors.

Projection has these exact result semantics:

- an unrequested field is absent;
- a requested field that is missing in the record is also absent;
- a requested field present with `null` remains present with `null`;
- selected metadata leaves retain the existing nested `attributes`,
  `state_meta`, or `fields` map shape;
- selectors are unique after normalization, so dotted and equivalent bracket
  notation cannot name the same field twice; and
- selector order does not affect the returned map or cursor identity.

Projection is result shaping, not a covering-index promise. FerricStore still
authorizes the complete prepared request, validates index ownership and
generation, hydrates authoritative rows, evaluates every predicate, computes
order/cursor keys, and enforces all budgets before hiding unrequested fields.
It therefore does not reduce index entries scanned or authoritative records
hydrated. It does reduce retained winner-map data on general top-K plans,
response shaping and encoding work, response bytes, and network/client decode
work. Fixed, history, and lineage pages may briefly hold validated full rows
alongside projected rows, and that coexistence is charged to the executor
memory budget.

`EXPLAIN` exposes this as `plan.projection`: `fields` is the requested list (or
`all_allowlisted_fields`), `application` is
`after_authoritative_recheck`, and `index_only` is `false`. Cursors authenticate
the canonical selector set. Changing it invalidates a cursor; reordering the
same selectors does not.

## Predicates And Values

### Equality And IN

`=` is typed equality. `IN` is the union of up to 20 distinct typed equality
values. Multiple `IN` dimensions multiply physical ranges; a plan is rejected
before execution if it would exceed the 32-range ceiling.

Flow attribute lists use membership semantics: equality matches when the list
contains the requested scalar. Attribute normalization stores at most 16
distinct values, and an index definition may contain at most one multivalue
attribute dimension. Results from overlapping ranges or multivalue projections
are deduplicated before hydration.

### BETWEEN And Time Windows

`BETWEEN lower AND upper` includes both endpoints. `FROM lower TO upper` is a
half-open `[lower, upper)` interval and is available only for the four time
fields listed in the grammar. Bounds must have the same type and lower must not
sort after upper.

Use half-open time windows for adjacent polling intervals because a boundary
record belongs to exactly one interval. An equal half-open bound is an exact
empty query and returns without scanning an index.

### NULL And MISSING

`NULL` means the field exists with a null value. `MISSING` means the field does
not exist. Equality and ranges match neither sentinel.

```fql
FROM runs
WHERE partition_key = @partition AND attribute.reviewed_at IS NULL
ORDER BY updated_at_ms DESC
LIMIT 25
RETURN RECORDS
```

```fql
FROM runs
WHERE partition_key = @partition AND attribute.reviewed_at IS MISSING
ORDER BY updated_at_ms DESC
LIMIT 25
RETURN RECORDS
```

Concrete values sort before null, and null sorts before missing, for both
ascending and descending query order. Use `IS NULL` and `IS MISSING` directly;
they cannot be expressed as named parameter values.

### Literals And Parameters

FQL text has string and signed-integer literals. Escape a single quote by
doubling it: `'customer''s order'`. Integer literals cover the signed 64-bit
range.

Named parameters are exact: every referenced name must be present, every
provided name must be referenced, and the bound type must match. One parameter
may be reused. Parameters represent values only; source names, fields,
directions, operators, and `LIMIT` cannot be parameterized.

Typed native transports may bind:

| FQL field type | Accepted parameter values |
| --- | --- |
| keyword | binary string |
| integer | signed 64-bit integer |
| dynamic metadata | binary string, signed 64-bit integer, finite float, or boolean |
| cursor | binary string |

Textual `COMMAND_EXEC` arguments are strings. The binder parses a string as a
signed integer when the referenced built-in field requires an integer. SDKs
should prefer the typed `0x0231` request and should never construct a query by
concatenating untrusted values.

Typed comparisons do not coerce values. Integer `1`, float `1.0`, boolean
`true`, and string `'1'` are distinct. Non-finite floats are rejected. Floating
signed zero is canonicalized so `-0.0` and `0.0` have identical equality and
index semantics.

## Ordering And Pagination

Run collections require one or two distinct non-metadata integer fields in
`ORDER BY`, each with an explicit `ASC` or `DESC`. History is the exception:
it requires the single keyword field `event_id`. Count and point forms do not
accept an order clause.

FerricStore appends an opaque run identity to every run sort key. This creates
a deterministic total order when visible fields tie. Do not append `run_id` to
`ORDER BY`; it is not an integer order field and the planner already supplies
the stable tie breaker.

The first page omits `CURSOR`. When `page.has_more` is true, repeat the exact
same query and parameters while adding the returned token as a named cursor:

```fql
FROM runs
WHERE partition_key = @partition AND state = @state
ORDER BY updated_at_ms DESC
LIMIT 50
CURSOR @cursor
RETURN RECORDS
```

Cursors:

- begin with `fqc1_` and are opaque AEAD tokens;
- expire after five minutes by default;
- bind the instance, authorized scope, value-sensitive query digest, order,
  canonical result projection, index logical ID, index generation, and
  physical build ID;
- are forward-only seek positions, not offsets or snapshots;
- must not be decoded, edited, logged, or reused with changed values; and
- become invalid if their generation retires or the cursor key changes.

Live pagination is deterministic and duplicate-safe for the chosen seek order,
but it is not snapshot isolation. Concurrent writes may appear or disappear on
later pages according to where they fall relative to the cursor. Read the
`quality.pagination` value instead of assuming database snapshot semantics.

`EXPLAIN` and `EXPLAIN ANALYZE` always make a fresh plan and reject `CURSOR`.

## Result Contract

Point, history, lineage, fixed-index, and composite reads all use
`ferric.flow.query.result/v1`. Keys are shown as JSON strings below; embedded
Elixir calls may expose atom keys.

### Record Pages

`RETURN RECORD` and `RETURN RECORDS` both return a page envelope. A point result
contains zero or one item.

```json
{
  "version": "ferric.flow.query.result/v1",
  "records": [],
  "page": {
    "has_more": false,
    "cursor": null
  },
  "quality": {
    "exactness": "projected_exact",
    "freshness": "projection_watermark",
    "coverage": "complete",
    "pagination": "complete"
  },
  "usage": {
    "range_seeks": 0,
    "range_pages": 0,
    "scanned_entries": 0,
    "scanned_bytes": 0,
    "hydrated_records": 0,
    "residual_checks": 0,
    "duplicate_entries": 0,
    "result_records": 0,
    "response_bytes": 0,
    "memory_high_water_bytes": 0,
    "wall_time_us": 0
  }
}
```

The response above illustrates the envelope and field types, not measured
usage. In a real response, `usage.response_bytes` is settled to the encoded
response size and the other usage counters reflect the selected path. For the
negotiated native compact form, this is the uncompressed `0xA0` query-result
payload size; it excludes the status and frame/chunk headers.

Run records are an allowlisted structural projection:

| Returned field | Meaning |
| --- | --- |
| `id` | Run ID. |
| `type`, `state`, `run_state` | Workflow type and state information. |
| `version`, `attempts`, `priority` | Version, attempt count, and priority. |
| `partition_key` | Logical partition. |
| `created_at_ms`, `updated_at_ms`, `next_run_at_ms`, `lease_deadline_ms` | Time fields. |
| `max_active_ms` | Configured maximum active lifetime. |
| `parent_flow_id`, `root_flow_id`, `correlation_id` | Lineage identifiers. |
| `attributes`, `state_meta` | Queryable metadata maps. |

The full allowlist excludes payload/result/error bytes and references, named
values, child bookkeeping, worker and lease ownership, fencing tokens,
retention controls, parent partition keys, and unknown future storage fields.
Use the dedicated get/value APIs for those data.

An explicit projection makes each returned map sparse according to the field
semantics above. History items use only `event_id` and an event `fields` map as
defined by the Flow history contract.

### Count Results

```json
{
  "version": "ferric.flow.query.result/v1",
  "result": {"kind": "count", "value": 42},
  "quality": {
    "exactness": "projected_exact",
    "freshness": "projection_watermark",
    "coverage": "complete",
    "pagination": "none"
  },
  "usage": {
    "range_seeks": 1,
    "range_pages": 1,
    "scanned_entries": 1,
    "scanned_bytes": 64,
    "hydrated_records": 0,
    "residual_checks": 0,
    "duplicate_entries": 0,
    "result_records": 1,
    "response_bytes": 512,
    "memory_high_water_bytes": 256,
    "wall_time_us": 100
  }
}
```

The count value is non-negative and exact for the projection watermark. Count
responses have no `records` or `page` member.

### Quality

| Field | Typical values | Interpretation |
| --- | --- | --- |
| `exactness` | `authoritative`, `projected_exact`, `exact` | Whether results come from authoritative state, an exact query projection, or an exact empty proof. |
| `freshness` | `current`, `projection_watermark`, `not_applicable` | Whether the read is current or follows the asynchronous projection watermark. |
| `coverage` | `complete`, `unavailable` | Whether the admitted path covers the requested scope. Rejected EXPLAIN plans use `unavailable`. |
| `pagination` | `none`, `complete`, `authenticated_seek`, `live_seek` | No paging, terminal page, authoritative seek, or live projected seek. |

Point, history, and lineage paths are authoritative/current. Composite and
fixed collection paths are projected-exact at their projection watermark.
Command success does not wait for every asynchronous query projection to flush.

### Usage

| Field | Meaning |
| --- | --- |
| `range_seeks` | Physical index ranges opened. |
| `range_pages` | Bounded pages read across ranges. |
| `scanned_entries` | Index entries examined, including stale or duplicate entries. |
| `scanned_bytes` | Encoded index bytes charged to the scan budget. |
| `hydrated_records` | Authoritative records fetched for verification. |
| `residual_checks` | Predicate evaluations after index selection. |
| `duplicate_entries` | Overlapping index hits discarded. |
| `result_records` | Returned records, or `1` for a count scalar. |
| `response_bytes` | Final encoded query response size. |
| `memory_high_water_bytes` | Executor-accounted live memory high-water mark. |
| `wall_time_us` | End-to-end server query time against the monotonic deadline. |

### Native Binary Results

Native results are binary, not JSON. Without compact negotiation, the result
uses the protocol's generic typed map/list representation. After discovering
`flow_query_result_v1` under `OPTIONS.response_codecs`, SDKs should send
`compact_response_codecs: ["flow_query_result_v1"]` in `HELLO` or `STARTUP`.
Successful record pages and count results then use a PostgreSQL-like row-oriented
shape: result metadata is encoded once, each row has a fixed presence bitmap,
and only field values are repeated. Dynamic `attributes`, `state_meta`, and
history `fields` retain the binary-safe typed-value encoding.

The logical maps shown in this guide do not change. The SDK decoder reconstructs
`version`, `records`/`page` or `result`, `quality`, and `usage`. `EXPLAIN`, error
responses, and unknown future result shapes fall back to typed values, so a
client must retain both decoders. See [Native Protocol](native-protocol.md#compact-flow-query-results)
for the exact `0xA0` layout, enum codes, record bitmap, and validation rules.

## Reading EXPLAIN

FerricStore follows the useful PostgreSQL and SQLite documentation pattern of
showing the chosen access method, index constraints, residual filters, order
work, estimates, and alternatives. Unlike SQLite's intentionally unstable
human-debug format, `ferric.flow.explain/v1` is a versioned machine contract.

Static plan inspection:

```fql
EXPLAIN FROM runs
WHERE partition_key = @partition
  AND type = @type
  AND state = @state
ORDER BY updated_at_ms DESC
LIMIT 50
RETURN RECORDS
```

Measured bounded execution:

```fql
EXPLAIN ANALYZE FROM runs
WHERE partition_key = @partition
  AND type = @type
  AND state = @state
ORDER BY updated_at_ms DESC
LIMIT 50
RETURN RECORDS
```

`EXPLAIN ANALYZE` performs the read under normal admission, scan, hydration,
memory, response, and deadline limits. It discards records/count values and
adds the validated `actual` usage map. Its `actual.response_bytes` is the size
of the query response that would have been returned, not the EXPLAIN envelope.
Plan selection and tie-breaking are deterministic for the same prepared
request, active catalog, immutable budgets, and usable statistics. Rendered
statistics age and freshness remain time-dependent.

### Top-Level Fields

| Field | How to read it |
| --- | --- |
| `version` | Must be `ferric.flow.explain/v1`. |
| `query_fingerprint` | Value-redacted stable shape identity; do not treat it as a cursor. |
| `status` | `planned`, `executed`, or `rejected`. |
| `plan` | Selected access path, generation, constraints, residuals, scope, return kind, and limit. |
| `estimate` | Expected and hard scan/hydration/memory work plus relative cost. |
| `actual` | `null` for static/rejected plans; bounded usage after `EXPLAIN ANALYZE`. |
| `stats` | Statistics source, confidence, age, watermark, and freshness state. |
| `quality` | Result-quality promise of the selected path. |
| `bounds` | Immutable hard execution ceilings. |
| `pressure` | Per-resource estimated, hard, and actual utilization in parts per million. |
| `decision` | Cost model, candidate count, selection reason, and deterministic tie breakers. |
| `diagnostic` | Structured remediation for a rejected plan; otherwise `null`. |
| `alternatives` | Other bounded candidates and why each lost. |

### Access Paths

| `plan.path` | Meaning |
| --- | --- |
| `primary_key` | Authoritative run point lookup. |
| `history` | Authoritative event-history seek. |
| `lineage` | Authoritative parent/root/correlation seek. |
| `fixed_index` | Current specialized bounded run index. |
| `ordered_range` | One composite LMDB range. |
| `ordered_range_union` | Multiple bounded ranges combined and deduplicated. |
| `ordered_filter` | Bounded index range plus residual predicate checks. |
| `counter_lookup` | Exact transactional count-prefix read. |
| `count_scan` | Exact bounded index scan for a count. |
| `empty` | Predicate proof produces no rows without an index scan. |
| `reject` | No correct bounded plan fits the active catalog and budgets. |

`plan.order` is `native`, `bounded_top_k`, or `none`. Native order can stop
after `LIMIT + 1`. Top-K order retains at most `LIMIT + 1` records in a bounded
ordered set. It is not an unbounded SQL sort and never spills beyond the
executor budget.

`constrained_dimensions` identifies predicates used to form index ranges.
`residual_predicates` identifies predicates rechecked after hydration. Both
redact values. A residual predicate can reduce output but does not reduce the
number of candidate index entries, just as a SQL filter that is not an index
condition does not narrow its scan.

`plan.projection` reports result shaping independently of index constraints.
`index_only: false` means a narrow return list must not be interpreted as fewer
authoritative reads.

### Estimates, Bounds, And Pressure

`estimate.scanned_entries` is the planner's expected work.
`estimate.hard_scanned_entries` is the conservative maximum the selected plan
may consume. Compare the hard estimate with `bounds.scanned_entries` when
deciding whether the plan is safe; compare `actual.scanned_entries` with the
estimate when deciding whether statistics and index shape are effective.

The same expected/hard distinction applies to scanned bytes and hydration.
Planner memory is enforced internally but emitted as `null` because literal
lengths affect the value and EXPLAIN redacts literals. Executor memory is
reported.

Utilization fields end in `_ppm`: `500000` means 50% of that bound. Focus first
on `hard_limiting_resource`, then use `actual_limiting_resource` after an
analyzed run. A low relative `cost` does not override a hard-budget rejection.

### Statistics

Statistics guide candidate selection but never relax correctness checks or
hard limits. `stats.state` is:

| State | Meaning |
| --- | --- |
| `current` | Authoritative/specialized path or exact transactional counter. |
| `fresh` | A usable bounded projection sample. |
| `stale` | Sample exceeded its maximum age or has an invalid clock. |
| `unavailable` | No usable sample; hard configured ceilings drive the estimate. |

Static `EXPLAIN` may read cached statistics but does not enqueue a statistics
probe. Ordinary execution and `EXPLAIN ANALYZE` may enqueue bounded background
probes. Prefix probes read at most 257 entries and 1 MiB; a probe that does not
exhaust its range is not published as an exact count. Each prefix retains its
own observation time.

### Alternatives And Rejections

The planner chooses the lowest-cost candidate that fits all bounds, then uses
hard scan ceiling, native order, range count, and stable index identity as
deterministic tie breakers. Alternatives report cost/scan deltas and one of:
`higher_estimated_cost`, `higher_hard_scan_ceiling`,
`requires_bounded_sort`, `more_ranges`, or `stable_index_tiebreak`.

A rejected plan includes a value-redacted diagnostic with the rejected
predicate shapes, order, planner reason, bounds, suggested catalog layout, any
residual predicates, and `FLOW.QUERY.INDEXES` guidance. It does not include
partition values, query values, run IDs, physical ranges, keys, or scope
digests.

## Index Design

FerricStore composite indexes are partition-leading ordered tuples in LMDB. The
useful mental model from multi-column SQL B-trees applies: exact leading
dimensions narrow the range, then one ordered dimension can provide a range or
output order. FerricStore additionally hashes equality dimensions, proves that
every physical bound stays inside the authorized partition, and verifies every
hydrated record against the complete predicate.

An index definition has:

- a logical `id` of at most 64 restricted ASCII bytes;
- a monotonic positive `version` (called `generation` in EXPLAIN);
- source `runs`;
- two to eight distinct fields;
- optional workload labels and exact count-prefix lengths; and
- a content-derived physical `build_id`.

The first field must be `partition_key ASC HASHED`. A hashed field supports
equality/`IN`/null/missing and must be ascending. An ordered field must be a
built-in integer and can be ascending or descending. At most one attribute
field may appear because attributes can be multivalued. Definitions that could
exceed LMDB's 511-byte key ceiling are rejected before registration.

### Bundled Catalog

The OSS distribution currently bundles these five definitions:

| Logical ID | Ordered tuple | Exact count prefixes |
| --- | --- | --- |
| `flow_runs_tenant_updated` | partition, updated time descending | none |
| `flow_runs_tenant_state_updated` | partition, state, updated time descending | partition; partition + state |
| `flow_runs_tenant_type_updated` | partition, type, updated time descending | partition + type |
| `flow_runs_tenant_type_state_updated` | partition, type, state, updated time descending | partition + type + state |
| `flow_runs_tenant_type_state_lease_deadline` | partition, type, state, lease deadline ascending | none |

The planner also has current specialized bounded paths for point reads, event
history, lineage, and supported fixed Flow indexes. These are physical
operators, not additional public commands.

### Choosing A Layout

Design from a concrete query:

1. Put `partition_key` first; every collection already requires it.
2. Put exact high-value equality predicates next as hashed dimensions.
3. Put the range/order integer next with the query's desired direction.
4. Leave low-selectivity predicates as residuals only when the hard scan bound
   remains comfortably below its ceiling.
5. Add a count prefix only when every counted predicate is a non-overlapping
   hashed prefix.
6. Confirm with static EXPLAIN, then compare actual use on representative and
   adversarial distributions.

Do not create redundant prefix definitions without benchmark evidence. Every
index increases write work, backfill time, validation work, disk use, reverse
metadata, and retirement cleanup.

FQL1 does not expose `CREATE INDEX`, `DROP INDEX`, or index hints. The current
OSS catalog is a deployment-managed artifact loaded by the query-index
provider. A planner's `suggested_index` is a definition proposal for that
catalog/provider workflow, not SQL to send to the server. Catalog changes must
increase the catalog version; changing an existing logical definition also
requires a higher definition version. A definition change without a version
change, or a catalog regression, fails startup/reconciliation closed.

## Index Operations

Use the admin-only management command through native `COMMAND_EXEC`:

```json
{"command": "FLOW.QUERY.INDEXES", "args": []}
```

Filter all generations of one logical index with:

```json
{"command": "FLOW.QUERY.INDEXES", "args": ["flow_runs_tenant_state_updated"]}
```

The response contract is `ferric.flow.query.indexes/v1`. It contains no
resume cursors, physical keys, partition values, query literals, or scope
digests.

### What To Check

| Field | Healthy interpretation |
| --- | --- |
| `registry.epoch` | Monotonically changes as the durable registry snapshot changes. |
| `registry.catalog_version` | Matches the deployed catalog version. |
| `services.*` | All are `ready`; an unavailable worker explains stalled progress or stats. |
| `index.state` | `active` for a selectable generation. |
| `index.queryable` | `true` only after every shard built and validation passed. |
| `coverage.complete_shards/total_shards` | Equal before activation. |
| `coverage.validation` | `passed` for an active generation. |
| `build.current_phases` | Advances `pending -> snapshot -> backfill -> done`. |
| `validation.status` | Advances from `pending` to `passed`; investigate `failed`. |
| `validation.mismatches` | Zero. Nonzero evidence fails the candidate generation. |
| `validation.failure_reason` | `null` unless validation failed. |
| `retirement.status` | `not_applicable` for retained generations; otherwise cleanup progresses. |
| `statistics.status` | Prefer `fresh`; `missing`, `stale`, `future`, or `mixed` make estimates conservative. |

The top-level `statistics_max_age_ms` defines freshness. Summary counts are
aggregated and deliberately do not identify partition/scope samples.

### Lifecycle

```text
building -> validating -> active -> retiring -> removed
                    \-> failed -> cleaned
```

1. A writer barrier establishes a durable build fence.
2. Existing authoritative source keys are snapshotted and backfilled in small,
   replay-safe pages with durable checkpoints.
3. A second fence starts source, physical-index, exact-counter, and cleanup
   validation phases.
4. Concurrent source changes cause a retry; they do not create a false pass or
   rollback.
5. Every shard must pass before the catalog generation becomes active
   atomically.
6. A mismatch fails the full candidate generation while the previous active
   generation stays queryable.
7. Retiring/failed generations stop receiving writes, drain pinned queries,
   then delete index rows, counters, reverse rows, and staging data in bounded
   restartable phases.

Backfill and validation pause under operational pressure; retirement may still
proceed so obsolete storage can be reclaimed. A building or validating
generation receives projection writes but is never queryable. A retiring
generation is not selected for new queries, while already admitted queries pin
their exact physical build until drained.

The registry is durably written with file sync, atomic rename, and directory
sync before publishing an immutable ETS snapshot. A failed cache publication
terminates the registry process so supervision reloads the durable state rather
than serving an ambiguous snapshot.

## Errors And Troubleshooting

Structured errors always include `code`, `message`, `retryable`,
`safe_to_retry`, and `retry_after_ms`. They may also include `detail`, `hint`,
one-based `position` (`byte`, `line`, UTF-8 character `column`), and redacted
`context`. Preserve these fields in SDK exceptions.

Only `query_projection_changed` and `query_storage_unavailable` are marked safe
to retry by the current contract. Apply bounded backoff even then. Do not retry
deterministic syntax, shape, authorization, cursor, or budget errors unchanged.

| Code | Meaning and action |
| --- | --- |
| `duplicate_projection_field` | Remove the repeated result selector; dotted and equivalent bracket forms identify the same field. |
| `invalid_parameters` | `params` is not an object/map; fix the request envelope. |
| `invalid_parameter_type` | A literal or bound value has the wrong type/range; compare it with the field table. |
| `invalid_syntax` | Parsing failed; use the returned source position and hint. |
| `missing_parameter` | Add the named parameter referenced by the query. |
| `unexpected_parameter` | Remove a parameter not referenced by the query. |
| `query_too_large` | Reduce query text below 16 KiB. |
| `query_value_too_large` | Reduce the bound string/key value. |
| `unsupported_query_version` | Negotiate `FQL1` through capabilities. |
| `unsupported_source` | Use `runs` or `events`. |
| `unsupported_field` | Use `context.supported_fields` and the metadata forms in this guide. |
| `unsupported_query_shape` | Fix clauses to match one of the query forms and advertised shapes. |
| `unauthorized_scope` | Grant the required command and read-key pattern; do not reveal whether data exists. |
| `query_no_bounded_plan` | Inspect the suggested definition and `FLOW.QUERY.INDEXES`; tighten predicates or deploy/activate the required generation. |
| `query_range_budget_exceeded` | Reduce `IN` expansion or use a narrower prefix. |
| `query_scan_budget_exceeded` | Narrow indexed predicates or select a more selective index. |
| `query_scan_byte_budget_exceeded` | Narrow the range or reduce large index-entry pressure. |
| `query_hydration_budget_exceeded` | Reduce residual candidates with a better composite prefix. |
| `query_result_budget_exceeded` | Lower `LIMIT` to at most the permitted result bound. |
| `query_response_budget_exceeded` | Lower `LIMIT` or reduce returned metadata size. |
| `query_memory_budget_exceeded` | Lower `LIMIT`/range fanout or choose native index order. |
| `query_deadline_exceeded` | Narrow the query; retry only when transient load caused the deadline. |
| `query_concurrency_exceeded` | Back off with jitter; per-scope/node count or memory admission is full. |
| `query_cursor_invalid` | Restart from page one using the exact original query and values. |
| `query_cursor_expired` | Restart pagination; do not persist cursors beyond their TTL. |
| `query_cursor_too_large` | Reject the token locally; valid server cursors stay within 4 KiB. |
| `query_projection_changed` | The underlying index or visibility projection changed during the read; retry from a fresh plan/page. |
| `query_projection_limit_exceeded` | Select at most 32 result fields or use a bare return for the complete allowlist. |
| `query_storage_unavailable` | Storage, registry, or pinned generation is temporarily unavailable; retry with backoff. |
| `query_storage_inconsistent` | Integrity validation failed; stop retrying and investigate storage/projection health. |
| `query_engine_failure` | Internal provider/contract failure; do not retry in a tight loop, inspect server logs/metrics. |

### No Bounded Plan Checklist

1. Confirm exactly one `partition_key = ...` for collection/count queries.
2. Run `EXPLAIN` and read `diagnostic.context.planner_reason`.
3. Check `suggested_index.fields` and `residual_predicates`.
4. Run `FLOW.QUERY.INDEXES` and verify the required generation is active,
   queryable, fully covered, and validation-passed.
5. Check whether `IN` products exceed 32 ranges or estimates exceed a hard
   bound.
6. For `state_meta`, verify that the Flow type policy projects the searched
   metadata key.
7. Tighten the query or change the deployment catalog; repeating it unchanged
   cannot produce a different deterministic plan unless lifecycle/stats state
   changes.

### Slow Or Expensive Query Checklist

1. Compare `plan.order`; prefer `native` for frequent large pages.
2. Compare constrained dimensions with residual predicates.
3. Compare actual scan/hydration with result records. A large ratio indicates a
   weak prefix or stale entries.
4. Check `duplicate_entries` and `range_seeks` for multivalue/`IN` expansion.
5. Check statistics state/age and compare expected with actual work.
6. Lower page size when response bytes or top-K memory, rather than scan work,
   is limiting.
7. Benchmark index changes against both read gains and write/storage cost.

## Security

Query execution and plan inspection use separate command permissions:

| Mode | Required command permission |
| --- | --- |
| ordinary execution | `FLOW.QUERY` |
| `EXPLAIN` | `FLOW.QUERY.EXPLAIN` |
| `EXPLAIN ANALYZE` | both permissions |
| index status | `FLOW.QUERY.INDEXES` |

`FLOW.QUERY.EXPLAIN` is an authorization-only ACL name, not a callable wire
command. It and `FLOW.QUERY.INDEXES` belong to `@admin`, not `@read` or
`@flow`. A narrowly scoped inspector can be granted, for example,
`-@all +FLOW.QUERY.EXPLAIN %R~tenant-a:*`. An analyzed query also needs
`+FLOW.QUERY` because it executes the read.

Command authorization does not replace data authorization. The parser and
binder produce one prepared request before ACL checks, routing, planning, or
execution. The same bound partition becomes the ACL resource and routing key.
A run-ID-only point/history request derives the same automatic partition for
both checks. No later stage reparses query text.

Security properties:

- use parameters for all untrusted values; field identifiers are validated
  syntax and cannot be parameters;
- query text and parameters are fully redacted from the slow log;
- EXPLAIN and planner errors redact literals, partitions, run IDs, physical
  keys/ranges, cursor data, and scope digests;
- unsupported-field diagnostics list valid fields without reflecting the
  rejected identifier;
- returned run records use an allowlist so future internal fields do not become
  remotely visible automatically;
- every LMDB hit is partition-checked, generation-checked, matched to its
  authoritative state row, and re-evaluated against all predicates;
- cursors are authenticated and value-sensitive, preventing cross-query,
  cross-partition, cross-instance, or cross-generation replay; and
- no syntactically valid query can authorize or execute an unbounded full run
  scan.

Trusted native proxies may attach `request_context` with bounded `subject`,
`tenant`, and `scopes`. Only users frozen as trusted when the connection is
accepted can supply it; untrusted context is ignored. Trust configuration
changes apply to new connections. This context can narrow extension scope but
cannot widen the prepared query's ACL access. It is an extension/proxy context,
not a second OSS query scope; FQL still routes and authorizes by
`partition_key`.

The cursor service uses a 32-byte per-instance key. Configure one explicitly or
allow FerricStore to create a private, fsynced key file under the instance
registry directory. Back up/rotate it with the understanding that rotation
invalidates outstanding cursors.

## Performance Tuning

Start with query shape, not global limits. The following immutable defaults are
the public execution ceilings. FQL1 has no caller-supplied budget override;
internal execution contexts may enforce lower values but never higher ones:

| Budget field | Default | Operational meaning |
| --- | ---: | --- |
| `range_seeks` | `32` | Maximum generated physical ranges. |
| `scan_entries` | `50000` | Maximum index entries examined. |
| `scan_bytes` | `67108864` | 64 MiB aggregate scanned-index bytes. |
| `hydrated_records` | `50000` | Maximum authoritative rows loaded. |
| `result_records` | `100` | Maximum returned page size. |
| `response_bytes` | `524288` | 512 KiB encoded response. |
| `planner_memory_bytes` | `4194304` | 4 MiB planner/request memory. |
| `executor_memory_bytes` | `16777216` | 16 MiB live executor memory. |
| `wall_time_ms` | `750` | End-to-end monotonic query deadline. |

LMDB reads are bounded by both entry count and aggregate bytes. Native-order
plans stop after one look-ahead record. Non-native plans retain at most
`LIMIT + 1` candidates. Multi-range execution is bounded and deduplicated; it
does not pretend to be a streaming k-way merge. A provably scalar single range
avoids allocating the global deduplication set.

Use an explicit return projection when callers do not need full metadata maps,
especially for large pages. Confirm the benefit with `usage.response_bytes`
and client-side decode measurements. Do not expect `scanned_entries` or
`hydrated_records` to fall; improve the index prefix when those counters are
the limiting factor.

Admission is count- and memory-weighted:

| Scope | Default concurrent count | Default reserved memory |
| --- | ---: | ---: |
| one logical partition | 8 | 64 MiB |
| one node | 32 | 256 MiB, additionally capped relative to node memory |

Static EXPLAIN reserves planner/response memory but not executor memory.
Executable plans reserve planner + executor + response memory before reading.
Process monitors reclaim leaked leases. The deadline covers routing, cursor
authentication, planning, execution, cursor issuance, and response assembly.

For performance evidence, use `bench/flow_query_index_bench.exs` for real LMDB
projection and end-to-end reads, the open-loop and query-shape soak runners for
load behavior, and the parser/NIF/LMDB microbenchmarks under `bench/`. See
`bench/README.md` for the workload matrix, Linux profiling requirements, and
same-host median comparison rules. Publish at least three identical-hardware
runs using both representative and adversarial distributions; smoke tests are
not capacity evidence.

Evaluate another storage engine only after repeatable profiling shows LMDB is
the limiting component rather than parsing, hydration, encoding, or projection
lag. Preserve the same durability, partition proof, validation, cursor, and budget
contracts in any comparison.

## Architecture And Correctness

The query pipeline is:

```text
native envelope
  -> bounded Rust parse (Elixir differential oracle)
  -> typed named-parameter bind
  -> canonical prepared request
  -> command + partition ACL
  -> shard routing and memory/count admission
  -> mandatory-scope derivation
  -> active-index snapshot and costed bounded plan
  -> cursor authentication / physical generation pin
  -> LMDB range read + authoritative hydration + predicate recheck
  -> bounded sort/deduplication
  -> cursor issuance and versioned response settlement
```

The canonical `%Ferricstore.Flow.Query.Request{version: 1}` is created once.
Authorization, routing, shape classification, planning, cursor binding, and
execution all consume it. This shared prepared-command contract prevents a
parser/ACL/routing divergence.

The capability manifest and engine/provider choices are validated and frozen
in the immutable instance context. Runtime application-environment changes do
not replace the engine of an existing instance. Execution budgets are explicit
data passed through the request path; Raft apply does not read mutable process
configuration to decide query semantics.

### Capability Negotiation

Clients must inspect `OPTIONS.flow_query` and validate:

| Manifest field | OSS value |
| --- | --- |
| request contract | `ferric.flow.query.request/v1` |
| result contract | `ferric.flow.query.result/v1` |
| explain contract | `ferric.flow.explain/v1` |
| index-status contract | `ferric.flow.query.indexes/v1` |
| language versions | includes `FQL1` |
| capabilities | includes `flow_query_v1`, `flow_query_result_projection_v1`, `flow_explain_v1`, `flow_explain_analyze_v1`, `flow_composite_index_v1`, `flow_query_index_status_v1` |

The `shapes` array is authoritative. FQL1 grammar support does not imply that a
provider advertises or executes every classified shape. The default OSS engine
advertises:

- `runs_by_run_id_record`;
- `runs_by_partition_and_run_id_record`;
- `runs_by_partition_predicates_ordered_records`;
- `runs_by_partition_type_state_ordered_records`;
- `runs_by_partition_type_terminals_ordered_records`;
- `runs_by_partition_metadata_ordered_records`;
- `runs_by_partition_type_running_lease_deadline_ordered_records`;
- `runs_by_partition_parent_ordered_records`;
- `runs_by_partition_root_ordered_records`;
- `runs_by_partition_correlation_ordered_records`;
- `runs_by_partition_predicates_count`; and
- `events_by_run_id_ordered_records`.

SDKs must require `flow_query_result_projection_v1` before exposing typed projection
builders and must preserve sparse maps, absent fields, and present nulls. They
must discover `0x0231 FLOW.QUERY` and these contracts. For compact native
results, they must also discover `flow_query_result_v1`, request
`compact_response_codecs: ["flow_query_result_v1"]` in `HELLO` or `STARTUP`,
dispatch custom tag `0xA0`, and retain typed-value fallback for EXPLAIN and
errors. The removed beta
collection opcodes (`FLOW.LIST`, `FLOW.SEARCH`, `FLOW.TERMINALS`,
`FLOW.FAILURES`, `FLOW.STUCK`, `FLOW.BY_PARENT`, `FLOW.BY_ROOT`, and
`FLOW.BY_CORRELATION`) are unsupported, not aliases. Convenience methods may
remain only as local typed FQL builders.

### Index Correctness

Composite projection updates compare previous reverse metadata, delete stale
keys, insert current keys, update exact counters, and replace the reverse row
in one LMDB transaction. Every key ends with an opaque SHA-256 run identity.
Each range is proven to remain under its mandatory partition prefix.

Execution does not trust the index as authoritative. It validates index tuple
encoding and generation, hydrates the authoritative record, verifies physical
key ownership/version, rechecks partition scope and every predicate, and rejects
storage inconsistencies instead of returning a possibly wrong row. Overlapping
ranges and multivalue expansions deduplicate before output.

Exact counters are updated transactionally with projection keys and bind the
complete physical prefix. The planner uses them only for fully represented,
disjoint scalar predicates. Missing coverage, residuals, ranges, or overlapping
multivalue unions use an exact bounded scan instead.

The executor returns success only after all actual counters and the encoded
response fit their bounds. It never converts budget exhaustion into a partial
or truncated success.

## SQL Comparison

FQL is declarative and its planner uses concepts familiar from PostgreSQL and
SQLite, but it is not a SQL dialect.

| Concept | PostgreSQL/SQLite | FerricStore FQL1 |
| --- | --- | --- |
| data model | General tables, columns, joins, expressions | Fixed `runs` and `events` domain sources |
| projection | `SELECT` expressions/columns | Source-specific fields after `RETURN RECORD(S)`; no expressions or index-only reads |
| scope | Query may scan a table | Collections/counts require one exact partition |
| Boolean logic | General `AND`/`OR`/`NOT` | Conjunction (`AND`) plus bounded `IN` union |
| aggregation | General aggregates/grouping | Exact `RETURN COUNT` only |
| ordering | General expressions and possibly unbounded sort | One/two validated keys and bounded native/top-K work |
| pagination | Offset/keyset/cursor patterns chosen by caller | Authenticated, expiring, generation-bound seek cursor |
| indexes | Runtime DDL and optimizer catalog | Deployment-managed versioned catalog and online lifecycle |
| missing/null | SQL `NULL` model | Explicitly distinct `NULL` and `MISSING` sentinels |
| no usable index | Planner may choose a sequential scan | Query is rejected; no unbounded run scan exists |
| EXPLAIN | Human and/or machine plan formats | Stable value-redacted machine contract |
| EXPLAIN ANALYZE | Executes statement and reports actual work | Executes a read, discards data, reports bounded usage |
| statistics | Explicit/automatic analyze mechanisms | Bounded background prefix probes and transactional counters |

PostgreSQL's distinction between index conditions and post-fetch filters maps
to FQL's constrained dimensions and residual predicates. SQLite's distinction
between index-provided order and temporary sorting maps to FQL's `native` and
`bounded_top_k` order modes. FQL adds strict authorization containment,
generation pinning, and hard resource admission to those concepts.

## Further Reading

These official upstream documents were used as organization and plan-reading
references, not as compatibility specifications:

- [PostgreSQL: Using EXPLAIN](https://www.postgresql.org/docs/current/using-explain.html)
- [PostgreSQL: EXPLAIN command reference](https://www.postgresql.org/docs/current/sql-explain.html)
- [PostgreSQL: Indexes](https://www.postgresql.org/docs/current/indexes.html)
- [PostgreSQL: Statistics Used by the Planner](https://www.postgresql.org/docs/current/planner-stats.html)
- [SQLite: EXPLAIN QUERY PLAN](https://www.sqlite.org/eqp.html)
- [SQLite: Query Planning](https://www.sqlite.org/queryplanner.html)
- [SQLite: Query Optimizer Overview](https://www.sqlite.org/optoverview.html)

FerricStore-specific companion references:

- [Native Protocol](native-protocol.md) for envelopes, opcodes, and capability discovery.
- [Commands Reference](../guides/commands.md) for command-level invocation.
- [Flow Production Readiness](flow-production-readiness.md) for projection lag and operations.
- [Benchmarks](benchmarks.md) and the source checkout's `bench/README.md` for measurement methodology.
- [Security Guide](../guides/security.md) for ACL, TLS, and deployment hardening.
