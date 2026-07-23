# Flow Query Architecture

`FLOW.QUERY` is the versioned, bounded read surface for Flow runs. FQL1 parses
into one canonical request type before authorization, routing, planning, or
execution. FerricStore OSS owns the complete query implementation: the parser,
stable semantics, cost-aware planner, point/history/fixed/composite operators,
online index registry and lifecycle, bounded executor, cursor service,
statistics, management surface, and EXPLAIN contract. FerricStore Enterprise
uses this OSS surface unchanged and adds its metadata scope and governance
integrations; it does not replace the planner or its wire contracts.

## Capability Negotiation

Clients must negotiate capabilities rather than infer collection support from
the FQL language version.

| Surface | Request contract | Result contract | Explain contract | Shapes |
| --- | --- | --- | --- | --- |
| OSS default, also used by Enterprise | `ferric.flow.query.request/v1` | `ferric.flow.query.result/v1` | `ferric.flow.explain/v1` | every advertised bounded point, history, lineage, fixed-index, composite collection, and count shape |

The shared surface advertises `FQL1`, `flow_query_v1`, `flow_explain_v1`,
`flow_explain_analyze_v1`,
`flow_composite_index_v1`, and `flow_query_index_status_v1`.

The capability manifest is validated and frozen in the immutable instance
context. Runtime application-environment changes cannot replace the query
engine of an existing instance.

The default engine advertises the full list returned by the parser's canonical
shape classifier. A client must still use that `shapes` list rather than assume
that every syntactically valid FQL1 request has a physical plan. The list
includes the general `runs_by_partition_predicates_ordered_records` and count
shapes as well as the fixed-path
`runs_by_partition_type_state_ordered_records`,
`runs_by_partition_type_terminals_ordered_records`,
`runs_by_partition_metadata_ordered_records`, and
`runs_by_partition_type_running_lease_deadline_ordered_records` shapes. A fixed
path remains a bounded fallback while a matching composite generation builds.

## FQL1 Surface

Exact point read:

```text
FROM runs
WHERE partition_key = @tenant AND run_id = @run_id
RETURN RECORD
```

Bounded collection read:

```text
FROM runs
WHERE partition_key = @tenant
  AND state IN ('failed', 'completed')
  AND updated_at_ms FROM @from_ms TO @until_ms
ORDER BY updated_at_ms DESC
LIMIT 50
CURSOR @cursor
RETURN RECORDS
```

Exact bounded count:

```text
FROM runs
WHERE partition_key = @tenant
  AND type = 'invoice'
  AND state IN ('failed', 'completed')
RETURN COUNT
```

Prefix the request with `EXPLAIN` to return a redacted physical plan without
executing it. Clients that negotiated `flow_explain_analyze_v1` may use
`EXPLAIN ANALYZE` to execute the same bounded read and return actual usage
without records or count values. Both modes run a fresh plan and reject
`CURSOR`; ordinary collection execution may use an optional named-parameter
cursor.

Supported predicates are:

- `field = value`;
- `field IN (value, ...)`;
- `field BETWEEN lower AND upper`, inclusive at both ends;
- `time_field FROM lower TO upper`, a half-open `[lower, upper)` window;
- `field IS NULL`; and
- `field IS MISSING`.

Collection queries require exactly one `partition_key` equality. Record results
also require one or two non-metadata integer `ORDER BY` fields, a positive
bounded `LIMIT`, and `RETURN RECORDS`. Stable pagination adds an implicit opaque
run-identity tie breaker, so clients must not add `run_id` to `ORDER BY`. Count
results use `RETURN COUNT` and reject ordering, limits, and cursors. A second
partition predicate is rejected as ambiguous before authorization or routing.

The parser accepts typed string and signed-integer literals plus exact named
parameters. The binder rejects missing parameters, extra parameters, and type
mismatches. Query text is limited to 16 KiB, 256 tokens, 12 predicates, 20
values per `IN`, two order fields, 64 parameters, and 100 result records.

Simple metadata names use dotted selectors such as `attribute.region` and
`state_meta.review.risk_tier`. Legal names containing dots, spaces, or quotes
use bracket selectors: `attribute['customer.region']` and
`state_meta['review.v2']['ai.model']`. A single quote inside a bracket name is
escaped as `''`. Field names are part of the validated query shape and cannot
be supplied as value parameters; predicate values should remain named
parameters.

Production native traffic uses the bounded Rust parser. The independent Elixir
parser is the differential-test oracle. Both produce
`%Ferricstore.Flow.Query.Request{version: 1}` and validate the same canonical
shape.

## Beta Command Consolidation

`FLOW.QUERY` is the only native record-collection command. The beta commands
`FLOW.LIST`, `FLOW.SEARCH`, `FLOW.TERMINALS`, `FLOW.FAILURES`, `FLOW.STUCK`,
`FLOW.BY_PARENT`, `FLOW.BY_ROOT`, and `FLOW.BY_CORRELATION` are removed, not
aliases. Their indexed physical operators remain internal implementation
details.

| Former workload | FQL1 predicates and order |
| --- | --- |
| type/state list | `partition_key`, `type`, one `state`; order `updated_at_ms` |
| attribute or state metadata search | partition plus indexed metadata equality; order `updated_at_ms` |
| terminal/failure records | partition, type, terminal state equality/`IN`; order `updated_at_ms` |
| stuck leases | partition, type, running state, bounded `lease_deadline_ms`; order `lease_deadline_ms` |
| parent/root/correlation lineage | partition plus the matching lineage ID; order `updated_at_ms` |

Native SDKs send opcode `0x0231` with `version`, `query`, and `params`. Textual
`COMMAND_EXEC` clients send `FLOW.QUERY FQL1 <query> [name value ...]`. SDKs
should remove old methods or implement them locally as typed query builders;
they must not probe or retry the removed opcodes.

## Authorization

Query execution and plan inspection use separate command permissions:

| FQL mode | Required command permission |
| --- | --- |
| ordinary execution | `FLOW.QUERY` |
| `EXPLAIN` | `FLOW.QUERY.EXPLAIN` |
| `EXPLAIN ANALYZE` | both `FLOW.QUERY` and `FLOW.QUERY.EXPLAIN` |

`FLOW.QUERY.EXPLAIN` is an authorization-only capability, not a callable wire
command. It belongs to `@admin`, not `@read` or `@flow`, and may be granted
directly when broader administrative access is inappropriate. For
example, a scoped plan-inspection account can use `-@all
+FLOW.QUERY.EXPLAIN %R~tenant-a:*`; an analysis account additionally needs
`+FLOW.QUERY`. The `FLOW.QUERY.INDEXES` permission is independently
administrative and likewise belongs to `@admin`, not `@read` or `@flow`.

Command authorization does not replace data-scope authorization. Every mode
uses the bound collection partition from the same prepared request for ACL key
checks. A run-ID-only point read derives and authorizes the deterministic
auto-partition; it cannot find an explicitly partitioned run. An
explain-only account therefore cannot inspect another permitted command's
tenant or bypass its key patterns. Authorization failures identify the missing
command permission without exposing query values.

## Actionable Diagnostics

Structured query failures keep a stable `code` and `message` and may add
`detail`, `hint`, `position`, and value-redacted `context`. Positions are
one-based and include a byte offset plus line and UTF-8 character-column
coordinates. Syntax diagnostics
point at the invalid token or the end of an incomplete query. Unsupported-field
diagnostics include the sorted built-in field list plus
documented dotted and bracket metadata forms. Neither the rejected
identifier nor literal values are copied into the response.

A `query_no_bounded_plan` failure reports redacted predicate operator/field
shapes, requested ordering, the planner rejection reason, and a concrete
suggested composite-index layout. Predicates that cannot fit that definition
are called out as residuals. Exact count suggestions identify the counter
prefix only when every predicate fits. Raw RESP errors carry the same position and useful hint text;
structured native responses retain the complete context map.

OSS exposes `FLOW.QUERY.INDEXES [index-id]` when
`flow_query_index_status_v1` is advertised. With no argument it returns the
bounded catalog; an ID filters all generations of that logical index. The
response includes registry epoch, catalog version, definition version, build
ID, fields, workloads, queryability, build/validation/retirement phase counts
and counters, validation failure evidence, service availability, and aggregated
statistics freshness. Resume cursors, physical keys, scope digests, tenant
values, and query literals are never returned.

## Stable Semantics

All comparisons are typed. Integers do not compare equal to floats, and
booleans do not compare equal to integers. Finite floating signed zero is
canonicalized so `-0.0` and `0.0` have identical equality and index semantics.
Non-finite floats are rejected.

`NULL` and `MISSING` are distinct sentinels. Ordinary equality and range
predicates do not match either sentinel; callers must use `IS NULL` or
`IS MISSING`. Concrete values sort first, then null, then missing, in both
ascending and descending order.

`BETWEEN` is closed. Time windows are half-open. Attribute lists use membership
semantics for equality and `IN`. Flow attribute normalization limits a list to
16 distinct values; composite projection also fails closed before an
unsupported projection cardinality can allocate an unbounded Cartesian
product.

Sort keys append an opaque SHA-256 run identity, producing a deterministic
total order without exposing record IDs in physical index keys. Cursors bind
the full value-sensitive query digest, instance, tenant, index generation, and
order, so a token cannot be replayed against another query or tenant.

## Physical Indexes

The launch catalog contains four tenant-leading indexes:

| Logical ID | Tuple |
| --- | --- |
| `flow_runs_tenant_updated` | tenant, updated time descending |
| `flow_runs_tenant_state_updated` | tenant, state, updated time descending |
| `flow_runs_tenant_type_updated` | tenant, type, updated time descending |
| `flow_runs_tenant_type_state_updated` | tenant, type, state, updated time descending |

Tenant, state, and type components are SHA-256 keyed equality dimensions.
Ordered integer components use a prefix-free, lexicographically sortable tuple
encoding. Every physical key ends with an opaque run identity and remains
within LMDB's 511-byte key limit. The first component must be tenant and every
planner-produced bound is verified to remain under that tenant prefix.

Each record has bounded reverse metadata listing its composite keys. Projection
updates compare the previous reverse value, delete stale keys, insert current
keys, and replace the reverse row in one LMDB transaction. This prevents a
concurrent update from silently leaving an inconsistent generation.

Selected hashed prefixes also maintain exact unsigned counters in that same
transaction. Counter values bind the complete physical prefix, and reads batch
at most the range-seek ceiling. The planner uses them only for fully represented,
disjoint scalar predicates. Missing coverage, residual predicates, ranges, and
overlapping multivalue unions stay on the bounded scan path.

The representative state/type plus half-open time-window query becomes one
LMDB range. `IN` expands to at most 32 ranges. Because the executor does not
implement a streaming k-way merge, multi-range plans are explicitly costed and
executed as bounded top-K; they are never mislabeled as a native merge.

## Planner And Statistics

The planner considers only fully covered, validation-passed active indexes.
It constructs candidate ranges, proves tenant containment, estimates scan and
hydration work, charges sort and simultaneous page/hydration memory, rejects
over-budget candidates, and applies a deterministic logical-ID/generation tie
break.

Statistics are advisory. Missing, stale, future-dated, or tenant-mismatched
statistics use the configured hard runtime ceilings. Exact prefix probes read
at most 257 entries and 1 MiB; a non-exhausted probe is not published as an
exact count. Each prefix count carries its own observation time, so probing one
prefix cannot refresh an unrelated stale count. The cache and probe queue are
both bounded.

The executor remains authoritative when estimates are wrong. It never returns
a partial success after exhausting a scan, byte, hydration, memory, response,
or wall-time budget.

`EXPLAIN` uses the same bound request and planner as execution. It includes the
selected logical index, generation, and build ID; path and order mode; redacted
constraint shapes and residual predicates; normalized estimates; statistics
freshness; hard bounds; mandatory-scope enforcement without scope values;
resource pressure; the selection reason; and bounded alternative comparisons.
Pressure distinguishes expected utilization from the conservative hard
execution ceiling. Rejected plans carry the same actionable diagnostic as
execution, including a suggested index and `FLOW.QUERY.INDEXES` guidance.

`EXPLAIN ANALYZE` additionally executes that admitted plan under the normal
scan, byte, hydration, result, response, memory, and deadline ceilings. Its
`actual` object is the validated shared result usage map for the
discarded query response; `actual.response_bytes` therefore measures the query
response that would have been returned, not the EXPLAIN envelope. The pressure
table adds actual utilization and the actual limiting resource. Planner-memory
estimates remain `null` externally because encoded literal lengths affect that
internal enforcement value. Static `EXPLAIN` does not enqueue statistics probes;
ordinary execution and `EXPLAIN ANALYZE` may refresh missing statistics through
the bounded background worker.

Both modes omit literal values, tenant names, run IDs, records, count values,
physical keys, cursors, and scope digests. Static output is deterministic for
the same catalog, statistics, and request shape. The beta
`ferric.flow.explain/v1` contract was updated in place: external estimate and
bound names use `scanned_entries` and `scanned_bytes`, and index objects now
include `build_id`.

## Execution Bounds

Default limits are:

| Resource | Default |
| --- | ---: |
| range seeks | 32 |
| scanned entries | 50,000 |
| scanned bytes | 64 MiB |
| hydrated records | 50,000 |
| result records | 100 |
| response bytes | 512 KiB |
| planner memory | 4 MiB |
| executor memory | 16 MiB |
| wall time | 750 ms |

LMDB reads are page-bounded by entry count and bytes. Every decoded index row is
tenant checked, version matched against the authoritative state row, verified
as a physical key owned by that row, and re-evaluated against every predicate.
Overlapping ranges and multivalue projections deduplicate before hydration. A
provably scalar single range avoids the global ID set. The executor accounts for
the live range page while hydrated rows and retained top-K records coexist.
Exact response-size settlement happens once at the final response boundary.

Native-order plans stop after one look-ahead result. Non-native plans retain at
most `LIMIT + 1` records in a bounded ordered set. There is no implicit full
scan, unbounded sort, or truncating success fallback.

Execution is protected by per-tenant and per-node concurrency admission. A
monotonic deadline covers routing, planning, execution, cursor creation, and
response assembly. Process monitors reclaim leaked admission leases.

## Cursor And Response Contract

All editions use `ferric.flow.query.result/v1` for collection responses:

```text
%{
  version: "ferric.flow.query.result/v1",
  records: [...],
  page: %{has_more: boolean, cursor: binary | nil},
  quality: %{
    exactness: binary,
    freshness: binary,
    coverage: binary,
    pagination: binary
  },
  usage: %{...bounded counters...}
}
```

Count responses use the same version, quality, and usage envelope with no page
or record array:

```text
%{
  version: "ferric.flow.query.result/v1",
  result: %{kind: "count", value: non_neg_integer},
  quality: %{...},
  usage: %{...bounded counters...}
}
```

Records are an allowlisted structural projection. Attributes and state metadata
are returned; payload, result, error and named-value references, child
bookkeeping, lease/worker ownership, fencing tokens, retention controls, and
unknown future fields are not returned. Point, fixed-index, and composite
execution all return the same versioned result envelope; there is no
edition-specific or bare-list response contract.

Cursors are opaque `fqc1_` AEAD tokens with a default five-minute TTL. The
32-byte key is configured per instance or created once as a private, fsynced
file under the instance registry directory. SDKs must not decode, edit,
persist beyond expiry, or reuse cursors with changed predicates, parameters,
order, tenant, or instance.

SDKs must preserve structured query error codes, especially unsupported
version/shape, unauthorized scope, no bounded index, budget exhaustion,
deadline, concurrency, invalid/expired cursor, projection change, storage
inconsistency, and storage unavailability. Retrying a deterministic shape or
budget rejection without changing the request is not useful.

## Online Lifecycle

The index catalog is versioned and durable. Logical IDs are unique; a changed
definition requires a new generation. Registry state is persisted with file
sync, atomic rename, and directory sync before an immutable ETS snapshot is
published.

The lifecycle is:

```text
building -> validating -> active -> retiring -> removed
                    \-> failed -> cleaned
```

1. A writer barrier establishes a durable build fence.
2. Existing authoritative state keys are staged and backfilled in bounded,
   replay-safe pages with durable checkpoints.
3. A second fence starts source and physical-index validation passes.
4. Validation re-reads state and index rows around each comparison. Concurrent
   changes cause retry, not false activation or rollback.
5. All shards complete validation before the whole catalog generation becomes
   active atomically.
6. A mismatch fails the entire candidate generation while the previous active
   generation remains queryable.
7. Retiring or failed generations leave write projection, pass a writer fence,
   delete index rows and exact counters, scrub reverse rows, and remove staging
   data in bounded, restartable phases.

No partial generation is queryable. A post-persist cache-publication failure is
fatal to the registry process so supervision reloads the already durable
transition. A 16-entry catalog plus one full retiring generation is bounded to
32 registry entries; removed failed generations release their slot after
cleanup.

## Performance Evidence

`bench/flow_query_index_bench.exs` measures every launch
index through real LMDB projection and end-to-end planning/execution. It emits
read latency percentiles, write operations and logical bytes per record,
logical and physical storage growth, and backfill throughput. See the adjacent
benchmark README for representative and smoke commands.

The OSS repository also provides open-loop and query-shape soak runners, parser
allocation and Criterion suites, plus
NIF scheduler, native ordered-index, and LMDB cache benchmarks under `bench/`.
See `bench/README.md` for the full matrix, Linux profiling requirements, and
the same-host 15% median regression comparison.

Publish release evidence from at least three runs on identical hardware and
durability settings, using both a representative production distribution and a
high-cardinality/adversarial distribution. Compare medians; do not treat a
small smoke run as capacity evidence.

LMDB remains the storage engine while bounded range reads and the single-writer
projection model meet the product SLO. Open a RocksDB evaluation only when a
repeatable launch-index benchmark demonstrates at least one of these conditions
at target load:

- read p95 misses its SLO by at least 20%, and profiling attributes at least
  40% of query wall time to LMDB rather than parsing, hydration, or encoding;
- LMDB writer utilization stays above 70% for 15 minutes or its queue p99
  exceeds 100 ms;
- a resumable backfill cannot finish within half of the allowed maintenance
  window, leaving no safety margin; or
- physical index growth exceeds 2.5 times logical index bytes after compaction
  and normal reclaim operations.

Adopt another engine only if an equivalent prototype, with the same durability
and correctness checks, improves the triggering metric by at least 20% across
both datasets and does not regress write amplification, storage, or non-target
latency by more than 10%. This keeps an engine migration evidence-driven rather
than architectural speculation.
