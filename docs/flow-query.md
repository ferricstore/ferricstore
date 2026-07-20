# Flow Query Architecture

`FLOW.QUERY` is the versioned, bounded read surface for Flow runs. FQL1 parses
into one canonical request type before authorization, routing, planning, or
execution. FerricStore OSS supplies the parser, stable semantics, point-read
operator, composite-index primitives, and lifecycle projection hooks.
FerricStore Enterprise installs the collection planner, online index registry,
bounded executor, cursor service, statistics, and EXPLAIN contract.

## Capability Negotiation

Clients must negotiate capabilities rather than infer collection support from
the FQL language version.

| Edition surface | Query contract | Shapes |
| --- | --- | --- |
| OSS default | `ferric.flow.query.point/v1` | exact tenant + run ID |
| Enterprise | `ferric.flow.query/v1` | exact point and bounded collection |

Both advertise `FQL1`. Enterprise additionally advertises
`flow_query_v1`, `flow_explain_v1`, and `flow_composite_index_v1`.

The capability manifest is validated and frozen in the immutable instance
context. Runtime application-environment changes cannot replace the query
engine of an existing instance.

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
ORDER BY updated_at_ms DESC, run_id DESC
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

Prefix the request with `EXPLAIN` to return a redacted physical plan instead of
records. `CURSOR` is optional and accepts a named parameter only.

Supported predicates are:

- `field = value`;
- `field IN (value, ...)`;
- `field BETWEEN lower AND upper`, inclusive at both ends;
- `time_field FROM lower TO upper`, a half-open `[lower, upper)` window;
- `field IS NULL`; and
- `field IS MISSING`.

Collection queries require exactly one `partition_key` equality. Record results
also require one or two non-metadata `ORDER BY` fields, a positive bounded
`LIMIT`, and `RETURN RECORDS`. Count results use `RETURN COUNT` and reject
ordering, limits, and cursors. A second partition predicate is rejected as
ambiguous before authorization or routing.

The parser accepts typed string and signed-integer literals plus exact named
parameters. The binder rejects missing parameters, extra parameters, and type
mismatches. Query text is limited to 16 KiB, 256 tokens, 12 predicates, 20
values per `IN`, two order fields, 64 parameters, and 100 result records.

Production native traffic uses the bounded Rust parser. The independent Elixir
parser is the differential-test oracle. Both produce
`%Ferricstore.Flow.Query.Request{version: 1}` and validate the same canonical
shape.

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
selected logical index and generation, path, order mode, redacted constraint
shapes, residual predicates, estimates, evidence age/confidence, alternatives,
and all hard bounds. It omits literal values, tenant names, run IDs, physical
keys, cursors, and scope digests. Output is deterministic for the same
catalog, statistics, and request shape.

## Execution Bounds

Enterprise defaults are:

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

Collection responses use `ferric.flow.query/v1`:

```text
%{
  version: "ferric.flow.query/v1",
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
  version: "ferric.flow.query/v1",
  result: %{kind: "count", value: non_neg_integer},
  quality: %{...},
  usage: %{...bounded counters...}
}
```

Records are an allowlisted structural projection. Payload, result, error and
named-value references, arbitrary attributes and state metadata, child
bookkeeping, lease/worker ownership, fencing tokens, retention controls, and
unknown future fields are not returned.

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

`ferricstore_enterprise/bench/flow_query_index_bench.exs` measures every launch
index through real LMDB projection and end-to-end planning/execution. It emits
read latency percentiles, write operations and logical bytes per record,
logical and physical storage growth, and backfill throughput. See the adjacent
benchmark README for representative and smoke commands.

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
