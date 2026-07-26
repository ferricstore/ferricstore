# Benchmarks

FerricStore standalone benchmarks use the Ferric native protocol data plane.

Useful local runners:

| File | Purpose |
| --- | --- |
| `commands_bench.exs` | Embedded command microbenchmarks. |
| `flow_api_bench.exs` | Embedded FerricFlow API benchmark. |
| `flow_workflow_bench.exs` | Embedded workflow benchmark. |
| `flow_governance_bench.exs` | Governance command benchmark. |
| `fql_parser_bench.exs` | Rust NIF boundary, wrapper, binder, and explain-planner benchmark. |
| `fql_scheduler_bench.exs` | Normal-scheduler saturation and heartbeat tail-latency benchmark. |
| `flow_query_native_index_bench.exs` | Native ordered-index scale, paging, skew, fanout, and contention benchmark. |
| `flow_query_lmdb_bench.exs` | LMDB warm, reopened, evicted-cache, range, and hydration benchmark. |
| `flow_query_index_bench.exs` | End-to-end launch-index projection, planning, execution, cursor, response, storage, and backfill benchmark. |
| `flow_query_soak.exs` | Open-loop query capacity and online-backfill soak. |
| `flow_query_shape_soak.exs` | Correctness and performance soak across every launch index and physical query shape. |
| `query_performance_compare.exs` | Same-host JSON median and memory regression gate. |
| `query_performance_criterion_export.exs` | Criterion median exporter for the shared regression gate. |
| `query_performance_linux_profile.sh` | Linux Criterion, allocation, perf, flamegraph, and end-to-end runner. |
| `query_planner_lmdb_candidates_bench.exs` | Compact query-row codec and bounded deferred-expiry candidates. |
| `query_planner_lmdb_multishard_candidates_bench.exs` | Sequential/parallel shard reads and sort/heap merge candidates. |
| `query_planner_merge_candidates_bench.exs` | Auto-partition and lineage merge candidates. |
| `query_planner_lmdb_catalog_candidates_bench.exs` | Catalog/backfill transaction-fusion lower bound. |
| `query_planner_core_read_candidates_bench.exs` | Durable counts, selective continuations, bounded state merge, and short-key candidates. |
| `query_planner_composite_codec_candidates_bench.exs` | Composite entry and reverse-value codec candidates. |
| `query_planner_projection_candidates_bench.exs` | Counter writes, exact reverse-row prefetch, and projection page-size candidates. |
| `query_planner_covering_index_candidates_bench.exs` | Production covering-index reads versus primary-row hydration. |
| `query_covering_write_cost_bench.exs` | Launch-catalog covering write cost and compact cover codec comparison. |
| `query_planner_residual_candidates_bench.exs` | Direct, fetch-once, and prepared residual predicate evaluation candidates. |
| `query_planner_intersection_candidates_bench.exs` | Single-index hydration and bounded multi-index intersection candidates. |
| `query_result_codec_candidates_bench.exs` | Compact result-record ordering candidates across sparse and full pages. |
| `query_row_codec_bench.exs` | QueryRow encode/decode, batch/reference decode, and compact result encode costs by row shape and page size. |
| `query_tuple_codec_candidates_bench.exs` | Production tuple comparison and binary encoding versus the previous paths. |
| `query_planner_native_read_candidates_bench.exs` | Reference-vs-production fused composite and source-aware shard reads. |
| `apply_projection_retention_bench.exs` | Previous unbounded compaction retention versus the production hybrid spill collector. |
| `query_storage_layout_bench.exs` | Previous duplicated-record LMDB layout versus compact QueryRow storage and authoritative-log hydration. |
| `flow_lmdb_soak.exs` | Long-running Flow/LMDB projection soak using `ferric://`. |
| `flow_state_lmdb_soak/` | Sectioned state-machine soak using `ferric://`. |

Native-protocol SET/GET and DBOS-style workflow benchmarks live in the Python
SDK repository.

Composite-index read latency, write amplification, storage growth, and backfill
throughput are measured in OSS by `flow_query_index_bench.exs` for every launch
index. The open-loop and shape-matrix runners exercise sustained load and broad
bounded plans through the same production modules.

## Query performance

The query parser and planner cross three performance boundaries. Criterion
measures the pure Rust FQL parser without BEAM encoding. `fql_parser_bench.exs`
then separates NIF term construction, Elixir request decoding, binding, and an
explain-planner call. The native index and LMDB runners measure physical
operators through the production NIF API. Each runner performs untimed
correctness preflight before collecting samples; a malformed workload or wrong
operator result fails instead of being reported as a fast benchmark.

Compile and run the deterministic parser allocation ceilings:

```bash
cargo bench \
  --manifest-path apps/ferricstore_server/native/native_protocol_nif/Cargo.toml \
  --bench fql_parser --bench fql_allocations --no-run

BENCH_ALLOC_ITERATIONS=10000 cargo bench \
  --manifest-path apps/ferricstore_server/native/native_protocol_nif/Cargo.toml \
  --bench fql_allocations
```

Run a short end-to-end smoke matrix:

```bash
BENCH_WARMUP=0.1 BENCH_TIME=0.5 BENCH_MEMORY_TIME=0 \
  MIX_ENV=bench mix run --no-start bench/fql_parser_bench.exs

BENCH_CONCURRENCY=1,4 BENCH_DURATION_MS=1000 \
  MIX_ENV=bench mix run --no-start bench/fql_scheduler_bench.exs

BENCH_CARDINALITIES=1000 BENCH_WARMUP=0.1 BENCH_TIME=0.5 \
  BENCH_MEMORY_TIME=0 BENCH_CONTENTION_DURATION_MS=1000 \
  MIX_ENV=bench mix run --no-start bench/flow_query_native_index_bench.exs

BENCH_LMDB_ENTRIES=1000 BENCH_LMDB_VALUE_BYTES=128 \
  BENCH_WARMUP=0.1 BENCH_TIME=0.5 BENCH_MEMORY_TIME=0 \
  MIX_ENV=bench mix run --no-start bench/flow_query_lmdb_bench.exs
```

Run the query-planner optimization gates independently of production code:

```bash
BENCH_CANDIDATE_SECTION=codec-shapes BENCH_WARMUP=0.1 BENCH_TIME=0.75 \
  BENCH_MEMORY_TIME=0 MIX_ENV=bench mix run --no-start \
  bench/query_planner_lmdb_candidates_bench.exs

BENCH_WARMUP=0.1 BENCH_TIME=0.75 BENCH_MEMORY_TIME=0 \
  MIX_ENV=bench mix run --no-start \
  bench/query_planner_lmdb_multishard_candidates_bench.exs

BENCH_WARMUP=0.1 BENCH_TIME=0.75 BENCH_MEMORY_TIME=0 \
  MIX_ENV=bench mix run --no-start bench/query_planner_merge_candidates_bench.exs

MIX_ENV=bench mix run --no-start \
  bench/query_planner_lmdb_catalog_candidates_bench.exs

BENCH_CANDIDATE_SECTION=prefetch BENCH_WARMUP=0.1 BENCH_TIME=0.75 \
  BENCH_MEMORY_TIME=0 MIX_ENV=bench mix run --no-start \
  bench/query_planner_projection_candidates_bench.exs

BENCH_CANDIDATE_SECTION=projection-batch BENCH_PROJECTION_BATCH_RECORDS=1,64,512 \
  BENCH_WARMUP=0.1 BENCH_TIME=0.75 BENCH_MEMORY_TIME=0 MIX_ENV=bench \
  mix run --no-start bench/query_planner_projection_candidates_bench.exs

BENCH_CANDIDATE_SECTION=composite BENCH_WARMUP=0.1 BENCH_TIME=0.75 \
  BENCH_MEMORY_TIME=0 MIX_ENV=bench mix run --no-start \
  bench/query_planner_native_read_candidates_bench.exs

BENCH_STORAGE_RECORDS=10000 BENCH_STORAGE_RECORDS_PER_LOG_ENTRY=64 \
  BENCH_WARMUP=0.1 BENCH_TIME=0.5 BENCH_MEMORY_TIME=0 \
  MIX_ENV=bench mix run --no-start bench/query_storage_layout_bench.exs
```

These candidate runners perform result-equivalence preflight before timing.
The compact codec additionally checks long IDs, nil states, discovery-backed
rows, and rejection of a discovery component inconsistent with its key digest.
Treat a candidate as implementation evidence only after three sequential runs
on the same idle host clear the 15% median threshold for its intended workload
without regressing the smaller workload. A Linux cold-cache change still needs
the Linux profiling runner below; warm macOS results are not a substitute.

The accepted production set is intentionally narrower than the candidate set:
compact/fused composite rows, source-aware bounded shard merge, a bounded
k-way state merge, exact durable counts, 64-record projection pages, and
batched reverse-row reads. Production covering rows avoid primary hydration for
eligible sparse projections and counts. The native production harness measured
2.4-7.9x read improvement across 1-100 row pages and 128-byte to 4KiB source
records. Across all five launch indexes, compact covers added 17% logical index
bytes and about 14.7% median projection time; the compact codec itself encoded
1.84x and decoded 1.58x faster than the previous ETF envelope. Large decoded
fields are detached only when needed so selected sub-binaries cannot retain an
unaccounted 64KiB cover.

Eligible native-order metadata pages also fuse the composite scan and QueryRow
lookup into one scope-bound LMDB read transaction. The retained paired runner
measured 6.0-48.5% lower storage-operator p50 across 1, 25, and 100-row pages
with 256-byte and 4KiB QueryRows. In the complete 25-row planner/executor path,
the fused read improved p50 by about 2% beyond the separately measured
prepared-key path; the combined change cleared 7.7% against the pre-change run.

`query_row_codec_bench.exs` locks codec equivalence before measuring sparse,
typical, and metadata-heavy rows at 1, 25, and 100-row page sizes. Paired runs
against the exact pre-change module measured typical QueryRow decode about
17-21% faster and encode about 8-10% faster. For metadata-heavy pages, omitting
unused dynamic sections improved 100-row decode about 50% with about 38% less
allocation. Selecting one attribute and one state-metadata field remained about
19% faster with about 11% less allocation than materializing the full attribute
section alone. Encoding a negotiated compact result once removes the separate
measurement traversal, which accounted for about 23-30% extra work on typical
25 and 100-row pages. Batch-decoder specialization, repeated-plan-validation
elision, and nested metadata assembly measured below the acceptance threshold
and were not added to the production path.

Prepared residual predicates avoid repeated field lookup and use precomputed
exact-type membership sets. A corrected production-vs-legacy paired run
improved equality filters by 21-42%, mixed filters by 4.29x, and 20-value `IN`
filters by 13.26-29.6x. Direct tuple comparison improved 10,000-value batches by
4.7-8.5x in a short production-vs-legacy run. The thresholded bulk binary
encoder remains on the per-byte path below 32 bytes and improved 32-512 byte
inputs by roughly 1.3-12.8x without a relevant small-input regression.

Counter-operation coalescing, synchronous parallel shard reads, front-coded
reverse writes, cached LMDB handles, and catalog transaction fusion did not
clear the gate and remain unimplemented. Short
durable index IDs reduce storage but did not improve latency enough to justify
the added catalog lookup. Reusing one statistics-scope digest across planner
candidates reduced allocations slightly but did not improve 1-, 8-, or
32-index planner latency, so the simpler per-candidate validation path remains.
Size-aware result-field ordering improved dense rows but produced an unstable
large sparse-page regression, so compact result encoding keeps its single sort
path. Streaming iodata regressed the relevant 512KiB response envelope and only
won above the product response ceiling, so compact results still flatten once
before framing. Covering plans retain the full executor admission reservation:
version-1 index rows, oversized cover fallbacks, and expiring rows can still
require primary-record hydration, so a projection-sized reservation would not
be a hard bound. Hydration batches already use a byte-bounded LMDB prefix read
that returns the largest fitting prefix; an additional adaptive batch-size
controller would duplicate that contract without reducing reads.
Skipping cover decoding when a plan still required authoritative hydration
improved stable paired medians by only 1-14% across 25- and 100-row pages with
128-byte and 4KiB source records. It did not reproducibly clear the 15% gate,
so the extra production reader branch was removed; the paired lower-bound
candidate remains in `query_planner_covering_index_candidates_bench.exs`.
Automatic multi-index intersection is also excluded: it improved sparse-overlap
synthetic shapes but regressed 1.5-2.7x for highly correlated indexes and about
30x when the best single index was already selective. The available statistics
do not measure cross-index correlation, while the launch catalog already has
the useful state/type composites, so the planner cannot choose intersection
reliably without joint statistics.
Selectivity-aware continuation remains benchmark
evidence only until RAM and LMDB share a cursor contract; it is not implemented
with offset rescans.

The storage-layout benchmark includes the shared exact-type source catalog in
both layouts. At 10,000 records and 64 records per authoritative frame, the
compact layout reduced physical LMDB bytes by about 23.0% for sparse records,
35.5% for typical records, and 6.0% for metadata-heavy records. Logical LMDB
bytes fell by 27.9% and 7.36% for the typical and metadata-heavy shapes. Sparse
logical bytes grew by 15.54% because a nearly payload-free source record is
smaller than the query-visible metadata plus its checksum-bearing physical
locator; allocator and page effects still made the production LMDB smaller at
scale.

Two hydration candidates remain rejected. Retaining a direct inline-record
decode path did not reproducibly clear the 15% gate for the typical 100-row
workload. Packing 256 instead of 64 records per authoritative frame improved
that workload by only about 13% while materially regressing one-row sparse
reads. A retained-descriptor reader saved only roughly 15-20 microseconds over
the production pooled disk reader at 100 rows. Neither result justifies an
additional read path or a larger production frame.

The full native-index default includes 1K, 100K, and 1M entries, page sizes 1,
25, 100, and 4096, forward/reverse/cursor/deep-offset reads, duplicate scores,
hot and uniform partitions, 1-256-key claim fanout, and concurrent readers and
writers. Setup throughput is emitted alongside contention latency. The
scheduler runner reports missed heartbeat deadlines without replaying missed
ticks into the measured workload. `BENCH_CARDINALITIES` narrows that set; do
not use a smoke matrix as release capacity evidence.

LMDB defaults cover 128-byte, 4KiB, and 1MiB values while bounding each dataset
with `BENCH_LMDB_MAX_DATASET_BYTES`. Raise that budget and
`BENCH_LMDB_ENTRIES` to create a dataset larger than RAM. A real cold-cache run
requires Linux plus `vmtouch`; `BENCH_REQUIRE_COLD_CACHE=1` makes missing cache
eviction support fail instead of silently reporting reopened-cache numbers as
cold. Successful hydration inputs are capped at the 64MiB response budget; a
separate `budget + 1` workload measures and validates deterministic rejection.
LMDB map sizes use the host page size, and setup time plus logical/physical
bytes are included in the JSON evidence.

All Benchee runners accept `BENCH_WARMUP`, `BENCH_TIME`,
`BENCH_MEMORY_TIME`, `BENCH_REDUCTION_TIME`, and `BENCH_PARALLEL`. Set
`BENCH_SAVE=/path/to/results` to emit comparable JSON containing median,
p95/p99, memory, reductions, throughput, logical bytes, and physical bytes.
Compare paired runs made on the same host with:

```bash
BENCH_REGRESSION_LIMIT=0.15 MIX_ENV=bench mix run --no-start \
  bench/query_performance_compare.exs baseline-results current-results
```

The comparator records OS, architecture, CPU model, OTP, Elixir, and online
scheduler count and rejects mismatched systems. Set
`BENCH_ALLOW_SYSTEM_MISMATCH=1` only for exploratory comparisons where hardware
noise is explicitly acceptable.

`.github/workflows/query-performance.yml` performs five paired rounds on one
Linux runner. A regression gates only when all five same-round ratios exceed
the budget, a one-sided sign test with `p < 0.05`; noisy or order-dependent
slowdowns remain visible in the artifact without blocking a release. The normal
test workflow only compiles the benchmark binaries and runs deterministic
allocation ceilings; noisy wall-clock thresholds are not used as PR tests.

For Linux hardware-counter and cache evidence, install `vmtouch`, `perf`, and
`cargo-flamegraph`, then run:

```bash
BENCH_REQUIRE_PROFILERS=1 bench/query_performance_linux_profile.sh
```

The Linux runner stores Criterion estimates under its output directory and
exports them into the same JSON comparison schema as the Elixir suites.
