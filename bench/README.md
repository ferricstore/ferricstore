# Benchmarks

FerricStore standalone benchmarks use the Ferric native protocol data plane.

Useful local runners:

| File | Purpose |
| --- | --- |
| `commands_bench.exs` | Embedded command microbenchmarks. |
| `flow_api_bench.exs` | Embedded FerricFlow API benchmark. |
| `flow_workflow_bench.exs` | Embedded workflow benchmark. |
| `flow_governance_bench.exs` | Governance command benchmark. |
| `fql_parser_bench.exs` | Rust FQL parser and parse/bind microbenchmark. |
| `flow_lmdb_soak.exs` | Long-running Flow/LMDB projection soak using `ferric://`. |
| `flow_state_lmdb_soak/` | Sectioned state-machine soak using `ferric://`. |

Native-protocol SET/GET and DBOS-style workflow benchmarks live in the Python
SDK repository.

Enterprise composite-index read latency, write amplification, storage growth,
and backfill throughput are measured by
`../ferricstore_enterprise/bench/flow_query_index_bench.exs` for every launch
index.
