# Benchmarks

FerricStore standalone benchmarks now use the Ferric protocol data plane.

Useful local runners:

| File | Purpose |
| --- | --- |
| `commands_bench.exs` | Embedded command microbenchmarks. |
| `flow_api_bench.exs` | Embedded FerricFlow API benchmark. |
| `flow_workflow_bench.exs` | Embedded workflow benchmark. |
| `flow_governance_bench.exs` | Governance command benchmark. |
| `flow_lmdb_soak.exs` | Long-running Flow/LMDB projection soak using `ferric://`. |
| `flow_state_lmdb_soak/` | Sectioned state-machine soak using `ferric://`. |

Protocol SET/GET and DBOS-style workflow benchmarks live in the Python SDK
repository under `examples/protocol_*_benchmark.py`.
