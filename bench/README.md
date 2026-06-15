# Benchmarks

This directory contains stable benchmark entry points for local and Azure reproduction. Public benchmark results are summarized in [docs/benchmarks.md](../docs/benchmarks.md).

Generated output should not be committed. Use temporary directories or local `bench/results/` when running experiments.

## General KV / RESP

| Script | Purpose |
| --- | --- |
| `tcp_throughput.sh` | RESP TCP throughput wrapper. |
| `tcp_bench.exs` | TCP latency/throughput microbenchmarks. |
| `resp_bench.exs` | RESP parser/router benchmark. |
| `commands_bench.exs` | Command-level benchmark coverage. |
| `router_write_bench.exs` | Router write-path benchmark. |
| `shard_bench.sh` | Shard-count benchmark helper. |

## Flow

| Script | Purpose |
| --- | --- |
| `flow_api_bench.exs` | Embedded Flow API benchmark. |
| `flow_workflow_bench.exs` | Flow workflow/state-machine benchmark. |
| `flow_lmdb_soak.exs` | Flow LMDB projection soak. |
| `flow_state_lmdb_soak.exs` | Flow state/history/LMDB soak. |

## Recovery / Cluster

| Script | Purpose |
| --- | --- |
| `durability_check.exs` | Durability smoke checks. |
| `startup_recovery_bench.exs` | Startup/recovery benchmark. |
| `cluster_throughput.exs` | Cluster throughput benchmark. |
| `haproxy.cfg` | Optional load-balancer config for cluster tests. |
| `support/resp_router_load.exs` | Shared support script for RESP router load testing. |

## Notes

- Keep benchmark scripts stable and documented.
- Keep one-off profiling probes out of the public repo.
- Keep raw run logs/results out of git unless they are intentionally summarized in `docs/benchmarks.md`.

## Local regression baseline runner

Use this when checking cleanup/refactor work against the known local baseline:

```bash
python3 bench/local_regression_baseline.py --dry-run
python3 bench/local_regression_baseline.py --start-server --suite all
```

Fast checks:

```bash
python3 bench/local_regression_baseline.py --start-server --suite memtier --memtier-test-time 10
python3 bench/local_regression_baseline.py --start-server --suite dbos --flows 100000
```

Defaults match the local baseline shape:

```text
memtier: --clients=200 --threads=4 --pipeline=50
DBOS-style Flow: --flows=1000000 --transport=many --server-shards=16
```

The runner measures RESP/memtier and Python SDK Flow paths. It does not measure the native TCP protocol; native needs a dedicated SDK/client adapter before comparison is meaningful.
