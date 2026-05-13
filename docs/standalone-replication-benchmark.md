# Standalone Replication Benchmark

Date: 2026-05-09

This benchmark compared the removed standalone Bitcask-ack write path against the normal Raft path on a single node. The standalone path used the 1 ms group-commit delay that was added for standalone fsync batching.

## Workload

- Tool: `memtier_benchmark`
- Protocol: RESP3
- Threads: 4
- Clients per thread: 50
- Requests per client: 1000
- Total operations per cell: 200,000
- Value size for SET: 100 bytes
- Key range: 20,000
- Pipeline: 1
- Shards: 4
- Data dirs: fresh per run

## Results

| Mode | Command | Ops/sec | Avg latency | p50 | p99 | p99.9 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| standalone | SET | 10,753 | 18.73 ms | 17.92 ms | 35.07 ms | 241.66 ms |
| raft | SET | 45,079 | 5.38 ms | 4.45 ms | 11.84 ms | 17.41 ms |
| standalone | GET | 187,863 | 1.06 ms | 1.04 ms | 2.61 ms | 3.10 ms |
| raft | GET | 190,802 | 1.05 ms | 1.03 ms | 1.38 ms | 3.09 ms |

Raw result files from the local run:

- `/private/tmp/ferricstore_bench_standalone_set.json`
- `/private/tmp/ferricstore_bench_standalone_get.json`
- `/private/tmp/ferricstore_bench_raft_set.json`
- `/private/tmp/ferricstore_bench_raft_get.json`

## Decision

Standalone replication mode is not worth keeping for performance. In this run, Raft SET throughput was about 4.2x higher than standalone SET throughput, while GET throughput was effectively the same. The extra standalone promotion, fsync, crash recovery, cross-shard, and command-surface complexity does not buy a product win.

## Post-Removal Raft Checks

After removing standalone replication mode, the Raft-only path was rerun locally with the same memtier shape against fresh data dirs on port 6392. The first run was a spot check; three additional fresh cycles were then run to check variance.

| Run | Command | Ops/sec | Avg latency | p50 | p99 | p99.9 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| spot check | SET | 34,047 | 5.87 ms | 4.74 ms | 14.21 ms | 20.35 ms |
| spot check | GET | 188,941 | 1.06 ms | 1.03 ms | 2.56 ms | 2.88 ms |
| run 1 | SET | 39,645 | 5.03 ms | 4.38 ms | 9.66 ms | 14.66 ms |
| run 1 | GET | 189,713 | 1.05 ms | 1.03 ms | 2.56 ms | 2.72 ms |
| run 2 | SET | 37,178 | 5.79 ms | 4.67 ms | 14.08 ms | 19.71 ms |
| run 2 | GET | 192,257 | 1.04 ms | 1.02 ms | 1.82 ms | 2.99 ms |
| run 3 | SET | 40,855 | 4.99 ms | 4.42 ms | 10.05 ms | 14.08 ms |
| run 3 | GET | 192,310 | 1.04 ms | 1.02 ms | 1.71 ms | 2.86 ms |
| runs 1-3 avg | SET | 39,226 | 5.27 ms | 4.49 ms | 11.26 ms | 16.15 ms |
| runs 1-3 avg | GET | 191,427 | 1.04 ms | 1.02 ms | 2.03 ms | 2.86 ms |

Raw rerun files:

- `/private/tmp/ferricstore_bench_raft_after_remove_set.json`
- `/private/tmp/ferricstore_bench_raft_after_remove_get.json`
- `/private/tmp/ferricstore_bench_raft_after_remove_run1_set.json`
- `/private/tmp/ferricstore_bench_raft_after_remove_run1_get.json`
- `/private/tmp/ferricstore_bench_raft_after_remove_run2_set.json`
- `/private/tmp/ferricstore_bench_raft_after_remove_run2_get.json`
- `/private/tmp/ferricstore_bench_raft_after_remove_run3_set.json`
- `/private/tmp/ferricstore_bench_raft_after_remove_run3_get.json`

GET stayed effectively the same as the original Raft GET cell. The repeated SET average was lower than the original saved Raft SET cell, but the spot check was the low outlier; local machine noise was visible across runs. The important decision remains unchanged: standalone SET was much slower than Raft SET under this workload, while GET was essentially identical.
