# Benchmarks

This page keeps only the latest public benchmark summaries. Raw benchmark logs and one-off profiling runs are intentionally not committed.

Use these numbers as reproducible reference points, not universal hardware claims. Throughput and latency depend on VM type, local NVMe availability, shard count, client concurrency, pipeline depth, payload size, and resource guards.

## FerricFlow: latest Azure runs

Workload shape:

```text
1,000,000 flows
single FerricStore server VM
single Python SDK client VM
Flow queue/workflow workers
live mode: create and process run together
preloaded mode: create first, then process
```

The best balanced 16-vCPU server result was with 32 Flow shards.

| Mode | API shape | Server shards | Create rate | Process/complete rate | End-to-end rate |
| --- | --- | ---: | ---: | ---: | ---: |
| Sync live | Queue worker | 32 | - | - | 53,790 flows/s |
| Sync live | Workflow worker | 32 | - | - | 54,060 workflows/s |
| Async live | Queue worker | 32 | 95,896 flows/s | - | 45,608 flows/s |
| Async live | Workflow worker | 32 | 97,196 workflows/s | - | 47,888 workflows/s |

Preloaded 16-vCPU runs:

| API shape | Server shards | Create rate | Process/complete rate | End-to-end rate |
| --- | ---: | ---: | ---: | ---: |
| Queue worker | 32 | 99,645 flows/s | 125,161 flows/s | 55,478 flows/s |
| Workflow worker | 32 | 99,892 workflows/s | 101,080 workflows/s | 50,241 workflows/s |

### Server CPU scale

These runs used default server behavior and live 1M-flow workloads.

| Server size | Sync queue | Sync workflow | Async queue | Async workflow |
| ---: | ---: | ---: | ---: | ---: |
| 4 vCPU | 15,854/s | 16,005/s | failed under write timeout | failed under write timeout |
| 8 vCPU | 30,113/s | 27,674/s | 23,882/s | 24,712/s |
| 16 vCPU | 46,964/s | 45,375/s | 41,131/s | 41,121/s |

### 16-vCPU shard sweep

Sync live runs:

| Server shards | Queue end-to-end | Workflow end-to-end |
| ---: | ---: | ---: |
| 16 | 46,964/s | 45,375/s |
| 24 | 51,644/s | 51,977/s |
| 32 | 53,790/s | 54,060/s |
| 64 | 54,287/s | 53,736/s |

Async live runs:

| Server shards | Queue create | Queue end-to-end | Workflow create | Workflow end-to-end |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 86,892/s | 41,131/s | 90,504/s | 41,121/s |
| 32 | 95,896/s | 45,608/s | 97,196/s | 47,888/s |
| 64 | 96,219/s | 43,997/s | 95,195/s | 45,137/s |

Interpretation: 32 shards was the best balanced setting in these Azure runs. 64 shards slightly improved queue-only sync throughput, but 32 shards was better for the workflow mix.

## KV SET/GET: latest Azure local-NVMe runs

Environment:

```text
server: Azure Standard_L4as_v4, 4 vCPU
client: Azure Standard_D2as_v4
storage: local NVMe, ext4, noatime/nodiratime, scheduler=none
protocol: Ferric protocol over TCP
value size: 256 bytes
client load: memtier, 200 connections, pipeline 30, 4 threads, 30 seconds
```

| Operation | Durability/read mode | Throughput | p50 latency | p99 latency | p99.9 latency |
| --- | --- | ---: | ---: | ---: | ---: |
| SET | Quorum durable write | 175,650 ops/s | 129 ms | 338 ms | 391 ms |
| SET | Async write | 267,935 ops/s | 59 ms | 414 ms | 532 ms |
| GET | Quorum read | 592,684 ops/s | 37 ms | 125 ms | 194 ms |
| GET | Async read | 387,057 ops/s | 36 ms | 200 ms | 11,403 ms |

The async-read p99.9 had a large outlier in this run. Treat p50/p99 as the useful signal for that row until the run is repeated.

A higher-pipeline quorum SET run on the same 4-vCPU NVMe setup reached:

| Operation | Client load | Throughput | p50 latency | p99 latency | p99.9 latency |
| --- | --- | ---: | ---: | ---: | ---: |
| SET quorum | 200 connections, pipeline 50 | 200,969 ops/s | 197.63 ms | 339.97 ms | 423.94 ms |

Single-command unloaded durable SET latency on the same NVMe server:

```text
n=30
min=7.724 ms
avg=8.129 ms
p50=8.054 ms
max=9.296 ms
```

## Reproducing the shapes

FerricFlow benchmarks are run from the Python SDK repository with the optimized queue/workflow benchmark scripts. KV benchmarks use `memtier_benchmark` with Ferric protocol, 256-byte values, and the connection/pipeline settings above.

For public reporting, prefer the 1M-flow live results and the 200-connection, pipeline-30 KV table. They are less bursty than short or preloaded-only runs.
