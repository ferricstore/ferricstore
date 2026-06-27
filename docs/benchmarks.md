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
```

The best balanced 16-vCPU server result was with 32 Flow shards.

| Mode | API shape | Server shards | Create rate | Process/complete rate | End-to-end rate |
| --- | --- | ---: | ---: | ---: | ---: |
| Sync live | Queue worker | 32 | - | - | 53,790 flows/s |
| Sync live | Workflow worker | 32 | - | - | 54,060 workflows/s |
| Async live | Queue worker | 32 | 95,896 flows/s | - | 45,608 flows/s |
| Async live | Workflow worker | 32 | 97,196 workflows/s | - | 47,888 workflows/s |

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

## External Reference: DBOS Published Numbers

DBOS publishes Postgres-backed durable workflow benchmark numbers that are useful
as an external reference point. These are not an apples-to-apples comparison
with the FerricFlow tables above: DBOS used a single AWS RDS Postgres
`db.m7i.24xlarge` instance with 96 vCPUs, 384 GB RAM, and 120K provisioned IOPS
on io2 storage, while the FerricFlow numbers above are from separate Azure
server/client VM runs with FerricStore's native storage engine.

Source: [DBOS, "Does Postgres Scale?", April 23, 2026](https://www.dbos.dev/blog/benchmarking-workflow-execution-scalability-on-postgres).

| DBOS workload | Published result | Notes |
| --- | ---: | --- |
| Raw Postgres point writes | 144,000 writes/s | Single-row inserts from async Python clients, one transaction per row. |
| Direct no-op durable workflows | 43,000 workflows/s | DBOS says each workflow performs two Postgres writes, so this is about 86,000 workflow-status writes/s. |

DBOS docs summarize the same scale as `>40K workflows or steps per second` for
a DBOS application using one Postgres database:
[DBOS Architecture](https://docs.dbos.dev/architecture) and
[DBOS Production Checklist](https://docs.dbos.dev/production/checklist).

Older DBOS workflow-latency benchmarks against AWS Step Functions are a
different workload shape, but they give another public DBOS reference:
[DBOS vs. AWS Step Functions Performance Benchmark](https://www.dbos.dev/blog/dbos-vs-aws-step-functions-benchmark)
reports DBOS Transact as 25x faster than standard Step Functions in their tests,
with a 5-step workflow around 40 ms in DBOS versus over 1 second in Step
Functions, and Express Step Functions around 3x slower than DBOS.

## KV SET/GET: native protocol baseline pending

The older KV benchmark shape is no longer valid because the standalone server
now exposes the Ferric native binary protocol. Publish KV SET/GET
numbers only after rerunning them through a native SDK or native protocol
benchmark client.

The replacement benchmark should report at least:

| Field | Required shape |
| --- | --- |
| Transport | Ferric native TCP/TLS protocol |
| Workload | SET and GET with fixed value size |
| Client concurrency | Connections, lanes, in-flight requests per lane |
| Durability mode | Quorum durable writes vs any async mode being tested |
| Hardware | Server/client VM size, storage type, filesystem |
| Metrics | Throughput, p50, p95, p99, p99.9 |

## Reproducing the shapes

FerricFlow benchmarks are run from the Python SDK repository with the optimized queue/workflow benchmark scripts. KV benchmarks should use a native-protocol SDK/client shape for current FerricStore releases.

For public reporting, prefer the 1M-flow live results. Add KV tables only after native-protocol SET/GET runs are available.
