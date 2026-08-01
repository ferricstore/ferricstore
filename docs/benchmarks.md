# Benchmarks

This page keeps only the latest public benchmark summaries. Raw benchmark logs and one-off profiling runs are intentionally not committed.

Use these numbers as reproducible reference points, not universal hardware claims. Throughput and latency depend on VM type, local NVMe availability, shard count, client concurrency, pipeline depth, payload size, and resource guards.

## FerricStore PubSub: compact native pipeline

These loopback TCP measurements use pipeline depth 8 and 256-byte messages.
Every subscriber acknowledgement, publish count, pushed delivery, and
slow-consumer isolation check is validated by the runner.

### Protocol-only comparison before batch-aware fanout

| Fanout | Publishers | Generic pipeline publishes/s | Compact mode 35 publishes/s | Change |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 27,634 | 29,204 | +5.7% |
| 1 | 4 | 10,106 | 11,590 | +14.7% |
| 8 | 1 | 3,070 | 3,302 | +7.6% |
| 8 | 4 | 2,872 | 3,096 | +7.8% |

### Batch-aware exact-channel fanout

The retained optimization resolves the exact subscriber list once per compact
pipeline, reserves outbound capacity once per subscriber batch, enqueues one
BEAM mailbox message per subscriber, and writes the batch's existing individual
event frames in one socket send. It does not change the wire format. A depth-8,
fanout-8 request therefore reduces 64 guarded reservations and mailbox messages
to 8. Pattern subscriptions conservatively retain the original per-publish path
so exact and pattern event ordering stays unchanged.

The following are three-sample medians from a matched compact-mode-35 comparison;
the only benchmark toggle was ordinary per-item publish versus batch-aware
publish:

| Fanout | Publishers | Per-item publishes/s | Batch-aware publishes/s | Change | Batch-aware deliveries/s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 37,270 | 48,556 | +30.3% | 48,556 |
| 1 | 4 | 13,392 | 50,020 | +273.5% | 50,020 |
| 8 | 1 | 3,348 | 16,483 | +392.3% | 131,865 |
| 8 | 4 | 3,273 | 13,605 | +315.7% | 108,840 |

Typed native pipelines containing only `PUBLISH` now reuse the same bounded
batch fanout path. This preserves the typed wire format for older SDKs while
removing the former per-item delivery overhead:

| Fanout | Publishers | Typed per-item publishes/s | Typed batch-aware publishes/s | Change |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 1 | 3,262 | 14,976 | +359.0% |
| 8 | 4 | 3,259 | 12,544 | +284.9% |

### Sustained batch-size gate

Larger explicit `publish_many` requests reduce the remaining per-subscriber
socket-write cost. With 65,536 publishes per publisher, 256-byte payloads, and
fanout 8, the production-default activity log, and compact mode 35, the matched
five-sample medians were:

| Pipeline depth | Publishers | Publishes/s | Delivered messages/s | Change vs depth 512 |
| ---: | ---: | ---: | ---: | ---: |
| 512 | 1 | 132,794 | 1.06M | baseline |
| 1,024 | 1 | 175,712 | 1.41M | +32.3% |
| 512 | 4 | 135,774 | 1.09M | baseline |
| 1,024 | 4 | 184,921 | 1.48M | +36.2% |

The isolated rerun resolved earlier depth-1,024 variance, so SDK
`publish_many` helpers cap requests at the protocol limit of 1,024 and split
larger inputs sequentially. Callers can choose a smaller explicit batch for a
tighter latency or partial-failure boundary; payload size and subscriber fanout
still need workload-specific measurement.

### Negotiated PubSub batch events

With the same 65,536 publishes per publisher, depth 1,024, 256-byte payload,
and compact-mode-35 workload, three-sample medians compare the legacy event
frames with `pubsub_batch_v1` negotiated on every subscriber:

| Fanout | Publishers | Legacy publishes/s | Batch-event publishes/s | Change | Batch-event deliveries/s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 359,286 | 685,588 | +90.8% | 685,588 |
| 1 | 4 | 396,652 | 635,082 | +60.1% | 635,082 |
| 8 | 1 | 152,361 | 295,206 | +93.8% | 2.36M |
| 8 | 4 | 165,958 | 293,794 | +77.0% | 2.35M |

For a 1,024-message batch with 256-byte values, one subscriber's encoded
output fell from 1,024 frames and 422,912 bytes to one frame and 267,428 bytes
(-36.8%). The encoder result also fell from 67,622 retained heap words to 10
because the batch is held by one encoded binary. Ordinary single-publish memory
was unchanged in the matched Benchee gate.

For homogeneous batches with eight matching pattern subscribers, resolving a
complex pattern once per channel batch reduced the 1,024-message median from
5.64 ms to 3.05 ms (-45.9%) while retaining exact-before-pattern ordering for
every message.

## FerricStore Streams: local optimization baseline

These development numbers were recorded on an Apple M4 Max with a one-node
Raft group and local durable WAL commits. They validate relative code-path
changes; they are not multi-node product claims.

| Workload | Reported unit | Local result |
| --- | --- | ---: |
| Embedded `XADD_MANY`, 32 concurrent batches of 1,024 entries, one Stream | appended entries/s | 395,391 median-run throughput |
| Embedded `XADD_MANY`, same workload across 12 partition Streams | appended entries/s | 687,797 median-run throughput |
| Embedded `XADD_MANY`, interleaved topics on distinct shards | appended entries/s | 391,017 median-run throughput |
| Embedded `XADD_MANY`, 64 interleaved topics forced onto one shard | appended entries/s | 253,338 median-run throughput |
| Native compact mode 34, 16 loopback TCP connections × 8 batches × 1,024 entries | appended entries/s | 400,604 sustained throughput |
| Native compact pipeline mode 34, 1,024 entries, one in-flight loopback TCP request | appended entries/s | 138,025 average; about 142,222 median-derived |
| Native compact pipeline mode 34, 256 entries, loopback TCP | appended entries/s | 55,654 average; 104,918 p50-derived |
| Embedded `XLEN` over a 4,096-entry Stream | requests/s | about 1,380,000 |
| Embedded `XRANGE COUNT 1` | requests/s and returned entries/s | 208,608 |
| Embedded `XRANGE COUNT 10` | requests/s | about 168,000-170,000; 4.79 µs median |
| Embedded `XRANGE COUNT 10` | returned entries/s | about 1,680,000-1,700,000 |
| Embedded `XREVRANGE COUNT 10` | requests/s | about 159,000-179,000; 4.88 µs median |

Targeted before/after gates during the Stream apply-plan cleanup measured the
following on the same machine and process shape:

- publishing 1,024 ordered member-catalog rows fell from roughly 0.55 ms median
  to 0.33 ms after the committed plan began carrying ready catalog rows and an
  exact idempotent count;
- publishing the derived metadata/index cache for 64 topics fell from roughly
  207 µs to 34-40 µs after terminal plans began carrying complete ready cache
  rows through the durable commit boundary;
- `XRANGE COUNT 10` improved from about 94.7K requests/s at 9.21 µs median to
  about 168-170K requests/s at 4.79 µs median after range catalogs began using
  OTP ordered lookup/traversal, dropped unused LFU-hit bookkeeping, reused
  per-shard catalog references, fused the type check with the bounded page read,
  and assembled the common exact persisted row without an intermediate result;
- new-topic type validation fell from about 1.86 µs to 0.62 µs in isolation by
  validating the already-read marker, and preparing the type/entry/meta physical
  keys together reduced that encoding component by about 41%;
- staging 64 shared-log topics fell from 122 µs to 89 µs median in a 40-sample
  paired run after their validated plans were staged once. Empty optional index
  checks fell from about 29 µs per 64 topics to 0.25 µs, and empty blocking-waiter
  notification work fell from about 52-60 µs to 0.3 µs;
- in a 200-append steady-state projection microbenchmark, passing ready
  put-only rows directly to the existing mixed-operation Bitcask NIF reduced
  total BEAM allocation/GC plus append/result processing from about 339 ms to
  192 ms (roughly 43%); the on-disk format and fsync boundary are unchanged;
- bounded activity-log publication for 1,024 results fell from 1,122 µs to
  222 µs median in the focused microbenchmark because only the newest retained
  rows are materialized;
- constructing a 64-entry deterministic append plan fell from 9.63 µs to
  7.25 µs median, while measured allocation fell from 34.40 KB to 30.42 KB,
  after each durable compound key became the single owning binary for its
  returned Stream ID slice;
- direct mixed-topic preparation uses one routing/grouping pass instead of
  allocating generic command tuples and rediscovering the Stream shape per
  shard. In the 64-entry/12-shard microbenchmark it used 22.27 KB instead of
  28.34 KB; the eight-topic/same-shard shape used 32.87 KB instead of 38.51 KB.
  Timing gains were about 3-16% across paired samples, while the same-shard
  median remained within measurement noise;
- grouped-command uniqueness validation now performs one map insertion/hash per
  topic. The 64-entry gate improved from 1.90 to 1.77 µs for eight topics and
  from 13.01 to 12.42 µs for 64 topics without relaxing duplicate, gap, or
  out-of-range rejection;
- exact native request/response schemas reduced the common five-binary XRANGE
  request decode from about 409 ns to 300 ns and the ten-row response value
  encode from about 1.07 µs to 0.46 µs. Python and TypeScript response decoding
  also improved without regressing mixed arrays; Go uses the same preallocated
  output shape but could not be executed on the benchmark host because its Go
  toolchain was unavailable;
- matched native mode-34 producers reached about 401K entries/s over loopback
  TCP with 16 concurrent connections, while a single in-flight request remained
  latency-bound. This is the native counterpart to the concurrent embedded gate,
  rather than multiplying a sequential request rate into a throughput claim;
- with the corrected compact-command apply budget, validated internal scale
  samples reached about 326K p50-derived entries/s at batch 1,024, 300K at
  batch 2,048, and 174K at batch 4,096. Batch 1,024 was the current sweet spot
  on this host; producers should benchmark rather than assuming that the
  largest admitted batch wins.

These are optimization gates, not replacements for the public table: the
absolute rerun was discarded because unrelated long-running processes were
saturating several CPU cores. Relative gates were accepted only when compared
inside one alternating run or isolated to one measured stage.

One measured experiment was explicitly rejected: carrying pre-encoded field
payloads in the Raft command improved 1,024-entry median latency by about 4%,
but enlarged the replicated term from 26.7 KB to 36.9 KB (about 38%). The branch
keeps the compact field-list WAL shape rather than trading durable write and
retention amplification for that small CPU gain.

The final sustained 1,024-entry topology gate used 32 batches, concurrency 16,
three samples, 12 shards, 64 same-shard topics, and disabled optional activity
logging. It verified exact final lengths for every physical Stream. The earlier
single-request 1,024-entry append sample had 4.19 ms average and 3.95 ms p50
latency. At 256 entries, traced embedded requests were typically about 1.8 ms; roughly
0.5 ms was deterministic apply work and the remaining majority was the Raft/WAL
commit wait, with occasional storage tails. This is why larger embedded producer
batches cross 200K entries/s while smaller batches can report fewer entries/s
even though the entry-processing code is the same. The native numbers
additionally include loopback TCP framing, decoding, dispatch, and response
encoding; their storage-tail deviation was high, so both mean throughput and
p50-derived rate are shown.

### Comparison boundary: Redis, RabbitMQ, and Kafka

Published headline rates are not directly comparable unless the acknowledgement
and durability boundary is identical:

| System/path | Batching and ordering unit | Successful acknowledgement means |
| --- | --- | --- |
| FerricStore compact Stream mode 34 | One batch for one Stream/shard; one total Redis Stream ID order | The Raft command has committed and the deterministic projection has applied; the local authoritative WAL is fsynced. |
| Redis Streams | Pipelined commands on one Redis execution path | Depends on persistence configuration. Redis documents 0.5–1M pipelined XADD/s on an average machine, but also says important messages require AOF with a strong fsync policy. Default `appendfsync everysec` can lose about one second. |
| RabbitMQ Streams | Batched publishing to a stream; superstreams partition for scale | Publisher confirms wait for quorum replication, but RabbitMQ Streams explicitly do not fsync before confirmation. RabbitMQ quorum queues provide the stronger written-and-flushed quorum boundary at a different performance point. |
| Kafka producer | Records batch per topic partition; ordering is per partition | `acks=all` waits for the in-sync replicas required by the topic settings. Kafka normally relies on replication and OS background flushing rather than forcing fsync per record or batch. |

For headline context only, Redis documents roughly 0.5-1M pipelined XADD/s on
an average machine, and RabbitMQ's Stream overview published about 1.2M
confirmed messages/s on its example workstation. Neither acknowledgement is
the same as FerricStore's local fsynced-WAL boundary: the Redis figure is
persistence-policy dependent, and RabbitMQ Streams confirm after quorum
replication without explicit fsync. Kafka deliberately scales by partition and
producer batching, so a single context-free records/s number would be more
misleading than useful.

Primary references: [Redis Streams performance](https://redis.io/docs/latest/develop/data-types/streams/#performance),
[Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/),
[RabbitMQ Streams data safety](https://www.rabbitmq.com/docs/streams#data-safety),
[RabbitMQ Streams overview benchmark](https://www.rabbitmq.com/blog/2021/07/13/rabbitmq-streams-overview),
[RabbitMQ quorum queues](https://www.rabbitmq.com/docs/quorum-queues),
[Kafka topic flush configuration](https://kafka.apache.org/42/configuration/topic-configs/),
and [Kafka replication design](https://kafka.apache.org/42/design/design/#replication).

For an honest cross-system run, hold payload bytes, producer count, batch size,
partition count, replica count, acknowledgement policy, fsync boundary,
retention, consumer acknowledgements, warm-up, and measurement interval fixed.
Report both batches/second and entries/second, plus p50/p95/p99 latency. A Flow
workflow completion is another distinct unit: it can include multiple state,
index, history, and read operations and must not be compared to one Stream entry
or one batch request.

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
