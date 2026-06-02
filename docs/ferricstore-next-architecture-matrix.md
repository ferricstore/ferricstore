# FerricStore Next Architecture Matrix

This document captures the major rewrite/refactor ideas discussed for a higher
performance FerricStore architecture. It is not a migration plan by itself. Use
it as a decision matrix when evaluating WARaft, a Rust data plane, unified
storage, and the next Flow primitive design.

## Goals

| Goal | Target Outcome | Why It Matters |
| --- | --- | --- |
| Lower write latency | Fewer fsync boundaries and fewer process hops | Current durable write path pays for Ra WAL plus Bitcask/checkpoint machinery. |
| Higher write throughput | Larger batches with less BEAM term churn | SET/large-value workloads are sensitive to serialization and copying. |
| Better large-value behavior | Stream/copy by file offsets where possible | Avoid materializing huge binaries in BEAM for read/write hot paths. |
| Simpler correctness model | One durable source for Raft log and storage records | Reduces replay, checkpoint, and cursor-frontier complexity. |
| Flow as a native primitive | Atomic workflow mutations with native indexes | Flow should not behave internally like many scattered Redis keys. |
| Keep operational resilience | Supervision, clear recovery, deterministic replay | Performance changes must not weaken crash safety. |

## Storage Architecture

| Area | Current Shape | Proposed Shape | Expected Gain | Main Risk |
| --- | --- | --- | --- | --- |
| Raft WAL + Bitcask | Ra WAL is durable ack boundary; Bitcask is separate state log/checkpoint. | Unified segmented log acts as both replicated Raft log and storage record log. | One fsync path, fewer recovery layers, simpler durable frontier. | Very high correctness blast radius. |
| Segment records | Ra entries and Bitcask records are separate encodings. | Segment record contains `{term, index, shard, command/state record, key/value/blob ref, crc}`. | Less duplication and less write amplification. | Must handle partial/torn records exactly. |
| Keydir pointers | Keydir points to Bitcask file id/offset. | Keydir points to `{segment_id, offset, size}` or blob segment location. | Reads use the same durable address space as replay. | Compaction must not race reads or Ra retention. |
| Compaction | Bitcask merge and tombstone cleanup are separate from Ra WAL retention. | Segment compaction rewrites live records and advances a safe Ra retention frontier. | Better disk usage and faster restart. | Releasing a Ra index too early can lose data. |
| Large values | Blob side channel exists for large values. | Large values become blob/large-value segment records referenced by keydir. | Less memory pressure, better streaming path. | Blob GC must be tied to segment/keydir liveness. |
| Fsync policy | Ra WAL fsync plus Bitcask/checkpoint fsync. | Group fsync per segment writer, with byte and time backpressure. | Better p99 and higher MB/s for large payloads. | Bad tuning can cause timeout ambiguity or memory growth. |

## Rust/Data-Plane Matrix

| Layer | Keep In Elixir | Move To Rust/Native | Reason |
| --- | --- | --- | --- |
| Supervision/app lifecycle | Yes | No | BEAM supervision is still valuable. |
| Cluster orchestration | Mostly yes | Maybe small helpers | Human-readable control plane is easier in Elixir. |
| RESP parser | No | Yes | Already moved/optimized; parser should produce normalized AST/plans. |
| RESP encoder | Maybe | Yes for hot replies | Reduces BEAM binary allocation on GET/MGET. |
| Pipeline planner | Maybe | Yes for hot path | Precompute command class, shard, key, and batch grouping. |
| Shard routing | Maybe | Yes for hot path | Avoid repeated map/grouping work at 700K+ ops/s. |
| Segment append/read | No | Yes | File IO, CRC, offsets, and batching fit Rust better. |
| Keydir/index engine | Mixed | Yes for highest scale | ETS is good, but native indexes can reduce copying/metadata overhead. |
| Cold/blob streaming | No | Yes | Rust can stream from file offsets and avoid BEAM heap pressure. |
| Raft core | Evaluate | WARaft or Rust Raft | Need benchmark proof plus full feature parity. |

## Shard Actor Model

| Responsibility | Proposed Owner | Notes |
| --- | --- | --- |
| Append batching | Shard actor | One actor per shard/partition controls batch boundaries. |
| Keydir updates | Shard actor | Apply and read state stay local to the shard. |
| TTL/index updates | Shard actor | Mutations update indexes atomically with state. |
| Compaction coordination | Shard actor plus background workers | Actor owns liveness/frontier decisions. |
| Raft apply | Shard actor/storage provider | Reply only after local durable/apply contract is satisfied. |
| Reads | Shard actor or lock-free local index | Hot GET should not go through strong Raft reads. |

The target is to avoid unnecessary Router -> Batcher -> Ra -> StateMachine ->
BitcaskWriter hops for the default write path. Elixir can supervise and expose
control APIs while the shard data plane runs as a tighter native engine.

## Raft Options

| Option | Upside | Downside | Decision Gate |
| --- | --- | --- | --- |
| Keep current Ra fork | Known behavior, already integrated | Limited control over internals and batching | Stay if WARaft/Rust does not beat it materially. |
| WARaft backend | More control, promising local spike numbers | Needs production storage provider and full cluster integration | Migrate if durable Azure benchmark beats current Ra and correctness gaps close. |
| Own Rust Raft | Maximum control over WAL/storage/network | Very large implementation and verification cost | Only if WARaft/current Ra cannot meet goals. |

Required Raft features before replacement:

- custom segmented WAL/storage backend
- snapshot create/install
- membership changes
- byte and queue backpressure
- command correlation/replies
- reply-after-local-apply semantics
- one partition per shard
- deterministic recovery
- crash-safe log truncation/retention
- three-node quorum benchmark

## Flow Primitive Matrix

| Area | Current/Transitional Shape | Proposed Native Shape | Gain |
| --- | --- | --- | --- |
| Flow state | Multiple logical state/history/value/index records. | One compact Flow record struct per active workflow. | Less metadata, fewer writes, easier atomicity. |
| Mutation record | State/history/index updates can be separate internal operations. | One atomic log record per Flow mutation. | Better correctness and fewer durable records. |
| Claim due | RESP batching reduces client overhead, but apply still does many updates. | Native bulk claim scans due index once and moves N records in one operation. | Biggest worker hot-path win. |
| Indexes | Flow-specific ETS/native ordered index plus LMDB projection. | Purpose-built native indexes per shard. | Faster claim/query without Redis ZSET overhead. |
| History | Hot/cold split exists conceptually. | RAM keeps state plus last `history_hot_max_events`; full history is cold. | Supports 50K-event workflows without RAM blowup. |
| Payloads | Payload refs supported. | Payload refs plus optional templates/fanout metadata. | Saves storage/network for 10K-1M fanout. |
| LMDB | Cold projection, should not block writes. | Async projection only, never write ACK path. | Keeps Flow write latency independent from LMDB stalls. |
| Retention | Cleanup command/sweeper removes terminal state/history/values. | Retention becomes part of segment compaction liveness. | Prevents large payload leaks with less extra scanning. |

## Native Flow Record

Target compact record fields:

```text
id
type
state
partition_key
priority
due_at_ms
lease_until_ms
fencing_token
attempts
max_attempts
retry_policy
payload_ref
result_ref
error_ref
retention_at_ms
history_hot_tail
```

The record should be the source of truth for active workflow execution. Cold
query projections can be rebuilt from the durable log.

## Flow Indexes

| Index | Key Shape | Purpose |
| --- | --- | --- |
| Due index | `{type, state, partition, priority, due_at_ms, id}` | `FLOW.CLAIM_DUE` scan. |
| Running lease index | `{lease_until_ms, id}` | Detect expired worker leases. |
| State/partition index | `{type, state, partition, id}` | Query active workflows by state/partition. |
| Retention index | `{retention_at_ms, id}` | Terminal cleanup. |
| ID lookup | `{id}` | Direct `FLOW.GET`. |
| History hot-tail index | `{id, seq}` | Fast recent history reads. |

Redis ZSET is useful as a public Redis feature, but Flow internals should use a
Flow-specific ordered index. It avoids score string conversion, compound-key
overhead, and generic type registry work.

## Flow Bulk Operations

| Operation | Native Behavior |
| --- | --- |
| `CREATE_MANY` | Group by shard, append one batch per shard, write compact records and index entries. |
| `CLAIM_DUE limit=N` | Scan due index once, move records to running lease index, bump fencing tokens, append one durable batch. |
| `TRANSITION_MANY` | Validate tokens, update state/index/history in one batch per shard. |
| `COMPLETE/FAIL/CANCEL` | Move to terminal state, set retention time, write result/error refs, update retention index. |
| Retention cleanup | Delete terminal state/history/value refs when retention expires. |

## Fanout/Template Ideas

| Feature | Description | When It Helps |
| --- | --- | --- |
| `FLOW.VALUE.PUT` | Store a large payload once and reuse its ref. | Same payload sent to many workflows/devices. |
| Linked value refs | A value can be linked to a flow/policy for cleanup ownership. | Prevents manual orphan cleanup. |
| Create template | Store common metadata/policy once; each flow stores only delta fields. | 10K-1M fanout with repeated type/state/policy/payload. |
| Partition-key fanout | Each created flow keeps its own partition key for FIFO worker consumption. | IoT/device/message fanout. |

Template/fanout is a specialized storage optimization, not required for the
first production Flow design. It becomes valuable when one event creates many
nearly identical workflows.

## Expected Gains

| Change | Throughput Gain | Latency Gain | Storage Gain | Confidence |
| --- | --- | --- | --- | --- |
| Unified Raft/storage segment log | High | High | High | Medium, high risk |
| Rust segment append/read | High | Medium-high | Medium | High |
| Rust RESP planner/encoder | Medium | Medium | None | High |
| Remove write ingress GenServer hops | Medium | Medium | None | Medium |
| Native Flow bulk claim | High for Flow workers | Medium-high | Medium | High |
| Flow history hot/cold split | Medium | Medium | High RAM gain | High |
| Payload refs/templates | Workload-dependent | Medium for fanout | Very high for fanout | Medium |
| LMDB async-only projection | Medium | High p99 protection | None | High |

## Migration Phases

| Phase | Scope | Exit Criteria |
| --- | --- | --- |
| 1. WARaft replacement gate | WARaft backend selector, real state machine, restart, snapshot install, multi-shard tests. | Same command behavior as current Ra for hot paths, green targeted tests. |
| 2. Durable backend benchmark | Production-style WAL grouping or integrated segment provider. | Beats current Ra by at least 5% on same Azure topology or materially improves p99. |
| 3. Full command surface | Compound, JSON, probabilistic, bitmap, stream, Flow, TTL through backend selector. | Guard tests prevent direct Ra bypasses. |
| 4. Cluster parity | 3-node quorum, membership, snapshot catchup, restart chaos. | Same cluster semantics as current Ra. |
| 5. Unified segment prototype | One segment log for Raft/storage in a branch. | Crash/replay/compaction proofs before benchmark claims. |
| 6. Flow native primitive | Native Flow record/index/bulk apply. | Claim/transition benchmarks beat current Flow and LMDB is off the ACK path. |

## Non-Negotiable Correctness Rules

- Never acknowledge a write before its selected durability boundary is met.
- Never release/truncate Raft log entries past the durable storage frontier.
- Snapshot install must be atomic from the perspective of startup/recovery.
- Compaction must not invalidate a keydir/blob pointer still visible to readers.
- Flow state, history, indexes, and payload refs must mutate atomically.
- LMDB/cold projections must be rebuildable and must not define write success.
- Large-value GC must follow actual liveness, not only file age.
- Any backend selector must have guard tests preventing accidental direct Ra
  bypasses in production write paths.

## Open Decisions

| Decision | Options | Needed Evidence |
| --- | --- | --- |
| Raft backend | Current Ra fork, WARaft, own Rust Raft | Same Azure benchmark and chaos test matrix. |
| Unified log timeline | Incremental vs new engine branch | Complexity estimate after WARaft backend is stable. |
| Rust boundary | NIF shard engine vs separate Rust node/process | Failure isolation and latency benchmark. |
| Flow storage | ETS/native indexes plus segment log vs LMDB primary | Flow write p99 with LMDB stalled and large history workloads. |
| Template/fanout | Defer vs implement early | Real user demand for 10K-1M repeated payload fanout. |

## Current Recommendation

Do not rewrite everything immediately. Keep using this matrix to drive bounded
proofs:

1. Finish the WARaft replacement gate and benchmark it against current Ra.
2. Build production-style segmented WAL/storage only if WARaft remains promising.
3. Move the next hot layers to Rust only where tests and benchmarks prove a real
   gain.
4. Treat Flow as a native workflow primitive, not a pile of Redis-like internal
   keys.
5. Keep Elixir supervision/control-plane value unless a specific layer proves it
   must move to Rust.
