# Multi-Shard Execution Boundary

## Supported Modes

- **Same Raft group**: execute directly through the shard-local command path.
- **Independent durable groups**: return `CROSSSLOT` before command execution.
- **Standalone instance**: coordinate local shards under ordered barriers and
  commit through the standalone compensation journal.
- **Direct store**: execute against the caller-provided store, primarily for
  embedded adapters and command-unit tests.

## Durable Safety Rule

A durable operation may mutate only one Raft group. FerricStore does not expose
a distributed mutation path without all of the following:

- a replicated prepare and commit decision,
- deterministic recovery that can finish or undo every participant,
- a consistent snapshot for read participants,
- fencing against stale coordinators,
- durable admission and resource validation for the complete write footprint.

Per-key ownership records alone cannot provide those properties. Commands such
as `MSETNX`, `RENAME`, `RENAMENX`, `COPY`, `LMOVE`, `SMOVE`, and set-store
operations therefore fail closed when their routing keys span durable groups.

## Execution Order

1. Resolve the caller's instance.
2. Group routing keys by shard.
3. Use the direct fast path when every key belongs to one shard.
4. Return `CROSSSLOT` immediately for multiple durable groups.
5. For standalone instances, enforce the key-count limit, acquire shard
   barriers in index order, and invoke the journaled local coordinator.

The durable rejection precedes value validation, pressure checks, reads, lock
acquisition, and the command callback. This gives every replica and API surface
the same deterministic result without unnecessary work.

## Performance

- Same-shard cost is one key-grouping pass plus the existing command path.
- Durable rejection is `O(number_of_keys)` with no network or disk round trip.
- Standalone coordination locks only involved shard processes and keeps the
  existing compensation-journal durability boundary.

## Future Distributed Transactions

A future implementation should be a separate replicated coordinator with
monotonic transaction epochs and explicit participant state. It must include
crash tests for every prepare/commit boundary and prove read-snapshot semantics
before any durable multi-group command is enabled.
