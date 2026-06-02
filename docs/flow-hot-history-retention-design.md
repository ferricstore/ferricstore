# Flow Hot History, Value Refs, and Retention Design

## Goal

Flow writes should keep the hot path small:

- active state and claim/transition indexes stay hot;
- history is durable, but cold by default;
- large payload/result/error values do not stay materialized in BEAM unless the current state needs them;
- terminal retention eventually removes the whole workflow footprint.

The default product stance is:

> Current state is hot. History is for investigation/query/debug and can be cold.

## Hot History Default

`history_hot_max_events` should default to `0`.

Semantics:

- `0` means keep no history events in the hot Flow history index after they are projected to LMDB.
- It does not mean history is disabled.
- Every mutation still writes a durable history event.
- A new history event may remain hot temporarily until async LMDB projection confirms it.
- `FLOW.HISTORY` reads cold LMDB history by default, so users still see history unless retention or `history_max_events` removed it.
- `FLOW.HISTORY include_cold=false` should return only the hot tail, which is usually empty when the cap is `0`.

The current state record is separate from history. With hot history `0`, an active workflow still has a current state row and the indexes needed for claims/transitions.

## Current-State Value Refs

Flow state may reference external/generated values:

- `payload_ref`
- `result_ref`
- `error_ref`
- named `value_refs`

These refs are not the same as history rows. Hot history trimming must not break current state reads.

Required invariant:

> A value referenced by the current state must remain readable even if all history rows are cold.

After LMDB projection confirms a history event:

1. collect refs from the projected history event;
2. collect refs from the current state record for the same flow;
3. for each history ref:
   - if the current state still references it, keep it hot/readable;
   - if only old history references it, dematerialize the hot keydir value and keep only its durable disk locator;
   - do not tombstone/delete the disk value during hot trimming.

This applies to payloads too. Payload is usually the largest value, so old transition payloads are the most important refs to dematerialize.

## Dematerialization vs Retention

There are two different cleanup phases:

| Phase | When | What it does | What it must not do |
| --- | --- | --- | --- |
| Hot dematerialization | After LMDB projection | Removes large BEAM/keydir value binaries for history-only refs, keeps disk locators | Must not delete disk data or break current state |
| Retention cleanup | After terminal retention expiry | Tombstones/removes state, history, generated values, indexes, and LMDB projection rows | Must not run before terminal retention expires |

Hot dematerialization is a memory optimization. Retention cleanup is lifecycle deletion.

## Compact Disk Encoding

Flow metadata uses compact durable codecs:

- `FSF5` for the current state record.
- `FSH2` for history entries.
- `FSV2` for generated value wrappers.

`FSF5` avoids writing nil/default state fields and stores the common
`root_flow_id == id` case as a flag. `FSH2` stores only per-event data and
reconstructs immutable identity fields from the current or snapshot state record
when `FLOW.HISTORY` is decoded for users. This keeps long histories from
rewriting the same type/id/partition/parent/root/correlation metadata on every
transition.

Codec changes must update both `Ferricstore.Flow` and the Rust
`flow_index.rs` NIF implementation. After public release, incompatible field
order or type changes need a new magic and an old-format decoder.

## Terminal Retention

Terminal retention should be finite by default. Completed, failed, canceled, or exhausted workflows should not live forever unless a future explicit archival mode is added.

Existing code already has important pieces:

- `Ferricstore.Flow.RetentionSweeper` runs periodically.
- Cleanup is scheduled as normal Flow/Raft-backed cleanup, so correctness stays in the state machine.
- Retention policy supports `ttl_ms`, `history_hot_max_events`, and `history_max_events`.
- Existing tests cover terminal hot pruning, retention cleanup, LMDB projection cleanup, and generated value cleanup in some paths.

The contract we want to verify/finish:

1. terminal command computes `retention_at_ms = terminal_now_ms + retention.ttl_ms`;
2. terminal indexes include the retention deadline;
3. sweeper finds expired terminal flows in bounded batches;
4. cleanup tombstones/removes:
   - current state row;
   - hot history rows;
   - LMDB state/history/index rows;
   - generated payload/result/error/named value refs owned by the flow;
   - Flow ordered-index entries and lookup entries;
   - terminal/lineage/correlation/state/history indexes;
5. restart after cleanup does not resurrect the flow from Bitcask/WARaft replay;
6. repeated cleanup is idempotent.

## Outcome-Specific Retention

It makes sense to allow different retention by terminal outcome:

- completed workflows often need short retention;
- failed/exhausted workflows often need longer debugging retention;
- canceled workflows may be product-specific.

Proposed policy shape:

```elixir
%{
  retention: %{
    ttl_ms: 7 * 24 * 60 * 60 * 1000,
    completed_ttl_ms: 24 * 60 * 60 * 1000,
    failed_ttl_ms: 14 * 24 * 60 * 60 * 1000,
    canceled_ttl_ms: 7 * 24 * 60 * 60 * 1000,
    history_hot_max_events: 0,
    history_max_events: 100_000
  }
}
```

Resolution order:

1. command override;
2. state policy;
3. flow policy;
4. default retention.

Outcome-specific TTL overrides only the terminal retention TTL. It should not change retry behavior.

Temporal comparison:

- Temporal keeps completed workflow history for a configured retention period.
- Temporal retention is usually namespace-level, not per-event hot memory management.
- Temporal does not make external side effects safe without idempotency.
- FerricStore Flow can reasonably add outcome-specific retention because Flow already owns terminal state metadata.

## What Is Missing / Needs Audit

### P0 Correctness

- Allow `history_hot_max_events: 0`.
  Current validation appears to require `> 0`; this blocks the desired default.
- Change default retention `history_hot_max_events` from `1` to `0`.
- Protect current-state refs during history projection dematerialization.
  Current tests prove generated payloads can be dematerialized after projection, but the stronger invariant should prove the current payload/result/error/custom refs remain hot/readable when still referenced by state.
- Add restart test for hot history `0`:
  create -> transition -> flush projection -> restart -> `FLOW.GET` returns current state and current payload, `FLOW.HISTORY` reads cold events.

### P1 Retention Contract

- Add an end-to-end retention test with generated payload/result/error/custom `value_refs`:
  complete/fail -> retention expiry -> sweeper/cleanup -> `FLOW.GET` nil -> `FLOW.HISTORY` empty/missing -> generated refs missing -> LMDB rows gone -> restart remains missing.
- Add idempotency test:
  running cleanup twice returns zero on second run and does not error.
- Add outcome-specific retention fields if we want completed/failed/canceled TTLs.
- Add telemetry assertion for sweeper backlog/errors, because a stuck sweeper means disk/RAM can grow forever.

### P2 Performance / Operations

- Benchmark with hot history `0` and 1KB/5KB payloads.
  Expected impact:
  - `keydir_history` should approach zero after projection catches up;
  - old history-only payload binaries should stop growing BEAM binary memory;
  - `keydir_value` remains nonzero because current states still reference current values.
- Track `value_ref_hot_current`, `value_ref_hot_history_only`, and `value_ref_dematerialized` counters in `INFO` or telemetry.
- Ensure LMDB projection lag remains visible:
  hot history `0` depends on projection confirmation before trimming.

## Acceptance Criteria

- Default Flow create without explicit retention reports `history_hot_max_events: 0`.
- Active flow with many transitions keeps current state and current payload readable.
- After LMDB projection, hot history count is `0` for that flow.
- `FLOW.HISTORY` returns cold LMDB events by default.
- `FLOW.HISTORY include_cold=false` returns no events when the hot cap is `0`.
- Old payload/result/error values referenced only by history are dematerialized from hot keydir.
- Current-state payload/result/error/named refs are not dematerialized while current state references them.
- Terminal retention expiry removes state/history/value/index/LMDB rows.
- Restart does not resurrect retained-away flows or old values.
