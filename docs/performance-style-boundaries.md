# Performance style boundaries

FerricStore code cleanup must preserve hot-path throughput. Prefer small,
mechanical changes and benchmark any refactor that changes hot request/write
paths.

## Hot paths: benchmark required

Run DBOS-style Flow and native-protocol SET/GET pipeline benchmarks before/after changes
in these areas:

- `FerricstoreServer.Native.Connection`
- `FerricstoreServer.Native.Lane`
- `Ferricstore.Store.Router`
- `Ferricstore.Store.Shard`
- `Ferricstore.Store.Shard.*`
- `Ferricstore.Raft.StateMachine`
- `Ferricstore.Commands.*`
- `Ferricstore.Commands.Flow`
- `Ferricstore.Flow.ClaimDueAPI`
- `Ferricstore.Flow.LMDBWriter`

Rules for hot paths:

- No behaviours/protocols/dynamic dispatch in per-command loops.
- No new maps/list allocations in per-command loops.
- No extra GenServer/Task calls in request/write paths.
- Keep dispatch mechanical and data-shape stable.
- If macro section extraction changes generated code, treat it as hot-path risk.

## Hot-adjacent paths: focused tests plus benchmark if enqueue/apply cost changes

- Flow LMDB projection enqueue/config/outbox modules.
- Flow history/value projection modules.
- Claim waiter scheduling.
- Promoted Bitcask compaction scheduling.
- Blob/value-ref ownership and GC metadata.

These modules may run async or in background, but many are fed by hot Flow
commands. Changes that add enqueue work, projection tuple construction, or
request-path coordination need benchmarks.

## Cold/control paths: normal refactor allowed

- Dashboard rendering.
- Docs and guides.
- Test support helpers.
- Health/admin endpoints outside command dispatch.
- ACL formatting/parsing helpers outside request authorization hot loops.
- Cluster orchestration setup/inspection paths.

Use normal readability refactors here: semantic modules, explicit helpers,
smaller files, and clearer tests.

## Macro section policy

Macro sections are acceptable only when replacing them would risk hot generated
code or require large movement. Prefer normal modules for cold/control code.

Do not start by replacing `Ferricstore.Raft.StateMachine` sections. It is the
core durable write path.

Lower-risk candidates:

- Dashboard/test section modules.
- Test-only section macros.
- Cold helper modules currently named `part_XX`.
