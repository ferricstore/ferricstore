# Clean Code Refactor Design

This document defines the FerricStore cleanup plan for reducing large modules, clarifying ownership, and preserving behavior and performance.

## Goal

FerricStore should be easy to read, safe to change, and still keep the current hot-path performance profile.

Hard rule:

```text
No production module should exceed 1000 lines.
```

Preferred target:

```text
Hot-path modules: 300-700 lines
Facade modules: 100-400 lines
Pure planner modules: 100-600 lines
Test files: split to match production domains, preferably under 1000 lines each
```

## Current oversized modules

These are the first modules that need structural cleanup:

```text
apps/ferricstore/lib/ferricstore/raft/state_machine.ex
apps/ferricstore_server/lib/ferricstore_server/health/dashboard.ex
apps/ferricstore/lib/ferricstore/store/router.ex
apps/ferricstore/lib/ferricstore/flow.ex
apps/ferricstore/lib/ferricstore.ex
apps/ferricstore/lib/ferricstore/raft/waraft_storage.ex
apps/ferricstore/lib/ferricstore/store/blob_store.ex
```

Large test files should be split during the same area refactor, not left behind as a second cleanup project.

## Elixir design principles

1. Split by bounded context, not by arbitrary helper buckets.
2. Keep public modules small and mostly delegating.
3. Keep hot paths explicit with pattern matching and direct function calls.
4. Avoid dynamic `apply/3`, runtime module lookup, or macro-generated dispatch in hot paths.
5. Keep pure planning separate from side effects.
6. Make side effects obvious: Ra apply, Bitcask append, OrderedIndex mutation, LMDB projection, blob writes, telemetry, and replies.
7. Prefer data-in/data-out functions for validation and planning modules.
8. Use structs or typed maps for complex intermediate data where that improves clarity.
9. Preserve telemetry names unless intentionally versioning metrics.
10. Do not change public APIs during cleanup unless explicitly planned.
11. Do not hide domain behavior in generic `Utils` modules.
12. Tests should describe behavior by domain, not by implementation accident.

## Dependency direction

Allowed direction:

```text
Public facade
-> API modules
-> command/domain modules
-> storage/router/state-machine modules
-> low-level native/storage adapters
```

Avoid reverse dependencies from storage internals into public API modules.

Avoid cyclic dependencies between Flow, Router, and StateMachine.

## Refactor order

1. Dashboard first. Lowest performance risk, biggest readability win.
2. Public `Ferricstore` facade split. Low risk if all functions delegate.
3. Flow command modules. Medium risk, high clarity gain.
4. Router split. Medium/high risk because hot path.
5. BlobStore split. Medium risk.
6. WARaftStorage split. High correctness risk.
7. Raft StateMachine split last. Highest risk; do after tests and patterns are proven.

## Testing rule

For each split:

1. Move tests next to the new domain.
2. Keep behavior assertions unchanged.
3. Add focused tests only where extraction exposes missing coverage.
4. Run targeted tests after each area.
5. Run DBOS + memtier baseline only after hot-path modules change.

Commit rule:

```text
If targeted tests pass and hot-path benchmarks stay at baseline, commit the area and move to the next area.
If performance regresses, do not commit that area. Fix until it reaches baseline again.
```

Baseline file:

```text
docs/local-clean-code-baseline.md
```

## Area 1: Dashboard

Current issue:

```text
FerricstoreServer.Health.Dashboard mixes route handling, data collection, form parsing, HTML rendering, page-specific logic, and live payload responses.
```

Target modules:

```text
FerricstoreServer.Health.Dashboard
FerricstoreServer.Health.Dashboard.Layout
FerricstoreServer.Health.Dashboard.Components
FerricstoreServer.Health.Dashboard.QueryParams
FerricstoreServer.Health.Dashboard.Forms
FerricstoreServer.Health.Dashboard.LivePayloads
FerricstoreServer.Health.Dashboard.Pages.Overview
FerricstoreServer.Health.Dashboard.Pages.Storage
FerricstoreServer.Health.Dashboard.Pages.Cluster
FerricstoreServer.Health.Dashboard.Pages.Flow
FerricstoreServer.Health.Dashboard.Pages.FlowDetail
FerricstoreServer.Health.Dashboard.Pages.FlowHistory
FerricstoreServer.Health.Dashboard.Pages.FlowLineage
FerricstoreServer.Health.Dashboard.Pages.FlowFailures
FerricstoreServer.Health.Dashboard.Pages.FlowSignals
FerricstoreServer.Health.Dashboard.Pages.FlowRetention
FerricstoreServer.Health.Dashboard.Pages.FlowPolicies
FerricstoreServer.Health.Dashboard.Collectors.Overview
FerricstoreServer.Health.Dashboard.Collectors.Flow
FerricstoreServer.Health.Dashboard.Collectors.Storage
FerricstoreServer.Health.Dashboard.Collectors.Cluster
```

Responsibilities:

```text
Dashboard: request routing and response selection only
Layout: shared HTML shell
Components: reusable HTML fragments
QueryParams: parse query strings and normalize options
Forms: parse POST bodies and validate dashboard forms
LivePayloads: endpoint payloads used by htmx/chart refreshes
Pages.*: one module per page, build assigns and render page body
Collectors.*: collect data from FerricStore/server internals
```

Rules:

```text
No page module should call unrelated page modules.
Collectors should not render HTML.
Render modules should not mutate server state.
Forms should not know storage internals.
```

Tests:

```text
Split dashboard_test.exs into page/form/query/live payload focused tests.
Keep existing HTTP-level tests for end-to-end dashboard routes.
Add focused tests only if extraction exposes missing behavior.
```

Benchmark requirement:

```text
No DBOS/memtier benchmark required for dashboard-only extraction.
```

## Area 2: Public Ferricstore facade

Current issue:

```text
Ferricstore is a large public API module that mixes docs, command API, Flow API, KV API, server API, and helper logic.
```

Target modules:

```text
Ferricstore
Ferricstore.API.Strings
Ferricstore.API.Hashes
Ferricstore.API.Lists
Ferricstore.API.Sets
Ferricstore.API.SortedSets
Ferricstore.API.Streams
Ferricstore.API.Flow
Ferricstore.API.Server
Ferricstore.API.Probabilistic
Ferricstore.API.Transactions
Ferricstore.API.Blobs
```

Responsibilities:

```text
Ferricstore: compatibility facade using direct delegates
Ferricstore.API.*: public docs, argument normalization, stable API surface
Domain modules: actual behavior and storage interaction
```

Rules:

```text
Keep existing public function names working.
Do not change return shapes.
Do not add new behavior while splitting.
Prefer defdelegate where no argument normalization is needed.
```

Tests:

```text
Existing public API tests should keep passing unchanged.
Add a small facade compatibility test for representative delegated functions.
```

Benchmark requirement:

```text
No DBOS/memtier benchmark required if facade delegates directly and no hot logic changes.
```

## Area 3: Flow command modules

Current issue:

```text
Flow logic mixes create, claim_due, transition, terminal commands, value refs, signals, history, policy, retention, index ops, validation, and responses.
```

Target modules:

```text
Ferricstore.Flow
Ferricstore.Flow.Record
Ferricstore.Flow.History
Ferricstore.Flow.ValueRefs
Ferricstore.Flow.IndexOps
Ferricstore.Flow.Validation
Ferricstore.Flow.Response
Ferricstore.Flow.Retention
Ferricstore.Flow.Policy
Ferricstore.Flow.Signals
Ferricstore.Flow.Commands.Create
Ferricstore.Flow.Commands.ClaimDue
Ferricstore.Flow.Commands.Transition
Ferricstore.Flow.Commands.Terminal
Ferricstore.Flow.Commands.Retry
Ferricstore.Flow.Commands.Cancel
Ferricstore.Flow.Commands.Rewind
Ferricstore.Flow.Commands.Values
Ferricstore.Flow.Commands.History
Ferricstore.Flow.Commands.Query
```

Responsibilities:

```text
Commands.*: command-specific validation and plan construction
Record: FlowState shape, encode/decode helpers, durable state helpers
History: history event construction and history response shaping
ValueRefs: named value refs, payload refs, idempotent put, fetch/mget, retention metadata
IndexOps: lifecycle/due/running/history/lineage index operation construction
Validation: reusable guards for type, state, partition, lease, priority, fencing
Response: response contracts for create/transition/terminal/claim/query
Retention: terminal TTL and cleanup policy helpers
Policy: retry and execution policy helpers
Signals: signal records and delivery helpers
```

Rules:

```text
Payload bytes stay outside Flow state.
Commands return staged writes and index ops, not hidden side effects.
Only claim_due hydrates records/payloads when contract requires it.
Terminal commands return compact OK-style responses.
Create/transition/terminal hot paths must not call query/projection modules.
```

Tests:

```text
Split flow_test.exs by command group.
Split flow_lmdb_test.exs by projection/retention/history behavior.
Keep public command behavior unchanged.
Add focused tests around value ref semantics and response contracts where needed.
```

Benchmark requirement:

```text
Run DBOS baseline after Flow hot-path extraction.
Run memtier only if Router/StateMachine/KV path changed.
```

## Area 4: Router split

Current issue:

```text
Router mixes shard resolution, quorum routing, forwarded writes, local reads, async latches, blob GC, pressure/backpressure, and batch calls.
```

Target modules:

```text
Ferricstore.Store.Router
Ferricstore.Store.Router.Shards
Ferricstore.Store.Router.Read
Ferricstore.Store.Router.Write
Ferricstore.Store.Router.Batch
Ferricstore.Store.Router.Quorum
Ferricstore.Store.Router.Forwarding
Ferricstore.Store.Router.AsyncLatches
Ferricstore.Store.Router.BlobGC
Ferricstore.Store.Router.Pressure
```

Responsibilities:

```text
Router: public store routing facade
Shards: shard id resolution and shard process lookup
Read: read-only routing
Write: single command write routing
Batch: batch grouping and dispatch
Quorum: Ra quorum write path wrappers
Forwarding: node-local and remote forwarding
AsyncLatches: async command tracking and wait logic
BlobGC: blob sweep/reconcile/live-ref coordination
Pressure: overload checks and retry-after shaping
```

Rules:

```text
Keep shard selection deterministic.
Do not add extra binary parsing/classification to hot paths.
Do not change retry/backpressure response shape unless explicitly planned.
Avoid per-command allocation growth.
```

Tests:

```text
Move router tests by behavior: reads, writes, batching, blob GC, pressure, cold due.
Keep integration tests unchanged unless names only change.
```

Benchmark requirement:

```text
Run DBOS baseline.
Run memtier SET and GET baseline.
Commit only if performance stays within baseline noise.
```

## Area 5: BlobStore split

Current issue:

```text
BlobStore mixes write/read/range/verify/recovery/GC/protection/path safety/statistics in one module.
```

Target modules:

```text
Ferricstore.Store.BlobStore
Ferricstore.Store.BlobStore.Writer
Ferricstore.Store.BlobStore.Reader
Ferricstore.Store.BlobStore.Verify
Ferricstore.Store.BlobStore.Segment
Ferricstore.Store.BlobStore.Recovery
Ferricstore.Store.BlobStore.GC
Ferricstore.Store.BlobStore.Protection
Ferricstore.Store.BlobStore.PathSafety
Ferricstore.Store.BlobStore.Stats
```

Responsibilities:

```text
BlobStore: public facade
Writer: put/append/commit logic
Reader: get/range/open file logic
Verify: checksum and integrity validation
Segment: segment naming, allocation, offsets
Recovery: startup scan and rebuild
GC: mark/sweep/delete candidate logic
Protection: hardening/pinning unknown commit outcomes
PathSafety: safe path construction and traversal defense
Stats: counters and metrics
```

Rules:

```text
Unknown commit outcomes remain safe.
GC must not delete protected blobs.
Path safety remains centralized.
No hidden payload materialization in terminal/retention paths.
```

Tests:

```text
Split blob tests by writer, reader, recovery, GC, protection, side-channel behavior.
Keep crash/recovery tests behavior-focused.
```

Benchmark requirement:

```text
Run DBOS baseline if Flow value refs or terminal payload handling changed.
Run memtier only if generic KV write/read path changed.
```

## Area 6: WARaftStorage split

Current issue:

```text
WARaftStorage mixes storage callbacks, segment projection, value projection, history trim, metadata, replay dependency logic, snapshots, and recovery.
```

Target modules:

```text
Ferricstore.Raft.WARaftStorage
Ferricstore.Raft.WARaftStorage.Open
Ferricstore.Raft.WARaftStorage.Apply
Ferricstore.Raft.WARaftStorage.Snapshot
Ferricstore.Raft.WARaftStorage.SegmentProjection
Ferricstore.Raft.WARaftStorage.ValueProjection
Ferricstore.Raft.WARaftStorage.HistoryTrim
Ferricstore.Raft.WARaftStorage.ReplayDependencies
Ferricstore.Raft.WARaftStorage.Metadata
Ferricstore.Raft.WARaftStorage.Recovery
```

Responsibilities:

```text
WARaftStorage: behavior callback wrapper
Open: storage initialization and path setup
Apply: top-level apply delegation
Snapshot: snapshot read/write/install helpers
SegmentProjection: segment command projection
ValueProjection: value-ref and blob projection
HistoryTrim: history retention and trim operations
ReplayDependencies: cursor/watermark dependency tracking
Metadata: durable metadata read/write
Recovery: startup recovery and repair
```

Rules:

```text
Do not change Ra log semantics.
Do not change snapshot metadata compatibility.
Do not change replay dependency persistence without migration plan.
Keep recovery idempotent.
```

Tests:

```text
Split WARaft backend/storage tests by storage behavior, snapshots, recovery, projection, replay dependencies.
Keep crash/recovery coverage intact.
```

Benchmark requirement:

```text
Run DBOS baseline.
Run memtier SET and GET baseline.
Commit only if performance stays at baseline.
```

## Area 7: Raft StateMachine split

Current issue:

```text
StateMachine is the highest-risk module. It owns command apply semantics and many hot paths.
```

Target modules:

```text
Ferricstore.Raft.StateMachine
Ferricstore.Raft.StateMachine.State
Ferricstore.Raft.StateMachine.Dispatch
Ferricstore.Raft.StateMachine.PendingBatch
Ferricstore.Raft.StateMachine.ReleaseCursor
Ferricstore.Raft.StateMachine.Checkpoint
Ferricstore.Raft.StateMachine.ReplayDependencies
Ferricstore.Raft.StateMachine.CrossShardTx
Ferricstore.Raft.StateMachine.Metrics
Ferricstore.Raft.StateMachine.Apply.KV
Ferricstore.Raft.StateMachine.Apply.Batch
Ferricstore.Raft.StateMachine.Apply.Compound
Ferricstore.Raft.StateMachine.Apply.Flow
Ferricstore.Raft.StateMachine.Apply.Probabilistic
Ferricstore.Raft.StateMachine.Apply.Transaction
Ferricstore.Raft.StateMachine.Apply.Locking
Ferricstore.Raft.StateMachine.Apply.Server
```

Responsibilities:

```text
StateMachine: Ra callbacks and top-level apply orchestration
State: state struct/defaults/init helpers
Dispatch: explicit command-domain routing
PendingBatch: pending write staging and flush helpers
ReleaseCursor: release cursor and durable release logic
Checkpoint: checkpoint state and checkpoint side effects
ReplayDependencies: dependency tracking during replay/release
CrossShardTx: transaction log and idempotent replay helpers
Metrics: telemetry emission helpers
Apply.*: domain-specific command apply logic
```

Rules:

```text
Keep apply dispatch explicit.
No generic command behavior registry in the hot path.
Do not change command tuples or persisted command shape.
Do not change reply contracts.
Do not change index/write flush order.
Do not change backpressure semantics.
```

Tests:

```text
Split state_machine_test.exs by apply domain and lifecycle behavior.
Keep integration-level raft tests intact.
Add narrow tests only where extraction exposes missing edge coverage.
```

Benchmark requirement:

```text
Run DBOS baseline.
Run memtier SET and GET baseline.
Commit only if performance stays at baseline.
```

## Performance gate

Current local baseline is stored in:

```text
docs/local-clean-code-baseline.md
```

Required DBOS command shape:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/dbos_style_benchmark.py --mode queued --transport many --flows 1000000 --server-shards 16
```

Required memtier shape:

```text
--clients=200 --threads=4 --pipeline=50
```

In memtier this means:

```text
200 connections per thread
4 threads
800 total connections
50 pipeline depth
40000 in-flight requests
```

Baseline numbers:

```text
DBOS-style Flow 1M: ~73k e2e workflows/s
SET: ~756k ops/s
GET: ~5.1M ops/s
```

Benchmark acceptance:

```text
Small noise is acceptable.
Material regression is not acceptable.
If regression appears, profile/fix before commit.
```

## Commit policy

Each area gets its own commit after passing its gate.

Commit message pattern:

```text
Refactor dashboard modules
Refactor public API facade
Refactor flow command modules
Refactor router modules
Refactor blob store modules
Refactor waraft storage modules
Refactor raft state machine modules
```

Do not mix unrelated cleanup into an area commit.

Do not delete behavior while splitting.

Do not remove tests just to make the split easier.

## Done definition

The cleanup is done when:

```text
No production module exceeds 1000 lines.
Oversized tests are split by domain.
Public APIs remain compatible.
Targeted tests pass for every area.
DBOS/memtier baselines are preserved after hot-path splits.
Each area is committed separately.
The module graph is understandable from names alone.
```
