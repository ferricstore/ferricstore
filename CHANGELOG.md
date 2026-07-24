# Changelog

All notable changes to FerricStore will be documented here.

## Unreleased

## 0.10.3 - 2026-07-24

- Added bounded, source-specific FQL1 result fields after `RETURN RECORD` and `RETURN RECORDS`, including whole or leaf metadata maps, sparse binary result rows, canonical cursor binding, and `plan.projection` EXPLAIN output. Projection is applied after authoritative validation and reduces result memory/encoding/transfer work without weakening scan, hydration, ACL, or budget checks.
- Added the explicitly negotiated `flow_query_result_v1` native response codec for `FLOW.QUERY` record pages and counts, using fixed metadata, row-presence bitmaps, and typed field values while retaining lossless typed-value fallback for EXPLAIN, errors, and unknown result shapes. Clients name supported compact codecs in `HELLO` or `STARTUP`, preventing broad compact-response opt-in from enabling unknown future tags.
- Removed the full-record intermediate from explicit fixed-index projections after a benchmark showed about 4x higher projection throughput and 2.9x lower allocation, while retaining authoritative ordering, cursor, integrity, and memory-budget checks.
- Added a byte-for-byte compact-result golden corpus for server and SDK conformance, made the native NIF ABI derive the release version from the umbrella project, and allowed clients to ignore well-formed unknown future response codecs without weakening duplicate-opcode validation.
- Kept native responses uncompressed when an established connection state omits optional compact-codec negotiation, while retaining fail-closed selection for malformed or unsupported codec declarations.
- Made completed query-index validation restart-safe before atomic activation, allowing the lifecycle worker to resume a durably validated build without exposing it early or rejecting the registry snapshot.

## 0.10.2 - 2026-07-23

- Made the native topology and event-subscription control commands individually grantable through ACLs and the connection category, enabling least-privilege topology-aware SDK sessions without broad command permissions.

## 0.10.1 - 2026-07-23

- Fixed the standalone container build to package the OSS query-index catalog required during startup.
- Fixed LMDB reconciliation to preserve valid cold terminal projections, remove stale reverse rows, rebuild exact terminal counts in bounded pages, and serialize writers with reconciliation per shard without blocking independent shards.
- Added native-port startup smoke tests for every CI image and each release architecture before publishing a multi-arch manifest.

## 0.10.0 - 2026-07-23

- Added the complete cost-aware `FLOW.QUERY` planner, composite index lifecycle, bounded executor, statistics, actionable diagnostics, index management, and `EXPLAIN`/`EXPLAIN ANALYZE` UX to OSS; Enterprise consumes the same implementation and contracts.
- Removed the beta wire commands `FLOW.LIST`, `FLOW.SEARCH`, `FLOW.TERMINALS`, `FLOW.FAILURES`, `FLOW.STUCK`, `FLOW.BY_PARENT`, `FLOW.BY_ROOT`, and `FLOW.BY_CORRELATION`; clients must use `FLOW.QUERY` and negotiate its advertised shapes.
- Unified point, history, lineage, fixed-index, composite, and count plans under `ferric.flow.query.result/v1` and `ferric.flow.explain/v1`, with one edition-neutral capability manifest containing every executable shape.
- Added memory-weighted query admission, pressure-aware index backfill, aggregate native merge limits, batched selected-plan statistics probes, exact one-pass response accounting, and resumable bounded-prefix LMDB hydration.
- Bounded fixed-index fallback candidate hydration to one maximum result page plus look-ahead, and kept namespace quota accounting distinct from Flow secondary indexes.
- Isolated query-only LMDB projection from scheduler and terminal lifecycle indexes, and bound MemoryGuard pressure publication to the supervised default guard so auxiliary guards cannot alter production admission state.
- Added lossless unsigned 64-bit native values for query catalog versions, registry epochs, and lifecycle counters while retaining the existing signed integer tag for ordinary command values.
- Added the `ferric.flow.query.indexes/v1` index-status contract to query capability negotiation so clients reject incompatible management responses during `HELLO` instead of after a query.

## 0.9.1 - 2026-07-19

- Advertised and accepted `expected_generation` and `replace` in typed-native `FLOW.POLICY.SET`, aligning native SDK policy CAS and replacement with the embedded and textual command paths.

## 0.9.0 - 2026-07-19

- Added durable per-state FIFO/parallel Flow execution, with exact partition-lane ordering across hibernation and restart, plus `mode :fifo | :parallel` declarations in the Elixir workflow DSL.
- Made direct Flow policy updates merge atomically by default with replicated generation compare-and-swap; set/get responses now expose `generation`, while workflow `install_policy/1` replaces its declaration snapshot by default.
- Changed workflow-derived composite partition keys to collision-free length-prefixed encoding. Explicit partition keys are unchanged.
- Fixed LMDB startup recovery to fall back to synchronous keydir reconciliation when an active Flow projection is invalid, and to rebuild the durable active projection before serving claims.

## 0.8.0 - 2026-07-18

- Default governance effect lookups to the Flow ID auto-partition when no explicit partition is provided.
- Added type-level and per-Flow `max_active_ms` limits with durable timeout failure history, cold-record enforcement, and parent/child coordination.
- Exposed maximum active runtime through embedded, native TCP, workflow, and dashboard policy surfaces, including an `infinity` override.
- Made Flow timeout sweeping instance-scoped and surfaced active timeout candidates and counts in retention maintenance views.
- Hardened native and health-server request handling with bounded decode/output work, HTTP framing and deadlines, CSRF/session checks, login throttling, and dashboard ACL redaction.
- Reserved internal Flow storage keys across public command, pipeline, transaction, embedded, and dashboard paths, and made shared-reference/retention backfills bounded, resumable, and fail-closed across destructive resets.
- Made Flow policy fan-out failure-atomic with bounded deterministic hot-record reindexing, repaired WARaft cold-row compensation, and routed ACL mutations through replicated invalidating paths with bounded protected-mode and load checks.
- Added monotonic internal Flow policy generations and command-captured policy snapshots so policy-sensitive work remains deterministic across independently ordered Raft groups without replacing public policy versions.
- Replaced synchronous Flow policy reindex scans with a durable exact-type catalog, bounded resumable catalog backfill, and replicated migration plans whose explicit candidates are safe to replay on every replica.
- Added a shared prepared-command contract for parsing, ACL keys, routing keys, and read/write footprints, and moved Flow apply limits into a compact replicated context persisted with WARaft recovery metadata.
- Canonicalized the beta-only Flow command, projection outbox, and dirty-marker contracts and removed obsolete compatibility paths.
- Fenced direct LMDB enqueues, outbox rows, and reconciliation markers by writer generation so destructive resets and snapshots cannot publish stale projection work.
- Made WARaft projection recovery authoritative, fenced client writes during restart recovery, hardened promotion recovery and compaction resource release, and prevented deleted projected values from being resurrected.
- Made expiry, scheduling, history, replay, and maintenance decisions deterministic from replicated command time and HLC state.
- Bounded batch, transaction, frame, response, allocation, fan-out, reindex, and cleanup work while preserving targeted hot paths and projection batching.
- Hardened ACL, protected-mode, TLS, path, corruption, admission, rate-governance, and global-concurrency enforcement to fail closed.

## 0.7.5 - 2026-07-08

- Added dashboard views for Flow details, capabilities, messaging, streams, Pub/Sub, and security state.
- Added bounded stream and Pub/Sub activity logs with ACL-filtered dashboard live payloads.
- Expanded health endpoint routing, dashboard access checks, and observability coverage.

## 0.7.4 - 2026-07-07

- Fixed lagged LMDB terminal Flow cleanup so stale cold keydir rows are pruned only after durable projection while startup rebuild remains non-destructive.
- Preserved LFU Flow state version wrappers during hotness updates for cold Flow rows.
- Added FIFO Flow coverage for partition-wide claims, explicit partition lists, batched claims, expired lease reclaim, state transitions, restart replay ordering, RESP command dispatch, and native opcode execution.

## 0.7.3 - 2026-07-07

- Added native `FLOW.POLICY.SET` support for indexed Flow attributes.
- Added native and `COMMAND_EXEC` coverage for `FLOW.SEARCH` attribute and state metadata queries.
- Rejected state-scoped indexed Flow policy options that are type-level only.
- Updated native protocol and command guides for newer Flow opcodes and `FLOW.SEARCH`.

## 0.7.2 - 2026-07-06

- Removed the embedded Elixir SDK app from this repository; the Elixir SDK now lives in the standalone SDK repository.
- Added `FLOW.SEARCH` support to the native `COMMAND_EXEC` parser and dispatcher.
- Hardened namespace-scope enforcement for `FLOW.SEARCH` over raw and typed native commands.

## 0.7.1 - 2026-07-05

- Added native `FLOW.SEARCH` support for indexed per-state metadata queries.
- Exposed Flow `state_meta`, `indexed_state_meta`, and `FLOW.SEARCH` through the embedded Elixir SDK.
- Added SDK and server coverage for state metadata indexing/search and HA topology refresh after node restart/rejoin.

## 0.7.0 - 2026-07-04

- Added HA-aware native route metadata for `HELLO`, `ROUTE`, `ROUTE_BATCH`, and `SHARDS`, including advertised native endpoints and leader hints.
- Added the embedded Elixir native SDK with topology-aware routing, keyed KV helpers, Flow helpers, and admin/governance helpers.
- Hardened SDK rerouting so automatic replay is limited to connection-open/send failures and explicit native `REROUTE`; post-send close/timeout results are surfaced to callers as unknown outcomes.
- Added SDK durability coverage for routed leader loss, no-quorum failures, and topology refresh recovery.
- Exposed state metadata in the governance dashboard and Flow detail views using bounded indexed `state_meta` queries.
- Hardened native route/governance ACL checks and expanded CI coverage for SDK and durability routing paths.

## 0.6.0 - 2026-07-01

- Added bounded per-state Flow metadata with durable record/history encoding.
- Added one policy-controlled indexed state metadata key per Flow type for indexed search.
- Added automatic backfill and stale index cleanup when a Flow type changes its indexed state metadata key.
- Added native command support for `STATE_META` mutation options and `INDEXED_STATE_META` Flow policy configuration.
- Added LMDB projection, rebuild, and retention cleanup coverage for indexed state metadata rows.
- Exposed Flow policy and retention cleanup functions through embedded `use FerricStore` instances.

## 0.5.6 - 2026-06-29

- Added trusted native request context for extension command execution.
- Gated native `request_context` acceptance behind trusted native users so arbitrary clients cannot spoof control-plane authority.
- Propagated trusted request context from typed pipelines into nested `COMMAND_EXEC` extension commands.
- Added `FERRICSTORE_NATIVE_TRUSTED_REQUEST_CONTEXT_USERS` for production configuration.

## 0.5.4 - 2026-06-27

- Added the command extension interface for optional command providers.
- Exposed extension commands through native command execution, catalog metadata, and ACL categories.
- Hardened built-in command precedence so extension metadata cannot shadow core routing, key metadata, or ACL access classes.
- Stabilized embedded ACL regression tests by explicitly waiting for the shared default write path.

## 0.5.3 - 2026-06-23

- Updated public docs for the Ferric native TCP protocol architecture after RESP removal.
- Removed stale Redis/RESP protocol wording from public docs and dashboard copy.
- Replaced obsolete RESP benchmark helpers with native-protocol benchmark guidance.
- Fixed `INFO server` fields to report FerricStore/native protocol names without the legacy `tcp_port` fallback.
- Aligned the shard active-file fallback default with the 8 GiB runtime default.
