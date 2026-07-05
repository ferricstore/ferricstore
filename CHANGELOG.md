# Changelog

All notable changes to FerricStore will be documented here.

## Unreleased

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
