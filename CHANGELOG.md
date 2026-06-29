# Changelog

All notable changes to FerricStore will be documented here.

## Unreleased

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
