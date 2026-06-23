# Changelog

All notable changes to FerricStore will be documented here.

## Unreleased

## 0.5.3 - 2026-06-23

- Updated public docs for the Ferric native TCP protocol architecture after RESP removal.
- Removed stale Redis/RESP protocol wording from public docs and dashboard copy.
- Replaced obsolete RESP benchmark helpers with native-protocol benchmark guidance.
- Fixed `INFO server` fields to report FerricStore/native protocol names without the legacy `tcp_port` fallback.
- Aligned the shard active-file fallback default with the 8 GiB runtime default.
