# Redis Migration Guide

FerricStore exposes Redis-compatible commands where the engine can preserve
the expected behavior, and it exposes FerricStore-native commands for durable
Flow, management, and observability features. Treat migration as a workload
assessment, not only a protocol swap.

## Compatibility Matrix

Generate the public compatibility matrix from the command metadata in this
repo:

```sh
mix ferricstore.redis_compat matrix --format markdown
mix ferricstore.redis_compat matrix --format json --output redis-compat.json
```

The matrix uses `Ferricstore.Commands.Catalog` first. Commands accepted by the
native parser but not yet represented in full Redis `COMMAND` metadata are
marked with the native parser source. Important unsupported Redis commands are
listed from the migration catalog so assessment reports can fail closed.

## Workload Assessment

Assess a command trace before migration:

```sh
mix ferricstore.redis_compat assess redis-monitor.log --format markdown
mix ferricstore.redis_compat assess commandstats.txt --format json
```

Supported input formats:

- Redis `MONITOR` style lines, where the first quoted token is the command.
- `INFO commandstats` lines such as `cmdstat_get:calls=10,...`.
- Simple command-per-line traces such as `SET key value`.

Assessment reports count compatible, different, partial, unsupported, extension,
and unknown commands. Unknown commands should be investigated before production
migration.

## High-Priority Differences

`SELECT` is intentionally different. FerricStore does not expose Redis numbered
databases; map each Redis logical DB to a named cache or namespace.

Lua commands such as `EVAL`, `EVALSHA`, `SCRIPT`, and Redis Functions are not
supported. Move server-side logic to FerricFlow workflows or to application-side
command batches with explicit error handling.

Redis replication and live key movement commands such as `MIGRATE`, `SYNC`,
`PSYNC`, `REPLICAOF`, and `SENTINEL` are not migration paths for FerricStore.
Use FerricStore-native clustering and import workflows instead.

## Import Strategy

Prefer logical migration over binary Redis internals:

- Use application-level dual writes or command replay when the source workload
  is still live.
- Use sanitized AOF replay for workloads that mostly use supported commands.
- Use explicit export jobs for hashes, sets, sorted sets, lists, streams, and
  strings when you need validation or transformation.
- Treat RDB files as source backups, not as a direct FerricStore import format.
  Convert them through a reviewed logical export path before loading.
- Validate TTL, stream IDs, sorted-set scores, and hash/list/set cardinalities
  after import.

Do not import secrets or payloads into assessment reports. The assessment tool
only needs command names and counts.

## Compatibility Tests

The compatibility test suite covers high-priority Redis commands in the matrix
and verifies that catalog-backed entries remain aligned with command metadata.
Behavioral Redis compatibility tests live under
`apps/ferricstore/test/ferricstore/commands/redis_compat*_test.exs`.
