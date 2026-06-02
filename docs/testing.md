# Testing Notes

FerricStore tests share process-wide runtime state: app supervision, ETS tables, data directories, LMDB projectors, atomics/counters, and background shard processes.

## Rule

Run only one full FerricStore test command at a time from a working tree.

Safe:

```bash
mix test apps/ferricstore/test/ferricstore/flow_test.exs
```

Safe:

```bash
mix test apps/ferricstore_server/test
```

Unsafe:

```bash
# two shells at the same time in the same working tree
mix test apps/ferricstore/test
mix test apps/ferricstore_server/test
```

## Why

A single ExUnit run can execute async test modules safely because the suite controls setup and cleanup ordering. Separate `mix test` processes do not coordinate app shutdown, data directory cleanup, or shared projectors, so they can interfere with each other.

## Practical Guidance

- Use targeted file-level tests while developing.
- Use one full `mix test` run before release or large merges.
- Use separate git worktrees and separate data directories if you need truly parallel full-suite runs.
- Do not commit generated test data, `test-results/`, `_build/`, or `deps/`.
