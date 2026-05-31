# Testing Notes

## Shared App Suite Isolation

Do not run multiple full-app `mix test` invocations at the same time. Suites
such as Flow, state machine, cluster, Jepsen, and restart tests boot or mutate
global FerricStore state: registered shard names, Ra/WARaft systems, default
data directories, LMDB projection writers, atomics/counters, and process-wide
configuration. Running two of those suites in parallel can create false
failures that do not reproduce in isolation.

Safe pattern:

```sh
mix test apps/ferricstore/test/ferricstore/flow_codec_test.exs apps/ferricstore/test/ferricstore/flow_test.exs --max-failures 5
mix test apps/ferricstore/test/ferricstore/raft/state_machine_test.exs --max-failures 3
(cd apps/ferricstore/native/ferricstore_bitcask && cargo test)
```

Avoid:

```sh
cmd1 & cmd2 &
```

Also avoid `multi_tool_use.parallel` for full-app test commands. Parallel shell
reads such as `rg`, `sed`, `git diff`, and isolated Rust unit tests are fine.

Within one `mix test` invocation, ExUnit concurrency is acceptable where the
test modules opt into it. The important rule is one FerricStore app/runtime test
process at a time.
