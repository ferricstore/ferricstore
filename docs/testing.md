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

## Linked Worktree Smoke Test

Development configuration gives linked checkouts their own data directory and
OS-assigned listener ports. After `mix compile`, verify this with:

```bash
elixir tools/worktree-isolation-test.exs
```

The test creates two temporary linked worktrees, starts independent server
processes, checks all four listeners per process, and writes different values
under the same key. It then restarts one process and verifies that both values
remain isolated. Only test-owned checkouts and data are removed afterward.
The harness reuses the existing compiled application read-only, so do not run
a build at the same time. It does not replace clean-build or release tests.

## Storage Recovery Regressions

Run these focused groups serially. They use test-owned paths; do not point them
at a production volume or a preserved incident volume.

```bash
mix test apps/ferricstore/test/ferricstore/store/blob_store_test.exs \
  apps/ferricstore/test/ferricstore/store/blob_store_owner_crash_test.exs \
  apps/ferricstore/test/ferricstore/store/blob_store_reentrant_lock_test.exs \
  apps/ferricstore/test/ferricstore/store/blob_store_empty_payload_test.exs \
  apps/ferricstore/test/ferricstore/store/blob_store_boundary_matrix_test.exs \
  apps/ferricstore/test/ferricstore/store/blob_store_tail_recovery_test.exs \
  apps/ferricstore/test/ferricstore/store/promotion_raft_recovery_test.exs \
  apps/ferricstore/test/ferricstore/store/shard_torn_tail_recovery_test.exs \
  apps/ferricstore/test/ferricstore/flow/history_projector/tombstone_recovery_test.exs

mix test apps/ferricstore/test/ferricstore/flow/lmdb_writer/restart_recovery_test.exs \
  apps/ferricstore/test/ferricstore/flow/policy_mirror_recovery_test.exs \
  apps/ferricstore/test/ferricstore/flow_lmdb_test.exs

mix test apps/ferricstore/test/ferricstore/flow_production_recovery_test.exs \
  apps/ferricstore/test/ferricstore/store/promotion_delete_restart_test.exs \
  apps/ferricstore/test/ferricstore/flow/policy_mirror_writer_restart_test.exs \
  --include shard_kill

cargo test --manifest-path apps/ferricstore/native/ferricstore_bitcask/Cargo.toml \
  log::tests -- --test-threads=2
```

The regressions distinguish a repairable incomplete active tail from checksum
corruption or an incomplete sealed file, which must fail without discarding
bytes. An incomplete body is checked for complete CRC-valid records behind the
damage before truncation. Recovery scans large tails in bounded-memory windows
and caps speculative CRC work at 32 MiB per 1 MiB window. This keeps work linear
in the tail size, including for large zero-filled interrupted values. Finding a
record candidate or exceeding the checksum-work budget stops recovery and
preserves the original file. Preserve a copy for diagnosis rather than
manually truncating an ambiguous tail. Short incomplete headers remain directly
repairable. Hint metadata is durably invalidated before repairing a covered
tail, so subsequent appends cannot make the obsolete hint valid again.

Blob tests also kill callers after complete and partial writes, exercise
competing lock takeovers, and check exceptional exits. A replacement owner must
invalidate cached append/recovery state before another append can use it.
Healthy writes must retain the cached path without recovery scans. Ambiguous
blob tails are checked using their format magic and SHA-256 checksum, with
1 MiB windows and a 32 MiB total speculative hashing budget; recovery preserves
files it cannot validate safely. Large interrupted writes must remain
recoverable without allocating the whole suffix. Tests cover records and
headers crossing probe-window boundaries.

The blob boundary matrix checks every header alignment at a probe boundary for
empty and nonempty payloads. Empty reads must still validate the segment, header,
and payload checksum; a zero-byte read at EOF must not be treated as a missing
blob or bypass corruption checks.

Writer restart tests interrupt queued LMDB work and verify primary records and derived indexes,
cold-source preservation, and repeated recovery. History tests cover durable
tombstones, bounded deletion batches, and retry after publication failures.

The Raft-owned startup path validates the active Bitcask log before opening
its append writer. This adds startup I/O proportional to that active file;
it is not a per-request scan. Measure cold-disk restart time separately from
warm-cache microbenchmarks. Linux `io_uring` coverage still requires Linux CI.

After a test build, run the bounded policy-recovery microbenchmark with:

```bash
elixir -pa '_build/test/lib/*/ebin' tools/recovery-second-review-bench.exs
```

It validates read results and reports median scalar/batched read and full
reconciliation timings using temporary data. These warm-cache measurements
are not end-to-end restart or production request-throughput measurements.

The blob append benchmark checks every returned reference and rejects an
unexpected recovery scan during healthy appends:

```bash
elixir -pa '_build/test/lib/*/ebin' tools/blob-recovery-bench.exs
```

Its synced-write timings depend heavily on local filesystem latency. Compare
repeated runs on the same host; do not extrapolate them to production throughput.

### Idle CPU and Empty Claims

After a test build, run the isolated 16-shard idle benchmark with the production
Flow scheduler enabled:

```bash
elixir --erl '+S 16:16 +sbwt very_short +swt very_low +A 128' \
  -pa '_build/test/lib/*/ebin' tools/idle-cpu-bench.exs
```

It creates temporary storage, uses ephemeral ports, disables discovery, and
reports three ten-second samples of VM CPU, literal-collector reductions, and
shard writes. It does not connect to or modify existing FerricStore instances.
Optional arguments load a saved segment-log BEAM and a saved Router BEAM before
startup for same-toolchain A/B comparisons. Run variants sequentially without
concurrent builds or workloads. VM runtime measurements are not Docker's whole
container CPU metric or a PostgreSQL comparison.

The `config_cache_idle` regression verifies that appends advance the negative
Raft configuration cache without replacing its global term. The `idle_claim`
regressions cover empty all-partition claims, subsequent arrivals, future
deadlines, escaped names, bounded-page fallback, and hibernated records. The
scheduler test also checks that entering a blocking wait does not append empty
claims and that a future schedule still wakes and fires.

The all-partition empty check is deliberately conservative: it is limited to a
single local Raft member and a complete page of fewer than 128 due-index keys.
Unknown state, an incomplete page, missing storage, or cold records fall back
to the authoritative claim path. There is no cached negative result that can
outlive a new arrival. Hot keys are rechecked around the cold-store lookup to
avoid hiding a concurrent promotion.

### Operational Sampling

The operational guard reads disk capacity through a dirty-I/O
[`statvfs`](https://www.man7.org/linux/man-pages/man3/statvfs.3.html) NIF,
without launching `df`. Used bytes exclude filesystem-reserved free blocks;
available bytes remain the unprivileged allowance. Errors and overflow remain
unknown capacity. Explicit positive memory limits skip host detection; absent
or invalid overrides still trigger fresh detection. Monitoring intervals are
unchanged, and no memory or disk measurements are cached.

`operational_sampling_test.exs` traces actual calls to enforce the no-subprocess
and lazy-detection contracts. Native `system_capacity` tests cover reserved
blocks, zero counts, overflow, and path errors.

Save the old `Elixir.Ferricstore.OperationalLimits.beam` before rebuilding, then
compare the same snapshot path with fixed memory/RSS inputs and real disk reads:

```bash
elixir --erl '+S 2:2' -pa '_build/test/lib/*/ebin' \
  tools/operational-sampling-bench.exs /absolute/path/to/before.beam
```

The benchmark alternates five before/after rounds of 200 samples, reporting
wall time, VM CPU time, and reductions. This is a monitoring-path microbenchmark,
not total server CPU. For an isolated whole-VM idle comparison, set
`FERRICSTORE_IDLE_LIMITS_BEAM` to the saved module when running
`tools/idle-cpu-bench.exs`, then rerun without that variable.
Alternatively, `FERRICSTORE_IDLE_COMPARE_LIMITS_BEAM` alternates old/new modules
over six measured windows in the same warmed server, reducing between-instance
noise. Use only one of these environment variables at a time.
