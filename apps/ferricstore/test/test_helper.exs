# :bench — long-running throughput/latency benchmarks; run with `mix test --include bench`
# :linux_io_uring — tests that require Linux + io_uring kernel support (≥ 5.1).
#   Always excluded from the default run (`mix test`) on all platforms.
#   In CI, the dedicated workflow step runs them with `mix test --only linux_io_uring`
#   only when io_uring is confirmed available on the runner.
#   Locally on Linux, run them with `mix test --include linux_io_uring`.
# :large_alloc — tests that allocate ≥ 512 MiB in-process to verify the size
#   guard fires. Excluded by default to avoid OOM on CI runners with < 2 GB RAM.
#   Run locally with `mix test --include large_alloc`.
# :cluster — multi-node `:peer` cluster tests (OTP 25+). These spin up
#   multiple BEAM nodes and are slow (~10-30s per test). Excluded by default.
#   Run with `mix test --include cluster` or
#   `mix test test/ferricstore/cluster/ --include cluster`.
# :jepsen — Jepsen-style durability and consistency tests that spin up multi-node
#   `:peer` clusters and inject faults. Excluded by default; run with
#   `mix test test/ferricstore/jepsen/ --include jepsen`.
# :bench — long-running throughput benchmarks (30s+ per test). Excluded by default.
#   Run with `mix test test/ferricstore/cluster/throughput_bench_test.exs --include bench`.
# :perf_guard — short bounded latency/throughput regression guards. Excluded
#   from default local runs and exercised by the dedicated CI perf-guard job.
ExUnit.start(
  exclude: [
    :bench,
    :perf_guard,
    :linux_io_uring,
    :large_alloc,
    :cluster,
    :jepsen,
    :shard_kill,
    :partition_test
  ],
  formatters: [ExUnit.CLIFormatter, JUnitFormatter, Ferricstore.Test.AuditFormatter]
)

# NOTE: data directory cleanup is handled by the ferricstore_server app's
# test_helper.exs which runs last in the umbrella. We must NOT delete the
# data directory here because in umbrella `mix test`, after_suite callbacks
# fire between apps while the application supervisor (and its shards) are
# still running. Deleting the data directory here causes "No such file or
# directory" errors for all shard writes in the ferricstore_server test suite.
