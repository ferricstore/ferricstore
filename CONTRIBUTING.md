# Contributing

Thanks for helping improve FerricStore. This repo is focused on FerricStore core, FerricFlow, and the native TCP server.

## Development Setup

Required tools:

- Elixir >= 1.20
- Erlang/OTP >= 29
- Rust stable toolchain
- `mix local.hex` and `mix local.rebar`

Install dependencies:

```bash
mix deps.get
```

Compile:

```bash
mix compile
```

Run the development server locally:

```bash
mix run --no-halt scripts/run_dev.exs
```

The launcher prints the actual native, dashboard, and health-probe endpoints
and the data directory. Linked Git worktrees use checkout-local data and
OS-assigned ports, so they can run beside the main checkout. The ports can
change after a restart; use the printed endpoints when configuring SDKs or
browser checks. Tests use separate temporary data directories and ephemeral
ports for each run.

Do not share a data directory, build directory, or native build output between
running checkouts. Explicit endpoint or data-directory overrides are your
responsibility to keep distinct. Production release configuration is separate
from these development defaults.

Build a release:

```bash
MIX_ENV=prod mix release ferricstore
```

## Tests

Run the full test suite when changing core behavior:

```bash
mix test
```

Run targeted tests while developing:

```bash
mix test apps/ferricstore/test/ferricstore/flow_test.exs
mix test apps/ferricstore_server/test
```

## Formatting And Static Checks

```bash
mix format
mix credo
```

## Benchmarks

Stable benchmark entry points live in `bench/`. Public benchmark results are summarized in `docs/benchmarks.md`. Do not commit raw benchmark output, local logs, Terraform state, or one-off profiling artifacts.

## Pull Request Expectations

- Keep public APIs stable unless the PR explicitly changes them.
- Add tests for correctness changes.
- Update docs for user-visible behavior.
- Mention performance impact for hot-path Flow, native protocol, Raft, Bitcask, or NIF changes.
- Do not include local tooling files, cloud state, secrets, or generated build artifacts.

## Public Docs Tone

Use neutral product language. Code-shape examples are fine; avoid negative comparisons with other tools.
