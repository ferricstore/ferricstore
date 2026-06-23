# Contributing

Thanks for helping improve FerricStore. This repo is focused on FerricStore core, FerricFlow, and the native TCP server.

## Development Setup

Required tools:

- Elixir >= 1.19
- Erlang/OTP >= 28
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

Run the server locally:

```bash
MIX_ENV=prod FERRICSTORE_DATA_DIR=/tmp/ferricstore mix run --no-halt
```

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
