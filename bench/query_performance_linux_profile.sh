#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "query performance profiling requires Linux" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${BENCH_OUTPUT_DIR:-$ROOT/bench/output/query-performance}"
MANIFEST="$ROOT/apps/ferricstore_server/native/native_protocol_nif/Cargo.toml"
PROFILE_FILTER="${BENCH_PROFILE_FILTER:-fql_parser/shapes/collection}"
TAG="${BENCH_TAG:-current}"
mkdir -p "$OUT/results"
cd "$ROOT"

export MIX_ENV=bench
export FERRICSTORE_BUILD_NIF=1
export BENCH_SAVE="$OUT/results"
export BENCH_REQUIRE_COLD_CACHE=1
export CRITERION_HOME="${CRITERION_HOME:-$OUT/criterion}"

command -v vmtouch >/dev/null || {
  echo "vmtouch is required for cold-cache LMDB measurements" >&2
  exit 1
}

cargo bench --manifest-path "$MANIFEST" --bench fql_allocations
cargo bench --manifest-path "$MANIFEST" --bench fql_parser -- --save-baseline "$TAG"

if command -v perf >/dev/null; then
  perf stat \
    -e cycles,instructions,cache-references,cache-misses,branches,branch-misses \
    -o "$OUT/perf-stat.txt" \
    cargo bench --manifest-path "$MANIFEST" --bench fql_parser -- \
      "$PROFILE_FILTER" --warm-up-time 1 --measurement-time 3

  perf record -g -o "$OUT/perf.data" -- \
    cargo bench --manifest-path "$MANIFEST" --bench fql_parser -- \
      "$PROFILE_FILTER" --warm-up-time 1 --measurement-time 3
elif [[ "${BENCH_REQUIRE_PROFILERS:-0}" == "1" ]]; then
  echo "perf is required when BENCH_REQUIRE_PROFILERS=1" >&2
  exit 1
fi

if command -v cargo-flamegraph >/dev/null || cargo flamegraph --help >/dev/null 2>&1; then
  cargo flamegraph --manifest-path "$MANIFEST" --bench fql_parser \
    --output "$OUT/fql-parser-flamegraph.svg" -- \
    "$PROFILE_FILTER" --warm-up-time 1 --measurement-time 3
elif [[ "${BENCH_REQUIRE_PROFILERS:-0}" == "1" ]]; then
  echo "cargo flamegraph is required when BENCH_REQUIRE_PROFILERS=1" >&2
  exit 1
fi

mix run --no-start bench/fql_parser_bench.exs
mix run --no-start bench/fql_scheduler_bench.exs
mix run --no-start bench/flow_query_native_index_bench.exs
mix run --no-start bench/flow_query_lmdb_bench.exs
mix run --no-start bench/query_performance_criterion_export.exs "$CRITERION_HOME" "$TAG" "$OUT/results"

if [[ -n "${BENCH_BASELINE_DIR:-}" ]]; then
  mix run --no-start bench/query_performance_compare.exs "$BENCH_BASELINE_DIR" "$OUT/results"
fi

echo "query performance artifacts: $OUT"
