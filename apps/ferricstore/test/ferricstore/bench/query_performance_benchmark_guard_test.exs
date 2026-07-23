defmodule Ferricstore.Bench.QueryPerformanceBenchmarkGuardTest do
  use ExUnit.Case, async: true

  @root Path.expand("../../../../..", __DIR__)
  @rust_manifest Path.join(
                   @root,
                   "apps/ferricstore_server/native/native_protocol_nif/Cargo.toml"
                 )
  @rust_bench Path.join(
                @root,
                "apps/ferricstore_server/native/native_protocol_nif/benches/fql_parser.rs"
              )
  @rust_alloc_bench Path.join(
                      @root,
                      "apps/ferricstore_server/native/native_protocol_nif/benches/fql_allocations.rs"
                    )
  @rust_workloads Path.join(
                    @root,
                    "apps/ferricstore_server/native/native_protocol_nif/benches/support/fql_workloads.rs"
                  )
  @support Path.join(@root, "bench/support/query_performance.exs")
  @fql_bench Path.join(@root, "bench/fql_parser_bench.exs")
  @scheduler_bench Path.join(@root, "bench/fql_scheduler_bench.exs")
  @index_bench Path.join(@root, "bench/flow_query_native_index_bench.exs")
  @lmdb_bench Path.join(@root, "bench/flow_query_lmdb_bench.exs")
  @compare Path.join(@root, "bench/query_performance_compare.exs")
  @criterion_export Path.join(@root, "bench/query_performance_criterion_export.exs")
  @linux_profile Path.join(@root, "bench/query_performance_linux_profile.sh")
  @lmdb_candidates Path.join(@root, "bench/query_planner_lmdb_candidates_bench.exs")
  @multishard_candidates Path.join(
                           @root,
                           "bench/query_planner_lmdb_multishard_candidates_bench.exs"
                         )
  @merge_candidates Path.join(@root, "bench/query_planner_merge_candidates_bench.exs")
  @catalog_candidates Path.join(
                        @root,
                        "bench/query_planner_lmdb_catalog_candidates_bench.exs"
                      )
  @core_read_candidates Path.join(
                          @root,
                          "bench/query_planner_core_read_candidates_bench.exs"
                        )
  @composite_codec_candidates Path.join(
                                @root,
                                "bench/query_planner_composite_codec_candidates_bench.exs"
                              )
  @projection_candidates Path.join(
                           @root,
                           "bench/query_planner_projection_candidates_bench.exs"
                         )
  @native_read_candidates Path.join(
                            @root,
                            "bench/query_planner_native_read_candidates_bench.exs"
                          )
  @test_workflow Path.join(@root, ".github/workflows/test.yml")
  @benchmark_workflow Path.join(@root, ".github/workflows/query-performance.yml")

  test "Rust parser benchmark covers shapes, scaling, allocations, and adversarial inputs" do
    manifest = read!(@rust_manifest)
    source = read!(@rust_bench) <> read!(@rust_alloc_bench) <> read!(@rust_workloads)

    assert manifest =~ "criterion"
    assert manifest =~ "[[bench]]"
    assert manifest =~ ~s(name = "fql_parser")
    assert manifest =~ "harness = false"

    for contract <- ~w(
          point
          collection
          count
          history
          explain
          explain_analyze
          max_valid
          max_malformed
          token_scaling
          predicate_scaling
          in_cardinality
          metadata_fields
          escaped_strings
          allocation_profile
          adversarial
        ) do
      assert source =~ contract, "missing Rust FQL benchmark contract #{inspect(contract)}"
    end

    assert source =~ "CountingAllocator"
    assert source =~ "Throughput::Bytes"
    assert source =~ "MAX_QUERY_BYTES"
    assert source =~ "MAX_TOKENS"
    assert source =~ "MAX_PREDICATES"
    assert source =~ "MAX_IN_VALUES"
    assert source =~ "assert_eq!(workloads::MAX_QUERY_BYTES, fql::MAX_QUERY_BYTES)"
    assert source =~ "assert_eq!(workloads::MAX_TOKENS, fql::MAX_TOKENS)"
    assert source =~ "assert_eq!(workloads::MAX_PREDICATES, fql::MAX_PREDICATES)"
    assert source =~ "assert_eq!(workloads::MAX_IN_VALUES, fql::MAX_IN_VALUES)"

    allocation_source = read!(@rust_alloc_bench)

    for allocation_case <- [
          "escaped_strings/512",
          "quote_storm",
          "long_identifier",
          "huge_integer",
          "max_tokens"
        ] do
      assert allocation_source =~ allocation_case,
             "missing allocation ceiling #{inspect(allocation_case)}"
    end

    assert allocation_source =~ "in_cardinality/{cardinality}"
    assert allocation_source =~ "(96, (140, 32_000))"
  end

  test "NIF benchmark separates Rust parsing, term encoding, wrapper decoding, and binding" do
    source = read!(@fql_bench) <> read!(@support)

    for contract <- [
          "NIF.parse_fql",
          "FQLParser.parse",
          "Binder.bind",
          "EXPLAIN ANALYZE",
          "max valid",
          "max malformed",
          "memory_time",
          "BENCH_PARALLEL"
        ] do
      assert source =~ contract, "missing FQL NIF benchmark contract #{inspect(contract)}"
    end

    assert source =~ "QueryPerformance.benchee_options"
    assert source =~ "preflight_inputs!"
  end

  test "scheduler benchmark measures saturation throughput and heartbeat tail latency" do
    source = read!(@scheduler_bench)

    for contract <- [
          "System.schedulers_online",
          "BENCH_CONCURRENCY",
          "NIF.parse_fql",
          "heartbeat",
          "p50",
          "p95",
          "p99",
          "max_malformed",
          "missed_heartbeats",
          "ceil_delay_ms"
        ] do
      assert source =~ contract,
             "missing scheduler responsiveness contract #{inspect(contract)}"
    end
  end

  test "native ordered-index benchmark covers scale, paging, skew, fanout, and contention" do
    source = read!(@index_bench) <> read!(@support)

    for contract <- [
          "BENCH_CARDINALITIES",
          "1_000_000",
          "page_sizes",
          "4_096",
          "forward",
          "reverse",
          "cursor",
          "deep offset",
          "duplicate scores",
          "hot partition",
          "uniform partitions",
          "claim fanout",
          "contention",
          "p95",
          "p99"
        ] do
      assert source =~ contract, "missing native-index benchmark contract #{inspect(contract)}"
    end

    assert source =~ "flow_index_claim_due_candidates"
    assert source =~ "QueryPerformance.benchee_options"
    assert source =~ "List.to_tuple(uniform_keys)"
    assert source =~ "min(delay_ms * 2, 8)"
    assert source =~ "preflight_dataset!"
    assert source =~ "setup/cardinality-"
  end

  test "LMDB benchmark covers warm, reopened, evicted, oversized, and hydrated reads" do
    source = read!(@lmdb_bench) <> read!(@support)

    for contract <- [
          "warm",
          "reopened",
          "cold cache",
          "vmtouch",
          "BENCH_REQUIRE_COLD_CACHE",
          "BENCH_LMDB_ENTRIES",
          "BENCH_LMDB_VALUE_BYTES",
          "lmdb_get_many_bounded",
          "lmdb_prefix_entries_after_bounded",
          "lmdb_range_entries_bounded",
          "logical bytes",
          "physical bytes",
          "p95",
          "p99"
        ] do
      assert source =~ contract, "missing LMDB benchmark contract #{inspect(contract)}"
    end

    assert source =~ "QueryPerformance.benchee_options"
    assert source =~ "dataset.hydration_keys"
    assert source =~ "system_page_size"
    assert source =~ "round_up(page_size)"
    assert source =~ "preflight_dataset!"
    assert source =~ "bounded hydration rejection"
    assert source =~ "oversized_hydration_keys"
    assert source =~ "setup/value-"
  end

  test "benchmark support emits comparable results and enforces a median regression budget" do
    support = read!(@support)
    compare = read!(@compare)
    criterion_export = read!(@criterion_export)

    assert support =~ "median_ns"
    assert support =~ "p95_ns"
    assert support =~ "p99_ns"
    assert support =~ "memory_median_bytes"
    assert support =~ "BENCH_SAVE"
    assert support =~ "cpu_model"
    assert support =~ "architecture"

    assert compare =~ "BENCH_REGRESSION_LIMIT"
    assert compare =~ "BENCH_ALLOW_SYSTEM_MISMATCH"
    assert compare =~ "0.15"
    assert compare =~ "median_ns"
    assert compare =~ "operation_median_ns"
    assert compare =~ "ops_per_second"
    assert compare =~ "compare_decrease"
    assert compare =~ "System.halt(1)"

    assert criterion_export =~ "estimates.json"
    assert criterion_export =~ ~s(["median"]["point_estimate"])
    assert criterion_export =~ "fql-rust-criterion.json"
  end

  test "query-planner candidate gates preserve correctness before measuring speed" do
    lmdb = read!(@lmdb_candidates)
    multishard = read!(@multishard_candidates)
    merge = read!(@merge_candidates)
    catalog = read!(@catalog_candidates)
    core = read!(@core_read_candidates)
    codec = read!(@composite_codec_candidates)
    projection = read!(@projection_candidates)
    native = read!(@native_read_candidates)

    for contract <- [
          "assert_exact_shape!",
          "current_decoded == compact_decoded",
          "invalid_compact_query_index_entry",
          "decode_discovery_entries",
          "long-id",
          "nil-state",
          "cleanup_queue_full"
        ] do
      assert lmdb =~ contract,
             "missing LMDB candidate correctness contract #{inspect(contract)}"
    end

    for contract <- [
          "preflight!",
          "sequential+sort",
          "sequential+heap",
          "parallel+sort",
          "parallel+heap"
        ] do
      assert multishard =~ contract,
             "missing multi-shard candidate contract #{inspect(contract)}"
    end

    for contract <- [
          "preflight!",
          "auto-hot repeated-sort",
          "auto-hot incremental-merge",
          "lineage sort+group",
          "lineage validate+merge"
        ] do
      assert merge =~ contract,
             "missing merge candidate contract #{inspect(contract)}"
    end

    for contract <- [
          "current pre-read+guarded-write",
          "candidate fused-guarded-write",
          "Enum.all?",
          "guarded_missing_puts",
          "guarded_equal_puts"
        ] do
      assert catalog =~ contract,
             "missing catalog candidate contract #{inspect(contract)}"
    end

    for contract <- [
          "candidate durable-count",
          "candidate LMDB selectivity-aware-continuation",
          "candidate bounded-k-way",
          "current full-digest scan",
          "candidate prepared-short scan"
        ] do
      assert core =~ contract,
             "missing core-read candidate contract #{inspect(contract)}"
    end

    for contract <- [
          "current_decoded == compact_decoded",
          "candidate compact composite encode",
          "candidate front-coded reverse encode",
          "CompactCodec.decode_reverse"
        ] do
      assert codec =~ contract,
             "missing composite-codec candidate contract #{inspect(contract)}"
    end

    for contract <- [
          "current individual reverse reads",
          "candidate bounded prefetch",
          "CompositeIndex.decode_reverse_state",
          "Enum.each([16, 64, 256]"
        ] do
      assert projection =~ contract,
             "missing projection candidate contract #{inspect(contract)}"
    end

    for contract <- [
          "LMDB.composite_range_entries_bounded",
          "LMDB.prefix_merge_entries",
          "invalid_composite_entry",
          "true = scanned == min(shards * page, shards * 5_000)",
          "paired native"
        ] do
      assert native =~ contract,
             "missing native-read candidate contract #{inspect(contract)}"
    end

    refute native =~ "lmdb_bench_"
  end

  test "Linux profiling runner records perf, flamegraph, cache, and allocator evidence" do
    source = read!(@linux_profile)

    for contract <- [
          "uname -s",
          "cargo bench",
          "cargo flamegraph",
          "perf stat",
          "perf record",
          "cache-misses",
          "branch-misses",
          "vmtouch",
          "BENCH_REQUIRE_COLD_CACHE=1",
          "CRITERION_HOME",
          "query_performance_criterion_export.exs"
        ] do
      assert source =~ contract, "missing Linux profiling contract #{inspect(contract)}"
    end
  end

  test "CI compiles allocation guards and compares scheduled results on one Linux host" do
    test_workflow = read!(@test_workflow)
    benchmark_workflow = read!(@benchmark_workflow)

    assert test_workflow =~ "cargo bench --manifest-path"
    assert test_workflow =~ "--bench fql_parser --bench fql_allocations --no-run"
    assert test_workflow =~ "BENCH_ALLOC_ITERATIONS=1000"

    assert benchmark_workflow =~ "schedule:"
    assert benchmark_workflow =~ "ubuntu-24.04"
    assert benchmark_workflow =~ "git worktree add"
    assert benchmark_workflow =~ "baseline-results"
    assert benchmark_workflow =~ "current-results"
    assert benchmark_workflow =~ "query_performance_compare.exs"
    assert benchmark_workflow =~ "CRITERION_HOME"
    assert benchmark_workflow =~ "--save-baseline baseline"
    assert benchmark_workflow =~ "--baseline baseline"
    assert benchmark_workflow =~ "query_performance_criterion_export.exs"
    assert benchmark_workflow =~ "BENCH_REGRESSION_LIMIT: \"0.15\""
    assert benchmark_workflow =~ "BENCH_REQUIRE_COLD_CACHE: \"1\""
    assert benchmark_workflow =~ "git rev-parse --verify --end-of-options"
    assert benchmark_workflow =~ "rounds must be an integer from 1 through 5"
    assert benchmark_workflow =~ "Configure benchmark result paths"
    assert benchmark_workflow =~ ~s(BASELINE_RESULTS=$RUNNER_TEMP/baseline-results)
    refute benchmark_workflow =~ ~s(${{ runner.temp }})
  end

  defp read!(path) do
    assert File.regular?(path), "required benchmark file is missing: #{path}"
    File.read!(path)
  end
end
