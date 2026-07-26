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
  @result_projection_bench Path.join(@root, "bench/query_result_projection_bench.exs")
  @native_read_candidates Path.join(
                            @root,
                            "bench/query_planner_native_read_candidates_bench.exs"
                          )
  @covering_read_candidates Path.join(
                              @root,
                              "bench/query_planner_covering_index_candidates_bench.exs"
                            )
  @covering_write_cost Path.join(@root, "bench/query_covering_write_cost_bench.exs")
  @storage_layout Path.join(@root, "bench/query_storage_layout_bench.exs")
  @storage_fixture Path.join(@root, "bench/support/query_storage_fixture.exs")
  @soak_fixture Path.join(@root, "bench/support/query_soak_fixture.exs")
  @index_benchmark Path.join(@root, "bench/flow_query_index_bench.exs")
  @query_soak Path.join(@root, "bench/flow_query_soak.exs")
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
    assert source =~ "FQLPlannerContext.start!"
    assert read!(@fql_bench) =~ "RETURN RECORDS (run_id, state, attribute['customer'])"
    assert read!(@fql_bench) =~ "{:ok, _, _, _, _, _, _, _, _}"
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

    assert source =~ "RETURN RECORDS (run_id, state, attribute['customer'])"
    assert read!(@scheduler_bench) =~ "{:ok, _, _, _, _, _, _, _, _}"
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

  test "benchmark comparison does not gate an unreproduced paired-round slowdown" do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_paired_benchmark_#{System.unique_integer([:positive, :monotonic])}"
      )

    baseline = Path.join(root, "baseline")
    current = Path.join(root, "current")
    on_exit(fn -> File.rm_rf!(root) end)

    # Four slow pairs out of five are not enough to reject runner noise with a
    # one-sided sign test at p < 0.05. A release gate must be reproducible.
    write_benchmark_rounds!(baseline, [1_000, 1_000, 1_000, 1_000, 1_000])
    write_benchmark_rounds!(current, [1_300, 1_300, 1_300, 1_300, 1_000])

    {output, status} =
      System.cmd(
        "mix",
        ["run", "--no-start", @compare, baseline, current],
        cd: @root,
        env: [{"BENCH_REGRESSION_LIMIT", "0.15"}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "paired-round"
  end

  test "benchmark comparison rejects a regression reproduced across five paired rounds" do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_regressed_benchmark_#{System.unique_integer([:positive, :monotonic])}"
      )

    baseline = Path.join(root, "baseline")
    current = Path.join(root, "current")
    on_exit(fn -> File.rm_rf!(root) end)

    write_benchmark_rounds!(baseline, [1_000, 1_000, 1_000, 1_000, 1_000])
    write_benchmark_rounds!(current, [1_300, 1_300, 1_300, 1_300, 1_300])

    {output, status} =
      System.cmd(
        "mix",
        ["run", "--no-start", @compare, baseline, current],
        cd: @root,
        env: [{"BENCH_REGRESSION_LIMIT", "0.15"}],
        stderr_to_stdout: true
      )

    assert status == 1
    assert output =~ "median_ns increased 30.0%"
    assert output =~ "paired-round ratio=1.3 across 5 pairs"
  end

  test "benchmark comparison rejects statistically underpowered paired evidence" do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_underpowered_benchmark_#{System.unique_integer([:positive, :monotonic])}"
      )

    baseline = Path.join(root, "baseline")
    current = Path.join(root, "current")
    on_exit(fn -> File.rm_rf!(root) end)

    write_benchmark_rounds!(baseline, [1_000, 1_000, 1_000])
    write_benchmark_rounds!(current, [1_300, 1_300, 1_300])

    {output, status} =
      System.cmd(
        "mix",
        ["run", "--no-start", @compare, baseline, current],
        cd: @root,
        stderr_to_stdout: true
      )

    assert status == 1
    assert output =~ "requires at least 5 paired rounds"
  end

  test "benchmark comparison rejects incomplete field-level round pairing" do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_incomplete_benchmark_#{System.unique_integer([:positive, :monotonic])}"
      )

    baseline = Path.join(root, "baseline")
    current = Path.join(root, "current")
    on_exit(fn -> File.rm_rf!(root) end)

    complete = %{"median_ns" => 1_000, "memory_median_bytes" => 256}
    write_benchmark_metric_rounds!(baseline, List.duplicate(complete, 5))

    write_benchmark_metric_rounds!(current, [
      complete,
      %{"median_ns" => 1_000},
      complete,
      complete,
      complete
    ])

    {output, status} =
      System.cmd(
        "mix",
        ["run", "--no-start", @compare, baseline, current],
        cd: @root,
        stderr_to_stdout: true
      )

    assert status == 1
    assert output =~ "memory_median_bytes rounds differ"
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
          "paired native",
          "def smoke"
        ] do
      assert native =~ contract,
             "missing native-read candidate contract #{inspect(contract)}"
    end

    refute native =~ "lmdb_bench_"
  end

  test "result projection benchmark compares the safe fixed-index candidate" do
    source = read!(@result_projection_bench)

    for contract <- [
          "^expected = candidate(records)",
          "current full allowlist then sparse projection",
          "candidate validation summary plus direct sparse projection",
          "RecordProjection.project_result",
          "RecordProjection.project_validated",
          "internal_payload_ref"
        ] do
      assert source =~ contract,
             "missing result projection benchmark contract #{inspect(contract)}"
    end
  end

  test "covering write benchmark includes committed steady-state and migration workloads" do
    source = read!(@covering_write_cost)

    for contract <- [
          "CompositeProjection.reconcile",
          "LMDB.write_batch",
          "plain v1 reconcile + LMDB commit",
          "covering v2 reconcile + LMDB commit",
          "upgrade dual-generation reconcile + LMDB commit"
        ] do
      assert source =~ contract,
             "missing covering-write benchmark contract #{inspect(contract)}"
    end
  end

  test "covering read benchmark compares production QueryRow hydration with zero-log reads" do
    source = read!(@covering_read_candidates)

    for contract <- [
          "QueryStorageFixture.write!",
          "BENCH_COVERING_RECORDS_PER_LOG_ENTRY",
          "page_records: min(records_per_log_entry, count)",
          "QueryRecordStore.read_many(",
          "production index scan plus authoritative hydration",
          "production covered projection without log reads"
        ] do
      assert source =~ contract,
             "missing covering-read benchmark contract #{inspect(contract)}"
    end

    refute source =~ "LMDB.encode_value("
    refute source =~ "LMDB.decode_value("
    refute source =~ "Codec.decode_records("
    refute source =~ "legacy"
  end

  test "storage layout benchmark proves equivalence and reports duplication savings" do
    source = read!(@storage_layout)

    for contract <- [
          "QueryStorageFixture.write!",
          "BENCH_STORAGE_RECORDS_PER_LOG_ENTRY",
          "QueryRecordStore.read_many(",
          "previous duplicated LMDB full-record read",
          "production QueryRow + authoritative-log read",
          "lmdb_logical_savings_percent",
          "total_write_savings_percent",
          "^previous = production",
          ~s|:binary.copy("a", 64)|,
          ~s|:binary.copy("m", 64)|
        ] do
      assert source =~ contract,
             "missing storage-layout benchmark contract #{inspect(contract)}"
    end

    refute source =~ ":binary.copy(<<"
  end

  test "storage layout benchmark can isolate compact-row lookup and authoritative hydration" do
    source = read!(@storage_layout)

    for contract <- [
          "BENCH_STORAGE_DIAGNOSTICS",
          "BENCH_STORAGE_BATCH_READ_ONLY",
          "BENCH_STORAGE_HYDRATION_ONLY",
          "BENCH_STORAGE_CANDIDATE_READER_LANES",
          "BENCH_STORAGE_COLD_CACHE",
          "QueryRowStore.read_references_many(",
          "RecordHydrator.read_many(",
          "diagnostic compact QueryRow reference read",
          "diagnostic authoritative-log hydration",
          "candidate inline-record hydration",
          "diagnostic authoritative raw batch read",
          "production authoritative vector batch read",
          "production physical locator batch read",
          "candidate registry + retained-fd vector read",
          "candidate retained-fd authoritative batch read",
          "candidate coalesced-adjacent authoritative batch read",
          "diagnostic locator SHA-256 validation",
          "diagnostic authoritative record batch decode",
          "WARaftSegmentReader.read_values_from_location(",
          "WARaftSegmentReader.read_values_from_locations(",
          "WARaftSegmentReader.read_physical_values(",
          "RecordIdentity.owns_state_key?",
          "diagnostic segment location lookup",
          "diagnostic segment read at known location",
          "candidate verified single-pread segment read",
          "candidate retained-fd single-pread segment read",
          ":ferricstore_waraft_spike_segment_log.location_for_index(",
          ":ferricstore_waraft_spike_segment_log.read_disk_at(",
          ":erlang.crc32(payload)",
          ":erlang.binary_to_term(payload, [:safe, :used])",
          "DiskReader.invalidate",
          ~s|System.cmd("vmtouch", ["-e", path]|,
          ~s|write_manual_metrics("query-storage-layout-cold"|,
          "Task.async(fn -> candidate_retained_segment_read(dataset) end)",
          "retained_segment_reader_loop",
          ":file.pread(fd, locations)",
          ":file.pread(fd, offset, bytes)",
          ":ferricstore_waraft_segment_offset_registry",
          "diagnostic_requests"
        ] do
      assert source =~ contract,
             "missing storage-layout diagnostic contract #{inspect(contract)}"
    end
  end

  test "end-to-end query benchmarks use authoritative log records and compact QueryRows" do
    fixture = read!(@storage_fixture)
    consumers = read!(@soak_fixture) <> read!(@index_benchmark) <> read!(@query_soak)

    for contract <- [
          "WARaftSegmentReader.put_apply_projection",
          "ensure_apply_projection_entries_durable",
          "WARaftSegmentReader.physical_location",
          "RuntimeSupervisor.ensure_started()",
          "ProjectionLocator.decode_source",
          "QueryRowCodec.encode",
          "SourceCatalog.put_op"
        ] do
      assert fixture =~ contract,
             "missing production storage benchmark contract #{inspect(contract)}"
    end

    assert consumers =~ "QueryStorageFixture.write!"
    refute consumers =~ "LMDB.encode_value(Codec.encode_record(record), 0)"

    soak = read!(@query_soak)
    index = read!(@index_benchmark)

    assert soak =~ "QueryRecordStore.read_many("
    assert soak =~ "QueryRowStore.read_references_many("
    refute soak =~ "LMDB.decode_value("
    refute soak =~ "Codec.decode_records("

    assert index =~ "storage.encoded_by_key"
    refute index =~ "Codec.encode_record(record)"
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
    assert test_workflow =~ "MIX_ENV=bench mix deps.get"
    assert test_workflow =~ "BENCH_CANDIDATE_SECTION=smoke"
    assert test_workflow =~ "bench/query_planner_native_read_candidates_bench.exs"

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
    assert benchmark_workflow =~ ~s(default: "5")
    assert benchmark_workflow =~ ~s(REQUESTED_ROUNDS:-5)
    assert benchmark_workflow =~ "git rev-parse --verify --end-of-options"
    assert benchmark_workflow =~ "rounds must be 5 so the paired sign test has p < 0.05"
    assert benchmark_workflow =~ "Configure benchmark result paths"
    assert benchmark_workflow =~ ~s(BENCH_HARNESS: ${{ github.workspace }})
    assert benchmark_workflow =~ ~s("$BENCH_HARNESS/bench/fql_parser_bench.exs")
    assert benchmark_workflow =~ ~s(BASELINE_RESULTS=$RUNNER_TEMP/baseline-results)
    assert benchmark_workflow =~ "Run authoritative query storage acceptance"
    assert benchmark_workflow =~ ~s(BENCH_STORAGE_RECORDS_PER_LOG_ENTRY: "1")
    assert benchmark_workflow =~ ~s(BENCH_STORAGE_COLD_CACHE: "1")
    assert benchmark_workflow =~ ~s(BENCH_PARALLEL: "16")
    assert benchmark_workflow =~ "bench/query_storage_layout_bench.exs"
    refute benchmark_workflow =~ ~s(${{ runner.temp }})
  end

  defp read!(path) do
    assert File.regular?(path), "required benchmark file is missing: #{path}"
    File.read!(path)
  end

  defp write_benchmark_rounds!(root, medians) do
    metrics = Enum.map(medians, &%{"median_ns" => &1})
    write_benchmark_metric_rounds!(root, metrics)
  end

  defp write_benchmark_metric_rounds!(root, metrics) do
    metrics
    |> Enum.with_index(1)
    |> Enum.each(fn {metric, round} ->
      path = Path.join([root, "round-#{round}", "paired.json"])
      File.mkdir_p!(Path.dirname(path))

      File.write!(
        path,
        Jason.encode!(%{
          "version" => 1,
          "suite" => "paired",
          "system" => %{
            "os" => "unix/linux",
            "architecture" => "x86_64",
            "cpu_model" => "test",
            "otp" => "28",
            "elixir" => "1.19",
            "schedulers_online" => 4
          },
          "scenarios" => %{
            "scenario" => metric
          }
        })
      )
    end)
  end
end
