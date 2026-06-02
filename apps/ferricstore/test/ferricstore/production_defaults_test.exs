defmodule Ferricstore.ProductionDefaultsTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../..", __DIR__)

  test "production and benchmark config default to the production WARaft path" do
    config_exs = File.read!(Path.join(@repo_root, "config/config.exs"))
    bench_exs = File.read!(Path.join(@repo_root, "config/bench.exs"))
    runtime_exs = File.read!(Path.join(@repo_root, "config/runtime.exs"))

    # These defaults are load-bearing for normal deploys and benchmarks: the
    # production path should not require per-run WARAFT_* or FLOW_* env flags.
    assert config_exs =~ "flow_async_history: true"
    assert config_exs =~ "wal_commit_delay_us: 6_000"
    assert config_exs =~ "waraft_commit_batch_max: 10_000"
    assert bench_exs =~ "wal_commit_delay_us: 6_000"
    assert bench_exs =~ "waraft_commit_batch_max: 10_000"
    assert config_exs =~ "flow_retention_sweeper_initial_delay_ms: 600_000"
    assert config_exs =~ "flow_retention_sweeper_interval_ms: 600_000"
    assert config_exs =~ "flow_retention_sweeper_pressure_interval_ms: 1_000"
    assert config_exs =~ "flow_retention_sweeper_pressure_limit: 10_000"
    assert config_exs =~ "flow_retention_sweeper_pressure_compaction_interval_ms: 60_000"
    assert bench_exs =~ "flow_retention_sweeper_initial_delay_ms: 600_000"
    assert bench_exs =~ "flow_retention_sweeper_interval_ms: 600_000"
    assert bench_exs =~ "flow_retention_sweeper_pressure_interval_ms: 1_000"
    assert bench_exs =~ "flow_retention_sweeper_pressure_limit: 10_000"
    assert bench_exs =~ "flow_retention_sweeper_pressure_compaction_interval_ms: 60_000"
    assert runtime_exs =~ "flow_async_history: true"
    assert runtime_exs =~ "\"6000\""
    assert runtime_exs =~ "\"10000\""
    backend_mode_key = "raft_" <> "backend"
    backend_mode_env = "FERRICSTORE_RAFT_" <> "BACKEND"

    refute config_exs =~ backend_mode_key
    refute config_exs =~ "waraft_async_log_append"
    refute bench_exs =~ backend_mode_key
    refute bench_exs =~ "waraft_async_log_append"
    refute runtime_exs =~ backend_mode_key
    refute runtime_exs =~ backend_mode_env
    refute runtime_exs =~ "waraft_async_log_append"
    refute runtime_exs =~ "FERRICSTORE_WARAFT_ASYNC_LOG_APPEND"

    for source <- [config_exs, bench_exs, runtime_exs] do
      refute source =~ "waraft_segment_log_io_mode"
      refute source =~ "waraft_segment_log_sync_method"
      refute source =~ "waraft_segment_log_file_writer_mode"
      refute source =~ "WARAFT_SEGMENT_IO_MODE"
      refute source =~ "WARAFT_SEGMENT_SYNC_METHOD"
      refute source =~ "WARAFT_FILE_WRITER_MODE"
    end

    refute config_exs =~ "flow_lmdb_mode"
    refute config_exs =~ "flow_lmdb_enabled"
    refute bench_exs =~ "flow_lmdb_mode"
    refute bench_exs =~ "flow_lmdb_enabled"
    refute runtime_exs =~ "flow_lmdb_mode"
    refute runtime_exs =~ "flow_lmdb_enabled"
    refute runtime_exs =~ "FERRICSTORE_FLOW_LMDB_MODE"
    refute runtime_exs =~ "FERRICSTORE_FLOW_ASYNC_HISTORY"
    refute runtime_exs =~ "write_through"
  end

  test "native NIFs build from source by default in repo checkouts" do
    config_exs = File.read!(Path.join(@repo_root, "config/config.exs"))
    bench_exs = File.read!(Path.join(@repo_root, "config/bench.exs"))

    assert config_exs =~ "config :rustler_precompiled, :force_build"
    assert config_exs =~ "ferricstore: true"
    assert config_exs =~ "skip_compilation?: false"
    assert bench_exs =~ "skip_compilation?: false"

    # Local development, tests, Docker builds, and benchmarks should not depend
    # on remembering a per-command build flag. Packaged dependency users still
    # get RustlerPrecompiled artifact loading because dependency config files are
    # not imported into the parent application.
    refute config_exs =~ "FERRICSTORE_BUILD"
    refute bench_exs =~ "FERRICSTORE_BUILD"

    nif_paths = [
      "apps/ferricstore/lib/ferricstore/bitcask/nif.ex",
      "apps/ferricstore/lib/ferricstore/wal_nif.ex",
      "apps/ferricstore/lib/ferricstore/resp/parser_nif.ex"
    ]

    for path <- nif_paths do
      source = File.read!(Path.join(@repo_root, path))

      refute source =~ "FERRICSTORE_BUILD", "#{path} still requires FERRICSTORE_BUILD"
      refute source =~ "force_build: System.get_env", "#{path} still owns build-mode env logic"
      assert source =~ "use RustlerPrecompiled"
    end
  end

  test "current docs, Docker, deploy, and bench examples do not require native build flag" do
    deploy_paths =
      Path.wildcard(Path.join(@repo_root, "deploy/**/*"))
      |> Enum.filter(&File.regular?/1)

    bench_paths = Path.wildcard(Path.join(@repo_root, "bench/**/*.exs"))
    doc_paths = Path.wildcard(Path.join(@repo_root, "docs/**/*.md"))

    paths =
      ["Dockerfile", "RELEASING.md"]
      |> Enum.map(&Path.join(@repo_root, &1))
      |> Kernel.++(deploy_paths)
      |> Kernel.++(bench_paths)
      |> Kernel.++(doc_paths)
      |> Enum.map(&Path.relative_to(&1, @repo_root))

    for path <- paths do
      source = File.read!(Path.join(@repo_root, path))
      refute source =~ "FERRICSTORE_BUILD", "#{path} still teaches the old native build flag"
    end
  end

  test "Raft hot-path optimizations are not exposed as runtime mode flags" do
    runtime_exs = File.read!(Path.join(@repo_root, "config/runtime.exs"))
    configuration_guide = File.read!(Path.join(@repo_root, "guides/configuration.md"))

    for token <- [
          "FERRICSTORE_RAFT_PIPELINE_PRIORITY",
          "FERRICSTORE_RAFT_DIRECT_BATCH_COMMANDS",
          "FERRICSTORE_RAFT_COMPACT_HOT_BATCHES",
          "FERRICSTORE_RAFT_PUT_BATCH_APPLY_FAST_PATH",
          "FERRICSTORE_RAFT_DELETE_BATCH_APPLY_FAST_PATH",
          "raft_pipeline_priority",
          "raft_direct_batch_commands",
          "raft_compact_hot_batches",
          "raft_put_batch_apply_fast_path",
          "raft_delete_batch_apply_fast_path"
        ] do
      refute runtime_exs =~ token
      refute configuration_guide =~ token
    end

    for path <- [
          "apps/ferricstore/lib/ferricstore/store/router.ex",
          "apps/ferricstore/lib/ferricstore/raft/state_machine.ex"
        ] do
      source = File.read!(Path.join(@repo_root, path))
      refute source =~ "PerfToggles", "#{path} still branches on performance toggles"
    end
  end

  test "WARaft is the only runtime backend mode" do
    assert Ferricstore.Raft.Backend.selected() == :waraft
    assert Ferricstore.Raft.Backend.waraft?()

    backend_source =
      File.read!(Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/raft/backend.ex"))

    waraft_source =
      File.read!(Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/raft/waraft_backend.ex"))

    refute backend_source =~ "Application.get_env(:ferricstore, :raft_" <> "backend"
    refute backend_source =~ "normalize_selected"
    refute waraft_source =~ ":waraft_async_log_append"
  end

  test "production code has no Erlang ra dependency path" do
    root = @repo_root
    mix_source = File.read!(Path.join(root, "apps/ferricstore/mix.exs"))
    lock_source = File.read!(Path.join(root, "mix.lock"))
    config_source = File.read!(Path.join(root, "config/config.exs"))

    refute mix_source =~ "{:" <> "ra,"
    refute mix_source =~ ":" <> "patched" <> "_wal"
    refute lock_source =~ "\"r" <> "a\""
    refute config_source =~ "config :" <> "ra"

    forbidden = [
      ":" <> "ra.",
      ":" <> "ra_system",
      ":" <> "ra_log" <> "_wal",
      ":" <> "ra_counters",
      "@behaviour :" <> "ra" <> "_machine"
    ]

    production_sources =
      Path.wildcard(Path.join(root, "apps/ferricstore/lib/**/*.{ex,exs}")) ++
        Path.wildcard(Path.join(root, "apps/ferricstore_server/lib/**/*.{ex,exs}"))

    for path <- production_sources,
        token <- forbidden,
        source = File.read!(path) do
      refute source =~ token, "#{Path.relative_to(path, root)} still mentions #{token}"
    end
  end

  test "WARaft storage has no selectable apply mode branch" do
    source =
      File.read!(Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/raft/waraft_storage.ex"))

    # The segment/keydir apply path is now the storage contract. Keeping a
    # boolean helper here would invite benchmarks and deploys to drift again.
    refute source =~ "segment_keydir_apply?"
  end

  test "Flow create_many uses the optimized create planner before generic fallback" do
    source =
      File.read!(Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/raft/state_machine.ex"))

    [_prefix, create_many_clause] =
      String.split(
        source,
        "defp do_flow_create_many(state, %{records: [_ | _] = attrs_list} = attrs) do",
        parts: 2
      )

    [create_many_body, _suffix] =
      String.split(create_many_clause, "defp do_flow_create_many(_state, _attrs)", parts: 2)

    assert create_many_body =~
             "case flow_create_pipeline_batch_fast_prepare(state, attrs_list, stamped_shard)"

    assert create_many_body =~ "flow_create_many_fast_apply(state, plans)"
  end

  test "benchmark scripts do not expose old WARaft mode selectors" do
    bench_sources =
      Path.wildcard(Path.join(@repo_root, "bench/**/*.exs"))
      |> Enum.map(&{&1, File.read!(&1)})

    forbidden = [
      "FERRICSTORE_RAFT_" <> "BACKEND",
      "FERRICSTORE_WARAFT_ASYNC_LOG_APPEND",
      "WARAFT_ASYNC_LOG_APPEND",
      "WARAFT_SEGMENT_IO_MODE",
      "WARAFT_SEGMENT_SYNC_METHOD",
      "WARAFT_FILE_WRITER_MODE",
      "WARAFT_SEGMENT_SYNC_DELAY_US",
      "WARAFT_FILE_WRITER_GROUP_DELAY_MS",
      "waraft_async_log_append",
      "waraft_segment_log_io_mode",
      "waraft_segment_log_sync_method",
      "waraft_segment_log_file_writer_mode",
      "waraft_segment_log_sync_delay_us",
      "waraft_segment_log_file_writer_group_delay_ms"
    ]

    for {path, source} <- bench_sources, token <- forbidden do
      refute source =~ token, "#{Path.relative_to(path, @repo_root)} still mentions #{token}"
    end
  end

  test "runtime config and guides do not expose removed Ra batcher flags" do
    paths = [
      "config/runtime.exs",
      "guides/configuration.md"
    ]

    for path <- paths do
      source = File.read!(Path.join(@repo_root, path))
      refute source =~ "FERRICSTORE_RAFT_BATCHER", "#{path} still exposes removed Ra flags"
      refute source =~ "raft_batcher_max_", "#{path} still configures unused Ra batcher keys"
    end
  end

  test "DBOS-style benchmark telemetry profiler is opt-in" do
    source = File.read!(Path.join(@repo_root, "bench/flow_python_backend_profile.exs"))

    assert source =~ "PROFILE_TELEMETRY"
    assert source =~ "if telemetry_profile?, do: print_profile(table)"
    assert source =~ "if telemetry_profile?, do: :telemetry.detach(handler_id)"
    refute source =~ "attach!(handler_id, table)\n\n    if internal_waraft_profile?()"
  end

  test "DBOS-style benchmark has one guarded production profile" do
    source = File.read!(Path.join(@repo_root, "bench/flow_python_backend_profile.exs"))

    assert source =~ "@dbos_profile_defaults"

    for expected <- [
          ~s(queued_shape: "live"),
          ~s(transport: "many"),
          ~s(worker_api: "lowlevel"),
          ~s(worker_mode: "blocking"),
          ~s(flows: "1000000"),
          ~s(workers: "16"),
          ~s(producers: "8"),
          ~s(partitions: "1024"),
          ~s(claim_batch_size: "1000"),
          ~s(claim_partition_batch_size: "16"),
          ~s(claim_block_ms: "5000"),
          ~s(claim_drain_block_ms: "50"),
          ~s(claim_drain_batches: "1"),
          ~s(create_batch_size: "1000"),
          ~s(complete_async_depth: "4"),
          ~s(shards: "16")
        ] do
      assert source =~ expected
    end

    assert source =~ ~S|env_default("PRODUCERS", :producers)|
    assert source =~ ~S|env_default("CLAIM_PARTITION_BATCH_SIZE", :claim_partition_batch_size)|
    assert source =~ ~S|env_default("CLAIM_DRAIN_BATCHES", :claim_drain_batches)|
    assert source =~ ~S|env_default("WAKE_COALESCE_MS", :wake_coalesce_ms)|
    refute source =~ ~S|env("PRODUCERS", "16")|
  end

  test "Flow soak benchmark uses its memory guard budget without a required env flag" do
    source = File.read!(Path.join(@repo_root, "bench/flow_state_lmdb_soak.exs"))

    assert source =~ "app_memory_budget_bytes(max_total_mem_mb)"
    assert source =~ "FERRICSTORE_MAX_MEMORY"
    refute source =~ ~S|int_env("FERRICSTORE_MAX_MEMORY", 0)|
  end

  test "Flow soak benchmark defaults to product worker claim shape" do
    source = File.read!(Path.join(@repo_root, "bench/flow_state_lmdb_soak.exs"))

    # The production SDK worker path uses server-side BLOCK claims. A clean soak
    # run should exercise that same path instead of requiring NORMAL_* flags to
    # avoid the older cursor/polling workload shape.
    assert source =~ ~S|defp normal_claim_states_mode, do: env("NORMAL_CLAIM_STATES_MODE", "any")|
    assert source =~ ~S|defp long_claim_states_mode, do: env("LONG_CLAIM_STATES_MODE", "any")|

    assert source =~
             ~S|defp normal_worker_mode, do: env("NORMAL_WORKER_MODE", env("WORKER_MODE", "blocking"))|

    assert source =~
             ~S|defp long_worker_mode, do: env("LONG_WORKER_MODE", env("WORKER_MODE", "blocking"))|
  end

  test "segment log production code has no alternate I/O mode selectors" do
    source =
      File.read!(
        Path.join(@repo_root, "apps/ferricstore/src/ferricstore_waraft_spike_segment_log.erl")
      )

    for token <- [
          "waraft_segment_log_io_mode",
          "waraft_segment_log_sync_method",
          "waraft_segment_log_file_writer_mode",
          "waraft_segment_log_sync_delay_us",
          "waraft_segment_log_file_writer_group_delay_ms"
        ] do
      refute source =~ token
    end
  end

  test "runtime modules use app config rather than direct Flow history env flags" do
    paths = [
      "apps/ferricstore/lib/ferricstore/raft/state_machine.ex",
      "apps/ferricstore/lib/ferricstore/store/shard.ex",
      "apps/ferricstore/lib/ferricstore/flow/lmdb_rebuilder.ex"
    ]

    for path <- paths do
      source = File.read!(Path.join(@repo_root, path))

      assert source =~ ":flow_async_history"
      refute source =~ "System.get_env(\"FLOW_ASYNC_HISTORY\""
    end
  end

  test "Flow LMDB exposes one production mode only" do
    lmdb_source =
      File.read!(Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/flow/lmdb.ex"))

    assert Ferricstore.Flow.LMDB.enabled?()
    assert Ferricstore.Flow.LMDB.mode() == :lagged
    assert Ferricstore.Flow.LMDB.mirror?()
    refute lmdb_source =~ "normalize_mode"
    refute lmdb_source =~ "Application.get_env(:flow_lmdb_mode"
  end

  test "Flow LMDB background flushes stay serial by default" do
    config_exs = File.read!(Path.join(@repo_root, "config/config.exs"))
    bench_exs = File.read!(Path.join(@repo_root, "config/bench.exs"))
    runtime_exs = File.read!(Path.join(@repo_root, "config/runtime.exs"))

    assert Ferricstore.Flow.LMDBFlushCoordinator.default_max_concurrent() == 1
    assert runtime_exs =~ ~s(FERRICSTORE_FLOW_LMDB_MAX_CONCURRENT_FLUSHES", "1")
    refute config_exs =~ "flow_lmdb_max_concurrent_flushes"
    refute bench_exs =~ "flow_lmdb_max_concurrent_flushes"
  end

  test "Flow history projector production defaults avoid tiny background fsync batches" do
    source =
      File.read!(
        Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/flow/history_projector.ex")
      )

    # The history projector is cold/lagged, but its fsyncs share the same disk as
    # WARaft commits. Small defaults create p99 write stalls under Flow soak.
    assert source =~ "@default_batch_size 25_000"
    assert source =~ "@default_flush_interval_ms 1_000"
  end

  test "Flow soak benchmark reports hidden WARaft batching and segment flags" do
    source = File.read!(Path.join(@repo_root, "bench/flow_state_lmdb_soak.exs"))

    assert source =~ "WARAFT_HOT_BATCH_WINDOW_MS"
    assert source =~ "FERRICSTORE_WARAFT_HOT_BATCH_WINDOW_MS"
    assert source =~ "WARAFT_GENERIC_BATCH_WINDOW_MS"
    assert source =~ "FERRICSTORE_WARAFT_GENERIC_BATCH_WINDOW_MS"
    assert source =~ "WARAFT_GENERIC_BATCH_DURING_FLUSH"
    assert source =~ "FERRICSTORE_WARAFT_GENERIC_BATCH_DURING_FLUSH"
    assert source =~ "FERRICSTORE_WARAFT_APPLY_LOG_BATCH_SIZE"
    assert source =~ "waraft_generic_batch_during_flush="
    assert source =~ "waraft_segment_records_per_segment="
    assert source =~ "waraft_segment_preallocate_bytes="
    assert source =~ "flow_history_projector_batch_size="
  end

  test "WARaft apply-log batch default is tuned for Flow throughput" do
    config_exs = File.read!(Path.join(@repo_root, "config/config.exs"))
    bench_exs = File.read!(Path.join(@repo_root, "config/bench.exs"))
    runtime_exs = File.read!(Path.join(@repo_root, "config/runtime.exs"))

    backend_source =
      File.read!(Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/raft/waraft_backend.ex"))

    assert config_exs =~ "waraft_apply_log_batch_size: 4_096"
    assert bench_exs =~ "waraft_apply_log_batch_size: 4_096"
    assert runtime_exs =~ ~s(FERRICSTORE_WARAFT_APPLY_LOG_BATCH_SIZE", "4096")
    assert backend_source =~ ":waraft_apply_log_batch_size, 4096"
  end
end
