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
    assert runtime_exs =~ "flow_async_history: true"
    refute config_exs =~ "raft_backend"
    refute config_exs =~ "waraft_async_log_append"
    refute bench_exs =~ "raft_backend"
    refute bench_exs =~ "waraft_async_log_append"
    refute runtime_exs =~ "raft_backend"
    refute runtime_exs =~ "FERRICSTORE_RAFT_BACKEND"
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

  test "WARaft is the only runtime backend mode" do
    old_backend = Application.get_env(:ferricstore, :raft_backend)

    on_exit(fn ->
      case old_backend do
        nil -> Application.delete_env(:ferricstore, :raft_backend)
        value -> Application.put_env(:ferricstore, :raft_backend, value)
      end
    end)

    Application.put_env(:ferricstore, :raft_backend, :ra)
    assert Ferricstore.Raft.Backend.selected() == :waraft
    assert Ferricstore.Raft.Backend.waraft?()

    backend_source =
      File.read!(Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/raft/backend.ex"))

    waraft_source =
      File.read!(Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/raft/waraft_backend.ex"))

    refute backend_source =~ "Application.get_env(:ferricstore, :raft_backend"
    refute backend_source =~ "normalize_selected"
    refute waraft_source =~ ":waraft_async_log_append"
  end

  test "WARaft storage has no selectable apply mode branch" do
    source =
      File.read!(Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/raft/waraft_storage.ex"))

    # The segment/keydir apply path is now the storage contract. Keeping a
    # boolean helper here would invite benchmarks and deploys to drift again.
    refute source =~ "segment_keydir_apply?"
  end

  test "benchmark scripts do not expose old WARaft mode selectors" do
    bench_sources =
      Path.wildcard(Path.join(@repo_root, "bench/**/*.exs"))
      |> Enum.map(&{&1, File.read!(&1)})

    forbidden = [
      "FERRICSTORE_RAFT_BACKEND",
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
end
