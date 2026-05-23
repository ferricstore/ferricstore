defmodule Ferricstore.ProductionDefaultsTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../..", __DIR__)

  test "production and benchmark config default to the production WARaft path" do
    config_exs = File.read!(Path.join(@repo_root, "config/config.exs"))
    bench_exs = File.read!(Path.join(@repo_root, "config/bench.exs"))
    runtime_exs = File.read!(Path.join(@repo_root, "config/runtime.exs"))

    # These defaults are load-bearing for normal deploys and benchmarks: the
    # production path should not require per-run WARAFT_* or FLOW_* env flags.
    assert config_exs =~ "raft_backend: :waraft"
    assert config_exs =~ "waraft_async_log_append: true"
    assert config_exs =~ "flow_async_history: true"
    assert bench_exs =~ "raft_backend: :waraft"
    assert bench_exs =~ "waraft_async_log_append: true"
    assert runtime_exs =~ ~s/System.get_env("FERRICSTORE_RAFT_BACKEND", "waraft")/
    assert runtime_exs =~ ~s/System.get_env("FERRICSTORE_WARAFT_ASYNC_LOG_APPEND", "true")/
    assert runtime_exs =~ "flow_async_history: true"
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
