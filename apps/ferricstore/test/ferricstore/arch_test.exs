defmodule Ferricstore.ArchTest do
  # ArchTest walks the loaded code graph and xref metadata. Keep it strict, but
  # do not run it beside runtime GenServer timeout tests; the scanner is global
  # and CPU-heavy enough to create false timeout noise in unrelated product tests.
  use ExUnit.Case, async: false
  use ArchTest, app: :ferricstore

  # ---------------------------------------------------------------------------
  # Dependency rules
  #
  # Layer order (outermost -> innermost):
  #   Commands -> Store -> Bitcask/NIF
  #
  # Server-specific modules (Connection, Listener, Resp, ACL, ClientTracking)
  # have been moved to the separate :ferricstore_server umbrella app.
  # The library has zero references to server modules.
  # ---------------------------------------------------------------------------

  @max_production_file_lines 1_000

  test "core production files stay below the agreed readability budget" do
    assert files_over_line_budget(core_production_files()) == []
  end

  test "core library source does not reference the server application" do
    assert source_reference_violations(core_production_files(), ~r/FerricstoreServer(\.|$)/) == []
  end

  test "core implementation does not use anonymous part section files" do
    assert Path.wildcard(core_path("lib/**/sections/part_*.ex")) == []
  end

  test "public API layer does not depend on durability internals" do
    api_modules = modules_matching("FerricStore.API.**")

    api_modules
    |> should_not_depend_on(modules_matching("Ferricstore.Raft.**"))

    api_modules
    |> should_not_depend_on(modules_matching("Ferricstore.Bitcask.**"))
  end

  @tag timeout: 60_000
  test "store layer does not depend on commands (except Shard for tx)" do
    # Shard depends on Commands.Dispatcher for tx_execute (MULTI/EXEC).
    # This is an intentional coupling: tx_execute must dispatch queued
    # commands within the shard's handle_call for atomicity.
    modules_matching("Ferricstore.Store.**")
    |> excluding("Ferricstore.Store.Shard")
    |> excluding("Ferricstore.Store.Shard.Transaction")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "bitcask NIF wrapper does not depend on any Ferricstore layer" do
    modules_matching("Ferricstore.Bitcask.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Store.**"))

    modules_matching("Ferricstore.Bitcask.**")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "raft state machine may depend on Commands.Dispatcher for cross-shard tx" do
    modules_matching("Ferricstore.Raft.**")
    |> excluding("Ferricstore.Raft.StateMachine")
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "command modules do not depend directly on durability internals" do
    command_modules =
      modules_matching("Ferricstore.Commands.**")
      |> excluding("Ferricstore.Commands.Cluster")
      |> excluding("Ferricstore.Commands.Server")
      |> excluding("Ferricstore.Commands.Server.Info")

    command_modules
    |> should_not_depend_on(modules_matching("Ferricstore.Raft.**"))

    command_modules
    |> excluding("Ferricstore.Commands.Bloom")
    |> excluding("Ferricstore.Commands.CMS")
    |> excluding("Ferricstore.Commands.Cuckoo")
    |> excluding("Ferricstore.Commands.TopK")
    |> should_not_depend_on(modules_matching("Ferricstore.Bitcask.**"))
  end

  # Remaining cycle exceptions are subsystem boundaries, not file-split cleanup.
  #
  # Router ↔ CrossShardOp is intentional today: multi-key commands choose shard
  # ownership in Router, then CrossShardOp locks the involved shards and routes
  # the underlying primitive writes back through Router. Breaking this requires
  # a dedicated cross-shard dispatch layer, not a local extraction.
  #
  # WARaft is a replacement-backend boundary under active spike work. It must
  # bridge Raft transport, storage apply, keydir recovery, and metrics while it
  # proves parity with the current Ra path, so it is covered by its dedicated
  # WARaft test suite instead of this broad layering rule.
  #
  # Flow projection workers are GenServer/process-state coordinators bridging
  # hot Flow truth, LMDB/history projection, retention, and release-cursor
  # pokes. They remain excluded until projection coordination is split into a
  # lower service module with no parent facade dependency.
  test "no circular dependencies in Ferricstore" do
    modules_matching("Ferricstore.**")
    |> excluding("Ferricstore.CrossShardOp")
    |> excluding("Ferricstore.Store.Router")
    |> excluding("Ferricstore.Raft.WARaftBackend")
    |> excluding("Ferricstore.Raft.WARaftBackend.**")
    |> excluding("Ferricstore.Raft.WARaftStorage")
    |> excluding("Ferricstore.Flow.LMDBWriter")
    |> excluding("Ferricstore.Flow.HistoryProjector")
    |> should_be_free_of_cycles()
  end

  defp core_production_files do
    core_path("lib/**/*.ex")
    |> Path.wildcard()
    |> Enum.reject(&String.contains?(&1, "/test/"))
  end

  defp core_path(path), do: Path.expand("../../#{path}", __DIR__)

  defp files_over_line_budget(paths) do
    paths
    |> Enum.map(fn path -> {Path.relative_to_cwd(path), line_count(path)} end)
    |> Enum.filter(fn {_path, count} -> count > @max_production_file_lines end)
  end

  defp line_count(path) do
    path
    |> File.stream!()
    |> Enum.count()
  end

  defp source_reference_violations(paths, pattern) do
    paths
    |> Enum.filter(fn path ->
      path
      |> source_without_comments()
      |> String.match?(pattern)
    end)
    |> Enum.map(&Path.relative_to_cwd/1)
  end

  defp source_without_comments(path) do
    path
    |> File.stream!()
    |> Stream.reject(fn line -> line |> String.trim_leading() |> String.starts_with?("#") end)
    |> Enum.join()
  end
end
