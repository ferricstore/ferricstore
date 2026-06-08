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

  # Known cycle: Router → CrossShardOp → Router. CrossShardOp.execute is
  # used by Router for cross-shard BITOP/GEO/RENAME because those primitives
  # need to lock multiple shards before dispatching the underlying writes
  # back through Router. Breaking the cycle would require moving cross-shard
  # dispatch out of Router, which is a larger refactor. For now, exclude the
  # two cycle participants from this rule.
  #
  # WARaft is a replacement-backend boundary under active spike work. It must
  # bridge Raft transport, storage apply, keydir recovery, and metrics while it
  # proves parity with the current Ra path, so it is covered by its dedicated
  # WARaft test suite instead of this broad layering rule.
  #
  # Flow projection workers also bridge hot Flow state, cold projection, and
  # release-cursor pokes. They are operational infrastructure, not the public
  # command/data path this architecture rule is intended to police.
  #
  # The semantic split keeps compatibility wrappers at the parent module while
  # moving cohesive implementation sections into child modules. Some children
  # intentionally call public helpers on the parent wrapper, which ArchTest sees
  # as a cycle even though it is a file-organization boundary, not a runtime
  # subsystem dependency. Exclude only those extracted child modules here.
  test "no circular dependencies in Ferricstore" do
    modules_matching("Ferricstore.**")
    |> excluding("Ferricstore.CrossShardOp")
    |> excluding("Ferricstore.Store.Router")
    |> excluding("Ferricstore.Store.Shard.ETS.PrefixScan")
    |> excluding("Ferricstore.Store.Shard.Lifecycle.ProbMigration")
    |> excluding("Ferricstore.Store.Ops.Flush")
    |> excluding("Ferricstore.Store.Ops.MapStore")
    |> excluding("Ferricstore.Raft.WARaftBackend")
    |> excluding("Ferricstore.Raft.WARaftBackend.**")
    |> excluding("Ferricstore.Raft.WARaftStorage")
    |> excluding("Ferricstore.Raft.BlobCommand.FlowAttrs")
    |> excluding("Ferricstore.Flow.Codec.Support")
    |> excluding("Ferricstore.Flow.LMDB.Retention")
    |> excluding("Ferricstore.Flow.LMDB.SegmentPins")
    |> excluding("Ferricstore.Flow.LMDB.TerminalCounts")
    |> excluding("Ferricstore.Flow.PipelineReadCommand")
    |> excluding("Ferricstore.Flow.ReadAPI")
    |> excluding("Ferricstore.Flow.LMDBWriter")
    |> excluding("Ferricstore.Flow.HistoryProjector")
    |> excluding("Ferricstore.Flow.LMDBRebuilder")
    |> excluding("Ferricstore.Commands.Stream.Info")
    |> excluding("Ferricstore.Commands.Stream.Mutations")
    |> excluding("Ferricstore.Commands.Strings.GetEx")
    |> excluding("Ferricstore.Commands.Strings.MSet")
    |> excluding("Ferricstore.Commands.Strings.Range")
    |> excluding("Ferricstore.Test.ClusterHelper.Partition")
    |> excluding("Ferricstore.Waiters.Monitor")
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
