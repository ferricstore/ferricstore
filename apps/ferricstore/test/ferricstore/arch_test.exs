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
  test "no circular dependencies in Ferricstore" do
    modules_matching("Ferricstore.**")
    |> excluding("Ferricstore.CrossShardOp")
    |> excluding("Ferricstore.Store.Router")
    |> excluding("Ferricstore.Raft.WARaftBackend")
    |> excluding("Ferricstore.Raft.WARaftBackend.**")
    |> excluding("Ferricstore.Raft.WARaftStorage")
    |> excluding("Ferricstore.Flow.LMDBWriter")
    |> excluding("Ferricstore.Flow.HistoryProjector")
    |> excluding("Ferricstore.Flow.LMDBRebuilder")
    |> should_be_free_of_cycles()
  end
end
