defmodule Mix.Tasks.FerricstoreTest do
  @moduledoc """
  Tests for FerricStore Mix operational tasks.

  These tests verify that the CLI tasks -- info, keys, config,
  and merge -- produce correct output and handle edge cases gracefully.

  The application is started via the normal test_helper.exs before any
  test runs. Each test captures stdout via `ExUnit.CaptureIO` to assert
  on printed output without side-effecting the terminal.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  import ExUnit.CaptureIO

  alias Ferricstore.Store.Router
  alias Ferricstore.NamespaceConfig

  setup do
    # Ensure a clean slate before each test: flush all keys, reset
    # namespace config overrides, and drain Raft batchers.
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()

    on_exit(fn ->
      Ferricstore.Test.ShardHelpers.flush_all_keys()
      NamespaceConfig.reset_all()
    end)

    :ok
  end

  # -------------------------------------------------------------------
  # mix ferricstore.info
  # -------------------------------------------------------------------

  describe "mix ferricstore.info" do
    test "outputs node status including uptime, shard count, key count, and memory" do
      output = capture_io(fn -> Mix.Tasks.Ferricstore.Info.run([]) end)

      assert output =~ "FerricStore Node Status"
      assert output =~ "uptime_seconds:"
      assert output =~ "shard_count:"
      assert output =~ "total_keys:"
      assert output =~ "memory_used_bytes:"
      assert output =~ "total_commands:"
    end

    test "outputs raft status for each shard" do
      output = capture_io(fn -> Mix.Tasks.Ferricstore.Info.run([]) end)

      assert output =~ "Shard Status"
      # Should show at least shard 0
      assert output =~ "shard_0:"
    end

    test "uses backend-aware membership lookup" do
      source =
        Path.expand("../../../lib/mix/tasks/ferricstore.info.ex", __DIR__)
        |> File.read!()

      refute source =~ ":" <> "ra.members(",
             "info task must not bypass Ferricstore.Raft.Cluster.members/1 when WARaft is selected"

      assert source =~ "Ferricstore.Raft.Cluster.members(shard_index, 1_000)",
             "info task membership probes must stay bounded under WARaft"
    end

    test "reflects actual key count after writes" do
      baseline = length(Router.keys(FerricStore.Instance.get(:default)))
      Router.put(FerricStore.Instance.get(:default), "info_test_key1", "v1")
      Router.put(FerricStore.Instance.get(:default), "info_test_key2", "v2")

      output = capture_io(fn -> Mix.Tasks.Ferricstore.Info.run([]) end)

      expected = baseline + 2
      assert output =~ "total_keys: #{expected}"
    end
  end

  # -------------------------------------------------------------------
  # mix ferricstore.keys
  # -------------------------------------------------------------------

  describe "mix ferricstore.keys" do
    test "lists all keys when no pattern given" do
      Router.put(FerricStore.Instance.get(:default), "key_alpha", "a")
      Router.put(FerricStore.Instance.get(:default), "key_beta", "b")

      output = capture_io(fn -> Mix.Tasks.Ferricstore.Keys.run([]) end)

      assert output =~ "key_alpha"
      assert output =~ "key_beta"
    end

    test "lists keys matching glob pattern" do
      Router.put(FerricStore.Instance.get(:default), "user:1", "a")
      Router.put(FerricStore.Instance.get(:default), "user:2", "b")
      Router.put(FerricStore.Instance.get(:default), "session:1", "c")

      output = capture_io(fn -> Mix.Tasks.Ferricstore.Keys.run(["user:*"]) end)

      assert output =~ "user:1"
      assert output =~ "user:2"
      refute output =~ "session:1"
    end

    test "reports zero keys when store is empty" do
      # Re-flush immediately before the assertion to drain any async Raft
      # writes that may have landed between the setup flush and this test.
      Ferricstore.Test.ShardHelpers.flush_all_keys()

      output = capture_io(fn -> Mix.Tasks.Ferricstore.Keys.run([]) end)

      assert output =~ "0 key(s)"
    end

    test "matches single-char wildcard with ?" do
      Router.put(FerricStore.Instance.get(:default), "k1", "a")
      Router.put(FerricStore.Instance.get(:default), "k2", "b")
      Router.put(FerricStore.Instance.get(:default), "k10", "c")

      output = capture_io(fn -> Mix.Tasks.Ferricstore.Keys.run(["k?"]) end)

      assert output =~ "k1"
      assert output =~ "k2"
      refute output =~ "k10"
    end
  end

  # -------------------------------------------------------------------
  # mix ferricstore.config
  # -------------------------------------------------------------------

  describe "mix ferricstore.config" do
    test "get returns default config for a namespace" do
      output = capture_io(fn -> Mix.Tasks.Ferricstore.Config.run(["get", "myns"]) end)

      assert output =~ "prefix: myns"
      assert output =~ "window_ms: 1"
      refute output =~ "durability:"
    end

    test "set updates a namespace config field" do
      output =
        capture_io(fn -> Mix.Tasks.Ferricstore.Config.run(["set", "myns", "window_ms", "50"]) end)

      assert output =~ "OK"

      # Verify the value was persisted
      verify_output = capture_io(fn -> Mix.Tasks.Ferricstore.Config.run(["get", "myns"]) end)
      assert verify_output =~ "window_ms: 50"
    end

    test "rejects removed durability field" do
      output =
        capture_io(fn ->
          Mix.Tasks.Ferricstore.Config.run(["set", "fast_ns", "durability", "async"])
        end)

      assert output =~ "ERROR"
      assert output =~ "unknown namespace config field 'durability'"

      verify = capture_io(fn -> Mix.Tasks.Ferricstore.Config.run(["get", "fast_ns"]) end)
      refute verify =~ "durability:"
    end

    test "set with invalid field returns error" do
      output =
        capture_io(fn -> Mix.Tasks.Ferricstore.Config.run(["set", "ns", "bogus_field", "1"]) end)

      assert output =~ "ERROR"
    end

    test "set with invalid value returns error" do
      output =
        capture_io(fn -> Mix.Tasks.Ferricstore.Config.run(["set", "ns", "window_ms", "-5"]) end)

      assert output =~ "ERROR"
    end

    test "prints usage when called with no arguments" do
      output = capture_io(fn -> Mix.Tasks.Ferricstore.Config.run([]) end)

      assert output =~ "Usage:"
    end

    test "prints usage when called with unknown subcommand" do
      output = capture_io(fn -> Mix.Tasks.Ferricstore.Config.run(["delete", "ns"]) end)

      assert output =~ "Usage:"
    end
  end

  # -------------------------------------------------------------------
  # mix ferricstore.merge
  # -------------------------------------------------------------------

  describe "mix ferricstore.merge" do
    test "triggers merge check on specified shard" do
      output = capture_io(fn -> Mix.Tasks.Ferricstore.Merge.run(["0"]) end)

      # Should complete without error -- merge may or may not find work
      assert output =~ "shard 0"
    end

    test "triggers merge check on all shards when no arg given" do
      output = capture_io(fn -> Mix.Tasks.Ferricstore.Merge.run([]) end)

      assert output =~ "shard 0"
      assert output =~ "shard 1"
      assert output =~ "shard 2"
      assert output =~ "shard 3"
    end

    test "rejects invalid shard index" do
      output = capture_io(fn -> Mix.Tasks.Ferricstore.Merge.run(["99"]) end)

      assert output =~ "ERROR" or output =~ "Invalid"
    end

    test "rejects non-numeric shard argument" do
      output = capture_io(fn -> Mix.Tasks.Ferricstore.Merge.run(["abc"]) end)

      assert output =~ "ERROR" or output =~ "Invalid"
    end
  end

  # -------------------------------------------------------------------
  # Edge cases: tasks work when app not started
  # -------------------------------------------------------------------

  describe "edge cases" do
    test "tasks ensure app is started before executing" do
      # The application is already started by test_helper.exs, so this
      # test verifies that the ensure_started call in each task is a
      # no-op when the app is already running (does not crash).
      output = capture_io(fn -> Mix.Tasks.Ferricstore.Info.run([]) end)
      assert output =~ "FerricStore Node Status"

      output = capture_io(fn -> Mix.Tasks.Ferricstore.Keys.run([]) end)
      assert output =~ "key(s)"

      output = capture_io(fn -> Mix.Tasks.Ferricstore.Config.run(["get", "default"]) end)
      assert output =~ "prefix:"

      output = capture_io(fn -> Mix.Tasks.Ferricstore.Merge.run(["0"]) end)
      assert output =~ "shard 0"
    end
  end
end
