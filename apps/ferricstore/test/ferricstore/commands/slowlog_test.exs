defmodule Ferricstore.Commands.SlowLogTest do
  @moduledoc """
  Tests for the SLOWLOG command family.

  Tests both the SlowLog module directly and the Server command handlers
  that expose SLOWLOG GET, SLOWLOG LEN, SLOWLOG RESET, and SLOWLOG HELP.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Server
  alias Ferricstore.SlowLog
  alias Ferricstore.Test.{MockStore, ShardHelpers}

  setup do
    # Reset the slow log before each test to ensure isolation.
    SlowLog.reset()
    :ok
  end

  # ---------------------------------------------------------------------------
  # SlowLog module — direct API tests
  # ---------------------------------------------------------------------------

  describe "SlowLog module" do
    test "get returns empty list initially" do
      assert SlowLog.get() == []
    end

    test "len returns 0 initially" do
      assert SlowLog.len() == 0
    end

    test "reset returns :ok" do
      assert SlowLog.reset() == :ok
    end

    test "maybe_log records command when duration exceeds threshold" do
      # Set threshold to 0 so every command is logged.
      original = SlowLog.threshold()
      SlowLog.set_threshold(0)

      on_exit(fn ->
        SlowLog.set_threshold(original)
      end)

      SlowLog.maybe_log(["SET", "key", "value"], 100)

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() == 1
        end,
        "slowlog entry not recorded",
        20,
        10
      )

      entries = SlowLog.get()
      assert length(entries) == 1
      [{id, timestamp_us, duration_us, command}] = entries
      assert id == 0
      assert is_integer(timestamp_us)
      assert duration_us == 100
      assert command == ["SET", "key", "value"]
    end

    test "maybe_log does not record when duration is below threshold" do
      # Default threshold is 10_000 us.
      SlowLog.maybe_log(["GET", "key"], 5)

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() == 0
        end,
        "slowlog should remain empty",
        10,
        10
      )
    end

    test "maybe_log does not record when threshold is -1 (disabled)" do
      original = SlowLog.threshold()
      SlowLog.set_threshold(-1)

      on_exit(fn ->
        SlowLog.set_threshold(original)
      end)

      SlowLog.maybe_log(["SET", "key", "value"], 999_999)

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() == 0
        end,
        "slowlog should remain empty when disabled",
        10,
        10
      )
    end

    test "get with count limits results" do
      original = SlowLog.threshold()
      SlowLog.set_threshold(0)

      on_exit(fn ->
        SlowLog.set_threshold(original)
      end)

      for i <- 1..5 do
        SlowLog.maybe_log(["CMD_#{i}"], i * 100)
        Process.sleep(10)
      end

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() == 5
        end,
        "5 slowlog entries not recorded",
        20,
        10
      )

      assert length(SlowLog.get(2)) == 2
      assert SlowLog.get(0) == []
    end

    test "entries are returned newest first" do
      original = SlowLog.threshold()
      SlowLog.set_threshold(0)

      on_exit(fn ->
        SlowLog.set_threshold(original)
      end)

      SlowLog.maybe_log(["FIRST"], 100)
      Process.sleep(10)
      SlowLog.maybe_log(["SECOND"], 200)

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() == 2
        end,
        "2 slowlog entries not recorded",
        20,
        10
      )

      [{id1, _, _, cmd1}, {id2, _, _, cmd2}] = SlowLog.get()
      assert id1 > id2
      assert cmd1 == ["SECOND"]
      assert cmd2 == ["FIRST"]
    end

    test "ring buffer evicts oldest entries when max_len exceeded" do
      original_threshold = SlowLog.threshold()
      original_max = SlowLog.max_len()

      SlowLog.set_threshold(0)
      SlowLog.set_max_len(3)

      on_exit(fn ->
        SlowLog.set_threshold(original_threshold)
        SlowLog.set_max_len(original_max)
      end)

      for i <- 1..5 do
        SlowLog.maybe_log(["CMD_#{i}"], i * 100)
        Process.sleep(10)
      end

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() == 3
        end,
        "slowlog eviction not complete",
        20,
        10
      )

      # The newest 3 entries should be retained.
      entries = SlowLog.get()
      commands = Enum.map(entries, fn {_, _, _, cmd} -> cmd end)
      assert ["CMD_5"] in commands
      assert ["CMD_4"] in commands
      assert ["CMD_3"] in commands
    end

    test "reset clears all entries and resets ID counter" do
      original = SlowLog.threshold()
      SlowLog.set_threshold(0)

      on_exit(fn ->
        SlowLog.set_threshold(original)
      end)

      SlowLog.maybe_log(["OLD_CMD"], 100)

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() == 1
        end,
        "slowlog entry not recorded",
        20,
        10
      )

      SlowLog.reset()
      assert SlowLog.len() == 0
      assert SlowLog.get() == []

      # After reset, IDs restart from 0.
      SlowLog.maybe_log(["NEW_CMD"], 200)

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() == 1
        end,
        "slowlog entry not recorded after reset",
        20,
        10
      )

      [{id, _, _, _}] = SlowLog.get()
      assert id == 0
    end
  end

  # ---------------------------------------------------------------------------
  # SLOWLOG command handlers — via Server.handle/3
  # ---------------------------------------------------------------------------

  describe "SLOWLOG GET" do
    test "returns empty list initially" do
      result = Server.handle("SLOWLOG", ["GET"], MockStore.make())
      assert result == []
    end

    test "subcommand is case-insensitive" do
      assert [] == Server.handle("SLOWLOG", ["get"], MockStore.make())
    end

    test "returns entries after slow operations are logged" do
      original = SlowLog.threshold()
      SlowLog.set_threshold(0)

      on_exit(fn ->
        SlowLog.set_threshold(original)
      end)

      SlowLog.maybe_log(["SET", "mykey", "myval"], 15_000)

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() == 1
        end,
        "slowlog entry not recorded",
        20,
        10
      )

      result = Server.handle("SLOWLOG", ["GET"], MockStore.make())
      assert length(result) == 1
      [[id, timestamp, duration, command]] = result
      assert is_integer(id)
      assert is_integer(timestamp)
      assert duration == 15_000
      assert command == ["SET", "mykey", "myval"]
    end

    test "with count limits results" do
      original = SlowLog.threshold()
      SlowLog.set_threshold(0)

      on_exit(fn ->
        SlowLog.set_threshold(original)
      end)

      for i <- 1..5 do
        SlowLog.maybe_log(["CMD_#{i}"], i * 100)
        Process.sleep(10)
      end

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() == 5
        end,
        "5 slowlog entries not recorded",
        20,
        10
      )

      result = Server.handle("SLOWLOG", ["GET", "2"], MockStore.make())
      assert length(result) == 2
    end

    test "with invalid count returns error" do
      result = Server.handle("SLOWLOG", ["GET", "abc"], MockStore.make())
      assert {:error, _} = result
    end

    test "entry format is [id, timestamp, duration, command_array]" do
      original = SlowLog.threshold()
      SlowLog.set_threshold(0)

      on_exit(fn ->
        SlowLog.set_threshold(original)
      end)

      SlowLog.maybe_log(["GET", "foo"], 42)

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() == 1
        end,
        "slowlog entry not recorded",
        20,
        10
      )

      [[id, timestamp, duration, command]] =
        Server.handle("SLOWLOG", ["GET"], MockStore.make())

      assert is_integer(id) and id >= 0
      assert is_integer(timestamp) and timestamp > 0
      assert is_integer(duration) and duration == 42
      assert is_list(command) and command == ["GET", "foo"]
    end
  end

  describe "SLOWLOG LEN" do
    test "returns 0 initially" do
      assert 0 == Server.handle("SLOWLOG", ["LEN"], MockStore.make())
    end

    test "subcommand is case-insensitive" do
      assert 0 == Server.handle("SLOWLOG", ["len"], MockStore.make())
    end

    test "returns correct count after logging" do
      original = SlowLog.threshold()
      SlowLog.set_threshold(0)

      on_exit(fn ->
        SlowLog.set_threshold(original)
      end)

      SlowLog.maybe_log(["A"], 10)
      SlowLog.maybe_log(["B"], 20)

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() == 2
        end,
        "2 slowlog entries not recorded",
        20,
        10
      )

      assert 2 == Server.handle("SLOWLOG", ["LEN"], MockStore.make())
    end
  end

  describe "SLOWLOG RESET" do
    test "returns :ok" do
      assert :ok == Server.handle("SLOWLOG", ["RESET"], MockStore.make())
    end

    test "clears all entries" do
      original = SlowLog.threshold()
      SlowLog.set_threshold(0)

      on_exit(fn ->
        SlowLog.set_threshold(original)
      end)

      SlowLog.maybe_log(["CMD"], 100)

      ShardHelpers.eventually(
        fn ->
          SlowLog.len() > 0
        end,
        "slowlog entry not recorded",
        20,
        10
      )

      Server.handle("SLOWLOG", ["RESET"], MockStore.make())
      assert 0 == Server.handle("SLOWLOG", ["LEN"], MockStore.make())
    end
  end

  describe "SLOWLOG HELP" do
    test "returns list of help strings" do
      result = Server.handle("SLOWLOG", ["HELP"], MockStore.make())
      assert is_list(result)
      assert result != []
      assert Enum.all?(result, &is_binary/1)
    end
  end

  describe "SLOWLOG unknown subcommand" do
    test "returns error for unknown subcommand" do
      result = Server.handle("SLOWLOG", ["UNKNOWN"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "unknown subcommand"
    end

    test "returns error for no subcommand" do
      result = Server.handle("SLOWLOG", [], MockStore.make())
      assert {:error, _} = result
    end
  end
end
