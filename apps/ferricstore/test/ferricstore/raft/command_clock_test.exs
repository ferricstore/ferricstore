defmodule Ferricstore.Raft.CommandClockTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.CommandClock

  defp app_path(path) do
    Path.join(Path.expand("../../..", __DIR__), path)
  end

  describe "stamp/1" do
    test "wraps a raft command with an HLC timestamp" do
      command = {:put, "clock_key", "value", 0}

      assert {^command, %{hlc_ts: {physical_ms, logical}}} = CommandClock.stamp(command)
      assert is_integer(physical_ms)
      assert physical_ms > 0
      assert is_integer(logical)
      assert logical >= 0
    end

    test "does not restamp an already stamped command" do
      stamped = {{:delete, "clock_key"}, %{hlc_ts: {123_456, 7}}}

      assert ^stamped = CommandClock.stamp(stamped)
    end
  end

  describe "to_ttb/1" do
    test "serializes the stamped command for ra ttb submission" do
      command = {:batch, [{:put, "clock_a", "a", 0}, {:delete, "clock_b"}]}

      assert {:ttb, binary} = CommandClock.to_ttb(command)
      assert {^command, %{hlc_ts: {_physical_ms, _logical}}} = :erlang.binary_to_term(binary)
    end
  end

  describe "raft submit paths" do
    test "batcher serializes HLC-stamped payloads before pipeline submission" do
      source = File.read!(app_path("lib/ferricstore/raft/batcher.ex"))

      assert source =~ "CommandClock.to_ttb(single_cmd)"
      assert source =~ "CommandClock.to_ttb({:batch, batch})"
      assert source =~ "CommandClock.to_ttb({:batch, wrapped_batch})"
    end

    test "direct cross-shard raft calls go through CommandClock" do
      cross_shard = File.read!(app_path("lib/ferricstore/cross_shard_op.ex"))
      resolver = File.read!(app_path("lib/ferricstore/cross_shard_op/intent_resolver.ex"))
      tx = File.read!(app_path("lib/ferricstore/transaction/coordinator.ex"))

      assert cross_shard =~ "CommandClock.process_command"
      assert resolver =~ "CommandClock.process_command"
      assert tx =~ "CommandClock.pipeline_command"

      refute cross_shard =~ ":ra.process_command("
      refute resolver =~ ":ra.process_command("
      refute tx =~ ":ra.pipeline_command("
    end
  end
end
