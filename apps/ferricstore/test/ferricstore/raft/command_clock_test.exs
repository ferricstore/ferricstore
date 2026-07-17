defmodule Ferricstore.Raft.CommandClockTest do
  use ExUnit.Case, async: false
  @moduletag :raft

  alias Ferricstore.Raft.Cluster, as: RaftCluster
  alias Ferricstore.Raft.CommandClock
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.Router

  describe "stamp/1" do
    test "wraps a raft command with an HLC timestamp" do
      command = {:put, "clock_key", "value", 0}

      assert {^command, %{hlc_ts: {physical_ms, logical}, wall_time_ms: wall_time_ms}} =
               CommandClock.stamp(command)

      assert is_integer(physical_ms)
      assert physical_ms > 0
      assert is_integer(logical)
      assert logical >= 0
      assert wall_time_ms > 0
      assert wall_time_ms <= physical_ms
    end

    test "does not restamp an already stamped command" do
      stamped =
        {{:delete, "clock_key"}, %{hlc_ts: {123_456, 7}, wall_time_ms: 123_000}}

      assert ^stamped = CommandClock.stamp(stamped)
    end

    test "rejects the removed stamp shape without a wall snapshot" do
      assert_raise ArgumentError, fn ->
        CommandClock.stamp({{:delete, "clock_key"}, %{hlc_ts: {123_456, 7}}})
      end
    end
  end

  describe "to_ttb/1" do
    test "serializes the stamped command for replicated submission" do
      command = {:batch, [{:put, "clock_a", "a", 0}, {:delete, "clock_b"}]}

      assert {:ttb, binary} = CommandClock.to_ttb(command)

      assert {^command, %{hlc_ts: {_physical_ms, _logical}, wall_time_ms: wall_time_ms}} =
               :erlang.binary_to_term(binary)

      assert is_integer(wall_time_ms)
    end
  end

  describe "raft submit paths" do
    test "CommandClock submits through WARaft" do
      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-command-clock-waraft-#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)
      Ferricstore.DataDir.ensure_layout!(root, 1)
      Ferricstore.Store.ActiveFile.init(1)

      ctx =
        FerricStore.Instance.build(
          :"command_clock_waraft_#{System.unique_integer([:positive])}",
          data_dir: root,
          shard_count: 1,
          max_memory_bytes: 256 * 1024 * 1024,
          keydir_max_ram: 64 * 1024 * 1024,
          hot_cache_max_value_size: 65_536,
          max_active_file_size: 64 * 1024 * 1024
        )

      try do
        assert :ok = WARaftBackend.start(ctx)

        shard_id = RaftCluster.shard_server_id(0)

        assert {:ok, {:applied_at, process_index, :ok}, _leader} =
                 CommandClock.process_command(shard_id, {:put, "clock:w process", "pv", 0})

        assert is_integer(process_index)
        assert "pv" == Router.get(ctx, "clock:w process")

        corr = make_ref()

        assert :ok =
                 CommandClock.pipeline_command(
                   shard_id,
                   {:put, "clock:w pipe", "iv", 0},
                   corr,
                   :low
                 )

        assert_receive {:raft_pipeline_applied, ^corr, {:applied_at, pipe_index, :ok}},
                       1_000

        assert is_integer(pipe_index)
        assert "iv" == Router.get(ctx, "clock:w pipe")
      after
        _ = WARaftBackend.stop()
        FerricStore.Instance.cleanup(ctx.name)
        File.rm_rf!(root)
      end
    end
  end
end
