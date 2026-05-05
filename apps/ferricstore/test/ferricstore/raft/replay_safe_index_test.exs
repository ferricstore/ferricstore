defmodule Ferricstore.Raft.ReplaySafeIndexTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Ferricstore.Raft.{Batcher, ReplaySafeIndex, ReplaySafeIndexWriter}

  test "persists and reads replay-safe index" do
    dir = tmp_dir()

    assert :ok = ReplaySafeIndex.persist(dir, 123)
    assert ReplaySafeIndex.read(dir) == 123
  end

  test "missing or invalid marker reads as zero" do
    dir = tmp_dir()

    assert ReplaySafeIndex.read(dir) == 0

    File.mkdir_p!(dir)
    File.write!(ReplaySafeIndex.path(dir), "bad\n")

    assert ReplaySafeIndex.read(dir) == 0
  end

  test "persist returns error when marker directory cannot be created" do
    dir = tmp_dir()
    File.write!(dir, "not a directory")

    on_exit(fn -> File.rm(dir) end)

    assert {:error, :enotdir} = ReplaySafeIndex.persist(dir, 456)
    assert ReplaySafeIndex.read(dir) == 0
  end

  test "persist reports tmp cleanup failure" do
    dir = tmp_dir()
    File.mkdir_p!(dir)
    tmp_path = ReplaySafeIndex.path(dir) <> ".tmp"
    File.mkdir!(tmp_path)
    parent = self()
    handler_id = {:replay_safe_index_cleanup_failed, parent, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :raft, :replay_safe_index, :cleanup_failed],
      fn event, measurements, metadata, _config ->
        send(parent, {:cleanup_failed, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      File.rm_rf(dir)
    end)

    log =
      capture_log(fn ->
        assert {:error, _reason} = ReplaySafeIndex.persist(dir, 789)
      end)

    assert log =~ "failed to remove raft replay-safe tmp index"

    assert_receive {:cleanup_failed, [:ferricstore, :raft, :replay_safe_index, :cleanup_failed],
                    %{count: 1}, %{path: ^tmp_path, reason: {_kind, _message}}},
                   1_000
  end

  test "writer pokes raft apply path after marker becomes durable" do
    shard_index = 100_000 + System.unique_integer([:positive])
    batcher_name = Batcher.batcher_name(shard_index)
    writer_name = ReplaySafeIndexWriter.process_name(shard_index, nil)
    dir = tmp_dir()
    parent = self()

    Process.register(parent, batcher_name)

    on_exit(fn ->
      if Process.whereis(batcher_name) == parent, do: Process.unregister(batcher_name)
      File.rm_rf(dir)
    end)

    {:ok, writer} =
      ReplaySafeIndexWriter.start_link(
        shard_index: shard_index,
        shard_data_path: dir,
        name: writer_name
      )

    assert :requested = ReplaySafeIndexWriter.request(nil, shard_index, dir, 321)
    assert_receive {:"$gen_cast", {:origin_submit, {:release_cursor_poke, 321}}}, 1_000
    assert ReplaySafeIndex.read(dir) == 321

    GenServer.stop(writer)
  end

  test "writer coalesces many marker requests to latest index" do
    shard_index = 110_000 + System.unique_integer([:positive])
    batcher_name = Batcher.batcher_name(shard_index)
    writer_name = ReplaySafeIndexWriter.process_name(shard_index, nil)
    dir = tmp_dir()
    parent = self()
    handler_id = {:replay_safe_index_coalesce, parent, make_ref()}

    Process.register(parent, batcher_name)

    :telemetry.attach(
      handler_id,
      [:ferricstore, :raft, :replay_safe_index, :persist],
      fn event, measurements, metadata, _config ->
        send(parent, {:persist_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      if Process.whereis(batcher_name) == parent, do: Process.unregister(batcher_name)
      File.rm_rf(dir)
    end)

    {:ok, writer} =
      ReplaySafeIndexWriter.start_link(
        shard_index: shard_index,
        shard_data_path: dir,
        name: writer_name,
        flush_delay_ms: 25
      )

    assert :requested = ReplaySafeIndexWriter.request(nil, shard_index, dir, 100)
    assert :requested = ReplaySafeIndexWriter.request(nil, shard_index, dir, 101)
    assert :requested = ReplaySafeIndexWriter.request(nil, shard_index, dir, 150)

    assert_receive {:"$gen_cast", {:origin_submit, {:release_cursor_poke, 150}}}, 1_000

    assert_receive {:persist_event, [:ferricstore, :raft, :replay_safe_index, :persist],
                    %{index: 150, requested_index: 150, durable_index: 150, lag: 0},
                    %{status: :ok, shard_index: ^shard_index}},
                   1_000

    refute_receive {:"$gen_cast", {:origin_submit, {:release_cursor_poke, 100}}}, 50
    refute_receive {:"$gen_cast", {:origin_submit, {:release_cursor_poke, 101}}}, 50
    assert ReplaySafeIndex.read(dir) == 150

    GenServer.stop(writer)
  end

  test "writer records requested lag and persist failure metrics" do
    shard_index = 0
    dir = tmp_dir()
    File.write!(dir, "not a directory")
    parent = self()
    handler_id = {:replay_safe_index_failure, parent, make_ref()}
    requested = :atomics.new(1, signed: false)
    durable = :atomics.new(1, signed: false)
    failures = :atomics.new(1, signed: false)
    instance_name = :"replay_safe_failure_#{System.unique_integer([:positive])}"

    instance_ctx = %{
      name: instance_name,
      replay_safe_index: durable,
      replay_safe_requested_index: requested,
      replay_safe_persist_failures: failures
    }

    :telemetry.attach(
      handler_id,
      [:ferricstore, :raft, :replay_safe_index, :persist],
      fn event, measurements, metadata, _config ->
        send(parent, {:persist_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      File.rm(dir)
    end)

    {:ok, writer} =
      ReplaySafeIndexWriter.start_link(
        shard_index: shard_index,
        shard_data_path: dir,
        instance_ctx: instance_ctx
      )

    assert :requested = ReplaySafeIndexWriter.request(instance_ctx, shard_index, dir, 456)

    assert_receive {:persist_event, [:ferricstore, :raft, :replay_safe_index, :persist],
                    %{index: 456, requested_index: 456, durable_index: 0, lag: 456},
                    %{status: :error, shard_index: ^shard_index}},
                   1_000

    assert :atomics.get(requested, 1) == 456
    assert :atomics.get(durable, 1) == 0
    assert :atomics.get(failures, 1) == 1

    GenServer.stop(writer)
  end

  test "writer restart publishes existing durable marker after crash before release" do
    shard_index = 0
    dir = tmp_dir()
    durable = :atomics.new(1, signed: false)
    instance_name = :"replay_safe_restart_#{System.unique_integer([:positive])}"
    instance_ctx = %{name: instance_name, replay_safe_index: durable}

    assert :ok = ReplaySafeIndex.persist(dir, 777)

    {:ok, writer} =
      ReplaySafeIndexWriter.start_link(
        shard_index: shard_index,
        shard_data_path: dir,
        instance_ctx: instance_ctx
      )

    assert :atomics.get(durable, 1) == 777
    assert :durable = ReplaySafeIndexWriter.request(instance_ctx, shard_index, dir, 777)

    GenServer.stop(writer)
    File.rm_rf(dir)
  end

  test "writer crash before flush does not publish false durable marker" do
    shard_index = 0
    dir = tmp_dir()
    durable = :atomics.new(1, signed: false)
    requested = :atomics.new(1, signed: false)
    instance_name = :"replay_safe_preflush_#{System.unique_integer([:positive])}"

    instance_ctx = %{
      name: instance_name,
      replay_safe_index: durable,
      replay_safe_requested_index: requested
    }

    {:ok, writer} =
      ReplaySafeIndexWriter.start_link(
        shard_index: shard_index,
        shard_data_path: dir,
        instance_ctx: instance_ctx,
        flush_delay_ms: 1_000
      )

    assert :requested = ReplaySafeIndexWriter.request(instance_ctx, shard_index, dir, 888)
    GenServer.stop(writer)

    assert ReplaySafeIndex.read(dir) == 0
    assert :atomics.get(durable, 1) == 0
    assert :atomics.get(requested, 1) == 888

    {:ok, writer2} =
      ReplaySafeIndexWriter.start_link(
        shard_index: shard_index,
        shard_data_path: dir,
        instance_ctx: instance_ctx
      )

    assert :atomics.get(durable, 1) == 0
    assert :requested = ReplaySafeIndexWriter.request(instance_ctx, shard_index, dir, 889)
    assert wait_until(fn -> ReplaySafeIndex.read(dir) == 889 end)

    GenServer.stop(writer2)
    File.rm_rf(dir)
  end

  defp tmp_dir do
    suffix = "#{System.os_time(:nanosecond)}_#{System.unique_integer([:positive])}"
    Path.join(System.tmp_dir!(), "replay_safe_index_#{suffix}")
  end

  defp wait_until(fun, timeout_ms \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_until(fun, deadline, nil)
  end

  defp wait_until(fun, deadline, _last) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        false
      else
        Process.sleep(10)
        wait_until(fun, deadline, nil)
      end
    end
  end
end
