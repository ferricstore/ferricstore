defmodule Ferricstore.Store.BitcaskCheckpointerTest do
  @moduledoc """
  Verifies the background-checkpoint contract:

    1. When a writer raises the per-shard `checkpoint_flags` atomic, the
       checkpointer tick fires `v2_fsync_async` on the shard's active
       file, and telemetry emits `{:ferricstore, :bitcask, :checkpoint}`.
    2. When the flag is not set (idle shard), no fsync happens — no
       `[:ferricstore, :bitcask, :checkpoint]` telemetry is emitted.
    3. `sync_now/1` performs a synchronous fsync and clears the flag
       even if it was set.

  We register a telemetry handler that forwards events to the test
  process and assert on the received messages.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Store.ActiveFile
  alias Ferricstore.Store.BitcaskCheckpointer
  alias Ferricstore.Bitcask.NIF

  # Checkpointer is linked via start_link — when the test process exits,
  # the :EXIT signal shuts the checkpointer down. on_exit runs AFTER the
  # test process is gone, so the pid may already be dead by then. Tolerate
  # that race: alive? + GenServer.stop is still racy; catch + :noproc is
  # the sturdy form.
  defp safe_stop(pid) do
    try do
      GenServer.stop(pid, :normal, 5000)
    catch
      :exit, {:noproc, _} -> :ok
      :exit, :noproc -> :ok
      :exit, _ -> :ok
    end
  end

  setup do
    # Minimal instance-like context with just `checkpoint_flags` (the
    # only field the checkpointer reads). Using shard_index 0 so
    # flag_idx = 1.
    ctx = %{
      name: :"test_ck_#{:erlang.unique_integer([:positive])}",
      shard_count: 1,
      checkpoint_flags: :atomics.new(1, signed: false),
      checkpoint_in_flight: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false)
    }

    tmp =
      Path.join(System.tmp_dir!(), "ck_test_#{:erlang.unique_integer([:positive])}")

    File.mkdir_p!(tmp)
    active_path = Path.join(tmp, "00000.log")
    File.touch!(active_path)

    # Publish into ActiveFile registry so the checkpointer can find the
    # path. Use shard index 0.
    ActiveFile.init(1)
    ActiveFile.publish(ctx, 0, 0, active_path, tmp)

    # Attach a telemetry handler that forwards checkpoint events.
    parent = self()

    handler_id = "ck-test-#{:erlang.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:ferricstore, :bitcask, :checkpoint],
      fn _evt, meas, meta, _ -> send(parent, {:checkpoint, meas, meta}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      File.rm_rf!(tmp)
    end)

    %{ctx: ctx, tmp: tmp, active_path: active_path}
  end

  test "checkpointer fsyncs only when the dirty flag is set", %{
    ctx: ctx,
    active_path: active_path
  } do
    # Start checkpointer with a fast 20ms tick so we don't wait long.
    {:ok, pid} =
      BitcaskCheckpointer.start_link(
        index: 0,
        instance_ctx: ctx,
        checkpoint_interval_ms: 20,
        name: :"ck_dirty_#{:erlang.unique_integer([:positive])}"
      )

    on_exit(fn -> safe_stop(pid) end)

    # No flag set → no checkpoint should fire over three ticks.
    refute_receive {:checkpoint, _meas, %{status: :ok}}, 100

    # Raise the dirty flag (simulates a writer batch).
    :atomics.put(ctx.checkpoint_flags, 1, 1)

    assert_receive {:checkpoint, _meas, %{status: :ok}}, 2000

    # After the fsync fires, the flag must have been cleared.
    assert :atomics.get(ctx.checkpoint_flags, 1) == 0

    # Sanity: the fsync actually touched the active file (v2_fsync on a
    # real path returns :ok; the NIF is loaded).
    assert File.exists?(active_path)
  end

  test "sync_now performs a synchronous fsync and clears the flag", %{ctx: ctx} do
    {:ok, pid} =
      BitcaskCheckpointer.start_link(
        index: 0,
        instance_ctx: ctx,
        checkpoint_interval_ms: 10_000,
        name: :"ck_sync_#{:erlang.unique_integer([:positive])}"
      )

    on_exit(fn -> safe_stop(pid) end)

    :atomics.put(ctx.checkpoint_flags, 1, 1)
    assert :ok = BitcaskCheckpointer.sync_now(pid)
    assert :atomics.get(ctx.checkpoint_flags, 1) == 0
  end

  test "async fsync submit errors reset dirty and in-flight markers", %{
    ctx: ctx,
    active_path: active_path
  } do
    parent = self()

    ctx =
      Map.put(ctx, :fsync_async, fn caller, corr_id, path ->
        dirty = :atomics.get(ctx.checkpoint_flags, 1)
        in_flight = :atomics.get(ctx.checkpoint_in_flight, 1)
        send(parent, {:fsync_async_called, caller, corr_id, path, dirty, in_flight})
        {:error, :submit_failed}
      end)

    {:ok, pid} =
      BitcaskCheckpointer.start_link(
        index: 0,
        instance_ctx: ctx,
        checkpoint_interval_ms: 10_000,
        name: :"ck_submit_error_#{:erlang.unique_integer([:positive])}"
      )

    on_exit(fn -> safe_stop(pid) end)

    :atomics.put(ctx.checkpoint_flags, 1, 1)

    send(pid, :tick)

    assert_receive {:fsync_async_called, ^pid, 1, ^active_path, 0, 1}, 2_000
    assert_receive {:checkpoint, _meas, %{status: :error}}, 2_000
    state = :sys.get_state(pid)

    assert state.in_flight? == false
    assert state.current_corr_id == nil
    assert :atomics.get(ctx.checkpoint_flags, 1) == 1
    assert :atomics.get(ctx.checkpoint_in_flight, 1) == 0
  end

  test "dirty tick with a missing active file does not leave checkpoint in-flight stuck", %{
    ctx: ctx
  } do
    ActiveFile.cleanup_instance(ctx)

    {:ok, pid} =
      BitcaskCheckpointer.start_link(
        index: 0,
        instance_ctx: ctx,
        checkpoint_interval_ms: 10_000,
        name: :"ck_missing_active_#{:erlang.unique_integer([:positive])}"
      )

    on_exit(fn -> safe_stop(pid) end)

    :atomics.put(ctx.checkpoint_flags, 1, 1)

    send(pid, :tick)

    assert_receive {:checkpoint, %{shard_index: 0}, %{status: :error}}, 2_000
    state = :sys.get_state(pid)

    assert state.in_flight? == false
    assert state.current_corr_id == nil
    assert :atomics.get(ctx.checkpoint_flags, 1) == 1
    assert :atomics.get(ctx.checkpoint_in_flight, 1) == 0
  end

  test "sync_now with a missing active file preserves dirty state and in-flight cleanliness", %{
    ctx: ctx
  } do
    ActiveFile.cleanup_instance(ctx)

    {:ok, pid} =
      BitcaskCheckpointer.start_link(
        index: 0,
        instance_ctx: ctx,
        checkpoint_interval_ms: 10_000,
        name: :"ck_sync_missing_active_#{:erlang.unique_integer([:positive])}"
      )

    on_exit(fn -> safe_stop(pid) end)

    :atomics.put(ctx.checkpoint_flags, 1, 1)

    assert {:error, :not_initialized} = BitcaskCheckpointer.sync_now(pid)
    state = :sys.get_state(pid)

    assert state.in_flight? == false
    assert state.current_corr_id == nil
    assert :atomics.get(ctx.checkpoint_flags, 1) == 1
    assert :atomics.get(ctx.checkpoint_in_flight, 1) == 0
  end

  test "writer via state-machine-style put raises the flag", %{ctx: ctx, active_path: active_path} do
    # Emulate the write-path: append a record, then flip the flag the
    # way StateMachine.flush_pending_writes does.
    {:ok, _} = NIF.v2_append_record(active_path, "k", "v", 0)
    :atomics.put(ctx.checkpoint_flags, 1, 1)

    assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
           "writer must raise the dirty flag so the checkpointer picks it up"
  end

  test "shutdown with a dirty shard fires a synchronous fsync", %{
    ctx: ctx,
    active_path: active_path
  } do
    # Append a record via the NOSYNC NIF so the data is only in page
    # cache. Then raise the dirty flag and stop the checkpointer. The
    # terminate/2 barrier must run v2_fsync on the active file and emit
    # a :checkpoint_shutdown telemetry event with dirty?: true.
    parent = self()
    handler_id = "ck-shutdown-#{:erlang.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:ferricstore, :bitcask, :checkpoint_shutdown],
      fn _e, meas, meta, _ -> send(parent, {:shutdown_sync, meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, pid} =
      BitcaskCheckpointer.start_link(
        index: 0,
        instance_ctx: ctx,
        # Very long tick so only terminate/2 can fire the fsync.
        checkpoint_interval_ms: 10_000,
        name: :"ck_shutdown_#{:erlang.unique_integer([:positive])}"
      )

    {:ok, _loc} = NIF.v2_append_batch_nosync(active_path, [{"k", "v", 0}])
    :atomics.put(ctx.checkpoint_flags, 1, 1)

    :ok = GenServer.stop(pid, :normal, 5_000)

    assert_receive {:shutdown_sync, %{shard_index: 0}, %{dirty?: true, result: :ok}},
                   2_000,
                   "terminate/2 must fsync the active file on graceful shutdown"

    assert :atomics.get(ctx.checkpoint_flags, 1) == 0,
           "successful shutdown-fsync must clear the dirty flag"
  end

  test "shutdown with a clean shard skips the fsync", %{ctx: ctx} do
    parent = self()
    handler_id = "ck-shutdown-clean-#{:erlang.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:ferricstore, :bitcask, :checkpoint_shutdown],
      fn _e, meas, meta, _ -> send(parent, {:shutdown_sync, meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, pid} =
      BitcaskCheckpointer.start_link(
        index: 0,
        instance_ctx: ctx,
        checkpoint_interval_ms: 10_000,
        name: :"ck_shutdown_clean_#{:erlang.unique_integer([:positive])}"
      )

    # Ensure flag is cleared.
    :atomics.put(ctx.checkpoint_flags, 1, 0)

    :ok = GenServer.stop(pid, :normal, 5_000)

    assert_receive {:shutdown_sync, %{shard_index: 0}, %{dirty?: false, result: :clean}},
                   2_000,
                   "terminate/2 must observe the clean flag and skip fsync"
  end

  test "shutdown while async fsync is in flight re-dirties and clears in-flight", %{
    ctx: ctx,
    active_path: active_path
  } do
    parent = self()

    ctx =
      Map.put(ctx, :fsync_async, fn caller, corr_id, path ->
        send(parent, {:fsync_async_called, caller, corr_id, path})
        :ok
      end)

    {:ok, pid} =
      BitcaskCheckpointer.start_link(
        index: 0,
        instance_ctx: ctx,
        checkpoint_interval_ms: 10_000,
        name: :"ck_shutdown_in_flight_#{:erlang.unique_integer([:positive])}"
      )

    :atomics.put(ctx.checkpoint_flags, 1, 1)
    send(pid, :tick)

    assert_receive {:fsync_async_called, ^pid, 1, ^active_path}, 2_000
    assert :atomics.get(ctx.checkpoint_flags, 1) == 0
    assert :atomics.get(ctx.checkpoint_in_flight, 1) == 1

    :ok = GenServer.stop(pid, :normal, 5_000)

    assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
           "shutdown cannot prove the in-flight async fsync completed, so the next checkpointer must retry"

    assert :atomics.get(ctx.checkpoint_in_flight, 1) == 0,
           "a stopped checkpointer must not leave release_cursor permanently gated"
  end
end
