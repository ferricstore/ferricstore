defmodule Ferricstore.Flow.LMDBTest.Sections.StateMachineStillRequiresLmdbMirrorEnqueueModeConfiguredOff do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Flow.LMDBTest.FlushProbeWriter

      test "state-machine still requires LMDB mirror enqueue when mode is configured off" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_trap = Process.flag(:trap_exit, true)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :off)

        ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)
        writer_name = Ferricstore.Flow.LMDBWriter.name(ctx.name, 0)
        writer_pid = Process.whereis(writer_name)

        on_exit(fn ->
          Process.flag(:trap_exit, old_trap)
          Ferricstore.Test.IsolatedInstance.checkin(ctx)
          restore_env(:flow_lmdb_mode, old_mode)
        end)

        Process.exit(writer_pid, :kill)
        assert_receive {:EXIT, ^writer_pid, :killed}

        assert :ok =
                 Ferricstore.Flow.create(ctx, "mirror-off-no-enqueue",
                   type: "mirror-off-no-enqueue",
                   partition_key: "tenant-mirror-off-no-enqueue",
                   correlation_id: "correlation-mirror-off-no-enqueue",
                   run_at_ms: 1,
                   now_ms: 1
                 )

        assert {:ok, [claimed]} =
                 Ferricstore.Flow.claim_due(ctx, "mirror-off-no-enqueue",
                   partition_key: "tenant-mirror-off-no-enqueue",
                   worker: "worker-mirror-off-no-enqueue",
                   now_ms: 2
                 )

        assert :ok =
                 Ferricstore.Flow.complete(ctx, claimed.id, claimed.lease_token,
                   partition_key: "tenant-mirror-off-no-enqueue",
                   fencing_token: claimed.fencing_token,
                   now_ms: 3
                 )

        assert :atomics.get(ctx.flow_lmdb_mirror_enqueue_failures, 1) == 3
        assert :atomics.get(ctx.flow_lmdb_mirror_degraded, 1) == 1
      end

      test "mirror writer persists replay-safe marker only after pending ops flush" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_marker_#{System.unique_integer([:positive])}"
          )

        shard_index = 42
        instance_name = :"flow_lmdb_writer_marker_#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        key = "flow:{flow:test}:state:marker"
        atomics_size = shard_index + 1
        durable = :atomics.new(atomics_size, signed: false)
        requested = :atomics.new(atomics_size, signed: false)
        failures = :atomics.new(atomics_size, signed: false)

        instance_ctx = %{
          name: instance_name,
          flow_lmdb_replay_safe_index: durable,
          flow_lmdb_replay_safe_requested_index: requested,
          flow_lmdb_replay_safe_persist_failures: failures
        }

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          File.rm_rf!(data_dir)
        end)

        File.mkdir_p!(shard_data_path)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index,
           data_dir: data_dir,
           instance_ctx: instance_ctx,
           instance_name: instance_name}
        )

        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, key, "v1"}
                 ])

        assert :requested =
                 Ferricstore.Flow.LMDBWriter.request(
                   instance_ctx,
                   shard_index,
                   shard_data_path,
                   123
                 )

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

        assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)
        assert :atomics.get(durable, shard_index + 1) == 123
        assert Ferricstore.Flow.LMDBReplaySafeIndex.read(shard_data_path) == 123
      end

      test "mirror writer persists replay-safe marker without consensus pokes" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_waraft_marker_#{System.unique_integer([:positive])}"
          )

        shard_index = 52
        instance_name = :"flow_lmdb_writer_waraft_marker_#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        key = "flow:{flow:test}:state:waraft-marker"
        atomics_size = shard_index + 1
        durable = :atomics.new(atomics_size, signed: false)
        requested = :atomics.new(atomics_size, signed: false)
        failures = :atomics.new(atomics_size, signed: false)

        instance_ctx = %{
          name: instance_name,
          flow_lmdb_replay_safe_index: durable,
          flow_lmdb_replay_safe_requested_index: requested,
          flow_lmdb_replay_safe_persist_failures: failures
        }

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          File.rm_rf!(data_dir)
        end)

        File.mkdir_p!(shard_data_path)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index,
           data_dir: data_dir,
           instance_ctx: instance_ctx,
           instance_name: instance_name}
        )

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, key, "v1"}
                 ])

        assert :requested =
                 Ferricstore.Flow.LMDBWriter.request(
                   instance_ctx,
                   shard_index,
                   shard_data_path,
                   222
                 )

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert :atomics.get(durable, shard_index + 1) == 222
        assert Ferricstore.Flow.LMDBReplaySafeIndex.read(shard_data_path) == 222
        refute_receive {:"$gen_cast", {:origin_submit, _command}}, 100
      end

      test "mirror writer refuses replay-safe marker while shard is degraded" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_degraded_marker_#{System.unique_integer([:positive])}"
          )

        shard_index = 7
        instance_name = :"flow_lmdb_writer_degraded_marker_#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        atomics_size = shard_index + 1
        durable = :atomics.new(atomics_size, signed: false)
        requested = :atomics.new(atomics_size, signed: false)
        failures = :atomics.new(atomics_size, signed: false)
        degraded = :atomics.new(atomics_size, signed: false)

        instance_ctx = %{
          name: instance_name,
          flow_lmdb_replay_safe_index: durable,
          flow_lmdb_replay_safe_requested_index: requested,
          flow_lmdb_replay_safe_persist_failures: failures,
          flow_lmdb_mirror_degraded: degraded
        }

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          File.rm_rf!(data_dir)
        end)

        File.mkdir_p!(shard_data_path)
        :atomics.put(degraded, shard_index + 1, 1)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index,
           data_dir: data_dir,
           instance_ctx: instance_ctx,
           instance_name: instance_name}
        )

        assert {:error, :mirror_degraded} =
                 Ferricstore.Flow.LMDBWriter.request(
                   instance_ctx,
                   shard_index,
                   shard_data_path,
                   789
                 )

        assert :atomics.get(requested, shard_index + 1) == 789
        assert :atomics.get(durable, shard_index + 1) == 0
        assert Ferricstore.Flow.LMDBReplaySafeIndex.read(shard_data_path) == 0
      end

      test "mirror writer crash before marker flush does not publish replay-safe index" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
        old_trap = Process.flag(:trap_exit, true)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_marker_crash_#{System.unique_integer([:positive])}"
          )

        shard_index = 5
        instance_name = :"flow_lmdb_writer_marker_crash_#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        key = "flow:{flow:test}:state:marker-crash"
        atomics_size = shard_index + 1
        durable = :atomics.new(atomics_size, signed: false)
        requested = :atomics.new(atomics_size, signed: false)
        failures = :atomics.new(atomics_size, signed: false)

        instance_ctx = %{
          name: instance_name,
          flow_lmdb_replay_safe_index: durable,
          flow_lmdb_replay_safe_requested_index: requested,
          flow_lmdb_replay_safe_persist_failures: failures
        }

        on_exit(fn ->
          Process.flag(:trap_exit, old_trap)
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          File.rm_rf!(data_dir)
        end)

        File.mkdir_p!(shard_data_path)

        assert {:ok, pid} =
                 Ferricstore.Flow.LMDBWriter.start_link(
                   shard_index: shard_index,
                   data_dir: data_dir,
                   instance_ctx: instance_ctx,
                   instance_name: instance_name
                 )

        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, key, "v1"}
                 ])

        assert %{count: 1} = :sys.get_state(pid)
        :ok = :sys.suspend(pid)

        assert :requested =
                 Ferricstore.Flow.LMDBWriter.request(
                   instance_ctx,
                   shard_index,
                   shard_data_path,
                   456
                 )

        ref = Process.monitor(pid)
        Process.exit(pid, :kill)
        assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

        assert :atomics.get(requested, shard_index + 1) == 456
        assert :atomics.get(durable, shard_index + 1) == 0
        assert Ferricstore.Flow.LMDBReplaySafeIndex.read(shard_data_path) == 0
        assert :not_found = Ferricstore.Flow.LMDB.get(path, key)
      end

      test "mirror writer restart refuses replay-safe advancement after losing async enqueue mailbox" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
        old_trap = Process.flag(:trap_exit, true)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_async_loss_#{System.unique_integer([:positive])}"
          )

        shard_index = 6
        instance_name = :"flow_lmdb_writer_async_loss_#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        key = "flow:{flow:test}:state:async-loss"
        atomics_size = shard_index + 1
        durable = :atomics.new(atomics_size, signed: false)
        requested = :atomics.new(atomics_size, signed: false)
        failures = :atomics.new(atomics_size, signed: false)
        degraded = :atomics.new(atomics_size, signed: false)

        instance_ctx = %{
          name: instance_name,
          flow_lmdb_replay_safe_index: durable,
          flow_lmdb_replay_safe_requested_index: requested,
          flow_lmdb_replay_safe_persist_failures: failures,
          flow_lmdb_mirror_degraded: degraded
        }

        on_exit(fn ->
          Process.flag(:trap_exit, old_trap)
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          File.rm_rf!(data_dir)
        end)

        File.mkdir_p!(shard_data_path)

        writer_opts = [
          shard_index: shard_index,
          data_dir: data_dir,
          instance_ctx: instance_ctx,
          instance_name: instance_name
        ]

        assert {:ok, pid} = Ferricstore.Flow.LMDBWriter.start_link(writer_opts)
        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        :ok = :sys.suspend(pid)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue_async(instance_name, shard_index, [
                   {:put, key, "v1"}
                 ])

        ref = Process.monitor(pid)
        Process.exit(pid, :kill)
        assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

        assert {:ok, restarted} = Ferricstore.Flow.LMDBWriter.start_link(writer_opts)
        assert Process.alive?(restarted)

        assert {:error, :mirror_degraded} =
                 Ferricstore.Flow.LMDBWriter.request(
                   instance_ctx,
                   shard_index,
                   shard_data_path,
                   654
                 )

        assert :atomics.get(requested, shard_index + 1) == 654
        assert :atomics.get(degraded, shard_index + 1) == 1
        assert :atomics.get(durable, shard_index + 1) == 0
        assert Ferricstore.Flow.LMDBReplaySafeIndex.read(shard_data_path) == 0
        assert :not_found = Ferricstore.Flow.LMDB.get(path, key)
      end

      test "mirror writer suspend drains async enqueues accepted before suspend" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_suspend_async_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_writer_suspend_async_#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        key = "flow:{flow:test}:state:suspend-async"
        degraded = :atomics.new(1, signed: false)

        instance_ctx = %{
          name: instance_name,
          flow_lmdb_mirror_degraded: degraded
        }

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          File.rm_rf!(data_dir)
        end)

        File.mkdir_p!(shard_data_path)

        {:ok, pid} =
          start_supervised(
            {Ferricstore.Flow.LMDBWriter,
             shard_index: shard_index,
             data_dir: data_dir,
             instance_ctx: instance_ctx,
             instance_name: instance_name}
          )

        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        :ok = :sys.suspend(pid)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue_async(instance_name, shard_index, [
                   {:put, key, "v1"}
                 ])

        parent = self()

        suspend_task =
          Task.async(fn ->
            send(parent, :suspend_started)
            result = Ferricstore.Flow.LMDBWriter.suspend(instance_name, shard_index)
            send(parent, {:suspend_finished, result})
            result
          end)

        assert_receive :suspend_started, 500
        Process.sleep(20)
        :ok = :sys.resume(pid)

        assert Task.await(suspend_task, 1_000) == :ok
        assert_receive {:suspend_finished, :ok}, 500
        assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)
        assert :atomics.get(degraded, 1) == 0
      end

      test "snapshot preparation flushes queued projection writes before suspending" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_snapshot_prepare_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_snapshot_prepare_#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        key = "flow:{flow:test}:state:snapshot-prepare"
        stale_key = "flow:{flow:test}:state:snapshot-prepare-stale"
        degraded = :atomics.new(1, signed: false)

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)

        writer =
          start_supervised!(
            {Ferricstore.Flow.LMDBWriter,
             shard_index: shard_index,
             data_dir: data_dir,
             instance_ctx: %{
               name: instance_name,
               flow_lmdb_mirror_degraded: degraded
             },
             instance_name: instance_name}
          )

        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, key, "v1"}
                 ])

        assert :not_found = Ferricstore.Flow.LMDB.get(path, key)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.prepare_snapshot_install(
                   instance_name,
                   shard_index
                 )

        assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)

        assert {:error, :writer_suspended} =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, key, "v2"}
                 ])

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.resume_after_snapshot_install(
                   instance_name,
                   shard_index
                 )

        seq_ref =
          :persistent_term.get(
            {Ferricstore.Flow.LMDBWriter, :enqueue_seq, instance_name, shard_index}
          )

        seq = :atomics.add_get(seq_ref, 1, 1)

        assert :ok = GenServer.call(writer, :prepare_snapshot_install)

        GenServer.cast(writer, {:enqueue, seq, [{:put, stale_key, "stale"}], [], {seq_ref, 0}})
        _state = :sys.get_state(writer)

        assert :atomics.get(degraded, 1) == 0

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.resume_after_snapshot_install(
                   instance_name,
                   shard_index
                 )

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, stale_key)

        :sys.replace_state(writer, &Map.put(&1, :terminal_atomic_write?, true))

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.prepare_snapshot_install(
                   instance_name,
                   shard_index
                 )

        reset_state = :sys.get_state(writer)
        refute reset_state.terminal_atomic_write?
        refute Map.has_key?(reset_state, :terminal_count_cache)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.resume_after_snapshot_install(
                   instance_name,
                   shard_index
                 )
      end

      test "mirror writer persists replay-safe marker with no pending ops" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_empty_marker_#{System.unique_integer([:positive])}"
          )

        shard_index = 3
        instance_name = :"flow_lmdb_writer_empty_marker_#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        atomics_size = shard_index + 1
        durable = :atomics.new(atomics_size, signed: false)
        requested = :atomics.new(atomics_size, signed: false)
        failures = :atomics.new(atomics_size, signed: false)

        instance_ctx = %{
          name: instance_name,
          flow_lmdb_replay_safe_index: durable,
          flow_lmdb_replay_safe_requested_index: requested,
          flow_lmdb_replay_safe_persist_failures: failures
        }

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          File.rm_rf!(data_dir)
        end)

        File.mkdir_p!(shard_data_path)

        pid =
          start_supervised!(
            {Ferricstore.Flow.LMDBWriter,
             shard_index: shard_index,
             data_dir: data_dir,
             instance_ctx: instance_ctx,
             instance_name: instance_name}
          )

        assert :requested =
                 Ferricstore.Flow.LMDBWriter.request(
                   instance_ctx,
                   shard_index,
                   shard_data_path,
                   321
                 )

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert Process.alive?(pid)
        assert :atomics.get(durable, shard_index + 1) == 321
        assert Ferricstore.Flow.LMDBReplaySafeIndex.read(shard_data_path) == 321
      end

      test "mirror writer maintains terminal counts and TTL index atomically" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_terminal_#{System.unique_integer([:positive])}"
          )

        shard_index = 7
        instance_name = :"flow_lmdb_writer_terminal_#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        state_index_key = Ferricstore.Flow.Keys.state_index_key("kind", "completed", "tenant")
        terminal_key = Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, "flow-a", 10)
        state_key = Ferricstore.Flow.Keys.state_key("flow-a", "tenant")
        reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)
        count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)
        expire_at_ms = System.os_time(:millisecond) + 60_000
        expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)

        value =
          Ferricstore.Flow.LMDB.encode_terminal_index_value(
            "flow-a",
            10,
            expire_at_ms,
            state_key,
            count_key
          )

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          File.rm_rf!(data_dir)
        end)

        File.mkdir_p!(shard_data_path)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
        )

        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:terminal_put, terminal_key, value, state_key, count_key},
                   {:terminal_put, terminal_key, value, state_key, count_key}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

        assert {:ok, ^value} = Ferricstore.Flow.LMDB.get(path, terminal_key)
        assert {:ok, ^terminal_key} = Ferricstore.Flow.LMDB.get(path, reverse_key)
        assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(path, state_index_key)
        assert {:ok, _expire_value} = Ferricstore.Flow.LMDB.get(path, expire_key)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:terminal_delete, terminal_key, state_key, count_key}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

        assert :not_found = Ferricstore.Flow.LMDB.get(path, terminal_key)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, reverse_key)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, expire_key)
        assert {:ok, 0} = Ferricstore.Flow.LMDB.terminal_count(path, state_index_key)
      end

      test "terminal projection replay repairs legacy partial terminal commits" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_partial_terminal_#{System.unique_integer([:positive])}"
          )

        shard_index = 9
        instance_name = :"flow_lmdb_writer_partial_terminal_#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        state_index_key = Ferricstore.Flow.Keys.state_index_key("kind", "completed", "tenant")
        terminal_key = Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, "flow-a", 10)
        state_key = Ferricstore.Flow.Keys.state_key("flow-a", "tenant")
        count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

        value =
          Ferricstore.Flow.LMDB.encode_terminal_index_value(
            "flow-a",
            10,
            0,
            state_key,
            count_key
          )

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          File.rm_rf!(data_dir)
        end)

        File.mkdir_p!(shard_data_path)
        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(path, [
                   {:put, terminal_key, value}
                 ])

        assert {:ok, ^value} = Ferricstore.Flow.LMDB.get(path, terminal_key)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, count_key)

        {:ok, _pid} =
          Ferricstore.Flow.LMDBWriter.start_link(
            shard_index: shard_index,
            data_dir: data_dir,
            instance_name: instance_name
          )

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:terminal_put, terminal_key, value, state_key, count_key}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(path, state_index_key)
      end

      test "terminal projection flush does not expose partial chunk results" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
        old_chunk_ops = Application.get_env(:ferricstore, :flow_lmdb_flush_chunk_ops)
        old_chunk_pause = Application.get_env(:ferricstore, :flow_lmdb_flush_chunk_pause_ms)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
        Application.put_env(:ferricstore, :flow_lmdb_flush_chunk_ops, 1)
        Application.put_env(:ferricstore, :flow_lmdb_flush_chunk_pause_ms, 1_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_no_partial_terminal_#{System.unique_integer([:positive])}"
          )

        shard_index = 9

        instance_name =
          :"flow_lmdb_writer_no_partial_terminal_#{System.unique_integer([:positive])}"

        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        state_index_key = Ferricstore.Flow.Keys.state_index_key("kind", "completed", "tenant")
        terminal_key = Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, "flow-a", 10)
        state_key = Ferricstore.Flow.Keys.state_key("flow-a", "tenant")
        count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

        value =
          Ferricstore.Flow.LMDB.encode_terminal_index_value(
            "flow-a",
            10,
            0,
            state_key,
            count_key
          )

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          restore_env(:flow_lmdb_flush_chunk_ops, old_chunk_ops)
          restore_env(:flow_lmdb_flush_chunk_pause_ms, old_chunk_pause)
          File.rm_rf!(data_dir)
        end)

        File.mkdir_p!(shard_data_path)

        {:ok, _pid} =
          Ferricstore.Flow.LMDBWriter.start_link(
            shard_index: shard_index,
            data_dir: data_dir,
            instance_name: instance_name
          )

        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:terminal_put, terminal_key, value, state_key, count_key}
                 ])

        parent = self()

        {flusher, monitor} =
          spawn_monitor(fn ->
            result = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
            send(parent, {:no_partial_terminal_flush, result})
          end)

        refute wait_until_true(
                 fn ->
                   Ferricstore.Flow.LMDB.get(path, terminal_key) == {:ok, value} and
                     Ferricstore.Flow.LMDB.terminal_count(path, state_index_key) == :not_found
                 end,
                 50
               )

        assert_receive {:no_partial_terminal_flush, :ok}, 2_000
        assert_receive {:DOWN, ^monitor, :process, ^flusher, :normal}, 1_000
        assert {:ok, ^value} = Ferricstore.Flow.LMDB.get(path, terminal_key)
        assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(path, state_index_key)
      end

      test "non-terminal writer flush honors chunk sizing under an in-progress marker" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
        old_chunk_ops = Application.get_env(:ferricstore, :flow_lmdb_flush_chunk_ops)
        old_chunk_pause = Application.get_env(:ferricstore, :flow_lmdb_flush_chunk_pause_ms)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
        Application.put_env(:ferricstore, :flow_lmdb_flush_chunk_ops, 2)
        Application.put_env(:ferricstore, :flow_lmdb_flush_chunk_pause_ms, 100)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_chunked_puts_#{System.unique_integer([:positive])}"
          )

        shard_index = 9
        instance_name = :"flow_lmdb_writer_chunked_puts_#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          restore_env(:flow_lmdb_flush_chunk_ops, old_chunk_ops)
          restore_env(:flow_lmdb_flush_chunk_pause_ms, old_chunk_pause)
          File.rm_rf!(data_dir)
        end)

        File.mkdir_p!(shard_data_path)

        {:ok, _pid} =
          Ferricstore.Flow.LMDBWriter.start_link(
            shard_index: shard_index,
            data_dir: data_dir,
            instance_name: instance_name
          )

        path = Ferricstore.Flow.LMDB.path(shard_data_path)
        keys = Enum.map(1..5, &"chunked-put:#{&1}")

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(
                   instance_name,
                   shard_index,
                   Enum.map(keys, fn key -> {:put, key, "value:" <> key} end)
                 )

        parent = self()

        {flusher, monitor} =
          spawn_monitor(fn ->
            result = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
            send(parent, {:chunked_non_terminal_flush, result})
          end)

        assert wait_until_true(
                 fn ->
                   Ferricstore.Flow.LMDB.flush_in_progress?(path) and
                     Ferricstore.Flow.LMDB.get(path, hd(keys)) == {:ok, "value:" <> hd(keys)} and
                     Ferricstore.Flow.LMDB.get(path, List.last(keys)) == :not_found
                 end,
                 50
               )

        assert_receive {:chunked_non_terminal_flush, :ok}, 2_000
        assert_receive {:DOWN, ^monitor, :process, ^flusher, :normal}, 1_000
        refute Ferricstore.Flow.LMDB.flush_in_progress?(path)

        for key <- keys do
          expected = "value:" <> key
          assert {:ok, ^expected} = Ferricstore.Flow.LMDB.get(path, key)
        end
      end

      test "mirror writer maintains terminal metadata index without state reverse pointer" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_terminal_metadata_#{System.unique_integer([:positive])}"
          )

        shard_index = 8

        instance_name =
          :"flow_lmdb_writer_terminal_metadata_#{System.unique_integer([:positive])}"

        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        metadata_index_key = Ferricstore.Flow.Keys.root_index_key("root-a", "tenant")
        terminal_key = Ferricstore.Flow.LMDB.terminal_index_key(metadata_index_key, "flow-a", 10)
        count_key = Ferricstore.Flow.LMDB.terminal_count_key(metadata_index_key)
        expire_at_ms = System.os_time(:millisecond) + 60_000
        expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)

        value =
          Ferricstore.Flow.LMDB.encode_terminal_index_value(
            "flow-a",
            10,
            expire_at_ms,
            nil,
            count_key
          )

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          File.rm_rf!(data_dir)
        end)

        File.mkdir_p!(shard_data_path)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
        )

        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:terminal_put, terminal_key, value, nil, count_key},
                   {:terminal_put, terminal_key, value, nil, count_key}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

        assert {:ok, ^value} = Ferricstore.Flow.LMDB.get(path, terminal_key)
        assert {:ok, _expire_value} = Ferricstore.Flow.LMDB.get(path, expire_key)
        assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(path, metadata_index_key)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:terminal_delete, terminal_key, nil, count_key}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

        assert :not_found = Ferricstore.Flow.LMDB.get(path, terminal_key)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, expire_key)
        assert {:ok, 0} = Ferricstore.Flow.LMDB.terminal_count(path, metadata_index_key)
      end
    end
  end
end
