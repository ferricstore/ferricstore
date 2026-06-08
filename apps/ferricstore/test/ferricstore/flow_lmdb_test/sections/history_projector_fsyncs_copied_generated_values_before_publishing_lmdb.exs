defmodule Ferricstore.Flow.LMDBTest.Sections.HistoryProjectorFsyncsCopiedGeneratedValuesBeforePublishingLmdb do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Flow.LMDBTest.FlushProbeWriter

      test "history projector fsyncs copied generated values before publishing LMDB locators" do
        old_hook = Application.get_env(:ferricstore, :flow_history_projector_fsync_hook)
        parent = self()

        Application.put_env(:ferricstore, :flow_history_projector_fsync_hook, fn file_path ->
          send(parent, {:history_projector_fsync, file_path})
          :ok
        end)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_projected_value_fsync_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_projected_value_fsync_#{System.unique_integer([:positive])}"
        flow_id = "projected-value-fsync"
        history_key = Ferricstore.Flow.Keys.history_key(flow_id)
        event_id = "1"
        event_key = Ferricstore.Flow.Keys.stream_entry_key_from_history_key(history_key, event_id)
        value_ref = "f:{#{flow_id}}:v:p:#{flow_id}:1"
        payload = "projected-payload"

        on_exit(fn ->
          restore_env(:flow_history_projector_fsync_hook, old_hook)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        keydir = :ets.new(:flow_lmdb_projected_value_fsync_keydir, [:set, :public])
        :ets.insert(keydir, {value_ref, payload, 0, 0, 0, 0, byte_size(payload)})

        instance_ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency])
        }

        record = %{
          id: flow_id,
          type: "projected-value-fsync",
          state: "queued",
          version: 1,
          payload_ref: value_ref,
          created_at_ms: 1,
          updated_at_ms: 1
        }

        entry = %{
          key: event_key,
          expire_at_ms: 0,
          history_key: history_key,
          event_id: event_id,
          event_ms: 1,
          version: 1,
          value: Ferricstore.Flow.encode_history_fields(record, "created", 1),
          value_refs: [value_ref]
        }

        assert :ok =
                 Ferricstore.Flow.HistoryProjector.write_entries_sync(
                   instance_ctx,
                   shard_index,
                   shard_data_path,
                   [entry],
                   1,
                   keydir: keydir
                 )

        history_path = Ferricstore.Flow.HistoryProjector.history_file_path(shard_data_path, 0)
        assert_receive {:history_projector_fsync, ^history_path}, 500
        assert_receive {:history_projector_fsync, ^history_path}, 500

        lmdb_path = Ferricstore.Flow.LMDB.path(shard_data_path)
        assert {:ok, locator} = Ferricstore.Flow.LMDB.get(lmdb_path, value_ref)

        assert {:ok, {{:flow_history, 0}, offset, value_size}} =
                 Ferricstore.Flow.LMDB.decode_value_locator(locator, 0)

        assert {:ok, ^payload} =
                 Ferricstore.Flow.HistoryProjector.read_value(
                   shard_data_path,
                   {:flow_history, 0},
                   offset
                 )

        assert value_size == byte_size(payload)
        assert [] = :ets.lookup(keydir, value_ref)
      end

      test "flush coordinator serializes LMDB writers when configured with one permit" do
        instance_name = :"flow_lmdb_flush_coordinator_#{System.unique_integer([:positive])}"
        parent = self()

        start_supervised!(
          {Ferricstore.Flow.LMDBFlushCoordinator, instance_name: instance_name, max_concurrent: 1}
        )

        first =
          spawn(fn ->
            Ferricstore.Flow.LMDBFlushCoordinator.with_permit(instance_name, fn ->
              send(parent, :first_entered)

              receive do
                :release_first -> :ok
              end

              send(parent, :first_leaving)
            end)
          end)

        assert_receive :first_entered

        _second =
          spawn(fn ->
            Ferricstore.Flow.LMDBFlushCoordinator.with_permit(instance_name, fn ->
              send(parent, :second_entered)
            end)
          end)

        refute_receive :second_entered, 50
        send(first, :release_first)
        assert_receive :first_leaving
        assert_receive :second_entered
      end

      test "flush_all requests shard flushes concurrently instead of multiplying timeout by shard" do
        instance_name = :"flow_lmdb_flush_all_parallel_#{System.unique_integer([:positive])}"
        parent = self()

        for shard_index <- 0..1 do
          start_supervised!(
            {FlushProbeWriter,
             name: Ferricstore.Flow.LMDBWriter.name(instance_name, shard_index),
             parent: parent,
             shard_index: shard_index},
            id: {FlushProbeWriter, instance_name, shard_index}
          )
        end

        task =
          Task.async(fn -> Ferricstore.Flow.LMDBWriter.flush_all(instance_name, 2, 2_000) end)

        entered =
          for _ <- 1..2 do
            assert_receive {:flush_entered, shard_index}, 200
            shard_index
          end

        assert Enum.sort(entered) == [0, 1]

        for shard_index <- 0..1 do
          send(
            Process.whereis(Ferricstore.Flow.LMDBWriter.name(instance_name, shard_index)),
            :release_flush
          )
        end

        assert :ok = Task.await(task, 2_000)
      end

      test "suspend_all can mark projection writers suspended without another flush pass" do
        instance_name = :"flow_lmdb_suspend_no_flush_#{System.unique_integer([:positive])}"
        parent = self()

        start_supervised!(
          {FlushProbeWriter,
           name: Ferricstore.Flow.LMDBWriter.name(instance_name, 0),
           parent: parent,
           shard_index: 0}
        )

        assert :ok = Ferricstore.Flow.LMDBWriter.suspend_all(instance_name, 1, flush: false)

        assert_receive {:suspend_without_flush, 0}, 200
        refute_receive {:flush_entered, 0}, 50
      end

      test "resume_all clears a no-flush suspend for test and restart cleanup" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_resume_all_#{System.unique_integer([:positive])}"
          )

        instance_name = :"flow_lmdb_resume_all_#{System.unique_integer([:positive])}"
        shard_index = 0
        key = "flow:{flow:resume}:state:a"

        on_exit(fn -> File.rm_rf!(data_dir) end)
        Ferricstore.DataDir.ensure_layout!(data_dir, 1)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
        )

        assert :ok = Ferricstore.Flow.LMDBWriter.suspend_all(instance_name, 1, flush: false)

        assert {:error, :writer_suspended} =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, key, "v1"}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.resume_all(instance_name, 1)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, key, "v1"}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
      end

      test "LMDB writer projects Flow state from provided value without source keydir read" do
        fixture = start_active_lmdb_projection_fixture!("direct-state")

        record =
          active_lmdb_record("flow-direct-state", "direct-state", "queued",
            partition_key: fixture.partition_key,
            updated_at_ms: 10,
            next_run_at_ms: 20
          )

        state_key = Ferricstore.Flow.Keys.state_key(record.id, record.partition_key)
        encoded = Ferricstore.Flow.encode_record(record)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(fixture.instance_name, fixture.shard_index, [
                   {:project_flow_state, state_key, encoded, 0}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(fixture.instance_name, fixture.shard_index)

        assert {:ok, wrapped} = Ferricstore.Flow.LMDB.get(fixture.lmdb_path, state_key)
        assert {:ok, ^encoded} = Ferricstore.Flow.LMDB.decode_value(wrapped, 0)

        queued_state_key =
          Ferricstore.Flow.Keys.state_index_key(record.type, record.state, record.partition_key)

        assert {:ok, 1} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.active_index_prefix(queued_state_key)
                 )
      end

      test "terminal Flow state projection removes stale active LMDB rows" do
        fixture = start_active_lmdb_projection_fixture!("terminal-fast-state")

        active =
          active_lmdb_record("flow-terminal-fast-state", "terminal-fast-state", "queued",
            partition_key: fixture.partition_key,
            updated_at_ms: 10,
            next_run_at_ms: 20
          )

        state_key = Ferricstore.Flow.Keys.state_key(active.id, active.partition_key)

        {active_ops, active_reverse} =
          Ferricstore.Flow.LMDB.active_index_put_ops_with_reverse(state_key, active, 0)

        assert :ok = Ferricstore.Flow.LMDB.write_batch(fixture.lmdb_path, active_ops)

        assert {:ok, ^active_reverse} =
                 Ferricstore.Flow.LMDB.get(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.active_by_state_key_key(state_key)
                 )

        completed = %{active | state: "completed", version: 2, updated_at_ms: 30}
        encoded_completed = Ferricstore.Flow.encode_record(completed)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(fixture.instance_name, fixture.shard_index, [
                   {:project_flow_state, state_key, encoded_completed, 0}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(fixture.instance_name, fixture.shard_index)

        assert {:ok, wrapped} = Ferricstore.Flow.LMDB.get(fixture.lmdb_path, state_key)
        assert {:ok, ^encoded_completed} = Ferricstore.Flow.LMDB.decode_value(wrapped, 0)

        completed_state_key =
          Ferricstore.Flow.Keys.state_index_key(
            completed.type,
            completed.state,
            completed.partition_key
          )

        assert {:ok, 1} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.terminal_index_prefix(completed_state_key)
                 )

        queued_state_key =
          Ferricstore.Flow.Keys.state_index_key(active.type, active.state, active.partition_key)

        assert {:ok, 0} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.active_index_prefix(queued_state_key)
                 )

        assert :not_found =
                 Ferricstore.Flow.LMDB.get(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.active_by_state_key_key(state_key)
                 )
      end

      test "LMDB writer drains terminal Flow projection outbox from durable source" do
        fixture = start_active_lmdb_projection_fixture!("terminal-outbox")

        completed =
          active_lmdb_record("flow-terminal-outbox", "terminal-outbox", "completed",
            partition_key: fixture.partition_key,
            updated_at_ms: 30,
            next_run_at_ms: nil,
            version: 7
          )

        state_key = Ferricstore.Flow.Keys.state_key(completed.id, completed.partition_key)
        encoded = Ferricstore.Flow.encode_record(completed)

        :ets.insert(
          fixture.source_keydir,
          {state_key, encoded, 0, {:flow_state_version, completed.version, 0}, :hot, 0,
           byte_size(encoded)}
        )

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue_projection_outbox(
                   fixture.instance_name,
                   fixture.shard_index,
                   [{state_key, completed.version}]
                 )

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(fixture.instance_name, fixture.shard_index)

        assert {:ok, wrapped} = Ferricstore.Flow.LMDB.get(fixture.lmdb_path, state_key)
        assert {:ok, ^encoded} = Ferricstore.Flow.LMDB.decode_value(wrapped, 0)

        completed_state_key =
          Ferricstore.Flow.Keys.state_index_key(
            completed.type,
            completed.state,
            completed.partition_key
          )

        assert {:ok, 1} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.terminal_index_prefix(completed_state_key)
                 )
      end

      test "lagged LMDB projector reconciles dirty shard from durable Flow state" do
        fixture = start_active_lmdb_projection_fixture!("lagged-dirty")

        completed =
          active_lmdb_record("flow-lagged-dirty", "lagged-dirty", "completed",
            partition_key: fixture.partition_key,
            updated_at_ms: 40,
            next_run_at_ms: nil,
            version: 9
          )

        state_key = Ferricstore.Flow.Keys.state_key(completed.id, completed.partition_key)
        encoded = Ferricstore.Flow.encode_record(completed)

        :ets.insert(
          fixture.source_keydir,
          {state_key, encoded, 0, {:flow_state_version, completed.version, 0}, :hot, 0,
           byte_size(encoded)}
        )

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.mark_projection_dirty(
                   fixture.instance_name,
                   fixture.shard_index
                 )

        writer = Process.whereis(Ferricstore.Flow.LMDBWriter.name(fixture.instance_name, 0))

        assert wait_until_true(
                 fn ->
                   %{projection_dirty?: dirty?} = :sys.get_state(writer)
                   dirty?
                 end,
                 20
               )

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(fixture.instance_name, fixture.shard_index)

        assert {:ok, wrapped} = Ferricstore.Flow.LMDB.get(fixture.lmdb_path, state_key)
        assert {:ok, ^encoded} = Ferricstore.Flow.LMDB.decode_value(wrapped, 0)

        completed_state_key =
          Ferricstore.Flow.Keys.state_index_key(
            completed.type,
            completed.state,
            completed.partition_key
          )

        assert {:ok, 1} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   fixture.lmdb_path,
                   Ferricstore.Flow.LMDB.terminal_index_prefix(completed_state_key)
                 )

        assert wait_until_true(fn -> :ets.lookup(fixture.source_keydir, state_key) == [] end, 20)
      end

      test "empty rebuild does not open LMDB" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_empty_rebuild_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        keydir = :ets.new(:flow_lmdb_empty_rebuild_keydir, [:set])

        on_exit(fn ->
          if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

        refute File.exists?(lmdb_path)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
                   shard_path,
                   keydir,
                   shard_index,
                   nil,
                   nil,
                   nil,
                   nil,
                   nil
                 )

        refute File.exists?(lmdb_path)
      end

      test "reconcile clears cold-read process state when rebuild fails early" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_reconcile_cleanup_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        keydir = :ets.new(:flow_lmdb_reconcile_cleanup_keydir, [:set])

        on_exit(fn ->
          Process.delete(:flow_lmdb_rebuild_cold_read_errors)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        :ets.delete(keydir)
        Process.put(:flow_lmdb_rebuild_cold_read_errors, 123)

        _ =
          Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
            shard_path,
            keydir,
            shard_index,
            nil,
            nil,
            nil,
            nil,
            nil
          )

        assert Process.get(:flow_lmdb_rebuild_cold_read_errors) == nil
      end

      test "active index rebuild clears cold-read process state when rebuild fails early" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_active_cleanup_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        keydir = :ets.new(:flow_lmdb_active_cleanup_keydir, [:set])

        on_exit(fn ->
          Process.delete(:flow_lmdb_rebuild_cold_read_errors)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        :ets.delete(keydir)
        Process.put(:flow_lmdb_rebuild_cold_read_errors, 123)

        _ =
          Ferricstore.Flow.LMDBRebuilder.rebuild_active_indexes_from_keydir(
            shard_path,
            keydir,
            shard_index,
            nil,
            nil,
            nil,
            nil,
            nil
          )

        assert Process.get(:flow_lmdb_rebuild_cold_read_errors) == nil
      end

      test "startup reconcile counts LMDB history projection write failures" do
        old_async_history = Application.get_env(:ferricstore, :flow_async_history)
        old_hook = Application.get_env(:ferricstore, :flow_lmdb_rebuild_history_write_hook)

        Application.put_env(:ferricstore, :flow_async_history, false)

        test_pid = self()

        Application.put_env(:ferricstore, :flow_lmdb_rebuild_history_write_hook, fn _path, ops ->
          send(test_pid, {:history_projection_rebuild_write, length(ops)})
          {:error, :injected_history_projection_failure}
        end)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_history_rebuild_failure_#{System.unique_integer([:positive])}"
          )

        instance_name = :"flow_lmdb_history_rebuild_failure_#{System.unique_integer([:positive])}"
        shard_index = 0
        keydir = :ets.new(:flow_lmdb_history_rebuild_failure_keydir, [:set])
        handler_id = {:flow_lmdb_history_rebuild_failure, self(), make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :flow, :lmdb_rebuild],
          &__MODULE__.forward_flow_lmdb_rebuild_event/4,
          test_pid
        )

        on_exit(fn ->
          restore_env(:flow_async_history, old_async_history)
          restore_env(:flow_lmdb_rebuild_history_write_hook, old_hook)
          :telemetry.detach(handler_id)
          if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)

        record = %{
          id: "history-rebuild-failure",
          type: "startup-history-rebuild",
          state: "completed",
          version: 2,
          attempts: 1,
          fencing_token: 1,
          created_at_ms: 1_000,
          updated_at_ms: 2_000,
          terminal_retention_until_ms: 60_000,
          partition_key: "tenant-history-rebuild",
          root_flow_id: "history-rebuild-failure"
        }

        state_key = Ferricstore.Flow.Keys.state_key(record.id, record.partition_key)
        encoded = Ferricstore.Flow.encode_record(record)
        :ets.insert(keydir, {state_key, encoded, 0, 0, :hot, 0, byte_size(encoded)})

        event_id = "2000-1"
        history_key = Ferricstore.Flow.Keys.history_key(record.id, record.partition_key)
        event_key = Ferricstore.Flow.Keys.stream_entry_key_from_history_key(history_key, event_id)
        event_value = Ferricstore.Flow.encode_history_fields(record, "completed", 2_000)
        :ets.insert(keydir, {event_key, event_value, 0, 0, :hot, 0, byte_size(event_value)})

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(instance_name, shard_index)

        Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
                   shard_path,
                   keydir,
                   shard_index,
                   nil,
                   nil,
                   nil,
                   flow_index,
                   flow_lookup
                 )

        assert_receive {:history_projection_rebuild_write, count} when count > 0

        assert_receive {:flow_lmdb_rebuild, [:ferricstore, :flow, :lmdb_rebuild],
                        %{history: 1, history_lmdb_errors: 1, lmdb_errors: 1}, %{shard_index: 0}}
      end

      test "startup reconcile writes one history flow expiry marker per flow" do
        old_async_history = Application.get_env(:ferricstore, :flow_async_history)
        old_hook = Application.get_env(:ferricstore, :flow_lmdb_rebuild_history_write_hook)

        Application.put_env(:ferricstore, :flow_async_history, false)

        test_pid = self()

        Application.put_env(:ferricstore, :flow_lmdb_rebuild_history_write_hook, fn _path, ops ->
          send(test_pid, {:history_projection_rebuild_ops, ops})
          :ok
        end)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_history_rebuild_expire_#{System.unique_integer([:positive])}"
          )

        instance_name = :"flow_lmdb_history_rebuild_expire_#{System.unique_integer([:positive])}"
        shard_index = 0
        keydir = :ets.new(:flow_lmdb_history_rebuild_expire_keydir, [:set])

        on_exit(fn ->
          restore_env(:flow_async_history, old_async_history)
          restore_env(:flow_lmdb_rebuild_history_write_hook, old_hook)
          if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)

        record = %{
          id: "history-rebuild-expire",
          type: "startup-history-rebuild",
          state: "completed",
          version: 3,
          attempts: 1,
          fencing_token: 1,
          created_at_ms: 1_000,
          updated_at_ms: 3_000,
          terminal_retention_until_ms: 60_000,
          partition_key: "tenant-history-rebuild",
          root_flow_id: "history-rebuild-expire",
          history_max_events: 10
        }

        state_key = Ferricstore.Flow.Keys.state_key(record.id, record.partition_key)
        encoded = Ferricstore.Flow.encode_record(record)
        :ets.insert(keydir, {state_key, encoded, 0, 0, :hot, 0, byte_size(encoded)})

        history_key = Ferricstore.Flow.Keys.history_key(record.id, record.partition_key)

        for {event_id, version, event_ms} <- [{"1000-1", 1, 1_000}, {"2000-2", 2, 2_000}] do
          event_key =
            Ferricstore.Flow.Keys.stream_entry_key_from_history_key(history_key, event_id)

          event_value =
            Ferricstore.Flow.encode_history_fields(
              %{record | version: version},
              "queued",
              event_ms
            )

          :ets.insert(keydir, {event_key, event_value, 0, 0, :hot, 0, byte_size(event_value)})
        end

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(instance_name, shard_index)

        Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
                   shard_path,
                   keydir,
                   shard_index,
                   nil,
                   nil,
                   nil,
                   flow_index,
                   flow_lookup
                 )

        expire_key =
          Ferricstore.Flow.LMDB.history_flow_expire_key(
            record.terminal_retention_until_ms,
            history_key
          )

        assert_receive {:history_projection_rebuild_ops, ops}

        assert 1 ==
                 Enum.count(ops, fn
                   {:put, ^expire_key, _value} -> true
                   _ -> false
                 end)
      end

      test "startup reconcile rebuilds Flow state from WARaft segment-backed keydir rows" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_waraft_rebuild_#{System.unique_integer([:positive])}"
          )

        instance_name = :"flow_lmdb_waraft_rebuild_#{System.unique_integer([:positive])}"
        shard_index = 0
        index = 12_321
        keydir = :ets.new(:flow_lmdb_waraft_rebuild_keydir, [:set])
        degraded = :atomics.new(1, signed: false)

        on_exit(fn ->
          Ferricstore.Raft.WARaftSegmentReader.clear_apply_projection_cache(data_dir, shard_index)
          if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

        record = %{
          id: "flow-waraft-rebuild",
          type: "startup-waraft",
          state: "queued",
          version: 1,
          attempts: 0,
          fencing_token: 0,
          created_at_ms: 1,
          updated_at_ms: 2,
          next_run_at_ms: 10,
          priority: 0,
          partition_key: "tenant-waraft",
          root_flow_id: "flow-waraft-rebuild"
        }

        state_key = Ferricstore.Flow.Keys.state_key(record.id, record.partition_key)
        encoded = Ferricstore.Flow.encode_record(record)
        file_id = {:waraft_apply_projection, index}

        assert :ok =
                 Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                   data_dir,
                   shard_index,
                   index,
                   [{state_key, encoded, 0}]
                 )

        :ets.insert(keydir, {state_key, nil, 0, 0, file_id, 0, byte_size(encoded)})

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(instance_name, shard_index)

        Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
                   shard_path,
                   keydir,
                   shard_index,
                   %{
                     data_dir: data_dir,
                     flow_lmdb_mirror_degraded: degraded
                   },
                   nil,
                   nil,
                   flow_index,
                   flow_lookup
                 )

        assert :atomics.get(degraded, 1) == 0
        assert {:ok, lmdb_blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
        assert {:ok, ^encoded} = Ferricstore.Flow.LMDB.decode_value(lmdb_blob, 10)

        state_index_key =
          Ferricstore.Flow.Keys.state_index_key(record.type, record.state, record.partition_key)

        assert [{record.id, 2.0}] ==
                 Ferricstore.Flow.OrderedIndex.range_slice(
                   flow_index,
                   state_index_key,
                   :neg_inf,
                   :inf,
                   false,
                   0,
                   :all
                 )
      end
    end
  end
end
