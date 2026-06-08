defmodule Ferricstore.Flow.HistoryProjectorTest.Sections.SyncProjectionWritesDedicatedHistoryLogUpdatesIndexAdvancesWate do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.NativeOrderedIndex
      alias Ferricstore.Flow.OrderedIndex

      test "sync projection writes dedicated history log, updates index, and advances watermark" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_test_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_#{unique}")
        hot_path = Path.join(dir, "00000.log")

        File.mkdir_p!(dir)
        File.touch!(hot_path)

        keydir = :ets.new(:"history_projector_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        on_exit(fn ->
          File.rm_rf(dir)
        end)

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        history_key = Ferricstore.Flow.Keys.history_key("flow-1")
        event_id = "1000-1"
        key = Ferricstore.Flow.Keys.stream_entry_key("flow-1", event_id, nil)

        record = %{
          id: "flow-1",
          type: "audit",
          state: "queued",
          version: 1,
          partition_key: nil,
          priority: 0,
          attempts: 0,
          next_run_at_ms: 1_000,
          created_at_ms: 1_000,
          updated_at_ms: 1_000,
          lease_owner: nil,
          lease_deadline_ms: 0
        }

        entry = %{
          key: key,
          expire_at_ms: 0,
          history_key: history_key,
          event_id: event_id,
          event_ms: 1_000,
          version: 1,
          snapshot: Ferricstore.Flow.history_snapshot(record, "created", 1_000, %{}),
          ra_index: 42
        }

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 42)

        assert [{^key, nil, 0, _lfu, {:flow_history, 0}, offset, value_size}] =
                 :ets.lookup(keydir, key)

        assert value_size > 0
        history_path = HistoryProjector.history_file_path(dir, 0)
        assert {:ok, value} = NIF.v2_pread_at(history_path, offset)
        assert {:ok, ^value} = HistoryProjector.read_value(dir, {:flow_history, 0}, offset)
        assert {:ok, ^value} = HistoryProjector.scan_event_value(dir, key)
        assert File.stat!(hot_path).size == 0
        assert OrderedIndex.count_all(flow_lookup, history_key) == 1

        assert OrderedIndex.rank_range(flow_index, history_key, 0, 1, false) == [
                 {event_id, 1000.0}
               ]

        assert HistoryProjector.durable?(ctx, 0, dir, 42)
      end

      test "request publishes requested index and pending backlog metrics without flushing hot path" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_metrics_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_metrics_#{unique}")

        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_metrics_keydir_#{unique}", [:set, :public])

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_requested_index: :atomics.new(1, signed: false),
          flow_history_projector_pending_entries: :atomics.new(1, signed: false),
          flow_history_projector_oldest_pending_age_us: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false),
          flow_history_projector_queue_full: :atomics.new(1, signed: false)
        }

        {:ok, pid} =
          HistoryProjector.start_link(
            shard_index: 0,
            shard_data_path: dir,
            instance_ctx: ctx,
            recover_on_init: false
          )

        on_exit(fn ->
          if Process.alive?(pid), do: GenServer.stop(pid)
          File.rm_rf(dir)
        end)

        entry = %{
          key: Ferricstore.Flow.Keys.stream_entry_key("flow-metrics", "1000-1", nil),
          expire_at_ms: 0,
          history_key: Ferricstore.Flow.Keys.history_key("flow-metrics"),
          event_id: "1000-1",
          event_ms: 1_000,
          version: 1,
          value: "history-metrics",
          ra_index: 42
        }

        assert :requested = HistoryProjector.request(ctx, 0, dir, 42)
        assert :ok = HistoryProjector.enqueue(ctx, 0, [entry], 42)

        assert :atomics.get(ctx.flow_history_requested_index, 1) == 42
        assert :atomics.get(ctx.flow_history_projector_pending_entries, 1) == 1
        assert :atomics.get(ctx.flow_history_projector_oldest_pending_age_us, 1) >= 0
      end

      test "projection fsyncs history log before publishing LMDB locations without watermark" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_fsync_order_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_fsync_order_#{unique}")
        test_pid = self()

        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_fsync_order_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        old_fsync_hook = Application.get_env(:ferricstore, :flow_history_projector_fsync_hook)

        old_lmdb_hook =
          Application.get_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)

        Application.put_env(:ferricstore, :flow_history_projector_fsync_hook, fn path ->
          send(test_pid, {:history_fsync, path})
          :ok
        end)

        Application.put_env(
          :ferricstore,
          :flow_history_projector_lmdb_publish_hook,
          fn _shard_path, _file_id, _entries ->
            send(test_pid, :history_lmdb_publish)
            :ok
          end
        )

        on_exit(fn ->
          restore_env(:flow_history_projector_fsync_hook, old_fsync_hook)
          restore_env(:flow_history_projector_lmdb_publish_hook, old_lmdb_hook)
          File.rm_rf(dir)
        end)

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        history_key = Ferricstore.Flow.Keys.history_key("flow-fsync-order")
        event_id = "1000-1"

        entry = %{
          key: Ferricstore.Flow.Keys.stream_entry_key("flow-fsync-order", event_id, nil),
          expire_at_ms: 0,
          history_key: history_key,
          event_id: event_id,
          event_ms: 1_000,
          version: 1,
          value: "history-fsync-order",
          ra_index: 42
        }

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], nil)

        history_path = HistoryProjector.history_file_path(dir, 0)
        assert_receive {:history_fsync, ^history_path}, 500
        assert_receive :history_lmdb_publish, 500
      end

      test "sync projection does not publish hot keydir or native index when LMDB publish fails" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_lmdb_publish_fail_#{unique}"

        dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_history_projector_lmdb_publish_fail_#{unique}"
          )

        File.mkdir_p!(dir)

        keydir =
          :ets.new(:"history_projector_lmdb_publish_fail_keydir_#{unique}", [:set, :public])

        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        old_lmdb_hook =
          Application.get_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)

        Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, fn _path,
                                                                                        _file_id,
                                                                                        _entries ->
          {:error, :boom}
        end)

        on_exit(fn ->
          restore_env(:flow_history_projector_lmdb_publish_hook, old_lmdb_hook)
          File.rm_rf(dir)
        end)

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        history_key = Ferricstore.Flow.Keys.history_key("flow-lmdb-publish-fail")
        event_id = "1000-1"
        key = Ferricstore.Flow.Keys.stream_entry_key("flow-lmdb-publish-fail", event_id, nil)

        entry = %{
          key: key,
          expire_at_ms: 0,
          history_key: history_key,
          event_id: event_id,
          event_ms: 1_000,
          version: 1,
          value: "history-lmdb-publish-fail",
          ra_index: 42
        }

        assert {:error, :boom} = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 42)
        assert [] = :ets.lookup(keydir, key)
        assert OrderedIndex.count_all(flow_lookup, history_key) == 0
        assert OrderedIndex.rank_range(flow_index, history_key, 0, 1, false) == []
        refute HistoryProjector.durable?(ctx, 0, dir, 42)
      end

      test "hard history cap fsyncs tombstones before removing old LMDB locations" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_cap_tombstone_fsync_#{unique}"

        dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_history_projector_cap_tombstone_fsync_#{unique}"
          )

        test_pid = self()

        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_cap_tombstone_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        old_fsync_hook = Application.get_env(:ferricstore, :flow_history_projector_fsync_hook)

        Application.put_env(:ferricstore, :flow_history_projector_fsync_hook, fn path ->
          send(test_pid, {:history_fsync, path})
          :ok
        end)

        on_exit(fn ->
          restore_env(:flow_history_projector_fsync_hook, old_fsync_hook)
          File.rm_rf(dir)
        end)

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          data_dir: Path.dirname(dir),
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        history_key = Ferricstore.Flow.Keys.history_key("flow-hard-cap-fsync")

        entries =
          for version <- 1..2 do
            event_ms = 1_000 + version
            event_id = "#{event_ms}-#{version}"

            %{
              key: Ferricstore.Flow.Keys.stream_entry_key("flow-hard-cap-fsync", event_id, nil),
              expire_at_ms: 0,
              history_key: history_key,
              event_id: event_id,
              event_ms: event_ms,
              version: version,
              value: "history-hard-cap-#{version}",
              history_max_events: 1,
              history_hot_max_events: 1,
              ra_index: 50 + version
            }
          end

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, entries, nil)

        history_path = HistoryProjector.history_file_path(dir, 0)
        assert_receive {:history_fsync, ^history_path}, 500

        old_lmdb_key = Ferricstore.Flow.LMDB.history_index_key(history_key, "1001-1", 1001)

        assert :not_found =
                 Ferricstore.Flow.LMDB.get(Ferricstore.Flow.LMDB.path(dir), old_lmdb_key)
      end

      test "sync projection evicts old hot history only after LMDB stores direct cold locations" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_hot_cap_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_hot_cap_#{unique}")
        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_hot_cap_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        on_exit(fn ->
          File.rm_rf(dir)
        end)

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          data_dir: Path.dirname(dir),
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        history_key = Ferricstore.Flow.Keys.history_key("flow-hot-cap")

        entries =
          for version <- 1..3 do
            event_ms = 1_000 + version
            event_id = "#{event_ms}-#{version}"

            %{
              key: Ferricstore.Flow.Keys.stream_entry_key("flow-hot-cap", event_id, nil),
              expire_at_ms: 0,
              history_key: history_key,
              event_id: event_id,
              event_ms: event_ms,
              version: version,
              value: String.duplicate("history-#{version}", 16),
              history_hot_max_events: 1,
              ra_index: 100 + version
            }
          end

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, entries, 103)

        hot_key = Ferricstore.Flow.Keys.stream_entry_key("flow-hot-cap", "1003-3", nil)
        cold_key = Ferricstore.Flow.Keys.stream_entry_key("flow-hot-cap", "1001-1", nil)

        assert [{^hot_key, nil, 0, _lfu, {:flow_history, 0}, _offset, _size}] =
                 :ets.lookup(keydir, hot_key)

        assert [] = :ets.lookup(keydir, cold_key)
        assert OrderedIndex.count_all(flow_lookup, history_key) == 1

        assert :atomics.get(ctx.keydir_binary_bytes, 1) == 0

        lmdb_path = Ferricstore.Flow.LMDB.path(dir)
        lmdb_key = Ferricstore.Flow.LMDB.history_index_key(history_key, "1001-1", 1001)

        assert {:ok, lmdb_value} = Ferricstore.Flow.LMDB.get(lmdb_path, lmdb_key)

        assert {:ok,
                {_event_id, _event_ms, _expire_at_ms, _compound_key, {:flow_history, 0}, offset,
                 value_size}} =
                 Ferricstore.Flow.LMDB.decode_history_index_location(lmdb_value)

        assert value_size > 0
        assert {:ok, value} = HistoryProjector.read_value(dir, {:flow_history, 0}, offset)
        assert value == String.duplicate("history-1", 16)
        assert HistoryProjector.durable?(ctx, 0, dir, 103)
      end

      test "sync projection with zero hot history evicts all hot rows after LMDB stores locations" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_zero_hot_cap_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_zero_hot_cap_#{unique}")
        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_zero_hot_cap_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        on_exit(fn ->
          File.rm_rf(dir)
        end)

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          data_dir: Path.dirname(dir),
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        history_key = Ferricstore.Flow.Keys.history_key("flow-zero-hot")

        entries =
          for version <- 1..2 do
            event_ms = 2_000 + version
            event_id = "#{event_ms}-#{version}"

            %{
              key: Ferricstore.Flow.Keys.stream_entry_key("flow-zero-hot", event_id, nil),
              expire_at_ms: 0,
              history_key: history_key,
              event_id: event_id,
              event_ms: event_ms,
              version: version,
              value: "history-zero-hot-#{version}",
              history_hot_max_events: 0,
              ra_index: 200 + version
            }
          end

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, entries, 202)

        for entry <- entries do
          assert [] = :ets.lookup(keydir, entry.key)

          lmdb_path = Ferricstore.Flow.LMDB.path(dir)

          lmdb_key =
            Ferricstore.Flow.LMDB.history_index_key(history_key, entry.event_id, entry.event_ms)

          assert {:ok, lmdb_value} = Ferricstore.Flow.LMDB.get(lmdb_path, lmdb_key)

          assert {:ok,
                  {_event_id, _event_ms, _expire_at_ms, _compound_key, {:flow_history, 0}, offset,
                   _value_size}} =
                   Ferricstore.Flow.LMDB.decode_history_index_location(lmdb_value)

          assert {:ok, value} = HistoryProjector.read_value(dir, {:flow_history, 0}, offset)
          assert value == entry.value
        end

        assert OrderedIndex.count_all(flow_lookup, history_key) == 0
        assert HistoryProjector.durable?(ctx, 0, dir, 202)
      end

      test "recover with zero hot history uses existing LMDB projection without rebuilding hot history index" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_fast_recover_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_fast_recover_#{unique}")
        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_fast_recover_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        previous_hot = Application.get_env(:ferricstore, :flow_default_history_hot_max_events)
        Application.put_env(:ferricstore, :flow_default_history_hot_max_events, 0)

        on_exit(fn ->
          restore_env(:flow_default_history_hot_max_events, previous_hot)
          File.rm_rf(dir)
        end)

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          data_dir: Path.dirname(dir),
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        history_key = Ferricstore.Flow.Keys.history_key("flow-fast-recover")
        event_id = "3001-1"

        entry = %{
          key: Ferricstore.Flow.Keys.stream_entry_key("flow-fast-recover", event_id, nil),
          expire_at_ms: 0,
          history_key: history_key,
          event_id: event_id,
          event_ms: 3_001,
          version: 1,
          value: "history-fast-recover",
          history_hot_max_events: 0,
          ra_index: 301
        }

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 301)
        assert HistoryProjector.durable?(ctx, 0, dir, 301)

        NativeOrderedIndex.reset(flow_index, flow_lookup)
        :ets.delete_all_objects(keydir)
        :atomics.put(ctx.flow_history_projected_index, 1, 0)

        assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)
        assert HistoryProjector.durable?(ctx, 0, dir, 301)
        assert OrderedIndex.count_all(flow_lookup, history_key) == 0

        lmdb_key = Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, 3_001)

        assert {:ok, lmdb_value} =
                 Ferricstore.Flow.LMDB.get(Ferricstore.Flow.LMDB.path(dir), lmdb_key)

        assert {:ok, {_event_id, 3_001, _expire, _compound, {:flow_history, 0}, _offset, _size}} =
                 Ferricstore.Flow.LMDB.decode_history_index_location(lmdb_value)
      end

      test "recover with zero hot history scans log when projected marker is behind LMDB" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_behind_recover_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_behind_#{unique}")
        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_behind_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        previous_hot = Application.get_env(:ferricstore, :flow_default_history_hot_max_events)
        Application.put_env(:ferricstore, :flow_default_history_hot_max_events, 0)

        on_exit(fn ->
          restore_env(:flow_default_history_hot_max_events, previous_hot)
          File.rm_rf(dir)
        end)

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          data_dir: Path.dirname(dir),
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        history_key = Ferricstore.Flow.Keys.history_key("flow-behind-recover")

        projected_entry = %{
          key: Ferricstore.Flow.Keys.stream_entry_key("flow-behind-recover", "3003-1", nil),
          expire_at_ms: 0,
          history_key: history_key,
          event_id: "3003-1",
          event_ms: 3_003,
          version: 1,
          value: "history-already-projected",
          history_hot_max_events: 0,
          ra_index: 301
        }

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [projected_entry], 301)
        assert HistoryProjector.durable?(ctx, 0, dir, 301)

        unprojected_event_id = "3004-2"

        unprojected_key =
          Ferricstore.Flow.Keys.stream_entry_key("flow-behind-recover", unprojected_event_id, nil)

        history_path = HistoryProjector.history_file_path(dir, 0)

        assert {:ok, [_location]} =
                 NIF.v2_append_batch_nosync(history_path, [
                   {unprojected_key, "history-needs-recovery", 0}
                 ])

        lmdb_key =
          Ferricstore.Flow.LMDB.history_index_key(history_key, unprojected_event_id, 3_004)

        assert :not_found == Ferricstore.Flow.LMDB.get(Ferricstore.Flow.LMDB.path(dir), lmdb_key)

        NativeOrderedIndex.reset(flow_index, flow_lookup)
        :ets.delete_all_objects(keydir)
        :atomics.put(ctx.flow_history_projected_index, 1, 0)

        assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)

        assert {:ok, lmdb_value} =
                 Ferricstore.Flow.LMDB.get(Ferricstore.Flow.LMDB.path(dir), lmdb_key)

        assert {:ok,
                {^unprojected_event_id, 3_004, _expire, _compound, {:flow_history, 0}, _offset,
                 _size}} =
                 Ferricstore.Flow.LMDB.decode_history_index_location(lmdb_value)
      end

      test "recover with zero hot history trusts existing LMDB projection when projected marker is missing" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_missing_marker_recover_#{unique}"

        dir =
          Path.join(System.tmp_dir!(), "ferricstore_history_projector_missing_marker_#{unique}")

        File.mkdir_p!(dir)

        keydir = :ets.new(:"history_projector_missing_marker_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        previous_hot = Application.get_env(:ferricstore, :flow_default_history_hot_max_events)
        Application.put_env(:ferricstore, :flow_default_history_hot_max_events, 0)

        on_exit(fn ->
          restore_env(:flow_default_history_hot_max_events, previous_hot)
          File.rm_rf(dir)
        end)

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          data_dir: Path.dirname(dir),
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        history_key = Ferricstore.Flow.Keys.history_key("flow-missing-marker-recover")
        event_id = "3002-1"

        entry = %{
          key:
            Ferricstore.Flow.Keys.stream_entry_key("flow-missing-marker-recover", event_id, nil),
          expire_at_ms: 0,
          history_key: history_key,
          event_id: event_id,
          event_ms: 3_002,
          version: 1,
          value: "history-missing-marker-recover",
          history_hot_max_events: 0,
          ra_index: 302
        }

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 302)

        history_path = HistoryProjector.history_file_path(dir, 0)
        File.rm!(Ferricstore.Flow.HistoryProjectedIndex.path(dir))
        File.rm!(history_path)
        File.mkdir!(history_path)
        NativeOrderedIndex.reset(flow_index, flow_lookup)
        :ets.delete_all_objects(keydir)
        :atomics.put(ctx.flow_history_projected_index, 1, 0)

        assert HistoryProjector.__skip_history_log_recover_for_test__(dir, 0)
        assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)
        assert OrderedIndex.count_all(flow_lookup, history_key) == 0

        lmdb_key = Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, 3_002)

        assert {:ok, lmdb_value} =
                 Ferricstore.Flow.LMDB.get(Ferricstore.Flow.LMDB.path(dir), lmdb_key)

        assert {:ok, {_event_id, 3_002, _expire, _compound, {:flow_history, 0}, _offset, _size}} =
                 Ferricstore.Flow.LMDB.decode_history_index_location(lmdb_value)
      end

      test "sync projection does not copy Flow values already durable in LMDB" do
        unique = System.unique_integer([:positive])
        instance_name = :"history_projector_lmdb_value_#{unique}"
        dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_lmdb_value_#{unique}")
        source_path = Path.join(dir, "00000.log")

        File.mkdir_p!(dir)
        File.touch!(source_path)

        keydir = :ets.new(:"history_projector_lmdb_value_keydir_#{unique}", [:set, :public])
        {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
        NativeOrderedIndex.reset(flow_index, flow_lookup)

        on_exit(fn ->
          File.rm_rf(dir)
        end)

        ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          checkpoint_flags: :atomics.new(1, signed: false),
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency]),
          keydir_binary_bytes: :atomics.new(1, signed: true),
          flow_history_projected_index: :atomics.new(1, signed: false),
          flow_history_projector_flush_failures: :atomics.new(1, signed: false)
        }

        id = "flow-lmdb-value"
        history_key = Ferricstore.Flow.Keys.history_key(id)
        event_id = "1000-1"
        history_entry_key = Ferricstore.Flow.Keys.stream_entry_key(id, event_id, nil)
        value_key = "f:{flow-lmdb-value}:v:p:#{id}:1"
        source_value = Ferricstore.Flow.encode_value("source-payload")

        assert {:ok, [{value_offset, value_size}]} =
                 NIF.v2_append_batch_nosync(source_path, [{value_key, source_value, 0}])

        :ets.insert(
          keydir,
          {value_key, nil, 0, Ferricstore.Store.LFU.initial(), 0, value_offset, value_size}
        )

        lmdb_path = Ferricstore.Flow.LMDB.path(dir)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
                   {:put, value_key, Ferricstore.Flow.LMDB.encode_value(source_value, 0)}
                 ])

        record = %{
          id: id,
          type: "audit",
          state: "queued",
          version: 1,
          partition_key: nil,
          priority: 0,
          attempts: 0,
          next_run_at_ms: 1_000,
          created_at_ms: 1_000,
          updated_at_ms: 1_000,
          lease_owner: nil,
          lease_deadline_ms: 0,
          payload_ref: value_key
        }

        entry = %{
          key: history_entry_key,
          expire_at_ms: 0,
          history_key: history_key,
          event_id: event_id,
          event_ms: 1_000,
          version: 1,
          value: Ferricstore.Flow.encode_history_fields(record, "created", 1_000, %{}),
          ra_index: 42
        }

        assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 42)

        history_path = HistoryProjector.history_file_path(dir, 0)
        assert {:ok, records} = NIF.v2_scan_file(history_path)

        keys =
          Enum.map(records, fn {key, _offset, _value_size, _expire_at_ms, _deleted?} -> key end)

        assert history_entry_key in keys
        refute value_key in keys
        assert [] = :ets.lookup(keydir, value_key)
        assert {:ok, lmdb_value} = Ferricstore.Flow.LMDB.get(lmdb_path, value_key)
        assert {:ok, ^source_value} = Ferricstore.Flow.LMDB.decode_value(lmdb_value, 1_000)
      end
    end
  end
end
