defmodule Ferricstore.Flow.HistoryProjectorTest do
  use ExUnit.Case, async: false

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
    assert OrderedIndex.rank_range(flow_index, history_key, 0, 1, false) == [{event_id, 1000.0}]
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
    old_lmdb_hook = Application.get_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)

    Application.put_env(:ferricstore, :flow_history_projector_fsync_hook, fn path ->
      send(test_pid, {:history_fsync, path})
      :ok
    end)

    Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, fn _shard_path,
                                                                                    _file_id,
                                                                                    _entries ->
      send(test_pid, :history_lmdb_publish)
      :ok
    end)

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
      Path.join(System.tmp_dir!(), "ferricstore_history_projector_lmdb_publish_fail_#{unique}")

    File.mkdir_p!(dir)

    keydir = :ets.new(:"history_projector_lmdb_publish_fail_keydir_#{unique}", [:set, :public])
    {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
    NativeOrderedIndex.reset(flow_index, flow_lookup)

    old_lmdb_hook = Application.get_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)

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
      Path.join(System.tmp_dir!(), "ferricstore_history_projector_cap_tombstone_fsync_#{unique}")

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
    assert :not_found = Ferricstore.Flow.LMDB.get(Ferricstore.Flow.LMDB.path(dir), old_lmdb_key)
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

    lmdb_key = Ferricstore.Flow.LMDB.history_index_key(history_key, unprojected_event_id, 3_004)
    assert :not_found == Ferricstore.Flow.LMDB.get(Ferricstore.Flow.LMDB.path(dir), lmdb_key)

    NativeOrderedIndex.reset(flow_index, flow_lookup)
    :ets.delete_all_objects(keydir)
    :atomics.put(ctx.flow_history_projected_index, 1, 0)

    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)

    assert {:ok, lmdb_value} =
             Ferricstore.Flow.LMDB.get(Ferricstore.Flow.LMDB.path(dir), lmdb_key)

    assert {:ok,
            {^unprojected_event_id, 3_004, _expire, _compound, {:flow_history, 0}, _offset, _size}} =
             Ferricstore.Flow.LMDB.decode_history_index_location(lmdb_value)
  end

  test "recover with zero hot history trusts existing LMDB projection when projected marker is missing" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_missing_marker_recover_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_missing_marker_#{unique}")
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
      key: Ferricstore.Flow.Keys.stream_entry_key("flow-missing-marker-recover", event_id, nil),
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
    keys = Enum.map(records, fn {key, _offset, _value_size, _expire_at_ms, _deleted?} -> key end)

    assert history_entry_key in keys
    refute value_key in keys
    assert [] = :ets.lookup(keydir, value_key)
    assert {:ok, lmdb_value} = Ferricstore.Flow.LMDB.get(lmdb_path, value_key)
    assert {:ok, ^source_value} = Ferricstore.Flow.LMDB.decode_value(lmdb_value, 1_000)
  end

  test "sync projection hydrates spilled WARaft apply-projection values by batch" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_apply_projection_#{unique}"

    data_dir =
      Path.join(System.tmp_dir!(), "ferricstore_history_projector_apply_projection_#{unique}")

    dir = Path.join([data_dir, "data", "shard_0"])

    File.mkdir_p!(dir)

    keydir = :ets.new(:"history_projector_apply_projection_keydir_#{unique}", [:set, :public])
    {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
    NativeOrderedIndex.reset(flow_index, flow_lookup)

    on_exit(fn ->
      File.rm_rf(data_dir)
    end)

    ctx = %{
      name: instance_name,
      data_dir: data_dir,
      keydir_refs: {keydir},
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false),
      write_version: :counters.new(1, [:write_concurrency]),
      keydir_binary_bytes: :atomics.new(1, signed: true),
      flow_history_projected_index: :atomics.new(1, signed: false),
      flow_history_projector_flush_failures: :atomics.new(1, signed: false)
    }

    projection_index = 42
    id_a = "flow-apply-projection-a"
    id_b = "flow-apply-projection-b"
    value_key_a = "f:{flow-apply-projection-a}:v:p:#{id_a}:1"
    value_key_b = "f:{flow-apply-projection-b}:v:p:#{id_b}:1"
    source_value_a = Ferricstore.Flow.encode_value("payload-a")
    source_value_b = Ferricstore.Flow.encode_value("payload-b")

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
               data_dir,
               0,
               projection_index,
               [{value_key_a, source_value_a, 0}, {value_key_b, source_value_b, 0}]
             )

    assert {:ok, 2} =
             Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

    for {key, value} <- [{value_key_a, source_value_a}, {value_key_b, source_value_b}] do
      :ets.insert(
        keydir,
        {key, nil, 0, Ferricstore.Store.LFU.initial(),
         {:waraft_apply_projection, projection_index}, 0, byte_size(value)}
      )
    end

    entries =
      [
        {id_a, value_key_a, source_value_a, 1_000, 1},
        {id_b, value_key_b, source_value_b, 1_001, 2}
      ]
      |> Enum.map(fn {id, payload_ref, _source_value, event_ms, version} ->
        record = %{
          id: id,
          type: "audit",
          state: "queued",
          version: version,
          partition_key: nil,
          priority: 0,
          attempts: 0,
          next_run_at_ms: event_ms,
          created_at_ms: event_ms,
          updated_at_ms: event_ms,
          lease_owner: nil,
          lease_deadline_ms: 0,
          payload_ref: payload_ref
        }

        event_id = "#{event_ms}-#{version}"

        %{
          key: Ferricstore.Flow.Keys.stream_entry_key(id, event_id, nil),
          expire_at_ms: 0,
          history_key: Ferricstore.Flow.Keys.history_key(id),
          event_id: event_id,
          event_ms: event_ms,
          version: version,
          value: Ferricstore.Flow.encode_history_fields(record, "created", event_ms, %{}),
          ra_index: projection_index
        }
      end)

    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, entries, projection_index)

    history_path = HistoryProjector.history_file_path(dir, 0)
    assert {:ok, records} = NIF.v2_scan_file(history_path)
    keys = Enum.map(records, fn {key, _offset, _value_size, _expire_at_ms, _deleted?} -> key end)

    assert value_key_a in keys
    assert value_key_b in keys
    assert [] = :ets.lookup(keydir, value_key_a)
    assert [] = :ets.lookup(keydir, value_key_b)

    lmdb_path = Ferricstore.Flow.LMDB.path(dir)
    assert {:ok, lmdb_value_a} = Ferricstore.Flow.LMDB.get(lmdb_path, value_key_a)
    assert {:ok, lmdb_value_b} = Ferricstore.Flow.LMDB.get(lmdb_path, value_key_b)

    assert {:ok, {{:flow_history, file_id_a}, offset_a, value_size_a}} =
             Ferricstore.Flow.LMDB.decode_value_locator(lmdb_value_a, 1_000)

    assert {:ok, {{:flow_history, file_id_b}, offset_b, value_size_b}} =
             Ferricstore.Flow.LMDB.decode_value_locator(lmdb_value_b, 1_001)

    assert value_size_a == byte_size(source_value_a)
    assert value_size_b == byte_size(source_value_b)
    assert file_id_a == 0
    assert file_id_b == 0
    assert is_integer(offset_a) and offset_a >= 0
    assert is_integer(offset_b) and offset_b >= 0

    assert {:ok, ^source_value_a} =
             HistoryProjector.read_value(dir, {:flow_history, file_id_a}, offset_a)

    assert {:ok, ^source_value_b} =
             HistoryProjector.read_value(dir, {:flow_history, file_id_b}, offset_b)

    assert {:ok, pins} =
             Ferricstore.Flow.LMDB.segment_value_pin_entries_before(
               lmdb_path,
               projection_index + 1,
               100
             )

    assert pins == []
  end

  test "apply projection cache spill clears only the spilled shard root" do
    unique = System.unique_integer([:positive])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_apply_projection_spill_#{unique}")
    File.mkdir_p!(data_dir)

    ctx = %{data_dir: data_dir}
    shard0_key_a = "apply-projection-spill-shard0-a"
    shard0_key_b = "apply-projection-spill-shard0-b"
    shard1_key = "apply-projection-spill-shard1"

    on_exit(fn -> File.rm_rf(data_dir) end)

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
               data_dir,
               0,
               101,
               [{shard0_key_a, "value-a", 0}, {shard0_key_b, "value-b", 0}]
             )

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
               data_dir,
               1,
               202,
               [{shard1_key, "value-c", 0}]
             )

    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 2
    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 1) == 1

    assert {:ok, 2} =
             Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 0
    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 1) == 1

    assert {:ok, %{^shard0_key_a => "value-a", ^shard0_key_b => "value-b"}} =
             Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
               ctx,
               0,
               {:waraft_apply_projection, 101},
               [shard0_key_a, shard0_key_b]
             )

    assert {:ok, %{^shard1_key => "value-c"}} =
             Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
               ctx,
               1,
               {:waraft_apply_projection, 202},
               [shard1_key]
             )
  end

  test "apply projection cache spill can remove a bounded slice" do
    unique = System.unique_integer([:positive])

    data_dir =
      Path.join(System.tmp_dir!(), "ferricstore_apply_projection_bounded_spill_#{unique}")

    File.mkdir_p!(data_dir)

    ctx = %{data_dir: data_dir}
    shard0_key_a = "apply-projection-bounded-shard0-a"
    shard0_key_b = "apply-projection-bounded-shard0-b"
    shard0_key_c = "apply-projection-bounded-shard0-c"
    shard1_key = "apply-projection-bounded-shard1"

    on_exit(fn -> File.rm_rf(data_dir) end)

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
               data_dir,
               0,
               301,
               [{shard0_key_a, "value-a", 0}, {shard0_key_b, "value-b", 0}]
             )

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
               data_dir,
               0,
               302,
               [{shard0_key_c, "value-c", 0}]
             )

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
               data_dir,
               1,
               401,
               [{shard1_key, "value-d", 0}]
             )

    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 3
    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 1) == 1

    assert {:ok, removed} =
             Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0, 2)

    assert removed <= 3
    assert removed > 0

    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) ==
             3 - removed

    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 1) == 1

    assert {:ok, %{^shard0_key_a => "value-a", ^shard0_key_b => "value-b"}} =
             Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
               ctx,
               0,
               {:waraft_apply_projection, 301},
               [shard0_key_a, shard0_key_b]
             )

    assert {:ok, %{^shard0_key_c => "value-c"}} =
             Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
               ctx,
               0,
               {:waraft_apply_projection, 302},
               [shard0_key_c]
             )
  end

  test "bounded apply projection cache spill keeps one apply index atomic" do
    unique = System.unique_integer([:positive])

    data_dir =
      Path.join(System.tmp_dir!(), "ferricstore_apply_projection_atomic_spill_#{unique}")

    File.mkdir_p!(data_dir)

    ctx = %{data_dir: data_dir}
    shard_index = 0
    projection_index = 501
    key_a = "apply-projection-atomic-a"
    key_b = "apply-projection-atomic-b"
    key_c = "apply-projection-atomic-c"

    on_exit(fn -> File.rm_rf(data_dir) end)

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
               data_dir,
               shard_index,
               projection_index,
               [{key_a, "value-a", 0}, {key_b, "value-b", 0}, {key_c, "value-c", 0}]
             )

    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(
             data_dir,
             shard_index
           ) == 3

    assert {:ok, 3} =
             Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(
               data_dir,
               shard_index,
               2
             )

    assert {:ok, 0} =
             Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(
               data_dir,
               shard_index,
               2
             )

    assert {:ok, %{^key_a => "value-a", ^key_b => "value-b", ^key_c => "value-c"}} =
             Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
               ctx,
               shard_index,
               {:waraft_apply_projection, projection_index},
               [key_a, key_b, key_c]
             )
  end

  test "value projection filters absent keydir refs before LMDB live checks" do
    unique = System.unique_integer([:positive])
    keydir = :ets.new(:"history_projector_value_refs_#{unique}", [:set, :public])
    present_ref = "f:{flow-present}:v:p:flow-present:1"
    absent_ref = "f:{flow-absent}:v:p:flow-absent:1"
    stale_ref = "f:{flow-stale}:v:p:flow-stale:1"

    :ets.insert(
      keydir,
      {present_ref, nil, 0, Ferricstore.Store.LFU.initial(), 0, 0, 128}
    )

    :ets.insert(
      keydir,
      {stale_ref, nil, 0, Ferricstore.Store.LFU.initial(), :bad_file, 0, 128}
    )

    assert [^present_ref] =
             HistoryProjector.__projected_flow_value_keydir_refs_for_test__(
               keydir,
               [absent_ref, present_ref, stale_ref]
             )
  end

  test "value projection never flushes the LMDB writer synchronously" do
    source =
      File.read!(
        Path.expand(
          "../../../lib/ferricstore/flow/history_projector.ex",
          __DIR__
        )
      )

    refute source =~ "LMDBWriter.flush",
           "Flow value projection must not block behind the async LMDB writer; it writes value locators itself before deleting keydir refs"
  end

  test "value projection copies WARaft apply-projection values into the history log" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_direct_projection_#{unique}"

    data_dir =
      Path.join(System.tmp_dir!(), "ferricstore_history_projector_direct_projection_#{unique}")

    dir = Path.join([data_dir, "data", "shard_0"])
    File.mkdir_p!(dir)

    keydir = :ets.new(:"history_projector_direct_projection_keydir_#{unique}", [:set, :public])

    on_exit(fn ->
      File.rm_rf(data_dir)
    end)

    ctx = %{
      name: instance_name,
      data_dir: data_dir,
      keydir_refs: {keydir},
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false),
      write_version: :counters.new(1, [:write_concurrency]),
      keydir_binary_bytes: :atomics.new(1, signed: true),
      flow_history_projected_index: :atomics.new(1, signed: false),
      flow_history_projector_flush_failures: :atomics.new(1, signed: false)
    }

    projection_index = 4242
    id = "flow-copy-projection"
    value_key = "f:{flow-copy-projection}:v:p:#{id}:1"
    source_value = Ferricstore.Flow.encode_value("payload-copy")

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
               data_dir,
               0,
               projection_index,
               [{value_key, source_value, 0}]
             )

    :ets.insert(
      keydir,
      {value_key, nil, 0, Ferricstore.Store.LFU.initial(),
       {:waraft_apply_projection, projection_index}, 0, byte_size(source_value)}
    )

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
      key: Ferricstore.Flow.Keys.stream_entry_key(id, "1000-1", nil),
      expire_at_ms: 0,
      history_key: Ferricstore.Flow.Keys.history_key(id),
      event_id: "1000-1",
      event_ms: 1_000,
      version: 1,
      value: Ferricstore.Flow.encode_history_fields(record, "created", 1_000, %{}),
      ra_index: projection_index
    }

    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], projection_index)
    assert [] = :ets.lookup(keydir, value_key)

    lmdb_path = Ferricstore.Flow.LMDB.path(dir)
    assert {:ok, lmdb_value} = Ferricstore.Flow.LMDB.get(lmdb_path, value_key)

    assert {:ok, {{:flow_history, file_id}, offset, value_size}} =
             Ferricstore.Flow.LMDB.decode_value_locator(lmdb_value, 1_000)

    assert file_id == 0
    assert is_integer(offset) and offset >= 0
    assert value_size == byte_size(source_value)

    assert {:ok, ^source_value} =
             HistoryProjector.read_value(dir, {:flow_history, file_id}, offset)
  end

  test "value projection overwrites stale LMDB locator from newer direct segment keydir row" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_stale_value_locator_#{unique}"

    data_dir =
      Path.join(System.tmp_dir!(), "ferricstore_history_projector_stale_value_locator_#{unique}")

    dir = Path.join([data_dir, "data", "shard_0"])
    File.mkdir_p!(dir)

    keydir = :ets.new(:"history_projector_stale_value_locator_keydir_#{unique}", [:set, :public])

    on_exit(fn ->
      File.rm_rf(data_dir)
    end)

    ctx = %{
      name: instance_name,
      data_dir: data_dir,
      keydir_refs: {keydir},
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false),
      write_version: :counters.new(1, [:write_concurrency]),
      keydir_binary_bytes: :atomics.new(1, signed: true),
      flow_history_projected_index: :atomics.new(1, signed: false),
      flow_history_projector_flush_failures: :atomics.new(1, signed: false)
    }

    id = "flow-stale-locator"
    value_key = "f:{flow-stale-locator}:v:p:#{id}:2"
    lmdb_path = Ferricstore.Flow.LMDB.path(dir)

    stale_locator =
      Ferricstore.Flow.LMDB.encode_value_locator(0, {:flow_history, 0}, 11, 22)

    assert :ok = Ferricstore.Flow.LMDB.write_batch(lmdb_path, [{:put, value_key, stale_locator}])

    :ets.insert(
      keydir,
      {value_key, nil, 0, Ferricstore.Store.LFU.initial(), {:waraft_segment, 44}, 123, 456}
    )

    record = %{
      id: id,
      type: "audit",
      state: "running",
      version: 2,
      partition_key: nil,
      priority: 0,
      attempts: 0,
      next_run_at_ms: 1_000,
      created_at_ms: 1_000,
      updated_at_ms: 2_000,
      lease_owner: nil,
      lease_deadline_ms: 0,
      payload_ref: value_key
    }

    entry = %{
      key: Ferricstore.Flow.Keys.stream_entry_key(id, "2000-2", nil),
      expire_at_ms: 0,
      history_key: Ferricstore.Flow.Keys.history_key(id),
      event_id: "2000-2",
      event_ms: 2_000,
      version: 2,
      value: Ferricstore.Flow.encode_history_fields(record, "transitioned", 2_000, %{}),
      ra_index: 44
    }

    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 44)
    assert [] = :ets.lookup(keydir, value_key)
    assert {:ok, lmdb_value} = Ferricstore.Flow.LMDB.get(lmdb_path, value_key)

    assert {:ok, {{:waraft_segment, 44}, 123, 456}} =
             Ferricstore.Flow.LMDB.decode_value_locator(lmdb_value, 2_000)
  end

  test "sync projection directly evicts hot history rows when previous event is known" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_direct_hot_evict_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_direct_hot_evict_#{unique}")
    File.mkdir_p!(dir)

    keydir = :ets.new(:"history_projector_direct_hot_evict_keydir_#{unique}", [:set, :public])

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

    history_key = Ferricstore.Flow.Keys.history_key("flow-direct-hot")
    old_event_id = "1000-1"
    old_key = Ferricstore.Flow.Keys.stream_entry_key("flow-direct-hot", old_event_id, nil)
    new_event_id = "1001-2"
    new_key = Ferricstore.Flow.Keys.stream_entry_key("flow-direct-hot", new_event_id, nil)

    :ets.insert(
      keydir,
      {old_key, nil, 0, Ferricstore.Store.LFU.initial(), {:flow_history, 0}, 0, 16}
    )

    entry = %{
      key: new_key,
      expire_at_ms: 0,
      history_key: history_key,
      event_id: new_event_id,
      event_ms: 1_001,
      version: 2,
      value: "history-2",
      history_hot_max_events: 1,
      hot_evict_event_ids: [old_event_id],
      ra_index: 44
    }

    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 44)

    assert [] = :ets.lookup(keydir, old_key)

    assert [{^new_key, nil, 0, _lfu, {:flow_history, 0}, _offset, _size}] =
             :ets.lookup(keydir, new_key)
  end

  test "direct hot-history eviction uses one native batch instead of per-flow deletes" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_direct_hot_batch_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_direct_hot_batch_#{unique}")
    File.mkdir_p!(dir)

    keydir = :ets.new(:"history_projector_direct_hot_batch_keydir_#{unique}", [:set, :public])
    {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
    native = NativeOrderedIndex.reset(flow_index, flow_lookup)

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

    entries =
      for idx <- 1..32 do
        id = "flow-hot-batch-#{idx}"
        history_key = Ferricstore.Flow.Keys.history_key(id)
        old_event_id = "1000-1"
        old_key = Ferricstore.Flow.Keys.stream_entry_key(id, old_event_id, nil)
        new_event_id = "1001-2"

        :ets.insert(
          keydir,
          {old_key, nil, 0, Ferricstore.Store.LFU.initial(), {:flow_history, 0}, idx, 16}
        )

        :ok = NativeOrderedIndex.put_entries(native, [{history_key, old_event_id, 1_000}])

        %{
          key: Ferricstore.Flow.Keys.stream_entry_key(id, new_event_id, nil),
          expire_at_ms: 0,
          history_key: history_key,
          event_id: new_event_id,
          event_ms: 1_001,
          version: 2,
          value: "history-#{idx}",
          history_hot_max_events: 1,
          hot_evict_event_ids: [old_event_id],
          ra_index: 100 + idx
        }
      end

    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, entries, 132)

    source =
      File.read!(
        Path.expand(
          "../../../lib/ferricstore/flow/history_projector.ex",
          __DIR__
        )
      )

    assert source =~ "NativeFlowIndex.delete_entries(native, history_delete_entries(items))"
    refute source =~ "NativeFlowIndex.delete_members(native"
    assert :ets.info(keydir, :size) == 32
  end

  test "zero hot-history cap is planned as direct eviction without rank scans" do
    entries =
      for idx <- 1..3 do
        id = "flow-zero-direct-#{idx}"
        event_id = "100#{idx}-#{idx}"

        %{
          key: Ferricstore.Flow.Keys.stream_entry_key(id, event_id, nil),
          expire_at_ms: 0,
          history_key: Ferricstore.Flow.Keys.history_key(id),
          event_id: event_id,
          event_ms: 1_000 + idx,
          version: idx,
          value: "history-#{idx}",
          history_hot_max_events: 0,
          ra_index: idx
        }
      end

    assert HistoryProjector.__direct_hot_history_evict_items_for_test__(entries) ==
             Enum.map(entries, fn entry ->
               {entry.history_key, entry.event_id, entry.key}
             end)

    assert HistoryProjector.__history_hot_rank_entries_for_test__(entries) == []
  end

  test "terminal hot-history trim keeps rank scan path to evict retained older events" do
    entry = %{
      key: Ferricstore.Flow.Keys.stream_entry_key("flow-terminal-direct", "1009-9", nil),
      expire_at_ms: 0,
      history_key: Ferricstore.Flow.Keys.history_key("flow-terminal-direct"),
      event_id: "1009-9",
      event_ms: 1_009,
      version: 9,
      value: "terminal",
      history_hot_max_events: 3,
      terminal?: true,
      ra_index: 9
    }

    assert HistoryProjector.__history_hot_rank_entries_for_test__([entry]) == [entry]
  end

  test "history index planning partitions new and update entries in order" do
    entries = [
      %{history_key: "h:1", event_id: "1000-1", event_ms: 1_000, version: 1},
      %{history_key: "h:1", event_id: "1001-2", event_ms: 1_001, version: 2},
      %{history_key: "h:2", event_id: "1002-1", event_ms: 1_002, version: 1},
      %{history_key: "h:2", event_id: "1003-4", event_ms: 1_003, version: 4}
    ]

    assert HistoryProjector.__history_index_entries_for_test__(entries) ==
             {
               [{"h:1", "1000-1", 1_000}, {"h:2", "1002-1", 1_002}],
               [{"h:1", "1001-2", 1_001}, {"h:2", "1003-4", 1_003}]
             }
  end

  test "history cap planning is one pass for unique under-cap entries" do
    entries =
      for idx <- 1..1_000 do
        %{
          history_key: "history:#{idx}",
          event_id: "1000-2",
          event_ms: 1_000,
          version: 2,
          history_max_events: 100_000,
          key: "history:#{idx}:1000-2",
          value: "history"
        }
      end

    requirements =
      HistoryProjector.__trim_cap_requirements_for_test__(entries, fn history_key ->
        flunk("unexpected cap load for #{inspect(history_key)}")
      end)

    assert map_size(requirements) == 1_000
    assert Enum.all?(requirements, fn {_history_key, {100_000, false}} -> true end)
  end

  test "history cap planning loads missing caps once and marks over-cap keys" do
    parent = self()

    entries = [
      %{history_key: "history:one", event_id: "1000-3", event_ms: 1_000, version: 3},
      %{history_key: "history:one", event_id: "1001-5", event_ms: 1_001, version: 5},
      %{history_key: "history:two", event_id: "1002-1", event_ms: 1_002, version: 1}
    ]

    requirements =
      HistoryProjector.__trim_cap_requirements_for_test__(entries, fn
        "history:one" ->
          send(parent, {:loaded_cap, "history:one"})
          4

        "history:two" ->
          send(parent, {:loaded_cap, "history:two"})
          10
      end)

    assert requirements == %{
             "history:one" => {4, true},
             "history:two" => {10, false}
           }

    assert_receive {:loaded_cap, "history:one"}
    assert_receive {:loaded_cap, "history:two"}
    refute_receive {:loaded_cap, _}
  end

  test "recover returns an error and emits telemetry when history path is invalid" do
    unique = System.unique_integer([:positive])
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_error_#{unique}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "history"), "not-a-directory")

    handler_id = {:history_projector_recover_error, self(), make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :flow, :history_projector, :recover],
      &__MODULE__.handle_recover_telemetry/4,
      self()
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      File.rm_rf(dir)
    end)

    assert {:error, _reason} = HistoryProjector.recover(nil, 0, dir)

    assert_receive {:history_projector_recover_error,
                    [:ferricstore, :flow, :history_projector, :recover], %{errors: 1},
                    %{shard_index: 0, reason: _reason}}
  end

  def handle_recover_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:history_projector_recover_error, event, measurements, metadata})
  end

  test "async enqueue rejects above configured pending cap so apply can fall back to sync projection" do
    unique = System.unique_integer([:positive])

    old_max_pending =
      Application.get_env(:ferricstore, :flow_history_projector_max_pending_entries)

    old_flush_interval =
      Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

    instance_name = :"history_projector_pending_cap_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_pending_cap_#{unique}")
    File.mkdir_p!(dir)

    keydir = :ets.new(:"history_projector_pending_cap_keydir_#{unique}", [:set, :public])
    {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
    NativeOrderedIndex.reset(flow_index, flow_lookup)

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

    try do
      Application.put_env(:ferricstore, :flow_history_projector_max_pending_entries, 1)
      Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)

      {:ok, pid} =
        HistoryProjector.start_link(
          shard_index: 0,
          shard_data_path: dir,
          instance_ctx: ctx,
          recover_on_init: false
        )

      entry = fn version ->
        event_ms = 1_000 + version
        event_id = "#{event_ms}-#{version}"

        %{
          key: Ferricstore.Flow.Keys.stream_entry_key("flow-cap", event_id, nil),
          expire_at_ms: 0,
          history_key: Ferricstore.Flow.Keys.history_key("flow-cap"),
          event_id: event_id,
          event_ms: event_ms,
          version: version,
          value: "history-#{version}",
          ra_index: version
        }
      end

      assert :ok = HistoryProjector.enqueue_async(ctx, 0, [entry.(1)], 1)
      assert {:error, :queue_full} = HistoryProjector.enqueue_async(ctx, 0, [entry.(2)], 2)

      assert Process.alive?(pid)
    after
      if Process.whereis(HistoryProjector.name(ctx, 0)),
        do: GenServer.stop(HistoryProjector.name(ctx, 0))

      File.rm_rf(dir)
      restore_env(:flow_history_projector_max_pending_entries, old_max_pending)
      restore_env(:flow_history_projector_flush_interval_ms, old_flush_interval)
    end
  end

  test "fire-and-forget enqueue does not wait behind projector work" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_async_enqueue_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_async_enqueue_#{unique}")
    File.mkdir_p!(dir)

    keydir = :ets.new(:"history_projector_async_enqueue_keydir_#{unique}", [:set, :public])
    {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
    NativeOrderedIndex.reset(flow_index, flow_lookup)

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

    try do
      {:ok, pid} =
        HistoryProjector.start_link(
          shard_index: 0,
          shard_data_path: dir,
          instance_ctx: ctx,
          recover_on_init: false
        )

      :ok = :sys.suspend(pid)

      entry = %{
        key: Ferricstore.Flow.Keys.stream_entry_key("flow-async-enqueue", "1001-1", nil),
        expire_at_ms: 0,
        history_key: Ferricstore.Flow.Keys.history_key("flow-async-enqueue"),
        event_id: "1001-1",
        event_ms: 1001,
        version: 1,
        value: "history-1",
        ra_index: 1
      }

      started = System.monotonic_time()
      assert :ok = HistoryProjector.enqueue_async(ctx, 0, [entry], 1)

      elapsed_ms =
        System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond)

      assert elapsed_ms < 50

      :ok = :sys.resume(pid)
      assert :ok = HistoryProjector.flush(ctx, 0)
    after
      if Process.whereis(HistoryProjector.name(ctx, 0)),
        do: GenServer.stop(HistoryProjector.name(ctx, 0))

      File.rm_rf(dir)
    end
  end

  test "requested replay index is not published after async enqueue is lost before projector handles it" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_lost_async_enqueue_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_lost_async_#{unique}")
    File.mkdir_p!(dir)

    keydir = :ets.new(:"history_projector_lost_async_keydir_#{unique}", [:set, :public])
    {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
    NativeOrderedIndex.reset(flow_index, flow_lookup)

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

    try do
      {:ok, pid} =
        HistoryProjector.start_link(
          shard_index: 0,
          shard_data_path: dir,
          instance_ctx: ctx,
          recover_on_init: false
        )

      :ok = :sys.suspend(pid)

      event_id = "1001-1"
      key = Ferricstore.Flow.Keys.stream_entry_key("flow-lost-async", event_id, nil)

      entry = %{
        key: key,
        expire_at_ms: 0,
        history_key: Ferricstore.Flow.Keys.history_key("flow-lost-async"),
        event_id: event_id,
        event_ms: 1001,
        version: 1,
        value: "history-lost-before-handle",
        ra_index: 42
      }

      assert :ok = HistoryProjector.enqueue_async(ctx, 0, [entry], 42)

      ref = Process.monitor(pid)
      Process.unlink(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000

      {:ok, _pid} =
        HistoryProjector.start_link(
          shard_index: 0,
          shard_data_path: dir,
          instance_ctx: ctx,
          recover_on_init: false
        )

      assert :requested = HistoryProjector.request(ctx, 0, dir, 42)
      assert {:error, :flush_failed} = HistoryProjector.flush(ctx, 0)
      refute HistoryProjector.durable?(ctx, 0, dir, 42)
      assert HistoryProjector.scan_event_value(dir, key) == :miss
    after
      if Process.whereis(HistoryProjector.name(ctx, 0)),
        do: GenServer.stop(HistoryProjector.name(ctx, 0))

      File.rm_rf(dir)
    end
  end

  test "timer-driven projection flushes one bounded chunk at a time" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_chunked_flush_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_chunked_#{unique}")
    File.mkdir_p!(dir)

    old_batch = Application.get_env(:ferricstore, :flow_history_projector_batch_size)
    old_flush = Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)
    old_chunk = Application.get_env(:ferricstore, :flow_history_projector_chunk_interval_ms)

    keydir = :ets.new(:"history_projector_chunked_keydir_#{unique}", [:set, :public])
    {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
    NativeOrderedIndex.reset(flow_index, flow_lookup)

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

    try do
      Application.put_env(:ferricstore, :flow_history_projector_batch_size, 2)
      Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)
      Application.put_env(:ferricstore, :flow_history_projector_chunk_interval_ms, 60_000)

      {:ok, pid} =
        HistoryProjector.start_link(
          shard_index: 0,
          shard_data_path: dir,
          instance_ctx: ctx,
          recover_on_init: false
        )

      entries =
        for version <- 1..5 do
          event_ms = 2_000 + version
          event_id = "#{event_ms}-#{version}"

          %{
            key: Ferricstore.Flow.Keys.stream_entry_key("flow-chunked", event_id, nil),
            expire_at_ms: 0,
            history_key: Ferricstore.Flow.Keys.history_key("flow-chunked"),
            event_id: event_id,
            event_ms: event_ms,
            version: version,
            value: "history-#{version}",
            ra_index: version
          }
        end

      assert :ok = HistoryProjector.enqueue_async(ctx, 0, entries, 5)

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          state = :sys.get_state(pid)
          state.pending_count == 3 and :ets.info(keydir, :size) == 2
        end,
        "history projector should flush only one configured chunk",
        50,
        10
      )

      assert :ok = HistoryProjector.flush(ctx, 0)
      assert :ets.info(keydir, :size) == 5
    after
      if Process.whereis(HistoryProjector.name(ctx, 0)),
        do: GenServer.stop(HistoryProjector.name(ctx, 0))

      File.rm_rf(dir)
      restore_env(:flow_history_projector_batch_size, old_batch)
      restore_env(:flow_history_projector_flush_interval_ms, old_flush)
      restore_env(:flow_history_projector_chunk_interval_ms, old_chunk)
    end
  end

  test "start_link can skip recovery when shard startup already imported history" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_skip_recover_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_skip_#{unique}")
    File.mkdir_p!(dir)

    keydir = :ets.new(:"history_projector_skip_keydir_#{unique}", [:set, :public])
    {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
    NativeOrderedIndex.reset(flow_index, flow_lookup)

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

    history_key = Ferricstore.Flow.Keys.history_key("flow-skip")
    event_id = "1000-1"
    key = Ferricstore.Flow.Keys.stream_entry_key("flow-skip", event_id, nil)

    entry = %{
      key: key,
      expire_at_ms: 0,
      history_key: history_key,
      event_id: event_id,
      event_ms: 1_000,
      version: 1,
      value: "encoded-history",
      history_max_events: nil,
      ra_index: 7
    }

    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 7)
    :ets.delete_all_objects(keydir)
    NativeOrderedIndex.reset(flow_index, flow_lookup)

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

    assert [] = :ets.lookup(keydir, key)
    assert OrderedIndex.count_all(flow_lookup, history_key) == 0
    assert HistoryProjector.durable?(ctx, 0, dir, 7)
  end

  test "projected Flow value refs use apply-stamped refs without decoding history payload" do
    generated_ref = "f:{flow-fast-ref}:v:p:flow-fast-ref:2"
    generated_shared_ref = "f:{flow-fast-ref}:v:s:flow-fast-ref:doc:1"
    external_ref = "external-payload-ref"

    refs =
      HistoryProjector.__projected_flow_value_refs_for_test__([
        %{
          value: "not-a-decodable-history-record",
          value_refs: [generated_ref, generated_shared_ref, external_ref, nil, ""]
        }
      ])

    assert MapSet.member?(refs, generated_ref)
    assert MapSet.member?(refs, generated_shared_ref)
    refute MapSet.member?(refs, external_ref)
  end

  test "projected watermark stays behind when marker persist fails" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_marker_fail_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_marker_fail_#{unique}")
    File.mkdir_p!(dir)
    File.mkdir_p!(Ferricstore.Flow.HistoryProjectedIndex.path(dir))

    keydir = :ets.new(:"history_projector_marker_fail_keydir_#{unique}", [:set, :public])
    {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
    NativeOrderedIndex.reset(flow_index, flow_lookup)

    on_exit(fn -> File.rm_rf(dir) end)

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

    entry = %{
      key: Ferricstore.Flow.Keys.stream_entry_key("flow-marker-fail", "9000-1", nil),
      expire_at_ms: 0,
      history_key: Ferricstore.Flow.Keys.history_key("flow-marker-fail"),
      event_id: "9000-1",
      event_ms: 9_000,
      version: 1,
      value: "history-marker-fail",
      ra_index: 900
    }

    assert {:error, _reason} = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 900)
    refute HistoryProjector.durable?(ctx, 0, dir, 900)
    assert :atomics.get(ctx.flow_history_projected_index, 1) == 0
  end

  test "projected index marker persists concurrently without tmp collisions or regression" do
    unique = System.unique_integer([:positive])
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projected_index_concurrent_#{unique}")
    File.mkdir_p!(dir)

    on_exit(fn -> File.rm_rf(dir) end)

    results =
      1..100
      |> Task.async_stream(
        fn index -> Ferricstore.Flow.HistoryProjectedIndex.persist(dir, index) end,
        max_concurrency: 32,
        timeout: 10_000
      )
      |> Enum.to_list()

    assert Enum.all?(results, &match?({:ok, :ok}, &1))
    assert Ferricstore.Flow.HistoryProjectedIndex.read(dir) == 100
  end

  test "projected index marker persistence fsyncs tmp file and parent directory" do
    source = File.read!("lib/ferricstore/flow/history_projected_index.ex")

    assert source =~ "NIF.v2_fsync(tmp_path)",
           "projected marker must fsync tmp contents before rename"

    assert source =~ "NIF.v2_fsync_dir(shard_data_path)",
           "projected marker must fsync the directory after rename"
  end

  test "pending_count fails closed when projector is not started" do
    unique = System.unique_integer([:positive])
    ctx = %{name: :"history_projector_missing_pending_#{unique}"}

    assert {:error, :not_started} = HistoryProjector.pending_count(ctx, 0)
  end

  test "async enqueue uses existing pending registry without waiting on table owner" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_enqueue_fast_registry_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_enqueue_fast_#{unique}")
    File.mkdir_p!(dir)

    keydir = :ets.new(:"history_projector_enqueue_fast_keydir_#{unique}", [:set, :public])

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

    {:ok, pid} =
      HistoryProjector.start_link(
        shard_index: 0,
        shard_data_path: dir,
        instance_ctx: ctx,
        recover_on_init: false
      )

    owner = Process.whereis(Ferricstore.Flow.HistoryProjector.TableOwner)
    assert is_pid(owner)
    :sys.suspend(owner)

    on_exit(fn ->
      if Process.alive?(owner), do: :sys.resume(owner)
      if Process.alive?(pid), do: GenServer.stop(pid)
      File.rm_rf(dir)
    end)

    entry = %{
      key: Ferricstore.Flow.Keys.stream_entry_key("flow-fast-enqueue", "1000-1", nil),
      expire_at_ms: 0,
      history_key: Ferricstore.Flow.Keys.history_key("flow-fast-enqueue"),
      event_id: "1000-1",
      event_ms: 1_000,
      version: 1,
      value: "encoded-history",
      ra_index: 1
    }

    task = Task.async(fn -> HistoryProjector.enqueue_async(ctx, 0, [entry], 1) end)
    assert :ok = Task.await(task, 100)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
