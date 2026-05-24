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
          value: "history-#{version}",
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

    lmdb_path = Ferricstore.Flow.LMDB.path(dir)
    lmdb_key = Ferricstore.Flow.LMDB.history_index_key(history_key, "1001-1", 1001)

    assert {:ok, lmdb_value} = Ferricstore.Flow.LMDB.get(lmdb_path, lmdb_key)

    assert {:ok,
            {_event_id, _event_ms, _expire_at_ms, _compound_key, {:flow_history, 0}, offset,
             value_size}} =
             Ferricstore.Flow.LMDB.decode_history_index_location(lmdb_value)

    assert value_size > 0
    assert {:ok, "history-1"} = HistoryProjector.read_value(dir, {:flow_history, 0}, offset)
    assert HistoryProjector.durable?(ctx, 0, dir, 103)
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

      assert :ok = HistoryProjector.enqueue(ctx, 0, [entry.(1)], 1)
      assert {:error, :queue_full} = HistoryProjector.enqueue(ctx, 0, [entry.(2)], 2)

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

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
