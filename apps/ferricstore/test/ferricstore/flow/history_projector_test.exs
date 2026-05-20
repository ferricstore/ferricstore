defmodule Ferricstore.Flow.HistoryProjectorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.OrderedIndex

  test "name falls back to default for checkpoint-only instance maps" do
    assert HistoryProjector.name(%{checkpoint_flags: :atomics.new(1, signed: false)}, 0) ==
             HistoryProjector.name(nil, 0)
  end

  test "sync projection writes dedicated history log, updates index, and advances watermark" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_test_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_#{unique}")
    hot_path = Path.join(dir, "00000.log")

    File.mkdir_p!(dir)
    File.touch!(hot_path)

    keydir = :ets.new(:"history_projector_keydir_#{unique}", [:set, :public])
    {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
    :ets.new(flow_index, [:ordered_set, :public, :named_table])
    :ets.new(flow_lookup, [:set, :public, :named_table])

    on_exit(fn ->
      if :ets.whereis(flow_index) != :undefined, do: :ets.delete(flow_index)
      if :ets.whereis(flow_lookup) != :undefined, do: :ets.delete(flow_lookup)
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

  test "recover returns an error and emits telemetry when history path is invalid" do
    unique = System.unique_integer([:positive])
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_error_#{unique}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "history"), "not-a-directory")

    test_pid = self()
    handler_id = {:history_projector_recover_error, self(), make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :flow, :history_projector, :recover],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:history_projector_recover_error, event, measurements, metadata})
      end,
      nil
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

  test "start_link can skip recovery when shard startup already imported history" do
    unique = System.unique_integer([:positive])
    instance_name = :"history_projector_skip_recover_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_skip_#{unique}")
    File.mkdir_p!(dir)

    keydir = :ets.new(:"history_projector_skip_keydir_#{unique}", [:set, :public])
    {flow_index, flow_lookup} = OrderedIndex.table_names(instance_name, 0)
    :ets.new(flow_index, [:ordered_set, :public, :named_table])
    :ets.new(flow_lookup, [:set, :public, :named_table])

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
    :ets.delete_all_objects(flow_index)
    :ets.delete_all_objects(flow_lookup)

    {:ok, pid} =
      HistoryProjector.start_link(
        shard_index: 0,
        shard_data_path: dir,
        instance_ctx: ctx,
        recover_on_init: false
      )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
      if :ets.whereis(flow_index) != :undefined, do: :ets.delete(flow_index)
      if :ets.whereis(flow_lookup) != :undefined, do: :ets.delete(flow_lookup)
      File.rm_rf(dir)
    end)

    assert [] = :ets.lookup(keydir, key)
    assert OrderedIndex.count_all(flow_lookup, history_key) == 0
    assert HistoryProjector.durable?(ctx, 0, dir, 7)
  end
end
