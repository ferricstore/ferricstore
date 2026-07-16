defmodule Ferricstore.Flow.LMDBWriter.AfterFlushTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.Locator
  alias Ferricstore.Flow.LMDBWriter
  alias Ferricstore.Flow.LMDBWriter.AfterFlush

  test "after-flush actions expose only the current beta prune contracts" do
    source =
      File.read!(
        Path.expand("../../../../lib/ferricstore/flow/lmdb_writer/after_flush.ex", __DIR__)
      )

    refute source =~ ":prune_terminal_flow_v2"
    refute source =~ ":prune_terminal_flow_v3"
    refute source =~ ":prune_terminal_flow_from_source_v1"
    refute source =~ ":hibernate_flow_evict_hot_v1"

    assert source =~ "{:prune_terminal_flow, data_dir, shard_index"
    assert source =~ "{:prune_terminal_flow_from_source, data_dir, shard_index"
    assert source =~ "{:hibernate_flow_evict_hot,"
  end

  test "unknown after-flush actions fail closed" do
    assert {:error, :invalid_after_flush_action} =
             AfterFlush.apply_after_flush(:unknown_action)
  end

  test "flow tombstone deletion returns the after-flush success contract" do
    ets = :ets.new(:after_flush_tombstone, [:set])
    key = "flow:{flow:tombstone}:state:a"
    true = :ets.insert(ets, {key, nil, 0, :flow_state_deleted, :deleted, 0, 0})

    assert :ok = AfterFlush.apply_after_flush({:delete_flow_tombstone, ets, key})
    assert :ets.lookup(ets, key) == []
  end

  test "terminal prune reports a vanished source keydir" do
    ets = :ets.new(:after_flush_missing_keydir, [:set])
    :ets.delete(ets)

    action =
      {:prune_terminal_flow, "/data", 0, ets, nil, nil, nil, nil, "state-key", "type",
       "completed", nil, nil, nil, nil, "flow-1", 1}

    assert {:error, :source_keydir_unavailable} =
             AfterFlush.apply_after_flush(action)
  end

  test "source-based terminal prune reports a vanished source keydir" do
    ets = :ets.new(:after_flush_missing_source_keydir, [:set])
    :ets.delete(ets)

    action =
      {:prune_terminal_flow_from_source, "/data", 0, ets, nil, nil, nil, nil, "state-key", 1}

    assert {:error, :source_keydir_unavailable} =
             AfterFlush.apply_after_flush(action)
  end

  test "terminal source decoding waits for pending rows and reads expired WARaft projections" do
    state_key = "flow:{flow:after-flush-waraft}:state:a"

    pending_row =
      {state_key, nil, 1, 0, :pending, 0, 0}

    assert {:error, {:source_pending, ^state_key}} =
             AfterFlush.flow_record_from_keydir_row(
               "/data",
               0,
               state_key,
               pending_row
             )

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-after-flush-waraft-#{System.unique_integer([:positive])}"
      )

    projection_index = 17

    record = %{
      id: "after-flush-waraft",
      type: "cleanup",
      state: "completed",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: 2,
      next_run_at_ms: nil,
      priority: 0,
      ttl_ms: nil,
      history_hot_max_events: nil,
      history_max_events: nil,
      retention_ttl_ms: nil,
      max_active_ms: nil,
      terminal_retention_until_ms: 1,
      partition_key: nil,
      payload_ref: nil,
      parent_flow_id: nil,
      parent_partition_key: nil,
      root_flow_id: "after-flush-waraft",
      correlation_id: nil,
      result_ref: nil,
      error_ref: nil,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0,
      run_state: nil,
      child_groups: %{}
    }

    encoded = Ferricstore.Flow.encode_record(record)

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
               data_dir,
               0,
               projection_index,
               [{state_key, encoded, 1}]
             )

    on_exit(fn ->
      Ferricstore.Raft.WARaftSegmentReader.delete_apply_projection_entries(
        data_dir,
        0,
        [{projection_index, state_key}]
      )
    end)

    row =
      {state_key, nil, 1, 0, {:waraft_apply_projection, projection_index}, 0, byte_size(encoded)}

    assert {:ok, %{id: "after-flush-waraft", state: "completed"}} =
             AfterFlush.flow_record_from_keydir_row(data_dir, 0, state_key, row)

    assert 1 =
             Ferricstore.Raft.WARaftSegmentReader.delete_apply_projection_entries(
               data_dir,
               0,
               [{projection_index, state_key}]
             )
  end

  test "terminal prune reports a vanished hot zset index" do
    ets = :ets.new(:after_flush_terminal_source, [:set])
    zset_index = :ets.new(:after_flush_terminal_zset, [:ordered_set])
    zset_lookup = :ets.new(:after_flush_terminal_lookup, [:set])
    state_key = "flow:{flow:terminal-prune}:state:a"

    true =
      :ets.insert(
        ets,
        {state_key, "encoded", 0, {:flow_state_version, 1, 0}, :deleted, 0, 0}
      )

    true = :ets.delete(zset_lookup)

    action =
      {:prune_terminal_flow, "/data", 0, ets, zset_index, zset_lookup, nil, nil, state_key,
       "type", "completed", nil, nil, nil, nil, "flow-1", 1}

    assert {:error, :zset_index_unavailable} = AfterFlush.apply_after_flush(action)
  end

  test "terminal prune reports a corrupt untagged source record" do
    ets = :ets.new(:after_flush_corrupt_terminal_source, [:set])
    state_key = "flow:{flow:corrupt-terminal}:state:a"
    true = :ets.insert(ets, {state_key, "not-a-flow-record", 0, 0, :deleted, 0, 0})

    action =
      {:prune_terminal_flow, "/data", 0, ets, nil, nil, nil, nil, state_key, "type", "completed",
       nil, nil, nil, nil, "flow-1", 1}

    assert {:error, :invalid_source_flow_record} = AfterFlush.apply_after_flush(action)
    assert :ets.member(ets, state_key)
  end

  test "hibernation eviction reports a vanished source keydir" do
    ets = :ets.new(:after_flush_hibernation_source, [:set])
    true = :ets.delete(ets)

    locator = %Locator{
      flow_id: "flow-1",
      kind: :state,
      version: 1,
      raft_index: 1,
      file_id: 1,
      offset: 0,
      value_size: 1
    }

    action =
      {:hibernate_flow_evict_hot,
       %{
         data_dir: "/data",
         shard_index: 0,
         ets: ets,
         flow_index: nil,
         flow_lookup: nil,
         state_key: "flow:{flow:hibernate}:state:a",
         record: %{id: "flow-1"},
         locator: locator
       }}

    assert {:error, :source_keydir_unavailable} = AfterFlush.apply_after_flush(action)
  end

  test "deferred cleanup delay is bounded by the runtime timer limit" do
    assert AfterFlush.normalize_delay_ms(9_999_999_999) == 4_294_967_295
    assert AfterFlush.normalize_delay_ms(250) == 250
  end

  test "writer surfaces after-flush failures and marks the mirror dirty after LMDB commit" do
    unique = System.unique_integer([:positive])
    instance_name = :"lmdb_after_flush_failure_#{unique}"
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_after_flush_failure_#{unique}")
    degraded = :atomics.new(1, signed: false)
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    on_exit(fn -> File.rm_rf!(data_dir) end)

    pid =
      start_supervised!(
        {LMDBWriter,
         shard_index: 0,
         data_dir: data_dir,
         instance_ctx: %{
           name: instance_name,
           data_dir: data_dir,
           shard_count: 1,
           flow_lmdb_mirror_degraded: degraded
         }}
      )

    assert :ok =
             LMDBWriter.enqueue(
               instance_name,
               0,
               [{:put, "projection-key", "projection-value"}],
               [:unknown_action]
             )

    assert {:error, {:after_flush_failed, :invalid_after_flush_action}} =
             LMDBWriter.flush(instance_name, 0)

    assert {:ok, "projection-value"} =
             data_dir
             |> Ferricstore.DataDir.shard_data_path(0)
             |> LMDB.path()
             |> LMDB.get("projection-key")

    state = :sys.get_state(pid)
    assert state.projection_dirty?
    assert :atomics.get(degraded, 1) == 1
  end

  test "deferred after-flush failures mark the mirror dirty" do
    unique = System.unique_integer([:positive])
    instance_name = :"lmdb_deferred_after_flush_failure_#{unique}"
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_deferred_after_flush_#{unique}")
    degraded = :atomics.new(1, signed: false)
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    on_exit(fn -> File.rm_rf!(data_dir) end)

    pid =
      start_supervised!(
        {LMDBWriter,
         shard_index: 0,
         data_dir: data_dir,
         instance_ctx: %{
           name: instance_name,
           data_dir: data_dir,
           shard_count: 1,
           flow_lmdb_mirror_degraded: degraded
         }}
      )

    send(pid, {:apply_after_flush, :unknown_action})

    eventually(fn ->
      state = :sys.get_state(pid)
      state.projection_dirty? and :atomics.get(degraded, 1) == 1
    end)
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: flunk("condition did not become true")
end
