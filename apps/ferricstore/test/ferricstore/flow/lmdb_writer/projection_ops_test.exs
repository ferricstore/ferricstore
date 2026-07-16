defmodule Ferricstore.Flow.LMDBWriter.ProjectionOpsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDBWriter.ProjectionOps
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.NativeOrderedIndex
  alias Ferricstore.Flow.OrderedIndex

  test "flow record envelope decoding rejects non-canonical external terms" do
    encoded_record =
      Ferricstore.Flow.encode_record(%{
        id: "projection-flow",
        type: "projection-type",
        state: "running",
        version: 1,
        attempts: 0,
        fencing_token: 0,
        created_at_ms: 100,
        updated_at_ms: 100,
        next_run_at_ms: nil,
        priority: 0,
        ttl_ms: nil,
        history_hot_max_events: nil,
        history_max_events: nil,
        retention_ttl_ms: nil,
        max_active_ms: nil,
        terminal_retention_until_ms: nil,
        partition_key: nil,
        payload_ref: nil,
        parent_flow_id: nil,
        parent_partition_key: nil,
        root_flow_id: "projection-flow",
        correlation_id: nil,
        result_ref: nil,
        error_ref: nil,
        lease_owner: nil,
        lease_token: nil,
        lease_deadline_ms: 0,
        run_state: nil,
        child_groups: %{}
      })

    envelope = Ferricstore.Flow.LMDB.encode_value(encoded_record, 0)
    assert {:ok, %{id: "projection-flow"}} = ProjectionOps.decode_flow_record_value(envelope)
    assert :error = ProjectionOps.decode_flow_record_value(envelope <> <<0>>)
  end

  test "terminal history projection walks the native index in bounded cursor pages" do
    instance_name = :"history_projection_#{System.unique_integer([:positive, :monotonic])}"
    {index, lookup} = OrderedIndex.table_names(instance_name, 0)
    NativeOrderedIndex.reset(index, lookup)
    native = NativeOrderedIndex.get(index, lookup)
    history_key = "history:bounded"

    entries =
      for number <- 1..2_050 do
        event_id = "#{String.pad_leading(Integer.to_string(number), 5, "0")}-0"
        {history_key, event_id, number}
      end

    assert :ok = NativeOrderedIndex.put_entries(native, entries)

    projected = ProjectionOps.history_project_from_native_entries(native, history_key)
    assert length(projected) == 2_050
    assert hd(projected) == {"00001-0", 1}
    assert List.last(projected) == {"02050-0", 2_050}

    source =
      File.read!(
        Path.expand(
          "../../../../lib/ferricstore/flow/lmdb_writer/projection_ops.ex",
          __DIR__
        )
      )

    assert source =~ "@history_projection_page_size"
    refute source =~ ~r/history_project_from_native_entries[\s\S]*?\n\s*:all\n/
  end

  test "existing history locator reads distinguish absence from corruption and I/O failure" do
    encoded_without_location =
      Ferricstore.Flow.LMDB.encode_history_index_value("event-1", 100, "compound-1", 0)

    encoded_with_location =
      Ferricstore.Flow.LMDB.encode_history_index_value(
        "event-1",
        100,
        "compound-1",
        0,
        {:flow_history, 7},
        42,
        128
      )

    assert {:ok, nil} =
             ProjectionOps.__existing_history_index_location_result_for_test__(:not_found)

    assert {:ok, nil} =
             ProjectionOps.__existing_history_index_location_result_for_test__({
               :ok,
               encoded_without_location
             })

    assert {:ok, {{:flow_history, 7}, 42, 128}} =
             ProjectionOps.__existing_history_index_location_result_for_test__({
               :ok,
               encoded_with_location
             })

    assert {:error, :invalid_history_index_value} =
             ProjectionOps.__existing_history_index_location_result_for_test__({:ok, "corrupt"})

    assert {:error, :busy} =
             ProjectionOps.__existing_history_index_location_result_for_test__({:error, :busy})
  end

  test "stale terminal reverse reads propagate I/O failures and clean dangling rows" do
    state_key = "flow-state-key"
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

    acc = %{
      ops: [],
      counts: %{},
      terminal_values: %{},
      active_reverse_values: %{},
      terminal_count_inits: MapSet.new()
    }

    assert {:error, :busy} =
             ProjectionOps.__maybe_expand_stale_terminal_delete_for_test__(
               "unused",
               state_key,
               acc,
               fn ^reverse_key -> {:error, :busy} end
             )

    assert {:ok, cleaned} =
             ProjectionOps.__maybe_expand_stale_terminal_delete_for_test__(
               "unused",
               state_key,
               acc,
               fn
                 ^reverse_key -> {:ok, "terminal-key"}
                 "terminal-key" -> :not_found
               end
             )

    assert cleaned.ops == [
             {:delete, reverse_key},
             {:compare, reverse_key, "terminal-key"}
           ]
  end

  test "stale active reverse corruption aborts projection without partial cleanup" do
    state_key = "flow-state-key"

    acc = %{
      ops: [],
      counts: %{},
      terminal_values: %{},
      active_reverse_values: %{state_key => "corrupt"},
      terminal_count_inits: MapSet.new()
    }

    assert {:error, :invalid_active_index_reverse} =
             ProjectionOps.maybe_expand_stale_active_delete("unused", state_key, acc)
  end

  test "stale persisted active reverses cannot delete another state's active row" do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_projection_active_owner_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    state_key = "state-a"
    active_key = Ferricstore.Flow.LMDB.active_index_key("active-index", "flow-b", 10)

    active_value =
      Ferricstore.Flow.LMDB.encode_active_index_value(
        "active-index",
        "flow-b",
        10,
        0,
        "state-b"
      )

    reverse_key = Ferricstore.Flow.LMDB.active_by_state_key_key(state_key)
    reverse_value = Ferricstore.Flow.LMDB.encode_active_index_reverse_value([active_key])

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(path, [
               {:put, reverse_key, reverse_value},
               {:put, active_key, active_value}
             ])

    acc = %{
      ops: [],
      counts: %{},
      terminal_values: %{},
      active_reverse_values: %{},
      terminal_count_inits: MapSet.new()
    }

    assert {:error, {:active_index_reverse_state_mismatch, ^active_key}} =
             ProjectionOps.maybe_expand_stale_active_delete(path, state_key, acc)

    assert {:ok, ^reverse_value} = Ferricstore.Flow.LMDB.get(path, reverse_key)
    assert {:ok, ^active_value} = Ferricstore.Flow.LMDB.get(path, active_key)
  end

  test "stale active cleanup emits one atomic delete plan" do
    path = tmp_lmdb_path("stale_active_single_delete_plan")
    state_key = "state-a"
    active_key = Ferricstore.Flow.LMDB.active_index_key("active-index", "flow-a", 10)
    reverse_key = Ferricstore.Flow.LMDB.active_by_state_key_key(state_key)
    reverse_value = Ferricstore.Flow.LMDB.encode_active_index_reverse_value([active_key])

    active_value =
      Ferricstore.Flow.LMDB.encode_active_index_value(
        "active-index",
        "flow-a",
        10,
        0,
        state_key
      )

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(path, [
               {:put, reverse_key, reverse_value},
               {:put, active_key, active_value}
             ])

    assert {:ok, expected_ops} =
             Ferricstore.Flow.LMDB.active_index_delete_ops_result(path, state_key)

    acc = %{
      ops: [],
      counts: %{},
      terminal_values: %{},
      active_reverse_values: %{},
      terminal_count_inits: MapSet.new()
    }

    assert {:ok, cleaned} =
             ProjectionOps.maybe_expand_stale_active_delete(path, state_key, acc)

    assert cleaned.ops == Enum.reverse(expected_ops)
  end

  test "old projected flow reads do not turn corruption or I/O failure into absence" do
    record = %{
      id: "projection-flow",
      type: "projection-type",
      state: "running",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 100,
      updated_at_ms: 100,
      next_run_at_ms: nil,
      priority: 0,
      ttl_ms: nil,
      history_hot_max_events: nil,
      history_max_events: nil,
      retention_ttl_ms: nil,
      max_active_ms: nil,
      terminal_retention_until_ms: nil,
      partition_key: nil,
      payload_ref: nil,
      parent_flow_id: nil,
      parent_partition_key: nil,
      root_flow_id: "projection-flow",
      correlation_id: nil,
      result_ref: nil,
      error_ref: nil,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0,
      run_state: nil,
      child_groups: %{}
    }

    envelope =
      record
      |> Ferricstore.Flow.encode_record()
      |> Ferricstore.Flow.LMDB.encode_value(0)

    assert {:ok, %{id: "projection-flow"}} =
             ProjectionOps.__old_projected_flow_record_result_for_test__({:ok, envelope})

    assert {:ok, nil} = ProjectionOps.__old_projected_flow_record_result_for_test__(:not_found)

    assert {:error, :invalid_projected_flow_record} =
             ProjectionOps.__old_projected_flow_record_result_for_test__({:ok, "corrupt"})

    assert {:error, :busy} =
             ProjectionOps.__old_projected_flow_record_result_for_test__({:error, :busy})
  end

  test "terminal count reads and repairs fail closed on corrupt or unavailable data" do
    count_key = Ferricstore.Flow.LMDB.terminal_count_key("state-index")

    acc = %{
      ops: [],
      counts: %{},
      terminal_values: %{},
      active_reverse_values: %{},
      terminal_count_inits: MapSet.new()
    }

    assert {:error, :invalid_terminal_count_value} =
             ProjectionOps.__terminal_count_result_for_test__({:ok, "corrupt"}, count_key, acc)

    assert {:error, :busy} =
             ProjectionOps.__terminal_count_result_for_test__({:error, :busy}, count_key, acc)

    assert {:error, :busy} =
             ProjectionOps.__terminal_prefix_count_result_for_test__(
               {:error, :busy},
               count_key,
               acc
             )

    assert {:ok, 3, repaired} =
             ProjectionOps.__terminal_prefix_count_result_for_test__({:ok, 3}, count_key, acc)

    assert repaired.counts[count_key] == 3
  end

  test "terminal put plans abort when the exact counter changes before commit" do
    path = tmp_lmdb_path("terminal_put_cas")
    state_index_key = "state:completed"
    terminal_key = LMDB.terminal_index_key(state_index_key, "flow-1", 10)
    count_key = LMDB.terminal_count_key(state_index_key)
    state_key = "state-key"
    count_value = LMDB.encode_count(1)

    value =
      LMDB.encode_terminal_index_value("flow-1", 10, 0, state_key, count_key)

    assert :ok = LMDB.write_batch(path, [{:put, count_key, count_value}])

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(
               %{path: path, terminal_count_inits: MapSet.new()},
               [{:terminal_put, terminal_key, value, state_key, count_key}]
             )

    assert {:compare_missing, terminal_key} in ops
    assert {:compare, count_key, count_value} in ops

    replacement_count = LMDB.encode_count(3)
    assert :ok = LMDB.write_batch(path, [{:put, count_key, replacement_count}])

    assert {:error, {:compare_failed, ^count_key}} = LMDB.write_batch(path, ops)
    assert LMDB.get(path, terminal_key) == :not_found
    assert {:ok, ^replacement_count} = LMDB.get(path, count_key)
  end

  test "terminal expansion uses a bounded atomic-write marker instead of count-cache state" do
    path = tmp_lmdb_path("terminal_atomic_marker")
    state_index_key = "state:completed"
    terminal_key = LMDB.terminal_index_key(state_index_key, "flow-1", 10)
    count_key = LMDB.terminal_count_key(state_index_key)
    value = LMDB.encode_terminal_index_value("flow-1", 10, 0, nil, count_key)
    state = %{path: path, terminal_count_inits: MapSet.new()}

    assert {:ok, _ops, terminal_state} =
             ProjectionOps.expand_ops(
               state,
               [{:terminal_put, terminal_key, value, nil, count_key}]
             )

    assert terminal_state.terminal_atomic_write?
    refute Map.has_key?(terminal_state, :terminal_count_cache)

    assert {:ok, _ops, nonterminal_state} =
             ProjectionOps.expand_ops(state, [{:put, "ordinary-key", "ordinary-value"}])

    refute nonterminal_state.terminal_atomic_write?
    refute Map.has_key?(nonterminal_state, :terminal_count_cache)
  end

  test "terminal projection observes count initialization queued earlier in the same batch" do
    path = tmp_lmdb_path("terminal_count_init_batch")
    type = "jobs"
    partition_key = "partition-1"
    state_key = Ferricstore.Flow.Keys.state_key("flow-1", partition_key)
    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, "completed", partition_key)
    terminal_key = LMDB.terminal_index_key(state_index_key, "flow-1", 20)
    count_key = LMDB.terminal_count_key(state_index_key)

    active_value =
      active_flow_record("flow-1", type, partition_key)
      |> Ferricstore.Flow.encode_record()
      |> LMDB.encode_value(0)

    terminal_value =
      LMDB.encode_terminal_index_value("flow-1", 20, 0, state_key, count_key)

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(
               %{path: path, terminal_count_inits: MapSet.new()},
               [
                 {:put, state_key, active_value},
                 {:terminal_put, terminal_key, terminal_value, state_key, count_key}
               ]
             )

    assert :ok = LMDB.write_batch(path, ops)
    assert {:ok, 1} = LMDB.get(path, count_key) |> decode_count_result()
    assert {:ok, ^terminal_value} = LMDB.get(path, terminal_key)
  end

  test "terminal puts reject mismatched prepared-command identity" do
    path = tmp_lmdb_path("terminal_put_identity")
    state_index_key = "state:completed"
    terminal_key = LMDB.terminal_index_key(state_index_key, "flow-1", 10)
    count_key = LMDB.terminal_count_key(state_index_key)
    state_key = "state-key"

    mismatched_values = [
      LMDB.encode_terminal_index_value("other-flow", 10, 0, state_key, count_key),
      LMDB.encode_terminal_index_value("flow-1", 10, 0, "other-state-key", count_key),
      LMDB.encode_terminal_index_value("flow-1", 10, 0, state_key, "other-count-key")
    ]

    Enum.each(mismatched_values, fn value ->
      assert {:error, :invalid_terminal_index_value} =
               ProjectionOps.expand_ops(
                 %{path: path, terminal_count_inits: MapSet.new()},
                 [{:terminal_put, terminal_key, value, state_key, count_key}]
               )
    end)

    assert LMDB.get(path, terminal_key) == :not_found
    assert LMDB.get(path, count_key) == :not_found
  end

  test "terminal deletes reject persisted rows owned by another command identity" do
    path = tmp_lmdb_path("terminal_delete_identity")
    state_index_key = "state:completed"
    terminal_key = LMDB.terminal_index_key(state_index_key, "flow-1", 10)
    count_key = LMDB.terminal_count_key(state_index_key)
    state_key = "state-key"

    mismatched_values = [
      LMDB.encode_terminal_index_value("other-flow", 10, 0, state_key, count_key),
      LMDB.encode_terminal_index_value("flow-1", 10, 0, "other-state-key", count_key),
      LMDB.encode_terminal_index_value("flow-1", 10, 0, state_key, "other-count-key")
    ]

    Enum.each(mismatched_values, fn value ->
      assert :ok =
               LMDB.write_batch(path, [
                 {:put, terminal_key, value},
                 {:put, count_key, LMDB.encode_count(1)}
               ])

      assert {:error, :invalid_terminal_index_value} =
               ProjectionOps.expand_ops(
                 %{path: path, terminal_count_inits: MapSet.new()},
                 [{:terminal_delete, terminal_key, state_key, count_key}]
               )

      assert {:ok, ^value} = LMDB.get(path, terminal_key)
      assert {:ok, 1} = LMDB.get(path, count_key) |> decode_count_result()
    end)
  end

  test "terminal puts serialize ownership across independent count buckets" do
    path = tmp_lmdb_path("terminal_put_reverse_cas")
    state_key = "state-key"
    reverse_key = LMDB.terminal_by_state_key_key(state_key)

    completed_index_key = "state:completed"
    completed_terminal_key = LMDB.terminal_index_key(completed_index_key, "flow-1", 10)
    completed_count_key = LMDB.terminal_count_key(completed_index_key)

    completed_value =
      LMDB.encode_terminal_index_value(
        "flow-1",
        10,
        0,
        state_key,
        completed_count_key
      )

    failed_index_key = "state:failed"
    failed_terminal_key = LMDB.terminal_index_key(failed_index_key, "flow-1", 20)
    failed_count_key = LMDB.terminal_count_key(failed_index_key)

    failed_value =
      LMDB.encode_terminal_index_value("flow-1", 20, 0, state_key, failed_count_key)

    assert {:ok, completed_ops, _state} =
             ProjectionOps.expand_ops(
               %{path: path, terminal_count_inits: MapSet.new()},
               [
                 {:terminal_put, completed_terminal_key, completed_value, state_key,
                  completed_count_key}
               ]
             )

    assert {:ok, failed_ops, _state} =
             ProjectionOps.expand_ops(
               %{path: path, terminal_count_inits: MapSet.new()},
               [
                 {:terminal_put, failed_terminal_key, failed_value, state_key, failed_count_key}
               ]
             )

    assert {:compare_missing, reverse_key} in completed_ops
    assert {:compare_missing, reverse_key} in failed_ops
    assert :ok = LMDB.write_batch(path, completed_ops)

    assert {:error, {:compare_failed, ^reverse_key}} = LMDB.write_batch(path, failed_ops)
    assert {:ok, ^completed_terminal_key} = LMDB.get(path, reverse_key)
    assert {:ok, ^completed_value} = LMDB.get(path, completed_terminal_key)
    assert LMDB.get(path, failed_terminal_key) == :not_found
    assert {:ok, 1} = LMDB.get(path, completed_count_key) |> decode_count_result()
    assert LMDB.get(path, failed_count_key) == :not_found
  end

  test "terminal delete plans preserve a concurrently replaced reverse owner" do
    path = tmp_lmdb_path("terminal_delete_reverse_cas")
    state_index_key = "state:completed"
    terminal_key = LMDB.terminal_index_key(state_index_key, "flow-1", 10)
    replacement_terminal_key = LMDB.terminal_index_key("state:failed", "flow-1", 20)
    count_key = LMDB.terminal_count_key(state_index_key)
    state_key = "state-key"
    reverse_key = LMDB.terminal_by_state_key_key(state_key)
    value = LMDB.encode_terminal_index_value("flow-1", 10, 0, state_key, count_key)
    count_value = LMDB.encode_count(1)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, terminal_key, value},
               {:put, reverse_key, terminal_key},
               {:put, count_key, count_value}
             ])

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(
               %{path: path, terminal_count_inits: MapSet.new()},
               [{:terminal_delete, terminal_key, state_key, count_key}]
             )

    assert {:compare, reverse_key, terminal_key} in ops
    assert :ok = LMDB.write_batch(path, [{:put, reverse_key, replacement_terminal_key}])

    assert {:error, {:compare_failed, ^reverse_key}} = LMDB.write_batch(path, ops)
    assert {:ok, ^value} = LMDB.get(path, terminal_key)
    assert {:ok, ^replacement_terminal_key} = LMDB.get(path, reverse_key)
    assert {:ok, ^count_value} = LMDB.get(path, count_key)
  end

  test "terminal delete repairs an already missing reverse pointer atomically" do
    path = tmp_lmdb_path("terminal_delete_missing_reverse")
    state_index_key = "state:completed"
    terminal_key = LMDB.terminal_index_key(state_index_key, "flow-1", 10)
    count_key = LMDB.terminal_count_key(state_index_key)
    state_key = "state-key"
    reverse_key = LMDB.terminal_by_state_key_key(state_key)
    value = LMDB.encode_terminal_index_value("flow-1", 10, 0, state_key, count_key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, terminal_key, value},
               {:put, count_key, LMDB.encode_count(1)}
             ])

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(
               %{path: path, terminal_count_inits: MapSet.new()},
               [{:terminal_delete, terminal_key, state_key, count_key}]
             )

    assert {:compare_missing, reverse_key} in ops
    assert :ok = LMDB.write_batch(path, ops)
    assert LMDB.get(path, terminal_key) == :not_found
    assert LMDB.get(path, reverse_key) == :not_found
    assert {:ok, 0} = LMDB.get(path, count_key) |> decode_count_result()
  end

  test "terminal reprojection replaces the previous timestamped row without inflating counts" do
    path = tmp_lmdb_path("terminal_reproject")
    type = "jobs"
    state = "completed"
    partition_key = "partition-1"
    id = "flow-1"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, state, partition_key)
    count_key = LMDB.terminal_count_key(state_index_key)
    old_terminal_key = LMDB.terminal_index_key(state_index_key, id, 10)
    new_terminal_key = LMDB.terminal_index_key(state_index_key, id, 20)
    reverse_key = LMDB.terminal_by_state_key_key(state_key)

    old_value =
      LMDB.encode_terminal_index_value(id, 10, 0, state_key, count_key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, old_terminal_key, old_value},
               {:put, reverse_key, old_terminal_key},
               {:put, count_key, LMDB.encode_count(1)}
             ])

    terminal_project =
      {:terminal_project, id, type, state, partition_key, 20, state_key, 0, nil, id, nil, %{}, [],
       %{}, nil}

    assert {:ok, ops, _state} =
             ProjectionOps.expand_ops(
               %{path: path, terminal_count_inits: MapSet.new()},
               [terminal_project]
             )

    assert :ok = LMDB.write_batch(path, ops)
    assert LMDB.get(path, old_terminal_key) == :not_found
    assert {:ok, _new_value} = LMDB.get(path, new_terminal_key)
    assert {:ok, ^new_terminal_key} = LMDB.get(path, reverse_key)
    assert {:ok, 1} = LMDB.get(path, count_key) |> decode_count_result()
  end

  test "malformed flow state cannot be acknowledged as a successful projection" do
    acc = %{
      ops: [],
      counts: %{},
      terminal_values: %{},
      active_reverse_values: %{},
      terminal_count_inits: MapSet.new()
    }

    assert {:error, :invalid_flow_state_projection} =
             ProjectionOps.expand_flow_state_value(
               "unused",
               "flow-state-key",
               "not-a-flow-record",
               0,
               acc
             )
  end

  test "WARaft source locators materialize blob-backed Flow records" do
    root = tmp_lmdb_path("waraft_blob_source")
    keydir = :ets.new(:projection_blob_source, [:set, :public])
    state_key = Ferricstore.Flow.Keys.state_key("blob-source-flow", "blob-source-partition")

    encoded_record =
      active_flow_record("blob-source-flow", "jobs", "blob-source-partition")
      |> Map.put(:correlation_id, :binary.copy("correlation-", 16))
      |> Ferricstore.Flow.encode_record()

    assert byte_size(encoded_record) > 64
    assert {:ok, ref} = Ferricstore.Store.BlobStore.put(root, 0, encoded_record)
    encoded_ref = Ferricstore.Store.BlobRef.encode!(ref)

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(root, 0, 1, [
               {state_key, encoded_ref, 0}
             ])

    :ets.insert(
      keydir,
      {state_key, nil, 0, 0, {:waraft_apply_projection, 1}, 0, byte_size(encoded_ref)}
    )

    ctx = %{
      data_dir: root,
      keydir_refs: {keydir},
      blob_side_channel_threshold_bytes: 64
    }

    state = %{
      instance_ctx: ctx,
      shard_index: 0,
      shard_data_path: Ferricstore.DataDir.shard_data_path(root, 0)
    }

    on_exit(fn ->
      Ferricstore.Raft.WARaftSegmentReader.clear_apply_projection_cache(root, 0)
      File.rm_rf!(root)
    end)

    assert {:ok, ^encoded_record, 0} = ProjectionOps.read_source_value(state, state_key)
  end

  test "source pending retry configuration has a bounded combined wait budget" do
    assert ProjectionOps.__normalize_source_pending_config_for_test__("many", :slow) == {100, 1}

    assert ProjectionOps.__normalize_source_pending_config_for_test__(100_000, 100_000) ==
             {50, 100}

    assert ProjectionOps.__normalize_source_pending_config_for_test__(2_000, 0) ==
             {1_000, 0}

    assert ProjectionOps.__normalize_source_pending_config_for_test__(-1, -1) == {100, 1}
  end

  defp tmp_lmdb_path(prefix) do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_projection_#{prefix}_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)
    path
  end

  defp decode_count_result({:ok, value}) do
    case LMDB.decode_count(value) do
      {:ok, count} -> {:ok, count}
      :error -> :error
    end
  end

  defp active_flow_record(id, type, partition_key) do
    %{
      id: id,
      type: type,
      state: "running",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 10,
      updated_at_ms: 10,
      next_run_at_ms: 10,
      priority: 0,
      ttl_ms: nil,
      history_hot_max_events: nil,
      history_max_events: nil,
      retention_ttl_ms: nil,
      max_active_ms: nil,
      terminal_retention_until_ms: nil,
      partition_key: partition_key,
      payload_ref: nil,
      parent_flow_id: nil,
      parent_partition_key: nil,
      root_flow_id: id,
      correlation_id: nil,
      result_ref: nil,
      error_ref: nil,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0,
      run_state: nil,
      child_groups: %{}
    }
  end
end
