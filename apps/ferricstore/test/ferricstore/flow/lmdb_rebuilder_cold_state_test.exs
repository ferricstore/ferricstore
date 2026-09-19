defmodule Ferricstore.Flow.LMDBRebuilder.ColdStateTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, LMDB, LMDBRebuilder, Locator}
  alias Ferricstore.Flow.Query.QueryRowCodec
  alias Ferricstore.Flow.LMDBRebuilder.ColdState
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Store.LFU

  test "malformed source state is counted as a rebuild read failure" do
    Process.put(:flow_lmdb_rebuild_cold_read_errors, 0)

    on_exit(fn -> Process.delete(:flow_lmdb_rebuild_cold_read_errors) end)

    assert [] = ColdState.decode_state_record("flow-state", "corrupt", 0, nil, nil)
    assert Process.get(:flow_lmdb_rebuild_cold_read_errors) == 1
  end

  test "active index rebuild rejects a partial decode" do
    keydir = :ets.new(:flow_active_index_partial_decode, [:set])
    state_key = Keys.state_key("partial-active-index")

    :ets.insert(
      keydir,
      {state_key, "corrupt", 0, LFU.initial(), :memory, 0, byte_size("corrupt")}
    )

    assert {:error, {:cold_read_errors, 1}} =
             LMDBRebuilder.rebuild_active_indexes_from_keydir(
               System.tmp_dir!(),
               keydir,
               0,
               nil,
               nil,
               nil,
               nil,
               nil
             )
  end

  @tag :flow_deleted_marker
  test "active index rebuild ignores a Flow deletion marker" do
    keydir = :ets.new(:flow_active_index_deleted_marker, [:set])
    state_key = Keys.state_key("deleted-active-index")

    :ets.insert(keydir, {state_key, nil, 0, :flow_state_deleted, :deleted, 0, 0})

    assert :ok =
             LMDBRebuilder.rebuild_active_indexes_from_keydir(
               System.tmp_dir!(),
               keydir,
               0,
               nil,
               nil,
               nil,
               nil,
               nil
             )
  end

  test "cached WARaft state is pinned and physicalized before rebuilding query rows" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "flow-rebuilder-hot-waraft-#{System.unique_integer([:positive])}"
      )

    index = System.unique_integer([:positive, :monotonic])
    file_id = {:waraft_apply_projection, index}
    state_key = Keys.state_key("hot-waraft-source")

    record = %{
      id: "hot-waraft-source",
      type: "job",
      state: "queued",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: 2,
      next_run_at_ms: 3,
      priority: 0,
      partition_key: nil,
      root_flow_id: "hot-waraft-source"
    }

    encoded = Ferricstore.Flow.encode_record(record)

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {state_key, encoded, 0}
             ])

    entry =
      {state_key, encoded, 0, LFU.initial(), file_id, 0, byte_size(encoded)}

    assert [{^state_key, ^encoded, 0, rebuilt, %Locator{} = locator}] =
             ColdState.read_and_decode([entry], data_dir, 0, %{data_dir: data_dir})

    assert rebuilt.id == record.id
    assert rebuilt.version == record.version
    assert Locator.hydration_ready?(locator)

    assert {:ok, physical_location} =
             WARaftSegmentReader.physical_location(%{data_dir: data_dir}, 0, file_id)

    assert physical_location ==
             {locator.segment_generation, locator.offset, locator.frame_size}

    assert WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 0
  end

  test "metadata-only cleanup decodes a hot WARaft row without physical context" do
    state_key = Keys.state_key("hot-waraft-metadata-only")

    record = %{
      id: "hot-waraft-metadata-only",
      type: "job",
      state: "completed",
      version: 2,
      attempts: 1,
      fencing_token: 1,
      created_at_ms: 1,
      updated_at_ms: 2,
      next_run_at_ms: 0,
      priority: 0,
      partition_key: nil,
      root_flow_id: "hot-waraft-metadata-only"
    }

    encoded = Ferricstore.Flow.encode_record(record)

    entry =
      {state_key, encoded, 0, LFU.initial(), {:waraft_apply_projection, 17}, 0,
       byte_size(encoded)}

    Process.put(:flow_lmdb_rebuild_cold_read_errors, 0)
    on_exit(fn -> Process.delete(:flow_lmdb_rebuild_cold_read_errors) end)

    assert [{^state_key, ^encoded, 0, rebuilt, %Locator{}}] =
             ColdState.read_and_decode([entry], System.tmp_dir!())

    assert rebuilt.id == record.id
    assert Process.get(:flow_lmdb_rebuild_cold_read_errors) == 0
  end

  test "reconcile ignores a WARaft source replaced while durability is checked" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "flow-rebuilder-replaced-waraft-#{System.unique_integer([:positive])}"
      )

    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    keydir = :ets.new(:flow_rebuilder_replaced_waraft_keydir, [:set])
    state_key = Keys.state_key("replaced-waraft-source")
    projection_index = 17

    :ets.insert(
      keydir,
      {state_key, nil, 0, LFU.initial(), {:waraft_apply_projection, projection_index}, 0, 16}
    )

    Process.put(
      :ferricstore_waraft_apply_projection_disk_read_hook,
      fn _root, ^projection_index, :latest ->
        :ets.insert(keydir, {state_key, nil, 0, :flow_state_deleted, :deleted, 0, 0})
        :ok
      end
    )

    on_exit(fn ->
      Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               %{data_dir: data_dir},
               nil,
               nil,
               nil,
               nil
             )
  end

  test "reconcile keeps an unchanged HOT WARaft source failure unhealthy" do
    data_dir = temp_data_dir("hot-current-source-failure")
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    keydir = :ets.new(:flow_rebuilder_hot_current_source_keydir, [:set])
    state_key = Keys.state_key("hot-current-source-failure")
    record = test_record("hot-current-source-failure", "queued", 1)
    encoded = Ferricstore.Flow.encode_record(record)
    index = 101

    :ets.insert(
      keydir,
      {state_key, encoded, 0, LFU.initial(), {:waraft_apply_projection, index}, 0,
       byte_size(encoded)}
    )

    on_exit(fn -> File.rm_rf!(data_dir) end)

    assert {:error,
            {:flow_lmdb_reconcile_unhealthy,
             %{cold_read_errors: 1, history_lmdb_errors: 0, lmdb_errors: 0}}} =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               %{data_dir: data_dir},
               nil,
               nil,
               nil,
               nil
             )
  end

  test "reconcile keeps an unchanged COLD WARaft source failure unhealthy" do
    data_dir = temp_data_dir("cold-current-source-failure")
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    keydir = :ets.new(:flow_rebuilder_cold_current_source_keydir, [:set])
    state_key = Keys.state_key("cold-current-source-failure")
    index = 102

    :ets.insert(
      keydir,
      {state_key, nil, 0, LFU.initial(), {:waraft_apply_projection, index}, 0, 16}
    )

    on_exit(fn -> File.rm_rf!(data_dir) end)

    assert {:error,
            {:flow_lmdb_reconcile_unhealthy,
             %{cold_read_errors: 1, history_lmdb_errors: 0, lmdb_errors: 0}}} =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               %{data_dir: data_dir},
               nil,
               nil,
               nil,
               nil
             )
  end

  test "reconcile refreshes and projects a newer HOT WARaft source" do
    data_dir = temp_data_dir("hot-source-replacement")
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = LMDB.path(shard_path)
    keydir = :ets.new(:flow_rebuilder_hot_source_replacement_keydir, [:set])
    state_key = Keys.state_key("hot-source-replacement", "tenant-reconcile")
    old_record = test_record("hot-source-replacement", "queued", 1, "tenant-reconcile")
    new_record = %{old_record | state: "ready", version: 2, updated_at_ms: 3}
    old_encoded = Ferricstore.Flow.encode_record(old_record)
    new_encoded = Ferricstore.Flow.encode_record(new_record)
    old_index = 103
    new_index = 104

    old_entry =
      {state_key, old_encoded, 0, LFU.initial(), {:waraft_apply_projection, old_index}, 0,
       byte_size(old_encoded)}

    new_entry =
      {state_key, new_encoded, 0, LFU.initial(), {:waraft_apply_projection, new_index}, 0,
       byte_size(new_encoded)}

    :ets.insert(keydir, old_entry)

    Process.put(
      :ferricstore_waraft_apply_projection_disk_read_hook,
      fn _root, index, :latest ->
        if index == old_index do
          true = :ets.insert(keydir, new_entry)

          assert :ok =
                   WARaftSegmentReader.put_apply_projection(data_dir, 0, new_index, [
                     {state_key, new_encoded, 0}
                   ])
        end

        :ok
      end
    )

    on_exit(fn ->
      Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               %{data_dir: data_dir},
               nil,
               nil,
               nil,
               nil
             )

    assert {:ok, query_row} = LMDB.get(lmdb_path, state_key)

    assert {:ok, %{record: %{version: 2, state: "ready"}}} =
             QueryRowCodec.decode(query_row, state_key)
  end

  test "reconcile refreshes and projects a newer COLD WARaft source" do
    data_dir = temp_data_dir("cold-source-replacement")
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = LMDB.path(shard_path)
    keydir = :ets.new(:flow_rebuilder_cold_source_replacement_keydir, [:set])
    state_key = Keys.state_key("cold-source-replacement", "tenant-reconcile")
    old_record = test_record("cold-source-replacement", "queued", 1, "tenant-reconcile")
    new_record = %{old_record | state: "ready", version: 2, updated_at_ms: 3}
    old_encoded = Ferricstore.Flow.encode_record(old_record)
    new_encoded = Ferricstore.Flow.encode_record(new_record)
    old_index = 109
    new_index = 110

    old_entry =
      {
        state_key,
        nil,
        0,
        LFU.initial(),
        {:waraft_apply_projection, old_index},
        0,
        byte_size(old_encoded)
      }

    new_entry =
      {
        state_key,
        nil,
        0,
        LFU.initial(),
        {:waraft_apply_projection, new_index},
        0,
        byte_size(new_encoded)
      }

    :ets.insert(keydir, old_entry)

    Process.put(
      :ferricstore_waraft_apply_projection_disk_read_hook,
      fn _root, index, :latest ->
        if index == old_index do
          true = :ets.insert(keydir, new_entry)

          assert :ok =
                   WARaftSegmentReader.put_apply_projection(data_dir, 0, new_index, [
                     {state_key, new_encoded, 0}
                   ])
        end

        :ok
      end
    )

    on_exit(fn ->
      Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               %{data_dir: data_dir},
               nil,
               nil,
               nil,
               nil
             )

    assert {:ok, query_row} = LMDB.get(lmdb_path, state_key)

    assert {:ok, %{record: %{version: 2, state: "ready"}}} =
             QueryRowCodec.decode(query_row, state_key)
  end

  test "reconcile refreshes a tombstone and removes its existing LMDB row" do
    data_dir = temp_data_dir("hot-source-tombstone")
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = LMDB.path(shard_path)
    keydir = :ets.new(:flow_rebuilder_hot_source_tombstone_keydir, [:set])
    state_key = Keys.state_key("hot-source-tombstone", "tenant-reconcile")
    record = test_record("hot-source-tombstone", "queued", 1, "tenant-reconcile")
    encoded = Ferricstore.Flow.encode_record(record)
    index = 105

    entry =
      {state_key, encoded, 0, LFU.initial(), {:waraft_apply_projection, index}, 0,
       byte_size(encoded)}

    tombstone = {state_key, nil, 0, :flow_state_deleted, :deleted, 0, 0}
    :ets.insert(keydir, entry)

    locator =
      Locator.new!(
        flow_id: record.id,
        kind: :state,
        version: record.version,
        raft_index: record.version,
        file_id: 0,
        offset: 0,
        value_size: byte_size(encoded),
        checksum: :crypto.hash(:sha256, encoded),
        expire_at_ms: 0
      )

    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 0)
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, state_key, query_row}])

    Process.put(
      :ferricstore_waraft_apply_projection_disk_read_hook,
      fn _root, ^index, :latest ->
        true = :ets.insert(keydir, tombstone)
        :ok
      end
    )

    on_exit(fn ->
      Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               %{data_dir: data_dir},
               nil,
               nil,
               nil,
               nil
             )

    assert :not_found = LMDB.get(lmdb_path, state_key)
  end

  test "reconcile bounds repeated source changes and stays unhealthy" do
    data_dir = temp_data_dir("hot-source-churn")
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    keydir = :ets.new(:flow_rebuilder_hot_source_churn_keydir, [:set])
    state_key = Keys.state_key("hot-source-churn", "tenant-reconcile")
    record = test_record("hot-source-churn", "queued", 1, "tenant-reconcile")
    encoded = Ferricstore.Flow.encode_record(record)
    index = 106

    :ets.insert(
      keydir,
      {state_key, encoded, 0, LFU.initial(), {:waraft_apply_projection, index}, 0,
       byte_size(encoded)}
    )

    Process.put(:flow_rebuilder_source_churn_calls, 0)

    Process.put(
      :ferricstore_waraft_apply_projection_disk_read_hook,
      fn _root, current_index, :latest ->
        Process.put(
          :flow_rebuilder_source_churn_calls,
          Process.get(:flow_rebuilder_source_churn_calls, 0) + 1
        )

        next_index = current_index + 1

        :ets.insert(
          keydir,
          {state_key, encoded, 0, LFU.initial(), {:waraft_apply_projection, next_index}, 0,
           byte_size(encoded)}
        )

        :ok
      end
    )

    on_exit(fn ->
      Process.delete(:flow_rebuilder_source_churn_calls)
      Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
      File.rm_rf!(data_dir)
    end)

    assert {:error,
            {:flow_lmdb_reconcile_unhealthy,
             %{cold_read_errors: 0, history_lmdb_errors: 0, lmdb_errors: 1}}} =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               %{data_dir: data_dir},
               nil,
               nil,
               nil,
               nil
             )

    assert Process.get(:flow_rebuilder_source_churn_calls) == 3
  end

  test "reconcile retries a changed member of a shared WARaft group without poisoning siblings" do
    data_dir = temp_data_dir("hot-shared-source-replacement")
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = LMDB.path(shard_path)
    keydir = :ets.new(:flow_rebuilder_hot_shared_source_keydir, [:set])
    changed_key = Keys.state_key("hot-shared-changed", "tenant-reconcile")
    stable_key = Keys.state_key("hot-shared-stable", "tenant-reconcile")
    changed_record = test_record("hot-shared-changed", "queued", 1, "tenant-reconcile")
    stable_record = test_record("hot-shared-stable", "queued", 1, "tenant-reconcile")
    newer_record = %{changed_record | state: "ready", version: 2, updated_at_ms: 3}
    changed_encoded = Ferricstore.Flow.encode_record(changed_record)
    stable_encoded = Ferricstore.Flow.encode_record(stable_record)
    newer_encoded = Ferricstore.Flow.encode_record(newer_record)
    old_index = 107
    new_index = 108

    old_changed_entry =
      {changed_key, changed_encoded, 0, LFU.initial(), {:waraft_apply_projection, old_index}, 0,
       byte_size(changed_encoded)}

    stable_entry =
      {stable_key, stable_encoded, 0, LFU.initial(), {:waraft_apply_projection, old_index}, 0,
       byte_size(stable_encoded)}

    new_changed_entry =
      {changed_key, newer_encoded, 0, LFU.initial(), {:waraft_apply_projection, new_index}, 0,
       byte_size(newer_encoded)}

    :ets.insert(keydir, [old_changed_entry, stable_entry])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, old_index, [
               {stable_key, stable_encoded, 0}
             ])

    Process.put(
      :ferricstore_waraft_apply_projection_disk_read_hook,
      fn _root, index, :latest ->
        if index == old_index do
          true = :ets.insert(keydir, new_changed_entry)

          assert :ok =
                   WARaftSegmentReader.put_apply_projection(data_dir, 0, new_index, [
                     {changed_key, newer_encoded, 0}
                   ])
        end

        :ok
      end
    )

    on_exit(fn ->
      Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               %{data_dir: data_dir},
               nil,
               nil,
               nil,
               nil
             )

    assert {:ok, changed_query_row} = LMDB.get(lmdb_path, changed_key)

    assert {:ok, %{record: %{version: 2, state: "ready"}}} =
             QueryRowCodec.decode(changed_query_row, changed_key)

    assert {:ok, stable_query_row} = LMDB.get(lmdb_path, stable_key)

    assert {:ok, %{record: %{version: 1, state: "queued"}}} =
             QueryRowCodec.decode(stable_query_row, stable_key)
  end

  defp temp_data_dir(label),
    do:
      Path.join(
        System.tmp_dir!(),
        "flow-rebuilder-#{label}-#{System.unique_integer([:positive])}"
      )

  defp test_record(id, state, version, partition_key \\ nil) do
    %{
      id: id,
      type: "job",
      state: state,
      version: version,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: version + 1,
      next_run_at_ms: 0,
      priority: 0,
      partition_key: partition_key,
      state_enter_seq: version,
      root_flow_id: id
    }
  end
end
