defmodule Ferricstore.Flow.HistoryProjector.RecoveryTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryProjector.Recovery
  alias Ferricstore.Flow.HistoryProjector.Storage
  alias Ferricstore.Flow.{Keys, LMDB, Locator, StorageScope}
  alias Ferricstore.Flow.Query.QueryRowCodec
  alias Ferricstore.Store.BlobRef

  test "loads history routing metadata directly from a query row" do
    shard_data_path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_history_query_row_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(shard_data_path) end)

    state_key = Keys.state_key("history-query-row", "tenant-a")

    record = %{
      id: "history-query-row",
      type: "job",
      state: "completed",
      version: 3,
      partition_key: "tenant-a",
      history_max_events: 10_000,
      history_hot_max_events: 123
    }

    locator =
      Locator.new!(
        flow_id: record.id,
        kind: :state,
        version: record.version,
        raft_index: 9,
        file_id: {:waraft_segment, 9},
        offset: 0,
        value_size: 512,
        frame_size: 512,
        checksum: :binary.copy(<<1>>, 32),
        expire_at_ms: 0,
        segment_generation: 0
      )

    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 0)
    assert :ok = LMDB.write_batch(LMDB.path(shard_data_path), [{:put, state_key, query_row}])

    assert {:ok, %{history_hot_max_events: 123}} =
             Recovery.load_lmdb_history_state_record(state_key, shard_data_path)
  end

  test "loads expired query-row routing metadata during recovery" do
    {shard_data_path, state_key, record, locator} = query_row_fixture("expired-query-row")
    on_exit(fn -> File.rm_rf!(shard_data_path) end)

    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 1)
    assert :ok = LMDB.write_batch(LMDB.path(shard_data_path), [{:put, state_key, query_row}])

    assert {:ok, %{history_hot_max_events: 123}} =
             Recovery.load_lmdb_history_state_record(state_key, shard_data_path)
  end

  test "loads routing metadata from LMDB for a WARaft-backed keydir row" do
    {shard_data_path, state_key, record, locator} = query_row_fixture("waraft-keydir-row")
    on_exit(fn -> File.rm_rf!(shard_data_path) end)
    keydir = :ets.new(:history_recovery_waraft_keydir_row, [:set])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 0)
    assert :ok = LMDB.write_batch(LMDB.path(shard_data_path), [{:put, state_key, query_row}])

    true =
      :ets.insert(
        keydir,
        {state_key, nil, 0, 0, locator.file_id, locator.offset, locator.value_size}
      )

    assert {:ok, %{history_hot_max_events: 123}} =
             Recovery.load_history_state_record(state_key, keydir, shard_data_path)
  end

  test "loads routing metadata for a cached blob ref without opening the blob" do
    {shard_data_path, state_key, record, locator} = query_row_fixture("blob-ref-keydir-row")
    on_exit(fn -> File.rm_rf!(shard_data_path) end)
    keydir = :ets.new(:history_recovery_blob_ref_keydir_row, [:set])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    encoded_ref =
      BlobRef.encode!(%BlobRef{
        checksum: locator.checksum,
        size: 1_024,
        segment_id: 7,
        offset: 11
      })

    locator = Locator.relocate!(locator, value_size: byte_size(encoded_ref))
    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 0)
    assert :ok = LMDB.write_batch(LMDB.path(shard_data_path), [{:put, state_key, query_row}])

    true =
      :ets.insert(
        keydir,
        {state_key, encoded_ref, 0, 0, locator.file_id, locator.offset, locator.value_size}
      )

    assert {:ok, %{history_hot_max_events: 123}} =
             Recovery.load_history_state_record(state_key, keydir, shard_data_path)
  end

  test "rejects stale LMDB routing metadata for an indirect keydir row" do
    {shard_data_path, state_key, record, locator} = query_row_fixture("stale-query-row")
    on_exit(fn -> File.rm_rf!(shard_data_path) end)
    keydir = :ets.new(:history_recovery_stale_query_row, [:set])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 0)
    assert :ok = LMDB.write_batch(LMDB.path(shard_data_path), [{:put, state_key, query_row}])

    true =
      :ets.insert(
        keydir,
        {state_key, nil, 0, 0, {:waraft_segment, 10}, locator.offset, locator.value_size}
      )

    assert {:error, :corrupt_history_query_row_location} =
             Recovery.load_history_state_record(state_key, keydir, shard_data_path)
  end

  test "accepts relocated LMDB metadata from the same WARaft generation" do
    {shard_data_path, state_key, record, source_locator} =
      query_row_fixture("relocated-query-row")

    on_exit(fn -> File.rm_rf!(shard_data_path) end)
    keydir = :ets.new(:history_recovery_relocated_query_row, [:set])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    relocated =
      Locator.relocate!(source_locator,
        file_id: 22,
        offset: 4_096,
        frame_size: nil,
        segment_generation: nil
      )

    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, relocated, 0)
    assert :ok = LMDB.write_batch(LMDB.path(shard_data_path), [{:put, state_key, query_row}])

    true =
      :ets.insert(
        keydir,
        {state_key, nil, 0, 0, {:waraft_apply_projection, source_locator.raft_index}, 0,
         source_locator.value_size}
      )

    assert {:ok, %{history_hot_max_events: 123}} =
             Recovery.load_history_state_record(state_key, keydir, shard_data_path)
  end

  test "does not mask malformed inline state with valid LMDB metadata" do
    {shard_data_path, state_key, record, locator} = query_row_fixture("malformed-inline-row")
    on_exit(fn -> File.rm_rf!(shard_data_path) end)
    keydir = :ets.new(:history_recovery_malformed_inline_row, [:set])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 0)
    assert :ok = LMDB.write_batch(LMDB.path(shard_data_path), [{:put, state_key, query_row}])

    true =
      :ets.insert(
        keydir,
        {state_key, "not-a-flow-record", 0, 0, locator.file_id, locator.offset,
         locator.value_size}
      )

    assert {:error, :corrupt_history_state_record} =
             Recovery.load_history_state_record(state_key, keydir, shard_data_path)
  end

  test "history entry recovery rejects corrupt query rows instead of applying the default cap" do
    shard_data_path = tmp_path("corrupt-query-row")
    on_exit(fn -> File.rm_rf!(shard_data_path) end)
    keydir = :ets.new(:history_recovery_corrupt_query_row, [:set])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    id = "corrupt-query-row"
    state_key = Keys.state_key(id, "tenant-a")
    history_key = Keys.history_key(id, "tenant-a")

    entry_key =
      Ferricstore.Flow.HistoryProjector.KeyCodec.history_entry_key(history_key, "1000-1")

    assert :ok = LMDB.write_batch(LMDB.path(shard_data_path), [{:put, state_key, "corrupt"}])

    assert {:error, :corrupt_history_query_row} =
             Recovery.recovered_history_entries(
               %{entry_key => {10, 20, 0}},
               keydir,
               shard_data_path
             )
  end

  test "history entry recovery rejects a hot record with foreign sealed scope" do
    shard_data_path = tmp_path("forged-hot-state")
    on_exit(fn -> File.rm_rf!(shard_data_path) end)
    keydir = :ets.new(:history_recovery_forged_hot_state, [:set])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    scope = <<42::unsigned-big-64>>
    assert {:ok, physical_partition} = StorageScope.physical_partition_key("tenant-a", scope)
    id = "forged-hot-state"
    state_key = Keys.state_key(id, physical_partition)
    history_key = Keys.history_key(id, physical_partition)

    entry_key =
      Ferricstore.Flow.HistoryProjector.KeyCodec.history_entry_key(history_key, "1000-1")

    forged = %{
      id: id,
      type: "job",
      state: "completed",
      version: 3,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: 2,
      next_run_at_ms: 3,
      priority: 0,
      partition_key: physical_partition,
      root_flow_id: id,
      state_enter_seq: 1,
      history_hot_max_events: 123,
      system_metadata: scope_metadata(99)
    }

    encoded = Ferricstore.Flow.encode_record(forged)
    true = :ets.insert(keydir, {state_key, encoded, 0, 0, 0, 0, byte_size(encoded)})

    assert {:error, :corrupt_history_state_record} =
             Recovery.recovered_history_entries(
               %{entry_key => {10, 20, 0}},
               keydir,
               shard_data_path
             )
  end

  test "history entry recovery defaults only when the owning state is absent" do
    shard_data_path = tmp_path("missing-state")
    on_exit(fn -> File.rm_rf!(shard_data_path) end)
    keydir = :ets.new(:history_recovery_missing_state, [:set])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    history_key = Keys.history_key("missing-state", "tenant-a")

    entry_key =
      Ferricstore.Flow.HistoryProjector.KeyCodec.history_entry_key(history_key, "1000-1")

    assert {:ok, {[entry], [{10, 20}]}} =
             Recovery.recovered_history_entries(
               %{entry_key => {10, 20, 0}},
               keydir,
               shard_data_path
             )

    assert entry.history_hot_max_events == Recovery.default_history_hot_max_events()
  end

  test "history log recovery never treats a directory as an empty safe log" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_history_recovery_path_#{System.unique_integer([:positive])}"
      )

    history_log = Path.join([dir, "history", "00000.log"])
    File.mkdir_p!(history_log)
    on_exit(fn -> File.rm_rf!(dir) end)

    refute Recovery.history_log_safe_to_skip?(dir)
    assert Storage.ensure_history_file(dir) == {:error, {:invalid_history_file_type, :directory}}
  end

  test "history recovery rejects unknown current-log keys instead of silently dropping them" do
    history_key = Ferricstore.Flow.Keys.history_key("recovery-schema")
    event_id = "1000-1"

    physical_key =
      Ferricstore.Flow.HistoryProjector.KeyCodec.history_entry_key(history_key, event_id)

    assert Recovery.live_history_records([{physical_key, 10, 20, 0, false}]) ==
             {:ok, %{physical_key => {10, 20, 0}}}

    value_key = Ferricstore.Flow.Keys.value_key("recovery-schema", :payload, 1)
    assert Recovery.live_history_records([{value_key, 30, 40, 0, false}]) == {:ok, %{}}

    assert Recovery.live_history_records([{"unknown-log-key", 0, 1, 0, false}]) ==
             {:error, {:invalid_history_log_key, "unknown-log-key"}}
  end

  defp query_row_fixture(suffix) do
    shard_data_path = tmp_path(suffix)
    state_key = Keys.state_key(suffix, "tenant-a")

    record = %{
      id: suffix,
      type: "job",
      state: "completed",
      version: 3,
      partition_key: "tenant-a",
      history_max_events: 10_000,
      history_hot_max_events: 123
    }

    locator =
      Locator.new!(
        flow_id: record.id,
        kind: :state,
        version: record.version,
        raft_index: 9,
        file_id: {:waraft_segment, 9},
        offset: 0,
        value_size: 512,
        frame_size: 512,
        checksum: :binary.copy(<<1>>, 32),
        expire_at_ms: 0,
        segment_generation: 0
      )

    {shard_data_path, state_key, record, locator}
  end

  defp tmp_path(suffix) do
    Path.join(
      System.tmp_dir!(),
      "ferricstore_history_recovery_#{suffix}_#{System.unique_integer([:positive])}"
    )
  end

  defp scope_metadata(value), do: %{0x8001 => {1, :uint64, :isolation_scope, value}}
end
