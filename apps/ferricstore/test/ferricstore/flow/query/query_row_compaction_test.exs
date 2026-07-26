defmodule Ferricstore.Flow.Query.QueryRowCompactionTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, LMDB, Locator}

  alias Ferricstore.Flow.Query.{
    QueryRecordStore,
    QueryRowCodec,
    QueryRowCompaction,
    SourceCatalog
  }

  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Store.{BlobRef, BlobStore}

  test "retention validates a materialized record but copies only its stored blob reference" do
    root = tmp_root("blob_ref")
    ctx = %{data_dir: root, blob_side_channel_threshold_bytes: 64}
    record = record("run-blob-ref", 3)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded_record = Ferricstore.Flow.encode_record(record)
    assert {:ok, blob_ref} = BlobStore.put(root, 0, encoded_record)
    stored_ref = BlobRef.encode!(blob_ref)
    index = 21

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, index, [
               {state_key, stored_ref, 0}
             ])

    assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(root, 0)

    locator =
      physical_locator!(ctx, record, encoded_record, index, value_size: byte_size(stored_ref))

    lmdb_path = lmdb_path(root)
    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 0)
    assert {:ok, catalog_op} = source_catalog_op(record, state_key)
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, state_key, query_row}, catalog_op])

    assert {:ok, [{{^index, ^state_key}, {^state_key, ^stored_ref, 0}}]} =
             QueryRowCompaction.collect_retention_entries(ctx, 0, lmdb_path, index + 1, 0)

    parent = self()

    assert {:ok, 1} =
             QueryRowCompaction.stream_retention_entries(
               ctx,
               0,
               lmdb_path,
               index + 1,
               0,
               0,
               fn entries, count ->
                 send(parent, {:retention_page, entries})
                 {:ok, count + length(entries)}
               end
             )

    assert_received {:retention_page, [{{^index, ^state_key}, {^state_key, ^stored_ref, 0}}]}

    refute stored_ref == encoded_record
  end

  test "retention preserves the authoritative frame expiry instead of the shorter row expiry" do
    root = tmp_root("source_expiry")
    ctx = %{data_dir: root}
    record = record("run-source-expiry", 4)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded_record = Ferricstore.Flow.encode_record(record)
    index = 31
    source_expiry_ms = 2_000
    row_expiry_ms = 1_000

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, index, [
               {state_key, encoded_record, source_expiry_ms}
             ])

    assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(root, 0)

    locator =
      physical_locator!(ctx, record, encoded_record, index, expire_at_ms: source_expiry_ms)

    lmdb_path = lmdb_path(root)
    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, row_expiry_ms)
    assert {:ok, catalog_op} = source_catalog_op(record, state_key)
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, state_key, query_row}, catalog_op])

    assert {:ok,
            [
              {{^index, ^state_key}, {^state_key, ^encoded_record, ^source_expiry_ms}}
            ]} =
             QueryRowCompaction.collect_retention_entries(ctx, 0, lmdb_path, index + 1, 0)
  end

  test "a query repairs a stale QueryRow after a crash between rewrite and locator CAS" do
    root = tmp_root("crash_repair")
    ctx = %{data_dir: root, max_value_size: 1_048_576}
    dead_index = 10
    query_index = 11
    trim_index = 12
    dead_key = "query-row-compaction-dead"
    record = record("run-crash-repair", 7)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded_record = Ferricstore.Flow.encode_record(record)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, dead_index, [
               {dead_key, :binary.copy("d", 64 * 1_024), 0}
             ])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, query_index, [
               {state_key, encoded_record, 0}
             ])

    assert {:ok, 2} = WARaftSegmentReader.spill_apply_projection_cache(root, 0)
    old_locator = physical_locator!(ctx, record, encoded_record, query_index)
    lmdb_path = lmdb_path(root)
    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, old_locator, 0)
    assert {:ok, catalog_op} = source_catalog_op(record, state_key)
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, state_key, query_row}, catalog_op])

    assert {:ok, [{{^query_index, ^state_key}, {^state_key, ^encoded_record, 0}}]} =
             QueryRowCompaction.collect_retention_entries(
               ctx,
               0,
               lmdb_path,
               trim_index,
               0
             )

    retained_batches = [{{:raft_log_pos, query_index, 0}, [{state_key, encoded_record, 0}]}]

    assert :ok =
             WARaftSegmentReader.with_apply_projection_disk_lock(root, 0, fn ->
               :ferricstore_waraft_spike_segment_log.compact_apply_projection(
                 to_charlist(apply_projection_root(root)),
                 trim_index,
                 retained_batches
               )
             end)

    assert {:ok, unchanged_row} = LMDB.get(lmdb_path, state_key)
    assert {:ok, %{locator: ^old_locator}} = QueryRowCodec.decode(unchanged_row, state_key)

    assert {:ok, [hydrated], true} =
             QueryRecordStore.read_many(
               ctx,
               0,
               lmdb_path,
               [state_key],
               1_000,
               QueryRecordStore.max_input_bytes(ctx)
             )

    assert hydrated.id == record.id
    assert hydrated.version == record.version
    assert {:ok, repaired_row} = LMDB.get(lmdb_path, state_key)
    assert {:ok, %{locator: repaired_locator}} = QueryRowCodec.decode(repaired_row, state_key)
    assert Locator.same_logical_record?(old_locator, repaired_locator)
    refute Locator.same_physical_record?(old_locator, repaired_locator)

    assert :ok = QueryRowCompaction.relocate_after_rewrite(ctx, 0, lmdb_path, 0)
    assert {:ok, ^repaired_row} = LMDB.get(lmdb_path, state_key)
  end

  test "the next retention pass repairs a stale QueryRow left after the rewrite swap" do
    root = tmp_root("retention_crash_repair")
    ctx = %{data_dir: root, max_value_size: 1_048_576}
    dead_index = 20
    query_index = 21
    trim_index = 22
    record = record("run-retention-crash-repair", 8)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded_record = Ferricstore.Flow.encode_record(record)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, dead_index, [
               {"retention-crash-repair-dead", :binary.copy("d", 64 * 1_024), 0}
             ])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, query_index, [
               {state_key, encoded_record, 0}
             ])

    assert {:ok, 2} = WARaftSegmentReader.spill_apply_projection_cache(root, 0)
    stale_locator = physical_locator!(ctx, record, encoded_record, query_index)
    lmdb_path = lmdb_path(root)
    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, stale_locator, 0)
    assert {:ok, catalog_op} = source_catalog_op(record, state_key)
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, state_key, query_row}, catalog_op])

    retained_batches = [{{:raft_log_pos, query_index, 0}, [{state_key, encoded_record, 0}]}]

    assert :ok =
             WARaftSegmentReader.with_apply_projection_disk_lock(root, 0, fn ->
               :ferricstore_waraft_spike_segment_log.compact_apply_projection(
                 to_charlist(apply_projection_root(root)),
                 trim_index,
                 retained_batches
               )
             end)

    assert {:ok, [{{^query_index, ^state_key}, {^state_key, ^encoded_record, 0}}]} =
             QueryRowCompaction.collect_retention_entries(
               ctx,
               0,
               lmdb_path,
               trim_index,
               0
             )

    assert {:ok, repaired_row} = LMDB.get(lmdb_path, state_key)
    assert {:ok, %{locator: repaired_locator}} = QueryRowCodec.decode(repaired_row, state_key)
    assert Locator.same_logical_record?(stale_locator, repaired_locator)
    refute Locator.same_physical_record?(stale_locator, repaired_locator)
  end

  test "an interrupted rewrite rolls back without invalidating the existing QueryRow" do
    root = tmp_root("rewrite_rollback")
    ctx = %{data_dir: root}
    query_index = 41
    trim_index = 42
    record = record("run-rewrite-rollback", 9)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded_record = Ferricstore.Flow.encode_record(record)
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_rewrite_hook)

    on_exit(fn -> restore_env(:waraft_segment_log_rewrite_hook, previous_hook) end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, query_index, [
               {state_key, encoded_record, 0}
             ])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, trim_index, [
               {"current", "keep", 0}
             ])

    assert {:ok, 2} = WARaftSegmentReader.spill_apply_projection_cache(root, 0)
    locator = physical_locator!(ctx, record, encoded_record, query_index)
    lmdb_path = lmdb_path(root)
    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 0)
    assert {:ok, catalog_op} = source_catalog_op(record, state_key)
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, state_key, query_row}, catalog_op])

    assert {:ok, [{{^query_index, ^state_key}, retained_entry}]} =
             QueryRowCompaction.collect_retention_entries(
               ctx,
               0,
               lmdb_path,
               trim_index,
               0
             )

    Application.put_env(
      :ferricstore,
      :waraft_segment_log_rewrite_hook,
      {:fail_once_after_live_backup, self()}
    )

    assert {:error, {:rewrite_hook, :after_live_backup}} =
             WARaftSegmentReader.with_apply_projection_disk_lock(root, 0, fn ->
               :ferricstore_waraft_spike_segment_log.compact_apply_projection(
                 to_charlist(apply_projection_root(root)),
                 trim_index,
                 [{{:raft_log_pos, query_index, 0}, [retained_entry]}]
               )
             end)

    assert_receive {:waraft_segment_log_rewrite_hook, :after_live_backup}
    assert {:ok, ^query_row} = LMDB.get(lmdb_path, state_key)

    assert {:ok, [%{id: "run-rewrite-rollback", version: 9}]} =
             Ferricstore.Flow.RecordHydrator.read_many(
               ctx,
               0,
               [{state_key, locator}],
               max_bytes: 1_000_000
             )
  end

  test "relocation covers QueryRows on both sides of the trim boundary" do
    root = tmp_root("both_sides")
    ctx = %{data_dir: root}
    trim_index = 52
    before = record("run-before-trim", 1)
    at_trim = record("run-at-trim", 2)
    before_key = Keys.state_key(before.id, before.partition_key)
    after_key = Keys.state_key(at_trim.id, at_trim.partition_key)
    before_encoded = Ferricstore.Flow.encode_record(before)
    after_encoded = Ferricstore.Flow.encode_record(at_trim)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 50, [
               {"dead", :binary.copy("d", 32 * 1_024), 0}
             ])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 51, [
               {before_key, before_encoded, 0}
             ])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, trim_index, [
               {after_key, after_encoded, 0}
             ])

    assert {:ok, 3} = WARaftSegmentReader.spill_apply_projection_cache(root, 0)
    before_locator = physical_locator!(ctx, before, before_encoded, 51)
    after_locator = physical_locator!(ctx, at_trim, after_encoded, trim_index)
    lmdb_path = lmdb_path(root)
    assert {:ok, before_row} = QueryRowCodec.encode(before_key, before, before_locator, 0)
    assert {:ok, after_row} = QueryRowCodec.encode(after_key, at_trim, after_locator, 0)
    assert {:ok, before_catalog} = source_catalog_op(before, before_key)
    assert {:ok, after_catalog} = source_catalog_op(at_trim, after_key)

    assert :ok =
             LMDB.write_batch(lmdb_path, [
               {:put, before_key, before_row},
               before_catalog,
               {:put, after_key, after_row},
               after_catalog
             ])

    assert {:ok, [{{51, ^before_key}, retained_entry}]} =
             QueryRowCompaction.collect_retention_entries(ctx, 0, lmdb_path, trim_index, 0)

    assert :ok =
             WARaftSegmentReader.with_apply_projection_disk_lock(root, 0, fn ->
               with :ok <-
                      :ferricstore_waraft_spike_segment_log.compact_apply_projection(
                        to_charlist(apply_projection_root(root)),
                        trim_index,
                        [{{:raft_log_pos, 51, 0}, [retained_entry]}]
                      ) do
                 QueryRowCompaction.relocate_after_rewrite(ctx, 0, lmdb_path, 0)
               end
             end)

    assert {:ok, relocated_before_row} = LMDB.get(lmdb_path, before_key)
    assert {:ok, relocated_after_row} = LMDB.get(lmdb_path, after_key)

    assert {:ok, %{locator: relocated_before}} =
             QueryRowCodec.decode(relocated_before_row, before_key)

    assert {:ok, %{locator: relocated_after}} =
             QueryRowCodec.decode(relocated_after_row, after_key)

    refute Locator.same_physical_record?(before_locator, relocated_before)
    refute Locator.same_physical_record?(after_locator, relocated_after)

    assert {:ok, [%{id: "run-before-trim"}, %{id: "run-at-trim"}]} =
             Ferricstore.Flow.RecordHydrator.read_many(
               ctx,
               0,
               [{before_key, relocated_before}, {after_key, relocated_after}],
               max_bytes: 1_000_000
             )
  end

  test "streaming compaction consumes bounded retention pages and keeps later records" do
    root = tmp_root("streamed_rewrite")
    ctx = %{data_dir: root}

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 61, [
               {"retain-a", "value-a", 10},
               {"drop-a", "old-a", 0}
             ])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 62, [
               {"retain-b", "value-b", 20},
               {"drop-b", "old-b", 0}
             ])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 63, [
               {"current", "current-value", 0}
             ])

    assert {:ok, 5} = WARaftSegmentReader.spill_apply_projection_cache(root, 0)
    parent = self()

    page = fn
      "" ->
        send(parent, {:retention_cursor, ""})

        {:ok, [{{:raft_log_pos, 61, 0}, [{"retain-a", "value-a", 10}]}], "page-2", false}

      "page-2" ->
        send(parent, {:retention_cursor, "page-2"})

        {:ok, [{{:raft_log_pos, 62, 0}, [{"retain-b", "value-b", 20}]}], "done", true}
    end

    assert :ok =
             WARaftSegmentReader.with_apply_projection_disk_lock(root, 0, fn ->
               :ferricstore_waraft_spike_segment_log.compact_apply_projection_stream(
                 to_charlist(apply_projection_root(root)),
                 63,
                 page
               )
             end)

    assert_received {:retention_cursor, ""}
    assert_received {:retention_cursor, "page-2"}

    assert {:ok, "value-a"} =
             WARaftSegmentReader.read_value_from_location_including_expired(
               ctx,
               0,
               {:waraft_apply_projection, 61},
               "retain-a"
             )

    assert :not_found =
             WARaftSegmentReader.read_value_from_location_including_expired(
               ctx,
               0,
               {:waraft_apply_projection, 61},
               "drop-a"
             )

    assert {:ok, "value-b"} =
             WARaftSegmentReader.read_value_from_location_including_expired(
               ctx,
               0,
               {:waraft_apply_projection, 62},
               "retain-b"
             )

    assert {:ok, "current-value"} =
             WARaftSegmentReader.read_value_from_location_including_expired(
               ctx,
               0,
               {:waraft_apply_projection, 63},
               "current"
             )
  end

  test "a failed retention stream leaves the source projection log unchanged" do
    root = tmp_root("streamed_rewrite_failure")
    ctx = %{data_dir: root}

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 71, [
               {"keep", "original", 0}
             ])

    assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(root, 0)

    assert {:error, {:apply_projection_retention_stream_failed, :injected}} =
             WARaftSegmentReader.with_apply_projection_disk_lock(root, 0, fn ->
               :ferricstore_waraft_spike_segment_log.compact_apply_projection_stream(
                 to_charlist(apply_projection_root(root)),
                 72,
                 fn "" -> {:error, :injected} end
               )
             end)

    assert {:ok, "original"} =
             WARaftSegmentReader.read_value_from_location_including_expired(
               ctx,
               0,
               {:waraft_apply_projection, 71},
               "keep"
             )
  end

  defp physical_locator!(ctx, record, encoded_record, index, opts \\ []) do
    assert {:ok, {ordinal, offset, frame_size}} =
             WARaftSegmentReader.physical_location(
               ctx,
               0,
               {:waraft_apply_projection, index}
             )

    Locator.new!(
      flow_id: record.id,
      kind: :state,
      version: record.version,
      raft_index: index,
      file_id: {:waraft_apply_projection, index},
      segment_generation: ordinal,
      offset: offset,
      frame_size: frame_size,
      value_size: Keyword.get(opts, :value_size, byte_size(encoded_record)),
      checksum: :crypto.hash(:sha256, encoded_record),
      expire_at_ms: Keyword.get(opts, :expire_at_ms)
    )
  end

  defp record(id, version) do
    %{
      id: id,
      version: version,
      type: "job",
      state: "completed",
      partition_key: "tenant-a",
      priority: 0,
      created_at_ms: 100,
      updated_at_ms: 200,
      next_run_at_ms: 0,
      attempts: 1,
      attributes: %{"customer" => "acme"},
      state_meta: %{"completed" => %{"reason" => "ok"}}
    }
  end

  defp source_catalog_op(record, state_key) do
    record.type
    |> Keys.type_catalog_member_key(state_key)
    |> SourceCatalog.put_op(state_key)
  end

  defp lmdb_path(root) do
    root
    |> Ferricstore.DataDir.shard_data_path(0)
    |> LMDB.path()
  end

  defp apply_projection_root(root) do
    Path.join([
      root,
      "waraft",
      "ferricstore_waraft_backend.1",
      "apply_projection_log"
    ])
  end

  defp tmp_root(suffix) do
    root =
      Path.join(
        System.tmp_dir!(),
        "query_row_compaction_#{suffix}_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(Ferricstore.DataDir.shard_data_path(root, 0))

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(root, 0)
      _ = LMDB.release(lmdb_path(root))
      File.rm_rf!(root)
    end)

    root
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
