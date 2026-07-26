defmodule Ferricstore.Raft.ApplyProjectionRetentionIntegrationTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, LMDB, Locator}
  alias Ferricstore.Flow.Query.{QueryRowCodec, SourceCatalog}
  alias Ferricstore.Raft.{WARaftSegmentReader, WARaftStorage}
  alias Ferricstore.Store.LFU

  @retained_count 4_097

  test "production compaction spills a large retention set without splitting one Raft frame" do
    root = tmp_root()
    keydir = :ets.new(:apply_projection_retention_integration, [:ordered_set, :public])
    ctx = %{data_dir: root, keydir_refs: {keydir}}
    storage_root = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])
    projection_root = Path.join(storage_root, "apply_projection_log")
    lmdb_path = root |> Ferricstore.DataDir.shard_data_path(0) |> LMDB.path()

    entries =
      for number <- 1..@retained_count do
        key = "retained:#{number |> Integer.to_string() |> String.pad_leading(4, "0")}"
        {key, "v", 0}
      end

    rows =
      Enum.map(entries, fn {key, value, expire_at_ms} ->
        {key, value, expire_at_ms, LFU.initial(), {:waraft_apply_projection, 1}, 0,
         byte_size(value)}
      end)

    true = :ets.insert(keydir, rows)
    assert :ok = WARaftSegmentReader.put_apply_projection(root, 0, 1, entries)
    assert {:ok, @retained_count} = WARaftSegmentReader.spill_apply_projection_cache(root, 0)

    assert :ok =
             WARaftStorage.__compact_apply_projection_log_for_test__(
               storage_root,
               ctx,
               0,
               2,
               lmdb_path
             )

    assert {:ok,
            {0, {:ferricstore_segment_apply_projection_batch, {:raft_log_pos, 1, 0}, retained}}} =
             :ferricstore_waraft_spike_segment_log.read_disk(to_charlist(projection_root), 1)

    assert length(retained) == @retained_count

    assert {:ok, {ordinal, offset, frame_size}} =
             WARaftSegmentReader.physical_location(ctx, 0, {:waraft_apply_projection, 1})

    requests =
      for key <- ["retained:0001", "retained:4097"] do
        %{
          file_id: {:waraft_apply_projection, 1},
          ordinal: ordinal,
          offset: offset,
          frame_size: frame_size,
          key: key
        }
      end

    assert {:ok, %{"retained:0001" => "v", "retained:4097" => "v"}} =
             WARaftSegmentReader.read_physical_values(ctx, 0, requests, 5_000, :include_expired)

    refute File.exists?(Path.join(storage_root, ".apply_projection_retention"))
  end

  test "temporary retention setup failures use the compaction error contract" do
    root = tmp_root()
    File.write!(root, "not-a-directory")
    keydir = :ets.new(:apply_projection_retention_setup_failure, [:ordered_set, :public])
    ctx = %{data_dir: root, keydir_refs: {keydir}}

    assert {:error, {:compact_apply_projection_log_failed, _reason}} =
             WARaftStorage.__compact_apply_projection_log_for_test__(
               root,
               ctx,
               0,
               2,
               Path.join(root, "lmdb")
             )
  end

  test "one rewrite relocates the live and snapshot QueryRow catalogs" do
    root = tmp_root()
    keydir = :ets.new(:apply_projection_snapshot_query_rows, [:ordered_set, :public])
    ctx = %{data_dir: root, keydir_refs: {keydir}}
    storage_root = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])
    live_lmdb_path = root |> Ferricstore.DataDir.shard_data_path(0) |> LMDB.path()
    snapshot_path = Path.join(root, "snapshot")
    snapshot_lmdb_path = Path.join([snapshot_path, "data", "flow_lmdb"])
    record = flow_record("snapshot-query-row")
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded_record = Ferricstore.Flow.encode_record(record)
    dead_index = 1
    retained_index = 2

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, dead_index, [
               {"dead-before-snapshot-query-row", :binary.copy("d", 64 * 1_024), 0}
             ])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, retained_index, [
               {state_key, encoded_record, 0}
             ])

    assert {:ok, 2} = WARaftSegmentReader.spill_apply_projection_cache(root, 0)

    assert {:ok, {old_ordinal, old_offset, old_frame_size}} =
             WARaftSegmentReader.physical_location(
               ctx,
               0,
               {:waraft_apply_projection, retained_index}
             )

    locator =
      Locator.new!(
        flow_id: record.id,
        kind: :state,
        version: record.version,
        raft_index: retained_index,
        file_id: {:waraft_apply_projection, retained_index},
        segment_generation: old_ordinal,
        offset: old_offset,
        frame_size: old_frame_size,
        value_size: byte_size(encoded_record),
        checksum: :crypto.hash(:sha256, encoded_record),
        expire_at_ms: nil
      )

    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 0)
    catalog_key = Keys.type_catalog_member_key(record.type, state_key)
    assert {:ok, catalog_op} = SourceCatalog.put_op(catalog_key, state_key)

    for path <- [live_lmdb_path, snapshot_lmdb_path] do
      assert :ok = LMDB.write_batch(path, [{:put, state_key, query_row}, catalog_op])
    end

    assert :ok =
             WARaftStorage.__compact_snapshot_apply_projection_log_for_test__(
               storage_root,
               ctx,
               0,
               retained_index + 1,
               snapshot_path
             )

    assert {:ok, {new_ordinal, new_offset, new_frame_size}} =
             WARaftSegmentReader.physical_location(
               ctx,
               0,
               {:waraft_apply_projection, retained_index}
             )

    refute {old_ordinal, old_offset, old_frame_size} ==
             {new_ordinal, new_offset, new_frame_size}

    for path <- [live_lmdb_path, snapshot_lmdb_path] do
      assert {:ok, encoded_row} = LMDB.get(path, state_key)
      assert {:ok, %{locator: relocated}} = QueryRowCodec.decode(encoded_row, state_key)
      assert relocated.segment_generation == new_ordinal
      assert relocated.offset == new_offset
      assert relocated.frame_size == new_frame_size
    end

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(root, 0)
      _ = LMDB.release(live_lmdb_path)
      _ = LMDB.release(snapshot_lmdb_path)
    end)
  end

  defp flow_record(id) do
    %{
      id: id,
      version: 7,
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

  defp tmp_root do
    root =
      Path.join(
        System.tmp_dir!(),
        "apply-projection-retention-integration-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(root) end)
    root
  end
end
