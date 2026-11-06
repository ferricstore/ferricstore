defmodule Ferricstore.Flow.Query.BackfillSourceTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, LMDB, Locator, PolicyMigration, StorageScope}
  alias Ferricstore.Flow.Query.{BackfillSource, QueryRowCodec}

  setup do
    suffix = System.unique_integer([:positive, :monotonic])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_query_backfill_#{suffix}")

    ctx = %{
      data_dir: data_dir,
      shard_count: 1,
      max_value_size: 1_048_576
    }

    on_exit(fn -> File.rm_rf!(data_dir) end)

    %{ctx: ctx}
  end

  test "snapshots long state keys behind fixed-size staging keys and pages current records", %{
    ctx: ctx
  } do
    long_id = String.duplicate("r", 60_000)
    first_key = Keys.state_key(long_id, "tenant-a")
    second_key = Keys.state_key("short", "tenant-a")
    put_catalog_member!(ctx, "invoice", first_key, 0)
    put_catalog_member!(ctx, "invoice", second_key, 0)
    assert :ok = LMDB.write_batch(lmdb_path(ctx), [{:put, "ordinary-key", "ignored"}])

    build_id = "build-a"
    snapshot_all!(ctx, build_id, 1)

    lmdb_path = lmdb_path(ctx)
    prefix = BackfillSource.staging_prefix(build_id)
    assert {:ok, staged} = LMDB.prefix_entries(lmdb_path, prefix, 10)
    assert length(staged) == 2
    assert Enum.all?(staged, fn {key, value} -> byte_size(key) < 128 and byte_size(value) > 0 end)

    assert {:ok, staged_first} =
             BackfillSource.staging_page(ctx, 0, build_id, "", 1, 1_024 * 1_024)

    assert length(staged_first.state_keys) == 1
    refute staged_first.done?

    assert {:ok, staged_second} =
             BackfillSource.staging_page(
               ctx,
               0,
               build_id,
               staged_first.cursor,
               1,
               1_024 * 1_024
             )

    assert staged_second.done?

    assert Enum.sort(staged_first.state_keys ++ staged_second.state_keys) ==
             Enum.sort([first_key, second_key])

    values = %{
      first_key => encoded_record(long_id, "tenant-a", 1),
      second_key => encoded_record("short", "tenant-a", 2)
    }

    read_entries = fn _ctx, 0, keys ->
      {:ok, Enum.map(keys, &{Map.fetch!(values, &1), 0})}
    end

    assert {:ok, first_page} =
             BackfillSource.page(ctx, 0, build_id, "", 1, 2 * 1_024 * 1_024,
               read_entries_fun: read_entries
             )

    assert length(first_page.records) == 1
    assert is_binary(first_page.cursor) and first_page.cursor != ""
    refute first_page.done?

    assert {:ok, second_page} =
             BackfillSource.page(ctx, 0, build_id, first_page.cursor, 1, 2 * 1_024 * 1_024,
               read_entries_fun: read_entries
             )

    assert length(second_page.records) == 1
    assert second_page.done?

    assert Enum.sort(Enum.map(first_page.records ++ second_page.records, & &1.record.id)) ==
             Enum.sort([long_id, "short"])

    assert :ok = BackfillSource.cleanup(ctx, 0, build_id)
    assert {:ok, []} = LMDB.prefix_entries(lmdb_path, prefix, 10)
  end

  test "pages small records past the worst-case value size in bounded primary reads", %{
    ctx: ctx
  } do
    # A 1 MiB max_value_size allows eight primaries per 8 MiB read, but a page
    # of small records is not cut to eight.
    build_id = "small-records"
    values = staged_records!(ctx, build_id, 64, "")
    parent = self()

    read_entries = fn _ctx, 0, keys ->
      send(parent, {:primary_read, length(keys)})
      {:ok, Enum.map(keys, &{Map.fetch!(values, &1), 0})}
    end

    assert {:ok, page} =
             BackfillSource.page(ctx, 0, build_id, "", 64, 8 * 1_024 * 1_024,
               read_entries_fun: read_entries
             )

    assert length(page.records) == 64
    assert page.scanned_entries == 64
    assert page.done?
    assert Enum.sort(Enum.map(page.records, & &1.state_key)) == Enum.sort(Map.keys(values))

    reads = primary_reads()
    assert Enum.sum(reads) == 64
    assert Enum.all?(reads, &(&1 <= 8))
  end

  test "ends a page at the byte budget and resumes after its last hydrated record", %{
    ctx: ctx
  } do
    build_id = "budget-records"
    values = staged_records!(ctx, build_id, 10, String.duplicate("x", 400))
    value_size = values |> Map.values() |> Enum.map(&byte_size/1) |> Enum.max()
    ctx = %{ctx | max_value_size: value_size}
    max_bytes = 4 * value_size + div(value_size, 2)

    read_entries = fn _ctx, 0, keys ->
      {:ok, Enum.map(keys, &{Map.fetch!(values, &1), 0})}
    end

    pages =
      Stream.unfold({"", false}, fn
        {_cursor, true} ->
          nil

        {cursor, false} ->
          assert {:ok, page} =
                   BackfillSource.page(ctx, 0, build_id, cursor, 64, max_bytes,
                     read_entries_fun: read_entries
                   )

          assert page.hydrated_bytes <= max_bytes
          assert page.done? or page.cursor > cursor
          {page, {page.cursor, page.done?}}
      end)
      |> Enum.to_list()

    assert Enum.map(pages, &length(&1.records)) == [4, 4, 2]
    assert Enum.map(pages, & &1.scanned_entries) == [4, 4, 2]

    hydrated = Enum.flat_map(pages, fn page -> Enum.map(page.records, & &1.state_key) end)
    assert Enum.sort(hydrated) == Enum.sort(Map.keys(values))
  end

  test "keeps each primary read within the shard reader's key limits", %{ctx: ctx} do
    # Small values allow large reads, but 64 state keys of 40 KB each are
    # more key bytes than one shard batch read accepts.
    build_id = "long-keys"
    padding = String.duplicate("k", 40_000)
    ctx = %{ctx | max_value_size: 4_096}

    values =
      Map.new(1..64, fn number ->
        id = "run-#{number}-#{padding}"
        state_key = Keys.state_key(id, "tenant-a")
        put_catalog_member!(ctx, "invoice", state_key, 0)
        {state_key, encoded_record(id, "tenant-a", number)}
      end)

    snapshot_all!(ctx, build_id, 8)
    parent = self()

    read_entries = fn _ctx, 0, keys ->
      send(parent, {:primary_read_keys, length(keys), Enum.sum(Enum.map(keys, &byte_size/1))})
      {:ok, Enum.map(keys, &{Map.fetch!(values, &1), 0})}
    end

    assert {:ok, page} =
             BackfillSource.page(ctx, 0, build_id, "", 64, 8 * 1_024 * 1_024,
               read_entries_fun: read_entries
             )

    assert length(page.records) == 64
    reads = primary_read_keys()
    assert Enum.sum(Enum.map(reads, &elem(&1, 0))) == 64
    assert Enum.all?(reads, fn {count, key_bytes} -> count <= 512 and key_bytes <= 1_048_576 end)
  end

  test "resumes a durable source-catalog cursor without a live keydir", %{ctx: ctx} do
    first_key = Keys.state_key("one", "tenant-a")
    second_key = Keys.state_key("two", "tenant-a")
    put_catalog_member!(ctx, "invoice", first_key, 0)
    put_catalog_member!(ctx, "invoice", second_key, 0)

    assert {:ok, %{done?: false}} =
             BackfillSource.snapshot_page(ctx, 0, "restart-build", 1, 1_024 * 1_024)

    snapshot_all!(ctx, "restart-build", 1)

    assert {:ok, staged} =
             LMDB.prefix_entries(
               lmdb_path(ctx),
               BackfillSource.staging_prefix("restart-build"),
               10
             )

    assert length(staged) == 2
  end

  test "cleanup removes at most one bounded staging page per call", %{ctx: ctx} do
    build_id = "bounded-cleanup"
    prefix = BackfillSource.staging_prefix(build_id)
    path = lmdb_path(ctx)

    ops =
      Enum.map(1..300, fn value ->
        key = prefix <> String.pad_leading(Integer.to_string(value), 4, "0")
        {:put, key, "state-key"}
      end)

    assert :ok = LMDB.write_batch(path, ops)
    assert {:ok, :progress} = BackfillSource.cleanup(ctx, 0, build_id)
    assert {:ok, remaining} = LMDB.prefix_entries(path, prefix, 400)
    assert length(remaining) == 44

    assert :ok = BackfillSource.cleanup(ctx, 0, build_id)
    assert {:ok, []} = LMDB.prefix_entries(path, prefix, 400)
  end

  test "bounds collision reads by the snapshot byte budget", %{ctx: ctx} do
    build_id = "bounded-collision"
    state_key = Keys.state_key("candidate", "tenant-a")
    put_catalog_member!(ctx, "invoice", state_key, 0)

    staging_key =
      BackfillSource.staging_prefix(build_id) <> :crypto.hash(:sha256, state_key)

    assert :ok = LMDB.write_batch(lmdb_path(ctx), [{:put, staging_key, :binary.copy("x", 1_024)}])

    assert {:error, :query_backfill_snapshot_page_too_large} =
             BackfillSource.snapshot_page(ctx, 0, build_id, 1, 512)
  end

  test "snapshot cannot overwrite a staging row replaced after collision validation", %{ctx: ctx} do
    state_key = Keys.state_key("candidate", "tenant-a")
    conflicting_state_key = Keys.state_key("conflicting", "tenant-a")
    put_catalog_member!(ctx, "invoice", state_key, 0)
    snapshot_all!(ctx, "bootstrap-primer", 1)

    build_id = "collision-race"
    staging_key = BackfillSource.staging_prefix(build_id) <> :crypto.hash(:sha256, state_key)
    path = lmdb_path(ctx)

    write_batch = fn ^path, ops ->
      assert :ok = LMDB.write_batch(path, [{:put, staging_key, conflicting_state_key}])
      LMDB.write_batch(path, ops)
    end

    assert {:error, {:compare_failed, ^staging_key}} =
             BackfillSource.snapshot_page(ctx, 0, build_id, 1, 1_024 * 1_024,
               write_batch_fun: write_batch
             )

    assert {:ok, ^conflicting_state_key} = LMDB.get(path, staging_key)
  end

  test "rejects a corrupt durable snapshot completion marker", %{ctx: ctx} do
    build_id = "corrupt-complete"

    marker_key =
      "flow-query-backfill:1:" <>
        :crypto.hash(:sha256, build_id) <> ":snapshot-complete"

    assert :ok = LMDB.write_batch(lmdb_path(ctx), [{:put, marker_key, "another-build"}])

    assert {:error, :invalid_query_backfill_snapshot_complete_marker} =
             BackfillSource.snapshot_page(ctx, 0, build_id, 1, 1_024)
  end

  test "rejects malformed decoded record ownership without raising", %{ctx: ctx} do
    build_id = "malformed-owner"
    state_key = Keys.state_key("candidate", "tenant-a")
    put_catalog_member!(ctx, "invoice", state_key, 0)
    snapshot_all!(ctx, build_id, 1)

    assert {:error, :corrupt_query_backfill_record} =
             BackfillSource.page(ctx, 0, build_id, "", 1, 1_024,
               read_entries_fun: fn _ctx, 0, [^state_key] -> {:ok, [{"encoded", 0}]} end,
               decode_record_fun: fn "encoded" ->
                 {:ok, %{id: "candidate", partition_key: %{malformed: true}}}
               end
             )
  end

  test "binds shared source records to their sealed physical scope", %{ctx: ctx} do
    build_id = "shared-owner"
    scope = <<42::unsigned-big-64>>
    assert {:ok, physical_partition} = StorageScope.physical_partition_key("tenant-a", scope)
    state_key = Keys.state_key("candidate", physical_partition)
    put_catalog_member!(ctx, "invoice", state_key, 0)
    snapshot_all!(ctx, build_id, 1)

    valid = %{
      id: "candidate",
      partition_key: physical_partition,
      system_metadata: scope_metadata(42)
    }

    read_entries = fn _ctx, 0, [^state_key] -> {:ok, [{"encoded", 0}]} end

    assert {:ok, %{records: [%{record: ^valid}]}} =
             BackfillSource.page(ctx, 0, build_id, "", 1, 1_024,
               read_entries_fun: read_entries,
               decode_record_fun: fn "encoded" -> {:ok, valid} end
             )

    forged = Map.put(valid, :system_metadata, scope_metadata(99))

    assert {:error, :corrupt_query_backfill_record} =
             BackfillSource.page(ctx, 0, build_id, "", 1, 1_024,
               read_entries_fun: read_entries,
               decode_record_fun: fn "encoded" -> {:ok, forged} end
             )
  end

  test "rejects staging cursors that exceed the LMDB key boundary", %{ctx: ctx} do
    build_id = "bounded-cursor"
    cursor = BackfillSource.staging_prefix(build_id) <> String.duplicate("x", 512)

    assert {:error, :invalid_query_backfill_cursor} =
             BackfillSource.staging_page(ctx, 0, build_id, cursor, 1, 1_024)
  end

  test "returns an explicit tombstone when a staged state was concurrently deleted", %{ctx: ctx} do
    build_id = "deleted-state"
    state_key = Keys.state_key("deleted", "tenant-a")
    put_catalog_member!(ctx, "invoice", state_key, 0)
    snapshot_all!(ctx, build_id, 1)

    assert {:ok, page} =
             BackfillSource.page(ctx, 0, build_id, "", 1, 1_024,
               read_entries_fun: fn _ctx, 0, [^state_key] -> {:ok, [nil]} end
             )

    assert page.done?
    assert page.scanned_entries == 1

    assert page.records == [
             %{
               state_key: state_key,
               record: nil,
               expire_at_ms: 0,
               projection_guard: :missing
             }
           ]
  end

  test "hydrates a retained terminal query row when the live state key is absent", %{ctx: ctx} do
    build_id = "retained-terminal-state"
    state_key = Keys.state_key("completed", "tenant-a")

    record =
      encoded_record("completed", "tenant-a", 3, "completed")
      |> Ferricstore.Flow.decode_record()

    expiry = 9_000_000_000_000
    put_catalog_member!(ctx, record.type, state_key, 0)

    assert :ok =
             LMDB.write_batch(lmdb_path(ctx), [
               {:put, state_key, query_row_blob!(state_key, record, expiry)}
             ])

    snapshot_all!(ctx, build_id, 1)

    assert {:ok, page} =
             BackfillSource.page(ctx, 0, build_id, "", 1, 1_024 * 1_024,
               read_entries_fun: fn _ctx, 0, [^state_key] -> {:ok, [nil]} end
             )

    assert [projected] = page.records
    assert projected.state_key == state_key
    assert projected.expire_at_ms == expiry
    assert projected.record.id == "completed"
    assert projected.record.state == "completed"
    assert projected.record.version == 3
    assert {:value, encoded_guard} = projected.projection_guard
    assert is_binary(encoded_guard)
  end

  test "does not resurrect a retained nonterminal row after its live state disappears", %{
    ctx: ctx
  } do
    build_id = "stale-active-state"
    state_key = Keys.state_key("stale-active", "tenant-a")

    record =
      encoded_record("stale-active", "tenant-a", 2, "running")
      |> Ferricstore.Flow.decode_record()

    put_catalog_member!(ctx, record.type, state_key, 0)

    assert :ok =
             LMDB.write_batch(lmdb_path(ctx), [
               {:put, state_key, query_row_blob!(state_key, record, 0)}
             ])

    snapshot_all!(ctx, build_id, 1)

    assert {:ok, page} =
             BackfillSource.page(ctx, 0, build_id, "", 1, 1_024 * 1_024,
               read_entries_fun: fn _ctx, 0, [^state_key] -> {:ok, [nil]} end
             )

    assert [tombstone] = page.records
    assert tombstone.state_key == state_key
    assert tombstone.record == nil
    assert tombstone.expire_at_ms == 0
    assert {:value, encoded_guard} = tombstone.projection_guard
    assert is_binary(encoded_guard)
  end

  test "preserves the primary storage expiry for active records", %{ctx: ctx} do
    build_id = "active-expiry"
    state_key = Keys.state_key("active", "tenant-a")
    expiry = 9_000_000_000_000
    encoded = encoded_record("active", "tenant-a", 1)
    put_catalog_member!(ctx, "invoice", state_key, 0)
    snapshot_all!(ctx, build_id, 1)

    assert {:ok, page} =
             BackfillSource.page(ctx, 0, build_id, "", 1, 1_024,
               read_entries_fun: fn _ctx, 0, [^state_key] ->
                 {:ok, [{encoded, expiry}]}
               end
             )

    assert [%{state_key: ^state_key, expire_at_ms: ^expiry}] = page.records
  end

  test "backfills a cold-parked scheduled row without reading its WAL payload", %{ctx: ctx} do
    {state_key, row_blob, park_blob, row} = parked_row!(ctx)
    page = parked_page!(ctx)
    assert [projected] = page.records
    assert projected.record == row.record
    assert projected.projection_guard == {:parked, row_blob, park_blob}
    assert page.hydrated_bytes == byte_size(row_blob) + byte_size(park_blob)

    definition =
      Ferricstore.Flow.Query.IndexDefinition.new!(%{
        id: "parked_by_updated",
        version: 1,
        fields: [{:partition_key, :asc}, {:updated_at_ms, :desc}]
      })

    assert {:ok, %{projected_records: 1}} =
             Ferricstore.Flow.Query.CompositeBackfill.project_page(
               ctx,
               0,
               page.records,
               [definition],
               projection_definitions: [definition],
               read_entries_fun: fn _, _, _ ->
                 flunk("guarded parked metadata needs no WAL read")
               end
             )

    assert {:ok, 1} =
             Ferricstore.Flow.Query.IndexValidation.DataPasses.validate_source_rows(
               ctx,
               0,
               [definition],
               [state_key],
               1_048_576,
               []
             )

    assert :ok =
             LMDB.write_batch(lmdb_path(ctx), [
               {:delete, LMDB.cold_park_key_for_state_key(state_key)}
             ])

    assert {:error, :query_backfill_concurrent_change} =
             Ferricstore.Flow.Query.CompositeBackfill.project_page(
               ctx,
               0,
               page.records,
               [definition],
               projection_definitions: [definition]
             )

    tombstone_page = parked_page!(ctx)
    assert [tombstone] = tombstone_page.records
    assert tombstone.record == nil

    assert :ok =
             LMDB.write_batch(lmdb_path(ctx), [
               {:put, LMDB.cold_park_key_for_state_key(state_key), park_blob}
             ])

    assert {:error, :query_backfill_concurrent_change} =
             Ferricstore.Flow.Query.CompositeBackfill.project_page(
               ctx,
               0,
               tombstone_page.records,
               [definition],
               projection_definitions: [definition]
             )

    assert {:ok, 1} =
             Ferricstore.Flow.Query.IndexValidation.DataPasses.validate_source_rows(
               ctx,
               0,
               [definition],
               [state_key],
               1_048_576,
               []
             )
  end

  test "mismatched or corrupt parked metadata is not treated as deletion", %{ctx: ctx} do
    {state_key, _row_blob, park_blob, row} = parked_row!(ctx)
    park_key = LMDB.cold_park_key_for_state_key(state_key)
    {:ok, park} = LMDB.decode_cold_park(park_blob)
    stale = LMDB.encode_cold_park(%{row.locator | version: row.record.version + 1}, park)
    assert :ok = LMDB.write_batch(lmdb_path(ctx), [{:put, park_key, stale}])
    assert {:error, :query_backfill_concurrent_change} = parked_page(ctx)
    assert :ok = LMDB.write_batch(lmdb_path(ctx), [{:put, park_key, "corrupt"}])
    assert {:error, :corrupt_query_backfill_record} = parked_page(ctx)
  end

  defp parked_row!(ctx) do
    key = Keys.state_key("parked", "tenant-a")
    record = encoded_record("parked", "tenant-a", 3, "queued") |> Ferricstore.Flow.decode_record()
    blob = query_row_blob!(key, record, 0)
    {:ok, row} = QueryRowCodec.decode(blob, key)

    park =
      LMDB.encode_cold_park(row.locator, %{
        state_key: key,
        type: record.type,
        state: record.state,
        partition_key: record.partition_key,
        due_at_ms: record.next_run_at_ms
      })

    put_catalog_member!(ctx, record.type, key, 0)

    assert :ok =
             LMDB.write_batch(lmdb_path(ctx), [
               {:put, key, blob},
               {:put, LMDB.cold_park_key_for_state_key(key), park}
             ])

    snapshot_all!(ctx, "parked-build", 1)
    {key, blob, park, row}
  end

  defp parked_page(ctx) do
    BackfillSource.page(ctx, 0, "parked-build", "", 1, 1_048_576,
      read_entries_fun: fn _, _, [_key] -> {:ok, [nil]} end
    )
  end

  defp parked_page!(ctx) do
    assert {:ok, page} = parked_page(ctx)
    page
  end

  defp snapshot_all!(ctx, build_id, page_size) do
    case BackfillSource.snapshot_page(ctx, 0, build_id, page_size, 1_024 * 1_024) do
      {:ok, %{done?: true}} -> :ok
      {:ok, %{done?: false}} -> snapshot_all!(ctx, build_id, page_size)
    end
  end

  defp staged_records!(ctx, build_id, count, padding) do
    values =
      Map.new(1..count, fn number ->
        id = "run-#{number}"
        state_key = Keys.state_key(id, "tenant-a")
        put_catalog_member!(ctx, "invoice", state_key, 0)

        encoded =
          encoded_record(id, "tenant-a", number)
          |> Ferricstore.Flow.decode_record()
          |> Map.put(:correlation_id, if(padding == "", do: nil, else: padding))
          |> Ferricstore.Flow.encode_record()

        {state_key, encoded}
      end)

    snapshot_all!(ctx, build_id, 256)
    values
  end

  defp primary_read_keys(acc \\ []) do
    receive do
      {:primary_read_keys, count, key_bytes} -> primary_read_keys([{count, key_bytes} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp primary_reads(acc \\ []) do
    receive do
      {:primary_read, count} -> primary_reads([count | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp put_catalog_member!(ctx, type, state_key, generation) do
    catalog_key = Keys.type_catalog_member_key(type, state_key)
    catalog_value = PolicyMigration.encode_catalog(type, state_key, generation)
    projection_key = Keys.policy_catalog_projection_key(type, catalog_key, generation)

    assert :ok =
             LMDB.write_batch(lmdb_path(ctx), [
               {:put, catalog_key, LMDB.encode_value(catalog_value, 0)},
               {:put, projection_key, <<1>>}
             ])
  end

  defp encoded_record(id, partition_key, version, state \\ "failed") do
    %{
      id: id,
      type: "invoice",
      state: state,
      version: version,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: version,
      updated_at_ms: version,
      next_run_at_ms: version,
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
      lease_owner: "",
      lease_token: nil,
      lease_deadline_ms: 0,
      run_state: nil,
      state_enter_seq: version,
      child_groups: %{}
    }
    |> Ferricstore.Flow.encode_record()
  end

  defp query_row_blob!(state_key, record, expire_at_ms) do
    locator =
      Locator.new!(
        flow_id: record.id,
        kind: :state,
        version: record.version,
        raft_index: record.version,
        file_id: {:waraft_apply_projection, record.version},
        offset: 0,
        value_size: 512,
        checksum: :crypto.hash(:sha256, Ferricstore.Flow.encode_record(record)),
        expire_at_ms: expire_at_ms,
        segment_generation: 0,
        frame_size: 1_024
      )

    assert {:ok, encoded} = QueryRowCodec.encode(state_key, record, locator, expire_at_ms)
    encoded
  end

  defp scope_metadata(value), do: %{0x8001 => {1, :uint64, :isolation_scope, value}}

  defp lmdb_path(ctx) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(0)
    |> LMDB.path()
  end
end
