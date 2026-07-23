defmodule Ferricstore.Flow.Query.BackfillSourceTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, LMDB, PolicyMigration}
  alias Ferricstore.Flow.Query.BackfillSource

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
    assert page.records == [%{state_key: state_key, record: nil, expire_at_ms: 0}]
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

  defp snapshot_all!(ctx, build_id, page_size) do
    case BackfillSource.snapshot_page(ctx, 0, build_id, page_size, 1_024 * 1_024) do
      {:ok, %{done?: true}} -> :ok
      {:ok, %{done?: false}} -> snapshot_all!(ctx, build_id, page_size)
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

  defp encoded_record(id, partition_key, version) do
    %{
      id: id,
      type: "invoice",
      state: "failed",
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

  defp lmdb_path(ctx) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(0)
    |> LMDB.path()
  end
end
