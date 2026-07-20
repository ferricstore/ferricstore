defmodule Ferricstore.Flow.Query.LineageReadTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, LMDB, LMDBWriter}
  alias Ferricstore.Flow.Query.{Engine, MandatoryScope, Request}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  test "correlation pages seek backward by the exact update tuple" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    unique = System.unique_integer([:positive, :monotonic])
    partition_key = "query-correlation-tenant-#{unique}"
    correlation_id = "query-correlation-#{unique}"
    record_ids = Enum.map(1..4, &"query-correlation-run-#{unique}-#{&1}")

    try do
      Enum.each(record_ids, fn id ->
        assert :ok =
                 Ferricstore.Flow.create(ctx, id,
                   type: "query-correlation",
                   partition_key: partition_key,
                   correlation_id: correlation_id,
                   now_ms: 1_000
                 )
      end)

      request = lineage_request(:correlation_id, correlation_id, partition_key, :desc, 2)

      assert {:ok, first} =
               Engine.execute_lineage_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 nil
               )

      assert first.has_more
      assert first.scanned_entries <= 6

      assert first.memory_high_water_bytes >=
               :erlang.external_size(first.records, minor_version: 2)

      assert {:ok, second} =
               Engine.execute_lineage_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 first.continuation
               )

      refute second.has_more
      assert second.scanned_entries <= 6

      assert second.memory_high_water_bytes >=
               :erlang.external_size(second.records, minor_version: 2)

      records = first.records ++ second.records
      ids = Enum.map(records, & &1.id)
      keys = Enum.map(records, &{&1.updated_at_ms, &1.id})

      assert Enum.sort(ids) == Enum.sort(record_ids)
      assert length(ids) == length(Enum.uniq(ids))
      assert keys == Enum.sort(keys, :desc)
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "expired cold rows consume the scan window without query-side deletion" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    unique = System.unique_integer([:positive, :monotonic])
    partition_key = "query-lineage-expired-tenant-#{unique}"
    parent_id = "query-lineage-expired-parent-#{unique}"
    live_id = "query-lineage-expired-live-#{unique}"

    try do
      assert :ok =
               Ferricstore.Flow.create(ctx, live_id,
                 type: "query-lineage-expired",
                 partition_key: partition_key,
                 parent_flow_id: parent_id,
                 now_ms: 3_000
               )

      index_key = Keys.parent_index_key(parent_id, partition_key)
      {path, shard_index} = lmdb_location(ctx, index_key)
      assert :ok = LMDBWriter.flush(ctx.name, shard_index)

      {ops, expired} =
        Enum.map_reduce(1..3, [], fn number, acc ->
          id = "query-lineage-expired-missing-#{unique}-#{number}"

          {key, value} =
            LMDB.query_index_entry(
              index_key,
              id,
              1_000 + number,
              1,
              Keys.state_key(id, partition_key)
            )

          {{:put, key, value}, [{key, value} | acc]}
        end)

      assert :ok = LMDB.write_batch(path, ops)

      request = lineage_request(:parent_flow_id, parent_id, partition_key, :asc, 2)

      assert {:error, :query_scan_budget_exceeded} =
               Engine.execute_lineage_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 nil
               )

      Enum.each(expired, fn {key, value} ->
        assert {:ok, ^value} = LMDB.get(path, key)
      end)
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "dangling lineage index references fail closed" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    unique = System.unique_integer([:positive, :monotonic])
    partition_key = "query-lineage-stale-tenant-#{unique}"
    parent_id = "query-lineage-stale-parent-#{unique}"
    missing_id = "query-lineage-stale-run-#{unique}"

    try do
      index_key = Keys.parent_index_key(parent_id, partition_key)
      {path, shard_index} = lmdb_location(ctx, index_key)
      assert :ok = LMDBWriter.flush(ctx.name, shard_index)

      {key, value} =
        LMDB.query_index_entry(
          index_key,
          missing_id,
          1_000,
          0,
          Keys.state_key(missing_id, partition_key)
        )

      assert :ok = LMDB.write_batch(path, [{:put, key, value}])

      request = lineage_request(:parent_flow_id, parent_id, partition_key, :asc, 1)

      assert {:error, :query_storage_inconsistent} =
               Engine.execute_lineage_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 nil
               )
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "a dangling lookahead cannot produce a false has-more cursor" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    unique = System.unique_integer([:positive, :monotonic])
    partition_key = "query-lineage-lookahead-tenant-#{unique}"
    parent_id = "query-lineage-lookahead-parent-#{unique}"
    live_id = "query-lineage-lookahead-live-#{unique}"
    missing_id = "query-lineage-lookahead-missing-#{unique}"

    try do
      assert :ok =
               Ferricstore.Flow.create(ctx, live_id,
                 type: "query-lineage-lookahead",
                 partition_key: partition_key,
                 parent_flow_id: parent_id,
                 now_ms: 1_000
               )

      index_key = Keys.parent_index_key(parent_id, partition_key)
      {path, shard_index} = lmdb_location(ctx, index_key)
      assert :ok = LMDBWriter.flush(ctx.name, shard_index)

      {key, value} =
        LMDB.query_index_entry(
          index_key,
          missing_id,
          2_000,
          0,
          Keys.state_key(missing_id, partition_key)
        )

      assert :ok = LMDB.write_batch(path, [{:put, key, value}])

      request = lineage_request(:parent_flow_id, parent_id, partition_key, :asc, 1)

      assert {:error, :query_storage_inconsistent} =
               Engine.execute_lineage_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 nil
               )
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "lineage boundaries reject timestamps outside the exact ordered-index domain" do
    ctx = IsolatedInstance.checkout(shard_count: 1)

    try do
      request = lineage_request(:parent_flow_id, "parent", "tenant", :asc, 1)

      assert {:error, :query_cursor_invalid} =
               Engine.execute_lineage_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 {9_007_199_254_740_992, "run"}
               )
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "an existing root with an inexact ordering timestamp fails closed" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    unique = System.unique_integer([:positive, :monotonic])
    partition_key = "query-root-inexact-tenant-#{unique}"
    root_id = "query-root-inexact-#{unique}"

    try do
      assert :ok =
               Ferricstore.Flow.create(ctx, root_id,
                 type: "query-root-inexact",
                 partition_key: partition_key,
                 now_ms: 1_000
               )

      state_key = Keys.state_key(root_id, partition_key)
      encoded = Router.get(ctx, state_key)
      assert is_binary(encoded)

      corrupted =
        encoded
        |> Ferricstore.Flow.decode_record()
        |> Map.put(:updated_at_ms, 9_007_199_254_740_992)
        |> Ferricstore.Flow.Codec.encode_record_elixir()

      assert :ok = Router.put(ctx, state_key, corrupted, 0)

      request = lineage_request(:root_flow_id, root_id, partition_key, :asc, 1)

      assert {:error, :query_storage_inconsistent} =
               Engine.execute_lineage_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 nil
               )
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  defp lineage_request(field, value, partition_key, direction, limit) do
    Request.collection(
      :execute,
      [eq(field, value), eq(:partition_key, partition_key)],
      [{:updated_at_ms, direction}],
      limit,
      :record
    )
  end

  defp lmdb_location(ctx, index_key) do
    shard_index = Router.shard_for(ctx, index_key)

    path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> LMDB.path()

    {path, shard_index}
  end

  defp eq(field, value), do: {:eq, field, {:literal, :keyword, value}}
end
