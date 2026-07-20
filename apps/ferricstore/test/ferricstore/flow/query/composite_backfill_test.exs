defmodule Ferricstore.Flow.Query.CompositeBackfillTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB

  alias Ferricstore.Flow.Query.{
    CompositeBackfill,
    CompositeIndex,
    IndexDefinition
  }

  test "projects a bounded record page for every definition in one LMDB transaction" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_composite_backfill_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(data_dir) end)
    ctx = %{data_dir: data_dir, shard_count: 1}

    definitions = [
      IndexDefinition.new!(%{
        id: "by_updated",
        version: 1,
        fields: [{:partition_key, :asc}, {:updated_at_ms, :desc}]
      }),
      IndexDefinition.new!(%{
        id: "by_state_updated",
        version: 1,
        fields: [{:partition_key, :asc}, {:state, :asc}, {:updated_at_ms, :desc}]
      })
    ]

    records = [
      projected_record("one", 1),
      projected_record("two", 2)
    ]

    verify_values = fn _ctx, 0, keys ->
      versions = Map.new(records, &{&1.state_key, &1.record.version})

      {:ok,
       Enum.map(keys, fn key ->
         encoded_record(key, Map.fetch!(versions, key))
       end)}
    end

    assert {:ok,
            %{
              projected_records: 2,
              written_entries: 4,
              write_ops: write_ops,
              written_bytes: written_bytes
            }} =
             CompositeBackfill.project_page(ctx, 0, records, definitions,
               read_values_fun: verify_values
             )

    assert write_ops >= 6
    assert written_bytes > 0

    path = lmdb_path(data_dir)

    for record <- records, definition <- definitions do
      assert {:ok, [entry]} =
               CompositeIndex.entries(
                 definition,
                 record.record,
                 record.state_key,
                 record.expire_at_ms
               )

      assert {:ok, _value} = LMDB.get(path, entry.key)
    end
  end

  test "rejects an oversized page before allocating projection state" do
    records = List.duplicate(projected_record("one", 1), 17)
    assert {:error, :query_backfill_page_too_large} = CompositeBackfill.project_page(%{}, 0, records, [])
  end

  test "retries and then removes a record deleted during backfill" do
    data_dir = tmp_data_dir("delete-race")
    ctx = %{data_dir: data_dir, shard_count: 1}
    definition = definition()
    stale = projected_record("deleted", 1)

    assert {:error, :query_backfill_concurrent_change} =
             CompositeBackfill.project_page(ctx, 0, [stale], [definition],
               read_values_fun: fn _ctx, 0, [state_key] when state_key == stale.state_key ->
                 {:ok, [nil]}
               end
             )

    assert {:ok, [entry]} =
             CompositeIndex.entries(
               definition,
               stale.record,
               stale.state_key,
               stale.expire_at_ms
             )

    assert {:ok, _value} = LMDB.get(lmdb_path(data_dir), entry.key)

    tombstone = %{state_key: stale.state_key, record: nil, expire_at_ms: 0}

    assert {:ok, %{projected_records: 1, written_entries: 0}} =
             CompositeBackfill.project_page(ctx, 0, [tombstone], [definition],
               read_values_fun: fn _ctx, 0, [state_key] when state_key == stale.state_key ->
                 {:ok, [nil]}
               end
             )

    assert :not_found = LMDB.get(lmdb_path(data_dir), entry.key)
    assert :not_found = LMDB.get(lmdb_path(data_dir), CompositeIndex.reverse_key(stale.state_key))
  end

  test "rejects a forged definition before writing its namespace" do
    data_dir = tmp_data_dir("forged-definition")
    ctx = %{data_dir: data_dir, shard_count: 1}
    forged = %{definition() | fingerprint: <<0::256>>}

    assert {:error, :invalid_query_backfill_definitions} =
             CompositeBackfill.project_page(ctx, 0, [projected_record("one", 1)], [forged],
               read_values_fun: fn _ctx, 0, _keys -> {:ok, []} end
             )

    assert {:ok, []} = LMDB.prefix_entries(lmdb_path(data_dir), IndexDefinition.global_storage_prefix(), 10)
  end

  test "rejects aggregate projection operations above the bounded page budget" do
    data_dir = tmp_data_dir("write-budget")
    ctx = %{data_dir: data_dir, shard_count: 1}

    definitions =
      Enum.map(1..16, fn number ->
        IndexDefinition.new!(%{
          id: "by_updated_#{number}",
          version: 1,
          fields: [{:partition_key, :asc}, {:updated_at_ms, :desc}]
        })
      end)

    records =
      Enum.map(1..16, fn number ->
        projected_record("record-#{number}", number)
      end)

    assert {:error, :query_backfill_projection_budget_exceeded} =
             CompositeBackfill.project_page(ctx, 0, records, definitions,
               max_operation_bytes: 10_000,
               read_values_fun: fn _ctx, _shard, _keys -> flunk("write budget checked too late") end
             )

    assert {:ok, []} =
             LMDB.prefix_entries(
               lmdb_path(data_dir),
               IndexDefinition.global_storage_prefix(),
               1
             )
  end

  defp projected_record(id, version) do
    %{
      state_key: Ferricstore.Flow.Keys.state_key(id, "tenant-a"),
      expire_at_ms: 0,
      record: %{
        id: id,
        partition_key: "tenant-a",
        type: "invoice",
        state: "failed",
        updated_at_ms: version,
        version: version
      }
    }
  end

  defp definition do
    IndexDefinition.new!(%{
      id: "by_updated",
      version: 1,
      fields: [{:partition_key, :asc}, {:updated_at_ms, :desc}]
    })
  end

  defp encoded_record(state_key, version) do
    {:ok, _tag, id} = split_state_key(state_key)

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
      partition_key: "tenant-a",
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

  defp split_state_key(state_key) do
    [tag, id] = String.split(state_key, ":s:", parts: 2)
    {:ok, tag, id}
  end

  defp tmp_data_dir(label) do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_composite_backfill_#{label}_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(data_dir) end)
    data_dir
  end

  defp lmdb_path(data_dir) do
    data_dir
    |> Ferricstore.DataDir.shard_data_path(0)
    |> LMDB.path()
  end
end
