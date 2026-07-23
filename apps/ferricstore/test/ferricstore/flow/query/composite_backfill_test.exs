defmodule Ferricstore.Flow.Query.CompositeBackfillTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB

  alias Ferricstore.Flow.Query.{
    CompositeBackfill,
    CompositeCounter,
    CompositeIndex,
    IndexDefinition
  }

  defmodule Provider do
    @behaviour FerricStore.Flow.QueryIndexProvider

    @impl true
    def snapshot(%{test_pid: test_pid, definitions: definitions}, shard_index) do
      send(test_pid, {:projection_snapshot, shard_index})

      indexes =
        Enum.map(definitions, fn definition ->
          Ferricstore.Flow.Query.RegisteredIndex.new!(definition, :active)
        end)

      {:ok,
       Ferricstore.Flow.Query.RegistrySnapshot.new!(%{
         epoch: 1,
         catalog_version: 1,
         indexes: indexes
       })}
    end
  end

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

    verify_entries = fn _ctx, 0, keys ->
      versions = Map.new(records, &{&1.state_key, &1.record.version})

      {:ok,
       Enum.map(keys, fn key ->
         {encoded_record(key, Map.fetch!(versions, key)), 0}
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
               read_entries_fun: verify_entries
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

  test "backfilling a new definition preserves existing index and counter projections" do
    data_dir = tmp_data_dir("preserve-existing")
    record = projected_record("one", 1)

    existing =
      IndexDefinition.new!(%{
        id: "existing_by_state",
        version: 1,
        fields: [{:partition_key, :asc}, {:state, :asc}],
        count_prefixes: [2]
      })

    added =
      IndexDefinition.new!(%{
        id: "added_by_updated",
        version: 1,
        fields: [{:partition_key, :asc}, {:updated_at_ms, :desc}],
        count_prefixes: [1]
      })

    ctx = %{
      data_dir: data_dir,
      shard_count: 1,
      query_index_provider: Provider,
      test_pid: self(),
      definitions: [existing]
    }

    read_entries = fn _ctx, 0, [state_key] ->
      {:ok, [{encoded_record(state_key, record.record.version), record.expire_at_ms}]}
    end

    assert {:ok, _metrics} =
             CompositeBackfill.project_page(ctx, 0, [record], [existing],
               read_entries_fun: read_entries
             )

    assert_received {:projection_snapshot, 0}
    refute_received {:projection_snapshot, 0}

    assert {:ok, [existing_entry]} =
             CompositeIndex.entries(
               existing,
               record.record,
               record.state_key,
               record.expire_at_ms
             )

    path = lmdb_path(data_dir)
    assert {:ok, _value} = LMDB.get(path, existing_entry.key)
    assert {:ok, 1} = CompositeCounter.read(path, existing, nil, ["tenant-a", "failed"])

    build_ctx = %{ctx | definitions: [existing, added]}

    assert {:ok, _metrics} =
             CompositeBackfill.project_page(build_ctx, 0, [record], [added],
               read_entries_fun: read_entries
             )

    assert_received {:projection_snapshot, 0}
    refute_received {:projection_snapshot, 0}

    assert {:ok, [added_entry]} =
             CompositeIndex.entries(added, record.record, record.state_key, record.expire_at_ms)

    assert {:ok, _value} = LMDB.get(path, existing_entry.key)
    assert {:ok, _value} = LMDB.get(path, added_entry.key)
    assert {:ok, 1} = CompositeCounter.read(path, existing, nil, ["tenant-a", "failed"])
    assert {:ok, 1} = CompositeCounter.read(path, added, nil, ["tenant-a"])

    assert {:ok, %{keys: keys}} =
             path
             |> LMDB.get(CompositeIndex.reverse_key(record.state_key))
             |> then(fn {:ok, blob} ->
               CompositeIndex.decode_reverse_state(blob, record.state_key)
             end)

    assert Enum.sort(keys) == Enum.sort([existing_entry.key, added_entry.key])

    tombstone = %{state_key: record.state_key, record: nil, expire_at_ms: 0}

    assert {:ok, _metrics} =
             CompositeBackfill.project_page(build_ctx, 0, [tombstone], [added],
               read_entries_fun: fn _ctx, 0, [state_key]
                                    when state_key == record.state_key ->
                 {:ok, [nil]}
               end
             )

    assert_received {:projection_snapshot, 0}
    refute_received {:projection_snapshot, 0}

    assert {:ok, _value} = LMDB.get(path, existing_entry.key)
    assert :not_found = LMDB.get(path, added_entry.key)
    assert {:ok, 1} = CompositeCounter.read(path, existing, nil, ["tenant-a", "failed"])
    assert {:ok, 0} = CompositeCounter.read(path, added, nil, ["tenant-a"])

    assert {:ok, %{keys: [preserved_key]}} =
             path
             |> LMDB.get(CompositeIndex.reverse_key(record.state_key))
             |> then(fn {:ok, blob} ->
               CompositeIndex.decode_reverse_state(blob, record.state_key)
             end)

    assert preserved_key == existing_entry.key
  end

  test "projects the benchmarked 64-record page and reuses existing reverse ownership" do
    data_dir = tmp_data_dir("page-64")
    ctx = %{data_dir: data_dir, shard_count: 1}
    definition = definition()
    records = Enum.map(1..64, &projected_record("record-#{&1}", &1))

    read_entries = fn _ctx, 0, state_keys ->
      versions = Map.new(records, &{&1.state_key, &1.record.version})

      {:ok,
       Enum.map(state_keys, fn state_key ->
         {encoded_record(state_key, Map.fetch!(versions, state_key)), 0}
       end)}
    end

    assert CompositeBackfill.max_page_records() == 64

    for _pass <- 1..2 do
      assert {:ok, %{projected_records: 64, written_entries: 64}} =
               CompositeBackfill.project_page(ctx, 0, records, [definition],
                 read_entries_fun: read_entries
               )
    end

    assert {:ok, 64} =
             LMDB.prefix_count(lmdb_path(data_dir), IndexDefinition.storage_prefix(definition))
  end

  test "rejects an oversized page before allocating projection state" do
    records = List.duplicate(projected_record("one", 1), 65)

    assert {:error, :query_backfill_page_too_large} =
             CompositeBackfill.project_page(%{}, 0, records, [])
  end

  test "retries and then removes a record deleted during backfill" do
    data_dir = tmp_data_dir("delete-race")
    ctx = %{data_dir: data_dir, shard_count: 1}
    definition = definition()
    stale = projected_record("deleted", 1)

    assert {:error, :query_backfill_concurrent_change} =
             CompositeBackfill.project_page(ctx, 0, [stale], [definition],
               read_entries_fun: fn _ctx, 0, [state_key] when state_key == stale.state_key ->
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
               read_entries_fun: fn _ctx, 0, [state_key] when state_key == stale.state_key ->
                 {:ok, [nil]}
               end
             )

    assert :not_found = LMDB.get(lmdb_path(data_dir), entry.key)
    assert :not_found = LMDB.get(lmdb_path(data_dir), CompositeIndex.reverse_key(stale.state_key))
  end

  test "detects an expiry-only concurrent change after projection" do
    data_dir = tmp_data_dir("expiry-race")
    ctx = %{data_dir: data_dir, shard_count: 1}
    record = %{projected_record("expiring", 1) | expire_at_ms: 1_000}

    assert {:error, :query_backfill_concurrent_change} =
             CompositeBackfill.project_page(ctx, 0, [record], [definition()],
               read_entries_fun: fn _ctx, 0, [state_key] when state_key == record.state_key ->
                 {:ok, [{encoded_record(state_key, record.record.version), 2_000}]}
               end
             )
  end

  test "rejects a forged definition before writing its namespace" do
    data_dir = tmp_data_dir("forged-definition")
    ctx = %{data_dir: data_dir, shard_count: 1}
    forged = %{definition() | fingerprint: <<0::256>>}

    assert {:error, :invalid_query_backfill_definitions} =
             CompositeBackfill.project_page(ctx, 0, [projected_record("one", 1)], [forged],
               read_entries_fun: fn _ctx, 0, _keys -> {:ok, []} end
             )

    assert {:ok, []} =
             LMDB.prefix_entries(lmdb_path(data_dir), IndexDefinition.global_storage_prefix(), 10)
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
               read_entries_fun: fn _ctx, _shard, _keys ->
                 flunk("write budget checked too late")
               end
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
