defmodule Ferricstore.Flow.Query.CompositeRangeTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, LMDB}

  alias Ferricstore.Flow.Query.{
    CompositeIndex,
    CompositeRange,
    CompositeRangeReader,
    IndexDefinition
  }

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_composite_range_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    definition =
      IndexDefinition.new!(%{
        id: "runs_by_state_updated",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:state, :asc},
          {:updated_at_ms, :desc}
        ]
      })

    %{path: path, definition: definition}
  end

  test "reads one tenant-isolated half-open descending time range", %{
    path: path,
    definition: definition
  } do
    records = [
      record("too-old", "tenant-a", 99),
      record("lower", "tenant-a", 100),
      record("middle", "tenant-a", 150),
      record("upper-minus-one", "tenant-a", 199),
      record("upper", "tenant-a", 200),
      record("too-new", "tenant-a", 201),
      record("foreign", "tenant-b", 150)
    ]

    write_records!(path, definition, records)

    assert {:ok, range} =
             CompositeRange.bounded(
               definition,
               ["tenant-a", "failed"],
               100,
               :inclusive,
               200,
               :exclusive
             )

    assert {:ok,
            %{
              entries: entries,
              cursor: nil,
              exhausted: true,
              scanned_entries: 3,
              scanned_bytes: scanned_bytes
            }} = CompositeRangeReader.read(path, range, nil, 10, 16_384)

    assert Enum.map(entries, & &1.id) == ["upper-minus-one", "middle", "lower"]
    assert scanned_bytes > 0
    assert Enum.sum(Enum.map(entries, & &1.storage_bytes)) == scanned_bytes
  end

  test "resumes at an exact entry boundary without duplicates", %{
    path: path,
    definition: definition
  } do
    write_records!(path, definition, [
      record("one", "tenant-a", 100),
      record("two", "tenant-a", 200),
      record("three", "tenant-a", 300)
    ])

    assert {:ok, range} = CompositeRange.prefix(definition, ["tenant-a", "failed"])

    assert {:ok, %{entries: first, cursor: cursor, exhausted: false}} =
             CompositeRangeReader.read(path, range, nil, 2, 16_384)

    assert is_binary(cursor)
    assert Enum.map(first, & &1.id) == ["three", "two"]

    assert {:ok, %{entries: second, cursor: nil, exhausted: true}} =
             CompositeRangeReader.read(path, range, cursor, 2, 16_384)

    assert Enum.map(second, & &1.id) == ["one"]
  end

  test "fused LMDB range returns validated source rows and exact byte accounting", %{
    path: path,
    definition: definition
  } do
    record = record("one", "tenant-a", 100)
    write_records!(path, definition, [record])
    assert {:ok, range} = CompositeRange.prefix(definition, ["tenant-a", "failed"])

    assert {:ok,
            [
              {storage_key, "one", state_key, 1, 0, storage_bytes, nil}
            ], true, scanned_bytes} =
             LMDB.composite_range_entries_bounded(
               path,
               range.prefix,
               range.after_key,
               range.before_key,
               10,
               16_384
             )

    assert state_key == Keys.state_key("one", "tenant-a")
    assert String.starts_with?(storage_key, range.prefix)
    assert storage_bytes == scanned_bytes

    invalid_state_key = "f:{invalid}:s:one"

    invalid_value =
      <<1, 3::unsigned-big-32, 1::unsigned-big-64, 0::unsigned-big-64, "one",
        invalid_state_key::binary>>

    assert :ok = LMDB.write_batch(path, [{:put, storage_key, invalid_value}])

    assert {:error, :invalid_composite_entry} =
             LMDB.composite_range_entries_bounded(
               path,
               range.prefix,
               range.after_key,
               range.before_key,
               10,
               16_384
             )

    assert {:error, :invalid_composite_entry} =
             LMDB.composite_range_query_rows_bounded(
               path,
               range.prefix,
               Keys.state_key("", "tenant-a"),
               range.after_key,
               range.before_key,
               10,
               16_384,
               16_384
             )
  end

  test "single LMDB snapshot returns composite entries with bounded query rows", %{
    path: path,
    definition: definition
  } do
    records = [record("newer", "tenant-a", 200), record("older", "tenant-a", 100)]
    write_records!(path, definition, records)

    query_rows =
      Enum.map(records, fn record ->
        {Keys.state_key(record.id, record.partition_key), "query-row:" <> record.id}
      end)

    assert :ok =
             LMDB.write_batch(path, Enum.map(query_rows, fn {key, row} -> {:put, key, row} end))

    assert {:ok, range} = CompositeRange.prefix(definition, ["tenant-a", "failed"])
    first_row_bytes = byte_size("query-row:newer")
    state_key_prefix = Keys.state_key("", "tenant-a")

    assert {:ok,
            [
              {{first_storage_key, "newer", _state_key, 1, 0, storage_bytes, nil},
               "query-row:newer"}
            ], false, scanned_bytes, ^first_row_bytes} =
             LMDB.composite_range_query_rows_bounded(
               path,
               range.prefix,
               state_key_prefix,
               range.after_key,
               range.before_key,
               10,
               16_384,
               first_row_bytes
             )

    assert scanned_bytes == storage_bytes

    assert {:ok,
            [
              {{_storage_key, "older", _state_key, 1, 0, _storage_bytes, nil}, "query-row:older"}
            ], true, _scanned_bytes, 15} =
             LMDB.composite_range_query_rows_bounded(
               path,
               range.prefix,
               state_key_prefix,
               first_storage_key,
               range.before_key,
               10,
               16_384,
               16_384
             )

    assert {:error, :batch_value_budget_exceeded} =
             LMDB.composite_range_query_rows_bounded(
               path,
               range.prefix,
               state_key_prefix,
               range.after_key,
               range.before_key,
               10,
               16_384,
               first_row_bytes - 1
             )

    older_state_key = Keys.state_key("older", "tenant-a")
    assert :ok = LMDB.write_batch(path, [{:delete, older_state_key}])

    assert {:ok, [{{_key, "older", _state_key, 1, 0, _bytes, nil}, nil}], true, _scan_bytes, 0} =
             LMDB.composite_range_query_rows_bounded(
               path,
               range.prefix,
               state_key_prefix,
               first_storage_key,
               range.before_key,
               10,
               16_384,
               16_384
             )
  end

  test "fused lookup rejects a composite row outside the expected state-key scope", %{
    path: path,
    definition: definition
  } do
    record = record("one", "tenant-a", 100)
    write_records!(path, definition, [record])
    assert {:ok, range} = CompositeRange.prefix(definition, ["tenant-a", "failed"])
    state_key_prefix = Keys.state_key("", "tenant-a")
    assert {:ok, [{storage_key, _value}]} = LMDB.prefix_entries(path, range.prefix, 1)

    foreign_state_key = Keys.state_key("one", "tenant-b")

    body =
      <<3::unsigned-big-32, 1::unsigned-big-64, 0::unsigned-big-64, "one",
        foreign_state_key::binary>>

    forged_value = <<1, :erlang.crc32(body)::unsigned-big-32, body::binary>>

    assert :ok =
             LMDB.write_batch(path, [
               {:put, storage_key, forged_value},
               {:put, foreign_state_key, "foreign-query-row"}
             ])

    assert {:error, :invalid_composite_entry} =
             LMDB.composite_range_query_rows_bounded(
               path,
               range.prefix,
               state_key_prefix,
               range.after_key,
               range.before_key,
               10,
               16_384,
               16_384
             )
  end

  test "fused reader keeps query rows aligned across cursor pages", %{
    path: path,
    definition: definition
  } do
    records = [
      record("newest", "tenant-a", 300),
      record("middle", "tenant-a", 200),
      record("oldest", "tenant-a", 100)
    ]

    write_records!(path, definition, records)

    assert :ok =
             LMDB.write_batch(
               path,
               Enum.map(records, fn record ->
                 {:put, Keys.state_key(record.id, record.partition_key), "row:" <> record.id}
               end)
             )

    assert {:ok, range} = CompositeRange.prefix(definition, ["tenant-a", "failed"])
    state_key_prefix = Keys.state_key("", "tenant-a")

    assert {:ok,
            %{
              entries: first_entries,
              query_row_values: ["row:newest", "row:middle"],
              query_row_bytes: first_value_bytes,
              cursor: cursor,
              exhausted: false
            }} =
             CompositeRangeReader.read_query_rows(
               path,
               range,
               state_key_prefix,
               nil,
               2,
               16_384,
               16_384
             )

    assert Enum.map(first_entries, & &1.id) == ["newest", "middle"]
    assert first_value_bytes == byte_size("row:newest") + byte_size("row:middle")
    assert is_binary(cursor)

    assert {:ok,
            %{
              entries: [%{id: "oldest"}],
              query_row_values: ["row:oldest"],
              query_row_bytes: 10,
              cursor: nil,
              exhausted: true
            }} =
             CompositeRangeReader.read_query_rows(
               path,
               range,
               state_key_prefix,
               cursor,
               2,
               16_384,
               16_384
             )
  end

  test "fused QueryRow lookup uses the compact physical key for a long state key", %{
    path: path,
    definition: definition
  } do
    id = String.duplicate("x", 600)
    record = record(id, "tenant-a", 100)
    state_key = Keys.state_key(id, "tenant-a")
    write_records!(path, definition, [record])
    assert :ok = LMDB.write_batch(path, [{:put, state_key, "long-query-row"}])
    assert {:ok, range} = CompositeRange.prefix(definition, ["tenant-a", "failed"])

    assert {:ok, [{{_key, ^id, ^state_key, 1, 0, _bytes, nil}, "long-query-row"}], true,
            _scan_bytes, 14} =
             LMDB.composite_range_query_rows_bounded(
               path,
               range.prefix,
               Keys.state_key("", "tenant-a"),
               range.after_key,
               range.before_key,
               1,
               16_384,
               16_384
             )
  end

  test "fused LMDB range returns a validated covering record", %{path: path} do
    definition =
      IndexDefinition.new!(%{
        id: "runs_by_state_updated_covering",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:state, :asc},
          {:updated_at_ms, :desc}
        ],
        covering_fields: [:partition_key, :run_id, :state, :updated_at_ms, :version]
      })

    write_records!(path, definition, [record("one", "tenant-a", 100)])
    assert {:ok, range} = CompositeRange.prefix(definition, ["tenant-a", "failed"])

    assert {:ok,
            %{
              entries: [
                %{
                  id: "one",
                  record_version: 1,
                  covering_record: %{
                    id: "one",
                    partition_key: "tenant-a",
                    state: "failed",
                    updated_at_ms: 100,
                    version: 1
                  }
                }
              ]
            }} = CompositeRangeReader.read(path, range, nil, 10, 16_384)
  end

  test "maps inclusive bounds correctly for an ascending ordered field", %{path: path} do
    definition =
      IndexDefinition.new!(%{
        id: "runs_by_priority",
        version: 1,
        fields: [{:partition_key, :asc}, {:priority, :asc}]
      })

    write_records!(path, definition, [
      record("one", "tenant-a", 0, 1),
      record("two", "tenant-a", 0, 2),
      record("three", "tenant-a", 0, 3),
      record("four", "tenant-a", 0, 4)
    ])

    assert {:ok, range} =
             CompositeRange.bounded(
               definition,
               ["tenant-a"],
               2,
               :exclusive,
               4,
               :inclusive
             )

    assert {:ok, %{entries: entries, exhausted: true}} =
             CompositeRangeReader.read(path, range, nil, 10, 16_384)

    assert Enum.map(entries, & &1.id) == ["three", "four"]
  end

  test "rejects malformed range shapes and cursors", %{path: path, definition: definition} do
    assert {:error, :range_field_not_ordered} =
             CompositeRange.bounded(definition, ["tenant-a"], 1, :inclusive, 2, :inclusive)

    assert {:error, :invalid_range_order} =
             CompositeRange.bounded(
               definition,
               ["tenant-a", "failed"],
               200,
               :inclusive,
               100,
               :inclusive
             )

    assert {:error, :invalid_index_value_type} =
             CompositeRange.bounded(
               definition,
               ["tenant-a", "failed"],
               "100",
               :inclusive,
               "200",
               :inclusive
             )

    assert {:ok, range} = CompositeRange.prefix(definition, ["tenant-a"])

    assert {:error, :invalid_composite_cursor} =
             CompositeRangeReader.read(path, range, "foreign-key", 10, 16_384)

    assert {:error, :invalid_composite_cursor} =
             CompositeRangeReader.read(
               path,
               range,
               range.prefix <> String.duplicate("x", 512),
               10,
               16_384
             )
  end

  test "reader rejects a forged range outside the composite index keyspace", %{
    path: path,
    definition: definition
  } do
    assert {:ok, [entry]} =
             CompositeIndex.entries(
               definition,
               record("run-1", "tenant-a", 100),
               Keys.state_key("run-1", "tenant-a"),
               0
             )

    identity = binary_part(entry.key, byte_size(entry.key) - 33, 33)
    assert :ok = LMDB.write_batch(path, [{:put, "ordinary:" <> identity, entry.value}])

    forged = %CompositeRange{
      index_id: definition.id,
      index_version: definition.version,
      prefix: "ordinary:",
      after_key: "",
      before_key: ""
    }

    assert {:error, :invalid_composite_range} =
             CompositeRangeReader.read(path, forged, nil, 10, 16_384)
  end

  test "rejects forged definitions before constructing physical ranges", %{
    definition: definition
  } do
    forged = %{definition | fingerprint: <<0::256>>}

    assert {:error, :invalid_index_definition} =
             CompositeRange.prefix(forged, ["tenant-a", "failed"])

    assert {:error, :invalid_index_definition} =
             CompositeRange.bounded(
               forged,
               ["tenant-a", "failed"],
               100,
               :inclusive,
               200,
               :exclusive
             )
  end

  test "fails closed on a corrupt projected value", %{path: path, definition: definition} do
    assert {:ok, range} = CompositeRange.prefix(definition, ["tenant-a", "failed"])
    assert {:ok, key_prefix} = CompositeIndex.encode_prefix(definition, ["tenant-a", "failed"])

    assert :ok =
             LMDB.write_batch(path, [{:put, key_prefix <> <<0x30, 0::64, 0x60, 0::256>>, "bad"}])

    assert {:error, :invalid_composite_entry} =
             CompositeRangeReader.read(path, range, nil, 10, 16_384)

    assert {:error, :invalid_composite_entry} =
             CompositeRangeReader.read_query_rows(
               path,
               range,
               Keys.state_key("", "tenant-a"),
               nil,
               10,
               16_384,
               16_384
             )
  end

  defp write_records!(path, definition, records) do
    ops =
      Enum.flat_map(records, fn record ->
        assert {:ok, entries} =
                 CompositeIndex.entries(
                   definition,
                   record,
                   Keys.state_key(record.id, record.partition_key),
                   0
                 )

        Enum.map(entries, &{:put, &1.key, &1.value})
      end)

    assert :ok = LMDB.write_batch(path, ops)
  end

  defp record(id, tenant, updated_at_ms, priority \\ 1) do
    %{
      id: id,
      partition_key: tenant,
      state: "failed",
      updated_at_ms: updated_at_ms,
      priority: priority,
      version: 1
    }
  end
end
