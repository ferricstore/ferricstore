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
