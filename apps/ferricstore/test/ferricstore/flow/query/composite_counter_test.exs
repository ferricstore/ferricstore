defmodule Ferricstore.Flow.Query.CompositeCounterTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.Query.{CompositeCounter, CompositeIndex, IndexDefinition}

  test "counter values bind the full logical prefix and reject digest collisions" do
    definition = definition()
    assert {:ok, prefix} = CompositeIndex.encode_prefix(definition, ["tenant-a", "failed"])
    assert {:ok, other} = CompositeIndex.encode_prefix(definition, ["tenant-a", "running"])
    key = CompositeCounter.key(definition, prefix)
    blob = CompositeCounter.encode_value(prefix, 7)

    assert byte_size(key) <= 511
    assert {:ok, 7} = CompositeCounter.decode_value(blob, prefix)
    assert :error = CompositeCounter.decode_value(blob, other)

    assert_raise ArgumentError, fn ->
      CompositeCounter.encode_value(prefix, -1)
    end
  end

  test "counter values track the bounded subset that can expire" do
    definition = definition()
    assert {:ok, prefix} = CompositeIndex.encode_prefix(definition, ["tenant-a", "failed"])
    blob = CompositeCounter.encode_value(prefix, 7, 3)

    assert {:ok, %{count: 7, expiring_count: 3, physical_count: 7}} =
             CompositeCounter.decode_state(blob, prefix)

    assert {:ok, 7} = CompositeCounter.decode_value(blob, prefix)

    assert_raise ArgumentError, fn ->
      CompositeCounter.encode_value(prefix, 2, 3)
    end
  end

  test "counter values can bind logical membership to a larger physical fanout" do
    definition = definition()
    assert {:ok, prefix} = CompositeIndex.encode_prefix(definition, ["tenant-a"])
    blob = CompositeCounter.encode_value(prefix, 2, 1, 5)

    assert {:ok, %{count: 2, expiring_count: 1, physical_count: 5}} =
             CompositeCounter.decode_state(blob, prefix)

    assert_raise ArgumentError, fn ->
      CompositeCounter.encode_value(prefix, 3, 0, 2)
    end
  end

  test "decodes a storage row only when its key, prefix, and definition agree" do
    definition = definition()
    other_definition = %{definition() | version: 2}
    assert {:ok, prefix} = CompositeIndex.encode_prefix(definition, ["tenant-a", "failed"])
    assert {:ok, other_prefix} = CompositeIndex.encode_prefix(definition, ["tenant-a"])
    key = CompositeCounter.key(definition, prefix)
    blob = CompositeCounter.encode_value(prefix, 7)

    assert {:ok, %{prefix: ^prefix, count: 7}} =
             CompositeCounter.decode_storage_entry(definition, key, blob)

    assert :error = CompositeCounter.decode_storage_entry(definition, key <> <<0>>, blob)

    assert :error =
             CompositeCounter.decode_storage_entry(
               definition,
               key,
               CompositeCounter.encode_value(other_prefix, 7)
             )

    assert :error = CompositeCounter.decode_storage_entry(other_definition, key, blob)
  end

  test "derives declared prefixes for one physical key in stable prefix order" do
    definition = definition()

    record = %{
      id: "run-1",
      partition_key: "tenant-a",
      state: "failed",
      version: 1,
      updated_at_ms: 100
    }

    state_key = Ferricstore.Flow.Keys.state_key("run-1", "tenant-a")
    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 0)
    assert {:ok, tenant} = CompositeIndex.encode_prefix(definition, ["tenant-a"])
    assert {:ok, state} = CompositeIndex.encode_prefix(definition, ["tenant-a", "failed"])

    assert {:ok, [^tenant, ^state]} = CompositeCounter.prefixes_for_key(definition, entry.key)

    assert {:error, :invalid_composite_counter_key} =
             CompositeCounter.prefixes_for_key(definition, entry.key <> <<0>>)
  end

  test "derives one run counter prefix per declared dimension despite trailing fanout" do
    definition =
      IndexDefinition.new!(%{
        id: "runs_by_tag_updated",
        version: 1,
        count_prefixes: [1, 2],
        fields: [
          {:partition_key, :asc},
          {{:attribute, "tags"}, :asc},
          {:updated_at_ms, :desc}
        ]
      })

    record = %{
      id: "run-1",
      partition_key: "tenant-a",
      version: 1,
      updated_at_ms: 100,
      attributes: %{"tags" => ["blue", "green"]}
    }

    state_key = Ferricstore.Flow.Keys.state_key("run-1", "tenant-a")
    assert {:ok, entries} = CompositeIndex.entries(definition, record, state_key, 0)

    assert {:ok, prefixes} =
             CompositeCounter.prefixes_for_keys([definition], Enum.map(entries, & &1.key))

    assert MapSet.size(prefixes) == 3
    assert {:ok, tenant} = CompositeIndex.encode_prefix(definition, ["tenant-a"])
    assert {:ok, blue} = CompositeIndex.encode_prefix(definition, ["tenant-a", "blue"])
    assert {:ok, green} = CompositeIndex.encode_prefix(definition, ["tenant-a", "green"])
    assert prefixes == MapSet.new([{definition, tenant}, {definition, blue}, {definition, green}])
  end

  test "reads declared prefixes in one bounded batch with exact storage accounting" do
    definition = definition()
    path = lmdb_path()
    assert {:ok, failed} = CompositeIndex.encode_prefix(definition, ["tenant-a", "failed"])
    assert {:ok, running} = CompositeIndex.encode_prefix(definition, ["tenant-a", "running"])
    assert {:ok, missing} = CompositeIndex.encode_prefix(definition, ["tenant-a", "missing"])
    failed_blob = CompositeCounter.encode_value(failed, 7)
    running_blob = CompositeCounter.encode_value(running, 3)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, CompositeCounter.key(definition, failed), failed_blob},
               {:put, CompositeCounter.key(definition, running), running_blob}
             ])

    assert {:ok,
            %{
              counts: [7, 0, 3],
              expiring_counts: [0, 0, 0],
              scanned_entries: 3,
              scanned_bytes: scanned_bytes,
              memory_bytes: memory_bytes
            }} =
             CompositeCounter.read_prefixes(
               path,
               definition,
               [failed, missing, running],
               byte_size(failed_blob) + byte_size(running_blob)
             )

    assert scanned_bytes == byte_size(failed_blob) + byte_size(running_blob)
    assert memory_bytes >= scanned_bytes
  end

  test "bounded reads reject oversized, duplicate, undeclared, and mismatched counters" do
    definition = definition()
    path = lmdb_path()
    assert {:ok, failed} = CompositeIndex.encode_prefix(definition, ["tenant-a", "failed"])
    assert {:ok, tenant} = CompositeIndex.encode_prefix(definition, ["tenant-a"])

    assert {:ok, undeclared} =
             CompositeIndex.encode_prefix(definition, ["tenant-a", "failed", 10])

    blob = CompositeCounter.encode_value(failed, 7)

    assert :ok =
             LMDB.write_batch(path, [{:put, CompositeCounter.key(definition, failed), blob}])

    assert {:error, :batch_value_budget_exceeded} =
             CompositeCounter.read_prefixes(path, definition, [failed], byte_size(blob) - 1)

    assert {:error, :invalid_composite_counter_prefixes} =
             CompositeCounter.read_prefixes(path, definition, [failed, failed], 4_096)

    assert {:error, :invalid_composite_counter_prefixes} =
             CompositeCounter.read_prefixes(path, definition, [undeclared], 4_096)

    wrong_blob = CompositeCounter.encode_value(tenant, 7)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, CompositeCounter.key(definition, failed), wrong_blob}
             ])

    assert {:error, :invalid_composite_counter} =
             CompositeCounter.read_prefixes(path, definition, [failed], 4_096)
  end

  defp definition do
    IndexDefinition.new!(%{
      id: "runs_by_state_updated",
      version: 1,
      count_prefixes: [1, 2],
      fields: [
        {:partition_key, :asc},
        {:state, :asc},
        {:updated_at_ms, :desc}
      ]
    })
  end

  defp lmdb_path do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-composite-counter-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(path)
    path
  end
end
