defmodule Ferricstore.Flow.Query.CompositeProjectionTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, LMDB}

  alias Ferricstore.Flow.Query.{
    CompositeCounter,
    CompositeIndex,
    CompositeProjection,
    IndexDefinition
  }

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_composite_projection_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    definition =
      IndexDefinition.new!(%{
        id: "runs_by_state_updated",
        version: 1,
        count_prefixes: [1, 2],
        fields: [{:partition_key, :asc}, {:state, :asc}, {:updated_at_ms, :desc}]
      })

    %{path: path, definition: definition}
  end

  test "atomically replaces stale entry keys and reverse ownership", %{
    path: path,
    definition: definition
  } do
    state_key = Keys.state_key("run-1", "tenant-a")

    assert {:ok, first_ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record("run-1", "running", 100),
               0,
               [definition],
               CompositeProjection.new_cache()
             )

    assert :ok = LMDB.write_batch(path, first_ops)
    assert {:ok, [first_key]} = reverse_keys(path, state_key)
    assert {:ok, _value} = LMDB.get(path, first_key)
    assert {:ok, 1} = CompositeCounter.read(path, definition, nil, ["tenant-a"])
    assert {:ok, 1} = CompositeCounter.read(path, definition, nil, ["tenant-a", "running"])

    assert {:ok, second_ops, _acc} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record("run-1", "failed", 200),
               0,
               [definition],
               CompositeProjection.new_cache()
             )

    assert {:delete, first_key} in second_ops
    assert :ok = LMDB.write_batch(path, second_ops)
    assert LMDB.get(path, first_key) == :not_found
    assert {:ok, [second_key]} = reverse_keys(path, state_key)
    refute second_key == first_key
    assert {:ok, 1} = CompositeCounter.read(path, definition, nil, ["tenant-a"])
    assert {:ok, 0} = CompositeCounter.read(path, definition, nil, ["tenant-a", "running"])
    assert {:ok, 1} = CompositeCounter.read(path, definition, nil, ["tenant-a", "failed"])
  end

  test "multiple updates in one LMDB transaction use the pending reverse value", %{
    path: path,
    definition: definition
  } do
    state_key = Keys.state_key("run-1", "tenant-a")

    assert {:ok, first_ops, cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record("run-1", "running", 100),
               0,
               [definition],
               CompositeProjection.new_cache()
             )

    assert {:ok, second_ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record("run-1", "failed", 200),
               0,
               [definition],
               cache
             )

    assert :ok = LMDB.write_batch(path, first_ops ++ second_ops)
    assert {:ok, [only_key]} = reverse_keys(path, state_key)
    assert {:ok, value} = LMDB.get(path, only_key)
    assert {:ok, %{record_version: 2}} = CompositeIndex.decode_entry_value(value)
  end

  test "compare guards reject a concurrent reverse replacement", %{
    path: path,
    definition: definition
  } do
    state_key = Keys.state_key("run-1", "tenant-a")

    assert {:ok, ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record("run-1", "running", 100),
               0,
               [definition],
               CompositeProjection.new_cache()
             )

    reverse_key = CompositeIndex.reverse_key(state_key)

    concurrent =
      CompositeIndex.encode_reverse_value(state_key, [fake_entry_key(definition, "run-1")])

    assert :ok = LMDB.write_batch(path, [{:put, reverse_key, concurrent}])

    assert {:error, {:compare_failed, ^reverse_key}} = LMDB.write_batch(path, ops)
  end

  test "record deletion removes every owned entry", %{path: path, definition: definition} do
    state_key = Keys.state_key("run-1", "tenant-a")

    assert {:ok, ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record("run-1", "running", 100),
               0,
               [definition],
               CompositeProjection.new_cache()
             )

    assert :ok = LMDB.write_batch(path, ops)
    assert {:ok, keys} = reverse_keys(path, state_key)

    assert {:ok, delete_ops, _cache} =
             CompositeProjection.remove(
               path,
               state_key,
               [definition],
               CompositeProjection.new_cache()
             )

    assert :ok = LMDB.write_batch(path, delete_ops)
    assert LMDB.get(path, CompositeIndex.reverse_key(state_key)) == :not_found
    assert Enum.all?(keys, &(LMDB.get(path, &1) == :not_found))
    assert {:ok, 0} = CompositeCounter.read(path, definition, nil, ["tenant-a"])
    assert {:ok, 0} = CompositeCounter.read(path, definition, nil, ["tenant-a", "running"])
  end

  test "multiple records in one write batch compose guarded counter deltas", %{
    path: path,
    definition: definition
  } do
    cache = CompositeProjection.new_cache()

    assert {:ok, first_ops, cache} =
             CompositeProjection.reconcile(
               path,
               Keys.state_key("run-1", "tenant-a"),
               record("run-1", "failed", 100),
               0,
               [definition],
               cache
             )

    assert {:ok, second_ops, _cache} =
             CompositeProjection.reconcile(
               path,
               Keys.state_key("run-2", "tenant-a"),
               record("run-2", "failed", 100),
               0,
               [definition],
               cache
             )

    assert :ok = LMDB.write_batch(path, first_ops ++ second_ops)
    assert {:ok, 2} = CompositeCounter.read(path, definition, nil, ["tenant-a"])
    assert {:ok, 2} = CompositeCounter.read(path, definition, nil, ["tenant-a", "failed"])
  end

  test "fails closed when reverse ownership would underflow a missing counter", %{
    path: path,
    definition: definition
  } do
    state_key = Keys.state_key("run-1", "tenant-a")

    assert {:ok, ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record("run-1", "running", 100),
               0,
               [definition],
               CompositeProjection.new_cache()
             )

    assert :ok = LMDB.write_batch(path, ops)
    assert {:ok, prefix} = CompositeIndex.encode_prefix(definition, ["tenant-a", "running"])
    counter_key = CompositeCounter.key(definition, prefix)
    assert :ok = LMDB.write_batch(path, [{:delete, counter_key}])

    assert {:error, :composite_counter_underflow} =
             CompositeProjection.remove(
               path,
               state_key,
               [definition],
               CompositeProjection.new_cache()
             )

    assert {:ok, [_owned_entry]} = reverse_keys(path, state_key)
  end

  test "counter compare guards reject a concurrent projection without partial index writes", %{
    path: path,
    definition: definition
  } do
    state_key = Keys.state_key("run-1", "tenant-a")

    assert {:ok, first_ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record("run-1", "running", 100),
               0,
               [definition],
               CompositeProjection.new_cache()
             )

    assert :ok = LMDB.write_batch(path, first_ops)
    assert {:ok, [old_entry]} = reverse_keys(path, state_key)

    assert {:ok, update_ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record("run-1", "failed", 200),
               0,
               [definition],
               CompositeProjection.new_cache()
             )

    assert {:ok, failed_prefix} =
             CompositeIndex.encode_prefix(definition, ["tenant-a", "failed"])

    failed_counter = CompositeCounter.key(definition, failed_prefix)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, failed_counter, CompositeCounter.encode_value(failed_prefix, 1)}
             ])

    assert {:error, {:compare_failed, ^failed_counter}} = LMDB.write_batch(path, update_ops)
    assert {:ok, [^old_entry]} = reverse_keys(path, state_key)
    assert {:ok, _old_value} = LMDB.get(path, old_entry)
  end

  defp reverse_keys(path, state_key) do
    with {:ok, value} <- LMDB.get(path, CompositeIndex.reverse_key(state_key)) do
      CompositeIndex.decode_reverse_value(value, state_key)
    end
  end

  defp fake_entry_key(definition, id) do
    IndexDefinition.storage_prefix(definition) <>
      <<0x60, :crypto.hash(:sha256, id)::binary-size(32)>>
  end

  defp record(id, state, updated_at_ms) do
    %{
      id: id,
      partition_key: "tenant-a",
      type: "invoice",
      state: state,
      version: div(updated_at_ms, 100),
      updated_at_ms: updated_at_ms
    }
  end
end
