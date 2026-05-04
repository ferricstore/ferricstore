defmodule Ferricstore.Flow.LMDBTest do
  use ExUnit.Case, async: false

  test "stores, reads, overwrites, and deletes raw flow state blobs" do
    path =
      Path.join(System.tmp_dir!(), "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}")

    key = "flow:{flow:test}:state:a"

    on_exit(fn -> File.rm_rf!(path) end)

    assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put, key, "v1"}])
    assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(path, [
               {:put, key, "v2"},
               {:put, key <> ":other", "v3"}
             ])

    assert {:ok, "v2"} = Ferricstore.Flow.LMDB.get(path, key)
    assert {:ok, "v3"} = Ferricstore.Flow.LMDB.get(path, key <> ":other")

    assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:delete, key}])
    assert :not_found = Ferricstore.Flow.LMDB.get(path, key)
  end

  test "batch write can return pre-batch originals for rollback" do
    path =
      Path.join(System.tmp_dir!(), "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}")

    key = "flow:{flow:test}:state:rollback"

    on_exit(fn -> File.rm_rf!(path) end)

    assert {:ok, [{^key, :missing}]} =
             Ferricstore.Flow.LMDB.write_batch_with_originals(path, [{:put_new, key, "v1"}])

    assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)

    assert {:ok, [{^key, {:value, "v1"}}]} =
             Ferricstore.Flow.LMDB.write_batch_with_originals(path, [
               {:put, key, "v2"},
               {:put, key, "v3"}
             ])

    assert {:ok, "v3"} = Ferricstore.Flow.LMDB.get(path, key)
  end
end
