defmodule Ferricstore.Flow.LMDBCompareBatchTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDB

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "flow_lmdb_compare_batch_#{System.unique_integer([:positive])}"
      )

    path = LMDB.path(root)

    on_exit(fn ->
      _ = LMDB.release(path, 1_000)
      File.rm_rf(root)
    end)

    %{path: path}
  end

  test "compare and writes commit atomically when the expected value matches", %{path: path} do
    assert :ok = LMDB.write_batch(path, [{:put, "park", "v1"}])

    assert :ok =
             LMDB.write_batch(path, [
               {:compare, "park", "v1"},
               {:put, "park", "v2"},
               {:put, "reverse", "park"}
             ])

    assert {:ok, "v2"} = LMDB.get(path, "park")
    assert {:ok, "park"} = LMDB.get(path, "reverse")
  end

  test "a failed comparison aborts every mutation in the batch", %{path: path} do
    assert :ok = LMDB.write_batch(path, [{:put, "park", "current"}])

    assert {:error, {:compare_failed, "park"}} =
             LMDB.write_batch(path, [
               {:compare, "park", "stale"},
               {:put, "park", "overwritten"},
               {:put, "reverse", "park"}
             ])

    assert {:ok, "current"} = LMDB.get(path, "park")
    assert :not_found = LMDB.get(path, "reverse")
  end

  test "compare_missing commits only while the key remains absent", %{path: path} do
    assert :ok =
             LMDB.write_batch(path, [
               {:compare_missing, "reverse"},
               {:put, "reverse", "created"}
             ])

    assert {:ok, "created"} = LMDB.get(path, "reverse")

    assert {:error, {:compare_failed, "reverse"}} =
             LMDB.write_batch(path, [
               {:compare_missing, "reverse"},
               {:put, "reverse", "overwritten"},
               {:put, "other", "must-not-commit"}
             ])

    assert {:ok, "created"} = LMDB.get(path, "reverse")
    assert :not_found = LMDB.get(path, "other")
  end
end
