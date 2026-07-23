defmodule Ferricstore.Flow.LMDBBoundedRangeTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_range_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, "idx:a:1", "one"},
               {:put, "idx:a:2", "two"},
               {:put, "idx:a:3", "three"},
               {:put, "idx:b:1", "other"}
             ])

    %{path: path}
  end

  test "scans an exclusive cursor to an exclusive upper bound", %{path: path} do
    assert {:ok, [{"idx:a:2", "two"}], true, 10} =
             LMDB.range_entries_bounded(
               path,
               "idx:a:",
               "idx:a:1",
               "idx:a:3",
               10,
               1_024
             )
  end

  test "reports a resumable item boundary exactly", %{path: path} do
    assert {:ok, [{"idx:a:1", "one"}], false, 10} =
             LMDB.range_entries_bounded(path, "idx:a:", "", "", 1, 1_024)

    assert {:ok, [{"idx:a:2", "two"}, {"idx:a:3", "three"}], true, 22} =
             LMDB.range_entries_bounded(path, "idx:a:", "idx:a:1", "", 10, 1_024)
  end

  test "stops before crossing the byte budget and never returns one oversized row", %{path: path} do
    assert {:ok, [{"idx:a:1", "one"}], false, 10} =
             LMDB.range_entries_bounded(path, "idx:a:", "", "", 10, 10)

    assert {:error, :range_entry_too_large} =
             LMDB.range_entries_bounded(path, "idx:a:", "", "", 10, 9)
  end

  test "rejects cursors and bounds outside the requested prefix", %{path: path} do
    assert {:error, :invalid_lmdb_range} =
             LMDB.range_entries_bounded(path, "idx:a:", "idx:b:1", "", 10, 100)

    assert {:error, :invalid_lmdb_range} =
             LMDB.range_entries_bounded(path, "idx:a:", "", "idx:b:1", 10, 100)
  end

  test "bounds aggregate values before materializing a point-read batch", %{path: path} do
    assert :ok =
             LMDB.write_batch(path, [
               {:put, "record:1", String.duplicate("a", 600)},
               {:put, "record:2", String.duplicate("b", 600)}
             ])

    assert {:error, :batch_value_budget_exceeded} =
             LMDB.get_many_bounded(path, ["record:1", "record:2"], 1_000)

    assert {:ok, [{:ok, first}, {:ok, second}], 1_200} =
             LMDB.get_many_bounded(path, ["record:1", "record:2"], 1_200)

    assert byte_size(first) == 600
    assert byte_size(second) == 600
  end

  test "returns a resumable bounded point-read prefix without copying the rejected suffix", %{
    path: path
  } do
    assert :ok =
             LMDB.write_batch(path, [
               {:put, "record:1", String.duplicate("a", 600)},
               {:put, "record:2", String.duplicate("b", 600)},
               {:put, "record:3", String.duplicate("c", 200)}
             ])

    assert {:ok, [{:ok, first}], 600, false} =
             LMDB.get_many_prefix_bounded(
               path,
               ["record:1", "record:2", "record:3"],
               1_000
             )

    assert byte_size(first) == 600

    assert {:ok, [{:ok, second}, {:ok, third}], 800, true} =
             LMDB.get_many_prefix_bounded(path, ["record:2", "record:3"], 1_000)

    assert byte_size(second) == 600
    assert byte_size(third) == 200

    assert {:error, :batch_value_budget_exceeded} =
             LMDB.get_many_prefix_bounded(path, ["record:1"], 599)
  end

  test "rejects an oversized bounded point-read key list", %{path: path} do
    assert {:error, :batch_key_budget_exceeded} =
             LMDB.get_many_bounded(path, List.duplicate("missing", 4_097), 1_024)
  end
end
