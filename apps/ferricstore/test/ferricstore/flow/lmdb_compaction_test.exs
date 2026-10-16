defmodule Ferricstore.Flow.LMDBCompactionTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{LMDB, LMDBCompaction}

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_compaction_#{System.unique_integer([:positive])}"
      )

    on_exit(fn ->
      _ = LMDB.release(path)
      File.rm_rf!(path)
    end)

    %{path: path}
  end

  test "compaction returns the space freed by deletes and keeps every remaining row", %{
    path: path
  } do
    rows =
      for i <- 1..20_000,
          do: {"row:" <> String.pad_leading("#{i}", 6, "0"), :crypto.strong_rand_bytes(180)}

    rows
    |> Enum.chunk_every(1_000)
    |> Enum.each(fn chunk ->
      :ok = LMDB.write_batch(path, Enum.map(chunk, fn {k, v} -> {:put, k, v} end))
    end)

    {kept, deleted} = Enum.split_with(rows, fn {key, _value} -> String.ends_with?(key, "0") end)

    deleted
    |> Enum.chunk_every(1_000)
    |> Enum.each(fn chunk ->
      :ok = LMDB.write_batch(path, Enum.map(chunk, &{:delete, elem(&1, 0)}))
    end)

    before_bytes = File.stat!(Path.join(path, "data.mdb")).size

    assert {:ok, %{before_bytes: ^before_bytes, after_bytes: after_bytes}} =
             LMDBCompaction.maybe_compact(path, min_free_ratio: 0.3, min_file_bytes: 0)

    assert after_bytes * 3 < before_bytes
    assert File.stat!(Path.join(path, "data.mdb")).size == after_bytes
    refute File.exists?(Path.join(path, "data.mdb.compact"))

    for {key, value} <- kept, do: assert({:ok, ^value} = LMDB.get(path, key))
    for {key, _value} <- Enum.take(deleted, 200), do: assert(:not_found = LMDB.get(path, key))

    assert :ok = LMDB.write_batch(path, [{:put, "row:new", "after-compaction"}])
    assert {:ok, "after-compaction"} = LMDB.get(path, "row:new")
  end

  test "compaction is skipped below the thresholds and for a missing environment", %{path: path} do
    assert {:ok, :skipped} =
             LMDBCompaction.maybe_compact(path, min_free_ratio: 0.0, min_file_bytes: 0)

    refute File.exists?(path)

    :ok = LMDB.write_batch(path, [{:put, "a", "1"}])
    before = File.stat!(Path.join(path, "data.mdb"))

    assert {:ok, :skipped} =
             LMDBCompaction.maybe_compact(path, min_free_ratio: 0.99, min_file_bytes: 0)

    assert {:ok, :skipped} =
             LMDBCompaction.maybe_compact(path,
               min_free_ratio: 0.0,
               min_file_bytes: before.size + 1
             )

    assert File.stat!(Path.join(path, "data.mdb")) == before
    assert {:ok, "1"} = LMDB.get(path, "a")
  end

  test "a copy left by an interrupted compaction is discarded", %{path: path} do
    :ok = LMDB.write_batch(path, [{:put, "a", "1"}])
    File.write!(Path.join(path, "data.mdb.compact"), "partial copy")

    assert {:ok, :skipped} =
             LMDBCompaction.maybe_compact(path, min_free_ratio: 0.99, min_file_bytes: 0)

    refute File.exists?(Path.join(path, "data.mdb.compact"))
    assert {:ok, "1"} = LMDB.get(path, "a")
  end
end
