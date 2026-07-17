defmodule Ferricstore.Store.CompactionTombstoneCatalogTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.CompactionTombstoneCatalog
  alias Ferricstore.Flow.LMDB

  setup do
    shard_path =
      Path.join(
        System.tmp_dir!(),
        "compaction_tombstone_catalog_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(shard_path)
    on_exit(fn -> File.rm_rf(shard_path) end)
    %{shard_path: shard_path}
  end

  test "catalog keeps only the newest source tombstone and newest lower-file state", %{
    shard_path: shard_path
  } do
    assert {:ok, catalog} = CompactionTombstoneCatalog.open(shard_path, 7)

    source_page = [
      record("a", 10, true),
      record("b", 20, true),
      record("a", 30, true)
    ]

    assert :ok = CompactionTombstoneCatalog.record_source_page(catalog, source_page)

    assert :ok =
             CompactionTombstoneCatalog.observe_lower_page(catalog, [
               record("a", 1, false),
               record("b", 2, false)
             ])

    assert :ok =
             CompactionTombstoneCatalog.observe_lower_page(catalog, [
               record("b", 3, true)
             ])

    assert {:ok, [30]} = CompactionTombstoneCatalog.needed_offsets(catalog, source_page)

    expired_at = Ferricstore.HLC.now_ms() - 1

    assert :ok =
             CompactionTombstoneCatalog.observe_lower_page(catalog, [
               record("a", 4, false, expired_at)
             ])

    assert {:ok, []} = CompactionTombstoneCatalog.needed_offsets(catalog, source_page)
    assert :ok = CompactionTombstoneCatalog.close(catalog)
    refute File.exists?(catalog.path)
  end

  @tag :hlc_drift_guard
  test "catalog preserves tombstones masking wall-live records during unsafe HLC drift", %{
    shard_path: shard_path
  } do
    hlc_ref = :persistent_term.get(:ferricstore_hlc_ref)
    previous_hlc = :atomics.get(hlc_ref, 1)
    source_page = [record("masked", 30, true)]

    assert {:ok, catalog} = CompactionTombstoneCatalog.open(shard_path, 7)

    try do
      wall_ms = System.os_time(:millisecond)
      expire_at_ms = wall_ms + 30_000

      assert :ok = CompactionTombstoneCatalog.record_source_page(catalog, source_page)
      :atomics.put(hlc_ref, 1, Bitwise.bsl(wall_ms + 60_000, 16))

      assert :ok =
               CompactionTombstoneCatalog.observe_lower_page(catalog, [
                 record("masked", 1, false, expire_at_ms)
               ])

      assert {:ok, [30]} = CompactionTombstoneCatalog.needed_offsets(catalog, source_page)
    after
      :atomics.put(hlc_ref, 1, previous_hlc)
      CompactionTombstoneCatalog.close(catalog)
    end
  end

  test "catalog rejects compressed or trailing candidate terms", %{shard_path: shard_path} do
    key = String.duplicate("key", 2_048)
    source = [record(key, 10, true)]
    catalog_key = <<1, :crypto.hash(:sha256, key)::binary>>
    term = {:compaction_tombstone, 1, key, 10, :missing, nil}

    compressed = :erlang.term_to_binary(term, compressed: 9)
    assert <<131, 80, _::binary>> = compressed

    for payload <- [compressed, :erlang.term_to_binary(term) <> <<0>>] do
      assert {:ok, catalog} = CompactionTombstoneCatalog.open(shard_path, 7)
      assert :ok = CompactionTombstoneCatalog.record_source_page(catalog, source)
      assert :ok = LMDB.write_batch(catalog.path, [{:put, catalog_key, payload}])

      assert {:error, :invalid_catalog_value} =
               CompactionTombstoneCatalog.needed_offsets(catalog, source)

      assert :ok = CompactionTombstoneCatalog.close(catalog)
    end
  end

  test "reopening a catalog releases the cached LMDB environment before removing files", %{
    shard_path: shard_path
  } do
    source = [record("first", 10, true)]
    replacement = [record("replacement", 20, true)]

    assert {:ok, first_catalog} = CompactionTombstoneCatalog.open(shard_path, 7)
    assert :ok = CompactionTombstoneCatalog.record_source_page(first_catalog, source)

    assert {:ok, reopened_catalog} = CompactionTombstoneCatalog.open(shard_path, 7)
    assert :ok = CompactionTombstoneCatalog.record_source_page(reopened_catalog, replacement)

    assert :ok =
             CompactionTombstoneCatalog.observe_lower_page(reopened_catalog, [
               record("replacement", 1, false)
             ])

    assert :ok = LMDB.release(reopened_catalog.path, 1_000)
    assert {:ok, [20]} = CompactionTombstoneCatalog.needed_offsets(reopened_catalog, replacement)
    assert :ok = CompactionTombstoneCatalog.close(reopened_catalog)
  end

  defp record(key, offset, tombstone?, expire_at_ms \\ 0) do
    {key, offset, 0, expire_at_ms, tombstone?}
  end
end
