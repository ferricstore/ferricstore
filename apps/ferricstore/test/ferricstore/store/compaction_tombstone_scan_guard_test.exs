defmodule Ferricstore.Store.CompactionTombstoneScanGuardTest do
  use ExUnit.Case, async: true

  test "tombstone dependency planning is disk-backed, paged, and newest-first" do
    compaction = File.read!("lib/ferricstore/store/shard/compaction.ex")
    catalog = File.read!("lib/ferricstore/store/compaction_tombstone_catalog.ex")

    assert compaction =~ "CompactionTombstoneCatalog.record_source_page_count"
    assert compaction =~ "CompactionTombstoneCatalog.observe_lower_page_count"
    assert compaction =~ "NIF.v2_scan_file_page"
    assert compaction =~ "Enum.sort_by(lower_files, fn {lower_fid, _path} -> -lower_fid end)"
    assert compaction =~ "when resolved >= candidate_count"

    assert catalog =~ ":crypto.hash(:sha256, key)"
    assert catalog =~ "LMDB.get_many"
    assert catalog =~ "LMDB.write_batch"

    refute compaction =~ "NIF.v2_scan_tombstones"
    refute compaction =~ "MapSet.new(tombstones"
  end
end
