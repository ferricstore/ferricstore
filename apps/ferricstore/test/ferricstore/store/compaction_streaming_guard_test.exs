defmodule Ferricstore.Store.CompactionStreamingGuardTest do
  use ExUnit.Case, async: true

  test "shared-log compaction does not materialize a segment-sized relocation catalog" do
    compaction = File.read!("lib/ferricstore/store/shard/compaction.ex")
    admin = File.read!("lib/ferricstore/store/shard/calls/admin.ex")

    assert compaction =~ "NIF.v2_scan_file_page"
    assert compaction =~ "LMDB.get_many"
    assert compaction =~ "CompactionPlan.append"
    assert compaction =~ "CompactionPlan.reduce_pages"

    refute compaction =~ ":ets.foldl("
    refute compaction =~ "collect_compaction_cold_pages"
    refute compaction =~ "compaction_hot_key_set"
    refute admin =~ "live_entries_by_fid"
  end
end
