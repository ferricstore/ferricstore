defmodule Ferricstore.Store.CompactionTombstoneScanGuardTest do
  use ExUnit.Case, async: true

  @shard_path "lib/ferricstore/store/shard.ex"

  test "lower tombstone dependency scan is newest-first and bounded by unresolved keys" do
    source = File.read!(@shard_path)
    [_before, section] = String.split(source, "defp scan_lower_tombstone_key_states", parts: 2)
    [function_source | _after] = String.split(section, "\n  defp tombstone_offsets", parts: 2)

    assert function_source =~ "Enum.sort_by(fn {other_fid, _path} -> -other_fid end)",
           "scan lower files newest-first so the first seen state for a masked key is final"

    assert function_source =~ "unresolved_keys",
           "track unresolved tombstone keys so scans can stop before older irrelevant files"

    assert function_source =~ "MapSet.size(next_unresolved_keys) == 0",
           "stop scanning lower files once every tombstone key has a newest lower state"
  end

  test "lower tombstone dependency scan uses key-state metadata scan" do
    source = File.read!(@shard_path)
    [_before, section] = String.split(source, "defp scan_lower_tombstone_key_states", parts: 2)
    [function_source | _after] = String.split(section, "\n  defp tombstone_offsets", parts: 2)

    assert function_source =~ "v2_scan_key_states",
           "compaction tombstone dependency checks only need key/tombstone/expiry state, not live value payload hashing"

    refute function_source =~ "v2_scan_file(path)",
           "using v2_scan_file here hashes every live payload in lower files during compaction"
  end
end
