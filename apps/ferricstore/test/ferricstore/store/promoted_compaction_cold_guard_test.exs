defmodule Ferricstore.Store.PromotedCompactionColdGuardTest do
  use ExUnit.Case, async: true

  @compound_path Path.expand(
                   "../../../lib/ferricstore/store/shard/compound.ex",
                   __DIR__
                 )

  test "promoted compaction batches cold dedicated reads" do
    source = File.read!(@compound_path)
    [_before, section] = String.split(source, "def compact_dedicated", parts: 2)
    [compaction_section | _after] = String.split(section, "  @spec maybe_promote", parts: 2)

    assert compaction_section =~ "ColdRead.pread_batch",
           "promoted compaction should batch cold dedicated reads"

    refute compaction_section =~ "read_cold_async(",
           "promoted compaction should not spawn one async waiter per cold entry"
  end
end
