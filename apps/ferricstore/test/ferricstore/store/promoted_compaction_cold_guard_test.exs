defmodule Ferricstore.Store.PromotedCompactionColdGuardTest do
  use ExUnit.Case, async: true

  @promoted_path Path.expand("../../../lib/ferricstore/store/shard/compound/promoted.ex", __DIR__)

  test "promoted compaction batches cold dedicated reads" do
    source = File.read!(@promoted_path)
    [_before, section] = String.split(source, "def compact_dedicated", parts: 2)
    [compaction_section | _after] = String.split(section, "  @spec maybe_promote", parts: 2)

    assert compaction_section =~ "ColdRead.pread_batch",
           "promoted compaction should batch cold dedicated reads"

    assert compaction_section =~ "ColdRead.emit_pread_error",
           "promoted compaction should report corrupt/missing cold dedicated records"

    refute compaction_section =~ "read_cold_async(",
           "promoted compaction should not spawn one async waiter per cold entry"
  end
end
