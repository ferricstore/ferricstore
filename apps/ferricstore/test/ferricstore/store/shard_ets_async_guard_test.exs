defmodule Ferricstore.Store.ShardETSAsyncGuardTest do
  use ExUnit.Case, async: true

  test "Shard ETS warm helpers use async cold reads" do
    source = Ferricstore.Test.SourceFiles.shard_ets_source()

    # RMW and compound read helpers use these warm paths. Keep disk reads on the
    # async NIF path so a cold value does not block a Normal scheduler.
    assert source =~ "ColdRead.pread_batch_keyed",
           "expected Shard.ETS prefix warm helpers to use keyed batched async cold reads"

    assert source =~ "ColdRead.pread_keyed",
           "expected Shard.ETS point warm helper to use keyed ColdRead async wrapper"

    refute Regex.match?(~r/NIF\.v2_pread_at\(/, source),
           "expected Shard.ETS warm helpers to avoid blocking v2_pread_at/2"
  end
end
