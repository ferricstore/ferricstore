defmodule Ferricstore.Store.ShardETSAsyncGuardTest do
  use ExUnit.Case, async: true

  @ets_path Path.expand("../../../lib/ferricstore/store/shard/ets.ex", __DIR__)

  test "Shard ETS warm helpers use async cold reads" do
    source = File.read!(@ets_path)

    # RMW and compound read helpers use these warm paths. Keep disk reads on the
    # async NIF path so a cold value does not block a Normal scheduler.
    assert source =~ "NIF.v2_pread_at_async",
           "expected Shard.ETS warm helpers to use v2_pread_at_async/4"

    refute Regex.match?(~r/NIF\.v2_pread_at\(/, source),
           "expected Shard.ETS warm helpers to avoid blocking v2_pread_at/2"
  end
end
