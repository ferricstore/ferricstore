defmodule Ferricstore.Store.ShardETSPrefixScanGuardTest do
  use ExUnit.Case, async: true

  @ets_path Path.expand("../../../lib/ferricstore/store/shard/ets.ex", __DIR__)

  test "prefix scans batch cold disk reads" do
    source = File.read!(@ets_path)

    # HGETALL and related compound scans can touch many cold large values.
    # Keep the scan path from regressing to one blocking pread per cold entry.
    assert source =~ "ColdRead.pread_batch_keyed",
           "expected Shard.ETS prefix scan cold path to use keyed batched cold reads"

    refute Regex.match?(~r/(?<!_)v2_pread_at\(/, source),
           "expected Shard.ETS prefix scan cold path to avoid blocking v2_pread_at/2"
  end
end
