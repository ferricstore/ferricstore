defmodule Ferricstore.Store.ShardETSPrefixScanGuardTest do
  use ExUnit.Case, async: true

  @ets_path Path.expand("../../../lib/ferricstore/store/shard/ets.ex", __DIR__)

  test "prefix scans batch cold disk reads" do
    source = File.read!(@ets_path)

    # HGETALL and related compound scans can touch many cold large values.
    # Keep the scan path from regressing to one blocking pread per cold entry.
    assert source =~ "v2_pread_batch_async",
           "expected Shard.ETS prefix scan cold path to use v2_pread_batch_async/3"
  end
end
