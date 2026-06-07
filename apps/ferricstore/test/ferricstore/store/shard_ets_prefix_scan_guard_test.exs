defmodule Ferricstore.Store.ShardETSPrefixScanGuardTest do
  use ExUnit.Case, async: true

  @prefix_scan_path Path.expand("../../../lib/ferricstore/store/shard/ets/prefix_scan.ex", __DIR__)

  test "prefix scans batch cold disk reads" do
    source = File.read!(@prefix_scan_path)

    # HGETALL and related compound scans can touch many cold large values.
    # Keep the scan path from regressing to one blocking pread per cold entry.
    assert source =~ "ColdRead.pread_batch_keyed",
           "expected Shard.ETS prefix scan cold path to use keyed batched cold reads"

    assert source =~ "ColdRead.emit_pread_error",
           "expected Shard.ETS prefix scan cold path to report corrupt/missing cold records"

    refute Regex.match?(~r/(?<!_)v2_pread_at\(/, source),
           "expected Shard.ETS prefix scan cold path to avoid blocking v2_pread_at/2"
  end

  test "prefix scans batch-materialize blob refs" do
    source = File.read!(@prefix_scan_path)
    [_before, section] = String.split(source, "def prefix_read_cold_batch_async", parts: 2)

    [read_body, helper_section] =
      String.split(section, "def prefix_materialize_blob_values", parts: 2)

    assert read_body =~ "prefix_materialize_blob_values",
           "expected prefix scans to materialize duplicate blob refs once per batch"

    assert helper_section =~ "BlobValue.maybe_materialize_many",
           "expected prefix scans to use the BlobValue batch materializer"

    refute read_body =~ "materialize_blob_value(state, value)",
           "prefix scans should not materialize blob refs one entry at a time"
  end
end
