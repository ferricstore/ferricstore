defmodule Ferricstore.BitcaskNifSpecGuardTest do
  use ExUnit.Case, async: true

  @nif_path Path.expand("../../lib/ferricstore/bitcask/nif.ex", __DIR__)

  test "v2_pread_batch spec matches Rust offset-only API" do
    source = File.read!(@nif_path)

    assert source =~ "@spec v2_pread_batch(binary(), [non_neg_integer()])",
           "v2_pread_batch/2 takes offset integers; stale tuple specs mislead callers"

    assert source =~
             "@spec v2_pread_batch(binary(), [non_neg_integer()]) ::\n          {:ok, [binary() | nil]} | {:error, term()}",
           "v2_pread_batch/2 can return nil entries for tombstones/missing offsets"
  end

  test "async batch pread specs document per-index error entries" do
    source = File.read!(@nif_path)

    assert source =~ "@type pread_batch_value :: binary() | nil | {:error, binary()}",
           "async batch pread NIFs can return per-index {:error, reason}; specs must expose that"

    assert source =~ "@type pread_batch_result :: [pread_batch_value()]",
           "async batch pread result list must use the per-index value type"
  end
end
