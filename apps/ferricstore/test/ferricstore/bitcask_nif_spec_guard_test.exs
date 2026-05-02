defmodule Ferricstore.BitcaskNifSpecGuardTest do
  use ExUnit.Case, async: true

  @nif_path Path.expand("../../lib/ferricstore/bitcask/nif.ex", __DIR__)

  test "v2_pread_batch spec matches Rust offset-only API" do
    source = File.read!(@nif_path)

    assert source =~ "@spec v2_pread_batch(binary(), [non_neg_integer()])",
           "v2_pread_batch/2 takes offset integers; stale tuple specs mislead callers"
  end
end
