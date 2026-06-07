defmodule Ferricstore.Store.PromotedColdAsyncGuardTest do
  use ExUnit.Case, async: true

  test "promoted compound cold reads use async pread" do
    source = Ferricstore.Test.SourceFiles.shard_compound_source()

    # Promoted hashes/sets/zsets are the large-collection path. Their cold read
    # fallbacks must stay async and include the expected key, so stale ETS
    # offsets cannot return another promoted field's value.
    assert source =~ "ColdRead.pread_keyed(path, offset, key,",
           "expected promoted compound cold reads to use keyed ColdRead.pread_keyed/4"

    refute Regex.match?(~r/NIF\.v2_pread_at\(/, source),
           "expected promoted compound cold reads to avoid blocking v2_pread_at/2"
  end

  test "transaction promoted cold reads use async pread" do
    source = Ferricstore.Test.SourceFiles.store_ops_source()

    assert source =~ "ColdRead.pread_keyed(path, off, key,",
           "expected transaction promoted cold reads to use keyed ColdRead.pread_keyed/4"

    refute Regex.match?(~r/NIF\.v2_pread_at\(/, source),
           "expected transaction promoted cold reads to avoid blocking v2_pread_at/2"
  end
end
