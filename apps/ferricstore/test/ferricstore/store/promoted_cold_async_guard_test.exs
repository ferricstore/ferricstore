defmodule Ferricstore.Store.PromotedColdAsyncGuardTest do
  use ExUnit.Case, async: true

  @compound_path Path.expand("../../../lib/ferricstore/store/shard/compound.ex", __DIR__)
  @ops_path Path.expand("../../../lib/ferricstore/store/ops.ex", __DIR__)

  test "promoted compound cold reads use async pread" do
    source = File.read!(@compound_path)

    # Promoted hashes/sets/zsets are the large-collection path. Their cold read
    # fallbacks must not run blocking pread on a Normal scheduler.
    assert source =~ "NIF.v2_pread_at_async",
           "expected promoted compound cold reads to use v2_pread_at_async/4"

    refute Regex.match?(~r/NIF\.v2_pread_at\(/, source),
           "expected promoted compound cold reads to avoid blocking v2_pread_at/2"
  end

  test "transaction promoted cold reads use async pread" do
    source = File.read!(@ops_path)

    assert source =~ "NIF.v2_pread_at_async",
           "expected transaction promoted cold reads to use v2_pread_at_async/4"

    refute Regex.match?(~r/NIF\.v2_pread_at\(/, source),
           "expected transaction promoted cold reads to avoid blocking v2_pread_at/2"
  end
end
