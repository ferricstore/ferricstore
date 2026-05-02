defmodule Ferricstore.Store.PromotionAsyncGuardTest do
  use ExUnit.Case, async: true

  @promotion_path Path.expand("../../../lib/ferricstore/store/promotion.ex", __DIR__)

  test "promotion recovery and compaction avoid blocking pread" do
    source = File.read!(@promotion_path)

    # Promotion recovery/compaction can scan many large cold entries. Keep those
    # reads off Normal schedulers even though the maintenance flow waits for the
    # value before mutating ETS.
    assert source =~ "NIF.v2_pread_at_async",
           "expected promotion cold reads to use v2_pread_at_async/4"

    refute Regex.match?(~r/NIF\.v2_pread_at\(/, source),
           "expected promotion cold reads to avoid blocking v2_pread_at/2"
  end
end
