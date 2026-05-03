defmodule Ferricstore.Store.PromotionAsyncGuardTest do
  use ExUnit.Case, async: true

  @promotion_path Path.expand("../../../lib/ferricstore/store/promotion.ex", __DIR__)

  test "promotion recovery and compaction avoid blocking pread" do
    source = File.read!(@promotion_path)

    # Promotion recovery/compaction can scan many large cold entries. Keep those
    # reads async and keyed, so stale ETS offsets cannot promote another key's
    # value under the promoted collection.
    assert source =~ "ColdRead.pread_at(path, offset, key,",
           "expected promotion cold reads to use keyed ColdRead.pread_at/4"

    refute Regex.match?(~r/NIF\.v2_pread_at\(/, source),
           "expected promotion cold reads to avoid blocking v2_pread_at/2"
  end
end
