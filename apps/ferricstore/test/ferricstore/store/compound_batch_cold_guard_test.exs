defmodule Ferricstore.Store.CompoundBatchColdGuardTest do
  use ExUnit.Case, async: true

  @compound_path Path.expand(
                   "../../../lib/ferricstore/store/shard/compound.ex",
                   __DIR__
                 )

  test "compound batch cold reads use the async batch pread NIF" do
    source = File.read!(@compound_path)

    # HMGET/ZMSCORE/SMISMEMBER-style commands can read many cold large fields.
    # The shared compound batch path must submit those cold reads together
    # instead of serializing one blocking pread per field on the shard process.
    assert source =~ "v2_pread_batch_async",
           "expected Shard.Compound shared batch get path to use v2_pread_batch_async/3"
  end
end
