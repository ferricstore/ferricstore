defmodule Ferricstore.Store.ShardReadsAsyncGuardTest do
  use ExUnit.Case, async: true

  @reads_path Path.expand("../../../lib/ferricstore/store/shard/reads.ex", __DIR__)

  test "Shard cold get fallbacks submit async pread work" do
    source = File.read!(@reads_path)

    # Shard GET/GET_META are GenServer calls; cold disk I/O must be submitted to
    # the async NIF and completed by message so the server process is not stuck in
    # a blocking pread.
    assert source =~ "NIF.v2_pread_at_key_async",
           "expected Shard.Reads cold GET/GET_META fallback to use keyed async pread"

    refute Regex.match?(~r/(?<!_)v2_pread_at\(/, source),
           "expected Shard.Reads cold paths to avoid blocking v2_pread_at/2"
  end
end
