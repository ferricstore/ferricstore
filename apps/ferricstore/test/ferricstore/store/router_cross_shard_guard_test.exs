defmodule Ferricstore.Store.RouterCrossShardGuardTest do
  @moduledoc false

  use ExUnit.Case, async: true

  test "router multi-source write commands use CrossShardOp locks" do
    source = File.read!(Path.join([__DIR__, "..", "..", "..", "lib", "ferricstore", "store", "router.ex"]))

    # These commands read source keys that can live on shards other than the
    # destination's shard. The destination-shard Raft entry gives ordering only
    # for the destination, so the router must lock source keys with CrossShardOp
    # before dispatching the destination write.
    assert source =~ ~r/def hll_op\(ctx, "PFMERGE".*CrossShardOp\.execute/s
    assert source =~ ~r/def geo_op\(ctx, "GEOSEARCHSTORE".*CrossShardOp\.execute/s
    assert source =~ ~r/def tdigest_op\(ctx, "TDIGEST\.MERGE".*CrossShardOp\.execute/s
  end
end
