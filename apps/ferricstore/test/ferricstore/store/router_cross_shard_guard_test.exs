defmodule Ferricstore.Store.RouterCrossShardGuardTest do
  @moduledoc false

  use ExUnit.Case, async: true

  test "router multi-source write commands use CrossShardOp locks" do
    source =
      File.read!(
        Path.join([__DIR__, "..", "..", "..", "lib", "ferricstore", "store", "router.ex"])
      )

    # These commands read source keys that can live on shards other than the
    # destination's shard. The destination-shard Raft entry gives ordering only
    # for the destination, so the router must lock source keys with CrossShardOp
    # before dispatching the destination write.
    assert source =~ ~r/def hll_op\(ctx, "PFMERGE".*CrossShardOp\.execute/s
    assert source =~ ~r/def geo_op\(ctx, "GEOSEARCHSTORE".*CrossShardOp\.execute/s
    assert source =~ ~r/def tdigest_op\(ctx, "TDIGEST\.MERGE".*CrossShardOp\.execute/s
  end

  test "router CrossShardOp calls keep the caller instance context" do
    source =
      File.read!(
        Path.join([__DIR__, "..", "..", "..", "lib", "ferricstore", "store", "router.ex"])
      )

    # Router accepts an explicit instance context. CrossShardOp must receive
    # that context too; otherwise same-shard fast paths and routing stores can
    # silently execute against the default instance.
    assert source =~ ~r/def hll_op\(ctx, "PFMERGE".*CrossShardOp\.execute.*instance: ctx/s
    assert source =~ ~r/def bitmap_op\(ctx, "BITOP".*CrossShardOp\.execute.*instance: ctx/s
    assert source =~ ~r/def geo_op\(ctx, "GEOSEARCHSTORE".*CrossShardOp\.execute.*instance: ctx/s

    assert source =~
             ~r/def tdigest_op\(ctx, "TDIGEST\.MERGE".*CrossShardOp\.execute.*instance: ctx/s

    assert source =~ ~r/def list_op\(ctx, key, \{:lmove.*CrossShardOp\.execute.*instance: ctx/s
  end
end
