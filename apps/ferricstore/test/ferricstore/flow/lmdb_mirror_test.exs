defmodule Ferricstore.Flow.LMDBMirrorTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.LMDBMirror

  test "nil partition targets every shard" do
    assert LMDBMirror.index_shards(%{shard_count: 3}, "index", nil) == [0, 1, 2]
    assert LMDBMirror.index_shards(%{shard_count: 0}, "index", nil) == []
  end

  test "degraded flag reads atomics by one-based shard slot" do
    flags = :atomics.new(3, [])
    ctx = %{shard_count: 3, flow_lmdb_mirror_degraded: flags}

    refute LMDBMirror.degraded_flag?(ctx, 1)

    :atomics.put(flags, 2, 1)

    assert LMDBMirror.degraded_flag?(ctx, 1)
    refute LMDBMirror.degraded_flag?(ctx, 2)
  end

  test "require healthy reports degraded mirror" do
    flags = :atomics.new(2, [])
    :atomics.put(flags, 1, 1)

    assert LMDBMirror.require_healthy(
             %{shard_count: 2, flow_lmdb_mirror_degraded: flags},
             "index",
             nil
           ) == {:error, "ERR flow LMDB mirror degraded"}
  end
end
