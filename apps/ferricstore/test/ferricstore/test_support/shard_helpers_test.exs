defmodule Ferricstore.Test.ShardHelpersTest do
  use ExUnit.Case, async: false

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Test.ShardHelpers

  test "flush_all_keys clears stale keydir memory pressure accounting" do
    ctx = FerricStore.Instance.get(:default)
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    for idx <- 1..shard_count do
      :atomics.put(ctx.keydir_binary_bytes, idx, 1_000_000_000)
    end

    MemoryGuard.set_keydir_full(true)
    MemoryGuard.set_reject_writes(true)
    MemoryGuard.set_skip_promotion(true)

    ShardHelpers.flush_all_keys()

    for idx <- 1..shard_count do
      assert :atomics.get(ctx.keydir_binary_bytes, idx) == 0
    end

    refute MemoryGuard.keydir_full?()
    refute MemoryGuard.reject_writes?()
    refute MemoryGuard.skip_promotion?()
  end

  test "flush_all_keys returns only after the default Ra pipeline is apply-ready" do
    ShardHelpers.flush_all_keys()

    assert ShardHelpers.wait_default_pipeline_ready(5_000) == :ok
  end
end
