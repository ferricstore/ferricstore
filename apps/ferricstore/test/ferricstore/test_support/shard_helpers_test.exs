defmodule Ferricstore.Test.ShardHelpersTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Store.{DiskPressure, Router}
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

  test "wait_default_quorum_writable clears leaked write pressure before probing" do
    ctx = FerricStore.Instance.get(:default)
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    on_exit(fn ->
      ShardHelpers.reset_memory_guard_pressure()

      for shard_index <- 0..(shard_count - 1) do
        DiskPressure.clear(ctx, shard_index)
        DiskPressure.clear(shard_index)
        DiskPressure.clear_operational(shard_index)
      end
    end)

    for shard_index <- 0..(shard_count - 1) do
      DiskPressure.set(ctx, shard_index)
      DiskPressure.set(shard_index)
    end

    MemoryGuard.set_keydir_full(true)
    MemoryGuard.set_reject_writes(true)

    assert {:error, _reason} =
             Router.put(ctx, "__blocked_ready_probe_#{System.unique_integer([:positive])}", "1")

    assert :ok = ShardHelpers.wait_default_quorum_writable(5_000)

    assert :ok =
             Router.put(ctx, "__after_ready_probe_#{System.unique_integer([:positive])}", "1")
  end

  test "wait_default_quorum_writable restarts WARaft before public probes" do
    ctx = FerricStore.Instance.get(:default)

    on_exit(fn ->
      _ = Ferricstore.Raft.WARaftBackend.start(ctx)
      ShardHelpers.wait_default_pipeline_ready(30_000)
    end)

    assert :ok = Ferricstore.Raft.WARaftBackend.stop()
    assert :ok = ShardHelpers.wait_default_quorum_writable(5_000)
  end
end
