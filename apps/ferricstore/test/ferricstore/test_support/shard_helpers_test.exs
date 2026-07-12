defmodule Ferricstore.Test.ShardHelpersTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Flow.Governance.LimitCache
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.{DiskPressure, Router}
  alias Ferricstore.Test.ShardHelpers

  test "flush_all_keys drains cached governance reservations before deleting owners" do
    ctx = FerricStore.Instance.get(:default)
    scope = "shard-helper-cache-flush-#{System.unique_integer([:positive])}"
    old_enabled = Application.get_env(:ferricstore, :flow_governance_limit_cache_enabled)

    on_exit(fn ->
      if is_nil(old_enabled) do
        Application.delete_env(:ferricstore, :flow_governance_limit_cache_enabled)
      else
        Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, old_enabled)
      end
    end)

    assert {:ok, %{errors: 0}} = LimitCache.clear(ctx)
    assert :ok = ShardHelpers.flush_all_keys()
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)

    assert {:ok, _lease} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 0,
               amount: 4,
               limit: 4,
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    assert {:ok, %{reservation_ids: [_active_id]}} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 1_000,
               now_ms: 1_001
             )

    assert [_entry] =
             :ets.lookup(:ferricstore_flow_governance_limit_cache, {ctx.name, scope, 0})

    assert :ok = ShardHelpers.flush_all_keys()
    assert :ok = LimitCache.clear()
  end

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

  test "restore_default_waraft! replaces a stopped foreign backend" do
    default_ctx = FerricStore.Instance.get(:default)
    name = :"shard_helpers_foreign_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), Atom.to_string(name))
    File.rm_rf!(root)
    Ferricstore.DataDir.ensure_layout!(root, 1)
    foreign_ctx = FerricStore.Instance.build(name, data_dir: root, shard_count: 1)

    on_exit(fn ->
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(name)
      File.rm_rf!(root)
      :ok = WARaftBackend.start(default_ctx)
    end)

    assert :ok =
             WARaftBackend.start(foreign_ctx,
               log_module: :ferricstore_waraft_spike_segment_log
             )

    assert :ok = WARaftBackend.stop()
    assert :ok = ShardHelpers.restore_default_waraft!()

    active_ctx = WARaftBackend.context!(:ferricstore_waraft_backend)
    identity_fields = [:name, :data_dir_expanded, :shard_count, :keydir_refs]

    assert Map.take(active_ctx, identity_fields) == Map.take(default_ctx, identity_fields)
    assert :ok = ShardHelpers.wait_default_pipeline_ready(5_000)
  end
end
