defmodule Ferricstore.Test.ShardHelpersTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Flow.Governance.LimitCache
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.ServerCatalog
  alias Ferricstore.Store.{CompoundKey, DiskPressure, Promotion, Router}
  alias Ferricstore.Test.ShardHelpers

  test "flush_all_keys preserves the durable server catalog" do
    ctx = FerricStore.Instance.get(:default)
    namespace = "cleanup-probe-#{System.unique_integer([:positive, :monotonic])}"

    keys = [
      ServerCatalog.entry_key(namespace, "subject"),
      ServerCatalog.revision_key(namespace),
      ServerCatalog.live_count_key(namespace)
    ]

    values = [
      ServerCatalog.encode_entry(11, "value"),
      ServerCatalog.encode_revision(11),
      ServerCatalog.encode_live_count(1)
    ]

    Enum.zip(keys, values)
    |> Enum.each(fn {key, value} -> assert :ok = Router.put(ctx, key, value, 0) end)

    on_exit(fn -> Enum.each(keys, &Router.delete(ctx, &1)) end)

    before = Map.new(keys, &{&1, Router.get(ctx, &1)})
    assert Enum.all?(before, fn {_key, value} -> is_binary(value) end)

    assert :ok = ShardHelpers.flush_all_keys()
    assert before == Map.new(keys, &{&1, Router.get(ctx, &1)})
  end

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

  test "flush_all_keys durably removes promoted storage" do
    assert :ok = ShardHelpers.flush_all_keys()

    apply_context_snapshot =
      ShardHelpers.replace_default_apply_context(promotion_threshold: 1)

    on_exit(fn ->
      ShardHelpers.restore_default_apply_context(apply_context_snapshot)
    end)

    ctx = FerricStore.Instance.get(:default)
    redis_key = "shard-helper-promoted-#{System.unique_integer([:positive])}"
    shard_index = Router.shard_for(ctx, redis_key)
    shard = Router.shard_name(ctx, shard_index)
    type_key = CompoundKey.type_key(redis_key)
    first = CompoundKey.hash_field(redis_key, "first")
    second = CompoundKey.hash_field(redis_key, "second")

    assert :ok = Router.compound_put(ctx, redis_key, type_key, "hash", 0)
    assert :ok = Router.compound_put(ctx, redis_key, first, "one", 0)
    assert :ok = Router.compound_put(ctx, redis_key, second, "two", 0)

    ShardHelpers.eventually(
      fn ->
        shard
        |> :sys.get_state()
        |> Map.fetch!(:promoted_instances)
        |> Map.has_key?(redis_key)
      end,
      "expected cleanup fixture to be promoted"
    )

    dedicated_path = Promotion.dedicated_path(ctx.data_dir, shard_index, :hash, redis_key)
    assert File.dir?(dedicated_path)

    assert :ok = ShardHelpers.flush_all_keys()
    refute File.exists?(dedicated_path)
    assert [] = :ets.lookup(elem(ctx.keydir_refs, shard_index), Promotion.marker_key(redis_key))
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

  test "flush_all_keys does not restart a healthy WARaft backend" do
    server_name = :wa_raft_server.registered_name(:ferricstore_waraft_backend, 1)
    server_pid = Process.whereis(server_name)

    assert is_pid(server_pid)
    assert :ok = ShardHelpers.flush_all_keys()
    assert Process.whereis(server_name) == server_pid
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

  test "replace_default_apply_context restarts a stopped WARaft backend" do
    ctx = FerricStore.Instance.get(:default)

    on_exit(fn ->
      _ = WARaftBackend.start(ctx)
      ShardHelpers.wait_default_pipeline_ready(30_000)
    end)

    assert :ok = WARaftBackend.stop()

    snapshot =
      ShardHelpers.replace_default_apply_context(promotion_threshold: 1)

    on_exit(fn -> ShardHelpers.restore_default_apply_context(snapshot) end)

    assert WARaftBackend.context!(:ferricstore_waraft_backend).apply_context.promotion_threshold ==
             1
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

  test "failed distribution startup restores the default application runtime" do
    original_ctx = FerricStore.Instance.get(:default)

    assert_raise RuntimeError, "forced distribution startup failure", fn ->
      ShardHelpers.ensure_distribution_started!(:forced_distribution_failure,
        force?: true,
        start_fun: fn _node_name ->
          raise "forced distribution startup failure"
        end
      )
    end

    restored_ctx = FerricStore.Instance.get(:default)

    assert restored_ctx.data_dir == original_ctx.data_dir
    assert :ets.whereis(:ferricstore_waiters) != :undefined
    assert :ets.whereis(:ferricstore_flow_claim_waiters) != :undefined
    assert :ets.whereis(:ferricstore_waraft_apply_projection_cache) != :undefined
    assert is_pid(Process.whereis(Ferricstore.Waiters.Monitor))
    assert is_pid(Process.whereis(Ferricstore.Raft.WARaftSegmentReader.TableOwner))
    assert :ok = ShardHelpers.wait_default_pipeline_ready(30_000)
  end
end
