defmodule Ferricstore.CrossShardOpEdgeCasesTest do
  @moduledoc """
  Edge case tests for CrossShardOp Mini-Percolator.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Commands.Set
  alias Ferricstore.Raft.Cluster

  setup do
    ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()

    on_exit(fn ->
      NamespaceConfig.reset_all()
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  # Unique keys that route to specific shards
  defp cross_shard_keys do
    suffix = :rand.uniform(9_999_999)
    k1 = ShardHelpers.key_for_shard(0) <> "_#{suffix}"
    k2 = ShardHelpers.key_for_shard(1) <> "_#{suffix}"
    {k1, k2}
  end

  # ---------------------------------------------------------------------------
  # 1. Lock expiry — locks expire after TTL and keys become writable
  # ---------------------------------------------------------------------------

  describe "lock expiry" do
    test "locks expire after TTL and keys become writable" do
      {k1, _k2} = cross_shard_keys()
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Cluster.shard_server_id(shard_idx)

      # Lock with 200ms TTL
      owner_ref = make_ref()
      expire_at = System.os_time(:millisecond) + 200

      {:ok, {:applied_at, _, :ok}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [k1], owner_ref, expire_at})

      # Wait for expiry
      Process.sleep(300)

      # Another lock should succeed (old one expired)
      other_ref = make_ref()
      new_expire = System.os_time(:millisecond) + 5000

      {:ok, {:applied_at, _, result}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [k1], other_ref, new_expire})

      assert result == :ok

      # Cleanup
      Ferricstore.Raft.CommandClock.process_command(shard_id, {:unlock_keys, [k1], other_ref})
    end

    test "expired lock allows regular writes" do
      {k1, _k2} = cross_shard_keys()
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Cluster.shard_server_id(shard_idx)

      # Lock with 200ms TTL
      owner_ref = make_ref()
      expire_at = System.os_time(:millisecond) + 200

      {:ok, {:applied_at, _, :ok}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [k1], owner_ref, expire_at})

      # Wait for expiry
      Process.sleep(300)

      # Regular write should succeed
      assert :ok = Router.put(FerricStore.Instance.get(:default), k1, "after_expiry", 0)
      assert "after_expiry" == Router.get(FerricStore.Instance.get(:default), k1)
    end
  end

  # ---------------------------------------------------------------------------
  # 2. Concurrent operations on same source — different members
  # ---------------------------------------------------------------------------

  describe "concurrent SMOVE on same source" do
    test "two SMOVEs on different members both succeed" do
      {src, dst} = cross_shard_keys()

      # Create source set through the public command path.
      ctx = FerricStore.Instance.get(:default)
      assert 2 = Set.handle("SADD", [src, "a", "b"], ctx)

      # Two sequential SMOVE (concurrent is hard to guarantee without races)
      result1 = Set.handle("SMOVE", [src, dst, "a"], ctx)
      result2 = Set.handle("SMOVE", [src, dst, "b"], ctx)

      # Both should succeed
      assert result1 == 1
      assert result2 == 1

      # Source should be empty, dest should have both
      src_members = Set.handle("SMEMBERS", [src], ctx)
      assert src_members == [] or src_members == nil

      dst_members = Set.handle("SMEMBERS", [dst], ctx)
      assert "a" in dst_members
      assert "b" in dst_members
    end
  end

  # ---------------------------------------------------------------------------
  # 3. Intent resolver timing
  # ---------------------------------------------------------------------------

  describe "intent resolver timing" do
    test "fresh intent is NOT cleaned up" do
      shard_id = Cluster.shard_server_id(0)
      owner_ref = make_ref()

      Ferricstore.Raft.CommandClock.process_command(
        shard_id,
        {:cross_shard_intent, owner_ref,
         %{
           command: :smove,
           keys: %{source: "a", dest: "b"},
           status: :executing,
           created_at: System.os_time(:millisecond)
         }}
      )

      Ferricstore.CrossShardOp.IntentResolver.resolve_shard_intents(0)

      {:ok, {:applied_at, _, intents}, _} = Ferricstore.Raft.CommandClock.process_command(shard_id, {:get_intents})
      assert Map.has_key?(intents, owner_ref)

      # Cleanup
      Ferricstore.Raft.CommandClock.process_command(shard_id, {:delete_intent, owner_ref})
    end

    test "stale intent IS cleaned up" do
      shard_id = Cluster.shard_server_id(0)
      owner_ref = make_ref()

      Ferricstore.Raft.CommandClock.process_command(
        shard_id,
        {:cross_shard_intent, owner_ref,
         %{
           command: :smove,
           keys: %{source: "a", dest: "b"},
           status: :executing,
           created_at: System.os_time(:millisecond) - 20_000
         }}
      )

      Ferricstore.CrossShardOp.IntentResolver.resolve_shard_intents(0)

      {:ok, {:applied_at, _, intents}, _} = Ferricstore.Raft.CommandClock.process_command(shard_id, {:get_intents})
      refute Map.has_key?(intents, owner_ref)
    end
  end

  # ---------------------------------------------------------------------------
  # 5. Compound key lock mapping
  # ---------------------------------------------------------------------------

  describe "compound key lock mapping" do
    test "reads on locked keys still work" do
      {k1, _k2} = cross_shard_keys()
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Cluster.shard_server_id(shard_idx)

      # Write data first
      Router.put(FerricStore.Instance.get(:default), k1, "readable", 0)

      # Lock the key
      owner_ref = make_ref()
      expire_at = System.os_time(:millisecond) + 5000

      {:ok, {:applied_at, _, :ok}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [k1], owner_ref, expire_at})

      # Read should still work
      assert "readable" == Router.get(FerricStore.Instance.get(:default), k1)
      assert Router.exists?(FerricStore.Instance.get(:default), k1)

      # Cleanup
      Ferricstore.Raft.CommandClock.process_command(shard_id, {:unlock_keys, [k1], owner_ref})
    end

    test "writes on locked keys are rejected" do
      {k1, _k2} = cross_shard_keys()
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Cluster.shard_server_id(shard_idx)

      # Lock the key
      owner_ref = make_ref()
      expire_at = System.os_time(:millisecond) + 5000

      {:ok, {:applied_at, _, :ok}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [k1], owner_ref, expire_at})

      # Write should be rejected
      result = Router.put(FerricStore.Instance.get(:default), k1, "blocked", 0)

      assert result == {:error, :key_locked},
             "Expected write to locked key to be rejected, got: #{inspect(result)}"

      # Cleanup
      Ferricstore.Raft.CommandClock.process_command(shard_id, {:unlock_keys, [k1], owner_ref})
    end
  end
end
