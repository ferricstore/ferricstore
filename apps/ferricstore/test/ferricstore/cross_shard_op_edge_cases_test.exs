defmodule Ferricstore.CrossShardOpEdgeCasesTest do
  @moduledoc """
  Edge case tests for CrossShardOp Mini-Percolator.
  """

  use ExUnit.Case, async: false

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
      {:ok, {:applied_at, _, :ok}, _} = :ra.process_command(shard_id, {:lock_keys, [k1], owner_ref, expire_at})

      # Wait for expiry
      Process.sleep(300)

      # Another lock should succeed (old one expired)
      other_ref = make_ref()
      new_expire = System.os_time(:millisecond) + 5000
      {:ok, {:applied_at, _, result}, _} = :ra.process_command(shard_id, {:lock_keys, [k1], other_ref, new_expire})
      assert result == :ok

      # Cleanup
      :ra.process_command(shard_id, {:unlock_keys, [k1], other_ref})
    end

    test "expired lock allows regular writes" do
      {k1, _k2} = cross_shard_keys()
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Cluster.shard_server_id(shard_idx)

      # Lock with 200ms TTL
      owner_ref = make_ref()
      expire_at = System.os_time(:millisecond) + 200
      {:ok, {:applied_at, _, :ok}, _} = :ra.process_command(shard_id, {:lock_keys, [k1], owner_ref, expire_at})

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

      # Create source set
      shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), src))
      GenServer.call(shard, {:tx_execute, [{"SADD", [src, "a", "b"]}], nil}, 10_000)

      # Two sequential SMOVE (concurrent is hard to guarantee without races)
      result1 = Set.handle("SMOVE", [src, dst, "a"], %{})
      result2 = Set.handle("SMOVE", [src, dst, "b"], %{})

      # Both should succeed
      assert result1 == 1
      assert result2 == 1

      # Source should be empty, dest should have both
      [src_members] = GenServer.call(shard, {:tx_execute, [{"SMEMBERS", [src]}], nil}, 10_000)
      assert src_members == [] or src_members == nil

      dst_shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), dst))
      [dst_members] = GenServer.call(dst_shard, {:tx_execute, [{"SMEMBERS", [dst]}], nil}, 10_000)
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

      :ra.process_command(shard_id, {:cross_shard_intent, owner_ref, %{
        command: :smove,
        keys: %{source: "a", dest: "b"},
        status: :executing,
        created_at: System.os_time(:millisecond)
      }})

      Ferricstore.CrossShardOp.IntentResolver.resolve_shard_intents(0)

      {:ok, {:applied_at, _, intents}, _} = :ra.process_command(shard_id, {:get_intents})
      assert Map.has_key?(intents, owner_ref)

      # Cleanup
      :ra.process_command(shard_id, {:delete_intent, owner_ref})
    end

    test "stale intent IS cleaned up" do
      shard_id = Cluster.shard_server_id(0)
      owner_ref = make_ref()

      :ra.process_command(shard_id, {:cross_shard_intent, owner_ref, %{
        command: :smove,
        keys: %{source: "a", dest: "b"},
        status: :executing,
        created_at: System.os_time(:millisecond) - 20_000
      }})

      Ferricstore.CrossShardOp.IntentResolver.resolve_shard_intents(0)

      {:ok, {:applied_at, _, intents}, _} = :ra.process_command(shard_id, {:get_intents})
      refute Map.has_key?(intents, owner_ref)
    end
  end

  # ---------------------------------------------------------------------------
  # 4. Mixed namespace — any async key returns CROSSSLOT
  # ---------------------------------------------------------------------------

  describe "mixed namespace durability" do
    @tag skip: "async durability feature removed; mixed async/quorum routing no longer exists"
    test "CROSSSLOT when source is async, dest is quorum" do
      src = "asyncns:src_#{:rand.uniform(9_999_999)}"
      dst = "quorumns:dst_#{:rand.uniform(9_999_999)}"

      # Ensure cross-shard
      if Router.shard_for(FerricStore.Instance.get(:default), src) == Router.shard_for(FerricStore.Instance.get(:default), dst) do
        # Skip if they happen to route to same shard
        :ok
      else
        NamespaceConfig.set("asyncns", "durability", "async")
        NamespaceConfig.set("quorumns", "durability", "quorum")

        # Set up source
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), src))
        GenServer.call(shard, {:tx_execute, [{"SADD", [src, "member"]}], nil}, 10_000)

        result = Set.handle("SMOVE", [src, dst, "member"], %{})
        assert {:error, msg} = result
        assert String.contains?(msg, "CROSSSLOT")
      end
    end

    @tag skip: "async durability feature removed; mixed async/quorum routing no longer exists"
    test "CROSSSLOT when dest is async, source is quorum" do
      src = "quorumns:src2_#{:rand.uniform(9_999_999)}"
      dst = "asyncns:dst2_#{:rand.uniform(9_999_999)}"

      if Router.shard_for(FerricStore.Instance.get(:default), src) == Router.shard_for(FerricStore.Instance.get(:default), dst) do
        :ok
      else
        NamespaceConfig.set("asyncns", "durability", "async")
        NamespaceConfig.set("quorumns", "durability", "quorum")

        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), src))
        GenServer.call(shard, {:tx_execute, [{"SADD", [src, "member"]}], nil}, 10_000)

        result = Set.handle("SMOVE", [src, dst, "member"], %{})
        assert {:error, msg} = result
        assert String.contains?(msg, "CROSSSLOT")
      end
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
      {:ok, {:applied_at, _, :ok}, _} = :ra.process_command(shard_id, {:lock_keys, [k1], owner_ref, expire_at})

      # Read should still work
      assert "readable" == Router.get(FerricStore.Instance.get(:default), k1)
      assert Router.exists?(FerricStore.Instance.get(:default), k1)

      # Cleanup
      :ra.process_command(shard_id, {:unlock_keys, [k1], owner_ref})
    end

    test "writes on locked keys are rejected" do
      {k1, _k2} = cross_shard_keys()
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Cluster.shard_server_id(shard_idx)

      # Lock the key
      owner_ref = make_ref()
      expire_at = System.os_time(:millisecond) + 5000
      {:ok, {:applied_at, _, :ok}, _} = :ra.process_command(shard_id, {:lock_keys, [k1], owner_ref, expire_at})

      # Write should be rejected
      result = Router.put(FerricStore.Instance.get(:default), k1, "blocked", 0)
      assert result == {:error, :key_locked},
             "Expected write to locked key to be rejected, got: #{inspect(result)}"

      # Cleanup
      :ra.process_command(shard_id, {:unlock_keys, [k1], owner_ref})
    end
  end
end
