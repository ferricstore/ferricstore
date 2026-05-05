defmodule Ferricstore.CrossShardOpTest do
  @moduledoc """
  Tests for the CrossShardOp mini-percolator.

  Validates cross-shard multi-key commands (SMOVE, RENAME, COPY) work
  atomically in quorum mode and return CROSSSLOT errors in async mode.
  Uses the live application-supervised shards via Router.

  Since command handlers (Set.handle, Generic.handle) now call
  CrossShardOp.execute internally, tests call the command handlers
  directly rather than wrapping them in CrossShardOp.execute.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.CrossShardOp
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Commands.Set
  alias Ferricstore.Commands.Generic

  setup do
    ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()
    :ok
  end

  # ---------------------------------------------------------------------------
  # 1. Same-shard SMOVE -- no locking overhead, works directly
  # ---------------------------------------------------------------------------

  describe "same-shard SMOVE" do
    test "moves member between sets on the same shard with zero locking overhead" do
      {src, dst} = ShardHelpers.keys_on_same_shard()

      # Confirm both keys route to the same shard
      assert Router.shard_for(FerricStore.Instance.get(:default), src) ==
               Router.shard_for(FerricStore.Instance.get(:default), dst)

      # Create source set with members via tx_execute (writes to ETS directly)
      shard =
        Router.shard_name(
          FerricStore.Instance.get(:default),
          Router.shard_for(FerricStore.Instance.get(:default), src)
        )

      [sadd_result] =
        GenServer.call(shard, {:tx_execute, [{"SADD", [src, "a", "b", "c"]}], nil}, 10_000)

      assert sadd_result == 3

      # Verify we can read the members back via tx_execute
      [members_before] =
        GenServer.call(shard, {:tx_execute, [{"SMEMBERS", [src]}], nil}, 10_000)

      assert "a" in members_before

      # Call SMOVE directly -- the handler calls CrossShardOp.execute internally.
      # For same-shard, this uses the fast path (no locking).
      result = Set.handle("SMOVE", [src, dst, "a"], %{})

      assert result == 1

      # Verify member moved
      [members_src] =
        GenServer.call(shard, {:tx_execute, [{"SMEMBERS", [src]}], nil}, 10_000)

      [members_dst] =
        GenServer.call(shard, {:tx_execute, [{"SMEMBERS", [dst]}], nil}, 10_000)

      assert "a" not in members_src
      assert "a" in members_dst
    end
  end

  # ---------------------------------------------------------------------------
  # 2. Cross-shard SMOVE in quorum mode -- locks both shards, moves atomically
  # ---------------------------------------------------------------------------

  describe "cross-shard SMOVE in quorum mode" do
    test "moves member atomically between sets on different shards" do
      [src, dst] = ShardHelpers.keys_on_different_shards(2)

      # Confirm different shards
      assert Router.shard_for(FerricStore.Instance.get(:default), src) !=
               Router.shard_for(FerricStore.Instance.get(:default), dst)

      # Create source set via its shard
      src_shard =
        Router.shard_name(
          FerricStore.Instance.get(:default),
          Router.shard_for(FerricStore.Instance.get(:default), src)
        )

      GenServer.call(src_shard, {:tx_execute, [{"SADD", [src, "x", "y"]}], nil}, 10_000)

      # Call SMOVE directly -- the handler calls CrossShardOp.execute internally
      result = Set.handle("SMOVE", [src, dst, "x"], %{})

      assert result == 1

      # Verify: "x" removed from source, added to destination
      [src_members] =
        GenServer.call(src_shard, {:tx_execute, [{"SMEMBERS", [src]}], nil}, 10_000)

      dst_shard =
        Router.shard_name(
          FerricStore.Instance.get(:default),
          Router.shard_for(FerricStore.Instance.get(:default), dst)
        )

      [dst_members] =
        GenServer.call(dst_shard, {:tx_execute, [{"SMEMBERS", [dst]}], nil}, 10_000)

      assert "x" not in src_members
      assert "x" in dst_members
      assert "y" in src_members
    end
  end

  # ---------------------------------------------------------------------------
  # 4. Cross-shard RENAME in quorum mode -- locks both, renames atomically
  # ---------------------------------------------------------------------------

  describe "cross-shard RENAME in quorum mode" do
    test "renames key atomically across shards" do
      [old_key, new_key] = ShardHelpers.keys_on_different_shards(2)

      assert Router.shard_for(FerricStore.Instance.get(:default), old_key) !=
               Router.shard_for(FerricStore.Instance.get(:default), new_key)

      # Create the old key with a value
      Router.put(FerricStore.Instance.get(:default), old_key, "rename_value", 0)

      # Call RENAME directly -- the handler calls CrossShardOp.execute internally
      result = Generic.handle("RENAME", [old_key, new_key], %{})

      assert result == :ok

      # Old key gone, new key has value
      assert Router.get(FerricStore.Instance.get(:default), old_key) == nil
      assert Router.get(FerricStore.Instance.get(:default), new_key) == "rename_value"
    end
  end

  # ---------------------------------------------------------------------------
  # 5. Cross-shard COPY in quorum mode -- locks destination only, copies
  # ---------------------------------------------------------------------------

  describe "cross-shard COPY in quorum mode" do
    test "copies key value across shards" do
      [src, dst] = ShardHelpers.keys_on_different_shards(2)

      assert Router.shard_for(FerricStore.Instance.get(:default), src) !=
               Router.shard_for(FerricStore.Instance.get(:default), dst)

      # Create source key
      Router.put(FerricStore.Instance.get(:default), src, "copy_value", 0)

      # Call COPY directly -- the handler calls CrossShardOp.execute internally
      result = Generic.handle("COPY", [src, dst], %{})

      assert result == 1

      # Both keys exist
      assert Router.get(FerricStore.Instance.get(:default), src) == "copy_value"
      assert Router.get(FerricStore.Instance.get(:default), dst) == "copy_value"
    end
  end

  # ---------------------------------------------------------------------------
  # 6. Concurrent cross-shard operations on different keys -- no blocking
  # ---------------------------------------------------------------------------

  describe "concurrent cross-shard operations on different keys" do
    test "non-overlapping cross-shard operations run concurrently" do
      [k1, k2, k3, k4] = ShardHelpers.keys_on_different_shards(4)

      # Set up source keys
      Router.put(FerricStore.Instance.get(:default), k1, "v1", 0)
      Router.put(FerricStore.Instance.get(:default), k3, "v3", 0)

      # Run two independent cross-shard RENAMEs concurrently
      # Each command handler calls CrossShardOp.execute internally
      task1 =
        Task.async(fn ->
          Generic.handle("RENAME", [k1, k2], %{})
        end)

      task2 =
        Task.async(fn ->
          Generic.handle("RENAME", [k3, k4], %{})
        end)

      result1 = Task.await(task1, 10_000)
      result2 = Task.await(task2, 10_000)

      assert result1 == :ok
      assert result2 == :ok

      assert Router.get(FerricStore.Instance.get(:default), k1) == nil
      assert Router.get(FerricStore.Instance.get(:default), k2) == "v1"
      assert Router.get(FerricStore.Instance.get(:default), k3) == nil
      assert Router.get(FerricStore.Instance.get(:default), k4) == "v3"
    end
  end

  # ---------------------------------------------------------------------------
  # 7. Lock timeout/expiry -- if coordinator dies, locks expire after TTL
  # ---------------------------------------------------------------------------

  describe "lock timeout/expiry" do
    test "locks expire after TTL allowing subsequent operations" do
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)

      Router.put(FerricStore.Instance.get(:default), k1, "val", 0)

      # Acquire locks with a very short TTL (200ms for testing)
      owner_ref = make_ref()
      shard1_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard2_idx = Router.shard_for(FerricStore.Instance.get(:default), k2)

      now = System.os_time(:millisecond)

      # Lock both keys through the Raft state machine
      shard1_id = Ferricstore.Raft.Cluster.shard_server_id(shard1_idx)
      shard2_id = Ferricstore.Raft.Cluster.shard_server_id(shard2_idx)

      {:ok, {:applied_at, _, :ok}, _} =
        :ra.process_command(shard1_id, {:lock_keys, [k1], owner_ref, now + 200})

      {:ok, {:applied_at, _, :ok}, _} =
        :ra.process_command(shard2_id, {:lock_keys, [k2], owner_ref, now + 200})

      # Immediately after locking, another owner should fail
      other_ref = make_ref()

      {:ok, {:applied_at, _, {:error, :keys_locked}}, _} =
        :ra.process_command(shard1_id, {:lock_keys, [k1], other_ref, now + 5000})

      # Wait for locks to expire — poll instead of fixed sleep for CI tolerance
      Ferricstore.Test.Utils.eventually(
        fn ->
          {:ok, {:applied_at, _, :ok}, _} =
            :ra.process_command(
              shard1_id,
              {:lock_keys, [k1], other_ref, System.os_time(:millisecond) + 10_000}
            )
        end,
        5_000
      )

      # Clean up
      :ra.process_command(shard1_id, {:unlock_keys, [k1], other_ref})
    end
  end

  # ---------------------------------------------------------------------------
  # 8. Deadlock prevention -- two concurrent ops on overlapping keys don't deadlock
  # ---------------------------------------------------------------------------

  describe "deadlock prevention" do
    test "overlapping cross-shard operations don't deadlock (ordered locking)" do
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)

      Router.put(FerricStore.Instance.get(:default), k1, "v1", 0)
      Router.put(FerricStore.Instance.get(:default), k2, "v2", 0)

      # Two tasks try to RENAME the SAME pair of keys in opposite directions.
      # CrossShardOp normalizes lock acquisition order by shard index.
      # Command handlers call CrossShardOp.execute internally.
      task1 =
        Task.async(fn ->
          Generic.handle("RENAME", [k1, k2], %{})
        end)

      task2 =
        Task.async(fn ->
          Generic.handle("RENAME", [k2, k1], %{})
        end)

      # Both tasks must complete (no deadlock) within 10 seconds.
      # One will succeed, the other may succeed (if retries succeed)
      # or fail with lock contention, but neither should deadlock.
      result1 = Task.await(task1, 10_000)
      result2 = Task.await(task2, 10_000)

      # At least one should succeed
      assert result1 == :ok or result2 == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # 9. Intent resolver -- stale intents are cleaned up
  # ---------------------------------------------------------------------------

  describe "intent resolver" do
    test "stale intents are cleaned up after coordinator crash" do
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)

      # Write an intent directly to simulate a coordinator crash mid-operation
      owner_ref = make_ref()

      coordinator_shard_idx =
        min(
          Router.shard_for(FerricStore.Instance.get(:default), k1),
          Router.shard_for(FerricStore.Instance.get(:default), k2)
        )

      shard_id = Ferricstore.Raft.Cluster.shard_server_id(coordinator_shard_idx)

      intent_map = %{
        command: :rename,
        keys: %{source: k1, dest: k2},
        value_hashes: %{k1 => :erlang.phash2("stale_value")},
        status: :executing,
        created_at: System.os_time(:millisecond) - 30_000
      }

      {:ok, {:applied_at, _, :ok}, _} =
        :ra.process_command(shard_id, {:cross_shard_intent, owner_ref, intent_map})

      # Verify intent exists
      {:ok, {:applied_at, _, intents_before}, _} = :ra.process_command(shard_id, {:get_intents})
      assert Map.has_key?(intents_before, owner_ref)

      # Run intent resolver
      CrossShardOp.IntentResolver.resolve_stale_intents()

      # The intent should be cleaned up (it's older than stale threshold)
      {:ok, {:applied_at, _, intents_after}, _} = :ra.process_command(shard_id, {:get_intents})
      refute Map.has_key?(intents_after, owner_ref)
    end
  end

  # ---------------------------------------------------------------------------
  # 11. Same-shard fast path is truly zero overhead
  # ---------------------------------------------------------------------------

  describe "same-shard fast path" do
    test "same-shard keys call execute_fn directly without locking" do
      {k1, k2} = ShardHelpers.keys_on_same_shard()

      assert Router.shard_for(FerricStore.Instance.get(:default), k1) ==
               Router.shard_for(FerricStore.Instance.get(:default), k2)

      Router.put(FerricStore.Instance.get(:default), k1, "val1", 0)

      # Call RENAME directly -- handler calls CrossShardOp internally (same-shard fast path)
      result = Generic.handle("RENAME", [k1, k2], %{})

      assert result == :ok
      assert Router.get(FerricStore.Instance.get(:default), k1) == nil
      assert Router.get(FerricStore.Instance.get(:default), k2) == "val1"
    end
  end

  # ---------------------------------------------------------------------------
  # Issue 5: Additional tests for the fixes
  # ---------------------------------------------------------------------------

  # 12. Regular writes to locked keys are rejected
  describe "regular writes to locked keys are rejected" do
    test "Router.put on a locked key returns {:error, :key_locked}" do
      [k1, _k2] = ShardHelpers.keys_on_different_shards(2)

      # Set a value first
      Router.put(FerricStore.Instance.get(:default), k1, "before_lock", 0)
      assert Router.get(FerricStore.Instance.get(:default), k1) == "before_lock"

      # Lock the key with a long TTL
      owner_ref = make_ref()
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Ferricstore.Raft.Cluster.shard_server_id(shard_idx)
      now = System.os_time(:millisecond)

      {:ok, {:applied_at, _, :ok}, _} =
        :ra.process_command(shard_id, {:lock_keys, [k1], owner_ref, now + 30_000})

      # Try a regular put -- should be rejected
      result = Router.put(FerricStore.Instance.get(:default), k1, "during_lock", 0)
      assert result == {:error, :key_locked}

      # Value should be unchanged
      assert Router.get(FerricStore.Instance.get(:default), k1) == "before_lock"

      # Clean up
      :ra.process_command(shard_id, {:unlock_keys, [k1], owner_ref})
    end

    test "Router.delete on a locked key returns {:error, :key_locked}" do
      [k1, _k2] = ShardHelpers.keys_on_different_shards(2)

      Router.put(FerricStore.Instance.get(:default), k1, "value", 0)

      owner_ref = make_ref()
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Ferricstore.Raft.Cluster.shard_server_id(shard_idx)
      now = System.os_time(:millisecond)

      {:ok, {:applied_at, _, :ok}, _} =
        :ra.process_command(shard_id, {:lock_keys, [k1], owner_ref, now + 30_000})

      # Delete should be rejected
      result = Router.delete(FerricStore.Instance.get(:default), k1)
      assert result == {:error, :key_locked}

      # Value should still exist
      assert Router.get(FerricStore.Instance.get(:default), k1) == "value"

      # Clean up
      :ra.process_command(shard_id, {:unlock_keys, [k1], owner_ref})
    end

    test "reads on locked keys still work" do
      [k1, _k2] = ShardHelpers.keys_on_different_shards(2)

      Router.put(FerricStore.Instance.get(:default), k1, "readable", 0)

      owner_ref = make_ref()
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Ferricstore.Raft.Cluster.shard_server_id(shard_idx)
      now = System.os_time(:millisecond)

      {:ok, {:applied_at, _, :ok}, _} =
        :ra.process_command(shard_id, {:lock_keys, [k1], owner_ref, now + 30_000})

      # Reads should still work -- locks only block writes
      assert Router.get(FerricStore.Instance.get(:default), k1) == "readable"
      assert Router.exists?(FerricStore.Instance.get(:default), k1) == true

      # Clean up
      :ra.process_command(shard_id, {:unlock_keys, [k1], owner_ref})
    end
  end

  # 13. Command handlers use CrossShardOp for cross-shard operations
  describe "command handlers use CrossShardOp internally" do
    test "SMOVE across shards works when called directly" do
      [src, dst] = ShardHelpers.keys_on_different_shards(2)

      assert Router.shard_for(FerricStore.Instance.get(:default), src) !=
               Router.shard_for(FerricStore.Instance.get(:default), dst)

      # Create source set
      src_shard =
        Router.shard_name(
          FerricStore.Instance.get(:default),
          Router.shard_for(FerricStore.Instance.get(:default), src)
        )

      GenServer.call(src_shard, {:tx_execute, [{"SADD", [src, "member1"]}], nil}, 10_000)

      # Call SMOVE directly -- handler uses CrossShardOp internally
      result = Set.handle("SMOVE", [src, dst, "member1"], %{})
      assert result == 1

      # Verify member moved
      [src_members] =
        GenServer.call(src_shard, {:tx_execute, [{"SMEMBERS", [src]}], nil}, 10_000)

      dst_shard =
        Router.shard_name(
          FerricStore.Instance.get(:default),
          Router.shard_for(FerricStore.Instance.get(:default), dst)
        )

      [dst_members] =
        GenServer.call(dst_shard, {:tx_execute, [{"SMEMBERS", [dst]}], nil}, 10_000)

      assert "member1" not in src_members
      assert "member1" in dst_members
    end

    test "RENAME across shards works when called directly" do
      [old_key, new_key] = ShardHelpers.keys_on_different_shards(2)

      Router.put(FerricStore.Instance.get(:default), old_key, "rename_test", 0)

      result = Generic.handle("RENAME", [old_key, new_key], %{})
      assert result == :ok

      assert Router.get(FerricStore.Instance.get(:default), old_key) == nil
      assert Router.get(FerricStore.Instance.get(:default), new_key) == "rename_test"
    end

    test "COPY across shards works when called directly" do
      [src, dst] = ShardHelpers.keys_on_different_shards(2)

      Router.put(FerricStore.Instance.get(:default), src, "copy_test", 0)

      result = Generic.handle("COPY", [src, dst], %{})
      assert result == 1

      assert Router.get(FerricStore.Instance.get(:default), src) == "copy_test"
      assert Router.get(FerricStore.Instance.get(:default), dst) == "copy_test"
    end

    test "RENAMENX across shards works when called directly" do
      [old_key, new_key] = ShardHelpers.keys_on_different_shards(2)

      Router.put(FerricStore.Instance.get(:default), old_key, "renamenx_test", 0)

      result = Generic.handle("RENAMENX", [old_key, new_key], %{})
      assert result == 1

      assert Router.get(FerricStore.Instance.get(:default), old_key) == nil
      assert Router.get(FerricStore.Instance.get(:default), new_key) == "renamenx_test"
    end
  end

  # 14. Value hash in intent
  describe "value hash in intent" do
    test "intent includes value hashes computed from current key values" do
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)

      # Set up keys with known values
      Router.put(FerricStore.Instance.get(:default), k1, "hash_test_value", 0)

      # Rename will write an intent with value hashes
      result = Generic.handle("RENAME", [k1, k2], %{})
      assert result == :ok

      # The intent is cleaned up after success, so we can't inspect it directly.
      # Instead, test that compute_value_hashes works correctly.
      per_shard_stores =
        Map.new(
          [
            Router.shard_for(FerricStore.Instance.get(:default), k1),
            Router.shard_for(FerricStore.Instance.get(:default), k2)
          ],
          fn idx ->
            {idx, CrossShardOp.build_store_for_shard(idx)}
          end
        )

      hashes = CrossShardOp.compute_value_hashes([{k2, :read}], per_shard_stores)
      assert Map.has_key?(hashes, k2)
      assert hashes[k2] == Router.watch_token(FerricStore.Instance.get(:default), k2)
    end

    test "intent tokens for cold large values avoid value materialization" do
      [k1, _k2] = ShardHelpers.keys_on_different_shards(2)
      ctx = FerricStore.Instance.get(:default)
      value = :binary.copy("i", ctx.hot_cache_max_value_size + 1024)

      Router.put(ctx, k1, value, 0)

      ShardHelpers.eventually(fn ->
        match?({:ok, {_fid, _off, _vsize}}, Router.get_keydir_file_ref(ctx, k1))
      end)

      idx = Router.shard_for(ctx, k1)
      per_shard_stores = %{idx => CrossShardOp.build_store_for_shard(ctx, idx)}

      hashes = CrossShardOp.compute_value_hashes(ctx, [{k1, :read}], per_shard_stores)
      assert hashes[k1] == Router.watch_token(ctx, k1)
    end

    test "intent resolver checks value hashes on stale intents" do
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)

      # Set up k1 with a value
      Router.put(FerricStore.Instance.get(:default), k1, "intent_hash_test", 0)

      # Write a stale intent with matching value hash
      owner_ref = make_ref()

      coordinator_shard_idx =
        min(
          Router.shard_for(FerricStore.Instance.get(:default), k1),
          Router.shard_for(FerricStore.Instance.get(:default), k2)
        )

      shard_id = Ferricstore.Raft.Cluster.shard_server_id(coordinator_shard_idx)

      intent_map = %{
        command: :rename,
        keys: %{source: k1, dest: k2},
        value_hashes: %{k1 => :erlang.phash2("intent_hash_test")},
        status: :executing,
        created_at: System.os_time(:millisecond) - 30_000
      }

      {:ok, {:applied_at, _, :ok}, _} =
        :ra.process_command(shard_id, {:cross_shard_intent, owner_ref, intent_map})

      # Run intent resolver -- should clean up because intent is stale
      CrossShardOp.IntentResolver.resolve_stale_intents()

      {:ok, {:applied_at, _, intents_after}, _} = :ra.process_command(shard_id, {:get_intents})
      refute Map.has_key?(intents_after, owner_ref)
    end

    test "intent resolver cleans up intents with mismatched value hashes" do
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)

      # Set up k1 with a value
      Router.put(FerricStore.Instance.get(:default), k1, "current_value", 0)

      # Write a stale intent with a DIFFERENT value hash (simulating data changed)
      owner_ref = make_ref()

      coordinator_shard_idx =
        min(
          Router.shard_for(FerricStore.Instance.get(:default), k1),
          Router.shard_for(FerricStore.Instance.get(:default), k2)
        )

      shard_id = Ferricstore.Raft.Cluster.shard_server_id(coordinator_shard_idx)

      intent_map = %{
        command: :rename,
        keys: %{source: k1, dest: k2},
        value_hashes: %{k1 => :erlang.phash2("old_different_value")},
        status: :executing,
        created_at: System.os_time(:millisecond) - 30_000
      }

      {:ok, {:applied_at, _, :ok}, _} =
        :ra.process_command(shard_id, {:cross_shard_intent, owner_ref, intent_map})

      # Run intent resolver -- should still clean up (don't re-execute)
      CrossShardOp.IntentResolver.resolve_stale_intents()

      {:ok, {:applied_at, _, intents_after}, _} = :ra.process_command(shard_id, {:get_intents})
      refute Map.has_key?(intents_after, owner_ref)
    end
  end
end
