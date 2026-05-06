defmodule Ferricstore.Transaction.CrossShardAtomicTest do
  @moduledoc """
  TDD tests for cross-shard atomic MULTI/EXEC transactions.

  These tests verify the NEW behavior: cross-shard transactions should succeed
  atomically (not return CROSSSLOT). The approach: submit a single Raft log
  entry to an "anchor shard" containing commands for ALL involved shards.
  One Raft entry = one fsync = atomic commit or no commit.

  All key-to-shard mappings are discovered dynamically via ShardHelpers so
  tests work with any shard count (not just 4).
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Transaction.Coordinator
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()

    [k0, k1, k2, k3] = ShardHelpers.keys_on_different_shards(4)
    {same1, same2} = ShardHelpers.keys_on_same_shard()

    %{
      cross_keys: [k0, k1, k2, k3],
      k0: k0,
      k1: k1,
      k2: k2,
      k3: k3,
      same1: same1,
      same2: same2
    }
  end

  # ---------------------------------------------------------------------------
  # Test infrastructure: verify key-to-shard assumptions
  # ---------------------------------------------------------------------------

  describe "test infrastructure" do
    test "keys route to different shards", %{cross_keys: keys} do
      shards =
        Enum.map(keys, fn k -> Router.shard_for(FerricStore.Instance.get(:default), k) end)
        |> Enum.uniq()

      assert length(shards) == length(keys)
    end

    test "cross-shard key pairs are on different shards", %{k0: k0, k1: k1, k2: k2, k3: k3} do
      assert Router.shard_for(FerricStore.Instance.get(:default), k0) !=
               Router.shard_for(FerricStore.Instance.get(:default), k1)

      assert Router.shard_for(FerricStore.Instance.get(:default), k0) !=
               Router.shard_for(FerricStore.Instance.get(:default), k2)

      assert Router.shard_for(FerricStore.Instance.get(:default), k0) !=
               Router.shard_for(FerricStore.Instance.get(:default), k3)
    end
  end

  # ---------------------------------------------------------------------------
  # Basic cross-shard atomicity
  # ---------------------------------------------------------------------------

  describe "basic cross-shard atomicity" do
    test "SET keys on 2 different shards — both written", %{k0: k0, k1: k1} do
      queue = [{"SET", [k0, "val_k0"]}, {"SET", [k1, "val_k1"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok]
      assert Router.get(FerricStore.Instance.get(:default), k0) == "val_k0"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "val_k1"
    end

    test "SET keys across all 4 shards — all written", %{k0: k0, k1: k1, k2: k2, k3: k3} do
      queue = [
        {"SET", [k0, "s0"]},
        {"SET", [k1, "s1"]},
        {"SET", [k2, "s2"]},
        {"SET", [k3, "s3"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, :ok, :ok]
      assert Router.get(FerricStore.Instance.get(:default), k0) == "s0"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "s1"
      assert Router.get(FerricStore.Instance.get(:default), k2) == "s2"
      assert Router.get(FerricStore.Instance.get(:default), k3) == "s3"
    end

    test "mixed GET and SET across shards — results in correct order", %{
      k0: k0,
      k1: k1,
      k2: k2,
      k3: k3
    } do
      Router.put(FerricStore.Instance.get(:default), k0, "existing_k0", 0)
      Router.put(FerricStore.Instance.get(:default), k2, "existing_k2", 0)

      queue = [
        {"GET", [k0]},
        {"SET", [k1, "new_k1"]},
        {"GET", [k2]},
        {"SET", [k3, "new_k3"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == ["existing_k0", :ok, "existing_k2", :ok]
      assert Router.get(FerricStore.Instance.get(:default), k1) == "new_k1"
      assert Router.get(FerricStore.Instance.get(:default), k3) == "new_k3"
    end

    test "INCR on keys across different shards — both incremented", %{k0: k0, k1: k1} do
      Router.put(FerricStore.Instance.get(:default), k0, "10", 0)
      Router.put(FerricStore.Instance.get(:default), k1, "20", 0)

      queue = [{"INCR", [k0]}, {"INCR", [k1]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [{:ok, 11}, {:ok, 21}]
      assert Router.get(FerricStore.Instance.get(:default), k0) == "11"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "21"
    end

    test "DEL across shards — all deleted", %{k0: k0, k1: k1, k2: k2} do
      Router.put(FerricStore.Instance.get(:default), k0, "v0", 0)
      Router.put(FerricStore.Instance.get(:default), k1, "v1", 0)
      Router.put(FerricStore.Instance.get(:default), k2, "v2", 0)

      queue = [
        {"DEL", [k0]},
        {"DEL", [k1]},
        {"DEL", [k2]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [1, 1, 1]
      assert Router.get(FerricStore.Instance.get(:default), k0) == nil
      assert Router.get(FerricStore.Instance.get(:default), k1) == nil
      assert Router.get(FerricStore.Instance.get(:default), k2) == nil
    end

    test "GET before SET within same transaction across shards (read-before-write ordering)", %{
      k0: k0,
      k1: k1
    } do
      queue = [
        {"GET", [k0]},
        {"SET", [k0, "created"]},
        {"GET", [k1]},
        {"SET", [k1, "also_created"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [nil, :ok, nil, :ok]
      assert Router.get(FerricStore.Instance.get(:default), k0) == "created"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "also_created"
    end
  end

  # ---------------------------------------------------------------------------
  # Atomicity verification (the key tests)
  # ---------------------------------------------------------------------------

  describe "atomicity verification" do
    test "all writes from a cross-shard tx are visible AFTER execute returns", %{
      k0: k0,
      k1: k1,
      k2: k2,
      k3: k3
    } do
      queue = [
        {"SET", [k0, "atomic_k0"]},
        {"SET", [k1, "atomic_k1"]},
        {"SET", [k2, "atomic_k2"]},
        {"SET", [k3, "atomic_k3"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      # All must succeed
      assert result == [:ok, :ok, :ok, :ok]

      # All values must be readable immediately after execute returns
      assert Router.get(FerricStore.Instance.get(:default), k0) == "atomic_k0"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "atomic_k1"
      assert Router.get(FerricStore.Instance.get(:default), k2) == "atomic_k2"
      assert Router.get(FerricStore.Instance.get(:default), k3) == "atomic_k3"
    end

    test "concurrent readers eventually see all-new values after execute returns", %{
      k0: k0,
      k1: k1
    } do
      # Pre-set values on two different shards
      Router.put(FerricStore.Instance.get(:default), k0, "old_k0", 0)
      Router.put(FerricStore.Instance.get(:default), k1, "old_k1", 0)

      # Execute cross-shard transaction
      queue = [{"SET", [k0, "new_k0"]}, {"SET", [k1, "new_k1"]}]
      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok]

      # After execute returns, ALL writes must be visible (durability guarantee)
      assert Router.get(FerricStore.Instance.get(:default), k0) == "new_k0"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "new_k1"
    end
  end

  # ---------------------------------------------------------------------------
  # WATCH integration
  # ---------------------------------------------------------------------------

  describe "WATCH integration with cross-shard tx" do
    test "WATCH key, modify from outside, cross-shard EXEC aborts — NO writes on ANY shard", %{
      k0: k0,
      k1: k1
    } do
      Router.put(FerricStore.Instance.get(:default), k0, "original_k0", 0)
      Router.put(FerricStore.Instance.get(:default), k1, "original_k1", 0)

      watched = %{k0 => Router.watch_token(FerricStore.Instance.get(:default), k0)}

      # Simulate another client modifying the watched key
      Router.put(FerricStore.Instance.get(:default), k0, "modified_by_other", 0)

      # Cross-shard transaction
      queue = [
        {"SET", [k0, "from_tx_k0"]},
        {"SET", [k1, "from_tx_k1"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      # WATCH conflict -> nil (transaction aborted)
      assert result == nil

      # Neither shard should have been written to
      assert Router.get(FerricStore.Instance.get(:default), k0) == "modified_by_other"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "original_k1"
    end

    test "WATCH key, no modification, cross-shard EXEC succeeds", %{k0: k0, k1: k1} do
      Router.put(FerricStore.Instance.get(:default), k0, "original_k0", 0)

      watched = %{k0 => Router.watch_token(FerricStore.Instance.get(:default), k0)}

      # Cross-shard transaction with unmodified watched key
      queue = [
        {"SET", [k0, "new_k0"]},
        {"SET", [k1, "new_k1"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      assert result == [:ok, :ok]
      assert Router.get(FerricStore.Instance.get(:default), k0) == "new_k0"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "new_k1"
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "empty transaction returns empty list" do
      result = Coordinator.execute([], %{}, nil)
      assert result == []
    end

    test "cross-shard tx with command error (INCR on non-integer) — error in results, other commands still execute",
         %{k0: k0, k1: k1} do
      # Set a non-integer value
      Router.put(FerricStore.Instance.get(:default), k0, "not_a_number", 0)

      # Cross-shard tx: INCR non-integer, SET on different shard
      queue = [
        {"INCR", [k0]},
        {"SET", [k1, "val_k1"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      # Redis semantics: individual command errors don't abort the tx.
      # INCR returns an error, SET succeeds.
      assert is_list(result)
      assert length(result) == 2

      # First result: INCR error
      assert {:error, _msg} = Enum.at(result, 0)

      # Second result: SET success
      assert Enum.at(result, 1) == :ok

      # SET should have been applied despite INCR error
      assert Router.get(FerricStore.Instance.get(:default), k1) == "val_k1"
    end

    test "large cross-shard transaction (many commands across shards)", %{
      k0: k0,
      k1: k1,
      k2: k2,
      k3: k3
    } do
      # Build commands using our dynamic keys with unique suffixed variants.
      # Use hash tags to keep variants on the same shard as their base key.
      base_keys = [k0, k1, k2, k3]

      queue =
        base_keys
        |> Enum.flat_map(fn key ->
          Enum.map(0..11, fn idx -> {"SET", ["{#{key}}:sub_#{idx}", "val_#{idx}"]} end)
        end)

      assert length(queue) == 48

      result = Coordinator.execute(queue, %{}, nil)

      assert is_list(result)
      assert length(result) == 48
      assert Enum.all?(result, &(&1 == :ok))

      # Verify a sample from each shard is written
      assert Router.get(FerricStore.Instance.get(:default), "{#{k0}}:sub_0") != nil
      assert Router.get(FerricStore.Instance.get(:default), "{#{k1}}:sub_0") != nil
      assert Router.get(FerricStore.Instance.get(:default), "{#{k2}}:sub_0") != nil
      assert Router.get(FerricStore.Instance.get(:default), "{#{k3}}:sub_0") != nil
    end

    test "cross-shard tx with PING (keyless command) mixed with keyed commands", %{
      k0: k0,
      k1: k1
    } do
      queue = [
        {"PING", []},
        {"SET", [k0, "val_k0"]},
        {"SET", [k1, "val_k1"]},
        {"PING", []}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert is_list(result)
      assert length(result) == 4

      # PING returns {:simple, "PONG"}, SET returns :ok
      assert Enum.at(result, 0) == {:simple, "PONG"}
      assert Enum.at(result, 1) == :ok
      assert Enum.at(result, 2) == :ok
      assert Enum.at(result, 3) == {:simple, "PONG"}

      assert Router.get(FerricStore.Instance.get(:default), k0) == "val_k0"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "val_k1"
    end

    test "sandbox namespace with cross-shard keys" do
      ns = "test_ns:"

      # Find two keys that route to different shards under this namespace
      {ns_key_a, ns_key_b} = ShardHelpers.cross_shard_keys_for_namespace(ns)

      assert Router.shard_for(FerricStore.Instance.get(:default), ns <> ns_key_a) !=
               Router.shard_for(FerricStore.Instance.get(:default), ns <> ns_key_b)

      queue = [
        {"SET", [ns_key_a, "ns_val_a"]},
        {"SET", [ns_key_b, "ns_val_b"]}
      ]

      result = Coordinator.execute(queue, %{}, ns)

      assert result == [:ok, :ok]

      # The actual stored keys include the namespace prefix
      assert Router.get(FerricStore.Instance.get(:default), ns <> ns_key_a) == "ns_val_a"
      assert Router.get(FerricStore.Instance.get(:default), ns <> ns_key_b) == "ns_val_b"
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent transactions
  # ---------------------------------------------------------------------------

  describe "concurrent cross-shard transactions" do
    test "two concurrent cross-shard transactions on overlapping keys — both complete, consistent state",
         %{k0: k0, k1: k1} do
      # Pre-set values
      Router.put(FerricStore.Instance.get(:default), k0, "0", 0)
      Router.put(FerricStore.Instance.get(:default), k1, "0", 0)

      # Two concurrent transactions both INCR the same keys across shards
      task1 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", [k0]}, {"INCR", [k1]}], %{}, nil)
        end)

      task2 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", [k0]}, {"INCR", [k1]}], %{}, nil)
        end)

      result1 = Task.await(task1, 10_000)
      result2 = Task.await(task2, 10_000)

      # Both transactions should complete (not error)
      assert is_list(result1)
      assert is_list(result2)

      # Final values should reflect both increments
      assert Router.get(FerricStore.Instance.get(:default), k0) == "2"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "2"
    end
  end
end
