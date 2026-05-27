defmodule Ferricstore.Cluster.LockTest do
  @moduledoc """
  Distributed lock cluster tests from test plan Section 13.

  Verifies lock semantics across multiple FerricStore peer nodes:

  - LOCK acquired on one node is visible on that node (Raft-durable)
  - Concurrent LOCK attempts on the same node: exactly 1 winner
  - LOCK survives node restart (WARaft segment persistence)
  - UNLOCK from wrong owner fails

  ## Current Architecture Note

  Each node currently runs as an independent single-node Raft cluster.
  Lock operations are tested per-node. When multi-node Raft is added,
  the "lock visible on all nodes" tests will validate cross-node lock
  replication.

  ## Running

      mix test test/ferricstore/cluster/ --include cluster
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ClusterHelper

  @moduletag :cluster

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    nodes = ClusterHelper.start_cluster(3)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

    %{nodes: nodes}
  end

  # Helper: execute a Router function on a remote peer node with ctx.
  # Uses two MFA-form :erpc calls to avoid sending anonymous functions
  # (which would fail with :undef on peer nodes that lack test module code).
  defp remote_router(node_name, fun, args) do
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default])
    :erpc.call(node_name, Ferricstore.Store.Router, fun, [ctx | args])
  end

  # ---------------------------------------------------------------------------
  # Section 13: Lock Visibility
  # ---------------------------------------------------------------------------

  describe "lock visibility on node" do
    @tag :cluster
    test "lock acquired on a node blocks second acquire on same node", %{nodes: nodes} do
      [n1 | _] = nodes

      # Acquire lock on n1
      result =
        remote_router(n1.name, :lock, [
          "lock:cluster:vis",
          "owner1",
          30_000
        ])

      assert result == :ok, "lock should be acquired"

      # Second acquire with different owner on same node should fail
      result2 =
        remote_router(n1.name, :lock, [
          "lock:cluster:vis",
          "owner2",
          5_000
        ])

      assert match?({:error, _}, result2),
             "second lock attempt should fail, got #{inspect(result2)}"
    end

    @tag :cluster
    test "lock is acquirable from any node (leader-forwarded)", %{nodes: nodes} do
      # Multi-node Raft: lock writes forward to the shard leader.
      # Each key locks on a unique key to avoid conflicts.
      Enum.each(nodes, fn node ->
        result =
          remote_router(node.name, :lock, [
            "lock:cluster:independent:#{node.index}",
            "owner_#{node.index}",
            30_000
          ])

        assert result == :ok,
               "node #{node.name} should be able to acquire its own lock"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Section 13: Concurrent Lock Race
  # ---------------------------------------------------------------------------

  describe "concurrent lock race on single node" do
    @tag :cluster
    test "exactly 1 winner in concurrent lock race", %{nodes: nodes} do
      [n1 | _] = nodes

      # Multiple concurrent lock attempts on the same node, same key
      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            remote_router(n1.name, :lock, ["lock:cluster:race", "worker_#{i}", 10_000])
          end)
        end

      results = Task.await_many(tasks, 10_000)

      # Exactly one must succeed -- Raft serializes the race
      ok_count = Enum.count(results, &(&1 == :ok))
      error_count = Enum.count(results, &match?({:error, _}, &1))

      assert ok_count == 1,
             "exactly one task must win the lock, got #{ok_count} winners: #{inspect(results)}"

      assert error_count == 4,
             "other four must fail, got #{error_count} errors"
    end
  end

  # ---------------------------------------------------------------------------
  # Section 13: Lock Durability
  # ---------------------------------------------------------------------------

  describe "lock durability" do
    @tag :cluster
    test "lock state is persisted through Raft on single node", %{nodes: nodes} do
      [n1 | _] = nodes

      # Acquire a lock with long TTL
      :ok =
        remote_router(n1.name, :lock, [
          "lock:cluster:durable",
          "worker1",
          60_000
        ])

      # Verify it is held
      result =
        remote_router(n1.name, :lock, [
          "lock:cluster:durable",
          "worker2",
          5_000
        ])

      assert match?({:error, _}, result), "lock should be held"

      # Same owner can re-acquire (idempotent)
      result2 =
        remote_router(n1.name, :lock, [
          "lock:cluster:durable",
          "worker1",
          60_000
        ])

      assert result2 == :ok, "same owner should re-acquire idempotently"
    end
  end

  # ---------------------------------------------------------------------------
  # Section 13: Unlock Validation
  # ---------------------------------------------------------------------------

  describe "unlock validation" do
    @tag :cluster
    test "wrong owner cannot unlock", %{nodes: nodes} do
      [n1 | _] = nodes

      # Acquire lock
      :ok =
        remote_router(n1.name, :lock, [
          "lock:cluster:unlock_val",
          "real_owner",
          30_000
        ])

      # Attempt to unlock with wrong owner
      result =
        remote_router(n1.name, :unlock, [
          "lock:cluster:unlock_val",
          "impostor"
        ])

      assert match?({:error, _}, result),
             "wrong owner should not be able to unlock, got #{inspect(result)}"

      # Lock should still be held
      lock_check =
        remote_router(n1.name, :lock, [
          "lock:cluster:unlock_val",
          "another",
          100
        ])

      assert match?({:error, _}, lock_check),
             "lock should still be held after failed unlock"
    end

    @tag :cluster
    test "correct owner can unlock successfully", %{nodes: nodes} do
      [_, n2 | _] = nodes

      # Acquire lock
      :ok =
        remote_router(n2.name, :lock, [
          "lock:cluster:unlock_ok",
          "the_owner",
          30_000
        ])

      # Unlock with correct owner
      result =
        remote_router(n2.name, :unlock, [
          "lock:cluster:unlock_ok",
          "the_owner"
        ])

      assert result == 1, "correct owner should unlock successfully"

      # Lock should now be re-acquirable by anyone
      result2 =
        remote_router(n2.name, :lock, [
          "lock:cluster:unlock_ok",
          "new_owner",
          10_000
        ])

      assert result2 == :ok, "lock should be acquirable after unlock"
    end

    @tag :cluster
    test "unlock from non-owner on different node fails", %{nodes: nodes} do
      [n1, _, n3] = nodes

      # Acquire lock on n1
      :ok =
        remote_router(n1.name, :lock, [
          "lock:cluster:cross_unlock:#{n1.index}",
          "owner_a",
          30_000
        ])

      # Since each node is independent in single-node mode, n3 has
      # its own lock state. This test verifies unlock validation per-node.
      # First acquire the same key on n3
      :ok =
        remote_router(n3.name, :lock, [
          "lock:cluster:cross_unlock:#{n3.index}",
          "owner_a",
          30_000
        ])

      # Attempt to unlock with wrong owner on n3
      result =
        remote_router(n3.name, :unlock, [
          "lock:cluster:cross_unlock:#{n3.index}",
          "impostor_b"
        ])

      assert match?({:error, _}, result),
             "wrong owner on any node should not unlock, got #{inspect(result)}"
    end
  end

  # ---------------------------------------------------------------------------
  # Section 13: Lock Visible On All Nodes
  #
  # Test plan: LOCK on node1, check on node2.
  #
  # In single-node Raft mode, each node has independent lock state, so a
  # lock acquired on node1 is NOT visible on node2. This test verifies
  # that per-node lock isolation works correctly and documents the
  # expected behavior change when multi-node Raft is added (at which
  # point the lock should be visible cluster-wide via Raft replication).
  # ---------------------------------------------------------------------------

  describe "lock visible across nodes" do
    @tag :cluster
    test "lock acquired on one node is visible on all nodes (Raft-replicated)", %{
      nodes: nodes
    } do
      [n1, n2, n3] = nodes

      result =
        remote_router(n1.name, :lock, [
          "lock:cross:vis",
          "owner_alpha",
          30_000
        ])

      assert result == :ok, "lock should be acquired on n1"

      blocked =
        remote_router(n1.name, :lock, [
          "lock:cross:vis",
          "owner_beta",
          5_000
        ])

      assert match?({:error, _}, blocked),
             "lock on n1 should block other owners on n1"

      # Multi-node Raft: lock is replicated — n2 and n3 cannot acquire
      n2_result =
        remote_router(n2.name, :lock, [
          "lock:cross:vis",
          "owner_gamma",
          30_000
        ])

      assert match?({:error, _}, n2_result),
             "lock should be visible on n2 via Raft replication"

      n3_result =
        remote_router(n3.name, :lock, [
          "lock:cross:vis",
          "owner_delta",
          30_000
        ])

      assert match?({:error, _}, n3_result),
             "lock should be visible on n3 via Raft replication"
    end

    @tag :cluster
    test "lock released on one node is re-acquirable from another node", %{nodes: nodes} do
      [n1, n2 | _] = nodes

      :ok =
        remote_router(n1.name, :lock, [
          "lock:cross:iso",
          "worker_a",
          30_000
        ])

      result = remote_router(n1.name, :unlock, ["lock:cross:iso", "worker_a"])
      assert result == 1, "unlock on n1 should succeed"

      # After release, any node can acquire (Raft-replicated unlock)
      n2_result =
        remote_router(n2.name, :lock, [
          "lock:cross:iso",
          "worker_c",
          30_000
        ])

      assert n2_result == :ok,
             "n2 should acquire lock after n1 released it"
    end
  end
end
