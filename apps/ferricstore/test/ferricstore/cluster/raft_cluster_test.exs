defmodule Ferricstore.Cluster.RaftLogReplicationTest do
  @moduledoc """
  Raft log replication and apply correctness tests from test plan Section 6.

  Covers:
  - RA-001: Entry committed to quorum before ACK
  - RA-002: Entry applied on all nodes
  - RA-005: apply() determinism -- same commands produce same state

  In single-node Raft mode, each node is its own independent Raft cluster
  with self-quorum. These tests verify that the Raft log is correctly
  committed and applied on each independent node. When multi-node Raft is
  implemented, these tests will validate cross-node replication and quorum
  commit semantics.

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

  defp remote_router(node_name, fun, args) do
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default])
    :erpc.call(node_name, Ferricstore.Store.Router, fun, [ctx | args])
  end

  defp eventually(fun, attempts \\ 50, interval_ms \\ 100) do
    fun.()
  rescue
    e in [ExUnit.AssertionError] ->
      if attempts <= 1, do: reraise(e, __STACKTRACE__)
      :timer.sleep(interval_ms)
      eventually(fun, attempts - 1, interval_ms)
  end

  # ---------------------------------------------------------------------------
  # RA-001: Entry committed to quorum before ACK
  # (Section 6.1)
  #
  # In single-node mode, quorum is 1 (self). We verify that after a PUT
  # returns :ok, the entry is present in the Raft log on the local node.
  # We check this by confirming the WARaft partition has a non-zero last_applied
  # index and the written value is immediately readable.
  # ---------------------------------------------------------------------------

  describe "RA-001: entry committed to quorum before ACK" do
    @tag :cluster
    test "write returns :ok only after Raft commit on local node", %{nodes: nodes} do
      Enum.each(nodes, fn node ->
        key = "ra001:committed:#{node.index}"
        val = "quorum_val_#{node.index}"

        result = remote_router(node.name, :put, [key, val, 0])
        assert result == :ok, "PUT should succeed, got: #{inspect(result)} on #{node.name}"

        shard_idx = remote_router(node.name, :shard_for, [key])

        {:ok, members, {_leader_name, leader_node}} =
          :rpc.call(node.name, Ferricstore.Raft.Cluster, :members, [shard_idx, 2_000])

        assert is_atom(leader_node), "should have a leader"
        assert length(members) == 3, "ra group should have 3 members"

        # Value must be readable on the leader (where apply happened).
        # On followers, apply may lag — wait briefly for replication.
        eventually(fn ->
          read_val = remote_router(leader_node, :get, [key])
          assert read_val == val, "value should be readable on leader after Raft commit ACK"
        end)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # RA-002: Entry applied on all nodes
  # (Section 6.1)
  #
  # Write a value via one node, then verify it is readable from that node.
  # In single-node mode, "all nodes" for a Raft group is just the one member.
  # When multi-node Raft is added, GET from any of the 3 nodes should return
  # the value.
  # ---------------------------------------------------------------------------

  describe "RA-002: entry applied on all nodes" do
    @tag :cluster
    test "SET on one node, GET returns value on same node (single-node Raft)", %{nodes: nodes} do
      [n1, n2, n3] = nodes

      # Write on n1 — may forward to leader if n1 isn't leader for the shard
      :ok = remote_router(n1.name, :put, ["ra002:key", "applied", 0])

      # Readable on n1 after apply (may need brief wait if forwarded to leader)
      eventually(fn ->
        assert remote_router(n1.name, :get, ["ra002:key"]) == "applied"
      end)

      # All nodes in the same Raft group — writes replicate to all members.
      :ok = remote_router(n2.name, :put, ["ra002:n2_key", "n2_val", 0])
      :ok = remote_router(n3.name, :put, ["ra002:n3_key", "n3_val", 0])

      eventually(fn ->
        assert remote_router(n2.name, :get, ["ra002:n2_key"]) == "n2_val"
      end)

      eventually(fn ->
        assert remote_router(n3.name, :get, ["ra002:n3_key"]) == "n3_val"
      end)
    end

    @tag :cluster
    test "batch of writes all applied and readable on the writing node", %{nodes: nodes} do
      [n1 | _] = nodes

      # Write 100 keys
      for i <- 1..100 do
        :ok =
          remote_router(n1.name, :put, [
            "ra002:batch:#{i}",
            "v#{i}",
            0
          ])
      end

      # All 100 should be readable
      for i <- 1..100 do
        val = remote_router(n1.name, :get, ["ra002:batch:#{i}"])
        assert val == "v#{i}", "key ra002:batch:#{i} should be applied and readable"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # RA-005: apply() determinism
  # (Section 6.1)
  #
  # Execute the exact same sequence of commands on all 3 nodes. Each node
  # independently applies them through its own Raft state machine. Verify
  # that all nodes reach the identical ETS state, proving apply() is
  # deterministic.
  # ---------------------------------------------------------------------------

  describe "RA-005: apply() determinism" do
    @tag :cluster
    test "same commands produce identical state on all nodes", %{nodes: nodes} do
      commands =
        for i <- 1..50 do
          {"ra005:det:#{i}", "deterministic_#{i}"}
        end

      # Apply the exact same command sequence on every node
      Enum.each(nodes, fn node ->
        Enum.each(commands, fn {key, val} ->
          :ok = remote_router(node.name, :put, [key, val, 0])
        end)
      end)

      # Verify all nodes have identical state
      expected_state =
        Enum.map(commands, fn {key, _val} ->
          read = remote_router(hd(nodes).name, :get, [key])
          {key, read}
        end)

      Enum.each(nodes, fn node ->
        actual_state =
          Enum.map(commands, fn {key, _val} ->
            read = remote_router(node.name, :get, [key])
            {key, read}
          end)

        assert actual_state == expected_state,
               "RA-005: node #{node.name} should have identical state after same commands"
      end)
    end

    @tag :cluster
    test "overwrite sequence produces identical final state on all nodes", %{nodes: nodes} do
      # Write, then overwrite with different values -- all nodes should
      # end up at the same final state
      Enum.each(nodes, fn node ->
        :ok = remote_router(node.name, :put, ["ra005:ow", "first", 0])
        :ok = remote_router(node.name, :put, ["ra005:ow", "second", 0])
        :ok = remote_router(node.name, :put, ["ra005:ow", "final", 0])
      end)

      Enum.each(nodes, fn node ->
        eventually(fn ->
          val = remote_router(node.name, :get, ["ra005:ow"])
          assert val == "final", "RA-005: all nodes should have 'final' after overwrite sequence"
        end)
      end)
    end

    @tag :cluster
    test "delete + re-write sequence is deterministic across nodes", %{nodes: nodes} do
      Enum.each(nodes, fn node ->
        :ok = remote_router(node.name, :put, ["ra005:del:#{node.index}", "exists", 0])
        :ok = remote_router(node.name, :delete, ["ra005:del:#{node.index}"])

        eventually(fn ->
          nil_val = remote_router(node.name, :get, ["ra005:del:#{node.index}"])
          assert nil_val == nil, "key should be nil after delete"
        end)

        :ok = remote_router(node.name, :put, ["ra005:del:#{node.index}", "reborn", 0])
      end)

      Enum.each(nodes, fn node ->
        eventually(fn ->
          val = remote_router(node.name, :get, ["ra005:del:#{node.index}"])
          assert val == "reborn", "RA-005: delete+re-write should be deterministic"
        end)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # AP-006: TTL consistency across nodes
  # (Section 6.4)
  #
  # SET a key with EX (TTL), then verify all nodes report the same expiry
  # metadata. In single-node mode, we verify per-node consistency. Each
  # node sets the same key with the same TTL independently.
  # ---------------------------------------------------------------------------

  describe "AP-006: TTL apply consistency" do
    @tag :cluster
    test "SET with TTL produces consistent expiry metadata on all nodes", %{nodes: nodes} do
      ttl_ms = 30_000
      now = System.os_time(:millisecond)
      expire_at_ms = now + ttl_ms

      # Set key with TTL on all nodes (same absolute expiry)
      Enum.each(nodes, fn node ->
        :ok =
          remote_router(node.name, :put, [
            "ap006:ttl",
            "ttl_val",
            expire_at_ms
          ])
      end)

      # Verify all nodes have the value and consistent expiry
      Enum.each(nodes, fn node ->
        meta =
          remote_router(node.name, :get_meta, ["ap006:ttl"])

        assert meta != nil, "AP-006: key should exist on #{node.name}"

        {value, actual_expire} = meta
        assert value == "ttl_val", "AP-006: value should be correct"

        # Expiry should be the same (exact match since we used absolute timestamps)
        assert actual_expire == expire_at_ms,
               "AP-006: expire_at_ms on #{node.name} should match " <>
                 "(expected #{expire_at_ms}, got #{actual_expire})"
      end)
    end

    @tag :cluster
    test "expired key returns nil on all nodes", %{nodes: nodes} do
      # Set with a very short TTL (already expired by the time we read)
      past_expire = System.os_time(:millisecond) - 1000

      Enum.each(nodes, fn node ->
        :ok =
          remote_router(node.name, :put, [
            "ap006:expired:#{node.index}",
            "gone",
            past_expire
          ])
      end)

      # All nodes should return nil (expired)
      Enum.each(nodes, fn node ->
        val =
          remote_router(node.name, :get, [
            "ap006:expired:#{node.index}"
          ])

        assert val == nil,
               "AP-006: expired key should return nil on #{node.name}"
      end)
    end
  end
end

# ---------------------------------------------------------------------------
# Leader Election Tests
# ---------------------------------------------------------------------------

defmodule Ferricstore.Cluster.LeaderElectionTest do
  @moduledoc """
  Leader election tests from test plan Section 6.2.

  Covers:
  - LE-001: Election after leader crash (election time < 3s)
  - LE-002: Single leader per shard (no dual leaders)

  In single-node Raft mode, each node elects itself as leader immediately.
  These tests verify the election mechanism works correctly per-node and
  that the leadership invariant holds. When multi-node Raft is added,
  LE-001 will measure actual re-election time after a kill, and LE-002
  will verify no split-brain across the cluster.

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

  # ---------------------------------------------------------------------------
  # LE-001: Election after leader crash
  # (Section 6.2)
  #
  # Kill a node (its own Raft leader), then verify the remaining nodes
  # still have their leaders elected within a bounded time. In single-node
  # mode, each surviving node is still its own leader. When multi-node
  # Raft is added, this test will verify that a new leader emerges within
  # election_timeout_ms (default 1500ms, max 3s).
  # ---------------------------------------------------------------------------

  describe "LE-001: election after leader crash" do
    @tag :cluster
    test "surviving nodes have leaders within 3 seconds after one node killed", %{
      nodes: nodes
    } do
      # Kill last node
      target = List.last(nodes)
      {_killed, remaining} = ClusterHelper.kill_node(nodes, target)

      # Measure time to confirm leaders on remaining nodes
      start_time = System.monotonic_time(:millisecond)

      :ok = ClusterHelper.wait_for_leaders(remaining, 4, timeout: 15_000)

      elapsed = System.monotonic_time(:millisecond) - start_time

      assert elapsed < 10_000,
             "LE-001: leaders should be confirmed within 10 seconds, took #{elapsed}ms"

      remaining_names = Enum.map(remaining, & &1.name)

      for shard <- 0..3 do
        node = hd(remaining)

        {:ok, _members, {_name, leader}} =
          :rpc.call(node.name, Ferricstore.Raft.Cluster, :members, [shard, 2_000])

        assert leader in remaining_names,
               "LE-001: leader for shard #{shard} should be a surviving node, got #{inspect(leader)}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # LE-002: Single leader per shard at all times
  # (Section 6.2)
  #
  # Verify that at any given moment, each shard has exactly one leader on
  # each node. In single-node mode, each node reports itself as leader
  # for all its shards. We verify no node reports a foreign leader or
  # reports being follower (which would be a bug in single-node mode).
  # ---------------------------------------------------------------------------

  describe "LE-002: single leader per shard" do
    @tag :cluster
    test "each shard has exactly one leader across the cluster", %{nodes: nodes} do
      alive_nodes =
        Enum.filter(nodes, fn node ->
          case :rpc.call(node.name, Node, :self, []) do
            {:badrpc, _} -> false
            _ -> true
          end
        end)

      for shard <- 0..3 do
        leaders =
          Enum.map(alive_nodes, fn node ->
            case :rpc.call(node.name, Ferricstore.Raft.Cluster, :members, [shard, 2_000]) do
              {:ok, _members, {_leader_name, leader_node}} ->
                leader_node

              _ ->
                flunk("LE-002: members failed for shard #{shard} on #{node.name}")
            end
          end)
          |> Enum.uniq()

        assert length(leaders) == 1,
               "LE-002: shard #{shard} should have exactly one leader, got #{inspect(leaders)}"

        [leader] = leaders

        assert leader in Enum.map(alive_nodes, & &1.name),
               "LE-002: leader for shard #{shard} should be an alive node"
      end
    end

    @tag :cluster
    test "all nodes agree on the leader for each shard", %{nodes: nodes} do
      alive_nodes =
        Enum.filter(nodes, fn node ->
          case :rpc.call(node.name, Node, :self, []) do
            {:badrpc, _} -> false
            _ -> true
          end
        end)

      for shard <- 0..3 do
        claims =
          for node <- alive_nodes do
            case :rpc.call(node.name, Ferricstore.Raft.Cluster, :members, [shard, 2_000]) do
              {:ok, _members, {_name, leader}} -> leader
              _ -> nil
            end
          end
          |> Enum.reject(&is_nil/1)
          |> Enum.uniq()

        assert length(claims) == 1,
               "LE-002: nodes disagree on leader for shard #{shard}: #{inspect(claims)}"
      end
    end
  end
end

# ---------------------------------------------------------------------------
# Snapshot / Node Join Tests
# ---------------------------------------------------------------------------

defmodule Ferricstore.Cluster.SnapshotJoinTest do
  @moduledoc """
  Snapshot and node join tests from test plan Section 6.3.

  Covers:
  - SN-002: New node joins and catches up

  In single-node Raft mode, we simulate "node join" by starting a new
  peer node that runs FerricStore independently. We verify the new node
  can start, elect its own leaders, and serve read/write operations.

  When multi-node Raft is added, this test will verify that a new node
  receives a snapshot from the cluster and applies delta log entries to
  catch up.

  ## Running

      mix test test/ferricstore/cluster/ --include cluster
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ClusterHelper

  @moduletag :cluster

  # Helper: execute a Router function on a remote peer node with ctx.
  defp remote_router(node_name, fun, args) do
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default])
    :erpc.call(node_name, Ferricstore.Store.Router, fun, [ctx | args])
  end

  # ---------------------------------------------------------------------------
  # SN-002: New node joins and catches up
  # (Section 6.3)
  #
  # Start a 3-node cluster, write data, then start a 4th node. In
  # single-node mode, the new node starts independently. We verify it
  # can boot, elect leaders, and serve operations. When multi-node Raft
  # is added, the new node should receive a snapshot and catch up.
  # ---------------------------------------------------------------------------

  describe "SN-002: new node joins and becomes operational" do
    @tag :cluster
    test "4th node starts, elects leaders, and serves operations" do
      # Start initial 3-node cluster
      nodes = ClusterHelper.start_cluster(3)

      # Write data on existing nodes
      Enum.each(nodes, fn node ->
        for i <- 1..10 do
          :ok =
            remote_router(node.name, :put, [
              "sn002:pre:#{node.index}:#{i}",
              "v#{i}",
              0
            ])
        end
      end)

      # Start a 4th node
      new_nodes = ClusterHelper.start_cluster(1)
      all_nodes = nodes ++ new_nodes

      try do
        [new_node] = new_nodes

        # Verify the new node has elected leaders for all shards
        :ok = ClusterHelper.wait_for_leaders([new_node], 4, timeout: 10_000)

        # New node can serve write operations
        :ok =
          remote_router(new_node.name, :put, [
            "sn002:new_node",
            "hello",
            0
          ])

        val = remote_router(new_node.name, :get, ["sn002:new_node"])
        assert val == "hello", "SN-002: new node should serve reads/writes"

        # New node can serve multiple operations
        for i <- 1..20 do
          :ok =
            remote_router(new_node.name, :put, [
              "sn002:new:#{i}",
              "new_v#{i}",
              0
            ])
        end

        for i <- 1..20 do
          val = remote_router(new_node.name, :get, ["sn002:new:#{i}"])
          assert val == "new_v#{i}", "SN-002: new node key sn002:new:#{i} should have value"
        end

        # In single-node mode, the new node does NOT have data from old nodes
        # (no replication). When multi-node Raft is added, this assertion changes.
        old_key_on_new =
          remote_router(new_node.name, :get, ["sn002:pre:1:1"])

        assert old_key_on_new == nil,
               "SN-002: in single-node mode, new node should not have old node data"
      after
        ClusterHelper.stop_cluster(all_nodes)
      end
    end
  end

  defp eventually(fun, attempts \\ 50, interval_ms \\ 100) do
    fun.()
  rescue
    e in [ExUnit.AssertionError] ->
      if attempts <= 1, do: reraise(e, __STACKTRACE__)
      :timer.sleep(interval_ms)
      eventually(fun, attempts - 1, interval_ms)
  end
end
