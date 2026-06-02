defmodule Ferricstore.Cluster.TopologyTest do
  @moduledoc """
  Cluster topology tests covering test plan Sections 5.1-5.2.

  Non-destructive tests that verify basic cluster startup and operation.
  Destructive tests (node kill, partition) are in separate test modules
  to avoid `setup_all` conflicts.

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
    :ok = ClusterHelper.ensure_node_reachable(node_name, timeout: 2_000)
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
  # Section 5.2: Three-Node Cluster -- Normal Operation
  # ---------------------------------------------------------------------------

  @tag :cluster
  test "all nodes started and all shards have elected leaders", %{nodes: nodes} do
    shard_count = 4

    Enum.each(nodes, fn node ->
      for shard <- 0..(shard_count - 1) do
        result = :rpc.call(node.name, Ferricstore.Raft.Cluster, :members, [shard, 2_000])

        assert match?({:ok, _, _}, result),
               "shard #{shard} on #{node.name} should have a leader, got #{inspect(result)}"
      end
    end)
  end

  @tag :cluster
  test "each node can serve read/write operations", %{nodes: nodes} do
    Enum.each(nodes, fn node ->
      key = "topo:rw:#{node.index}"
      value = "value_#{node.index}"

      result = remote_router(node.name, :put, [key, value, 0])
      assert result == :ok, "PUT on #{node.name} should succeed"

      # Read may need to wait for follower apply if write was forwarded
      eventually(fn ->
        read = remote_router(node.name, :get, [key])
        assert read == value, "GET on #{node.name} should return #{value}"
      end)
    end)
  end

  @tag :cluster
  test "nodes are connected via Erlang distribution", %{nodes: nodes} do
    [n1, n2, n3] = nodes

    n1_nodes = :rpc.call(n1.name, Node, :list, [])
    assert n2.name in n1_nodes, "n1 should see n2"
    assert n3.name in n1_nodes, "n1 should see n3"
  end

  @tag :cluster
  test "multiple writes and reads work correctly on each node", %{nodes: nodes} do
    Enum.each(nodes, fn node ->
      for i <- 1..20 do
        key = "topo:multi:#{node.index}:#{i}"
        val = "v#{i}"
        :ok = remote_router(node.name, :put, [key, val, 0])
      end

      for i <- 1..20 do
        key = "topo:multi:#{node.index}:#{i}"
        val = remote_router(node.name, :get, [key])
        assert val == "v#{i}", "key #{key} on #{node.name} should have value v#{i}"
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # CT-007: Follower read returns correct value
  # (Section 5.2 C3-003)
  #
  # In single-node Raft mode, each node is its own independent leader. This
  # test verifies that a value written on one node can be read back from a
  # different node (each operating independently). When multi-node Raft is
  # added, this test will verify true follower reads via Raft replication.
  # ---------------------------------------------------------------------------

  @tag :cluster
  test "CT-007: write on one node, read from another node (Raft-replicated)", %{nodes: nodes} do
    [n1, n2, n3] = nodes

    :ok = remote_router(n1.name, :put, ["ct007:n1", "from_n1", 0])
    :ok = remote_router(n2.name, :put, ["ct007:n2", "from_n2", 0])
    :ok = remote_router(n3.name, :put, ["ct007:n3", "from_n3", 0])

    # Multi-node Raft: writes replicate to all members. Cross-node reads work.
    eventually(fn ->
      assert remote_router(n2.name, :get, ["ct007:n1"]) == "from_n1",
             "n2 should see n1's key via Raft replication"
    end)

    eventually(fn ->
      assert remote_router(n1.name, :get, ["ct007:n3"]) == "from_n3",
             "n1 should see n3's key via Raft replication"
    end)

    eventually(fn ->
      assert remote_router(n3.name, :get, ["ct007:n2"]) == "from_n2",
             "n3 should see n2's key via Raft replication"
    end)
  end
end

# ---------------------------------------------------------------------------
# Destructive test: node failure (separate module, own cluster)
# ---------------------------------------------------------------------------

defmodule Ferricstore.Cluster.NodeFailureTest do
  @moduledoc """
  Cluster tests for node failure scenarios from test plan Section 5.3.

  Uses its own cluster via `setup_all` so that destructive operations
  (killing nodes) do not affect other test modules.
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
  defp remote_router(node_name, fun, args) do
    :ok = ClusterHelper.ensure_node_reachable(node_name, timeout: 2_000)
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default])
    :erpc.call(node_name, Ferricstore.Store.Router, fun, [ctx | args])
  end

  @tag :cluster
  test "remaining nodes continue operating after one node is killed", %{nodes: nodes} do
    # Write to all nodes first
    Enum.each(nodes, fn node ->
      remote_router(node.name, :put, [
        "topo:kill:before:#{node.index}",
        "safe",
        0
      ])
    end)

    # Kill last node
    target = List.last(nodes)
    {_killed, remaining} = ClusterHelper.kill_node(nodes, target)

    # Allow time for the BEAM distribution to settle after the kill.
    # Without this, RPC calls to remaining nodes can transiently fail
    # while the distribution layer detects the dead peer and cleans up
    # pending monitor/link notifications.
    Process.sleep(1_000)

    # Wait for leaders to be re-confirmed on remaining nodes.
    # On slow CI runners, distribution cleanup and leader re-election can
    # take longer than expected after a node kill.
    :ok = ClusterHelper.wait_for_leaders(remaining, 4, timeout: 15_000)

    # Remaining nodes should still work
    Enum.each(remaining, fn node ->
      result =
        remote_router(node.name, :put, [
          "topo:kill:after:#{node.index}",
          "still_working",
          0
        ])

      assert result == :ok,
             "node #{node.name} should still serve writes after sibling killed"

      val =
        remote_router(node.name, :get, [
          "topo:kill:after:#{node.index}"
        ])

      assert val == "still_working"
    end)
  end

  @tag :cluster
  test "killed node does not affect other nodes' shard leaders", %{nodes: nodes} do
    alive_nodes =
      Enum.filter(nodes, fn node ->
        case :rpc.call(node.name, Node, :self, []) do
          {:badrpc, _} -> false
          _ -> true
        end
      end)

    :ok = ClusterHelper.wait_for_leaders(alive_nodes, 4, timeout: 15_000)

    alive_names = Enum.map(alive_nodes, & &1.name)

    for shard <- 0..3 do
      leader = ClusterHelper.find_leader(alive_nodes, shard)

      assert leader in alive_names,
             "shard #{shard} leader #{inspect(leader)} should be an alive node"
    end
  end
end

# ---------------------------------------------------------------------------
# CT-005 & CT-006: Failover tests (own cluster to avoid state pollution)
# ---------------------------------------------------------------------------

defmodule Ferricstore.Cluster.FailoverTest do
  @moduledoc """
  Leader failover tests from test plan Section 5.3.

  Covers:
  - CT-005: Write after failover succeeds on surviving nodes
  - CT-006: Data survives leader failover

  Uses its own cluster so that destructive node kills do not pollute
  other test modules.

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

    # Allow distribution to fully settle from any previous module's
    # cluster teardown before starting our own fresh cluster.
    Process.sleep(2_000)

    nodes = ClusterHelper.start_cluster(3)
    :ok = ClusterHelper.wait_for_leaders(nodes, 4, timeout: 30_000)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    %{nodes: nodes}
  end

  # Helper: execute a Router function on a remote peer node with ctx.
  defp remote_router(node_name, fun, args) do
    :ok = ClusterHelper.ensure_node_reachable(node_name, timeout: 2_000)
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default])
    :erpc.call(node_name, Ferricstore.Store.Router, fun, [ctx | args])
  end

  # Calls a Router function on a remote node with retries. Returns the result
  # on success, or re-raises on persistent failure. Retries on :erpc errors
  # to handle transient distribution hiccups after cluster stop/start cycles.
  defp rpc_with_retry(node_name, fun, args, retries \\ 20) do
    remote_router(node_name, fun, args)
  rescue
    e ->
      if retries > 0 do
        Process.sleep(500)
        rpc_with_retry(node_name, fun, args, retries - 1)
      else
        reraise e, __STACKTRACE__
      end
  catch
    :exit, reason ->
      if retries > 0 do
        Process.sleep(500)
        rpc_with_retry(node_name, fun, args, retries - 1)
      else
        exit(reason)
      end
  end

  # ---------------------------------------------------------------------------
  # CT-005: Write after failover succeeds on surviving nodes
  # (Section 5.3 CF-002)
  #
  # Kill the leader for shard 0, then verify that writes succeed on the
  # surviving nodes. In single-node mode each node is its own leader, so
  # "failover" means the remaining independent nodes continue operating.
  # ---------------------------------------------------------------------------

  @tag :cluster
  test "CT-005: write after failover succeeds on surviving nodes", %{nodes: nodes} do
    [n1, n2, n3] = nodes

    # Write a value on n1 before kill (with retry to handle transient
    # distribution issues from previous module's cluster teardown)
    result_pre =
      rpc_with_retry(n1.name, :put, [
        "ct005:pre_kill",
        "before",
        0
      ])

    assert result_pre == :ok, "CT-005: pre-kill write on n1 should succeed"

    # Kill n3
    {_killed, remaining} = ClusterHelper.kill_node(nodes, n3)

    # Wait for failover to complete before asserting post-failover writes.
    :ok = ClusterHelper.wait_for_leaders(remaining, 4, timeout: 15_000)

    # n1 and n2 should still serve writes
    result1 =
      rpc_with_retry(n1.name, :put, [
        "ct005:post_kill:n1",
        "after_failover_n1",
        0
      ])

    assert result1 == :ok,
           "CT-005: write on n1 should succeed after n3 killed"

    result2 =
      rpc_with_retry(n2.name, :put, [
        "ct005:post_kill:n2",
        "after_failover_n2",
        0
      ])

    assert result2 == :ok,
           "CT-005: write on n2 should succeed after n3 killed"

    # Verify reads work too
    assert rpc_with_retry(n1.name, :get, ["ct005:post_kill:n1"]) ==
             "after_failover_n1"

    assert rpc_with_retry(n2.name, :get, ["ct005:post_kill:n2"]) ==
             "after_failover_n2"
  end

  # ---------------------------------------------------------------------------
  # CT-006: Data survives leader failover
  # (Section 5.3 CF-003)
  #
  # Write data, kill a node, verify the surviving nodes still have their
  # data intact. In single-node mode, each node's data is local only.
  # When multi-node Raft is added, the data should be replicated and
  # visible on any surviving node.
  # ---------------------------------------------------------------------------

  @tag :cluster
  test "CT-006: data written before kill survives on surviving nodes", %{nodes: nodes} do
    # Filter alive nodes (n3 may have been killed by the CT-005 test)
    alive_nodes =
      Enum.filter(nodes, fn node ->
        case :rpc.call(node.name, Node, :self, []) do
          {:badrpc, _} -> false
          _ -> true
        end
      end)

    assert length(alive_nodes) >= 2, "need at least 2 alive nodes for CT-006"

    # Write data to all alive nodes
    Enum.each(alive_nodes, fn node ->
      remote_router(node.name, :put, [
        "ct006:durable:#{node.index}",
        "must_survive_#{node.index}",
        0
      ])
    end)

    # Kill one alive node
    [target | survivors] = alive_nodes
    {_killed, _remaining} = ClusterHelper.kill_node(nodes, target)

    # Allow distribution to settle
    Process.sleep(500)

    # Verify all surviving nodes still have their own data
    Enum.each(survivors, fn node ->
      val =
        remote_router(node.name, :get, [
          "ct006:durable:#{node.index}"
        ])

      assert val == "must_survive_#{node.index}",
             "CT-006: data on #{node.name} should survive sibling kill"
    end)
  end
end

# ---------------------------------------------------------------------------
# Destructive test: network partition (separate module, own cluster)
# ---------------------------------------------------------------------------

defmodule Ferricstore.Cluster.PartitionTest do
  @moduledoc """
  Cluster tests for network partition scenarios from test plan Section 5.5.

  Covers CT-009 (rejoin after partition -- node catches up).

  Uses its own cluster via `setup_all` so that partition simulation does
  not affect other test modules.
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
    :ok = ClusterHelper.ensure_node_reachable(node_name, timeout: 2_000)
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default])
    :erpc.call(node_name, Ferricstore.Store.Router, fun, [ctx | args])
  end

  defp eventually(fun, attempts \\ 50, interval_ms \\ 200) do
    fun.()
  rescue
    e in [ExUnit.AssertionError] ->
      if attempts <= 1, do: reraise(e, __STACKTRACE__)
      :timer.sleep(interval_ms)
      eventually(fun, attempts - 1, interval_ms)
  end

  @tag :cluster
  test "partitioned node loses Raft membership", %{nodes: nodes} do
    [n1, n2, n3] = nodes
    shards = :rpc.call(n1.name, Application, :get_env, [:ferricstore, :shard_count, 4])

    # Stop consensus on n3 to simulate local consensus unavailability.
    :ok = ClusterHelper.stop_consensus(n3.name)

    # A stopped consensus node may have been leader for some shards. Wait until
    # the remaining majority has elected replacement leaders before asserting
    # write availability; Raft failover is not instantaneous.
    :ok = ClusterHelper.wait_for_leaders([n1, n2], shards, timeout: 15_000)

    # Majority side (n1+n2) can still write (2-of-3 quorum)
    result =
      remote_router(n1.name, :put, [
        "topo:part:majority",
        "works_with_quorum",
        0
      ])

    assert result == :ok, "majority side should still accept writes"

    # Restart consensus on n3 to simulate heal.
    :ok = ClusterHelper.start_consensus(n3.name)

    ClusterHelper.wait_for_leaders(nodes, shards, timeout: 10_000)

    # n3 should catch up and see the write
    eventually(fn ->
      val = remote_router(n3.name, :get, ["topo:part:majority"])
      assert val == "works_with_quorum", "n3 should catch up after rejoin"
    end)
  end

  @tag :cluster
  test "heal partition: node catches up on writes made during its absence", %{nodes: nodes} do
    [n1, n2, n3] = nodes
    shards = :rpc.call(n1.name, Application, :get_env, [:ferricstore, :shard_count, 4])

    # Write before partition
    remote_router(n1.name, :put, ["topo:heal:pre", "before", 0])

    # Stop consensus on n3
    :ok = ClusterHelper.stop_consensus(n3.name)

    # The write below is only guaranteed after the surviving majority has
    # replacement leaders for shards that were led by n3.
    :ok = ClusterHelper.wait_for_leaders([n1, n2], shards, timeout: 15_000)

    # Write on n1 during n3's absence
    :ok = remote_router(n1.name, :put, ["topo:heal:during", "during", 0])

    # Restart consensus on n3
    :ok = ClusterHelper.start_consensus(n3.name)

    ClusterHelper.wait_for_leaders(nodes, shards, timeout: 10_000)

    # n1 should still have its data
    n1_val = remote_router(n1.name, :get, ["topo:heal:pre"])
    assert n1_val == "before"

    n1_during = remote_router(n1.name, :get, ["topo:heal:during"])
    assert n1_during == "during"

    # n3 catches up and can operate
    eventually(fn ->
      val = remote_router(n3.name, :get, ["topo:heal:during"])
      assert val == "during", "n3 should catch up on writes made during absence"
    end)

    n3_result = remote_router(n3.name, :put, ["topo:heal:post", "after", 0])
    assert n3_result == :ok
  end

  @tag :cluster
  test "CT-009: rejoin after partition -- node catches up on 50 writes", %{
    nodes: nodes
  } do
    [n1, n2, n3] = nodes
    shards = :rpc.call(n1.name, Application, :get_env, [:ferricstore, :shard_count, 4])

    # Stop consensus on n3 — simulates partition
    :ok = ClusterHelper.stop_consensus(n3.name)

    # Wait for majority-side leaders before writing. If the stopped node was a
    # leader, writes can correctly fail until the new election completes.
    :ok = ClusterHelper.wait_for_leaders([n1, n2], shards, timeout: 15_000)

    # Write 50 keys on n1 during n3's absence (majority still has quorum)
    for i <- 1..50 do
      :ok =
        remote_router(n1.name, :put, [
          "ct009:majority:#{i}",
          "val_#{i}",
          0
        ])
    end

    # Restart consensus on n3 — heals the partition
    :ok = ClusterHelper.start_consensus(n3.name)

    ClusterHelper.wait_for_leaders(nodes, shards, timeout: 15_000)

    # Verify n1 still has its data
    for i <- 1..50 do
      val = remote_router(n1.name, :get, ["ct009:majority:#{i}"])
      assert val == "val_#{i}", "n1 should retain writes"
    end

    # n3 catches up via Raft log replay — eventually sees all 50 keys
    eventually(
      fn ->
        missing =
          Enum.count(1..50, fn i ->
            remote_router(n3.name, :get, ["ct009:majority:#{i}"]) == nil
          end)

        assert missing == 0, "n3 still missing #{missing}/50 keys"
      end,
      60,
      200
    )

    # All nodes can write after rejoin
    :ok = remote_router(n1.name, :put, ["ct009:post:n1", "ok", 0])
    :ok = remote_router(n2.name, :put, ["ct009:post:n2", "ok", 0])
    :ok = remote_router(n3.name, :put, ["ct009:post:n3", "ok", 0])

    eventually(fn -> assert remote_router(n1.name, :get, ["ct009:post:n1"]) == "ok" end)
    eventually(fn -> assert remote_router(n2.name, :get, ["ct009:post:n2"]) == "ok" end)
    eventually(fn -> assert remote_router(n3.name, :get, ["ct009:post:n3"]) == "ok" end)
  end
end
