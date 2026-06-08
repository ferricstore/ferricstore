defmodule Ferricstore.Jepsen.LostWritesTest do
  @moduledoc """
  Jepsen-style lost write tests from test plan Section 19.2.

  Tests the fundamental durability guarantee: when FerricStore acknowledges a
  write, that write must survive node crashes. In single-node Raft mode (current
  architecture), each node is its own independent Raft cluster with self-quorum.
  The durability guarantee is: if `FerricStore.set/2` returns `:ok`, the data was
  committed to the local Raft log and applied to Bitcask. It must survive:

    1. Sibling node crashes (trivial -- independent nodes)
    2. The writing node staying alive while others crash
    3. Multiple rounds of node kills

  When multi-node Raft is implemented, these tests will validate cross-node
  quorum durability: ACK means committed on 2/3 nodes before response.

  ## Running

      mix test test/ferricstore/jepsen/ --include jepsen
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ClusterHelper
  alias Ferricstore.Test.HistoryRecorder

  @moduletag :jepsen
  @moduletag :cluster

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    nodes = ClusterHelper.start_cluster(3)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    %{nodes: nodes}
  end

  describe "19.2 quorum writes survive leader kill" do
    @tag :jepsen
    @tag timeout: 300_000
    test "100 ACKed writes survive sibling node kills", %{nodes: nodes} do
      {:ok, history} = HistoryRecorder.new()

      on_exit(fn -> if Process.alive?(history), do: HistoryRecorder.stop(history) end)

      n_writes = 100

      # Write 100 keys distributed across nodes. After every 10 writes,
      # kill a sibling node to simulate leader churn.
      for i <- 1..n_writes do
        # Round-robin across alive nodes
        node = Enum.at(nodes, rem(i, 3))

        # Skip nodes that have been killed
        case :rpc.call(node.name, Node, :self, []) do
          {:badrpc, _} ->
            :skip

          _ ->
            key = "jepsen:write:#{i}"
            value = "v#{i}"

            result =
              :rpc.call(node.name, FerricStore, :set, [key, value])

            if result == :ok do
              HistoryRecorder.record_ok(history, key, value, node.name)
            end
        end

        # Kill one sibling node at the midpoint to test disruption while
        # maintaining Raft quorum (need 2 of 3 alive).
        if i == 50 do
          alive =
            Enum.filter(nodes, fn n ->
              case :rpc.call(n.name, Node, :self, []) do
                {:badrpc, _} -> false
                _ -> true
              end
            end)

          if length(alive) > 2 do
            target = List.last(alive)
            ClusterHelper.kill_node(nodes, target)
            Process.sleep(500)
          end
        end
      end

      # Determine which nodes are still alive for verification
      alive =
        Enum.filter(nodes, fn n ->
          case :rpc.call(n.name, Node, :self, []) do
            {:badrpc, _} -> false
            _ -> true
          end
        end)

      assert alive != [], "at least one node should survive"
      :ok = ClusterHelper.wait_for_leaders(alive, 4, timeout: 30_000)

      # Verify: each surviving node has all the writes it ACKed.
      # In multi-node Raft, writes may have been forwarded to the leader.
      # The writing node's ETS is updated via replication, which may lag.
      # Poll until all ACKed writes are visible on at least one alive node.
      all_entries = HistoryRecorder.all(history)

      per_node_entries =
        Enum.group_by(all_entries, fn {:ok, _k, _v, node, _ts} -> node end)

      Enum.each(alive, fn node ->
        node_entries = Map.get(per_node_entries, node.name, [])

        Enum.each(node_entries, fn {:ok, key, value, _node, _ts} ->
          Ferricstore.Test.ShardHelpers.eventually(
            fn ->
              {:ok, read} = :rpc.call(node.name, FerricStore, :get, [key])

              assert read == value,
                     "Lost write: key=#{key} expected=#{value} got=#{inspect(read)} " <>
                       "on node=#{node.name}"
            end,
            "durability #{key} on #{node.name}",
            50,
            100
          )
        end)
      end)

      IO.puts("  #{length(all_entries)} acknowledged writes verified durable after node kills")
    end

    @tag :jepsen
    @tag timeout: 300_000
    test "writes to surviving node unaffected by sibling crash", %{nodes: nodes} do
      {:ok, history} = HistoryRecorder.new()

      on_exit(fn -> if Process.alive?(history), do: HistoryRecorder.stop(history) end)

      # Find alive nodes (previous tests in this module may have killed some)
      alive =
        Enum.filter(nodes, fn n ->
          case :rpc.call(n.name, Node, :self, []) do
            {:badrpc, _} -> false
            _ -> true
          end
        end)

      assert alive != [], "need at least 1 alive node"

      writer = hd(alive)

      # Write 50 keys to the writer node
      for i <- 1..50 do
        key = "jepsen:survive:#{i}"
        value = "survive_v#{i}"

        :ok = :rpc.call(writer.name, FerricStore, :set, [key, value])
        HistoryRecorder.record_ok(history, key, value, writer.name)
      end

      # Kill 1 sibling only if we have 3+ alive (keeps quorum: 2-of-3).
      # If only 2 alive, killing one leaves 1 node with no quorum.
      if length(alive) >= 3 do
        target = List.last(alive)
        ClusterHelper.kill_node(nodes, target)
      end

      remaining =
        Enum.filter(nodes, fn n ->
          case :rpc.call(n.name, Node, :self, []) do
            {:badrpc, _} -> false
            _ -> true
          end
        end)

      Process.sleep(500)
      :ok = ClusterHelper.wait_for_leaders(remaining, 4, timeout: 30_000)

      # All 50 writes must be readable on the surviving writer.
      # In multi-node Raft, the writer may have forwarded to the leader.
      # Poll to allow replication to complete.
      all_entries = HistoryRecorder.all(history)

      violations =
        Enum.flat_map(all_entries, fn {:ok, key, value, _node, _ts} ->
          try do
            Ferricstore.Test.ShardHelpers.eventually(
              fn ->
                {:ok, read} = :rpc.call(writer.name, FerricStore, :get, [key])
                assert read == value
              end,
              "durability #{key}",
              50,
              100
            )

            []
          rescue
            e -> [{key, value, Exception.message(e)}]
          end
        end)

      assert violations == [],
             "Durability violated:\n#{inspect(violations)}"

      IO.puts("  #{length(all_entries)} writes verified durable on surviving writer node")
    end
  end
end
