defmodule Ferricstore.Jepsen.AsyncDurabilityTest do
  @moduledoc """
  Jepsen-style async durability tests from test plan Section 19.7.

  Verifies durability guarantees under `:async` namespace durability mode.
  In async mode, writes are ACKed after the leader WAL fdatasync only --
  replication to followers is not guaranteed before ACK. The durability
  contract:

    * Leader restart (crash + recover same node): ACKed writes survive
      because the WAL was fsynced before ACK.
    * Leader kill (process terminated, no restart): writes may be lost if
      replication had not completed before the kill. This is the documented
      async data loss window and is expected per spec.

  ## Architecture note

  In single-node Raft mode, each node is its own independent cluster with
  self-quorum. Async durability writes locally (ETS + BitcaskWriter) in the
  Router before returning :ok to the caller, then submits the batch to
  Raft via `Batcher.async_submit` for replication. After a node restart,
  the Bitcask log ensures the data survives (Raft WAL replays anything
  that hadn't been persisted yet).

  ## Running

      mix test test/ferricstore/jepsen/ --include jepsen
  """

  use ExUnit.Case, async: false
  @moduletag skip: "async durability feature removed; quorum is the only supported durability"

  alias Ferricstore.Test.ClusterHelper

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

  # ---------------------------------------------------------------------------
  # 19.7.1 Configure async namespace and verify writes survive
  #
  # Configure a namespace "async:" with durability :async. Write 100 keys.
  # All ACKed writes must survive because the Bitcask write path fsyncs.
  #
  # NOTE: In single-node mode, we verify that async writes are durable on the
  # writing node itself (the WAL fsync guarantee). When multi-node Raft is
  # added, this test will verify replication after restart.
  # ---------------------------------------------------------------------------

  describe "async: ACKed writes survive on the writing node" do
    @tag :jepsen
    test "100 async writes all readable after write completes", %{nodes: nodes} do
      alive_nodes = alive(nodes)

      Enum.each(alive_nodes, fn node ->
        # Configure "async" namespace with :async durability
        config_result =
          :rpc.call(node.name, Ferricstore.NamespaceConfig, :set, [
            "async",
            "durability",
            "async"
          ])

        assert config_result == :ok,
               "Failed to set async durability on #{node.name}: #{inspect(config_result)}"

        # Verify the configuration took effect
        {:ok, ns_config} =
          :rpc.call(node.name, Ferricstore.NamespaceConfig, :get, ["async"])

        assert ns_config.durability == :async,
               "Namespace durability should be :async, got #{inspect(ns_config.durability)}"
      end)

      # Write 100 keys to each alive node using the async namespace prefix
      acked_writes =
        Enum.flat_map(alive_nodes, fn node ->
          for i <- 1..100 do
            key = "async:restart:#{node.index}:#{i}"
            value = "v#{i}"

            result =
              :rpc.call(node.name, FerricStore, :set, [key, value])

            if result == :ok do
              {node, key, value}
            else
              nil
            end
          end
          |> Enum.filter(&(&1 != nil))
        end)

      assert acked_writes != [],
             "At least some async writes should have succeeded"

      # Verify all ACKed writes are immediately readable on their writing node
      violations =
        Enum.flat_map(acked_writes, fn {node, key, expected_value} ->
          {:ok, actual} = :rpc.call(node.name, FerricStore, :get, [key])

          if actual == expected_value do
            []
          else
            [{:lost_write, key, expected: expected_value, got: actual, node: node.name}]
          end
        end)

      assert violations == [],
             "Async writes lost immediately after ACK:\n#{format_violations(violations)}"

      IO.puts(
        "  #{length(acked_writes)} async writes verified readable immediately after ACK"
      )
    end

    @tag :jepsen
    test "async writes persist through shard flush cycle", %{nodes: nodes} do
      alive_nodes = alive(nodes)
      n1 = hd(alive_nodes)

      # Configure async namespace
      :ok =
        :rpc.call(n1.name, Ferricstore.NamespaceConfig, :set, [
          "async",
          "durability",
          "async"
        ])

      # Write keys and track ACKed ones
      acked =
        for i <- 1..100 do
          key = "async:flush:#{i}"
          value = "flush_v#{i}"

          case :rpc.call(n1.name, FerricStore, :set, [key, value]) do
            :ok -> {key, value}
            _ -> nil
          end
        end
        |> Enum.filter(&(&1 != nil))

      # Give the async write path time to flush to disk
      Process.sleep(100)

      # Verify all ACKed writes are still readable
      missing =
        Enum.filter(acked, fn {key, value} ->
          {:ok, actual} = :rpc.call(n1.name, FerricStore, :get, [key])
          actual != value
        end)

      assert missing == [],
             "#{length(missing)} async writes missing after flush cycle: #{inspect(Enum.take(missing, 5))}"
    end
  end

  # ---------------------------------------------------------------------------
  # 19.7.2 Document the async data loss window
  #
  # Write and immediately kill a node -- the race condition between ACK and
  # replication means the write MAY or MAY NOT survive on other nodes. Both
  # outcomes are correct under async durability.
  #
  # In single-node mode, killing the node loses the in-memory ETS state.
  # The Bitcask WAL should have the data if it was fsynced before ACK,
  # but the node is gone. This test documents the expected behavior.
  # ---------------------------------------------------------------------------

  describe "async: documents data loss window" do
    @tag :jepsen
    test "async write to killed node: surviving nodes may or may not have data", %{nodes: nodes} do
      alive_nodes = alive(nodes)
      assert length(alive_nodes) >= 2, "Need at least 2 alive nodes for this test"

      [target, reader | _] = alive_nodes

      # Configure async on target
      :ok =
        :rpc.call(target.name, Ferricstore.NamespaceConfig, :set, [
          "async",
          "durability",
          "async"
        ])

      # Write an async key
      key = "async:race:lossy"

      _result =
        :rpc.call(target.name, FerricStore, :set, [key, "maybe_lost"])

      # Kill the target node immediately after write
      {_killed, _remaining} = ClusterHelper.kill_node(nodes, target)

      # In single-node mode, the reader node is independent and never had this data.
      # This documents the expected behavior: async writes on a killed node are
      # lost from the cluster perspective when that node doesn't come back.
      {:ok, result_on_reader} = :rpc.call(reader.name, FerricStore, :get, [key])

      # Both outcomes are correct:
      # - nil: reader never had the data (single-node mode, no replication)
      # - "maybe_lost": multi-node mode replication completed before kill
      assert result_on_reader == nil or result_on_reader == "maybe_lost",
             "Unexpected value on surviving node: #{inspect(result_on_reader)}"

      IO.puts(
        "  async race result on surviving node: #{inspect(result_on_reader)} " <>
          "(nil expected in single-node mode)"
      )
    end

    @tag :jepsen
    test "quorum namespace writes are immediately durable (contrast with async)", %{nodes: nodes} do
      alive_nodes = alive(nodes)

      Enum.each(alive_nodes, fn node ->
        key = "quorum:contrast:#{node.index}"
        value = "durable_value"

        :ok = :rpc.call(node.name, FerricStore, :set, [key, value])

        # In multi-node Raft the write may land on the leader and replicate
        # back asynchronously, so poll until the local node sees it.
        Ferricstore.Test.ShardHelpers.eventually(fn ->
          {:ok, read} = :rpc.call(node.name, FerricStore, :get, [key])

          assert read == value,
                 "Quorum write should be durable on #{node.name}"
        end, "quorum write durable on #{node.name}", 50, 100)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Filters nodes to only those that are still alive (haven't been killed
  # by a previous test in the same module).
  defp alive(nodes) do
    Enum.filter(nodes, fn node ->
      case :rpc.call(node.name, Node, :self, []) do
        {:badrpc, _} -> false
        _ -> true
      end
    end)
  end

  defp format_violations(violations) do
    Enum.map_join(violations, "\n", fn v -> "  #{inspect(v)}" end)
  end
end
