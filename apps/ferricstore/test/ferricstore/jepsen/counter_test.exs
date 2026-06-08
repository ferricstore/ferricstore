defmodule Ferricstore.Jepsen.CounterTest do
  @moduledoc """
  Jepsen-style counter monotonicity tests from test plan Section 19.4.

  The classic Jepsen counter test: concurrent workers INCR the same key while
  the cluster is being disrupted. After recovery, the counter must be
  monotonically non-decreasing -- no acknowledged increment can be undone.

  In single-node Raft mode (current architecture), each node is independent.
  Counter increments are local to each node. The test verifies:

    1. Concurrent INCR operations on a single node produce the correct final
       value (no double-count, no loss).
    2. After a sibling node crash, the counter on the surviving node is
       unaffected.
    3. The final counter value on each node equals the number of successful
       INCRs on that node.

  When multi-node Raft is implemented, these tests will validate that INCR
  is linearizable across the cluster under leader churn.

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

    Process.sleep(500)

    nodes = ClusterHelper.start_cluster(3)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    %{nodes: nodes}
  end

  describe "19.4 counter monotonicity under faults" do
    @tag :jepsen
    test "counter: no acknowledged increment is lost under sibling crash", %{nodes: nodes} do
      [n1, n2, n3] = nodes
      key = "jepsen:counter"

      {:ok, history} = HistoryRecorder.new()

      on_exit(fn -> if Process.alive?(history), do: HistoryRecorder.stop(history) end)

      # Initialize counter to 0 on n1
      :ok = :rpc.call(n1.name, FerricStore, :set, [key, "0"])

      # 50 concurrent workers increment the counter on n1.
      # Simultaneously, kill n3 to introduce disruption.
      nemesis =
        Task.async(fn ->
          Process.sleep(50)
          ClusterHelper.kill_node(nodes, n3)
          Process.sleep(200)
        end)

      worker_tasks =
        for _w <- 1..50 do
          Task.async(fn ->
            # Use atomic Raft-based incr via Router on the remote node
            case remote_incr(n1.name, key, 1) do
              {:ok, n} ->
                HistoryRecorder.record_ok(
                  history,
                  key,
                  Integer.to_string(n),
                  n1.name
                )

                {:ok, n}

              other ->
                other
            end
          end)
        end

      Task.await(nemesis, 15_000)
      results = Task.await_many(worker_tasks, 15_000)

      # Allow time for any pending flushes
      Process.sleep(200)
      :ok = ClusterHelper.wait_for_leaders([n1, n2], 4, timeout: 15_000)

      # Count successful increments
      ok_count = Enum.count(results, &match?({:ok, _}, &1))

      # Counter on n1 must not have regressed below max ACKed value
      violations = HistoryRecorder.verify_counter_monotonic(history, [n1], key)

      assert violations == [],
             "Counter monotonicity violated:\n" <>
               HistoryRecorder.format_violations(violations)

      # Final value must equal number of successful increments
      {:ok, final_val} = :rpc.call(n1.name, FerricStore, :get, [key])
      assert final_val != nil, "counter key should exist"
      final_int = String.to_integer(final_val)

      assert final_int == ok_count,
             "Counter #{final_int} != successful increments #{ok_count}"

      IO.puts(
        "  Final counter value: #{final_int} " <>
          "(#{ok_count} successful increments, 0 lost)"
      )
    end

    @tag :jepsen
    test "counter: all increments on a node are accounted for", %{nodes: nodes} do
      # Filter for alive nodes (previous test may have killed one)
      alive =
        Enum.filter(nodes, fn n ->
          case :rpc.call(n.name, Node, :self, []) do
            {:badrpc, _} -> false
            _ -> true
          end
        end)

      assert alive != [], "need at least 1 alive node"
      node = hd(alive)

      key = "jepsen:counter2"

      # Initialize
      :ok = :rpc.call(node.name, FerricStore, :set, [key, "0"])

      # 100 concurrent increments on a single node
      tasks =
        for _i <- 1..100 do
          Task.async(fn ->
            remote_incr(node.name, key, 1)
          end)
        end

      results = Task.await_many(tasks, 30_000)
      ok_count = Enum.count(results, &match?({:ok, _}, &1))

      # Allow flush
      Process.sleep(100)

      # Final value must equal number of successful increments
      {:ok, final_val} = :rpc.call(node.name, FerricStore, :get, [key])
      assert final_val != nil, "counter key should exist"
      final_int = String.to_integer(final_val)

      assert final_int == ok_count,
             "Counter #{final_int} != successful increments #{ok_count}"

      IO.puts("  Counter agreement: final=#{final_int}, ok_increments=#{ok_count}")
    end

    @tag :jepsen
    test "counter: observed values are monotonically non-decreasing per node", %{nodes: nodes} do
      alive =
        Enum.filter(nodes, fn n ->
          case :rpc.call(n.name, Node, :self, []) do
            {:badrpc, _} -> false
            _ -> true
          end
        end)

      assert alive != [], "need at least 1 alive node"
      node = hd(alive)

      key = "jepsen:counter3"
      :ok = :rpc.call(node.name, FerricStore, :set, [key, "0"])

      # Sequentially increment and record each observed value
      observed_values =
        for _i <- 1..100 do
          case remote_incr(node.name, key, 1) do
            {:ok, n} -> n
            _ -> nil
          end
        end
        |> Enum.reject(&is_nil/1)

      # Verify strict monotonic increase (each value > previous)
      pairs = Enum.zip(observed_values, tl(observed_values))

      non_monotonic =
        Enum.filter(pairs, fn {prev, curr} -> curr <= prev end)

      assert non_monotonic == [],
             "Counter values are not monotonically increasing: " <>
               "found #{length(non_monotonic)} regressions in " <>
               "#{inspect(Enum.take(non_monotonic, 5))}"

      IO.puts("  #{length(observed_values)} increments observed, all monotonically increasing")
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Atomically increments a counter on the remote node using the Raft-based
  # Router.incr, which guarantees linearizability for concurrent increments.
  # Gets ctx from the remote node, then calls Router.incr with it.
  defp remote_incr(node_name, key, delta) do
    ctx = :rpc.call(node_name, FerricStore.Instance, :get, [:default])
    :rpc.call(node_name, Ferricstore.Store.Router, :incr, [ctx, key, delta])
  end
end
