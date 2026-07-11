defmodule Ferricstore.Jepsen.SetDurabilityTest do
  @moduledoc """
  Jepsen-style set durability tests from test plan Section 19.5.

  SADD members that were ACKed must always be in SMEMBERS after any fault.
  This is the Jepsen set test -- one of the clearest correctness checks
  because set membership is unambiguous: a member is either present or it
  is not.

  The test verifies:

    1. ACKed SADD members survive sibling node crashes.
    2. No phantom members appear (members that were never ACKed).
    3. After multiple rounds of node kills, all ACKed members are still present.

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

  describe "19.5 set members never disappear" do
    @tag :jepsen
    @tag :set_sibling_kill
    @tag timeout: 300_000
    test "set: no ACKed member missing after sibling node kills", %{nodes: nodes} do
      [n1, _n2, n3] = nodes
      key = "jepsen:set"

      {:ok, history} = HistoryRecorder.new()

      on_exit(fn -> if Process.alive?(history), do: HistoryRecorder.stop(history) end)

      # Establish committed state before the fault.
      for i <- 1..100 do
        member = "member:#{i}"
        result = :rpc.call(n1.name, FerricStore, :sadd, [key, [member]])

        assert {:ok, 1} = result
        HistoryRecorder.record_ok(history, key, member, n1.name)
      end

      # Remove the current sibling leader, preserving a two-voter quorum. The
      # immediate write may succeed or fail while leadership is unavailable.
      ClusterHelper.kill_node(nodes, n3)
      Process.sleep(500)

      maybe_record_set_write(history, n1, key, 101)

      alive =
        Enum.filter(nodes, fn n ->
          case :rpc.call(n.name, Node, :self, []) do
            {:badrpc, _} -> false
            _ -> true
          end
        end)

      :ok = ClusterHelper.wait_for_leaders(alive, 4, timeout: 30_000)

      # Continue through the surviving cluster after election. These writes are
      # the regression for redirected ACKs outrunning apply on n1.
      for i <- 102..200 do
        member = "member:#{i}"
        assert {:ok, 1} = :rpc.call(n1.name, FerricStore, :sadd, [key, [member]])
        HistoryRecorder.record_ok(history, key, member, n1.name)
      end

      # In multi-node Raft, writes forwarded to the leader replicate back
      # to followers asynchronously. Poll until all ACKed members appear.
      violations = wait_for_set_durability(history, [n1], key, 50, 100)

      assert violations == [],
             "Set durability violated:\n" <>
               HistoryRecorder.format_violations(violations)

      {:ok, members} = :rpc.call(n1.name, FerricStore, :smembers, [key])

      IO.puts(
        "  #{length(members)} members present; 0 ACKed members lost " <>
          "after sibling node kills"
      )
    end

    @tag :jepsen
    @tag timeout: 300_000
    test "set: no phantom member appears that was never ACKed", %{nodes: nodes} do
      alive =
        Enum.filter(nodes, fn n ->
          case :rpc.call(n.name, Node, :self, []) do
            {:badrpc, _} -> false
            _ -> true
          end
        end)

      assert alive != [], "need at least 1 alive node"
      :ok = ClusterHelper.wait_for_leaders(alive, 4, timeout: 30_000)
      node = hd(alive)

      key = "jepsen:set:phantom"
      {:ok, history} = HistoryRecorder.new()

      on_exit(fn -> if Process.alive?(history), do: HistoryRecorder.stop(history) end)

      # Add 50 members, recording which ones were ACKed
      for i <- 1..50 do
        member = "m:#{i}"

        case :rpc.call(node.name, FerricStore, :sadd, [key, [member]]) do
          {:ok, _count} ->
            HistoryRecorder.record_ok(history, key, member, node.name)

          _other ->
            :ok
        end
      end

      # Build the set of ACKed members
      acked =
        history
        |> HistoryRecorder.all()
        |> Enum.map(fn {:ok, _k, v, _n, _t} -> v end)
        |> MapSet.new()

      # Poll until all ACKed members appear on the node (replication lag).
      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          {:ok, present} = :rpc.call(node.name, FerricStore, :smembers, [key])
          present_set = MapSet.new(present)

          # Check for phantom members: present but never ACKed
          phantom = MapSet.difference(present_set, acked)

          assert MapSet.size(phantom) == 0,
                 "Phantom members appeared that were never ACKed: " <>
                   inspect(MapSet.to_list(phantom))

          # Check for missing ACKed members
          missing = MapSet.difference(acked, present_set)

          assert MapSet.size(missing) == 0,
                 "ACKed members missing from SMEMBERS: " <>
                   inspect(MapSet.to_list(missing))
        end,
        "set phantom check on #{node.name}",
        50,
        100
      )

      IO.puts(
        "  #{MapSet.size(acked)} ACKed members present; " <>
          "0 phantom members; 0 missing members"
      )
    end

    @tag :jepsen
    @tag timeout: 300_000
    test "set: concurrent SADD operations produce correct membership", %{nodes: nodes} do
      alive =
        Enum.filter(nodes, fn n ->
          case :rpc.call(n.name, Node, :self, []) do
            {:badrpc, _} -> false
            _ -> true
          end
        end)

      assert alive != [], "need at least 1 alive node"
      :ok = ClusterHelper.wait_for_leaders(alive, 4, timeout: 30_000)
      node = hd(alive)

      key = "jepsen:set:concurrent"
      {:ok, history} = HistoryRecorder.new()

      on_exit(fn -> if Process.alive?(history), do: HistoryRecorder.stop(history) end)

      # 50 concurrent SADD tasks, each adding a unique member
      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            member = "concurrent:#{i}"

            case :rpc.call(node.name, FerricStore, :sadd, [key, [member]]) do
              {:ok, _count} ->
                HistoryRecorder.record_ok(history, key, member, node.name)
                {:ok, member}

              other ->
                {:error, other}
            end
          end)
        end

      results = Task.await_many(tasks, 15_000)
      ok_count = Enum.count(results, &match?({:ok, _}, &1))

      # Poll until all ACKed members appear (replication lag on followers).
      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          violations = HistoryRecorder.verify_set_durability(history, [node], key)

          assert violations == [],
                 "Set durability violated under concurrency:\n" <>
                   HistoryRecorder.format_violations(violations)
        end,
        "concurrent set durability on #{node.name}",
        50,
        100
      )

      {:ok, members} = :rpc.call(node.name, FerricStore, :smembers, [key])

      assert length(members) == ok_count,
             "Expected #{ok_count} members after #{ok_count} successful SADDs, " <>
               "got #{length(members)}"

      IO.puts(
        "  #{ok_count} concurrent SADD operations; " <>
          "#{length(members)} members present; 0 missing"
      )
    end
  end

  defp wait_for_set_durability(history, nodes, key, attempts, interval_ms) do
    Enum.reduce_while(1..attempts, [], fn attempt, _previous ->
      case HistoryRecorder.verify_set_durability(history, nodes, key) do
        [] ->
          {:halt, []}

        violations when attempt < attempts ->
          Process.sleep(interval_ms)
          {:cont, violations}

        violations ->
          {:halt, violations}
      end
    end)
  end

  defp maybe_record_set_write(history, node, key, member_number) do
    member = "member:#{member_number}"

    case :rpc.call(node.name, FerricStore, :sadd, [key, [member]]) do
      {:ok, 1} -> HistoryRecorder.record_ok(history, key, member, node.name)
      {:error, _reason} -> :ok
    end
  end
end
