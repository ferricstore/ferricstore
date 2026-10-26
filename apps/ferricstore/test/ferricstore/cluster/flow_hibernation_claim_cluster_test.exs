defmodule Ferricstore.Cluster.FlowHibernationClaimClusterTest do
  @moduledoc """
  Each node hibernates a Flow once its own LMDB writer has flushed it. Before
  feature level 8 that removed the Flow's index entries, and a claim took the
  Flows still in the index first, so a node whose writer was behind claimed a
  different Flow than the others. From level 8 hibernation keeps the entries,
  and every node claims the same Flow.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 300_000

  alias Ferricstore.Test.ClusterHelper

  @hot_window_ms 1_000

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  setup do
    nodes =
      ClusterHelper.start_cluster(3,
        shards: 1,
        app_env: [flow_hibernation_hot_window_ms: @hot_window_ms]
      )

    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    {:ok, nodes: nodes}
  end

  test "every node claims the same Flow while one node's writer is behind", %{
    nodes: [first, _second, third] = nodes
  } do
    run = fn node, m, f, a -> ClusterHelper.run(node.name, m, f, a) end

    clear_pause = fn ->
      for node <- nodes, do: run.(node, Ferricstore.Flow.Admission, :clear_create_pause, [])
    end

    assert {:ok, %{active: 8}} = run.(first, FerricStore, :raise_cluster_feature_level, [8])
    writer = run.(third, Process, :whereis, [:"Ferricstore.Flow.LMDBWriter.0"])
    :ok = run.(third, :sys, :suspend, [writer])

    try do
      # x is due later than the hot window, so it hibernates on the nodes whose
      # writer flushes; the third node keeps it in memory.
      t0 = System.system_time(:millisecond)
      clear_pause.()

      assert :ok =
               run.(first, FerricStore, :flow_create, ["x", [type: "hc", run_at_ms: t0 + 3_000]])

      await_applied!(nodes)

      state_key = Ferricstore.Flow.Keys.state_key("x", nil)

      assert eventually(fn ->
               for node <- Enum.take(nodes, 2),
                   do: run.(node, Ferricstore.Flow.LMDBWriter, :flush_all, [:default, 1, 10_000])

               Enum.map(nodes, &cached?(&1, state_key)) == [false, false, true]
             end),
             "x should hibernate on the first two nodes only"

      # y is due after x but within the hot window, so it stays in memory.
      Process.sleep(max(t0 + 2_500 - System.system_time(:millisecond), 0))
      clear_pause.()

      assert :ok =
               run.(first, FerricStore, :flow_create, ["y", [type: "hc", run_at_ms: t0 + 3_100]])

      assert {:ok, [%{id: "x"}]} =
               run.(first, FerricStore, :flow_claim_due, [
                 "hc",
                 [worker: "w", limit: 1, lease_ms: 600_000, now_ms: t0 + 4_000]
               ])

      await_applied!(nodes)
    after
      run.(third, :sys, :resume, [writer])
    end

    for node <- nodes,
        do:
          assert(:ok = run.(node, Ferricstore.Flow.LMDBWriter, :flush_all, [:default, 1, 60_000]))

    views =
      for node <- nodes do
        for id <- ["x", "y"] do
          case run.(node, FerricStore, :flow_get, [id]) do
            {:ok, %{state: state, version: version}} -> {id, state, version}
            other -> {id, other}
          end
        end
      end

    assert [[{"x", "running", _}, {"y", _, _}] = view | _] = views
    assert views == List.duplicate(view, 3), inspect(views)
  end

  test "a node that crashes after level 8 is raised keeps the Flows it restored", %{
    nodes: [first, _second, third] = nodes
  } do
    run = fn node, m, f, a -> ClusterHelper.run(node.name, m, f, a) end
    state_key = Ferricstore.Flow.Keys.state_key("old", nil)

    assert {:ok, %{active: 7}} = run.(first, FerricStore, :raise_cluster_feature_level, [7])
    for node <- nodes, do: run.(node, Ferricstore.Flow.Admission, :clear_create_pause, [])
    t0 = System.system_time(:millisecond)

    assert :ok =
             run.(first, FerricStore, :flow_create, ["old", [type: "hc", run_at_ms: t0 + 60_000]])

    # Below level 8 hibernation removes the keydir row on every node.
    assert eventually(fn ->
             for node <- nodes,
                 do: run.(node, Ferricstore.Flow.LMDBWriter, :flush_all, [:default, 1, 10_000])

             Enum.all?(nodes, &(keydir_row(&1, state_key) == []))
           end),
           "old should hibernate on every node"

    assert {:ok, %{active: 8}} = run.(first, FerricStore, :raise_cluster_feature_level, [8])
    await_applied!(nodes)

    {killed, survivors} = ClusterHelper.kill_node(nodes, third)

    restarted =
      ClusterHelper.restart_node([killed | survivors], killed,
        shards: 1,
        app_env: [flow_hibernation_hot_window_ms: @hot_window_ms]
      )

    nodes = survivors ++ [restarted]
    await_applied!(nodes)

    for node <- nodes do
      assert [{^state_key, _value, _, _, _, _, _}] = keydir_row(node, state_key),
             "#{node.name} should hold old's keydir row"
    end

    assert {:ok, [%{id: "old"}]} =
             run.(first, FerricStore, :flow_claim_due, [
               "hc",
               [worker: "w", limit: 1, lease_ms: 600_000, now_ms: t0 + 61_000]
             ])

    await_applied!(nodes)

    views =
      for node <- nodes do
        case run.(node, FerricStore, :flow_get, ["old"]) do
          {:ok, %{state: state, version: version}} -> {state, version}
          other -> other
        end
      end

    assert [{"running", _} = view | _] = views
    assert views == List.duplicate(view, 3), inspect(views)
  end

  defp keydir_row(node, key) do
    ctx = ClusterHelper.run(node.name, FerricStore.Instance, :get, [:default])
    ClusterHelper.run(node.name, :ets, :lookup, [elem(ctx.keydir_refs, 0), key])
  end

  # Whether the node's keydir row for the key still holds the value.
  defp cached?(node, key) do
    ctx = ClusterHelper.run(node.name, FerricStore.Instance, :get, [:default])

    case ClusterHelper.run(node.name, :ets, :lookup, [elem(ctx.keydir_refs, 0), key]) do
      [{^key, value, _, _, _, _, _}] -> is_binary(value)
      _missing -> false
    end
  end

  # A follower applies an entry once the leader tells it the entry committed,
  # which can be after the leader answered; until then reads on it show the
  # state before.
  defp await_applied!(nodes) do
    target = nodes |> Enum.map(&last_applied/1) |> Enum.max()

    for node <- nodes do
      assert eventually(fn -> last_applied(node) >= target end),
             "#{node.name} applied #{last_applied(node)} of #{target}"
    end
  end

  defp last_applied(node) do
    case ClusterHelper.run(node.name, Ferricstore.Raft.WARaftBackend, :storage_status, [0]) do
      {:ok, status} -> Keyword.fetch!(status, :last_applied)
      _unavailable -> 0
    end
  end

  defp eventually(fun, attempts \\ 200)
  defp eventually(_fun, 0), do: false

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(50)
      eventually(fun, attempts - 1)
    end
  end
end
