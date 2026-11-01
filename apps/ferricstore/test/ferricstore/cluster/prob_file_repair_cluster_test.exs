defmodule Ferricstore.Cluster.ProbFileRepairClusterTest do
  @moduledoc """
  A node that lost a probabilistic key's file gets an exact copy back from a
  replica that still has it (`Ferricstore.ProbRepair`), instead of the key
  answering errors on that node until it is deleted.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 600_000

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ClusterHelper

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  setup do
    nodes = ClusterHelper.start_cluster(3, shards: 1)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    [first | _] = nodes

    assert {:ok, %{active: 12}} =
             ClusterHelper.run(first.name, FerricStore, :raise_cluster_feature_level, [12])

    {:ok, nodes: nodes}
  end

  test "a node that lost a file gets an exact copy back from a replica", %{nodes: nodes} do
    # The damaged node is a follower, so the writes below answer from a
    # replica that has the file.
    {[first, third], second} = split_follower(nodes)
    cms = "{repair}cms"
    bloom = "{repair}bf"

    assert :ok == dispatch(first, "CMS.INITBYDIM", [cms, "200", "5"])
    assert [3] == dispatch(first, "CMS.INCRBY", [cms, "a", "3"])
    # Big enough (about 2.4 MB) to be copied in several chunks.
    assert :ok == dispatch(first, "BF.RESERVE", [bloom, "0.01", "2000000"])
    assert 1 == dispatch(first, "BF.ADD", [bloom, "x"])
    assert eventually(fn -> Enum.all?(nodes, &file_exists?(&1, cms, "cms")) end)
    assert eventually(fn -> Enum.all?(nodes, &file_exists?(&1, bloom, "bloom")) end)

    remove_file(second, cms, "cms")
    remove_file(second, bloom, "bloom")

    # A write the damaged node cannot apply asks for the repair; the copy it
    # gets back already holds that write, so the count is exact.
    assert [5] == dispatch(first, "CMS.INCRBY", [cms, "a", "2"])
    assert eventually(fn -> dispatch(second, "CMS.QUERY", [cms, "a"]) == [5] end)

    # A read that finds the file missing asks for it too.
    _ = dispatch(second, "BF.EXISTS", [bloom, "x"])
    assert eventually(fn -> dispatch(second, "BF.EXISTS", [bloom, "x"]) == 1 end)

    assert [6] == dispatch(first, "CMS.INCRBY", [cms, "a", "1"])

    for node <- nodes do
      assert eventually(fn -> dispatch(node, "CMS.QUERY", [cms, "a"]) == [6] end), "#{node.name}"
    end

    assert eventually(fn -> file_sha(second, cms, "cms") == file_sha(third, cms, "cms") end)

    assert eventually(fn ->
             file_sha(second, bloom, "bloom") == file_sha(first, bloom, "bloom")
           end)

    # The leader waits in its own apply for the copy while the followers
    # apply the same entry and serve it.
    leader_name = ClusterHelper.find_leader(nodes, 0)
    leader = Enum.find(nodes, &(&1.name == leader_name))
    remove_file(leader, cms, "cms")
    _ = dispatch(leader, "CMS.QUERY", [cms, "a"])
    assert eventually(fn -> dispatch(leader, "CMS.QUERY", [cms, "a"]) == [6] end)
    assert [7] == dispatch(leader, "CMS.INCRBY", [cms, "a", "1"])
  end

  test "a node restarted without a file repairs it, and a file no replica has stays lost", %{
    nodes: nodes
  } do
    {[first, _third], second} = split_follower(nodes)
    cuckoo = "{repair}cf"
    topk = "{repair}topk"

    assert :ok == dispatch(first, "CF.RESERVE", [cuckoo, "1000"])
    assert 1 == dispatch(first, "CF.ADD", [cuckoo, "x"])
    assert :ok == dispatch(first, "TOPK.RESERVE", [topk, "3"])
    assert [nil] == dispatch(first, "TOPK.ADD", [topk, "x"])
    assert eventually(fn -> Enum.all?(nodes, &file_exists?(&1, cuckoo, "cuckoo")) end)
    assert eventually(fn -> Enum.all?(nodes, &file_exists?(&1, topk, "topk")) end)

    # Lost on every node: there is nothing to copy, and the shard keeps working.
    for node <- nodes, do: remove_file(node, topk, "topk")
    assert {:error, "ERR top-k file is missing" <> _} = dispatch(second, "TOPK.ADD", [topk, "y"])
    Process.sleep(2_000)
    refute Enum.any?(nodes, &file_exists?(&1, topk, "topk"))
    assert :ok == dispatch(first, "SET", ["{repair}after", "v"])
    assert eventually(fn -> dispatch(second, "GET", ["{repair}after"]) == "v" end)

    # Lost while the node was down after a clean stop, so replaying its log
    # does not recreate it: the node repairs it once it starts.
    path = prob_path(second, cuckoo, "cuckoo")
    :ok = ClusterHelper.run(second.name, Application, :stop, [:ferricstore])
    {stopped, _remaining} = ClusterHelper.kill_node(nodes, second)
    File.rm!(path)
    restarted = ClusterHelper.restart_node(nodes, stopped, shards: 1)

    assert eventually(fn -> file_exists?(restarted, cuckoo, "cuckoo") end, 300)
    assert eventually(fn -> dispatch(restarted, "CF.EXISTS", [cuckoo, "x"]) == 1 end)
  end

  defp split_follower(nodes) do
    leader = ClusterHelper.find_leader(nodes, 0)
    [follower | _] = Enum.reject(nodes, &(&1.name == leader))
    {nodes -- [follower], follower}
  end

  defp dispatch(node, command, args),
    do: ClusterHelper.run(node.name, Dispatcher, :dispatch, [command, args, ctx(node)])

  defp ctx(node), do: ClusterHelper.run(node.name, FerricStore.Instance, :get, [:default])

  defp prob_path(node, key, type) do
    instance = ctx(node)
    shard = ClusterHelper.run(node.name, Ferricstore.Store.Router, :shard_for, [instance, key])
    shard_path = Ferricstore.DataDir.shard_data_path(instance.data_dir, shard)
    Ferricstore.ProbFile.path(Path.join(shard_path, "prob"), key, type)
  end

  defp file_exists?(node, key, type),
    do: ClusterHelper.run(node.name, File, :exists?, [prob_path(node, key, type)])

  defp remove_file(node, key, type),
    do: :ok = ClusterHelper.run(node.name, File, :rm, [prob_path(node, key, type)])

  defp file_sha(node, key, type) do
    case ClusterHelper.run(node.name, File, :read, [prob_path(node, key, type)]) do
      {:ok, bytes} -> :crypto.hash(:sha256, bytes)
      {:error, reason} -> {:error, reason}
    end
  end

  defp eventually(fun, attempts \\ 150)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(100)
      eventually(fun, attempts - 1)
    end
  end
end
