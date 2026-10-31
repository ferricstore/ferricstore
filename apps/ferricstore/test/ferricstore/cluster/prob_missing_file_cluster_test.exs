defmodule Ferricstore.Cluster.ProbMissingFileClusterTest do
  @moduledoc """
  A node that lost a Bloom filter's file keeps applying the shard: the key is
  damaged on that node only, the others keep it, and DEL heals it everywhere.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 300_000

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ClusterHelper

  @key "{prob-loss}bf"
  @other "{prob-loss}other"

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  setup do
    nodes = ClusterHelper.start_cluster(3, shards: 1)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    {:ok, nodes: nodes}
  end

  test "the node that lost the file keeps applying, and DEL heals it", %{
    nodes: [first, second, _third] = nodes
  } do
    assert :ok == dispatch(first, "BF.RESERVE", [@key, "0.01", "100"])
    assert eventually(fn -> Enum.all?(nodes, &(prob_files(&1) != [])) end)

    [path] = prob_files(second)
    :ok = ClusterHelper.run(second.name, File, :rm, [path])

    # The damaged node may be the one answering; either way the others apply.
    reply = dispatch(first, "BF.ADD", [@key, "a"])

    assert reply == 1 or
             match?({:error, "ERR bloom filter file is missing on this node" <> _}, reply)

    assert :ok == dispatch(first, "SET", [@other, "v"])

    for node <- nodes do
      assert eventually(fn -> dispatch(node, "GET", [@other]) == "v" end), "#{node.name}"
    end

    assert {:error, "ERR bloom filter file is missing" <> _} =
             dispatch(second, "BF.EXISTS", [@key, "a"])

    for node <- nodes -- [second] do
      assert eventually(fn -> dispatch(node, "BF.EXISTS", [@key, "a"]) == 1 end), "#{node.name}"
    end

    assert 1 == dispatch(first, "DEL", [@key])
    assert 1 == dispatch(first, "BF.ADD", [@key, "b"])

    for node <- nodes do
      assert eventually(fn -> dispatch(node, "BF.EXISTS", [@key, "b"]) == 1 end), "#{node.name}"
    end
  end

  defp prob_files(node) do
    data_dir = ClusterHelper.run(node.name, Application, :get_env, [:ferricstore, :data_dir])
    filename = Ferricstore.ProbFile.filename(@key, "bloom")
    ClusterHelper.run(node.name, Path, :wildcard, [Path.join([data_dir, "**", filename])])
  end

  defp dispatch(node, command, args),
    do: ClusterHelper.run(node.name, Dispatcher, :dispatch, [command, args, ctx(node)])

  defp ctx(node), do: ClusterHelper.run(node.name, FerricStore.Instance, :get, [:default])

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
