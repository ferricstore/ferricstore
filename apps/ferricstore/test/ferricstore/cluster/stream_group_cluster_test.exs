defmodule Ferricstore.Cluster.StreamGroupClusterTest do
  @moduledoc """
  XCLAIM, XAUTOCLAIM and the XGROUP subcommands are replicated stream
  operations (from feature level 11): every replica decides from the same
  group state and the entry's apply time, so their pending lists agree.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 300_000

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ClusterHelper

  @stream "{xgroup-cluster}s"

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

  test "every replica keeps the same pending entries", %{nodes: [first, second, third] = nodes} do
    assert {:ok, %{active: 11}} =
             ClusterHelper.run(first.name, FerricStore, :raise_cluster_feature_level, [11])

    :ok = dispatch(first, "XGROUP", ["CREATE", @stream, "g", "0", "MKSTREAM"])
    for id <- ~w(1-0 2-0 3-0 4-0), do: ^id = dispatch(first, "XADD", [@stream, id, "f", id])

    [[_, [_, _, _, _]]] =
      dispatch(second, "XREADGROUP", ~w(GROUP g alice STREAMS) ++ [@stream, ">"])

    assert [_, _] = dispatch(third, "XCLAIM", [@stream, "g", "bob", "0", "1-0", "2-0"])

    assert ["0-0", ["3-0", "4-0"], []] ==
             dispatch(first, "XAUTOCLAIM", [@stream, "g", "carol", "0", "3-0", "JUSTID"])

    assert 1 == dispatch(second, "XGROUP", ["CREATECONSUMER", @stream, "g", "dave"])
    assert 0 == dispatch(third, "XGROUP", ["DELCONSUMER", @stream, "g", "alice"])

    expected = [
      {"1-0", "bob", 2},
      {"2-0", "bob", 2},
      {"3-0", "carol", 1},
      {"4-0", "carol", 1}
    ]

    for node <- nodes do
      assert eventually(fn ->
               pending(node) == expected and
                 consumers(node) == ["bob", "carol", "dave"]
             end),
             "#{node.name}: #{inspect({pending(node), consumers(node)})}"
    end
  end

  defp pending(node) do
    case dispatch(node, "XPENDING", [@stream, "g", "-", "+", "10"]) do
      entries when is_list(entries) ->
        Enum.map(entries, fn [id, consumer, _idle, count] -> {id, consumer, count} end)

      other ->
        other
    end
  end

  defp consumers(node) do
    case dispatch(node, "XINFO", ["CONSUMERS", @stream, "g"]) do
      infos when is_list(infos) -> Enum.map(infos, & &1["name"])
      other -> other
    end
  end

  defp dispatch(node, command, args),
    do: ClusterHelper.run(node.name, Dispatcher, :dispatch, [command, args, ctx(node)])

  defp ctx(node), do: ClusterHelper.run(node.name, FerricStore.Instance, :get, [:default])

  defp eventually(fun, attempts \\ 100)
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
