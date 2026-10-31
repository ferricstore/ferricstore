defmodule Ferricstore.Cluster.LargeCollectionClusterTest do
  @moduledoc """
  From feature level 11 RENAME, the STORE forms, EXPIRE and DEL apply past the
  per-command member bound as one entry each, which every replica applies (by
  its state machine or WARaft's projection) to the same result; from level 12
  EXPIRE and PERSIST do so as compound command entries.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 600_000

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.{CompoundKey, Router}
  alias Ferricstore.Test.ClusterHelper

  @size 10_000
  @hash "{large-cluster}hash"
  @renamed "{large-cluster}renamed"
  @set "{large-cluster}set"
  @union "{large-cluster}union"

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

  test "every replica applies large collection commands alike", %{
    nodes: [first, second, third] = nodes
  } do
    assert {:ok, %{active: 11}} =
             ClusterHelper.run(first.name, FerricStore, :raise_cluster_feature_level, [11])

    for chunk <- Enum.chunk_every(1..@size, 2_000) do
      dispatch(first, "HSET", [@hash | Enum.flat_map(chunk, &["f#{&1}", "v#{&1}"])])
      dispatch(first, "SADD", [@set | Enum.map(chunk, &"m#{&1}")])
    end

    assert :ok == dispatch(second, "RENAME", [@hash, @renamed])
    assert @size == dispatch(third, "SUNIONSTORE", [@union, @set])
    assert 1 == dispatch(first, "EXPIRE", [@renamed, "1000"])

    for node <- nodes do
      assert eventually(fn ->
               dispatch(node, "HLEN", [@renamed]) == @size and
                 dispatch(node, "SCARD", [@union]) == @size and
                 dispatch(node, "EXISTS", [@hash]) == 0 and
                 dispatch(node, "TTL", [@renamed]) in 990..1000
             end),
             "#{node.name}: #{inspect(state(node))}"
    end

    assert 3 == dispatch(second, "DEL", [@renamed, @union, @set])

    for node <- nodes do
      assert eventually(fn ->
               dispatch(node, "EXISTS", [@renamed, @union, @set]) == 0 and
                 member_rows(node, @renamed, CompoundKey.hash_prefix(@renamed)) == 0 and
                 member_rows(node, @union, CompoundKey.set_prefix(@union)) == 0
             end),
             "#{node.name}: #{inspect(state(node))}"
    end
  end

  test "from feature level 12 every replica applies EXPIRE and PERSIST alike", %{
    nodes: [first, second, third] = nodes
  } do
    assert {:ok, %{active: 12}} =
             ClusterHelper.run(first.name, FerricStore, :raise_cluster_feature_level, [12])

    list = "{large-cluster}list"

    for chunk <- Enum.chunk_every(1..5_000, 2_000) do
      dispatch(first, "HSET", [@hash | Enum.flat_map(chunk, &["f#{&1}", "v#{&1}"])])
      dispatch(first, "RPUSH", [list | Enum.map(chunk, &"e#{&1}")])
    end

    assert 1 == dispatch(second, "EXPIRE", [@hash, "1000"])
    assert 1 == dispatch(third, "EXPIRE", [list, "1000", "NX"])

    for node <- nodes do
      assert eventually(fn ->
               dispatch(node, "TTL", [@hash]) in 990..1000 and
                 dispatch(node, "TTL", [list]) in 990..1000
             end),
             "#{node.name}: #{inspect({dispatch(node, "TTL", [@hash]), dispatch(node, "TTL", [list])})}"
    end

    assert 1 == dispatch(third, "PERSIST", [@hash])
    assert 1 == dispatch(first, "PEXPIRE", [list, "500", "LT"])

    for node <- nodes do
      assert eventually(fn ->
               dispatch(node, "TTL", [@hash]) == -1 and
                 dispatch(node, "HLEN", [@hash]) == 5_000 and
                 dispatch(node, "EXISTS", [list]) == 0 and
                 member_rows(node, list, CompoundKey.list_prefix(list)) == 0
             end),
             "#{node.name}: #{inspect({dispatch(node, "TTL", [@hash]), dispatch(node, "EXISTS", [list])})}"
    end
  end

  defp state(node) do
    %{
      renamed: dispatch(node, "HLEN", [@renamed]),
      union: dispatch(node, "SCARD", [@union]),
      hash: dispatch(node, "EXISTS", [@hash]),
      ttl: dispatch(node, "TTL", [@renamed]),
      renamed_rows: member_rows(node, @renamed, CompoundKey.hash_prefix(@renamed))
    }
  end

  defp member_rows(node, key, prefix),
    do:
      node.name |> ClusterHelper.run(Router, :compound_scan, [ctx(node), key, prefix]) |> length()

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
