defmodule Ferricstore.Cluster.WatchExecClusterTest do
  @moduledoc """
  Every replica applies a watched EXEC, so all of them must reach the leader's
  decision. WATCH on a promoted hash used to compare per-node revision tokens:
  the leader applied the transaction and both followers skipped it.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 300_000

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Store.{CompoundKey, Router}
  alias Ferricstore.Test.ClusterHelper
  alias Ferricstore.Transaction.Coordinator

  @hash "{watch-exec}hash"
  @plain "{watch-exec}plain"

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  setup do
    nodes = ClusterHelper.start_cluster(3, shards: 1)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    [first | _rest] = nodes

    assert {:ok, %{active: 5}} =
             ClusterHelper.run(first.name, FerricStore, :raise_cluster_feature_level, [5])

    fields = Enum.flat_map(1..150, &["f#{&1}", "v#{&1}"])
    assert 150 == dispatch(first, "HSET", [@hash | fields])
    assert :ok == dispatch(first, "SET", [@plain, "v0"])

    for node <- nodes do
      assert eventually(fn -> promoted?(node, @hash) end), "#{node.name} did not promote"
    end

    {:ok, nodes: nodes}
  end

  test "a clean watched EXEC applies on every replica", %{nodes: [first | _] = nodes} do
    watched = watch_tokens(first, [@hash, @plain])

    assert [0, :ok] =
             exec(first, watched, [{"HSET", [@hash, "f1", "new"]}, {"SET", [@plain, "v1"]}])

    for node <- nodes do
      assert eventually(fn -> applied?(node, "new", "v1") end),
             "#{node.name} skipped the transaction"
    end
  end

  test "a watched EXEC after a write applies on no replica", %{nodes: [first, second | _] = nodes} do
    watched = watch_tokens(first, [@hash, @plain])
    assert 0 == dispatch(second, "HSET", [@hash, "f2", "concurrent"])

    assert nil == exec(first, watched, [{"HSET", [@hash, "f1", "new"]}, {"SET", [@plain, "v1"]}])
    assert :ok == dispatch(first, "SET", ["{watch-exec}after", "x"])

    for node <- nodes do
      assert eventually(fn -> get(node, "{watch-exec}after") == "x" end)
      refute applied?(node, "new", "v1"), "#{node.name} applied an aborted transaction"
    end
  end

  defp watch_tokens(node, keys) do
    tokens = ClusterHelper.run(node.name, Router, :watch_tokens, [ctx(node), keys])
    assert map_size(tokens) == length(keys)
    tokens
  end

  defp exec(node, watched, commands) do
    queue =
      Enum.map(commands, fn {command, args} ->
        {:ok, prepared} = ClusterHelper.run(node.name, PreparedCommand, :prepare, [command, args])
        prepared
      end)

    ClusterHelper.run(node.name, Coordinator, :execute, [queue, watched, nil])
  end

  defp applied?(node, field_value, plain_value) do
    field =
      ClusterHelper.run(node.name, Router, :compound_get, [
        ctx(node),
        @hash,
        CompoundKey.hash_field(@hash, "f1")
      ])

    field == field_value and get(node, @plain) == plain_value
  end

  defp get(node, key), do: ClusterHelper.run(node.name, Router, :get, [ctx(node), key])

  defp dispatch(node, command, args),
    do: ClusterHelper.run(node.name, Dispatcher, :dispatch, [command, args, ctx(node)])

  defp promoted?(node, key) do
    ctx = ctx(node)
    shard = ClusterHelper.run(node.name, Router, :shard_name, [ctx, 0])
    ClusterHelper.run(node.name, GenServer, :call, [shard, {:promoted?, key}])
  end

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
