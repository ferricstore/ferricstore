defmodule Ferricstore.Cluster.CrossNodeDecideWriteTest do
  @moduledoc """
  Commands that read a key and decide their write from it hold a key latch,
  but the latch is node-local: clients on other nodes are not serialized by
  it. Such a command has to make its decision where the write applies.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 300_000

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
    [first | _rest] = nodes

    assert {:ok, %{active: 5}} =
             ClusterHelper.run(first.name, FerricStore, :raise_cluster_feature_level, [5])

    {:ok, nodes: nodes}
  end

  test "EXPIRE NX from every node has one winner", %{nodes: [first | _] = nodes} do
    for round <- 1..10 do
      string = "{decide}s#{round}"
      hash = "{decide}h#{round}"
      assert :ok == dispatch(first, "SET", [string, "v"])
      assert 1 == dispatch(first, "HSET", [hash, "f", "v"])

      for key <- [string, hash] do
        replies =
          race(nodes, fn node, client ->
            dispatch(node, "EXPIRE", [key, Integer.to_string(1000 + client), "NX"])
          end)

        assert Enum.count(replies, &(&1 == 1)) == 1, "#{key}: #{inspect(replies)}"
      end
    end
  end

  # A blocked list pop waits in its own node's waiter registry. A push made
  # through another node only notified that node's registry, so the pop slept
  # until its timeout (or for good, with timeout 0) while the list had data.
  test "a list push wakes list waiters on every node", %{nodes: [first, second, third]} do
    for {push, args} <- [
          {"RPUSH", ["{decide}l", "a"]},
          {"LPUSH", ["{decide}l2", "b"]},
          {"LMOVE", ["{decide}src", "{decide}l3", "LEFT", "RIGHT"]}
        ] do
      key = Enum.at(args, if(push == "LMOVE", do: 1, else: 0))
      if push == "LMOVE", do: assert(1 == dispatch(first, "RPUSH", ["{decide}src", "m"]))

      waiters =
        for node <- [second, third] do
          Task.async(fn -> await_waiter_notify(node, key) end)
        end

      Process.sleep(300)
      dispatch(first, push, args)

      assert [:notified, :notified] == Task.await_many(waiters, 10_000), push
    end
  end

  # Claim waiters are woken by the node that wrote the flow, so a claim
  # blocked on another node slept through its block and then answered
  # nothing although the flow had been ready for most of it. It now claims
  # once more when the block ends.
  test "a blocked claim takes a flow created through another node", %{
    nodes: [first, second, _third] = nodes
  } do
    type = "cross-node-claim-#{System.unique_integer([:positive])}"

    claim =
      Task.async(fn ->
        ClusterHelper.run(second.name, FerricStore, :flow_claim_due, [
          type,
          [worker: "w", block_ms: 1_500]
        ])
      end)

    Process.sleep(300)
    # A crowded local disk pauses new flows on the peers; the test is not about that.
    for node <- nodes,
        do: ClusterHelper.run(node.name, Ferricstore.Flow.Admission, :clear_create_pause, [])

    assert :ok ==
             ClusterHelper.run(first.name, FerricStore, :flow_create, ["#{type}-1", [type: type]])

    assert {:ok, [_claimed]} = Task.await(claim, 10_000)
  end

  # Registers a waiter for `key` on `node` and reports whether it was notified.
  defp await_waiter_notify(node, key) do
    code = """
    Ferricstore.Waiters.register(key, self(), System.monotonic_time(:millisecond) + 5_000)

    receive do
      {:waiter_notify, ^key} -> :notified
    after
      3_000 -> :not_notified
    end
    """

    {result, _binding} = ClusterHelper.run(node.name, Code, :eval_string, [code, [key: key]])
    result
  end

  # Two clients on each node, all at once.
  defp race(nodes, fun) do
    for node <- nodes, client <- 1..2 do
      Task.async(fn -> fun.(node, client) end)
    end
    |> Task.await_many(120_000)
  end

  defp dispatch(node, command, args),
    do: ClusterHelper.run(node.name, Dispatcher, :dispatch, [command, args, ctx(node)])

  defp ctx(node), do: ClusterHelper.run(node.name, FerricStore.Instance, :get, [:default])
end
