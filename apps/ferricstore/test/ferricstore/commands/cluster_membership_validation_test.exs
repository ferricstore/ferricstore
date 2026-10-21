defmodule Ferricstore.Commands.ClusterMembershipValidationTest do
  @moduledoc """
  A membership change naming something that is not a distributed node
  (`name@host`) is refused before it is proposed. Once committed, the shard's
  storage rejected the peer while persisting the config, on every replay, so
  every shard stopped serving.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Raft.Cluster, as: RaftCluster

  # Makes sure the atom exists, as it would after any earlier use of the name.
  @not_a_node :k

  test "CLUSTER.PROMOTE and CLUSTER.DEMOTE refuse a name that is not a node" do
    ctx = FerricStore.Instance.get(:default)
    storage = storage_pids(ctx)

    for command <- ["CLUSTER.PROMOTE", "CLUSTER.DEMOTE"] do
      assert {:error, "ERR invalid node name" <> _} =
               Dispatcher.dispatch(command, [Atom.to_string(@not_a_node)], ctx)
    end

    for membership <- [:voter, :promotable] do
      assert {:error, {:invalid_node_name, @not_a_node}} =
               RaftCluster.add_member(0, @not_a_node, membership)
    end

    assert {:error, {:invalid_node_name, @not_a_node}} =
             RaftCluster.remove_member(0, @not_a_node)

    Process.sleep(200)
    assert storage_pids(ctx) == storage

    for n <- 1..(ctx.shard_count * 8),
        do: assert(:ok = Dispatcher.dispatch("SET", ["cluster-validate:#{n}", "v"], ctx))
  end

  defp storage_pids(ctx) do
    for shard <- 0..(ctx.shard_count - 1),
        do: Process.whereis(:"raft_storage_ferricstore_waraft_backend_#{shard}")
  end
end
