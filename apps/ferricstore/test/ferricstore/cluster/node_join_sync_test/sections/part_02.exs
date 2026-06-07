defmodule Ferricstore.Cluster.NodeJoinSyncTest.Sections.Part02 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ClusterHelper

  describe "leader failover during sync" do
    @tag timeout: 180_000
    test "sync recovers after leader dies mid-join" do
      nodes = ClusterHelper.start_cluster(3, shards: @shards)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
      [node_a, node_b, _node_c] = nodes
      keys = write_keys(node_a, "pre_failover", 1..30)

      eventually(
        fn ->
          assert Enum.all?(keys, fn k -> read_key(node_b, k) != nil end), "keys missing on b"
        end,
        "pre-replication incomplete",
        60,
        500
      )

      leader_name = ClusterHelper.find_leader(nodes, 0)
      leader_node = Enum.find(nodes, &(&1.name == leader_name))
      {_killed, remaining} = ClusterHelper.kill_node(nodes, leader_node)
      ClusterHelper.wait_for_leaders(remaining, @shards, timeout: 15_000)
      surviving = hd(remaining)
      node_d = ClusterHelper.start_node(shards: @shards)
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)
      :ok = join_cluster(node_d, surviving)
      post_keys = write_keys(surviving, "post_failover", 1..10)
      all_keys = keys ++ post_keys

      eventually(
        fn ->
          missing = Enum.count(all_keys, fn k -> read_key(node_d, k) == nil end)
          assert missing == 0, "#{missing} keys missing after leader failover join"
        end,
        "keys not replicated after failover join",
        120,
        500
      )

      eventually(
        fn ->
          assert dump_keydir_sorted(surviving) == dump_keydir_sorted(node_d),
                 "keydir mismatch after failover join"
        end,
        "keydirs not converged after failover",
        40,
        500
      )
    end
  end
    end
  end
end
