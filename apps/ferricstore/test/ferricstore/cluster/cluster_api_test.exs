defmodule Ferricstore.Cluster.ClusterApiTest do
  @moduledoc """
  Unit tests for the WARaft cluster facade.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Cluster

  describe "server ids" do
    test "shard_server_id_on/2 returns the WARaft partition identity" do
      assert Cluster.shard_server_id_on(0, :some_node@host) ==
               {:raft_server_ferricstore_waraft_backend_1, :some_node@host}

      assert Cluster.shard_server_id_on(7, node()) ==
               {:raft_server_ferricstore_waraft_backend_8, node()}
    end

    test "shard_server_id/1 uses the local node" do
      assert Cluster.shard_server_id(0) ==
               {:raft_server_ferricstore_waraft_backend_1, Cluster.local_raft_node()}
    end
  end

  test "system_name/0 returns the WARaft system atom" do
    assert Cluster.system_name() == :ferricstore_waraft_backend
  end

  describe "members/1" do
    test "returns members and a leader for the default app shards" do
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        Ferricstore.Test.ShardHelpers.eventually(
          fn ->
            assert {:ok, members, leader} = Cluster.members(i)
            assert is_list(members)
            assert members != []
            assert leader != nil
          end,
          "shard #{i} should have members",
          10,
          200
        )
      end
    end
  end

  describe "compatibility hooks" do
    test "system start/stop hooks are no-ops because WARaft owns runtime startup" do
      root =
        Path.join(System.tmp_dir!(), "ferricstore_cluster_api_#{System.unique_integer([:positive])}")

      on_exit(fn -> File.rm_rf(root) end)

      assert :ok = Cluster.start_system(root)
      assert :ok = Cluster.stop_system()
      refute File.dir?(Path.join(root, "ra"))
    end

    test "shard start/stop hooks are no-ops" do
      assert :ok =
               Cluster.start_shard_server(
                 0,
                 System.tmp_dir!(),
                 0,
                 Path.join(System.tmp_dir!(), "00000.log"),
                 :missing_keydir
               )

      assert :ok = Cluster.stop_shard_server(0)
    end
  end

  describe "membership facade" do
    test "adding self as voter returns :ok" do
      assert :ok = Cluster.add_member(0, Cluster.local_raft_node(), :voter)
    end

    test "removing a non-member remote node is handled by the facade" do
      result = Cluster.remove_member(0, :nonexistent@nowhere)
      assert result == :ok or match?({:error, _}, result)
    end

    test "trigger_shard_elections_parallel/2 delegates to WARaft" do
      assert :ok = Cluster.trigger_shard_elections_parallel(1, timeout: 5_000)
    end
  end
end
