defmodule Ferricstore.Raft.WARaftBackendTest.Sections.ThreePeerBackendClusterRemovesMemberThroughBackendApi do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog

      alias Ferricstore.ErrorReasons
      alias Ferricstore.Raft.Cluster, as: RaftCluster
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Raft.WARaftStorage
      alias Ferricstore.Store.{BlobRef, BlobStore}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Raft.WARaftBackendTest.LabelCounter
      alias Ferricstore.Raft.WARaftBackendTest.OversizedLabel

  test "three peer backend cluster removes a member through the backend API" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    removed = Enum.find(names, &(&1 != leader))
    kept = Enum.reject(names, &(&1 == removed))
    removed_peer = {:raft_server_ferricstore_waraft_backend_1, removed}

    assert {:ok, {:raft_log_pos, _, _}} =
             :rpc.call(leader, WARaftBackend, :adjust_membership, [0, :remove, removed])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and removed_peer not in membership
           end)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-membership:k", "v", 0}
             ])

    assert eventually(fn ->
             Enum.all?(kept, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster-membership:k"]) ==
                 "v"
             end)
           end)
  end

  @tag :cluster
  test "backend cluster adds a new member and catches up real Bitcask state" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add:before", "v1", 0}
             ])

    assert eventually(fn ->
             Enum.all?(Enum.map(initial_nodes, & &1.name), fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster-add:before"]) ==
                 "v1"
             end)
           end)

    start_waraft_backend_peer!(joining_node, unique)
    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert {:ok, {:raft_log_pos, _, _}} =
             :rpc.call(leader, WARaftBackend, :add_member, [0, joining_node.name])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer in membership
           end)

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-add:before"
             ]) == "v1"
           end)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add:after", "v2", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster-add:after"]) ==
                 "v2"
             end)
           end)
  end

  @tag :cluster
  test "Raft.Cluster add_member delegates to WARaft backend when selected" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-add:before", "v1", 0}
             ])

    start_waraft_backend_peer!(joining_node, unique)
    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert :ok = :rpc.call(leader, RaftCluster, :add_member, [0, joining_node.name, :voter])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer in membership
           end)

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-api-add:before"
             ]) == "v1"
           end)
  end

  @tag :cluster
  test "Raft.Cluster add_member redirects from WARaft follower to leader" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)
    follower = initial_nodes |> Enum.map(& &1.name) |> Enum.find(&(&1 != leader))

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-follower-add:before", "v1", 0}
             ])

    start_waraft_backend_peer!(joining_node, unique)
    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert :ok = :rpc.call(follower, RaftCluster, :add_member, [0, joining_node.name, :voter])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer in membership
           end)

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-api-follower-add:before"
             ]) == "v1"
           end)
  end

  @tag :cluster
  test "Raft.Cluster promotable WARaft member catches up from snapshot without becoming voter" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)
    follower = initial_nodes |> Enum.map(& &1.name) |> Enum.find(&(&1 != leader))

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-participant:before", "v1", 0}
             ])

    start_waraft_backend_peer!(joining_node, unique)
    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert :ok =
             :rpc.call(follower, RaftCluster, :add_member, [0, joining_node.name, :promotable])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer not in membership
           end)

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-api-participant:before"
             ]) == "v1"
           end)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-participant:after", "v2", 0}
             ])

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-api-participant:after"
             ]) == "v2"
           end)
  end

  @tag :cluster
  test "Raft.Cluster demotes an existing WARaft voter from a follower caller" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    target = Enum.find(names, &(&1 != leader))
    caller = Enum.find(names, &(&1 not in [leader, target]))
    target_peer = {:raft_server_ferricstore_waraft_backend_1, target}

    assert target_peer in :rpc.call(leader, WARaftBackend, :membership, [0])
    assert :ok = :rpc.call(caller, RaftCluster, :add_member, [0, target, :promotable])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and target_peer not in membership
           end)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-demote:k", "v", 0}
             ])

    assert eventually(fn ->
             :rpc.call(target, WARaftBackend, :local_get, [
               0,
               "backend-cluster-api-demote:k"
             ]) == "v"
           end)
  end

  @tag :cluster
  test "Raft.Cluster remove_member redirects from WARaft follower to leader" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    removed = Enum.find(names, &(&1 != leader))
    kept = Enum.reject(names, &(&1 == removed))
    caller = Enum.find(kept, &(&1 != leader))
    removed_peer = {:raft_server_ferricstore_waraft_backend_1, removed}

    assert :ok = :rpc.call(caller, RaftCluster, :remove_member, [0, removed])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and removed_peer not in membership
           end)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-remove:k", "v", 0}
             ])

    assert eventually(fn ->
             Enum.all?(kept, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-api-remove:k"
               ]) == "v"
             end)
           end)
  end

  @tag :cluster
  test "Raft.Cluster members reports WARaft leader through shared API" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    leader_peer = {:raft_server_ferricstore_waraft_backend_1, leader}
    follower = Enum.find(names, &(&1 != leader))

    assert {:ok, members, ^leader_peer} = :rpc.call(leader, RaftCluster, :members, [0])
    assert leader_peer in members
    assert {:ok, follower_members, ^leader_peer} = :rpc.call(follower, RaftCluster, :members, [0])
    assert leader_peer in follower_members
  end

  @tag :cluster
  test "Raft.Cluster transfer_leadership delegates to WARaft handover when selected" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    target = Enum.find(names, &(&1 != leader))
    caller = Enum.find(names, &(&1 not in [leader, target]))

    assert :ok = :rpc.call(caller, RaftCluster, :transfer_leadership, [0, target])

    assert eventually(fn ->
             case :rpc.call(target, WARaftBackend, :status, [0]) do
               status when is_list(status) -> Keyword.get(status, :state) == :leader
               _other -> false
             end
           end)

    assert :ok =
             :rpc.call(target, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-failover:k", "v", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-api-failover:k"
               ]) == "v"
             end)
           end)
  end

  @tag :cluster
  test "backend cluster keeps added member and data after full restart" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)
    initial_names = Enum.map(initial_nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add-restart:before", "v1", 0}
             ])

    assert eventually(fn ->
             Enum.all?(initial_names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-add-restart:before"
               ]) == "v1"
             end)
           end)

    start_waraft_backend_peer!(joining_node, unique)
    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert {:ok, {:raft_log_pos, _, _}} =
             :rpc.call(leader, WARaftBackend, :add_member, [0, joining_node.name])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer in membership
           end)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add-restart:after", "v2", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-add-restart:after"
               ]) == "v2"
             end)
           end)

    stop_waraft_backend_peer_cluster!(nodes, unique)
    restarted_leader = start_waraft_backend_peer_cluster!(nodes, unique)

    assert eventually(fn ->
             membership = :rpc.call(restarted_leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer in membership
           end)

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-add-restart:before"
               ]) == "v1" and
                 :rpc.call(node, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-add-restart:after"
                 ]) == "v2"
             end)
           end)

    assert :ok =
             :rpc.call(restarted_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add-restart:after-restart", "v3", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-add-restart:after-restart"
               ]) == "v3"
             end)
           end)
  end

  @tag :cluster
  test "backend add_member catches up large blob-backed values" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)
    large_value = :binary.copy("blob-value", 40_000)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add-blob:large", large_value, 0}
             ])

    assert eventually(fn ->
             Enum.all?(Enum.map(initial_nodes, & &1.name), fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-add-blob:large"
               ]) == large_value
             end)
           end)

    start_waraft_backend_peer!(joining_node, unique)

    assert {:ok, {:raft_log_pos, _, _}} =
             :rpc.call(leader, WARaftBackend, :add_member, [0, joining_node.name])

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-add-blob:large"
             ]) == large_value
           end)
  end

  @tag :cluster
  test "one-node cluster disables blob side-channel while a participant is staged" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 2)
    [leader_node, joining_node] = nodes

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    for left <- Enum.map(nodes, & &1.name),
        right <- Enum.map(nodes, & &1.name),
        left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!([leader_node], unique)
    start_waraft_backend_peer!(joining_node, unique)

    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert {:ok, {:raft_log_pos, _, _}} =
             :rpc.call(leader, WARaftBackend, :add_participant, [0, joining_node.name])

    assert eventually(fn ->
             case :rpc.call(leader, WARaftBackend, :status, [0]) do
               status when is_list(status) ->
                 config = Keyword.get(status, :config, %{})
                 participants = Map.get(config, :participants, Map.get(config, :membership, []))
                 membership = Map.get(config, :membership, [])

                 joining_peer in participants and joining_peer not in membership

               _other ->
                 false
             end
           end)

    large_value = :binary.copy("participant-window-blob", 30_000)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-participant-window:large", large_value, 0}
             ])

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-participant-window:large"
             ]) == large_value
           end)
  end

  @tag :cluster
    end
  end
end
