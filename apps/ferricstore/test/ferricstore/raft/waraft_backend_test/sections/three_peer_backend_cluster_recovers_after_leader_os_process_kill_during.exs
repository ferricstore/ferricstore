defmodule Ferricstore.Raft.WARaftBackendTest.Sections.ThreePeerBackendClusterRecoversAfterLeaderOsProcessKillDuring do
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

  test "three peer backend cluster recovers after leader OS process kill during active write load" do
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

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    killed = Enum.find(nodes, &(&1.name == leader))
    live_names = names -- [leader]
    parent = self()

    writer =
      Task.async(fn ->
        for i <- 1..80 do
          key = "backend-cluster-leader-kill9:#{i}"
          value = "v#{i}"
          result = :rpc.call(leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    acked_before_kill = wait_for_cluster_kill_acks(5, [])
    kill_peer_os_process!(killed)

    _ = Task.yield(writer, 10_000) || Task.shutdown(writer, :brutal_kill)
    acked = drain_cluster_kill_results(acked_before_kill)

    assert length(acked) >= 5

    recovered_leader = wait_for_waraft_backend_leader(live_names, 0, 200)

    assert :ok =
             :rpc.call(recovered_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-leader-kill9:after", "after", 0}
             ])

    assert eventually(fn ->
             Enum.all?(live_names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-leader-kill9:after"
               ]) == "after"
             end)
           end)

    assert eventually(fn ->
             Enum.all?(acked, fn {key, value} ->
               Enum.all?(live_names, fn node ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)

    restarted = restart_waraft_backend_peer!(killed)

    on_exit(fn ->
      try do
        :peer.stop(restarted.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted.name)

    for live <- live_names do
      :rpc.call(restarted.name, Node, :connect, [live])
      :rpc.call(live, Node, :connect, [restarted.name])
    end

    start_waraft_backend_peer!(restarted, unique,
      election_timeout_ms: 200,
      election_timeout_ms_max: 300
    )

    assert :ok =
             :rpc.call(recovered_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-leader-kill9:after-restart", "after-restart", 0}
             ])

    assert eventually(fn ->
             :rpc.call(restarted.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-leader-kill9:after-restart"
             ]) == "after-restart"
           end)

    for {key, value} <- acked do
      assert eventually(fn ->
               :rpc.call(restarted.name, WARaftBackend, :local_get, [0, key]) == value
             end)
    end
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster catches up follower OS process kill during active write load" do
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

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    killed = Enum.find(nodes, &(&1.name != leader))
    live_names = names -- [killed.name]
    parent = self()

    writer =
      Task.async(fn ->
        for i <- 1..80 do
          key = "backend-cluster-follower-kill9:#{i}"
          value = "v#{i}"
          result = :rpc.call(leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    acked_before_kill = wait_for_cluster_kill_acks(5, [])
    kill_peer_os_process!(killed)

    _ = Task.yield(writer, 10_000) || Task.shutdown(writer, :brutal_kill)
    acked = drain_cluster_kill_results(acked_before_kill)

    assert length(acked) >= 5

    assert eventually(fn ->
             Enum.all?(acked, fn {key, value} ->
               Enum.all?(live_names, fn node ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)

    restarted = restart_waraft_backend_peer!(killed)

    on_exit(fn ->
      try do
        :peer.stop(restarted.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted.name)

    for live <- live_names do
      :rpc.call(restarted.name, Node, :connect, [live])
      :rpc.call(live, Node, :connect, [restarted.name])
    end

    start_waraft_backend_peer!(restarted, unique,
      election_timeout_ms: 200,
      election_timeout_ms_max: 300
    )

    live_leader = wait_for_waraft_backend_leader(live_names, 0, 200)

    assert :ok =
             :rpc.call(live_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-follower-kill9:after-restart", "after-restart", 0}
             ])

    assert eventually(fn ->
             :rpc.call(restarted.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-follower-kill9:after-restart"
             ]) == "after-restart"
           end)

    for {key, value} <- acked do
      assert eventually(fn ->
               :rpc.call(restarted.name, WARaftBackend, :local_get, [0, key]) == value
             end)
    end
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster rejects writes without quorum after two OS kills and recovers" do
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

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-two-kill9:before", "before", 0}
             ])

    [first_killed, second_killed] =
      nodes
      |> Enum.reject(&(&1.name == leader))
      |> Enum.take(2)

    kill_peer_os_process!(first_killed)
    kill_peer_os_process!(second_killed)

    assert {:error, :no_quorum} =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-two-kill9:no-quorum", "no-quorum", 0}
             ])

    restarted_first = restart_waraft_backend_peer!(first_killed)

    on_exit(fn ->
      try do
        :peer.stop(restarted_first.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted_first.name)
    :rpc.call(restarted_first.name, Node, :connect, [leader])
    :rpc.call(leader, Node, :connect, [restarted_first.name])

    start_waraft_backend_peer!(restarted_first, unique,
      election_timeout_ms: 200,
      election_timeout_ms_max: 300
    )

    quorum_names = [leader, restarted_first.name]
    quorum_leader = wait_for_waraft_backend_leader(quorum_names, 0, 200)

    assert :ok =
             :rpc.call(quorum_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-two-kill9:after-quorum", "after-quorum", 0}
             ])

    restarted_second = restart_waraft_backend_peer!(second_killed)

    on_exit(fn ->
      try do
        :peer.stop(restarted_second.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted_second.name)

    for live <- quorum_names do
      :rpc.call(restarted_second.name, Node, :connect, [live])
      :rpc.call(live, Node, :connect, [restarted_second.name])
    end

    start_waraft_backend_peer!(restarted_second, unique,
      election_timeout_ms: 200,
      election_timeout_ms_max: 300
    )

    assert eventually(fn ->
             Enum.all?([leader, restarted_first.name, restarted_second.name], fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-two-kill9:before"
               ]) == "before" and
                 :rpc.call(node, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-two-kill9:after-quorum"
                 ]) == "after-quorum" and
                 :rpc.call(node, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-two-kill9:no-quorum"
                 ]) == nil
             end)
           end)
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster catches up isolated follower after network partition heal" do
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

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    isolated = Enum.find(nodes, &(&1.name != leader))
    majority_names = names -- [isolated.name]
    real_cookie = partition_waraft_peer!(isolated, nodes)

    assert {:error, _reason} =
             :rpc.call(isolated.name, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-partition:minority", "minority", 0}
             ])

    majority_leader = wait_for_waraft_backend_leader(majority_names, 0, 200)

    for i <- 1..20 do
      assert :ok =
               :rpc.call(majority_leader, WARaftBackend, :write, [
                 0,
                 {:put, "backend-cluster-partition:#{i}", "v#{i}", 0}
               ])
    end

    assert eventually(fn ->
             Enum.all?(majority_names, fn node ->
               Enum.all?(1..20, fn i ->
                 :rpc.call(node, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-partition:#{i}"
                 ]) == "v#{i}"
               end)
             end)
           end)

    heal_waraft_peer_partition!(isolated, nodes, real_cookie)

    assert eventually(fn ->
             Enum.all?(1..20, fn i ->
               :rpc.call(isolated.name, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-partition:#{i}"
               ]) == "v#{i}"
             end)
           end)
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster rejects isolated leader writes and catches up after heal" do
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

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    isolated = Enum.find(nodes, &(&1.name == leader))
    majority_names = names -- [leader]
    real_cookie = partition_waraft_peer!(isolated, nodes)

    assert {:error, _reason} =
             :rpc.call(isolated.name, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-leader-partition:minority", "minority", 0}
             ])

    majority_leader = wait_for_waraft_backend_leader(majority_names, 0, 200)

    for i <- 1..20 do
      assert :ok =
               :rpc.call(majority_leader, WARaftBackend, :write, [
                 0,
                 {:put, "backend-cluster-leader-partition:#{i}", "v#{i}", 0}
               ])
    end

    assert eventually(fn ->
             Enum.all?(majority_names, fn node ->
               Enum.all?(1..20, fn i ->
                 :rpc.call(node, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-leader-partition:#{i}"
                 ]) == "v#{i}"
               end)
             end)
           end)

    heal_waraft_peer_partition!(isolated, nodes, real_cookie)

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               Enum.all?(1..20, fn i ->
                 :rpc.call(node, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-leader-partition:#{i}"
                 ]) == "v#{i}"
               end)
             end)
           end)

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-leader-partition:minority"
               ]) == nil
             end)
           end)
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster survives repeated partition and heal cycles" do
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

    _leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    expected =
      Enum.reduce(1..3, [], fn cycle, acc ->
        current_leader = wait_for_waraft_backend_leader(names, 0, 200)

        isolated =
          if rem(cycle, 2) == 1 do
            Enum.find(nodes, &(&1.name == current_leader))
          else
            Enum.find(nodes, &(&1.name != current_leader))
          end

        majority_names = names -- [isolated.name]
        real_cookie = partition_waraft_peer!(isolated, nodes)

        assert {:error, _reason} =
                 :rpc.call(isolated.name, WARaftBackend, :write, [
                   0,
                   {:put, "backend-cluster-flap:minority:#{cycle}", "minority", 0}
                 ])

        majority_leader = wait_for_waraft_backend_leader(majority_names, 0, 200)

        cycle_expected =
          for i <- 1..5 do
            key = "backend-cluster-flap:#{cycle}:#{i}"
            value = "v#{cycle}:#{i}"

            assert :ok =
                     :rpc.call(majority_leader, WARaftBackend, :write, [
                       0,
                       {:put, key, value, 0}
                     ])

            {key, value}
          end

        assert eventually(fn ->
                 Enum.all?(majority_names, fn node ->
                   Enum.all?(cycle_expected, fn {key, value} ->
                     :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
                   end)
                 end)
               end)

        heal_waraft_peer_partition!(isolated, nodes, real_cookie)

        assert eventually(fn ->
                 Enum.all?(names, fn node ->
                   Enum.all?(cycle_expected, fn {key, value} ->
                     :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
                   end)
                 end)
               end)

        assert eventually(fn ->
                 Enum.all?(names, fn node ->
                   :rpc.call(node, WARaftBackend, :local_get, [
                     0,
                     "backend-cluster-flap:minority:#{cycle}"
                   ]) == nil
                 end)
               end)

        cycle_expected ++ acc
      end)

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               Enum.all?(expected, fn {key, value} ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster preserves acked writes across partition heal and follower OS kill" do
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

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    isolated = Enum.find(nodes, &(&1.name != leader))
    majority_names = names -- [isolated.name]
    real_cookie = partition_waraft_peer!(isolated, nodes)
    majority_leader = wait_for_waraft_backend_leader(majority_names, 0, 200)
    parent = self()

    partition_writer =
      Task.async(fn ->
        for i <- 1..40 do
          key = "backend-cluster-mixed-chaos:partition:#{i}"
          value = "pv#{i}"
          result = :rpc.call(majority_leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    partition_acked_before_heal = wait_for_cluster_kill_acks(5, [])
    heal_waraft_peer_partition!(isolated, nodes, real_cookie)

    _ = Task.yield(partition_writer, 10_000) || Task.shutdown(partition_writer, :brutal_kill)
    partition_acked = drain_cluster_kill_results(partition_acked_before_heal)

    assert length(partition_acked) >= 5

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               Enum.all?(partition_acked, fn {key, value} ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)

    live_leader = wait_for_waraft_backend_leader(names, 0, 200)
    killed = Enum.find(nodes, &(&1.name != live_leader))
    live_names = names -- [killed.name]

    kill_writer =
      Task.async(fn ->
        for i <- 1..40 do
          key = "backend-cluster-mixed-chaos:kill:#{i}"
          value = "kv#{i}"
          result = :rpc.call(live_leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    kill_acked_before_os_kill = wait_for_cluster_kill_acks(5, [])
    kill_peer_os_process!(killed)

    _ = Task.yield(kill_writer, 10_000) || Task.shutdown(kill_writer, :brutal_kill)
    kill_acked = drain_cluster_kill_results(kill_acked_before_os_kill)

    assert length(kill_acked) >= 5

    assert eventually(fn ->
             Enum.all?(live_names, fn node ->
               Enum.all?(kill_acked, fn {key, value} ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)

    restarted = restart_waraft_backend_peer!(killed)

    on_exit(fn ->
      try do
        :peer.stop(restarted.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted.name)

    for live <- live_names do
      :rpc.call(restarted.name, Node, :connect, [live])
      :rpc.call(live, Node, :connect, [restarted.name])
    end

    start_waraft_backend_peer!(restarted, unique,
      election_timeout_ms: 200,
      election_timeout_ms_max: 300
    )

    assert eventually(fn ->
             Enum.all?(partition_acked ++ kill_acked, fn {key, value} ->
               :rpc.call(restarted.name, WARaftBackend, :local_get, [0, key]) == value
             end)
           end)
  end

  @tag :cluster
    end
  end
end
