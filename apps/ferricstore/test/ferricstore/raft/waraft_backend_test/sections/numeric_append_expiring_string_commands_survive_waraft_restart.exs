defmodule Ferricstore.Raft.WARaftBackendTest.Sections.NumericAppendExpiringStringCommandsSurviveWaraftRestart do
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

  test "numeric append and expiring string commands survive WARaft restart", %{root: root} do
    ctx = build_ctx(Path.join(root, "strings-numeric"), shard_count: 2)

    int_key = key_for_shard(ctx, 0, "router:strings-numeric:int")
    float_key = key_for_shard(ctx, 0, "router:strings-numeric:float")
    append_key = key_for_shard(ctx, 1, "router:strings-numeric:append")
    setex_key = key_for_shard(ctx, 1, "router:strings-numeric:setex")
    psetex_key = key_for_shard(ctx, 1, "router:strings-numeric:psetex")

    try do
      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert {:ok, 3} = Ferricstore.Commands.Strings.handle_ast({:incrby, int_key, 3}, ctx)

      float_result =
        Ferricstore.Commands.Strings.handle_ast({:incrbyfloat, float_key, 1.5}, ctx)

      {float_score, ""} = Float.parse(float_result)
      assert_in_delta 1.5, float_score, 0.001

      assert 5 = Ferricstore.Commands.Strings.handle_ast({:append, append_key, "hello"}, ctx)
      assert 11 = Ferricstore.Commands.Strings.handle_ast({:append, append_key, " world"}, ctx)

      assert :ok =
               Ferricstore.Commands.Strings.handle_ast({:setex, setex_key, 60, "seconds"}, ctx)

      assert :ok =
               Ferricstore.Commands.Strings.handle_ast(
                 {:psetex, psetex_key, 60_000, "millis"},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(Path.join(root, "strings-numeric"), shard_count: 2)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert "3" == Router.get(restarted_ctx, int_key)
      assert "1.5" == Router.get(restarted_ctx, float_key)
      assert "hello world" == Router.get(restarted_ctx, append_key)

      assert "hello" ==
               Ferricstore.Commands.Strings.handle_ast(
                 {:getrange, append_key, 0, 4},
                 restarted_ctx
               )

      assert "seconds" == Router.get(restarted_ctx, setex_key)
      assert "millis" == Router.get(restarted_ctx, psetex_key)
      assert Ferricstore.Commands.Expiry.handle_ast({:pttl, setex_key}, restarted_ctx) > 0
      assert Ferricstore.Commands.Expiry.handle_ast({:pttl, psetex_key}, restarted_ctx) > 0
    after
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)
    end
  end

  test "Router WARaft backend keeps shard partitions isolated", %{root: root} do
    ctx = build_ctx(Path.join(root, "multi"), shard_count: 4)

    try do
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      entries =
        for shard_idx <- 0..3 do
          key = key_for_shard(ctx, shard_idx)
          {key, "value:#{shard_idx}"}
        end

      assert [:ok, :ok, :ok, :ok] = Router.batch_quorum_put(ctx, entries)

      for {key, value} <- entries do
        assert value == Router.get(ctx, key)
      end

      for shard_idx <- 0..3 do
        assert {:ok, {:raft_log_pos, index, _term}} = WARaftBackend.storage_position(shard_idx)
        assert index >= 2
      end
    after
      FerricStore.Instance.cleanup(ctx.name)
    end
  end

  @tag :cluster
  test "three peer backend nodes commit through the real FerricStore state machine" do
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
               {:put, "backend-cluster:k", "v", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster:k"]) == "v"
             end)
           end)
  end

  @tag :cluster
  test "backend write through a follower redirects to the WARaft leader" do
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
    follower = Enum.find(names, &(&1 != leader))

    assert :ok =
             :rpc.call(follower, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster:follower-write", "v", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster:follower-write"
               ]) == "v"
             end)
           end)
  end

  @tag :cluster
  test "backend follower redirect submits read-modify-write commands once" do
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
    follower = Enum.find(names, &(&1 != leader))

    assert {:ok, 1} =
             :rpc.call(follower, WARaftBackend, :write, [
               0,
               {:incr, "backend-cluster:follower-incr-once", 1}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster:follower-incr-once"
               ]) == "1"
             end)
           end)
  end

  @tag :cluster
  test "three peer backend cluster replicates multiple FerricStore shards" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)
    instance_name = waraft_backend_peer_instance_name(unique)

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

    leader = start_waraft_backend_peer_cluster!(nodes, unique, shard_count: 2)
    shard1_leader = wait_for_waraft_backend_leader(names, 1)
    follower = Enum.find(names, &(&1 != shard1_leader))
    leader_ctx = :rpc.call(leader, FerricStore.Instance, :get, [instance_name])

    key0 = key_for_shard(leader_ctx, 0, "backend-cluster:multi")
    key1 = key_for_shard(leader_ctx, 1, "backend-cluster:multi")

    assert :ok = :rpc.call(leader, WARaftBackend, :write, [0, {:put, key0, "v0", 0}])
    assert :ok = :rpc.call(follower, WARaftBackend, :write, [1, {:put, key1, "v1", 0}])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, key0]) == "v0" and
                 :rpc.call(node, WARaftBackend, :local_get, [1, key1]) == "v1"
             end)
           end)
  end

  @tag :cluster
  test "three peer backend cluster restarts after an acknowledged write" do
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

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-restart:k", "v1", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster-restart:k"]) ==
                 "v1"
             end)
           end)

    stop_waraft_backend_peer_cluster!(nodes, unique)
    restarted_leader = start_waraft_backend_peer_cluster!(nodes, unique)

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster-restart:k"]) ==
                 "v1"
             end)
           end)

    assert :ok =
             :rpc.call(restarted_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-restart:k2", "v2", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster-restart:k2"]) ==
                 "v2"
             end)
           end)
  end

  @tag :cluster
  @tag :shard_kill
  test "single peer no-sync backend replays acknowledged write after OS process kill" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    [node] = nodes = start_waraft_backend_peers(unique, 1)

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

    :rpc.call(node.name, Application, :put_env, [
      :ferricstore,
      :waraft_storage_apply_mode,
      :replay_safe_nosync
    ])

    :rpc.call(node.name, Application, :put_env, [
      :ferricstore,
      :waraft_bitcask_payload_fsync_every,
      :never
    ])

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    assert leader == node.name

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-nosync-kill:k", "v", 0}
             ])

    assert "v" == :rpc.call(leader, WARaftBackend, :local_get, [0, "backend-nosync-kill:k"])

    kill_peer_os_process!(node)

    restarted = restart_waraft_backend_peer!(node)

    on_exit(fn ->
      try do
        :peer.stop(restarted.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted.name)

    :rpc.call(restarted.name, Application, :put_env, [
      :ferricstore,
      :waraft_storage_apply_mode,
      :replay_safe_nosync
    ])

    :rpc.call(restarted.name, Application, :put_env, [
      :ferricstore,
      :waraft_bitcask_payload_fsync_every,
      :never
    ])

    restarted_leader =
      start_waraft_backend_peer_cluster!([restarted], unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    assert restarted_leader == restarted.name

    assert_eventually(
      fn ->
        :rpc.call(restarted.name, WARaftBackend, :local_get, [0, "backend-nosync-kill:k"])
      end,
      "v"
    )
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster preserves acked writes when leader server is killed during load" do
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
    parent = self()

    writer =
      Task.async(fn ->
        for i <- 1..80 do
          key = "backend-cluster-kill:#{i}"
          value = "v#{i}"
          result = :rpc.call(leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    acked_before_kill = wait_for_cluster_kill_acks(5, [])
    kill_waraft_server!(leader, 0)

    _ = Task.yield(writer, 10_000) || Task.shutdown(writer, :brutal_kill)
    acked = drain_cluster_kill_results(acked_before_kill)

    assert length(acked) >= 5

    recovered_leader = wait_for_waraft_backend_leader(names, 0, 200)

    assert :ok =
             :rpc.call(recovered_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-kill:after", "after", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-kill:after"
               ]) == "after"
             end)
           end)

    for {key, value} <- acked do
      assert eventually(fn ->
               Enum.all?(names, fn node ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
    end
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster catches up follower node crash during active write load" do
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

    crashed = Enum.find(nodes, &(&1.name != leader))
    live_names = names -- [crashed.name]
    parent = self()

    writer =
      Task.async(fn ->
        for i <- 1..80 do
          key = "backend-cluster-node-crash:#{i}"
          value = "v#{i}"
          result = :rpc.call(leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    acked_before_crash = wait_for_cluster_kill_acks(5, [])
    :peer.stop(crashed.peer)

    _ = Task.yield(writer, 10_000) || Task.shutdown(writer, :brutal_kill)
    acked = drain_cluster_kill_results(acked_before_crash)

    assert length(acked) >= 5

    assert eventually(fn ->
             Enum.all?(acked, fn {key, value} ->
               Enum.all?(live_names, fn node ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)

    restarted = restart_waraft_backend_peer!(crashed)

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
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-node-crash:after", "after", 0}
             ])

    assert eventually(fn ->
             :rpc.call(restarted.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-node-crash:after"
             ]) == "after"
           end)

    for {key, value} <- acked do
      assert eventually(fn ->
               :rpc.call(restarted.name, WARaftBackend, :local_get, [0, key]) == value
             end)
    end
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster re-elects after leader node crash during active write load" do
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

    crashed = Enum.find(nodes, &(&1.name == leader))
    live_names = names -- [leader]
    parent = self()

    writer =
      Task.async(fn ->
        for i <- 1..80 do
          key = "backend-cluster-leader-node-crash:#{i}"
          value = "v#{i}"
          result = :rpc.call(leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    acked_before_crash = wait_for_cluster_kill_acks(5, [])
    :peer.stop(crashed.peer)

    _ = Task.yield(writer, 10_000) || Task.shutdown(writer, :brutal_kill)
    acked = drain_cluster_kill_results(acked_before_crash)

    assert length(acked) >= 5

    recovered_leader = wait_for_waraft_backend_leader(live_names, 0, 200)

    assert :ok =
             :rpc.call(recovered_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-leader-node-crash:after", "after", 0}
             ])

    assert eventually(fn ->
             Enum.all?(live_names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-leader-node-crash:after"
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

    restarted = restart_waraft_backend_peer!(crashed)

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
               {:put, "backend-cluster-leader-node-crash:after-restart", "after-restart", 0}
             ])

    assert eventually(fn ->
             :rpc.call(restarted.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-leader-node-crash:after-restart"
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
    end
  end
end
