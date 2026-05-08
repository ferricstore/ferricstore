defmodule Ferricstore.Cluster.StandalonePromotionIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag :standalone_promotion

  alias Ferricstore.Test.ClusterHelper

  @shards 1

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  setup do
    cleanup_orphan_peers()
    :ok
  end

  @tag timeout: 120_000
  test "CLUSTER.ENABLE promotes stable manual peer end-to-end and survives restart" do
    node = ClusterHelper.start_node(shards: @shards, raft_mode: :manual)
    ctx = rpc!(node, FerricStore.Instance, :get, [:default])
    on_exit(fn -> force_stop_node(node, ctx.data_dir) end)

    assert true = rpc!(node, Node, :alive?, [])
    assert :standalone = rpc!(node, Ferricstore.ReplicationMode, :current, [])
    assert :undefined = rpc!(node, :ra_system, :fetch, [Ferricstore.Raft.Cluster.system_name()])

    assert :ok = put_key(node, "enable:fresh:before", "before")

    assert :ok =
             rpc!(
               node,
               Ferricstore.Cluster.Manager,
               :enable_cluster,
               [[followup: false]],
               120_000
             )

    eventually(fn ->
      assert :raft = rpc!(node, Ferricstore.ReplicationMode, :current, [])
      assert "before" = get_key(node, "enable:fresh:before")

      assert {:ok, %{replication_mode: :raft, promotion_epoch: epoch, barrier_indices: barriers}} =
               rpc!(node, Ferricstore.ReplicationMode, :read, [ctx.data_dir])

      assert is_integer(epoch)
      assert is_map(barriers)
      assert map_size(barriers) == @shards
    end)

    assert :ok = ClusterHelper.wait_for_node_leaders(node, @shards, timeout: 30_000)
    assert :ok = put_key(node, "enable:fresh:after", "after")
    assert "after" = get_key(node, "enable:fresh:after")

    assert :ok = rpc!(node, Application, :stop, [:ferricstore], 30_000)
    assert {:ok, _apps} = rpc!(node, Application, :ensure_all_started, [:ferricstore], 60_000)
    assert :ok = ClusterHelper.wait_for_node_leaders(node, @shards, timeout: 30_000)
    assert :raft = rpc!(node, Ferricstore.ReplicationMode, :current, [])
    assert "before" = get_key(node, "enable:fresh:before")
    assert "after" = get_key(node, "enable:fresh:after")
  end

  @tag timeout: 120_000
  test "manual peer killed before standalone fsync does not recover unacked write" do
    node = ClusterHelper.start_node(shards: @shards, raft_mode: :manual)
    ctx = rpc!(node, FerricStore.Instance, :get, [:default])
    data_dir = ctx.data_dir
    key = "standalone:peer-kill-before-fsync"

    on_exit(fn ->
      try do
        :erpc.call(node, :erlang, :halt, [0], 2_000)
      catch
        _, _ -> :ok
      end

      File.rm_rf(data_dir)
    end)

    :ok =
      rpc!(node, Application, :put_env, [
        :ferricstore,
        :standalone_fsync_max_delay_ms,
        30_000
      ])

    previous_trap_exit = Process.flag(:trap_exit, true)

    try do
      write = Task.async(fn -> put_key(node, key, "value") end)

      eventually(fn ->
        assert %{batch_count: 1, inflight_count: 0} = standalone_commit_debug(node, ctx, key)
      end)

      refute Task.yield(write, 100)

      try do
        :erpc.call(node, :erlang, :halt, [0], 2_000)
      catch
        _, _ -> :ok
      end

      Node.disconnect(node)

      assert match?({:exit, _}, Task.yield(write, 10_000) || Task.shutdown(write, :brutal_kill))
    after
      Process.flag(:trap_exit, previous_trap_exit)
    end

    restarted = ClusterHelper.start_node(shards: @shards, raft_mode: :manual, data_dir: data_dir)

    try do
      assert :standalone = rpc!(restarted, Ferricstore.ReplicationMode, :current, [])
      assert nil == get_key(restarted, key)
      assert :ok = put_key(restarted, key, "after-restart")
      assert "after-restart" = get_key(restarted, key)
    after
      force_stop_node(restarted, data_dir)
    end
  end

  @tag timeout: 120_000
  test "manual standalone peer restarts from enabling marker and recovers promotion" do
    node = ClusterHelper.start_node(shards: @shards, raft_mode: :manual)
    ctx = rpc!(node, FerricStore.Instance, :get, [:default])
    on_exit(fn -> force_stop_node(node, ctx.data_dir) end)

    assert :standalone = rpc!(node, Ferricstore.ReplicationMode, :current, [])
    assert :undefined = rpc!(node, :ra_system, :fetch, [Ferricstore.Raft.Cluster.system_name()])

    assert :ok = put_key(node, "promotion:survives", "before")
    assert "before" = get_key(node, "promotion:survives")

    assert :ok = rpc!(node, Application, :stop, [:ferricstore], 30_000)

    assert :ok =
             rpc!(node, Ferricstore.ReplicationMode, :mark_enabling!, [ctx.data_dir, @shards, 99])

    assert {:ok, _apps} = rpc!(node, Application, :ensure_all_started, [:ferricstore], 60_000)

    eventually(fn ->
      assert :raft = rpc!(node, Ferricstore.ReplicationMode, :current, [])
      assert "before" = get_key(node, "promotion:survives")

      status =
        rpc!(
          node,
          Ferricstore.Commands.Cluster,
          :handle,
          ["CLUSTER.ENABLE", ["STATUS"], %{}]
        )

      assert status =~ "marker_mode: raft"
      assert status =~ "promotion_epoch: 99"
    end)

    assert :ok = ClusterHelper.wait_for_node_leaders(node, @shards, timeout: 30_000)
  end

  @tag timeout: 120_000
  test "CLUSTER.JOIN REPLACE cleans real target data and writes target marker" do
    source = ClusterHelper.start_node(shards: @shards)
    target = ClusterHelper.start_node(shards: @shards)
    source_ctx = rpc!(source, FerricStore.Instance, :get, [:default])
    target_ctx = rpc!(target, FerricStore.Instance, :get, [:default])

    on_exit(fn ->
      force_stop_node(target, target_ctx.data_dir)
      force_stop_node(source, source_ctx.data_dir)
    end)

    assert :ok = ClusterHelper.wait_for_node_leaders(source, @shards, timeout: 30_000)
    assert :ok = ClusterHelper.wait_for_node_leaders(target, @shards, timeout: 30_000)

    assert :ok = put_key(source, "replace:source", "source-value")
    assert :ok = put_key(target, "replace:foreign", "foreign-value")

    assert "source-value" = get_key(source, "replace:source")
    assert "foreign-value" = get_key(target, "replace:foreign")

    assert true = rpc!(source, Node, :connect, [target])
    assert true = rpc!(target, Node, :connect, [source])

    assert :ok =
             rpc!(
               source,
               Ferricstore.Cluster.Manager,
               :add_node,
               [target, :voter, [replace: true]],
               120_000
             )

    {:ok, source_marker} = rpc!(source, Ferricstore.ReplicationMode, :read, [source_ctx.data_dir])

    eventually(fn ->
      assert "source-value" = get_key(target, "replace:source")
      assert nil == get_key(target, "replace:foreign")

      assert {:ok, %{cluster_id: cluster_id, replication_mode: :raft}} =
               rpc!(target, Ferricstore.ReplicationMode, :read, [target_ctx.data_dir])

      assert cluster_id == source_marker.cluster_id
    end)
  end

  defp put_key(node, key, value) do
    ctx = rpc!(node, FerricStore.Instance, :get, [:default])
    rpc!(node, Ferricstore.Store.Router, :put, [ctx, key, value, 0], 30_000)
  end

  defp get_key(node, key) do
    ctx = rpc!(node, FerricStore.Instance, :get, [:default])
    rpc!(node, Ferricstore.Store.Router, :get, [ctx, key], 30_000)
  end

  defp standalone_commit_debug(node, ctx, key) do
    idx = Ferricstore.Store.Router.shard_for(ctx, key)
    shard = elem(ctx.shard_names, idx)
    rpc!(node, GenServer, :call, [shard, :standalone_commit_debug, 1_000], 2_000)
  end

  defp rpc!(node, module, function, args, timeout \\ 10_000) do
    case :erpc.call(node, module, function, args, timeout) do
      {:badrpc, reason} -> flunk("rpc #{inspect(module)}.#{function} failed: #{inspect(reason)}")
      other -> other
    end
  end

  defp eventually(fun, attempts \\ 60, sleep_ms \\ 500) do
    try do
      fun.()
    rescue
      error ->
        if attempts <= 1 do
          reraise error, __STACKTRACE__
        else
          Process.sleep(sleep_ms)
          eventually(fun, attempts - 1, sleep_ms)
        end
    end
  end

  defp force_stop_node(node, data_dir) do
    try do
      :erpc.call(node, :erlang, :halt, [0], 2_000)
    catch
      _, _ -> :ok
    end

    Node.disconnect(node)
    File.rm_rf(data_dir)
    :ok
  end

  defp cleanup_orphan_peers do
    if :ets.whereis(:ferricstore_solo_peers) != :undefined do
      :ets.tab2list(:ferricstore_solo_peers)
      |> Enum.each(fn {name, peer_pid, _dir} ->
        try do
          :peer.stop(peer_pid)
        catch
          _, _ -> :ok
        end

        :ets.delete(:ferricstore_solo_peers, name)
      end)
    end

    Node.list()
    |> Enum.filter(fn n -> n |> Atom.to_string() |> String.contains?("ferric_") end)
    |> Enum.each(fn n ->
      try do
        :erpc.call(n, :erlang, :halt, [0], 2_000)
      catch
        _, _ -> :ok
      end

      Node.disconnect(n)
    end)

    Path.wildcard(Path.join(System.tmp_dir!(), "ferricstore_solo_*")) |> Enum.each(&File.rm_rf/1)
  end
end
