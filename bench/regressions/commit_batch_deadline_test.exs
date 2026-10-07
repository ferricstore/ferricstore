defmodule Ferricstore.Cluster.CommitBatchDeadlineTest do
  use ExUnit.Case, async: false
  @moduletag :cluster
  alias Ferricstore.Test.ClusterHelper
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.Router

  test "a follower acknowledgement cannot postpone a pending commit to the heartbeat deadline" do
    nodes = ClusterHelper.start_cluster(3, shards: 1, timeout: 30_000)

    try do
      leader = ClusterHelper.find_leader(nodes, 0)
      follower = Enum.find(nodes, &(&1.name != leader)).name
      server = :wa_raft_server.registered_name(:ferricstore_waraft_backend, 1)
      ctx = :erpc.call(leader, FerricStore.Instance, :get, [:default])
      # Drain startup work before changing the batching interval. A command that
      # joins an existing batch correctly inherits that batch's earlier deadline.
      :ok = :erpc.call(leader, Router, :put, [ctx, "batch-deadline-seed", "seed", 0], 10_000)
      await_idle(leader, System.monotonic_time(:millisecond) + 1_000)

      for node <- nodes do
        :ok =
          :erpc.call(node.name, Application, :put_env, [
            :ferricstore_waraft_backend,
            :raft_commit_batch_interval_ms,
            100
          ])

        :ok =
          :erpc.call(node.name, Application, :put_env, [
            :ferricstore_waraft_backend,
            :raft_heartbeat_interval_ms,
            2_000
          ])
      end

      status = :erpc.call(leader, WARaftBackend, :status, [0])
      term = Keyword.fetch!(status, :current_term)
      committed = Keyword.fetch!(status, :commit_index)

      writer =
        Task.async(fn ->
          started = System.monotonic_time(:millisecond)
          result = :erpc.call(leader, Router, :put, [ctx, "batch-deadline", "value", 0], 10_000)
          {result, System.monotonic_time(:millisecond) - started}
        end)

      try do
        deadline = System.monotonic_time(:millisecond) + 1_000
        await_pending(leader, deadline)
        # Replay a valid acknowledgement for the already committed prefix. It
        # must not restart the timer for the newly buffered command.
        :ok =
          :erpc.call(leader, :gen_statem, :cast, [
            server,
            {:rpc, :append_entries_response, term, server, follower,
             {committed, true, committed, committed}}
          ])

        assert {:ok, {:ok, elapsed_ms}} = Task.yield(writer, 400)
        # Check the batching window with the operation's own timing, not a
        # yield after a remote status probe that can itself consume the window.
        assert elapsed_ms >= 90
        assert :erpc.call(leader, Router, :get, [ctx, "batch-deadline"]) == "value"
      after
        Task.shutdown(writer, :brutal_kill)
      end
    after
      ClusterHelper.stop_cluster(nodes)
    end
  end

  defp await_idle(leader, deadline) do
    status = :erpc.call(leader, WARaftBackend, :status, [0])

    if Keyword.fetch!(status, :pending_high) + Keyword.fetch!(status, :pending_low) == 0 do
      :ok
    else
      assert System.monotonic_time(:millisecond) < deadline, "startup commits did not drain"
      Process.sleep(1)
      await_idle(leader, deadline)
    end
  end

  defp await_pending(leader, deadline) do
    status = :erpc.call(leader, WARaftBackend, :status, [0])

    if Keyword.fetch!(status, :pending_high) + Keyword.fetch!(status, :pending_low) > 0 do
      :ok
    else
      assert System.monotonic_time(:millisecond) < deadline, "commit was not buffered"
      Process.sleep(1)
      await_pending(leader, deadline)
    end
  end

  @tag :commit_propagation
  test "a newly committed forwarded write is applied without waiting for the periodic heartbeat" do
    nodes = ClusterHelper.start_cluster(3, shards: 1, timeout: 30_000)

    try do
      leader = ClusterHelper.find_leader(nodes, 0)
      follower = Enum.find(nodes, &(&1.name != leader)).name

      for node <- nodes do
        :ok =
          :erpc.call(node.name, Application, :put_env, [
            :ferricstore_waraft_backend,
            :raft_commit_batch_interval_ms,
            10
          ])

        :ok =
          :erpc.call(node.name, Application, :put_env, [
            :ferricstore_waraft_backend,
            :raft_heartbeat_interval_ms,
            2_000
          ])
      end

      ctx = :erpc.call(follower, FerricStore.Instance, :get, [:default])

      writer =
        Task.async(fn ->
          :erpc.call(follower, Router, :put, [ctx, "commit-propagation", "value", 0], 10_000)
        end)

      try do
        assert Task.yield(writer, 400) == {:ok, :ok}
        assert :erpc.call(follower, Router, :get, [ctx, "commit-propagation"]) == "value"
      after
        Task.shutdown(writer, :brutal_kill)
      end
    after
      ClusterHelper.stop_cluster(nodes)
    end
  end
end
