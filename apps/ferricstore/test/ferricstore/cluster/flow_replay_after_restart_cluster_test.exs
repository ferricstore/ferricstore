defmodule Ferricstore.Cluster.FlowReplayAfterRestartClusterTest do
  @moduledoc """
  A node that stops before its history projector wrote a Flow's history keeps
  its replay cursor behind those writes, and WARaft applies them again after
  the restart. The node's Flow LMDB mirror already holds what they wrote, so
  the replay must not take those rows for existing Flows: it refused the
  create and transitions, wrote no history and left the state cold-only.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 300_000

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Test.ClusterHelper

  @partition "replay-pk"

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  test "a restarted node replays Flow writes its LMDB mirror already holds" do
    nodes = ClusterHelper.start_cluster(3, shards: 1)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    [writer, _other, victim] = nodes
    ids = for i <- 1..5, do: "replay-flow-#{i}"

    # The victim applies the writes but never projects their history.
    projector = run(victim, Ferricstore.Flow.HistoryProjector, :name, [ctx(victim), 0])
    :ok = run(victim, :sys, :suspend, [projector])

    writer_ctx = ctx(writer)

    for id <- ids do
      assert :ok =
               run(writer, Ferricstore.Flow, :create, [
                 writer_ctx,
                 id,
                 [type: "replay", partition_key: @partition]
               ])

      assert :ok =
               run(writer, Ferricstore.Flow, :transition, [
                 writer_ctx,
                 id,
                 "queued",
                 "reviewed",
                 [partition_key: @partition, fencing_token: 0]
               ])
    end

    target = last_applied(writer)
    assert eventually(fn -> last_applied(victim) >= target end)
    assert :ok = run(victim, Ferricstore.Flow.LMDBWriter, :flush, [ctx(victim).name, 0])

    {stopped, _remaining} = ClusterHelper.kill_node(nodes, victim)
    restarted = ClusterHelper.restart_node(nodes, stopped, shards: 1)
    assert eventually(fn -> last_applied(restarted) >= target end)
    restarted_ctx = ctx(restarted)

    for id <- ids do
      assert eventually(fn -> length(history(restarted, id)) == 2 end),
             "#{id} history on the restarted node: #{inspect(history(restarted, id))}"

      # The replayed create wrote the state again, not only the mirror.
      assert [_row] =
               run(restarted, :ets, :lookup, [
                 elem(restarted_ctx.keydir_refs, 0),
                 Keys.state_key(id, @partition)
               ])
    end
  end

  defp run(node, mod, fun, args), do: ClusterHelper.run(node.name, mod, fun, args)
  defp ctx(node), do: run(node, FerricStore.Instance, :get, [:default])

  defp last_applied(node) do
    case run(node, Ferricstore.Raft.WARaftBackend, :storage_status, [0]) do
      {:ok, status} -> Keyword.fetch!(status, :last_applied)
      _unavailable -> 0
    end
  end

  defp history(node, id) do
    case run(node, Ferricstore.Flow, :history, [ctx(node), id, [partition_key: @partition]]) do
      {:ok, events} -> events
      _other -> []
    end
  end

  defp eventually(fun, attempts \\ 200)
  defp eventually(_fun, 0), do: false

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(50)
      eventually(fun, attempts - 1)
    end
  end
end
