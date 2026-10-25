defmodule Ferricstore.Cluster.FlowRetentionMirrorLagClusterTest do
  @moduledoc """
  Retention cleanup deletes a completed Flow in apply; each node's LMDB writer
  removes the Flow's mirror copy later, at its own pace. Apply read that copy
  when deciding whether a Flow exists, so a create of the same id was refused
  on a node whose writer was behind and applied on the others: the replicas
  diverged for good. A delete tombstone now hides the copy until it is gone.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 300_000

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
    {:ok, nodes: nodes}
  end

  test "a re-create after cleanup applies on every node, with one writer behind", %{
    nodes: [first, second, third] = nodes
  } do
    run = fn node, m, f, a -> ClusterHelper.run(node.name, m, f, a) end
    flush = fn node -> run.(node, Ferricstore.Flow.LMDBWriter, :flush_all, [:default, 1, 60_000]) end
    clear_pause = fn -> for node <- nodes, do: run.(node, Ferricstore.Flow.Admission, :clear_create_pause, []) end

    clear_pause.()
    assert :ok = run.(first, FerricStore, :flow_create, ["lag-1", [type: "lag", payload: "in"]])
    assert {:ok, [c]} = run.(first, FerricStore, :flow_claim_due, ["lag", [worker: "w", limit: 1]])

    assert :ok =
             run.(first, FerricStore, :flow_complete, [
               c.id,
               c.lease_token,
               [fencing_token: c.fencing_token, result: "ok"]
             ])

    assert {:ok, record} = run.(first, FerricStore, :flow_get, ["lag-1"])
    for node <- nodes, do: assert(:ok = flush.(node))

    # The third node's writer is busy: its queued ops wait, none is lost.
    writer = run.(third, Process, :whereis, [:"Ferricstore.Flow.LMDBWriter.0"])
    :ok = run.(third, :sys, :suspend, [writer])

    try do
      assert {:ok, %{flows: 1}} =
               run.(first, FerricStore, :flow_retention_cleanup, [
                 [limit: 10, now_ms: record.terminal_retention_until_ms + 1]
               ])

      assert :ok = flush.(first)
      assert :ok = flush.(second)
      clear_pause.()
      assert :ok = run.(first, FerricStore, :flow_create, ["lag-1", [type: "lag", payload: "again"]])
    after
      run.(third, :sys, :resume, [writer])
    end

    for node <- nodes, do: assert(:ok = flush.(node))

    views =
      for node <- nodes do
        case run.(node, FerricStore, :flow_get, ["lag-1"]) do
          {:ok, %{state: state, version: version}} -> {state, version}
          other -> other
        end
      end

    assert views == List.duplicate({"queued", 1}, 3), inspect(views)
  end
end
