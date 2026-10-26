defmodule Ferricstore.ZzProbe.GhostClusterProbeTest do
  use ExUnit.Case, async: false
  @moduletag :cluster
  @moduletag timeout: 600_000

  alias Ferricstore.Test.ClusterHelper

  test "re-create after retention cleanup with one node's mirror behind" do
    nodes = ClusterHelper.start_cluster(3, shards: 1)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    [first, _second, third] = nodes
    run = fn node, m, f, a -> ClusterHelper.run(node.name, m, f, a) end

    for node <- nodes, do: run.(node, Ferricstore.Flow.Admission, :clear_create_pause, [])
    :ok = run.(first, FerricStore, :flow_create, ["g1", [type: "gc", payload: "in"]])
    {:ok, [c]} = run.(first, FerricStore, :flow_claim_due, ["gc", [worker: "w", limit: 1]])

    :ok =
      run.(first, FerricStore, :flow_complete, [
        c.id,
        c.lease_token,
        [fencing_token: c.fencing_token, result: "ok"]
      ])

    {:ok, rec} = run.(first, FerricStore, :flow_get, ["g1"])

    flush = fn node ->
      run.(node, Ferricstore.Flow.LMDBWriter, :flush_all, [:default, 1, 60_000])
    end

    Process.sleep(1_000)
    for node <- nodes, do: IO.inspect({node.name, flush.(node)}, label: "flush")

    writer = run.(third, Process, :whereis, [:"Ferricstore.Flow.LMDBWriter.0"])
    IO.inspect(run.(third, :sys, :suspend, [writer]), label: "sys.suspend third writer")

    cleanup =
      run.(first, FerricStore, :flow_retention_cleanup, [
        [limit: 10, now_ms: rec.terminal_retention_until_ms + 1]
      ])

    IO.inspect(cleanup, label: "cleanup")
    [n1, n2, _] = nodes
    IO.inspect({flush.(n1), flush.(n2)}, label: "flush first two")
    for node <- nodes, do: run.(node, Ferricstore.Flow.Admission, :clear_create_pause, [])

    IO.puts(
      "recreate: " <>
        inspect(run.(first, FerricStore, :flow_create, ["g1", [type: "gc", payload: "again"]]))
    )

    Process.sleep(1_000)

    for node <- nodes do
      view =
        case run.(node, FerricStore, :flow_get, ["g1"]) do
          {:ok, nil} -> nil
          {:ok, r} -> {r.state, r.version}
          other -> other
        end

      IO.puts("BEFORE RESUME #{node.name}: #{inspect(view)}")
    end

    IO.inspect(run.(third, :sys, :resume, [writer]), label: "sys.resume third writer")
    Process.sleep(1_000)
    for node <- nodes, do: flush.(node)

    for node <- nodes do
      view =
        case run.(node, FerricStore, :flow_get, ["g1"]) do
          {:ok, nil} -> nil
          {:ok, r} -> {r.state, r.version, r.created_at_ms}
          other -> other
        end

      IO.puts("VIEW #{node.name}: #{inspect(view)}")
    end
  end
end
