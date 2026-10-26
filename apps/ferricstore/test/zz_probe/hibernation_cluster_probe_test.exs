defmodule Ferricstore.ZzProbe.HibernationClusterProbeTest do
  use ExUnit.Case, async: false
  @moduletag :cluster
  @moduletag timeout: 600_000

  alias Ferricstore.Test.ClusterHelper

  test "claims of hibernated flows agree when one node's writer is behind" do
    nodes = ClusterHelper.start_cluster(3, shards: 1)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    [first, second, third] = nodes
    run = fn node, m, f, a -> ClusterHelper.run(node.name, m, f, a) end

    flush = fn node ->
      run.(node, Ferricstore.Flow.LMDBWriter, :flush_all, [:default, 1, 60_000])
    end

    writer = run.(third, Process, :whereis, [:"Ferricstore.Flow.LMDBWriter.0"])
    :ok = run.(third, :sys, :suspend, [writer])
    now = System.system_time(:millisecond)
    due = now + 10 * 60_000

    for node <- nodes, do: run.(node, Ferricstore.Flow.Admission, :clear_create_pause, [])

    for i <- 1..20 do
      for node <- nodes, do: run.(node, Ferricstore.Flow.Admission, :clear_create_pause, [])

      :ok =
        run.(first, FerricStore, :flow_create, [
          "h#{i}",
          [type: "hib", run_at_ms: due + i, now_ms: now]
        ])
    end

    IO.inspect({flush.(first), flush.(second)}, label: "flush 1,2")
    Process.sleep(2_000)
    IO.inspect({flush.(first), flush.(second)}, label: "flush 1,2 again")

    claim =
      run.(first, FerricStore, :flow_claim_due, [
        "hib",
        [worker: "w", limit: 5, lease_ms: 600_000, now_ms: due + 100]
      ])

    IO.inspect(
      claim
      |> then(fn
        {:ok, c} -> Enum.map(c, & &1.id) |> Enum.sort()
        o -> o
      end),
      label: "claimed on leader"
    )

    :ok = run.(third, :sys, :resume, [writer])
    for node <- nodes, do: flush.(node)

    for node <- nodes do
      states =
        for i <- 1..20 do
          case run.(node, FerricStore, :flow_get, ["h#{i}"]) do
            {:ok, %{state: state}} -> {i, state}
            other -> {i, other}
          end
        end

      running = for {i, "running"} <- states, do: i

      IO.puts(
        "VIEW #{node.name}: running=#{inspect(running)} other=#{inspect(Enum.reject(states, fn {_, s} -> s in ["running", "queued"] end))}"
      )
    end
  end
end
