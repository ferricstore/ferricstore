defmodule Ferricstore.ZzProbe.GhostProbeTest do
  use ExUnit.Case, async: false
  @moduletag timeout: :infinity
  @moduletag :global_state
  alias Ferricstore.Test.ShardHelpers

  test "reads and writes right after retention cleanup" do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    ctx = FerricStore.Instance.get(:default)

    for round <- 1..5 do
      id = "ghost-#{round}"
      :ok = FerricStore.flow_create(id, type: "gh", payload: "in")
      {:ok, [c]} = FerricStore.flow_claim_due("gh", worker: "w", limit: 1)

      :ok =
        FerricStore.flow_complete(c.id, c.lease_token,
          fencing_token: c.fencing_token,
          result: "ok"
        )

      {:ok, rec} = FerricStore.flow_get(id)
      :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 60_000)

      {:ok, r} =
        FerricStore.flow_retention_cleanup(
          limit: 100,
          now_ms: rec.terminal_retention_until_ms + 1
        )

      get =
        FerricStore.flow_get(id)
        |> then(fn
          {:ok, r} -> r && r.state
          o -> o
        end)

      list =
        FerricStore.flow_list("gh", state: "completed")
        |> then(fn
          {:ok, l} -> length(l)
          o -> o
        end)

      terminals =
        FerricStore.flow_terminals("gh")
        |> then(fn
          {:ok, l} -> length(l)
          o -> inspect(o) |> String.slice(0, 80)
        end)

      recreate = FerricStore.flow_create(id, type: "gh")

      after_recreate =
        FerricStore.flow_get(id)
        |> then(fn
          {:ok, r} -> r && {r.state, r.version}
          o -> o
        end)

      IO.puts(
        "round #{round}: cleaned=#{r.flows} get=#{inspect(get)} list=#{inspect(list)} terminals=#{inspect(terminals)} recreate=#{inspect(recreate)} now=#{inspect(after_recreate)}"
      )

      :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 60_000)

      later =
        FerricStore.flow_get(id)
        |> then(fn
          {:ok, r} -> r && {r.state, r.version}
          o -> o
        end)

      IO.puts("round #{round}: after flush get=#{inspect(later)}")
      {:ok, [c2]} = FerricStore.flow_claim_due("gh", worker: "w", limit: 1)
      :ok = FerricStore.flow_cancel(c2.id, [])
    end
  end
end
