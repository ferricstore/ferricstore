defmodule Ferricstore.ZzProbe.SharedValueProbeTest do
  use ExUnit.Case, async: false
  @moduletag timeout: :infinity
  @moduletag :global_state
  alias Ferricstore.Test.ShardHelpers

  test "shared value used as a payload after the flow is cleaned" do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    ctx = FerricStore.Instance.get(:default)

    {:ok, %{ref: ref}} =
      FerricStore.flow_value_put(%{"big" => String.duplicate("x", 2000)}, partition_key: "p1")

    :ok = FerricStore.flow_create("sv-1", type: "sv", payload_ref: ref, partition_key: "p1")
    {:ok, [c]} = FerricStore.flow_claim_due("sv", worker: "w", limit: 1, partition_key: "p1")

    :ok =
      FerricStore.flow_complete(c.id, c.lease_token,
        fencing_token: c.fencing_token,
        result: "ok",
        partition_key: "p1"
      )

    {:ok, rec} = FerricStore.flow_get("sv-1", partition_key: "p1")

    IO.inspect(Map.take(rec, [:payload_ref, :result_ref, :terminal_retention_until_ms]),
      label: "record"
    )

    {:ok, r} =
      FerricStore.flow_retention_cleanup(limit: 100, now_ms: rec.terminal_retention_until_ms + 1)

    IO.inspect(r, label: "cleanup")

    IO.inspect(
      FerricStore.flow_get("sv-1", partition_key: "p1")
      |> then(fn
        {:ok, r} -> r && r.state
        o -> o
      end),
      label: "flow after (no flush)"
    )

    :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 60_000)
    for i <- 0..(ctx.shard_count - 1), do: Ferricstore.Flow.HistoryProjector.flush(ctx, i, 60_000)
    :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 60_000)

    IO.inspect(
      FerricStore.flow_get("sv-1", partition_key: "p1")
      |> then(fn
        {:ok, r} -> r && r.state
        o -> o
      end),
      label: "flow after flush"
    )

    Process.sleep(2000)

    IO.inspect(
      FerricStore.flow_get("sv-1", partition_key: "p1")
      |> then(fn
        {:ok, r} -> r && r.state
        o -> o
      end),
      label: "flow after 2s"
    )

    IO.inspect(FerricStore.flow_value_mget([ref], partition_key: "p1"),
      label: "shared value after",
      limit: 5,
      printable_limit: 60
    )

    IO.inspect(Ferricstore.Store.Router.get(ctx, ref) |> then(&(&1 && byte_size(&1))),
      label: "raw shared value bytes"
    )
  end
end
