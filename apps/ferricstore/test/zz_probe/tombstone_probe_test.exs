defmodule Ferricstore.ZzProbe.TombstoneProbeTest do
  use ExUnit.Case, async: false
  @moduletag timeout: :infinity
  @moduletag :global_state
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  test "keydir row for a cleaned flow" do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    ctx = FerricStore.Instance.get(:default)
    :ok = FerricStore.flow_create("tb", type: "tb", payload: "in")
    {:ok, [c]} = FerricStore.flow_claim_due("tb", worker: "w", limit: 1)

    :ok =
      FerricStore.flow_complete(c.id, c.lease_token, fencing_token: c.fencing_token, result: "ok")

    {:ok, rec} = FerricStore.flow_get("tb")
    key = Keys.state_key("tb", nil)
    keydir = elem(ctx.keydir_refs, Router.shard_for(ctx, key))

    IO.inspect(:ets.lookup(keydir, key) |> Enum.map(&put_elem(&1, 1, :value)),
      label: "before flush"
    )

    :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 60_000)

    IO.inspect(:ets.lookup(keydir, key) |> Enum.map(&put_elem(&1, 1, :value)),
      label: "after flush (pruned?)"
    )

    {:ok, r} =
      FerricStore.flow_retention_cleanup(limit: 10, now_ms: rec.terminal_retention_until_ms + 1)

    row = :ets.lookup(keydir, key)

    get =
      FerricStore.flow_get("tb")
      |> then(fn
        {:ok, x} -> x && x.state
        o -> o
      end)

    IO.inspect({r.flows, row, get}, label: "right after cleanup")
  end
end
