defmodule Ferricstore.Flow.RetentionMissingMirrorTest do
  @moduledoc """
  Retention removes a terminal Flow only once it has read the Flow's whole
  history, which needs the LMDB mirror. With the mirror missing it can plan no
  Flow, and it used to report `more?` anyway: a caller that followed the
  continuation got the same one back on every pass.
  """
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state
  @moduletag timeout: 300_000

  alias Ferricstore.Flow.{HistoryProjector, Keys, LMDB}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @partition "retention-missing-mirror"

  setup do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    :ok
  end

  test "a pass over a shard without its mirror ends instead of repeating" do
    ctx = FerricStore.Instance.get(:default)
    ids = for i <- 1..8, do: "missing-mirror-#{System.unique_integer([:positive])}-#{i}"

    for id <- ids do
      Ferricstore.Flow.Admission.clear_create_pause()

      assert :ok =
               FerricStore.flow_create(id,
                 type: "missing-mirror",
                 partition_key: @partition,
                 retention_ttl_ms: 1,
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )
    end

    assert {:ok, claimed} =
             FerricStore.flow_claim_due("missing-mirror",
               partition_key: @partition,
               worker: "w",
               limit: 100,
               now_ms: 1_000
             )

    for flow <- claimed do
      assert :ok =
               FerricStore.flow_complete(flow.id, flow.lease_token,
                 partition_key: @partition,
                 fencing_token: flow.fencing_token,
                 result: "done",
                 now_ms: 1_100
               )
    end

    flush!(ctx)

    shard = Router.shard_for(ctx, Keys.state_key(hd(ids), @partition))
    mirror = LMDB.path(Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard))
    _ = LMDB.release(mirror)
    File.rm_rf!(mirror)
    refute LMDB.env_present?(mirror)

    results =
      Enum.reduce_while(1..20, {nil, []}, fn _pass, {continuation, seen} ->
        opts =
          [limit: 3, now_ms: 10_000_000_000_000] ++
            if(continuation, do: [continuation: continuation], else: [])

        assert {:ok, result} = FerricStore.flow_retention_cleanup(opts)

        if result.more?,
          do: {:cont, {result.continuation, [result | seen]}},
          else: {:halt, {:done, [result | seen]}}
      end)

    assert {:done, passes} = results, "retention kept saying more? without finishing"
    assert Enum.all?(passes, &(&1.flows == 0))

    # Without the mirror nothing is removed: the Flows wait for it.
    for id <- ids,
        do: assert({:ok, true} = FerricStore.Impl.exists?(ctx, Keys.registry_key(id, @partition)))
  end

  defp flush!(ctx) do
    for shard <- 0..(ctx.shard_count - 1),
        do: assert(:ok = HistoryProjector.flush(ctx, shard, 30_000))

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 30_000)
  end
end
