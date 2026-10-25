defmodule Ferricstore.Flow.RetentionCleanupMirrorLagTest do
  @moduledoc """
  A completed Flow lives only in the LMDB mirror once its hot row is pruned.
  Retention cleanup deletes it in apply and queues the mirror delete, which
  the node's LMDB writer runs later. Until then the deleted Flow was still
  read from the mirror: FLOW.GET returned it, and a create of the same id was
  refused as "already exists" in apply. A replica whose writer had caught up
  applied that same create, so replicas diverged. The delete now leaves a
  tombstone that hides the mirror copy until the writer has removed it.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Flow.LMDBWriter
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    %{ctx: FerricStore.Instance.get(:default)}
  end

  defp completed_and_pruned!(ctx, id) do
    :ok = FerricStore.flow_create(id, type: "mirror-lag", payload: "in")
    {:ok, [c]} = FerricStore.flow_claim_due("mirror-lag", worker: "w", limit: 1)

    :ok =
      FerricStore.flow_complete(c.id, c.lease_token, fencing_token: c.fencing_token, result: "ok")

    {:ok, record} = FerricStore.flow_get(id)
    # As FLOW.RETENTION_CLEANUP does before planning.
    :ok = LMDBWriter.flush_all(ctx.name, ctx.shard_count, 60_000)

    for shard <- 0..(ctx.shard_count - 1),
        do: :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard, 60_000)

    :ok = LMDBWriter.flush_all(ctx.name, ctx.shard_count, 60_000)
    record
  end

  defp writer_for(ctx, id) do
    shard = Router.shard_for(ctx, Ferricstore.Flow.Keys.state_key(id, nil))
    Process.whereis(:"Ferricstore.Flow.LMDBWriter.#{shard}")
  end

  # The tombstone is a keydir row with no value and no location; a segment
  # projection checkpoint must skip it rather than fail reading it.
  test "a checkpoint taken while a tombstone exists succeeds", %{ctx: ctx} do
    record = completed_and_pruned!(ctx, "checkpointed")
    state_key = Ferricstore.Flow.Keys.state_key("checkpointed", nil)
    shard = Router.shard_for(ctx, state_key)
    writer = writer_for(ctx, "checkpointed")
    parent = self()
    handler = {__MODULE__, :checkpoint, make_ref()}

    :telemetry.attach(
      handler,
      [:ferricstore, :waraft, :segment_projection_checkpoint, :stop],
      fn _event, _measurements, metadata, _config -> send(parent, {:checkpoint, metadata}) end,
      nil
    )

    previous =
      for key <- [
            :waraft_segment_projection_checkpoint_every,
            :waraft_segment_projection_checkpoint_min_interval_ms
          ],
          do: {key, Application.fetch_env(:ferricstore, key)}

    on_exit(fn ->
      :telemetry.detach(handler)

      for {key, value} <- previous do
        case value do
          {:ok, v} -> Application.put_env(:ferricstore, key, v)
          :error -> Application.delete_env(:ferricstore, key)
        end
      end
    end)

    :ok = :sys.suspend(writer)

    try do
      assert {:ok, %{flows: 1}} =
               Router.flow_retention_cleanup(ctx, %{
                 limit: 10,
                 now_ms: record.terminal_retention_until_ms + 1,
                 continuation: nil
               })

      keydir = elem(ctx.keydir_refs, shard)

      assert [{^state_key, nil, 0, :flow_state_deleted, :deleted, 0, 0}] =
               :ets.lookup(keydir, state_key)

      Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_min_interval_ms, 0)
      Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, 1)
      key = Enum.find(1..10_000, &(Router.shard_for(ctx, "ckpt-#{&1}") == shard))
      assert :ok = Router.put(ctx, "ckpt-#{key}", "v", 0)

      assert_receive {:checkpoint, %{shard_index: ^shard} = metadata}, 30_000
      assert metadata.result == :ok, inspect(metadata)
    after
      :sys.resume(writer)
    end
  end

  test "a cleaned Flow is gone and its id reusable before the mirror delete runs", %{ctx: ctx} do
    record = completed_and_pruned!(ctx, "lagged")
    writer = writer_for(ctx, "lagged")
    :ok = :sys.suspend(writer)

    try do
      # The cleanup entry itself, without the command's writer flush.
      assert {:ok, %{flows: 1}} =
               Router.flow_retention_cleanup(ctx, %{
                 limit: 10,
                 now_ms: record.terminal_retention_until_ms + 1,
                 continuation: nil
               })

      assert {:ok, nil} = FerricStore.flow_get("lagged")
      assert :ok = FerricStore.flow_create("lagged", type: "mirror-lag", payload: "again")
      assert {:ok, %{state: "queued", version: 1}} = FerricStore.flow_get("lagged")
    after
      :sys.resume(writer)
    end

    :ok = LMDBWriter.flush_all(ctx.name, ctx.shard_count, 60_000)
    assert {:ok, %{state: "queued", version: 1}} = FerricStore.flow_get("lagged")
  end
end
