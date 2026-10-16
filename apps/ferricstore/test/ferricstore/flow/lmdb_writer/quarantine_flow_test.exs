defmodule Ferricstore.Flow.LMDBWriter.QuarantineFlowTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.{Keys, LMDB, LMDBWriter}
  alias Ferricstore.Store.Router

  setup do
    previous = Application.get_env(:ferricstore, :flow_lmdb_quarantine_retry_ms)
    Application.put_env(:ferricstore, :flow_lmdb_quarantine_retry_ms, 20)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :flow_lmdb_quarantine_retry_ms)
        value -> Application.put_env(:ferricstore, :flow_lmdb_quarantine_retry_ms, value)
      end
    end)

    :ok
  end

  test "an undecodable Flow record is quarantined while the rest of its shard keeps projecting" do
    ctx = FerricStore.Instance.get(:default)
    now_ms = 4_050
    target_prefix = unique_flow_id("quarantine-target")
    partition = unique_flow_id("quarantine-partition")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(unique_flow_id("quarantine-schedule"),
               kind: :interval,
               every_ms: 100,
               start_at_ms: now_ms,
               now_ms: now_ms,
               overlap_policy: :allow,
               target: [
                 id_prefix: target_prefix,
                 type: unique_flow_id("quarantine-type"),
                 partition_key: partition
               ]
             )

    assert {:ok, %{fired: 1}} = FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "q")

    # The first target's projection is still pending when its record turns corrupt.
    corrupt_key = Keys.state_key("#{target_prefix}:#{now_ms}:1", partition)
    shard_index = Router.shard_for(ctx, corrupt_key)
    path = ctx.data_dir |> Ferricstore.DataDir.shard_data_path(shard_index) |> LMDB.path()

    assert :ok = Router.put(ctx, corrupt_key, "corrupt", 0)

    assert {:ok, %{fired: 1}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 100, worker: "q")

    # Same partition, so the same shard as the corrupt record.
    healthy_key = Keys.state_key("#{target_prefix}:#{now_ms + 100}:2", partition)
    assert Router.shard_for(ctx, healthy_key) == shard_index

    assert {:error, {:flow_projection_quarantined, 1}} =
             LMDBWriter.flush(ctx.name, shard_index, 30_000)

    assert {:ok, _query_row} = LMDB.get(path, healthy_key)
    assert writer_mirror_degraded?(ctx, shard_index)

    # Every op up to the shard's raft position is already queued, so asking
    # for that replay-safe index is legitimate. It must wait for the held
    # projection: LMDB does not have it yet.
    writer = Process.whereis(LMDBWriter.name(ctx.name, shard_index))
    durable_before = :sys.get_state(writer).durable_index

    assert {:ok, {:raft_log_pos, raft_index, _term}} =
             Ferricstore.Raft.WARaftBackend.storage_position(shard_index)

    assert raft_index > durable_before
    GenServer.cast(writer, {:persist_replay_safe, raft_index})

    assert {:error, {:flow_projection_quarantined, 1}} =
             LMDBWriter.flush(ctx.name, shard_index, 30_000)

    assert :sys.get_state(writer).durable_index == durable_before

    # Once the record is gone, the held projection retries on its own, deletes
    # what LMDB held for it, and the replay-safe index catches up.
    assert :ok = Router.delete(ctx, corrupt_key)

    assert eventually(fn -> LMDBWriter.flush(ctx.name, shard_index, 30_000) == :ok end)
    assert :not_found = LMDB.get(path, corrupt_key)
    assert :sys.get_state(writer).durable_index >= raft_index
  end

  defp writer_mirror_degraded?(ctx, shard_index),
    do: :atomics.get(ctx.flow_lmdb_mirror_degraded, shard_index + 1) == 1
end
