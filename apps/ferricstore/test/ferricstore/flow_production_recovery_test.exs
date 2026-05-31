defmodule Ferricstore.FlowProductionRecoveryTest do
  use ExUnit.Case, async: false

  @moduletag :shard_kill
  @moduletag timeout: 180_000

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    old_threshold = Application.get_env(:ferricstore, :blob_side_channel_threshold_bytes)
    old_reconcile = Application.get_env(:ferricstore, :blob_protection_reconcile_enabled)

    Application.put_env(:ferricstore, :blob_side_channel_threshold_bytes, 64)
    Application.put_env(:ferricstore, :blob_protection_reconcile_enabled, true)

    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      restore_env(:blob_side_channel_threshold_bytes, old_threshold)
      restore_env(:blob_protection_reconcile_enabled, old_reconcile)
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    :ok
  end

  test "Flow truth, hot indexes, blob values, history, and cold projection survive supervised shard crash" do
    type = unique("prod-recovery-type")
    partition = unique("tenant-prod-recovery")
    terminal_id = unique("prod-recovery-terminal")
    ready_id = unique("prod-recovery-ready")

    assert :ok =
             FerricStore.flow_create(terminal_id,
               type: type,
               partition_key: partition,
               state: "queued",
               payload: :binary.copy("payload:", 128),
               run_at_ms: 1_000,
               now_ms: 1_000,
               history_max_events: 20
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               state: "queued",
               worker: "prod-worker-a",
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == terminal_id
    assert claimed.payload == :binary.copy("payload:", 128)

    assert :ok =
             FerricStore.flow_transition(terminal_id, "running", "waiting",
               partition_key: partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               payload: :binary.copy("transition:", 128),
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert {:ok, [reclaimed]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               state: "waiting",
               worker: "prod-worker-b",
               limit: 1,
               now_ms: 2_000
             )

    assert reclaimed.id == terminal_id
    assert reclaimed.payload == :binary.copy("transition:", 128)

    assert :ok =
             FerricStore.flow_complete(terminal_id, reclaimed.lease_token,
               partition_key: partition,
               fencing_token: reclaimed.fencing_token,
               result: :binary.copy("result:", 128),
               now_ms: 2_100
             )

    assert :ok =
             FerricStore.flow_create(ready_id,
               type: type,
               partition_key: partition,
               state: "queued",
               payload: :binary.copy("ready:", 128),
               run_at_ms: 1_000,
               now_ms: 1_000,
               history_max_events: 20
             )

    ShardHelpers.flush_all_shards()

    ctx = FerricStore.Instance.get(:default)
    projection_tasks = start_background_projection_work(ctx)
    gc_task = Task.async(fn -> Router.sweep_blob_garbage(ctx) end)

    terminal_shard = flow_shard(ctx, terminal_id, partition)
    ShardHelpers.kill_shard_safely(terminal_shard, timeout: 45_000)
    await_or_shutdown([gc_task | projection_tasks])

    restarted_ctx = FerricStore.Instance.get(:default)

    ShardHelpers.eventually(
      fn ->
        with {:ok, record} <- FerricStore.flow_get(terminal_id, partition_key: partition, full: true) do
          record.state == "completed" and
            record.payload == :binary.copy("transition:", 128) and
            record.result == :binary.copy("result:", 128)
        else
          _ -> false
        end
      end,
      "terminal Flow state and blob-backed values should survive shard crash",
      300,
      100
    )

    assert {:ok, history} =
             FerricStore.flow_history(terminal_id,
               partition_key: partition,
               count: 20,
               include_cold: true,
               consistent_projection: true,
               values: true
             )

    history_events = Enum.map(history, fn {_event_id, fields} -> fields["event"] end)
    assert "created" in history_events
    assert "transitioned" in history_events
    assert "completed" in history_events

    assert {:ok, terminals} =
             FerricStore.flow_terminals(type,
               partition_key: partition,
               state: "completed",
               count: 10,
               include_cold: true,
               consistent_projection: true
             )

    assert Enum.any?(terminals, &(&1.id == terminal_id))

    ready_claim = claim_ready_flow(type, partition, ready_id)
    assert ready_claim.id == ready_id
    assert ready_claim.payload == :binary.copy("ready:", 128)

    assert {:ok, _stats} = Router.sweep_blob_garbage(restarted_ctx)
  end

  defp start_background_projection_work(ctx) do
    lmdb_task =
      Task.async(fn ->
        Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 45_000)
      end)

    history_task =
      Task.async(fn ->
        for shard <- 0..(ctx.shard_count - 1), reduce: :ok do
          :ok ->
            case Ferricstore.Flow.HistoryProjector.flush(ctx, shard, 45_000) do
              :ok -> :ok
              {:error, reason} -> {:error, reason}
            end

          error ->
            error
        end
      end)

    [lmdb_task, history_task]
  end

  defp await_or_shutdown(tasks) do
    Enum.each(tasks, fn task ->
      case Task.yield(task, 45_000) || Task.shutdown(task, :brutal_kill) do
        {:ok, _result} -> :ok
        {:exit, _reason} -> :ok
        nil -> :ok
      end
    end)
  end

  defp flow_shard(ctx, id, partition_key) do
    Router.shard_for(ctx, Ferricstore.Flow.Keys.state_key(id, partition_key))
  end

  defp claim_ready_flow(type, partition, ready_id, attempts \\ 100)

  defp claim_ready_flow(type, partition, ready_id, attempts) when attempts > 0 do
    case FerricStore.flow_claim_due(type,
           partition_key: partition,
           state: "queued",
           worker: "prod-worker-c",
           limit: 1,
           now_ms: 2_500
         ) do
      {:ok, [%{id: ^ready_id} = claim]} ->
        claim

      {:ok, []} ->
        Process.sleep(100)
        claim_ready_flow(type, partition, ready_id, attempts - 1)

      other ->
        flunk("unexpected claim_due result while waiting for ready flow: #{inspect(other)}")
    end
  end

  defp claim_ready_flow(_type, _partition, ready_id, _attempts) do
    flunk("ready flow #{ready_id} was not claimable after shard recovery")
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp unique(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"
end
