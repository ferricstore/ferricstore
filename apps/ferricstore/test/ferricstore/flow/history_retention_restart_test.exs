defmodule Ferricstore.Flow.HistoryRetentionRestartTest do
  @moduledoc """
  History that retention removed stays removed across a restart. Startup
  recovery scans the active history log file again, so the removal has to be
  in the log, not only in the indexes.
  """
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state
  @moduletag timeout: 180_000

  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Test.ShardHelpers

  @partition "history-retention-restart"

  setup do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, isolated: isolated}
  end

  test "a Flow ID reused after retention and a restart starts with an empty history",
       %{isolated: isolated} do
    id = "retained-#{System.unique_integer([:positive])}"
    create!(id, 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("retained",
               partition_key: @partition,
               worker: "w",
               limit: 1,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: @partition,
               fencing_token: claimed.fencing_token,
               now_ms: 1_100
             )

    ctx = flush!()
    assert {:ok, completed} = FerricStore.flow_get(id, partition_key: @partition)

    assert {:ok, %{flows: 1}} =
             FerricStore.flow_retention_cleanup(
               limit: 10,
               now_ms: completed.terminal_retention_until_ms + 1
             )

    flush!()
    :ok = restart_as_new_process(isolated, ctx.shard_count)

    create!(id, 5_000)
    flush!()

    assert {:ok, [{_event_id, %{"event" => "created"}}]} =
             FerricStore.flow_history(id,
               partition_key: @partition,
               count: 10,
               include_cold: true,
               consistent_projection: true
             )
  end

  # The node dies before the history projector writes the tombstones. Raft
  # replays the cleanup on the next start; that start removes the history
  # again either way, so only the start after it shows whether the replay
  # wrote the tombstones.
  test "history retention removed stays removed when the node dies before its tombstones",
       %{isolated: isolated} do
    id = "retained-crash-#{System.unique_integer([:positive])}"
    create!(id, 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("retained",
               partition_key: @partition,
               worker: "w",
               limit: 1,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: @partition,
               fencing_token: claimed.fencing_token,
               now_ms: 1_100
             )

    ctx = flush!()
    assert {:ok, completed} = FerricStore.flow_get(id, partition_key: @partition)

    shard =
      Ferricstore.Store.Router.shard_for(ctx, Ferricstore.Flow.Keys.state_key(id, @partition))

    restart_projector_flushing_every(shard, 600_000)

    assert {:ok, %{flows: 1}} =
             FerricStore.flow_retention_cleanup(
               limit: 10,
               now_ms: completed.terminal_retention_until_ms + 1
             )

    # The tombstones wait in its memory and die with the node. A projector
    # restarted on its own would scan the log and move the recovery
    # checkpoint past the removed entries, which a crash does not.
    assert {:ok, pending} = HistoryProjector.pending_count(ctx, shard)
    assert pending > 0
    :ok = Supervisor.terminate_child(Ferricstore.Store.ShardSupervisor, projector_child(shard))
    :ok = HistoryProjector.Pending.discard(HistoryProjector.name(ctx, shard))
    :ok = restart_as_new_process(isolated, ctx.shard_count)
    flush!()
    :ok = restart_as_new_process(isolated, ctx.shard_count)

    create!(id, 5_000)
    flush!()

    assert {:ok, [{_event_id, %{"event" => "created"}}]} =
             FerricStore.flow_history(id,
               partition_key: @partition,
               count: 10,
               include_cold: true,
               consistent_projection: true
             )
  end

  defp restart_projector_flushing_every(shard, interval_ms) do
    key = :flow_history_projector_flush_interval_ms
    previous = Application.fetch_env(:ferricstore, key)
    Application.put_env(:ferricstore, key, interval_ms)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:ferricstore, key, value)
        :error -> Application.delete_env(:ferricstore, key)
      end
    end)

    :ok = Supervisor.terminate_child(Ferricstore.Store.ShardSupervisor, projector_child(shard))

    {:ok, _pid} =
      Supervisor.restart_child(Ferricstore.Store.ShardSupervisor, projector_child(shard))

    :ok
  end

  defp projector_child(shard), do: :"flow_history_projector_#{shard}"

  defp create!(id, now_ms) do
    assert :ok =
             FerricStore.flow_create(id,
               type: "retained",
               partition_key: @partition,
               state: "queued",
               run_at_ms: now_ms,
               now_ms: now_ms,
               retention_ttl_ms: 60_000
             )
  end

  defp flush! do
    ctx = FerricStore.Instance.get(:default)

    for shard <- 0..(ctx.shard_count - 1),
        do: assert(:ok = HistoryProjector.flush(ctx, shard, 30_000))

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 30_000)
    ctx
  end

  # A real process restart does not inherit VM-local LMDB writer generations.
  defp restart_as_new_process(%{tmp_dir: data_dir, server_started?: server_started?}, shards) do
    for app <- [:ferricstore_server, :ferricstore],
        Enum.any?(Application.started_applications(), &(elem(&1, 0) == app)) do
      _ = Application.stop(app)
    end

    Ferricstore.Raft.WARaftBackend.stop()

    for shard_index <- 0..(shards - 1) do
      :persistent_term.erase(
        Ferricstore.Flow.LMDBWriter.Registry.enqueue_seq_key(:default, shard_index)
      )
    end

    Application.put_env(:ferricstore, :data_dir, data_dir)
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    ShardHelpers.wait_shards_alive(30_000)

    if server_started? do
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    end

    Ferricstore.Health.set_ready(true)
    :ok
  end
end
