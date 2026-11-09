defmodule Ferricstore.Flow.HistoryProjectorKillTest do
  @moduledoc """
  A history projector killed outright runs no terminate/2, so history it had
  not synced is gone from it. Only Raft can bring it back, by replaying the
  commands on the next start, and only if nothing claimed it was written.
  """
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state
  @moduletag timeout: 180_000

  alias Ferricstore.FaultInjection
  alias Ferricstore.Flow.{HistoryProjector, Keys}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, isolated: isolated}
  end

  test "history a killed projector never synced comes back on the next start",
       %{isolated: isolated} do
    ctx = FerricStore.Instance.get(:default)
    lost = "lost-#{System.unique_integer([:positive])}"
    shard = Router.shard_for(ctx, Keys.state_key(lost, "lost"))
    later = "later-#{System.unique_integer([:positive])}"
    later_partition = partition_on_shard(ctx, later, shard)
    restart_projector_flushing_every(shard, 600_000)
    hold_history_writes(ctx, shard)

    create!(lost, "lost")
    assert {:error, :flush_failed} = HistoryProjector.flush(ctx, shard, 30_000)
    assert {:ok, pending} = HistoryProjector.pending_count(ctx, shard)
    assert pending > 0
    kill_projector!(ctx, shard)
    FaultInjection.clear_hook()

    # Newer history on the shard is written past the lost one.
    create!(later, later_partition)
    write_past_loss!(ctx, shard)

    :ok = restart_as_new_process(isolated, ctx.shard_count)
    flush!()

    assert ["created"] == events(lost, "lost")
    assert ["created"] == events(later, later_partition)
  end

  defp events(id, partition) do
    assert {:ok, history} =
             FerricStore.flow_history(id,
               partition_key: partition,
               count: 10,
               include_cold: true,
               consistent_projection: true
             )

    Enum.map(history, fn {_event_id, fields} -> fields["event"] end)
  end

  defp create!(id, partition) do
    assert :ok =
             FerricStore.flow_create(id,
               type: "killed",
               partition_key: partition,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 1_000
             )
  end

  defp partition_on_shard(ctx, id, shard) do
    Enum.find_value(1..10_000, fn n ->
      partition = "p#{n}"
      if Router.shard_for(ctx, Keys.state_key(id, partition)) == shard, do: partition
    end)
  end

  defp kill_projector!(ctx, shard) do
    pid = Process.whereis(HistoryProjector.name(ctx, shard))
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

    assert Ferricstore.Test.Eventually.eventually(fn ->
             successor = Process.whereis(HistoryProjector.name(ctx, shard))
             is_pid(successor) and successor != pid
           end)
  end

  defp flush! do
    ctx = FerricStore.Instance.get(:default)

    for shard <- 0..(ctx.shard_count - 1),
        do: assert(:ok = HistoryProjector.flush(ctx, shard, 30_000))

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 30_000)
    ctx
  end

  # Until the node restarts, the shard that lost history publishes no index
  # past the loss, so its flush fails once WARaft asks for one. It still
  # writes its newer history: then it has none left to write.
  defp write_past_loss!(ctx, lost_shard) do
    for shard <- 0..(ctx.shard_count - 1), shard != lost_shard do
      assert :ok = HistoryProjector.flush(ctx, shard, 30_000)
    end

    assert HistoryProjector.flush(ctx, lost_shard, 30_000) in [:ok, {:error, :flush_failed}]
    assert {:ok, 0} = HistoryProjector.pending_count(ctx, lost_shard)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 30_000)
  end

  # WARaft asks the projector to write whenever it moves its release cursor,
  # whatever the flush interval, so the projector's writes fail instead, to
  # keep the history in its memory.
  defp hold_history_writes(ctx, shard) do
    projector = Process.whereis(HistoryProjector.name(ctx, shard))
    on_exit(&FaultInjection.clear_hook/0)

    FaultInjection.put_hook(fn
      :before_flow_history_write, _metadata when self() == projector -> {:error, :held_by_test}
      _point, _metadata -> :ok
    end)
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

    child = :"flow_history_projector_#{shard}"
    :ok = Supervisor.terminate_child(Ferricstore.Store.ShardSupervisor, child)
    {:ok, _pid} = Supervisor.restart_child(Ferricstore.Store.ShardSupervisor, child)
    :ok
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
