defmodule Ferricstore.Flow.ScheduleRecoveryTest do
  use ExUnit.Case, async: false

  @moduletag :flow
  @moduletag :global_state
  @moduletag :shard_kill
  @moduletag timeout: 180_000

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, isolated_ctx: isolated}
  end

  test "schedule record and due index survive application restart", %{isolated_ctx: isolated} do
    now_ms = 10_000
    schedule_id = unique_id("schedule-restart")
    target_id = unique_id("schedule-restart-target")
    target_type = unique_id("schedule-restart-type")
    target_partition = unique_id("schedule-restart-partition")

    assert {:ok, schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms,
               now_ms: now_ms - 1,
               target: [
                 id: target_id,
                 type: target_type,
                 partition_key: target_partition,
                 payload: %{restart: true}
               ]
             )

    assert schedule.state == "active"
    assert schedule.next_run_at_ms == now_ms

    Ferricstore.Application.prep_stop(nil)
    ShardHelpers.restart_current_data_dir(isolated)

    ShardHelpers.eventually(
      fn ->
        assert {:ok, restarted_schedule} = FerricStore.flow_schedule_get(schedule_id)
        assert restarted_schedule.state == "active"
        assert restarted_schedule.next_run_at_ms == now_ms
      end,
      "schedule should be readable after full application restart",
      300,
      100
    )

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "restart-scheduler")

    assert {:ok, target} =
             FerricStore.flow_get(target_id, partition_key: target_partition, payload: true)

    assert target.type == target_type
    assert target.payload == %{restart: true}

    assert {:ok, %{state: "completed"}} = FerricStore.flow_schedule_get(schedule_id)

    assert {:ok, %{fired: 0, claimed: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 1, worker: "restart-scheduler")
  end

  test "claimed schedule is reclaimable after shard leader restart" do
    now_ms = 20_000
    schedule_id = unique_id("schedule-claimed-restart")
    target_id = unique_id("schedule-claimed-restart-target")
    target_type = unique_id("schedule-claimed-restart-type")
    target_partition = unique_id("schedule-claimed-restart-partition")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms,
               now_ms: now_ms,
               target: [
                 id: target_id,
                 type: target_type,
                 partition_key: target_partition,
                 payload: "after-reclaim"
               ]
             )

    ctx = FerricStore.Instance.get(:default)

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(
               ctx,
               "__ferricstore_schedule",
               Ferricstore.Flow.Internal.put(
                 state: "active",
                 partition_key: :any,
                 worker: "scheduler-before-crash",
                 limit: 1,
                 lease_ms: 1,
                 now_ms: now_ms,
                 payload: true
               )
             )

    assert claimed.id == Ferricstore.Flow.Schedule.flow_id(schedule_id)

    schedule_id
    |> schedule_shard()
    |> ShardHelpers.kill_shard_safely(timeout: 45_000)

    ShardHelpers.wait_default_quorum_writable(60_000)

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: now_ms + 100,
               worker: "scheduler-after-crash"
             )

    assert {:ok, target} =
             FerricStore.flow_get(target_id, partition_key: target_partition, payload: true)

    assert target.type == target_type
    assert target.payload == "after-reclaim"
    assert {:ok, %{state: "completed"}} = FerricStore.flow_schedule_get(schedule_id)
  end

  defp unique_id(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"

  defp schedule_shard(schedule_id) do
    ctx = FerricStore.Instance.get(:default)
    flow_id = Ferricstore.Flow.Schedule.flow_id(schedule_id)

    partition_key =
      "__ferricstore_schedule__:" <> Integer.to_string(:erlang.phash2(schedule_id, 256))

    state_key = Ferricstore.Flow.Keys.state_key(flow_id, partition_key)

    Router.shard_for(ctx, state_key)
  end
end
