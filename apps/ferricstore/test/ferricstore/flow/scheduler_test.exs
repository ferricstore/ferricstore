defmodule Ferricstore.Flow.SchedulerTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.ClaimWaiters
  alias Ferricstore.Flow.Scheduler

  @claim_waiter_table :ferricstore_flow_claim_waiters

  test "configuration validates the batch limit and is a stable snapshot" do
    previous_limit = Application.get_env(:ferricstore, :flow_scheduler_limit)

    on_exit(fn ->
      if is_nil(previous_limit) do
        Application.delete_env(:ferricstore, :flow_scheduler_limit)
      else
        Application.put_env(:ferricstore, :flow_scheduler_limit, previous_limit)
      end
    end)

    Application.put_env(:ferricstore, :flow_scheduler_limit, 0)

    config = Scheduler.configuration(enabled: true, initial_delay_ms: -1, error_sleep_ms: -1)

    assert config == %{
             enabled?: true,
             limit: 100,
             initial_delay_ms: 2_000,
             error_sleep_ms: 1_000
           }

    Application.put_env(:ferricstore, :flow_scheduler_limit, 25)

    assert config.limit == 100
    assert Scheduler.configuration().limit == 25
  end

  test "configuration defaults the runner on but fails closed for invalid boolean values" do
    previous_enabled = Application.get_env(:ferricstore, :flow_scheduler_enabled)

    on_exit(fn ->
      restore_env(:flow_scheduler_enabled, previous_enabled)
    end)

    Application.delete_env(:ferricstore, :flow_scheduler_enabled)
    assert Scheduler.configuration().enabled?

    Application.put_env(:ferricstore, :flow_scheduler_enabled, :invalid)
    refute Scheduler.configuration().enabled?
    refute Scheduler.configuration(enabled: "true").enabled?
  end

  test "configuration caps the scheduler batch at the claim command maximum" do
    previous_limit = Application.get_env(:ferricstore, :flow_scheduler_limit)
    previous_max = Application.get_env(:ferricstore, :flow_max_claim_limit)

    on_exit(fn ->
      restore_env(:flow_scheduler_limit, previous_limit)
      restore_env(:flow_max_claim_limit, previous_max)
    end)

    Application.put_env(:ferricstore, :flow_scheduler_limit, 2_000)
    Application.put_env(:ferricstore, :flow_max_claim_limit, 50)

    assert Scheduler.configuration().limit == 50
    assert Scheduler.configuration(limit: 75).limit == 50

    Application.put_env(:ferricstore, :flow_scheduler_limit, 25)
    assert Scheduler.configuration().limit == 25
  end

  test "a claimed batch drains immediately even when every schedule was skipped" do
    task = make_ref()

    state = %{
      ctx: :unused,
      task: task,
      config: %{enabled?: true, limit: 100, initial_delay_ms: 0, error_sleep_ms: 60_000}
    }

    result = {:ok, %{claimed: 1, fired: 0, skipped: 1, coalesced: 0, errors: []}}

    assert {:noreply, %{task: nil}} =
             Scheduler.handle_info({:fire_due_done, task, result}, state)

    assert_receive :fire_due
  end

  test "a scheduler waiting on an empty store wakes for a newly-created future schedule" do
    ctx = FerricStore.Instance.get(:default)
    versions = fn -> for i <- 1..ctx.shard_count, do: :counters.get(ctx.write_version, i) end
    before_idle = versions.()

    {:ok, scheduler} =
      Scheduler.start_link(
        name: nil,
        enabled: true,
        initial_delay_ms: 0,
        error_sleep_ms: 5_000
      )

    on_exit(fn ->
      Process.unlink(scheduler)
      Process.exit(scheduler, :shutdown)
    end)

    waiter_keys = ClaimWaiters.wait_keys("__ferricstore_schedule", "active", nil, :any)

    assert eventually(
             fn -> scheduler_waiting?(scheduler, waiter_keys) end,
             timeout: 15_000,
             interval: 25
           ),
           "scheduler did not register a waiter; state: #{inspect(scheduler_state(scheduler), limit: 10)}"

    assert versions.() == before_idle,
           "registering an idle scheduler must not append empty claims to every Raft shard"

    now_ms = Ferricstore.CommandTime.now_ms()
    schedule_id = unique_flow_id("scheduler-future")
    target_id = unique_flow_id("scheduler-future-target")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms + 150,
               now_ms: now_ms,
               target: [id: target_id, type: unique_flow_id("scheduler-future-type")]
             )

    assert eventually(
             fn -> match?({:ok, %{id: ^target_id}}, FerricStore.flow_get(target_id)) end,
             timeout: 5_000,
             interval: 10
           )
  end

  test "orphaned cold schedules and unrelated cold work do not append empty claims" do
    ctx = FerricStore.Instance.get(:default)
    now_ms = Ferricstore.CommandTime.now_ms()

    for shard <- 0..(ctx.shard_count - 1) do
      path =
        ctx.data_dir
        |> Ferricstore.DataDir.shard_data_path(shard)
        |> Ferricstore.Flow.LMDB.path()

      orphan_key =
        Ferricstore.Flow.LMDB.cold_due_key(
          type: "__ferricstore_schedule",
          state: "active",
          partition_key: "old-schedule",
          priority: 0,
          due_at_ms: now_ms - 60_000,
          flow_id: "missing-schedule-#{shard}",
          version: 1
        )

      unrelated_key =
        Ferricstore.Flow.LMDB.cold_due_key(
          type: "unrelated-work",
          state: "queued",
          partition_key: "unrelated",
          priority: 0,
          due_at_ms: now_ms - 60_000,
          flow_id: "unrelated-#{shard}",
          version: 1
        )

      unrelated_park = "flow:park:v1:unrelated-#{shard}"

      assert :ok =
               Ferricstore.Flow.LMDB.write_batch(path, [
                 {:put, orphan_key, "flow:park:v1:missing-schedule-#{shard}"},
                 {:put, unrelated_key, unrelated_park},
                 {:put, unrelated_park, "unrelated-park"}
               ])

      on_exit(fn ->
        Ferricstore.Flow.LMDB.write_batch(path, [
          {:delete, orphan_key},
          {:delete, unrelated_key},
          {:delete, unrelated_park}
        ])
      end)
    end

    versions = fn -> for i <- 1..ctx.shard_count, do: :counters.get(ctx.write_version, i) end
    before_idle = versions.()

    {:ok, scheduler} =
      Scheduler.start_link(
        name: nil,
        enabled: true,
        initial_delay_ms: 0,
        error_sleep_ms: 5_000
      )

    on_exit(fn ->
      Process.unlink(scheduler)
      Process.exit(scheduler, :shutdown)
    end)

    waiter_keys = ClaimWaiters.wait_keys("__ferricstore_schedule", "active", nil, :any)

    assert eventually(fn -> scheduler_waiting?(scheduler, waiter_keys) end,
             timeout: 15_000,
             interval: 25
           )

    assert versions.() == before_idle
  end

  test "a committed target completion wakes a queued schedule before its fallback retry" do
    scheduler = Process.whereis(Scheduler)
    assert is_pid(scheduler)

    now_ms = System.system_time(:millisecond)
    schedule_id = unique_flow_id("schedule-completion-wake")
    target_prefix = unique_flow_id("schedule-completion-wake-target")
    target_type = unique_flow_id("schedule-completion-wake-type")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: 100,
               start_at_ms: now_ms - 100,
               now_ms: now_ms - 100,
               overlap_policy: :queue_after_previous,
               overlap_retry_ms: 10_000,
               target: [id_prefix: target_prefix, type: target_type]
             )

    assert {:ok, %{fired: 1}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms - 100, worker: "wake-test")

    assert {:ok, %{skipped: 1}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "wake-test")

    assert {:ok, waiting} = FerricStore.flow_schedule_get(schedule_id)
    assert waiting.next_run_at_ms == now_ms + 10_000

    previous_config = :sys.get_state(scheduler).config

    on_exit(fn ->
      if Process.alive?(scheduler) do
        :sys.replace_state(scheduler, fn state -> %{state | config: previous_config} end)
      end
    end)

    :sys.replace_state(scheduler, fn state ->
      %{state | config: %{state.config | enabled?: true}}
    end)

    assert {:ok, [job]} =
             FerricStore.flow_claim_due(target_type,
               worker: "schedule-completion-worker",
               limit: 1,
               now_ms: now_ms
             )

    assert :ok =
             FerricStore.flow_complete(job.id, job.lease_token,
               fencing_token: job.fencing_token,
               now_ms: now_ms
             )

    assert eventually(
             fn ->
               match?({:ok, %{fire_count: 2}}, FerricStore.flow_schedule_get(schedule_id))
             end,
             timeout: 5_000,
             interval: 25
           )

    :sys.replace_state(scheduler, fn state -> %{state | config: previous_config} end)

    assert eventually(fn -> :sys.get_state(scheduler).task == nil end,
             timeout: 5_000,
             interval: 25
           )
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp scheduler_waiting?(scheduler, waiter_keys) do
    case scheduler_state(scheduler) do
      %{task: task} when is_pid(task) ->
        case :ets.whereis(@claim_waiter_table) do
          :undefined ->
            false

          table ->
            Enum.any?(waiter_keys, &(:ets.match_object(table, {&1, task, :_, :_, :_}) != []))
        end

      _state ->
        false
    end
  end

  defp scheduler_state(scheduler) do
    :sys.get_state(scheduler)
  catch
    :exit, reason -> {:exit, reason}
  end
end
