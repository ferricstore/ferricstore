defmodule Ferricstore.Flow.SchedulerTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.ClaimWaiters
  alias Ferricstore.Flow.Scheduler

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

    assert eventually(fn -> ClaimWaiters.total_count() > 0 end)

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

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
