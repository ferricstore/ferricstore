defmodule Ferricstore.Flow.ScheduleCatchupTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.Schedule.Catchup

  test "interval schedules expose the bounded catch-up policy" do
    now_ms = 10_000

    assert {:ok, schedule} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-catchup-contract"),
               kind: :interval,
               every_ms: 1_000,
               start_at_ms: now_ms,
               now_ms: now_ms,
               target: [
                 id_prefix: unique_flow_id("schedule-catchup-contract-target"),
                 type: unique_flow_id("schedule-catchup-contract-type")
               ]
             )

    assert schedule.catchup_policy == :fire_once
    assert schedule.coalesced_count == 0
    assert schedule.last_catchup_at_ms == nil
    assert schedule.last_coalesced_count == 0
  end

  test "an overdue interval coalesces every missed occurrence into one fire" do
    due_at_ms = 20_000
    every_ms = 1_000
    recovery_ms = due_at_ms + 1_000_000 * every_ms + 500
    schedule_id = unique_flow_id("schedule-catchup-overdue")
    target_prefix = unique_flow_id("schedule-catchup-overdue-target")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: every_ms,
               start_at_ms: due_at_ms,
               now_ms: due_at_ms - 1,
               target: [
                 id_prefix: target_prefix,
                 type: unique_flow_id("schedule-catchup-overdue-type")
               ]
             )

    assert {:ok, %{claimed: 1, fired: 1, coalesced: 1_000_000, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: recovery_ms,
               worker: "schedule-catchup-test"
             )

    first_target_id = "#{target_prefix}:#{due_at_ms}:1"
    assert {:ok, %{id: ^first_target_id}} = FerricStore.flow_get(first_target_id)

    assert {:ok, schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert schedule.fire_count == 1
    assert schedule.coalesced_count == 1_000_000
    assert schedule.last_coalesced_count == 1_000_000
    assert schedule.last_catchup_at_ms == recovery_ms
    assert schedule.next_run_at_ms == recovery_ms + every_ms

    assert {:ok, %{claimed: 0, fired: 0, coalesced: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: recovery_ms,
               worker: "schedule-catchup-test"
             )

    assert {:ok, %{claimed: 0, fired: 0, coalesced: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: recovery_ms + every_ms - 1,
               worker: "schedule-catchup-test"
             )

    assert {:ok, %{claimed: 1, fired: 1, coalesced: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: recovery_ms + every_ms,
               worker: "schedule-catchup-test"
             )

    second_target_id = "#{target_prefix}:#{recovery_ms + every_ms}:2"
    assert {:ok, %{id: ^second_target_id}} = FerricStore.flow_get(second_target_id)
  end

  test "catch-up planning remains constant-work near the exact-integer limit" do
    recovery_ms = 9_007_199_254_740_990
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:ok, 9_007_199_254_740_991, ^recovery_ms} =
             Catchup.next_interval(
               %{kind: :interval, every_ms: 1},
               0,
               recovery_ms
             )

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 1_000
  end

  test "an interval less than one period late keeps its original cadence" do
    due_at_ms = 30_000
    every_ms = 1_000
    schedule_id = unique_flow_id("schedule-catchup-on-time")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: every_ms,
               start_at_ms: due_at_ms,
               now_ms: due_at_ms - 1,
               target: [
                 id_prefix: unique_flow_id("schedule-catchup-on-time-target"),
                 type: unique_flow_id("schedule-catchup-on-time-type")
               ]
             )

    assert {:ok, %{claimed: 1, fired: 1, coalesced: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: due_at_ms + every_ms - 1,
               worker: "schedule-catchup-test"
             )

    assert {:ok, schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert schedule.next_run_at_ms == due_at_ms + every_ms
    assert schedule.coalesced_count == 0
  end

  test "catch-up counts only occurrences within an interval schedule end bound" do
    due_at_ms = 40_000
    every_ms = 100
    schedule_id = unique_flow_id("schedule-catchup-end-bound")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: every_ms,
               start_at_ms: due_at_ms,
               now_ms: due_at_ms - 1,
               end_at_ms: due_at_ms + 3 * every_ms,
               target: [
                 id_prefix: unique_flow_id("schedule-catchup-end-bound-target"),
                 type: unique_flow_id("schedule-catchup-end-bound-type")
               ]
             )

    assert {:ok, %{claimed: 1, fired: 1, coalesced: 3, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: due_at_ms + 10_000,
               worker: "schedule-catchup-test"
             )

    assert {:ok, schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert schedule.state == "completed"
    assert schedule.end_reason == "end_at_ms"
    assert schedule.fire_count == 1
    assert schedule.coalesced_count == 3
    assert schedule.next_run_at_ms == nil
  end

  test "catch-up remains independent from overlap skipping" do
    due_at_ms = 50_000
    every_ms = 100
    schedule_id = unique_flow_id("schedule-catchup-overlap")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: every_ms,
               start_at_ms: due_at_ms,
               now_ms: due_at_ms,
               overlap_policy: :skip,
               target: [
                 id_prefix: unique_flow_id("schedule-catchup-overlap-target"),
                 type: unique_flow_id("schedule-catchup-overlap-type")
               ]
             )

    assert {:ok, %{claimed: 1, fired: 1, coalesced: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: due_at_ms,
               worker: "schedule-catchup-test"
             )

    assert {:ok, %{claimed: 1, fired: 0, skipped: 1, coalesced: 4, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: due_at_ms + 5 * every_ms,
               worker: "schedule-catchup-test"
             )

    assert {:ok, schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert schedule.fire_count == 1
    assert schedule.skipped_count == 1
    assert schedule.coalesced_count == 4
    assert schedule.next_run_at_ms == due_at_ms + 6 * every_ms
  end

  test "a queued overlap fires once and coalesces intervals missed while waiting" do
    due_at_ms = 55_000
    every_ms = 100
    recovery_ms = due_at_ms + 10 * every_ms
    schedule_id = unique_flow_id("schedule-catchup-overlap-queue")
    target_prefix = unique_flow_id("schedule-catchup-overlap-queue-target")
    target_type = unique_flow_id("schedule-catchup-overlap-queue-type")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: every_ms,
               start_at_ms: due_at_ms,
               now_ms: due_at_ms,
               overlap_policy: :queue_after_previous,
               overlap_retry_ms: every_ms,
               target: [id_prefix: target_prefix, type: target_type]
             )

    assert {:ok, %{claimed: 1, fired: 1, coalesced: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: due_at_ms,
               worker: "schedule-catchup-test"
             )

    first_target_id = "#{target_prefix}:#{due_at_ms}:1"

    assert {:ok, %{claimed: 1, fired: 0, skipped: 1, coalesced: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: due_at_ms + every_ms,
               worker: "schedule-catchup-test"
             )

    assert {:ok, [first_target]} =
             FerricStore.flow_claim_due(target_type,
               worker: "schedule-catchup-target-worker",
               limit: 1,
               now_ms: recovery_ms
             )

    assert first_target.id == first_target_id

    assert :ok =
             FerricStore.flow_complete(first_target.id, first_target.lease_token,
               fencing_token: first_target.fencing_token,
               now_ms: recovery_ms
             )

    assert {:ok, %{claimed: 1, fired: 1, coalesced: 9, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: recovery_ms,
               worker: "schedule-catchup-test"
             )

    queued_due_at_ms = due_at_ms + every_ms
    second_target_id = "#{target_prefix}:#{queued_due_at_ms}:2"
    assert {:ok, %{id: ^second_target_id}} = FerricStore.flow_get(second_target_id)

    assert {:ok, schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert schedule.fire_count == 2
    assert schedule.coalesced_count == 9
    assert schedule.overlap_queued_due_at_ms == nil
    assert schedule.next_run_at_ms == recovery_ms + every_ms
  end

  test "resuming an overdue interval coalesces the paused periods into one fire" do
    due_at_ms = 57_000
    every_ms = 100
    recovery_ms = due_at_ms + 10 * every_ms
    schedule_id = unique_flow_id("schedule-catchup-pause-resume")
    target_prefix = unique_flow_id("schedule-catchup-pause-resume-target")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: every_ms,
               start_at_ms: due_at_ms,
               now_ms: due_at_ms - 1,
               target: [
                 id_prefix: target_prefix,
                 type: unique_flow_id("schedule-catchup-pause-resume-type")
               ]
             )

    assert {:ok, %{state: "paused"}} =
             FerricStore.flow_schedule_pause(schedule_id, now_ms: due_at_ms)

    assert {:ok, %{claimed: 0, fired: 0, coalesced: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: recovery_ms,
               worker: "schedule-catchup-test"
             )

    assert {:ok, %{state: "active", next_run_at_ms: ^due_at_ms}} =
             FerricStore.flow_schedule_resume(schedule_id, now_ms: recovery_ms)

    assert {:ok, %{claimed: 1, fired: 1, coalesced: 10, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: recovery_ms,
               worker: "schedule-catchup-test"
             )

    first_target_id = "#{target_prefix}:#{due_at_ms}:1"
    assert {:ok, %{id: ^first_target_id}} = FerricStore.flow_get(first_target_id)

    assert {:ok, schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert schedule.fire_count == 1
    assert schedule.coalesced_count == 10
    assert schedule.next_run_at_ms == recovery_ms + every_ms
  end

  test "an overlap skip completes when catch-up passes the interval end bound" do
    due_at_ms = 60_000
    every_ms = 100
    schedule_id = unique_flow_id("schedule-catchup-overlap-end")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: every_ms,
               start_at_ms: due_at_ms,
               now_ms: due_at_ms,
               end_at_ms: due_at_ms + 5 * every_ms,
               overlap_policy: :skip,
               target: [
                 id_prefix: unique_flow_id("schedule-catchup-overlap-end-target"),
                 type: unique_flow_id("schedule-catchup-overlap-end-type")
               ]
             )

    assert {:ok, %{claimed: 1, fired: 1, coalesced: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: due_at_ms,
               worker: "schedule-catchup-test"
             )

    assert {:ok, %{claimed: 1, fired: 0, skipped: 1, coalesced: 4, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: due_at_ms + 10 * every_ms,
               worker: "schedule-catchup-test"
             )

    assert {:ok, schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert schedule.state == "completed"
    assert schedule.end_reason == "end_at_ms"
    assert schedule.fire_count == 1
    assert schedule.skipped_count == 1
    assert schedule.coalesced_count == 4
    assert schedule.next_run_at_ms == nil
  end

  test "an overlap skip completes safely at the exact timestamp limit" do
    max_exact_integer = 9_007_199_254_740_991
    due_at_ms = max_exact_integer - 10
    every_ms = 6
    schedule_id = unique_flow_id("schedule-catchup-overlap-timestamp-limit")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: every_ms,
               start_at_ms: due_at_ms,
               now_ms: due_at_ms,
               overlap_policy: :skip,
               target: [
                 id_prefix: unique_flow_id("schedule-catchup-overlap-limit-target"),
                 type: unique_flow_id("schedule-catchup-overlap-limit-type")
               ]
             )

    assert {:ok, %{claimed: 1, fired: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: due_at_ms,
               lease_ms: 1,
               worker: "schedule-catchup-test"
             )

    assert {:ok, %{claimed: 1, fired: 0, skipped: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: due_at_ms + every_ms,
               lease_ms: 1,
               worker: "schedule-catchup-test"
             )

    assert {:ok, schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert schedule.state == "completed"
    assert schedule.end_reason == "timestamp_limit"
    assert schedule.fire_count == 1
    assert schedule.skipped_count == 1
    assert schedule.next_run_at_ms == nil
  end

  test "an elapsed end bound takes precedence over recovery timestamp overflow" do
    max_exact_integer = 9_007_199_254_740_991
    recovery_ms = max_exact_integer - 1
    due_at_ms = recovery_ms - 20
    every_ms = 5
    end_at_ms = due_at_ms + 2 * every_ms

    assert {:complete, :end_at_ms, 2} =
             Catchup.next_interval(
               %{kind: :interval, every_ms: every_ms, end_at_ms: end_at_ms},
               due_at_ms,
               recovery_ms
             )
  end

  test "max_fires keeps precedence when the end bound is reached by the same fire" do
    due_at_ms = 65_000
    schedule_id = unique_flow_id("schedule-catchup-simultaneous-bounds")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: 100,
               start_at_ms: due_at_ms,
               end_at_ms: due_at_ms,
               max_fires: 1,
               now_ms: due_at_ms - 1,
               target: [
                 id_prefix: unique_flow_id("schedule-catchup-simultaneous-target"),
                 type: unique_flow_id("schedule-catchup-simultaneous-type")
               ]
             )

    assert {:ok, %{claimed: 1, fired: 1, coalesced: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: due_at_ms,
               worker: "schedule-catchup-test"
             )

    assert {:ok, schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert schedule.state == "completed"
    assert schedule.end_reason == "max_fires"
  end

  test "a batch catch-up total stays within the public exact-integer contract" do
    max_exact_integer = 9_007_199_254_740_991
    recovery_ms = max_exact_integer - 1

    for suffix <- ["a", "b"] do
      assert {:ok, _schedule} =
               FerricStore.flow_schedule_create(
                 unique_flow_id("schedule-catchup-batch-limit-#{suffix}"),
                 kind: :interval,
                 every_ms: 1,
                 start_at_ms: 0,
                 now_ms: 0,
                 target: [
                   id_prefix: unique_flow_id("schedule-catchup-batch-target-#{suffix}"),
                   type: unique_flow_id("schedule-catchup-batch-type-#{suffix}")
                 ]
               )
    end

    assert {:ok,
            %{
              claimed: 2,
              fired: 2,
              coalesced: ^max_exact_integer,
              errors: []
            }} =
             FerricStore.flow_schedule_fire_due(
               now_ms: recovery_ms,
               lease_ms: 1,
               limit: 2,
               worker: "schedule-catchup-test"
             )
  end

  test "catchup_policy accepts only fire_once on interval schedules" do
    target = [
      id_prefix: unique_flow_id("schedule-catchup-validation-target"),
      type: unique_flow_id("schedule-catchup-validation-type")
    ]

    assert {:ok, %{catchup_policy: :fire_once}} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-catchup-explicit"),
               kind: :interval,
               every_ms: 1_000,
               catchup_policy: :fire_once,
               target: target
             )

    assert {:error, "ERR flow schedule catchup_policy must be :fire_once"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-catchup-invalid"),
               kind: :interval,
               every_ms: 1_000,
               catchup_policy: :buffer_one,
               target: target
             )

    assert {:error, "ERR flow schedule catchup_policy is only supported for interval schedules"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-catchup-cron"),
               kind: :cron,
               cron: "*/5 * * * *",
               catchup_policy: :fire_once,
               target: target
             )
  end
end
