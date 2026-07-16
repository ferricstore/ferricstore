defmodule Ferricstore.Flow.ScheduleTest do
  use Ferricstore.Test.FlowCase

  test "one-shot schedule fires target flow once and completes schedule" do
    now_ms = 1_000
    schedule_id = unique_flow_id("schedule-once")
    target_id = unique_flow_id("schedule-target-once")
    target_type = unique_flow_id("schedule-target-type")
    target_partition = unique_flow_id("schedule-target-partition")

    assert {:ok, schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms + 100,
               now_ms: now_ms,
               target: [
                 id: target_id,
                 type: target_type,
                 state: "queued",
                 partition_key: target_partition,
                 payload: %{source: "schedule"}
               ]
             )

    assert schedule.id == schedule_id
    assert schedule.kind == :one_shot
    assert schedule.next_run_at_ms == now_ms + 100

    assert {:ok, %{fired: 0, claimed: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 99, worker: "schedule-test")

    assert {:ok, nil} = FerricStore.flow_get(target_id, partition_key: target_partition)

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 100, worker: "schedule-test")

    assert {:ok, target} =
             FerricStore.flow_get(target_id, partition_key: target_partition, payload: true)

    assert target.type == target_type
    assert target.state == "queued"
    assert target.payload == %{source: "schedule"}

    assert {:ok, fired_schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert fired_schedule.state == "completed"

    assert {:ok, %{fired: 0, claimed: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 200, worker: "schedule-test")
  end

  test "interval schedule reschedules without consuming retry attempts" do
    now_ms = 2_000
    schedule_id = unique_flow_id("schedule-interval")
    target_prefix = unique_flow_id("schedule-target-interval")
    target_type = unique_flow_id("schedule-interval-type")
    target_partition = unique_flow_id("schedule-interval-partition")

    assert {:ok, schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               start_at_ms: now_ms,
               every_ms: 500,
               now_ms: now_ms - 100,
               target: [
                 id_prefix: target_prefix,
                 type: target_type,
                 state: "queued",
                 partition_key: target_partition,
                 payload: "tick"
               ]
             )

    assert schedule.next_run_at_ms == now_ms

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "schedule-test")

    first_target_id = "#{target_prefix}:#{now_ms}:1"

    assert {:ok, first_target} =
             FerricStore.flow_get(first_target_id, partition_key: target_partition, payload: true)

    assert first_target.payload == "tick"

    assert {:ok, after_first} = FerricStore.flow_schedule_get(schedule_id)
    assert after_first.state == "active"
    assert after_first.fire_count == 1
    assert after_first.next_run_at_ms == now_ms + 500
    assert after_first.attempts == 0

    assert {:ok, %{fired: 0, claimed: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 499, worker: "schedule-test")

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 500, worker: "schedule-test")

    second_target_id = "#{target_prefix}:#{now_ms + 500}:2"

    assert {:ok, second_target} =
             FerricStore.flow_get(second_target_id,
               partition_key: target_partition,
               payload: true
             )

    assert second_target.payload == "tick"

    assert {:ok, after_second} = FerricStore.flow_schedule_get(schedule_id)
    assert after_second.fire_count == 2
    assert after_second.next_run_at_ms == now_ms + 1_000
  end

  test "concurrent schedule firing leases a due schedule once" do
    now_ms = 2_500
    schedule_id = unique_flow_id("schedule-concurrent")
    target_id = unique_flow_id("schedule-concurrent-target")
    target_partition = unique_flow_id("schedule-concurrent-partition")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms,
               now_ms: now_ms,
               target: [
                 id: target_id,
                 type: unique_flow_id("schedule-concurrent-type"),
                 partition_key: target_partition,
                 payload: "fire-once"
               ]
             )

    results =
      1..8
      |> Task.async_stream(
        fn worker ->
          FerricStore.flow_schedule_fire_due(
            now_ms: now_ms,
            worker: "schedule-concurrent-#{worker}"
          )
        end,
        max_concurrency: 8,
        timeout: 10_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(results, &match?({:ok, %{errors: []}}, &1))
    assert results |> Enum.map(fn {:ok, result} -> result.fired end) |> Enum.sum() == 1
    assert results |> Enum.map(fn {:ok, result} -> result.claimed end) |> Enum.sum() == 1

    assert {:ok, target} =
             FerricStore.flow_get(target_id, partition_key: target_partition, payload: true)

    assert target.payload == "fire-once"
    assert {:ok, %{state: "completed"}} = FerricStore.flow_schedule_get(schedule_id)
  end

  test "recurring schedule rejects fixed target id" do
    assert {:error, "ERR recurring schedule target must use id_prefix, not id"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-invalid"),
               kind: :interval,
               every_ms: 1_000,
               target: [id: "fixed-target", type: "scheduled"]
             )
  end

  test "public flow APIs reject internal schedule namespace" do
    internal_id = "__ferricstore_schedule__:manual"

    assert {:error, "ERR flow type is reserved for internal use"} =
             FerricStore.flow_create(unique_flow_id("reserved-type"),
               type: "__ferricstore_schedule"
             )

    assert {:error, "ERR flow id is reserved for internal use"} =
             FerricStore.flow_create(internal_id, type: "regular")

    assert {:error, "ERR flow id is reserved for internal use"} =
             FerricStore.flow_get(internal_id)

    assert {:error, "ERR flow type is reserved for internal use"} =
             FerricStore.flow_claim_due("__ferricstore_schedule", worker: "public-worker")
  end

  test "public callers cannot forge the internal schedule capability" do
    forged_opts = [__ferricstore_internal__: true]

    refute Ferricstore.Flow.Internal.allowed?(forged_opts)
    assert Ferricstore.Flow.Internal.allowed?(Ferricstore.Flow.Internal.put([]))

    assert {:error, "ERR flow type is reserved for internal use"} =
             FerricStore.flow_create(unique_flow_id("forged-reserved-type"),
               type: "__ferricstore_schedule",
               __ferricstore_internal__: true
             )

    assert {:error, "ERR flow id is reserved for internal use"} =
             FerricStore.flow_get("__ferricstore_schedule__:forged",
               __ferricstore_internal__: true
             )

    assert {:error, "ERR flow type is reserved for internal use"} =
             FerricStore.flow_claim_due("__ferricstore_schedule",
               worker: "forged-public-worker",
               __ferricstore_internal__: true
             )
  end

  test "schedule APIs reject non-keyword option lists without raising" do
    schedule_id = unique_flow_id("schedule-invalid-opts")
    invalid_opts = [:not_a_keyword_pair]
    error = {:error, "ERR flow schedule opts must be a keyword list"}

    assert ^error = FerricStore.flow_schedule_create(schedule_id, invalid_opts)
    assert ^error = FerricStore.flow_schedule_get(schedule_id, invalid_opts)
    assert ^error = FerricStore.flow_schedule_fire(schedule_id, invalid_opts)
    assert ^error = FerricStore.flow_schedule_pause(schedule_id, invalid_opts)
    assert ^error = FerricStore.flow_schedule_resume(schedule_id, invalid_opts)
    assert ^error = FerricStore.flow_schedule_list(invalid_opts)
    assert ^error = FerricStore.flow_schedule_delete(schedule_id, invalid_opts)
    assert ^error = FerricStore.flow_schedule_fire_due(invalid_opts)
  end

  test "manual fire without a timestamp uses the current command time" do
    schedule_id = unique_flow_id("schedule-manual-default-time")
    target_prefix = unique_flow_id("schedule-manual-default-time-target")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: 0,
               now_ms: 0,
               target: [
                 id_prefix: target_prefix,
                 type: unique_flow_id("schedule-manual-default-time-type")
               ]
             )

    assert {:ok, %{fired: 1, target_id: target_id}} =
             FerricStore.flow_schedule_fire(schedule_id)

    assert String.starts_with?(target_id, target_prefix <> ":")
  end

  test "schedule get keeps internal routing and payload options authoritative" do
    schedule_id = unique_flow_id("schedule-get-protected-options")

    assert {:ok, created} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: 1_000,
               now_ms: 0,
               target: [
                 id: unique_flow_id("schedule-get-protected-options-target"),
                 type: unique_flow_id("schedule-get-protected-options-type")
               ]
             )

    assert {:ok, fetched} =
             FerricStore.flow_schedule_get(schedule_id,
               partition_key: "wrong-partition",
               payload: false,
               payload_max_bytes: 1
             )

    assert fetched.id == created.id
    assert fetched.kind == :one_shot
    assert fetched.target == created.target
  end

  test "schedule rejects large inline target payloads" do
    assert {:error, "ERR flow schedule payload too large; use payload_ref/value_refs"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-large-payload"),
               kind: :one_shot,
               target: [
                 id: unique_flow_id("schedule-large-target"),
                 type: unique_flow_id("schedule-large-type"),
                 payload: String.duplicate("x", 9_000)
               ]
             )
  end

  test "schedule rejects large inline target named values" do
    assert {:error, "ERR flow schedule values too large; use values_ref/value_refs"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-large-values"),
               kind: :one_shot,
               target: [
                 id: unique_flow_id("schedule-large-values-target"),
                 type: unique_flow_id("schedule-large-values-type"),
                 values: %{"doc" => String.duplicate("x", 9_000)}
               ]
             )
  end

  test "schedule rejects reserved target ids and prefixes" do
    assert {:error, "ERR scheduled target id is reserved for internal use"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-reserved-target-id"),
               kind: :one_shot,
               target: [
                 id: "__ferricstore_schedule__:target",
                 type: unique_flow_id("schedule-reserved-target-id-type")
               ]
             )

    assert {:error, "ERR scheduled target id_prefix is reserved for internal use"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-reserved-target-prefix"),
               kind: :interval,
               every_ms: 1_000,
               target: [
                 id_prefix: "__ferricstore_schedule__:target",
                 type: unique_flow_id("schedule-reserved-target-prefix-type")
               ]
             )
  end

  test "schedule rejects target definitions that cannot create a flow" do
    assert {:error, "ERR flow type is reserved for internal use"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-reserved-target-type"),
               kind: :one_shot,
               target: [
                 id: unique_flow_id("schedule-reserved-target-type-id"),
                 type: "__ferricstore_schedule"
               ]
             )

    assert {:error, "ERR flow priority must be between 0 and 2"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-invalid-target-priority"),
               kind: :interval,
               every_ms: 1_000,
               target: [
                 id_prefix: unique_flow_id("schedule-invalid-target-priority-prefix"),
                 type: unique_flow_id("schedule-invalid-target-priority-type"),
                 priority: "high"
               ]
             )
  end

  test "schedule time inputs are bounded and cron conversion never raises" do
    max_exact_integer = 9_007_199_254_740_991

    overflow_error =
      {:error, "ERR flow schedule start_at_ms exceeds maximum #{max_exact_integer}"}

    assert ^overflow_error =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-cron-time-overflow"),
               kind: :cron,
               cron: "* * * * *",
               start_at_ms: max_exact_integer + 1,
               now_ms: 0,
               target: [
                 id_prefix: unique_flow_id("schedule-cron-time-overflow-target"),
                 type: unique_flow_id("schedule-cron-time-overflow-type")
               ]
             )

    assert {:error, "ERR flow schedule timestamp is outside supported calendar range"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-cron-calendar-overflow"),
               kind: :cron,
               cron: "* * * * *",
               start_at_ms: max_exact_integer,
               now_ms: 0,
               target: [
                 id_prefix: unique_flow_id("schedule-cron-calendar-overflow-target"),
                 type: unique_flow_id("schedule-cron-calendar-overflow-type")
               ]
             )
  end

  test "duplicate target id must belong to the schedule before it is treated as idempotent" do
    now_ms = 3_000
    schedule_id = unique_flow_id("schedule-duplicate-owner")
    target_id = unique_flow_id("schedule-duplicate-target")
    target_partition = unique_flow_id("schedule-duplicate-partition")

    assert :ok =
             FerricStore.flow_create(target_id,
               type: "manual-owner",
               partition_key: target_partition,
               correlation_id: "manual",
               now_ms: now_ms
             )

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms,
               now_ms: now_ms,
               target: [
                 id: target_id,
                 type: "scheduled-owner",
                 partition_key: target_partition
               ]
             )

    assert {:ok, %{fired: 0, claimed: 1, errors: [{_flow_id, reason}]}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "schedule-test")

    assert reason == "ERR scheduled target id already exists with different owner"
  end

  test "existing target with scheduler owner is idempotent and finishes schedule fire" do
    now_ms = 3_500
    schedule_id = unique_flow_id("schedule-duplicate-owned")
    target_id = unique_flow_id("schedule-owned-target")
    target_type = unique_flow_id("schedule-owned-type")
    target_partition = unique_flow_id("schedule-owned-partition")
    owner_correlation = "__ferricstore_schedule__:" <> schedule_id

    assert :ok =
             FerricStore.flow_create(target_id,
               type: target_type,
               partition_key: target_partition,
               correlation_id: owner_correlation,
               payload: "already-created",
               now_ms: now_ms - 1
             )

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms,
               now_ms: now_ms - 1,
               target: [
                 id: target_id,
                 type: target_type,
                 partition_key: target_partition,
                 payload: "would-create"
               ]
             )

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "schedule-test")

    assert {:ok, target} =
             FerricStore.flow_get(target_id, partition_key: target_partition, payload: true)

    assert target.payload == "already-created"
    assert target.correlation_id == owner_correlation

    assert {:ok, fired_schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert fired_schedule.state == "completed"
    assert fired_schedule.fire_count == 1
  end

  test "duplicate schedule create returns already exists unless overwrite is explicit" do
    now_ms = 3_250
    schedule_id = unique_flow_id("schedule-duplicate")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms,
               now_ms: now_ms,
               target: [
                 id: unique_flow_id("schedule-duplicate-target"),
                 type: unique_flow_id("schedule-duplicate-type")
               ]
             )

    assert {:error, "ERR flow already exists"} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms + 1_000,
               now_ms: now_ms,
               target: [
                 id: unique_flow_id("schedule-duplicate-replacement-target"),
                 type: unique_flow_id("schedule-duplicate-replacement-type")
               ]
             )
  end

  test "schedule overwrite atomically replaces active definition" do
    now_ms = 3_500
    schedule_id = unique_flow_id("schedule-overwrite")
    old_target_id = unique_flow_id("schedule-overwrite-old-target")
    new_target_id = unique_flow_id("schedule-overwrite-new-target")
    old_partition = unique_flow_id("schedule-overwrite-old-partition")
    new_partition = unique_flow_id("schedule-overwrite-new-partition")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms + 100,
               now_ms: now_ms,
               target: [
                 id: old_target_id,
                 type: unique_flow_id("schedule-overwrite-old-type"),
                 partition_key: old_partition,
                 payload: "old"
               ]
             )

    assert {:ok, replacement} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms + 500,
               now_ms: now_ms + 1,
               overwrite: true,
               target: [
                 id: new_target_id,
                 type: unique_flow_id("schedule-overwrite-new-type"),
                 partition_key: new_partition,
                 payload: "new"
               ]
             )

    assert replacement.state == "active"
    assert replacement.next_run_at_ms == now_ms + 500
    assert replacement.fire_count == 0

    assert {:ok, %{fired: 0, claimed: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 100, worker: "schedule-test")

    assert {:ok, nil} = FerricStore.flow_get(old_target_id, partition_key: old_partition)

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 500, worker: "schedule-test")

    assert {:ok, target} =
             FerricStore.flow_get(new_target_id, partition_key: new_partition, payload: true)

    assert target.payload == "new"
  end

  test "schedule overwrite rejects a currently leased schedule" do
    now_ms = 3_600
    schedule_id = unique_flow_id("schedule-overwrite-leased")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms,
               now_ms: now_ms,
               target: [
                 id: unique_flow_id("schedule-overwrite-leased-target"),
                 type: unique_flow_id("schedule-overwrite-leased-type")
               ]
             )

    assert {:ok, [_claimed]} =
             Ferricstore.Flow.claim_due(
               FerricStore.Instance.get(:default),
               "__ferricstore_schedule",
               Ferricstore.Flow.Internal.put(
                 state: "active",
                 partition_key: :any,
                 worker: "schedule-overwrite-test",
                 limit: 1,
                 lease_ms: 30_000,
                 now_ms: now_ms,
                 payload: true
               )
             )

    assert {:error, "ERR flow schedule is currently leased"} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms + 1_000,
               now_ms: now_ms + 1,
               overwrite: true,
               target: [
                 id: unique_flow_id("schedule-overwrite-leased-replacement"),
                 type: unique_flow_id("schedule-overwrite-leased-replacement-type")
               ]
             )
  end

  test "schedule overwrite can reactivate a completed one-shot schedule" do
    now_ms = 3_700
    schedule_id = unique_flow_id("schedule-overwrite-completed")
    first_target_id = unique_flow_id("schedule-overwrite-completed-first")
    second_target_id = unique_flow_id("schedule-overwrite-completed-second")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms,
               now_ms: now_ms,
               target: [
                 id: first_target_id,
                 type: unique_flow_id("schedule-overwrite-completed-first-type"),
                 payload: "first"
               ]
             )

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "schedule-test")

    assert {:ok, %{state: "completed"}} = FerricStore.flow_schedule_get(schedule_id)

    assert {:ok, replacement} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms + 100,
               now_ms: now_ms + 1,
               overwrite: true,
               target: [
                 id: second_target_id,
                 type: unique_flow_id("schedule-overwrite-completed-second-type"),
                 payload: "second"
               ]
             )

    assert replacement.state == "active"
    assert replacement.next_run_at_ms == now_ms + 100

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 100, worker: "schedule-test")

    assert {:ok, second_target} = FerricStore.flow_get(second_target_id, payload: true)
    assert second_target.payload == "second"
  end

  test "schedule target can use payload_ref and value_refs" do
    now_ms = 3_750
    schedule_id = unique_flow_id("schedule-value-refs")
    target_id = unique_flow_id("schedule-value-ref-target")
    target_partition = unique_flow_id("schedule-value-ref-partition")

    assert {:ok, %{ref: payload_ref}} =
             FerricStore.flow_value_put(%{large: String.duplicate("p", 512)},
               partition_key: target_partition
             )

    assert {:ok, %{ref: doc_ref}} =
             FerricStore.flow_value_put(%{doc: "reservation"},
               partition_key: target_partition
             )

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms,
               now_ms: now_ms,
               target: [
                 id: target_id,
                 type: unique_flow_id("schedule-value-ref-type"),
                 partition_key: target_partition,
                 payload_ref: payload_ref,
                 value_refs: %{"reservation" => doc_ref}
               ]
             )

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "schedule-test")

    assert {:ok, target} =
             FerricStore.flow_get(target_id,
               partition_key: target_partition,
               payload: true,
               values: true
             )

    assert target.payload == %{large: String.duplicate("p", 512)}
    assert target.values == %{"reservation" => %{doc: "reservation"}}
    assert target.payload_ref == payload_ref
    assert get_in(target.value_refs, ["reservation", :ref]) == doc_ref
  end

  test "schedule list filters schedules by kind timezone target type and due range" do
    now_ms = 3_900
    cron_id = unique_flow_id("schedule-list-cron")
    interval_id = unique_flow_id("schedule-list-interval")
    target_type = unique_flow_id("schedule-list-target-type")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(cron_id,
               kind: :cron,
               cron: "0 9 * * *",
               timezone: "Asia/Jerusalem",
               start_at_ms: now_ms,
               now_ms: now_ms,
               target: [
                 id_prefix: unique_flow_id("schedule-list-cron-target"),
                 type: target_type
               ]
             )

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(interval_id,
               kind: :interval,
               every_ms: 1_000,
               start_at_ms: now_ms,
               now_ms: now_ms,
               target: [
                 id_prefix: unique_flow_id("schedule-list-interval-target"),
                 type: unique_flow_id("schedule-list-other-type")
               ]
             )

    assert {:ok, [schedule]} =
             FerricStore.flow_schedule_list(
               kind: :cron,
               timezone: "Asia/Jerusalem",
               target_type: target_type,
               from_ms: now_ms,
               count: 10
             )

    assert schedule.id == cron_id
    assert schedule.kind == :cron
    assert schedule.timezone == "Asia/Jerusalem"
  end

  test "schedule list applies reverse ordering before count" do
    now_ms = 3_950
    id_prefix = unique_flow_id("schedule-list-reverse")

    {earlier_id, later_id} =
      Enum.reduce_while(0..256, %{}, fn suffix, ids_by_bucket ->
        id = id_prefix <> ":" <> Integer.to_string(suffix)
        bucket = :erlang.phash2(id, 256)

        case Map.fetch(ids_by_bucket, bucket) do
          {:ok, existing_id} -> {:halt, {existing_id, id}}
          :error -> {:cont, Map.put(ids_by_bucket, bucket, id)}
        end
      end)

    target_type = unique_flow_id("schedule-list-reverse-target-type")

    for {id, at_ms} <- [{earlier_id, now_ms + 100}, {later_id, now_ms + 200}] do
      assert {:ok, _schedule} =
               FerricStore.flow_schedule_create(id,
                 kind: :one_shot,
                 at_ms: at_ms,
                 now_ms: now_ms,
                 target: [id_prefix: id <> ":target", type: target_type]
               )
    end

    assert {:ok, schedules} =
             FerricStore.flow_schedule_list(
               kind: :one_shot,
               target_type: target_type,
               from_ms: now_ms,
               to_ms: now_ms + 300,
               count: 1,
               rev: true
             )

    assert Enum.map(schedules, & &1.id) == [later_id]
  end

  test "schedule due ranges exclude terminal schedules without a next run" do
    now_ms = 3_975
    schedule_id = unique_flow_id("schedule-list-terminal-range")
    target_type = unique_flow_id("schedule-list-terminal-range-type")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms,
               now_ms: now_ms,
               target: [
                 id: unique_flow_id("schedule-list-terminal-range-target"),
                 type: target_type
               ]
             )

    assert {:ok, %{fired: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "schedule-test")

    assert {:ok, []} =
             FerricStore.flow_schedule_list(
               state: :all,
               target_type: target_type,
               from_ms: 0,
               to_ms: now_ms + 1,
               count: 10
             )
  end

  test "schedule list fails closed when a partition index is unavailable" do
    ctx = %{
      name: :schedule_unavailable_test,
      shard_names: {:schedule_missing_shard},
      slot_map: List.duplicate(0, 1_024) |> List.to_tuple()
    }

    assert {:error, "ERR flow index unavailable"} =
             Ferricstore.Flow.Schedule.list(ctx, count: 1)
  end

  test "recurring schedule completes after max_fires" do
    now_ms = 4_010
    schedule_id = unique_flow_id("schedule-max-fires")
    target_prefix = unique_flow_id("schedule-max-fires-target")
    target_type = unique_flow_id("schedule-max-fires-type")

    assert {:ok, schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: 100,
               start_at_ms: now_ms,
               now_ms: now_ms,
               max_fires: 2,
               target: [id_prefix: target_prefix, type: target_type]
             )

    assert schedule.max_fires == 2

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "schedule-test")

    assert {:ok, active} = FerricStore.flow_schedule_get(schedule_id)
    assert active.state == "active"
    assert active.fire_count == 1

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 100, worker: "schedule-test")

    assert {:ok, completed} = FerricStore.flow_schedule_get(schedule_id)
    assert completed.state == "completed"
    assert completed.fire_count == 2
    assert completed.end_reason == "max_fires"
    assert completed.next_run_at_ms == nil

    assert {:ok, nil} = FerricStore.flow_get("#{target_prefix}:#{now_ms + 200}:3")
  end

  test "recurring schedule completes when next fire would pass end_at_ms" do
    now_ms = 4_020
    schedule_id = unique_flow_id("schedule-end-at")
    target_prefix = unique_flow_id("schedule-end-at-target")
    target_type = unique_flow_id("schedule-end-at-type")

    assert {:ok, schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: 100,
               start_at_ms: now_ms,
               now_ms: now_ms,
               end_at_ms: now_ms + 100,
               target: [id_prefix: target_prefix, type: target_type]
             )

    assert schedule.end_at_ms == now_ms + 100

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "schedule-test")

    assert {:ok, active} = FerricStore.flow_schedule_get(schedule_id)
    assert active.state == "active"
    assert active.next_run_at_ms == now_ms + 100

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 100, worker: "schedule-test")

    assert {:ok, completed} = FerricStore.flow_schedule_get(schedule_id)
    assert completed.state == "completed"
    assert completed.fire_count == 2
    assert completed.end_reason == "end_at_ms"
    assert completed.next_run_at_ms == nil
  end

  test "recurring schedule completes when its next timestamp exceeds the exact limit" do
    max_exact_integer = 9_007_199_254_740_991
    schedule_id = unique_flow_id("schedule-timestamp-limit")
    target_prefix = unique_flow_id("schedule-timestamp-limit-target")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: 2,
               start_at_ms: max_exact_integer - 1,
               now_ms: max_exact_integer - 1,
               target: [
                 id_prefix: target_prefix,
                 type: unique_flow_id("schedule-timestamp-limit-type")
               ]
             )

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(
               now_ms: max_exact_integer - 1,
               lease_ms: 1,
               worker: "schedule-test"
             )

    assert {:ok, completed} = FerricStore.flow_schedule_get(schedule_id)
    assert completed.state == "completed"
    assert completed.fire_count == 1
    assert completed.end_reason == "timestamp_limit"
    assert completed.next_run_at_ms == nil
  end

  test "one-shot and delay schedules reject recurring end conditions" do
    target_type = unique_flow_id("schedule-end-condition-reject-type")

    assert {:error, "ERR flow schedule max_fires is only supported for recurring schedules"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-max-fires-reject"),
               kind: :one_shot,
               max_fires: 1,
               target: [id: unique_flow_id("schedule-max-fires-reject-target"), type: target_type]
             )

    assert {:error, "ERR flow schedule end_at_ms is only supported for recurring schedules"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-end-at-reject"),
               kind: :delay,
               delay_ms: 100,
               end_at_ms: 1_000,
               target: [id: unique_flow_id("schedule-end-at-reject-target"), type: target_type]
             )
  end

  test "manual schedule fire targets one schedule immediately" do
    now_ms = 4_030
    schedule_id = unique_flow_id("schedule-manual-fire")
    target_id = unique_flow_id("schedule-manual-fire-target")
    target_type = unique_flow_id("schedule-manual-fire-type")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms + 60_000,
               now_ms: now_ms,
               target: [id: target_id, type: target_type, payload: "manual"]
             )

    assert {:ok, %{fired: 1, target_id: ^target_id, schedule: %{state: "completed"}}} =
             FerricStore.flow_schedule_fire(schedule_id, now_ms: now_ms + 1)

    assert {:ok, target} = FerricStore.flow_get(target_id, payload: true)
    assert target.payload == "manual"

    assert {:ok, completed} = FerricStore.flow_schedule_get(schedule_id)
    assert completed.state == "completed"
    assert completed.fire_count == 1
  end

  test "pause and resume controls schedule firing" do
    now_ms = 4_035
    schedule_id = unique_flow_id("schedule-pause-resume")
    target_id = unique_flow_id("schedule-pause-resume-target")
    target_type = unique_flow_id("schedule-pause-resume-type")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms + 100,
               now_ms: now_ms,
               target: [id: target_id, type: target_type]
             )

    assert {:ok, paused} = FerricStore.flow_schedule_pause(schedule_id, now_ms: now_ms + 1)
    assert paused.state == "paused"

    assert {:ok, %{fired: 0, claimed: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 100, worker: "schedule-test")

    assert {:ok, nil} = FerricStore.flow_get(target_id)

    assert {:ok, active} = FerricStore.flow_schedule_resume(schedule_id, now_ms: now_ms + 2)
    assert active.state == "active"

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 100, worker: "schedule-test")

    assert {:ok, target} = FerricStore.flow_get(target_id)
    assert target.type == target_type
  end

  test "schedule lifecycle writes explicit signal history events" do
    now_ms = 4_040
    schedule_id = unique_flow_id("schedule-history-events")
    target_id = unique_flow_id("schedule-history-events-target")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms,
               now_ms: now_ms,
               target: [id: target_id, type: unique_flow_id("schedule-history-events-type")]
             )

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "schedule-test")

    assert {:ok, history} =
             FerricStore.flow_history(Ferricstore.Flow.Schedule.flow_id(schedule_id),
               partition_key: schedule_partition_key(schedule_id),
               count: 20
             )

    signals =
      history
      |> Enum.map(fn {_event_id, event} -> Map.get(event, "signal") end)
      |> Enum.reject(&is_nil/1)

    assert "schedule_created" in signals
    assert "schedule_fired" in signals
  end

  test "overlap policy skip advances recurring schedule without creating a new target" do
    now_ms = 4_100
    schedule_id = unique_flow_id("schedule-overlap-skip")
    target_prefix = unique_flow_id("schedule-overlap-skip-target")
    target_type = unique_flow_id("schedule-overlap-skip-type")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: 100,
               start_at_ms: now_ms,
               now_ms: now_ms,
               overlap_policy: :skip,
               target: [id_prefix: target_prefix, type: target_type]
             )

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "schedule-test")

    assert {:ok, %{fired: 0, skipped: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 100, worker: "schedule-test")

    assert {:ok, nil} = FerricStore.flow_get("#{target_prefix}:#{now_ms + 100}:2")

    assert {:ok, schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert schedule.fire_count == 1
    assert schedule.skipped_count == 1
    assert schedule.next_run_at_ms == now_ms + 200
    assert schedule.last_overlap_target_id == "#{target_prefix}:#{now_ms}:1"
  end

  test "overlap policy queue_after_previous preserves queued due occurrence" do
    now_ms = 4_300
    schedule_id = unique_flow_id("schedule-overlap-queue")
    target_prefix = unique_flow_id("schedule-overlap-queue-target")
    target_type = unique_flow_id("schedule-overlap-queue-type")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: 100,
               start_at_ms: now_ms,
               now_ms: now_ms,
               overlap_policy: :queue_after_previous,
               overlap_retry_ms: 50,
               target: [id_prefix: target_prefix, type: target_type]
             )

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "schedule-test")

    first_target_id = "#{target_prefix}:#{now_ms}:1"

    assert {:ok, %{fired: 0, skipped: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 100, worker: "schedule-test")

    assert {:ok, queued} = FerricStore.flow_schedule_get(schedule_id)
    assert queued.next_run_at_ms == now_ms + 150
    assert queued.overlap_queued_due_at_ms == now_ms + 100

    assert {:ok, [job]} =
             FerricStore.flow_claim_due(target_type,
               worker: "schedule-overlap-worker",
               limit: 1,
               now_ms: now_ms + 100
             )

    assert job.id == first_target_id

    assert :ok =
             FerricStore.flow_complete(job.id, job.lease_token, fencing_token: job.fencing_token)

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 150, worker: "schedule-test")

    assert {:ok, second_target} = FerricStore.flow_get("#{target_prefix}:#{now_ms + 100}:2")
    assert second_target.type == target_type
  end

  test "overlap policy fail_schedule marks recurring schedule failed" do
    now_ms = 4_500
    schedule_id = unique_flow_id("schedule-overlap-fail")
    target_prefix = unique_flow_id("schedule-overlap-fail-target")
    target_type = unique_flow_id("schedule-overlap-fail-type")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :interval,
               every_ms: 100,
               start_at_ms: now_ms,
               now_ms: now_ms,
               overlap_policy: :fail_schedule,
               target: [id_prefix: target_prefix, type: target_type]
             )

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms, worker: "schedule-test")

    assert {:ok, %{fired: 0, claimed: 1, errors: [{_flow_id, reason}]}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 100, worker: "schedule-test")

    assert reason =~ "previous target still active"

    assert {:ok, schedule} = FerricStore.flow_schedule_get(schedule_id)
    assert schedule.state == "failed"
  end

  test "schedule delete cancels future schedule" do
    now_ms = 4_000
    schedule_id = unique_flow_id("schedule-delete")
    target_id = unique_flow_id("schedule-delete-target")

    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :one_shot,
               at_ms: now_ms + 1_000,
               now_ms: now_ms,
               target: [id: target_id, type: unique_flow_id("schedule-delete-type")]
             )

    assert :ok = FerricStore.flow_schedule_delete(schedule_id, now_ms: now_ms + 1)

    assert {:ok, deleted} = FerricStore.flow_schedule_get(schedule_id)
    assert deleted.state == "cancelled"

    assert {:ok, history} =
             FerricStore.flow_history(Ferricstore.Flow.Schedule.flow_id(schedule_id),
               partition_key: schedule_partition_key(schedule_id),
               count: 20
             )

    assert Enum.any?(history, fn {_event_id, event} ->
             Map.get(event, "signal") == "schedule_deleted"
           end)

    assert {:ok, %{fired: 0, claimed: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: now_ms + 1_000, worker: "schedule-test")
  end

  test "invalid cron is rejected during schedule creation" do
    assert {:error, "ERR flow schedule cron value out of range"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-invalid-cron"),
               kind: :cron,
               cron: "99 * * * *",
               target: [id_prefix: unique_flow_id("schedule-invalid-cron-target"), type: "cron"]
             )
  end

  test "invalid cron range is rejected during schedule creation" do
    assert {:error, "ERR flow schedule cron range is invalid"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-invalid-cron-range"),
               kind: :cron,
               cron: "10-5 * * * *",
               target: [
                 id_prefix: unique_flow_id("schedule-invalid-cron-range-target"),
                 type: "cron"
               ]
             )
  end

  test "cron schedule supports aliases, ranges, and steps" do
    schedule_id = unique_flow_id("schedule-cron-alias-step")
    target_prefix = unique_flow_id("schedule-target-cron-alias-step")
    target_partition = unique_flow_id("schedule-cron-alias-step-partition")

    assert {:ok, schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :cron,
               cron: "0-30/15 0 1 JAN ?",
               start_at_ms: 0,
               now_ms: 0,
               target: [
                 id_prefix: target_prefix,
                 type: unique_flow_id("schedule-cron-alias-step-type"),
                 partition_key: target_partition,
                 payload: "cron-step"
               ]
             )

    assert schedule.next_run_at_ms == 0

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: 0, worker: "schedule-test")

    assert {:ok, after_first} = FerricStore.flow_schedule_get(schedule_id)
    assert after_first.next_run_at_ms == 15 * 60_000

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: 15 * 60_000, worker: "schedule-test")

    assert {:ok, after_second} = FerricStore.flow_schedule_get(schedule_id)
    assert after_second.next_run_at_ms == 30 * 60_000

    assert {:ok, first_target} =
             FerricStore.flow_get("#{target_prefix}:0:1",
               partition_key: target_partition,
               payload: true
             )

    assert {:ok, second_target} =
             FerricStore.flow_get("#{target_prefix}:#{15 * 60_000}:2",
               partition_key: target_partition,
               payload: true
             )

    assert first_target.payload == "cron-step"
    assert second_target.payload == "cron-step"
  end

  test "cron schedule supports weekday aliases" do
    assert {:ok, schedule} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-cron-weekday"),
               kind: :cron,
               cron: "0 0 ? JAN THU",
               start_at_ms: 0,
               now_ms: 0,
               target: [
                 id_prefix: unique_flow_id("schedule-cron-weekday-target"),
                 type: unique_flow_id("schedule-cron-weekday-type")
               ]
             )

    assert schedule.next_run_at_ms == 0
  end

  test "cron schedule matches wall-clock time in configured timezone" do
    schedule_id = unique_flow_id("schedule-cron-timezone")
    start_ms = DateTime.to_unix(~U[2026-01-01 00:00:00Z], :millisecond)
    expected_ms = DateTime.to_unix(~U[2026-01-01 07:00:00Z], :millisecond)

    assert {:ok, schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :cron,
               cron: "0 9 * * *",
               timezone: "Asia/Jerusalem",
               start_at_ms: start_ms,
               now_ms: start_ms,
               target: [
                 id_prefix: unique_flow_id("schedule-cron-timezone-target"),
                 type: unique_flow_id("schedule-cron-timezone-type")
               ]
             )

    assert schedule.next_run_at_ms == expected_ms
  end

  test "cron schedule handles repeated DST wall-clock minute as distinct due times" do
    schedule_id = unique_flow_id("schedule-cron-dst-repeat")
    first_due_ms = DateTime.to_unix(~U[2026-11-01 05:30:00Z], :millisecond)
    second_due_ms = DateTime.to_unix(~U[2026-11-01 06:30:00Z], :millisecond)

    assert {:ok, schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :cron,
               cron: "30 1 1 NOV *",
               timezone: "America/New_York",
               start_at_ms: DateTime.to_unix(~U[2026-11-01 05:00:00Z], :millisecond),
               now_ms: DateTime.to_unix(~U[2026-11-01 05:00:00Z], :millisecond),
               target: [
                 id_prefix: unique_flow_id("schedule-cron-dst-repeat-target"),
                 type: unique_flow_id("schedule-cron-dst-repeat-type")
               ]
             )

    assert schedule.next_run_at_ms == first_due_ms

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: first_due_ms, worker: "schedule-test")

    assert {:ok, after_first} = FerricStore.flow_schedule_get(schedule_id)
    assert after_first.next_run_at_ms == second_due_ms
  end

  test "due schedule is not missed after hibernation removes its hot due key" do
    ctx = FerricStore.Instance.get(:default)
    schedule_id = unique_flow_id("schedule-hibernation-precheck-race")
    partition_key = schedule_partition_key(schedule_id)

    state_key =
      Ferricstore.Flow.Keys.state_key(
        Ferricstore.Flow.Schedule.flow_id(schedule_id),
        partition_key
      )

    shard_index = Ferricstore.Store.Router.shard_for(ctx, state_key)
    writer_name = Ferricstore.Flow.LMDBWriter.name(ctx.name, shard_index)
    writer = Process.whereis(writer_name)
    assert is_pid(writer)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)

    original_flush_interval_ms = :sys.get_state(writer).flush_interval_ms

    :sys.replace_state(writer, fn state ->
      if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
      %{state | flush_interval_ms: 60_000, timer_ref: nil}
    end)

    acceptor =
      :wa_raft_acceptor.registered_name(:ferricstore_waraft_backend, shard_index + 1)

    try do
      now_ms = DateTime.to_unix(~U[2026-11-01 05:00:00Z], :millisecond)
      due_ms = now_ms + 30 * 60_000

      assert {:ok, schedule} =
               FerricStore.flow_schedule_create(schedule_id,
                 kind: :one_shot,
                 at_ms: due_ms,
                 now_ms: now_ms,
                 target: [
                   id: unique_flow_id("schedule-hibernation-precheck-target"),
                   type: unique_flow_id("schedule-hibernation-precheck-type")
                 ]
               )

      assert schedule.next_run_at_ms == due_ms
      :ok = :sys.suspend(acceptor)

      task =
        Task.async(fn ->
          FerricStore.flow_schedule_fire_due(now_ms: due_ms, worker: "schedule-test")
        end)

      try do
        acceptor_pid = Process.whereis(acceptor)

        assert eventually(fn ->
                 match?(
                   {:message_queue_len, count} when count > 0,
                   Process.info(acceptor_pid, :message_queue_len)
                 )
               end)

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)
        :ok = :sys.resume(acceptor)

        assert {:ok, %{fired: 1, claimed: 1, errors: []}} = Task.await(task, 10_000)
      after
        resume_process(acceptor)

        if Process.alive?(task.pid) do
          Task.shutdown(task, :brutal_kill)
        end
      end
    after
      assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)

      :sys.replace_state(writer, fn state ->
        if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
        %{state | flush_interval_ms: original_flush_interval_ms, timer_ref: nil}
      end)
    end
  end

  test "cron schedule rejects invalid timezone" do
    assert {:error, "ERR flow schedule timezone is invalid or unavailable"} =
             FerricStore.flow_schedule_create(unique_flow_id("schedule-cron-invalid-timezone"),
               kind: :cron,
               cron: "0 9 * * *",
               timezone: "Mars/Olympus",
               target: [
                 id_prefix: unique_flow_id("schedule-cron-invalid-timezone-target"),
                 type: unique_flow_id("schedule-cron-invalid-timezone-type")
               ]
             )
  end

  test "cron schedule fires on matching UTC minutes and reschedules to next match" do
    schedule_id = unique_flow_id("schedule-cron")
    target_prefix = unique_flow_id("schedule-target-cron")
    target_type = unique_flow_id("schedule-cron-type")
    target_partition = unique_flow_id("schedule-cron-partition")

    assert {:ok, schedule} =
             FerricStore.flow_schedule_create(schedule_id,
               kind: :cron,
               cron: "*/5 * * * *",
               start_at_ms: 0,
               now_ms: 0,
               target: [
                 id_prefix: target_prefix,
                 type: target_type,
                 state: "queued",
                 partition_key: target_partition,
                 payload: "cron"
               ]
             )

    assert schedule.kind == :cron
    assert schedule.next_run_at_ms == 0

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: 0, worker: "schedule-test")

    first_target_id = "#{target_prefix}:0:1"

    assert {:ok, first_target} =
             FerricStore.flow_get(first_target_id, partition_key: target_partition, payload: true)

    assert first_target.payload == "cron"

    assert {:ok, after_first} = FerricStore.flow_schedule_get(schedule_id)
    assert after_first.state == "active"
    assert after_first.fire_count == 1
    assert after_first.next_run_at_ms == 5 * 60_000

    assert {:ok, %{fired: 0, claimed: 0, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: 5 * 60_000 - 1, worker: "schedule-test")

    assert {:ok, %{fired: 1, claimed: 1, errors: []}} =
             FerricStore.flow_schedule_fire_due(now_ms: 5 * 60_000, worker: "schedule-test")

    second_target_id = "#{target_prefix}:#{5 * 60_000}:2"

    assert {:ok, second_target} =
             FerricStore.flow_get(second_target_id,
               partition_key: target_partition,
               payload: true
             )

    assert second_target.payload == "cron"
  end

  defp schedule_partition_key(id) do
    "__ferricstore_schedule__:" <> Integer.to_string(:erlang.phash2(id, 256))
  end

  defp resume_process(name) do
    :sys.resume(name)
  catch
    :exit, _reason -> :ok
  end
end
