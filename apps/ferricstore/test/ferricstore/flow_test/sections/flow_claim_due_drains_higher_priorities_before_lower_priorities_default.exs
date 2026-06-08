defmodule Ferricstore.FlowTest.Sections.FlowClaimDueDrainsHigherPrioritiesBeforeLowerPrioritiesDefault do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

  test "flow_claim_due drains higher priorities before lower priorities by default" do
    low_id = uid("flow-low-priority")
    high_id = uid("flow-high-priority")

    assert {:ok, _} =
             flow_create_and_get(low_id,
               type: "priority",
               priority: 0,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(high_id,
               type: "priority",
               priority: 2,
               run_at_ms: 1_000
             )

    assert {:ok, [high]} =
             FerricStore.flow_claim_due("priority",
               worker: "worker-priority",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert high.id == high_id

    assert {:ok, [low]} =
             FerricStore.flow_claim_due("priority",
               worker: "worker-priority",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert low.id == low_id
  end

  test "flow_claim_due priority option targets one priority band" do
    low_id = uid("flow-low-priority-target")
    high_id = uid("flow-high-priority-target")

    assert {:ok, _} =
             flow_create_and_get(low_id,
               type: "priority-target",
               priority: 0,
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             flow_create_and_get(high_id,
               type: "priority-target",
               priority: 2,
               run_at_ms: 1_000
             )

    assert {:ok, [low]} =
             FerricStore.flow_claim_due("priority-target",
               worker: "worker-priority",
               lease_ms: 30_000,
               limit: 1,
               priority: 0,
               now_ms: 1_000
             )

    assert low.id == low_id

    assert {:ok, [high]} =
             FerricStore.flow_claim_due("priority-target",
               worker: "worker-priority",
               lease_ms: 30_000,
               limit: 1,
               priority: 2,
               now_ms: 1_000
             )

    assert high.id == high_id
  end

  test "flow_complete enforces lease token guard and writes terminal state" do
    id = uid("flow-complete")

    assert {:ok, _} =
             flow_create_and_get(id, type: "image", state: "queued", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("image",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR stale flow lease"} =
             flow_complete_and_get(id, "wrong-token",
               fencing_token: claimed.fencing_token,
               result: "result:" <> id
             )

    assert {:error, "ERR stale flow lease"} =
             flow_complete_and_get(id, claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               result: "result:" <> id
             )

    assert {:ok, completed} =
             flow_complete_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               result: "result:" <> id
             )

    assert completed.state == "completed"
    assert is_binary(completed.result_ref)
    assert completed.result_ref != "result:" <> id
    assert completed.lease_token == nil
    assert completed.version == 3
  end

  test "terminal flows reject normal transition and cancel" do
    id = uid("flow-terminal-guard")

    assert {:ok, _} =
             flow_create_and_get(id, type: "image", state: "queued", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("image",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, completed} =
             flow_complete_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               result: "result:" <> id
             )

    assert completed.state == "completed"

    assert {:error, "ERR flow is terminal; use FLOW.REWIND"} =
             flow_transition_and_get(id, "completed", "queued",
               fencing_token: completed.fencing_token
             )

    assert {:error, "ERR flow is terminal; use FLOW.REWIND"} =
             flow_cancel_and_get(id, fencing_token: completed.fencing_token)

    assert {:ok, still_completed} = FerricStore.flow_get(id)
    assert still_completed.state == "completed"
  end

  test "flow_retry clears lease and reschedules flow" do
    id = uid("flow-retry")

    assert {:ok, _} =
             flow_create_and_get(id, type: "webhook", state: "queued", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("webhook",
               state: "queued",
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR stale flow lease"} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token + 1,
               error: "error:" <> id,
               run_at_ms: 2_000
             )

    assert {:ok, retried} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               error: "error:" <> id,
               run_at_ms: 2_000
             )

    assert retried.state == "queued"
    assert retried.attempts == 1
    assert is_binary(retried.error_ref)
    assert retried.error_ref != "error:" <> id
    assert retried.lease_token == nil

    assert {:ok, [reclaimed]} =
             FerricStore.flow_claim_due("webhook",
               state: "queued",
               worker: "worker-b",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2_000
             )

    assert reclaimed.id == id
    assert reclaimed.lease_owner == "worker-b"
  end

  test "flow_claim_due promotes hibernated cold due flow" do
    ctx = FerricStore.Instance.get(:default)
    id = uid("flow-cold-due-claim")
    type = uid("cold-due-type") <> ":bench"
    partition_key = uid("cold-due-partition")
    now_ms = System.system_time(:millisecond)
    run_at_ms = now_ms + 301_000

    assert {:ok, created} =
             flow_create_and_get(id,
               type: type,
               state: "waiting",
               partition_key: partition_key,
               now_ms: now_ms,
               run_at_ms: run_at_ms
             )

    assert created.next_run_at_ms == run_at_ms
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    shard_index = shard_for(state_key)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    park_key = Ferricstore.Flow.LMDB.cold_park_key_for_state_key(state_key)
    assert {:ok, park_blob} = Ferricstore.Flow.LMDB.get(lmdb_path, park_key)

    assert {:ok, %{locator: locator, state_value: state_value} = park} =
             Ferricstore.Flow.LMDB.decode_cold_park(park_blob)

    assert is_binary(state_value)

    bad_locator = %{locator | file_id: {:waraft_apply_projection, 999_999_999}}

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
               {:put, park_key,
                Ferricstore.Flow.LMDB.encode_cold_park(
                  bad_locator,
                  Map.delete(park, :locator)
                )}
             ])

    assert {:ok, fetched_cold} =
             FerricStore.flow_get(id, flow_partition_opts(partition_key))

    assert fetched_cold.id == id

    assert {:ok, []} =
             FerricStore.flow_claim_due(type,
               state: "waiting",
               partition_key: partition_key,
               worker: "worker-before-due",
               lease_ms: 30_000,
               limit: 1,
               now_ms: run_at_ms - 1
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               state: "waiting",
               partition_key: partition_key,
               worker: "worker-after-due",
               lease_ms: 30_000,
               limit: 1,
               now_ms: run_at_ms
             )

    assert claimed.id == id
    assert claimed.lease_owner == "worker-after-due"

    list_id = uid("flow-cold-due-list-claim")

    assert {:ok, _created} =
             flow_create_and_get(list_id,
               type: type,
               state: "waiting",
               partition_key: partition_key,
               now_ms: 10_000,
               run_at_ms: 312_000
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    assert {:ok, [list_claimed]} =
             FerricStore.flow_claim_due(type,
               state: :any,
               partition_keys: [partition_key],
               worker: "worker-list-after-due",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 312_000
             )

    assert list_claimed.id == list_id
    assert list_claimed.lease_owner == "worker-list-after-due"

    auto_id = uid("flow-cold-due-auto-claim")
    auto_partition = Ferricstore.Flow.Keys.auto_partition_key(auto_id)

    assert {:ok, auto_created} =
             flow_create_and_get(auto_id,
               type: type,
               state: "waiting",
               now_ms: 20_000,
               run_at_ms: 322_000
             )

    assert auto_created.partition_key == auto_partition
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    assert {:ok, [auto_claimed]} =
             FerricStore.flow_claim_due(type,
               state: :any,
               partition_keys: [auto_partition],
               worker: "worker-auto-after-due",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 322_000
             )

    assert auto_claimed.id == auto_id
    assert auto_claimed.lease_owner == "worker-auto-after-due"

    blocking_id = uid("flow-cold-due-blocking-auto-claim")
    blocking_partition = Ferricstore.Flow.Keys.auto_partition_key(blocking_id)

    assert {:ok, _created} =
             flow_create_and_get(blocking_id,
               type: type,
               state: "waiting",
               now_ms: 30_000,
               run_at_ms: 332_000
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    assert {:ok, [blocking_claimed]} =
             FerricStore.flow_claim_due(type,
               state: :any,
               partition_keys: [blocking_partition],
               worker: "worker-blocking-auto-after-due",
               lease_ms: 30_000,
               limit: 1,
               block_ms: 1,
               now_ms: 332_000
             )

    assert blocking_claimed.id == blocking_id
    assert blocking_claimed.lease_owner == "worker-blocking-auto-after-due"

    many_ids = Enum.map(1..64, &uid("flow-cold-due-many-auto-#{&1}"))
    many_items = Enum.map(many_ids, &%{id: &1})

    many_partitions =
      many_ids |> Enum.map(&Ferricstore.Flow.Keys.auto_partition_key/1) |> Enum.uniq()

    assert {:ok, created_many} =
             flow_create_many_and_get(nil, many_items,
               type: type,
               state: "waiting",
               now_ms: 40_000,
               run_at_ms: 342_000,
               independent: true
             )

    assert length(created_many) == 64
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    claimed_many_ids =
      Enum.reduce_while(1..8, MapSet.new(), fn attempt, acc ->
        {:ok, claimed_many} =
          FerricStore.flow_claim_due(type,
            state: :any,
            partition_keys: many_partitions,
            worker: "worker-many-auto-after-due",
            lease_ms: 30_000,
            limit: 64,
            now_ms: 342_000 + attempt
          )

        next = Enum.reduce(claimed_many, acc, &MapSet.put(&2, &1.id))

        if MapSet.size(next) == length(many_ids) do
          {:halt, next}
        else
          {:cont, next}
        end
      end)

    assert MapSet.new(many_ids) == claimed_many_ids

    grouped_type = uid("cold-due-grouped-type") <> ":bench"
    grouped_ids = Enum.map(1..64, &uid("flow-cold-due-sdk-bucket-#{&1}"))

    grouped_ids
    |> Enum.group_by(&Ferricstore.Flow.Keys.auto_partition_key/1)
    |> Enum.each(fn {bucket, ids} ->
      items = Enum.map(ids, &%{id: &1})

      assert {:ok, created_group} =
               flow_create_many_and_get(bucket, items,
                 type: grouped_type,
                 state: "waiting",
                 now_ms: 50_000,
                 run_at_ms: 352_000,
                 independent: true
               )

      assert length(created_group) == length(ids)
    end)

    grouped_partitions =
      grouped_ids |> Enum.map(&Ferricstore.Flow.Keys.auto_partition_key/1) |> Enum.uniq()

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    grouped_claimed_ids =
      Enum.reduce_while(1..8, MapSet.new(), fn attempt, acc ->
        {:ok, claimed_grouped} =
          FerricStore.flow_claim_due(grouped_type,
            state: :any,
            partition_keys: grouped_partitions,
            worker: "worker-sdk-bucket-after-due",
            lease_ms: 30_000,
            limit: 64,
            now_ms: 352_000 + attempt
          )

        next = Enum.reduce(claimed_grouped, acc, &MapSet.put(&2, &1.id))

        if MapSet.size(next) == length(grouped_ids) do
          {:halt, next}
        else
          {:cont, next}
        end
      end)

    assert MapSet.new(grouped_ids) == grouped_claimed_ids

    epoch_id = uid("flow-cold-due-epoch-auto-claim")
    epoch_type = uid("cold-due-epoch-type") <> ":bench"
    epoch_partition = Ferricstore.Flow.Keys.auto_partition_key(epoch_id)
    epoch_now_ms = 1_780_318_697_811
    epoch_run_at_ms = epoch_now_ms + 301_000

    assert {:ok, _created} =
             flow_create_and_get(epoch_id,
               type: epoch_type,
               state: "waiting",
               now_ms: epoch_now_ms,
               run_at_ms: epoch_run_at_ms
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    assert {:ok, [epoch_claimed]} =
             FerricStore.flow_claim_due(epoch_type,
               state: :any,
               partition_keys: [epoch_partition],
               worker: "worker-epoch-auto-after-due",
               lease_ms: 30_000,
               limit: 1,
               now_ms: epoch_run_at_ms
             )

    assert epoch_claimed.id == epoch_id
    assert epoch_claimed.lease_owner == "worker-epoch-auto-after-due"
  end

  test "blocking claim_due schedules wake from cold due rows" do
    ctx = FerricStore.Instance.get(:default)
    id = uid("flow-cold-due-waiter")
    type = uid("cold-due-waiter-type") <> ":bench"
    state = "waiting"
    partition_key = uid("cold-due-waiter-partition")
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    shard_index = shard_for(state_key)
    due_at_ms = System.system_time(:millisecond) + 30_000

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    locator =
      Ferricstore.Flow.Locator.new!(
        flow_id: id,
        kind: :state,
        version: 1,
        raft_index: 1,
        file_id: 0,
        offset: 0,
        value_size: 0
      )

    park_key = Ferricstore.Flow.LMDB.cold_park_key_for_state_key(state_key)

    due_key =
      Ferricstore.Flow.LMDB.cold_due_key(
        type: type,
        state: state,
        partition_key: partition_key,
        priority: 0,
        due_at_ms: due_at_ms,
        flow_id: id,
        version: 1
      )

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
               {:put, park_key,
                Ferricstore.Flow.LMDB.encode_cold_park(locator,
                  due_at_ms: due_at_ms,
                  type: type,
                  state: state,
                  partition_key: partition_key,
                  state_key: state_key,
                  priority: 0
                )},
               {:put, due_key, park_key}
             ])

    before_count = Ferricstore.Flow.ClaimWaiters.scheduled_count()

    assert :ok =
             Ferricstore.Flow.schedule_claim_due_waiter_next_due(ctx, type,
               partition_keys: [partition_key],
               priority: 0
             )

    assert Ferricstore.Flow.ClaimWaiters.scheduled_count() > before_count

    scheduled_due_at_ms = div(due_at_ms + 9, 10) * 10

    Ferricstore.Flow.ClaimWaiters.notify_scheduled_ready(
      {type, :any, 0, partition_key, scheduled_due_at_ms}
    )
  end

  test "hibernation schedules due wake for existing claim waiter" do
    type = uid("cold-due-existing-waiter-type") <> ":bench"
    partition_key = uid("cold-due-existing-waiter-partition")
    due_at_ms = Ferricstore.CommandTime.now_ms() + 100

    opts = [
      state: :any,
      partition_keys: [partition_key],
      priority: 0,
      limit: 10
    ]

    assert {:ok, keys, limit} = Ferricstore.Flow.claim_due_wait_registration(type, opts)

    assert :ok =
             Ferricstore.Flow.ClaimWaiters.register(keys, self(), 0, limit: limit)

    try do
      assert :ok =
               Ferricstore.Flow.Hibernation.maybe_schedule_claim_waiter(%{
                 type: type,
                 state: "waiting",
                 priority: 0,
                 partition_key: partition_key,
                 next_run_at_ms: due_at_ms
               })

      assert_receive {:flow_claim_due_wake, :ready}, 1_000
    after
      Ferricstore.Flow.ClaimWaiters.unregister(keys, self())
    end
  end

  test "flow_retry returns to claimed run_state with computed backoff" do
    id = uid("flow-retry-run-state")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: "payment",
               state: "charge_card",
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("payment",
               state: "charge_card",
               worker: "worker-charge",
               limit: 1,
               lease_ms: 30_000,
               now_ms: 1_000
             )

    assert claimed.state == "running"
    assert claimed.run_state == "charge_card"

    assert {:ok, retried} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000,
               retry: [
                 max_retries: 3,
                 backoff: [kind: :exponential, base_ms: 1_000, max_ms: 30_000, jitter_pct: 0],
                 exhausted_to: "payment_failed"
               ]
             )

    assert retried.state == "charge_card"
    assert retried.attempts == 1
    assert retried.next_run_at_ms == 3_000
    assert retried.lease_token == nil

    assert {:ok, history} = FerricStore.flow_history(id, count: 10)

    {_event_id, retry_fields} =
      Enum.find(history, fn {_event_id, fields} -> fields["event"] == "retry" end)

    assert retry_fields["retry_decision"] == "scheduled"
    assert retry_fields["retry_run_state"] == "charge_card"
    assert retry_fields["retry_next_run_at_ms"] == "3000"
    assert retry_fields["retry_max_retries"] == "3"
    assert retry_fields["retry_backoff_kind"] == "exponential"
    assert retry_fields["retry_backoff_base_ms"] == "1000"
    assert retry_fields["retry_backoff_max_ms"] == "30000"
    assert retry_fields["retry_jitter_pct"] == "0"
    assert retry_fields["retry_exhausted_to"] == "payment_failed"

    assert {:ok, []} =
             FerricStore.flow_claim_due("payment",
               state: "charge_card",
               worker: "worker-charge-b",
               limit: 1,
               now_ms: 2_999
             )

    assert {:ok, [reclaimed]} =
             FerricStore.flow_claim_due("payment",
               state: "charge_card",
               worker: "worker-charge-b",
               limit: 1,
               now_ms: 3_000
             )

    assert reclaimed.id == id
    assert reclaimed.run_state == "charge_card"
  end

  test "flow_retry exhausts to configured active state" do
    id = uid("flow-retry-exhaust")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: "payment-exhaust",
               state: "charge_card",
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("payment-exhaust",
               state: "charge_card",
               worker: "worker-charge",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, exhausted} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000,
               retry: [
                 max_retries: 0,
                 backoff: [kind: :fixed, base_ms: 10_000, max_ms: 10_000, jitter_pct: 0],
                 exhausted_to: "payment_failed"
               ]
             )

    assert exhausted.state == "payment_failed"
    assert exhausted.attempts == 1
    assert exhausted.next_run_at_ms == 2_000
    assert exhausted.lease_token == nil

    assert {:ok, [manual]} =
             FerricStore.flow_claim_due("payment-exhaust",
               state: "payment_failed",
               worker: "worker-manual",
               limit: 1,
               now_ms: 2_000
             )

    assert manual.id == id
  end

  test "flow_retry terminal exhaustion keeps stable audit metadata" do
    id = uid("flow-retry-terminal-exhaust")

    assert {:ok, _} =
             flow_create_and_get(id,
               type: "payment-terminal-exhaust",
               state: "charge_card",
               run_at_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("payment-terminal-exhaust",
               state: "charge_card",
               worker: "worker-charge",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, exhausted} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000,
               retry: [
                 max_retries: 0,
                 backoff: [kind: :fixed, base_ms: 10_000, max_ms: 10_000, jitter_pct: 0],
                 exhausted_to: "failed"
               ]
             )

    assert exhausted.state == "failed"
    assert exhausted.next_run_at_ms == nil

    assert {:ok, history} = FerricStore.flow_history(id, count: 10)

    {_event_id, retry_fields} =
      Enum.find(history, fn {_event_id, fields} -> fields["event"] == "retry" end)

    assert retry_fields["retry_decision"] == "exhausted"
    assert retry_fields["retry_next_run_at_ms"] == ""
    assert retry_fields["retry_exhausted_to"] == "failed"
  end

  test "flow_retry terminal exhaustion updates cross-shard parent child group" do
    parent = uid("flow-retry-parent-cross")
    child = uid("flow-retry-child-cross")
    {partition, _same_partition, other_partition} = mixed_partition_keys()

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition
             )

    assert {:ok, waiting} =
             flow_spawn_children_and_get(
               parent,
               [%{id: child, type: "child", partition_key: other_partition}],
               group_id: "retry-fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :fail_parent,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert waiting.state == "waiting_children"
    claimed = create_claimed_flow_child(child, other_partition, "worker-retry-cross")

    assert {:ok, exhausted_child} =
             flow_retry_and_get(child, claimed.lease_token,
               partition_key: other_partition,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000,
               retry: [max_retries: 0, exhausted_to: "failed"]
             )

    assert exhausted_child.state == "failed"

    assert {:ok, failed_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert failed_parent.state == "children_failed"
    assert failed_parent.child_groups["retry-fanout"]["children"][child] == "failed"
    assert failed_parent.child_groups["retry-fanout"]["summary"]["failed"] == 1
  end

  test "flow_retry rejects invalid retry policy" do
    id = uid("flow-retry-policy-invalid")

    assert {:ok, _} =
             flow_create_and_get(id, type: "retry-policy-invalid", run_at_ms: 1_000)

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("retry-policy-invalid",
               worker: "worker-invalid",
               limit: 1,
               now_ms: 1_000
             )

    assert {:error, "ERR flow retry max_retries must be between 0 and 1000"} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               retry: [max_retries: 1001]
             )

    assert {:error, "ERR flow retry exhausted_to cannot be running"} =
             flow_retry_and_get(claimed.id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               retry: [exhausted_to: "running"]
             )
  end
    end
  end
end
