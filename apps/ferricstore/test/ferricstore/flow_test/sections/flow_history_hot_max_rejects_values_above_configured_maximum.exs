defmodule Ferricstore.FlowTest.Sections.FlowHistoryHotMaxRejectsValuesAboveConfiguredMaximum do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

      test "flow history hot max rejects values above configured maximum" do
        original = Application.get_env(:ferricstore, :flow_max_history_hot_max_events)
        Application.put_env(:ferricstore, :flow_max_history_hot_max_events, 2)

        on_exit(fn ->
          if is_nil(original) do
            Application.delete_env(:ferricstore, :flow_max_history_hot_max_events)
          else
            Application.put_env(:ferricstore, :flow_max_history_hot_max_events, original)
          end
        end)

        assert {:error, "ERR flow history_hot_max_events exceeds maximum 2"} =
                 flow_create_and_get(uid("flow-history-max"),
                   type: "audit-history-max",
                   history_hot_max_events: 3
                 )
      end

      test "flow create_many and transition_many reject oversized batches" do
        original = Application.get_env(:ferricstore, :flow_max_batch_items)
        Application.put_env(:ferricstore, :flow_max_batch_items, 2)

        on_exit(fn ->
          if is_nil(original) do
            Application.delete_env(:ferricstore, :flow_max_batch_items)
          else
            Application.put_env(:ferricstore, :flow_max_batch_items, original)
          end
        end)

        assert {:error, "ERR flow batch item count exceeds maximum 2"} =
                 flow_create_many_and_get("tenant-batch-cap", ["a", "b", "c"], type: "batch-cap")

        assert {:error, "ERR flow batch item count exceeds maximum 2"} =
                 flow_transition_many_and_get(
                   "tenant-batch-cap",
                   "queued",
                   "waiting",
                   [
                     %{id: "a", fencing_token: 0},
                     %{id: "b", fencing_token: 0},
                     %{id: "c", fencing_token: 0}
                   ]
                 )
      end

      test "flow claim_due rejects oversized limit" do
        original = Application.get_env(:ferricstore, :flow_max_claim_limit)
        Application.put_env(:ferricstore, :flow_max_claim_limit, 2)

        on_exit(fn ->
          if is_nil(original) do
            Application.delete_env(:ferricstore, :flow_max_claim_limit)
          else
            Application.put_env(:ferricstore, :flow_max_claim_limit, original)
          end
        end)

        assert {:error, "ERR flow limit exceeds maximum 2"} =
                 FerricStore.flow_claim_due("claim-limit-cap",
                   worker: "worker-a",
                   limit: 3
                 )

        assert {:error, "ERR flow limit exceeds maximum 2"} =
                 FerricStore.flow_reclaim("claim-limit-cap",
                   worker: "worker-a",
                   limit: 3
                 )
      end

      test "flow_rewind rejects trimmed target event with stale stream index" do
        id = uid("flow-rewind-trimmed")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: "rewind-trimmed",
                   run_at_ms: 1_000,
                   history_hot_max_events: 2,
                   history_max_events: 2,
                   now_ms: 1_000
                 )

        assert {:ok, [{created_event_id, _fields} | _]} = FerricStore.flow_history(id, count: 10)

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("rewind-trimmed",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, _completed} =
                 flow_complete_and_get(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: 2_000
                 )

        assert {:ok, events} = FerricStore.flow_history(id, count: 10)

        assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == [
                 "claimed",
                 "completed"
               ]

        assert {:error, "ERR flow rewind target event not found"} =
                 FerricStore.flow_rewind(id,
                   to_event: created_event_id,
                   expect_state: "completed",
                   now_ms: 3_000
                 )
      end

      test "flow_rewind restores a previous history state and reindexes atomically" do
        id = uid("flow-rewind")

        assert {:ok, _} = flow_create_and_get(id, type: "rewind", run_at_ms: 1_000, now_ms: 1_000)

        assert {:ok, [{created_event_id, %{"event" => "created", "state" => "queued"}} | _]} =
                 FerricStore.flow_history(id, count: 10)

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("rewind",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, completed} =
                 flow_complete_and_get(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: 2_000
                 )

        assert completed.state == "completed"
        assert {:ok, %{queued: 0, completed: 1}} = FerricStore.flow_info("rewind")

        assert :ok =
                 FerricStore.flow_rewind(id,
                   to_event: created_event_id,
                   run_at_ms: 5_000,
                   expect_state: "completed",
                   now_ms: 3_000
                 )

        assert {:ok, rewound} = FerricStore.flow_get(id)

        assert rewound.state == "queued"
        assert rewound.next_run_at_ms == 5_000
        assert rewound.lease_token == nil
        assert rewound.lease_owner == nil
        assert rewound.fencing_token == completed.fencing_token + 1

        assert {:ok, %{queued: 1, completed: 0}} = FerricStore.flow_info("rewind")

        assert {:ok, [claimed_again]} =
                 FerricStore.flow_claim_due("rewind",
                   worker: "worker-b",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 5_000
                 )

        assert claimed_again.id == id
        assert claimed_again.state == "running"

        assert {:ok, events} = FerricStore.flow_history(id, count: 10)

        assert Enum.any?(events, fn {_event_id, fields} ->
                 fields["event"] == "rewound" and
                   fields["rewound_to_event_id"] == created_event_id
               end)
      end

      test "flow_rewind validates target, expected state, and active leases" do
        id = uid("flow-rewind-guard")

        assert {:ok, _} = flow_create_and_get(id, type: "rewind-guard", run_at_ms: 1_000)
        assert {:ok, [{created_event_id, _fields} | _]} = FerricStore.flow_history(id, count: 10)

        assert {:error, "ERR flow wrong state"} =
                 FerricStore.flow_rewind(id,
                   to_event: created_event_id,
                   expect_state: "completed"
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("rewind-guard",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:error, "ERR flow cannot rewind leased flow"} =
                 FerricStore.flow_rewind(id, to_event: created_event_id)

        assert {:ok, _} =
                 flow_complete_and_get(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: 2_000
                 )

        assert {:error, "ERR flow rewind target event not found"} =
                 FerricStore.flow_rewind(id, to_event: "999999-0")
      end

      test "flow_rewind rejects parent and child flows" do
        parent = uid("flow-rewind-parent")
        child = uid("flow-rewind-child")
        {partition, _same_partition, other_partition} = mixed_partition_keys()

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition,
                   now_ms: 1_000
                 )

        assert {:ok, [{parent_created_event_id, _fields} | _]} =
                 FerricStore.flow_history(parent, partition_key: partition, count: 10)

        assert {:ok, _waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child, type: "child", partition_key: other_partition}],
                   group_id: "rewind-fanout",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :ignore,
                   on_parent_closed: :abandon_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token,
                   now_ms: 2_000
                 )

        assert {:ok, [{child_created_event_id, _fields} | _]} =
                 FerricStore.flow_history(child, partition_key: other_partition, count: 10)

        assert {:error, "ERR flow cannot rewind parent or child flow"} =
                 FerricStore.flow_rewind(parent,
                   partition_key: partition,
                   to_event: parent_created_event_id,
                   expect_state: "waiting_children",
                   now_ms: 3_000
                 )

        assert {:error, "ERR flow cannot rewind parent or child flow"} =
                 FerricStore.flow_rewind(child,
                   partition_key: other_partition,
                   to_event: child_created_event_id,
                   expect_state: "running",
                   now_ms: 3_000
                 )
      end

      test "terminal retention from create expires flow state record" do
        id = uid("flow-terminal-ttl")

        assert {:ok, created} =
                 flow_create_and_get(id,
                   type: "ttl",
                   run_at_ms: 1_000,
                   retention_ttl_ms: 20,
                   now_ms: 1_000
                 )

        assert created.retention_ttl_ms == 20
        assert created.terminal_retention_until_ms == nil

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("ttl",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, _} =
                 flow_complete_and_get(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token
                 )

        Process.sleep(40)

        assert {:ok, nil} = FerricStore.flow_get(id)
      end

      test "terminal retention uses wall-valid command time" do
        id = uid("flow-terminal-command-time-ttl")
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 10_000

        assert {:ok, _created} =
                 flow_create_and_get(id,
                   type: "ttl-command-time",
                   run_at_ms: create_now,
                   retention_ttl_ms: 5_000,
                   now_ms: create_now
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("ttl-command-time",
                   worker: "worker-command-time",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now
                 )

        assert {:ok, completed} =
                 flow_complete_and_get(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: complete_now
                 )

        assert completed.terminal_retention_until_ms == complete_now + 5_000
      end

      test "retention cleanup revalidates stale LMDB terminal candidate after rewind" do
        ctx = FerricStore.Instance.get(:default)
        id = uid("flow-retention-rewind")
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000
        rewind_now = complete_now + 10
        cleanup_now = complete_now + 1_000

        assert {:ok, _created} =
                 flow_create_and_get(id,
                   type: "retention-rewind",
                   run_at_ms: create_now,
                   retention_ttl_ms: 20,
                   now_ms: create_now
                 )

        assert {:ok, [{created_event_id, %{"event" => "created"}} | _]} =
                 FerricStore.flow_history(id, count: 10)

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("retention-rewind",
                   worker: "worker-retention-rewind",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now
                 )

        assert {:ok, completed} =
                 flow_complete_and_get(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: complete_now
                 )

        assert completed.state == "completed"
        assert completed.terminal_retention_until_ms == complete_now + 20
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert :ok =
                 FerricStore.flow_rewind(id,
                   to_event: created_event_id,
                   run_at_ms: cleanup_now,
                   expect_state: "completed",
                   now_ms: rewind_now
                 )

        assert {:ok, %{state: "queued"}} = FerricStore.flow_get(id)

        assert {:ok, %{flows: 0}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: cleanup_now
                 })

        assert {:ok, %{state: "queued"}} = FerricStore.flow_get(id)
      end

      test "retention cleanup skips while async history projection is still pending" do
        old_flush = Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)
        old_batch = Application.get_env(:ferricstore, :flow_history_projector_batch_size)

        Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)
        Application.put_env(:ferricstore, :flow_history_projector_batch_size, 1_000_000)

        on_exit(fn ->
          restore_env(:flow_history_projector_flush_interval_ms, old_flush)
          restore_env(:flow_history_projector_batch_size, old_batch)
        end)

        ctx = FerricStore.Instance.get(:default)
        id = uid("flow-retention-pending-history")
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000
        cleanup_now = complete_now + 1_000

        shard_index = shard_for(Ferricstore.Flow.Keys.state_key(id, nil))
        projector = Ferricstore.Flow.HistoryProjector.name(ctx, shard_index)

        assert {:ok, _created} =
                 flow_create_and_get(id,
                   type: "retention-pending-history",
                   run_at_ms: create_now,
                   retention_ttl_ms: 10,
                   now_ms: create_now
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("retention-pending-history",
                   worker: "worker-retention-pending-history",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now
                 )

        assert {:ok, completed} =
                 flow_complete_and_get(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: complete_now
                 )

        assert completed.state == "completed"
        assert completed.terminal_retention_until_ms == complete_now + 10

        ShardHelpers.eventually(
          fn ->
            pid = Process.whereis(projector)
            is_pid(pid) and :sys.get_state(pid).pending_count > 0
          end,
          "history projector should have pending entries before retention cleanup"
        )

        assert {:ok, %{flows: 0, history: 0, values: 0}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: cleanup_now
                 })

        assert {:ok, %{state: "completed"}} = FerricStore.flow_get(id)

        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)

        assert {:ok, %{flows: 1}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: cleanup_now
                 })

        assert {:ok, nil} = FerricStore.flow_get(id)
      end

      test "retention cleanup preserves shared value refs used by another flow" do
        ctx = FerricStore.Instance.get(:default)
        id_a = uid("flow-retention-shared-a")
        id_b = uid("flow-retention-shared-b")
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000
        cleanup_now = complete_now + 1_000

        assert {:ok, %{ref: shared_ref}} =
                 FerricStore.flow_value_put("shared-doc", now_ms: create_now)

        for id <- [id_a, id_b] do
          assert {:ok, flow} =
                   flow_create_and_get(id,
                     type: "retention-shared-ref",
                     state: "queued",
                     value_refs: %{"doc" => shared_ref},
                     run_at_ms: create_now,
                     retention_ttl_ms: 20,
                     now_ms: create_now
                   )

          assert flow.value_refs["doc"].ref == shared_ref
        end

        assert {:ok, [claimed | _]} =
                 FerricStore.flow_claim_due("retention-shared-ref",
                   worker: "worker-retention-shared-ref",
                   lease_ms: 30_000,
                   limit: 2,
                   now_ms: create_now
                 )

        assert {:ok, completed} =
                 flow_complete_and_get(claimed.id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: complete_now
                 )

        assert completed.terminal_retention_until_ms == complete_now + 20

        shard_index = shard_for(Ferricstore.Flow.Keys.state_key(claimed.id, nil))
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, %{flows: 1}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: cleanup_now
                 })

        assert {:ok, ["shared-doc"]} = FerricStore.flow_value_mget([shared_ref])

        survivor_id = if claimed.id == id_a, do: id_b, else: id_a
        assert {:ok, survivor} = FerricStore.flow_get(survivor_id, values: true)
        assert survivor.values == %{"doc" => "shared-doc"}
      end

      test "retention cleanup does not global-scan refs for private payload values" do
        ctx = FerricStore.Instance.get(:default)
        id = uid("flow-retention-private-payload-scan")
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000
        cleanup_now = complete_now + 1_000
        parent = self()
        old_hook = Application.get_env(:ferricstore, :flow_retention_reference_scan_hook)

        Application.put_env(:ferricstore, :flow_retention_reference_scan_hook, fn record, refs ->
          send(parent, {:retention_reference_scan, Map.get(record, :id), refs})
          :ok
        end)

        on_exit(fn -> restore_env(:flow_retention_reference_scan_hook, old_hook) end)

        assert {:ok, created} =
                 flow_create_and_get(id,
                   type: "retention-private-payload-scan",
                   state: "queued",
                   payload: "private-payload",
                   run_at_ms: create_now,
                   retention_ttl_ms: 20,
                   now_ms: create_now
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("retention-private-payload-scan",
                   worker: "worker-retention-private-payload-scan",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now
                 )

        assert {:ok, completed} =
                 flow_complete_and_get(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: complete_now
                 )

        assert completed.terminal_retention_until_ms == complete_now + 20

        shard_index = shard_for(Ferricstore.Flow.Keys.state_key(id, nil))
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, %{flows: 1}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: cleanup_now
                 })

        refute_receive {:retention_reference_scan, ^id, _refs}, 50
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
        assert nil == Ferricstore.Store.Router.get(ctx, created.payload_ref)
      end

      test "retention cleanup reclaims unreferenced owner named shared values without scanning" do
        ctx = FerricStore.Instance.get(:default)
        id = uid("flow-retention-shared-value-lifecycle")
        partition_key = uid("tenant-retention-shared-value-lifecycle")
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000
        cleanup_now = complete_now + 1_000
        parent = self()
        old_hook = Application.get_env(:ferricstore, :flow_retention_reference_scan_hook)

        Application.put_env(:ferricstore, :flow_retention_reference_scan_hook, fn record, refs ->
          send(parent, {:retention_reference_scan, Map.get(record, :id), refs})
          :ok
        end)

        on_exit(fn -> restore_env(:flow_retention_reference_scan_hook, old_hook) end)

        assert {:ok, _created} =
                 flow_create_and_get(id,
                   type: "retention-shared-value-lifecycle",
                   partition_key: partition_key,
                   run_at_ms: create_now,
                   retention_ttl_ms: 20,
                   now_ms: create_now
                 )

        assert {:ok, %{ref: shared_ref}} =
                 FerricStore.flow_value_put("shared-doc",
                   partition_key: partition_key,
                   owner_flow_id: id,
                   name: "doc",
                   now_ms: create_now + 1
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("retention-shared-value-lifecycle",
                   partition_key: partition_key,
                   worker: "worker-retention-shared-value-lifecycle",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now + 2
                 )

        assert {:ok, completed} =
                 flow_complete_and_get(id, claimed.lease_token,
                   partition_key: partition_key,
                   fencing_token: claimed.fencing_token,
                   now_ms: complete_now
                 )

        assert completed.terminal_retention_until_ms == complete_now + 20

        shard_index = shard_for(Ferricstore.Flow.Keys.state_key(id, partition_key))
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, %{flows: 1}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: cleanup_now
                 })

        refute_receive {:retention_reference_scan, ^id, _refs}, 50
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
        assert {:ok, [nil]} = FerricStore.flow_value_mget([shared_ref])
      end

      test "retention cleanup preserves owner named values referenced by a live flow" do
        ctx = FerricStore.Instance.get(:default)
        owner_id = uid("flow-retention-owned-shared-owner")
        consumer_id = uid("flow-retention-owned-shared-consumer")
        {owner_partition, _same_partition, consumer_partition} = mixed_partition_keys()
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000
        cleanup_now = complete_now + 1_000

        assert {:ok, _owner} =
                 flow_create_and_get(owner_id,
                   type: "retention-owned-shared-owner",
                   state: "queued",
                   partition_key: owner_partition,
                   run_at_ms: create_now,
                   retention_ttl_ms: 20,
                   now_ms: create_now
                 )

        assert {:ok, %{ref: shared_ref}} =
                 FerricStore.flow_value_put("shared-doc",
                   partition_key: owner_partition,
                   owner_flow_id: owner_id,
                   name: "doc",
                   now_ms: create_now + 1
                 )

        assert {:ok, owner_with_value} =
                 FerricStore.flow_get(owner_id,
                   partition_key: owner_partition,
                   values: ["doc"]
                 )

        assert owner_with_value.values == %{"doc" => "shared-doc"}

        assert {:ok, consumer} =
                 flow_create_and_get(consumer_id,
                   type: "retention-owned-shared-consumer",
                   state: "queued",
                   partition_key: consumer_partition,
                   value_refs: %{"doc" => shared_ref},
                   run_at_ms: create_now,
                   retention_ttl_ms: 20,
                   now_ms: create_now + 2
                 )

        assert consumer.value_refs["doc"].ref == shared_ref

        assert shard_for(Ferricstore.Flow.Keys.state_key(owner_id, owner_partition)) !=
                 shard_for(Ferricstore.Flow.Keys.state_key(consumer_id, consumer_partition))

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("retention-owned-shared-owner",
                   partition_key: owner_partition,
                   worker: "worker-retention-owned-shared-owner",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now + 3
                 )

        assert {:ok, completed} =
                 flow_complete_and_get(owner_id, claimed.lease_token,
                   partition_key: owner_partition,
                   fencing_token: claimed.fencing_token,
                   now_ms: complete_now
                 )

        assert completed.terminal_retention_until_ms == complete_now + 20

        shard_index = shard_for(Ferricstore.Flow.Keys.state_key(owner_id, owner_partition))
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, %{flows: 1}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: cleanup_now
                 })

        assert {:ok, ["shared-doc"]} = FerricStore.flow_value_mget([shared_ref])

        assert {:ok, survivor} =
                 FerricStore.flow_get(consumer_id,
                   partition_key: consumer_partition,
                   values: ["doc"]
                 )

        assert survivor.values == %{"doc" => "shared-doc"}

        assert {:ok, [consumer_claim]} =
                 FerricStore.flow_claim_due("retention-owned-shared-consumer",
                   partition_key: consumer_partition,
                   worker: "worker-retention-owned-shared-consumer",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: cleanup_now + 1
                 )

        consumer_complete_now = cleanup_now + 2

        assert {:ok, _completed_consumer} =
                 flow_complete_and_get(consumer_id, consumer_claim.lease_token,
                   partition_key: consumer_partition,
                   fencing_token: consumer_claim.fencing_token,
                   now_ms: consumer_complete_now
                 )

        consumer_shard =
          shard_for(Ferricstore.Flow.Keys.state_key(consumer_id, consumer_partition))

        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, consumer_shard)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, %{flows: 1}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: consumer_complete_now + 100
                 })

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
        assert {:ok, [nil]} = FerricStore.flow_value_mget([shared_ref])
      end

      test "retention cleanup preserves an owner shared value used as a payload ref" do
        ctx = FerricStore.Instance.get(:default)
        owner_id = uid("flow-retention-shared-payload-owner")
        consumer_id = uid("flow-retention-shared-payload-consumer")
        {owner_partition, _same_partition, consumer_partition} = mixed_partition_keys()
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000

        assert {:ok, _owner} =
                 flow_create_and_get(owner_id,
                   type: "retention-shared-payload-owner",
                   state: "queued",
                   partition_key: owner_partition,
                   run_at_ms: create_now,
                   retention_ttl_ms: 20,
                   now_ms: create_now
                 )

        assert {:ok, %{ref: shared_ref}} =
                 FerricStore.flow_value_put("shared-payload",
                   partition_key: owner_partition,
                   owner_flow_id: owner_id,
                   name: "payload",
                   now_ms: create_now + 1
                 )

        assert {:ok, consumer} =
                 flow_create_and_get(consumer_id,
                   type: "retention-shared-payload-consumer",
                   state: "queued",
                   partition_key: consumer_partition,
                   payload_ref: shared_ref,
                   run_at_ms: create_now,
                   now_ms: create_now + 2
                 )

        assert consumer.payload_ref == shared_ref

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("retention-shared-payload-owner",
                   partition_key: owner_partition,
                   worker: "worker-retention-shared-payload-owner",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now + 3
                 )

        assert {:ok, _completed} =
                 flow_complete_and_get(owner_id, claimed.lease_token,
                   partition_key: owner_partition,
                   fencing_token: claimed.fencing_token,
                   now_ms: complete_now
                 )

        owner_shard = shard_for(Ferricstore.Flow.Keys.state_key(owner_id, owner_partition))
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, owner_shard)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, %{flows: 1}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: complete_now + 1_000
                 })

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
        assert {:ok, ["shared-payload"]} = FerricStore.flow_value_mget([shared_ref])
      end

      test "retention tracks named payload result and error shared refs" do
        ctx = FerricStore.Instance.get(:default)
        owner_id = uid("flow-retention-direct-ref-owner")
        {owner_partition, _same_partition, consumer_partition} = mixed_partition_keys()
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000

        assert {:ok, _owner} =
                 flow_create_and_get(owner_id,
                   type: "retention-direct-ref-owner",
                   state: "queued",
                   partition_key: owner_partition,
                   run_at_ms: create_now,
                   retention_ttl_ms: 20,
                   now_ms: create_now
                 )

        assert {:ok, %{ref: shared_ref}} =
                 FerricStore.flow_value_put("shared-direct-ref",
                   partition_key: owner_partition,
                   owner_flow_id: owner_id,
                   name: "doc",
                   now_ms: create_now + 1
                 )

        named_id = uid("flow-retention-direct-ref-named")
        payload_id = uid("flow-retention-direct-ref-payload")

        assert {:ok, named} =
                 flow_create_and_get(named_id,
                   type: "retention-direct-ref-named",
                   state: "queued",
                   partition_key: consumer_partition,
                   value_refs: %{"doc" => shared_ref},
                   run_at_ms: create_now,
                   now_ms: create_now + 2
                 )

        assert named.value_refs["doc"].ref == shared_ref

        assert {:ok, payload} =
                 flow_create_and_get(payload_id,
                   type: "retention-direct-ref-payload",
                   state: "queued",
                   partition_key: consumer_partition,
                   payload_ref: shared_ref,
                   run_at_ms: create_now,
                   now_ms: create_now + 3
                 )

        assert payload.payload_ref == shared_ref

        terminal_refs =
          for kind <- [:result, :error], into: %{} do
            id = uid("flow-retention-direct-ref-#{kind}")
            type = uid("retention-direct-ref-#{kind}-type")

            assert :ok =
                     FerricStore.flow_create(id,
                       type: type,
                       state: "queued",
                       partition_key: consumer_partition,
                       run_at_ms: create_now,
                       now_ms: create_now + 4
                     )

            assert {:ok, [claimed]} =
                     FerricStore.flow_claim_due(type,
                       partition_key: consumer_partition,
                       worker: "worker-retention-direct-ref-#{kind}",
                       limit: 1,
                       now_ms: create_now + 5
                     )

            ref_field = if(kind == :result, do: :result_ref, else: :error_ref)

            attrs = %{
              ref_field => shared_ref,
              id: id,
              lease_token: claimed.lease_token,
              fencing_token: claimed.fencing_token,
              partition_key: consumer_partition,
              now_ms: create_now + 6
            }

            result =
              case kind do
                :result -> Ferricstore.Store.Router.flow_complete(ctx, attrs)
                :error -> Ferricstore.Store.Router.flow_fail(ctx, attrs)
              end

            assert result == :ok
            assert {:ok, record} = FerricStore.flow_get(id, partition_key: consumer_partition)
            assert Map.fetch!(record, ref_field) == shared_ref
            {kind, id}
          end

        assert Map.keys(terminal_refs) |> Enum.sort() == [:error, :result]

        assert {:ok, [owner_claim]} =
                 FerricStore.flow_claim_due("retention-direct-ref-owner",
                   partition_key: owner_partition,
                   worker: "worker-retention-direct-ref-owner",
                   limit: 1,
                   now_ms: create_now + 7
                 )

        assert {:ok, _completed_owner} =
                 flow_complete_and_get(owner_id, owner_claim.lease_token,
                   partition_key: owner_partition,
                   fencing_token: owner_claim.fencing_token,
                   now_ms: complete_now
                 )

        for shard_index <- 0..(ctx.shard_count - 1) do
          assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
        end

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, %{flows: 1}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: complete_now + 1_000)

        assert {:ok, ["shared-direct-ref"]} = FerricStore.flow_value_mget([shared_ref])
      end

      test "bounded partial and full cleanup preserve a shared value with a live consumer" do
        ctx = FerricStore.Instance.get(:default)
        owner_id = uid("flow-retention-partial-shared-owner")
        consumer_id = uid("flow-retention-partial-shared-consumer")
        {owner_partition, _same_partition, consumer_partition} = mixed_partition_keys()
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000
        cleanup_now = complete_now + 1_000
        old_budget = Application.get_env(:ferricstore, :flow_retention_cleanup_key_budget)
        Application.put_env(:ferricstore, :flow_retention_cleanup_key_budget, 8)

        on_exit(fn -> restore_env(:flow_retention_cleanup_key_budget, old_budget) end)

        assert {:ok, _owner} =
                 flow_create_and_get(owner_id,
                   type: "retention-partial-shared-owner",
                   state: "queued",
                   partition_key: owner_partition,
                   run_at_ms: create_now,
                   retention_ttl_ms: 20,
                   now_ms: create_now
                 )

        assert {:ok, %{ref: shared_ref}} =
                 FerricStore.flow_value_put("shared-partial",
                   partition_key: owner_partition,
                   owner_flow_id: owner_id,
                   name: "doc",
                   now_ms: create_now + 1
                 )

        assert {:ok, _consumer} =
                 flow_create_and_get(consumer_id,
                   type: "retention-partial-shared-consumer",
                   state: "queued",
                   partition_key: consumer_partition,
                   value_refs: %{"doc" => shared_ref},
                   run_at_ms: create_now,
                   now_ms: create_now + 2
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("retention-partial-shared-owner",
                   partition_key: owner_partition,
                   worker: "worker-retention-partial-shared-owner",
                   limit: 1,
                   now_ms: create_now + 3
                 )

        assert {:ok, _completed} =
                 flow_complete_and_get(owner_id, claimed.lease_token,
                   partition_key: owner_partition,
                   fencing_token: claimed.fencing_token,
                   now_ms: complete_now
                 )

        owner_shard = shard_for(Ferricstore.Flow.Keys.state_key(owner_id, owner_partition))
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, owner_shard)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, %{flows: 0}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: cleanup_now
                 })

        assert {:ok, ["shared-partial"]} = FerricStore.flow_value_mget([shared_ref])

        assert {:ok, %{state: "completed"}} =
                 FerricStore.flow_get(owner_id, partition_key: owner_partition)

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, %{flows: 1}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: cleanup_now + 1
                 })

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
        assert {:ok, nil} = FerricStore.flow_get(owner_id, partition_key: owner_partition)
        assert {:ok, ["shared-partial"]} = FerricStore.flow_value_mget([shared_ref])
      end

      test "retention cleanup serializes with a racing shared value consumer" do
        ctx = FerricStore.Instance.get(:default)
        owner_id = uid("flow-retention-racing-shared-owner")
        consumer_id = uid("flow-retention-racing-shared-consumer")
        {owner_partition, _same_partition, consumer_partition} = mixed_partition_keys()
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000
        cleanup_now = complete_now + 1_000
        parent = self()
        release_ref = make_ref()

        assert {:ok, _owner} =
                 flow_create_and_get(owner_id,
                   type: "retention-racing-shared-owner",
                   state: "queued",
                   partition_key: owner_partition,
                   run_at_ms: create_now,
                   retention_ttl_ms: 20,
                   now_ms: create_now
                 )

        assert {:ok, %{ref: shared_ref}} =
                 FerricStore.flow_value_put("shared-doc",
                   partition_key: owner_partition,
                   owner_flow_id: owner_id,
                   name: "doc",
                   now_ms: create_now + 1
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("retention-racing-shared-owner",
                   partition_key: owner_partition,
                   worker: "worker-retention-racing-shared-owner",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now + 2
                 )

        assert {:ok, completed} =
                 flow_complete_and_get(owner_id, claimed.lease_token,
                   partition_key: owner_partition,
                   fencing_token: claimed.fencing_token,
                   now_ms: complete_now
                 )

        assert completed.terminal_retention_until_ms == complete_now + 20

        owner_shard = shard_for(Ferricstore.Flow.Keys.state_key(owner_id, owner_partition))

        consumer_shard =
          shard_for(Ferricstore.Flow.Keys.state_key(consumer_id, consumer_partition))

        assert owner_shard != consumer_shard
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, owner_shard)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        old_hook =
          Application.get_env(:ferricstore, :flow_shared_value_ref_acquisition_hook)

        Application.put_env(
          :ferricstore,
          :flow_shared_value_ref_acquisition_hook,
          fn record, refs ->
            if Map.get(record, :id) == consumer_id and shared_ref in refs do
              send(parent, {:shared_ref_acquisition_paused, self(), release_ref})

              receive do
                {:release_shared_ref_acquisition, ^release_ref} -> :ok
              after
                10_000 -> {:error, :shared_ref_acquisition_hook_timeout}
              end
            else
              :ok
            end
          end
        )

        on_exit(fn ->
          restore_env(:flow_shared_value_ref_acquisition_hook, old_hook)
        end)

        consumer_task =
          Task.async(fn ->
            flow_create_and_get(consumer_id,
              type: "retention-racing-shared-consumer",
              state: "queued",
              partition_key: consumer_partition,
              value_refs: %{"doc" => shared_ref},
              run_at_ms: create_now,
              retention_ttl_ms: 20,
              now_ms: create_now + 3
            )
          end)

        assert_receive {:shared_ref_acquisition_paused, apply_pid, ^release_ref}, 5_000

        cleanup_task =
          Task.async(fn ->
            Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
              limit: 10,
              now_ms: cleanup_now
            })
          end)

        cleanup_while_consumer_paused = Task.yield(cleanup_task, 1_000)
        send(apply_pid, {:release_shared_ref_acquisition, release_ref})

        assert {:ok, consumer} = Task.await(consumer_task, 10_000)
        assert consumer.value_refs["doc"].ref == shared_ref

        cleanup_result =
          case cleanup_while_consumer_paused do
            nil -> Task.await(cleanup_task, 10_000)
            {:ok, result} -> result
          end

        assert cleanup_while_consumer_paused == nil
        assert {:ok, %{flows: 1}} = cleanup_result
        assert {:ok, ["shared-doc"]} = FerricStore.flow_value_mget([shared_ref])
      end

      test "retention cleanup serializes with same-shard child shared ref acquisition" do
        ctx = FerricStore.Instance.get(:default)
        owner_id = uid("flow-retention-spawn-shared-owner")
        parent_id = uid("flow-retention-spawn-shared-parent")
        child_id = uid("flow-retention-spawn-shared-child")
        {parent_partition, child_partition, owner_partition} = mixed_partition_keys()
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000
        cleanup_now = complete_now + 1_000
        parent = self()
        release_ref = make_ref()

        assert shard_for(Ferricstore.Flow.Keys.state_key(parent_id, parent_partition)) ==
                 shard_for(Ferricstore.Flow.Keys.state_key(child_id, child_partition))

        assert {:ok, _owner} =
                 flow_create_and_get(owner_id,
                   type: "retention-spawn-shared-owner",
                   state: "queued",
                   partition_key: owner_partition,
                   run_at_ms: create_now,
                   retention_ttl_ms: 20,
                   now_ms: create_now
                 )

        assert {:ok, %{ref: shared_ref}} =
                 FerricStore.flow_value_put("shared-spawn",
                   partition_key: owner_partition,
                   owner_flow_id: owner_id,
                   name: "doc",
                   now_ms: create_now + 1
                 )

        assert {:ok, _parent} =
                 flow_create_and_get(parent_id,
                   type: "retention-spawn-shared-parent",
                   state: "queued",
                   partition_key: parent_partition,
                   run_at_ms: create_now,
                   now_ms: create_now + 2
                 )

        assert {:ok, [owner_claim]} =
                 FerricStore.flow_claim_due("retention-spawn-shared-owner",
                   partition_key: owner_partition,
                   worker: "worker-retention-spawn-shared-owner",
                   limit: 1,
                   now_ms: create_now + 3
                 )

        assert {:ok, [parent_claim]} =
                 FerricStore.flow_claim_due("retention-spawn-shared-parent",
                   partition_key: parent_partition,
                   worker: "worker-retention-spawn-shared-parent",
                   limit: 1,
                   now_ms: create_now + 3
                 )

        assert {:ok, _completed} =
                 flow_complete_and_get(owner_id, owner_claim.lease_token,
                   partition_key: owner_partition,
                   fencing_token: owner_claim.fencing_token,
                   now_ms: complete_now
                 )

        owner_shard = shard_for(Ferricstore.Flow.Keys.state_key(owner_id, owner_partition))
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, owner_shard)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        old_hook =
          Application.get_env(:ferricstore, :flow_shared_value_ref_acquisition_hook)

        Application.put_env(
          :ferricstore,
          :flow_shared_value_ref_acquisition_hook,
          fn record, refs ->
            if Map.get(record, :id) == child_id and shared_ref in refs do
              send(parent, {:spawn_shared_ref_acquisition_paused, self(), release_ref})

              receive do
                {:release_spawn_shared_ref_acquisition, ^release_ref} -> :ok
              after
                10_000 -> {:error, :spawn_shared_ref_acquisition_hook_timeout}
              end
            else
              :ok
            end
          end
        )

        on_exit(fn ->
          restore_env(:flow_shared_value_ref_acquisition_hook, old_hook)
        end)

        spawn_task =
          Task.async(fn ->
            FerricStore.flow_spawn_children(
              parent_id,
              [
                %{
                  id: child_id,
                  type: "retention-spawn-shared-child",
                  partition_key: child_partition,
                  value_refs: %{"doc" => shared_ref},
                  run_at_ms: create_now
                }
              ],
              partition_key: parent_partition,
              lease_token: parent_claim.lease_token,
              fencing_token: parent_claim.fencing_token,
              group_id: "retention-spawn-shared-group",
              wait_state: "waiting_children",
              success: "completed",
              failure: "failed",
              now_ms: create_now + 4
            )
          end)

        assert_receive {:spawn_shared_ref_acquisition_paused, apply_pid, ^release_ref}, 5_000

        cleanup_task =
          Task.async(fn ->
            Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
              limit: 10,
              now_ms: cleanup_now
            })
          end)

        cleanup_while_spawn_paused = Task.yield(cleanup_task, 1_000)
        send(apply_pid, {:release_spawn_shared_ref_acquisition, release_ref})

        assert :ok = Task.await(spawn_task, 10_000)

        cleanup_result =
          case cleanup_while_spawn_paused do
            nil -> Task.await(cleanup_task, 10_000)
            {:ok, result} -> result
          end

        assert cleanup_while_spawn_paused == nil
        assert {:ok, %{flows: 1}} = cleanup_result
        assert {:ok, ["shared-spawn"]} = FerricStore.flow_value_mget([shared_ref])

        assert {:ok, child} =
                 FerricStore.flow_get(child_id,
                   partition_key: child_partition,
                   values: ["doc"]
                 )

        assert child.values == %{"doc" => "shared-spawn"}
      end

      test "retention cleanup preserves owner named values referenced by retained history" do
        ctx = FerricStore.Instance.get(:default)
        owner_id = uid("flow-retention-history-owner")
        consumer_id = uid("flow-retention-history-consumer")
        partition_key = uid("tenant-retention-history-shared")
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000
        cleanup_now = complete_now + 1_000

        assert {:ok, _owner} =
                 flow_create_and_get(owner_id,
                   type: "retention-history-owner",
                   state: "queued",
                   partition_key: partition_key,
                   run_at_ms: create_now,
                   retention_ttl_ms: 20,
                   now_ms: create_now
                 )

        assert {:ok, %{ref: shared_ref}} =
                 FerricStore.flow_value_put("shared-doc",
                   partition_key: partition_key,
                   owner_flow_id: owner_id,
                   name: "doc",
                   now_ms: create_now + 1
                 )

        assert {:ok, _consumer} =
                 flow_create_and_get(consumer_id,
                   type: "retention-history-consumer",
                   state: "queued",
                   partition_key: partition_key,
                   value_refs: %{"doc" => shared_ref},
                   run_at_ms: create_now,
                   now_ms: create_now + 2
                 )

        assert {:ok, consumer_without_ref} =
                 flow_transition_and_get(consumer_id, "queued", "ready",
                   partition_key: partition_key,
                   fencing_token: 0,
                   drop_values: ["doc"],
                   now_ms: create_now + 3
                 )

        refute Map.has_key?(Map.get(consumer_without_ref, :value_refs, %{}), "doc")

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("retention-history-owner",
                   partition_key: partition_key,
                   worker: "worker-retention-history-owner",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now + 4
                 )

        assert {:ok, completed} =
                 flow_complete_and_get(owner_id, claimed.lease_token,
                   partition_key: partition_key,
                   fencing_token: claimed.fencing_token,
                   now_ms: complete_now
                 )

        assert completed.terminal_retention_until_ms == complete_now + 20

        [owner_id, consumer_id]
        |> Enum.map(fn id -> shard_for(Ferricstore.Flow.Keys.state_key(id, partition_key)) end)
        |> Enum.uniq()
        |> Enum.each(fn shard_index ->
          assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
        end)

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, pre_cleanup_history} =
                 FerricStore.flow_history(consumer_id,
                   partition_key: partition_key,
                   count: 10,
                   values: true
                 )

        pre_cleanup_events =
          Map.new(pre_cleanup_history, fn {_event_id, fields} -> {fields["event"], fields} end)

        assert {:ok, %{"doc" => %{"ref" => ^shared_ref}}} =
                 Jason.decode(pre_cleanup_events["created"]["value_refs"])

        assert {:ok, %{flows: 1}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: cleanup_now
                 })

        assert {:ok, ["shared-doc"]} = FerricStore.flow_value_mget([shared_ref])

        assert {:ok, history} =
                 FerricStore.flow_history(consumer_id,
                   partition_key: partition_key,
                   count: 10,
                   values: true
                 )

        events = Map.new(history, fn {_event_id, fields} -> {fields["event"], fields} end)

        assert {:ok, %{"doc" => %{"ref" => ^shared_ref}}} =
                 Jason.decode(events["created"]["value_refs"])
      end

      test "retention cleanup does not delete generated values for colon-prefixed flow ids" do
        ctx = FerricStore.Instance.get(:default)
        parent_id = uid("flow-retention-prefix")
        child_id = parent_id <> ":child"
        partition = uid("tenant-retention-prefix")
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 1_000
        cleanup_now = complete_now + 1_000

        assert {:ok, parent} =
                 flow_create_and_get(parent_id,
                   type: "retention-prefix-parent",
                   state: "queued",
                   partition_key: partition,
                   payload: "parent-payload",
                   run_at_ms: create_now,
                   retention_ttl_ms: 10,
                   now_ms: create_now
                 )

        assert {:ok, child} =
                 flow_create_and_get(child_id,
                   type: "retention-prefix-child",
                   state: "queued",
                   partition_key: partition,
                   payload: "child-payload",
                   run_at_ms: create_now,
                   retention_ttl_ms: 10,
                   now_ms: create_now
                 )

        assert parent.payload_ref != child.payload_ref

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("retention-prefix-parent",
                   worker: "worker-retention-prefix",
                   partition_key: partition,
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now
                 )

        assert claimed.id == parent_id

        assert {:ok, completed} =
                 flow_complete_and_get(parent_id, claimed.lease_token,
                   partition_key: partition,
                   fencing_token: claimed.fencing_token,
                   now_ms: complete_now
                 )

        assert completed.terminal_retention_until_ms == complete_now + 10

        shard_index = shard_for(Ferricstore.Flow.Keys.state_key(parent_id, partition))
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, %{flows: 1}} =
                 Ferricstore.Store.Router.flow_retention_cleanup(ctx, %{
                   limit: 10,
                   now_ms: cleanup_now
                 })

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, [nil, "child-payload"]} =
                 FerricStore.flow_value_mget([parent.payload_ref, child.payload_ref])

        assert {:ok, %{id: ^child_id, payload_ref: child_payload_ref}} =
                 FerricStore.flow_get(child_id, partition_key: partition, values: true)

        assert child_payload_ref == child.payload_ref
      end
    end
  end
end
