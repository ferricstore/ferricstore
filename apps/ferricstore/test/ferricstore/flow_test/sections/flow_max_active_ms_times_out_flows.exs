defmodule Ferricstore.FlowTest.Sections.FlowMaxActiveMsTimesOutFlows do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      test "max_active_ms defaults to infinite and does not affect active flows when unset" do
        id = uid("flow-active-timeout-unset")
        now = System.system_time(:millisecond) + 60_000

        assert {:ok, created} =
                 flow_create_and_get(id,
                   type: "active-timeout-unset",
                   state: "queued",
                   run_at_ms: now,
                   now_ms: now
                 )

        assert Map.get(created, :max_active_ms) == nil

        assert {:ok, %{active_timeouts: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 86_400_000)

        assert {:ok, active} = FerricStore.flow_get(id)
        assert active.state == "queued"
        assert active.terminal_retention_until_ms == nil
      end

      test "max_active_ms moves overdue non-terminal flows to failed and starts retention" do
        id = uid("flow-active-timeout")
        partition_key = uid("tenant-active-timeout")
        type = uid("active-timeout-type")
        create_now = System.system_time(:millisecond) + 60_000
        before_timeout = create_now + 499
        timeout_now = create_now + 500

        assert {:ok, policy} =
                 FerricStore.flow_policy_set(type,
                   max_active_ms: 500,
                   retention: [ttl_ms: 1_000]
                 )

        assert policy.max_active_ms == 500

        assert {:ok, created} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   run_at_ms: create_now + 10_000,
                   now_ms: create_now
                 )

        assert created.max_active_ms == 500

        assert {:ok, %{active_timeouts: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: before_timeout)

        assert {:ok, queued} = FerricStore.flow_get(id, partition_key: partition_key)
        assert queued.state == "queued"

        assert {:ok, %{active_timeouts: 1, flows: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: timeout_now)

        assert {:ok, failed} = FerricStore.flow_get(id, partition_key: partition_key, full: true)
        assert failed.state == "failed"
        assert failed.max_active_ms == 500
        assert failed.updated_at_ms == timeout_now
        assert failed.terminal_retention_until_ms == timeout_now + 1_000
        assert failed.lease_owner == nil
        assert failed.lease_token == nil
        assert failed.next_run_at_ms == nil
        assert failed.error == %{reason: "max_active_ms", max_active_ms: 500}

        assert {:ok, history} =
                 FerricStore.flow_history(id,
                   partition_key: partition_key,
                   count: 10,
                   direction: :forward
                 )

        assert Enum.any?(history, fn {_event_id, event} ->
                 event["event"] == "failed" and event["reason"] == "max_active_ms" and
                   event["max_active_ms"] == "500"
               end)
      end

      test "timeout cleanup also removes terminal flows already expired before the pass" do
        partition_key = uid("tenant-active-timeout-mixed-cleanup")
        expired_id = uid("flow-active-timeout-mixed-expired")
        active_id = uid("flow-active-timeout-mixed-active")
        type = uid("active-timeout-mixed-type")
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 10
        cleanup_now = create_now + 100

        assert {:ok, _created} =
                 flow_create_and_get(expired_id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   run_at_ms: create_now,
                   retention_ttl_ms: 10,
                   now_ms: create_now
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-active-timeout-mixed",
                   partition_key: partition_key,
                   now_ms: create_now
                 )

        assert {:ok, completed} =
                 flow_complete_and_get(expired_id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   partition_key: partition_key,
                   now_ms: complete_now
                 )

        assert completed.terminal_retention_until_ms == complete_now + 10

        assert {:ok, _created} =
                 flow_create_and_get(active_id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   max_active_ms: 100,
                   run_at_ms: create_now + 1_000,
                   now_ms: create_now
                 )

        assert {:ok, %{active_timeouts: 1, flows: 1}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: cleanup_now)

        ctx = FerricStore.Instance.get(:default)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, nil} = FerricStore.flow_get(expired_id, partition_key: partition_key)

        assert {:ok, %{state: "failed"}} =
                 FerricStore.flow_get(active_id, partition_key: partition_key)
      end

      test "timeout cleanup applies one active candidate limit across shards" do
        {partition_a, _same_partition, partition_b} = mixed_partition_keys()
        type = uid("active-timeout-global-limit-type")
        create_now = System.system_time(:millisecond) + 60_000
        cleanup_now = create_now + 100

        ids =
          for {partition_key, shard_label} <- [{partition_a, "a"}, {partition_b, "b"}],
              index <- 1..2 do
            id = uid("flow-active-timeout-global-limit-#{shard_label}-#{index}")

            assert :ok =
                     FerricStore.flow_create(id,
                       type: type,
                       state: "queued",
                       partition_key: partition_key,
                       max_active_ms: 100,
                       run_at_ms: create_now + 1_000,
                       now_ms: create_now
                     )

            {id, partition_key}
          end

        assert {:ok, %{active_timeouts: 2, flows: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 2, now_ms: cleanup_now)

        states =
          Enum.map(ids, fn {id, partition_key} ->
            assert {:ok, flow} = FerricStore.flow_get(id, partition_key: partition_key)
            flow.state
          end)

        assert Enum.count(states, &(&1 == "failed")) == 2
        assert Enum.count(states, &(&1 == "queued")) == 2
      end

      test "timeout and terminal cleanup share one limit across shards" do
        ctx = FerricStore.Instance.get(:default)
        {partition_a, _same_partition, partition_b} = mixed_partition_keys()
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 10
        cleanup_now = create_now + 100

        terminal_flows =
          for {partition_key, shard_label} <- [{partition_a, "a"}, {partition_b, "b"}] do
            id = uid("flow-retention-global-limit-terminal-#{shard_label}")
            type = uid("retention-global-limit-terminal-type-#{shard_label}")

            assert :ok =
                     FerricStore.flow_create(id,
                       type: type,
                       state: "queued",
                       partition_key: partition_key,
                       run_at_ms: create_now,
                       retention_ttl_ms: 10,
                       now_ms: create_now
                     )

            assert {:ok, [claimed]} =
                     FerricStore.flow_claim_due(type,
                       worker: "worker-retention-global-limit-#{shard_label}",
                       partition_key: partition_key,
                       limit: 1,
                       now_ms: create_now
                     )

            assert {:ok, completed} =
                     flow_complete_and_get(id, claimed.lease_token,
                       fencing_token: claimed.fencing_token,
                       partition_key: partition_key,
                       now_ms: complete_now
                     )

            assert completed.terminal_retention_until_ms == complete_now + 10
            {id, partition_key}
          end

        for {partition_key, shard_label} <- [{partition_a, "a"}, {partition_b, "b"}] do
          assert :ok =
                   FerricStore.flow_create(
                     uid("flow-retention-global-limit-active-#{shard_label}"),
                     type: uid("retention-global-limit-active-type-#{shard_label}"),
                     state: "queued",
                     partition_key: partition_key,
                     max_active_ms: 100,
                     run_at_ms: create_now + 1_000,
                     now_ms: create_now
                   )
        end

        terminal_flows
        |> Enum.map(fn {id, partition_key} ->
          shard_for(Ferricstore.Flow.Keys.state_key(id, partition_key))
        end)
        |> Enum.uniq()
        |> Enum.each(fn shard_index ->
          assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
        end)

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, %{active_timeouts: 2, flows: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 2, now_ms: cleanup_now)

        Enum.each(terminal_flows, fn {id, partition_key} ->
          assert {:ok, %{state: "completed"}} =
                   FerricStore.flow_get(id, partition_key: partition_key)
        end)
      end

      test "a stale active candidate does not consume the terminal cleanup limit" do
        ctx = FerricStore.Instance.get(:default)
        partition_key = uid("tenant-retention-stale-active-budget")
        terminal_id = uid("flow-retention-stale-active-terminal")
        active_id = uid("flow-retention-stale-active-candidate")
        terminal_type = uid("retention-stale-active-terminal-type")
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 10
        cleanup_now = create_now + 100
        parent = self()

        assert :ok =
                 FerricStore.flow_create(terminal_id,
                   type: terminal_type,
                   state: "queued",
                   partition_key: partition_key,
                   run_at_ms: create_now,
                   retention_ttl_ms: 10,
                   now_ms: create_now
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(terminal_type,
                   worker: "worker-retention-stale-active",
                   partition_key: partition_key,
                   now_ms: create_now
                 )

        assert :ok =
                 FerricStore.flow_complete(terminal_id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   partition_key: partition_key,
                   now_ms: complete_now
                 )

        assert :ok =
                 FerricStore.flow_create(active_id,
                   type: uid("retention-stale-active-type"),
                   state: "queued",
                   partition_key: partition_key,
                   max_active_ms: 100,
                   run_at_ms: cleanup_now + 10_000,
                   now_ms: create_now
                 )

        shard_index = shard_for(Ferricstore.Flow.Keys.state_key(terminal_id, partition_key))
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        old_hook = Application.get_env(:ferricstore, :flow_retention_after_plan_hook)

        Application.put_env(:ferricstore, :flow_retention_after_plan_hook, fn
          :active, candidates when is_list(candidates) ->
            if Enum.any?(candidates, &(&1.record.id == active_id)) do
              assert :ok =
                       FerricStore.flow_transition(active_id, "queued", "ready",
                         partition_key: partition_key,
                         fencing_token: 0,
                         now_ms: cleanup_now
                       )

              active_shard =
                shard_for(Ferricstore.Flow.Keys.state_key(active_id, partition_key))

              assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, active_shard)
              assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
              send(parent, :stale_active_candidate_created)
            end

            :ok

          _kind, _candidates ->
            :ok
        end)

        on_exit(fn -> restore_env(:flow_retention_after_plan_hook, old_hook) end)

        assert {:ok, %{active_timeouts: 0, flows: 1}} =
                 FerricStore.flow_retention_cleanup(limit: 1, now_ms: cleanup_now)

        assert_receive :stale_active_candidate_created
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
        assert {:ok, nil} = FerricStore.flow_get(terminal_id, partition_key: partition_key)

        assert {:ok, %{state: "ready"}} =
                 FerricStore.flow_get(active_id, partition_key: partition_key)
      end

      test "retention bounds the exact cross-shard cleanup command globally" do
        ctx = FerricStore.Instance.get(:default)
        {partition_a, _same_partition, partition_b} = mixed_partition_keys()
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 10
        cleanup_now = create_now + 100
        key_budget = 12
        byte_budget = 12_000
        parent = self()

        old_key_budget = Application.get_env(:ferricstore, :flow_retention_cleanup_key_budget)

        old_byte_budget =
          Application.get_env(:ferricstore, :flow_retention_cleanup_byte_budget)

        old_hook = Application.get_env(:ferricstore, :flow_retention_command_hook)
        Application.put_env(:ferricstore, :flow_retention_cleanup_key_budget, key_budget)
        Application.put_env(:ferricstore, :flow_retention_cleanup_byte_budget, byte_budget)

        Application.put_env(:ferricstore, :flow_retention_command_hook, fn kind, command ->
          send(parent, {:retention_command, kind, command})
          :ok
        end)

        on_exit(fn ->
          restore_env(:flow_retention_cleanup_key_budget, old_key_budget)
          restore_env(:flow_retention_cleanup_byte_budget, old_byte_budget)
          restore_env(:flow_retention_command_hook, old_hook)
        end)

        for {partition_key, label} <- [{partition_a, "a"}, {partition_b, "b"}], index <- 1..2 do
          id = uid("flow-retention-command-budget-#{label}-#{index}")
          type = uid("retention-command-budget-type-#{label}-#{index}")

          assert :ok =
                   FerricStore.flow_create(id,
                     type: type,
                     state: "queued",
                     partition_key: partition_key,
                     payload: String.duplicate("p", 128),
                     run_at_ms: create_now,
                     retention_ttl_ms: 10,
                     now_ms: create_now
                   )

          assert {:ok, [claimed]} =
                   FerricStore.flow_claim_due(type,
                     worker: "worker-retention-command-budget",
                     partition_key: partition_key,
                     now_ms: create_now
                   )

          assert :ok =
                   FerricStore.flow_complete(id, claimed.lease_token,
                     fencing_token: claimed.fencing_token,
                     partition_key: partition_key,
                     result: String.duplicate("r", 128),
                     now_ms: complete_now
                   )
        end

        for shard_index <- 0..(ctx.shard_count - 1) do
          assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
        end

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, _counts} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: cleanup_now)

        assert_receive {:retention_command, :terminal,
                        command =
                          {:cross_shard_tx,
                           [{0, [{0, {:flow_cross_retention_cleanup, command_attrs}}], nil} | _]}},
                       5_000

        assert :erlang.external_size(command) <= byte_budget

        assert command_attrs.terminal_candidates != []

        assert Enum.sum(Enum.map(command_attrs.terminal_candidates, & &1.planned_key_count)) <=
                 key_budget

        refute_receive {:retention_command, :terminal, _other_command}, 100

        assert :ok = Ferricstore.Raft.WARaftBackend.start(ctx)
        assert :ok = Ferricstore.Raft.Backend.write(0, {:clear_locks})
        assert :ok = Ferricstore.Raft.WARaftBackend.start(ctx)
        assert :ok = Ferricstore.Raft.Backend.write(0, {:clear_locks})
      end

      test "terminal cleanup requires trusted backfill verification and the exact watermark" do
        ctx = FerricStore.Instance.get(:default)
        id = uid("flow-retention-backfill-watermark")
        type = uid("retention-backfill-watermark-type")
        partition_key = uid("tenant-retention-backfill-watermark")
        create_now = System.system_time(:millisecond) + 60_000
        complete_now = create_now + 10
        cleanup_now = create_now + 100

        assert :ok =
                 FerricStore.flow_create(id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   run_at_ms: create_now,
                   retention_ttl_ms: 10,
                   now_ms: create_now
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-retention-backfill-watermark",
                   partition_key: partition_key,
                   now_ms: create_now
                 )

        assert :ok =
                 FerricStore.flow_complete(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   partition_key: partition_key,
                   now_ms: complete_now
                 )

        state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
        shard_index = shard_for(state_key)
        keydir = elem(ctx.keydir_refs, shard_index)
        watermark_key = Ferricstore.Flow.Keys.shared_value_ref_backfill_key(shard_index)
        [watermark_entry] = :ets.lookup(keydir, watermark_key)

        verified_key =
          {Ferricstore.Flow.SharedRefBackfill, :verified_complete, ctx.name, shard_index}

        assert Ferricstore.Flow.SharedRefBackfill.verified_complete?(ctx.name, shard_index)

        on_exit(fn ->
          :ets.insert(keydir, watermark_entry)
          :persistent_term.put(verified_key, true)
        end)

        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        :persistent_term.erase(verified_key)

        assert {:ok, %{flows: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: cleanup_now)

        assert {:ok, %{state: "completed"}} =
                 FerricStore.flow_get(id, partition_key: partition_key)

        :persistent_term.put(verified_key, true)

        corrupt_watermark_entry = put_elem(watermark_entry, 1, <<2>>)
        true = :ets.insert(keydir, corrupt_watermark_entry)

        assert {:ok, %{flows: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: cleanup_now + 1)

        assert {:ok, %{state: "completed"}} =
                 FerricStore.flow_get(id, partition_key: partition_key)

        true = :ets.delete(keydir, watermark_key)

        assert {:ok, %{flows: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: cleanup_now + 2)

        assert {:ok, %{state: "completed"}} =
                 FerricStore.flow_get(id, partition_key: partition_key)

        true = :ets.insert(keydir, watermark_entry)

        assert {:ok, %{flows: 1}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: cleanup_now + 3)
      end

      test "create max_active_ms overrides the type policy for one flow" do
        id = uid("flow-active-timeout-override")
        type = uid("active-timeout-override-type")
        now = System.system_time(:millisecond) + 60_000

        assert {:ok, _policy} = FerricStore.flow_policy_set(type, max_active_ms: 10_000)

        assert {:ok, created} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   max_active_ms: 100,
                   run_at_ms: now,
                   now_ms: now
                 )

        assert created.max_active_ms == 100

        assert {:ok, %{active_timeouts: 1}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 100)

        assert {:ok, failed} = FerricStore.flow_get(id)
        assert failed.state == "failed"
      end

      test "create max_active_ms infinity opts one flow out of the type policy timeout" do
        id = uid("flow-active-timeout-infinity")
        type = uid("active-timeout-infinity-type")
        now = System.system_time(:millisecond) + 60_000

        assert {:ok, _policy} = FerricStore.flow_policy_set(type, max_active_ms: 100)

        assert {:ok, created} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   max_active_ms: :infinity,
                   run_at_ms: now,
                   now_ms: now
                 )

        assert Map.get(created, :max_active_ms) == nil

        assert {:ok, %{active_timeouts: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 100)

        assert {:ok, queued} = FerricStore.flow_get(id)
        assert queued.state == "queued"
      end

      test "claim_due fails a flow whose max active deadline has elapsed" do
        id = uid("flow-active-timeout-claim")
        type = uid("active-timeout-claim-type")
        create_now = System.system_time(:millisecond) + 60_000
        timeout_now = create_now + 100

        assert {:ok, _created} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   max_active_ms: 100,
                   run_at_ms: create_now,
                   now_ms: create_now
                 )

        assert {:ok, []} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-active-timeout",
                   now_ms: timeout_now
                 )

        assert {:ok, failed} = FerricStore.flow_get(id, full: true)
        assert failed.state == "failed"
        assert failed.updated_at_ms == timeout_now
        assert failed.error == %{reason: "max_active_ms", max_active_ms: 100}
      end

      test "claim_due keeps a batch of unexpired max active candidates eligible" do
        type = uid("active-timeout-claim-live-batch-type")
        partition_key = uid("tenant-active-timeout-claim-live-batch")
        create_now = System.system_time(:millisecond) + 60_000

        ids =
          for index <- 1..3 do
            id = uid("flow-active-timeout-claim-live-#{index}")

            assert :ok =
                     FerricStore.flow_create(id,
                       type: type,
                       state: "queued",
                       partition_key: partition_key,
                       max_active_ms: 1_000,
                       run_at_ms: create_now + index,
                       now_ms: create_now
                     )

            id
          end

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-active-timeout-live-batch",
                   partition_key: partition_key,
                   limit: 3,
                   now_ms: create_now + 100
                 )

        assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new(ids)
        assert Enum.all?(claimed, &(&1.state == "running"))
      end

      test "claim_due skips an expired candidate and claims the next eligible flow" do
        expired_id = uid("flow-active-timeout-claim-expired")
        eligible_id = uid("flow-active-timeout-claim-eligible")
        type = uid("active-timeout-claim-next-type")
        partition_key = uid("tenant-active-timeout-claim-next")
        create_now = System.system_time(:millisecond) + 60_000
        timeout_now = create_now + 100

        assert :ok =
                 FerricStore.flow_create(expired_id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   max_active_ms: 100,
                   run_at_ms: create_now,
                   now_ms: create_now
                 )

        assert :ok =
                 FerricStore.flow_create(eligible_id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   run_at_ms: create_now + 1,
                   now_ms: create_now
                 )

        assert {:ok, [%{id: ^eligible_id}]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-active-timeout",
                   partition_key: partition_key,
                   limit: 1,
                   now_ms: timeout_now
                 )

        assert {:ok, %{state: "failed"}} =
                 FerricStore.flow_get(expired_id, partition_key: partition_key)
      end

      test "claim_due defers an expired child whose parent is on another shard" do
        parent = uid("flow-active-timeout-claim-parent")
        child = uid("flow-active-timeout-claim-child")
        type = uid("active-timeout-claim-child-type")
        {parent_partition, _same_partition, child_partition} = mixed_partition_keys()
        create_now = System.system_time(:millisecond) + 60_000
        timeout_now = create_now + 100

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "active-timeout-claim-parent",
                   state: "dispatch",
                   partition_key: parent_partition,
                   run_at_ms: create_now,
                   now_ms: create_now
                 )

        assert {:ok, _waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [
                     %{
                       id: child,
                       type: type,
                       partition_key: child_partition,
                       max_active_ms: 100,
                       run_at_ms: create_now
                     }
                   ],
                   group_id: "timeout-claim-cross-group",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :abandon_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: parent_partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token,
                   now_ms: create_now
                 )

        assert {:ok, []} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-active-timeout",
                   partition_key: child_partition,
                   now_ms: timeout_now
                 )

        assert {:ok, %{state: "queued"}} =
                 FerricStore.flow_get(child, partition_key: child_partition)

        assert {:ok, waiting_parent} =
                 FerricStore.flow_get(parent, partition_key: parent_partition)

        assert waiting_parent.state == "waiting_children"

        assert waiting_parent.child_groups["timeout-claim-cross-group"]["children"][child] ==
                 "running"

        assert {:ok, %{active_timeouts: 1}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: timeout_now)

        assert {:ok, %{state: "failed"}} =
                 FerricStore.flow_get(child, partition_key: child_partition)

        assert {:ok, failed_parent} =
                 FerricStore.flow_get(parent, partition_key: parent_partition)

        assert failed_parent.state == "children_failed"
      end

      test "deferred cross-shard timeouts do not starve a later eligible claim" do
        parent = uid("flow-active-timeout-claim-many-parent")
        type = uid("active-timeout-claim-many-child-type")
        {parent_partition, _same_partition, child_partition} = mixed_partition_keys()
        create_now = System.system_time(:millisecond) + 60_000
        timeout_now = create_now + 100

        children =
          for index <- 1..32 do
            %{
              id: uid("flow-active-timeout-claim-many-child-#{index}"),
              type: type,
              partition_key: child_partition,
              max_active_ms: 100,
              run_at_ms: create_now
            }
          end

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "active-timeout-claim-many-parent",
                   state: "dispatch",
                   partition_key: parent_partition,
                   run_at_ms: create_now,
                   now_ms: create_now
                 )

        assert {:ok, _waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   children,
                   group_id: "timeout-claim-many-cross-group",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :abandon_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: parent_partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token,
                   now_ms: create_now
                 )

        eligible_id = uid("flow-active-timeout-claim-many-eligible")

        assert :ok =
                 FerricStore.flow_create(eligible_id,
                   type: type,
                   state: "queued",
                   partition_key: child_partition,
                   run_at_ms: create_now + 1,
                   now_ms: create_now
                 )

        assert {:ok, [%{id: ^eligible_id}]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-active-timeout-many",
                   partition_key: child_partition,
                   limit: 1,
                   now_ms: timeout_now
                 )

        ctx = FerricStore.Instance.get(:default)
        due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, child_partition)
        due_any_key = Ferricstore.Flow.Keys.due_any_key(type, 0, child_partition)
        shard_index = Ferricstore.Store.Router.shard_for(ctx, due_key)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)

        native = Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup)

        for index_key <- [due_key, due_any_key] do
          assert [] =
                   Ferricstore.Flow.NativeOrderedIndex.range_slice(
                     native,
                     index_key,
                     :neg_inf,
                     {:inclusive, timeout_now},
                     false,
                     0,
                     100
                   )
        end

        any_id = uid("flow-active-timeout-claim-many-any-eligible")

        assert :ok =
                 FerricStore.flow_create(any_id,
                   type: type,
                   state: "queued",
                   partition_key: child_partition,
                   run_at_ms: create_now + 2,
                   now_ms: create_now
                 )

        assert {:ok, [%{id: ^any_id}]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-active-timeout-many-any",
                   partition_key: child_partition,
                   state: :any,
                   limit: 1,
                   now_ms: timeout_now
                 )

        assert {:ok, %{state: "queued"}} =
                 FerricStore.flow_get(hd(children).id, partition_key: child_partition)
      end

      test "claim_due applies an expired child to a same-shard parent in another partition" do
        parent = uid("flow-active-timeout-claim-same-parent")
        child = uid("flow-active-timeout-claim-same-child")
        type = uid("active-timeout-claim-same-child-type")
        {parent_partition, child_partition, _other_partition} = mixed_partition_keys()
        create_now = System.system_time(:millisecond) + 60_000
        timeout_now = create_now + 100

        assert parent_partition != child_partition

        assert shard_for(Ferricstore.Flow.Keys.state_key(parent, parent_partition)) ==
                 shard_for(Ferricstore.Flow.Keys.state_key(child, child_partition))

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "active-timeout-claim-same-parent",
                   state: "dispatch",
                   partition_key: parent_partition,
                   run_at_ms: create_now,
                   now_ms: create_now
                 )

        assert {:ok, _waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [
                     %{
                       id: child,
                       type: type,
                       partition_key: child_partition,
                       max_active_ms: 100,
                       run_at_ms: create_now
                     }
                   ],
                   group_id: "timeout-claim-same-group",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :abandon_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: parent_partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token,
                   now_ms: create_now
                 )

        assert {:ok, []} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-active-timeout",
                   partition_key: child_partition,
                   now_ms: timeout_now
                 )

        assert {:ok, %{state: "failed"}} =
                 FerricStore.flow_get(child, partition_key: child_partition)

        assert {:ok, failed_parent} =
                 FerricStore.flow_get(parent, partition_key: parent_partition)

        assert failed_parent.state == "children_failed"

        assert failed_parent.child_groups["timeout-claim-same-group"]["children"][child] ==
                 "failed"
      end

      test "complete fails a running flow whose max active deadline has elapsed" do
        id = uid("flow-active-timeout-complete")
        type = uid("active-timeout-complete-type")
        create_now = System.system_time(:millisecond) + 60_000
        timeout_now = create_now + 100

        assert {:ok, _created} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   max_active_ms: 100,
                   run_at_ms: create_now,
                   now_ms: create_now
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-active-timeout",
                   lease_ms: 1_000,
                   now_ms: create_now
                 )

        assert {:error, reason} =
                 FerricStore.flow_complete(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: timeout_now
                 )

        assert reason =~ "max_active_ms"
        assert {:ok, failed} = FerricStore.flow_get(id, full: true)
        assert failed.state == "failed"
        assert failed.updated_at_ms == timeout_now
        assert failed.error == %{reason: "max_active_ms", max_active_ms: 100}
      end

      test "complete enforces max_active_ms for a child whose parent is on another shard" do
        parent = uid("flow-active-timeout-complete-parent")
        child = uid("flow-active-timeout-complete-child")
        type = uid("active-timeout-complete-child-type")
        {parent_partition, _same_partition, child_partition} = mixed_partition_keys()
        create_now = System.system_time(:millisecond) + 60_000
        timeout_now = create_now + 100

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "active-timeout-complete-parent",
                   state: "dispatch",
                   partition_key: parent_partition,
                   run_at_ms: create_now,
                   now_ms: create_now
                 )

        assert {:ok, _waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [
                     %{
                       id: child,
                       type: type,
                       partition_key: child_partition,
                       max_active_ms: 100,
                       run_at_ms: create_now
                     }
                   ],
                   group_id: "timeout-complete-cross-group",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :abandon_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: parent_partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token,
                   now_ms: create_now
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-active-timeout",
                   partition_key: child_partition,
                   lease_ms: 1_000,
                   now_ms: create_now
                 )

        assert {:error, reason} =
                 FerricStore.flow_complete(child, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   partition_key: child_partition,
                   now_ms: timeout_now
                 )

        assert reason =~ "max_active_ms"

        assert {:ok, %{state: "failed"}} =
                 FerricStore.flow_get(child, partition_key: child_partition)

        assert {:ok, failed_parent} =
                 FerricStore.flow_get(parent, partition_key: parent_partition)

        assert failed_parent.state == "children_failed"

        assert failed_parent.child_groups["timeout-complete-cross-group"]["children"][child] ==
                 "failed"
      end

      test "timeout cleanup reloads lineage records changed earlier in the same batch" do
        parent = uid("flow-active-timeout-parent")
        child = uid("flow-active-timeout-child")
        partition_key = uid("tenant-active-timeout-lineage")
        now = System.system_time(:millisecond) + 60_000

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "active-timeout-parent",
                   state: "dispatch",
                   partition_key: partition_key,
                   max_active_ms: 100,
                   run_at_ms: now + 10_000,
                   now_ms: now
                 )

        assert {:ok, _waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [
                     %{
                       id: child,
                       type: "active-timeout-child",
                       partition_key: partition_key,
                       max_active_ms: 100,
                       run_at_ms: now + 10_000
                     }
                   ],
                   group_id: "timeout-group",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "children_done", failure: "failed"},
                   partition_key: partition_key,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token,
                   now_ms: now
                 )

        assert {:ok, %{max_active_ms: 100}} =
                 FerricStore.flow_get(child, partition_key: partition_key)

        assert {:ok, %{active_timeouts: 1}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 100)

        assert {:ok, terminal_parent} =
                 FerricStore.flow_get(parent, partition_key: partition_key)

        assert {:ok, terminal_child} = FerricStore.flow_get(child, partition_key: partition_key)

        child_status = terminal_parent.child_groups["timeout-group"]["children"][child]
        assert child_status == terminal_child.state
        assert terminal_parent.state == "failed"
        assert terminal_child.state in ["failed", "cancelled"]
      end

      test "timeout cleanup enforces max_active_ms for hibernated flows" do
        id = uid("flow-active-timeout-cold")
        partition_key = uid("tenant-active-timeout-cold")
        now = System.system_time(:millisecond) + 60_000
        ctx = FerricStore.Instance.get(:default)
        state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
        shard_index = shard_for(state_key)
        keydir = elem(ctx.keydir_refs, shard_index)

        {index_name, lookup_name} =
          Ferricstore.Flow.NativeOrderedIndex.table_names(ctx.name, shard_index)

        lmdb_path =
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(shard_index)
          |> Ferricstore.Flow.LMDB.path()

        assert {:ok, %{max_active_ms: 100}} =
                 flow_create_and_get(id,
                   type: "active-timeout-cold",
                   state: "queued",
                   partition_key: partition_key,
                   max_active_ms: 100,
                   run_at_ms: now + 600_000,
                   now_ms: now
                 )

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        Ferricstore.Test.ShardHelpers.eventually(
          fn -> :ets.lookup(keydir, state_key) == [] end,
          "flow should be evicted from the hot keydir after hibernation"
        )

        native = Ferricstore.Flow.NativeOrderedIndex.get(index_name, lookup_name)

        assert [{^state_key, _deadline}] =
                 Ferricstore.Flow.NativeOrderedIndex.range_slice(
                   native,
                   "f:{f}:i:active-timeout",
                   :neg_inf,
                   {:inclusive, now + 100},
                   false,
                   0,
                   1
                 )

        assert {:ok, [^state_key]} =
                 Ferricstore.Flow.LMDB.expired_active_timeout_state_keys(
                   lmdb_path,
                   now + 100,
                   1
                 )

        assert {:ok, %{active_timeouts: 1}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 100)

        assert {:ok, %{state: "failed"}} =
                 FerricStore.flow_get(id, partition_key: partition_key)

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, []} =
                 Ferricstore.Flow.LMDB.expired_active_timeout_state_keys(
                   lmdb_path,
                   now + 100,
                   1
                 )
      end

      test "timeout cleanup propagates a child failure to a parent on another shard" do
        ctx = FerricStore.Instance.get(:default)
        parent = uid("flow-active-timeout-cross-parent")
        child = uid("flow-active-timeout-cross-child")
        {parent_partition, _same_partition, child_partition} = mixed_partition_keys()
        now = System.system_time(:millisecond) + 60_000

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "active-timeout-cross-parent",
                   state: "dispatch",
                   partition_key: parent_partition,
                   run_at_ms: now + 10_000,
                   now_ms: now
                 )

        assert {:ok, _waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [
                     %{
                       id: child,
                       type: "active-timeout-cross-child",
                       partition_key: child_partition,
                       max_active_ms: 100,
                       run_at_ms: now + 10_000
                     }
                   ],
                   group_id: "timeout-cross-group",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :abandon_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: parent_partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token,
                   now_ms: now
                 )

        assert shard_for(Ferricstore.Flow.Keys.state_key(parent, parent_partition)) !=
                 shard_for(Ferricstore.Flow.Keys.state_key(child, child_partition))

        assert {:ok, %{active_timeouts: 1}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 100)

        assert {:ok, %{state: "failed"}} =
                 FerricStore.flow_get(child, partition_key: child_partition)

        assert {:ok, failed_parent} =
                 FerricStore.flow_get(parent, partition_key: parent_partition)

        assert failed_parent.state == "children_failed"
        assert failed_parent.child_groups["timeout-cross-group"]["children"][child] == "failed"

        assert :ok = Ferricstore.Raft.WARaftBackend.start(ctx)
        assert :ok = Ferricstore.Raft.Backend.write(0, {:clear_locks})
        assert :ok = Ferricstore.Raft.WARaftBackend.start(ctx)
        assert :ok = Ferricstore.Raft.Backend.write(0, {:clear_locks})
      end

      test "max active deadlines are projected into a bounded hot index" do
        id = uid("flow-active-timeout-indexed")
        partition_key = uid("tenant-active-timeout-indexed")
        now = System.system_time(:millisecond) + 60_000
        deadline = now + 100
        ctx = FerricStore.Instance.get(:default)
        state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
        shard_index = shard_for(state_key)

        {index_name, lookup_name} =
          Ferricstore.Flow.NativeOrderedIndex.table_names(ctx.name, shard_index)

        assert {:ok, _created} =
                 flow_create_and_get(id,
                   type: "active-timeout-indexed",
                   state: "queued",
                   partition_key: partition_key,
                   max_active_ms: 100,
                   run_at_ms: now + 10_000,
                   now_ms: now
                 )

        native = Ferricstore.Flow.NativeOrderedIndex.get(index_name, lookup_name)

        assert [{^state_key, score}] =
                 Ferricstore.Flow.NativeOrderedIndex.range_slice(
                   native,
                   "f:{f}:i:active-timeout",
                   :neg_inf,
                   {:inclusive, deadline},
                   false,
                   0,
                   1
                 )

        assert score == deadline * 1.0

        assert {:ok, %{active_timeouts: 1}} =
                 FerricStore.flow_retention_cleanup(limit: 1, now_ms: deadline)

        assert [] =
                 Ferricstore.Flow.NativeOrderedIndex.range_slice(
                   native,
                   "f:{f}:i:active-timeout",
                   :neg_inf,
                   {:inclusive, deadline},
                   false,
                   0,
                   1
                 )
      end
    end
  end
end
