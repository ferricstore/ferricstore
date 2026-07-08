defmodule Ferricstore.FlowTest.Sections.FlowPolicyExposesTypeStateRetryRetentionDefaults do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

      test "flow policy exposes type and state retry and retention defaults" do
        type = uid("flow-policy")

        assert {:ok, policy} =
                 FerricStore.flow_policy_set(type,
                   max_active_ms: 120_000,
                   retention: [ttl_ms: 60_000, history_max_events: 512],
                   retry: [
                     max_retries: 5,
                     backoff: [kind: :fixed, base_ms: 10_000, max_ms: 30_000, jitter_pct: 0],
                     exhausted_to: "failed"
                   ],
                   states: %{
                     "charge_card" => [
                       retry: [
                         max_retries: 2,
                         exhausted_to: "payment_failed"
                       ],
                       retention: [
                         ttl_ms: 30_000,
                         history_max_events: 256
                       ]
                     ]
                   }
                 )

        assert policy.retry.max_retries == 5
        assert policy.max_active_ms == 120_000
        assert policy.retention.ttl_ms == 60_000
        refute Map.has_key?(policy.retention, :history_hot_max_events)
        assert policy.retention.history_max_events == 512
        assert policy.states["charge_card"].retry.max_retries == 2
        assert policy.states["charge_card"].retry.backoff.kind == :fixed
        assert policy.states["charge_card"].retry.exhausted_to == "payment_failed"
        assert policy.states["charge_card"].retention.ttl_ms == 30_000
        refute Map.has_key?(policy.states["charge_card"].retention, :history_hot_max_events)
        assert policy.states["charge_card"].retention.history_max_events == 256

        assert {:ok, state_policy} = FerricStore.flow_policy_get(type, state: "charge_card")
        assert state_policy.max_active_ms == 120_000
        assert state_policy.retry.max_retries == 2
        assert state_policy.retry.backoff.base_ms == 10_000
        assert state_policy.retry.exhausted_to == "payment_failed"
        assert state_policy.retention.ttl_ms == 30_000
        refute Map.has_key?(state_policy.retention, :history_hot_max_events)
        assert state_policy.retention.history_max_events == 256
      end

      test "flow policy rejects hot history cap because it is internal" do
        type = uid("flow-policy-hot-internal")

        assert {:error, "ERR flow retention history_hot_max_events is internal"} =
                 FerricStore.flow_policy_set(type,
                   retention: [ttl_ms: 5_000, history_hot_max_events: 3]
                 )
      end

      test "flow policy rejects state-level max_active_ms because it is type-level only" do
        type = uid("flow-policy-state-active-timeout")

        assert {:error, "ERR flow max_active_ms is type-level only"} =
                 FerricStore.flow_policy_set(type,
                   states: %{
                     "queued" => [max_active_ms: 1_000]
                   }
                 )
      end

      test "flow policy is mirrored to LMDB asynchronously" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        ctx = FerricStore.Instance.get(:default)
        type = uid("policy-lmdb")
        policy_key = Ferricstore.Flow.Keys.policy_key(type)
        shard_index = Ferricstore.Store.Router.shard_for(ctx, policy_key)

        lmdb_path =
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(shard_index)
          |> Ferricstore.Flow.LMDB.path()

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
        end)

        assert {:ok, _policy} =
                 FerricStore.flow_policy_set(type,
                   retry: [max_retries: 7, exhausted_to: "failed"]
                 )

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)
        assert {:ok, blob} = Ferricstore.Flow.LMDB.get(lmdb_path, policy_key)

        assert {:ok, encoded_policy} =
                 Ferricstore.Flow.LMDB.decode_value(blob, System.system_time(:millisecond))

        assert {:ok, policy} = Ferricstore.Flow.RetryPolicy.decode_flow_policy(encoded_policy)
        assert policy.retry.max_retries == 7
      end

      test "standalone restart rebuilds stored policy and retry uses it" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        name = :"flow_policy_restart_#{System.unique_integer([:positive])}"
        data_dir = Path.join(System.tmp_dir!(), "ferricstore_#{name}")
        ctx = start_flow_restart_instance(name, data_dir)

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)

          current_ctx =
            try do
              FerricStore.Instance.get(name)
            rescue
              _ -> ctx
            end

          stop_flow_restart_instance(current_ctx, delete?: true)
        end)

        type = uid("policy-restart")
        id = uid("flow-policy-restart")

        assert {:ok, _policy} =
                 FerricStore.Impl.flow_policy_set(ctx, type,
                   states: %{
                     "charge_card" => [retry: [max_retries: 0, exhausted_to: "payment_failed"]]
                   }
                 )

        assert {:ok, _flow} =
                 impl_flow_create_and_get(ctx, id,
                   type: type,
                   state: "charge_card",
                   run_at_ms: 1_000,
                   now_ms: 1_000
                 )

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
        stop_flow_restart_instance(ctx, delete?: false)

        restarted = start_flow_restart_instance(name, data_dir)

        assert {:ok, state_policy} =
                 FerricStore.Impl.flow_policy_get(restarted, type, state: "charge_card")

        assert state_policy.retry.max_retries == 0

        assert {:ok, [claimed]} =
                 FerricStore.Impl.flow_claim_due(restarted, type,
                   state: "charge_card",
                   worker: "worker-restart",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, retried} =
                 impl_flow_retry_and_get(restarted, claimed.id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: 2_000
                 )

        assert retried.state == "payment_failed"
        assert retried.next_run_at_ms == 2_000
      end

      test "native claim batch state write survives standalone restart" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        name = :"flow_native_claim_restart_#{System.unique_integer([:positive])}"
        data_dir = Path.join(System.tmp_dir!(), "ferricstore_#{name}")
        ctx = start_flow_restart_instance(name, data_dir)

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)

          current_ctx =
            try do
              FerricStore.Instance.get(name)
            rescue
              _ -> ctx
            end

          stop_flow_restart_instance(current_ctx, delete?: true)
        end)

        type = uid("native-claim-restart")
        id = uid("flow-native-claim-restart")

        assert {:ok, _flow} =
                 impl_flow_create_and_get(ctx, id,
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 1_000
                 )

        assert {:ok, [claimed]} =
                 FerricStore.Impl.flow_claim_due(ctx, type,
                   state: "queued",
                   worker: "worker-native-restart",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert claimed.id == id
        assert claimed.state == "running"

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
        stop_flow_restart_instance(ctx, delete?: false)

        restarted = start_flow_restart_instance(name, data_dir)

        assert {:ok, restored} = FerricStore.Impl.flow_get(restarted, id)
        assert restored.state == "running"
        assert restored.lease_token == claimed.lease_token
        assert restored.fencing_token == claimed.fencing_token
        assert restored.lease_owner == "worker-native-restart"
      end

      test "standalone restart preserves spawned child group state" do
        name = :"flow_children_restart_#{System.unique_integer([:positive])}"
        data_dir = Path.join(System.tmp_dir!(), "ferricstore_#{name}")
        ctx = start_flow_restart_instance(name, data_dir)

        on_exit(fn ->
          current_ctx =
            try do
              FerricStore.Instance.get(name)
            rescue
              _ -> ctx
            end

          stop_flow_restart_instance(current_ctx, delete?: true)
        end)

        parent = uid("flow-parent-restart")
        child = uid("flow-child-restart")
        partition = uid("tenant-restart")

        assert {:ok, created_parent} =
                 impl_flow_create_and_get(ctx, parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition,
                   now_ms: 1_000
                 )

        assert {:ok, _waiting} =
                 impl_flow_spawn_children_and_get(
                   ctx,
                   parent,
                   [%{id: child, type: "child", state: "queued"}],
                   group_id: "fanout",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token,
                   now_ms: 1_010
                 )

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
        stop_flow_restart_instance(ctx, delete?: false)

        restarted = start_flow_restart_instance(name, data_dir)

        assert {:ok, restored_parent} =
                 FerricStore.Impl.flow_get(restarted, parent, partition_key: partition)

        assert restored_parent.state == "waiting_children"
        assert restored_parent.child_groups["fanout"]["children"][child] == "running"
        assert restored_parent.child_groups["fanout"]["resolved"] == nil

        assert {:ok, restored_child} =
                 FerricStore.Impl.flow_get(restarted, child, partition_key: partition)

        assert restored_child.parent_flow_id == parent
      end

      test "stored state retry policy drives retry exhaustion without command override" do
        type = uid("flow-policy-state-exhaust")
        id = uid("flow-policy-state-exhaust-id")

        assert {:ok, _policy} =
                 FerricStore.flow_policy_set(type,
                   retry: [max_retries: 10, exhausted_to: "failed"],
                   states: %{
                     "charge_card" => [retry: [max_retries: 0, exhausted_to: "payment_failed"]]
                   }
                 )

        assert {:ok, _} =
                 flow_create_and_get(id, type: type, state: "charge_card", run_at_ms: 1_000)

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   state: "charge_card",
                   worker: "worker-policy",
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, exhausted} =
                 flow_retry_and_get(claimed.id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: 2_000
                 )

        assert exhausted.state == "payment_failed"
        assert exhausted.next_run_at_ms == 2_000
      end

      test "command retry policy overrides stored state policy" do
        type = uid("flow-policy-command-override")
        id = uid("flow-policy-command-override-id")

        assert {:ok, _policy} =
                 FerricStore.flow_policy_set(type,
                   states: %{
                     "charge_card" => [retry: [max_retries: 0, exhausted_to: "payment_failed"]]
                   }
                 )

        assert {:ok, _} =
                 flow_create_and_get(id, type: type, state: "charge_card", run_at_ms: 1_000)

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   state: "charge_card",
                   worker: "worker-policy-override",
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, retried} =
                 flow_retry_and_get(claimed.id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: 2_000,
                   retry: [
                     max_retries: 3,
                     backoff: [kind: :fixed, base_ms: 5_000, max_ms: 5_000, jitter_pct: 0],
                     exhausted_to: "needs_review"
                   ]
                 )

        assert retried.state == "charge_card"
        assert retried.next_run_at_ms == 7_000
      end

      test "flow retry policy accepts thirty day backoff cap" do
        assert {:ok, policy} =
                 Ferricstore.Flow.RetryPolicy.normalize(
                   max_retries: 1000,
                   backoff: [
                     kind: :exponential,
                     base_ms: 2_592_000_000,
                     max_ms: 2_592_000_000,
                     jitter_pct: 100
                   ],
                   exhausted_to: "needs_review"
                 )

        assert policy.backoff.base_ms == 2_592_000_000
        assert policy.backoff.max_ms == 2_592_000_000
      end

      test "flow retry policy computes standard backoff timing" do
        assert {:ok, fixed} =
                 Ferricstore.Flow.RetryPolicy.normalize(
                   max_retries: 3,
                   backoff: [kind: :fixed, base_ms: 100, max_ms: 1_000, jitter_pct: 0],
                   exhausted_to: "failed"
                 )

        assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(fixed, "flow-a", 1, 1_000) == 1_100
        assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(fixed, "flow-a", 3, 1_000) == 1_100

        assert {:ok, linear} =
                 Ferricstore.Flow.RetryPolicy.normalize(
                   max_retries: 3,
                   backoff: [kind: :linear, base_ms: 100, max_ms: 1_000, jitter_pct: 0],
                   exhausted_to: "failed"
                 )

        assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(linear, "flow-a", 1, 1_000) == 1_100
        assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(linear, "flow-a", 2, 1_000) == 1_200
        assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(linear, "flow-a", 3, 1_000) == 1_300

        assert {:ok, exponential} =
                 Ferricstore.Flow.RetryPolicy.normalize(
                   max_retries: 3,
                   backoff: [kind: :exponential, base_ms: 100, max_ms: 250, jitter_pct: 0],
                   exhausted_to: "failed"
                 )

        assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(exponential, "flow-a", 1, 1_000) ==
                 1_100

        assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(exponential, "flow-a", 2, 1_000) ==
                 1_200

        assert Ferricstore.Flow.RetryPolicy.next_run_at_ms(exponential, "flow-a", 3, 1_000) ==
                 1_250

        assert {:ok, jittered} =
                 Ferricstore.Flow.RetryPolicy.normalize(
                   max_retries: 3,
                   backoff: [kind: :exponential, base_ms: 100, max_ms: 250, jitter_pct: 100],
                   exhausted_to: "failed"
                 )

        next_run = Ferricstore.Flow.RetryPolicy.next_run_at_ms(jittered, "flow-a", 3, 1_000)
        assert next_run >= 1_000
        assert next_run <= 1_250
      end

      test "flow retry policy keeps generated backoff values bounded and deterministic" do
        now_ms = 123_456
        attempts = [0, 1, 2, 3, 10, 1_000]
        base_values = [0, 1, 100, 60_000, 2_592_000_000]
        max_values = [0, 1, 50, 1_000, 2_592_000_000]

        for kind <- [:none, :fixed, :linear, :exponential],
            base_ms <- base_values,
            max_ms <- max_values,
            jitter_pct <- [0, 25, 100],
            attempt <- attempts do
          assert {:ok, policy} =
                   Ferricstore.Flow.RetryPolicy.normalize(
                     max_retries: 1_000,
                     backoff: [
                       kind: kind,
                       base_ms: base_ms,
                       max_ms: max_ms,
                       jitter_pct: jitter_pct
                     ],
                     exhausted_to: "failed"
                   )

          first =
            Ferricstore.Flow.RetryPolicy.next_run_at_ms(policy, "flow-prop", attempt, now_ms)

          second =
            Ferricstore.Flow.RetryPolicy.next_run_at_ms(policy, "flow-prop", attempt, now_ms)

          assert first == second
          assert first >= now_ms
          assert first <= now_ms + max_ms
        end

        assert Ferricstore.Flow.RetryPolicy.attempt_allowed?(
                 Ferricstore.Flow.RetryPolicy.default(),
                 3
               )

        refute Ferricstore.Flow.RetryPolicy.attempt_allowed?(
                 Ferricstore.Flow.RetryPolicy.default(),
                 4
               )

        assert {:ok, no_retries} =
                 Ferricstore.Flow.RetryPolicy.normalize(max_retries: 0, exhausted_to: "failed")

        refute Ferricstore.Flow.RetryPolicy.attempt_allowed?(no_retries, 1)
      end

      test "flow retry policy rejects old max_attempts name" do
        assert {:error, "ERR flow retry max_attempts was renamed to max_retries"} =
                 Ferricstore.Flow.RetryPolicy.normalize(
                   max_attempts: 3,
                   backoff: [kind: :fixed, base_ms: 100, max_ms: 1_000, jitter_pct: 0],
                   exhausted_to: "failed"
                 )
      end

      test "flow retry policy decoder rejects old stored max_attempts key" do
        blob =
          :erlang.term_to_binary(
            {:flow_policy_v1,
             %{
               type: "checkout",
               states: %{"charge_card" => %{retry: %{max_attempts: 2}}},
               retry: %{max_attempts: 5}
             }}
          )

        assert :error = Ferricstore.Flow.RetryPolicy.decode_flow_policy(blob)
      end

      test "flow_retry_many atomically reschedules one-partition batch" do
        partition = uid("tenant-retry-many")
        type = uid("bulk-retry-many")
        id_a = uid("retry-many-a")
        id_b = uid("retry-many-b")

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000
                 )

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-retry",
                   limit: 2,
                   now_ms: 1_000
                 )

        items =
          claimed
          |> Enum.sort_by(& &1.id)
          |> Enum.map(fn record ->
            %{id: record.id, lease_token: record.lease_token, fencing_token: record.fencing_token}
          end)

        assert {:ok, retried} =
                 flow_retry_many_and_get(partition, items,
                   error: "retry-error",
                   run_at_ms: 2_000,
                   now_ms: 2_000
                 )

        assert Enum.map(retried, & &1.id) == Enum.map(items, & &1.id)
        assert Enum.all?(retried, &(&1.state == "queued"))
        assert Enum.all?(retried, &(&1.attempts == 1))
        assert Enum.all?(retried, &(is_binary(&1.error_ref) and &1.error_ref != "retry-error"))

        assert {:ok, reclaimed} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-retry-b",
                   limit: 2,
                   now_ms: 2_000
                 )

        assert reclaimed |> Enum.map(& &1.id) |> Enum.sort() == [id_a, id_b] |> Enum.sort()
      end

      test "flow_retry_many rolls back when any item fails guard" do
        partition = uid("tenant-retry-many-rollback")
        type = uid("bulk-retry-many-rollback")
        id_a = uid("retry-many-good")
        id_b = uid("retry-many-bad")

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000
                 )

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-retry",
                   limit: 2,
                   now_ms: 1_000
                 )

        items =
          claimed
          |> Enum.sort_by(& &1.id)
          |> Enum.map(fn record ->
            fencing_token =
              if record.id == id_b, do: record.fencing_token + 1, else: record.fencing_token

            %{id: record.id, lease_token: record.lease_token, fencing_token: fencing_token}
          end)

        assert {:error, "ERR stale flow lease"} =
                 flow_retry_many_and_get(partition, items, now_ms: 2_000, run_at_ms: 2_000)

        assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
        assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
        assert fetched_a.state == "running"
        assert fetched_b.state == "running"
        assert fetched_a.version == 2
        assert fetched_b.version == 2
      end

      test "flow_retry_many rejects invalid retry policy before mutating records" do
        partition = uid("tenant-retry-many-policy")
        type = uid("bulk-retry-many-policy")
        id_a = uid("retry-many-policy-a")
        id_b = uid("retry-many-policy-b")

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000
                 )

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-retry-policy",
                   limit: 2,
                   now_ms: 1_000
                 )

        items =
          Enum.map(claimed, fn record ->
            %{id: record.id, lease_token: record.lease_token, fencing_token: record.fencing_token}
          end)

        assert {:error, "ERR flow retry max_retries must be between 0 and 1000"} =
                 flow_retry_many_and_get(partition, items,
                   retry: [max_retries: 1001],
                   now_ms: 2_000
                 )

        assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
        assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
        assert fetched_a.state == "running"
        assert fetched_b.state == "running"
        assert fetched_a.attempts == 0
        assert fetched_b.attempts == 0
      end

      test "expired running lease can be reclaimed" do
        id = uid("flow-reclaim")

        assert {:ok, _} =
                 flow_create_and_get(id, type: "lease", state: "queued", run_at_ms: 1_000)

        assert {:ok, [first]} =
                 FerricStore.flow_claim_due("lease",
                   state: "queued",
                   worker: "worker-a",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert first.lease_deadline_ms == 1_050

        assert {:ok, []} =
                 FerricStore.flow_claim_due("lease",
                   state: "running",
                   worker: "worker-b",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_049
                 )

        assert {:ok, [second]} =
                 FerricStore.flow_claim_due("lease",
                   state: "running",
                   worker: "worker-b",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_050
                 )

        assert second.id == id
        assert second.lease_owner == "worker-b"
        assert second.version == 3
        assert second.lease_token != first.lease_token
      end

      test "flow_reclaim exposes expired running lease reclaim" do
        id = uid("flow-reclaim-api")

        assert {:ok, _} =
                 flow_create_and_get(id, type: "lease-api", state: "queued", run_at_ms: 1_000)

        assert {:ok, [first]} =
                 FerricStore.flow_claim_due("lease-api",
                   worker: "worker-a",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, []} =
                 FerricStore.flow_reclaim("lease-api",
                   worker: "worker-b",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_049
                 )

        assert {:ok, [second]} =
                 FerricStore.flow_reclaim("lease-api",
                   worker: "worker-b",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_050
                 )

        assert second.id == id
        assert second.lease_owner == "worker-b"
        assert second.lease_token != first.lease_token
      end

      test "flow_extend_lease extends running lease with fencing guards" do
        type = uid("lease-extend")
        id = uid("flow-lease-extend")

        assert {:ok, _} = flow_create_and_get(id, type: type, run_at_ms: 1_000)

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-a",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, extended} =
                 FerricStore.flow_extend_lease(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   lease_ms: 500,
                   now_ms: 1_020
                 )

        assert extended.state == "running"
        assert extended.version == claimed.version + 1
        assert extended.lease_token == claimed.lease_token
        assert extended.fencing_token == claimed.fencing_token
        assert extended.lease_deadline_ms == 1_520

        assert {:ok, []} =
                 FerricStore.flow_reclaim(type,
                   worker: "worker-b",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_100
                 )

        assert {:error, "ERR stale flow lease"} =
                 FerricStore.flow_extend_lease(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token + 1,
                   lease_ms: 500,
                   now_ms: 1_030
                 )
      end

      test "expired lease reclaim fences old worker and records takeover history" do
        type = uid("lease-reclaim-fence")
        id = uid("flow-reclaim-fence")

        assert {:ok, _} = flow_create_and_get(id, type: type, run_at_ms: 1_000)

        assert {:ok, [first]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-a",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, [second]} =
                 FerricStore.flow_reclaim(type,
                   worker: "worker-b",
                   lease_ms: 500,
                   limit: 1,
                   now_ms: 1_050
                 )

        assert second.id == id
        assert second.lease_owner == "worker-b"
        assert second.fencing_token == first.fencing_token + 1
        assert second.lease_token != first.lease_token

        assert {:error, "ERR stale flow lease"} =
                 flow_complete_and_get(id, first.lease_token,
                   fencing_token: first.fencing_token,
                   now_ms: 1_060
                 )

        assert {:error, "ERR stale flow lease"} =
                 FerricStore.flow_extend_lease(id, first.lease_token,
                   fencing_token: first.fencing_token,
                   lease_ms: 500,
                   now_ms: 1_060
                 )

        assert {:ok, history} = FerricStore.flow_history(id, count: 10)

        assert history
               |> Enum.map(fn {_event_id, fields} -> fields["event"] end)
               |> Enum.frequencies() == %{"created" => 1, "claimed" => 2}

        assert Enum.any?(history, fn {_event_id, fields} ->
                 fields["event"] == "claimed" and fields["lease_owner"] == "worker-b"
               end)
      end

      test "flow_extend_lease rejects terminal flow" do
        type = uid("lease-extend-terminal")
        id = uid("flow-lease-extend-terminal")

        assert {:ok, _} = flow_create_and_get(id, type: type, run_at_ms: 1_000)

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-a",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, _completed} =
                 flow_complete_and_get(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: 1_010
                 )

        assert {:error, "ERR stale flow lease"} =
                 FerricStore.flow_extend_lease(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   lease_ms: 500,
                   now_ms: 1_020
                 )
      end
    end
  end
end
