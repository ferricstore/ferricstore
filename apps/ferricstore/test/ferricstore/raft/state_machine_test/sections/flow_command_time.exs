defmodule Ferricstore.Raft.StateMachineTest.Sections.FlowCommandTime do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

      describe "Flow command time" do
        @tag :mixed_fast_delete_flow_put
        test "Flow writes materialize a preceding compound fast delete", %{
          state: state,
          ets: ets
        } do
          setup_flow_indexes(state)

          collection_key = "flow-batch-delete"
          member_key = CompoundKey.set_member(collection_key, "member")

          {state, :ok} =
            StateMachine.apply(%{}, {:compound_put, member_key, "1", 0}, state)

          id = "flow-after-fast-delete"
          partition_key = "tenant-after-fast-delete"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          {_state, {:ok, [:ok, :ok]}} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:batch,
               [
                 {:compound_batch_delete, collection_key, [member_key]},
                 {:flow_create, state_key,
                  %{
                    id: id,
                    type: "flow-after-fast-delete",
                    state: "queued",
                    partition_key: partition_key
                  }}
               ]},
              state
            )

          assert [] == :ets.lookup(ets, member_key)
          assert %{id: ^id, state: "queued"} = flow_record!(state, state_key)
        end

        @tag :mixed_fast_delete_put_batch
        test "MSET falls back to staged puts after a compound fast delete", %{
          state: state,
          ets: ets
        } do
          collection_key = "mset-batch-delete"
          member_key = CompoundKey.hash_field(collection_key, "field")
          string_key = "mset-after-fast-delete"

          {state, :ok} =
            StateMachine.apply(%{}, {:compound_put, member_key, "old", 0}, state)

          {_state, {:ok, [:ok, :ok]}} =
            StateMachine.apply(
              %{},
              {:batch,
               [
                 {:compound_batch_delete, collection_key, [member_key]},
                 {:mset, [{string_key, "value", 0}]}
               ]},
              state
            )

          assert [] == :ets.lookup(ets, member_key)

          assert [{^string_key, "value", 0, _lfu, _file_id, _offset, 5}] =
                   :ets.lookup(ets, string_key)
        end

        @tag :mixed_fast_delete_rmw
        test "RMW commands observe a preceding compound fast delete", %{
          state: state,
          ets: ets
        } do
          collection_key = "hincrby-batch-delete"
          member_key = CompoundKey.hash_field(collection_key, "field")

          {state, 1} =
            StateMachine.apply(%{}, {:hset_single, collection_key, "field", "5"}, state)

          {_state, {:ok, [:ok, 1]}} =
            StateMachine.apply(
              %{},
              {:batch,
               [
                 {:compound_batch_delete, collection_key, [member_key]},
                 {:hincrby, collection_key, "field", 1}
               ]},
              state
            )

          assert [{^member_key, "1", 0, _lfu, _file_id, _offset, 1}] =
                   :ets.lookup(ets, member_key)
        end

        @tag :mixed_fast_delete_list_read
        test "list reads observe a preceding compound fast delete", %{
          state: state,
          ets: ets
        } do
          collection_key = "list-batch-delete"
          first_member_key = CompoundKey.list_element(collection_key, 0)

          {state, 2} =
            StateMachine.apply(
              %{},
              {:list_op, collection_key, {:rpush, ["first", "second"]}},
              state
            )

          {_state, {:ok, [:ok, ["second"]]}} =
            StateMachine.apply(
              %{},
              {:batch,
               [
                 {:compound_batch_delete, collection_key, [first_member_key]},
                 {:list_op, collection_key, {:lrange, 0, 0}}
               ]},
              state
            )

          assert [] == :ets.lookup(ets, first_member_key)
        end

        @tag :fast_delete_materialization_scope
        test "fast-delete materialization ignores unrelated pending writes", %{
          state: state,
          ets: ets
        } do
          fast_key = "tracked-fast-delete"
          unrelated_key = "already-staged-delete"
          fast_entry = {fast_key, "fast", 0, LFU.initial(), 0, 0, 4}
          unrelated_entry = {unrelated_key, "keep", 0, LFU.initial(), 0, 0, 4}

          :ets.insert(ets, [fast_entry, unrelated_entry])

          unrelated_tail =
            Enum.map(1..10_000, fn index ->
              {:delete, "unrelated-pending-delete-#{index}", nil}
            end)

          pending_writes =
            [{:delete, unrelated_key, nil}, {:delete, fast_key, nil} | unrelated_tail]

          assert {:ok, 1} =
                   Ferricstore.Raft.StateMachine.__materialize_pending_fast_deletes_for_test__(
                     state,
                     pending_writes,
                     [fast_key]
                   )

          assert [] == :ets.lookup(ets, fast_key)
          assert [^unrelated_entry] = :ets.lookup(ets, unrelated_key)
        end

        @tag :flow_policy_generation
        test "Flow create rejects a policy reference older than the local generation",
             %{
               state: state,
               ets: ets
             } do
          setup_flow_indexes(state)

          type = "captured-policy-create"
          partition_key = "captured-policy-tenant"
          state_key = Ferricstore.Flow.Keys.state_key("captured-policy-flow", partition_key)
          policy_key = Ferricstore.Flow.Keys.policy_key(type)

          {:ok, captured_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 1_000)

          {:ok, newer_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 9_000)

          newer_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(newer_policy, 2)

          :ets.insert(
            ets,
            {policy_key, newer_value, 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size(newer_value)}
          )

          {_state, {:error, "ERR stale flow policy generation"}} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: "captured-policy-flow",
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 policy_ref: flow_command_policy_ref(captured_policy, 1)
               }},
              state
            )

          assert [] = :ets.lookup(ets, state_key)

          {_state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: "captured-policy-flow",
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 policy_ref: flow_command_policy_ref(newer_policy, 2)
               }},
              state
            )

          assert flow_record!(state, state_key).max_active_ms == 9_000

          {:ok, newest_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 15_000)

          newest_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(newest_policy, 3)

          :ets.insert(
            ets,
            {policy_key, newest_value, 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size(newest_value)}
          )

          second_key = Ferricstore.Flow.Keys.state_key("captured-policy-flow-2", partition_key)

          {_state, {:error, "ERR stale flow policy generation"}} =
            StateMachine.apply(
              %{system_time: 1_001},
              {:flow_create, second_key,
               %{
                 id: "captured-policy-flow-2",
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 policy_ref: flow_command_policy_ref(newer_policy, 2)
               }},
              state
            )

          assert [] = :ets.lookup(ets, second_key)
        end

        @tag :flow_policy_generation
        test "Flow create rejects a policy generation not yet applied locally", %{
          state: state,
          ets: ets
        } do
          setup_flow_indexes(state)

          type = "future-policy-create"
          partition_key = "future-policy-tenant"
          state_key = Ferricstore.Flow.Keys.state_key("future-policy-flow", partition_key)

          {:ok, future_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 9_000)

          {_state, {:error, "ERR flow policy generation is not applied"}} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: "future-policy-flow",
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 policy_ref: flow_command_policy_ref(future_policy, 1)
               }},
              state
            )

          assert [] = :ets.lookup(ets, state_key)
        end

        @tag :flow_policy_generation
        test "a policy fence installs a future generation before its Flow command", %{
          state: state,
          ets: ets
        } do
          setup_flow_indexes(state)

          type = "fenced-future-policy"
          partition_key = "fenced-future-tenant"
          id = "fenced-future-flow"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
          policy_key = Ferricstore.Flow.Keys.policy_key(type)

          {:ok, future_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 9_000)

          future_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(future_policy, 1)

          command =
            {:flow_policy_fence, [{policy_key, future_value, 0}],
             {:flow_create, state_key,
              %{
                id: id,
                type: type,
                state: "queued",
                partition_key: partition_key,
                policy_ref: flow_command_policy_ref(future_policy, 1)
              }}}

          {_state, :ok} =
            StateMachine.apply(%{system_time: 1_000}, command, state)

          assert [{^policy_key, ^future_value, 0, _, _, _, _}] = :ets.lookup(ets, policy_key)
          assert flow_record!(state, state_key).max_active_ms == 9_000
        end

        @tag :flow_policy_generation
        test "Flow create rejects a conflicting digest at the same generation", %{
          state: state,
          ets: ets
        } do
          setup_flow_indexes(state)

          type = "conflicting-policy-create"
          partition_key = "conflicting-policy-tenant"
          state_key = Ferricstore.Flow.Keys.state_key("conflicting-policy-flow", partition_key)
          policy_key = Ferricstore.Flow.Keys.policy_key(type)

          {:ok, local_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 1_000)

          {:ok, conflicting_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 9_000)

          local_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(local_policy, 1)

          :ets.insert(
            ets,
            {policy_key, local_value, 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size(local_value)}
          )

          {_state, {:error, "ERR conflicting flow policy generation"}} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: "conflicting-policy-flow",
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 policy_ref: flow_command_policy_ref(conflicting_policy, 1)
               }},
              state
            )

          assert [] = :ets.lookup(ets, state_key)
        end

        @tag :flow_policy_target_guard
        test "an existing-flow command rejects a recreated flow incarnation", %{
          state: state,
          ets: ets
        } do
          setup_flow_indexes(state)

          id = "policy-incarnation-flow"
          partition_key = "policy-incarnation-tenant"
          old_type = "policy-incarnation-old"
          new_type = "policy-incarnation-new"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          {_state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: id,
                 type: old_type,
                 state: "queued",
                 partition_key: partition_key
               }},
              state
            )

          original = flow_record!(state, state_key)
          replacement = %{original | type: new_type, incarnation: original.incarnation + 1}
          encoded_replacement = Ferricstore.Flow.encode_record(replacement)

          :ets.insert(
            ets,
            {state_key, encoded_replacement, 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size(encoded_replacement)}
          )

          {_state, {:error, "ERR stale flow policy target"}} =
            StateMachine.apply(
              %{system_time: 1_001},
              {:flow_retry, state_key,
               %{
                 id: id,
                 partition_key: partition_key,
                 policy_ref: flow_command_policy_ref(%{type: old_type}, 0),
                 policy_guard: %{
                   state_key: state_key,
                   type: old_type,
                   incarnation: original.incarnation
                 }
               }},
              state
            )

          assert flow_record!(state, state_key).type == new_type
        end

        @tag :flow_policy_generation
        test "unstamped Flow commands are rejected instead of reading replica-local policy", %{
          state: state,
          ets: ets
        } do
          setup_flow_indexes(state)

          type = "unstamped-policy-create"
          partition_key = "unstamped-policy-tenant"
          state_key = Ferricstore.Flow.Keys.state_key("unstamped-policy-flow", partition_key)
          policy_key = Ferricstore.Flow.Keys.policy_key(type)

          {:ok, local_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 9_000)

          local_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(local_policy, 7)

          :ets.insert(
            ets,
            {policy_key, local_value, 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size(local_value)}
          )

          {_state, {:error, "ERR flow policy reference is required"}} =
            Ferricstore.Raft.StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: "unstamped-policy-flow",
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 policy_snapshot_captured: true,
                 policy_generation: 7,
                 policy_snapshot: local_policy
               }},
              state
            )

          assert :ets.lookup(ets, state_key) == []
        end

        @tag :flow_policy_generation
        test "generation-zero direct puts remain valid before a high-water exists", %{
          state: state,
          ets: ets
        } do
          type = "generation-zero-policy-put"
          policy_key = Ferricstore.Flow.Keys.policy_key(type)

          {:ok, policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 1_000)

          candidate_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy)

          {_state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_policy_put, policy_key, candidate_value, 0},
              state
            )

          assert [{^policy_key, ^candidate_value, 0, _, _, _, _}] =
                   :ets.lookup(ets, policy_key)
        end

        @tag :flow_policy_generation
        test "reserved policy puts reject invalid keys and values without storing them", %{
          state: state,
          ets: ets
        } do
          type = "invalid-reserved-policy-put"
          policy_key = Ferricstore.Flow.Keys.policy_key(type)
          invalid_key = "not-a-flow-policy-key"
          invalid_value = "not-a-flow-policy-value"

          {:ok, policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 1_000)

          valid_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy)

          {:ok, mismatched_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type <> "-other",
              max_active_ms: 2_000
            )

          mismatched_value =
            Ferricstore.Flow.RetryPolicy.encode_flow_policy(mismatched_policy)

          {_state, {:error, "ERR invalid flow policy value"}} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_policy_put, policy_key, invalid_value, 0},
              state
            )

          {_state, {:error, "ERR invalid flow policy key"}} =
            StateMachine.apply(
              %{system_time: 1_001},
              {:flow_policy_put, invalid_key, valid_value, 0},
              state
            )

          {_state, {:error, "ERR invalid flow policy value"}} =
            StateMachine.apply(
              %{system_time: 1_002},
              {:flow_policy_put, policy_key, mismatched_value, 0},
              state
            )

          {_state, {:error, "ERR invalid flow policy value"}} =
            StateMachine.apply(
              %{system_time: 1_003},
              {:flow_policy_allocate, policy_key, invalid_value, 0},
              state
            )

          {_state, {:error, "ERR invalid flow policy value"}} =
            StateMachine.apply(
              %{system_time: 1_004},
              {:flow_policy_allocate, policy_key, mismatched_value, 0},
              state
            )

          assert [] = :ets.lookup(ets, policy_key)
          assert [] = :ets.lookup(ets, invalid_key)
        end

        @tag :flow_policy_generation
        test "policy allocation stamps generations and rejects generationless payloads", %{
          state: state,
          ets: ets
        } do
          current_type = "current-cross-policy-wire"
          current_key = Ferricstore.Flow.Keys.policy_key(current_type)

          {:ok, current_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(current_type,
              max_active_ms: 1_000
            )

          current_input = Ferricstore.Flow.RetryPolicy.encode_flow_policy(current_policy)

          {_state, {:ok, current_value}} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_policy_allocate, current_key, current_input, 0},
              state
            )

          assert [{^current_key, ^current_value, 0, _, _, _, _}] =
                   :ets.lookup(ets, current_key)

          assert {:ok, {1, ^current_policy}} =
                   Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(current_value)

          beta_type = "beta-cross-policy-wire"
          beta_key = Ferricstore.Flow.Keys.policy_key(beta_type)

          {:ok, beta_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(beta_type,
              max_active_ms: 2_000
            )

          beta_input = :erlang.term_to_binary({:flow_policy_v1, beta_policy})

          {_state, {:error, "ERR invalid flow policy value"}} =
            StateMachine.apply(
              %{system_time: 2_000},
              {:flow_policy_allocate, beta_key, beta_input, 0},
              state
            )

          assert [] = :ets.lookup(ets, beta_key)

          {:ok, updated_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(current_type,
              max_active_ms: 3_000
            )

          updated_input = Ferricstore.Flow.RetryPolicy.encode_flow_policy(updated_policy)

          {_state, {:ok, updated_value}} =
            StateMachine.apply(
              %{system_time: 3_000},
              {:flow_policy_allocate, current_key, updated_input, 0},
              state
            )

          assert [{^current_key, ^updated_value, 0, _, _, _, _}] =
                   :ets.lookup(ets, current_key)

          assert {:ok, {2, ^updated_policy}} =
                   Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(updated_value)
        end

        @tag :flow_policy_generation
        test "successive generation-zero direct puts replay before a stamped policy", %{
          state: state,
          ets: ets
        } do
          type = "successive-generation-zero-policy-puts"
          policy_key = Ferricstore.Flow.Keys.policy_key(type)
          job_key = Ferricstore.Flow.Keys.policy_migration_job_key(type)

          {:ok, first_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type,
              indexed_state_meta: "owner"
            )

          {:ok, second_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type,
              indexed_state_meta: "team"
            )

          first_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(first_policy)
          second_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(second_policy)

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_policy_put, policy_key, first_value, 0},
              state
            )

          assert [{^job_key, first_job, 0, _, _, _, _}] = :ets.lookup(ets, job_key)

          assert {:ok, %{migration_generation: 1, indexed_state_meta: "owner"}} =
                   Ferricstore.Flow.PolicyMigration.decode_job(first_job)

          {_state, :ok} =
            StateMachine.apply(
              %{system_time: 2_000},
              {:flow_policy_put, policy_key, second_value, 0},
              state
            )

          assert [{^policy_key, ^second_value, 0, _, _, _, _}] = :ets.lookup(ets, policy_key)
          assert [{^job_key, second_job, 0, _, _, _, _}] = :ets.lookup(ets, job_key)

          assert {:ok, %{migration_generation: 2, indexed_state_meta: "team"}} =
                   Ferricstore.Flow.PolicyMigration.decode_job(second_job)
        end

        @tag :flow_policy_generation
        test "generationless policy payloads cannot overwrite a generation-stamped policy", %{
          state: state,
          ets: ets
        } do
          type = "stale-beta-stored-policy"
          policy_key = Ferricstore.Flow.Keys.policy_key(type)

          {:ok, versioned_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 2_000)

          {:ok, beta_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 1_000)

          versioned_value =
            Ferricstore.Flow.RetryPolicy.encode_flow_policy(versioned_policy, 2)

          beta_value = :erlang.term_to_binary({:flow_policy_v1, beta_policy})

          {_state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_policy_put, policy_key, versioned_value, 0},
              state
            )

          {_state, {:error, "ERR invalid flow policy value"}} =
            StateMachine.apply(
              %{system_time: 2_000},
              {:flow_policy_put, policy_key, beta_value, 0},
              state
            )

          assert [{^policy_key, ^versioned_value, 0, _, _, _, _}] =
                   :ets.lookup(ets, policy_key)
        end

        @tag :flow_policy_generation
        test "generation-zero direct puts replay across an active migration high-water", %{
          state: state,
          ets: ets
        } do
          type = "generation-zero-active-migration"
          policy_key = Ferricstore.Flow.Keys.policy_key(type)
          job_key = Ferricstore.Flow.Keys.policy_migration_job_key(type)

          {:ok, stored_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 2_000)

          {:ok, stale_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 1_000)

          stored_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(stored_policy)
          stale_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(stale_policy)

          job_value =
            Ferricstore.Flow.PolicyMigration.encode_job(type, 5, nil, :active)

          Enum.each([{policy_key, stored_value}, {job_key, job_value}], fn {key, value} ->
            :ets.insert(
              ets,
              {key, value, 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size(value)}
            )
          end)

          {_state, :ok} =
            StateMachine.apply(
              %{system_time: 2_000},
              {:flow_policy_put, policy_key, stale_value, 0},
              state
            )

          assert [{^policy_key, ^stale_value, 0, _, _, _, _}] = :ets.lookup(ets, policy_key)
          assert [{^job_key, ^job_value, 0, _, _, _, _}] = :ets.lookup(ets, job_key)
        end

        @tag :flow_policy_generation
        test "generation-zero direct puts replay across a completed migration high-water", %{
          state: state,
          ets: ets
        } do
          type = "generation-zero-completed-migration"
          policy_key = Ferricstore.Flow.Keys.policy_key(type)
          marker_key = Ferricstore.Flow.Keys.policy_migration_marker_key(type)

          {:ok, stored_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 2_000)

          {:ok, stale_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 1_000)

          stored_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(stored_policy)
          stale_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(stale_policy)

          marker_value =
            Ferricstore.Flow.PolicyMigration.encode_job(type, 7, nil, :done)

          Enum.each([{policy_key, stored_value}, {marker_key, marker_value}], fn {key, value} ->
            :ets.insert(
              ets,
              {key, value, 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size(value)}
            )
          end)

          {_state, :ok} =
            StateMachine.apply(
              %{system_time: 2_000},
              {:flow_policy_put, policy_key, stale_value, 0},
              state
            )

          assert [{^policy_key, ^stale_value, 0, _, _, _, _}] = :ets.lookup(ets, policy_key)
          assert [{^marker_key, ^marker_value, 0, _, _, _, _}] = :ets.lookup(ets, marker_key)
        end

        @tag :flow_policy_generation
        test "terminal batches are rejected without canonical policy metadata",
             %{
               state: state
             } do
          setup_flow_indexes(state)

          id = "unstamped-cross-terminal-many"
          type = "unstamped-cross-terminal-many"
          partition_key = "unstamped-cross-terminal-many-tenant"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 policy_reference_captured: true
               }},
              state
            )

          created = flow_record!(state, state_key)

          unstamped_attrs = [
            %{
              id: id,
              fencing_token: created.fencing_token,
              partition_key: partition_key
            }
          ]

          {_state, {:error, "ERR flow policy reference is required"}} =
            Ferricstore.Raft.StateMachine.apply(
              %{system_time: 2_000},
              {:flow_cancel_many, state_key, %{records: unstamped_attrs}},
              state
            )

          assert flow_record!(state, state_key).state == "queued"
        end

        @tag :flow_policy_generation
        test "policy allocation fails closed on corrupt migration high-water state", %{
          state: state,
          ets: ets
        } do
          Enum.each([:job, :marker], fn kind ->
            type = "corrupt-policy-high-water-#{kind}"
            policy_key = Ferricstore.Flow.Keys.policy_key(type)

            high_water_key =
              case kind do
                :job -> Ferricstore.Flow.Keys.policy_migration_job_key(type)
                :marker -> Ferricstore.Flow.Keys.policy_migration_marker_key(type)
              end

            {:ok, stored_policy} =
              Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 1_000)

            {:ok, new_policy} =
              Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 2_000)

            stored_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(stored_policy, 3)
            input_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(new_policy)
            corrupt_value = "corrupt-policy-migration-high-water"

            Enum.each([{policy_key, stored_value}, {high_water_key, corrupt_value}], fn {key,
                                                                                         value} ->
              :ets.insert(
                ets,
                {key, value, 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size(value)}
              )
            end)

            command = {:flow_policy_allocate, policy_key, input_value, 0}

            {_state, {:error, "ERR corrupt flow policy migration high-water"}} =
              StateMachine.apply(%{system_time: 1_000}, command, state)

            assert [{^policy_key, ^stored_value, 0, _, _, _, _}] =
                     :ets.lookup(ets, policy_key)

            assert [{^high_water_key, ^corrupt_value, 0, _, _, _, _}] =
                     :ets.lookup(ets, high_water_key)
          end)
        end

        @tag :flow_policy_generation
        test "policy allocation and install fail closed on a corrupt stored high-water", %{
          state: state,
          ets: ets
        } do
          type = "corrupt-stored-policy-high-water"
          policy_key = Ferricstore.Flow.Keys.policy_key(type)
          corrupt_value = "corrupt-stored-policy"

          {:ok, policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 2_000)

          allocation_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy)
          install_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy, 4)

          :ets.insert(
            ets,
            {policy_key, corrupt_value, 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size(corrupt_value)}
          )

          Enum.each(
            [
              {:flow_policy_allocate, policy_key, allocation_value, 0},
              {:flow_policy_put, policy_key, install_value, 0}
            ],
            fn command ->
              {_state, {:error, "ERR corrupt flow policy high-water"}} =
                StateMachine.apply(%{system_time: 1_000}, command, state)

              assert [{^policy_key, ^corrupt_value, 0, _, _, _, _}] =
                       :ets.lookup(ets, policy_key)
            end
          )
        end

        @tag :flow_policy_generation
        test "captured empty context does not fall back to local policy after a missing-record race",
             %{
               state: state,
               ets: ets
             } do
          setup_flow_indexes(state)

          type = "missing-record-policy-race"
          partition_key = "missing-record-policy-tenant"
          id = "missing-record-policy-flow"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          {_state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: id,
                 type: type,
                 state: "queued",
                 state_meta: %{"owner" => "alice"},
                 partition_key: partition_key,
                 policy_ref: flow_command_policy_ref(%{type: type}, 0)
               }},
              state
            )

          {:ok, local_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type,
              indexed_state_meta: "owner"
            )

          local_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(local_policy, 1)
          policy_key = Ferricstore.Flow.Keys.policy_key(type)

          :ets.insert(
            ets,
            {policy_key, local_value, 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size(local_value)}
          )

          created = flow_record!(state, state_key)

          {_state, :ok} =
            StateMachine.apply(
              %{system_time: 2_000},
              {:flow_transition, state_key,
               %{
                 id: id,
                 from_state: "queued",
                 to_state: "processing",
                 fencing_token: created.fencing_token,
                 partition_key: partition_key,
                 policy_reference_captured: true
               }},
              state
            )

          transitioned = flow_record!(state, state_key)
          assert Ferricstore.Flow.StateMeta.indexed_key(transitioned) == nil
        end

        @tag :flow_policy_generation
        test "Flow apply rejects an invalid policy reference before mutation", %{
          state: state,
          ets: ets
        } do
          setup_flow_indexes(state)

          partition_key = "invalid-policy-snapshot-tenant"

          state_key =
            Ferricstore.Flow.Keys.state_key("invalid-policy-snapshot-flow", partition_key)

          {_state, {:error, "ERR invalid flow policy reference"}} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: "invalid-policy-snapshot-flow",
                 type: "invalid-policy-snapshot",
                 state: "queued",
                 partition_key: partition_key,
                 policy_ref: %{type: "invalid-policy-snapshot", generation: 1}
               }},
              state
            )

          assert [] = :ets.lookup(ets, state_key)

          batch_key = Ferricstore.Flow.Keys.state_key("__batch__", partition_key)

          {_state, {:error, "ERR invalid flow policy reference"}} =
            StateMachine.apply(
              %{system_time: 1_001},
              {:flow_create_many, batch_key,
               %{
                 records: [
                   %{
                     id: "invalid-policy-snapshot-batch-flow",
                     type: "invalid-policy-snapshot",
                     state: "queued",
                     partition_key: partition_key
                   }
                 ],
                 policy_refs: %{
                   "invalid-policy-snapshot" => %{
                     type: "invalid-policy-snapshot",
                     generation: -1,
                     digest: <<0::256>>
                   }
                 }
               }},
              state
            )

          invalid_batch_state_key =
            Ferricstore.Flow.Keys.state_key(
              "invalid-policy-snapshot-batch-flow",
              partition_key
            )

          assert [] = :ets.lookup(ets, invalid_batch_state_key)

          overflow_state_key =
            Ferricstore.Flow.Keys.state_key("overflow-policy-snapshot-flow", partition_key)

          max_generation = Ferricstore.Flow.RetryPolicy.max_policy_generation()

          {_state, {:error, "ERR invalid flow policy reference"}} =
            StateMachine.apply(
              %{system_time: 1_002},
              {:flow_create, overflow_state_key,
               %{
                 id: "overflow-policy-snapshot-flow",
                 type: "invalid-policy-snapshot",
                 state: "queued",
                 partition_key: partition_key,
                 policy_ref: %{
                   type: "invalid-policy-snapshot",
                   generation: max_generation + 1,
                   digest: <<0::256>>
                 }
               }},
              state
            )

          assert [] = :ets.lookup(ets, overflow_state_key)
        end

        @tag :flow_policy_generation
        test "Flow apply rejects conflicting batch policy references before mutation", %{
          state: state,
          ets: ets
        } do
          setup_flow_indexes(state)

          type = "conflicting-policy-snapshot"
          partition_key = "conflicting-policy-snapshot-tenant"

          {:ok, first_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 1_000)

          {:ok, second_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 2_000)

          records = [
            %{
              id: "conflicting-policy-snapshot-a",
              type: type,
              state: "queued",
              partition_key: partition_key,
              policy_ref: flow_command_policy_ref(first_policy, 1)
            },
            %{
              id: "conflicting-policy-snapshot-b",
              type: type,
              state: "queued",
              partition_key: partition_key,
              policy_ref: flow_command_policy_ref(second_policy, 2)
            }
          ]

          batch_key = Ferricstore.Flow.Keys.state_key("__batch__", partition_key)

          {_state, {:error, "ERR conflicting flow policy references"}} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create_many, batch_key, %{records: records}},
              state
            )

          Enum.each(records, fn record ->
            key = Ferricstore.Flow.Keys.state_key(record.id, partition_key)
            assert [] = :ets.lookup(ets, key)
          end)
        end

        @tag :flow_policy_generation
        test "Flow batches resolve one deduplicated command-level policy reference", %{
          state: state,
          ets: ets
        } do
          setup_flow_indexes(state)

          type = "deduplicated-apply-policy"
          partition_key = "deduplicated-apply-tenant"
          policy_key = Ferricstore.Flow.Keys.policy_key(type)

          {:ok, local_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 9_000)

          local_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(local_policy, 2)

          :ets.insert(
            ets,
            {policy_key, local_value, 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size(local_value)}
          )

          records =
            Enum.map(["deduplicated-apply-a", "deduplicated-apply-b"], fn id ->
              %{id: id, type: type, state: "queued", partition_key: partition_key}
            end)

          attrs = %{
            records: records,
            policy_refs: %{
              type => flow_command_policy_ref(local_policy, 2)
            }
          }

          batch_key = Ferricstore.Flow.Keys.state_key("__batch__", partition_key)

          {_state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create_many, batch_key, attrs},
              state
            )

          Enum.each(records, fn record ->
            key = Ferricstore.Flow.Keys.state_key(record.id, partition_key)
            assert flow_record!(state, key).max_active_ms == 9_000
          end)
        end

        @tag :flow_policy_generation
        test "internal policy generations do not replace public policy versions" do
          {:ok, policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy("versioned-policy",
              version: "customer-v7",
              max_active_ms: 1_000
            )

          encoded = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy, 42)

          assert {:ok, {42, %{version: "customer-v7"} = decoded}} =
                   Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(encoded)

          assert {:ok, ^decoded} = Ferricstore.Flow.RetryPolicy.decode_flow_policy(encoded)

          candidate = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy)

          assert {:ok, {0, %{version: "customer-v7"}}} =
                   Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(candidate)

          beta = :erlang.term_to_binary({:flow_policy_v1, policy})

          assert :error = Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(beta)

          invalid = :erlang.term_to_binary({:flow_policy_v1, -1, policy})
          assert :error = Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(invalid)
        end

        test "Flow create does not return projection failure after committing state", %{
          state: state
        } do
          setup_flow_indexes(state)
          state = %{state | release_cursor_interval: 1}

          old_hook = Application.get_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)

          Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, fn _path,
                                                                                          _file_id,
                                                                                          _entries ->
            {:error, :forced_history_projection_failure}
          end)

          on_exit(fn -> restore_env(:flow_history_projector_lmdb_publish_hook, old_hook) end)

          id = "flow-projection-after-commit"
          partition_key = "tenant-projection-after-commit"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          {new_state, {:applied_at, 1, result}, effects} =
            StateMachine.apply(
              %{index: 1, term: 1, system_time: 1_000},
              {:flow_create, state_key,
               %{id: id, type: "projection-flow", state: "queued", partition_key: partition_key}},
              state
            )

          assert result == :ok
          assert %{id: ^id, state: "queued"} = flow_record!(new_state, state_key)
          refute Enum.any?(effects, &match?({:release_cursor, _index}, &1))
        end

        test "uses stamped apply time when Flow attrs omit now_ms", %{state: state} do
          setup_flow_indexes(state)

          id = "flow-command-time"
          type = "command-time"
          partition_key = "tenant-command-time"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{id: id, type: type, state: "queued", partition_key: partition_key}},
              state
            )

          created = flow_record!(state, state_key)
          assert created.created_at_ms == 1_000
          assert created.updated_at_ms == 1_000
          assert created.next_run_at_ms == 1_000

          due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

          {state, {:ok, [claimed]}} =
            StateMachine.apply(
              %{system_time: 1_250},
              {:flow_claim_due, due_key,
               %{
                 type: type,
                 state: "queued",
                 worker: "worker-command-time",
                 lease_ms: 500,
                 limit: 1,
                 priority: nil,
                 partition_key: partition_key
               }},
              state
            )

          assert claimed.updated_at_ms == 1_250
          assert claimed.lease_deadline_ms == 1_750

          running_due_key = Ferricstore.Flow.Keys.due_key(type, "running", 0, partition_key)

          running_state_index_key =
            Ferricstore.Flow.Keys.state_index_key(type, "running", partition_key)

          waiting_due_key = Ferricstore.Flow.Keys.due_key(type, "waiting", 0, partition_key)

          waiting_state_index_key =
            Ferricstore.Flow.Keys.state_index_key(type, "waiting", partition_key)

          inflight_index_key = Ferricstore.Flow.Keys.inflight_index_key(type, partition_key)

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 2_000},
              {:flow_transition, state_key,
               %{
                 id: id,
                 from_state: "running",
                 to_state: "waiting",
                 lease_token: claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 partition_key: partition_key
               }},
              state
            )

          transitioned = flow_record!(state, state_key)
          assert transitioned.updated_at_ms == 2_000
          assert transitioned.next_run_at_ms == 2_000

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   running_due_key,
                   id
                 ) ==
                   :miss

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   running_state_index_key,
                   id
                 ) == :miss

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   inflight_index_key,
                   id
                 ) == :miss

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   waiting_due_key,
                   id
                 ) ==
                   {:ok, 2_000.0}

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   waiting_state_index_key,
                   id
                 ) == {:ok, 2_000.0}
        end

        test "create_many stages Flow state writes into one append batch and projects history", %{
          state: state,
          shard_index: shard_index
        } do
          start_flow_history_projector!(state)

          setup_flow_indexes(state)

          handler_id = {:flow_create_many_append_batch, self(), make_ref()}

          :ok =
            :telemetry.attach(
              handler_id,
              [:ferricstore, :bitcask, :append],
              &__MODULE__.handle_flow_append_telemetry/4,
              {self(), shard_index}
            )

          partition_key = "tenant-batched-append"

          records =
            for id <- ["flow-batch-a", "flow-batch-b", "flow-batch-c"] do
              %{
                id: id,
                type: "append-batch",
                state: "queued",
                partition_key: partition_key,
                now_ms: 1_000
              }
            end

          try do
            {_state, :ok} =
              StateMachine.apply(
                %{system_time: 1_000},
                {:flow_create_many, nil, %{records: records}},
                state
              )

            assert Enum.all?(records, fn %{id: id, partition_key: partition_key} ->
                     state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
                     flow_record!(state, state_key).id == id
                   end)

            assert_receive {:flow_bitcask_append, measurements,
                            %{shard_index: ^shard_index, status: :ok}},
                           500

            assert measurements.batch_size == 15
            assert measurements.delete_count == 0
            assert measurements.batch_bytes > 0

            Enum.each(records, fn %{id: id, partition_key: partition_key} ->
              assert_flow_history_event!(state, id, partition_key, "1000-1", "created")
            end)

            refute_receive {:flow_bitcask_append, _measurements, _metadata}, 100
          after
            :telemetry.detach(handler_id)
          end
        end

        test "Ra-batched Flow commands share state append batch but keep semantic results", %{
          state: state,
          shard_index: shard_index
        } do
          start_flow_history_projector!(state)

          setup_flow_indexes(state)

          handler_id = {:flow_batch_append_per_command_results, self(), make_ref()}

          :ok =
            :telemetry.attach(
              handler_id,
              [:ferricstore, :bitcask, :append],
              &__MODULE__.handle_flow_append_telemetry/4,
              {self(), shard_index}
            )

          partition_key = "tenant-ra-batched-flow"

          create_a = %{
            id: "flow-ra-batch-a",
            type: "ra-batch",
            state: "queued",
            partition_key: partition_key,
            now_ms: 1_000
          }

          create_b = %{
            id: "flow-ra-batch-b",
            type: "ra-batch",
            state: "queued",
            partition_key: partition_key,
            now_ms: 1_000
          }

          try do
            {_state, {:ok, [result_a, duplicate_result, result_b]}} =
              StateMachine.apply(
                %{system_time: 1_000},
                {:batch,
                 [
                   {:flow_create, nil, create_a},
                   {:flow_create, nil, create_a},
                   {:flow_create, nil, create_b}
                 ]},
                state
              )

            assert :ok = result_a
            assert {:error, "ERR flow already exists"} = duplicate_result
            assert :ok = result_b

            assert flow_record!(
                     state,
                     Ferricstore.Flow.Keys.state_key("flow-ra-batch-a", partition_key)
                   ).id ==
                     "flow-ra-batch-a"

            assert flow_record!(
                     state,
                     Ferricstore.Flow.Keys.state_key("flow-ra-batch-b", partition_key)
                   ).id ==
                     "flow-ra-batch-b"

            assert_receive {:flow_bitcask_append, measurements,
                            %{shard_index: ^shard_index, status: :ok}},
                           500

            assert measurements.batch_size == 10
            assert measurements.delete_count == 0
            assert measurements.batch_bytes > 0

            assert_flow_history_event!(
              state,
              "flow-ra-batch-a",
              partition_key,
              "1000-1",
              "created"
            )

            assert_flow_history_event!(
              state,
              "flow-ra-batch-b",
              partition_key,
              "1000-1",
              "created"
            )

            refute_receive {:flow_bitcask_append, _measurements, _metadata}, 100
          after
            :telemetry.detach(handler_id)
          end
        end

        @tag :flow_due_catalog_page
        test "claim_due pages past 256 stale due indexes without hiding an eligible job", %{
          state: state
        } do
          setup_flow_indexes(state)

          type = "claim-due-catalog-page"
          partition_key = "eligible-partition"
          id = "eligible-flow"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: id,
                 type: type,
                 state: "waiting",
                 partition_key: partition_key,
                 now_ms: 1_000,
                 run_at_ms: 1_000
               }},
              state
            )

          native =
            Ferricstore.Flow.NativeOrderedIndex.get(
              state.flow_index_name,
              state.flow_lookup_name
            )

          stale_entries =
            for number <- 0..255 do
              stale_partition =
                "stale-#{String.pad_leading(Integer.to_string(number), 3, "0")}"

              {
                Ferricstore.Flow.Keys.due_key(type, "waiting", 0, stale_partition),
                "missing-flow-#{number}",
                number
              }
            end

          assert :ok =
                   Ferricstore.Flow.NativeOrderedIndex.put_new_entries(native, stale_entries)

          empty_due_key =
            Ferricstore.Flow.Keys.due_key(type, "waiting", 0, "empty-stale-partition")

          catalog =
            Enum.reduce(stale_entries, state.flow_due_catalog, fn {due_key, _id, score},
                                                                  catalog ->
              Ferricstore.Flow.DueCatalog.put(catalog, due_key, score)
            end)
            |> Ferricstore.Flow.DueCatalog.put(empty_due_key, 0)

          state = %{state | flow_due_catalog: catalog}

          {claimed_state, {:ok, [claimed]}} =
            StateMachine.apply(
              %{system_time: 2_000},
              {:flow_claim_due, nil,
               %{
                 type: type,
                 state: :any,
                 exclude_states: ["running"],
                 worker: "catalog-page-worker",
                 lease_ms: 30_000,
                 limit: 1,
                 priority: 0,
                 partition_key: :any,
                 now_ms: 2_000
               }},
              state
            )

          assert claimed.id == id
          assert claimed.state == "running"

          running_due_key =
            Ferricstore.Flow.Keys.due_key(type, "running", 0, partition_key)

          assert {:ok, %{keys: [^running_due_key]}} =
                   Ferricstore.Flow.DueCatalog.select(
                     claimed_state.flow_due_catalog,
                     type,
                     0,
                     :any,
                     :any,
                     2
                   )

          assert {:ok, %{keys: []}} =
                   Ferricstore.Flow.DueCatalog.select(
                     claimed_state.flow_due_catalog,
                     type,
                     0,
                     :any,
                     "waiting",
                     1
                   )

          orphan_type = "claim-due-empty-catalog-repair"

          orphan_due_key =
            Ferricstore.Flow.Keys.due_key(orphan_type, "waiting", 0, "orphan-partition")

          orphan_state = %{
            claimed_state
            | flow_due_catalog:
                Ferricstore.Flow.DueCatalog.put(
                  claimed_state.flow_due_catalog,
                  orphan_due_key,
                  0
                )
          }

          {repaired_state, {:ok, []}} =
            StateMachine.apply(
              %{system_time: 3_000},
              {:flow_claim_due, nil,
               %{
                 type: orphan_type,
                 state: :any,
                 exclude_states: [],
                 worker: "catalog-repair-worker",
                 lease_ms: 30_000,
                 limit: 1,
                 priority: 0,
                 partition_key: :any,
                 now_ms: 3_000
               }},
              orphan_state
            )

          assert {:ok, %{keys: []}} =
                   Ferricstore.Flow.DueCatalog.select(
                     repaired_state.flow_due_catalog,
                     orphan_type,
                     0,
                     :any,
                     :any,
                     1
                   )
        end

        test "claim_due stages claimed state records into one append batch", %{
          state: state,
          shard_index: shard_index
        } do
          setup_flow_indexes(state)

          partition_key = "tenant-claim-append"
          type = "claim-append"

          records =
            for id <- ["flow-claim-a", "flow-claim-b", "flow-claim-c"] do
              %{
                id: id,
                type: type,
                state: "queued",
                partition_key: partition_key,
                now_ms: 1_000,
                run_at_ms: 1_000,
                history_hot_max_events: 1
              }
            end

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create_many, nil, %{records: records}},
              state
            )

          assert Enum.all?(records, fn %{id: id, partition_key: partition_key} ->
                   state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
                   flow_record!(state, state_key).id == id
                 end)

          handler_id = {:flow_claim_due_append_batch, self(), make_ref()}

          :ok =
            :telemetry.attach(
              handler_id,
              [:ferricstore, :bitcask, :append],
              &__MODULE__.handle_flow_append_telemetry/4,
              {self(), shard_index}
            )

          due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

          try do
            {_state, {:ok, claimed}} =
              StateMachine.apply(
                %{system_time: 2_000},
                {:flow_claim_due, due_key,
                 %{
                   type: type,
                   state: "queued",
                   worker: "worker-claim-append",
                   lease_ms: 30_000,
                   limit: 3,
                   priority: nil,
                   partition_key: partition_key
                 }},
                state
              )

            assert Enum.map(claimed, & &1.id) == ["flow-claim-a", "flow-claim-b", "flow-claim-c"]

            assert_receive {:flow_bitcask_append, measurements,
                            %{shard_index: ^shard_index, status: :ok}},
                           500

            assert measurements.batch_size == 3
            assert measurements.delete_count == 0
            assert measurements.batch_bytes > 0

            refute_receive {:flow_bitcask_append, _measurements, _metadata}, 100
          after
            :telemetry.detach(handler_id)
          end
        end

        test "claim_due apply uses a claim-specific bulk index plan" do
          source = Ferricstore.Test.SourceFiles.state_machine_source()

          [_, body] =
            Regex.run(
              ~r/defp flow_apply_claim_batch\(\s*state,\s*due_key,\s*plans,\s*stale_due_ids,\s*deferred_timeout_records,\s*now_ms\s*\) do(.*?)(?=^\s*defp flow_deferred_timeout_due_index_deletes)/ms,
              source
            )

          assert body =~ "flow_claim_move_indexes(state, plans)"
          refute body =~ "flow_transition_move_indexes(state, plans)"
        end

        test "claim_due bulk index plan keeps metadata and reclaimed running indexes correct", %{
          state: state
        } do
          setup_flow_indexes(state)

          id = "flow-claim-bulk-index"
          type = "claim-bulk-index"
          partition_key = "tenant-claim-bulk-index"
          parent_id = "parent-claim-bulk-index"
          correlation_id = "corr-claim-bulk-index"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 parent_flow_id: parent_id,
                 correlation_id: correlation_id,
                 now_ms: 1_000,
                 run_at_ms: 1_000
               }},
              state
            )

          queued_due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

          {state, {:ok, [first_claim]}} =
            StateMachine.apply(
              %{system_time: 2_000},
              {:flow_claim_due, queued_due_key,
               %{
                 type: type,
                 state: "queued",
                 worker: "worker-old",
                 lease_ms: 100,
                 limit: 1,
                 priority: nil,
                 partition_key: partition_key
               }},
              state
            )

          running_due_key = Ferricstore.Flow.Keys.due_key(type, "running", 0, partition_key)

          running_state_key =
            Ferricstore.Flow.Keys.state_index_key(type, "running", partition_key)

          parent_index_key = Ferricstore.Flow.Keys.parent_index_key(parent_id, partition_key)

          correlation_index_key =
            Ferricstore.Flow.Keys.correlation_index_key(correlation_id, partition_key)

          inflight_key = Ferricstore.Flow.Keys.inflight_index_key(type, partition_key)
          old_worker_key = Ferricstore.Flow.Keys.worker_index_key("worker-old", partition_key)

          assert first_claim.lease_deadline_ms == 2_100

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   queued_due_key,
                   id
                 ) ==
                   :miss

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   running_due_key,
                   id
                 ) ==
                   {:ok, 2_100.0}

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   running_state_key,
                   id
                 ) ==
                   {:ok, 2_000.0}

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   parent_index_key,
                   id
                 ) ==
                   {:ok, 2_000.0}

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   correlation_index_key,
                   id
                 ) == {:ok, 2_000.0}

          assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, inflight_key, id) ==
                   {:ok, 2_100.0}

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   old_worker_key,
                   id
                 ) ==
                   {:ok, 2_100.0}

          {_, {:ok, [reclaimed]}} =
            StateMachine.apply(
              %{system_time: 2_200},
              {:flow_claim_due, running_due_key,
               %{
                 type: type,
                 state: "running",
                 worker: "worker-new",
                 lease_ms: 300,
                 limit: 1,
                 priority: nil,
                 partition_key: partition_key
               }},
              state
            )

          new_worker_key = Ferricstore.Flow.Keys.worker_index_key("worker-new", partition_key)

          assert reclaimed.lease_deadline_ms == 2_500
          assert reclaimed.lease_owner == "worker-new"

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   running_due_key,
                   id
                 ) ==
                   {:ok, 2_500.0}

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   running_state_key,
                   id
                 ) ==
                   {:ok, 2_200.0}

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   parent_index_key,
                   id
                 ) ==
                   {:ok, 2_200.0}

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   correlation_index_key,
                   id
                 ) == {:ok, 2_200.0}

          assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, inflight_key, id) ==
                   {:ok, 2_500.0}

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   old_worker_key,
                   id
                 ) ==
                   :miss

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   new_worker_key,
                   id
                 ) ==
                   {:ok, 2_500.0}
        end

        test "claim_due mirror does not enqueue full active state blobs", %{
          state: state,
          shard_index: shard_index
        } do
          old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
          old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
          old_max_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

          Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
          Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
          Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 10_000)
          state = %{state | flow_lmdb_mirror?: true}

          setup_flow_indexes(state)

          {:ok, writer_pid} =
            Ferricstore.Flow.LMDBWriter.start_link(
              instance_name: state.instance_name,
              shard_index: shard_index,
              data_dir: state.data_dir
            )

          on_exit(fn ->
            try do
              if Process.alive?(writer_pid), do: GenServer.stop(writer_pid, :normal, 5_000)
            catch
              :exit, _ -> :ok
            end

            restore_env(:flow_lmdb_mode, old_mode)
            restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
            restore_env(:flow_lmdb_max_batch_ops, old_max_ops)
          end)

          partition_key = "tenant-claim-lmdb-enqueue"
          type = "claim-lmdb-enqueue"

          records =
            for idx <- 1..10 do
              %{
                id: "flow-lmdb-claim-#{idx}",
                type: type,
                state: "queued",
                partition_key: partition_key,
                now_ms: 1_000,
                run_at_ms: 1_000
              }
            end

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create_many, nil, %{records: records}},
              state
            )

          assert Enum.all?(records, fn %{id: id, partition_key: partition_key} ->
                   state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
                   flow_record!(state, state_key).id == id
                 end)

          assert :ok = Ferricstore.Flow.LMDBWriter.flush(state.instance_name, shard_index)

          handler_id = {:flow_claim_due_lmdb_enqueue, self(), make_ref()}

          :ok =
            :telemetry.attach(
              handler_id,
              [:ferricstore, :flow, :lmdb_writer, :backlog],
              fn _event, measurements, metadata, test_pid ->
                send(test_pid, {:flow_lmdb_backlog, measurements, metadata})
              end,
              self()
            )

          due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

          try do
            {_state, {:ok, claimed}} =
              StateMachine.apply(
                %{system_time: 2_000},
                {:flow_claim_due, due_key,
                 %{
                   type: type,
                   state: "queued",
                   worker: "worker-claim-lmdb",
                   lease_ms: 30_000,
                   limit: 10,
                   priority: nil,
                   partition_key: partition_key
                 }},
                state
              )

            assert length(claimed) == 10

            refute_receive {:flow_lmdb_backlog, _measurements, _metadata}, 100
          after
            :telemetry.detach(handler_id)
          end
        end

        test "active Flow writes do not enqueue cold LMDB projection", %{
          state: state,
          shard_index: shard_index
        } do
          old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
          old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
          old_max_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

          Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
          Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
          Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 10_000)
          state = %{state | flow_lmdb_mirror?: true}

          setup_flow_indexes(state)

          {:ok, writer_pid} =
            Ferricstore.Flow.LMDBWriter.start_link(
              instance_name: state.instance_name,
              shard_index: shard_index,
              data_dir: state.data_dir
            )

          handler_id = {:active_flow_lmdb_projection, self(), make_ref()}

          :ok =
            :telemetry.attach(
              handler_id,
              [:ferricstore, :flow, :lmdb_writer, :backlog],
              fn _event, measurements, metadata, test_pid ->
                send(test_pid, {:flow_lmdb_backlog, measurements, metadata})
              end,
              self()
            )

          on_exit(fn ->
            :telemetry.detach(handler_id)

            try do
              if Process.alive?(writer_pid), do: GenServer.stop(writer_pid, :normal, 5_000)
            catch
              :exit, _ -> :ok
            end

            restore_env(:flow_lmdb_mode, old_mode)
            restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
            restore_env(:flow_lmdb_max_batch_ops, old_max_ops)
          end)

          partition_key = "tenant-active-lmdb-projection"
          type = "active-lmdb-projection"
          id = "flow-active-lmdb-projection"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 parent_flow_id: "parent-active-lmdb-projection",
                 correlation_id: "correlation-active-lmdb-projection",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               }},
              state
            )

          assert_receive {:flow_lmdb_backlog, %{pending_ops: 3}, %{shard_index: ^shard_index}},
                         500

          assert :ok = Ferricstore.Flow.LMDBWriter.flush(state.instance_name, shard_index)
          assert :not_found = Ferricstore.Flow.LMDB.get(state.flow_lmdb_path, state_key)

          due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

          {state, {:ok, [claimed]}} =
            StateMachine.apply(
              %{system_time: 2_000},
              {:flow_claim_due, due_key,
               %{
                 type: type,
                 state: "queued",
                 worker: "worker-active-lmdb-projection",
                 lease_ms: 30_000,
                 limit: 1,
                 priority: nil,
                 partition_key: partition_key
               }},
              state
            )

          refute_receive {:flow_lmdb_backlog, _measurements, _metadata}, 100

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 3_000},
              {:flow_complete, state_key,
               %{
                 id: claimed.id,
                 lease_token: claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 partition_key: partition_key,
                 now_ms: 3_000
               }},
              state
            )

          completed = flow_record!(state, state_key)
          assert completed.state == "completed"
          refute_receive {:flow_lmdb_backlog, _measurements, _metadata}, 100
        end

        test "Flow hot path does not depend on LMDB writer availability", %{
          state: state,
          shard_index: shard_index
        } do
          old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
          old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
          old_max_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

          Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
          Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
          Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 10_000)
          state = %{state | flow_lmdb_mirror?: true}

          setup_flow_indexes(state)

          writer_name = Ferricstore.Flow.LMDBWriter.name(state.instance_name, shard_index)
          assert Process.whereis(writer_name) == nil

          backlog_handler_id = {:flow_hot_path_lmdb_backlog, self(), make_ref()}
          degraded_handler_id = {:flow_hot_path_lmdb_degraded, self(), make_ref()}

          :ok =
            :telemetry.attach(
              backlog_handler_id,
              [:ferricstore, :flow, :lmdb_writer, :backlog],
              fn _event, measurements, metadata, test_pid ->
                send(test_pid, {:flow_lmdb_backlog, measurements, metadata})
              end,
              self()
            )

          :ok =
            :telemetry.attach(
              degraded_handler_id,
              [:ferricstore, :flow, :lmdb_mirror, :degraded],
              fn _event, measurements, metadata, test_pid ->
                send(test_pid, {:flow_lmdb_degraded, measurements, metadata})
              end,
              self()
            )

          on_exit(fn ->
            :telemetry.detach(backlog_handler_id)
            :telemetry.detach(degraded_handler_id)
            restore_env(:flow_lmdb_mode, old_mode)
            restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
            restore_env(:flow_lmdb_max_batch_ops, old_max_ops)
          end)

          partition_key = "tenant-flow-hot-path-lmdb"
          type = "flow-hot-path-lmdb"
          id = "flow-hot-path-lmdb"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{
                 id: id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 parent_flow_id: "parent-flow-hot-path-lmdb",
                 correlation_id: "correlation-flow-hot-path-lmdb",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               }},
              state
            )

          assert flow_record!(state, state_key).state == "queued"

          assert_receive {:flow_lmdb_degraded, %{count: 1},
                          %{shard_index: ^shard_index, reason: :writer_not_started}},
                         500

          due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

          {state, {:ok, [claimed]}} =
            StateMachine.apply(
              %{system_time: 2_000},
              {:flow_claim_due, due_key,
               %{
                 type: type,
                 state: "queued",
                 worker: "worker-flow-hot-path-lmdb",
                 lease_ms: 30_000,
                 limit: 1,
                 priority: nil,
                 partition_key: partition_key
               }},
              state
            )

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 3_000},
              {:flow_transition, state_key,
               %{
                 id: id,
                 from_state: "running",
                 to_state: "waiting",
                 lease_token: claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 partition_key: partition_key,
                 now_ms: 3_000
               }},
              state
            )

          waiting = flow_record!(state, state_key)
          assert waiting.state == "waiting"
          refute_receive {:flow_lmdb_backlog, _measurements, _metadata}, 100
          refute_receive {:flow_lmdb_degraded, _measurements, _metadata}, 100

          waiting_due_key = Ferricstore.Flow.Keys.due_key(type, "waiting", 0, partition_key)

          {state, {:ok, [claimed_again]}} =
            StateMachine.apply(
              %{system_time: 4_000},
              {:flow_claim_due, waiting_due_key,
               %{
                 type: type,
                 state: "waiting",
                 worker: "worker-flow-hot-path-lmdb",
                 lease_ms: 30_000,
                 limit: 1,
                 priority: nil,
                 partition_key: partition_key
               }},
              state
            )

          {state, :ok} =
            StateMachine.apply(
              %{system_time: 5_000},
              {:flow_complete, state_key,
               %{
                 id: id,
                 lease_token: claimed_again.lease_token,
                 fencing_token: claimed_again.fencing_token,
                 partition_key: partition_key,
                 now_ms: 5_000
               }},
              state
            )

          completed = flow_record!(state, state_key)
          assert completed.state == "completed"

          assert_receive {:flow_lmdb_degraded, %{count: 1},
                          %{shard_index: ^shard_index, reason: :writer_not_started}},
                         500
        end
      end

      defp flow_command_policy_ref(policy, generation) do
        encoded = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy, generation)

        %{
          type: Map.fetch!(policy, :type),
          generation: generation,
          digest: :crypto.hash(:sha256, encoded)
        }
      end
    end
  end
end
