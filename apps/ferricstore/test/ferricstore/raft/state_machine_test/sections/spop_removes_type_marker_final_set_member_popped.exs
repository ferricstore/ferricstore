defmodule Ferricstore.Raft.StateMachineTest.Sections.SpopRemovesTypeMarkerFinalSetMemberPopped do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.{BlobCommand, StateMachine}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

      test "SPOP removes the type marker when the final set member is popped", %{
        state: state,
        ets: ets
      } do
        key = "spop:type-marker"
        type_key = CompoundKey.type_key(key)
        member_key = CompoundKey.set_member(key, "only")

        :ets.insert(ets, {
          type_key,
          CompoundKey.encode_type(:set),
          0,
          LFU.initial(),
          0,
          0,
          byte_size("set")
        })

        :ets.insert(ets, {member_key, "1", 0, LFU.initial(), 0, 0, 1})

        result = apply_result_value(StateMachine.apply(%{index: 1}, {:spop, key, 1}, state))

        assert result == ["only"]
        assert [] == :ets.lookup(ets, member_key)
        assert [] == :ets.lookup(ets, type_key)
      end

      test "ZPOPMIN removes the type marker when the final zset member is popped", %{
        state: state,
        ets: ets
      } do
        key = "zpop:type-marker"
        type_key = CompoundKey.type_key(key)
        member_key = CompoundKey.zset_member(key, "only")

        :ets.insert(ets, {
          type_key,
          CompoundKey.encode_type(:zset),
          0,
          LFU.initial(),
          0,
          0,
          byte_size("zset")
        })

        :ets.insert(ets, {member_key, "1.0", 0, LFU.initial(), 0, 0, byte_size("1.0")})

        result = apply_result_value(StateMachine.apply(%{index: 1}, {:zpop, key, 1, :min}, state))

        assert result == ["only", "1.0"]
        assert [] == :ets.lookup(ets, member_key)
        assert [] == :ets.lookup(ets, type_key)
      end

      test "standalone sync append reports NIF errors instead of raising case clauses" do
        missing_path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore-missing-#{System.unique_integer([:positive])}/00000.log"
          )

        assert {:error, reason} =
                 StateMachine.__append_pending_batch_sync_for_test__(missing_path, [
                   {:put, "key", "value", 0}
                 ])

        assert reason != %CaseClauseError{}
      end

      test "standalone rollback tolerates keydir table disappearing during shutdown", %{
        state: state,
        ets: ets
      } do
        old_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

        Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, _batch ->
          :ets.delete(ets)
          {:error, :shutdown_keydir_removed}
        end)

        on_exit(fn -> restore_env(:standalone_durability_hook, old_hook) end)

        {_new_state, result} =
          StateMachine.apply_standalone_command({:put, "late_shutdown_key", "value", 0}, state)

        assert {:error, {:bitcask_append_failed, :shutdown_keydir_removed}} = result
        assert :undefined == :ets.whereis(ets)
      end

      test "Flow read during apply tolerates keydir table disappearing during shutdown", %{
        state: state,
        ets: ets
      } do
        setup_flow_indexes(state)
        :ets.delete(ets)

        state_key = Ferricstore.Flow.Keys.state_key("late-flow", "tenant-shutdown")

        {_state, result} =
          StateMachine.apply(
            %{system_time: 2_000},
            {:flow_transition, state_key,
             %{
               id: "late-flow",
               from_state: "running",
               to_state: "waiting",
               partition_key: "tenant-shutdown"
             }},
            state
          )

        assert {:error, "ERR flow not found"} = result
        assert :undefined == :ets.whereis(ets)
      end

      test "Flow claim_due native hydration tolerates keydir table disappearing during shutdown",
           %{
             state: state,
             ets: ets
           } do
        setup_flow_indexes(state)

        id = "late-claim-flow"
        type = "late-claim"
        partition_key = "tenant-shutdown"
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
               now_ms: 1_000,
               run_at_ms: 1_000
             }},
            state
          )

        :ets.delete(ets)

        due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

        {_state, result} =
          StateMachine.apply(
            %{system_time: 2_000},
            {:flow_claim_due, due_key,
             %{
               type: type,
               state: "queued",
               worker: "worker-shutdown",
               lease_ms: 30_000,
               limit: 1,
               priority: nil,
               partition_key: partition_key
             }},
            state
          )

        assert {:ok, []} = result
        assert :undefined == :ets.whereis(ets)
      end

      test "Flow claim_due native hot probe does not warm cold state values one by one", %{
        state: state,
        ets: ets
      } do
        id = "cold-native-probe"
        partition_key = "tenant-cold-native-probe"
        state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
        test_pid = self()

        :ets.insert(ets, {state_key, nil, 0, 1, {:waraft_projection, 999_999}, 0, 64})

        Process.put(:ferricstore_state_machine_cold_location_miss_hook, fn ->
          send(test_pid, :unexpected_cold_retry)
        end)

        try do
          assert [nil] =
                   StateMachine.__flow_read_claim_hot_values_for_test__(
                     state,
                     [{id, 1.0}],
                     nil,
                     partition_key
                   )

          refute_receive :unexpected_cold_retry, 20
        after
          Process.delete(:ferricstore_state_machine_cold_location_miss_hook)
        end
      end
    end
  end
end
