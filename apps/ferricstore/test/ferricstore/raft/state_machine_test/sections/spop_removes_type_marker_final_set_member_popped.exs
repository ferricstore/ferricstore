defmodule Ferricstore.Raft.StateMachineTest.Sections.SpopRemovesTypeMarkerFinalSetMemberPopped do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}
      alias Ferricstore.Store.Shard.{CompoundMemberIndex, ZSetIndex}

      test "SPOP removes the type marker when the final set member is popped", %{
        state: state,
        ets: ets
      } do
        setup_pop_indexes(state)
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
        CompoundMemberIndex.put(state.compound_member_index_name, member_key)

        result = apply_result_value(StateMachine.apply(%{index: 1}, {:spop, key, 1}, state))

        assert result == ["only"]
        assert [] == :ets.lookup(ets, member_key)
        assert [] == :ets.lookup(ets, type_key)
      end

      test "ZPOPMIN removes the type marker when the final zset member is popped", %{
        state: state,
        ets: ets
      } do
        setup_pop_indexes(state)
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
        CompoundMemberIndex.put(state.compound_member_index_name, member_key)

        assert :ok =
                 ZSetIndex.rebuild_key(
                   state.zset_score_index_name,
                   state.zset_score_lookup_name,
                   key,
                   [{"only", "1.0"}]
                 )

        result = apply_result_value(StateMachine.apply(%{index: 1}, {:zpop, key, 1, :min}, state))

        assert result == ["only", "1.0"]
        assert [] == :ets.lookup(ets, member_key)
        assert [] == :ets.lookup(ets, type_key)
      end

      test "SPOP count zero does not inspect set members", %{state: state, ets: ets} do
        setup_pop_indexes(state)
        key = "spop:zero"
        type_key = CompoundKey.type_key(key)
        member_key = CompoundKey.set_member(key, "broken")
        index_key = {CompoundKey.set_prefix(key), "broken"}

        :ets.insert(
          ets,
          {type_key, CompoundKey.encode_type(:set), 0, LFU.initial(), 0, 0, 3}
        )

        :ets.insert(ets, {member_key, nil, 0, LFU.initial(), :invalid, -1, -1})
        CompoundMemberIndex.put(state.compound_member_index_name, member_key)

        assert [] = apply_result_value(StateMachine.apply(%{index: 7}, {:spop, key, 0}, state))

        assert [{^index_key, ^member_key}] =
                 :ets.lookup(state.compound_member_index_name, index_key)

        assert [_row] = :ets.lookup(ets, member_key)
      end

      test "SPOP rejects malformed member metadata without mutation", %{state: state, ets: ets} do
        setup_pop_indexes(state)
        key = "spop:malformed"
        type_key = CompoundKey.type_key(key)
        member_key = CompoundKey.set_member(key, "broken")

        :ets.insert(
          ets,
          {type_key, CompoundKey.encode_type(:set), 0, LFU.initial(), 0, 0, 3}
        )

        :ets.insert(ets, {member_key, nil, 0, LFU.initial(), :invalid, -1, -1})
        CompoundMemberIndex.put(state.compound_member_index_name, member_key)

        result =
          apply_result_value(StateMachine.apply(%{index: 8}, {:spop, key, 1}, state))

        assert {:error, {:state_read_failed, _reason}} = result
        assert [_row] = :ets.lookup(ets, member_key)
        assert [_row] = :ets.lookup(ets, type_key)
      end

      @tag :compound_member_index_readiness
      test "SPOP refuses an unready partial member catalog without mutation", %{
        state: state,
        ets: ets
      } do
        setup_pop_indexes(state)
        key = "spop:partial-catalog"
        type_key = CompoundKey.type_key(key)
        indexed_key = CompoundKey.set_member(key, "indexed")
        missing_key = CompoundKey.set_member(key, "missing")
        index = state.compound_member_index_name

        :ets.insert(
          ets,
          {type_key, CompoundKey.encode_type(:set), 0, LFU.initial(), 0, 0, 3}
        )

        :ets.insert(ets, {indexed_key, "1", 0, LFU.initial(), 0, 0, 1})
        :ets.insert(ets, {missing_key, "1", 0, LFU.initial(), 0, 0, 1})

        :ets.delete_all_objects(index)
        CompoundMemberIndex.put(index, indexed_key)

        result = apply_result_value(StateMachine.apply(%{index: 11}, {:spop, key, 1}, state))

        assert {:error, {:state_read_failed, :compound_member_index_unavailable}} = result
        assert [_row] = :ets.lookup(ets, type_key)
        assert [_row] = :ets.lookup(ets, indexed_key)
        assert [_row] = :ets.lookup(ets, missing_key)
      end

      test "ready ZPOPMIN pops a cold member without reading its value", %{state: state, ets: ets} do
        setup_pop_indexes(state)
        key = "zpop:cold-ready"
        type_key = CompoundKey.type_key(key)
        member_key = CompoundKey.zset_member(key, "cold")

        :ets.insert(
          ets,
          {type_key, CompoundKey.encode_type(:zset), 0, LFU.initial(), 0, 0, 4}
        )

        :ets.insert(ets, {member_key, nil, 0, LFU.initial(), 999_999, 123, 3})
        CompoundMemberIndex.put(state.compound_member_index_name, member_key)

        assert :ok =
                 ZSetIndex.rebuild_key(
                   state.zset_score_index_name,
                   state.zset_score_lookup_name,
                   key,
                   [{"cold", "2.0"}]
                 )

        result = apply_result_value(StateMachine.apply(%{index: 9}, {:zpop, key, 1, :min}, state))

        assert ["cold", "2.0"] = result
        assert [] = :ets.lookup(ets, member_key)
        assert [] = :ets.lookup(ets, type_key)
      end

      test "lazy ZPOPMIN rebuild preserves data when a cold score is unavailable", %{
        state: state,
        ets: ets
      } do
        setup_pop_indexes(state)
        key = "zpop:cold-rebuild"
        type_key = CompoundKey.type_key(key)
        member_key = CompoundKey.zset_member(key, "cold")

        :ets.insert(
          ets,
          {type_key, CompoundKey.encode_type(:zset), 0, LFU.initial(), 0, 0, 4}
        )

        :ets.insert(ets, {member_key, nil, 0, LFU.initial(), 999_999, 123, 3})
        CompoundMemberIndex.put(state.compound_member_index_name, member_key)

        result =
          apply_result_value(StateMachine.apply(%{index: 10}, {:zpop, key, 1, :min}, state))

        assert {:error, {:state_read_failed, _reason}} = result
        assert [_row] = :ets.lookup(ets, member_key)
        assert [_row] = :ets.lookup(ets, type_key)
        refute ZSetIndex.ready?(state.zset_score_lookup_name, key)
      end

      defp setup_pop_indexes(state) do
        CompoundMemberIndex.ensure_table!(state.compound_member_index_name)
        CompoundMemberIndex.reset(state.compound_member_index_name)
        ensure_pop_index_table(state.zset_score_index_name, :ordered_set)
        ensure_pop_index_table(state.zset_score_lookup_name, :set)
        :ets.delete_all_objects(state.zset_score_index_name)
        :ets.delete_all_objects(state.zset_score_lookup_name)

        on_exit(fn ->
          safe_delete_ets(state.compound_member_index_name)
          safe_delete_ets(state.zset_score_index_name)
          safe_delete_ets(state.zset_score_lookup_name)
        end)
      end

      defp ensure_pop_index_table(table, type) do
        if :ets.whereis(table) == :undefined do
          :ets.new(table, [type, :public, :named_table])
        end

        table
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

      test "standalone ENOSPC append rolls back keydir state and clears pressure after recovery",
           %{
             state: state,
             ets: ets
           } do
        root = Path.join(System.tmp_dir!(), "sm_enospc_#{System.unique_integer([:positive])}")
        shard_index = 0
        shard_path = Ferricstore.DataDir.shard_data_path(root, shard_index)
        active_file_path = Path.join(shard_path, "00000.log")
        instance_name = :"sm_enospc_#{System.unique_integer([:positive])}"
        old_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

        File.mkdir_p!(shard_path)
        File.touch!(active_file_path)
        Ferricstore.Store.ActiveFile.init(1)

        instance_ctx = %{
          FerricStore.Instance.build(instance_name, shard_count: 1, data_dir: root)
          | keydir_refs: {ets},
            hot_cache_max_value_size: 64
        }

        Ferricstore.Store.ActiveFile.publish(
          instance_ctx,
          shard_index,
          0,
          active_file_path,
          shard_path
        )

        failing_key = "enospc_key"
        recovered_key = "after_enospc_key"

        Application.put_env(:ferricstore, :standalone_durability_hook, fn
          _path, [{:put, ^failing_key, "value", 0}] -> {:error, :enospc}
          _path, _batch -> :passthrough
        end)

        on_exit(fn -> restore_env(:standalone_durability_hook, old_hook) end)

        staged_state = %{
          state
          | shard_index: shard_index,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: active_file_path,
            instance_ctx: instance_ctx
        }

        try do
          {_new_state, result} =
            StateMachine.apply_standalone_command({:put, failing_key, "value", 0}, staged_state)

          assert {:error, {:bitcask_append_failed, :enospc}} = result
          assert [] == :ets.lookup(ets, failing_key)
          assert Ferricstore.Store.DiskPressure.under_pressure?(instance_ctx, shard_index)

          Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, _batch ->
            :passthrough
          end)

          {_new_state, :ok} =
            StateMachine.apply_standalone_command(
              {:put, recovered_key, "recovered", 0},
              staged_state
            )

          assert [{^recovered_key, "recovered", 0, _lfu, 0, _offset, 9}] =
                   :ets.lookup(ets, recovered_key)

          refute Ferricstore.Store.DiskPressure.under_pressure?(instance_ctx, shard_index)
        after
          Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
          File.rm_rf!(root)
        end
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
