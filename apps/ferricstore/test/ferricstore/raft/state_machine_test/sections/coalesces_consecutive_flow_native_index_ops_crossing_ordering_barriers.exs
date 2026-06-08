defmodule Ferricstore.Raft.StateMachineTest.Sections.CoalescesConsecutiveFlowNativeIndexOpsCrossingOrderingBarriers do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.{BlobCommand, StateMachine}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

      test "coalesces consecutive Flow native index ops without crossing ordering barriers" do
        native_a = make_ref()
        native_b = make_ref()

        ops = [
          {native_a, {:put_entries, [{"idx", "a", 1.0}]}},
          {native_a, {:put_entries, [{"idx", "b", 2.0}]}},
          {native_a, {:delete_members, "idx", ["a"]}},
          {native_a, {:put_entries, [{"idx", "a", 3.0}]}},
          {native_b, {:put_entries, [{"idx", "c", 4.0}]}},
          {native_b, {:put_entries, [{"idx", "d", 5.0}]}},
          {native_b, {:apply_claim_entries, [{:claim, "flow-1"}]}},
          {native_b, {:apply_claim_entries, [{:claim, "flow-2"}]}}
        ]

        assert [
                 {^native_a,
                  [
                    {:put_entries, [{"idx", "a", 1.0}]},
                    {:put_entries, [{"idx", "b", 2.0}]}
                  ]},
                 {^native_a, [{:delete_members, "idx", ["a"]}]},
                 {^native_a, [{:put_entries, [{"idx", "a", 3.0}]}]},
                 {^native_b,
                  [
                    {:put_entries, [{"idx", "c", 4.0}]},
                    {:put_entries, [{"idx", "d", 5.0}]}
                  ]},
                 {^native_b,
                  [
                    {:apply_claim_entries, [{:claim, "flow-1"}]},
                    {:apply_claim_entries, [{:claim, "flow-2"}]}
                  ]}
               ] = StateMachine.__coalesce_flow_native_ops_for_test__(ops)
      end

      test "Flow native index rolls back when apply fails after native flush", %{state: state} do
        id = "flow-native-rollback"
        type = "native-rollback"
        partition_key = "tenant-native-rollback"
        state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
        due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

        Process.put(:ferricstore_state_machine_after_flow_native_apply_batch_hook, fn _native,
                                                                                      _ops ->
          raise "native flush follow-up failed"
        end)

        try do
          assert_raise RuntimeError, ~r/native flush follow-up failed/, fn ->
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
          end
        after
          Process.delete(:ferricstore_state_machine_after_flow_native_apply_batch_hook)
        end

        assert [] = :ets.lookup(state.ets, state_key)

        assert native =
                 Ferricstore.Flow.NativeOrderedIndex.get(
                   state.flow_index_name,
                   state.flow_lookup_name
                 )

        assert [] =
                 Ferricstore.Flow.NativeOrderedIndex.range_slice(
                   native,
                   due_key,
                   :neg_inf,
                   :inf,
                   false,
                   0,
                   10
                 )
      end

      test "flow history projection shard routing uses stamped shard before hashing key" do
        ctx = %{slot_map: List.to_tuple(List.duplicate(0, 1024))}
        state = %{shard_index: 7}

        assert [3, 0] =
                 StateMachine.__flow_history_projection_shards_for_test__(ctx, state, [
                   %{key: "flow-history-a", shard_index: 3},
                   %{key: "flow-history-b"}
                 ])
      end

      test "flow history projection same-shard check trusts apply-stamped batches" do
        ctx = %{slot_map: List.to_tuple(List.duplicate(0, 1024))}
        state = %{shard_index: 7}

        assert StateMachine.__flow_history_projection_same_shard_for_test__(ctx, state, [
                 %{key: "flow-history-a", shard_index: 7},
                 %{key: "flow-history-b", shard_index: 7}
               ])

        refute StateMachine.__flow_history_projection_same_shard_for_test__(ctx, state, [
                 %{key: "flow-history-a"},
                 %{key: "flow-history-b"}
               ])
      end

      test "flow history projection entries carry direct value refs for projector dematerialization" do
        assert [
                 "f:{flow-fast-ref}:v:p:flow-fast-ref:2",
                 "f:{flow-fast-ref}:v:r:flow-fast-ref:2",
                 "external-ref"
               ] =
                 StateMachine.__flow_history_projection_value_refs_for_test__(%{
                   payload_ref: "f:{flow-fast-ref}:v:p:flow-fast-ref:2",
                   result_ref: "f:{flow-fast-ref}:v:r:flow-fast-ref:2",
                   error_ref: nil,
                   value_refs: %{
                     "shared" => %{ref: "external-ref"},
                     "empty" => ""
                   }
                 })
      end

      test "tagged LMDB mirror enqueue failure marks the failed shard", %{state: state} do
        instance_name = :"tagged_lmdb_missing_writer_#{System.unique_integer([:positive])}"
        enqueue_failures = :atomics.new(2, signed: false)
        degraded = :atomics.new(2, signed: false)

        state = %{
          state
          | shard_index: 0,
            instance_name: instance_name,
            instance_ctx: %{
              flow_lmdb_mirror_enqueue_failures: enqueue_failures,
              flow_lmdb_mirror_degraded: degraded
            }
        }

        test_pid = self()
        handler_id = {:tagged_lmdb_missing_writer, self(), make_ref()}

        :ok =
          :telemetry.attach(
            handler_id,
            [:ferricstore, :flow, :lmdb_mirror, :degraded],
            fn _event, measurements, metadata, _config ->
              send(test_pid, {:tagged_lmdb_degraded, measurements, metadata})
            end,
            nil
          )

        on_exit(fn -> :telemetry.detach(handler_id) end)

        assert {:error, {:lmdb_shard, 1, :writer_not_started}} =
                 StateMachine.__observe_tagged_lmdb_enqueue_failure_for_test__(
                   state,
                   [{:lmdb_shard, 1, {:put, "flow-lmdb-key", "value"}}]
                 )

        assert :atomics.get(enqueue_failures, 1) == 0
        assert :atomics.get(degraded, 1) == 0
        assert :atomics.get(enqueue_failures, 2) == 1
        assert :atomics.get(degraded, 2) == 1

        assert_receive {:tagged_lmdb_degraded, %{count: 1},
                        %{shard_index: 1, reason: :writer_not_started}},
                       500
      end

      # ---------------------------------------------------------------------------
      # init/1
      # ---------------------------------------------------------------------------

      describe "init/1" do
        test "creates initial state with expected fields", %{
          state: state,
          shard_index: shard_index
        } do
          assert state.shard_index == shard_index
          assert is_binary(state.shard_data_path)
          assert is_binary(state.active_file_path)
          assert is_atom(state.ets)
          assert state.applied_count == 0
        end

        test "derives canonical data_dir root from data/shard_N path" do
          root = Path.join(System.tmp_dir!(), "sm_data_dir_#{System.unique_integer([:positive])}")
          shard_path = Path.join([root, "data", "shard_0"])
          File.mkdir_p!(shard_path)

          ets = :ets.new(:"sm_data_dir_#{System.unique_integer([:positive])}", [:set, :public])

          try do
            state =
              StateMachine.init(%{
                shard_index: 0,
                shard_data_path: shard_path,
                active_file_id: 0,
                active_file_path: Path.join(shard_path, "00000.log"),
                ets: ets
              })

            assert state.data_dir == root
          after
            :ets.delete(ets)
            File.rm_rf!(root)
          end
        end

        test "rejects legacy shard_N path outside canonical data layout" do
          root =
            Path.join(System.tmp_dir!(), "sm_legacy_dir_#{System.unique_integer([:positive])}")

          shard_path = Path.join(root, "shard_0")
          File.mkdir_p!(shard_path)

          ets = :ets.new(:"sm_legacy_dir_#{System.unique_integer([:positive])}", [:set, :public])

          try do
            assert_raise ArgumentError, ~r/expected canonical shard data path/, fn ->
              StateMachine.init(%{
                shard_index: 0,
                shard_data_path: shard_path,
                active_file_id: 0,
                active_file_path: Path.join(shard_path, "00000.log"),
                ets: ets
              })
            end
          after
            :ets.delete(ets)
            File.rm_rf!(root)
          end
        end

        test "server_command apply resolves hook from checkpoint-safe instance_name" do
          name = :"sm_hook_instance_#{System.unique_integer([:positive])}"
          root = Path.join(System.tmp_dir!(), "sm_hook_#{System.unique_integer([:positive])}")
          shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
          File.mkdir_p!(shard_path)

          ets = :ets.new(:"sm_hook_ets_#{System.unique_integer([:positive])}", [:set, :public])

          ctx =
            FerricStore.Instance.build(name,
              data_dir: root,
              shard_count: 1,
              max_memory_bytes: 256 * 1024 * 1024,
              keydir_max_ram: 64 * 1024 * 1024
            )

          FerricStore.Instance.inject_callbacks(name,
            raft_apply_hook: fn {:echo, value} -> {:custom_instance, value} end
          )

          try do
            state =
              StateMachine.init(%{
                shard_index: 0,
                shard_data_path: shard_path,
                active_file_id: 0,
                active_file_path: Path.join(shard_path, "00000.log"),
                ets: ets,
                instance_name: name
              })

            assert {_state, {:custom_instance, "ok"}} =
                     StateMachine.apply(%{}, {:server_command, {:echo, "ok"}}, state)
          after
            FerricStore.Instance.cleanup(name)
            safe_delete_ets(ets)
            safe_delete_ets(elem(ctx.keydir_refs, 0))
            safe_delete_ets(ctx.hotness_table)
            safe_delete_ets(ctx.config_table)
            File.rm_rf!(root)
          end
        end

        test "stamped server_command exposes stamped HLC time to hook" do
          name = :"sm_hook_time_instance_#{System.unique_integer([:positive])}"

          root =
            Path.join(System.tmp_dir!(), "sm_hook_time_#{System.unique_integer([:positive])}")

          shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
          File.mkdir_p!(shard_path)

          ets =
            :ets.new(:"sm_hook_time_ets_#{System.unique_integer([:positive])}", [:set, :public])

          stamped_now = Ferricstore.HLC.now_ms() - 30_000

          ctx =
            FerricStore.Instance.build(name,
              data_dir: root,
              shard_count: 1,
              max_memory_bytes: 256 * 1024 * 1024,
              keydir_max_ram: 64 * 1024 * 1024
            )

          FerricStore.Instance.inject_callbacks(name,
            raft_apply_hook: fn :now_ms -> Ferricstore.CommandTime.now_ms() end
          )

          try do
            state =
              StateMachine.init(%{
                shard_index: 0,
                shard_data_path: shard_path,
                active_file_id: 0,
                active_file_path: Path.join(shard_path, "00000.log"),
                ets: ets,
                instance_name: name
              })

            assert {_state, ^stamped_now} =
                     StateMachine.apply(
                       %{system_time: stamped_now + 60_000},
                       {{:server_command, :now_ms}, %{hlc_ts: {stamped_now, 0}}},
                       state
                     )
          after
            FerricStore.Instance.cleanup(name)
            safe_delete_ets(ets)
            safe_delete_ets(elem(ctx.keydir_refs, 0))
            safe_delete_ets(ctx.hotness_table)
            safe_delete_ets(ctx.config_table)
            File.rm_rf!(root)
          end
        end
      end

      describe "Flow retention cleanup" do
        test "paged ETS scan reports incomplete when table disappeared" do
          table = :ets.new(:retention_paged_scan_deleted, [:set])
          :ets.delete(table)

          assert {[], false} =
                   StateMachine.__safe_ets_select_page_for_test__(
                     table,
                     [{{:"$1", :_}, [], [:"$1"]}],
                     10
                   )
        end

        test "does not crash if keydir is already gone during shutdown", %{state: state, ets: ets} do
          :ets.delete(ets)

          assert {_state, {:applied_at, 1, {:ok, %{flows: 0, history: 0, values: 0}}}, _effects} =
                   StateMachine.apply(
                     %{index: 1, system_time: 1_000},
                     {:flow_retention_cleanup, "__flow_retention_cleanup__:#{state.shard_index}",
                      %{now_ms: 1_000, limit: 10}},
                     state
                   )
        end

        test "does not crash on cold LMDB terminal candidate after keydir is gone", %{
          state: state,
          ets: ets
        } do
          state = %{state | flow_lmdb_mirror?: true}
          state_key = write_expired_lmdb_terminal!(state, now_ms: 1_000)

          assert {:ok, [^state_key]} =
                   Ferricstore.Flow.LMDB.expired_terminal_state_keys(
                     state.flow_lmdb_path,
                     1_000,
                     10
                   )

          :ets.delete(ets)

          assert {_state, {:applied_at, 1, {:ok, %{flows: 0, history: 0, values: 0}}}, _effects} =
                   StateMachine.apply(
                     %{index: 1, system_time: 1_000},
                     {:flow_retention_cleanup, "__flow_retention_cleanup__:#{state.shard_index}",
                      %{now_ms: 1_000, limit: 10}},
                     state
                   )
        end
      end
    end
  end
end
