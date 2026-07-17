defmodule Ferricstore.Raft.StateMachineTest.Sections.Apply3DeleteKey do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

      describe "apply/3 with {:delete, key}" do
        test "removes key from ETS", %{state: state, ets: ets} do
          {state2, :ok} = StateMachine.apply(%{}, {:put, "del_me", "val", 0}, state)
          {state3, :ok} = StateMachine.apply(%{}, {:delete, "del_me"}, state2)

          assert state3.applied_count == 2
          assert [] == :ets.lookup(ets, "del_me")
        end

        test "delete nonexistent key returns :ok", %{state: state} do
          {_new_state, result} = StateMachine.apply(%{}, {:delete, "nonexistent"}, state)
          assert result == :ok
        end

        test "delete after delete is idempotent", %{state: state} do
          {s1, :ok} = StateMachine.apply(%{}, {:put, "k", "v", 0}, state)
          {s2, :ok} = StateMachine.apply(%{}, {:delete, "k"}, s1)
          {_s3, :ok} = StateMachine.apply(%{}, {:delete, "k"}, s2)
        end

        @tag :prob_metadata_path_confinement
        test "delete never trusts a probabilistic metadata path from the stored value", %{
          state: state,
          dir: dir
        } do
          key = "crafted-prob-metadata"
          victim = Path.join(dir, "must-not-be-unlinked")
          prob_dir = Path.join(dir, "prob")
          expected_sidecar = Ferricstore.ProbFile.path(prob_dir, key, "bloom")

          File.mkdir_p!(prob_dir)
          File.write!(victim, "keep")
          File.write!(expected_sidecar, "sidecar")

          metadata =
            Ferricstore.TermCodec.encode(
              {:bloom_meta, %{path: victim, num_bits: 8, num_hashes: 1}}
            )

          {state2, :ok} = StateMachine.apply(%{}, {:put, key, metadata, 0}, state)
          {_state3, :ok} = StateMachine.apply(%{}, {:delete, key}, state2)

          assert File.read!(victim) == "keep"
          refute File.exists?(expected_sidecar)
        end

        test "missing active file fails delete and keeps ETS entry", %{state: state, ets: ets} do
          {state2, :ok} =
            StateMachine.apply(%{}, {:put, "missing_active_delete", "val", 0}, state)

          old_entry = :ets.lookup(ets, "missing_active_delete")
          missing_state = state_with_missing_active_file(state2)

          {_new_state, result} =
            StateMachine.apply(%{}, {:delete, "missing_active_delete"}, missing_state)

          assert {:error, :active_file_unavailable} = result
          assert old_entry == :ets.lookup(ets, "missing_active_delete")
        end

        test "missing active file does not remove prob file during failed delete", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "missing_active_prob_delete"
          prob_dir = Path.join(dir, "prob")
          File.mkdir_p!(prob_dir)
          prob_path = Ferricstore.ProbFile.path(prob_dir, key, "cms")
          File.write!(prob_path, "cms")

          meta = :erlang.term_to_binary({:cms_meta, %{width: 1, depth: 1}})
          {state2, :ok} = StateMachine.apply(%{}, {:put, key, meta, 0}, state)
          old_entry = :ets.lookup(ets, key)
          missing_state = state_with_missing_active_file(state2)

          {_new_state, result} = StateMachine.apply(%{}, {:delete, key}, missing_state)

          assert {:error, :active_file_unavailable} = result
          assert old_entry == :ets.lookup(ets, key)
          assert File.exists?(prob_path)
        end

        test "prob sidecar cleanup fsync failure emits telemetry", %{
          state: state,
          dir: dir,
          shard_index: shard_index
        } do
          key = "prob_delete_fsync_telemetry"
          prob_dir = Path.join(dir, "prob")
          File.mkdir_p!(prob_dir)
          prob_path = Ferricstore.ProbFile.path(prob_dir, key, "cms")
          File.write!(prob_path, "cms")

          handler_id = {__MODULE__, self(), :prob_sidecar_delete_failed}

          :telemetry.attach(
            handler_id,
            [:ferricstore, :prob, :sidecar_delete_failed],
            fn event, measurements, metadata, pid ->
              send(pid, {:prob_sidecar_delete_failed, event, measurements, metadata})
            end,
            self()
          )

          Process.put(:ferricstore_prob_fsync_dir_hook, fn
            ^prob_dir -> {:error, :eio}
            _path -> :ok
          end)

          on_exit(fn ->
            :telemetry.detach(handler_id)
            Process.delete(:ferricstore_prob_fsync_dir_hook)
          end)

          meta = :erlang.term_to_binary({:cms_meta, %{width: 1, depth: 1}})
          {state2, :ok} = StateMachine.apply(%{}, {:put, key, meta, 0}, state)
          {_state3, :ok} = StateMachine.apply(%{}, {:delete, key}, state2)

          assert_receive {:prob_sidecar_delete_failed,
                          [:ferricstore, :prob, :sidecar_delete_failed], %{count: 1},
                          %{
                            shard_index: ^shard_index,
                            path: ^prob_path,
                            reason: {:fsync_dir_failed, :prob_file_dir, :eio}
                          }}
        end

        test "append failure rolls back deleted entry in a mixed batch", %{state: state, ets: ets} do
          {state2, :ok} =
            StateMachine.apply(%{}, {:put, "delete_append_failure_keep", "old", 0}, state)

          old_entry = :ets.lookup(ets, "delete_append_failure_keep")
          file_id = 9_100_000 + :erlang.unique_integer([:positive])
          bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
          File.mkdir_p!(bad_active_path)

          bad_state = %{state2 | active_file_id: file_id, active_file_path: bad_active_path}

          {_new_state, result} =
            StateMachine.apply(
              %{},
              {:batch,
               [
                 {:put, "delete_append_failure_new", "new", 0},
                 {:delete, "delete_append_failure_keep"}
               ]},
              bad_state
            )

          assert {:error, {:bitcask_append_failed, _reason}} = result
          assert old_entry == :ets.lookup(ets, "delete_append_failure_keep")
          assert [] == :ets.lookup(ets, "delete_append_failure_new")
        end
      end

      # ---------------------------------------------------------------------------
      # apply/3 with :batch
      # ---------------------------------------------------------------------------

      describe "apply/3 with {:batch, commands}" do
        test "WARaft projection writer cannot observe unpublished pending ETS rows", %{
          state: state,
          ets: ets
        } do
          parent = self()

          writer = fn
            [{:put, "waraft_projection_stage", "value", 0}] ->
              send(
                parent,
                {:projection_writer_observed, Process.get(:sm_standalone_staged_apply),
                 :ets.lookup(ets, "waraft_projection_stage")}
              )

              {:ok, {:waraft_apply_projection, 1}, [{:put, 0, byte_size("value")}]}
          end

          assert {_new_state, {:applied_at, 1, :ok}, _effects} =
                   StateMachine.apply_waraft_segment_command(
                     {:put, "waraft_projection_stage", "value", 0},
                     %{index: 1, term: 1},
                     state,
                     writer
                   )

          assert_receive {:projection_writer_observed, true, []}, 500

          assert [
                   {"waraft_projection_stage", "value", 0, _lfu, {:waraft_apply_projection, 1}, 0,
                    5}
                 ] =
                   :ets.lookup(ets, "waraft_projection_stage")
        end

        test "WARaft projection failure never publishes pending ETS rows", %{
          state: state,
          ets: ets
        } do
          parent = self()

          writer = fn
            [{:put, "waraft_projection_failure_stage", "value", 0}] ->
              send(
                parent,
                {:projection_writer_failure_observed, Process.get(:sm_standalone_staged_apply),
                 :ets.lookup(ets, "waraft_projection_failure_stage")}
              )

              {:error, :forced_projection_failure}
          end

          assert {_new_state,
                  {:applied_at, 2,
                   {:error, {:waraft_projection_failed, :forced_projection_failure}}}, _effects} =
                   StateMachine.apply_waraft_segment_command(
                     {:put, "waraft_projection_failure_stage", "value", 0},
                     %{index: 2, term: 1},
                     state,
                     writer
                   )

          assert_receive {:projection_writer_failure_observed, true, []}, 500
          assert [] = :ets.lookup(ets, "waraft_projection_failure_stage")
        end

        test "uses raft meta system_time for TTL checks inside batch read-modify-write", %{
          state: state,
          ets: ets
        } do
          local_now = Ferricstore.HLC.now_ms()
          apply_now = local_now - 20_000
          expires_after_apply_time = apply_now + 10_000

          :ets.insert(
            ets,
            {"batch_meta_time_incr", "5", expires_after_apply_time,
             Ferricstore.Store.LFU.initial(), 0, 0, byte_size("5")}
          )

          {_new_state, {:ok, [{:ok, 6}]}} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:batch, [{:incr, "batch_meta_time_incr", 1}]},
              state
            )

          assert [{"batch_meta_time_incr", "6", ^expires_after_apply_time, _, _, _, _}] =
                   :ets.lookup(ets, "batch_meta_time_incr")
        end

        test "processes all commands and returns results list", %{state: state, ets: ets} do
          commands = [
            {:put, "batch_a", "val_a", 0},
            {:put, "batch_b", "val_b", 0},
            {:put, "batch_c", "val_c", 0}
          ]

          {new_state, {:ok, results}} =
            StateMachine.apply(%{}, {:batch, commands}, state)

          assert results == [:ok, :ok, :ok]
          assert new_state.applied_count == 3

          # All keys in ETS (single-table format)
          assert [{"batch_a", "val_a", 0, _, _, _, _}] = :ets.lookup(ets, "batch_a")
          assert [{"batch_b", "val_b", 0, _, _, _, _}] = :ets.lookup(ets, "batch_b")
          assert [{"batch_c", "val_c", 0, _, _, _, _}] = :ets.lookup(ets, "batch_c")
        end

        test "RMW command in same batch reads prior pending large put", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          key = "batch_large_then_append"
          large = String.duplicate("L", 70_000)
          expected = large <> "!"

          {new_state, {:ok, results}} =
            StateMachine.apply(%{}, {:batch, [{:put, key, large, 0}, {:append, key, "!"}]}, state)

          assert results == [:ok, {:ok, byte_size(expected)}]
          assert new_state.applied_count == 2

          assert [{^key, nil, 0, _, 0, offset, value_size}] = :ets.lookup(ets, key)
          assert value_size == byte_size(expected)
          assert {:ok, ^expected} = NIF.v2_pread_at(active_file_path, offset)
        end

        test "probabilistic command in batch does not drop earlier pending puts", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          commands = [
            {:put, "batch_before_prob", "keep-me", 0},
            {:cms_create, "batch_cms", 20, 4}
          ]

          {_new_state, {:ok, [:ok, :ok]}} =
            StateMachine.apply(%{}, {:batch, commands}, state)

          value_size = byte_size("keep-me")

          assert [{"batch_before_prob", "keep-me", 0, _, 0, _offset, ^value_size}] =
                   :ets.lookup(ets, "batch_before_prob")

          assert {:ok, records} = NIF.v2_scan_file(active_file_path)

          assert Enum.any?(
                   records,
                   &match?({"batch_before_prob", _off, ^value_size, 0, false}, &1)
                 )

          assert Enum.any?(records, &match?({"batch_cms", _off, _size, 0, false}, &1))
        end

        test "CMS merge resolves replicated source keys through local shard paths" do
          root =
            Path.join(System.tmp_dir!(), "sm_cms_merge_#{System.unique_integer([:positive])}")

          instance_name = :"sm_cms_merge_#{System.unique_integer([:positive])}"
          ctx = FerricStore.Instance.build(instance_name, data_dir: root, shard_count: 4)

          src_key = key_for_shard(ctx, "cms_src", 1)
          dst_key = key_for_shard(ctx, "cms_dst", 0)
          src_dir = Path.join(Ferricstore.DataDir.shard_data_path(root, 1), "prob")
          dst_shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
          dst_dir = Path.join(dst_shard_path, "prob")
          src_path = prob_test_path(src_dir, src_key, "cms")
          dst_path = prob_test_path(dst_dir, dst_key, "cms")

          ets = :ets.new(:"sm_cms_merge_#{System.unique_integer([:positive])}", [:set, :public])

          try do
            File.mkdir_p!(src_dir)
            File.mkdir_p!(dst_shard_path)
            File.touch!(Path.join(dst_shard_path, "00000.log"))

            assert {:ok, _} = NIF.cms_file_create(src_path, 64, 4)
            assert {:ok, _} = NIF.cms_file_incrby(src_path, [{"element", 9}])

            state =
              StateMachine.init(%{
                shard_index: 0,
                shard_data_path: dst_shard_path,
                active_file_id: 0,
                active_file_path: Path.join(dst_shard_path, "00000.log"),
                ets: ets,
                instance_ctx: ctx,
                instance_name: instance_name
              })

            apply_result =
              StateMachine.apply(
                %{},
                {:cms_merge, dst_key, [src_key], [1], %{width: 64, depth: 4}},
                state
              )

            assert :ok = apply_result_value(apply_result)

            assert {:ok, [9]} = NIF.cms_file_query(dst_path, ["element"])
          after
            :ets.delete(ets)
            FerricStore.Instance.cleanup(instance_name)
            File.rm_rf!(root)
          end
        end

        test "probabilistic create failures in batch do not publish metadata", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          state = %{state | shard_index: 0}
          prob_dir = Path.join(dir, "prob")
          File.write!(prob_dir, "not-a-directory")

          commands = [
            {"batch_bloom_create_fail",
             {:bloom_create, "batch_bloom_create_fail", 9586, 7,
              {:bloom_meta, %{num_bits: 9586, num_hashes: 7, capacity: 1000, error_rate: 0.01}}}},
            {"batch_cms_create_fail", {:cms_create, "batch_cms_create_fail", 100, 5}},
            {"batch_cuckoo_create_fail", {:cuckoo_create, "batch_cuckoo_create_fail", 1024, 4}},
            {"batch_topk_create_fail", {:topk_create, "batch_topk_create_fail", 10, 8, 7}}
          ]

          {_state, {:ok, results}} =
            StateMachine.apply(%{}, {:batch, Enum.map(commands, &elem(&1, 1))}, state)

          assert Enum.all?(results, &match?({:error, _}, &1))

          for {key, _cmd} <- commands do
            assert [] == :ets.lookup(ets, key)
          end
        end

        test "mixed put and delete batch", %{state: state, ets: ets} do
          {state2, :ok} = StateMachine.apply(%{}, {:put, "mix_a", "va", 0}, state)

          commands = [
            {:put, "mix_b", "vb", 0},
            {:delete, "mix_a"},
            {:put, "mix_c", "vc", 0}
          ]

          {new_state, {:ok, results}} =
            StateMachine.apply(%{}, {:batch, commands}, state2)

          assert results == [:ok, :ok, :ok]
          assert new_state.applied_count == 4

          assert [] == :ets.lookup(ets, "mix_a")
          assert [{"mix_b", "vb", 0, _, _, _, _}] = :ets.lookup(ets, "mix_b")
          assert [{"mix_c", "vc", 0, _, _, _, _}] = :ets.lookup(ets, "mix_c")
        end

        test "mixed put and delete batch emits one Bitcask append for all ops", %{
          state: state
        } do
          {state2, :ok} = StateMachine.apply(%{}, {:put, "batched_delete_seed", "old", 0}, state)
          handler_id = {:state_machine_mixed_delete_batch_telemetry, self(), make_ref()}

          :ok =
            :telemetry.attach(
              handler_id,
              [:ferricstore, :bitcask, :append],
              fn _event, measurements, metadata, test_pid ->
                send(test_pid, {:bitcask_append, measurements, metadata})
              end,
              self()
            )

          try do
            commands = [
              {:put, "batched_delete_put_a", "a", 0},
              {:delete, "batched_delete_seed"},
              {:put, "batched_delete_put_b", "b", 0}
            ]

            {_new_state, {:ok, [:ok, :ok, :ok]}} =
              StateMachine.apply(%{}, {:batch, commands}, state2)

            assert_receive {:bitcask_append, measurements, %{status: :ok}}, 500
            assert measurements.batch_size == 3
            assert measurements.delete_count == 1

            assert measurements.batch_bytes ==
                     byte_size("batched_delete_put_a") + 1 + byte_size("batched_delete_seed") +
                       byte_size("batched_delete_put_b") + 1
          after
            :telemetry.detach(handler_id)
          end
        end

        test "empty batch returns empty results", %{state: state} do
          {new_state, {:ok, results}} =
            StateMachine.apply(%{}, {:batch, []}, state)

          assert results == []
          assert new_state.applied_count == 0
        end

        test "large batch (100 commands)", %{state: state} do
          commands = for i <- 1..100, do: {:put, "batch_k#{i}", "batch_v#{i}", 0}

          {new_state, {:ok, results}} =
            StateMachine.apply(%{}, {:batch, commands}, state)

          assert length(results) == 100
          assert Enum.all?(results, &(&1 == :ok))
          assert new_state.applied_count == 100
        end
      end

      describe "pending batch location validation" do
        @tag :append_result_validation
        test "rejects a non-list native append result" do
          assert {:error, {:bitcask_append_result_mismatch, {:invalid_locations, :bad_reply}}} =
                   StateMachine.__validate_pending_locations__(
                     [{:put, "k1", "v1", 0}],
                     :bad_reply
                   )
        end

        test "rejects a result count that does not match the batch" do
          batch = [
            {:put, "k1", "v1", 0},
            {:delete, "k2", nil}
          ]

          assert {:error, {:bitcask_append_result_mismatch, {:length_mismatch, 2, 1}}} =
                   StateMachine.__validate_pending_locations__(batch, [{:put, 0, 2}])
        end

        test "rejects out-of-order operation tags" do
          batch = [
            {:put, "k1", "v1", 0},
            {:delete, "k2", nil}
          ]

          locations = [
            {:delete, 0, 28},
            {:put, 28, 2}
          ]

          assert {:error, {:bitcask_append_result_mismatch, {:op_mismatch, 0, :put, :delete}}} =
                   StateMachine.__validate_pending_locations__(batch, locations)
        end

        test "rejects invalid put and delete offsets from append results" do
          assert {:error,
                  {:bitcask_append_result_mismatch, {:invalid_location, 0, {:put, -1, 2}}}} =
                   StateMachine.__validate_pending_locations__(
                     [{:put, "k1", "v1", 0}],
                     [{:put, -1, 2}]
                   )

          assert {:error,
                  {:bitcask_append_result_mismatch, {:invalid_location, 0, {:delete, 0, -1}}}} =
                   StateMachine.__validate_pending_locations__(
                     [{:delete, "k2", nil}],
                     [{:delete, 0, -1}]
                   )
        end

        test "accepts matching put and delete result tags in order" do
          batch = [
            {:put, "k1", "v1", 0},
            {:delete, "k2", nil}
          ]

          locations = [
            {:put, 0, 2},
            {:delete, 30, 28}
          ]

          assert :ok = StateMachine.__validate_pending_locations__(batch, locations)
        end

        @tag :append_result_validation
        test "malformed successful append results fail apply without publishing", %{
          state: state,
          ets: ets
        } do
          key = "malformed-pending-append"
          previous_hook = Application.get_env(:ferricstore, :pending_append_hook)

          Application.put_env(:ferricstore, :pending_append_hook, fn _path, _batch ->
            {:ok, :bad_reply}
          end)

          try do
            assert {_state,
                    {:error,
                     {:bitcask_append_failed,
                      {:bitcask_append_result_mismatch, {:invalid_locations, :bad_reply}}}}} =
                     StateMachine.apply(%{}, {:put_batch, [{key, "value", 0}]}, state)

            assert [] == :ets.lookup(ets, key)
          after
            restore_env(:pending_append_hook, previous_hook)
          end
        end
      end

      describe "pending batch location application" do
        test "does not attach an old append location to a newer pending value", %{
          state: state,
          ets: ets
        } do
          key = "stale-location-key"

          :ets.insert(
            ets,
            {key, "new", 456, Ferricstore.Store.LFU.initial(), :pending, 0, byte_size("new")}
          )

          StateMachine.__apply_pending_locations_for_test__(
            state,
            7,
            [{:put, key, "old", 123}],
            [{:put, 42, byte_size("old")}]
          )

          assert [{^key, "new", 456, _lfu, :pending, 0, 3}] = :ets.lookup(ets, key)
        end

        test "attaches append location when the pending value still matches", %{
          state: state,
          ets: ets
        } do
          key = "matching-location-key"

          :ets.insert(
            ets,
            {key, "new", 456, Ferricstore.Store.LFU.initial(), :pending, 0, byte_size("new")}
          )

          StateMachine.__apply_pending_locations_for_test__(
            state,
            7,
            [{:put, key, "new", 456}],
            [{:put, 42, byte_size("new")}]
          )

          assert [{^key, "new", 456, _lfu, 7, 42, 3}] = :ets.lookup(ets, key)
        end

        test "attaches append location to a forced-hot staged value above the cache threshold", %{
          state: state,
          ets: ets
        } do
          key = "forced-hot-flow-location-key"
          value = "flow-state-kept-hot"
          expire_at_ms = 456
          state = Map.put(state, :instance_ctx, %{hot_cache_max_value_size: 1})

          :ets.insert(
            ets,
            {key, value, expire_at_ms, Ferricstore.Store.LFU.initial(), :pending, 0,
             byte_size(value)}
          )

          try do
            Process.put(:sm_pending_fast_staged_put_batch, true)
            Process.put(:sm_pending_values, %{key => {value, expire_at_ms}})

            StateMachine.__apply_pending_locations_for_test__(
              state,
              7,
              [{:put, key, value, expire_at_ms}],
              [{:put, 42, byte_size(value)}]
            )
          after
            Process.delete(:sm_pending_fast_staged_put_batch)
            Process.delete(:sm_pending_values)
          end

          assert [{^key, ^value, ^expire_at_ms, _lfu, 7, 42, _value_size}] =
                   :ets.lookup(ets, key)
        end

        test "does not attach a stale forced-hot append when the staged value changed", %{
          state: state,
          ets: ets
        } do
          key = "stale-forced-hot-flow-location-key"
          expire_at_ms = 456
          state = Map.put(state, :instance_ctx, %{hot_cache_max_value_size: 1})

          :ets.insert(
            ets,
            {key, "new", expire_at_ms, Ferricstore.Store.LFU.initial(), :pending, 0,
             byte_size("new")}
          )

          try do
            Process.put(:sm_pending_fast_staged_put_batch, true)
            Process.put(:sm_pending_values, %{key => {"new", expire_at_ms}})

            StateMachine.__apply_pending_locations_for_test__(
              state,
              7,
              [{:put, key, "old", expire_at_ms}],
              [{:put, 42, byte_size("old")}]
            )
          after
            Process.delete(:sm_pending_fast_staged_put_batch)
            Process.delete(:sm_pending_values)
          end

          assert [{^key, "new", ^expire_at_ms, _lfu, :pending, 0, 3}] =
                   :ets.lookup(ets, key)
        end

        test "attaches WARaft tuple file ids to matching hot and cold pending rows", %{
          state: state,
          ets: ets
        } do
          hot_key = "matching-waraft-hot-location-key"
          cold_key = "matching-waraft-cold-location-key"
          file_id = {:waraft_apply_projection, 17}
          cold_lfu = {:flow_state_version, 2, 123_456}

          :ets.insert(
            ets,
            {hot_key, "hot", 456, Ferricstore.Store.LFU.initial(), :pending, 0, byte_size("hot")}
          )

          :ets.insert(
            ets,
            {cold_key, nil, 789, cold_lfu, :pending, 0, byte_size("cold")}
          )

          try do
            Process.put(:sm_pending_fast_staged_put_batch, true)

            StateMachine.__apply_pending_locations_for_test__(
              state,
              file_id,
              [
                {:put, hot_key, "hot", 456},
                {:put_cold, cold_key, "cold", 789, cold_lfu}
              ],
              [
                {:put, 11, byte_size("hot")},
                {:put, 22, byte_size("cold")}
              ]
            )
          after
            Process.delete(:sm_pending_fast_staged_put_batch)
          end

          assert [{^hot_key, "hot", 456, _lfu, ^file_id, 11, 3}] = :ets.lookup(ets, hot_key)
          assert [{^cold_key, nil, 789, ^cold_lfu, ^file_id, 22, 4}] = :ets.lookup(ets, cold_key)
        end

        test "batch deletes stale apply-projection cache for matching staged rows", %{
          state: state,
          ets: ets
        } do
          hot_key = "staged-hot-apply-projection-cache"
          cold_key = "staged-cold-apply-projection-cache"
          cold_lfu = {:flow_state_version, 2, 123_456}
          old_index = 41
          new_file_id = {:waraft_apply_projection, 42}

          :ok =
            Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
              state.data_dir,
              state.shard_index,
              old_index,
              [
                {hot_key, "old-hot", 0},
                {cold_key, "old-cold", 0}
              ]
            )

          assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(
                   state.data_dir,
                   state.shard_index
                 ) == 2

          :ets.insert(
            ets,
            {hot_key, "hot", 456, Ferricstore.Store.LFU.initial(), :pending, 0, byte_size("hot")}
          )

          :ets.insert(
            ets,
            {cold_key, nil, 789, cold_lfu, :pending, 0, byte_size("cold")}
          )

          try do
            Process.put(:sm_pending_fast_staged_put_batch, true)

            Process.put(:sm_pending_originals, %{
              hot_key =>
                {:entry,
                 {hot_key, nil, 0, Ferricstore.Store.LFU.initial(),
                  {:waraft_apply_projection, old_index}, 0, 7}},
              cold_key =>
                {:entry,
                 {cold_key, nil, 0, cold_lfu, {:waraft_apply_projection, old_index}, 0, 8}}
            })

            StateMachine.__apply_pending_locations_for_test__(
              state,
              new_file_id,
              [
                {:put, hot_key, "hot", 456},
                {:put_cold, cold_key, "cold", 789, cold_lfu}
              ],
              [
                {:put, 11, byte_size("hot")},
                {:put, 22, byte_size("cold")}
              ]
            )
          after
            Process.delete(:sm_pending_fast_staged_put_batch)
            Process.delete(:sm_pending_originals)
          end

          assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(
                   state.data_dir,
                   state.shard_index
                 ) == 0
        end

        test "duplicate-key staged batch deletes stale apply-projection cache for final row", %{
          state: state,
          ets: ets
        } do
          key = "duplicate-staged-apply-projection-cache"
          old_index = 141
          new_file_id = {:waraft_apply_projection, 142}

          :ok =
            Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
              state.data_dir,
              state.shard_index,
              old_index,
              [{key, "old-value", 0}]
            )

          :ets.insert(
            ets,
            {key, "final-value", 0, Ferricstore.Store.LFU.initial(), :pending, 0,
             byte_size("final-value")}
          )

          try do
            Process.put(:sm_pending_fast_staged_put_batch, true)

            Process.put(:sm_pending_originals, %{
              key =>
                {:entry,
                 {key, nil, 0, Ferricstore.Store.LFU.initial(),
                  {:waraft_apply_projection, old_index}, 0, byte_size("old-value")}}
            })

            StateMachine.__apply_pending_locations_for_test__(
              state,
              new_file_id,
              [
                {:put, key, "intermediate-value", 0},
                {:put, key, "final-value", 0}
              ],
              [
                {:put, 11, byte_size("intermediate-value")},
                {:put, 22, byte_size("final-value")}
              ]
            )
          after
            Process.delete(:sm_pending_fast_staged_put_batch)
            Process.delete(:sm_pending_originals)
          end

          assert [{^key, "final-value", 0, _lfu, ^new_file_id, 22, _value_size}] =
                   :ets.lookup(ets, key)

          assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(
                   state.data_dir,
                   state.shard_index
                 ) == 0
        end
      end

      describe "apply/3 probabilistic native failures" do
        @tag :prob_long_key_path
        test "replicated creates use bounded sidecar filenames for long keys", %{
          state: state,
          dir: dir
        } do
          long_key = String.duplicate("long-probabilistic-key", 1_000)

          commands = [
            {long_key <> ":bf", "bloom",
             {:bloom_create, long_key <> ":bf", 9586, 7,
              {:bloom_meta, %{num_bits: 9586, num_hashes: 7, capacity: 1000, error_rate: 0.01}}}},
            {long_key <> ":cms", "cms", {:cms_create, long_key <> ":cms", 100, 5}},
            {long_key <> ":cf", "cuckoo", {:cuckoo_create, long_key <> ":cf", 1024, 4}},
            {long_key <> ":topk", "topk", {:topk_create, long_key <> ":topk", 10, 8, 7}}
          ]

          Enum.reduce(commands, state, fn {key, extension, command}, acc_state ->
            {next_state, :ok} = StateMachine.apply(%{}, command, acc_state)
            path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, extension)

            assert File.regular?(path)
            assert byte_size(Path.basename(path)) <= 255

            next_state
          end)
        end

        test "create reports prob directory parent fsync failure", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "bloom_create_prob_dir_fsync_fail"

          Process.put(:ferricstore_prob_fsync_dir_hook, fn ^dir ->
            {:error, :eio}
          end)

          try do
            {_state2, result} =
              StateMachine.apply(
                %{},
                {:bloom_create, key, 9586, 7,
                 {:bloom_meta, %{num_bits: 9586, num_hashes: 7, capacity: 1000, error_rate: 0.01}}},
                state
              )

            assert {:error, {:fsync_dir_failed, :create_prob_dir, :eio}} = result
            assert [] == :ets.lookup(ets, key)
          after
            Process.delete(:ferricstore_prob_fsync_dir_hook)
          end
        end

        test "create failures do not publish metadata", %{state: state, ets: ets, dir: dir} do
          state = %{state | shard_index: 0}
          prob_dir = Path.join(dir, "prob")
          File.write!(prob_dir, "not-a-directory")

          commands = [
            {"bloom_create_fail",
             {:bloom_create, "bloom_create_fail", 9586, 7,
              {:bloom_meta, %{num_bits: 9586, num_hashes: 7, capacity: 1000, error_rate: 0.01}}}},
            {"cms_create_fail", {:cms_create, "cms_create_fail", 100, 5}},
            {"cuckoo_create_fail", {:cuckoo_create, "cuckoo_create_fail", 1024, 4}},
            {"topk_create_fail", {:topk_create, "topk_create_fail", 10, 8, 7}}
          ]

          Enum.reduce(commands, state, fn {key, command}, acc_state ->
            {next_state, result} = StateMachine.apply(%{}, command, acc_state)

            assert {:error, _reason} = result
            assert [] == :ets.lookup(ets, key)

            next_state
          end)
        end

        test "auto-create failures do not publish metadata", %{state: state, ets: ets, dir: dir} do
          state = %{state | shard_index: 0}
          prob_dir = Path.join(dir, "prob")
          File.write!(prob_dir, "not-a-directory")

          commands = [
            {"bloom_add_fail",
             {:bloom_add, "bloom_add_fail", "item",
              %{num_bits: 9586, num_hashes: 7, capacity: 1000, error_rate: 0.01}}},
            {"bloom_madd_fail",
             {:bloom_madd, "bloom_madd_fail", ["item"],
              %{num_bits: 9586, num_hashes: 7, capacity: 1000, error_rate: 0.01}}},
            {"cms_merge_create_fail",
             {:cms_merge, "cms_merge_create_fail", [], [], %{width: 100, depth: 5}}},
            {"cuckoo_add_fail",
             {:cuckoo_add, "cuckoo_add_fail", "item", %{capacity: 1024, bucket_size: 4}}},
            {"cuckoo_addnx_fail",
             {:cuckoo_addnx, "cuckoo_addnx_fail", "item", %{capacity: 1024, bucket_size: 4}}}
          ]

          Enum.reduce(commands, state, fn {key, command}, acc_state ->
            {next_state, result} = StateMachine.apply(%{}, command, acc_state)

            assert {:error, _reason} = result
            assert [] == :ets.lookup(ets, key)

            next_state
          end)
        end
      end

      # ---------------------------------------------------------------------------
      # state_enter/2
      # ---------------------------------------------------------------------------

      describe "state_enter/2" do
        test "returns empty effects for all roles", %{state: state} do
          assert StateMachine.state_enter(:leader, state) == []
          assert StateMachine.state_enter(:follower, state) == []
          assert StateMachine.state_enter(:candidate, state) == []
          assert StateMachine.state_enter(:await_condition, state) == []
          assert StateMachine.state_enter(:delete_and_terminate, state) == []
          assert StateMachine.state_enter(:receive_snapshot, state) == []
        end
      end

      # ---------------------------------------------------------------------------
      # tick/2
      # ---------------------------------------------------------------------------

      describe "tick/2" do
        test "returns empty effects", %{state: state} do
          assert StateMachine.tick(System.os_time(:millisecond), state) == []
        end
      end

      # ---------------------------------------------------------------------------
      # init_aux/1
      # ---------------------------------------------------------------------------

      describe "init_aux/1" do
        test "returns initial aux state with empty hot_keys" do
          aux = StateMachine.init_aux(:test_name)
          assert aux == %{hot_keys: %{}}
        end
      end

      # ---------------------------------------------------------------------------
      # handle_aux/5
      # ---------------------------------------------------------------------------
    end
  end
end
