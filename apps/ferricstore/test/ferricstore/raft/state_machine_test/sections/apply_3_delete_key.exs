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

        @tag :prob_type_catalog
        test "delete does not treat user string bytes as probabilistic metadata", %{
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
          assert File.read!(expected_sidecar) == "sidecar"
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

        @tag :prob_sidecar_delete_durability
        test "prob sidecar cleanup fsync failure is a typed storage error", %{
          state: state,
          dir: dir,
          shard_index: shard_index
        } do
          key = "prob_delete_fsync_telemetry"
          prob_dir = Path.join(dir, "prob")
          prob_path = Ferricstore.ProbFile.path(prob_dir, key, "cms")
          {state, :ok} = StateMachine.apply(%{}, {:cms_create, key, 64, 4}, state)

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

          assert {_state, {:error, reason}} =
                   StateMachine.apply(%{}, {:delete, key}, state)

          assert {:prob_sidecar_delete_failed, ^prob_path,
                  {:fsync_dir_failed, :delete_prob_files, :eio}} = reason

          assert Ferricstore.Raft.ApplyFailure.storage_reason?(reason)

          assert_receive {:prob_sidecar_delete_failed,
                          [:ferricstore, :prob, :sidecar_delete_failed], %{count: 1},
                          %{
                            shard_index: ^shard_index,
                            path: ^prob_path,
                            reason: {:fsync_dir_failed, :delete_prob_files, :eio}
                          }}
        end

        @tag :prob_sidecar_delete_durability
        test "batch delete fsyncs the sidecar directory once", %{state: state, dir: dir} do
          first = "batched-delete-cms"
          second = "batched-delete-topk"
          {state, :ok} = StateMachine.apply(%{}, {:cms_create, first, 64, 4}, state)
          {state, :ok} = StateMachine.apply(%{}, {:topk_create, second, 10, 32, 4}, state)

          prob_dir = Path.join(dir, "prob")
          hook_count_key = {__MODULE__, :prob_batch_delete_fsync_count}
          Process.put(hook_count_key, 0)

          Process.put(:ferricstore_prob_fsync_dir_hook, fn
            ^prob_dir ->
              Process.put(hook_count_key, Process.get(hook_count_key, 0) + 1)
              :ok

            _other_dir ->
              :ok
          end)

          try do
            assert {_state, {:ok, [:ok, :ok]}} =
                     StateMachine.apply(
                       %{},
                       {:batch, [{:delete, first}, {:delete, second}]},
                       state
                     )

            assert Process.get(hook_count_key) == 1
          after
            Process.delete(:ferricstore_prob_fsync_dir_hook)
            Process.delete(hook_count_key)
          end
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

        @tag :cms_merge_locality
        test "CMS merge rejects source keys owned by another Raft shard" do
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

            assert {:error, "CROSSSLOT CMS.MERGE keys must hash to the same shard"} =
                     apply_result_value(apply_result)

            assert {:error, :enoent} = NIF.cms_file_query(dst_path, ["element"])
          after
            :ets.delete(ets)
            FerricStore.Instance.cleanup(instance_name)
            File.rm_rf!(root)
          end
        end

        @tag :cms_merge_locality
        test "CMS merge reads co-located sources from the applying shard" do
          root =
            Path.join(
              System.tmp_dir!(),
              "sm_cms_local_merge_#{System.unique_integer([:positive])}"
            )

          instance_name = :"sm_cms_local_merge_#{System.unique_integer([:positive])}"
          ctx = FerricStore.Instance.build(instance_name, data_dir: root, shard_count: 4)

          src_key = key_for_shard(ctx, "cms_local_src", 0)
          dst_key = key_for_shard(ctx, "cms_local_dst", 0)
          shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
          prob_dir = Path.join(shard_path, "prob")
          src_path = prob_test_path(prob_dir, src_key, "cms")
          dst_path = prob_test_path(prob_dir, dst_key, "cms")
          ets = :ets.new(:"sm_cms_local_merge_#{System.unique_integer([:positive])}", [:set])

          try do
            File.mkdir_p!(prob_dir)
            File.touch!(Path.join(shard_path, "00000.log"))

            state =
              StateMachine.init(%{
                shard_index: 0,
                shard_data_path: shard_path,
                active_file_id: 0,
                active_file_path: Path.join(shard_path, "00000.log"),
                ets: ets,
                instance_ctx: ctx,
                instance_name: instance_name
              })

            {state, :ok} = StateMachine.apply(%{}, {:cms_create, src_key, 64, 4}, state)

            {state, {:ok, [9]}} =
              StateMachine.apply(%{}, {:cms_incrby, src_key, [{"element", 9}]}, state)

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

          apply_result =
            StateMachine.apply(%{}, {:batch, Enum.map(commands, &elem(&1, 1))}, state)

          assert {:error, reason} = apply_result_value(apply_result)
          assert Ferricstore.Raft.ApplyFailure.storage_reason?(reason)

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
          assert new_state.applied_count == 1
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
        @tag :prob_storage_failure
        test "probabilistic directory creation returns a typed storage error", %{
          state: state,
          dir: dir
        } do
          blocked_parent = Path.join(dir, "prob-parent-is-a-file")
          File.write!(blocked_parent, "not-a-directory")
          blocked_state = %{state | shard_data_path: Path.join(blocked_parent, "shard")}

          assert {_state, {:error, {:prob_dir_create_failed, _reason}}} =
                   StateMachine.apply(
                     %{},
                     {:cms_create, "blocked-prob-dir", 64, 4},
                     blocked_state
                   )
        end

        @tag :prob_storage_failure
        test "probabilistic sidecar creation returns a typed storage error", %{
          state: state,
          dir: dir
        } do
          key = "blocked-prob-sidecar-create"
          prob_dir = Path.join(dir, "prob")
          path = Ferricstore.ProbFile.path(prob_dir, key, "cms")
          File.mkdir_p!(path <> ".pending-create")

          assert {_state, {:error, reason}} =
                   StateMachine.apply(%{}, {:cms_create, key, 64, 4}, state)

          assert {:prob_sidecar_create_failed, _native_reason} = reason
          assert Ferricstore.Raft.ApplyFailure.storage_reason?(reason)
        end

        @tag :prob_storage_failure
        test "native create failure removes a visible uncommitted sidecar", %{
          state: state,
          dir: dir
        } do
          create_path = Path.join(dir, "visible-uncommitted.pending-create")
          final_path = Path.join(dir, "visible-uncommitted.cms")
          File.write!(create_path, "uncommitted")

          assert {:error, {:prob_sidecar_create_failed, :directory_fsync_failed}} =
                   StateMachine.__prob_create_and_fsync_for_test__(
                     state,
                     create_path,
                     final_path,
                     {:error, :directory_fsync_failed}
                   )

          refute File.exists?(create_path)
          refute File.exists?(final_path)
        end

        @tag :prob_storage_failure
        test "missing probabilistic sidecars stop every replicated mutation", %{
          state: state,
          dir: dir
        } do
          cases = [
            {:bloom_add, "missing-bloom-add", "bloom",
             {:bloom_create, "missing-bloom-add", 128, 3,
              {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}},
             {:bloom_add, "missing-bloom-add", "item", nil}},
            {:bloom_madd, "missing-bloom-madd", "bloom",
             {:bloom_create, "missing-bloom-madd", 128, 3,
              {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}},
             {:bloom_madd, "missing-bloom-madd", ["item"], nil}},
            {:cms_incrby, "missing-cms-incrby", "cms", {:cms_create, "missing-cms-incrby", 64, 4},
             {:cms_incrby, "missing-cms-incrby", [{"item", 1}]}},
            {:cuckoo_add, "missing-cuckoo-add", "cuckoo",
             {:cuckoo_create, "missing-cuckoo-add", 64, 4},
             {:cuckoo_add, "missing-cuckoo-add", "item", nil}},
            {:cuckoo_addnx, "missing-cuckoo-addnx", "cuckoo",
             {:cuckoo_create, "missing-cuckoo-addnx", 64, 4},
             {:cuckoo_addnx, "missing-cuckoo-addnx", "item", nil}},
            {:cuckoo_del, "missing-cuckoo-del", "cuckoo",
             {:cuckoo_create, "missing-cuckoo-del", 64, 4},
             {:cuckoo_del, "missing-cuckoo-del", "item"}},
            {:topk_add, "missing-topk-add", "topk", {:topk_create, "missing-topk-add", 10, 32, 4},
             {:topk_add, "missing-topk-add", ["item"]}},
            {:topk_incrby, "missing-topk-incrby", "topk",
             {:topk_create, "missing-topk-incrby", 10, 32, 4},
             {:topk_incrby, "missing-topk-incrby", [{"item", 1}]}}
          ]

          Enum.reduce(cases, state, fn {operation, key, extension, create, mutation}, acc ->
            {created_state, :ok} = StateMachine.apply(%{}, create, acc)
            path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, extension)
            File.rm!(path)

            {next_state, {:error, reason}} = StateMachine.apply(%{}, mutation, created_state)

            assert {:prob_sidecar_apply_failed, ^operation, :enoent} = reason
            assert Ferricstore.Raft.ApplyFailure.storage_reason?(reason)
            next_state
          end)
        end

        @tag :prob_storage_failure
        test "missing probabilistic sidecars stop batched mutations", %{
          state: state,
          dir: dir
        } do
          key = "missing-batched-cms-incrby"
          {state, :ok} = StateMachine.apply(%{}, {:cms_create, key, 64, 4}, state)
          later_key = "batch-command-after-missing-sidecar"

          bloom_meta =
            {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}

          {state, :ok} =
            StateMachine.apply(
              %{},
              {:bloom_create, later_key, 128, 3, bloom_meta},
              state
            )

          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cms")
          later_path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), later_key, "bloom")
          File.rm!(path)

          assert {_state, {:error, {:prob_sidecar_apply_failed, :cms_incrby, :enoent}}} =
                   StateMachine.apply(
                     %{},
                     {:batch,
                      [
                        {:cms_incrby, key, [{"item", 1}]},
                        {:bloom_add, later_key, "must-not-run", nil}
                      ]},
                     state
                   )

          assert {:ok, 0} = NIF.bloom_file_exists(later_path, "must-not-run")
        end

        @tag :prob_storage_failure
        test "deterministic probabilistic command errors remain ordinary results", %{
          state: state
        } do
          key = "topk-semantic-error"
          {state, :ok} = StateMachine.apply(%{}, {:topk_create, key, 10, 32, 4}, state)

          assert {_state, {:error, "TopK increment must be positive"}} =
                   StateMachine.apply(
                     %{},
                     {:topk_incrby, key, [{"item", 0}]},
                     state
                   )

          refute Ferricstore.Raft.ApplyFailure.storage_reason?("TopK increment must be positive")
        end

        @tag :prob_sidecar_replay
        test "create removes a staged sidecar left before metadata commit", %{
          state: state,
          dir: dir
        } do
          key = "stale-staged-prob-create"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cms")
          staged_path = path <> ".pending-create"

          File.mkdir_p!(Path.dirname(staged_path))
          assert {:ok, :ok} = NIF.cms_file_create(staged_path, 32, 2)
          assert File.regular?(staged_path)

          assert {_state, :ok} = StateMachine.apply(%{}, {:cms_create, key, 64, 4}, state)

          refute File.exists?(staged_path)
          assert {:ok, {64, 4, 0}} = NIF.cms_file_info(path)
        end

        @tag :prob_sidecar_replay
        test "create replay repairs a rename that was not directory-durable", %{
          state: state,
          dir: dir
        } do
          key = "replayed-prob-create"
          command = {:cms_create, key, 64, 4}
          ra_index = 8_100_001
          prob_dir = Path.join(dir, "prob")
          path = Ferricstore.ProbFile.path(prob_dir, key, "cms")
          hook_count_key = {__MODULE__, :prob_publish_fsync_count}
          Process.put(hook_count_key, 0)

          Process.put(:ferricstore_prob_fsync_dir_hook, fn
            ^prob_dir ->
              count = Process.get(hook_count_key, 0) + 1
              Process.put(hook_count_key, count)
              if count == 1, do: {:error, :eio}, else: :ok

            _other_dir ->
              :ok
          end)

          try do
            assert {failed_state,
                    {:applied_at, ^ra_index,
                     {:error,
                      {:prob_sidecar_publish_failed, ^path, _staged_path,
                       {:fsync_dir_failed, :publish_prob_file, :eio}}}}, _effects} =
                     StateMachine.apply(
                       %{index: ra_index, system_time: 1_000},
                       command,
                       state
                     )

            assert File.regular?(path)
            Process.delete(:ferricstore_prob_fsync_dir_hook)

            assert {replayed_state, {:applied_at, ^ra_index, :ok}, _effects} =
                     StateMachine.apply(
                       %{index: ra_index, system_time: 1_000},
                       command,
                       failed_state
                     )

            assert {:ok, {64, 4, 0}} = NIF.cms_file_info(path)

            next_index = ra_index + 1

            assert {_state, {:applied_at, ^next_index, {:error, "ERR item already exists"}},
                    _effects} =
                     StateMachine.apply(
                       %{index: next_index, system_time: 1_001},
                       command,
                       replayed_state
                     )
          after
            Process.delete(:ferricstore_prob_fsync_dir_hook)
            Process.delete(hook_count_key)
          end
        end

        @tag :prob_sidecar_replay
        test "auto-create mutation replay rebuilds instead of double-applying", %{
          state: state,
          dir: dir
        } do
          key = "replayed-cuckoo-auto-create"
          auto_params = %{capacity: 64, bucket_size: 4}
          command = {:cuckoo_add, key, "item", auto_params}
          ra_index = 8_100_002
          prob_dir = Path.join(dir, "prob")
          path = Ferricstore.ProbFile.path(prob_dir, key, "cuckoo")
          hook_count_key = {__MODULE__, :prob_auto_publish_fsync_count}
          Process.put(hook_count_key, 0)

          Process.put(:ferricstore_prob_fsync_dir_hook, fn
            ^prob_dir ->
              count = Process.get(hook_count_key, 0) + 1
              Process.put(hook_count_key, count)
              if count == 1, do: {:error, :eio}, else: :ok

            _other_dir ->
              :ok
          end)

          try do
            assert {failed_state,
                    {:applied_at, ^ra_index,
                     {:error,
                      {:prob_sidecar_publish_failed, ^path, _staged_path,
                       {:fsync_dir_failed, :publish_prob_file, :eio}}}}, _effects} =
                     StateMachine.apply(
                       %{index: ra_index, system_time: 1_000},
                       command,
                       state
                     )

            Process.delete(:ferricstore_prob_fsync_dir_hook)

            assert {_replayed_state, {:applied_at, ^ra_index, {:ok, 1}}, _effects} =
                     StateMachine.apply(
                       %{index: ra_index, system_time: 1_000},
                       command,
                       failed_state
                     )

            assert {:ok, {_buckets, 4, _fingerprint_size, 1, 0, _slots, _max_kicks}} =
                     NIF.cuckoo_file_info(path)

            assert {:ok, 1} = NIF.cuckoo_file_exists(path, "item")
          after
            Process.delete(:ferricstore_prob_fsync_dir_hook)
            Process.delete(hook_count_key)
          end
        end

        @tag :prob_sidecar_replay
        test "created sidecars replay same-batch mutations exactly once", %{
          state: state,
          dir: dir
        } do
          key = "replayed-created-cms-mutation"

          command =
            {:batch,
             [
               {:cms_create, key, 64, 4},
               {:cms_incrby, key, [{"item", 7}]}
             ]}

          ra_index = 8_100_003
          prob_dir = Path.join(dir, "prob")
          path = Ferricstore.ProbFile.path(prob_dir, key, "cms")
          hook_count_key = {__MODULE__, :prob_batch_publish_fsync_count}
          Process.put(hook_count_key, 0)

          Process.put(:ferricstore_prob_fsync_dir_hook, fn
            ^prob_dir ->
              count = Process.get(hook_count_key, 0) + 1
              Process.put(hook_count_key, count)
              if count == 1, do: {:error, :eio}, else: :ok

            _other_dir ->
              :ok
          end)

          try do
            assert {failed_state,
                    {:applied_at, ^ra_index,
                     {:error,
                      {:prob_sidecar_publish_failed, ^path, _staged_path,
                       {:fsync_dir_failed, :publish_prob_file, :eio}}}}, _effects} =
                     StateMachine.apply(
                       %{index: ra_index, system_time: 1_000},
                       command,
                       state
                     )

            assert {:ok, [7]} = NIF.cms_file_query(path, ["item"])
            Process.delete(:ferricstore_prob_fsync_dir_hook)

            assert {_replayed_state, {:applied_at, ^ra_index, {:ok, [:ok, {:ok, [7]}]}}, _effects} =
                     StateMachine.apply(
                       %{index: ra_index, system_time: 1_000},
                       command,
                       failed_state
                     )

            assert {:ok, [7]} = NIF.cms_file_query(path, ["item"])
          after
            Process.delete(:ferricstore_prob_fsync_dir_hook)
            Process.delete(hook_count_key)
          end
        end

        @tag :prob_apply_ordering
        test "same-batch duplicate probabilistic creates remain deterministic errors", %{
          state: state,
          dir: dir
        } do
          key = "duplicate-created-in-batch"
          create = {:cms_create, key, 64, 4}

          assert {_state, {:ok, [:ok, {:error, "ERR item already exists"}]}} =
                   StateMachine.apply(%{}, {:batch, [create, create]}, state)

          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cms")
          assert {:ok, {64, 4, 0}} = NIF.cms_file_info(path)
        end

        @tag :prob_apply_ordering
        test "mutations cannot use a sidecar after its logical key was deleted", %{
          state: state,
          dir: dir
        } do
          cases = [
            {"deleted-before-cms-mutation", "cms",
             {:cms_create, "deleted-before-cms-mutation", 64, 4},
             {:cms_incrby, "deleted-before-cms-mutation", [{"item", 1}]}},
            {"deleted-before-cuckoo-mutation", "cuckoo",
             {:cuckoo_create, "deleted-before-cuckoo-mutation", 64, 4},
             {:cuckoo_del, "deleted-before-cuckoo-mutation", "item"}},
            {"deleted-before-topk-mutation", "topk",
             {:topk_create, "deleted-before-topk-mutation", 10, 32, 4},
             {:topk_add, "deleted-before-topk-mutation", ["item"]}}
          ]

          Enum.reduce(cases, state, fn {key, extension, create, mutation}, acc_state ->
            {created_state, :ok} = StateMachine.apply(%{}, create, acc_state)

            {next_state, result} =
              StateMachine.apply(%{}, {:batch, [{:delete, key}, mutation]}, created_state)

            assert {:ok, [_deleted, {:error, :enoent}]} = result

            path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, extension)
            refute File.exists?(path)
            next_state
          end)
        end

        @tag :prob_apply_ordering
        test "CMS merge cannot read a source deleted earlier in the batch", %{
          state: state,
          dir: dir
        } do
          source = "deleted-cms-merge-source"
          destination = "cms-merge-destination"
          {state, :ok} = StateMachine.apply(%{}, {:cms_create, source, 64, 4}, state)

          {state, {:ok, [5]}} =
            StateMachine.apply(%{}, {:cms_incrby, source, [{"item", 5}]}, state)

          {state, :ok} = StateMachine.apply(%{}, {:cms_create, destination, 64, 4}, state)

          {_state, result} =
            StateMachine.apply(
              %{},
              {:batch,
               [
                 {:delete, source},
                 {:cms_merge, destination, [source], [1], %{width: 64, depth: 4}}
               ]},
              state
            )

          assert {:ok, [_deleted, {:error, :enoent}]} = result

          destination_path =
            Ferricstore.ProbFile.path(Path.join(dir, "prob"), destination, "cms")

          assert {:ok, [0]} = NIF.cms_file_query(destination_path, ["item"])
        end

        @tag :prob_hot_path
        test "existing probabilistic mutations do not load cold metadata", %{
          state: state,
          ets: ets
        } do
          key = "cold-prob-metadata-mutation"

          metadata =
            {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}

          {state, :ok} =
            StateMachine.apply(%{}, {:bloom_create, key, 128, 3, metadata}, state)

          assert true = :ets.update_element(ets, key, {2, nil})

          Process.put(:ferricstore_state_machine_cold_read_success_hook, fn _ctx, ^key ->
            flunk("probabilistic mutation loaded cold metadata")
          end)

          try do
            ra_index = 8_100_004

            assert {_state, {:applied_at, ^ra_index, {:ok, 1}}, _effects} =
                     StateMachine.apply(
                       %{index: ra_index, system_time: 1_000},
                       {:bloom_add, key, "item",
                        %{
                          num_bits: 128,
                          num_hashes: 3,
                          capacity: 32,
                          error_rate: 0.01
                        }},
                       state
                     )
          after
            Process.delete(:ferricstore_state_machine_cold_read_success_hook)
          end
        end

        @tag :prob_type_catalog
        test "probabilistic creates publish exact replicated type markers", %{
          state: state,
          ets: ets
        } do
          commands = [
            {"catalog_bloom", "bloom",
             {:bloom_create, "catalog_bloom", 128, 3,
              {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}}},
            {"catalog_cms", "cms", {:cms_create, "catalog_cms", 64, 4}},
            {"catalog_cuckoo", "cuckoo", {:cuckoo_create, "catalog_cuckoo", 64, 4}},
            {"catalog_topk", "topk", {:topk_create, "catalog_topk", 10, 32, 4}}
          ]

          Enum.reduce(commands, state, fn {key, expected_type, command}, acc_state ->
            {next_state, :ok} = StateMachine.apply(%{}, command, acc_state)
            type_key = CompoundKey.type_key(key)
            expected_type_atom = String.to_existing_atom(expected_type)

            assert [{^type_key, type_marker, 0, _lfu, _file_id, _offset, _size}] =
                     :ets.lookup(ets, type_key)

            assert {:ok, {^expected_type_atom, create_token}} =
                     CompoundKey.decode_prob_type(type_marker)

            assert is_integer(create_token)

            next_state
          end)
        end

        @tag :prob_sidecar_lifecycle
        test "string puts remove replaced probabilistic sidecars after publish", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          creates = [
            {"replace_bloom", "bloom",
             {:bloom_create, "replace_bloom", 128, 3,
              {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}}},
            {"replace_cms", "cms", {:cms_create, "replace_cms", 64, 4}},
            {"replace_cuckoo", "cuckoo", {:cuckoo_create, "replace_cuckoo", 64, 4}},
            {"replace_topk", "topk", {:topk_create, "replace_topk", 10, 32, 4}}
          ]

          Enum.reduce(creates, state, fn {key, ext, create}, acc_state ->
            {acc_state, :ok} = StateMachine.apply(%{}, create, acc_state)
            path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, ext)
            assert File.exists?(path)

            {next_state, :ok} =
              StateMachine.apply(%{}, {:put, key, "string-value", 0}, acc_state)

            refute File.exists?(path)
            assert [] == :ets.lookup(ets, CompoundKey.type_key(key))

            assert [{^key, "string-value", 0, _lfu, _file_id, _offset, 12}] =
                     :ets.lookup(ets, key)

            next_state
          end)
        end

        @tag :prob_sidecar_lifecycle
        test "failed string put keeps probabilistic metadata and sidecar", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "failed_prob_replacement"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cms")
          {state, :ok} = StateMachine.apply(%{}, {:cms_create, key, 64, 4}, state)
          original_entry = :ets.lookup(ets, key)
          original_type_entry = :ets.lookup(ets, CompoundKey.type_key(key))
          assert File.exists?(path)

          file_id = 9_200_000 + :erlang.unique_integer([:positive])
          bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
          File.mkdir_p!(bad_active_path)
          bad_state = %{state | active_file_id: file_id, active_file_path: bad_active_path}

          {_state, {:error, {:bitcask_append_failed, _reason}}} =
            StateMachine.apply(%{}, {:put, key, "string-value", 0}, bad_state)

          assert original_entry == :ets.lookup(ets, key)
          assert original_type_entry == :ets.lookup(ets, CompoundKey.type_key(key))
          assert File.exists?(path)
          assert {:ok, {_width, _depth, _count}} = NIF.cms_file_info(path)
        end

        @tag :prob_sidecar_lifecycle
        test "same-batch probabilistic sidecar cleanup follows the final logical value", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          auto_params = %{
            num_bits: 9586,
            num_hashes: 7,
            capacity: 1000,
            error_rate: 0.01
          }

          recreate_key = "batch_prob_recreate"

          recreate_path =
            Ferricstore.ProbFile.path(Path.join(dir, "prob"), recreate_key, "bloom")

          create =
            {:bloom_create, recreate_key, 128, 3,
             {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}}

          {state, :ok} = StateMachine.apply(%{}, create, state)

          {state, {:ok, 1}} =
            StateMachine.apply(%{}, {:bloom_add, recreate_key, "old-item", nil}, state)

          assert File.regular?(recreate_path <> ".mutation")

          {state, {:ok, [:ok, :ok, {:ok, _added}]}} =
            StateMachine.apply(
              %{},
              {:batch,
               [
                 {:put, recreate_key, "temporary-string", 0},
                 {:delete, recreate_key},
                 {:bloom_add, recreate_key, "fresh-item", auto_params}
               ]},
              state
            )

          assert File.exists?(recreate_path)
          assert {:ok, 1} = NIF.bloom_file_exists(recreate_path, "fresh-item")
          assert {:ok, 0} = NIF.bloom_file_exists(recreate_path, "old-item")
          assert File.regular?(recreate_path <> ".mutation")

          assert [{^recreate_key, encoded_meta, 0, _lfu, _file_id, _offset, _size}] =
                   :ets.lookup(ets, recreate_key)

          assert {:ok, {:bloom_meta, _metadata}} = Ferricstore.TermCodec.decode(encoded_meta)

          overwrite_key = "batch_prob_overwrite"

          overwrite_path =
            Ferricstore.ProbFile.path(Path.join(dir, "prob"), overwrite_key, "bloom")

          {_state, {:ok, [{:ok, _added}, :ok]}} =
            StateMachine.apply(
              %{},
              {:batch,
               [
                 {:bloom_add, overwrite_key, "discarded-item", auto_params},
                 {:put, overwrite_key, "final-string", 0}
               ]},
              state
            )

          refute File.exists?(overwrite_path)

          assert [{^overwrite_key, "final-string", 0, _lfu, _file_id, _offset, 12}] =
                   :ets.lookup(ets, overwrite_key)
        end

        @tag :prob_sidecar_lifecycle
        test "failed delete and auto-create batch restores the original sidecar", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "failed_prob_recreate"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "bloom")

          meta =
            {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}

          {state, :ok} = StateMachine.apply(%{}, {:bloom_create, key, 128, 3, meta}, state)
          {state, {:ok, 1}} = StateMachine.apply(%{}, {:bloom_add, key, "keep-me", nil}, state)
          original_entry = :ets.lookup(ets, key)
          original_receipt = File.read!(path <> ".mutation")

          file_id = 9_300_000 + :erlang.unique_integer([:positive])
          bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
          File.mkdir_p!(bad_active_path)
          bad_state = %{state | active_file_id: file_id, active_file_path: bad_active_path}

          auto_params = %{
            num_bits: 9586,
            num_hashes: 7,
            capacity: 1000,
            error_rate: 0.01
          }

          {_state, {:error, {:bitcask_append_failed, _reason}}} =
            StateMachine.apply(
              %{},
              {:batch, [{:delete, key}, {:bloom_add, key, "new-item", auto_params}]},
              bad_state
            )

          assert original_entry == :ets.lookup(ets, key)
          assert File.exists?(path)
          assert {:ok, 1} = NIF.bloom_file_exists(path, "keep-me")
          assert {:ok, 0} = NIF.bloom_file_exists(path, "new-item")
          assert File.read!(path <> ".mutation") == original_receipt
          refute File.exists?(path <> ".pending-create.mutation")
        end

        @tag :prob_sidecar_lifecycle
        test "rollback removes a staged sidecar deleted later in the batch", %{
          state: state,
          dir: dir
        } do
          key = "rolled-back-created-then-overwritten"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cms")
          staged_path = path <> ".pending-create"
          file_id = 9_350_000 + :erlang.unique_integer([:positive])
          bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
          File.mkdir_p!(bad_active_path)
          bad_state = %{state | active_file_id: file_id, active_file_path: bad_active_path}

          assert {_state, {:error, {:bitcask_append_failed, _reason}}} =
                   StateMachine.apply(
                     %{},
                     {:batch,
                      [
                        {:cms_create, key, 64, 4},
                        {:put, key, "final-string", 0}
                      ]},
                     bad_state
                   )

          refute File.exists?(path)
          refute File.exists?(staged_path)
        end

        @tag :prob_sidecar_lifecycle
        test "batch create fsyncs the sidecar directory once", %{state: state, dir: dir} do
          prob_dir = Path.join(dir, "prob")
          hook_count_key = {__MODULE__, :prob_batch_create_fsync_count}
          Process.put(hook_count_key, 0)

          Process.put(:ferricstore_prob_fsync_dir_hook, fn
            ^prob_dir ->
              Process.put(hook_count_key, Process.get(hook_count_key, 0) + 1)
              :ok

            _other_dir ->
              :ok
          end)

          try do
            assert {_state, {:ok, [:ok, :ok]}} =
                     StateMachine.apply(
                       %{},
                       {:batch,
                        [
                          {:cms_create, "batched-fsync-cms", 64, 4},
                          {:topk_create, "batched-fsync-topk", 10, 32, 4}
                        ]},
                       state
                     )

            assert Process.get(hook_count_key) == 1
          after
            Process.delete(:ferricstore_prob_fsync_dir_hook)
            Process.delete(hook_count_key)
          end
        end

        @tag :prob_sidecar_lifecycle
        test "put batches remove replaced probabilistic sidecars", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "fast_batch_prob_replacement"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cms")
          {state, :ok} = StateMachine.apply(%{}, {:cms_create, key, 64, 4}, state)
          assert File.exists?(path)

          {_state, {:ok, [:ok]}} =
            StateMachine.apply(%{}, {:put_batch, [{key, "string-value", 0}]}, state)

          refute File.exists?(path)
          assert [] == :ets.lookup(ets, CompoundKey.type_key(key))

          assert [{^key, "string-value", 0, _lfu, _file_id, _offset, 12}] =
                   :ets.lookup(ets, key)
        end

        @tag :prob_sidecar_lifecycle
        test "plain put batches do not read overwritten cold values", %{state: state, ets: ets} do
          key = "cold_batch_overwrite"
          {state, :ok} = StateMachine.apply(%{}, {:put, key, "old-value", 0}, state)
          assert true = :ets.update_element(ets, key, {2, nil})

          Process.put(:ferricstore_state_machine_cold_read_success_hook, fn _ctx, ^key ->
            flunk("plain put batch performed a cold read")
          end)

          try do
            {_state, {:ok, [:ok]}} =
              StateMachine.apply(%{}, {:put_batch, [{key, "new-value", 0}]}, state)
          after
            Process.delete(:ferricstore_state_machine_cold_read_success_hook)
          end

          assert [{^key, "new-value", 0, _lfu, _file_id, _offset, 9}] = :ets.lookup(ets, key)
        end

        @tag :prob_sidecar_lifecycle
        test "standalone transaction replacement publishes probabilistic cleanup", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "transaction_prob_replacement"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cms")
          {state, :ok} = StateMachine.apply(%{}, {:cms_create, key, 64, 4}, state)
          assert File.exists?(path)

          execute = fn store ->
            Ferricstore.Commands.Strings.replace_string_key(key, "string-value", 0, store)
          end

          assert {:ok, _flushed_state} = StateMachine.apply_standalone_cross_shard(execute, state)

          refute File.exists?(path)
          assert [] == :ets.lookup(ets, CompoundKey.type_key(key))

          assert [{^key, "string-value", 0, _lfu, _file_id, _offset, 12}] =
                   :ets.lookup(ets, key)
        end

        @tag :prob_sidecar_lifecycle
        test "standalone RENAME moves probabilistic metadata, type, and sidecar", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          source = "transaction_prob_rename_source"
          destination = "transaction_prob_rename_destination"
          prob_dir = Path.join(dir, "prob")
          source_path = Ferricstore.ProbFile.path(prob_dir, source, "cms")
          destination_path = Ferricstore.ProbFile.path(prob_dir, destination, "cms")

          {state, :ok} = StateMachine.apply(%{}, {:cms_create, source, 64, 4}, state)

          {state, {:ok, [7]}} =
            StateMachine.apply(%{}, {:cms_incrby, source, [{"item", 7}]}, state)

          execute = fn store ->
            Ferricstore.Commands.Generic.handle_ast({:rename, source, destination}, store)
          end

          assert {:ok, _flushed_state} = StateMachine.apply_standalone_cross_shard(execute, state)

          refute File.exists?(source_path)
          assert {:ok, [7]} = NIF.cms_file_query(destination_path, ["item"])
          assert [] == :ets.lookup(ets, source)
          assert [] == :ets.lookup(ets, CompoundKey.type_key(source))
          assert [{_, _, _, _, _, _, _}] = :ets.lookup(ets, destination)
          assert [{_, _, _, _, _, _, _}] = :ets.lookup(ets, CompoundKey.type_key(destination))
        end

        @tag :prob_sidecar_lifecycle
        test "replicated lifecycle command moves a probabilistic key", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          source = "replicated_prob_rename_source"
          destination = "replicated_prob_rename_destination"
          prob_dir = Path.join(dir, "prob")
          source_path = Ferricstore.ProbFile.path(prob_dir, source, "cms")
          destination_path = Ferricstore.ProbFile.path(prob_dir, destination, "cms")

          {state, :ok} = StateMachine.apply(%{}, {:cms_create, source, 64, 4}, state)

          {state, {:ok, [7]}} =
            StateMachine.apply(%{}, {:cms_incrby, source, [{"item", 7}]}, state)

          {renamed_state, :ok} =
            StateMachine.apply(
              %{},
              {:key_lifecycle, {:rename, source, destination}},
              state
            )

          refute File.exists?(source_path)
          assert {:ok, [7]} = NIF.cms_file_query(destination_path, ["item"])
          assert [] == :ets.lookup(ets, source)
          assert [] == :ets.lookup(ets, CompoundKey.type_key(source))
          assert [{_, _, _, _, _, _, _}] = :ets.lookup(ets, destination)
          assert [{_, _, _, _, _, _, _}] = :ets.lookup(ets, CompoundKey.type_key(destination))
          assert renamed_state.applied_count == state.applied_count + 1
        end

        @tag :prob_sidecar_lifecycle
        test "lifecycle replay propagates metadata storage read failures" do
          destination = "replay-read-failure-destination"
          lifecycle_id = {71, <<0::128>>}
          type_key = CompoundKey.type_key(destination)
          marker = CompoundKey.encode_prob_type(:cms, 71)

          store = %{
            compound_get: fn
              ^destination, ^type_key -> marker
              _redis_key, _compound_key -> nil
            end,
            get_meta: fn ^destination ->
              Ferricstore.Store.ReadResult.failure(:missing_file)
            end
          }

          assert {:error, "ERR storage read failed"} =
                   StateMachine.__prob_lifecycle_replay_type_for_test__(
                     store,
                     {:rename, "source", destination},
                     lifecycle_id
                   )
        end

        @tag :prob_mutation_replay
        test "CMS mutation replay at the same Raft index is idempotent", %{
          state: state,
          dir: dir
        } do
          key = "cms_same_raft_index_replay"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cms")
          {state, :ok} = StateMachine.apply(%{}, {:cms_create, key, 64, 4}, state)

          Process.put(:sm_current_ra_index, 77)

          try do
            {state, {:ok, [3]}} =
              StateMachine.apply(%{}, {:cms_incrby, key, [{"item", 3}]}, state)

            {_state, {:ok, [3]}} =
              StateMachine.apply(%{}, {:cms_incrby, key, [{"item", 3}]}, state)
          after
            Process.delete(:sm_current_ra_index)
          end

          assert {:ok, [3]} = NIF.cms_file_query(path, ["item"])
        end

        @tag :prob_mutation_replay
        test "CMS merge replay at the same Raft index is idempotent when destination is a source",
             %{
               state: state,
               dir: dir
             } do
          key = "cms_same_raft_index_merge_replay"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cms")
          {state, :ok} = StateMachine.apply(%{}, {:cms_create, key, 64, 4}, state)
          {state, {:ok, [5]}} = StateMachine.apply(%{}, {:cms_incrby, key, [{"item", 5}]}, state)

          command = {:cms_merge, key, [key], [2], %{width: 64, depth: 4}}
          Process.put(:sm_current_ra_index, 78)

          try do
            {state, :ok} = StateMachine.apply(%{}, command, state)
            {_state, :ok} = StateMachine.apply(%{}, command, state)
          after
            Process.delete(:sm_current_ra_index)
          end

          assert {:ok, [10]} = NIF.cms_file_query(path, ["item"])
        end

        @tag :prob_mutation_replay
        test "Bloom add replay at the same Raft index preserves the original reply", %{
          state: state,
          dir: dir
        } do
          key = "bloom_same_raft_index_replay"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "bloom")

          metadata =
            {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}

          {state, :ok} =
            StateMachine.apply(%{}, {:bloom_create, key, 128, 3, metadata}, state)

          Process.put(:sm_current_ra_index, 79)

          try do
            {state, {:ok, 1}} = StateMachine.apply(%{}, {:bloom_add, key, "item", nil}, state)
            {_state, {:ok, 1}} = StateMachine.apply(%{}, {:bloom_add, key, "item", nil}, state)
          after
            Process.delete(:sm_current_ra_index)
          end

          assert {:ok, 1} = NIF.bloom_file_card(path)
        end

        @tag :prob_mutation_replay
        test "Bloom madd replay preserves ordered duplicate-element replies", %{
          state: state,
          dir: dir
        } do
          key = "bloom_madd_same_raft_index_replay"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "bloom")

          metadata =
            {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}

          {state, :ok} =
            StateMachine.apply(%{}, {:bloom_create, key, 128, 3, metadata}, state)

          command = {:bloom_madd, key, ["first", "second", "first"], nil}
          Process.put(:sm_current_ra_index, 80)

          try do
            {state, {:ok, [1, 1, 0]}} = StateMachine.apply(%{}, command, state)
            {_state, {:ok, [1, 1, 0]}} = StateMachine.apply(%{}, command, state)
          after
            Process.delete(:sm_current_ra_index)
          end

          assert {:ok, 2} = NIF.bloom_file_card(path)
        end

        @tag :prob_mutation_replay
        test "Bloom auto-create replay reuses the published sidecar", %{
          state: state,
          dir: dir
        } do
          key = "bloom_auto_create_same_raft_index_replay"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "bloom")

          auto_params = %{
            num_bits: 128,
            num_hashes: 3,
            capacity: 32,
            error_rate: 0.01
          }

          command = {:bloom_add, key, "item", auto_params}
          Process.put(:sm_current_ra_index, 81)

          try do
            {state, {:ok, 1}} = StateMachine.apply(%{}, command, state)
            published_inode = File.stat!(path).inode
            pending_receipt = path <> ".pending-create.mutation"
            File.rename!(path <> ".mutation", pending_receipt)

            {_state, {:ok, 1}} = StateMachine.apply(%{}, command, state)
            assert File.stat!(path).inode == published_inode
            assert File.regular?(path <> ".mutation")
            refute File.exists?(pending_receipt)
          after
            Process.delete(:sm_current_ra_index)
          end

          assert {:ok, 1} = NIF.bloom_file_card(path)
        end

        @tag :prob_mutation_replay
        test "Cuckoo add replay does not insert a duplicate fingerprint", %{
          state: state,
          dir: dir
        } do
          key = "cuckoo_add_same_raft_index_replay"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cuckoo")
          {state, :ok} = StateMachine.apply(%{}, {:cuckoo_create, key, 128, 4}, state)

          Process.put(:sm_current_ra_index, 82)

          try do
            {state, {:ok, 1}} = StateMachine.apply(%{}, {:cuckoo_add, key, "item", nil}, state)
            {_state, {:ok, 1}} = StateMachine.apply(%{}, {:cuckoo_add, key, "item", nil}, state)
          after
            Process.delete(:sm_current_ra_index)
          end

          assert {:ok, 1} = NIF.cuckoo_file_count(path, "item")
        end

        @tag :prob_mutation_replay
        test "Cuckoo addnx replay preserves the successful insert reply", %{
          state: state,
          dir: dir
        } do
          key = "cuckoo_addnx_same_raft_index_replay"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cuckoo")
          {state, :ok} = StateMachine.apply(%{}, {:cuckoo_create, key, 128, 4}, state)

          Process.put(:sm_current_ra_index, 83)

          try do
            {state, {:ok, 1}} =
              StateMachine.apply(%{}, {:cuckoo_addnx, key, "item", nil}, state)

            {_state, {:ok, 1}} =
              StateMachine.apply(%{}, {:cuckoo_addnx, key, "item", nil}, state)
          after
            Process.delete(:sm_current_ra_index)
          end

          assert {:ok, 1} = NIF.cuckoo_file_count(path, "item")
        end

        @tag :prob_mutation_replay
        test "Cuckoo delete replay removes only the originally deleted occurrence", %{
          state: state,
          dir: dir
        } do
          key = "cuckoo_delete_same_raft_index_replay"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cuckoo")
          {state, :ok} = StateMachine.apply(%{}, {:cuckoo_create, key, 128, 4}, state)
          {state, {:ok, 1}} = StateMachine.apply(%{}, {:cuckoo_add, key, "item", nil}, state)
          {state, {:ok, 1}} = StateMachine.apply(%{}, {:cuckoo_add, key, "item", nil}, state)

          Process.put(:sm_current_ra_index, 84)

          try do
            {state, {:ok, 1}} = StateMachine.apply(%{}, {:cuckoo_del, key, "item"}, state)
            {_state, {:ok, 1}} = StateMachine.apply(%{}, {:cuckoo_del, key, "item"}, state)
          after
            Process.delete(:sm_current_ra_index)
          end

          assert {:ok, 1} = NIF.cuckoo_file_count(path, "item")

          assert {:ok, {_buckets, _bucket_size, _fp_size, 1, 1, _slots, _kicks}} =
                   NIF.cuckoo_file_info(path)
        end

        @tag :prob_mutation_replay
        test "TopK add replay at the same Raft index increments once", %{
          state: state,
          dir: dir
        } do
          key = "topk_add_same_raft_index_replay"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "topk")
          {state, :ok} = StateMachine.apply(%{}, {:topk_create, key, 4, 128, 4}, state)

          Process.put(:sm_current_ra_index, 85)

          try do
            {state, [nil]} = StateMachine.apply(%{}, {:topk_add, key, ["item"]}, state)
            {_state, [nil]} = StateMachine.apply(%{}, {:topk_add, key, ["item"]}, state)
          after
            Process.delete(:sm_current_ra_index)
          end

          assert [1] = NIF.topk_file_count_v2(path, ["item"])
        end

        @tag :prob_mutation_replay
        test "TopK incrby replay preserves the original eviction reply", %{
          state: state,
          dir: dir
        } do
          key = "topk_incrby_same_raft_index_replay"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "topk")
          {state, :ok} = StateMachine.apply(%{}, {:topk_create, key, 1, 128, 4}, state)
          {state, [nil]} = StateMachine.apply(%{}, {:topk_incrby, key, [{"first", 1}]}, state)

          Process.put(:sm_current_ra_index, 86)

          try do
            {state, ["first"]} =
              StateMachine.apply(%{}, {:topk_incrby, key, [{"second", 2}]}, state)

            {_state, ["first"]} =
              StateMachine.apply(%{}, {:topk_incrby, key, [{"second", 2}]}, state)
          after
            Process.delete(:sm_current_ra_index)
          end

          assert [2] = NIF.topk_file_count_v2(path, ["second"])
        end

        @tag :prob_sidecar_lifecycle
        test "standalone COPY creates an independent probabilistic sidecar", %{
          state: state,
          dir: dir
        } do
          source = "transaction_prob_copy_source"
          destination = "transaction_prob_copy_destination"
          prob_dir = Path.join(dir, "prob")
          source_path = Ferricstore.ProbFile.path(prob_dir, source, "cms")
          destination_path = Ferricstore.ProbFile.path(prob_dir, destination, "cms")

          {state, :ok} = StateMachine.apply(%{}, {:cms_create, source, 64, 4}, state)

          {state, {:ok, [7]}} =
            StateMachine.apply(%{}, {:cms_incrby, source, [{"item", 7}]}, state)

          execute = fn store ->
            Ferricstore.Commands.Generic.handle_ast({:copy, source, destination, false}, store)
          end

          assert {1, copied_state} = StateMachine.apply_standalone_cross_shard(execute, state)
          assert {:ok, [7]} = NIF.cms_file_query(source_path, ["item"])
          assert {:ok, [7]} = NIF.cms_file_query(destination_path, ["item"])
          refute File.stat!(source_path).inode == File.stat!(destination_path).inode

          {_, {:ok, [10]}} =
            StateMachine.apply(%{}, {:cms_incrby, destination, [{"item", 3}]}, copied_state)

          assert {:ok, [7]} = NIF.cms_file_query(source_path, ["item"])
          assert {:ok, [10]} = NIF.cms_file_query(destination_path, ["item"])
        end

        @tag :prob_sidecar_lifecycle
        test "standalone COPY REPLACE removes the old destination sidecar", %{
          state: state,
          dir: dir
        } do
          source = "transaction_prob_copy_replace_source"
          destination = "transaction_prob_copy_replace_destination"
          prob_dir = Path.join(dir, "prob")
          source_path = Ferricstore.ProbFile.path(prob_dir, source, "cms")
          destination_path = Ferricstore.ProbFile.path(prob_dir, destination, "cms")
          old_destination_path = Ferricstore.ProbFile.path(prob_dir, destination, "bloom")

          bloom_meta =
            {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}

          {state, :ok} = StateMachine.apply(%{}, {:cms_create, source, 64, 4}, state)

          {state, {:ok, [9]}} =
            StateMachine.apply(%{}, {:cms_incrby, source, [{"item", 9}]}, state)

          {state, :ok} =
            StateMachine.apply(%{}, {:bloom_create, destination, 128, 3, bloom_meta}, state)

          {state, {:ok, 1}} =
            StateMachine.apply(%{}, {:bloom_add, destination, "old", nil}, state)

          execute = fn store ->
            Ferricstore.Commands.Generic.handle_ast({:copy, source, destination, true}, store)
          end

          assert {1, _copied_state} = StateMachine.apply_standalone_cross_shard(execute, state)
          assert File.exists?(source_path)
          refute File.exists?(old_destination_path)
          assert {:ok, [9]} = NIF.cms_file_query(destination_path, ["item"])
        end

        @tag :prob_sidecar_lifecycle
        test "same-type COPY REPLACE discards the destination mutation receipt", %{
          state: state,
          dir: dir
        } do
          source = "transaction_prob_copy_receipt_source"
          destination = "transaction_prob_copy_receipt_destination"
          prob_dir = Path.join(dir, "prob")
          source_path = Ferricstore.ProbFile.path(prob_dir, source, "cms")
          destination_path = Ferricstore.ProbFile.path(prob_dir, destination, "cms")

          {state, :ok} = StateMachine.apply(%{}, {:cms_create, source, 64, 4}, state)

          {state, {:ok, [9]}} =
            StateMachine.apply(%{}, {:cms_incrby, source, [{"item", 9}]}, state)

          {state, :ok} = StateMachine.apply(%{}, {:cms_create, destination, 64, 4}, state)

          {state, {:ok, [4]}} =
            StateMachine.apply(%{}, {:cms_incrby, destination, [{"item", 4}]}, state)

          assert File.regular?(source_path <> ".mutation")
          assert File.regular?(destination_path <> ".mutation")

          execute = fn store ->
            Ferricstore.Commands.Generic.handle_ast({:copy, source, destination, true}, store)
          end

          assert {1, copied_state} = StateMachine.apply_standalone_cross_shard(execute, state)
          refute File.exists?(destination_path <> ".mutation")

          {_state, mutation_result} =
            StateMachine.apply(%{}, {:cms_incrby, destination, [{"item", 1}]}, copied_state)

          assert mutation_result == {:ok, [10]}

          assert {:ok, [9]} = NIF.cms_file_query(source_path, ["item"])
          assert {:ok, [10]} = NIF.cms_file_query(destination_path, ["item"])
        end

        @tag :prob_sidecar_lifecycle
        test "failed standalone COPY rolls back its staged sidecar", %{state: state, dir: dir} do
          source = "failed_transaction_prob_copy_source"
          destination = "failed_transaction_prob_copy_destination"
          prob_dir = Path.join(dir, "prob")
          source_path = Ferricstore.ProbFile.path(prob_dir, source, "cms")
          destination_path = Ferricstore.ProbFile.path(prob_dir, destination, "cms")
          {state, :ok} = StateMachine.apply(%{}, {:cms_create, source, 64, 4}, state)

          file_id = 9_500_000 + :erlang.unique_integer([:positive])
          bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
          File.mkdir_p!(bad_active_path)
          bad_state = %{state | active_file_id: file_id, active_file_path: bad_active_path}

          execute = fn store ->
            Ferricstore.Commands.Generic.handle_ast({:copy, source, destination, false}, store)
          end

          assert {:error, {:bitcask_append_failed, _reason}, _partial_state} =
                   StateMachine.apply_standalone_cross_shard(execute, bad_state)

          assert File.exists?(source_path)
          refute File.exists?(destination_path)
          refute File.exists?(destination_path <> ".pending-create")
        end

        @tag :prob_sidecar_lifecycle
        test "standalone RENAME replay repairs publication without duplicating the sidecar", %{
          state: state,
          dir: dir
        } do
          source = "replayed_transaction_prob_rename_source"
          destination = "replayed_transaction_prob_rename_destination"
          prob_dir = Path.join(dir, "prob")
          source_path = Ferricstore.ProbFile.path(prob_dir, source, "cms")
          destination_path = Ferricstore.ProbFile.path(prob_dir, destination, "cms")
          hook_count_key = {__MODULE__, :prob_rename_replay_fsync_count}
          Process.put(hook_count_key, 0)

          {state, :ok} = StateMachine.apply(%{}, {:cms_create, source, 64, 4}, state)

          {state, {:ok, [11]}} =
            StateMachine.apply(%{}, {:cms_incrby, source, [{"item", 11}]}, state)

          execute = fn store ->
            Ferricstore.Commands.Generic.handle_ast({:rename, source, destination}, store)
          end

          Process.put(:ferricstore_prob_fsync_dir_hook, fn
            ^prob_dir ->
              count = Process.get(hook_count_key, 0) + 1
              Process.put(hook_count_key, count)
              if count == 1, do: {:error, :eio}, else: :ok

            _other_dir ->
              :ok
          end)

          try do
            assert {{:error,
                     {:prob_sidecar_publish_failed, ^destination_path, _staged_path,
                      {:fsync_dir_failed, :publish_prob_file, :eio}}}, failed_state} =
                     StateMachine.apply_standalone_cross_shard(execute, state)

            assert File.exists?(source_path)
            assert File.exists?(destination_path)

            assert {:ok, replayed_state} =
                     StateMachine.apply_standalone_cross_shard(execute, failed_state)

            refute File.exists?(source_path)
            assert {:ok, [11]} = NIF.cms_file_query(destination_path, ["item"])
            assert replayed_state.shard_index == state.shard_index
          after
            Process.delete(:ferricstore_prob_fsync_dir_hook)
            Process.delete(hook_count_key)
          end
        end

        @tag :prob_sidecar_lifecycle
        test "failed standalone transaction replacement preserves probabilistic state", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "failed_transaction_prob_replacement"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cms")
          {state, :ok} = StateMachine.apply(%{}, {:cms_create, key, 64, 4}, state)
          original_entry = :ets.lookup(ets, key)
          original_type_entry = :ets.lookup(ets, CompoundKey.type_key(key))

          file_id = 9_400_000 + :erlang.unique_integer([:positive])
          bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
          File.mkdir_p!(bad_active_path)
          bad_state = %{state | active_file_id: file_id, active_file_path: bad_active_path}

          execute = fn store ->
            Ferricstore.Commands.Strings.replace_string_key(key, "string-value", 0, store)
          end

          assert {:error, {:bitcask_append_failed, _reason}, _partial_state} =
                   StateMachine.apply_standalone_cross_shard(execute, bad_state)

          assert File.exists?(path)
          assert original_entry == :ets.lookup(ets, key)
          assert original_type_entry == :ets.lookup(ets, CompoundKey.type_key(key))
        end

        @tag :prob_apply_ordering
        test "Bloom create cannot replace an existing replicated filter", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "bloom_duplicate_create"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "bloom")

          first_meta =
            {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}

          {state, :ok} =
            StateMachine.apply(%{}, {:bloom_create, key, 128, 3, first_meta}, state)

          assert {:ok, _added} = NIF.bloom_file_add(path, "keep-me")
          original_entry = :ets.lookup(ets, key)
          assert {:ok, original_info} = NIF.bloom_file_info(path)

          second_meta =
            {:bloom_meta, %{num_bits: 256, num_hashes: 4, capacity: 64, error_rate: 0.01}}

          {_state, {:error, "ERR item exists"}} =
            StateMachine.apply(%{}, {:bloom_create, key, 256, 4, second_meta}, state)

          assert original_entry == :ets.lookup(ets, key)
          assert {:ok, ^original_info} = NIF.bloom_file_info(path)
          assert {:ok, 1} = NIF.bloom_file_exists(path, "keep-me")
        end

        @tag :prob_apply_ordering
        test "CMS create cannot replace an existing replicated sketch", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "cms_duplicate_create"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cms")

          {state, :ok} = StateMachine.apply(%{}, {:cms_create, key, 64, 4}, state)
          assert {:ok, [7]} = NIF.cms_file_incrby(path, [{"keep-me", 7}])
          original_entry = :ets.lookup(ets, key)
          assert {:ok, original_info} = NIF.cms_file_info(path)

          {_state, {:error, "ERR item already exists"}} =
            StateMachine.apply(%{}, {:cms_create, key, 128, 4}, state)

          assert original_entry == :ets.lookup(ets, key)
          assert {:ok, ^original_info} = NIF.cms_file_info(path)
          assert {:ok, [7]} = NIF.cms_file_query(path, ["keep-me"])
        end

        @tag :prob_apply_ordering
        test "Cuckoo create cannot replace an existing replicated filter", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "cuckoo_duplicate_create"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cuckoo")

          {state, :ok} = StateMachine.apply(%{}, {:cuckoo_create, key, 64, 4}, state)
          assert {:ok, _added} = NIF.cuckoo_file_add(path, "keep-me")
          original_entry = :ets.lookup(ets, key)
          assert {:ok, original_info} = NIF.cuckoo_file_info(path)

          {_state, {:error, "ERR item exists"}} =
            StateMachine.apply(%{}, {:cuckoo_create, key, 128, 4}, state)

          assert original_entry == :ets.lookup(ets, key)
          assert {:ok, ^original_info} = NIF.cuckoo_file_info(path)
          assert {:ok, 1} = NIF.cuckoo_file_exists(path, "keep-me")
        end

        @tag :prob_apply_ordering
        test "TopK create cannot replace an existing replicated sketch", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "topk_duplicate_create"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "topk")

          {state, :ok} = StateMachine.apply(%{}, {:topk_create, key, 10, 32, 4}, state)
          assert [_evicted] = NIF.topk_file_add_v2(path, ["keep-me"])
          assert [1] = NIF.topk_file_count_v2(path, ["keep-me"])
          original_entry = :ets.lookup(ets, key)
          original_info = NIF.topk_file_info_v2(path)

          {_state, {:error, "ERR item already exists"}} =
            StateMachine.apply(%{}, {:topk_create, key, 20, 32, 4}, state)

          assert original_entry == :ets.lookup(ets, key)
          assert ^original_info = NIF.topk_file_info_v2(path)
          assert [1] = NIF.topk_file_count_v2(path, ["keep-me"])
        end

        @tag :prob_apply_ordering
        test "Bloom auto-create cannot overwrite a string ordered before apply", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "bloom_stale_auto_create"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "bloom")
          {state, :ok} = StateMachine.apply(%{}, {:put, key, "string-value", 0}, state)
          original_entry = :ets.lookup(ets, key)

          auto_params = %{
            num_bits: 9586,
            num_hashes: 7,
            capacity: 1000,
            error_rate: 0.01
          }

          {_state, {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}} =
            StateMachine.apply(%{}, {:bloom_add, key, "item", auto_params}, state)

          assert original_entry == :ets.lookup(ets, key)
          refute File.exists?(path)
        end

        @tag :prob_apply_ordering
        test "Cuckoo auto-create cannot overwrite a string ordered before apply", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          key = "cuckoo_stale_auto_create"
          path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cuckoo")
          {state, :ok} = StateMachine.apply(%{}, {:put, key, "string-value", 0}, state)
          original_entry = :ets.lookup(ets, key)
          auto_params = %{capacity: 1024, bucket_size: 4}

          {_state, {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}} =
            StateMachine.apply(%{}, {:cuckoo_addnx, key, "item", auto_params}, state)

          assert original_entry == :ets.lookup(ets, key)
          refute File.exists?(path)
        end

        @tag :prob_apply_ordering
        test "probabilistic create commands reject a string at apply time", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          commands = [
            {"bloom_create_over_string", "bloom",
             {:bloom_create, "bloom_create_over_string", 128, 3,
              {:bloom_meta, %{num_bits: 128, num_hashes: 3, capacity: 32, error_rate: 0.01}}}},
            {"cms_create_over_string", "cms", {:cms_create, "cms_create_over_string", 64, 4}},
            {"cuckoo_create_over_string", "cuckoo",
             {:cuckoo_create, "cuckoo_create_over_string", 64, 4}},
            {"topk_create_over_string", "topk",
             {:topk_create, "topk_create_over_string", 10, 32, 4}}
          ]

          Enum.reduce(commands, state, fn {key, ext, command}, acc_state ->
            {acc_state, :ok} =
              StateMachine.apply(%{}, {:put, key, "string-value", 0}, acc_state)

            original_entry = :ets.lookup(ets, key)

            {next_state,
             {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}} =
              StateMachine.apply(%{}, command, acc_state)

            assert original_entry == :ets.lookup(ets, key)
            refute File.exists?(Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, ext))
            next_state
          end)
        end

        @tag :prob_command_validation
        test "Bloom create rejects malformed dimensions before side effects", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          invalid_dimensions = [
            {nil, 1},
            {1, nil},
            {0, 1},
            {1, 0},
            {8_589_934_593, 1},
            {1, 1_025}
          ]

          _state =
            Enum.reduce(invalid_dimensions, state, fn {num_bits, num_hashes}, acc_state ->
              key = "bloom_create_invalid:#{System.unique_integer([:positive])}"
              path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "bloom")

              assert {next_state, {:error, :invalid_bloom_dimensions}} =
                       StateMachine.apply(
                         %{},
                         {:bloom_create, key, num_bits, num_hashes, {:bloom_meta, %{}}},
                         acc_state
                       )

              refute File.exists?(path)
              assert [] == :ets.lookup(ets, key)
              next_state
            end)

          refute File.exists?(Path.join(dir, "prob"))
        end

        @tag :prob_command_validation
        test "Bloom auto-create rejects malformed parameter maps before side effects", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          invalid_params = [
            %{},
            %{num_bits: nil, num_hashes: 1},
            %{num_bits: 1, num_hashes: 0},
            %{num_bits: 8_589_934_593, num_hashes: 1},
            %{num_bits: 1, num_hashes: 1_025}
          ]

          command_builders = [
            fn key, params -> {:bloom_add, key, "item", params} end,
            fn key, params -> {:bloom_madd, key, ["item"], params} end
          ]

          _state =
            Enum.reduce(command_builders, state, fn build_command, command_state ->
              Enum.reduce(invalid_params, command_state, fn params, acc_state ->
                key = "bloom_auto_invalid:#{System.unique_integer([:positive])}"
                path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "bloom")

                assert {next_state, {:error, :invalid_bloom_dimensions}} =
                         StateMachine.apply(%{}, build_command.(key, params), acc_state)

                refute File.exists?(path)
                assert [] == :ets.lookup(ets, key)
                next_state
              end)
            end)

          refute File.exists?(Path.join(dir, "prob"))
        end

        @tag :prob_command_validation
        test "Cuckoo create rejects malformed parameters before side effects", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          invalid_parameters = [
            {nil, 4},
            {0, 4},
            {1_073_741_825, 4},
            {1, nil},
            {1, 0},
            {1, 1},
            {1, 5}
          ]

          _state =
            Enum.reduce(invalid_parameters, state, fn {capacity, bucket_size}, acc_state ->
              key = "cuckoo_create_invalid:#{System.unique_integer([:positive])}"
              path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cuckoo")

              assert {next_state, {:error, :invalid_cuckoo_parameters}} =
                       StateMachine.apply(
                         %{},
                         {:cuckoo_create, key, capacity, bucket_size},
                         acc_state
                       )

              refute File.exists?(path)
              assert [] == :ets.lookup(ets, key)
              next_state
            end)

          refute File.exists?(Path.join(dir, "prob"))
        end

        @tag :prob_command_validation
        test "Cuckoo auto-create rejects malformed parameter maps before side effects", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          invalid_params = [
            %{},
            %{capacity: nil, bucket_size: 4},
            %{capacity: 0, bucket_size: 4},
            %{capacity: 1_073_741_825, bucket_size: 4},
            %{capacity: 1, bucket_size: 0},
            %{capacity: 1, bucket_size: 1}
          ]

          command_builders = [
            fn key, params -> {:cuckoo_add, key, "item", params} end,
            fn key, params -> {:cuckoo_addnx, key, "item", params} end
          ]

          _state =
            Enum.reduce(command_builders, state, fn build_command, command_state ->
              Enum.reduce(invalid_params, command_state, fn params, acc_state ->
                key = "cuckoo_auto_invalid:#{System.unique_integer([:positive])}"
                path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cuckoo")

                assert {next_state, {:error, :invalid_cuckoo_parameters}} =
                         StateMachine.apply(%{}, build_command.(key, params), acc_state)

                refute File.exists?(path)
                assert [] == :ets.lookup(ets, key)
                next_state
              end)
            end)

          refute File.exists?(Path.join(dir, "prob"))
        end

        @tag :prob_command_validation
        test "TopK create rejects malformed parameters before side effects", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          invalid_parameters = [
            {nil, 1, 1},
            {1, nil, 1},
            {1, 1, nil},
            {0, 1, 1},
            {1, 0, 1},
            {1, 1, 0},
            {100_001, 1, 1},
            {1, 1_048_577, 1},
            {1, 1, 1_048_577}
          ]

          _state =
            Enum.reduce(invalid_parameters, state, fn {k, width, depth}, acc_state ->
              key = "topk_create_invalid:#{System.unique_integer([:positive])}"
              path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "topk")

              assert {next_state, {:error, :invalid_topk_parameters}} =
                       StateMachine.apply(
                         %{},
                         {:topk_create, key, k, width, depth},
                         acc_state
                       )

              refute File.exists?(path)
              assert [] == :ets.lookup(ets, key)
              next_state
            end)

          refute File.exists?(Path.join(dir, "prob"))
        end

        @tag :prob_command_validation
        test "CMS merge rejects malformed destination dimensions before side effects", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          invalid_params = [
            nil,
            %{},
            %{width: 0, depth: 1},
            %{width: 1, depth: 0},
            %{width: 16_777_217, depth: 1},
            %{width: 1, depth: 1_025}
          ]

          _state =
            Enum.reduce(invalid_params, state, fn params, acc_state ->
              key = "cms_merge_invalid:#{System.unique_integer([:positive])}"
              path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, "cms")

              assert {next_state, {:error, :invalid_cms_dimensions}} =
                       StateMachine.apply(
                         %{},
                         {:cms_merge, key, [], [], params},
                         acc_state
                       )

              refute File.exists?(path)
              assert [] == :ets.lookup(ets, key)
              next_state
            end)

          refute File.exists?(Path.join(dir, "prob"))
        end

        @tag :prob_command_validation
        test "CMS merge apply rejects excessive source and counter work before side effects", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          source_limited =
            {:cms_merge, "source-limited-dst", List.duplicate("source", 129),
             List.duplicate(1, 129), %{width: 1, depth: 1}}

          work_limited =
            {:cms_merge, "work-limited-dst", List.duplicate("source", 128),
             List.duplicate(1, 128), %{width: 131_073, depth: 1}}

          {state, {:error, :cms_merge_source_limit_exceeded}} =
            StateMachine.apply(%{}, source_limited, state)

          {state, {:ok, [{:error, :cms_merge_work_limit_exceeded}]}} =
            StateMachine.apply(%{}, {:batch, [work_limited]}, state)

          assert [] == :ets.lookup(ets, "source-limited-dst")
          assert [] == :ets.lookup(ets, "work-limited-dst")
          refute File.exists?(Path.join(dir, "prob"))
        end

        @tag :prob_command_validation
        test "probabilistic parameter validation is preserved in generic batches", %{
          state: state,
          ets: ets,
          dir: dir
        } do
          commands = [
            {"batch_invalid_bloom_create",
             {:bloom_create, "batch_invalid_bloom_create", nil, 1, {:bloom_meta, %{}}},
             :invalid_bloom_dimensions, "bloom"},
            {"batch_invalid_bloom_add", {:bloom_add, "batch_invalid_bloom_add", "item", %{}},
             :invalid_bloom_dimensions, "bloom"},
            {"batch_invalid_bloom_madd", {:bloom_madd, "batch_invalid_bloom_madd", ["item"], %{}},
             :invalid_bloom_dimensions, "bloom"},
            {"batch_invalid_cms_create", {:cms_create, "batch_invalid_cms_create", nil, 1},
             :invalid_cms_dimensions, "cms"},
            {"batch_invalid_cms_merge", {:cms_merge, "batch_invalid_cms_merge", [], [], %{}},
             :invalid_cms_dimensions, "cms"},
            {"batch_invalid_cuckoo_create",
             {:cuckoo_create, "batch_invalid_cuckoo_create", nil, 4}, :invalid_cuckoo_parameters,
             "cuckoo"},
            {"batch_invalid_cuckoo_add", {:cuckoo_add, "batch_invalid_cuckoo_add", "item", %{}},
             :invalid_cuckoo_parameters, "cuckoo"},
            {"batch_invalid_cuckoo_addnx",
             {:cuckoo_addnx, "batch_invalid_cuckoo_addnx", "item", %{}},
             :invalid_cuckoo_parameters, "cuckoo"},
            {"batch_invalid_topk_create", {:topk_create, "batch_invalid_topk_create", nil, 1, 1},
             :invalid_topk_parameters, "topk"}
          ]

          {_next_state, {:ok, results}} =
            StateMachine.apply(%{}, {:batch, Enum.map(commands, &elem(&1, 1))}, state)

          assert results ==
                   Enum.map(commands, fn {_key, _command, reason, _ext} -> {:error, reason} end)

          Enum.each(commands, fn {key, _command, _reason, ext} ->
            path = Ferricstore.ProbFile.path(Path.join(dir, "prob"), key, ext)
            refute File.exists?(path)
            assert [] == :ets.lookup(ets, key)
          end)

          refute File.exists?(Path.join(dir, "prob"))
        end

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
