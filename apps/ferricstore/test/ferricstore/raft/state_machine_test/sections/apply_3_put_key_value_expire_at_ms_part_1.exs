defmodule Ferricstore.Raft.StateMachineTest.Sections.Apply3PutKeyValueExpireAtMsPart1 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

      describe "apply/3 with {:put, key, value, expire_at_ms} part 1" do
        @tag :single_pending_rollback
        test "non-local apply exits execute pending rollback exactly once", %{state: state} do
          previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)
          module = Ferricstore.Raft.StateMachine
          test_pid = self()

          tracer =
            spawn_link(fn ->
              loop = fn loop ->
                receive do
                  {:trace, _pid, :call, {^module, :rollback_pending_writes, [_state]}} ->
                    send(test_pid, :pending_rollback_called)
                    loop.(loop)

                  :stop ->
                    :ok

                  _other ->
                    loop.(loop)
                end
              end

              loop.(loop)
            end)

          Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, _batch ->
            throw(:forced_append_exit)
          end)

          assert :erlang.trace_pattern({module, :rollback_pending_writes, 1}, true, [:local]) > 0
          assert :erlang.trace(self(), true, [:call, {:tracer, tracer}]) == 1

          on_exit(fn ->
            :erlang.trace(self(), false, [:call])
            :erlang.trace_pattern({module, :rollback_pending_writes, 1}, false, [:local])
            send(tracer, :stop)

            if previous_hook do
              Application.put_env(:ferricstore, :standalone_durability_hook, previous_hook)
            else
              Application.delete_env(:ferricstore, :standalone_durability_hook)
            end
          end)

          assert catch_throw(
                   StateMachine.apply_standalone_command(
                     {:put, "single-rollback", "value", 0},
                     state
                   )
                 ) == :forced_append_exit

          assert_receive :pending_rollback_called, 100
          refute_receive :pending_rollback_called, 50
        end

        test "transaction apply rejects raw queue tuples before mutation", %{
          state: state,
          ets: ets
        } do
          command = {:tx_execute, [{"SET", ["unprepared_tx", "must-not-apply"]}], nil}

          assert {_new_state, {:error, "ERR invalid transaction command"}} =
                   Ferricstore.Raft.StateMachine.apply(%{}, command, state)

          assert [] = :ets.lookup(ets, "unprepared_tx")
        end

        @tag :transaction_generic_batch_barrier
        test "generic Raft batches reject nested transaction entries before mutation", %{
          state: state,
          ets: ets
        } do
          {:ok, prepared_set} =
            Ferricstore.Commands.PreparedCommand.prepare("SET", [
              "nested_transaction_key",
              "transaction-value"
            ])

          {:ok, set_entry} =
            Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_set)

          command =
            {:batch,
             [
               {:put, "outside_transaction_key", "outside-value", 0},
               {:ferricstore_latency_trace, {:tx_execute, [set_entry], nil}}
             ]}

          {_new_state, result} = StateMachine.apply(%{}, command, state)

          assert {:error, {:batch_barrier_command, :tx_execute}} = result
          assert [] == :ets.lookup(ets, "outside_transaction_key")
          assert [] == :ets.lookup(ets, "nested_transaction_key")
        end

        test "uses raft meta system_time when checking fetch-or-compute lock expiry", %{
          state: state,
          ets: ets
        } do
          local_now = Ferricstore.HLC.now_ms()
          apply_now = local_now - 20_000
          lock_expires_after_apply_time = apply_now + 10_000

          locked_state = %{
            state
            | fetch_or_compute_locks: %{
                "meta_time_locked" => {make_ref(), lock_expires_after_apply_time}
              }
          }

          {_new_state, result} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:put, "meta_time_locked", "value", 0},
              locked_state
            )

          assert {:error, :key_locked} = result
          assert [] == :ets.lookup(ets, "meta_time_locked")
        end

        test "prefers stamped command HLC over raft meta system_time", %{
          state: state,
          ets: ets
        } do
          local_now = Ferricstore.HLC.now_ms()
          meta_now = local_now - 30_000
          hlc_now = meta_now + 20_000
          lock_expires_between_meta_and_hlc = meta_now + 10_000

          locked_state = %{
            state
            | fetch_or_compute_locks: %{
                "hlc_time_locked" => {make_ref(), lock_expires_between_meta_and_hlc}
              }
          }

          {_new_state, result} =
            StateMachine.apply(
              %{system_time: meta_now},
              {{:put, "hlc_time_locked", "value", 0}, %{hlc_ts: {hlc_now, 0}}},
              locked_state
            )

          assert :ok = result

          assert [{"hlc_time_locked", "value", 0, _, _, _, _}] =
                   :ets.lookup(ets, "hlc_time_locked")
        end

        test "transaction SETEX uses stamped HLC time for relative expiry", %{
          state: state,
          ets: ets
        } do
          local_now = Ferricstore.HLC.now_ms()
          stamped_now = local_now - 30_000

          {_new_state, [:ok]} =
            StateMachine.apply(
              %{system_time: local_now},
              {{:tx_execute, [{"SETEX", ["stamped_setex", "5", "value"]}], nil},
               %{hlc_ts: {stamped_now, 0}}},
              state
            )

          expected_expire_at_ms = stamped_now + 5_000

          assert [{"stamped_setex", "value", ^expected_expire_at_ms, _, _, _, _}] =
                   :ets.lookup(ets, "stamped_setex")
        end

        test "transaction RMW commands preserve existing TTL", %{
          state: state,
          ets: ets
        } do
          expire_at_ms = Ferricstore.HLC.now_ms() + 60_000

          cases = [
            {"cross_tx_incr_ttl", "5", {"INCR", ["cross_tx_incr_ttl"]}, "6"},
            {"cross_tx_append_ttl", "base", {"APPEND", ["cross_tx_append_ttl", "-tail"]},
             "base-tail"},
            {"cross_tx_setrange_ttl", "abcdef",
             {"SETRANGE", ["cross_tx_setrange_ttl", "2", "ZZ"]}, "abZZef"}
          ]

          Enum.each(cases, fn {key, initial, command, expected} ->
            :ets.insert(
              ets,
              {key, initial, expire_at_ms, Ferricstore.Store.LFU.initial(), 0, 0,
               byte_size(initial)}
            )

            {_new_state, [_result]} =
              StateMachine.apply(
                %{system_time: Ferricstore.HLC.now_ms()},
                {:tx_execute, [command], nil},
                state
              )

            assert [{^key, ^expected, ^expire_at_ms, _lfu, _fid, _off, _vsize}] =
                     :ets.lookup(ets, key)
          end)
        end

        test "transaction EXISTS uses cold metadata without pread", %{
          state: state,
          ets: ets
        } do
          key = "cross_tx_exists_cold"

          :ets.insert(
            ets,
            {key, nil, 0, Ferricstore.Store.LFU.initial(), 0, 999_999, byte_size("large-cold")}
          )

          {_new_state, [1]} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:tx_execute, [{"EXISTS", [key]}], nil},
              state
            )

          assert [{^key, nil, 0, _lfu, 0, 999_999, _vsize}] = :ets.lookup(ets, key)
        end

        test "transaction SET is appended before acknowledgement", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path,
          writer_pid: writer_pid
        } do
          GenServer.stop(writer_pid, :normal, 5_000)

          {_new_state, [:ok]} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:tx_execute, [{"SET", ["tx_durable", "durable-value"]}], nil},
              state
            )

          value_size = byte_size("durable-value")

          assert {:ok, [{"tx_durable", _off, ^value_size, 0, false}]} =
                   NIF.v2_scan_file(active_file_path)

          assert [{"tx_durable", "durable-value", 0, _, 0, _off, ^value_size}] =
                   :ets.lookup(ets, "tx_durable")
        end

        @tag :transaction_stream_cache_commit
        test "failed transaction DEL preserves committed stream cache metadata", %{
          state: state,
          ets: ets
        } do
          key = "tx_stream_del_#{System.unique_integer([:positive])}"
          type_key = CompoundKey.type_key(key)
          durable_meta_key = CompoundKey.stream_meta_key(key)

          {state2, :ok} = StateMachine.apply(%{}, {:compound_put, type_key, "stream", 0}, state)

          {state3, :ok} =
            StateMachine.apply(%{}, {:compound_put, durable_meta_key, "durable-meta", 0}, state2)

          Ferricstore.Commands.Stream.ensure_meta_table()
          cache_key = {:default, key}
          cached_meta = {cache_key, 1, "1-0", "1-0", 1, 0}
          :ets.insert(Ferricstore.Stream.Meta, cached_meta)

          previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

          Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, _batch ->
            {:error, :enospc}
          end)

          on_exit(fn ->
            if previous_hook do
              Application.put_env(:ferricstore, :standalone_durability_hook, previous_hook)
            else
              Application.delete_env(:ferricstore, :standalone_durability_hook)
            end

            if :ets.whereis(Ferricstore.Stream.Meta) != :undefined do
              :ets.delete(Ferricstore.Stream.Meta, cache_key)
            end
          end)

          {:ok, prepared_del} =
            Ferricstore.Commands.PreparedCommand.prepare("DEL", [key])

          {:ok, del_entry} =
            Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_del)

          {failed_state, result} =
            StateMachine.apply_standalone_command(
              {:tx_execute, [del_entry], nil},
              state3
            )

          assert {:error, {:bitcask_append_failed, :enospc}} = result
          assert [cached_meta] == :ets.lookup(Ferricstore.Stream.Meta, cache_key)
          assert [{^type_key, "stream", 0, _, _, _, _}] = :ets.lookup(ets, type_key)

          assert [{^durable_meta_key, "durable-meta", 0, _, _, _, _}] =
                   :ets.lookup(ets, durable_meta_key)

          Application.put_env(
            :ferricstore,
            :standalone_durability_hook,
            fn _path, _batch -> :passthrough end
          )

          {_committed_state, [1]} =
            StateMachine.apply_standalone_command(
              {:tx_execute, [del_entry], nil},
              failed_state
            )

          assert [] == :ets.lookup(Ferricstore.Stream.Meta, cache_key)
          assert [] == :ets.lookup(ets, type_key)
          assert [] == :ets.lookup(ets, durable_meta_key)
        end

        @tag :transaction_compound_batch_failure
        test "compound batch externalization failure rolls back fields staged earlier", %{
          state: state,
          ets: ets
        } do
          threshold = 32
          key = "tx_hash_blob_failure"

          instance_ctx = %{
            data_dir: state.data_dir,
            shard_count: 1,
            keydir_refs: {ets},
            blob_side_channel_threshold_bytes: threshold
          }

          state = Map.put(state, :instance_ctx, instance_ctx)

          Process.put(:ferricstore_blob_store_write_hook, fn _io, _iodata ->
            {:error, :enospc}
          end)

          on_exit(fn -> Process.delete(:ferricstore_blob_store_write_hook) end)

          {:ok, prepared_hset} =
            Ferricstore.Commands.PreparedCommand.prepare("HSET", [
              key,
              "small",
              "inline",
              "large",
              :binary.copy("x", threshold + 1)
            ])

          {:ok, hset_entry} =
            Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_hset)

          {_failed_state, result} =
            StateMachine.apply_standalone_command(
              {:tx_execute, [hset_entry], nil},
              state
            )

          assert {:error, {:blob_externalize_failed, _reason}} = result
          assert [] == :ets.lookup(ets, CompoundKey.type_key(key))
          assert [] == :ets.lookup(ets, CompoundKey.hash_field(key, "small"))
          assert [] == :ets.lookup(ets, CompoundKey.hash_field(key, "large"))
        end

        @tag :transaction_compound_batch_failure
        test "compound batch externalization uses one blob fsync", %{state: state, ets: ets} do
          threshold = 32
          fsync_count = :atomics.new(1, signed: false)

          instance_ctx = %{
            data_dir: state.data_dir,
            shard_count: 1,
            keydir_refs: {ets},
            blob_side_channel_threshold_bytes: threshold
          }

          state = Map.put(state, :instance_ctx, instance_ctx)

          Process.put(:ferricstore_blob_store_fsync_file_hook, fn _path ->
            :atomics.add(fsync_count, 1, 1)
            :ok
          end)

          on_exit(fn -> Process.delete(:ferricstore_blob_store_fsync_file_hook) end)

          {:ok, prepared_hset} =
            Ferricstore.Commands.PreparedCommand.prepare("HSET", [
              "tx_hash_blob_batch",
              "first",
              :binary.copy("a", threshold + 1),
              "second",
              :binary.copy("b", threshold + 1)
            ])

          {:ok, hset_entry} =
            Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_hset)

          {_new_state, [2]} =
            StateMachine.apply_standalone_command(
              {:tx_execute, [hset_entry], nil},
              state
            )

          assert :atomics.get(fsync_count, 1) == 1
        end

        @tag :transaction_plain_batch_externalization
        test "MSET externalizes large values with one blob fsync", %{state: state, ets: ets} do
          threshold = 32
          fsync_count = :atomics.new(1, signed: false)

          instance_ctx = %{
            data_dir: state.data_dir,
            shard_count: 1,
            keydir_refs: {ets},
            blob_side_channel_threshold_bytes: threshold
          }

          state = Map.put(state, :instance_ctx, instance_ctx)

          Process.put(:ferricstore_blob_store_fsync_file_hook, fn _path ->
            :atomics.add(fsync_count, 1, 1)
            :ok
          end)

          on_exit(fn -> Process.delete(:ferricstore_blob_store_fsync_file_hook) end)

          first = :binary.copy("a", threshold + 1)
          second = :binary.copy("b", threshold + 1)

          {:ok, prepared_mset} =
            Ferricstore.Commands.PreparedCommand.prepare("MSET", [
              "tx_mset_blob_first",
              first,
              "tx_mset_blob_second",
              second
            ])

          {:ok, mset_entry} =
            Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_mset)

          {_new_state, [:ok]} =
            StateMachine.apply_standalone_command(
              {:tx_execute, [mset_entry], nil},
              state
            )

          assert :atomics.get(fsync_count, 1) == 1

          assert [{"tx_mset_blob_first", _, 0, _, _, _, _}] =
                   :ets.lookup(ets, "tx_mset_blob_first")

          assert [{"tx_mset_blob_second", _, 0, _, _, _, _}] =
                   :ets.lookup(ets, "tx_mset_blob_second")
        end

        @tag :transaction_plain_batch_externalization
        test "MSET externalization failure leaves every destination absent", %{
          state: state,
          ets: ets
        } do
          threshold = 32

          instance_ctx = %{
            data_dir: state.data_dir,
            shard_count: 1,
            keydir_refs: {ets},
            blob_side_channel_threshold_bytes: threshold
          }

          state = Map.put(state, :instance_ctx, instance_ctx)

          Process.put(:ferricstore_blob_store_write_hook, fn _io, _iodata ->
            {:error, :enospc}
          end)

          on_exit(fn -> Process.delete(:ferricstore_blob_store_write_hook) end)

          {:ok, prepared_mset} =
            Ferricstore.Commands.PreparedCommand.prepare("MSET", [
              "tx_mset_inline",
              "inline",
              "tx_mset_blob",
              :binary.copy("x", threshold + 1)
            ])

          {:ok, mset_entry} =
            Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_mset)

          {_failed_state, result} =
            StateMachine.apply_standalone_command(
              {:tx_execute, [mset_entry], nil},
              state
            )

          assert {:error, {:blob_externalize_failed, _reason}} = result
          assert [] == :ets.lookup(ets, "tx_mset_inline")
          assert [] == :ets.lookup(ets, "tx_mset_blob")
        end

        test "request-scoped commands cannot enter replicated transaction apply", %{ets: ets} do
          commands = [
            {"CAS", ["tx_cas", "before", "after"], "tx_cas"},
            {"LOCK", ["tx_lock", "owner", "5000"], "tx_lock"},
            {"EXTEND", ["tx_lock", "owner", "9000"], "tx_lock"},
            {"UNLOCK", ["tx_lock", "owner"], "tx_lock"},
            {"RATELIMIT.ADD", ["tx_rate", "1000", "2", "1"], "tx_rate"}
          ]

          Enum.each(commands, fn {command, args, key} ->
            assert {:ok, prepared} =
                     Ferricstore.Commands.PreparedCommand.prepare(command, args)

            expected_error =
              "ERR command '#{String.downcase(command)}' is not supported inside transactions"

            assert {:error, ^expected_error} =
                     Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)

            assert [] = :ets.lookup(ets, key)
          end)
        end

        test "probabilistic commands cannot enter replicated transaction apply", %{ets: ets} do
          assert {:ok, prepared} =
                   Ferricstore.Commands.PreparedCommand.prepare("BF.ADD", ["tx_bloom", "member"])

          assert {:error, "ERR command 'bf.add' is not supported inside transactions"} =
                   Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)

          assert [] = :ets.lookup(ets, "tx_bloom")
        end

        @tag :flow_policy_atomicity
        test "policy allocation ignores unrelated keydir size", %{state: state, ets: ets} do
          filler_rows =
            Enum.map(1..10_001, fn index ->
              key = "unrelated:#{index}"
              {key, <<1>>, 0, Ferricstore.Store.LFU.initial(), 0, 0, 1}
            end)

          true = :ets.insert(ets, filler_rows)

          type = "atomic-policy"
          policy_key = Ferricstore.Flow.Keys.policy_key(type)
          job_key = Ferricstore.Flow.Keys.policy_migration_job_key(type)

          {:ok, policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type,
              indexed_state_meta: "version"
            )

          policy_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy)

          {_new_state, {:ok, stored_value}} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:flow_policy_allocate, policy_key, policy_value, 0},
              state
            )

          assert [{^policy_key, ^stored_value, 0, _, _, _, _}] = :ets.lookup(ets, policy_key)

          assert {:ok, {1, ^policy}} =
                   Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(stored_value)

          assert [{^job_key, job_value, 0, _, _, _, _}] = :ets.lookup(ets, job_key)

          assert {:ok,
                  %{
                    type: ^type,
                    migration_generation: 1,
                    indexed_state_meta: "version",
                    status: :active
                  }} = Ferricstore.Flow.PolicyMigration.decode_job(job_value)
        end

        @tag :flow_policy_generation
        test "policy allocation advances high-water and stale installs cannot overwrite", %{
          state: state,
          ets: ets
        } do
          type = "target-high-water-policy"
          policy_key = Ferricstore.Flow.Keys.policy_key(type)

          {:ok, old_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 1_000)

          {:ok, new_policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 2_000)

          stored_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(old_policy, 3)
          input_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(new_policy)

          {_state, :ok} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_policy_put, policy_key, stored_value, 0},
              state
            )

          {_state, {:ok, allocated_value}} =
            StateMachine.apply(
              %{system_time: 2_000},
              {:flow_policy_allocate, policy_key, input_value, 0},
              state
            )

          assert {:ok, {4, ^new_policy}} =
                   Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(allocated_value)

          stale_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(old_policy, 2)

          {_state, :ok} =
            StateMachine.apply(
              %{system_time: 3_000},
              {:flow_policy_put, policy_key, stale_value, 0},
              state
            )

          assert [{^policy_key, ^allocated_value, 0, _, _, _, _}] =
                   :ets.lookup(ets, policy_key)
        end

        test "rejected multi-group envelope preserves the replayable original record", %{
          state: state,
          ets: ets,
          active_file_path: shard0_file
        } do
          root =
            Path.join(System.tmp_dir!(), "sm_cross_partial_#{System.unique_integer([:positive])}")

          shard0 = 0
          shard1 = 1
          shard1_path = Ferricstore.DataDir.shard_data_path(root, shard1)
          shard1_bad_active = Path.join(shard1_path, "active_is_directory.log")

          ets1 =
            :ets.new(:"sm_cross_partial_#{System.unique_integer([:positive])}", [:set, :public])

          instance_name = :"sm_cross_partial_#{System.unique_integer([:positive])}"

          {:ok, {old_offset, old_size}} =
            NIF.v2_append_record(shard0_file, "partial_existing", "old", 0)

          :ets.insert(
            ets,
            {"partial_existing", "old", 0, Ferricstore.Store.LFU.initial(), 0, old_offset,
             old_size}
          )

          File.mkdir_p!(shard1_bad_active)
          Ferricstore.Store.ActiveFile.init(2)

          instance_ctx = %{
            name: instance_name,
            data_dir: root,
            shard_count: 2,
            keydir_refs: List.to_tuple([ets, ets1]),
            keydir_binary_bytes: :atomics.new(2, signed: false),
            checkpoint_flags: :atomics.new(shard1 + 1, signed: false),
            checkpoint_in_flight: :atomics.new(shard1 + 1, signed: false),
            disk_pressure: :atomics.new(shard1 + 1, signed: false),
            hot_cache_max_value_size: 64
          }

          Ferricstore.Store.ActiveFile.publish(
            instance_ctx,
            shard0,
            0,
            shard0_file,
            state.shard_data_path
          )

          Ferricstore.Store.ActiveFile.publish(
            instance_ctx,
            shard1,
            0,
            shard1_bad_active,
            shard1_path
          )

          state = %{state | shard_index: shard0, instance_ctx: instance_ctx}

          try do
            {_new_state, {:error, "CROSSSLOT cross-shard Raft transactions are not supported"}} =
              StateMachine.apply(
                %{system_time: Ferricstore.HLC.now_ms()},
                {:cross_shard_tx,
                 [
                   {shard0, [{"SET", ["partial_existing", "new"]}], nil},
                   {shard1, [{"SET", ["partial_failure", "fail"]}], nil}
                 ]},
                state
              )

            assert [{"partial_existing", "old", 0, _, 0, ^old_offset, ^old_size}] =
                     :ets.lookup(ets, "partial_existing")

            recovered =
              :ets.new(:"sm_cross_partial_recovered_#{System.unique_integer([:positive])}", [
                :set,
                :public
              ])

            try do
              Ferricstore.Store.Shard.Lifecycle.recover_keydir(
                state.shard_data_path,
                recovered,
                shard0,
                instance_ctx
              )

              assert [{"partial_existing", nil, 0, _, 0, recovered_offset, 3}] =
                       :ets.lookup(recovered, "partial_existing")

              assert {:ok, "old"} = NIF.v2_pread_at(shard0_file, recovered_offset)
            after
              :ets.delete(recovered)
            end
          after
            :ets.delete(ets1)
            Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
            File.rm_rf!(root)
          end
        end

        @tag :cross_shard_waraft_compensation
        test "rejected multi-group envelope preserves a cold apply-projection original", %{
          state: state,
          ets: ets,
          active_file_path: shard0_file
        } do
          shard0 = 0
          shard1 = 1
          root = state.data_dir
          shard1_path = Ferricstore.DataDir.shard_data_path(root, shard1)
          shard1_bad_active = Path.join(shard1_path, "active_is_directory.log")

          ets1 =
            :ets.new(:"sm_cross_waraft_#{System.unique_integer([:positive])}", [:set, :public])

          instance_ctx = %{
            name: :"sm_cross_waraft_#{System.unique_integer([:positive])}",
            data_dir: root,
            shard_count: 2,
            keydir_refs: List.to_tuple([ets, ets1]),
            keydir_binary_bytes: :atomics.new(2, signed: false),
            checkpoint_flags: :atomics.new(2, signed: false),
            checkpoint_in_flight: :atomics.new(2, signed: false),
            disk_pressure: :atomics.new(2, signed: false),
            hot_cache_max_value_size: 64
          }

          key = "partial_waraft_existing"
          old_value = "old-segment-value"
          projection_index = 91

          assert :ok =
                   Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                     root,
                     shard0,
                     projection_index,
                     [{key, old_value, 0}]
                   )

          original_row =
            {key, nil, 0, Ferricstore.Store.LFU.initial(),
             {:waraft_apply_projection, projection_index}, 0, byte_size(old_value)}

          :ets.insert(ets, original_row)
          File.mkdir_p!(shard1_bad_active)
          Ferricstore.Store.ActiveFile.init(2)

          Ferricstore.Store.ActiveFile.publish(
            instance_ctx,
            shard0,
            0,
            shard0_file,
            state.shard_data_path
          )

          Ferricstore.Store.ActiveFile.publish(
            instance_ctx,
            shard1,
            0,
            shard1_bad_active,
            shard1_path
          )

          try do
            state = %{state | shard_index: shard0, instance_ctx: instance_ctx}

            {_new_state, {:error, "CROSSSLOT cross-shard Raft transactions are not supported"}} =
              StateMachine.apply(
                %{system_time: Ferricstore.HLC.now_ms()},
                {:cross_shard_tx,
                 [
                   {shard0, [{"SET", [key, "new-value"]}], nil},
                   {shard1, [{"SET", ["partial_waraft_failure", "fail"]}], nil}
                 ]},
                state
              )

            assert [^original_row] = :ets.lookup(ets, key)
          after
            :ets.delete(ets1)
            Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
            File.rm_rf!(shard1_path)
          end
        end

        test "transaction large SET has a cold location before acknowledgement", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path,
          writer_pid: writer_pid
        } do
          GenServer.stop(writer_pid, :normal, 5_000)
          large_value = String.duplicate("x", 70_000)

          {_new_state, [:ok]} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:tx_execute, [{"SET", ["tx_large", large_value]}], nil},
              state
            )

          value_size = byte_size(large_value)

          assert {:ok, [{"tx_large", offset, ^value_size, 0, false}]} =
                   NIF.v2_scan_file(active_file_path)

          assert [{"tx_large", nil, 0, _, 0, ^offset, ^value_size}] =
                   :ets.lookup(ets, "tx_large")

          assert {:ok, ^large_value} = NIF.v2_pread_at(active_file_path, offset)
        end

        test "transaction PEXPIRE uses stamped HLC time for relative expiry", %{
          state: state,
          ets: ets
        } do
          local_now = Ferricstore.HLC.now_ms()
          stamped_now = local_now - 30_000

          :ets.insert(
            ets,
            {"stamped_pexpire", "value", 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size("value")}
          )

          {_new_state, [1]} =
            StateMachine.apply(
              %{system_time: local_now},
              {{:tx_execute, [{"PEXPIRE", ["stamped_pexpire", "5000"]}], nil},
               %{hlc_ts: {stamped_now, 0}}},
              state
            )

          expected_expire_at_ms = stamped_now + 5_000

          assert [{"stamped_pexpire", "value", ^expected_expire_at_ms, _, _, _, _}] =
                   :ets.lookup(ets, "stamped_pexpire")
        end

        test "transaction PEXPIREAT compares absolute expiry to stamped HLC time", %{
          state: state,
          ets: ets
        } do
          local_now = Ferricstore.HLC.now_ms()
          stamped_now = local_now - 30_000
          expire_at_ms = stamped_now + 5_000

          :ets.insert(
            ets,
            {"stamped_pexpireat", "value", 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size("value")}
          )

          {_new_state, [1]} =
            StateMachine.apply(
              %{system_time: local_now},
              {{:tx_execute,
                [{"PEXPIREAT", ["stamped_pexpireat", Integer.to_string(expire_at_ms)]}], nil},
               %{hlc_ts: {stamped_now, 0}}},
              state
            )

          assert [{"stamped_pexpireat", "value", ^expire_at_ms, _, _, _, _}] =
                   :ets.lookup(ets, "stamped_pexpireat")
        end

        test "transaction PTTL reports remaining time from stamped HLC time", %{
          state: state,
          ets: ets
        } do
          local_now = Ferricstore.HLC.now_ms()
          stamped_now = local_now - 30_000
          expire_at_ms = stamped_now + 5_000

          :ets.insert(
            ets,
            {"stamped_pttl", "value", expire_at_ms, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size("value")}
          )

          {_new_state, [5_000]} =
            StateMachine.apply(
              %{system_time: local_now},
              {{:tx_execute, [{"PTTL", ["stamped_pttl"]}], nil}, %{hlc_ts: {stamped_now, 0}}},
              state
            )
        end

        test "transaction GET reads cold value from valid file id zero", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          {:ok, {offset, value_size}} =
            NIF.v2_append_record(active_file_path, "cross_cold_fid0", "cold-value", 0)

          :ets.insert(
            ets,
            {"cross_cold_fid0", nil, 0, Ferricstore.Store.LFU.initial(), 0, offset, value_size}
          )

          {_new_state, ["cold-value"]} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:tx_execute, [{"GET", ["cross_cold_fid0"]}], nil},
              state
            )
        end

        test "transaction GET reads WARaft apply projection cold rows", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          key = "cross_waraft_projection_get"
          value = "segment-cold-value"
          projection_index = 77

          assert :ok =
                   Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                     state.data_dir,
                     shard_index,
                     projection_index,
                     [{key, value, 0}]
                   )

          :ets.insert(
            ets,
            {key, nil, 0, Ferricstore.Store.LFU.initial(),
             {:waraft_apply_projection, projection_index}, 0, byte_size(value)}
          )

          {_new_state, [^value]} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:tx_execute, [{"GET", [key]}], nil},
              state
            )
        end

        test "transaction MGET preserves values from hot keydir entries", %{
          state: state,
          ets: ets
        } do
          :ets.insert(
            ets,
            {"cross_mget_a", "value-a", 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size("value-a")}
          )

          :ets.insert(
            ets,
            {"cross_mget_b", "value-b", 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size("value-b")}
          )

          {_new_state, [["value-a", "value-b"]]} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:tx_execute, [{"MGET", ["cross_mget_a", "cross_mget_b"]}], nil},
              state
            )
        end

        test "transaction MGET reads WARaft apply projection cold rows", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          key_a = "cross_waraft_projection_mget_a"
          key_b = "cross_waraft_projection_mget_b"
          projection_index = 78

          assert :ok =
                   Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                     state.data_dir,
                     shard_index,
                     projection_index,
                     [{key_a, "value-a", 0}, {key_b, "value-b", 0}]
                   )

          :ets.insert(
            ets,
            {key_a, nil, 0, Ferricstore.Store.LFU.initial(),
             {:waraft_apply_projection, projection_index}, 0, byte_size("value-a")}
          )

          :ets.insert(
            ets,
            {key_b, nil, 0, Ferricstore.Store.LFU.initial(),
             {:waraft_apply_projection, projection_index}, 0, byte_size("value-b")}
          )

          {_new_state, [["value-a", "value-b"]]} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:tx_execute, [{"MGET", [key_a, key_b]}], nil},
              state
            )
        end
      end
    end
  end
end
