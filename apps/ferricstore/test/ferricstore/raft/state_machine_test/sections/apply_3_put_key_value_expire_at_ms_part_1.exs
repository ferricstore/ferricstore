defmodule Ferricstore.Raft.StateMachineTest.Sections.Apply3PutKeyValueExpireAtMsPart1 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.{BlobCommand, StateMachine}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

      describe "apply/3 with {:put, key, value, expire_at_ms} part 1" do
        test "uses raft meta system_time when checking cross-shard lock expiry", %{
          state: state,
          ets: ets
        } do
          local_now = Ferricstore.HLC.now_ms()
          apply_now = local_now - 20_000
          lock_expires_after_apply_time = apply_now + 10_000

          locked_state = %{
            state
            | cross_shard_locks: %{
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
            | cross_shard_locks: %{
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

        test "cross-shard dispatched SETEX uses stamped HLC time for relative expiry", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          local_now = Ferricstore.HLC.now_ms()
          stamped_now = local_now - 30_000

          {_new_state, %{^shard_index => [:ok]}} =
            StateMachine.apply(
              %{system_time: local_now},
              {{:cross_shard_tx,
                [{shard_index, [{"SETEX", ["stamped_setex", "5", "value"]}], nil}]},
               %{hlc_ts: {stamped_now, 0}}},
              state
            )

          expected_expire_at_ms = stamped_now + 5_000

          assert [{"stamped_setex", "value", ^expected_expire_at_ms, _, _, _, _}] =
                   :ets.lookup(ets, "stamped_setex")
        end

        test "cross-shard dispatched RMW commands preserve existing TTL", %{
          state: state,
          ets: ets,
          shard_index: shard_index
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

            {_new_state, %{^shard_index => [_result]}} =
              StateMachine.apply(
                %{system_time: Ferricstore.HLC.now_ms()},
                {:cross_shard_tx, [{shard_index, [command], nil}]},
                state
              )

            assert [{^key, ^expected, ^expire_at_ms, _lfu, _fid, _off, _vsize}] =
                     :ets.lookup(ets, key)
          end)
        end

        test "cross-shard dispatched EXISTS uses cold metadata without pread", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          key = "cross_tx_exists_cold"

          :ets.insert(
            ets,
            {key, nil, 0, Ferricstore.Store.LFU.initial(), 0, 999_999, byte_size("large-cold")}
          )

          {_new_state, %{^shard_index => [1]}} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:cross_shard_tx, [{shard_index, [{"EXISTS", [key]}], nil}]},
              state
            )

          assert [{^key, nil, 0, _lfu, 0, 999_999, _vsize}] = :ets.lookup(ets, key)
        end

        test "cross-shard dispatched SET is appended before acknowledgement", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path,
          shard_index: shard_index,
          writer_pid: writer_pid
        } do
          GenServer.stop(writer_pid, :normal, 5_000)

          {_new_state, %{^shard_index => [:ok]}} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:cross_shard_tx,
               [{shard_index, [{"SET", ["cross_durable", "durable-value"]}], nil}]},
              state
            )

          value_size = byte_size("durable-value")

          assert {:ok, [{"cross_durable", _off, ^value_size, 0, false}]} =
                   NIF.v2_scan_file(active_file_path)

          assert [{"cross_durable", "durable-value", 0, _, 0, _off, ^value_size}] =
                   :ets.lookup(ets, "cross_durable")
        end

        test "cross-shard transaction rolls back staged writes when a later entry errors", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path,
          shard_index: shard_index,
          writer_pid: writer_pid
        } do
          GenServer.stop(writer_pid, :normal, 5_000)

          {_new_state, {:error, "ERR invalid flow cross-shard terminal op"}} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:cross_shard_tx,
               [
                 {shard_index,
                  [
                    {"SET", ["cross_error_rollback", "must-not-persist"]},
                    {:flow_cross_terminal, :bogus, %{}}
                  ], nil}
               ]},
              state
            )

          assert [] = :ets.lookup(ets, "cross_error_rollback")
          assert {:ok, []} = NIF.v2_scan_file(active_file_path)
        end

        test "cross-shard transaction rolls back staged writes when a later entry raises", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path,
          shard_index: shard_index,
          writer_pid: writer_pid
        } do
          GenServer.stop(writer_pid, :normal, 5_000)

          assert_raise FunctionClauseError, fn ->
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:cross_shard_tx,
               [
                 {shard_index,
                  [
                    {"SET", ["cross_raise_rollback", "must-not-persist"]},
                    {:flow_cross_terminal, :complete, %{id: "missing-lease-token"}}
                  ], nil}
               ]},
              state
            )
          end

          assert [] = :ets.lookup(ets, "cross_raise_rollback")
          assert {:ok, []} = NIF.v2_scan_file(active_file_path)
        end

        test "failed cross-shard multi-target append does not leave replayable partial records",
             %{
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
            {_new_state, {:error, {:bitcask_append_failed, _reason}}} =
              StateMachine.apply(
                %{system_time: Ferricstore.HLC.now_ms()},
                {:cross_shard_tx,
                 [
                   {shard0, [{"SET", ["partial_success", "must-not-replay"]}], nil},
                   {shard1, [{"SET", ["partial_failure", "fail"]}], nil}
                 ]},
                state
              )

            assert [] = :ets.lookup(ets, "partial_success")

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

              assert [] = :ets.lookup(recovered, "partial_success")
            after
              :ets.delete(recovered)
            end
          after
            :ets.delete(ets1)
            Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
            File.rm_rf!(root)
          end
        end

        @tag :flow_policy_atomicity
        test "cross-shard Flow policy put rolls back every shard when reindex is rejected", %{
          state: state,
          ets: ets,
          active_file_path: shard0_file
        } do
          root =
            Path.join(
              System.tmp_dir!(),
              "sm_cross_policy_#{System.unique_integer([:positive])}"
            )

          shard0 = 0
          shard1 = 1
          shard1_path = Ferricstore.DataDir.shard_data_path(root, shard1)
          shard1_file = Path.join(shard1_path, "00000.log")
          File.mkdir_p!(shard1_path)
          File.touch!(shard1_file)

          ets1 =
            :ets.new(:"sm_cross_policy_#{System.unique_integer([:positive])}", [:set, :public])

          filler_rows =
            Enum.map(1..10_001, fn index ->
              key = "unrelated:#{index}"
              {key, <<1>>, 0, Ferricstore.Store.LFU.initial(), 0, 0, 1}
            end)

          true = :ets.insert(ets1, filler_rows)

          instance_ctx = %{
            name: :"sm_cross_policy_#{System.unique_integer([:positive])}",
            data_dir: root,
            shard_count: 2,
            keydir_refs: List.to_tuple([ets, ets1]),
            keydir_binary_bytes: :atomics.new(2, signed: false),
            checkpoint_flags: :atomics.new(2, signed: false),
            checkpoint_in_flight: :atomics.new(2, signed: false),
            disk_pressure: :atomics.new(2, signed: false),
            hot_cache_max_value_size: 64
          }

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
            shard1_file,
            shard1_path
          )

          type = "atomic-policy"
          policy_key = Ferricstore.Flow.Keys.policy_key(type)

          {:ok, policy} =
            Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type,
              indexed_state_meta: "version"
            )

          policy_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy)

          command =
            {:cross_shard_tx,
             [
               {shard0, [{0, {:flow_cross_policy_put, shard0, policy_key, policy_value, 0}}],
                nil},
               {shard1, [{1, {:flow_cross_policy_put, shard1, policy_key, policy_value, 0}}], nil}
             ]}

          try do
            state = %{state | shard_index: shard0, instance_ctx: instance_ctx}

            {_new_state, {:error, error}} =
              StateMachine.apply(%{system_time: Ferricstore.HLC.now_ms()}, command, state)

            assert error =~ "flow policy reindex exceeds"
            assert [] = :ets.lookup(ets, policy_key)
            assert [] = :ets.lookup(ets1, policy_key)
          after
            :ets.delete(ets1)
            Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
            File.rm_rf!(root)
          end
        end

        test "failed cross-shard multi-target overwrite restores replayable original record", %{
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
            {_new_state, {:error, {:bitcask_append_failed, _reason}}} =
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
        test "cross-shard compensation restores a cold WARaft apply-projection original", %{
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

            {_new_state, {:error, {:bitcask_append_failed, _reason}}} =
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

        test "cross-shard dispatched large SET has a cold location before acknowledgement", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path,
          shard_index: shard_index,
          writer_pid: writer_pid
        } do
          GenServer.stop(writer_pid, :normal, 5_000)
          large_value = String.duplicate("x", 70_000)

          {_new_state, %{^shard_index => [:ok]}} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:cross_shard_tx, [{shard_index, [{"SET", ["cross_large", large_value]}], nil}]},
              state
            )

          value_size = byte_size(large_value)

          assert {:ok, [{"cross_large", offset, ^value_size, 0, false}]} =
                   NIF.v2_scan_file(active_file_path)

          assert [{"cross_large", nil, 0, _, 0, ^offset, ^value_size}] =
                   :ets.lookup(ets, "cross_large")

          assert {:ok, ^large_value} = NIF.v2_pread_at(active_file_path, offset)
        end

        test "cross-shard dispatched PEXPIRE uses stamped HLC time for relative expiry", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          local_now = Ferricstore.HLC.now_ms()
          stamped_now = local_now - 30_000

          :ets.insert(
            ets,
            {"stamped_pexpire", "value", 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size("value")}
          )

          {_new_state, %{^shard_index => [1]}} =
            StateMachine.apply(
              %{system_time: local_now},
              {{:cross_shard_tx,
                [{shard_index, [{"PEXPIRE", ["stamped_pexpire", "5000"]}], nil}]},
               %{hlc_ts: {stamped_now, 0}}},
              state
            )

          expected_expire_at_ms = stamped_now + 5_000

          assert [{"stamped_pexpire", "value", ^expected_expire_at_ms, _, _, _, _}] =
                   :ets.lookup(ets, "stamped_pexpire")
        end

        test "cross-shard dispatched PEXPIREAT compares absolute expiry to stamped HLC time", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          local_now = Ferricstore.HLC.now_ms()
          stamped_now = local_now - 30_000
          expire_at_ms = stamped_now + 5_000

          :ets.insert(
            ets,
            {"stamped_pexpireat", "value", 0, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size("value")}
          )

          {_new_state, %{^shard_index => [1]}} =
            StateMachine.apply(
              %{system_time: local_now},
              {{:cross_shard_tx,
                [
                  {shard_index,
                   [{"PEXPIREAT", ["stamped_pexpireat", Integer.to_string(expire_at_ms)]}], nil}
                ]}, %{hlc_ts: {stamped_now, 0}}},
              state
            )

          assert [{"stamped_pexpireat", "value", ^expire_at_ms, _, _, _, _}] =
                   :ets.lookup(ets, "stamped_pexpireat")
        end

        test "cross-shard dispatched PTTL reports remaining time from stamped HLC time", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          local_now = Ferricstore.HLC.now_ms()
          stamped_now = local_now - 30_000
          expire_at_ms = stamped_now + 5_000

          :ets.insert(
            ets,
            {"stamped_pttl", "value", expire_at_ms, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size("value")}
          )

          {_new_state, %{^shard_index => [5_000]}} =
            StateMachine.apply(
              %{system_time: local_now},
              {{:cross_shard_tx, [{shard_index, [{"PTTL", ["stamped_pttl"]}], nil}]},
               %{hlc_ts: {stamped_now, 0}}},
              state
            )
        end

        test "cross-shard GET reads cold value from valid file id zero", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path,
          shard_index: shard_index
        } do
          {:ok, {offset, value_size}} =
            NIF.v2_append_record(active_file_path, "cross_cold_fid0", "cold-value", 0)

          :ets.insert(
            ets,
            {"cross_cold_fid0", nil, 0, Ferricstore.Store.LFU.initial(), 0, offset, value_size}
          )

          {_new_state, %{^shard_index => ["cold-value"]}} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:cross_shard_tx, [{shard_index, [{"GET", ["cross_cold_fid0"]}], nil}]},
              state
            )
        end

        test "cross-shard GET reads WARaft apply projection cold rows", %{
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

          {_new_state, %{^shard_index => [^value]}} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:cross_shard_tx, [{shard_index, [{"GET", [key]}], nil}]},
              state
            )
        end

        test "cross-shard MGET preserves values from hot keydir entries", %{
          state: state,
          ets: ets,
          shard_index: shard_index
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

          {_new_state, %{^shard_index => [["value-a", "value-b"]]}} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:cross_shard_tx,
               [{shard_index, [{"MGET", ["cross_mget_a", "cross_mget_b"]}], nil}]},
              state
            )
        end

        test "cross-shard MGET reads WARaft apply projection cold rows", %{
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

          {_new_state, %{^shard_index => [["value-a", "value-b"]]}} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:cross_shard_tx, [{shard_index, [{"MGET", [key_a, key_b]}], nil}]},
              state
            )
        end
      end
    end
  end
end
