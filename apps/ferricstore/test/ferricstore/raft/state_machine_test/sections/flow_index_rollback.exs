defmodule Ferricstore.Raft.StateMachineTest.Sections.FlowIndexRollback do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.{BlobCommand, StateMachine}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

      describe "Flow index rollback" do
        test "rolls back Flow.OrderedIndex mutations when apply append fails", %{
          state: state,
          dir: dir
        } do
          :ets.new(state.zset_score_index_name, [:ordered_set, :public, :named_table])
          :ets.new(state.zset_score_lookup_name, [:set, :public, :named_table])
          :ets.new(state.flow_index_name, [:ordered_set, :public, :named_table])
          :ets.new(state.flow_lookup_name, [:set, :public, :named_table])

          on_exit(fn ->
            safe_delete_ets(state.zset_score_index_name)
            safe_delete_ets(state.zset_score_lookup_name)
            safe_delete_ets(state.flow_index_name)
            safe_delete_ets(state.flow_lookup_name)
          end)

          id = "flow-index-rollback"
          type = "index-rollback"
          partition_key = "tenant-index-rollback"
          due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)
          state_index_key = Ferricstore.Flow.Keys.state_index_key(type, "queued", partition_key)
          history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          bad_state = %{state | active_file_path: Path.join(dir, "missing.log")}

          {_state, {:error, :active_file_unavailable}} =
            StateMachine.apply(
              %{system_time: 1_000},
              {:flow_create, state_key,
               %{id: id, type: type, state: "queued", partition_key: partition_key}},
              bad_state
            )

          assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, due_key, id) ==
                   :miss

          assert Ferricstore.Flow.OrderedIndex.score_of(
                   state.flow_lookup_name,
                   state_index_key,
                   id
                 ) ==
                   :miss

          assert Ferricstore.Flow.OrderedIndex.count_all(state.flow_lookup_name, history_key) == 0
          assert Ferricstore.Flow.OrderedIndex.count_all(state.flow_lookup_name, due_key) == 0

          assert Ferricstore.Flow.OrderedIndex.count_all(state.flow_lookup_name, state_index_key) ==
                   0
        end
      end

      describe "promoted compound prefix delete" do
        test "waits for promoted compaction latch before cleanup", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          redis_key = "promoted_prefix_delete_latch_#{System.unique_integer([:positive])}"
          prefix = CompoundKey.hash_prefix(redis_key)
          compound_key = CompoundKey.hash_field(redis_key, "field")
          dedicated_path = Promotion.dedicated_path(state.data_dir, shard_index, :hash, redis_key)

          File.mkdir_p!(dedicated_path)
          File.touch!(Path.join(dedicated_path, "00000.log"))
          :ets.insert(ets, {compound_key, "value", 0, LFU.initial(), 0, 0, 5})

          latch_tab =
            :ets.new(:"sm_promoted_prefix_latch_#{System.unique_integer([:positive])}", [
              :set,
              :public
            ])

          latch_key = {:promoted_compaction, redis_key}
          assert :ets.insert_new(latch_tab, {latch_key, self()})

          latch_refs =
            List.duplicate(latch_tab, shard_index + 1)
            |> List.to_tuple()

          instance_ctx = %FerricStore.Instance{
            name: :state_machine_test,
            data_dir: state.data_dir,
            data_dir_expanded: state.data_dir,
            latch_refs: latch_refs
          }

          state = %{state | instance_ctx: instance_ctx}

          task =
            Task.async(fn ->
              StateMachine.apply(%{}, {:compound_delete_prefix, prefix}, state)
            end)

          try do
            refute Task.yield(task, 50)

            :ets.delete(latch_tab, latch_key)
            assert {%{}, :ok} = Task.await(task, 1_000)
          after
            safe_delete_ets(latch_tab)
          end
        end
      end

      describe "Bitcask rotation/accounting" do
        test "applied writes rotate the active file when they exceed max_active_file_size", %{
          state: state,
          active_file_path: active_file_path
        } do
          state = %{state | max_active_file_size: 80}
          value = String.duplicate("x", 48)

          {state, :ok} = StateMachine.apply(%{}, {:put, "rotate_a", value, 0}, state)
          {state, :ok} = StateMachine.apply(%{}, {:put, "rotate_b", value, 0}, state)

          assert state.active_file_id > 0
          assert state.active_file_size == 0
          assert File.exists?(state.active_file_path)
          assert state.active_file_path != active_file_path
          assert {old_total, 0} = Map.fetch!(state.file_stats, 0)
          assert old_total == File.stat!(active_file_path).size

          assert {:ok, [{_key, _offset, _value_size, _expire_at_ms, false} | _]} =
                   NIF.v2_scan_file(active_file_path)
        end
      end

      describe "origin async PUT replay" do
        test "persists the pending origin value when ETS still matches the command", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          :ets.insert(ets, {"origin_put", "old", 0, 1, :pending, 0, 0})

          {_state2, :ok} =
            StateMachine.apply(%{}, {:async, node(), {:put, "origin_put", "old", 0}}, state)

          assert {:ok, [{"origin_put", _off, 3, 0, false}]} = NIF.v2_scan_file(active_file_path)
          assert {:ok, "old"} = NIF.v2_pread_at(active_file_path, 0)
        end

        test "replays origin PUT when recovery has no local pending row", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          # If the origin crashes after Ra accepts the async command but before the
          # local pending write reaches Bitcask, recovery must apply the Ra log entry
          # instead of skipping it as an already-local write.
          {_state2, :ok} =
            StateMachine.apply(%{}, {:async, node(), {:put, "missing_origin_put", "v", 0}}, state)

          assert [{"missing_origin_put", "v", 0, _lfu, 0, 0, 1}] =
                   :ets.lookup(ets, "missing_origin_put")

          assert {:ok, [{"missing_origin_put", 0, 1, 0, false}]} =
                   NIF.v2_scan_file(active_file_path)
        end

        test "replays origin large PUT over an older cold value", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          key = "origin_large_put"
          old_value = String.duplicate("o", 70_000)
          new_value = String.duplicate("n", 70_000)

          {:ok, [{old_offset, _old_record_size}]} =
            NIF.v2_append_batch(active_file_path, [{key, old_value, 0}])

          :ets.insert(ets, {key, nil, 0, 1, 0, old_offset, byte_size(old_value)})

          {_state2, :ok} =
            StateMachine.apply(%{}, {:async, node(), {:put, key, new_value, 0}}, state)

          assert [{^key, nil, 0, _lfu, 0, new_offset, 70_000}] = :ets.lookup(ets, key)
          refute new_offset == old_offset
          assert {:ok, ^new_value} = NIF.v2_pread_at(active_file_path, new_offset)
        end

        test "does not duplicate an already-applied origin large PUT", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          key = "origin_large_put_applied"
          value = String.duplicate("v", 70_000)

          {:ok, [{offset, _record_size}]} =
            NIF.v2_append_batch(active_file_path, [{key, value, 0}])

          :ets.insert(ets, {key, nil, 0, 1, 0, offset, byte_size(value)})

          {_state2, :ok} =
            StateMachine.apply(%{}, {:async, node(), {:put, key, value, 0}}, state)

          assert [{^key, nil, 0, _lfu, 0, ^offset, 70_000}] = :ets.lookup(ets, key)
          assert {:ok, [{^key, ^offset, 70_000, 0, false}]} = NIF.v2_scan_file(active_file_path)
        end

        test "persists stale origin PUT for replay without publishing over newer pending ETS", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path,
          shard_index: shard_index
        } do
          # Router may apply a later async RMW locally before the earlier async PUT
          # reaches StateMachine.apply/3. The origin replay must not write the old
          # command value over the newer local value in ETS, but the earlier Ra
          # entry still needs a Bitcask record before its cursor can be released.
          checkpoint_flags = :atomics.new(shard_index + 1, signed: false)
          checkpoint_in_flight = :atomics.new(shard_index + 1, signed: false)
          disk_pressure = :atomics.new(shard_index + 1, signed: false)
          last_applied_index = :atomics.new(shard_index + 1, signed: false)
          last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)
          key = "stale_origin_put"

          state = %{
            state
            | release_cursor_interval: 1,
              instance_ctx: %{
                checkpoint_flags: checkpoint_flags,
                checkpoint_in_flight: checkpoint_in_flight,
                disk_pressure: disk_pressure,
                last_applied_index: last_applied_index,
                last_released_cursor_index: last_released_cursor_index,
                hot_cache_max_value_size: 64
              }
          }

          :ets.insert(ets, {key, "new", 0, 1, :pending, 0, 0})

          meta = %{index: 1, term: 1, system_time: System.os_time(:millisecond)}

          {_state2, {:applied_at, 1, :ok}, effects} =
            StateMachine.apply(meta, {:async, node(), {:put, key, "old", 0}}, state)

          assert [{^key, "new", 0, _lfu, :pending, 0, 0}] = :ets.lookup(ets, key)
          assert {:ok, [{^key, old_offset, 3, 0, false}]} = NIF.v2_scan_file(active_file_path)
          assert {:ok, "old"} = NIF.v2_pread_at(active_file_path, old_offset)
          assert :atomics.get(checkpoint_flags, shard_index + 1) == 1
          assert :atomics.get(last_released_cursor_index, shard_index + 1) == 0

          refute Enum.any?(effects, &match?({:release_cursor, 1}, &1))
        end

        test "replays origin RMW when recovery has no local value", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          {_state2, {:ok, 1}} =
            StateMachine.apply(%{}, {:async, node(), {:incr, "missing_origin_incr", 1}}, state)

          assert [{"missing_origin_incr", "1", 0, _lfu, 0, 0, 1}] =
                   :ets.lookup(ets, "missing_origin_incr")

          assert {:ok, [{"missing_origin_incr", 0, 1, 0, false}]} =
                   NIF.v2_scan_file(active_file_path)
        end

        test "replays origin RMW when recovery has the old pre-command value", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          :ets.insert(ets, {"old_origin_incr", "1", 0, 1, 0, 0, 1})

          {_state2, {:ok, 2}} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "old_origin_incr", {:incr, "old_origin_incr", 1}, "2", 0}},
              state
            )

          assert [{"old_origin_incr", "2", 0, _lfu, 0, _off, 1}] =
                   :ets.lookup(ets, "old_origin_incr")

          assert {:ok, records} = NIF.v2_scan_file(active_file_path)
          assert {"old_origin_incr", _offset, 1, 0, false} = List.last(records)
        end

        test "replays origin APPEND when cold location changes during apply read", %{
          state: state,
          ets: ets
        } do
          key = "old_origin_append_cold_retry"
          value = :binary.copy("r", 70_000)
          suffix = "tail"
          expected = value <> suffix
          test_pid = self()

          {state, :ok} = StateMachine.apply(%{}, {:put, key, value, 0}, state)

          assert [{^key, nil, 0, lfu, file_id, _offset, value_size} = live_entry] =
                   :ets.lookup(ets, key)

          :ets.insert(ets, {key, nil, 0, lfu, file_id + 10_000, 0, value_size})

          Process.put(:ferricstore_state_machine_cold_location_miss_hook, fn ->
            send(test_pid, :state_machine_cold_location_retry_hook)
            :ets.insert(ets, live_entry)
          end)

          try do
            {state, {:ok, expected_size}} =
              StateMachine.apply(
                %{},
                {:async, node(),
                 {:origin_checked, key, {:append, key, suffix}, value, 0, expected, 0}},
                state
              )

            assert expected_size == byte_size(expected)
            assert_receive :state_machine_cold_location_retry_hook, 500
            assert [{^key, nil, 0, _lfu, fid, off, ^expected_size}] = :ets.lookup(ets, key)
            assert {:ok, ^expected} = NIF.v2_pread_at(state.active_file_path, off)
            assert fid == state.active_file_id
          after
            Process.delete(:ferricstore_state_machine_cold_location_miss_hook)
          end
        end

        test "replays origin GETSET when recovery has the pre-command value", %{
          state: state,
          ets: ets
        } do
          :ets.insert(ets, {"old_origin_getset", "old", 0, 1, 0, 0, 3})

          {_state2, "old"} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "old_origin_getset", {:getset, "old_origin_getset", "new"},
                "old", 0, "new", 0}},
              state
            )

          assert [{"old_origin_getset", "new", 0, _lfu, 0, _off, 3}] =
                   :ets.lookup(ets, "old_origin_getset")
        end

        test "replays origin GETSET over an unaccepted pending local value", %{
          state: state,
          ets: ets
        } do
          :ets.insert(ets, {"future_origin_getset", "future", 0, 1, :pending, 0, 0})

          {_state2, "old"} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "future_origin_getset", {:getset, "future_origin_getset", "new"},
                "old", 0, "new", 0}},
              state
            )

          assert [{"future_origin_getset", "new", 0, _lfu, 0, _off, 3}] =
                   :ets.lookup(ets, "future_origin_getset")
        end

        test "does not replay origin GETSET over a durable newer local value", %{
          state: state,
          ets: ets
        } do
          :ets.insert(ets, {"durable_future_getset", "future", 0, 1, 0, 0, 6})

          {_state2, :ok} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "durable_future_getset",
                {:getset, "durable_future_getset", "new"}, "old", 0, "new", 0}},
              state
            )

          assert [{"durable_future_getset", "future", 0, _lfu, 0, 0, 6}] =
                   :ets.lookup(ets, "durable_future_getset")
        end

        test "does not replay stale origin PUT over a durable newer local value", %{
          state: state,
          ets: ets
        } do
          :ets.insert(ets, {"durable_future_put", "future", 0, 1, 0, 0, 6})

          {_state2, :ok} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "durable_future_put", {:put, "durable_future_put", "old", 0},
                nil, 0, "old", 0}},
              state
            )

          assert [{"durable_future_put", "future", 0, _lfu, 0, 0, 6}] =
                   :ets.lookup(ets, "durable_future_put")
        end

        test "does not replay stale origin DELETE over a durable newer local value", %{
          state: state,
          ets: ets
        } do
          :ets.insert(ets, {"durable_future_delete", "future", 0, 1, 0, 0, 6})

          {_state2, :ok} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "durable_future_delete", {:delete, "durable_future_delete"},
                "old", 0, nil, 0}},
              state
            )

          assert [{"durable_future_delete", "future", 0, _lfu, 0, 0, 6}] =
                   :ets.lookup(ets, "durable_future_delete")
        end

        test "does not replay stale origin GETDEL over a durable newer local value", %{
          state: state,
          ets: ets
        } do
          :ets.insert(ets, {"durable_future_getdel", "future", 0, 1, 0, 0, 6})

          {_state2, :ok} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "durable_future_getdel", {:getdel, "durable_future_getdel"},
                "old", 0, nil, 0}},
              state
            )

          assert [{"durable_future_getdel", "future", 0, _lfu, 0, 0, 6}] =
                   :ets.lookup(ets, "durable_future_getdel")
        end

        test "does not replay stale origin PUT over a pending newer local value", %{
          state: state,
          ets: ets
        } do
          :ets.insert(ets, {"pending_future_put", "future", 0, 1, :pending, 0, 0})

          {_state2, :ok} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "pending_future_put", {:put, "pending_future_put", "old", 0},
                nil, 0, "old", 0}},
              state
            )

          assert [{"pending_future_put", "future", 0, _lfu, :pending, 0, 0}] =
                   :ets.lookup(ets, "pending_future_put")
        end

        test "does not replay stale origin DELETE over a pending newer local value", %{
          state: state,
          ets: ets
        } do
          :ets.insert(ets, {"pending_future_delete", "future", 0, 1, :pending, 0, 0})

          {_state2, :ok} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "pending_future_delete", {:delete, "pending_future_delete"},
                "old", 0, nil, 0}},
              state
            )

          assert [{"pending_future_delete", "future", 0, _lfu, :pending, 0, 0}] =
                   :ets.lookup(ets, "pending_future_delete")
        end

        test "does not replay stale origin GETDEL over a pending newer local value", %{
          state: state,
          ets: ets
        } do
          :ets.insert(ets, {"pending_future_getdel", "future", 0, 1, :pending, 0, 0})

          {_state2, :ok} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "pending_future_getdel", {:getdel, "pending_future_getdel"},
                "old", 0, nil, 0}},
              state
            )

          assert [{"pending_future_getdel", "future", 0, _lfu, :pending, 0, 0}] =
                   :ets.lookup(ets, "pending_future_getdel")
        end

        test "replays origin GETDEL tombstone when local delete already removed the key", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          {_state2, :ok} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "origin_getdel_tombstone", {:getdel, "origin_getdel_tombstone"},
                "old", 0, nil, 0}},
              state
            )

          assert [] == :ets.lookup(ets, "origin_getdel_tombstone")

          assert {:ok, [{"origin_getdel_tombstone", _offset, _record_size, 0, true}]} =
                   NIF.v2_scan_file(active_file_path)
        end

        test "materializes pending origin GETSET even when value equals expected", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          :ets.insert(ets, {"pending_origin_getset", "new", 0, 1, :pending, 0, 0})

          {_state2, "old"} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "pending_origin_getset",
                {:getset, "pending_origin_getset", "new"}, "old", 0, "new", 0}},
              state
            )

          assert [{"pending_origin_getset", "new", 0, _lfu, 0, 0, 3}] =
                   :ets.lookup(ets, "pending_origin_getset")

          assert {:ok, [{"pending_origin_getset", 0, 3, 0, false}]} =
                   NIF.v2_scan_file(active_file_path)
        end

        test "does not replay origin INCR over a provably newer pending local value", %{
          state: state,
          ets: ets
        } do
          :ets.insert(ets, {"pending_origin_incr_newer", "10", 0, 1, :pending, 0, 0})

          {_state2, :ok} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "pending_origin_incr_newer",
                {:incr, "pending_origin_incr_newer", 1}, "4", 0, "5", 0}},
              state
            )

          assert [{"pending_origin_incr_newer", "10", 0, _lfu, :pending, 0, 0}] =
                   :ets.lookup(ets, "pending_origin_incr_newer")
        end

        test "does not replay origin DECR over a provably newer pending local value", %{
          state: state,
          ets: ets
        } do
          :ets.insert(ets, {"pending_origin_decr_newer", "-10", 0, 1, :pending, 0, 0})

          {_state2, :ok} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "pending_origin_decr_newer",
                {:incr, "pending_origin_decr_newer", -1}, "-4", 0, "-5", 0}},
              state
            )

          assert [{"pending_origin_decr_newer", "-10", 0, _lfu, :pending, 0, 0}] =
                   :ets.lookup(ets, "pending_origin_decr_newer")
        end

        test "replays origin GETEX when recovery has the old expiry", %{state: state, ets: ets} do
          old_expire_at_ms = Ferricstore.HLC.now_ms() + 10_000
          new_expire_at_ms = old_expire_at_ms + 10_000

          :ets.insert(ets, {"old_origin_getex", "value", old_expire_at_ms, 1, 0, 0, 5})

          {_state2, "value"} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "old_origin_getex",
                {:getex, "old_origin_getex", new_expire_at_ms}, "value", old_expire_at_ms,
                "value", new_expire_at_ms}},
              state
            )

          assert [{"old_origin_getex", "value", ^new_expire_at_ms, _lfu, 0, _off, 5}] =
                   :ets.lookup(ets, "old_origin_getex")
        end

        test "replays origin SETRANGE when recovery has the pre-command value", %{
          state: state,
          ets: ets
        } do
          :ets.insert(ets, {"old_origin_setrange", "hello", 0, 1, 0, 0, 5})

          {_state2, {:ok, 5}} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "old_origin_setrange",
                {:setrange, "old_origin_setrange", 2, "X"}, "hello", 0, "heXlo", 0}},
              state
            )

          assert [{"old_origin_setrange", "heXlo", 0, _lfu, 0, _off, 5}] =
                   :ets.lookup(ets, "old_origin_setrange")
        end

        test "replays origin SETRANGE expected value over pending local value", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          :ets.insert(ets, {"pending_origin_setrange_newer", "heXlo!", 0, 1, :pending, 0, 0})

          {_state2, {:ok, 5}} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, "pending_origin_setrange_newer",
                {:setrange, "pending_origin_setrange_newer", 2, "X"}, "hello", 0, "heXlo", 0}},
              state
            )

          assert [{"pending_origin_setrange_newer", "heXlo", 0, _lfu, 0, 0, 5}] =
                   :ets.lookup(ets, "pending_origin_setrange_newer")

          assert {:ok, [{"pending_origin_setrange_newer", 0, 5, 0, false}]} =
                   NIF.v2_scan_file(active_file_path)
        end

        test "replays origin checked large APPEND over an older cold value", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          key = "old_origin_large_append"
          old_value = String.duplicate("o", 70_000)
          suffix = String.duplicate("n", 2048)
          expected_value = old_value <> suffix

          {:ok, [{old_offset, _old_record_size}]} =
            NIF.v2_append_batch(active_file_path, [{key, old_value, 0}])

          :ets.insert(ets, {key, nil, 0, 1, 0, old_offset, byte_size(old_value)})

          {_state2, {:ok, expected_size}} =
            StateMachine.apply(
              %{},
              {:async, node(),
               {:origin_checked, key, {:append, key, suffix}, old_value, 0, expected_value, 0}},
              state
            )

          assert expected_size == byte_size(expected_value)
          assert [{^key, nil, 0, _lfu, 0, new_offset, value_size}] = :ets.lookup(ets, key)
          assert value_size == byte_size(expected_value)
          refute new_offset == old_offset
          assert {:ok, ^expected_value} = NIF.v2_pread_at(active_file_path, new_offset)
        end

        test "replays origin async DELETE when recovery still has an older value", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          {:ok, [{old_offset, old_size}]} =
            NIF.v2_append_batch(active_file_path, [{"origin_delete", "old", 0}])

          :ets.insert(ets, {"origin_delete", nil, 0, 1, 0, old_offset, old_size})

          {_state2, :ok} =
            StateMachine.apply(%{}, {:async, node(), {:delete, "origin_delete"}}, state)

          assert [] == :ets.lookup(ets, "origin_delete")
          assert {:ok, records} = NIF.v2_scan_file(active_file_path)

          assert Enum.any?(records, fn {"origin_delete", _off, _size, _exp, tombstone?} ->
                   tombstone?
                 end)
        end

        test "DELETE aborts when pending PUT cannot be flushed before tombstone", %{
          state: state,
          ets: ets,
          dir: dir,
          shard_index: shard_index,
          writer_pid: writer_pid,
          active_file_path: active_file_path
        } do
          key = "pending_delete_ordering"
          missing_path = Path.join([dir, "missing_parent", "00000.log"])
          :ets.insert(ets, {key, "value", 0, 1, :pending, 0, 0})

          :sys.replace_state(writer_pid, fn writer_state ->
            %{
              writer_state
              | pending: [{:write, nil, missing_path, 0, ets, key, "value", 0}],
                pending_count: 1
            }
          end)

          {_state2, {:error, {:bitcask_writer_flush_failed, {:flush_failed, 1}}}} =
            StateMachine.apply(%{}, {:delete, key}, state)

          assert [{^key, "value", 0, _lfu, :pending, 0, 0}] = :ets.lookup(ets, key)
          assert {:ok, records} = NIF.v2_scan_file(active_file_path)

          refute Enum.any?(records, fn {record_key, _off, _size, _exp, tombstone?} ->
                   record_key == key and tombstone?
                 end)

          assert shard_index == state.shard_index
        end

        test "origin async DELETE persists tombstone even when Router already removed ETS", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          assert [] == :ets.lookup(ets, "origin_delete_missing_ets")

          {_state2, :ok} =
            StateMachine.apply(
              %{},
              {:async, node(), {:delete, "origin_delete_missing_ets"}},
              state
            )

          assert [] == :ets.lookup(ets, "origin_delete_missing_ets")
          assert {:ok, records} = NIF.v2_scan_file(active_file_path)

          assert Enum.any?(records, fn {"origin_delete_missing_ets", _off, _size, _exp,
                                        tombstone?} ->
                   tombstone?
                 end)
        end

        test "replays origin async GETDEL when recovery still has an older value", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          {:ok, [{old_offset, old_size}]} =
            NIF.v2_append_batch(active_file_path, [{"origin_getdel", "old", 0}])

          :ets.insert(ets, {"origin_getdel", nil, 0, 1, 0, old_offset, old_size})

          {_state2, "old"} =
            StateMachine.apply(%{}, {:async, node(), {:getdel, "origin_getdel"}}, state)

          assert [] == :ets.lookup(ets, "origin_getdel")
          assert {:ok, records} = NIF.v2_scan_file(active_file_path)

          assert Enum.any?(records, fn {"origin_getdel", _off, _size, _exp, tombstone?} ->
                   tombstone?
                 end)
        end

        test "origin async GETDEL persists tombstone even when Router already removed ETS", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          assert [] == :ets.lookup(ets, "origin_getdel_missing_ets")

          {_state2, nil} =
            StateMachine.apply(
              %{},
              {:async, node(), {:getdel, "origin_getdel_missing_ets"}},
              state
            )

          assert [] == :ets.lookup(ets, "origin_getdel_missing_ets")
          assert {:ok, records} = NIF.v2_scan_file(active_file_path)

          assert Enum.any?(records, fn {"origin_getdel_missing_ets", _off, _size, _exp,
                                        tombstone?} ->
                   tombstone?
                 end)
        end

        test "GETDEL returns flush error when pending PUT cannot be ordered before tombstone", %{
          state: state,
          ets: ets,
          dir: dir,
          writer_pid: writer_pid,
          active_file_path: active_file_path
        } do
          key = "pending_getdel_ordering"
          missing_path = Path.join([dir, "missing_parent", "00000.log"])
          :ets.insert(ets, {key, "value", 0, 1, :pending, 0, 0})

          :sys.replace_state(writer_pid, fn writer_state ->
            %{
              writer_state
              | pending: [{:write, nil, missing_path, 0, ets, key, "value", 0}],
                pending_count: 1
            }
          end)

          {_state2, {:error, {:bitcask_writer_flush_failed, {:flush_failed, 1}}}} =
            StateMachine.apply(%{}, {:getdel, key}, state)

          assert [{^key, "value", 0, _lfu, :pending, 0, 0}] = :ets.lookup(ets, key)
          assert {:ok, records} = NIF.v2_scan_file(active_file_path)

          refute Enum.any?(records, fn {record_key, _off, _size, _exp, tombstone?} ->
                   record_key == key and tombstone?
                 end)
        end
      end
    end
  end
end
