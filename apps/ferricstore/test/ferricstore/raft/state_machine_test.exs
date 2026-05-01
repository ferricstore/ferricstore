defmodule Ferricstore.Raft.StateMachineTest do
  @moduledoc """
  Unit tests for `Ferricstore.Raft.StateMachine`.

  These tests exercise the state machine callbacks directly without running
  a full ra server. The state machine is deterministic and its callbacks can
  be tested in isolation by constructing state manually.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.Store.BitcaskWriter

  # ---------------------------------------------------------------------------
  # Setup: create a temporary Bitcask store and ETS table for each test.
  # Also starts a BitcaskWriter for shard 0 so that background writes from
  # StateMachine.apply work in isolation tests.
  # ---------------------------------------------------------------------------

  setup do
    dir = Path.join(System.tmp_dir!(), "sm_test_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)

    # v2: create a .log file instead of NIF.new
    active_file_path = Path.join(dir, "00000.log")
    File.touch!(active_file_path)

    suffix = :rand.uniform(9_999_999)
    keydir_name = :"sm_test_keydir_#{suffix}"
    :ets.new(keydir_name, [:set, :public, :named_table])

    # Use a unique shard index to avoid name conflicts with other test processes.
    shard_index = 9000 + :rand.uniform(999)

    state =
      StateMachine.init(%{
        shard_index: shard_index,
        shard_data_path: dir,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir_name
      })

    # Start a BitcaskWriter for this shard so deferred writes are processed.
    {:ok, writer_pid} = BitcaskWriter.start_link(shard_index: shard_index)

    on_exit(fn ->
      try do
        if Process.alive?(writer_pid), do: GenServer.stop(writer_pid, :normal, 5000)
      rescue
        _ -> :ok
      catch
        :exit, _ -> :ok
      end

      try do
        :ets.delete(keydir_name)
      rescue
        ArgumentError -> :ok
      end

      File.rm_rf!(dir)
    end)

    %{
      state: state,
      ets: keydir_name,
      store: nil,
      dir: dir,
      active_file_path: active_file_path,
      shard_index: shard_index
    }
  end

  # ---------------------------------------------------------------------------
  # init/1
  # ---------------------------------------------------------------------------

  describe "init/1" do
    test "creates initial state with expected fields", %{state: state, shard_index: shard_index} do
      assert state.shard_index == shard_index
      assert is_binary(state.shard_data_path)
      assert is_binary(state.active_file_path)
      assert is_atom(state.ets)
      assert state.applied_count == 0
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

    test "does not persist a stale origin PUT after local RMW changed the pending ETS value", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      # Router may apply a later async RMW locally before the earlier async PUT
      # reaches StateMachine.apply/3. The origin replay must not write the old
      # command value over the newer local value in Bitcask recovery order.
      :ets.insert(ets, {"stale_origin_put", "new", 0, 1, :pending, 0, 0})

      {_state2, :ok} =
        StateMachine.apply(%{}, {:async, node(), {:put, "stale_origin_put", "old", 0}}, state)

      assert {:ok, []} = NIF.v2_scan_file(active_file_path)
    end
  end

  # ---------------------------------------------------------------------------
  # apply/3 with :put
  # ---------------------------------------------------------------------------

  describe "apply/3 with {:put, key, value, expire_at_ms}" do
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
      assert [{"hlc_time_locked", "value", 0, _, _, _, _}] = :ets.lookup(ets, "hlc_time_locked")
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
          {{:cross_shard_tx, [{shard_index, [{"SETEX", ["stamped_setex", "5", "value"]}], nil}]},
           %{hlc_ts: {stamped_now, 0}}},
          state
        )

      expected_expire_at_ms = stamped_now + 5_000

      assert [{"stamped_setex", "value", ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "stamped_setex")
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
        {"stamped_pexpire", "value", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("value")}
      )

      {_new_state, %{^shard_index => [1]}} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:cross_shard_tx, [{shard_index, [{"PEXPIRE", ["stamped_pexpire", "5000"]}], nil}]},
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

    test "cross-shard PTTL reads cold metadata from valid file id zero", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path,
      shard_index: shard_index
    } do
      now = Ferricstore.HLC.now_ms()
      expire_at_ms = now + 5_000

      {:ok, {offset, value_size}} =
        NIF.v2_append_record(active_file_path, "cross_cold_meta_fid0", "cold-meta", expire_at_ms)

      :ets.insert(
        ets,
        {"cross_cold_meta_fid0", nil, expire_at_ms, Ferricstore.Store.LFU.initial(), 0, offset,
         value_size}
      )

      {_new_state, %{^shard_index => [5_000]}} =
        StateMachine.apply(
          %{system_time: now},
          {:cross_shard_tx, [{shard_index, [{"PTTL", ["cross_cold_meta_fid0"]}], nil}]},
          state
        )
    end

    test "stamped ratelimit ignores legacy embedded now_ms", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      stamped_now = local_now - 30_000
      embedded_now = local_now + 30_000
      window_ms = 10_000

      {_new_state, ["allowed", 1, 9, ^window_ms]} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:ratelimit_add, "stamped_ratelimit", window_ms, 10, 1, embedded_now},
           %{hlc_ts: {stamped_now, 0}}},
          state
        )

      expected_expire_at_ms = stamped_now + window_ms * 2

      assert [{"stamped_ratelimit", encoded, ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "stamped_ratelimit")

      assert {1, ^stamped_now, 0} = Ferricstore.Store.ValueCodec.decode_ratelimit(encoded)
    end

    test "stamped batch ratelimit ignores legacy embedded now_ms", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      stamped_now = local_now - 30_000
      embedded_now = local_now + 30_000
      window_ms = 10_000

      {_new_state, {:ok, [["allowed", 1, 9, ^window_ms]]}} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:batch,
            [{:ratelimit_add, "batch_stamped_ratelimit", window_ms, 10, 1, embedded_now}]},
           %{hlc_ts: {stamped_now, 0}}},
          state
        )

      expected_expire_at_ms = stamped_now + window_ms * 2

      assert [{"batch_stamped_ratelimit", encoded, ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "batch_stamped_ratelimit")

      assert {1, ^stamped_now, 0} = Ferricstore.Store.ValueCodec.decode_ratelimit(encoded)
    end

    test "legacy unwrapped ratelimit keeps embedded now_ms for replay compatibility", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      embedded_now = local_now - 30_000
      window_ms = 10_000

      {_new_state, ["allowed", 1, 9, ^window_ms]} =
        StateMachine.apply(
          %{},
          {:ratelimit_add, "legacy_ratelimit", window_ms, 10, 1, embedded_now},
          state
        )

      expected_expire_at_ms = embedded_now + window_ms * 2

      assert [{"legacy_ratelimit", encoded, ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "legacy_ratelimit")

      assert {1, ^embedded_now, 0} = Ferricstore.Store.ValueCodec.decode_ratelimit(encoded)
    end

    test "uses raft meta system_time when acquiring cross-shard locks", %{state: state} do
      local_now = Ferricstore.HLC.now_ms()
      apply_now = local_now - 20_000
      existing_lock_expiry = apply_now + 10_000
      existing_owner = make_ref()

      locked_state = %{
        state
        | cross_shard_locks: %{
            "meta_time_lock_conflict" => {existing_owner, existing_lock_expiry}
          }
      }

      {new_state, result} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:lock_keys, ["meta_time_lock_conflict"], make_ref(), apply_now + 30_000},
          locked_state
        )

      assert {:error, :keys_locked} = result

      assert %{"meta_time_lock_conflict" => {^existing_owner, ^existing_lock_expiry}} =
               new_state.cross_shard_locks
    end

    test "uses raft meta system_time for standalone read-modify-write TTL checks", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      apply_now = local_now - 20_000
      expires_after_apply_time = apply_now + 10_000

      :ets.insert(
        ets,
        {"meta_time_incr", "5", expires_after_apply_time, Ferricstore.Store.LFU.initial(), 0, 0,
         byte_size("5")}
      )

      {_new_state, {:ok, 6}} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:incr, "meta_time_incr", 1},
          state
        )

      assert [{"meta_time_incr", "6", ^expires_after_apply_time, _, _, _, _}] =
               :ets.lookup(ets, "meta_time_incr")
    end

    test "missing active file fails put and rolls back new key", %{
      state: state,
      ets: ets
    } do
      missing_state = state_with_missing_active_file(state)

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "missing_active_new", "value", 0}, missing_state)

      assert {:error, :active_file_unavailable} = result
      assert [] == :ets.lookup(ets, "missing_active_new")
    end

    test "missing active file fails overwrite and restores old ETS entry", %{
      state: state,
      ets: ets
    } do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "missing_active_existing", "old", 0}, state)
      old_entry = :ets.lookup(ets, "missing_active_existing")
      missing_state = state_with_missing_active_file(state2)

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "missing_active_existing", "new", 0}, missing_state)

      assert {:error, :active_file_unavailable} = result
      assert old_entry == :ets.lookup(ets, "missing_active_existing")
    end

    test "missing state active file falls back to live ActiveFile registry", %{
      state: state,
      ets: ets,
      dir: dir,
      shard_index: shard_index
    } do
      file_id = 8_000_000 + :erlang.unique_integer([:positive])
      live_path = Path.join(dir, "#{file_id}.log")
      File.touch!(live_path)
      Ferricstore.Store.ActiveFile.publish(shard_index, file_id, live_path, dir)

      missing_state = state_with_missing_active_file(state, shard_index: shard_index)

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "missing_active_fallback", "value", 0}, missing_state)

      assert :ok = result

      assert [{"missing_active_fallback", "value", 0, _, ^file_id, offset, value_size}] =
               :ets.lookup(ets, "missing_active_fallback")

      assert is_integer(offset)
      assert value_size > 0
    end

    test "uses live ActiveFile registry when state active file is stale but still exists", %{
      state: state,
      ets: ets,
      dir: dir,
      shard_index: shard_index
    } do
      live_file_id = 8_100_000 + :erlang.unique_integer([:positive])
      live_path = Path.join(dir, "#{live_file_id}.log")
      File.touch!(live_path)
      Ferricstore.Store.ActiveFile.publish(shard_index, live_file_id, live_path, dir)

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "stale_active_registry", "value", 0}, state)

      assert :ok = result

      assert [{"stale_active_registry", "value", 0, _, ^live_file_id, offset, value_size}] =
               :ets.lookup(ets, "stale_active_registry")

      assert is_integer(offset)
      assert value_size > 0
      assert {:ok, "value"} = NIF.v2_pread_at(live_path, offset)
    end

    test "Bitcask append errors fail quorum apply and roll back pending ETS", %{
      state: state,
      ets: ets
    } do
      file_id = 9_000_000 + :erlang.unique_integer([:positive])
      bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
      File.mkdir_p!(bad_active_path)

      bad_state = %{state | active_file_id: file_id, active_file_path: bad_active_path}

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "append_error_key", "value", 0}, bad_state)

      assert {:error, {:bitcask_append_failed, _reason}} = result
      assert [] == :ets.lookup(ets, "append_error_key")
    end

    test "batch Bitcask append errors restore deleted ETS entries and remove new puts", %{
      state: state,
      ets: ets
    } do
      {state2, :ok} =
        StateMachine.apply(%{}, {:put, "delete_failure_existing", "old_value", 0}, state)

      old_entry = :ets.lookup(ets, "delete_failure_existing")

      file_id = 9_100_000 + :erlang.unique_integer([:positive])
      bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
      File.mkdir_p!(bad_active_path)
      bad_state = %{state2 | active_file_id: file_id, active_file_path: bad_active_path}

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:batch,
           [
             {:delete, "delete_failure_existing"},
             {:put, "delete_failure_new", "new_value", 0}
           ]},
          bad_state
        )

      assert {:error, {:bitcask_append_failed, _reason}} = result
      assert old_entry == :ets.lookup(ets, "delete_failure_existing")
      assert [] == :ets.lookup(ets, "delete_failure_new")
    end

    test "emits bounded apply and Bitcask append telemetry", %{
      state: state,
      shard_index: shard_index
    } do
      handler_id = {:state_machine_quorum_telemetry, self(), make_ref()}

      :ok =
        :telemetry.attach_many(
          handler_id,
          [
            [:ferricstore, :raft, :apply],
            [:ferricstore, :bitcask, :append]
          ],
          fn event, measurements, metadata, test_pid ->
            send(test_pid, {:quorum_telemetry, event, measurements, metadata})
          end,
          self()
        )

      try do
        {_new_state, :ok} = StateMachine.apply(%{}, {:put, "telemetry_key", "value", 0}, state)

        assert_receive {:quorum_telemetry, [:ferricstore, :bitcask, :append], append_meas,
                        %{shard_index: ^shard_index, status: :ok}},
                       500

        assert append_meas.batch_size == 1
        assert append_meas.batch_bytes > 0
        assert is_integer(append_meas.duration_us)

        assert_receive {:quorum_telemetry, [:ferricstore, :raft, :apply], apply_meas,
                        %{shard_index: ^shard_index, result: :ok, disk: :ok}},
                       500

        assert is_integer(apply_meas.duration_us)
      after
        :telemetry.detach(handler_id)
      end
    end

    test "writes value to disk and ETS", %{state: state, ets: ets, shard_index: shard_index} do
      {new_state, result} =
        StateMachine.apply(%{}, {:put, "key1", "value1", 0}, state)

      assert result == :ok
      assert new_state.applied_count == 1

      # Verify ETS (v2 7-tuple format) — value is available immediately
      assert [{"key1", "value1", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, "key1")

      # Flush background writer so disk location is materialized
      BitcaskWriter.flush(shard_index)

      # Verify disk via pread
      [{_, _, _, _, fid, off, _}] = :ets.lookup(ets, "key1")
      assert is_integer(fid)

      log_path =
        Path.join(
          state.shard_data_path,
          "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log"
        )

      assert {:ok, "value1"} = NIF.v2_pread_at(log_path, off)
    end

    test "put with expiry stores expire_at_ms", %{state: state, ets: ets} do
      future = System.os_time(:millisecond) + 60_000

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "expiring", "val", future}, state)

      assert result == :ok

      assert [{"expiring", "val", ^future, _lfu, _fid, _off, _vsize}] =
               :ets.lookup(ets, "expiring")
    end

    test "put overwrites previous value", %{state: state, ets: ets} do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "k", "v1", 0}, state)
      {state3, :ok} = StateMachine.apply(%{}, {:put, "k", "v2", 0}, state2)

      assert state3.applied_count == 2
      assert [{"k", "v2", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, "k")
    end

    test "increments applied_count on each put", %{state: state} do
      {s1, :ok} = StateMachine.apply(%{}, {:put, "a", "1", 0}, state)
      {s2, :ok} = StateMachine.apply(%{}, {:put, "b", "2", 0}, s1)
      {s3, :ok} = StateMachine.apply(%{}, {:put, "c", "3", 0}, s2)

      assert s3.applied_count == 3
    end
  end

  # ---------------------------------------------------------------------------
  # apply/3 with :delete
  # ---------------------------------------------------------------------------

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

    test "missing active file fails delete and keeps ETS entry", %{state: state, ets: ets} do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "missing_active_delete", "val", 0}, state)
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
      prob_path = Path.join(prob_dir, "#{Base.url_encode64(key, padding: false)}.cms")
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
    test "uses raft meta system_time for TTL checks inside batch read-modify-write", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      apply_now = local_now - 20_000
      expires_after_apply_time = apply_now + 10_000

      :ets.insert(
        ets,
        {"batch_meta_time_incr", "5", expires_after_apply_time, Ferricstore.Store.LFU.initial(),
         0, 0, byte_size("5")}
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

  describe "handle_aux/5" do
    test "key_written increments hot key counter" do
      aux = %{hot_keys: %{}}
      int_state = %{some: :internal_state}

      {:no_reply, new_aux, returned_state} =
        StateMachine.handle_aux(:leader, :cast, {:key_written, "hot_key"}, aux, int_state)

      assert new_aux.hot_keys["hot_key"] == 1
      assert returned_state == int_state
    end

    test "key_written accumulates counts" do
      aux = %{hot_keys: %{"hot_key" => 5}}
      int_state = %{}

      {:no_reply, new_aux, _} =
        StateMachine.handle_aux(:leader, :cast, {:key_written, "hot_key"}, aux, int_state)

      assert new_aux.hot_keys["hot_key"] == 6
    end

    test "unknown command returns aux unchanged" do
      aux = %{hot_keys: %{}}
      int_state = %{}

      {:no_reply, returned_aux, returned_state} =
        StateMachine.handle_aux(:leader, :cast, :unknown, aux, int_state)

      assert returned_aux == aux
      assert returned_state == int_state
    end
  end

  # ---------------------------------------------------------------------------
  # overview/1
  # ---------------------------------------------------------------------------

  describe "overview/1" do
    test "returns shard_index, keydir_size, and applied_count", %{
      state: state,
      shard_index: shard_index
    } do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "ov_k", "ov_v", 0}, state)

      overview = StateMachine.overview(state2)
      assert overview.shard_index == shard_index
      assert overview.keydir_size == 1
      assert overview.applied_count == 1
    end

    test "keydir_size reflects ETS size", %{state: state} do
      {s1, :ok} = StateMachine.apply(%{}, {:put, "a", "1", 0}, state)
      {s2, :ok} = StateMachine.apply(%{}, {:put, "b", "2", 0}, s1)
      {s3, :ok} = StateMachine.apply(%{}, {:put, "c", "3", 0}, s2)

      assert StateMachine.overview(s3).keydir_size == 3
    end
  end

  # ---------------------------------------------------------------------------
  # release_cursor for Raft log compaction (spec 2E.5)
  # ---------------------------------------------------------------------------

  describe "release_cursor log compaction" do
    test "init/1 stores release_cursor_interval from config", %{store: _store, ets: ets} do
      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: System.tmp_dir!(),
          active_file_id: 0,
          active_file_path: Path.join(System.tmp_dir!(), "00000.log"),
          ets: ets
        })

      assert is_integer(state.release_cursor_interval)
      assert state.release_cursor_interval > 0
    end

    test "init/1 accepts custom release_cursor_interval", %{store: _store, ets: ets} do
      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: System.tmp_dir!(),
          active_file_id: 0,
          active_file_path: Path.join(System.tmp_dir!(), "00000.log"),
          ets: ets,
          release_cursor_interval: 500
        })

      assert state.release_cursor_interval == 500
    end

    test "no release_cursor emitted before interval is reached", %{store: _store, ets: ets} do
      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: System.tmp_dir!(),
          active_file_id: 0,
          active_file_path: Path.join(System.tmp_dir!(), "00000.log"),
          ets: ets,
          release_cursor_interval: 5
        })

      # Apply 4 commands (below interval of 5) -- none should emit release_cursor
      result =
        Enum.reduce(1..4, state, fn i, acc ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, :ok}, effects} =
            StateMachine.apply(meta, {:put, "rc_key_#{i}", "v#{i}", 0}, acc)

          if Enum.any?(effects, &match?({:release_cursor, _, _}, &1)) do
            flunk("release_cursor emitted before interval reached at apply #{i}")
          end

          new_state
        end)

      assert result.applied_count == 4
    end

    test "release_cursor emitted exactly at interval boundary for put", %{store: _store, ets: ets} do
      interval = 5

      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: System.tmp_dir!(),
          active_file_id: 0,
          active_file_path: Path.join(System.tmp_dir!(), "00000.log"),
          ets: ets,
          release_cursor_interval: interval
        })

      # Apply (interval - 1) commands without release_cursor
      state_before =
        Enum.reduce(1..(interval - 1), state, fn i, acc ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, :ok}, _effects} =
            StateMachine.apply(meta, {:put, "rc_#{i}", "v#{i}", 0}, acc)

          new_state
        end)

      assert state_before.applied_count == interval - 1

      # The N-th apply (index = interval) should emit release_cursor
      meta = %{index: interval, term: 1, system_time: System.os_time(:millisecond)}

      {new_state, {:applied_at, _, :ok}, effects} =
        StateMachine.apply(meta, {:put, "rc_#{interval}", "v#{interval}", 0}, state_before)

      assert new_state.applied_count == interval

      # Verify the release_cursor effect
      cursor_effect = Enum.find(effects, &match?({:release_cursor, _, _}, &1))
      assert {:release_cursor, ra_index, cursor_state} = cursor_effect
      assert ra_index == interval
      assert cursor_state.shard_index == 0
      assert cursor_state.applied_count == interval
    end

    test "release_cursor emitted at every interval multiple", %{store: _store, ets: ets} do
      interval = 3

      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: System.tmp_dir!(),
          active_file_id: 0,
          active_file_path: Path.join(System.tmp_dir!(), "00000.log"),
          ets: ets,
          release_cursor_interval: interval
        })

      # Apply 9 commands, expect release_cursor at positions 3, 6, 9
      {_final_state, cursor_indices} =
        Enum.reduce(1..9, {state, []}, fn i, {acc, cursors} ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, :ok}, effects} =
            StateMachine.apply(meta, {:put, "mc_#{i}", "v#{i}", 0}, acc)

          cursor_idx =
            Enum.find_value(effects, fn
              {:release_cursor, idx, _snap} -> idx
              _ -> nil
            end)

          if cursor_idx, do: {new_state, cursors ++ [cursor_idx]}, else: {new_state, cursors}
        end)

      assert cursor_indices == [3, 6, 9]
    end

    test "release_cursor emitted for delete at interval boundary", %{store: _store, ets: ets} do
      interval = 3

      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: System.tmp_dir!(),
          active_file_id: 0,
          active_file_path: Path.join(System.tmp_dir!(), "00000.log"),
          ets: ets,
          release_cursor_interval: interval
        })

      # Put two keys (applied_count = 2), then delete at the 3rd apply
      meta1 = %{index: 10, term: 1, system_time: System.os_time(:millisecond)}

      {s1, {:applied_at, _, :ok}, _e1} =
        StateMachine.apply(meta1, {:put, "del_rc_a", "va", 0}, state)

      meta2 = %{index: 11, term: 1, system_time: System.os_time(:millisecond)}

      {s2, {:applied_at, _, :ok}, _e2} =
        StateMachine.apply(meta2, {:put, "del_rc_b", "vb", 0}, s1)

      # 3rd command is a delete -- should trigger release_cursor
      meta3 = %{index: 12, term: 1, system_time: System.os_time(:millisecond)}
      {_s3, {:applied_at, _, :ok}, effects} = StateMachine.apply(meta3, {:delete, "del_rc_a"}, s2)

      cursor_effect = Enum.find(effects, &match?({:release_cursor, _, _}, &1))
      assert {:release_cursor, 12, _cursor_state} = cursor_effect
    end

    test "release_cursor emitted for batch that crosses interval boundary", %{
      store: _store,
      ets: ets
    } do
      interval = 5

      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: System.tmp_dir!(),
          active_file_id: 0,
          active_file_path: Path.join(System.tmp_dir!(), "00000.log"),
          ets: ets,
          release_cursor_interval: interval
        })

      # Apply 3 single commands (applied_count = 3)
      state_before =
        Enum.reduce(1..3, state, fn i, acc ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, :ok}, _e} =
            StateMachine.apply(meta, {:put, "pre_#{i}", "v#{i}", 0}, acc)

          new_state
        end)

      assert state_before.applied_count == 3

      # Batch of 3 commands takes applied_count from 3 to 6 -- crosses interval at 5
      batch = [
        {:put, "batch_1", "bv1", 0},
        {:put, "batch_2", "bv2", 0},
        {:put, "batch_3", "bv3", 0}
      ]

      meta = %{index: 4, term: 1, system_time: System.os_time(:millisecond)}

      {new_state, {:applied_at, _, {:ok, results}}, effects} =
        StateMachine.apply(meta, {:batch, batch}, state_before)

      assert results == [:ok, :ok, :ok]
      assert new_state.applied_count == 6
      cursor_effect = Enum.find(effects, &match?({:release_cursor, _, _}, &1))
      assert {:release_cursor, 4, _cursor_state} = cursor_effect
    end

    test "release_cursor not emitted when meta has no index", %{state: state} do
      # Use default interval (1000). Even if we manually set applied_count to 999,
      # without an index in meta, release_cursor should not be emitted.
      state_near = %{state | applied_count: 999, release_cursor_interval: 1000}

      # No :index in meta -- simulates unit test / non-ra context
      result = StateMachine.apply(%{}, {:put, "no_idx", "val", 0}, state_near)

      case result do
        {new_state, :ok} ->
          assert new_state.applied_count == 1000

        {_new_state, :ok, _effects} ->
          flunk("release_cursor should not be emitted when meta has no :index key")
      end
    end

    test "release_cursor state snapshot contains correct machine state", %{
      store: _store,
      ets: ets
    } do
      interval = 3

      state =
        StateMachine.init(%{
          shard_index: 2,
          shard_data_path: System.tmp_dir!(),
          active_file_id: 0,
          active_file_path: Path.join(System.tmp_dir!(), "00000.log"),
          ets: ets,
          release_cursor_interval: interval
        })

      # Apply 3 commands to trigger release_cursor
      state_after =
        Enum.reduce(1..2, state, fn i, acc ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, :ok}, _e} =
            StateMachine.apply(meta, {:put, "snap_#{i}", "v#{i}", 0}, acc)

          new_state
        end)

      meta = %{index: 3, term: 1, system_time: System.os_time(:millisecond)}

      {_new_state, {:applied_at, _, :ok}, effects} =
        StateMachine.apply(meta, {:put, "snap_3", "v3", 0}, state_after)

      cursor_effect = Enum.find(effects, &match?({:release_cursor, _, _}, &1))
      assert {:release_cursor, 3, cursor_state} = cursor_effect

      # The snapshot state should reflect the current state
      assert cursor_state.shard_index == 2
      assert cursor_state.applied_count == 3
      assert is_binary(cursor_state.shard_data_path)
      assert cursor_state.ets == ets
      assert cursor_state.release_cursor_interval == interval
    end

    test "overview/1 includes release_cursor_interval", %{state: state} do
      overview = StateMachine.overview(state)
      assert Map.has_key?(overview, :release_cursor_interval)
      assert is_integer(overview.release_cursor_interval)
    end
  end

  defp state_with_missing_active_file(state, opts \\ []) do
    file_id = 9_000_000 + :erlang.unique_integer([:positive])
    shard_index = Keyword.get(opts, :shard_index, 100_000 + :erlang.unique_integer([:positive]))

    %{
      state
      | shard_index: shard_index,
        active_file_id: file_id,
        active_file_path: Path.join(state.shard_data_path, "#{file_id}.log")
    }
  end
end
