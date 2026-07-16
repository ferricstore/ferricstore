defmodule Ferricstore.Raft.HlcRaftIntegrationTest do
  @moduledoc """
  Tests for HLC integration with Raft heartbeats (spec 2G.6).

  Verifies that:

    * The state machine correctly unwraps HLC-stamped commands and merges
      the piggybacked timestamp into the local HLC.
    * Unwrapped (legacy) commands continue to work without HLC merging.
    * The `state_enter(:leader, ...)` callback advances the HLC.
    * The Batcher stamps commands with HLC timestamps before submitting
      to ra.
    * Drift stays near zero in single-node mode (leader == follower).
  """

  use ExUnit.Case, async: false
  @moduletag :raft

  alias Ferricstore.HLC
  alias Ferricstore.Raft.StateMachine

  # ---------------------------------------------------------------------------
  # Setup: create a temporary Bitcask store, ETS tables, and reset HLC
  # ---------------------------------------------------------------------------

  setup do
    dir = Path.join(System.tmp_dir!(), "hlc_raft_test_#{:rand.uniform(9_999_999)}")
    shard_path = Ferricstore.DataDir.shard_data_path(dir, 0)
    File.mkdir_p!(shard_path)

    active_file_path = Path.join(shard_path, "00000.log")
    File.touch!(active_file_path)

    suffix = :rand.uniform(9_999_999)
    keydir_name = :"hlc_raft_keydir_#{suffix}"
    :ets.new(keydir_name, [:set, :public, :named_table])

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        data_dir: dir,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir_name
      })

    # Reset the HLC atomics to a clean slate so tests don't interfere.
    ref = :persistent_term.get(:ferricstore_hlc_ref)
    :atomics.put(ref, 1, 0)

    on_exit(fn ->
      try do
        :ets.delete(keydir_name)
      rescue
        ArgumentError -> :ok
      end

      File.rm_rf(dir)
    end)

    %{
      state: state,
      ets: keydir_name,
      store: nil,
      dir: dir
    }
  end

  # ---------------------------------------------------------------------------
  # HLC-wrapped command processing
  # ---------------------------------------------------------------------------

  describe "apply/3 with HLC-wrapped commands" do
    test "unwraps and processes a put command with hlc_ts metadata", %{
      state: state,
      ets: ets
    } do
      hlc_ts = HLC.now()
      wrapped = {{:put, "hlc_key", "hlc_val", 0}, %{hlc_ts: hlc_ts}}

      {new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == :ok
      assert new_state.applied_count == 1

      # Verify the inner command was processed correctly (v2 7-tuple)
      assert [{"hlc_key", "hlc_val", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, "hlc_key")
    end

    test "unwraps and processes a delete command with hlc_ts metadata", %{
      state: state,
      ets: ets
    } do
      # First, put a key
      {state2, :ok} = StateMachine.apply(%{}, {:put, "del_hlc", "v", 0}, state)

      # Now delete it with an HLC-wrapped command
      hlc_ts = HLC.now()
      wrapped = {{:delete, "del_hlc"}, %{hlc_ts: hlc_ts}}
      {state3, result} = StateMachine.apply(%{}, wrapped, state2)

      assert result == :ok
      assert state3.applied_count == 2
      assert [] == :ets.lookup(ets, "del_hlc")
    end

    test "unwraps and processes a batch command with hlc_ts metadata", %{
      state: state,
      ets: ets
    } do
      hlc_ts = HLC.now()
      batch = [{:put, "b1", "v1", 0}, {:put, "b2", "v2", 0}]
      wrapped = {{:batch, batch}, %{hlc_ts: hlc_ts}}

      {new_state, {:ok, results}} = StateMachine.apply(%{}, wrapped, state)

      assert results == [:ok, :ok]
      assert new_state.applied_count == 2
      assert [{"b1", "v1", 0, _, _, _, _}] = :ets.lookup(ets, "b1")
      assert [{"b2", "v2", 0, _, _, _, _}] = :ets.lookup(ets, "b2")
    end

    test "unwraps and processes an incr_float command with hlc_ts metadata", %{
      state: state
    } do
      hlc_ts = HLC.now()
      wrapped = {{:incr_float, "counter", 10.0}, %{hlc_ts: hlc_ts}}

      {new_state, {:ok, result}} = StateMachine.apply(%{}, wrapped, state)

      assert new_state.applied_count == 1
      assert_in_delta result, 10.0, 0.001
    end

    test "unwraps and processes an append command with hlc_ts metadata", %{
      state: state
    } do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "app_key", "hello", 0}, state)

      hlc_ts = HLC.now()
      wrapped = {{:append, "app_key", " world"}, %{hlc_ts: hlc_ts}}

      {_state3, {:ok, new_len}} = StateMachine.apply(%{}, wrapped, state2)

      assert new_len == 11
    end

    test "legacy unwrapped commands still work (backward compatibility)", %{
      state: state,
      ets: ets
    } do
      # Ensure unwrapped commands (no HLC metadata) continue to function
      {new_state, result} = StateMachine.apply(%{}, {:put, "legacy", "val", 0}, state)

      assert result == :ok
      assert new_state.applied_count == 1
      assert [{"legacy", "val", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, "legacy")
    end
  end

  # ---------------------------------------------------------------------------
  # HLC merging during apply
  # ---------------------------------------------------------------------------

  describe "HLC merging during apply" do
    test "merges a remote HLC timestamp piggybacked on a command", %{state: state} do
      # Record the HLC before the command
      {_before_phys, _before_log} = HLC.now()

      # Create a "remote" timestamp that is 50ms ahead of current wall clock.
      # In multi-node mode, this simulates a leader that is slightly ahead.
      remote_phys = System.os_time(:millisecond) + 50
      remote_ts = {remote_phys, 0}

      wrapped = {{:put, "merge_test", "val", 0}, %{hlc_ts: remote_ts}}
      {_new_state, :ok} = StateMachine.apply(%{}, wrapped, state)

      # After apply, the local HLC should have merged the remote timestamp.
      # The physical component should be at least as large as the remote.
      {after_phys, _after_log} = HLC.now()
      assert after_phys >= remote_phys
    end

    test "HLC merge with a past timestamp does not regress the clock", %{state: state} do
      # Advance the HLC to current time
      {current_phys, _} = HLC.now()

      # Send a command with a timestamp from the past
      remote_ts = {current_phys - 1000, 0}
      wrapped = {{:put, "past_test", "val", 0}, %{hlc_ts: remote_ts}}
      {_new_state, :ok} = StateMachine.apply(%{}, wrapped, state)

      # HLC should not have regressed
      {after_phys, _} = HLC.now()
      assert after_phys >= current_phys
    end

    test "drift stays near zero in single-node mode after multiple applies", %{state: state} do
      # In single-node mode, the HLC timestamp on the command comes from the
      # same node that applies it. Drift should remain near zero.
      state_acc =
        Enum.reduce(1..100, state, fn i, acc ->
          hlc_ts = HLC.now()
          wrapped = {{:put, "drift_key_#{i}", "v#{i}", 0}, %{hlc_ts: hlc_ts}}
          {new_state, :ok} = StateMachine.apply(%{}, wrapped, acc)
          new_state
        end)

      assert state_acc.applied_count == 100

      # Drift should be very small since we are single-node.
      # Allow up to 50ms to avoid CI flakes under load.
      drift = HLC.drift_ms()
      assert drift < 50, "Expected drift < 50ms in single-node mode, got #{drift}ms"
    end

    test "HLC update is called even when the command result is an error", %{state: state} do
      # Write a non-float value
      {state2, :ok} = StateMachine.apply(%{}, {:put, "not_a_float", "abc", 0}, state)

      # Try to incr_float it with an HLC-wrapped command -- should fail but still merge HLC
      remote_phys = System.os_time(:millisecond) + 25
      remote_ts = {remote_phys, 0}
      wrapped = {{:incr_float, "not_a_float", 1.0}, %{hlc_ts: remote_ts}}

      {_state3, {:error, _reason}} = StateMachine.apply(%{}, wrapped, state2)

      # The HLC should still have been merged despite the command error
      {after_phys, _} = HLC.now()
      assert after_phys >= remote_phys
    end
  end

  # ---------------------------------------------------------------------------
  # Release cursor with HLC-wrapped commands
  # ---------------------------------------------------------------------------

  describe "release_cursor with HLC-wrapped commands" do
    test "release_cursor is emitted at interval boundary for wrapped commands", %{
      ets: ets,
      dir: dir
    } do
      interval = 3
      shard_path = Ferricstore.DataDir.shard_data_path(dir, 0)
      instance_name = :"hlc_release_cursor_#{System.unique_integer([:positive])}"
      instance_ctx = FerricStore.Instance.build(instance_name, shard_count: 1, data_dir: dir)

      :atomics.put(instance_ctx.replay_safe_index, 1, 3)
      :atomics.put(instance_ctx.flow_lmdb_replay_safe_index, 1, 3)
      :atomics.put(instance_ctx.flow_history_projected_index, 1, 3)

      on_exit({:hlc_release_cursor_instance, instance_name}, fn ->
        FerricStore.Instance.cleanup(instance_name)
      end)

      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: shard_path,
          data_dir: dir,
          active_file_id: 0,
          active_file_path: Path.join(shard_path, "00000.log"),
          ets: ets,
          release_cursor_interval: interval,
          instance_ctx: instance_ctx
        })

      # Apply 2 wrapped commands (below interval)
      state_before =
        Enum.reduce(1..2, state, fn i, acc ->
          hlc_ts = HLC.now()
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}
          wrapped = {{:getdel, "rc_hlc_#{i}"}, %{hlc_ts: hlc_ts}}
          {new_state, {:applied_at, _, nil}, _effects} = StateMachine.apply(meta, wrapped, acc)
          new_state
        end)

      assert state_before.applied_count == 2

      # The 3rd apply should emit release_cursor
      hlc_ts = HLC.now()
      meta = %{index: 3, term: 1, system_time: System.os_time(:millisecond)}
      wrapped = {{:getdel, "rc_hlc_3"}, %{hlc_ts: hlc_ts}}

      {new_state, {:applied_at, _, nil}, effects} =
        StateMachine.apply(meta, wrapped, state_before)

      assert new_state.applied_count == 3
      cursor_effect = Enum.find(effects, &match?({:release_cursor, _}, &1))
      assert {:release_cursor, 3} = cursor_effect
    end
  end

  # ---------------------------------------------------------------------------
  # state_enter/2 -- leader HLC advancement
  # ---------------------------------------------------------------------------

  describe "state_enter(:leader, ...) HLC advancement" do
    test "state_enter(:leader) advances the HLC clock", %{state: state} do
      # Reset HLC to zero
      ref = :persistent_term.get(:ferricstore_hlc_ref)
      :atomics.put(ref, 1, 0)

      # Becoming leader should advance the HLC
      effects = StateMachine.state_enter(:leader, state)

      assert effects == []

      # After state_enter(:leader), HLC should be at wall-clock time
      {phys, _log} = HLC.now()
      wall = System.os_time(:millisecond)

      assert phys > 0
      # Physical component should be very close to wall clock
      assert abs(wall - phys) < 10
    end

    test "state_enter(:follower) still returns empty effects", %{state: state} do
      assert StateMachine.state_enter(:follower, state) == []
    end

    test "state_enter(:candidate) still returns empty effects", %{state: state} do
      assert StateMachine.state_enter(:candidate, state) == []
    end
  end

  # ---------------------------------------------------------------------------
  # Batcher HLC stamping (structural test via state machine)
  # ---------------------------------------------------------------------------

  describe "Batcher HLC stamp format" do
    test "wrapped command format is correctly handled by apply/3", %{state: state} do
      # Simulate the exact format that Batcher.stamp_hlc/1 produces
      inner_cmd = {:put, "stamped_key", "stamped_val", 0}
      hlc_ts = {System.os_time(:millisecond), 42}
      stamped = {inner_cmd, %{hlc_ts: hlc_ts}}

      {new_state, result} = StateMachine.apply(%{}, stamped, state)

      assert result == :ok
      assert new_state.applied_count == 1
    end

    test "wrapped batch command format is correctly handled", %{state: state} do
      batch = [{:put, "bk1", "bv1", 0}, {:put, "bk2", "bv2", 0}]
      hlc_ts = {System.os_time(:millisecond), 0}
      stamped = {{:batch, batch}, %{hlc_ts: hlc_ts}}

      {new_state, {:ok, results}} = StateMachine.apply(%{}, stamped, state)

      assert results == [:ok, :ok]
      assert new_state.applied_count == 2
    end

    test "wrapped command with high logical counter merges correctly", %{state: state} do
      # Simulate a remote leader with a very high logical counter.
      # This tests that the HLC merge handles high logical values.
      remote_physical = System.os_time(:millisecond) + 50
      hlc_ts = {remote_physical, 99_999}
      stamped = {{:put, "high_log_key", "val", 0}, %{hlc_ts: hlc_ts}}

      {_new_state, :ok} = StateMachine.apply(%{}, stamped, state)

      after_timestamp = HLC.now()
      assert HLC.compare(after_timestamp, hlc_ts) == :gt
      assert elem(after_timestamp, 0) > remote_physical
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "wrapped command with zero timestamp is handled gracefully", %{state: state} do
      hlc_ts = {0, 0}
      wrapped = {{:put, "zero_ts", "val", 0}, %{hlc_ts: hlc_ts}}

      {new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == :ok
      assert new_state.applied_count == 1
    end

    test "multiple wrapped commands maintain monotonic HLC", %{state: state} do
      timestamps =
        Enum.reduce(1..50, {state, []}, fn i, {acc, ts_list} ->
          hlc_ts = HLC.now()
          wrapped = {{:put, "mono_#{i}", "v#{i}", 0}, %{hlc_ts: hlc_ts}}
          {new_state, :ok} = StateMachine.apply(%{}, wrapped, acc)

          # Record the HLC after each apply
          after_ts = HLC.now()
          {new_state, ts_list ++ [after_ts]}
        end)
        |> elem(1)

      # All timestamps should be monotonically increasing
      timestamps
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [a, b] ->
        assert HLC.compare(b, a) == :gt,
               "Expected #{inspect(b)} > #{inspect(a)}"
      end)
    end
  end
end
