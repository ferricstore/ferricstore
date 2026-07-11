defmodule Ferricstore.Raft.StateMachineTest.Sections.ReleaseCursorLogCompaction do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

      describe "release_cursor log compaction" do
        test "init/1 stores release_cursor_interval from app env", %{store: _store, ets: ets} do
          state = init_state_for_release_cursor(ets)

          assert state.release_cursor_interval ==
                   Application.fetch_env!(:ferricstore, :release_cursor_interval)
        end

        test "init/1 accepts custom release_cursor_interval", %{store: _store, ets: ets} do
          state = init_state_for_release_cursor(ets, release_cursor_interval: 500)

          assert state.release_cursor_interval == 500
        end

        test "apply does not inherit stale pending state from a previous crashed apply", %{
          store: _store,
          ets: ets
        } do
          state = init_state_for_release_cursor(ets)

          stale_state = %{
            state
            | active_file_id: 99,
              active_file_path: Path.join(state.shard_data_path, "00099.log"),
              active_file_size: 12_345,
              file_stats: %{99 => %{live_bytes: 1, dead_bytes: 0}}
          }

          Process.put(:sm_apply_state, %{pending_state: stale_state})

          try do
            assert {new_state, _result, _effects} =
                     StateMachine.apply(%{index: 1}, {:getdel, "missing_after_stale"}, state)

            assert new_state.active_file_id == state.active_file_id
            assert new_state.active_file_path == state.active_file_path
            assert new_state.active_file_size == state.active_file_size
            assert new_state.file_stats == state.file_stats
          after
            Process.delete(:sm_apply_state)
          end
        end

        test "cross-shard control apply does not inherit stale pending state", %{
          store: _store,
          ets: ets
        } do
          state = init_state_for_release_cursor(ets)

          stale_state = %{
            state
            | active_file_id: 99,
              active_file_path: Path.join(state.shard_data_path, "00099.log"),
              active_file_size: 12_345,
              file_stats: %{99 => %{live_bytes: 1, dead_bytes: 0}}
          }

          Process.put(:sm_apply_state, %{pending_state: stale_state})

          try do
            assert {new_state, _result, _effects} =
                     StateMachine.apply(
                       %{index: 1},
                       {:cross_shard_intent, make_ref(), %{}},
                       state
                     )

            assert new_state.active_file_id == state.active_file_id
            assert new_state.active_file_path == state.active_file_path
            assert new_state.active_file_size == state.active_file_size
            assert new_state.file_stats == state.file_stats
          after
            Process.delete(:sm_apply_state)
          end
        end

        test "init/1 caches expanded paths for release cursor checkpoint checks", %{
          store: _store,
          ets: ets
        } do
          state = init_state_for_release_cursor(ets)

          assert state.data_dir_expanded == Path.expand(state.data_dir)
          assert state.shard_data_path_expanded == Path.expand(state.shard_data_path)
        end

        test "checkpoint path ownership check does not expand paths during release cursor checks" do
          source =
            Ferricstore.Test.SourceFiles.state_machine_source()

          [_match, body] =
            Regex.run(
              ~r/(defp instance_data_path\?\(.*?)(?=^\s*defp initial_file_stats)/ms,
              source
            )

          refute body =~ "Path.expand",
                 "release_cursor checkpoint checks must use paths normalized at state-machine init"
        end

        test "no release_cursor emitted before interval is reached", %{store: _store, ets: ets} do
          state = init_state_for_release_cursor(ets, release_cursor_interval: 5)

          # Apply 4 commands (below interval of 5) -- none should emit release_cursor
          result =
            Enum.reduce(1..4, state, fn i, acc ->
              meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

              {new_state, {:applied_at, _, :ok}, effects} =
                StateMachine.apply(meta, {:put, "rc_key_#{i}", "v#{i}", 0}, acc)

              if Enum.any?(effects, &match?({:release_cursor, _}, &1)) do
                flunk("release_cursor emitted before interval reached at apply #{i}")
              end

              new_state
            end)

          assert result.applied_count == 4
        end

        test "release_cursor emitted exactly at interval boundary for read-only command", %{
          store: _store,
          ets: ets
        } do
          interval = 5

          state =
            init_state_for_release_cursor(ets, release_cursor_interval: interval)

          # Apply (interval - 1) commands without release_cursor
          state_before =
            Enum.reduce(1..(interval - 1), state, fn i, acc ->
              meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

              {new_state, {:applied_at, _, nil}, _effects} =
                StateMachine.apply(meta, {:getdel, "rc_#{i}"}, acc)

              new_state
            end)

          assert state_before.applied_count == interval - 1

          # The N-th apply (index = interval) should emit release_cursor
          meta = %{index: interval, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, nil}, effects} =
            StateMachine.apply(meta, {:getdel, "rc_#{interval}"}, state_before)

          assert new_state.applied_count == interval

          # Verify the recovery checkpoint and release_cursor promotion effects.
          checkpoint_effect = Enum.find(effects, &match?({:checkpoint, _, _}, &1))
          assert {:checkpoint, ^interval, checkpoint_state} = checkpoint_effect
          assert checkpoint_state.shard_index == 0
          assert checkpoint_state.applied_count == interval

          cursor_effect = Enum.find(effects, &match?({:release_cursor, _}, &1))
          assert {:release_cursor, ra_index} = cursor_effect
          assert ra_index == interval

          assert Ferricstore.Raft.ReplaySafeIndexWriter.durable?(
                   new_state.instance_ctx,
                   new_state.shard_index,
                   new_state.shard_data_path,
                   interval
                 )
        end

        test "release_cursor waits while the shard has uncheckpointed bitcask data", %{
          state: state,
          shard_index: shard_index
        } do
          checkpoint_flags = :atomics.new(shard_index + 1, signed: false)
          disk_pressure = :atomics.new(shard_index + 1, signed: false)
          last_applied_index = :atomics.new(shard_index + 1, signed: false)
          last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)

          state = %{
            state
            | release_cursor_interval: 1,
              instance_ctx: %{
                checkpoint_flags: checkpoint_flags,
                last_applied_index: last_applied_index,
                last_released_cursor_index: last_released_cursor_index,
                disk_pressure: disk_pressure,
                hot_cache_max_value_size: 64
              }
          }

          meta = %{index: 1, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, 1, :ok}, effects} =
            StateMachine.apply(meta, {:put, "dirty_rc_key", "dirty_rc_value", 0}, state)

          assert new_state.applied_count == 1
          assert :atomics.get(checkpoint_flags, shard_index + 1) == 1
          assert :atomics.get(last_applied_index, shard_index + 1) == 1
          assert :atomics.get(last_released_cursor_index, shard_index + 1) == 0

          refute Enum.any?(effects, &match?({:release_cursor, 1}, &1)),
                 "Raft cursor must not advance past data that still needs a Bitcask checkpoint"
        end

        test "release_cursor block metric records consecutive blocked applies", %{
          ets: ets
        } do
          shard0 = 0
          shard1 = 1

          root =
            Path.join(System.tmp_dir!(), "sm_blocked_rc_#{System.unique_integer([:positive])}")

          shard1_path = Ferricstore.DataDir.shard_data_path(root, shard1)
          bad_active_path = Path.join(shard1_path, "active_as_dir.log")
          ets1 = :ets.new(:"sm_blocked_rc_#{System.unique_integer([:positive])}", [:set, :public])

          File.mkdir_p!(bad_active_path)
          Ferricstore.Store.ActiveFile.init(2)

          checkpoint_flags = :atomics.new(2, signed: false)
          checkpoint_in_flight = :atomics.new(2, signed: false)
          disk_pressure = :atomics.new(2, signed: false)
          last_applied_index = :atomics.new(2, signed: false)
          last_released_cursor_index = :atomics.new(2, signed: false)
          release_cursor_blocked_apply_count = :atomics.new(2, signed: false)

          state =
            init_state_for_release_cursor(ets,
              shard_index: shard0,
              release_cursor_interval: 1
            )

          instance_ctx = %{
            name: :"sm_blocked_rc_#{System.unique_integer([:positive])}",
            data_dir: root,
            shard_count: 2,
            keydir_refs: List.to_tuple([ets, ets1]),
            keydir_binary_bytes: :atomics.new(2, signed: false),
            checkpoint_flags: checkpoint_flags,
            checkpoint_in_flight: checkpoint_in_flight,
            disk_pressure: disk_pressure,
            last_applied_index: last_applied_index,
            last_released_cursor_index: last_released_cursor_index,
            release_cursor_blocked_apply_count: release_cursor_blocked_apply_count,
            hot_cache_max_value_size: 64
          }

          Ferricstore.Store.ActiveFile.publish(
            instance_ctx,
            shard0,
            state.active_file_id,
            state.active_file_path,
            state.shard_data_path
          )

          Ferricstore.Store.ActiveFile.publish(
            instance_ctx,
            shard1,
            0,
            bad_active_path,
            shard1_path
          )

          state = %{state | instance_ctx: instance_ctx}

          # The old cold location intentionally points at a missing retired file.
          # If shard1 fails after shard0 accepted its write, compensation must fail
          # instead of releasing Ra's cursor past divergent Bitcask state.
          key = "blocked_cold_original"
          :ets.insert(ets, {key, nil, 0, 0, 1, 0, 5})

          try do
            {state, {:applied_at, 1, {:error, {:cross_shard_compensation_failed, _reason}}},
             effects1} =
              StateMachine.apply(
                %{index: 1, term: 1, system_time: System.os_time(:millisecond)},
                {:cross_shard_tx,
                 [
                   {shard0, [{"SET", [key, "new"]}], nil},
                   {shard1, [{"SET", ["blocked_remote_fail", "value"]}], nil}
                 ]},
                state
              )

            assert :atomics.get(release_cursor_blocked_apply_count, shard0 + 1) == 1
            refute Enum.any?(effects1, &match?({:release_cursor, _}, &1))

            {_state, {:applied_at, 2, nil}, _effects2} =
              StateMachine.apply(
                %{index: 2, term: 1, system_time: System.os_time(:millisecond)},
                {:getdel, "unblocked_missing"},
                state
              )

            assert :atomics.get(release_cursor_blocked_apply_count, shard0 + 1) == 0
          after
            :ets.delete(ets1)
            Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
            File.rm_rf!(root)
          end
        end

        test "cross-shard SET dirties checkpoint state before release_cursor", %{
          ets: ets,
          shard_index: shard_index
        } do
          checkpoint_flags = :atomics.new(shard_index + 1, signed: false)
          checkpoint_in_flight = :atomics.new(shard_index + 1, signed: false)
          disk_pressure = :atomics.new(shard_index + 1, signed: false)
          last_applied_index = :atomics.new(shard_index + 1, signed: false)
          last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)

          state =
            init_state_for_release_cursor(ets,
              shard_index: shard_index,
              release_cursor_interval: 1
            )

          state = %{
            state
            | instance_ctx: %{
                checkpoint_flags: checkpoint_flags,
                checkpoint_in_flight: checkpoint_in_flight,
                disk_pressure: disk_pressure,
                last_applied_index: last_applied_index,
                last_released_cursor_index: last_released_cursor_index,
                hot_cache_max_value_size: 64,
                data_dir: state.data_dir
              }
          }

          meta = %{index: 1, term: 1, system_time: System.os_time(:millisecond)}

          {_new_state, {:applied_at, 1, %{^shard_index => [:ok]}}, effects} =
            StateMachine.apply(
              meta,
              {:cross_shard_tx, [{shard_index, [{"SET", ["cross_cursor_dirty", "value"]}], nil}]},
              state
            )

          assert :atomics.get(checkpoint_flags, shard_index + 1) == 1
          assert :atomics.get(last_released_cursor_index, shard_index + 1) == 0

          refute Enum.any?(effects, &match?({:release_cursor, 1}, &1)),
                 "cross-shard writes append to Bitcask and must not release Ra log before checkpoint fsync"
        end

        test "remote-only cross-shard SET blocks coordinator release_cursor until remote checkpoint is clean",
             %{ets: ets} do
          shard0 = 0
          shard1 = 1

          root =
            Path.join(System.tmp_dir!(), "sm_remote_rc_#{System.unique_integer([:positive])}")

          shard1_path = Ferricstore.DataDir.shard_data_path(root, shard1)
          shard1_file = Path.join(shard1_path, "00000.log")
          ets1 = :ets.new(:"sm_remote_rc_#{System.unique_integer([:positive])}", [:set, :public])

          File.mkdir_p!(shard1_path)
          File.touch!(shard1_file)
          Ferricstore.Store.ActiveFile.init(2)

          checkpoint_flags = :atomics.new(2, signed: false)
          checkpoint_in_flight = :atomics.new(2, signed: false)
          disk_pressure = :atomics.new(2, signed: false)
          last_applied_index = :atomics.new(2, signed: false)
          last_released_cursor_index = :atomics.new(2, signed: false)
          pending_release_cursor_checkpoint_count = :atomics.new(2, signed: false)
          replay_safe_index = :atomics.new(2, signed: false)
          flow_lmdb_replay_safe_index = :atomics.new(2, signed: false)
          flow_history_projected_index = :atomics.new(2, signed: false)

          :atomics.put(replay_safe_index, shard0 + 1, 2)
          :atomics.put(flow_lmdb_replay_safe_index, shard0 + 1, 2)
          :atomics.put(flow_history_projected_index, shard0 + 1, 2)

          state =
            init_state_for_release_cursor(ets,
              shard_index: shard0,
              release_cursor_interval: 1
            )

          instance_ctx = %{
            name: :"sm_remote_rc_#{System.unique_integer([:positive])}",
            data_dir: root,
            shard_count: 2,
            keydir_refs: List.to_tuple([ets, ets1]),
            keydir_binary_bytes: :atomics.new(2, signed: false),
            checkpoint_flags: checkpoint_flags,
            checkpoint_in_flight: checkpoint_in_flight,
            disk_pressure: disk_pressure,
            last_applied_index: last_applied_index,
            last_released_cursor_index: last_released_cursor_index,
            pending_release_cursor_checkpoint_count: pending_release_cursor_checkpoint_count,
            replay_safe_index: replay_safe_index,
            flow_lmdb_replay_safe_index: flow_lmdb_replay_safe_index,
            flow_history_projected_index: flow_history_projected_index,
            hot_cache_max_value_size: 64
          }

          Ferricstore.Store.ActiveFile.publish(
            instance_ctx,
            shard0,
            state.active_file_id,
            state.active_file_path,
            state.shard_data_path
          )

          Ferricstore.Store.ActiveFile.publish(instance_ctx, shard1, 0, shard1_file, shard1_path)

          state = %{state | instance_ctx: instance_ctx}

          try do
            {state, {:applied_at, 1, %{^shard1 => [:ok]}}, effects1} =
              StateMachine.apply(
                %{index: 1, term: 1, system_time: System.os_time(:millisecond)},
                {:cross_shard_tx, [{shard1, [{"SET", ["remote_cursor_dirty", "value"]}], nil}]},
                state
              )

            assert :atomics.get(checkpoint_flags, shard1 + 1) == 1
            assert :atomics.get(last_released_cursor_index, shard0 + 1) == 0
            assert :atomics.get(pending_release_cursor_checkpoint_count, shard0 + 1) == 1

            refute Enum.any?(effects1, &match?({:release_cursor, 1}, &1)),
                   "coordinator Ra log must wait for remote Bitcask checkpoint durability"

            :atomics.put(checkpoint_flags, shard1 + 1, 0)
            :atomics.put(checkpoint_in_flight, shard1 + 1, 0)

            {_state, {:applied_at, 2, nil}, effects2} =
              StateMachine.apply(
                %{index: 2, term: 1, system_time: System.os_time(:millisecond)},
                {:getdel, "remote_cursor_missing"},
                state
              )

            assert Enum.any?(effects2, &match?({:release_cursor, 2}, &1))
            assert :atomics.get(last_released_cursor_index, shard0 + 1) == 2
            assert :atomics.get(pending_release_cursor_checkpoint_count, shard0 + 1) == 0
          after
            :ets.delete(ets1)
            Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
            File.rm_rf!(root)
          end
        end

        test "remote-only cross-shard SET rotates the remote active file when it grows past threshold",
             %{ets: ets} do
          shard0 = 0
          shard1 = 1

          root =
            Path.join(System.tmp_dir!(), "sm_remote_rotate_#{System.unique_integer([:positive])}")

          shard1_path = Ferricstore.DataDir.shard_data_path(root, shard1)
          shard1_file = Path.join(shard1_path, "00000.log")

          ets1 =
            :ets.new(:"sm_remote_rotate_#{System.unique_integer([:positive])}", [:set, :public])

          File.mkdir_p!(shard1_path)
          File.touch!(shard1_file)
          Ferricstore.Store.ActiveFile.init(2)

          state =
            init_state_for_release_cursor(ets,
              shard_index: shard0,
              release_cursor_interval: 1
            )

          instance_ctx = %{
            name: :"sm_remote_rotate_#{System.unique_integer([:positive])}",
            data_dir: root,
            shard_count: 2,
            keydir_refs: List.to_tuple([ets, ets1]),
            keydir_binary_bytes: :atomics.new(2, signed: false),
            checkpoint_flags: :atomics.new(2, signed: false),
            checkpoint_in_flight: :atomics.new(2, signed: false),
            disk_pressure: :atomics.new(2, signed: false),
            last_applied_index: :atomics.new(2, signed: false),
            last_released_cursor_index: :atomics.new(2, signed: false),
            hot_cache_max_value_size: 64,
            max_active_file_size: 80
          }

          Ferricstore.Store.ActiveFile.publish(
            instance_ctx,
            shard0,
            state.active_file_id,
            state.active_file_path,
            state.shard_data_path
          )

          Ferricstore.Store.ActiveFile.publish(instance_ctx, shard1, 0, shard1_file, shard1_path)

          state = %{state | instance_ctx: instance_ctx}
          value = :binary.copy("R", 120)

          try do
            {_state, {:applied_at, 1, %{^shard1 => [:ok]}}, _effects} =
              StateMachine.apply(
                %{index: 1, term: 1, system_time: System.os_time(:millisecond)},
                {:cross_shard_tx, [{shard1, [{"SET", ["remote_rotate_key", value]}], nil}]},
                state
              )

            assert {1, rotated_path, ^shard1_path} =
                     Ferricstore.Store.ActiveFile.get(instance_ctx, shard1)

            assert rotated_path == Path.join(shard1_path, "00001.log")
            assert File.exists?(rotated_path)
          after
            :ets.delete(ets1)
            Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
            File.rm_rf!(root)
          end
        end

        test "release_cursor promotes prior checkpoint when shard was clean before next write", %{
          state: state,
          shard_index: shard_index
        } do
          checkpoint_flags = :atomics.new(shard_index + 1, signed: false)
          checkpoint_in_flight = :atomics.new(shard_index + 1, signed: false)
          last_applied_index = :atomics.new(shard_index + 1, signed: false)
          last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)

          state = %{
            state
            | release_cursor_interval: 2,
              instance_ctx:
                release_cursor_instance_ctx(shard_index, 2,
                  checkpoint_flags: checkpoint_flags,
                  checkpoint_in_flight: checkpoint_in_flight,
                  last_applied_index: last_applied_index,
                  last_released_cursor_index: last_released_cursor_index
                )
          }

          {state, {:applied_at, 1, :ok}, effects1} =
            StateMachine.apply(
              %{index: 1, term: 1, system_time: System.os_time(:millisecond)},
              {:put, "cursor_starve_1", "value", 0},
              state
            )

          refute Enum.any?(effects1, &match?({:release_cursor, _}, &1))

          {state, {:applied_at, 2, :ok}, effects2} =
            StateMachine.apply(
              %{index: 2, term: 1, system_time: System.os_time(:millisecond)},
              {:put, "cursor_starve_2", "value", 0},
              state
            )

          assert Enum.any?(effects2, &match?({:checkpoint, 2, _}, &1))
          refute Enum.any?(effects2, &match?({:release_cursor, _}, &1))

          :atomics.put(checkpoint_flags, shard_index + 1, 0)
          :atomics.put(checkpoint_in_flight, shard_index + 1, 0)

          {_state, {:applied_at, 3, :ok}, effects3} =
            StateMachine.apply(
              %{index: 3, term: 1, system_time: System.os_time(:millisecond)},
              {:put, "cursor_starve_3", "value", 0},
              state
            )

          assert Enum.any?(effects3, &match?({:release_cursor, 2}, &1))
          assert :atomics.get(last_released_cursor_index, shard_index + 1) == 2
        end

        test "release_cursor records last released cursor index when emitted", %{
          state: state,
          shard_index: shard_index
        } do
          last_applied_index = :atomics.new(shard_index + 1, signed: false)
          last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)

          state = %{
            state
            | release_cursor_interval: 1,
              instance_ctx:
                release_cursor_instance_ctx(shard_index, 42,
                  last_applied_index: last_applied_index,
                  last_released_cursor_index: last_released_cursor_index
                )
          }

          meta = %{index: 42, term: 1, system_time: System.os_time(:millisecond)}

          {_new_state, {:applied_at, 42, nil}, effects} =
            StateMachine.apply(meta, {:getdel, "released_cursor_metric_missing"}, state)

          assert Enum.any?(effects, &match?({:release_cursor, 42}, &1))
          assert :atomics.get(last_applied_index, shard_index + 1) == 42
          assert :atomics.get(last_released_cursor_index, shard_index + 1) == 42
        end

        test "release_cursor tolerates legacy recovered state without pending cursor fields", %{
          state: state,
          shard_index: shard_index
        } do
          last_applied_index = :atomics.new(shard_index + 1, signed: false)
          last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)

          state =
            state
            |> Map.merge(%{
              release_cursor_interval: 1,
              instance_ctx:
                release_cursor_instance_ctx(shard_index, 44,
                  last_applied_index: last_applied_index,
                  last_released_cursor_index: last_released_cursor_index
                )
            })
            |> Map.drop([
              :pending_release_cursor_index,
              :pending_replay_safe_marker_index,
              :pending_release_cursor_checkpoint_indices
            ])

          meta = %{index: 44, term: 1, system_time: System.os_time(:millisecond)}

          {_new_state, {:applied_at, 44, nil}, effects} =
            StateMachine.apply(meta, {:getdel, "legacy_release_cursor_missing"}, state)

          assert Enum.any?(effects, &match?({:release_cursor, 44}, &1))
          assert :atomics.get(last_applied_index, shard_index + 1) == 44
          assert :atomics.get(last_released_cursor_index, shard_index + 1) == 44
        end

        test "release_cursor is not emitted when replay-safe marker cannot persist", %{
          state: state,
          shard_index: shard_index
        } do
          last_applied_index = :atomics.new(shard_index + 1, signed: false)
          last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)

          invalid_marker_dir =
            Path.join(
              System.tmp_dir!(),
              "replay_safe_marker_file_#{System.unique_integer([:positive])}"
            )

          File.write!(invalid_marker_dir, "not a directory")
          on_exit(fn -> File.rm(invalid_marker_dir) end)

          state = %{
            state
            | release_cursor_interval: 1,
              shard_data_path: invalid_marker_dir,
              instance_ctx: %{
                checkpoint_flags: :atomics.new(shard_index + 1, signed: false),
                checkpoint_in_flight: :atomics.new(shard_index + 1, signed: false),
                last_applied_index: last_applied_index,
                last_released_cursor_index: last_released_cursor_index
              }
          }

          meta = %{index: 43, term: 1, system_time: System.os_time(:millisecond)}

          {_new_state, {:applied_at, 43, nil}, effects} =
            StateMachine.apply(meta, {:getdel, "released_cursor_persist_failure"}, state)

          refute Enum.any?(effects, &match?({:release_cursor, 43}, &1))
          refute Enum.any?(effects, &match?({:checkpoint, 43, _}, &1))
          assert :atomics.get(last_applied_index, shard_index + 1) == 43
          assert :atomics.get(last_released_cursor_index, shard_index + 1) == 0
        end

        test "release cursor metrics resolve instance context by name like production Raft config",
             %{
               state: state,
               shard_index: shard_index
             } do
          instance_name = :"cursor_metric_instance_#{System.unique_integer([:positive])}"
          root = Path.join(System.tmp_dir!(), Atom.to_string(instance_name))
          File.rm_rf!(root)
          File.mkdir_p!(root)

          instance_ctx =
            FerricStore.Instance.build(instance_name,
              shard_count: shard_index + 1,
              data_dir: root
            )

          :atomics.put(instance_ctx.replay_safe_index, shard_index + 1, 77)
          :atomics.put(instance_ctx.flow_lmdb_replay_safe_index, shard_index + 1, 77)
          :atomics.put(instance_ctx.flow_history_projected_index, shard_index + 1, 77)

          on_exit({:cursor_metric_instance, instance_name}, fn ->
            FerricStore.Instance.cleanup(instance_name)
            File.rm_rf!(root)
          end)

          state = %{
            state
            | release_cursor_interval: 1,
              instance_ctx: nil,
              instance_name: instance_name
          }

          meta = %{index: 77, term: 1, system_time: System.os_time(:millisecond)}

          {_new_state, {:applied_at, 77, nil}, effects} =
            StateMachine.apply(meta, {:getdel, "released_cursor_metric_by_name_missing"}, state)

          assert Enum.any?(effects, &match?({:release_cursor, 77}, &1))
          assert :atomics.get(instance_ctx.last_applied_index, shard_index + 1) == 77
          assert :atomics.get(instance_ctx.last_released_cursor_index, shard_index + 1) == 77
        end

        test "named state machine marks checkpoint dirty and blocks release after nosync write",
             %{
               ets: ets
             } do
          instance_name = :"cursor_dirty_instance_#{System.unique_integer([:positive])}"
          root = Path.join(System.tmp_dir!(), Atom.to_string(instance_name))
          File.rm_rf!(root)
          File.mkdir_p!(root)

          instance_ctx = FerricStore.Instance.build(instance_name, shard_count: 1, data_dir: root)

          on_exit({:cursor_dirty_instance, instance_name}, fn ->
            FerricStore.Instance.cleanup(instance_name)
            File.rm_rf!(root)
          end)

          state =
            init_state_for_release_cursor(ets,
              shard_index: 0,
              release_cursor_interval: 1,
              instance_ctx: nil,
              instance_name: instance_name
            )

          meta = %{index: 88, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, 88, :ok}, effects} =
            StateMachine.apply(meta, {:put, "dirty_named_rc_key", "value", 0}, state)

          assert new_state.applied_count == 1
          assert :atomics.get(instance_ctx.checkpoint_flags, 1) == 1
          refute Enum.any?(effects, &match?({:release_cursor, 88}, &1))
          assert Ferricstore.Raft.ReplaySafeIndex.read(new_state.shard_data_path) == 0
        end

        test "release_cursor waits while checkpoint fsync is in flight", %{
          state: state,
          shard_index: shard_index
        } do
          checkpoint_flags = :atomics.new(shard_index + 1, signed: false)
          checkpoint_in_flight = :atomics.new(shard_index + 1, signed: false)

          # The checkpointer clears checkpoint_flags before async fsync starts.
          # StateMachine must still see the in-flight marker and keep Ra log
          # entries until the fsync completion arrives.
          :atomics.put(checkpoint_flags, shard_index + 1, 0)
          :atomics.put(checkpoint_in_flight, shard_index + 1, 1)

          state = %{
            state
            | release_cursor_interval: 1,
              instance_ctx: %{
                checkpoint_flags: checkpoint_flags,
                checkpoint_in_flight: checkpoint_in_flight
              }
          }

          meta = %{index: 1, term: 1, system_time: System.os_time(:millisecond)}

          {_new_state, {:applied_at, 1, nil}, effects} =
            StateMachine.apply(meta, {:getdel, "missing_during_checkpoint"}, state)

          refute Enum.any?(effects, &match?({:release_cursor, 1}, &1)),
                 "Raft cursor must not advance while Bitcask fsync is still in flight"
        end

        test "release_cursor waits when custom instance checkpoint state is unresolved", %{
          state: state
        } do
          name = :"missing_custom_instance_#{System.unique_integer([:positive])}"

          state = %{
            state
            | release_cursor_interval: 1,
              instance_ctx: nil,
              instance_name: name
          }

          meta = %{index: 1, term: 1, system_time: System.os_time(:millisecond)}

          {_new_state, {:applied_at, 1, nil}, effects} =
            StateMachine.apply(meta, {:getdel, "missing_custom_checkpoint_ctx"}, state)

          refute Enum.any?(effects, &match?({:release_cursor, 1}, &1)),
                 "custom instance state must fail closed until checkpoint atomics are resolved"
        end

        test "release_cursor emitted at every interval multiple", %{store: _store, ets: ets} do
          interval = 3

          state =
            init_state_for_release_cursor(ets, release_cursor_interval: interval)

          # Apply 9 commands, expect release_cursor at positions 3, 6, 9
          {_final_state, cursor_indices} =
            Enum.reduce(1..9, {state, []}, fn i, {acc, cursors} ->
              meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

              {new_state, {:applied_at, _, nil}, effects} =
                StateMachine.apply(meta, {:getdel, "mc_#{i}"}, acc)

              cursor_idx =
                Enum.find_value(effects, fn
                  {:release_cursor, idx} -> idx
                  _ -> nil
                end)

              if cursor_idx, do: {new_state, cursors ++ [cursor_idx]}, else: {new_state, cursors}
            end)

          assert cursor_indices == [3, 6, 9]
        end

        test "release_cursor emitted for read-only delete miss at interval boundary", %{
          store: _store,
          ets: ets
        } do
          interval = 3

          state =
            init_state_for_release_cursor(ets, release_cursor_interval: interval)

          # Apply two read-only misses (applied_count = 2), then another at the 3rd apply.
          meta1 = %{index: 10, term: 1, system_time: System.os_time(:millisecond)}

          {s1, {:applied_at, _, nil}, _e1} =
            StateMachine.apply(meta1, {:getdel, "del_rc_a"}, state)

          meta2 = %{index: 11, term: 1, system_time: System.os_time(:millisecond)}

          {s2, {:applied_at, _, nil}, _e2} =
            StateMachine.apply(meta2, {:getdel, "del_rc_b"}, s1)

          # 3rd read-only delete miss should trigger release_cursor.
          meta3 = %{index: 12, term: 1, system_time: System.os_time(:millisecond)}

          {_s3, {:applied_at, _, nil}, effects} =
            StateMachine.apply(meta3, {:getdel, "del_rc_a"}, s2)

          cursor_effect = Enum.find(effects, &match?({:release_cursor, _}, &1))
          assert {:release_cursor, 12} = cursor_effect
        end

        test "release_cursor emitted for batch that crosses interval boundary", %{
          store: _store,
          ets: ets
        } do
          interval = 5

          state =
            init_state_for_release_cursor(ets, release_cursor_interval: interval)

          # Apply 3 single commands (applied_count = 3)
          state_before =
            Enum.reduce(1..3, state, fn i, acc ->
              meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

              {new_state, {:applied_at, _, nil}, _e} =
                StateMachine.apply(meta, {:getdel, "pre_#{i}"}, acc)

              new_state
            end)

          assert state_before.applied_count == 3

          # Batch of 3 commands takes applied_count from 3 to 6 -- crosses interval at 5
          batch = [
            {:getdel, "batch_1"},
            {:getdel, "batch_2"},
            {:getdel, "batch_3"}
          ]

          meta = %{index: 4, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, {:ok, results}}, effects} =
            StateMachine.apply(meta, {:batch, batch}, state_before)

          assert results == [nil, nil, nil]
          assert new_state.applied_count == 6
          cursor_effect = Enum.find(effects, &match?({:release_cursor, _}, &1))
          assert {:release_cursor, 4} = cursor_effect
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
            init_state_for_release_cursor(ets, shard_index: 2, release_cursor_interval: interval)

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

          checkpoint_effect = Enum.find(effects, &match?({:checkpoint, _, _}, &1))
          assert {:checkpoint, 3, cursor_state} = checkpoint_effect

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
    end
  end
end
