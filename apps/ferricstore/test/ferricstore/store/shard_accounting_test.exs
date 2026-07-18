defmodule Ferricstore.Store.ShardAccountingTest do
  @moduledoc false
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Ferricstore.Store.{CompoundKey, LFU, Promotion}
  alias Ferricstore.Store.Shard
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle

  @record_header_size 26

  describe "shared log dead-byte accounting" do
    test "delete counts live records in file 0" do
      keydir = new_keydir()
      key = "accounting:file0:delete"
      old_value_size = 5

      try do
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, 12, old_value_size})

        state =
          ShardFlush.track_delete_dead_bytes(
            %{keydir: keydir, file_stats: %{0 => {100, 3}}},
            key
          )

        assert state.file_stats[0] ==
                 {100, 3 + @record_header_size + byte_size(key) + old_value_size}
      after
        :ets.delete(keydir)
      end
    end

    test "delete counts empty live records" do
      keydir = new_keydir()
      key = "accounting:empty:delete"

      try do
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, 20, 0})

        state =
          ShardFlush.track_delete_dead_bytes(
            %{keydir: keydir, file_stats: %{0 => {100, 0}}},
            key
          )

        assert state.file_stats[0] == {100, @record_header_size + byte_size(key)}
      after
        :ets.delete(keydir)
      end
    end

    test "pending overwrite counts old empty records in file 0" do
      keydir = new_keydir()
      key = "accounting:empty:overwrite"

      try do
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, 8, 0})

        state = %{
          keydir: keydir,
          active_file_id: 1,
          file_stats: %{0 => {100, 2}, 1 => {0, 0}}
        }

        ShardETS.ets_insert(state, key, "new-value", 0)
        state = ShardFlush.update_ets_locations(state, [{key, "new-value", 0}], [{30, 35}])

        assert state.file_stats[0] == {100, 2 + @record_header_size + byte_size(key)}
        assert [{^key, _value, _exp, _lfu, 1, 30, 9}] = :ets.lookup(keydir, key)
      after
        :ets.delete(keydir)
      end
    end

    test "async SET overwrite flow counts the old disk record" do
      keydir = new_keydir()
      key = "accounting:async:set:overwrite"
      old_fid = 2
      old_value_size = 9

      try do
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), old_fid, 8, old_value_size})

        state = %{
          keydir: keydir,
          active_file_id: 3,
          file_stats: %{old_fid => {120, 0}, 3 => {0, 0}}
        }

        # This mirrors the direct async SET path: write the new pending value
        # into ETS first, then update ETS locations after the batch append.
        ShardETS.ets_insert(state, key, "new-value", 0)
        state = ShardFlush.update_ets_locations(state, [{key, "new-value", 0}], [{30, 35}])

        assert state.file_stats[old_fid] ==
                 {120, @record_header_size + byte_size(key) + old_value_size}
      after
        :ets.delete(keydir)
      end
    end

    test "async SET batch with repeated key counts original and superseded pending records" do
      keydir = new_keydir()
      key = "accounting:async:set:repeat"
      old_fid = 2
      old_value_size = 3
      first_value = "first"
      second_value = "second"

      try do
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), old_fid, 8, old_value_size})

        state = %{
          keydir: keydir,
          active_file_id: 3,
          file_stats: %{old_fid => {120, 0}, 3 => {0, 0}}
        }

        ShardETS.ets_insert(state, key, first_value, 0)
        ShardETS.ets_insert(state, key, second_value, 0)

        state =
          ShardFlush.update_ets_locations(
            state,
            [{key, first_value, 0}, {key, second_value, 0}],
            [{30, 31}, {61, 32}]
          )

        old_record_size = @record_header_size + byte_size(key) + old_value_size
        first_record_size = @record_header_size + byte_size(key) + byte_size(first_value)

        assert state.file_stats[old_fid] == {120, old_record_size}
        assert state.file_stats[3] == {0, first_record_size}
      after
        :ets.delete(keydir)
      end
    end

    test "stale flush metadata does not overwrite a newer committed ETS row" do
      keydir = new_keydir()
      key = "accounting:async:set:stale-flush"

      try do
        :ets.insert(keydir, {key, "newer-value", 0, LFU.initial(), 4, 80, 11})

        state = %{
          keydir: keydir,
          active_file_id: 3,
          file_stats: %{3 => {0, 0}, 4 => {120, 0}}
        }

        state =
          ShardFlush.update_ets_locations(
            state,
            [{key, "stale-value", 0}],
            [{30, 37}]
          )

        assert [{^key, "newer-value", 0, _lfu, 4, 80, 11}] = :ets.lookup(keydir, key)
        assert state.file_stats[4] == {120, 0}
      after
        :ets.delete(keydir)
      end
    end

    test "recomputed file_stats count live records in file 0" do
      keydir = new_keydir()

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-accounting-#{System.unique_integer([:positive])}"
        )

      key = "accounting:recover:file0"

      try do
        File.mkdir_p!(dir)
        File.write!(Path.join(dir, "00000.log"), :binary.copy("x", 100))
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, 1, 0})

        stats = ShardFlush.compute_file_stats(dir, keydir)

        assert stats[0] == {100, 100 - (@record_header_size + byte_size(key))}
      after
        File.rm_rf(dir)
        :ets.delete(keydir)
      end
    end

    test "recomputed file_stats count expired ETS rows as dead" do
      keydir = new_keydir()

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-accounting-expired-#{System.unique_integer([:positive])}"
        )

      key = "accounting:recover:expired"
      expired_at = Ferricstore.HLC.now_ms() - 1_000

      try do
        File.mkdir_p!(dir)
        File.write!(Path.join(dir, "00000.log"), :binary.copy("x", 100))
        :ets.insert(keydir, {key, nil, expired_at, LFU.initial(), 0, 1, 0})

        stats = ShardFlush.compute_file_stats(dir, keydir)

        assert stats[0] == {100, 100}
      after
        File.rm_rf(dir)
        :ets.delete(keydir)
      end
    end

    test "recomputed file_stats ignore noncanonical aliases of canonical segments" do
      keydir = new_keydir()

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-accounting-alias-#{System.unique_integer([:positive])}"
        )

      try do
        File.mkdir_p!(dir)
        File.write!(Path.join(dir, "000000.log"), "alias")

        assert ShardFlush.compute_file_stats(dir, keydir) == %{}
      after
        File.rm_rf(dir)
        :ets.delete(keydir)
      end
    end

    test "expiry sweep counts old shared records as dead bytes" do
      keydir = new_keydir()

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-expiry-accounting-#{System.unique_integer([:positive])}"
        )

      key = "accounting:expiry:dead"
      old_fid = 2
      old_value_size = 7
      old_dead = 3
      old_total = 150
      active_path = Path.join(dir, "00003.log")
      expired_at = Ferricstore.HLC.now_ms() - 1_000

      try do
        File.mkdir_p!(dir)
        File.touch!(active_path)
        :ets.insert(keydir, {key, nil, expired_at, LFU.initial(), old_fid, 8, old_value_size})

        state = %{
          index: 0,
          keydir: keydir,
          active_file_path: active_path,
          file_stats: %{old_fid => {old_total, old_dead}},
          promoted_instances: %{},
          instance_ctx: nil,
          sweep_at_ceiling_count: 0,
          sweep_struggling: false
        }

        state = ShardLifecycle.do_expiry_sweep(state)

        assert :ets.lookup(keydir, key) == []

        assert state.file_stats[old_fid] ==
                 {old_total, old_dead + @record_header_size + byte_size(key) + old_value_size}
      after
        File.rm_rf(dir)
        :ets.delete(keydir)
      end
    end
  end

  describe "promoted compound dead-byte accounting" do
    test "promoted compaction reserves its latch before spawning the worker" do
      source =
        File.read!(Path.expand("../../../lib/ferricstore/store/shard/info.ex", __DIR__))

      [_before, start_body] =
        String.split(source, "defp maybe_start_promoted_compaction(", parts: 2)

      [start_body | _after] =
        String.split(start_body, "defp maybe_start_promoted_compaction(state", parts: 2)

      acquire_offset = source_offset!(start_body, "Promotion.acquire_compaction_latch")
      spawn_call_offset = source_offset!(start_body, "spawn_promoted_compaction_worker")

      assert acquire_offset < spawn_call_offset
      assert start_body =~ "latch_token: latch_token"

      [_before, spawn_body] =
        String.split(source, "defp spawn_promoted_compaction_worker(", parts: 2)

      assert spawn_body =~ "spawn_monitor"

      transfer_offset =
        source_offset!(spawn_body, "transfer_promoted_compaction_latch(latch_token, pid)")

      start_offset =
        source_offset!(spawn_body, "send(pid, {:start_promoted_compaction, job_ref})")

      assert transfer_offset < start_offset
    end

    test "failed promoted compaction releases its pre-acquired latch" do
      tmp_dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-promoted-latch-down-#{System.unique_integer([:positive])}"
        )

      instance_name = :"promoted_latch_down_#{System.unique_integer([:positive])}"
      File.mkdir_p!(tmp_dir)

      ctx =
        FerricStore.Instance.build(instance_name,
          data_dir: tmp_dir,
          shard_count: 1
        )

      redis_key = "hash:promoted:latch-down"
      owner = %{instance_ctx: ctx, index: 0}
      latch_token = Promotion.acquire_compaction_latch(owner, redis_key)
      {latch_table, latch_key} = latch_token
      worker_pid = spawn(fn -> Process.sleep(:infinity) end)
      monitor_ref = make_ref()
      true = :ets.update_element(latch_table, latch_key, {2, worker_pid})

      state = %Shard{
        index: 0,
        promoted_compaction_worker: %{
          monitor_ref: monitor_ref,
          pid: worker_pid,
          redis_key: redis_key,
          latch_token: latch_token
        },
        promoted_compaction_retry_ms: 60_000,
        promoted_instances: %{
          redis_key => %{
            path: tmp_dir,
            total_bytes: 2_200_000,
            dead_bytes: 1_200_000,
            last_compacted_at: nil
          }
        }
      }

      try do
        assert [_entry] = :ets.lookup(latch_table, latch_key)

        assert {:noreply, next_state} =
                 Shard.handle_info(
                   {:DOWN, monitor_ref, :process, worker_pid, :synthetic_failure},
                   state
                 )

        assert [] = :ets.lookup(latch_table, latch_key)

        Enum.each(next_state.promoted_compaction_retry_timers, fn {_key, timer} ->
          Process.cancel_timer(timer.timer_ref, async: false, info: false)
        end)
      after
        Process.exit(worker_pid, :kill)
        FerricStore.Instance.cleanup(instance_name)
        File.rm_rf(tmp_dir)
      end
    end

    test "failed promoted compaction backs off without immediately restarting the same key" do
      redis_key = "hash:promoted:worker-backoff"
      worker_pid = spawn(fn -> Process.sleep(:infinity) end)
      monitor_ref = make_ref()

      state = %Shard{
        index: 0,
        promoted_compaction_worker: %{
          monitor_ref: monitor_ref,
          pid: worker_pid,
          redis_key: redis_key
        },
        promoted_compaction_pending: MapSet.new([redis_key]),
        promoted_compaction_retry_ms: 5,
        promoted_compaction_retry_timers: %{},
        promoted_instances: %{
          redis_key => %{
            path: "/tmp/promoted-worker-backoff",
            total_bytes: 2_200_000,
            dead_bytes: 1_200_000,
            last_compacted_at: nil
          }
        }
      }

      assert {:noreply, next_state} =
               Shard.handle_info(
                 {:DOWN, monitor_ref, :process, worker_pid, :synthetic_failure},
                 state
               )

      assert next_state.promoted_compaction_worker == nil
      refute MapSet.member?(next_state.promoted_compaction_pending, redis_key)
      assert %{tag: retry_tag} = next_state.promoted_compaction_retry_timers[redis_key]
      assert_receive {:retry_promoted_compaction, ^redis_key, ^retry_tag}, 50

      Process.exit(worker_pid, :kill)
    end

    test "failed promoted compaction result suppresses due messages until its retry timer" do
      redis_key = "hash:promoted:result-backoff"
      worker_pid = self()
      monitor_ref = make_ref()
      job_ref = make_ref()

      state = %Shard{
        index: 0,
        promoted_compaction_worker: %{
          job_ref: job_ref,
          monitor_ref: monitor_ref,
          pid: worker_pid,
          redis_key: redis_key,
          path: "/tmp/promoted-result-backoff",
          baseline_dead: 1_200_000
        },
        promoted_compaction_pending: MapSet.new([redis_key]),
        promoted_compaction_retry_ms: 50,
        promoted_compaction_retry_timers: %{},
        promoted_instances: %{
          redis_key => %{
            path: "/tmp/promoted-result-backoff",
            total_bytes: 2_200_000,
            dead_bytes: 1_200_000,
            last_compacted_at: nil
          }
        }
      }

      assert {:noreply, backed_off} =
               Shard.handle_info(
                 {:promoted_compaction_complete, job_ref, worker_pid, :error},
                 state
               )

      assert backed_off.promoted_compaction_worker == nil
      refute MapSet.member?(backed_off.promoted_compaction_pending, redis_key)
      assert map_size(backed_off.promoted_compaction_retry_timers) == 1

      assert {:noreply, still_backed_off} =
               Shard.handle_info({:maybe_compact_promoted, redis_key}, backed_off)

      assert still_backed_off.promoted_compaction_worker == nil

      assert still_backed_off.promoted_compaction_retry_timers ==
               backed_off.promoted_compaction_retry_timers
    end

    test "dedicated compaction preserves type metadata outside the member catalog" do
      keydir = new_keydir()
      member_index = :ets.new(:promoted_compaction_type_members, [:ordered_set, :public])
      redis_key = "hash:promoted:type-metadata"
      type_key = CompoundKey.type_key(redis_key)

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-promoted-type-#{System.unique_integer([:positive])}"
        )

      try do
        File.mkdir_p!(dir)
        old_log = Path.join(dir, "00000.log")
        File.touch!(old_log)
        CompoundMemberIndex.reset(member_index)

        {:ok, {offset, _record_size}} =
          Ferricstore.Bitcask.NIF.v2_append_record(old_log, type_key, "hash", 0)

        :ets.insert(keydir, {Promotion.marker_key(redis_key), "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(keydir, {type_key, "hash", 0, LFU.initial(), 0, offset, 4})

        state = %{
          index: 0,
          keydir: keydir,
          compound_member_index: member_index,
          instance_ctx: nil,
          promoted_instances: %{
            redis_key => %{
              path: dir,
              total_bytes: 2_200_000,
              dead_bytes: 1_200_000,
              last_compacted_at: nil
            }
          }
        }

        assert {:ok, _state} = ShardCompound.compact_dedicated_result(state, redis_key, dir)
        assert [{^type_key, _value, 0, _lfu, 1, new_offset, 4}] = :ets.lookup(keydir, type_key)

        assert {:ok, "hash"} =
                 Ferricstore.Bitcask.NIF.v2_pread_at(Path.join(dir, "00001.log"), new_offset)
      after
        File.rm_rf(dir)
        :ets.delete(member_index)
        :ets.delete(keydir)
      end
    end

    test "dedicated compaction pages through the exact member catalog" do
      keydir = new_keydir()
      member_index = :ets.new(:promoted_compaction_members, [:ordered_set, :public])
      redis_key = "hash:promoted:paged-compaction"

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-promoted-paged-#{System.unique_integer([:positive])}"
        )

      try do
        File.mkdir_p!(dir)
        File.touch!(Path.join(dir, "00000.log"))
        CompoundMemberIndex.reset(member_index)

        :ets.insert(keydir, {Promotion.marker_key(redis_key), "hash", 0, LFU.initial(), 0, 0, 4})

        Enum.each(1..1_100, fn index ->
          compound_key = CompoundKey.hash_field(redis_key, Integer.to_string(index))
          :ets.insert(keydir, {compound_key, "value", 0, LFU.initial(), 0, index, 5})
          CompoundMemberIndex.put(member_index, compound_key)
        end)

        state = %{
          index: 0,
          keydir: keydir,
          compound_member_index: member_index,
          instance_ctx: nil,
          promoted_instances: %{
            redis_key => %{
              path: dir,
              total_bytes: 2_200_000,
              dead_bytes: 1_200_000,
              last_compacted_at: nil
            }
          }
        }

        Process.put(:promoted_compaction_page_sizes, [])

        Process.put(:ferricstore_promoted_compaction_after_collect_hook, fn ^redis_key,
                                                                            live_entries ->
          Process.put(
            :promoted_compaction_page_sizes,
            [length(live_entries) | Process.get(:promoted_compaction_page_sizes, [])]
          )
        end)

        assert {:ok, _state} = ShardCompound.compact_dedicated_result(state, redis_key, dir)
        page_sizes = Process.get(:promoted_compaction_page_sizes)
        assert Enum.sum(page_sizes) == 1_100
        assert Enum.max(page_sizes) <= 256
      after
        Process.delete(:ferricstore_promoted_compaction_after_collect_hook)
        Process.delete(:promoted_compaction_page_sizes)
        File.rm_rf(dir)
        :ets.delete(member_index)
        :ets.delete(keydir)
      end
    end

    test "dedicated compaction fails closed when the exact member catalog is unready" do
      keydir = new_keydir()
      member_index = :ets.new(:promoted_compaction_unready_members, [:ordered_set, :public])
      redis_key = "hash:promoted:unready-compaction"

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-promoted-unready-#{System.unique_integer([:positive])}"
        )

      try do
        File.mkdir_p!(dir)
        old_log = Path.join(dir, "00000.log")
        File.write!(old_log, "old-bytes")
        :ets.insert(keydir, {Promotion.marker_key(redis_key), "hash", 0, LFU.initial(), 0, 0, 4})

        state = %{
          index: 0,
          keydir: keydir,
          compound_member_index: member_index,
          instance_ctx: nil,
          promoted_instances: %{
            redis_key => %{
              path: dir,
              total_bytes: 2_200_000,
              dead_bytes: 1_200_000,
              last_compacted_at: nil
            }
          }
        }

        assert {:error, _state} = ShardCompound.compact_dedicated_result(state, redis_key, dir)
        assert File.read!(old_log) == "old-bytes"
      after
        File.rm_rf(dir)
        :ets.delete(member_index)
        :ets.delete(keydir)
      end
    end

    test "committed maintenance applies exact append and reclaimable-byte deltas" do
      redis_key = "hash:promoted:committed"

      state = %{
        promoted_instances: %{
          redis_key => %{
            path: "/tmp/promoted-committed",
            writes: 2,
            total_bytes: 100,
            dead_bytes: 7,
            last_compacted_at: nil
          }
        }
      }

      state =
        ShardCompound.apply_promoted_maintenance(state, redis_key, %{
          appended_bytes: 41,
          reclaimable_bytes: 29,
          writes: 1
        })

      assert %{
               writes: 3,
               total_bytes: 141,
               dead_bytes: 36,
               last_compacted_at: nil
             } = state.promoted_instances[redis_key]
    end

    test "committed maintenance ignores collections removed before delivery" do
      state = %{promoted_instances: %{}}

      assert state ==
               ShardCompound.apply_promoted_maintenance(state, "removed", %{
                 appended_bytes: 41,
                 reclaimable_bytes: 29,
                 writes: 1
               })
    end

    test "overwrite counts old empty compound records" do
      keydir = new_keydir()
      redis_key = "hash:promoted"
      compound_key = "H:#{redis_key}\0field"

      try do
        :ets.insert(keydir, {compound_key, nil, 0, LFU.initial(), 0, 10, 0})

        state = %{
          keydir: keydir,
          promoted_instances: %{redis_key => %{total_bytes: 100, dead_bytes: 4}}
        }

        state = ShardCompound.track_promoted_dead_bytes(state, redis_key, compound_key, 12)
        info = state.promoted_instances[redis_key]

        assert info.total_bytes == 112
        assert info.dead_bytes == 4 + @record_header_size + byte_size(compound_key)
      after
        :ets.delete(keydir)
      end
    end

    test "delete counts old empty compound records" do
      keydir = new_keydir()
      redis_key = "hash:promoted:delete"
      compound_key = "H:#{redis_key}\0field"

      try do
        :ets.insert(keydir, {compound_key, nil, 0, LFU.initial(), 0, 10, 0})

        state = %{
          keydir: keydir,
          promoted_instances: %{redis_key => %{total_bytes: 100, dead_bytes: 6}}
        }

        state = ShardCompound.track_promoted_delete_bytes(state, redis_key, compound_key)
        info = state.promoted_instances[redis_key]
        tombstone_size = @record_header_size + byte_size(compound_key)

        assert info.total_bytes == 100 + tombstone_size
        assert info.dead_bytes == 6 + tombstone_size + tombstone_size
      after
        :ets.delete(keydir)
      end
    end

    test "failed dedicated compaction preserves retry accounting" do
      keydir = new_keydir()
      redis_key = "hash:promoted:compact:failed"
      compound_key = CompoundKey.hash_field(redis_key, "field")

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-promoted-compact-fail-#{System.unique_integer([:positive])}"
        )

      original_total = 2_200_000
      original_dead = 1_200_000

      try do
        File.mkdir_p!(dir)
        File.touch!(Path.join(dir, "00000.log"))

        :ets.insert(keydir, {Promotion.marker_key(redis_key), "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(keydir, {compound_key, "value", 0, LFU.initial(), 0, 32, 5})

        state = %{
          index: 0,
          keydir: keydir,
          compound_member_index: ready_member_index(compound_key),
          instance_ctx: nil,
          promoted_instances: %{
            redis_key => %{
              path: dir,
              total_bytes: original_total,
              dead_bytes: original_dead,
              last_compacted_at: nil
            }
          }
        }

        Process.put(:ferricstore_promoted_compaction_after_collect_hook, fn ^redis_key,
                                                                            _live_entries ->
          # Force v2_append_batch/2 to fail without touching unrelated paths.
          new_log = Path.join(dir, "00001.log")
          File.rm(new_log)
          File.mkdir!(new_log)
        end)

        assert {:error, after_state} =
                 ShardCompound.compact_dedicated_result(state, redis_key, dir)

        info = after_state.promoted_instances[redis_key]

        assert info.dead_bytes == original_dead
        assert info.total_bytes == original_total
        assert info.last_compacted_at == nil
      after
        Process.delete(:ferricstore_promoted_compaction_after_collect_hook)
        File.rm_rf(dir)
        :ets.delete(keydir)
      end
    end

    test "failed dedicated compaction reports rollback cleanup failure" do
      keydir = new_keydir()
      redis_key = "hash:promoted:compact:rollback_cleanup_failed"
      compound_key = CompoundKey.hash_field(redis_key, "field")

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-promoted-compact-rollback-fail-#{System.unique_integer([:positive])}"
        )

      try do
        File.mkdir_p!(dir)
        File.touch!(Path.join(dir, "00000.log"))

        :ets.insert(keydir, {Promotion.marker_key(redis_key), "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(keydir, {compound_key, "value", 0, LFU.initial(), 0, 32, 5})

        state = %{
          index: 0,
          keydir: keydir,
          compound_member_index: ready_member_index(compound_key),
          instance_ctx: nil,
          promoted_instances: %{
            redis_key => %{
              path: dir,
              total_bytes: 2_200_000,
              dead_bytes: 1_200_000,
              last_compacted_at: nil
            }
          }
        }

        Process.put(:ferricstore_promoted_compaction_after_collect_hook, fn ^redis_key,
                                                                            _live_entries ->
          new_log = Path.join(dir, "00001.log")
          File.rm(new_log)
          File.mkdir!(new_log)
        end)

        log =
          capture_log(fn ->
            assert {:error, _after_state} =
                     ShardCompound.compact_dedicated_result(state, redis_key, dir)
          end)

        assert log =~ "dedicated compaction rollback failed to remove new active file"
      after
        Process.delete(:ferricstore_promoted_compaction_after_collect_hook)
        File.rm_rf(dir)
        :ets.delete(keydir)
      end
    end

    test "dedicated compaction read failure reports rollback cleanup failure" do
      keydir = new_keydir()
      redis_key = "hash:promoted:compact:read_rollback_cleanup_failed"
      compound_key = CompoundKey.hash_field(redis_key, "missing_cold")

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-promoted-compact-read-rollback-fail-#{System.unique_integer([:positive])}"
        )

      try do
        File.mkdir_p!(dir)
        File.touch!(Path.join(dir, "00000.log"))

        :ets.insert(keydir, {Promotion.marker_key(redis_key), "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(keydir, {compound_key, nil, 0, LFU.initial(), 99, 0, 128})

        state = %{
          index: 0,
          keydir: keydir,
          compound_member_index: ready_member_index(compound_key),
          instance_ctx: nil,
          promoted_instances: %{
            redis_key => %{
              path: dir,
              total_bytes: 2_200_000,
              dead_bytes: 1_200_000,
              last_compacted_at: nil
            }
          }
        }

        Process.put(:ferricstore_promoted_compaction_fsync_dir_hook, fn ^dir ->
          new_log = Path.join(dir, "00001.log")

          if File.regular?(new_log) do
            File.rm!(new_log)
            File.mkdir!(new_log)
          end

          :ok
        end)

        log =
          capture_log(fn ->
            assert {:error, _after_state} =
                     ShardCompound.compact_dedicated_result(state, redis_key, dir)
          end)

        assert log =~ "dedicated compaction rollback failed to remove new active file"
      after
        Process.delete(:ferricstore_promoted_compaction_fsync_dir_hook)
        File.rm_rf(dir)
        :ets.delete(keydir)
      end
    end

    test "dedicated compaction dir fsync failure preserves retry accounting" do
      keydir = new_keydir()
      redis_key = "hash:promoted:compact:fsync_failed"
      compound_key = CompoundKey.hash_field(redis_key, "field")

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-promoted-compact-fsync-fail-#{System.unique_integer([:positive])}"
        )

      original_total = 2_200_000
      original_dead = 1_200_000

      try do
        File.mkdir_p!(dir)
        File.touch!(Path.join(dir, "00000.log"))

        :ets.insert(keydir, {Promotion.marker_key(redis_key), "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(keydir, {compound_key, "value", 0, LFU.initial(), 0, 32, 5})

        state = %{
          index: 0,
          keydir: keydir,
          compound_member_index: ready_member_index(compound_key),
          instance_ctx: nil,
          promoted_instances: %{
            redis_key => %{
              path: dir,
              total_bytes: original_total,
              dead_bytes: original_dead,
              last_compacted_at: nil
            }
          }
        }

        Process.put(:ferricstore_promoted_compaction_fsync_dir_hook, fn ^dir ->
          {:error, :eio}
        end)

        assert {:error, after_state} =
                 ShardCompound.compact_dedicated_result(state, redis_key, dir)

        info = after_state.promoted_instances[redis_key]

        assert info.dead_bytes == original_dead
        assert info.total_bytes == original_total
        assert info.last_compacted_at == nil
      after
        Process.delete(:ferricstore_promoted_compaction_fsync_dir_hook)
        File.rm_rf(dir)
        :ets.delete(keydir)
      end
    end

    test "dedicated compaction old-log removal failure preserves retry accounting" do
      keydir = new_keydir()
      redis_key = "hash:promoted:compact:remove_failed"
      compound_key = CompoundKey.hash_field(redis_key, "field")

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-promoted-compact-remove-fail-#{System.unique_integer([:positive])}"
        )

      original_total = 2_200_000
      original_dead = 1_200_000
      stale_dir = Path.join(dir, "00000.log")

      try do
        File.mkdir_p!(dir)
        File.mkdir!(stale_dir)
        File.touch!(Path.join(dir, "00001.log"))

        :ets.insert(keydir, {Promotion.marker_key(redis_key), "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(keydir, {compound_key, "value", 0, LFU.initial(), 1, 0, 5})

        state = %{
          index: 0,
          keydir: keydir,
          compound_member_index: ready_member_index(compound_key),
          instance_ctx: nil,
          promoted_instances: %{
            redis_key => %{
              path: dir,
              total_bytes: original_total,
              dead_bytes: original_dead,
              last_compacted_at: nil
            }
          }
        }

        assert {:error, after_state} =
                 ShardCompound.compact_dedicated_result(state, redis_key, dir)

        info = after_state.promoted_instances[redis_key]

        assert File.dir?(stale_dir)
        assert info.dead_bytes == original_dead
        assert info.total_bytes == original_total
        assert info.last_compacted_at == nil
      after
        File.rm_rf(dir)
        :ets.delete(keydir)
      end
    end

    test "dedicated compaction active-file fsync failure preserves retry accounting" do
      keydir = new_keydir()
      redis_key = "hash:promoted:compact:file_fsync_failed"
      compound_key = CompoundKey.hash_field(redis_key, "field")

      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-promoted-compact-file-fsync-fail-#{System.unique_integer([:positive])}"
        )

      original_total = 2_200_000
      original_dead = 1_200_000

      try do
        File.mkdir_p!(dir)
        active = Path.join(dir, "00000.log")
        File.touch!(active)

        :ets.insert(keydir, {Promotion.marker_key(redis_key), "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(keydir, {compound_key, "value", 0, LFU.initial(), 0, 32, 5})

        state = %{
          index: 0,
          keydir: keydir,
          compound_member_index: ready_member_index(compound_key),
          instance_ctx: nil,
          promoted_instances: %{
            redis_key => %{
              path: dir,
              total_bytes: original_total,
              dead_bytes: original_dead,
              last_compacted_at: nil
            }
          }
        }

        Process.put(:ferricstore_promoted_compaction_fsync_file_hook, fn ^active ->
          {:error, :eio}
        end)

        assert {:error, after_state} =
                 ShardCompound.compact_dedicated_result(state, redis_key, dir)

        info = after_state.promoted_instances[redis_key]

        assert info.dead_bytes == original_dead
        assert info.total_bytes == original_total
        assert info.last_compacted_at == nil
      after
        Process.delete(:ferricstore_promoted_compaction_fsync_file_hook)
        File.rm_rf(dir)
        :ets.delete(keydir)
      end
    end
  end

  defp new_keydir do
    :ets.new(:"shard_accounting_#{System.unique_integer([:positive])}", [:set, :public])
  end

  defp source_offset!(source, pattern) do
    case :binary.match(source, pattern) do
      {offset, _length} -> offset
      :nomatch -> flunk("expected source to contain #{inspect(pattern)}")
    end
  end

  defp ready_member_index(compound_key) do
    table = :ets.new(:promoted_compaction_member_fixture, [:ordered_set, :public])
    CompoundMemberIndex.reset(table)
    CompoundMemberIndex.put(table, compound_key)

    on_exit(fn ->
      if :ets.info(table) != :undefined, do: :ets.delete(table)
    end)

    table
  end
end
