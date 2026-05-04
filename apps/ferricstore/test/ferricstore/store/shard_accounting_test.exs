defmodule Ferricstore.Store.ShardAccountingTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Store.{CompoundKey, LFU, Promotion}
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
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

    test "overwrite counts old empty records in file 0" do
      keydir = new_keydir()
      key = "accounting:empty:overwrite"

      try do
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, 8, 0})

        state = %{
          keydir: keydir,
          active_file_id: 1,
          file_stats: %{0 => {100, 2}, 1 => {0, 0}}
        }

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
        File.write!(Path.join(dir, "0.log"), :binary.copy("x", 100))
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
        File.write!(Path.join(dir, "0.log"), :binary.copy("x", 100))
        :ets.insert(keydir, {key, nil, expired_at, LFU.initial(), 0, 1, 0})

        stats = ShardFlush.compute_file_stats(dir, keydir)

        assert stats[0] == {100, 100}
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

        assert info.total_bytes == 100
        assert info.dead_bytes == 6 + @record_header_size + byte_size(compound_key)
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

        after_state = ShardCompound.bump_promoted_writes(state, redis_key)
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

        after_state = ShardCompound.bump_promoted_writes(state, redis_key)
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

        after_state = ShardCompound.bump_promoted_writes(state, redis_key)
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

        after_state = ShardCompound.bump_promoted_writes(state, redis_key)
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
end
