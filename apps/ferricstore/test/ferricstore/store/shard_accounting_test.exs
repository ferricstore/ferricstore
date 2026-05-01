defmodule Ferricstore.Store.ShardAccountingTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

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
  end

  defp new_keydir do
    :ets.new(:"shard_accounting_#{System.unique_integer([:positive])}", [:set, :public])
  end
end
