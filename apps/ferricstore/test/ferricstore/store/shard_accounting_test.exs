defmodule Ferricstore.Store.ShardAccountingTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
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
