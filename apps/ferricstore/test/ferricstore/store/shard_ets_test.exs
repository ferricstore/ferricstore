defmodule Ferricstore.Store.ShardETSTest do
  @moduledoc false
  # Cold-read warming consults the global MemoryGuard skip-promotion flag, so
  # these tests must not race modules that intentionally force memory pressure.
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Reads, as: ShardReads
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.reset_memory_guard_pressure()
    :ok
  end

  test "fresh no-ttl location batch inserts records and batches binary accounting" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])
    ref = :atomics.new(1, signed: true)
    key1 = "ets:fresh-batch:1"
    key2 = "ets:fresh-batch:2"
    value1 = String.duplicate("a", 128)
    value2 = String.duplicate("b", 96)

    state = %{
      keydir: keydir,
      index: 0,
      instance_ctx: %{hot_cache_max_value_size: 512, keydir_binary_bytes: ref, shard_count: 1}
    }

    try do
      assert {:ok, 2} =
               ShardETS.ets_insert_fresh_no_expiry_many_with_location(
                 state,
                 [{key1, value1, 0}, {key2, value2, 0}],
                 {:waraft_segment, 10},
                 123,
                 512
               )

      assert [{^key1, ^value1, 0, _lfu1, {:waraft_segment, 10}, 123, 128}] =
               :ets.lookup(keydir, key1)

      assert [{^key2, ^value2, 0, _lfu2, {:waraft_segment, 10}, 123, 96}] =
               :ets.lookup(keydir, key2)

      assert :atomics.get(ref, 1) == byte_size(value1) + byte_size(value2)
    after
      :ets.delete(keydir)
    end
  end

  test "fresh no-ttl location batch falls back when it would overwrite or clear compound data" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])
    key = "ets:fresh-batch:fallback"
    compound_key = "ets:fresh-batch:compound"

    state = %{
      keydir: keydir,
      index: 0,
      instance_ctx: %{
        hot_cache_max_value_size: 512,
        keydir_binary_bytes: :atomics.new(1, signed: true),
        shard_count: 1
      }
    }

    try do
      :ets.insert(keydir, {key, "old", 0, LFU.initial(), 1, 2, 3})
      :ets.insert(keydir, {CompoundKey.type_key(compound_key), "hash", 0, LFU.initial(), 1, 4, 4})

      assert :fallback =
               ShardETS.ets_insert_fresh_no_expiry_many_with_location(
                 state,
                 [{key, "new", 0}],
                 {:waraft_segment, 11},
                 456,
                 512
               )

      assert :fallback =
               ShardETS.ets_insert_fresh_no_expiry_many_with_location(
                 state,
                 [{compound_key, "new", 0}],
                 {:waraft_segment, 11},
                 456,
                 512
               )
    after
      :ets.delete(keydir)
    end
  end

  test "stale async cold-read completion does not warm over a pending large write" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])
    key = "ets:pending:stale-read"

    state = %{
      keydir: keydir,
      instance_ctx: %{hot_cache_max_value_size: 5}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 7, 12, 3})

      ShardETS.ets_insert(state, key, "new-large-value", 0)
      assert [{^key, nil, 0, _lfu, :pending, 7, 3}] = :ets.lookup(keydir, key)

      assert :ok == ShardETS.cold_read_warm_ets(state, key, "old")

      assert [{^key, nil, 0, _lfu, :pending, 7, 3}] = :ets.lookup(keydir, key)
      assert :miss == ShardETS.ets_lookup(state, key)
      assert ShardETS.pending_cold?(state, key)
    after
      :ets.delete(keydir)
    end
  end

  test "shard GET reads cold WARaft apply-projection locations without Bitcask file path conversion" do
    unique = System.unique_integer([:positive])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_shard_apply_projection_get_#{unique}")
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    keydir = :ets.new(:"shard_ets_apply_projection_get_#{unique}", [:set, :public])
    key = "ets:cold:apply-projection-get"
    value = "apply-projection-value"
    projection_index = 101

    File.mkdir_p!(shard_path)

    try do
      assert :ok =
               Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                 data_dir,
                 0,
                 projection_index,
                 [{key, value, 0}]
               )

      :ets.insert(
        keydir,
        {key, nil, 0, LFU.initial(), {:waraft_apply_projection, projection_index}, 0,
         byte_size(value)}
      )

      state = %{
        keydir: keydir,
        index: 0,
        data_dir: data_dir,
        shard_data_path: shard_path,
        instance_ctx: %{data_dir: data_dir, hot_cache_max_value_size: 64, shard_count: 1}
      }

      assert {:reply, [^key], ^state} = ShardReads.handle_keys(state)
      assert {:reply, ^value, ^state} = ShardReads.handle_get(key, state)
      assert {:reply, ^value, ^state} = ShardReads.handle_get(key, {self(), make_ref()}, state)
    after
      :ets.delete(keydir)
      File.rm_rf(data_dir)
    end
  end

  test "stale direct cold-read completion does not warm over a pending write" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])
    key = "ets:pending:stale-direct-read"

    state = %{
      keydir: keydir,
      instance_ctx: %{hot_cache_max_value_size: 64}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 7, 12, 3})

      ShardETS.ets_insert(state, key, "new-large-value", 0)
      assert [{^key, "new-large-value", 0, _lfu, :pending, 7, 3}] = :ets.lookup(keydir, key)

      assert :ok == ShardETS.cold_read_warm_ets(state, key, "old", 0, 7, 12, 3)

      assert [{^key, "new-large-value", 0, _lfu, :pending, 7, 3}] = :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  test "direct cold-read completion warms when location still matches" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])
    key = "ets:cold:direct-read"

    state = %{
      keydir: keydir,
      instance_ctx: %{hot_cache_max_value_size: 64}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 7, 12, 3})

      assert true == ShardETS.cold_read_warm_ets(state, key, "old", 0, 7, 12, 3)

      assert [{^key, "old", 0, _lfu, 7, 12, 3}] = :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  test "warm lookup rejects malformed cold location without calling NIF" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])
    key = "ets:cold:bad-offset"

    state = %{
      keydir: keydir,
      shard_data_path: System.tmp_dir!(),
      instance_ctx: %{hot_cache_max_value_size: 64}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 3})

      assert :expired == ShardETS.ets_lookup_warm(state, key)
      assert [] == :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  test "warm lookup rejects mismatched cold offsets" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])

    dir =
      Path.join(System.tmp_dir!(), "shard_ets_warm_stale_#{System.unique_integer([:positive])}")

    key = "ets:warm:stale-offset"
    other_key = "ets:warm:other-offset"
    path = ShardETS.file_path(dir, 0)

    state = %{
      keydir: keydir,
      shard_data_path: dir,
      instance_ctx: %{hot_cache_max_value_size: 64}
    }

    try do
      File.mkdir_p!(dir)

      {:ok, [{other_offset, _}, {_key_offset, value_size}]} =
        NIF.v2_append_batch(path, [{other_key, "wrong-value", 0}, {key, "right-value", 0}])

      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, other_offset, value_size})

      assert :miss == ShardETS.ets_lookup_warm(state, key)
      assert [{^key, nil, 0, _lfu, 0, ^other_offset, ^value_size}] = :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "warm_from_store rejects malformed cold location without calling NIF" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])
    key = "ets:warm-store:bad-offset"

    state = %{
      keydir: keydir,
      shard_data_path: System.tmp_dir!(),
      instance_ctx: %{hot_cache_max_value_size: 64}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 3})

      assert nil == ShardETS.warm_from_store(state, key)
      assert [] == :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  test "warm_from_store rejects mismatched cold offsets" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])

    dir =
      Path.join(System.tmp_dir!(), "shard_ets_store_stale_#{System.unique_integer([:positive])}")

    key = "ets:warm-store:stale-offset"
    other_key = "ets:warm-store:other-offset"
    path = ShardETS.file_path(dir, 0)

    state = %{
      keydir: keydir,
      shard_data_path: dir,
      instance_ctx: %{hot_cache_max_value_size: 64}
    }

    try do
      File.mkdir_p!(dir)

      {:ok, [{other_offset, _}, {_key_offset, value_size}]} =
        NIF.v2_append_batch(path, [{other_key, "wrong-value", 0}, {key, "right-value", 0}])

      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, other_offset, value_size})

      assert nil == ShardETS.warm_from_store(state, key)
      assert [{^key, nil, 0, _lfu, 0, ^other_offset, ^value_size}] = :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "prefix scan skips malformed cold locations without calling NIF" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])
    key = "H:bad-scan" <> <<0>> <> "field"

    state = %{
      keydir: keydir,
      instance_ctx: %{hot_cache_max_value_size: 64}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 3})

      assert [] == ShardETS.prefix_scan_entries(state, "H:bad-scan", System.tmp_dir!())
      assert [] == :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  test "prefix scan rejects mismatched cold offsets" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])

    dir =
      Path.join(System.tmp_dir!(), "shard_ets_prefix_stale_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)

    prefix = "H:stale-scan"
    key = prefix <> <<0>> <> "field"
    other_key = "H:other-scan" <> <<0>> <> "field"
    path = ShardETS.file_path(dir, 0)

    try do
      {:ok, [{other_offset, _}, {_key_offset, value_size}]} =
        NIF.v2_append_batch(path, [{other_key, "wrong-value", 0}, {key, "right-value", 0}])

      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, other_offset, value_size})

      assert [] == ShardETS.prefix_scan_entries(keydir, prefix, dir)
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "prefix count skips malformed cold locations" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])
    key = "H:bad-count" <> <<0>> <> "field"

    state = %{
      keydir: keydir,
      instance_ctx: %{hot_cache_max_value_size: 64}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 3})

      assert 0 == ShardETS.prefix_count_entries(state, "H:bad-count")
      assert [] == :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  test "local transaction read rejects malformed cold location without calling NIF" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])
    key = "ets:tx-read:bad-offset"

    state = %{
      keydir: keydir,
      shard_data_path: System.tmp_dir!(),
      instance_ctx: %{hot_cache_max_value_size: 64}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 3})

      assert {:ok, nil} == ShardReads.v2_local_read(state, key)
      assert [] == :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  test "live keys skip malformed cold locations" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])
    good = "ets:keys:good"
    bad = "ets:keys:bad-offset"

    state = %{
      keydir: keydir,
      shard_data_path: System.tmp_dir!(),
      instance_ctx: %{hot_cache_max_value_size: 64}
    }

    try do
      :ets.insert(keydir, {good, "value", 0, LFU.initial(), :pending, 0, 0})
      :ets.insert(keydir, {bad, nil, 0, LFU.initial(), 0, :pending_offset, 3})

      assert [^good] = ShardReads.live_keys(state)
      assert [] == :ets.lookup(keydir, bad)
    after
      :ets.delete(keydir)
    end
  end

  test "handle_keys reads ETS without forcing pending writes to disk" do
    keydir = :ets.new(:"shard_ets_#{System.unique_integer([:positive])}", [:set, :public])
    key = "ets:keys:pending-hot"

    state = %{
      index: 0,
      keydir: keydir,
      pending: [{key, "value", 0}],
      pending_count: 1,
      flush_in_flight: nil,
      instance_ctx: nil
    }

    try do
      :ets.insert(keydir, {key, "value", 0, LFU.initial(), :pending, 0, 5})

      assert {:reply, [^key], ^state} = ShardReads.handle_keys(state)
    after
      :ets.delete(keydir)
    end
  end
end
