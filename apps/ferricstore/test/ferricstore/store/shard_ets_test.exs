defmodule Ferricstore.Store.ShardETSTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Reads, as: ShardReads

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
