defmodule Ferricstore.Store.ShardETSTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

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
end
