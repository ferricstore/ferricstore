defmodule Ferricstore.Store.RouterWarmTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Router

  test "cold-read warm only updates the same disk location" do
    keydir = new_keydir()
    key = "router:warm:same-location"
    ctx = ctx()

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 4, 10, 3})

      Router.warm_ets_after_cold_read(ctx, keydir, key, "old", 4, 10)

      assert [{^key, "old", 0, _lfu, 4, 10, 3}] = :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  test "stale cold-read warm does not update a pending write" do
    keydir = new_keydir()
    key = "router:warm:pending"
    ctx = ctx()

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), :pending, 4, 3})

      Router.warm_ets_after_cold_read(ctx, keydir, key, "old", 4, 10)

      assert [{^key, nil, 0, _lfu, :pending, 4, 3}] = :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  test "stale cold-read warm does not update a newer disk location" do
    keydir = new_keydir()
    key = "router:warm:newer-location"
    ctx = ctx()

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 5, 20, 3})

      Router.warm_ets_after_cold_read(ctx, keydir, key, "old", 4, 10)

      assert [{^key, nil, 0, _lfu, 5, 20, 3}] = :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  defp ctx do
    %{
      hot_cache_max_value_size: 64,
      pressure_flags: :atomics.new(3, [])
    }
  end

  defp new_keydir do
    :ets.new(:"router_warm_#{System.unique_integer([:positive])}", [:set, :public])
  end
end
