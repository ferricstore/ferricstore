defmodule Ferricstore.Store.RouterColdEmptyTest.Sections.ExistsRejectsColdRowsInvalidOffsets do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Store.{CompoundKey, LFU}
      alias Ferricstore.Store.Router
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.WARaftSegmentReader
      alias Ferricstore.Stats
      alias Ferricstore.Test.IsolatedInstance

  test "exists rejects cold rows with invalid offsets", %{ctx: ctx, keydir: keydir} do
    key = "cold_invalid_exists:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    refute Router.exists?(ctx, key)
    refute Router.exists_fast?(ctx, key)
    assert [] == :ets.lookup(keydir, key)
  end

  test "expire_at rejects cold rows with invalid offsets", %{ctx: ctx, keydir: keydir} do
    key = "cold_invalid_expire_at:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert nil == Router.expire_at_ms(ctx, key)
    assert [] == :ets.lookup(keydir, key)
  end

  test "expire_at_ms preserves live pending cold rows", %{ctx: ctx, keydir: keydir} do
    key = "cold_pending_expire_at:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    expire_at_ms = System.system_time(:millisecond) + 60_000
    :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), :pending, 0, 8192})

    assert expire_at_ms == Router.expire_at_ms(ctx, key)
    assert [{^key, nil, ^expire_at_ms, _lfu, :pending, 0, 8192}] = :ets.lookup(keydir, key)
  end
    end
  end
end
