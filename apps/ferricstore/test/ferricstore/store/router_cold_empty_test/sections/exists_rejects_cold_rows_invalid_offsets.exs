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

      test "exists preserves live cold rows with invalid offsets", %{ctx: ctx, keydir: keydir} do
        key = "cold_invalid_exists:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

        assert Router.exists?(ctx, key)
        assert Router.exists_fast?(ctx, key)
        assert [{^key, nil, 0, _lfu, 0, :pending_offset, 5}] = :ets.lookup(keydir, key)
      end

      test "expire_at reads independent metadata without deleting an invalid location", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "cold_invalid_expire_at:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

        assert 0 == Router.expire_at_ms(ctx, key)
        assert [{^key, nil, 0, _lfu, 0, :pending_offset, 5}] = :ets.lookup(keydir, key)
      end

      test "expire_at_ms preserves live pending cold rows", %{ctx: ctx, keydir: keydir} do
        key = "cold_pending_expire_at:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        expire_at_ms = System.system_time(:millisecond) + 60_000
        :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), :pending, 0, 8192})

        assert expire_at_ms == Router.expire_at_ms(ctx, key)
        assert [{^key, nil, ^expire_at_ms, _lfu, :pending, 0, 8192}] = :ets.lookup(keydir, key)
      end

      test "object_lfu reads independent metadata without deleting an invalid location", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "cold_invalid_lfu:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        lfu = LFU.initial()
        :ets.insert(keydir, {key, nil, 0, lfu, 0, :pending_offset, 5})

        assert lfu == Router.object_lfu(ctx, key)
        assert [{^key, nil, 0, ^lfu, 0, :pending_offset, 5}] = :ets.lookup(keydir, key)
      end

      test "key enumeration fails closed without deleting rows whose expiry is invalid", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "invalid_expiry_keys:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        before_size = Router.dbsize(ctx)
        assert :ok = Router.put(ctx, key, "value", 0)

        [{^key, value, 0, lfu, file_id, offset, value_size}] = :ets.lookup(keydir, key)
        row = {key, value, -1, lfu, file_id, offset, value_size}
        :ets.insert(keydir, row)

        assert {:error, {:storage_read_failed, {:invalid_keydir_entry, ^key, [^row]}}} =
                 Router.keys(ctx)

        assert before_size + 1 == Router.dbsize(ctx)
        assert [^row] = :ets.lookup(keydir, key)
      end
    end
  end
end
