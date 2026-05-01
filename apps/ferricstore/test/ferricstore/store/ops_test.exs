defmodule Ferricstore.Store.OpsTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.LocalTxStore
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.Router

  describe "LocalTxStore SET" do
    test "KEEPTTL preserves cold key TTL without reading the old value" do
      ctx = FerricStore.Instance.get(:default)
      key = "ops:local_tx:keepttl:#{System.unique_integer([:positive])}"
      shard_index = Router.shard_for(ctx, key)
      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])
      expire_at_ms = System.os_time(:millisecond) + 60_000

      try do
        :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), 99, 123, 3})

        tx = %LocalTxStore{
          instance_ctx: ctx,
          shard_index: shard_index,
          shard_state: %{
            instance_ctx: ctx,
            keydir: keydir,
            index: shard_index,
            shard_data_path: System.tmp_dir!(),
            data_dir: System.tmp_dir!(),
            promoted_instances: %{}
          }
        }

        assert :ok == Ops.set(tx, key, "new", set_opts(%{keepttl: true}))

        assert [{^key, "new", ^expire_at_ms, _lfu, :pending, 99, 3}] =
                 :ets.lookup(keydir, key)
      after
        :ets.delete(keydir)
      end
    end
  end

  describe "LocalTxStore promoted compound reads" do
    test "compound_get rejects malformed promoted cold location without calling NIF" do
      ctx = FerricStore.Instance.get(:default)
      redis_key = "ops:local_tx:promoted:#{System.unique_integer([:positive])}"
      compound_key = "H:" <> redis_key <> <<0>> <> "field"
      shard_index = Router.shard_for(ctx, redis_key)
      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        :ets.insert(keydir, {compound_key, nil, 0, LFU.initial(), 0, :pending_offset, 3})

        tx =
          local_tx(ctx, shard_index, keydir, %{
            redis_key => %{path: System.tmp_dir!()}
          })

        assert nil == Ops.compound_get(tx, redis_key, compound_key)
        assert [] == :ets.lookup(keydir, compound_key)
      after
        :ets.delete(keydir)
      end
    end

    test "compound_get_meta rejects malformed promoted cold location without calling NIF" do
      ctx = FerricStore.Instance.get(:default)
      redis_key = "ops:local_tx:promoted-meta:#{System.unique_integer([:positive])}"
      compound_key = "H:" <> redis_key <> <<0>> <> "field"
      shard_index = Router.shard_for(ctx, redis_key)
      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        :ets.insert(keydir, {compound_key, nil, 0, LFU.initial(), 0, :pending_offset, 3})

        tx =
          local_tx(ctx, shard_index, keydir, %{
            redis_key => %{path: System.tmp_dir!()}
          })

        assert nil == Ops.compound_get_meta(tx, redis_key, compound_key)
        assert [] == :ets.lookup(keydir, compound_key)
      after
        :ets.delete(keydir)
      end
    end
  end

  defp set_opts(overrides) do
    Map.merge(
      %{expire_at_ms: 0, nx: false, xx: false, get: false, keepttl: false, has_expiry: false},
      overrides
    )
  end

  defp local_tx(ctx, shard_index, keydir, promoted_instances) do
    %LocalTxStore{
      instance_ctx: ctx,
      shard_index: shard_index,
      shard_state: %{
        instance_ctx: ctx,
        keydir: keydir,
        index: shard_index,
        shard_data_path: System.tmp_dir!(),
        data_dir: System.tmp_dir!(),
        promoted_instances: promoted_instances
      }
    }
  end
end
