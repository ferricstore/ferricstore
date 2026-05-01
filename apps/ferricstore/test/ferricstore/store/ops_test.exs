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
        assert [{^key, "new", ^expire_at_ms, _lfu, :pending, 0, 0}] = :ets.lookup(keydir, key)
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
end
