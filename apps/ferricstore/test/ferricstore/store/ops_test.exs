defmodule Ferricstore.Store.OpsTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.LocalTxStore
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.Router
  alias Ferricstore.Bitcask.NIF

  @ops_path Path.expand("../../../lib/ferricstore/store/ops.ex", __DIR__)

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
    test "local compound batch reads use one cold pread batch" do
      source = File.read!(@ops_path)

      assert source =~ "ColdRead.pread_batch",
             "LocalTxStore compound_batch_get must batch cold reads instead of one waiter per field"
    end

    test "compound_batch_get returns ordered cold values and warms matching ETS entries" do
      ctx = FerricStore.Instance.get(:default)
      redis_key = "ops:local_tx:batch-cold:#{System.unique_integer([:positive])}"

      keys = [
        "H:" <> redis_key <> <<0>> <> "a",
        "H:" <> redis_key <> <<0>> <> "b",
        "H:" <> redis_key <> <<0>> <> "c"
      ]

      shard_index = Router.shard_for(ctx, redis_key)

      dir =
        Path.join(System.tmp_dir!(), "ops_local_tx_batch_#{System.unique_integer([:positive])}")

      keydir = :ets.new(:"ops_local_tx_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        File.mkdir_p!(dir)
        path = Path.join(dir, "00000.log")
        File.touch!(path)

        assert {:ok, [{off_a, size_a}, {off_b, size_b}]} =
                 NIF.v2_append_batch_nosync(path, [
                   {Enum.at(keys, 0), "va", 0},
                   {Enum.at(keys, 1), "vb", 0}
                 ])

        :ets.insert(keydir, {Enum.at(keys, 0), nil, 0, LFU.initial(), 0, off_a, size_a})
        :ets.insert(keydir, {Enum.at(keys, 1), nil, 0, LFU.initial(), 0, off_b, size_b})

        tx =
          local_tx(ctx, shard_index, keydir, %{})
          |> put_in([Access.key!(:shard_state), :shard_data_path], dir)

        assert ["va", nil, "vb"] ==
                 Ops.compound_batch_get(tx, redis_key, [
                   Enum.at(keys, 0),
                   Enum.at(keys, 2),
                   Enum.at(keys, 1)
                 ])

        assert [{_, "va", 0, _lfu, 0, ^off_a, ^size_a}] = :ets.lookup(keydir, Enum.at(keys, 0))
        assert [{_, "vb", 0, _lfu, 0, ^off_b, ^size_b}] = :ets.lookup(keydir, Enum.at(keys, 1))
      after
        :ets.delete(keydir)
        File.rm_rf(dir)
      end
    end

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
