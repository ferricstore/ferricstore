defmodule Ferricstore.Store.ShardETSTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  describe "pending_cold?/2" do
    test "releases tracked key bytes when deleting an expired pending-cold entry" do
      keydir = :ets.new(:"shard_ets_test_#{System.unique_integer([:positive])}", [:set, :public])
      ref = :atomics.new(1, [])
      key = :binary.copy("k", 128)
      expired_at = Ferricstore.HLC.now_ms() - 1

      state = %{keydir: keydir, index: 0, instance_ctx: %{keydir_binary_bytes: ref}}

      try do
        :ets.insert(keydir, {key, nil, expired_at, 0, :pending, 0, 0})
        :atomics.add(ref, 1, byte_size(key))

        refute ShardETS.pending_cold?(state, key)
        assert :ets.lookup(keydir, key) == []
        assert :atomics.get(ref, 1) == 0
      after
        :ets.delete(keydir)
      end
    end
  end
end
