defmodule Ferricstore.Store.KeydirConcurrencyTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.Store.Keydir
  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "exact delete cannot remove a replacement committed after lookup" do
    table = new_keydir()
    key = "keydir-cas-delete"
    observed = {key, "old", 10, LFU.initial(), 1, 20, 3}
    replacement = {key, "new", 0, LFU.initial(), 2, 40, 3}

    try do
      true = :ets.insert(table, observed)
      true = :ets.insert(table, replacement)

      refute Keydir.delete_exact(table, observed)
      assert [^replacement] = :ets.lookup(table, key)
    after
      :ets.delete(table)
    end
  end

  test "exact replacement cannot resurrect a stale cold value" do
    table = new_keydir()
    key = "keydir-cas-warm"
    observed = {key, nil, 10, LFU.initial(), 1, 20, 3}
    committed = {key, "new", 0, LFU.initial(), 2, 40, 3}
    stale_warm = {key, "old", 10, LFU.initial(), 1, 20, 3}

    try do
      true = :ets.insert(table, observed)
      true = :ets.insert(table, committed)

      refute Keydir.replace_exact(table, observed, stale_warm)
      assert [^committed] = :ets.lookup(table, key)
    after
      :ets.delete(table)
    end
  end

  test "compaction relocation cannot overwrite a replacement committed after planning" do
    table = new_keydir()
    key = "keydir-cas-compaction"
    observed = {key, nil, 10, LFU.initial(), 1, 20, 3}
    committed = {key, "new", 0, LFU.initial(), 2, 40, 3}

    try do
      true = :ets.insert(table, observed)
      true = :ets.insert(table, committed)

      refute Keydir.relocate_exact(table, key, 1, 20, 80)
      assert [^committed] = :ets.lookup(table, key)
    after
      :ets.delete(table)
    end
  end

  test "compaction relocation atomically preserves the rest of the keydir row" do
    table = new_keydir()
    key = "keydir-cas-compaction-live"
    observed = {key, nil, 10, LFU.initial(), 1, 20, 3}
    relocated = put_elem(observed, 5, 80)

    try do
      true = :ets.insert(table, observed)

      assert Keydir.relocate_exact(table, key, 1, 20, 80)
      assert [^relocated] = :ets.lookup(table, key)
    after
      :ets.delete(table)
    end
  end

  test "expired cleanup cannot erase a replacement from the compound member catalog" do
    keydir = new_keydir()
    index = :ets.new(:keydir_compound_index_race, [:ordered_set, :public])
    :ok = CompoundMemberIndex.reset(index)
    prefix = CompoundKey.set_prefix("renewed-set")
    key = CompoundKey.set_member("renewed-set", "member")
    expired = {key, "old", 1, LFU.initial(), 1, 20, 3}
    replacement = {key, "new", 0, LFU.initial(), 2, 40, 3}
    state = %{keydir: keydir, compound_member_index: index}

    try do
      true = :ets.insert(keydir, expired)
      :ok = CompoundMemberIndex.put(index, key)

      Process.put(:ferricstore_after_exact_keydir_delete_hook, fn _state, ^expired ->
        true = :ets.insert(keydir, replacement)
        :ok = CompoundMemberIndex.put(index, key)
      end)

      assert ShardETS.delete_exact_entry(state, expired)
      assert [^replacement] = :ets.lookup(keydir, key)
      assert {:ok, [^key]} = CompoundMemberIndex.keys_for_prefix(index, prefix)
    after
      Process.delete(:ferricstore_after_exact_keydir_delete_hook)
      :ets.delete(index)
      :ets.delete(keydir)
    end
  end

  test "exact expiry deletion releases its WARaft apply-projection cache entry" do
    keydir = new_keydir()
    compound_index = :ets.new(:keydir_apply_projection_delete, [:ordered_set, :public])
    :ok = CompoundMemberIndex.reset(compound_index)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-keydir-apply-projection-#{System.unique_integer([:positive])}"
      )

    key = "keydir-apply-projection-expired"
    projection_index = 41

    expired =
      {key, nil, 1, LFU.initial(), {:waraft_apply_projection, projection_index}, 0, 3}

    state = %{
      keydir: keydir,
      index: 0,
      instance_ctx: %{data_dir: data_dir},
      compound_member_index: compound_index
    }

    try do
      :ok =
        Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
          data_dir,
          0,
          projection_index,
          [{key, "old", 1}]
        )

      true = :ets.insert(keydir, expired)
      assert 1 == Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0)

      assert ShardETS.delete_exact_entry(state, expired)
      assert [] == :ets.lookup(keydir, key)
      assert 0 == Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0)
    after
      Ferricstore.Raft.WARaftSegmentReader.delete_apply_projection_entries(
        data_dir,
        0,
        [{projection_index, key}]
      )

      :ets.delete(compound_index)
      :ets.delete(keydir)
    end
  end

  test "compound member counting cannot erase a replacement renewed during expiry cleanup" do
    keydir = new_keydir()
    index = :ets.new(:keydir_compound_count_race, [:ordered_set, :public])
    :ok = CompoundMemberIndex.reset(index)
    prefix = CompoundKey.set_prefix("renewed-count-set")
    key = CompoundKey.set_member("renewed-count-set", "member")
    expired = {key, "old", 1, LFU.initial(), 1, 20, 3}
    replacement = {key, "new", 0, LFU.initial(), 2, 40, 3}
    state = %{keydir: keydir, compound_member_index: index}

    try do
      true = :ets.insert(keydir, expired)
      :ok = CompoundMemberIndex.put(index, key)

      Process.put(:ferricstore_after_exact_keydir_delete_hook, fn _state, ^expired ->
        true = :ets.insert(keydir, replacement)
        :ok = CompoundMemberIndex.put(index, key)
      end)

      assert {:ok, 0} = CompoundMemberIndex.count_live(index, state, prefix)
      assert [^replacement] = :ets.lookup(keydir, key)
      assert {:ok, [^key]} = CompoundMemberIndex.keys_for_prefix(index, prefix)
    after
      Process.delete(:ferricstore_after_exact_keydir_delete_hook)
      :ets.delete(index)
      :ets.delete(keydir)
    end
  end

  test "replicated expiry batch ignores a key renewed after the sweep scan" do
    ctx = FerricStore.Instance.get(:default)
    key = unique_key("renewed")
    expired_at_ms = System.os_time(:millisecond) - 1_000
    renewed_at_ms = System.os_time(:millisecond) + 60_000
    shard_index = Router.shard_for(ctx, key)

    assert :ok = Router.put(ctx, key, "old", expired_at_ms)
    assert :ok = Router.put(ctx, key, "new", renewed_at_ms)

    assert [false] = Router.expire_if_batch(ctx, shard_index, [{key, expired_at_ms}])
    assert {"new", ^renewed_at_ms} = Router.get_meta(ctx, key)
  end

  test "replicated expiry batch removes the matching expired generation" do
    ctx = FerricStore.Instance.get(:default)
    key = unique_key("expired")
    expired_at_ms = System.os_time(:millisecond) - 1_000
    shard_index = Router.shard_for(ctx, key)

    assert :ok = Router.put(ctx, key, "old", expired_at_ms)
    assert [true] = Router.expire_if_batch(ctx, shard_index, [{key, expired_at_ms}])
    assert Router.get(ctx, key) == nil
  end

  test "Router read-side cleanup never deletes a key without matching its observed generation" do
    router_dir = Path.expand("../../../lib/ferricstore/store/router", __DIR__)

    for part <- ["part_04.ex", "part_09.ex"] do
      source = File.read!(Path.join(router_dir, part))

      refute source =~ ":ets.delete(keydir, key)",
             "#{part} contains a key-only delete that can remove a concurrent replacement"
    end
  end

  defp new_keydir do
    :ets.new(:keydir_concurrency, [:set, :public])
  end

  defp unique_key(suffix) do
    "keydir-concurrency:#{suffix}:#{System.unique_integer([:positive, :monotonic])}"
  end
end
