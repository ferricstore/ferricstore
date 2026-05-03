defmodule Ferricstore.Store.RouterColdEmptyTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Router
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1024)
    shard = Process.whereis(elem(ctx.shard_names, 0))
    keydir = elem(ctx.keydir_refs, 0)

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    %{ctx: ctx, shard: shard, keydir: keydir}
  end

  test "get_with_file_ref treats cold empty values as valid file refs", %{
    ctx: ctx,
    shard: shard,
    keydir: keydir
  } do
    key = "cold_empty:" <> Integer.to_string(:erlang.unique_integer([:positive]))

    :ok = GenServer.call(shard, {:put, key, "", 0})
    :ok = GenServer.call(shard, :flush)

    assert [{^key, "", exp, lfu, fid, off, 0}] = :ets.lookup(keydir, key)
    :ets.insert(keydir, {key, nil, exp, lfu, fid, off, 0})

    assert {:cold_ref, path, value_offset, 0} = Router.get_with_file_ref(ctx, key)
    assert File.exists?(path)
    assert is_integer(value_offset)
  end

  test "get_with_file_ref falls back on cold rows with invalid offsets", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_invalid_offset:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert :miss == Router.get_with_file_ref(ctx, key)
  end

  test "direct cold reads do not crash on cold rows with invalid offsets", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_invalid_get:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert nil == Router.get(ctx, key)
    assert nil == Router.get_meta(ctx, key)
  end

  test "batch cold reads do not crash on cold rows with invalid offsets", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_invalid_batch:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert [nil] == Router.batch_get(ctx, [key])
  end

  test "direct cold reads do not return a value from a mismatched key offset", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_stale_offset:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    other_key = key <> ":other"
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = Path.join(shard_path, "00000.log")

    {:ok, [{other_offset, _}, {_key_offset, value_size}]} =
      NIF.v2_append_batch(path, [{other_key, "wrong-value", 0}, {key, "right-value", 0}])

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, other_offset, value_size})

    assert nil == Router.get(ctx, key)
    assert nil == Router.get_meta(ctx, key)
  end

  test "batch cold reads do not return values from mismatched key offsets", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_batch_stale_offset:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    other_key = key <> ":other"
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = Path.join(shard_path, "00000.log")

    {:ok, [{other_offset, _}, {_key_offset, value_size}]} =
      NIF.v2_append_batch(path, [{other_key, "wrong-value", 0}, {key, "right-value", 0}])

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, other_offset, value_size})

    assert [nil] == Router.batch_get(ctx, [key])
  end

  test "batch_get preserves mixed cold result order including empty values", %{
    ctx: ctx,
    shard: shard,
    keydir: keydir
  } do
    cold_empty = "cold_batch_empty:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    cold_large = "cold_batch_large:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    hot = "cold_batch_hot:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    large_value = :binary.copy("x", 2_048)

    :ok = GenServer.call(shard, {:put, cold_empty, "", 0})
    :ok = GenServer.call(shard, {:put, cold_large, large_value, 0})
    :ok = GenServer.call(shard, {:put, hot, "hot", 0})
    :ok = GenServer.call(shard, :flush)

    assert [{^cold_empty, "", exp_empty, lfu_empty, fid_empty, off_empty, 0}] =
             :ets.lookup(keydir, cold_empty)

    :ets.insert(keydir, {cold_empty, nil, exp_empty, lfu_empty, fid_empty, off_empty, 0})

    assert [{^cold_large, _stored, exp_large, lfu_large, fid_large, off_large, vsize_large}] =
             :ets.lookup(keydir, cold_large)

    :ets.insert(
      keydir,
      {cold_large, nil, exp_large, lfu_large, fid_large, off_large, vsize_large}
    )

    assert Router.batch_get(ctx, [cold_large, "missing", hot, cold_empty, cold_large]) ==
             [large_value, nil, "hot", "", large_value]
  end

  test "get_file_ref rejects cold rows with invalid offsets", %{ctx: ctx, keydir: keydir} do
    key = "cold_invalid_sendfile:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert nil == Router.get_file_ref(ctx, key)
  end

  test "get_keydir_file_ref rejects cold rows with invalid offsets", %{ctx: ctx, keydir: keydir} do
    key = "cold_invalid_file_ref:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert :miss == Router.get_keydir_file_ref(ctx, key)
    assert [] == :ets.lookup(keydir, key)
  end

  test "value_size rejects cold rows with invalid offsets", %{ctx: ctx, keydir: keydir} do
    key = "cold_invalid_value_size:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert nil == Router.value_size(ctx, key)
    assert [] == :ets.lookup(keydir, key)
  end

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
end
