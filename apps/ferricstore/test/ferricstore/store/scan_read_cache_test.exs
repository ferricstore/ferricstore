defmodule Ferricstore.Store.ScanReadCacheTest do
  @moduledoc """
  Background scans (the Flow query index and policy catalog backfills) read
  every Flow of a shard. A cold value they read stays cold: caching it would
  bring back every Flow that hibernation or memory pressure left on disk.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.{Router, ScanRead}
  alias Ferricstore.Test.IsolatedInstance
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  test "a shard entry read leaves a cold value on disk" do
    {ctx, key} = default_key()

    assert_read_keeps_cold(ctx, key, fn idx ->
      assert {:ok, [{"value", _expire_at_ms}]} = Router.read_shard_entries(ctx, idx, [key])
    end)
  end

  test "a standalone instance's shard entry read leaves a cold value on disk" do
    {ctx, key} = standalone_key()

    assert_read_keeps_cold(ctx, key, fn idx ->
      assert {:ok, [{"value", _expire_at_ms}]} = Router.read_shard_entries(ctx, idx, [key])
    end)
  end

  test "a scan's shard value read leaves a cold value on disk" do
    {ctx, key} = default_key()

    assert_read_keeps_cold(ctx, key, fn idx ->
      assert {:ok, "value"} = ScanRead.run(fn -> Router.read_shard_value(ctx, idx, key) end)
    end)
  end

  test "a standalone instance's scan reads a shard value without caching it" do
    {ctx, key} = standalone_key()

    assert_read_keeps_cold(ctx, key, fn idx ->
      assert {:ok, "value"} = ScanRead.run(fn -> Router.read_shard_value(ctx, idx, key) end)
    end)
  end

  defp default_key do
    ctx = FerricStore.Instance.get(:default)
    key = "scan-cold:#{System.unique_integer([:positive, :monotonic])}"
    assert :ok = Router.put(ctx, key, "value")
    ShardHelpers.flush_all_shards()
    {ctx, key}
  end

  defp standalone_key do
    ctx = IsolatedInstance.checkout(shard_count: 2)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    key = "scan-cold:#{System.unique_integer([:positive, :monotonic])}"
    assert :ok = Router.put(ctx, key, "value")
    {ctx, key}
  end

  defp assert_read_keeps_cold(ctx, key, read) do
    idx = Router.shard_for(ctx, key)
    keydir = elem(ctx.keydir_refs, idx)
    assert eventually(fn -> located?(keydir, key) end), "#{key} never reached disk"

    [{^key, "value", _exp, _lfu, _file_id, _offset, _size} = row] = :ets.lookup(keydir, key)
    assert Ferricstore.Store.Keydir.drop_value_unchanged_source(keydir, row)

    read.(idx)
    assert [{^key, nil, _, _, _, _, _}] = :ets.lookup(keydir, key)
  end

  defp located?(keydir, key) do
    case :ets.lookup(keydir, key) do
      [{^key, value, _exp, _lfu, file_id, offset, _size}] when is_binary(value) ->
        (is_integer(file_id) or is_tuple(file_id)) and is_integer(offset)

      _other ->
        false
    end
  end

  defp eventually(fun, attempts \\ 100)
  defp eventually(_fun, 0), do: false

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(20)
      eventually(fun, attempts - 1)
    end
  end
end
