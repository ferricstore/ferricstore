defmodule Ferricstore.Store.HotCacheMaxValueSizeTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Test.IsolatedInstance

  @small_threshold 128

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: @small_threshold)
    pid = Process.whereis(elem(ctx.shard_names, 0))
    keydir = elem(ctx.keydir_refs, 0)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    %{shard: pid, index: 0, keydir: keydir, ctx: ctx}
  end

  describe "value below threshold" do
    test "small value is stored hot in ETS", %{shard: shard, keydir: keydir} do
      small_value = String.duplicate("a", @small_threshold - 1)
      :ok = GenServer.call(shard, {:put, "small", small_value, 0})

      # Flush to disk so ETS has disk location
      :ok = GenServer.call(shard, :flush)

      # Check ETS directly -- value should be present (hot)
      [{_key, ets_value, _exp, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, "small")
      assert ets_value != nil
      assert ets_value == small_value
    end

    test "small value is returned via get", %{shard: shard} do
      small_value = String.duplicate("b", 50)
      :ok = GenServer.call(shard, {:put, "small_get", small_value, 0})
      assert small_value == GenServer.call(shard, {:get, "small_get"})
    end
  end

  describe "value above threshold" do
    test "large value is stored cold (nil) in ETS", %{shard: shard, keydir: keydir} do
      large_value = String.duplicate("x", @small_threshold + 1)
      :ok = GenServer.call(shard, {:put, "large", large_value, 0})

      # Flush to disk so ETS has disk location
      :ok = GenServer.call(shard, :flush)

      # Check ETS directly -- value should be nil (cold)
      [{_key, ets_value, _exp, _lfu, _fid, _off, vsize}] = :ets.lookup(keydir, "large")
      assert ets_value == nil
      # value_size should be the actual size, not 0
      assert vsize == byte_size(large_value)
    end

    test "GET on large cold value returns correct data from disk", %{shard: shard} do
      large_value = String.duplicate("y", @small_threshold + 100)
      :ok = GenServer.call(shard, {:put, "large_get", large_value, 0})

      # Flush to ensure it's on disk
      :ok = GenServer.call(shard, :flush)

      # GET should return the correct value (read from disk)
      assert large_value == GenServer.call(shard, {:get, "large_get"})
    end
  end

  describe "value exactly at threshold" do
    test "value at exactly threshold bytes is stored hot (> not >=)", %{shard: shard, keydir: keydir} do
      exact_value = String.duplicate("e", @small_threshold)
      :ok = GenServer.call(shard, {:put, "exact", exact_value, 0})

      :ok = GenServer.call(shard, :flush)

      [{_key, ets_value, _exp, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, "exact")
      assert ets_value != nil
      assert ets_value == exact_value
    end
  end

  describe "overwrite transitions" do
    test "small value -> large value transitions from hot to cold", %{shard: shard, keydir: keydir} do
      small = String.duplicate("s", 10)
      large = String.duplicate("L", @small_threshold + 50)

      :ok = GenServer.call(shard, {:put, "transition", small, 0})
      :ok = GenServer.call(shard, :flush)

      # Should be hot
      [{_, v1, _, _, _, _, _}] = :ets.lookup(keydir, "transition")
      assert v1 != nil

      :ok = GenServer.call(shard, {:put, "transition", large, 0})
      :ok = GenServer.call(shard, :flush)

      # Should be cold now
      [{_, v2, _, _, _, _, vsize}] = :ets.lookup(keydir, "transition")
      assert v2 == nil
      assert vsize == byte_size(large)

      # GET still returns the correct large value
      assert large == GenServer.call(shard, {:get, "transition"})
    end

    test "large value -> small value transitions from cold to hot", %{shard: shard, keydir: keydir} do
      large = String.duplicate("L", @small_threshold + 50)
      small = String.duplicate("s", 10)

      :ok = GenServer.call(shard, {:put, "transition2", large, 0})
      :ok = GenServer.call(shard, :flush)

      # Should be cold
      [{_, v1, _, _, _, _, _}] = :ets.lookup(keydir, "transition2")
      assert v1 == nil

      :ok = GenServer.call(shard, {:put, "transition2", small, 0})
      :ok = GenServer.call(shard, :flush)

      # Should be hot now
      [{_, v2, _, _, _, _, _}] = :ets.lookup(keydir, "transition2")
      assert v2 != nil
      assert v2 == small
    end
  end

  describe "edge cases" do
    test "empty value (0 bytes) is always hot", %{shard: shard, keydir: keydir} do
      :ok = GenServer.call(shard, {:put, "empty", "", 0})
      :ok = GenServer.call(shard, :flush)

      [{_, ets_value, _, _, _, _, _}] = :ets.lookup(keydir, "empty")
      assert ets_value == ""
    end

    test "binary with embedded nulls works correctly", %{shard: shard} do
      # Large value with embedded nulls
      value = String.duplicate(<<0, 1, 2, 0, 3>>, 30)
      :ok = GenServer.call(shard, {:put, "null_bytes", value, 0})
      :ok = GenServer.call(shard, :flush)

      assert value == GenServer.call(shard, {:get, "null_bytes"})
    end

    test "STRLEN on large cold value uses value_size from 7-tuple (no disk read needed)", %{shard: shard, keydir: keydir} do
      large_value = String.duplicate("z", @small_threshold + 200)
      :ok = GenServer.call(shard, {:put, "strlen_key", large_value, 0})
      :ok = GenServer.call(shard, :flush)

      # The keydir entry should have the correct vsize even though value is nil
      [{_, nil, _, _, _fid, _off, vsize}] = :ets.lookup(keydir, "strlen_key")
      assert vsize == byte_size(large_value)
    end
  end

  describe "config set to 0 (everything cold)" do
    test "all values stored cold when threshold is 0" do
      ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 0)
      pid = Process.whereis(elem(ctx.shard_names, 0))
      keydir = elem(ctx.keydir_refs, 0)
      on_exit(fn -> IsolatedInstance.checkin(ctx) end)

      :ok = GenServer.call(pid, {:put, "zero_threshold", "tiny", 0})
      :ok = GenServer.call(pid, :flush)

      [{_, ets_value, _, _, _, _, _}] = :ets.lookup(keydir, "zero_threshold")
      assert ets_value == nil

      # GET still works (reads from disk)
      assert "tiny" == GenServer.call(pid, {:get, "zero_threshold"})
    end
  end

  describe "config set to very large number (everything hot)" do
    test "all values stored hot when threshold is very large" do
      ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1_073_741_824)
      pid = Process.whereis(elem(ctx.shard_names, 0))
      keydir = elem(ctx.keydir_refs, 0)
      on_exit(fn -> IsolatedInstance.checkin(ctx) end)

      large_value = String.duplicate("H", @small_threshold + 500)
      :ok = GenServer.call(pid, {:put, "huge_threshold", large_value, 0})
      :ok = GenServer.call(pid, :flush)

      [{_, ets_value, _, _, _, _, _}] = :ets.lookup(keydir, "huge_threshold")
      assert ets_value == large_value
    end
  end

  describe "CONFIG SET/GET integration" do
    test "CONFIG SET hot-cache-max-value-size updates persistent_term" do
      original = :persistent_term.get(:ferricstore_hot_cache_max_value_size)

      :ok = Ferricstore.Config.set("hot-cache-max-value-size", "131072")
      assert :persistent_term.get(:ferricstore_hot_cache_max_value_size) == 131_072

      # CONFIG GET returns the new value
      pairs = Ferricstore.Config.get("hot-cache-max-value-size")
      assert [{"hot-cache-max-value-size", "131072"}] = pairs

      # Restore
      :persistent_term.put(:ferricstore_hot_cache_max_value_size, original)
      Application.put_env(:ferricstore, :hot_cache_max_value_size, original)
    end

    test "CONFIG SET hot-cache-max-value-size rejects negative values" do
      assert {:error, _} = Ferricstore.Config.set("hot-cache-max-value-size", "-1")
    end

    test "CONFIG SET hot-cache-max-value-size accepts 0" do
      original = :persistent_term.get(:ferricstore_hot_cache_max_value_size)

      :ok = Ferricstore.Config.set("hot-cache-max-value-size", "0")
      assert :persistent_term.get(:ferricstore_hot_cache_max_value_size) == 0

      # Restore
      :persistent_term.put(:ferricstore_hot_cache_max_value_size, original)
      Application.put_env(:ferricstore, :hot_cache_max_value_size, original)
    end
  end
end
