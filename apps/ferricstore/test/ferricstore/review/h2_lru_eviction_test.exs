defmodule Ferricstore.Review.H2LruEvictionTest do
  @moduledoc """
  Verifies that volatile_lru and allkeys_lru eviction policies implement
  actual LRU ordering — evicting least recently accessed keys first.

  Previously the code fell through to a default clause that took arbitrary
  ETS fold order. Now it sorts by ldt (last decrement time) ascending.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  import Bitwise

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Store.LFU
  alias Ferricstore.Test.ShardHelpers

  @keydir :keydir_0

  # Each test key claims more bytes than any eviction deficit, so the guard
  # stops after one eviction and that one shows which key it picks. (With small
  # sizes it evicts every key it samples, and with other keys in the keydir it
  # samples only some of these.)
  @claimed_value_size 1 <<< 50

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      for i <- 0..9 do
        :ets.delete(@keydir, "lru_test_#{i}")
      end

      MemoryGuard.set_reject_writes(false)
      MemoryGuard.set_keydir_full(false)
    end)

    :ok
  end

  describe "volatile_lru eviction respects last access time" do
    test "stale keys are evicted before recently-accessed keys" do
      far_future = System.os_time(:millisecond) + 3_600_000

      # Stale keys: ldt 100 minutes ago.
      old_ldt = LFU.now_minutes() - 100 &&& 0xFFFF
      old_lfu = LFU.pack(old_ldt, 5)

      for i <- 0..4 do
        :ets.insert(
          @keydir,
          {"lru_test_#{i}", "val", far_future, old_lfu, 1, 0, @claimed_value_size}
        )
      end

      # Fresh keys: ldt = now, touched.
      for i <- 5..9 do
        fresh_lfu = LFU.pack(LFU.now_minutes(), 5)

        :ets.insert(
          @keydir,
          {"lru_test_#{i}", "val", far_future, fresh_lfu, 1, 0, @claimed_value_size}
        )

        LFU.touch(@keydir, "lru_test_#{i}", fresh_lfu)
      end

      {:ok, pid} =
        GenServer.start_link(MemoryGuard,
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :volatile_lru
        )

      send(pid, :check)
      Process.sleep(200)
      GenServer.stop(pid)

      {evicted, survived} = partition_keys()

      assert [evicted_key] = evicted
      assert ldts_for([evicted_key]) == [old_ldt]
      assert length(survived) == 9
    end
  end

  describe "allkeys_lru eviction respects last access time" do
    test "stale keys are evicted before recently-accessed keys" do
      far_future = System.os_time(:millisecond) + 3_600_000

      old_ldt = LFU.now_minutes() - 100 &&& 0xFFFF
      old_lfu = LFU.pack(old_ldt, 5)

      for i <- 0..4 do
        :ets.insert(
          @keydir,
          {"lru_test_#{i}", "val", far_future, old_lfu, 1, 0, @claimed_value_size}
        )
      end

      for i <- 5..9 do
        fresh_lfu = LFU.pack(LFU.now_minutes(), 5)

        :ets.insert(
          @keydir,
          {"lru_test_#{i}", "val", far_future, fresh_lfu, 1, 0, @claimed_value_size}
        )

        LFU.touch(@keydir, "lru_test_#{i}", fresh_lfu)
      end

      {:ok, pid} =
        GenServer.start_link(MemoryGuard,
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :allkeys_lru
        )

      send(pid, :check)
      Process.sleep(200)
      GenServer.stop(pid)

      {evicted, survived} = partition_keys()

      assert [evicted_key] = evicted
      assert ldts_for([evicted_key]) == [old_ldt]
      assert length(survived) == 9
    end
  end

  # The access clock is 16-bit minutes and wraps every 65,536 minutes (about
  # 45 days): a key touched just before the wrap has the larger stamp, yet it
  # has been idle longer than one touched just after.
  test "LRU order counts idle time across the minute clock's wrap" do
    now = 14
    fresh = {"fresh", 0, LFU.pack(14, 5), 1}
    stale = {"stale", 0, LFU.pack(65_450, 5), 1}
    staler = {"staler", 0, LFU.pack(65_000, 5), 1}

    assert ["staler", "stale", "fresh"] =
             [fresh, stale, staler]
             |> MemoryGuard.lru_order(now)
             |> Enum.map(&elem(&1, 0))
  end

  # -- helpers ---------------------------------------------------------------

  defp partition_keys do
    Enum.reduce(0..9, {[], []}, fn i, {ev, sv} ->
      key = "lru_test_#{i}"

      case :ets.lookup(@keydir, key) do
        [{_, nil, _, _, _, _, _}] -> {[key | ev], sv}
        [{_, _, _, _, _, _, _}] -> {ev, [key | sv]}
        [] -> {ev, sv}
      end
    end)
  end

  defp ldts_for(keys) do
    Enum.map(keys, fn key ->
      [{_, _, _, lfu, _, _, _}] = :ets.lookup(@keydir, key)
      {ldt, _counter} = LFU.unpack(lfu)
      ldt
    end)
  end
end
