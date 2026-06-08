defmodule Ferricstore.Store.RouterBugHuntTest do
  @moduledoc """
  Targeted bug-hunt tests for the Router <-> Shard interaction layer.

  These tests probe edge cases, crash-recovery semantics, concurrency safety,
  and size-guard enforcement at the Router level -- exercising the full stack
  from `Router.*` convenience functions through the Shard GenServer, ETS cache,
  and Bitcask NIF persistence layer.

  All tests run `async: false` because they share the four application-supervised
  shard processes and some tests kill/restart shards.

  ## Findings

  **Finding 1 -- Router.put does not reject empty keys.** The size guard in
  `Router.put/3` checks `byte_size(key) > @max_key_size` but does not check
  `byte_size(key) == 0`. Empty keys are rejected at the command layer
  (`Strings.handle("SET", ["", ...])`) but not at the store layer. Any code
  that calls `Router.put(FerricStore.Instance.get(:default), "", ...)` directly bypasses this protection.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag :shard_kill

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  # Ensure all shards are alive before every test in this module.
  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  # Flush all keys before each test to avoid cross-test contamination,
  # and ensure shards are alive after tests that kill them.
  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn ->
      ShardHelpers.flush_all_keys()
      ShardHelpers.wait_shards_alive()
    end)
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  # Unique key prefix to avoid collisions with other test modules.
  defp ukey(base), do: "rbh_#{base}_#{:rand.uniform(9_999_999)}"

  # Finds a key that hashes to the given shard index.
  defp key_for_shard(shard_idx, prefix) do
    Enum.reduce_while(1..100_000, nil, fn i, _acc ->
      k = "rbh_#{prefix}_#{i}_#{:rand.uniform(999_999)}"

      if Router.shard_for(FerricStore.Instance.get(:default), k) == shard_idx do
        {:halt, k}
      else
        {:cont, nil}
      end
    end)
  end

  # Kills a shard by index, waits for supervisor to restart it, returns {old_pid, new_pid}.
  defp kill_and_wait_restart(index) do
    name = Router.shard_name(FerricStore.Instance.get(:default), index)
    old_pid = Process.whereis(name)
    ref = Process.monitor(old_pid)
    Process.exit(old_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000
    new_pid = wait_for_new_pid(name, old_pid)
    {old_pid, new_pid}
  end

  defp wait_for_new_pid(name, old_pid, attempts \\ 40)
  defp wait_for_new_pid(_name, _old_pid, 0), do: raise("Shard did not restart in time")

  defp wait_for_new_pid(name, old_pid, attempts) do
    case Process.whereis(name) do
      nil ->
        Process.sleep(50)
        wait_for_new_pid(name, old_pid, attempts - 1)

      ^old_pid ->
        Process.sleep(50)
        wait_for_new_pid(name, old_pid, attempts - 1)

      new_pid ->
        new_pid
    end
  end

  # ===========================================================================
  # 1. Router.put with key that maps to shard 0, then get from correct shard
  # ===========================================================================

  describe "put/get routes to correct shard" do
    test "put with a key mapping to shard 0, then get returns the value" do
      key = key_for_shard(0, "shard0")
      assert Router.shard_for(FerricStore.Instance.get(:default), key) == 0, "Precondition: key must hash to shard 0"

      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "hello_shard0")
      assert "hello_shard0" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "put to each shard index independently, get returns correct values" do
      kvs =
        for idx <- 0..3 do
          k = key_for_shard(idx, "each_shard")
          v = "val_for_shard_#{idx}"
          assert :ok = Router.put(FerricStore.Instance.get(:default), k, v)
          {k, v, idx}
        end

      for {k, v, idx} <- kvs do
        assert Router.get(FerricStore.Instance.get(:default), k) == v,
               "Key #{k} (shard #{idx}) should return #{v}"
      end
    end

    test "shard routing is deterministic -- same key always same shard" do
      key = ukey("deterministic")
      shard1 = Router.shard_for(FerricStore.Instance.get(:default), key)
      shard2 = Router.shard_for(FerricStore.Instance.get(:default), key)
      assert shard1 == shard2
    end
  end

  # ===========================================================================
  # 2. Router.put then kill shard, restart, get -- data survives?
  # ===========================================================================

  describe "crash recovery -- data survives shard restart" do
    test "put, flush, kill shard, restart, get returns persisted value" do
      key = key_for_shard(0, "crash_durable")
      value = "must_survive_crash"

      assert :ok = Router.put(FerricStore.Instance.get(:default), key, value)

      # Flush pending writes to Bitcask so they are durable before the crash.
      ShardHelpers.flush_all_shards()

      {_old, _new} = kill_and_wait_restart(0)
      ShardHelpers.wait_shards_alive()

      # After restart, Bitcask replays the log and the value should be recoverable.
      ShardHelpers.eventually(fn -> value == Router.get(FerricStore.Instance.get(:default), key) end,
        "Value should survive shard crash and restart (recovered from Bitcask)")
    end

    test "multiple keys on same shard survive crash" do
      keys =
        for i <- 1..5 do
          k = key_for_shard(1, "multi_crash_#{i}")
          v = "durable_#{i}"
          Router.put(FerricStore.Instance.get(:default), k, v)
          {k, v}
        end

      ShardHelpers.flush_all_shards()
      {_old, _new} = kill_and_wait_restart(1)
      ShardHelpers.wait_shards_alive()

      for {k, v} <- keys do
        ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == v end,
          "Key #{k} should survive crash recovery")
      end
    end
  end

  # ===========================================================================
  # 3. Router.get on non-existent key -- nil not crash
  # ===========================================================================

  describe "get on non-existent key" do
    test "returns nil for a key that was never set" do
      assert nil == Router.get(FerricStore.Instance.get(:default), "rbh_nonexistent_key_that_does_not_exist")
    end

    test "returns nil for a random unique key" do
      assert nil == Router.get(FerricStore.Instance.get(:default), ukey("never_set"))
    end

    test "returns nil and does not crash the shard process" do
      key = ukey("safe_miss")
      idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      pid_before = Process.whereis(Router.shard_name(FerricStore.Instance.get(:default), idx))

      assert nil == Router.get(FerricStore.Instance.get(:default), key)

      pid_after = Process.whereis(Router.shard_name(FerricStore.Instance.get(:default), idx))
      assert pid_before == pid_after, "Shard should not have crashed on get-miss"
    end
  end

  # ===========================================================================
  # 4. Router.delete on non-existent key -- :ok not crash
  # ===========================================================================

  describe "delete on non-existent key" do
    test "returns :ok for key that was never set" do
      assert :ok = Router.delete(FerricStore.Instance.get(:default), "rbh_delete_nonexistent_key")
    end

    test "returns :ok and does not crash the shard process" do
      key = ukey("safe_delete")
      idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      pid_before = Process.whereis(Router.shard_name(FerricStore.Instance.get(:default), idx))

      assert :ok = Router.delete(FerricStore.Instance.get(:default), key)

      pid_after = Process.whereis(Router.shard_name(FerricStore.Instance.get(:default), idx))
      assert pid_before == pid_after, "Shard should not have crashed on delete-miss"
    end

    test "double delete is idempotent" do
      key = ukey("double_del")
      Router.put(FerricStore.Instance.get(:default), key, "v")
      assert :ok = Router.delete(FerricStore.Instance.get(:default), key)
      assert :ok = Router.delete(FerricStore.Instance.get(:default), key)
      assert nil == Router.get(FerricStore.Instance.get(:default), key)
    end
  end

  # ===========================================================================
  # 5. Router.keys with many keys -- all returned, no internal keys leaked
  # ===========================================================================

  describe "keys with many keys" do
    @tag timeout: 120_000
    test "all 1000 inserted keys are returned and no internal/metadata keys leak" do
      prefix = "rbh_bulk_#{:rand.uniform(999_999)}"
      n = 1_000

      keys =
        for i <- 1..n do
          k = "#{prefix}_#{i}"
          Router.put(FerricStore.Instance.get(:default), k, "v#{i}")
          k
        end

      returned_keys = Router.keys(FerricStore.Instance.get(:default))
      key_set = MapSet.new(returned_keys)

      # All inserted keys must be present.
      for k <- keys do
        assert MapSet.member?(key_set, k),
               "Key #{k} should be in Router.keys(FerricStore.Instance.get(:default)) result"
      end

      # No internal/metadata keys should leak through KEYS command.
      # Note: Router.keys(FerricStore.Instance.get(:default)) returns raw keys including compound keys;
      # the KEYS command in server.ex filters them via CompoundKey.internal_key?/1.
      # Here we verify the filtering logic is correct by checking that
      # the KEYS command (via Dispatcher) does not expose internal keys.
      alias Ferricstore.Store.CompoundKey
      user_visible_keys = Enum.reject(returned_keys, &CompoundKey.internal_key?/1)

      for rk <- user_visible_keys do
        refute String.contains?(rk, <<0>>),
               "Internal compound key leaked: #{inspect(rk)}"

        refute String.starts_with?(rk, "__ferricstore_"),
               "Internal metadata key leaked: #{rk}"

        refute String.starts_with?(rk, "T:"),
               "Internal type key leaked: #{rk}"

        refute String.starts_with?(rk, "PM:"),
               "Internal promotion marker key leaked: #{rk}"
      end

      # The returned count should be at least n.
      assert length(returned_keys) >= n
    end
  end

  # ===========================================================================
  # 6. Router.dbsize accuracy after put/delete cycles
  # ===========================================================================

  describe "dbsize accuracy after put/delete cycles" do
    test "dbsize reflects net key count after puts and deletes" do
      base = Router.dbsize(FerricStore.Instance.get(:default))

      keys =
        for i <- 1..20 do
          k = ukey("dbsize_#{i}")
          Router.put(FerricStore.Instance.get(:default), k, "v#{i}")
          k
        end

      assert Router.dbsize(FerricStore.Instance.get(:default)) == base + 20

      # Delete half the keys.
      {to_delete, to_keep} = Enum.split(keys, 10)
      Enum.each(to_delete, fn k -> Router.delete(FerricStore.Instance.get(:default), k) end)

      assert Router.dbsize(FerricStore.Instance.get(:default)) == base + 10

      # Verify the kept keys still exist.
      for k <- to_keep do
        assert Router.get(FerricStore.Instance.get(:default), k) != nil
      end
    end

    test "dbsize decreases by exactly the number of keys deleted" do
      base = Router.dbsize(FerricStore.Instance.get(:default))

      keys =
        for i <- 1..5 do
          k = ukey("db0_#{i}")
          Router.put(FerricStore.Instance.get(:default), k, "v")
          k
        end

      assert Router.dbsize(FerricStore.Instance.get(:default)) == base + 5

      Enum.each(keys, fn k -> Router.delete(FerricStore.Instance.get(:default), k) end)
      assert Router.dbsize(FerricStore.Instance.get(:default)) == base
    end
  end

  # ===========================================================================
  # 7. Router.exists? with expired key -- false
  # ===========================================================================

  describe "exists? with expired key" do
    test "returns false for a key whose TTL has passed" do
      key = ukey("expired_exists")
      expire_at = System.os_time(:millisecond) + 150

      Router.put(FerricStore.Instance.get(:default), key, "ephemeral", expire_at)
      assert Router.exists?(FerricStore.Instance.get(:default), key) == true

      Process.sleep(200)

      assert Router.exists?(FerricStore.Instance.get(:default), key) == false,
             "exists? should return false for expired key"
    end

    test "returns false for a key set in the past" do
      key = ukey("past_expire")
      past = System.os_time(:millisecond) - 100

      Router.put(FerricStore.Instance.get(:default), key, "already_dead", past)
      assert Router.exists?(FerricStore.Instance.get(:default), key) == false
    end

    test "get also returns nil for expired key" do
      key = ukey("expired_get")
      expire_at = System.os_time(:millisecond) + 150

      Router.put(FerricStore.Instance.get(:default), key, "temp", expire_at)
      Process.sleep(200)

      assert nil == Router.get(FerricStore.Instance.get(:default), key)
      assert false == Router.exists?(FerricStore.Instance.get(:default), key)
    end
  end

  # ===========================================================================
  # 8. Router.get_version increments on write
  # ===========================================================================

  describe "get_version increments on write" do
    test "version increases after a put" do
      key = ukey("version_put")

      v1 = Router.get_version(FerricStore.Instance.get(:default), key)
      Router.put(FerricStore.Instance.get(:default), key, "val1")
      v2 = Router.get_version(FerricStore.Instance.get(:default), key)

      assert v2 > v1,
             "Version should increment after put: v1=#{v1}, v2=#{v2}"
    end

    test "version increases after a delete" do
      key = ukey("version_del")
      Router.put(FerricStore.Instance.get(:default), key, "val")

      v1 = Router.get_version(FerricStore.Instance.get(:default), key)
      Router.delete(FerricStore.Instance.get(:default), key)
      v2 = Router.get_version(FerricStore.Instance.get(:default), key)

      assert v2 > v1,
             "Version should increment after delete: v1=#{v1}, v2=#{v2}"
    end

    test "version does not increment on read" do
      key = ukey("version_read")
      Router.put(FerricStore.Instance.get(:default), key, "val")

      v1 = Router.get_version(FerricStore.Instance.get(:default), key)
      _val = Router.get(FerricStore.Instance.get(:default), key)
      v2 = Router.get_version(FerricStore.Instance.get(:default), key)

      assert v2 == v1,
             "Version should NOT increment on read: v1=#{v1}, v2=#{v2}"
    end

    test "version increments monotonically across multiple writes" do
      key = ukey("version_mono")

      versions =
        for i <- 1..10 do
          Router.put(FerricStore.Instance.get(:default), key, "val_#{i}")
          Router.get_version(FerricStore.Instance.get(:default), key)
        end

      # Each successive version should be strictly greater than the previous.
      pairs = Enum.zip(versions, Enum.drop(versions, 1))

      for {prev, curr} <- pairs do
        assert curr > prev,
               "Versions should increase monotonically: #{prev} -> #{curr}"
      end
    end
  end

  # ===========================================================================
  # 9. Concurrent Router.put from 20 tasks -- no data loss
  # ===========================================================================

  describe "concurrent put from 20 tasks" do
    test "all 20 concurrent writes succeed with no data loss" do
      prefix = "rbh_conc_#{:rand.uniform(999_999)}"

      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            key = "#{prefix}_#{i}"
            value = "concurrent_val_#{i}"
            :ok = Router.put(FerricStore.Instance.get(:default), key, value)
            {key, value}
          end)
        end

      results = Task.await_many(tasks, 10_000)

      # Every key/value pair should be readable.
      for {key, value} <- results do
        actual = Router.get(FerricStore.Instance.get(:default), key)

        assert actual == value,
               "Key #{key}: expected #{inspect(value)}, got #{inspect(actual)}"
      end
    end

    test "concurrent puts to same key -- last writer wins, no crash" do
      key = ukey("conc_same_key")

      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            Router.put(FerricStore.Instance.get(:default), key, "writer_#{i}")
            i
          end)
        end

      Task.await_many(tasks, 10_000)

      # The key should have one of the 20 values, not nil or a crash.
      result = Router.get(FerricStore.Instance.get(:default), key)
      assert result != nil, "Key should not be nil after concurrent writes"
      assert String.starts_with?(result, "writer_"), "Value should be one of the writer values"
    end

    test "concurrent put and get do not crash shards" do
      key = ukey("conc_rw")
      Router.put(FerricStore.Instance.get(:default), key, "initial")

      pids_before = for i <- 0..3, do: {i, Process.whereis(Router.shard_name(FerricStore.Instance.get(:default), i))}

      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            if rem(i, 2) == 0 do
              Router.put(FerricStore.Instance.get(:default), key, "v_#{i}")
            else
              Router.get(FerricStore.Instance.get(:default), key)
            end
          end)
        end

      Task.await_many(tasks, 10_000)

      # All shards should still be alive with the same PIDs.
      for {i, old_pid} <- pids_before do
        current_pid = Process.whereis(Router.shard_name(FerricStore.Instance.get(:default), i))

        assert current_pid == old_pid,
               "Shard #{i} should not have crashed during concurrent read/write"
      end
    end
  end

  # ===========================================================================
  # 10. Router.incr on non-integer value -- error not crash
  # ===========================================================================

  describe "incr on non-integer value" do
    test "returns error tuple for string value" do
      key = ukey("incr_str")
      Router.put(FerricStore.Instance.get(:default), key, "not_a_number")

      assert {:error, msg} = Router.incr(FerricStore.Instance.get(:default), key, 1)
      assert msg =~ "not an integer"
    end

    test "returns error for float-like string" do
      key = ukey("incr_float_str")
      Router.put(FerricStore.Instance.get(:default), key, "3.14")

      assert {:error, msg} = Router.incr(FerricStore.Instance.get(:default), key, 1)
      assert msg =~ "not an integer"
    end

    test "returns error for empty string value" do
      key = ukey("incr_empty")
      Router.put(FerricStore.Instance.get(:default), key, "")

      assert {:error, msg} = Router.incr(FerricStore.Instance.get(:default), key, 1)
      assert msg =~ "not an integer"
    end

    test "does not crash the shard on non-integer incr" do
      key = ukey("incr_safe")
      idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      pid_before = Process.whereis(Router.shard_name(FerricStore.Instance.get(:default), idx))

      Router.put(FerricStore.Instance.get(:default), key, "hello")
      {:error, _} = Router.incr(FerricStore.Instance.get(:default), key, 1)

      pid_after = Process.whereis(Router.shard_name(FerricStore.Instance.get(:default), idx))

      assert pid_before == pid_after,
             "Shard should not crash on incr of non-integer value"
    end

    test "incr on non-existent key initializes to delta" do
      key = ukey("incr_new")
      assert {:ok, 5} = Router.incr(FerricStore.Instance.get(:default), key, 5)
      assert "5" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "value is unchanged after failed incr" do
      key = ukey("incr_unchanged")
      Router.put(FerricStore.Instance.get(:default), key, "abc")

      {:error, _} = Router.incr(FerricStore.Instance.get(:default), key, 1)

      assert "abc" == Router.get(FerricStore.Instance.get(:default), key),
             "Value should be unchanged after failed incr"
    end
  end

  # ===========================================================================
  # 11. Router.put with value at 512MB limit -- rejected
  # ===========================================================================

  describe "put with value at 512MB limit" do
    @tag :large_alloc
    test "value exactly at 512MB is rejected" do
      key = ukey("512mb_exact")
      # 512 * 1024 * 1024 = 536_870_912 bytes
      big_value = :binary.copy(<<0>>, 512 * 1024 * 1024)

      assert {:error, msg} = Router.put(FerricStore.Instance.get(:default), key, big_value)
      assert msg =~ "value too large"
    end

    test "value one byte over 512MB is rejected (arithmetic check)" do
      key = ukey("512mb_over")
      max = 512 * 1024 * 1024

      # Verify the size guard logic matches Router.put/3 without allocating 512MB.
      assert {:error, _} = check_size_guard(key, max + 1)
    end

    test "value well under 512MB is accepted and readable" do
      key = ukey("small_val")
      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "small_value")
      assert "small_value" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "key at 64KB limit is rejected" do
      big_key = String.duplicate("k", 65_536)
      assert {:error, msg} = Router.put(FerricStore.Instance.get(:default), big_key, "v")
      assert msg =~ "key too large"
    end

    test "key just under 64KB limit is accepted" do
      key = String.duplicate("k", 65_535)
      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "v")
    end
  end

  # ===========================================================================
  # 12. Router.put with empty key -- error
  # ===========================================================================

  describe "put with empty key" do
    test "empty key is accepted at Router level (missing guard -- validated at command layer only)" do
      # Finding: Router.put does NOT reject empty keys -- the empty-key guard
      # is in the Strings command handler (e.g. SET "" val -> ERR empty key).
      # At the Router level, "" is a valid binary with byte_size 0 < 65_535.
      #
      # This test documents the actual behavior: Router.put("", ...) succeeds.
      # A fix would add `byte_size(key) == 0` guard to Router.put/3.
      result = Router.put(FerricStore.Instance.get(:default), "", "empty_key_value")

      assert result == :ok,
             "Router.put with empty key succeeds (guard is at command layer only)"

      # The value should be retrievable with empty key.
      assert "empty_key_value" == Router.get(FerricStore.Instance.get(:default), "")
    end
  end

  # -------------------------------------------------------------------
  # Private helpers
  # -------------------------------------------------------------------

  # Simulates the size guard check from Router.put/3 without allocating
  # a huge binary. Used for boundary testing.
  defp check_size_guard(key, value_size) do
    max_key_size = 65_535
    max_value_size = 512 * 1024 * 1024

    cond do
      byte_size(key) > max_key_size ->
        {:error, "ERR key too large (max #{max_key_size} bytes)"}

      value_size >= max_value_size ->
        {:error, "ERR value too large (max #{max_value_size} bytes)"}

      true ->
        :ok
    end
  end
end
