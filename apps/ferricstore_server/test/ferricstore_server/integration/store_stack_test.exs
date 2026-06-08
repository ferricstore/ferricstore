defmodule FerricstoreServer.Integration.StoreStackTest do
  @moduledoc false

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag :global_state
  @moduletag :shard_kill
  @moduletag :cluster

  alias Ferricstore.Store.Router
  alias Ferricstore.Commands.{Dispatcher, Expiry, Server, Strings}
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  # Ensure all shards are alive after tests that kill them, and flush all keys
  # before each test so the keydir stays small — a large keydir makes KEYS/DBSIZE
  # calls progressively slower and can trigger GenServer call timeouts.
  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Builds a store map backed by the real Router (application-supervised shards).
  defp real_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      flush: fn -> :ok end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end
    }
  end

  # Generates a unique key to prevent cross-test interference.
  defp ukey(base), do: "#{base}_#{:rand.uniform(9_999_999)}"

  # Returns the keydir ETS table name for the shard that owns `key`.
  defp keydir_for(key), do: :"keydir_#{Router.shard_for(FerricStore.Instance.get(:default), key)}"

  # Returns the PID of the shard GenServer that owns `key`.
  defp shard_pid_for(key) do
    name =
      Router.shard_name(
        FerricStore.Instance.get(:default),
        Router.shard_for(FerricStore.Instance.get(:default), key)
      )

    Process.whereis(name)
  end

  # ---------------------------------------------------------------------------
  # Strings commands through real Router
  # ---------------------------------------------------------------------------

  describe "Strings commands through real Router" do
    test "SET and GET round-trip through Router" do
      store = real_store()
      k = ukey("set_get")

      Strings.handle("SET", [k, "hello"], store)
      assert "hello" == Strings.handle("GET", [k], store)
    end

    test "SET overwrites existing value in Bitcask" do
      store = real_store()
      k = ukey("overwrite")

      Strings.handle("SET", [k, "v1"], store)
      Strings.handle("SET", [k, "v2"], store)
      assert "v2" == Strings.handle("GET", [k], store)
    end

    test "DEL removes from all layers (ETS + Bitcask)" do
      store = real_store()
      k = ukey("del_layers")

      Strings.handle("SET", [k, "value"], store)
      # Verify key exists before deletion
      assert "value" == Strings.handle("GET", [k], store)

      Strings.handle("DEL", [k], store)
      assert nil == Strings.handle("GET", [k], store)

      # Verify ETS is clean
      assert [] == :ets.lookup(keydir_for(k), k)
    end

    test "EXISTS reflects current store state" do
      store = real_store()
      k = ukey("exists_state")

      Strings.handle("SET", [k, "present"], store)
      assert 1 == Strings.handle("EXISTS", [k], store)

      Strings.handle("DEL", [k], store)
      assert 0 == Strings.handle("EXISTS", [k], store)
    end

    test "MSET then MGET" do
      store = real_store()
      k1 = ukey("mset_a")
      k2 = ukey("mset_b")
      k3 = ukey("mset_c")

      :ok = Strings.handle("MSET", [k1, "v1", k2, "v2", k3, "v3"], store)
      assert ["v1", "v2", "v3"] == Strings.handle("MGET", [k1, k2, k3], store)
    end

    test "keys spread across multiple shards" do
      store = real_store()

      keys =
        for i <- 1..100 do
          k = ukey("spread_#{i}")
          Strings.handle("SET", [k, "val"], store)
          k
        end

      shard_indices =
        Enum.map(keys, fn k -> Router.shard_for(FerricStore.Instance.get(:default), k) end)
        |> Enum.uniq()

      # With 100 keys and 4 shards, at least 2 shards should be used
      assert length(shard_indices) >= 2,
             "Expected keys in at least 2 shards, got shards: #{inspect(shard_indices)}"

      # Verify every key is retrievable through shard GenServer calls
      for i <- 0..3 do
        shard_keys =
          GenServer.call(Router.shard_name(FerricStore.Instance.get(:default), i), :keys)

        matching = Enum.filter(keys, fn k -> k in shard_keys end)

        for k <- matching do
          assert Router.shard_for(FerricStore.Instance.get(:default), k) == i
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Expiry commands through real Router
  # ---------------------------------------------------------------------------

  describe "Expiry commands through real Router" do
    test "EXPIRE then TTL shows seconds remaining" do
      store = real_store()
      k = ukey("expire_ttl")

      Strings.handle("SET", [k, "value"], store)
      assert 1 == Expiry.handle("EXPIRE", [k, "100"], store)

      ttl = Expiry.handle("TTL", [k], store)
      assert ttl >= 1 and ttl <= 100
    end

    test "EXPIRE then GET after real sleep returns nil" do
      store = real_store()
      k = ukey("expire_sleep")

      Strings.handle("SET", [k, "ephemeral", "PX", "200"], store)
      assert "ephemeral" == Strings.handle("GET", [k], store)

      :timer.sleep(300)
      assert nil == Strings.handle("GET", [k], store)
    end

    test "PERSIST removes TTL from live key" do
      store = real_store()
      k = ukey("persist")

      Strings.handle("SET", [k, "value"], store)
      Expiry.handle("EXPIRE", [k, "100"], store)
      assert 1 == Expiry.handle("PERSIST", [k], store)

      ttl = Expiry.handle("TTL", [k], store)
      assert ttl == -1
    end

    test "PTTL returns sub-second precision" do
      store = real_store()
      k = ukey("pttl_precision")

      future = System.os_time(:millisecond) + 5_000
      Router.put(FerricStore.Instance.get(:default), k, "v", future)

      pttl = Expiry.handle("PTTL", [k], store)
      assert pttl >= 1 and pttl <= 5_000
    end

    test "key expires and is not in KEYS output" do
      store = real_store()
      k = ukey("exp_keys")

      Strings.handle("SET", [k, "value", "PX", "200"], store)
      :timer.sleep(300)

      all_keys = Server.handle("KEYS", ["*"], store)
      refute k in all_keys
    end

    test "expired key returns -2 for TTL" do
      store = real_store()
      k = ukey("ttl_expired")

      Strings.handle("SET", [k, "value", "PX", "200"], store)
      :timer.sleep(300)

      assert -2 == Expiry.handle("TTL", [k], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Dispatcher with real store
  # ---------------------------------------------------------------------------

  describe "Dispatcher with real store" do
    test "dispatch SET then GET returns value" do
      store = real_store()
      k = ukey("dispatch_sg")

      Dispatcher.dispatch("SET", [k, "world"], store)
      assert "world" == Dispatcher.dispatch("GET", [k], store)
    end

    test "dispatch is case insensitive with real store" do
      store = real_store()
      k = ukey("dispatch_ci")

      Dispatcher.dispatch("set", [k, "val"], store)
      assert "val" == Dispatcher.dispatch("get", [k], store)
    end

    test "dispatch DEL returns 1 then 0" do
      store = real_store()
      k = ukey("dispatch_del")

      Dispatcher.dispatch("SET", [k, "v"], store)
      assert 1 == Dispatcher.dispatch("DEL", [k], store)
      assert 0 == Dispatcher.dispatch("DEL", [k], store)
    end

    test "dispatch EXPIRE + TTL chain" do
      store = real_store()
      k = ukey("dispatch_exp")

      Dispatcher.dispatch("SET", [k, "v"], store)
      assert 1 == Dispatcher.dispatch("EXPIRE", [k, "100"], store)

      ttl = Dispatcher.dispatch("TTL", [k], store)
      assert is_integer(ttl) and ttl > 0
    end

    test "dispatch KEYS pattern with real data" do
      store = real_store()
      prefix = ukey("pfx")
      k1 = "#{prefix}:a"
      k2 = "#{prefix}:b"
      k3 = ukey("other_c")

      Dispatcher.dispatch("SET", [k1, "1"], store)
      Dispatcher.dispatch("SET", [k2, "2"], store)
      Dispatcher.dispatch("SET", [k3, "3"], store)

      matched = Dispatcher.dispatch("KEYS", ["#{prefix}:*"], store)
      assert k1 in matched
      assert k2 in matched
      refute k3 in matched
    end

    test "dispatch DBSIZE counts live keys" do
      store = real_store()

      keys =
        for i <- 1..5 do
          k = ukey("dbsize_#{i}")
          Dispatcher.dispatch("SET", [k, "v"], store)
          k
        end

      size = Dispatcher.dispatch("DBSIZE", [], store)
      # At least 5 (other tests may have added keys to the live shards)
      assert size >= 5

      # All our keys should be in the store
      all_keys = Router.keys(FerricStore.Instance.get(:default))
      for k <- keys, do: assert(k in all_keys)
    end
  end

  # ---------------------------------------------------------------------------
  # ETS cache integration
  # ---------------------------------------------------------------------------

  describe "ETS cache integration" do
    test "ETS cache is warm after GET" do
      k = ukey("ets_warm")
      Router.put(FerricStore.Instance.get(:default), k, "cached_val", 0)

      # GET warms the ETS cache
      assert "cached_val" == Router.get(FerricStore.Instance.get(:default), k)

      # Single-table format: {key, value, expire_at_ms, lfu_counter}
      assert [{^k, "cached_val", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir_for(k), k)
    end

    test "ETS cache is cleared after DEL" do
      k = ukey("ets_del")
      Router.put(FerricStore.Instance.get(:default), k, "val", 0)
      # Warm the cache
      Router.get(FerricStore.Instance.get(:default), k)
      # Single-table format: {key, value, expire_at_ms, lfu_counter}
      assert [{^k, _, _, _, _, _, _}] = :ets.lookup(keydir_for(k), k)

      Router.delete(FerricStore.Instance.get(:default), k)
      assert [] == :ets.lookup(keydir_for(k), k)
    end

    test "ETS cache stores correct expiry" do
      k = ukey("ets_expiry")
      future = System.os_time(:millisecond) + 60_000

      Router.put(FerricStore.Instance.get(:default), k, "v", future)
      # PUT writes to ETS directly, single-table format
      assert [{^k, "v", ^future, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir_for(k), k)
    end

    test "expired key is evicted from ETS on get" do
      k = ukey("ets_evict")
      past = System.os_time(:millisecond) - 500

      Router.put(FerricStore.Instance.get(:default), k, "v", past)
      # Confirm ETS has the entry (put always writes), single-table format
      assert [{^k, "v", ^past, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir_for(k), k)

      # GET detects expiry, evicts from ETS, returns nil
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
      assert [] == :ets.lookup(keydir_for(k), k)
    end
  end

  # ---------------------------------------------------------------------------
  # Cross-shard operations
  # ---------------------------------------------------------------------------

  describe "cross-shard operations" do
    test "keys() fans out across all 4 shards" do
      # Find one key per shard to guarantee coverage
      keys_by_shard = find_keys_for_all_shards("fan_out")

      for {_shard_idx, k} <- keys_by_shard do
        Router.put(FerricStore.Instance.get(:default), k, "present", 0)
      end

      all_keys = Router.keys(FerricStore.Instance.get(:default))

      for {_shard_idx, k} <- keys_by_shard do
        assert k in all_keys, "Expected #{k} in keys(), but it was missing"
      end
    end

    test "dbsize() sums across all shards" do
      keys_by_shard = find_keys_for_all_shards("dbsize_sum")

      for {_shard_idx, k} <- keys_by_shard do
        Router.put(FerricStore.Instance.get(:default), k, "v", 0)
      end

      assert Router.dbsize(FerricStore.Instance.get(:default)) >= 4
    end

    test "keys from different shards all retrievable" do
      keys =
        for i <- 1..20 do
          k = ukey("multi_shard_#{i}")
          Router.put(FerricStore.Instance.get(:default), k, "val_#{i}", 0)
          k
        end

      for k <- keys do
        assert Router.get(FerricStore.Instance.get(:default), k) != nil,
               "Key #{k} should be retrievable"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Data durability through shard restart
  # ---------------------------------------------------------------------------

  describe "data durability through shard restart" do
    test "data survives shard GenServer crash and restart" do
      k = ukey("durability")
      Router.put(FerricStore.Instance.get(:default), k, "persistent_value", 0)

      # Confirm value is there
      assert "persistent_value" == Router.get(FerricStore.Instance.get(:default), k)

      # Flush pending writes to Bitcask before killing so data survives the crash.
      pid = shard_pid_for(k)
      assert is_pid(pid), "Expected to find shard PID for key #{k}"
      :ok = GenServer.call(pid, :flush)

      # Kill the owning shard process
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)

      # Wait for the DOWN message confirming the process died
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000

      # Wait for supervisor to restart the shard and init to complete
      ShardHelpers.wait_shards_alive()

      # The new shard process should have a different PID
      new_pid = shard_pid_for(k)
      assert is_pid(new_pid)
      assert new_pid != pid, "Expected a new process after restart"

      # Data persisted in Bitcask should survive the restart
      ShardHelpers.eventually(
        fn -> "persistent_value" == Router.get(FerricStore.Instance.get(:default), k) end,
        "Data should survive shard restart"
      )
    end

    test "ETS cache is rebuilt on warm after shard restart" do
      k = ukey("ets_rebuild")
      Router.put(FerricStore.Instance.get(:default), k, "rebuild_val", 0)
      Router.get(FerricStore.Instance.get(:default), k)

      # Confirm ETS has the value
      assert [{^k, "rebuild_val", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir_for(k), k)

      # Flush pending writes to Bitcask before killing so data survives the crash.
      pid = shard_pid_for(k)
      :ok = GenServer.call(pid, :flush)

      # Kill shard
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
      ShardHelpers.wait_shards_alive()

      # New ETS table is empty (fresh shard), but GET should warm it from Bitcask
      ShardHelpers.eventually(
        fn -> "rebuild_val" == Router.get(FerricStore.Instance.get(:default), k) end,
        "Value should be recovered from Bitcask after shard restart"
      )

      assert [{^k, "rebuild_val", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir_for(k), k)
    end

    test "multiple keys survive shard crash" do
      # Generate keys that all hash to the same shard
      base = ukey("multi_dur")
      target_shard = Router.shard_for(FerricStore.Instance.get(:default), base)
      same_shard_keys = find_n_keys_for_shard(target_shard, 5, "multi_dur")

      for k <- same_shard_keys do
        Router.put(FerricStore.Instance.get(:default), k, "val_#{k}", 0)
      end

      # Flush all pending async writes to disk before killing the shard.
      # With the async io_uring write path, puts are kernel-managed once
      # submitted, but rapid consecutive puts may still be in state.pending
      # (batched for the next flush). An explicit flush ensures all writes
      # are durable before we simulate a crash.
      pid = shard_pid_for(List.first(same_shard_keys))
      :ok = GenServer.call(pid, :flush)

      # Kill the shard
      pid = shard_pid_for(List.first(same_shard_keys))
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
      ShardHelpers.wait_shards_alive()

      # All keys should survive
      for k <- same_shard_keys do
        ShardHelpers.eventually(
          fn -> "val_#{k}" == Router.get(FerricStore.Instance.get(:default), k) end,
          "Key #{k} should survive shard restart"
        )
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers for cross-shard key generation
  # ---------------------------------------------------------------------------

  # Finds one unique key per shard (0..3) by brute-force generating keys
  # until each shard has at least one.
  defp find_keys_for_all_shards(prefix) do
    Enum.reduce_while(1..10_000, %{}, fn i, acc ->
      if map_size(acc) == 4 do
        {:halt, acc}
      else
        k = "#{prefix}_shard_probe_#{i}"
        shard = Router.shard_for(FerricStore.Instance.get(:default), k)

        if Map.has_key?(acc, shard) do
          {:cont, acc}
        else
          {:cont, Map.put(acc, shard, k)}
        end
      end
    end)
    |> Map.to_list()
  end

  # Finds N unique keys that all hash to the given shard index.
  defp find_n_keys_for_shard(shard_idx, n, prefix) do
    Enum.reduce_while(1..100_000, [], fn i, acc ->
      if length(acc) >= n do
        {:halt, acc}
      else
        k = "#{prefix}_probe_#{i}_#{:rand.uniform(999_999)}"

        if Router.shard_for(FerricStore.Instance.get(:default), k) == shard_idx do
          {:cont, [k | acc]}
        else
          {:cont, acc}
        end
      end
    end)
  end
end
