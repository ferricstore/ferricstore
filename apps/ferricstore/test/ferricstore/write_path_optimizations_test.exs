defmodule Ferricstore.WritePathOptimizationsTest do
  @moduledoc """
  Remaining write path optimization tests:

  1. LFU touch skip optimization
  2. WriteVersion atomic counters
  3. Router value size limits
  4. RESP parser value size limits
  5. WARaft backend loading
  6. exists_fast? (ETS-direct existence check on write path)

  See also:
  - write_path_nosync_test.exs — StateMachine nosync + BitcaskWriter
  - write_path_quorum_test.exs — Router quorum bypass + async WAL
  - write_path_hash_tag_test.exs — Hash tag routing
  - write_path_set_options_test.exs — SET EXAT/PXAT/GET/KEEPTTL
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  import Bitwise

  alias Ferricstore.Store.{BitcaskWriter, LFU, Router, WriteVersion}
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.wait_shards_alive()
    end)
  end

  # Helper: unique key with a given prefix
  defp ukey(prefix), do: "#{prefix}:#{:rand.uniform(9_999_999)}"

  # Safe ETS delete that doesn't raise if the table is already gone
  defp safe_ets_delete(table) do
    try do
      :ets.delete(table)
    rescue
      ArgumentError -> :ok
    end
  end

  # Helper: builds a store map backed by the real Router
  defp real_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      incr: fn k, d -> Router.incr(FerricStore.Instance.get(:default), k, d) end,
      incr_float: fn k, d -> Router.incr_float(FerricStore.Instance.get(:default), k, d) end,
      append: fn k, s -> Router.append(FerricStore.Instance.get(:default), k, s) end,
      getset: fn k, v -> Router.getset(FerricStore.Instance.get(:default), k, v) end,
      getdel: fn k -> Router.getdel(FerricStore.Instance.get(:default), k) end,
      getex: fn k, e -> Router.getex(FerricStore.Instance.get(:default), k, e) end,
      setrange: fn k, o, v -> Router.setrange(FerricStore.Instance.get(:default), k, o, v) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end
    }
  end

  # =========================================================================
  # LFU touch skip optimization
  # =========================================================================

  describe "LFU touch skip optimization" do
    test "pack and unpack are inverse operations" do
      for ldt <- [0, 1, 100, 0xFFFF], counter <- [0, 5, 100, 255] do
        packed = LFU.pack(ldt, counter)
        assert {ldt, counter} == LFU.unpack(packed)
      end
    end

    test "initial value has counter 5" do
      packed = LFU.initial()
      {_ldt, counter} = LFU.unpack(packed)
      assert counter == 5
    end

    test "same-minute access with unchanged counter does not write to ETS" do
      # Create a standalone ETS table to observe writes
      table = :ets.new(:lfu_test, [:set, :public])
      on_exit(fn -> safe_ets_delete(table) end)

      key = "lfu_skip_test"
      now_min = LFU.now_minutes()
      # Counter at 255 will never increment (probability is ~0 with log_factor=10)
      packed = LFU.pack(now_min, 255)

      :ets.insert(table, {key, "val", 0, packed, 1, 0, 3})

      # Touch should be a no-op (same minute, counter won't increment at 255)
      LFU.touch(table, key, packed)

      [{^key, _v, _e, new_packed, _fid, _off, _vsize}] = :ets.lookup(table, key)
      # The packed value should remain the same
      assert new_packed == packed
    end

    test "different minute DOES update ETS (ldt changes)" do
      table = :ets.new(:lfu_test2, [:set, :public])
      on_exit(fn -> safe_ets_delete(table) end)

      key = "lfu_ldt_test"
      # Use a past minute so the current minute is different
      old_min = LFU.now_minutes() - 2 &&& 0xFFFF
      packed = LFU.pack(old_min, 100)

      :ets.insert(table, {key, "val", 0, packed, 1, 0, 3})

      LFU.touch(table, key, packed)

      [{^key, _v, _e, new_packed, _fid, _off, _vsize}] = :ets.lookup(table, key)
      # ldt should be updated to current minute, so packed changes
      assert new_packed != packed
      {new_ldt, _new_counter} = LFU.unpack(new_packed)
      assert new_ldt == LFU.now_minutes()
    end

    test "effective_counter applies decay based on elapsed time" do
      old_min = LFU.now_minutes() - 5 &&& 0xFFFF
      packed = LFU.pack(old_min, 100)

      effective = LFU.effective_counter(packed)
      # With default decay_time=1, 5 elapsed minutes should decay by 5
      assert effective == 95
    end

    test "10000 reads of same key — counter is reasonable (not 0, not 255)" do
      key = ukey("lfu_freq")
      Router.put(FerricStore.Instance.get(:default), key, "val", 0)

      for _ <- 1..10_000 do
        Router.get(FerricStore.Instance.get(:default), key)
      end

      idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{idx}"

      case :ets.lookup(keydir, key) do
        [{^key, _v, _e, packed, _fid, _off, _vsize}] ->
          {_ldt, counter} = LFU.unpack(packed)
          # After 10000 accesses with log_factor=10, counter should be > initial (5)
          # but probabilistic increment means it won't be 10000
          assert counter > 5
          assert counter <= 255

        [] ->
          flunk("Key not found in keydir")
      end
    end

    test "elapsed_minutes handles 16-bit wraparound" do
      # now=2, ldt=0xFFFE (near wrap): should be 4 minutes elapsed
      assert LFU.elapsed_minutes(2, 0xFFFE) == 4
      assert LFU.elapsed_minutes(0, 0xFFFE) == 2
    end
  end

  # =========================================================================
  # WriteVersion atomic counters
  # =========================================================================

  describe "WriteVersion atomic counters" do
    test "init creates counters accessible via get" do
      # Already initialized in application startup
      for i <- 0..3 do
        v = WriteVersion.get(i)
        assert is_integer(v)
        assert v >= 0
      end
    end

    test "increment atomically increases version" do
      idx = 0
      before = WriteVersion.get(idx)
      WriteVersion.increment(idx)
      after_val = WriteVersion.get(idx)
      assert after_val == before + 1
    end

    test "concurrent increments are atomic" do
      idx = 1
      before = WriteVersion.get(idx)

      tasks =
        for _ <- 1..100 do
          Task.async(fn -> WriteVersion.increment(idx) end)
        end

      Enum.each(tasks, &Task.await(&1, 10_000))

      after_val = WriteVersion.get(idx)
      assert after_val == before + 100
    end

    test "write through Router increments version" do
      key = ukey("wv")
      idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      before = WriteVersion.get(idx)

      Router.put(FerricStore.Instance.get(:default), key, "val", 0)

      after_val = WriteVersion.get(idx)
      assert after_val > before
    end

    test "different shards have independent counters" do
      # Find two keys that map to different shards
      {k0, k1} = find_keys_on_different_shards()
      idx0 = Router.shard_for(FerricStore.Instance.get(:default), k0)
      idx1 = Router.shard_for(FerricStore.Instance.get(:default), k1)

      v0_before = WriteVersion.get(idx0)
      v1_before = WriteVersion.get(idx1)

      Router.put(FerricStore.Instance.get(:default), k0, "val", 0)

      v0_after = WriteVersion.get(idx0)
      v1_after = WriteVersion.get(idx1)

      assert v0_after > v0_before
      assert v1_after == v1_before
    end
  end

  # =========================================================================
  # Router value size limits
  # =========================================================================

  describe "Router value size limits" do
    test "key too large (> 65535 bytes) rejected" do
      big_key = String.duplicate("k", 65_536)
      assert {:error, _} = Router.put(FerricStore.Instance.get(:default), big_key, "val", 0)
    end

    test "key at exactly 65535 bytes accepted" do
      key_at_limit = String.duplicate("k", 65_535)
      # This should succeed (at limit, not over)
      result = Router.put(FerricStore.Instance.get(:default), key_at_limit, "val", 0)
      assert result == :ok
      Router.delete(FerricStore.Instance.get(:default), key_at_limit)
    end

    test "value too large (>= 512MB) rejected" do
      # We can't actually allocate 512MB in a test, but we can verify the limit
      assert Router.max_value_size() == 512 * 1024 * 1024
    end

    test "empty key rejected by SET command" do
      store = real_store()
      result = Ferricstore.Commands.Strings.handle("SET", ["", "val"], store)
      assert {:error, "ERR empty key"} = result
    end

    test "GET on empty key rejected" do
      store = real_store()
      result = Ferricstore.Commands.Strings.handle("GET", [""], store)
      assert {:error, "ERR empty key"} = result
    end
  end

  # =========================================================================
  # RESP parser value size limits
  # =========================================================================

  if Code.ensure_loaded?(FerricstoreServer.Resp.Parser) do
    describe "RESP parser value size limits" do
      test "bulk string within default limit parses successfully" do
        data = "$5\r\nhello\r\n"
        assert {:ok, ["hello"], ""} = FerricstoreServer.Resp.Parser.parse(data)
      end

      test "bulk string exceeding custom limit returns error" do
        # Set a very small limit
        data = "$100\r\n" <> String.duplicate("x", 100) <> "\r\n"

        assert {:error, {:value_too_large, 100, 50}} =
                 FerricstoreServer.Resp.Parser.parse(data, 50)
      end

      test "hard cap of 64MB enforced regardless of config" do
        assert FerricstoreServer.Resp.Parser.hard_cap_bytes() == 67_108_864

        # Even with a huge limit, 64MB+ is rejected
        huge_len = 67_108_865
        data = "$#{huge_len}\r\n"

        assert {:error, {:value_too_large, ^huge_len, _}} =
                 FerricstoreServer.Resp.Parser.parse(data, 100_000_000)
      end

      test "bulk string at exact limit parses successfully" do
        limit = 10
        data = "$10\r\n" <> String.duplicate("x", 10) <> "\r\n"
        assert {:ok, [val], ""} = FerricstoreServer.Resp.Parser.parse(data, limit)
        assert byte_size(val) == 10
      end

      test "null bulk string always parses" do
        assert {:ok, [nil], ""} = FerricstoreServer.Resp.Parser.parse("$-1\r\n")
      end
    end
  end

  # =========================================================================
  # Application WARaft loading
  # =========================================================================

  describe "application WARaft loading" do
    test "WARaft is the only selected backend" do
      assert Ferricstore.Raft.Backend.selected() == :waraft
      assert Ferricstore.Raft.Backend.running_or_selected() == :waraft
    end

    test "WARaft commit batcher is application-supervised" do
      assert is_pid(Process.whereis(Ferricstore.Raft.Batcher.batcher_name(0)))

      acceptor = :wa_raft_acceptor.registered_name(:ferricstore_waraft_backend, 1)
      assert is_pid(Process.whereis(acceptor))
    end
  end

  # =========================================================================
  # exists_fast? (ETS-direct existence check on write path)
  # =========================================================================

  describe "exists_fast? ETS-direct check" do
    test "returns true for existing key" do
      key = ukey("ef")
      Router.put(FerricStore.Instance.get(:default), key, "val", 0)
      assert Router.exists_fast?(FerricStore.Instance.get(:default), key) == true
    end

    test "returns false for non-existent key" do
      assert Router.exists_fast?(
               FerricStore.Instance.get(:default),
               "nonexistent_key_#{:rand.uniform(999_999)}"
             ) == false
    end

    test "returns false for expired key" do
      key = ukey("ef_exp")
      expire_at = System.os_time(:millisecond) + 500
      Router.put(FerricStore.Instance.get(:default), key, "ephemeral", expire_at)

      assert Router.exists_fast?(FerricStore.Instance.get(:default), key) == true
      Process.sleep(600)
      assert Router.exists_fast?(FerricStore.Instance.get(:default), key) == false
    end

    test "returns true for cold key (value=nil but in keydir)" do
      # Set a value larger than hot_cache_max_value_size to make it cold
      key = ukey("ef_cold")
      threshold = :persistent_term.get(:ferricstore_hot_cache_max_value_size, 65_536)
      large_value = String.duplicate("c", threshold + 1)

      Router.put(FerricStore.Instance.get(:default), key, large_value, 0)
      BitcaskWriter.flush_all()

      # Cold key should still be "exists" (nil value but valid keydir entry)
      assert Router.exists_fast?(FerricStore.Instance.get(:default), key) == true
    end
  end

  # =========================================================================
  # Helpers
  # =========================================================================

  defp find_keys_on_different_shards do
    # Generate keys until we find two that map to different shards
    Enum.reduce_while(1..1000, nil, fn i, _acc ->
      k = "diff:#{i}"
      idx = Router.shard_for(FerricStore.Instance.get(:default), k)

      if idx != Router.shard_for(FerricStore.Instance.get(:default), "diff:1") do
        {:halt, {"diff:1", k}}
      else
        {:cont, nil}
      end
    end)
  end
end
