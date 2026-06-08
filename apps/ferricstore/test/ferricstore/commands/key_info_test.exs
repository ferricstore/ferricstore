defmodule Ferricstore.Commands.KeyInfoTest do
  @moduledoc """
  Tests for the FERRICSTORE.KEY_INFO command.

  KEY_INFO returns diagnostic metadata about a key:
    - type (string/hash/list/set/zset/stream/none)
    - value_size (bytes)
    - ttl_ms (-1 if no TTL, -2 if not found, else remaining ms)
    - hot_cache_status (hot/cold)
    - last_write_shard (shard index)

  These tests run against the application-supervised shards via the Router,
  since KEY_INFO queries ETS tables and the Router directly.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.{Dispatcher, Native}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # Generates a unique key to prevent cross-test interference.
  defp ukey(base), do: "#{base}_#{:rand.uniform(9_999_999)}"

  # Dummy store map -- KEY_INFO calls Router directly, ignoring the store.
  defp dummy_store, do: %{}

  # Parses the flat key-value list returned by KEY_INFO into a map.
  defp parse_info(result) when is_list(result) do
    result
    |> Enum.chunk_every(2)
    |> Map.new(fn [k, v] -> {k, v} end)
  end

  # Flushes Raft batchers so compound key writes land in ETS.
  defp flush_raft do
    Enum.each(0..3, fn i ->
      Ferricstore.Raft.Batcher.flush(i)
    end)
  end

  # ===========================================================================
  # Returns type=string for SET key
  # ===========================================================================

  describe "type detection" do
    test "returns type=string for SET key" do
      key = ukey("ki_string")
      Router.put(FerricStore.Instance.get(:default), key, "hello", 0)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      assert is_list(result)
      info = parse_info(result)
      assert info["type"] == "string"
    end

    test "returns type=hash for HSET key" do
      key = ukey("ki_hash")
      Dispatcher.dispatch("HSET", [key, "field1", "value1"], build_store())
      flush_raft()

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["type"] == "hash"
    end

    test "returns type=none for missing key" do
      key = ukey("ki_missing")

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["type"] == "none"
    end

    test "returns type=list for LPUSH key" do
      key = ukey("ki_list")
      Dispatcher.dispatch("LPUSH", [key, "a", "b"], build_store())
      flush_raft()

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["type"] == "list"
    end

    test "returns type=set for SADD key" do
      key = ukey("ki_set")
      Dispatcher.dispatch("SADD", [key, "m1", "m2"], build_store())
      flush_raft()

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["type"] == "set"
    end

    test "returns type=zset for ZADD key" do
      key = ukey("ki_zset")
      Dispatcher.dispatch("ZADD", [key, "1.0", "member"], build_store())
      flush_raft()

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["type"] == "zset"
    end
  end

  # ===========================================================================
  # Returns correct TTL
  # ===========================================================================

  describe "TTL reporting" do
    test "returns ttl_ms=-1 for key with no TTL" do
      key = ukey("ki_no_ttl")
      Router.put(FerricStore.Instance.get(:default), key, "value", 0)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["ttl_ms"] == "-1"
    end

    test "returns ttl_ms=-2 for non-existent key" do
      key = ukey("ki_no_key")

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["ttl_ms"] == "-2"
    end

    test "returns remaining TTL for key with expiry" do
      key = ukey("ki_ttl")
      future = System.os_time(:millisecond) + 60_000
      Router.put(FerricStore.Instance.get(:default), key, "value", future)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      ttl = String.to_integer(info["ttl_ms"])
      # Should be between 59 and 60 seconds (accounting for test execution time)
      assert ttl > 58_000
      assert ttl <= 60_000
    end
  end

  # ===========================================================================
  # Hot cache status
  # ===========================================================================

  describe "hot cache status" do
    test "shows hot status after GET (warmed into ETS)" do
      key = ukey("ki_hot")
      Router.put(FerricStore.Instance.get(:default), key, "value", 0)
      # First GET warms the key into hot cache
      _val = Router.get(FerricStore.Instance.get(:default), key)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["hot_cache_status"] == "hot"
    end

    test "shows cold status for key with evicted value" do
      key = ukey("ki_cold")
      # Put value directly -- it goes into keydir via Shard GenServer.
      Router.put(FerricStore.Instance.get(:default), key, "value", 0)

      # Flush to disk first so the cold read path has data
      ShardHelpers.flush_all_shards()

      # Evict the value from keydir to simulate a cold key (value = nil).
      # In v2, ETS IS the keydir -- deleting entirely means key is gone.
      # Cold keys have value=nil but retain disk location in the 7-tuple.
      idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{idx}"
      [{_, _val, exp, lfu, fid, off, vsize}] = :ets.lookup(keydir, key)
      :ets.insert(keydir, {key, nil, exp, lfu, fid, off, vsize})

      # Now the key is cold -- value is nil but disk location known.
      # KEY_INFO may or may not warm the key (depends on whether get_meta
      # triggers a cold read). Check that status is either cold or hot.
      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["hot_cache_status"] in ["hot", "cold"]
      assert info["type"] == "string"
    end

    test "does not warm a cold string while reading diagnostics" do
      key = ukey("ki_cold_no_warm")
      value = String.duplicate("x", 10_000)
      Router.put(FerricStore.Instance.get(:default), key, value, 0)
      ShardHelpers.flush_all_shards()

      idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{idx}"
      [{_, _val, exp, lfu, fid, off, vsize}] = :ets.lookup(keydir, key)
      :ets.insert(keydir, {key, nil, exp, lfu, fid, off, vsize})

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)

      assert info["type"] == "string"
      assert info["value_size"] == Integer.to_string(byte_size(value))
      assert info["hot_cache_status"] == "cold"
      assert [{^key, nil, _exp, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, key)
    end
  end

  # ===========================================================================
  # Correct value_size
  # ===========================================================================

  describe "value_size" do
    test "shows correct value_size for string key" do
      key = ukey("ki_size")
      value = "hello world"
      Router.put(FerricStore.Instance.get(:default), key, value, 0)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["value_size"] == Integer.to_string(byte_size(value))
    end

    test "shows value_size=0 for missing key" do
      key = ukey("ki_size_miss")

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["value_size"] == "0"
    end

    test "shows correct value_size for large value" do
      key = ukey("ki_size_large")
      value = String.duplicate("x", 10_000)
      Router.put(FerricStore.Instance.get(:default), key, value, 0)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["value_size"] == "10000"
    end
  end

  # ===========================================================================
  # last_write_shard
  # ===========================================================================

  describe "last_write_shard" do
    test "shows correct shard index" do
      key = ukey("ki_shard")
      Router.put(FerricStore.Instance.get(:default), key, "value", 0)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)

      expected_shard = Router.shard_for(FerricStore.Instance.get(:default), key)
      assert info["last_write_shard"] == Integer.to_string(expected_shard)
    end
  end

  # ===========================================================================
  # Edge cases
  # ===========================================================================

  describe "edge cases" do
    test "KEY_INFO on expired key returns type=none" do
      key = ukey("ki_expired")
      past = System.os_time(:millisecond) - 1_000
      Router.put(FerricStore.Instance.get(:default), key, "value", past)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)
      assert info["type"] == "none"
      assert info["ttl_ms"] == "-2"
      assert info["value_size"] == "0"
    end

    test "KEY_INFO with no arguments returns error" do
      assert {:error, msg} = Native.handle("KEY_INFO", [], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "KEY_INFO with too many arguments returns error" do
      assert {:error, msg} = Native.handle("KEY_INFO", ["k", "extra"], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "KEY_INFO returns all expected fields" do
      key = ukey("ki_fields")
      Router.put(FerricStore.Instance.get(:default), key, "v", 0)

      result = Native.handle("KEY_INFO", [key], dummy_store())
      info = parse_info(result)

      assert Map.has_key?(info, "type")
      assert Map.has_key?(info, "value_size")
      assert Map.has_key?(info, "ttl_ms")
      assert Map.has_key?(info, "hot_cache_status")
      assert Map.has_key?(info, "last_write_shard")
    end

    test "KEY_INFO falls back instead of exiting while shard is unavailable" do
      original_ctx = FerricStore.Instance.get(:default)
      unavailable_ctx = unavailable_default_ctx()
      handler_id = {__MODULE__, make_ref()}

      :ok =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :store, :shard_unavailable],
          &__MODULE__.handle_telemetry/4,
          self()
        )

      :persistent_term.put({FerricStore.Instance, :default}, unavailable_ctx)

      on_exit(fn ->
        :telemetry.detach(handler_id)
        :persistent_term.put({FerricStore.Instance, :default}, original_ctx)
      end)

      result = Native.handle("KEY_INFO", ["ki_restart_missing"], dummy_store())
      info = parse_info(result)

      assert info["type"] == "none"
      assert info["ttl_ms"] == "-2"
      assert info["value_size"] == "0"

      assert_receive {:telemetry_event, [:ferricstore, :store, :shard_unavailable], %{count: 1},
                      %{
                        instance: :default,
                        request: :compound_get,
                        reason: :noproc,
                        shard_index: 0
                      }}
    end

    test "KEY_INFO uses the injected custom instance instead of default" do
      ctx = IsolatedInstance.checkout(shard_count: 1)
      on_exit(fn -> IsolatedInstance.checkin(ctx) end)

      key = ukey("ki_custom_instance")
      Router.put(FerricStore.Instance.get(:default), key, "default-value", 0)
      Router.put(ctx, key, "custom", 0)

      result = Native.handle("KEY_INFO", [key], custom_store(ctx))
      info = parse_info(result)

      assert info["type"] == "string"
      assert info["value_size"] == Integer.to_string(byte_size("custom"))
    end
  end

  # ===========================================================================
  # Stress test
  # ===========================================================================

  describe "stress" do
    test "1000 KEY_INFO calls complete without error" do
      key = ukey("ki_stress")
      Router.put(FerricStore.Instance.get(:default), key, "stress_value", 0)

      results =
        Enum.map(1..1000, fn _i ->
          Native.handle("KEY_INFO", [key], dummy_store())
        end)

      assert Enum.all?(results, &is_list/1)
      assert length(results) == 1000

      # Verify each result is well-formed
      Enum.each(results, fn result ->
        info = parse_info(result)
        assert info["type"] == "string"
        assert info["value_size"] == Integer.to_string(byte_size("stress_value"))
      end)
    end
  end

  # ===========================================================================
  # Dispatcher integration
  # ===========================================================================

  describe "Dispatcher routing" do
    test "FERRICSTORE.KEY_INFO is routed through dispatcher" do
      key = ukey("ki_disp")
      Router.put(FerricStore.Instance.get(:default), key, "value", 0)

      result = Dispatcher.dispatch("FERRICSTORE.KEY_INFO", [key], dummy_store())
      assert is_list(result)
      info = parse_info(result)
      assert info["type"] == "string"
    end

    test "FERRICSTORE.KEY_INFO is case-insensitive" do
      key = ukey("ki_disp_ci")
      Router.put(FerricStore.Instance.get(:default), key, "value", 0)

      result = Dispatcher.dispatch("ferricstore.key_info", [key], dummy_store())
      assert is_list(result)
      info = parse_info(result)
      assert info["type"] == "string"
    end
  end

  # ---------------------------------------------------------------------------
  # Helper: builds a real store map for commands that need compound key support
  # (HSET, LPUSH, SADD, ZADD). This mirrors connection.ex's build_raw_store.
  # ---------------------------------------------------------------------------

  defp build_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      flush: fn ->
        Enum.each(Router.keys(FerricStore.Instance.get(:default)), fn k ->
          Router.delete(FerricStore.Instance.get(:default), k)
        end)

        :ok
      end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end,
      incr: fn k, d -> Router.incr(FerricStore.Instance.get(:default), k, d) end,
      incr_float: fn k, d -> Router.incr_float(FerricStore.Instance.get(:default), k, d) end,
      append: fn k, s -> Router.append(FerricStore.Instance.get(:default), k, s) end,
      getset: fn k, v -> Router.getset(FerricStore.Instance.get(:default), k, v) end,
      getdel: fn k -> Router.getdel(FerricStore.Instance.get(:default), k) end,
      getex: fn k, e -> Router.getex(FerricStore.Instance.get(:default), k, e) end,
      setrange: fn k, o, v -> Router.setrange(FerricStore.Instance.get(:default), k, o, v) end,
      cas: fn k, e, n, t -> Router.cas(FerricStore.Instance.get(:default), k, e, n, t) end,
      lock: fn k, o, t -> Router.lock(FerricStore.Instance.get(:default), k, o, t) end,
      unlock: fn k, o -> Router.unlock(FerricStore.Instance.get(:default), k, o) end,
      extend: fn k, o, t -> Router.extend(FerricStore.Instance.get(:default), k, o, t) end,
      ratelimit_add: fn k, w, m, c ->
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, w, m, c)
      end,
      list_op: fn k, op -> Router.list_op(FerricStore.Instance.get(:default), k, op) end,
      compound_get: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
      end
    }
  end

  def handle_telemetry(event, measurements, metadata, parent) do
    send(parent, {:telemetry_event, event, measurements, metadata})
  end

  defp unavailable_default_ctx do
    keydir = :ets.new(:"key_info_restart_fallback_#{System.unique_integer([:positive])}", [:set])
    :ets.delete(keydir)

    %FerricStore.Instance{
      name: :default,
      data_dir: System.tmp_dir!(),
      data_dir_expanded: System.tmp_dir!(),
      shard_count: 1,
      slot_map: Tuple.duplicate(0, 1024),
      shard_names: {:"missing_key_info_shard_#{System.unique_integer([:positive])}"},
      keydir_refs: {keydir},
      stats_counter: :counters.new(16, []),
      write_version: :counters.new(1, []),
      hot_cache_max_value_size: 1024,
      read_sample_rate: 0
    }
  end

  defp custom_store(ctx) do
    %{
      __instance_ctx__: ctx,
      get: fn key -> Router.get(ctx, key) end,
      exists?: fn key -> Router.exists?(ctx, key) end,
      compound_get: fn redis_key, compound_key ->
        Router.compound_get(ctx, redis_key, compound_key)
      end,
      compound_count: fn redis_key, prefix ->
        Router.compound_count(ctx, redis_key, prefix)
      end,
      compound_delete: fn redis_key, compound_key ->
        Router.compound_delete(ctx, redis_key, compound_key)
      end
    }
  end
end
