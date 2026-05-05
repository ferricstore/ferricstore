defmodule FerricstoreServer.Spec.CommandFunctionalityTest do
  @moduledoc """
  Spec-driven tests for Section 2 (Command Functionality Tests) of the
  FerricStore test plan.

  This file covers ONLY the test IDs that are not yet exercised by existing
  test modules. Each test is annotated with its spec ID for traceability.

  Sections covered:
    2.7  TTL Expiry Correctness   — TTL-002, TTL-004, TTL-010
    2.8  Stream Commands          — (no new tests; unimplemented commands skipped)
    2.15 Native Commands
      2.15.2 RATELIMIT.ADD        — RL-005, RL-006
      2.15.4 FERRICSTORE.CONFIG   — CFG-009, CFG-010 (command-handler level)
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Namespace
  alias Ferricstore.Commands.Native
  alias Ferricstore.Commands.Strings
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.MockStore
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  # Generates a unique key to prevent cross-test interference.
  defp ukey(base), do: "spec_#{base}_#{System.unique_integer([:positive])}"

  # Dummy store map -- native commands ignore it and call Router directly.
  defp dummy_store, do: %{}

  # Builds a real store map backed by the application-supervised shards.
  # This mirrors the store construction in Connection.build_raw_store/0.
  defp build_real_store do
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

  # ===========================================================================
  # 2.7  TTL Expiry Correctness
  # ===========================================================================

  describe "2.7 TTL Expiry — TTL-002: EXISTS returns 0 after expiry" do
    @tag :spec_ttl_002
    test "EXISTS returns 0 for expired key" do
      past = System.os_time(:millisecond) - 1_000
      store = MockStore.make(%{"ttl002" => {"bar", past}})
      assert 0 == Strings.handle("EXISTS", ["ttl002"], store)
    end
  end

  describe "2.7 TTL Expiry — TTL-004: SCAN skips expired keys" do
    @tag :spec_ttl_004
    test "SCAN does not return expired keys (integration)" do
      key = ukey("ttl004")
      past = System.os_time(:millisecond) - 1_000

      # Insert an expired key into the real store.
      Router.put(FerricStore.Instance.get(:default), key, "gone", past)

      # Trigger expiry sweep on the relevant shard so the key is removed.
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      shard_name = Router.shard_name(FerricStore.Instance.get(:default), shard_idx)
      GenServer.call(shard_name, :expiry_sweep)

      # After sweep, the key should not appear in KEYS.
      all_keys = Router.keys(FerricStore.Instance.get(:default))

      refute key in all_keys,
             "Expired key #{key} should not appear in keys after sweep"
    end
  end

  describe "2.7 TTL Expiry — TTL-010: hash field expiry via HEXPIRE" do
    @tag :spec_ttl_010
    test "HEXPIRE expires individual hash fields (integration)" do
      alias Ferricstore.Commands.Dispatcher

      key = ukey("ttl010")
      store = build_real_store()

      # Create a hash with two fields.
      assert 2 == Dispatcher.dispatch("HSET", [key, "f1", "v1", "f2", "v2"], store)

      # Verify both fields are accessible.
      assert "v1" == Dispatcher.dispatch("HGET", [key, "f1"], store)
      assert "v2" == Dispatcher.dispatch("HGET", [key, "f2"], store)

      # Expire only f1 with 2 second TTL.
      result = Dispatcher.dispatch("HEXPIRE", [key, "2", "FIELDS", "1", "f1"], store)
      assert [1] == result

      # Verify HTTL returns a non-negative value for f1.
      [ttl] = Dispatcher.dispatch("HTTL", [key, "FIELDS", "1", "f1"], store)
      assert ttl >= 0

      # Wait for f1 to expire.
      Process.sleep(2_200)

      # f1 should be gone but f2 should remain.
      assert nil == Dispatcher.dispatch("HGET", [key, "f1"], store)
      assert "v2" == Dispatcher.dispatch("HGET", [key, "f2"], store)
    end

    @tag :spec_ttl_010
    test "key-level EXPIRE returns 0 for hash keys (compound key architecture)" do
      # FerricStore stores hash fields as compound keys without a top-level
      # sentinel entry. Therefore, key-level EXPIRE returns 0 for hash keys.
      # This is a known architectural deviation from Redis. Use HEXPIRE for
      # per-field expiry instead.
      alias Ferricstore.Commands.Dispatcher

      key = ukey("ttl010_keylevel")
      store = build_real_store()

      # Create a hash.
      assert 1 == Dispatcher.dispatch("HSET", [key, "f1", "v1"], store)

      # Key-level EXPIRE returns 0 because there is no top-level key.
      assert 0 == Dispatcher.dispatch("EXPIRE", [key, "1"], store)
    end
  end

  # ===========================================================================
  # 2.15.2  RATELIMIT.ADD
  # ===========================================================================

  describe "2.15.2 RATELIMIT.ADD — RL-005: separate namespaces are independent" do
    @tag :spec_rl_005
    test "rate limits for different keys are independent" do
      key_a = ukey("rl005_a")
      key_b = ukey("rl005_b")

      # Fill up key_a to its limit of 5.
      for _ <- 1..5 do
        ["allowed" | _] = Native.handle("RATELIMIT.ADD", [key_a, "10000", "5"], dummy_store())
      end

      # key_a is now at limit.
      ["denied" | _] = Native.handle("RATELIMIT.ADD", [key_a, "10000", "5"], dummy_store())

      # key_b should still be fully available -- completely independent.
      result_b = Native.handle("RATELIMIT.ADD", [key_b, "10000", "5"], dummy_store())
      assert ["allowed", 1, 4, _ms_reset] = result_b

      # Add a few more to key_b -- should work.
      result_b2 = Native.handle("RATELIMIT.ADD", [key_b, "10000", "5"], dummy_store())
      assert ["allowed", 2, 3, _ms_reset] = result_b2
    end
  end

  describe "2.15.2 RATELIMIT.ADD — RL-006: different windows on same key" do
    @tag :spec_rl_006
    test "different window sizes track independently per key" do
      # The rate limiter uses the key string as the namespace, so to test
      # different windows we use different keys (as one would in production:
      # rate:u1:minute vs rate:u1:hour).
      key_minute = ukey("rl006_min")
      key_hour = ukey("rl006_hour")

      # Short window (500ms), limit 3 -- CI-friendly timing.
      for _ <- 1..3 do
        ["allowed" | _] = Native.handle("RATELIMIT.ADD", [key_minute, "500", "3"], dummy_store())
      end

      # Minute-window key is now at limit.
      result_min = Native.handle("RATELIMIT.ADD", [key_minute, "500", "3"], dummy_store())
      assert ["denied" | _] = result_min

      # Hour-window key with higher limit should still be available.
      result_hour = Native.handle("RATELIMIT.ADD", [key_hour, "3600000", "1000"], dummy_store())
      assert ["allowed", 1, 999, _ms_reset] = result_hour

      # The sliding window approximation requires waiting for 2x the window
      # duration to fully expire the previous window's count. Sleep for
      # 1200ms (>= 500ms * 2 + margin) so that both the current and previous
      # windows are fully expired.
      Process.sleep(1_200)

      # Short window should have fully reset.
      result_min_after = Native.handle("RATELIMIT.ADD", [key_minute, "500", "3"], dummy_store())
      assert ["allowed" | _] = result_min_after
    end
  end

  # ===========================================================================
  # 2.15.4  FERRICSTORE.CONFIG
  # ===========================================================================

  describe "2.15.4 FERRICSTORE.CONFIG — CFG-009: removed durability field via command" do
    setup do
      NamespaceConfig.reset_all()
      on_exit(fn -> NamespaceConfig.reset_all() end)
      :ok
    end

    @tag :spec_cfg_009
    test "FERRICSTORE.CONFIG SET durability returns unknown field error" do
      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["SET", "s", "durability", "invalid"],
          MockStore.make()
        )

      assert {:error, msg} = result
      assert msg =~ "unknown namespace config field 'durability'"
    end

    @tag :spec_cfg_009
    test "FERRICSTORE.CONFIG SET durability 'sync' returns unknown field error" do
      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["SET", "s", "durability", "sync"],
          MockStore.make()
        )

      assert {:error, msg} = result
      assert msg =~ "unknown namespace config field 'durability'"
    end
  end

  describe "2.15.4 FERRICSTORE.CONFIG — CFG-010: invalid window_ms via command" do
    setup do
      NamespaceConfig.reset_all()
      on_exit(fn -> NamespaceConfig.reset_all() end)
      :ok
    end

    @tag :spec_cfg_010
    test "FERRICSTORE.CONFIG SET window_ms -1 returns error" do
      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["SET", "s", "window_ms", "-1"],
          MockStore.make()
        )

      assert {:error, msg} = result
      assert msg =~ "positive integer"
    end

    @tag :spec_cfg_010
    test "FERRICSTORE.CONFIG SET window_ms 0 returns error" do
      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["SET", "s", "window_ms", "0"],
          MockStore.make()
        )

      assert {:error, msg} = result
      assert msg =~ "positive integer"
    end

    @tag :spec_cfg_010
    test "FERRICSTORE.CONFIG SET window_ms non-integer returns error" do
      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["SET", "s", "window_ms", "abc"],
          MockStore.make()
        )

      assert {:error, _msg} = result
    end
  end

  describe "2.15.4 FERRICSTORE.CONFIG — CFG-001: unconfigured prefix returns defaults" do
    setup do
      NamespaceConfig.reset_all()
      on_exit(fn -> NamespaceConfig.reset_all() end)
      :ok
    end

    @tag :spec_cfg_001
    test "GET unconfigured prefix returns built-in defaults via command handler" do
      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["GET", "newprefix"],
          MockStore.make()
        )

      # Should return flat key-value list with defaults.
      assert is_list(result)
      assert "prefix" in result
      assert "newprefix" in result
      assert "window_ms" in result
      assert "1" in result
      refute "durability" in result
    end
  end

  describe "2.15.4 FERRICSTORE.CONFIG — CFG-004: GET after SET reflects changes" do
    setup do
      NamespaceConfig.reset_all()
      on_exit(fn -> NamespaceConfig.reset_all() end)
      :ok
    end

    @tag :spec_cfg_004
    test "FERRICSTORE.CONFIG GET after SET returns new values" do
      store = MockStore.make()

      # Set window_ms for the "sensor" prefix.
      assert :ok =
               Namespace.handle(
                 "FERRICSTORE.CONFIG",
                 ["SET", "sensor", "window_ms", "50"],
                 store
               )

      assert {:error, msg} =
               Namespace.handle(
                 "FERRICSTORE.CONFIG",
                 ["SET", "sensor", "durability", "async"],
                 store
               )

      assert msg =~ "unknown namespace config field 'durability'"

      # GET should reflect the supported change only.
      result = Namespace.handle("FERRICSTORE.CONFIG", ["GET", "sensor"], store)
      assert is_list(result)
      assert "50" in result
      refute "durability" in result
    end
  end

  describe "2.15.4 FERRICSTORE.CONFIG — CFG-005: GET with no prefix returns all" do
    setup do
      NamespaceConfig.reset_all()
      on_exit(fn -> NamespaceConfig.reset_all() end)
      :ok
    end

    @tag :spec_cfg_005
    test "FERRICSTORE.CONFIG GET (no prefix) returns all configured prefixes" do
      store = MockStore.make()

      Namespace.handle("FERRICSTORE.CONFIG", ["SET", "rate", "window_ms", "10"], store)
      Namespace.handle("FERRICSTORE.CONFIG", ["SET", "session", "window_ms", "5"], store)

      result = Namespace.handle("FERRICSTORE.CONFIG", ["GET"], store)
      assert is_list(result)
      assert "rate" in result
      assert "session" in result
    end
  end

  describe "2.15.4 FERRICSTORE.CONFIG — CFG-006: RESET removes override" do
    setup do
      NamespaceConfig.reset_all()
      on_exit(fn -> NamespaceConfig.reset_all() end)
      :ok
    end

    @tag :spec_cfg_006
    test "FERRICSTORE.CONFIG SET then RESET restores defaults" do
      store = MockStore.make()

      # Set a custom value.
      assert :ok =
               Namespace.handle(
                 "FERRICSTORE.CONFIG",
                 ["SET", "sensor", "window_ms", "50"],
                 store
               )

      # Verify it's set.
      result_before = Namespace.handle("FERRICSTORE.CONFIG", ["GET", "sensor"], store)
      assert "50" in result_before

      # Reset.
      assert :ok =
               Namespace.handle(
                 "FERRICSTORE.CONFIG",
                 ["RESET", "sensor"],
                 store
               )

      # Verify defaults are restored.
      result_after = Namespace.handle("FERRICSTORE.CONFIG", ["GET", "sensor"], store)
      assert "1" in result_after
      refute "durability" in result_after
    end
  end
end
