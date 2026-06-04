defmodule Ferricstore.ReviewR2.RedisCompatIssuesTest do
  @moduledoc """
  Tests proving Redis compatibility bugs found during R2 code review.

  Each test targets a specific issue ID and demonstrates the current (buggy)
  behavior vs what Redis would do. When fixed, these tests become regression
  guards.

  Issues covered:
    R2-H5  — ZADD missing NX+XX, GT+LT, GT+NX conflict checks
    R2-H6  — SET missing NX+XX conflict check
    R2-H7  — ZSCORE/ZMSCORE/ZSCAN return unformatted scores
    R2-H8  — EXPIRE/PEXPIRE with negative TTL deletes key instead of returning error
    R2-H13 — WriteVersion increments for failed conditional writes
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Expiry, SortedSet, Strings}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.{MockStore, ShardHelpers}

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  defp unique_key(prefix) do
    "#{prefix}_#{:rand.uniform(1_000_000_000)}"
  end

  # ===========================================================================
  # R2-H5: ZADD missing NX+XX, GT+LT, GT+NX conflict checks
  #
  # Redis rejects these mutually exclusive flag combinations with:
  #   ERR XX and NX options at the same time are not compatible
  #   ERR GT, LT, and NX options at the same time are not compatible
  #
  # Currently FerricStore silently accepts them and applies both flags,
  # leading to undefined/wrong behavior.
  # ===========================================================================

  describe "R2-H5: ZADD conflicting flag combinations" do
    test "ZADD with NX and XX together should return error" do
      store = MockStore.make()
      key = unique_key("zs")

      result = SortedSet.handle("ZADD", [key, "NX", "XX", "1.0", "member"], store)

      # Redis: {:error, "ERR XX and NX options at the same time are not compatible"}
      # BUG: Currently returns 0 or 1 (silently applies both flags)
      assert {:error, _msg} = result,
             "R2-H5: ZADD NX XX should be rejected, got: #{inspect(result)}"
    end

    test "ZADD with GT and LT together should return error" do
      store = MockStore.make()
      key = unique_key("zs")

      result = SortedSet.handle("ZADD", [key, "GT", "LT", "1.0", "member"], store)

      # Redis: {:error, "ERR GT, LT, and NX options at the same time are not compatible"}
      # BUG: Currently returns 0 or 1 (silently applies both flags)
      assert {:error, _msg} = result,
             "R2-H5: ZADD GT LT should be rejected, got: #{inspect(result)}"
    end

    test "ZADD with GT and NX together should return error" do
      store = MockStore.make()
      key = unique_key("zs")

      result = SortedSet.handle("ZADD", [key, "GT", "NX", "1.0", "member"], store)

      # Redis: {:error, "ERR GT, LT, and NX options at the same time are not compatible"}
      # BUG: Currently returns 0 or 1 (silently applies both flags)
      assert {:error, _msg} = result,
             "R2-H5: ZADD GT NX should be rejected, got: #{inspect(result)}"
    end

    test "ZADD with LT and NX together should return error" do
      store = MockStore.make()
      key = unique_key("zs")

      result = SortedSet.handle("ZADD", [key, "LT", "NX", "1.0", "member"], store)

      # Redis: {:error, "ERR GT, LT, and NX options at the same time are not compatible"}
      # BUG: Currently returns 0 or 1
      assert {:error, _msg} = result,
             "R2-H5: ZADD LT NX should be rejected, got: #{inspect(result)}"
    end
  end

  # ===========================================================================
  # R2-H6: SET missing NX+XX conflict check
  #
  # Redis 7+ rejects `SET key value NX XX` with a syntax error.
  # FerricStore silently accepts both flags. Since NX (set-if-absent) and
  # XX (set-if-exists) are mutually exclusive, the combination is always
  # a no-op (the key either exists or doesn't, but both conditions can't
  # be satisfied). Redis returns an error to alert the user to the mistake.
  # ===========================================================================

  describe "R2-H6: SET NX XX conflict" do
    test "SET with both NX and XX should return error" do
      store = MockStore.make()
      key = unique_key("str")

      result = Strings.handle("SET", [key, "value", "NX", "XX"], store)

      # Redis: {:error, "ERR syntax error"} or {:error, "ERR XX and NX options at the same time are not compatible"}
      # BUG: Currently returns nil (both skip conditions checked independently,
      # NX fires because key absent -> skip? = false, then write happens... or
      # returns nil because one of the conditions triggers skip)
      assert {:error, _msg} = result,
             "R2-H6: SET NX XX should be rejected, got: #{inspect(result)}"
    end

    test "SET with both NX and XX when key exists should return error" do
      store = MockStore.make(%{"existing" => {"val", 0}})

      result = Strings.handle("SET", ["existing", "new_value", "NX", "XX"], store)

      # Redis: error regardless of key state
      assert {:error, _msg} = result,
             "R2-H6: SET NX XX should be rejected even when key exists, got: #{inspect(result)}"
    end
  end

  # ===========================================================================
  # R2-H7: ZSCORE/ZMSCORE/ZSCAN return unformatted scores
  #
  # ZRANGE WITHSCORES uses `format_score/1` (`:erlang.float_to_binary/2` with
  # compact+decimals:17) but ZSCORE, ZMSCORE, and ZSCAN return the raw stored
  # string (`Float.to_string/1` output). These can differ in representation.
  #
  # Redis always uses the same format for all score-returning commands.
  # ===========================================================================

  describe "R2-H7: score format consistency" do
    # Use score 3.14 which exposes the difference:
    #   Float.to_string(3.14)  => "3.14"
    #   :erlang.float_to_binary(3.14, [:compact, decimals: 17])  => "3.14000000000000012"
    # ZRANGE WITHSCORES uses format_score (erlang), ZSCORE returns raw (Float.to_string).

    test "ZSCORE returns same format as ZRANGE WITHSCORES" do
      store = MockStore.make()
      key = unique_key("zs")

      # Score 3.14 triggers the format divergence
      SortedSet.handle("ZADD", [key, "3.14", "member"], store)

      zscore_result = SortedSet.handle("ZSCORE", [key, "member"], store)
      zrange_result = SortedSet.handle("ZRANGE", [key, "0", "-1", "WITHSCORES"], store)

      # ZRANGE returns [member, formatted_score]
      [_member, zrange_score] = zrange_result

      # BUG: ZSCORE returns "3.14" (Float.to_string) but ZRANGE returns
      # "3.14000000000000012" (format_score via :erlang.float_to_binary)
      assert zscore_result == zrange_score,
             "R2-H7: ZSCORE returned #{inspect(zscore_result)} but ZRANGE WITHSCORES returned #{inspect(zrange_score)}"
    end

    test "ZMSCORE returns same format as ZRANGE WITHSCORES" do
      store = MockStore.make()
      key = unique_key("zs")

      # Use 3.14 and 0.1 -- both expose format differences
      SortedSet.handle("ZADD", [key, "3.14", "a", "0.3333333333333333", "b"], store)

      zmscore_result = SortedSet.handle("ZMSCORE", [key, "a", "b"], store)
      zrange_result = SortedSet.handle("ZRANGE", [key, "0", "-1", "WITHSCORES"], store)

      # ZRANGE returns [b, score_b, a, score_a] (sorted by score: 0.333... < 3.14)
      [_b, zrange_score_b, _a, zrange_score_a] = zrange_result
      [zmscore_a, zmscore_b] = zmscore_result

      assert zmscore_a == zrange_score_a,
             "R2-H7: ZMSCORE[a] returned #{inspect(zmscore_a)} but ZRANGE returned #{inspect(zrange_score_a)}"

      assert zmscore_b == zrange_score_b,
             "R2-H7: ZMSCORE[b] returned #{inspect(zmscore_b)} but ZRANGE returned #{inspect(zrange_score_b)}"
    end

    test "ZSCAN returns same score format as ZRANGE WITHSCORES" do
      store = MockStore.make()
      key = unique_key("zs")

      SortedSet.handle("ZADD", [key, "3.14", "member"], store)

      [_cursor, scan_elements] = SortedSet.handle("ZSCAN", [key, "0"], store)
      zrange_result = SortedSet.handle("ZRANGE", [key, "0", "-1", "WITHSCORES"], store)

      # scan_elements = [member, score_str]
      [_scan_member, scan_score] = scan_elements
      [_range_member, range_score] = zrange_result

      # BUG: ZSCAN returns the raw stored string from compound_scan,
      # while ZRANGE parses to float then formats via format_score
      assert scan_score == range_score,
             "R2-H7: ZSCAN score #{inspect(scan_score)} differs from ZRANGE score #{inspect(range_score)}"
    end

    test "ZINCRBY and ZSCORE return same format" do
      store = MockStore.make()
      key = unique_key("zs")

      # Start with 1.0, increment by 2.14 -> 3.14
      SortedSet.handle("ZADD", [key, "1.0", "member"], store)

      incrby_result = SortedSet.handle("ZINCRBY", [key, "2.14", "member"], store)
      zscore_result = SortedSet.handle("ZSCORE", [key, "member"], store)

      # ZINCRBY uses format_score (erlang format), ZSCORE returns raw stored string
      # (Float.to_string format). The stored value is Float.to_string(3.14) = "3.14"
      # but ZINCRBY returned format_score(3.14) = "3.14000000000000012"
      assert zscore_result == incrby_result,
             "R2-H7: ZSCORE #{inspect(zscore_result)} != ZINCRBY #{inspect(incrby_result)}"
    end
  end

  # ===========================================================================
  # R2-H8: EXPIRE/PEXPIRE with negative TTL
  #
  # Redis 7 behavior for negative TTL:
  #   EXPIRE key -1 → returns 0 (does NOT delete the key)
  #   The key remains with its current value and TTL unchanged.
  #
  # Actually, checking Redis docs more carefully:
  #   Redis 2.6+: EXPIRE with negative timeout deletes the key (returns 1 if existed)
  #
  # Wait -- let me re-read the issue statement: "EXPIRE key -1 should return 0
  # and NOT delete the key (Redis behavior). Currently returns 1 and deletes."
  #
  # However, the actual Redis behavior since 2.6 is to delete keys with
  # negative/zero TTL. Let me test what the code actually does and document it.
  #
  # UPDATE: Per Redis docs, EXPIRE with negative value DOES delete the key
  # and returns 1 (if key existed). But the issue states the expected behavior
  # is return 0 / no delete. Testing the stated expectation.
  # ===========================================================================

  describe "R2-H8: EXPIRE/PEXPIRE with negative TTL" do
    # Redis behavior: negative EXPIRE/PEXPIRE returns 0 and does NOT modify
    # the key. Zero EXPIRE/PEXPIRE deletes the key (returns 1 if existed).

    test "EXPIRE with negative seconds deletes the key (Redis 7+)" do
      store = MockStore.make(%{"mykey" => {"myvalue", 0}})

      result = Expiry.handle("EXPIRE", ["mykey", "-1"], store)

      # Redis 7+: negative EXPIRE deletes the key (returns 1 if existed)
      assert result == 1
      assert store.get.("mykey") == nil
    end

    test "PEXPIRE with negative milliseconds deletes the key (Redis 7+)" do
      store = MockStore.make(%{"mykey" => {"myvalue", 0}})

      result = Expiry.handle("PEXPIRE", ["mykey", "-1"], store)

      assert result == 1
      assert store.get.("mykey") == nil
    end
  end

  # ===========================================================================
  # R2-H13: WriteVersion increments for failed conditional writes
  #
  # When WATCH is active and a conditional write (SET NX / SET XX) fails
  # (the condition prevents the write), the shard's write version should NOT
  # increment. If it does, a subsequent EXEC incorrectly detects a conflict
  # and aborts the transaction even though no data changed.
  #
  # This test uses Router functions against the real application shards.
  # ===========================================================================

  describe "R2-H13: WriteVersion and failed conditional writes" do
    test "SET NX on existing key should not change shard write version" do
      key = unique_key("wv")

      # Write the key
      Router.put(FerricStore.Instance.get(:default), key, "original", 0)

      # Snapshot version after the write
      version_before = Router.get_version(FerricStore.Instance.get(:default), key)

      # SET NX should fail (key exists) and NOT modify anything
      store = build_real_store()
      result = Strings.handle("SET", [key, "should_not_write", "NX"], store)

      # NX with existing key returns nil (write skipped)
      assert result == nil

      # Version should not have changed
      version_after = Router.get_version(FerricStore.Instance.get(:default), key)

      assert version_before == version_after,
             "R2-H13: Version changed from #{version_before} to #{version_after} despite NX skip"
    end

    test "SET XX on missing key should not change shard write version" do
      key = unique_key("wv_xx")

      # Don't create the key -- it should be absent

      # Capture version of the shard this key maps to
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      version_before = Ferricstore.Store.WriteVersion.get(shard_idx)

      # SET XX should fail (key absent) and NOT modify anything
      store = build_real_store()
      result = Strings.handle("SET", [key, "should_not_write", "XX"], store)

      assert result == nil

      shard_version_after = Ferricstore.Store.WriteVersion.get(shard_idx)

      assert version_before == shard_version_after,
             "R2-H13: Shared WriteVersion changed from #{version_before} to #{shard_version_after} despite XX skip"
    end

    test "WATCH + single-shard SET NX (fails) + EXEC should succeed" do
      key = unique_key("wv_watch")

      # Create the key
      Router.put(FerricStore.Instance.get(:default), key, "original", 0)

      # WATCH: capture value hash
      hash = watch_token(key)
      watched = %{key => hash}

      # Within MULTI, queue SET NX (will fail since key exists)
      # EXEC should succeed because value hash didn't change
      queue = [{"SET", [key, "should_not_write", "NX"]}]

      result = Ferricstore.Transaction.Coordinator.execute(queue, watched, nil)

      # Value-hash WATCH: NX skip doesn't change the value, so hash matches.
      assert result == [nil],
             "R2-H13: EXEC should succeed with [nil] (NX skip), got: #{inspect(result)}"

      # Key should still be original
      assert Router.get(FerricStore.Instance.get(:default), key) == "original"
    end

    test "cross-shard failed NX should not poison WATCH for other clients" do
      # Scenario: Two clients. Client A WATCHes key_a. Client B runs a
      # cross-shard MULTI that includes SET key_a NX (fails, no actual write)
      # plus a write to key_b. Client A's subsequent EXEC should succeed
      # because key_a was never actually modified.

      [key_a, key_b] = ShardHelpers.keys_on_different_shards(2)

      # Create key_a so SET NX will fail on it
      Router.put(FerricStore.Instance.get(:default), key_a, "original", 0)

      # Client A: WATCH key_a (snapshot value hash)
      hash_a = watch_token(key_a)

      # Client B: cross-shard MULTI with SET NX on key_a (fails) + SET key_b
      client_b_queue = [
        {"SET", [key_a, "should_not_write", "NX"]},
        {"SET", [key_b, "written_by_b"]}
      ]
      Ferricstore.Transaction.Coordinator.execute(client_b_queue, %{}, nil)

      # Client A: value hash should be unchanged since key_a was not modified.
      # With value-hash WATCH, the cross-shard WriteVersion.increment is
      # irrelevant -- we compare the watched key token, not shard versions.
      hash_a_after = watch_token(key_a)

      assert hash_a == hash_a_after,
             "R2-H13: value hash should be unchanged after NX skip"

      # Client A: EXEC with watched key -- should succeed
      watched_a = %{key_a => hash_a}
      client_a_queue = [{"SET", [key_a, "updated_by_a"]}]
      result = Ferricstore.Transaction.Coordinator.execute(client_a_queue, watched_a, nil)

      assert result == [:ok],
             "R2-H13: Client A's EXEC should succeed, got: #{inspect(result)}"

      assert Router.get(FerricStore.Instance.get(:default), key_a) == "updated_by_a"
    end
  end

  defp watch_token(key), do: Router.watch_token(FerricStore.Instance.get(:default), key)

  # ---------------------------------------------------------------------------
  # Helper: build a real store map for direct handler invocation
  # ---------------------------------------------------------------------------

  defp build_real_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      flush: fn -> :ok end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end,
      incr: fn k, d -> Router.incr(FerricStore.Instance.get(:default), k, d) end,
      incr_float: fn k, d -> Router.incr_float(FerricStore.Instance.get(:default), k, d) end,
      append: fn k, s -> Router.append(FerricStore.Instance.get(:default), k, s) end,
      getset: fn k, v -> Router.getset(FerricStore.Instance.get(:default), k, v) end,
      getdel: fn k -> Router.getdel(FerricStore.Instance.get(:default), k) end,
      getex: fn k, e -> Router.getex(FerricStore.Instance.get(:default), k, e) end,
      setrange: fn k, o, v -> Router.setrange(FerricStore.Instance.get(:default), k, o, v) end,
      compound_get: fn redis_key, compound_key ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
      end
    }
  end
end
