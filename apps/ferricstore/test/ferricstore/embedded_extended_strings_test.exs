defmodule Ferricstore.EmbeddedExtendedStringsTest do
  @moduledoc """
  String operation tests extracted from EmbeddedExtendedTest.

  Covers: decr, decr_by, incr_by_float, mget, mset, append, strlen, getset,
  getdel, getex, setnx, setex, psetex, getrange, setrange, msetnx,
  SET EXAT/PXAT/GET/KEEPTTL options.
  """
  use ExUnit.Case, async: false

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ===========================================================================
  # DECR / DECR_BY
  # ===========================================================================

  describe "decr/1" do
    test "decrements nonexistent key from 0 to -1" do
      assert {:ok, -1} = FerricStore.decr("decr:new")
    end

    test "decrements existing integer value" do
      FerricStore.set("decr:existing", "10")
      assert {:ok, 9} = FerricStore.decr("decr:existing")
    end

    test "multiple decrements accumulate" do
      assert {:ok, -1} = FerricStore.decr("decr:multi")
      assert {:ok, -2} = FerricStore.decr("decr:multi")
      assert {:ok, -3} = FerricStore.decr("decr:multi")
    end

    test "decr on non-integer value returns error" do
      FerricStore.set("decr:bad", "abc")
      assert {:error, _} = FerricStore.decr("decr:bad")
    end

    test "decr on hash key returns WRONGTYPE and leaves hash unchanged" do
      assert :ok = FerricStore.hset("decr:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.decr("decr:hash")
      assert {:ok, "hash"} = FerricStore.type("decr:hash")
      assert {:ok, "old"} = FerricStore.hget("decr:hash", "field")
    end
  end

  describe "decr_by/2" do
    test "decrements by specified amount" do
      FerricStore.set("decrby:key", "100")
      assert {:ok, 90} = FerricStore.decr_by("decrby:key", 10)
    end

    test "decrements nonexistent key from 0" do
      assert {:ok, -5} = FerricStore.decr_by("decrby:new", 5)
    end

    test "decrement by 0 returns current value" do
      FerricStore.set("decrby:zero", "42")
      assert {:ok, 42} = FerricStore.decr_by("decrby:zero", 0)
    end

    test "decr_by on non-integer value returns error" do
      FerricStore.set("decrby:bad", "hello")
      assert {:error, _} = FerricStore.decr_by("decrby:bad", 5)
    end
  end

  # ===========================================================================
  # INCR_BY_FLOAT
  # ===========================================================================

  describe "incr_by_float/2" do
    test "increments nonexistent key by float" do
      assert {:ok, result} = FerricStore.incr_by_float("ibf:new", 3.14)
      assert_in_delta result, 3.14, 0.001
    end

    test "increments existing integer-string by float" do
      FerricStore.set("ibf:int", "10")
      assert {:ok, result} = FerricStore.incr_by_float("ibf:int", 0.5)
      assert_in_delta result, 10.5, 0.001
    end

    test "increments by negative float" do
      FerricStore.set("ibf:neg", "10")
      assert {:ok, result} = FerricStore.incr_by_float("ibf:neg", -2.5)
      assert_in_delta result, 7.5, 0.001
    end

    test "incr_by_float on hash key returns WRONGTYPE and leaves hash unchanged" do
      assert :ok = FerricStore.hset("ibf:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.incr_by_float("ibf:hash", 1.5)
      assert {:ok, "hash"} = FerricStore.type("ibf:hash")
      assert {:ok, "old"} = FerricStore.hget("ibf:hash", "field")
    end
  end

  # ===========================================================================
  # MGET / MSET
  # ===========================================================================

  describe "mget/1" do
    test "returns values for multiple keys" do
      FerricStore.set("mg:a", "1")
      FerricStore.set("mg:b", "2")
      assert {:ok, ["1", "2"]} = FerricStore.mget(["mg:a", "mg:b"])
    end

    test "returns nil for missing keys" do
      FerricStore.set("mg:c", "3")
      assert {:ok, ["3", nil]} = FerricStore.mget(["mg:c", "mg:missing"])
    end

    test "returns all nils for all missing keys" do
      assert {:ok, [nil, nil]} = FerricStore.mget(["mg:x", "mg:y"])
    end

    test "empty list returns empty" do
      assert {:ok, []} = FerricStore.mget([])
    end
  end

  describe "mset/1" do
    test "sets multiple key-value pairs" do
      assert :ok = FerricStore.mset(%{"ms:a" => "1", "ms:b" => "2"})
      assert {:ok, "1"} = FerricStore.get("ms:a")
      assert {:ok, "2"} = FerricStore.get("ms:b")
    end

    test "overwrites existing keys" do
      FerricStore.set("ms:ow", "old")
      FerricStore.mset(%{"ms:ow" => "new"})
      assert {:ok, "new"} = FerricStore.get("ms:ow")
    end

    test "replaces hash key with string and clears stale hash metadata" do
      assert :ok = FerricStore.hset("ms:hash", %{"field" => "old"})
      assert :ok = FerricStore.mset(%{"ms:hash" => "string"})
      assert {:ok, "string"} = FerricStore.get("ms:hash")
      assert {:ok, "string"} = FerricStore.type("ms:hash")
      assert {:error, "WRONGTYPE" <> _} = FerricStore.hget("ms:hash", "field")
    end
  end

  describe "set replacement across data structure keys" do
    test "set replaces hash key with string and clears stale hash metadata" do
      assert :ok = FerricStore.hset("set:hash", %{"field" => "old"})
      assert :ok = FerricStore.set("set:hash", "string")
      assert {:ok, "string"} = FerricStore.get("set:hash")
      assert {:ok, "string"} = FerricStore.type("set:hash")
      assert {:error, "WRONGTYPE" <> _} = FerricStore.hget("set:hash", "field")
    end

    test "setex and psetex replace hash key with string and clear stale hash metadata" do
      assert :ok = FerricStore.hset("setex:hash", %{"field" => "old"})
      assert :ok = FerricStore.setex("setex:hash", 10, "string")
      assert {:ok, "string"} = FerricStore.get("setex:hash")
      assert {:ok, "string"} = FerricStore.type("setex:hash")
      assert {:error, "WRONGTYPE" <> _} = FerricStore.hget("setex:hash", "field")

      assert :ok = FerricStore.hset("psetex:hash", %{"field" => "old"})
      assert :ok = FerricStore.psetex("psetex:hash", 10_000, "string")
      assert {:ok, "string"} = FerricStore.get("psetex:hash")
      assert {:ok, "string"} = FerricStore.type("psetex:hash")
      assert {:error, "WRONGTYPE" <> _} = FerricStore.hget("psetex:hash", "field")
    end

    test "set nx treats hash key as existing and leaves it unchanged" do
      assert :ok = FerricStore.hset("setnx:hash", %{"field" => "old"})
      assert nil == FerricStore.set("setnx:hash", "string", nx: true)
      assert {:ok, "hash"} = FerricStore.type("setnx:hash")
      assert {:ok, "old"} = FerricStore.hget("setnx:hash", "field")
    end

    test "set xx treats hash key as existing and replaces it with string" do
      assert :ok = FerricStore.hset("setxx:hash", %{"field" => "old"})
      assert :ok = FerricStore.set("setxx:hash", "string", xx: true)
      assert {:ok, "string"} = FerricStore.get("setxx:hash")
      assert {:ok, "string"} = FerricStore.type("setxx:hash")
      assert {:error, "WRONGTYPE" <> _} = FerricStore.hget("setxx:hash", "field")
    end

    test "set get on hash key returns WRONGTYPE and leaves hash unchanged" do
      assert :ok = FerricStore.hset("setget:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.set("setget:hash", "string", get: true)
      assert {:ok, "hash"} = FerricStore.type("setget:hash")
      assert {:ok, "old"} = FerricStore.hget("setget:hash", "field")
    end
  end

  # ===========================================================================
  # APPEND / STRLEN
  # ===========================================================================

  describe "append/2" do
    test "appends to existing key" do
      FerricStore.set("ap:key", "Hello")
      assert {:ok, 11} = FerricStore.append("ap:key", " World")
      assert {:ok, "Hello World"} = FerricStore.get("ap:key")
    end

    test "creates key if it does not exist" do
      assert {:ok, 5} = FerricStore.append("ap:new", "Hello")
      assert {:ok, "Hello"} = FerricStore.get("ap:new")
    end

    test "appends empty string" do
      FerricStore.set("ap:empty", "data")
      assert {:ok, 4} = FerricStore.append("ap:empty", "")
    end

    test "append on hash key returns WRONGTYPE and leaves hash unchanged" do
      assert :ok = FerricStore.hset("ap:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.append("ap:hash", "suffix")
      assert {:ok, "hash"} = FerricStore.type("ap:hash")
      assert {:ok, "old"} = FerricStore.hget("ap:hash", "field")
    end
  end

  describe "strlen/1" do
    test "returns length of existing string" do
      FerricStore.set("sl:key", "Hello")
      assert {:ok, 5} = FerricStore.strlen("sl:key")
    end

    test "returns 0 for nonexistent key" do
      assert {:ok, 0} = FerricStore.strlen("sl:missing")
    end

    test "returns correct length for binary data" do
      FerricStore.set("sl:bin", <<1, 2, 3, 4, 5>>)
      assert {:ok, 5} = FerricStore.strlen("sl:bin")
    end

    test "strlen on hash key returns WRONGTYPE" do
      assert :ok = FerricStore.hset("sl:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.strlen("sl:hash")
      assert {:ok, "hash"} = FerricStore.type("sl:hash")
    end
  end

  # ===========================================================================
  # GETSET / GETDEL / GETEX
  # ===========================================================================

  describe "getset/2" do
    test "returns nil and sets value for nonexistent key" do
      assert {:ok, nil} = FerricStore.getset("gs:new", "value")
      assert {:ok, "value"} = FerricStore.get("gs:new")
    end

    test "returns old value and sets new value" do
      FerricStore.set("gs:key", "old")
      assert {:ok, "old"} = FerricStore.getset("gs:key", "new")
      assert {:ok, "new"} = FerricStore.get("gs:key")
    end

    test "getset on hash key returns WRONGTYPE and leaves hash unchanged" do
      assert :ok = FerricStore.hset("gs:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.getset("gs:hash", "new")
      assert {:ok, "hash"} = FerricStore.type("gs:hash")
      assert {:ok, "old"} = FerricStore.hget("gs:hash", "field")
    end
  end

  describe "getdel/1" do
    test "gets and deletes existing key" do
      FerricStore.set("gd:key", "value")
      assert {:ok, "value"} = FerricStore.getdel("gd:key")
      assert {:ok, nil} = FerricStore.get("gd:key")
    end

    test "returns nil for nonexistent key" do
      assert {:ok, nil} = FerricStore.getdel("gd:missing")
    end

    test "getdel on hash key returns WRONGTYPE and leaves hash unchanged" do
      assert :ok = FerricStore.hset("gd:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.getdel("gd:hash")
      assert {:ok, "hash"} = FerricStore.type("gd:hash")
      assert {:ok, "old"} = FerricStore.hget("gd:hash", "field")
    end
  end

  describe "getex/2" do
    test "gets value and sets TTL" do
      FerricStore.set("gex:key", "value")
      assert {:ok, "value"} = FerricStore.getex("gex:key", ttl: 60_000)
      assert {:ok, ms} = FerricStore.ttl("gex:key")
      assert is_integer(ms) and ms > 0
    end

    test "gets value and removes TTL with persist" do
      FerricStore.set("gex:persist", "value", ttl: 60_000)
      assert {:ok, "value"} = FerricStore.getex("gex:persist", persist: true)
      assert {:ok, nil} = FerricStore.ttl("gex:persist")
    end

    test "returns nil for nonexistent key" do
      assert {:ok, nil} = FerricStore.getex("gex:missing", ttl: 60_000)
    end

    test "getex on hash key returns WRONGTYPE and leaves hash unchanged" do
      assert :ok = FerricStore.hset("gex:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.getex("gex:hash", ttl: 60_000)
      assert {:ok, "hash"} = FerricStore.type("gex:hash")
      assert {:ok, "old"} = FerricStore.hget("gex:hash", "field")
    end

    test "getex without TTL on hash key returns WRONGTYPE" do
      assert :ok = FerricStore.hset("gex:no_opts_hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.getex("gex:no_opts_hash")
      assert {:ok, "hash"} = FerricStore.type("gex:no_opts_hash")
    end
  end

  # ===========================================================================
  # SETNX / SETEX / PSETEX
  # ===========================================================================

  describe "setnx/2" do
    test "sets value when key does not exist" do
      assert {:ok, true} = FerricStore.setnx("snx:new", "value")
      assert {:ok, "value"} = FerricStore.get("snx:new")
    end

    test "does not set value when key exists" do
      FerricStore.set("snx:exists", "original")
      assert {:ok, false} = FerricStore.setnx("snx:exists", "other")
      assert {:ok, "original"} = FerricStore.get("snx:exists")
    end

    test "treats hash key as existing and leaves it unchanged" do
      assert :ok = FerricStore.hset("snx:hash", %{"field" => "old"})
      assert {:ok, false} = FerricStore.setnx("snx:hash", "string")
      assert {:ok, "hash"} = FerricStore.type("snx:hash")
      assert {:ok, "old"} = FerricStore.hget("snx:hash", "field")
    end
  end

  describe "setex/3" do
    test "sets value with seconds-based TTL" do
      assert :ok = FerricStore.setex("sex:key", 60, "value")
      assert {:ok, "value"} = FerricStore.get("sex:key")
      assert {:ok, ms} = FerricStore.ttl("sex:key")
      assert is_integer(ms) and ms > 0 and ms <= 60_000
    end
  end

  describe "psetex/3" do
    test "sets value with millisecond TTL" do
      assert :ok = FerricStore.psetex("psx:key", 60_000, "value")
      assert {:ok, "value"} = FerricStore.get("psx:key")
      assert {:ok, ms} = FerricStore.ttl("psx:key")
      assert is_integer(ms) and ms > 0 and ms <= 60_000
    end
  end

  # ===========================================================================
  # GETRANGE / SETRANGE
  # ===========================================================================

  describe "getrange/3" do
    test "returns substring of existing value" do
      FerricStore.set("gr:key", "Hello World")
      assert {:ok, "World"} = FerricStore.getrange("gr:key", 6, 10)
    end

    test "returns full string with 0..-1-style indices" do
      FerricStore.set("gr:full", "Hello")
      assert {:ok, "Hello"} = FerricStore.getrange("gr:full", 0, -1)
    end

    test "returns empty string for nonexistent key" do
      assert {:ok, ""} = FerricStore.getrange("gr:missing", 0, 10)
    end

    test "returns WRONGTYPE for hash key" do
      assert :ok = FerricStore.hset("gr:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.getrange("gr:hash", 0, 10)
      assert {:ok, "hash"} = FerricStore.type("gr:hash")
    end
  end

  describe "setrange/3" do
    test "overwrites part of a string" do
      FerricStore.set("sr:key", "Hello World")
      assert {:ok, 11} = FerricStore.setrange("sr:key", 6, "Redis")
      assert {:ok, "Hello Redis"} = FerricStore.get("sr:key")
    end

    test "pads with null bytes if offset is beyond string length" do
      assert {:ok, len} = FerricStore.setrange("sr:pad", 5, "Hi")
      assert len == 7
      {:ok, val} = FerricStore.get("sr:pad")
      assert byte_size(val) == 7
    end

    test "returns WRONGTYPE for hash key and leaves it unchanged" do
      assert :ok = FerricStore.hset("sr:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.setrange("sr:hash", 0, "new")
      assert {:ok, "hash"} = FerricStore.type("sr:hash")
      assert {:ok, "old"} = FerricStore.hget("sr:hash", "field")
    end
  end

  # ===========================================================================
  # MSETNX
  # ===========================================================================

  describe "msetnx/1" do
    test "sets all keys when none exist" do
      assert {:ok, true} = FerricStore.msetnx(%{"mnx:a" => "1", "mnx:b" => "2"})
      assert {:ok, "1"} = FerricStore.get("mnx:a")
      assert {:ok, "2"} = FerricStore.get("mnx:b")
    end

    test "sets nothing when any key exists" do
      FerricStore.set("mnx:c", "existing")
      assert {:ok, false} = FerricStore.msetnx(%{"mnx:c" => "new", "mnx:d" => "new"})
      assert {:ok, "existing"} = FerricStore.get("mnx:c")
      assert {:ok, nil} = FerricStore.get("mnx:d")
    end
  end

  # ===========================================================================
  # SET — EXAT option (absolute Unix timestamp in seconds)
  # ===========================================================================

  describe "set/3 with :exat" do
    test "sets key with absolute expiry in seconds" do
      # Expire 60 seconds from now
      future_ts = div(System.os_time(:millisecond), 1000) + 60
      assert :ok = FerricStore.set("exat:basic", "val", exat: future_ts)
      assert {:ok, "val"} = FerricStore.get("exat:basic")

      # TTL should be roughly 60 seconds (within a tolerance)
      {:ok, ttl_ms} = FerricStore.ttl("exat:basic")
      assert ttl_ms != nil
      assert ttl_ms > 55_000
      assert ttl_ms <= 60_000
    end

    test "key set with past EXAT is immediately expired" do
      past_ts = div(System.os_time(:millisecond), 1000) - 10
      # EXAT with past timestamp: Redis still accepts the write but the key
      # expires immediately. FerricStore rejects ts <= 0 from the parser, but
      # a past-but-positive timestamp is accepted and expires on next read.
      assert :ok = FerricStore.set("exat:past", "gone", exat: past_ts)
      assert {:ok, nil} = FerricStore.get("exat:past")
    end
  end

  # ===========================================================================
  # SET — PXAT option (absolute Unix timestamp in milliseconds)
  # ===========================================================================

  describe "set/3 with :pxat" do
    test "sets key with absolute expiry in milliseconds" do
      future_ms = System.os_time(:millisecond) + 60_000
      assert :ok = FerricStore.set("pxat:basic", "val", pxat: future_ms)
      assert {:ok, "val"} = FerricStore.get("pxat:basic")

      {:ok, ttl_ms} = FerricStore.ttl("pxat:basic")
      assert ttl_ms != nil
      assert ttl_ms > 55_000
      assert ttl_ms <= 60_000
    end

    test "key set with past PXAT is immediately expired" do
      past_ms = System.os_time(:millisecond) - 5_000
      assert :ok = FerricStore.set("pxat:past", "gone", pxat: past_ms)
      assert {:ok, nil} = FerricStore.get("pxat:past")
    end
  end

  # ===========================================================================
  # SET — GET option (return old value)
  # ===========================================================================

  describe "set/3 with :get" do
    test "returns nil when key does not exist" do
      assert {:ok, nil} = FerricStore.set("get:new", "first", get: true)
      assert {:ok, "first"} = FerricStore.get("get:new")
    end

    test "returns old value when key exists" do
      FerricStore.set("get:existing", "old_val")
      assert {:ok, "old_val"} = FerricStore.set("get:existing", "new_val", get: true)
      assert {:ok, "new_val"} = FerricStore.get("get:existing")
    end

    test "GET combined with NX returns old value even when write is skipped" do
      FerricStore.set("get:nx:exists", "original")
      # NX prevents the write since key exists; GET still returns old value
      assert {:ok, "original"} = FerricStore.set("get:nx:exists", "attempt", get: true, nx: true)
      # Value unchanged
      assert {:ok, "original"} = FerricStore.get("get:nx:exists")
    end

    test "GET combined with XX returns nil when key does not exist" do
      # XX prevents write since key doesn't exist; GET returns nil
      assert {:ok, nil} = FerricStore.set("get:xx:missing", "attempt", get: true, xx: true)
      assert {:ok, nil} = FerricStore.get("get:xx:missing")
    end

    test "GET combined with EX returns old value and sets TTL" do
      FerricStore.set("get:ex", "old")
      assert {:ok, "old"} = FerricStore.set("get:ex", "new", get: true, ttl: 30_000)
      assert {:ok, "new"} = FerricStore.get("get:ex")

      {:ok, ttl_ms} = FerricStore.ttl("get:ex")
      assert ttl_ms != nil
      assert ttl_ms > 25_000
    end
  end

  # ===========================================================================
  # SET — KEEPTTL option (preserve existing TTL)
  # ===========================================================================

  describe "set/3 with :keepttl" do
    test "preserves existing TTL when overwriting value" do
      # Set with 60s TTL
      FerricStore.set("keepttl:basic", "v1", ttl: 60_000)

      {:ok, ttl_before} = FerricStore.ttl("keepttl:basic")
      assert ttl_before != nil
      assert ttl_before > 55_000

      # Overwrite with KEEPTTL — TTL should be preserved
      assert :ok = FerricStore.set("keepttl:basic", "v2", keepttl: true)
      assert {:ok, "v2"} = FerricStore.get("keepttl:basic")

      {:ok, ttl_after} = FerricStore.ttl("keepttl:basic")
      assert ttl_after != nil
      # TTL should be close to what it was before (allow 2s tolerance)
      assert ttl_after > 53_000
    end

    test "key without TTL remains without TTL after KEEPTTL" do
      FerricStore.set("keepttl:nottl", "v1")

      {:ok, nil} = FerricStore.ttl("keepttl:nottl")

      assert :ok = FerricStore.set("keepttl:nottl", "v2", keepttl: true)
      assert {:ok, "v2"} = FerricStore.get("keepttl:nottl")

      {:ok, nil} = FerricStore.ttl("keepttl:nottl")
    end

    test "KEEPTTL on nonexistent key behaves like normal set (no TTL)" do
      assert :ok = FerricStore.set("keepttl:noexist", "val", keepttl: true)
      assert {:ok, "val"} = FerricStore.get("keepttl:noexist")

      {:ok, nil} = FerricStore.ttl("keepttl:noexist")
    end

    test "KEEPTTL combined with XX" do
      FerricStore.set("keepttl:xx", "v1", ttl: 60_000)

      assert :ok = FerricStore.set("keepttl:xx", "v2", keepttl: true, xx: true)
      assert {:ok, "v2"} = FerricStore.get("keepttl:xx")

      {:ok, ttl_ms} = FerricStore.ttl("keepttl:xx")
      assert ttl_ms != nil
      assert ttl_ms > 53_000
    end

    test "KEEPTTL combined with GET returns old value and preserves TTL" do
      FerricStore.set("keepttl:get", "old", ttl: 60_000)

      assert {:ok, "old"} = FerricStore.set("keepttl:get", "new", keepttl: true, get: true)
      assert {:ok, "new"} = FerricStore.get("keepttl:get")

      {:ok, ttl_ms} = FerricStore.ttl("keepttl:get")
      assert ttl_ms != nil
      assert ttl_ms > 53_000
    end
  end
end
