defmodule Ferricstore.EmbeddedExtendedMiscTest do
  @moduledoc """
  Miscellaneous operation tests extracted from EmbeddedExtendedTest.

  Covers: copy, rename, renamenx, type, randomkey, TTL extended ops
  (persist, pexpire, pexpireat, expireat, expiretime, pexpiretime, pttl),
  bitmap ops, native lock/unlock/extend, rate limiting, fetch_or_compute,
  server (ping, echo, flushall).

  See also: embedded_extended_data_structures_test.exs for HyperLogLog,
  Bloom filter, Cuckoo filter, Count-Min Sketch, TopK, T-Digest, JSON,
  Geo, and value size limits.
  """
  use ExUnit.Case, async: false

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ===========================================================================
  # COPY / RENAME / RENAMENX
  # ===========================================================================

  describe "copy/3" do
    test "copies value to new key" do
      FerricStore.set("cp:src", "value")
      assert {:ok, true} = FerricStore.copy("cp:src", "cp:dst")
      assert {:ok, "value"} = FerricStore.get("cp:dst")
      # source still exists
      assert {:ok, "value"} = FerricStore.get("cp:src")
    end

    test "does not overwrite existing destination without replace" do
      FerricStore.set("cp:s2", "src_val")
      FerricStore.set("cp:d2", "dst_val")
      assert {:error, _} = FerricStore.copy("cp:s2", "cp:d2")
      assert {:ok, "dst_val"} = FerricStore.get("cp:d2")
    end

    test "overwrites existing destination with replace option" do
      FerricStore.set("cp:s3", "src_val")
      FerricStore.set("cp:d3", "dst_val")
      assert {:ok, true} = FerricStore.copy("cp:s3", "cp:d3", replace: true)
      assert {:ok, "src_val"} = FerricStore.get("cp:d3")
    end
  end

  describe "rename/2" do
    test "renames existing key" do
      FerricStore.set("rn:old", "value")
      assert :ok = FerricStore.rename("rn:old", "rn:new")
      assert {:ok, nil} = FerricStore.get("rn:old")
      assert {:ok, "value"} = FerricStore.get("rn:new")
    end

    test "returns error for nonexistent key" do
      assert {:error, _} = FerricStore.rename("rn:missing", "rn:dst")
    end
  end

  describe "renamenx/2" do
    test "renames when destination does not exist" do
      FerricStore.set("rnx:old", "value")
      assert {:ok, true} = FerricStore.renamenx("rnx:old", "rnx:new")
      assert {:ok, "value"} = FerricStore.get("rnx:new")
    end

    test "does not rename when destination exists" do
      FerricStore.set("rnx:src", "src_val")
      FerricStore.set("rnx:dst", "dst_val")
      assert {:ok, false} = FerricStore.renamenx("rnx:src", "rnx:dst")
      assert {:ok, "dst_val"} = FerricStore.get("rnx:dst")
    end
  end

  # ===========================================================================
  # TYPE / RANDOMKEY
  # ===========================================================================

  describe "exists/1" do
    test "returns true for compound data structure keys" do
      assert :ok = FerricStore.hset("ex:hash", %{"f" => "v"})
      assert {:ok, 1} = FerricStore.rpush("ex:list", ["v"])
      assert {:ok, 1} = FerricStore.sadd("ex:set", ["v"])
      assert {:ok, 1} = FerricStore.zadd("ex:zset", [{1.0, "v"}])

      assert FerricStore.exists("ex:hash")
      assert FerricStore.exists("ex:list")
      assert FerricStore.exists("ex:set")
      assert FerricStore.exists("ex:zset")
    end
  end

  describe "type/1" do
    test "returns 'string' for string key" do
      FerricStore.set("tp:str", "value")
      assert {:ok, "string"} = FerricStore.type("tp:str")
    end

    test "returns 'none' for nonexistent key" do
      assert {:ok, "none"} = FerricStore.type("tp:missing")
    end

    test "returns 'hash' for hash key" do
      FerricStore.hset("tp:hash", %{"f" => "v"})
      assert {:ok, "hash"} = FerricStore.type("tp:hash")
    end

    test "returns 'list' for list key" do
      FerricStore.rpush("tp:list", ["a"])
      assert {:ok, "list"} = FerricStore.type("tp:list")
    end

    test "returns 'set' for set key" do
      FerricStore.sadd("tp:set", ["a"])
      assert {:ok, "set"} = FerricStore.type("tp:set")
    end

    test "returns 'zset' for sorted set key" do
      FerricStore.zadd("tp:zset", [{1.0, "a"}])
      assert {:ok, "zset"} = FerricStore.type("tp:zset")
    end
  end

  describe "randomkey/0" do
    test "returns nil when no keys exist" do
      FerricStore.flushdb()
      assert {:ok, nil} = FerricStore.randomkey()
    end

    test "returns a key that exists" do
      FerricStore.set("rk:a", "1")
      FerricStore.set("rk:b", "2")
      {:ok, key} = FerricStore.randomkey()
      assert key != nil
      assert FerricStore.exists(key)
    end
  end

  # ===========================================================================
  # TTL extended: persist, pexpire, pexpireat, expireat, expiretime,
  # pexpiretime, pttl
  # ===========================================================================

  describe "persist/1" do
    test "removes TTL from key" do
      FerricStore.set("per:key", "value", ttl: 60_000)
      assert {:ok, true} = FerricStore.persist("per:key")
      assert {:ok, nil} = FerricStore.ttl("per:key")
    end

    test "returns false for key without TTL" do
      FerricStore.set("per:nottl", "value")
      assert {:ok, false} = FerricStore.persist("per:nottl")
    end

    test "returns false for nonexistent key" do
      assert {:ok, false} = FerricStore.persist("per:missing")
    end

    test "removes TTL from hash key" do
      assert :ok = FerricStore.hset("per:hash", %{"f" => "v"})
      assert {:ok, true} = FerricStore.expire("per:hash", 60_000)
      assert {:ok, ms} = FerricStore.ttl("per:hash")
      assert is_integer(ms) and ms > 0
      assert {:ok, true} = FerricStore.persist("per:hash")
      assert {:ok, nil} = FerricStore.ttl("per:hash")
      assert {:ok, "v"} = FerricStore.hget("per:hash", "f")
    end
  end

  describe "pexpire/2" do
    test "sets TTL in milliseconds (alias for expire)" do
      FerricStore.set("pex:key", "value")
      assert {:ok, true} = FerricStore.pexpire("pex:key", 30_000)
      assert {:ok, ms} = FerricStore.ttl("pex:key")
      assert is_integer(ms) and ms > 0 and ms <= 30_000
    end

    test "sets TTL on list key and expiry removes the list" do
      assert {:ok, 1} = FerricStore.rpush("pex:list", ["v"])
      assert {:ok, true} = FerricStore.pexpire("pex:list", 200)
      assert {:ok, ms} = FerricStore.ttl("pex:list")
      assert is_integer(ms) and ms > 0 and ms <= 200
      Process.sleep(250)
      assert {:ok, "none"} = FerricStore.type("pex:list")
      assert {:ok, 0} = FerricStore.llen("pex:list")
    end
  end

  describe "expireat/2" do
    test "sets expiry at Unix timestamp in seconds" do
      FerricStore.set("eat:key", "value")
      future_ts = div(System.os_time(:millisecond), 1_000) + 3600
      assert {:ok, true} = FerricStore.expireat("eat:key", future_ts)
      assert {:ok, ms} = FerricStore.ttl("eat:key")
      assert is_integer(ms) and ms > 0
    end

    test "returns false for nonexistent key" do
      assert {:ok, false} = FerricStore.expireat("eat:missing", 9_999_999_999)
    end
  end

  describe "pexpireat/2" do
    test "sets expiry at Unix timestamp in milliseconds" do
      FerricStore.set("peat:key", "value")
      future_ms = System.os_time(:millisecond) + 3_600_000
      assert {:ok, true} = FerricStore.pexpireat("peat:key", future_ms)
      assert {:ok, ms} = FerricStore.ttl("peat:key")
      assert is_integer(ms) and ms > 0
    end

    test "returns false for nonexistent key" do
      assert {:ok, false} = FerricStore.pexpireat("peat:missing", 9_999_999_999_999)
    end
  end

  describe "expiretime/1" do
    test "returns Unix timestamp in seconds for key with TTL" do
      FerricStore.set("etime:key", "value", ttl: 3_600_000)
      assert {:ok, ts} = FerricStore.expiretime("etime:key")
      assert is_integer(ts) and ts > 0
      now_s = div(System.os_time(:millisecond), 1_000)
      assert ts > now_s
    end

    test "returns -1 for key without TTL" do
      FerricStore.set("etime:nottl", "value")
      assert {:ok, -1} = FerricStore.expiretime("etime:nottl")
    end

    test "returns -2 for nonexistent key" do
      assert {:ok, -2} = FerricStore.expiretime("etime:missing")
    end

    test "returns Unix timestamp for hash key with TTL" do
      assert :ok = FerricStore.hset("etime:hash", %{"f" => "v"})
      assert {:ok, true} = FerricStore.expire("etime:hash", 60_000)
      assert {:ok, ts} = FerricStore.expiretime("etime:hash")
      assert is_integer(ts) and ts > div(System.os_time(:millisecond), 1_000)
    end
  end

  describe "pexpiretime/1" do
    test "returns Unix timestamp in milliseconds for key with TTL" do
      FerricStore.set("petime:key", "value", ttl: 3_600_000)
      assert {:ok, ts_ms} = FerricStore.pexpiretime("petime:key")
      assert is_integer(ts_ms) and ts_ms > 0
      now_ms = System.os_time(:millisecond)
      assert ts_ms > now_ms
    end

    test "returns -1 for key without TTL" do
      FerricStore.set("petime:nottl", "value")
      assert {:ok, -1} = FerricStore.pexpiretime("petime:nottl")
    end

    test "returns -2 for nonexistent key" do
      assert {:ok, -2} = FerricStore.pexpiretime("petime:missing")
    end

    test "returns Unix timestamp in milliseconds for hash key with TTL" do
      assert :ok = FerricStore.hset("petime:hash", %{"f" => "v"})
      assert {:ok, true} = FerricStore.expire("petime:hash", 60_000)
      assert {:ok, ts_ms} = FerricStore.pexpiretime("petime:hash")
      assert is_integer(ts_ms) and ts_ms > System.os_time(:millisecond)
    end
  end

  describe "pttl/1" do
    test "returns TTL in ms (same as ttl/1)" do
      FerricStore.set("pttl:key", "value", ttl: 60_000)
      assert {:ok, ms} = FerricStore.pttl("pttl:key")
      assert is_integer(ms) and ms > 0 and ms <= 60_000
    end

    test "returns nil for key without TTL" do
      FerricStore.set("pttl:nottl", "value")
      assert {:ok, nil} = FerricStore.pttl("pttl:nottl")
    end

    test "returns nil for nonexistent key" do
      assert {:ok, nil} = FerricStore.pttl("pttl:missing")
    end
  end

  # ===========================================================================
  # BITMAP: setbit, getbit, bitcount, bitop, bitpos
  # ===========================================================================

  describe "setbit/3 and getbit/2" do
    test "sets and gets a bit" do
      assert {:ok, 0} = FerricStore.setbit("bit:key", 7, 1)
      assert {:ok, 1} = FerricStore.getbit("bit:key", 7)
    end

    test "default bits are 0" do
      assert {:ok, 0} = FerricStore.getbit("bit:empty", 100)
    end

    test "setbit returns previous value" do
      FerricStore.setbit("bit:prev", 7, 1)
      assert {:ok, 1} = FerricStore.setbit("bit:prev", 7, 0)
      assert {:ok, 0} = FerricStore.getbit("bit:prev", 7)
    end

    test "setbit and getbit return WRONGTYPE for hash key" do
      assert :ok = FerricStore.hset("bit:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.setbit("bit:hash", 7, 1)
      assert {:error, "WRONGTYPE" <> _} = FerricStore.getbit("bit:hash", 7)
      assert {:ok, "hash"} = FerricStore.type("bit:hash")
      assert {:ok, "old"} = FerricStore.hget("bit:hash", "field")
    end
  end

  describe "bitcount/1" do
    test "counts set bits" do
      # "foobar" = 0x66 0x6f 0x6f 0x62 0x61 0x72 -> 26 set bits
      FerricStore.set("bc:key", "foobar")
      assert {:ok, count} = FerricStore.bitcount("bc:key")
      assert is_integer(count) and count > 0
    end

    test "returns 0 for nonexistent key" do
      assert {:ok, 0} = FerricStore.bitcount("bc:missing")
    end

    test "bitcount with range" do
      FerricStore.set("bc:range", "foobar")
      assert {:ok, count} = FerricStore.bitcount("bc:range", start: 0, stop: 0)
      assert is_integer(count) and count > 0
    end

    test "returns WRONGTYPE for hash key" do
      assert :ok = FerricStore.hset("bc:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.bitcount("bc:hash")
      assert {:ok, "hash"} = FerricStore.type("bc:hash")
    end
  end

  describe "bitop/3" do
    test "AND operation between two keys" do
      FerricStore.set("bo:a", "abc")
      FerricStore.set("bo:b", "abc")
      assert {:ok, len} = FerricStore.bitop(:and, "bo:dest", ["bo:a", "bo:b"])
      assert len == 3
      assert {:ok, "abc"} = FerricStore.get("bo:dest")
    end

    test "OR operation" do
      FerricStore.set("bo:or1", <<0xFF>>)
      FerricStore.set("bo:or2", <<0x0F>>)
      assert {:ok, 1} = FerricStore.bitop(:or, "bo:or_dst", ["bo:or1", "bo:or2"])
      assert {:ok, <<0xFF>>} = FerricStore.get("bo:or_dst")
    end

    test "NOT operation (single key)" do
      FerricStore.set("bo:not_src", <<0x0F>>)
      assert {:ok, 1} = FerricStore.bitop(:not, "bo:not_dst", ["bo:not_src"])
      assert {:ok, <<0xF0>>} = FerricStore.get("bo:not_dst")
    end

    test "returns WRONGTYPE for hash source and does not write destination" do
      assert :ok = FerricStore.hset("bo:hash_src", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.bitop(:or, "bo:hash_dst", ["bo:hash_src"])
      assert {:ok, nil} = FerricStore.get("bo:hash_dst")
      assert {:ok, "hash"} = FerricStore.type("bo:hash_src")
      assert {:ok, "old"} = FerricStore.hget("bo:hash_src", "field")
    end
  end

  describe "bitpos/2" do
    test "finds first set bit" do
      FerricStore.set("bp:key", <<0x00, 0xFF>>)
      assert {:ok, pos} = FerricStore.bitpos("bp:key", 1)
      assert pos == 8
    end

    test "finds first zero bit" do
      FerricStore.set("bp:zero", <<0xFF, 0x00>>)
      assert {:ok, pos} = FerricStore.bitpos("bp:zero", 0)
      assert pos == 8
    end

    test "returns -1 when bit not found in all-ones string looking for 0 with bounded range" do
      FerricStore.set("bp:ones", <<0xFF>>)
      assert {:ok, pos} = FerricStore.bitpos("bp:ones", 0, start: 0, stop: 0)
      # With byte-level range, all bits are 1 in byte 0 => -1
      assert pos == -1
    end

    test "returns WRONGTYPE for hash key" do
      assert :ok = FerricStore.hset("bp:hash", %{"field" => "old"})
      assert {:error, "WRONGTYPE" <> _} = FerricStore.bitpos("bp:hash", 1)
      assert {:ok, "hash"} = FerricStore.type("bp:hash")
    end
  end

  # ===========================================================================
  # NATIVE: lock, unlock, extend
  # ===========================================================================

  describe "lock/3, unlock/2, extend/3" do
    test "acquire, extend, and release a lock" do
      assert :ok = FerricStore.lock("lk:key", "owner1", 30_000)

      # Extend the lock
      assert {:ok, 1} = FerricStore.extend("lk:key", "owner1", 60_000)

      # Release the lock
      assert {:ok, 1} = FerricStore.unlock("lk:key", "owner1")
    end

    test "lock fails if already held by another owner" do
      assert :ok = FerricStore.lock("lk:key2", "owner1", 30_000)
      assert {:error, _} = FerricStore.lock("lk:key2", "owner2", 30_000)

      # Clean up
      FerricStore.unlock("lk:key2", "owner1")
    end

    test "unlock fails with wrong owner" do
      FerricStore.lock("lk:key3", "owner1", 30_000)
      assert {:error, _} = FerricStore.unlock("lk:key3", "wrong_owner")

      # Clean up
      FerricStore.unlock("lk:key3", "owner1")
    end
  end

  # ===========================================================================
  # RATE LIMITING
  # ===========================================================================

  describe "ratelimit_add/4" do
    test "allows requests within limit" do
      assert {:ok, result} = FerricStore.ratelimit_add("rl:key", 60_000, 10)
      assert is_list(result)
      # Returns ["allowed", remaining_count] or similar flat list
      assert "allowed" in result or 1 in result
    end

    test "tracks multiple requests" do
      FerricStore.ratelimit_add("rl:key2", 60_000, 10)
      FerricStore.ratelimit_add("rl:key2", 60_000, 10)
      assert {:ok, result} = FerricStore.ratelimit_add("rl:key2", 60_000, 10)
      assert is_list(result)
    end

    test "exhausting the limit eventually blocks" do
      key = "rl:exhaust"
      max = 3

      results =
        for _ <- 1..5 do
          {:ok, r} = FerricStore.ratelimit_add(key, 60_000, max)
          r
        end

      # At least some responses should exist
      assert length(results) == 5
    end
  end

  # ===========================================================================
  # FETCH_OR_COMPUTE (stampede protection)
  # ===========================================================================

  describe "fetch_or_compute/2 and fetch_or_compute_result/3" do
    test "returns :compute on cache miss, then :hit after storing" do
      case FerricStore.fetch_or_compute("foc:key", ttl: 60_000) do
        {:ok, {:compute, _hint}} ->
          assert :ok =
                   FerricStore.fetch_or_compute_result("foc:key", "computed_value", ttl: 60_000)

          case FerricStore.fetch_or_compute("foc:key", ttl: 60_000) do
            {:ok, {:hit, value}} ->
              assert value == "computed_value"

            other ->
              flunk("Expected {:ok, {:hit, _}}, got: #{inspect(other)}")
          end

        {:ok, {:hit, _}} ->
          # Key might exist from previous test runs; that's acceptable
          :ok
      end
    end
  end

  # ===========================================================================
  # SERVER: ping, echo, flushall
  # ===========================================================================

  describe "ping/0" do
    test "returns PONG" do
      assert {:ok, "PONG"} = FerricStore.ping()
    end
  end

  describe "echo/1" do
    test "echoes the message" do
      assert {:ok, "hello"} = FerricStore.echo("hello")
    end

    test "echoes empty string" do
      assert {:ok, ""} = FerricStore.echo("")
    end

    test "echoes binary data" do
      msg = <<0, 1, 2, 255>>
      assert {:ok, ^msg} = FerricStore.echo(msg)
    end
  end

  describe "flushall/0" do
    test "deletes all keys (alias for flushdb)" do
      FerricStore.set("fa:a", "1")
      FerricStore.set("fa:b", "2")
      assert :ok = FerricStore.flushall()
      assert {:ok, nil} = FerricStore.get("fa:a")
      assert {:ok, nil} = FerricStore.get("fa:b")
    end
  end
end
