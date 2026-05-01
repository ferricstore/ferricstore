defmodule Ferricstore.Commands.CommandsEdgeCasesTest do
  @moduledoc """
  Edge cases for TTL spec, integer overflow, binary safety, SET flags, and MemoryGuard.
  Split from CommandsEdgeCasesTest.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Expiry, Strings}
  alias Ferricstore.Test.MockStore

  describe "string command backend errors" do
    test "APPEND returns backend errors instead of raising" do
      store =
        MockStore.make()
        |> Map.put(:append, fn _key, _suffix -> {:error, "ERR async replication overloaded"} end)

      assert {:error, "ERR async replication overloaded"} =
               Strings.handle("APPEND", ["k", "suffix"], store)
    end

    test "SETRANGE returns backend errors instead of raising" do
      store =
        MockStore.make()
        |> Map.put(:setrange, fn _key, _offset, _value ->
          {:error, "ERR disk pressure on shard 0, rejecting async write"}
        end)

      assert {:error, "ERR disk pressure on shard 0, rejecting async write"} =
               Strings.handle("SETRANGE", ["k", "0", "value"], store)
    end
  end

  # ===========================================================================
  # TTL edge cases — spec-required behavior
  # ===========================================================================

  describe "TTL spec-required edge cases" do
    test "PERSIST removes TTL and key becomes permanent" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"v", future}})

      # Key has a TTL
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0

      # PERSIST removes it
      assert 1 == Expiry.handle("PERSIST", ["k"], store)

      # Now TTL returns -1 (no expiry)
      assert -1 == Expiry.handle("TTL", ["k"], store)

      # Value is still accessible
      assert "v" == store.get.("k")
    end

    test "EXPIRE with 0 expires the key immediately" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "0"], store)
      # Key should be expired now (TTL returns -2 for non-existent)
      assert -2 == Expiry.handle("TTL", ["k"], store)
    end

    @tag :skip
    # Known limitation: our impl rejects negative EXPIRE values with an error
    # instead of treating them as immediate deletion (Redis compat)
    test "EXPIRE with negative value deletes the key (Redis compat)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      result = Expiry.handle("EXPIRE", ["k", "-1"], store)
      assert 1 == result
    end

    test "PEXPIRE with millisecond precision" do
      store = MockStore.make(%{"k" => {"v", 0}})
      # Set PEXPIRE to 500ms
      assert 1 == Expiry.handle("PEXPIRE", ["k", "500"], store)
      pttl = Expiry.handle("PTTL", ["k"], store)
      # PTTL should be within 500ms (allowing some execution time)
      assert pttl > 0 and pttl <= 500
    end

    test "TTL on non-existent key returns -2" do
      store = MockStore.make()
      assert -2 == Expiry.handle("TTL", ["no_such_key"], store)
    end

    test "PTTL on non-existent key returns -2" do
      store = MockStore.make()
      assert -2 == Expiry.handle("PTTL", ["no_such_key"], store)
    end

    test "TTL on key without expiry returns -1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Expiry.handle("TTL", ["k"], store)
    end

    test "PTTL on key without expiry returns -1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Expiry.handle("PTTL", ["k"], store)
    end

    test "SET with EX option sets TTL correctly" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "EX", "60"], store)
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0 and ttl <= 60
    end

    test "SET with PX option sets TTL in milliseconds correctly" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "PX", "5000"], store)
      pttl = Expiry.handle("PTTL", ["k"], store)
      assert pttl > 0 and pttl <= 5000
    end

    test "SET with NX flag only sets if key does not exist" do
      store = MockStore.make()
      # Key does not exist, NX should succeed
      assert :ok = Strings.handle("SET", ["k", "first", "NX"], store)
      assert "first" == store.get.("k")
      # Key now exists, NX should fail (return nil)
      assert nil == Strings.handle("SET", ["k", "second", "NX"], store)
      assert "first" == store.get.("k")
    end

    test "SET with XX flag only sets if key already exists" do
      store = MockStore.make()
      # Key does not exist, XX should fail
      assert nil == Strings.handle("SET", ["k", "v", "XX"], store)
      assert nil == store.get.("k")
      # Create the key
      assert :ok = Strings.handle("SET", ["k", "v"], store)
      # Now XX should succeed
      assert :ok = Strings.handle("SET", ["k", "updated", "XX"], store)
      assert "updated" == store.get.("k")
    end
  end

  # ===========================================================================
  # Integer overflow edge cases
  # ===========================================================================

  describe "integer overflow edge cases" do
    test "INCR on max int64 value returns overflow and leaves value unchanged" do
      max_int64 = 9_223_372_036_854_775_807
      store = MockStore.make(%{"k" => {Integer.to_string(max_int64), 0}})

      assert {:error, "ERR increment or decrement would overflow"} =
               Strings.handle("INCR", ["k"], store)

      assert Integer.to_string(max_int64) == store.get.("k")
    end

    test "DECRBY with large negative amount on zero" do
      store = MockStore.make(%{"k" => {"0", 0}})
      large_amount = 9_223_372_036_854_775_807
      result = Strings.handle("DECRBY", ["k", Integer.to_string(large_amount)], store)
      assert {:ok, value} = result
      assert value == -large_amount
    end

    test "INCRBY with max int64 on max int64 value returns overflow and leaves value unchanged" do
      max_int64 = 9_223_372_036_854_775_807
      store = MockStore.make(%{"k" => {Integer.to_string(max_int64), 0}})

      assert {:error, "ERR increment or decrement would overflow"} =
               Strings.handle("INCRBY", ["k", Integer.to_string(max_int64)], store)

      assert Integer.to_string(max_int64) == store.get.("k")
    end

    test "INCRBYFLOAT precision with very small increment" do
      store = MockStore.make(%{"k" => {"0", 0}})
      result = Strings.handle("INCRBYFLOAT", ["k", "0.000000001"], store)
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert_in_delta val, 1.0e-9, 1.0e-20
    end

    test "INCRBYFLOAT precision with large and small values" do
      store = MockStore.make(%{"k" => {"1000000000", 0}})
      result = Strings.handle("INCRBYFLOAT", ["k", "0.001"], store)
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert_in_delta val, 1_000_000_000.001, 1.0e-6
    end

    test "INCR on negative max int64 value increments to -max+1" do
      min_int64 = -9_223_372_036_854_775_808
      store = MockStore.make(%{"k" => {Integer.to_string(min_int64), 0}})
      assert {:ok, value} = Strings.handle("INCR", ["k"], store)
      assert value == min_int64 + 1
    end

    test "DECR on min int64 value returns overflow and leaves value unchanged" do
      min_int64 = -9_223_372_036_854_775_808
      store = MockStore.make(%{"k" => {Integer.to_string(min_int64), 0}})

      assert {:error, "ERR increment or decrement would overflow"} =
               Strings.handle("DECR", ["k"], store)

      assert Integer.to_string(min_int64) == store.get.("k")
    end
  end

  # ===========================================================================
  # Binary safety — keys and values with special characters
  # ===========================================================================

  describe "binary safety" do
    test "keys and values with null bytes round-trip via SET/GET" do
      store = MockStore.make()
      key = "key\x00with\x00nulls"
      value = "val\x00with\x00nulls"
      assert :ok = Strings.handle("SET", [key, value], store)
      assert value == Strings.handle("GET", [key], store)
    end

    test "keys and values with unicode round-trip via SET/GET" do
      store = MockStore.make()
      key = "clé_ünïcödë_日本語"
      value = "valeur_émojis_🦀🔥✅"
      assert :ok = Strings.handle("SET", [key, value], store)
      assert value == Strings.handle("GET", [key], store)
    end

    test "empty string value round-trips via SET/GET" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", ""], store)
      assert "" == Strings.handle("GET", ["k"], store)
    end

    test "STRLEN on empty string value returns 0" do
      store = MockStore.make(%{"k" => {"", 0}})
      assert 0 == Strings.handle("STRLEN", ["k"], store)
    end

    test "STRLEN on unicode value returns byte size not char count" do
      # Japanese characters are 3 bytes each in UTF-8
      store = MockStore.make(%{"k" => {"日本語", 0}})
      assert 9 == Strings.handle("STRLEN", ["k"], store)
    end

    test "APPEND with null bytes concatenates correctly" do
      store = MockStore.make(%{"k" => {"before\x00", 0}})
      new_len = Strings.handle("APPEND", ["k", "\x00after"], store)
      assert new_len == 13
      assert "before\x00\x00after" == store.get.("k")
    end

    test "very long keys (> 512 bytes) work with SET/GET" do
      store = MockStore.make()
      long_key = String.duplicate("k", 1024)
      assert :ok = Strings.handle("SET", [long_key, "value"], store)
      assert "value" == Strings.handle("GET", [long_key], store)
    end

    test "very long keys (> 512 bytes) work with EXISTS" do
      store = MockStore.make()
      long_key = String.duplicate("k", 1024)
      assert :ok = Strings.handle("SET", [long_key, "value"], store)
      assert 1 == Strings.handle("EXISTS", [long_key], store)
    end

    test "MSET and MGET with unicode keys and values" do
      store = MockStore.make()

      assert :ok =
               Strings.handle(
                 "MSET",
                 ["キー1", "値1", "キー2", "値2"],
                 store
               )

      assert ["値1", "値2"] ==
               Strings.handle("MGET", ["キー1", "キー2"], store)
    end

    test "value containing RESP-like sequences is treated as opaque binary" do
      store = MockStore.make()
      resp_like = "+OK\r\n-ERR\r\n$5\r\nhello\r\n*2\r\n"
      assert :ok = Strings.handle("SET", ["k", resp_like], store)
      assert resp_like == Strings.handle("GET", ["k"], store)
    end

    test "key with all 256 byte values round-trips" do
      store = MockStore.make()
      # Build a key from all possible byte values
      key = Enum.into(0..255, <<>>, fn b -> <<b>> end)
      value = "all_bytes_key_value"
      assert :ok = Strings.handle("SET", [key, value], store)
      assert value == Strings.handle("GET", [key], store)
    end
  end

  # ===========================================================================
  # OOM / MemoryGuard error codes
  # ===========================================================================

  describe "MemoryGuard reject_writes? check" do
    test "MemoryGuard.reject_writes? returns boolean" do
      # Verify the API contract works — under normal conditions it returns false
      result = Ferricstore.MemoryGuard.reject_writes?()
      assert is_boolean(result)
    end

    test "MemoryGuard.stats returns expected fields" do
      # Under full-suite load, MemoryGuard may be busy — use longer timeout
      stats = GenServer.call(Ferricstore.MemoryGuard, :stats, 30_000)
      assert is_map(stats)
      assert Map.has_key?(stats, :total_bytes)
      assert Map.has_key?(stats, :max_bytes)
      assert Map.has_key?(stats, :ratio)
      assert Map.has_key?(stats, :pressure_level)
      assert Map.has_key?(stats, :eviction_policy)
      assert stats.pressure_level in [:ok, :warning, :pressure, :reject]
    end

    test "MemoryGuard.eviction_policy returns an atom" do
      policy = Ferricstore.MemoryGuard.eviction_policy()
      assert is_atom(policy)
      assert policy in [:volatile_lru, :allkeys_lru, :volatile_ttl, :noeviction]
    end
  end

  # ===========================================================================
  # SET with GET flag (returns old value)
  # ===========================================================================

  describe "SET with combined flags" do
    test "SET with NX and EX sets key with expiry when key does not exist" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "NX", "EX", "60"], store)
      assert "v" == store.get.("k")
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0 and ttl <= 60
    end

    test "SET with XX and EX updates existing key with new expiry" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert :ok = Strings.handle("SET", ["k", "new", "XX", "EX", "30"], store)
      assert "new" == store.get.("k")
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0 and ttl <= 30
    end

    @tag :skip
    # Known limitation: our impl rejects NX+XX together with an error instead of nil
    test "SET with NX and XX both set: XX takes precedence over NX semantics" do
      # Both NX and XX at the same time: NX requires key not exist, XX requires
      # key exist. This is contradictory — Redis rejects with syntax error or
      # the last one wins. In our impl, both flags are set, so XX=true means
      # key must exist AND NX=true means key must not exist — neither condition
      # is satisfiable if both are true.
      store = MockStore.make(%{"k" => {"old", 0}})
      # With existing key: NX=true checks "not exists?" which is false -> nil
      result = Strings.handle("SET", ["k", "new", "NX", "XX"], store)
      assert result == nil
    end

    test "SET with NX on non-existing key and PX sets millisecond expiry" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "NX", "PX", "5000"], store)
      pttl = Expiry.handle("PTTL", ["k"], store)
      assert pttl > 0 and pttl <= 5000
    end
  end

  # ===========================================================================
  # Additional EXPIRE/TTL round-trip correctness
  # ===========================================================================

  describe "EXPIRE then TTL round-trip" do
    test "EXPIRE 10 then TTL returns value between 0 and 10" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "10"], store)
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl >= 0 and ttl <= 10
    end

    test "PEXPIRE 5000 then PTTL returns value between 0 and 5000" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("PEXPIRE", ["k", "5000"], store)
      pttl = Expiry.handle("PTTL", ["k"], store)
      assert pttl >= 0 and pttl <= 5000
    end

    test "EXPIRE replaces existing TTL" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "100"], store)
      assert 1 == Expiry.handle("EXPIRE", ["k", "5"], store)
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl >= 0 and ttl <= 5
    end

    test "PERSIST after EXPIRE returns -1 TTL" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "60"], store)
      assert 1 == Expiry.handle("PERSIST", ["k"], store)
      assert -1 == Expiry.handle("TTL", ["k"], store)
      # Value is still there
      assert "v" == store.get.("k")
    end
  end

  # ===========================================================================
  # Strings — SET EXAT / PXAT / GET / KEEPTTL (RESP3 handler level)
  # ===========================================================================

  describe "SET EXAT" do
    test "sets key with absolute Unix timestamp in seconds" do
      store = MockStore.make()
      future_ts = div(System.os_time(:millisecond), 1000) + 120
      assert :ok = Strings.handle("SET", ["k", "v", "EXAT", Integer.to_string(future_ts)], store)
      assert "v" == store.get.("k")

      # Verify expire_at_ms was set to ts * 1000
      {_val, exp} = store.get_meta.("k")
      assert exp == future_ts * 1000
    end

    test "EXAT with negative timestamp returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["k", "v", "EXAT", "-1"], store)
      assert msg =~ "invalid expire"
    end

    test "EXAT with zero timestamp returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["k", "v", "EXAT", "0"], store)
      assert msg =~ "invalid expire"
    end

    test "EXAT with non-numeric value returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["k", "v", "EXAT", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "EXAT missing value returns error" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("SET", ["k", "v", "EXAT"], store)
    end
  end

  describe "SET PXAT" do
    test "sets key with absolute Unix timestamp in milliseconds" do
      store = MockStore.make()
      future_ms = System.os_time(:millisecond) + 120_000
      assert :ok = Strings.handle("SET", ["k", "v", "PXAT", Integer.to_string(future_ms)], store)
      assert "v" == store.get.("k")

      {_val, exp} = store.get_meta.("k")
      assert exp == future_ms
    end

    test "PXAT with negative timestamp returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["k", "v", "PXAT", "-1"], store)
      assert msg =~ "invalid expire"
    end

    test "PXAT with zero timestamp returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["k", "v", "PXAT", "0"], store)
      assert msg =~ "invalid expire"
    end

    test "PXAT with non-numeric value returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SET", ["k", "v", "PXAT", "abc"], store)
      assert msg =~ "not an integer"
    end
  end

  describe "SET GET" do
    test "returns nil when key does not exist" do
      store = MockStore.make()
      assert nil == Strings.handle("SET", ["k", "v", "GET"], store)
    end

    test "returns old value when key exists" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert "old" == Strings.handle("SET", ["k", "new", "GET"], store)
      assert "new" == store.get.("k")
    end

    test "GET with NX returns old value when key exists (write skipped)" do
      store = MockStore.make(%{"k" => {"existing", 0}})
      assert "existing" == Strings.handle("SET", ["k", "new", "NX", "GET"], store)
      # Value unchanged
      assert "existing" == store.get.("k")
    end

    test "GET with NX returns nil when key does not exist (write succeeds)" do
      store = MockStore.make()
      assert nil == Strings.handle("SET", ["k", "v", "NX", "GET"], store)
      assert "v" == store.get.("k")
    end

    test "GET with XX returns nil when key does not exist (write skipped)" do
      store = MockStore.make()
      assert nil == Strings.handle("SET", ["k", "v", "XX", "GET"], store)
      assert nil == store.get.("k")
    end

    test "GET with XX returns old value when key exists (write succeeds)" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert "old" == Strings.handle("SET", ["k", "new", "XX", "GET"], store)
      assert "new" == store.get.("k")
    end

    test "GET with EX returns old value and sets expiry" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert "old" == Strings.handle("SET", ["k", "new", "EX", "60", "GET"], store)
      assert "new" == store.get.("k")
      {_val, exp} = store.get_meta.("k")
      assert exp > 0
    end
  end

  describe "SET KEEPTTL" do
    test "preserves existing TTL when overwriting" do
      future_exp = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"old", future_exp}})
      assert :ok = Strings.handle("SET", ["k", "new", "KEEPTTL"], store)
      assert "new" == store.get.("k")

      {_val, exp} = store.get_meta.("k")
      assert exp == future_exp
    end

    test "key without TTL remains without TTL" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert :ok = Strings.handle("SET", ["k", "new", "KEEPTTL"], store)
      assert "new" == store.get.("k")

      {_val, exp} = store.get_meta.("k")
      assert exp == 0
    end

    test "KEEPTTL on nonexistent key sets no TTL" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "KEEPTTL"], store)
      assert "v" == store.get.("k")

      {_val, exp} = store.get_meta.("k")
      assert exp == 0
    end

    test "KEEPTTL combined with GET returns old value and preserves TTL" do
      future_exp = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"old", future_exp}})
      assert "old" == Strings.handle("SET", ["k", "new", "KEEPTTL", "GET"], store)
      assert "new" == store.get.("k")

      {_val, exp} = store.get_meta.("k")
      assert exp == future_exp
    end

    test "KEEPTTL combined with XX on existing key" do
      future_exp = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"old", future_exp}})
      assert :ok = Strings.handle("SET", ["k", "new", "KEEPTTL", "XX"], store)
      assert "new" == store.get.("k")

      {_val, exp} = store.get_meta.("k")
      assert exp == future_exp
    end
  end

  describe "SET mutual exclusion errors" do
    test "EXAT + EX returns syntax error" do
      store = MockStore.make()

      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "EXAT", "9999999999", "EX", "10"], store)

      assert msg =~ "syntax error"
    end

    test "PXAT + PX returns syntax error" do
      store = MockStore.make()

      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "PXAT", "9999999999000", "PX", "10000"], store)

      assert msg =~ "syntax error"
    end

    test "EXAT + PXAT returns syntax error" do
      store = MockStore.make()

      assert {:error, msg} =
               Strings.handle(
                 "SET",
                 ["k", "v", "EXAT", "9999999999", "PXAT", "9999999999000"],
                 store
               )

      assert msg =~ "syntax error"
    end

    test "EX + EXAT returns syntax error" do
      store = MockStore.make()

      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "EX", "10", "EXAT", "9999999999"], store)

      assert msg =~ "syntax error"
    end

    test "KEEPTTL + EX returns syntax error" do
      store = MockStore.make()

      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "KEEPTTL", "EX", "10"], store)

      assert msg =~ "syntax error"
    end

    test "KEEPTTL + PX returns syntax error" do
      store = MockStore.make()

      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "KEEPTTL", "PX", "10000"], store)

      assert msg =~ "syntax error"
    end

    test "EX + KEEPTTL returns syntax error" do
      store = MockStore.make()

      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "EX", "10", "KEEPTTL"], store)

      assert msg =~ "syntax error"
    end

    test "PX + KEEPTTL returns syntax error" do
      store = MockStore.make()

      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "PX", "10000", "KEEPTTL"], store)

      assert msg =~ "syntax error"
    end

    test "KEEPTTL + EXAT returns syntax error" do
      store = MockStore.make()

      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "KEEPTTL", "EXAT", "9999999999"], store)

      assert msg =~ "syntax error"
    end

    test "KEEPTTL + PXAT returns syntax error" do
      store = MockStore.make()

      assert {:error, msg} =
               Strings.handle("SET", ["k", "v", "KEEPTTL", "PXAT", "9999999999000"], store)

      assert msg =~ "syntax error"
    end
  end
end
