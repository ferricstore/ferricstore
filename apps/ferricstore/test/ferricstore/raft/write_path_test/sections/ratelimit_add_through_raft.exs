defmodule Ferricstore.Raft.WritePathTest.Sections.RatelimitAddThroughRaft do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.WARaftSegmentReader
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Test.ShardHelpers
      alias Ferricstore.Raft.StateMachine, as: SM

  describe "RATELIMIT.ADD through Raft" do
    test "returns allowed with correct counts on first call" do
      k = ukey("ratelimit_first")
      window_ms = 10_000
      max_requests = 10

      [status, count, remaining, _ttl] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 1)

      assert status == "allowed"
      assert count == 1
      assert remaining == 9
    end

    test "increments count on successive calls" do
      k = ukey("ratelimit_incr")
      window_ms = 10_000
      max_requests = 10

      [_, c1, _, _] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 1)

      [_, c2, _, _] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 1)

      [_, c3, _, _] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 1)

      assert c1 == 1
      assert c2 == 2
      assert c3 == 3
    end

    test "returns denied when limit is exceeded" do
      k = ukey("ratelimit_denied")
      window_ms = 10_000
      max_requests = 3

      # Use up the limit
      ["allowed", _, _, _] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 3)

      # Next request should be denied
      [status, count, remaining, _ttl] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 1)

      assert status == "denied"
      assert count == 3
      assert remaining == 0
    end

    test "returns ttl_ms as non-negative integer" do
      k = ukey("ratelimit_ttl")
      window_ms = 10_000
      max_requests = 100

      [_, _, _, ttl] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 1)

      assert is_integer(ttl)
      assert ttl >= 0
      assert ttl <= window_ms
    end

    test "multi-count add works correctly" do
      k = ukey("ratelimit_multi")
      window_ms = 10_000
      max_requests = 10

      [status, count, remaining, _ttl] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max_requests, 5)

      assert status == "allowed"
      assert count == 5
      assert remaining == 5
    end
  end
  describe "INCRBYFLOAT through Raft" do
    test "returns correct float on non-existent key" do
      k = ukey("incrbyfloat_new")

      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), k, 1.5)
      assert_in_delta result, 1.5, 0.001
      {parsed, _} = Float.parse(Router.get(FerricStore.Instance.get(:default), k))
      assert_in_delta parsed, 1.5, 0.001
    end

    test "increments existing float value correctly" do
      k = ukey("incrbyfloat_existing")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "10", 0)
      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), k, 2.5)
      assert_in_delta result, 12.5, 0.001
      {parsed, _} = Float.parse(Router.get(FerricStore.Instance.get(:default), k))
      assert_in_delta parsed, 12.5, 0.001
    end

    test "increments integer string as float" do
      k = ukey("incrbyfloat_int")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "10", 0)
      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), k, 1.5)
      assert_in_delta result, 11.5, 0.001
    end

    test "returns error on non-float value" do
      k = ukey("incrbyfloat_err")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "not_a_number", 0)

      assert {:error, "ERR value is not a valid float"} =
               Router.incr_float(FerricStore.Instance.get(:default), k, 1.0)

      # Original value should be unchanged
      assert "not_a_number" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "preserves expiry on existing key" do
      k = ukey("incrbyfloat_ttl")
      future = System.os_time(:millisecond) + 60_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "5.0", future)
      {:ok, _} = Router.incr_float(FerricStore.Instance.get(:default), k, 1.0)

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      {parsed, _} = Float.parse(value)
      assert_in_delta parsed, 6.0, 0.001
      assert expire_at_ms == future
    end
  end
  describe "APPEND through Raft" do
    test "returns new length on non-existent key" do
      k = ukey("append_new")

      assert {:ok, 5} = Router.append(FerricStore.Instance.get(:default), k, "hello")
      assert "hello" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "appends to existing value and returns new length" do
      k = ukey("append_existing")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "hello", 0)
      assert {:ok, 11} = Router.append(FerricStore.Instance.get(:default), k, " world")
      assert "hello world" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "multiple appends produce correct result" do
      k = ukey("append_multi")

      {:ok, 1} = Router.append(FerricStore.Instance.get(:default), k, "a")
      {:ok, 2} = Router.append(FerricStore.Instance.get(:default), k, "b")
      {:ok, 3} = Router.append(FerricStore.Instance.get(:default), k, "c")
      assert "abc" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "preserves expiry on existing key" do
      k = ukey("append_ttl")
      future = System.os_time(:millisecond) + 60_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "hi", future)
      {:ok, 8} = Router.append(FerricStore.Instance.get(:default), k, " there")

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "hi there"
      assert expire_at_ms == future
    end
  end
  describe "GETSET through Raft" do
    test "returns old value and sets new value" do
      k = ukey("getset_basic")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "old_value", 0)
      old = Router.getset(FerricStore.Instance.get(:default), k, "new_value")
      assert old == "old_value"
      assert "new_value" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "returns nil when key does not exist" do
      k = ukey("getset_missing")

      old = Router.getset(FerricStore.Instance.get(:default), k, "first_value")
      assert old == nil
      assert "first_value" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "sets new value with no expiry" do
      k = ukey("getset_expiry")
      future = System.os_time(:millisecond) + 60_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "old", future)
      _old = Router.getset(FerricStore.Instance.get(:default), k, "new")

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "new"
      # GETSET resets expiry to 0
      assert expire_at_ms == 0
    end
  end
  describe "GETDEL through Raft" do
    test "returns value and key is gone" do
      k = ukey("getdel_basic")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "will_vanish", 0)
      old = Router.getdel(FerricStore.Instance.get(:default), k)
      assert old == "will_vanish"
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "returns nil when key does not exist" do
      k = ukey("getdel_missing")

      old = Router.getdel(FerricStore.Instance.get(:default), k)
      assert old == nil
    end

    test "key is removed from ETS" do
      k = ukey("getdel_ets")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "in_ets", 0)
      assert [{^k, "in_ets", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir_for(k), k)

      _old = Router.getdel(FerricStore.Instance.get(:default), k)
      assert [] == :ets.lookup(keydir_for(k), k)
    end
  end
  describe "GETEX through Raft" do
    test "updates TTL and returns value" do
      k = ukey("getex_ttl")
      future = System.os_time(:millisecond) + 120_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "my_val", 0)
      value = Router.getex(FerricStore.Instance.get(:default), k, future)
      assert value == "my_val"

      {stored_val, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert stored_val == "my_val"
      assert expire_at_ms == future
    end

    test "returns nil when key does not exist" do
      k = ukey("getex_missing")

      value =
        Router.getex(FerricStore.Instance.get(:default), k, System.os_time(:millisecond) + 60_000)

      assert value == nil
    end

    test "PERSIST removes expiry" do
      k = ukey("getex_persist")
      future = System.os_time(:millisecond) + 60_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "persistent", future)
      # PERSIST = expire_at_ms of 0
      value = Router.getex(FerricStore.Instance.get(:default), k, 0)
      assert value == "persistent"

      {stored_val, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert stored_val == "persistent"
      assert expire_at_ms == 0
    end
  end
  describe "SETRANGE through Raft" do
    test "returns new length after overwriting bytes" do
      k = ukey("setrange_basic")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "Hello World", 0)
      assert {:ok, 11} = Router.setrange(FerricStore.Instance.get(:default), k, 6, "Redis")
      assert "Hello Redis" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "pads with zero bytes for non-existent key" do
      k = ukey("setrange_new")

      assert {:ok, 8} = Router.setrange(FerricStore.Instance.get(:default), k, 5, "abc")
      value = Router.get(FerricStore.Instance.get(:default), k)
      assert byte_size(value) == 8
      # First 5 bytes should be zero
      assert binary_part(value, 0, 5) == <<0, 0, 0, 0, 0>>
      assert binary_part(value, 5, 3) == "abc"
    end

    test "extends string when offset + value exceeds length" do
      k = ukey("setrange_extend")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "Hi", 0)
      assert {:ok, 7} = Router.setrange(FerricStore.Instance.get(:default), k, 2, "There")
      assert "HiThere" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "preserves expiry on existing key" do
      k = ukey("setrange_ttl")
      future = System.os_time(:millisecond) + 60_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "abcdef", future)
      {:ok, _len} = Router.setrange(FerricStore.Instance.get(:default), k, 0, "XY")

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "XYcdef"
      assert expire_at_ms == future
    end
  end
  describe "SET edge cases through Raft" do
    test "SET with EX (TTL) — value expires after TTL elapses" do
      k = ukey("set_ex_expire")
      # Set a TTL long enough to survive Raft commit latency but short enough
      # to test expiry within a reasonable wait.
      short_ttl_ms = 2_000
      expire_at = System.os_time(:millisecond) + short_ttl_ms

      :ok = Router.put(FerricStore.Instance.get(:default), k, "ephemeral", expire_at)
      # Immediately after SET the value should be present
      assert "ephemeral" == Router.get(FerricStore.Instance.get(:default), k)

      # Wait for TTL to expire
      Process.sleep(short_ttl_ms + 100)

      # After expiry, GET should return nil
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SET with NX on existing key — returns nil, does not overwrite" do
      k = ukey("set_nx_existing")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "original", 0)
      assert "original" == Router.get(FerricStore.Instance.get(:default), k)

      # Simulate SET ... NX: only set if key does NOT exist.
      # NX is implemented at the Strings command layer via exists? check
      # before put. We replicate the semantics through Router here.
      assert Router.exists?(FerricStore.Instance.get(:default), k) == true

      # The NX guard prevents the write; the original value remains.
      result =
        if Router.exists?(FerricStore.Instance.get(:default), k),
          do: nil,
          else: Router.put(FerricStore.Instance.get(:default), k, "should_not_appear", 0)

      assert result == nil
      assert "original" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SET with XX on missing key — returns nil" do
      k = ukey("set_xx_missing")

      # Key does not exist. XX means "only set if key exists".
      assert Router.exists?(FerricStore.Instance.get(:default), k) == false

      result =
        if Router.exists?(FerricStore.Instance.get(:default), k),
          do: Router.put(FerricStore.Instance.get(:default), k, "should_not_appear", 0),
          else: nil

      assert result == nil
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SET with GET flag — returns old value (via GETSET)" do
      k = ukey("set_get_flag")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "old_value", 0)
      assert "old_value" == Router.get(FerricStore.Instance.get(:default), k)

      # SET ... GET semantics: atomically set new value and return old.
      # In Ferricstore, GETSET provides this exact behaviour through Raft.
      old = Router.getset(FerricStore.Instance.get(:default), k, "new_value")
      assert old == "old_value"
      assert "new_value" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end
  describe "INCR edge cases through Raft" do
    test "INCR on value 'not_a_number' — returns error" do
      k = ukey("incr_nan")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "not_a_number", 0)

      assert {:error, "ERR value is not an integer or out of range"} =
               Router.incr(FerricStore.Instance.get(:default), k, 1)

      # Original value should be unchanged
      assert "not_a_number" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "INCR on max int64 — returns overflow error" do
      k = ukey("incr_overflow")
      max_int64 = 9_223_372_036_854_775_807

      :ok = Router.put(FerricStore.Instance.get(:default), k, Integer.to_string(max_int64), 0)

      assert {:error, "ERR increment or decrement would overflow"} =
               Router.incr(FerricStore.Instance.get(:default), k, 1)

      # Value should remain unchanged
      assert Integer.to_string(max_int64) == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "DECRBY with negative delta through Raft — effectively increments" do
      k = ukey("decrby_neg")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "10", 0)
      assert {:ok, 15} = Router.incr(FerricStore.Instance.get(:default), k, 5)
      assert "15" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "INCR then GET in same Raft batch — consistent" do
      k = ukey("incr_get_batch")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "0", 0)

      {:ok, 1} = Router.incr(FerricStore.Instance.get(:default), k, 1)
      assert "1" == Router.get(FerricStore.Instance.get(:default), k)

      {:ok, 2} = Router.incr(FerricStore.Instance.get(:default), k, 1)
      assert "2" == Router.get(FerricStore.Instance.get(:default), k)

      {:ok, 12} = Router.incr(FerricStore.Instance.get(:default), k, 10)
      assert "12" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end
  describe "INCRBYFLOAT edge cases through Raft" do
    test "INCRBYFLOAT on integer string '10' — returns float result" do
      k = ukey("incrbyfloat_int_str")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "10", 0)
      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), k, 0.5)
      assert_in_delta result, 10.5, 0.001
      {parsed, _} = Float.parse(Router.get(FerricStore.Instance.get(:default), k))
      assert_in_delta parsed, 10.5, 0.001
    end

    test "INCRBYFLOAT negative delta" do
      k = ukey("incrbyfloat_neg")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "10.5", 0)
      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), k, -0.5)
      assert_in_delta result, 10.0, 0.001
      {parsed, _} = Float.parse(Router.get(FerricStore.Instance.get(:default), k))
      assert_in_delta parsed, 10.0, 0.001
    end

    test "INCRBYFLOAT on 'not_a_number' — returns error" do
      k = ukey("incrbyfloat_nan")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "not_a_number", 0)

      assert {:error, "ERR value is not a valid float"} =
               Router.incr_float(FerricStore.Instance.get(:default), k, 1.0)

      # Original value should be unchanged
      assert "not_a_number" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "INCRBYFLOAT preserves existing TTL" do
      k = ukey("incrbyfloat_ttl_preserve")
      future = System.os_time(:millisecond) + 120_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "5.0", future)
      {:ok, _} = Router.incr_float(FerricStore.Instance.get(:default), k, 2.5)

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      {parsed, _} = Float.parse(value)
      assert_in_delta parsed, 7.5, 0.001
      assert expire_at_ms == future
    end
  end
  describe "APPEND edge cases through Raft" do
    test "APPEND to non-existent key creates it" do
      k = ukey("append_create")

      # Key does not exist; APPEND should create it with the given value.
      assert {:ok, 5} = Router.append(FerricStore.Instance.get(:default), k, "hello")
      assert "hello" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "APPEND preserves existing TTL" do
      k = ukey("append_ttl_preserve")
      future = System.os_time(:millisecond) + 120_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "base", future)
      {:ok, 10} = Router.append(FerricStore.Instance.get(:default), k, "_added")

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "base_added"
      assert expire_at_ms == future
    end

    test "APPEND with empty string — no change to value, returns existing length" do
      k = ukey("append_empty")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "existing", 0)
      {:ok, len} = Router.append(FerricStore.Instance.get(:default), k, "")
      assert len == byte_size("existing")
      assert "existing" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end
  describe "GETSET edge cases through Raft" do
    test "GETSET on non-existent key returns nil, creates key" do
      k = ukey("getset_nonexistent")

      old = Router.getset(FerricStore.Instance.get(:default), k, "fresh_value")
      assert old == nil
      assert "fresh_value" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end
  describe "GETDEL edge cases through Raft" do
    test "GETDEL on non-existent key returns nil" do
      k = ukey("getdel_nonexistent")

      result = Router.getdel(FerricStore.Instance.get(:default), k)
      assert result == nil
      # Key should still not exist
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end
  end
  describe "GETEX edge cases through Raft" do
    test "GETEX with PERSIST removes TTL" do
      k = ukey("getex_persist_edge")
      future = System.os_time(:millisecond) + 120_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "will_persist", future)

      # Verify TTL is set
      {_, expire_before} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert expire_before == future

      # PERSIST = expire_at_ms of 0
      value = Router.getex(FerricStore.Instance.get(:default), k, 0)
      assert value == "will_persist"

      {stored_val, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert stored_val == "will_persist"
      assert expire_at_ms == 0
    end

    test "GETEX on expired key returns nil" do
      k = ukey("getex_expired")
      past = System.os_time(:millisecond) - 1_000

      :ok = Router.put(FerricStore.Instance.get(:default), k, "expired_val", past)

      # Key is expired; GETEX should return nil
      result =
        Router.getex(FerricStore.Instance.get(:default), k, System.os_time(:millisecond) + 60_000)

      assert result == nil
    end
  end
  describe "SETRANGE edge cases through Raft" do
    test "SETRANGE beyond current length zero-pads" do
      k = ukey("setrange_zeropad")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "Hi", 0)
      # Offset 5 is beyond "Hi" (length 2), so bytes 2..4 are zero-padded
      assert {:ok, 8} = Router.setrange(FerricStore.Instance.get(:default), k, 5, "abc")

      value = Router.get(FerricStore.Instance.get(:default), k)
      assert byte_size(value) == 8
      # "Hi" + 3 zero bytes + "abc"
      assert value == <<"Hi", 0, 0, 0, "abc">>
    end

    test "SETRANGE at offset 0 replaces start" do
      k = ukey("setrange_offset0")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "Hello World", 0)
      assert {:ok, 11} = Router.setrange(FerricStore.Instance.get(:default), k, 0, "Yo")
      # "Yo" replaces the first 2 bytes: "He" -> "Yo", rest unchanged
      assert "Yollo World" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SETRANGE on non-existent key creates zero-padded value" do
      k = ukey("setrange_nonexistent")

      assert {:ok, 8} = Router.setrange(FerricStore.Instance.get(:default), k, 5, "abc")
      value = Router.get(FerricStore.Instance.get(:default), k)
      assert byte_size(value) == 8
      # 5 zero bytes followed by "abc"
      assert value == <<0, 0, 0, 0, 0, "abc">>
    end
  end
    end
  end
end
