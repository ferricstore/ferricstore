defmodule Ferricstore.Commands.StringsExtendedTest do
  @moduledoc """
  Tests for APPEND, STRLEN, GETSET, GETDEL, GETEX, SETNX, SETEX, PSETEX,
  GETRANGE, SETRANGE, and MSETNX commands.

  Covers happy paths, error cases, and edge conditions.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Strings
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # APPEND
  # ===========================================================================

  describe "APPEND" do
    test "APPEND to non-existent key creates it with value" do
      store = MockStore.make()
      assert 5 = Strings.handle("APPEND", ["k", "hello"], store)
      assert "hello" == store.get.("k")
    end

    test "APPEND to existing key concatenates value" do
      store = MockStore.make(%{"k" => {"hello", 0}})
      assert 11 = Strings.handle("APPEND", ["k", " world"], store)
      assert "hello world" == store.get.("k")
    end

    test "APPEND returns new byte length" do
      store = MockStore.make(%{"k" => {"abc", 0}})
      assert 6 = Strings.handle("APPEND", ["k", "def"], store)
    end

    test "APPEND with empty string returns existing length" do
      store = MockStore.make(%{"k" => {"hello", 0}})
      assert 5 = Strings.handle("APPEND", ["k", ""], store)
      assert "hello" == store.get.("k")
    end

    test "APPEND with empty string to non-existent key returns 0" do
      store = MockStore.make()
      assert 0 = Strings.handle("APPEND", ["k", ""], store)
      assert "" == store.get.("k")
    end

    test "APPEND preserves TTL" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"hello", future}})
      assert 11 = Strings.handle("APPEND", ["k", " world"], store)
      {_, exp} = store.get_meta.("k")
      assert exp == future
    end

    test "APPEND with binary data" do
      store = MockStore.make(%{"k" => {<<1, 2, 3>>, 0}})
      assert 6 = Strings.handle("APPEND", ["k", <<4, 5, 6>>], store)
      assert <<1, 2, 3, 4, 5, 6>> == store.get.("k")
    end

    test "APPEND wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("APPEND", [], store)
      assert {:error, _} = Strings.handle("APPEND", ["k"], store)
      assert {:error, _} = Strings.handle("APPEND", ["k", "v", "extra"], store)
    end
  end

  # ===========================================================================
  # STRLEN
  # ===========================================================================

  describe "STRLEN" do
    test "STRLEN of existing key returns byte length" do
      store = MockStore.make(%{"k" => {"hello", 0}})
      assert 5 = Strings.handle("STRLEN", ["k"], store)
    end

    test "STRLEN reads value size without loading the value" do
      store = %{
        value_size: fn "cold_large" -> 10_000 end,
        get: fn _key -> flunk("STRLEN should not load the value") end,
        compound_get: fn _redis_key, _compound_key -> nil end
      }

      assert 10_000 == Strings.handle("STRLEN", ["cold_large"], store)
    end

    test "STRLEN of non-existent key returns 0" do
      store = MockStore.make()
      assert 0 = Strings.handle("STRLEN", ["missing"], store)
    end

    test "STRLEN of empty string returns 0" do
      store = MockStore.make(%{"k" => {"", 0}})
      assert 0 = Strings.handle("STRLEN", ["k"], store)
    end

    test "STRLEN counts bytes not characters (multibyte UTF-8)" do
      # UTF-8 encoding of non-ASCII chars
      store = MockStore.make(%{"k" => {"cafe\u0301", 0}})
      # 'cafe' + combining accent = 4 bytes + 2 bytes = 6 bytes
      assert byte_size("cafe\u0301") == Strings.handle("STRLEN", ["k"], store)
    end

    test "STRLEN wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("STRLEN", [], store)
      assert {:error, _} = Strings.handle("STRLEN", ["a", "b"], store)
    end
  end

  # ===========================================================================
  # GETSET
  # ===========================================================================

  describe "GETSET" do
    test "GETSET on existing key returns old value and sets new" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert "old" = Strings.handle("GETSET", ["k", "new"], store)
      assert "new" == store.get.("k")
    end

    test "GETSET on non-existent key returns nil and sets value" do
      store = MockStore.make()
      assert nil == Strings.handle("GETSET", ["k", "val"], store)
      assert "val" == store.get.("k")
    end

    test "GETSET removes expiry (stores with expire_at_ms=0)" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"old", future}})
      assert "old" = Strings.handle("GETSET", ["k", "new"], store)
      {_, exp} = store.get_meta.("k")
      assert exp == 0
    end

    test "GETSET wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("GETSET", [], store)
      assert {:error, _} = Strings.handle("GETSET", ["k"], store)
      assert {:error, _} = Strings.handle("GETSET", ["k", "v", "extra"], store)
    end
  end

  # ===========================================================================
  # GETDEL
  # ===========================================================================

  describe "GETDEL" do
    test "GETDEL on existing key returns value and deletes" do
      store = MockStore.make(%{"k" => {"val", 0}})
      assert "val" = Strings.handle("GETDEL", ["k"], store)
      assert nil == store.get.("k")
    end

    test "GETDEL on non-existent key returns nil" do
      store = MockStore.make()
      assert nil == Strings.handle("GETDEL", ["missing"], store)
    end

    test "GETDEL on expired key returns nil" do
      past = System.os_time(:millisecond) - 1_000
      store = MockStore.make(%{"k" => {"val", past}})
      assert nil == Strings.handle("GETDEL", ["k"], store)
    end

    test "GETDEL wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("GETDEL", [], store)
      assert {:error, _} = Strings.handle("GETDEL", ["a", "b"], store)
    end

    test "GETDEL followed by EXISTS returns 0" do
      store = MockStore.make(%{"k" => {"val", 0}})
      Strings.handle("GETDEL", ["k"], store)
      assert false == store.exists?.("k")
    end
  end

  # ===========================================================================
  # GETEX
  # ===========================================================================

  describe "GETEX" do
    test "GETEX without options returns value without changing TTL" do
      store = MockStore.make(%{"k" => {"val", 0}})
      assert "val" = Strings.handle("GETEX", ["k"], store)
    end

    test "GETEX with EX sets expiry in seconds" do
      store = MockStore.make(%{"k" => {"val", 0}})
      before_ms = System.os_time(:millisecond)
      assert "val" = Strings.handle("GETEX", ["k", "EX", "10"], store)
      {_, exp} = store.get_meta.("k")
      assert exp >= before_ms + 9_000
      assert exp <= before_ms + 11_000
    end

    test "GETEX with PX sets expiry in milliseconds" do
      store = MockStore.make(%{"k" => {"val", 0}})
      before_ms = System.os_time(:millisecond)
      assert "val" = Strings.handle("GETEX", ["k", "PX", "5000"], store)
      {_, exp} = store.get_meta.("k")
      assert exp >= before_ms + 4_000
      assert exp <= before_ms + 6_000
    end

    test "GETEX with EXAT sets absolute expiry in seconds" do
      store = MockStore.make(%{"k" => {"val", 0}})
      future_unix = div(System.os_time(:millisecond), 1_000) + 3_600
      assert "val" = Strings.handle("GETEX", ["k", "EXAT", "#{future_unix}"], store)
      {_, exp} = store.get_meta.("k")
      assert exp == future_unix * 1_000
    end

    test "GETEX with PXAT sets absolute expiry in milliseconds" do
      store = MockStore.make(%{"k" => {"val", 0}})
      future_ms = System.os_time(:millisecond) + 3_600_000
      assert "val" = Strings.handle("GETEX", ["k", "PXAT", "#{future_ms}"], store)
      {_, exp} = store.get_meta.("k")
      assert exp == future_ms
    end

    test "GETEX with PERSIST removes expiry" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"val", future}})
      assert "val" = Strings.handle("GETEX", ["k", "PERSIST"], store)
      {_, exp} = store.get_meta.("k")
      assert exp == 0
    end

    test "GETEX on non-existent key returns nil" do
      store = MockStore.make()
      assert nil == Strings.handle("GETEX", ["missing", "EX", "10"], store)
    end

    test "GETEX with EX 0 returns error" do
      store = MockStore.make(%{"k" => {"val", 0}})
      assert {:error, msg} = Strings.handle("GETEX", ["k", "EX", "0"], store)
      assert msg =~ "invalid expire"
    end

    test "GETEX with EX negative returns error" do
      store = MockStore.make(%{"k" => {"val", 0}})
      assert {:error, msg} = Strings.handle("GETEX", ["k", "EX", "-1"], store)
      assert msg =~ "invalid expire"
    end

    test "GETEX with PX 0 returns error" do
      store = MockStore.make(%{"k" => {"val", 0}})
      assert {:error, _} = Strings.handle("GETEX", ["k", "PX", "0"], store)
    end

    test "GETEX with unknown option returns syntax error" do
      store = MockStore.make(%{"k" => {"val", 0}})
      assert {:error, _} = Strings.handle("GETEX", ["k", "BOGUS"], store)
    end

    test "GETEX with no args returns error" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("GETEX", [], store)
    end

    test "GETEX with non-integer EX returns error" do
      store = MockStore.make(%{"k" => {"val", 0}})
      assert {:error, _} = Strings.handle("GETEX", ["k", "EX", "abc"], store)
    end
  end

  # ===========================================================================
  # SETNX
  # ===========================================================================

  describe "SETNX" do
    test "SETNX on non-existent key sets value and returns 1" do
      store = MockStore.make()
      assert 1 = Strings.handle("SETNX", ["k", "val"], store)
      assert "val" == store.get.("k")
    end

    test "SETNX on existing key does not overwrite and returns 0" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert 0 = Strings.handle("SETNX", ["k", "new"], store)
      assert "old" == store.get.("k")
    end

    test "SETNX wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("SETNX", [], store)
      assert {:error, _} = Strings.handle("SETNX", ["k"], store)
      assert {:error, _} = Strings.handle("SETNX", ["k", "v", "extra"], store)
    end
  end

  # ===========================================================================
  # SETEX
  # ===========================================================================

  describe "SETEX" do
    test "SETEX sets key with expiry in seconds" do
      store = MockStore.make()
      before_ms = System.os_time(:millisecond)
      assert :ok = Strings.handle("SETEX", ["k", "10", "val"], store)
      assert "val" == store.get.("k")
      {_, exp} = store.get_meta.("k")
      assert exp >= before_ms + 9_000
    end

    test "SETEX with 0 seconds returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SETEX", ["k", "0", "val"], store)
      assert msg =~ "invalid expire"
    end

    test "SETEX with negative seconds returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SETEX", ["k", "-1", "val"], store)
      assert msg =~ "invalid expire"
    end

    test "SETEX with non-integer seconds returns error" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("SETEX", ["k", "abc", "val"], store)
    end

    test "SETEX overwrites existing key" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert :ok = Strings.handle("SETEX", ["k", "10", "new"], store)
      assert "new" == store.get.("k")
    end

    test "SETEX wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("SETEX", [], store)
      assert {:error, _} = Strings.handle("SETEX", ["k", "10"], store)
      assert {:error, _} = Strings.handle("SETEX", ["k"], store)
    end
  end

  # ===========================================================================
  # PSETEX
  # ===========================================================================

  describe "PSETEX" do
    test "PSETEX sets key with expiry in milliseconds" do
      store = MockStore.make()
      before_ms = System.os_time(:millisecond)
      assert :ok = Strings.handle("PSETEX", ["k", "5000", "val"], store)
      assert "val" == store.get.("k")
      {_, exp} = store.get_meta.("k")
      assert exp >= before_ms + 4_000
    end

    test "PSETEX with 0 ms returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("PSETEX", ["k", "0", "val"], store)
      assert msg =~ "invalid expire"
    end

    test "PSETEX with negative ms returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("PSETEX", ["k", "-1", "val"], store)
      assert msg =~ "invalid expire"
    end

    test "PSETEX with non-integer ms returns error" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("PSETEX", ["k", "abc", "val"], store)
    end

    test "PSETEX wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("PSETEX", [], store)
      assert {:error, _} = Strings.handle("PSETEX", ["k", "5000"], store)
    end
  end

  # ===========================================================================
  # GETRANGE
  # ===========================================================================

  describe "GETRANGE" do
    test "GETRANGE returns substring" do
      store = MockStore.make(%{"k" => {"Hello, World!", 0}})
      assert "Hello" = Strings.handle("GETRANGE", ["k", "0", "4"], store)
    end

    test "GETRANGE with negative start index" do
      store = MockStore.make(%{"k" => {"Hello, World!", 0}})
      assert "World!" = Strings.handle("GETRANGE", ["k", "-6", "-1"], store)
    end

    test "GETRANGE with negative end index" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      assert "Hell" = Strings.handle("GETRANGE", ["k", "0", "-2"], store)
    end

    test "GETRANGE entire string with 0 -1" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      assert "Hello" = Strings.handle("GETRANGE", ["k", "0", "-1"], store)
    end

    test "GETRANGE end beyond string length clamps" do
      store = MockStore.make(%{"k" => {"Hi", 0}})
      assert "Hi" = Strings.handle("GETRANGE", ["k", "0", "100"], store)
    end

    test "GETRANGE start > end returns empty string" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      assert "" = Strings.handle("GETRANGE", ["k", "3", "1"], store)
    end

    test "GETRANGE on non-existent key returns empty string" do
      store = MockStore.make()
      assert "" = Strings.handle("GETRANGE", ["missing", "0", "5"], store)
    end

    test "GETRANGE with both negative indices" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      # -3 = index 2, -1 = index 4 -> "llo"
      assert "llo" = Strings.handle("GETRANGE", ["k", "-3", "-1"], store)
    end

    test "GETRANGE start negative beyond string start clamps to 0" do
      store = MockStore.make(%{"k" => {"Hi", 0}})
      assert "Hi" = Strings.handle("GETRANGE", ["k", "-100", "-1"], store)
    end

    test "GETRANGE with non-integer start returns error" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      assert {:error, _} = Strings.handle("GETRANGE", ["k", "abc", "5"], store)
    end

    test "GETRANGE with non-integer end returns error" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      assert {:error, _} = Strings.handle("GETRANGE", ["k", "0", "abc"], store)
    end

    test "GETRANGE wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("GETRANGE", [], store)
      assert {:error, _} = Strings.handle("GETRANGE", ["k"], store)
      assert {:error, _} = Strings.handle("GETRANGE", ["k", "0"], store)
    end

    test "GETRANGE single character" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      assert "H" = Strings.handle("GETRANGE", ["k", "0", "0"], store)
    end

    test "GETRANGE last character" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      assert "o" = Strings.handle("GETRANGE", ["k", "-1", "-1"], store)
    end
  end

  # ===========================================================================
  # SETRANGE
  # ===========================================================================

  describe "SETRANGE" do
    test "SETRANGE overwrites part of existing string" do
      store = MockStore.make(%{"k" => {"Hello World", 0}})
      assert 11 = Strings.handle("SETRANGE", ["k", "6", "Redis"], store)
      assert "Hello Redis" == store.get.("k")
    end

    test "SETRANGE extends string if overwrite goes past end" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      assert 10 = Strings.handle("SETRANGE", ["k", "5", " Wor!"], store)
      assert "Hello Wor!" == store.get.("k")
    end

    test "SETRANGE on non-existent key zero-pads" do
      store = MockStore.make()
      assert 8 = Strings.handle("SETRANGE", ["k", "5", "abc"], store)
      value = store.get.("k")
      assert byte_size(value) == 8
      assert binary_part(value, 0, 5) == <<0, 0, 0, 0, 0>>
      assert binary_part(value, 5, 3) == "abc"
    end

    test "SETRANGE at offset 0 replaces beginning" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      assert 5 = Strings.handle("SETRANGE", ["k", "0", "Jello"], store)
      assert "Jello" == store.get.("k")
    end

    test "SETRANGE with empty value at offset beyond string pads" do
      store = MockStore.make(%{"k" => {"Hi", 0}})
      # Empty value: just pad up to offset if needed
      assert 5 = Strings.handle("SETRANGE", ["k", "5", ""], store)
      value = store.get.("k")
      assert byte_size(value) == 5
    end

    test "SETRANGE with empty value at offset within string is no-op" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      assert 5 = Strings.handle("SETRANGE", ["k", "3", ""], store)
      assert "Hello" == store.get.("k")
    end

    test "SETRANGE returns new total length" do
      store = MockStore.make(%{"k" => {"abc", 0}})
      assert 3 = Strings.handle("SETRANGE", ["k", "0", "x"], store)
      assert "xbc" == store.get.("k")
    end

    test "SETRANGE preserves TTL" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"Hello", future}})
      Strings.handle("SETRANGE", ["k", "0", "J"], store)
      {_, exp} = store.get_meta.("k")
      assert exp == future
    end

    test "SETRANGE with negative offset returns error" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      assert {:error, _} = Strings.handle("SETRANGE", ["k", "-1", "x"], store)
    end

    test "SETRANGE with non-integer offset returns error" do
      store = MockStore.make(%{"k" => {"Hello", 0}})
      assert {:error, _} = Strings.handle("SETRANGE", ["k", "abc", "x"], store)
    end

    test "SETRANGE wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("SETRANGE", [], store)
      assert {:error, _} = Strings.handle("SETRANGE", ["k"], store)
      assert {:error, _} = Strings.handle("SETRANGE", ["k", "0"], store)
    end
  end

  # ===========================================================================
  # MSETNX
  # ===========================================================================

  describe "MSETNX" do
    test "MSETNX sets all keys when none exist and returns 1" do
      store = MockStore.make()
      assert 1 = Strings.handle("MSETNX", ["a", "1", "b", "2", "c", "3"], store)
      assert "1" == store.get.("a")
      assert "2" == store.get.("b")
      assert "3" == store.get.("c")
    end

    test "MSETNX sets nothing and returns 0 when any key exists" do
      store = MockStore.make(%{"b" => {"existing", 0}})
      assert 0 = Strings.handle("MSETNX", ["a", "1", "b", "2", "c", "3"], store)
      # a and c should NOT be set
      assert nil == store.get.("a")
      assert "existing" == store.get.("b")
      assert nil == store.get.("c")
    end

    test "MSETNX sets nothing when all keys exist" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert 0 = Strings.handle("MSETNX", ["a", "new1", "b", "new2"], store)
      assert "1" == store.get.("a")
      assert "2" == store.get.("b")
    end

    test "MSETNX with single pair works" do
      store = MockStore.make()
      assert 1 = Strings.handle("MSETNX", ["k", "v"], store)
      assert "v" == store.get.("k")
    end

    test "MSETNX with single pair when key exists returns 0" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert 0 = Strings.handle("MSETNX", ["k", "new"], store)
    end

    test "MSETNX with odd number of args returns error" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("MSETNX", ["a", "1", "b"], store)
    end

    test "MSETNX with no args returns error" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("MSETNX", [], store)
    end

    test "MSETNX partial existence: first key exists, rest don't" do
      store = MockStore.make(%{"a" => {"exists", 0}})
      assert 0 = Strings.handle("MSETNX", ["a", "1", "b", "2"], store)
      assert nil == store.get.("b")
    end

    test "MSETNX partial existence: last key exists, rest don't" do
      store = MockStore.make(%{"c" => {"exists", 0}})
      assert 0 = Strings.handle("MSETNX", ["a", "1", "b", "2", "c", "3"], store)
      assert nil == store.get.("a")
      assert nil == store.get.("b")
    end

    test "MSETNX with duplicate keys in args when none exist sets all" do
      store = MockStore.make()
      # Redis allows duplicate keys in MSETNX args
      assert 1 = Strings.handle("MSETNX", ["k", "first", "k", "second"], store)
      # The last value wins
      assert "second" == store.get.("k")
    end
  end
end
