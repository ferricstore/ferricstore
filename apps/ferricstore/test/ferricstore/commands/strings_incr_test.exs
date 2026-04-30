defmodule Ferricstore.Commands.StringsIncrTest do
  @moduledoc """
  Tests for INCR, DECR, INCRBY, DECRBY, and INCRBYFLOAT commands.

  Covers: non-existent keys, non-integer values, overflow boundaries,
  negative results, and float precision.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Strings
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # INCR
  # ===========================================================================

  describe "INCR" do
    test "INCR on non-existent key returns 1" do
      store = MockStore.make()
      assert {:ok, 1} = Strings.handle("INCR", ["counter"], store)
    end

    test "INCR on existing integer value increments by 1" do
      store = MockStore.make(%{"counter" => {"10", 0}})
      assert {:ok, 11} = Strings.handle("INCR", ["counter"], store)
    end

    test "INCR on non-integer value returns error" do
      store = MockStore.make(%{"key" => {"hello", 0}})
      assert {:error, msg} = Strings.handle("INCR", ["key"], store)
      assert msg =~ "not an integer"
    end

    test "INCR on float value returns error" do
      store = MockStore.make(%{"key" => {"3.14", 0}})
      assert {:error, msg} = Strings.handle("INCR", ["key"], store)
      assert msg =~ "not an integer"
    end

    test "INCR on empty string value returns error" do
      store = MockStore.make(%{"key" => {"", 0}})
      assert {:error, _} = Strings.handle("INCR", ["key"], store)
    end

    test "INCR stores result as string" do
      store = MockStore.make()
      assert {:ok, 1} = Strings.handle("INCR", ["k"], store)
      assert "1" == store.get.("k")
    end

    test "INCR multiple times accumulates" do
      store = MockStore.make()
      assert {:ok, 1} = Strings.handle("INCR", ["k"], store)
      assert {:ok, 2} = Strings.handle("INCR", ["k"], store)
      assert {:ok, 3} = Strings.handle("INCR", ["k"], store)
      assert "3" == store.get.("k")
    end

    test "INCR on negative integer works" do
      store = MockStore.make(%{"k" => {"-5", 0}})
      assert {:ok, -4} = Strings.handle("INCR", ["k"], store)
    end

    test "INCR on zero works" do
      store = MockStore.make(%{"k" => {"0", 0}})
      assert {:ok, 1} = Strings.handle("INCR", ["k"], store)
    end

    test "INCR near max int64 works" do
      max_minus_one = Integer.to_string(9_223_372_036_854_775_806)
      store = MockStore.make(%{"k" => {max_minus_one, 0}})
      assert {:ok, 9_223_372_036_854_775_807} = Strings.handle("INCR", ["k"], store)
    end

    test "INCR preserves TTL" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"5", future}})
      assert {:ok, 6} = Strings.handle("INCR", ["k"], store)
      {_, exp} = store.get_meta.("k")
      assert exp == future
    end

    test "INCR wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("INCR", [], store)
      assert msg =~ "wrong number of arguments"
    end

    test "INCR with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("INCR", ["k", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "INCR on value with leading/trailing spaces returns error" do
      store = MockStore.make(%{"k" => {" 10 ", 0}})
      assert {:error, _} = Strings.handle("INCR", ["k"], store)
    end
  end

  # ===========================================================================
  # DECR
  # ===========================================================================

  describe "DECR" do
    test "DECR on non-existent key returns -1" do
      store = MockStore.make()
      assert {:ok, -1} = Strings.handle("DECR", ["counter"], store)
    end

    test "DECR on existing integer value decrements by 1" do
      store = MockStore.make(%{"counter" => {"10", 0}})
      assert {:ok, 9} = Strings.handle("DECR", ["counter"], store)
    end

    test "DECR on non-integer value returns error" do
      store = MockStore.make(%{"key" => {"hello", 0}})
      assert {:error, msg} = Strings.handle("DECR", ["key"], store)
      assert msg =~ "not an integer"
    end

    test "DECR to negative works" do
      store = MockStore.make(%{"k" => {"0", 0}})
      assert {:ok, -1} = Strings.handle("DECR", ["k"], store)
      assert {:ok, -2} = Strings.handle("DECR", ["k"], store)
    end

    test "DECR on already negative value goes more negative" do
      store = MockStore.make(%{"k" => {"-10", 0}})
      assert {:ok, -11} = Strings.handle("DECR", ["k"], store)
    end

    test "DECR wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("DECR", [], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # INCRBY
  # ===========================================================================

  describe "INCRBY" do
    test "INCRBY on non-existent key sets to delta" do
      store = MockStore.make()
      assert {:ok, 5} = Strings.handle("INCRBY", ["k", "5"], store)
    end

    test "INCRBY on existing integer adds delta" do
      store = MockStore.make(%{"k" => {"10", 0}})
      assert {:ok, 15} = Strings.handle("INCRBY", ["k", "5"], store)
    end

    test "INCRBY with negative delta decrements" do
      store = MockStore.make(%{"k" => {"10", 0}})
      assert {:ok, 7} = Strings.handle("INCRBY", ["k", "-3"], store)
    end

    test "INCRBY with zero delta returns same value" do
      store = MockStore.make(%{"k" => {"42", 0}})
      assert {:ok, 42} = Strings.handle("INCRBY", ["k", "0"], store)
    end

    test "INCRBY on non-integer value returns error" do
      store = MockStore.make(%{"k" => {"hello", 0}})
      assert {:error, _} = Strings.handle("INCRBY", ["k", "5"], store)
    end

    test "INCRBY with non-integer delta returns error" do
      store = MockStore.make(%{"k" => {"10", 0}})
      assert {:error, msg} = Strings.handle("INCRBY", ["k", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "INCRBY with float delta returns error" do
      store = MockStore.make(%{"k" => {"10", 0}})
      assert {:error, _} = Strings.handle("INCRBY", ["k", "1.5"], store)
    end

    test "INCRBY wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("INCRBY", ["k"], store)
      assert {:error, _} = Strings.handle("INCRBY", [], store)
    end

    test "INCRBY with large delta" do
      store = MockStore.make()
      assert {:ok, 1_000_000} = Strings.handle("INCRBY", ["k", "1000000"], store)
    end

    test "INCRBY errors when result exceeds max int64 and leaves value unchanged" do
      store = MockStore.make(%{"k" => {"9223372036854775806", 0}})

      assert {:error, "ERR increment or decrement would overflow"} =
               Strings.handle("INCRBY", ["k", "2"], store)

      assert "9223372036854775806" == store.get.("k")
    end
  end

  # ===========================================================================
  # DECRBY
  # ===========================================================================

  describe "DECRBY" do
    test "DECRBY on non-existent key sets to negative delta" do
      store = MockStore.make()
      assert {:ok, -5} = Strings.handle("DECRBY", ["k", "5"], store)
    end

    test "DECRBY on existing integer subtracts delta" do
      store = MockStore.make(%{"k" => {"10", 0}})
      assert {:ok, 7} = Strings.handle("DECRBY", ["k", "3"], store)
    end

    test "DECRBY with negative delta increments" do
      store = MockStore.make(%{"k" => {"10", 0}})
      assert {:ok, 13} = Strings.handle("DECRBY", ["k", "-3"], store)
    end

    test "DECRBY on non-integer value returns error" do
      store = MockStore.make(%{"k" => {"hello", 0}})
      assert {:error, _} = Strings.handle("DECRBY", ["k", "1"], store)
    end

    test "DECRBY with non-integer delta returns error" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("DECRBY", ["k", "abc"], store)
    end

    test "DECRBY wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("DECRBY", [], store)
      assert {:error, _} = Strings.handle("DECRBY", ["k"], store)
    end

    test "DECRBY errors when result is below min int64 and leaves value unchanged" do
      store = MockStore.make(%{"k" => {"-9223372036854775807", 0}})

      assert {:error, "ERR increment or decrement would overflow"} =
               Strings.handle("DECRBY", ["k", "2"], store)

      assert "-9223372036854775807" == store.get.("k")
    end

    test "DECRBY errors when negating min int64 delta would overflow" do
      store = MockStore.make(%{"k" => {"0", 0}})

      assert {:error, "ERR increment or decrement would overflow"} =
               Strings.handle("DECRBY", ["k", "-9223372036854775808"], store)

      assert "0" == store.get.("k")
    end
  end

  # ===========================================================================
  # INCRBYFLOAT
  # ===========================================================================

  describe "INCRBYFLOAT" do
    test "INCRBYFLOAT on non-existent key sets to delta" do
      store = MockStore.make()
      result = Strings.handle("INCRBYFLOAT", ["k", "2.5"], store)
      assert is_binary(result)
      assert String.to_float(result) == 2.5
    end

    test "INCRBYFLOAT on existing integer value adds float" do
      store = MockStore.make(%{"k" => {"10", 0}})
      result = Strings.handle("INCRBYFLOAT", ["k", "1.5"], store)
      assert is_binary(result)
      assert String.to_float(result) == 11.5
    end

    test "INCRBYFLOAT on existing float value adds float" do
      store = MockStore.make(%{"k" => {"10.5", 0}})
      result = Strings.handle("INCRBYFLOAT", ["k", "0.5"], store)
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert val == 11.0
    end

    test "INCRBYFLOAT with integer delta (string) works" do
      store = MockStore.make(%{"k" => {"10.5", 0}})
      result = Strings.handle("INCRBYFLOAT", ["k", "2"], store)
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert val == 12.5
    end

    test "INCRBYFLOAT with negative delta works" do
      store = MockStore.make(%{"k" => {"10.0", 0}})
      result = Strings.handle("INCRBYFLOAT", ["k", "-2.5"], store)
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert val == 7.5
    end

    test "INCRBYFLOAT on non-numeric value returns error" do
      store = MockStore.make(%{"k" => {"hello", 0}})
      assert {:error, msg} = Strings.handle("INCRBYFLOAT", ["k", "1.0"], store)
      assert msg =~ "not a valid float"
    end

    test "INCRBYFLOAT with non-numeric delta returns error" do
      store = MockStore.make(%{"k" => {"10", 0}})
      assert {:error, _} = Strings.handle("INCRBYFLOAT", ["k", "abc"], store)
    end

    test "INCRBYFLOAT returns bulk string (not integer)" do
      store = MockStore.make(%{"k" => {"10", 0}})
      result = Strings.handle("INCRBYFLOAT", ["k", "0.0"], store)
      # Result should be a string, not an integer
      assert is_binary(result)
    end

    test "INCRBYFLOAT wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("INCRBYFLOAT", [], store)
      assert {:error, _} = Strings.handle("INCRBYFLOAT", ["k"], store)
    end

    test "INCRBYFLOAT preserves TTL" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"5.0", future}})
      _result = Strings.handle("INCRBYFLOAT", ["k", "1.0"], store)
      {_, exp} = store.get_meta.("k")
      assert exp == future
    end

    test "INCRBYFLOAT precision: small increments" do
      store = MockStore.make(%{"k" => {"0", 0}})
      result = Strings.handle("INCRBYFLOAT", ["k", "0.1"], store)
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert_in_delta val, 0.1, 1.0e-15
    end
  end
end
