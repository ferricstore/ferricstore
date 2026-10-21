defmodule Ferricstore.Commands.NativeRangeArgumentsTest do
  @moduledoc """
  BITCOUNT, BITPOS and ZRANGE range arguments through the native command parser,
  the path the TCP and HTTP gateways use.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  @integer_error {:error, "ERR value is not an integer or out of range"}
  @syntax_error {:error, "ERR syntax error"}

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "native-range:#{System.unique_integer([:positive])}"
    }
  end

  test "BITCOUNT counts a byte or bit range and rejects malformed ranges", %{ctx: ctx, key: key} do
    # "a" 0x61, "b" 0x62 and "c" 0x63 have 3, 3 and 4 set bits.
    assert :ok = FerricStore.set(key, "abc")

    assert 10 == Dispatcher.dispatch("BITCOUNT", [key], ctx)
    assert 7 == Dispatcher.dispatch("BITCOUNT", [key, "1", "-1"], ctx)
    assert 3 == Dispatcher.dispatch("BITCOUNT", [key, "0", "7", "BIT"], ctx)
    assert @syntax_error == Dispatcher.dispatch("BITCOUNT", [key, "1"], ctx)
    assert @syntax_error == Dispatcher.dispatch("BITCOUNT", [key, "0", "-1", "FOO"], ctx)
    assert @integer_error == Dispatcher.dispatch("BITCOUNT", [key, "x", "-1"], ctx)
  end

  test "BITPOS searches a byte or bit range and rejects malformed ranges", %{ctx: ctx, key: key} do
    assert :ok = FerricStore.set(key, "abc")

    assert 1 == Dispatcher.dispatch("BITPOS", [key, "1"], ctx)
    assert 9 == Dispatcher.dispatch("BITPOS", [key, "1", "1"], ctx)
    assert 9 == Dispatcher.dispatch("BITPOS", [key, "1", "1", "-1"], ctx)
    assert 2 == Dispatcher.dispatch("BITPOS", [key, "1", "2", "-1", "BIT"], ctx)
    assert @integer_error == Dispatcher.dispatch("BITPOS", [key, "1", "x"], ctx)
    assert @integer_error == Dispatcher.dispatch("BITPOS", [key, "1", "0", "y"], ctx)
  end

  test "ZRANGE and ZREVRANGE reject a non-integer start or stop", %{ctx: ctx, key: key} do
    assert {:ok, 2} = FerricStore.zadd(key, [{1.0, "m"}, {2.0, "n"}])

    assert ["m", "n"] == Dispatcher.dispatch("ZRANGE", [key, "0", "-1"], ctx)
    assert @integer_error == Dispatcher.dispatch("ZRANGE", [key, "x", "-1"], ctx)
    assert @integer_error == Dispatcher.dispatch("ZRANGE", [key, "0", "y"], ctx)
    assert @integer_error == Dispatcher.dispatch("ZREVRANGE", [key, "x", "1"], ctx)
    assert @integer_error == Dispatcher.dispatch("ZREVRANGE", [key, "0", "y", "WITHSCORES"], ctx)
  end
end
