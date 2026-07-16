defmodule Ferricstore.Commands.ExpiryTimestampBoundsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Expiry, Hash, Strings}
  alias Ferricstore.Commands.Hash.Helpers
  alias Ferricstore.Commands.Strings.SetOptions
  alias Ferricstore.Test.MockStore

  @max_int64 9_223_372_036_854_775_807
  @max_relative_ms @max_int64 - 281_474_976_710_655
  @too_large_relative_ms @max_relative_ms + 1

  @set_defaults %{
    expire_at_ms: 0,
    nx: false,
    xx: false,
    get: false,
    keepttl: false,
    has_expiry: false
  }

  test "relative expiry commands reject timestamps outside the persistence range" do
    too_large = Integer.to_string(@too_large_relative_ms)
    store = MockStore.make(%{"expiry" => {"v", 0}, "getex" => {"v", 0}})

    assert integer_range_error() == Expiry.handle("PEXPIRE", ["expiry", too_large], store)

    assert integer_range_error() ==
             Expiry.handle_ast({:pexpire, "expiry", @too_large_relative_ms}, store)

    assert integer_range_error() ==
             Strings.handle("PSETEX", ["psetex", too_large, "value"], store)

    assert integer_range_error() ==
             Strings.handle_ast({:psetex, "psetex-ast", @too_large_relative_ms, "value"}, store)

    assert integer_range_error() == Strings.handle("GETEX", ["getex", "PX", too_large], store)

    assert integer_range_error() ==
             Strings.handle_ast({:getex, "getex", {:px, @too_large_relative_ms}}, store)

    assert integer_range_error() == SetOptions.parse(["PX", too_large], @set_defaults)

    assert integer_range_error() ==
             SetOptions.from_ast([px: @too_large_relative_ms], @set_defaults)

    assert integer_range_error() == Helpers.parse_expiry_mode("PX", too_large)

    assert integer_range_error() ==
             Hash.handle_ast({:hpexpire, "hash", @too_large_relative_ms, ["field"]}, store)

    assert {"v", 0} == store.get_meta.("expiry")
    assert {"v", 0} == store.get_meta.("getex")
    assert nil == store.get.("psetex")
    assert nil == store.get.("psetex-ast")
  end

  test "absolute expiry commands reject values above signed 64-bit milliseconds" do
    too_large = Integer.to_string(@max_int64 + 1)
    store = MockStore.make(%{"expiry" => {"v", 0}, "getex" => {"v", 0}})

    assert integer_range_error() == Expiry.handle("PEXPIREAT", ["expiry", too_large], store)
    assert integer_range_error() == Strings.handle("GETEX", ["getex", "PXAT", too_large], store)
    assert integer_range_error() == SetOptions.parse(["PXAT", too_large], @set_defaults)
    assert integer_range_error() == Helpers.parse_expiry_mode("PXAT", too_large)

    assert {"v", 0} == store.get_meta.("expiry")
    assert {"v", 0} == store.get_meta.("getex")
  end

  defp integer_range_error, do: {:error, "ERR value is not an integer or out of range"}
end
