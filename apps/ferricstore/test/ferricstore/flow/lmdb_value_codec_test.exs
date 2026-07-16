defmodule Ferricstore.Flow.LMDBValueCodecTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDB.ValueLocator

  test "value wrappers accept only zero or positive integer expirations" do
    assert {:ok, "value"} = LMDB.decode_value(LMDB.encode_value("value", 0), 10)
    assert {:ok, "value"} = LMDB.decode_value(LMDB.encode_value("value", 20), 10)
    assert :expired = LMDB.decode_value(LMDB.encode_value("value", 20), 20)

    for invalid_expiration <- [-1, nil, "20", 1.5] do
      malformed = :erlang.term_to_binary({invalid_expiration, "value"})
      assert :error = LMDB.decode_value(malformed, 10)
    end

    assert_raise ArgumentError, fn -> LMDB.encode_value(%{record: true}, 0) end

    assert :error =
             Ferricstore.TermCodec.encode({0, %{record: true}})
             |> LMDB.decode_value(10)

    assert :error = LMDB.decode_value(LMDB.encode_value("value", 0), -1)
  end

  test "value locators reject malformed tagged expirations" do
    assert {:ok, {7, 11, 13}} =
             LMDB.decode_value_locator(LMDB.encode_value_locator(0, 7, 11, 13), 10)

    assert {:ok, {7, 11, 13}} =
             LMDB.decode_value_locator(LMDB.encode_value_locator(20, 7, 11, 13), 10)

    assert :expired = LMDB.decode_value_locator(LMDB.encode_value_locator(20, 7, 11, 13), 20)

    for invalid_expiration <- [-1, nil, "20", 1.5] do
      malformed =
        :erlang.term_to_binary({:flow_value_locator, 1, invalid_expiration, 7, 11, 13})

      assert :error = LMDB.decode_value_locator(malformed, 10)
    end

    assert :not_locator = LMDB.decode_value_locator(:erlang.term_to_binary({:other, 1}), 10)
  end

  test "segment-pin locator writer rejects metadata its reader cannot decode" do
    for {expire_at_ms, offset, value_size} <- [
          {-1, 11, 13},
          {0, -1, 13},
          {0, 11, -1},
          {"0", 11, 13}
        ] do
      assert_raise ArgumentError, fn ->
        ValueLocator.encode(expire_at_ms, {:waraft_segment, 1}, offset, value_size)
      end
    end
  end

  test "value and locator decoders reject non-canonical external terms" do
    value = LMDB.encode_value(String.duplicate("value", 2_048), 0)
    compressed = value |> :erlang.binary_to_term([:safe]) |> :erlang.term_to_binary(compressed: 9)
    assert <<131, 80, _rest::binary>> = compressed

    assert :error = LMDB.decode_value(compressed, 10)
    assert :error = LMDB.decode_value(value <> <<0>>, 10)

    locator = LMDB.encode_value_locator(0, 7, 11, 13)
    assert :not_locator = LMDB.decode_value_locator(locator <> <<0>>, 10)
  end

  test "value locators enforce supported file ids and unsigned 64-bit metadata" do
    too_large = 18_446_744_073_709_551_616

    for {expire_at_ms, file_id, offset, value_size} <- [
          {too_large, 7, 11, 13},
          {0, 7, too_large, 13},
          {0, 7, 11, too_large},
          {0, -1, 11, 13},
          {0, {:waraft_segment, 0}, 11, 13},
          {0, {:unknown, 1}, 11, 13},
          {0, "segment", 11, 13}
        ] do
      assert_raise ArgumentError, fn ->
        LMDB.encode_value_locator(expire_at_ms, file_id, offset, value_size)
      end
    end

    for malformed <- [
          {:flow_value_locator, 1, too_large, 7, 11, 13},
          {:flow_value_locator, 1, 0, {:unknown, 1}, 11, 13},
          {:flow_value_locator, 1, 0, 7, too_large, 13}
        ] do
      assert :error =
               malformed
               |> Ferricstore.TermCodec.encode()
               |> LMDB.decode_value_locator(10)
    end
  end
end
