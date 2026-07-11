defmodule Ferricstore.Flow.LMDBValueCodecTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB

  test "value wrappers accept only zero or positive integer expirations" do
    assert {:ok, "value"} = LMDB.decode_value(LMDB.encode_value("value", 0), 10)
    assert {:ok, "value"} = LMDB.decode_value(LMDB.encode_value("value", 20), 10)
    assert :expired = LMDB.decode_value(LMDB.encode_value("value", 20), 20)

    for invalid_expiration <- [-1, nil, "20", 1.5] do
      malformed = :erlang.term_to_binary({invalid_expiration, "value"})
      assert :error = LMDB.decode_value(malformed, 10)
    end
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
end
