defmodule Ferricstore.NativeValueCodecTest do
  use ExUnit.Case, async: true

  alias Ferricstore.NativeValueCodec

  test "reports the exact encoded size without materializing a second value" do
    value = %{
      status: :ok,
      values: [nil, true, false, 42, -7, 3.5, "payload", {:tuple, 2}]
    }

    encoded = NativeValueCodec.encode(value)
    size = NativeValueCodec.encoded_size(value)

    assert size == byte_size(encoded)
    assert NativeValueCodec.fits?(value, size)
    refute NativeValueCodec.fits?(value, size - 1)
  end

  test "uses the signed 64-bit integer contract enforced by the native wire" do
    assert byte_size(NativeValueCodec.encode(-0x8000_0000_0000_0000)) == 9
    assert byte_size(NativeValueCodec.encode(0x7FFF_FFFF_FFFF_FFFF)) == 9

    assert_raise ArgumentError, fn -> NativeValueCodec.encode(-0x8000_0000_0000_0001) end
    assert_raise ArgumentError, fn -> NativeValueCodec.encode(0x8000_0000_0000_0000) end
  end
end
