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

  test "uses distinct signed and unsigned 64-bit native integer tags" do
    assert byte_size(NativeValueCodec.encode(-0x8000_0000_0000_0000)) == 9
    assert byte_size(NativeValueCodec.encode(0x7FFF_FFFF_FFFF_FFFF)) == 9

    assert <<8, 0x8000_0000_0000_0000::unsigned-64>> =
             NativeValueCodec.encode(0x8000_0000_0000_0000)

    assert <<8, 0xFFFF_FFFF_FFFF_FFFF::unsigned-64>> =
             NativeValueCodec.encode(0xFFFF_FFFF_FFFF_FFFF)

    assert_raise ArgumentError, fn -> NativeValueCodec.encode(-0x8000_0000_0000_0001) end
    assert_raise ArgumentError, fn -> NativeValueCodec.encode(0x1_0000_0000_0000_0000) end
  end
end
