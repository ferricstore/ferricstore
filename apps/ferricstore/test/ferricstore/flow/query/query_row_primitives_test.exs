defmodule Ferricstore.Flow.Query.QueryRowPrimitivesTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.QueryRowPrimitives

  test "round trips canonical unsigned integers across every width" do
    for value <- [
          0,
          1,
          0x7F,
          0x80,
          0x3FFF,
          0x4000,
          0xFFFF_FFFF,
          0x7FFF_FFFF_FFFF_FFFF,
          0xFFFF_FFFF_FFFF_FFFF
        ] do
      assert {:ok, encoded} = QueryRowPrimitives.encode_u64(value)
      assert {:ok, ^value, "tail"} = QueryRowPrimitives.decode_u64(encoded <> "tail")
    end
  end

  test "rejects truncated, overlong, and overflowing unsigned integers" do
    for invalid <- [
          <<0x80>>,
          <<0x80, 0>>,
          <<0x81, 0>>,
          :binary.copy(<<0xFF>>, 10),
          <<0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 2>>
        ] do
      assert :error = QueryRowPrimitives.decode_u64(invalid)
    end
  end

  test "round trips every supported scalar category and binary lists" do
    values = [nil, false, true, -9_007_199_254_740_991, 0, 9_007_199_254_740_991, 1.5, "value"]

    for value <- values do
      assert {:ok, encoded, _semantic_bytes} = QueryRowPrimitives.encode(value, 256, false)

      assert {:ok, ^value, <<>>, _semantic_bytes} =
               QueryRowPrimitives.decode(encoded, 256, false)
    end

    list = ["finance", "urgent"]
    assert {:ok, encoded, 13} = QueryRowPrimitives.encode(list, 256, true)
    assert {:ok, ^list, <<>>, 13} = QueryRowPrimitives.decode(encoded, 256, true)
    assert :error = QueryRowPrimitives.decode(encoded, 256, false)
  end

  test "rejects non-finite floats and bounded-value violations" do
    assert :error =
             QueryRowPrimitives.decode(<<4, 0x7FF0_0000_0000_0000::unsigned-big-64>>, 256, false)

    assert :error = QueryRowPrimitives.encode(:binary.copy("x", 257), 256, false)
    assert :error = QueryRowPrimitives.encode([], 256, true)
    assert :error = QueryRowPrimitives.encode(["same", 1], 256, true)

    assert :error =
             QueryRowPrimitives.decode(<<5, 0x81, 0, "x">>, 256, false)
  end

  test "skips canonical values without materializing them" do
    for {value, allow_list?} <- [
          {nil, false},
          {false, false},
          {true, false},
          {-42, false},
          {1.5, false},
          {String.duplicate("x", 256), false},
          {["finance", "urgent"], true}
        ] do
      assert {:ok, encoded, semantic_bytes} =
               QueryRowPrimitives.encode(value, 256, allow_list?)

      assert {:ok, "tail", ^semantic_bytes} =
               QueryRowPrimitives.skip(encoded <> "tail", 256, allow_list?)
    end
  end

  test "skip rejects malformed and noncanonical values even when their result is unused" do
    assert :error = QueryRowPrimitives.skip(<<5, 0x81, 0, "x">>, 256, false)
    assert :error = QueryRowPrimitives.skip(<<6, 2, 4, "same", 4, "same">>, 256, true)
    assert :error = QueryRowPrimitives.skip(<<6, 1, 1, "x">>, 256, false)

    assert :error =
             QueryRowPrimitives.skip(<<4, 0x7FF0_0000_0000_0000::unsigned-big-64>>, 256, false)
  end
end
