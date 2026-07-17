defmodule Ferricstore.Store.ValueCodecTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.ValueCodec

  describe "rate-limit state" do
    test "round-trips the fixed-width binary encoding" do
      encoded = ValueCodec.encode_ratelimit(7, 1_234, 5)

      assert byte_size(encoded) == 24
      assert ValueCodec.decode_ratelimit(encoded, 99) == {7, 1_234, 5}
    end

    test "rejects string-encoded state instead of accepting an obsolete format" do
      assert ValueCodec.decode_ratelimit("7:1234:5", 99) == {0, 99, 0}
    end
  end

  describe "float parsing" do
    test "rejects integers outside the finite float range without raising" do
      assert ValueCodec.parse_float(String.duplicate("9", 400)) == :error
      assert {:ok, value} = ValueCodec.parse_float(String.duplicate("9", 300))
      assert is_float(value)
    end
  end

  describe "float formatting" do
    test "trims only the mantissa in scientific notation" do
      assert ValueCodec.format_float(1.0e20) == "1e20"
      assert ValueCodec.format_float(1.23e10) == "1.23e10"
      assert ValueCodec.format_float(1.0e-10) == "1e-10"

      for value <- [1.0e20, 1.23e10, 1.0e-10] do
        assert {:ok, ^value} = value |> ValueCodec.format_float() |> ValueCodec.parse_float()
      end
    end
  end
end
