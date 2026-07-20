defmodule Ferricstore.Flow.Query.TupleCodecTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Ferricstore.Flow.Query.{Field, TupleCodec}

  test "uses stable golden encodings for scalar values" do
    assert TupleCodec.encode_component(nil, :asc) == <<0xFE>>
    assert TupleCodec.encode_component(Field.missing(), :asc) == <<0xFF>>
    assert TupleCodec.encode_component(nil, :desc) == <<0xFE>>
    assert TupleCodec.encode_component(Field.missing(), :desc) == <<0xFF>>
    assert TupleCodec.encode_component(false, :asc) == <<0x20>>
    assert TupleCodec.encode_component(true, :asc) == <<0x21>>

    assert TupleCodec.encode_component(-1, :asc) ==
             <<0x30, 0x7FFF_FFFF_FFFF_FFFF::unsigned-big-64>>

    assert TupleCodec.encode_component(0, :asc) ==
             <<0x30, 0x8000_0000_0000_0000::unsigned-big-64>>

    assert TupleCodec.encode_component("a\0b", :asc) ==
             <<0x40, ?a, 0, 0xFF, ?b, 0, 0>>
  end

  test "round trips supported values in both directions" do
    values = [Field.missing(), nil, false, true, -9, 0, 42, -1.25, 0.0, 8.5, "", "a\0b"]

    for direction <- [:asc, :desc], value <- values do
      encoded = TupleCodec.encode_component(value, direction)
      assert {:ok, decoded, "tail"} = TupleCodec.decode_component(encoded <> "tail", direction)
      assert same_value?(decoded, value)
    end
  end

  test "canonicalizes signed floating zero to the equality semantics" do
    negative_zero = -0.0

    for direction <- [:asc, :desc] do
      assert TupleCodec.encode_component(negative_zero, direction) ==
               TupleCodec.encode_component(0.0, direction)

      assert TupleCodec.compare_values(negative_zero, 0.0, direction) == :eq
    end
  end

  test "ascending bytes preserve the declared total order" do
    values = [
      Field.missing(),
      nil,
      false,
      true,
      -100,
      -1,
      0,
      1,
      100,
      -3.5,
      0.0,
      8.25,
      "",
      "a",
      "aa",
      "b"
    ]

    for left <- values, right <- values do
      expected = TupleCodec.compare_values(left, right)

      actual =
        compare_binary(
          TupleCodec.encode_component(left, :asc),
          TupleCodec.encode_component(right, :asc)
        )

      assert actual == expected
    end
  end

  test "descending bytes reverse value ordering without changing component boundaries" do
    values = [Field.missing(), nil, false, true, -2, 0, 3, -1.5, 2.5, "", "a", "a\0b", "z"]

    for left <- values, right <- values do
      expected = TupleCodec.compare_values(left, right, :desc)

      actual =
        compare_binary(
          TupleCodec.encode_component(left, :desc),
          TupleCodec.encode_component(right, :desc)
        )

      assert actual == expected
    end
  end

  test "both directions place concrete values before null and missing" do
    concrete = [false, true, -2, 0, 3, -1.5, 2.5, "", "a", "z"]

    for direction <- [:asc, :desc], value <- concrete do
      assert TupleCodec.encode_component(value, direction) <
               TupleCodec.encode_component(nil, direction)

      assert TupleCodec.encode_component(nil, direction) <
               TupleCodec.encode_component(Field.missing(), direction)
    end
  end

  test "composite prefixes are collision-free across embedded zero bytes" do
    fields = [{:partition_key, :asc}, {{:attribute, "region"}, :asc}]

    assert {:ok, first} = TupleCodec.encode(["tenant\0a", "eu"], fields)
    assert {:ok, second} = TupleCodec.encode(["tenant", "a\0eu"], fields)
    refute first == second

    assert {:ok, prefix} = TupleCodec.encode_prefix(["tenant\0a"], fields)
    assert String.starts_with?(first, prefix)
  end

  test "rejects unsupported or out-of-range values instead of producing unstable keys" do
    assert {:error, :integer_out_of_range} = TupleCodec.encode([1 <<< 63], [{:priority, :asc}])
    assert {:error, :unsupported_index_value} = TupleCodec.encode([%{}], [{:priority, :asc}])

    assert {:error, :invalid_tuple_arity} =
             TupleCodec.encode(["tenant"], [{:partition_key, :asc}, {:state, :asc}])
  end

  defp same_value?(left, right) when is_float(left) and is_float(right), do: left == right
  defp same_value?(left, right), do: left == right

  defp compare_binary(left, right) when left < right, do: :lt
  defp compare_binary(left, right) when left > right, do: :gt
  defp compare_binary(_left, _right), do: :eq
end
