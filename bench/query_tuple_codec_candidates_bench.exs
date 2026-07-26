# Compares accepted tuple codec fast paths with their previous implementations.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryTupleCodecCandidates do
  @moduledoc false

  import Bitwise

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Flow.Query.{Field, TupleCodec}

  @binary_tag 0x40

  def run do
    encode_inputs = encode_inputs()
    compare_inputs = compare_inputs()

    Enum.each(encode_inputs, fn {_name, values} ->
      Enum.each(values, fn {value, direction} ->
        expected = legacy_encode_binary(value, direction)
        ^expected = TupleCodec.encode_component(value, direction)
      end)
    end)

    Enum.each(compare_inputs, fn {_name, pairs} ->
      Enum.each(pairs, fn {left, right, direction} ->
        expected = legacy_compare(left, right, direction)
        ^expected = TupleCodec.compare_values(left, right, direction)
      end)
    end)

    Benchee.run(
      %{
        "legacy per-byte binary encode" => fn values ->
          Enum.map(values, fn {value, direction} -> legacy_encode_binary(value, direction) end)
        end,
        "production thresholded binary encode" => fn values ->
          Enum.map(values, fn {value, direction} ->
            TupleCodec.encode_component(value, direction)
          end)
        end
      },
      [inputs: Map.new(encode_inputs)] ++
        QueryPerformance.benchee_options("query-tuple-codec-encode-candidates")
    )

    Benchee.run(
      %{
        "legacy encode then compare" => fn pairs ->
          Enum.map(pairs, fn {left, right, direction} ->
            legacy_compare(left, right, direction)
          end)
        end,
        "production direct total-order compare" => fn pairs ->
          Enum.map(pairs, fn {left, right, direction} ->
            TupleCodec.compare_values(left, right, direction)
          end)
        end
      },
      [inputs: Map.new(compare_inputs)] ++
        QueryPerformance.benchee_options("query-tuple-codec-compare-candidates")
    )
  end

  defp encode_inputs do
    for bytes <- [8, 17, 32, 64, 512], nul? <- [false, true] do
      name = "1000 strings/#{bytes} bytes/#{if(nul?, do: "embedded-nul", else: "plain")}"

      values =
        Enum.map(1..1_000, fn number ->
          seed = :binary.copy(<<rem(number, 251) + 1>>, bytes)
          value = if nul?, do: put_nul(seed), else: seed
          {value, if(rem(number, 2) == 0, do: :asc, else: :desc)}
        end)

      {name, values}
    end
  end

  defp compare_inputs do
    [
      {"10000 integers", pairs(Enum.to_list(-100..100))},
      {"10000 floats", pairs(Enum.map(-100..100, &(&1 / 7)))},
      {"10000 binaries", pairs(Enum.map(1..201, &"value-#{&1}"))},
      {"10000 mixed types",
       pairs([false, true, -1, 0, 1, -0.0, 1.5, "", "a", nil, Field.missing()])}
    ]
  end

  defp pairs(values) do
    count = length(values)

    Enum.map(1..10_000, fn number ->
      left = Enum.at(values, rem(number * 17, count))
      right = Enum.at(values, rem(number * 31 + 7, count))
      {left, right, if(rem(number, 2) == 0, do: :asc, else: :desc)}
    end)
  end

  defp put_nul(value) do
    offset = div(byte_size(value), 2)

    binary_part(value, 0, offset) <>
      <<0>> <> binary_part(value, offset + 1, byte_size(value) - offset - 1)
  end

  defp legacy_encode_binary(value, direction) when is_binary(value) do
    escaped = for <<byte <- value>>, do: if(byte == 0, do: <<0, 0xFF>>, else: <<byte>>)
    escaped = IO.iodata_to_binary([<<@binary_tag>>, escaped, <<0, 0>>])

    maybe_invert(escaped, direction)
  end

  defp legacy_compare(left, right, direction) do
    left_encoded = TupleCodec.encode_component(left, direction)
    right_encoded = TupleCodec.encode_component(right, direction)

    cond do
      left_encoded < right_encoded -> :lt
      left_encoded > right_encoded -> :gt
      true -> :eq
    end
  end

  defp maybe_invert(encoded, :asc), do: encoded

  defp maybe_invert(encoded, :desc) do
    for <<byte <- encoded>>, into: <<>>, do: <<bxor(byte, 0xFF)>>
  end
end

Ferricstore.Bench.QueryTupleCodecCandidates.run()
