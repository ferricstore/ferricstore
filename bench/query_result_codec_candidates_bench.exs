Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryResultCodecCandidates do
  @moduledoc false

  import Bitwise

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Flow.Query.ResultCodec
  alias Ferricstore.NativeValueCodec

  @fields ResultCodec.record_fields()
  @atom_indexes @fields |> Enum.with_index() |> Map.new()
  @string_indexes Map.new(@atom_indexes, fn {field, index} -> {Atom.to_string(field), index} end)

  def run do
    inputs = %{
      "1 sparse row" => records(1, 3),
      "100 sparse rows" => records(100, 3),
      "100 medium rows" => records(100, 9),
      "100 full rows" => records(100, 18),
      "1000 sparse rows" => records(1_000, 3)
    }

    Enum.each(inputs, fn {_name, records} -> preflight!(records) end)

    Benchee.run(
      %{
        "current collect plus sort" => fn records ->
          encode_records(records, &current_record/1)
        end,
        "candidate fixed schema scan" => fn records ->
          encode_records(records, &schema_scan_record/1)
        end,
        "candidate insertion ordering" => fn records ->
          encode_records(records, &insertion_record/1)
        end,
        "candidate size-aware hybrid" => fn records ->
          encode_records(records, &hybrid_record/1)
        end
      },
      [inputs: inputs] ++ QueryPerformance.benchee_options("query-result-codec-candidates")
    )
  end

  defp preflight!(records) do
    expected = encode_records(records, &current_record/1)
    ^expected = encode_records(records, &schema_scan_record/1)
    ^expected = encode_records(records, &insertion_record/1)
    ^expected = encode_records(records, &hybrid_record/1)
  end

  defp encode_records(records, encoder) do
    records
    |> Enum.map(encoder)
    |> IO.iodata_to_binary()
  end

  defp current_record(record) do
    {bitmap, indexed} =
      Enum.reduce(record, {0, []}, fn {field, value}, {bitmap, values} ->
        index = field_index!(field)
        {bitmap ||| 1 <<< index, [{index, NativeValueCodec.encode(value)} | values]}
      end)

    values = indexed |> Enum.sort() |> Enum.map(&elem(&1, 1))
    [<<bitmap::unsigned-32>>, values]
  end

  defp schema_scan_record(record) do
    {bitmap, values, found} =
      @fields
      |> Enum.with_index()
      |> Enum.reduce({0, [], 0}, fn {field, index}, {bitmap, values, found} ->
        case Map.fetch(record, field) do
          {:ok, value} ->
            {bitmap ||| 1 <<< index, [values, NativeValueCodec.encode(value)], found + 1}

          :error ->
            {bitmap, values, found}
        end
      end)

    true = found == map_size(record)
    [<<bitmap::unsigned-32>>, values]
  end

  defp insertion_record(record) do
    {bitmap, indexed} =
      Enum.reduce(record, {0, []}, fn {field, value}, {bitmap, values} ->
        index = field_index!(field)

        {
          bitmap ||| 1 <<< index,
          insert_ordered({index, NativeValueCodec.encode(value)}, values)
        }
      end)

    [<<bitmap::unsigned-32>>, Enum.map(indexed, &elem(&1, 1))]
  end

  defp hybrid_record(record) when map_size(record) <= 4, do: current_record(record)
  defp hybrid_record(record) when map_size(record) <= 12, do: insertion_record(record)
  defp hybrid_record(record), do: schema_scan_record(record)

  defp insert_ordered(value, []), do: [value]

  defp insert_ordered({index, _encoded} = value, [{next, _other} | _rest] = values)
       when index < next,
       do: [value | values]

  defp insert_ordered(value, [head | tail]), do: [head | insert_ordered(value, tail)]

  defp field_index!(field) when is_atom(field), do: Map.fetch!(@atom_indexes, field)
  defp field_index!(field) when is_binary(field), do: Map.fetch!(@string_indexes, field)

  defp records(count, field_count) do
    fields = Enum.take(@fields, field_count)

    Enum.map(1..count, fn number ->
      Map.new(fields, fn field -> {field, value(field, number)} end)
    end)
  end

  defp value(field, number)
       when field in [
              :version,
              :priority,
              :created_at_ms,
              :updated_at_ms,
              :next_run_at_ms,
              :lease_deadline_ms,
              :attempts,
              :max_active_ms
            ],
       do: number

  defp value(:attributes, number), do: %{"customer" => "customer-#{number}"}
  defp value(:state_meta, number), do: %{"ready" => %{"attempt" => number}}
  defp value(:fields, number), do: %{"event" => number}
  defp value(_field, number), do: "value-#{number}"
end

Ferricstore.Bench.QueryResultCodecCandidates.run()
