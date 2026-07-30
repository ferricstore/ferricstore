defmodule StreamCodecBench do
  def manual([field, value]) do
    <<131, 108, 0, 0, 0, 2, 109, byte_size(field)::unsigned-big-32, field::binary, 109,
      byte_size(value)::unsigned-big-32, value::binary, 106>>
  end

  def generic(fields), do: :erlang.term_to_binary(fields)
end

fields = [
  System.get_env("BENCH_STREAM_FIELD", "field"),
  System.unique_integer() |> Integer.to_string()
]

true = StreamCodecBench.generic(fields) == StreamCodecBench.manual(fields)

Benchee.run(
  %{
    "manual exact two-binary ETF encode" => fn -> StreamCodecBench.manual(fields) end,
    "term_to_binary two-binary list" => fn -> StreamCodecBench.generic(fields) end
  },
  warmup: 2,
  time: 5,
  memory_time: 0,
  reduction_time: 0,
  print: [benchmarking: false, configuration: false]
)
