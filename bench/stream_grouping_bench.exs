alias Ferricstore.Commands.Stream.AppendBatch

commands_for = fn topics ->
  Enum.map(0..63, fn index ->
    key = "stream:#{rem(index, topics)}"
    {:stream_append, key, :auto, ["field", Integer.to_string(index)], nil, false}
  end)
end

commands_1 = commands_for.(1)
commands_8 = commands_for.(8)
commands_64 = commands_for.(64)

{:ok, groups_1, 64} = AppendBatch.group_commands(commands_1)
{:ok, groups_8, 64} = AppendBatch.group_commands(commands_8)
{:ok, groups_64, 64} = AppendBatch.group_commands(commands_64)

benchmarks =
  %{
    "group 1 topic x64" => fn -> AppendBatch.group_commands(commands_1) end,
    "group 64 topics x64" => fn -> AppendBatch.group_commands(commands_64) end,
    "group 8 topics x64" => fn -> AppendBatch.group_commands(commands_8) end,
    "validate 1 topic x64" => fn -> AppendBatch.validate_groups_with_work(groups_1, 64) end,
    "validate 64 topics x64" => fn -> AppendBatch.validate_groups_with_work(groups_64, 64) end,
    "validate 8 topics x64" => fn -> AppendBatch.validate_groups_with_work(groups_8, 64) end
  }

benchmarks =
  case System.get_env("BENCH_FILTER") do
    nil -> benchmarks
    "" -> benchmarks
    filter -> Map.filter(benchmarks, fn {name, _fun} -> String.contains?(name, filter) end)
  end

Benchee.run(
  benchmarks,
  warmup: 2,
  time: 5,
  memory_time: 0,
  reduction_time: 0,
  print: [benchmarking: false, configuration: false]
)
