alias Ferricstore.Stream.ActivityLog

Application.put_env(:ferricstore, :stream_activity_log_max_len, 512)
Logger.configure(level: :warning)

{:ok, activity_log} = ActivityLog.start_link()

make_input = fn count ->
  results = Enum.map(1..count, &{:ok, "#{&1}-0"})
  activity = Enum.map(1..count, &{"stream:#{&1}", ["field", Integer.to_string(&1)]})
  {results, activity}
end

{results_1, activity_1} = make_input.(1)
{results_8, activity_8} = make_input.(8)
{results_64, activity_64} = make_input.(64)
{results_512, activity_512} = make_input.(512)

:ok = ActivityLog.record_xadd_results(results_512, activity_512)

Benchee.run(
  %{
    "get 128 newest results" => fn -> ActivityLog.get(128) end,
    "get newest result" => fn -> ActivityLog.get(1) end,
    "record one result" => fn -> ActivityLog.record_xadd_results(results_1, activity_1) end,
    "record 8 results" => fn -> ActivityLog.record_xadd_results(results_8, activity_8) end,
    "record 64 results" => fn -> ActivityLog.record_xadd_results(results_64, activity_64) end
  },
  warmup: 2,
  time: 5,
  memory_time: 0,
  reduction_time: 0,
  print: [benchmarking: false, configuration: false]
)

GenServer.stop(activity_log)
