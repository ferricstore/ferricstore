alias Ferricstore.Commands.Stream.AtomicAppend

key = "bench:stream:plan"
fields_lists = List.duplicate(["field", "value"], 64)

store = %{
  stream_apply_now_ms: 123,
  max_value_size: 1_048_576,
  compound_get: fn _redis_key, _compound_key -> nil end
}

{:ok, %AtomicAppend.Plan{results: results}} =
  AtomicAppend.plan_many_auto(key, fields_lists, store)

true = length(results) == 64

Benchee.run(
  %{
    "plan 64 auto-ID Stream entries" => fn ->
      AtomicAppend.plan_many_auto(key, fields_lists, store)
    end
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  reduction_time: 0,
  print: [benchmarking: false, configuration: false]
)
