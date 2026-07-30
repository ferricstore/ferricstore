alias Ferricstore.Store.Router

values = Enum.map(1..64, &"123-#{&1}")
result = {:ok, values}

{^values, 64} = Router.__normalize_batch_write_result_for_test__(result, 64)

Benchee.run(
  %{
    "normalize 64 successful Stream results" => fn ->
      Router.__normalize_batch_write_result_for_test__(result, 64)
    end
  },
  warmup: 2,
  time: 5,
  memory_time: 0,
  reduction_time: 0,
  print: [benchmarking: false, configuration: false]
)
