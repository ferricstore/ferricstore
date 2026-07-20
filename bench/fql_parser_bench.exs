# Run with:
#   MIX_ENV=bench mix run --no-start bench/fql_parser_bench.exs

alias Ferricstore.Flow.Query.{Binder, ReferenceParser}
alias FerricstoreServer.Native.FlowQuery
alias FerricstoreServer.Native.FQLParser

warmup = System.get_env("BENCH_WARMUP", "1") |> String.to_integer()
time = System.get_env("BENCH_TIME", "3") |> String.to_integer()
parallel = System.get_env("BENCH_PARALLEL", "1") |> String.to_integer()

query =
  "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"

params = %{"partition" => "tenant-a", "flow_id" => "run-123"}
explain_query = "EXPLAIN " <> query
max_byte_malformed_query = "'" <> String.duplicate("x", 16 * 1024 - 1)

Benchee.run(
  %{
    "rust parse" => fn ->
      {:ok, _request} = FQLParser.parse(query)
    end,
    "rust parse + bind" => fn ->
      {:ok, request} = FQLParser.parse(query)
      {:ok, _bound} = Binder.bind(request, params)
    end,
    "rust parse + bind + explain" => fn ->
      {:ok, _explain} = FlowQuery.execute(%{}, "FQL1", explain_query, params)
    end,
    "rust parse max-byte malformed string" => fn ->
      {:error, :invalid_syntax} = FQLParser.parse(max_byte_malformed_query)
    end,
    "elixir reference parse" => fn ->
      {:ok, _request} = ReferenceParser.parse(query)
    end
  },
  warmup: warmup,
  time: time,
  parallel: parallel,
  memory_time: 0
)
