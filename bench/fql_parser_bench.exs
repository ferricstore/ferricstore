# Run with:
#   MIX_ENV=bench mix run --no-start bench/fql_parser_bench.exs

Code.require_file("support/query_performance.exs", __DIR__)

alias Ferricstore.Bench.QueryPerformance
alias Ferricstore.Flow.Query.{Binder, ReferenceParser}
alias FerricstoreServer.Native.{FlowQuery, FQLParser, NIF}

defmodule Ferricstore.Bench.FQLParserPreflight do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Binder, ReferenceParser}
  alias FerricstoreServer.Native.{FQLParser, NIF}

  def preflight_inputs!(inputs) do
    Enum.each(inputs, fn {name, input} ->
      reference = ReferenceParser.parse(input.query)
      native = FQLParser.parse(input.query)

      if native != reference do
        raise "FQL benchmark parser mismatch for #{name}: " <>
                "native=#{inspect(native)} reference=#{inspect(reference)}"
      end

      case {input[:malformed?], native, NIF.parse_fql(input.query)} do
        {true, {:error, _native_reason}, {:error, _nif_reason, _diagnostic}} ->
          :ok

        {malformed?, {:ok, request}, {:ok, _, _, _, _, _, _, _}} when malformed? != true ->
          if input[:bind?] != false do
            {:ok, _bound} = Binder.bind(request, input.params)
          end

        preflight_result ->
          raise "unexpected FQL benchmark preflight result for #{name}: " <>
                  inspect(preflight_result)
      end
    end)
  end
end

max_query_bytes = 16 * 1024
max_prefix = "FROM runs WHERE partition_key = '"
max_suffix = "' ORDER BY updated_at_ms ASC LIMIT 25 RETURN RECORDS"

max_valid =
  max_prefix <>
    String.duplicate("x", max_query_bytes - byte_size(max_prefix) - byte_size(max_suffix)) <>
    max_suffix

max_malformed = "'" <> String.duplicate("x", max_query_bytes - 1)

inputs = %{
  "point" => %{
    query: "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD",
    params: %{"partition" => "tenant-a", "flow_id" => "run-123"}
  },
  "collection" => %{
    query:
      "FROM runs WHERE partition_key = @partition AND state IN ('failed', 'completed') AND updated_at_ms FROM @from TO @until ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS",
    params: %{"partition" => "tenant-a", "from" => 1, "until" => 1_000_000}
  },
  "count" => %{
    query:
      "FROM runs WHERE partition_key = @partition AND type = 'payment' AND state = 'failed' RETURN COUNT",
    params: %{"partition" => "tenant-a"}
  },
  "history" => %{
    query:
      "FROM events WHERE partition_key = @partition AND run_id = @flow_id ORDER BY event_id DESC LIMIT 100 RETURN RECORDS",
    params: %{"partition" => "tenant-a", "flow_id" => "run-123"}
  },
  "explain" => %{
    query:
      "EXPLAIN FROM runs WHERE partition_key = @partition AND state = 'failed' ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS",
    params: %{"partition" => "tenant-a"}
  },
  "explain analyze" => %{
    query:
      "EXPLAIN ANALYZE FROM runs WHERE partition_key = @partition AND state = 'failed' ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS",
    params: %{"partition" => "tenant-a"}
  },
  "max valid" => %{query: max_valid, params: %{}, bind?: false},
  "max malformed" => %{query: max_malformed, params: %{}, malformed?: true}
}

Ferricstore.Bench.FQLParserPreflight.preflight_inputs!(inputs)

Benchee.run(
  %{
    "raw Rust parse + NIF term encoding" => fn %{query: query} -> NIF.parse_fql(query) end,
    "Rust parse + Elixir wrapper decoding" => fn %{query: query} -> FQLParser.parse(query) end,
    "Elixir reference parse" => fn %{query: query} -> ReferenceParser.parse(query) end
  },
  [inputs: inputs] ++ QueryPerformance.benchee_options("fql-nif-boundary")
)

valid_inputs =
  Map.reject(inputs, fn {_name, input} ->
    input[:malformed?] == true or input[:bind?] == false
  end)

Benchee.run(
  %{
    "Rust parse + wrapper + bind" => fn %{query: query, params: params} ->
      {:ok, request} = FQLParser.parse(query)
      {:ok, bound} = Binder.bind(request, params)
      bound
    end
  },
  [inputs: valid_inputs] ++ QueryPerformance.benchee_options("fql-parse-bind")
)

planner_query =
  "EXPLAIN FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"

planner_params = %{"partition" => "tenant-a", "flow_id" => "run-123"}
{:ok, _explain} = FlowQuery.execute(%{}, "FQL1", planner_query, planner_params)

Benchee.run(
  %{
    "Rust parse + bind + explain planner" => fn ->
      FlowQuery.execute(%{}, "FQL1", planner_query, planner_params)
    end
  },
  QueryPerformance.benchee_options("fql-explain-planner")
)
