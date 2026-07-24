Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryResultProjection do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Flow.Query.RecordProjection

  @limit 100
  @projection [:run_id, :state, {:attribute, "customer"}]

  def run do
    records = records(@limit + 1)
    expected = current(records)
    ^expected = candidate(records)

    Benchee.run(
      %{
        "current full allowlist then sparse projection" => fn -> current(records) end,
        "candidate validation summary plus direct sparse projection" => fn ->
          candidate(records)
        end
      },
      QueryPerformance.benchee_options("query-result-projection")
    )
  end

  defp current(records) do
    full =
      Enum.map(records, fn record ->
        {:ok, projected} = RecordProjection.project_result({:ok, record})
        projected
      end)

    page = Enum.take(full, @limit)
    {:ok, output} = RecordProjection.project_records(page, :runs, @projection)
    {Enum.map(full, &summary/1), output}
  end

  defp candidate(records) do
    {summaries, reversed_output, _remaining} =
      Enum.reduce(records, {[], [], @limit}, fn record, {summaries, output, remaining} ->
        output =
          if remaining > 0 do
            {:ok, projected} = RecordProjection.project_validated(record, :runs, @projection)
            [projected | output]
          else
            output
          end

        {[summary(record) | summaries], output, max(remaining - 1, 0)}
      end)

    {Enum.reverse(summaries), Enum.reverse(reversed_output)}
  end

  defp summary(record), do: {Map.fetch!(record, :updated_at_ms), Map.fetch!(record, :id)}

  defp records(count) do
    attributes =
      Map.new(1..128, fn index ->
        {"attribute-#{index}", :binary.copy(<<rem(index, 251)>>, 128)}
      end)
      |> Map.put("customer", "acme")

    Enum.map(1..count, fn index ->
      %{
        id: "run-#{index}",
        type: "invoice",
        state: "queued",
        version: index,
        priority: 0,
        partition_key: "tenant-a",
        created_at_ms: index,
        updated_at_ms: index,
        next_run_at_ms: index,
        lease_deadline_ms: nil,
        attempts: 0,
        run_state: "ready",
        max_active_ms: nil,
        parent_flow_id: nil,
        root_flow_id: nil,
        correlation_id: nil,
        attributes: attributes,
        state_meta: %{},
        internal_payload_ref: "must-not-leak"
      }
    end)
  end
end

Ferricstore.Bench.QueryResultProjection.run()
