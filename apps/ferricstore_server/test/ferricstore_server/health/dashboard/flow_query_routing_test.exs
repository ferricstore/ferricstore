defmodule FerricstoreServer.Health.Dashboard.FlowQueryRoutingTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.Request
  alias FerricstoreServer.Health.Dashboard.Flow.QueryResult
  alias FerricstoreServer.Health.Dashboard.Render.FlowQueryResults

  test "projected links retain the prepared exact partition without adding returned fields" do
    result = result([{:eq, :partition_key, {:literal, :keyword, "scope & a"}}])
    assert result.rows == [%{id: "run/1", state: "queued"}]
    assert result.column_selectors == [:run_id, :state]
    html = FlowQueryResults.render_flow_query_table(result)
    assert html =~ "/dashboard/flow/run%2F1?partition_key=scope+%26+a"
  end

  test "explicit per-row partition takes precedence over request routing context" do
    result = result([{:eq, :partition_key, {:literal, :keyword, "scope-a"}}])
    result = %{result | rows: [%{id: "run/1", partition_key: "scope-b"}]}
    html = FlowQueryResults.render_flow_query_table(result)
    assert html =~ "partition_key=scope-b"
    refute html =~ "partition_key=scope-a"
  end

  test "unscoped, unresolved, or conflicting projections cannot navigate to an arbitrary partition" do
    for predicates <- [
          [],
          [{:eq, :partition_key, {:parameter, :keyword, "partition"}}],
          [
            {:eq, :partition_key, {:literal, :keyword, "a"}},
            {:eq, :partition_key, {:literal, :keyword, "b"}}
          ],
          [{:in, :partition_key, [{:literal, :keyword, "a"}, {:literal, :keyword, "b"}]}]
        ] do
      html = predicates |> result() |> FlowQueryResults.render_flow_query_table()
      assert html =~ "run/1"
      refute html =~ "href=\"/dashboard/flow/"
      assert html =~ "Include partition_key"
    end
  end

  defp result(predicates) do
    request = %Request{
      mode: :execute,
      source: :runs,
      predicate: {:and, predicates},
      return: :record,
      projection: [:run_id, :state]
    }

    QueryResult.success("FLOW.QUERY", %{records: [%{id: "run/1", state: "queued"}]},
      request: request
    )
  end
end
