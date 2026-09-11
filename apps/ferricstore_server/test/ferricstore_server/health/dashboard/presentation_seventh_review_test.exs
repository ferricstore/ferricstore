defmodule FerricstoreServer.Health.Dashboard.PresentationSeventhReviewTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Flow.QueryWorkbench
  alias FerricstoreServer.Health.Dashboard.Render.{FlowCharts, FlowQueryHelp}
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Signals

  test "failed timeline intervals are errors rather than terminal successes" do
    for fields <- [%{"action" => "fail"}, %{"action" => "failed"}, %{"state" => "failed"}] do
      assert FlowCharts.flow_timeline_bar_class(%{fields: fields}) == "bar-red"
    end

    assert FlowCharts.flow_timeline_bar_class(%{fields: %{"action" => "complete"}}) ==
             "bar-green"
  end

  test "unscanned Signals does not report a measured zero" do
    html = Signals.render_flow_signals_table([], 10, 10, 400, %{scan_history: false})
    assert html =~ "Not scanned"
    refute html =~ "0 signal events"
  end

  test "completed empty scans and loaded detail histories retain measured zero" do
    html = Signals.render_flow_signals_table([], 10, 10, 400, %{scan_history: true})
    assert html =~ "0 signal events"
    refute html =~ "Not scanned"

    detail = Signals.render_flow_signals_table([], nil, nil, nil, %{}, :detail)
    assert detail =~ "0 signal events"
    refute detail =~ "Not scanned"
  end

  test "state charts disclose their visualization cap and table order" do
    rows = for n <- 1..18, do: %{type: "orders", state: "step-#{n}", count: 1}
    html = FlowCharts.render_flow_states_chart(rows)
    assert html =~ "16 of 18"
    assert html =~ "table order"
    assert html =~ "2 omitted"
    assert length(Regex.scan(~r/<tr data-state=/, html)) == 16
    refute html =~ "orders:step-17"
  end

  test "worker charts disclose their independent cap without expanding work" do
    workers = for n <- 1..23, do: %{worker: "worker-#{n}", running: n, expired: 0}
    html = FlowCharts.render_flow_workers_chart(workers)
    assert html =~ "20 of 23"
    assert html =~ "table order"
    assert html =~ "3 omitted"
    refute html =~ ">worker-21<"
    refute FlowCharts.render_flow_workers_chart(Enum.take(workers, 2)) =~ "omitted"
  end

  test "raw reference describes supported bounded syntax and all examples prepare" do
    html = FlowQueryHelp.render_reference()
    assert html =~ "FQL1 reference"
    assert html =~ "RETURN RECORDS"
    assert html =~ "state_meta"
    assert html =~ "EXPLAIN ANALYZE"
    assert html =~ "does not execute"

    for %{query: query, params: params} <- FlowQueryHelp.examples() do
      assert {:ok, _, _} =
               QueryWorkbench.prepare(%{
                 "fql" => query,
                 "params_json" => Jason.encode!(params),
                 "action" => "run"
               })
    end
  end

  test "empty guided recovery keeps the executed scope escaped and offers draft-only actions" do
    html =
      FlowQueryHelp.render_empty_recovery(%{
        result: %{status: :ok, rows: [], command: "FLOW.QUERY"},
        filters: %{kind: "list", type: "<orders>", partition_key: "a&b"},
        workbench: %{mode: :guided}
      })

    assert html =~ "No rows in this result page"
    assert html =~ "&lt;orders&gt;"
    assert html =~ "a&amp;b"
    assert html =~ "Review query scope"
    assert html =~ "data-flow-query-clear-optional"
    assert html =~ "Remove optional filters"
    refute html =~ "type=submit"
  end

  test "raw, non-query, nonempty, scalar and error responses cannot acquire unsafe reset actions" do
    raw =
      FlowQueryHelp.render_empty_recovery(%{
        result: %{status: :ok, rows: [], command: "FLOW.QUERY"},
        workbench: %{mode: :advanced}
      })

    assert raw =~ "Edit FQL"
    refute raw =~ "data-flow-query-clear-optional"

    for result <- [
          %{status: :idle, rows: []},
          %{status: :error, rows: []},
          %{status: :ok, rows: [%{id: "one"}]},
          %{status: :ok, rows: [], scalar: %{kind: :count, value: 0}}
        ] do
      assert FlowQueryHelp.render_empty_recovery(%{result: result}) == ""
    end
  end
end
