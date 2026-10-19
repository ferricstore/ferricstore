defmodule FerricstoreServer.Health.Dashboard.QueryRedesignTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Access

  test "a query that has not run keeps its prompt for signed-in users" do
    idle = %{status: :idle, command: "FLOW.QUERY", rows: [], message: "Enter a partition key"}

    assert Access.flow_query_filter_result_for_acl(idle, "default", "*") == idle
  end

  alias FerricstoreServer.Health.Dashboard.Render.FlowQueryResults
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.{Lineage, Signals}

  test "an unscanned Signals page offers the scan in one click, keeping the filters" do
    html =
      Signals.render_flow_signals_table([], 102, 30, 400, %{scan_history: false, type: "orders"})

    assert html =~ "Not scanned"
    refute html =~ "<table"
    assert html =~ ~s(<div class="section-empty">)
    assert html =~ ~s(href="/dashboard/flow/signals?type=orders&amp;scan=true")
    assert html =~ "Scan 30 run histories"
  end

  test "a scan that found nothing says so without an empty table" do
    html = Signals.render_flow_signals_table([], 10, 10, 400, %{scan_history: true})

    assert html =~ "0 signal events"
    assert html =~ ~s(<p class="section-empty">No signal events in the scanned histories.</p>)
    refute html =~ "<table"
  end

  test "before a lookup, Lineage offers recent runs to start from" do
    html =
      Lineage.render_flow_lineage_status(%{
        result: %{status: :idle},
        filters: %{target: nil, partition_key: nil},
        hints: [%{mode: "root", label: "root", id: "order/root", partition_key: "tenant north"}]
      })

    assert html =~ ~s(<div class="section-empty">)
    assert html =~ "Choose parent, root, or correlation and enter an id."
    assert html =~ "Or start from a recent run:"

    assert html =~
             ~s(href="/dashboard/flow/lineage?id=order%2Froot&amp;mode=root&amp;partition_key=tenant+north")

    refute html =~ "<details"
  end

  test "a query that has not run is a prompt" do
    assert FlowQueryResults.render_flow_query_status(%{
             status: :idle,
             message: "Enter a partition key"
           }) ==
             ~s(<p class="section-empty">Enter a partition key</p>)
  end

  test "the Query page explains itself once" do
    html = Dashboard.render_flow_query_page(Dashboard.collect_flow_query_page([]))

    assert html =~ ~s(<p class="page-intro">Run bounded queries over workflow records)
    refute html =~ "flow-query-payload-note"

    assert html =~
             "Independent drafts: switching does not carry changes over or replace the results. Payloads stay unloaded unless a query asks for them."
  end
end
