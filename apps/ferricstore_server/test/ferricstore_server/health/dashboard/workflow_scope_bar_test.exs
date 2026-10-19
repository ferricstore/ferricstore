defmodule FerricstoreServer.Health.Dashboard.WorkflowScopeBarTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Layout
  alias FerricstoreServer.Health.Dashboard.Render.FlowOverview

  test "list pages keep the run lookup and drop the links the sidebar and tabs already give" do
    html =
      FlowOverview.render_flow_context_tools(
        %{filters: %{type: "orders", partition_key: "tenant"}},
        "flow_states"
      )

    assert html =~ ~s(aria-label="Flow lookup")
    assert html =~ "data-dashboard-workflow-scope"
    refute html =~ ~s(class="flow-context-link")
    refute html =~ "Workflow investigation context"
    assert html =~ ~s(<div class="flow-scope-bar">)
  end

  test "the scope a list page carries is named once, next to the lookup" do
    html =
      FlowOverview.render_flow_context_tools(
        %{filters: %{type: "orders", partition_key: "tenant"}},
        "flow_failures"
      )

    assert html =~ ~s(<span class="flow-scope-bar-label">Scope</span>)
    assert html =~ "type orders"
    assert html =~ "partition tenant"

    unscoped = FlowOverview.render_flow_context_tools(%{filters: %{}}, "flow_failures")
    refute unscoped =~ "flow-scope-bar-label"
  end

  test "a run's page keeps the links that follow its own lineage" do
    html =
      FlowOverview.render_flow_context_tools(
        %{record: %{id: "a", type: "t", partition_key: "p", root_flow_id: "root-1"}},
        "flow_detail",
        show_scope: false
      )

    assert html =~ "Workflow investigation context"
    assert html =~ "id=root-1"
  end

  test "the header tabs carry the workflow scope like the sidebar does" do
    assert Layout.dashboard_live_script() =~ "'.sidebar a[href], .page-tabs a[href]'"
  end
end
