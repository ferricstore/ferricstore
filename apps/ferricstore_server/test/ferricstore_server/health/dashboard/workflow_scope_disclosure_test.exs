defmodule FerricstoreServer.Health.Dashboard.WorkflowScopeDisclosureTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Render.FlowOverview

  test "route contract describes narrower operational scope" do
    html =
      FlowOverview.render_flow_scope_contract(%{
        filters: %{
          type: " orders ",
          partition_key: " tenant ",
          state: "queued",
          from_ms: 1_234,
          to_ms: 5_678
        }
      })

    for route <- ~w(workers due failures signals) do
      anchor = route_anchor(html, route)
      assert anchor =~ "Carries type and partition only"
      assert anchor =~ "runtime status and updated time are not carried"
      assert anchor =~ "aria-description="
      assert anchor =~ "partition_key=+tenant+"
      refute anchor =~ "state=queued"
    end

    assert route_anchor(html, "lineage") =~
             "type, runtime status and updated time are not carried"
  end

  test "visible contextual links disclose their compatible scope" do
    html =
      FlowOverview.render_flow_context_tools(
        %{filters: %{state: "queued", range: "1h"}},
        "flow_states"
      )

    [anchor] =
      Regex.run(
        ~r/<a class="flow-context-link"[^>]*href="\/dashboard\/flow\/failures[^>]*>/,
        html
      )

    assert anchor =~ "title=\"Carries type and partition only"
    assert anchor =~ "aria-description="
  end

  test "advanced Query navigation does not imply arbitrary FQL transfer" do
    html =
      FlowOverview.render_flow_scope_contract(%{
        filters: %{type: "orders", partition_key: "tenant"},
        workbench: %{mode: :advanced}
      })

    for route <- ~w(states query workers lineage) do
      assert route_anchor(html, route) =~ "Additional FQL predicates and ordering are not carried"
    end
  end

  defp route_anchor(html, route),
    do: Regex.run(~r/<a data-dashboard-route="\/dashboard\/flow\/#{route}"[^>]*>/, html) |> hd()
end
