defmodule FerricstoreServer.Health.Dashboard.WorkflowFifthReviewTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Flow.PolicyEditor
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records

  alias FerricstoreServer.Health.Dashboard.Render.{
    FlowGovernance,
    FlowHistory,
    FlowIndexCatalog,
    FlowPolicy,
    FlowSchedules
  }

  test "failure records expose literal partition identity beside the workflow link" do
    html =
      Records.render_flow_failures_rows([
        %{id: "shared-id", type: "invoice", state: "running", partition_key: "customer/a&b"},
        %{id: "shared-id", type: "invoice", state: "failed", partition_key: "customer-2"}
      ])

    assert html =~ ~s(class="flow-run-identity mono")
    assert html =~ ~s(title="Partition">customer/a&amp;b</span>)
    assert html =~ ~s(title="Partition">customer-2</span>)
    assert html =~ "partition_key=customer%2Fa%26b"
  end

  test "bare running runtime status is neutral while terminal failure retains attention" do
    assert FlowHistory.flow_state_class("running") == ""
    assert FlowHistory.flow_state_badge_class("running") == "badge-idle"
    assert FlowHistory.flow_state_class("failed") == "c-red"
    assert FlowHistory.flow_state_badge_class("failed") == "badge-pressure"
  end

  test "state policy editors disable every type-wide field and link to type defaults" do
    editor = %{PolicyEditor.empty() | type: "invoice/a&b", state: "queued", max_active_ms: 45_000}
    html = FlowPolicy.render_flow_policy_editor(%{editor: editor})

    for name <- ~w(max_active_ms indexed_attributes indexed_state_meta) do
      assert html =~ ~r/<input[^>]*name="#{name}"[^>]* disabled/
    end

    assert html =~
             ~s(href="/dashboard/flow/policies?edit=invoice%2Fa%26b#flow-policy-editor")

    assert html =~ "Type-wide settings are unchanged"
    assert html =~ "Edit type defaults"
  end

  test "type policy editors keep the maximum active duration editable" do
    html =
      FlowPolicy.render_flow_policy_editor(%{editor: %{PolicyEditor.empty() | type: "invoice"}})

    refute html =~ ~r/<input[^>]*name="max_active_ms"[^>]* disabled/
  end

  test "index lifecycle prioritizes health and discloses copyable opaque build identity" do
    build_id = "build/" <> String.duplicate("opaque&", 30)

    html =
      FlowIndexCatalog.render(%{
        status: :ok,
        snapshot: %{
          "indexes" => [
            %{"id" => "invoice/region", "version" => 7, "build_id" => build_id}
          ]
        }
      })

    assert html =~ ~s(class="flow-index-identity mono")
    assert html =~ ~r/<th>Validation<\/th><th>Statistics<\/th>.*<th>Retirement<\/th>/s

    assert html =~
             ~r/<details class="flow-index-build-identity">\s*<summary>Build identity<\/summary>/

    assert html =~ ~s(aria-label="Copy build ID")
    assert html =~ "data-copy-text=\"build/opaque&amp;"
    assert html =~ ~s(role="region" aria-label="Query index lifecycle table" tabindex="0")
  end

  test "schedule payload editor declares a multiline editing surface" do
    html = FlowSchedules.render_flow_schedule_create_form()
    assert html =~ ~r/<textarea[^>]*class="[^"]*flow-schedule-payload[^"]*"[^>]*rows="5"/
  end

  test "zero circuit categories have no colored fill but positive counts retain a bar" do
    empty = FlowGovernance.render_flow_governance_circuit_graph([])
    refute empty =~ ~r/<span class="status-(?:bad|warn|good)"/

    populated = FlowGovernance.render_flow_governance_circuit_graph([%{status: :open}])
    assert populated =~ ~s(<span class="status-bad" style="width: 100.0%")
    refute populated =~ ~r/<span class="status-(?:warn|good)"/
  end
end
