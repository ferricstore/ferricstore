defmodule FerricstoreServer.Health.Dashboard.GovernanceRedesignTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.FlowGovernance

  test "open circuits lead, with waiting approvals beside them" do
    html =
      summary(
        circuits: 3,
        open_circuits: 2,
        half_open_circuits: 1,
        approvals: 4,
        pending_approvals: 1
      )

    assert html =~ "governance-status is-degraded"
    assert html =~ "<strong>2 circuits open</strong>"
    assert html =~ "rejecting effects"
    assert html =~ "1 half-open"
    assert html =~ "1 approval waiting"
  end

  test "waiting approvals alone are a warning" do
    html = summary(approvals: 2, pending_approvals: 2)

    assert html =~ "governance-status is-warning"
    assert html =~ "<strong>2 approvals waiting</strong>"
  end

  test "records with nothing waiting read as healthy, and no records as not in use" do
    assert summary(approvals: 3, budgets: 1, circuits: 2) =~ "<strong>Nothing waiting</strong>"

    unused = summary([])
    assert unused =~ "governance-status is-idle"
    assert unused =~ "<strong>Governance is not in use</strong>"
    assert unused =~ "No approvals, budgets, limits or circuits are visible."
  end

  test "the six counts are one compact strip instead of cards" do
    html = summary(approvals: 3, pending_approvals: 1, open_circuits: 0)

    refute html =~ "flow-card-grid"
    assert html =~ ~s(<dl class="stat-strip" aria-label="Governance counts">)
    assert html =~ ~s(<div class="is-warning"><dt>Pending</dt><dd>1</dd></div>)
    assert html =~ ~s(<div><dt>Open circuits</dt><dd>0</dd></div>)
  end

  defp summary(counts),
    do: FlowGovernance.render_flow_governance_summary(%{counts: Map.new(counts)})
end
