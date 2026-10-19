defmodule FerricstoreServer.Health.Dashboard.DueRedesignTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records

  @snapshot 100_000

  test "due work is reported as a neutral status, never as an incident" do
    html =
      Records.render_flow_due_summary(
        [due("a", 58_000), due("b", 90_000)],
        [due("c", 340_000)]
      )

    assert html =~ "due-status is-idle"
    assert html =~ "<strong>2 runs due now</strong>"
    assert html =~ "oldest 42.0s overdue"
    assert html =~ "1 scheduled, next in 4.0m"
    assert html =~ "does not establish claimability"
    refute html =~ "is-warning"
  end

  test "an empty sample says nothing is due" do
    html = Records.render_flow_due_summary([], [])

    assert html =~ "due-status is-healthy"
    assert html =~ "<strong>Nothing is due now</strong>"
    assert html =~ "Nothing scheduled in this sample"
  end

  test "rows lead with how overdue a run is, with the clock beside it" do
    html = Records.render_flow_due_records("Due Now", [due("a", 58_000)], 1, 400)

    assert html =~
             ~r{<span class="flow-due-when">42.0s overdue</span><span class="flow-run-secondary" title="[^"]+">\d\d:\d\d:\d\d UTC</span>}

    assert html =~
             ~s(<h2 class="section-title">Due now <span class="badge badge-idle">1</span></h2>)
  end

  test "an empty section is a short prompt, not an empty table" do
    html = Records.render_flow_due_records("Scheduled Future", [], 0, 400)

    assert html =~
             ~s(<p class="section-empty">No runs are scheduled for later in this sample.</p>)

    refute html =~ "<table"
  end

  test "FIFO lanes stay out of the way when the sample has none" do
    assert Records.render_flow_fifo_lanes([], 10, 400, %{}) == ""

    unavailable =
      Records.render_flow_fifo_lanes([], 10, 400, %{
        status: :unavailable,
        unavailable_types: ["t"]
      })

    assert unavailable =~ "FIFO coverage unavailable"
  end

  test "the page's own scope form has room below it" do
    html =
      FerricstoreServer.Health.Dashboard.Render.FlowOverview.render_flow_context_tools(
        %{filters: %{}},
        "flow_due"
      )

    assert html =~ ~s(<form class="flow-filter-form flow-local-scope")
    css = FerricstoreServer.Health.Dashboard.Layout.Styles.stylesheet()
    assert css =~ ".flow-local-scope { margin-bottom: 20px; }"
  end

  defp due(id, run_at_ms),
    do: %{
      id: id,
      type: "payment",
      state: "queued",
      partition_key: "tenant-a",
      run_at_ms: run_at_ms,
      dashboard_snapshot_ms: @snapshot
    }
end
