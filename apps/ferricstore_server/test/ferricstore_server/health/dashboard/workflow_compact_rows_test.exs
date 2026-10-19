defmodule FerricstoreServer.Health.Dashboard.WorkflowCompactRowsTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records

  @run_at 1_792_409_410_580
  @updated_at 1_792_409_410_665

  test "a recent run reads on two short lines" do
    html = Records.render_flow_recent_records([record(state: "queued", run_state: "queued")])

    assert html =~
             ~s(<span class="flow-run-secondary">payment · <span class="mono">tenant-a</span></span>)

    assert html =~
             ~s(<td class="flow-run-updated" title="Run 2026-10-19 11:30:10.580 UTC · Updated 2026-10-19 11:30:10.665 UTC">11:30:10 UTC</td>)

    assert html =~ ~s(<span class="flow-run-reason">)
    assert html =~ ~s(<span class="flow-run-attempts">0 attempts</span>)
    refute html =~ ~s(<span class="c-muted">Run</span>)
    assert html =~ "<th>Updated (UTC)</th>"
  end

  test "the stored state is shown only when it differs from the workflow state" do
    same = Records.render_flow_recent_records([record(state: "queued", run_state: "queued")])
    refute same =~ ~s(class="flow-run-stored")
    assert same =~ ~s(title="Workflow state: queued · Stored state: queued")

    different =
      Records.render_flow_recent_records([
        record(state: "running", run_state: "inventory_allocated")
      ])

    assert different =~ ~s(<span class="flow-run-stored">stored running</span>)
    assert different =~ "inventory_<wbr>allocated"
  end

  defp record(opts) do
    %{
      id: "flow-1",
      type: "payment",
      partition_key: "tenant-a",
      state: Keyword.fetch!(opts, :state),
      run_state: Keyword.fetch!(opts, :run_state),
      attempts: 0,
      run_at_ms: @run_at,
      updated_at_ms: @updated_at
    }
  end
end
