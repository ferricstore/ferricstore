defmodule FerricstoreServer.Health.Dashboard.RetentionRedesignTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.FlowRetention

  test "the summary says whether the sample has anything to clean up" do
    clean = FlowRetention.render_flow_retention_summary(%{total_sampled: 102, sample_limit: 400})
    assert clean =~ "retention-status is-healthy"
    assert clean =~ "<strong>Nothing to clean up in this sample</strong>"
    assert clean =~ "102 sampled records"

    due =
      FlowRetention.render_flow_retention_summary(%{
        active_timeout_eligible_sampled: 2,
        terminal_eligible_sampled: 5,
        total_sampled: 102
      })

    assert due =~ "retention-status is-idle"
    assert due =~ "<strong>7 runs ready for cleanup in this sample</strong>"
    assert due =~ "2 past their max active runtime"
    assert due =~ "5 past their retention"
    refute due =~ "Sample Preview"
  end

  test "with no candidates one prompt replaces both empty tables" do
    html = FlowRetention.render_flow_retention_candidates(%{now_ms: 1_000})

    assert html =~ ~s(<h2 class="section-title">Cleanup candidates</h2>)
    assert html =~ "No sampled run is past its max active runtime or its retention."
    refute html =~ "<table"
  end

  test "an empty category beside a populated one is a one-line prompt" do
    record = %{id: "r", type: "t", state: "completed", partition_key: "p", updated_at_ms: 1}

    html =
      FlowRetention.render_flow_retention_candidates(%{
        candidates: [record],
        terminal_eligible_sampled: 1,
        now_ms: 1_000
      })

    assert html =~ ~s(<p class="section-empty">No sampled run is past its max active runtime.</p>)
    assert html =~ "Sampled terminal deletions"
    assert length(Regex.scan(~r/<table/, html)) == 1
  end
end
