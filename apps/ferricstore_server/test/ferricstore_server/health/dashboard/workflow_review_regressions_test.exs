defmodule FerricstoreServer.Health.Dashboard.WorkflowReviewRegressionsTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Render.FlowCharts
  alias FerricstoreServer.Health.Dashboard.Render.FlowComponents
  alias FerricstoreServer.Health.Dashboard.Render.FlowDetail
  alias FerricstoreServer.Health.Dashboard.Render.FlowFilters
  alias FerricstoreServer.Health.Dashboard.Render.FlowOverview
  alias FerricstoreServer.Health.Dashboard.Render.FlowQueryControls
  alias FerricstoreServer.Health.Dashboard.Render.FlowSchedules
  alias FerricstoreServer.Health.Dashboard.Render.FlowRetention
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records
  alias FerricstoreServer.Health.Dashboard.FlowRecord

  test "Due Work uses a compact unframed comparison and keeps exact sampled counts" do
    for {due, scheduled} <- [{[], []}, {[1, 2, 3], [4]}, {[], [1, 2]}] do
      html = FlowCharts.render_flow_due_chart(due, scheduled)

      assert html =~
               ~s(<section class="flow-due-summary" aria-label="Sampled due and scheduled work">)

      refute html =~ "chart-card"
      refute html =~ "chart-grid"
      refute html =~ "Due Charts"
      assert html =~ "Current sample"
      assert html =~ "does not establish claimability"
      assert html =~ ~s(<span class="chart-bar-value">#{length(due)}</span>)
      assert html =~ ~s(<span class="chart-bar-value">#{length(scheduled)}</span>)
    end
  end

  test "running records keep core fields visible and disclose escaped technical lease fields" do
    record = %{
      id: "run<&",
      type: "invoices<&",
      partition_key: "scope<&",
      state: "running",
      run_state: "queued",
      worker: "worker<&",
      lease_expires_at_ms: 1,
      lease_token: "token<&",
      fencing_token: 42
    }

    html = Records.render_flow_running_records([record], 1, 400)
    assert html =~ ~s(<table class="flow-worker-records-table">)
    assert html =~ "<th>Workflow</th><th>Worker</th><th>Status</th><th>Lease Expires (UTC)</th>"
    refute html =~ "<th>Lease Token</th>"
    refute html =~ "<th>Fencing</th>"
    assert html =~ "invoices&lt;&amp;"
    assert html =~ "worker&lt;&amp;"
    assert html =~ "scope&lt;&amp;"
    assert html =~ "partition_key=scope%3C%26"
    assert html =~ "lease expired; check recovery eligibility"

    [_, disclosure] =
      Regex.run(~r/(<details class="flow-worker-lease-details".*?<\/details>)/s, html)

    assert disclosure =~ "<summary>Lease details"
    assert disclosure =~ "data-dashboard-disclosure-key="
    assert disclosure =~ "data-dashboard-live-pause"
    assert disclosure =~ "<dt>Lease token</dt><dd>token&lt;&amp;</dd>"
    assert disclosure =~ "<dt>Fencing token</dt><dd>42</dd>"
    refute disclosure =~ ~r/<details[^>]*\sopen[\s>]/
    refute html =~ "token<&"
  end

  test "lease disclosure identities are stable, partition-safe and leave empty rows aligned" do
    record = %{
      id: "same-id",
      type: "type",
      state: "running",
      lease_token: nil,
      fencing_token: nil
    }

    render = fn partition ->
      Records.render_flow_running_records([Map.put(record, :partition_key, partition)], 1, 400)
    end

    key = fn html ->
      [_, key] = Regex.run(~r/data-dashboard-disclosure-key="([^"]+)"/, html)
      key
    end

    assert key.(render.("a")) == key.(render.("a"))
    refute key.(render.("a")) == key.(render.("b"))
    assert key.(render.(nil)) == key.(render.(""))
    assert render.(nil) =~ "<dt>Lease token</dt><dd>-</dd>"
    assert render.(nil) =~ "<dt>Fencing token</dt><dd>-</dd>"
    assert Records.render_flow_running_records([], 0, 400) =~ ~s(colspan="4")
  end

  test "Workers leads with workers and leased records while secondary charts stay disclosed" do
    html =
      Dashboard.render_flow_workers_page(%{
        workers: [],
        running_records: [],
        total_sampled: 0,
        sample_limit: 400
      })

    assert html =~ ~s(<details class="dashboard-disclosure" id="flow-worker-breakdown">)

    assert position(html, ~s(data-live-component="flow_workers")) <
             position(html, ~s(data-live-component="flow_running_records"))

    assert position(html, ~s(data-live-component="flow_running_records")) <
             position(html, ~s(id="flow-worker-breakdown"))

    for component <- ~w(flow_workers flow_running_records flow_workers_chart flow_fifo_lanes) do
      assert length(Regex.scan(Regex.compile!(~s(data-live-component="#{component}")), html)) == 1
    end
  end

  test "Retention uses compact metrics and controls without hiding cleanup safeguards" do
    summary = FlowRetention.render_flow_retention_summary(%{})
    assert summary =~ ~s(<dl class="flow-overview-ribbon" aria-label="Retention sample metrics">)
    refute summary =~ "flow-card"
    assert summary =~ "Active Timeouts"
    assert summary =~ "Disk"
    restricted = FlowRetention.render_flow_retention_summary(%{storage: %{restricted: true}})
    refute restricted =~ "Disk"
    refute restricted =~ "Pending index operations"

    controls = FlowRetention.render_flow_retention_controls(%{})
    assert controls =~ ~s(class="flow-retention-controls")
    assert controls =~ ~s(class="flow-policy-field flow-retention-limit")
    refute controls =~ "flow-policy-grid"
    refute controls =~ ~s(name="confirm_cleanup")
    assert controls =~ ~s(value="dry_run")
    assert controls =~ ~s(value="review_cleanup")
    assert controls =~ "Global record limit"

    review =
      FerricstoreServer.Health.Dashboard.Flow.RetentionReview.prepare(3)
      |> Map.put(:kind, :review)

    reviewed = FlowRetention.render_flow_retention_controls(%{flash: review})
    assert reviewed =~ ~r/name="confirm_cleanup"[^>]*required/
    assert reviewed =~ ~s(value="cleanup")
    assert reviewed =~ "All shards, all workflow types and partitions"

    html = Dashboard.render_flow_retention_page(%{})

    assert position(html, "Sampled Active Timeouts") <
             position(html, ~s(id="flow-retention-reference"))

    assert html =~ ~s(<details class="dashboard-disclosure" id="flow-retention-reference">)
  end

  test "bar charts give zero no fill while preserving small positive counts" do
    html =
      FlowCharts.render_bar_chart([
        %{
          label: "worker",
          values: [
            {"Expired", 0, "bar-red"},
            {"Leased", 1, "bar-green"},
            {"Total", 1000, "bar-neutral"}
          ]
        }
      ])

    assert html =~ ~s(class="chart-bar-fill bar-red" style="width: 0%")
    assert html =~ ~s(class="chart-bar-fill bar-green" style="width: 2%")
    assert html =~ ~s(class="chart-bar-fill bar-neutral" style="width: 100%")
  end

  test "due charts and record diagnostics do not infer claim eligibility from timestamps" do
    record = %{id: "waiting", type: "invoices", state: "queued", run_at_ms: 1}
    html = FlowCharts.render_flow_due_chart([record], [])
    refute html =~ "Claim readiness"
    refute html =~ "bar-yellow"
    assert html =~ "does not establish claimability"
    refute FlowRecord.flow_waiting_reason(record) =~ "waiting for worker claim"
    assert FlowDetail.flow_execution_debug_summary(record) == "due time reached"

    expired = Map.merge(record, %{state: "running", run_state: "queued", lease_expires_at_ms: 1})
    refute FlowRecord.flow_waiting_reason(expired) =~ "reclaimable"
    refute FlowDetail.render_flow_diagnostic_hero(%{record: expired}) =~ "Work is reclaimable"
  end

  test "due-time summaries never promise claimability or report ordinary waiting as an incident" do
    for mode <- [:fifo, :parallel], running <- [0, 1] do
      state = state(mode, running)
      html = Records.render_flow_states_table([state], 10, 9, 400, %{})
      refute html =~ "Workers should be able to claim"
      refute html =~ "workers should drain"
      refute html =~ "no running sample"
      refute html =~ ~s(class="c-yellow")
      assert html =~ "Due time reached"
      assert html =~ "does not establish claimability"
    end

    html = FlowCharts.render_flow_states_chart([state(:fifo, 0)])
    refute html =~ "bar-yellow"
    assert html =~ "bar-neutral"
    html = FlowOverview.render_flow_overview(%{due_now_sampled: 9}, 10, 400)
    assert html =~ "Due now"
    refute html =~ "Ready now"
  end

  test "actual expired leases and failures still take priority over due-time hints" do
    for {field, hint} <- [expired_leases: "Lease expired", failed: "terminal failed"] do
      html = Records.flow_state_operational_hint(Map.put(state(:fifo, 0), field, 1))
      assert html =~ hint
      assert html =~ "c-red"
    end
  end

  test "attention actions retain exact type and partition without turning all into a wildcard" do
    for type <- ["all", "ALL", "type<&"], partition <- [nil, "scope /&"] do
      data = %{filters: %{type: type, partition_key: partition}}
      html = FlowCharts.render_flow_issue_cards(%{expired_leases_sampled: 2, failed: 1}, data)
      [_, href] = Regex.run(~r/<a[^>]+href="([^"]+)"/, html)
      uri = href |> String.replace("&amp;", "&") |> URI.parse()
      assert uri.path == "/dashboard/flow/failures"
      params = URI.decode_query(uri.query)
      assert params["type"] == type
      assert params["partition_key"] == partition
      assert html =~ "Investigate"
      assert html =~ "Current sample"
      refute html =~ "type<&"
    end
  end

  test "each States filter keeps its label and control in one field group" do
    html = FlowFilters.render_flow_type_filter(states_data(%{}))

    for id <- ~w(type state partition name range from to limit) do
      assert html =~
               Regex.compile!(
                 ~s'<label class="flow-filter-field" for="flow-state-#{id}-filter"[^>]*>\\s*<span>[^<]+</span>\\s*<(?:input|select)[^>]+id="flow-state-#{id}-filter"'
               )
    end

    assert html =~ ~s(<fieldset class="flow-filter-time-group">)
    assert html =~ "Updated time (UTC)"
  end

  test "an exact lane scope leads with FIFO evidence and discloses secondary summaries" do
    for name <- ["queued", "all"] do
      data = states_data(%{type: "invoices", state: name, partition_key: "scope"})
      data = %{data | fifo_lanes: [%{type: "invoices", state: name, partition_key: "scope"}]}
      html = Dashboard.render_flow_states_page(data)

      assert position(html, ~s(data-live-component="flow_fifo_lanes")) <
               position(html, ~s(data-live-component="flow_states_table"))

      assert html =~ ~s(<details class="dashboard-disclosure" id="flow-state-summaries">)
      assert length(Regex.scan(~r/data-live-component="flow_fifo_lanes"/, html)) == 1
      assert length(Regex.scan(~r/data-live-component="flow_states_table"/, html)) == 1
    end
  end

  test "broader States views lead with the state table and disclose the repeated pressure chart" do
    html = Dashboard.render_flow_states_page(states_data(%{type: "invoices"}))

    assert position(html, ~s(data-live-component="flow_states_table")) <
             position(html, ~s(data-live-component="flow_fifo_lanes"))

    assert html =~ ~s(<details class="dashboard-disclosure" id="flow-state-pressure">)
  end

  test "parallel and empty scopes do not lead with an empty FIFO inspector" do
    html =
      Dashboard.render_flow_states_page(
        states_data(%{type: "parallel", state: "queued", partition_key: "scope"})
      )

    assert position(html, ~s(data-live-component="flow_states_table")) <
             position(html, ~s(data-live-component="flow_fifo_lanes"))
  end

  test "long logical steps wrap at separators while preserving escaped full identifiers" do
    html =
      Records.render_flow_recent_records([
        %{id: "run", type: "type", state: "inventory_<img>", partition_key: "scope"}
      ])

    assert html =~ "inventory_<wbr>&lt;img&gt;"
    assert html =~ ~s(title="Workflow state: inventory_&lt;img&gt;")
    refute html =~ "<img>"
  end

  test "schedule disclosure has a native marker and a stable label in open and closed states" do
    for data <- [%{}, %{draft: %{"id" => "draft"}}] do
      html = FlowSchedules.render_flow_schedule_create_form(data)
      assert html =~ ~s(<details class="dashboard-disclosure" id="flow-schedule-create-panel")
      assert html =~ "<summary>Create durable schedule</summary>"
      refute html =~ "click to expand"
      refute html =~ "list-style: none"
    end

    assert FlowSchedules.render_flow_schedule_create_form(%{draft: %{}}) =~
             ~s(id="flow-schedule-create-panel" open)
  end

  test "query field hints are concise without losing scope or all-state semantics" do
    filters = %{kind: "list", type: "type", state: nil}

    assert FlowQueryControls.render_flow_query_type_field(filters) =~
             "Filters records, not permissions."

    assert FlowQueryControls.render_flow_query_state_field(filters) =~
             "Empty includes all states."

    assert FlowQueryControls.render_flow_query_dynamic_script() =~ "Empty includes all states."
  end

  test "successful query counts stay in the result header while errors retain their alert" do
    result = %{status: :ok, command: "FLOW.QUERY", message: "1 row(s)", rows: []}
    html = FlowComponents.render_flow_query_result(%{result: result})
    assert html =~ ~s'<span class="flow-query-result-count">1 row(s)</span>'
    refute html =~ ~s(class="flow-alert flow-alert-ok")

    error =
      FlowComponents.render_flow_query_result(%{
        result: %{result | status: :error, message: "Denied <&"}
      })

    assert error =~ ~s(class="flow-alert flow-alert-error")
    assert error =~ "Denied &lt;&amp;"
  end

  defp state(mode, running) do
    %{
      type: "invoices",
      state: "queued",
      mode: mode,
      due_now: 9,
      running: running,
      count: 9,
      expired_leases: 0,
      oldest_due_ms: 1_000
    }
  end

  defp states_data(filters) do
    %{
      filters: Map.merge(%{type: nil, state: nil, partition_key: nil, limit: 40}, filters),
      states: [],
      fifo_lanes: [],
      records: [],
      available_types: [],
      available_states: [],
      total_sampled: 0,
      filtered_sampled: 0,
      sample_limit: 400,
      limit: 40
    }
  end

  defp position(html, text), do: elem(:binary.match(html, text), 0)
end
