defmodule FerricstoreServer.Health.Dashboard.WorkflowReviewRegressionsTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Render.FlowCharts
  alias FerricstoreServer.Health.Dashboard.Render.FlowComponents
  alias FerricstoreServer.Health.Dashboard.Render.FlowFilters
  alias FerricstoreServer.Health.Dashboard.Render.FlowOverview
  alias FerricstoreServer.Health.Dashboard.Render.FlowQueryControls
  alias FerricstoreServer.Health.Dashboard.Render.FlowSchedules
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records

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
    for {field, hint} <- [expired_leases: "leases need reclaim", failed: "terminal failed"] do
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
                 ~s'<label class="flow-filter-field" for="flow-state-#{id}-filter">\\s*<span>[^<]+</span>\\s*<(?:input|select)[^>]+id="flow-state-#{id}-filter"'
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
    assert html =~ ~s(title="inventory_&lt;img&gt;")
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
