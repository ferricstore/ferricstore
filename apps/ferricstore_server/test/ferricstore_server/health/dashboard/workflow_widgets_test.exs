defmodule FerricstoreServer.Health.Dashboard.WorkflowWidgetsTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.FlowRecord
  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Layout
  alias FerricstoreServer.Health.Dashboard.Render.FlowCharts
  alias FerricstoreServer.Health.Dashboard.Render.FlowDetail
  alias FerricstoreServer.Health.Dashboard.Render.FlowHistory
  alias FerricstoreServer.Health.Dashboard.Render.FlowQueryResults
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Projection
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records
  alias FerricstoreServer.Health.Dashboard.Render.FlowComponents
  alias FerricstoreServer.Health.Dashboard.Flow.Query

  describe "execution-first composition" do
    test "guided run tables show only returned fields, with scoped detail links" do
      result = %{
        command: "FLOW.QUERY",
        rows: [
          %{
            id: "run<&",
            type: "email",
            state: "running",
            run_state: "send_email",
            partition_key: "scope&1",
            updated_at_ms: 1_000
          }
        ]
      }

      html = FlowQueryResults.render_flow_query_table(result)
      refute html =~ ~s(>Worker</th>)
      refute html =~ ~s(>Values</th>)
      refute html =~ ~s(>none</span>)
      assert html =~ "run&lt;&amp;"
      assert html =~ "partition_key=scope%261"
      assert html =~ ~s(>Workflow state</th>)
      assert html =~ "send_email"
      assert length(Regex.scan(~r/<th scope=/, html)) == 5
      assert length(Regex.scan(~r/<td[ >]/, html)) == 5
      assert FlowQueryResults.render_flow_query_table(%{result | rows: []}) =~ ~s(colspan="5")

      history = FlowQueryResults.render_flow_query_table(%{command: "FLOW.HISTORY", rows: []})
      assert history =~ ~s(>Worker</th>)
      assert history =~ ~s(>Values</th>)
    end

    test "query provenance is captured, escaped, and absent before any result" do
      data = %{
        result: %{status: :ok},
        generated_at_ms: 1_000,
        filters: %{
          kind: "list",
          type: "email<script>",
          partition_key: "scope&1",
          state: nil,
          limit: 40
        }
      }

      html = FlowQueryResults.render_flow_query_provenance(data)
      assert html =~ "email&lt;script&gt;"
      assert html =~ "scope&amp;1"
      assert html =~ "All states"
      assert html =~ ~s(datetime="1970-01-01T00:00:01.000Z")
      assert html =~ ~s(data-flow-query-draft-status hidden)
      assert html =~ "Executed inputs"

      assert FlowQueryResults.render_flow_query_provenance(%{data | result: %{status: :idle}}) ==
               ""

      raw =
        Map.put(data, :workbench, %{
          mode: :advanced,
          fql: "FROM runs <script>",
          params_json: ~s({"x":"<&"})
        })

      html = FlowQueryResults.render_flow_query_provenance(raw)
      assert html =~ "FROM runs &lt;script&gt;"
      assert html =~ "Raw FQL"
      refute html =~ "All states"

      for kind <- ~w(terminals failures stuck history) do
        scoped = %{data | filters: Map.put(data.filters, :kind, kind)}
        refute FlowQueryResults.render_flow_query_provenance(scoped) =~ "All states"
      end
    end

    test "the run list precedes secondary workload tables" do
      html =
        Dashboard.render_flow_page(%{
          summary: %{},
          types: [],
          workers: [],
          records: [],
          total_sampled: 0,
          filtered_sampled: 0,
          sample_limit: 400,
          projection: Projection.default_flow_projection_health()
        })

      assert position(html, ~s(data-live-component="flow_recent_records")) <
               position(html, ~s(data-live-component="flow_state_breakdown"))

      assert html =~ ~s(<details class="dashboard-disclosure")
    end

    test "workflow identity is compact and metadata is secondary to history" do
      data = %{
        id: "workflow-1",
        record: record("ready"),
        waiting_reason: "waiting in ready",
        history: [{"1000-1", %{"event" => "created", "state" => "ready"}}]
      }

      html = Dashboard.render_flow_detail_page(data)

      assert html =~ ~s(<dl class="flow-execution-summary")
      assert html =~ "Workflow state"

      assert position(html, ~s(data-live-component="flow_history")) <
               position(html, ~s(data-live-component="flow_detail_metadata"))

      refute FlowDetail.render_flow_detail(data) =~ ~s(class="flow-card")
    end

    test "event details have a dedicated linked inspector without fetching payloads" do
      html =
        FlowHistory.render_flow_history_timeline(
          [
            {"1000-1", %{"event" => "created", "state" => "ready"}}
          ],
          :ok,
          nil
        )

      assert html =~ ~s(class="flow-journal-workspace")
      assert html =~ ~s(<aside class="flow-journal-inspector")
      assert html =~ ~s(aria-controls="journal-inspector-flow-event-MTAwMC0x")
      assert html =~ ~s(id="journal-inspector-flow-event-MTAwMC0x")
      assert html =~ ~s(<aside class="flow-journal-inspector" hidden)
      refute html =~ "No event selected"
    end

    test "empty, single-event, and simultaneous histories do not invent a timing chart" do
      for history <- [[], history([1_000]), history([1_000, 1_000])] do
        assert FlowCharts.render_flow_timeline_chart(history) == ""
      end
    end

    test "measurable timing retains event links without nested chart framing" do
      html = FlowCharts.render_flow_timeline_chart(history([1_000, 2_000]))

      assert html =~ "Event intervals"
      assert html =~ "flow-step-waterfall-row"
      assert html =~ "#journal-flow-event-"
      refute html =~ ~s(class="chart-card")
      refute html =~ "Step durations"
      refute html =~ "events on this page"
    end

    test "waterfall distinguishes missing end evidence from measured zero time" do
      rows =
        FlowCharts.flow_timeline_duration_rows([
          %{time_ms: 1_000},
          %{time_ms: 1_000},
          %{time_ms: 2_000}
        ])

      assert Enum.map(rows, & &1.duration_ms) == [0, 1_000, nil]
      assert FlowCharts.flow_timeline_duration_ms(%{time_ms: 2}, %{time_ms: 1}) == nil
      assert FlowCharts.flow_timeline_duration_ms(%{time_ms: nil}, %{time_ms: 1}) == nil

      html = FlowCharts.render_flow_timeline_chart(history([1_000, 1_000, 2_000]))
      assert html =~ "No next event"
      assert html =~ ">0ms</span>"
      assert html =~ ">1.0s</span>"
      assert length(Regex.scan(~r/class="flow-step-waterfall-bar /, html)) == 2
      assert FlowCharts.flow_step_waterfall_range(rows).total_ms == 1_000
      assert FlowCharts.flow_timeline_duration_rows([]) == []
    end

    test "timing rendering stays bounded and identifies the truncated history window" do
      html = FlowCharts.render_flow_timeline_chart(history(Enum.to_list(1..100)))
      assert length(Regex.scan(~r/class="flow-step-waterfall-row"/, html)) == 80
      assert html =~ "Latest 80 of 100 events on this page"
    end

    test "confirmed complete history has one event count and no inactive paging controls" do
      page = %{has_older: false, has_newer: false, count: 50}
      html = FlowHistory.render_flow_history_timeline(history([1_000]), :ok, page)

      assert html =~ "1 event on this page"
      refute html =~ "1 events"
      refute html =~ "flow-history-controls"
      assert html =~ "Raw Events"
    end

    test "paging controls remain for partial, cursor, custom-size, and failed history views" do
      complete = %{has_older: false, has_newer: false, count: 50}

      for {page, status} <- [
            {Map.put(complete, :has_older, true), :ok},
            {Map.put(complete, :newer_url, "?history_after=1-1"), :ok},
            {Map.put(complete, :before, "1-1"), :ok},
            {Map.put(complete, :after_cursor, "1-1"), :ok},
            {%{complete | count: 100}, :ok},
            {complete, :timeout},
            {%{}, :ok}
          ] do
        html = FlowHistory.render_flow_history_timeline(history([1_000]), status, page)
        assert html =~ "flow-history-controls", inspect({page, status})
      end
    end

    test "history pagination is shared by the journal and raw events views" do
      history = for n <- 1..50, do: {"#{n * 1000}-1", %{"event" => "created", "state" => "ready"}}
      page = %{id: "workflow-1", partition_key: "region-1", older_url: "?history_after=cursor-1"}
      html = FlowHistory.render_flow_history_timeline(history, :ok, page)

      assert position(html, ~s(class="flow-history-controls")) <
               position(html, ~s(id="journal-panel-tree"))

      assert length(Regex.scan(~r/class="flow-history-controls"/, html)) == 1
      assert html =~ "50 events on this page"
      assert html =~ ~s(href="?history_after=cursor-1">Older)
      assert html =~ "partition_key=region-1"
    end

    test "the subpage shell retains product and connection identity" do
      html = Layout.render_subpage_header("Workflow runs")
      assert html =~ "FerricStore"
      assert html =~ "Workflow runs"
      assert html =~ "data-dashboard-instance"
      assert Layout.dashboard_live_script() =~ "window.location.host"
    end

    test "query controls use one workspace and keep active metadata predicates visible" do
      data = Query.collect_query_page()
      filters = Map.merge(data.filters, %{kind: "search", state_meta_key: "risk"})
      html = FlowComponents.render_flow_query_controls(%{data | filters: filters})

      assert html =~ "data-flow-query-workbench"
      assert html =~ ~s(<details class="flow-query-advanced" open>)
      assert html =~ ~s(name="state_meta_key" value="risk")
      assert html =~ ~s(data-flow-query-mode="guided")
      assert html =~ ~s(data-flow-query-mode="advanced")
      refute html =~ ~s(style=")
      refute html =~ "(click to toggle)"
    end

    test "query records lead diagnostics and charts remain available on demand" do
      result = %{
        status: :ok,
        message: "2 row(s)",
        rows: [record("ready"), record("failed")],
        quality: %{exactness: "projected_exact"},
        usage: %{range_seeks: 1},
        visualization: %{
          scope: :current_page,
          row_count: 2,
          charts: [
            %{
              kind: :category,
              field: "state",
              values: [%{label: "ready", count: 1}, %{label: "failed", count: 1}]
            }
          ]
        }
      }

      html = FlowComponents.render_flow_query_result(%{result: result})

      assert position(html, ~s(class="flow-query-table-wrap")) <
               position(html, ~s(class="flow-query-metadata"))

      assert position(html, ~s(class="flow-query-table-wrap")) <
               position(html, ~s(class="flow-query-visualization"))

      assert html =~ ~s(<details class="flow-query-visualization">)
      assert html =~ "projected exact"
      assert html =~ "Range seeks"
      assert html =~ "Current page"
    end
  end

  describe "durable status semantics" do
    for state <- ~w(payment_failed_review signal_dispatch fail complete canceled) do
      test "does not invent a terminal or suspended status for #{state}" do
        record = record(unquote(state))
        html = render_status(record)

        refute html =~ "Terminal Failed"
        refute html =~ "Workflow completed successfully"
        refute html =~ "explicitly cancelled"
        refute html =~ "Waiting for Signal"
        refute html =~ "Signal action below to resume"
        assert html =~ "Due time reached"
        refute html =~ "Ready for worker claim"
      end
    end

    test "presentation text cannot override a scheduled durable state" do
      html =
        render_status(record("payment_review", run_at_ms: now() + 60_000),
          waiting_reason: "failed signal delivery"
        )

      assert html =~ "Scheduled"
      refute html =~ "Terminal Failed"
      refute html =~ "hero-failed"
      refute html =~ "Waiting for Signal"
    end

    test "a valid lease reports ownership without claiming live worker activity" do
      html =
        render_status(record("running", worker: "worker-1", lease_expires_at_ms: now() + 60_000))

      assert html =~ "Leased to worker-1"
      assert html =~ "Lease expires in"
      refute html =~ "Executing (Worker:"
      refute html =~ "pulse-dot"
    end

    test "ordinary FIFO ordering is neutral, while an expired blocker needs attention" do
      lane = %{head_id: "head-1", head_status: "blocked by active flow"}
      html = render_status(record("ready"), fifo_lane: lane, state_mode: :fifo)

      assert html =~ "Waiting behind FIFO head"
      assert html =~ "head-1"
      refute html =~ "hero-blocked"

      expired = %{lane | head_status: "blocked by expired lease"}
      html = render_status(record("ready"), fifo_lane: expired, state_mode: :fifo)
      assert html =~ "FIFO head lease expired"
      assert html =~ "hero-blocked"
    end

    test "ready work alone does not create an incident widget" do
      assert FlowCharts.render_flow_issue_cards(25, 0, 0) == ""
      html = FlowCharts.render_flow_issue_cards(25, 2, 3)
      assert html =~ "Needs attention"
      assert html =~ "Expired leases"
      assert html =~ "Failed"
      refute html =~ "Due Now"
    end

    test "status values remain escaped" do
      html = render_status(record("running", worker: "<script>alert(1)</script>"))
      refute html =~ "<script>"
      assert html =~ "&lt;script&gt;"
    end

    test "the run list exposes logical state and an expired lease instead of a running badge" do
      html =
        Records.render_flow_recent_records([
          record("running", run_state: "payment_review", lease_expires_at_ms: now() - 60_000)
        ])

      assert html =~ "payment_review"
      assert html =~ ~s(class="badge badge-failed">expired lease)
      refute html =~ "pulse-dot-green"
      assert html =~ ~s(class="flow-runs-table")
    end
  end

  defp now, do: System.system_time(:millisecond)

  defp history(times) do
    times
    |> Enum.with_index(1)
    |> Enum.map(fn {time, sequence} ->
      {"#{time}-#{sequence}", %{"event" => "created", "state" => "ready"}}
    end)
  end

  defp position(html, text), do: elem(:binary.match(html, text), 0)

  defp record(state, fields \\ []) do
    Map.merge(
      %{id: "workflow-1", type: "orders", state: state, partition_key: "region-1", run_at_ms: 1},
      Map.new(fields)
    )
  end

  defp render_status(record, fields \\ []) do
    data = %{record: record, waiting_reason: FlowRecord.flow_waiting_reason(record)}
    FlowDetail.render_flow_diagnostic_hero(Map.merge(data, Map.new(fields)))
  end
end
