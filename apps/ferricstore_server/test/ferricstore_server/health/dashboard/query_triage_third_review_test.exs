defmodule FerricstoreServer.Health.Dashboard.QueryTriageThirdReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Flow.QueryWorkbench
  alias FerricstoreServer.Health.Dashboard.Flow.{Browse, Sample}
  alias FerricstoreServer.Health.Dashboard.Render.FlowFilters
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.{Records, Signals}

  test "explicit Guided import preserves the complete Builder plan without executing it" do
    data =
      Dashboard.collect_flow_query_page(
        kind: "search",
        inspect: true,
        type: "email",
        partition_key: "tenant-a",
        state: "running",
        run_state: "queued",
        attribute_key: "priority",
        attribute_value_type: "integer",
        attribute_value: "7",
        state_meta_state: "queued",
        state_meta_key: "flag",
        state_meta_value_type: "boolean",
        state_meta_value: "false",
        from_ms: 1_000,
        to_ms: 2_000,
        limit: 7,
        rev: false
      )

    assert data.guided_import.fql =~ "LIMIT 7"
    assert data.guided_import.fql =~ "updated_at_ms ASC"
    params = Jason.decode!(data.guided_import.params_json)
    assert params["state"] == "running"
    assert params["run_state"] == "queued"
    assert params["attribute_value"] == 7
    assert params["state_meta_value"] == false
    assert params["partition_key"] == "tenant-a"
    assert data.guided_import.fql =~ "BETWEEN"
    html = Dashboard.render_flow_query_page(data)
    assert html =~ "Import submitted Guided query"
    assert html =~ "window.confirm"
    assert html =~ "Guided draft changed. Submit it before importing."
  end

  test "Raw FQL is an independent draft rather than a partial Guided translation" do
    filters = %{type: "email", partition_key: "tenant-a", state: "failed", limit: 7, rev: false}

    assert QueryWorkbench.default_form(filters) == QueryWorkbench.default_form()

    html = Dashboard.render_flow_query_page(Dashboard.collect_flow_query_page(inspect: true))
    assert html =~ ">Guided draft</button>"
    assert html =~ ">Raw FQL draft</button>"
    assert html =~ "Independent drafts"
    assert html =~ "Switching drafts does not transfer changes"
  end

  test "expired lease hint states evidence without prescribing reclaim" do
    html = Records.flow_state_operational_hint(%{expired_leases: 1})
    assert html =~ "Lease expired"
    refute html =~ "need reclaim"
  end

  test "States and Workers prioritize exceptions and retain named distribution order" do
    now = System.system_time(:millisecond)

    records = [
      %{id: "routine-1", type: "routine", state: "queued", run_at_ms: now - 100},
      %{id: "routine-2", type: "routine", state: "queued", run_at_ms: now - 100},
      %{
        id: "expired",
        type: "incident",
        state: "running",
        lease_owner: "incident",
        lease_expires_at_ms: now - 1
      },
      %{
        id: "busy-1",
        type: "busy",
        state: "running",
        lease_owner: "busy",
        lease_expires_at_ms: now + 60_000
      },
      %{
        id: "busy-2",
        type: "busy",
        state: "running",
        lease_owner: "busy",
        lease_expires_at_ms: now + 60_000
      }
    ]

    assert hd(Sample.state_summaries(records)).type == "incident"
    assert hd(Sample.worker_summaries(records)).worker == "incident"
    assert hd(Sample.worker_summaries(records, "distribution")).worker == "busy"
    assert hd(Sample.state_summaries(records, "distribution")).type == "routine"

    filters =
      "sort=distribution" |> Browse.states_opts_from_query() |> Sample.state_filters_from_opts()

    assert filters.sort == "distribution"
    assert FlowFilters.render_flow_type_filter(%{filters: filters}) =~ "Attention first"
    assert Records.render_flow_workers([], %{sort: "distribution"}) =~ "Distribution"
  end

  test "States puts exception columns before routine counts" do
    html = Records.render_flow_states_table([], 0, 0, 400, %{})
    assert html =~ ~s(class="flow-states-table")
    assert html =~ "Scroll for distribution columns"
    {failed, _} = :binary.match(html, "<th>Failed")
    {count, _} = :binary.match(html, "<th>Sample Count")
    assert failed < count
  end

  test "time fields work without JavaScript and enhancement hides inactive modes" do
    html =
      FlowFilters.render_flow_type_filter(%{
        filters: Sample.state_filters_from_opts(time_mode: "all")
      })

    refute html =~ ~s(data-flow-time-mode="relative" hidden)
    refute html =~ ~s(data-flow-time-mode="custom" hidden)
    assert html =~ "Time mode controls which bounds are applied"
    assert html =~ "label.hidden = label.dataset.flowTimeMode !== mode.value"
  end

  test "Due renders captured relative timing and links its known FIFO blocker" do
    type = "triage-fifo-#{System.unique_integer([:positive])}"
    partition = "same lane"
    assert {:ok, _} = FerricStore.flow_policy_set(type, states: %{"queued" => [mode: :fifo]})

    for id <- ["blocker", "waiting"] do
      assert :ok =
               FerricStore.flow_create(type <> id,
                 type: type,
                 partition_key: partition,
                 run_at_ms: 1
               )
    end

    assert {:ok, [_]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker",
               lease_ms: 60_000,
               limit: 1
             )

    data = Browse.collect_due_page(type: type, partition_key: partition)
    assert is_integer(data.generated_at_ms)
    waiting = Enum.find(data.due_now, &(&1.id == type <> "waiting"))
    assert waiting.dashboard_fifo_blocker == type <> "blocker"
    assert waiting.dashboard_snapshot_ms == data.generated_at_ms
    html = Records.render_flow_due_records("Due Now", [waiting], 2, 400)
    assert html =~ "Observed FIFO blocker"
    assert html =~ "/dashboard/flow/#{type}blocker?partition_key=same+lane"
    assert html =~ "overdue"

    future = Map.merge(waiting, %{run_at_ms: 65_000, dashboard_snapshot_ms: 5_000})
    assert Records.render_flow_due_records("Scheduled Future", [future], 2, 400) =~ "in 1.0m"
  end

  test "Signals distinguishes results, candidate coverage and history bounds" do
    filters = %{scan_history: true, limit: 40}

    scan = %{
      requested: true,
      sampled_flows: 30,
      inspected_flows: 16,
      completed_flows: 15,
      failed_flows: 1,
      truncated: true,
      history_limited_flows: 3,
      matched_events: 4,
      result_truncated: false,
      history_limit: 25
    }

    html =
      FlowFilters.render_flow_signals_filter(%{
        filters: filters,
        signal_scan: scan,
        signals: [%{signal: "go"}],
        total_sampled: 100,
        filtered_sampled: 30
      })

    assert html =~ "30 candidate workflows"
    assert html =~ "15 histories read"
    assert html =~ "16 attempted"
    assert html =~ "Partial coverage"
    assert html =~ "Narrow Type, Partition, or Flow ID"
    assert html =~ "3 histories reached the 25-event bound"
    table = Signals.render_flow_signals_table([%{signal: "go"}], 100, 30, 400, filters)
    assert table =~ "1 signal event"
    refute table =~ "30 / 100 sampled"
  end

  test "Signal coverage records existing scan and event limits without expanding reads" do
    type = "triage-signals-#{System.unique_integer([:positive])}"

    for suffix <- ["a", "b"] do
      assert :ok = FerricStore.flow_create(type <> suffix, type: type, partition_key: "tenant")
    end

    previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
    previous_max = Application.get_env(:ferricstore, :flow_dashboard_signal_scan_max_flows)

    on_exit(fn ->
      restore_env(:flow_dashboard_flow_history_fun, previous_history)
      restore_env(:flow_dashboard_signal_scan_max_flows, previous_max)
    end)

    pid = self()
    Application.put_env(:ferricstore, :flow_dashboard_signal_scan_max_flows, 1)

    Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn id, opts ->
      send(pid, {:history_read, id, opts})
      {:ok, for(n <- 1..25, do: {"#{n}-0", %{"action" => "signaled", "signal" => "ready"}})}
    end)

    data =
      Dashboard.collect_flow_signals_page(
        type: type,
        partition_key: "tenant",
        scan_history: true,
        limit: 1
      )

    assert length(data.signals) == 1
    assert data.signal_scan.sampled_flows == 2
    assert data.signal_scan.inspected_flows == 1
    assert data.signal_scan.completed_flows == 1
    assert data.signal_scan.history_limited_flows == 1
    assert data.signal_scan.matched_events == 25
    assert data.signal_scan.result_truncated
    assert data.signal_scan.truncated
    assert_received {:history_read, _id, opts}
    assert opts[:count] == 25
    assert opts[:values] == false
    refute_received {:history_read, _id, _opts}
  end
end
