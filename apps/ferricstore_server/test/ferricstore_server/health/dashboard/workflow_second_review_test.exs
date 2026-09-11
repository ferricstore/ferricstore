defmodule FerricstoreServer.Health.Dashboard.WorkflowSecondReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Flow.{Browse, Detail, Sample}
  alias FerricstoreServer.Health.Dashboard.Render.{FlowDetail, FlowFilters, FlowOverview}
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.{Lineage, Records, Signals}
  alias FerricstoreServer.Health.Dashboard.LivePayload

  test "due collection orders the bounded scoped sample by execution time" do
    type = "due-review-#{System.unique_integer([:positive])}"
    partition = " tenant "
    now = System.system_time(:millisecond)

    for {id, run_at} <- [
          {"late", now - 10_000},
          {"old", now - 20_000},
          {"future-late", now + 90_000},
          {"future-soon", now + 60_000}
        ] do
      assert :ok =
               FerricStore.flow_create(type <> id,
                 type: type,
                 partition_key: partition,
                 run_at_ms: run_at
               )
    end

    data = Browse.collect_due_page(type: type, partition_key: partition)
    assert Enum.map(data.due_now, & &1.id) == [type <> "old", type <> "late"]
    assert Enum.map(data.scheduled, & &1.id) == [type <> "future-soon", type <> "future-late"]
    assert data.sample_limit == 400
  end

  test "worker selection narrows only running records and survives live URLs" do
    type = "worker-review-#{System.unique_integer([:positive])}"
    partition = " partition "

    for worker <- [" first ", " second "] do
      assert :ok = FerricStore.flow_create(type <> worker, type: type, partition_key: partition)

      assert {:ok, [_]} =
               FerricStore.flow_claim_due(type,
                 partition_key: partition,
                 worker: worker,
                 lease_ms: 60_000,
                 limit: 1
               )
    end

    opts =
      Browse.scope_opts_from_query(
        URI.encode_query(%{type: type, partition_key: partition, worker: " first "})
      )

    data = Browse.collect_workers_page(opts)
    assert Enum.map(data.running_records, & &1.lease_owner) == [" first "]
    assert length(data.workers) == 2

    assert URI.decode_query(URI.parse(Browse.scope_live_url("workers", data)).query)["worker"] ==
             " first "
  end

  test "live detail publishes changing version without rewriting reviewed action forms" do
    id = " live-review-#{System.unique_integer([:positive])} "
    partition = " tenant "

    assert :ok =
             FerricStore.flow_create(id,
               type: "live-review",
               partition_key: partition,
               payload: "literal value"
             )

    path =
      URI.encode(id, &URI.char_unreserved?/1) <>
        "?" <> URI.encode_query(%{partition_key: partition})

    assert {:ok, before} = LivePayload.live_payload("flow/" <> path)
    assert before.action_snapshot.available
    assert :ok = FerricStore.flow_signal(id, signal: "review", partition_key: partition)
    assert {:ok, after_payload} = LivePayload.live_payload("flow/" <> path)
    refute before.action_snapshot.version == after_payload.action_snapshot.version
    refute Map.has_key?(after_payload.components, "flow_actions")

    assert {:ok, current} = FerricStore.flow_get(id, partition_key: partition)
    query = URI.encode_query(%{flow: id, partition_key: partition, ref: current.payload_ref})
    assert {:ok, value} = LivePayload.live_payload("flow/value?" <> query)
    assert value.status == "ok"
    assert Jason.encode!(value) =~ "literal value"

    assert {:ok, missing} =
             LivePayload.live_payload(
               "flow/not-visible-review-#{System.unique_integer([:positive])}"
             )

    assert missing.action_snapshot == %{available: false, version: nil, state: nil}
  end

  test "inactive time modes cannot silently override custom or all-time filtering" do
    custom =
      "time_mode=custom&range=1h&from_ms=1000&to_ms=2000"
      |> Browse.states_opts_from_query()
      |> Sample.state_filters_from_opts()

    assert custom.from_ms == 1_000
    assert custom.to_ms == 2_000
    assert custom.range == nil

    all =
      "time_mode=all&range=1h&from_ms=bad"
      |> Browse.states_opts_from_query()
      |> Sample.state_filters_from_opts()

    assert all.from_ms == nil
    assert all.range == nil
    assert all.errors == %{}
    html = FlowFilters.render_flow_type_filter(%{filters: %{state: " literal state "}})
    assert html =~ ~s(value=" literal state " selected)
  end

  test "scope identifiers retain every literal byte through parsing and navigation" do
    for value <- [" tenant ", " ", "all", "<&"] do
      assert Sample.normalize_partition_query(value) == value
      assert Sample.normalize_type_filter(value) == value
      assert Sample.normalize_state_filter(value) == value
      query = URI.encode_query(%{type: value, state: value, partition_key: value})
      filters = query |> Browse.states_opts_from_query() |> Sample.state_filters_from_opts()
      assert filters.type == value
      assert filters.state == value
      assert filters.partition_key == value
    end

    assert Sample.normalize_partition_query("") == nil
    assert Sample.normalize_type_filter(nil) == nil
  end

  test "due detail describes evidence without asserting claimability" do
    html = FlowDetail.render_flow_diagnostic_hero(%{record: record()})
    assert html =~ "Due time reached"
    refute html =~ "Ready for worker claim"
  end

  test "reviewed action version is separate from live snapshot metadata" do
    data = %{record: record(version: 7), history: []}
    html = FlowDetail.render_flow_actions(data)
    assert html =~ ~s(data-flow-action-snapshot-version="7")
    assert html =~ ~s(data-flow-action-stale)
    assert html =~ "Refresh and review"
    assert html =~ ~s(name="expected_version" value="7")
    assert Detail.action_snapshot(data) == %{available: true, version: "7", state: "queued"}
    assert Detail.action_snapshot(%{record: nil}) == %{available: false, version: nil, state: nil}
  end

  test "rendered route contract keeps exact state and millisecond bounds on compatible routes" do
    filters = %{
      type: " type ",
      partition_key: " partition ",
      state: " queued ",
      range: "1h",
      from_ms: 1_000_001,
      to_ms: 2_000_999
    }

    html = FlowOverview.render_flow_scope_contract(%{filters: filters})
    assert html =~ "data-dashboard-workflow-scope"
    query = route_query(html, "/dashboard/flow/query")
    assert query["type"] == " type "
    assert query["partition_key"] == " partition "
    assert query["state"] == " queued "
    assert query["from_ms"] == "1000001"
    assert query["to_ms"] == "2000999"
    assert route_query(html, "/dashboard/flow/states")["range"] == "1h"
    refute Map.has_key?(route_query(html, "/dashboard/flow/due"), "state")
  end

  test "relative and custom modes apply active bounds with or without enhancement" do
    filters = Sample.state_filters_from_opts(range: "1h")
    html = FlowFilters.render_flow_type_filter(%{filters: filters})
    assert html =~ ~s(name="time_mode")
    assert html =~ ~s(value="relative" selected)
    refute input_tag(html, "from") =~ "disabled"
    refute input_tag(html, "to") =~ "disabled"
    assert html =~ "flow-state-time-mode"
    assert html =~ "input.disabled ="
    assert html =~ "Time mode controls which bounds are applied"

    custom =
      Sample.state_filters_from_opts(
        time_mode: "custom",
        range: "1h",
        from_ms: 1_000,
        to_ms: 2_000
      )

    assert custom.range == nil
    assert custom.from_ms == 1_000
    assert custom.to_ms == 2_000
  end

  test "recent record cells explicitly distinguish workflow and stored states" do
    html =
      Records.render_flow_recent_records([
        record(state: "running", run_state: "inventory_allocated")
      ])

    assert html =~ "Workflow / stored state"
    assert html =~ "inventory_<wbr>allocated"
    assert html =~ "Stored state: running"
  end

  test "state and worker aggregates link to bounded affected-record views" do
    state = %{
      type: " orders ",
      state: "queued",
      count: 3,
      due_now: 2,
      running: 0,
      expired_leases: 0,
      oldest_due_ms: 5_000
    }

    html = Records.render_flow_states_table([state], 3, 3, 400, %{partition_key: " tenant "})
    assert html =~ "/dashboard/flow/states?"
    assert html =~ "partition_key=+tenant+"
    assert html =~ "type=+orders+"
    assert html =~ "#flow-recent-records"

    workers =
      Records.render_flow_workers([
        %{worker: " worker ", running: 2, expired: 1, oldest_lease_ms: 1}
      ])

    assert workers =~ "/dashboard/flow/workers?worker=+worker+"
  end

  test "overview metrics retain scope in investigation links" do
    html =
      FlowOverview.render_flow_overview(%{running: 2, due_now_sampled: 3}, 3, 400, %{
        type: " t ",
        partition_key: " p "
      })

    assert html =~ ~s(href="/dashboard/flow/workers?type=+t+&amp;partition_key=+p+")
    assert html =~ ~s(href="/dashboard/flow/due?type=+t+&amp;partition_key=+p+")
  end

  test "due and signal identities visibly include the partition" do
    due =
      Records.render_flow_due_records("Due Now", [record(partition_key: " tenant<& ")], 1, 400)

    assert due =~ "tenant&lt;&amp;"
    assert due =~ "Oldest due first"

    signals =
      Signals.render_flow_signal_row(
        %{id: "id", type: "orders", partition_key: " tenant<& ", event_id: "1-0"},
        :page
      )

    assert signals =~ "tenant&lt;&amp;"
  end

  test "lineage relationships use the known partition for bounded relationship queries" do
    html =
      Lineage.render_flow_lineage_rows([
        record(parent_flow_id: " parent ", root_flow_id: " root ", correlation_id: " corr ")
      ])

    for {mode, id} <- [{"parent", " parent "}, {"root", " root "}, {"correlation", " corr "}] do
      query =
        URI.encode_query(%{"id" => id, "mode" => mode, "partition_key" => "tenant"})
        |> String.replace("&", "&amp;")

      assert html =~ query
    end

    refute Lineage.render_flow_lineage_rows([
             record(partition_key: nil, parent_flow_id: "parent")
           ]) =~ "mode=parent"
  end

  test "missing workflow lookup can be corrected without losing its literal scope" do
    html =
      FlowDetail.render_flow_detail(%{
        record: nil,
        id: " missing<& ",
        partition_key: " tenant ",
        record_status: :not_found
      })

    assert html =~ ~s(name="id" value=" missing&lt;&amp; ")
    assert html =~ ~s(name="partition_key" value=" tenant ")
    assert html =~ "Open workflow"
    assert html =~ "/dashboard/flow/states?partition_key=+tenant+"
  end

  test "lookup and page-local scope are separate actions on operational pages" do
    html =
      FlowOverview.render_flow_context_tools(
        %{filters: %{type: " orders ", partition_key: " tenant "}},
        "flow_due"
      )

    assert html =~ "Open workflow"
    assert html =~ ~s(aria-label="Workflow scope")
    assert html =~ ~s(action="/dashboard/flow/due")
    assert html =~ "Apply scope"
    assert html =~ ~s(name="id")
    [lookup] = Regex.run(~r/<form[^>]*aria-label="Flow lookup".*?<\/form>/s, html)
    assert lookup =~ "required"
  end

  test "state urgency leads secondary counts and sections have semantic headings" do
    html = Records.render_flow_states_table([], 0, 0, 400, %{})
    assert html =~ ~s(<h2 class="section-title">Flow States)
    assert position(html, "<th>Hint</th>") < position(html, "<th>Sample Count</th>")
    assert position(html, "<th>Oldest Due</th>") < position(html, "<th>Sample Count</th>")

    assert Records.render_flow_recent_records([]) =~
             ~s(<h2 class="section-title" id="flow-recent-records")
  end

  defp record(overrides \\ []) do
    Map.merge(
      %{
        id: "id",
        type: "orders",
        state: "queued",
        partition_key: "tenant",
        version: 3,
        run_at_ms: 1,
        updated_at_ms: 2,
        attempts: 0
      },
      Map.new(overrides)
    )
  end

  defp route_query(html, route) do
    [_, href] = Regex.run(~r/data-dashboard-route="#{Regex.escape(route)}" href="([^"]+)"/, html)

    href
    |> String.replace("&amp;", "&")
    |> URI.parse()
    |> Map.fetch!(:query)
    |> then(&URI.decode_query(&1 || ""))
  end

  defp input_tag(html, name), do: Regex.run(~r/<input\b[^>]*name="#{name}"[^>]*>/, html) |> hd()
  defp position(html, text), do: :binary.match(html, text) |> elem(0)
end
