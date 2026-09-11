defmodule FerricstoreServer.Health.Dashboard.QuerySecondReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Flow.{QueryResult, QueryWorkbench}
  alias FerricstoreServer.Health.Dashboard.Render.{FlowQueryControls, FlowQueryResults}
  alias FerricstoreServer.Health.Endpoint

  @fql "FROM runs WHERE partition_key = @partition AND type = @type ORDER BY updated_at_ms DESC LIMIT 2 RETURN RECORDS"
  @params ~s({"partition":"customer-a","type":"invoice"})

  setup do
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    :ok
  end

  test "guided first and continuation pages keep the same compact projection" do
    form =
      QueryWorkbench.guided_form(
        @fql,
        Jason.decode!(@params),
        "kind=list&type=invoice&partition_key=customer-a"
      )

    first =
      QueryResult.success("FLOW.QUERY", response(true, cursor(1)))
      |> QueryWorkbench.attach_continuation(form)

    {:ok, prepared, continued_form} =
      QueryWorkbench.prepare(Map.put(post(form), "cursor", cursor(1)))

    {:flow_query, request} = prepared.ast

    continued =
      QueryResult.success("FLOW.QUERY", response(false, nil), request: request)
      |> QueryWorkbench.attach_continuation(continued_form)

    assert first.column_selectors == [:run_id, :type, :state, :run_state, :updated_at_ms]
    assert continued.column_selectors == first.column_selectors
    assert continued.columns == first.columns

    assert headers(FlowQueryResults.render_flow_query_table(first)) ==
             headers(FlowQueryResults.render_flow_query_table(continued))
  end

  test "advanced projection is not replaced by the guided table" do
    params = %{
      "fql" => String.replace(@fql, "RETURN RECORDS", "RETURN RECORDS (run_id, attribute.flag)"),
      "params_json" => @params
    }

    {:ok, prepared, form} = QueryWorkbench.prepare(params)
    {:flow_query, request} = prepared.ast

    result =
      QueryResult.success("FLOW.QUERY", response(true, cursor(1)), request: request)
      |> QueryWorkbench.attach_continuation(form)

    assert result.column_selectors == [:run_id, {:attribute, "flag"}]
  end

  test "last query page retains previous and first navigation without a count query" do
    {:ok, _, form} = QueryWorkbench.prepare(%{"fql" => @fql, "params_json" => @params})

    first =
      QueryResult.success("FLOW.QUERY", response(true, cursor(1)))
      |> QueryWorkbench.attach_continuation(form)

    assert first.navigation.page_number == 1
    assert first.navigation.previous == nil
    assert first.continuation.page_number == 2
    assert first.continuation.cursor_history == [nil]

    {:ok, _, second_form} = QueryWorkbench.prepare(post(first.continuation))

    second =
      QueryResult.success("FLOW.QUERY", response(true, cursor(2)))
      |> QueryWorkbench.attach_continuation(second_form)

    {:ok, _, third_form} = QueryWorkbench.prepare(post(second.continuation))

    third =
      QueryResult.success("FLOW.QUERY", response(false, nil))
      |> QueryWorkbench.attach_continuation(third_form)

    assert third.navigation.page_number == 3
    assert third.navigation.previous.cursor == cursor(1)
    assert third.navigation.previous.cursor_history == [nil]
    assert third.navigation.first.cursor == nil
    html = FlowQueryResults.render_flow_query_continuation(third)
    assert html =~ "Previous page"
    assert html =~ "First page"
    assert html =~ "Page 3"
    refute html =~ "Next page"

    {:ok, back, back_form} = QueryWorkbench.prepare(post(third.navigation.previous))
    assert elem(back.ast, 1).cursor == {:literal, :keyword, cursor(1)}
    assert back_form.page_number == 2
  end

  test "query navigation rejects malformed or oversized history before query preparation" do
    for history <- [
          "{}",
          "not json",
          Jason.encode!([42]),
          Jason.encode!(List.duplicate(cursor(1), 17)),
          String.duplicate(" ", 32_769)
        ] do
      assert {:error, _form, message} =
               QueryWorkbench.prepare(%{
                 "fql" => @fql,
                 "params_json" => @params,
                 "cursor_history" => history
               })

      assert message =~ "page history"
    end
  end

  test "query navigation retains bounded cursor history and a first-page exit" do
    history = Enum.map(1..16, &cursor/1)

    {:ok, _, form} =
      QueryWorkbench.prepare(%{
        "fql" => @fql,
        "params_json" => @params,
        "cursor" => cursor(17),
        "cursor_history" => Jason.encode!(history),
        "page_number" => "18"
      })

    result =
      QueryResult.success("FLOW.QUERY", response(true, cursor(18)))
      |> QueryWorkbench.attach_continuation(form)

    assert length(result.continuation.cursor_history) == 16
    assert hd(result.continuation.cursor_history) == cursor(2)
    assert result.navigation.first.cursor == nil
    assert result.continuation.page_number == 19
  end

  test "starting over clears the continuation cursor and navigation history" do
    {:ok, _, form} =
      QueryWorkbench.prepare(%{
        "fql" => @fql,
        "params_json" => @params,
        "cursor" => cursor(1),
        "page_number" => "2",
        "cursor_history" => "[null]"
      })

    result =
      QueryResult.success("FLOW.QUERY", response(false, nil))
      |> QueryWorkbench.attach_continuation(form)

    {:ok, prepared, reset} = QueryWorkbench.prepare(post(result.navigation.first))
    assert elem(prepared.ast, 1).cursor == nil
    assert reset.cursor_history == []
    assert reset.page_number == 1
  end

  test "an embedded cursor is a continuation and First page explicitly resets it" do
    embedded = String.replace(@fql, "RETURN RECORDS", "CURSOR @page RETURN RECORDS")
    params = Jason.decode!(@params) |> Map.put("page", cursor(1)) |> Jason.encode!()
    {:ok, _, form} = QueryWorkbench.prepare(%{"fql" => embedded, "params_json" => params})
    assert form.cursor == cursor(1)
    assert form.page_number == nil

    result =
      QueryResult.success("FLOW.QUERY", response(false, nil))
      |> QueryWorkbench.attach_continuation(form)

    {:ok, prepared, reset} = QueryWorkbench.prepare(post(result.navigation.first))
    assert elem(prepared.ast, 1).cursor == nil
    assert reset.page_number == 1
  end

  test "cursor history rejects an individually oversized cursor" do
    history = Jason.encode!([String.duplicate("a", 4097)])

    assert {:error, _, message} =
             QueryWorkbench.prepare(%{
               "fql" => @fql,
               "params_json" => @params,
               "cursor_history" => history
             })

    assert message =~ "page history"
  end

  test "projected metadata distinguishes string boolean and null values without changing bytes" do
    rows =
      Enum.map(
        [true, "true", nil, "null", "  value  ", "<script>"],
        &%{attributes: %{"flag" => &1}}
      )

    html =
      FlowQueryResults.render_flow_query_table(%{
        status: :ok,
        presentation: :workbench,
        source: :runs,
        rows: rows,
        columns: ["attribute.flag"],
        column_selectors: [{:attribute, "flag"}]
      })

    assert html =~ ">true</td>"
    assert html =~ "&quot;true&quot;"
    assert html =~ ">null</td>"
    assert html =~ "&quot;null&quot;"
    assert html =~ "&quot;  value  &quot;"
    assert html =~ "&lt;script&gt;"
    refute html =~ "<script>"
  end

  test "scalar value controls have dedicated explicit accessible labels" do
    filters = %{kind: "search"}
    attribute = FlowQueryControls.render_flow_query_attribute_fields(filters)
    metadata = FlowQueryControls.render_flow_query_state_meta_fields(filters)
    assert attribute =~ ~s(<label for="flow-query-attribute-value">Attribute value</label>)
    assert attribute =~ ~s(id="flow-query-attribute-value")
    assert metadata =~ ~s(<label for="flow-query-state-meta-value">State meta value</label>)
    assert metadata =~ ~s(id="flow-query-state-meta-value")
    refute attribute =~ ~r/<label[^>]*data-flow-query-scalar-group/
    refute metadata =~ ~r/<label[^>]*data-flow-query-scalar-group/
  end

  test "raw FQL copy delegates to the shared truthful clipboard contract" do
    script = FlowQueryControls.render_flow_query_mode_script(:advanced)
    assert script =~ "window.dashboardCopyText"
    refute script =~ "document.execCommand"
  end

  test "workflow lookup redirects preserve significant identifier whitespace" do
    query = URI.encode_query(%{"id" => " workflow ", "partition_key" => " partition "})
    response = http_get(Endpoint.port(), "/dashboard/flow/lookup?" <> query)
    assert extract_status_code(response) == 302
    location = extract_header(response, "location")
    assert URI.decode(URI.parse(location).path) == "/dashboard/flow/ workflow "
    assert URI.decode_query(URI.parse(location).query)["partition_key"] == " partition "
  end

  test "discovered type partition and workflow states preserve literal whitespace" do
    alias FerricstoreServer.Health.Dashboard.Flow.QueryDiscovery

    discovery =
      QueryDiscovery.collect(%{}, %{rows: []})
      |> QueryDiscovery.merge_sample_records([
        %{
          type: " invoice ",
          partition_key: " customer ",
          state: " ready ",
          run_state: " review "
        },
        %{type: " ", partition_key: " ", state: " ", run_state: " "}
      ])

    assert " invoice " in discovery.available_types
    assert " " in discovery.available_types
    assert " customer " in discovery.available_partitions
    assert " ready " in discovery.lifecycle_states
    assert " review " in discovery.workflow_steps
  end

  test "all dashboard main landmarks are skip targets without JavaScript" do
    templates =
      Path.expand("../../../../lib/ferricstore_server/health/dashboard/templates", __DIR__)

    files = Path.wildcard(Path.join(templates, "*.eex"))
    assert length(files) > 20

    for file <- files, html = File.read!(file), String.contains?(html, "<main") do
      assert html =~ ~r/<main\b[^>]*id="dashboard-main"[^>]*tabindex="-1"/, file
    end
  end

  test "state live URLs retain the explicit time-filter mode" do
    url =
      FlowQueryControls.flow_states_live_url(%{time_mode: "custom", from_ms: 1_000, to_ms: 2_000})

    assert URI.decode_query(URI.parse(url).query)["time_mode"] == "custom"
  end

  test "query and signal capture timestamps identify UTC exactly once" do
    alias FerricstoreServer.Health.Dashboard.Render.FlowQueryProvenance

    html = FlowQueryProvenance.render(%{result: %{status: :ok}, generated_at_ms: 1_234})
    assert html =~ "00:00:01.234 UTC</time>"
    refute html =~ "UTC UTC"

    template =
      Path.expand(
        "../../../../lib/ferricstore_server/health/dashboard/templates/flow_signals.html.eex",
        __DIR__
      )

    refute File.read!(template) =~ ~r/format_timestamp_ms_or_dash\([^\n]+%> UTC/
  end

  test "Query Studio scope links round trip precise updated bounds" do
    alias FerricstoreServer.Health.Dashboard.Flow.Query
    alias FerricstoreServer.Health.Dashboard.Render.FlowOverview

    html =
      FlowOverview.render_flow_scope_contract(%{
        filters: %{
          type: " invoice ",
          partition_key: " customer ",
          state: "failed",
          from_ms: 1_234,
          to_ms: 5_678
        }
      })

    [_, href] =
      Regex.run(~r/data-dashboard-route="\/dashboard\/flow\/query" href="([^"]+)"/, html)

    opts =
      href
      |> String.replace("&amp;", "&")
      |> URI.parse()
      |> Map.fetch!(:query)
      |> Query.query_opts_from_query()

    assert opts[:from_ms] == "1234"
    assert opts[:to_ms] == "5678"
    assert opts[:state] == "failed"
    assert opts[:partition_key] == " customer "
  end

  test "prepared workbench scope replaces inactive state and time controls" do
    alias FerricstoreServer.Health.Dashboard.Flow.Query

    fql =
      String.replace(
        @fql,
        "ORDER BY",
        "AND state = 'failed' AND updated_at_ms FROM 1234 TO 5678 ORDER BY"
      )

    {:ok, prepared, form} =
      QueryWorkbench.prepare(%{
        "fql" => fql,
        "params_json" => @params,
        "surface" => "guided",
        "guided_query" => "kind=list&state=queued&from=99&to=999"
      })

    data = Query.collect_workbench_page(prepared, form)
    assert data.filters.state == "failed"
    assert data.filters.from_ms == 1_234
    assert data.filters.to_ms == 5_677

    {:ok, unscoped, unscoped_form} =
      QueryWorkbench.prepare(%{
        "fql" => @fql,
        "params_json" => @params,
        "surface" => "guided",
        "guided_query" => "kind=list&state=queued&from=99&to=999"
      })

    cleared = Query.collect_workbench_page(unscoped, unscoped_form)
    assert cleared.filters.state == nil
    assert cleared.filters.from_ms == nil
    assert cleared.filters.to_ms == nil
  end

  test "investigation sections expose semantic headings" do
    base = Path.expand("../../../../lib/ferricstore_server/health/dashboard", __DIR__)

    for file <- [
          "render/flow_fifo.ex",
          "render/flow_charts.ex",
          "render/flow_tables/projection.ex",
          "render/flow_history.ex",
          "render/kv_pages.ex",
          "render/capabilities.ex",
          "templates/flow_failures_table.html.eex",
          "templates/flow_recovery_actions.html.eex"
        ] do
      refute File.read!(Path.join(base, file)) =~ ~r/<div class="section-title/, file
    end
  end

  defp response(has_more, cursor),
    do: %{
      records: [
        %{
          id: "invoice-1",
          type: "invoice",
          state: "queued",
          partition_key: "customer-a",
          updated_at_ms: 1_000
        }
      ],
      page: %{has_more: has_more, cursor: cursor}
    }

  defp cursor(n), do: "fqc1_" <> String.pad_leading(Integer.to_string(n), 32, "a")
  defp headers(html), do: Regex.scan(~r/<th\b[^>]*>(.*?)<\/th>/s, html, capture: :all_but_first)

  defp post(form) do
    %{
      "action" => "run",
      "fql" => form.fql,
      "params_json" => form.params_json,
      "cursor" => Map.get(form, :cursor) || "",
      "surface" => if(form.mode == :guided, do: "guided", else: "advanced"),
      "guided_query" => Map.get(form, :guided_query) || ""
    }
    |> maybe_put("cursor_history", Map.get(form, :cursor_history), &Jason.encode!/1)
    |> maybe_put("page_number", Map.get(form, :page_number), &to_string/1)
    |> maybe_put("page_action", Map.get(form, :page_action), &to_string/1)
  end

  defp maybe_put(map, _key, nil, _fun), do: map
  defp maybe_put(map, key, value, fun), do: Map.put(map, key, fun.(value))
end
