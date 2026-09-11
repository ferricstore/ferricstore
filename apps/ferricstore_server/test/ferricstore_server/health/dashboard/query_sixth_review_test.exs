defmodule FerricstoreServer.Health.Dashboard.QuerySixthReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Flow.{Detail, QueryVisualization, QueryWorkbench}

  alias FerricstoreServer.Health.Dashboard.Render.{
    FlowQueryControls,
    FlowQueryExport,
    FlowQueryResults
  }

  test "typed chart identities never collapse integer text boolean or decimal values" do
    chart = category([1, "1", true, "true", 1.0])
    assert length(chart.values) == 5
    assert Enum.all?(chart.values, &(&1.count == 1))

    assert Enum.sort(Enum.map(chart.values, & &1.label)) ==
             Enum.sort(["1", "\"1\"", "true", "\"true\"", "1.0"])
  end

  test "non UTF8 categories stay binary and distinct from literal base64 text" do
    chart = category([<<255, 0>>, "Base64 /wA="])
    assert length(chart.values) == 2
    assert Enum.any?(chart.values, &(&1.label == "Base64 /wA="))
    assert Enum.any?(chart.values, &(&1.label == "\"Base64 /wA=\""))
    assert Enum.sum(Enum.map(chart.values, & &1.count)) == 2
  end

  test "empty text null and complex categories preserve the current-page denominator" do
    chart = category(["high", "", nil, %{"nested" => true}])
    assert Enum.sum(Enum.map(chart.values, & &1.count)) == 4
    assert Enum.any?(chart.values, &(&1.label == "\"\""))
    assert Enum.any?(chart.values, &(&1.label == "null"))
    assert Enum.any?(chart.values, &(&1.label == "Structured values"))

    html =
      FlowQueryResults.render_flow_query_visualization(%{
        visualization: %{scope: :current_page, row_count: 4, charts: [chart]}
      })

    assert html =~ "25%"
    refute html =~ "100%"
  end

  test "real Other remains separate from bounded remaining categories" do
    chart = category(List.duplicate("Other", 3) ++ Enum.map(1..13, &"value-#{&1}"))
    assert length(chart.values) == 12
    assert Enum.sum(Enum.map(chart.values, & &1.count)) == 16
    assert Enum.any?(chart.values, &(&1.label == "\"Other\"" and &1.count == 3))
    assert List.last(chart.values).label == "Remaining categories"
  end

  test "time chart ticks have unique integer labels at mathematically correct coordinates" do
    for maximum <- [1, 3, 5] do
      html =
        FlowQueryResults.render_flow_query_visualization(%{
          visualization: %{
            scope: :current_page,
            row_count: maximum + 1,
            charts: [
              %{
                kind: :time,
                field: "updated_at_ms",
                values: [%{from_ms: 1, to_ms: 2, count: maximum}]
              }
            ]
          }
        })

      ticks =
        Regex.scan(~r/class="flow-query-time-tick"[^>]*y="([^"]+)"[^>]*>([^<]+)<\/text>/, html,
          capture: :all_but_first
        )

      labels = Enum.map(ticks, fn [_y, label] -> String.to_integer(label) end)
      assert Enum.uniq(labels) == labels

      for [y, label] <- ticks do
        {y, _} = Float.parse(y)
        assert_in_delta y - 3, 130 - String.to_integer(label) * 112 / maximum, 0.02
      end
    end
  end

  test "structured projections have bounded full JSON inspection and untouched export" do
    value = Map.new(1..30, &{"key#{&1}", "value#{&1}"})
    result = projected([%{state_meta: value}], [:state_meta], ["state_meta"])
    html = FlowQueryResults.render_flow_query_table(result)
    assert html =~ "<details"
    assert html =~ "Full projected state_meta"
    assert html =~ "&quot;key30&quot;: &quot;value30&quot;"
    refute html =~ "%{"
    assert {:ok, exported} = FlowQueryExport.encode(result)
    assert Jason.decode!(exported)["rows"] == [[value]]

    oversized =
      projected([%{state_meta: %{"large" => String.duplicate("x", 70_000)}}], [:state_meta], [
        "state_meta"
      ])

    assert FlowQueryResults.render_flow_query_table(oversized) =~ "64 KiB inspector limit"
  end

  test "Guided adds logical workflow state without changing Raw nulls or projections" do
    data =
      Dashboard.collect_flow_query_page(inspect: true, type: "example", partition_key: "tenant")

    form = data.guided_import

    result = %{
      status: :ok,
      command: "FLOW.QUERY",
      rows: [
        %{
          id: "run",
          type: "example",
          state: "running",
          run_state: "vector_rag_retrieved",
          updated_at_ms: 1
        }
      ]
    }

    guided = QueryWorkbench.attach_continuation(result, form)
    assert :run_state in guided.column_selectors
    assert "Stored state" in guided.column_labels
    assert "Workflow state" in guided.column_labels
    assert FlowQueryResults.render_flow_query_table(guided) =~ "vector_rag_retrieved"
    raw = projected([%{state: "queued", run_state: nil}], [:run_state], ["run_state"])
    assert FlowQueryResults.render_flow_query_table(raw) =~ ">null</td>"
    assert {:ok, json} = FlowQueryExport.encode(raw)
    assert Jason.decode!(json)["rows"] == [[nil]]
  end

  test "structured inspectors share an aggregate page budget without truncating Raw export" do
    rows =
      for n <- 1..400, do: %{id: "row-#{n}", state_meta: %{"text" => String.duplicate("&", 800)}}

    result = projected(rows, [:state_meta], ["state_meta"])
    html = FlowQueryResults.render_flow_query_table(result)
    assert byte_size(html) < 1_150_000
    assert html =~ "Page JSON inspector limit reached"
    assert length(Regex.scan(~r/<details /, html)) < 400
    assert {:ok, exported} = FlowQueryExport.encode(result, max_bytes: 4 * 1024 * 1024)
    assert Jason.decode!(exported)["rows"] == Enum.map(rows, &[&1.state_meta])
  end

  test "JSON inspection is indented without changing integer or binary escape lexemes" do
    value = %{
      "outer" => %{
        "large" => 9_007_199_254_740_993_123_456_789,
        "binary" => <<255, 0>>,
        "escaped" => "<&\"",
        "empty" => []
      }
    }

    result = projected([%{state_meta: value}], [:state_meta], ["state_meta"])
    html = FlowQueryResults.render_flow_query_table(result)
    assert html =~ "{\n  &quot;outer&quot;: {\n"
    assert html =~ "    &quot;large&quot;: 9007199254740993123456789"
    assert html =~ "      &quot;data&quot;: &quot;/wA=&quot;"
    assert html =~ "\\u003c\\u0026"
    assert {:ok, compact} = FlowQueryExport.encode_value(value, 64 * 1024)
    assert [_, inspected] = Regex.run(~r/<pre[^>]*>([\s\S]*?)<\/pre>/, html)

    expected =
      compact
      |> Jason.Formatter.pretty_print()
      |> FerricstoreServer.Health.Dashboard.Format.escape()

    assert inspected == expected
    assert {:ok, exported} = FlowQueryExport.encode(result)
    assert Jason.decode!(exported)["rows"] == [[Jason.decode!(compact)]]
  end

  test "indentation expansion is included in the per-cell JSON byte limit" do
    value = %{"outer" => %{"inner" => %{"values" => List.duplicate(0, 7_000)}}}
    assert {:ok, compact} = FlowQueryExport.encode_value(value, 64 * 1024)
    assert byte_size(compact) < 64 * 1024
    assert IO.iodata_length(Jason.Formatter.pretty_print_to_iodata(compact)) > 64 * 1024

    html =
      FlowQueryResults.render_flow_query_table(
        projected([%{state_meta: value}], [:state_meta], ["state_meta"])
      )

    refute html =~ "<details"
    assert html =~ "64 KiB inspector limit"
  end

  test "deep JSON nesting is rejected before formatter indentation can amplify it" do
    value = Enum.reduce(1..80, %{}, fn _, inner -> %{"nested" => inner} end)

    html =
      FlowQueryResults.render_flow_query_table(
        projected([%{state_meta: value}], [:state_meta], ["state_meta"])
      )

    refute html =~ "<details"
    assert html =~ "64-level inspector limit"
  end

  test "invalid date guidance recognizes an already supplied type without discovery reads" do
    parent = self()
    previous = Application.get_env(:ferricstore, :flow_dashboard_flow_policy_get_fun)

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn _ ->
      send(parent, :unexpected_discovery)
      {:error, :unavailable}
    end)

    on_exit(fn -> restore_env(:flow_dashboard_flow_policy_get_fun, previous) end)
    data = Dashboard.collect_flow_query_page(type: "ai_agent_pipeline", from_ms: "not-a-date")
    html = FlowQueryControls.render_flow_query_discovery(data.discovery)
    refute html =~ "Enter a workflow type"
    assert html =~ "Options are not loaded"
    refute_received :unexpected_discovery
  end

  test "positioned parser errors retain structured byte positions and UTF16 editor offsets" do
    for prefix <- [
          "FROM runs WHERE type = 'ascii' AND ",
          "FROM runs WHERE type = 'é😀' AND ",
          "FROM runs\r\nWHERE type = 'é😀' AND "
        ] do
      query = prefix <> "!"

      assert {:error, form, _message} =
               QueryWorkbench.prepare(%{"fql" => query, "params_json" => "{}", "action" => "run"})

      assert %{fql: %{byte: byte}} = Map.get(form, :error_positions)
      assert byte == byte_size(prefix) + 1
      attrs = FlowQueryControls.workbench_error_attrs(form, :fql)

      expected =
        prefix
        |> String.replace("\r\n", "\n")
        |> :unicode.characters_to_binary(:utf8, {:utf16, :big})
        |> byte_size()
        |> div(2)

      assert attrs =~ ~s(data-flow-query-error-start="#{expected}")
      assert attrs =~ ~s(data-flow-query-error-end="#{expected + 1}")
    end
  end

  test "history values link to scoped event-inclusive detail instead of local missing anchors" do
    result = %{
      status: :ok,
      command: "FLOW.HISTORY",
      history_scope: %{id: " flow/id ", partition_key: " tenant & one ", count: 7},
      rows: [{"1234-2", %{"action" => "created", "payload_ref" => "blob-ref"}}]
    }

    html = FlowQueryResults.render_flow_query_table(result)
    assert [_, href] = Regex.run(~r/href="([^"]+)"/, html)
    uri = URI.parse(String.replace(href, "&amp;", "&"))
    assert URI.decode(uri.path) == "/dashboard/flow/ flow/id "

    assert URI.decode_query(uri.query) == %{
             "partition_key" => " tenant & one ",
             "history_event" => "1234-2",
             "history_count" => "7"
           }

    assert uri.fragment =~ "flow-value-"
    assert Detail.opts_from_query(uri.query)[:history_event] == "1234-2"
  end

  test "selected historical event authorizes only values from its bounded inclusive page" do
    keys = [
      :protected_mode,
      :flow_dashboard_flow_get_fun,
      :flow_dashboard_flow_history_fun,
      :flow_dashboard_flow_value_mget_fun
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:ferricstore, &1)})
    on_exit(fn -> Enum.each(previous, fn {key, value} -> restore_env(key, value) end) end)
    Application.put_env(:ferricstore, :protected_mode, false)
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn "sixth-history", opts ->
      assert opts[:partition_key] == "sixth-scope"

      {:ok,
       %{id: "sixth-history", type: "sixth", state: "completed", partition_key: "sixth-scope"}}
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn "sixth-history",
                                                                           opts ->
      send(parent, {:history_read, opts})

      events =
        for n <- 1..80,
            do: {"#{1000 + n}-0", %{"event" => "updated", "payload_ref" => "history-#{n}"}}

      selected = if opts[:from_event] == "1001-0", do: events, else: Enum.drop(events, 70)
      {:ok, Enum.take(selected, opts[:count])}
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn refs ->
      send(parent, {:value_read, refs})
      {:ok, ["historical payload"]}
    end)

    scope = %{
      "flow" => "sixth-history",
      "partition_key" => "sixth-scope",
      "history_event" => "1001-0",
      "history_count" => "7",
      "ref" => "history-1"
    }

    assert {:ok, %{status: "ok", value: "historical payload"}} =
             Dashboard.live_payload("flow/value?" <> URI.encode_query(scope))

    assert_receive {:history_read, opts}
    assert opts[:from_event] == "1001-0"
    assert opts[:count] == 8
    assert opts[:values] == false
    assert opts[:consistent_projection]
    assert_receive {:value_read, ["history-1"]}

    assert {:ok, %{status: "error"}} =
             Dashboard.live_payload(
               "flow/value?" <> URI.encode_query(%{scope | "ref" => "history-8"})
             )

    assert {:ok, %{status: "error"}} =
             Dashboard.live_payload(
               "flow/value?" <> URI.encode_query(Map.delete(scope, "history_event"))
             )

    refute_receive {:value_read, _}

    data =
      Detail.collect_page("sixth-history",
        partition_key: "sixth-scope",
        history_event: "1001-0",
        history_count: 7,
        values: false
      )

    assert length(data.history) == 7
    assert elem(hd(data.history), 0) == "1001-0"

    assert data.history_page.current_live_params == %{
             "history_event" => "1001-0",
             "history_count" => 7
           }
  end

  defp category(values) do
    %{charts: [chart]} =
      QueryVisualization.build(
        projected(Enum.map(values, &%{attributes: %{"risk" => &1}}), [{:attribute, "risk"}], [
          "attribute.risk"
        ])
      )

    chart
  end

  defp projected(rows, selectors, columns),
    do: %{
      status: :ok,
      presentation: :workbench,
      source: :runs,
      rows: rows,
      column_selectors: selectors,
      columns: columns
    }
end
