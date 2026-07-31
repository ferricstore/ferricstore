defmodule FerricstoreServer.Health.Dashboard.FlowQueryWorkbenchTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.RouteRequirements
  alias FerricstoreServer.Health.Endpoint.Session
  alias FerricstoreServer.Health.Dashboard.Flow.QueryWorkbench
  alias FerricstoreServer.Health.Dashboard.Flow.QueryVisualization
  alias FerricstoreServer.Acl
  alias Ferricstore.Commands.PreparedCommand
  alias Ferricstore.Flow.Query.Request

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)

    previous_query = Application.get_env(:ferricstore, :flow_dashboard_flow_query_fun)

    previous_prepared_query =
      Application.get_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun)

    previous_policy_get =
      Application.get_env(:ferricstore, :flow_dashboard_flow_policy_get_fun)

    previous_protected_mode = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)

    on_exit(fn ->
      restore_env(:flow_dashboard_flow_query_fun, previous_query)
      restore_env(:flow_dashboard_flow_query_prepared_fun, previous_prepared_query)
      restore_env(:flow_dashboard_flow_policy_get_fun, previous_policy_get)
      restore_env(:protected_mode, previous_protected_mode)
    end)

    :ok
  end

  test "preserves the complete bounded FQL record envelope" do
    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn _query, _params ->
      {:ok,
       %{
         version: "ferric.flow.query.result/v1",
         records: [
           %{
             id: "flow-1",
             type: "email",
             state: "queued",
             partition_key: "tenant-a",
             updated_at_ms: 1_000
           }
         ],
         page: %{has_more: true, cursor: "cursor-1"},
         quality: %{
           exactness: "projected_exact",
           freshness: "projection_watermark",
           coverage: "complete",
           pagination: "authenticated_seek"
         },
         usage: %{
           scanned_entries: 8,
           scanned_bytes: 640,
           hydrated_records: 0,
           result_records: 1,
           response_bytes: 512,
           memory_high_water_bytes: 1_024,
           wall_time_us: 250
         }
       }}
    end)

    data =
      Dashboard.collect_flow_query_page(
        kind: "list",
        type: "email",
        partition_key: "tenant-a"
      )

    assert data.result.rows == [
             %{
               id: "flow-1",
               type: "email",
               state: "queued",
               partition_key: "tenant-a",
               updated_at_ms: 1_000
             }
           ]

    assert data.result.version == "ferric.flow.query.result/v1"
    assert data.result.page == %{has_more: true, cursor: "cursor-1"}
    assert data.result.quality.exactness == "projected_exact"
    assert data.result.quality.freshness == "projection_watermark"
    assert data.result.usage.scanned_entries == 8
    assert data.result.usage.hydrated_records == 0
    assert data.result.usage.wall_time_us == 250
  end

  test "guided query pagination preserves its surface and exact filters" do
    cursor = "fqc1_" <> String.duplicate("g", 32)

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn _query, _params ->
      {:ok,
       %{
         records: [%{id: "flow-1", type: "email", state: "queued"}],
         page: %{has_more: true, cursor: cursor}
       }}
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun, fn request ->
      assert request.cursor == {:literal, :keyword, cursor}

      {:ok,
       %{
         records: [%{id: "flow-2", type: "email", state: "queued"}],
         page: %{has_more: false, cursor: nil}
       }}
    end)

    data =
      Dashboard.collect_flow_query_page(
        kind: "list",
        type: "email",
        state: "queued",
        attribute_key: "customer",
        attribute_value: "acme",
        partition_key: "tenant-a",
        limit: 1,
        rev: true
      )

    assert %{
             mode: :guided,
             cursor: ^cursor,
             guided_query: guided_query,
             fql: fql,
             params_json: params_json
           } = data.result.continuation

    assert fql =~ "attribute.customer = @attribute_value"
    assert Jason.decode!(params_json)["attribute_value"] == "acme"

    assert URI.decode_query(guided_query) == %{
             "attribute_key" => "customer",
             "attribute_value" => "acme",
             "kind" => "list",
             "limit" => "1",
             "partition_key" => "tenant-a",
             "rev" => "true",
             "state" => "queued",
             "type" => "email"
           }

    html = Dashboard.render_flow_query_page(data)
    assert html =~ "Next page"
    assert html =~ ~s(name="surface" value="guided")
    assert html =~ ~s(name="guided_query" value=)
    assert html =~ ~s(name="cursor" value="#{cursor}")

    continuation_params = %{
      "action" => "run",
      "surface" => "guided",
      "guided_query" => guided_query,
      "fql" => fql,
      "params_json" => params_json,
      "cursor" => cursor
    }

    assert {:ok, prepared, form} = QueryWorkbench.prepare(continuation_params)
    assert form.mode == :guided
    assert form.guided_query == guided_query

    continued = Dashboard.collect_flow_query_workbench_page(prepared, form)
    assert continued.workbench.mode == :guided
    assert continued.filters.attribute_key == "customer"
    assert continued.filters.attribute_value == "acme"
    assert continued.filters.rev
    assert continued.result.rows == [%{id: "flow-2", type: "email", state: "queued"}]
  end

  test "renders query provenance and resource usage without payload hydration" do
    data = %{
      filters: query_filters(),
      available_types: ["email"],
      total_sampled: 1,
      sample_limit: 400,
      result: %{
        status: :ok,
        command: "FLOW.QUERY",
        message: "1 row",
        rows: [
          %{
            id: "flow-1",
            type: "email",
            state: "queued",
            updated_at_ms: 1_000,
            payload: "secret-payload"
          }
        ],
        page: %{has_more: true, cursor: "cursor-1"},
        quality: %{
          exactness: "projected_exact",
          freshness: "projection_watermark",
          coverage: "complete",
          pagination: "authenticated_seek"
        },
        usage: %{
          scanned_entries: 8,
          scanned_bytes: 640,
          hydrated_records: 0,
          result_records: 1,
          response_bytes: 512,
          memory_high_water_bytes: 1_024,
          wall_time_us: 250
        }
      }
    }

    html = Dashboard.render_flow_query_page(data)

    assert html =~ "Query Quality"
    assert html =~ "projected exact"
    assert html =~ "projection watermark"
    assert html =~ "Query Usage"
    assert html =~ "Scanned entries"
    assert html =~ "Hydrated records"
    assert html =~ "Wall time"
    assert html =~ "More rows are available"
    refute html =~ "secret-payload"
  end

  test "prepares advanced FQL once with typed JSON parameters and scoped ACL keys" do
    params = %{
      "action" => "run",
      "fql" =>
        "FROM runs WHERE partition_key = @partition AND type = @type AND priority = @priority ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS (run_id, state, priority, updated_at_ms)",
      "params_json" =>
        Jason.encode!(%{"partition" => "tenant-a", "type" => "email", "priority" => 7})
    }

    assert {:ok,
            %PreparedCommand{
              ast: {:flow_query, %Request{mode: :execute} = request},
              acl_keys: ["tenant-a"]
            } = prepared, form} = QueryWorkbench.prepare(params)

    assert form.mode == :advanced
    assert form.action == :run
    assert request.projection == [:run_id, :state, :priority, :updated_at_ms]
    assert {:eq, :priority, {:literal, :integer, 7}} in elem(request.predicate, 1)
    assert QueryWorkbench.requirements(prepared) == [{"FLOW.QUERY", key: {"tenant-a", :read}}]
  end

  test "explain and analyze buttons use the native query ACL contract" do
    params = %{
      "fql" => "FROM runs WHERE partition_key = @partition AND type = @type RETURN COUNT",
      "params_json" => Jason.encode!(%{"partition" => "tenant-a", "type" => "email"})
    }

    assert {:ok, explain, _form} = QueryWorkbench.prepare(Map.put(params, "action", "explain"))
    assert {:flow_query, %Request{mode: :explain}} = explain.ast

    assert QueryWorkbench.requirements(explain) == [
             {"FLOW.QUERY.EXPLAIN", key: {"tenant-a", :read}}
           ]

    assert {:ok, analyze, _form} = QueryWorkbench.prepare(Map.put(params, "action", "analyze"))
    assert {:flow_query, %Request{mode: :analyze}} = analyze.ast

    assert QueryWorkbench.requirements(analyze) == [
             {"FLOW.QUERY", key: {"tenant-a", :read}},
             {"FLOW.QUERY.EXPLAIN", key: {"tenant-a", :read}}
           ]
  end

  test "derives command-only ACL requirements before parsing untrusted FQL" do
    assert QueryWorkbench.action_requirements(%{"action" => "run"}) == [
             {"FLOW.QUERY", []}
           ]

    assert QueryWorkbench.action_requirements(%{"action" => "explain"}) == [
             {"FLOW.QUERY.EXPLAIN", []}
           ]

    assert QueryWorkbench.action_requirements(%{"action" => "analyze"}) == [
             {"FLOW.QUERY", []},
             {"FLOW.QUERY.EXPLAIN", []}
           ]

    assert QueryWorkbench.action_requirements(%{"action" => "invalid"}) == [
             {"FLOW.QUERY", []}
           ]
  end

  test "rejects non-object parameter JSON before query execution" do
    assert {:error, form, message} =
             QueryWorkbench.prepare(%{
               "action" => "run",
               "fql" => "FROM runs WHERE partition_key = 'tenant-a' RETURN COUNT",
               "params_json" => "[1, 2, 3]"
             })

    assert form.mode == :advanced
    assert message =~ "JSON object"
  end

  test "labels malformed parameter JSON while preserving the decoder position" do
    assert {:error, form, message} =
             QueryWorkbench.prepare(%{
               "action" => "run",
               "fql" => "FROM runs WHERE partition_key = 'tenant-a' RETURN COUNT",
               "params_json" => "{bad json"
             })

    assert form.mode == :advanced
    assert message =~ "Parameters JSON is invalid"
    assert message =~ "position 1"
  end

  test "executes the prepared request without parsing the FQL again" do
    previous = Application.get_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun)
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun, fn request ->
      send(parent, {:prepared_query_executed, request})

      {:ok,
       %{
         version: "ferric.flow.query.result/v1",
         result: %{kind: "count", value: 42},
         quality: %{exactness: "projected_exact", freshness: "projection_watermark"},
         usage: %{scanned_entries: 1, scanned_bytes: 64, wall_time_us: 10}
       }}
    end)

    on_exit(fn -> restore_env(:flow_dashboard_flow_query_prepared_fun, previous) end)

    assert {:ok, prepared, _form} =
             QueryWorkbench.prepare(%{
               "action" => "run",
               "fql" =>
                 "FROM runs WHERE partition_key = @partition AND type = @type RETURN COUNT",
               "params_json" => Jason.encode!(%{"partition" => "tenant-a", "type" => "email"})
             })

    assert %{status: :ok, scalar: %{kind: "count", value: 42}} =
             QueryWorkbench.execute(prepared)

    assert_receive {:prepared_query_executed, %Request{mode: :execute, return: :count}}
  end

  test "advanced queries preserve exact type discovery from the prepared request" do
    Application.put_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun, fn _request ->
      {:ok, %{records: []}}
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn type, _opts ->
      {:ok,
       %{
         type: type,
         generation: 4,
         states: %{"review" => %{}},
         indexed_attributes: ["customer"],
         indexed_state_meta: "risk_tier"
       }}
    end)

    assert {:ok, prepared, form} =
             QueryWorkbench.prepare(%{
               "action" => "run",
               "fql" =>
                 "FROM runs WHERE partition_key = @partition AND type = @type RETURN COUNT",
               "params_json" => Jason.encode!(%{"partition" => "tenant-a", "type" => "email"})
             })

    data = Dashboard.collect_flow_query_workbench_page(prepared, form)

    assert data.filters.type == "email"
    assert data.filters.partition_key == "tenant-a"
    assert data.discovery.type == "email"
    assert data.discovery.indexed_attributes == ["customer"]
    assert data.discovery.indexed_state_meta == "risk_tier"

    html = Dashboard.render_flow_query_page(data)
    assert html =~ "Query fields for <code>email</code>"
    assert html =~ ~s|activate("advanced")|
  end

  test "preserves an authorized scalar result message in protected mode" do
    username = "query-count-#{System.unique_integer([:positive])}"

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "%R~tenant-a",
               "-@all",
               "+FLOW.QUERY"
             ])

    on_exit(fn -> Acl.del_user(username) end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun, fn _request ->
      {:ok, %{result: %{kind: "count", value: 42}}}
    end)

    assert {:ok, prepared, form} =
             QueryWorkbench.prepare(%{
               "action" => "run",
               "fql" =>
                 "FROM runs WHERE partition_key = @partition AND type = @type RETURN COUNT",
               "params_json" => Jason.encode!(%{"partition" => "tenant-a", "type" => "email"})
             })

    data =
      Dashboard.collect_flow_query_workbench_page(prepared, form, acl_username: username)

    assert data.result.scalar == %{kind: "count", value: 42}
    assert data.result.message == "42 count result"
  end

  test "query page exposes guided and advanced FQL modes" do
    html =
      Dashboard.collect_flow_query_page()
      |> Dashboard.render_flow_query_page()

    assert html =~ ~s(data-flow-query-mode="guided")
    assert html =~ ~s(data-flow-query-mode="advanced")
    assert html =~ ~s(name="fql")
    assert html =~ ~s(name="params_json")
    assert html =~ ~s(name="action" value="run")
    assert html =~ ~s(name="action" value="explain")
    assert html =~ ~s(name="action" value="analyze")
    assert html =~ ~s(method="post")
    assert html =~ ~s(event.key === "ArrowRight")
    assert html =~ ~s(event.key === "ArrowLeft")
    assert html =~ "tabs[nextIndex].focus()"
  end

  test "advanced query POST uses enabled-user preflight before exact prepared authorization" do
    assert RouteRequirements.dashboard_route_requirement(
             "POST",
             "/dashboard/flow/query"
           ) == {"*", []}
  end

  test "empty query workbench GET uses enabled-user preflight for explain-only users" do
    assert RouteRequirements.dashboard_route_requirement(
             "GET",
             "/dashboard/flow/query"
           ) == {"*", []}

    assert RouteRequirements.dashboard_route_requirement(
             "GET",
             "/dashboard/flow/query?kind=list&type=email&partition_key=tenant-a"
           ) == {"FLOW.QUERY", key: {"tenant-a", :read}}
  end

  test "advanced query POST rejects missing CSRF before execution" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun, fn request ->
      send(parent, {:unexpected_query_execution, request})
      {:ok, %{result: %{kind: "count", value: 1}}}
    end)

    response =
      http_post_form(
        Endpoint.port(),
        "/dashboard/flow/query",
        workbench_params("tenant-a", "run")
      )

    assert extract_status_code(response) == 403
    assert extract_body(response) =~ "CSRF"
    refute_receive {:unexpected_query_execution, _request}
  end

  test "advanced query POST denies an unauthorized prepared partition before execution" do
    username = "query-scope-#{System.unique_integer([:positive])}"
    Application.put_env(:ferricstore, :protected_mode, true)

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "%R~tenant-a",
               "-@all",
               "+FLOW.QUERY"
             ])

    on_exit(fn -> Acl.del_user(username) end)
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun, fn request ->
      send(parent, {:unexpected_query_execution, request})
      {:ok, %{result: %{kind: "count", value: 1}}}
    end)

    {csrf_token, cookie} = protected_request_credentials(username)

    response =
      http_post_form(
        Endpoint.port(),
        "/dashboard/flow/query",
        Map.put(workbench_params("tenant-b", "run"), "_csrf_token", csrf_token),
        [{"Cookie", cookie}, {"Origin", "http://localhost"}]
      )

    assert extract_status_code(response) == 403
    assert extract_body(response) =~ "tenant-b"
    assert extract_body(response) =~ "+FLOW.QUERY"
    refute_receive {:unexpected_query_execution, _request}
  end

  test "advanced query POST enforces explain and analyze command permissions" do
    username = "query-explain-#{System.unique_integer([:positive])}"
    Application.put_env(:ferricstore, :protected_mode, true)

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "%R~tenant-a",
               "-@all",
               "+FLOW.QUERY.EXPLAIN"
             ])

    on_exit(fn -> Acl.del_user(username) end)
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun, fn request ->
      send(parent, {:prepared_query_executed_over_http, request})

      {:ok,
       %{
         result: %{kind: "plan", value: "bounded index lookup"},
         quality: %{exactness: "plan_only", freshness: "not_applicable"}
       }}
    end)

    {csrf_token, cookie} = protected_request_credentials(username)
    headers = [{"Cookie", cookie}, {"Origin", "http://localhost"}]

    explain_response =
      http_post_form(
        Endpoint.port(),
        "/dashboard/flow/query",
        Map.put(workbench_params("tenant-a", "explain"), "_csrf_token", csrf_token),
        headers
      )

    assert extract_status_code(explain_response) == 200
    assert_receive {:prepared_query_executed_over_http, %Request{mode: :explain}}

    analyze_response =
      http_post_form(
        Endpoint.port(),
        "/dashboard/flow/query",
        Map.put(workbench_params("tenant-a", "analyze"), "_csrf_token", csrf_token),
        headers
      )

    assert extract_status_code(analyze_response) == 403
    assert extract_body(analyze_response) =~ "+FLOW.QUERY"
    refute_receive {:prepared_query_executed_over_http, %Request{mode: :analyze}}
  end

  test "renders arbitrary record projections in requested order without unselected fields" do
    Application.put_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun, fn _request ->
      {:ok,
       %{
         version: "ferric.flow.query.result/v1",
         records: [
           %{
             id: "flow-1",
             state: "queued",
             priority: 7,
             attributes: %{"customer" => "customer-1"},
             payload: "must-not-render"
           }
         ],
         page: %{has_more: false, cursor: nil},
         quality: %{exactness: "projected_exact", freshness: "projection_watermark"},
         usage: %{hydrated_records: 0, result_records: 1}
       }}
    end)

    params = %{
      "action" => "run",
      "fql" =>
        "FROM runs WHERE partition_key = @partition AND type = @type ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS (run_id, state, priority, attribute['customer'])",
      "params_json" => Jason.encode!(%{"partition" => "tenant-a", "type" => "email"})
    }

    assert {:ok, prepared, form} = QueryWorkbench.prepare(params)
    data = Dashboard.collect_flow_query_workbench_page(prepared, form)
    html = Dashboard.render_flow_query_page(data)

    assert data.result.columns == [
             "run_id",
             "state",
             "priority",
             "attribute.customer"
           ]

    assert html =~
             ~r/<th scope="col">run_id<\/th>.*<th scope="col">state<\/th>.*<th scope="col">priority<\/th>.*<th scope="col">attribute.customer<\/th>/s

    assert html =~ ~s(<caption class="sr-only">Query result records</caption>)

    assert html =~ "customer-1"
    refute html =~ "must-not-render"
    refute html =~ "<th>Worker</th>"
  end

  test "keeps partition-authorized projected rows that omit the partition column" do
    username = "query-projection-#{System.unique_integer([:positive])}"

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "%R~tenant-a",
               "-@all",
               "+FLOW.QUERY"
             ])

    on_exit(fn -> Acl.del_user(username) end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun, fn _request ->
      {:ok,
       %{
         records: [
           %{id: "flow-visible", state: "queued"},
           %{id: "flow-wrong-partition", state: "queued", partition_key: "tenant-b"}
         ],
         page: %{has_more: false, cursor: nil}
       }}
    end)

    assert {:ok, prepared, form} =
             QueryWorkbench.prepare(%{
               "action" => "run",
               "fql" =>
                 "FROM runs WHERE partition_key = @partition AND type = @type ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS (run_id, state)",
               "params_json" => Jason.encode!(%{"partition" => "tenant-a", "type" => "email"})
             })

    data =
      Dashboard.collect_flow_query_workbench_page(prepared, form, acl_username: username)

    assert Enum.map(data.result.rows, & &1.id) == ["flow-visible"]
  end

  test "keeps projected result columns readable in the mobile scroll container" do
    css = FerricstoreServer.Health.Dashboard.Layout.Styles.stylesheet()

    assert css =~
             ".flow-query-table-wrap > .flow-query-projection-table { display: table; width: max-content; min-width: 100%; max-width: none; overflow: visible; }"

    assert css =~
             ".flow-query-projection-table th, .flow-query-projection-table td { min-width: 108px; white-space: nowrap; overflow-wrap: normal; }"

    assert css =~ ".flow-search { flex-direction: column; }"
  end

  test "escapes advanced form text and projected values" do
    form = %{
      QueryWorkbench.default_form()
      | mode: :advanced,
        fql: "</textarea><script>alert('query')</script>"
    }

    error_html =
      form
      |> Dashboard.collect_flow_query_workbench_error_page("<img src=x onerror=alert(1)>")
      |> Dashboard.render_flow_query_page()

    refute error_html =~ "<script>alert('query')</script>"
    refute error_html =~ "<img src=x onerror=alert(1)>"
    assert error_html =~ "&lt;/textarea&gt;&lt;script&gt;"
    assert error_html =~ "&lt;img src=x onerror=alert(1)&gt;"
  end

  test "renders EXPLAIN as a structured plan with estimates, bounds, and index identity" do
    Application.put_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun, fn _request ->
      {:ok,
       %{
         version: "ferric.flow.explain/v1",
         status: "planned",
         plan: %{
           path: "ordered_filter",
           record_source: "covering_index",
           index: %{
             logical_id: "flow_runs_state_updated_v1",
             generation: 3,
             build_id: "build-3"
           },
           order: "native",
           range_count: 1,
           projection: %{
             fields: ["run_id", "state"],
             source: "covering_index",
             index_only: true,
             requires_hydration: false
           }
         },
         estimate: %{scanned_entries: 12, result_records: 5, cost: 17},
         bounds: %{scanned_entries: 10_000, result_records: 100, wall_time_ms: 2_000},
         stats: %{source: "sampled", confidence: "high", age_ms: 250},
         decision: %{
           reason: "lowest_cost_bounded_candidate",
           bounded_candidate_count: 2,
           cost_model: "ferric.flow.cost/v1"
         },
         quality: %{
           exactness: "projected_exact",
           freshness: "projection_watermark",
           coverage: "complete",
           pagination: "live_seek"
         },
         diagnostic: nil,
         alternatives: []
       }}
    end)

    params =
      workbench_params("tenant-a", "explain")
      |> Map.put(
        "fql",
        "FROM runs WHERE partition_key = @partition AND type = @type ORDER BY updated_at_ms DESC LIMIT 5 RETURN RECORDS (run_id, state)"
      )

    assert {:ok, prepared, form} = QueryWorkbench.prepare(params)
    data = Dashboard.collect_flow_query_workbench_page(prepared, form)
    html = Dashboard.render_flow_query_page(data)

    assert data.result.explain.status == "planned"
    assert html =~ "Chosen Plan"
    assert html =~ "ordered filter"
    assert html =~ "covering index"
    assert html =~ "flow_runs_state_updated_v1"
    assert html =~ "Generation 3"
    assert html =~ "Estimates and Bounds"
    assert html =~ "10.0K"
    assert html =~ "2.0s"
    refute html =~ "2.0e3 ms"
    assert html =~ "lowest cost bounded candidate"
  end

  test "continues record pages with an opaque POST cursor without rewriting FQL" do
    cursor = "fqc1_" <> String.duplicate("a", 32)

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun, fn request ->
      {:ok,
       %{
         records: [%{id: "flow-1", state: "queued"}],
         page: %{has_more: is_nil(request.cursor), cursor: cursor},
         quality: %{pagination: "authenticated_seek"},
         usage: %{result_records: 1}
       }}
    end)

    params = %{
      "action" => "run",
      "fql" =>
        "FROM runs WHERE partition_key = @partition AND type = @type ORDER BY updated_at_ms DESC LIMIT 1 RETURN RECORDS (run_id, state)",
      "params_json" => Jason.encode!(%{"partition" => "tenant-a", "type" => "email"})
    }

    assert {:ok, first, form} = QueryWorkbench.prepare(params)
    data = Dashboard.collect_flow_query_workbench_page(first, form)
    html = Dashboard.render_flow_query_page(data)

    assert html =~ ~s(name="cursor" value="#{cursor}")
    assert html =~ "Next page"

    assert {:ok, continued, continued_form} =
             QueryWorkbench.prepare(Map.put(params, "cursor", cursor))

    assert {:flow_query, %Request{cursor: {:literal, :keyword, ^cursor}, mode: :execute}} =
             continued.ast

    assert continued_form.cursor == cursor
    assert %{page: %{has_more: false}} = QueryWorkbench.execute(continued)
  end

  test "rejects cursor continuation for EXPLAIN before execution" do
    cursor = "fqc1_" <> String.duplicate("b", 32)

    assert {:error, _form, message} =
             "tenant-a"
             |> workbench_params("explain")
             |> Map.put("cursor", cursor)
             |> QueryWorkbench.prepare()

    assert message =~ "cursor"
  end

  test "builds current-page charts from returned columns without another query" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_prepared_fun, fn _request ->
      send(parent, :visualized_query_executed)

      {:ok,
       %{
         records: [
           %{id: "flow-1", type: "email", state: "queued", updated_at_ms: 1_000},
           %{id: "flow-2", type: "email", state: "queued", updated_at_ms: 2_000},
           %{id: "flow-3", type: "email", state: "running", updated_at_ms: 3_000},
           %{id: "flow-4", type: "sms", state: "failed", updated_at_ms: 4_000}
         ],
         page: %{has_more: false, cursor: nil},
         quality: %{pagination: "complete"},
         usage: %{result_records: 4}
       }}
    end)

    params = %{
      "action" => "run",
      "fql" =>
        "FROM runs WHERE partition_key = @partition AND type IN (@type, @other_type) ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS (run_id, type, state, updated_at_ms)",
      "params_json" =>
        Jason.encode!(%{
          "partition" => "tenant-a",
          "type" => "email",
          "other_type" => "sms"
        })
    }

    assert {:ok, prepared, form} = QueryWorkbench.prepare(params)
    data = Dashboard.collect_flow_query_workbench_page(prepared, form)
    html = Dashboard.render_flow_query_page(data)

    assert_receive :visualized_query_executed
    refute_receive :visualized_query_executed
    assert data.result.visualization.scope == :current_page
    assert data.result.visualization.row_count == 4
    assert html =~ "Visualize current page"
    assert html =~ "Current page"
    assert html =~ "State"
    assert html =~ "queued"
    assert html =~ "Updated at"
    assert html =~ ~s(<svg class="flow-query-donut" role="img")
    assert html =~ "Type distribution"
    assert html =~ ~s(<svg class="flow-query-time-chart" role="img")
    assert html =~ ~s(<rect class="flow-query-time-bar )

    assert html =~
             ".flow-query-chart-segment { fill: none; stroke: var(--flow-query-chart-color);"

    assert html =~ ".flow-query-time-bar { fill: var(--flow-query-chart-color);"
    refute html =~ "flow-query-chart-track"
  end

  test "time charts preserve empty buckets so gaps remain visible" do
    rows =
      List.duplicate(%{updated_at_ms: 1_000}, 5) ++
        List.duplicate(%{updated_at_ms: 10_000}, 4)

    visualization =
      QueryVisualization.build(%{
        presentation: :workbench,
        source: :runs,
        columns: ["updated_at_ms"],
        column_selectors: [:updated_at_ms],
        rows: rows
      })

    assert %{charts: [%{kind: :time, values: buckets}]} = visualization
    assert Enum.map(buckets, & &1.count) == [5, 0, 4]
    assert Enum.sum(Enum.map(buckets, & &1.count)) == 9
  end

  test "bounds high-cardinality page charts and preserves the exact page total" do
    rows =
      for index <- 1..40 do
        %{state: "state-#{index}"}
      end

    visualization =
      QueryVisualization.build(%{
        presentation: :workbench,
        source: :runs,
        columns: ["state"],
        column_selectors: [:state],
        rows: rows
      })

    assert %{charts: [%{field: "state", values: values}]} = visualization
    assert length(values) == 12
    assert Enum.sum(Enum.map(values, & &1.count)) == 40
    assert List.last(values).label == "Other"
  end

  test "guided query results skip unused sampling and chart only returned rows" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:guided_query, query, params})

      {:ok,
       %{
         records: [
           %{id: "flow-1", type: "email", state: "queued", updated_at_ms: 1_000},
           %{id: "flow-2", type: "email", state: "running", updated_at_ms: 2_000}
         ],
         page: %{has_more: false, cursor: nil}
       }}
    end)

    data =
      Dashboard.collect_flow_query_page(
        kind: "list",
        type: "email",
        partition_key: "tenant-a"
      )

    refute Map.has_key?(data, :available_types)
    refute Map.has_key?(data, :total_sampled)
    refute Map.has_key?(data, :sample_limit)
    assert data.result.visualization.row_count == 2
    assert Enum.any?(data.result.visualization.charts, &(&1.field == "state"))
    assert_receive {:guided_query, query, params}
    refute query =~ "state ="
    refute Map.has_key?(params, "state")
    assert data.filters.state == nil
    assert Dashboard.render_flow_query_page(data) =~ "Leave empty to include all states."
  end

  defp query_filters do
    %{
      kind: "list",
      type: "email",
      state: nil,
      attribute_key: nil,
      attribute_value: nil,
      state_meta_state: nil,
      state_meta_key: nil,
      state_meta_value: nil,
      id: nil,
      partition_key: "tenant-a",
      limit: 40,
      from_ms: nil,
      to_ms: nil,
      rev: false
    }
  end

  defp workbench_params(partition, action) do
    %{
      "action" => action,
      "fql" => "FROM runs WHERE partition_key = @partition AND type = @type RETURN COUNT",
      "params_json" => Jason.encode!(%{"partition" => partition, "type" => "email"})
    }
  end

  defp protected_request_credentials(username) do
    {csrf_token, csrf_cookie} = Session.csrf_pair({127, 0, 0, 1}, %{})
    session_cookie = Session.session_cookie(username, {127, 0, 0, 1}, %{})

    cookie = cookie_pair(session_cookie) <> "; " <> cookie_pair(csrf_cookie)
    {csrf_token, cookie}
  end

  defp cookie_pair(cookie), do: cookie |> String.split(";", parts: 2) |> hd()
end
