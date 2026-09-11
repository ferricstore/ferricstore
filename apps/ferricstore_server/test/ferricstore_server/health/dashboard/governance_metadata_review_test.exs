defmodule FerricstoreServer.Health.Dashboard.GovernanceMetadataReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Flow.Governance
  alias FerricstoreServer.Health.Dashboard.Render.FlowGovernance
  alias FerricstoreServer.Health.Endpoint.RouteRequirements
  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.Session
  alias FerricstoreServer.Acl

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :flow_dashboard_flow_query_fun)
    on_exit(fn -> restore_env(:flow_dashboard_flow_query_fun, previous) end)
    :ok
  end

  test "metadata results retain bounded continuation and allowlisted quality" do
    parent = self()

    stub(fn query, params ->
      send(parent, {:query, query, params})

      {:ok,
       %{
         records: [],
         page: %{has_more: true, cursor: "next&<page>", secret: "hidden"},
         quality: %{coverage: "partial", freshness: "lagging", secret: "hidden"}
       }}
    end)

    data = collect(%{"limit" => "1"})
    assert data.state_meta_result.page == %{has_more: true, cursor: "next&<page>"}
    assert data.state_meta_result.quality == %{coverage: "partial", freshness: "lagging"}
    html = FlowGovernance.render_flow_governance_state_meta(data)
    assert html =~ "Next page"
    assert html =~ "partial"
    assert html =~ "lagging"
    refute html =~ "hidden"
    assert_receive {:query, query, _}
    assert query =~ "LIMIT 1"
    refute_receive {:query, _, _}
  end

  test "metadata continuation is bound and preserves all filter namespaces" do
    parent = self()

    stub(fn query, params ->
      send(parent, {:query, query, params})
      {:ok, %{records: [], page: %{has_more: false}}}
    end)

    data =
      collect(%{
        "meta_cursor" => "opaque+<&",
        "scope" => "scope-a",
        "flow_id" => "run-a",
        "approval_status" => "pending",
        "circuit_status" => "open"
      })

    assert_receive {:query, query, params}
    assert query =~ "CURSOR @cursor"
    assert params["cursor"] == "opaque+<&"
    html = FlowGovernance.render_flow_governance_state_meta(data)
    assert html =~ "First page"
    assert html =~ "scope=scope-a"
    assert html =~ "meta_type=review"
    refute html =~ "Next page"
  end

  test "string query values preserve spaces, empty strings and Unicode" do
    parent = self()

    stub(fn _, params ->
      send(parent, {:value, params["state_meta_value"]})
      {:ok, %{records: []}}
    end)

    for value <- [" high ", "", "   ", "\u00e9clair"] do
      data = collect(%{"meta_value" => value})
      assert data.state_meta_result.status == :ok
      assert_receive {:value, ^value}
    end
  end

  test "absent metadata value remains idle and typed values still normalize whitespace" do
    parent = self()

    stub(fn _, params ->
      send(parent, {:value, params["state_meta_value"]})
      {:ok, %{records: []}}
    end)

    assert base_params()
           |> Map.delete("meta_value")
           |> URI.encode_query()
           |> Governance.opts_from_query()
           |> Governance.collect_page()
           |> get_in([:state_meta_result, :status]) == :idle

    refute_receive {:value, _}

    for {kind, input, expected} <- [
          {"integer", " 42 ", 42},
          {"float", " 1.5 ", 1.5},
          {"boolean", " true ", true}
        ] do
      assert collect(%{"meta_value_type" => kind, "meta_value" => input}).state_meta_result.status ==
               :ok

      assert_receive {:value, ^expected}
    end
  end

  test "empty string predicates still require FLOW.QUERY authorization" do
    for value <- ["", "   ", "value"] do
      query = base_params() |> Map.put("meta_value", value) |> URI.encode_query()

      assert RouteRequirements.dashboard_route_requirement(
               "GET",
               "/dashboard/flow/governance?" <> query
             ) ==
               [
                 {"FLOW.GOVERNANCE.OVERVIEW", key: {"*", :read}},
                 {"FLOW.QUERY", key: {"partition", :read}}
               ]
    end
  end

  test "HTTP denies empty-string metadata queries until the principal has query permission" do
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, true)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    username = "governance-query-review-#{System.unique_integer([:positive])}"

    assert :ok =
             Acl.set_user(username, ["on", "nopass", "%R~*", "-@all", "+FLOW.GOVERNANCE.OVERVIEW"])

    on_exit(fn -> Acl.del_user(username) end)
    parent = self()

    stub(fn _, _ ->
      send(parent, :query_executed)
      {:ok, %{records: []}}
    end)

    cookie = fn -> Session.session_cookie(username) |> String.split(";", parts: 2) |> hd() end

    path =
      "/dashboard/flow/governance?" <> URI.encode_query(Map.put(base_params(), "meta_value", ""))

    response = http_get(Endpoint.port(), path, [{"Cookie", cookie.()}])
    assert extract_status_code(response) == 403
    refute_receive :query_executed
    assert :ok = Acl.set_user(username, ["+FLOW.QUERY"])
    allowed = http_get(Endpoint.port(), path, [{"Cookie", cookie.()}])
    assert extract_status_code(allowed) == 200
    assert_receive :query_executed
  end

  test "both search forms preserve the other namespace but reset the metadata cursor" do
    stub(fn _, _ -> {:ok, %{records: []}} end)

    data =
      collect(%{
        "scope" => "scope-a",
        "approval_status" => "pending",
        "flow_id" => "run-a",
        "circuit_status" => "open",
        "meta_cursor" => "old"
      })

    meta_html = FlowGovernance.render_flow_governance_state_meta_filters(data)

    for {key, value} <- [
          {"scope", "scope-a"},
          {"approval_status", "pending"},
          {"flow_id", "run-a"},
          {"circuit_status", "open"}
        ] do
      assert meta_html =~ ~s(type="hidden" name="#{key}" value="#{value}")
    end

    refute meta_html =~ ~s(name="meta_cursor")
    overview_html = FlowGovernance.render_flow_governance_filters(data)
    assert overview_html =~ ~s(type="hidden" name="meta_type" value="review")
    assert overview_html =~ ~s(type="hidden" name="meta_value" value="high")
    refute overview_html =~ ~s(name="meta_cursor")
  end

  test "mutation redirects preserve metadata including empty strings without reflecting arbitrary input" do
    params =
      Map.merge(base_params(), %{
        "scope" => "s",
        "meta_value" => "",
        "meta_cursor" => "page",
        "_csrf_token" => "secret",
        "unexpected" => "bad"
      })

    location = Governance.redirect_location(params, {:ok, "saved"})
    query = location |> URI.parse() |> Map.fetch!(:query) |> URI.decode_query()
    assert query["meta_value"] == ""
    assert query["meta_cursor"] == "page"
    assert query["meta_type"] == "review"
    refute Map.has_key?(query, "_csrf_token")
    refute Map.has_key?(query, "unexpected")
  end

  test "string-keyed envelopes and errors retain actionable pagination state" do
    stub(fn _, _ ->
      {:ok,
       %{
         "records" => [],
         "page" => %{"has_more" => false},
         "quality" => %{"coverage" => "complete"}
       }}
    end)

    assert collect().state_meta_result.page == %{has_more: false}
    stub(fn _, _ -> {:error, "ERR cursor expired"} end)

    html =
      collect(%{"meta_cursor" => "expired"}) |> FlowGovernance.render_flow_governance_state_meta()

    assert html =~ "First page"
    assert html =~ "Retry query"
    refute html =~ "Next page"
  end

  test "real indexed metadata searches distinguish exact strings and traverse bounded pages" do
    Application.delete_env(:ferricstore, :flow_dashboard_flow_query_fun)
    type = "governance-exact-#{System.unique_integer([:positive])}"
    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_state_meta: "risk")
    ctx = FerricStore.Instance.get(:default)
    :ok = Ferricstore.Flow.LMDBWriter.flush_all(:default, ctx.shard_count, 30_000)

    for {suffix, value} <- [
          {"spaced", " high "},
          {"plain-a", "high"},
          {"plain-b", "high"},
          {"empty", ""}
        ] do
      assert :ok =
               FerricStore.flow_create(type <> suffix,
                 type: type,
                 state: "queued",
                 partition_key: "partition",
                 state_meta: %{"risk" => value}
               )
    end

    :ok = Ferricstore.Flow.LMDBWriter.flush_all(:default, ctx.shard_count, 30_000)

    for {value, suffix} <- [{" high ", "spaced"}, {"", "empty"}] do
      data = collect(%{"meta_type" => type, "meta_value" => value, "limit" => "1"})
      assert [%{id: id}] = data.state_meta_result.rows
      assert id == type <> suffix
      refute data.state_meta_result.page.has_more
    end

    first = collect(%{"meta_type" => type, "limit" => "1"}).state_meta_result
    assert first.page.has_more

    second =
      collect(%{"meta_type" => type, "limit" => "1", "meta_cursor" => first.page.cursor}).state_meta_result

    assert length(first.rows) == 1
    assert length(second.rows) == 1
    refute second.page.has_more

    assert Enum.map(first.rows ++ second.rows, & &1.id) |> Enum.sort() == [
             type <> "plain-a",
             type <> "plain-b"
           ]
  end

  test "circuit actions preserve a distinct return scope and the metadata query" do
    stub(fn _, _ -> {:ok, %{records: []}} end)
    data = collect(%{"scope" => "selected-scope"})
    html = FlowGovernance.render_flow_governance_circuit_actions(data.filters)
    assert html =~ ~s(method="get")
    assert html =~ ~s(name="scope" value="selected-scope")
    assert html =~ ~s(name="meta_type" value="review")

    reviewed = %{
      status: :ok,
      scope: "effect:charge",
      circuit: %{scope: "effect:charge", status: :open},
      fingerprint: String.duplicate("x", 43)
    }

    html =
      FlowGovernance.render_flow_governance_circuit_actions(
        Map.put(data.filters, :circuit_review, reviewed)
      )

    assert html =~ ~s(name="return_scope" value="selected-scope")
    assert html =~ ~s(name="scope" value="effect:charge")

    result =
      Governance.redirect_location(
        Map.merge(base_params(), %{"scope" => "effect:charge", "return_scope" => "selected-scope"}),
        {:ok, "closed"}
      )

    params = result |> URI.parse() |> Map.fetch!(:query) |> URI.decode_query()
    assert params["scope"] == "selected-scope"
    assert params["meta_type"] == "review"
    refute Map.has_key?(params, "return_scope")
  end

  defp stub(fun), do: Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fun)

  defp base_params,
    do: %{
      "meta_type" => "review",
      "meta_state" => "queued",
      "meta_key" => "risk",
      "meta_value" => "high",
      "meta_value_type" => "string",
      "meta_partition_key" => "partition"
    }

  defp collect(params \\ %{}),
    do:
      base_params()
      |> Map.merge(params)
      |> URI.encode_query()
      |> Governance.opts_from_query()
      |> Governance.collect_page()
end
