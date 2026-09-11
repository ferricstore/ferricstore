defmodule FerricstoreServer.Health.Dashboard.CircuitReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias Ferricstore.Flow.Governance.CircuitStore
  alias FerricstoreServer.Health.Dashboard.Flow.Governance
  alias FerricstoreServer.Health.Dashboard.Render.FlowGovernance
  alias FerricstoreServer.Health.Endpoint.RouteRequirements
  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.Session
  alias FerricstoreServer.Acl

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    :ok
  end

  test "circuit entry is a read-only review lookup, not an immediate mutation" do
    html = FlowGovernance.render_flow_governance_circuit_actions(%{})
    assert html =~ ~s(method="get")
    assert html =~ ~s(name="circuit_review_scope")
    assert html =~ "Review circuit"
    refute html =~ ~s(method="post")
  end

  test "row actions carry exact snapshot review and independent confirmation" do
    scope = unique_scope()
    {:ok, circuit} = FerricStore.flow_circuit_open(scope)
    html = FlowGovernance.render_flow_governance_circuits([circuit])
    assert html =~ "Confirm Close"
    assert html =~ ~s(name="expected_review")
    assert html =~ ~s(name="confirm_action" value="true" required)
    assert html =~ "Effects in this scope may proceed"
    refute html =~ ~s(name="failure_threshold")
    refute html =~ ~s(name="open_ms")
  end

  test "dashboard rejects mutations missing review or confirmation" do
    for extra <- [%{}, %{"confirm_action" => "true"}, %{"expected_review" => "unreviewed"}] do
      scope = unique_scope()

      assert {:error, _} =
               Governance.apply_form(
                 Map.merge(%{"action" => "open_circuit", "scope" => scope}, extra)
               )

      assert {:ok, nil} = FerricStore.flow_circuit_get(scope)
    end
  end

  test "manual circuit store mutations reject mismatched review without writing" do
    scope = unique_scope()
    {:ok, before} = FerricStore.flow_circuit_open(scope)
    assert {:error, message} = FerricStore.flow_circuit_close(scope, expected_review: "stale")
    assert message =~ "changed"
    assert {:ok, ^before} = FerricStore.flow_circuit_get(scope)
  end

  test "unchanged close ignores irrelevant Open fields and repeat rejects stale review" do
    scope = unique_scope()
    {:ok, circuit} = FerricStore.flow_circuit_open(scope)

    params = %{
      "action" => "close_circuit",
      "scope" => scope,
      "expected_review" => CircuitStore.review_fingerprint(circuit),
      "confirm_action" => "true",
      "failure_threshold" => "0",
      "open_ms" => "invalid"
    }

    assert {:ok, _} = Governance.apply_form(params)
    assert {:ok, %{status: :closed} = closed} = FerricStore.flow_circuit_get(scope)
    assert {:error, _} = Governance.apply_form(params)
    assert {:ok, ^closed} = FerricStore.flow_circuit_get(scope)
  end

  test "review requires exact circuit read permission in addition to overview" do
    requirement =
      RouteRequirements.dashboard_route_requirement(
        "GET",
        "/dashboard/flow/governance?circuit_review_scope=effect%3Apayment"
      )

    assert {"FLOW.CIRCUIT.GET", [key: {"effect:payment", :read}]} in requirement

    assert {"FLOW.CIRCUIT.GET", [key: {"effect:payment", :read}]} in RouteRequirements.flow_governance_form_requirement(
             %{"action" => "open_circuit", "scope" => "effect:payment"}
           )
  end

  test "exact review renders escaped scope and separates Open and Close controls" do
    scope = unique_scope() <> "<script>"
    {:ok, circuit} = FerricStore.flow_circuit_open(scope)
    data = Governance.collect_page(circuit_review_scope: scope)
    review = data.filters.circuit_review
    assert review.circuit == circuit
    html = FlowGovernance.render_flow_governance_circuit_actions(data.filters)
    assert html =~ "&lt;script&gt;"
    refute html =~ "<script>"
    assert length(Regex.scan(~r/method="post"/, html)) == 2
    assert length(Regex.scan(~r/name="failure_threshold"/, html)) == 1
  end

  test "only one concurrent mutation with the same review can commit" do
    scope = unique_scope()
    {:ok, circuit} = FerricStore.flow_circuit_open(scope)
    fingerprint = CircuitStore.review_fingerprint(circuit)
    now = System.system_time(:millisecond)

    results =
      1..8
      |> Task.async_stream(
        fn _ ->
          FerricStore.flow_circuit_close(scope, expected_review: fingerprint, now_ms: now)
        end,
        max_concurrency: 8
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &match?({:ok, _}, &1)) == 1
    assert Enum.count(results, &match?({:error, _}, &1)) == 7
  end

  test "missing scope review is bound to scope and rejects a concurrent creation" do
    scope = unique_scope()
    fingerprint = CircuitStore.missing_review_fingerprint(scope)

    assert {:error, _} =
             FerricStore.flow_circuit_open(unique_scope(), expected_review: fingerprint)

    {:ok, before} = FerricStore.flow_circuit_open(scope)
    assert {:error, _} = FerricStore.flow_circuit_open(scope, expected_review: fingerprint)
    assert {:ok, ^before} = FerricStore.flow_circuit_get(scope)
  end

  test "circuit review and mutation HTTP requests enforce read commands, writes, and CSRF" do
    scope = unique_scope()
    {:ok, circuit} = FerricStore.flow_circuit_open(scope)

    params = %{
      "action" => "close_circuit",
      "scope" => scope,
      "confirm_action" => "true",
      "expected_review" => CircuitStore.review_fingerprint(circuit)
    }

    response = http_post_form(Endpoint.port(), "/dashboard/flow/governance", params)
    assert extract_status_code(response) == 403

    Application.put_env(:ferricstore, :protected_mode, true)
    user = unique_scope()

    :ok =
      Acl.set_user(user, [
        "on",
        "nopass",
        "-@all",
        "+FLOW.GOVERNANCE.OVERVIEW",
        "+FLOW.CIRCUIT.CLOSE",
        "%R~*",
        "%W~#{scope}"
      ])

    on_exit(fn -> Acl.del_user(user) end)
    cookie = Session.session_cookie(user) |> String.split(";", parts: 2) |> hd()

    review_path =
      "/dashboard/flow/governance?" <> URI.encode_query(%{"circuit_review_scope" => scope})

    assert extract_status_code(http_get(Endpoint.port(), review_path, [{"Cookie", cookie}])) ==
             403

    {csrf, csrf_cookie} = Session.csrf_pair()
    cookies = cookie <> "; " <> (csrf_cookie |> String.split(";", parts: 2) |> hd())

    post = fn ->
      http_post_form(
        Endpoint.port(),
        "/dashboard/flow/governance",
        Map.put(params, "_csrf_token", csrf),
        [{"Cookie", cookies}]
      )
    end

    assert extract_status_code(post.()) == 403
    :ok = Acl.set_user(user, ["+FLOW.CIRCUIT.GET"])
    fresh_session = Session.session_cookie(user) |> String.split(";", parts: 2) |> hd()

    forbidden =
      http_post_form(
        Endpoint.port(),
        "/dashboard/flow/governance",
        params |> Map.put("scope", scope <> ":other") |> Map.put("_csrf_token", csrf),
        [{"Cookie", fresh_session <> "; " <> csrf_cookie}]
      )

    assert extract_status_code(forbidden) == 403

    response =
      http_post_form(
        Endpoint.port(),
        "/dashboard/flow/governance",
        Map.put(params, "_csrf_token", csrf),
        [{"Cookie", fresh_session <> "; " <> csrf_cookie}]
      )

    assert extract_status_code(response) == 302
    assert extract_header(response, "location") =~ "status=ok"
    assert {:ok, %{status: :closed}} = FerricStore.flow_circuit_get(scope)
  end

  defp unique_scope, do: "circuit-review-#{System.unique_integer([:positive])}"
end
