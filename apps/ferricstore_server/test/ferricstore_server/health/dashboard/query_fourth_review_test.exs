defmodule FerricstoreServer.Health.Dashboard.QueryFourthReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Flow.QueryWorkbench
  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.Session

  @query "FROM runs WHERE partition_key = @partition AND type = @type ORDER BY updated_at_ms DESC LIMIT 7 RETURN RECORDS"
  @params ~s({"partition":"tenant-a","type":"email"})

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    :ok
  end

  test "malformed JSON and invalid parameter values identify the parameters editor" do
    for json <- ["{bad<&", "[]", ~s({"nested":{"secret":"value"}}), ~s({"":"value"})] do
      assert {:error, form, message} = prepare(@query, json)
      assert form.errors == %{params_json: message}
      assert form.params_json == json
      html = render_error(form, message)
      assert html =~ ~s(id="flow-query-params-json-error")
      assert html =~ ~s(aria-describedby="flow-query-params-json-error")
      assert html =~ ~s(name="params_json")
      assert html =~ ~s(aria-invalid="true")
      refute html =~ "{bad<&"
    end
  end

  test "parser diagnostics identify FQL and retain the diagnostic and editor draft" do
    query = "FROM runs WHERE <script>"
    assert {:error, form, message} = prepare(query, "{}")
    assert form.errors == %{fql: message}
    assert form.fql == query
    html = render_error(form, message)
    assert html =~ ~s(aria-describedby="flow-query-fql-error")
    assert html =~ ~s(id="flow-query-fql-error")
    assert html =~ "FROM runs WHERE &lt;script&gt;"
    refute html =~ query
  end

  test "missing named parameters identify parameters rather than valid FQL syntax" do
    assert {:error, form, message} = prepare(@query, "{}")
    assert form.errors == %{params_json: message}
    assert message =~ "parameter"
  end

  test "valid requests have no stale field errors and keep existing query bounds" do
    assert {:ok, prepared, form} = prepare(@query, @params)
    assert Map.get(form, :errors, %{}) == %{}
    assert {:flow_query, request} = prepared.ast
    assert request.limit == 7
    assert prepared.acl_keys == ["tenant-a"]
  end

  test "query pages provide identity-scoped continuity on success and validation errors" do
    html = Dashboard.render_flow_query_page(Dashboard.collect_flow_query_page(inspect: true))
    assert html =~ ~s(data-flow-query-draft-scope=")
    assert html =~ "sessionStorage.removeItem(storageKey)"
    assert html =~ "128 * 1024"
    assert html =~ "5 * 60 * 1000"

    assert {:error, form, message} = prepare(@query, "{bad")
    assert render_error(form, message) =~ ~s(data-flow-query-first-error)
  end

  test "HTTP validation response connects the invalid JSON editor and preserves its draft" do
    {token, cookie} = Session.csrf_pair()

    response =
      http_post_form(
        Endpoint.port(),
        "/dashboard/flow/query",
        %{"fql" => @query, "params_json" => "{bad<&", "action" => "run", "_csrf_token" => token},
        [{"Cookie", cookie}, {"Origin", "http://localhost"}]
      )

    assert extract_status_code(response) == 400
    html = extract_body(response)

    assert html =~
             ~r/<textarea[^>]*name="params_json"[^>]*aria-invalid="true"[^>]*aria-describedby="flow-query-params-json-error"[^>]*data-flow-query-first-error/

    assert html =~ "{bad&lt;&amp;"
    assert html =~ ~s(id="flow-query-params-json-error")
    assert html =~ ~s(data-flow-query-draft-scope=")
  end

  test "protected requests without a verified enabled account cannot persist drafts" do
    Application.put_env(:ferricstore, :protected_mode, true)
    assert QueryWorkbench.draft_scope([]) == nil
    assert QueryWorkbench.draft_scope(acl_username: "not-an-account") == nil
    data = Dashboard.collect_flow_query_page(inspect: true)
    assert Map.get(data, :draft_scope) == nil
  end

  test "draft continuity separates accounts and invalidates after account disable" do
    Application.put_env(:ferricstore, :protected_mode, true)

    users =
      for suffix <- ["a", "b"], do: "query-fourth-#{System.unique_integer([:positive])}-#{suffix}"

    on_exit(fn -> Enum.each(users, &Acl.del_user/1) end)
    for user <- users, do: assert(:ok = Acl.set_user(user, ["on", "nopass", "+@all", "~*"]))
    [first, second] = users
    first_scope = QueryWorkbench.draft_scope(acl_username: first)
    second_scope = QueryWorkbench.draft_scope(acl_username: second)
    assert is_binary(first_scope) and byte_size(first_scope) > 0
    assert first_scope != second_scope
    refute String.contains?(first_scope, first)
    assert :ok = Acl.set_user(first, ["off"])
    assert QueryWorkbench.draft_scope(acl_username: first) == nil
  end

  defp prepare(fql, params_json),
    do: QueryWorkbench.prepare(%{"fql" => fql, "params_json" => params_json, "action" => "run"})

  defp render_error(form, message),
    do:
      form
      |> Dashboard.collect_flow_query_workbench_error_page(message)
      |> Dashboard.render_flow_query_page()
end
