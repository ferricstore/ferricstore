defmodule FerricstoreServer.Health.Dashboard.QueryTimeValidationTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.{Dashboard, Endpoint}

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :protected_mode)
    query_fun = Application.get_env(:ferricstore, :flow_dashboard_flow_query_fun)
    policy_fun = Application.get_env(:ferricstore, :flow_dashboard_flow_policy_get_fun)
    Application.put_env(:ferricstore, :protected_mode, false)
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:query, query, params})
      {:ok, %{records: []}}
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn type ->
      send(parent, {:policy, type})
      {:ok, %{}}
    end)

    on_exit(fn ->
      restore_env(:protected_mode, previous)
      restore_env(:flow_dashboard_flow_query_fun, query_fun)
      restore_env(:flow_dashboard_flow_policy_get_fun, policy_fun)
    end)

    :ok
  end

  test "malformed time input is rejected before execution and options discovery" do
    for field <- ["from", "to"],
        value <- [
          "bad<&",
          "2026-02-30T12:00",
          "99999999999999999999",
          "0000-01-01T00:00",
          "-62167219200000"
        ],
        inspect <- [false, true] do
      data = collect(%{field => value, "inspect" => to_string(inspect)})
      assert data.result.status == :error
      assert data.filters.errors[String.to_existing_atom(field)]
      assert data.filters.draft[String.to_existing_atom(field)] == value
      refute_receive {:query, _, _}, 10
      refute_receive {:policy, _}, 10
    end
  end

  test "HTTP renders escaped drafts as 422 and exposes the invalid time field" do
    response =
      http_get(Endpoint.port(), path(%{"from" => ~s|bad"<&|, "to" => "2026-09-09T12:00"}))

    assert extract_status_code(response) == 422
    html = extract_body(response)
    assert html =~ ~s(type="text" name="from")
    assert html =~ "bad&quot;&lt;&amp;"
    assert html =~ ~s(aria-invalid="true")
    assert html =~ ~s(<details class="flow-query-advanced" open)
    assert html =~ "Enter a valid From UTC date and time"
    assert html =~ ~s(value="2026-09-09T12:00")
    refute_receive {:query, _, _}
  end

  test "reversed dates retain millisecond precision while equal and blank bounds remain valid" do
    data = collect(%{"from" => "2001", "to" => "1001"})
    assert data.result.status == :error
    html = Dashboard.render_flow_query_page(data)
    assert html =~ ~s(value="1970-01-01T00:00:02.001")
    assert html =~ ~s(value="1970-01-01T00:00:01.001")
    assert html =~ ~s(name="from" step="0.001")
    refute_receive {:query, _, _}

    for params <- [%{"from" => "", "to" => ""}, %{"from" => "1001", "to" => "1001"}] do
      assert collect(params).result.status == :ok
      assert_receive {:query, _, _}
    end
  end

  test "valid UTC dates and numeric timestamps produce equivalent bounded predicates" do
    assert collect(%{"from" => "1970-01-01T00:00:01.001Z"}).result.status == :ok
    assert_receive {:query, query1, params1}
    assert collect(%{"from" => "1001"}).result.status == :ok
    assert_receive {:query, query2, params2}
    assert query1 == query2
    assert params1 == params2
  end

  defp collect(params),
    do:
      path(params)
      |> String.split("?", parts: 2)
      |> List.last()
      |> Dashboard.flow_query_opts_from_query()
      |> Dashboard.collect_flow_query_page()

  defp path(params),
    do:
      "/dashboard/flow/query?" <>
        URI.encode_query(
          Map.merge(%{"kind" => "list", "type" => "review", "partition_key" => "review"}, params)
        )
end
