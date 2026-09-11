defmodule FerricstoreServer.Health.Dashboard.StateFilterValidationTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Flow.{Browse, Sample}
  alias FerricstoreServer.Health.Endpoint

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    :ok
  end

  test "reversed dates return actionable errors on HTML and live requests" do
    query = URI.encode_query(%{from: "2026-09-08T00:00", to: "2026-09-01T00:00"})
    response = http_get(Endpoint.port(), "/dashboard/flow/states?" <> query)
    assert extract_status_code(response) == 422
    html = extract_body(response)
    assert html =~ "From UTC must not be later than To UTC"
    assert html =~ ~s(value="2026-09-08T00:00")
    assert html =~ ~s(value="2026-09-01T00:00")
    assert html =~ ~s(aria-invalid="true")
    assert html =~ ~s(data-dashboard-live-url="")
    refute html =~ "No Flow states discovered"
    refute html =~ "matching records"

    api = http_get(Endpoint.port(), "/dashboard/api/flow/states?" <> query)
    assert extract_status_code(api) == 422
    assert Jason.decode!(extract_body(api))["error"] =~ "From UTC"
  end

  test "malformed and out-of-range dates are rejected and their escaped draft is preserved" do
    for {field, label} <- [from: "From", to: "To"],
        value <- ["bad<&", "2026-02-30T12:00", "99999999999999999999"] do
      query = URI.encode_query(%{field => value})

      data =
        query |> Dashboard.flow_states_opts_from_query() |> Dashboard.collect_flow_states_page()

      assert data.filters.errors[field] == "Enter a valid #{label} UTC date and time"
      assert data.filters.draft[field] == value
      html = Dashboard.render_flow_states_page(data)
      assert html =~ "Enter a valid #{label} UTC date and time"
      assert html =~ ~s(type="text" name="#{field}")
      assert html =~ ~s(class="flow-filter-clear")
      refute html =~ "bad<&"
    end
  end

  test "invalid filters do not perform hot sampling or cold terminal lookups" do
    {data, calls} =
      trace_collection(fn ->
        Dashboard.collect_flow_states_page(
          type: "type",
          state: "failed",
          partition_key: "scope",
          from_ms: "invalid"
        )
      end)

    assert data.filters.errors[:from]
    assert calls == []
    assert data.records == []
  end

  test "blank bounds, equal bounds and explicit sliding windows remain valid" do
    for query <- ["from=&to=", "from=1000&to=1000", "range=1h&from=invalid&to=also-invalid"] do
      data =
        query |> Dashboard.flow_states_opts_from_query() |> Dashboard.collect_flow_states_page()

      assert data.filters.errors == %{}
    end
  end

  test "sub-minute bounds keep their precision so an error draft can be corrected" do
    data = Dashboard.collect_flow_states_page(from_ms: 2001, to_ms: 1001)
    html = Dashboard.render_flow_states_page(data)
    assert html =~ ~s(value="1970-01-01T00:00:02.001")
    assert html =~ ~s(value="1970-01-01T00:00:01.001")
    assert html =~ ~s(name="from" step="0.001")
  end

  defp trace_collection(fun) do
    parent = self()

    pid =
      spawn(fn ->
        receive do
          :collect ->
            send(parent, {:collected, self(), fun.()})
            receive do: (:stop -> :ok)
        end
      end)

    patterns = [
      {Sample, :collect_flow_records_sample_for_acl, 2},
      {Browse, :collect_flow_states_terminal_records, 1}
    ]

    Enum.each(patterns, fn {module, _, _} = pattern ->
      Code.ensure_loaded!(module)
      assert :erlang.trace_pattern(pattern, true, [:local]) == 1
    end)

    :erlang.trace(pid, true, [:call, {:tracer, parent}])

    try do
      send(pid, :collect)
      assert_receive {:collected, ^pid, data}, 10_000
      delivered = :erlang.trace_delivered(pid)
      assert_receive {:trace_delivered, ^pid, ^delivered}, 5_000
      {data, drain_calls(pid, [])}
    after
      :erlang.trace(pid, false, [:call])
      Enum.each(patterns, &:erlang.trace_pattern(&1, false, [:local]))
      send(pid, :stop)
    end
  end

  defp drain_calls(pid, calls) do
    receive do
      {:trace, ^pid, :call, call} -> drain_calls(pid, [call | calls])
    after
      0 -> Enum.reverse(calls)
    end
  end
end
