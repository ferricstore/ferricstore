defmodule FerricstoreServer.Health.Dashboard.QueryScopeTest do
  use FerricstoreServer.Test.DashboardCase

  alias Ferricstore.Flow.Query.{Builder, Request}
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Health.Dashboard.Flow.{Calls, QueryScope, QueryWorkbench}

  test "literal exact identities survive without normalization" do
    for value <- [" type ", " ", "all", "ALL", "a<&\""] do
      scope =
        QueryScope.options(
          request([eq(:type, value), eq(:partition_key, value), eq(:state, value)]),
          []
        )

      assert scope[:type] == value
      assert scope[:partition_key] == value
      assert scope[:state] == value
    end
  end

  test "events do not transfer historical runtime state or updated time" do
    scope =
      QueryScope.options(
        request(
          [
            eq(:type, "events"),
            eq(:partition_key, " tenant "),
            eq(:state, "historical"),
            range(:updated_at_ms, 100, 200)
          ],
          :events
        ),
        stale()
      )

    assert scope[:type] == "events"
    assert scope[:partition_key] == " tenant "
    refute Keyword.has_key?(scope, :state)
    refute Keyword.has_key?(scope, :from_ms)
    refute Keyword.has_key?(scope, :to_ms)
  end

  test "created, lease, scheduled time and workflow step are not relabeled" do
    predicates = [
      range(:created_at_ms, 100, 200),
      range(:lease_deadline_ms, 300, 400),
      range(:next_run_at_ms, 500, 600),
      eq(:run_state, "step")
    ]

    assert QueryScope.options(request(predicates), stale()) == [acl_username: "reader", limit: 40]
  end

  test "duplicate exact identities agree but contradictory equalities are omitted" do
    scope =
      QueryScope.options(
        request([
          eq(:type, " same "),
          eq(:type, " same "),
          eq(:partition_key, "a"),
          eq(:partition_key, "b"),
          eq(:state, "ready"),
          eq(:state, "failed")
        ]),
        stale()
      )

    assert scope[:type] == " same "
    refute Keyword.has_key?(scope, :partition_key)
    refute Keyword.has_key?(scope, :state)
  end

  test "non-scalar identity selections do not retain stale scalar predicates" do
    predicates = [
      {:in, :state, [keyword("queued"), keyword("running")]},
      {:eq, :type, {:parameter, :keyword, "type"}},
      eq(:partition_key, "")
    ]

    assert QueryScope.options(request(predicates), stale()) == [acl_username: "reader", limit: 40]
  end

  test "missing prepared predicates clear every old transferable scope key" do
    assert QueryScope.options(request([]), stale()) == [acl_username: "reader", limit: 40]
  end

  test "conjunctive updated ranges intersect without rounding milliseconds" do
    scope =
      QueryScope.options(
        request([range(:updated_at_ms, 1_001, 2_999), range(:updated_at_ms, 1_777, 3_001)]),
        []
      )

    assert scope[:from_ms] == 1_777
    assert scope[:to_ms] == 2_999

    assert QueryScope.options(
             request([range(:updated_at_ms, 1, 2), range(:updated_at_ms, 3, 4)]),
             stale()
           ) == [acl_username: "reader", limit: 40]
  end

  test "half-open updated window converts its exclusive millisecond upper edge" do
    scope =
      QueryScope.options(
        request([{:time_window, :updated_at_ms, integer(1_001), integer(1_003)}]),
        []
      )

    assert scope[:from_ms] == 1_001
    assert scope[:to_ms] == 1_002

    single =
      QueryScope.options(request([{:time_window, :updated_at_ms, integer(0), integer(1)}]), [])

    assert single[:from_ms] == 0
    assert single[:to_ms] == 0
  end

  test "empty half-open and unrepresentable bounds do not become invented valid dates" do
    for predicate <- [
          {:time_window, :updated_at_ms, integer(100), integer(100)},
          range(:updated_at_ms, 9_223_372_036_854_775_800, 9_223_372_036_854_775_807)
        ] do
      assert QueryScope.options(request([predicate]), stale()) == [
               acl_username: "reader",
               limit: 40
             ]
    end
  end

  test "updated equality intersects as a one-millisecond inclusive range" do
    scope =
      QueryScope.options(
        request([range(:updated_at_ms, 100, 200), {:eq, :updated_at_ms, integer(123)}]),
        []
      )

    assert scope[:from_ms] == 123
    assert scope[:to_ms] == 123
  end

  test "one-sided guided From survives the builder's open-upper sentinel" do
    assert {:ok, built} =
             Builder.build(:list, %{
               type: " orders ",
               partition_key: " tenant ",
               from_ms: 1_234,
               limit: 40
             })

    assert {:ok, prepared, _form} =
             QueryWorkbench.prepare(%{
               "fql" => built.query,
               "params_json" => Jason.encode!(built.params)
             })

    {:flow_query, request} = prepared.ast
    scope = QueryScope.options(request, stale())
    assert scope[:from_ms] == 1_234
    refute Keyword.has_key?(scope, :to_ms)
  end

  test "scope extraction never invokes record, history, query or payload readers" do
    patterns = [
      {Router, :get, 2},
      {Calls, :flow_dashboard_flow_get, 2},
      {Calls, :flow_dashboard_flow_history, 2},
      {Calls, :flow_dashboard_flow_query_prepared, 1},
      {Calls, :flow_dashboard_flow_value_mget, 1}
    ]

    Enum.each(patterns, fn {module, _, _} = pattern ->
      Code.ensure_loaded!(module)
      assert :erlang.trace_pattern(pattern, true, [:local]) == 1
    end)

    parent = self()

    pid =
      spawn(fn ->
        receive do
          :collect ->
            scope =
              QueryScope.options(
                request([eq(:partition_key, " tenant "), range(:updated_at_ms, 1_001, 2_002)]),
                []
              )

            send(parent, {:scope, self(), scope})
            receive do: (:stop -> :ok)
        end
      end)

    :erlang.trace(pid, true, [:call, {:tracer, parent}])

    try do
      send(pid, :collect)
      assert_receive {:scope, ^pid, scope}
      assert scope[:partition_key] == " tenant "
      delivered = :erlang.trace_delivered(pid)
      assert_receive {:trace_delivered, ^pid, ^delivered}
      refute_receive {:trace, ^pid, :call, _}, 0
    after
      :erlang.trace(pid, false, [:call])
      Enum.each(patterns, &:erlang.trace_pattern(&1, false, [:local]))
      send(pid, :stop)
    end
  end

  defp request(predicates, source \\ :runs),
    do: %Request{mode: :execute, source: source, predicate: {:and, predicates}, return: :record}

  defp keyword(value), do: {:literal, :keyword, value}
  defp integer(value), do: {:literal, :integer, value}
  defp eq(field, value), do: {:eq, field, keyword(value)}
  defp range(field, lower, upper), do: {:range, field, integer(lower), integer(upper)}

  defp stale,
    do: [
      type: "old",
      partition_key: "old",
      state: "old",
      from_ms: 4,
      to_ms: 5,
      range: "1h",
      time_mode: "relative",
      acl_username: "reader",
      limit: 40
    ]
end
