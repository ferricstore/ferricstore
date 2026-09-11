defmodule FerricstoreServer.Health.Dashboard.WorkflowSixthReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Flow.{Browse, Fifo}
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)

    for key <- [
          :flow_dashboard_flow_query_fun,
          :flow_dashboard_flow_policy_get_fun,
          :flow_dashboard_list_fetch_timeout_ms
        ] do
      previous = Application.get_env(:ferricstore, key)
      on_exit(fn -> restore_env(key, previous) end)
    end

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn _, _ ->
      {:ok, %{states: %{}}}
    end)

    type = "sixth-source-#{System.unique_integer([:positive])}"
    %{type: type, partition: type <> "-partition"}
  end

  test "cold read errors and actual timeouts are unavailable rather than empty", context do
    for {fun, expected} <- [
          {fn _, _ -> {:error, :storage_unavailable} end, :error},
          {fn _, _ ->
             Process.sleep(100)
             {:ok, []}
           end, :timeout}
        ] do
      Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fun)
      Application.put_env(:ferricstore, :flow_dashboard_list_fetch_timeout_ms, 10)
      data = Browse.collect_states_page(scope(context) ++ [state: "failed"])
      assert Map.get(data, :source_status) == :unavailable
      assert Map.get(data, :terminal_source_status) == expected
      html = Dashboard.render_flow_states_page(data)
      assert html =~ "Terminal records unavailable"
      assert html =~ "Retry current scope"
      refute html =~ "No Flow states discovered"
      refute html =~ "No Flow records discovered"
      refute html =~ "0 matching records"

      query =
        URI.encode_query(%{type: context.type, partition_key: context.partition, state: "failed"})

      assert {:ok, live} = Dashboard.live_payload("flow/states?" <> query)
      assert live.components["flow_states_sources"] =~ "Terminal records unavailable"
      assert live.components["flow_states_chart"] == ""
      refute live.components["flow_states_table"] =~ "0 matching records"
    end
  end

  test "failed cold source preserves hot evidence as explicitly partial", context do
    assert :ok =
             FerricStore.flow_create(context.type <> "-hot",
               type: context.type,
               state: "queued",
               partition_key: context.partition
             )

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn _, _ ->
      {:error, :timeout}
    end)

    data = Browse.collect_states_page(scope(context))
    assert Map.get(data, :source_status) == :partial
    assert Map.get(data, :terminal_source_status) == :timeout
    assert length(data.records) == 1
    assert Enum.sum(Enum.map(data.states, & &1.count)) == 1
    assert Dashboard.render_flow_states_page(data) =~ "Partial results"
  end

  test "successful empty terminal reads remain honest empty states", context do
    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn _, _ -> {:ok, []} end)
    data = Browse.collect_states_page(scope(context) ++ [state: "failed"])
    assert Map.get(data, :source_status) == :ok
    assert Map.get(data, :terminal_source_status) == :ok
    assert Dashboard.render_flow_states_page(data) =~ "No Flow states discovered"
  end

  test "Recent Limit only bounds displayed records, not cold summary sampling", context do
    owner = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, _ ->
      [_, limit] = Regex.run(~r/ LIMIT ([0-9]+)/, query)
      send(owner, {:cold_limit, String.to_integer(limit)})
      records = for id <- 1..10, do: record(context, id, "failed")
      {:ok, %{records: Enum.take(records, String.to_integer(limit))}}
    end)

    for limit <- [1, 5] do
      data = Browse.collect_states_page(scope(context) ++ [state: "failed", limit: limit])
      assert data.total_sampled == 10
      assert data.sample_limit == 500
      assert Enum.sum(Enum.map(data.states, & &1.count)) == 10
      assert length(data.records) == limit
      assert_receive {:cold_limit, 100}
      refute_receive {:cold_limit, _}, 10
    end
  end

  test "failed or raising policy sources are unknown, not parallel", context do
    for fun <- [
          fn _, _ -> {:error, :unavailable} end,
          fn _, _ -> raise "unavailable policy source" end,
          fn _, _ -> exit(:timeout) end
        ] do
      Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fun)
      assert Fifo.effective_state_mode(context.type, "queued") == :unknown
      [summary] = Fifo.annotate_state_summaries([%{type: context.type, state: "queued"}])
      assert summary.mode == :unknown
    end
  end

  test "successful default policies still establish parallel mode", context do
    assert Fifo.effective_state_mode(context.type, "queued") == :parallel
  end

  test "hot-only scopes retain the existing 400-record sample cap", context do
    data = Browse.collect_states_page(scope(context) ++ [state: "queued"])
    assert data.terminal_source_status == :not_requested
    assert data.sample_limit == 400
  end

  test "actual policy timeout is bounded and keeps detail mode unavailable", context do
    Application.put_env(:ferricstore, :flow_dashboard_list_fetch_timeout_ms, 10)

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn _, _ ->
      Process.sleep(100)
      {:ok, %{states: %{}}}
    end)

    assert Fifo.effective_state_mode(context.type, "queued") == :unknown

    html =
      FerricstoreServer.Health.Dashboard.Render.FlowDetail.render_flow_detail_fifo_lane(%{
        state_mode: :unknown
      })

    assert html =~ "FIFO coverage unavailable"
    assert html =~ "Retry current scope"
  end

  test "mixed policy availability retains verified lanes and labels incomplete coverage",
       context do
    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn type, _ ->
      if type == context.type,
        do: {:error, :unavailable},
        else: {:ok, %{states: %{"queued" => %{mode: :fifo}}}}
    end)

    records = [
      record(context, 1, "queued"),
      %{record(context, 2, "queued") | type: context.type <> "-known"}
    ]

    snapshot = Fifo.lane_snapshot(records)
    assert snapshot.coverage == %{status: :partial, unavailable_types: [context.type]}
    assert [%{type: known_type}] = snapshot.lanes
    assert known_type == context.type <> "-known"
    html = Records.render_flow_fifo_lanes(snapshot.lanes, 2, 400, snapshot.coverage)
    assert html =~ "FIFO coverage unavailable for some types"
    assert html =~ "1 verified lanes"
  end

  test "policy failure preserves unknown FIFO coverage in initial and live state views",
       context do
    assert :ok =
             FerricStore.flow_create(context.type <> "-hot",
               type: context.type,
               state: "queued",
               partition_key: context.partition
             )

    owner = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn _, _ ->
      send(owner, :policy_read)
      {:error, :timeout}
    end)

    data = Browse.collect_states_page(scope(context) ++ [state: "queued"])

    assert Map.get(data, :fifo_coverage) == %{
             status: :unavailable,
             unavailable_types: [context.type]
           }

    assert [%{mode: :unknown}] = data.states
    assert_receive :policy_read
    refute_receive :policy_read, 10
    html = Dashboard.render_flow_states_page(data)
    assert html =~ "FIFO coverage unavailable"
    assert html =~ ">Unavailable</span>"
    refute html =~ "No FIFO lanes discovered"

    query =
      URI.encode_query(%{type: context.type, partition_key: context.partition, state: "queued"})

    assert {:ok, live} = Dashboard.live_payload("flow/states?" <> query)
    assert live.components["flow_fifo_lanes"] =~ "FIFO coverage unavailable"
    assert live.components["flow_states_table"] =~ ">Unavailable</span>"
  end

  test "stored-state headers do not call custom application states runtime status" do
    html =
      Records.render_flow_due_records(
        "Due Now",
        [%{id: "manual", type: "review", state: "manual_compliance_review"}],
        1,
        400
      )

    assert html =~ "<th>Stored state</th>"
    assert html =~ "manual_compliance_review"
    refute html =~ "<th>Runtime status</th>"
  end

  defp scope(context), do: [type: context.type, partition_key: context.partition]

  defp record(context, id, state),
    do: %{
      id: "cold-#{id}",
      type: context.type,
      state: state,
      partition_key: context.partition,
      updated_at_ms: id
    }
end
