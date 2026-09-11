defmodule FerricstoreServer.Health.Dashboard.FifoWidgetsTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Flow.Fifo
  alias FerricstoreServer.Health.Dashboard.Flow.Sample
  alias FerricstoreServer.Health.Dashboard.Render.FlowDetail
  alias FerricstoreServer.Health.Dashboard.Render.FlowNavigation
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records
  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.Session
  alias FerricstoreServer.Acl

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    type = "fifo-widget-#{System.unique_integer([:positive])}"
    assert {:ok, _} = FerricStore.flow_policy_set(type, states: %{"queued" => [mode: :fifo]})
    %{type: type}
  end

  test "running summaries use logical-state policy and identify mixed modes", %{type: type} do
    fifo = Map.merge(record(type, 1), %{state: "running", run_state: "queued"})
    parallel = Map.merge(record(type, 2), %{state: "running", run_state: "review"})
    [summary] = [fifo] |> Sample.flow_state_summaries() |> Fifo.annotate_state_summaries()
    assert summary.state == "running"
    assert summary.mode == :fifo

    [mixed] = [fifo, parallel] |> Sample.flow_state_summaries() |> Fifo.annotate_state_summaries()
    assert mixed.mode == :mixed
    html = Records.render_flow_states_table([mixed], 2, 2, 400, %{})
    assert html =~ ">mixed</span>"
    assert html =~ "logical states"
  end

  test "running filter retains the matching leased FIFO lane in initial and live views", %{
    type: type
  } do
    id = "fifo-running-#{System.unique_integer([:positive])}"

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: id,
               run_at_ms: 1
             )

    assert {:ok, [_]} =
             FerricStore.flow_claim_due(type,
               state: "queued",
               partition_key: id,
               worker: "worker",
               limit: 1,
               lease_ms: 60_000
             )

    query = URI.encode_query(%{"type" => type, "state" => "running", "partition_key" => id})

    data =
      query |> Dashboard.flow_states_opts_from_query() |> Dashboard.collect_flow_states_page()

    assert [%{mode: :fifo, state: "running"}] = data.states
    assert [%{state: "queued", blocked_by_id: ^id}] = data.fifo_lanes
    assert [%{id: ^id}] = data.records
    assert {:ok, live} = Dashboard.live_payload("flow/states?" <> query)
    assert live.components["flow_fifo_lanes"] =~ id
    assert live.components["flow_states_table"] =~ ">FIFO</span>"
  end

  test "lane member previews retain at most eight lean records in entry order", %{type: type} do
    records = for seq <- 20..1//-1, do: record(type, seq)
    [lane] = Fifo.lane_summaries(records)
    assert Enum.map(lane.members, & &1.state_enter_seq) == Enum.to_list(1..8)
    assert lane.members_omitted == 12
    assert lane.order_known
    assert lane.coverage == :sampled
    assert lane.count == 20
    refute inspect(lane) =~ "not-for-the-lane-widget"
    refute Enum.any?(lane.members, &Map.has_key?(&1, :payload))
  end

  test "multi-lane overview does not materialize member previews", %{type: type} do
    records = for seq <- 1..400, do: %{record(type, seq) | partition_key: "scope-#{rem(seq, 40)}"}
    lanes = Fifo.lane_summaries(records)
    assert length(lanes) == 40
    assert Enum.all?(lanes, &(&1.members == []))
    html = Records.render_flow_fifo_lanes(lanes, 400, 400)
    refute html =~ "flow-fifo-member-list"
    assert html =~ "Inspect lane"
  end

  test "overview markup remains bounded when the sample contains hundreds of lanes", %{type: type} do
    lanes =
      Fifo.lane_summaries(
        for seq <- 1..400, do: %{record(type, seq) | partition_key: "scope-#{seq}"}
      )

    html = Records.render_flow_fifo_lanes(lanes, 400, 400)
    assert length(Regex.scan(~r/<tr>/, html)) == 41
    assert html =~ "Showing 40 of 400 sampled lanes"
  end

  test "detail lane keeps entry sequence from the matching durable sample", %{type: type} do
    id = "fifo-sequence-#{System.unique_integer([:positive])}"
    assert :ok = FerricStore.flow_create(id, type: type, state: "queued", partition_key: id)
    data = Dashboard.collect_flow_detail_page(id, partition_key: id, values: false)
    member = Enum.find(data.fifo_lane.members, &(&1.id == id))
    assert is_integer(member.state_enter_seq)
  end

  test "leased blocker is retained before waiting previews with its exact deadline", %{type: type} do
    leased =
      Map.merge(record(type, 30), %{
        state: "running",
        run_state: "queued",
        worker: "worker<&",
        lease_expires_at_ms: 1
      })

    [lane] = Fifo.lane_summaries([leased | for(seq <- 1..20, do: record(type, seq))])
    assert hd(lane.members).id == leased.id
    assert hd(lane.members).status == :expired
    assert hd(lane.members).lease_expires_at_ms == 1
    assert lane.blocked_by_id == leased.id
    assert length(lane.members) == 8
  end

  test "missing sequence is explicit and does not pretend to establish a queue position", %{
    type: type
  } do
    [lane] = Fifo.lane_summaries([record(type, 2), Map.delete(record(type, 1), :state_enter_seq)])
    refute lane.order_known
    html = Records.render_flow_fifo_lanes([lane], 2, 400)
    assert html =~ "Sequence unavailable"
    assert html =~ "not the complete queue"
    refute html =~ "head claimable"
  end

  test "observed sequence takes precedence over due time", %{type: type} do
    future = System.system_time(:millisecond) + 3_600_000
    [lane] = Fifo.lane_summaries([record(type, 2), %{record(type, 1) | run_at_ms: future}])
    assert Enum.map(lane.members, & &1.id) == ["member-1", "member-2"]
    assert hd(lane.members).status == :scheduled
    html = Records.render_flow_fifo_lanes([lane], 2, 400)
    assert html =~ "Scheduled in sample"
    refute html =~ "head claimable"
  end

  test "member display is escaped, scoped and expandable without extra reads", %{type: type} do
    member = %{record(type, 1) | id: "run<&", partition_key: "scope&one"}
    [lane] = Fifo.lane_summaries([member, %{record(type, 2) | partition_key: "scope&one"}])
    html = Records.render_flow_fifo_lanes([lane], 2, 400)
    assert html =~ "flow-fifo-members"
    assert html =~ "data-dashboard-live-pause"
    assert html =~ "run&lt;&amp;"
    assert html =~ "partition_key=scope%26one"
    assert html =~ "Observed members"
    assert html =~ "Sequence 1"
    refute html =~ "not-for-the-lane-widget"
  end

  test "related runs preserve exact type and partition and do not add a state filter" do
    record = %{
      id: "run",
      type: "email <&",
      state: "running",
      run_state: "queued",
      partition_key: "scope /&"
    }

    url = FlowNavigation.related_runs_path(record)
    params = url |> URI.parse() |> Map.fetch!(:query) |> URI.decode_query()

    assert params == %{
             "kind" => "list",
             "type" => record.type,
             "partition_key" => record.partition_key,
             "limit" => "40"
           }

    html = FlowDetail.render_flow_detail(%{record: record})
    assert html =~ ">Related runs</a>"
    assert html =~ "partition_key=scope+%2F%26"
    refute FlowNavigation.related_runs_path(%{record | partition_key: nil})
    refute FlowNavigation.related_runs_path(%{record | type: ""})
  end

  test "detail related-run navigation executes an exact scoped query and enforces destination ACL",
       %{type: type} do
    previous = Application.get_env(:ferricstore, :protected_mode)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    Application.put_env(:ferricstore, :protected_mode, false)
    id = "fifo-related-#{System.unique_integer([:positive])}"
    partition = "fifo-related-scope-#{id}"

    assert :ok =
             FerricStore.flow_create(id, type: type, state: "queued", partition_key: partition)

    peer = "fifo-related-peer-#{System.unique_integer([:positive])}"

    assert :ok =
             FerricStore.flow_create(peer, type: type, state: "failed", partition_key: partition)

    excluded = "fifo-unrelated-#{System.unique_integer([:positive])}"

    assert :ok =
             FerricStore.flow_create(excluded,
               type: type,
               state: "queued",
               partition_key: excluded
             )

    ctx = FerricStore.Instance.get(:default)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(:default, ctx.shard_count, 30_000)
    await_indexes(ctx, 100)
    {:ok, record} = FerricStore.flow_get(id, partition_key: partition)
    url = FlowNavigation.related_runs_path(record)
    response = http_get(Endpoint.port(), url)
    assert extract_status_code(response) == 200
    assert extract_body(response) =~ ">#{id}</a>"
    assert extract_body(response) =~ ">#{peer}</a>"
    refute extract_body(response) =~ ">#{excluded}</a>"

    username = "fifo-related-reader-#{id}"
    assert :ok = Acl.set_user(username, ["on", "nopass", "%R~#{partition}", "-@all", "+FLOW.GET"])
    on_exit(fn -> Acl.del_user(username) end)
    Application.put_env(:ferricstore, :protected_mode, true)
    headers = [{"Cookie", Session.session_cookie(username)}]

    detail =
      http_get(
        Endpoint.port(),
        "/dashboard/flow/#{id}?" <> URI.encode_query(%{"partition_key" => partition}),
        headers
      )

    assert extract_status_code(detail) == 200
    assert extract_body(detail) =~ ">Related runs</a>"
    assert extract_status_code(http_get(Endpoint.port(), url, headers)) == 403

    assert :ok = Acl.set_user(username, ["+FLOW.QUERY"])
    headers = [{"Cookie", Session.session_cookie(username)}]
    allowed = http_get(Endpoint.port(), url, headers)
    assert extract_status_code(allowed) == 200
    assert extract_body(allowed) =~ ">#{id}</a>"
    other = FlowNavigation.related_runs_path(%{record | partition_key: "forbidden-scope"})
    assert extract_status_code(http_get(Endpoint.port(), other, headers)) == 403
  end

  test "literal all identifiers survive lane, related-run, and live navigation" do
    previous = Application.get_env(:ferricstore, :protected_mode)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    Application.put_env(:ferricstore, :protected_mode, false)
    partition = "literal-all-#{System.unique_integer([:positive])}"

    for name <- ["all", "ALL", "All"] do
      assert {:ok, _} = FerricStore.flow_policy_set(name, states: %{name => [mode: :fifo]})

      assert :ok =
               FerricStore.flow_create(name, type: name, state: name, partition_key: partition)
    end

    assert :ok =
             FerricStore.flow_create("other",
               type: "other",
               state: "queued",
               partition_key: partition
             )

    ctx = FerricStore.Instance.get(:default)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(:default, ctx.shard_count, 30_000)
    await_indexes(ctx, 100)

    for name <- ["all", "ALL", "All"] do
      lane = %{type: name, state: name, partition_key: partition}
      path = FlowNavigation.lane_path(lane)
      query = URI.parse(path).query
      opts = Dashboard.flow_states_opts_from_query(query)
      assert opts[:type] == name
      assert opts[:state] == name
      assert [%{id: ^name}] = Dashboard.collect_flow_states_page(opts).records
      assert [%{type: ^name, state: ^name}] = Dashboard.collect_flow_states_page(opts).fifo_lanes

      for route <- [path, "/dashboard/api/flow/states?" <> query] do
        response = http_get(Endpoint.port(), route)
        assert extract_status_code(response) == 200
        body = extract_body(response)

        body =
          if String.starts_with?(route, "/dashboard/api"),
            do: Jason.decode!(body)["components"]["flow_recent_records"],
            else: body

        assert body =~ ">#{name}</a>"
        refute body =~ ">other</a>"
      end

      {:ok, record} = FerricStore.flow_get(name, partition_key: partition)
      related = FlowNavigation.related_runs_path(record)

      related_opts =
        related |> URI.parse() |> Map.fetch!(:query) |> Dashboard.flow_query_opts_from_query()

      assert related_opts[:type] == name
      response = http_get(Endpoint.port(), related)
      assert extract_status_code(response) == 200
      assert extract_body(response) =~ ">#{name}</a>"
      refute extract_body(response) =~ ">other</a>"
    end
  end

  test "all is a literal predicate across shared workflow query parsers" do
    for name <- ["all", "ALL", "All"] do
      query =
        URI.encode_query(%{
          "type" => name,
          "state" => name,
          "run_state" => name,
          "state_meta_state" => name
        })

      assert Dashboard.flow_states_opts_from_query(query)[:type] == name
      assert Dashboard.flow_failures_opts_from_query(query)[:type] == name
      assert Dashboard.flow_signals_opts_from_query(query)[:type] == name
      opts = Dashboard.flow_query_opts_from_query(query)
      for key <- [:type, :state, :run_state, :state_meta_state], do: assert(opts[key] == name)
    end
  end

  defp await_indexes(ctx, remaining) do
    {:ok, status} = Ferricstore.Flow.Query.IndexStatus.fetch(ctx)

    unless Enum.all?(status["indexes"], & &1["queryable"]) do
      assert remaining > 0, inspect(status)
      refute Enum.any?(status["indexes"], &(&1["state"] == "failed")), inspect(status)

      assert {:ok, _} =
               Ferricstore.Flow.Query.IndexLifecycleWorker.run_once(
                 Ferricstore.Flow.Query.IndexLifecycleWorker.name(ctx)
               )

      await_indexes(ctx, remaining - 1)
    end
  end

  defp record(type, seq) do
    %{
      id: "member-#{seq}",
      type: type,
      state: "queued",
      partition_key: "scope",
      state_enter_seq: seq,
      run_at_ms: 1,
      created_at_ms: 100 - seq,
      payload: "not-for-the-lane-widget",
      attributes: %{secret: "not-for-the-lane-widget"}
    }
  end
end
