defmodule FerricstoreServer.Health.Dashboard.QueryFifthReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias Ferricstore.Flow.Query.Builder
  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Flow.QueryWorkbench
  alias FerricstoreServer.Health.Dashboard.Render.{FlowQueryControls, FlowQueryResults}

  setup do
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    :ok
  end

  test "real Guided queries return only the literal any type, never unrelated types" do
    partition = "query-fifth-any-#{System.unique_integer([:positive])}"

    assert :ok =
             FerricStore.flow_create(partition <> "-literal",
               type: "any",
               partition_key: partition
             )

    assert :ok =
             FerricStore.flow_create(partition <> "-other",
               type: "other-type",
               partition_key: partition
             )

    flush_projection()

    data =
      Dashboard.collect_flow_query_page(
        kind: "list",
        type: "any",
        state: "queued",
        partition_key: partition
      )

    assert data.result.status == :ok, inspect(data.result)
    assert Enum.map(data.result.rows, & &1.id) == [partition <> "-literal"]

    response =
      http_get(
        FerricstoreServer.Health.Endpoint.port(),
        "/dashboard/flow/query?" <>
          URI.encode_query(%{
            "kind" => "list",
            "type" => "any",
            "state" => "queued",
            "partition_key" => partition
          })
      )

    assert extract_status_code(response) == 200
    html = extract_body(response)
    assert html =~ partition <> "-literal"
    refute html =~ partition <> "-other"
  end

  test "empty Text attribute is a real indexed predicate distinct from missing and nonempty" do
    type = "query-fifth-text-#{System.unique_integer([:positive])}"
    partition = type
    assert {:ok, _} = FerricStore.flow_policy_set(type, indexed_attributes: ["flag"])

    for {suffix, attrs} <- [
          {"empty", %{"flag" => ""}},
          {"nonempty", %{"flag" => "value"}},
          {"missing", %{}}
        ] do
      assert :ok =
               FerricStore.flow_create(type <> suffix,
                 type: type,
                 partition_key: partition,
                 attributes: attrs
               )
    end

    flush_projection()

    opts = [
      kind: "search",
      type: type,
      partition_key: partition,
      attribute_key: "flag",
      attribute_value_type: "string",
      attribute_value: ""
    ]

    data = Dashboard.collect_flow_query_page(opts)
    assert data.result.status == :ok
    assert Enum.map(data.result.rows, & &1.id) == [type <> "empty"]

    query =
      opts |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end) |> URI.encode_query()

    response =
      http_get(FerricstoreServer.Health.Endpoint.port(), "/dashboard/flow/query?" <> query)

    assert extract_status_code(response) == 200
    html = extract_body(response)
    assert html =~ type <> "empty"
    refute html =~ type <> "nonempty"
    refute html =~ type <> "missing"
  end

  test "real expired-lease continuation retains deadline values and payload-free projection" do
    type = "query-fifth-expired-#{System.unique_integer([:positive])}"
    now = System.system_time(:millisecond) - 120_000

    for suffix <- ["a", "b"] do
      assert {:ok, _} =
               FerricStore.flow_start_and_claim(type <> suffix, type, "queued",
                 partition_key: type,
                 worker: "worker",
                 now_ms: now,
                 lease_ms: 1_000
               )
    end

    flush_projection()

    first =
      Dashboard.collect_flow_query_page(kind: "stuck", type: type, partition_key: type, limit: 1)

    assert first.result.status == :ok
    assert first.result.page.has_more
    [row] = first.result.rows

    assert FerricstoreServer.Health.Dashboard.Flow.QueryProjection.value(
             row,
             :runs,
             :lease_deadline_ms
           ) == now + 1_000

    refute Map.has_key?(row, :payload)
    continuation = first.result.continuation

    assert {:ok, prepared, form} =
             QueryWorkbench.prepare(%{
               "fql" => continuation.fql,
               "params_json" => continuation.params_json,
               "surface" => "guided",
               "guided_query" => continuation.guided_query,
               "cursor" => continuation.cursor,
               "action" => "run"
             })

    assert elem(prepared.ast, 1).projection == [
             :run_id,
             :type,
             :state,
             :run_state,
             :updated_at_ms,
             :lease_deadline_ms,
             :partition_key
           ]

    second = Dashboard.collect_flow_query_workbench_page(prepared, form)
    assert second.result.status == :ok
    assert second.result.column_selectors == first.result.column_selectors
    assert hd(second.result.rows).id != row.id
    html = Dashboard.render_flow_query_page(second)
    assert html =~ ">Lease deadline</th>"
    assert html =~ "expired at capture"
    assert html =~ "partition_key=#{type}"
  end

  test "literal any remains an explicit Guided type, runtime status and workflow state" do
    data =
      Dashboard.collect_flow_query_page(
        kind: "list",
        inspect: true,
        type: "any",
        state: "any",
        run_state: "any",
        partition_key: "tenant"
      )

    assert data.guided_import.fql =~ "type = @type"
    assert data.guided_import.fql =~ "state = @state"
    assert data.guided_import.fql =~ "run_state = @run_state"
    params = Jason.decode!(data.guided_import.params_json)

    assert Map.take(params, ~w(type state run_state)) == %{
             "type" => "any",
             "state" => "any",
             "run_state" => "any"
           }
  end

  test "omitted dashboard state uses an explicit wildcard without changing Builder queued default" do
    data =
      Dashboard.collect_flow_query_page(inspect: true, type: "email", partition_key: "tenant")

    refute data.guided_import.fql =~ "state = @state"
    assert {:ok, default} = Builder.build(:list, %{type: "email", partition_key: "tenant"})
    assert default.params["state"] == "queued"

    assert {:ok, all} =
             Builder.build(:list, %{type: "email", state: :any, partition_key: "tenant"})

    refute Map.has_key?(all.params, "state")

    assert {:ok, terminals} =
             Builder.build(:terminals, %{type: "email", state: :any, partition_key: "tenant"})

    assert terminals.query =~ "state IN"
  end

  test "Search exposes its required either-or predicate and invalid controls reveal disclosures" do
    data =
      Dashboard.collect_flow_query_page(
        kind: "search",
        inspect: true,
        type: "email",
        partition_key: "tenant"
      )

    html = Dashboard.render_flow_query_page(data)
    assert html =~ ~s(class="flow-query-advanced" open)
    assert html =~ "Search requires an indexed attribute or state metadata predicate."
    script = FlowQueryControls.render_flow_query_dynamic_script()
    assert script =~ ~s(addEventListener("invalid")
    assert script =~ "details.open = true"
    assert script =~ "type.value !== \"string\""
  end

  test "query drafts protect Refresh and native navigation without new persistent storage" do
    script = FlowQueryControls.render_flow_query_mode_script(:guided)
    assert script =~ "dashboard:before-refresh"
    assert script =~ "beforeunload"
    assert script =~ "Discard unsubmitted query edits and refresh?"
  end

  test "query controls and executed inputs name the selected clock" do
    for {kind, clock, field} <- [
          {"list", "Updated time", "updated_at_ms"},
          {"stuck", "Lease deadline", "lease_deadline_ms"}
        ] do
      data =
        Dashboard.collect_flow_query_page(
          kind: kind,
          inspect: true,
          type: "email",
          partition_key: "tenant"
        )

      html =
        Dashboard.render_flow_query_page(%{
          data
          | result: %{status: :ok, command: "FLOW.QUERY", rows: []}
        })

      assert html =~ "#{clock} from UTC"
      assert html =~ "#{clock} to UTC"
      assert html =~ "Latest #{String.downcase(clock)} first"
      assert html =~ "&quot;time_field&quot;: &quot;#{field}&quot;"
    end
  end

  test "expired Guided query prepares a payload-free deadline projection and preserves it on continuation" do
    data =
      Dashboard.collect_flow_query_page(
        kind: "stuck",
        inspect: true,
        type: "email",
        partition_key: "tenant"
      )

    form = data.guided_import

    assert form.fql =~
             "RETURN RECORDS (run_id, type, state, run_state, updated_at_ms, lease_deadline_ms, partition_key)"

    response = %{
      status: :ok,
      command: "FLOW.QUERY",
      rows: [
        %{
          id: "expired",
          partition_key: "tenant",
          type: "email",
          state: "running",
          lease_expires_at_ms: 1_000,
          updated_at_ms: 900
        }
      ]
    }

    for result <- [
          QueryWorkbench.attach_continuation(response, form),
          QueryWorkbench.attach_continuation(Map.put(response, :source, :runs), form)
        ] do
      assert result.column_selectors == [
               :run_id,
               :type,
               :state,
               :run_state,
               :updated_at_ms,
               :lease_deadline_ms
             ]

      html = FlowQueryResults.render_flow_query_table(result)
      assert html =~ ">Lease deadline</th>"
      assert html =~ "expired"
      assert html =~ "1970-01-01"
    end
  end

  test "Analyze joins wall time units into one actual-versus-bound row" do
    html =
      FlowQueryResults.render_flow_query_table(%{
        status: :ok,
        explain: %{actual: %{wall_time_us: 321}, bounds: %{wall_time_ms: 750}}
      })

    rows = Regex.scan(~r/<tr><th scope="row">Wall time<\/th>.*?<\/tr>/s, html) |> List.flatten()
    assert length(rows) == 1
    assert hd(rows) =~ "321"
    assert hd(rows) =~ "750"
  end

  test "Explain alternatives retain index identity costs and escaped build details" do
    html =
      FlowQueryResults.render_flow_query_table(%{
        status: :ok,
        explain: %{
          alternatives: [
            %{
              path: "ordered_filter",
              record_source: "query_row",
              index: %{logical_id: "index_priority", generation: 7, build_id: "build<&"},
              estimate: %{cost: 420},
              comparison: %{cost_delta: 120, reason_not_selected: "higher_estimated_cost"}
            }
          ]
        }
      })

    assert html =~ "index_priority"
    assert html =~ ">Estimated cost</th>"
    assert html =~ ">Cost delta</th>"
    assert html =~ ">420</td>"
    assert html =~ ">120</td>"
    assert html =~ "Generation"
    assert html =~ "build&lt;&amp;"
    refute html =~ "build<&"
  end

  defp flush_projection do
    ctx = FerricStore.Instance.get(:default)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(:default, ctx.shard_count, 30_000)
  end
end
