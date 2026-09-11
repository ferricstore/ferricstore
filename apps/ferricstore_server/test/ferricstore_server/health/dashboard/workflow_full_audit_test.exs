defmodule FerricstoreServer.Health.Dashboard.WorkflowFullAuditTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Flow.{Detail, Schedules}

  alias FerricstoreServer.Health.Dashboard.Render.{
    FlowDetail,
    FlowGovernance,
    FlowOverview,
    FlowQueryResults,
    FlowQueryControls,
    FlowSchedules,
    FlowPolicy,
    FlowIndexCatalog,
    FlowQueryExport
  }

  test "read-only capabilities show requirements without inviting an impossible mutation" do
    data = %{record: record(), history: [], action_capabilities: %{signal: false, rewind: false}}
    html = FlowDetail.render_flow_actions(data)
    assert html =~ "+FLOW.SIGNAL"
    assert html =~ "+FLOW.REWIND"
    refute html =~ ~s(action="/dashboard/flow/a/signal")
    refute html =~ ~s(action="/dashboard/flow/a/rewind")

    assert FlowDetail.render_flow_signal_action(%{data | action_capabilities: %{signal: true}}) =~
             ~s(action="/dashboard/flow/a/signal")
  end

  test "prepared capabilities honor both command grants and partition write access" do
    name = "audit-capabilities-#{System.unique_integer([:positive])}"
    assert :ok = FerricStore.flow_create(name, type: name, partition_key: name, state: "queued")

    assert :ok =
             FerricstoreServer.Acl.set_user(name, [
               "on",
               ">test-password",
               "-@all",
               "+FLOW.GET",
               "+FLOW.SIGNAL",
               "%R~" <> name
             ])

    on_exit(fn -> FerricstoreServer.Acl.del_user(name) end)
    data = Detail.collect_page(name, partition_key: name, acl_username: name, values: false)
    assert data.record_status == :ok
    assert data.action_capabilities == %{signal: false, rewind: false}
    assert :ok = FerricstoreServer.Acl.set_user(name, ["%W~" <> name])

    assert Detail.collect_page(name, partition_key: name, acl_username: name, values: false).action_capabilities ==
             %{signal: true, rewind: false}

    assert :ok = FerricstoreServer.Acl.set_user(name, ["+FLOW.REWIND", "+FLOW.HISTORY"])

    assert Detail.collect_page(name, partition_key: name, acl_username: name, values: false).action_capabilities ==
             %{signal: true, rewind: true}
  end

  test "missing records have recovery rather than execution sections or live freshness" do
    html =
      Dashboard.render_flow_detail_page(%{
        id: "missing<&",
        partition_key: "p &",
        record: nil,
        record_status: :not_found,
        history: [],
        history_status: :ok
      })

    assert html =~ "Workflow not found"
    assert html =~ "Open workflow"
    assert html =~ ~s(aria-label="Correct workflow lookup")
    assert html =~ ~s(name="id" value="missing&lt;&amp;" required)
    assert html =~ ~s(name="partition_key" value="p &amp;")
    assert html =~ "partition_key=p+%26"
    refute html =~ ~s(data-dashboard-live-url=)
    refute html =~ ~s(id="workflow-timeline")
    refute html =~ ~s(id="workflow-actions")
    refute html =~ "missing<&"
  end

  test "auto-partition capabilities use the workflow ID submitted by action forms" do
    name = "audit-auto-capabilities-#{System.unique_integer([:positive])}"
    assert :ok = FerricStore.flow_create(name, type: name, state: "queued")

    assert :ok =
             FerricstoreServer.Acl.set_user(name, [
               "on",
               ">password",
               "-@all",
               "+FLOW.GET",
               "+FLOW.SIGNAL",
               "%R~*",
               "%W~" <> name
             ])

    on_exit(fn -> FerricstoreServer.Acl.del_user(name) end)
    data = Detail.collect_page(name, acl_username: name, values: false)
    assert data.record_status == :ok
    assert data.partition_key == nil
    assert data.action_capabilities.signal
    html = FlowDetail.render_flow_signal_action(data)
    refute html =~ ~s(name="partition_key")
    assert html =~ ~s(action="/dashboard/flow/#{name}/signal")
  end

  test "governance controls retain visible labels when populated" do
    html =
      FlowGovernance.render_flow_governance_filters(%{}) <>
        FlowGovernance.render_flow_governance_circuit_actions(%{
          circuit_review: %{
            status: :ok,
            scope: "effect:review",
            circuit: %{scope: "effect:review", status: :closed},
            fingerprint: "review-test"
          }
        })

    for name <- ~w(scope flow_id approval_status circuit_status limit failure_threshold open_ms) do
      assert html =~
               Regex.compile!(
                 ~s|<label[^>]*>\\s*<span>[^<]+</span>\\s*<(?:input\x7cselect)[^>]*name="#{name}"|
               )
    end
  end

  test "metric descriptions remain inside their definition rather than beside it" do
    html = FlowOverview.render_flow_overview(%{}, 0, 400)
    refute html =~ ~r/<\/dd>\s*<span>/
    assert html =~ ~r/<dd>.*?<span>.*?<\/span><\/dd>/s
  end

  test "failure metric descriptions belong to their definition values" do
    html =
      FerricstoreServer.Health.Dashboard.Render.FlowComponents.render_flow_failures_summary(%{
        summary: %{failed: 2}
      })

    refute html =~ ~r/<\/dd>\s*<span>/
    assert html =~ ~s(<dd class="c-red">2<span>terminal records</span></dd>)
  end

  test "index catalog section and scroll region have distinct accessible names" do
    html = FlowIndexCatalog.render(%{status: :ok, snapshot: %{"indexes" => []}})
    assert html =~ ~s(<section aria-labelledby="query-index-lifecycle-title">)
    assert html =~ ~s(role="region" aria-label="Query index lifecycle table")
    refute html =~ ~s(role="region" aria-label="Query index lifecycle")
  end

  test "unexecuted and failed query results do not masquerade as empty result tables" do
    for status <- [:idle, :error, :timeout] do
      assert FlowQueryResults.render_flow_query_table(%{status: status, rows: []}) == ""
    end

    assert FlowQueryResults.render_flow_query_table(%{status: :ok, rows: []}) =~ "No rows"
  end

  test "single-row results do not render redundant distribution charts" do
    assert FlowQueryResults.render_flow_query_visualization(%{
             visualization: %{
               scope: :current_page,
               row_count: 1,
               charts: [%{kind: :category, field: :state, values: [%{label: "failed", count: 1}]}]
             }
           }) == ""
  end

  test "time distributions expose exact bucket counts as an accessible table" do
    html =
      FlowQueryResults.render_flow_query_visualization(%{
        visualization: %{
          scope: :current_page,
          row_count: 2,
          charts: [
            %{
              kind: :time,
              field: "updated_at_ms",
              values: [
                %{from_ms: 1_000, to_ms: 2_000, count: 2},
                %{from_ms: 2_000, to_ms: 3_000, count: 0}
              ]
            }
          ]
        }
      })

    assert html =~ "Time bucket values"
    assert html =~ "Start (UTC)"
    assert html =~ "End (UTC)"
    assert html =~ ~s(<td class="num">0</td>)
  end

  test "query vocabulary distinguishes runtime status from logical workflow state" do
    assert FlowQueryControls.render_flow_query_state_field(%{kind: "list", state: nil}) =~
             "Runtime status"

    assert FlowQueryControls.render_flow_query_run_state_field(%{kind: "list"}) =~
             "Workflow state"

    assert FlowDetail.render_flow_detail(%{record: record()}) =~ "Workflow state"
  end

  test "query export preserves projection order and typed values without unselected payloads" do
    result = %{
      status: :ok,
      source: :runs,
      presentation: :workbench,
      columns: ["id", "attributes.score", "attributes.flag", "attributes.absent"],
      column_selectors: [
        :run_id,
        {:attribute, "score"},
        {:attribute, "flag"},
        {:attribute, "absent"}
      ],
      rows: [
        %{id: "</script><&", attributes: %{"score" => 42, "flag" => false}, payload: "secret"}
      ]
    }

    assert {:ok, json} = FlowQueryExport.encode(result)

    assert Jason.decode!(json) == %{
             "scope" => "current_page",
             "columns" => result.columns,
             "rows" => [["</script><&", 42, false, nil]]
           }

    html = FlowQueryExport.render(result)
    assert html =~ "Download current page"
    refute html =~ "</script><&"
    refute html =~ "secret"
    assert FlowQueryExport.render(%{status: :idle}) == ""
  end

  test "export has an explicit byte bound and handles binary result values losslessly" do
    result = %{
      status: :ok,
      source: :runs,
      columns: ["attributes.raw"],
      column_selectors: [{:attribute, "raw"}],
      rows: [%{attributes: %{"raw" => <<255>>}}]
    }

    assert {:ok, json} = FlowQueryExport.encode(result)
    assert Jason.decode!(json)["rows"] == [[%{"encoding" => "base64", "data" => "/w=="}]]
    assert {:error, :too_large} = FlowQueryExport.encode(result, max_bytes: 2)
  end

  test "unsupported export values do not crash the query page" do
    result = %{
      status: :ok,
      columns: ["fields"],
      column_selectors: [:fields],
      source: :events,
      rows: [%{fields: %{<<255>> => "binary map key"}}]
    }

    assert {:error, :unsupported_result} = FlowQueryExport.encode(result)
    assert FlowQueryExport.render(result) == ""
  end

  test "export byte limit includes HTML-safe escaping, not only the original JSON" do
    result = %{
      status: :ok,
      columns: ["fields.value"],
      column_selectors: [{:event_field, "value"}],
      source: :events,
      rows: [%{fields: %{"value" => String.duplicate("<&", 150_000)}}]
    }

    assert {:error, :too_large} = FlowQueryExport.encode(result)
    html = FlowQueryExport.render(result)
    assert html =~ "exceeds the 1 MiB"
    refute html =~ ~s(id="flow-query-export")
  end

  test "export retains large integers and event field projections" do
    result = %{
      status: :ok,
      columns: ["event_id", "fields.seq", "fields.absent"],
      column_selectors: [:event_id, {:event_field, "seq"}, {:event_field, "absent"}],
      source: :events,
      rows: [%{"event_id" => "1-0", "fields" => %{"seq" => 9_007_199_254_740_993}}]
    }

    assert {:ok, json} = FlowQueryExport.encode(result)
    assert Jason.decode!(json)["rows"] == [["1-0", 9_007_199_254_740_993, nil]]
  end

  test "index catalog gates the global snapshot before reading and calls the existing status API once" do
    name = "audit-index-reader-#{System.unique_integer([:positive])}"

    assert :ok =
             FerricstoreServer.Acl.set_user(name, [
               "on",
               ">password",
               "-@all",
               "+FLOW.POLICY.GET",
               "~*"
             ])

    on_exit(fn -> FerricstoreServer.Acl.del_user(name) end)

    {denied, calls} =
      trace_index_fetch(fn ->
        FerricstoreServer.Health.Dashboard.Flow.IndexCatalog.collect(acl_username: name)
      end)

    assert denied == %{status: :forbidden}
    assert calls == []
    assert :ok = FerricstoreServer.Acl.set_user(name, ["+FLOW.QUERY.INDEXES"])

    {allowed, calls} =
      trace_index_fetch(fn ->
        FerricstoreServer.Health.Dashboard.Flow.IndexCatalog.collect(acl_username: name)
      end)

    assert allowed.status == :ok
    assert is_list(allowed.snapshot["indexes"])
    assert length(calls) == 1
  end

  test "policy catalog has a separate bounded local filter and visible generation" do
    html = FlowPolicy.render_flow_policies_table([], %{})
    assert html =~ "Filter loaded policies"
    assert html =~ "data-dashboard-table-filter"
    assert html =~ ~s(id="flow-policy-catalog")
    assert html =~ "Generation"
  end

  test "index lifecycle shows actual build validation retirement and statistic metadata" do
    snapshot = %{
      "observed_at_ms" => 1_000,
      "registry" => %{"epoch" => 4, "catalog_version" => 7},
      "services" => %{"registry" => "ready"},
      "indexes" => [
        %{
          "id" => "idx<&",
          "version" => 3,
          "build_id" => "build-9",
          "state" => "building",
          "queryable" => false,
          "fields" => [%{"name" => "type", "direction" => "asc"}],
          "build" => %{"completed_shards" => 2, "total_shards" => 4, "scanned_records" => 42},
          "validation" => %{
            "status" => "failed",
            "mismatches" => 3,
            "failure_reason" => "missing_entry"
          },
          "retirement" => %{"status" => "pending"},
          "statistics" => %{"status" => "stale", "oldest_age_ms" => 12_000}
        }
      ]
    }

    html = FlowIndexCatalog.render(%{status: :ok, snapshot: snapshot})

    for text <- [
          "idx&lt;&amp;",
          "build-9",
          "2 / 4",
          "missing_entry",
          "stale",
          "pending",
          "Generation"
        ] do
      assert html =~ text
    end

    assert FlowIndexCatalog.render(%{status: :forbidden}) =~ "+FLOW.QUERY.INDEXES"
    refute FlowIndexCatalog.render(%{status: :unavailable}) =~ "No indexes"
  end

  test "schedule form offers one-shot UTC instants and recurring UTC bounds" do
    html =
      FlowSchedules.render_flow_schedule_create_form(%{draft: %{"schedule_kind" => "one_shot"}})

    assert html =~ ~s(value="one_shot" selected)
    assert html =~ ~s(name="at_utc")
    assert html =~ ~s(name="start_at_utc")
    assert html =~ ~s(name="end_at_utc")
    assert html =~ "UTC"
  end

  test "absolute one-shot and bounded interval forms reach the real schedule parser" do
    id = "audit-schedule-#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             Schedules.apply_form(%{
               "action" => "create",
               "id" => id,
               "target_type" => "audit",
               "schedule_kind" => "one_shot",
               "at_utc" => "2035-03-30T01:30"
             })

    assert {:ok, schedule} = FerricStore.flow_schedule_get(id)
    assert schedule.kind == :one_shot
    assert schedule.next_run_at_ms == DateTime.to_unix(~U[2035-03-30 01:30:00Z], :millisecond)

    assert {:ok, _} =
             Schedules.apply_form(%{
               "action" => "create",
               "id" => id <> "-interval",
               "target_type" => "audit",
               "schedule_kind" => "interval",
               "every_ms" => "60000",
               "start_at_utc" => "2035-03-30T01:30",
               "end_at_utc" => "2035-03-30T03:30"
             })

    assert {:ok, recurring} = FerricStore.flow_schedule_get(id <> "-interval")
    assert recurring.end_at_ms == DateTime.to_unix(~U[2035-03-30 03:30:00Z], :millisecond)
  end

  test "invalid UTC times and incompatible timing fields cannot silently broaden schedules" do
    base = %{"action" => "create", "id" => "invalid-audit", "target_type" => "audit"}

    for extra <- [
          %{"schedule_kind" => "one_shot", "at_utc" => "2035-02-30T09:00"},
          %{"schedule_kind" => "one_shot", "at_utc" => "2035-03-30T09:00:00+02:00"},
          %{"schedule_kind" => "one_shot", "at_utc" => "2035-03-30T09:00", "every_ms" => "1"},
          %{"schedule_kind" => "delay", "delay_ms" => "1", "end_at_utc" => "2035-03-30T09:00"},
          %{
            "schedule_kind" => "interval",
            "every_ms" => "1000",
            "start_at_utc" => "2035-03-30T09:00",
            "end_at_utc" => "2035-03-30T08:00"
          }
        ] do
      assert {:error, _} = Schedules.apply_form(Map.merge(base, extra))
    end
  end

  defp record, do: %{id: "a", type: "audit", state: "queued", partition_key: "p"}

  defp trace_index_fetch(fun) do
    module = Ferricstore.Flow.Query.IndexStatus
    Code.ensure_loaded!(module)
    parent = self()

    pid =
      spawn(fn ->
        receive do
          :collect ->
            send(parent, {:collected, self(), fun.()})
            receive do: (:stop -> :ok)
        end
      end)

    :erlang.trace_pattern({module, :fetch, 3}, true, [:local])
    :erlang.trace(pid, true, [:call, {:tracer, parent}])

    try do
      send(pid, :collect)
      assert_receive {:collected, ^pid, result}, 10_000
      delivered = :erlang.trace_delivered(pid)
      assert_receive {:trace_delivered, ^pid, ^delivered}, 5_000
      {result, drain_calls(pid, [])}
    after
      :erlang.trace_pattern({module, :fetch, 3}, false, [:local])
      :erlang.trace(pid, false, [:call])
      send(pid, :stop)
    end
  end

  defp drain_calls(pid, calls) do
    receive do
      {:trace, ^pid, :call, call} -> drain_calls(pid, [call | calls])
    after
      0 -> calls
    end
  end
end
