defmodule Ferricstore.Flow.Query.ExplainTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{IndexDefinition, RegisteredIndex, Request}
  alias Ferricstore.Flow.Query.{Budget, Explain, IndexStatistics, Plan, Planner}

  test "renders deterministic, value-redacted plan evidence" do
    index = active_index()
    first = request("tenant-secret-a", "failed", 100, 200)
    second = request("tenant-secret-b", "completed", 300, 400)

    assert {:ok, first_plan} = Planner.plan(first, [index], now_ms: 1_000)
    assert {:ok, second_plan} = Planner.plan(second, [index], now_ms: 1_000)

    first_explain = Explain.render(first_plan, first)
    second_explain = Explain.render(second_plan, second)

    assert first_explain == second_explain
    assert first_explain.version == "ferric.flow.explain/v1"
    assert first_explain.status == "planned"
    assert first_explain.plan.path == "ordered_range"

    assert first_explain.plan.index == %{
             logical_id: "flow_runs_tenant_state_updated",
             generation: 1,
             build_id: index.build_id
           }

    assert first_explain.plan.range_count == 1
    assert first_explain.plan.order == "native"

    assert first_explain.plan.mandatory_scope == %{
             mode: "dedicated",
             generation: 0,
             branch_count: 1,
             enforcement: "logical_partition",
             values_redacted: true
           }

    assert first_explain.quality.pagination == "live_seek"
    assert first_explain.decision.reason == "only_bounded_candidate"
    assert first_explain.decision.bounded_candidate_count == 1
    assert first_explain.decision.cost_model == "ferric.flow.cost/v1"

    assert first_explain.pressure.estimated_limiting_resource in [
             "range_seeks",
             "scanned_entries",
             "scanned_bytes",
             "hydrated_records",
             "result_records",
             "executor_memory_bytes"
           ]

    assert first_explain.pressure.actual_limiting_resource == nil

    assert first_explain.estimate.planner_memory_bytes == nil

    assert %{estimated: nil, actual: nil, bound: 4_194_304} =
             Enum.find(first_explain.pressure.resources, &(&1.name == "planner_memory_bytes"))

    assert first_explain.estimate.executor_memory_bytes >= 0
    assert first_explain.estimate.scanned_entries >= 0
    assert first_explain.estimate.hard_scanned_entries >= first_explain.estimate.scanned_entries
    refute Map.has_key?(first_explain.estimate, :scan_entries)
    assert first_explain.bounds.scanned_entries > 0
    refute Map.has_key?(first_explain.bounds, :scan_entries)

    encoded = :erlang.term_to_binary(first_explain)
    refute encoded =~ "tenant-secret-a"
    refute encoded =~ "tenant-secret-b"
    refute encoded =~ "failed"
    refute encoded =~ "completed"

    for range <- first_plan.ranges do
      refute encoded =~ range.prefix
      refute encoded =~ range.after_key
      refute encoded =~ range.before_key
    end
  end

  test "lists alternatives deterministically without physical bounds" do
    state_index = active_index()

    type_index =
      IndexDefinition.new!(%{
        id: "flow_runs_tenant_type_updated",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:type, :asc},
          {:updated_at_ms, :desc}
        ]
      })
      |> active_index()

    request =
      Request.collection(
        :explain,
        [eq(:partition_key, "tenant"), eq(:state, "failed"), eq(:type, "invoice")],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert {:ok, plan} = Planner.plan(request, [type_index, state_index], now_ms: 1_000)
    explain = Explain.render(plan, request)

    assert Enum.map(explain.alternatives, & &1.index.logical_id) ==
             Enum.sort(Enum.map(explain.alternatives, & &1.index.logical_id))

    assert Enum.all?(explain.alternatives, fn alternative ->
             is_binary(alternative.index.build_id) and
               alternative.comparison.reason_not_selected in [
                 "higher_estimated_cost",
                 "higher_hard_scan_ceiling",
                 "requires_bounded_sort",
                 "more_ranges",
                 "stable_index_tiebreak"
               ] and
               alternative.comparison.cost_delta >= 0 and
               is_boolean(alternative.sort_required) and
               is_integer(alternative.residual_predicate_count) and
               is_map(alternative.stats)
           end)

    assert explain.decision.reason == "lowest_cost_bounded_candidate"
    assert explain.decision.bounded_candidate_count == 2

    refute inspect(explain.alternatives) =~ "flow-composite-index"
  end

  test "renders a stable rejected plan with a bounded reason" do
    request = request("tenant", "failed", 100, 200)
    assert {:ok, %Plan{path: :reject} = plan} = Planner.plan(request, [], now_ms: 1_000)

    assert %{
             status: "rejected",
             plan: %{
               path: "reject",
               fallback_reason: "no_active_bounded_index",
               index: nil,
               range_count: 0
             },
             quality: %{
               exactness: "not_applicable",
               freshness: "not_applicable",
               coverage: "unavailable",
               pagination: "none"
             },
             decision: %{reason: "no_active_bounded_index", bounded_candidate_count: 0},
             diagnostic: %{
               "code" => "query_no_bounded_plan",
               "context" => %{
                 "status_command" => "FLOW.QUERY.INDEXES",
                 "suggested_index" => %{"fields" => suggested_fields}
               }
             }
           } = Explain.render(plan, request)

    assert Enum.map(suggested_fields, & &1["name"]) == [
             "partition_key",
             "state",
             "updated_at_ms"
           ]

    refute inspect(Explain.render(plan, request)) =~ "tenant"
  end

  test "renders scalar count semantics without row pagination" do
    request =
      Request.count(:explain, [
        eq(:partition_key, "tenant"),
        eq(:state, "failed")
      ])

    assert {:ok, plan} = Planner.plan(request, [active_index()], now_ms: 1_000)
    explain = Explain.render(plan, request)

    assert explain.plan.return == "count"
    assert explain.plan.limit == nil
    assert explain.plan.order == "none"
    assert explain.quality.pagination == "none"
  end

  test "describes authoritative history pagination without projection semantics" do
    request =
      Request.history(
        :explain,
        [eq(:run_id, "run-history")],
        :asc,
        25
      )

    assert {:ok, %Plan{path: :history} = plan} = Planner.plan(request, [], now_ms: 1_000)

    assert Explain.render(plan, request).quality == %{
             exactness: "authoritative",
             freshness: "current",
             coverage: "complete",
             pagination: "authenticated_seek"
           }
  end

  test "describes authoritative lineage reads without projection semantics" do
    request =
      Request.collection(
        :explain,
        [
          eq(:partition_key, "tenant"),
          eq(:parent_flow_id, "parent")
        ],
        [{:updated_at_ms, :desc}],
        25,
        :record
      )

    assert {:ok, %Plan{path: :lineage} = plan} = Planner.plan(request, [], now_ms: 1_000)

    assert Explain.render(plan, request).quality == %{
             exactness: "authoritative",
             freshness: "current",
             coverage: "complete",
             pagination: "authenticated_seek"
           }
  end

  test "turns budget plan rejection into bounded remediation instead of crashing" do
    request =
      Request.collection(
        :explain,
        [
          eq(:partition_key, "tenant-secret"),
          {:in, :state, [keyword("failed-secret"), keyword("running-secret")]}
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert {:ok, budget} = Budget.lower(Budget.default(), range_seeks: 1)

    assert {:ok, %Plan{path: :reject, fallback_reason: :range_budget_exceeded} = plan} =
             Planner.plan(request, [active_index()], budget: budget, now_ms: 1_000)

    explain = Explain.render(plan, request)

    assert explain.status == "rejected"
    assert explain.diagnostic["code"] == "query_range_budget_exceeded"
    assert explain.diagnostic["context"]["planner_reason"] == "range_budget_exceeded"
    assert explain.diagnostic["context"]["bounds"]["range_seeks"] == 1
    assert explain.diagnostic["hint"] =~ "Tighten predicates"
    refute inspect(explain) =~ "tenant-secret"
    refute inspect(explain) =~ "failed-secret"
    refute inspect(explain) =~ "running-secret"
  end

  test "keeps the maximum registry alternative set inside the response budget" do
    indexes =
      for number <- 1..32 do
        number = number |> Integer.to_string() |> String.pad_leading(2, "0")

        IndexDefinition.new!(%{
          id: "flow_runs_tenant_state_updated_#{number}",
          version: 1,
          fields: [
            {:partition_key, :asc},
            {:state, :asc},
            {:updated_at_ms, :desc}
          ]
        })
        |> active_index()
      end

    request = request("tenant-secret", "failed-secret", 100, 200)
    assert {:ok, plan} = Planner.plan(request, indexes, now_ms: 1_000)
    explain = Explain.render(plan, request)

    assert explain.decision.bounded_candidate_count == 32
    assert length(explain.alternatives) == 31
    assert Ferricstore.NativeValueCodec.encoded_size(explain) <= Budget.default().response_bytes
    refute inspect(explain) =~ "tenant-secret"
    refute inspect(explain) =~ "failed-secret"
  end

  test "adds validated actual counters without changing logical plan evidence" do
    request = request("tenant", "failed", 100, 200)
    assert {:ok, plan} = Planner.plan(request, [active_index()], now_ms: 1_000)

    actual = %{
      range_seeks: 1,
      range_pages: 2,
      scanned_entries: 12,
      scanned_bytes: 1_024,
      hydrated_records: 4,
      residual_checks: 8,
      duplicate_entries: 1,
      result_records: 3,
      response_bytes: 900,
      memory_high_water_bytes: 2_048,
      wall_time_us: 700
    }

    assert {:ok, explained} = Explain.executed(plan, request, actual)
    assert explained.status == "executed"
    assert explained.actual == actual
    assert explained.plan == Explain.render(plan, request).plan

    assert explained.pressure.actual_limiting_resource in [
             "range_seeks",
             "scanned_entries",
             "scanned_bytes",
             "hydrated_records",
             "result_records",
             "response_bytes",
             "executor_memory_bytes",
             "wall_time_us"
           ]

    assert %{
             actual: 12,
             actual_utilization_ppm: actual_utilization,
             estimated_utilization_ppm: estimated_utilization
           } = Enum.find(explained.pressure.resources, &(&1.name == "scanned_entries"))

    assert actual_utilization >= 0
    assert estimated_utilization >= 0

    assert {:error, :invalid_query_usage} =
             Explain.executed(plan, request, %{actual | scanned_entries: -1})
  end

  test "distinguishes expected pressure from the hard execution ceiling" do
    index = active_index()
    request = request("tenant-secret", "failed-secret", 100, 200)

    statistics =
      IndexStatistics.new!(%{
        index_id: index.definition.id,
        index_version: index.definition.version,
        scope_digest: IndexStatistics.scope_digest("tenant-secret"),
        collected_at_ms: 1_000,
        source_watermark: 10,
        total_entries: 100,
        distinct_runs: 100,
        prefix_counts: %{
          IndexStatistics.prefix_digest(["tenant-secret", "failed-secret"]) => 100
        },
        prefix_observed_at_ms: %{
          IndexStatistics.prefix_digest(["tenant-secret", "failed-secret"]) => 1_000
        },
        histograms: %{},
        null_counts: %{},
        missing_counts: %{},
        average_entry_bytes: 96,
        average_row_bytes: 384,
        sample_rate_ppm: 1_000_000,
        confidence: :high
      })

    assert {:ok, plan} =
             Planner.plan(request, [index],
               now_ms: 1_000,
               stats: %{{index.definition.id, index.definition.version} => statistics}
             )

    pressure = Explain.render(plan, request).pressure
    scanned = Enum.find(pressure.resources, &(&1.name == "scanned_entries"))

    assert scanned.estimated == 100
    assert scanned.hard_estimated == 50_000
    assert scanned.estimated_utilization_ppm == 2_000
    assert scanned.hard_utilization_ppm == 1_000_000
    assert pressure.estimated_limiting_resource == "result_records"
    assert pressure.hard_limiting_resource == "scanned_entries"
  end

  defp request(tenant, state, lower, upper) do
    Request.collection(
      :explain,
      [
        eq(:partition_key, tenant),
        eq(:state, state),
        {:time_window, :updated_at_ms, integer(lower), integer(upper)}
      ],
      [{:updated_at_ms, :desc}],
      25,
      :record
    )
  end

  defp active_index(definition \\ nil)

  defp active_index(nil) do
    IndexDefinition.new!(%{
      id: "flow_runs_tenant_state_updated",
      version: 1,
      fields: [
        {:partition_key, :asc},
        {:state, :asc},
        {:updated_at_ms, :desc}
      ]
    })
    |> active_index()
  end

  defp active_index(definition) do
    RegisteredIndex.new!(definition, :active,
      coverage: %{complete_shards: 1, total_shards: 1, validation: :passed}
    )
  end

  defp eq(field, value), do: {:eq, field, keyword(value)}
  defp keyword(value), do: {:literal, :keyword, value}
  defp integer(value), do: {:literal, :integer, value}
end
