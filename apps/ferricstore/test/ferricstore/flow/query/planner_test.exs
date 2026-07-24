defmodule Ferricstore.Flow.Query.PlannerTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.Query

  alias Ferricstore.Flow.Query.{
    Builder,
    CompositeIndex,
    IndexDefinition,
    MandatoryScope,
    RegisteredIndex,
    Request
  }

  alias Ferricstore.Flow.Query.{
    Budget,
    IndexCatalog,
    IndexStatistics,
    Plan,
    Planner
  }

  @now_ms 1_800_000_000_000

  defmodule SharedScopeProvider do
    @behaviour MetadataExtension

    @impl true
    def configure(_opts) do
      {:ok,
       %{
         mode: :shared,
         generation: 1,
         fields: [
           %{
             id: 0x8001,
             version: 1,
             logical_name: "tenant_ref",
             type: :uint64,
             role: :isolation_scope,
             visibility: :hidden,
             mutability: :immutable,
             index: :required_prefix,
             required_in: :shared
           }
         ]
       }}
    end

    @impl true
    def bind_write(_operation, %{"tenant_ref" => tenant_ref}, _snapshot),
      do: {:ok, %{0x8001 => tenant_ref}}

    @impl true
    def bind_query(:runs, %{"tenant_ref" => tenant_ref}, _snapshot),
      do: {:ok, {:required, [{0x8001, :eq, tenant_ref}]}}

    def bind_query(_source, _context, _snapshot), do: {:error, :flow_scope_required}
  end

  test "run-id-only point reads use the primary key without a composite scan" do
    request = %Request{
      mode: :execute,
      source: :runs,
      predicate: {:and, [{:eq, :run_id, keyword("run-auto")}]},
      order_by: [],
      limit: 1,
      return: :record
    }

    assert {:ok,
            %Plan{
              path: :primary_key,
              index_id: "flow_runs_primary_v1",
              ranges: [],
              estimate: %{scan_entries: 1}
            }} = Planner.plan(request, [])
  end

  test "event history uses the direct history index without registry candidates" do
    request = %Request{
      mode: :execute,
      source: :events,
      predicate: {:and, [{:eq, :run_id, keyword("run-history")}]},
      order_by: [{:event_id, :asc}],
      limit: 25,
      return: :record
    }

    assert {:ok,
            %Plan{
              path: :history,
              index_id: "flow_events_history_v1",
              ranges: [],
              order: :native,
              estimate: %{
                range_seeks: 2,
                scan_entries: 52,
                hard_scan_entries: 52,
                hydrated_records: 26
              }
            }} = Planner.plan(request, [])

    assert {:ok, hydration_limited} = Budget.lower(Budget.default(), hydrated_records: 25)

    assert {:ok, %Plan{path: :reject, fallback_reason: :hydration_budget_exceeded}} =
             Planner.plan(request, [], budget: hydration_limited)

    assert {:ok,
            %Plan{
              path: :history,
              index_id: "flow_events_history_v1",
              index_version: 1,
              index_build_id: "flow_events_history_v1"
            }} =
             Planner.plan(%{request | cursor: {:literal, :keyword, "authenticated-token"}}, [])

    assert {:ok, scan_limited} = Budget.lower(Budget.default(), scan_entries: 51)

    assert {:ok, %Plan{path: :reject, fallback_reason: :scan_budget_exceeded}} =
             Planner.plan(request, [], budget: scan_limited)

    assert {:ok, result_limited} = Budget.lower(Budget.default(), result_records: 24)

    assert {:ok, %Plan{path: :reject, fallback_reason: :result_budget_exceeded}} =
             Planner.plan(request, [], budget: result_limited)
  end

  test "partition-contained parent lineage uses its built-in index" do
    request =
      Request.collection(
        :execute,
        [
          eq(:partition_key, "tenant-a"),
          eq(:parent_flow_id, "parent-1")
        ],
        [{:updated_at_ms, :desc}],
        25,
        :record
      )

    assert {:ok,
            %Plan{
              path: :lineage,
              index_id: "flow_runs_parent_v1",
              index_version: 1,
              index_build_id: "flow_runs_parent_v1",
              ranges: [],
              order: :native,
              estimate: %{
                range_seeks: 2,
                scan_entries: 52,
                hard_scan_entries: 52,
                hydrated_records: 26
              }
            }} = Planner.plan(request, [])

    assert {:ok, hydration_limited} = Budget.lower(Budget.default(), hydrated_records: 25)

    assert {:ok, %Plan{path: :reject, fallback_reason: :hydration_budget_exceeded}} =
             Planner.plan(request, [], budget: hydration_limited)
  end

  test "plans the bounded lease-deadline workload through the launch index" do
    assert {:ok, built} =
             Builder.build(:stuck, %{
               partition_key: "tenant-a",
               type: "invoice",
               now_ms: @now_ms,
               limit: 25
             })

    assert {:ok, request} = Query.prepare_reference("FQL1", built.query, built.params)
    assert {:ok, catalog} = IndexCatalog.load()

    definition =
      Enum.find(
        catalog.definitions,
        &(&1.id == "flow_runs_tenant_type_state_lease_deadline")
      )

    assert {:ok,
            %Plan{
              path: :ordered_range,
              index_id: "flow_runs_tenant_type_state_lease_deadline",
              order: :native,
              residual_predicates: [],
              ranges: [_range]
            }} = Planner.plan(request, [active_index(definition)], now_ms: @now_ms)
  end

  test "root lineage accounts for its authoritative root probe" do
    request =
      Request.collection(
        :execute,
        [
          eq(:root_flow_id, "root-1"),
          eq(:partition_key, "tenant-a")
        ],
        [{:updated_at_ms, :asc}],
        25,
        :record
      )

    assert {:ok,
            %Plan{
              path: :lineage,
              index_id: "flow_runs_root_v1",
              estimate: %{
                range_seeks: 3,
                scan_entries: 53,
                hard_scan_entries: 53,
                hydrated_records: 27,
                result_records: 25
              }
            }} = Planner.plan(request, [])

    assert {:ok, scan_limited} = Budget.lower(Budget.default(), scan_entries: 52)

    assert {:ok, %Plan{path: :reject, fallback_reason: :scan_budget_exceeded}} =
             Planner.plan(request, [], budget: scan_limited)

    assert {:ok, hydration_limited} = Budget.lower(Budget.default(), hydrated_records: 26)

    assert {:ok, %Plan{path: :reject, fallback_reason: :hydration_budget_exceeded}} =
             Planner.plan(request, [], budget: hydration_limited)
  end

  test "plans a selective equality plus half-open time window as one bounded range" do
    index = active_index(state_definition())

    request =
      collection(
        [
          eq(:partition_key, "tenant-a"),
          eq(:state, "failed"),
          {:time_window, :updated_at_ms, integer(100), integer(200)}
        ],
        [{:updated_at_ms, :desc}],
        25
      )

    stats =
      stats(index, "tenant-a", %{
        ["tenant-a"] => 10_000,
        ["tenant-a", "failed"] => 80
      })

    assert {:ok,
            %Plan{
              path: :ordered_range,
              index_build_id: build_id,
              ranges: [range],
              order: :native,
              residual_predicates: [],
              estimate: %{scan_entries: 80, range_seeks: 1, residual_checks: 0},
              stats: %{source: :exact_prefix, confidence: :high}
            }} = Planner.plan(request, [index], stats: stats_lookup(stats), now_ms: @now_ms)

    assert build_id == index.build_id
    assert range.index_id == index.definition.id
    assert {:ok, tenant_prefix} = CompositeIndex.encode_prefix(index.definition, ["tenant-a"])
    assert String.starts_with?(range.prefix, tenant_prefix)

    assert {:ok, upper_prefix} =
             CompositeIndex.encode_prefix(index.definition, ["tenant-a", "failed", 200])

    # Descending physical bounds skip the complete logical-upper bucket.
    assert range.after_key > upper_prefix
    assert String.starts_with?(range.after_key, range.prefix)
  end

  test "plans scalar counts as bounded unordered scans with one scalar result" do
    definition =
      IndexDefinition.new!(%{
        id: "flow_runs_tenant_type_state",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:type, :asc},
          {:state, :asc}
        ]
      })

    index = active_index(definition)

    request =
      Request.count(:execute, [
        eq(:partition_key, "tenant-a"),
        eq(:type, "payment"),
        eq(:state, "failed")
      ])

    statistics =
      stats(index, "tenant-a", %{
        ["tenant-a"] => 10_000,
        ["tenant-a", "payment"] => 200,
        ["tenant-a", "payment", "failed"] => 7
      })

    assert {:ok,
            %Plan{
              path: :count_scan,
              deduplicate: false,
              ranges: [_range],
              order: :none,
              residual_predicates: [],
              estimate: %{
                scan_entries: 7,
                hard_scan_entries: 50_000,
                result_records: 1,
                sort_rows: 0
              }
            }} =
             Planner.plan(request, [index],
               stats: stats_lookup(statistics),
               now_ms: @now_ms
             )

    assert {:ok, memory_budget} =
             Budget.lower(Budget.default(), executor_memory_bytes: 1 * 1_024 * 1_024)

    assert {:ok, %Plan{path: :count_scan}} =
             Planner.plan(request, [index],
               budget: memory_budget,
               stats: stats_lookup(statistics),
               now_ms: @now_ms
             )

    assert {:ok, %Plan{path: :reject, fallback_reason: :no_active_bounded_index}} =
             Planner.plan(request, [], now_ms: @now_ms)
  end

  test "routes a fully covered scalar count to transactional prefix counters" do
    definition =
      IndexDefinition.new!(%{
        id: "flow_runs_tenant_type_state_updated",
        version: 1,
        count_prefixes: [3],
        fields: [
          {:partition_key, :asc},
          {:type, :asc},
          {:state, :asc},
          {:updated_at_ms, :desc}
        ]
      })

    request =
      Request.count(:execute, [
        eq(:partition_key, "tenant-a"),
        eq(:type, "payment"),
        {:in, :state, [keyword("failed"), keyword("completed")]}
      ])

    assert {:ok,
            %Plan{
              path: :counter_lookup,
              ranges: [_, _],
              order: :none,
              deduplicate: false,
              residual_predicates: [],
              estimate:
                %{
                  range_seeks: 2,
                  scan_entries: 2,
                  hard_scan_entries: 2,
                  hydrated_records: 0,
                  result_records: 1,
                  hard_scan_bytes: hard_scan_bytes,
                  planner_memory_bytes: planner_memory_bytes,
                  memory_bytes: memory_bytes
                } = estimate,
              stats: %{source: :transactional_counter, confidence: :exact},
              statistics_probes: []
            }} = Planner.plan(request, [active_index(definition)], now_ms: @now_ms)

    assert estimate.scan_bytes == hard_scan_bytes
    assert memory_bytes > hard_scan_bytes + planner_memory_bytes

    residual =
      Request.count(:execute, [
        eq(:partition_key, "tenant-a"),
        eq(:type, "payment"),
        eq(:state, "failed"),
        eq(:run_state, "waiting")
      ])

    assert {:ok,
            %Plan{
              path: :count_scan,
              residual_predicates: [_residual],
              estimate: %{hydrated_records: hydrated_records, residual_checks: residual_checks}
            }} =
             Planner.plan(residual, [active_index(definition)], now_ms: @now_ms)

    assert residual_checks == hydrated_records
  end

  test "does not sum multivalue IN counters that could double-count one run" do
    definition =
      IndexDefinition.new!(%{
        id: "flow_runs_tenant_tag_updated",
        version: 1,
        count_prefixes: [2],
        fields: [
          {:partition_key, :asc},
          {{:attribute, "tags"}, :asc},
          {:updated_at_ms, :desc}
        ]
      })

    request =
      Request.count(:execute, [
        eq(:partition_key, "tenant-a"),
        {:in, {:attribute, "tags"}, [keyword("blue"), keyword("green")]}
      ])

    assert {:ok, %Plan{path: :count_scan, deduplicate: true}} =
             Planner.plan(request, [active_index(definition)], now_ms: @now_ms)
  end

  test "expands bounded IN to an ordered range union without intersecting scans" do
    index = active_index(state_definition())

    request =
      collection(
        [
          eq(:partition_key, "tenant-a"),
          {:in, :state, [keyword("failed"), keyword("completed")]},
          {:time_window, :updated_at_ms, integer(100), integer(200)}
        ],
        [{:updated_at_ms, :desc}],
        20
      )

    stats =
      stats(index, "tenant-a", %{
        ["tenant-a", "failed"] => 30,
        ["tenant-a", "completed"] => 40
      })

    assert {:ok,
            %Plan{
              path: :ordered_range_union,
              ranges: ranges,
              order: :bounded_top_k,
              estimate: %{scan_entries: 70, range_seeks: 2, sort_rows: 70}
            }} = Planner.plan(request, [index], stats: stats_lookup(stats), now_ms: @now_ms)

    assert length(ranges) == 2
    assert Enum.uniq(Enum.map(ranges, & &1.prefix)) |> length() == 2
  end

  test "predicate and IN value ordering do not change the physical plan" do
    index = active_index(state_definition())

    predicates = [
      eq(:partition_key, "tenant-a"),
      {:in, :state, [keyword("failed"), keyword("completed")]},
      {:time_window, :updated_at_ms, integer(100), integer(200)}
    ]

    reordered = [
      Enum.at(predicates, 2),
      {:in, :state, [keyword("completed"), keyword("failed")]},
      hd(predicates)
    ]

    stats =
      stats(index, "tenant-a", %{
        ["tenant-a", "failed"] => 30,
        ["tenant-a", "completed"] => 40
      })

    plans =
      for request_predicates <- [predicates, reordered] do
        request = collection(request_predicates, [{:updated_at_ms, :desc}], 20)

        assert {:ok, %Plan{} = plan} =
                 Planner.plan(request, [index], stats: stats_lookup(stats), now_ms: @now_ms)

        %{
          path: plan.path,
          index_id: plan.index_id,
          ranges: plan.ranges,
          order: plan.order,
          residual_predicates: plan.residual_predicates,
          estimate: plan.estimate,
          stats: plan.stats,
          query_fingerprint: plan.query_fingerprint
        }
      end

    assert [canonical, canonical] = plans
  end

  test "contradictory exact predicates remain residual and cannot use counters" do
    definition =
      IndexDefinition.new!(%{
        id: "flow_runs_tenant_state_counter",
        version: 1,
        count_prefixes: [2],
        fields: [
          {:partition_key, :asc},
          {:state, :asc}
        ]
      })

    request =
      Request.count(:execute, [
        eq(:partition_key, "tenant-a"),
        eq(:state, "failed"),
        eq(:state, "completed")
      ])

    assert {:ok,
            %Plan{
              path: :count_scan,
              ranges: [_range],
              residual_predicates: [residual],
              recheck_predicates: rechecks
            }} = Planner.plan(request, [active_index(definition)], now_ms: @now_ms)

    assert residual in [eq(:state, "failed"), eq(:state, "completed")]
    assert Enum.sort(rechecks) == Enum.sort(request |> Map.fetch!(:predicate) |> elem(1))
  end

  test "chooses the lowest-cost selective index and uses a stable logical-id tie break" do
    tenant = active_index(tenant_definition())
    state = active_index(state_definition())
    type = active_index(type_definition())

    request =
      collection(
        [
          eq(:partition_key, "tenant-a"),
          eq(:state, "failed"),
          eq(:type, "invoice")
        ],
        [{:updated_at_ms, :desc}],
        10
      )

    stats = %{
      identity(tenant) => stats(tenant, "tenant-a", %{["tenant-a"] => 20_000}),
      identity(state) => stats(state, "tenant-a", %{["tenant-a", "failed"] => 1_000}),
      identity(type) => stats(type, "tenant-a", %{["tenant-a", "invoice"] => 25})
    }

    assert {:ok, %Plan{index_id: "flow_runs_tenant_type_updated"}} =
             Planner.plan(request, [state, tenant, type], stats: stats, now_ms: @now_ms)

    tied =
      Map.put(
        stats,
        identity(state),
        stats(state, "tenant-a", %{["tenant-a", "failed"] => 25})
      )

    first = Planner.plan(request, [type, state, tenant], stats: tied, now_ms: @now_ms)
    second = Planner.plan(request, [tenant, state, type], stats: tied, now_ms: @now_ms)

    assert first == second
    assert {:ok, %Plan{index_id: "flow_runs_tenant_state_updated"}} = first
  end

  test "prefers the fully constrained native-order index when estimates are unavailable" do
    state = active_index(state_definition())
    type_state = active_index(type_state_definition())

    request =
      collection(
        [
          eq(:partition_key, "tenant-a"),
          eq(:type, "invoice"),
          eq(:state, "failed")
        ],
        [{:updated_at_ms, :desc}],
        25
      )

    assert {:ok,
            %Plan{
              index_id: "flow_runs_tenant_type_state_updated",
              order: :native,
              residual_predicates: []
            }} = Planner.plan(request, [state, type_state], now_ms: @now_ms)
  end

  test "does not prefer native order over a much cheaper bounded top-K index" do
    native = active_index(tenant_definition())

    selective =
      IndexDefinition.new!(%{
        id: "flow_runs_tenant_type_created",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:type, :asc},
          {:created_at_ms, :desc}
        ]
      })
      |> active_index()

    request =
      collection(
        [eq(:partition_key, "tenant-a"), eq(:type, "invoice")],
        [{:updated_at_ms, :desc}],
        10
      )

    stats = %{
      identity(native) =>
        stats(native, "tenant-a", %{
          ["tenant-a"] => 10_000
        }),
      identity(selective) =>
        stats(selective, "tenant-a", %{
          ["tenant-a"] => 10_000,
          ["tenant-a", "invoice"] => 10
        })
    }

    assert {:ok, %Plan{index_id: "flow_runs_tenant_type_created", order: :bounded_top_k}} =
             Planner.plan(request, [native, selective], stats: stats, now_ms: @now_ms)
  end

  test "missing and stale statistics stay pessimistic" do
    index = active_index(state_definition())
    request = collection([eq(:partition_key, "tenant-a"), eq(:state, "failed")])

    assert {:ok,
            %Plan{
              estimate: %{scan_entries: 50_000, hard_scan_entries: 50_000},
              stats: %{source: :default_upper_bound, confidence: :none}
            }} = Planner.plan(request, [index], now_ms: @now_ms)

    stale =
      stats(index, "tenant-a", %{["tenant-a", "failed"] => 1}, collected_at_ms: @now_ms - 600_001)

    assert {:ok,
            %Plan{
              estimate: %{scan_entries: 50_000, hard_scan_entries: 50_000},
              stats: %{source: :stale, confidence: :low}
            }} = Planner.plan(request, [index], stats: stats_lookup(stale), now_ms: @now_ms)
  end

  test "forged statistics cannot bypass planner byte budgets" do
    index = active_index(state_definition())
    request = collection([eq(:partition_key, "tenant-a"), eq(:state, "failed")])

    forged =
      index
      |> stats("tenant-a", %{["tenant-a"] => 1, ["tenant-a", "failed"] => 1})
      |> Map.put(:average_entry_bytes, -1)

    assert {:ok, budget} = Budget.new(scan_bytes: 1)

    assert {:ok, %Plan{path: :reject, fallback_reason: :scan_byte_budget_exceeded}} =
             Planner.plan(request, [index],
               stats: stats_lookup(forged),
               budget: budget,
               now_ms: @now_ms
             )
  end

  test "a failing statistics provider falls back to pessimistic planning" do
    index = active_index(state_definition())
    request = collection([eq(:partition_key, "tenant-a"), eq(:state, "failed")])

    assert {:ok,
            %Plan{
              path: :ordered_filter,
              estimate: %{scan_entries: 50_000},
              stats: %{source: :default_upper_bound}
            }} =
             Planner.plan(request, [index],
               stats: fn _index, _scope -> raise "statistics unavailable" end,
               now_ms: @now_ms
             )
  end

  test "returns deterministic bounded probes only for the selected candidate" do
    state = active_index(state_definition())
    type = active_index(type_definition())

    request =
      collection([
        eq(:partition_key, "tenant-a"),
        eq(:state, "failed"),
        eq(:type, "invoice")
      ])

    assert {:ok, %Plan{statistics_probes: probes}} =
             Planner.plan(request, [type, state], now_ms: @now_ms)

    assert Enum.map(probes, fn probe ->
             {
               probe.definition.id,
               probe.definition.version,
               probe.equality_values,
               probe.range.index_id,
               probe.scope_digest,
               probe.prefix_digest
             }
           end) == [
             {
               "flow_runs_tenant_state_updated",
               1,
               ["tenant-a", "failed"],
               "flow_runs_tenant_state_updated",
               IndexStatistics.scope_digest("tenant-a"),
               IndexStatistics.prefix_digest(["tenant-a", "failed"])
             }
           ]
  end

  test "does not probe prefixes already covered by fresh exact statistics" do
    index = active_index(state_definition())
    request = collection([eq(:partition_key, "tenant-a"), eq(:state, "failed")])

    stat =
      stats(index, "tenant-a", %{
        ["tenant-a"] => 100,
        ["tenant-a", "failed"] => 10
      })

    assert {:ok, %Plan{statistics_probes: []}} =
             Planner.plan(request, [index], stats: stats_lookup(stat), now_ms: @now_ms)
  end

  test "histogram selectivity never lowers the hard prefix bound" do
    index = active_index(state_definition())

    request =
      collection([
        eq(:partition_key, "tenant-a"),
        eq(:state, "failed"),
        {:range, :updated_at_ms, integer(100), integer(101)}
      ])

    stat =
      stats(index, "tenant-a", %{["tenant-a", "failed"] => 1_000},
        histograms: %{
          updated_at_ms: [
            %{lower: 0, upper: 99, count: 999},
            %{lower: 100, upper: 101, count: 1}
          ]
        }
      )

    assert {:ok, budget} = Budget.new(scan_entries: 100)

    assert {:ok, %Plan{path: :reject, fallback_reason: :scan_budget_exceeded}} =
             Planner.plan(request, [index],
               stats: stats_lookup(stat),
               budget: budget,
               now_ms: @now_ms
             )
  end

  test "a fresh mutable prefix count never lowers the execution hard bound" do
    index = active_index(state_definition())
    request = collection([eq(:partition_key, "tenant-a"), eq(:state, "failed")])
    stat = stats(index, "tenant-a", %{["tenant-a", "failed"] => 1})
    assert {:ok, budget} = Budget.new(scan_entries: 100)

    assert {:ok,
            %Plan{
              path: :ordered_filter,
              estimate: %{scan_entries: 1, hard_scan_entries: 100}
            }} =
             Planner.plan(request, [index],
               stats: stats_lookup(stat),
               budget: budget,
               now_ms: @now_ms
             )
  end

  test "keeps an unsupported hash-field range as a bounded residual" do
    index = active_index(state_definition())

    request =
      collection(
        [
          eq(:partition_key, "tenant-a"),
          {:range, :state, keyword("a"), keyword("z")}
        ],
        [{:updated_at_ms, :desc}],
        10
      )

    stat = stats(index, "tenant-a", %{["tenant-a"] => 50})

    assert {:ok,
            %Plan{
              path: :ordered_filter,
              ranges: [_one],
              residual_predicates: [{:range, :state, _, _}],
              estimate: %{hard_scan_entries: 50_000}
            }} = Planner.plan(request, [index], stats: stats_lookup(stat), now_ms: @now_ms)
  end

  test "never selects partial, non-active, or validation-pending generations" do
    definition = state_definition()

    unsafe = [
      RegisteredIndex.new!(definition, :building),
      RegisteredIndex.new!(definition, :validating),
      RegisteredIndex.new!(definition, :retiring),
      RegisteredIndex.new!(definition, :active,
        coverage: %{complete_shards: 0, total_shards: 1, validation: :passed}
      ),
      RegisteredIndex.new!(definition, :active,
        coverage: %{complete_shards: 1, total_shards: 1, validation: :pending}
      )
    ]

    request = collection([eq(:partition_key, "tenant-a"), eq(:state, "failed")])

    for index <- unsafe do
      assert {:ok, %Plan{path: :reject, fallback_reason: :no_active_bounded_index}} =
               Planner.plan(request, [index], now_ms: @now_ms)
    end
  end

  test "rejects range explosion before constructing physical keys" do
    definition =
      IndexDefinition.new!(%{
        id: "explosive",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:type, :asc},
          {:state, :asc},
          {:updated_at_ms, :desc}
        ]
      })

    index = active_index(definition)

    request =
      collection([
        eq(:partition_key, "tenant-a"),
        {:in, :type, Enum.map(1..20, &keyword("type-#{&1}"))},
        {:in, :state, Enum.map(1..20, &keyword("state-#{&1}"))}
      ])

    assert {:ok, %Plan{path: :reject, fallback_reason: :range_budget_exceeded}} =
             Planner.plan(request, [index], now_ms: @now_ms)
  end

  test "represents a non-native numeric order as bounded sorting" do
    index = active_index(state_definition())

    request =
      collection(
        [eq(:partition_key, "tenant-a"), eq(:state, "failed")],
        [{:priority, :desc}]
      )

    stats = stats(index, "tenant-a", %{["tenant-a", "failed"] => 500})

    assert {:ok,
            %Plan{
              order: :bounded_top_k,
              estimate: %{sort_rows: 500, memory_bytes: memory_bytes}
            }} = Planner.plan(request, [index], stats: stats_lookup(stats), now_ms: @now_ms)

    assert memory_bytes <= Budget.default().executor_memory_bytes
  end

  test "never treats hashed fields or unrequested trailing fields as native order" do
    hashed = active_index(state_definition())
    hashed_request = collection([eq(:partition_key, "tenant-a")], [{:state, :asc}], 10)

    assert {:error, :unsupported_query_shape} =
             Planner.plan(hashed_request, [hashed], now_ms: @now_ms)

    trailing =
      IndexDefinition.new!(%{
        id: "flow_runs_tenant_state_updated_priority",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:state, :asc},
          {:updated_at_ms, :desc},
          {:priority, :asc}
        ]
      })
      |> active_index()

    trailing_request =
      collection(
        [eq(:partition_key, "tenant-a"), eq(:state, "failed")],
        [{:updated_at_ms, :desc}],
        10
      )

    trailing_stats =
      stats(trailing, "tenant-a", %{
        ["tenant-a", "failed"] => 20
      })

    assert {:ok, %Plan{order: :bounded_top_k}} =
             Planner.plan(trailing_request, [trailing],
               stats: stats_lookup(trailing_stats),
               now_ms: @now_ms
             )
  end

  test "rejects a plan that exceeds the planner memory ceiling" do
    index = active_index(state_definition())
    request = collection([eq(:partition_key, "tenant-a"), eq(:state, "failed")])
    stat = stats(index, "tenant-a", %{["tenant-a"] => 500, ["tenant-a", "failed"] => 500})
    {:ok, budget} = Budget.new(planner_memory_bytes: 1)

    assert {:ok, %Plan{path: :reject, fallback_reason: :memory_budget_exceeded}} =
             Planner.plan(request, [index],
               stats: stats_lookup(stat),
               budget: budget,
               now_ms: @now_ms
             )
  end

  test "includes bounded page and hydration peaks in executor memory estimates" do
    index = active_index(state_definition())
    request = collection([eq(:partition_key, "tenant-a"), eq(:state, "failed")])
    stat = stats(index, "tenant-a", %{["tenant-a"] => 500, ["tenant-a", "failed"] => 500})
    {:ok, budget} = Budget.new(executor_memory_bytes: 100_000)

    assert {:ok, %Plan{path: :reject, fallback_reason: :memory_budget_exceeded}} =
             Planner.plan(request, [index],
               stats: stats_lookup(stat),
               budget: budget,
               now_ms: @now_ms
             )
  end

  test "an empty half-open time window returns an empty plan without a read" do
    index = active_index(state_definition())

    request =
      collection([
        eq(:partition_key, "tenant-a"),
        eq(:state, "failed"),
        {:time_window, :updated_at_ms, integer(100), integer(100)}
      ])

    assert {:ok, %Plan{path: :empty, ranges: [], estimate: %{scan_entries: 0}}} =
             Planner.plan(request, [index], now_ms: @now_ms)
  end

  test "all generated ranges remain inside the bound tenant prefix" do
    index = active_index(state_definition())
    :rand.seed(:exsss, {91, 17, 44})

    for case_id <- 1..250 do
      tenant = "tenant-#{case_id}-#{:rand.uniform(1_000_000)}"
      states = Enum.map(1..:rand.uniform(8), &"state-#{case_id}-#{&1}")
      lower = :rand.uniform(10_000) - 1
      upper = lower + :rand.uniform(10_000)

      request =
        collection([
          eq(:partition_key, tenant),
          {:in, :state, Enum.map(states, &keyword/1)},
          {:time_window, :updated_at_ms, integer(lower), integer(upper)}
        ])

      assert {:ok, %Plan{path: path, ranges: ranges}} =
               Planner.plan(request, [index], now_ms: @now_ms)

      assert path in [:ordered_range, :ordered_range_union]
      assert {:ok, tenant_prefix} = CompositeIndex.encode_prefix(index.definition, [tenant])
      assert Enum.all?(ranges, &String.starts_with?(&1.prefix, tenant_prefix))
      assert Enum.all?(ranges, &String.starts_with?(&1.after_key, tenant_prefix))
      assert Enum.all?(ranges, &String.starts_with?(&1.before_key, tenant_prefix))
    end
  end

  test "point requests preserve the primary-key fast path" do
    request = Request.point_read(:execute, keyword("tenant-a"), keyword("run-1"))

    assert {:ok,
            %Plan{
              path: :primary_key,
              index_id: "flow_runs_primary_v1",
              estimate: %{scan_entries: 1, hard_scan_entries: 1}
            }} = Planner.plan(request, [], now_ms: @now_ms)
  end

  test "rejects forged budget structs at the planner boundary" do
    request = Request.point_read(:execute, keyword("tenant-a"), keyword("run-1"))
    invalid = %{Budget.default() | scan_entries: 0}

    assert {:error, :invalid_query_budget} =
             Planner.plan(request, [], budget: invalid, now_ms: @now_ms)
  end

  test "does not plan against an active entry with a forged definition" do
    index = active_index(state_definition())
    <<first, rest::binary>> = index.definition.fingerprint

    forged_definition = %{
      index.definition
      | fingerprint: <<Bitwise.bxor(first, 1), rest::binary>>
    }

    forged = %{index | definition: forged_definition}
    request = collection([eq(:partition_key, "tenant-a"), eq(:state, "failed")])

    assert {:ok, %Plan{path: :reject, fallback_reason: :no_active_bounded_index}} =
             Planner.plan(request, [forged], now_ms: @now_ms)
  end

  test "EXPLAIN, ANALYZE, and execution share one value-redacted query fingerprint" do
    index = active_index(state_definition())
    execute = collection([eq(:partition_key, "tenant-a"), eq(:state, "failed")])
    explain = %{execute | mode: :explain}
    analyze = %{execute | mode: :analyze}

    assert {:ok, execute_plan} = Planner.plan(execute, [index], now_ms: @now_ms)
    assert {:ok, explain_plan} = Planner.plan(explain, [index], now_ms: @now_ms)
    assert {:ok, analyze_plan} = Planner.plan(analyze, [index], now_ms: @now_ms)
    assert execute_plan.query_fingerprint == explain_plan.query_fingerprint
    assert execute_plan.query_fingerprint == analyze_plan.query_fingerprint

    other_values = collection([eq(:partition_key, "tenant-b"), eq(:state, "completed")])
    assert {:ok, other_plan} = Planner.plan(other_values, [index], now_ms: @now_ms)
    assert execute_plan.query_fingerprint == other_plan.query_fingerprint
  end

  test "result projection is bound into query fingerprints and cursor digests" do
    request = collection([eq(:partition_key, "tenant-a"), eq(:state, "failed")])
    projected = %{request | projection: [:run_id, :state]}
    reordered = %{request | projection: [:state, :run_id]}

    refute Planner.query_fingerprint(request) == Planner.query_fingerprint(projected)
    assert Planner.query_fingerprint(projected) == Planner.query_fingerprint(reordered)
    refute Planner.query_digest(request) == Planner.query_digest(projected)
    assert Planner.query_digest(projected) == Planner.query_digest(reordered)
  end

  test "retains alternative evidence only for EXPLAIN modes" do
    indexes = [active_index(state_definition()), active_index(tenant_definition())]
    execute = collection([eq(:partition_key, "tenant-a"), eq(:state, "failed")])

    assert {:ok, execute_plan} = Planner.plan(execute, indexes, now_ms: @now_ms)

    assert {:ok, explain_plan} =
             Planner.plan(%{execute | mode: :explain}, indexes, now_ms: @now_ms)

    assert {:ok, analyze_plan} =
             Planner.plan(%{execute | mode: :analyze}, indexes, now_ms: @now_ms)

    assert execute_plan.alternatives == []
    assert length(explain_plan.alternatives) == 1
    assert analyze_plan.alternatives == explain_plan.alternatives
    assert execute_plan.statistics_probes != []
    assert analyze_plan.statistics_probes == execute_plan.statistics_probes
    assert explain_plan.statistics_probes == []
  end

  test "mandatory shared scope creates disjoint physical plans outside the user AST" do
    definition =
      IndexDefinition.new!(%{
        id: "flow_runs_tenant_state_updated_shared",
        version: 1,
        scope_bytes: 8,
        fields: [
          {:partition_key, :asc},
          {:state, :asc},
          {:updated_at_ms, :desc}
        ]
      })

    index = active_index(definition)
    request = collection([eq(:partition_key, "logical"), eq(:state, "failed")])
    scope_a = shared_scope(11)
    scope_b = shared_scope(22)

    assert {:ok, contract_a} = Planner.physical_contract(request, definition, scope_a, 8)
    assert {:ok, contract_b} = Planner.physical_contract(request, definition, scope_b, 8)
    assert [range_a] = contract_a.ranges
    assert [range_b] = contract_b.ranges
    refute range_a.prefix == range_b.prefix

    assert {:ok, prefix_a} = MandatoryScope.single_prefix(scope_a)
    assert {:ok, prefix_b} = MandatoryScope.single_prefix(scope_b)
    assert {:ok, expected_a} = CompositeIndex.encode_prefix(definition, prefix_a, ["logical"])
    assert {:ok, expected_b} = CompositeIndex.encode_prefix(definition, prefix_b, ["logical"])
    assert String.starts_with?(range_a.prefix, expected_a)
    assert String.starts_with?(range_b.prefix, expected_b)

    caller = self()

    assert {:ok, %Plan{mandatory_scope: ^scope_a}} =
             Planner.plan(request, [index],
               mandatory_scope: scope_a,
               stats: fn _index, scope -> send(caller, {:statistics_scope, scope}) end,
               now_ms: @now_ms
             )

    assert_receive {:statistics_scope, statistics_scope}
    assert statistics_scope == scope_a.digest
    refute statistics_scope == "logical"
  end

  defp collection(
         predicates,
         order_by \\ [{:updated_at_ms, :desc}],
         limit \\ 25
       ) do
    Request.collection(:execute, predicates, order_by, limit, :record)
  end

  defp eq(field, value), do: {:eq, field, keyword(value)}
  defp keyword(value), do: {:literal, :keyword, value}
  defp integer(value), do: {:literal, :integer, value}

  defp shared_scope(tenant_ref) do
    {:ok, snapshot} = MetadataExtension.configure(SharedScopeProvider, [])

    {:ok, scope} =
      MandatoryScope.bind(
        %{
          flow_metadata_snapshot: snapshot,
          request_context: %{"tenant_ref" => tenant_ref}
        },
        :runs
      )

    scope
  end

  defp active_index(definition) do
    RegisteredIndex.new!(definition, :active,
      coverage: %{complete_shards: 1, total_shards: 1, validation: :passed}
    )
  end

  defp tenant_definition do
    IndexDefinition.new!(%{
      id: "flow_runs_tenant_updated",
      version: 1,
      fields: [{:partition_key, :asc}, {:updated_at_ms, :desc}]
    })
  end

  defp state_definition do
    IndexDefinition.new!(%{
      id: "flow_runs_tenant_state_updated",
      version: 1,
      fields: [
        {:partition_key, :asc},
        {:state, :asc},
        {:updated_at_ms, :desc}
      ]
    })
  end

  defp type_definition do
    IndexDefinition.new!(%{
      id: "flow_runs_tenant_type_updated",
      version: 1,
      fields: [
        {:partition_key, :asc},
        {:type, :asc},
        {:updated_at_ms, :desc}
      ]
    })
  end

  defp type_state_definition do
    IndexDefinition.new!(%{
      id: "flow_runs_tenant_type_state_updated",
      version: 1,
      fields: [
        {:partition_key, :asc},
        {:type, :asc},
        {:state, :asc},
        {:updated_at_ms, :desc}
      ]
    })
  end

  defp stats(index, tenant, prefix_counts, opts \\ []) do
    %{
      index_id: index.definition.id,
      index_version: index.definition.version,
      scope_digest: IndexStatistics.scope_digest(tenant),
      collected_at_ms: Keyword.get(opts, :collected_at_ms, @now_ms),
      source_watermark: 10,
      total_entries: Enum.max([0 | Map.values(prefix_counts)]),
      distinct_runs: Enum.max([0 | Map.values(prefix_counts)]),
      prefix_counts:
        Map.new(prefix_counts, fn {values, count} ->
          {IndexStatistics.prefix_digest(values), count}
        end),
      prefix_observed_at_ms:
        Map.new(prefix_counts, fn {values, _count} ->
          {IndexStatistics.prefix_digest(values), Keyword.get(opts, :collected_at_ms, @now_ms)}
        end),
      histograms: %{},
      null_counts: %{},
      missing_counts: %{},
      average_entry_bytes: 96,
      average_row_bytes: 384,
      sample_rate_ppm: 1_000_000,
      confidence: :high
    }
    |> Map.put(:histograms, Keyword.get(opts, :histograms, %{}))
    |> IndexStatistics.new!()
  end

  defp stats_lookup(stat), do: %{identity(stat) => stat}
  defp identity(%RegisteredIndex{definition: definition}), do: {definition.id, definition.version}
  defp identity(%IndexStatistics{} = stat), do: {stat.index_id, stat.index_version}
end
