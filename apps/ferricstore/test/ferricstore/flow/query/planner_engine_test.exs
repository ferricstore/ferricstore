defmodule Ferricstore.Flow.Query.PlannerEngineTest do
  use ExUnit.Case, async: false

  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.{NativeValueCodec, TermCodec}
  alias FerricstoreServer.Native.FlowQuery

  alias Ferricstore.Flow.Query.{
    Error,
    ExecutionContext,
    Field,
    IndexDefinition,
    MandatoryScope,
    Request
  }

  alias Ferricstore.Flow.Query.{
    AdmissionController,
    Budget,
    Cursor,
    CursorKeyStore,
    PlannerEngine,
    IndexRegistry,
    IndexStatistics,
    Planner,
    StatisticsStore,
    StatisticsWorker
  }

  @cursor_key :binary.copy(<<0x72>>, 32)

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
    def bind_query(source, %{"tenant_ref" => tenant_ref}, _snapshot)
        when source in [:runs, :events],
        do: {:ok, {:required, [{0x8001, :eq, tenant_ref}]}}

    def bind_query(_source, _context, _snapshot), do: {:error, :flow_scope_required}
  end

  test "OSS publishes the complete parser-supported query surface" do
    assert PlannerEngine.capabilities() == %{
             request_contract: "ferric.flow.query.request/v1",
             result_contract: "ferric.flow.query.result/v1",
             explain_contract: "ferric.flow.explain/v1",
             capabilities: [
               "flow_query_v1",
               "flow_explain_v1",
               "flow_explain_analyze_v1",
               "flow_composite_index_v1",
               "flow_query_index_status_v1"
             ],
             language_versions: ["FQL1"],
             shapes: [
               "runs_by_run_id_record",
               "runs_by_partition_and_run_id_record",
               "runs_by_partition_predicates_ordered_records",
               "runs_by_partition_type_state_ordered_records",
               "runs_by_partition_type_terminals_ordered_records",
               "runs_by_partition_metadata_ordered_records",
               "runs_by_partition_type_running_lease_deadline_ordered_records",
               "runs_by_partition_parent_ordered_records",
               "runs_by_partition_root_ordered_records",
               "runs_by_partition_correlation_ordered_records",
               "runs_by_partition_predicates_count",
               "events_by_run_id_ordered_records"
             ]
           }

    assert FerricStore.Flow.QueryEngine.capabilities_for(PlannerEngine) ==
             PlannerEngine.capabilities()

    assert FerricStore.Flow.QueryEngine.configured_implementation([]) ==
             FerricStore.Flow.QueryEngine.Default
  end

  test "plans and executes through the active registry with one bounded response contract" do
    ctx = active_context()
    request = collection("tenant-a", "failed")

    assert {:ok, response} = PlannerEngine.execute(ctx, request)

    assert response.version == "ferric.flow.query.result/v1"
    assert response.records == []
    assert response.page == %{has_more: false, cursor: nil}
    assert response.quality.coverage == "complete"
    assert response.usage.range_seeks == 1
    assert response.usage.scanned_entries == 0
    assert response.usage.response_bytes == NativeValueCodec.encoded_size(response)

    eventually(fn ->
      state = :sys.get_state(StatisticsWorker.server_name(ctx))
      MapSet.size(state.pending) == 1 and :queue.len(state.queue) == 1
    end)
  end

  test "static EXPLAIN does not enqueue deferred statistics I/O" do
    ctx = active_context()
    request = %{collection("tenant-a", "failed") | mode: :explain}

    assert {:ok, %{status: "planned"}} = PlannerEngine.execute(ctx, request)

    state = :sys.get_state(StatisticsWorker.server_name(ctx))
    assert MapSet.size(state.pending) == 0
    assert :queue.len(state.queue) == 0
  end

  test "explains the missing predicate and index when no bounded plan exists" do
    {ctx, _admission} = bare_context()

    ctx =
      Map.put(
        ctx,
        :query_index_provider,
        FerricStore.Flow.QueryIndexProvider.Disabled
      )

    request = collection("tenant-secret", "failed-secret")

    assert {:error,
            %Error{
              reason: :query_no_bounded_plan,
              detail: "No active index can bound this runs query.",
              hint: hint,
              context: context
            } = diagnostic} = PlannerEngine.execute(ctx, request)

    assert hint =~ "FLOW.QUERY.INDEXES"
    assert hint =~ "partition_key ASC HASHED"
    assert hint =~ "state ASC HASHED"
    assert hint =~ "updated_at_ms DESC ORDERED"
    assert context["planner_reason"] == "no_active_bounded_index"
    assert context["source"] == "runs"

    assert context["predicates"] == [
             %{"field" => "partition_key", "operator" => "eq"},
             %{"field" => "state", "operator" => "eq"}
           ]

    assert context["order_by"] == [
             %{"field" => "updated_at_ms", "direction" => "desc"}
           ]

    assert context["suggested_index"] == %{
             "fields" => [
               %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
               %{"name" => "state", "direction" => "asc", "encoding" => "hashed"},
               %{"name" => "updated_at_ms", "direction" => "desc", "encoding" => "ordered"}
             ],
             "source" => "runs"
           }

    assert context["status_command"] == "FLOW.QUERY.INDEXES"

    encoded = inspect(Error.payload(diagnostic))
    refute encoded =~ "tenant-secret"
    refute encoded =~ "failed-secret"
    refute encoded =~ ctx.data_dir
  end

  test "keeps missing-index recommendations invariant under predicate reordering" do
    {ctx, _admission} = bare_context()
    ctx = Map.put(ctx, :query_index_provider, FerricStore.Flow.QueryIndexProvider.Disabled)

    predicates = [
      {:eq, :partition_key, {:literal, :keyword, "tenant-secret"}},
      {:eq, :state, {:literal, :keyword, "failed-secret"}},
      {:eq, :priority, {:literal, :integer, 7}}
    ]

    suggestions =
      for reordered <- [
            predicates,
            [Enum.at(predicates, 2), hd(predicates), Enum.at(predicates, 1)]
          ] do
        request =
          Request.collection(
            :execute,
            reordered,
            [{:updated_at_ms, :desc}],
            10,
            :record
          )

        assert {:error,
                %Error{
                  context: %{
                    "predicates" => predicate_shapes,
                    "suggested_index" => suggestion
                  }
                }} = PlannerEngine.execute(ctx, request)

        {predicate_shapes, suggestion}
      end

    assert [canonical, canonical] = suggestions
  end

  test "missing-index diagnostics depend on query shape rather than literal values" do
    {ctx, _admission} = bare_context()
    ctx = Map.put(ctx, :query_index_provider, FerricStore.Flow.QueryIndexProvider.Disabled)

    diagnostics =
      for {tenant, state} <- [
            {"first-tenant-secret", "first-state-secret"},
            {"second-tenant-secret", "second-state-secret"}
          ] do
        assert {:error, %Error{} = diagnostic} =
                 PlannerEngine.execute(ctx, collection(tenant, state))

        Error.payload(diagnostic)
      end

    assert [canonical, canonical] = diagnostics

    encoded = inspect(diagnostics)
    refute encoded =~ "first-tenant-secret"
    refute encoded =~ "first-state-secret"
    refute encoded =~ "second-tenant-secret"
    refute encoded =~ "second-state-secret"
  end

  test "suggests equality fields before ranges and preserves requested range ordering" do
    {ctx, _admission} = bare_context()
    ctx = Map.put(ctx, :query_index_provider, FerricStore.Flow.QueryIndexProvider.Disabled)

    request =
      Request.collection(
        :execute,
        [
          {:range, :priority, {:literal, :integer, 1}, {:literal, :integer, 5}},
          {:eq, :state, {:literal, :keyword, "failed-secret"}},
          {:eq, :partition_key, {:literal, :keyword, "tenant-secret"}}
        ],
        [{:priority, :desc}],
        10,
        :record
      )

    assert {:error, %Error{context: %{"suggested_index" => %{"fields" => fields}}}} =
             PlannerEngine.execute(ctx, request)

    assert fields == [
             %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
             %{"name" => "state", "direction" => "asc", "encoding" => "hashed"},
             %{"name" => "priority", "direction" => "desc", "encoding" => "ordered"}
           ]

    assert {:ok, _definition} =
             IndexDefinition.new(
               id: "suggested_range_index",
               version: 1,
               source: :runs,
               fields: suggested_field_specs(fields)
             )
  end

  test "suggests an exact counter prefix for an unplanned count" do
    {ctx, _admission} = bare_context()

    ctx =
      Map.put(
        ctx,
        :query_index_provider,
        FerricStore.Flow.QueryIndexProvider.Disabled
      )

    request =
      Request.count(:execute, [
        {:eq, :partition_key, {:literal, :keyword, "tenant-secret"}},
        {:eq, :state, {:literal, :keyword, "failed-secret"}}
      ])

    assert {:error,
            %Error{
              reason: :query_no_bounded_plan,
              context: %{
                "suggested_index" => %{
                  "count_prefixes" => [2],
                  "fields" => fields,
                  "source" => "runs"
                }
              }
            }} = PlannerEngine.execute(ctx, request)

    assert fields == [
             %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
             %{"name" => "state", "direction" => "asc", "encoding" => "hashed"}
           ]

    field_specs =
      Enum.map(fields, fn field ->
        {:ok, name} = Field.parse(field["name"])

        {name, String.to_existing_atom(field["direction"]),
         String.to_existing_atom(field["encoding"])}
      end)

    assert {:ok, _definition} =
             IndexDefinition.new(
               id: "suggested_count_index",
               version: 1,
               source: :runs,
               fields: field_specs,
               count_prefixes: [2]
             )
  end

  test "does not suggest an inexact counter when predicates exceed the index field bound" do
    {ctx, _admission} = bare_context()
    ctx = Map.put(ctx, :query_index_provider, FerricStore.Flow.QueryIndexProvider.Disabled)

    request =
      Request.count(:execute, [
        {:eq, :partition_key, {:literal, :keyword, "tenant-secret"}},
        {:eq, :state, {:literal, :keyword, "failed"}},
        {:eq, :type, {:literal, :keyword, "invoice"}},
        {:eq, :run_state, {:literal, :keyword, "waiting"}},
        {:eq, :priority, {:literal, :integer, 1}},
        {:eq, :attempts, {:literal, :integer, 2}},
        {:eq, :version, {:literal, :integer, 3}},
        {:eq, :parent_flow_id, {:literal, :keyword, "parent"}},
        {:eq, :root_flow_id, {:literal, :keyword, "root"}}
      ])

    assert {:error, %Error{context: %{"suggested_index" => %{"fields" => fields} = suggestion}}} =
             PlannerEngine.execute(ctx, request)

    assert length(fields) == 8
    refute Map.has_key?(suggestion, "count_prefixes")
  end

  test "keeps suggested definitions within the single multivalue field limit" do
    {ctx, _admission} = bare_context()
    ctx = Map.put(ctx, :query_index_provider, FerricStore.Flow.QueryIndexProvider.Disabled)

    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, {:literal, :keyword, "tenant-secret"}},
          {:eq, {:attribute, "region"}, {:literal, :keyword, "eu"}},
          {:eq, {:attribute, "tier"}, {:literal, :keyword, "gold"}}
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert {:error,
            %Error{
              hint: hint,
              context: %{
                "residual_predicates" => residual_predicates,
                "suggested_index" => %{"fields" => fields}
              }
            }} =
             PlannerEngine.execute(ctx, request)

    assert Enum.count(fields, &String.starts_with?(&1["name"], "attribute.")) == 1
    refute Enum.any?(fields, &(&1["name"] == "attribute.tier"))
    assert residual_predicates == [%{"field" => "attribute.tier", "operator" => "eq"}]
    assert hint =~ "Residual predicates: attribute.tier EQ"
  end

  test "does not suggest exact counters for overlapping multivalue IN predicates" do
    {ctx, _admission} = bare_context()
    ctx = Map.put(ctx, :query_index_provider, FerricStore.Flow.QueryIndexProvider.Disabled)

    request =
      Request.count(:execute, [
        {:eq, :partition_key, {:literal, :keyword, "tenant-secret"}},
        {:in, {:attribute, "region"}, [{:literal, :keyword, "eu"}, {:literal, :keyword, "us"}]}
      ])

    assert {:error, %Error{context: %{"suggested_index" => %{"fields" => fields} = suggestion}}} =
             PlannerEngine.execute(ctx, request)

    assert Enum.any?(fields, &(&1["name"] == "attribute.region"))
    refute Map.has_key?(suggestion, "count_prefixes")
  end

  test "suggests exact counters only for disjoint exact predicate shapes" do
    {ctx, _admission} = bare_context()
    ctx = Map.put(ctx, :query_index_provider, FerricStore.Flow.QueryIndexProvider.Disabled)

    exact_requests = [
      Request.count(:execute, [
        {:eq, :partition_key, {:literal, :keyword, "tenant-secret"}},
        {:in, :state,
         [
           {:literal, :keyword, "failed-secret"},
           {:literal, :keyword, "completed-secret"}
         ]}
      ]),
      Request.count(:execute, [
        {:eq, :partition_key, {:literal, :keyword, "tenant-secret"}},
        {:is, :state, :null}
      ])
    ]

    for request <- exact_requests do
      assert {:error,
              %Error{
                context: %{
                  "suggested_index" => %{"count_prefixes" => [2], "fields" => fields}
                }
              }} = PlannerEngine.execute(ctx, request)

      assert {:ok, _definition} =
               IndexDefinition.new(
                 id: "suggested_exact_index",
                 version: 1,
                 source: :runs,
                 fields: suggested_field_specs(fields),
                 count_prefixes: [2]
               )
    end

    range_request =
      Request.count(:execute, [
        {:eq, :partition_key, {:literal, :keyword, "tenant-secret"}},
        {:range, :priority, {:literal, :integer, 1}, {:literal, :integer, 5}}
      ])

    assert {:error, %Error{context: %{"suggested_index" => %{"fields" => fields} = suggestion}}} =
             PlannerEngine.execute(ctx, range_request)

    assert Enum.any?(fields, &(&1["name"] == "priority"))
    refute Map.has_key?(suggestion, "count_prefixes")
  end

  test "keeps maximum-size recommendations valid, bounded, and value-redacted" do
    {ctx, _admission} = bare_context()
    ctx = Map.put(ctx, :query_index_provider, FerricStore.Flow.QueryIndexProvider.Disabled)

    predicates = [
      {:eq, :partition_key, {:literal, :keyword, "tenant-secret"}},
      {:eq, :state, {:literal, :keyword, "state-secret"}},
      {:eq, :type, {:literal, :keyword, "type-secret"}},
      {:eq, :run_state, {:literal, :keyword, "run-state-secret"}},
      {:eq, :priority, {:literal, :integer, 1}},
      {:eq, :attempts, {:literal, :integer, 2}},
      {:eq, :version, {:literal, :integer, 3}},
      {:eq, :parent_flow_id, {:literal, :keyword, "parent-secret"}},
      {:eq, :root_flow_id, {:literal, :keyword, "root-secret"}},
      {:eq, :correlation_id, {:literal, :keyword, "correlation-secret"}},
      {:eq, :updated_at_ms, {:literal, :integer, 4}},
      {:eq, :created_at_ms, {:literal, :integer, 5}}
    ]

    assert {:error,
            %Error{
              hint: hint,
              context: %{"suggested_index" => %{"fields" => fields} = suggestion}
            } = diagnostic} = PlannerEngine.execute(ctx, Request.count(:execute, predicates))

    assert byte_size(hint) <= 1_024
    assert length(fields) == 8
    refute Map.has_key?(suggestion, "count_prefixes")

    assert {:ok, _definition} =
             IndexDefinition.new(
               id: "suggested_maximum_index",
               version: 1,
               source: :runs,
               fields: suggested_field_specs(fields)
             )

    encoded = inspect(Error.payload(diagnostic))

    for secret <- [
          "tenant-secret",
          "state-secret",
          "type-secret",
          "run-state-secret",
          "parent-secret",
          "root-secret",
          "correlation-secret"
        ] do
      refute encoded =~ secret
    end
  end

  test "does not execute an index fenced after the active snapshot was published" do
    ctx = active_context()
    admission = ctx.query_admission_controller

    assert {:ok, %{indexes: [index | _]}} = IndexRegistry.snapshot(ctx, 0)

    identity = {
      index.definition.id,
      index.definition.version,
      index.build_id
    }

    assert :ok = AdmissionController.fence_index(admission, ctx, identity)

    assert {:error, :query_storage_unavailable} =
             PlannerEngine.execute(ctx, collection("tenant-a", "failed"))
  end

  test "executes a scalar count through the active bounded index without row pagination" do
    ctx = active_context()

    request =
      Request.count(:execute, [
        {:eq, :partition_key, {:literal, :keyword, "tenant-a"}},
        {:eq, :state, {:literal, :keyword, "failed"}}
      ])

    assert {:ok, response} = PlannerEngine.execute(ctx, request)

    assert response.version == "ferric.flow.query.result/v1"
    assert response.result == %{kind: "count", value: 0}
    assert response.quality.pagination == "none"
    assert response.usage.result_records == 1
    assert response.usage.scanned_entries == 0
    assert response.usage.response_bytes == NativeValueCodec.encoded_size(response)
    refute Map.has_key?(response, :page)
    refute Map.has_key?(response, :records)
  end

  test "returns an exact zero count for a logically empty window without registry access" do
    {ctx, _admission} = bare_context()

    request =
      Request.count(:execute, [
        {:eq, :partition_key, {:literal, :keyword, "tenant-a"}},
        {:time_window, :updated_at_ms, {:literal, :integer, 100}, {:literal, :integer, 100}}
      ])

    assert {:ok, response} = PlannerEngine.execute(ctx, request)
    assert response.result == %{kind: "count", value: 0}
    assert response.quality.exactness == "exact"
    assert response.quality.freshness == "not_applicable"
  end

  test "shared authorities query the same logical partition through disjoint physical plans" do
    ctx = active_context(:single_index, flow_metadata_snapshot: shared_snapshot())
    tenant_a = execution_context(ctx, 11)
    tenant_b = execution_context(ctx, 22)
    request = collection("logical", "failed")

    assert {:ok, response_a} = PlannerEngine.execute(tenant_a, request)
    assert {:ok, response_b} = PlannerEngine.execute(tenant_b, request)
    assert response_a.records == []
    assert response_b.records == []

    eventually(fn ->
      state = :sys.get_state(StatisticsWorker.server_name(ctx))
      MapSet.size(state.pending) == 2 and :queue.len(state.queue) == 2
    end)
  end

  test "EXPLAIN is deterministic and does not expose bound values or physical keys" do
    ctx = active_context()
    first = %{collection("tenant-secret-a", "failed-secret") | mode: :explain}
    second = %{collection("tenant-secret-b", "running-secret") | mode: :explain}

    assert {:ok, first_explain} = PlannerEngine.execute(ctx, first)
    assert {:ok, second_explain} = PlannerEngine.execute(ctx, second)

    assert first_explain == second_explain
    assert first_explain.version == "ferric.flow.explain/v1"
    assert first_explain.plan.path == "ordered_filter"

    encoded = inspect(first_explain)
    refute encoded =~ "tenant-secret"
    refute encoded =~ "failed-secret"
    refute encoded =~ "running-secret"
    refute encoded =~ ctx.data_dir
    refute encoded =~ "flow_query_index"
  end

  test "EXPLAIN ANALYZE executes the bounded plan and returns usage without records" do
    ctx = active_context()
    request = %{collection("tenant-secret", "failed-secret") | mode: :analyze}

    assert {:ok, explain} = PlannerEngine.execute(ctx, request)

    assert explain.version == "ferric.flow.explain/v1"
    assert explain.status == "executed"
    assert explain.plan.path == "ordered_filter"
    assert explain.actual.range_seeks == 1
    assert explain.actual.scanned_entries == 0
    assert explain.actual.result_records == 0
    assert explain.actual.response_bytes > 0
    assert explain.actual.wall_time_us >= 0
    refute Map.has_key?(explain, :records)
    refute Map.has_key?(explain, :page)

    eventually(fn ->
      state = :sys.get_state(StatisticsWorker.server_name(ctx))
      MapSet.size(state.pending) == 1 and :queue.len(state.queue) == 1
    end)

    encoded = inspect(explain)
    refute encoded =~ "tenant-secret"
    refute encoded =~ "failed-secret"
    refute encoded =~ ctx.data_dir
  end

  test "EXPLAIN ANALYZE executes scalar counts without returning the count value" do
    ctx = active_context()

    request =
      Request.count(:analyze, [
        {:eq, :partition_key, {:literal, :keyword, "tenant-secret"}},
        {:eq, :state, {:literal, :keyword, "failed-secret"}}
      ])

    assert {:ok, explain} = PlannerEngine.execute(ctx, request)
    assert explain.status == "executed"
    assert explain.plan.return == "count"
    assert explain.actual.result_records == 1
    refute Map.has_key?(explain, :result)
    refute inspect(explain) =~ "tenant-secret"
  end

  test "EXPLAIN ANALYZE keeps rejected plans non-executing and actionable" do
    {ctx, _admission} = bare_context()
    ctx = Map.put(ctx, :query_index_provider, FerricStore.Flow.QueryIndexProvider.Disabled)
    request = %{collection("tenant-secret", "failed-secret") | mode: :analyze}

    assert {:ok, explain} = PlannerEngine.execute(ctx, request)
    assert explain.status == "rejected"
    assert explain.actual == nil
    assert explain.diagnostic["code"] == "query_no_bounded_plan"
    assert explain.diagnostic["context"]["status_command"] == "FLOW.QUERY.INDEXES"
    refute inspect(explain) =~ "tenant-secret"
    refute inspect(explain) =~ "failed-secret"
  end

  test "the public FQL1 boundary exposes EXPLAIN ANALYZE end to end" do
    ctx = active_context() |> Map.put(:query_engine, PlannerEngine)

    query =
      "EXPLAIN ANALYZE FROM runs WHERE partition_key = @partition " <>
        "AND state = @state ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS"

    assert {:ok,
            %{
              version: "ferric.flow.explain/v1",
              status: "executed",
              actual: %{scanned_entries: 0},
              plan: %{path: "ordered_filter"}
            }} =
             FlowQuery.execute(ctx, "FQL1", query, %{
               "partition" => "tenant-secret",
               "state" => "failed-secret"
             })
  end

  test "the public FQL1 boundary preserves actionable planner diagnostics" do
    {ctx, _admission} = bare_context()

    ctx =
      ctx
      |> Map.put(:query_engine, PlannerEngine)
      |> Map.put(:query_index_provider, FerricStore.Flow.QueryIndexProvider.Disabled)

    query =
      "FROM runs WHERE partition_key = @partition " <>
        "AND state = @state ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS"

    assert {:error,
            %Error{
              reason: :query_no_bounded_plan,
              hint: hint,
              context: %{
                "planner_reason" => "no_active_bounded_index",
                "status_command" => "FLOW.QUERY.INDEXES"
              }
            } = diagnostic} =
             FlowQuery.execute(ctx, "FQL1", query, %{
               "partition" => "tenant-secret",
               "state" => "failed-secret"
             })

    assert hint =~ "create or activate an index"
    refute inspect(Error.payload(diagnostic)) =~ "tenant-secret"
    refute inspect(Error.payload(diagnostic)) =~ "failed-secret"
  end

  test "EXPLAIN ANALYZE covers point, history, and lineage operators" do
    ctx = history_context()
    unique = System.unique_integer([:positive, :monotonic])
    id = "analyze-run-#{unique}"
    partition = "analyze-tenant-#{unique}"
    child_id = "analyze-child-#{unique}"

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: "analyze",
               partition_key: partition,
               now_ms: 1_000
             )

    assert :ok =
             Ferricstore.Flow.signal(ctx, id,
               partition_key: partition,
               signal: "one",
               now_ms: 1_001
             )

    assert :ok =
             Ferricstore.Flow.create(ctx, child_id,
               type: "analyze",
               partition_key: partition,
               parent_flow_id: id,
               now_ms: 1_002
             )

    point =
      Request.point_read(
        :analyze,
        {:literal, :keyword, partition},
        {:literal, :keyword, id}
      )

    assert {:ok, point_explain} = PlannerEngine.execute(ctx, point)
    assert point_explain.status == "executed"
    assert point_explain.plan.path == "primary_key"
    assert point_explain.actual.result_records == 1
    refute Map.has_key?(point_explain, :records)

    history =
      Request.history(
        :analyze,
        [
          {:eq, :partition_key, {:literal, :keyword, partition}},
          {:eq, :run_id, {:literal, :keyword, id}}
        ],
        :asc,
        10
      )

    assert {:ok, history_explain} = PlannerEngine.execute(ctx, history)
    assert history_explain.status == "executed"
    assert history_explain.plan.path == "history"
    assert history_explain.actual.result_records >= 2
    refute Map.has_key?(history_explain, :records)

    lineage =
      Request.collection(
        :analyze,
        [
          {:eq, :partition_key, {:literal, :keyword, partition}},
          {:eq, :parent_flow_id, {:literal, :keyword, id}}
        ],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )

    assert {:ok, lineage_explain} = PlannerEngine.execute(ctx, lineage)
    assert lineage_explain.status == "executed"
    assert lineage_explain.plan.path == "lineage"
    assert lineage_explain.actual.result_records == 1
    refute Map.has_key?(lineage_explain, :records)

    for explain <- [point_explain, history_explain, lineage_explain] do
      encoded = inspect(explain)
      refute encoded =~ id
      refute encoded =~ partition
      refute encoded =~ child_id
    end
  end

  test "dedicated mode does not confuse request authority with a user partition" do
    {ctx, _admission} = bare_context()

    wrapped = %ExecutionContext{
      instance_ctx: ctx,
      request_context: %{"tenant" => "tenant-b"}
    }

    request =
      Request.point_read(
        :explain,
        {:literal, :keyword, "tenant-a"},
        {:literal, :keyword, "run-1"}
      )

    assert {:ok, explain} = PlannerEngine.execute(wrapped, request)
    assert explain.plan.path == "primary_key"
  end

  test "shared mode fails closed when server authority is missing" do
    {ctx, _admission} = bare_context(flow_metadata_snapshot: shared_snapshot())

    wrapped = %ExecutionContext{
      instance_ctx: ctx,
      request_context: %{}
    }

    assert {:error, :unauthorized_scope} =
             PlannerEngine.execute(wrapped, collection("tenant-a", "failed"))
  end

  test "shared admission is tenant-wide and isolated from the logical partition" do
    snapshot = shared_snapshot()
    {ctx, admission} = bare_context(flow_metadata_snapshot: snapshot, max_node: 2)
    tenant_a = execution_context(ctx, 11)
    tenant_b = execution_context(ctx, 22)

    assert {:ok, scope_a} =
             MandatoryScope.bind(
               Map.put(ctx, :request_context, %{"tenant_ref" => 11}),
               :runs
             )

    assert {:ok, admission_key_a} = MandatoryScope.admission_key(scope_a, "partition-a")
    assert {:ok, lease} = AdmissionController.acquire(admission, ctx, admission_key_a)

    assert {:error, :query_concurrency_exceeded} =
             PlannerEngine.execute(tenant_a, collection("other-partition", "failed"))

    assert {:error, :query_storage_unavailable} =
             PlannerEngine.execute(tenant_b, collection("other-partition", "failed"))

    assert :ok = AdmissionController.release(admission, lease)
  end

  test "rejects an expired propagated deadline before registry access" do
    {ctx, _admission} = bare_context()

    wrapped = %ExecutionContext{
      instance_ctx: ctx,
      deadline_ms: System.system_time(:millisecond) - 1,
      request_context: %{}
    }

    assert {:error, :query_deadline_exceeded} =
             PlannerEngine.execute(wrapped, collection("tenant-a", "failed"))
  end

  test "enforces the instance scope and node permit before planning" do
    {ctx, admission} = bare_context()
    assert {:ok, lease} = AdmissionController.acquire(admission, ctx, "tenant-a")

    assert {:error, :query_concurrency_exceeded} =
             PlannerEngine.execute(ctx, collection("tenant-a", "failed"))

    assert :ok = AdmissionController.release(admission, lease)
  end

  test "resizes admission to the planned working set before storage access" do
    planner_bytes = Budget.default().planner_memory_bytes

    {ctx, _admission} =
      bare_context(
        max_scope_memory_bytes: planner_bytes,
        max_node_memory_bytes: planner_bytes
      )

    request =
      Request.point_read(
        :execute,
        {:literal, :keyword, "tenant-a"},
        {:literal, :keyword, "run-1"}
      )

    assert {:error, :query_concurrency_exceeded} = PlannerEngine.execute(ctx, request)
  end

  test "composite execution reserves its hard memory ceiling rather than an average estimate" do
    eight_mib = 8 * 1_024 * 1_024

    ctx =
      active_context(:single_index,
        max_scope_memory_bytes: eight_mib,
        max_node_memory_bytes: eight_mib
      )

    assert {:error, :query_concurrency_exceeded} =
             PlannerEngine.execute(ctx, collection("tenant-a", "failed"))
  end

  test "keeps the primary-key EXPLAIN path independent of the composite registry" do
    {ctx, _admission} = bare_context()

    request =
      Request.point_read(
        :explain,
        {:literal, :keyword, "tenant-a"},
        {:literal, :keyword, "run-1"}
      )

    assert {:ok, explain} = PlannerEngine.execute(ctx, request)
    assert explain.version == "ferric.flow.explain/v1"
    assert explain.plan.path == "primary_key"

    assert explain.plan.index == %{
             logical_id: "flow_runs_primary_v1",
             generation: 1,
             build_id: "flow_runs_primary_v1"
           }
  end

  test "shared run-id-only point EXPLAIN derives a tenant-contained auto partition" do
    {ctx, _admission} = bare_context(flow_metadata_snapshot: shared_snapshot())

    request = %Request{
      mode: :explain,
      source: :runs,
      predicate: {:and, [{:eq, :run_id, {:literal, :keyword, "run-auto"}}]},
      order_by: [],
      limit: 1,
      return: :record
    }

    assert {:ok, explain} = PlannerEngine.execute(execution_context(ctx, 11), request)
    assert explain.plan.path == "primary_key"

    assert explain.plan.mandatory_scope == %{
             mode: "shared",
             generation: 1,
             branch_count: 1,
             enforcement: "physical_prefix",
             values_redacted: true
           }

    refute inspect(explain) =~ "tenant_ref"
  end

  test "shared event history EXPLAIN stays on the tenant-contained direct index" do
    {ctx, _admission} = bare_context(flow_metadata_snapshot: shared_snapshot())

    request = %Request{
      mode: :explain,
      source: :events,
      predicate: {:and, [{:eq, :run_id, {:literal, :keyword, "run-history"}}]},
      order_by: [{:event_id, :asc}],
      limit: 25,
      return: :record
    }

    assert {:ok, explain} = PlannerEngine.execute(execution_context(ctx, 11), request)
    assert explain.plan.path == "history"
    assert explain.plan.index.logical_id == "flow_events_history_v1"
    refute inspect(explain) =~ "tenant_ref"
  end

  test "shared parent lineage EXPLAIN bypasses the composite registry" do
    {ctx, _admission} = bare_context(flow_metadata_snapshot: shared_snapshot())

    request =
      Request.collection(
        :explain,
        [
          {:eq, :partition_key, {:literal, :keyword, "logical"}},
          {:eq, :parent_flow_id, {:literal, :keyword, "parent-1"}}
        ],
        [{:updated_at_ms, :desc}],
        25,
        :record
      )

    assert {:ok, explain} = PlannerEngine.execute(execution_context(ctx, 11), request)
    assert explain.plan.path == "lineage"
    assert explain.plan.index.logical_id == "flow_runs_parent_v1"
    refute inspect(explain) =~ "tenant_ref"
  end

  test "event history executes as bounded authenticated pages in global order" do
    ctx = history_context()
    id = "enterprise-history-#{System.unique_integer([:positive, :monotonic])}"

    assert :ok = Ferricstore.Flow.create(ctx, id, type: "query-history", now_ms: 1_000)

    for signal <- ["one", "two", "three"] do
      assert :ok = Ferricstore.Flow.signal(ctx, id, signal: signal, now_ms: 1_000)
    end

    request = Request.history(:execute, [{:eq, :run_id, {:literal, :keyword, id}}], :asc, 2)

    assert {:ok, first} = PlannerEngine.execute(ctx, request)
    assert first.page.has_more
    assert is_binary(first.page.cursor)
    assert length(first.records) == 2
    assert first.usage.scanned_entries <= 6

    continued = %{request | cursor: {:literal, :keyword, first.page.cursor}}
    assert {:ok, second} = PlannerEngine.execute(ctx, continued)
    refute second.page.has_more
    assert second.page.cursor == nil
    assert length(second.records) == 2
    assert second.usage.scanned_entries <= 6

    first_ids = Enum.map(first.records, & &1.event_id)
    second_ids = Enum.map(second.records, & &1.event_id)
    all_ids = first_ids ++ second_ids

    assert MapSet.disjoint?(MapSet.new(first_ids), MapSet.new(second_ids))
    assert length(Enum.uniq(all_ids)) == 4
    assert all_ids == Enum.sort(all_ids)
  end

  test "parent lineage executes as bounded authenticated tuple pages" do
    ctx = history_context()
    unique = System.unique_integer([:positive, :monotonic])
    partition_key = "enterprise-lineage-tenant-#{unique}"
    parent_id = "enterprise-lineage-parent-#{unique}"
    child_ids = Enum.map(1..4, &"enterprise-lineage-child-#{unique}-#{&1}")

    Enum.each(child_ids, fn child_id ->
      assert :ok =
               Ferricstore.Flow.create(ctx, child_id,
                 type: "query-lineage",
                 partition_key: partition_key,
                 parent_flow_id: parent_id,
                 now_ms: 1_000
               )
    end)

    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, {:literal, :keyword, partition_key}},
          {:eq, :parent_flow_id, {:literal, :keyword, parent_id}}
        ],
        [{:updated_at_ms, :asc}],
        2,
        :record
      )

    assert {:ok, first} = PlannerEngine.execute(ctx, request)
    assert first.page.has_more
    assert is_binary(first.page.cursor)
    assert length(first.records) == 2
    assert first.usage.range_seeks == 2
    assert first.usage.scanned_entries <= 6

    continued = %{request | cursor: {:literal, :keyword, first.page.cursor}}
    assert {:ok, second} = PlannerEngine.execute(ctx, continued)
    refute second.page.has_more
    assert second.page.cursor == nil
    assert length(second.records) == 2
    assert second.usage.scanned_entries <= 6

    first_ids = Enum.map(first.records, & &1.id)
    second_ids = Enum.map(second.records, & &1.id)
    all_records = first.records ++ second.records
    all_ids = first_ids ++ second_ids
    all_keys = Enum.map(all_records, &{&1.updated_at_ms, &1.id})

    assert MapSet.disjoint?(MapSet.new(first_ids), MapSet.new(second_ids))
    assert Enum.sort(all_ids) == Enum.sort(child_ids)
    assert all_keys == Enum.sort(all_keys)
  end

  test "root lineage includes its root and reports the additional point probe" do
    ctx = history_context()
    unique = System.unique_integer([:positive, :monotonic])
    partition_key = "enterprise-root-tenant-#{unique}"
    root_id = "enterprise-root-#{unique}"
    child_ids = Enum.map(1..3, &"enterprise-root-child-#{unique}-#{&1}")

    assert :ok =
             Ferricstore.Flow.create(ctx, root_id,
               type: "query-root",
               partition_key: partition_key,
               now_ms: 1_000
             )

    Enum.each(child_ids, fn child_id ->
      assert :ok =
               Ferricstore.Flow.create(ctx, child_id,
                 type: "query-root",
                 partition_key: partition_key,
                 parent_flow_id: root_id,
                 root_flow_id: root_id,
                 now_ms: 1_000
               )
    end)

    request =
      Request.collection(
        :execute,
        [
          {:eq, :root_flow_id, {:literal, :keyword, root_id}},
          {:eq, :partition_key, {:literal, :keyword, partition_key}}
        ],
        [{:updated_at_ms, :desc}],
        2,
        :record
      )

    assert {:ok, first} = PlannerEngine.execute(ctx, request)
    assert first.page.has_more
    assert first.usage.range_seeks == 3
    assert first.usage.scanned_entries <= 7
    assert first.usage.hydrated_records <= 4

    assert {:ok, second} =
             PlannerEngine.execute(
               ctx,
               %{request | cursor: {:literal, :keyword, first.page.cursor}}
             )

    records = first.records ++ second.records
    ids = Enum.map(records, & &1.id)
    keys = Enum.map(records, &{&1.updated_at_ms, &1.id})

    assert Enum.sort(ids) == Enum.sort([root_id | child_ids])
    assert length(ids) == length(Enum.uniq(ids))
    assert keys == Enum.sort(keys, :desc)
  end

  test "shared lineage execution isolates authorities and returns logical partitions" do
    ctx =
      history_context(
        flow_metadata_extension: SharedScopeProvider,
        flow_tenancy_mode: :shared
      )

    tenant_a = Map.put(ctx, :request_context, %{"tenant_ref" => 11})
    tenant_b = Map.put(ctx, :request_context, %{"tenant_ref" => 22})
    partition_key = "enterprise-shared-lineage"
    parent_id = "enterprise-shared-parent"
    child_id = "enterprise-shared-child"

    assert :ok =
             Ferricstore.Flow.create(tenant_a, child_id,
               type: "tenant-a-lineage",
               partition_key: partition_key,
               parent_flow_id: parent_id,
               now_ms: 1_000
             )

    assert :ok =
             Ferricstore.Flow.create(tenant_b, child_id,
               type: "tenant-b-lineage",
               partition_key: partition_key,
               parent_flow_id: parent_id,
               now_ms: 2_000
             )

    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, {:literal, :keyword, partition_key}},
          {:eq, :parent_flow_id, {:literal, :keyword, parent_id}}
        ],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )

    assert {:ok,
            %{
              records: [%{id: ^child_id, type: "tenant-a-lineage", partition_key: ^partition_key}]
            }} =
             PlannerEngine.execute(tenant_a, request)

    assert {:ok,
            %{
              records: [%{id: ^child_id, type: "tenant-b-lineage", partition_key: ^partition_key}]
            }} =
             PlannerEngine.execute(tenant_b, request)
  end

  test "fixed-index fallback paginates without truncating records" do
    ctx = history_context()
    unique = System.unique_integer([:positive, :monotonic])
    partition_key = "fixed-page-tenant-#{unique}"
    ids = Enum.map(1..5, &"fixed-page-#{unique}-#{&1}")

    Enum.with_index(ids, 1)
    |> Enum.each(fn {id, position} ->
      assert :ok =
               Ferricstore.Flow.create(ctx, id,
                 type: "fixed-page",
                 partition_key: partition_key,
                 now_ms: 1_000 + position
               )
    end)

    for direction <- [:asc, :desc] do
      request =
        Request.collection(
          :execute,
          [
            {:eq, :partition_key, {:literal, :keyword, partition_key}},
            {:eq, :type, {:literal, :keyword, "fixed-page"}},
            {:eq, :state, {:literal, :keyword, "queued"}}
          ],
          [{:updated_at_ms, direction}],
          2,
          :record
        )

      assert {:ok, first} = PlannerEngine.execute(ctx, request)
      assert first.page.has_more
      assert is_binary(first.page.cursor)
      assert first.quality.pagination == "authenticated_seek"

      assert {:ok, second} =
               PlannerEngine.execute(
                 ctx,
                 %{request | cursor: {:literal, :keyword, first.page.cursor}}
               )

      assert second.page.has_more
      assert is_binary(second.page.cursor)

      assert {:ok, third} =
               PlannerEngine.execute(
                 ctx,
                 %{request | cursor: {:literal, :keyword, second.page.cursor}}
               )

      refute third.page.has_more
      assert third.page.cursor == nil

      records = first.records ++ second.records ++ third.records
      assert Enum.map(records, & &1.id) == Enum.sort(ids, direction)
      assert length(records) == length(Enum.uniq_by(records, & &1.id))
    end
  end

  test "fixed-index fallback cursor preserves timestamp tie ordering" do
    ctx = history_context()
    unique = System.unique_integer([:positive, :monotonic])
    partition_key = "fixed-tie-tenant-#{unique}"
    ids = Enum.map(1..3, &"fixed-tie-#{unique}-#{&1}")

    Enum.each(ids, fn id ->
      assert :ok =
               Ferricstore.Flow.create(ctx, id,
                 type: "fixed-tie",
                 partition_key: partition_key,
                 now_ms: 2_000
               )
    end)

    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, {:literal, :keyword, partition_key}},
          {:eq, :type, {:literal, :keyword, "fixed-tie"}},
          {:eq, :state, {:literal, :keyword, "queued"}}
        ],
        [{:updated_at_ms, :asc}],
        1,
        :record
      )

    assert {:ok, first} = PlannerEngine.execute(ctx, request)

    assert {:ok, second} =
             PlannerEngine.execute(
               ctx,
               %{request | cursor: {:literal, :keyword, first.page.cursor}}
             )

    assert {:ok, third} =
             PlannerEngine.execute(
               ctx,
               %{request | cursor: {:literal, :keyword, second.page.cursor}}
             )

    records = first.records ++ second.records ++ third.records
    assert Enum.map(records, & &1.id) == Enum.sort(ids)
    assert Enum.all?(records, &(&1.updated_at_ms == 2_000))
    refute third.page.has_more
  end

  test "fixed inflight-index fallback paginates by lease deadline" do
    ctx = history_context()
    unique = System.unique_integer([:positive, :monotonic])
    partition_key = "fixed-lease-tenant-#{unique}"
    type = "fixed-lease-#{unique}"

    expected =
      Enum.map(1..5, fn position ->
        id = "fixed-lease-#{unique}-#{position}"
        now_ms = 3_000 + position
        lease_ms = 100

        assert {:ok, record} =
                 Ferricstore.Flow.start_and_claim(ctx, id, type, "work",
                   partition_key: partition_key,
                   worker: "query-test",
                   lease_ms: lease_ms,
                   now_ms: now_ms
                 )

        {record.lease_deadline_ms, id}
      end)

    for direction <- [:asc, :desc] do
      request =
        Request.collection(
          :execute,
          [
            {:eq, :partition_key, {:literal, :keyword, partition_key}},
            {:eq, :type, {:literal, :keyword, type}},
            {:eq, :state, {:literal, :keyword, "running"}},
            {:range, :lease_deadline_ms, {:literal, :integer, 3_000}, {:literal, :integer, 4_000}}
          ],
          [{:lease_deadline_ms, direction}],
          2,
          :record
        )

      assert {:ok, first} = PlannerEngine.execute(ctx, request)
      assert first.page.has_more

      assert {:ok, second} =
               PlannerEngine.execute(
                 ctx,
                 %{request | cursor: {:literal, :keyword, first.page.cursor}}
               )

      assert second.page.has_more

      assert {:ok, third} =
               PlannerEngine.execute(
                 ctx,
                 %{request | cursor: {:literal, :keyword, second.page.cursor}}
               )

      refute third.page.has_more

      keys =
        (first.records ++ second.records ++ third.records)
        |> Enum.map(&{&1.lease_deadline_ms, &1.id})

      assert keys == Enum.sort(expected, direction)
      assert length(keys) == length(Enum.uniq(keys))
    end
  end

  test "executes a logically empty half-open window without registry availability" do
    {ctx, _admission} = bare_context()

    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, {:literal, :keyword, "tenant-a"}},
          {:time_window, :updated_at_ms, {:literal, :integer, 100}, {:literal, :integer, 100}}
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert {:ok, response} = PlannerEngine.execute(ctx, request)
    assert response.records == []
    assert response.quality.exactness == "exact"
    assert response.quality.freshness == "not_applicable"
  end

  test "keeps a continuation on its authenticated index when statistics change" do
    ctx = active_context(:competing_indexes)

    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, {:literal, :keyword, "tenant-a"}},
          {:eq, :state, {:literal, :keyword, "failed"}},
          {:eq, :type, {:literal, :keyword, "invoice"}}
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert {:ok, snapshot} = IndexRegistry.snapshot(ctx, 0)

    state_index =
      Enum.find(snapshot.indexes, &(&1.definition.id == "flow_runs_tenant_state_updated"))

    type_index =
      Enum.find(snapshot.indexes, &(&1.definition.id == "flow_runs_tenant_type_updated"))

    now_ms = System.system_time(:millisecond)

    assert :ok =
             StatisticsStore.put(
               StatisticsStore.server_name(ctx),
               statistics(state_index, "tenant-a", ["tenant-a", "failed"], 1_000, now_ms)
             )

    assert :ok =
             StatisticsStore.put(
               StatisticsStore.server_name(ctx),
               statistics(type_index, "tenant-a", ["tenant-a", "invoice"], 1, now_ms)
             )

    binding = %{
      instance: ctx.name,
      scope: dedicated_query_binding("tenant-a"),
      query_fingerprint: Planner.query_fingerprint(request),
      query_digest: query_digest(request),
      index_id: state_index.definition.id,
      index_version: state_index.definition.version,
      index_build_id: state_index.build_id,
      order_by: request.order_by
    }

    continuation = TermCodec.encode({:ferric_flow_query_seek, 1, "seek", nil})

    assert {:ok, token} = Cursor.issue(binding, continuation, key: @cursor_key, now_ms: now_ms)

    continued = %{request | cursor: {:literal, :keyword, token}}

    assert {:ok, response} = PlannerEngine.execute(ctx, continued)
    assert response.records == []
    assert response.page == %{has_more: false, cursor: nil}
  end

  test "rejects a continuation whose authenticated index generation is no longer active" do
    ctx = active_context()
    request = collection("tenant-a", "failed")
    now_ms = System.system_time(:millisecond)

    binding = %{
      instance: ctx.name,
      scope: dedicated_query_binding("tenant-a"),
      query_fingerprint: Planner.query_fingerprint(request),
      query_digest: query_digest(request),
      index_id: "flow_runs_tenant_state_updated",
      index_version: 2,
      index_build_id: "retired-build",
      order_by: request.order_by
    }

    continuation = TermCodec.encode({:ferric_flow_query_seek, 1, "seek", nil})
    assert {:ok, token} = Cursor.issue(binding, continuation, key: @cursor_key, now_ms: now_ms)

    assert {:error, :query_cursor_invalid} =
             PlannerEngine.execute(ctx, %{request | cursor: {:literal, :keyword, token}})
  end

  test "rejects a continuation from a replaced build with the same index id and version" do
    ctx = active_context()
    request = collection("tenant-a", "failed")
    now_ms = System.system_time(:millisecond)
    assert {:ok, snapshot} = IndexRegistry.snapshot(ctx, 0)
    assert [index] = snapshot.indexes

    binding = %{
      instance: ctx.name,
      scope: dedicated_query_binding("tenant-a"),
      query_fingerprint: Planner.query_fingerprint(request),
      query_digest: query_digest(request),
      index_id: index.definition.id,
      index_version: index.definition.version,
      index_build_id: index.build_id <> "-replaced",
      order_by: request.order_by
    }

    continuation = TermCodec.encode({:ferric_flow_query_seek, 1, "seek", nil})
    assert {:ok, token} = Cursor.issue(binding, continuation, key: @cursor_key, now_ms: now_ms)

    assert {:error, :query_cursor_invalid} =
             PlannerEngine.execute(ctx, %{request | cursor: {:literal, :keyword, token}})
  end

  test "reports the current budget rejection for a valid pinned continuation" do
    ctx = active_context()
    request = collection("tenant-a", "failed")
    now_ms = System.system_time(:millisecond)
    assert {:ok, snapshot} = IndexRegistry.snapshot(ctx, 0)
    assert [index] = snapshot.indexes

    assert :ok =
             StatisticsStore.put(
               StatisticsStore.server_name(ctx),
               statistics(index, "tenant-a", ["tenant-a", "failed"], 50_001, now_ms)
             )

    binding = %{
      instance: ctx.name,
      scope: dedicated_query_binding("tenant-a"),
      query_fingerprint: Planner.query_fingerprint(request),
      query_digest: query_digest(request),
      index_id: index.definition.id,
      index_version: index.definition.version,
      index_build_id: index.build_id,
      order_by: request.order_by
    }

    continuation = TermCodec.encode({:ferric_flow_query_seek, 1, "seek", nil})
    assert {:ok, token} = Cursor.issue(binding, continuation, key: @cursor_key, now_ms: now_ms)

    assert {:error,
            %Error{
              reason: :query_scan_budget_exceeded,
              detail:
                "The planner rejected every active runs index because the scanned-entry ceiling would be exceeded.",
              hint: hint,
              context: %{
                "planner_reason" => "scan_budget_exceeded",
                "bounds" => bounds,
                "status_command" => "FLOW.QUERY.INDEXES"
              }
            } = diagnostic} =
             PlannerEngine.execute(ctx, %{request | cursor: {:literal, :keyword, token}})

    assert hint =~ "Tighten predicates or activate a narrower composite index"
    assert bounds["scanned_entries"] == 50_000
    assert bounds["scanned_bytes"] == 64 * 1_024 * 1_024
    refute inspect(Error.payload(diagnostic)) =~ "tenant-a"
    refute inspect(Error.payload(diagnostic)) =~ "failed"
  end

  defp active_context(catalog \\ :single_index, opts \\ []) do
    {ctx, _admission} = bare_context(opts)
    File.mkdir_p!(ctx.data_dir)
    assert :ok = Ferricstore.Flow.LMDB.ensure_shard_dirs(ctx.data_dir, 1)

    catalog_path = Path.join(ctx.data_dir, "index-catalog.json")
    write_catalog!(catalog_path, catalog)

    start_supervised!(
      {CursorKeyStore,
       instance_ctx: ctx, name: CursorKeyStore.server_name(ctx), key: {:raw, @cursor_key}}
    )

    start_supervised!(
      {IndexRegistry,
       instance_ctx: ctx, name: IndexRegistry.server_name(ctx), catalog_path: catalog_path}
    )

    activate_catalog!(ctx)

    start_supervised!(
      {StatisticsStore,
       instance_ctx: ctx, name: StatisticsStore.server_name(ctx), max_entries: 32}
    )

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx,
       name: StatisticsWorker.server_name(ctx),
       statistics_store: StatisticsStore.server_name(ctx),
       probe_interval_ms: 60_000}
    )

    ctx
  end

  defp bare_context(opts \\ []) do
    suffix = System.unique_integer([:positive, :monotonic])
    name = :"query_engine_instance_#{suffix}"
    admission = :"query_engine_admission_#{suffix}"
    data_dir = Path.join(System.tmp_dir!(), Atom.to_string(name))

    flow_metadata_snapshot =
      Keyword.get_lazy(opts, :flow_metadata_snapshot, fn ->
        {:ok, snapshot} =
          MetadataExtension.configure(FerricStore.Flow.MetadataExtension.Disabled, [])

        snapshot
      end)

    ctx = %{
      name: name,
      data_dir: data_dir,
      shard_count: 1,
      slot_map: List.to_tuple(List.duplicate(0, 1_024)),
      query_index_provider: Ferricstore.Flow.Query.IndexProvider,
      query_admission_controller: admission,
      flow_metadata_snapshot: flow_metadata_snapshot
    }

    on_exit(fn -> File.rm_rf!(data_dir) end)

    start_supervised!(
      {AdmissionController,
       name: admission,
       max_scope: Keyword.get(opts, :max_scope, 1),
       max_node: Keyword.get(opts, :max_node, 1),
       max_scope_memory_bytes: Keyword.get(opts, :max_scope_memory_bytes, 64 * 1_024 * 1_024),
       max_node_memory_bytes: Keyword.get(opts, :max_node_memory_bytes, 256 * 1_024 * 1_024)}
    )

    {ctx, admission}
  end

  defp history_context(opts \\ []) do
    suffix = System.unique_integer([:positive, :monotonic])
    name = :"query_history_instance_#{suffix}"
    admission = :"query_history_admission_#{suffix}"
    data_dir = Path.join(System.tmp_dir!(), Atom.to_string(name))

    instance_opts =
      Keyword.merge(
        [
          data_dir: data_dir,
          shard_count: 1,
          query_index_provider: FerricStore.Flow.QueryIndexProvider.Disabled,
          flow_shared_ref_backfill?: false
        ],
        opts
      )

    start_supervised!(%{
      id: {FerricStore.Instance.Supervisor, name},
      start:
        {FerricStore.Instance.Supervisor, :start_link,
         [
           name,
           instance_opts
         ]},
      restart: :temporary
    })

    instance_ctx = FerricStore.Instance.get(name)

    start_supervised!({AdmissionController, name: admission, max_scope: 1, max_node: 1})

    ctx =
      instance_ctx
      |> Map.from_struct()
      |> Map.put(:query_admission_controller, admission)

    start_supervised!(
      {CursorKeyStore,
       instance_ctx: ctx, name: CursorKeyStore.server_name(ctx), key: {:raw, @cursor_key}}
    )

    on_exit(fn ->
      FerricStore.Instance.cleanup(name)
      File.rm_rf!(data_dir)
    end)

    ctx
  end

  defp activate_catalog!(ctx) do
    server = IndexRegistry.server_name(ctx)
    assert {:ok, snapshot} = IndexRegistry.snapshot(ctx, 0)
    assert snapshot.indexes != []
    assert [build_id] = snapshot.indexes |> Enum.map(& &1.build_id) |> Enum.uniq()
    definition_count = length(snapshot.indexes)

    assert :ok =
             IndexRegistry.checkpoint_build(server, build_id, 0,
               phase: :snapshot,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    assert :ok =
             IndexRegistry.checkpoint_build(server, build_id, 0,
               phase: :backfill,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    assert :ok = IndexRegistry.complete_build_shard(server, build_id, 0)

    assert :ok =
             IndexRegistry.checkpoint_validation(server, build_id, 0,
               phase: :source,
               cursor: "",
               fenced: true,
               definition_position: 0,
               checked_records: 0,
               checked_entries: 0,
               mismatches: 0
             )

    Enum.each(0..(definition_count - 1), fn definition_position ->
      assert :ok =
               IndexRegistry.checkpoint_validation(server, build_id, 0,
                 phase: :index,
                 cursor: "",
                 fenced: true,
                 definition_position: definition_position,
                 checked_records: 0,
                 checked_entries: 0,
                 mismatches: 0
               )
    end)

    Enum.each(0..(definition_count - 1), fn definition_position ->
      assert :ok =
               IndexRegistry.checkpoint_validation(server, build_id, 0,
                 phase: :counter,
                 cursor: "",
                 fenced: true,
                 definition_position: definition_position,
                 checked_records: 0,
                 checked_entries: 0,
                 mismatches: 0
               )
    end)

    assert :ok =
             IndexRegistry.checkpoint_validation(server, build_id, 0,
               phase: :cleanup,
               cursor: "",
               fenced: true,
               definition_position: definition_count,
               checked_records: 0,
               checked_entries: 0,
               mismatches: 0
             )

    assert :ok = IndexRegistry.complete_validation_shard(server, build_id, 0)

    assert :ok = IndexRegistry.activate_build(server, build_id)
  end

  defp collection(tenant, state) do
    Request.collection(
      :execute,
      [
        {:eq, :partition_key, {:literal, :keyword, tenant}},
        {:eq, :state, {:literal, :keyword, state}}
      ],
      [{:updated_at_ms, :desc}],
      10,
      :record
    )
  end

  defp suggested_field_specs(fields) do
    Enum.map(fields, fn field ->
      {:ok, name} = Field.parse(field["name"])

      {name, String.to_existing_atom(field["direction"]),
       String.to_existing_atom(field["encoding"])}
    end)
  end

  defp shared_snapshot do
    {:ok, snapshot} = MetadataExtension.configure(SharedScopeProvider, [])
    snapshot
  end

  defp execution_context(ctx, tenant_ref) do
    %ExecutionContext{
      instance_ctx: ctx,
      request_context: %{"tenant_ref" => tenant_ref}
    }
  end

  defp dedicated_query_binding(partition) do
    {:ok, binding} = MandatoryScope.query_binding(MandatoryScope.dedicated(), partition)
    binding
  end

  defp write_catalog!(path, catalog) do
    indexes =
      [
        %{
          "id" => "flow_runs_tenant_state_updated",
          "version" => 1,
          "source" => "runs",
          "workloads" => ["WF-LIST-001"],
          "fields" => [
            %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
            %{"name" => "state", "direction" => "asc", "encoding" => "hashed"},
            %{
              "name" => "updated_at_ms",
              "direction" => "desc",
              "encoding" => "ordered"
            }
          ]
        }
      ]
      |> maybe_add_type_index(catalog)

    File.write!(
      path,
      Jason.encode!(%{
        "catalog_version" => 1,
        "contract_version" => "ferric.flow.query.index-catalog/v1",
        "indexes" => indexes
      })
    )
  end

  defp maybe_add_type_index(indexes, :single_index), do: indexes

  defp maybe_add_type_index(indexes, :competing_indexes) do
    indexes ++
      [
        %{
          "id" => "flow_runs_tenant_type_updated",
          "version" => 1,
          "source" => "runs",
          "workloads" => ["WF-LIST-002"],
          "fields" => [
            %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
            %{"name" => "type", "direction" => "asc", "encoding" => "hashed"},
            %{
              "name" => "updated_at_ms",
              "direction" => "desc",
              "encoding" => "ordered"
            }
          ]
        }
      ]
  end

  defp statistics(index, scope, prefix, count, collected_at_ms) do
    digest = IndexStatistics.prefix_digest(prefix)

    IndexStatistics.new!(%{
      index_id: index.definition.id,
      index_version: index.definition.version,
      scope_digest: IndexStatistics.scope_digest(scope),
      collected_at_ms: collected_at_ms,
      source_watermark: 1,
      total_entries: count,
      distinct_runs: count,
      prefix_counts: %{digest => count},
      prefix_observed_at_ms: %{digest => collected_at_ms},
      histograms: %{},
      null_counts: %{},
      missing_counts: %{},
      average_entry_bytes: 96,
      average_row_bytes: 384,
      sample_rate_ppm: 1_000_000,
      confidence: :high
    })
  end

  defp query_digest(request) do
    {request.version, request.source, request.predicate, request.order_by, request.limit,
     request.return}
    |> TermCodec.encode()
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp eventually(fun, attempts \\ 100)
  defp eventually(fun, 0), do: assert(fun.())

  defp eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end
end
