defmodule Ferricstore.Flow.Query.ExecutorTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.{Codec, Keys, LMDB, Locator}
  alias Ferricstore.Flow.RecordProjection, as: FlowRecordProjection
  alias Ferricstore.Flow.StorageScope
  alias Ferricstore.TermCodec

  alias Ferricstore.Flow.Query.{
    CompositeCounter,
    CompositeIndex,
    IndexDefinition,
    Limits,
    MandatoryScope,
    RecordOrder,
    RecordProjection,
    ReferenceEvaluator,
    RegisteredIndex,
    Request,
    QueryRow,
    QueryRowCodec,
    QueryRowDecodePlan,
    QueryRowStore
  }

  alias Ferricstore.Flow.Query.{
    Budget,
    Cursor,
    Executor,
    MemoryBudget,
    PageMemory,
    Plan,
    Planner,
    Response
  }

  @cursor_key :binary.copy(<<0x3C>>, 32)
  @max_exact_integer 9_007_199_254_740_991
  @max_u64 0xFFFF_FFFF_FFFF_FFFF

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

  test "executes a selective range and evaluates only residual predicates" do
    definition = state_definition()
    request = request([eq(:state, "failed"), eq(:type, "invoice")], 3)
    plan = plan!(request, definition)

    records = [
      record("run-1", 140, "failed", "invoice"),
      record("run-2", 130, "failed", "other"),
      record("run-3", 120, "failed", "invoice"),
      record("run-4", 110, "failed", "invoice")
    ]

    {range_read, record_read} = storage(definition, records)

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               page_entries: 2,
               now_ms: 1_000
             )

    assert Enum.map(result.records, & &1.id) == ["run-1", "run-3", "run-4"]
    assert result.has_more == false
    assert result.usage.scanned_entries == 4
    assert result.usage.hydrated_records == 4
    assert result.usage.residual_checks == 4
    assert result.usage.range_pages == 2
  end

  test "projects selected fields only after authoritative filtering and ordering" do
    definition = state_definition()

    request =
      request([eq(:state, "failed"), eq(:type, "invoice")], 2)
      |> Map.put(:projection, [:run_id, :state, {:attribute, "customer"}])

    plan = plan!(request, definition)

    records = [
      record("run-1", 140) |> Map.put(:attributes, %{"customer" => "one", "secret" => "x"}),
      record("run-2", 130) |> Map.put(:attributes, %{"customer" => nil}),
      record("run-3", 120, "failed", "other")
    ]

    {range_read, record_read} = storage(definition, records)

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               now_ms: 1_000
             )

    assert result.records == [
             %{id: "run-1", state: "failed", attributes: %{"customer" => "one"}},
             %{id: "run-2", state: "failed", attributes: %{"customer" => nil}}
           ]

    assert result.usage.hydrated_records == 3
    assert result.usage.residual_checks == 3
  end

  test "evaluates sparse projections and residual predicates from query rows without log hydration" do
    definition = state_definition()

    request =
      request([eq(:state, "failed"), eq(:type, "invoice")], 2)
      |> Map.put(:projection, [
        :run_id,
        :state,
        {:attribute, "customer"},
        {:state_meta, "failed", "reason"}
      ])

    records = [
      record("run-1", 140)
      |> Map.put(:attributes, %{"customer" => "one"})
      |> Map.put(:state_meta, %{"failed" => %{"reason" => "declined"}}),
      record("run-2", 130)
      |> Map.put(:attributes, %{"customer" => "two"})
      |> Map.put(:state_meta, %{"failed" => %{"reason" => nil}}),
      record("run-3", 120, "failed", "other")
    ]

    {range_read, _record_read} = storage(definition, records)
    rows = Map.new(records, &{state_key(&1), query_row(&1)})

    query_row_read = fn _path, state_keys, _now_ms, _max_bytes ->
      selected = Enum.map(state_keys, &Map.get(rows, &1))
      {:ok, selected, MemoryBudget.term_bytes(selected), true}
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               query_row_read: query_row_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a metadata-row-covered query must not read the authoritative log")
               end,
               now_ms: 1_000
             )

    assert result.records == [
             %{
               id: "run-1",
               state: "failed",
               attributes: %{"customer" => "one"},
               state_meta: %{"failed" => %{"reason" => "declined"}}
             },
             %{
               id: "run-2",
               state: "failed",
               attributes: %{"customer" => "two"},
               state_meta: %{"failed" => %{"reason" => nil}}
             }
           ]

    assert result.usage.hydrated_records == 0
    assert result.usage.residual_checks == 3
  end

  test "returns the complete bare run allowlist from query rows without log hydration" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)

    records = [
      record("run-1", 140)
      |> Map.merge(%{
        attributes: %{"customer" => "one", "reviewed" => nil},
        state_meta: %{"failed" => %{"reason" => "declined"}},
        payload_ref: "payload-secret",
        result_ref: "result-secret",
        error_ref: "error-secret",
        lease_token: "lease-secret"
      }),
      record("run-2", 130)
      |> Map.merge(%{
        next_run_at_ms: 500,
        run_state: "ready",
        root_flow_id: "run-2",
        correlation_id: "correlation-2"
      })
    ]

    {range_read, _record_read} = storage(definition, records)

    rows =
      Map.new(records, fn record ->
        row = query_row(record)
        {state_key(record), %{row | record: Map.take(row.record, RecordProjection.fields())}}
      end)

    query_row_read = fn _path, state_keys, _now_ms, _max_bytes ->
      selected = Enum.map(state_keys, &Map.get(rows, &1))
      {:ok, selected, MemoryBudget.term_bytes(selected), true}
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               query_row_read: query_row_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a bare metadata record must not read the authoritative log")
               end,
               now_ms: 1_000
             )

    assert result.records == Enum.map(records, &Map.take(&1, RecordProjection.fields()))
    assert result.usage.hydrated_records == 0

    Enum.each(result.records, fn record ->
      refute Map.has_key?(record, :payload_ref)
      refute Map.has_key?(record, :result_ref)
      refute Map.has_key?(record, :error_ref)
      refute Map.has_key?(record, :lease_token)
    end)
  end

  test "prepares the physical state-key prefix once for a query-row page" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 4)
    plan = plan!(request, definition)
    records = Enum.map(1..4, &record("run-#{&1}", 200 - &1))
    {range_read, _record_read} = storage(definition, records)
    rows = Map.new(records, &{state_key(&1), query_row(&1)})

    query_row_read = fn _path, state_keys, _now_ms, _max_bytes ->
      selected = Enum.map(state_keys, &Map.get(rows, &1))
      {:ok, selected, MemoryBudget.term_bytes(selected), true}
    end

    {execution, state_key_calls} =
      execute_with_call_trace({Keys, :state_key, 2}, fn ->
        Executor.execute(context(), 0, request, plan,
          range_read: range_read,
          query_row_read: query_row_read,
          record_read: fn _path, _keys, _now_ms, _max_bytes ->
            flunk("a metadata-row-covered query must not read the authoritative log")
          end,
          now_ms: 1_000
        )
      end)

    assert {:ok, %{records: returned}} = execution
    assert Enum.map(returned, & &1.id) == Enum.map(records, & &1.id)
    assert state_key_calls == 1
  end

  test "prepares and applies a selective decoder for non-fused query rows" do
    definition = state_definition()

    request =
      request([eq(:state, "failed")], 1)
      |> Map.put(:projection, [:run_id, :state])

    record =
      record("run-1", 140)
      |> Map.put(:attributes, %{"customer" => "one"})
      |> Map.put(:state_meta, %{"failed" => %{"reason" => "declined"}})

    {range_read, _record_read} = storage(definition, [record])
    state_key = state_key(record)
    assert {:ok, encoded} = record |> query_row() |> QueryRowCodec.encode()

    query_row_read = fn _path,
                        [^state_key],
                        now_ms,
                        _max_bytes,
                        %QueryRowDecodePlan{attributes: :none, state_meta: :none} = decode_plan ->
      assert {:ok, rows} = QueryRowStore.decode_many([state_key], [encoded], now_ms, decode_plan)
      {:ok, rows, byte_size(encoded), true}
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               query_row_read: query_row_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a metadata-row-covered query must not read the authoritative log")
               end,
               now_ms: 1_000
             )

    assert result.records == [%{id: "run-1", state: "failed"}]
  end

  test "uses exact dynamic fields for residual checks and response projection" do
    definition = state_definition()

    request =
      request(
        [
          eq(:state, "failed"),
          eq({:attribute, "customer"}, "one")
        ],
        1
      )
      |> Map.put(:projection, [
        :run_id,
        {:attribute, "customer"},
        {:state_meta, "failed", "reason"}
      ])

    record =
      record("run-1", 140)
      |> Map.put(:attributes, %{"customer" => "one", "unused" => "not-decoded"})
      |> Map.put(:state_meta, %{
        "failed" => %{"reason" => "declined", "unused" => "not-decoded"}
      })

    {range_read, _record_read} = storage(definition, [record])
    state_key = state_key(record)
    assert {:ok, encoded} = record |> query_row() |> QueryRowCodec.encode()

    query_row_read = fn _path,
                        [^state_key],
                        now_ms,
                        _max_bytes,
                        %QueryRowDecodePlan{
                          attributes: attributes,
                          state_meta: state_meta
                        } = decode_plan ->
      assert attributes == MapSet.new(["customer"])
      assert state_meta == %{"failed" => MapSet.new(["reason"])}
      assert {:ok, [row]} = QueryRowStore.decode_many([state_key], [encoded], now_ms, decode_plan)
      refute Map.has_key?(row.record, :attributes)
      refute Map.has_key?(row.record, :state_meta)
      refute Map.has_key?(row.record, {:attribute, "unused"})
      refute Map.has_key?(row.record, {:state_meta, "failed", "unused"})
      {:ok, [row], byte_size(encoded), true}
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               query_row_read: query_row_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a query-row-covered query must not read the authoritative log")
               end,
               now_ms: 1_000
             )

    assert result.records == [
             %{
               id: "run-1",
               attributes: %{"customer" => "one"},
               state_meta: %{"failed" => %{"reason" => "declined"}}
             }
           ]
  end

  test "processes a fused composite and QueryRow page without legacy reads" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    records = [record("run-1", 140), record("run-2", 130)]
    {fixture_range_read, _record_read} = storage(definition, records)
    [range] = plan.ranges

    assert {:ok, page} = fixture_range_read.("unused", range, nil, 3, 1_048_576)

    query_row_values =
      Enum.map(records, fn record ->
        assert {:ok, encoded} = record |> query_row() |> QueryRowCodec.encode()
        encoded
      end)

    query_row_bytes = Enum.sum(Enum.map(query_row_values, &byte_size/1))

    fused_read = fn
      _path, ^range, _state_key_prefix, nil, _max_entries, _max_scan_bytes, max_query_row_bytes ->
        assert query_row_bytes <= max_query_row_bytes

        {:ok,
         Map.merge(page, %{
           query_row_values: query_row_values,
           query_row_bytes: query_row_bytes
         })}
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               query_row_range_read: fused_read,
               range_read: fn _path, _range, _cursor, _max_entries, _max_bytes ->
                 flunk("the legacy composite range reader must not run")
               end,
               query_row_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("the second QueryRow lookup must not run")
               end,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a metadata-row-covered query must not read the authoritative log")
               end,
               now_ms: 1_000
             )

    assert Enum.map(result.records, & &1.id) == ["run-1", "run-2"]
    assert result.usage.hydrated_records == 0
  end

  test "falls back to bounded two-stage reads when the first fused QueryRow exceeds its budget" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    records = [record("run-1", 140), record("run-2", 130)]
    {range_read, _record_read} = storage(definition, records)
    rows = Map.new(records, &{state_key(&1), query_row(&1)})
    parent = self()

    fused_read = fn
      _path, _range, _state_key_prefix, _cursor, _max_entries, _scan_bytes, _query_row_bytes ->
        send(parent, :fused_read)
        {:error, :batch_value_budget_exceeded}
    end

    query_row_read = fn _path, state_keys, _now_ms, _max_bytes ->
      send(parent, :query_row_read)
      selected = Enum.map(state_keys, &Map.get(rows, &1))
      {:ok, selected, MemoryBudget.term_bytes(selected), true}
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               query_row_range_read: fused_read,
               range_read: range_read,
               query_row_read: query_row_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a metadata-row-covered query must not read the authoritative log")
               end,
               now_ms: 1_000
             )

    assert_receive :fused_read
    assert_receive :query_row_read
    assert Enum.map(result.records, & &1.id) == ["run-1", "run-2"]
  end

  test "uses the bounded legacy path when worst-case fused page memory cannot be reserved" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 1)
    plan = plan!(request, definition)
    {:ok, budget} = Budget.lower(plan.budget, executor_memory_bytes: 64 * 1_024)
    plan = %{plan | budget: budget}
    records = [record("run-1", 140)]
    {fixture_range_read, _record_read} = storage(definition, records)
    row = query_row(hd(records))
    assert {:ok, encoded} = QueryRowCodec.encode(row)
    parent = self()

    range_read = fn path, range, cursor, max_entries, max_bytes ->
      send(parent, :legacy_range_read)
      fixture_range_read.(path, range, cursor, max_entries, max_bytes)
    end

    query_row_read = fn _path, [_state_key], _now_ms, max_bytes ->
      send(parent, :legacy_query_row_read)
      assert byte_size(encoded) <= max_bytes
      {:ok, [row], byte_size(encoded), true}
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               query_row_range_read: fn
                 _path,
                 _range,
                 _state_key_prefix,
                 _cursor,
                 _max_entries,
                 _scan_bytes,
                 _query_row_bytes ->
                   flunk("the fused read must not run without a worst-case memory reservation")
               end,
               range_read: range_read,
               query_row_read: query_row_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a metadata-row-covered query must not read the authoritative log")
               end,
               now_ms: 1_000
             )

    assert_receive :legacy_range_read
    assert_receive :legacy_query_row_read
    assert Enum.map(result.records, & &1.id) == ["run-1"]
    assert result.usage.memory_high_water_bytes <= budget.executor_memory_bytes
  end

  test "rejects forged fused QueryRow byte accounting before decoding" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 1)
    plan = plan!(request, definition)
    record = record("run-1", 140)
    {fixture_range_read, _record_read} = storage(definition, [record])
    [range] = plan.ranges
    assert {:ok, page} = fixture_range_read.("unused", range, nil, 2, 1_048_576)
    assert {:ok, encoded} = record |> query_row() |> QueryRowCodec.encode()

    fused_read = fn
      _path, ^range, _state_key_prefix, nil, _max_entries, _scan_bytes, _query_row_bytes ->
        {:ok,
         Map.merge(page, %{
           query_row_values: [encoded],
           query_row_bytes: byte_size(encoded) - 1
         })}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               query_row_range_read: fused_read,
               range_read: fn _path, _range, _cursor, _max_entries, _max_bytes ->
                 flunk("a forged fused response must not trigger a second transaction")
               end,
               query_row_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a forged fused response must not trigger QueryRow lookup")
               end,
               now_ms: 1_000
             )
  end

  test "rejects malformed prefetched QueryRow bytes without a legacy retry" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 1)
    plan = plan!(request, definition)
    record = record("run-1", 140)
    {fixture_range_read, _record_read} = storage(definition, [record])
    [range] = plan.ranges
    assert {:ok, page} = fixture_range_read.("unused", range, nil, 2, 1_048_576)

    fused_read = fn
      _path, ^range, _state_key_prefix, nil, _max_entries, _scan_bytes, _query_row_bytes ->
        {:ok, Map.merge(page, %{query_row_values: ["corrupt"], query_row_bytes: 7})}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               query_row_range_read: fused_read,
               range_read: fn _path, _range, _cursor, _max_entries, _max_bytes ->
                 flunk("a corrupt fused row must not trigger an unpinned range retry")
               end,
               query_row_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a corrupt fused row must not trigger a second LMDB transaction")
               end,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a corrupt fused row must not reach the authoritative log")
               end,
               now_ms: 1_000
             )
  end

  test "rejects a query-row plan with a non-durable locator before log IO" do
    definition = state_definition()

    request =
      request([eq(:state, "failed"), eq(:type, "invoice")], 1)
      |> Map.put(:projection, [:run_id, :state])

    record = record("run-invalid-locator", 140)
    {range_read, _record_read} = storage(definition, [record])
    row = query_row(record)
    malformed = %{row | locator: %{row.locator | file_id: {:flow_state, 0}}}

    query_row_read = fn _path, [_state_key], _now_ms, _max_bytes ->
      {:ok, [malformed], MemoryBudget.term_bytes([malformed]), true}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               query_row_read: query_row_read,
               record_read: fn _path, _state_keys, _now_ms, _max_bytes ->
                 flunk("a non-durable locator reached authoritative storage")
               end,
               now_ms: 1_000
             )
  end

  test "a custom authoritative reader remains an explicit query-row fallback" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 1)
    record = record("run-1", 140)
    {range_read, record_read} = storage(definition, [record])
    parent = self()

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               record_read: fn path, keys, now_ms, max_bytes ->
                 send(parent, :authoritative_hydration)
                 record_read.(path, keys, now_ms, max_bytes)
               end,
               now_ms: 1_000
             )

    assert_receive :authoritative_hydration
    assert Enum.map(result.records, & &1.id) == [record.id]
    assert result.usage.hydrated_records == 1
  end

  test "serves a covered sparse projection without primary-record hydration" do
    definition = covering_state_definition()

    request =
      request([eq(:state, "failed")], 2)
      |> Map.put(:projection, [:run_id, :state, :updated_at_ms])

    records = [record("run-1", 140), record("run-2", 130)]
    {range_read, _record_read} = storage(definition, records)

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a covered projection must not hydrate the primary record")
               end,
               now_ms: 1_000
             )

    assert result.records == [
             %{id: "run-1", state: "failed", updated_at_ms: 140},
             %{id: "run-2", state: "failed", updated_at_ms: 130}
           ]

    assert result.usage.hydrated_records == 0
  end

  test "serves covered dynamic attributes and state metadata without hydration" do
    definition =
      IndexDefinition.new!(%{
        id: "flow_runs_tenant_tier_updated_covering",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {{:attribute, "tier"}, :asc},
          {:updated_at_ms, :desc}
        ],
        covering_fields: [
          :partition_key,
          :run_id,
          :updated_at_ms,
          :version,
          {:attribute, "tier"},
          {:attribute, "labels"},
          {:state_meta, "failed", "score"}
        ]
      })

    request =
      request([eq({:attribute, "tier"}, "gold")], 2)
      |> Map.put(:projection, [
        :run_id,
        {:attribute, "tier"},
        {:attribute, "labels"},
        {:state_meta, "failed", "score"}
      ])

    records = [
      record("run-1", 140)
      |> Map.put(:attributes, %{"tier" => "gold", "labels" => ["urgent", "finance"]})
      |> Map.put(:state_meta, %{"failed" => %{"score" => 1.5}}),
      record("run-2", 130)
      |> Map.put(:attributes, %{"tier" => "gold", "labels" => ["standard"]})
      |> Map.put(:state_meta, %{"failed" => %{"score" => nil}})
    ]

    {range_read, _record_read} = storage(definition, records)

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("covered dynamic fields must not hydrate primary records")
               end,
               now_ms: 1_000
             )

    assert result.records == [
             %{
               id: "run-1",
               attributes: %{"tier" => "gold", "labels" => ["urgent", "finance"]},
               state_meta: %{"failed" => %{"score" => 1.5}}
             },
             %{
               id: "run-2",
               attributes: %{"tier" => "gold", "labels" => ["standard"]},
               state_meta: %{"failed" => %{"score" => nil}}
             }
           ]

    assert result.usage.hydrated_records == 0
  end

  test "rejects a covering index row that omits its required projection" do
    definition = covering_state_definition()

    request =
      request([eq(:state, "failed")], 1)
      |> Map.put(:projection, [:run_id, :state])

    plan = plan!(request, definition)
    [entry] = entries(definition, record("run-1", 140))
    malformed = %{entry | covering_record: nil}

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), [malformed]),
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a missing covering projection must fail before hydration")
               end,
               now_ms: 1_000
             )
  end

  test "evaluates covered residual predicates without metadata or WAL hydration" do
    definition = covering_state_definition([:type])

    request =
      request([eq(:state, "failed"), eq(:type, "invoice")], 1)
      |> Map.put(:projection, [:run_id, :state])

    records = [record("run-1", 140), record("run-2", 130, "failed", "other")]
    {range_read, _record_read} = storage(definition, records)

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               query_row_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a covered residual predicate must not read QueryRows")
               end,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a covered residual predicate must not hydrate WAL records")
               end,
               now_ms: 1_000
             )

    assert Enum.map(result.records, & &1.id) == ["run-1"]
    assert result.usage.hydrated_records == 0
    assert result.usage.residual_checks == 2
  end

  test "counts covering rows without primary-record hydration" do
    definition = covering_state_definition()
    request = count_request([eq(:state, "failed")])
    {range_read, _record_read} = storage(definition, [record("run-1", 140), record("run-2", 130)])

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("an authoritative covering count must not hydrate primary records")
               end,
               now_ms: 1_000
             )

    assert result.count == 2
    assert result.usage.hydrated_records == 0
  end

  test "rejects a covering row whose payload does not match its index identity" do
    definition = covering_state_definition()

    request =
      request([eq(:state, "failed")], 1)
      |> Map.put(:projection, [:run_id, :state])

    plan = plan!(request, definition)
    [entry] = entries(definition, record("run-1", 140))
    forged = put_in(entry.covering_record.id, "other")

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), [forged]),
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a malformed cover must fail before hydration")
               end,
               now_ms: 1_000
             )
  end

  test "shared execution hydrates only the sealed physical scope and evaluates logical records" do
    scope = shared_scope(11)
    definition = shared_state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition, scope)
    logical = record("run-shared", 140)
    raw = scoped_record(logical, scope)
    {range_read, record_read} = storage(definition, [raw])

    assert {:ok, result} =
             Executor.execute(context(scope), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               now_ms: 1_000
             )

    assert [%{id: "run-shared", partition_key: "tenant"} = projected] = result.records
    refute Map.has_key?(projected, :system_metadata)

    wrong_scope = shared_scope(22)
    forged = scoped_record(logical, wrong_scope) |> Map.put(:partition_key, raw.partition_key)
    forged_read = fn _path, _keys, _now_ms, _max_bytes -> {:ok, [forged]} end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(scope), 0, request, plan,
               range_read: range_read,
               record_read: forged_read,
               now_ms: 1_000
             )
  end

  test "shared execution validates and serves a scope-bound covering row" do
    scope = shared_scope(11)

    definition =
      shared_state_definition()
      |> Map.from_struct()
      |> Map.put(:covering_fields, [
        :partition_key,
        :run_id,
        :state,
        :updated_at_ms,
        :version
      ])
      |> IndexDefinition.new!()

    request =
      request([eq(:state, "failed")], 1)
      |> Map.put(:projection, [:run_id, :partition_key, :state])

    raw = record("run-shared", 140) |> scoped_record(scope)
    {range_read, _record_read} = storage(definition, [raw])

    assert {:ok, result} =
             Executor.execute(context(scope), 0, request, plan!(request, definition, scope),
               range_read: range_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a scope-bound covering row must not hydrate")
               end,
               now_ms: 1_000
             )

    assert result.records == [%{id: "run-shared", partition_key: "tenant", state: "failed"}]
    assert result.usage.hydrated_records == 0
  end

  test "shared execution serves a scope-bound query row without log hydration" do
    scope = shared_scope(11)
    definition = shared_state_definition()

    request =
      request([eq(:state, "failed")], 1)
      |> Map.put(:projection, [:run_id, :partition_key, :state])

    raw = record("run-shared-row", 140) |> scoped_record(scope)
    {range_read, _record_read} = storage(definition, [raw])
    encoded_record = Codec.encode_record(raw)

    locator =
      Locator.new!(
        flow_id: raw.id,
        kind: :state,
        version: raw.version,
        raft_index: raw.version,
        file_id: {:waraft_apply_projection, raw.version},
        offset: 0,
        value_size: byte_size(encoded_record),
        checksum: :crypto.hash(:sha256, encoded_record),
        segment_generation: 0,
        frame_size: 1_024
      )

    state_key = state_key(raw)
    assert {:ok, encoded_row} = QueryRowCodec.encode(state_key, raw, locator, 0)
    assert {:ok, row} = QueryRowCodec.decode(encoded_row, state_key)

    query_row_read = fn _path, [^state_key], _now_ms, _max_bytes ->
      {:ok, [row], MemoryBudget.term_bytes([row]), true}
    end

    assert {:ok, result} =
             Executor.execute(context(scope), 0, request, plan!(request, definition, scope),
               range_read: range_read,
               query_row_read: query_row_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a scope-bound query row must not hydrate")
               end,
               now_ms: 1_000
             )

    assert result.records == [
             %{id: "run-shared-row", partition_key: "tenant", state: "failed"}
           ]

    assert result.usage.hydrated_records == 0

    forged = %{row | scope_prefix: <<22::unsigned-big-64>>}

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(scope), 0, request, plan!(request, definition, scope),
               range_read: range_read,
               query_row_read: fn _path, [_key], _now_ms, _max_bytes ->
                 {:ok, [forged], MemoryBudget.term_bytes([forged]), true}
               end,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a forged query row must fail before hydration")
               end,
               now_ms: 1_000
             )
  end

  test "rejects a self-consistent plan outside the independently bound scope before storage" do
    authorized_scope = shared_scope(11)
    forged_scope = shared_scope(22)
    definition = shared_state_definition()
    request = request([eq(:state, "failed")], 1)
    forged_plan = plan!(request, definition, forged_scope)

    storage_read = fn _path, _range, _cursor, _max_entries, _max_bytes ->
      flunk("scope authority must be checked before index storage")
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(authorized_scope), 0, request, forged_plan,
               range_read: storage_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("scope authority must be checked before record storage")
               end,
               cursor_key: @cursor_key,
               now_ms: 1_000
             )
  end

  test "stops an order-preserving range after one lookahead result" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2, [{:updated_at_ms, :desc}])
    assert %Plan{order: :native} = plan = plan!(request, definition)

    records = for value <- 1..20, do: record("run-#{value}", 1_000 - value)
    {range_read, record_read} = storage(definition, records)

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               page_entries: 3,
               cursor_key: @cursor_key,
               now_ms: 1_000
             )

    assert result.usage.scanned_entries == 3
    assert result.has_more

    assert {:ok, expected} = RecordOrder.sort(records, request.order_by)
    assert Enum.map(result.records, & &1.id) == expected |> Enum.take(2) |> Enum.map(& &1.id)
  end

  test "bounds a covered native-order storage page to the result lookahead" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2, [{:updated_at_ms, :desc}])

    assert %Plan{order: :native, residual_predicates: [], deduplicate: false} =
             plan = plan!(request, definition)

    records = for value <- 1..20, do: record("run-#{value}", 1_000 - value)
    {base_range_read, record_read} = storage(definition, records)
    test_pid = self()

    range_read = fn path, range, cursor, max_entries, max_bytes ->
      send(test_pid, {:range_page_budget, max_entries})
      base_range_read.(path, range, cursor, max_entries, max_bytes)
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               cursor_key: @cursor_key,
               now_ms: 1_000
             )

    assert_received {:range_page_budget, 3}
    refute_received {:range_page_budget, _other}
    assert result.usage.scanned_entries == 3
    assert result.usage.hydrated_records == 3
    assert result.has_more
  end

  test "continues bounded native reads after an expired projection is skipped" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2, [{:updated_at_ms, :desc}])
    plan = plan!(request, definition)
    records = for value <- 1..4, do: record("run-#{value}", 1_000 - value)
    {base_range_read, base_record_read} = storage(definition, records)
    expired_state_key = records |> hd() |> state_key()
    test_pid = self()

    range_read = fn path, range, cursor, max_entries, max_bytes ->
      send(test_pid, {:range_page_budget, max_entries})
      {:ok, page} = base_range_read.(path, range, cursor, max_entries, max_bytes)

      entries =
        Enum.map(page.entries, fn entry ->
          if entry.state_key == expired_state_key, do: %{entry | expire_at_ms: 999}, else: entry
        end)

      {:ok, %{page | entries: entries}}
    end

    record_read = fn path, state_keys, now_ms, max_bytes ->
      {:ok, values} = base_record_read.(path, state_keys, now_ms, max_bytes)

      {:ok,
       Enum.zip_with(state_keys, values, fn
         ^expired_state_key, _record -> nil
         _state_key, record -> record
       end)}
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               cursor_key: @cursor_key,
               now_ms: 1_000
             )

    assert_received {:range_page_budget, 3}
    assert_received {:range_page_budget, 1}
    assert result.usage.scanned_entries == 4
    assert Enum.map(result.records, & &1.id) == ["run-2", "run-3"]
    assert result.has_more
  end

  test "paginates an order-preserving range with an opaque bounded seek cursor" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2, [{:updated_at_ms, :desc}])
    records = for value <- 1..6, do: record("run-#{value}", 1_000 - value)
    {range_read, record_read} = storage(definition, records)

    assert {:ok, first} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               record_read: record_read,
               page_entries: 3,
               cursor_key: @cursor_key,
               now_ms: 1_000
             )

    assert first.has_more
    assert is_binary(first.continuation)
    assert String.starts_with?(first.continuation, "fqc1_")

    second_request = with_cursor(request, first.continuation)

    assert {:ok, second} =
             Executor.execute(context(), 0, second_request, plan!(second_request, definition),
               range_read: range_read,
               record_read: record_read,
               page_entries: 3,
               cursor_key: @cursor_key,
               now_ms: 1_001
             )

    third_request = with_cursor(request, second.continuation)

    assert {:ok, third} =
             Executor.execute(context(), 0, third_request, plan!(third_request, definition),
               range_read: range_read,
               record_read: record_read,
               page_entries: 3,
               cursor_key: @cursor_key,
               now_ms: 1_002
             )

    assert {:ok, expected_records} = RecordOrder.sort(records, request.order_by)
    expected = Enum.map(expected_records, & &1.id)

    assert Enum.map(first.records ++ second.records ++ third.records, & &1.id) == expected
    assert second.usage.scanned_entries <= 3
    assert third.usage.scanned_entries <= 3
    refute third.has_more
    assert third.continuation == nil
  end

  test "rejects replaying a cursor with different bound predicate values" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 1, [{:updated_at_ms, :desc}])
    records = [record("failed-1", 200), record("failed-2", 100)]
    {range_read, record_read} = storage(definition, records)

    assert {:ok, %{continuation: token}} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               record_read: record_read,
               cursor_key: @cursor_key,
               now_ms: 1_000
             )

    changed = request([eq(:state, "running")], 1, [{:updated_at_ms, :desc}]) |> with_cursor(token)

    assert {:error, :query_cursor_invalid} =
             Executor.execute(context(), 0, changed, plan!(changed, definition),
               range_read: fn _path, _range, _cursor, _entries, _bytes ->
                 flunk("invalid cursor reached storage")
               end,
               record_read: record_read,
               cursor_key: @cursor_key,
               now_ms: 1_001
             )
  end

  test "rejects an authenticated oversized native storage cursor before storage" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 1, [{:updated_at_ms, :desc}])
    plan = plan!(request, definition)
    [range] = plan.ranges
    oversized_storage_key = range.prefix <> :binary.copy(<<0>>, 512)

    continuation =
      TermCodec.encode({:ferric_flow_query_seek, 1, <<1>>, oversized_storage_key})

    binding = %{
      instance: context().name,
      scope: dedicated_query_binding("tenant"),
      query_fingerprint: plan.query_fingerprint,
      query_digest: query_digest(request),
      index_id: plan.index_id,
      index_version: plan.index_version,
      index_build_id: plan.index_build_id,
      order_by: request.order_by
    }

    assert {:ok, token} =
             Cursor.issue(binding, continuation, key: @cursor_key, now_ms: 1_000)

    parent = self()

    range_read = fn _path, _range, cursor, _max_entries, _max_bytes ->
      send(parent, {:range_read, cursor})
      {:error, :unexpected}
    end

    assert {:error, :query_cursor_invalid} =
             Executor.execute(context(), 0, with_cursor(request, token), plan,
               range_read: range_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes -> {:ok, []} end,
               cursor_key: @cursor_key,
               now_ms: 1_001
             )

    refute_receive {:range_read, _cursor}
  end

  test "uses bounded top-K for an explicit non-native numeric order" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2, [{:priority, :desc}])
    assert %Plan{order: :bounded_top_k} = plan = plan!(request, definition)

    records = [
      record("a", 100) |> Map.put(:priority, 1),
      record("z", 90) |> Map.put(:priority, 4),
      record("m", 80) |> Map.put(:priority, 3),
      record("b", 70) |> Map.put(:priority, 2)
    ]

    {range_read, record_read} = storage(definition, records)

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               page_entries: 2,
               cursor_key: @cursor_key,
               now_ms: 1_000
             )

    assert Enum.map(result.records, & &1.id) == ["z", "m"]
    assert result.has_more
    assert result.usage.scanned_entries == 4
    assert result.usage.residual_checks == 0
    assert result.usage.memory_high_water_bytes <= plan.budget.executor_memory_bytes
  end

  test "bounded top-K does not project candidates that cannot enter the retained heap" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2, [{:priority, :desc}])
    plan = plan!(request, definition)

    records = [
      record("high", 100) |> Map.put(:priority, 4),
      record("middle", 90) |> Map.put(:priority, 3),
      record("low", 80) |> Map.put(:priority, 2),
      record("loser", 70) |> Map.put(:priority, 1)
    ]

    {range_read, record_read} = storage(definition, records)

    {execution, projection_calls} =
      execute_with_call_trace({RecordProjection, :project_validated, 3}, fn ->
        Executor.execute(context(), 0, request, plan,
          range_read: range_read,
          record_read: record_read,
          page_entries: 4,
          cursor_key: @cursor_key,
          now_ms: 1_000
        )
      end)

    assert {:ok, result} = execution
    assert Enum.map(result.records, & &1.id) == ["high", "middle"]
    assert projection_calls == 3
  end

  test "does not revalidate the sealed index definition per hydrated row" do
    definition = state_definition()
    request = count_request([eq(:state, "failed")])
    plan = plan!(request, definition)
    records = Enum.map(1..4, &record("run-#{&1}", 200 - &1))
    {single_range_read, single_record_read} = storage(definition, Enum.take(records, 1))
    {multi_range_read, multi_record_read} = storage(definition, records)

    {single_execution, single_validation_calls} =
      execute_with_call_trace({IndexDefinition, :validate, 1}, fn ->
        Executor.execute(context(), 0, request, plan,
          range_read: single_range_read,
          record_read: single_record_read,
          page_entries: 4,
          now_ms: 1_000
        )
      end)

    {multi_execution, multi_validation_calls} =
      execute_with_call_trace({IndexDefinition, :validate, 1}, fn ->
        Executor.execute(context(), 0, request, plan,
          range_read: multi_range_read,
          record_read: multi_record_read,
          page_entries: 4,
          now_ms: 1_000
        )
      end)

    assert {:ok, %{count: 1}} = single_execution
    assert {:ok, %{count: 4}} = multi_execution
    assert multi_validation_calls == single_validation_calls
  end

  test "paginates bounded top-K order without duplicates" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2, [{:priority, :desc}])

    records =
      [{"a", 1}, {"z", 6}, {"m", 4}, {"b", 2}, {"y", 5}, {"c", 3}]
      |> Enum.map(fn {id, priority} -> record(id, 100) |> Map.put(:priority, priority) end)

    {range_read, record_read} = storage(definition, records)

    {ids, final} = collect_pages(request, definition, range_read, record_read, [], 0)
    assert ids == ~w(z y m c b a)
    refute final.has_more
    assert final.continuation == nil
  end

  test "paginates a multi-range union in global order after deduplication" do
    definition = tag_definition()

    request =
      request(
        [{:in, {:attribute, "tags"}, [keyword("blue"), keyword("green")]}],
        1,
        [{:updated_at_ms, :desc}]
      )

    records = [
      record("both", 300) |> Map.put(:attributes, %{"tags" => ["blue", "green"]}),
      record("blue", 200) |> Map.put(:attributes, %{"tags" => ["blue"]}),
      record("green", 100) |> Map.put(:attributes, %{"tags" => ["green"]})
    ]

    {range_read, record_read} = storage(definition, records)
    {ids, final} = collect_pages(request, definition, range_read, record_read, [], 0)

    assert ids == ~w(both blue green)
    assert final.usage.duplicate_entries == 1
    refute final.has_more
  end

  test "deduplicates a multi-value IN union before hydration" do
    definition = tag_definition()

    request =
      request(
        [
          {:in, {:attribute, "tags"}, [keyword("blue"), keyword("green")]}
        ],
        10,
        [{:updated_at_ms, :desc}]
      )

    assert %Plan{path: :ordered_range_union} = plan = plan!(request, definition)

    records = [
      record("both", 300) |> Map.put(:attributes, %{"tags" => ["blue", "green"]}),
      record("blue", 200) |> Map.put(:attributes, %{"tags" => ["blue"]}),
      record("green", 100) |> Map.put(:attributes, %{"tags" => ["green"]})
    ]

    parent = self()
    {range_read, base_record_read} = storage(definition, records)

    record_read = fn path, state_keys, now_ms, max_bytes ->
      send(parent, {:hydrate, state_keys})
      base_record_read.(path, state_keys, now_ms, max_bytes)
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               now_ms: 1_000
             )

    assert Enum.map(result.records, & &1.id) == ["both", "blue", "green"]
    assert result.usage.scanned_entries == 4
    assert result.usage.hydrated_records == 3
    assert result.usage.duplicate_entries == 1

    hydrated = receive_hydrated([])
    assert hydrated |> List.flatten() |> Enum.sort() == Enum.sort(Enum.map(records, &state_key/1))
  end

  test "counts every exact match across bounded pages without retaining result rows" do
    definition = state_definition()

    request =
      count_request([
        eq(:state, "failed"),
        eq(:type, "invoice")
      ])

    assert %Plan{path: :count_scan, order: :none} = plan = plan!(request, definition)

    records = [
      record("run-1", 140, "failed", "invoice"),
      record("run-2", 130, "failed", "other"),
      record("run-3", 120, "failed", "invoice"),
      record("run-4", 110, "failed", "invoice")
    ]

    {range_read, record_read} = storage(definition, records)

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               page_entries: 2,
               now_ms: 1_000
             )

    assert result.count == 3
    assert result.usage.scanned_entries == 4
    assert result.usage.hydrated_records == 4
    assert result.usage.result_records == 1
    assert result.usage.range_pages == 2
    assert result.quality.pagination == "none"
    refute Map.has_key?(result, :records)
  end

  test "fully covered counts do not materialize public record maps" do
    definition = state_definition()
    request = count_request([eq(:state, "failed")])
    plan = plan!(request, definition)
    records = Enum.map(1..4, &record("run-#{&1}", 200 - &1))
    {range_read, record_read} = storage(definition, records)

    {execution, public_projection_calls} =
      execute_with_call_trace({FlowRecordProjection, :public, 1}, fn ->
        Executor.execute(context(), 0, request, plan,
          range_read: range_read,
          record_read: record_read,
          page_entries: 4,
          now_ms: 1_000
        )
      end)

    assert {:ok, %{count: 4}} = execution
    assert public_projection_calls == 0
  end

  test "prepared record matching does not reconstruct logical scope per covered count row" do
    definition = state_definition()
    request = count_request([eq(:state, "failed")])
    plan = plan!(request, definition)
    records = Enum.map(1..4, &record("run-#{&1}", 200 - &1))
    {range_read, record_read} = storage(definition, records)

    {execution, logical_scope_calls} =
      execute_with_call_trace({StorageScope, :logical_partition_key, 1}, fn ->
        Executor.execute(context(), 0, request, plan,
          range_read: range_read,
          record_read: record_read,
          page_entries: 4,
          now_ms: 1_000
        )
      end)

    assert {:ok, %{count: 4}} = execution
    assert logical_scope_calls == 0
  end

  test "contradictory exact predicates execute as an empty bounded count" do
    definition = counted_state_definition()

    request =
      count_request([
        eq(:state, "failed"),
        eq(:state, "completed")
      ])

    assert %Plan{path: :count_scan, residual_predicates: [_residual]} =
             plan = plan!(request, definition)

    records = [
      record("failed-run", 200, "failed"),
      record("completed-run", 100, "completed")
    ]

    {range_read, record_read} = storage(definition, records)

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               page_entries: 1,
               now_ms: 1_000
             )

    assert result.count == 0
    assert result.usage.scanned_entries == 1
    assert result.usage.hydrated_records == 1
    assert result.usage.result_records == 1
  end

  test "count unions deduplicate multivalue index entries before hydration" do
    definition = tag_definition()

    request =
      count_request([
        {:in, {:attribute, "tags"}, [keyword("blue"), keyword("green")]}
      ])

    plan = plan!(request, definition)

    records = [
      record("both", 300) |> Map.put(:attributes, %{"tags" => ["blue", "green"]}),
      record("blue", 200) |> Map.put(:attributes, %{"tags" => ["blue"]}),
      record("green", 100) |> Map.put(:attributes, %{"tags" => ["green"]})
    ]

    {range_read, record_read} = storage(definition, records)

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               page_entries: 2,
               now_ms: 1_000
             )

    assert result.count == 3
    assert result.usage.scanned_entries == 4
    assert result.usage.hydrated_records == 3
    assert result.usage.duplicate_entries == 1
  end

  test "single-range counts do not retain an unnecessary global ID set" do
    definition = state_definition()
    request = count_request([eq(:state, "failed")])
    assert %Plan{deduplicate: false} = plan = plan!(request, definition)
    id = :binary.copy("r", 2_048)
    matching = record(id, 100)
    [entry] = entries(definition, matching)

    exact_working_set =
      PageMemory.retained_bytes([entry], entry.storage_bytes) +
        MemoryBudget.term_bytes([{entry, matching}]) + 128

    assert {:ok, budget} =
             Budget.lower(plan.budget, executor_memory_bytes: exact_working_set)

    {range_read, record_read} = storage(definition, [matching])

    assert {:ok, %{count: 1} = result} =
             Executor.execute(context(), 0, request, %{plan | budget: budget},
               range_read: range_read,
               record_read: record_read,
               page_entries: 1,
               now_ms: 1_000
             )

    assert result.usage.memory_high_water_bytes <= exact_working_set
  end

  test "single-range counts reject a duplicate current-version key not owned by the hydrated row" do
    definition = state_definition()
    request = count_request([eq(:state, "failed")])
    assert %Plan{deduplicate: false, ranges: [range]} = plan = plan!(request, definition)
    current = record("duplicate", 100)
    [authoritative] = entries(definition, current)
    [forged] = entries(definition, record("duplicate", 90))
    page = Enum.sort_by([authoritative, forged], & &1.storage_key)

    record_read = fn _path, state_keys, _now_ms, _max_bytes ->
      assert state_keys == [state_key(current), state_key(current)]
      {:ok, [current, current]}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(range, page),
               record_read: record_read,
               now_ms: 1_000
             )
  end

  test "count budget exhaustion fails instead of returning a partial scalar" do
    definition = state_definition()
    request = count_request([eq(:state, "failed")])
    plan = plan!(request, definition)
    {:ok, budget} = Budget.lower(plan.budget, scan_entries: 2)
    plan = %{plan | budget: budget}
    records = [record("a", 100), record("b", 90), record("c", 80)]
    {range_read, record_read} = storage(definition, records)

    assert {:error, :query_scan_budget_exceeded} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               page_entries: 10,
               now_ms: 1_000
             )
  end

  test "reads covered count prefixes without index scans or row hydration" do
    definition = counted_state_definition()
    request = count_request([eq(:state, "failed")])
    assert %Plan{path: :counter_lookup, ranges: [range]} = plan = plan!(request, definition)
    path = lmdb_path()
    counter_key = CompositeCounter.key(definition, range.prefix)
    blob = CompositeCounter.encode_value(range.prefix, 7)
    assert :ok = LMDB.write_batch(path, [{:put, counter_key, blob}])

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               path: path,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("counter lookup hydrated Flow rows")
               end,
               now_ms: 1_000
             )

    assert result.count == 7
    assert result.usage.range_seeks == 1
    assert result.usage.scanned_entries == 1
    assert result.usage.hydrated_records == 0
    assert result.usage.residual_checks == 0
  end

  test "falls back to a bounded scan when a covered count can include expired rows" do
    definition = counted_state_definition()
    request = count_request([eq(:state, "failed")])
    assert %Plan{path: :counter_lookup, ranges: [range]} = plan = plan!(request, definition)
    path = lmdb_path()
    record = record("expired", 100)
    state_key = state_key(record)

    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 500)

    locator =
      Locator.new!(
        flow_id: record.id,
        kind: :state,
        version: record.version,
        raft_index: record.version,
        file_id: {:waraft_apply_projection, record.version},
        offset: 0,
        value_size: byte_size(Codec.encode_record(record)),
        checksum: :crypto.hash(:sha256, Codec.encode_record(record)),
        expire_at_ms: 500,
        segment_generation: 0,
        frame_size: 1_024
      )

    assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 500)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, state_key, query_row},
               {:put, entry.key, entry.value},
               {:put, CompositeCounter.key(definition, range.prefix),
                CompositeCounter.encode_value(range.prefix, 1, 1)}
             ])

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan, path: path, now_ms: 1_000)

    assert result.count == 0
    assert result.usage.range_seeks == 1
    assert result.usage.range_pages == 1
    assert result.usage.scanned_entries == 2
    assert result.usage.hydrated_records == 0
  end

  test "counts live expiring rows from query metadata during counter fallback" do
    definition = counted_state_definition()
    request = count_request([eq(:state, "failed")])
    assert %Plan{path: :counter_lookup, ranges: [range]} = plan = plan!(request, definition)
    path = lmdb_path()
    record = record("live-expiring", 100)
    state_key = state_key(record)

    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 2_000)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, entry.key, entry.value},
               {:put, CompositeCounter.key(definition, range.prefix),
                CompositeCounter.encode_value(range.prefix, 1, 1)}
             ])

    row = %{query_row(record) | expire_at_ms: 2_000}

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               path: path,
               query_row_read: fn ^path, [^state_key], 1_000, _max_bytes ->
                 {:ok, [row], MemoryBudget.term_bytes([row]), true}
               end,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("an exact metadata count fallback must not hydrate the WAL record")
               end,
               now_ms: 1_000
             )

    assert result.count == 1
    assert result.usage.hydrated_records == 0
  end

  test "sums disjoint scalar counter ranges and rejects overflow or prefix mismatch" do
    definition = counted_state_definition()

    request =
      count_request([
        {:in, :state, [keyword("failed"), keyword("completed")]}
      ])

    assert %Plan{path: :counter_lookup, ranges: [first, second]} =
             plan = plan!(request, definition)

    path = lmdb_path()

    ops =
      Enum.zip([first, second], [2, 3])
      |> Enum.map(fn {range, count} ->
        {:put, CompositeCounter.key(definition, range.prefix),
         CompositeCounter.encode_value(range.prefix, count)}
      end)

    assert :ok = LMDB.write_batch(path, ops)

    assert {:ok, %{count: 5}} =
             Executor.execute(context(), 0, request, plan, path: path, now_ms: 1_000)

    wrong_blob = CompositeCounter.encode_value(first.prefix <> "forged", 2)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, CompositeCounter.key(definition, first.prefix), wrong_blob}
             ])

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan, path: path, now_ms: 1_000)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, CompositeCounter.key(definition, first.prefix),
                CompositeCounter.encode_value(first.prefix, @max_u64)},
               {:put, CompositeCounter.key(definition, second.prefix),
                CompositeCounter.encode_value(second.prefix, 1)}
             ])

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan, path: path, now_ms: 1_000)
  end

  test "returns no partial result when an index-entry budget is exhausted" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2, [{:priority, :desc}])
    plan = plan!(request, definition)
    {:ok, budget} = Budget.lower(plan.budget, scan_entries: 2)
    plan = %{plan | budget: budget}

    records = [record("a", 100), record("b", 90), record("c", 80)]
    {range_read, record_read} = storage(definition, records)

    assert {:error, :query_scan_budget_exceeded} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               page_entries: 10,
               now_ms: 1_000
             )
  end

  test "rejects a cross-tenant state key before any record read" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    [range] = plan.ranges
    record = record("run-1", 100)
    [entry] = entries(definition, record)
    corrupt = %{entry | state_key: Keys.state_key(record.id, "other-tenant")}
    parent = self()

    range_read = one_page_reader(range, [corrupt])

    record_read = fn _path, _keys, _now_ms, _max_bytes ->
      send(parent, :record_read)
      {:ok, []}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               now_ms: 1_000
             )

    refute_receive :record_read
  end

  test "rejects underreported range bytes before any record read" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    [entry] = entries(definition, record("run-1", 100))
    parent = self()

    range_read = fn _path, range, nil, _max_entries, _max_bytes ->
      assert range == hd(plan.ranges)

      {:ok,
       %{
         entries: [entry],
         cursor: nil,
         exhausted: true,
         scanned_entries: 1,
         scanned_bytes: entry.storage_bytes - 1
       }}
    end

    record_read = fn _path, _keys, _now_ms, _max_bytes ->
      send(parent, :record_read)
      {:ok, []}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               now_ms: 1_000
             )

    refute_receive :record_read
  end

  test "rejects an entry whose decoded binaries exceed its claimed storage bytes" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    [entry] = entries(definition, record("run-1", 100))
    corrupt = %{entry | storage_bytes: byte_size(entry.storage_key) + 1}
    parent = self()

    record_read = fn _path, _keys, _now_ms, _max_bytes ->
      send(parent, :record_read)
      {:ok, [record("run-1", 100)]}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), [corrupt]),
               record_read: record_read,
               now_ms: 1_000
             )

    refute_receive :record_read
  end

  test "rejects a malformed final page entry before reading its cursor field" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    parent = self()

    range_read = fn _path, _range, nil, _max_entries, _max_bytes ->
      {:ok,
       %{
         entries: [%{}],
         cursor: "forged-cursor",
         exhausted: false,
         scanned_entries: 1,
         scanned_bytes: 0
       }}
    end

    record_read = fn _path, _keys, _now_ms, _max_bytes ->
      send(parent, :record_read)
      {:ok, []}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               now_ms: 1_000
             )

    refute_receive :record_read
  end

  test "rejects an oversized projected identity before hashing or hydration" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    oversized_id = :binary.copy("x", Limits.max_run_id_bytes() + 1)
    oversized_record = record(oversized_id, 100)
    [valid_entry] = entries(definition, record("run-1", 100))
    entry = %{valid_entry | id: oversized_id}
    parent = self()

    record_read = fn _path, _keys, _now_ms, _max_bytes ->
      send(parent, :record_read)
      {:ok, [oversized_record]}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), [entry]),
               record_read: record_read,
               now_ms: 1_000
             )

    refute_receive :record_read
  end

  test "rejects an out-of-domain projected version before hydration" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    [entry] = entries(definition, record("run-1", 100))
    corrupt = %{entry | record_version: @max_exact_integer + 1}
    parent = self()

    record_read = fn _path, _keys, _now_ms, _max_bytes ->
      send(parent, :record_read)
      {:ok, [record("run-1", 100)]}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), [corrupt]),
               record_read: record_read,
               now_ms: 1_000
             )

    refute_receive :record_read
  end

  test "rejects an out-of-domain projected expiry before hydration" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    [entry] = entries(definition, record("run-1", 100))
    corrupt = %{entry | expire_at_ms: @max_u64 + 1}
    parent = self()

    record_read = fn _path, _keys, _now_ms, _max_bytes ->
      send(parent, :record_read)
      {:ok, [record("run-1", 100)]}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), [corrupt]),
               record_read: record_read,
               now_ms: 1_000
             )

    refute_receive :record_read
  end

  test "rejects an oversized composite storage key before hydration" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    [entry] = entries(definition, record("run-1", 100))
    identity_offset = byte_size(entry.storage_key) - 33

    oversized_key =
      binary_part(entry.storage_key, 0, identity_offset) <>
        :binary.copy(<<0>>, 512) <>
        binary_part(entry.storage_key, identity_offset, 33)

    corrupt = %{
      entry
      | storage_key: oversized_key,
        storage_bytes: entry.storage_bytes + 512
    }

    parent = self()

    record_read = fn _path, _keys, _now_ms, _max_bytes ->
      send(parent, :record_read)
      {:ok, [record("run-1", 100)]}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), [corrupt]),
               record_read: record_read,
               now_ms: 1_000
             )

    refute_receive :record_read
  end

  test "detects a projection change between range and row reads" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    record = record("run-1", 100)
    {range_read, _record_read} = storage(definition, [record])

    changed = %{record | version: record.version + 1}
    record_read = fn _path, _keys, _now_ms, _max_bytes -> {:ok, [changed]} end

    assert {:error, :query_projection_changed} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               now_ms: 1_000
             )
  end

  test "does not hide a live row behind a stale projected expiry" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    record = record("run-1", 100)
    [entry] = entries(definition, record)
    stale_expiry = %{entry | expire_at_ms: 999}

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), [stale_expiry]),
               record_read: fn _path, _keys, _now_ms, _max_bytes -> {:ok, [record]} end,
               now_ms: 1_000
             )

    assert Enum.map(result.records, & &1.id) == ["run-1"]
    assert result.usage.hydrated_records == 1
  end

  test "serves a live expiring covering row without authoritative hydration" do
    definition = covering_state_definition()

    request =
      request([eq(:state, "failed")], 1)
      |> Map.put(:projection, [:run_id, :state])

    plan = plan!(request, definition)
    live = record("run-1", 100)
    [entry] = entries(definition, live)
    projected_expiry = %{entry | expire_at_ms: 1_001}

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), [projected_expiry]),
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("a live covering row must not read the authoritative log")
               end,
               now_ms: 1_000
             )

    assert result.records == [%{id: "run-1", state: "failed"}]
    assert result.usage.hydrated_records == 0
  end

  test "skips an expired covering row without authoritative hydration" do
    definition = covering_state_definition()

    request =
      request([eq(:state, "failed")], 1)
      |> Map.put(:projection, [:run_id, :state])

    plan = plan!(request, definition)
    [entry] = entries(definition, record("run-1", 100))
    expired = %{entry | expire_at_ms: 1_000}

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), [expired]),
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("an expired covering row must not read the authoritative log")
               end,
               now_ms: 1_000
             )

    assert result.records == []
    assert result.usage.hydrated_records == 0
  end

  test "skips a missing row only when its projected entry is expired" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    [entry] = entries(definition, record("run-1", 100))
    expired = %{entry | expire_at_ms: 999}

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), [expired]),
               record_read: fn _path, _keys, _now_ms, _max_bytes -> {:ok, [nil]} end,
               now_ms: 1_000
             )

    assert result.records == []
    assert result.usage.hydrated_records == 1
  end

  test "does not hide a malformed hydrated value behind an expired projected entry" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    [entry] = entries(definition, record("run-1", 100))
    expired = %{entry | expire_at_ms: 999}

    assert {:error, :query_projection_changed} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), [expired]),
               record_read: fn _path, _keys, _now_ms, _max_bytes -> {:ok, [:malformed]} end,
               now_ms: 1_000
             )
  end

  test "checks the wall deadline between bounded pages" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2, [{:priority, :desc}])
    plan = plan!(request, definition)
    records = [record("a", 100), record("b", 90), record("c", 80)]
    {range_read, record_read} = storage(definition, records)
    counter = :counters.new(1, [])

    clock = fn ->
      current = :counters.get(counter, 1)
      :counters.add(counter, 1, 400_000)
      current
    end

    assert {:error, :query_deadline_exceeded} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               page_entries: 1,
               clock_us: clock,
               now_ms: 1_000
             )
  end

  test "checks the wall deadline after a query-row read before validating its rows" do
    definition = state_definition()

    request =
      request([eq(:state, "failed"), eq(:type, "invoice")], 1)
      |> Map.put(:projection, [:run_id, :state])

    record = record("run-query-row-deadline", 100)
    {range_read, _record_read} = storage(definition, [record])
    row = query_row(record)
    malformed = %{row | state_key: state_key(record("other", 100))}
    clock_calls = :counters.new(1, [])

    clock = fn ->
      call = :counters.get(clock_calls, 1)
      :counters.add(clock_calls, 1, 1)
      if call < 4, do: 0, else: 800_000
    end

    assert {:error, :query_deadline_exceeded} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               query_row_read: fn _path, [_state_key], _now_ms, _max_bytes ->
                 {:ok, [malformed], MemoryBudget.term_bytes([malformed]), true}
               end,
               record_read: fn _path, _state_keys, _now_ms, _max_bytes ->
                 flunk("a sparse projection must not hydrate the authoritative record")
               end,
               clock_us: clock,
               now_ms: 1_000
             )
  end

  test "samples the wall deadline during a large covering page" do
    definition = covering_state_definition()
    request = count_request([eq(:state, "failed")])
    plan = plan!(request, definition)

    page_entries =
      1..64
      |> Enum.map(&record("run-cover-deadline-#{&1}", &1))
      |> Enum.flat_map(&entries(definition, &1))
      |> Enum.sort_by(& &1.storage_key)

    last = List.last(page_entries)
    malformed_last = %{last | covering_record: Map.delete(last.covering_record, :partition_key)}
    page_entries = List.replace_at(page_entries, -1, malformed_last)
    clock_calls = :counters.new(1, [])

    clock = fn ->
      call = :counters.get(clock_calls, 1)
      :counters.add(clock_calls, 1, 1)
      if call < 4, do: 0, else: 800_000
    end

    assert {:error, :query_deadline_exceeded} =
             Executor.execute(context(), 0, request, plan,
               range_read: one_page_reader(hd(plan.ranges), page_entries),
               record_read: fn _path, _state_keys, _now_ms, _max_bytes ->
                 flunk("a covering count must not hydrate authoritative records")
               end,
               page_entries: 64,
               clock_us: clock,
               now_ms: 1_000
             )
  end

  test "bounds authoritative reads by the remaining query deadline" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 1)
    plan = plan!(request, definition)
    record = record("run-1", 100)
    {range_read, _record_read} = storage(definition, [record])

    record_read = fn _path, _state_keys, _now_ms, _max_bytes, timeout_ms ->
      assert timeout_ms in 1..124
      {:ok, [record]}
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               clock_us: fn -> 0 end,
               deadline_us: 123_001,
               now_ms: 1_000
             )

    assert Enum.map(result.records, & &1.id) == [record.id]
  end

  test "does not start authoritative IO after the query deadline" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 1)
    plan = plan!(request, definition)
    {range_read, _record_read} = storage(definition, [record("run-1", 100)])
    counter = :counters.new(1, [])
    parent = self()

    clock = fn ->
      call = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      if call < 2, do: 0, else: 800_000
    end

    assert {:error, :query_deadline_exceeded} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: fn _path, _state_keys, _now_ms, _max_bytes, _timeout_ms ->
                 send(parent, :record_read)
                 {:ok, []}
               end,
               clock_us: clock,
               now_ms: 1_000
             )

    refute_receive :record_read
  end

  test "reports a deadline that expires during final response assembly" do
    definition = state_definition()

    request =
      request(
        [
          eq(:state, "failed"),
          {:time_window, :updated_at_ms, integer(100), integer(100)}
        ],
        2
      )

    plan = plan!(request, definition)
    counter = :counters.new(1, [])

    clock = fn ->
      call = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)

      case call do
        0 -> 0
        1 -> 100
        _final -> 800_000
      end
    end

    assert {:error, :query_deadline_exceeded} =
             Executor.execute(context(), 0, request, plan,
               clock_us: clock,
               now_ms: 1_000
             )
  end

  test "defers exact response-size enforcement to the response boundary" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    {range_read, record_read} = storage(definition, [record("run-1", 100)])

    assert {:ok, unrestricted} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               clock_us: fn -> 0 end,
               now_ms: 1_000
             )

    assert {:ok, response} =
             Response.build(
               unrestricted.records,
               unrestricted.has_more,
               unrestricted.continuation,
               unrestricted.quality,
               unrestricted.usage,
               plan.budget
             )

    exact_bytes = response.usage.response_bytes
    {:ok, budget} = Budget.lower(plan.budget, response_bytes: exact_bytes)
    plan = %{plan | budget: budget}

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               clock_us: fn -> 0 end,
               now_ms: 1_000
             )

    assert {:ok, %{usage: %{response_bytes: ^exact_bytes}}} =
             Response.build(
               result.records,
               result.has_more,
               result.continuation,
               result.quality,
               result.usage,
               budget
             )
  end

  test "accounts for the range page while hydrated rows are live" do
    definition = state_definition()
    request = request([eq(:state, "failed"), eq(:type, "invoice")], 2)
    plan = plan!(request, definition)
    {:ok, budget} = Budget.lower(plan.budget, executor_memory_bytes: 600)
    plan = %{plan | budget: budget}

    # The row is hydrated and then rejected by the residual type predicate. This
    # isolates the page-plus-hydration peak from result selection memory.
    records = [record("run-1", 100, "failed", "other")]
    {range_read, record_read} = storage(definition, records)

    assert {:error, :query_memory_budget_exceeded} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: record_read,
               now_ms: 1_000
             )
  end

  test "rejects a decoded covering page that exceeds the executor memory budget" do
    definition = covering_state_definition()
    request = count_request([eq(:state, "failed")])
    plan = plan!(request, definition)
    matching = record("run-cover-memory", 100)
    [entry] = entries(definition, matching)
    legacy_page_bytes = entry.storage_bytes + 192

    assert MemoryBudget.term_bytes([entry]) > legacy_page_bytes
    assert {:ok, budget} = Budget.lower(plan.budget, executor_memory_bytes: legacy_page_bytes)

    {range_read, _record_read} = storage(definition, [matching])

    assert {:error, :query_memory_budget_exceeded} =
             Executor.execute(context(), 0, request, %{plan | budget: budget},
               range_read: range_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("an authoritative covering count must not hydrate")
               end,
               page_entries: 1,
               now_ms: 1_000
             )
  end

  test "rejects decoded record heap that only fits under external-term accounting" do
    definition = state_definition()
    request = count_request([eq(:state, "failed")])
    plan = plan!(request, definition)

    state_meta =
      for state <- 1..16, into: %{} do
        entries = for entry <- 1..16, into: %{}, do: {"k#{entry}", entry}
        {"s#{state}", entries}
      end

    heavy = record("run-heavy", 100) |> Map.put(:state_meta, state_meta)
    [entry] = entries(definition, heavy)

    external_working_set =
      entry.storage_bytes + 192 + :erlang.external_size(heavy, minor_version: 2) + 128

    assert MemoryBudget.term_bytes(heavy) > :erlang.external_size(heavy, minor_version: 2)
    {:ok, budget} = Budget.lower(plan.budget, executor_memory_bytes: external_working_set)
    {range_read, record_read} = storage(definition, [heavy])

    assert {:error, :query_memory_budget_exceeded} =
             Executor.execute(context(), 0, request, %{plan | budget: budget},
               range_read: range_read,
               record_read: record_read,
               page_entries: 1,
               now_ms: 1_000
             )
  end

  test "splits an over-budget hydration batch before materializing its records" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2, [{:priority, :desc}])
    plan = plan!(request, definition)
    records = [record("run-a", 100), record("run-b", 90)]
    {range_read, _record_read} = storage(definition, records)
    by_state_key = Map.new(records, &{state_key(&1), &1})
    parent = self()

    bounded_record_read = fn _path, state_keys, _now_ms, max_bytes ->
      send(parent, {:hydrate_batch, length(state_keys), max_bytes})

      if length(state_keys) > 1,
        do: {:error, :query_hydration_batch_too_large},
        else: {:ok, Enum.map(state_keys, &Map.fetch!(by_state_key, &1))}
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: bounded_record_read,
               now_ms: 1_000
             )

    assert Enum.map(result.records, & &1.id) == ["run-b", "run-a"]
    assert_receive {:hydrate_batch, 2, available_bytes}
    assert available_bytes > 0
    assert_receive {:hydrate_batch, 1, _bytes}
    assert_receive {:hydrate_batch, 1, _bytes}
  end

  test "resumes a bounded hydration prefix without rereading returned state keys" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 4, [{:priority, :desc}])
    plan = plan!(request, definition)

    records = [
      record("run-a", 100),
      record("run-b", 90),
      record("run-c", 80),
      record("run-d", 70)
    ]

    {range_read, _record_read} = storage(definition, records)
    by_state_key = Map.new(records, &{state_key(&1), &1})
    parent = self()

    prefix_record_read = fn _path, state_keys, _now_ms, max_bytes ->
      send(parent, {:hydrate_prefix, state_keys, max_bytes})
      returned_keys = Enum.take(state_keys, 2)

      {:ok, Enum.map(returned_keys, &Map.fetch!(by_state_key, &1)), length(state_keys) <= 2}
    end

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: prefix_record_read,
               now_ms: 1_000
             )

    assert {:ok, expected_records} =
             Ferricstore.Flow.Query.RecordOrder.sort(records, [{:priority, :desc}])

    assert result.records == expected_records
    assert result.usage.hydrated_records == 4

    assert_receive {:hydrate_prefix, first_keys, first_bytes}
    assert length(first_keys) == 4
    assert first_bytes > 0

    assert_receive {:hydrate_prefix, second_keys, second_bytes}
    assert second_keys == Enum.drop(first_keys, 2)
    assert second_bytes > 0
    refute_receive {:hydrate_prefix, _keys, _bytes}
  end

  test "rejects a plan whose defensive rechecks do not exactly match the request" do
    definition = state_definition()
    request = request([eq(:state, "failed"), eq(:type, "invoice")], 2)
    plan = plan!(request, definition)
    tampered = %{plan | recheck_predicates: [eq(:partition_key, "tenant")]}
    parent = self()

    range_read = fn _path, _range, _cursor, _max_entries, _max_bytes ->
      send(parent, :range_read)
      {:error, :unexpected}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, tampered,
               range_read: range_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes -> {:ok, []} end,
               now_ms: 1_000
             )

    refute_receive :range_read
  end

  test "rejects incompatible plan versions and request fingerprints before storage" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)

    tampered_plans = [
      %{plan | version: 2},
      %{plan | query_fingerprint: String.duplicate("0", 64)},
      %{plan | record_source: :authoritative_log}
    ]

    for tampered <- tampered_plans do
      assert {:error, :query_storage_inconsistent} =
               Executor.execute(context(), 0, request, tampered,
                 range_read: fn _path, _range, _cursor, _max_entries, _max_bytes ->
                   flunk("incompatible plan reached storage")
                 end,
                 record_read: fn _path, _keys, _now_ms, _max_bytes -> {:ok, []} end,
                 now_ms: 1_000
               )
    end
  end

  test "rejects a forged index definition and matching forged range before storage" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    <<first, rest::binary>> = definition.fingerprint
    forged = %{definition | fingerprint: <<Bitwise.bxor(first, 1), rest::binary>>}
    [range] = plan.ranges
    forged_range = %{range | prefix: IndexDefinition.storage_prefix(forged)}
    tampered = %{plan | definition: forged, ranges: [forged_range]}
    parent = self()

    range_read = fn _path, _range, _cursor, _max_entries, _max_bytes ->
      send(parent, :range_read)
      {:error, :unexpected}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, tampered,
               range_read: range_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes -> {:ok, []} end,
               now_ms: 1_000
             )

    refute_receive :range_read
  end

  test "rejects an empty plan for a non-empty request" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    tampered = %{plan | path: :empty, ranges: []}

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, tampered,
               range_read: fn _path, _range, _cursor, _max_entries, _max_bytes ->
                 flunk("invalid empty plan reached storage")
               end,
               record_read: fn _path, _keys, _now_ms, _max_bytes -> {:ok, []} end,
               now_ms: 1_000
             )
  end

  test "rejects malformed composite range fields before storage" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    [range] = plan.ranges
    tampered = %{plan | ranges: [%{range | prefix: :not_a_binary}]}

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, tampered,
               range_read: fn _path, _range, _cursor, _max_entries, _max_bytes ->
                 flunk("malformed range reached storage")
               end,
               record_read: fn _path, _keys, _now_ms, _max_bytes -> {:ok, []} end,
               now_ms: 1_000
             )
  end

  test "rejects a tenant-contained range widened beyond the request constraints" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    [range] = plan.ranges
    assert {:ok, tenant_prefix} = CompositeIndex.encode_prefix(definition, ["tenant"])

    widened = %{range | prefix: tenant_prefix, after_key: "", before_key: ""}
    tampered = %{plan | ranges: [widened]}

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, tampered,
               range_read: fn _path, _range, _cursor, _max_entries, _max_bytes ->
                 flunk("widened plan reached storage")
               end,
               record_read: fn _path, _keys, _now_ms, _max_bytes -> {:ok, []} end,
               now_ms: 1_000
             )
  end

  test "rejects removed plan order states before storage" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = %{plan!(request, definition) | order: :merged}
    parent = self()

    range_read = fn _path, _range, _cursor, _max_entries, _max_bytes ->
      send(parent, :range_read)
      {:error, :unexpected}
    end

    assert {:error, :query_storage_inconsistent} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: fn _path, _keys, _now_ms, _max_bytes -> {:ok, []} end,
               now_ms: 1_000
             )

    refute_receive :range_read
  end

  test "rejects an unbounded hydration callback before storage" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2)
    plan = plan!(request, definition)
    parent = self()

    range_read = fn _path, _range, _cursor, _max_entries, _max_bytes ->
      send(parent, :range_read)
      {:error, :unexpected}
    end

    assert {:error, :query_engine_failure} =
             Executor.execute(context(), 0, request, plan,
               range_read: range_read,
               record_read: fn _path, _keys, _now_ms -> {:ok, []} end,
               now_ms: 1_000
             )

    refute_receive :range_read
  end

  test "generated planned executions agree with the independent reference evaluator" do
    :rand.seed(:exsss, {91, 27, 413})
    definition = state_definition()

    for iteration <- 1..200 do
      records =
        for value <- 1..30 do
          state = Enum.at(["failed", "running", "completed"], rem(value + iteration, 3))
          type = if rem(value * iteration, 4) == 0, do: "invoice", else: "other"

          record(
            "run-#{iteration}-#{value}",
            :rand.uniform(12) * 10,
            state,
            type
          )
        end

      state = Enum.random(["failed", "running", "completed"])
      lower = Enum.random(0..8) * 10
      upper = lower + Enum.random(0..5) * 10
      limit = Enum.random(1..10)

      predicates =
        [eq(:state, state), {:time_window, :updated_at_ms, integer(lower), integer(upper)}]
        |> maybe_add_type_predicate(iteration)

      order_by =
        if rem(iteration, 2) == 0,
          do: [{:updated_at_ms, :desc}],
          else: [{:priority, :desc}]

      request = request(predicates, limit, order_by)
      plan = plan!(request, definition)
      {range_read, record_read} = storage(definition, records)

      assert {:ok, expected} = ReferenceEvaluator.execute(records, request)

      assert {:ok, actual} =
               Executor.execute(context(), 0, request, plan,
                 range_read: range_read,
                 record_read: record_read,
                 page_entries: 7,
                 cursor_key: @cursor_key,
                 now_ms: 1_000
               )

      assert Enum.map(actual.records, & &1.id) == Enum.map(expected, & &1.id),
             "planner/executor mismatch at generated case #{iteration}"
    end
  end

  test "reads real composite entries from LMDB and authoritative Flow rows from WARaft" do
    definition = state_definition()
    request = request([eq(:state, "failed")], 2, [{:updated_at_ms, :desc}])
    plan = plan!(request, definition)
    records = [record("older", 100), record("newer", 200), record("other", 300, "running")]
    root = lmdb_path()
    path = Path.join(root, "query-lmdb")

    source_values =
      Enum.map(records, fn record ->
        {state_key(record), Codec.encode_record(record), 0}
      end)

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
               root,
               0,
               41,
               source_values
             )

    assert {:ok, 3} =
             Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(root, 0)

    assert {:ok, {segment_generation, frame_offset, frame_size}} =
             Ferricstore.Raft.WARaftSegmentReader.physical_location(
               %{data_dir: root},
               0,
               {:waraft_apply_projection, 41}
             )

    on_exit(fn ->
      Ferricstore.Raft.WARaftSegmentReader.clear_apply_projection_cache(root, 0)
    end)

    ops =
      Enum.flat_map(records, fn record ->
        state_key = state_key(record)
        encoded_record = Codec.encode_record(record)

        locator =
          Locator.new!(
            flow_id: record.id,
            kind: :state,
            version: record.version,
            raft_index: 41,
            file_id: {:waraft_apply_projection, 41},
            offset: frame_offset,
            value_size: byte_size(encoded_record),
            checksum: :crypto.hash(:sha256, encoded_record),
            segment_generation: segment_generation,
            frame_size: frame_size
          )

        assert {:ok, query_row} = QueryRowCodec.encode(state_key, record, locator, 0)
        state_op = {:put, state_key, query_row}

        index_ops =
          definition
          |> entries_with_values(record)
          |> Enum.map(&{:put, &1.key, &1.value})

        [state_op | index_ops]
      end)

    assert :ok = LMDB.write_batch(path, ops)

    assert {:ok, result} =
             Executor.execute(%{context() | data_dir: root}, 0, request, plan,
               path: path,
               page_entries: 3,
               now_ms: 1_000
             )

    assert Enum.map(result.records, & &1.id) == ["newer", "older"]
    assert result.usage.range_pages == 1
  end

  test "serves real covering composite entries from LMDB without primary rows" do
    definition = covering_state_definition()

    request =
      request([eq(:state, "failed")], 2, [{:updated_at_ms, :desc}])
      |> Map.put(:projection, [:run_id, :state, :updated_at_ms])

    records = [record("older", 100), record("newer", 200)]
    path = lmdb_path()

    ops =
      Enum.flat_map(records, fn record ->
        definition
        |> entries_with_values(record)
        |> Enum.map(&{:put, &1.key, &1.value})
      end)

    assert :ok = LMDB.write_batch(path, ops)

    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               path: path,
               page_entries: 3,
               now_ms: 1_000
             )

    assert result.records == [
             %{id: "newer", state: "failed", updated_at_ms: 200},
             %{id: "older", state: "failed", updated_at_ms: 100}
           ]

    assert result.usage.hydrated_records == 0
  end

  defp plan!(request, definition, mandatory_scope \\ MandatoryScope.dedicated()) do
    index =
      RegisteredIndex.new!(definition, :active,
        coverage: %{complete_shards: 1, total_shards: 1, validation: :passed}
      )

    assert {:ok, plan} =
             Planner.plan(request, [index],
               mandatory_scope: mandatory_scope,
               now_ms: 1_000
             )

    plan
  end

  defp request(predicates, limit, order_by \\ [{:updated_at_ms, :desc}]) do
    Request.collection(
      :execute,
      [eq(:partition_key, "tenant") | predicates],
      order_by,
      limit,
      :record
    )
  end

  defp count_request(predicates) do
    Request.count(:execute, [eq(:partition_key, "tenant") | predicates])
  end

  defp with_cursor(request, token), do: %{request | cursor: {:literal, :keyword, token}}

  defp query_digest(request), do: Planner.query_digest(request)

  defp collect_pages(request, definition, range_read, record_read, ids, page)
       when page < 10 do
    assert {:ok, result} =
             Executor.execute(context(), 0, request, plan!(request, definition),
               range_read: range_read,
               record_read: record_read,
               cursor_key: @cursor_key,
               now_ms: 1_000 + page
             )

    ids = ids ++ Enum.map(result.records, & &1.id)

    if result.has_more do
      assert is_binary(result.continuation)

      collect_pages(
        with_cursor(request, result.continuation),
        definition,
        range_read,
        record_read,
        ids,
        page + 1
      )
    else
      {ids, result}
    end
  end

  defp context(scope \\ MandatoryScope.dedicated()) do
    {:ok, metadata_snapshot} =
      case scope.mode do
        :dedicated -> MetadataExtension.configure(FerricStore.Flow.MetadataExtension.Disabled, [])
        :shared -> MetadataExtension.configure(SharedScopeProvider, [])
      end

    %{
      name: :test,
      data_dir: "/not-used",
      shard_count: 1,
      slot_map: List.to_tuple(List.duplicate(0, 1_024)),
      flow_metadata_snapshot: metadata_snapshot,
      query_mandatory_scope: scope
    }
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

  defp covering_state_definition(extra_fields \\ []) do
    IndexDefinition.new!(%{
      id: "flow_runs_tenant_state_updated_covering",
      version: 1,
      covering_fields: [:partition_key, :run_id, :state, :updated_at_ms, :version | extra_fields],
      fields: [
        {:partition_key, :asc},
        {:state, :asc},
        {:updated_at_ms, :desc}
      ]
    })
  end

  defp counted_state_definition do
    IndexDefinition.new!(%{
      id: "flow_runs_tenant_state_updated_counted",
      version: 1,
      count_prefixes: [2],
      fields: [
        {:partition_key, :asc},
        {:state, :asc},
        {:updated_at_ms, :desc}
      ]
    })
  end

  defp shared_state_definition do
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
  end

  defp tag_definition do
    IndexDefinition.new!(%{
      id: "flow_runs_tenant_tag_updated",
      version: 1,
      fields: [
        {:partition_key, :asc},
        {{:attribute, "tags"}, :asc},
        {:updated_at_ms, :desc}
      ]
    })
  end

  defp record(id, updated_at_ms, state \\ "failed", type \\ "invoice") do
    %{
      id: id,
      type: type,
      state: state,
      version: 1,
      priority: 0,
      partition_key: "tenant",
      created_at_ms: updated_at_ms - 10,
      updated_at_ms: updated_at_ms,
      attempts: 0
    }
  end

  defp storage(definition, records) do
    all_entries =
      records |> Enum.flat_map(&entries(definition, &1)) |> Enum.sort_by(& &1.storage_key)

    by_state_key = Map.new(records, &{state_key(&1), &1})

    range_read = fn _path, range, cursor, max_entries, max_bytes ->
      lower = if is_binary(cursor), do: cursor, else: range.after_key

      eligible =
        Enum.filter(all_entries, fn entry ->
          String.starts_with?(entry.storage_key, range.prefix) and
            (lower == "" or entry.storage_key > lower) and
            (range.before_key == "" or entry.storage_key < range.before_key)
        end)

      page = Enum.take(eligible, max_entries)
      scanned_bytes = Enum.sum(Enum.map(page, & &1.storage_bytes))

      if scanned_bytes > max_bytes do
        {:error, :range_entry_too_large}
      else
        exhausted = length(page) == length(eligible)

        {:ok,
         %{
           entries: page,
           cursor: if(exhausted, do: nil, else: List.last(page).storage_key),
           exhausted: exhausted,
           scanned_entries: length(page),
           scanned_bytes: scanned_bytes
         }}
      end
    end

    record_read = fn _path, state_keys, _now_ms, _max_bytes ->
      {:ok, Enum.map(state_keys, &Map.get(by_state_key, &1))}
    end

    {range_read, record_read}
  end

  defp one_page_reader(expected_range, entries) do
    fn _path, range, nil, _max_entries, _max_bytes ->
      assert range == expected_range

      {:ok,
       %{
         entries: entries,
         cursor: nil,
         exhausted: true,
         scanned_entries: length(entries),
         scanned_bytes: Enum.sum(Enum.map(entries, & &1.storage_bytes))
       }}
    end
  end

  defp entries(definition, record) do
    projected = entries_with_values(definition, record)

    Enum.map(projected, fn entry ->
      assert {:ok, decoded} = CompositeIndex.decode_entry_value(entry.value)

      decoded
      |> Map.put(:storage_key, entry.key)
      |> Map.put(:storage_bytes, byte_size(entry.key) + byte_size(entry.value))
    end)
  end

  defp entries_with_values(definition, record) do
    assert {:ok, projected} = CompositeIndex.entries(definition, record, state_key(record), 0)
    projected
  end

  defp state_key(record), do: Keys.state_key(record.id, record.partition_key)

  defp query_row(record) do
    %QueryRow{
      state_key: state_key(record),
      record: FlowRecordProjection.public(record),
      locator:
        Locator.new!(
          flow_id: record.id,
          kind: :state,
          version: record.version,
          raft_index: record.version,
          file_id: {:waraft_apply_projection, record.version},
          offset: 0,
          value_size: byte_size(Codec.encode_record(record)),
          checksum: :crypto.hash(:sha256, Codec.encode_record(record)),
          segment_generation: 0,
          frame_size: 1_024
        ),
      expire_at_ms: 0
    }
  end

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

  defp scoped_record(record, scope) do
    {:ok, metadata} = MandatoryScope.single_metadata(scope)

    {:ok, physical_partition} =
      MandatoryScope.physical_partition_key(scope, record.partition_key)

    record
    |> Map.put(:partition_key, physical_partition)
    |> Ferricstore.Flow.SystemMetadata.put_record(metadata)
  end

  defp dedicated_query_binding(partition) do
    {:ok, binding} = MandatoryScope.query_binding(MandatoryScope.dedicated(), partition)
    binding
  end

  defp eq(field, value), do: {:eq, field, keyword(value)}
  defp keyword(value), do: {:literal, :keyword, value}
  defp integer(value), do: {:literal, :integer, value}

  defp maybe_add_type_predicate(predicates, iteration) do
    if rem(iteration, 3) == 0,
      do: predicates ++ [eq(:type, "invoice")],
      else: predicates
  end

  defp receive_hydrated(acc) do
    receive do
      {:hydrate, keys} -> receive_hydrated([keys | acc])
    after
      20 -> Enum.reverse(acc)
    end
  end

  defp execute_with_call_trace({module, function, arity} = mfa, execute_fun) do
    parent = self()
    reference = make_ref()

    executor =
      spawn(fn ->
        receive do
          :execute ->
            send(parent, {reference, execute_fun.()})
            receive do: (:stop -> :ok)
        end
      end)

    Code.ensure_loaded!(module)
    :erlang.trace(executor, true, [:call, {:tracer, self()}])
    :erlang.trace_pattern(mfa, true, [:local])

    try do
      send(executor, :execute)
      assert_receive {^reference, result}, 5_000
      await_trace_delivery(executor)
      {result, traced_calls(module, function, arity)}
    after
      :erlang.trace_pattern(mfa, false, [:local])
      :erlang.trace(executor, false, [:call])
      send(executor, :stop)
    end
  end

  defp traced_calls(module, function, arity, count \\ 0) do
    receive do
      {:trace, _pid, :call, {^module, ^function, arguments}} when length(arguments) == arity ->
        traced_calls(module, function, arity, count + 1)
    after
      0 -> count
    end
  end

  defp await_trace_delivery(pid) do
    reference = :erlang.trace_delivered(pid)

    receive do
      {:trace_delivered, _pid, ^reference} -> :ok
    end
  end

  defp lmdb_path do
    suffix = System.unique_integer([:positive, :monotonic])
    path = Path.join(System.tmp_dir!(), "ferricstore_query_executor_#{suffix}")
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end
end
