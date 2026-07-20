defmodule Ferricstore.Flow.Query.EngineTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, Query}
  alias Ferricstore.Flow.Query.{Error, Limits}
  alias Ferricstore.Flow.Query.{MandatoryScope, Request}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  defmodule RejectingEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(ctx, request) do
      send(self(), {:query_engine_called, ctx, request})
      {:error, :unauthorized_scope}
    end
  end

  defmodule AlternateEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: {:ok, :alternate_engine}
  end

  defmodule RaisingEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: raise("provider-secret")
  end

  defmodule ThrowingEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: throw({:provider_secret, "do-not-expose"})
  end

  defmodule InvalidResultEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: :invalid_result
  end

  defmodule UnknownErrorEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: {:error, :provider_private_error}
  end

  defmodule CapabilityEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: {:ok, :capability_engine}

    @impl true
    def capabilities do
      %{
        query_contract: "test.flow.query/v1",
        explain_contract: "test.flow.explain/v1",
        capabilities: ["test_query_v1"],
        language_versions: ["FQL1"],
        shapes: ["runs_by_partition_and_run_id_record"]
      }
    end
  end

  defmodule InvalidCapabilitiesEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: {:ok, nil}

    @impl true
    def capabilities, do: %{capabilities: [:not_a_wire_string]}
  end

  defmodule CollectionCapabilityEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: {:ok, nil}

    @impl true
    def capabilities do
      %{
        query_contract: "ferric.flow.query/v1",
        explain_contract: "ferric.flow.explain/v1",
        capabilities: ["flow_query_v1", "flow_composite_index_v1"],
        language_versions: ["FQL1"],
        shapes: [
          "runs_by_partition_and_run_id_record",
          "runs_by_partition_predicates_ordered_records"
        ]
      }
    end
  end

  defmodule UnsupportedParserCapabilitiesEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: {:ok, nil}

    @impl true
    def capabilities do
      %{
        query_contract: "future.query/v1",
        explain_contract: nil,
        capabilities: ["future_query_v1"],
        language_versions: ["FQL2"],
        shapes: ["collection_scan"]
      }
    end
  end

  defmodule IncoherentCapabilitiesEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: {:ok, nil}

    @impl true
    def capabilities do
      %{
        query_contract: nil,
        explain_contract: "detached.explain/v1",
        capabilities: ["detached_explain_v1"],
        language_versions: ["FQL1"],
        shapes: ["runs_by_partition_and_run_id_record"]
      }
    end
  end

  setup do
    previous = Application.get_env(:ferricstore, FerricStore.Flow.QueryEngine)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, FerricStore.Flow.QueryEngine)
        value -> Application.put_env(:ferricstore, FerricStore.Flow.QueryEngine, value)
      end
    end)

    :ok
  end

  test "EXPLAIN rejects a canonical request without a supported physical path" do
    request =
      Request.collection(
        :explain,
        [{:eq, :partition_key, {:literal, :keyword, "tenant-a"}}],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert {:error, :unsupported_query_shape} = Query.execute(%{}, request)
  end

  test "malformed canonical envelopes never reach an installed query engine" do
    request =
      Request.point_read(
        :execute,
        {:literal, :keyword, "tenant-a"},
        {:literal, :keyword, "run-123"}
      )

    ctx = %{query_engine: RejectingEngine}

    for {malformed, expected_error} <- [
          {%{request | source: :events}, :unsupported_query_shape},
          {%{request | mode: :invalid}, :unsupported_query_shape},
          {%{request | order_by: [{:run_id, :asc}]}, :unsupported_query_shape},
          {%{request | limit: 2}, :unsupported_query_shape},
          {%{request | return: :payload}, :unsupported_query_shape}
        ] do
      assert {:error, ^expected_error} = Query.execute(ctx, malformed)
    end

    refute_received {:query_engine_called, _ctx, _request}
  end

  test "EXPLAIN rejects an unsupported canonical request version" do
    request =
      Request.point_read(
        :explain,
        {:literal, :keyword, "tenant-a"},
        {:literal, :keyword, "run-123"}
      )

    assert {:error, :unsupported_query_version} = Query.execute(%{}, %{request | version: 2})
  end

  test "the engine rejects oversized bound point values before storage" do
    request =
      Request.point_read(
        :execute,
        {:literal, :keyword, String.duplicate("p", Router.max_key_size() + 1)},
        {:literal, :keyword, "run-123"}
      )

    assert {:error, :query_value_too_large} = Query.execute(%{}, request)

    request =
      Request.point_read(
        :execute,
        {:literal, :keyword, "tenant-a"},
        {:literal, :keyword, String.duplicate("r", Router.max_key_size())}
      )

    assert {:error, :query_value_too_large} = Query.execute(%{}, request)
  end

  test "canonical validation rejects oversized values before any installed engine" do
    oversized_partition =
      Request.point_read(
        :execute,
        {:literal, :keyword, String.duplicate("p", Limits.max_partition_key_bytes() + 1)},
        {:literal, :keyword, "run-123"}
      )

    oversized_run =
      Request.point_read(
        :execute,
        {:literal, :keyword, "tenant-a"},
        {:literal, :keyword, String.duplicate("r", Limits.max_run_id_bytes() + 1)}
      )

    ctx = %{query_engine: RejectingEngine}

    assert {:error, :query_value_too_large} = Query.execute(ctx, oversized_partition)
    assert {:error, :query_value_too_large} = Query.execute(ctx, oversized_run)
    refute_received {:query_engine_called, _ctx, _request}
  end

  test "dispatches a validated canonical request through the installed query engine" do
    request =
      Request.point_read(
        :execute,
        {:literal, :keyword, "tenant-a"},
        {:literal, :keyword, "run-123"}
      )

    ctx = %{
      query_engine: RejectingEngine,
      request_context: %{"tenant" => "tenant-a"}
    }

    assert {:error, :unauthorized_scope} = Query.execute(ctx, request)
    assert_received {:query_engine_called, ^ctx, ^request}
  end

  test "an instance freezes its query engine when it is built" do
    name = :"query_engine_freeze_#{System.unique_integer([:positive, :monotonic])}"
    Application.put_env(:ferricstore, FerricStore.Flow.QueryEngine, RejectingEngine)

    ctx =
      FerricStore.Instance.build(name,
        shard_count: 1,
        data_dir: Path.join(System.tmp_dir!(), Atom.to_string(name))
      )

    on_exit(fn -> FerricStore.Instance.cleanup(name) end)

    Application.put_env(:ferricstore, FerricStore.Flow.QueryEngine, AlternateEngine)

    request =
      Request.point_read(
        :execute,
        {:literal, :keyword, "tenant-a"},
        {:literal, :keyword, "run-123"}
      )

    assert ctx.query_engine == RejectingEngine
    assert {:error, :unauthorized_scope} = Query.execute(ctx, request)
    assert_received {:query_engine_called, ^ctx, ^request}
  end

  test "explicit per-instance query engines are isolated from application configuration" do
    name = :"query_engine_override_#{System.unique_integer([:positive, :monotonic])}"
    Application.put_env(:ferricstore, FerricStore.Flow.QueryEngine, RejectingEngine)

    ctx =
      FerricStore.Instance.build(name,
        shard_count: 1,
        data_dir: Path.join(System.tmp_dir!(), Atom.to_string(name)),
        query_engine: AlternateEngine
      )

    on_exit(fn -> FerricStore.Instance.cleanup(name) end)

    request =
      Request.point_read(
        :explain,
        {:literal, :keyword, "tenant-a"},
        {:literal, :keyword, "run-123"}
      )

    assert ctx.query_engine == AlternateEngine
    assert {:ok, :alternate_engine} = Query.execute(ctx, request)
  end

  test "an instance freezes and exposes its validated query capability manifest" do
    name = :"query_capabilities_#{System.unique_integer([:positive, :monotonic])}"

    ctx =
      FerricStore.Instance.build(name,
        shard_count: 1,
        data_dir: Path.join(System.tmp_dir!(), Atom.to_string(name)),
        query_engine: CapabilityEngine
      )

    on_exit(fn -> FerricStore.Instance.cleanup(name) end)

    assert ctx.query_capabilities == CapabilityEngine.capabilities()
    assert FerricStore.Flow.QueryEngine.capabilities(ctx) == CapabilityEngine.capabilities()
  end

  test "accepts the bounded collection shape produced by the canonical FQL1 parser" do
    assert FerricStore.Flow.QueryEngine.capabilities_for(CollectionCapabilityEngine) ==
             CollectionCapabilityEngine.capabilities()

    assert Ferricstore.Flow.Query.Surface.default_capability_manifest().shapes == [
             "runs_by_run_id_record",
             "runs_by_partition_and_run_id_record",
             "events_by_run_id_ordered_records"
           ]
  end

  test "capability support predicates fail closed for malformed provider values" do
    refute Ferricstore.Flow.Query.Surface.supported_language_versions?(:not_a_list)
    refute Ferricstore.Flow.Query.Surface.supported_shapes?(%{"shape" => true})
  end

  test "capability vocabulary distinguishes the specialized lineage shapes" do
    lineage_shapes = [
      "runs_by_partition_parent_ordered_records",
      "runs_by_partition_root_ordered_records",
      "runs_by_partition_correlation_ordered_records"
    ]

    assert Ferricstore.Flow.Query.Surface.supported_shapes?(lineage_shapes)

    default_shapes = Ferricstore.Flow.Query.Surface.default_capability_manifest().shapes
    refute Enum.any?(lineage_shapes, &(&1 in default_shapes))
  end

  test "instance construction rejects an invalid query engine" do
    name = :"invalid_query_engine_#{System.unique_integer([:positive, :monotonic])}"
    on_exit(fn -> FerricStore.Instance.cleanup(name) end)

    assert_raise ArgumentError, ~r/query_engine/, fn ->
      FerricStore.Instance.build(name,
        shard_count: 1,
        data_dir: Path.join(System.tmp_dir!(), Atom.to_string(name)),
        query_engine: String
      )
    end
  end

  test "instance construction rejects an invalid query capability manifest" do
    name = :"invalid_query_capabilities_#{System.unique_integer([:positive, :monotonic])}"
    on_exit(fn -> FerricStore.Instance.cleanup(name) end)

    assert_raise ArgumentError, ~r/query capability manifest/, fn ->
      FerricStore.Instance.build(name,
        shard_count: 1,
        data_dir: Path.join(System.tmp_dir!(), Atom.to_string(name)),
        query_engine: InvalidCapabilitiesEngine
      )
    end
  end

  test "instance construction rejects capabilities the canonical parser cannot produce" do
    name = :"unsupported_query_capabilities_#{System.unique_integer([:positive, :monotonic])}"
    on_exit(fn -> FerricStore.Instance.cleanup(name) end)

    assert_raise ArgumentError, ~r/query capability manifest/, fn ->
      FerricStore.Instance.build(name,
        shard_count: 1,
        data_dir: Path.join(System.tmp_dir!(), Atom.to_string(name)),
        query_engine: UnsupportedParserCapabilitiesEngine
      )
    end
  end

  test "capability negotiation rejects a surface without a query contract" do
    assert_raise ArgumentError, ~r/query capability manifest/, fn ->
      FerricStore.Flow.QueryEngine.capabilities_for(IncoherentCapabilitiesEngine)
    end
  end

  test "keeps scope failures structured, permissioned, and value-free" do
    assert Query.error_message(:unauthorized_scope) ==
             "NOPERM Flow query scope is not authorized"

    assert Error.status(:unauthorized_scope) == :noperm
  end

  test "query engine failures are fail-closed and never escape the boundary" do
    request =
      Request.point_read(
        :execute,
        {:literal, :keyword, "tenant-a"},
        {:literal, :keyword, "run-123"}
      )

    for implementation <- [
          RaisingEngine,
          ThrowingEngine,
          InvalidResultEngine,
          UnknownErrorEngine
        ] do
      assert {:error, :query_engine_failure} =
               FerricStore.Flow.QueryEngine.execute(
                 %{query_engine: implementation},
                 request
               )
    end

    assert Error.status(:query_engine_failure) == :error
    refute Error.message(:query_engine_failure) =~ "secret"
  end

  test "point-read storage outages remain structured and safe to retry" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    shard = elem(ctx.shard_names, 0)

    request =
      Request.point_read(
        :execute,
        {:literal, :keyword, "tenant-a"},
        {:literal, :keyword, "run-123"}
      )

    try do
      GenServer.stop(shard, :normal, 5_000)

      assert {:error, :query_storage_unavailable} = Query.execute(ctx, request)

      assert Error.payload(:query_storage_unavailable) == %{
               "code" => "query_storage_unavailable",
               "message" => "ERR Flow query storage is unavailable",
               "retryable" => true,
               "safe_to_retry" => true,
               "retry_after_ms" => 0
             }

      assert Error.status(:query_storage_unavailable) == :error
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "run-id-only point reads retain the direct auto-partition lookup" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    id = "query-auto-#{System.unique_integer([:positive, :monotonic])}"

    try do
      assert :ok = Ferricstore.Flow.create(ctx, id, type: "query-auto", now_ms: 1_000)

      request = %Request{
        mode: :execute,
        source: :runs,
        predicate: {:and, [{:eq, :run_id, {:literal, :keyword, id}}]},
        order_by: [],
        limit: 1,
        return: :record
      }

      assert {:ok, %{id: ^id, partition_key: partition}} = Query.execute(ctx, request)
      assert partition == Keys.auto_partition_key(id)
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "event queries retain the direct ordered history path" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    id = "query-history-#{System.unique_integer([:positive, :monotonic])}"

    try do
      assert :ok = Ferricstore.Flow.create(ctx, id, type: "query-history", now_ms: 1_000)

      request = %Request{
        mode: :execute,
        source: :events,
        predicate: {:and, [{:eq, :run_id, {:literal, :keyword, id}}]},
        order_by: [{:event_id, :asc}],
        limit: 10,
        return: :record
      }

      assert {:ok, [%{event_id: event_id, fields: %{"event" => "created"}}]} =
               Query.execute(ctx, request)

      assert is_binary(event_id)
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "history continuation performs bounded exclusive seeks without duplicates" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    id = "query-history-page-#{System.unique_integer([:positive, :monotonic])}"

    try do
      assert :ok = Ferricstore.Flow.create(ctx, id, type: "query-history", now_ms: 1_000)

      for signal <- ["one", "two", "three"] do
        assert :ok = Ferricstore.Flow.signal(ctx, id, signal: signal, now_ms: 1_000)
      end

      request = Request.history(:execute, [eq(:run_id, id)], :asc, 2)
      scope = MandatoryScope.dedicated()

      assert {:ok,
              %{
                records: first,
                has_more: true,
                continuation: continuation,
                scanned_entries: first_scanned,
                memory_high_water_bytes: first_memory
              }} =
               Ferricstore.Flow.Query.Engine.execute_history_page_resolved(
                 ctx,
                 request,
                 scope,
                 nil
               )

      assert length(first) == 2
      assert first_scanned <= 6
      assert first_memory >= :erlang.external_size(first, minor_version: 2)
      assert is_binary(continuation)
      assert continuation == first |> List.last() |> Map.fetch!(:event_id)

      assert {:ok,
              %{
                records: second,
                has_more: false,
                continuation: nil,
                scanned_entries: second_scanned
              }} =
               Ferricstore.Flow.Query.Engine.execute_history_page_resolved(
                 ctx,
                 request,
                 scope,
                 continuation
               )

      assert length(second) == 2
      assert second_scanned <= 6
      assert MapSet.disjoint?(MapSet.new(ids(first)), MapSet.new(ids(second)))
      assert length(Enum.uniq(ids(first) ++ ids(second))) == 4
      assert ids(first) ++ ids(second) == Enum.sort(ids(first) ++ ids(second))
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "history pages fail their raw scan budget without mutating expired LMDB rows" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    id = "query-history-expired-#{System.unique_integer([:positive, :monotonic])}"

    try do
      assert :ok = Ferricstore.Flow.create(ctx, id, type: "query-history", now_ms: 1_000)

      partition_key = Keys.auto_partition_key(id)
      history_key = Keys.history_key(id, partition_key)
      shard_index = Router.shard_for(ctx, history_key)
      assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)

      path =
        ctx.data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> Ferricstore.Flow.LMDB.path()

      {expired_ops, expired_keys} =
        Enum.map_reduce(10..12, [], fn version, keys ->
          event_id = "2000-#{version}"
          key = Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, 2_000)

          value =
            Ferricstore.Flow.LMDB.encode_history_index_value(
              event_id,
              2_000,
              "missing-expired-#{version}",
              1
            )

          {{:put, key, value}, [key | keys]}
        end)

      live_event_id = "2000-13"

      live_op =
        {:put, Ferricstore.Flow.LMDB.history_index_key(history_key, live_event_id, 2_000),
         Ferricstore.Flow.LMDB.encode_history_index_value(
           live_event_id,
           2_000,
           "missing-live",
           0
         )}

      assert :ok = Ferricstore.Flow.LMDB.write_batch(path, expired_ops ++ [live_op])

      request = Request.history(:execute, [eq(:run_id, id)], :asc, 2)

      assert {:error, :query_scan_budget_exceeded} =
               Ferricstore.Flow.Query.Engine.execute_history_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 nil
               )

      Enum.each(expired_keys, fn key ->
        assert {:ok, _value} = Ferricstore.Flow.LMDB.get(path, key)
      end)
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "history pages fail closed when an indexed event payload is missing" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    id = "query-history-stale-#{System.unique_integer([:positive, :monotonic])}"

    try do
      assert :ok = Ferricstore.Flow.create(ctx, id, type: "query-history", now_ms: 1_000)

      partition_key = Keys.auto_partition_key(id)
      history_key = Keys.history_key(id, partition_key)
      shard_index = Router.shard_for(ctx, history_key)
      assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)

      path =
        ctx.data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> Ferricstore.Flow.LMDB.path()

      event_id = "2000-10"
      index_key = Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, 2_000)

      index_value =
        Ferricstore.Flow.LMDB.encode_history_index_value(
          event_id,
          2_000,
          "missing-history-payload",
          0
        )

      assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put, index_key, index_value}])

      request = Request.history(:execute, [eq(:run_id, id)], :asc, 2)

      assert {:error, :query_storage_inconsistent} =
               Ferricstore.Flow.Query.Engine.execute_history_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 nil
               )
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "history pages validate a dangling lookahead before issuing a cursor" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    id = "query-history-lookahead-#{System.unique_integer([:positive, :monotonic])}"

    try do
      assert :ok = Ferricstore.Flow.create(ctx, id, type: "query-history", now_ms: 1_000)

      partition_key = Keys.auto_partition_key(id)
      history_key = Keys.history_key(id, partition_key)
      shard_index = Router.shard_for(ctx, history_key)
      assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)

      path =
        ctx.data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> Ferricstore.Flow.LMDB.path()

      event_id = "2000-10"
      index_key = Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, 2_000)

      index_value =
        Ferricstore.Flow.LMDB.encode_history_index_value(
          event_id,
          2_000,
          "missing-history-lookahead",
          0
        )

      assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put, index_key, index_value}])

      request = Request.history(:execute, [eq(:run_id, id)], :asc, 1)

      assert {:error, :query_storage_inconsistent} =
               Ferricstore.Flow.Query.Engine.execute_history_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 nil
               )
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "parent lineage continuation performs bounded exclusive tuple seeks" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    unique = System.unique_integer([:positive, :monotonic])
    partition_key = "query-lineage-tenant-#{unique}"
    parent_id = "query-lineage-parent-#{unique}"
    child_ids = Enum.map(1..3, &"query-lineage-child-#{unique}-#{&1}")

    try do
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
          [eq(:partition_key, partition_key), eq(:parent_flow_id, parent_id)],
          [{:updated_at_ms, :asc}],
          2,
          :record
        )

      assert {:ok,
              %{
                records: first,
                has_more: true,
                continuation: first_boundary,
                scanned_entries: first_scanned
              }} =
               Ferricstore.Flow.Query.Engine.execute_lineage_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 nil
               )

      assert first_scanned <= 6

      assert first_boundary ==
               first
               |> List.last()
               |> then(&{&1.updated_at_ms, &1.id})

      assert {:ok,
              %{
                records: second,
                has_more: false,
                continuation: nil,
                scanned_entries: second_scanned
              }} =
               Ferricstore.Flow.Query.Engine.execute_lineage_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 first_boundary
               )

      assert second_scanned <= 6
      records = first ++ second
      ids = Enum.map(records, & &1.id)
      keys = Enum.map(records, &{&1.updated_at_ms, &1.id})
      first_ids = MapSet.new(Enum.map(first, & &1.id))
      second_ids = MapSet.new(Enum.map(second, & &1.id))

      assert length(first) == 2
      assert length(second) == 1
      assert MapSet.disjoint?(first_ids, second_ids)
      assert Enum.sort(ids) == Enum.sort(child_ids)
      assert keys == Enum.sort(keys)
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "root lineage pages include the authoritative root record within their hard bound" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    unique = System.unique_integer([:positive, :monotonic])
    partition_key = "query-root-tenant-#{unique}"
    root_id = "query-root-#{unique}"
    child_ids = Enum.map(1..3, &"query-root-child-#{unique}-#{&1}")

    try do
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
          [eq(:root_flow_id, root_id), eq(:partition_key, partition_key)],
          [{:updated_at_ms, :asc}],
          2,
          :record
        )

      assert {:ok, first_page} =
               Ferricstore.Flow.Query.Engine.execute_lineage_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 nil
               )

      assert first_page.has_more
      assert first_page.scanned_entries <= 7

      assert first_page.continuation ==
               first_page.records
               |> List.last()
               |> then(&{&1.updated_at_ms, &1.id})

      assert {:ok, second_page} =
               Ferricstore.Flow.Query.Engine.execute_lineage_page_resolved(
                 ctx,
                 request,
                 MandatoryScope.dedicated(),
                 first_page.continuation
               )

      refute second_page.has_more
      assert second_page.continuation == nil
      assert second_page.scanned_entries <= 7

      records = first_page.records ++ second_page.records
      ids = Enum.map(records, & &1.id)
      keys = Enum.map(records, &{&1.updated_at_ms, &1.id})

      assert length(ids) == 4
      assert Enum.sort(ids) == Enum.sort([root_id | child_ids])
      assert length(ids) == length(Enum.uniq(ids))
      assert keys == Enum.sort(keys)
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "point reads fail closed when the physical record violates the bound predicates" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    id = "query-misplaced-#{System.unique_integer([:positive, :monotonic])}"
    source_partition = "tenant-source"
    requested_partition = "tenant-requested"

    try do
      assert :ok =
               Ferricstore.Flow.create(ctx, id,
                 type: "query-isolation",
                 state: "ready",
                 partition_key: source_partition,
                 now_ms: 1_000
               )

      encoded = Router.get(ctx, Keys.state_key(id, source_partition))
      assert is_binary(encoded)
      assert :ok = Router.put(ctx, Keys.state_key(id, requested_partition), encoded, 0)

      request =
        Request.point_read(
          :execute,
          {:literal, :keyword, requested_partition},
          {:literal, :keyword, id}
        )

      assert {:error, :query_storage_inconsistent} = Query.execute(ctx, request)

      assert Error.payload(:query_storage_inconsistent) == %{
               "code" => "query_storage_inconsistent",
               "message" => "ERR Flow query storage record is inconsistent",
               "retryable" => false,
               "safe_to_retry" => false,
               "retry_after_ms" => 0
             }

      assert Error.status(:query_storage_inconsistent) == :error
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  defp eq(field, value), do: {:eq, field, {:literal, :keyword, value}}
  defp ids(records), do: Enum.map(records, & &1.event_id)

  test "point reads classify corrupt encoded records as storage inconsistency" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    id = "query-corrupt-#{System.unique_integer([:positive, :monotonic])}"
    partition = "tenant-corrupt"

    try do
      assert :ok = Router.put(ctx, Keys.state_key(id, partition), "not-a-flow-record", 0)

      request =
        Request.point_read(
          :execute,
          {:literal, :keyword, partition},
          {:literal, :keyword, id}
        )

      assert {:error, :query_storage_inconsistent} = Query.execute(ctx, request)
    after
      IsolatedInstance.checkin(ctx)
    end
  end
end
