defmodule Ferricstore.Flow.SharedScopeTest do
  use ExUnit.Case, async: false

  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.{ClaimWaiters, Keys}
  alias Ferricstore.Flow.Query.MandatoryScope
  alias Ferricstore.Flow.Query.Request
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  defmodule Provider do
    @behaviour MetadataExtension

    @field_id 0x8001

    @impl true
    def configure(_opts) do
      {:ok,
       %{
         mode: :shared,
         generation: 1,
         fields: [
           %{
             id: @field_id,
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
    def bind_write(operation, %{"tenant_ref" => tenant_ref, "observer" => observer}, _snapshot)
        when is_pid(observer) do
      send(observer, {:flow_scope_bound, operation, tenant_ref})
      {:ok, %{@field_id => tenant_ref}}
    end

    def bind_write(_operation, %{"tenant_ref" => tenant_ref}, _snapshot),
      do: {:ok, %{@field_id => tenant_ref}}

    def bind_write(_operation, _context, _snapshot), do: {:error, :flow_scope_required}

    @impl true
    def bind_query(
          source,
          %{"tenant_ref" => tenant_ref, "observer" => observer},
          _snapshot
        )
        when source in [:runs, :events] and is_pid(observer) do
      send(observer, {:flow_query_scope_bound, source, tenant_ref})
      {:ok, {:required, [{@field_id, :eq, tenant_ref}]}}
    end

    def bind_query(source, %{"tenant_ref" => tenant_ref}, _snapshot)
        when source in [:runs, :events],
        do: {:ok, {:required, [{@field_id, :eq, tenant_ref}]}}

    def bind_query(_source, _context, _snapshot), do: {:error, :flow_scope_required}
  end

  setup do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        flow_metadata_extension: Provider,
        flow_tenancy_mode: :shared
      )

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    {:ok, ctx: ctx}
  end

  test "identical logical records remain isolated and hidden metadata never escapes", %{ctx: ctx} do
    tenant_a = Map.put(ctx, :request_context, %{"tenant_ref" => 11})
    tenant_b = Map.put(ctx, :request_context, %{"tenant_ref" => 22})
    id = "same-run"
    partition = "same-partition"

    assert :ok =
             Ferricstore.Flow.create(tenant_a, id,
               type: "tenant-a-type",
               partition_key: partition,
               now_ms: 1_000
             )

    assert :ok =
             Ferricstore.Flow.create(tenant_b, id,
               type: "tenant-b-type",
               partition_key: partition,
               now_ms: 2_000
             )

    assert {:ok, %{type: "tenant-a-type", partition_key: ^partition} = record_a} =
             Ferricstore.Flow.get(tenant_a, id, partition_key: partition)

    assert {:ok, %{type: "tenant-b-type", partition_key: ^partition} = record_b} =
             Ferricstore.Flow.get(tenant_b, id, partition_key: partition)

    assert {:ok, scope_a} = MandatoryScope.bind(tenant_a, :runs)
    assert {:ok, metadata_a} = MandatoryScope.single_metadata(scope_a)

    assert {:ok, %{type: "tenant-a-type", partition_key: ^partition}} =
             Ferricstore.Flow.get_resolved(
               ctx,
               id,
               [partition_key: partition],
               metadata_a
             )

    point_request =
      Request.point_read(
        :execute,
        {:literal, :keyword, partition},
        {:literal, :keyword, id}
      )

    assert {:ok, %{type: "tenant-a-type", partition_key: ^partition}} =
             Ferricstore.Flow.Query.Engine.execute_resolved(ctx, point_request, scope_a)

    refute Map.has_key?(record_a, :system_metadata)
    refute Map.has_key?(record_b, :system_metadata)
    assert Router.get(ctx, Keys.state_key(id, partition)) == nil

    assert :ok =
             Ferricstore.Flow.transition(
               tenant_a,
               id,
               "queued",
               "tenant-a-ready",
               partition_key: partition,
               fencing_token: 0,
               now_ms: 3_000
             )

    assert :ok =
             Ferricstore.Flow.transition(
               tenant_b,
               id,
               "queued",
               "tenant-b-ready",
               partition_key: partition,
               fencing_token: 0,
               now_ms: 4_000
             )

    assert {:ok, %{state: "tenant-a-ready"}} =
             Ferricstore.Flow.get(tenant_a, id, partition_key: partition)

    assert {:ok, %{state: "tenant-b-ready"}} =
             Ferricstore.Flow.get(tenant_b, id, partition_key: partition)
  end

  test "collection reads bind logical partitions and reject a foreign physical partition", %{
    ctx: ctx
  } do
    tenant_a = Map.put(ctx, :request_context, %{"tenant_ref" => 11})
    tenant_b = Map.put(ctx, :request_context, %{"tenant_ref" => 22})
    partition = "shared-read-partition"
    type = "shared-read-type"

    assert :ok =
             Ferricstore.Flow.create(tenant_a, "tenant-a-run",
               type: type,
               partition_key: partition,
               now_ms: 1_000
             )

    assert :ok =
             Ferricstore.Flow.create(tenant_b, "tenant-b-run",
               type: type,
               partition_key: partition,
               now_ms: 2_000
             )

    assert :ok =
             Ferricstore.Flow.create(tenant_a, "tenant-a-auto",
               type: type,
               now_ms: 3_000
             )

    assert :ok =
             Ferricstore.Flow.create(tenant_b, "tenant-b-auto",
               type: type,
               now_ms: 4_000
             )

    assert {:ok, scope_a} = MandatoryScope.bind(tenant_a, :runs)
    assert {:ok, physical_a} = MandatoryScope.physical_partition_key(scope_a, partition)

    assert {:ok, [%{id: "tenant-a-run", partition_key: ^partition} = record_a]} =
             Ferricstore.Flow.list(tenant_a, type, partition_key: partition)

    assert {:ok, [%{id: "tenant-b-run", partition_key: ^partition} = record_b]} =
             Ferricstore.Flow.list(tenant_b, type, partition_key: partition)

    refute Map.has_key?(record_a, :system_metadata)
    refute Map.has_key?(record_b, :system_metadata)

    assert {:ok, []} =
             Ferricstore.Flow.list(tenant_b, type, partition_key: physical_a)

    assert {:ok, [%{id: "tenant-a-auto"}]} = Ferricstore.Flow.list(tenant_a, type)
    assert {:ok, [%{id: "tenant-b-auto"}]} = Ferricstore.Flow.list(tenant_b, type)
  end

  test "history reads bind event scope and reject a foreign physical partition", %{ctx: ctx} do
    tenant_a = Map.put(ctx, :request_context, %{"tenant_ref" => 51})
    tenant_b = Map.put(ctx, :request_context, %{"tenant_ref" => 52})
    id = "same-history-run"
    partition = "shared-history-partition"

    assert :ok =
             Ferricstore.Flow.create(tenant_a, id,
               type: "tenant-a-history",
               partition_key: partition,
               now_ms: 1_000
             )

    assert :ok =
             Ferricstore.Flow.create(tenant_b, id,
               type: "tenant-b-history",
               partition_key: partition,
               now_ms: 2_000
             )

    assert {:ok, scope_a} = MandatoryScope.bind(tenant_a, :events)
    assert {:ok, physical_a} = MandatoryScope.physical_partition_key(scope_a, partition)

    assert {:ok, [{event_a, %{"event" => "created"}}]} =
             Ferricstore.Flow.history(tenant_a, id, partition_key: partition)

    assert {:ok, [{event_b, %{"event" => "created"}}]} =
             Ferricstore.Flow.history(tenant_b, id, partition_key: partition)

    assert String.starts_with?(event_a, "1000-")
    assert String.starts_with?(event_b, "2000-")

    assert {:ok, []} =
             Ferricstore.Flow.history(tenant_b, id, partition_key: physical_a)
  end

  test "lineage and count readers remain tenant-contained", %{ctx: ctx} do
    tenant_a = Map.put(ctx, :request_context, %{"tenant_ref" => 61})
    tenant_b = Map.put(ctx, :request_context, %{"tenant_ref" => 62})
    partition = "shared-lineage-partition"
    type = "shared-lineage-type"
    parent = "same-parent"
    root = "same-root"
    correlation = "same-correlation"

    for {tenant, id, now_ms} <- [
          {tenant_a, "tenant-a-child", 1_000},
          {tenant_b, "tenant-b-child", 2_000}
        ] do
      assert :ok =
               Ferricstore.Flow.create(tenant, id,
                 type: type,
                 partition_key: partition,
                 parent_flow_id: parent,
                 root_flow_id: root,
                 correlation_id: correlation,
                 now_ms: now_ms
               )
    end

    assert {:ok, scope_a} = MandatoryScope.bind(tenant_a, :runs)
    assert {:ok, physical_a} = MandatoryScope.physical_partition_key(scope_a, partition)

    for {reader, value} <- [
          {:by_parent, parent},
          {:by_root, root},
          {:by_correlation, correlation}
        ] do
      assert {:ok, [%{id: "tenant-a-child", partition_key: ^partition}]} =
               apply(Ferricstore.Flow, reader, [tenant_a, value, [partition_key: partition]])

      assert {:ok, [%{id: "tenant-b-child", partition_key: ^partition}]} =
               apply(Ferricstore.Flow, reader, [tenant_b, value, [partition_key: partition]])

      assert {:ok, []} =
               apply(Ferricstore.Flow, reader, [tenant_b, value, [partition_key: physical_a]])
    end

    assert {:ok, %{count: 1}} =
             Ferricstore.Flow.stats(tenant_a, type,
               state: "queued",
               partition_key: partition
             )

    assert {:ok, %{count: 0}} =
             Ferricstore.Flow.stats(tenant_b, type,
               state: "queued",
               partition_key: physical_a
             )

    assert {:ok, %{queued: 1, partition_key: ^partition}} =
             Ferricstore.Flow.info(tenant_a, type, partition_key: partition)

    assert {:ok, %{queued: 0}} =
             Ferricstore.Flow.info(tenant_b, type, partition_key: physical_a)
  end

  test "attribute, search, and terminal projections remain tenant-contained", %{ctx: ctx} do
    tenant_a = Map.put(ctx, :request_context, %{"tenant_ref" => 63})
    tenant_b = Map.put(ctx, :request_context, %{"tenant_ref" => 64})
    partition = "shared-projection-partition"
    type = "shared-projection-type"

    assert {:ok, %{indexed_attributes: ["segment"]}} =
             Ferricstore.Flow.policy_set(ctx, type, indexed_attributes: ["segment"])

    for {tenant, id, segment, now_ms} <- [
          {tenant_a, "tenant-a-projection", "alpha", 1_000},
          {tenant_b, "tenant-b-projection", "beta", 2_000}
        ] do
      assert :ok =
               Ferricstore.Flow.create(tenant, id,
                 type: type,
                 partition_key: partition,
                 attributes: %{"segment" => segment},
                 now_ms: now_ms
               )

      assert {:ok, [claimed]} =
               Ferricstore.Flow.claim_due(tenant, type,
                 worker: "projection-worker",
                 partition_key: partition,
                 now_ms: now_ms + 100,
                 limit: 1
               )

      assert :ok =
               Ferricstore.Flow.fail(tenant, id, claimed.lease_token,
                 partition_key: partition,
                 fencing_token: claimed.fencing_token,
                 now_ms: now_ms + 200
               )
    end

    assert {:ok, scope_a} = MandatoryScope.bind(tenant_a, :runs)
    assert {:ok, physical_a} = MandatoryScope.physical_partition_key(scope_a, partition)

    assert {:ok, [%{id: "tenant-a-projection", partition_key: ^partition}]} =
             Ferricstore.Flow.search(tenant_a,
               type: type,
               state: "failed",
               partition_key: partition,
               attributes: %{"segment" => "alpha"},
               consistent_projection: true
             )

    assert {:ok, []} =
             Ferricstore.Flow.search(tenant_b,
               type: type,
               state: "failed",
               partition_key: physical_a,
               attributes: %{"segment" => "alpha"},
               consistent_projection: true
             )

    assert {:ok, [%{name: "segment", count: 1}]} =
             Ferricstore.Flow.attributes(tenant_a, type,
               state: "failed",
               partition_key: partition,
               consistent_projection: true
             )

    assert {:ok, [%{value: "alpha", count: 1}]} =
             Ferricstore.Flow.attribute_values(tenant_a, type, "segment",
               state: "failed",
               partition_key: partition,
               consistent_projection: true
             )

    assert {:ok, [%{id: "tenant-b-projection", partition_key: ^partition}]} =
             Ferricstore.Flow.failures(tenant_b, type,
               partition_key: partition,
               consistent_projection: true
             )

    assert {:ok, []} =
             Ferricstore.Flow.failures(tenant_b, type,
               partition_key: physical_a,
               consistent_projection: true
             )
  end

  test "batched point and history reads bind scope before their fast paths", %{ctx: ctx} do
    tenant_a = Map.put(ctx, :request_context, %{"tenant_ref" => 71})
    tenant_b = Map.put(ctx, :request_context, %{"tenant_ref" => 72})
    id = "same-batched-read"
    partition = "shared-batched-read-partition"

    assert :ok =
             Ferricstore.Flow.create(tenant_a, id,
               type: "tenant-a-batched",
               partition_key: partition,
               now_ms: 1_000
             )

    assert :ok =
             Ferricstore.Flow.create(tenant_b, id,
               type: "tenant-b-batched",
               partition_key: partition,
               now_ms: 2_000
             )

    assert {:ok, scope_a} = MandatoryScope.bind(tenant_a, :runs)
    assert {:ok, physical_a} = MandatoryScope.physical_partition_key(scope_a, partition)

    assert [
             {:ok, %{type: "tenant-a-batched", partition_key: ^partition}},
             {:ok, [{event_a, %{"event" => "created"}}]}
           ] =
             Ferricstore.Flow.pipeline_read_batch(tenant_a, [
               {:get, id, [partition_key: partition]},
               {:history, id, [partition_key: partition]}
             ])

    assert String.starts_with?(event_a, "1000-")

    assert [{:ok, nil}, {:ok, []}] =
             Ferricstore.Flow.pipeline_read_batch(tenant_b, [
               {:get, id, [partition_key: physical_a]},
               {:history, id, [partition_key: physical_a]}
             ])
  end

  test "a read pipeline resolves tenant authority once per referenced source", %{ctx: ctx} do
    tenant =
      Map.put(ctx, :request_context, %{
        "tenant_ref" => 73,
        "observer" => self()
      })

    assert [{:ok, nil}, {:ok, nil}, {:ok, []}, {:ok, []}] =
             Ferricstore.Flow.pipeline_read_batch(tenant, [
               {:get, "missing-a", []},
               {:get, "missing-b", []},
               {:history, "missing-a", [include_cold: false, consistent: false]},
               {:history, "missing-b", [include_cold: false, consistent: false]}
             ])

    assert_receive {:flow_query_scope_bound, :runs, 73}
    assert_receive {:flow_query_scope_bound, :events, 73}
    refute_receive {:flow_query_scope_bound, _source, 73}
  end

  test "missing shared scope fails before a record is replicated", %{ctx: ctx} do
    assert {:error, message} =
             Ferricstore.Flow.create(ctx, "missing-scope",
               type: "test",
               partition_key: "same-partition",
               now_ms: 1_000
             )

    assert message =~ "scope"
    assert Router.get(ctx, Keys.state_key("missing-scope", "same-partition")) == nil

    assert {:error, transition_message} =
             Ferricstore.Flow.transition(
               ctx,
               "missing-scope",
               "queued",
               "ready",
               partition_key: "same-partition",
               fencing_token: 0,
               now_ms: 2_000
             )

    assert transition_message =~ "scope"
  end

  test "signals and mixed pipeline writes preserve the bound scope", %{ctx: ctx} do
    tenant_a = Map.put(ctx, :request_context, %{"tenant_ref" => 11})
    tenant_b = Map.put(ctx, :request_context, %{"tenant_ref" => 22})
    id = "pipeline-same-run"
    partition = "pipeline-same-partition"

    create = fn tenant, type ->
      Ferricstore.Flow.pipeline_write_batch_independent(tenant, [
        {:create, id, [type: type, partition_key: partition, now_ms: 1_000]}
      ])
    end

    assert [:ok] = create.(tenant_a, "tenant-a-type")
    assert [:ok] = create.(tenant_b, "tenant-b-type")

    assert [:ok] =
             Ferricstore.Flow.pipeline_write_batch_independent(tenant_a, [
               {:signal, id,
                [
                  signal: "advance-a",
                  if_state: "queued",
                  transition_to: "tenant-a-ready",
                  partition_key: partition,
                  now_ms: 2_000
                ]}
             ])

    assert :ok =
             Ferricstore.Flow.signal(tenant_b, id,
               signal: "advance-b",
               if_state: "queued",
               transition_to: "tenant-b-ready",
               partition_key: partition,
               now_ms: 3_000
             )

    assert {:ok, %{state: "tenant-a-ready", type: "tenant-a-type"}} =
             Ferricstore.Flow.get(tenant_a, id, partition_key: partition)

    assert {:ok, %{state: "tenant-b-ready", type: "tenant-b-type"}} =
             Ferricstore.Flow.get(tenant_b, id, partition_key: partition)

    assert [{:error, message}] = create.(ctx, "unscoped")
    assert message =~ "scope"

    assert {:error, signal_message} =
             Ferricstore.Flow.signal(ctx, id,
               signal: "forbidden",
               partition_key: partition,
               now_ms: 4_000
             )

    assert signal_message =~ "scope"
  end

  test "named values with identical logical owners and names remain isolated", %{ctx: ctx} do
    tenant_a = Map.put(ctx, :request_context, %{"tenant_ref" => 11})
    tenant_b = Map.put(ctx, :request_context, %{"tenant_ref" => 22})
    id = "same-value-owner"
    partition = "same-value-partition"

    for {tenant, type} <- [{tenant_a, "tenant-a"}, {tenant_b, "tenant-b"}] do
      assert :ok =
               Ferricstore.Flow.create(tenant, id,
                 type: type,
                 partition_key: partition,
                 now_ms: 1_000
               )
    end

    assert {:ok, value_a} =
             Ferricstore.Flow.value_put(tenant_a, "value-a",
               owner_flow_id: id,
               name: "result",
               partition_key: partition,
               now_ms: 2_000
             )

    assert {:ok, value_b} =
             Ferricstore.Flow.value_put(tenant_b, "value-b",
               owner_flow_id: id,
               name: "result",
               partition_key: partition,
               now_ms: 2_000
             )

    refute value_a.ref == value_b.ref

    assert {:ok, %{values: %{"result" => "value-a"}}} =
             Ferricstore.Flow.get(tenant_a, id, partition_key: partition, values: ["result"])

    assert {:ok, %{values: %{"result" => "value-b"}}} =
             Ferricstore.Flow.get(tenant_b, id, partition_key: partition, values: ["result"])

    assert {:error, message} =
             Ferricstore.Flow.value_put(ctx, "forbidden",
               owner_flow_id: id,
               name: "result",
               partition_key: partition,
               now_ms: 3_000
             )

    assert message =~ "scope"
  end

  test "claiming binds explicit and automatic partitions to one tenant", %{ctx: ctx} do
    tenant_a = Map.put(ctx, :request_context, %{"tenant_ref" => 11})
    tenant_b = Map.put(ctx, :request_context, %{"tenant_ref" => 22})

    for {tenant, type} <- [{tenant_a, "tenant-a"}, {tenant_b, "tenant-b"}] do
      assert :ok =
               Ferricstore.Flow.create(tenant, "same-claim",
                 type: type,
                 partition_key: "logical",
                 now_ms: 1_000
               )

      assert :ok =
               Ferricstore.Flow.create(tenant, "same-auto",
                 type: type,
                 now_ms: 1_000
               )
    end

    assert {:ok, [claimed_a]} =
             Ferricstore.Flow.claim_due(tenant_a, "tenant-a",
               worker: "worker-a",
               partition_key: "logical",
               now_ms: 2_000,
               return: :records
             )

    assert claimed_a.id == "same-claim"
    assert claimed_a.partition_key == "logical"
    refute Map.has_key?(claimed_a, :system_metadata)

    assert {:ok, [["same-claim", "logical", _lease, _fencing]]} =
             Ferricstore.Flow.claim_due(tenant_b, "tenant-b",
               worker: "worker-b",
               partition_key: "logical",
               now_ms: 2_000,
               return: :jobs_compact
             )

    assert {:ok, [%{id: "same-auto"} = auto_a]} =
             Ferricstore.Flow.claim_due(tenant_a, "tenant-a",
               worker: "worker-a",
               now_ms: 2_001,
               return: :records
             )

    assert String.starts_with?(auto_a.partition_key, "__flow_auto__:")

    assert {:error, any_message} =
             Ferricstore.Flow.claim_due(tenant_a, "tenant-a",
               worker: "worker-a",
               partition_key: :any,
               now_ms: 2_002
             )

    assert any_message =~ "scope"
  end

  test "pipeline claims bind one scope once and preserve logical tenant isolation", %{ctx: ctx} do
    tenant_a = Map.put(ctx, :request_context, %{"tenant_ref" => 31})
    tenant_b = Map.put(ctx, :request_context, %{"tenant_ref" => 32})
    partition = "pipeline-claim-partition"
    type = "pipeline-claim-type"

    for tenant <- [tenant_a, tenant_b] do
      assert :ok =
               Ferricstore.Flow.create(tenant, "same-pipeline-claim",
                 type: type,
                 partition_key: partition,
                 now_ms: 1_000
               )
    end

    observed_a =
      Map.put(tenant_a, :request_context, %{
        "tenant_ref" => 31,
        "observer" => self()
      })

    assert [
             {:ok, [%{id: "same-pipeline-claim", partition_key: ^partition} = record]},
             {:ok, []}
           ] =
             Ferricstore.Flow.pipeline_claim_due_batch(observed_a, [
               {:claim_due, type,
                [worker: "worker-a", partition_key: partition, now_ms: 2_000, return: :records]},
               {:claim_due, type,
                [worker: "worker-a", partition_key: partition, now_ms: 2_000, return: :records]}
             ])

    refute Map.has_key?(record, :system_metadata)
    assert_receive {:flow_scope_bound, :claim_due, 31}
    refute_receive {:flow_scope_bound, :claim_due, 31}, 25

    assert [{:ok, [["same-pipeline-claim", ^partition, _lease, _fencing]]}] =
             Ferricstore.Flow.pipeline_claim_due_batch(tenant_b, [
               {:claim_due, type,
                [
                  worker: "worker-b",
                  partition_key: partition,
                  now_ms: 2_000,
                  return: :jobs_compact
                ]}
             ])

    assert [{:error, message}] =
             Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
               {:claim_due, type, [worker: "forbidden", partition_key: partition, now_ms: 2_002]}
             ])

    assert message =~ "scope"
  end

  test "blocking claims reuse one scope and wake only for their physical tenant lane", %{ctx: ctx} do
    tenant_a = Map.put(ctx, :request_context, %{"tenant_ref" => 41})
    tenant_b = Map.put(ctx, :request_context, %{"tenant_ref" => 42})
    partition = "blocking-claim-partition"
    type = "blocking-claim-type"

    assert :ok =
             Ferricstore.Flow.create(tenant_b, "tenant-b-ready",
               type: type,
               partition_key: partition,
               now_ms: 1_000
             )

    test_pid = self()

    task =
      Task.async(fn ->
        tenant_a
        |> Map.put(:request_context, %{
          "tenant_ref" => 41,
          "observer" => test_pid
        })
        |> Ferricstore.Flow.claim_due(type,
          worker: "worker-a",
          partition_key: partition,
          now_ms: 2_000,
          block_ms: 1_000,
          return: :records
        )
      end)

    assert_receive {:flow_scope_bound, :claim_due, 41}
    assert eventually(fn -> ClaimWaiters.total_count() > 0 end)
    refute Task.yield(task, 25)

    assert :ok =
             Ferricstore.Flow.create(tenant_a, "tenant-a-ready",
               type: type,
               partition_key: partition,
               now_ms: 1_001
             )

    assert {:ok, [%{id: "tenant-a-ready", partition_key: ^partition}]} =
             Task.await(task, 1_000)

    refute_receive {:flow_scope_bound, :claim_due, 41}, 25

    assert {:ok, [%{id: "tenant-b-ready"}]} =
             Ferricstore.Flow.claim_due(tenant_b, type,
               worker: "worker-b",
               partition_key: partition,
               now_ms: 2_000
             )
  end

  defp eventually(fun, attempts \\ 100)

  defp eventually(fun, attempts) when is_function(fun, 0) and attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(5)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false
end
