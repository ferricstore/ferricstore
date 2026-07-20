defmodule FerricstoreServer.Native.CommandsTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  import ExUnit.CaptureLog

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.CatalogProjector
  alias FerricstoreServer.AuthRateLimiter
  alias Ferricstore.Commands.PreparedCommand
  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.{ClaimWaiters, Keys, StorageScope}
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Native.Codec
  alias FerricstoreServer.Native.Commands
  alias FerricstoreServer.Native.Session
  alias Ferricstore.Stats
  alias Ferricstore.Test.IsolatedInstance

  @op_hello 0x0001
  @op_auth 0x0002
  @op_client_set_name 0x0004
  @op_route 0x0006
  @op_route_batch 0x000F
  @op_shards 0x0007
  @op_options 0x000B
  @op_startup 0x000C
  @op_pipeline 0x000E
  @op_subscribe_events 0x0011
  @op_command_exec 0x0100
  @op_get 0x0101
  @op_mget 0x0104
  @op_set 0x0102
  @op_hset 0x0110
  @op_hgetall 0x0113
  @op_smembers 0x0132
  @op_ferricstore_metrics 0x030F
  @op_flow_create 0x0201
  @op_flow_get 0x0202
  @op_flow_claim_due 0x0203
  @op_flow_history 0x020A
  @op_flow_value_put 0x020B
  @op_flow_list 0x020E
  @op_flow_create_many 0x020F
  @op_flow_complete_many 0x0210
  @op_flow_reclaim 0x0215
  @op_flow_by_parent 0x0219
  @op_flow_by_root 0x021A
  @op_flow_by_correlation 0x021B
  @op_flow_policy_set 0x021E
  @op_flow_spawn_children 0x0220
  @op_flow_start_and_claim 0x0223
  @op_flow_run_steps_many 0x0224
  @op_flow_schedule_create 0x0225
  @op_flow_schedule_get 0x0226
  @op_flow_schedule_delete 0x0227
  @op_flow_schedule_fire 0x022A
  @op_flow_stats 0x022D
  @op_flow_search 0x0230
  @op_flow_query 0x0231
  @op_flow_approval_request 0x0246
  @op_flow_approval_get 0x0249
  @op_flow_circuit_open 0x024A
  @op_flow_circuit_get 0x024C
  @op_flow_budget_reserve 0x024D
  @op_flow_limit_lease 0x024F

  defmodule WakeScopeProvider do
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
    def bind_write(_operation, %{"tenant" => "tenant-a"}, _snapshot),
      do: {:ok, %{@field_id => 11}}

    def bind_write(_operation, %{"tenant" => "tenant-b"}, _snapshot),
      do: {:ok, %{@field_id => 22}}

    def bind_write(_operation, _context, _snapshot), do: {:error, :flow_scope_required}

    @impl true
    def bind_query(:runs, context, snapshot) do
      with {:ok, values} <- bind_write(:query, context, snapshot),
           {:ok, tenant_ref} <- Map.fetch(values, @field_id) do
        {:ok, {:required, [{@field_id, :eq, tenant_ref}]}}
      end
    end

    def bind_query(_source, _context, _snapshot), do: {:error, :flow_scope_required}
  end

  @tag :native_collection_preflight
  test "compact collection pipelines reject over-limit counts before scanning" do
    ctx = FerricStore.Instance.get(:default)
    suffix = System.unique_integer([:positive])
    set_key = "native:preflight:set:#{suffix}"
    hash_key = "native:preflight:hash:#{suffix}"

    assert {:ok, 2} = FerricStore.Impl.sadd(ctx, set_key, ["a", "b"])
    assert {:ok, 2} = FerricStore.Impl.hset(ctx, hash_key, %{"a" => "1", "b" => "2"})
    on_exit(fn -> FerricStore.del([set_key, hash_key]) end)

    traced_pid = self()
    tracer = spawn_link(fn -> forward_router_traces(traced_pid) end)
    :erlang.trace_pattern({Ferricstore.Store.Router, :compound_scan_raw, 3}, true, [])
    :erlang.trace(traced_pid, true, [:call, {:tracer, tracer}])

    try do
      for {mode, key} <- [{27, set_key}, {30, hash_key}] do
        assert {:bad_request, message, _state} =
                 Commands.execute(
                   @op_pipeline,
                   %{"return" => "pairs", "compact_pipeline" => {mode, [key]}},
                   state(%{max_collection_response_items: 1})
                 )

        assert message =~ "collection response item limit"

        refute_receive {:router_trace,
                        {:trace, ^traced_pid, :call,
                         {Ferricstore.Store.Router, :compound_scan_raw, _arguments}}}
      end
    after
      :erlang.trace(traced_pid, false, [:call])
      :erlang.trace_pattern({Ferricstore.Store.Router, :compound_scan_raw, 3}, false, [])
      Process.exit(tracer, :normal)
    end
  end

  @tag :native_compact_storage_failure
  test "compact compound reads preserve shard failures" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    shard = elem(ctx.shard_names, 0)

    try do
      GenServer.stop(shard, :normal, 5_000)

      native_state =
        state(%{
          instance_ctx: ctx,
          stats_counter: ctx.stats_counter
        })

      for {mode, item} <- [
            {18, {"missing-hash", "field"}},
            {19, {"missing-set", "member"}},
            {20, {"missing-list", 0, -1}},
            {21, {"missing-zset-range", 0, -1, false}},
            {27, "missing-set-members"},
            {29, {"missing-zset", "member"}},
            {30, "missing-hash-fields"}
          ] do
        assert {:ok, [["error", "ERR storage read failed"]], _state} =
                 Commands.execute(
                   @op_pipeline,
                   %{"return" => "pairs", "compact_pipeline" => {mode, [item]}},
                   native_state
                 )
      end
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "FLOW.QUERY exposes retry-safe storage outages without provider details" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    shard = elem(ctx.shard_names, 0)

    try do
      GenServer.stop(shard, :normal, 5_000)

      assert {:error, payload, _state} =
               Commands.execute(
                 @op_flow_query,
                 %{
                   "version" => "FQL1",
                   "query" =>
                     "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD"
                 },
                 state(%{instance_ctx: ctx, stats_counter: ctx.stats_counter})
               )

      assert payload == %{
               "code" => "query_storage_unavailable",
               "message" => "ERR Flow query storage is unavailable",
               "retryable" => true,
               "safe_to_retry" => true,
               "retry_after_ms" => 0
             }
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "FLOW.QUERY never exposes a primary record that violates its predicates" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    id = "native-query-misplaced-#{System.unique_integer([:positive, :monotonic])}"
    source_partition = "tenant-native-source"
    requested_partition = "tenant-native-requested"

    try do
      assert :ok =
               Ferricstore.Flow.create(ctx, id,
                 type: "native-query-isolation",
                 state: "ready",
                 partition_key: source_partition,
                 now_ms: 1_000
               )

      encoded = Router.get(ctx, Keys.state_key(id, source_partition))
      assert is_binary(encoded)
      assert :ok = Router.put(ctx, Keys.state_key(id, requested_partition), encoded, 0)

      assert {:error, payload, _state} =
               Commands.execute(
                 @op_flow_query,
                 %{
                   "version" => "FQL1",
                   "query" =>
                     "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD",
                   "params" => %{"partition" => requested_partition, "flow_id" => id}
                 },
                 state(%{instance_ctx: ctx, stats_counter: ctx.stats_counter})
               )

      assert payload == %{
               "code" => "query_storage_inconsistent",
               "message" => "ERR Flow query storage record is inconsistent",
               "retryable" => false,
               "safe_to_retry" => false,
               "retry_after_ms" => 0
             }

      refute inspect(payload) =~ source_partition
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  defmodule TestExtension do
    @behaviour Ferricstore.Commands.Extension

    @impl true
    def commands do
      [
        %{
          name: "EXT.READ",
          arity: 2,
          flags: ["readonly"],
          first_key: 1,
          last_key: 1,
          step: 1,
          access: :read,
          summary: "Test extension read"
        },
        %{
          name: "EXT.WRITE",
          arity: 2,
          flags: ["write"],
          first_key: 1,
          last_key: 1,
          step: 1,
          access: :write,
          summary: "Test extension write"
        },
        %{
          name: "EXT.CONTEXT",
          arity: 1,
          flags: ["readonly"],
          first_key: 0,
          last_key: 0,
          step: 0,
          access: :read,
          summary: "Test extension request context"
        }
      ]
    end

    @impl true
    def handle("EXT.READ", [key], _store), do: {:ok, ["read", key]}
    def handle("EXT.WRITE", [key], _store), do: {:ok, ["write", key]}

    def handle("EXT.CONTEXT", [], store),
      do: {:ok, Ferricstore.Commands.Extension.request_context(store)}
  end

  defmodule CountingKeyExtension do
    @behaviour Ferricstore.Commands.Extension

    @impl true
    def commands do
      [
        %{
          name: "EXT.COUNTED",
          arity: 2,
          flags: ["readonly"],
          first_key: 1,
          last_key: 1,
          step: 1,
          access: :read,
          summary: "Counts key discovery calls"
        }
      ]
    end

    @impl true
    def keys("EXT.COUNTED", [key]) do
      counter_key = {__MODULE__, :key_discovery_calls}
      Process.put(counter_key, Process.get(counter_key, 0) + 1)
      {:ok, [key]}
    end

    @impl true
    def handle("EXT.COUNTED", [key], _store), do: {:ok, key}
  end

  defmodule RaisingExtension do
    @behaviour Ferricstore.Commands.Extension

    @impl true
    def commands do
      [
        %{
          name: "EXT.RAISE",
          arity: 1,
          flags: ["readonly"],
          first_key: 0,
          last_key: 0,
          step: 0,
          access: :read,
          summary: "Raises for command boundary tests"
        }
      ]
    end

    @impl true
    def handle("EXT.RAISE", [], _store), do: raise("extension-internal-secret")
  end

  defmodule ContextQueryEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(ctx, request) do
      {:ok,
       %{
         mode: request.mode,
         request_context: FerricStore.Flow.QueryEngine.request_context(ctx)
       }}
    end
  end

  defmodule CompactContextQueryEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(ctx, request) do
      instance_ctx = FerricStore.Flow.QueryEngine.instance_context(ctx)
      request_context = FerricStore.Flow.QueryEngine.request_context(ctx)

      {:ok,
       %{
         compact: is_map(ctx) and map_size(ctx) <= 3,
         instance_name: instance_ctx.name,
         mode: request.mode,
         request_context: request_context
       }}
    end
  end

  defmodule DeadlineQueryEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(ctx, request) do
      {:ok,
       %{
         deadline_ms: FerricStore.Flow.QueryEngine.deadline_ms(ctx),
         mode: request.mode,
         request_context: FerricStore.Flow.QueryEngine.request_context(ctx)
       }}
    end
  end

  defmodule RejectingQueryEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: {:error, :unauthorized_scope}
  end

  defmodule RaisingQueryEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: raise("query-provider-secret")
  end

  defmodule NotifyingQueryEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request) do
      send(self(), :query_engine_called)
      {:ok, nil}
    end
  end

  defmodule QueryResourceLimits do
    @behaviour FerricStore.ResourceLimits

    @impl true
    def set_limit(_scope, _limit_spec, _opts), do: {:error, :unsupported}

    @impl true
    def get_limit(_scope, _opts), do: {:error, :unsupported}

    @impl true
    def usage(_scope, _opts), do: {:ok, %{}}

    @impl true
    def check(_scope, _resource, _amount, _opts), do: :ok

    @impl true
    def reserve(_scope, _resource, _amount, _opts), do: {:ok, nil}

    @impl true
    def release(_reservation, _opts), do: :ok

    @impl true
    def record_activity(keys, _opts) do
      send(self(), {:query_resource_activity, keys})
      :ok
    end

    @impl true
    def check_command(command, args, keys, _opts) do
      send(self(), {:query_resource_check, command, args, keys})
      Process.get(:query_resource_check_result, :ok)
    end
  end

  defmodule AdvertisedQueryEngine do
    @behaviour FerricStore.Flow.QueryEngine

    @impl true
    def execute(_ctx, _request), do: {:ok, nil}

    @impl true
    def capabilities do
      %{
        query_contract: "enterprise.query/v1",
        explain_contract: "enterprise.explain/v1",
        capabilities: ["enterprise_query_v1"],
        language_versions: ["FQL1"],
        shapes: ["runs_by_partition_and_run_id_record"]
      }
    end
  end

  setup do
    previous_extensions = Application.get_env(:ferricstore, :command_extensions)

    previous_query_engine =
      Application.get_env(:ferricstore, FerricStore.Flow.QueryEngine)

    previous_trusted_request_context_users =
      Application.get_env(:ferricstore, :native_trusted_request_context_users)

    previous_acl_management = Application.get_env(:ferricstore, FerricStore.Management.ACL)

    Application.delete_env(:ferricstore, :command_extensions)
    Application.delete_env(:ferricstore, FerricStore.Flow.QueryEngine)
    Application.delete_env(:ferricstore, :native_trusted_request_context_users)

    Application.put_env(
      :ferricstore,
      FerricStore.Management.ACL,
      FerricstoreServer.Management.ACL
    )

    ConnRegistry.init_table()
    FerricstoreServer.Acl.reset!()
    {:ok, _} = Application.ensure_all_started(:ferricstore)

    on_exit(fn ->
      FerricstoreServer.Acl.reset!()

      case previous_extensions do
        nil -> Application.delete_env(:ferricstore, :command_extensions)
        value -> Application.put_env(:ferricstore, :command_extensions, value)
      end

      case previous_query_engine do
        nil -> Application.delete_env(:ferricstore, FerricStore.Flow.QueryEngine)
        value -> Application.put_env(:ferricstore, FerricStore.Flow.QueryEngine, value)
      end

      case previous_trusted_request_context_users do
        nil -> Application.delete_env(:ferricstore, :native_trusted_request_context_users)
        value -> Application.put_env(:ferricstore, :native_trusted_request_context_users, value)
      end

      case previous_acl_management do
        nil -> Application.delete_env(:ferricstore, FerricStore.Management.ACL)
        value -> Application.put_env(:ferricstore, FerricStore.Management.ACL, value)
      end
    end)

    :ok
  end

  test "OPTIONS advertises native protocol capabilities and command coverage" do
    {status, payload, _state} = Commands.execute(@op_options, %{}, state())

    assert status == :ok
    assert payload.protocol_versions == [1]
    assert payload.multiplexing.request_id == true
    assert payload.multiplexing.concurrent_lanes == true
    assert payload.response_codecs.typed_value == true

    assert payload.response_codecs.compact_response_opcodes == %{
             "flow_claim_jobs_v1" => [0x0203],
             "flow_record_list_v1" => [0x020E, 0x0217, 0x0218, 0x0219, 0x021A, 0x021B, 0x021D],
             "flow_record_v1" => [0x0202],
             "kv_get_v1" => [0x0101],
             "kv_mget_v1" => [0x0104, 0x020C],
             "ok_list_v1" => [0x0102, 0x0105, 0x020F, 0x0210, 0x0212, 0x0213, 0x0214],
             "pipeline_v1" => [0x000E]
           }

    assert "FLOW.CREATE" in schema_names(payload)
    assert "FLOW.CLAIM_DUE" in schema_names(payload)
    assert "FLOW.COMPLETE" in schema_names(payload)
    assert "FLOW.RUN_STEPS_MANY" in schema_names(payload)
    assert "FLOW.SCHEDULE.CREATE" in schema_names(payload)
    assert "FLOW.ATTRIBUTES" in schema_names(payload)
    assert "FLOW.SEARCH" in opcode_names(payload)
    assert "FLOW.QUERY" in opcode_names(payload)
    assert "FLOW.BUDGET.RESERVE" in opcode_names(payload)
    assert "GET" in opcode_names(payload)
    assert "SET" in opcode_names(payload)
    refute "GET.COMPACT" in opcode_names(payload)

    for command <- [
          "FLOW.CREATE",
          "FLOW.CREATE_MANY",
          "FLOW.POLICY.SET",
          "FLOW.SPAWN_CHILDREN",
          "FLOW.START_AND_CLAIM"
        ] do
      assert "max_active_ms" in payload.schemas[command]["fields"]
    end

    assert "expected_generation" in payload.schemas["FLOW.POLICY.SET"]["fields"]
    assert "replace" in payload.schemas["FLOW.POLICY.SET"]["fields"]

    assert payload.flow_query == %{
             query_contract: "ferric.flow.query/v1",
             explain_contract: "ferric.flow.explain/v1",
             capabilities: ["flow_query_point_v1", "flow_query_history_v1"],
             language_versions: ["FQL1"],
             shapes: [
               "runs_by_run_id_record",
               "runs_by_partition_and_run_id_record",
               "events_by_run_id_ordered_records"
             ]
           }

    assert payload.schemas["FLOW.QUERY"] == %{
             "required" => ["version", "query"],
             "fields" => ["version", "query", "params", "deadline_ms"]
           }

    assert "parent_flow_id" in payload.schemas["FLOW.CREATE"]["fields"]
    assert "root_flow_id" in payload.schemas["FLOW.CREATE"]["fields"]
    refute "parent_id" in payload.schemas["FLOW.CREATE"]["fields"]
    refute "root_id" in payload.schemas["FLOW.CREATE"]["fields"]
    assert "rev" in payload.schemas["FLOW.SCHEDULE.LIST"]["fields"]

    assert payload.schemas["FLOW.SIGNAL"]["required"] == ["id", "signal"]

    for field <- [
          "partition_key",
          "idempotency_key",
          "if_state",
          "transition_to",
          "run_at_ms",
          "now_ms",
          "values",
          "value_refs",
          "drop_values",
          "override_values"
        ] do
      assert field in payload.schemas["FLOW.SIGNAL"]["fields"]
    end

    for command <- [
          "FLOW.GET",
          "FLOW.LIST",
          "FLOW.HISTORY",
          "FLOW.RETRY",
          "FLOW.FAIL",
          "FLOW.CANCEL",
          "FLOW.POLICY.GET"
        ] do
      assert Map.has_key?(payload.schemas, command)
    end
  end

  test "OPTIONS advertises the capability manifest frozen into its instance" do
    {status, payload, _state} =
      Commands.execute(@op_options, %{}, state_with_query_engine(AdvertisedQueryEngine))

    assert status == :ok
    assert payload.flow_query == AdvertisedQueryEngine.capabilities()
  end

  test "HELLO returns native route metadata only" do
    {status, payload, new_state} =
      Commands.execute(@op_hello, %{"client_name" => "sdk-a"}, state())

    assert status == :ok
    assert payload.protocol == "ferricstore-native"
    assert is_binary(payload.route.host)
    assert is_integer(payload.route.native_port)
    assert payload.route.endpoint.host == payload.route.host
    assert payload.route.endpoint.native_port == payload.route.native_port
    refute Map.has_key?(payload.route, String.to_atom("resp" <> "_port"))

    policy_fields = payload.capabilities.schemas["FLOW.POLICY.SET"]["fields"]
    assert "expected_generation" in policy_fields
    assert "replace" in policy_fields
    assert payload.capabilities.flow_query.language_versions == ["FQL1"]

    assert new_state.client_name == "sdk-a"
  end

  test "FLOW_WAKE fails closed without shared tenant authority" do
    ctx = shared_wake_context()

    assert {:bad_request, error, _state} =
             Commands.execute(
               @op_subscribe_events,
               %{
                 "events" => ["FLOW_WAKE"],
                 "flow_wake" => %{"type" => "email"}
               },
               wake_state(ctx)
             )

    assert error =~ "scope"
    assert ClaimWaiters.total_count() == 0
  end

  test "FLOW_WAKE broad subscriptions do not observe another shared tenant" do
    ctx = shared_wake_context()

    assert {:ok, _payload, subscribed_state} =
             Commands.execute(
               @op_subscribe_events,
               %{
                 "events" => ["FLOW_WAKE"],
                 "flow_wake" => %{"type" => "email"},
                 "request_context" => %{"tenant" => "tenant-a"}
               },
               wake_state(ctx)
             )

    assert %{keys: [_one_key]} = subscribed_state.flow_wake_subscription
    tenant_b_partition = scoped_auto_partition(22)
    tenant_a_partition = scoped_auto_partition(11)

    assert ClaimWaiters.notify_ready("email", "queued", 0, tenant_b_partition, 1) == 0
    refute_receive {:flow_claim_due_wake, _key}, 25

    assert ClaimWaiters.notify_ready("email", "queued", 0, tenant_a_partition, 1) == 1
    assert_receive {:flow_claim_due_wake, _key}, 100
  end

  test "compact FLOW.GET pipelines preserve shared tenant scope" do
    ctx = shared_wake_context()
    tenant_a = Map.put(ctx, :request_context, %{"tenant" => "tenant-a"})
    tenant_b = Map.put(ctx, :request_context, %{"tenant" => "tenant-b"})
    id = "native-shared-flow"
    partition = "native-shared-partition"

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

    for {tenant, expected_type} <- [
          {"tenant-a", "tenant-a-type"},
          {"tenant-b", "tenant-b-type"}
        ] do
      assert {:ok, [["ok", %{} = record]], _state} =
               Commands.execute(
                 @op_pipeline,
                 %{
                   "return" => "pairs",
                   "request_context" => %{"tenant" => tenant},
                   "compact_pipeline" => {16, [{:flow_get, id, [partition_key: partition]}]}
                 },
                 wake_state(ctx)
               )

      assert record.type == expected_type
      assert record.partition_key == partition
      refute Map.has_key?(record, :system_metadata)
    end

    assert {:ok,
            [
              %{
                "status" => "ok",
                "value" => %{type: "tenant-a-type", partition_key: ^partition}
              }
            ], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "request_context" => %{"tenant" => "tenant-a"},
                 "commands" => [
                   %{
                     "opcode" => @op_flow_get,
                     "request_id" => 1,
                     "body" => %{"id" => id, "partition_key" => partition}
                   }
                 ]
               },
               wake_state(ctx)
             )

    assert {:ok, %{type: "tenant-b-type", partition_key: ^partition}, _state} =
             Commands.execute(
               @op_flow_get,
               %{
                 "id" => id,
                 "partition_key" => partition,
                 "request_context" => %{"tenant" => "tenant-b"}
               },
               wake_state(ctx)
             )
  end

  @tag :native_client_name_limit
  test "HELLO bounds retained client names and requires UTF-8" do
    max_name = String.duplicate("n", 1_024)

    assert {:ok, _payload, %{client_name: ^max_name}} =
             Commands.execute(@op_hello, %{"client_name" => max_name}, state())

    assert {:bad_request, oversized_error, %{client_name: nil}} =
             Commands.execute(
               @op_hello,
               %{"client_name" => max_name <> "n"},
               state()
             )

    assert oversized_error =~ "client name exceeds 1024 bytes"

    assert {:bad_request, utf8_error, %{client_name: nil}} =
             Commands.execute(@op_hello, %{"client_name" => <<255>>}, state())

    assert utf8_error =~ "valid UTF-8"

    assert {:bad_request, startup_error, %{client_name: nil}} =
             Commands.execute(@op_startup, %{"driver_name" => max_name <> "n"}, state())

    assert startup_error =~ "driver name exceeds 1024 bytes"

    assert {:bad_request, setname_error, %{client_name: nil}} =
             Commands.execute(@op_client_set_name, %{"name" => <<255>>}, state())

    assert setname_error =~ "valid UTF-8"
  end

  test "HELLO redacts native endpoints before authentication is complete" do
    {status, payload, _state} =
      Commands.execute(
        @op_hello,
        %{"client_name" => "sdk-a"},
        state(%{require_auth: true, authenticated: false, acl_cache: nil})
      )

    assert status == :ok
    assert payload.auth_required == true
    assert payload.route.slots == 1024
    assert payload.route.shard_count >= 1
    refute Map.has_key?(payload.route, :host)
    refute Map.has_key?(payload.route, :native_host)
    refute Map.has_key?(payload.route, :native_port)
    refute Map.has_key?(payload.route, :endpoint)
  end

  test "native AUTH does not reveal whether an ACL username exists" do
    assert :ok = Acl.set_user("known_auth_user", ["on", ">secret"])

    assert {:auth, known_error, _state} =
             Commands.execute(
               @op_auth,
               %{"username" => "known_auth_user", "password" => "wrong"},
               state()
             )

    assert {:auth, missing_error, _state} =
             Commands.execute(
               @op_auth,
               %{"username" => "missing_auth_user", "password" => "wrong"},
               state()
             )

    assert known_error == missing_error
    assert known_error =~ "WRONGPASS"
  end

  test "native AUTH rate limits repeated password verification by peer IP" do
    previous_max_attempts = Application.get_env(:ferricstore, :auth_rate_limit_max_attempts)
    previous_window_ms = Application.get_env(:ferricstore, :auth_rate_limit_window_ms)

    Application.put_env(:ferricstore, :auth_rate_limit_max_attempts, 1)
    Application.put_env(:ferricstore, :auth_rate_limit_window_ms, 60_000)
    :ok = AuthRateLimiter.reset()

    on_exit(fn ->
      restore_env(:auth_rate_limit_max_attempts, previous_max_attempts)
      restore_env(:auth_rate_limit_window_ms, previous_window_ms)
      AuthRateLimiter.reset()
    end)

    assert :ok = Acl.set_user("rate_limited_auth_user", ["on", ">secret"])
    state = state(%{peer: {{10, 20, 30, 40}, 12_345}})

    assert {:auth, first_error, _state} =
             Commands.execute(
               @op_auth,
               %{"username" => "rate_limited_auth_user", "password" => "wrong"},
               state
             )

    assert first_error =~ "WRONGPASS"

    assert {:auth, limited_error, _state} =
             Commands.execute(
               @op_auth,
               %{"username" => "rate_limited_auth_user", "password" => "wrong"},
               %{state | peer: {{10, 20, 30, 40}, 54_321}}
             )

    assert limited_error =~ "too many authentication attempts"
  end

  test "successful native AUTH does not consume the failure budget" do
    previous_max_attempts = Application.get_env(:ferricstore, :auth_rate_limit_max_attempts)
    username = "successful_rate_limited_auth_user"

    Application.put_env(:ferricstore, :auth_rate_limit_max_attempts, 1)
    :ok = AuthRateLimiter.reset()
    assert :ok = Acl.set_user(username, ["on", ">secret"])

    on_exit(fn ->
      Acl.del_user(username)
      restore_env(:auth_rate_limit_max_attempts, previous_max_attempts)
      AuthRateLimiter.reset()
    end)

    state = state(%{peer: {{10, 20, 31, 40}, 12_345}})

    for port <- [12_345, 54_321] do
      assert {:ok, "OK", _state} =
               Commands.execute(
                 @op_auth,
                 %{"username" => username, "password" => "secret"},
                 %{state | peer: {{10, 20, 31, 40}, port}}
               )
    end
  end

  test "native AUTH rejects oversized credentials before authentication" do
    assert {:auth, username_error, _state} =
             Commands.execute(
               @op_auth,
               %{"username" => :binary.copy("u", 1_025), "password" => "password"},
               state()
             )

    assert username_error =~ "username exceeds 1024 bytes"
    refute username_error =~ "too many authentication attempts"

    assert {:auth, password_error, _state} =
             Commands.execute(
               @op_auth,
               %{"username" => "default", "password" => :binary.copy("p", 4_097)},
               state()
             )

    assert password_error =~ "password exceeds 4096 bytes"
    refute password_error =~ "WRONGPASS"
  end

  test "ROUTE returns leader-aware native endpoint metadata" do
    {status, payload, _state} =
      Commands.execute(@op_route, %{"key" => "{sdk-route}:a"}, state())

    assert status == :ok
    assert payload.slot in 0..1023
    assert payload.lane_id == payload.shard + 1
    assert is_binary(payload.owner_node)
    assert is_binary(payload.leader_node)
    assert payload.owner_node == payload.leader_node
    assert is_binary(payload.native_host)
    assert is_integer(payload.native_port)
    assert payload.endpoint.node == payload.leader_node
    assert payload.endpoint.host == payload.native_host
    assert payload.endpoint.native_port == payload.native_port
    assert payload.hint in ["leader", "remote_leader", "local"]
  end

  test "SHARDS returns leader-aware endpoint metadata per slot range" do
    {status, payload, _state} = Commands.execute(@op_shards, %{}, state())

    assert status == :ok
    assert payload.slots == 1024
    assert is_list(payload.ranges)
    assert [range | _] = payload.ranges
    assert range.first_slot <= range.last_slot
    assert range.lane_id == range.shard + 1
    assert range.owner_node == range.leader_node
    assert range.endpoint.node == range.leader_node
    assert range.endpoint.host == range.native_host
    assert range.endpoint.native_port == range.native_port
    assert range.hint in ["leader", "remote_leader", "local"]
  end

  test "COMMAND_EXEC delegates through native AST parser" do
    {status, payload, _state} =
      Commands.execute(@op_command_exec, %{"command" => "PING", "args" => []}, state())

    assert status == :ok
    assert payload == "PONG"
  end

  test "native command exceptions are logged without exposing details to clients" do
    Application.put_env(:ferricstore, :command_extensions, [RaisingExtension])

    log =
      capture_log(fn ->
        assert {:error, "ERR internal server error", _state} =
                 Commands.execute(
                   @op_command_exec,
                   %{"command" => "EXT.RAISE", "args" => []},
                   state()
                 )
      end)

    assert log =~ "native command execution failed"
    assert log =~ "extension-internal-secret"
  end

  test "metrics exceptions return an error instead of empty success" do
    metrics_handler = fn "FERRICSTORE.METRICS", [] -> raise("metrics-internal-secret") end
    state = state(%{metrics_handler: metrics_handler})

    log =
      capture_log(fn ->
        for {opcode, payload} <- [
              {@op_ferricstore_metrics, %{}},
              {@op_command_exec, %{"command" => "FERRICSTORE.METRICS", "args" => []}}
            ] do
          assert {:error, "ERR internal server error", _state} =
                   Commands.execute(opcode, payload, state)
        end
      end)

    assert log =~ "metrics-internal-secret"
  end

  test "metrics exits return an error instead of terminating the request" do
    metrics_handler = fn "FERRICSTORE.METRICS", [] -> exit(:metrics_collector_unavailable) end

    log =
      capture_log(fn ->
        assert {:error, "ERR internal server error", _state} =
                 Commands.execute(
                   @op_ferricstore_metrics,
                   %{},
                   state(%{metrics_handler: metrics_handler})
                 )
      end)

    assert log =~ "metrics_collector_unavailable"
  end

  test "native admin args reject structured values without raising" do
    assert {:bad_request, "ERR native field args contains an unsupported value", _state} =
             Commands.execute(@op_ferricstore_metrics, %{"args" => [%{}]}, state())
  end

  test "FLOW.POLICY.SET accepts indexed attributes through native opcode" do
    type = "native-policy-indexes-#{System.unique_integer([:positive, :monotonic])}"

    {status, payload, _state} =
      Commands.execute(
        @op_flow_policy_set,
        %{
          "type" => type,
          "indexed_attributes" => ["tenant", "region"],
          "indexed_state_meta" => "version",
          "retry" => %{"max_retries" => 5}
        },
        state()
      )

    assert status == :ok
    assert payload.indexed_attributes == ["tenant", "region"]
    assert payload.indexed_state_meta == "version"
    assert payload.retry.max_retries == 5
  end

  test "FLOW.POLICY.SET accepts max_active_ms through native opcode" do
    type = "native-policy-active-limit-#{System.unique_integer([:positive, :monotonic])}"

    assert {:ok, %{max_active_ms: 30_000}, _state} =
             Commands.execute(
               @op_flow_policy_set,
               %{"type" => type, "max_active_ms" => 30_000},
               state()
             )
  end

  test "FLOW.POLICY.SET supports patch, replacement, and generation CAS through native opcode" do
    type = "native-policy-cas-#{System.unique_integer([:positive, :monotonic])}"

    assert {:ok,
            %{
              generation: 1,
              max_active_ms: 1_000,
              states: %{"queued" => %{mode: :fifo}}
            }, _state} =
             Commands.execute(
               @op_flow_policy_set,
               %{
                 "type" => type,
                 "max_active_ms" => 1_000,
                 "states" => %{"queued" => %{"mode" => "fifo"}}
               },
               state()
             )

    assert {:ok,
            %{
              generation: 2,
              max_active_ms: 2_000,
              states: %{"queued" => %{mode: :fifo}}
            }, _state} =
             Commands.execute(
               @op_flow_policy_set,
               %{
                 "type" => type,
                 "expected_generation" => 1,
                 "replace" => false,
                 "max_active_ms" => 2_000
               },
               state()
             )

    assert {:error, "ERR stale flow policy generation", _state} =
             Commands.execute(
               @op_flow_policy_set,
               %{
                 "type" => type,
                 "expected_generation" => 1,
                 "max_active_ms" => 3_000
               },
               state()
             )

    assert {:ok, %{generation: 3, max_active_ms: nil, states: %{}}, _state} =
             Commands.execute(
               @op_flow_policy_set,
               %{
                 "type" => type,
                 "expected_generation" => 2,
                 "replace" => true
               },
               state()
             )
  end

  test "native Flow creation opcodes accept max_active_ms" do
    suffix = System.unique_integer([:positive, :monotonic])
    create_id = "native-active-create-#{suffix}"
    many_id = "native-active-create-many-#{suffix}"
    start_id = "native-active-start-#{suffix}"
    partition = "native-active-partition-#{suffix}"

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create,
               %{
                 "id" => create_id,
                 "type" => "native-active",
                 "state" => "queued",
                 "now_ms" => 1_000,
                 "max_active_ms" => 10_000
               },
               state()
             )

    assert {:ok, created} = FerricStore.flow_get(create_id)
    assert created.max_active_ms == 10_000

    assert {:ok, _payload, _state} =
             Commands.execute(
               @op_flow_create_many,
               %{
                 "items" => [%{"id" => many_id, "max_active_ms" => 20_000}],
                 "partition_key" => partition,
                 "type" => "native-active",
                 "state" => "queued",
                 "now_ms" => 1_000
               },
               state()
             )

    assert {:ok, created_many} =
             FerricStore.flow_get(many_id, partition_key: partition)

    assert created_many.max_active_ms == 20_000

    assert {:ok, started, _state} =
             Commands.execute(
               @op_flow_start_and_claim,
               %{
                 "id" => start_id,
                 "type" => "native-active",
                 "initial_state" => "queued",
                 "worker" => "native-worker",
                 "now_ms" => 1_000,
                 "max_active_ms" => 30_000
               },
               state()
             )

    assert started.max_active_ms == 30_000
  end

  test "native FLOW.CREATE preserves canonical lineage and creation semantics" do
    suffix = System.unique_integer([:positive, :monotonic])
    id = "native-lineage-child-#{suffix}"
    parent_id = "native-lineage-parent-#{suffix}"
    root_id = "native-lineage-root-#{suffix}"

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create,
               %{
                 "id" => id,
                 "type" => "native-lineage",
                 "parent_flow_id" => parent_id,
                 "root_flow_id" => root_id,
                 "idempotent" => true,
                 "max_active_ms" => 60_000,
                 "history_hot_max_events" => 100,
                 "history_max_events" => 1_000,
                 "now_ms" => 1_000
               },
               state()
             )

    assert {:ok, record} = FerricStore.flow_get(id)
    assert record.parent_flow_id == parent_id
    assert record.root_flow_id == root_id
    assert record.max_active_ms == 60_000
    assert record.history_hot_max_events == 100
    assert record.history_max_events == 1_000
  end

  test "FLOW.SPAWN_CHILDREN accepts max_active_ms in child payloads" do
    suffix = System.unique_integer([:positive, :monotonic])
    parent_id = "native-active-parent-#{suffix}"
    child_id = "native-active-child-#{suffix}"
    partition = "native-active-family-#{suffix}"

    assert :ok =
             FerricStore.flow_create(parent_id,
               type: "native-parent",
               state: "dispatch",
               partition_key: partition,
               now_ms: 1_000
             )

    assert {:ok, parent} = FerricStore.flow_get(parent_id, partition_key: partition)

    assert {:ok, _payload, _state} =
             Commands.execute(
               @op_flow_spawn_children,
               %{
                 "id" => parent_id,
                 "children" => [
                   %{
                     "id" => child_id,
                     "type" => "native-child",
                     "max_active_ms" => 40_000
                   }
                 ],
                 "partition_key" => partition,
                 "group_id" => "native-group",
                 "wait" => "none",
                 "success" => "dispatched",
                 "failure" => "dispatch_failed",
                 "from_state" => "dispatch",
                 "fencing_token" => parent.fencing_token,
                 "now_ms" => 1_010
               },
               state()
             )

    assert {:ok, child} = FerricStore.flow_get(child_id, partition_key: partition)
    assert child.max_active_ms == 40_000
  end

  test "native Flow opcodes enforce FIFO lane claims" do
    suffix = System.unique_integer([:positive, :monotonic])
    type = "native-fifo-#{suffix}"
    partition = "native:fifo:#{suffix}:partition"
    first_id = "z-native-fifo-first:#{suffix}"
    second_id = "a-native-fifo-second:#{suffix}"

    {status, policy, _state} =
      Commands.execute(
        @op_flow_policy_set,
        %{
          "type" => type,
          "states" => %{"queued" => %{"mode" => "fifo"}}
        },
        state()
      )

    assert status == :ok
    assert policy.states["queued"].mode == :fifo

    for {id, now_ms} <- [{first_id, 1_000}, {second_id, 1_000}] do
      {status, payload, _state} =
        Commands.execute(
          @op_flow_create,
          %{
            "id" => id,
            "type" => type,
            "state" => "queued",
            "partition_key" => partition,
            "payload" => id,
            "now_ms" => now_ms,
            "run_at_ms" => 2_000
          },
          state()
        )

      assert status == :ok
      assert payload == "OK"
    end

    {status, claimed, _state} =
      Commands.execute(
        @op_flow_claim_due,
        %{
          "type" => type,
          "state" => "queued",
          "partition_key" => partition,
          "worker" => "native-fifo-worker",
          "limit" => 10,
          "now_ms" => 2_000
        },
        state()
      )

    assert status == :ok
    assert [%{id: ^first_id}] = claimed

    {status, claimed, _state} =
      Commands.execute(
        @op_flow_claim_due,
        %{
          "type" => type,
          "state" => "queued",
          "partition_key" => partition,
          "worker" => "native-fifo-worker",
          "limit" => 10,
          "now_ms" => 2_001
        },
        state()
      )

    assert status == :ok
    assert claimed == []
  end

  test "FLOW.SEARCH returns indexed records through COMMAND_EXEC and native opcode" do
    suffix = System.unique_integer([:positive, :monotonic])
    type = "native-search-#{suffix}"
    id = "native:search:#{suffix}"
    partition = "native:search:#{suffix}:partition"
    marker = "marker-#{suffix}"
    now = System.system_time(:millisecond)

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               indexed_attributes: ["search_marker"],
               indexed_state_meta: "version"
             )

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "searchable",
               partition_key: partition,
               attributes: %{"search_marker" => marker},
               state_meta: %{"version" => "1"},
               idempotent: true,
               run_at_ms: now,
               now_ms: now
             )

    command_exec_payload = %{
      "command" => "FLOW.SEARCH",
      "args" => [
        "TYPE",
        type,
        "STATE",
        "searchable",
        "ATTRIBUTE",
        "search_marker",
        marker,
        "STATE_META",
        "searchable",
        "version",
        "1",
        "PARTITION",
        partition,
        "COUNT",
        "10",
        "CONSISTENT_PROJECTION",
        "true"
      ]
    }

    assert {:ok, command_records, _state} =
             Commands.execute(@op_command_exec, command_exec_payload, state())

    assert id in flow_record_ids(command_records)

    native_payload = %{
      "type" => type,
      "state" => "searchable",
      "partition_key" => partition,
      "attributes" => %{"search_marker" => marker},
      "state_meta" => %{"searchable" => %{"version" => "1"}},
      "count" => 10,
      "consistent_projection" => true
    }

    assert {:ok, native_records, _state} =
             Commands.execute(@op_flow_search, native_payload, state())

    assert id in flow_record_ids(native_records)
  end

  test "FLOW.QUERY executes a parameterized run lookup through the point-read path" do
    suffix = System.unique_integer([:positive, :monotonic])
    id = "native-query-#{suffix}"
    partition = "native-query-partition-#{suffix}"

    assert :ok =
             FerricStore.flow_create(id,
               type: "native-query",
               state: "ready",
               partition_key: partition,
               now_ms: 1_000
             )

    query =
      "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"

    assert {:ok, native_record, _state} =
             Commands.execute(
               @op_flow_query,
               %{
                 "version" => "FQL1",
                 "query" => query,
                 "params" => %{"partition" => partition, "flow_id" => id}
               },
               state()
             )

    assert native_record.id == id

    assert {:ok, command_record, _state} =
             Commands.execute(
               @op_command_exec,
               %{
                 "command" => "FLOW.QUERY",
                 "args" => ["FQL1", query, "partition", partition, "flow_id", id]
               },
               state()
             )

    assert command_record.id == id
  end

  test "FLOW.QUERY preserves partition isolation for identical run IDs" do
    suffix = System.unique_integer([:positive, :monotonic])
    id = "native-query-shared-id-#{suffix}"
    partition_a = "native-query-partition-a-#{suffix}"
    partition_b = "native-query-partition-b-#{suffix}"

    assert :ok =
             FerricStore.flow_create(id,
               type: "partition-a",
               state: "ready",
               partition_key: partition_a,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_create(id,
               type: "partition-b",
               state: "waiting",
               partition_key: partition_b,
               now_ms: 2_000
             )

    query =
      "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"

    for {partition, expected_type, expected_state} <- [
          {partition_a, "partition-a", "ready"},
          {partition_b, "partition-b", "waiting"}
        ] do
      assert {:ok, record, _state} =
               Commands.execute(
                 @op_flow_query,
                 %{
                   "version" => "FQL1",
                   "query" => query,
                   "params" => %{"partition" => partition, "flow_id" => id}
                 },
                 state()
               )

      assert record.partition_key == partition
      assert record.type == expected_type
      assert record.state == expected_state
    end

    assert {:ok, nil, _state} =
             Commands.execute(
               @op_flow_query,
               %{
                 "version" => "FQL1",
                 "query" => query,
                 "params" => %{"partition" => "missing-partition", "flow_id" => id}
               },
               state()
             )
  end

  test "FLOW.QUERY executes reversed predicates and escaped literals exactly" do
    suffix = System.unique_integer([:positive, :monotonic])
    id = "native-query-'quoted'-#{suffix}"
    partition = "native-query-literal-#{suffix}"

    assert :ok =
             FerricStore.flow_create(id,
               type: "literal-query",
               state: "ready",
               partition_key: partition,
               now_ms: 1_000
             )

    escaped_id = String.replace(id, "'", "''")

    query =
      "from RUNS where RUN_ID = '#{escaped_id}' and PARTITION_KEY = '#{partition}' return record;"

    assert {:ok, record, _state} =
             Commands.execute(
               @op_flow_query,
               %{"version" => "FQL1", "query" => query},
               state()
             )

    assert record.id == id
    assert record.partition_key == partition
  end

  test "FLOW.QUERY record projection excludes worker tokens, value refs, and arbitrary metadata" do
    suffix = System.unique_integer([:positive, :monotonic])
    id = "native-query-redaction-#{suffix}"
    partition = "native-query-redaction-partition-#{suffix}"

    assert :ok =
             FerricStore.flow_create(id,
               type: "native-query-redaction",
               state: "ready",
               partition_key: partition,
               payload: "payload-secret",
               attributes: %{"sensitive" => "attribute-secret"},
               now_ms: 1_000
             )

    assert {:ok, record, _state} =
             Commands.execute(
               @op_flow_query,
               %{
                 "version" => "FQL1",
                 "query" =>
                   "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD",
                 "params" => %{"partition" => partition, "flow_id" => id}
               },
               state()
             )

    assert record.id == id
    assert record.type == "native-query-redaction"
    assert record.state == "ready"

    for field <- [
          :lease_token,
          :lease_owner,
          :fencing_token,
          :payload_ref,
          :result_ref,
          :error_ref,
          :value_refs,
          :attributes,
          :state_meta,
          :child_groups,
          :retention_ttl_ms,
          :parent_partition_key
        ] do
      refute Map.has_key?(record, field)
    end

    refute inspect(record) =~ "secret"
  end

  test "FLOW.QUERY EXPLAIN is deterministic and redacts predicate values" do
    explain = fn id ->
      partition = "partition-for-#{id}"

      assert {:ok, payload, _state} =
               Commands.execute(
                 @op_flow_query,
                 %{
                   "version" => "FQL1",
                   "query" =>
                     "EXPLAIN FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD",
                   "params" => %{"partition" => partition, "flow_id" => id}
                 },
                 state()
               )

      payload
    end

    first = explain.("secret-run-one")
    second = explain.("secret-run-two")

    assert first == second
    assert first.version == "ferric.flow.query.point-explain/v1"
    assert first.status == "planned"

    assert first.capabilities == %{
             requested: [],
             available: ["flow_query_point_v1"],
             missing: []
           }

    assert first.plan.path == "primary_key"
    assert first.plan.index == "flow_runs_primary_v1"
    assert first.plan.fallback_reason == "none"
    assert first.estimate.scan_records == 1
    assert first.estimate.result_records == 1
    assert first.bounds == %{scan_records: 1, result_records: 1, groups: 0}
    refute Map.has_key?(first.estimate, :response_bytes)
    refute Map.has_key?(first, :quality)
    refute Map.has_key?(first, :budgets)

    assert first |> Map.keys() |> Enum.sort() ==
             Enum.sort([
               :version,
               :query_fingerprint,
               :status,
               :capabilities,
               :plan,
               :estimate,
               :bounds
             ])

    refute inspect(first) =~ "secret-run"
  end

  test "FLOW.QUERY rejects versions and unsupported shapes before storage access" do
    assert {:bad_request, version_error, _state} =
             Commands.execute(
               @op_flow_query,
               %{"version" => "FQL2", "query" => "not valid FQL1"},
               state()
             )

    assert version_error["code"] == "unsupported_query_version"

    assert {:bad_request, shape_error, _state} =
             Commands.execute(
               @op_flow_query,
               %{"version" => "FQL1", "query" => "FROM runs RETURN RECORDS"},
               state()
             )

    assert shape_error["code"] == "unsupported_query_shape"
  end

  test "FLOW.QUERY rejects malformed envelopes before provider dispatch" do
    query =
      "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD"

    cases = [
      %{"query" => query},
      %{"version" => 1, "query" => query},
      %{"version" => "FQL1"},
      %{"version" => "FQL1", "query" => 123},
      %{"version" => "FQL1", "query" => query, "params" => []},
      %{"version" => "FQL1", "query" => query, "unknown" => "secret"}
    ]

    for payload <- cases do
      assert {status, _error, _state} =
               Commands.execute(
                 @op_flow_query,
                 payload,
                 state_with_query_engine(NotifyingQueryEngine)
               )

      assert status in [:bad_request, :error]
      refute_received :query_engine_called
    end
  end

  test "COMMAND_EXEC FLOW.QUERY preserves named-parameter envelope errors" do
    query =
      "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"

    cases = [
      {["FQL1", query, "partition"], "parameters must be name/value pairs"},
      {["FQL1", query, "partition", "one", "partition", "two"], "parameter names must be unique"},
      {["FQL1", query, "one", "1", "two", "2", "three", "3"], "unexpected parameter"}
    ]

    for {args, expected} <- cases do
      assert {:bad_request, message, _state} =
               Commands.execute(
                 @op_command_exec,
                 %{"command" => "FLOW.QUERY", "args" => args},
                 state()
               )

      assert message =~ expected
      refute message =~ "wrong number of arguments"
    end
  end

  test "FLOW.QUERY rejects malformed deadlines before invoking its engine" do
    query =
      "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD"

    for invalid_deadline <- ["soon", -1, 1.5, nil, true] do
      assert {:bad_request, payload, _state} =
               Commands.execute(
                 @op_flow_query,
                 %{
                   "version" => "FQL1",
                   "query" => query,
                   "deadline_ms" => invalid_deadline
                 },
                 state_with_query_engine(ContextQueryEngine)
               )

      assert payload["code"] == "invalid_deadline"
      refute inspect(payload) =~ inspect(invalid_deadline)
    end
  end

  test "FLOW.QUERY carries absolute deadlines to both query execution paths" do
    deadline_ms = System.system_time(:millisecond) + 30_000

    query =
      "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD"

    query_state =
      state_with_query_engine(DeadlineQueryEngine)
      |> Map.put(:trusted_request_context_users, ["default"])

    expected = %{
      deadline_ms: deadline_ms,
      mode: :execute,
      request_context: %{"tenant" => "tenant-a"}
    }

    assert {:ok, ^expected, _state} =
             Commands.execute(
               @op_flow_query,
               %{
                 "version" => "FQL1",
                 "query" => query,
                 "deadline_ms" => deadline_ms,
                 "request_context" => %{"tenant" => "tenant-a"}
               },
               query_state
             )

    assert {:ok, ^expected, _state} =
             Commands.execute(
               @op_command_exec,
               %{
                 "command" => "FLOW.QUERY",
                 "args" => ["FQL1", query],
                 "deadline_ms" => deadline_ms,
                 "request_context" => %{"tenant" => "tenant-a"}
               },
               query_state
             )

    assert {:ok, %{deadline_ms: nil}, _state} =
             Commands.execute(
               @op_flow_query,
               %{"version" => "FQL1", "query" => query},
               query_state
             )
  end

  test "FLOW.QUERY passes only trusted request context to the installed query engine" do
    Application.put_env(:ferricstore, :native_trusted_request_context_users, ["default"])
    query_state = state_with_query_engine(ContextQueryEngine)

    query =
      "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"

    assert {:ok, payload, _state} =
             Commands.execute(
               @op_flow_query,
               %{
                 "version" => "FQL1",
                 "query" => query,
                 "params" => %{"partition" => "tenant-a", "flow_id" => "run-123"},
                 "request_context" => %{
                   "subject" => "client-1",
                   "tenant" => "tenant-a",
                   "ignored" => "not-trusted"
                 }
               },
               query_state
             )

    assert payload == %{
             mode: :execute,
             request_context: %{"subject" => "client-1", "tenant" => "tenant-a"}
           }

    assert {:ok,
            [
              %{
                "status" => "ok",
                "value" => %{
                  mode: :execute,
                  request_context: %{"subject" => "client-1", "tenant" => "tenant-a"}
                }
              }
            ], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "request_context" => %{"subject" => "client-1", "tenant" => "tenant-a"},
                 "commands" => [
                   %{
                     "opcode" => @op_flow_query,
                     "lane_id" => 1,
                     "request_id" => 7,
                     "body" => %{
                       "version" => "FQL1",
                       "query" => query,
                       "params" => %{
                         "partition" => "tenant-a",
                         "flow_id" => "run-123"
                       }
                     }
                   }
                 ]
               },
               query_state
             )
  end

  test "FLOW.QUERY passes trusted authority without copying the instance context" do
    query =
      "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD"

    query_state =
      state_with_query_engine(CompactContextQueryEngine)
      |> Map.put(:trusted_request_context_users, ["default"])

    assert {:ok, payload, _state} =
             Commands.execute(
               @op_flow_query,
               %{
                 "version" => "FQL1",
                 "query" => query,
                 "request_context" => %{"tenant" => "tenant-a"}
               },
               query_state
             )

    assert payload == %{
             compact: true,
             instance_name: query_state.instance_ctx.name,
             mode: :execute,
             request_context: %{"tenant" => "tenant-a"}
           }

    assert {:ok, ^payload, _state} =
             Commands.execute(
               @op_command_exec,
               %{
                 "command" => "FLOW.QUERY",
                 "args" => ["FQL1", query],
                 "request_context" => %{"tenant" => "tenant-a"}
               },
               query_state
             )
  end

  test "FLOW.QUERY uses the connection-frozen request-context trust policy" do
    query =
      "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD"

    Application.put_env(:ferricstore, :native_trusted_request_context_users, [])

    trusted_state =
      state_with_query_engine(ContextQueryEngine)
      |> Map.put(:trusted_request_context_users, ["default"])

    assert {:ok, %{request_context: %{"tenant" => "tenant-a"}}, _state} =
             Commands.execute(
               @op_flow_query,
               %{
                 "version" => "FQL1",
                 "query" => query,
                 "request_context" => %{"tenant" => "tenant-a"}
               },
               trusted_state
             )

    Application.put_env(:ferricstore, :native_trusted_request_context_users, ["default"])

    untrusted_state =
      state_with_query_engine(ContextQueryEngine)
      |> Map.put(:trusted_request_context_users, [])

    assert {:ok, %{request_context: %{}}, _state} =
             Commands.execute(
               @op_flow_query,
               %{
                 "version" => "FQL1",
                 "query" => query,
                 "request_context" => %{"tenant" => "tenant-a"}
               },
               untrusted_state
             )
  end

  test "FLOW.QUERY bounds trusted request context before provider dispatch" do
    Application.put_env(:ferricstore, :native_trusted_request_context_users, ["default"])

    query =
      "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD"

    contexts = [
      %{"tenant" => String.duplicate("t", 4_097)},
      %{"scopes" => List.duplicate("tenant:a:read", 65)},
      %{"scopes" => [String.duplicate("s", 1_025)]}
    ]

    for context <- contexts do
      assert {:bad_request, message, _state} =
               Commands.execute(
                 @op_flow_query,
                 %{
                   "version" => "FQL1",
                   "query" => query,
                   "request_context" => context
                 },
                 state_with_query_engine(ContextQueryEngine)
               )

      assert message == "ERR native request_context exceeds limits"
      refute message =~ "tenant:a"
    end
  end

  test "FLOW.QUERY preserves installed engine authorization failures" do
    assert {:noperm, payload, _state} =
             Commands.execute(
               @op_flow_query,
               %{
                 "version" => "FQL1",
                 "query" =>
                   "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD"
               },
               state_with_query_engine(RejectingQueryEngine)
             )

    assert payload["code"] == "unauthorized_scope"
    assert payload["message"] == "NOPERM Flow query scope is not authorized"
    refute inspect(payload) =~ "tenant-a"
  end

  test "FLOW.QUERY contains installed engine failures at the native boundary" do
    query =
      "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD"

    for {opcode, payload} <- [
          {@op_flow_query, %{"version" => "FQL1", "query" => query}},
          {@op_command_exec, %{"command" => "FLOW.QUERY", "args" => ["FQL1", query]}}
        ] do
      assert {:error, error, _state} =
               Commands.execute(opcode, payload, state_with_query_engine(RaisingQueryEngine))

      assert error["code"] == "query_engine_failure"
      assert error["message"] == "ERR Flow query engine failed"
      refute inspect(error) =~ "secret"
    end
  end

  test "FLOW.QUERY keeps structured failures consistent in pipelines" do
    query = "FROM runs RETURN RECORDS"

    assert {:bad_request, direct_error, query_state} =
             Commands.execute(
               @op_flow_query,
               %{"version" => "FQL1", "query" => query},
               state()
             )

    assert {:bad_request, raw_error, query_state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "FLOW.QUERY", "args" => ["FQL1", query]},
               query_state
             )

    assert direct_error == raw_error

    assert {:ok, [pipeline_result], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "commands" => [
                   %{
                     "opcode" => @op_flow_query,
                     "lane_id" => 1,
                     "request_id" => 7,
                     "body" => %{"version" => "FQL1", "query" => query}
                   }
                 ]
               },
               query_state
             )

    assert pipeline_result["status"] == "bad_request"
    assert pipeline_result["value"] == direct_error
  end

  test "COMMAND_EXEC FLOW.QUERY preserves resource governance and activity accounting" do
    previous = Application.get_env(:ferricstore, FerricStore.ResourceLimits)
    Application.put_env(:ferricstore, FerricStore.ResourceLimits, QueryResourceLimits)

    on_exit(fn ->
      case previous do
        nil ->
          Application.delete_env(:ferricstore, FerricStore.ResourceLimits)

        implementation ->
          Application.put_env(:ferricstore, FerricStore.ResourceLimits, implementation)
      end
    end)

    query =
      "FROM runs WHERE partition_key = 'missing' AND run_id = 'missing' RETURN RECORD"

    assert {:ok, nil, query_state} =
             Commands.execute(
               @op_flow_query,
               %{"version" => "FQL1", "query" => query},
               state()
             )

    assert_received {:query_resource_check, "FLOW.QUERY", [], ["*"]}
    assert_received {:query_resource_activity, ["*"]}

    assert {:ok, nil, _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "FLOW.QUERY", "args" => ["FQL1", query]},
               query_state
             )

    assert_received {:query_resource_check, "FLOW.QUERY", [], ["*"]}
    assert_received {:query_resource_activity, ["*"]}
  end

  test "COMMAND_EXEC FLOW.QUERY rejects resource-limit failures before provider dispatch" do
    previous = Application.get_env(:ferricstore, FerricStore.ResourceLimits)
    Application.put_env(:ferricstore, FerricStore.ResourceLimits, QueryResourceLimits)
    Process.put(:query_resource_check_result, {:error, :query_rate_limited})

    on_exit(fn ->
      case previous do
        nil ->
          Application.delete_env(:ferricstore, FerricStore.ResourceLimits)

        implementation ->
          Application.put_env(:ferricstore, FerricStore.ResourceLimits, implementation)
      end
    end)

    query =
      "FROM runs WHERE partition_key = 'missing' AND run_id = 'missing' RETURN RECORD"

    assert {:error, "ERR quota query_rate_limited", _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "FLOW.QUERY", "args" => ["FQL1", query]},
               state_with_query_engine(NotifyingQueryEngine)
             )

    assert_received {:query_resource_check, "FLOW.QUERY", [], ["*"]}
    refute_received :query_engine_called
    refute_received {:query_resource_activity, _keys}
  end

  test "COMMAND_EXEC delegates configured extension commands" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "ext.read", "args" => ["tenant:1"]},
        state()
      )

    assert status == :ok
    assert payload == ["read", "tenant:1"]
  end

  test "COMMAND_EXEC ignores request context unless the native user is trusted" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{
          "command" => "EXT.CONTEXT",
          "args" => [],
          "request_context" => %{"subject" => "client-1"}
        },
        state()
      )

    assert status == :ok
    assert payload == %{}
  end

  test "COMMAND_EXEC attaches trusted request context to extension store" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])
    Application.put_env(:ferricstore, :native_trusted_request_context_users, ["default"])

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{
          "command" => "EXT.CONTEXT",
          "args" => [],
          "request_context" => %{
            "subject" => "client-1",
            "tenant" => "t1",
            "scopes" => ["tenant:t1:write", nil]
          }
        },
        state()
      )

    assert status == :ok

    assert payload == %{
             "subject" => "client-1",
             "tenant" => "t1",
             "scopes" => ["tenant:t1:write"]
           }
  end

  test "PIPELINE attaches top-level trusted request context to extension commands" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])
    Application.put_env(:ferricstore, :native_trusted_request_context_users, ["default"])

    {status, payload, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "request_context" => %{
            "subject" => "client-1",
            "tenant" => "t1",
            "scopes" => "tenant:t1:write invocation:create:*"
          },
          "commands" => [
            %{
              "opcode" => @op_command_exec,
              "lane_id" => 1,
              "request_id" => 7,
              "body" => %{"command" => "EXT.CONTEXT", "args" => []}
            }
          ]
        },
        state()
      )

    assert status == :ok

    assert [
             %{
               "request_id" => 7,
               "status" => "ok",
               "value" => %{
                 "subject" => "client-1",
                 "tenant" => "t1",
                 "scopes" => ["tenant:t1:write", "invocation:create:*"]
               }
             }
           ] = payload
  end

  test "PIPELINE rejects a structured atomicity value without crashing" do
    assert {:bad_request, "ERR native field atomicity must be binary", _state} =
             Commands.execute(
               @op_pipeline,
               %{"commands" => [], "atomicity" => %{"mode" => "none"}},
               state()
             )
  end

  test "COMMAND_EXEC authorizes extension command and key metadata" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])

    assert :ok =
             Acl.set_user("ext-reader", [
               "on",
               "nopass",
               "-@all",
               "+ext.read",
               "~tenant:*"
             ])

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "EXT.READ", "args" => ["tenant:1"]},
        state_as("ext-reader")
      )

    assert status == :ok
    assert payload == ["read", "tenant:1"]

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "EXT.READ", "args" => ["other:1"]},
        state_as("ext-reader")
      )

    assert status == :noperm
    assert payload =~ "keys mentioned"

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "EXT.WRITE", "args" => ["tenant:1"]},
        state_as("ext-reader")
      )

    assert status == :noperm
    assert payload =~ "ext.write"
  end

  test "COMMAND_EXEC prepares extension key metadata once" do
    Application.put_env(:ferricstore, :command_extensions, [CountingKeyExtension])
    counter_key = {CountingKeyExtension, :key_discovery_calls}
    Process.delete(counter_key)

    assert {:ok, "tenant:one", _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "EXT.COUNTED", "args" => ["tenant:one"]},
               state()
             )

    assert Process.get(counter_key) == 1
  after
    Process.delete({CountingKeyExtension, :key_discovery_calls})
  end

  test "same_shard pipeline validates COMMAND_EXEC routing keys" do
    ctx = FerricStore.Instance.get(:default)
    first = "prepared:pipeline:one"
    first_shard = Ferricstore.Store.Router.shard_for(ctx, first)

    second =
      Enum.find_value(2..1_000, fn suffix ->
        candidate = "prepared:pipeline:#{suffix}"

        if Ferricstore.Store.Router.shard_for(ctx, candidate) != first_shard,
          do: candidate
      end)

    assert is_binary(second)

    assert {:bad_request, message, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "atomicity" => "same_shard",
                 "commands" => [
                   %{
                     "opcode" => @op_command_exec,
                     "request_id" => 1,
                     "body" => %{"command" => "MGET", "args" => [first, second]}
                   }
                 ]
               },
               state()
             )

    assert message =~ "multiple shards"
  end

  @tag :native_response_byte_budget
  test "GET rejects an oversized cold value before materializing it" do
    ctx = FerricStore.Instance.get(:default)
    key = "native:get:cold-byte-budget:#{System.unique_integer([:positive])}"
    value = String.duplicate("x", 128)
    shard_index = Ferricstore.Store.Router.shard_for(ctx, key)
    keydir = elem(ctx.keydir_refs, shard_index)

    assert :ok = FerricStore.set(key, value)
    assert :ok = GenServer.call(elem(ctx.shard_names, shard_index), :flush, 5_000)
    on_exit(fn -> FerricStore.del(key) end)

    assert [{^key, ^value, expire_at_ms, lfu, file_id, offset, 128}] = :ets.lookup(keydir, key)
    refute file_id == :pending
    assert true = :ets.insert(keydir, {key, nil, expire_at_ms, lfu, file_id, offset, 128})

    cold_reads_before = Stats.total_cold_reads(ctx)

    for {opcode, payload} <- [
          {@op_get, %{"key" => key}},
          {@op_command_exec, %{"command" => "GET", "args" => [key]}}
        ] do
      assert {:bad_request, message, _state} =
               Commands.execute(opcode, payload, state(%{max_response_bytes: 32}))

      assert message =~ "response byte limit"
      assert Stats.total_cold_reads(ctx) == cold_reads_before
      assert [{^key, nil, ^expire_at_ms, ^lfu, ^file_id, ^offset, 128}] = :ets.lookup(keydir, key)
    end
  end

  @tag :native_response_byte_budget
  test "MGET rejects a response whose encoded bytes exceed the connection budget" do
    suffix = System.unique_integer([:positive])
    first = "native:mget:byte-budget:first:#{suffix}"
    second = "native:mget:byte-budget:second:#{suffix}"

    assert :ok = FerricStore.set(first, "12345678")
    assert :ok = FerricStore.set(second, "abcdefgh")
    on_exit(fn -> FerricStore.del([first, second]) end)

    assert {:bad_request, message, _state} =
             Commands.execute(
               @op_mget,
               %{"keys" => [first, second]},
               state(%{max_response_bytes: 32})
             )

    assert message =~ "response byte limit"
  end

  @tag :native_response_byte_budget
  test "pipeline GET fast paths reject oversized cold values before materializing them" do
    ctx = FerricStore.Instance.get(:default)
    suffix = System.unique_integer([:positive])
    key = "native:pipeline:cold-byte-budget:#{suffix}"
    set_key = "native:pipeline:cold-byte-budget:set:#{suffix}"
    value = String.duplicate("x", 128)
    shard_index = Ferricstore.Store.Router.shard_for(ctx, key)
    keydir = elem(ctx.keydir_refs, shard_index)

    assert :ok = FerricStore.set(key, value)
    assert :ok = GenServer.call(elem(ctx.shard_names, shard_index), :flush, 5_000)
    on_exit(fn -> FerricStore.del([key, set_key]) end)

    assert [{^key, ^value, expire_at_ms, lfu, file_id, offset, 128}] = :ets.lookup(keydir, key)
    assert true = :ets.insert(keydir, {key, nil, expire_at_ms, lfu, file_id, offset, 128})
    cold_reads_before = Stats.total_cold_reads(ctx)

    payloads = [
      %{
        "commands" => [
          %{"opcode" => @op_get, "request_id" => 1, "body" => %{"key" => key}}
        ]
      },
      %{"compact_pipeline" => {2, [key]}},
      %{"compact_pipeline" => {5, [{:get, key}, {:set, set_key, "written"}]}}
    ]

    for payload <- payloads do
      assert {:bad_request, message, _state} =
               Commands.execute(@op_pipeline, payload, state(%{max_response_bytes: 32}))

      assert message =~ "response byte limit"
      assert Stats.total_cold_reads(ctx) == cold_reads_before
      assert [{^key, nil, ^expire_at_ms, ^lfu, ^file_id, ^offset, 128}] = :ets.lookup(keydir, key)
    end

    assert {:ok, "written"} == FerricStore.get(set_key)
  end

  @tag :native_response_byte_budget
  test "collection reads reject oversized cold payloads before materializing members" do
    ctx = FerricStore.Instance.get(:default)
    suffix = System.unique_integer([:positive])
    hash_key = "native:hgetall:cold-byte-budget:#{suffix}"
    set_key = "native:smembers:cold-byte-budget:#{suffix}"
    hash_field = "field"
    set_member = String.duplicate("m", 128)
    hash_value = String.duplicate("v", 128)
    hash_compound_key = Ferricstore.Store.CompoundKey.hash_field(hash_key, hash_field)
    set_compound_key = Ferricstore.Store.CompoundKey.set_member(set_key, set_member)

    assert {:ok, 1} = FerricStore.Impl.hset(ctx, hash_key, %{hash_field => hash_value})
    assert {:ok, 1} = FerricStore.Impl.sadd(ctx, set_key, [set_member])
    on_exit(fn -> FerricStore.del([hash_key, set_key]) end)

    [hash_key, set_key]
    |> Enum.map(&Ferricstore.Store.Router.shard_for(ctx, &1))
    |> Enum.uniq()
    |> Enum.each(fn shard_index ->
      assert :ok = GenServer.call(elem(ctx.shard_names, shard_index), :flush, 5_000)
    end)

    cold_rows =
      Enum.map(
        [{hash_key, hash_compound_key}, {set_key, set_compound_key}],
        fn {logical_key, compound_key} ->
          shard_index = Ferricstore.Store.Router.shard_for(ctx, logical_key)
          keydir = elem(ctx.keydir_refs, shard_index)

          assert [{^compound_key, value, expire_at_ms, lfu, file_id, offset, value_size}] =
                   :ets.lookup(keydir, compound_key)

          assert is_binary(value)
          refute file_id == :pending

          assert true =
                   :ets.insert(
                     keydir,
                     {compound_key, nil, expire_at_ms, lfu, file_id, offset, value_size}
                   )

          {keydir, compound_key, expire_at_ms, lfu, file_id, offset, value_size}
        end
      )

    requests = [
      {@op_hgetall, %{"key" => hash_key}},
      {@op_smembers, %{"key" => set_key}},
      {@op_pipeline, %{"return" => "pairs", "compact_pipeline" => {30, [hash_key]}}},
      {@op_pipeline, %{"return" => "pairs", "compact_pipeline" => {27, [set_key]}}}
    ]

    for {opcode, payload} <- requests do
      assert {:bad_request, message, _state} =
               Commands.execute(opcode, payload, state(%{max_response_bytes: 32}))

      assert message =~ "response byte limit"

      Enum.each(cold_rows, fn {keydir, compound_key, exp, lfu, fid, off, size} ->
        assert [{^compound_key, nil, ^exp, ^lfu, ^fid, ^off, ^size}] =
                 :ets.lookup(keydir, compound_key)
      end)
    end
  end

  @tag :compact_zrange_metadata
  test "compact ZRANGE checks a cold string's metadata without reading its value" do
    ctx = FerricStore.Instance.get(:default)
    key = "native:compact-zrange:cold-string:#{System.unique_integer([:positive])}"
    value = String.duplicate("x", 128)
    shard_index = Ferricstore.Store.Router.shard_for(ctx, key)
    keydir = elem(ctx.keydir_refs, shard_index)

    assert :ok = FerricStore.set(key, value)
    assert :ok = GenServer.call(elem(ctx.shard_names, shard_index), :flush, 5_000)
    on_exit(fn -> FerricStore.del(key) end)

    assert [{^key, ^value, expire_at_ms, lfu, file_id, offset, 128}] = :ets.lookup(keydir, key)
    refute file_id == :pending
    assert true = :ets.insert(keydir, {key, nil, expire_at_ms, lfu, file_id, offset, 128})

    cold_reads_before = Stats.total_cold_reads(ctx)

    assert {:ok, [["error", message]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, [{key, 0, -1, false}]}},
               state()
             )

    assert message =~ "WRONGTYPE"
    assert Stats.total_cold_reads(ctx) == cold_reads_before
    assert [{^key, nil, ^expire_at_ms, ^lfu, ^file_id, ^offset, 128}] = :ets.lookup(keydir, key)
  end

  test "compact ZRANGE ignores a ready index after the type marker expires" do
    ctx = FerricStore.Instance.get(:default)
    key = "native:compact-zrange:expired-index:#{System.unique_integer([:positive])}"
    shard_index = Ferricstore.Store.Router.shard_for(ctx, key)
    keydir = elem(ctx.keydir_refs, shard_index)
    type_key = Ferricstore.Store.CompoundKey.type_key(key)

    {index, lookup} =
      Ferricstore.Store.Shard.ZSetIndex.table_names(ctx.name, shard_index)

    :ok = Ferricstore.Store.Shard.ZSetIndex.mark_ready_empty(index, lookup, key)
    :ok = Ferricstore.Store.Shard.ZSetIndex.put_member(index, lookup, key, "stale", "1")

    true =
      :ets.insert(
        keydir,
        {type_key, "zset", System.os_time(:millisecond) - 1, 1, 0, 0, byte_size("zset")}
      )

    on_exit(fn ->
      Ferricstore.Store.Shard.ZSetIndex.clear_key(index, lookup, key)
      :ets.delete(keydir, type_key)
    end)

    assert {:ok, [["ok", []]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, [{key, 0, -1, false}]}},
               state()
             )
  end

  @tag :prepared_flow_routing
  test "same_shard pipeline rejects coordinated COMMAND_EXEC Flow routing" do
    type = "prepared-flow-routing-#{System.unique_integer([:positive, :monotonic])}"

    assert {:bad_request, message, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "atomicity" => "same_shard",
                 "commands" => [
                   %{
                     "opcode" => @op_command_exec,
                     "request_id" => 1,
                     "body" => %{
                       "command" => "FLOW.POLICY.GET",
                       "args" => [type]
                     }
                   }
                 ]
               },
               state()
             )

    assert message =~ "coordinated"

    assert {:bad_request, native_message, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "atomicity" => "same_shard",
                 "commands" => [
                   %{
                     "opcode" => @op_flow_policy_set,
                     "request_id" => 2,
                     "body" => %{
                       "type" => type,
                       "indexed_attributes" => ["tenant"]
                     }
                   }
                 ]
               },
               state()
             )

    assert native_message =~ "coordinated"
  end

  @tag :prepared_multi_routing
  test "same_shard pipeline rejects global data mutations and keyspace reads" do
    for {command, args} <- [
          {"FLUSHDB", []},
          {"DBSIZE", []},
          {"KEYS", ["*"]},
          {"RANDOMKEY", []},
          {"SCAN", ["0"]}
        ] do
      assert {:bad_request, message, _state} =
               Commands.execute(
                 @op_pipeline,
                 %{
                   "atomicity" => "same_shard",
                   "commands" => [
                     %{
                       "opcode" => @op_command_exec,
                       "request_id" => 1,
                       "body" => %{"command" => command, "args" => args}
                     }
                   ]
                 },
                 state()
               )

      assert message =~ "coordinated"
    end
  end

  test "COMMAND_EXEC authorizes ACL subcommands before dispatch" do
    assert :ok = Acl.set_user("operator", ["on", "nopass", "+@all", "-acl|setuser", "~*"])

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "ACL", "args" => ["SETUSER", "target", "on"]},
        state_as("operator")
      )

    assert status == :noperm
    assert payload =~ "acl.setuser"
  end

  test "COMMAND_EXEC ACL WHOAMI returns the authenticated session username" do
    {status, payload, returned_state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "ACL", "args" => ["WHOAMI"]},
        state(%{username: "session-user"})
      )

    assert status == :ok
    assert payload == "session-user"
    assert returned_state.username == "session-user"
  end

  test "native full-access fast path fails closed while ACL projection is stale" do
    on_exit(fn -> CatalogProjector.mark_ready() end)
    :ok = CatalogProjector.mark_stale(:injected_projection_failure)

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_set,
               %{"key" => "stale-acl-fast-path", "value" => "denied"},
               state()
             )

    assert message =~ "ACL catalog projection unavailable"
  end

  @tag :acl_command_exec_replication
  test "COMMAND_EXEC dispatches replicated ACL mutations and invalidates cached sessions" do
    join_acl_invalidation_group("native-target")

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{
          "command" => "ACL",
          "args" => ["SETUSER", "native-target", "on", "nopass", "-@all", "+GET", "~tenant:*"]
        },
        state()
      )

    assert status == :ok
    assert payload == "OK"
    assert_receive {:acl_invalidate, "native-target", _revision}

    target_state = state_as("native-target")
    assert_native_get_ok("tenant:key", target_state)

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "ACL", "args" => ["GETUSER", "native-target"]},
        state()
      )

    assert status == :ok
    assert "flags" in payload

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "ACL", "args" => ["DELUSER", "native-target"]},
        state()
      )

    assert status == :ok
    assert payload == 1
    assert_receive {:acl_invalidate, "native-target", _revision}

    target_state = ConnAuth.maybe_refresh_acl_cache(target_state, "native-target")
    assert_native_get_denied("tenant:key", target_state)
  end

  test "COMMAND_EXEC enforces scoped keys for management commands" do
    assert :ok =
             Acl.set_user("tenant-a-manager", [
               "on",
               "nopass",
               "+ferricstore.quota",
               "~tenant:a:*"
             ])

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "FERRICSTORE.QUOTA", "args" => ["GET", "tenant:b"]},
        state_as("tenant-a-manager")
      )

    assert status == :noperm
    assert payload =~ "keys mentioned"

    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "FERRICSTORE.QUOTA", "args" => ["GET", "tenant:a"]},
        state_as("tenant-a-manager")
      )

    assert status == :error
    assert payload == "ERR unsupported management command"
  end

  test "COMMAND_EXEC enforces admin and dangerous categories for scoped credentials" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    denied_commands = [
      {"ACL", ["SETUSER", "target", "on"], "acl.setuser"},
      {"CONFIG", ["GET", "*"], "config"},
      {"FLUSHDB", [], "flushdb"},
      {"FERRICSTORE.METRICS", [], "ferricstore.metrics"},
      {"FERRICSTORE.NAMESPACE", ["LIST"], "ferricstore.namespace"},
      {"FERRICSTORE.QUOTA", ["GET", "tenant:a"], "ferricstore.quota"},
      {"FLOW.RETENTION_CLEANUP", [], "flow.retention_cleanup"}
    ]

    for {command, args, expected} <- denied_commands do
      {status, payload, _state} =
        Commands.execute(
          @op_command_exec,
          %{"command" => command, "args" => args},
          state
        )

      assert status == :noperm
      assert payload =~ expected
    end
  end

  test "COMMAND_EXEC enforces key scope for scoped credentials" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "SET", "args" => ["tenant:a:key", "value"]},
               state
             )

    assert {:noperm, payload, _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "SET", "args" => ["tenant:b:key", "value"]},
               state
             )

    assert payload =~ "keys mentioned"
  end

  @tag :prepared_unlink_keys
  test "COMMAND_EXEC authorizes every variadic UNLINK key" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, payload, _state} =
             Commands.execute(
               @op_command_exec,
               %{
                 "command" => "UNLINK",
                 "args" => ["tenant:a:allowed", "tenant:b:forbidden"]
               },
               state
             )

    assert payload =~ "keys mentioned"
  end

  test "typed command ACLs preserve read-modify-write and routing access" do
    assert :ok =
             Acl.set_user("native-write-only", [
               "on",
               "nopass",
               "+@all",
               "%W~secret:*"
             ])

    write_only = state_as("native-write-only")

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_set,
               %{"key" => "secret:key", "value" => "initial"},
               write_only
             )

    assert {:noperm, _message, _state} =
             Commands.execute(
               @op_set,
               %{"key" => "secret:key", "value" => "replacement", "get" => true},
               write_only
             )

    assert {:noperm, _message, _state} =
             Commands.execute(
               @op_hset,
               %{"key" => "secret:hash", "fields" => %{"field" => "value"}},
               write_only
             )

    assert :ok =
             Acl.set_user("native-route-reader", [
               "on",
               "nopass",
               "+@all",
               "%R~route:*"
             ])

    route_reader = state_as("native-route-reader")

    assert {status, _payload, _state} =
             Commands.execute(
               @op_route_batch,
               %{"keys" => ["route:one", "route:two"]},
               route_reader
             )

    refute status == :noperm
  end

  test "FLOW.SEARCH enforces scoped key boundaries" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_command_exec,
               %{
                 "command" => "FLOW.SEARCH",
                 "args" => [
                   "TYPE",
                   "checkout",
                   "ATTRIBUTE",
                   "tenant",
                   "acme",
                   "PARTITION",
                   "tenant:b"
                 ]
               },
               state
             )

    assert message =~ "keys mentioned"

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_search,
               %{
                 "type" => "checkout",
                 "attributes" => %{"tenant" => "acme"},
                 "partition_key" => "tenant:b"
               },
               state
             )

    assert message =~ "keys mentioned"
  end

  test "FLOW.QUERY authorizes the bound partition for dedicated and command-exec paths" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    query =
      "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"

    requests = fn partition ->
      [
        {@op_flow_query,
         %{
           "version" => "FQL1",
           "query" => query,
           "params" => %{"partition" => partition, "flow_id" => "opaque-id"}
         }},
        {@op_command_exec,
         %{
           "command" => "FLOW.QUERY",
           "args" => ["FQL1", query, "partition", partition, "flow_id", "opaque-id"]
         }}
      ]
    end

    for {opcode, payload} <- requests.("tenant:a:partition") do
      assert {:ok, nil, _state} = Commands.execute(opcode, payload, state)
    end

    for {opcode, payload} <- requests.("tenant:b:partition") do
      assert {:noperm, message, _state} = Commands.execute(opcode, payload, state)
      assert message =~ "keys mentioned"
    end
  end

  test "typed FLOW commands authorize the effective partition instead of the flow id" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    for {opcode, payload} <- [
          {@op_flow_get, %{"id" => "tenant:a:flow", "partition_key" => "tenant:b:partition"}},
          {@op_flow_claim_due,
           %{"type" => "tenant:a:type", "partition_key" => "tenant:b:partition"}},
          {@op_flow_list,
           %{"type" => "tenant:a:type", "opts" => %{"partition_key" => "tenant:b:partition"}}}
        ] do
      assert {:noperm, message, _state} = Commands.execute(opcode, payload, state)
      assert message =~ "keys mentioned"
    end
  end

  test "FLOW.GET and FLOW.HISTORY without a partition require unrestricted read scope" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")
    visible_id = "tenant:a:visible-flow"

    for {opcode, payload} <- [
          {@op_flow_get, %{"id" => visible_id}},
          {@op_flow_history, %{"id" => visible_id}},
          {@op_command_exec, %{"command" => "FLOW.GET", "args" => [visible_id]}},
          {@op_command_exec, %{"command" => "FLOW.HISTORY", "args" => [visible_id]}}
        ] do
      assert {:noperm, message, _state} = Commands.execute(opcode, payload, state)
      assert message =~ "keys mentioned"
    end

    for {mode, item} <- [
          {9, {:flow_get, visible_id, []}},
          {10, {:flow_history, visible_id, []}}
        ] do
      assert {:ok, [%{"status" => "noperm"}], _state} =
               Commands.execute(
                 @op_pipeline,
                 %{"compact_pipeline" => {mode, [item]}},
                 state
               )
    end
  end

  test "FLOW.GET and FLOW.HISTORY authorize an explicit effective partition" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    for {opcode, payload} <- [
          {@op_flow_get, %{"id" => "opaque-id", "partition_key" => "tenant:a:partition"}},
          {@op_flow_history, %{"id" => "opaque-id", "partition_key" => "tenant:a:partition"}},
          {@op_command_exec,
           %{
             "command" => "FLOW.GET",
             "args" => ["opaque-id", "PARTITION", "tenant:a:partition"]
           }},
          {@op_command_exec,
           %{
             "command" => "FLOW.HISTORY",
             "args" => ["opaque-id", "PARTITION", "tenant:a:partition"]
           }}
        ] do
      {status, _payload, _state} = Commands.execute(opcode, payload, state)
      refute status == :noperm
    end
  end

  test "typed FLOW commands authorize nested options that override direct partitions" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    for {opcode, payload} <- [
          {@op_flow_get,
           %{
             "id" => "tenant:a:flow",
             "partition_key" => "tenant:a:direct",
             "opts" => %{"partition_key" => "tenant:b:nested"}
           }},
          {@op_flow_claim_due,
           %{
             "type" => "tenant:a:type",
             "worker" => "worker",
             "partition_key" => "tenant:a:direct",
             "opts" => %{"partition_key" => "tenant:b:nested"}
           }},
          {@op_flow_list,
           %{
             "type" => "tenant:a:type",
             "partition_key" => "tenant:a:direct",
             "opts" => %{"partition_key" => "tenant:b:nested"}
           }},
          {@op_flow_search,
           %{
             "type" => "checkout",
             "partition_key" => "tenant:a:direct",
             "opts" => %{"partition_key" => "tenant:b:nested"}
           }}
        ] do
      assert {:noperm, message, _state} = Commands.execute(opcode, payload, state)
      assert message =~ "keys mentioned"
    end
  end

  test "typed FLOW.CLAIM_DUE authorizes every requested partition" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_claim_due,
               %{
                 "type" => "tenant:a:type",
                 "partition_keys" => ["tenant:a:partition", "tenant:b:partition"]
               },
               state
             )

    assert message =~ "keys mentioned"
  end

  test "typed FLOW partition-wide commands require unrestricted scope without a partition" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    for {opcode, payload} <- [
          {@op_flow_claim_due, %{"type" => "tenant:a:type", "worker" => "worker"}},
          {@op_flow_list, %{"type" => "tenant:a:type"}}
        ] do
      assert {:noperm, message, _state} = Commands.execute(opcode, payload, state)
      assert message =~ "keys mentioned"
    end
  end

  test "typed FLOW relationship queries require unrestricted scope without a partition" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    for {opcode, field} <- [
          {@op_flow_by_parent, "parent_id"},
          {@op_flow_by_root, "root_id"},
          {@op_flow_by_correlation, "correlation_id"}
        ] do
      assert {:noperm, message, _state} =
               Commands.execute(opcode, %{field => "tenant:a:selector"}, state)

      assert message =~ "keys mentioned"
    end
  end

  test "claim selectors AUTO ANY and GLOBAL require unrestricted key scope" do
    assert :ok =
             Acl.set_user("claim_selector_guard", [
               "on",
               "nopass",
               "-@all",
               "+FLOW.CLAIM_DUE",
               "+FLOW.RECLAIM",
               "~AUTO",
               "~ANY",
               "~GLOBAL"
             ])

    state = state_as("claim_selector_guard")

    for selector <- ["AUTO", "ANY", "GLOBAL"] do
      for {opcode, payload} <- [
            {@op_flow_claim_due,
             %{"type" => "claim-selector", "worker" => "worker", "partition_key" => selector}},
            {@op_flow_reclaim, %{"type" => "claim-selector", "partition_key" => selector}}
          ] do
        assert {:noperm, message, _state} = Commands.execute(opcode, payload, state)
        assert message =~ "keys mentioned"
      end
    end

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_command_exec,
               %{
                 "command" => "FLOW.CLAIM_DUE",
                 "args" => [
                   "claim-selector",
                   "WORKER",
                   "worker",
                   "PARTITION",
                   "AUTO"
                 ]
               },
               state
             )

    assert message =~ "keys mentioned"
  end

  test "raw FLOW ACL extraction cannot confuse option values with partition options" do
    assert :ok =
             Acl.set_user("raw_flow_option_guard", [
               "on",
               "nopass",
               "-@all",
               "+FLOW.CLAIM_DUE",
               "~LEASE_MS"
             ])

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_command_exec,
               %{
                 "command" => "FLOW.CLAIM_DUE",
                 "args" => [
                   "tenant:a:type",
                   "WORKER",
                   "PARTITION",
                   "LEASE_MS",
                   "30000"
                 ]
               },
               state_as("raw_flow_option_guard")
             )

    assert message =~ "keys mentioned"
  end

  test "typed FLOW batch commands authorize shared and item partitions" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")
    suffix = System.unique_integer([:positive, :monotonic])

    for payload <- [
          %{
            "partition_key" => "tenant:b:partition",
            "items" => [%{"id" => "tenant:a:shared", "payload" => %{}}]
          },
          %{
            "items" => [
              %{
                "id" => "tenant:a:mixed",
                "partition_key" => "tenant:b:partition",
                "payload" => %{}
              }
            ]
          },
          %{
            "type" => "batch-list",
            "state" => "queued",
            "items" => [["tenant:a:list:#{suffix}", "tenant:b:partition", %{}]]
          },
          %{
            "type" => "batch-nested-options",
            "state" => "queued",
            "opts" => %{"partition_key" => "tenant:a:ignored"},
            "items" => [
              %{
                "id" => "tenant:a:nested:#{suffix}",
                "partition_key" => "tenant:b:partition",
                "payload" => %{}
              }
            ]
          }
        ] do
      assert {:noperm, message, _state} =
               Commands.execute(@op_flow_create_many, payload, state)

      assert message =~ "keys mentioned"
    end
  end

  test "compact FLOW batches authorize auto-partitioned normalized item ids" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    payload = %{
      "items" => [
        {:id, "tenant:b:compact", :lease_token, "lease", :fencing_token, 1}
      ],
      __wire_flow_items_normalized__: true,
      __wire_flow_opts__: []
    }

    assert {:noperm, message, _state} =
             Commands.execute(@op_flow_complete_many, payload, state)

    assert message =~ "keys mentioned"
  end

  test "FLOW.RUN_STEPS_MANY authorizes each effective item partition" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_run_steps_many,
               %{
                 "partition_key" => "tenant:a:decoy",
                 "type" => "checkout",
                 "worker" => "worker-1",
                 "states" => ["queued", "done"],
                 "items" => [
                   %{"id" => "tenant:b:flow", "partition_key" => "tenant:b:partition"}
                 ]
               },
               state
             )

    assert message =~ "keys mentioned"

    {status, _payload, _state} =
      Commands.execute(
        @op_flow_run_steps_many,
        %{
          "type" => "checkout",
          "worker" => "worker-1",
          "states" => ["queued", "done"],
          "items" => [%{"id" => "tenant:a:auto"}]
        },
        state
      )

    refute status == :noperm
  end

  test "typed FLOW global queries require unrestricted key scope" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, message, _state} =
             Commands.execute(@op_flow_stats, %{"type" => "tenant:a:type"}, state)

    assert message =~ "keys mentioned"

    {status, _payload, _state} =
      Commands.execute(
        @op_flow_stats,
        %{"type" => "tenant:a:type", "partition_key" => "tenant:a:partition"},
        state
      )

    refute status == :noperm
  end

  test "anonymous FLOW.VALUE.PUT requires unrestricted scope in typed and raw paths" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, message, _state} =
             Commands.execute(@op_flow_value_put, %{"value" => "secret"}, state)

    assert message =~ "keys mentioned"

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "FLOW.VALUE.PUT", "args" => ["secret"]},
               state
             )

    assert message =~ "keys mentioned"
  end

  test "schedule ACLs ignore unsupported decoy partitions and protect target partitions" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    for opcode <- [@op_flow_schedule_fire, @op_flow_schedule_delete] do
      assert {:noperm, message, _state} =
               Commands.execute(
                 opcode,
                 %{"id" => "tenant:b:schedule", "partition_key" => "tenant:a:decoy"},
                 state
               )

      assert message =~ "keys mentioned"
    end

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_schedule_create,
               %{
                 "id" => "tenant:a:schedule",
                 "kind" => "delay",
                 "delay_ms" => 1_000,
                 "target" => %{
                   "id" => "tenant:b:target",
                   "type" => "scheduled",
                   "partition_key" => "tenant:b:partition"
                 }
               },
               state
             )

    assert message =~ "keys mentioned"

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_schedule_get,
               %{"id" => "tenant:a:schedule"},
               state
             )

    assert message =~ "keys mentioned"

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "FLOW.SCHEDULE.GET", "args" => ["tenant:a:schedule"]},
               state
             )

    assert message =~ "keys mentioned"
  end

  test "approval ACLs ignore decoy partitions and protect requested flow scope" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_approval_get,
               %{"id" => "tenant:a:approval", "partition_key" => "tenant:a:decoy"},
               state
             )

    assert message =~ "keys mentioned"

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "FLOW.APPROVAL.GET", "args" => ["tenant:a:approval"]},
               state
             )

    assert message =~ "keys mentioned"

    assert {:noperm, message, _state} =
             Commands.execute(
               @op_flow_approval_request,
               %{
                 "id" => "tenant:a:approval",
                 "flow_id" => "tenant:b:flow",
                 "scope" => "tenant:b:scope"
               },
               state
             )

    assert message =~ "keys mentioned"
  end

  test "PIPELINE preserves COMMAND_EXEC and typed command ACL checks" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    {status, payload, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "commands" => [
            %{
              "opcode" => @op_command_exec,
              "request_id" => 1,
              "body" => %{"command" => "PING", "args" => []}
            },
            %{
              "opcode" => @op_command_exec,
              "request_id" => 2,
              "body" => %{"command" => "FERRICSTORE.METRICS", "args" => []}
            },
            %{
              "opcode" => @op_set,
              "request_id" => 3,
              "body" => %{"key" => "tenant:b:key", "value" => "value"}
            }
          ]
        },
        state
      )

    assert status == :ok
    assert Enum.map(payload, & &1["status"]) == ["ok", "noperm", "noperm"]
    assert Enum.at(payload, 1)["value"] =~ "ferricstore.metrics"
    assert Enum.at(payload, 2)["value"] =~ "keys mentioned"
  end

  test "compact PIPELINE fallback preserves key ACL checks" do
    put_platform_scoped_user("platform_scoped")
    state = state_as("platform_scoped")

    {status, payload, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "compact_pipeline" => {1, [{"tenant:a:key", "ok"}, {"tenant:b:key", "denied"}]}
        },
        state
      )

    assert status == :ok
    assert [["ok", "OK"], ["noperm", message]] = payload
    assert message =~ "keys mentioned"
  end

  test "native transactions deny scoped credential boundary escapes before queueing" do
    put_platform_scoped_user("platform_scoped")
    state = session_state_as("platform_scoped")

    assert {:ok, "OK", state} =
             Session.execute(%{"command" => "MULTI", "args" => []}, state)

    assert {:noperm, admin_payload, admin_state} =
             Session.execute(%{"command" => "FLUSHDB", "args" => []}, state)

    assert admin_payload =~ "flushdb"
    assert admin_state.multi_queue == []

    assert {:noperm, key_payload, key_state} =
             Session.execute(
               %{"command" => "SET", "args" => ["tenant:b:key", "value"]},
               state
             )

    assert key_payload =~ "keys mentioned"
    assert key_state.multi_queue == []

    assert {:ok, "QUEUED", queued_state} =
             Session.execute(
               %{"command" => "SET", "args" => ["tenant:a:key", "value"]},
               state
             )

    assert queued_state.multi_queue_count == 1

    assert [%PreparedCommand{command: "SET", write_keys: ["tenant:a:key"]}] =
             queued_state.multi_queue

    assert {:ok, ["OK"], executed_state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, queued_state)

    assert executed_state.multi_state == :none
    assert executed_state.multi_queue == []
  end

  test "native MULTI rejects retained command bytes before growing the queue" do
    state =
      "default"
      |> session_state_as()
      |> Map.put(:multi_queue_byte_limit, 256)

    assert {:ok, "OK", multi_state} =
             Session.execute(%{"command" => "MULTI", "args" => []}, state)

    assert {:error, message, rejected_state} =
             Session.execute(
               %{"command" => "SET", "args" => ["bounded-multi", :binary.copy("x", 512)]},
               multi_state
             )

    assert message =~ "byte limit"
    assert rejected_state.multi_state == :none
    assert rejected_state.multi_queue == []
    assert rejected_state.multi_queue_count == 0
    assert rejected_state.multi_queue_bytes == 0
  end

  test "native MULTI detaches queued arguments from the decoded request frame" do
    retained_value = :binary.copy("v", 65)

    body =
      Codec.encode_value(%{
        "command" => "SET",
        "args" => ["detached-multi-key", retained_value],
        "ignored_padding" => :binary.copy("p", 1_000_000)
      })

    assert {:ok, payload} = Codec.decode_body(@op_command_exec, 0, body)

    assert {:ok, "OK", multi_state} =
             Session.execute(
               %{"command" => "MULTI", "args" => []},
               session_state_as("default")
             )

    assert {:ok, "QUEUED", queued_state} = Session.execute(payload, multi_state)
    assert [%PreparedCommand{args: [_key, queued_value]}] = queued_state.multi_queue
    assert queued_value == retained_value

    for binary <- collect_binaries(hd(queued_state.multi_queue)) do
      assert :binary.referenced_byte_size(binary) == byte_size(binary)
    end
  end

  test "native WATCH enforces connection limits before submitting Raft commands" do
    tag = System.unique_integer([:positive, :monotonic])
    keys = Enum.map(1..3, &"native-watch-limit:{#{tag}}:#{&1}")
    ctx = FerricStore.Instance.get(:default)
    [first | _] = keys
    shard_index = Ferricstore.Store.Router.shard_for(ctx, first)
    before_position = raft_position(shard_index)

    state =
      "default"
      |> session_state_as()
      |> Map.merge(%{watch_key_limit: 2, watch_key_byte_limit: 1_024})

    assert {:error, message, rejected_state} =
             Session.execute(%{"command" => "WATCH", "args" => keys}, state)

    assert message =~ "WATCH key limit"
    assert rejected_state.watched_keys == %{}
    assert rejected_state.watched_key_bytes == 0
    assert raft_position(shard_index) == before_position
  end

  test "native WATCH detaches retained keys from the decoded request frame" do
    key =
      "native-watch-detached:{#{System.unique_integer([:positive, :monotonic])}}:" <>
        :binary.copy("k", 65)

    body =
      Codec.encode_value(%{
        "command" => "WATCH",
        "args" => [key],
        "ignored_padding" => :binary.copy("p", 1_000_000)
      })

    assert {:ok, payload} = Codec.decode_body(@op_command_exec, 0, body)
    assert {:ok, "OK", watched_state} = Session.execute(payload, session_state_as("default"))
    assert [{stored_key, _token}] = Map.to_list(watched_state.watched_keys)
    assert stored_key == key
    assert :binary.referenced_byte_size(stored_key) == byte_size(stored_key)
  end

  test "native WATCH batches same-shard tokens into one Raft entry" do
    tag = System.unique_integer([:positive, :monotonic])
    keys = ["native-watch-batch:{#{tag}}:one", "native-watch-batch:{#{tag}}:two"]
    ctx = FerricStore.Instance.get(:default)
    [first | _] = keys
    shard_index = Ferricstore.Store.Router.shard_for(ctx, first)
    before_position = raft_position(shard_index)

    assert {:ok, "OK", watched_state} =
             Session.execute(
               %{"command" => "WATCH", "args" => keys},
               session_state_as("default")
             )

    assert map_size(watched_state.watched_keys) == 2
    assert watched_state.watched_key_bytes > 0
    assert raft_position(shard_index) == before_position + 1

    assert {:ok, "OK", unwatched_state} =
             Session.execute(%{"command" => "UNWATCH", "args" => []}, watched_state)

    assert unwatched_state.watched_keys == %{}
    assert unwatched_state.watched_key_bytes == 0
  end

  @tag :prepared_multi_routing
  test "native MULTI rejects prepared routing across Raft groups at EXEC" do
    ctx = FerricStore.Instance.get(:default)
    first = "native-multi-routing:#{System.unique_integer([:positive, :monotonic])}:one"
    first_idx = Ferricstore.Store.Router.shard_for(ctx, first)

    second =
      Enum.find_value(2..1_000, fn suffix ->
        candidate = "#{first}:#{suffix}"

        if Ferricstore.Store.Router.shard_for(ctx, candidate) != first_idx,
          do: candidate
      end)

    assert is_binary(second)
    second_idx = Ferricstore.Store.Router.shard_for(ctx, second)
    first_before = Ferricstore.Store.WriteVersion.get(first_idx)
    second_before = Ferricstore.Store.WriteVersion.get(second_idx)

    assert {:ok, "OK", multi_state} =
             Session.execute(%{"command" => "MULTI", "args" => []}, session_state_as("default"))

    assert {:ok, "QUEUED", queued_state} =
             Session.execute(
               %{"command" => "MSET", "args" => [first, "one", second, "two"]},
               multi_state
             )

    assert [%PreparedCommand{routing_keys: [^first, ^second]}] = queued_state.multi_queue

    assert {:error, "CROSSSLOT Keys in request don't hash to the same slot", executed_state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, queued_state)

    assert executed_state.multi_state == :none
    assert Ferricstore.Store.Router.get(ctx, first) == nil
    assert Ferricstore.Store.Router.get(ctx, second) == nil
    assert Ferricstore.Store.WriteVersion.get(first_idx) == first_before
    assert Ferricstore.Store.WriteVersion.get(second_idx) == second_before
  end

  @tag :prepared_multi_routing
  test "native MULTI rejects coordinated prepared commands before queueing" do
    assert {:ok, "OK", multi_state} =
             Session.execute(%{"command" => "MULTI", "args" => []}, session_state_as("default"))

    assert {:error, message, rejected_state} =
             Session.execute(
               %{"command" => "FLOW.POLICY.GET", "args" => ["coordinated-policy"]},
               multi_state
             )

    assert message =~ "coordinated"
    assert rejected_state.multi_error
    assert rejected_state.multi_queue == []
    assert rejected_state.multi_queue_count == 0

    assert {:error, abort_message, final_state} =
             Session.execute(%{"command" => "EXEC", "args" => []}, rejected_state)

    assert abort_message =~ "EXECABORT"
    assert final_state.multi_state == :none
  end

  test "native MULTI rejects request-scoped commands before queueing" do
    for {command, args} <- [
          {"PUBLISH", ["tx-channel", "must-not-publish"]},
          {"KEY_INFO", ["tx-key-info"]},
          {"FETCH_OR_COMPUTE", ["tx-fetch", "1000"]},
          {"SPOP", ["tx-random-set"]},
          {"BF.ADD", ["tx-bloom", "member"]}
        ] do
      assert {:ok, "OK", multi_state} =
               Session.execute(%{"command" => "MULTI", "args" => []}, session_state_as("default"))

      assert {:error, message, rejected_state} =
               Session.execute(%{"command" => command, "args" => args}, multi_state)

      assert message ==
               "ERR command '#{String.downcase(command)}' is not supported inside transactions"

      assert rejected_state.multi_error
      assert rejected_state.multi_queue == []
      assert rejected_state.multi_queue_count == 0
    end
  end

  @tag :prepared_multi_routing
  test "native MULTI rejects global data mutations before queueing" do
    assert {:ok, "OK", multi_state} =
             Session.execute(%{"command" => "MULTI", "args" => []}, session_state_as("default"))

    assert {:error, message, rejected_state} =
             Session.execute(%{"command" => "FLUSHDB", "args" => []}, multi_state)

    assert message =~ "coordinated"
    assert rejected_state.multi_error
    assert rejected_state.multi_queue == []
  end

  test "replicated ACL disable denies rotated-out native service credential sessions only" do
    assert :ok =
             Acl.set_user("platform_worker_old", [
               "on",
               "nopass",
               "-@all",
               "+get",
               "~tenant:a:*"
             ])

    assert :ok =
             Acl.set_user("platform_worker_new", [
               "on",
               "nopass",
               "-@all",
               "+get",
               "~tenant:a:*"
             ])

    old_state = state_as("platform_worker_old")
    new_state = state_as("platform_worker_new")

    assert_native_get_ok("tenant:a:key", old_state)
    assert_native_get_ok("tenant:a:key", new_state)

    join_acl_invalidation_group("platform_worker_old")
    assert :ok = Acl.set_user("platform_worker_old", ["off"])
    assert_receive {:acl_invalidate, "platform_worker_old", _revision}

    old_state = ConnAuth.maybe_refresh_acl_cache(old_state, "platform_worker_old")
    new_state = ConnAuth.maybe_refresh_acl_cache(new_state, "platform_worker_old")

    assert_native_get_denied("tenant:a:key", old_state)
    assert_native_get_ok("tenant:a:key", new_state)
  end

  @tag :acl_direct_invalidation
  test "direct ACL mutations invalidate cached native permissions" do
    join_acl_invalidation_group("direct-revoke")

    assert :ok =
             Acl.set_user("direct-revoke", [
               "on",
               "nopass",
               "-@all",
               "+get",
               "~tenant:direct:*"
             ])

    assert_receive {:acl_invalidate, "direct-revoke", _revision}

    state = state_as("direct-revoke")
    assert_native_get_ok("tenant:direct:key", state)

    assert :ok = Acl.set_user("direct-revoke", ["off"])
    assert_receive {:acl_invalidate, "direct-revoke", _revision}

    state = ConnAuth.maybe_refresh_acl_cache(state, "direct-revoke")
    assert_native_get_denied("tenant:direct:key", state)
  end

  test "replicated ACL delete denies active native service credential sessions" do
    assert :ok =
             Acl.set_user("platform_revoke_abcd", [
               "on",
               "nopass",
               "-@all",
               "+get",
               "~tenant:revoke:*"
             ])

    state = state_as("platform_revoke_abcd")
    assert_native_get_ok("tenant:revoke:key", state)

    join_acl_invalidation_group("platform_revoke_abcd")
    assert :ok = Acl.del_user("platform_revoke_abcd")
    assert_receive {:acl_invalidate, "platform_revoke_abcd", _revision}

    state = ConnAuth.maybe_refresh_acl_cache(state, "platform_revoke_abcd")
    assert_native_get_denied("tenant:revoke:key", state)
  end

  test "replicated ACL delete of missing credential does not invalidate other sessions" do
    join_acl_invalidation_group("platform_other_abcd")

    assert :ok =
             Acl.set_user("platform_other_abcd", [
               "on",
               "nopass",
               "-@all",
               "+get",
               "~tenant:other:*"
             ])

    state = state_as("platform_other_abcd")
    assert_native_get_ok("tenant:other:key", state)

    assert_receive {:acl_invalidate, "platform_other_abcd", _revision}

    assert {:error, "ERR User 'platform_missing_abcd' does not exist"} =
             Acl.del_user("platform_missing_abcd")

    refute_receive {:acl_invalidate, _username, _revision}, 100

    state = ConnAuth.maybe_refresh_acl_cache(state, "platform_missing_abcd")
    assert_native_get_ok("tenant:other:key", state)
  end

  test "native scope-based governance commands enforce key ACLs" do
    assert :ok =
             Acl.set_user("scope_guard", [
               "on",
               "nopass",
               "-@all",
               "+FLOW.CIRCUIT.OPEN",
               "+FLOW.CIRCUIT.GET",
               "+FLOW.BUDGET.RESERVE",
               "+FLOW.LIMIT.LEASE",
               "~tenant:a:*"
             ])

    state = state_as("scope_guard")

    for {opcode, payload} <- [
          {@op_flow_circuit_open,
           %{"scope" => "tenant:b:effect", "failure_threshold" => 1, "open_ms" => 1_000}},
          {@op_flow_circuit_get, %{"scope" => "tenant:b:effect"}},
          {@op_flow_budget_reserve, %{"scope" => "tenant:b:budget", "amount" => 10}},
          {@op_flow_limit_lease, %{"scope" => "tenant:b:limit", "limit" => 10}}
        ] do
      assert {:noperm, message, _state} = Commands.execute(opcode, payload, state)
      assert message =~ "NOPERM"
    end

    {status, _payload, _state} =
      Commands.execute(
        @op_flow_circuit_open,
        %{"scope" => "tenant:a:effect", "failure_threshold" => 1, "open_ms" => 1_000},
        state
      )

    refute status == :noperm
  end

  test "CLIENT TRACKING is explicitly rejected after text protocol removal" do
    {status, payload, _state} =
      Commands.execute(
        @op_command_exec,
        %{"command" => "CLIENT", "args" => ["TRACKING", "ON"]},
        state()
      )

    assert status == :error
    assert payload =~ "CLIENT TRACKING is not supported"
  end

  defp state(overrides \\ %{}) do
    %{
      client_id: System.unique_integer([:positive, :monotonic]),
      client_name: nil,
      username: "default",
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access,
      peer: {{127, 0, 0, 1}, 12_345},
      created_at: System.monotonic_time(:millisecond),
      instance_ctx: FerricStore.Instance.get(:default),
      stats_counter: FerricStore.Instance.get(:default).stats_counter,
      compression: :none,
      compact_flow_responses: false,
      subscribed_events: MapSet.new(),
      flow_wake_subscriptions: MapSet.new()
    }
    |> Map.merge(overrides)
  end

  defp state_with_query_engine(query_engine) do
    instance_ctx = %{
      FerricStore.Instance.get(:default)
      | query_engine: query_engine,
        query_capabilities: FerricStore.Flow.QueryEngine.capabilities_for(query_engine)
    }

    state(%{instance_ctx: instance_ctx})
  end

  defp shared_wake_context do
    ClaimWaiters.init()
    ClaimWaiters.cleanup(self())

    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        flow_metadata_extension: WakeScopeProvider,
        flow_tenancy_mode: :shared
      )

    test_pid = self()

    on_exit(fn ->
      ClaimWaiters.cleanup(test_pid)
      IsolatedInstance.checkin(ctx)
    end)

    ctx
  end

  defp wake_state(ctx) do
    state(%{
      instance_ctx: ctx,
      trusted_request_context_users: MapSet.new(["default"])
    })
  end

  defp scoped_auto_partition(tenant_ref) do
    assert {:ok, partition} =
             StorageScope.physical_partition_key(
               hd(Keys.auto_partition_keys()),
               <<tenant_ref::unsigned-big-64>>
             )

    partition
  end

  defp forward_router_traces(parent) do
    receive do
      message ->
        send(parent, {:router_trace, message})
        forward_router_traces(parent)
    end
  end

  defp state_as(username) do
    state(%{
      username: username,
      acl_cache: ConnAuth.build_acl_cache(username),
      require_auth: ConnAuth.user_requires_auth?(username)
    })
  end

  defp session_state_as(username) do
    username
    |> state_as()
    |> Map.merge(%{
      multi_state: :none,
      multi_queue: [],
      multi_queue_count: 0,
      multi_queue_bytes: 0,
      multi_error: false,
      watched_keys: %{},
      watched_key_bytes: 0,
      pubsub_channels: nil,
      pubsub_patterns: nil
    })
  end

  defp raft_position(shard_index) do
    {:ok, {:raft_log_pos, index, _term}} =
      Ferricstore.Raft.WARaftBackend.storage_position(shard_index)

    index
  end

  defp collect_binaries(binary) when is_binary(binary), do: [binary]

  defp collect_binaries([head | tail]),
    do: collect_binaries(head) ++ collect_binaries(tail)

  defp collect_binaries([]), do: []

  defp collect_binaries(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> collect_binaries()
  end

  defp collect_binaries(map) when is_map(map) do
    map
    |> :maps.to_list()
    |> Enum.flat_map(fn {key, value} -> collect_binaries(key) ++ collect_binaries(value) end)
  end

  defp collect_binaries(_term), do: []

  defp join_acl_invalidation_group(username) do
    client_id = System.unique_integer([:positive, :monotonic])
    :ok = ConnRegistry.register(client_id, self(), %{username: username})
    on_exit(fn -> ConnRegistry.unregister(client_id, self()) end)
  end

  defp put_platform_scoped_user(username) do
    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "-@all",
               "+PING",
               "+@read",
               "+@write",
               "+MULTI",
               "+EXEC",
               "+DISCARD",
               "-@dangerous",
               "-@admin",
               "~tenant:a:*",
               "&tenant:a:*"
             ])
  end

  defp assert_native_get_ok(key, state) do
    assert {:ok, _payload, _state} =
             Commands.execute(@op_command_exec, %{"command" => "GET", "args" => [key]}, state)
  end

  defp assert_native_get_denied(key, state) do
    assert {:noperm, message, _state} =
             Commands.execute(@op_command_exec, %{"command" => "GET", "args" => [key]}, state)

    assert message =~ "NOPERM"
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp flow_record_ids(records) when is_list(records) do
    Enum.map(records, fn record ->
      Map.get(record, :id) || Map.get(record, "id")
    end)
  end

  defp schema_names(payload), do: Map.keys(payload.schemas)
  defp opcode_names(payload), do: Enum.map(payload.opcodes, & &1["name"])
end
