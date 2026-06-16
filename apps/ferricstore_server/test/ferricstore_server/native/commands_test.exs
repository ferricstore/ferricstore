defmodule FerricstoreServer.Native.CommandsTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Config
  alias Ferricstore.Flow.ClaimWaiters
  alias Ferricstore.Store.{CompoundKey, Router}
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Native.Commands

  @op_auth 0x0002
  @op_ping 0x0003
  @op_options 0x000B
  @op_startup 0x000C
  @op_pipeline 0x000E
  @op_subscribe_events 0x0011
  @op_unsubscribe_events 0x0012
  @op_get 0x0101
  @op_mget 0x0104
  @op_mset 0x0105
  @op_cas 0x0106
  @op_hset 0x0110
  @op_hget 0x0111
  @op_hmget 0x0112
  @op_hgetall 0x0113
  @op_lpush 0x0120
  @op_rpush 0x0121
  @op_lpop 0x0122
  @op_rpop 0x0123
  @op_lrange 0x0124
  @op_sadd 0x0130
  @op_srem 0x0131
  @op_smembers 0x0132
  @op_sismember 0x0133
  @op_zadd 0x0140
  @op_zrem 0x0141
  @op_zrange 0x0142
  @op_zscore 0x0143
  @op_cluster_keyslot 0x0303
  @op_flow_create 0x0201
  @op_flow_get 0x0202
  @op_flow_claim_due 0x0203
  @op_flow_history 0x020A
  @op_flow_value_put 0x020B
  @op_flow_value_mget 0x020C
  @op_flow_signal 0x020D
  @op_flow_list 0x020E
  @op_flow_stats 0x022D
  @op_flow_create_many 0x020F
  @op_flow_transition_many 0x0211
  @op_flow_retry_many 0x0212
  @op_flow_fail_many 0x0213
  @op_flow_cancel_many 0x0214
  @op_flow_step_continue 0x0222
  @op_flow_start_and_claim 0x0223
  @op_flow_run_steps_many 0x0224
  @op_flow_schedule_create 0x0225
  @op_flow_schedule_get 0x0226
  @op_flow_schedule_delete 0x0227
  @op_flow_schedule_fire_due 0x0228
  @op_flow_schedule_list 0x0229
  @op_flow_schedule_fire 0x022A
  @op_flow_schedule_pause 0x022B
  @op_flow_schedule_resume 0x022C

  setup do
    ConnRegistry.init_table()
    FerricstoreServer.Acl.reset!()
    old_max_pipeline_commands = Application.get_env(:ferricstore, :native_max_pipeline_commands)

    old_max_collection_response_items =
      Application.get_env(:ferricstore, :native_max_collection_response_items)

    old_request_compression_enabled =
      Application.get_env(:ferricstore, :native_request_compression_enabled)

    on_exit(fn ->
      Config.set("requirepass", "")
      restore_env(:native_max_pipeline_commands, old_max_pipeline_commands)
      restore_env(:native_max_collection_response_items, old_max_collection_response_items)
      restore_env(:native_request_compression_enabled, old_request_compression_enabled)
      FerricstoreServer.Acl.reset!()
    end)

    :ok
  end

  test "OPTIONS advertises multiplexing, flow control, and event support" do
    {status, payload, _state} = Commands.execute(@op_options, %{}, state())

    assert status == :ok
    assert payload.protocol_versions == [1]
    assert payload.multiplexing.lane_id == true
    assert payload.multiplexing.ordered_per_lane == true
    assert payload.flow_control.window_update == true
    assert payload.flow_control.enforced == true
    assert payload.compression == ["none"]
    assert payload.limits.max_collection_response_items == 10_000
    assert payload.chunking.request_reassembly == true
    assert payload.chunking.response_chunks == true
    assert payload.response_codecs.typed_value == true
    assert "flow_claim_jobs_v1" in payload.response_codecs.supported
    assert "ok_list_v1" in payload.response_codecs.supported
    assert payload.schemas["FLOW.CREATE"]["required"] == ["id"]
    assert "type" in payload.schemas["FLOW.CREATE"]["fields"]
    assert "return" in payload.schemas["FLOW.CLAIM_DUE"]["fields"]
    assert "partition_keys" in payload.schemas["FLOW.CLAIM_DUE"]["fields"]
    assert "block_ms" in payload.schemas["FLOW.CLAIM_DUE"]["fields"]
    assert "reclaim_expired" in payload.schemas["FLOW.CLAIM_DUE"]["fields"]
    assert "reclaim_ratio" in payload.schemas["FLOW.CLAIM_DUE"]["fields"]
    assert "attributes" in payload.schemas["FLOW.STATS"]["fields"]
    assert "AUTH_INVALIDATED" in payload.events
    assert Enum.any?(payload.opcodes, &(&1["name"] == "PIPELINE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "ROUTE_BATCH"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.CREATE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.CLAIM_DUE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.COMPLETE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.SIGNAL"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.STATS"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "CAS"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "GET"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "MGET"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "HSET"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "LPUSH"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "SADD"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "ZADD"))
    refute Enum.any?(payload.opcodes, &(&1["name"] == "GET.COMPACT"))
    refute Enum.any?(payload.opcodes, &(&1["name"] == "MGET.COMPACT"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "LOCK"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "RATELIMIT.ADD"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "CLUSTER.STATUS"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FERRICSTORE.KEY_INFO"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FERRICSTORE.CONFIG"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FERRICSTORE.METRICS"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.VALUE.PUT"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.VALUE.MGET"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.POLICY.SET"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.POLICY.GET"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.SPAWN_CHILDREN"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.RETENTION_CLEANUP"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.STEP_CONTINUE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.START_AND_CLAIM"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.RUN_STEPS_MANY"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.SCHEDULE.CREATE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.SCHEDULE.GET"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.SCHEDULE.FIRE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.SCHEDULE.PAUSE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.SCHEDULE.RESUME"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.SCHEDULE.DELETE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.SCHEDULE.FIRE_DUE"))
    assert Enum.any?(payload.opcodes, &(&1["name"] == "FLOW.SCHEDULE.LIST"))

    assert payload.schemas["FLOW.VALUE.MGET"]["fields"] == [
             "refs",
             "max_bytes",
             "payload_max_bytes",
             "value_max_bytes",
             "deadline_ms"
           ]

    assert "owner_flow_id" in payload.schemas["FLOW.VALUE.PUT"]["fields"]
    assert "retention_ttl_ms" in payload.schemas["FLOW.CREATE"]["fields"]

    assert payload.schemas["FLOW.START_AND_CLAIM"]["required"] == [
             "id",
             "type",
             "initial_state",
             "worker"
           ]

    assert "fencing_token" in payload.schemas["FLOW.STEP_CONTINUE"]["required"]

    assert payload.schemas["FLOW.RUN_STEPS_MANY"]["required"] == [
             "items",
             "type",
             "worker"
           ]

    assert "states" in payload.schemas["FLOW.RUN_STEPS_MANY"]["fields"]
    assert "steps" in payload.schemas["FLOW.RUN_STEPS_MANY"]["fields"]
    assert payload.schemas["FLOW.SCHEDULE.CREATE"]["required"] == ["id", "target"]
    assert "overwrite" in payload.schemas["FLOW.SCHEDULE.CREATE"]["fields"]
    assert "timezone" in payload.schemas["FLOW.SCHEDULE.CREATE"]["fields"]
    assert "overlap_policy" in payload.schemas["FLOW.SCHEDULE.CREATE"]["fields"]
    assert "max_fires" in payload.schemas["FLOW.SCHEDULE.CREATE"]["fields"]
    assert "end_at_ms" in payload.schemas["FLOW.SCHEDULE.CREATE"]["fields"]
    assert payload.schemas["FLOW.SCHEDULE.FIRE"]["required"] == ["id"]
    assert "fire_at_ms" in payload.schemas["FLOW.SCHEDULE.FIRE"]["fields"]
    assert payload.schemas["FLOW.SCHEDULE.PAUSE"]["required"] == ["id"]
    assert payload.schemas["FLOW.SCHEDULE.RESUME"]["required"] == ["id"]
    assert "block_ms" in payload.schemas["FLOW.SCHEDULE.FIRE_DUE"]["fields"]
    assert "target_type" in payload.schemas["FLOW.SCHEDULE.LIST"]["fields"]
  end

  test "OPTIONS advertises zlib request compression only when enabled" do
    Application.put_env(:ferricstore, :native_request_compression_enabled, true)

    {status, payload, _state} = Commands.execute(@op_options, %{}, state())

    assert status == :ok
    assert "zlib" in payload.compression
  end

  test "native validates custom command fields before dispatch" do
    {status, reason, _state} =
      Commands.execute(
        @op_cas,
        %{"key" => "k", "expected" => "old", "value" => "new", "ttl" => "bad"},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :bad_request
    assert reason =~ "ttl"

    {status, reason, _state} =
      Commands.execute(
        @op_hset,
        %{"key" => "h", "fields" => %{}},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :bad_request
    assert reason =~ "fields"
  end

  test "GET and MGET preserve optimized native KV read semantics" do
    ctx = FerricStore.Instance.get(:default)
    key = "native-compact-get-#{System.unique_integer([:positive])}"
    missing = key <> "-missing"

    assert :ok = FerricStore.Impl.set(ctx, key, "value")

    assert {:ok, "value", _state} =
             Commands.execute(@op_get, %{"key" => key}, state(instance_ctx: ctx))

    assert {:ok, nil, _state} =
             Commands.execute(@op_get, %{"key" => missing}, state(instance_ctx: ctx))

    assert {:ok, ["value", nil], _state} =
             Commands.execute(
               @op_mget,
               %{"keys" => [key, missing]},
               state(instance_ctx: ctx)
             )
  end

  test "native schedule commands create, overwrite, fire, get, and delete schedules" do
    ctx = FerricStore.Instance.get(:default)
    now_ms = 7_000
    schedule_id = "native-schedule-#{System.unique_integer([:positive])}"
    old_target_id = schedule_id <> "-old-target"
    new_target_id = schedule_id <> "-new-target"
    old_partition = schedule_id <> "-old-partition"
    new_partition = schedule_id <> "-new-partition"

    create_body = %{
      "id" => schedule_id,
      "kind" => "one_shot",
      "at_ms" => now_ms + 100,
      "now_ms" => now_ms,
      "target" => %{
        "id" => old_target_id,
        "type" => schedule_id <> "-old-type",
        "partition_key" => old_partition,
        "payload" => "old"
      }
    }

    assert {:ok, %{id: ^schedule_id, next_run_at_ms: next_run_at_ms}, _state} =
             Commands.execute(@op_flow_schedule_create, create_body, state(instance_ctx: ctx))

    assert next_run_at_ms == now_ms + 100

    assert {:error, "ERR flow already exists", _state} =
             Commands.execute(@op_flow_schedule_create, create_body, state(instance_ctx: ctx))

    overwrite_body =
      Map.merge(create_body, %{
        "at_ms" => now_ms + 500,
        "now_ms" => now_ms + 1,
        "overwrite" => true,
        "target" => %{
          "id" => new_target_id,
          "type" => schedule_id <> "-new-type",
          "partition_key" => new_partition,
          "payload" => "new"
        }
      })

    assert {:ok, %{id: ^schedule_id, next_run_at_ms: next_run_at_ms}, _state} =
             Commands.execute(@op_flow_schedule_create, overwrite_body, state(instance_ctx: ctx))

    assert next_run_at_ms == now_ms + 500

    assert {:ok, %{state: "paused"}, _state} =
             Commands.execute(
               @op_flow_schedule_pause,
               %{"id" => schedule_id, "now_ms" => now_ms + 2},
               state(instance_ctx: ctx)
             )

    assert {:ok, %{fired: 0, claimed: 0}, _state} =
             Commands.execute(
               @op_flow_schedule_fire_due,
               %{"now_ms" => now_ms + 500, "worker" => "native-schedule-test"},
               state(instance_ctx: ctx)
             )

    assert {:ok, %{state: "active"}, _state} =
             Commands.execute(
               @op_flow_schedule_resume,
               %{"id" => schedule_id, "now_ms" => now_ms + 3},
               state(instance_ctx: ctx)
             )

    manual_schedule_id = schedule_id <> "-manual"
    manual_target_id = manual_schedule_id <> "-target"

    assert {:ok, _manual_schedule, _state} =
             Commands.execute(
               @op_flow_schedule_create,
               %{
                 "id" => manual_schedule_id,
                 "kind" => "one_shot",
                 "at_ms" => now_ms + 30_000,
                 "now_ms" => now_ms,
                 "target" => %{
                   "id" => manual_target_id,
                   "type" => manual_schedule_id <> "-type",
                   "payload" => "manual"
                 }
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, %{fired: 1, target_id: ^manual_target_id}, _state} =
             Commands.execute(
               @op_flow_schedule_fire,
               %{"id" => manual_schedule_id, "now_ms" => now_ms + 2},
               state(instance_ctx: ctx)
             )

    assert {:ok, %{payload: "manual"}} =
             FerricStore.Impl.flow_get(ctx, manual_target_id, payload: true)

    assert {:ok, schedules, _state} =
             Commands.execute(
               @op_flow_schedule_list,
               %{
                 "state" => "active",
                 "kind" => "one_shot",
                 "target_type" => schedule_id <> "-new-type",
                 "count" => 10
               },
               state(instance_ctx: ctx)
             )

    assert Enum.any?(schedules, &(&1.id == schedule_id))

    assert {:ok, %{fired: 0, claimed: 0}, _state} =
             Commands.execute(
               @op_flow_schedule_fire_due,
               %{"now_ms" => now_ms + 100, "worker" => "native-schedule-test"},
               state(instance_ctx: ctx)
             )

    assert {:ok, nil} =
             FerricStore.Impl.flow_get(ctx, old_target_id, partition_key: old_partition)

    assert {:ok, %{fired: 1, claimed: 1}, _state} =
             Commands.execute(
               @op_flow_schedule_fire_due,
               %{"now_ms" => now_ms + 500, "worker" => "native-schedule-test"},
               state(instance_ctx: ctx)
             )

    assert {:ok, target} =
             FerricStore.Impl.flow_get(ctx, new_target_id,
               partition_key: new_partition,
               payload: true
             )

    assert target.payload == "new"

    assert {:ok, %{state: "completed"}, _state} =
             Commands.execute(
               @op_flow_schedule_get,
               %{"id" => schedule_id},
               state(instance_ctx: ctx)
             )

    future_schedule_id = schedule_id <> "-delete"

    assert {:ok, _schedule, _state} =
             Commands.execute(
               @op_flow_schedule_create,
               %{
                 "id" => future_schedule_id,
                 "kind" => "one_shot",
                 "at_ms" => now_ms + 10_000,
                 "now_ms" => now_ms,
                 "target" => %{
                   "id" => future_schedule_id <> "-target",
                   "type" => future_schedule_id <> "-type"
                 }
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_schedule_delete,
               %{"id" => future_schedule_id, "now_ms" => now_ms + 1},
               state(instance_ctx: ctx)
             )
  end

  test "native Flow query commands accept count and time filter options" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-flow-query-#{System.unique_integer([:positive])}"
    id = prefix <> ":flow"
    flow_type = prefix <> ":type"
    now = System.system_time(:millisecond)

    assert {:ok, _record, _state} =
             Commands.execute(
               @op_flow_create,
               %{
                 "id" => id,
                 "type" => flow_type,
                 "state" => "queued",
                 "now_ms" => now,
                 "run_at_ms" => now
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, records, _state} =
             Commands.execute(
               @op_flow_list,
               %{"type" => flow_type, "state" => "queued", "count" => 10},
               state(instance_ctx: ctx)
             )

    assert is_list(records)

    assert {:ok, history, _state} =
             Commands.execute(
               @op_flow_history,
               %{"id" => id, "count" => 10, "from_ms" => 0, "to_ms" => now + 1},
               state(instance_ctx: ctx)
             )

    assert is_list(history)
  end

  test "native FLOW.STATS filters by indexed attributes" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-flow-stats-#{System.unique_integer([:positive])}"
    flow_type = prefix <> ":type"
    partition = prefix <> ":partition"
    now = System.system_time(:millisecond)

    for {id, tenant} <- [{"acme", "acme"}, {"other", "other"}] do
      assert {:ok, _record, _state} =
               Commands.execute(
                 @op_flow_create,
                 %{
                   "id" => prefix <> ":" <> id,
                   "type" => flow_type,
                   "state" => "queued",
                   "partition_key" => partition,
                   "attributes" => %{"tenant" => tenant},
                   "now_ms" => now,
                   "run_at_ms" => now
                 },
                 state(instance_ctx: ctx)
               )
    end

    Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 30_000)

    assert {:ok, stats, _state} =
             Commands.execute(
               @op_flow_stats,
               %{
                 "type" => flow_type,
                 "state" => "queued",
                 "partition_key" => partition,
                 "attributes" => %{"tenant" => "acme"},
                 "consistent_projection" => true
               },
               state(instance_ctx: ctx)
             )

    assert Map.fetch!(stats, :count) == 1
    assert Map.fetch!(stats, :attributes) == %{"tenant" => "acme"}
  end

  test "native FLOW.RUN_STEPS_MANY dispatches deterministic chains" do
    ctx = FerricStore.Instance.get(:default)
    suffix = System.unique_integer([:positive, :monotonic])
    partition = "native-run-steps-#{suffix}"
    type = "native-run-steps-type-#{suffix}"
    id = "native-run-steps-id-#{suffix}"
    now = System.system_time(:millisecond)

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_run_steps_many,
               %{
                 "items" => [%{"id" => id, "partition_key" => partition}],
                 "type" => type,
                 "states" => ["reserve", "charge", "email"],
                 "worker" => "native-run-steps-worker",
                 "lease_ms" => 30_000,
                 "now_ms" => now,
                 "result" => "ok"
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, record} = FerricStore.flow_get(id, partition_key: partition)
    assert record.state == "completed"
    assert record.run_state == "email"
    assert record.fencing_token == 3
  end

  test "native FLOW.RUN_STEPS_MANY records trace phases" do
    ctx = FerricStore.Instance.get(:default)
    suffix = System.unique_integer([:positive, :monotonic])
    partition = "native-run-steps-trace-#{suffix}"
    type = "native-run-steps-trace-type-#{suffix}"
    id = "native-run-steps-trace-id-#{suffix}"
    now = System.system_time(:millisecond)
    previous_trace = Ferricstore.LatencyTrace.start(%{})

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_run_steps_many,
               %{
                 "items" => [%{"id" => id, "partition_key" => partition}],
                 "type" => type,
                 "states" => ["reserve", "charge", "email"],
                 "worker" => "native-run-steps-worker",
                 "lease_ms" => 30_000,
                 "now_ms" => now,
                 "result" => "ok"
               },
               state(instance_ctx: ctx)
             )

    trace = Ferricstore.LatencyTrace.finish(previous_trace)

    assert is_integer(trace["server_flow_run_steps_items_us"])
    assert is_integer(trace["server_flow_run_steps_opts_us"])
    assert is_integer(trace["server_flow_run_steps_impl_us"])
    assert is_integer(trace["server_flow_many_quorum_us"])
    assert is_integer(trace["server_flow_run_steps_prepare_us"])
    assert is_integer(trace["server_flow_run_steps_apply_us"])
    assert is_integer(trace["server_ra_wait_us"])
  end

  test "native FLOW.LIST RETURN META trims heavy fields" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-flow-list-meta-#{System.unique_integer([:positive])}"
    id = prefix <> ":flow"
    flow_type = prefix <> ":type"
    now = System.system_time(:millisecond)

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create,
               %{
                 "id" => id,
                 "type" => flow_type,
                 "state" => "queued",
                 "now_ms" => now,
                 "run_at_ms" => now,
                 "parent_id" => prefix <> ":parent",
                 "root_id" => prefix <> ":root",
                 "correlation_id" => prefix <> ":correlation",
                 "payload" => String.duplicate("x", 1024)
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, [record], _state} =
             Commands.execute(
               @op_flow_list,
               %{"type" => flow_type, "state" => "queued", "count" => 10, "return" => "META"},
               state(instance_ctx: ctx)
             )

    assert record.id == id
    assert record.type == flow_type
    assert record.state == "queued"
    refute Map.has_key?(record, :payload)
    refute Map.has_key?(record, :retention_ttl_ms)
    refute Map.has_key?(record, :history_hot_max_events)
    refute Map.has_key?(record, :history_max_events)
    refute Map.has_key?(record, :child_groups)
    refute Map.has_key?(record, :parent_flow_id)
    refute Map.has_key?(record, :root_flow_id)
    refute Map.has_key?(record, :correlation_id)
  end

  test "native FLOW.LIST auto partition keeps rank order across multiple flows" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-flow-list-auto-order-#{System.unique_integer([:positive])}"
    flow_type = prefix <> ":type"
    now = System.system_time(:millisecond)

    ids =
      for idx <- 1..8 do
        id = "#{prefix}:#{idx}"

        assert {:ok, "OK", _state} =
                 Commands.execute(
                   @op_flow_create,
                   %{
                     "id" => id,
                     "type" => flow_type,
                     "state" => "queued",
                     "now_ms" => now + idx,
                     "run_at_ms" => now + idx
                   },
                   state(instance_ctx: ctx)
                 )

        id
      end

    assert {:ok, records, _state} =
             Commands.execute(
               @op_flow_list,
               %{"type" => flow_type, "state" => "queued", "count" => 8, "return" => "META"},
               state(instance_ctx: ctx)
             )

    assert Enum.map(records, & &1.id) == ids
  end

  test "native FLOW.GET RETURN META trims heavy fields" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-flow-get-meta-#{System.unique_integer([:positive])}"
    id = prefix <> ":flow"
    flow_type = prefix <> ":type"
    now = System.system_time(:millisecond)

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create,
               %{
                 "id" => id,
                 "type" => flow_type,
                 "state" => "queued",
                 "now_ms" => now,
                 "run_at_ms" => now,
                 "payload" => String.duplicate("x", 1024)
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, record, _state} =
             Commands.execute(
               @op_flow_get,
               %{"id" => id, "return" => "META"},
               state(instance_ctx: ctx)
             )

    assert record.id == id
    assert record.type == flow_type
    assert record.state == "queued"
    refute Map.has_key?(record, :retention_ttl_ms)
    refute Map.has_key?(record, :history_hot_max_events)
    refute Map.has_key?(record, :history_max_events)
    refute Map.has_key?(record, :child_groups)
  end

  test "native MSET preserves batch write semantics" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-mset-#{System.unique_integer([:positive])}"

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_mset,
               %{"pairs" => [[prefix <> ":1", "v1"], [prefix <> ":2", "v2"]]},
               state(instance_ctx: ctx)
             )

    assert {:ok, "v1"} = FerricStore.Impl.get(ctx, prefix <> ":1")
    assert {:ok, "v2"} = FerricStore.Impl.get(ctx, prefix <> ":2")
  end

  test "native protocol exposes list hash set and sorted-set commands" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-ds-#{System.unique_integer([:positive])}"

    assert {:ok, 2, _state} =
             Commands.execute(
               @op_hset,
               %{"key" => prefix <> ":hash", "fields" => %{"name" => "alice", "age" => "30"}},
               state(instance_ctx: ctx)
             )

    assert {:ok, "alice", _state} =
             Commands.execute(
               @op_hget,
               %{"key" => prefix <> ":hash", "field" => "name"},
               state(instance_ctx: ctx)
             )

    assert {:ok, ["alice", nil], _state} =
             Commands.execute(
               @op_hmget,
               %{"key" => prefix <> ":hash", "fields" => ["name", "missing"]},
               state(instance_ctx: ctx)
             )

    assert {:ok, hash, _state} =
             Commands.execute(
               @op_hgetall,
               %{"key" => prefix <> ":hash"},
               state(instance_ctx: ctx)
             )

    assert hash["name"] == "alice"
    assert hash["age"] == "30"

    assert {:ok, 2, _state} =
             Commands.execute(
               @op_lpush,
               %{"key" => prefix <> ":list", "values" => ["a", "b"]},
               state(instance_ctx: ctx)
             )

    assert {:ok, ["b", "a"], _state} =
             Commands.execute(
               @op_lrange,
               %{"key" => prefix <> ":list", "start" => 0, "stop" => -1},
               state(instance_ctx: ctx)
             )

    assert {:ok, "b", _state} =
             Commands.execute(
               @op_lpop,
               %{"key" => prefix <> ":list", "count" => 1},
               state(instance_ctx: ctx)
             )

    assert {:ok, 2, _state} =
             Commands.execute(
               @op_sadd,
               %{"key" => prefix <> ":set", "members" => ["a", "b"]},
               state(instance_ctx: ctx)
             )

    assert {:ok, true, _state} =
             Commands.execute(
               @op_sismember,
               %{"key" => prefix <> ":set", "member" => "a"},
               state(instance_ctx: ctx)
             )

    assert {:ok, members, _state} =
             Commands.execute(
               @op_smembers,
               %{"key" => prefix <> ":set"},
               state(instance_ctx: ctx)
             )

    assert Enum.sort(members) == ["a", "b"]

    assert {:ok, 2, _state} =
             Commands.execute(
               @op_zadd,
               %{"key" => prefix <> ":zset", "items" => [[2.0, "b"], [1.0, "a"]]},
               state(instance_ctx: ctx)
             )

    assert {:ok, ["a", "b"], _state} =
             Commands.execute(
               @op_zrange,
               %{"key" => prefix <> ":zset", "start" => 0, "stop" => -1},
               state(instance_ctx: ctx)
             )

    assert {:ok, "1.0", _state} =
             Commands.execute(
               @op_zscore,
               %{"key" => prefix <> ":zset", "member" => "a"},
               state(instance_ctx: ctx)
             )
  end

  test "ZRANGE rejects oversized native collection responses when configured" do
    ctx = FerricStore.Instance.get(:default)
    key = "native-zrange-direct-guard-#{System.unique_integer([:positive])}"

    assert {:ok, 4, _state} =
             Commands.execute(
               @op_zadd,
               %{
                 "key" => key,
                 "items" => [
                   [1.0, "member-1"],
                   [2.0, "member-2"],
                   [3.0, "member-3"],
                   [4.0, "member-4"]
                 ]
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, ["member-1", "member-2"], _state} =
             Commands.execute(
               @op_zrange,
               %{"key" => key, "start" => 0, "stop" => 1},
               state(instance_ctx: ctx, max_collection_response_items: 2)
             )

    assert {:bad_request, reason, _state} =
             Commands.execute(
               @op_zrange,
               %{"key" => key, "start" => 0, "stop" => -1},
               state(instance_ctx: ctx, max_collection_response_items: 2)
             )

    assert reason == "ERR native collection response item limit exceeded"
  end

  test "OPTIONS advertises configured unlimited native collection responses" do
    Application.put_env(:ferricstore, :native_max_collection_response_items, 0)

    {status, payload, _state} = Commands.execute(@op_options, %{}, state())

    assert status == :ok
    assert payload.limits.max_collection_response_items == 0
  end

  test "direct collection reads reject oversized native responses when configured" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-direct-collection-guard-#{System.unique_integer([:positive])}"
    capped_state = state(instance_ctx: ctx, max_collection_response_items: 2)

    for i <- 1..3 do
      assert :ok = FerricStore.Impl.set(ctx, "#{prefix}:kv:#{i}", "value-#{i}")
    end

    assert {:bad_request, reason, _state} =
             Commands.execute(
               @op_mget,
               %{"keys" => ["#{prefix}:kv:1", "#{prefix}:kv:2", "#{prefix}:kv:3"]},
               capped_state
             )

    assert reason == "ERR native collection response item limit exceeded"

    assert {:ok, 3, _state} =
             Commands.execute(
               @op_hset,
               %{
                 "key" => prefix <> ":hash",
                 "fields" => %{"field-1" => "one", "field-2" => "two", "field-3" => "three"}
               },
               state(instance_ctx: ctx)
             )

    assert {:bad_request, reason, _state} =
             Commands.execute(
               @op_hmget,
               %{"key" => prefix <> ":hash", "fields" => ["field-1", "field-2", "field-3"]},
               capped_state
             )

    assert reason == "ERR native collection response item limit exceeded"

    assert {:bad_request, reason, _state} =
             Commands.execute(@op_hgetall, %{"key" => prefix <> ":hash"}, capped_state)

    assert reason == "ERR native collection response item limit exceeded"

    assert {:ok, 4, _state} =
             Commands.execute(
               @op_rpush,
               %{"key" => prefix <> ":list", "values" => ["a", "b", "c", "d"]},
               state(instance_ctx: ctx)
             )

    assert {:ok, ["a", "b"], _state} =
             Commands.execute(
               @op_lrange,
               %{"key" => prefix <> ":list", "start" => 0, "stop" => 1},
               capped_state
             )

    assert {:bad_request, reason, _state} =
             Commands.execute(
               @op_lrange,
               %{"key" => prefix <> ":list", "start" => 0, "stop" => -1},
               capped_state
             )

    assert reason == "ERR native collection response item limit exceeded"

    assert {:bad_request, reason, _state} =
             Commands.execute(@op_lpop, %{"key" => prefix <> ":list", "count" => 3}, capped_state)

    assert reason == "ERR native collection response item limit exceeded"

    assert {:ok, ["a", "b", "c", "d"], _state} =
             Commands.execute(
               @op_lrange,
               %{"key" => prefix <> ":list", "start" => 0, "stop" => -1},
               state(instance_ctx: ctx, max_collection_response_items: 4)
             )

    assert {:ok, 3, _state} =
             Commands.execute(
               @op_sadd,
               %{"key" => prefix <> ":set", "members" => ["one", "two", "three"]},
               state(instance_ctx: ctx)
             )

    assert {:bad_request, reason, _state} =
             Commands.execute(@op_smembers, %{"key" => prefix <> ":set"}, capped_state)

    assert reason == "ERR native collection response item limit exceeded"
  end

  test "PIPELINE compact set and zset writes return compact integer lists" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-compact-set-zset-write-#{System.unique_integer([:positive])}"

    assert {:ok, <<0x88, 2::unsigned-32, 1::signed-64, 0::signed-64>>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" =>
                   {25,
                    [
                      {prefix <> ":set", "member"},
                      {prefix <> ":set", "member"}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, <<0x88, 2::unsigned-32, 1::signed-64, 0::signed-64>>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" =>
                   {31,
                    [
                      {prefix <> ":set", "member"},
                      {prefix <> ":set", "member"}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, <<0x88, 2::unsigned-32, 1::signed-64, 0::signed-64>>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" =>
                   {26,
                    [
                      {prefix <> ":zset", 1.0, "member"},
                      {prefix <> ":zset", 2.0, "member"}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, <<0x88, 2::unsigned-32, 1::signed-64, 0::signed-64>>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" =>
                   {32,
                    [
                      {prefix <> ":zset", "member"},
                      {prefix <> ":zset", "member"}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact data-structure reads use native DS read modes" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-ds-compact-#{System.unique_integer([:positive])}"

    assert {:ok, 1, _state} =
             Commands.execute(
               @op_hset,
               %{"key" => prefix <> ":hash", "fields" => %{"field" => "value"}},
               state(instance_ctx: ctx)
             )

    assert {:ok, 1, _state} =
             Commands.execute(
               @op_sadd,
               %{"key" => prefix <> ":set", "members" => ["member"]},
               state(instance_ctx: ctx)
             )

    assert {:ok, 1, _state} =
             Commands.execute(
               @op_lpush,
               %{"key" => prefix <> ":list", "values" => ["item"]},
               state(instance_ctx: ctx)
             )

    assert {:ok, 1, _state} =
             Commands.execute(
               @op_zadd,
               %{"key" => prefix <> ":zset", "items" => [[1.0, "member"]]},
               state(instance_ctx: ctx)
             )

    assert {:ok, 1, _state} =
             Commands.execute(
               @op_zadd,
               %{"key" => prefix <> ":zset-2", "items" => [[2.0, "member-2"]]},
               state(instance_ctx: ctx)
             )

    assert {:ok, [["ok", "value"]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {18, [{prefix <> ":hash", "field"}]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, [["ok", ["value"]]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {28, [{prefix <> ":hash", ["field"]}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, [["ok", [nil]]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {28, [{prefix <> ":hash", ["missing"]}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, <<0x86, 1::unsigned-32, 1::unsigned-32, 5::unsigned-32, "value">>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" => {28, [{prefix <> ":hash", ["field"]}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, [["ok", %{"field" => "value"}]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {30, [prefix <> ":hash"]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok,
            <<0x87, 1::unsigned-32, 1::unsigned-32, 5::unsigned-32, "field", 5::unsigned-32,
              "value">>, _state} =
             Commands.execute(
               @op_pipeline,
               %{"compact_values" => true, "compact_pipeline" => {30, [prefix <> ":hash"]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert :ok = FerricStore.Impl.set(ctx, prefix <> ":string", "value")

    assert {:ok, [["ok", nil], ["error", hget_wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {18, [{prefix <> ":missing-hash", "field"}, {prefix <> ":string", "field"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert hget_wrongtype =~ "WRONGTYPE"

    assert {:ok, [["ok", [nil]], ["error", hmget_wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {28,
                    [{prefix <> ":missing-hash", ["field"]}, {prefix <> ":string", ["field"]}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert hmget_wrongtype =~ "WRONGTYPE"

    assert {:ok, [["ok", %{}], ["ok", %{}], ["error", hgetall_wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {30, [prefix <> ":missing-hash", prefix <> ":string", prefix <> ":list"]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert hgetall_wrongtype =~ "WRONGTYPE"

    assert {:ok, [["ok", true]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {19, [{prefix <> ":set", "member"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, [["ok", ["member"]]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {27, [prefix <> ":set"]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, <<0x86, 1::unsigned-32, 1::unsigned-32, 6::unsigned-32, "member">>, _state} =
             Commands.execute(
               @op_pipeline,
               %{"compact_values" => true, "compact_pipeline" => {27, [prefix <> ":set"]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, [["ok", []], ["ok", []]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {27, [prefix <> ":missing-set", prefix <> ":string"]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, [["ok", ["item"]]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {20, [{prefix <> ":list", 0, -1}]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, [["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {27, [prefix <> ":list"]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"

    assert {:ok, [["ok", ["member"]], ["ok", ["member-2"]]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {21,
                    [
                      {prefix <> ":zset", 0, -1, false},
                      {prefix <> ":zset-2", 0, -1, false}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, [["ok", "1.0"]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {29, [{prefix <> ":zset", "member"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, <<0x89, 1::unsigned-32, 3::unsigned-32, "1.0">>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" => {29, [{prefix <> ":zset", "member"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact LRANGE reads regular list windows without changing semantics" do
    ctx = FerricStore.Instance.get(:default)
    key = "native-lrange-window-#{System.unique_integer([:positive])}"
    values = Enum.map(1..20, &"item-#{&1}")

    assert {:ok, 20, _state} =
             Commands.execute(
               @op_rpush,
               %{"key" => key, "values" => values},
               state(instance_ctx: ctx)
             )

    assert {:ok,
            [
              ["ok", ["item-1"]],
              ["ok", ^values],
              ["ok", ["item-6", "item-7", "item-8"]],
              ["ok", ["item-19", "item-20"]]
            ], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {20,
                    [
                      {key, 0, 0},
                      {key, 0, -1},
                      {key, 5, 7},
                      {key, -2, -1}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact LRANGE counts bounded overlarge windows before materializing" do
    ctx = FerricStore.Instance.get(:default)
    small_key = "native-lrange-small-window-#{System.unique_integer([:positive])}"
    large_key = "native-lrange-large-window-#{System.unique_integer([:positive])}"

    assert {:ok, 2, _state} =
             Commands.execute(
               @op_rpush,
               %{"key" => small_key, "values" => ["small-1", "small-2"]},
               state(instance_ctx: ctx)
             )

    assert {:ok, 4, _state} =
             Commands.execute(
               @op_rpush,
               %{"key" => large_key, "values" => ["large-1", "large-2", "large-3", "large-4"]},
               state(instance_ctx: ctx)
             )

    assert {:ok, [["ok", ["small-1", "small-2"]]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {20, [{small_key, 0, 1000}]}},
               state(
                 instance_ctx: ctx,
                 stats_counter: test_stats_counter(),
                 max_collection_response_items: 2
               )
             )

    assert {:bad_request, reason, _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {20, [{large_key, 0, 1000}]}},
               state(
                 instance_ctx: ctx,
                 stats_counter: test_stats_counter(),
                 max_collection_response_items: 2
               )
             )

    assert reason == "ERR native collection response item limit exceeded"
  end

  test "Router pipeline batch applies semantic HSET commands with existing hash semantics" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-hset-batch-command-#{System.unique_integer([:positive])}"
    hash_key = prefix <> ":hash"
    string_key = prefix <> ":string"

    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert [1, 0, {:error, wrongtype}] =
             Router.pipeline_write_batch(ctx, [
               {hash_key, {:hset_single, hash_key, "field", "one"}},
               {hash_key, {:hset_single, hash_key, "field", "two"}},
               {string_key, {:hset_single, string_key, "field", "blocked"}}
             ])

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, "two"} = FerricStore.Impl.hget(ctx, hash_key, "field")
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "PIPELINE batches native HSET writes through shard batch apply path" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-hset-batch-#{System.unique_integer([:positive])}"
    hash_key = prefix <> ":hash"
    string_key = prefix <> ":string"

    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert {:ok, [["ok", 1], ["ok", 0], ["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "commands" => [
                   %{
                     "opcode" => @op_hset,
                     "body" => %{"key" => hash_key, "fields" => %{"field" => "one"}}
                   },
                   %{
                     "opcode" => @op_hset,
                     "body" => %{"key" => hash_key, "fields" => %{"field" => "two"}}
                   },
                   %{
                     "opcode" => @op_hset,
                     "body" => %{"key" => string_key, "fields" => %{"field" => "blocked"}}
                   }
                 ]
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, "two"} = FerricStore.Impl.hget(ctx, hash_key, "field")
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "Router pipeline batch applies semantic LPUSH commands with existing list semantics" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-lpush-batch-command-#{System.unique_integer([:positive])}"
    list_key = prefix <> ":list"
    string_key = prefix <> ":string"

    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert [1, 2, {:error, wrongtype}] =
             Router.pipeline_write_batch(ctx, [
               {list_key, {:lpush_single, list_key, "one"}},
               {list_key, {:lpush_single, list_key, "two"}},
               {string_key, {:lpush_single, string_key, "blocked"}}
             ])

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, ["two", "one"]} = FerricStore.Impl.lrange(ctx, list_key, 0, -1)
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "Router pipeline batch applies semantic RPUSH commands with existing list semantics" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-rpush-batch-command-#{System.unique_integer([:positive])}"
    list_key = prefix <> ":list"
    string_key = prefix <> ":string"

    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert [1, 2, {:error, wrongtype}] =
             Router.pipeline_write_batch(ctx, [
               {list_key, {:rpush_single, list_key, "one"}},
               {list_key, {:rpush_single, list_key, "two"}},
               {string_key, {:rpush_single, string_key, "blocked"}}
             ])

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, ["one", "two"]} = FerricStore.Impl.lrange(ctx, list_key, 0, -1)
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "PIPELINE batches native LPUSH writes through shard batch apply path" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-lpush-batch-#{System.unique_integer([:positive])}"
    list_key = prefix <> ":list"
    string_key = prefix <> ":string"

    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert {:ok, [["ok", 1], ["ok", 2], ["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "commands" => [
                   %{"opcode" => @op_lpush, "body" => %{"key" => list_key, "values" => ["one"]}},
                   %{"opcode" => @op_lpush, "body" => %{"key" => list_key, "values" => ["two"]}},
                   %{
                     "opcode" => @op_lpush,
                     "body" => %{"key" => string_key, "values" => ["blocked"]}
                   }
                 ]
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, ["two", "one"]} = FerricStore.Impl.lrange(ctx, list_key, 0, -1)
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "PIPELINE batches native RPUSH writes through shard batch apply path" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-rpush-batch-#{System.unique_integer([:positive])}"
    list_key = prefix <> ":list"
    string_key = prefix <> ":string"

    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert {:ok, [["ok", 1], ["ok", 2], ["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "commands" => [
                   %{"opcode" => @op_rpush, "body" => %{"key" => list_key, "values" => ["one"]}},
                   %{"opcode" => @op_rpush, "body" => %{"key" => list_key, "values" => ["two"]}},
                   %{
                     "opcode" => @op_rpush,
                     "body" => %{"key" => string_key, "values" => ["blocked"]}
                   }
                 ]
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, ["one", "two"]} = FerricStore.Impl.lrange(ctx, list_key, 0, -1)
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "PIPELINE batches native LPOP writes through shard batch apply path" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-lpop-batch-#{System.unique_integer([:positive])}"
    list_key = prefix <> ":list"
    string_key = prefix <> ":string"

    assert {:ok, 2} = FerricStore.Impl.rpush(ctx, list_key, ["one", "two"])
    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert {:ok, [["ok", "one"], ["ok", "two"], ["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "commands" => [
                   %{"opcode" => @op_lpop, "body" => %{"key" => list_key}},
                   %{"opcode" => @op_lpop, "body" => %{"key" => list_key, "count" => 1}},
                   %{"opcode" => @op_lpop, "body" => %{"key" => string_key}}
                 ]
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, []} = FerricStore.Impl.lrange(ctx, list_key, 0, -1)
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "PIPELINE batches native RPOP writes through shard batch apply path" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-rpop-batch-#{System.unique_integer([:positive])}"
    list_key = prefix <> ":list"
    string_key = prefix <> ":string"

    assert {:ok, 2} = FerricStore.Impl.rpush(ctx, list_key, ["one", "two"])
    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert {:ok, [["ok", "two"], ["ok", "one"], ["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "commands" => [
                   %{"opcode" => @op_rpop, "body" => %{"key" => list_key}},
                   %{"opcode" => @op_rpop, "body" => %{"key" => list_key, "count" => 1}},
                   %{"opcode" => @op_rpop, "body" => %{"key" => string_key}}
                 ]
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, []} = FerricStore.Impl.lrange(ctx, list_key, 0, -1)
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "PIPELINE compact LPUSH preserves duplicate-key per-command replies" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-compact-lpush-group-#{System.unique_integer([:positive])}"
    list_key = prefix <> ":list"
    string_key = prefix <> ":string"

    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert {:ok, [["ok", 1], ["ok", 2], ["ok", 3]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {23,
                    [
                      {list_key, "one"},
                      {list_key, "two"},
                      {list_key, "three"}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, ["three", "two", "one"]} = FerricStore.Impl.lrange(ctx, list_key, 0, -1)

    assert {:ok, [["error", wrongtype], ["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {23, [{string_key, "blocked-1"}, {string_key, "blocked-2"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "PIPELINE compact RPUSH preserves duplicate-key per-command replies" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-compact-rpush-group-#{System.unique_integer([:positive])}"
    list_key = prefix <> ":list"
    string_key = prefix <> ":string"

    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert {:ok, [["ok", 1], ["ok", 2], ["ok", 3]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {24,
                    [
                      {list_key, "one"},
                      {list_key, "two"},
                      {list_key, "three"}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, ["one", "two", "three"]} = FerricStore.Impl.lrange(ctx, list_key, 0, -1)

    assert {:ok, [["error", wrongtype], ["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {24, [{string_key, "blocked-1"}, {string_key, "blocked-2"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "PIPELINE batches native SADD writes through shard batch apply path" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-sadd-batch-#{System.unique_integer([:positive])}"
    set_key = prefix <> ":set"
    string_key = prefix <> ":string"

    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert {:ok, [["ok", 1], ["ok", 0], ["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "commands" => [
                   %{"opcode" => @op_sadd, "body" => %{"key" => set_key, "members" => ["one"]}},
                   %{"opcode" => @op_sadd, "body" => %{"key" => set_key, "members" => ["one"]}},
                   %{
                     "opcode" => @op_sadd,
                     "body" => %{"key" => string_key, "members" => ["blocked"]}
                   }
                 ]
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, true} = FerricStore.Impl.sismember(ctx, set_key, "one")
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "PIPELINE batches native SREM writes without double-counting duplicate removals" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-srem-batch-#{System.unique_integer([:positive])}"
    set_key = prefix <> ":set"
    string_key = prefix <> ":string"

    assert {:ok, 1} = FerricStore.Impl.sadd(ctx, set_key, ["one"])
    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert {:ok, [["ok", 1], ["ok", 0], ["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "commands" => [
                   %{"opcode" => @op_srem, "body" => %{"key" => set_key, "members" => ["one"]}},
                   %{"opcode" => @op_srem, "body" => %{"key" => set_key, "members" => ["one"]}},
                   %{
                     "opcode" => @op_srem,
                     "body" => %{"key" => string_key, "members" => ["blocked"]}
                   }
                 ]
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, false} = FerricStore.Impl.sismember(ctx, set_key, "one")
    assert [nil] = Router.batch_get(ctx, [CompoundKey.type_key(set_key)])
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "PIPELINE batches native ZADD writes through shard batch apply path" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-zadd-batch-#{System.unique_integer([:positive])}"
    zset_key = prefix <> ":zset"
    string_key = prefix <> ":string"

    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert {:ok, [["ok", 1], ["ok", 0], ["ok", 0], ["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "commands" => [
                   %{
                     "opcode" => @op_zadd,
                     "body" => %{"key" => zset_key, "items" => [[1.0, "one"]]}
                   },
                   %{
                     "opcode" => @op_zadd,
                     "body" => %{"key" => zset_key, "items" => [[2.0, "one"]]}
                   },
                   %{
                     "opcode" => @op_zadd,
                     "body" => %{"key" => zset_key, "items" => [[2.0, "one"]]}
                   },
                   %{
                     "opcode" => @op_zadd,
                     "body" => %{"key" => string_key, "items" => [[1.0, "blocked"]]}
                   }
                 ]
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, "2.0"} = FerricStore.Impl.zscore(ctx, zset_key, "one")
    assert {:ok, ["one"]} = FerricStore.Impl.zrange(ctx, zset_key, 0, -1, [])

    shard_index = Router.shard_for(ctx, zset_key)

    {_index_table, lookup_table} =
      Ferricstore.Store.Shard.ZSetIndex.table_names(ctx.name, shard_index)

    assert Ferricstore.Store.Shard.ZSetIndex.ready?(lookup_table, zset_key)
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "PIPELINE batches native ZREM writes without double-counting duplicate removals" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-zrem-batch-#{System.unique_integer([:positive])}"
    zset_key = prefix <> ":zset"
    string_key = prefix <> ":string"

    assert {:ok, 1} = FerricStore.Impl.zadd(ctx, zset_key, [{1.0, "one"}])
    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert {:ok, [["ok", 1], ["ok", 0], ["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "commands" => [
                   %{"opcode" => @op_zrem, "body" => %{"key" => zset_key, "members" => ["one"]}},
                   %{"opcode" => @op_zrem, "body" => %{"key" => zset_key, "members" => ["one"]}},
                   %{
                     "opcode" => @op_zrem,
                     "body" => %{"key" => string_key, "members" => ["blocked"]}
                   }
                 ]
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"
    assert {:ok, nil} = FerricStore.Impl.zscore(ctx, zset_key, "one")
    assert {:ok, []} = FerricStore.Impl.zrange(ctx, zset_key, 0, -1, [])
    assert [nil] = Router.batch_get(ctx, [CompoundKey.type_key(zset_key)])
    assert {:ok, "plain"} = FerricStore.Impl.get(ctx, string_key)
  end

  test "compact PIPELINE SREM and ZREM use delete write batch semantics" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-compact-delete-batch-#{System.unique_integer([:positive])}"
    set_key = prefix <> ":set"
    zset_key = prefix <> ":zset"

    assert {:ok, 1} = FerricStore.Impl.sadd(ctx, set_key, ["one"])
    assert {:ok, 1} = FerricStore.Impl.zadd(ctx, zset_key, [{1.0, "one"}])

    assert {:ok, [["ok", 1], ["ok", 0]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {31, [{set_key, "one"}, {set_key, "one"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, false} = FerricStore.Impl.sismember(ctx, set_key, "one")
    assert [nil] = Router.batch_get(ctx, [CompoundKey.type_key(set_key)])

    assert {:ok, [["ok", 1], ["ok", 0]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {32, [{zset_key, "one"}, {zset_key, "one"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, nil} = FerricStore.Impl.zscore(ctx, zset_key, "one")
    assert {:ok, []} = FerricStore.Impl.zrange(ctx, zset_key, 0, -1, [])
    assert [nil] = Router.batch_get(ctx, [CompoundKey.type_key(zset_key)])
  end

  test "PIPELINE native ZREM keeps zset type until the last staged member is removed" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-zrem-count-delta-#{System.unique_integer([:positive])}"
    zset_key = prefix <> ":zset"

    assert {:ok, 2} = FerricStore.Impl.zadd(ctx, zset_key, [{1.0, "one"}, {2.0, "two"}])

    assert {:ok, [["ok", 1], ["ok", ["two"]], ["ok", 1], ["ok", []]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "commands" => [
                   %{"opcode" => @op_zrem, "body" => %{"key" => zset_key, "members" => ["one"]}},
                   %{
                     "opcode" => @op_zrange,
                     "body" => %{"key" => zset_key, "start" => 0, "stop" => -1}
                   },
                   %{"opcode" => @op_zrem, "body" => %{"key" => zset_key, "members" => ["two"]}},
                   %{
                     "opcode" => @op_zrange,
                     "body" => %{"key" => zset_key, "start" => 0, "stop" => -1}
                   }
                 ]
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert [nil] = Router.batch_get(ctx, [CompoundKey.type_key(zset_key)])
  end

  test "PIPELINE compact data-structure writes use shard batch apply path" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-compact-ds-write-#{System.unique_integer([:positive])}"

    assert {:ok, [["ok", 1], ["ok", 0]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {22,
                    [
                      {prefix <> ":hash", "field", "one"},
                      {prefix <> ":hash", "field", "two"}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, "two"} = FerricStore.Impl.hget(ctx, prefix <> ":hash", "field")

    assert {:ok, [["ok", 1], ["ok", 2]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {23, [{prefix <> ":lpush", "one"}, {prefix <> ":lpush", "two"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, ["two", "one"]} = FerricStore.Impl.lrange(ctx, prefix <> ":lpush", 0, -1)

    assert {:ok, [["ok", 1], ["ok", 1], ["ok", 2]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {23,
                    [
                      {prefix <> ":lpush:a", "a-one"},
                      {prefix <> ":lpush:b", "b-one"},
                      {prefix <> ":lpush:a", "a-two"}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, ["a-two", "a-one"]} = FerricStore.Impl.lrange(ctx, prefix <> ":lpush:a", 0, -1)
    assert {:ok, ["b-one"]} = FerricStore.Impl.lrange(ctx, prefix <> ":lpush:b", 0, -1)

    assert {:ok, [["ok", 1], ["ok", 2]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {24, [{prefix <> ":rpush", "one"}, {prefix <> ":rpush", "two"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, ["one", "two"]} = FerricStore.Impl.lrange(ctx, prefix <> ":rpush", 0, -1)

    assert {:ok, [["ok", 1], ["ok", 0]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {25, [{prefix <> ":set", "member"}, {prefix <> ":set", "member"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, true} = FerricStore.Impl.sismember(ctx, prefix <> ":set", "member")

    assert {:ok, [["ok", 1], ["ok", 0]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {26,
                    [
                      {prefix <> ":zset", 1.0, "member"},
                      {prefix <> ":zset", 2.0, "member"}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, "2.0"} = FerricStore.Impl.zscore(ctx, prefix <> ":zset", "member")
    assert {:ok, ["member"]} = FerricStore.Impl.zrange(ctx, prefix <> ":zset", 0, -1, [])
  end

  test "PIPELINE keeps many native ZADD-created zsets index-ready for compact ZRANGE" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-zadd-many-#{System.unique_integer([:positive])}"

    commands =
      for i <- 1..128 do
        %{
          "opcode" => @op_zadd,
          "body" => %{"key" => "#{prefix}:#{i}", "items" => [[i * 1.0, "member-#{i}"]]}
        }
      end

    assert {:ok, results, _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "commands" => commands},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert Enum.all?(results, &(&1 == ["ok", 1]))

    for i <- [1, 32, 64, 96, 128] do
      key = "#{prefix}:#{i}"
      shard_index = Router.shard_for(ctx, key)

      {_index_table, lookup_table} =
        Ferricstore.Store.Shard.ZSetIndex.table_names(ctx.name, shard_index)

      assert Ferricstore.Store.Shard.ZSetIndex.ready?(lookup_table, key)

      assert {:ok, ["member-" <> Integer.to_string(i)]} ==
               FerricStore.Impl.zrange(ctx, key, 0, -1, [])
    end

    compact_items =
      for i <- [1, 32, 64, 96, 128] do
        {"#{prefix}:#{i}", 0, -1, false}
      end

    assert {:ok, compact_results, _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, compact_items}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert compact_results == [
             ["ok", ["member-1"]],
             ["ok", ["member-32"]],
             ["ok", ["member-64"]],
             ["ok", ["member-96"]],
             ["ok", ["member-128"]]
           ]
  end

  test "PIPELINE compact ZRANGE reads zset type markers from the zset shard" do
    ctx = FerricStore.Instance.get(:default)
    key = routed_compound_marker_mismatch_key(ctx, "native-zrange-routed-type")
    type_key = CompoundKey.type_key(key)

    assert Router.shard_for(ctx, key) != Router.shard_for(ctx, type_key)

    assert {:ok, [["ok", 1]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "commands" => [
                   %{
                     "opcode" => @op_zadd,
                     "body" => %{"key" => key, "items" => [[1.0, "member"]]}
                   }
                 ]
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert Router.batch_get(ctx, [type_key]) == [nil]
    assert Router.batch_get_on_route_keys(ctx, [{key, type_key}]) == ["zset"]

    assert {:ok, [["ok", ["member"]]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, [{key, 0, -1, false}]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, [["ok", ["member"]]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, [{key, 0, 0, false}]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact ZRANGE returns empty for missing keys and wrongtype for plain strings" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-zrange-missing-#{System.unique_integer([:positive])}"
    missing_key = prefix <> ":missing"
    string_key = prefix <> ":string"

    assert :ok = FerricStore.Impl.set(ctx, string_key, "value")

    assert {:ok, [["ok", []], ["error", wrongtype]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {21, [{missing_key, 0, -1, false}, {string_key, 0, -1, false}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert wrongtype =~ "WRONGTYPE"
  end

  test "PIPELINE compact ZRANGE bounded window reads finite rank range" do
    ctx = FerricStore.Instance.get(:default)
    key = "native-zrange-bounded-#{System.unique_integer([:positive])}"

    zadd_items =
      for i <- 1..20 do
        {key, i * 1.0, "member-#{i}"}
      end

    assert {:ok, write_results, _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {26, zadd_items}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert Enum.all?(write_results, &(&1 == ["ok", 1]))

    assert {:ok, [["ok", ["member-1"]]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, [{key, 0, 0, false}]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, [["ok", ["member-1", "member-2", "member-3"]]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, [{key, 0, 2, false}]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    expected_members = Enum.map(1..20, &"member-#{&1}")

    assert {:ok, [["ok", ^expected_members]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, [{key, 0, 999, false}]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, [["ok", ^expected_members]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, [{key, 0, -1, false}]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, <<0x86, 1::unsigned-32, 20::unsigned-32, _members::binary>>, _state} =
             Commands.execute(
               @op_pipeline,
               %{"compact_values" => true, "compact_pipeline" => {21, [{key, 0, -1, false}]}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact ZRANGE rejects oversized native collection responses when configured" do
    ctx = FerricStore.Instance.get(:default)
    key = "native-zrange-guard-#{System.unique_integer([:positive])}"

    zadd_items =
      for i <- 1..4 do
        {key, i * 1.0, "member-#{i}"}
      end

    assert {:ok, write_results, _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {26, zadd_items}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert Enum.all?(write_results, &(&1 == ["ok", 1]))

    assert {:ok, [["ok", ["member-1", "member-2"]]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, [{key, 0, 1, false}]}},
               state(
                 instance_ctx: ctx,
                 stats_counter: test_stats_counter(),
                 max_collection_response_items: 2
               )
             )

    assert {:bad_request, reason, _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, [{key, 0, -1, false}]}},
               state(
                 instance_ctx: ctx,
                 stats_counter: test_stats_counter(),
                 max_collection_response_items: 2
               )
             )

    assert reason == "ERR native collection response item limit exceeded"
  end

  test "PIPELINE compact ZRANGE counts bounded overlarge windows before materializing" do
    ctx = FerricStore.Instance.get(:default)
    small_key = "native-zrange-small-window-#{System.unique_integer([:positive])}"
    large_key = "native-zrange-large-window-#{System.unique_integer([:positive])}"

    small_items =
      for i <- 1..2 do
        {small_key, i * 1.0, "small-#{i}"}
      end

    large_items =
      for i <- 1..4 do
        {large_key, i * 1.0, "large-#{i}"}
      end

    assert {:ok, write_results, _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {26, small_items ++ large_items}},
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert Enum.all?(write_results, &(&1 == ["ok", 1]))

    assert {:ok, [["ok", ["small-1", "small-2"]]], _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, [{small_key, 0, 1000, false}]}},
               state(
                 instance_ctx: ctx,
                 stats_counter: test_stats_counter(),
                 max_collection_response_items: 2
               )
             )

    assert {:bad_request, reason, _state} =
             Commands.execute(
               @op_pipeline,
               %{"return" => "pairs", "compact_pipeline" => {21, [{large_key, 0, 1000, false}]}},
               state(
                 instance_ctx: ctx,
                 stats_counter: test_stats_counter(),
                 max_collection_response_items: 2
               )
             )

    assert reason == "ERR native collection response item limit exceeded"
  end

  test "PIPELINE compact SET payload writes through direct batch path" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-compact-set-#{System.unique_integer([:positive])}"

    assert {:ok, [["ok", "OK"], ["ok", "OK"]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {1, [{prefix <> ":1", "v1"}, {prefix <> ":2", "v2"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, "v1"} = FerricStore.Impl.get(ctx, prefix <> ":1")
    assert {:ok, "v2"} = FerricStore.Impl.get(ctx, prefix <> ":2")
  end

  test "PIPELINE compact GET payload reads through direct batch path" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-compact-get-#{System.unique_integer([:positive])}"

    assert :ok = FerricStore.Impl.set(ctx, prefix <> ":1", "v1")

    assert {:ok, [["ok", "v1"], ["ok", nil]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {2, [prefix <> ":1", prefix <> ":missing"]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact values mode returns compact MGET payload for GET" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-compact-values-get-#{System.unique_integer([:positive])}"

    assert :ok = FerricStore.Impl.set(ctx, prefix <> ":1", "v1")

    assert {:ok, <<0x83, _rest::binary>>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" => {2, [prefix <> ":1", prefix <> ":missing"]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact values mode returns compact OK list for SET" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-compact-values-set-#{System.unique_integer([:positive])}"

    assert {:ok, <<0x81, _rest::binary>>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" => {1, [{prefix <> ":1", "v1"}, {prefix <> ":2", "v2"}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact values mode returns compact integer list for data-structure writes" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-compact-values-write-#{System.unique_integer([:positive])}"

    assert {:ok, <<0x88, 2::unsigned-32, 1::signed-64, 0::signed-64>>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" =>
                   {22,
                    [
                      {prefix <> ":hash", "field", "one"},
                      {prefix <> ":hash", "field", "two"}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, <<0x88, 2::unsigned-32, 1::signed-64, 1::signed-64>>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" =>
                   {23,
                    [
                      {prefix <> ":left-1", "one"},
                      {prefix <> ":left-2", "two"}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, <<0x88, 2::unsigned-32, 1::signed-64, 1::signed-64>>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" =>
                   {24,
                    [
                      {prefix <> ":right-1", "one"},
                      {prefix <> ":right-2", "two"}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact HGET batch preserves missing and wrongtype semantics" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-compact-hget-#{System.unique_integer([:positive])}"
    hash_key = prefix <> ":hash"
    string_key = prefix <> ":string"
    missing_key = prefix <> ":missing"

    assert {:ok, 1, _state} =
             Commands.execute(
               @op_hset,
               %{"key" => hash_key, "fields" => %{"field" => "value"}},
               state(instance_ctx: ctx)
             )

    assert :ok = FerricStore.Impl.set(ctx, string_key, "plain")

    assert {:ok,
            [
              ["ok", "value"],
              ["ok", nil],
              ["ok", nil],
              ["error", "WRONGTYPE Operation against a key holding the wrong kind of value"]
            ], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {
                   18,
                   [
                     {hash_key, "field"},
                     {hash_key, "missing"},
                     {missing_key, "field"},
                     {string_key, "field"}
                   ]
                 }
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact mixed payload batches independent SET and GET" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-pipeline-compact-mixed-#{System.unique_integer([:positive])}"
    assert :ok = FerricStore.Impl.set(ctx, prefix <> ":read", "v1")

    assert {:ok, [["ok", "v1"], ["ok", "OK"]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {
                   5,
                   [{:get, prefix <> ":read"}, {:set, prefix <> ":write", "v2"}]
                 }
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, "v2"} = FerricStore.Impl.get(ctx, prefix <> ":write")
  end

  test "native VALUE.MGET accepts max byte options" do
    {status, reason, _state} =
      Commands.execute(
        @op_flow_value_mget,
        %{"refs" => [], "max_bytes" => "bad"},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :error
    assert reason =~ "max_bytes"
  end

  test "native FLOW.CREATE rejects obsolete terminal_ttl_ms option" do
    {status, reason, _state} =
      Commands.execute(
        @op_flow_create,
        %{"id" => "flow-1", "type" => "email", "terminal_ttl_ms" => 1000},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :bad_request
    assert reason =~ "terminal_ttl_ms"
  end

  test "native FLOW.CREATE_MANY accepts decoded item maps" do
    id = "native-create-many-#{System.unique_integer([:positive])}"
    type = "native-create-many-#{System.unique_integer([:positive])}"
    now_ms = System.system_time(:millisecond)

    {status, _reply, _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "type" => type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "items" => [%{"id" => id, "payload" => "payload"}]
        },
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
  end

  test "native FLOW.CREATE_MANY accepts compact item arrays" do
    id = "native-create-many-compact-#{System.unique_integer([:positive])}"
    type = "native-create-many-compact-#{System.unique_integer([:positive])}"
    now_ms = System.system_time(:millisecond)

    {status, _reply, _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "type" => type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "items" => [[id, "payload"]]
        },
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
  end

  test "native FLOW.CREATE_MANY accepts compact mixed partition item arrays" do
    id = "native-create-many-mixed-#{System.unique_integer([:positive])}"
    partition = "native-create-many-mixed-partition-#{System.unique_integer([:positive])}"
    type = "native-create-many-mixed-#{System.unique_integer([:positive])}"
    now_ms = System.system_time(:millisecond)

    {status, _reply, _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "type" => type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "items" => [[id, partition, "payload"]]
        },
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
  end

  test "native FLOW.CREATE_MANY accepts trusted compact tuple payload without generic option fields" do
    id = "native-create-many-wire-#{System.unique_integer([:positive])}"
    type = "native-create-many-wire-#{System.unique_integer([:positive])}"
    now_ms = System.system_time(:millisecond)

    {status, reply, _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "items" => [{:id, id, :payload, "payload"}],
          __wire_flow_items_normalized__: true,
          __wire_flow_opts__: [
            return: :ok_on_success,
            independent: true,
            type: type,
            state: "queued",
            now_ms: now_ms,
            run_at_ms: now_ms
          ]
        },
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
    assert reply == "OK"
  end

  test "PIPELINE same_shard validates compact FLOW.CREATE_MANY item arrays" do
    ctx = FerricStore.Instance.get(:default)
    {id1, id2} = different_shard_ids(ctx, "native-create-many-same-shard")
    now_ms = System.system_time(:millisecond)

    {status, reason, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "atomicity" => "same_shard",
          "commands" => [
            %{
              "opcode" => @op_flow_create_many,
              "lane_id" => 1,
              "request_id" => 501,
              "body" => %{
                "type" => "native-create-many-same-shard",
                "state" => "queued",
                "now_ms" => now_ms,
                "run_at_ms" => now_ms,
                "independent" => true,
                "items" => [[id1, "payload"], [id2, "payload"]]
              }
            }
          ]
        },
        state(instance_ctx: ctx)
      )

    assert status == :bad_request
    assert reason =~ "same_shard"
  end

  test "PIPELINE batches native FLOW.CREATE writes" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-flow-create-#{System.unique_integer([:positive])}"
    id1 = "#{type}:1"
    id2 = "#{type}:2"

    {status, reply, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "commands" => [
            %{
              "opcode" => @op_flow_create,
              "lane_id" => 1,
              "request_id" => 1,
              "body" => %{"id" => id1, "type" => type, "state" => "queued"}
            },
            %{
              "opcode" => @op_flow_create,
              "lane_id" => 1,
              "request_id" => 2,
              "body" => %{"id" => id2, "type" => type, "state" => "queued"}
            }
          ]
        },
        state(instance_ctx: ctx, stats_counter: test_stats_counter())
      )

    assert status == :ok
    assert reply == [["ok", "OK"], ["ok", "OK"]]
  end

  test "PIPELINE batches native FLOW.SIGNAL writes through Flow pipeline path" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-flow-signal-#{System.unique_integer([:positive])}"
    id1 = "#{type}:1"
    id2 = "#{type}:2"
    now_ms = System.system_time(:millisecond)

    {create_status, "OK", _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "type" => type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "return" => "ok_on_success",
          "items" => [[id1, "payload"], [id2, "payload"]]
        },
        state(instance_ctx: ctx)
      )

    assert create_status == :ok

    {status, reply, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "commands" => [
            %{
              "opcode" => @op_flow_signal,
              "lane_id" => 1,
              "request_id" => 1,
              "body" => %{
                "id" => id1,
                "signal" => "external_event",
                "now_ms" => now_ms
              }
            },
            %{
              "opcode" => @op_flow_signal,
              "lane_id" => 1,
              "request_id" => 2,
              "body" => %{
                "id" => id2,
                "signal" => "external_event",
                "now_ms" => now_ms
              }
            }
          ]
        },
        state(instance_ctx: ctx, stats_counter: test_stats_counter())
      )

    assert status == :ok
    assert reply == [["ok", "OK"], ["ok", "OK"]]
  end

  test "PIPELINE batches native FLOW read commands through Flow read pipeline path" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-flow-read-#{System.unique_integer([:positive])}"
    id = "#{type}:1"
    now_ms = System.system_time(:millisecond)
    handler_id = {__MODULE__, self(), :native_flow_pipeline_read}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :flow, :pipeline_read_batch],
      fn _event, measurements, metadata, pid ->
        send(pid, {:pipeline_read_batch, measurements, metadata})
      end,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {create_status, "OK", _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "type" => type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "return" => "ok_on_success",
          "items" => [[id, "payload"]]
        },
        state(instance_ctx: ctx)
      )

    assert create_status == :ok

    {status, reply, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "commands" => [
            %{
              "opcode" => @op_flow_get,
              "lane_id" => 1,
              "request_id" => 1,
              "body" => %{"id" => id}
            },
            %{
              "opcode" => @op_flow_history,
              "lane_id" => 1,
              "request_id" => 2,
              "body" => %{"id" => id, "count" => 1, "include_cold" => false}
            },
            %{
              "opcode" => @op_flow_list,
              "lane_id" => 1,
              "request_id" => 3,
              "body" => %{"type" => type, "state" => "queued", "count" => 1}
            }
          ]
        },
        state(instance_ctx: ctx, stats_counter: test_stats_counter())
      )

    assert status == :ok
    assert [["ok", record], ["ok", history], ["ok", listed]] = reply
    assert record.id == id
    assert is_list(history)
    assert is_list(listed)

    assert_receive {:pipeline_read_batch, %{count: 3, gets: 1, histories: 1},
                    %{source: :pipeline}},
                   1_000
  end

  test "PIPELINE compact FLOW.GET payload reads through Flow read pipeline path" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-compact-flow-get-#{System.unique_integer([:positive])}"
    id = "#{type}:1"
    now_ms = System.system_time(:millisecond)
    handler_id = {__MODULE__, self(), :native_compact_flow_get}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :flow, :pipeline_read_batch],
      fn _event, measurements, metadata, pid ->
        send(pid, {:pipeline_read_batch, measurements, metadata})
      end,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create,
               %{
                 "id" => id,
                 "type" => type,
                 "state" => "queued",
                 "now_ms" => now_ms,
                 "run_at_ms" => now_ms,
                 "payload" => "payload"
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, [["ok", record]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {9, [{:flow_get, id, []}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert record.id == id

    assert_receive {:pipeline_read_batch, %{count: 1, gets: 1}, %{source: :pipeline}}, 1_000

    assert {:ok, <<0x85, 1::unsigned-32, _rest::binary>>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" => {9, [{:flow_get, id, []}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact partitioned FLOW.GET payload reads through Flow read pipeline path" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-compact-partitioned-flow-get-#{System.unique_integer([:positive])}"
    id = "#{type}:1"
    partition = "tenant-a"
    now_ms = System.system_time(:millisecond)

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create,
               %{
                 "id" => id,
                 "type" => type,
                 "state" => "queued",
                 "partition_key" => partition,
                 "now_ms" => now_ms,
                 "run_at_ms" => now_ms,
                 "payload" => "payload"
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, [["ok", record]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {16, [{:flow_get, id, [partition_key: partition]}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert record.id == id
    assert record.partition_key == partition
  end

  test "PIPELINE compact FLOW.GET preserves validation errors on invalid ids" do
    ctx = FerricStore.Instance.get(:default)

    assert {:ok, [["error", "ERR flow id must be a non-empty string"]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" => {9, [{:flow_get, "", []}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact FLOW.GET meta payload trims heavy fields" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-compact-flow-get-meta-#{System.unique_integer([:positive])}"
    id = "#{type}:1"
    partition = "tenant-meta"
    now_ms = System.system_time(:millisecond)

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create,
               %{
                 "id" => id,
                 "type" => type,
                 "state" => "queued",
                 "partition_key" => partition,
                 "now_ms" => now_ms,
                 "run_at_ms" => now_ms,
                 "payload" => String.duplicate("x", 1024)
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, [["ok", record]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {17, [{:flow_get, id, [partition_key: partition, return: :meta]}]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert record.id == id
    assert record.type == type
    assert record.state == "queued"
    assert record.partition_key == partition
    refute Map.has_key?(record, :payload)
  end

  test "PIPELINE compact FLOW.HISTORY payload reads through Flow read pipeline path" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-compact-flow-history-#{System.unique_integer([:positive])}"
    id = "#{type}:1"
    now_ms = System.system_time(:millisecond)
    handler_id = {__MODULE__, self(), :native_compact_flow_history}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :flow, :pipeline_read_batch],
      fn _event, measurements, metadata, pid ->
        send(pid, {:pipeline_read_batch, measurements, metadata})
      end,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create,
               %{
                 "id" => id,
                 "type" => type,
                 "state" => "queued",
                 "now_ms" => now_ms,
                 "run_at_ms" => now_ms,
                 "payload" => "payload"
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, [["ok", history]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {10,
                    [
                      {:flow_history, id,
                       [
                         count: 10,
                         include_cold: false,
                         consistent_projection: true
                       ]}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert is_list(history)

    assert_receive {:pipeline_read_batch, %{count: 1, histories: 1}, %{source: :pipeline}}, 1_000
  end

  test "PIPELINE compact FLOW.SIGNAL payload writes through Flow pipeline path" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-compact-flow-signal-#{System.unique_integer([:positive])}"
    id = "#{type}:1"
    now_ms = System.system_time(:millisecond)

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create,
               %{
                 "id" => id,
                 "type" => type,
                 "state" => "queued",
                 "now_ms" => now_ms,
                 "run_at_ms" => now_ms,
                 "payload" => "payload"
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, [["ok", "OK"]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {11,
                    [
                      {:flow_signal, id,
                       [
                         signal: "bench_signal",
                         if_state: "queued",
                         transition_to: "next",
                         now_ms: now_ms
                       ]}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, record} = Ferricstore.Flow.get(ctx, id, [])
    assert record.state == "next"
  end

  test "PIPELINE batches native owned FLOW.VALUE.PUT writes through Flow pipeline path" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-flow-value-put-#{System.unique_integer([:positive])}"
    id1 = "#{type}:1"
    id2 = "#{type}:2"
    now_ms = System.system_time(:millisecond)

    {create_status, "OK", _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "type" => type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "return" => "ok_on_success",
          "items" => [[id1, "payload"], [id2, "payload"]]
        },
        state(instance_ctx: ctx)
      )

    assert create_status == :ok

    {status, reply, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "commands" => [
            %{
              "opcode" => @op_flow_value_put,
              "lane_id" => 1,
              "request_id" => 1,
              "body" => %{
                "value" => "reservation-v1",
                "owner_flow_id" => id1,
                "name" => "reservation",
                "now_ms" => now_ms + 1
              }
            },
            %{
              "opcode" => @op_flow_value_put,
              "lane_id" => 1,
              "request_id" => 2,
              "body" => %{
                "value" => "reservation-v2",
                "owner_flow_id" => id2,
                "name" => "reservation",
                "now_ms" => now_ms + 1
              }
            }
          ]
        },
        state(instance_ctx: ctx, stats_counter: test_stats_counter())
      )

    assert status == :ok
    assert [["ok", value1], ["ok", value2]] = reply
    assert value1.owner_flow_id == id1
    assert value1.name == "reservation"
    assert value1.created == true
    assert value1.stored == true
    assert value2.owner_flow_id == id2
    assert value2.name == "reservation"
  end

  test "PIPELINE compact owned FLOW.VALUE.PUT preserves per-item duplicate failures" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-compact-flow-value-put-#{System.unique_integer([:positive])}"
    id1 = "#{type}:1"
    id2 = "#{type}:2"
    now_ms = System.system_time(:millisecond)

    {create_status, "OK", _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "type" => type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "return" => "ok_on_success",
          "items" => [[id1, "payload"], [id2, "payload"]]
        },
        state(instance_ctx: ctx)
      )

    assert create_status == :ok

    assert {:ok, [["ok", value1], ["ok", value2]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {8,
                    [
                      {:flow_named_value_put, "reservation-v1",
                       [owner_flow_id: id1, name: "reservation", now_ms: now_ms + 1]},
                      {:flow_named_value_put, "reservation-v2",
                       [owner_flow_id: id2, name: "reservation", now_ms: now_ms + 1]}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert value1.owner_flow_id == id1
    assert value1.name == "reservation"
    assert value1.created == true
    assert value1.stored == true
    assert value2.owner_flow_id == id2
    assert value2.name == "reservation"
    assert value2.created == true
    assert value2.stored == true

    assert {:ok, [["error", reason]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {8,
                    [
                      {:flow_named_value_put, "reservation-v3",
                       [owner_flow_id: id1, name: "reservation", now_ms: now_ms + 2]}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert reason =~ "already exists with different digest"
  end

  test "PIPELINE compact owned FLOW.VALUE.PUT is idempotent for same value digest" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-compact-flow-value-put-idempotent-#{System.unique_integer([:positive])}"
    id = "#{type}:1"
    now_ms = System.system_time(:millisecond)

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create,
               %{
                 "id" => id,
                 "type" => type,
                 "state" => "queued",
                 "now_ms" => now_ms,
                 "run_at_ms" => now_ms,
                 "payload" => "payload"
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, [["ok", first], ["ok", second]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {8,
                    [
                      {:flow_named_value_put, "reservation-v1",
                       [owner_flow_id: id, name: "reservation", now_ms: now_ms + 1]},
                      {:flow_named_value_put, "reservation-v1",
                       [owner_flow_id: id, name: "reservation", now_ms: now_ms + 2]}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert first.owner_flow_id == id
    assert first.name == "reservation"
    assert first.created == true
    assert first.stored == true
    assert second.owner_flow_id == id
    assert second.name == "reservation"
    assert second.ref == first.ref
    assert second.version == first.version
    assert second.created == false
    assert second.stored == false
  end

  test "PIPELINE compact owned FLOW.VALUE.PUT supports success-only return mode" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-compact-flow-value-put-ok-#{System.unique_integer([:positive])}"
    id1 = "#{type}:1"
    id2 = "#{type}:2"
    now_ms = System.system_time(:millisecond)

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create_many,
               %{
                 "type" => type,
                 "state" => "queued",
                 "now_ms" => now_ms,
                 "run_at_ms" => now_ms,
                 "independent" => true,
                 "return" => "ok_on_success",
                 "items" => [[id1, "payload"], [id2, "payload"]]
               },
               state(instance_ctx: ctx)
             )

    assert {:ok, [["ok", "OK"], ["ok", "OK"]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {14,
                    [
                      {:flow_named_value_put, "reservation-v1",
                       [
                         owner_flow_id: id1,
                         name: "reservation",
                         now_ms: now_ms + 1,
                         return: :ok_on_success
                       ]},
                      {:flow_named_value_put, "reservation-v2",
                       [
                         owner_flow_id: id2,
                         name: "reservation",
                         now_ms: now_ms + 1,
                         return: :ok_on_success
                       ]}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )

    assert {:ok, record1} = Ferricstore.Flow.get(ctx, id1, [])
    assert {:ok, record2} = Ferricstore.Flow.get(ctx, id2, [])
    assert get_in(record1.value_refs, ["reservation", :ref])
    assert get_in(record2.value_refs, ["reservation", :ref])
  end

  test "PIPELINE compact shared FLOW.VALUE.PUT supports success-only return mode" do
    ctx = FerricStore.Instance.get(:default)
    now_ms = System.system_time(:millisecond)

    assert {:ok, [["ok", "OK"], ["ok", "OK"]], _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "return" => "pairs",
                 "compact_pipeline" =>
                   {15,
                    [
                      {"shared-v1", [now_ms: now_ms, return: :ok_on_success]},
                      {"shared-v2", [now_ms: now_ms + 1, return: :ok_on_success]}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE batches native shared FLOW.VALUE.PUT writes through direct put path" do
    ctx = FerricStore.Instance.get(:default)
    now_ms = System.system_time(:millisecond)

    {status, reply, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "commands" => [
            %{
              "opcode" => @op_flow_value_put,
              "lane_id" => 1,
              "request_id" => 1,
              "body" => %{"value" => "shared-v1", "now_ms" => now_ms}
            },
            %{
              "opcode" => @op_flow_value_put,
              "lane_id" => 1,
              "request_id" => 2,
              "body" => %{"value" => "shared-v2", "now_ms" => now_ms}
            }
          ]
        },
        state(instance_ctx: ctx, stats_counter: test_stats_counter())
      )

    assert status == :ok
    assert [["ok", value1], ["ok", value2]] = reply
    assert is_binary(value1.ref)
    assert is_binary(value2.ref)

    {mget_status, values, _state} =
      Commands.execute(
        @op_flow_value_mget,
        %{"refs" => [value1.ref, value2.ref]},
        state(instance_ctx: ctx)
      )

    assert mget_status == :ok
    assert values == ["shared-v1", "shared-v2"]
  end

  test "PIPELINE batches native FLOW.STEP_CONTINUE writes through Flow pipeline path" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-flow-step-#{System.unique_integer([:positive])}"
    id1 = "#{type}:1"
    id2 = "#{type}:2"
    now_ms = System.system_time(:millisecond)

    {:ok, started1, _state} =
      Commands.execute(
        @op_flow_start_and_claim,
        %{
          "id" => id1,
          "type" => type,
          "initial_state" => "reserve_inventory",
          "worker" => "native-worker",
          "lease_ms" => 7_000,
          "now_ms" => now_ms
        },
        state(instance_ctx: ctx)
      )

    {:ok, started2, _state} =
      Commands.execute(
        @op_flow_start_and_claim,
        %{
          "id" => id2,
          "type" => type,
          "initial_state" => "reserve_inventory",
          "worker" => "native-worker",
          "lease_ms" => 7_000,
          "now_ms" => now_ms
        },
        state(instance_ctx: ctx)
      )

    {status, reply, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "commands" => [
            %{
              "opcode" => @op_flow_step_continue,
              "lane_id" => 1,
              "request_id" => 1,
              "body" => %{
                "id" => id1,
                "lease_token" => started1.lease_token,
                "from_state" => "reserve_inventory",
                "to_state" => "charge_card",
                "fencing_token" => started1.fencing_token,
                "partition_key" => started1.partition_key,
                "lease_ms" => 5_000,
                "now_ms" => now_ms + 1
              }
            },
            %{
              "opcode" => @op_flow_step_continue,
              "lane_id" => 1,
              "request_id" => 2,
              "body" => %{
                "id" => id2,
                "lease_token" => started2.lease_token,
                "from_state" => "reserve_inventory",
                "to_state" => "charge_card",
                "fencing_token" => started2.fencing_token,
                "partition_key" => started2.partition_key,
                "lease_ms" => 5_000,
                "now_ms" => now_ms + 1
              }
            }
          ]
        },
        state(instance_ctx: ctx, stats_counter: test_stats_counter())
      )

    assert status == :ok
    assert [["ok", continued1], ["ok", continued2]] = reply
    assert continued1.id == id1
    assert continued1.state == "running"
    assert continued1.run_state == "charge_card"
    assert continued1.fencing_token == started1.fencing_token + 1
    assert continued2.id == id2
    assert continued2.state == "running"
    assert continued2.run_state == "charge_card"
  end

  test "compact PIPELINE mode 33 batches FLOW.STEP_CONTINUE and returns compact jobs" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-compact-flow-step-job-#{System.unique_integer([:positive])}"
    id = "#{type}:1"
    now_ms = System.system_time(:millisecond)

    {:ok, started, _state} =
      Commands.execute(
        @op_flow_start_and_claim,
        %{
          "id" => id,
          "type" => type,
          "initial_state" => "reserve_inventory",
          "worker" => "native-worker",
          "lease_ms" => 7_000,
          "now_ms" => now_ms
        },
        state(instance_ctx: ctx)
      )

    {status, reply, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "atomicity" => "none",
          "return" => "compact",
          "compact_values" => true,
          "compact_count" => 1,
          "compact_pipeline" =>
            {33,
             [
               {:flow_step_continue, id, started.lease_token, "reserve_inventory", "charge_card",
                [
                  partition_key: started.partition_key,
                  fencing_token: started.fencing_token,
                  lease_ms: 5_000,
                  now_ms: now_ms + 1,
                  return: :jobs_compact
                ]}
             ]}
        },
        state(instance_ctx: ctx, stats_counter: test_stats_counter())
      )

    assert status == :ok
    assert <<0x80, 1::unsigned-32, _rest::binary>> = reply
    assert reply =~ id
  end

  test "PIPELINE preserves independent errors for duplicate native FLOW.STEP_CONTINUE items" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-flow-step-duplicate-#{System.unique_integer([:positive])}"
    id = "#{type}:1"
    now_ms = System.system_time(:millisecond)

    {:ok, started, _state} =
      Commands.execute(
        @op_flow_start_and_claim,
        %{
          "id" => id,
          "type" => type,
          "initial_state" => "reserve_inventory",
          "worker" => "native-worker",
          "lease_ms" => 7_000,
          "now_ms" => now_ms
        },
        state(instance_ctx: ctx)
      )

    command = %{
      "opcode" => @op_flow_step_continue,
      "lane_id" => 1,
      "body" => %{
        "id" => id,
        "lease_token" => started.lease_token,
        "from_state" => "reserve_inventory",
        "to_state" => "charge_card",
        "fencing_token" => started.fencing_token,
        "partition_key" => started.partition_key,
        "lease_ms" => 5_000,
        "now_ms" => now_ms + 1
      }
    }

    {status, reply, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "commands" => [
            Map.put(command, "request_id", 1),
            Map.put(command, "request_id", 2)
          ]
        },
        state(instance_ctx: ctx, stats_counter: test_stats_counter())
      )

    assert status == :ok
    assert [["ok", continued], ["error", "ERR stale flow lease"]] = reply
    assert continued.id == id
    assert continued.state == "running"
    assert continued.run_state == "charge_card"
  end

  test "PIPELINE batches native FLOW.START_AND_CLAIM writes through Flow pipeline path" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-flow-start-#{System.unique_integer([:positive])}"
    id1 = "#{type}:1"
    id2 = "#{type}:2"
    now_ms = System.system_time(:millisecond)

    {status, reply, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "commands" => [
            %{
              "opcode" => @op_flow_start_and_claim,
              "lane_id" => 1,
              "request_id" => 1,
              "body" => %{
                "id" => id1,
                "type" => type,
                "initial_state" => "reserve_inventory",
                "worker" => "native-worker",
                "lease_ms" => 7_000,
                "now_ms" => now_ms
              }
            },
            %{
              "opcode" => @op_flow_start_and_claim,
              "lane_id" => 1,
              "request_id" => 2,
              "body" => %{
                "id" => id2,
                "type" => type,
                "initial_state" => "reserve_inventory",
                "worker" => "native-worker",
                "lease_ms" => 7_000,
                "now_ms" => now_ms
              }
            }
          ]
        },
        state(instance_ctx: ctx, stats_counter: test_stats_counter())
      )

    assert status == :ok
    assert [["ok", started1], ["ok", started2]] = reply
    assert started1.id == id1
    assert started1.state == "running"
    assert started1.run_state == "reserve_inventory"
    assert started1.fencing_token == 1
    assert started2.id == id2
    assert started2.state == "running"
    assert started2.run_state == "reserve_inventory"
    assert started2.fencing_token == 1
  end

  test "PIPELINE compact FLOW.START_AND_CLAIM writes through Flow pipeline path" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-compact-flow-start-#{System.unique_integer([:positive])}"
    id1 = "#{type}:1"
    id2 = "#{type}:2"
    now_ms = System.system_time(:millisecond)

    {status, reply, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "compact_pipeline" =>
            {12,
             [
               {:flow_start_and_claim, id1, type, "reserve_inventory",
                [
                  worker: "native-worker",
                  lease_ms: 7_000,
                  now_ms: now_ms,
                  payload: "payload-1"
                ]},
               {:flow_start_and_claim, id2, type, "reserve_inventory",
                [
                  worker: "native-worker",
                  lease_ms: 7_000,
                  now_ms: now_ms,
                  payload: "payload-2"
                ]}
             ]}
        },
        state(instance_ctx: ctx, stats_counter: test_stats_counter())
      )

    assert status == :ok
    assert [["ok", started1], ["ok", started2]] = reply
    assert started1.id == id1
    assert started1.state == "running"
    assert started1.run_state == "reserve_inventory"
    assert started1.lease_owner == "native-worker"
    assert started2.id == id2
    assert started2.state == "running"
    assert started2.run_state == "reserve_inventory"
    assert started2.lease_owner == "native-worker"
  end

  test "PIPELINE compact FLOW.START_AND_CLAIM can return job-only tuples" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-compact-flow-start-job-#{System.unique_integer([:positive])}"
    id1 = "#{type}:1"
    id2 = "#{type}:2"
    now_ms = System.system_time(:millisecond)

    {status, reply, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "compact_pipeline" =>
            {13,
             [
               {:flow_start_and_claim, id1, type, "reserve_inventory",
                [
                  return: :jobs_compact,
                  worker: "native-worker",
                  lease_ms: 7_000,
                  now_ms: now_ms,
                  payload: "payload-1"
                ]},
               {:flow_start_and_claim, id2, type, "reserve_inventory",
                [
                  return: :jobs_compact,
                  worker: "native-worker",
                  lease_ms: 7_000,
                  now_ms: now_ms,
                  payload: "payload-2"
                ]}
             ]}
        },
        state(instance_ctx: ctx, stats_counter: test_stats_counter())
      )

    assert status == :ok

    assert [
             ["ok", [^id1, partition1, lease1, 1]],
             ["ok", [^id2, partition2, lease2, 1]]
           ] = reply

    assert is_binary(partition1)
    assert is_binary(partition2)
    assert is_binary(lease1)
    assert is_binary(lease2)
  end

  test "PIPELINE compact values FLOW.START_AND_CLAIM returns compact claim jobs" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-compact-flow-start-values-#{System.unique_integer([:positive])}"
    id1 = "#{type}:1"
    id2 = "#{type}:2"
    now_ms = System.system_time(:millisecond)

    assert {:ok, <<0x80, 2::unsigned-32, _rest::binary>>, _state} =
             Commands.execute(
               @op_pipeline,
               %{
                 "compact_values" => true,
                 "compact_pipeline" =>
                   {13,
                    [
                      {:flow_start_and_claim, id1, type, "reserve_inventory",
                       [
                         return: :jobs_compact,
                         worker: "native-worker",
                         lease_ms: 7_000,
                         now_ms: now_ms,
                         payload: "payload-1"
                       ]},
                      {:flow_start_and_claim, id2, type, "reserve_inventory",
                       [
                         return: :jobs_compact,
                         worker: "native-worker",
                         lease_ms: 7_000,
                         now_ms: now_ms,
                         payload: "payload-2"
                       ]}
                    ]}
               },
               state(instance_ctx: ctx, stats_counter: test_stats_counter())
             )
  end

  test "PIPELINE compact FLOW.START_AND_CLAIM preserves per-item duplicate failures" do
    ctx = FerricStore.Instance.get(:default)
    type = "native-pipeline-compact-flow-start-dup-#{System.unique_integer([:positive])}"
    existing_id = "#{type}:existing"
    new_id = "#{type}:new"
    partition_key = "native-start-claim-dup-#{System.unique_integer([:positive])}"
    now_ms = System.system_time(:millisecond)

    assert {:ok, _started} =
             FerricStore.flow_start_and_claim(existing_id, type, "reserve_inventory",
               worker: "native-worker",
               lease_ms: 7_000,
               now_ms: now_ms,
               partition_key: partition_key
             )

    {status, reply, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "return" => "pairs",
          "compact_pipeline" =>
            {13,
             [
               {:flow_start_and_claim, existing_id, type, "reserve_inventory",
                [
                  return: :jobs_compact,
                  worker: "native-worker",
                  lease_ms: 7_000,
                  now_ms: now_ms + 1,
                  partition_key: partition_key
                ]},
               {:flow_start_and_claim, new_id, type, "reserve_inventory",
                [
                  return: :jobs_compact,
                  worker: "native-worker",
                  lease_ms: 7_000,
                  now_ms: now_ms + 1,
                  partition_key: partition_key
                ]}
             ]}
        },
        state(instance_ctx: ctx, stats_counter: test_stats_counter())
      )

    assert status == :ok

    assert [["error", "ERR flow already exists"], ["ok", [^new_id, ^partition_key, lease, 1]]] =
             reply

    assert is_binary(lease)
  end

  test "native FLOW.CREATE_MANY can return OK on all-success independent create" do
    id = "native-create-many-ok-#{System.unique_integer([:positive])}"
    type = "native-create-many-ok-#{System.unique_integer([:positive])}"
    now_ms = System.system_time(:millisecond)

    {status, reply, _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "type" => type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "return" => "ok_on_success",
          "items" => [[id, "payload"]]
        },
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
    assert reply == "OK"
  end

  test "native Flow many mutations can return OK on all-success independent batches" do
    ctx = FerricStore.Instance.get(:default)

    now_ms = System.system_time(:millisecond)
    transition_type = "native-many-ok-transition-#{System.unique_integer([:positive])}"
    transition_partition = "native-many-ok-transition-partition"

    {create_status, "OK", _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "partition_key" => transition_partition,
          "type" => transition_type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "return" => "ok_on_success",
          "items" => [["#{transition_type}:1", "payload"], ["#{transition_type}:2", "payload"]]
        },
        state(instance_ctx: ctx)
      )

    assert create_status == :ok

    {transition_status, transition_reply, _state} =
      Commands.execute(
        @op_flow_transition_many,
        %{
          "from_state" => "queued",
          "to_state" => "next",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "return" => "ok_on_success",
          "items" => [
            ["#{transition_type}:1", transition_partition, 0, nil],
            ["#{transition_type}:2", transition_partition, 0, nil]
          ]
        },
        state(instance_ctx: ctx)
      )

    assert transition_status == :ok
    assert transition_reply == "OK"

    cancel_type = "native-many-ok-cancel-#{System.unique_integer([:positive])}"
    cancel_partition = "native-many-ok-cancel-partition"

    {cancel_create_status, "OK", _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "partition_key" => cancel_partition,
          "type" => cancel_type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "return" => "ok_on_success",
          "items" => [["#{cancel_type}:1", "payload"], ["#{cancel_type}:2", "payload"]]
        },
        state(instance_ctx: ctx)
      )

    assert cancel_create_status == :ok

    {cancel_status, cancel_reply, _state} =
      Commands.execute(
        @op_flow_cancel_many,
        %{
          "now_ms" => now_ms,
          "independent" => true,
          "return" => "ok_on_success",
          "items" => [
            ["#{cancel_type}:1", cancel_partition, 0],
            ["#{cancel_type}:2", cancel_partition, 0]
          ]
        },
        state(instance_ctx: ctx)
      )

    assert cancel_status == :ok
    assert cancel_reply == "OK"

    for {op, payload_fun} <- [
          {@op_flow_retry_many,
           fn jobs, now_ms ->
             %{
               "now_ms" => now_ms,
               "run_at_ms" => now_ms,
               "independent" => true,
               "return" => "ok_on_success",
               "items" => jobs
             }
           end},
          {@op_flow_fail_many,
           fn jobs, now_ms ->
             %{
               "now_ms" => now_ms,
               "independent" => true,
               "return" => "ok_on_success",
               "items" => jobs
             }
           end}
        ] do
      now_ms = System.system_time(:millisecond)
      type = "native-many-ok-#{op}-#{System.unique_integer([:positive])}"
      partition_key = "native-many-ok-partition-#{System.unique_integer([:positive])}"

      {create_status, "OK", _state} =
        Commands.execute(
          @op_flow_create_many,
          %{
            "partition_key" => partition_key,
            "type" => type,
            "state" => "queued",
            "now_ms" => now_ms,
            "run_at_ms" => now_ms,
            "independent" => true,
            "return" => "ok_on_success",
            "items" => [["#{type}:1", "payload"], ["#{type}:2", "payload"]]
          },
          state(instance_ctx: ctx)
        )

      assert create_status == :ok

      {claim_status, jobs, _state} =
        Commands.execute(
          @op_flow_claim_due,
          %{
            "type" => type,
            "state" => "queued",
            "worker" => "native-worker",
            "limit" => 2,
            "lease_ms" => 30_000,
            "partition_keys" => [partition_key],
            "return" => "jobs_compact"
          },
          state(instance_ctx: ctx, compact_flow_responses: true)
        )

      assert claim_status == :ok
      assert length(jobs) == 2

      {status, reply, _state} =
        Commands.execute(op, payload_fun.(jobs, now_ms), state(instance_ctx: ctx))

      assert status == :ok
      assert reply == "OK"
    end
  end

  test "native FLOW.SIGNAL accepts guards and transition options" do
    ctx = FerricStore.Instance.get(:default)
    now_ms = System.system_time(:millisecond)
    id = "native-signal-#{System.unique_integer([:positive])}"
    type = "native-signal-type-#{System.unique_integer([:positive])}"
    partition_key = "native-signal-partition"

    {create_status, "OK", _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "partition_key" => partition_key,
          "type" => type,
          "state" => "queued",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms,
          "independent" => true,
          "return" => "ok_on_success",
          "items" => [[id, "payload"]]
        },
        state(instance_ctx: ctx)
      )

    assert create_status == :ok

    {status, reply, _state} =
      Commands.execute(
        @op_flow_signal,
        %{
          "id" => id,
          "partition_key" => partition_key,
          "signal" => "payment_received",
          "if_state" => "queued",
          "transition_to" => "paid",
          "now_ms" => now_ms,
          "run_at_ms" => now_ms
        },
        state(instance_ctx: ctx)
      )

    assert status == :ok
    assert reply == "OK"
  end

  test "native FLOW.START_AND_CLAIM starts a leased first step" do
    id = "native-start-and-claim-#{System.unique_integer([:positive])}"
    type = "native-start-and-claim-type-#{System.unique_integer([:positive])}"
    now_ms = System.system_time(:millisecond)

    {status, reply, _state} =
      Commands.execute(
        @op_flow_start_and_claim,
        %{
          "id" => id,
          "type" => type,
          "initial_state" => "reserve_inventory",
          "worker" => "native-worker",
          "lease_ms" => 7_000,
          "now_ms" => now_ms,
          "payload" => %{"order_id" => id}
        },
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
    assert reply.id == id
    assert reply.type == type
    assert reply.state == "running"
    assert reply.run_state == "reserve_inventory"
    assert reply.attempts == 1
    assert reply.fencing_token == 1
    assert reply.lease_owner == "native-worker"
    assert is_binary(reply.lease_token)
    assert reply.lease_deadline_ms == now_ms + 7_000
  end

  test "native FLOW.STEP_CONTINUE advances a leased step and returns fresh fencing" do
    id = "native-step-continue-#{System.unique_integer([:positive])}"
    type = "native-step-continue-type-#{System.unique_integer([:positive])}"
    now_ms = System.system_time(:millisecond)
    ctx = FerricStore.Instance.get(:default)

    {:ok, started, _state} =
      Commands.execute(
        @op_flow_start_and_claim,
        %{
          "id" => id,
          "type" => type,
          "initial_state" => "reserve_inventory",
          "worker" => "native-worker",
          "lease_ms" => 7_000,
          "now_ms" => now_ms
        },
        state(instance_ctx: ctx)
      )

    {status, continued, _state} =
      Commands.execute(
        @op_flow_step_continue,
        %{
          "id" => id,
          "lease_token" => started.lease_token,
          "from_state" => "reserve_inventory",
          "to_state" => "charge_card",
          "fencing_token" => started.fencing_token,
          "partition_key" => started.partition_key,
          "lease_ms" => 5_000,
          "now_ms" => now_ms + 1
        },
        state(instance_ctx: ctx)
      )

    assert status == :ok
    assert continued.state == "running"
    assert continued.run_state == "charge_card"
    assert continued.lease_token != started.lease_token
    assert continued.fencing_token == started.fencing_token + 1
    assert continued.lease_deadline_ms == now_ms + 5_001
  end

  test "native FLOW.CLAIM_DUE accepts reclaim options" do
    {status, reply, _state} =
      Commands.execute(
        @op_flow_claim_due,
        %{
          "type" => "native-claim-reclaim",
          "state" => "queued",
          "worker" => "w1",
          "limit" => 1,
          "lease_ms" => 1000,
          "reclaim_expired" => false,
          "reclaim_ratio" => 0
        },
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
    assert reply == []
  end

  test "SUBSCRIBE_EVENTS registers persistent Flow wake filters through claim waiters" do
    try do
      ClaimWaiters.cleanup(self())
      type = "native-wake-#{System.unique_integer([:positive])}"

      {status, reply, new_state} =
        Commands.execute(
          @op_subscribe_events,
          %{
            "events" => ["FLOW_WAKE"],
            "flow_wake" => %{
              "type" => type,
              "state" => "queued",
              "priority" => 0,
              "partition_keys" => ["bucket-0", "bucket-1"],
              "limit" => 500
            }
          },
          state()
        )

      assert status == :ok
      assert "FLOW_WAKE" in reply.subscribed
      assert new_state.event_subscriptions == MapSet.new(["FLOW_WAKE"])
      assert %{type: ^type, limit: 500, keys: keys} = new_state.flow_wake_subscription
      assert length(keys) == 2
      assert ClaimWaiters.total_count() == 2

      assert Commands.flow_wake_event_payload(new_state) == %{
               type: type,
               credit: 500,
               reason: "ready"
             }

      assert Commands.refresh_flow_wake_subscription(new_state) == new_state
      assert ClaimWaiters.total_count() == 2
    after
      ClaimWaiters.cleanup(self())
    end
  end

  test "UNSUBSCRIBE_EVENTS removes Flow wake waiter registrations" do
    try do
      ClaimWaiters.cleanup(self())
      type = "native-wake-unsub-#{System.unique_integer([:positive])}"

      {:ok, _reply, new_state} =
        Commands.execute(
          @op_subscribe_events,
          %{
            "events" => ["FLOW_WAKE"],
            "flow_wake" => %{"type" => type, "state" => "queued", "limit" => 10}
          },
          state()
        )

      assert ClaimWaiters.total_count() > 0

      {status, reply, unsubscribed_state} =
        Commands.execute(
          @op_unsubscribe_events,
          %{"events" => ["FLOW_WAKE"]},
          new_state
        )

      assert status == :ok
      assert reply.subscribed == []
      assert unsubscribed_state.flow_wake_subscription == nil
      assert ClaimWaiters.total_count() == 0
    after
      ClaimWaiters.cleanup(self())
    end
  end

  test "native admin bridge dispatches cluster keyslot" do
    {status, slot, _state} =
      Commands.execute(
        @op_cluster_keyslot,
        %{"key" => "user:1", "args" => ["user:1"]},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
    assert is_integer(slot)
  end

  test "SUBSCRIBE_EVENTS rejects unknown event names" do
    {status, reason, _state} =
      Commands.execute(@op_subscribe_events, %{"events" => ["unknown"]}, state())

    assert status == :bad_request
    assert reason =~ "known event names"
  end

  test "SUBSCRIBE_EVENTS stores normalized event subscriptions" do
    {status, payload, new_state} =
      Commands.execute(@op_subscribe_events, %{"events" => ["auth_invalidated"]}, state())

    assert status == :ok
    assert payload.subscribed == ["AUTH_INVALIDATED"]
    assert MapSet.member?(new_state.event_subscriptions, "AUTH_INVALIDATED")
  end

  test "UNSUBSCRIBE_EVENTS removes normalized event subscriptions" do
    state = state(event_subscriptions: MapSet.new(["AUTH_INVALIDATED", "FLOW_WAKE"]))

    {status, payload, new_state} =
      Commands.execute(@op_unsubscribe_events, %{"events" => ["auth_invalidated"]}, state)

    assert status == :ok
    assert payload.subscribed == ["FLOW_WAKE"]
    refute MapSet.member?(new_state.event_subscriptions, "AUTH_INVALIDATED")
    assert MapSet.member?(new_state.event_subscriptions, "FLOW_WAKE")
  end

  test "STARTUP sets client name and subscribes requested events" do
    {status, payload, new_state} =
      Commands.execute(
        @op_startup,
        %{"client_name" => "native-sdk", "events" => ["flow_wake"]},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
    assert payload.protocol == "ferricstore-native"
    assert new_state.client_name == "native-sdk"
    assert MapSet.member?(new_state.event_subscriptions, "FLOW_WAKE")
  end

  test "STARTUP rejects event subscriptions before authentication when auth is required" do
    {status, reason, new_state} =
      Commands.execute(
        @op_startup,
        %{"client_name" => "native-sdk", "events" => ["flow_wake"]},
        state(
          authenticated: false,
          require_auth: true,
          instance_ctx: FerricStore.Instance.get(:default)
        )
      )

    assert status == :auth
    assert reason =~ "NOAUTH"
    assert new_state.event_subscriptions == MapSet.new()
  end

  test "STARTUP enables compact Flow responses when requested" do
    {status, payload, new_state} =
      Commands.execute(
        @op_startup,
        %{"compact_flow_responses" => true},
        state(instance_ctx: FerricStore.Instance.get(:default))
      )

    assert status == :ok
    assert payload.capabilities.response_codecs.compact_flow_responses == true
    assert new_state.compact_flow_responses == true
  end

  test "STARTUP rejects unsupported compression negotiation" do
    {status, reason, new_state} =
      Commands.execute(
        @op_startup,
        %{"compression" => "brotli"},
        state(instance_ctx: FerricStore.Instance.get(:default), compression: :none)
      )

    assert status == :bad_request
    assert reason =~ "unsupported compression"
    assert new_state.compression == :none
  end

  test "PIPELINE rejects control commands" do
    {status, reason, _state} =
      Commands.execute(
        @op_pipeline,
        %{"commands" => [%{"opcode" => @op_ping, "body" => %{}}]},
        state()
      )

    assert status == :bad_request
    assert reason =~ "control commands"
  end

  test "PIPELINE enforces configured max command count before execution" do
    Application.put_env(:ferricstore, :native_max_pipeline_commands, 1)

    {status, reason, _state} =
      Commands.execute(
        @op_pipeline,
        %{
          "commands" => [
            %{"opcode" => @op_get, "body" => %{"key" => "a"}},
            %{"opcode" => @op_get, "body" => %{"key" => "b"}}
          ]
        },
        state()
      )

    assert status == :bad_request
    assert reason =~ "max commands"
  end

  test "AUTH does not bypass requirepass through passwordless default ACL user" do
    Config.set("requirepass", "secret")

    {status, reason, new_state} =
      Commands.execute(
        @op_auth,
        %{"username" => "default", "password" => "wrong"},
        state(require_auth: true)
      )

    assert status == :auth
    assert reason =~ "WRONGPASS"
    assert new_state.authenticated == false
  end

  test "FLOW many compact tuple items are still checked by key ACL" do
    ctx = FerricStore.Instance.get(:default)
    :ok = FerricstoreServer.Acl.set_user("limited-flow", ["on", ">pass", "~allowed:*", "+@all"])
    cache = ConnAuth.build_acl_cache("limited-flow")

    {status, reason, _state} =
      Commands.execute(
        @op_flow_create_many,
        %{
          "type" => "acl-flow",
          "state" => "queued",
          "now_ms" => 1,
          "run_at_ms" => 1,
          "items" => [{:id, "denied:1", :payload, "payload"}],
          __wire_flow_items_normalized__: true
        },
        state(
          username: "limited-flow",
          authenticated: true,
          acl_cache: cache,
          instance_ctx: ctx
        )
      )

    assert status == :noperm
    assert reason =~ "NOPERM"
  end

  defp state(overrides \\ []) do
    Map.merge(
      %{
        client_id: System.unique_integer([:positive]),
        client_name: nil,
        username: "default",
        authenticated: false,
        require_auth: false,
        peer: nil,
        created_at: 0,
        instance_ctx: nil,
        stats_counter: nil,
        acl_cache: ConnAuth.build_acl_cache("default"),
        max_frame_bytes: 16 * 1024 * 1024,
        max_lanes: 1024,
        lane_max_queue: 1024,
        max_inflight_per_connection: 4096,
        max_inflight_per_lane: 1024,
        compression: :none,
        event_subscriptions: MapSet.new(),
        flow_wake_subscription: nil,
        compact_flow_responses: false,
        close_after_reply: false
      },
      Map.new(overrides)
    )
  end

  defp test_stats_counter, do: :counters.new(10, [])

  defp different_shard_ids(ctx, prefix) do
    id1 = "#{prefix}-#{System.unique_integer([:positive])}-a"
    shard1 = Router.shard_for(ctx, id1)

    id2 =
      1..10_000
      |> Stream.map(&"#{prefix}-#{System.unique_integer([:positive])}-#{&1}")
      |> Enum.find(&(Router.shard_for(ctx, &1) != shard1))

    {id1, id2}
  end

  defp routed_compound_marker_mismatch_key(ctx, prefix) do
    unique = System.unique_integer([:positive])

    Enum.find_value(1..10_000, fn i ->
      key = "#{prefix}-#{unique}-#{i}"
      type_key = CompoundKey.type_key(key)

      if Router.shard_for(ctx, key) != Router.shard_for(ctx, type_key), do: key
    end) || flunk("expected to find a key whose compound marker routes differently")
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
