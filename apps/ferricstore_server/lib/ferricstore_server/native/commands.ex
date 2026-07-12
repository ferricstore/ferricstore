defmodule FerricstoreServer.Native.Commands do
  @moduledoc """
  Native protocol command dispatcher.

  This module intentionally stays as a thin protocol adapter. It performs:

    * native body validation and option conversion
    * ACL command/key checks
    * route/client/backpressure metadata responses
    * delegation to FerricStore embedded APIs for actual behavior

  It must not duplicate storage, Flow, WAL, or index semantics.
  """

  alias Ferricstore.{AuditLog, Stats}
  alias Ferricstore.Commands.PreparedCommand
  alias Ferricstore.Flow.{ClaimDueAPI, ClaimWaiters}
  alias Ferricstore.Flow.Codec, as: FlowCodec
  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.Flow.Keys, as: FlowKeys
  alias Ferricstore.Flow.RecordProjection, as: FlowRecordProjection
  alias Ferricstore.Flow.Telemetry, as: FlowTelemetry
  alias Ferricstore.Store.{CompoundKey, Ops, Router}
  alias Ferricstore.Store.Shard.ZSetIndex
  alias Ferricstore.Store.SlotMap
  alias FerricstoreServer.AuthRateLimiter
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Native.RouteMetadata

  @list_position_step 1_000_000_000
  @default_max_collection_response_items 10_000
  @wrongtype_error {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  @op_hello 0x0001
  @op_auth 0x0002
  @op_ping 0x0003
  @op_client_set_name 0x0004
  @op_client_info 0x0005
  @op_route 0x0006
  @op_shards 0x0007
  @op_backpressure 0x0008
  @op_quit 0x0009
  @op_goaway 0x000A
  @op_options 0x000B
  @op_startup 0x000C
  @op_window_update 0x000D
  @op_pipeline 0x000E
  @op_route_batch 0x000F
  @op_event 0x0010
  @op_subscribe_events 0x0011
  @op_unsubscribe_events 0x0012
  @op_command_exec 0x0100

  @op_get 0x0101
  @op_set 0x0102
  @op_del 0x0103
  @op_mget 0x0104
  @op_mset 0x0105
  @op_cas 0x0106
  @op_lock 0x0107
  @op_unlock 0x0108
  @op_extend 0x0109
  @op_ratelimit_add 0x010A
  @op_fetch_or_compute 0x010B
  @op_fetch_or_compute_result 0x010C
  @op_fetch_or_compute_error 0x010D
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

  @op_cluster_health 0x0301
  @op_cluster_stats 0x0302
  @op_cluster_keyslot 0x0303
  @op_cluster_slots 0x0304
  @op_cluster_status 0x0305
  @op_cluster_join 0x0306
  @op_cluster_leave 0x0307
  @op_cluster_failover 0x0308
  @op_cluster_promote 0x0309
  @op_cluster_demote 0x030A
  @op_cluster_role 0x030B
  @op_ferricstore_key_info 0x030C
  @op_ferricstore_config 0x030D
  @op_ferricstore_hotness 0x030E
  @op_ferricstore_metrics 0x030F
  @op_ferricstore_blobgc 0x0310

  @op_flow_create 0x0201
  @op_flow_get 0x0202
  @op_flow_claim_due 0x0203
  @op_flow_complete 0x0204
  @op_flow_transition 0x0205
  @op_flow_retry 0x0206
  @op_flow_fail 0x0207
  @op_flow_cancel 0x0208
  @op_flow_extend_lease 0x0209
  @op_flow_history 0x020A
  @op_flow_value_put 0x020B
  @op_flow_value_mget 0x020C
  @op_flow_signal 0x020D
  @op_flow_list 0x020E
  @op_flow_create_many 0x020F
  @op_flow_complete_many 0x0210
  @op_flow_transition_many 0x0211
  @op_flow_retry_many 0x0212
  @op_flow_fail_many 0x0213
  @op_flow_cancel_many 0x0214
  @op_flow_reclaim 0x0215
  @op_flow_rewind 0x0216
  @op_flow_terminals 0x0217
  @op_flow_failures 0x0218
  @op_flow_by_parent 0x0219
  @op_flow_by_root 0x021A
  @op_flow_by_correlation 0x021B
  @op_flow_info 0x021C
  @op_flow_stuck 0x021D
  @op_flow_policy_set 0x021E
  @op_flow_policy_get 0x021F
  @op_flow_spawn_children 0x0220
  @op_flow_retention_cleanup 0x0221
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
  @op_flow_stats 0x022D
  @op_flow_attributes 0x022E
  @op_flow_attribute_values 0x022F
  @op_flow_search 0x0230
  @op_flow_effect_reserve 0x0240
  @op_flow_effect_confirm 0x0241
  @op_flow_effect_fail 0x0242
  @op_flow_effect_compensate 0x0243
  @op_flow_effect_get 0x0244
  @op_flow_governance_ledger 0x0245
  @op_flow_approval_request 0x0246
  @op_flow_approval_approve 0x0247
  @op_flow_approval_reject 0x0248
  @op_flow_approval_get 0x0249
  @op_flow_circuit_open 0x024A
  @op_flow_circuit_close 0x024B
  @op_flow_circuit_get 0x024C
  @op_flow_budget_reserve 0x024D
  @op_flow_budget_get 0x024E
  @op_flow_limit_lease 0x024F
  @op_flow_limit_spend 0x0250
  @op_flow_limit_release 0x0251
  @op_flow_limit_get 0x0252
  @op_flow_approval_list 0x0253
  @op_flow_governance_overview 0x0254
  @op_flow_budget_list 0x0255
  @op_flow_limit_list 0x0256
  @op_flow_budget_commit 0x0257
  @op_flow_budget_release 0x0258

  @control_commands %{
    @op_hello => "HELLO",
    @op_auth => "AUTH",
    @op_ping => "PING",
    @op_client_set_name => "CLIENT.SETNAME",
    @op_client_info => "CLIENT.INFO",
    @op_route => "ROUTE",
    @op_shards => "SHARDS",
    @op_backpressure => "BACKPRESSURE",
    @op_quit => "QUIT",
    @op_goaway => "GOAWAY",
    @op_options => "OPTIONS",
    @op_startup => "STARTUP",
    @op_window_update => "WINDOW_UPDATE",
    @op_route_batch => "ROUTE_BATCH",
    @op_event => "EVENT",
    @op_subscribe_events => "SUBSCRIBE_EVENTS",
    @op_unsubscribe_events => "UNSUBSCRIBE_EVENTS"
  }

  @kv_commands %{
    @op_command_exec => "COMMAND_EXEC",
    @op_pipeline => "PIPELINE",
    @op_get => "GET",
    @op_set => "SET",
    @op_del => "DEL",
    @op_mget => "MGET",
    @op_mset => "MSET",
    @op_cas => "CAS",
    @op_lock => "LOCK",
    @op_unlock => "UNLOCK",
    @op_extend => "EXTEND",
    @op_ratelimit_add => "RATELIMIT.ADD",
    @op_fetch_or_compute => "FETCH_OR_COMPUTE",
    @op_fetch_or_compute_result => "FETCH_OR_COMPUTE_RESULT",
    @op_fetch_or_compute_error => "FETCH_OR_COMPUTE_ERROR",
    @op_hset => "HSET",
    @op_hget => "HGET",
    @op_hmget => "HMGET",
    @op_hgetall => "HGETALL",
    @op_lpush => "LPUSH",
    @op_rpush => "RPUSH",
    @op_lpop => "LPOP",
    @op_rpop => "RPOP",
    @op_lrange => "LRANGE",
    @op_sadd => "SADD",
    @op_srem => "SREM",
    @op_smembers => "SMEMBERS",
    @op_sismember => "SISMEMBER",
    @op_zadd => "ZADD",
    @op_zrem => "ZREM",
    @op_zrange => "ZRANGE",
    @op_zscore => "ZSCORE"
  }

  @admin_commands %{
    @op_cluster_health => "CLUSTER.HEALTH",
    @op_cluster_stats => "CLUSTER.STATS",
    @op_cluster_keyslot => "CLUSTER.KEYSLOT",
    @op_cluster_slots => "CLUSTER.SLOTS",
    @op_cluster_status => "CLUSTER.STATUS",
    @op_cluster_join => "CLUSTER.JOIN",
    @op_cluster_leave => "CLUSTER.LEAVE",
    @op_cluster_failover => "CLUSTER.FAILOVER",
    @op_cluster_promote => "CLUSTER.PROMOTE",
    @op_cluster_demote => "CLUSTER.DEMOTE",
    @op_cluster_role => "CLUSTER.ROLE",
    @op_ferricstore_key_info => "FERRICSTORE.KEY_INFO",
    @op_ferricstore_config => "FERRICSTORE.CONFIG",
    @op_ferricstore_hotness => "FERRICSTORE.HOTNESS",
    @op_ferricstore_metrics => "FERRICSTORE.METRICS",
    @op_ferricstore_blobgc => "FERRICSTORE.BLOBGC"
  }

  @flow_commands %{
    @op_flow_create => "FLOW.CREATE",
    @op_flow_get => "FLOW.GET",
    @op_flow_claim_due => "FLOW.CLAIM_DUE",
    @op_flow_complete => "FLOW.COMPLETE",
    @op_flow_transition => "FLOW.TRANSITION",
    @op_flow_retry => "FLOW.RETRY",
    @op_flow_fail => "FLOW.FAIL",
    @op_flow_cancel => "FLOW.CANCEL",
    @op_flow_extend_lease => "FLOW.EXTEND_LEASE",
    @op_flow_history => "FLOW.HISTORY",
    @op_flow_value_put => "FLOW.VALUE.PUT",
    @op_flow_value_mget => "FLOW.VALUE.MGET",
    @op_flow_signal => "FLOW.SIGNAL",
    @op_flow_list => "FLOW.LIST",
    @op_flow_create_many => "FLOW.CREATE_MANY",
    @op_flow_complete_many => "FLOW.COMPLETE_MANY",
    @op_flow_transition_many => "FLOW.TRANSITION_MANY",
    @op_flow_retry_many => "FLOW.RETRY_MANY",
    @op_flow_fail_many => "FLOW.FAIL_MANY",
    @op_flow_cancel_many => "FLOW.CANCEL_MANY",
    @op_flow_reclaim => "FLOW.RECLAIM",
    @op_flow_rewind => "FLOW.REWIND",
    @op_flow_terminals => "FLOW.TERMINALS",
    @op_flow_failures => "FLOW.FAILURES",
    @op_flow_by_parent => "FLOW.BY_PARENT",
    @op_flow_by_root => "FLOW.BY_ROOT",
    @op_flow_by_correlation => "FLOW.BY_CORRELATION",
    @op_flow_info => "FLOW.INFO",
    @op_flow_stuck => "FLOW.STUCK",
    @op_flow_policy_set => "FLOW.POLICY.SET",
    @op_flow_policy_get => "FLOW.POLICY.GET",
    @op_flow_spawn_children => "FLOW.SPAWN_CHILDREN",
    @op_flow_retention_cleanup => "FLOW.RETENTION_CLEANUP",
    @op_flow_step_continue => "FLOW.STEP_CONTINUE",
    @op_flow_start_and_claim => "FLOW.START_AND_CLAIM",
    @op_flow_run_steps_many => "FLOW.RUN_STEPS_MANY",
    @op_flow_schedule_create => "FLOW.SCHEDULE.CREATE",
    @op_flow_schedule_get => "FLOW.SCHEDULE.GET",
    @op_flow_schedule_delete => "FLOW.SCHEDULE.DELETE",
    @op_flow_schedule_fire_due => "FLOW.SCHEDULE.FIRE_DUE",
    @op_flow_schedule_list => "FLOW.SCHEDULE.LIST",
    @op_flow_schedule_fire => "FLOW.SCHEDULE.FIRE",
    @op_flow_schedule_pause => "FLOW.SCHEDULE.PAUSE",
    @op_flow_schedule_resume => "FLOW.SCHEDULE.RESUME",
    @op_flow_stats => "FLOW.STATS",
    @op_flow_attributes => "FLOW.ATTRIBUTES",
    @op_flow_attribute_values => "FLOW.ATTRIBUTE_VALUES",
    @op_flow_search => "FLOW.SEARCH",
    @op_flow_effect_reserve => "FLOW.EFFECT.RESERVE",
    @op_flow_effect_confirm => "FLOW.EFFECT.CONFIRM",
    @op_flow_effect_fail => "FLOW.EFFECT.FAIL",
    @op_flow_effect_compensate => "FLOW.EFFECT.COMPENSATE",
    @op_flow_effect_get => "FLOW.EFFECT.GET",
    @op_flow_governance_ledger => "FLOW.GOVERNANCE.LEDGER",
    @op_flow_approval_request => "FLOW.APPROVAL.REQUEST",
    @op_flow_approval_approve => "FLOW.APPROVAL.APPROVE",
    @op_flow_approval_reject => "FLOW.APPROVAL.REJECT",
    @op_flow_approval_get => "FLOW.APPROVAL.GET",
    @op_flow_circuit_open => "FLOW.CIRCUIT.OPEN",
    @op_flow_circuit_close => "FLOW.CIRCUIT.CLOSE",
    @op_flow_circuit_get => "FLOW.CIRCUIT.GET",
    @op_flow_budget_reserve => "FLOW.BUDGET.RESERVE",
    @op_flow_budget_get => "FLOW.BUDGET.GET",
    @op_flow_limit_lease => "FLOW.LIMIT.LEASE",
    @op_flow_limit_spend => "FLOW.LIMIT.SPEND",
    @op_flow_limit_release => "FLOW.LIMIT.RELEASE",
    @op_flow_limit_get => "FLOW.LIMIT.GET",
    @op_flow_approval_list => "FLOW.APPROVAL.LIST",
    @op_flow_governance_overview => "FLOW.GOVERNANCE.OVERVIEW",
    @op_flow_budget_list => "FLOW.BUDGET.LIST",
    @op_flow_limit_list => "FLOW.LIMIT.LIST",
    @op_flow_budget_commit => "FLOW.BUDGET.COMMIT",
    @op_flow_budget_release => "FLOW.BUDGET.RELEASE"
  }

  @commands @control_commands
            |> Map.merge(@kv_commands)
            |> Map.merge(@admin_commands)
            |> Map.merge(@flow_commands)

  @supported_compressions ["none"]
  @supported_auth ["password", "acl-password"]
  @supported_atomicity ["none", "per_shard", "same_shard"]
  @supported_events [
    "AUTH_INVALIDATED",
    "BACKPRESSURE_CHANGED",
    "FLOW_WAKE",
    "GOAWAY",
    "TOPOLOGY_CHANGED"
  ]

  @known_flow_options %{
    "after_ms" => :after_ms,
    "actual_amount" => :actual_amount,
    "amount" => :amount,
    "assignees" => :assignees,
    "at_ms" => :at_ms,
    "attempt" => :attempt,
    "attribute" => :attribute,
    "attributes" => :attributes,
    "attributes_delete" => :attributes_delete,
    "attributes_merge" => :attributes_merge,
    "backoff" => :backoff,
    "base_ms" => :base_ms,
    "block_ms" => :block_ms,
    "children" => :children,
    "consistent_projection" => :consistent_projection,
    "correlation_id" => :correlation_id,
    "config_version" => :config_version,
    "count" => :count,
    "cron" => :cron,
    "delay_ms" => :delay_ms,
    "drop_values" => :drop_values,
    "due_after_ms" => :due_after_ms,
    "end_at_ms" => :end_at_ms,
    "event" => :event,
    "every_ms" => :every_ms,
    "error" => :error,
    "error_ref" => :error_ref,
    "exhausted_to" => :exhausted_to,
    "exhaust_to" => :exhaust_to,
    "expect_state" => :expect_state,
    "failure" => :failure,
    "fire_at_ms" => :fire_at_ms,
    "fencing_token" => :fencing_token,
    "from_state" => :from_state,
    "full" => :full,
    "from_event" => :from_event,
    "from_ms" => :from_ms,
    "from_version" => :from_version,
    "group_id" => :group_id,
    "half_open_max_probes" => :half_open_max_probes,
    "half_open_success_threshold" => :half_open_success_threshold,
    "history_hot_max_events" => :history_hot_max_events,
    "history_max_events" => :history_max_events,
    "id" => :id,
    "id_prefix" => :id_prefix,
    "idempotent" => :idempotent,
    "idempotency_key" => :idempotency_key,
    "if_state" => :if_state,
    "include_cold" => :include_cold,
    "indexed_attributes" => :indexed_attributes,
    "indexed_state_meta" => :indexed_state_meta,
    "independent" => :independent,
    "initial_state" => :initial_state,
    "items" => :items,
    "effect_key" => :effect_key,
    "effect_type" => :effect_type,
    "error_class" => :error_class,
    "error_classes" => :error_classes,
    "external_id" => :external_id,
    "expires_at_ms" => :expires_at_ms,
    "failure_threshold" => :failure_threshold,
    "failure_rate_pct" => :failure_rate_pct,
    "flow_id" => :flow_id,
    "governance_limit_scope" => :governance_limit_scope,
    "governance_shard_id" => :governance_shard_id,
    "governance_scope" => :governance_scope,
    "jitter_pct" => :jitter_pct,
    "kind" => :kind,
    "latency_ms" => :latency_ms,
    "latency_threshold_ms" => :latency_threshold_ms,
    "lease_ms" => :lease_ms,
    "lease_token" => :lease_token,
    "limit" => :limit,
    "local_cache" => :local_cache,
    "max_active_ms" => :max_active_ms,
    "max_attempts" => :max_attempts,
    "max_bytes" => :max_bytes,
    "max_fires" => :max_fires,
    "max_ms" => :max_ms,
    "max_retries" => :max_retries,
    "min_calls" => :min_calls,
    "name" => :name,
    "now_ms" => :now_ms,
    "older_than_ms" => :older_than_ms,
    "on_child_failed" => :on_child_failed,
    "on_parent_closed" => :on_parent_closed,
    "open_ms" => :open_ms,
    "override" => :override,
    "override_values" => :override_values,
    "overlap_policy" => :overlap_policy,
    "overlap_retry_ms" => :overlap_retry_ms,
    "operation_digest" => :operation_digest,
    "approver" => :approver,
    "overwrite" => :overwrite,
    "owner_flow_id" => :owner_flow_id,
    "parent_id" => :parent_id,
    "partition_key" => :partition_key,
    "partition_keys" => :partition_keys,
    "payload" => :payload,
    "payload_max_bytes" => :payload_max_bytes,
    "payload_ref" => :payload_ref,
    "payload_refs" => :payload_refs,
    "priority" => :priority,
    "policy_hash" => :policy_hash,
    "policy_version" => :policy_version,
    "reason" => :reason,
    "reason_ref" => :reason_ref,
    "reservation_id" => :reservation_id,
    "reservation_ids" => :reservation_ids,
    "requested_by" => :requested_by,
    "reclaim_expired" => :reclaim_expired,
    "reclaim_ratio" => :reclaim_ratio,
    "result" => :result,
    "result_ref" => :result_ref,
    "return" => :return,
    "retry" => :retry,
    "retention_ttl_ms" => :retention_ttl_ms,
    "rev" => :rev,
    "retry_at_ms" => :retry_at_ms,
    "root_id" => :root_id,
    "run_at_ms" => :run_at_ms,
    "scope" => :scope,
    "signal" => :signal,
    "shard_id" => :shard_id,
    "state" => :state,
    "state_meta" => :state_meta,
    "status" => :status,
    "states" => :states,
    "start_at_ms" => :start_at_ms,
    "steps" => :steps,
    "success" => :success,
    "terminal_local_only" => :terminal_local_only,
    "terminal_only" => :terminal_only,
    "to_event" => :to_event,
    "to_state" => :to_state,
    "to_ms" => :to_ms,
    "to_version" => :to_version,
    "target" => :target,
    "target_type" => :target_type,
    "timezone" => :timezone,
    "transition_to" => :transition_to,
    "ttl_ms" => :ttl_ms,
    "timeout_ms" => :timeout_ms,
    "type" => :type,
    "usage" => :usage,
    "value" => :value,
    "value_max_bytes" => :value_max_bytes,
    "value_refs" => :value_refs,
    "values" => :values,
    "wait" => :wait,
    "wait_state" => :wait_state,
    "window_ms" => :window_ms,
    "worker" => :worker
  }

  @atom_values %{
    "all" => :all,
    "allow" => :allow,
    "cron" => :cron,
    "delay" => :delay,
    "exponential" => :exponential,
    "interval" => :interval,
    "linear" => :linear,
    "meta" => :meta,
    "none" => :none,
    "ok_on_success" => :ok_on_success,
    "one_shot" => :one_shot,
    "fail_schedule" => :fail_schedule,
    "queue_after_previous" => :queue_after_previous,
    "running" => :running,
    "skip" => :skip
  }

  @spec command_name(non_neg_integer()) :: binary() | nil
  def command_name(opcode), do: Map.get(@commands, opcode)

  @spec control_opcode?(non_neg_integer()) :: boolean()
  def control_opcode?(opcode), do: Map.has_key?(@control_commands, opcode)

  @spec event_opcode() :: non_neg_integer()
  def event_opcode, do: @op_event

  @spec execute(non_neg_integer(), term(), map()) :: {atom(), term(), map()}
  def execute(opcode, payload, %{acl_cache: :full_access, require_auth: false} = state) do
    payload = normalize_payload(payload)

    with :ok <- check_deadline(payload),
         :ok <- authorize_public_opcode(opcode, payload),
         :ok <- check_native_resource_limits(opcode, payload, state) do
      do_execute(opcode, payload, state) |> record_native_activity(opcode, payload)
    else
      {:error, reason} -> {:error, FerricStore.ResourceLimits.error_message(reason), state}
      {:error, status, reason} -> {status, reason, state}
    end
  rescue
    error ->
      rescued_execute_error(opcode, payload, state, error)
  end

  def execute(opcode, payload, state) do
    payload = normalize_payload(payload)

    with {:ok, command} <- fetch_command(opcode),
         :ok <- check_deadline(payload),
         :ok <- authorize(command, opcode, payload, state),
         :ok <- authorize_public_opcode(opcode, payload),
         :ok <- check_native_resource_limits(opcode, payload, state) do
      do_execute(opcode, payload, state) |> record_native_activity(opcode, payload)
    else
      {:error, reason} -> {:error, FerricStore.ResourceLimits.error_message(reason), state}
      {:error, status, reason} -> {status, reason, state}
    end
  rescue
    error ->
      rescued_execute_error(opcode, payload, state, error)
  end

  defp rescued_execute_error(opcode, payload, state, error) do
    if metrics_command_payload?(opcode, payload) do
      {:ok, "", state}
    else
      {:error, Exception.message(error), state}
    end
  end

  defp record_native_activity({:ok, _payload, %{instance_ctx: store}} = result, opcode, payload) do
    keys = keys(opcode, payload)

    if data_plane_activity_opcode?(opcode) and keys != [] do
      FerricStore.ResourceLimits.record_activity(keys, store: store)
    end

    result
  rescue
    _error -> result
  catch
    _kind, _reason -> result
  end

  defp record_native_activity(result, _opcode, _payload), do: result

  defp check_native_resource_limits(opcode, payload, %{instance_ctx: store}) do
    if data_plane_activity_opcode?(opcode) do
      command = Map.get(@commands, opcode)

      if is_binary(command) do
        FerricStore.ResourceLimits.check_command(
          command,
          native_resource_limit_args(opcode, payload),
          resource_limit_keys(opcode, payload),
          store: store,
          flow_create_count: native_flow_create_count(opcode, payload)
        )
      else
        :ok
      end
    else
      :ok
    end
  rescue
    _error -> {:error, :resource_limit_check_failed}
  catch
    _kind, _reason -> {:error, :resource_limit_check_failed}
  end

  defp check_native_resource_limits(_opcode, _payload, _state), do: :ok

  defp native_resource_limit_args(@op_set, payload) do
    [Map.get(payload, "key"), Map.get(payload, "value")]
  end

  defp native_resource_limit_args(@op_mset, payload) do
    payload
    |> Map.get("pairs", [])
    |> Enum.flat_map(fn
      %{"key" => key, "value" => value} -> [key, value]
      %{key: key, value: value} -> [key, value]
      {key, value} -> [key, value]
      _other -> []
    end)
  end

  defp native_resource_limit_args(_opcode, payload) when is_map(payload) do
    payload
    |> Map.drop(["request_context"])
    |> Enum.flat_map(fn {_key, value} -> List.wrap(value) end)
  end

  defp native_resource_limit_args(_opcode, _payload), do: []

  defp resource_limit_keys(opcode, payload)
       when opcode in [@op_flow_create, @op_flow_start_and_claim] do
    binary_list([Map.get(payload, "partition_key") || Map.get(payload, "id")])
  end

  defp resource_limit_keys(@op_flow_create_many, payload) do
    item_keys =
      payload
      |> Map.get("items", [])
      |> Enum.map(fn
        %{"partition_key" => partition_key} -> partition_key
        %{partition_key: partition_key} -> partition_key
        _item -> nil
      end)

    [Map.get(payload, "partition_key") | item_keys]
    |> binary_list()
  end

  defp resource_limit_keys(opcode, payload), do: keys(opcode, payload)

  defp native_flow_create_count(@op_flow_create, _payload), do: 1
  defp native_flow_create_count(@op_flow_start_and_claim, _payload), do: 1

  defp native_flow_create_count(@op_flow_create_many, payload) do
    case Map.get(payload, "items") do
      items when is_list(items) -> length(items)
      _other -> 1
    end
  end

  defp native_flow_create_count(_opcode, _payload), do: 0

  defp data_plane_activity_opcode?(opcode)
       when opcode in [@op_command_exec, @op_pipeline],
       do: false

  defp data_plane_activity_opcode?(opcode),
    do: Map.has_key?(@kv_commands, opcode) or Map.has_key?(@flow_commands, opcode)

  defp metrics_command_payload?(@op_ferricstore_metrics, _payload), do: true

  defp metrics_command_payload?(@op_command_exec, payload) when is_map(payload) do
    payload
    |> Map.get("command", Map.get(payload, :command))
    |> metrics_command_name?()
  end

  defp metrics_command_payload?(_opcode, _payload), do: false

  defp metrics_command_name?(command) when is_binary(command),
    do: String.upcase(command) == "FERRICSTORE.METRICS"

  defp metrics_command_name?(_command), do: false

  @spec summary(map()) :: map()
  def summary(state) do
    %{
      client_id: state.client_id,
      client_name: state.client_name,
      username: state.username,
      authenticated: state.authenticated,
      peer: format_peer(state.peer),
      created_at_ms: state.created_at,
      flags: "N",
      protocol: "native"
    }
  end

  @spec default_requires_auth?() :: boolean()
  def default_requires_auth? do
    FerricstoreServer.Acl.has_configured_users?()
  end

  defp do_execute(@op_hello, payload, state) do
    with {:ok, state} <- negotiate_compression(state, Map.get(payload, "compression", "none")) do
      state = maybe_set_client_name(state, Map.get(payload, "client_name"))
      {:ok, hello_payload(state), state}
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_options, _payload, state),
    do: {:ok, capabilities_payload(state), state}

  defp do_execute(@op_startup, payload, state) do
    with {:ok, state} <- negotiate_compression(state, Map.get(payload, "compression", "none")) do
      state =
        state
        |> maybe_set_client_name(Map.get(payload, "client_name"))
        |> maybe_set_client_name(Map.get(payload, "driver_name"))
        |> maybe_set_compact_flow_responses(Map.get(payload, "compact_flow_responses"))

      case maybe_startup_subscribe_events(state, Map.get(payload, "events", []), payload) do
        {:ok, state} -> {:ok, hello_payload(state), state}
        {:error, status, reason} -> {status, reason, state}
      end
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_auth, payload, state) do
    username = Map.get(payload, "username", "default")
    password = Map.get(payload, "password")

    if is_binary(username) and is_binary(password) do
      case AuthRateLimiter.permit(state.peer, username, password) do
        {:ok, reservation} ->
          result = do_auth(username, password, state)

          if match?({:ok, _response, _state}, result) do
            :ok = AuthRateLimiter.release_success(reservation)
          end

          result

        {:error, {:rate_limited, retry_after_ms}} ->
          auth_failure(username, state)

          {:auth, "ERR too many authentication attempts; retry after #{retry_after_ms} ms", state}

        {:error, reason} when is_binary(reason) ->
          {:auth, reason, state}
      end
    else
      {:auth, "ERR native AUTH requires username/password binaries", state}
    end
  end

  defp do_execute(@op_ping, payload, state),
    do: {:ok, Map.get(payload, "message", "PONG"), state}

  defp do_execute(@op_client_set_name, payload, state) do
    case require_binary(payload, "name") do
      {:ok, name} -> {:ok, "OK", maybe_set_client_name(state, name)}
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_client_info, _payload, state),
    do: {:ok, summary(state), state}

  defp do_execute(@op_route, payload, state) do
    case require_binary(payload, "key") do
      {:ok, key} -> {:ok, route_payload(state.instance_ctx, key), state}
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_shards, _payload, state),
    do: {:ok, shards_payload(state.instance_ctx), state}

  defp do_execute(@op_route_batch, payload, state) do
    with {:ok, keys} <- require_binary_list(payload, "keys") do
      routes = Enum.map(keys, &route_payload(state.instance_ctx, &1))
      {:ok, routes, state}
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_backpressure, _payload, state),
    do: {:ok, backpressure_payload(), state}

  defp do_execute(@op_event, _payload, state),
    do: {:bad_request, "ERR native EVENT is server-initiated", state}

  defp do_execute(@op_subscribe_events, payload, state) do
    with {:ok, events} <- event_list(payload),
         {:ok, state} <- maybe_subscribe_flow_wake(state, events, Map.get(payload, "flow_wake")) do
      state = subscribe_events(state, events)
      {:ok, event_subscription_payload(state), state}
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_unsubscribe_events, payload, state) do
    with {:ok, events} <- event_list(payload) do
      state =
        state
        |> maybe_unsubscribe_flow_wake(events)
        |> unsubscribe_events(events)

      {:ok, event_subscription_payload(state), state}
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_command_exec, payload, state) do
    with {:ok, command} <- require_binary(payload, "command"),
         {:ok, args} <- raw_command_args(payload),
         {:ok, request_context} <- request_context(payload, state) do
      with {:ok, prepared} <- prepared_raw_command(payload, command, args),
           :ok <- authorize_raw_command(prepared, state) do
        dispatch_command_exec(prepared, state, request_context)
      else
        {:error, reason} when is_binary(reason) -> {:bad_request, reason, state}
        {:error, status, reason} -> {status, reason, state}
      end
    else
      {:error, reason} when is_binary(reason) -> {:bad_request, reason, state}
      {:error, status, reason} -> {status, reason, state}
    end
  end

  defp do_execute(@op_window_update, payload, state) do
    state =
      state
      |> maybe_set_native_limit(
        :max_inflight_per_connection,
        Map.get(payload, "max_inflight_per_connection")
      )
      |> maybe_set_native_limit(:max_inflight_per_lane, Map.get(payload, "max_inflight_per_lane"))

    {:ok,
     %{
       accepted: true,
       lane_id: Map.get(payload, "lane_id"),
       credits: Map.get(payload, "credits"),
       limits: flow_control_payload(state)
     }, state}
  end

  defp do_execute(@op_pipeline, payload, state) do
    case Map.get(payload, "compact_pipeline") do
      {mode, items} ->
        execute_compact_pipeline(mode, items, payload, state)

      _not_compact ->
        execute_typed_pipeline(payload, state)
    end
  end

  defp do_execute(@op_quit, _payload, state),
    do: {:ok, "OK", %{state | close_after_reply: true}}

  defp do_execute(@op_get, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, value} <- FerricStore.Impl.get(state.instance_ctx, key) do
      {:ok, value, state}
    else
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp do_execute(@op_set, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, value} <- require_any(payload, "value"),
         {:ok, opts} <- kv_set_opts(payload) do
      result = FerricStore.Impl.set(state.instance_ctx, key, value, opts)
      result_to_reply(result, state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_del, payload, state) do
    with {:ok, keys} <- require_binary_list(payload, "keys") do
      result_to_reply(FerricStore.Impl.del(state.instance_ctx, keys), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_mget, payload, state) do
    with {:ok, keys} <- require_binary_list(payload, "keys") do
      requested_collection_result_to_reply(length(keys), state, fn ->
        FerricStore.Impl.mget(state.instance_ctx, keys)
      end)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_mset, payload, state) do
    with {:ok, pairs} <- kv_pairs(payload) do
      result_to_reply(FerricStore.Impl.mset(state.instance_ctx, pairs), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_cas, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, expected} <- require_any(payload, "expected"),
         {:ok, value} <- require_any(payload, "value"),
         {:ok, ttl} <- optional_integer(payload, "ttl") do
      result_to_reply(cas_result(Ops.cas(state.instance_ctx, key, expected, value, ttl)), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_lock, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, owner} <- require_binary(payload, "owner"),
         {:ok, ttl_ms} <- require_pos_integer(payload, "ttl_ms") do
      result_to_reply(Ops.lock(state.instance_ctx, key, owner, ttl_ms), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_unlock, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, owner} <- require_binary(payload, "owner") do
      result_to_reply(unlock_result(Ops.unlock(state.instance_ctx, key, owner)), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_extend, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, owner} <- require_binary(payload, "owner"),
         {:ok, ttl_ms} <- require_pos_integer(payload, "ttl_ms") do
      result_to_reply(unlock_result(Ops.extend(state.instance_ctx, key, owner, ttl_ms)), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_ratelimit_add, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, window_ms} <- require_pos_integer(payload, "window_ms"),
         {:ok, max} <- require_pos_integer(payload, "max"),
         {:ok, count} <- optional_pos_integer(payload, "count", 1) do
      result_to_reply(
        {:ok, Ops.ratelimit_add(state.instance_ctx, key, window_ms, max, count)},
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_fetch_or_compute, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, ttl_ms} <- require_pos_integer(payload, "ttl_ms"),
         {:ok, hint} <- optional_binary(payload, "hint", "") do
      case FerricStore.fetch_or_compute(key, ttl: ttl_ms, hint: hint) do
        {:ok, {:hit, value}} -> {:ok, ["hit", value], state}
        {:ok, {:compute, token}} -> {:ok, ["compute", token], state}
        {:error, reason} -> {:error, reason, state}
      end
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_fetch_or_compute_result, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, value} <- require_any(payload, "value"),
         {:ok, ttl_ms} <- require_pos_integer(payload, "ttl_ms") do
      result_to_reply(FerricStore.fetch_or_compute_result(key, value, ttl: ttl_ms), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_fetch_or_compute_error, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, message} <- require_binary(payload, "message") do
      result_to_reply(Ferricstore.FetchOrCompute.fetch_or_compute_error(key, message), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_hset, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, fields} <- require_nonempty_map(payload, "fields") do
      result_to_reply(FerricStore.Impl.hset(state.instance_ctx, key, fields), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_hget, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, field} <- require_binary(payload, "field") do
      result_to_reply(FerricStore.Impl.hget(state.instance_ctx, key, field), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_hmget, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, fields} <- require_nonempty_binary_list(payload, "fields") do
      requested_collection_result_to_reply(length(fields), state, fn ->
        FerricStore.Impl.hmget(state.instance_ctx, key, fields)
      end)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_hgetall, payload, state) do
    with {:ok, key} <- require_binary(payload, "key") do
      counted_collection_result_to_reply(
        FerricStore.Impl.hlen(state.instance_ctx, key),
        state,
        fn ->
          FerricStore.Impl.hgetall(state.instance_ctx, key)
        end
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(opcode, payload, state) when opcode in [@op_lpush, @op_rpush] do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, values} <- require_nonempty_binary_list(payload, "values") do
      fun = if opcode == @op_lpush, do: &FerricStore.Impl.lpush/3, else: &FerricStore.Impl.rpush/3
      result_to_reply(fun.(state.instance_ctx, key, values), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(opcode, payload, state) when opcode in [@op_lpop, @op_rpop] do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, count} <- optional_pos_integer(payload, "count", 1) do
      fun = if opcode == @op_lpop, do: &FerricStore.Impl.lpop/3, else: &FerricStore.Impl.rpop/3
      list_pop_result_to_reply(state.instance_ctx, key, count, state, fun)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_lrange, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, start} <- require_integer(payload, "start"),
         {:ok, stop} <- require_integer(payload, "stop") do
      lrange_result_to_reply(state.instance_ctx, key, start, stop, state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(opcode, payload, state) when opcode in [@op_sadd, @op_srem] do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, members} <- require_nonempty_binary_list(payload, "members") do
      fun = if opcode == @op_sadd, do: &FerricStore.Impl.sadd/3, else: &FerricStore.Impl.srem/3
      result_to_reply(fun.(state.instance_ctx, key, members), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_smembers, payload, state) do
    with {:ok, key} <- require_binary(payload, "key") do
      counted_collection_result_to_reply(
        FerricStore.Impl.scard(state.instance_ctx, key),
        state,
        fn ->
          FerricStore.Impl.smembers(state.instance_ctx, key)
        end
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_sismember, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, member} <- require_binary(payload, "member") do
      result_to_reply(FerricStore.Impl.sismember(state.instance_ctx, key, member), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_zadd, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, items} <- zadd_items(payload) do
      result_to_reply(FerricStore.Impl.zadd(state.instance_ctx, key, items), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_zrem, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, members} <- require_nonempty_binary_list(payload, "members") do
      result_to_reply(FerricStore.Impl.zrem(state.instance_ctx, key, members), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_zrange, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, start} <- require_integer(payload, "start"),
         {:ok, stop} <- require_integer(payload, "stop"),
         {:ok, opts} <- zrange_opts(payload) do
      zrange_result_to_reply(state.instance_ctx, key, start, stop, opts, state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_zscore, payload, state) do
    with {:ok, key} <- require_binary(payload, "key"),
         {:ok, member} <- require_binary(payload, "member") do
      result_to_reply(FerricStore.Impl.zscore(state.instance_ctx, key, member), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(opcode, payload, state)
       when opcode in [
              @op_cluster_health,
              @op_cluster_stats,
              @op_cluster_keyslot,
              @op_cluster_slots,
              @op_cluster_status,
              @op_cluster_join,
              @op_cluster_leave,
              @op_cluster_failover,
              @op_cluster_promote,
              @op_cluster_demote,
              @op_cluster_role,
              @op_ferricstore_hotness
            ] do
    with {:ok, command} <- fetch_command(opcode),
         {:ok, args} <- native_args(payload) do
      result_to_reply(
        Ferricstore.Commands.Cluster.handle(command, args, state.instance_ctx),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
      {:error, status, reason} -> {status, reason, state}
    end
  end

  defp do_execute(@op_ferricstore_key_info, payload, state) do
    with {:ok, key} <- require_binary(payload, "key") do
      result_to_reply(
        Ferricstore.Commands.Native.handle("KEY_INFO", [key], state.instance_ctx),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_ferricstore_config, payload, state) do
    with {:ok, args} <- native_args(payload) do
      result_to_reply(
        Ferricstore.Commands.Namespace.handle("FERRICSTORE.CONFIG", args, state.instance_ctx),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_ferricstore_metrics, payload, state) do
    with {:ok, args} <- native_args(payload) do
      result =
        try do
          Ferricstore.Metrics.handle("FERRICSTORE.METRICS", args)
        rescue
          _ -> ""
        catch
          :exit, _ -> ""
        end

      result_to_reply(result, state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_ferricstore_blobgc, payload, state) do
    with {:ok, args} <- native_args(payload) do
      result_to_reply(
        Ferricstore.Commands.Server.handle("FERRICSTORE.BLOBGC", args, state.instance_ctx),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_create, payload, state) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, opts} <- flow_opts(payload, ["id"]) do
      result_to_reply(FerricStore.Impl.flow_create(state.instance_ctx, id, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_get, payload, state),
    do: flow_get_call(payload, state)

  defp do_execute(@op_flow_claim_due, payload, state) do
    with {:ok, type} <- require_binary(payload, "type"),
         {:ok, opts} <- flow_opts(payload, ["type"]) do
      result_to_reply(FerricStore.Impl.flow_claim_due(state.instance_ctx, type, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_complete, payload, state),
    do: flow_lease_call(payload, state, &FerricStore.Impl.flow_complete/4)

  defp do_execute(@op_flow_transition, payload, state) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, from_state} <- require_binary(payload, "from_state"),
         {:ok, to_state} <- require_binary(payload, "to_state"),
         {:ok, opts} <- flow_opts(payload, ["id", "from_state", "to_state"]) do
      result_to_reply(
        FerricStore.Impl.flow_transition(state.instance_ctx, id, from_state, to_state, opts),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_step_continue, payload, state) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, lease_token} <- require_binary(payload, "lease_token"),
         {:ok, from_state} <- require_binary(payload, "from_state"),
         {:ok, to_state} <- require_binary(payload, "to_state"),
         {:ok, opts} <- flow_opts(payload, ["id", "lease_token", "from_state", "to_state"]) do
      result_to_reply(
        FerricStore.Impl.flow_step_continue(
          state.instance_ctx,
          id,
          lease_token,
          from_state,
          to_state,
          opts
        ),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_start_and_claim, payload, state) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, type} <- require_binary(payload, "type"),
         {:ok, initial_state} <- require_binary(payload, "initial_state"),
         {:ok, opts} <- flow_opts(payload, ["id", "type", "initial_state"]) do
      result_to_reply(
        FerricStore.Impl.flow_start_and_claim(state.instance_ctx, id, type, initial_state, opts),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_retry, payload, state),
    do: flow_lease_call(payload, state, &FerricStore.Impl.flow_retry/4)

  defp do_execute(@op_flow_fail, payload, state),
    do: flow_lease_call(payload, state, &FerricStore.Impl.flow_fail/4)

  defp do_execute(@op_flow_cancel, payload, state),
    do: flow_id_opts_call(payload, state, &FerricStore.Impl.flow_cancel/3)

  defp do_execute(@op_flow_extend_lease, payload, state),
    do: flow_lease_call(payload, state, &FerricStore.Impl.flow_extend_lease/4)

  defp do_execute(@op_flow_history, payload, state),
    do: flow_id_opts_call(payload, state, &FerricStore.Impl.flow_history/3)

  defp do_execute(@op_flow_value_put, payload, state) do
    with {:ok, value} <- require_any(payload, "value"),
         {:ok, opts} <- flow_opts(payload, ["value"]) do
      result_to_reply(Ferricstore.Flow.value_put(state.instance_ctx, value, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_value_mget, payload, state) do
    with {:ok, refs} <- require_binary_list(payload, "refs"),
         {:ok, opts} <- flow_opts(payload, ["refs"]) do
      result_to_reply(Ferricstore.Flow.value_mget(state.instance_ctx, refs, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_signal, payload, state),
    do: flow_id_opts_call(payload, state, &Ferricstore.Flow.signal/3)

  defp do_execute(@op_flow_list, payload, state),
    do: flow_list_call(payload, state)

  defp do_execute(@op_flow_stats, payload, state),
    do: flow_type_opts_call(payload, state, &FerricStore.Impl.flow_stats/3)

  defp do_execute(@op_flow_attributes, payload, state),
    do: flow_type_opts_call(payload, state, &FerricStore.Impl.flow_attributes/3)

  defp do_execute(@op_flow_attribute_values, payload, state),
    do: flow_attribute_values_call(payload, state)

  defp do_execute(@op_flow_search, payload, state) do
    with {:ok, opts} <- flow_opts(payload, []) do
      result_to_reply(FerricStore.Impl.flow_search(state.instance_ctx, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_effect_reserve, payload, state) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, effect_key} <- require_binary(payload, "effect_key"),
         {:ok, effect_type} <- require_binary(payload, "effect_type"),
         {:ok, opts} <- flow_opts(payload, ["id", "effect_key", "effect_type"]) do
      result_to_reply(
        FerricStore.Impl.flow_effect_reserve(
          state.instance_ctx,
          id,
          effect_key,
          effect_type,
          opts
        ),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_effect_confirm, payload, state),
    do: flow_effect_status_call(payload, state, &FerricStore.Impl.flow_effect_confirm/4)

  defp do_execute(@op_flow_effect_fail, payload, state),
    do: flow_effect_status_call(payload, state, &FerricStore.Impl.flow_effect_fail/4)

  defp do_execute(@op_flow_effect_compensate, payload, state),
    do: flow_effect_status_call(payload, state, &FerricStore.Impl.flow_effect_compensate/4)

  defp do_execute(@op_flow_effect_get, payload, state) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, effect_key} <- require_binary(payload, "effect_key"),
         {:ok, opts} <- flow_opts(payload, ["id", "effect_key"]) do
      result_to_reply(
        FerricStore.Impl.flow_effect_get(state.instance_ctx, id, effect_key, opts),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_create_many, payload, state) do
    with {:ok, items} <- flow_items(payload, "items", :create),
         {:ok, opts} <- flow_opts(payload, ["partition_key", "items"]) do
      result_to_reply(
        FerricStore.Impl.flow_create_many(
          state.instance_ctx,
          Map.get(payload, "partition_key"),
          items,
          opts
        ),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_complete_many, payload, state),
    do: flow_many_items_call(payload, state, &Ferricstore.Flow.complete_many/4, :claimed)

  defp do_execute(@op_flow_run_steps_many, payload, state) do
    with {:ok, items} <-
           Ferricstore.LatencyTrace.span("server_flow_run_steps_items_us", fn ->
             flow_items(payload, "items", :run_steps)
           end),
         {:ok, opts} <-
           Ferricstore.LatencyTrace.span("server_flow_run_steps_opts_us", fn ->
             flow_opts(payload, ["items"])
           end) do
      result_to_reply(
        Ferricstore.LatencyTrace.span("server_flow_run_steps_impl_us", fn ->
          FerricStore.Impl.flow_run_steps_many(state.instance_ctx, items, opts)
        end),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_transition_many, payload, state) do
    with {:ok, from_state} <- require_binary(payload, "from_state"),
         {:ok, to_state} <- require_binary(payload, "to_state"),
         {:ok, items} <- flow_items(payload, "items", :transition),
         {:ok, opts} <- flow_opts(payload, ["partition_key", "from_state", "to_state", "items"]) do
      result_to_reply(
        FerricStore.Impl.flow_transition_many(
          state.instance_ctx,
          Map.get(payload, "partition_key"),
          from_state,
          to_state,
          items,
          opts
        ),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_retry_many, payload, state),
    do: flow_many_items_call(payload, state, &Ferricstore.Flow.retry_many/4, :claimed)

  defp do_execute(@op_flow_fail_many, payload, state),
    do: flow_many_items_call(payload, state, &Ferricstore.Flow.fail_many/4, :claimed)

  defp do_execute(@op_flow_cancel_many, payload, state),
    do: flow_many_items_call(payload, state, &Ferricstore.Flow.cancel_many/4, :fenced)

  defp do_execute(@op_flow_reclaim, payload, state),
    do: flow_type_opts_call(payload, state, &FerricStore.Impl.flow_reclaim/3)

  defp do_execute(@op_flow_rewind, payload, state),
    do: flow_id_opts_call(payload, state, &FerricStore.Impl.flow_rewind/3)

  defp do_execute(@op_flow_terminals, payload, state),
    do: flow_type_opts_call(payload, state, &FerricStore.Impl.flow_terminals/3)

  defp do_execute(@op_flow_failures, payload, state),
    do: flow_type_opts_call(payload, state, &FerricStore.Impl.flow_failures/3)

  defp do_execute(@op_flow_by_parent, payload, state) do
    with {:ok, parent_id} <- require_binary(payload, "parent_id"),
         {:ok, opts} <- flow_opts(payload, ["parent_id"]) do
      result_to_reply(FerricStore.Impl.flow_by_parent(state.instance_ctx, parent_id, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_by_root, payload, state) do
    with {:ok, root_id} <- require_binary(payload, "root_id"),
         {:ok, opts} <- flow_opts(payload, ["root_id"]) do
      result_to_reply(FerricStore.Impl.flow_by_root(state.instance_ctx, root_id, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_by_correlation, payload, state) do
    with {:ok, correlation_id} <- require_binary(payload, "correlation_id"),
         {:ok, opts} <- flow_opts(payload, ["correlation_id"]) do
      result_to_reply(
        FerricStore.Impl.flow_by_correlation(state.instance_ctx, correlation_id, opts),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_governance_ledger, payload, state) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, opts} <- flow_opts(payload, ["id"]) do
      result_to_reply(
        FerricStore.Impl.flow_governance_ledger(state.instance_ctx, id, opts),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_approval_request, payload, state) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, opts} <- flow_opts(payload, ["id"]) do
      result_to_reply(FerricStore.Impl.flow_approval_request(state.instance_ctx, id, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_approval_approve, payload, state),
    do: flow_approval_status_call(payload, state, &FerricStore.Impl.flow_approval_approve/3)

  defp do_execute(@op_flow_approval_reject, payload, state),
    do: flow_approval_status_call(payload, state, &FerricStore.Impl.flow_approval_reject/3)

  defp do_execute(@op_flow_approval_get, payload, state) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, opts} <- flow_opts(payload, ["id"]) do
      result_to_reply(FerricStore.Impl.flow_approval_get(state.instance_ctx, id, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_approval_list, payload, state) do
    with {:ok, opts} <- flow_opts(payload, []) do
      result_to_reply(FerricStore.Impl.flow_approval_list(state.instance_ctx, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_governance_overview, payload, state) do
    with {:ok, opts} <- flow_opts(payload, []) do
      result_to_reply(FerricStore.Impl.flow_governance_overview(state.instance_ctx, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_circuit_open, payload, state),
    do: flow_scope_call(payload, state, &FerricStore.Impl.flow_circuit_open/3)

  defp do_execute(@op_flow_circuit_close, payload, state),
    do: flow_scope_call(payload, state, &FerricStore.Impl.flow_circuit_close/3)

  defp do_execute(@op_flow_circuit_get, payload, state),
    do: flow_scope_call(payload, state, &FerricStore.Impl.flow_circuit_get/3)

  defp do_execute(@op_flow_budget_reserve, payload, state) do
    with {:ok, scope} <- require_binary(payload, "scope"),
         {:ok, amount} <- require_pos_integer(payload, "amount"),
         {:ok, opts} <- flow_opts(payload, ["scope", "amount"]) do
      result_to_reply(
        FerricStore.Impl.flow_budget_reserve(state.instance_ctx, scope, amount, opts),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_budget_commit, payload, state) do
    with {:ok, scope} <- require_binary(payload, "scope"),
         {:ok, reservation_id} <- require_binary(payload, "reservation_id"),
         {:ok, actual_amount} <- require_non_neg_integer(payload, "actual_amount"),
         {:ok, opts} <- flow_opts(payload, ["scope", "reservation_id", "actual_amount"]) do
      result_to_reply(
        FerricStore.Impl.flow_budget_commit(
          state.instance_ctx,
          scope,
          reservation_id,
          actual_amount,
          opts
        ),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_budget_release, payload, state) do
    with {:ok, scope} <- require_binary(payload, "scope"),
         {:ok, reservation_id} <- require_binary(payload, "reservation_id"),
         {:ok, opts} <- flow_opts(payload, ["scope", "reservation_id"]) do
      result_to_reply(
        FerricStore.Impl.flow_budget_release(state.instance_ctx, scope, reservation_id, opts),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_budget_get, payload, state),
    do: flow_scope_call(payload, state, &FerricStore.Impl.flow_budget_get/3)

  defp do_execute(@op_flow_budget_list, payload, state) do
    with {:ok, opts} <- flow_opts(payload, []) do
      result_to_reply(FerricStore.Impl.flow_budget_list(state.instance_ctx, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_limit_lease, payload, state),
    do: flow_scope_call(payload, state, &FerricStore.Impl.flow_limit_lease/3)

  defp do_execute(@op_flow_limit_spend, payload, state),
    do: flow_scope_call(payload, state, &FerricStore.Impl.flow_limit_spend/3)

  defp do_execute(@op_flow_limit_release, payload, state),
    do: flow_scope_call(payload, state, &FerricStore.Impl.flow_limit_release/3)

  defp do_execute(@op_flow_limit_get, payload, state),
    do: flow_scope_call(payload, state, &FerricStore.Impl.flow_limit_get/3)

  defp do_execute(@op_flow_limit_list, payload, state) do
    with {:ok, opts} <- flow_opts(payload, []) do
      result_to_reply(FerricStore.Impl.flow_limit_list(state.instance_ctx, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_info, payload, state),
    do: flow_type_opts_call(payload, state, &FerricStore.Impl.flow_info/3)

  defp do_execute(@op_flow_stuck, payload, state),
    do: flow_type_opts_call(payload, state, &FerricStore.Impl.flow_stuck/3)

  defp do_execute(@op_flow_policy_set, payload, state),
    do: flow_type_opts_call(payload, state, &FerricStore.Impl.flow_policy_set/3)

  defp do_execute(@op_flow_policy_get, payload, state),
    do: flow_type_opts_call(payload, state, &FerricStore.Impl.flow_policy_get/3)

  defp do_execute(@op_flow_spawn_children, payload, state) do
    with {:ok, parent_id} <- require_binary(payload, "id"),
         {:ok, children} <- flow_items(payload, "children", :map),
         {:ok, opts} <- flow_opts(payload, ["id", "children"]) do
      result_to_reply(
        FerricStore.Impl.flow_spawn_children(state.instance_ctx, parent_id, children, opts),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_retention_cleanup, payload, state) do
    with {:ok, opts} <- flow_opts(payload, []) do
      result_to_reply(Ferricstore.Flow.retention_cleanup(state.instance_ctx, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_schedule_create, payload, state) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, opts} <- flow_opts(payload, ["id"]) do
      result_to_reply(FerricStore.Impl.flow_schedule_create(state.instance_ctx, id, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_schedule_get, payload, state),
    do: flow_id_opts_call(payload, state, &FerricStore.Impl.flow_schedule_get/3)

  defp do_execute(@op_flow_schedule_fire, payload, state),
    do: flow_id_opts_call(payload, state, &FerricStore.Impl.flow_schedule_fire/3)

  defp do_execute(@op_flow_schedule_pause, payload, state),
    do: flow_id_opts_call(payload, state, &FerricStore.Impl.flow_schedule_pause/3)

  defp do_execute(@op_flow_schedule_resume, payload, state),
    do: flow_id_opts_call(payload, state, &FerricStore.Impl.flow_schedule_resume/3)

  defp do_execute(@op_flow_schedule_list, payload, state) do
    with {:ok, opts} <- flow_opts(payload, []) do
      result_to_reply(FerricStore.Impl.flow_schedule_list(state.instance_ctx, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_flow_schedule_delete, payload, state),
    do: flow_id_opts_call(payload, state, &FerricStore.Impl.flow_schedule_delete/3)

  defp do_execute(@op_flow_schedule_fire_due, payload, state) do
    with {:ok, opts} <- flow_opts(payload, []) do
      result_to_reply(FerricStore.Impl.flow_schedule_fire_due(state.instance_ctx, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(_opcode, _payload, state),
    do: {:bad_request, "ERR native unsupported opcode", state}

  defp flow_id_opts_call(payload, state, fun) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, opts} <- flow_opts(payload, ["id"]) do
      result_to_reply(fun.(state.instance_ctx, id, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp flow_type_opts_call(payload, state, fun) do
    with {:ok, type} <- require_binary(payload, "type"),
         {:ok, opts} <- flow_opts(payload, ["type"]) do
      result_to_reply(fun.(state.instance_ctx, type, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp flow_get_call(payload, state) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, opts} <- flow_opts(payload, ["id"]) do
      read_opts = Keyword.delete(opts, :return)

      state.instance_ctx
      |> FerricStore.Impl.flow_get(id, read_opts)
      |> FlowRecordProjection.maybe_meta_result(opts)
      |> result_to_reply(state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp flow_list_call(payload, state) do
    with {:ok, type} <- require_binary(payload, "type"),
         {:ok, opts} <- flow_opts(payload, ["type"]) do
      read_opts = Keyword.delete(opts, :return)

      state.instance_ctx
      |> FerricStore.Impl.flow_list(type, read_opts)
      |> FlowRecordProjection.maybe_meta_result(opts)
      |> result_to_reply(state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp flow_attribute_values_call(payload, state) do
    with {:ok, type} <- require_binary(payload, "type"),
         {:ok, attr_name} <- require_binary(payload, "attribute"),
         {:ok, opts} <- flow_opts(payload, ["type", "attribute"]) do
      result_to_reply(
        FerricStore.Impl.flow_attribute_values(state.instance_ctx, type, attr_name, opts),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp flow_lease_call(payload, state, fun) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, lease_token} <- require_binary(payload, "lease_token"),
         {:ok, opts} <- flow_opts(payload, ["id", "lease_token"]) do
      result_to_reply(fun.(state.instance_ctx, id, lease_token, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp flow_effect_status_call(payload, state, fun) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, effect_key} <- require_binary(payload, "effect_key"),
         {:ok, opts} <- flow_opts(payload, ["id", "effect_key"]) do
      result_to_reply(fun.(state.instance_ctx, id, effect_key, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp flow_approval_status_call(payload, state, fun) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, opts} <- flow_opts(payload, ["id"]) do
      result_to_reply(fun.(state.instance_ctx, id, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp flow_scope_call(payload, state, fun) do
    with {:ok, scope} <- require_binary(payload, "scope"),
         {:ok, opts} <- flow_opts(payload, ["scope"]) do
      result_to_reply(fun.(state.instance_ctx, scope, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp dispatch_command_exec(
         %PreparedCommand{command: "CLIENT", args: [subcmd | rest]},
         state,
         _request_context
       ) do
    subcmd
    |> String.upcase()
    |> FerricstoreServer.Commands.Client.handle(rest, state, state.instance_ctx)
    |> client_command_result_to_reply(state)
  end

  defp dispatch_command_exec(
         %PreparedCommand{command: "CLIENT", args: []},
         state,
         _request_context
       ),
       do: {:bad_request, "ERR wrong number of arguments for 'client' command", state}

  defp dispatch_command_exec(
         %PreparedCommand{command: "FERRICSTORE.METRICS", args: parsed_args},
         state,
         _request_context
       ) do
    ferricstore_metrics_result(parsed_args, state)
  end

  defp dispatch_command_exec(%PreparedCommand{} = prepared, state, request_context) do
    store = attach_request_context(state.instance_ctx, request_context)
    result = Ferricstore.Commands.Dispatcher.dispatch_prepared(prepared, store)
    result |> raw_result_to_native() |> result_to_reply(state)
  end

  defp ferricstore_metrics_result(parsed_args, state) do
    result =
      try do
        Ferricstore.Metrics.handle("FERRICSTORE.METRICS", parsed_args)
      rescue
        _ -> ""
      catch
        :exit, _ -> ""
      end

    result |> raw_result_to_native() |> result_to_reply(state)
  end

  defp client_command_result_to_reply({:ok, new_state}, _state), do: {:ok, "OK", new_state}

  defp client_command_result_to_reply({result, new_state}, _state),
    do: result |> raw_result_to_native() |> result_to_reply(new_state)

  defp raw_command_args(%{"args" => args}) when is_list(args), do: {:ok, args}
  defp raw_command_args(%{"args" => nil}), do: {:ok, []}
  defp raw_command_args(payload) when not is_map_key(payload, "args"), do: {:ok, []}
  defp raw_command_args(_payload), do: {:error, "ERR native COMMAND_EXEC args must be a list"}

  defp request_context(payload, state) when is_map(payload) do
    case Map.get(payload, "request_context") do
      nil -> {:ok, %{}}
      %{} = context -> trusted_request_context(context, state)
      _other -> invalid_request_context(state)
    end
  end

  defp request_context(_payload, _state), do: {:ok, %{}}

  defp trusted_request_context(context, state) do
    if trusted_request_context_connection?(state) do
      {:ok, normalize_request_context(context)}
    else
      {:ok, %{}}
    end
  end

  defp invalid_request_context(state) do
    if trusted_request_context_connection?(state) do
      {:error, "ERR native request_context must be an object"}
    else
      {:ok, %{}}
    end
  end

  defp trusted_request_context_connection?(state) do
    users = trusted_request_context_users()
    username = Map.get(state, :username)

    "*" in users or (is_binary(username) and username in users)
  end

  defp trusted_request_context_users do
    case Application.get_env(:ferricstore, :native_trusted_request_context_users, []) do
      users when is_list(users) ->
        users
        |> Enum.flat_map(&trusted_request_context_user/1)
        |> Enum.uniq()

      users when is_binary(users) ->
        String.split(users, [",", " "], trim: true)

      :all ->
        ["*"]

      _other ->
        []
    end
  end

  defp trusted_request_context_user(user) when is_binary(user), do: [user]
  defp trusted_request_context_user(user) when is_atom(user), do: [Atom.to_string(user)]
  defp trusted_request_context_user(_user), do: []

  defp normalize_request_context(%{} = context) do
    %{}
    |> put_context_value("subject", context_value(context, "subject", :subject))
    |> put_context_value("tenant", context_value(context, "tenant", :tenant))
    |> put_context_scopes(context_value(context, "scopes", :scopes))
  end

  defp put_context_value(payload, _key, value) when value in [nil, ""], do: payload

  defp put_context_value(payload, key, value) when is_binary(value),
    do: Map.put(payload, key, value)

  defp put_context_value(payload, _key, _value), do: payload

  defp put_context_scopes(payload, scopes) do
    scopes =
      scopes
      |> normalize_context_scopes()
      |> Enum.uniq()

    case scopes do
      [] -> payload
      scopes -> Map.put(payload, "scopes", scopes)
    end
  end

  defp normalize_context_scopes(scopes) when is_binary(scopes) do
    String.split(scopes, [",", " "], trim: true)
  end

  defp normalize_context_scopes(scopes) when is_list(scopes),
    do: Enum.filter(scopes, &is_binary/1)

  defp normalize_context_scopes(_scopes), do: []

  defp context_value(%{} = context, string_key, atom_key) do
    Map.get(context, string_key) || Map.get(context, atom_key)
  end

  defp attach_request_context(store, request_context) when request_context == %{}, do: store

  defp attach_request_context(%{} = store, request_context) when is_map(request_context) do
    Map.put(store, :request_context, request_context)
  end

  defp attach_request_context(store, _request_context), do: store

  defp raw_result_to_native({:simple, value}), do: value
  defp raw_result_to_native({:bulk, value}), do: value
  defp raw_result_to_native({:integer, value}), do: value
  defp raw_result_to_native({:array, value}), do: value
  defp raw_result_to_native({:push, value}), do: value
  defp raw_result_to_native({:error, _reason} = error), do: error
  defp raw_result_to_native(value), do: value

  defp flow_many_items_call(payload, state, fun, item_kind) do
    with {:ok, items} <- flow_items(payload, "items", item_kind),
         {:ok, opts} <- flow_opts(payload, ["partition_key", "items"]) do
      result_to_reply(
        fun.(state.instance_ctx, Map.get(payload, "partition_key"), items, opts),
        state
      )
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp result_to_reply(:ok, state), do: {:ok, "OK", state}
  defp result_to_reply({:ok, :ok}, state), do: {:ok, "OK", state}
  defp result_to_reply({:ok, value}, state), do: {:ok, value, state}
  defp result_to_reply({:error, %{code: _code} = reason}, state), do: {:error, reason, state}

  defp result_to_reply({:error, reason}, state) when is_binary(reason) do
    status =
      cond do
        String.starts_with?(reason, "BUSY") -> :busy
        String.starts_with?(reason, "OOM") -> :busy
        true -> :error
      end

    {status, reason, state}
  end

  defp result_to_reply({:error, reason}, state), do: {:error, inspect(reason), state}
  defp result_to_reply(value, state), do: {:ok, value, state}

  defp collection_result_to_reply({:ok, value}, state) do
    if collection_items_exceeds_limit?(collection_result_items(value), state) do
      collection_response_limit_reply(state)
    else
      {:ok, value, state}
    end
  end

  defp collection_result_to_reply(result, state), do: result_to_reply(result, state)

  defp requested_collection_result_to_reply(requested_count, state, fun) do
    if collection_items_exceeds_limit?(requested_count, state) do
      collection_response_limit_reply(state)
    else
      collection_result_to_reply(fun.(), state)
    end
  end

  defp counted_collection_result_to_reply(count_result, state, fun) do
    case count_result do
      {:ok, count} when is_integer(count) ->
        requested_collection_result_to_reply(count, state, fun)

      {:error, reason} ->
        result_to_reply({:error, reason}, state)

      _other ->
        collection_result_to_reply(fun.(), state)
    end
  end

  defp list_pop_result_to_reply(ctx, key, count, state, fun) do
    limit = native_max_collection_response_items(state)

    if limit > 0 and count > limit do
      case FerricStore.Impl.llen(ctx, key) do
        {:ok, len} when is_integer(len) and min(count, len) > limit ->
          collection_response_limit_reply(state)

        _other ->
          collection_result_to_reply(fun.(ctx, key, count), state)
      end
    else
      collection_result_to_reply(fun.(ctx, key, count), state)
    end
  end

  defp lrange_result_to_reply(ctx, key, start, stop, state) do
    limit = native_max_collection_response_items(state)
    remaining = compact_collection_remaining(limit, 0)

    with :ok <- compact_lrange_fallback_limit(ctx, key, start, stop, remaining) do
      collection_result_to_reply(FerricStore.Impl.lrange(ctx, key, start, stop), state)
    else
      :collection_response_limit_exceeded ->
        collection_response_limit_reply(state)
    end
  end

  defp zrange_result_to_reply(ctx, key, start, stop, opts, state) do
    limit = native_max_collection_response_items(state)
    remaining = compact_collection_remaining(limit, 0)

    with :ok <- compact_zrange_fallback_limit(ctx, key, start, stop, remaining) do
      collection_result_to_reply(FerricStore.Impl.zrange(ctx, key, start, stop, opts), state)
    else
      :collection_response_limit_exceeded ->
        collection_response_limit_reply(state)
    end
  end

  defp fetch_command(opcode) do
    case Map.fetch(@commands, opcode) do
      {:ok, command} -> {:ok, command}
      :error -> {:error, :bad_request, "ERR native unsupported opcode #{opcode}"}
    end
  end

  defp authorize("HELLO", _opcode, _payload, _state), do: :ok
  defp authorize("OPTIONS", _opcode, _payload, _state), do: :ok
  defp authorize("STARTUP", _opcode, _payload, _state), do: :ok
  defp authorize("AUTH", _opcode, _payload, _state), do: :ok
  defp authorize("COMMAND_EXEC", _opcode, _payload, _state), do: :ok

  defp authorize(command, opcode, payload, state) do
    cond do
      state.require_auth and not state.authenticated ->
        {:error, :auth, "NOAUTH Authentication required."}

      true ->
        with :ok <- ConnAuth.check_command_cached(state.acl_cache, command),
             :ok <-
               check_key_acl_cached(state.acl_cache, opcode, payload) do
          :ok
        else
          {:error, reason} ->
            FerricstoreServer.Acl.Protection.log_command_denied(
              state.username,
              command,
              format_peer(state.peer),
              state.client_id
            )

            {:error, :noperm, reason}
        end
    end
  end

  defp authorize_public_opcode(opcode, payload) do
    if Map.has_key?(@flow_commands, opcode) do
      :ok
    else
      opcode |> keys(payload) |> InternalKey.authorize_public()
    end
  end

  defp check_key_acl_cached(:full_access, _opcode, _payload), do: :ok
  defp check_key_acl_cached(%{keys: :all}, _opcode, _payload), do: :ok

  defp check_key_acl_cached(cache, opcode, payload) do
    ConnAuth.check_keys_cached(cache, key_acl_command(opcode, payload), keys(opcode, payload))
  end

  defp authorize_raw_command(%PreparedCommand{} = prepared, state) do
    cond do
      state.require_auth and not state.authenticated ->
        {:error, :auth, "NOAUTH Authentication required."}

      true ->
        acl_command =
          ConnAuth.acl_command_name(prepared.command, prepared.args, prepared.ast)

        with :ok <- ConnAuth.check_command_cached(state.acl_cache, acl_command),
             :ok <- ConnAuth.check_keys_cached(state.acl_cache, prepared) do
          :ok
        else
          {:error, reason} ->
            FerricstoreServer.Acl.Protection.log_command_denied(
              state.username,
              acl_command,
              format_peer(state.peer),
              state.client_id
            )

            {:error, :noperm, reason}
        end
    end
  end

  defp prepared_raw_command(payload, command, args) do
    case Map.get(payload, :__prepared_command__) do
      %PreparedCommand{args: ^args} = prepared ->
        if prepared.command == String.upcase(command) do
          {:ok, prepared}
        else
          Ferricstore.Commands.Dispatcher.prepare_raw(command, args)
        end

      _not_prepared ->
        Ferricstore.Commands.Dispatcher.prepare_raw(command, args)
    end
  end

  @flow_partition_or_id_opcodes [
    @op_flow_create,
    @op_flow_get,
    @op_flow_complete,
    @op_flow_transition,
    @op_flow_step_continue,
    @op_flow_start_and_claim,
    @op_flow_retry,
    @op_flow_fail,
    @op_flow_cancel,
    @op_flow_extend_lease,
    @op_flow_history,
    @op_flow_signal,
    @op_flow_rewind,
    @op_flow_effect_reserve,
    @op_flow_effect_confirm,
    @op_flow_effect_fail,
    @op_flow_effect_compensate,
    @op_flow_effect_get,
    @op_flow_governance_ledger
  ]

  @flow_schedule_id_opcodes [
    @op_flow_schedule_get,
    @op_flow_schedule_fire,
    @op_flow_schedule_pause,
    @op_flow_schedule_resume,
    @op_flow_schedule_delete
  ]

  @flow_approval_id_opcodes [
    @op_flow_approval_approve,
    @op_flow_approval_reject,
    @op_flow_approval_get
  ]

  @flow_batch_opcodes [
    @op_flow_create_many,
    @op_flow_complete_many,
    @op_flow_transition_many,
    @op_flow_retry_many,
    @op_flow_fail_many,
    @op_flow_cancel_many,
    @op_flow_run_steps_many
  ]

  @flow_partition_wide_opcodes [
    @op_flow_list,
    @op_flow_terminals,
    @op_flow_failures,
    @op_flow_info,
    @op_flow_stuck
  ]

  @flow_partition_or_global_opcodes [
    @op_flow_stats,
    @op_flow_attributes,
    @op_flow_attribute_values,
    @op_flow_search
  ]

  @flow_global_opcodes [
    @op_flow_value_mget,
    @op_flow_retention_cleanup,
    @op_flow_schedule_fire_due,
    @op_flow_schedule_list,
    @op_flow_approval_list,
    @op_flow_governance_overview,
    @op_flow_budget_list,
    @op_flow_limit_list
  ]

  defp keys(opcode, payload)
       when opcode in [
              @op_get,
              @op_set,
              @op_route,
              @op_cas,
              @op_lock,
              @op_unlock,
              @op_extend,
              @op_ratelimit_add,
              @op_fetch_or_compute,
              @op_fetch_or_compute_result,
              @op_fetch_or_compute_error,
              @op_hset,
              @op_hget,
              @op_hmget,
              @op_hgetall,
              @op_lpush,
              @op_rpush,
              @op_lpop,
              @op_rpop,
              @op_lrange,
              @op_sadd,
              @op_srem,
              @op_smembers,
              @op_sismember,
              @op_zadd,
              @op_zrem,
              @op_zrange,
              @op_zscore,
              @op_cluster_keyslot,
              @op_ferricstore_key_info
            ],
       do: binary_list([Map.get(payload, "key")])

  defp keys(@op_route_batch, payload), do: binary_list(Map.get(payload, "keys", []))

  defp keys(opcode, payload) when opcode in [@op_del, @op_mget],
    do: binary_list(Map.get(payload, "keys", []))

  defp keys(@op_mset, payload),
    do: payload |> Map.get("pairs", []) |> Enum.map(&pair_key/1) |> binary_list()

  defp keys(opcode, payload) when opcode in @flow_partition_or_id_opcodes,
    do: flow_partition_or_fallback(payload, [Map.get(payload, "id")])

  defp keys(@op_flow_schedule_create, payload), do: flow_schedule_create_acl_keys(payload)

  defp keys(opcode, payload) when opcode in @flow_schedule_id_opcodes,
    do: binary_list([Map.get(payload, "id")])

  defp keys(@op_flow_approval_request, payload), do: flow_approval_request_acl_keys(payload)

  defp keys(opcode, payload) when opcode in @flow_approval_id_opcodes,
    do: binary_list([Map.get(payload, "id")])

  defp keys(opcode, payload) when opcode in @flow_batch_opcodes,
    do: flow_batch_acl_keys(opcode, payload)

  defp keys(opcode, payload) when opcode in [@op_flow_claim_due, @op_flow_reclaim],
    do: flow_claim_due_acl_keys(payload)

  defp keys(opcode, payload) when opcode in @flow_partition_wide_opcodes,
    do: flow_partition_or_global(payload)

  defp keys(opcode, payload) when opcode in @flow_partition_or_global_opcodes,
    do: flow_partition_or_global(payload)

  defp keys(opcode, _payload) when opcode in @flow_global_opcodes, do: ["*"]

  defp keys(opcode, payload) when opcode in [@op_flow_policy_set, @op_flow_policy_get],
    do: binary_list([Map.get(payload, "type")])

  defp keys(@op_flow_by_parent, payload),
    do: flow_partition_or_global(payload)

  defp keys(@op_flow_by_root, payload),
    do: flow_partition_or_global(payload)

  defp keys(@op_flow_by_correlation, payload),
    do: flow_partition_or_global(payload)

  defp keys(@op_flow_spawn_children, payload), do: flow_spawn_acl_keys(payload)

  defp keys(@op_flow_value_put, payload),
    do: flow_value_put_acl_keys(payload)

  defp keys(opcode, payload)
       when opcode in [
              @op_flow_circuit_open,
              @op_flow_circuit_close,
              @op_flow_circuit_get,
              @op_flow_budget_reserve,
              @op_flow_budget_commit,
              @op_flow_budget_release,
              @op_flow_budget_get,
              @op_flow_limit_lease,
              @op_flow_limit_spend,
              @op_flow_limit_release,
              @op_flow_limit_get
            ],
       do: binary_list([Map.get(payload, "scope")])

  defp keys(opcode, _payload) when is_map_key(@flow_commands, opcode), do: ["*"]
  defp keys(_opcode, _payload), do: []

  defp flow_partition_or_global(payload) do
    case payload_flow_option(payload, "partition_key") do
      value when is_binary(value) and value != "" -> [value]
      _other -> ["*"]
    end
  end

  defp flow_partition_or_fallback(payload, fallback) do
    case payload_flow_option(payload, "partition_key") do
      value when is_binary(value) and value != "" ->
        [value]

      _other ->
        binary_list(fallback)
    end
  end

  defp flow_batch_acl_keys(@op_flow_run_steps_many, payload) do
    payload
    |> Map.get("items", [])
    |> flow_run_steps_acl_keys()
  end

  defp flow_batch_acl_keys(opcode, payload) do
    case Map.get(payload, "partition_key") do
      value when is_binary(value) and value != "" ->
        [value]

      _other ->
        flow_item_acl_keys(Map.get(payload, "items", []), opcode)
    end
  end

  defp flow_run_steps_acl_keys(items) when is_list(items) and items != [] do
    items
    |> Enum.map(&flow_run_steps_item_acl_key/1)
    |> Enum.map(fn
      value when is_binary(value) and value != "" -> value
      _unknown -> "*"
    end)
    |> Enum.uniq()
  end

  defp flow_run_steps_acl_keys(_items), do: ["*"]

  defp flow_run_steps_item_acl_key(%{} = item) do
    Map.get(item, "partition_key") || Map.get(item, :partition_key) ||
      flow_acl_id(Map.get(item, "id") || Map.get(item, :id))
  end

  defp flow_run_steps_item_acl_key({:id, id, :partition_key, partition_key})
       when is_binary(id),
       do: partition_key

  defp flow_run_steps_item_acl_key({:id, id}) when is_binary(id),
    do: id

  defp flow_run_steps_item_acl_key(id) when is_binary(id), do: id
  defp flow_run_steps_item_acl_key(_item), do: nil

  defp flow_acl_id(id) when is_binary(id) and id != "", do: id
  defp flow_acl_id(_id), do: nil

  defp flow_claim_due_acl_keys(payload) do
    partition_keys =
      case payload_flow_option(payload, "partition_keys") do
        values when is_list(values) -> binary_list(values)
        _other -> []
      end

    partition_keys =
      case payload_flow_option(payload, "partition_key") do
        value when is_binary(value) and value != "" ->
          [flow_claim_acl_partition(value) | partition_keys]

        _other ->
          partition_keys
      end

    case Enum.uniq(partition_keys) do
      [] -> ["*"]
      keys -> keys
    end
  end

  defp flow_claim_acl_partition(value) when is_binary(value) do
    if String.upcase(value) in ["AUTO", "ANY", "GLOBAL"], do: "*", else: value
  end

  defp flow_spawn_acl_keys(payload) do
    parent_keys = flow_partition_or_fallback(payload, [Map.get(payload, "id")])
    child_keys = flow_spawn_child_acl_keys(Map.get(payload, "children", []))
    Enum.uniq(parent_keys ++ child_keys)
  end

  defp flow_value_put_acl_keys(payload) do
    case payload_flow_option(payload, "partition_key") do
      value when is_binary(value) and value != "" ->
        [value]

      _other ->
        case payload_flow_option(payload, "owner_flow_id") do
          owner_flow_id when is_binary(owner_flow_id) and owner_flow_id != "" -> [owner_flow_id]
          _anonymous -> ["*"]
        end
    end
  end

  defp flow_schedule_create_acl_keys(payload) do
    schedule_id = Map.get(payload, "id")
    target_key = payload |> payload_flow_option("target") |> flow_schedule_target_acl_key()
    binary_list([schedule_id, target_key || "*"])
  end

  defp flow_schedule_target_acl_key(%{} = target) do
    Map.get(target, "partition_key") || Map.get(target, :partition_key) ||
      Map.get(target, "id") || Map.get(target, :id) || Map.get(target, "id_prefix") ||
      Map.get(target, :id_prefix)
  end

  defp flow_schedule_target_acl_key(target) when is_list(target) do
    Keyword.get(target, :partition_key) || Keyword.get(target, :id) ||
      Keyword.get(target, :id_prefix)
  end

  defp flow_schedule_target_acl_key(_target), do: nil

  defp flow_approval_request_acl_keys(payload) do
    [
      Map.get(payload, "id"),
      payload_flow_option(payload, "flow_id"),
      payload_flow_option(payload, "scope")
    ]
    |> binary_list()
    |> Enum.uniq()
  end

  defp flow_spawn_child_acl_keys(children) when is_list(children) do
    children
    |> Enum.map(fn
      %{} = child ->
        Map.get(child, "partition_key") || Map.get(child, :partition_key)

      [_id, partition_key, _payload] ->
        partition_key

      {:id, _id, :partition_key, partition_key, :payload, _payload} ->
        partition_key

      {_id, partition_key, _payload} ->
        partition_key

      _child ->
        nil
    end)
    |> binary_list()
    |> Enum.uniq()
  end

  defp flow_spawn_child_acl_keys(_children), do: []

  defp flow_item_acl_keys(items, opcode) when is_list(items) do
    items
    |> Enum.map(&flow_item_acl_key(&1, opcode))
    |> binary_list()
    |> Enum.uniq()
  end

  defp flow_item_acl_keys(_items, _opcode), do: []

  defp flow_item_acl_key(%{} = item, _opcode) do
    Map.get(item, "partition_key") || Map.get(item, :partition_key) || Map.get(item, "id") ||
      Map.get(item, :id)
  end

  defp flow_item_acl_key([_id, partition_key, _payload], @op_flow_create_many),
    do: partition_key

  defp flow_item_acl_key([_id, partition_key, _lease_token, _fencing_token], opcode)
       when opcode in [
              @op_flow_complete_many,
              @op_flow_transition_many,
              @op_flow_retry_many,
              @op_flow_fail_many
            ],
       do: partition_key

  defp flow_item_acl_key([_id, partition_key, _fencing_token], @op_flow_cancel_many),
    do: partition_key

  defp flow_item_acl_key([id | _rest], _opcode), do: id

  defp flow_item_acl_key(
         {:id, _id, :partition_key, partition_key, :payload, _payload},
         _opcode
       ),
       do: partition_key

  defp flow_item_acl_key(
         {:id, _id, :partition_key, partition_key, :lease_token, _lease_token, :fencing_token,
          _fencing_token},
         _opcode
       ),
       do: partition_key

  defp flow_item_acl_key(
         {:id, _id, :partition_key, partition_key, :fencing_token, _fencing_token},
         _opcode
       ),
       do: partition_key

  defp flow_item_acl_key(
         {:id, _id, :partition_key, partition_key, :fencing_token, _fencing_token, :lease_token,
          _lease_token},
         _opcode
       ),
       do: partition_key

  defp flow_item_acl_key({:id, id, :payload, _payload}, _opcode), do: id
  defp flow_item_acl_key({:id, id, :fencing_token, _token}, _opcode), do: id

  defp flow_item_acl_key(
         {:id, id, :lease_token, _lease_token, :fencing_token, _fencing_token},
         _opcode
       ),
       do: id

  defp flow_item_acl_key(
         {:id, id, :fencing_token, _fencing_token, :lease_token, _lease_token},
         _opcode
       ),
       do: id

  defp flow_item_acl_key({_id, partition_key, _payload}, @op_flow_create_many),
    do: partition_key

  defp flow_item_acl_key({_id, partition_key, _lease_token, _fencing_token}, opcode)
       when opcode in [
              @op_flow_complete_many,
              @op_flow_transition_many,
              @op_flow_retry_many,
              @op_flow_fail_many
            ],
       do: partition_key

  defp flow_item_acl_key({_id, partition_key, _fencing_token}, @op_flow_cancel_many),
    do: partition_key

  defp flow_item_acl_key({id, _payload}, _opcode), do: id
  defp flow_item_acl_key({id, _second, _third}, _opcode), do: id
  defp flow_item_acl_key({id, _second, _third, _fourth}, _opcode), do: id
  defp flow_item_acl_key(_item, _opcode), do: nil

  defp payload_flow_option(payload, key) when is_map(payload) do
    opts = payload |> Map.get("opts", %{}) |> option_map()

    if Map.has_key?(opts, key) do
      Map.get(opts, key)
    else
      Map.get(payload, key)
    end
  end

  defp key_acl_command(@op_set, payload) when is_map(payload) do
    if Map.get(payload, "get") == true, do: "GETSET", else: "SET"
  end

  defp key_acl_command(opcode, _payload), do: Map.get(@commands, opcode, "SET")

  defp pair_key(%{"key" => key}), do: key
  defp pair_key([key, _value]) when is_binary(key), do: key
  defp pair_key({key, _value}) when is_binary(key), do: key
  defp pair_key(_), do: nil

  defp binary_list(values) when is_list(values), do: Enum.filter(values, &is_binary/1)
  defp binary_list(_), do: []

  defp complete_auth(username, state) do
    AuditLog.log(:auth_success, %{username: username, client_ip: format_peer(state.peer)})

    state = %{
      state
      | username: username,
        authenticated: true,
        require_auth: false,
        acl_cache: ConnAuth.build_acl_cache(username)
    }

    ConnRegistry.update(state.client_id, self(), summary(state))
    {:ok, "OK", state}
  end

  defp auth_failure(username, state) do
    AuditLog.log(:auth_failure, %{username: username, client_ip: format_peer(state.peer)})
  end

  defp maybe_set_client_name(state, name) when is_binary(name) do
    state = %{state | client_name: name}
    ConnRegistry.update(state.client_id, self(), summary(state))
    state
  end

  defp maybe_set_client_name(state, _name), do: state

  defp maybe_set_compact_flow_responses(state, value)
       when value in [true, "true", "1", 1],
       do: Map.put(state, :compact_flow_responses, true)

  defp maybe_set_compact_flow_responses(state, _value),
    do: Map.put(state, :compact_flow_responses, false)

  defp negotiate_compression(state, compression) when compression in ["zlib", :zlib],
    do:
      if(Application.get_env(:ferricstore, :native_request_compression_enabled, false),
        do: {:ok, Map.put(state, :compression, :zlib)},
        else: {:error, "ERR native request compression is disabled"}
      )

  defp negotiate_compression(state, compression) when compression in ["none", nil, :none],
    do: {:ok, Map.put(state, :compression, :none)}

  defp negotiate_compression(_state, compression),
    do: {:error, "ERR native unsupported compression #{inspect(compression)}"}

  defp maybe_set_native_limit(state, key, value)
       when key in [:max_inflight_per_connection, :max_inflight_per_lane] do
    if is_integer(value) and value >= 0 do
      Map.put(state, key, min(value, configured_native_limit(key)))
    else
      state
    end
  end

  defp configured_native_limit(:max_inflight_per_connection),
    do: Application.get_env(:ferricstore, :native_max_inflight_per_connection, 4096)

  defp configured_native_limit(:max_inflight_per_lane),
    do: Application.get_env(:ferricstore, :native_max_inflight_per_lane, 1024)

  defp hello_payload(state) do
    endpoint = RouteMetadata.endpoint()
    route = hello_route_payload(state, endpoint)

    %{
      protocol: "ferricstore-native",
      version: 1,
      compression: Atom.to_string(Map.get(state, :compression, :none)),
      client_id: state.client_id,
      route_epoch: route_epoch(),
      capabilities: capabilities_payload(state),
      server: server_payload(),
      route: route,
      auth_required: state.require_auth and not state.authenticated,
      backpressure: backpressure_payload()
    }
  end

  defp hello_route_payload(%{require_auth: true, authenticated: false} = state, _endpoint) do
    %{
      slots: SlotMap.num_slots(),
      shard_count: state.instance_ctx.shard_count
    }
  end

  defp hello_route_payload(state, endpoint) do
    Map.merge(endpoint, %{
      slots: SlotMap.num_slots(),
      shard_count: state.instance_ctx.shard_count,
      endpoint: endpoint
    })
  end

  defp capabilities_payload(state) do
    %{
      protocol_versions: [1],
      direction_bit: true,
      multiplexing: %{
        lane_id: true,
        request_id: true,
        ordered_per_lane: true,
        concurrent_lanes: true,
        max_lanes_per_connection: Map.get(state, :max_lanes) || 1024
      },
      limits: %{
        max_frame_bytes:
          Map.get(state, :max_frame_bytes) ||
            Application.get_env(:ferricstore, :native_max_frame_bytes, 16 * 1024 * 1024),
        max_collection_response_items:
          Map.get(state, :max_collection_response_items) ||
            native_max_collection_response_items(),
        max_lane_queue:
          Map.get(state, :lane_max_queue) ||
            Application.get_env(:ferricstore, :native_lane_max_queue, 1024),
        max_pipeline_commands:
          Application.get_env(:ferricstore, :native_max_pipeline_commands, 1024)
      },
      flow_control: flow_control_payload(),
      chunking: %{
        request_reassembly: true,
        response_chunks: true,
        response_chunk_bytes:
          Map.get(state, :response_chunk_bytes) ||
            Application.get_env(:ferricstore, :native_response_chunk_bytes, 0)
      },
      response_codecs: %{
        typed_value: true,
        compact_flow_responses: Map.get(state, :compact_flow_responses, false),
        supported: ["typed_value", "flow_claim_jobs_v1", "ok_list_v1"]
      },
      compression: supported_compressions(),
      auth: @supported_auth,
      atomicity: @supported_atomicity,
      deadlines: %{
        field: "deadline_ms",
        clock: "server_unix_ms"
      },
      schemas: schema_payload(),
      events: @supported_events,
      opcodes: supported_opcodes()
    }
  end

  defp schema_payload do
    %{
      "GET" => %{"required" => ["key"], "fields" => ["key", "deadline_ms"]},
      "MGET" => %{"required" => ["keys"], "fields" => ["keys", "deadline_ms"]},
      "SET" => %{
        "required" => ["key", "value"],
        "fields" => ["key", "value", "ttl", "nx", "xx", "get", "deadline_ms"]
      },
      "CAS" => %{
        "required" => ["key", "expected", "value"],
        "fields" => ["key", "expected", "value", "ttl", "deadline_ms"]
      },
      "LOCK" => %{
        "required" => ["key", "owner", "ttl_ms"],
        "fields" => ["key", "owner", "ttl_ms", "deadline_ms"]
      },
      "UNLOCK" => %{
        "required" => ["key", "owner"],
        "fields" => ["key", "owner", "deadline_ms"]
      },
      "EXTEND" => %{
        "required" => ["key", "owner", "ttl_ms"],
        "fields" => ["key", "owner", "ttl_ms", "deadline_ms"]
      },
      "RATELIMIT.ADD" => %{
        "required" => ["key", "window_ms", "max"],
        "fields" => ["key", "window_ms", "max", "count", "deadline_ms"]
      },
      "FETCH_OR_COMPUTE" => %{
        "required" => ["key", "ttl_ms"],
        "fields" => ["key", "ttl_ms", "hint", "deadline_ms"]
      },
      "FETCH_OR_COMPUTE_RESULT" => %{
        "required" => ["key", "value", "ttl_ms"],
        "fields" => ["key", "value", "ttl_ms", "deadline_ms"]
      },
      "FETCH_OR_COMPUTE_ERROR" => %{
        "required" => ["key", "message"],
        "fields" => ["key", "message", "deadline_ms"]
      },
      "HSET" => %{
        "required" => ["key", "fields"],
        "fields" => ["key", "fields", "deadline_ms"]
      },
      "HGET" => %{
        "required" => ["key", "field"],
        "fields" => ["key", "field", "deadline_ms"]
      },
      "HMGET" => %{
        "required" => ["key", "fields"],
        "fields" => ["key", "fields", "deadline_ms"]
      },
      "HGETALL" => %{"required" => ["key"], "fields" => ["key", "deadline_ms"]},
      "LPUSH" => %{
        "required" => ["key", "values"],
        "fields" => ["key", "values", "deadline_ms"]
      },
      "RPUSH" => %{
        "required" => ["key", "values"],
        "fields" => ["key", "values", "deadline_ms"]
      },
      "LPOP" => %{"required" => ["key"], "fields" => ["key", "count", "deadline_ms"]},
      "RPOP" => %{"required" => ["key"], "fields" => ["key", "count", "deadline_ms"]},
      "LRANGE" => %{
        "required" => ["key", "start", "stop"],
        "fields" => ["key", "start", "stop", "deadline_ms"]
      },
      "SADD" => %{
        "required" => ["key", "members"],
        "fields" => ["key", "members", "deadline_ms"]
      },
      "SREM" => %{
        "required" => ["key", "members"],
        "fields" => ["key", "members", "deadline_ms"]
      },
      "SMEMBERS" => %{"required" => ["key"], "fields" => ["key", "deadline_ms"]},
      "SISMEMBER" => %{
        "required" => ["key", "member"],
        "fields" => ["key", "member", "deadline_ms"]
      },
      "ZADD" => %{
        "required" => ["key", "items"],
        "fields" => ["key", "items", "deadline_ms"]
      },
      "ZREM" => %{
        "required" => ["key", "members"],
        "fields" => ["key", "members", "deadline_ms"]
      },
      "ZRANGE" => %{
        "required" => ["key", "start", "stop"],
        "fields" => ["key", "start", "stop", "withscores", "deadline_ms"]
      },
      "ZSCORE" => %{
        "required" => ["key", "member"],
        "fields" => ["key", "member", "deadline_ms"]
      },
      "CLUSTER.HEALTH" => %{"required" => [], "fields" => ["args", "deadline_ms"]},
      "CLUSTER.STATS" => %{"required" => [], "fields" => ["args", "deadline_ms"]},
      "CLUSTER.KEYSLOT" => %{"required" => ["key"], "fields" => ["key", "args", "deadline_ms"]},
      "CLUSTER.SLOTS" => %{"required" => [], "fields" => ["args", "deadline_ms"]},
      "CLUSTER.STATUS" => %{"required" => [], "fields" => ["args", "deadline_ms"]},
      "CLUSTER.JOIN" => %{"required" => ["args"], "fields" => ["args", "deadline_ms"]},
      "CLUSTER.LEAVE" => %{"required" => [], "fields" => ["args", "deadline_ms"]},
      "CLUSTER.FAILOVER" => %{"required" => ["args"], "fields" => ["args", "deadline_ms"]},
      "CLUSTER.PROMOTE" => %{"required" => ["args"], "fields" => ["args", "deadline_ms"]},
      "CLUSTER.DEMOTE" => %{"required" => ["args"], "fields" => ["args", "deadline_ms"]},
      "CLUSTER.ROLE" => %{"required" => [], "fields" => ["args", "deadline_ms"]},
      "FERRICSTORE.KEY_INFO" => %{
        "required" => ["key"],
        "fields" => ["key", "args", "deadline_ms"]
      },
      "FERRICSTORE.CONFIG" => %{"required" => ["args"], "fields" => ["args", "deadline_ms"]},
      "FERRICSTORE.HOTNESS" => %{"required" => [], "fields" => ["args", "deadline_ms"]},
      "FERRICSTORE.METRICS" => %{"required" => [], "fields" => ["args", "deadline_ms"]},
      "FERRICSTORE.BLOBGC" => %{"required" => [], "fields" => ["args", "deadline_ms"]},
      "FLOW.CREATE" => %{
        "required" => ["id"],
        "fields" => [
          "id",
          "type",
          "state",
          "payload",
          "payload_ref",
          "payload_refs",
          "value_refs",
          "attributes",
          "partition_key",
          "parent_id",
          "root_id",
          "correlation_id",
          "run_at_ms",
          "due_after_ms",
          "priority",
          "retention_ttl_ms",
          "max_active_ms",
          "deadline_ms"
        ]
      },
      "FLOW.CREATE_MANY" => %{
        "required" => ["items"],
        "fields" => [
          "items",
          "partition_key",
          "type",
          "state",
          "payload",
          "payload_ref",
          "attributes",
          "run_at_ms",
          "priority",
          "retention_ttl_ms",
          "max_active_ms",
          "history_hot_max_events",
          "history_max_events",
          "independent",
          "return",
          "now_ms",
          "deadline_ms"
        ]
      },
      "FLOW.CLAIM_DUE" => %{
        "required" => ["type"],
        "fields" => [
          "type",
          "states",
          "limit",
          "lease_ms",
          "worker",
          "partition_key",
          "partition_keys",
          "block_ms",
          "reclaim_expired",
          "reclaim_ratio",
          "return",
          "deadline_ms"
        ]
      },
      "FLOW.COMPLETE" => %{
        "required" => ["id", "lease_token"],
        "fields" => [
          "id",
          "lease_token",
          "result",
          "result_ref",
          "attributes",
          "attributes_merge",
          "attributes_delete",
          "deadline_ms"
        ]
      },
      "FLOW.TRANSITION" => %{
        "required" => ["id", "from_state", "to_state"],
        "fields" => [
          "id",
          "from_state",
          "to_state",
          "payload",
          "payload_ref",
          "attributes",
          "attributes_merge",
          "attributes_delete",
          "delay_ms",
          "deadline_ms"
        ]
      },
      "FLOW.STATS" => %{
        "required" => ["type"],
        "fields" => [
          "type",
          "state",
          "attributes",
          "partition_key",
          "count",
          "consistent_projection",
          "deadline_ms"
        ]
      },
      "FLOW.ATTRIBUTES" => %{
        "required" => ["type"],
        "fields" => [
          "type",
          "state",
          "partition_key",
          "count",
          "consistent_projection",
          "deadline_ms"
        ]
      },
      "FLOW.ATTRIBUTE_VALUES" => %{
        "required" => ["type", "attribute"],
        "fields" => [
          "type",
          "attribute",
          "state",
          "partition_key",
          "count",
          "consistent_projection",
          "deadline_ms"
        ]
      },
      "FLOW.POLICY.SET" => %{
        "required" => ["type"],
        "fields" => [
          "type",
          "max_active_ms",
          "retry",
          "retention",
          "states",
          "indexed_attributes",
          "indexed_state_meta",
          "version",
          "governance",
          "deadline_ms"
        ]
      },
      "FLOW.STEP_CONTINUE" => %{
        "required" => ["id", "lease_token", "from_state", "to_state", "fencing_token"],
        "fields" => [
          "id",
          "lease_token",
          "from_state",
          "to_state",
          "fencing_token",
          "partition_key",
          "lease_ms",
          "worker",
          "payload",
          "payload_ref",
          "values",
          "value_refs",
          "drop_values",
          "override_values",
          "attributes",
          "attributes_merge",
          "attributes_delete",
          "now_ms",
          "deadline_ms"
        ]
      },
      "FLOW.START_AND_CLAIM" => %{
        "required" => ["id", "type", "initial_state", "worker"],
        "fields" => [
          "id",
          "type",
          "initial_state",
          "worker",
          "lease_ms",
          "payload",
          "payload_ref",
          "values",
          "value_refs",
          "drop_values",
          "override_values",
          "attributes",
          "partition_key",
          "parent_id",
          "root_id",
          "correlation_id",
          "priority",
          "retention_ttl_ms",
          "max_active_ms",
          "history_hot_max_events",
          "history_max_events",
          "now_ms",
          "deadline_ms"
        ]
      },
      "FLOW.RUN_STEPS_MANY" => %{
        "required" => ["items", "type", "worker"],
        "fields" => [
          "items",
          "type",
          "states",
          "steps",
          "worker",
          "lease_ms",
          "payload",
          "result",
          "retention_ttl_ms",
          "now_ms",
          "deadline_ms"
        ]
      },
      "FLOW.VALUE.PUT" => %{
        "required" => ["value"],
        "fields" => [
          "value",
          "partition_key",
          "owner_flow_id",
          "name",
          "ttl_ms",
          "override",
          "local_cache",
          "deadline_ms"
        ]
      },
      "FLOW.VALUE.MGET" => %{
        "required" => ["refs"],
        "fields" => ["refs", "max_bytes", "payload_max_bytes", "value_max_bytes", "deadline_ms"]
      },
      "FLOW.SIGNAL" => %{
        "required" => ["id"],
        "fields" => ["id", "type", "payload", "deadline_ms"]
      },
      "FLOW.SPAWN_CHILDREN" => %{
        "required" => ["id", "children", "partition_key", "group_id", "fencing_token"],
        "fields" => [
          "id",
          "children",
          "partition_key",
          "group_id",
          "wait",
          "wait_state",
          "success",
          "failure",
          "from_state",
          "lease_token",
          "fencing_token",
          "on_child_failed",
          "on_parent_closed",
          "max_active_ms",
          "now_ms",
          "deadline_ms"
        ]
      },
      "FLOW.RETENTION_CLEANUP" => %{
        "required" => [],
        "fields" => ["limit", "now_ms", "deadline_ms"]
      },
      "FLOW.SCHEDULE.CREATE" => %{
        "required" => ["id", "target"],
        "fields" => [
          "id",
          "kind",
          "target",
          "at_ms",
          "delay_ms",
          "start_at_ms",
          "every_ms",
          "cron",
          "timezone",
          "now_ms",
          "overwrite",
          "overlap_policy",
          "overlap_retry_ms",
          "max_fires",
          "end_at_ms",
          "deadline_ms"
        ]
      },
      "FLOW.SCHEDULE.GET" => %{
        "required" => ["id"],
        "fields" => ["id", "deadline_ms"]
      },
      "FLOW.SCHEDULE.FIRE" => %{
        "required" => ["id"],
        "fields" => ["id", "now_ms", "fire_at_ms", "deadline_ms"]
      },
      "FLOW.SCHEDULE.PAUSE" => %{
        "required" => ["id"],
        "fields" => ["id", "now_ms", "deadline_ms"]
      },
      "FLOW.SCHEDULE.RESUME" => %{
        "required" => ["id"],
        "fields" => ["id", "now_ms", "deadline_ms"]
      },
      "FLOW.SCHEDULE.DELETE" => %{
        "required" => ["id"],
        "fields" => ["id", "now_ms", "deadline_ms"]
      },
      "FLOW.SCHEDULE.FIRE_DUE" => %{
        "required" => [],
        "fields" => ["now_ms", "worker", "limit", "lease_ms", "block_ms", "deadline_ms"]
      },
      "FLOW.SCHEDULE.LIST" => %{
        "required" => [],
        "fields" => [
          "state",
          "kind",
          "target_type",
          "timezone",
          "from_ms",
          "to_ms",
          "count",
          "deadline_ms"
        ]
      }
    }
  end

  defp flow_control_payload(state \\ %{}) do
    %{
      max_inflight_per_connection:
        Map.get(state, :max_inflight_per_connection) ||
          Application.get_env(:ferricstore, :native_max_inflight_per_connection, 4096),
      max_inflight_per_lane:
        Map.get(state, :max_inflight_per_lane) ||
          Application.get_env(:ferricstore, :native_max_inflight_per_lane, 1024),
      response_coalesce_bytes:
        Map.get(state, :response_coalesce_bytes) ||
          Application.get_env(:ferricstore, :native_response_coalesce_bytes, 8 * 1024 * 1024),
      enforced: true,
      window_update: true
    }
  end

  defp supported_compressions do
    if Application.get_env(:ferricstore, :native_request_compression_enabled, false) do
      ["none", "zlib"]
    else
      @supported_compressions
    end
  end

  defp server_payload do
    ctx = FerricStore.Instance.get(:default)
    base = %{node: Atom.to_string(node()), native_enabled: true}

    try do
      Map.merge(base, ctx.server_info_fn.())
    rescue
      _ -> base
    end
  end

  defp route_payload(ctx, key) do
    slot = Router.slot_for(ctx, key)
    shard = Router.shard_for(ctx, key)
    target = RouteMetadata.target_for_shard(shard)

    Map.merge(target, %{
      key: key,
      slot: slot,
      shard: shard,
      lane_id: shard + 1,
      route_epoch: route_epoch()
    })
  end

  defp shards_payload(ctx) do
    ranges =
      ctx.slot_map
      |> SlotMap.slot_ranges()
      |> Enum.map(fn {first, last, shard} ->
        target = RouteMetadata.target_for_shard(shard)

        Map.merge(target, %{
          first_slot: first,
          last_slot: last,
          shard: shard,
          lane_id: shard + 1
        })
      end)

    %{
      slots: SlotMap.num_slots(),
      shard_count: ctx.shard_count,
      route_epoch: route_epoch(),
      ranges: ranges
    }
  end

  defp route_epoch do
    :erlang.phash2(FerricStore.Instance.get(:default).slot_map)
  rescue
    _ -> 0
  end

  defp check_deadline(%{"deadline_ms" => deadline_ms}) when is_integer(deadline_ms) do
    if deadline_ms > 0 and deadline_ms < System.system_time(:millisecond) do
      {:error, :error,
       %{
         "code" => "deadline_exceeded",
         "message" => "ERR native request deadline exceeded",
         "retryable" => false,
         "safe_to_retry" => false,
         "retry_after_ms" => 0
       }}
    else
      :ok
    end
  end

  defp check_deadline(_payload), do: :ok

  defp backpressure_payload do
    %{
      reject_writes: safe_memory_guard(&Ferricstore.MemoryGuard.reject_writes?/0, false),
      keydir_full: safe_memory_guard(&Ferricstore.MemoryGuard.keydir_full?/0, false),
      retry_after_ms: retry_after_ms()
    }
  end

  defp safe_memory_guard(fun, default) do
    fun.()
  rescue
    _ -> default
  catch
    :exit, _ -> default
  end

  defp retry_after_ms do
    if safe_memory_guard(&Ferricstore.MemoryGuard.reject_writes?/0, false), do: 100, else: 0
  end

  defp do_auth(username, password, state) do
    user = FerricstoreServer.Acl.get_user(username)

    has_acl_password =
      case user do
        %{password: stored} when is_binary(stored) -> true
        _ -> false
      end

    requirepass = requirepass()
    has_requirepass = requirepass not in [nil, ""]
    passwordless_acl_user? = match?(%{enabled: true, password: nil}, user)

    cond do
      has_acl_password ->
        acl_auth(username, password, state)

      username == "default" and constant_time_equal?(password, requirepass) ->
        complete_auth(username, state)

      username == "default" and has_requirepass ->
        auth_failure(username, state)
        {:auth, "WRONGPASS invalid username-password pair or user is disabled.", state}

      username == "default" ->
        if passwordless_acl_user? do
          auth_without_configured_password(username, password, state)
        else
          acl_auth(username, password, state)
        end

      passwordless_acl_user? and not has_requirepass ->
        auth_without_configured_password(username, password, state)

      true ->
        acl_auth(username, password, state)
    end
  end

  defp auth_without_configured_password(username, password, state) do
    _result = FerricstoreServer.Acl.authenticate(username, password)
    auth_failure(username, state)

    {:auth,
     "ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?",
     state}
  end

  defp acl_auth(username, password, state) do
    case FerricstoreServer.Acl.authenticate(username, password) do
      {:ok, ^username} ->
        complete_auth(username, state)

      {:error, reason} ->
        auth_failure(username, state)
        {:auth, reason, state}
    end
  end

  defp constant_time_equal?(left, right) when is_binary(left) and is_binary(right) do
    byte_size(left) == byte_size(right) and :crypto.hash_equals(left, right)
  end

  defp constant_time_equal?(_left, _right), do: false

  defp requirepass do
    Ferricstore.Config.get_value("requirepass") || Application.get_env(:ferricstore, :requirepass)
  rescue
    _ -> Application.get_env(:ferricstore, :requirepass)
  catch
    :exit, _ -> Application.get_env(:ferricstore, :requirepass)
  end

  defp normalize_payload(payload) when is_map(payload), do: payload
  defp normalize_payload(nil), do: %{}
  defp normalize_payload(_payload), do: %{}

  defp require_binary(payload, key) do
    case Map.get(payload, key) do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, "ERR native field #{key} must be binary"}
    end
  end

  defp require_map(payload, key) do
    case Map.get(payload, key) do
      value when is_map(value) -> {:ok, value}
      _ -> {:error, "ERR native field #{key} must be a map"}
    end
  end

  defp require_nonempty_map(payload, key) do
    with {:ok, value} <- require_map(payload, key) do
      if map_size(value) > 0 do
        {:ok, value}
      else
        {:error, "ERR native field #{key} must not be empty"}
      end
    end
  end

  defp optional_binary(payload, key, default) do
    case Map.get(payload, key, default) do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, "ERR native field #{key} must be binary"}
    end
  end

  defp require_pos_integer(payload, key) do
    case Map.get(payload, key) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR native field #{key} must be a positive integer"}
    end
  end

  defp require_non_neg_integer(payload, key) do
    case Map.get(payload, key) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, "ERR native field #{key} must be a non-negative integer"}
    end
  end

  defp require_integer(payload, key) do
    case Map.get(payload, key) do
      value when is_integer(value) -> {:ok, value}
      _ -> {:error, "ERR native field #{key} must be an integer"}
    end
  end

  defp optional_integer(payload, key) do
    case Map.get(payload, key) do
      nil -> {:ok, nil}
      value when is_integer(value) -> {:ok, value}
      _ -> {:error, "ERR native field #{key} must be an integer"}
    end
  end

  defp optional_pos_integer(payload, key, default) do
    case Map.get(payload, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR native field #{key} must be a positive integer"}
    end
  end

  defp require_any(payload, key) do
    case Map.fetch(payload, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, "ERR native field #{key} is required"}
    end
  end

  defp require_binary_list(payload, key) do
    case {Map.get(payload, :__wire_compact_validated__), Map.get(payload, key)} do
      {true, values} when is_list(values) -> {:ok, values}
      {_validated, values} when is_list(values) -> require_all_binaries(values, key)
      _ -> {:error, "ERR native field #{key} must be a binary list"}
    end
  end

  defp require_nonempty_binary_list(payload, key) do
    with {:ok, values} <- require_binary_list(payload, key) do
      if values != [] do
        {:ok, values}
      else
        {:error, "ERR native field #{key} must not be empty"}
      end
    end
  end

  defp require_all_binaries(values, key) do
    if Enum.all?(values, &is_binary/1) do
      {:ok, values}
    else
      {:error, "ERR native field #{key} must be a binary list"}
    end
  end

  defp zadd_items(%{"items" => items}) when is_list(items) do
    items =
      Enum.map(items, fn
        [score, member] when is_number(score) and is_binary(member) ->
          {score, member}

        {score, member} when is_number(score) and is_binary(member) ->
          {score, member}

        %{"score" => score, "member" => member} when is_number(score) and is_binary(member) ->
          {score, member}

        _ ->
          :bad_item
      end)

    if Enum.any?(items, &(&1 == :bad_item)) do
      {:error, "ERR native zadd items must be [score, member] pairs"}
    else
      {:ok, items}
    end
  end

  defp zadd_items(_payload), do: {:error, "ERR native field items must be a list"}

  defp zrange_opts(payload) do
    case Map.get(payload, "withscores", false) do
      value when value in [true, false] -> {:ok, [withscores: value]}
      _ -> {:error, "ERR native field withscores must be boolean"}
    end
  end

  defp kv_set_opts(payload) do
    opts =
      []
      |> maybe_option(:ttl, Map.get(payload, "ttl"))
      |> maybe_option(:nx, Map.get(payload, "nx"))
      |> maybe_option(:xx, Map.get(payload, "xx"))
      |> maybe_option(:get, Map.get(payload, "get"))
      |> maybe_option(:keepttl, Map.get(payload, "keepttl"))
      |> maybe_option(:exat, Map.get(payload, "exat"))
      |> maybe_option(:pxat, Map.get(payload, "pxat"))

    {:ok, opts}
  end

  defp maybe_option(opts, _key, nil), do: opts
  defp maybe_option(opts, key, value), do: [{key, value} | opts]

  defp cas_result(1), do: {:ok, true}
  defp cas_result(0), do: {:ok, false}
  defp cas_result(nil), do: {:ok, nil}
  defp cas_result({:error, _reason} = error), do: error
  defp cas_result(other), do: {:error, "ERR native cas failed: #{inspect(other)}"}

  defp unlock_result(1), do: {:ok, 1}
  defp unlock_result({:error, _reason} = error), do: error
  defp unlock_result(other), do: {:error, "ERR native lock command failed: #{inspect(other)}"}

  defp kv_pairs(%{"pairs" => pairs} = payload) when is_list(pairs) do
    if Map.get(payload, :__wire_compact_validated__) do
      {:ok, pairs}
    else
      kv_pairs_validate(pairs)
    end
  end

  defp kv_pairs(_payload), do: {:error, "ERR native field pairs must be a list"}

  defp kv_pairs_validate(pairs) do
    pairs =
      Enum.map(pairs, fn
        %{"key" => key, "value" => value} when is_binary(key) -> {key, value}
        [key, value] when is_binary(key) -> {key, value}
        {key, value} when is_binary(key) -> {key, value}
        _ -> :bad_pair
      end)

    if Enum.any?(pairs, &(&1 == :bad_pair)) do
      {:error, "ERR native mset pairs must be maps or [key, value] arrays"}
    else
      {:ok, pairs}
    end
  end

  defp native_args(%{"args" => args}) when is_list(args) do
    {:ok, Enum.map(args, &native_arg/1)}
  end

  defp native_args(%{"args" => _args}), do: {:error, "ERR native field args must be a list"}
  defp native_args(_payload), do: {:ok, []}

  defp native_arg(value) when is_binary(value), do: value
  defp native_arg(value) when is_integer(value), do: Integer.to_string(value)
  defp native_arg(value) when is_atom(value), do: Atom.to_string(value)
  defp native_arg(value), do: to_string(value)

  defp event_list(%{"events" => raw_events}) when is_list(raw_events) do
    events = Enum.map(raw_events, &normalize_event/1)

    if Enum.any?(events, &is_nil/1) do
      {:error, "ERR native events must be known event names"}
    else
      {:ok, events}
    end
  end

  defp event_list(_payload), do: {:error, "ERR native field events must be a list"}

  defp maybe_startup_subscribe_events(state, events, payload) do
    requested? =
      (is_list(events) and events != []) or
        (is_map(payload) and Map.has_key?(payload, "flow_wake"))

    cond do
      requested? and state.require_auth and not state.authenticated ->
        {:error, :auth, "NOAUTH Authentication required before event subscription."}

      requested? ->
        with {:ok, events} <- event_list(%{"events" => events}),
             {:ok, state} <-
               maybe_subscribe_flow_wake(state, events, Map.get(payload, "flow_wake")) do
          {:ok, subscribe_events(state, events)}
        else
          {:error, reason} -> {:error, :bad_request, reason}
        end

      true ->
        {:ok, state}
    end
  end

  defp subscribe_events(state, events) do
    current = Map.get(state, :event_subscriptions, MapSet.new())
    Map.put(state, :event_subscriptions, Enum.reduce(events, current, &MapSet.put(&2, &1)))
  end

  defp unsubscribe_events(state, events) do
    current = Map.get(state, :event_subscriptions, MapSet.new())
    Map.put(state, :event_subscriptions, Enum.reduce(events, current, &MapSet.delete(&2, &1)))
  end

  defp maybe_subscribe_flow_wake(state, events, flow_wake) do
    cond do
      "FLOW_WAKE" not in events ->
        {:ok, state}

      is_nil(flow_wake) ->
        {:ok, state}

      is_map(flow_wake) ->
        with {:ok, subscription} <- flow_wake_subscription(flow_wake) do
          state = unsubscribe_flow_wake(state)

          case ClaimWaiters.register(subscription.keys, self(), 0, limit: subscription.limit) do
            :ok -> {:ok, Map.put(state, :flow_wake_subscription, subscription)}
            {:error, reason} -> {:error, reason}
          end
        end

      true ->
        {:error, "ERR native field flow_wake must be an object"}
    end
  end

  defp maybe_unsubscribe_flow_wake(state, events) do
    if "FLOW_WAKE" in events do
      unsubscribe_flow_wake(state)
    else
      state
    end
  end

  defp unsubscribe_flow_wake(state) do
    case Map.get(state, :flow_wake_subscription) do
      %{keys: keys} -> ClaimWaiters.unregister(keys, self())
      _other -> :ok
    end

    Map.put(state, :flow_wake_subscription, nil)
  end

  defp flow_wake_subscription(payload) do
    with {:ok, type} <- require_binary(payload, "type"),
         {:ok, opts} <- flow_opts(payload, ["type"]),
         {:ok, keys, limit} <- ClaimDueAPI.wait_registration(type, opts) do
      {:ok, %{type: type, keys: keys, limit: limit}}
    end
  end

  @spec refresh_flow_wake_subscription(map()) :: map()
  def refresh_flow_wake_subscription(state) do
    case Map.get(state, :flow_wake_subscription) do
      %{keys: keys, limit: limit} ->
        ClaimWaiters.unregister(keys, self())
        ClaimWaiters.register(keys, self(), 0, limit: limit)
        state

      _other ->
        state
    end
  end

  @spec flow_wake_event_payload(map()) :: map()
  def flow_wake_event_payload(state) do
    case Map.get(state, :flow_wake_subscription) do
      %{type: type, limit: limit} ->
        %{type: type, credit: limit, reason: "ready"}

      _other ->
        %{credit: 1, reason: "ready"}
    end
  end

  defp event_subscription_payload(state) do
    %{
      subscribed:
        state |> Map.get(:event_subscriptions, MapSet.new()) |> MapSet.to_list() |> Enum.sort(),
      supported: @supported_events
    }
  end

  defp normalize_event(event) when is_binary(event) do
    event = String.upcase(event)
    if event in @supported_events, do: event
  end

  defp normalize_event(_event), do: nil

  defp execute_typed_pipeline(payload, state) do
    with {:ok, request_context} <- request_context(payload, state),
         {:ok, commands} <- pipeline_commands(payload),
         {:ok, atomicity} <- pipeline_atomicity(payload),
         :ok <- authorize_pipeline_public_keys(commands),
         :ok <- validate_pipeline_atomicity(commands, atomicity, state) do
      execute_pipeline_commands(commands, pipeline_return_format(payload), state, request_context)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp execute_compact_pipeline(mode, items, payload, state) do
    with :ok <- validate_compact_pipeline(mode, items, payload, state) do
      return_format = compact_pipeline_return_format(payload)

      case execute_compact_pipeline_fast_path(mode, items, return_format, state) do
        {:ok, result} ->
          {:ok, result, state}

        {:error, reason} ->
          {:bad_request, reason, state}

        :fallback ->
          with {:ok, commands} <- compact_pipeline_commands(mode, items) do
            execute_pipeline_commands(commands, return_format, state, %{})
          else
            {:error, reason} -> {:bad_request, reason, state}
          end
      end
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp compact_pipeline_return_format(%{"compact_values" => true}), do: :values
  defp compact_pipeline_return_format(payload), do: pipeline_return_format(payload)

  defp execute_pipeline_commands(commands, return_format, state, request_context) do
    case execute_pipeline_fast_path(commands, state) do
      {:ok, results} ->
        {:ok, format_pipeline_results(results, return_format), state}

      :fallback ->
        {results, state} =
          Enum.map_reduce(commands, state, fn command, acc_state ->
            execute_pipeline_command(command, acc_state, request_context)
          end)

        {:ok, format_pipeline_results(results, return_format), state}
    end
  end

  defp pipeline_commands(%{"commands" => commands}) when is_list(commands) do
    max = Application.get_env(:ferricstore, :native_max_pipeline_commands, 1024)

    cond do
      length(commands) > max ->
        {:error, "ERR native pipeline exceeds max commands"}

      true ->
        commands
        |> Enum.reduce_while({:ok, []}, fn
          %{"opcode" => opcode, "body" => body} = command, {:ok, acc}
          when is_integer(opcode) and is_map(body) ->
            lane_id = Map.get(command, "lane_id", 0)
            request_id = Map.get(command, "request_id", 0)
            body = maybe_prepare_pipeline_command(opcode, body)

            if control_opcode?(opcode) do
              {:halt, {:error, "ERR native pipeline cannot contain control commands"}}
            else
              {:cont,
               {:ok,
                [%{opcode: opcode, body: body, lane_id: lane_id, request_id: request_id} | acc]}}
            end

          _command, _acc ->
            {:halt, {:error, "ERR native pipeline commands require opcode and body"}}
        end)
        |> case do
          {:ok, values} -> {:ok, Enum.reverse(values)}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp pipeline_commands(_payload), do: {:error, "ERR native pipeline requires commands list"}

  defp maybe_prepare_pipeline_command(@op_command_exec, body) do
    with {:ok, command} <- require_binary(body, "command"),
         {:ok, args} <- raw_command_args(body),
         {:ok, prepared} <- Ferricstore.Commands.Dispatcher.prepare_raw(command, args) do
      Map.put(body, :__prepared_command__, prepared)
    else
      _invalid_command -> body
    end
  end

  defp maybe_prepare_pipeline_command(_opcode, body), do: body

  defp authorize_pipeline_public_keys(commands) do
    Enum.reduce_while(commands, :ok, fn command, :ok ->
      case authorize_pipeline_public_command(command) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp authorize_pipeline_public_command(%{opcode: @op_command_exec, body: body}) do
    case Map.get(body, :__prepared_command__) do
      %PreparedCommand{} = prepared ->
        InternalKey.authorize_command(prepared.command, prepared.acl_keys)

      _not_prepared ->
        with {:ok, command} <- require_binary(body, "command"),
             {:ok, args} <- raw_command_args(body),
             {:ok, prepared} <- Ferricstore.Commands.Dispatcher.prepare_raw(command, args) do
          InternalKey.authorize_command(prepared.command, prepared.acl_keys)
        else
          _parse_or_validation_error -> :ok
        end
    end
  end

  defp authorize_pipeline_public_command(%{opcode: opcode, body: body}),
    do: authorize_public_opcode(opcode, body)

  defp validate_compact_pipeline(mode, items, payload, state)
       when mode in [
              1,
              2,
              3,
              5,
              6,
              7,
              8,
              9,
              10,
              11,
              12,
              13,
              14,
              15,
              16,
              17,
              18,
              19,
              20,
              21,
              22,
              23,
              24,
              25,
              26,
              27,
              28,
              29,
              30,
              31,
              32,
              33
            ] and is_list(items) do
    max = Application.get_env(:ferricstore, :native_max_pipeline_commands, 1024)
    count = Map.get(payload, "compact_count")
    decoded_from_wire? = is_integer(count) and count >= 0
    count = if decoded_from_wire?, do: count, else: length(items)

    cond do
      count > max ->
        {:error, "ERR native pipeline exceeds max commands"}

      not decoded_from_wire? and not valid_compact_pipeline_items?(mode, items) ->
        {:error, "ERR native compact PIPELINE payload is invalid"}

      mode == 5 and not safe_compact_mixed_pipeline?(items) ->
        {:error, "ERR native compact mixed PIPELINE cannot reorder dependent keys"}

      true ->
        with :ok <- authorize_compact_pipeline_public_keys(mode, items),
             {:ok, atomicity} <- pipeline_atomicity(payload) do
          validate_compact_pipeline_atomicity(mode, items, atomicity, state)
        end
    end
  end

  defp validate_compact_pipeline(_mode, _items, _payload, _state),
    do: {:error, "ERR native compact PIPELINE payload is invalid"}

  defp valid_compact_pipeline_items?(1, items),
    do:
      Enum.all?(items, fn
        {key, value} -> is_binary(key) and is_binary(value)
        _item -> false
      end)

  defp valid_compact_pipeline_items?(2, items),
    do: Enum.all?(items, &is_binary/1)

  defp valid_compact_pipeline_items?(27, items),
    do: Enum.all?(items, &is_binary/1)

  defp valid_compact_pipeline_items?(30, items),
    do: Enum.all?(items, &is_binary/1)

  defp valid_compact_pipeline_items?(mode, items) when mode in [18, 19] do
    Enum.all?(items, fn
      {key, item} -> is_binary(key) and is_binary(item)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(28, items) do
    Enum.all?(items, fn
      {key, fields} when is_binary(key) and is_list(fields) and fields != [] ->
        Enum.all?(fields, &is_binary/1)

      _item ->
        false
    end)
  end

  defp valid_compact_pipeline_items?(29, items) do
    Enum.all?(items, fn
      {key, member} -> is_binary(key) and is_binary(member)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(20, items) do
    Enum.all?(items, fn
      {key, start, stop} -> is_binary(key) and is_integer(start) and is_integer(stop)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(21, items) do
    Enum.all?(items, fn
      {key, start, stop, with_scores} ->
        is_binary(key) and is_integer(start) and is_integer(stop) and is_boolean(with_scores)

      _item ->
        false
    end)
  end

  defp valid_compact_pipeline_items?(22, items) do
    Enum.all?(items, fn
      {key, field, value} -> is_binary(key) and is_binary(field) and is_binary(value)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(mode, items) when mode in [23, 24, 25, 31, 32] do
    Enum.all?(items, fn
      {key, item} -> is_binary(key) and is_binary(item)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(26, items) do
    Enum.all?(items, fn
      {key, score, member} -> is_binary(key) and is_float(score) and is_binary(member)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(5, items) do
    Enum.all?(items, fn
      {:set, key, value} -> is_binary(key) and is_binary(value)
      {:get, key} -> is_binary(key)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(mode, items) when mode in [6, 33] do
    Enum.all?(items, fn
      {:flow_step_continue, id, lease_token, from_state, to_state, opts} ->
        is_binary(id) and is_binary(lease_token) and is_binary(from_state) and
          is_binary(to_state) and is_list(opts)

      _item ->
        false
    end)
  end

  defp valid_compact_pipeline_items?(7, items) do
    Enum.all?(items, fn
      {value, opts} -> is_binary(value) and is_list(opts)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(8, items) do
    Enum.all?(items, fn
      {:flow_named_value_put, value, opts} -> is_binary(value) and is_list(opts)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(14, items), do: valid_compact_pipeline_items?(8, items)

  defp valid_compact_pipeline_items?(15, items), do: valid_compact_pipeline_items?(7, items)

  defp valid_compact_pipeline_items?(9, items) do
    Enum.all?(items, fn
      {:flow_get, id, opts} -> is_binary(id) and opts == []
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(16, items) do
    Enum.all?(items, fn
      {:flow_get, id, opts} -> is_binary(id) and valid_flow_get_partition_opts?(opts)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(17, items) do
    Enum.all?(items, fn
      {:flow_get, id, opts} -> is_binary(id) and valid_flow_get_meta_opts?(opts)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(10, items) do
    Enum.all?(items, fn
      {:flow_history, id, opts} -> is_binary(id) and is_list(opts)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(11, items) do
    Enum.all?(items, fn
      {:flow_signal, id, opts} -> is_binary(id) and is_list(opts)
      _item -> false
    end)
  end

  defp valid_compact_pipeline_items?(mode, items) when mode in [12, 13] do
    Enum.all?(items, fn
      {:flow_start_and_claim, id, type, initial_state, opts} ->
        is_binary(id) and is_binary(type) and is_binary(initial_state) and is_list(opts)

      _item ->
        false
    end)
  end

  defp valid_flow_get_partition_opts?(opts) when is_list(opts) do
    Keyword.keyword?(opts) and
      Enum.all?(opts, fn
        {:partition_key, value} -> is_binary(value)
        _other -> false
      end)
  end

  defp valid_flow_get_partition_opts?(_opts), do: false

  defp valid_flow_get_meta_opts?(opts) when is_list(opts) do
    Keyword.keyword?(opts) and Keyword.get(opts, :return) == :meta and
      opts
      |> Keyword.delete(:return)
      |> valid_flow_get_partition_opts?()
  end

  defp valid_flow_get_meta_opts?(_opts), do: false

  defp authorize_compact_pipeline_public_keys(mode, items)
       when mode in [1, 2, 5, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32] do
    mode
    |> compact_pipeline_keys(items)
    |> InternalKey.authorize_public()
  end

  defp authorize_compact_pipeline_public_keys(_mode, _items), do: :ok

  defp safe_compact_mixed_pipeline?(items),
    do: safe_compact_mixed_pipeline?(items, MapSet.new(), MapSet.new())

  defp safe_compact_mixed_pipeline?([], _read_keys, _written_keys), do: true

  defp safe_compact_mixed_pipeline?([{:set, key, _value} | rest], read_keys, written_keys) do
    not MapSet.member?(read_keys, key) and not MapSet.member?(written_keys, key) and
      safe_compact_mixed_pipeline?(rest, read_keys, MapSet.put(written_keys, key))
  end

  defp safe_compact_mixed_pipeline?([{:get, key} | rest], read_keys, written_keys) do
    not MapSet.member?(written_keys, key) and
      safe_compact_mixed_pipeline?(rest, MapSet.put(read_keys, key), written_keys)
  end

  defp safe_compact_mixed_pipeline?(_items, _read_keys, _written_keys), do: false

  defp validate_compact_pipeline_atomicity(_mode, _items, atomicity, _state)
       when atomicity in ["none", "per_shard"],
       do: :ok

  defp validate_compact_pipeline_atomicity(mode, items, "same_shard", state) do
    shards =
      mode
      |> compact_pipeline_keys(items)
      |> Enum.map(&Router.shard_for(state.instance_ctx, &1))
      |> Enum.uniq()

    case shards do
      [] -> :ok
      [_one] -> :ok
      _ -> {:error, "ERR native same_shard pipeline contains multiple shards"}
    end
  end

  defp compact_pipeline_keys(1, kv_pairs), do: Enum.map(kv_pairs, fn {key, _value} -> key end)
  defp compact_pipeline_keys(2, keys), do: keys
  defp compact_pipeline_keys(27, keys), do: keys
  defp compact_pipeline_keys(30, keys), do: keys
  defp compact_pipeline_keys(mode, items) when mode in [18, 19], do: Enum.map(items, &elem(&1, 0))
  defp compact_pipeline_keys(mode, items) when mode in [28, 29], do: Enum.map(items, &elem(&1, 0))
  defp compact_pipeline_keys(20, items), do: Enum.map(items, &elem(&1, 0))
  defp compact_pipeline_keys(21, items), do: Enum.map(items, &elem(&1, 0))
  defp compact_pipeline_keys(22, items), do: Enum.map(items, &elem(&1, 0))

  defp compact_pipeline_keys(mode, items) when mode in [23, 24, 25, 26, 31, 32],
    do: Enum.map(items, &elem(&1, 0))

  defp compact_pipeline_keys(5, ops), do: Enum.map(ops, &compact_mixed_key/1)

  defp compact_pipeline_keys(mode, ops) when mode in [6, 33],
    do:
      Enum.map(ops, fn {:flow_step_continue, id, _lease_token, _from_state, _to_state, _opts} ->
        id
      end)

  defp compact_pipeline_keys(7, _ops), do: []
  defp compact_pipeline_keys(15, _ops), do: []

  defp compact_pipeline_keys(8, ops) do
    Enum.flat_map(ops, fn {:flow_named_value_put, _value, opts} ->
      case Keyword.fetch(opts, :owner_flow_id) do
        {:ok, owner_flow_id} -> [owner_flow_id]
        :error -> []
      end
    end)
  end

  defp compact_pipeline_keys(14, ops), do: compact_pipeline_keys(8, ops)

  defp compact_pipeline_keys(9, ops),
    do: Enum.map(ops, fn {:flow_get, id, []} -> id end)

  defp compact_pipeline_keys(16, ops),
    do: Enum.map(ops, fn {:flow_get, id, _opts} -> id end)

  defp compact_pipeline_keys(17, ops),
    do: Enum.map(ops, fn {:flow_get, id, _opts} -> id end)

  defp compact_pipeline_keys(10, ops),
    do: Enum.map(ops, fn {:flow_history, id, _opts} -> id end)

  defp compact_pipeline_keys(11, ops),
    do: Enum.map(ops, fn {:flow_signal, id, _opts} -> id end)

  defp compact_pipeline_keys(mode, ops) when mode in [12, 13],
    do: Enum.map(ops, fn {:flow_start_and_claim, id, _type, _initial_state, _opts} -> id end)

  defp compact_mixed_key({:set, key, _value}), do: key
  defp compact_mixed_key({_op, key}), do: key

  defp compact_pipeline_commands(1, kv_pairs) do
    commands =
      kv_pairs
      |> Enum.with_index(1)
      |> Enum.map(fn {{key, value}, request_id} ->
        %{
          opcode: @op_set,
          lane_id: 1,
          request_id: request_id,
          body: %{"key" => key, "value" => value}
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(2, keys) do
    commands =
      keys
      |> Enum.with_index(1)
      |> Enum.map(fn {key, request_id} ->
        %{opcode: @op_get, lane_id: 1, request_id: request_id, body: %{"key" => key}}
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(27, keys) do
    commands =
      keys
      |> Enum.with_index(1)
      |> Enum.map(fn {key, request_id} ->
        %{opcode: @op_smembers, lane_id: 1, request_id: request_id, body: %{"key" => key}}
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(30, keys) do
    commands =
      keys
      |> Enum.with_index(1)
      |> Enum.map(fn {key, request_id} ->
        %{opcode: @op_hgetall, lane_id: 1, request_id: request_id, body: %{"key" => key}}
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(18, items) do
    commands =
      items
      |> Enum.with_index(1)
      |> Enum.map(fn {{key, field}, request_id} ->
        %{
          opcode: @op_hget,
          lane_id: 1,
          request_id: request_id,
          body: %{"key" => key, "field" => field}
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(19, items) do
    commands =
      items
      |> Enum.with_index(1)
      |> Enum.map(fn {{key, member}, request_id} ->
        %{
          opcode: @op_sismember,
          lane_id: 1,
          request_id: request_id,
          body: %{"key" => key, "member" => member}
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(28, items) do
    commands =
      items
      |> Enum.with_index(1)
      |> Enum.map(fn {{key, fields}, request_id} ->
        %{
          opcode: @op_hmget,
          lane_id: 1,
          request_id: request_id,
          body: %{"key" => key, "fields" => fields}
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(29, items) do
    commands =
      items
      |> Enum.with_index(1)
      |> Enum.map(fn {{key, member}, request_id} ->
        %{
          opcode: @op_zscore,
          lane_id: 1,
          request_id: request_id,
          body: %{"key" => key, "member" => member}
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(20, items) do
    commands =
      items
      |> Enum.with_index(1)
      |> Enum.map(fn {{key, start, stop}, request_id} ->
        %{
          opcode: @op_lrange,
          lane_id: 1,
          request_id: request_id,
          body: %{"key" => key, "start" => start, "stop" => stop}
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(21, items) do
    commands =
      items
      |> Enum.with_index(1)
      |> Enum.map(fn {{key, start, stop, with_scores}, request_id} ->
        body = %{"key" => key, "start" => start, "stop" => stop}
        body = if with_scores, do: Map.put(body, "withscores", true), else: body

        %{
          opcode: @op_zrange,
          lane_id: 1,
          request_id: request_id,
          body: body
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(22, items) do
    commands =
      items
      |> Enum.with_index(1)
      |> Enum.map(fn {{key, field, value}, request_id} ->
        %{
          opcode: @op_hset,
          lane_id: 1,
          request_id: request_id,
          body: %{"key" => key, "fields" => %{field => value}}
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(mode, items) when mode in [23, 24] do
    opcode = if mode == 23, do: @op_lpush, else: @op_rpush

    commands =
      items
      |> Enum.with_index(1)
      |> Enum.map(fn {{key, value}, request_id} ->
        %{
          opcode: opcode,
          lane_id: 1,
          request_id: request_id,
          body: %{"key" => key, "values" => [value]}
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(25, items) do
    commands =
      items
      |> Enum.with_index(1)
      |> Enum.map(fn {{key, member}, request_id} ->
        %{
          opcode: @op_sadd,
          lane_id: 1,
          request_id: request_id,
          body: %{"key" => key, "members" => [member]}
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(26, items) do
    commands =
      items
      |> Enum.with_index(1)
      |> Enum.map(fn {{key, score, member}, request_id} ->
        %{
          opcode: @op_zadd,
          lane_id: 1,
          request_id: request_id,
          body: %{"key" => key, "items" => [[score, member]]}
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(5, ops) do
    commands =
      ops
      |> Enum.with_index(1)
      |> Enum.map(fn
        {{:set, key, value}, request_id} ->
          %{
            opcode: @op_set,
            lane_id: 1,
            request_id: request_id,
            body: %{"key" => key, "value" => value}
          }

        {{:get, key}, request_id} ->
          %{opcode: @op_get, lane_id: 1, request_id: request_id, body: %{"key" => key}}
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(mode, ops) when mode in [6, 33] do
    commands =
      ops
      |> Enum.with_index(1)
      |> Enum.map(fn
        {{:flow_step_continue, id, lease_token, from_state, to_state, opts}, request_id} ->
          %{
            opcode: @op_flow_step_continue,
            lane_id: 1,
            request_id: request_id,
            body:
              opts
              |> compact_flow_opts_body()
              |> Map.put("id", id)
              |> Map.put("lease_token", lease_token)
              |> Map.put("from_state", from_state)
              |> Map.put("to_state", to_state)
          }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(7, items) do
    commands =
      items
      |> Enum.with_index(1)
      |> Enum.map(fn {{value, opts}, request_id} ->
        %{
          opcode: @op_flow_value_put,
          lane_id: 1,
          request_id: request_id,
          body: opts |> compact_flow_opts_body() |> Map.put("value", value)
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(15, items), do: compact_pipeline_commands(7, items)

  defp compact_pipeline_commands(8, ops) do
    commands =
      ops
      |> Enum.with_index(1)
      |> Enum.map(fn {{:flow_named_value_put, value, opts}, request_id} ->
        %{
          opcode: @op_flow_value_put,
          lane_id: 1,
          request_id: request_id,
          body: opts |> compact_flow_opts_body() |> Map.put("value", value)
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(14, ops), do: compact_pipeline_commands(8, ops)

  defp compact_pipeline_commands(9, ops) do
    commands =
      ops
      |> Enum.with_index(1)
      |> Enum.map(fn {{:flow_get, id, []}, request_id} ->
        %{
          opcode: @op_flow_get,
          lane_id: 1,
          request_id: request_id,
          body: %{"id" => id}
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(16, ops) do
    commands =
      ops
      |> Enum.with_index(1)
      |> Enum.map(fn {{:flow_get, id, opts}, request_id} ->
        %{
          opcode: @op_flow_get,
          lane_id: 1,
          request_id: request_id,
          body: opts |> compact_flow_opts_body() |> Map.put("id", id)
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(17, ops) do
    commands =
      ops
      |> Enum.with_index(1)
      |> Enum.map(fn {{:flow_get, id, opts}, request_id} ->
        %{
          opcode: @op_flow_get,
          lane_id: 1,
          request_id: request_id,
          body:
            opts
            |> Keyword.delete(:return)
            |> compact_flow_opts_body()
            |> Map.put("id", id)
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(10, ops) do
    commands =
      ops
      |> Enum.with_index(1)
      |> Enum.map(fn {{:flow_history, id, opts}, request_id} ->
        %{
          opcode: @op_flow_history,
          lane_id: 1,
          request_id: request_id,
          body: opts |> compact_flow_opts_body() |> Map.put("id", id)
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(11, ops) do
    commands =
      ops
      |> Enum.with_index(1)
      |> Enum.map(fn {{:flow_signal, id, opts}, request_id} ->
        %{
          opcode: @op_flow_signal,
          lane_id: 1,
          request_id: request_id,
          body: opts |> compact_flow_opts_body() |> Map.put("id", id)
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(mode, ops) when mode in [12, 13] do
    commands =
      ops
      |> Enum.with_index(1)
      |> Enum.map(fn {{:flow_start_and_claim, id, type, initial_state, opts}, request_id} ->
        %{
          opcode: @op_flow_start_and_claim,
          lane_id: 1,
          request_id: request_id,
          body:
            opts
            |> compact_flow_opts_body()
            |> Map.put("id", id)
            |> Map.put("type", type)
            |> Map.put("initial_state", initial_state)
        }
      end)

    {:ok, commands}
  end

  defp compact_pipeline_commands(_mode, _items),
    do: {:error, "ERR native compact PIPELINE payload is invalid"}

  defp compact_flow_opts_body(opts) do
    Map.new(opts, fn {key, value} -> {Atom.to_string(key), value} end)
  end

  defp pipeline_atomicity(payload) do
    atomicity = Map.get(payload, "atomicity", "none")

    if atomicity in @supported_atomicity do
      {:ok, atomicity}
    else
      {:error, "ERR native unsupported pipeline atomicity #{atomicity}"}
    end
  end

  defp pipeline_return_format(%{"return" => "pairs"}), do: :pairs
  defp pipeline_return_format(%{"return" => "compact"}), do: :compact
  defp pipeline_return_format(_payload), do: :maps

  defp format_pipeline_results(results, :maps), do: results

  defp format_pipeline_results(results, :pairs) do
    Enum.map(results, fn %{"status" => status, "value" => value} -> [status, value] end)
  end

  defp format_pipeline_results(results, :compact) do
    pairs = format_pipeline_results(results, :pairs)

    case FerricstoreServer.Native.Codec.encode_compact_pipeline_response(pairs) do
      payload when is_binary(payload) -> payload
      nil -> pairs
    end
  end

  defp validate_pipeline_atomicity(_commands, atomicity, _state)
       when atomicity in ["none", "per_shard"],
       do: :ok

  defp validate_pipeline_atomicity(commands, "same_shard", state) do
    if Enum.any?(commands, &coordinated_pipeline_command?/1) do
      {:error, "ERR native same_shard pipeline contains coordinated command"}
    else
      shards =
        commands
        |> Enum.flat_map(&pipeline_routing_keys/1)
        |> Enum.map(&Router.shard_for(state.instance_ctx, &1))
        |> Enum.uniq()

      case shards do
        [] -> :ok
        [_one] -> :ok
        _ -> {:error, "ERR native same_shard pipeline contains multiple shards"}
      end
    end
  end

  defp coordinated_pipeline_command?(%{
         opcode: @op_command_exec,
         body: %{__prepared_command__: %PreparedCommand{routing_scope: :coordinated}}
       }),
       do: true

  defp coordinated_pipeline_command?(%{opcode: opcode}) when is_integer(opcode),
    do: Map.has_key?(@flow_commands, opcode)

  defp coordinated_pipeline_command?(_command), do: false

  defp pipeline_routing_keys(%{
         opcode: @op_command_exec,
         body: %{__prepared_command__: %PreparedCommand{routing_keys: keys}}
       }),
       do: keys

  defp pipeline_routing_keys(command), do: keys(command.opcode, command.body)

  defp execute_pipeline_fast_path(
         commands,
         %{
           acl_cache: :full_access,
           require_auth: false,
           instance_ctx: ctx
         } = state
       )
       when not is_nil(ctx) do
    case pipeline_plain_sets(commands, [], []) do
      {:ok, requests, kv_pairs} ->
        Stats.incr_commands_by(state.stats_counter, length(commands))

        results =
          ctx
          |> Router.batch_quorum_put(kv_pairs)
          |> pipeline_set_results(requests)

        {:ok, results}

      :fallback ->
        case pipeline_plain_gets(commands, [], []) do
          {:ok, requests, keys} ->
            Stats.incr_commands_by(state.stats_counter, length(commands))

            results =
              ctx
              |> Router.batch_get(keys)
              |> pipeline_get_results(requests)

            {:ok, results}

          :fallback ->
            case pipeline_data_writes(commands) do
              {:ok, requests, ops} ->
                Stats.incr_commands_by(state.stats_counter, length(commands))

                results =
                  ctx
                  |> Router.pipeline_write_batch(ops)
                  |> pipeline_flow_results(requests)

                {:ok, results}

              :fallback ->
                case pipeline_flow_create_many(commands, [], [], nil) do
                  {:ok, requests, items, opts} ->
                    Stats.incr_commands_by(state.stats_counter, length(commands))

                    results =
                      state.instance_ctx
                      |> Ferricstore.Flow.create_many(
                        nil,
                        items,
                        opts
                        |> Keyword.put(:independent, true)
                        |> Keyword.put(:return, :ok_on_success)
                      )
                      |> pipeline_create_many_results(requests)

                    {:ok, results}

                  :fallback ->
                    case pipeline_flow_shared_value_puts(commands, [], []) do
                      {:ok, requests, items} ->
                        Stats.incr_commands_by(state.stats_counter, length(commands))

                        results =
                          state.instance_ctx
                          |> Ferricstore.Flow.ValueStore.shared_value_put_batch(items)
                          |> pipeline_flow_results(requests)

                        {:ok, results}

                      :fallback ->
                        case pipeline_flow_writes(commands, [], []) do
                          {:ok, requests, ops} ->
                            Stats.incr_commands_by(state.stats_counter, length(commands))

                            results =
                              state.instance_ctx
                              |> Ferricstore.Flow.pipeline_write_batch_cross_shard_safe(ops)
                              |> pipeline_flow_results(requests)

                            {:ok, results}

                          :fallback ->
                            case pipeline_flow_reads(commands, [], []) do
                              {:ok, requests, ops} ->
                                Stats.incr_commands_by(state.stats_counter, length(commands))

                                results =
                                  state.instance_ctx
                                  |> Ferricstore.Flow.pipeline_read_batch(ops)
                                  |> pipeline_flow_results(requests)

                                {:ok, results}

                              :fallback ->
                                :fallback
                            end
                        end
                    end
                end
            end
        end
    end
  end

  defp execute_pipeline_fast_path(_commands, _state), do: :fallback

  defp execute_compact_pipeline_fast_path(
         1,
         kv_pairs,
         :values,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(kv_pairs) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(kv_pairs))

    case Router.batch_quorum_put_status(ctx, kv_pairs) do
      :ok ->
        {:ok, FerricstoreServer.Native.Codec.encode_compact_ok_count(length(kv_pairs))}

      {:error, _reason} = error ->
        {:ok, format_compact_set_results([error], :values)}
    end
  end

  defp execute_compact_pipeline_fast_path(
         1,
         kv_pairs,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(kv_pairs) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(kv_pairs))

    results = Router.batch_quorum_put(ctx, kv_pairs)

    {:ok, format_compact_set_results(results, return_format)}
  end

  defp execute_compact_pipeline_fast_path(
         mode,
         keys,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when mode == 2 and is_list(keys) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(keys))

    pairs =
      ctx
      |> Router.batch_get(keys)
      |> compact_pipeline_get_result(return_format)

    {:ok, format_compact_pipeline_get_result(pairs, @op_get, return_format)}
  end

  defp execute_compact_pipeline_fast_path(
         18,
         items,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(items) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(items))

    pairs =
      ctx
      |> compact_hget_results(items)

    {:ok, format_compact_pipeline_results(pairs, @op_hget, return_format)}
  end

  defp execute_compact_pipeline_fast_path(
         19,
         items,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(items) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(items))

    pairs =
      ctx
      |> compact_sismember_results(items)

    {:ok, format_compact_pipeline_results(pairs, @op_sismember, return_format)}
  end

  defp execute_compact_pipeline_fast_path(
         28,
         items,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(items) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(items))

    pairs =
      ctx
      |> compact_hmget_results(items)

    {:ok, format_compact_pipeline_results(pairs, @op_hmget, return_format)}
  end

  defp execute_compact_pipeline_fast_path(
         29,
         items,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(items) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(items))

    pairs =
      ctx
      |> compact_zscore_results(items)

    {:ok, format_compact_pipeline_results(pairs, @op_zscore, return_format)}
  end

  defp execute_compact_pipeline_fast_path(
         20,
         items,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(items) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(items))

    with {:ok, results} <- compact_lrange_results(ctx, items, state) do
      {:ok, format_compact_pipeline_results(results, @op_lrange, return_format)}
    end
  end

  defp execute_compact_pipeline_fast_path(
         21,
         items,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(items) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(items))

    with {:ok, results} <- compact_zrange_results(ctx, items, state) do
      {:ok, format_compact_pipeline_results(results, @op_zrange, return_format)}
    end
  end

  defp execute_compact_pipeline_fast_path(
         27,
         keys,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(keys) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(keys))

    with {:ok, results} <- guarded_collection_map(keys, state, &compact_smembers_result(ctx, &1)) do
      {:ok, format_compact_pipeline_results(results, @op_smembers, return_format)}
    end
  end

  defp execute_compact_pipeline_fast_path(
         30,
         keys,
         :compact,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(keys) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(keys))

    with {:ok, results} <-
           guarded_collection_map(keys, state, &compact_hgetall_entry_result(ctx, &1)) do
      case compact_hgetall_entry_values_payload(results) do
        payload when is_binary(payload) ->
          {:ok, payload}

        nil ->
          pairs =
            results
            |> Enum.map(&compact_hgetall_entry_result_to_map_result/1)
            |> compact_pipeline_result_pairs()

          {:ok, format_compact_pipeline_pairs(pairs, @op_hgetall, :compact)}
      end
    end
  end

  defp execute_compact_pipeline_fast_path(
         30,
         keys,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(keys) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(keys))

    with {:ok, results} <- guarded_collection_map(keys, state, &compact_hgetall_result(ctx, &1)) do
      {:ok, format_compact_pipeline_results(results, @op_hgetall, return_format)}
    end
  end

  defp execute_compact_pipeline_fast_path(
         mode,
         items,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when mode in [23, 24] and is_list(items) and not is_nil(ctx) do
    opcode = if mode == 23, do: @op_lpush, else: @op_rpush
    push_fun = if mode == 23, do: &FerricStore.Impl.lpush/3, else: &FerricStore.Impl.rpush/3

    case compact_list_push_grouped_results(ctx, items, push_fun) do
      :fallback ->
        execute_compact_data_write_pipeline(mode, items, return_format, state)

      results ->
        Stats.incr_commands_by(state.stats_counter, length(items))

        {:ok, format_compact_pipeline_results(results, opcode, return_format)}
    end
  end

  defp execute_compact_pipeline_fast_path(
         mode,
         items,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when mode in [22, 23, 24, 25, 26, 31, 32] and is_list(items) and not is_nil(ctx) do
    execute_compact_data_write_pipeline(mode, items, return_format, state)
  end

  defp execute_compact_pipeline_fast_path(
         5,
         ops,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(ops) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(ops))
    {get_keys, set_pairs} = compact_mixed_collect(ops, [], [])
    get_values = Router.batch_get(ctx, Enum.reverse(get_keys))
    set_results = Router.batch_quorum_put(ctx, Enum.reverse(set_pairs))

    pairs =
      compact_mixed_pairs(
        ops,
        get_values,
        Enum.map(set_results, &pipeline_set_result_pair/1),
        []
      )

    {:ok, format_compact_pipeline_pairs(pairs, @op_pipeline, return_format)}
  end

  defp execute_compact_pipeline_fast_path(
         mode,
         ops,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when mode in [6, 33] and is_list(ops) and not is_nil(ctx) do
    count = length(ops)
    Stats.incr_commands_by(state.stats_counter, count)
    requests = compact_pipeline_requests(@op_flow_step_continue, count)

    results =
      ctx
      |> Ferricstore.Flow.pipeline_write_batch_cross_shard_safe(ops)
      |> compact_pipeline_flow_pairs(requests)

    {:ok, format_compact_pipeline_pairs(results, @op_pipeline, return_format)}
  end

  defp execute_compact_pipeline_fast_path(
         mode,
         items,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when mode in [7, 15] and is_list(items) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(items))
    requests = compact_pipeline_requests(@op_flow_value_put, length(items))

    results =
      ctx
      |> Ferricstore.Flow.ValueStore.shared_value_put_batch(items)
      |> compact_pipeline_flow_pairs(requests)

    {:ok, format_compact_pipeline_pairs(results, @op_pipeline, return_format)}
  end

  defp execute_compact_pipeline_fast_path(
         mode,
         ops,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when mode in [8, 14] and is_list(ops) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(ops))
    requests = compact_pipeline_requests(@op_flow_value_put, length(ops))

    results =
      ctx
      |> Ferricstore.Flow.pipeline_write_batch_cross_shard_safe(ops)
      |> compact_pipeline_flow_pairs(requests)

    {:ok, format_compact_pipeline_pairs(results, @op_pipeline, return_format)}
  end

  defp execute_compact_pipeline_fast_path(
         11,
         ops,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(ops) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(ops))
    requests = compact_pipeline_requests(@op_flow_signal, length(ops))

    results =
      ctx
      |> Ferricstore.Flow.pipeline_write_batch_cross_shard_safe(ops)
      |> compact_pipeline_flow_pairs(requests)

    {:ok, format_compact_pipeline_pairs(results, @op_pipeline, return_format)}
  end

  defp execute_compact_pipeline_fast_path(
         mode,
         ops,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when mode in [9, 16, 17] and is_list(ops) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(ops))

    results =
      ctx
      |> compact_flow_get_results(mode, ops)
      |> maybe_compact_flow_get_meta_results(mode)

    payload =
      case return_format do
        :values ->
          compact_pipeline_flow_values(results)

        _other ->
          requests = compact_pipeline_requests(@op_flow_get, length(ops))

          results
          |> compact_pipeline_flow_pairs(requests)
          |> format_compact_pipeline_pairs(@op_pipeline, return_format)
      end

    {:ok, payload}
  end

  defp execute_compact_pipeline_fast_path(
         10,
         ops,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when is_list(ops) and not is_nil(ctx) do
    Stats.incr_commands_by(state.stats_counter, length(ops))
    requests = compact_pipeline_requests(@op_flow_history, length(ops))

    results =
      ctx
      |> Ferricstore.Flow.pipeline_read_batch(ops)
      |> compact_pipeline_flow_pairs(requests)

    {:ok, format_compact_pipeline_pairs(results, @op_pipeline, return_format)}
  end

  defp execute_compact_pipeline_fast_path(
         mode,
         ops,
         return_format,
         %{acl_cache: :full_access, require_auth: false, instance_ctx: ctx} = state
       )
       when mode in [12, 13] and is_list(ops) and not is_nil(ctx) do
    count = length(ops)
    Stats.incr_commands_by(state.stats_counter, count)
    requests = compact_pipeline_requests(@op_flow_start_and_claim, count)

    results =
      ctx
      |> Ferricstore.Flow.pipeline_write_batch_cross_shard_safe(ops)
      |> compact_pipeline_flow_pairs(requests)

    {:ok, format_compact_pipeline_pairs(results, @op_pipeline, return_format)}
  end

  defp execute_compact_pipeline_fast_path(_mode, _items, _return_format, _state), do: :fallback

  defp compact_hget_results(ctx, items) do
    lookups =
      Enum.flat_map(items, fn {key, field} ->
        [
          {key, Ferricstore.Store.CompoundKey.type_key(key)},
          {key, Ferricstore.Store.CompoundKey.hash_field(key, field)}
        ]
      end)

    values = Router.batch_get_on_route_keys(ctx, lookups)

    {results, plain_checks} =
      items
      |> Enum.zip(Enum.chunk_every(values, 2))
      |> Enum.with_index()
      |> Enum.map_reduce([], fn {{{key, _field}, [type, field_value]}, index}, checks ->
        case type do
          "hash" ->
            {{:ok, field_value}, checks}

          nil ->
            {{:check_plain, index}, [{index, {key, key}} | checks]}

          _other_type ->
            {{:error, "WRONGTYPE Operation against a key holding the wrong kind of value"},
             checks}
        end
      end)

    fill_compact_missing_type_results(ctx, results, plain_checks, nil_value: nil)
  end

  defp compact_hmget_results(ctx, items) do
    if Enum.all?(items, fn
         {_key, [_field]} -> true
         _other -> false
       end) do
      hget_items = Enum.map(items, fn {key, [field]} -> {key, field} end)

      ctx
      |> compact_hget_results(hget_items)
      |> Enum.map(fn
        {:ok, value} -> {:ok, [value]}
        {:error, _reason} = error -> error
      end)
    else
      Enum.map(items, fn {key, fields} -> compact_hmget_result(ctx, key, fields) end)
    end
  end

  defp compact_sismember_results(ctx, items) do
    lookups =
      Enum.flat_map(items, fn {key, member} ->
        [
          {key, Ferricstore.Store.CompoundKey.type_key(key)},
          {key, Ferricstore.Store.CompoundKey.set_member(key, member)}
        ]
      end)

    values = Router.batch_get_on_route_keys(ctx, lookups)

    {results, plain_checks} =
      items
      |> Enum.zip(Enum.chunk_every(values, 2))
      |> Enum.with_index()
      |> Enum.map_reduce([], fn {{{key, _member}, [type, member_value]}, index}, checks ->
        case type do
          "set" ->
            {{:ok, member_value != nil}, checks}

          nil ->
            {{:check_plain, index}, [{index, {key, key}} | checks]}

          _other_type ->
            {{:error, "WRONGTYPE Operation against a key holding the wrong kind of value"},
             checks}
        end
      end)

    fill_compact_missing_type_results(ctx, results, plain_checks, nil_value: false)
  end

  defp compact_zscore_results(ctx, items) do
    lookups =
      Enum.flat_map(items, fn {key, member} ->
        [
          {key, Ferricstore.Store.CompoundKey.type_key(key)},
          {key, Ferricstore.Store.CompoundKey.zset_member(key, member)}
        ]
      end)

    values = Router.batch_get_on_route_keys(ctx, lookups)

    {results, plain_checks} =
      items
      |> Enum.zip(Enum.chunk_every(values, 2))
      |> Enum.with_index()
      |> Enum.map_reduce([], fn {{{key, _member}, [type, score]}, index}, checks ->
        case type do
          "zset" ->
            {{:ok, score}, checks}

          nil ->
            {{:check_plain, index}, [{index, {key, key}} | checks]}

          _other_type ->
            {{:error, "WRONGTYPE Operation against a key holding the wrong kind of value"},
             checks}
        end
      end)

    fill_compact_missing_type_results(ctx, results, plain_checks, nil_value: nil)
  end

  defp fill_compact_missing_type_results(_ctx, results, [], _opts), do: results

  defp fill_compact_missing_type_results(ctx, results, plain_checks, opts) do
    plain_checks = Enum.reverse(plain_checks)

    plain_values =
      Router.batch_get_on_route_keys(ctx, Enum.map(plain_checks, fn {_index, pair} -> pair end))

    nil_value = Keyword.fetch!(opts, :nil_value)
    result_tuple = List.to_tuple(results)

    plain_checks
    |> Enum.zip(plain_values)
    |> Enum.reduce(result_tuple, fn {{index, _pair}, value}, acc ->
      result =
        if value == nil,
          do: {:ok, nil_value},
          else: {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

      put_elem(acc, index, result)
    end)
    |> Tuple.to_list()
  end

  defp execute_compact_data_write_pipeline(mode, items, return_format, state) do
    ctx = state.instance_ctx
    {opcode, ops} = compact_data_write_ops(mode, items)
    Stats.incr_commands_by(state.stats_counter, length(ops))

    results =
      ctx
      |> Router.pipeline_write_batch(ops)

    case return_format do
      :values ->
        {:ok, compact_pipeline_flow_values(results)}

      _ ->
        requests = compact_pipeline_requests(opcode, length(ops))

        results =
          results
          |> compact_pipeline_flow_pairs(requests)

        {:ok, format_compact_pipeline_pairs(results, @op_pipeline, return_format)}
    end
  end

  defp compact_data_write_ops(22, items) do
    {@op_hset,
     Enum.map(items, fn {key, field, value} -> {key, {:hset_single, key, field, value}} end)}
  end

  defp compact_data_write_ops(23, items) do
    {@op_lpush, Enum.map(items, fn {key, value} -> {key, {:lpush_single, key, value}} end)}
  end

  defp compact_data_write_ops(24, items) do
    {@op_rpush, Enum.map(items, fn {key, value} -> {key, {:rpush_single, key, value}} end)}
  end

  defp compact_data_write_ops(25, items) do
    {@op_sadd, Enum.map(items, fn {key, member} -> {key, {:sadd_single, key, member}} end)}
  end

  defp compact_data_write_ops(26, items) do
    {@op_zadd,
     Enum.map(items, fn {key, score, member} ->
       {key, {:zadd_single, key, score, member}}
     end)}
  end

  defp compact_data_write_ops(31, items) do
    {@op_srem, Enum.map(items, fn {key, member} -> {key, {:srem_single, key, member}} end)}
  end

  defp compact_data_write_ops(32, items) do
    {@op_zrem, Enum.map(items, fn {key, member} -> {key, {:zrem_single, key, member}} end)}
  end

  defp compact_list_push_grouped_results(_ctx, [], _push_fun), do: []

  defp compact_list_push_grouped_results(ctx, items, push_fun) do
    case compact_list_push_single_key_items(items) do
      :fallback ->
        :fallback

      {key, indices, values} ->
        indices = Enum.reverse(indices)
        values = Enum.reverse(values)

        push_fun.(ctx, key, values)
        |> compact_list_push_group_result(indices, length(values))
        |> compact_list_push_results(length(items))
    end
  end

  defp compact_list_push_single_key_items([{key, value} | rest]) do
    compact_list_push_single_key_items(rest, key, [0], [value], 1)
  end

  defp compact_list_push_single_key_items([], key, indices, values, _index),
    do: {key, indices, values}

  defp compact_list_push_single_key_items([{key, value} | rest], key, indices, values, index) do
    compact_list_push_single_key_items(rest, key, [index | indices], [value | values], index + 1)
  end

  defp compact_list_push_single_key_items(
         [{other_key, _value} | _rest],
         key,
         _indices,
         _values,
         _index
       )
       when other_key != key,
       do: :fallback

  defp compact_list_push_group_result({:ok, final_count}, indices, count)
       when is_integer(final_count) do
    base_count = final_count - count

    indices
    |> Enum.with_index(1)
    |> Map.new(fn {index, offset} -> {index, {:ok, base_count + offset}} end)
  end

  defp compact_list_push_group_result(result, indices, _count) do
    Map.new(indices, fn index -> {index, result} end)
  end

  defp compact_list_push_results(result_map, count) do
    Enum.map(0..(count - 1), &Map.fetch!(result_map, &1))
  end

  defp compact_smembers_result(ctx, key) do
    case Router.compound_get(ctx, key, CompoundKey.type_key(key)) do
      nil ->
        []

      "set" ->
        prefix = CompoundKey.set_prefix(key)

        ctx
        |> Router.compound_scan_raw(key, prefix)
        |> Enum.map(fn {member, _value} -> member end)

      _other_type ->
        @wrongtype_error
    end
  end

  defp compact_hmget_result(ctx, key, [field]) do
    case FerricStore.Impl.hget(ctx, key, field) do
      {:ok, value} -> {:ok, [value]}
      {:error, _reason} = error -> error
    end
  end

  defp compact_hmget_result(ctx, key, fields) do
    FerricStore.Impl.hmget(ctx, key, fields)
  end

  defp compact_hgetall_result(ctx, key) do
    case compact_hgetall_entry_result(ctx, key) do
      {:ok, entries} -> Map.new(entries)
      {:error, _reason} = error -> error
    end
  end

  defp compact_hgetall_entry_result(ctx, key) do
    case Router.compound_get(ctx, key, CompoundKey.type_key(key)) do
      nil ->
        {:ok, []}

      "hash" ->
        prefix = CompoundKey.hash_prefix(key)
        {:ok, Router.compound_scan_raw(ctx, key, prefix)}

      _other_type ->
        @wrongtype_error
    end
  end

  defp compact_hgetall_entry_values_payload(results) do
    values =
      Enum.reduce_while(results, [], fn
        {:ok, entries}, acc -> {:cont, [entries | acc]}
        _error, _acc -> {:halt, :error}
      end)

    case values do
      :error ->
        nil

      values ->
        values
        |> Enum.reverse()
        |> FerricstoreServer.Native.Codec.encode_compact_binary_map_entry_list()
    end
  end

  defp guarded_collection_map(items, state, mapper) do
    limit = native_max_collection_response_items(state)
    guarded_collection_map(items, mapper, limit, 0, [])
  end

  defp guarded_collection_map([item | rest], mapper, limit, count, acc) do
    result = mapper.(item)
    count = count + collection_result_items(result)

    if limit > 0 and count > limit do
      collection_response_limit_error()
    else
      guarded_collection_map(rest, mapper, limit, count, [result | acc])
    end
  end

  defp guarded_collection_map([], _mapper, _limit, _count, acc), do: {:ok, Enum.reverse(acc)}

  defp collection_result_items({:ok, value}), do: collection_result_items(value)
  defp collection_result_items({:error, _reason}), do: 0
  defp collection_result_items(value) when is_map(value), do: map_size(value)
  defp collection_result_items(value) when is_list(value), do: length(value)
  defp collection_result_items(_value), do: 1

  defp native_max_collection_response_items(state) do
    Map.get(state, :max_collection_response_items) ||
      native_max_collection_response_items()
  end

  defp native_max_collection_response_items do
    Application.get_env(
      :ferricstore,
      :native_max_collection_response_items,
      @default_max_collection_response_items
    )
  end

  defp collection_items_exceeds_limit?(count, state) do
    limit = native_max_collection_response_items(state)
    limit > 0 and count > limit
  end

  defp collection_response_limit_reply(state) do
    {:bad_request, "ERR native collection response item limit exceeded", state}
  end

  defp collection_response_limit_error,
    do: {:error, "ERR native collection response item limit exceeded"}

  defp compact_hgetall_entry_result_to_map_result({:ok, entries}), do: Map.new(entries)
  defp compact_hgetall_entry_result_to_map_result({:error, _reason} = error), do: error

  defp compact_lrange_results(ctx, items, state) do
    limit = native_max_collection_response_items(state)
    compact_lrange_results(items, ctx, limit, 0, [])
  end

  defp compact_lrange_results([{key, start, stop} | rest], ctx, limit, count, acc) do
    remaining = compact_collection_remaining(limit, count)
    result = compact_lrange_result(ctx, key, start, stop, remaining)

    case result do
      :collection_response_limit_exceeded ->
        collection_response_limit_error()

      result ->
        count = count + collection_result_items(result)

        if limit > 0 and count > limit do
          collection_response_limit_error()
        else
          compact_lrange_results(rest, ctx, limit, count, [result | acc])
        end
    end
  end

  defp compact_lrange_results([], _ctx, _limit, _count, acc), do: {:ok, Enum.reverse(acc)}

  defp compact_lrange_result(ctx, key, start, stop, remaining) do
    case Router.compound_get(ctx, key, CompoundKey.type_key(key)) do
      nil ->
        []

      "list" ->
        compact_lrange_list_result(ctx, key, start, stop, remaining)

      _other_type ->
        @wrongtype_error
    end
  end

  defp compact_lrange_list_result(ctx, key, start, stop, remaining) do
    case compact_lrange_meta(ctx, key) do
      nil ->
        []

      {0, _left_pos, _right_pos} ->
        []

      {len, left_pos, _right_pos} = meta ->
        if compact_regular_list_meta?(meta) do
          compact_regular_lrange(ctx, key, len, left_pos, start, stop, remaining)
        else
          compact_lrange_fallback(ctx, key, start, stop, remaining)
        end

      :invalid ->
        compact_lrange_fallback(ctx, key, start, stop, remaining)
    end
  end

  defp compact_lrange_meta(ctx, key) do
    case Router.compound_get(ctx, key, CompoundKey.list_meta_key(key)) do
      nil -> nil
      binary when is_binary(binary) -> decode_compact_lrange_meta(binary)
      _other -> :invalid
    end
  end

  defp decode_compact_lrange_meta(binary) do
    case :erlang.binary_to_term(binary, [:safe]) do
      {len, left_pos, right_pos}
      when is_integer(len) and len >= 0 and is_integer(left_pos) and is_integer(right_pos) ->
        {len, left_pos, right_pos}

      _other ->
        :invalid
    end
  rescue
    _ -> :invalid
  end

  defp compact_regular_list_meta?({len, left_pos, right_pos}) do
    right_pos - left_pos == (len + 1) * @list_position_step
  end

  defp compact_regular_lrange(ctx, key, len, left_pos, start, stop, remaining) do
    {start_idx, stop_idx} = compact_lrange_bounds(start, stop, len)

    cond do
      start_idx > stop_idx ->
        []

      start_idx >= len ->
        []

      true ->
        stop_idx = min(stop_idx, len - 1)

        if compact_lrange_window_exceeds_remaining?(start_idx, stop_idx, remaining) do
          :collection_response_limit_exceeded
        else
          keys = compact_lrange_element_keys(key, left_pos, start_idx, stop_idx)
          values = Router.compound_batch_get(ctx, key, keys)

          if Enum.any?(values, &is_nil/1) do
            compact_lrange_fallback(ctx, key, start, stop, remaining)
          else
            values
          end
        end
    end
  end

  defp compact_lrange_fallback(ctx, key, start, stop, remaining) do
    with :ok <- compact_lrange_fallback_limit(ctx, key, start, stop, remaining) do
      FerricStore.Impl.lrange(ctx, key, start, stop)
    end
  end

  defp compact_lrange_fallback_limit(_ctx, _key, _start, _stop, :unlimited), do: :ok

  defp compact_lrange_fallback_limit(ctx, key, start, stop, remaining) do
    if start >= 0 and stop >= 0 and
         not compact_lrange_window_exceeds_remaining?(start, stop, remaining) do
      :ok
    else
      case FerricStore.Impl.llen(ctx, key) do
        {:ok, len} when is_integer(len) ->
          {start_idx, stop_idx} = compact_lrange_bounds(start, stop, len)

          cond do
            start_idx > stop_idx ->
              :ok

            start_idx >= len ->
              :ok

            compact_lrange_window_exceeds_remaining?(start_idx, min(stop_idx, len - 1), remaining) ->
              :collection_response_limit_exceeded

            true ->
              :ok
          end

        _other ->
          :ok
      end
    end
  end

  defp compact_lrange_window_exceeds_remaining?(_start_idx, _stop_idx, :unlimited), do: false

  defp compact_lrange_window_exceeds_remaining?(start_idx, stop_idx, _remaining)
       when start_idx > stop_idx,
       do: false

  defp compact_lrange_window_exceeds_remaining?(start_idx, stop_idx, remaining),
    do: stop_idx - start_idx + 1 > remaining

  defp compact_lrange_bounds(start, stop, len) do
    {normalize_compact_lrange_index(start, len), normalize_compact_lrange_index(stop, len)}
  end

  defp normalize_compact_lrange_index(index, len) when index < 0, do: max(0, len + index)
  defp normalize_compact_lrange_index(index, _len), do: index

  defp compact_lrange_element_keys(key, left_pos, start_idx, stop_idx) do
    first_pos = left_pos + @list_position_step + start_idx * @list_position_step

    Enum.map(0..(stop_idx - start_idx), fn offset ->
      CompoundKey.list_element(key, first_pos + offset * @list_position_step)
    end)
  end

  defp compact_zrange_results(ctx, items, state) do
    routed =
      Enum.map(items, fn {key, start, stop, with_scores} ->
        {Router.shard_for(ctx, key), key, start, stop, with_scores}
      end)

    table_cache = compact_zrange_table_cache(ctx, routed)

    limit = native_max_collection_response_items(state)

    compact_zrange_results(routed, table_cache, ctx, limit, 0, [])
  end

  defp compact_zrange_results(
         [{idx, key, start, stop, with_scores} | rest],
         table_cache,
         ctx,
         limit,
         count,
         acc
       ) do
    remaining = compact_collection_remaining(limit, count)

    result =
      case Map.get(table_cache, idx, :unavailable) do
        {index, lookup} ->
          compact_zrange_index_result(
            ctx,
            index,
            lookup,
            key,
            start,
            stop,
            with_scores,
            remaining
          )

        :unavailable ->
          compact_zrange_fallback(ctx, key, start, stop, with_scores, remaining)
      end

    case result do
      :collection_response_limit_exceeded ->
        collection_response_limit_error()

      result ->
        count = count + collection_result_items(result)

        if limit > 0 and count > limit do
          collection_response_limit_error()
        else
          compact_zrange_results(rest, table_cache, ctx, limit, count, [result | acc])
        end
    end
  end

  defp compact_zrange_results([], _table_cache, _ctx, _limit, _count, acc),
    do: {:ok, Enum.reverse(acc)}

  defp compact_zrange_table_cache(ctx, routed) do
    Enum.reduce(routed, %{}, fn {idx, _key, _start, _stop, _with_scores}, acc ->
      if Map.has_key?(acc, idx) do
        acc
      else
        Map.put(acc, idx, compact_zrange_tables(ctx, idx))
      end
    end)
  end

  defp compact_zrange_tables(ctx, idx) do
    {index, lookup} = ZSetIndex.table_names(ctx.name, idx)

    if :ets.info(index) != :undefined and :ets.info(lookup) != :undefined do
      {index, lookup}
    else
      :unavailable
    end
  rescue
    ArgumentError -> :unavailable
  end

  defp compact_zrange_index_result(ctx, index, lookup, key, start, stop, with_scores, remaining) do
    if ZSetIndex.ready?(lookup, key) do
      compact_zrange_ready_index_result(index, lookup, key, start, stop, with_scores, remaining)
    else
      compact_zrange_not_ready_result(ctx, key, start, stop, with_scores, remaining)
    end
  end

  defp compact_zrange_ready_index_result(index, lookup, key, start, stop, with_scores, remaining)
       when start >= 0 and stop >= 0 do
    if compact_zrange_window_exceeds_remaining?(start, stop, remaining) do
      count = ZSetIndex.count(index, lookup, key, :neg_inf, :inf)
      {start_idx, stop_idx} = compact_zrange_bounds(start, stop, count)

      if compact_zrange_window_exceeds_remaining?(start_idx, stop_idx, remaining) do
        :collection_response_limit_exceeded
      else
        compact_zrange_rank_result(index, key, start_idx, stop_idx, with_scores)
      end
    else
      compact_zrange_rank_result(index, key, start, stop, with_scores)
    end
  end

  defp compact_zrange_ready_index_result(index, lookup, key, start, stop, with_scores, remaining) do
    count = ZSetIndex.count(index, lookup, key, :neg_inf, :inf)
    {start_idx, stop_idx} = compact_zrange_bounds(start, stop, count)

    cond do
      start_idx > stop_idx ->
        {:ok, []}

      compact_zrange_window_exceeds_remaining?(start_idx, stop_idx, remaining) ->
        :collection_response_limit_exceeded

      true ->
        compact_zrange_rank_result(index, key, start_idx, stop_idx, with_scores)
    end
  end

  defp compact_collection_remaining(limit, _count) when limit <= 0, do: :unlimited
  defp compact_collection_remaining(limit, count), do: max(limit - count, 0)

  defp compact_zrange_window_exceeds_remaining?(_start_idx, _stop_idx, :unlimited), do: false

  defp compact_zrange_window_exceeds_remaining?(start_idx, stop_idx, _remaining)
       when start_idx > stop_idx,
       do: false

  defp compact_zrange_window_exceeds_remaining?(start_idx, stop_idx, remaining),
    do: stop_idx - start_idx + 1 > remaining

  defp compact_zrange_rank_result(index, key, start_idx, stop_idx, with_scores) do
    if start_idx > stop_idx do
      {:ok, []}
    else
      index
      |> ZSetIndex.rank_range(key, start_idx, stop_idx, false)
      |> maybe_strip_zrange_scores(with_scores)
      |> then(&{:ok, &1})
    end
  end

  defp compact_zrange_not_ready_result(ctx, key, start, stop, with_scores, remaining) do
    case Router.compound_get(ctx, key, CompoundKey.type_key(key)) do
      nil ->
        case Router.batch_get(ctx, [key]) do
          [nil] -> {:ok, []}
          [_value] -> @wrongtype_error
        end

      "zset" ->
        compact_zrange_fallback(ctx, key, start, stop, with_scores, remaining)

      _other_type ->
        @wrongtype_error
    end
  end

  defp compact_zrange_bounds(start, _stop, count) when count <= 0 do
    {normalize_compact_zrange_index(start, count), -1}
  end

  defp compact_zrange_bounds(start, stop, count) do
    start_idx = normalize_compact_zrange_index(start, count)
    stop_idx = normalize_compact_zrange_index(stop, count)

    cond do
      start_idx >= count -> {1, 0}
      true -> {start_idx, min(stop_idx, count - 1)}
    end
  end

  defp normalize_compact_zrange_index(index, count) when index < 0, do: max(0, count + index)
  defp normalize_compact_zrange_index(index, _count), do: index

  defp compact_zrange_fallback(ctx, key, start, stop, with_scores, remaining) do
    with :ok <- compact_zrange_fallback_limit(ctx, key, start, stop, remaining) do
      opts = if with_scores, do: [withscores: true], else: []
      FerricStore.Impl.zrange(ctx, key, start, stop, opts)
    end
  end

  defp compact_zrange_fallback_limit(_ctx, _key, _start, _stop, :unlimited), do: :ok

  defp compact_zrange_fallback_limit(ctx, key, start, stop, remaining) do
    if start >= 0 and stop >= 0 and
         not compact_zrange_window_exceeds_remaining?(start, stop, remaining) do
      :ok
    else
      case FerricStore.Impl.zcard(ctx, key) do
        {:ok, count} when is_integer(count) ->
          {start_idx, stop_idx} = compact_zrange_bounds(start, stop, count)

          if compact_zrange_window_exceeds_remaining?(start_idx, stop_idx, remaining) do
            :collection_response_limit_exceeded
          else
            :ok
          end

        _other ->
          :ok
      end
    end
  end

  defp maybe_strip_zrange_scores(members, true), do: members

  defp maybe_strip_zrange_scores(members, false),
    do: Enum.map(members, fn {member, _score} -> member end)

  defp flow_get_read_ops(17, ops) do
    Enum.map(ops, fn {:flow_get, id, opts} ->
      {:flow_get, id, Keyword.delete(opts, :return)}
    end)
  end

  defp flow_get_read_ops(_mode, ops), do: ops

  defp compact_flow_get_results(ctx, mode, ops) do
    started = FlowTelemetry.start_time()
    read_ops = flow_get_read_ops(mode, ops)

    if compact_flow_get_valid_ops?(read_ops) do
      results =
        case read_ops do
          [] ->
            []

          [{:flow_get, _id, opts} | rest] ->
            partition_key = Keyword.get(opts, :partition_key)

            if compact_flow_get_same_partition?(rest, partition_key) do
              compact_flow_get_same_partition_results(ctx, read_ops, partition_key, mode)
            else
              compact_flow_get_partitioned_results(ctx, read_ops, mode)
            end
        end

      FlowTelemetry.observe_pipeline_read_batch(started, read_ops)
      results
    else
      Ferricstore.Flow.pipeline_read_batch(ctx, read_ops)
    end
  end

  defp compact_flow_get_valid_ops?(ops), do: Enum.all?(ops, &compact_flow_get_valid_op?/1)

  defp compact_flow_get_valid_op?({:flow_get, id, opts}) when is_binary(id) and id != "" do
    partition_key = Keyword.get(opts, :partition_key)
    max_key_size = Router.max_key_size()

    cond do
      is_binary(partition_key) and partition_key == "" ->
        false

      byte_size(id) + 53 <= max_key_size ->
        true

      true ->
        byte_size(FlowKeys.state_key(id, partition_key)) <= max_key_size
    end
  end

  defp compact_flow_get_valid_op?(_op), do: false

  defp compact_flow_get_same_partition?([], _partition_key), do: true

  defp compact_flow_get_same_partition?([{:flow_get, _id, opts} | rest], partition_key),
    do:
      Keyword.get(opts, :partition_key) == partition_key and
        compact_flow_get_same_partition?(rest, partition_key)

  defp compact_flow_get_same_partition?(_ops, _partition_key), do: false

  defp compact_flow_get_same_partition_results(ctx, ops, partition_key, mode) do
    ids = Enum.map(ops, fn {:flow_get, id, _opts} -> id end)

    ctx
    |> Router.flow_batch_get(ids, partition_key)
    |> Enum.map(&compact_flow_get_decode(&1, mode))
  end

  defp compact_flow_get_partitioned_results(ctx, ops, mode) do
    indexed_pairs =
      ops
      |> Enum.with_index()
      |> Enum.group_by(fn {{:flow_get, _id, opts}, _idx} -> Keyword.get(opts, :partition_key) end)
      |> Enum.flat_map(fn {partition_key, group} ->
        ids = Enum.map(group, fn {{:flow_get, id, _opts}, _idx} -> id end)
        values = Router.flow_batch_get(ctx, ids, partition_key)

        group
        |> Enum.zip(values)
        |> Enum.map(fn {{{:flow_get, _id, _opts}, idx}, value} ->
          {idx, compact_flow_get_decode(value, mode)}
        end)
      end)
      |> Map.new()

    for idx <- 0..(length(ops) - 1), do: Map.fetch!(indexed_pairs, idx)
  end

  defp compact_flow_get_decode(nil), do: {:ok, nil}

  defp compact_flow_get_decode(value) when is_binary(value) do
    {:ok, FlowCodec.decode_record(value)}
  rescue
    _ -> {:ok, nil}
  end

  defp compact_flow_get_decode({:error, _reason} = error), do: error
  defp compact_flow_get_decode(_value), do: {:ok, nil}

  defp compact_flow_get_decode(value, 17) when is_binary(value) do
    {:ok, FlowCodec.decode_record_meta(value)}
  rescue
    _ -> {:ok, nil}
  end

  defp compact_flow_get_decode(value, _mode), do: compact_flow_get_decode(value)

  defp maybe_compact_flow_get_meta_results(results, 17), do: results

  defp maybe_compact_flow_get_meta_results(results, _mode), do: results

  defp compact_pipeline_flow_pairs(results, requests) do
    results
    |> pipeline_flow_results(requests)
    |> format_pipeline_results(:pairs)
  end

  defp compact_pipeline_flow_values(results) do
    case compact_pipeline_flow_ok_values(results, []) do
      {:ok, values} ->
        compact_pipeline_values_payload(values)

      :error ->
        results
        |> compact_pipeline_result_pairs()
        |> format_compact_pipeline_pairs(@op_pipeline, :compact)
    end
  end

  defp compact_pipeline_flow_ok_values([], acc), do: {:ok, Enum.reverse(acc)}

  defp compact_pipeline_flow_ok_values([result | rest], acc) do
    case pipeline_flow_result(result) do
      {:ok, value} -> compact_pipeline_flow_ok_values(rest, [value | acc])
      _status_value -> :error
    end
  end

  defp compact_pipeline_requests(_opcode, 0), do: []

  defp compact_pipeline_requests(opcode, count) do
    Enum.map(1..count, fn request_id -> {opcode, request_id, 1} end)
  end

  defp compact_mixed_collect([], get_keys, set_pairs), do: {get_keys, set_pairs}

  defp compact_mixed_collect([{:set, key, value} | rest], get_keys, set_pairs),
    do: compact_mixed_collect(rest, get_keys, [{key, value} | set_pairs])

  defp compact_mixed_collect([{:get, key} | rest], get_keys, set_pairs),
    do: compact_mixed_collect(rest, [key | get_keys], set_pairs)

  defp compact_mixed_pairs([], [], [], acc), do: Enum.reverse(acc)

  defp compact_mixed_pairs(
         [{:set, _key, _value} | rest],
         get_values,
         [set_pair | set_pairs],
         acc
       ),
       do: compact_mixed_pairs(rest, get_values, set_pairs, [set_pair | acc])

  defp compact_mixed_pairs([{:get, _key} | rest], [value | get_values], set_pairs, acc),
    do: compact_mixed_pairs(rest, get_values, set_pairs, [["ok", value] | acc])

  defp pipeline_data_writes(commands) do
    case pipeline_hash_hsets(commands, [], []) do
      {:ok, _requests, _ops} = ok -> ok
      :fallback -> pipeline_list_lpushes_or_set_writes(commands)
    end
  end

  defp pipeline_list_lpushes_or_set_writes(commands) do
    case pipeline_list_pushes(commands, [], []) do
      {:ok, _requests, _ops} = ok -> ok
      :fallback -> pipeline_list_pops_or_set_writes(commands)
    end
  end

  defp pipeline_list_pops_or_set_writes(commands) do
    case pipeline_list_pops(commands, [], []) do
      {:ok, _requests, _ops} = ok -> ok
      :fallback -> pipeline_set_sadds_or_zset_zadds(commands)
    end
  end

  defp pipeline_set_sadds_or_zset_zadds(commands) do
    case pipeline_set_sadds(commands, [], []) do
      {:ok, _requests, _ops} = ok -> ok
      :fallback -> pipeline_set_srems_or_zset_writes(commands)
    end
  end

  defp pipeline_set_srems_or_zset_writes(commands) do
    case pipeline_set_srems(commands, [], []) do
      {:ok, _requests, _ops} = ok -> ok
      :fallback -> pipeline_zset_zadds_or_rems(commands)
    end
  end

  defp pipeline_zset_zadds_or_rems(commands) do
    case pipeline_zset_zadds(commands, [], []) do
      {:ok, _requests, _ops} = ok -> ok
      :fallback -> pipeline_zset_zrems(commands, [], [])
    end
  end

  defp pipeline_hash_hsets([], requests, ops),
    do: {:ok, Enum.reverse(requests), Enum.reverse(ops)}

  defp pipeline_hash_hsets(
         [
           %{opcode: @op_hset, body: %{"key" => key, "fields" => fields}} = command
           | rest
         ],
         requests,
         ops
       )
       when is_binary(key) and is_map(fields) and map_size(fields) == 1 do
    [{field, value}] = Map.to_list(fields)

    if is_binary(field) and is_binary(value) do
      pipeline_hash_hsets(
        rest,
        [pipeline_request(command) | requests],
        [{key, {:hset_single, key, field, value}} | ops]
      )
    else
      :fallback
    end
  end

  defp pipeline_hash_hsets(_commands, _requests, _ops), do: :fallback

  defp pipeline_list_pushes([], requests, ops),
    do: {:ok, Enum.reverse(requests), Enum.reverse(ops)}

  defp pipeline_list_pushes(
         [
           %{opcode: opcode, body: %{"key" => key, "values" => [value]}} = command
           | rest
         ],
         requests,
         ops
       )
       when opcode in [@op_lpush, @op_rpush] and is_binary(key) and is_binary(value) do
    op = if opcode == @op_lpush, do: :lpush_single, else: :rpush_single

    pipeline_list_pushes(
      rest,
      [pipeline_request(command) | requests],
      [{key, {op, key, value}} | ops]
    )
  end

  defp pipeline_list_pushes(_commands, _requests, _ops), do: :fallback

  defp pipeline_list_pops([], requests, ops),
    do: {:ok, Enum.reverse(requests), Enum.reverse(ops)}

  defp pipeline_list_pops(
         [
           %{opcode: opcode, body: %{"key" => key} = body} = command
           | rest
         ],
         requests,
         ops
       )
       when opcode in [@op_lpop, @op_rpop] and is_binary(key) do
    if list_pop_pipeline_body?(body) do
      op = if opcode == @op_lpop, do: :lpop, else: :rpop

      pipeline_list_pops(
        rest,
        [pipeline_request(command) | requests],
        [{key, {:list_op, key, {op, 1}}} | ops]
      )
    else
      :fallback
    end
  end

  defp pipeline_list_pops(_commands, _requests, _ops), do: :fallback

  defp list_pop_pipeline_body?(%{"count" => 1} = body), do: map_size(body) == 2
  defp list_pop_pipeline_body?(body), do: map_size(body) == 1

  defp pipeline_set_sadds([], requests, ops),
    do: {:ok, Enum.reverse(requests), Enum.reverse(ops)}

  defp pipeline_set_sadds(
         [
           %{opcode: @op_sadd, body: %{"key" => key, "members" => [member]}} = command
           | rest
         ],
         requests,
         ops
       )
       when is_binary(key) and is_binary(member) do
    pipeline_set_sadds(
      rest,
      [pipeline_request(command) | requests],
      [{key, {:sadd_single, key, member}} | ops]
    )
  end

  defp pipeline_set_sadds(_commands, _requests, _ops), do: :fallback

  defp pipeline_set_srems([], requests, ops),
    do: {:ok, Enum.reverse(requests), Enum.reverse(ops)}

  defp pipeline_set_srems(
         [
           %{opcode: @op_srem, body: %{"key" => key, "members" => [member]}} = command
           | rest
         ],
         requests,
         ops
       )
       when is_binary(key) and is_binary(member) do
    pipeline_set_srems(
      rest,
      [pipeline_request(command) | requests],
      [{key, {:srem_single, key, member}} | ops]
    )
  end

  defp pipeline_set_srems(_commands, _requests, _ops), do: :fallback

  defp pipeline_zset_zadds([], requests, ops),
    do: {:ok, Enum.reverse(requests), Enum.reverse(ops)}

  defp pipeline_zset_zadds(
         [
           %{opcode: @op_zadd, body: %{"key" => key, "items" => [[score, member]]}} = command
           | rest
         ],
         requests,
         ops
       )
       when is_binary(key) and is_number(score) and is_binary(member) do
    pipeline_zset_zadds(
      rest,
      [pipeline_request(command) | requests],
      [{key, {:zadd_single, key, score * 1.0, member}} | ops]
    )
  end

  defp pipeline_zset_zadds(_commands, _requests, _ops), do: :fallback

  defp pipeline_zset_zrems([], requests, ops),
    do: {:ok, Enum.reverse(requests), Enum.reverse(ops)}

  defp pipeline_zset_zrems(
         [
           %{opcode: @op_zrem, body: %{"key" => key, "members" => [member]}} = command
           | rest
         ],
         requests,
         ops
       )
       when is_binary(key) and is_binary(member) do
    pipeline_zset_zrems(
      rest,
      [pipeline_request(command) | requests],
      [{key, {:zrem_single, key, member}} | ops]
    )
  end

  defp pipeline_zset_zrems(_commands, _requests, _ops), do: :fallback

  defp pipeline_plain_sets([], requests, kv_pairs),
    do: {:ok, Enum.reverse(requests), Enum.reverse(kv_pairs)}

  defp pipeline_plain_sets(
         [%{opcode: @op_set, body: %{"key" => key, "value" => value} = body} = command | rest],
         requests,
         kv_pairs
       )
       when is_binary(key) and is_binary(value) do
    if map_size(body) == 2 do
      pipeline_plain_sets(rest, [pipeline_request(command) | requests], [{key, value} | kv_pairs])
    else
      :fallback
    end
  end

  defp pipeline_plain_sets(_commands, _requests, _kv_pairs), do: :fallback

  defp pipeline_plain_gets([], requests, keys),
    do: {:ok, Enum.reverse(requests), Enum.reverse(keys)}

  defp pipeline_plain_gets(
         [%{opcode: opcode, body: %{"key" => key} = body} = command | rest],
         requests,
         keys
       )
       when opcode == @op_get and is_binary(key) do
    if map_size(body) == 1 do
      pipeline_plain_gets(rest, [pipeline_request(command) | requests], [key | keys])
    else
      :fallback
    end
  end

  defp pipeline_plain_gets(_commands, _requests, _keys), do: :fallback

  defp pipeline_request(command) do
    {command.opcode, command.request_id, command.lane_id}
  end

  defp pipeline_flow_writes([], requests, ops),
    do: {:ok, Enum.reverse(requests), Enum.reverse(ops)}

  defp pipeline_flow_writes([command | rest], requests, ops) do
    case pipeline_flow_write_op(command) do
      {:ok, op} -> pipeline_flow_writes(rest, [pipeline_request(command) | requests], [op | ops])
      :fallback -> :fallback
    end
  end

  defp pipeline_flow_write_op(%{opcode: @op_flow_create, body: body}) do
    with {:ok, id} <- require_binary(body, "id"),
         {:ok, opts} <- flow_opts(body, ["id"]) do
      {:ok, {:flow_create, id, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_write_op(%{opcode: @op_flow_start_and_claim, body: body}) do
    with {:ok, id} <- require_binary(body, "id"),
         {:ok, type} <- require_binary(body, "type"),
         {:ok, initial_state} <- require_binary(body, "initial_state"),
         {:ok, opts} <- flow_opts(body, ["id", "type", "initial_state"]) do
      {:ok, {:flow_start_and_claim, id, type, initial_state, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_write_op(%{opcode: @op_flow_transition, body: body}) do
    with {:ok, id} <- require_binary(body, "id"),
         {:ok, from_state} <- require_binary(body, "from_state"),
         {:ok, to_state} <- require_binary(body, "to_state"),
         {:ok, opts} <- flow_opts(body, ["id", "from_state", "to_state"]) do
      {:ok, {:flow_transition, id, from_state, to_state, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_write_op(%{opcode: @op_flow_step_continue, body: body}) do
    with {:ok, id} <- require_binary(body, "id"),
         {:ok, lease_token} <- require_binary(body, "lease_token"),
         {:ok, from_state} <- require_binary(body, "from_state"),
         {:ok, to_state} <- require_binary(body, "to_state"),
         {:ok, opts} <- flow_opts(body, ["id", "lease_token", "from_state", "to_state"]) do
      {:ok, {:flow_step_continue, id, lease_token, from_state, to_state, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_write_op(%{opcode: @op_flow_value_put, body: body}) do
    with {:ok, value} <- require_any(body, "value"),
         true <- is_binary(Map.get(body, "owner_flow_id")) and is_binary(Map.get(body, "name")),
         {:ok, opts} <- flow_opts(body, ["value"]) do
      {:ok, {:flow_named_value_put, value, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_write_op(%{opcode: @op_flow_signal, body: body}) do
    with {:ok, id} <- require_binary(body, "id"),
         {:ok, opts} <- flow_opts(body, ["id"]) do
      {:ok, {:flow_signal, id, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_write_op(%{opcode: @op_flow_complete, body: body}) do
    pipeline_flow_lease_op(:flow_complete, body)
  end

  defp pipeline_flow_write_op(%{opcode: @op_flow_retry, body: body}) do
    pipeline_flow_lease_op(:flow_retry, body)
  end

  defp pipeline_flow_write_op(%{opcode: @op_flow_fail, body: body}) do
    pipeline_flow_lease_op(:flow_fail, body)
  end

  defp pipeline_flow_write_op(%{opcode: @op_flow_cancel, body: body}) do
    with {:ok, id} <- require_binary(body, "id"),
         {:ok, opts} <- flow_opts(body, ["id"]) do
      {:ok, {:flow_cancel, id, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_write_op(%{opcode: @op_flow_rewind, body: body}) do
    with {:ok, id} <- require_binary(body, "id"),
         {:ok, opts} <- flow_opts(body, ["id"]) do
      {:ok, {:flow_rewind, id, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_write_op(_command), do: :fallback

  defp pipeline_flow_reads([], requests, ops),
    do: {:ok, Enum.reverse(requests), Enum.reverse(ops)}

  defp pipeline_flow_reads([command | rest], requests, ops) do
    case pipeline_flow_read_op(command) do
      {:ok, op} -> pipeline_flow_reads(rest, [pipeline_request(command) | requests], [op | ops])
      :fallback -> :fallback
    end
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_get, body: body}) do
    with {:ok, id} <- require_binary(body, "id"),
         {:ok, opts} <- flow_opts(body, ["id"]) do
      {:ok, {:flow_get, id, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_history, body: body}) do
    with {:ok, id} <- require_binary(body, "id"),
         {:ok, opts} <- flow_opts(body, ["id"]) do
      {:ok, {:flow_history, id, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_list, body: body}) do
    pipeline_flow_type_read_op(:flow_list, body)
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_stats, body: body}) do
    pipeline_flow_type_read_op(:flow_stats, body)
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_attributes, body: body}) do
    pipeline_flow_type_read_op(:flow_attributes, body)
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_attribute_values, body: body}) do
    with {:ok, type} <- require_binary(body, "type"),
         {:ok, attr_name} <- require_binary(body, "attribute"),
         {:ok, opts} <- flow_opts(body, ["type", "attribute"]) do
      {:ok, {:flow_attribute_values, type, attr_name, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_terminals, body: body}) do
    pipeline_flow_type_read_op(:flow_terminals, body)
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_failures, body: body}) do
    pipeline_flow_type_read_op(:flow_failures, body)
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_info, body: body}) do
    pipeline_flow_type_read_op(:flow_info, body)
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_stuck, body: body}) do
    pipeline_flow_type_read_op(:flow_stuck, body)
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_by_parent, body: body}) do
    pipeline_flow_id_index_read_op(:flow_by_parent, "parent_id", body)
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_by_root, body: body}) do
    pipeline_flow_id_index_read_op(:flow_by_root, "root_id", body)
  end

  defp pipeline_flow_read_op(%{opcode: @op_flow_by_correlation, body: body}) do
    pipeline_flow_id_index_read_op(:flow_by_correlation, "correlation_id", body)
  end

  defp pipeline_flow_read_op(_command), do: :fallback

  defp pipeline_flow_type_read_op(op, body) do
    with {:ok, type} <- require_binary(body, "type"),
         {:ok, opts} <- flow_opts(body, ["type"]) do
      {:ok, {op, type, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_id_index_read_op(op, id_key, body) do
    with {:ok, id} <- require_binary(body, id_key),
         {:ok, opts} <- flow_opts(body, [id_key]) do
      {:ok, {op, id, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_create_many([], requests, items, opts) when is_list(opts),
    do: {:ok, Enum.reverse(requests), Enum.reverse(items), opts}

  defp pipeline_flow_create_many(
         [%{opcode: @op_flow_create, body: body} = command | rest],
         requests,
         items,
         base_opts
       ) do
    with {:ok, id} <- require_binary(body, "id"),
         {:ok, opts} <- flow_opts(body, ["id", "payload", "partition_key"]),
         true <- is_nil(base_opts) or opts == base_opts do
      item =
        %{"id" => id}
        |> maybe_put_flow_create_item_value("payload", Map.get(body, "payload"))
        |> maybe_put_flow_create_item_value("partition_key", Map.get(body, "partition_key"))

      pipeline_flow_create_many(
        rest,
        [pipeline_request(command) | requests],
        [item | items],
        opts
      )
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_create_many(_commands, _requests, _items, _opts), do: :fallback

  defp maybe_put_flow_create_item_value(item, _key, nil), do: item
  defp maybe_put_flow_create_item_value(item, key, value), do: Map.put(item, key, value)

  defp pipeline_flow_shared_value_puts([], requests, items),
    do: {:ok, Enum.reverse(requests), Enum.reverse(items)}

  defp pipeline_flow_shared_value_puts(
         [%{opcode: @op_flow_value_put, body: body} = command | rest],
         requests,
         items
       ) do
    with {:ok, value} <- require_any(body, "value"),
         true <- is_nil(Map.get(body, "name")),
         {:ok, opts} <- flow_opts(body, ["value"]) do
      pipeline_flow_shared_value_puts(
        rest,
        [pipeline_request(command) | requests],
        [{value, opts} | items]
      )
    else
      _error -> :fallback
    end
  end

  defp pipeline_flow_shared_value_puts(_commands, _requests, _items), do: :fallback

  defp pipeline_flow_lease_op(op, body) do
    with {:ok, id} <- require_binary(body, "id"),
         {:ok, lease_token} <- require_binary(body, "lease_token"),
         {:ok, opts} <- flow_opts(body, ["id", "lease_token"]) do
      {:ok, {op, id, lease_token, opts}}
    else
      _error -> :fallback
    end
  end

  defp pipeline_set_results(results, requests) do
    requests
    |> Enum.zip(results)
    |> Enum.map(fn {{opcode, request_id, lane_id}, result} ->
      {status, value} = pipeline_set_result(result)
      pipeline_result(opcode, request_id, lane_id, status, value)
    end)
  end

  defp pipeline_get_results(values, requests) do
    requests
    |> Enum.zip(values)
    |> Enum.map(fn {{opcode, request_id, lane_id}, value} ->
      pipeline_result(opcode, request_id, lane_id, :ok, value)
    end)
  end

  defp pipeline_flow_results(results, requests) do
    requests
    |> Enum.zip(results)
    |> Enum.map(fn {{opcode, request_id, lane_id}, result} ->
      {status, value} = pipeline_flow_result(result)
      pipeline_result(opcode, request_id, lane_id, status, value)
    end)
  end

  defp pipeline_create_many_results(:ok, requests), do: pipeline_ok_results(requests)
  defp pipeline_create_many_results({:ok, :ok}, requests), do: pipeline_ok_results(requests)

  defp pipeline_create_many_results({:ok, results}, requests) when is_list(results),
    do: pipeline_flow_results(results, requests)

  defp pipeline_create_many_results({:error, _reason} = error, requests),
    do: pipeline_flow_results(List.duplicate(error, length(requests)), requests)

  defp pipeline_create_many_results(result, requests),
    do: pipeline_flow_results(List.duplicate(result, length(requests)), requests)

  defp pipeline_ok_results(requests) do
    Enum.map(requests, fn {opcode, request_id, lane_id} ->
      pipeline_result(opcode, request_id, lane_id, :ok, "OK")
    end)
  end

  defp pipeline_set_result(:ok), do: {:ok, "OK"}
  defp pipeline_set_result({:ok, :ok}), do: {:ok, "OK"}
  defp pipeline_set_result({:ok, value}), do: {:ok, value}

  defp pipeline_set_result({:error, reason}) when is_binary(reason) do
    status =
      cond do
        String.starts_with?(reason, "BUSY") -> :busy
        String.starts_with?(reason, "OOM") -> :busy
        true -> :error
      end

    {status, reason}
  end

  defp pipeline_set_result({:error, reason}), do: {:error, inspect(reason)}
  defp pipeline_set_result(value), do: {:ok, value}

  defp pipeline_flow_result(:ok), do: {:ok, "OK"}
  defp pipeline_flow_result({:ok, :ok}), do: {:ok, "OK"}
  defp pipeline_flow_result({:ok, value}), do: {:ok, value}

  defp pipeline_flow_result({:error, reason}) when is_binary(reason) do
    status =
      cond do
        String.starts_with?(reason, "BUSY") -> :busy
        String.starts_with?(reason, "OOM") -> :busy
        true -> :error
      end

    {status, reason}
  end

  defp pipeline_flow_result({:error, reason}), do: {:error, inspect(reason)}
  defp pipeline_flow_result(value), do: {:ok, value}

  defp compact_pipeline_result_pairs(results) do
    Enum.map(results, fn result ->
      {status, value} = pipeline_flow_result(result)
      [Atom.to_string(status), value]
    end)
  end

  defp format_compact_pipeline_results(results, _opcode, :values),
    do: compact_pipeline_flow_values(results)

  defp format_compact_pipeline_results(results, opcode, return_format) do
    results
    |> compact_pipeline_result_pairs()
    |> format_compact_pipeline_pairs(opcode, return_format)
  end

  defp pipeline_set_result_pair(result) do
    {status, value} = pipeline_set_result(result)
    [Atom.to_string(status), value]
  end

  defp format_compact_set_results(results, :values) do
    case compact_ok_result_count(results, 0) do
      {:ok, count} ->
        FerricstoreServer.Native.Codec.encode_compact_ok_count(count)

      :error ->
        results
        |> Enum.map(&pipeline_set_result_pair/1)
        |> format_compact_pipeline_pairs(@op_set, :compact)
    end
  end

  defp format_compact_set_results(results, return_format) do
    results
    |> Enum.map(&pipeline_set_result_pair/1)
    |> format_compact_pipeline_pairs(@op_set, return_format)
  end

  defp compact_ok_result_count([], count), do: {:ok, count}
  defp compact_ok_result_count([:ok | rest], count), do: compact_ok_result_count(rest, count + 1)

  defp compact_ok_result_count([{:ok, :ok} | rest], count),
    do: compact_ok_result_count(rest, count + 1)

  defp compact_ok_result_count([{:ok, "OK"} | rest], count),
    do: compact_ok_result_count(rest, count + 1)

  defp compact_ok_result_count([{:ok, "ok"} | rest], count),
    do: compact_ok_result_count(rest, count + 1)

  defp compact_ok_result_count(_results, _count), do: :error

  defp compact_pipeline_get_result(values, :values), do: values
  defp compact_pipeline_get_result(values, _return_format), do: Enum.map(values, &["ok", &1])

  defp format_compact_pipeline_get_result(values, _opcode, :values) do
    case FerricstoreServer.Native.Codec.encode_compact_kv_mget(values) do
      payload when is_binary(payload) ->
        payload

      nil ->
        format_compact_pipeline_pairs(Enum.map(values, &["ok", &1]), @op_get, :compact)
    end
  end

  defp format_compact_pipeline_get_result(pairs, opcode, return_format),
    do: format_compact_pipeline_pairs(pairs, opcode, return_format)

  defp format_compact_pipeline_pairs(pairs, _opcode, :pairs), do: pairs

  defp format_compact_pipeline_pairs(pairs, _opcode, :values) do
    case compact_pipeline_ok_values(pairs, []) do
      {:ok, values} -> compact_pipeline_values_payload(values)
      :error -> format_compact_pipeline_pairs(pairs, @op_set, :compact)
    end
  end

  defp format_compact_pipeline_pairs(pairs, opcode, :maps) do
    pairs
    |> Enum.with_index(1)
    |> Enum.map(fn {[status, value], request_id} ->
      %{
        "opcode" => opcode,
        "request_id" => request_id,
        "lane_id" => 1,
        "status" => status,
        "value" => value
      }
    end)
  end

  defp format_compact_pipeline_pairs(pairs, _opcode, :compact) do
    case FerricstoreServer.Native.Codec.encode_compact_pipeline_response(pairs) do
      payload when is_binary(payload) -> payload
      nil -> pairs
    end
  end

  defp compact_pipeline_ok_values([], acc), do: {:ok, Enum.reverse(acc)}

  defp compact_pipeline_ok_values([["ok", value] | rest], acc),
    do: compact_pipeline_ok_values(rest, [value | acc])

  defp compact_pipeline_ok_values(_pairs, _acc), do: :error

  defp compact_pipeline_values_payload(values) do
    cond do
      Enum.all?(values, fn value -> value in ["OK", "ok", :ok] end) ->
        FerricstoreServer.Native.Codec.encode_compact_ok_list(values)

      payload = FerricstoreServer.Native.Codec.encode_compact_kv_mget(values) ->
        payload

      payload = FerricstoreServer.Native.Codec.encode_compact_integer_list(values) ->
        payload

      payload = FerricstoreServer.Native.Codec.encode_compact_flow_claim_jobs(values) ->
        payload

      payload = FerricstoreServer.Native.Codec.encode_compact_binary_list_list(values) ->
        payload

      payload = FerricstoreServer.Native.Codec.encode_compact_binary_map_list(values) ->
        payload

      payload = FerricstoreServer.Native.Codec.encode_compact_flow_record_list(values) ->
        payload

      true ->
        values
    end
  end

  defp pipeline_result(opcode, request_id, lane_id, status, value) do
    %{
      "opcode" => opcode,
      "request_id" => request_id,
      "lane_id" => lane_id,
      "status" => Atom.to_string(status),
      "value" => value
    }
  end

  defp execute_pipeline_command(command, state, request_context) do
    body = command_body_with_request_context(command.opcode, command.body, request_context)
    {status, value, state} = execute(command.opcode, body, state)

    result = pipeline_result(command.opcode, command.request_id, command.lane_id, status, value)

    {result, state}
  end

  defp command_body_with_request_context(@op_command_exec, body, request_context)
       when is_map(body) and map_size(request_context) > 0 do
    Map.put(body, "request_context", request_context)
  end

  defp command_body_with_request_context(_opcode, body, _request_context), do: body

  defp flow_opts(payload, drop_keys) do
    case Map.get(payload, :__wire_flow_opts__) do
      opts when is_list(opts) ->
        {:ok, opts}

      _other ->
        opts =
          payload
          |> Map.drop([
            "opts",
            "deadline_ms",
            :__wire_flow_items_normalized__,
            :__wire_flow_opts__ | drop_keys
          ])
          |> Map.merge(option_map(Map.get(payload, "opts", %{})))

        to_flow_opts(opts)
    end
  end

  defp flow_items(payload, key, item_kind) do
    case {Map.get(payload, :__wire_flow_items_normalized__), Map.get(payload, key)} do
      {true, items} when is_list(items) ->
        {:ok, items}

      {_normalized, items} when is_list(items) ->
        items
        |> Enum.reduce_while({:ok, []}, fn
          item, {:ok, acc} ->
            case flow_item(item, item_kind) do
              {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
              {:error, reason} -> {:halt, {:error, reason}}
            end

          _item, _acc ->
            {:halt, {:error, "ERR native flow item is invalid"}}
        end)
        |> case do
          {:ok, values} -> {:ok, Enum.reverse(values)}
          {:error, reason} -> {:error, reason}
        end

      _other ->
        {:error, "ERR native field #{key} must be a list"}
    end
  end

  defp flow_item(item, _item_kind) when is_map(item), do: flow_item_map(item)

  defp flow_item([id, payload], :create) when is_binary(id),
    do: {:ok, {:id, id, :payload, payload}}

  defp flow_item([id, partition_key, payload], :create) when is_binary(id),
    do: {:ok, %{"id" => id, "partition_key" => partition_key, "payload" => payload}}

  defp flow_item([id, lease_token, fencing_token], :claimed) when is_binary(id) do
    {:ok, %{"id" => id, "lease_token" => lease_token, "fencing_token" => fencing_token}}
  end

  defp flow_item([id, partition_key, lease_token, fencing_token], :claimed) when is_binary(id) do
    {:ok,
     %{
       "id" => id,
       "partition_key" => partition_key,
       "lease_token" => lease_token,
       "fencing_token" => fencing_token
     }}
  end

  defp flow_item([id, fencing_token], :fenced) when is_binary(id),
    do: {:ok, %{"id" => id, "fencing_token" => fencing_token}}

  defp flow_item([id, partition_key, fencing_token], :fenced) when is_binary(id) do
    {:ok, %{"id" => id, "partition_key" => partition_key, "fencing_token" => fencing_token}}
  end

  defp flow_item([id, fencing_token, lease_token], :transition) when is_binary(id) do
    {:ok, %{"id" => id, "fencing_token" => fencing_token, "lease_token" => lease_token}}
  end

  defp flow_item([id, partition_key, fencing_token, lease_token], :transition)
       when is_binary(id) do
    {:ok,
     %{
       "id" => id,
       "partition_key" => partition_key,
       "fencing_token" => fencing_token,
       "lease_token" => lease_token
     }}
  end

  defp flow_item(_item, _item_kind), do: {:error, "ERR native flow item is invalid"}

  defp flow_item_map(map) when is_map(map) do
    Enum.reduce_while(map, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      key = flow_option_key(key)

      case Map.fetch(@known_flow_options, key) do
        {:ok, atom_key} ->
          {:cont, {:ok, Map.put(acc, key, coerce_option_value(atom_key, value))}}

        :error ->
          {:halt, {:error, "ERR native unknown flow option #{key}"}}
      end
    end)
  end

  defp flow_option_key(key) when is_binary(key), do: key
  defp flow_option_key(key) when is_atom(key), do: Atom.to_string(key)
  defp flow_option_key(key), do: to_string(key)

  defp to_flow_opts(map) when map_size(map) == 0, do: {:ok, []}

  defp to_flow_opts(map) when is_map(map) do
    Enum.reduce_while(map, {:ok, []}, fn {key, value}, {:ok, acc} ->
      case Map.fetch(@known_flow_options, key) do
        {:ok, atom_key} ->
          {:cont, {:ok, [{atom_key, coerce_option_value(atom_key, value)} | acc]}}

        :error ->
          {:halt, {:error, "ERR native unknown flow option #{key}"}}
      end
    end)
    |> case do
      {:ok, opts} -> {:ok, Enum.reverse(opts)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp option_map(map) when is_map(map), do: map
  defp option_map(_), do: %{}

  defp coerce_option_value(key, value)
       when key in [:wait, :backoff] and is_binary(value),
       do: Map.get(@atom_values, String.downcase(value), value)

  defp coerce_option_value(:return, value) when is_binary(value),
    do: Map.get(@atom_values, String.downcase(value), value)

  defp coerce_option_value(:kind, value) when is_binary(value),
    do: Map.get(@atom_values, String.downcase(value), value)

  defp coerce_option_value(:overlap_policy, value) when is_binary(value),
    do: Map.get(@atom_values, String.downcase(value), value)

  defp coerce_option_value(_key, value), do: value

  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"

  defp supported_opcodes do
    @commands
    |> Enum.map(fn {opcode, name} -> %{"opcode" => opcode, "name" => name} end)
    |> Enum.sort_by(& &1["opcode"])
  end

  defp incr_commands(state) do
    Stats.incr_commands(state.stats_counter)
    :ok
  end

  @doc false
  def mark_command_seen(state), do: incr_commands(state)
end
