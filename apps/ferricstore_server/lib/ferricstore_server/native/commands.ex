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
  alias Ferricstore.Flow.{ClaimDueAPI, ClaimWaiters}
  alias Ferricstore.Store.{Ops, Router}
  alias Ferricstore.Store.SlotMap
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry

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
  @op_batch 0x000E
  @op_route_batch 0x000F
  @op_event 0x0010
  @op_subscribe_events 0x0011
  @op_unsubscribe_events 0x0012

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
    @op_batch => "BATCH",
    @op_route_batch => "ROUTE_BATCH",
    @op_event => "EVENT",
    @op_subscribe_events => "SUBSCRIBE_EVENTS",
    @op_unsubscribe_events => "UNSUBSCRIBE_EVENTS"
  }

  @kv_commands %{
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
    @op_fetch_or_compute_error => "FETCH_OR_COMPUTE_ERROR"
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
    @op_flow_retention_cleanup => "FLOW.RETENTION_CLEANUP"
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
    "attempt" => :attempt,
    "backoff" => :backoff,
    "base_ms" => :base_ms,
    "block_ms" => :block_ms,
    "children" => :children,
    "consistent_projection" => :consistent_projection,
    "correlation_id" => :correlation_id,
    "delay_ms" => :delay_ms,
    "drop_values" => :drop_values,
    "due_after_ms" => :due_after_ms,
    "error" => :error,
    "error_ref" => :error_ref,
    "exhaust_to" => :exhaust_to,
    "failure" => :failure,
    "fencing_token" => :fencing_token,
    "from_state" => :from_state,
    "full" => :full,
    "group_id" => :group_id,
    "history_hot_max_events" => :history_hot_max_events,
    "history_max_events" => :history_max_events,
    "id" => :id,
    "independent" => :independent,
    "lease_ms" => :lease_ms,
    "lease_token" => :lease_token,
    "limit" => :limit,
    "local_cache" => :local_cache,
    "max_attempts" => :max_attempts,
    "max_bytes" => :max_bytes,
    "max_ms" => :max_ms,
    "max_retries" => :max_retries,
    "name" => :name,
    "now_ms" => :now_ms,
    "on_child_failed" => :on_child_failed,
    "on_parent_closed" => :on_parent_closed,
    "override" => :override,
    "override_values" => :override_values,
    "owner_flow_id" => :owner_flow_id,
    "parent_id" => :parent_id,
    "partition_key" => :partition_key,
    "partition_keys" => :partition_keys,
    "payload" => :payload,
    "payload_max_bytes" => :payload_max_bytes,
    "payload_ref" => :payload_ref,
    "payload_refs" => :payload_refs,
    "priority" => :priority,
    "reason" => :reason,
    "reclaim_expired" => :reclaim_expired,
    "reclaim_ratio" => :reclaim_ratio,
    "result" => :result,
    "result_ref" => :result_ref,
    "return" => :return,
    "retention_ttl_ms" => :retention_ttl_ms,
    "retry_at_ms" => :retry_at_ms,
    "root_id" => :root_id,
    "run_at_ms" => :run_at_ms,
    "state" => :state,
    "states" => :states,
    "success" => :success,
    "to_state" => :to_state,
    "ttl_ms" => :ttl_ms,
    "type" => :type,
    "value" => :value,
    "value_max_bytes" => :value_max_bytes,
    "value_refs" => :value_refs,
    "values" => :values,
    "wait" => :wait,
    "wait_state" => :wait_state,
    "worker" => :worker
  }

  @atom_values %{
    "all" => :all,
    "exponential" => :exponential,
    "linear" => :linear,
    "none" => :none,
    "running" => :running
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

    case check_deadline(payload) do
      :ok -> do_execute(opcode, payload, state)
      {:error, status, reason} -> {status, reason, state}
    end
  rescue
    error ->
      {:error, Exception.message(error), state}
  end

  def execute(opcode, payload, state) do
    payload = normalize_payload(payload)

    with {:ok, command} <- fetch_command(opcode),
         :ok <- check_deadline(payload),
         :ok <- authorize(command, opcode, payload, state) do
      do_execute(opcode, payload, state)
    else
      {:error, status, reason} -> {status, reason, state}
    end
  rescue
    error ->
      {:error, Exception.message(error), state}
  end

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
    ConnAuth.user_requires_auth?("default") or requirepass_configured?()
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
        |> maybe_subscribe_events(Map.get(payload, "events", []))

      {:ok, hello_payload(state), state}
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp do_execute(@op_auth, payload, state) do
    username = Map.get(payload, "username", "default")
    password = Map.get(payload, "password")

    if is_binary(username) and is_binary(password) do
      do_auth(username, password, state)
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

  defp do_execute(@op_batch, payload, state) do
    with {:ok, commands} <- batch_commands(payload),
         {:ok, atomicity} <- batch_atomicity(payload),
         :ok <- validate_batch_atomicity(commands, atomicity, state) do
      {results, state} =
        Enum.map_reduce(commands, state, fn command, acc_state ->
          execute_batch_command(command, acc_state)
        end)

      {:ok, results, state}
    else
      {:error, reason} -> {:bad_request, reason, state}
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
      result_to_reply(FerricStore.Impl.mget(state.instance_ctx, keys), state)
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
      result_to_reply(Ferricstore.Metrics.handle("FERRICSTORE.METRICS", args), state)
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
    do: flow_id_opts_call(payload, state, &FerricStore.Impl.flow_get/3)

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
    do: flow_type_opts_call(payload, state, &FerricStore.Impl.flow_list/3)

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

  defp flow_lease_call(payload, state, fun) do
    with {:ok, id} <- require_binary(payload, "id"),
         {:ok, lease_token} <- require_binary(payload, "lease_token"),
         {:ok, opts} <- flow_opts(payload, ["id", "lease_token"]) do
      result_to_reply(fun.(state.instance_ctx, id, lease_token, opts), state)
    else
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

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

  defp check_key_acl_cached(:full_access, _opcode, _payload), do: :ok
  defp check_key_acl_cached(%{keys: :all}, _opcode, _payload), do: :ok

  defp check_key_acl_cached(cache, opcode, payload) do
    ConnAuth.check_keys_cached(cache, key_acl_command(opcode), keys(opcode, payload))
  end

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
              @op_cluster_keyslot,
              @op_ferricstore_key_info
            ],
       do: binary_list([Map.get(payload, "key")])

  defp keys(@op_route_batch, payload), do: binary_list(Map.get(payload, "keys", []))

  defp keys(opcode, payload) when opcode in [@op_del, @op_mget],
    do: binary_list(Map.get(payload, "keys", []))

  defp keys(@op_mset, payload),
    do: payload |> Map.get("pairs", []) |> Enum.map(&pair_key/1) |> binary_list()

  defp keys(opcode, payload)
       when opcode in [
              @op_flow_create,
              @op_flow_get,
              @op_flow_complete,
              @op_flow_transition,
              @op_flow_retry,
              @op_flow_fail,
              @op_flow_cancel,
              @op_flow_extend_lease,
              @op_flow_history,
              @op_flow_signal,
              @op_flow_rewind,
              @op_flow_spawn_children
            ],
       do: binary_list([Map.get(payload, "id")])

  defp keys(opcode, payload)
       when opcode in [
              @op_flow_create_many,
              @op_flow_complete_many,
              @op_flow_transition_many,
              @op_flow_retry_many,
              @op_flow_fail_many,
              @op_flow_cancel_many
            ],
       do: flow_item_ids(payload)

  defp keys(@op_flow_value_put, payload),
    do: binary_list([Map.get(payload, "owner_flow_id") || Map.get(payload, "id")])

  defp keys(_opcode, _payload), do: []

  defp key_acl_command(opcode)
       when opcode in [
              @op_get,
              @op_mget,
              @op_route,
              @op_flow_get,
              @op_flow_history,
              @op_flow_list,
              @op_flow_value_mget,
              @op_flow_policy_get,
              @op_cluster_keyslot,
              @op_ferricstore_key_info
            ],
       do: "GET"

  defp key_acl_command(_opcode), do: "SET"

  defp pair_key(%{"key" => key}), do: key
  defp pair_key([key, _value]) when is_binary(key), do: key
  defp pair_key({key, _value}) when is_binary(key), do: key
  defp pair_key(_), do: nil

  defp binary_list(values) when is_list(values), do: Enum.filter(values, &is_binary/1)
  defp binary_list(_), do: []

  defp flow_item_ids(%{"items" => items}) when is_list(items) do
    items
    |> Enum.map(fn
      %{"id" => id} -> id
      %{id: id} -> id
      [id | _rest] -> id
      {id, _payload} -> id
      {id, _partition_key, _payload} -> id
      {id, _partition_key, _lease_token, _fencing_token} -> id
      _ -> nil
    end)
    |> binary_list()
  end

  defp flow_item_ids(_payload), do: []

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
    %{
      protocol: "ferricstore-native",
      version: 1,
      compression: Atom.to_string(Map.get(state, :compression, :none)),
      client_id: state.client_id,
      route_epoch: route_epoch(),
      capabilities: capabilities_payload(state),
      server: server_payload(),
      route: %{
        slots: SlotMap.num_slots(),
        shard_count: state.instance_ctx.shard_count,
        native_port: Application.get_env(:ferricstore, :native_port, 6388),
        resp_port: Application.get_env(:ferricstore, :port, 6379)
      },
      auth_required: state.require_auth and not state.authenticated,
      backpressure: backpressure_payload()
    }
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
        max_lane_queue:
          Map.get(state, :lane_max_queue) ||
            Application.get_env(:ferricstore, :native_lane_max_queue, 1024),
        max_batch_commands: Application.get_env(:ferricstore, :native_max_batch_commands, 1024)
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
          "partition_key",
          "parent_id",
          "root_id",
          "correlation_id",
          "run_at_ms",
          "due_after_ms",
          "priority",
          "retention_ttl_ms",
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
          "delay_ms",
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
          "now_ms",
          "deadline_ms"
        ]
      },
      "FLOW.RETENTION_CLEANUP" => %{
        "required" => [],
        "fields" => ["limit", "now_ms", "deadline_ms"]
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

    %{
      key: key,
      slot: slot,
      shard: shard,
      lane_id: shard + 1,
      route_epoch: route_epoch(),
      owner_node: Atom.to_string(node()),
      native_port: Application.get_env(:ferricstore, :native_port, 6388),
      resp_port: Application.get_env(:ferricstore, :port, 6379),
      hint: "local"
    }
  end

  defp shards_payload(ctx) do
    ranges =
      ctx.slot_map
      |> SlotMap.slot_ranges()
      |> Enum.map(fn {first, last, shard} ->
        %{
          first_slot: first,
          last_slot: last,
          shard: shard,
          lane_id: shard + 1,
          owner_node: Atom.to_string(node()),
          native_port: Application.get_env(:ferricstore, :native_port, 6388),
          resp_port: Application.get_env(:ferricstore, :port, 6379)
        }
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

    cond do
      not has_acl_password and not has_requirepass ->
        auth_failure(username, state)

        {:auth,
         "ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?",
         state}

      has_acl_password ->
        acl_auth(username, password, state)

      username == "default" and constant_time_equal?(password, requirepass) ->
        complete_auth(username, state)

      username == "default" ->
        auth_failure(username, state)
        {:auth, "WRONGPASS invalid username-password pair or user is disabled.", state}

      true ->
        acl_auth(username, password, state)
    end
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

  defp requirepass_configured? do
    case requirepass() do
      nil -> false
      "" -> false
      _ -> true
    end
  end

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
    case Map.get(payload, key) do
      values when is_list(values) -> require_all_binaries(values, key)
      _ -> {:error, "ERR native field #{key} must be a binary list"}
    end
  end

  defp require_all_binaries(values, key) do
    if Enum.all?(values, &is_binary/1) do
      {:ok, values}
    else
      {:error, "ERR native field #{key} must be a binary list"}
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

  defp kv_pairs(%{"pairs" => pairs}) when is_list(pairs) do
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

  defp kv_pairs(_payload), do: {:error, "ERR native field pairs must be a list"}

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

  defp maybe_subscribe_events(state, []), do: state

  defp maybe_subscribe_events(state, events) when is_list(events) do
    case event_list(%{"events" => events}) do
      {:ok, normalized} -> subscribe_events(state, normalized)
      {:error, _reason} -> state
    end
  end

  defp maybe_subscribe_events(state, _events), do: state

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

  defp batch_commands(%{"commands" => commands}) when is_list(commands) do
    max = Application.get_env(:ferricstore, :native_max_batch_commands, 1024)

    cond do
      length(commands) > max ->
        {:error, "ERR native batch exceeds max commands"}

      true ->
        commands
        |> Enum.reduce_while({:ok, []}, fn
          %{"opcode" => opcode, "body" => body} = command, {:ok, acc}
          when is_integer(opcode) and is_map(body) ->
            lane_id = Map.get(command, "lane_id", 0)
            request_id = Map.get(command, "request_id", 0)

            if control_opcode?(opcode) do
              {:halt, {:error, "ERR native batch cannot contain control commands"}}
            else
              {:cont,
               {:ok,
                [%{opcode: opcode, body: body, lane_id: lane_id, request_id: request_id} | acc]}}
            end

          _command, _acc ->
            {:halt, {:error, "ERR native batch commands require opcode and body"}}
        end)
        |> case do
          {:ok, values} -> {:ok, Enum.reverse(values)}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp batch_commands(_payload), do: {:error, "ERR native batch requires commands list"}

  defp batch_atomicity(payload) do
    atomicity = Map.get(payload, "atomicity", "none")

    if atomicity in @supported_atomicity do
      {:ok, atomicity}
    else
      {:error, "ERR native unsupported batch atomicity #{atomicity}"}
    end
  end

  defp validate_batch_atomicity(_commands, atomicity, _state)
       when atomicity in ["none", "per_shard"],
       do: :ok

  defp validate_batch_atomicity(commands, "same_shard", state) do
    shards =
      commands
      |> Enum.flat_map(fn command -> keys(command.opcode, command.body) end)
      |> Enum.map(&Router.shard_for(state.instance_ctx, &1))
      |> Enum.uniq()

    case shards do
      [] -> :ok
      [_one] -> :ok
      _ -> {:error, "ERR native same_shard batch contains multiple shards"}
    end
  end

  defp execute_batch_command(command, state) do
    {status, value, state} = execute(command.opcode, command.body, state)

    result = %{
      "opcode" => command.opcode,
      "request_id" => command.request_id,
      "lane_id" => command.lane_id,
      "status" => Atom.to_string(status),
      "value" => value
    }

    {result, state}
  end

  defp flow_opts(payload, drop_keys) do
    opts =
      payload
      |> Map.drop(["opts", "deadline_ms" | drop_keys])
      |> Map.merge(option_map(Map.get(payload, "opts", %{})))

    to_flow_opts(opts)
  end

  defp flow_items(payload, key, item_kind) do
    case Map.get(payload, key) do
      items when is_list(items) ->
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

      _ ->
        {:error, "ERR native field #{key} must be a list"}
    end
  end

  defp flow_item(item, _item_kind) when is_map(item), do: flow_item_map(item)

  defp flow_item([id, payload], :create) when is_binary(id),
    do: {:ok, {:id, id, :payload, payload}}

  defp flow_item([id, partition_key, payload], :create) when is_binary(id),
    do: {:ok, {:id, id, :partition_key, partition_key, :payload, payload}}

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
       do: Map.get(@atom_values, value, value)

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
