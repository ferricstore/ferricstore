defmodule FerricStore.SDK.Flow do
  @moduledoc """
  Native FerricStore Flow commands.

  Functions accept the native typed-map payload documented by the protocol.
  Id-addressed payloads are routed to the relevant shard leader; keyless
  aggregate commands use the control connection.
  """

  alias FerricStore.SDK.Native.{Client, Opcodes}

  @flow_commands [
    create: :flow_create,
    get: :flow_get,
    claim_due: :flow_claim_due,
    complete: :flow_complete,
    transition: :flow_transition,
    retry: :flow_retry,
    fail: :flow_fail,
    cancel: :flow_cancel,
    extend_lease: :flow_extend_lease,
    history: :flow_history,
    value_put: :flow_value_put,
    value_mget: :flow_value_mget,
    signal: :flow_signal,
    list: :flow_list,
    create_many: :flow_create_many,
    complete_many: :flow_complete_many,
    transition_many: :flow_transition_many,
    retry_many: :flow_retry_many,
    fail_many: :flow_fail_many,
    cancel_many: :flow_cancel_many,
    reclaim: :flow_reclaim,
    rewind: :flow_rewind,
    terminals: :flow_terminals,
    failures: :flow_failures,
    by_parent: :flow_by_parent,
    by_root: :flow_by_root,
    by_correlation: :flow_by_correlation,
    info: :flow_info,
    stuck: :flow_stuck,
    policy_set: :flow_policy_set,
    policy_get: :flow_policy_get,
    spawn_children: :flow_spawn_children,
    retention_cleanup: :flow_retention_cleanup,
    step_continue: :flow_step_continue,
    start_and_claim: :flow_start_and_claim,
    run_steps_many: :flow_run_steps_many,
    schedule_create: :flow_schedule_create,
    schedule_get: :flow_schedule_get,
    schedule_delete: :flow_schedule_delete,
    schedule_fire_due: :flow_schedule_fire_due,
    schedule_list: :flow_schedule_list,
    schedule_fire: :flow_schedule_fire,
    schedule_pause: :flow_schedule_pause,
    schedule_resume: :flow_schedule_resume,
    stats: :flow_stats,
    attributes: :flow_attributes,
    attribute_values: :flow_attribute_values,
    search: :flow_search,
    effect_reserve: :flow_effect_reserve,
    effect_confirm: :flow_effect_confirm,
    effect_fail: :flow_effect_fail,
    effect_compensate: :flow_effect_compensate,
    effect_get: :flow_effect_get,
    governance_ledger: :flow_governance_ledger,
    approval_request: :flow_approval_request,
    approval_approve: :flow_approval_approve,
    approval_reject: :flow_approval_reject,
    approval_get: :flow_approval_get,
    circuit_open: :flow_circuit_open,
    circuit_close: :flow_circuit_close,
    circuit_get: :flow_circuit_get,
    budget_reserve: :flow_budget_reserve,
    budget_get: :flow_budget_get,
    limit_lease: :flow_limit_lease,
    limit_spend: :flow_limit_spend,
    limit_release: :flow_limit_release,
    limit_get: :flow_limit_get,
    approval_list: :flow_approval_list,
    governance_overview: :flow_governance_overview,
    budget_list: :flow_budget_list,
    limit_list: :flow_limit_list,
    budget_commit: :flow_budget_commit,
    budget_release: :flow_budget_release
  ]

  @key_fields [
    "partition_key",
    "id",
    "owner_flow_id",
    "parent_id",
    "root_id",
    "correlation_id",
    "scope"
  ]

  for {function, opcode_name} <- @flow_commands do
    def unquote(function)(client, payload \\ %{}, opts \\ []) when is_map(payload) do
      request(client, unquote(opcode_name), payload, opts)
    end
  end

  @spec request(GenServer.server(), non_neg_integer() | atom() | binary(), map(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def request(client, opcode, payload, opts \\ []) when is_map(payload) do
    payload = stringify_keys(payload)

    case route_key(payload, opts) do
      key when is_binary(key) -> Client.request_by_key(client, opcode, key, payload, opts)
      nil -> Client.request(client, opcode, payload, opts)
    end
  end

  @spec opcodes() :: map()
  def opcodes do
    Map.new(@flow_commands, fn {function, opcode_name} ->
      {function, Opcodes.fetch!(opcode_name)}
    end)
  end

  defp route_key(payload, opts) do
    case Keyword.get(opts, :route_key) do
      key when is_binary(key) -> key
      _other -> Enum.find_value(@key_fields, &payload[&1])
    end
  end

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), stringify_keys(value)}
      {key, value} -> {key, stringify_keys(value)}
    end)
  end

  defp stringify_keys(values) when is_list(values), do: Enum.map(values, &stringify_keys/1)
  defp stringify_keys(value), do: value
end
