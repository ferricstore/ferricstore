defmodule FerricStore.SDK.Native.Opcodes do
  @moduledoc false

  @opcodes [
    hello: {"HELLO", 0x0001},
    auth: {"AUTH", 0x0002},
    ping: {"PING", 0x0003},
    client_set_name: {"CLIENT.SETNAME", 0x0004},
    client_info: {"CLIENT.INFO", 0x0005},
    route: {"ROUTE", 0x0006},
    shards: {"SHARDS", 0x0007},
    backpressure: {"BACKPRESSURE", 0x0008},
    quit: {"QUIT", 0x0009},
    options: {"OPTIONS", 0x000B},
    startup: {"STARTUP", 0x000C},
    window_update: {"WINDOW_UPDATE", 0x000D},
    pipeline: {"PIPELINE", 0x000E},
    route_batch: {"ROUTE_BATCH", 0x000F},
    subscribe_events: {"SUBSCRIBE_EVENTS", 0x0011},
    unsubscribe_events: {"UNSUBSCRIBE_EVENTS", 0x0012},
    command_exec: {"COMMAND_EXEC", 0x0100},
    get: {"GET", 0x0101},
    set: {"SET", 0x0102},
    del: {"DEL", 0x0103},
    mget: {"MGET", 0x0104},
    mset: {"MSET", 0x0105},
    cas: {"CAS", 0x0106},
    lock: {"LOCK", 0x0107},
    unlock: {"UNLOCK", 0x0108},
    extend: {"EXTEND", 0x0109},
    ratelimit_add: {"RATELIMIT.ADD", 0x010A},
    fetch_or_compute: {"FETCH_OR_COMPUTE", 0x010B},
    fetch_or_compute_result: {"FETCH_OR_COMPUTE_RESULT", 0x010C},
    fetch_or_compute_error: {"FETCH_OR_COMPUTE_ERROR", 0x010D},
    hset: {"HSET", 0x0110},
    hget: {"HGET", 0x0111},
    hmget: {"HMGET", 0x0112},
    hgetall: {"HGETALL", 0x0113},
    lpush: {"LPUSH", 0x0120},
    rpush: {"RPUSH", 0x0121},
    lpop: {"LPOP", 0x0122},
    rpop: {"RPOP", 0x0123},
    lrange: {"LRANGE", 0x0124},
    sadd: {"SADD", 0x0130},
    srem: {"SREM", 0x0131},
    smembers: {"SMEMBERS", 0x0132},
    sismember: {"SISMEMBER", 0x0133},
    zadd: {"ZADD", 0x0140},
    zrem: {"ZREM", 0x0141},
    zrange: {"ZRANGE", 0x0142},
    zscore: {"ZSCORE", 0x0143},
    flow_create: {"FLOW.CREATE", 0x0201},
    flow_get: {"FLOW.GET", 0x0202},
    flow_claim_due: {"FLOW.CLAIM_DUE", 0x0203},
    flow_complete: {"FLOW.COMPLETE", 0x0204},
    flow_transition: {"FLOW.TRANSITION", 0x0205},
    flow_retry: {"FLOW.RETRY", 0x0206},
    flow_fail: {"FLOW.FAIL", 0x0207},
    flow_cancel: {"FLOW.CANCEL", 0x0208},
    flow_extend_lease: {"FLOW.EXTEND_LEASE", 0x0209},
    flow_history: {"FLOW.HISTORY", 0x020A},
    flow_value_put: {"FLOW.VALUE.PUT", 0x020B},
    flow_value_mget: {"FLOW.VALUE.MGET", 0x020C},
    flow_signal: {"FLOW.SIGNAL", 0x020D},
    flow_list: {"FLOW.LIST", 0x020E},
    flow_create_many: {"FLOW.CREATE_MANY", 0x020F},
    flow_complete_many: {"FLOW.COMPLETE_MANY", 0x0210},
    flow_transition_many: {"FLOW.TRANSITION_MANY", 0x0211},
    flow_retry_many: {"FLOW.RETRY_MANY", 0x0212},
    flow_fail_many: {"FLOW.FAIL_MANY", 0x0213},
    flow_cancel_many: {"FLOW.CANCEL_MANY", 0x0214},
    flow_reclaim: {"FLOW.RECLAIM", 0x0215},
    flow_rewind: {"FLOW.REWIND", 0x0216},
    flow_terminals: {"FLOW.TERMINALS", 0x0217},
    flow_failures: {"FLOW.FAILURES", 0x0218},
    flow_by_parent: {"FLOW.BY_PARENT", 0x0219},
    flow_by_root: {"FLOW.BY_ROOT", 0x021A},
    flow_by_correlation: {"FLOW.BY_CORRELATION", 0x021B},
    flow_info: {"FLOW.INFO", 0x021C},
    flow_stuck: {"FLOW.STUCK", 0x021D},
    flow_policy_set: {"FLOW.POLICY.SET", 0x021E},
    flow_policy_get: {"FLOW.POLICY.GET", 0x021F},
    flow_spawn_children: {"FLOW.SPAWN_CHILDREN", 0x0220},
    flow_retention_cleanup: {"FLOW.RETENTION_CLEANUP", 0x0221},
    flow_step_continue: {"FLOW.STEP_CONTINUE", 0x0222},
    flow_start_and_claim: {"FLOW.START_AND_CLAIM", 0x0223},
    flow_run_steps_many: {"FLOW.RUN_STEPS_MANY", 0x0224},
    flow_schedule_create: {"FLOW.SCHEDULE.CREATE", 0x0225},
    flow_schedule_get: {"FLOW.SCHEDULE.GET", 0x0226},
    flow_schedule_delete: {"FLOW.SCHEDULE.DELETE", 0x0227},
    flow_schedule_fire_due: {"FLOW.SCHEDULE.FIRE_DUE", 0x0228},
    flow_schedule_list: {"FLOW.SCHEDULE.LIST", 0x0229},
    flow_schedule_fire: {"FLOW.SCHEDULE.FIRE", 0x022A},
    flow_schedule_pause: {"FLOW.SCHEDULE.PAUSE", 0x022B},
    flow_schedule_resume: {"FLOW.SCHEDULE.RESUME", 0x022C},
    flow_stats: {"FLOW.STATS", 0x022D},
    flow_attributes: {"FLOW.ATTRIBUTES", 0x022E},
    flow_attribute_values: {"FLOW.ATTRIBUTE_VALUES", 0x022F},
    flow_effect_reserve: {"FLOW.EFFECT.RESERVE", 0x0240},
    flow_effect_confirm: {"FLOW.EFFECT.CONFIRM", 0x0241},
    flow_effect_fail: {"FLOW.EFFECT.FAIL", 0x0242},
    flow_effect_compensate: {"FLOW.EFFECT.COMPENSATE", 0x0243},
    flow_effect_get: {"FLOW.EFFECT.GET", 0x0244},
    flow_governance_ledger: {"FLOW.GOVERNANCE.LEDGER", 0x0245},
    flow_approval_request: {"FLOW.APPROVAL.REQUEST", 0x0246},
    flow_approval_approve: {"FLOW.APPROVAL.APPROVE", 0x0247},
    flow_approval_reject: {"FLOW.APPROVAL.REJECT", 0x0248},
    flow_approval_get: {"FLOW.APPROVAL.GET", 0x0249},
    flow_circuit_open: {"FLOW.CIRCUIT.OPEN", 0x024A},
    flow_circuit_close: {"FLOW.CIRCUIT.CLOSE", 0x024B},
    flow_circuit_get: {"FLOW.CIRCUIT.GET", 0x024C},
    flow_budget_reserve: {"FLOW.BUDGET.RESERVE", 0x024D},
    flow_budget_get: {"FLOW.BUDGET.GET", 0x024E},
    flow_limit_lease: {"FLOW.LIMIT.LEASE", 0x024F},
    flow_limit_spend: {"FLOW.LIMIT.SPEND", 0x0250},
    flow_limit_release: {"FLOW.LIMIT.RELEASE", 0x0251},
    flow_limit_get: {"FLOW.LIMIT.GET", 0x0252},
    flow_approval_list: {"FLOW.APPROVAL.LIST", 0x0253},
    flow_governance_overview: {"FLOW.GOVERNANCE.OVERVIEW", 0x0254},
    flow_budget_list: {"FLOW.BUDGET.LIST", 0x0255},
    flow_limit_list: {"FLOW.LIMIT.LIST", 0x0256},
    flow_budget_commit: {"FLOW.BUDGET.COMMIT", 0x0257},
    flow_budget_release: {"FLOW.BUDGET.RELEASE", 0x0258},
    cluster_health: {"CLUSTER.HEALTH", 0x0301},
    cluster_stats: {"CLUSTER.STATS", 0x0302},
    cluster_keyslot: {"CLUSTER.KEYSLOT", 0x0303},
    cluster_slots: {"CLUSTER.SLOTS", 0x0304},
    cluster_status: {"CLUSTER.STATUS", 0x0305},
    cluster_join: {"CLUSTER.JOIN", 0x0306},
    cluster_leave: {"CLUSTER.LEAVE", 0x0307},
    cluster_failover: {"CLUSTER.FAILOVER", 0x0308},
    cluster_promote: {"CLUSTER.PROMOTE", 0x0309},
    cluster_demote: {"CLUSTER.DEMOTE", 0x030A},
    cluster_role: {"CLUSTER.ROLE", 0x030B},
    ferricstore_key_info: {"FERRICSTORE.KEY_INFO", 0x030C},
    ferricstore_config: {"FERRICSTORE.CONFIG", 0x030D},
    ferricstore_hotness: {"FERRICSTORE.HOTNESS", 0x030E},
    ferricstore_metrics: {"FERRICSTORE.METRICS", 0x030F},
    ferricstore_blobgc: {"FERRICSTORE.BLOBGC", 0x0310}
  ]

  @by_atom Map.new(@opcodes, fn {atom, {_name, opcode}} -> {atom, opcode} end)
  @by_name Map.new(@opcodes, fn {atom, {name, opcode}} -> {name, {atom, opcode}} end)
  @by_atom_name Map.new(@opcodes, fn {atom, {_name, opcode}} ->
                  {atom |> Atom.to_string() |> String.upcase(), opcode}
                end)
  @names_by_opcode Map.new(@opcodes, fn {_atom, {name, opcode}} -> {opcode, name} end)

  for {atom, {_name, opcode}} <- @opcodes do
    def unquote(atom)(), do: unquote(opcode)
  end

  @spec fetch(non_neg_integer() | atom() | binary()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def fetch(opcode) when is_integer(opcode) and opcode >= 0 and opcode <= 0xFFFF,
    do: {:ok, opcode}

  def fetch(opcode) when is_atom(opcode) do
    case Map.fetch(@by_atom, opcode) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:unknown_opcode, opcode}}
    end
  end

  def fetch(name) when is_binary(name) do
    normalized =
      name
      |> String.trim()
      |> String.upcase()

    with :error <- Map.fetch(@by_name, normalized),
         :error <- Map.fetch(@by_atom_name, normalized |> String.replace(".", "_")) do
      {:error, {:unknown_opcode, name}}
    else
      {:ok, {_atom, opcode}} -> {:ok, opcode}
      {:ok, opcode} -> {:ok, opcode}
    end
  end

  @spec fetch!(non_neg_integer() | atom() | binary()) :: non_neg_integer()
  def fetch!(opcode) do
    case fetch(opcode) do
      {:ok, value} -> value
      {:error, reason} -> raise ArgumentError, inspect(reason)
    end
  end

  @spec name(non_neg_integer()) :: binary() | nil
  def name(opcode), do: Map.get(@names_by_opcode, opcode)

  @spec all() :: map()
  def all, do: @by_atom
end
