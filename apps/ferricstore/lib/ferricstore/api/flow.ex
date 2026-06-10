defmodule FerricStore.API.Flow do
  @moduledoc false

  defp default_ctx do
    FerricStore.Instance.get(:default)
  end

  @doc """
  Creates a durable Flow record.

  Required option: `:type`.
  Common options: `:state`, `:payload`, `:run_at_ms`, `:priority`.
  When `:payload` is provided, Flow stores the value internally and returns a
  generated `:payload_ref`.
  """
  @spec flow_create(binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_create(id, opts) when is_binary(id) and is_list(opts) do
    Ferricstore.Flow.create(default_ctx(), id, opts)
  end

  def flow_create(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_create(_id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Stores a reusable Flow value and returns a `:ref` that can be passed as
  `:payload_ref` to Flow create/transition commands.
  """
  @spec flow_value_put(term(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_value_put(value, opts \\ [])

  def flow_value_put(value, opts) when is_list(opts) do
    Ferricstore.Flow.value_put(default_ctx(), value, opts)
  end

  def flow_value_put(_value, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Reads reusable Flow values by reference, preserving request order.
  """
  @spec flow_value_mget([binary()], keyword()) :: {:ok, [term()]} | {:error, binary()}
  def flow_value_mget(refs, opts \\ [])

  def flow_value_mget(refs, opts) when is_list(refs) and is_list(opts) do
    Ferricstore.Flow.value_mget(default_ctx(), refs, opts)
  end

  def flow_value_mget(_refs, opts) when not is_list(opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def flow_value_mget(_refs, _opts), do: {:error, "ERR flow refs must be a list"}

  @doc """
  Records an external Flow signal and optionally attaches named values or moves
  the Flow through a guarded state transition.
  """
  @spec flow_signal(binary(), keyword()) :: :ok | {:error, binary()}
  def flow_signal(id, opts) when is_binary(id) and is_list(opts) do
    Ferricstore.Flow.signal(default_ctx(), id, opts)
  end

  def flow_signal(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_signal(_id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Creates a durable batch of Flow records.

  When `partition_key` is set, the batch is all-or-nothing because every item
  routes to the same shard. When `partition_key` is `nil`, each item must carry
  `:partition_key`; items are grouped by shard and each shard group is atomic.
  Required option: `:type`.
  """
  @spec flow_create_many(binary() | nil, list(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_create_many(partition_key, items, opts \\ [])

  def flow_create_many(partition_key, items, opts) when is_list(items) and is_list(opts) do
    Ferricstore.Flow.create_many(default_ctx(), partition_key, items, opts)
  end

  def flow_create_many(_partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Atomically creates child Flow records under `parent_id`.

  v1 requires a single `partition_key`, so parent and children are coordinated by
  one shard. `wait: :none` advances the parent immediately to
  `exhaust_to.success`; `wait: :all` keeps the parent in `wait_state` until all
  direct children are terminal.
  """
  @spec flow_spawn_children(binary(), list(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_spawn_children(parent_id, children, opts \\ [])

  def flow_spawn_children(parent_id, children, opts)
      when is_binary(parent_id) and is_list(children) and is_list(opts) do
    Ferricstore.Flow.spawn_children(default_ctx(), parent_id, children, opts)
  end

  def flow_spawn_children(parent_id, _children, _opts) when not is_binary(parent_id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_spawn_children(_parent_id, children, _opts) when not is_list(children),
    do: {:error, "ERR flow children must be a non-empty list"}

  def flow_spawn_children(_parent_id, _children, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Returns the latest Flow state record for `id`.

  By default this returns metadata and value references only. Pass `full: true`
  or `payload: true` to hydrate the current payload/result/error values from
  internal storage up to `:payload_max_bytes` (default
  `:flow_payload_return_max_bytes`, 64 KiB). Larger values return
  `:payload_omitted`/`:result_omitted`/`:error_omitted` with the stored size.
  """
  @spec flow_get(binary(), keyword()) :: {:ok, map() | nil} | {:error, binary()}
  def flow_get(id, opts \\ [])

  def flow_get(id, opts) when is_binary(id) and is_list(opts) do
    Ferricstore.Flow.get(default_ctx(), id, opts)
  end

  def flow_get(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_get(_id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Stores retry/backpressure policy defaults for a Flow type.

  Command-local retry policy still wins over these defaults.
  """
  @spec flow_policy_set(binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_policy_set(type, opts) when is_binary(type) and is_list(opts) do
    Ferricstore.Flow.policy_set(default_ctx(), type, opts)
  end

  def flow_policy_set(type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_policy_set(_type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Returns effective retry/backpressure policy for a Flow type or state.
  """
  @spec flow_policy_get(binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_policy_get(type, opts \\ [])

  def flow_policy_get(type, opts) when is_binary(type) and is_list(opts) do
    Ferricstore.Flow.policy_get(default_ctx(), type, opts)
  end

  def flow_policy_get(type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_policy_get(_type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Claims due Flow records for a type.

  Required option: `:worker`.
  Common options: `:state`, `:lease_ms`, `:limit`, `:priority`, `:now_ms`.
  Claimed records include payload values by default using the same
  `:payload_max_bytes` cap as `flow_get/2`; pass `payload: false` to return only
  metadata and references. Payload fetch failures or missing payload refs do not
  roll back the claim.
  """
  @spec flow_claim_due(binary(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_claim_due(type, opts) when is_binary(type) and is_list(opts) do
    Ferricstore.Flow.claim_due(default_ctx(), type, opts)
  end

  def flow_claim_due(type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_claim_due(_type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Reclaims expired running Flow leases for a type.

  This is equivalent to `flow_claim_due(type, state: "running", ...)` and keeps
  lease fencing/atomicity identical to normal claims.
  """
  @spec flow_reclaim(binary(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_reclaim(type, opts) when is_binary(type) and is_list(opts) do
    Ferricstore.Flow.reclaim(default_ctx(), type, opts)
  end

  def flow_reclaim(type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_reclaim(_type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Extends a running Flow lease when `lease_token` and `fencing_token` match."
  @spec flow_extend_lease(binary(), binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_extend_lease(id, lease_token, opts \\ [])

  def flow_extend_lease(id, lease_token, opts)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    Ferricstore.Flow.extend_lease(default_ctx(), id, lease_token, opts)
  end

  def flow_extend_lease(id, _lease_token, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_extend_lease(_id, lease_token, _opts) when not is_binary(lease_token),
    do: {:error, "ERR flow lease_token must be a string"}

  def flow_extend_lease(_id, _lease_token, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Completes a claimed Flow record when `lease_token` matches."
  @spec flow_complete(binary(), binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_complete(id, lease_token, opts \\ [])

  def flow_complete(id, lease_token, opts)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    Ferricstore.Flow.complete(default_ctx(), id, lease_token, opts)
  end

  def flow_complete(id, _lease_token, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_complete(_id, lease_token, _opts) when not is_binary(lease_token),
    do: {:error, "ERR flow lease_token must be a string"}

  def flow_complete(_id, _lease_token, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Completes a batch of claimed Flow records.

  When `partition_key` is set, the batch is all-or-nothing because every item
  routes to the same shard. When `partition_key` is `nil`, each item must carry
  `:partition_key`; items are grouped by shard and each shard group is atomic.
  Each item must provide `:id`, `:lease_token`, and `:fencing_token`.
  """
  @spec flow_complete_many(binary() | nil, list(), keyword()) ::
          {:ok, [map()]} | {:error, binary()}
  def flow_complete_many(partition_key, items, opts \\ [])

  def flow_complete_many(partition_key, items, opts) when is_list(items) and is_list(opts) do
    Ferricstore.Flow.complete_many(default_ctx(), partition_key, items, opts)
  end

  def flow_complete_many(_partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Moves a Flow record from one state to another, optionally guarded by a lease token."
  @spec flow_transition(binary(), binary(), binary(), keyword()) ::
          {:ok, map()} | {:error, binary()}
  def flow_transition(id, from_state, to_state, opts \\ [])

  def flow_transition(id, from_state, to_state, opts)
      when is_binary(id) and is_binary(from_state) and is_binary(to_state) and is_list(opts) do
    Ferricstore.Flow.transition(default_ctx(), id, from_state, to_state, opts)
  end

  def flow_transition(id, _from_state, _to_state, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_transition(_id, from_state, _to_state, _opts) when not is_binary(from_state),
    do: {:error, "ERR flow from must be a non-empty string"}

  def flow_transition(_id, _from_state, to_state, _opts) when not is_binary(to_state),
    do: {:error, "ERR flow to must be a non-empty string"}

  def flow_transition(_id, _from_state, _to_state, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Moves a batch of Flow records from one state to another.

  When `partition_key` is set, the batch is all-or-nothing because every item
  routes to the same shard. When `partition_key` is `nil`, each item must carry
  `:partition_key`; items are grouped by shard and each shard group is atomic.
  Each item must provide `:id` and `:fencing_token`; `:lease_token` is optional.
  """
  @spec flow_transition_many(binary() | nil, binary(), binary(), list(), keyword()) ::
          {:ok, [map()]} | {:error, binary()}
  def flow_transition_many(partition_key, from_state, to_state, items, opts \\ [])

  def flow_transition_many(partition_key, from_state, to_state, items, opts)
      when is_binary(from_state) and is_binary(to_state) and is_list(items) and is_list(opts) do
    Ferricstore.Flow.transition_many(
      default_ctx(),
      partition_key,
      from_state,
      to_state,
      items,
      opts
    )
  end

  def flow_transition_many(_partition_key, from_state, _to_state, _items, _opts)
      when not is_binary(from_state),
      do: {:error, "ERR flow from must be a non-empty string"}

  def flow_transition_many(_partition_key, _from_state, to_state, _items, _opts)
      when not is_binary(to_state),
      do: {:error, "ERR flow to must be a non-empty string"}

  def flow_transition_many(_partition_key, _from_state, _to_state, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Clears a claim and reschedules a Flow record when `lease_token` matches.

  `:error` stores a retry error value. Optional `:payload` replaces the current
  payload; omitting `:payload` preserves the payload currently stored on the
  Flow record.
  """
  @spec flow_retry(binary(), binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_retry(id, lease_token, opts)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    Ferricstore.Flow.retry(default_ctx(), id, lease_token, opts)
  end

  def flow_retry(id, _lease_token, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_retry(_id, lease_token, _opts) when not is_binary(lease_token),
    do: {:error, "ERR flow lease_token must be a string"}

  def flow_retry(_id, _lease_token, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Clears claims and reschedules a batch of Flow records.

  When `partition_key` is set, the batch is all-or-nothing because every item
  routes to the same shard. When `partition_key` is `nil`, each item must carry
  `:partition_key`; items are grouped by shard and each shard group is atomic.
  Each item must provide `:id`, `:lease_token`, and `:fencing_token`.
  """
  @spec flow_retry_many(binary() | nil, list(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_retry_many(partition_key, items, opts \\ [])

  def flow_retry_many(partition_key, items, opts) when is_list(items) and is_list(opts) do
    Ferricstore.Flow.retry_many(default_ctx(), partition_key, items, opts)
  end

  def flow_retry_many(_partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Fails a running Flow record when `lease_token` matches."
  @spec flow_fail(binary(), binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_fail(id, lease_token, opts \\ [])

  def flow_fail(id, lease_token, opts)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    Ferricstore.Flow.fail(default_ctx(), id, lease_token, opts)
  end

  def flow_fail(id, _lease_token, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_fail(_id, lease_token, _opts) when not is_binary(lease_token),
    do: {:error, "ERR flow lease_token must be a string"}

  def flow_fail(_id, _lease_token, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Fails a batch of running Flow records.

  When `partition_key` is set, the batch is all-or-nothing because every item
  routes to the same shard. When `partition_key` is `nil`, each item must carry
  `:partition_key`; items are grouped by shard and each shard group is atomic.
  Each item must provide `:id`, `:lease_token`, and `:fencing_token`.
  """
  @spec flow_fail_many(binary() | nil, list(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_fail_many(partition_key, items, opts \\ [])

  def flow_fail_many(partition_key, items, opts) when is_list(items) and is_list(opts) do
    Ferricstore.Flow.fail_many(default_ctx(), partition_key, items, opts)
  end

  def flow_fail_many(_partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Cancels a Flow record, optionally guarded by a lease token."
  @spec flow_cancel(binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_cancel(id, opts \\ [])

  def flow_cancel(id, opts) when is_binary(id) and is_list(opts) do
    Ferricstore.Flow.cancel(default_ctx(), id, opts)
  end

  def flow_cancel(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_cancel(_id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Cancels a batch of Flow records.

  When `partition_key` is set, the batch is all-or-nothing because every item
  routes to the same shard. When `partition_key` is `nil`, each item must carry
  `:partition_key`; items are grouped by shard and each shard group is atomic.
  Each item must provide `:id` and `:fencing_token`; `:lease_token` is optional.
  """
  @spec flow_cancel_many(binary() | nil, list(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_cancel_many(partition_key, items, opts \\ [])

  def flow_cancel_many(partition_key, items, opts) when is_list(items) and is_list(opts) do
    Ferricstore.Flow.cancel_many(default_ctx(), partition_key, items, opts)
  end

  def flow_cancel_many(_partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Removes expired terminal Flow state, history rows, and generated value payload keys.

  This is a bounded cleanup pass; pass `limit: n` to cap the number of expired
  terminal flows cleaned per shard.
  """
  @spec flow_retention_cleanup(keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_retention_cleanup(opts \\ [])

  def flow_retention_cleanup(opts) when is_list(opts) do
    Ferricstore.Flow.retention_cleanup(default_ctx(), opts)
  end

  def flow_retention_cleanup(_opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Rewinds a Flow record to a previous history event without rewriting history."
  @spec flow_rewind(binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_rewind(id, opts) when is_binary(id) and is_list(opts) do
    Ferricstore.Flow.rewind(default_ctx(), id, opts)
  end

  def flow_rewind(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_rewind(_id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Lists Flow records for `type` from the state index."
  @spec flow_list(binary(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_list(type, opts \\ [])

  def flow_list(type, opts) when is_binary(type) and is_list(opts) do
    Ferricstore.Flow.list(default_ctx(), type, opts)
  end

  def flow_list(type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_list(_type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Lists terminal Flow records for `type`, optionally bounded by terminal update time."
  @spec flow_terminals(binary(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_terminals(type, opts \\ [])

  def flow_terminals(type, opts) when is_binary(type) and is_list(opts) do
    Ferricstore.Flow.terminals(default_ctx(), type, opts)
  end

  def flow_terminals(type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_terminals(_type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Lists failed Flow records for `type`, optionally bounded by terminal update time."
  @spec flow_failures(binary(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_failures(type, opts \\ [])

  def flow_failures(type, opts) when is_binary(type) and is_list(opts) do
    Ferricstore.Flow.failures(default_ctx(), type, opts)
  end

  def flow_failures(type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_failures(_type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Lists Flow records by parent flow id."
  @spec flow_by_parent(binary(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_by_parent(parent_flow_id, opts \\ [])

  def flow_by_parent(parent_flow_id, opts) when is_binary(parent_flow_id) and is_list(opts) do
    Ferricstore.Flow.by_parent(default_ctx(), parent_flow_id, opts)
  end

  def flow_by_parent(parent_flow_id, _opts) when not is_binary(parent_flow_id),
    do: {:error, "ERR flow parent_flow_id must be a non-empty string"}

  def flow_by_parent(_parent_flow_id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Lists Flow records by root flow id, including the root record when present."
  @spec flow_by_root(binary(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_by_root(root_flow_id, opts \\ [])

  def flow_by_root(root_flow_id, opts) when is_binary(root_flow_id) and is_list(opts) do
    Ferricstore.Flow.by_root(default_ctx(), root_flow_id, opts)
  end

  def flow_by_root(root_flow_id, _opts) when not is_binary(root_flow_id),
    do: {:error, "ERR flow root_flow_id must be a non-empty string"}

  def flow_by_root(_root_flow_id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Lists Flow records by correlation id."
  @spec flow_by_correlation(binary(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_by_correlation(correlation_id, opts \\ [])

  def flow_by_correlation(correlation_id, opts)
      when is_binary(correlation_id) and is_list(opts) do
    Ferricstore.Flow.by_correlation(default_ctx(), correlation_id, opts)
  end

  def flow_by_correlation(correlation_id, _opts) when not is_binary(correlation_id),
    do: {:error, "ERR flow correlation_id must be a non-empty string"}

  def flow_by_correlation(_correlation_id, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Returns Flow index counters for `type`."
  @spec flow_info(binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_info(type, opts \\ [])

  def flow_info(type, opts) when is_binary(type) and is_list(opts) do
    Ferricstore.Flow.info(default_ctx(), type, opts)
  end

  def flow_info(type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_info(_type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Lists running Flow records with expired lease deadlines."
  @spec flow_stuck(binary(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_stuck(type, opts \\ [])

  def flow_stuck(type, opts) when is_binary(type) and is_list(opts) do
    Ferricstore.Flow.stuck(default_ctx(), type, opts)
  end

  def flow_stuck(type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_stuck(_type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Returns Flow history events for `id`.

  Default reads include history projected into LMDB. Hot history is only a
  short-lived keydir cache while projection catches up.
  When async history is enabled, reads flush the async projection by default.
  Pass `consistent_projection: false` to allow briefly stale history reads.

  History caps are set when the Flow is created. Defaults are
  `history_hot_max_events: 0` and `history_max_events: 100000`; hard caps are
  `10000` hot events and `1000000` total durable events.
  """
  @spec flow_history(binary(), keyword()) :: {:ok, [{binary(), map()}]} | {:error, binary()}
  def flow_history(id, opts \\ [])

  def flow_history(id, opts) when is_binary(id) and is_list(opts) do
    Ferricstore.Flow.history(default_ctx(), id, opts)
  end

  def flow_history(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_history(_id, _opts), do: {:error, "ERR flow opts must be a keyword list"}
end
