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

  @doc """
  Runs deterministic in-process Flow step chains and completes them.

  This is for no-external-IO workflow segments where the states can be advanced
  durably inside one shard-local Raft apply. Each item may be an id, `%{id: id}`,
  or `%{id: id, partition_key: partition_key}`. Missing partitions use the same
  deterministic auto partitioning as normal Flow creation.
  """
  @spec flow_run_steps_many(list(), keyword()) :: :ok | {:ok, list()} | {:error, binary()}
  def flow_run_steps_many(items, opts \\ [])

  def flow_run_steps_many(items, opts) when is_list(items) and is_list(opts) do
    Ferricstore.Flow.run_steps_many(default_ctx(), items, opts)
  end

  def flow_run_steps_many(_items, _opts),
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
  Creates a durable Flow schedule.

  Supported schedule kinds:

    * `kind: :one_shot, at_ms: ts`
    * `kind: :delay, delay_ms: ms`
    * `kind: :interval, every_ms: ms, start_at_ms: ts`
    * `kind: :cron, cron: "*/5 * * * *", start_at_ms: ts`

  Cron is minute-granularity and defaults to UTC. Pass `timezone: "IANA/Zone"`
  for wall-clock cron matching with DST handling. Recurring schedules can also
  set `:overlap_policy` to `:allow`, `:skip`, `:queue_after_previous`, or
  `:fail_schedule`.

  `:target` is a Flow create option list containing at least `:type`. Keep
  schedule definitions small; large target values should be stored with
  `flow_value_put/2` and referenced with `:payload_ref` or `:value_refs`.
  """
  @spec flow_schedule_create(binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_schedule_create(id, opts) when is_binary(id) and is_list(opts) do
    Ferricstore.Flow.Schedule.create(default_ctx(), id, opts)
  end

  def flow_schedule_create(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow schedule id must be a non-empty string"}

  def flow_schedule_create(_id, _opts),
    do: {:error, "ERR flow schedule opts must be a keyword list"}

  @doc "Returns a durable Flow schedule by id."
  @spec flow_schedule_get(binary(), keyword()) :: {:ok, map() | nil} | {:error, binary()}
  def flow_schedule_get(id, opts \\ [])

  def flow_schedule_get(id, opts) when is_binary(id) and is_list(opts) do
    Ferricstore.Flow.Schedule.get(default_ctx(), id, opts)
  end

  def flow_schedule_get(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow schedule id must be a non-empty string"}

  def flow_schedule_get(_id, _opts),
    do: {:error, "ERR flow schedule opts must be a keyword list"}

  @doc """
  Fires one active schedule immediately.

  This is intended for admin/debug/backfill flows. Pass `:fire_at_ms` to make a
  manual fire use a specific logical occurrence time; otherwise `:now_ms` is
  used.
  """
  @spec flow_schedule_fire(binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_schedule_fire(id, opts \\ [])

  def flow_schedule_fire(id, opts) when is_binary(id) and is_list(opts) do
    Ferricstore.Flow.Schedule.fire(default_ctx(), id, opts)
  end

  def flow_schedule_fire(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow schedule id must be a non-empty string"}

  def flow_schedule_fire(_id, _opts),
    do: {:error, "ERR flow schedule opts must be a keyword list"}

  @doc "Pauses an active durable Flow schedule."
  @spec flow_schedule_pause(binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_schedule_pause(id, opts \\ [])

  def flow_schedule_pause(id, opts) when is_binary(id) and is_list(opts) do
    Ferricstore.Flow.Schedule.pause(default_ctx(), id, opts)
  end

  def flow_schedule_pause(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow schedule id must be a non-empty string"}

  def flow_schedule_pause(_id, _opts),
    do: {:error, "ERR flow schedule opts must be a keyword list"}

  @doc "Resumes a paused durable Flow schedule."
  @spec flow_schedule_resume(binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_schedule_resume(id, opts \\ [])

  def flow_schedule_resume(id, opts) when is_binary(id) and is_list(opts) do
    Ferricstore.Flow.Schedule.resume(default_ctx(), id, opts)
  end

  def flow_schedule_resume(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow schedule id must be a non-empty string"}

  def flow_schedule_resume(_id, _opts),
    do: {:error, "ERR flow schedule opts must be a keyword list"}

  @doc "Lists durable Flow schedules."
  @spec flow_schedule_list(keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_schedule_list(opts \\ [])

  def flow_schedule_list(opts) when is_list(opts) do
    Ferricstore.Flow.Schedule.list(default_ctx(), opts)
  end

  def flow_schedule_list(_opts),
    do: {:error, "ERR flow schedule opts must be a keyword list"}

  @doc "Cancels a durable Flow schedule."
  @spec flow_schedule_delete(binary(), keyword()) :: :ok | {:error, binary()}
  def flow_schedule_delete(id, opts \\ [])

  def flow_schedule_delete(id, opts) when is_binary(id) and is_list(opts) do
    Ferricstore.Flow.Schedule.delete(default_ctx(), id, opts)
  end

  def flow_schedule_delete(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow schedule id must be a non-empty string"}

  def flow_schedule_delete(_id, _opts),
    do: {:error, "ERR flow schedule opts must be a keyword list"}

  @doc false
  @spec flow_schedule_fire_due(keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_schedule_fire_due(opts \\ [])

  def flow_schedule_fire_due(opts) when is_list(opts) do
    Ferricstore.Flow.Schedule.fire_due(default_ctx(), opts)
  end

  def flow_schedule_fire_due(_opts),
    do: {:error, "ERR flow schedule opts must be a keyword list"}

  @doc """
  Creates a Flow and immediately leases its first logical step to a worker.

  This is the "start execution now" primitive for step-style workflows. It
  creates the record in physical `"running"` state, stores `initial_state` as
  `:run_state`, and returns the fresh running record with `:lease_token` and
  `:fencing_token`.
  """
  @spec flow_start_and_claim(binary(), binary(), binary(), keyword()) ::
          {:ok, map()} | {:error, binary()}
  def flow_start_and_claim(id, type, initial_state, opts \\ [])

  def flow_start_and_claim(id, type, initial_state, opts)
      when is_binary(id) and is_binary(type) and is_binary(initial_state) and is_list(opts) do
    Ferricstore.Flow.start_and_claim(default_ctx(), id, type, initial_state, opts)
  end

  def flow_start_and_claim(id, _type, _initial_state, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_start_and_claim(_id, type, _initial_state, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_start_and_claim(_id, _type, initial_state, _opts)
      when not is_binary(initial_state),
      do: {:error, "ERR flow state must be a non-empty string"}

  def flow_start_and_claim(_id, _type, _initial_state, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc """
  Advances a claimed Flow to the next logical step and keeps execution leased
  to the same worker.

  Unlike `flow_transition/4`, this returns the fresh running Flow record because
  the caller needs the new `:lease_token` and `:fencing_token` for the next
  step. On handler failure, call `flow_retry/3` with that fresh lease instead.
  """
  @spec flow_step_continue(binary(), binary(), binary(), binary(), keyword()) ::
          {:ok, map()} | {:error, binary()}
  def flow_step_continue(id, lease_token, from_state, to_state, opts \\ [])

  def flow_step_continue(id, lease_token, from_state, to_state, opts)
      when is_binary(id) and is_binary(lease_token) and is_binary(from_state) and
             is_binary(to_state) and is_list(opts) do
    Ferricstore.Flow.step_continue(default_ctx(), id, lease_token, from_state, to_state, opts)
  end

  def flow_step_continue(id, _lease_token, _from_state, _to_state, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def flow_step_continue(_id, lease_token, _from_state, _to_state, _opts)
      when not is_binary(lease_token),
      do: {:error, "ERR flow lease_token must be a string"}

  def flow_step_continue(_id, _lease_token, from_state, _to_state, _opts)
      when not is_binary(from_state),
      do: {:error, "ERR flow from must be a non-empty string"}

  def flow_step_continue(_id, _lease_token, _from_state, to_state, _opts)
      when not is_binary(to_state),
      do: {:error, "ERR flow to must be a non-empty string"}

  def flow_step_continue(_id, _lease_token, _from_state, _to_state, _opts),
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

  @doc """
  Searches Flow records by policy-indexed attributes across optional `:type` and `:state` filters.

  Broad search uses the projected indexes configured by `flow_policy_set/2` `:indexed_attributes`.
  When `:type` is supplied, every queried attribute key must be indexed by that type policy or the
  call fails clearly. Use `flow_list/2` with a concrete type/state when you want to filter by normal
  non-indexed attributes.

  Changing `:indexed_attributes` is future-only for broad indexes: newly created or later-updated
  records receive the new broad indexes. Existing records remain exact-queryable by `flow_list/2`
  until a future explicit reprojection/reindex command is added.
  """
  @spec flow_search(keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_search(opts \\ [])

  def flow_search(opts) when is_list(opts) do
    Ferricstore.Flow.search(default_ctx(), opts)
  end

  def flow_search(_opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Lists policy-indexed Flow attribute keys for `type` and `:state`."
  @spec flow_attributes(binary(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def flow_attributes(type, opts \\ [])

  def flow_attributes(type, opts) when is_binary(type) and is_list(opts) do
    Ferricstore.Flow.attributes(default_ctx(), type, opts)
  end

  def flow_attributes(type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_attributes(_type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Lists top projected values for a policy-indexed Flow attribute."
  @spec flow_attribute_values(binary(), binary(), keyword()) ::
          {:ok, [map()]} | {:error, binary()}
  def flow_attribute_values(type, attr_name, opts \\ [])

  def flow_attribute_values(type, attr_name, opts) when is_binary(type) and is_list(opts) do
    Ferricstore.Flow.attribute_values(default_ctx(), type, attr_name, opts)
  end

  def flow_attribute_values(type, _attr_name, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_attribute_values(_type, _attr_name, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc "Returns bounded Flow stats for `type`, `:state`, and optional `:attributes` filters."
  @spec flow_stats(binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def flow_stats(type, opts \\ [])

  def flow_stats(type, opts) when is_binary(type) and is_list(opts) do
    Ferricstore.Flow.stats(default_ctx(), type, opts)
  end

  def flow_stats(type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def flow_stats(_type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

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
