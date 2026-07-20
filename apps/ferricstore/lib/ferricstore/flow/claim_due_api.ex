defmodule Ferricstore.Flow.ClaimDueAPI do
  @moduledoc """
  Public Flow claim path.

  This module validates `FLOW.CLAIM_DUE` / reclaim options, chooses the hot due
  index keys, coordinates blocking waiters, and delegates the actual mutation to
  the shard/Raft path. Returned payload/value hydration is capped and explicit.

  ## Performance boundary

  Claiming is a primary hot path. Avoid new maps/lists in per-candidate loops,
  behaviours/protocol dispatch, or extra GenServer calls. Any structural change
  here should be benchmarked with queued DBOS-style workers and focused
  `claim_due` tests.
  """

  alias Ferricstore.Flow.{
    ClaimFilter,
    ClaimScope,
    ClaimWaiters,
    Internal,
    PayloadReturn,
    StorageScope
  }

  alias Ferricstore.Flow.Telemetry, as: FlowTelemetry
  alias Ferricstore.Store.Router

  @claim_due_block_forever :infinity
  @default_lease_ms 30_000
  @default_limit 1
  @default_max_claim_limit 1_000
  @max_blocking_timeout_ms 0xFFFFFFFF
  @max_exact_ms 9_007_199_254_740_991
  @max_priority 2
  @claim_due_cold_schedule_horizon_ms Application.compile_env(
                                        :ferricstore,
                                        :flow_claim_due_cold_schedule_horizon_ms,
                                        24 * 60 * 60 * 1_000
                                      )

  def claim_due(ctx, type, opts) when is_binary(type) and is_list(opts) do
    started = flow_start_time()
    blocking? = Keyword.has_key?(opts, :block_ms)

    result =
      with {:ok, block_ms} <- optional_claim_block_ms(opts) do
        claim_opts = Keyword.delete(opts, :block_ms)

        if blocking? do
          blocking_result(ctx, type, claim_opts, block_ms)
        else
          result(ctx, type, claim_opts)
        end
      end

    FlowTelemetry.observe(:claim_due, started, result, %{flow_type: type})
  end

  def reclaim(ctx, type, opts) when is_binary(type) and is_list(opts) do
    started = flow_start_time()

    result =
      opts
      |> Keyword.put(:state, "running")
      |> Keyword.put(:reclaim_expired, false)
      |> then(&result(ctx, type, &1))

    FlowTelemetry.observe(:reclaim, started, result, %{flow_type: type})
  end

  def result(ctx, type, opts), do: result(ctx, type, opts, :allow)

  def result(ctx, type, opts, cold_due_mode) do
    do_result(ctx, type, opts, cold_due_mode, :resolve)
  end

  @doc false
  def result_resolved(ctx, type, opts, expected_metadata) do
    result_resolved(ctx, type, opts, expected_metadata, :allow)
  end

  @doc false
  def result_resolved(ctx, type, opts, expected_metadata, cold_due_mode)
      when is_map(expected_metadata) do
    do_result(ctx, type, opts, cold_due_mode, {:resolved, expected_metadata})
  end

  defp do_result(ctx, type, opts, cold_due_mode, scope_binding) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         :ok <- Internal.reject_reserved_type(type, opts),
         {:ok, state} <- optional_claim_states(opts),
         {:ok, worker} <- required_binary(opts, :worker),
         {:ok, lease_ms} <- optional_pos_integer(opts, :lease_ms, @default_lease_ms),
         {:ok, limit} <- optional_claim_limit(opts),
         {:ok, priority} <- optional_priority_or_nil(opts),
         {:ok, now} <- optional_now_ms(opts),
         :ok <- validate_deadline(now, lease_ms, :lease_ms),
         {:ok, return_mode} <- optional_claim_return(opts),
         {:ok, payload_return} <- PayloadReturn.options(opts, return_mode == :records),
         {:ok, named_values} <- named_value_return_opts(opts),
         {:ok, reclaim_expired?} <- optional_boolean(opts, :reclaim_expired, true),
         {:ok, reclaim_ratio} <- optional_reclaim_ratio(opts),
         {:ok, governance_limit} <-
           optional_governance_limit(
             ctx,
             type,
             state,
             worker,
             opts,
             limit,
             lease_ms,
             now
           ),
         {:ok, partition_key, partition_keys} <- optional_claim_partitions(opts),
         :ok <- validate_claim_due_keys(type, state, priority, partition_keys || partition_key) do
      attrs =
        %{
          type: type,
          state: state,
          worker: worker,
          lease_ms: lease_ms,
          limit: limit,
          priority: priority,
          partition_key: partition_key
        }
        |> maybe_put_attr(:partition_keys, partition_keys)
        |> maybe_put_attr(:now_ms, now)
        |> maybe_put_attr(:cold_due_mode, cold_due_mode)

      with {:ok, attrs, expected_metadata} <-
             bind_claim_scope(ctx, attrs, scope_binding),
           :ok <-
             validate_claim_due_key_lengths(
               type,
               state,
               priority,
               Map.get(attrs, :partition_keys) || Map.get(attrs, :partition_key),
               Router.max_key_size()
             ) do
        case router_result_with_governance_limit(
               ctx,
               attrs,
               reclaim_expired?,
               reclaim_ratio,
               governance_limit
             ) do
          {:ok, records} when is_list(records) ->
            with :ok <- ClaimScope.verify_records(records, expected_metadata) do
              {:ok, return_records(ctx, records, payload_return, return_mode, named_values)}
            end

          other ->
            other
        end
      end
    end
  end

  defp bind_claim_scope(ctx, attrs, :resolve), do: ClaimScope.bind_attrs(ctx, attrs)

  defp bind_claim_scope(_ctx, attrs, {:resolved, expected_metadata}),
    do: ClaimScope.bind_resolved_attrs(attrs, expected_metadata)

  def blocking_result(ctx, type, opts, block_ms) do
    with {:ok, expected_metadata} <- ClaimScope.resolve(ctx),
         {:ok, scoped_wait_opts} <- scoped_wait_opts(opts, expected_metadata) do
      do_blocking_result(ctx, type, opts, scoped_wait_opts, expected_metadata, block_ms)
    end
  end

  defp do_blocking_result(ctx, type, opts, scoped_wait_opts, expected_metadata, block_ms) do
    case result_resolved(ctx, type, opts, expected_metadata, :block) do
      {:ok, [_ | _]} = claimed ->
        claimed

      {:ok, []} ->
        with {:ok, keys, limit} <- wait_registration(type, scoped_wait_opts) do
          deadline = block_deadline(block_ms)

          with :ok <- ClaimWaiters.register(keys, self(), waiter_deadline(deadline), limit: limit) do
            try do
              case result_resolved(ctx, type, opts, expected_metadata, :block) do
                {:ok, [_ | _]} = claimed ->
                  claimed

                {:ok, []} ->
                  schedule_next_due(
                    ctx,
                    type,
                    wait_opts_with_horizon(scoped_wait_opts, deadline)
                  )

                  wait_loop(
                    ctx,
                    type,
                    opts,
                    scoped_wait_opts,
                    expected_metadata,
                    deadline,
                    keys,
                    limit
                  )

                other ->
                  other
              end
            after
              ClaimWaiters.unregister(keys, self())
            end
          end
        end

      other ->
        other
    end
  end

  defp scoped_wait_opts(opts, expected_metadata) do
    with {:ok, partition_key, partition_keys} <- optional_claim_partitions(opts),
         attrs <-
           %{partition_key: partition_key}
           |> maybe_put_attr(:partition_keys, partition_keys),
         {:ok, scoped_attrs, _expected_metadata} <-
           ClaimScope.bind_resolved_attrs(attrs, expected_metadata) do
      opts =
        opts
        |> Keyword.delete(:partition_key)
        |> Keyword.delete(:partition_keys)

      case Map.get(scoped_attrs, :partition_keys) do
        [_ | _] = scoped_partition_keys ->
          {:ok, Keyword.put(opts, :partition_keys, scoped_partition_keys)}

        nil ->
          {:ok, Keyword.put(opts, :partition_key, Map.get(scoped_attrs, :partition_key))}
      end
    end
  end

  def wait_registration(type, opts) when is_binary(type) and is_list(opts) do
    with {:ok, keys} <- wait_keys(type, opts),
         {:ok, limit} <- optional_claim_limit(opts) do
      {:ok, keys, limit}
    end
  end

  @doc false
  @spec wait_registration(map(), binary(), keyword()) ::
          {:ok, [ClaimWaiters.waiter_key()], pos_integer()} | {:error, binary()}
  def wait_registration(ctx, type, opts)
      when is_map(ctx) and is_binary(type) and is_list(opts) do
    with {:ok, expected_metadata} <- ClaimScope.resolve(ctx),
         {:ok, scoped_wait_opts} <- scoped_wait_opts(opts, expected_metadata) do
      wait_registration(type, scoped_wait_opts)
    end
  end

  def wait_registration(_ctx, _type, _opts), do: {:error, "ERR invalid Flow claim scope"}

  def wait_keys(type, opts) when is_binary(type) and is_list(opts) do
    with {:ok, state} <- optional_claim_states(opts),
         {:ok, priority} <- optional_priority_or_nil(opts),
         {:ok, partition_key, partition_keys} <- optional_claim_partitions(opts),
         partition_filter = partition_keys || partition_key,
         :ok <- validate_wait_footprint(state, partition_filter),
         :ok <-
           validate_claim_due_key_lengths(
             type,
             state,
             priority,
             partition_filter,
             Router.max_key_size()
           ) do
      {:ok, ClaimWaiters.wait_keys(type, state, priority, partition_filter)}
    end
  end

  def schedule_next_due(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with {:ok, state_filter} <- optional_claim_states(opts),
         {:ok, partition_key, partition_keys} <- optional_claim_partitions(opts),
         {:ok, priority} <- optional_priority_or_nil(opts),
         partition_filter = partition_keys || partition_key,
         :ok <- validate_wait_footprint(state_filter, partition_filter) do
      Ferricstore.Flow.ClaimWaiterScheduler.schedule_next_due(
        ctx,
        type,
        state_filter,
        priority,
        partition_filter,
        Keyword.get(opts, :wait_horizon_ms)
      )
    else
      _unsupported -> :ok
    end
  end

  def schedule_next_due(_ctx, _type, _opts), do: :ok

  defp validate_wait_footprint(state, partition_filter) when is_list(partition_filter) do
    case StorageScope.scoped_auto_partition_scope(partition_filter) do
      {:ok, _scope} -> ClaimFilter.validate_footprint(state, :auto)
      :error -> ClaimFilter.validate_footprint(state, partition_filter)
    end
  end

  defp validate_wait_footprint(state, partition_filter),
    do: ClaimFilter.validate_footprint(state, partition_filter)

  def return_records(_ctx, records, _payload_return, :jobs, _named_values),
    do: Enum.map(records, &job_response/1)

  def return_records(_ctx, records, _payload_return, :jobs_compact, _named_values),
    do: Enum.map(records, &job_compact_response/1)

  def return_records(_ctx, records, _payload_return, :jobs_compact_attrs, _named_values),
    do: Enum.map(records, &job_compact_attrs_response/1)

  def return_records(_ctx, records, _payload_return, :jobs_compact_state, _named_values),
    do: Enum.map(records, &job_compact_state_response/1)

  def return_records(_ctx, records, _payload_return, :jobs_compact_state_attrs, _named_values),
    do: Enum.map(records, &job_compact_state_attrs_response/1)

  def return_records(ctx, records, payload_return, :records, named_values) do
    hydrated = Ferricstore.Flow.ValueHydration.payload_records(ctx, records, payload_return)

    ctx
    |> Ferricstore.Flow.ValueHydration.named_value_records(hydrated, named_values)
    |> Enum.map(&Ferricstore.Flow.RecordProjection.public/1)
  end

  def normal_attrs(_attrs, nil, _limit), do: nil

  def normal_attrs(attrs, {:any_except_running, state}, limit) do
    attrs
    |> Map.put(:state, state)
    |> Map.put(:limit, limit)
    |> Map.put(:exclude_states, ["running"])
  end

  def normal_attrs(attrs, state, limit) do
    attrs
    |> Map.put(:state, state)
    |> Map.put(:limit, limit)
  end

  def normal_state_filter("running"), do: nil
  def normal_state_filter(:any), do: {:any_except_running, :any}

  def normal_state_filter(states) when is_list(states) do
    case Enum.reject(states, &(&1 == "running")) do
      [] -> nil
      [state] -> state
      filtered -> filtered
    end
  end

  def normal_state_filter(state), do: state

  defp wait_loop(
         ctx,
         type,
         opts,
         scoped_wait_opts,
         expected_metadata,
         deadline,
         keys,
         limit
       ) do
    waiter_message = ClaimWaiters.message()
    wait_ms = wait_ms(deadline)

    receive do
      {^waiter_message, _key} ->
        case result_resolved(ctx, type, opts, expected_metadata, :block) do
          {:ok, []} ->
            if expired?(deadline) do
              {:ok, []}
            else
              with :ok <- reregister_waiters(keys, deadline, limit) do
                schedule_next_due(
                  ctx,
                  type,
                  wait_opts_with_horizon(scoped_wait_opts, deadline)
                )

                wait_loop(
                  ctx,
                  type,
                  opts,
                  scoped_wait_opts,
                  expected_metadata,
                  deadline,
                  keys,
                  limit
                )
              end
            end

          other ->
            other
        end
    after
      wait_ms ->
        {:ok, []}
    end
  end

  defp reregister_waiters(keys, deadline, limit) do
    ClaimWaiters.unregister(keys, self())
    ClaimWaiters.register(keys, self(), waiter_deadline(deadline), limit: limit)
  end

  defp wait_opts_with_horizon(opts, @claim_due_block_forever) do
    Keyword.put(opts, :wait_horizon_ms, @claim_due_cold_schedule_horizon_ms)
  end

  defp wait_opts_with_horizon(opts, deadline) when is_integer(deadline) do
    horizon_ms =
      deadline
      |> Kernel.-(System.monotonic_time(:millisecond))
      |> max(0)

    Keyword.put(opts, :wait_horizon_ms, horizon_ms)
  end

  defp block_deadline(0), do: @claim_due_block_forever

  defp block_deadline(block_ms) when is_integer(block_ms) and block_ms > 0,
    do: System.monotonic_time(:millisecond) + block_ms

  defp waiter_deadline(@claim_due_block_forever), do: 0
  defp waiter_deadline(deadline), do: deadline

  defp wait_ms(@claim_due_block_forever), do: :infinity

  defp wait_ms(deadline) do
    deadline
    |> Kernel.-(System.monotonic_time(:millisecond))
    |> max(0)
  end

  defp expired?(@claim_due_block_forever), do: false
  defp expired?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  defp router_result(ctx, %{state: "running"} = attrs, _reclaim_expired?, _ratio) do
    Router.flow_claim_due(ctx, attrs)
  end

  defp router_result(ctx, attrs, false, _ratio) do
    normal_state = normal_state_filter(Map.fetch!(attrs, :state))
    router_maybe(ctx, normal_attrs(attrs, normal_state, Map.fetch!(attrs, :limit)))
  end

  defp router_result(ctx, %{limit: 1} = attrs, true, reclaim_ratio) when reclaim_ratio > 0 do
    normal_state = normal_state_filter(Map.fetch!(attrs, :state))

    case router_maybe(ctx, normal_attrs(attrs, normal_state, 1)) do
      {:ok, [_ | _]} = claimed ->
        claimed

      {:ok, []} ->
        router_maybe(ctx, %{attrs | state: "running", limit: 1})

      other ->
        other
    end
  end

  defp router_result(ctx, attrs, true, reclaim_ratio) when reclaim_ratio > 0 do
    limit = Map.fetch!(attrs, :limit)
    initial_reclaim_limit = max(1, div(limit * reclaim_ratio + 99, 100))
    normal_state = normal_state_filter(Map.fetch!(attrs, :state))

    with {:ok, reclaimed_first} <-
           router_maybe(ctx, %{attrs | state: "running", limit: initial_reclaim_limit}),
         remaining_after_reclaim = limit - length(reclaimed_first),
         {:ok, normal} <-
           router_maybe(ctx, normal_attrs(attrs, normal_state, remaining_after_reclaim)),
         remaining_after_normal = limit - length(reclaimed_first) - length(normal),
         {:ok, reclaimed_more} <-
           router_maybe(ctx, %{attrs | state: "running", limit: remaining_after_normal}) do
      {:ok, reclaimed_first ++ normal ++ reclaimed_more}
    end
  end

  defp router_result(ctx, attrs, _reclaim_expired?, _ratio) do
    Router.flow_claim_due(ctx, attrs)
  end

  defp router_result_with_governance_limit(ctx, attrs, reclaim_expired?, reclaim_ratio, nil) do
    router_result(ctx, attrs, reclaim_expired?, reclaim_ratio)
  end

  defp router_result_with_governance_limit(
         ctx,
         attrs,
         reclaim_expired?,
         reclaim_ratio,
         %{now_ms: now_ms} = governance_limit
       ) do
    case spend_governance_limit(ctx, governance_limit) do
      {:ok, spent} ->
        governance_limit = put_spent_reservation_ids(governance_limit, spent)

        governed_attrs =
          attrs
          |> Map.put(:now_ms, now_ms)
          |> Map.put(
            :governance_limit,
            Map.take(governance_limit, [
              :scope,
              :shard_id,
              :enforcement,
              :reservation_ids
            ])
          )

        result = router_result(ctx, governed_attrs, reclaim_expired?, reclaim_ratio)
        release_unused_claim_limit(ctx, governance_limit, result)
        result

      {:error, _reason} = error ->
        if governed_claim_definitely_empty?(ctx, attrs, reclaim_expired?, reclaim_ratio) do
          {:ok, []}
        else
          error
        end
    end
  end

  defp spend_governance_limit(ctx, governance_limit) do
    case do_spend_governance_limit(ctx, governance_limit) do
      {:error, _reason} when governance_limit.auto_lease? ->
        with {:ok, _lease} <- lease_governance_limit(ctx, governance_limit) do
          do_spend_governance_limit(ctx, governance_limit)
        end

      result ->
        result
    end
  end

  defp do_spend_governance_limit(ctx, governance_limit) do
    backend = governance_limit_backend(governance_limit.enforcement)

    opts = [
      shard_id: governance_limit.shard_id,
      amount: governance_limit.amount,
      now_ms: governance_limit.now_ms,
      ttl_ms: governance_limit.ttl_ms
    ]

    backend.spend(
      ctx,
      governance_limit.scope,
      maybe_put_limit_configuration(opts, governance_limit)
    )
  end

  defp lease_governance_limit(ctx, governance_limit) do
    opts = [
      shard_id: governance_limit.shard_id,
      amount: max(governance_limit.amount, governance_limit.lease_size),
      limit: governance_limit.limit,
      ttl_ms: governance_limit.ttl_ms,
      now_ms: governance_limit.now_ms
    ]

    opts = maybe_put_limit_configuration(opts, governance_limit)
    Ferricstore.Flow.Governance.LimitStore.lease(ctx, governance_limit.scope, opts)
  end

  defp governance_limit_backend(:strict_global),
    do: Ferricstore.Flow.Governance.LimitStore

  defp governance_limit_backend(:approximate_global),
    do: Ferricstore.Flow.Governance.LimitCache

  defp governed_claim_definitely_empty?(
         ctx,
         %{state: "running"} = attrs,
         _reclaim_expired?,
         _ratio
       ) do
    Router.flow_claim_due_presence(ctx, attrs) == :empty
  end

  defp governed_claim_definitely_empty?(ctx, attrs, false, _ratio) do
    normal_state = normal_state_filter(Map.fetch!(attrs, :state))

    case normal_attrs(attrs, normal_state, Map.fetch!(attrs, :limit)) do
      nil -> true
      normal_attrs -> Router.flow_claim_due_presence(ctx, normal_attrs) == :empty
    end
  end

  defp governed_claim_definitely_empty?(ctx, attrs, true, reclaim_ratio) when reclaim_ratio > 0 do
    normal_state = normal_state_filter(Map.fetch!(attrs, :state))
    normal_attrs = normal_attrs(attrs, normal_state, Map.fetch!(attrs, :limit))

    normal_empty? =
      case normal_attrs do
        nil -> true
        normal_attrs -> Router.flow_claim_due_presence(ctx, normal_attrs) == :empty
      end

    running_empty? =
      Router.flow_claim_due_presence(ctx, Map.put(attrs, :state, "running")) == :empty

    normal_empty? and running_empty?
  end

  defp governed_claim_definitely_empty?(_ctx, _attrs, _reclaim_expired?, _ratio), do: false

  defp release_unused_claim_limit(ctx, governance_limit, result)
       when is_map(governance_limit) do
    reservation_ids = Map.get(governance_limit, :reservation_ids, [])
    release_ids = governance_release_ids(result, reservation_ids)

    if release_ids != [] do
      release_opts = [
        shard_id: governance_limit.shard_id,
        amount: length(release_ids),
        reservation_ids: release_ids,
        now_ms: governance_limit.now_ms
      ]

      governance_limit_backend(governance_limit.enforcement).release(
        ctx,
        governance_limit.scope,
        release_opts
      )
    end
  end

  defp release_unused_claim_limit(_ctx, _governance_limit, _result), do: :ok

  @doc false
  def governance_release_ids({:error, {:timeout, :unknown_outcome}}, reservation_ids)
      when is_list(reservation_ids),
      do: []

  def governance_release_ids({:error, _reason}, reservation_ids) when is_list(reservation_ids),
    do: reservation_ids

  def governance_release_ids({:ok, records}, reservation_ids)
      when is_list(records) and is_list(reservation_ids) do
    used =
      records
      |> Enum.reduce(MapSet.new(), fn record, acc ->
        case get_in(record, [:governance_limit, :reservation_id]) do
          reservation_id when is_binary(reservation_id) -> MapSet.put(acc, reservation_id)
          _missing -> acc
        end
      end)

    Enum.reject(reservation_ids, &MapSet.member?(used, &1))
  end

  def governance_release_ids(_result, _reservation_ids), do: []

  defp put_spent_reservation_ids(governance_limit, %{reservation_ids: reservation_ids})
       when is_list(reservation_ids) do
    if length(reservation_ids) == governance_limit.amount do
      Map.put(governance_limit, :reservation_ids, reservation_ids)
    else
      governance_limit
    end
  end

  defp put_spent_reservation_ids(governance_limit, _spent), do: governance_limit

  defp router_maybe(_ctx, nil), do: {:ok, []}
  defp router_maybe(_ctx, %{limit: limit}) when limit <= 0, do: {:ok, []}
  defp router_maybe(ctx, attrs), do: Router.flow_claim_due(ctx, attrs)

  defp job_response(record) do
    record = Ferricstore.Flow.RecordProjection.public(record)

    %{
      id: Map.get(record, :id),
      type: Map.get(record, :type),
      state: Map.get(record, :state),
      run_state: Map.get(record, :run_state),
      partition_key: Map.get(record, :partition_key),
      lease_token: Map.get(record, :lease_token),
      fencing_token: Map.get(record, :fencing_token)
    }
  end

  defp job_compact_response(record) do
    record = Ferricstore.Flow.RecordProjection.public(record)

    [
      Map.get(record, :id),
      Map.get(record, :partition_key),
      Map.get(record, :lease_token),
      Map.get(record, :fencing_token)
    ]
  end

  defp job_compact_attrs_response(record) do
    record = Ferricstore.Flow.RecordProjection.public(record)

    [
      Map.get(record, :id),
      Map.get(record, :partition_key),
      Map.get(record, :lease_token),
      Map.get(record, :fencing_token),
      Map.get(record, :attributes, %{})
    ]
  end

  defp job_compact_state_response(record) do
    record = Ferricstore.Flow.RecordProjection.public(record)

    [
      Map.get(record, :id),
      Map.get(record, :partition_key),
      Map.get(record, :lease_token),
      Map.get(record, :fencing_token),
      Map.get(record, :run_state) || Map.get(record, :state)
    ]
  end

  defp job_compact_state_attrs_response(record) do
    record = Ferricstore.Flow.RecordProjection.public(record)

    [
      Map.get(record, :id),
      Map.get(record, :partition_key),
      Map.get(record, :lease_token),
      Map.get(record, :fencing_token),
      Map.get(record, :run_state) || Map.get(record, :state),
      Map.get(record, :attributes, %{})
    ]
  end

  defp validate_opts(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, "ERR flow opts must be a keyword list"}
  end

  defp validate_type(type) when is_binary(type) and type != "", do: :ok
  defp validate_type(_type), do: {:error, "ERR flow type must be a non-empty string"}

  defp required_binary(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      {:ok, _} -> {:error, "ERR flow #{key} must be a non-empty string"}
      :error -> {:error, "ERR flow #{key} is required"}
    end
  end

  defp optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a boolean"}
    end
  end

  defp optional_now_ms(opts) do
    case Keyword.fetch(opts, :now_ms) do
      {:ok, value} when is_integer(value) and value >= 0 and value <= @max_exact_ms ->
        {:ok, value}

      {:ok, value} when is_integer(value) and value > @max_exact_ms ->
        {:error, "ERR flow now_ms exceeds maximum #{@max_exact_ms}"}

      {:ok, _value} ->
        {:error, "ERR flow now_ms must be a non-negative integer"}

      :error ->
        {:ok, nil}
    end
  end

  defp optional_pos_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 and value <= @max_exact_ms ->
        {:ok, value}

      value when is_integer(value) and value > @max_exact_ms ->
        {:error, "ERR flow #{key} exceeds maximum #{@max_exact_ms}"}

      _ ->
        {:error, "ERR flow #{key} must be a positive integer"}
    end
  end

  defp validate_deadline(nil, _duration_ms, _key), do: :ok

  defp validate_deadline(now_ms, duration_ms, _key)
       when now_ms <= @max_exact_ms - duration_ms,
       do: :ok

  defp validate_deadline(_now_ms, _duration_ms, key),
    do: {:error, "ERR flow #{key} deadline exceeds maximum #{@max_exact_ms}"}

  defp optional_governance_limit(ctx, type, state, worker, opts, limit, lease_ms, now_ms) do
    scope = Keyword.get(opts, :governance_limit_scope)
    shard_id = Keyword.get(opts, :governance_shard_id)
    now_ms = now_ms || Ferricstore.CommandTime.now_ms()

    cond do
      is_nil(scope) and is_nil(shard_id) ->
        policy_governance_limit(ctx, type, state, worker, limit, lease_ms, now_ms)

      is_binary(scope) and scope != "" and is_integer(shard_id) and shard_id >= 0 and
          shard_id < ctx.shard_count ->
        {:ok,
         %{
           scope: scope,
           shard_id: shard_id,
           amount: limit,
           now_ms: now_ms,
           ttl_ms: lease_ms,
           enforcement: :approximate_global,
           auto_lease?: false,
           lease_size: limit,
           limit: limit
         }}

      is_binary(scope) and scope != "" and is_integer(shard_id) ->
        {:error, "ERR flow governance_shard_id is outside the instance shard range"}

      true ->
        {:error,
         "ERR flow governance_limit_scope and governance_shard_id must be provided together"}
    end
  end

  defp policy_governance_limit(ctx, type, state, worker, amount, lease_ms, now_ms) do
    with {:ok, {policy_generation, policy}} <- Ferricstore.Flow.Policy.raw_entry(ctx, type) do
      case resolved_running_limit(policy, state, amount) do
        %{limit: limit} = rule when is_integer(limit) and limit >= 0 ->
          enforcement = Map.get(rule, :enforcement, :strict_global)
          lease_size = Map.get(rule, :lease_size, amount)

          {:ok,
           %{
             scope: running_limit_scope(type),
             shard_id: :erlang.phash2(worker, ctx.shard_count),
             amount: amount,
             now_ms: now_ms,
             ttl_ms: lease_ms,
             enforcement: enforcement,
             auto_lease?: true,
             lease_size: lease_size,
             limit: limit,
             config_version: policy_config_version(policy_generation, policy),
             policy_version: Map.get(policy || %{}, :version)
           }}

        _no_running_limit ->
          {:ok, nil}
      end
    end
  end

  defp resolved_running_limit(policy, state_filter, amount) do
    policy
    |> governance_policy_states(state_filter)
    |> Enum.reduce([], fn state, rules ->
      case policy
           |> Ferricstore.Flow.Governance.Policy.resolve(state)
           |> get_in([:limits, "running"]) do
        %{limit: limit} = rule when is_integer(limit) and limit >= 0 -> [rule | rules]
        _no_running_limit -> rules
      end
    end)
    |> aggregate_running_limits(amount)
  end

  defp governance_policy_states(policy, :any) do
    configured_states =
      case policy do
        %{states: states} when is_map(states) -> Map.keys(states)
        _none -> []
      end

    [nil | configured_states]
  end

  defp governance_policy_states(_policy, states) when is_list(states), do: Enum.uniq(states)
  defp governance_policy_states(_policy, state) when is_binary(state), do: [state]
  defp governance_policy_states(_policy, _state_filter), do: [nil]

  defp aggregate_running_limits([], _amount), do: nil

  defp aggregate_running_limits(rules, amount) do
    %{
      limit: rules |> Enum.map(&Map.fetch!(&1, :limit)) |> Enum.min(),
      lease_size:
        rules
        |> Enum.map(&Map.get(&1, :lease_size, amount))
        |> Enum.min(),
      enforcement:
        if(Enum.any?(rules, &(Map.get(&1, :enforcement, :strict_global) == :strict_global)),
          do: :strict_global,
          else: :approximate_global
        )
    }
  end

  defp running_limit_scope(type) do
    digest = :sha256 |> :crypto.hash(type) |> Base.url_encode64(padding: false)
    "flow-running:" <> digest
  end

  defp policy_config_version(generation, _policy) when is_integer(generation) and generation > 0,
    do: generation

  defp policy_config_version(0, %{version: version})
       when is_integer(version) and version >= 0,
       do: version

  defp policy_config_version(_generation, _policy), do: nil

  defp maybe_put_limit_configuration(opts, governance_limit) do
    opts =
      case Map.get(governance_limit, :config_version) do
        version when is_integer(version) and version >= 0 ->
          opts
          |> Keyword.put(:limit, governance_limit.limit)
          |> Keyword.put(:config_version, version)

        _missing ->
          opts
      end

    case {Keyword.has_key?(opts, :limit), Map.get(governance_limit, :policy_version)} do
      {false, _version} ->
        opts

      {true, version} when is_integer(version) and version >= 0 ->
        Keyword.put(opts, :policy_version, version)

      {true, version} when is_binary(version) and version != "" ->
        Keyword.put(opts, :policy_version, version)

      {_has_limit, _missing} ->
        opts
    end
  end

  defp optional_non_neg_integer(opts, key, default) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      {:ok, _} -> {:error, "ERR flow #{key} must be a non-negative integer"}
      :error when is_integer(default) and default >= 0 -> {:ok, default}
      :error when is_nil(default) -> {:ok, nil}
      :error -> {:error, "ERR flow #{key} must be a non-negative integer"}
    end
  end

  defp optional_claim_limit(opts) do
    with {:ok, limit} <- optional_pos_integer(opts, :limit, @default_limit) do
      max = flow_max_claim_limit()

      if limit <= max do
        {:ok, limit}
      else
        {:error, "ERR flow limit exceeds maximum #{max}"}
      end
    end
  end

  defp optional_claim_block_ms(opts) do
    with {:ok, block_ms} <- optional_non_neg_integer(opts, :block_ms, 0) do
      if block_ms <= @max_blocking_timeout_ms do
        {:ok, block_ms}
      else
        {:error, "ERR flow block_ms exceeds maximum #{@max_blocking_timeout_ms}"}
      end
    end
  end

  defp optional_reclaim_ratio(opts) do
    case Keyword.get(opts, :reclaim_ratio, 25) do
      value when is_integer(value) and value >= 0 and value <= 100 -> {:ok, value}
      _ -> {:error, "ERR flow reclaim_ratio must be an integer between 0 and 100"}
    end
  end

  defp flow_max_claim_limit do
    case Application.get_env(:ferricstore, :flow_max_claim_limit, @default_max_claim_limit) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_max_claim_limit
    end
  end

  defp named_value_return_opts(opts) do
    case Keyword.fetch(opts, :values) do
      :error -> {:ok, nil}
      {:ok, true} -> {:ok, :all}
      {:ok, false} -> {:ok, []}
      {:ok, name} when is_binary(name) and name != "" -> {:ok, [name]}
      {:ok, names} when is_list(names) -> normalize_named_value_names(names)
      {:ok, _other} -> {:error, "ERR flow values must be true, false, a name, or a name list"}
    end
  end

  defp normalize_named_value_names(names) do
    names
    |> Enum.reduce_while({:ok, []}, fn
      name, {:ok, acc} when is_binary(name) and name != "" -> {:cont, {:ok, [name | acc]}}
      _bad, {:ok, _acc} -> {:halt, {:error, "ERR flow value name must be a non-empty string"}}
    end)
    |> case do
      {:ok, normalized} -> {:ok, normalized |> Enum.reverse() |> Enum.uniq()}
      {:error, _reason} = error -> error
    end
  end

  defp optional_priority_or_nil(opts) do
    case Keyword.get(opts, :priority, nil) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 and value <= @max_priority -> {:ok, value}
      _ -> {:error, "ERR flow priority must be between 0 and #{@max_priority}"}
    end
  end

  defp optional_claim_partition_key(opts) do
    case Keyword.get(opts, :partition_key, nil) do
      nil -> {:ok, :auto}
      :any -> {:ok, :any}
      :auto -> {:ok, :auto}
      :global -> {:ok, nil}
      value when is_binary(value) and value != "" -> normalize_claim_partition_key_string(value)
      _ -> optional_partition_key(opts)
    end
  end

  defp normalize_claim_partition_key_string(value) do
    case String.upcase(value) do
      "ANY" -> {:ok, :any}
      "AUTO" -> {:ok, :auto}
      "GLOBAL" -> {:ok, nil}
      _ -> {:ok, value}
    end
  end

  defp optional_partition_key(opts) do
    case Keyword.get(opts, :partition_key, nil) do
      nil -> {:ok, nil}
      :global -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow partition_key must be a non-empty string or :global"}
    end
  end

  defp optional_claim_partitions(opts) do
    case Keyword.fetch(opts, :partition_keys) do
      :error ->
        with {:ok, partition_key} <- optional_claim_partition_key(opts) do
          {:ok, partition_key, nil}
        end

      {:ok, partition_keys} ->
        cond do
          Keyword.has_key?(opts, :partition_key) ->
            {:error, "ERR flow partition_key and partition_keys are mutually exclusive"}

          not is_list(partition_keys) or partition_keys == [] ->
            {:error, "ERR flow partition_keys must be a non-empty list"}

          true ->
            normalize_claim_partition_keys(partition_keys)
        end
    end
  end

  defp normalize_claim_partition_keys(partition_keys) do
    if Enum.all?(partition_keys, &(is_binary(&1) and &1 != "")) do
      {:ok, nil, Enum.uniq(partition_keys)}
    else
      {:error, "ERR flow partition_keys must be non-empty strings"}
    end
  end

  defp optional_claim_return(opts) do
    case Keyword.get(opts, :return, :records) do
      value when value in [:records, :record, :full] ->
        {:ok, :records}

      value when value in [:jobs, :job] ->
        {:ok, :jobs}

      value when value in [:jobs_compact, :job_compact] ->
        {:ok, :jobs_compact}

      value when value in [:jobs_compact_attrs, :job_compact_attrs] ->
        {:ok, :jobs_compact_attrs}

      value
      when value in [
             :jobs_compact_state,
             :job_compact_state,
             :jobs_compact_with_state,
             :job_compact_with_state
           ] ->
        {:ok, :jobs_compact_state}

      value
      when value in [
             :jobs_compact_state_attrs,
             :job_compact_state_attrs,
             :jobs_compact_with_state_attrs,
             :job_compact_with_state_attrs
           ] ->
        {:ok, :jobs_compact_state_attrs}

      value when is_binary(value) ->
        parse_claim_return_string(value)

      _ ->
        {:error,
         "ERR flow claim return must be records, jobs, jobs_compact, jobs_compact_attrs, jobs_compact_state, or jobs_compact_state_attrs"}
    end
  end

  defp parse_claim_return_string(value) do
    case String.upcase(value) do
      "RECORDS" -> {:ok, :records}
      "RECORD" -> {:ok, :records}
      "FULL" -> {:ok, :records}
      "JOBS" -> {:ok, :jobs}
      "JOB" -> {:ok, :jobs}
      "JOBS_COMPACT" -> {:ok, :jobs_compact}
      "JOB_COMPACT" -> {:ok, :jobs_compact}
      "JOBS_COMPACT_ATTRS" -> {:ok, :jobs_compact_attrs}
      "JOB_COMPACT_ATTRS" -> {:ok, :jobs_compact_attrs}
      "JOBS_COMPACT_ATTRIBUTES" -> {:ok, :jobs_compact_attrs}
      "JOB_COMPACT_ATTRIBUTES" -> {:ok, :jobs_compact_attrs}
      "JOBS_COMPACT_STATE" -> {:ok, :jobs_compact_state}
      "JOB_COMPACT_STATE" -> {:ok, :jobs_compact_state}
      "JOBS_COMPACT_WITH_STATE" -> {:ok, :jobs_compact_state}
      "JOB_COMPACT_WITH_STATE" -> {:ok, :jobs_compact_state}
      "JOBS_COMPACT_STATE_ATTRS" -> {:ok, :jobs_compact_state_attrs}
      "JOB_COMPACT_STATE_ATTRS" -> {:ok, :jobs_compact_state_attrs}
      "JOBS_COMPACT_WITH_STATE_ATTRS" -> {:ok, :jobs_compact_state_attrs}
      "JOB_COMPACT_WITH_STATE_ATTRS" -> {:ok, :jobs_compact_state_attrs}
      "JOBS_COMPACT_STATE_ATTRIBUTES" -> {:ok, :jobs_compact_state_attrs}
      "JOB_COMPACT_STATE_ATTRIBUTES" -> {:ok, :jobs_compact_state_attrs}
      "JOBS_COMPACT_WITH_STATE_ATTRIBUTES" -> {:ok, :jobs_compact_state_attrs}
      "JOB_COMPACT_WITH_STATE_ATTRIBUTES" -> {:ok, :jobs_compact_state_attrs}
      _ -> {:error, "ERR flow claim return must be records, jobs, or compact jobs"}
    end
  end

  defp optional_claim_states(opts) do
    state_values = Keyword.get_values(opts, :state)
    states_value = Keyword.get(opts, :states, nil)

    cond do
      state_values != [] and not is_nil(states_value) ->
        {:error, "ERR flow state and states are mutually exclusive"}

      state_values != [] ->
        normalize_claim_state_values(state_values)

      not is_nil(states_value) ->
        normalize_claim_state_values(states_value)

      true ->
        {:ok, :any}
    end
  end

  defp normalize_claim_state_values(:any), do: {:ok, :any}

  defp normalize_claim_state_values(value) when is_binary(value) do
    cond do
      claim_state_any?(value) -> {:ok, :any}
      value != "" -> {:ok, value}
      true -> {:error, "ERR flow state must be a non-empty string"}
    end
  end

  defp normalize_claim_state_values([value]) do
    cond do
      claim_state_any?(value) -> {:ok, :any}
      is_binary(value) and value != "" -> {:ok, value}
      true -> {:error, "ERR flow state must be a non-empty string"}
    end
  end

  defp normalize_claim_state_values(values) when is_list(values) do
    if values == [] do
      {:error, "ERR flow states must be a non-empty list"}
    else
      normalize_claim_state_list(values)
    end
  end

  defp normalize_claim_state_values(_value),
    do: {:error, "ERR flow state must be a non-empty string"}

  defp claim_state_any?(:any), do: true
  defp claim_state_any?(<<a, n, y>>), do: ascii_a?(a) and ascii_n?(n) and ascii_y?(y)
  defp claim_state_any?(_value), do: false

  defp ascii_a?(?A), do: true
  defp ascii_a?(?a), do: true
  defp ascii_a?(_), do: false
  defp ascii_n?(?N), do: true
  defp ascii_n?(?n), do: true
  defp ascii_n?(_), do: false
  defp ascii_y?(?Y), do: true
  defp ascii_y?(?y), do: true
  defp ascii_y?(_), do: false

  defp normalize_claim_state_list(values) do
    values
    |> Enum.reduce_while({:ok, false, []}, fn value, {:ok, any?, acc} ->
      cond do
        claim_state_any?(value) -> {:cont, {:ok, true, acc}}
        is_binary(value) and value != "" -> {:cont, {:ok, any?, [value | acc]}}
        true -> {:halt, {:error, "ERR flow state must be a non-empty string"}}
      end
    end)
    |> case do
      {:ok, true, []} -> {:ok, :any}
      {:ok, true, _states} -> {:error, "ERR flow STATE ANY cannot be mixed with explicit states"}
      {:ok, false, states} -> normalize_deduped_claim_states(states)
      {:error, _reason} = error -> error
    end
  end

  defp normalize_deduped_claim_states(states) do
    case dedupe_claim_states_keep_last(states) do
      [single] -> {:ok, single}
      deduped -> {:ok, deduped}
    end
  end

  defp dedupe_claim_states_keep_last(states) do
    {deduped, _seen} =
      Enum.reduce(states, {[], MapSet.new()}, fn state, {acc, seen} ->
        if MapSet.member?(seen, state) do
          {acc, seen}
        else
          {[state | acc], MapSet.put(seen, state)}
        end
      end)

    deduped
  end

  defp validate_claim_due_keys(type, state, nil, partition_key) do
    with :ok <- ClaimFilter.validate_footprint(state, partition_key) do
      validate_claim_due_key_lengths(type, state, nil, partition_key, Router.max_key_size())
    end
  end

  defp validate_claim_due_keys(type, state, priority, partition_key) do
    with :ok <- ClaimFilter.validate_footprint(state, partition_key) do
      validate_claim_due_key_lengths(type, state, priority, partition_key, Router.max_key_size())
    end
  end

  defp validate_claim_due_key_lengths(type, :any, priority, partition_filter, max_key_size) do
    validate_generated_key_size(
      due_any_key_size(
        type,
        priority_key_size(priority),
        max_claim_partition_tag_size(partition_filter)
      ),
      max_key_size
    )
  end

  defp validate_claim_due_key_lengths(type, states, priority, partition_keys, max_key_size)
       when is_list(states) and is_list(partition_keys) do
    validate_generated_key_size(
      due_key_size(
        type,
        max_binary_size(states),
        priority_key_size(priority),
        max_partition_tag_size(partition_keys)
      ),
      max_key_size
    )
  end

  defp validate_claim_due_key_lengths(type, state, priority, partition_keys, max_key_size)
       when is_binary(state) and is_list(partition_keys) do
    validate_generated_key_size(
      due_key_size(
        type,
        byte_size(state),
        priority_key_size(priority),
        max_partition_tag_size(partition_keys)
      ),
      max_key_size
    )
  end

  defp validate_claim_due_key_lengths(type, states, priority, partition_key, max_key_size)
       when is_list(states) do
    validate_generated_key_size(
      due_key_size(
        type,
        max_binary_size(states),
        priority_key_size(priority),
        partition_tag_size(partition_key)
      ),
      max_key_size
    )
  end

  defp validate_claim_due_key_lengths(type, state, priority, partition_key, max_key_size) do
    validate_generated_key_size(
      due_key_size(
        type,
        byte_size(state),
        priority_key_size(priority),
        partition_tag_size(partition_key)
      ),
      max_key_size
    )
  end

  defp due_key_size(type, state_size, priority_size, tag_size),
    do:
      2 + tag_size + 3 + encoded_index_component_size(byte_size(type)) + 1 +
        encoded_index_component_size(state_size) + 2 + priority_size

  defp due_any_key_size(type, priority_size, tag_size),
    do: 2 + tag_size + 4 + encoded_index_component_size(byte_size(type)) + 2 + priority_size

  defp encoded_index_component_size(size) when is_integer(size) and size >= 0,
    do: div(size * 4 + 2, 3)

  defp priority_key_size(nil), do: max_key_priority_len()
  defp priority_key_size(priority), do: integer_decimal_size(priority)
  defp max_key_priority_len, do: integer_decimal_size(@max_priority)
  defp integer_decimal_size(value) when value < 10, do: 1
  defp integer_decimal_size(value), do: value |> Integer.to_string() |> byte_size()

  defp max_binary_size([head | tail]) do
    Enum.reduce(tail, byte_size(head), fn value, max_size -> max(max_size, byte_size(value)) end)
  end

  defp max_partition_tag_size([head | tail]) do
    Enum.reduce(tail, partition_tag_size(head), fn partition_key, max_size ->
      max(max_size, partition_tag_size(partition_key))
    end)
  end

  defp max_claim_partition_tag_size(partition_keys) when is_list(partition_keys),
    do: max_partition_tag_size(partition_keys)

  defp max_claim_partition_tag_size(partition_key), do: partition_tag_size(partition_key)

  defp partition_tag_size(nil), do: 3
  defp partition_tag_size(:any), do: 3
  defp partition_tag_size(:auto), do: 3

  defp partition_tag_size(partition_key),
    do: partition_key |> Ferricstore.Flow.Keys.tag() |> byte_size()

  defp validate_generated_key_size(size, max_key_size) when size <= max_key_size, do: :ok

  defp validate_generated_key_size(_size, max_key_size),
    do: {:error, "ERR key too large (max #{max_key_size} bytes)"}

  defp maybe_put_attr(attrs, _key, nil), do: attrs
  defp maybe_put_attr(attrs, key, value), do: Map.put(attrs, key, value)

  defp flow_start_time, do: System.monotonic_time()
end
