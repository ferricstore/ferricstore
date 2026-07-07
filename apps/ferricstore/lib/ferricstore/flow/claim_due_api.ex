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

  alias Ferricstore.Flow.{ClaimWaiters, Internal}
  alias Ferricstore.Flow.Telemetry, as: FlowTelemetry
  alias Ferricstore.Store.Router

  @claim_due_block_forever :infinity
  @default_lease_ms 30_000
  @default_limit 1
  @default_max_claim_limit 1_000
  @default_payload_return_max_bytes 64 * 1024
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
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         :ok <- Internal.reject_reserved_type(type, opts),
         {:ok, state} <- optional_claim_states(opts),
         {:ok, worker} <- required_binary(opts, :worker),
         {:ok, lease_ms} <- optional_pos_integer(opts, :lease_ms, @default_lease_ms),
         {:ok, limit} <- optional_claim_limit(opts),
         {:ok, priority} <- optional_priority_or_nil(opts),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, return_mode} <- optional_claim_return(opts),
         {:ok, payload_return} <- payload_return_opts(opts, return_mode == :records),
         {:ok, named_values} <- named_value_return_opts(opts),
         {:ok, reclaim_expired?} <- optional_boolean(opts, :reclaim_expired, true),
         {:ok, reclaim_ratio} <- optional_reclaim_ratio(opts),
         {:ok, governance_limit} <- optional_governance_limit(opts, limit, now),
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

      case router_result_with_governance_limit(
             ctx,
             attrs,
             reclaim_expired?,
             reclaim_ratio,
             governance_limit
           ) do
        {:ok, records} when is_list(records) ->
          {:ok, return_records(ctx, records, payload_return, return_mode, named_values)}

        other ->
          other
      end
    end
  end

  def blocking_result(ctx, type, opts, block_ms) do
    case result(ctx, type, opts, :block) do
      {:ok, [_ | _]} = claimed ->
        claimed

      {:ok, []} ->
        with {:ok, keys, limit} <- wait_registration(type, opts) do
          deadline = block_deadline(block_ms)

          with :ok <- ClaimWaiters.register(keys, self(), waiter_deadline(deadline), limit: limit) do
            try do
              case result(ctx, type, opts, :block) do
                {:ok, [_ | _]} = claimed ->
                  claimed

                {:ok, []} ->
                  schedule_next_due(ctx, type, wait_opts_with_horizon(opts, deadline))
                  wait_loop(ctx, type, opts, deadline, keys, limit)

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

  def wait_registration(type, opts) when is_binary(type) and is_list(opts) do
    with {:ok, keys} <- wait_keys(type, opts),
         {:ok, limit} <- optional_claim_limit(opts) do
      {:ok, keys, limit}
    end
  end

  def wait_keys(type, opts) when is_binary(type) and is_list(opts) do
    with {:ok, state} <- optional_claim_states(opts),
         {:ok, priority} <- optional_priority_or_nil(opts),
         {:ok, partition_key, partition_keys} <- optional_claim_partitions(opts) do
      {:ok, ClaimWaiters.wait_keys(type, state, priority, partition_keys || partition_key)}
    end
  end

  def schedule_next_due(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with {:ok, state_filter} <- optional_claim_states(opts),
         {:ok, partition_key, partition_keys} <- optional_claim_partitions(opts),
         {:ok, priority} <- optional_priority_or_nil(opts) do
      Ferricstore.Flow.ClaimWaiterScheduler.schedule_next_due(
        ctx,
        type,
        state_filter,
        priority,
        partition_keys || partition_key,
        Keyword.get(opts, :wait_horizon_ms)
      )
    else
      _unsupported -> :ok
    end
  end

  def schedule_next_due(_ctx, _type, _opts), do: :ok

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

  defp wait_loop(ctx, type, opts, deadline, keys, limit) do
    waiter_message = ClaimWaiters.message()
    wait_ms = wait_ms(deadline)

    receive do
      {^waiter_message, _key} ->
        case result(ctx, type, opts, :block) do
          {:ok, []} ->
            if expired?(deadline) do
              {:ok, []}
            else
              with :ok <- reregister_waiters(keys, deadline, limit) do
                schedule_next_due(ctx, type, wait_opts_with_horizon(opts, deadline))
                wait_loop(ctx, type, opts, deadline, keys, limit)
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
         %{scope: scope, shard_id: shard_id, amount: amount, now_ms: now_ms}
       ) do
    case Ferricstore.Flow.Governance.LimitCache.spend(ctx, scope,
           shard_id: shard_id,
           amount: amount,
           now_ms: now_ms
         ) do
      {:ok, _spent} ->
        result = router_result(ctx, attrs, reclaim_expired?, reclaim_ratio)
        release_unused_claim_limit(ctx, scope, shard_id, amount, result)
        result

      {:error, _reason} = error ->
        if governed_claim_definitely_empty?(ctx, attrs, reclaim_expired?, reclaim_ratio) do
          {:ok, []}
        else
          error
        end
    end
  end

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

  defp release_unused_claim_limit(ctx, scope, shard_id, amount, {:ok, records})
       when is_list(records) do
    unused = amount - length(records)

    if unused > 0 do
      Ferricstore.Flow.Governance.LimitCache.release(ctx, scope,
        shard_id: shard_id,
        amount: unused
      )
    end
  end

  defp release_unused_claim_limit(ctx, scope, shard_id, amount, {:error, _reason}) do
    Ferricstore.Flow.Governance.LimitCache.release(ctx, scope, shard_id: shard_id, amount: amount)
  end

  defp release_unused_claim_limit(_ctx, _scope, _shard_id, _amount, _result), do: :ok

  defp router_maybe(_ctx, nil), do: {:ok, []}
  defp router_maybe(_ctx, %{limit: limit}) when limit <= 0, do: {:ok, []}
  defp router_maybe(ctx, attrs), do: Router.flow_claim_due(ctx, attrs)

  defp job_response(record) do
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
    [
      Map.get(record, :id),
      Map.get(record, :partition_key),
      Map.get(record, :lease_token),
      Map.get(record, :fencing_token)
    ]
  end

  defp job_compact_attrs_response(record) do
    [
      Map.get(record, :id),
      Map.get(record, :partition_key),
      Map.get(record, :lease_token),
      Map.get(record, :fencing_token),
      Map.get(record, :attributes, %{})
    ]
  end

  defp job_compact_state_response(record) do
    [
      Map.get(record, :id),
      Map.get(record, :partition_key),
      Map.get(record, :lease_token),
      Map.get(record, :fencing_token),
      Map.get(record, :run_state) || Map.get(record, :state)
    ]
  end

  defp job_compact_state_attrs_response(record) do
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
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      {:ok, _value} -> {:error, "ERR flow now_ms must be a non-negative integer"}
      :error -> {:ok, nil}
    end
  end

  defp optional_pos_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a positive integer"}
    end
  end

  defp optional_governance_limit(opts, limit, now_ms) do
    scope = Keyword.get(opts, :governance_limit_scope)
    shard_id = Keyword.get(opts, :governance_shard_id)

    cond do
      is_nil(scope) and is_nil(shard_id) ->
        {:ok, nil}

      is_binary(scope) and scope != "" and is_integer(shard_id) and shard_id >= 0 ->
        {:ok,
         %{
           scope: scope,
           shard_id: shard_id,
           amount: limit,
           now_ms: now_ms || Ferricstore.CommandTime.now_ms()
         }}

      true ->
        {:error,
         "ERR flow governance_limit_scope and governance_shard_id must be provided together"}
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

  defp optional_claim_block_ms(opts), do: optional_non_neg_integer(opts, :block_ms, 0)

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

  defp payload_return_opts(opts, default_enabled?) do
    with {:ok, full?} <- optional_boolean(opts, :full, default_enabled?),
         {:ok, enabled?} <- optional_boolean(opts, :payload, full?),
         {:ok, max_bytes} <-
           optional_non_neg_integer(opts, :payload_max_bytes, flow_payload_return_max_bytes()) do
      {:ok, %{enabled?: enabled?, max_bytes: max_bytes}}
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

  defp flow_payload_return_max_bytes do
    case Application.get_env(
           :ferricstore,
           :flow_payload_return_max_bytes,
           @default_payload_return_max_bytes
         ) do
      value when is_integer(value) and value >= 0 -> value
      _ -> @default_payload_return_max_bytes
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
    validate_claim_due_key_lengths(type, state, nil, partition_key, Router.max_key_size())
  end

  defp validate_claim_due_keys(type, state, priority, partition_key) do
    validate_claim_due_key_lengths(type, state, priority, partition_key, Router.max_key_size())
  end

  defp validate_claim_due_key_lengths(type, :any, _priority, _partition_key, max_key_size) do
    validate_generated_key_size(due_any_key_size(type, max_key_priority_len()), max_key_size)
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
    do: 2 + tag_size + 3 + byte_size(type) + 1 + state_size + 2 + priority_size

  defp due_any_key_size(type, priority_size),
    do: 2 + partition_tag_size(nil) + 4 + byte_size(type) + 2 + priority_size

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
