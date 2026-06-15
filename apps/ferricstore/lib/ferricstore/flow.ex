defmodule Ferricstore.Flow do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.ClaimWaiters
  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.Telemetry, as: FlowTelemetry
  alias Ferricstore.Store.Router

  @default_priority 0
  @claim_waiter_min_wake_budget_per_ready_bucket 8
  @default_payload_return_max_bytes 64 * 1024

  def create(ctx, id, opts) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- Ferricstore.Flow.MutationAttrs.create_attrs(id, opts) do
        ctx
        |> Router.flow_create(attrs)
        |> maybe_notify_claim_waiters(attrs, :state)
      end

    FlowTelemetry.observe(:create, started, result, %{
      flow_id: id,
      flow_type: Keyword.get(opts, :type),
      _count: 1
    })
  end

  def start_and_claim(ctx, id, type, initial_state, opts)
      when is_binary(id) and is_binary(type) and is_binary(initial_state) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, return_mode} <- start_and_claim_return_mode(opts),
           {:ok, attrs} <-
             Ferricstore.Flow.MutationAttrs.start_and_claim_attrs(
               id,
               type,
               initial_state,
               opts
             ) do
        Router.flow_start_and_claim(ctx, attrs)
        |> maybe_return_start_and_claim(ctx, return_mode)
      end

    FlowTelemetry.observe(:start_and_claim, started, result, %{
      flow_id: id,
      flow_type: type,
      from_state: initial_state,
      _count: 1
    })
  end

  def start_and_claim(_ctx, _id, _type, _initial_state, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  @doc false
  def run_steps_many(_ctx, [], _opts), do: :ok

  def run_steps_many(ctx, items, opts) when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs_list} <- run_steps_many_attrs(items, opts) do
        Router.flow_run_steps_many(ctx, attrs_list)
      end

    FlowTelemetry.observe(:run_steps_many, started, result, %{
      flow_id: nil,
      flow_type: Keyword.get(opts, :type),
      _count: if(is_list(items), do: length(items), else: 0)
    })
  end

  def run_steps_many(_ctx, _items, _opts),
    do: {:error, "ERR flow run_steps_many opts must be a keyword list"}

  defp run_steps_many_attrs(items, opts) do
    with {:ok, type} <- run_steps_many_type(opts),
         {:ok, worker} <- run_steps_many_worker(opts),
         {:ok, states} <- run_steps_many_states(opts),
         {:ok, lease_ms} <- run_steps_many_pos_integer(opts, :lease_ms, 30_000),
         {:ok, now_ms} <- run_steps_many_now_ms(opts) do
      base_start_opts =
        [
          worker: worker,
          lease_ms: lease_ms,
          now_ms: now_ms
        ]
        |> run_steps_many_maybe_put(:payload, Keyword.get(opts, :payload))
        |> run_steps_many_maybe_put(:retention_ttl_ms, Keyword.get(opts, :retention_ttl_ms))

      result = Keyword.get(opts, :result)
      step_count = length(states)
      final_run_state = List.last(states)

      items
      |> Enum.reduce_while({:ok, []}, fn item, {:ok, acc} ->
        with {:ok, id, partition_key} <- run_steps_many_item(item),
             start_opts <- Keyword.put(base_start_opts, :partition_key, partition_key),
             {:ok, attrs} <-
               Ferricstore.Flow.MutationAttrs.start_and_claim_attrs(
                 id,
                 type,
                 hd(states),
                 start_opts
               ) do
          attrs =
            attrs
            |> Map.put(:step_states, states)
            |> Map.put(:step_count, step_count)
            |> Map.put(:final_run_state, final_run_state)
            |> Map.put(:step_now_ms, now_ms)
            |> run_steps_many_maybe_put_attr(:result, result)

          {:cont, {:ok, [attrs | acc]}}
        else
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, attrs_list} -> {:ok, Enum.reverse(attrs_list)}
        {:error, _reason} = error -> error
      end
    end
  end

  defp run_steps_many_item(%{id: id} = item) when is_binary(id) and id != "" do
    partition_key = Map.get(item, :partition_key) || Ferricstore.Flow.Keys.auto_partition_key(id)
    {:ok, id, partition_key}
  end

  defp run_steps_many_item(%{"id" => id} = item) when is_binary(id) and id != "" do
    partition_key = Map.get(item, "partition_key") || Ferricstore.Flow.Keys.auto_partition_key(id)
    {:ok, id, partition_key}
  end

  defp run_steps_many_item(id) when is_binary(id) and id != "" do
    {:ok, id, Ferricstore.Flow.Keys.auto_partition_key(id)}
  end

  defp run_steps_many_item(_item), do: {:error, "ERR flow id must be a non-empty string"}

  defp run_steps_many_type(opts) do
    case Keyword.fetch(opts, :type) do
      {:ok, type} when is_binary(type) and type != "" -> {:ok, type}
      _ -> {:error, "ERR flow type must be a non-empty string"}
    end
  end

  defp run_steps_many_worker(opts) do
    case Keyword.fetch(opts, :worker) do
      {:ok, worker} when is_binary(worker) and worker != "" -> {:ok, worker}
      _ -> {:error, "ERR flow worker must be a non-empty string"}
    end
  end

  defp run_steps_many_states(opts) do
    cond do
      is_list(Keyword.get(opts, :states)) ->
        states = Keyword.fetch!(opts, :states)

        if states != [] and Enum.all?(states, &(is_binary(&1) and &1 != "")) do
          {:ok, states}
        else
          {:error, "ERR flow states must be a non-empty list of strings"}
        end

      true ->
        with {:ok, steps} <- run_steps_many_pos_integer(opts, :steps, 1) do
          {:ok, Enum.map(1..steps, &("step_" <> Integer.to_string(&1)))}
        end
    end
  end

  defp run_steps_many_now_ms(opts) do
    case Keyword.fetch(opts, :now_ms) do
      {:ok, now_ms} when is_integer(now_ms) and now_ms >= 0 -> {:ok, now_ms}
      :error -> {:ok, System.system_time(:millisecond)}
      _ -> {:error, "ERR flow now_ms must be a non-negative integer"}
    end
  end

  defp run_steps_many_pos_integer(opts, key, default) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) and value > 0 -> {:ok, value}
      :error -> {:ok, default}
      _ -> {:error, "ERR flow #{key} must be a positive integer"}
    end
  end

  defp run_steps_many_maybe_put(opts, _key, nil), do: opts
  defp run_steps_many_maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp run_steps_many_maybe_put_attr(attrs, _key, nil), do: attrs
  defp run_steps_many_maybe_put_attr(attrs, key, value), do: Map.put(attrs, key, value)

  defp start_and_claim_return_mode(opts) do
    case Keyword.get(opts, :return, :record) do
      value when value in [nil, :record, :records, :full] ->
        {:ok, :record}

      value when value in [:jobs_compact, :job_compact] ->
        {:ok, :jobs_compact}

      value when is_binary(value) ->
        parse_start_and_claim_return_string(value)

      _ ->
        {:error, "ERR flow start_and_claim return must be record or jobs_compact"}
    end
  end

  defp parse_start_and_claim_return_string(value) do
    case String.upcase(value) do
      "RECORD" -> {:ok, :record}
      "RECORDS" -> {:ok, :record}
      "FULL" -> {:ok, :record}
      "JOBS_COMPACT" -> {:ok, :jobs_compact}
      "JOB_COMPACT" -> {:ok, :jobs_compact}
      _ -> {:error, "ERR flow start_and_claim return must be record or jobs_compact"}
    end
  end

  defp maybe_return_start_and_claim({:ok, record}, _ctx, :record), do: {:ok, record}

  defp maybe_return_start_and_claim({:ok, record}, ctx, :jobs_compact) do
    [job] = Ferricstore.Flow.ClaimDueAPI.return_records(ctx, [record], nil, :jobs_compact, [])
    {:ok, job}
  end

  defp maybe_return_start_and_claim(result, _ctx, _return_mode), do: result

  defdelegate value_put(ctx, value, opts \\ []), to: Ferricstore.Flow.ValueStore
  defdelegate value_mget(ctx, refs, opts \\ []), to: Ferricstore.Flow.ValueStore

  defdelegate signal(ctx, id, opts), to: Ferricstore.Flow.Signal, as: :run

  @doc false
  def create_batch_independent(_ctx, []), do: []

  def create_batch_independent(ctx, creates) when is_list(creates) do
    started = flow_start_time()

    {valid, indexed_results} =
      creates
      |> Enum.with_index()
      |> Enum.reduce({[], %{}}, fn
        {{id, opts}, idx}, {valid_acc, result_acc} when is_binary(id) and is_list(opts) ->
          case Ferricstore.Flow.MutationAttrs.create_attrs(id, opts) do
            {:ok, attrs} -> {[{idx, attrs} | valid_acc], result_acc}
            {:error, _reason} = error -> {valid_acc, Map.put(result_acc, idx, error)}
          end

        {_bad, idx}, {valid_acc, result_acc} ->
          {valid_acc, Map.put(result_acc, idx, {:error, "ERR flow opts must be a keyword list"})}
      end)

    valid = Enum.reverse(valid)
    valid_attrs = Enum.map(valid, fn {_idx, attrs} -> attrs end)

    valid_results =
      ctx
      |> Router.flow_create_batch(valid_attrs)
      |> maybe_notify_claim_waiters(valid_attrs, :state)

    indexed_results =
      valid
      |> Enum.map(fn {idx, _attrs} -> idx end)
      |> Enum.zip(valid_results)
      |> Enum.reduce(indexed_results, fn {idx, result}, acc -> Map.put(acc, idx, result) end)

    results = for idx <- 0..(length(creates) - 1), do: Map.fetch!(indexed_results, idx)
    FlowTelemetry.observe_batch(:create, started, results)
    results
  end

  def create_batch_independent(_ctx, _creates),
    do: [{:error, "ERR flow opts must be a keyword list"}]

  def create_many(ctx, partition_key, items, opts)
      when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_opts(opts, return: true),
           {:ok, return_mode} <- many_return_mode(opts),
           {:ok, independent?} <- optional_boolean(opts, :independent, false),
           :ok <- Ferricstore.Flow.MutationAttrs.validate_create_many_items(items),
           {:ok, attrs_list} <-
             Ferricstore.Flow.MutationAttrs.create_many_attrs(items, opts, partition_key),
           :ok <-
             Ferricstore.Flow.MutationAttrs.validate_unique_create_ids(attrs_list, independent?) do
        if independent? do
          result =
            if return_mode == :ok_on_success do
              Router.flow_create_pipeline_batch_ok_on_success(ctx, attrs_list)
            else
              Router.flow_create_many_independent(ctx, attrs_list)
            end

          result
          |> then(&{:ok, &1})
          |> maybe_notify_claim_waiters(attrs_list, :state)
        else
          ctx
          |> Router.flow_create_many(partition_key, attrs_list)
          |> maybe_notify_claim_waiters(attrs_list, :state)
        end
      end

    FlowTelemetry.observe(:create, started, result, %{
      flow_id: nil,
      flow_type: Keyword.get(opts, :type),
      _count: length(items)
    })
  end

  def create_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def spawn_children(ctx, parent_id, children, opts)
      when is_binary(parent_id) and is_list(children) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <-
             Ferricstore.Flow.MutationAttrs.spawn_children_attrs(parent_id, children, opts) do
        Router.flow_spawn_children(ctx, attrs)
      end

    FlowTelemetry.observe(:spawn_children, started, result, %{flow_id: parent_id, _count: 1})
  end

  def spawn_children(_ctx, parent_id, _children, _opts) when not is_binary(parent_id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def spawn_children(_ctx, _parent_id, children, _opts) when not is_list(children),
    do: {:error, "ERR flow children must be a non-empty list"}

  def spawn_children(_ctx, _parent_id, _children, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def get(ctx, id, opts \\ []) when is_binary(id) and is_list(opts) do
    with :ok <- validate_id(id),
         :ok <- validate_opts(opts),
         {:ok, payload_return} <- payload_return_opts(opts, false),
         {:ok, named_values} <- named_value_return_opts(opts),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)) do
      case Router.flow_get(ctx, id, partition_key) do
        nil ->
          {:ok, nil}

        value when is_binary(value) ->
          value
          |> safe_decode_record()
          |> then(&Ferricstore.Flow.ValueHydration.payload_result(ctx, &1, payload_return))
          |> Ferricstore.Flow.ValueHydration.named_value_result(ctx, named_values)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defdelegate policy_set(ctx, type, opts), to: Ferricstore.Flow.Policy, as: :set
  defdelegate policy_get(ctx, type, opts \\ []), to: Ferricstore.Flow.Policy, as: :get

  defdelegate claim_due(ctx, type, opts), to: Ferricstore.Flow.ClaimDueAPI
  defdelegate reclaim(ctx, type, opts), to: Ferricstore.Flow.ClaimDueAPI

  false

  defdelegate claim_due_wait_registration(type, opts),
    to: Ferricstore.Flow.ClaimDueAPI,
    as: :wait_registration

  false
  defdelegate claim_due_wait_keys(type, opts), to: Ferricstore.Flow.ClaimDueAPI, as: :wait_keys

  defp maybe_notify_claim_waiters(result, attrs, state_key) when is_map(attrs) do
    maybe_notify_claim_waiters(result, [attrs], state_key)
  end

  defp maybe_notify_claim_waiters(result, attrs_list, state_key) when is_list(attrs_list) do
    if flow_write_succeeded?(result) do
      attrs_list
      |> Enum.reduce([], fn attrs, hints ->
        case claim_waiter_ready_hint(attrs, state_key) do
          nil -> hints
          hint -> [hint | hints]
        end
      end)
      |> ClaimWaiters.notify_ready_many(@claim_waiter_min_wake_budget_per_ready_bucket)
    end

    result
  end

  defp maybe_notify_retry_claim_waiters(result, ctx, attrs) when is_map(attrs) do
    if flow_write_succeeded?(result) and ClaimWaiters.any_waiters?() do
      notify_claim_waiters_from_current_record(ctx, attrs)
    end

    result
  end

  defp maybe_notify_retry_claim_waiters(result, ctx, attrs_list) when is_list(attrs_list) do
    if flow_write_succeeded?(result) and ClaimWaiters.any_waiters?() do
      Enum.each(attrs_list, &notify_claim_waiters_from_current_record(ctx, &1))
    end

    result
  end

  defp notify_claim_waiters_from_current_record(ctx, %{id: id} = attrs) when is_binary(id) do
    case Router.flow_get(ctx, id, Map.get(attrs, :partition_key)) do
      value when is_binary(value) ->
        case safe_decode_record(value) do
          {:ok, record} when is_map(record) -> notify_claim_waiters_for_record(record)
          _other -> :ok
        end

      _other ->
        :ok
    end
  end

  defp notify_claim_waiters_from_current_record(_ctx, _attrs), do: :ok

  defp notify_claim_waiters_for_record(
         %{type: type, state: state, next_run_at_ms: next_run_at_ms} = record
       )
       when is_binary(type) and is_binary(state) and is_integer(next_run_at_ms) do
    priority = Map.get(record, :priority, @default_priority)
    partition_key = Map.get(record, :partition_key)
    now = now_ms()

    cond do
      next_run_at_ms <= now ->
        ClaimWaiters.notify_ready(type, state, priority, partition_key, 1)

      claim_waiters_waiting_for?(type, state, priority, partition_key) ->
        ClaimWaiters.schedule_ready(type, state, priority, partition_key, next_run_at_ms, 1)

      true ->
        :ok
    end
  end

  defp notify_claim_waiters_for_record(_record), do: :ok

  defp claim_waiters_waiting_for?(type, state, priority, partition_key) do
    type
    |> ClaimWaiters.ready_keys(state, priority, partition_key)
    |> Enum.any?(&ClaimWaiters.has_live_waiter?/1)
  end

  @doc false
  defdelegate schedule_claim_due_waiter_next_due(ctx, type, opts),
    to: Ferricstore.Flow.ClaimDueAPI,
    as: :schedule_next_due

  defp flow_write_succeeded?(:ok), do: true
  defp flow_write_succeeded?({:ok, _value}), do: true

  defp flow_write_succeeded?(results) when is_list(results),
    do: Enum.all?(results, &flow_write_succeeded?/1)

  defp flow_write_succeeded?(_result), do: false

  defp claim_waiter_ready_hint(attrs, state_key) do
    if claim_ready_hint_now?(attrs) do
      type = Map.get(attrs, :type)
      state = claim_ready_state(attrs, state_key)
      priority = Map.get(attrs, :priority, @default_priority)
      partition_key = Map.get(attrs, :partition_key)
      limit = max(Map.get(attrs, :limit, 1), 1)

      if is_binary(type) do
        {type, state, priority, partition_key, limit}
      end
    end
  end

  defp claim_ready_state(attrs, state_key) when is_atom(state_key), do: Map.get(attrs, state_key)
  defp claim_ready_state(_attrs, state), do: state

  defp claim_ready_hint_now?(attrs) do
    now = Map.get(attrs, :now_ms) || now_ms()

    case Map.get(attrs, :run_at_ms) do
      run_at_ms when is_integer(run_at_ms) -> run_at_ms <= now
      _ -> true
    end
  end

  def extend_lease(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <-
             Ferricstore.Flow.MutationAttrs.extend_lease_attrs(id, lease_token, opts) do
        Router.flow_extend_lease(ctx, attrs)
      end

    FlowTelemetry.observe(:extend_lease, started, result, %{flow_id: id, _count: 1})
  end

  def complete(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- Ferricstore.Flow.MutationAttrs.complete_attrs(id, lease_token, opts) do
        Router.flow_complete(ctx, attrs)
      end

    FlowTelemetry.observe(:complete, started, result, %{flow_id: id, _count: 1})
  end

  def complete_many(ctx, partition_key, items, opts)
      when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_opts(opts, return: true),
           {:ok, return_mode} <- many_return_mode(opts),
           {:ok, independent?} <- optional_boolean(opts, :independent, false),
           {:ok, terminal_local_only?} <- optional_boolean(opts, :terminal_local_only, false),
           :ok <- Ferricstore.Flow.MutationAttrs.validate_complete_many_items(items),
           {:ok, attrs_list} <-
             Ferricstore.Flow.MutationAttrs.complete_many_attrs(items, opts, partition_key),
           :ok <-
             Ferricstore.Flow.MutationAttrs.validate_unique_transition_ids(
               attrs_list,
               independent?
             ) do
        complete_result =
          cond do
            independent? ->
              flow_terminal_many_independent(ctx, :complete, attrs_list)

            terminal_local_only? ->
              Router.flow_complete_many_local(ctx, attrs_list)

            true ->
              Router.flow_complete_many(ctx, partition_key, attrs_list)
          end

        maybe_return_many_ok_on_success(complete_result, return_mode)
      end

    FlowTelemetry.observe(:complete, started, result, %{flow_id: nil, _count: length(items)})
  end

  def complete_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def transition(ctx, id, from_state, to_state, opts \\ [])
      when is_binary(id) and is_binary(from_state) and is_binary(to_state) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <-
             Ferricstore.Flow.MutationAttrs.transition_attrs(id, from_state, to_state, opts) do
        ctx
        |> Router.flow_transition(attrs)
        |> maybe_notify_claim_waiters(attrs, :to_state)
      end

    FlowTelemetry.observe(:transition, started, result, %{
      flow_id: id,
      from_state: from_state,
      to_state: to_state,
      _count: 1
    })
  end

  def transition_many(ctx, partition_key, from_state, to_state, items, opts)
      when is_binary(from_state) and is_binary(to_state) and is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_opts(opts, return: true),
           {:ok, return_mode} <- many_return_mode(opts),
           {:ok, independent?} <- optional_boolean(opts, :independent, false),
           :ok <- Ferricstore.Flow.MutationAttrs.validate_transition_many_items(items),
           {:ok, attrs_list} <-
             Ferricstore.Flow.MutationAttrs.transition_many_attrs(
               items,
               opts,
               partition_key,
               from_state,
               to_state
             ),
           :ok <-
             Ferricstore.Flow.MutationAttrs.validate_unique_transition_ids(
               attrs_list,
               independent?
             ) do
        transition_result =
          if independent? do
            ctx
            |> Router.flow_transition_batch(attrs_list)
            |> then(&{:ok, &1})
            |> maybe_notify_claim_waiters(attrs_list, :to_state)
          else
            ctx
            |> Router.flow_transition_many(partition_key, attrs_list)
            |> maybe_notify_claim_waiters(attrs_list, :to_state)
          end

        maybe_return_many_ok_on_success(transition_result, return_mode)
      end

    FlowTelemetry.observe(:transition, started, result, %{
      flow_id: nil,
      from_state: from_state,
      to_state: to_state,
      _count: length(items)
    })
  end

  def transition_many(_ctx, _partition_key, _from_state, _to_state, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def step_continue(ctx, id, lease_token, from_state, to_state, opts \\ [])

  def step_continue(ctx, id, lease_token, from_state, to_state, opts)
      when is_binary(id) and is_binary(lease_token) and is_binary(from_state) and
             is_binary(to_state) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, return_mode} <- step_continue_return_mode(opts),
           {:ok, attrs} <-
             Ferricstore.Flow.MutationAttrs.step_continue_attrs(
               id,
               lease_token,
               from_state,
               to_state,
               opts
             ) do
        Router.flow_step_continue(ctx, attrs)
        |> maybe_return_step_continue(ctx, return_mode)
      end

    FlowTelemetry.observe(:step_continue, started, result, %{
      flow_id: id,
      from_state: from_state,
      to_state: to_state,
      _count: 1
    })
  end

  def step_continue(_ctx, _id, _lease_token, _from_state, _to_state, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  defp step_continue_return_mode(opts) do
    case start_and_claim_return_mode(opts) do
      {:ok, mode} ->
        {:ok, mode}

      {:error, _reason} ->
        {:error, "ERR flow step_continue return must be record or jobs_compact"}
    end
  end

  defp maybe_return_step_continue({:ok, record}, _ctx, :record), do: {:ok, record}

  defp maybe_return_step_continue({:ok, record}, ctx, :jobs_compact) do
    [job] = Ferricstore.Flow.ClaimDueAPI.return_records(ctx, [record], nil, :jobs_compact, [])
    {:ok, job}
  end

  defp maybe_return_step_continue(result, _ctx, _return_mode), do: result

  @doc false
  def transition_batch_independent(_ctx, []), do: []

  def transition_batch_independent(ctx, transitions) when is_list(transitions) do
    started = flow_start_time()

    {valid, indexed_results} =
      transitions
      |> Enum.with_index()
      |> Enum.reduce({[], %{}}, fn
        {{id, from_state, to_state, opts}, idx}, {valid_acc, result_acc}
        when is_binary(id) and is_binary(from_state) and is_binary(to_state) and is_list(opts) ->
          case Ferricstore.Flow.MutationAttrs.transition_attrs(id, from_state, to_state, opts) do
            {:ok, attrs} -> {[{idx, attrs} | valid_acc], result_acc}
            {:error, _reason} = error -> {valid_acc, Map.put(result_acc, idx, error)}
          end

        {_bad, idx}, {valid_acc, result_acc} ->
          {valid_acc, Map.put(result_acc, idx, {:error, "ERR flow opts must be a keyword list"})}
      end)

    valid = Enum.reverse(valid)

    valid_results =
      Router.flow_transition_batch(ctx, Enum.map(valid, fn {_idx, attrs} -> attrs end))

    indexed_results =
      valid
      |> Enum.map(fn {idx, _attrs} -> idx end)
      |> Enum.zip(valid_results)
      |> Enum.reduce(indexed_results, fn {idx, result}, acc -> Map.put(acc, idx, result) end)

    results = for idx <- 0..(length(transitions) - 1), do: Map.fetch!(indexed_results, idx)
    FlowTelemetry.observe_batch(:transition, started, results)
    results
  end

  def transition_batch_independent(_ctx, _transitions),
    do: [{:error, "ERR flow opts must be a keyword list"}]

  @doc false
  def pipeline_write_batch_independent(_ctx, []), do: []

  def pipeline_write_batch_independent(ctx, ops) when is_list(ops) do
    command_callbacks = %{
      create_attrs: &Ferricstore.Flow.MutationAttrs.create_attrs/2,
      start_and_claim_attrs: &Ferricstore.Flow.MutationAttrs.start_and_claim_attrs/4,
      transition_attrs: &Ferricstore.Flow.MutationAttrs.transition_attrs/4,
      step_continue_attrs: &Ferricstore.Flow.MutationAttrs.step_continue_attrs/5,
      named_value_attrs: &Ferricstore.Flow.ValueStore.named_value_attrs/2,
      signal_attrs: &Ferricstore.Flow.Signal.attrs/2,
      complete_attrs: &Ferricstore.Flow.MutationAttrs.complete_attrs/3,
      retry_attrs: &Ferricstore.Flow.MutationAttrs.retry_attrs/3,
      fail_attrs: &Ferricstore.Flow.MutationAttrs.fail_attrs/3,
      cancel_attrs: &Ferricstore.Flow.MutationAttrs.cancel_attrs/2,
      rewind_attrs: &Ferricstore.Flow.MutationAttrs.rewind_attrs/2
    }

    results =
      Ferricstore.Flow.PipelineWrite.batch_independent(ctx, ops, %{
        start: &flow_start_time/0,
        command: fn op -> Ferricstore.Flow.PipelineWriteCommand.command(op, command_callbacks) end,
        notify: &maybe_notify_claim_waiters/3,
        observe: &FlowTelemetry.observe_batch/3
      })

    pipeline_write_return_results(ctx, ops, results)
  end

  def pipeline_write_batch_independent(_ctx, _ops),
    do: [{:error, "ERR flow opts must be a keyword list"}]

  defp pipeline_write_return_results(ctx, ops, results)
       when is_list(ops) and is_list(results) and length(ops) == length(results) do
    ops
    |> Enum.zip(results)
    |> Enum.map(fn {op, result} -> pipeline_write_return_result(ctx, op, result) end)
  end

  defp pipeline_write_return_results(_ctx, _ops, results), do: results

  defp pipeline_write_return_result(
         ctx,
         {:flow_start_and_claim, _id, _type, _initial_state, opts},
         {:ok, _record} = result
       ) do
    pipeline_start_and_claim_return_result(ctx, opts, result)
  end

  defp pipeline_write_return_result(
         ctx,
         {:start_and_claim, _id, _type, _initial_state, opts},
         {:ok, _record} = result
       ) do
    pipeline_start_and_claim_return_result(ctx, opts, result)
  end

  defp pipeline_write_return_result(
         ctx,
         {:flow_step_continue, _id, _lease_token, _from_state, _to_state, opts},
         {:ok, _record} = result
       ) do
    pipeline_step_continue_return_result(ctx, opts, result)
  end

  defp pipeline_write_return_result(
         ctx,
         {:step_continue, _id, _lease_token, _from_state, _to_state, opts},
         {:ok, _record} = result
       ) do
    pipeline_step_continue_return_result(ctx, opts, result)
  end

  defp pipeline_write_return_result(_ctx, _op, result), do: result

  defp pipeline_start_and_claim_return_result(ctx, opts, result) do
    case start_and_claim_return_mode(opts) do
      {:ok, :jobs_compact} -> maybe_return_start_and_claim(result, ctx, :jobs_compact)
      _ -> result
    end
  end

  defp pipeline_step_continue_return_result(ctx, opts, result) do
    case step_continue_return_mode(opts) do
      {:ok, :jobs_compact} -> maybe_return_step_continue(result, ctx, :jobs_compact)
      _ -> result
    end
  end

  @doc false
  def pipeline_claim_due_batch(_ctx, []), do: []

  def pipeline_claim_due_batch(ctx, ops) when is_list(ops) do
    started = flow_start_time()

    command_callbacks = %{
      optional_now_ms: &optional_now_ms/1,
      payload_return_opts: &payload_return_opts/2,
      named_value_return_opts: &named_value_return_opts/1
    }

    {results, stats} =
      ops
      |> Enum.map(&Ferricstore.Flow.PipelineClaimDueCommand.command(&1, command_callbacks))
      |> pipeline_claim_due_results(ctx, [], %{groups: 0, coalesced_calls: 0, batched_calls: 0})

    :telemetry.execute(
      [:ferricstore, :flow, :pipeline_claim_due_batch],
      Map.merge(stats, %{commands: length(ops), duration_us: flow_elapsed_us(started)}),
      %{source: :resp_pipeline}
    )

    results
  end

  def pipeline_claim_due_batch(_ctx, _ops),
    do: [{:error, "ERR flow opts must be a keyword list"}]

  @doc false
  def pipeline_read_batch(_ctx, []), do: []

  def pipeline_read_batch(ctx, ops) when is_list(ops) do
    Ferricstore.Flow.PipelineRead.batch(ctx, ops, %{
      start: &flow_start_time/0,
      command: &Ferricstore.Flow.PipelineReadCommand.command/2,
      batch_get: &Ferricstore.Store.Router.flow_batch_get/3,
      decode_get: &Ferricstore.Flow.PipelineReadCommand.decode_get/1,
      history_results: &Ferricstore.Flow.PipelineReadCommand.history_results/2,
      observe: &FlowTelemetry.observe_pipeline_read_batch/2
    })
  end

  def pipeline_read_batch(_ctx, _ops),
    do: [{:error, "ERR flow opts must be a keyword list"}]

  def retry(ctx, id, lease_token, opts)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- Ferricstore.Flow.MutationAttrs.retry_attrs(id, lease_token, opts) do
        ctx
        |> Router.flow_retry(attrs)
        |> maybe_notify_retry_claim_waiters(ctx, attrs)
      end

    FlowTelemetry.observe(:retry, started, result, %{flow_id: id, _count: 1})
  end

  def retry_many(ctx, partition_key, items, opts) when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_opts(opts, return: true),
           {:ok, return_mode} <- many_return_mode(opts),
           {:ok, independent?} <- optional_boolean(opts, :independent, false),
           :ok <- Ferricstore.Flow.MutationAttrs.validate_retry_many_items(items),
           {:ok, attrs_list} <-
             Ferricstore.Flow.MutationAttrs.retry_many_attrs(items, opts, partition_key),
           :ok <-
             Ferricstore.Flow.MutationAttrs.validate_unique_transition_ids(
               attrs_list,
               independent?
             ) do
        retry_result =
          if independent? do
            ctx
            |> flow_terminal_many_independent(:retry, attrs_list)
            |> maybe_notify_retry_claim_waiters(ctx, attrs_list)
          else
            ctx
            |> Router.flow_retry_many(partition_key, attrs_list)
            |> maybe_notify_retry_claim_waiters(ctx, attrs_list)
          end

        maybe_return_many_ok_on_success(retry_result, return_mode)
      end

    FlowTelemetry.observe(:retry, started, result, %{flow_id: nil, _count: length(items)})
  end

  def retry_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def fail(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- Ferricstore.Flow.MutationAttrs.fail_attrs(id, lease_token, opts) do
        Router.flow_fail(ctx, attrs)
      end

    FlowTelemetry.observe(:fail, started, result, %{flow_id: id, _count: 1})
  end

  def fail_many(ctx, partition_key, items, opts) when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_opts(opts, return: true),
           {:ok, return_mode} <- many_return_mode(opts),
           {:ok, independent?} <- optional_boolean(opts, :independent, false),
           :ok <- Ferricstore.Flow.MutationAttrs.validate_fail_many_items(items),
           {:ok, attrs_list} <-
             Ferricstore.Flow.MutationAttrs.fail_many_attrs(items, opts, partition_key),
           :ok <-
             Ferricstore.Flow.MutationAttrs.validate_unique_transition_ids(
               attrs_list,
               independent?
             ) do
        fail_result =
          if independent? do
            flow_terminal_many_independent(ctx, :fail, attrs_list)
          else
            Router.flow_fail_many(ctx, partition_key, attrs_list)
          end

        maybe_return_many_ok_on_success(fail_result, return_mode)
      end

    FlowTelemetry.observe(:fail, started, result, %{flow_id: nil, _count: length(items)})
  end

  def fail_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def cancel(ctx, id, opts \\ []) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- Ferricstore.Flow.MutationAttrs.cancel_attrs(id, opts) do
        Router.flow_cancel(ctx, attrs)
      end

    FlowTelemetry.observe(:cancel, started, result, %{flow_id: id, _count: 1})
  end

  def cancel_many(ctx, partition_key, items, opts) when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_opts(opts, return: true),
           {:ok, return_mode} <- many_return_mode(opts),
           {:ok, independent?} <- optional_boolean(opts, :independent, false),
           :ok <- Ferricstore.Flow.MutationAttrs.validate_cancel_many_items(items),
           {:ok, attrs_list} <-
             Ferricstore.Flow.MutationAttrs.cancel_many_attrs(items, opts, partition_key),
           :ok <-
             Ferricstore.Flow.MutationAttrs.validate_unique_transition_ids(
               attrs_list,
               independent?
             ) do
        cancel_result =
          if independent? do
            flow_terminal_many_independent(ctx, :cancel, attrs_list)
          else
            Router.flow_cancel_many(ctx, partition_key, attrs_list)
          end

        maybe_return_many_ok_on_success(cancel_result, return_mode)
      end

    FlowTelemetry.observe(:cancel, started, result, %{flow_id: nil, _count: length(items)})
  end

  def cancel_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  defdelegate retention_cleanup(ctx, opts \\ []), to: Ferricstore.Flow.Retention, as: :cleanup

  def rewind(ctx, id, opts) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- Ferricstore.Flow.MutationAttrs.rewind_attrs(id, opts) do
        ctx
        |> Router.flow_rewind(attrs)
        |> maybe_notify_claim_waiters(attrs, :any)
      end

    FlowTelemetry.observe(:rewind, started, result, %{flow_id: id, _count: 1})
  end

  defdelegate list(ctx, type, opts \\ []), to: Ferricstore.Flow.ReadAPI
  defdelegate terminals(ctx, type, opts \\ []), to: Ferricstore.Flow.ReadAPI
  defdelegate failures(ctx, type, opts \\ []), to: Ferricstore.Flow.ReadAPI

  defp flow_terminal_many_independent(ctx, op, attrs_list)
       when op in [:complete, :retry, :fail, :cancel] do
    commands = Enum.map(attrs_list, &{op, &1})
    {:ok, Router.flow_terminal_command_batch_independent(ctx, commands)}
  end

  defdelegate by_parent(ctx, parent_flow_id, opts \\ []), to: Ferricstore.Flow.ReadAPI
  defdelegate by_root(ctx, root_flow_id, opts \\ []), to: Ferricstore.Flow.ReadAPI
  defdelegate by_correlation(ctx, correlation_id, opts \\ []), to: Ferricstore.Flow.ReadAPI

  defdelegate info(ctx, type, opts \\ []), to: Ferricstore.Flow.InfoAPI

  defdelegate stuck(ctx, type, opts \\ []), to: Ferricstore.Flow.ReadAPI

  defdelegate history(ctx, id, opts \\ []), to: Ferricstore.Flow.HistoryAPI

  if Mix.env() == :test do
    def __flow_history_lmdb_query_scan_count_for_test__(count, reverse? \\ false),
      do: Ferricstore.Flow.HistoryRead.lmdb_query_scan_count_for_test(count, reverse?)
  end

  defp safe_decode_record(value) when is_binary(value) do
    {:ok, Codec.decode_record(value)}
  rescue
    _ -> {:ok, nil}
  end

  @doc false
  # Encodes the current Flow metadata schema. User payload bytes are not encoded
  # here; only payload_ref/result_ref/error_ref metadata is stored.
  defdelegate encode_record(record), to: Codec

  @doc false
  defdelegate encode_record_elixir(record), to: Codec

  @doc false
  defdelegate encode_value(value), to: Codec

  @doc false
  defdelegate decode_value(value), to: Codec

  @doc false
  defdelegate decode_value_with_user_size(value), to: Codec

  @doc false
  defdelegate record_blob?(value), to: Codec

  @doc false
  defdelegate decode_record(value), to: Codec

  defdelegate decode_record_meta(value), to: Codec

  @doc false
  defdelegate decode_record_elixir(value), to: Codec

  @doc false
  defdelegate encode_history_fields(record, event, now_ms, meta \\ %{}), to: Codec

  @doc false
  defdelegate encode_history_fields_elixir(record, event, now_ms, meta \\ %{}), to: Codec

  @doc false
  defdelegate history_snapshot(record, event, now_ms, meta \\ %{}), to: Codec

  @doc false
  defdelegate encode_history_snapshot(snapshot), to: Codec

  @doc false
  defdelegate decode_history_fields(value, context \\ %{}), to: Codec

  @doc false
  defdelegate decode_history_fields_elixir(value, context \\ %{}), to: Codec

  @doc false
  defdelegate flow_record_value_refs(record), to: Codec

  defp flow_start_time, do: System.monotonic_time()

  defp flow_elapsed_us(started) do
    System.convert_time_unit(System.monotonic_time() - started, :native, :microsecond)
  end

  defp validate_opts(opts, allowed \\ []) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, "ERR flow opts must be a keyword list"}

      Keyword.has_key?(opts, :return) and not Keyword.get(allowed, :return, false) ->
        {:error, "ERR flow return option is not supported"}

      true ->
        :ok
    end
  end

  defp many_return_mode(opts) do
    case Keyword.get(opts, :return, :items) do
      nil -> {:ok, :items}
      :items -> {:ok, :items}
      "items" -> {:ok, :items}
      "ITEMS" -> {:ok, :items}
      :ok_on_success -> {:ok, :ok_on_success}
      "ok_on_success" -> {:ok, :ok_on_success}
      "OK_ON_SUCCESS" -> {:ok, :ok_on_success}
      _ -> {:error, "ERR flow return must be items or ok_on_success"}
    end
  end

  defp maybe_return_many_ok_on_success(result, :items), do: result
  defp maybe_return_many_ok_on_success(:ok, :ok_on_success), do: :ok

  defp maybe_return_many_ok_on_success({:ok, results}, :ok_on_success) when is_list(results) do
    if Enum.all?(results, &many_item_success?/1), do: :ok, else: {:ok, results}
  end

  defp maybe_return_many_ok_on_success(result, :ok_on_success), do: result

  defp many_item_success?(:ok), do: true
  defp many_item_success?({:ok, _value}), do: true
  defp many_item_success?(_result), do: false

  defp validate_id(id) when is_binary(id) and id != "", do: :ok
  defp validate_id(_id), do: {:error, "ERR flow id must be a non-empty string"}

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  defp pipeline_claim_due_results(commands, ctx, acc, stats) do
    Ferricstore.Flow.PipelineClaimDue.results(commands, ctx, acc, stats, %{
      claim_due_result: &Ferricstore.Flow.ClaimDueAPI.result/3,
      return_records: &Ferricstore.Flow.ClaimDueAPI.return_records/5,
      normal_attrs: &Ferricstore.Flow.ClaimDueAPI.normal_attrs/3,
      normal_state_filter: &Ferricstore.Flow.ClaimDueAPI.normal_state_filter/1
    })
  end

  defp optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a boolean"}
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

  defp payload_return_opts(opts, default_enabled?) do
    with {:ok, full?} <- optional_boolean(opts, :full, default_enabled?),
         {:ok, enabled?} <- optional_boolean(opts, :payload, full?),
         {:ok, max_bytes} <-
           optional_non_neg_integer(
             opts,
             :payload_max_bytes,
             flow_payload_return_max_bytes()
           ) do
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

  defp optional_now_ms(opts) do
    case Keyword.fetch(opts, :now_ms) do
      {:ok, value} when is_integer(value) and value >= 0 ->
        {:ok, value}

      {:ok, _value} ->
        {:error, "ERR flow now_ms must be a non-negative integer"}

      :error ->
        {:ok, nil}
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

  defp now_ms, do: CommandTime.now_ms()
end
