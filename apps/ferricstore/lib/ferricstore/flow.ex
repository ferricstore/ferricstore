defmodule Ferricstore.Flow do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.ClaimWaiters
  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.RetryPolicy
  alias Ferricstore.Flow.Telemetry, as: FlowTelemetry
  alias Ferricstore.Stats
  alias Ferricstore.Store.Router

  @default_state "queued"
  @default_priority 0
  @max_priority 2
  @claim_waiter_min_wake_budget_per_ready_bucket 8
  @claim_due_block_forever :infinity
  @default_lease_ms 30_000
  @default_limit 1
  @max_history_hot_max_events 10_000
  @max_history_max_events 1_000_000
  @default_max_batch_items 1_000
  @default_max_claim_limit 1_000
  @default_payload_return_max_bytes 64 * 1024
  @max_ref_size 4_096
  @default_max_count 10_000
  @default_lmdb_query_scan_limit 10_000
  @claim_due_cold_schedule_horizon_ms Application.compile_env(
                                        :ferricstore,
                                        :flow_claim_due_cold_schedule_horizon_ms,
                                        24 * 60 * 60 * 1_000
                                      )
  @claim_due_cold_schedule_scan_limit Application.compile_env(
                                        :ferricstore,
                                        :flow_claim_due_cold_schedule_scan_limit,
                                        1_000
                                      )
  @claim_due_cold_schedule_min_horizon_ms Application.compile_env(
                                            :ferricstore,
                                            :flow_claim_due_cold_schedule_min_horizon_ms,
                                            60_000
                                          )
  @claim_due_cold_schedule_bucket_ms 60_000
  @terminal_states ["completed", "failed", "cancelled"]
  @u64_decimal_zero_pad "00000000000000000000"
  @value_bin_magic "FSV2"

  def create(ctx, id, opts) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- create_attrs(id, opts) do
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

  defdelegate value_put(ctx, value, opts \\ []), to: Ferricstore.Flow.ValueStore
  defdelegate value_mget(ctx, refs), to: Ferricstore.Flow.ValueStore

  def signal(ctx, id, opts) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- signal_attrs(id, opts) do
        Router.flow_signal(ctx, attrs)
      end

    FlowTelemetry.observe(:signal, started, result, %{flow_id: id, _count: 1})
  end

  def signal(_ctx, id, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def signal(_ctx, _id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc false
  def create_batch_independent(_ctx, []), do: []

  def create_batch_independent(ctx, creates) when is_list(creates) do
    started = flow_start_time()

    {valid, indexed_results} =
      creates
      |> Enum.with_index()
      |> Enum.reduce({[], %{}}, fn
        {{id, opts}, idx}, {valid_acc, result_acc} when is_binary(id) and is_list(opts) ->
          case create_attrs(id, opts) do
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
           :ok <- validate_opts(opts),
           {:ok, independent?} <- optional_boolean(opts, :independent, false),
           :ok <- validate_create_many_items(items),
           {:ok, attrs_list} <- create_many_attrs(items, opts, partition_key),
           :ok <- validate_unique_create_ids(attrs_list, independent?) do
        if independent? do
          ctx
          |> Router.flow_create_many_independent(attrs_list)
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
      with {:ok, attrs} <- spawn_children_attrs(parent_id, children, opts) do
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
          |> then(&hydrate_payload_result(ctx, &1, payload_return))
          |> hydrate_named_value_result(ctx, named_values)

        {:error, _reason} = error ->
          error
      end
    end
  end

  def policy_set(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         :ok <- validate_key_size(__MODULE__.Keys.policy_key(type)),
         {:ok, policy} <- RetryPolicy.normalize_flow_policy(type, opts) do
      case Router.flow_policy_put_all(
             ctx,
             __MODULE__.Keys.policy_key(type),
             RetryPolicy.encode_flow_policy(policy),
             0
           ) do
        :ok -> {:ok, policy_response(type, policy, Keyword.get(opts, :state))}
        {:error, _reason} = error -> error
      end
    end
  end

  def policy_set(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def policy_get(ctx, type, opts \\ [])

  def policy_get(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, state} <- optional_binary_or_nil(opts, :state, nil),
         :ok <- validate_key_size(__MODULE__.Keys.policy_key(type)),
         {:ok, policy} <- flow_policy_read(ctx, type) do
      {:ok, policy_response(type, policy, state)}
    end
  end

  def policy_get(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def claim_due(ctx, type, opts) when is_binary(type) and is_list(opts) do
    started = flow_start_time()
    blocking? = Keyword.has_key?(opts, :block_ms)

    result =
      with {:ok, block_ms} <- optional_claim_block_ms(opts) do
        claim_opts = Keyword.delete(opts, :block_ms)

        if blocking? do
          claim_due_blocking_result(ctx, type, claim_opts, block_ms)
        else
          claim_due_result(ctx, type, claim_opts)
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
      |> then(&claim_due_result(ctx, type, &1))

    FlowTelemetry.observe(:reclaim, started, result, %{flow_type: type})
  end

  defp claim_due_result(ctx, type, opts), do: claim_due_result(ctx, type, opts, :allow)

  defp claim_due_result(ctx, type, opts, cold_due_mode) do
    with :ok <- validate_opts(opts, return: true),
         :ok <- validate_type(type),
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

      case claim_due_router_result(ctx, attrs, reclaim_expired?, reclaim_ratio) do
        {:ok, records} when is_list(records) ->
          {:ok, claim_due_return_records(ctx, records, payload_return, return_mode, named_values)}

        other ->
          other
      end
    end
  end

  defp claim_due_blocking_result(ctx, type, opts, block_ms) do
    case claim_due_result(ctx, type, opts, :block) do
      {:ok, [_ | _]} = claimed ->
        claimed

      {:ok, []} ->
        with {:ok, keys, limit} <- claim_due_wait_registration(type, opts) do
          deadline = claim_due_block_deadline(block_ms)

          with :ok <-
                 ClaimWaiters.register(keys, self(), claim_due_waiter_deadline(deadline),
                   limit: limit
                 ) do
            try do
              case claim_due_result(ctx, type, opts, :block) do
                {:ok, [_ | _]} = claimed ->
                  claimed

                {:ok, []} ->
                  schedule_claim_due_waiter_next_due(
                    ctx,
                    type,
                    claim_due_wait_opts_with_horizon(opts, deadline)
                  )

                  claim_due_wait_loop(ctx, type, opts, deadline, keys, limit)

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

  defp claim_due_wait_loop(ctx, type, opts, deadline, keys, limit) do
    waiter_message = ClaimWaiters.message()
    wait_ms = claim_due_wait_ms(deadline)

    receive do
      {^waiter_message, _key} ->
        case claim_due_result(ctx, type, opts, :block) do
          {:ok, []} ->
            if claim_due_block_expired?(deadline) do
              {:ok, []}
            else
              with :ok <- reregister_claim_waiters(keys, deadline, limit) do
                schedule_claim_due_waiter_next_due(
                  ctx,
                  type,
                  claim_due_wait_opts_with_horizon(opts, deadline)
                )

                claim_due_wait_loop(ctx, type, opts, deadline, keys, limit)
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

  defp reregister_claim_waiters(keys, deadline, limit) do
    ClaimWaiters.unregister(keys, self())
    ClaimWaiters.register(keys, self(), claim_due_waiter_deadline(deadline), limit: limit)
  end

  defp claim_due_wait_opts_with_horizon(opts, @claim_due_block_forever) do
    Keyword.put(opts, :wait_horizon_ms, @claim_due_cold_schedule_horizon_ms)
  end

  defp claim_due_wait_opts_with_horizon(opts, deadline) when is_integer(deadline) do
    horizon_ms =
      deadline
      |> Kernel.-(System.monotonic_time(:millisecond))
      |> max(0)

    Keyword.put(opts, :wait_horizon_ms, horizon_ms)
  end

  defp claim_due_block_deadline(0), do: @claim_due_block_forever

  defp claim_due_block_deadline(block_ms) when is_integer(block_ms) and block_ms > 0,
    do: System.monotonic_time(:millisecond) + block_ms

  defp claim_due_waiter_deadline(@claim_due_block_forever), do: 0
  defp claim_due_waiter_deadline(deadline), do: deadline

  defp claim_due_wait_ms(@claim_due_block_forever), do: :infinity

  defp claim_due_wait_ms(deadline) do
    deadline
    |> Kernel.-(System.monotonic_time(:millisecond))
    |> max(0)
  end

  defp claim_due_block_expired?(@claim_due_block_forever), do: false
  defp claim_due_block_expired?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  @doc false
  def claim_due_wait_registration(type, opts) when is_binary(type) and is_list(opts) do
    with {:ok, keys} <- claim_due_wait_keys(type, opts),
         {:ok, limit} <- optional_claim_limit(opts) do
      {:ok, keys, limit}
    end
  end

  @doc false
  def claim_due_wait_keys(type, opts) when is_binary(type) and is_list(opts) do
    with {:ok, state} <- optional_claim_states(opts),
         {:ok, priority} <- optional_priority_or_nil(opts),
         {:ok, partition_key, partition_keys} <- optional_claim_partitions(opts) do
      {:ok, ClaimWaiters.wait_keys(type, state, priority, partition_keys || partition_key)}
    end
  end

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
  def schedule_claim_due_waiter_next_due(ctx, type, opts)
      when is_binary(type) and is_list(opts) do
    with {:ok, state_filter} <- optional_claim_states(opts),
         {:ok, partition_key, partition_keys} <- optional_claim_partitions(opts),
         {:ok, priority} <- optional_priority_or_nil(opts) do
      partition_filter = partition_keys || partition_key
      cold_horizon_ms = claim_due_wait_cold_horizon_ms(Keyword.get(opts, :wait_horizon_ms))

      schedule_claim_due_waiter_next_due_filter(
        ctx,
        type,
        state_filter,
        priority,
        partition_filter,
        cold_horizon_ms
      )
    else
      _unsupported -> :ok
    end
  end

  def schedule_claim_due_waiter_next_due(_ctx, _type, _opts), do: :ok

  defp schedule_claim_due_waiter_next_due_filter(
         ctx,
         type,
         state_filter,
         priority,
         partition_filter,
         cold_horizon_ms
       ) do
    case {claim_due_wait_schedule_states(state_filter),
          claim_due_wait_schedule_partitions(partition_filter)} do
      {{:ok, states}, {:ok, partitions}} ->
        priorities = claim_due_wait_schedule_priorities(priority)

        for state <- states,
            partition_key <- partitions,
            priority <- priorities do
          schedule_claim_due_waiter_next_due_key(ctx, type, state, priority, partition_key)
        end

        schedule_claim_due_waiter_next_cold_due(
          ctx,
          type,
          state_filter,
          priority,
          partition_filter,
          cold_horizon_ms
        )

        :ok

      _broad_filter ->
        schedule_claim_due_waiter_next_due_matching(
          ctx,
          type,
          state_filter,
          priority,
          partition_filter
        )

        schedule_claim_due_waiter_next_cold_due(
          ctx,
          type,
          state_filter,
          priority,
          partition_filter,
          cold_horizon_ms
        )
    end
  end

  defp claim_due_wait_schedule_states(state) do
    case state do
      state when is_binary(state) -> {:ok, [state]}
      states when is_list(states) -> claim_due_wait_schedule_binary_list(states)
      _unsupported -> :unsupported
    end
  end

  defp claim_due_wait_schedule_partitions(partition_filter) do
    case partition_filter do
      partition_key when is_binary(partition_key) ->
        {:ok, [partition_key]}

      partition_keys when is_list(partition_keys) ->
        claim_due_wait_schedule_binary_list(partition_keys)

      _unsupported ->
        :unsupported
    end
  end

  defp claim_due_wait_schedule_binary_list(values) when is_list(values) do
    if values != [] and Enum.all?(values, &is_binary/1), do: {:ok, values}, else: :unsupported
  end

  defp claim_due_wait_schedule_priorities(nil), do: [2, 1, 0]
  defp claim_due_wait_schedule_priorities(priority) when is_integer(priority), do: [priority]
  defp claim_due_wait_schedule_priorities(_priority), do: []

  defp schedule_claim_due_waiter_next_due_matching(
         ctx,
         type,
         state_filter,
         priority_filter,
         partition_filter
       ) do
    with {:ok, due_keys} <- Router.flow_due_count_keys(ctx),
         matched_keys =
           Enum.filter(
             due_keys,
             &claim_due_wait_due_key_matches?(
               &1,
               type,
               state_filter,
               priority_filter,
               partition_filter
             )
           ),
         true <- matched_keys != [],
         {:ok, results} <-
           Router.flow_index_rank_range_many(
             ctx,
             Enum.map(matched_keys, &{&1, 0, 0, false})
           ),
         due_at when is_integer(due_at) <- earliest_claim_due_wait_score(results) do
      for state <- claim_due_wait_notify_states(state_filter),
          partition_key <- claim_due_wait_notify_partitions(partition_filter) do
        ClaimWaiters.schedule_ready(type, state, priority_filter, partition_key, due_at, 1)
      end

      :ok
    else
      _other -> :ok
    end
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp schedule_claim_due_waiter_next_cold_due(
         ctx,
         type,
         state_filter,
         priority_filter,
         partition_filter,
         cold_horizon_ms
       ) do
    with true <- Ferricstore.Flow.Hibernation.enabled?(),
         true <- cold_horizon_ms >= @claim_due_cold_schedule_min_horizon_ms,
         due_at when is_integer(due_at) <-
           earliest_claim_due_wait_cold_score(
             ctx,
             type,
             state_filter,
             priority_filter,
             partition_filter,
             cold_horizon_ms
           ) do
      for state <- claim_due_wait_notify_states(state_filter),
          partition_key <- claim_due_wait_notify_partitions(partition_filter) do
        ClaimWaiters.schedule_ready(type, state, priority_filter, partition_key, due_at, 1)
      end

      :ok
    else
      _other -> :ok
    end
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp earliest_claim_due_wait_cold_score(
         ctx,
         type,
         state_filter,
         priority_filter,
         partition_filter,
         cold_horizon_ms
       )
       when is_binary(type) do
    now_ms = CommandTime.now_ms()

    ctx
    |> claim_due_wait_cold_lmdb_paths()
    |> Enum.reduce(nil, fn path, earliest ->
      path
      |> claim_due_wait_cold_bucket_prefixes(
        type,
        state_filter,
        priority_filter,
        partition_filter,
        now_ms,
        cold_horizon_ms
      )
      |> Enum.reduce_while(earliest, fn prefix, current ->
        case Ferricstore.Flow.LMDB.prefix_entries(
               path,
               prefix,
               @claim_due_cold_schedule_scan_limit
             ) do
          {:ok, entries} ->
            next =
              entries
              |> Enum.reduce(current, fn {_due_key, park_key}, acc ->
                case claim_due_wait_cold_due_at(
                       path,
                       park_key,
                       type,
                       state_filter,
                       priority_filter,
                       partition_filter
                     ) do
                  due_at when is_integer(due_at) ->
                    if is_integer(acc), do: min(acc, due_at), else: due_at

                  _ ->
                    acc
                end
              end)

            if is_integer(next) and next <= now_ms do
              {:halt, next}
            else
              {:cont, next}
            end

          _other ->
            {:cont, current}
        end
      end)
    end)
  end

  defp earliest_claim_due_wait_cold_score(_ctx, _type, _state, _priority, _partition, _horizon),
    do: nil

  defp claim_due_wait_cold_lmdb_paths(%{data_dir: data_dir, shard_count: shard_count})
       when is_binary(data_dir) and is_integer(shard_count) and shard_count > 0 do
    for shard_index <- 0..(shard_count - 1) do
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()
    end
  end

  defp claim_due_wait_cold_lmdb_paths(_ctx), do: []

  defp claim_due_wait_cold_bucket_prefixes(
         path,
         type,
         state_filter,
         priority_filter,
         partition_filter,
         now_ms,
         cold_horizon_ms
       ) do
    buckets = claim_due_wait_cold_schedule_buckets(now_ms, cold_horizon_ms)

    case {claim_due_wait_schedule_states(state_filter),
          claim_due_wait_schedule_partitions(partition_filter)} do
      {{:ok, states}, {:ok, partitions}} ->
        priorities = claim_due_wait_schedule_priorities(priority_filter)

        if priorities == [] do
          claim_due_wait_cold_type_prefixes(buckets, type)
        else
          for bucket_ms <- buckets,
              state <- states,
              partition_key <- partitions,
              priority <- priorities do
            Ferricstore.Flow.LMDB.cold_due_claim_prefix(
              bucket_ms: bucket_ms,
              type: type,
              state: state,
              partition_key: partition_key,
              priority: priority
            )
          end
        end

      _broad_filter ->
        claim_due_wait_cold_type_prefixes(buckets, type)
    end
    |> Enum.filter(&claim_due_wait_cold_prefix_present?(path, &1))
  end

  defp claim_due_wait_cold_type_prefixes(buckets, type) do
    Enum.map(buckets, &Ferricstore.Flow.LMDB.cold_due_type_bucket_prefix(&1, type))
  end

  defp claim_due_wait_cold_prefix_present?(path, prefix) do
    case Ferricstore.Flow.LMDB.prefix_entries(path, prefix, 1) do
      {:ok, [_ | _]} -> true
      _ -> false
    end
  end

  defp claim_due_wait_cold_horizon_ms(value) when is_integer(value) and value >= 0 do
    min(value, @claim_due_cold_schedule_horizon_ms)
  end

  defp claim_due_wait_cold_horizon_ms(_value), do: @claim_due_cold_schedule_horizon_ms

  defp claim_due_wait_cold_schedule_buckets(now_ms, horizon_ms) do
    first = Ferricstore.Flow.LMDB.cold_due_bucket_ms(now_ms, @claim_due_cold_schedule_bucket_ms)
    horizon = now_ms + claim_due_wait_cold_horizon_ms(horizon_ms)
    last = Ferricstore.Flow.LMDB.cold_due_bucket_ms(horizon, @claim_due_cold_schedule_bucket_ms)

    first
    |> Stream.iterate(&(&1 + @claim_due_cold_schedule_bucket_ms))
    |> Stream.take_while(&(&1 <= last))
    |> Enum.to_list()
  end

  defp claim_due_wait_cold_due_at(
         path,
         park_key,
         type,
         state_filter,
         priority_filter,
         partition_filter
       )
       when is_binary(park_key) do
    with {:ok, park_blob} <- Ferricstore.Flow.LMDB.get(path, park_key),
         {:ok, park} <- Ferricstore.Flow.LMDB.decode_cold_park(park_blob),
         true <-
           claim_due_wait_cold_park_matches?(
             park,
             type,
             state_filter,
             priority_filter,
             partition_filter
           ),
         due_at when is_integer(due_at) <- Map.get(park, :due_at_ms) do
      due_at
    else
      _other -> nil
    end
  end

  defp claim_due_wait_cold_due_at(_path, _park_key, _type, _state, _priority, _partition),
    do: nil

  defp claim_due_wait_cold_park_matches?(
         park,
         type,
         state_filter,
         priority_filter,
         partition_filter
       )
       when is_map(park) do
    Map.get(park, :type) == type and
      claim_due_wait_cold_state_match?(Map.get(park, :state), state_filter) and
      claim_due_wait_cold_priority_match?(Map.get(park, :priority, 0), priority_filter) and
      claim_due_wait_cold_partition_match?(Map.get(park, :partition_key), partition_filter)
  end

  defp claim_due_wait_cold_park_matches?(_park, _type, _state, _priority, _partition),
    do: false

  defp claim_due_wait_cold_state_match?(state, state_filter)
       when is_binary(state) and state_filter in [nil, :any],
       do: true

  defp claim_due_wait_cold_state_match?(state, states) when is_binary(state) and is_list(states),
    do: state in states

  defp claim_due_wait_cold_state_match?(state, state), do: true
  defp claim_due_wait_cold_state_match?(_state, _filter), do: false

  defp claim_due_wait_cold_priority_match?(_priority, nil), do: true
  defp claim_due_wait_cold_priority_match?(priority, priority), do: true
  defp claim_due_wait_cold_priority_match?(_priority, _filter), do: false

  defp claim_due_wait_cold_partition_match?(partition, partition_filter)
       when is_binary(partition) and partition_filter in [nil, :any],
       do: true

  defp claim_due_wait_cold_partition_match?(partition, :auto) when is_binary(partition),
    do: String.starts_with?(partition, "__flow_auto__:")

  defp claim_due_wait_cold_partition_match?(partition, partitions)
       when is_binary(partition) and is_list(partitions),
       do: partition in partitions

  defp claim_due_wait_cold_partition_match?(partition, partition), do: true
  defp claim_due_wait_cold_partition_match?(_partition, _filter), do: false

  defp earliest_claim_due_wait_score(results) when is_list(results) do
    results
    |> Enum.reduce(nil, fn
      [{_id, score} | _], nil when is_number(score) ->
        round(score)

      [{_id, score} | _], current when is_number(score) ->
        min(current, round(score))

      _other, current ->
        current
    end)
  end

  defp earliest_claim_due_wait_score(_results), do: nil

  defp claim_due_wait_notify_states(states) when is_list(states) do
    states
    |> Enum.filter(&is_binary/1)
    |> case do
      [] -> [:any]
      values -> Enum.uniq(values)
    end
  end

  defp claim_due_wait_notify_states(state) when is_binary(state), do: [state]
  defp claim_due_wait_notify_states(_state), do: [:any]

  defp claim_due_wait_notify_partitions(partitions) when is_list(partitions) do
    partitions
    |> Enum.filter(&is_binary/1)
    |> case do
      [] -> [:any]
      values -> Enum.uniq(values)
    end
  end

  defp claim_due_wait_notify_partitions(partition) when is_binary(partition), do: [partition]
  defp claim_due_wait_notify_partitions(partition), do: [partition]

  defp claim_due_wait_due_key_matches?(key, type, state_filter, priority_filter, partition_filter)
       when is_binary(key) and is_binary(type) do
    claim_due_wait_due_key?(key) and
      claim_due_wait_partition_match?(key, partition_filter) and
      claim_due_wait_state_match?(key, type, state_filter) and
      claim_due_wait_priority_match?(key, priority_filter)
  end

  defp claim_due_wait_due_key_matches?(_key, _type, _state_filter, _priority, _partition),
    do: false

  defp claim_due_wait_due_key?(key) do
    String.starts_with?(key, "f:{") and
      (:binary.match(key, "}:d:") != :nomatch or :binary.match(key, "}:da:") != :nomatch)
  end

  defp claim_due_wait_partition_match?(_key, nil), do: true
  defp claim_due_wait_partition_match?(_key, :any), do: true

  defp claim_due_wait_partition_match?(key, :auto),
    do: String.starts_with?(key, "f:{fa:") and claim_due_wait_due_key?(key)

  defp claim_due_wait_partition_match?(key, partitions) when is_list(partitions),
    do: Enum.any?(partitions, &claim_due_wait_partition_match?(key, &1))

  defp claim_due_wait_partition_match?(key, partition_key) do
    tag = __MODULE__.Keys.tag(partition_key)

    String.starts_with?(key, "f:" <> tag <> ":d:") or
      String.starts_with?(key, "f:" <> tag <> ":da:")
  end

  defp claim_due_wait_state_match?(key, type, state_filter) when state_filter in [nil, :any] do
    String.contains?(key, "}:d:" <> type <> ":") or
      String.contains?(key, "}:da:" <> type <> ":p")
  end

  defp claim_due_wait_state_match?(key, type, states) when is_list(states),
    do: Enum.any?(states, &claim_due_wait_state_match?(key, type, &1))

  defp claim_due_wait_state_match?(key, type, state) when is_binary(state),
    do: String.contains?(key, "}:d:" <> type <> ":" <> state <> ":p")

  defp claim_due_wait_state_match?(_key, _type, _state), do: false

  defp claim_due_wait_priority_match?(_key, nil), do: true

  defp claim_due_wait_priority_match?(key, priority) when is_integer(priority),
    do: String.ends_with?(key, ":p" <> Integer.to_string(priority))

  defp claim_due_wait_priority_match?(_key, _priority), do: false

  defp schedule_claim_due_waiter_next_due_key(ctx, type, state, priority, partition_key) do
    key = __MODULE__.Keys.due_key(type, state, priority, partition_key)

    case Router.flow_index_rank_range(ctx, key, 0, 0, false) do
      {:ok, [{_id, score}]} ->
        due_at_ms = round(score)
        ClaimWaiters.schedule_ready(type, state, priority, partition_key, due_at_ms, 1)

      _other ->
        :ok
    end
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp flow_write_succeeded?(:ok), do: true
  defp flow_write_succeeded?({:ok, _value}), do: true

  defp flow_write_succeeded?(results) when is_list(results),
    do: Enum.all?(results, &flow_write_succeeded?/1)

  defp flow_write_succeeded?(_result), do: false

  defp claim_waiter_ready_hint(attrs, state_key) do
    if claim_ready_hint_now?(attrs) do
      type = Map.get(attrs, :type)
      state = claim_ready_state(attrs, state_key)
      priority = Map.get(attrs, :priority)
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

  defp claim_due_return_records(_ctx, records, _payload_return, :jobs, _named_values),
    do: Enum.map(records, &claim_due_job_response/1)

  defp claim_due_return_records(_ctx, records, _payload_return, :jobs_compact, _named_values),
    do: Enum.map(records, &claim_due_job_compact_response/1)

  defp claim_due_return_records(
         _ctx,
         records,
         _payload_return,
         :jobs_compact_state,
         _named_values
       ),
       do: Enum.map(records, &claim_due_job_compact_state_response/1)

  defp claim_due_return_records(ctx, records, payload_return, :records, named_values) do
    hydrated = hydrate_payload_records(ctx, records, payload_return)
    hydrate_named_value_records(ctx, hydrated, named_values)
  end

  defp claim_due_job_response(record) do
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

  defp claim_due_job_compact_response(record) do
    [
      Map.get(record, :id),
      Map.get(record, :partition_key),
      Map.get(record, :lease_token),
      Map.get(record, :fencing_token)
    ]
  end

  defp claim_due_job_compact_state_response(record) do
    [
      Map.get(record, :id),
      Map.get(record, :partition_key),
      Map.get(record, :lease_token),
      Map.get(record, :fencing_token),
      Map.get(record, :run_state) || Map.get(record, :state)
    ]
  end

  defp claim_due_router_result(ctx, %{state: "running"} = attrs, _reclaim_expired?, _ratio) do
    Router.flow_claim_due(ctx, attrs)
  end

  defp claim_due_router_result(ctx, attrs, false, _ratio) do
    normal_state = claim_normal_state_filter(Map.fetch!(attrs, :state))

    claim_due_router_maybe(
      ctx,
      claim_normal_attrs(attrs, normal_state, Map.fetch!(attrs, :limit))
    )
  end

  defp claim_due_router_result(ctx, attrs, true, reclaim_ratio) when reclaim_ratio > 0 do
    limit = Map.fetch!(attrs, :limit)
    initial_reclaim_limit = max(1, div(limit * reclaim_ratio + 99, 100))
    normal_state = claim_normal_state_filter(Map.fetch!(attrs, :state))

    with {:ok, reclaimed_first} <-
           claim_due_router_maybe(ctx, %{attrs | state: "running", limit: initial_reclaim_limit}),
         remaining_after_reclaim = limit - length(reclaimed_first),
         {:ok, normal} <-
           claim_due_router_maybe(
             ctx,
             claim_normal_attrs(attrs, normal_state, remaining_after_reclaim)
           ),
         remaining_after_normal = limit - length(reclaimed_first) - length(normal),
         {:ok, reclaimed_more} <-
           claim_due_router_maybe(ctx, %{attrs | state: "running", limit: remaining_after_normal}) do
      {:ok, reclaimed_first ++ normal ++ reclaimed_more}
    end
  end

  defp claim_due_router_result(ctx, attrs, _reclaim_expired?, _ratio) do
    Router.flow_claim_due(ctx, attrs)
  end

  defp claim_due_router_maybe(_ctx, nil), do: {:ok, []}
  defp claim_due_router_maybe(_ctx, %{limit: limit}) when limit <= 0, do: {:ok, []}
  defp claim_due_router_maybe(ctx, attrs), do: Router.flow_claim_due(ctx, attrs)

  defp claim_normal_attrs(_attrs, nil, _limit), do: nil

  defp claim_normal_attrs(attrs, {:any_except_running, state}, limit) do
    attrs
    |> Map.put(:state, state)
    |> Map.put(:limit, limit)
    |> Map.put(:exclude_states, ["running"])
  end

  defp claim_normal_attrs(attrs, state, limit) do
    attrs
    |> Map.put(:state, state)
    |> Map.put(:limit, limit)
  end

  defp claim_normal_state_filter("running"), do: nil

  defp claim_normal_state_filter(:any), do: {:any_except_running, :any}

  defp claim_normal_state_filter(states) when is_list(states) do
    case Enum.reject(states, &(&1 == "running")) do
      [] -> nil
      [state] -> state
      filtered -> filtered
    end
  end

  defp claim_normal_state_filter(state), do: state

  def extend_lease(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- extend_lease_attrs(id, lease_token, opts) do
        Router.flow_extend_lease(ctx, attrs)
      end

    FlowTelemetry.observe(:extend_lease, started, result, %{flow_id: id, _count: 1})
  end

  def complete(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- complete_attrs(id, lease_token, opts) do
        Router.flow_complete(ctx, attrs)
      end

    FlowTelemetry.observe(:complete, started, result, %{flow_id: id, _count: 1})
  end

  def complete_many(ctx, partition_key, items, opts)
      when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_opts(opts),
           {:ok, independent?} <- optional_boolean(opts, :independent, false),
           :ok <- validate_complete_many_items(items),
           {:ok, attrs_list} <- complete_many_attrs(items, opts, partition_key),
           :ok <- validate_unique_transition_ids(attrs_list, independent?) do
        if independent? do
          flow_terminal_many_independent(ctx, :complete, attrs_list)
        else
          Router.flow_complete_many(ctx, partition_key, attrs_list)
        end
      end

    FlowTelemetry.observe(:complete, started, result, %{flow_id: nil, _count: length(items)})
  end

  def complete_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def transition(ctx, id, from_state, to_state, opts \\ [])
      when is_binary(id) and is_binary(from_state) and is_binary(to_state) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- transition_attrs(id, from_state, to_state, opts) do
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
           :ok <- validate_opts(opts),
           {:ok, independent?} <- optional_boolean(opts, :independent, false),
           :ok <- validate_transition_many_items(items),
           {:ok, attrs_list} <-
             transition_many_attrs(items, opts, partition_key, from_state, to_state),
           :ok <- validate_unique_transition_ids(attrs_list, independent?) do
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
          case transition_attrs(id, from_state, to_state, opts) do
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
    started = flow_start_time()

    results =
      ops
      |> Enum.map(fn op ->
        case pipeline_write_command(op) do
          {:ok, kind, command} -> {:ok, kind, command}
          {:error, _reason} = error -> error
        end
      end)
      |> pipeline_write_ordered_results(ctx, [])

    FlowTelemetry.observe_batch(:pipeline_write, started, results)
    results
  end

  def pipeline_write_batch_independent(_ctx, _ops),
    do: [{:error, "ERR flow opts must be a keyword list"}]

  defp pipeline_write_ordered_results([], _ctx, results_rev), do: Enum.reverse(results_rev)

  defp pipeline_write_ordered_results([{:error, _reason} = error | rest], ctx, results_rev) do
    pipeline_write_ordered_results(rest, ctx, [error | results_rev])
  end

  defp pipeline_write_ordered_results([{:ok, kind, command} | rest], ctx, results_rev)
       when kind in [:state, :terminal] do
    {run, rest} = take_pipeline_write_run(rest, kind, [command])

    results_rev =
      kind
      |> pipeline_write_run_results(Enum.reverse(run), ctx)
      |> Enum.reduce(results_rev, fn result, acc -> [result | acc] end)

    pipeline_write_ordered_results(rest, ctx, results_rev)
  end

  defp take_pipeline_write_run([{:ok, next_kind, command} | rest], kind, acc)
       when next_kind == kind and kind in [:state, :terminal] do
    take_pipeline_write_run(rest, kind, [command | acc])
  end

  defp take_pipeline_write_run(rest, _kind, acc), do: {acc, rest}

  defp pipeline_write_run_results(:state, run, ctx) do
    pipeline_write_state_run_results(ctx, run)
  end

  defp pipeline_write_run_results(:terminal, run, ctx) do
    Router.flow_terminal_command_batch(ctx, run)
  end

  defp pipeline_write_state_run_results(ctx, keyed_commands) do
    case pipeline_create_attrs(keyed_commands, [], MapSet.new()) do
      {:ok, attrs_list} ->
        ctx
        |> Router.flow_create_pipeline_batch(attrs_list)
        |> maybe_notify_claim_waiters(attrs_list, :state)

      :generic ->
        pipeline_write_transition_run_results(ctx, keyed_commands)
    end
  end

  defp pipeline_write_transition_run_results(ctx, keyed_commands) do
    case pipeline_transition_attrs(keyed_commands, []) do
      {:ok, attrs_list} ->
        ctx
        |> Router.flow_transition_batch(attrs_list)
        |> maybe_notify_claim_waiters(attrs_list, :to_state)

      :generic ->
        Router.flow_command_batch(ctx, keyed_commands)
    end
  end

  defp pipeline_create_attrs([], acc, _seen), do: {:ok, Enum.reverse(acc)}

  defp pipeline_create_attrs(
         [{key, {:flow_create, _state_key, attrs}} | rest],
         acc,
         seen
       )
       when is_map(attrs) do
    if MapSet.member?(seen, key) do
      :generic
    else
      pipeline_create_attrs(rest, [attrs | acc], MapSet.put(seen, key))
    end
  end

  defp pipeline_create_attrs(_keyed_commands, _acc, _seen), do: :generic

  defp pipeline_transition_attrs([], acc), do: {:ok, Enum.reverse(acc)}

  defp pipeline_transition_attrs(
         [{_key, {:flow_transition, _state_key, attrs}} | rest],
         acc
       )
       when is_map(attrs) do
    pipeline_transition_attrs(rest, [attrs | acc])
  end

  defp pipeline_transition_attrs(_keyed_commands, _acc), do: :generic

  @doc false
  def pipeline_claim_due_batch(_ctx, []), do: []

  def pipeline_claim_due_batch(ctx, ops) when is_list(ops) do
    started = flow_start_time()

    {results, stats} =
      ops
      |> Enum.map(&pipeline_claim_due_command/1)
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
    started = flow_start_time()

    {get_ops, history_ops, other_ops, indexed_results} =
      ops
      |> Enum.with_index()
      |> Enum.reduce({[], [], [], %{}}, fn {op, idx},
                                           {get_acc, history_acc, other_acc, result_acc} ->
        case pipeline_read_command(ctx, op) do
          {:get, id, partition_key, payload_return} ->
            {[{idx, id, partition_key, payload_return} | get_acc], history_acc, other_acc,
             result_acc}

          {:history, id, partition_key, history_key, query, include_cold?, consistent?,
           value_return} ->
            {get_acc,
             [
               {idx, id, partition_key, history_key, query, include_cold?, consistent?,
                value_return}
               | history_acc
             ], other_acc, result_acc}

          {:other, fun} ->
            {get_acc, history_acc, [{idx, fun} | other_acc], result_acc}

          {:error, _reason} = error ->
            {get_acc, history_acc, other_acc, Map.put(result_acc, idx, error)}
        end
      end)

    indexed_results =
      get_ops
      |> Enum.reverse()
      |> pipeline_read_get_results(ctx)
      |> Map.merge(indexed_results)

    indexed_results =
      history_ops
      |> Enum.reverse()
      |> pipeline_read_history_results(ctx)
      |> Map.merge(indexed_results)

    indexed_results =
      other_ops
      |> Enum.reverse()
      |> Enum.reduce(indexed_results, fn {idx, fun}, acc ->
        Map.put(acc, idx, fun.())
      end)

    results = for idx <- 0..(length(ops) - 1), do: Map.fetch!(indexed_results, idx)

    FlowTelemetry.observe_pipeline_read_batch(started, ops)
    results
  end

  def pipeline_read_batch(_ctx, _ops),
    do: [{:error, "ERR flow opts must be a keyword list"}]

  def retry(ctx, id, lease_token, opts)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- retry_attrs(id, lease_token, opts) do
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
           :ok <- validate_opts(opts),
           {:ok, independent?} <- optional_boolean(opts, :independent, false),
           :ok <- validate_retry_many_items(items),
           {:ok, attrs_list} <- retry_many_attrs(items, opts, partition_key),
           :ok <- validate_unique_transition_ids(attrs_list, independent?) do
        if independent? do
          ctx
          |> flow_terminal_many_independent(:retry, attrs_list)
          |> maybe_notify_retry_claim_waiters(ctx, attrs_list)
        else
          ctx
          |> Router.flow_retry_many(partition_key, attrs_list)
          |> maybe_notify_retry_claim_waiters(ctx, attrs_list)
        end
      end

    FlowTelemetry.observe(:retry, started, result, %{flow_id: nil, _count: length(items)})
  end

  def retry_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def fail(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- fail_attrs(id, lease_token, opts) do
        Router.flow_fail(ctx, attrs)
      end

    FlowTelemetry.observe(:fail, started, result, %{flow_id: id, _count: 1})
  end

  def fail_many(ctx, partition_key, items, opts) when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_opts(opts),
           {:ok, independent?} <- optional_boolean(opts, :independent, false),
           :ok <- validate_fail_many_items(items),
           {:ok, attrs_list} <- fail_many_attrs(items, opts, partition_key),
           :ok <- validate_unique_transition_ids(attrs_list, independent?) do
        if independent? do
          flow_terminal_many_independent(ctx, :fail, attrs_list)
        else
          Router.flow_fail_many(ctx, partition_key, attrs_list)
        end
      end

    FlowTelemetry.observe(:fail, started, result, %{flow_id: nil, _count: length(items)})
  end

  def fail_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def cancel(ctx, id, opts \\ []) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- cancel_attrs(id, opts) do
        Router.flow_cancel(ctx, attrs)
      end

    FlowTelemetry.observe(:cancel, started, result, %{flow_id: id, _count: 1})
  end

  def cancel_many(ctx, partition_key, items, opts) when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_opts(opts),
           {:ok, independent?} <- optional_boolean(opts, :independent, false),
           :ok <- validate_cancel_many_items(items),
           {:ok, attrs_list} <- cancel_many_attrs(items, opts, partition_key),
           :ok <- validate_unique_transition_ids(attrs_list, independent?) do
        if independent? do
          flow_terminal_many_independent(ctx, :cancel, attrs_list)
        else
          Router.flow_cancel_many(ctx, partition_key, attrs_list)
        end
      end

    FlowTelemetry.observe(:cancel, started, result, %{flow_id: nil, _count: length(items)})
  end

  def cancel_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def retention_cleanup(ctx, opts \\ [])

  def retention_cleanup(ctx, opts) when is_list(opts) do
    started = flow_start_time()

    result =
      with :ok <- validate_opts(opts),
           {:ok, limit} <- optional_pos_integer(opts, :limit, 100),
           {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
           :ok <- flush_lmdb_before_retention_cleanup(ctx),
           :ok <- flush_history_before_retention_cleanup(ctx),
           :ok <- flush_lmdb_before_retention_cleanup(ctx) do
        Router.flow_retention_cleanup(ctx, %{limit: limit, now_ms: now})
      end

    FlowTelemetry.observe(:retention_cleanup, started, result, %{flow_id: nil})
  end

  def retention_cleanup(_ctx, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  defp flush_lmdb_before_retention_cleanup(%{name: name, shard_count: shard_count})
       when is_atom(name) and is_integer(shard_count) and shard_count >= 0 do
    case Ferricstore.Flow.LMDBWriter.flush_all(name, shard_count) do
      :ok -> :ok
      {:error, :writer_not_started} -> :ok
      {:error, {:noproc, _}} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp flush_lmdb_before_retention_cleanup(_ctx), do: :ok

  defp flush_history_before_retention_cleanup(%{shard_count: shard_count} = ctx)
       when is_integer(shard_count) and shard_count >= 0 do
    Enum.reduce_while(0..max(shard_count - 1, -1)//1, :ok, fn shard_index, :ok ->
      case Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index, 120_000) do
        :ok -> {:cont, :ok}
        {:error, :not_started} -> {:cont, :ok}
        {:error, {:noproc, _}} -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flush_history_before_retention_cleanup(_ctx), do: :ok

  def rewind(ctx, id, opts) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- rewind_attrs(id, opts) do
        ctx
        |> Router.flow_rewind(attrs)
        |> maybe_notify_claim_waiters(attrs, :any)
      end

    FlowTelemetry.observe(:rewind, started, result, %{flow_id: id, _count: 1})
  end

  def list(ctx, type, opts \\ [])

  def list(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, state} <- flow_state(opts),
         {:ok, partition_key} <- optional_auto_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, records} <-
           flow_list_records(
             ctx,
             type,
             state,
             partition_key,
             count,
             include_cold? or consistent_projection?,
             consistent_projection?
           ) do
      {:ok, records}
    end
  end

  def list(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def list(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def terminals(ctx, type, opts \\ [])

  def terminals(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, state} <- flow_terminal_state(opts),
         {:ok, partition_key} <- optional_auto_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, rev?} <- optional_boolean(opts, :rev, false),
         {:ok, from_ms} <- optional_non_neg_integer(opts, :from_ms, nil),
         {:ok, to_ms} <- optional_non_neg_integer(opts, :to_ms, nil),
         :ok <- validate_ms_range(from_ms, to_ms),
         fetch_count = flow_time_filter_fetch_count(count, from_ms, to_ms),
         query = %{from_ms: from_ms, to_ms: to_ms, rev?: rev?},
         {:ok, records} <-
           flow_terminal_records(
             ctx,
             type,
             state,
             partition_key,
             fetch_count,
             include_cold? or consistent_projection?,
             consistent_projection?,
             query
           ) do
      {:ok,
       records
       |> filter_flow_records_by_ms(from_ms, to_ms)
       |> sort_flow_records_by_update()
       |> maybe_reverse_flow_records(rev?)
       |> Enum.take(count)}
    end
  end

  def terminals(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def terminals(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def failures(ctx, type, opts \\ [])

  def failures(ctx, type, opts) when is_binary(type) and is_list(opts) do
    terminals(ctx, type, Keyword.put(opts, :state, "failed"))
  end

  def failures(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def failures(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  defp flow_terminal_many_independent(ctx, op, attrs_list)
       when op in [:complete, :retry, :fail, :cancel] do
    commands = Enum.map(attrs_list, &{op, &1})
    {:ok, Router.flow_terminal_command_batch_independent(ctx, commands)}
  end

  def by_parent(ctx, parent_flow_id, opts \\ [])

  def by_parent(ctx, parent_flow_id, opts)
      when is_binary(parent_flow_id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(parent_flow_id),
         {:ok, partition_key} <- optional_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, query} <- flow_index_query_opts(opts, count),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         index_key = __MODULE__.Keys.parent_index_key(parent_flow_id, partition_key),
         :ok <- validate_key_size(index_key),
         {:ok, records} <-
           flow_records_for_index(
             ctx,
             index_key,
             partition_key,
             query,
             include_cold? or consistent_projection?,
             consistent_projection?
           ) do
      {:ok, filter_flow_index_records(records, :parent_flow_id, parent_flow_id, query)}
    end
  end

  def by_parent(_ctx, parent_flow_id, _opts) when not is_binary(parent_flow_id),
    do: {:error, "ERR flow parent_flow_id must be a non-empty string"}

  def by_parent(_ctx, _parent_flow_id, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def by_root(ctx, root_flow_id, opts \\ [])

  def by_root(ctx, root_flow_id, opts) when is_binary(root_flow_id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(root_flow_id),
         {:ok, partition_key} <- optional_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, query} <- flow_index_query_opts(opts, count),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         index_key = __MODULE__.Keys.root_index_key(root_flow_id, partition_key),
         :ok <- validate_key_size(index_key),
         {:ok, indexed_records} <-
           flow_records_for_index(
             ctx,
             index_key,
             partition_key,
             query,
             include_cold? or consistent_projection?,
             consistent_projection?
           ),
         {:ok, root_record} <- flow_root_record(ctx, root_flow_id, partition_key) do
      records =
        [root_record | indexed_records]
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq_by(&Map.get(&1, :id))

      {:ok, filter_flow_index_records(records, :root_flow_id, root_flow_id, query)}
    end
  end

  def by_root(_ctx, root_flow_id, _opts) when not is_binary(root_flow_id),
    do: {:error, "ERR flow root_flow_id must be a non-empty string"}

  def by_root(_ctx, _root_flow_id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def by_correlation(ctx, correlation_id, opts \\ [])

  def by_correlation(ctx, correlation_id, opts)
      when is_binary(correlation_id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(correlation_id),
         {:ok, partition_key} <- optional_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, query} <- flow_index_query_opts(opts, count),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         index_key = __MODULE__.Keys.correlation_index_key(correlation_id, partition_key),
         :ok <- validate_key_size(index_key),
         {:ok, records} <-
           flow_records_for_index(
             ctx,
             index_key,
             partition_key,
             query,
             include_cold? or consistent_projection?,
             consistent_projection?
           ) do
      {:ok, filter_flow_index_records(records, :correlation_id, correlation_id, query)}
    end
  end

  def by_correlation(_ctx, correlation_id, _opts) when not is_binary(correlation_id),
    do: {:error, "ERR flow correlation_id must be a non-empty string"}

  def by_correlation(_ctx, _correlation_id, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def info(ctx, type, opts \\ [])

  def info(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, partition_key} <- optional_auto_partition_key(opts),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, counts, inflight} <-
           flow_info_counts(
             ctx,
             type,
             partition_key,
             include_cold? or consistent_projection?,
             consistent_projection?
           ) do
      {:ok,
       counts
       |> Map.put(:type, type)
       |> Map.put(:partition_key, flow_response_partition_key(partition_key))
       |> Map.put(:inflight, inflight)}
    end
  end

  def info(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def info(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def stuck(ctx, type, opts \\ [])

  def stuck(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, partition_key} <- optional_auto_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, older_than_ms} <- optional_non_neg_integer(opts, :older_than_ms, 0),
         {:ok, now_ms} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
         cutoff = now_ms - older_than_ms,
         {:ok, records} <- flow_stuck_records(ctx, type, partition_key, cutoff, count) do
      {:ok, records}
    end
  end

  def stuck(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def stuck(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def history(ctx, id, opts \\ [])

  def history(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, partition_key} <- optional_partition_key(opts),
         history_key = __MODULE__.Keys.history_key(id, partition_key),
         :ok <- validate_key_size(history_key),
         {:ok, count} <- flow_count(opts),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, true),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, true),
         {:ok, value_return} <- history_value_return_opts(opts),
         {:ok, query} <- flow_history_query_opts(opts, count) do
      flow_history_read(
        ctx,
        id,
        partition_key,
        history_key,
        query,
        include_cold?,
        consistent_projection?,
        value_return
      )
    end
  end

  def history(_ctx, id, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def history(_ctx, _id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  defp flow_history_read(
         ctx,
         id,
         partition_key,
         history_key,
         query,
         false,
         consistent?,
         value_return
       ) do
    with :ok <- flow_maybe_flush_history_projector(ctx, history_key, consistent?) do
      if flow_history_state_exists?(ctx, id, partition_key) do
        fetch_count = flow_history_query_fetch_count(query)

        case flow_history_hot_refs(ctx, id, partition_key, history_key, fetch_count) do
          {:ok, []} ->
            flow_history_hot_fallback_scan(ctx, history_key, query, value_return)

          {:ok, event_refs} ->
            event_ids = Enum.map(event_refs, fn {event_id, _score} -> event_id end)

            with {:ok, events} <-
                   flow_history_from_event_ids(
                     ctx,
                     id,
                     partition_key,
                     history_key,
                     event_ids,
                     value_return
                   ) do
              {:ok, flow_history_apply_query(events, query)}
            end
        end
      else
        {:ok, []}
      end
    end
  end

  defp flow_history_read(
         ctx,
         id,
         partition_key,
         history_key,
         query,
         true,
         consistent?,
         value_return
       ) do
    with :ok <- flow_maybe_flush_history_projector(ctx, history_key, consistent?) do
      if flow_history_state_exists?(ctx, id, partition_key) do
        fetch_count = flow_history_query_fetch_count(query)

        with {:ok, hot_refs} <-
               flow_history_hot_refs(ctx, id, partition_key, history_key, fetch_count),
             {:ok, cold_refs} <-
               flow_history_lmdb_refs(
                 ctx,
                 history_key,
                 fetch_count,
                 consistent?,
                 query
               ) do
          event_ids =
            (hot_refs ++ cold_refs)
            |> flow_history_candidate_event_ids(query)

          case event_ids do
            [] ->
              flow_history_hot_fallback_scan(ctx, history_key, query, value_return)

            _ ->
              with {:ok, events} <-
                     flow_history_from_event_ids(
                       ctx,
                       id,
                       partition_key,
                       history_key,
                       event_ids,
                       value_return
                     ) do
                {:ok, flow_history_apply_query(events, query)}
              end
          end
        end
      else
        {:ok, []}
      end
    end
  end

  defp flow_history_state_exists?(ctx, id, partition_key) do
    case Router.flow_get(ctx, id, partition_key) do
      value when is_binary(value) -> true
      _ -> false
    end
  end

  defp flow_history_candidate_event_ids(refs, query) do
    refs
    |> Enum.sort_by(fn {event_id, score} -> {score, event_id} end)
    |> Enum.uniq_by(fn {event_id, _score} -> event_id end)
    |> flow_history_limit_candidate_refs(query)
    |> Enum.map(fn {event_id, _score} -> event_id end)
  end

  defp flow_history_limit_candidate_refs(refs, query) do
    if flow_history_query_filtering?(query), do: refs, else: Enum.take(refs, -query.count)
  end

  defp flow_history_hot_refs(ctx, id, partition_key, history_key, count) do
    {start_idx, stop_idx} = flow_history_hot_range(ctx, id, partition_key, history_key, count)

    case Router.flow_index_rank_range(ctx, history_key, start_idx, stop_idx, false) do
      {:ok, event_refs} -> {:ok, event_refs}
      :unavailable -> {:ok, []}
    end
  end

  defp flow_history_hot_range(ctx, id, partition_key, history_key, count) do
    with {:ok, max} <- flow_history_hot_max(ctx, id, partition_key),
         true <- is_integer(max) and max > 0,
         {:ok, total} <- flow_zcard(ctx, history_key) do
      oldest_hot_idx = max(total - max, 0)
      start_idx = max(total - count, oldest_hot_idx)
      {start_idx, total - 1}
    else
      _ -> {0, count - 1}
    end
  end

  defp flow_history_hot_max(ctx, id, partition_key) do
    case Router.flow_get(ctx, id, partition_key) do
      value when is_binary(value) ->
        case safe_decode_record(value) do
          {:ok, %{history_hot_max_events: max}} -> {:ok, max}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp flow_history_lmdb_refs(_ctx, _history_key, count, _consistent?, _query) when count <= 0,
    do: {:ok, []}

  defp flow_history_lmdb_refs(ctx, history_key, count, consistent?, query) do
    shard_index = Router.shard_for(ctx, history_key)

    with :ok <- flow_maybe_flush_lmdb_shard(ctx, shard_index, consistent?),
         :ok <- flow_require_lmdb_mirror_healthy_shard(ctx, history_key, shard_index) do
      path =
        ctx.data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> Ferricstore.Flow.LMDB.path()

      prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)
      now_ms = now_ms()
      sweep_limit = flow_history_lmdb_sweep_limit()
      scan_count = flow_history_lmdb_scan_count(count, query)

      with {:ok, _swept} <-
             Ferricstore.Flow.LMDB.sweep_expired_history(path, now_ms, sweep_limit),
           {:ok, entries} <-
             flow_history_lmdb_prefix_entries(path, prefix, scan_count, query) do
        {:ok, flow_decode_history_index_entries(entries, path, now_ms)}
      end
    end
  end

  defp flow_history_lmdb_scan_count(count, query) do
    cond do
      flow_history_requires_full_lmdb_scan?(query) ->
        flow_max_history_max_events()

      flow_history_query_filtering?(query) ->
        flow_history_lmdb_query_scan_count(count, Map.get(query, :rev?, false))

      true ->
        count
    end
  end

  defp flow_history_lmdb_prefix_entries(path, prefix, limit, query) do
    result =
      query
      |> flow_history_lmdb_scan_directions()
      |> Enum.reduce_while({:ok, []}, fn reverse?, {:ok, acc} ->
        case flow_history_lmdb_prefix_entries(path, prefix, limit, query, reverse?) do
          {:ok, entries} -> {:cont, {:ok, acc ++ entries}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, entries} -> {:ok, Enum.uniq_by(entries, fn {key, _value} -> key end)}
      {:error, _reason} = error -> error
    end
  end

  defp flow_history_lmdb_scan_directions(query) do
    cond do
      not flow_history_query_filtering?(query) -> [true]
      flow_history_requires_full_lmdb_scan?(query) -> [false]
      true -> [true, false]
    end
  end

  defp flow_history_requires_full_lmdb_scan?(%{
         from_event: nil,
         to_event: nil,
         from_version: nil,
         to_version: nil,
         event: nil,
         worker: nil
       }),
       do: false

  defp flow_history_requires_full_lmdb_scan?(_query), do: true

  defp flow_history_lmdb_prefix_entries(path, prefix, limit, %{to_ms: to_ms}, true)
       when is_integer(to_ms) and to_ms >= 0 do
    Ferricstore.Flow.LMDB.prefix_entries_reverse_before(
      path,
      prefix,
      flow_lmdb_time_upper_seek_key(prefix, to_ms),
      limit
    )
  end

  defp flow_history_lmdb_prefix_entries(path, prefix, limit, _query, true) do
    Ferricstore.Flow.LMDB.prefix_entries(path, prefix, limit, true)
  end

  defp flow_history_lmdb_prefix_entries(path, prefix, limit, %{from_ms: from_ms}, false)
       when is_integer(from_ms) and from_ms >= 0 do
    Ferricstore.Flow.LMDB.prefix_entries_after(
      path,
      prefix,
      flow_lmdb_time_seek_key(prefix, from_ms),
      limit
    )
  end

  defp flow_history_lmdb_prefix_entries(path, prefix, limit, _query, false) do
    Ferricstore.Flow.LMDB.prefix_entries(path, prefix, limit, false)
  end

  defp flow_maybe_flush_lmdb_shard(_ctx, _shard_index, false), do: :ok

  defp flow_maybe_flush_lmdb_shard(ctx, shard_index, true),
    do: Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)

  defp flow_maybe_flush_history_projector(_ctx, _history_key, false), do: :ok

  defp flow_maybe_flush_history_projector(ctx, history_key, true) do
    shard_index = Router.shard_for(ctx, history_key)

    case Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index, 120_000) do
      :ok -> :ok
      {:error, reason} -> {:error, "ERR flow history projection unavailable: #{inspect(reason)}"}
    end
  end

  defp flow_require_lmdb_mirror_healthy_shard(ctx, index_key, shard_index) do
    if flow_lmdb_mirror_degraded_shard?(ctx, shard_index) do
      {:error, "ERR flow LMDB projection unavailable for #{index_key}"}
    else
      :ok
    end
  end

  defp flow_history_from_event_ids(ctx, id, partition_key, history_key, event_ids, value_return) do
    compound_keys =
      Enum.map(event_ids, &__MODULE__.Keys.stream_entry_key(id, &1, partition_key))

    values = Router.compound_batch_get(ctx, history_key, compound_keys)
    hot_values = flow_history_hot_values_by_event(event_ids, values)
    cold_values = flow_history_cold_values_by_event(ctx, history_key, event_ids, hot_values)
    decode_context = flow_history_decode_context(ctx, id, partition_key)

    entries =
      Enum.flat_map(event_ids, fn event_id ->
        value = Map.get(hot_values, event_id) || Map.get(cold_values, event_id)

        if is_binary(value) do
          [{event_id, Codec.decode_history_fields(value, decode_context)}]
        else
          []
        end
      end)

    {:ok,
     entries
     |> Enum.map(&flow_history_entry_to_tuple/1)
     |> hydrate_history_values(ctx, value_return)}
  end

  defp flow_history_hot_values_by_event(event_ids, values) do
    event_ids
    |> Enum.zip(values)
    |> Enum.reduce(%{}, fn
      {event_id, value}, acc when is_binary(event_id) and is_binary(value) ->
        Map.put(acc, event_id, value)

      _missing, acc ->
        acc
    end)
  end

  defp flow_history_cold_values_by_event(_ctx, _history_key, [], _hot_values), do: %{}

  defp flow_history_cold_values_by_event(ctx, history_key, event_ids, hot_values) do
    missing_ids = Enum.reject(event_ids, &Map.has_key?(hot_values, &1))

    if missing_ids == [] do
      %{}
    else
      shard_index = Router.shard_for(ctx, history_key)
      shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_index)
      lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

      lmdb_keys =
        Enum.map(missing_ids, fn event_id ->
          Ferricstore.Flow.LMDB.history_index_key(
            history_key,
            event_id,
            flow_history_event_ms(event_id)
          )
        end)

      case Ferricstore.Flow.LMDB.get_many(lmdb_path, lmdb_keys) do
        {:ok, lmdb_values} ->
          missing_ids
          |> Enum.zip(lmdb_values)
          |> Enum.reduce(%{}, fn {event_id, lmdb_value}, acc ->
            case flow_history_cold_value_from_lmdb(shard_path, event_id, lmdb_value) do
              {:ok, value} -> Map.put(acc, event_id, value)
              _miss -> acc
            end
          end)

        _error ->
          %{}
      end
    end
  end

  defp flow_history_cold_value_from_lmdb(shard_path, event_id, {:ok, lmdb_value}),
    do: flow_history_cold_value_from_lmdb(shard_path, event_id, lmdb_value)

  defp flow_history_cold_value_from_lmdb(shard_path, event_id, lmdb_value)
       when is_binary(lmdb_value) do
    now = now_ms()

    with {:ok, {^event_id, _event_ms, expire_at_ms, _compound_key, file_ref, offset, _value_size}} <-
           Ferricstore.Flow.LMDB.decode_history_index_location(lmdb_value),
         true <- expire_at_ms <= 0 or expire_at_ms > now do
      case {file_ref, offset} do
        {{:flow_history, _file_id} = ref, offset} when is_integer(offset) and offset >= 0 ->
          Ferricstore.Flow.HistoryProjector.read_value(shard_path, ref, offset)

        _other ->
          :miss
      end
    else
      _ -> :miss
    end
  end

  defp flow_history_cold_value_from_lmdb(_shard_path, _event_id, _lmdb_value), do: :miss

  defp flow_history_decode_context(ctx, id, partition_key) do
    case Router.flow_get(ctx, id, partition_key) do
      value when is_binary(value) ->
        case safe_decode_record(value) do
          {:ok, record} -> record
          _ -> %{id: id}
        end

      _ ->
        %{id: id}
    end
  rescue
    _ -> %{id: id}
  end

  defp flow_history_decode_context_from_history_key(ctx, history_key) do
    case flow_state_key_from_history_key(history_key) do
      {:ok, state_key, id} -> flow_history_decode_context_by_state_key(ctx, state_key, id)
      :error -> %{}
    end
  end

  defp flow_history_decode_context_by_state_key(ctx, state_key, id) do
    case Stats.with_cache_tracking_disabled(fn -> Router.get(ctx, state_key) end) do
      value when is_binary(value) ->
        case safe_decode_record(value) do
          {:ok, record} -> record
          _ -> %{id: id}
        end

      _ ->
        %{id: id}
    end
  rescue
    _ -> %{id: id}
  end

  defp flow_state_key_from_history_key(history_key) when is_binary(history_key) do
    case :binary.match(history_key, "}:h:") do
      {pos, len} ->
        start = pos + len
        id = binary_part(history_key, start, byte_size(history_key) - start)
        tag_prefix = binary_part(history_key, 0, pos + 1)
        {:ok, tag_prefix <> ":s:" <> id, id}

      :nomatch ->
        :error
    end
  end

  defp flow_history_hot_fallback_scan(ctx, history_key, query, value_return) do
    prefix = "X:" <> history_key <> <<0>>
    prefix_size = byte_size(prefix)
    fetch_count = flow_history_query_fetch_count(query)
    decode_context = flow_history_decode_context_from_history_key(ctx, history_key)

    entries =
      ctx
      |> Router.compound_scan(history_key, prefix)
      |> Enum.flat_map(fn
        {<<^prefix::binary-size(prefix_size), event_id::binary>>, value}
        when is_binary(value) ->
          [{event_id, Codec.decode_history_fields(value, decode_context)}]

        {event_id, value} when is_binary(event_id) and is_binary(value) ->
          [{event_id, Codec.decode_history_fields(value, decode_context)}]

        _other ->
          []
      end)
      |> Enum.sort_by(fn {event_id, _fields} -> {flow_history_event_ms(event_id), event_id} end)
      |> Enum.take(-fetch_count)

    events =
      entries
      |> Enum.map(&flow_history_entry_to_tuple/1)
      |> hydrate_history_values(ctx, value_return)

    {:ok, flow_history_apply_query(events, query)}
  end

  defp flow_history_query_opts(opts, count) do
    with {:ok, from_event} <- optional_binary_or_nil(opts, :from_event, nil),
         {:ok, to_event} <- optional_binary_or_nil(opts, :to_event, nil),
         {:ok, from_ms} <- optional_non_neg_integer(opts, :from_ms, nil),
         {:ok, to_ms} <- optional_non_neg_integer(opts, :to_ms, nil),
         {:ok, from_version} <- optional_non_neg_integer(opts, :from_version, nil),
         {:ok, to_version} <- optional_non_neg_integer(opts, :to_version, nil),
         {:ok, rev?} <- optional_boolean(opts, :rev, false),
         {:ok, event} <- optional_binary_or_nil(opts, :event, nil),
         {:ok, worker} <- optional_binary_or_nil(opts, :worker, nil),
         :ok <- validate_ms_range(from_ms, to_ms),
         :ok <- validate_version_range(from_version, to_version),
         :ok <- validate_event_range(from_event, to_event) do
      query = %{
        count: count,
        from_event: from_event,
        to_event: to_event,
        from_ms: from_ms,
        to_ms: to_ms,
        from_version: from_version,
        to_version: to_version,
        rev?: rev?,
        event: event,
        worker: worker
      }

      {:ok, query}
    end
  end

  defp flow_history_query_fetch_count(%{count: count} = query) do
    if flow_history_query_filtering?(query) do
      flow_history_lmdb_query_scan_count(count, Map.get(query, :rev?, false))
    else
      count
    end
  end

  defp flow_history_query_filtering?(%{
         from_event: nil,
         to_event: nil,
         from_ms: nil,
         to_ms: nil,
         from_version: nil,
         to_version: nil,
         event: nil,
         worker: nil
       }),
       do: false

  defp flow_history_query_filtering?(_query), do: true

  defp flow_history_apply_query(events, query) do
    filtered = Enum.filter(events, &flow_history_event_matches?(&1, query))

    cond do
      query.rev? ->
        filtered
        |> Enum.reverse()
        |> Enum.take(query.count)

      flow_history_query_filtering?(query) ->
        Enum.take(filtered, -query.count)

      true ->
        Enum.take(filtered, query.count)
    end
  end

  defp flow_history_event_matches?({event_id, fields}, query) do
    event_ms = flow_history_event_ms(event_id)
    event_key = {event_ms, event_id}
    version = flow_history_field_int(fields, "version")

    flow_event_after?(event_key, query.from_event) and
      flow_event_before?(event_key, query.to_event) and
      flow_ms_after?(event_ms, query.from_ms) and
      flow_ms_before?(event_ms, query.to_ms) and
      flow_version_after?(version, query.from_version) and
      flow_version_before?(version, query.to_version) and
      flow_field_matches?(fields, "event", query.event) and
      flow_field_matches?(fields, "lease_owner", query.worker)
  end

  defp flow_event_after?(_event_key, nil), do: true

  defp flow_event_after?(event_key, from_event),
    do: event_key >= flow_history_event_key(from_event)

  defp flow_event_before?(_event_key, nil), do: true
  defp flow_event_before?(event_key, to_event), do: event_key <= flow_history_event_key(to_event)

  defp flow_history_event_key(event_id), do: {flow_history_event_ms(event_id), event_id}

  defp flow_ms_after?(_event_ms, nil), do: true
  defp flow_ms_after?(event_ms, from_ms), do: event_ms >= from_ms

  defp flow_ms_before?(_event_ms, nil), do: true
  defp flow_ms_before?(event_ms, to_ms), do: event_ms <= to_ms

  defp flow_version_after?(_version, nil), do: true
  defp flow_version_after?(version, from_version), do: version >= from_version

  defp flow_version_before?(_version, nil), do: true
  defp flow_version_before?(version, to_version), do: version <= to_version

  defp flow_history_field_int(fields, key) do
    case Map.get(fields, key) do
      value when is_integer(value) ->
        value

      value when is_binary(value) ->
        case Integer.parse(value) do
          {int, ""} -> int
          _ -> 0
        end

      _ ->
        0
    end
  end

  defp flow_field_matches?(_fields, _key, nil), do: true
  defp flow_field_matches?(fields, key, value), do: Map.get(fields, key) == value

  defp validate_ms_range(nil, _to_ms), do: :ok
  defp validate_ms_range(_from_ms, nil), do: :ok

  defp validate_ms_range(from_ms, to_ms) when from_ms <= to_ms, do: :ok
  defp validate_ms_range(_from_ms, _to_ms), do: {:error, "ERR flow from_ms must be <= to_ms"}

  defp validate_version_range(nil, _to_version), do: :ok
  defp validate_version_range(_from_version, nil), do: :ok

  defp validate_version_range(from_version, to_version) when from_version <= to_version, do: :ok

  defp validate_version_range(_from_version, _to_version),
    do: {:error, "ERR flow from_version must be <= to_version"}

  defp validate_event_range(nil, _to_event), do: :ok
  defp validate_event_range(_from_event, nil), do: :ok

  defp validate_event_range(from_event, to_event) do
    if flow_history_event_key(from_event) <= flow_history_event_key(to_event) do
      :ok
    else
      {:error, "ERR flow from_event must be <= to_event"}
    end
  end

  defp flow_time_filter_fetch_count(count, nil, nil), do: count

  defp flow_time_filter_fetch_count(count, _from_ms, _to_ms),
    do: flow_lmdb_query_scan_count(count)

  defp filter_flow_records_by_ms(records, from_ms, to_ms) do
    Enum.filter(records, fn record ->
      updated_at_ms = Map.get(record, :updated_at_ms, 0)
      flow_ms_after?(updated_at_ms, from_ms) and flow_ms_before?(updated_at_ms, to_ms)
    end)
  end

  defp sort_flow_records_by_update(records) do
    Enum.sort_by(records, fn record ->
      {Map.get(record, :updated_at_ms, 0), Map.get(record, :id, "")}
    end)
  end

  defp maybe_reverse_flow_records(records, true), do: Enum.reverse(records)
  defp maybe_reverse_flow_records(records, false), do: records

  defp prepend_flow_chunk(chunk, chunks), do: [chunk | chunks]
  defp flatten_flow_chunks(chunks), do: Enum.flat_map(chunks, & &1)

  defp flow_terminal_state(opts) do
    case Keyword.get(opts, :state, "any") do
      "any" -> {:ok, "any"}
      state when state in @terminal_states -> {:ok, state}
      _ -> {:error, "ERR flow terminal state must be failed, completed, cancelled, or any"}
    end
  end

  defp flow_terminal_records(
         ctx,
         type,
         state,
         :auto,
         count,
         include_cold?,
         consistent?,
         query
       ) do
    __MODULE__.Keys.auto_partition_keys()
    |> Enum.reduce_while({:ok, []}, fn partition_key, {:ok, acc} ->
      case flow_terminal_records(
             ctx,
             type,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             query
           ) do
        {:ok, records} -> {:cont, {:ok, prepend_flow_chunk(records, acc)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, flatten_flow_chunks(chunks)}
      {:error, _reason} = error -> error
    end
  end

  defp flow_terminal_records(
         ctx,
         type,
         "any",
         partition_key,
         count,
         include_cold?,
         consistent?,
         query
       ) do
    @terminal_states
    |> Enum.reduce_while({:ok, []}, fn state, {:ok, acc} ->
      case flow_terminal_ids(
             ctx,
             type,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             query
           ) do
        {:ok, ids} -> {:cont, {:ok, prepend_flow_chunk(ids, acc)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} ->
        ids =
          chunks
          |> flatten_flow_chunks()
          |> Enum.uniq()
          |> Enum.take(count * length(@terminal_states))

        with {:ok, records} <- flow_records_for_ids(ctx, ids, partition_key) do
          {:ok, Enum.filter(records, &(Map.get(&1, :state) in @terminal_states))}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_terminal_records(
         ctx,
         type,
         state,
         partition_key,
         count,
         include_cold?,
         consistent?,
         query
       ) do
    with {:ok, ids} <-
           flow_terminal_ids(
             ctx,
             type,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             query
           ),
         {:ok, records} <- flow_records_for_ids(ctx, ids, partition_key) do
      {:ok, Enum.filter(records, &(Map.get(&1, :state) == state))}
    end
  end

  defp flow_terminal_ids(
         ctx,
         type,
         state,
         partition_key,
         count,
         include_cold?,
         consistent?,
         query
       ) do
    index_key = __MODULE__.Keys.state_index_key(type, state, partition_key)

    with :ok <- validate_key_size(index_key),
         {:ok, ids} <-
           flow_index_ids(
             ctx,
             index_key,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             query
           ) do
      {:ok, ids}
    end
  end

  defp flow_history_event_ms(event_id) when is_binary(event_id) do
    case Integer.parse(event_id) do
      {ms, "-" <> _rest} -> ms
      {ms, ""} -> ms
      _ -> 0
    end
  end

  defp flow_history_event_ms(_event_id), do: 0

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

  defp flow_count(opts) do
    case Keyword.get(opts, :count, 100) do
      value when is_integer(value) and value > 0 ->
        max_count = flow_max_count()

        if value <= max_count do
          {:ok, value}
        else
          {:error, "ERR flow count exceeds maximum #{max_count}"}
        end

      _ ->
        {:error, "ERR flow count must be a positive integer"}
    end
  end

  defp flow_max_count do
    case Application.get_env(:ferricstore, :flow_max_count, @default_max_count) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_max_count
    end
  end

  defp flow_state(opts) do
    case Keyword.get(opts, :state, @default_state) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow state must be a non-empty string"}
    end
  end

  defp flow_records_for_ids(ctx, ids, partition_key) do
    keys = Enum.map(ids, &__MODULE__.Keys.state_key(&1, partition_key))

    case Enum.find(keys, &(byte_size(&1) > Router.max_key_size())) do
      nil ->
        values = Router.flow_batch_get(ctx, ids, partition_key)

        case Enum.find(values, &match?({:error, _reason}, &1)) do
          nil ->
            records =
              values
              |> Enum.reduce([], fn
                nil, acc -> acc
                value, acc when is_binary(value) -> prepend_decoded_record(value, acc)
              end)
              |> Enum.reverse()

            {:ok, records}

          {:error, _reason} = error ->
            error
        end

      _too_large ->
        {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  defp flow_list_records(ctx, type, state, :auto, count, include_cold?, consistent?) do
    __MODULE__.Keys.auto_partition_keys()
    |> Enum.reduce_while({:ok, []}, fn partition_key, {:ok, acc} ->
      case flow_list_records(ctx, type, state, partition_key, count, include_cold?, consistent?) do
        {:ok, records} -> {:cont, {:ok, prepend_flow_chunk(records, acc)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} ->
        {:ok,
         chunks
         |> flatten_flow_chunks()
         |> sort_flow_records_by_update()
         |> Enum.take(count)}

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_list_records(ctx, type, state, partition_key, count, include_cold?, consistent?) do
    index_key = __MODULE__.Keys.state_index_key(type, state, partition_key)

    with :ok <- validate_key_size(index_key),
         {:ok, ids} <-
           flow_index_ids(ctx, index_key, state, partition_key, count, include_cold?, consistent?) do
      flow_records_for_ids(ctx, ids, partition_key)
    end
  end

  defp safe_decode_record(value) when is_binary(value) do
    {:ok, Codec.decode_record(value)}
  rescue
    _ -> {:ok, nil}
  end

  defp prepend_decoded_record(value, acc) when is_binary(value) do
    case safe_decode_record(value) do
      {:ok, nil} -> acc
      {:ok, record} -> [record | acc]
    end
  end

  defp flow_records_for_index(
         ctx,
         index_key,
         partition_key,
         query,
         include_cold?,
         consistent?
       ) do
    fetch_count = flow_index_query_fetch_count(query)

    with {:ok, ram_entries} <- flow_ram_index_entries(ctx, index_key, query, fetch_count) do
      if include_cold? do
        with {:ok, lmdb_entries} <-
               flow_lmdb_query_index_entries(
                 ctx,
                 index_key,
                 partition_key,
                 fetch_count,
                 consistent?,
                 query
               ) do
          ids =
            (Enum.map(ram_entries, fn {id, score} -> {id, score} end) ++
               Enum.map(lmdb_entries, fn {id, updated_at_ms, _state_key} ->
                 {id, updated_at_ms}
               end))
            |> Enum.sort_by(fn {id, score} -> {score, id} end)
            |> maybe_reverse_flow_index_entries(query.rev?)
            |> Enum.uniq_by(fn {id, _score} -> id end)
            |> Enum.map(fn {id, _score} -> id end)
            |> Enum.take(fetch_count)

          flow_records_for_ids(ctx, ids, partition_key)
        end
      else
        ids = Enum.map(ram_entries, fn {id, _score} -> id end)
        flow_records_for_ids(ctx, ids, partition_key)
      end
    end
  end

  defp flow_index_query_opts(opts, count) do
    with {:ok, from_ms} <- optional_non_neg_integer(opts, :from_ms, nil),
         {:ok, to_ms} <- optional_non_neg_integer(opts, :to_ms, nil),
         {:ok, rev?} <- optional_boolean(opts, :rev, false),
         {:ok, state} <- optional_binary_or_nil(opts, :state, nil),
         {:ok, terminal_only?} <- optional_boolean(opts, :terminal_only, false),
         :ok <- validate_ms_range(from_ms, to_ms) do
      {:ok,
       %{
         count: count,
         from_ms: from_ms,
         to_ms: to_ms,
         rev?: rev?,
         state: state,
         terminal_only?: terminal_only?
       }}
    end
  end

  defp flow_index_query_fetch_count(%{count: count} = query) do
    if flow_index_query_filtering?(query) do
      flow_lmdb_query_scan_count(count)
    else
      count
    end
  end

  defp flow_index_query_filtering?(%{
         from_ms: nil,
         to_ms: nil,
         rev?: false,
         state: nil,
         terminal_only?: false
       }),
       do: false

  defp flow_index_query_filtering?(_query), do: true

  defp filter_flow_index_records(records, field, value, query) do
    records
    |> Enum.filter(&(Map.get(&1, field) == value))
    |> Enum.filter(&flow_index_record_matches?(&1, query))
    |> sort_flow_records_by_update()
    |> maybe_reverse_flow_records(query.rev?)
    |> Enum.take(query.count)
  end

  defp flow_index_record_matches?(record, query) do
    updated_at_ms = Map.get(record, :updated_at_ms, 0)
    state = Map.get(record, :state)

    flow_ms_after?(updated_at_ms, query.from_ms) and
      flow_ms_before?(updated_at_ms, query.to_ms) and
      flow_index_state_matches?(state, query.state) and
      flow_index_terminal_matches?(state, query.terminal_only?)
  end

  defp flow_index_state_matches?(_state, nil), do: true
  defp flow_index_state_matches?(state, expected), do: state == expected

  defp flow_index_terminal_matches?(_state, false), do: true
  defp flow_index_terminal_matches?(state, true), do: state in @terminal_states

  defp maybe_reverse_flow_index_entries(entries, true), do: Enum.reverse(entries)
  defp maybe_reverse_flow_index_entries(entries, false), do: entries

  defp flow_root_record(ctx, root_flow_id, partition_key) do
    case get(ctx, root_flow_id, partition_key: partition_key) do
      {:ok, %{root_flow_id: ^root_flow_id} = record} -> {:ok, record}
      {:ok, nil} -> {:ok, nil}
      {:ok, _record} -> {:ok, nil}
      {:error, _reason} = error -> error
    end
  end

  defp flow_info_counts(ctx, type, :auto, include_cold?, consistent?) do
    zero_counts =
      [@default_state, "running" | @terminal_states]
      |> Map.new(&{String.to_atom(&1), 0})

    __MODULE__.Keys.auto_partition_keys()
    |> Enum.reduce_while({:ok, zero_counts, 0}, fn partition_key,
                                                   {:ok, counts_acc, inflight_acc} ->
      case flow_info_counts(ctx, type, partition_key, include_cold?, consistent?) do
        {:ok, counts, inflight} ->
          merged =
            Map.merge(counts_acc, counts, fn _state, left, right -> left + right end)

          {:cont, {:ok, merged, inflight_acc + inflight}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp flow_info_counts(ctx, type, partition_key, include_cold?, consistent?) do
    state_keys =
      Enum.map([@default_state, "running" | @terminal_states], fn state ->
        {state, __MODULE__.Keys.state_index_key(type, state, partition_key)}
      end)

    inflight_key = {"inflight", __MODULE__.Keys.inflight_index_key(type, partition_key)}
    all_keys = state_keys ++ [inflight_key]

    with :ok <- flow_validate_index_keys(all_keys),
         {:ok, ram_counts} <-
           flow_zset_count_many(ctx, Enum.map(all_keys, fn {_state, key} -> key end)),
         {:ok, lmdb_counts} <-
           flow_terminal_lmdb_counts(ctx, state_keys, partition_key, include_cold?, consistent?) do
      {state_ram_counts, [inflight]} = Enum.split(ram_counts, length(state_keys))

      state_keys
      |> Enum.zip(state_ram_counts)
      |> Enum.reduce_while({:ok, %{}}, fn {{state, key}, ram_count}, {:ok, acc} ->
        with {:ok, count} <-
               flow_maybe_recount_overlapping_terminal(
                 ctx,
                 key,
                 state,
                 partition_key,
                 ram_count,
                 Map.get(lmdb_counts, key, 0)
               ) do
          {:cont, {:ok, Map.put(acc, String.to_atom(state), count)}}
        else
          {:error, _} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, counts} -> {:ok, counts, inflight}
        {:error, _reason} = error -> error
      end
    end
  end

  defp flow_response_partition_key(:auto), do: nil
  defp flow_response_partition_key(partition_key), do: partition_key

  defp flow_stuck_records(ctx, type, :auto, cutoff, count) do
    __MODULE__.Keys.auto_partition_keys()
    |> Enum.reduce_while({:ok, []}, fn partition_key, {:ok, acc} ->
      case flow_stuck_records(ctx, type, partition_key, cutoff, count) do
        {:ok, records} -> {:cont, {:ok, prepend_flow_chunk(records, acc)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} ->
        {:ok,
         chunks
         |> flatten_flow_chunks()
         |> sort_flow_records_by_update()
         |> Enum.take(count)}

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_stuck_records(ctx, type, partition_key, cutoff, count) do
    index_key = __MODULE__.Keys.inflight_index_key(type, partition_key)

    with :ok <- validate_key_size(index_key),
         {:ok, ids} <- flow_zrangebyscore(ctx, index_key, "-inf", Integer.to_string(cutoff)) do
      flow_records_for_ids(ctx, Enum.take(ids, count), partition_key)
    end
  end

  defp flow_terminal_lmdb_counts(_ctx, _state_keys, _partition_key, false, _consistent?),
    do: {:ok, %{}}

  defp flow_terminal_lmdb_counts(ctx, state_keys, partition_key, true, consistent?) do
    terminal_keys =
      state_keys
      |> Enum.filter(fn {state, _key} -> state in @terminal_states end)
      |> Enum.map(fn {_state, key} -> key end)

    case terminal_keys do
      [] ->
        {:ok, %{}}

      [first_key | _] ->
        with :ok <- flow_maybe_flush_lmdb_for_index(ctx, first_key, partition_key, consistent?),
             :ok <- flow_require_lmdb_mirror_healthy(ctx, first_key, partition_key) do
          now_ms = now_ms()
          sweep_limit = flow_terminal_lmdb_sweep_limit()

          ctx
          |> flow_lmdb_paths_for_index(first_key, partition_key)
          |> Enum.reduce_while({:ok, Map.new(terminal_keys, &{&1, 0})}, fn path, {:ok, acc} ->
            with {:ok, counts} <- Ferricstore.Flow.LMDB.terminal_counts(path, terminal_keys),
                 {:ok, counts} <-
                   flow_maybe_sweep_terminal_lmdb_counts(
                     path,
                     terminal_keys,
                     counts,
                     now_ms,
                     sweep_limit
                   ) do
              merged =
                terminal_keys
                |> Enum.zip(counts)
                |> Enum.reduce(acc, fn {key, count}, count_acc ->
                  Map.update!(count_acc, key, &(&1 + count))
                end)

              {:cont, {:ok, merged}}
            else
              {:error, _reason} = error -> {:halt, error}
            end
          end)
        end
    end
  end

  defp flow_maybe_sweep_terminal_lmdb_counts(path, terminal_keys, counts, now_ms, sweep_limit) do
    if Enum.any?(counts, &(&1 > 0)) do
      with {:ok, _swept} <-
             Ferricstore.Flow.LMDB.sweep_expired_terminal(path, now_ms, sweep_limit) do
        Ferricstore.Flow.LMDB.terminal_counts(path, terminal_keys)
      end
    else
      {:ok, counts}
    end
  end

  defp flow_validate_index_keys(state_keys) do
    Enum.reduce_while(state_keys, :ok, fn {_state, key}, :ok ->
      case validate_key_size(key) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flow_zset_count_many(_ctx, []), do: {:ok, []}

  defp flow_zset_count_many(ctx, keys) do
    case Router.flow_index_count_all_many(ctx, keys) do
      {:ok, counts} -> {:ok, counts}
      :unavailable -> flow_zcard_many_fallback(ctx, keys)
    end
  end

  defp flow_zcard_many_fallback(ctx, keys) do
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
      case flow_zcard(ctx, key) do
        {:ok, count} -> {:cont, {:ok, [count | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, counts} -> {:ok, Enum.reverse(counts)}
      {:error, _reason} = error -> error
    end
  end

  defp flow_maybe_flush_lmdb_for_index(_ctx, _index_key, _partition_key, false), do: :ok

  defp flow_maybe_flush_lmdb_for_index(ctx, index_key, partition_key, true) do
    flow_flush_lmdb_for_index(ctx, index_key, partition_key)
  end

  defp flow_flush_lmdb_for_index(ctx, index_key, partition_key) do
    case partition_key do
      nil ->
        Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

      partition_key when is_binary(partition_key) ->
        shard_index = Router.shard_for(ctx, index_key)
        Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)
    end
  end

  defp flow_index_ids(ctx, index_key, state, partition_key, count, include_cold?, consistent?) do
    flow_index_ids(ctx, index_key, state, partition_key, count, include_cold?, consistent?, nil)
  end

  defp flow_index_ids(
         ctx,
         index_key,
         state,
         partition_key,
         count,
         include_cold?,
         consistent?,
         query
       )
       when state in @terminal_states do
    with {:ok, ram_entries} <- flow_terminal_ram_index_entries(ctx, index_key, count, query),
         {:ok, lmdb_entries} <-
           flow_terminal_lmdb_entries(
             ctx,
             index_key,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             query
           ) do
      ids =
        (ram_entries ++ lmdb_entries)
        |> Enum.sort_by(fn {id, score} -> {score, id} end)
        |> maybe_reverse_flow_index_entries(flow_index_query_reverse?(query))
        |> Enum.uniq_by(fn {id, _score} -> id end)
        |> Enum.map(fn {id, _score} -> id end)
        |> Enum.take(count)

      {:ok, ids}
    end
  end

  defp flow_index_ids(
         ctx,
         index_key,
         state,
         partition_key,
         count,
         include_cold?,
         consistent?,
         _query
       ) do
    with {:ok, ram_ids} <- flow_zrange(ctx, index_key, 0, count - 1),
         {:ok, lmdb_ids} <-
           flow_terminal_lmdb_ids(
             ctx,
             index_key,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             nil
           ) do
      {:ok, (ram_ids ++ lmdb_ids) |> Enum.uniq() |> Enum.take(count)}
    end
  end

  defp flow_index_query_reverse?(%{rev?: true}), do: true
  defp flow_index_query_reverse?(_query), do: false

  defp flow_ram_index_entries(_ctx, _index_key, count) when count <= 0, do: {:ok, []}

  defp flow_ram_index_entries(ctx, index_key, count) do
    case Router.flow_index_rank_range(ctx, index_key, 0, count - 1, false) do
      {:ok, entries} -> {:ok, entries}
      :unavailable -> {:ok, []}
    end
  end

  defp flow_terminal_ram_index_entries(ctx, index_key, count, nil) do
    flow_ram_index_entries(ctx, index_key, count)
  end

  defp flow_terminal_ram_index_entries(ctx, index_key, count, query) do
    flow_ram_index_entries(ctx, index_key, query, count)
  end

  defp flow_ram_index_entries(_ctx, _index_key, _query, count) when count <= 0, do: {:ok, []}

  defp flow_ram_index_entries(ctx, index_key, query, count) do
    case Router.flow_index_score_range_slice(
           ctx,
           index_key,
           flow_index_min_bound(query.from_ms),
           flow_index_max_bound(query.to_ms),
           query.rev?,
           0,
           count
         ) do
      {:ok, entries} -> {:ok, entries}
      :unavailable -> {:ok, []}
    end
  end

  defp flow_index_min_bound(nil), do: :neg_inf
  defp flow_index_min_bound(ms), do: {:inclusive, ms}

  defp flow_index_max_bound(nil), do: :pos_inf
  defp flow_index_max_bound(ms), do: {:inclusive, ms}

  defp flow_maybe_recount_overlapping_terminal(
         ctx,
         index_key,
         state,
         partition_key,
         ram_count,
         lmdb_count
       )
       when state in @terminal_states and lmdb_count > 0 do
    with {:ok, ram_ids} <- flow_maybe_zrange_all(ctx, index_key, ram_count),
         {:ok, lmdb_ids} <-
           flow_terminal_lmdb_ids(
             ctx,
             index_key,
             state,
             partition_key,
             lmdb_count,
             true,
             false,
             nil
           ) do
      count =
        ram_ids
        |> MapSet.new()
        |> MapSet.union(MapSet.new(lmdb_ids))
        |> MapSet.size()

      {:ok, count}
    end
  end

  defp flow_maybe_recount_overlapping_terminal(
         _ctx,
         _index_key,
         _state,
         _partition_key,
         ram_count,
         lmdb_count
       ) do
    {:ok, ram_count + lmdb_count}
  end

  defp flow_maybe_zrange_all(_ctx, _index_key, count) when count <= 0, do: {:ok, []}
  defp flow_maybe_zrange_all(ctx, index_key, count), do: flow_zrange(ctx, index_key, 0, count - 1)

  defp flow_terminal_lmdb_ids(
         _ctx,
         _index_key,
         state,
         _partition_key,
         _count,
         _include_cold?,
         _consistent?,
         _query
       )
       when state not in @terminal_states,
       do: {:ok, []}

  defp flow_terminal_lmdb_ids(
         _ctx,
         _index_key,
         _state,
         _partition_key,
         count,
         _include_cold?,
         _consistent?,
         _query
       )
       when count <= 0,
       do: {:ok, []}

  defp flow_terminal_lmdb_ids(
         ctx,
         index_key,
         _state,
         partition_key,
         count,
         include_cold?,
         consistent?,
         query
       ) do
    if include_cold? do
      with {:ok, entries} <-
             flow_lmdb_index_entries(ctx, index_key, partition_key, count, consistent?, query) do
        ids =
          entries
          |> Enum.map(fn {id, _updated_at_ms} -> id end)
          |> Enum.take(count)

        {:ok, ids}
      end
    else
      {:ok, []}
    end
  end

  defp flow_terminal_lmdb_entries(
         _ctx,
         _index_key,
         state,
         _partition_key,
         _count,
         _include_cold?,
         _consistent?,
         _query
       )
       when state not in @terminal_states,
       do: {:ok, []}

  defp flow_terminal_lmdb_entries(
         _ctx,
         _index_key,
         _state,
         _partition_key,
         count,
         _include_cold?,
         _consistent?,
         _query
       )
       when count <= 0,
       do: {:ok, []}

  defp flow_terminal_lmdb_entries(
         ctx,
         index_key,
         _state,
         partition_key,
         count,
         include_cold?,
         consistent?,
         query
       ) do
    if include_cold? do
      flow_lmdb_index_entries(ctx, index_key, partition_key, count, consistent?, query)
    else
      {:ok, []}
    end
  end

  defp flow_lmdb_index_entries(
         _ctx,
         _index_key,
         _partition_key,
         count,
         _consistent?,
         _query
       )
       when count <= 0,
       do: {:ok, []}

  defp flow_lmdb_index_entries(ctx, index_key, partition_key, count, consistent?, query) do
    with :ok <- flow_maybe_flush_lmdb_for_index(ctx, index_key, partition_key, consistent?),
         :ok <- flow_require_lmdb_mirror_healthy(ctx, index_key, partition_key) do
      prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(index_key)
      now_ms = now_ms()
      sweep_limit = flow_terminal_lmdb_sweep_limit()
      scan_count = flow_lmdb_terminal_scan_count(count, query)

      ctx
      |> flow_lmdb_paths_for_index(index_key, partition_key)
      |> Enum.reduce_while({:ok, []}, fn path, {:ok, acc} ->
        with {:ok, _swept} <-
               Ferricstore.Flow.LMDB.sweep_expired_terminal(path, now_ms, sweep_limit),
             {:ok, entries} <- flow_lmdb_terminal_prefix_entries(path, prefix, scan_count, query) do
          {:cont,
           {:ok,
            prepend_flow_chunk(flow_decode_terminal_index_entries(entries, path, now_ms), acc)}}
        else
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, chunks} ->
          entries =
            chunks
            |> flatten_flow_chunks()
            |> Enum.sort_by(fn {id, updated_at_ms} -> {updated_at_ms, id} end)
            |> maybe_reverse_flow_index_entries(flow_index_query_reverse?(query))
            |> Enum.take(count)

          {:ok, entries}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp flow_lmdb_terminal_scan_count(count, nil), do: count

  defp flow_lmdb_terminal_scan_count(count, %{from_ms: nil, to_ms: nil, rev?: false}), do: count

  defp flow_lmdb_terminal_scan_count(count, _query), do: flow_lmdb_query_scan_count(count)

  defp flow_lmdb_terminal_prefix_entries(path, prefix, limit, nil) do
    Ferricstore.Flow.LMDB.prefix_entries(path, prefix, limit)
  end

  defp flow_lmdb_terminal_prefix_entries(path, prefix, limit, query) do
    flow_lmdb_query_prefix_entries(path, prefix, limit, query)
  end

  defp flow_lmdb_query_index_entries(
         _ctx,
         _index_key,
         _partition_key,
         count,
         _consistent?,
         _query
       )
       when count <= 0,
       do: {:ok, []}

  defp flow_lmdb_query_index_entries(ctx, index_key, partition_key, count, consistent?, query) do
    with :ok <- flow_maybe_flush_lmdb_for_index(ctx, index_key, partition_key, consistent?),
         :ok <- flow_require_lmdb_mirror_healthy(ctx, index_key, partition_key) do
      prefix = Ferricstore.Flow.LMDB.query_index_prefix(index_key)
      now_ms = now_ms()
      scan_count = flow_lmdb_query_scan_count(count)

      ctx
      |> flow_lmdb_paths_for_index(index_key, partition_key)
      |> Enum.reduce_while({:ok, []}, fn path, {:ok, acc} ->
        with {:ok, entries} <-
               flow_lmdb_query_prefix_entries(path, prefix, scan_count, query) do
          {:cont,
           {:ok, prepend_flow_chunk(flow_decode_query_index_entries(entries, path, now_ms), acc)}}
        else
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, chunks} ->
          entries =
            chunks
            |> flatten_flow_chunks()
            |> Enum.sort_by(fn {id, updated_at_ms, _state_key} -> {updated_at_ms, id} end)

          {:ok, entries}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp flow_lmdb_query_prefix_entries(path, prefix, limit, %{rev?: true, to_ms: to_ms})
       when is_integer(to_ms) and to_ms >= 0 do
    Ferricstore.Flow.LMDB.prefix_entries_reverse_before(
      path,
      prefix,
      flow_lmdb_time_upper_seek_key(prefix, to_ms),
      limit
    )
  end

  defp flow_lmdb_query_prefix_entries(path, prefix, limit, %{rev?: true}) do
    Ferricstore.Flow.LMDB.prefix_entries(path, prefix, limit, true)
  end

  defp flow_lmdb_query_prefix_entries(path, prefix, limit, %{from_ms: from_ms})
       when is_integer(from_ms) and from_ms >= 0 do
    # LMDB query index keys are prefix <> padded_updated_at <> id. Seeking to
    # the timestamp prefix keeps filtered cold queries from sampling only older
    # rows before the requested time window.
    Ferricstore.Flow.LMDB.prefix_entries_after(
      path,
      prefix,
      flow_lmdb_time_seek_key(prefix, from_ms),
      limit
    )
  end

  defp flow_lmdb_query_prefix_entries(path, prefix, limit, query) do
    Ferricstore.Flow.LMDB.prefix_entries(path, prefix, limit, Map.get(query, :rev?, false))
  end

  defp flow_lmdb_time_seek_key(prefix, ms) when is_binary(prefix) and is_integer(ms),
    do: prefix <> flow_lmdb_pad_u64(ms)

  defp flow_lmdb_time_upper_seek_key(prefix, ms) when is_binary(prefix) and is_integer(ms),
    do: prefix <> flow_lmdb_pad_u64(ms) <> <<255>>

  defp flow_lmdb_pad_u64(value) when is_integer(value) and value >= 0 do
    encoded = Integer.to_string(value)

    case byte_size(encoded) do
      size when size < 20 -> binary_part(@u64_decimal_zero_pad, 0, 20 - size) <> encoded
      _size -> encoded
    end
  end

  defp flow_lmdb_query_scan_count(count) when is_integer(count) and count > 0 do
    max_scan =
      Application.get_env(
        :ferricstore,
        :flow_lmdb_query_scan_limit,
        @default_lmdb_query_scan_limit
      )

    max_scan =
      case max_scan do
        value when is_integer(value) and value > 0 -> value
        _ -> @default_lmdb_query_scan_limit
      end

    count
    |> Kernel.+(64)
    |> max(count * 4)
    |> min(max_scan)
    |> max(count)
  end

  if Mix.env() == :test do
    def __flow_history_lmdb_query_scan_count_for_test__(count, reverse? \\ false),
      do: flow_history_lmdb_query_scan_count(count, reverse?)
  end

  defp flow_history_lmdb_query_scan_count(count, true) when is_integer(count) and count > 0,
    do: count

  defp flow_history_lmdb_query_scan_count(count, false) when is_integer(count) and count > 0 do
    max_scan =
      Application.get_env(
        :ferricstore,
        :flow_lmdb_history_query_scan_limit,
        flow_max_history_max_events()
      )

    max_scan =
      case max_scan do
        value when is_integer(value) and value > 0 -> min(value, flow_max_history_max_events())
        _ -> flow_max_history_max_events()
      end

    count
    |> Kernel.+(64)
    |> max(count * 4)
    |> min(max_scan)
    |> max(count)
  end

  defp flow_require_lmdb_mirror_healthy(ctx, index_key, partition_key) do
    if flow_lmdb_mirror_degraded?(ctx, index_key, partition_key) do
      {:error, "ERR flow LMDB mirror degraded"}
    else
      :ok
    end
  end

  defp flow_lmdb_mirror_degraded?(ctx, index_key, partition_key) do
    ctx
    |> flow_lmdb_index_shards(index_key, partition_key)
    |> Enum.any?(&flow_lmdb_mirror_degraded_shard?(ctx, &1))
  end

  defp flow_lmdb_index_shards(ctx, _index_key, nil) do
    if is_integer(ctx.shard_count) and ctx.shard_count > 0 do
      Enum.to_list(0..(ctx.shard_count - 1))
    else
      []
    end
  end

  defp flow_lmdb_index_shards(ctx, index_key, _partition_key),
    do: [Router.shard_for(ctx, index_key)]

  defp flow_lmdb_mirror_degraded_shard?(ctx, shard_index) do
    flow_lmdb_mirror_degraded_flag?(ctx, shard_index) or
      flow_lmdb_flush_in_progress_shard?(ctx, shard_index)
  end

  defp flow_lmdb_mirror_degraded_flag?(ctx, shard_index) do
    flag_idx = shard_index + 1

    case Map.get(ctx, :flow_lmdb_mirror_degraded) do
      ref when is_reference(ref) ->
        flag_idx <= :atomics.info(ref).size and :atomics.get(ref, flag_idx) == 1

      _ ->
        false
    end
  rescue
    _ -> false
  end

  defp flow_lmdb_flush_in_progress_shard?(%{data_dir: data_dir}, shard_index)
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> Ferricstore.Flow.LMDB.path()
    |> Ferricstore.Flow.LMDB.flush_in_progress?()
  rescue
    _ -> false
  end

  defp flow_lmdb_flush_in_progress_shard?(_ctx, _shard_index), do: false

  defp flow_lmdb_shard_paths(data_dir, shard_count) do
    Enum.map(0..(shard_count - 1), fn shard_index ->
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()
    end)
  end

  defp flow_lmdb_paths_for_index(ctx, _index_key, nil) do
    flow_lmdb_shard_paths(ctx.data_dir, ctx.shard_count)
  end

  defp flow_lmdb_paths_for_index(ctx, index_key, partition_key) when is_binary(partition_key) do
    shard_index = Router.shard_for(ctx, index_key)

    [
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()
    ]
  end

  defp flow_terminal_lmdb_sweep_limit do
    Application.get_env(:ferricstore, :flow_lmdb_terminal_sweep_limit, 10_000)
  end

  defp flow_history_lmdb_sweep_limit do
    Application.get_env(:ferricstore, :flow_lmdb_history_sweep_limit, 10_000)
  end

  defp flow_decode_terminal_index_entries(entries, path, now_ms) do
    entries
    |> Enum.flat_map(fn {key, value} ->
      case Ferricstore.Flow.LMDB.decode_terminal_index_value(value) do
        {:ok, {id, updated_at_ms, expire_at_ms, _state_key}}
        when expire_at_ms <= 0 or expire_at_ms > now_ms ->
          [{id, updated_at_ms}]

        {:ok, {_id, _updated_at_ms, _expire_at_ms, state_key}} ->
          Ferricstore.Flow.LMDB.delete_terminal_index_entry(path, key, state_key)
          []

        :error ->
          []
      end
    end)
  end

  defp flow_decode_query_index_entries(entries, path, now_ms) do
    entries
    |> Enum.flat_map(fn {key, value} ->
      case Ferricstore.Flow.LMDB.decode_query_index_value(value) do
        {:ok, {id, updated_at_ms, expire_at_ms, state_key}}
        when expire_at_ms <= 0 or expire_at_ms > now_ms ->
          [{id, updated_at_ms, state_key}]

        {:ok, {_id, _updated_at_ms, _expire_at_ms, _state_key}} ->
          Ferricstore.Flow.LMDB.write_batch(path, [{:delete, key}])
          []

        :error ->
          []
      end
    end)
  end

  defp flow_decode_history_index_entries(entries, path, now_ms) do
    entries
    |> Enum.flat_map(fn {key, value} ->
      case Ferricstore.Flow.LMDB.decode_history_index_value(value) do
        {:ok, {event_id, event_ms, expire_at_ms, _compound_key}}
        when expire_at_ms <= 0 or expire_at_ms > now_ms ->
          [{event_id, event_ms}]

        {:ok, {_event_id, _event_ms, _expire_at_ms, _compound_key}} ->
          Ferricstore.Flow.LMDB.delete_history_index_entry(path, key)
          []

        :error ->
          []
      end
    end)
  end

  defp flow_history_entry_to_tuple({event_id, fields}) when is_list(fields) do
    {event_id, flow_fields_to_map(fields)}
  end

  defp flow_history_entry_to_tuple([event_id | fields]) when is_list(fields) do
    {event_id, flow_fields_to_map(fields)}
  end

  defp hydrate_history_values(history, _ctx, %{enabled?: false}), do: history
  defp hydrate_history_values([], _ctx, _value_return), do: []

  defp hydrate_history_values(history, ctx, %{enabled?: true, max_bytes: max_bytes}) do
    refs =
      history
      |> Enum.flat_map(fn {_event_id, fields} ->
        ["payload_ref", "result_ref", "error_ref"]
        |> Enum.map(&Map.get(fields, &1))
      end)
      |> Enum.uniq()
      |> Enum.filter(fn ref ->
        is_binary(ref) and ref != "" and byte_size(ref) <= Router.max_key_size()
      end)

    values =
      ctx
      |> Ferricstore.Flow.ValueStore.raw_mget_with_file_refs(refs, file_ref_payload_threshold(max_bytes))
      |> Enum.zip(refs)
      |> Map.new(fn {value, ref} -> {ref, value} end)

    Enum.map(history, fn {event_id, fields} ->
      hydrated =
        Enum.reduce(["payload", "result", "error"], fields, fn kind, acc ->
          ref = Map.get(acc, kind <> "_ref")
          apply_history_value_result(acc, kind, ref, Map.get(values, ref), max_bytes)
        end)

      {event_id, hydrated}
    end)
  end

  defp apply_history_value_result(fields, _kind, nil, _value, _max_bytes), do: fields
  defp apply_history_value_result(fields, _kind, "", _value, _max_bytes), do: fields

  defp apply_history_value_result(fields, kind, ref, value, max_bytes) when is_binary(ref) do
    if byte_size(ref) > Router.max_key_size() do
      Map.put(fields, kind <> "_error", "ERR #{kind}_ref key too large")
    else
      apply_history_value_result_for_valid_ref(fields, kind, value, max_bytes)
    end
  end

  defp apply_history_value_result(fields, _kind, _ref, _value, _max_bytes), do: fields

  defp apply_history_value_result_for_valid_ref(fields, kind, nil, _max_bytes) do
    fields
    |> Map.put(kind, nil)
    |> Map.put(kind <> "_missing", true)
  end

  defp apply_history_value_result_for_valid_ref(
         fields,
         kind,
         {:file_ref, _path, _offset, size},
         _max_bytes
       ) do
    fields
    |> Map.put(kind <> "_omitted", true)
    |> Map.put(kind <> "_size", flow_value_user_size_from_file_size(size))
  end

  defp apply_history_value_result_for_valid_ref(fields, kind, encoded_value, max_bytes)
       when is_binary(encoded_value) do
    {decoded, size} = Codec.decode_value_with_user_size(encoded_value)

    if size <= max_bytes do
      fields
      |> Map.put(kind, decoded)
      |> Map.put(kind <> "_size", size)
    else
      fields
      |> Map.put(kind <> "_omitted", true)
      |> Map.put(kind <> "_size", size)
    end
  end

  defp apply_history_value_result_for_valid_ref(fields, _kind, _value, _max_bytes), do: fields

  defp flow_fields_to_map(fields) when is_list(fields) do
    fields
    |> Enum.chunk_every(2)
    |> Map.new(fn [key, value] -> {key, value} end)
  end

  defp flow_zcard(ctx, key) do
    case Router.flow_index_count_all(ctx, key) do
      {:ok, count} -> {:ok, count}
      :unavailable -> {:error, "ERR flow index unavailable"}
    end
  end

  defp flow_zrange(ctx, key, start, stop) do
    case Router.flow_index_rank_range(ctx, key, start, stop, false) do
      {:ok, members} -> {:ok, Enum.map(members, fn {member, _score} -> member end)}
      :unavailable -> {:error, "ERR flow index unavailable"}
    end
  end

  defp flow_zrangebyscore(ctx, key, min, max) do
    case Router.flow_index_score_range_slice(
           ctx,
           key,
           parse_zbound(min),
           parse_zbound(max),
           false,
           0,
           :all
         ) do
      {:ok, members} -> {:ok, Enum.map(members, fn {member, _score} -> member end)}
      :unavailable -> {:error, "ERR flow index unavailable"}
    end
  end

  defp parse_zbound("-inf"), do: :neg_inf
  defp parse_zbound("+inf"), do: :pos_inf

  defp parse_zbound("(" <> rest) do
    case Float.parse(rest) do
      {score, ""} -> {:exclusive, score}
      _ -> {:error, "ERR min or max is not a float"}
    end
  end

  defp parse_zbound(value) when is_binary(value) do
    case Float.parse(value) do
      {score, ""} -> {:inclusive, score}
      _ -> {:error, "ERR min or max is not a float"}
    end
  end

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

  defp validate_unique_create_ids(_attrs_list, true), do: :ok
  defp validate_unique_create_ids(attrs_list, false), do: validate_unique_create_ids(attrs_list)

  defp validate_unique_transition_ids(_attrs_list, true), do: :ok

  defp validate_unique_transition_ids(attrs_list, false),
    do: validate_unique_transition_ids(attrs_list)

  defp reject_public_value_ref_input(opts, ref_key, value_key) do
    if Keyword.has_key?(opts, ref_key) do
      {:error, "ERR flow #{ref_key} input is not supported; use #{value_key}"}
    else
      :ok
    end
  end

  defp validate_id(id) when is_binary(id) and id != "", do: :ok
  defp validate_id(_id), do: {:error, "ERR flow id must be a non-empty string"}

  defp validate_type(type) when is_binary(type) and type != "", do: :ok
  defp validate_type(_type), do: {:error, "ERR flow type must be a non-empty string"}

  defp validate_state(_name, state) when is_binary(state) and state != "", do: :ok
  defp validate_state(name, _state), do: {:error, "ERR flow #{name} must be a non-empty string"}

  defp reject_running_state_transition("running"),
    do: {:error, "ERR flow running state is only entered by FLOW.CLAIM_DUE"}

  defp reject_running_state_transition(_state), do: :ok

  defp validate_lease_token(token) when is_binary(token) and token != "", do: :ok

  defp validate_lease_token(_token),
    do: {:error, "ERR flow lease_token must be a non-empty string"}

  defp optional_lease_token(opts) do
    case Keyword.get(opts, :lease_token, nil) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow lease_token must be a non-empty string"}
    end
  end

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  defp create_attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, type} <- required_binary(opts, :type),
         {:ok, state} <- optional_binary(opts, :state, @default_state),
         {:ok, parent_flow_id} <- optional_binary_or_nil(opts, :parent_flow_id, nil),
         :ok <- validate_ref_size(:parent_flow_id, parent_flow_id),
         {:ok, root_flow_id} <- optional_binary_or_nil(opts, :root_flow_id, nil),
         :ok <- validate_ref_size(:root_flow_id, root_flow_id),
         root_flow_id = root_flow_id || id,
         {:ok, correlation_id} <- optional_binary_or_nil(opts, :correlation_id, nil),
         :ok <- validate_ref_size(:correlation_id, correlation_id),
         {:ok, idempotent} <- optional_boolean(opts, :idempotent, false),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, run_at_ms} <- optional_non_neg_integer(opts, :run_at_ms, now),
         {:ok, retention_ttl_ms} <- optional_retention_ttl_ms(opts),
         {:ok, history_hot_max_events} <- optional_history_hot_max_events(opts),
         {:ok, history_max_events} <- optional_history_max_events(opts),
         :ok <- validate_history_event_caps(history_hot_max_events, history_max_events),
         {:ok, priority} <- optional_priority(opts, @default_priority),
         {:ok, partition_key} <- optional_partition_key(opts) do
      partition_key = partition_key || __MODULE__.Keys.auto_partition_key(id)

      attrs =
        %{
          id: id,
          type: type,
          state: state,
          partition_key: partition_key
        }
        |> maybe_put_attr(:parent_flow_id, parent_flow_id)
        |> maybe_put_attr(:root_flow_id, if(root_flow_id == id, do: nil, else: root_flow_id))
        |> maybe_put_attr(:correlation_id, correlation_id)
        |> maybe_put_default_attr(:idempotent, idempotent, false)
        |> maybe_put_attr(:retention_ttl_ms, retention_ttl_ms)
        |> maybe_put_attr(:history_hot_max_events, history_hot_max_events)
        |> maybe_put_attr(:history_max_events, history_max_events)
        |> maybe_put_default_attr(:priority, priority, @default_priority)
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_flow_value_ref(opts, :payload_ref)
        |> maybe_put_named_value_opts(opts)
        |> maybe_put_attr(:now_ms, now)
        |> maybe_put_attr(:run_at_ms, run_at_ms)

      {:ok, attrs}
    end
  end

  defp create_many_attrs(
         items,
         opts,
         partition_key,
         mismatch_error \\ "ERR flow partition_key mismatch in batch"
       ) do
    base_opts = Keyword.delete(opts, :partition_key)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, item_opts} <- create_many_item_opts(item),
           {:ok, item_partition_key} <-
             many_item_partition_key(id, partition_key, item_opts, mismatch_error),
           {:ok, attrs} <-
             create_attrs(
               id,
               merge_many_item_opts(base_opts, item_opts, item_partition_key)
             ) do
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

  defp spawn_children_attrs(parent_id, children, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(parent_id),
         :ok <- validate_children(children),
         {:ok, partition_key} <- required_partition_key(Keyword.get(opts, :partition_key)),
         {:ok, group_id} <- required_binary(opts, :group_id),
         {:ok, wait} <- optional_child_policy(opts, :wait, :all, [:all, :any, :none]),
         {:ok, on_child_failed} <-
           optional_child_policy(opts, :on_child_failed, :fail_parent, [
             :fail_parent,
             :ignore
           ]),
         {:ok, on_parent_closed} <-
           optional_child_policy(opts, :on_parent_closed, :cancel_children, [
             :cancel_children,
             :abandon_children
           ]),
         {:ok, exhaust_to} <- exhaust_to_opts(opts),
         {:ok, from_state} <- optional_binary_or_nil(opts, :from_state, nil),
         {:ok, wait_state} <- optional_binary_or_nil(opts, :wait_state, nil),
         {:ok, lease_token} <- optional_lease_token(opts),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, child_attrs} <- create_many_attrs(children, opts, partition_key, :allow_override),
         :ok <- validate_unique_create_ids(child_attrs),
         :ok <- validate_no_parent_child_id(parent_id, child_attrs),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(parent_id, partition_key)) do
      attrs =
        %{
          id: parent_id,
          partition_key: partition_key,
          group_id: group_id,
          wait: wait,
          on_child_failed: on_child_failed,
          on_parent_closed: on_parent_closed,
          exhaust_to: exhaust_to,
          from_state: from_state,
          wait_state: wait_state,
          lease_token: lease_token,
          fencing_token: fencing_token,
          children: child_attrs
        }
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  defp validate_children([_ | _]), do: :ok
  defp validate_children(_children), do: {:error, "ERR flow children must be a non-empty list"}

  defp validate_no_parent_child_id(parent_id, child_attrs) do
    if Enum.any?(child_attrs, &(Map.get(&1, :id) == parent_id)) do
      {:error, "ERR flow child id must differ from parent id"}
    else
      :ok
    end
  end

  defp optional_child_policy(opts, key, default, allowed) do
    value =
      opts
      |> Keyword.get(key, default)
      |> normalize_child_policy_value()

    if value in allowed do
      {:ok, value}
    else
      {:error, "ERR flow #{key} has unsupported value"}
    end
  end

  defp normalize_child_policy_value(value) when is_atom(value), do: value

  defp normalize_child_policy_value(value) when is_binary(value) do
    value
    |> String.downcase()
    |> String.replace("-", "_")
    |> String.to_existing_atom()
  rescue
    ArgumentError -> value
  end

  defp normalize_child_policy_value(value), do: value

  defp exhaust_to_opts(opts) do
    case Keyword.get(opts, :exhaust_to) do
      nil ->
        exhaust_to_states(Keyword.get(opts, :success), Keyword.get(opts, :failure))

      %{success: success, failure: failure} ->
        exhaust_to_states(success, failure)

      %{"success" => success, "failure" => failure} ->
        exhaust_to_states(success, failure)

      _ ->
        {:error, "ERR flow exhaust_to must include success and failure states"}
    end
  end

  defp exhaust_to_states(success, failure)
       when is_binary(success) and success != "" and is_binary(failure) and failure != "" do
    {:ok, %{"success" => success, "failure" => failure}}
  end

  defp exhaust_to_states(_success, _failure) do
    {:error, "ERR flow exhaust_to must include success and failure states"}
  end

  defp create_many_item_opts(id) when is_binary(id), do: {:ok, id, []}

  defp create_many_item_opts(%{id: id} = item) when is_binary(id) do
    {:ok, id, create_many_item_opts_from_map(item)}
  end

  defp create_many_item_opts(%{"id" => id} = item) when is_binary(id) do
    {:ok, id, create_many_item_opts_from_map(item)}
  end

  defp create_many_item_opts({id, item_opts}) when is_binary(id) and is_list(item_opts) do
    if Keyword.keyword?(item_opts) do
      {:ok, id, item_opts}
    else
      {:error, "ERR flow opts must be a keyword list"}
    end
  end

  defp create_many_item_opts({:id, id, :payload_ref, payload_ref}) when is_binary(id) do
    {:ok, id, [payload_ref: payload_ref]}
  end

  defp create_many_item_opts({:id, id, :payload, payload}) when is_binary(id) do
    {:ok, id, [payload: payload]}
  end

  defp create_many_item_opts({:id, id, :partition_key, partition_key, :payload_ref, payload_ref})
       when is_binary(id) do
    {:ok, id, [partition_key: partition_key, payload_ref: payload_ref]}
  end

  defp create_many_item_opts(_item), do: {:error, "ERR flow id must be a non-empty string"}

  defp create_many_item_opts_from_map(item) do
    []
    |> maybe_put_item_opt(:type, item, :type, "type")
    |> maybe_put_item_opt(:state, item, :state, "state")
    |> maybe_put_item_opt(:run_at_ms, item, :run_at_ms, "run_at_ms")
    |> maybe_put_item_opt(:priority, item, :priority, "priority")
    |> maybe_put_item_opt(:payload, item, :payload, "payload")
    |> maybe_put_item_opt(:payload_ref, item, :payload_ref, "payload_ref")
    |> maybe_put_item_opt(:values, item, :values, "values")
    |> maybe_put_item_opt(:value_refs, item, :value_refs, "value_refs")
    |> maybe_put_item_opt(:partition_key, item, :partition_key, "partition_key")
    |> maybe_put_item_opt(:parent_flow_id, item, :parent_flow_id, "parent_flow_id")
    |> maybe_put_item_opt(:root_flow_id, item, :root_flow_id, "root_flow_id")
    |> maybe_put_item_opt(:correlation_id, item, :correlation_id, "correlation_id")
    |> maybe_put_item_opt(:idempotent, item, :idempotent, "idempotent")
    |> maybe_put_item_opt(:retention_ttl_ms, item, :retention_ttl_ms, "retention_ttl_ms")
    |> maybe_put_item_opt(
      :history_hot_max_events,
      item,
      :history_hot_max_events,
      "history_hot_max_events"
    )
    |> maybe_put_item_opt(:history_max_events, item, :history_max_events, "history_max_events")
  end

  defp transition_attrs(id, from_state, to_state, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         :ok <- validate_state(:from, from_state),
         :ok <- validate_state(:to, to_state),
         :ok <- reject_running_state_transition(to_state),
         {:ok, lease_token} <- optional_lease_token(opts),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, run_at_ms} <- optional_non_neg_integer(opts, :run_at_ms, now),
         {:ok, priority} <- optional_priority_or_nil(opts) do
      attrs =
        %{
          id: id,
          from_state: from_state,
          to_state: to_state,
          fencing_token: fencing_token,
          partition_key: partition_key
        }
        |> maybe_put_attr(:lease_token, lease_token)
        |> maybe_put_attr(:priority, priority)
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_flow_value_ref(opts, :payload_ref)
        |> maybe_put_named_value_opts(opts)
        |> maybe_put_attr(:now_ms, now)
        |> maybe_put_attr(:run_at_ms, run_at_ms)

      {:ok, attrs}
    end
  end

  defp signal_attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, signal} <- required_binary(opts, :signal),
         {:ok, if_state} <- optional_signal_states(opts),
         {:ok, transition_to} <- optional_binary_or_nil(opts, :transition_to, nil),
         :ok <- reject_running_state_transition(transition_to),
         {:ok, idempotency_key} <- optional_binary_or_nil(opts, :idempotency_key, nil),
         :ok <- validate_ref_size(:idempotency_key, idempotency_key),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, run_at_ms} <- optional_non_neg_integer_or_nil(opts, :run_at_ms) do
      attrs =
        %{
          id: id,
          signal: signal,
          partition_key: partition_key
        }
        |> maybe_put_attr(:if_state, if_state)
        |> maybe_put_attr(:transition_to, transition_to)
        |> maybe_put_attr(:idempotency_key, idempotency_key)
        |> maybe_put_named_value_opts(opts)
        |> maybe_put_attr(:now_ms, now)
        |> maybe_put_attr(:run_at_ms, run_at_ms)

      {:ok, attrs}
    end
  end

  defp optional_signal_states(opts) do
    values = Keyword.get_values(opts, :if_state)

    case values do
      [] -> {:ok, nil}
      [state] -> normalize_signal_states(state)
      [_ | _] -> normalize_signal_states(values)
    end
  end

  defp normalize_signal_states(state) when is_binary(state) and state != "", do: {:ok, state}

  defp normalize_signal_states(states) when is_list(states) do
    states
    |> Enum.reduce_while({:ok, []}, fn
      state, {:ok, acc} when is_binary(state) and state != "" ->
        {:cont, {:ok, [state | acc]}}

      _bad, {:ok, _acc} ->
        {:halt, {:error, "ERR flow if_state must be a non-empty string"}}
    end)
    |> case do
      {:ok, [single]} -> {:ok, single}
      {:ok, values} -> {:ok, values |> Enum.reverse() |> Enum.uniq()}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_signal_states(_state),
    do: {:error, "ERR flow if_state must be a non-empty string"}

  defp retry_attrs(id, lease_token, opts) do
    with :ok <- validate_opts(opts),
         :ok <- reject_public_value_ref_input(opts, :error_ref, :error),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, run_at_ms} <- optional_non_neg_integer_or_nil(opts, :run_at_ms),
         {:ok, retry_policy} <- optional_retry_policy(opts) do
      attrs =
        %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          partition_key: partition_key
        }
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_flow_value(opts, :error)
        |> maybe_put_attr(:now_ms, now)
        |> maybe_put_attr(:run_at_ms, run_at_ms)
        |> maybe_put_attr(:retry_policy, retry_policy)

      {:ok, attrs}
    end
  end

  defp complete_attrs(id, lease_token, opts) do
    with :ok <- validate_opts(opts),
         :ok <- reject_public_value_ref_input(opts, :result_ref, :result),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, ttl_ms} <- optional_pos_integer_or_nil(opts, :ttl_ms) do
      attrs =
        %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          partition_key: partition_key
        }
        |> maybe_put_attr(:ttl_ms, ttl_ms)
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_flow_value(opts, :result)
        |> maybe_put_named_value_opts(opts)
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  defp extend_lease_attrs(id, lease_token, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, lease_ms} <- optional_pos_integer(opts, :lease_ms, @default_lease_ms) do
      attrs =
        %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          lease_ms: lease_ms,
          partition_key: partition_key
        }
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  defp complete_many_attrs(items, opts, partition_key) do
    base_opts = Keyword.delete(opts, :partition_key)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, lease_token, item_opts} <- complete_many_item_opts(item),
           {:ok, item_partition_key} <- many_item_partition_key(id, partition_key, item_opts),
           {:ok, attrs} <-
             complete_attrs(
               id,
               lease_token,
               merge_many_item_opts(base_opts, item_opts, item_partition_key)
             ) do
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

  defp fail_attrs(id, lease_token, opts) do
    with :ok <- validate_opts(opts),
         :ok <- reject_public_value_ref_input(opts, :error_ref, :error),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, ttl_ms} <- optional_pos_integer_or_nil(opts, :ttl_ms) do
      attrs =
        %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          partition_key: partition_key
        }
        |> maybe_put_attr(:ttl_ms, ttl_ms)
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_flow_value(opts, :error)
        |> maybe_put_named_value_opts(opts)
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  defp cancel_attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- reject_external_ref_input(opts, :reason_ref, :reason),
         :ok <- validate_id(id),
         {:ok, lease_token} <- optional_lease_token(opts),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, ttl_ms} <- optional_pos_integer_or_nil(opts, :ttl_ms),
         :ok <- validate_cancel_reason_source(opts) do
      attrs =
        %{
          id: id,
          fencing_token: fencing_token,
          partition_key: partition_key
        }
        |> maybe_put_attr(:lease_token, lease_token)
        |> maybe_put_attr(:ttl_ms, ttl_ms)
        |> maybe_put_cancel_reason(opts)
        |> maybe_put_named_value_opts(opts)
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  defp validate_cancel_reason_source(opts) do
    if Keyword.has_key?(opts, :reason) and Keyword.has_key?(opts, :reason_ref) do
      {:error, "ERR flow reason and reason_ref are mutually exclusive"}
    else
      :ok
    end
  end

  defp reject_external_ref_input(opts, ref_key, replacement_key) do
    if Keyword.has_key?(opts, ref_key) do
      {:error, "ERR flow #{ref_key} input is not supported; use #{replacement_key}"}
    else
      :ok
    end
  end

  defp maybe_put_cancel_reason(attrs, opts) do
    if Keyword.has_key?(opts, :reason) do
      Map.put(attrs, :error, Keyword.fetch!(opts, :reason))
    else
      attrs
    end
  end

  defp rewind_attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- reject_external_ref_input(opts, :reason_ref, :reason),
         :ok <- validate_id(id),
         {:ok, to_event} <- required_binary(opts, :to_event),
         {:ok, expect_state} <- optional_binary_or_nil(opts, :expect_state, nil),
         {:ok, run_at_ms} <- optional_non_neg_integer_or_nil(opts, :run_at_ms),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         :ok <- validate_key_size(__MODULE__.Keys.history_key(id, partition_key)) do
      attrs =
        %{
          id: id,
          to_event: to_event,
          expect_state: expect_state,
          run_at_ms: run_at_ms,
          reason_ref: nil,
          partition_key: partition_key
        }
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  defp pipeline_write_command({:create, id, opts}) do
    with {:ok, attrs} <- create_attrs(id, opts) do
      pipeline_state_command(:flow_create, attrs)
    end
  end

  defp pipeline_write_command({:flow_create, id, opts}) do
    pipeline_write_command({:create, id, opts})
  end

  defp pipeline_write_command({:transition, id, from_state, to_state, opts}) do
    with {:ok, attrs} <- transition_attrs(id, from_state, to_state, opts) do
      pipeline_state_command(:flow_transition, attrs)
    end
  end

  defp pipeline_write_command({:flow_transition, id, from_state, to_state, opts}) do
    pipeline_write_command({:transition, id, from_state, to_state, opts})
  end

  defp pipeline_write_command({:complete, id, lease_token, opts}) do
    with {:ok, attrs} <- complete_attrs(id, lease_token, opts) do
      {:ok, :terminal, {:complete, attrs}}
    end
  end

  defp pipeline_write_command({:flow_complete, id, lease_token, opts}) do
    pipeline_write_command({:complete, id, lease_token, opts})
  end

  defp pipeline_write_command({:retry, id, lease_token, opts}) do
    with {:ok, attrs} <- retry_attrs(id, lease_token, opts) do
      {:ok, :terminal, {:retry, attrs}}
    end
  end

  defp pipeline_write_command({:flow_retry, id, lease_token, opts}) do
    pipeline_write_command({:retry, id, lease_token, opts})
  end

  defp pipeline_write_command({:fail, id, lease_token, opts}) do
    with {:ok, attrs} <- fail_attrs(id, lease_token, opts) do
      {:ok, :terminal, {:fail, attrs}}
    end
  end

  defp pipeline_write_command({:flow_fail, id, lease_token, opts}) do
    pipeline_write_command({:fail, id, lease_token, opts})
  end

  defp pipeline_write_command({:cancel, id, opts}) do
    with {:ok, attrs} <- cancel_attrs(id, opts) do
      {:ok, :terminal, {:cancel, attrs}}
    end
  end

  defp pipeline_write_command({:flow_cancel, id, opts}) do
    pipeline_write_command({:cancel, id, opts})
  end

  defp pipeline_write_command({:rewind, id, opts}) do
    with {:ok, attrs} <- rewind_attrs(id, opts) do
      pipeline_state_command(:flow_rewind, attrs)
    end
  end

  defp pipeline_write_command({:flow_rewind, id, opts}) do
    pipeline_write_command({:rewind, id, opts})
  end

  defp pipeline_write_command(_op), do: {:error, "ERR unsupported flow pipeline command"}

  defp pipeline_state_command(command, %{id: id, partition_key: partition_key} = attrs) do
    key = __MODULE__.Keys.state_key(id, partition_key)
    {:ok, :state, {key, {command, key, attrs}}}
  end

  defp pipeline_claim_due_command({:claim_due, type, opts})
       when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts, return: true),
         :ok <- validate_type(type),
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
         {:ok, partition_key, partition_keys} <- optional_claim_partitions(opts),
         partition_filter = partition_keys || partition_key,
         :ok <- validate_claim_due_keys(type, state, priority, partition_filter) do
      normalized_opts =
        claim_due_normalized_opts(
          state,
          worker,
          lease_ms,
          limit,
          priority,
          now,
          return_mode,
          payload_return,
          reclaim_expired?,
          reclaim_ratio,
          partition_key,
          partition_keys,
          named_values
        )

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

      queue_key = {type, state, priority, now, partition_filter}

      key =
        {type, state, worker, lease_ms, priority, now, return_mode, payload_return, named_values,
         reclaim_expired?, reclaim_ratio, partition_filter}

      {:ok,
       %{
         type: type,
         attrs: attrs,
         opts: normalized_opts,
         limit: limit,
         key: key,
         queue_key: queue_key,
         return_mode: return_mode,
         payload_return: payload_return,
         named_values: named_values,
         reclaim_expired?: reclaim_expired?,
         reclaim_ratio: reclaim_ratio,
         groupable?: true
       }}
    end
  end

  defp pipeline_claim_due_command(_op), do: {:error, "ERR unsupported flow pipeline command"}

  defp pipeline_claim_due_results(commands, ctx, acc, stats) do
    if global_claim_grouping_safe?(commands) do
      pipeline_claim_due_grouped_results(commands, ctx, stats)
    else
      pipeline_claim_due_adjacent_results(commands, ctx, acc, stats)
    end
  end

  defp pipeline_claim_due_adjacent_results([], _ctx, acc, stats), do: {Enum.reverse(acc), stats}

  defp pipeline_claim_due_adjacent_results([{:error, _reason} = error | rest], ctx, acc, stats),
    do: pipeline_claim_due_adjacent_results(rest, ctx, [error | acc], stats)

  defp pipeline_claim_due_adjacent_results([{:ok, claim} | rest], ctx, acc, stats) do
    {run, rest} = take_compatible_claims(rest, claim.key, [claim])
    claims = Enum.reverse(run)
    {results, stats} = execute_claim_due_run(ctx, claims, stats)
    pipeline_claim_due_adjacent_results(rest, ctx, prepend_claim_due_results(results, acc), stats)
  end

  defp prepend_claim_due_results(results, acc) do
    Enum.reduce(results, acc, fn result, acc -> [result | acc] end)
  end

  defp take_compatible_claims(
         [{:ok, %{key: key, groupable?: true} = claim} | rest],
         key,
         [
           %{groupable?: true} | _
         ] = acc
       ),
       do: take_compatible_claims(rest, key, [claim | acc])

  defp take_compatible_claims(rest, _key, acc), do: {acc, rest}

  defp global_claim_grouping_safe?(commands) do
    commands
    |> Enum.reduce_while(%{}, fn
      {:ok, %{groupable?: false}}, _seen ->
        {:halt, false}

      {:ok, %{queue_key: queue_key, key: key, groupable?: true}}, seen ->
        case Map.get(seen, queue_key) do
          nil -> {:cont, Map.put(seen, queue_key, key)}
          ^key -> {:cont, seen}
          _conflicting_key -> {:halt, false}
        end

      {:error, _reason}, seen ->
        {:cont, seen}
    end)
    |> is_map()
  end

  defp pipeline_claim_due_grouped_results(commands, ctx, stats) do
    {groups, indexed_results} =
      commands
      |> Enum.with_index()
      |> Enum.reduce({%{}, %{}}, fn
        {{:ok, claim}, idx}, {group_acc, result_acc} ->
          {Map.update(group_acc, claim.key, [{idx, claim}], fn acc -> [{idx, claim} | acc] end),
           result_acc}

        {{:error, _reason} = error, idx}, {group_acc, result_acc} ->
          {group_acc, Map.put(result_acc, idx, error)}
      end)

    {singletons, indexed_results, stats} =
      Enum.reduce(groups, {[], indexed_results, stats}, fn {_key, indexed_claims},
                                                           {singleton_acc, result_acc, stats_acc} ->
        indexed_claims = Enum.reverse(indexed_claims)

        case indexed_claims do
          [{_idx, _claim} = singleton] ->
            {[singleton | singleton_acc], result_acc, stats_acc}

          _ ->
            claims = Enum.map(indexed_claims, fn {_idx, claim} -> claim end)
            {results, stats_acc} = execute_claim_due_run(ctx, claims, stats_acc)

            {result_acc, stats_acc} =
              indexed_claims
              |> Enum.map(fn {idx, _claim} -> idx end)
              |> Enum.zip(results)
              |> Enum.reduce({result_acc, stats_acc}, fn {idx, result}, {acc, stats} ->
                {Map.put(acc, idx, result), stats}
              end)

            {singleton_acc, result_acc, stats_acc}
        end
      end)

    {indexed_results, stats} =
      execute_claim_due_singleton_batch(ctx, Enum.reverse(singletons), indexed_results, stats)

    results = for idx <- 0..(length(commands) - 1), do: Map.fetch!(indexed_results, idx)
    {results, stats}
  end

  defp execute_claim_due_singleton_batch(_ctx, [], indexed_results, stats),
    do: {indexed_results, stats}

  defp execute_claim_due_singleton_batch(ctx, indexed_claims, indexed_results, stats) do
    {router_claims, routed_claims} =
      Enum.split_with(indexed_claims, fn {_idx, claim} ->
        pipeline_claim_due_router_required?(claim)
      end)

    {indexed_results, stats} =
      execute_claim_due_router_singletons(ctx, router_claims, indexed_results, stats)

    execute_claim_due_routed_singleton_batch(ctx, routed_claims, indexed_results, stats)
  end

  defp execute_claim_due_router_singletons(_ctx, [], indexed_results, stats),
    do: {indexed_results, stats}

  defp execute_claim_due_router_singletons(ctx, indexed_claims, indexed_results, stats) do
    indexed_results =
      Enum.reduce(indexed_claims, indexed_results, fn {idx, claim}, acc ->
        result =
          pipeline_claim_due_hydrated_result(
            ctx,
            claim,
            pipeline_claim_due_router(ctx, claim.attrs)
          )

        Map.put(acc, idx, result)
      end)

    {indexed_results, %{stats | groups: stats.groups + length(indexed_claims)}}
  end

  defp execute_claim_due_routed_singleton_batch(_ctx, [], indexed_results, stats),
    do: {indexed_results, stats}

  defp execute_claim_due_routed_singleton_batch(ctx, indexed_claims, indexed_results, stats) do
    keyed_commands =
      Enum.map(indexed_claims, fn {_idx, claim} ->
        key = pipeline_claim_due_route_key(claim.attrs)
        {key, {:flow_claim_due, key, claim.attrs}}
      end)

    results = Router.pipeline_write_batch(ctx, keyed_commands)

    indexed_results =
      indexed_claims
      |> Enum.zip(results)
      |> Enum.reduce(indexed_results, fn {{idx, claim}, result}, acc ->
        Map.put(acc, idx, pipeline_claim_due_hydrated_result(ctx, claim, result))
      end)

    {indexed_results, %{stats | groups: stats.groups + 1, batched_calls: stats.batched_calls + 1}}
  end

  defp pipeline_claim_due_router_required?(%{attrs: %{partition_keys: [_ | _]}}), do: true
  defp pipeline_claim_due_router_required?(%{attrs: %{partition_key: :auto}}), do: true
  defp pipeline_claim_due_router_required?(%{attrs: %{partition_key: :any}}), do: true
  defp pipeline_claim_due_router_required?(_claim), do: false

  defp pipeline_claim_due_route_key(%{
         type: type,
         state: state,
         priority: priority,
         partition_keys: [partition_key | _]
       }) do
    __MODULE__.Keys.due_key(type, pipeline_claim_route_state(state), priority || 0, partition_key)
  end

  defp pipeline_claim_due_route_key(%{
         type: type,
         state: state,
         priority: priority,
         partition_key: partition_key
       }) do
    __MODULE__.Keys.due_key(type, pipeline_claim_route_state(state), priority || 0, partition_key)
  end

  defp pipeline_claim_route_state(:any), do: "queued"
  defp pipeline_claim_route_state([state | _]) when is_binary(state), do: state
  defp pipeline_claim_route_state(state) when is_binary(state), do: state
  defp pipeline_claim_route_state(_state), do: "queued"

  defp pipeline_claim_due_hydrated_result(ctx, claim, {:ok, records}) when is_list(records) do
    {:ok,
     claim_due_return_records(
       ctx,
       records,
       claim.payload_return,
       claim.return_mode,
       claim.named_values
     )}
  end

  defp pipeline_claim_due_hydrated_result(_ctx, _claim, other), do: other

  defp execute_claim_due_run(ctx, [%{type: type, opts: opts}], stats) do
    {[claim_due_result(ctx, type, opts)], %{stats | groups: stats.groups + 1}}
  end

  defp execute_claim_due_run(
         ctx,
         [
           %{
             attrs: %{state: state},
             reclaim_expired?: true,
             reclaim_ratio: reclaim_ratio
           }
           | _
         ] = claims,
         stats
       )
       when state != "running" and reclaim_ratio > 0 do
    results = execute_claim_due_reclaim_run(ctx, claims, reclaim_ratio)
    {results, %{stats | groups: stats.groups + 1, coalesced_calls: stats.coalesced_calls + 1}}
  end

  defp execute_claim_due_run(ctx, [%{type: type, opts: opts} | _] = claims, stats) do
    total_limit = Enum.reduce(claims, 0, fn %{limit: limit}, acc -> acc + limit end)
    combined_opts = Keyword.put(opts, :limit, total_limit)

    results =
      case claim_due_result(ctx, type, combined_opts) do
        {:ok, records} ->
          split_claim_due_records(records, claims, [])

        {:error, _reason} = error ->
          List.duplicate(error, length(claims))
      end

    {results, %{stats | groups: stats.groups + 1, coalesced_calls: stats.coalesced_calls + 1}}
  end

  defp execute_claim_due_reclaim_run(ctx, [%{attrs: base_attrs} | _] = claims, reclaim_ratio) do
    initial_caps =
      Enum.map(claims, fn %{limit: limit} ->
        max(1, div(limit * reclaim_ratio + 99, 100))
      end)

    with {:ok, reclaimed_first} <-
           pipeline_claim_due_router(ctx, %{
             base_attrs
             | state: "running",
               limit: Enum.sum(initial_caps)
           }),
         {first_allocations, _unused} <- allocate_claim_due_records(reclaimed_first, initial_caps),
         normal_caps = remaining_claim_due_caps(claims, first_allocations),
         normal_attrs =
           claim_normal_attrs(
             base_attrs,
             claim_normal_state_filter(base_attrs.state),
             Enum.sum(normal_caps)
           ),
         {:ok, normal} <- pipeline_claim_due_router_maybe(ctx, normal_attrs),
         {normal_allocations, _unused} <- allocate_claim_due_records(normal, normal_caps),
         final_caps = remaining_claim_due_caps(claims, first_allocations, normal_allocations),
         {:ok, reclaimed_more} <-
           pipeline_claim_due_router(ctx, %{
             base_attrs
             | state: "running",
               limit: Enum.sum(final_caps)
           }),
         {final_allocations, _unused} <- allocate_claim_due_records(reclaimed_more, final_caps) do
      [first_allocations, normal_allocations, final_allocations]
      |> combine_claim_due_allocations()
      |> Enum.map(fn allocations ->
        {:ok, Enum.flat_map(allocations, & &1)}
      end)
      |> hydrate_claim_due_pipeline_results(
        ctx,
        hd(claims).payload_return,
        hd(claims).return_mode,
        hd(claims).named_values
      )
    else
      {:error, _reason} = error -> List.duplicate(error, length(claims))
    end
  end

  defp pipeline_claim_due_router_maybe(_ctx, nil), do: {:ok, []}
  defp pipeline_claim_due_router_maybe(_ctx, %{limit: limit}) when limit <= 0, do: {:ok, []}
  defp pipeline_claim_due_router_maybe(ctx, attrs), do: pipeline_claim_due_router(ctx, attrs)

  defp pipeline_claim_due_router(_ctx, %{limit: limit}) when limit <= 0, do: {:ok, []}
  defp pipeline_claim_due_router(ctx, attrs), do: Router.flow_claim_due(ctx, attrs)

  defp allocate_claim_due_records(records, caps) do
    Enum.map_reduce(caps, records, fn cap, remaining_records ->
      Enum.split(remaining_records, cap)
    end)
  end

  defp combine_claim_due_allocations([first_allocations, normal_allocations, final_allocations]) do
    first_allocations
    |> Enum.zip(normal_allocations)
    |> Enum.zip(final_allocations)
    |> Enum.map(fn {{first, normal}, final} -> [first, normal, final] end)
  end

  defp remaining_claim_due_caps(claims, allocations) do
    claims
    |> Enum.zip(allocations)
    |> Enum.map(fn {%{limit: limit}, records} -> limit - length(records) end)
  end

  defp remaining_claim_due_caps(claims, first_allocations, normal_allocations) do
    claims
    |> Enum.zip(first_allocations)
    |> Enum.zip(normal_allocations)
    |> Enum.map(fn {{%{limit: limit}, first}, normal} ->
      limit - length(first) - length(normal)
    end)
  end

  defp hydrate_claim_due_pipeline_results(
         results,
         ctx,
         payload_return,
         return_mode,
         named_values
       ) do
    Enum.map(results, fn
      {:ok, records} ->
        {:ok, claim_due_return_records(ctx, records, payload_return, return_mode, named_values)}

      other ->
        other
    end)
  end

  defp split_claim_due_records(_records, [], acc), do: Enum.reverse(acc)

  defp split_claim_due_records(records, [%{limit: limit} | rest], acc) do
    {claimed, records} = Enum.split(records, limit)
    split_claim_due_records(records, rest, [{:ok, claimed} | acc])
  end

  defp claim_due_normalized_opts(
         state,
         worker,
         lease_ms,
         limit,
         priority,
         now,
         return_mode,
         payload_return,
         reclaim_expired?,
         reclaim_ratio,
         partition_key,
         partition_keys,
         named_values
       ) do
    [
      state: state,
      worker: worker,
      lease_ms: lease_ms,
      limit: limit,
      return: return_mode,
      payload: payload_return.enabled?,
      payload_max_bytes: payload_return.max_bytes,
      reclaim_expired: reclaim_expired?,
      reclaim_ratio: reclaim_ratio
    ]
    |> maybe_put_keyword(:priority, priority)
    |> maybe_put_keyword(:now_ms, now)
    |> maybe_put_keyword(:partition_key, partition_key)
    |> maybe_put_keyword(:partition_keys, partition_keys)
    |> maybe_put_keyword(:values, named_values)
  end

  defp maybe_put_keyword(opts, _key, nil), do: opts
  defp maybe_put_keyword(opts, key, value), do: Keyword.put(opts, key, value)

  defp pipeline_read_command(_ctx, {:get, id, opts}) when is_binary(id) and is_list(opts) do
    with :ok <- validate_id(id),
         :ok <- validate_opts(opts),
         {:ok, payload_return} <- payload_return_opts(opts, false),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)) do
      {:get, id, partition_key, payload_return}
    end
  end

  defp pipeline_read_command(ctx, {:flow_get, id, opts}) do
    pipeline_read_command(ctx, {:get, id, opts})
  end

  defp pipeline_read_command(_ctx, {:history, id, opts}) when is_binary(id) and is_list(opts) do
    with {:ok, {partition_key, history_key, count, include_cold?, consistent?, value_return}} <-
           history_query_attrs(id, opts) do
      {:history, id, partition_key, history_key, count, include_cold?, consistent?, value_return}
    end
  end

  defp pipeline_read_command(ctx, {:flow_history, id, opts}) do
    pipeline_read_command(ctx, {:history, id, opts})
  end

  defp pipeline_read_command(ctx, {:list, type, opts}) when is_binary(type) and is_list(opts),
    do: pipeline_read_result(fn -> list(ctx, type, opts) end)

  defp pipeline_read_command(ctx, {:flow_list, type, opts}) do
    pipeline_read_command(ctx, {:list, type, opts})
  end

  defp pipeline_read_command(ctx, {:terminals, type, opts})
       when is_binary(type) and is_list(opts),
       do: pipeline_read_result(fn -> terminals(ctx, type, opts) end)

  defp pipeline_read_command(ctx, {:flow_terminals, type, opts}) do
    pipeline_read_command(ctx, {:terminals, type, opts})
  end

  defp pipeline_read_command(ctx, {:failures, type, opts}) when is_binary(type) and is_list(opts),
    do: pipeline_read_result(fn -> failures(ctx, type, opts) end)

  defp pipeline_read_command(ctx, {:flow_failures, type, opts}) do
    pipeline_read_command(ctx, {:failures, type, opts})
  end

  defp pipeline_read_command(ctx, {:by_parent, parent_flow_id, opts})
       when is_binary(parent_flow_id) and is_list(opts),
       do: pipeline_read_result(fn -> by_parent(ctx, parent_flow_id, opts) end)

  defp pipeline_read_command(ctx, {:flow_by_parent, parent_flow_id, opts}) do
    pipeline_read_command(ctx, {:by_parent, parent_flow_id, opts})
  end

  defp pipeline_read_command(ctx, {:by_root, root_flow_id, opts})
       when is_binary(root_flow_id) and is_list(opts),
       do: pipeline_read_result(fn -> by_root(ctx, root_flow_id, opts) end)

  defp pipeline_read_command(ctx, {:flow_by_root, root_flow_id, opts}) do
    pipeline_read_command(ctx, {:by_root, root_flow_id, opts})
  end

  defp pipeline_read_command(ctx, {:by_correlation, correlation_id, opts})
       when is_binary(correlation_id) and is_list(opts),
       do: pipeline_read_result(fn -> by_correlation(ctx, correlation_id, opts) end)

  defp pipeline_read_command(ctx, {:flow_by_correlation, correlation_id, opts}) do
    pipeline_read_command(ctx, {:by_correlation, correlation_id, opts})
  end

  defp pipeline_read_command(ctx, {:info, type, opts}) when is_binary(type) and is_list(opts),
    do: pipeline_read_result(fn -> info(ctx, type, opts) end)

  defp pipeline_read_command(ctx, {:flow_info, type, opts}) do
    pipeline_read_command(ctx, {:info, type, opts})
  end

  defp pipeline_read_command(ctx, {:stuck, type, opts}) when is_binary(type) and is_list(opts),
    do: pipeline_read_result(fn -> stuck(ctx, type, opts) end)

  defp pipeline_read_command(ctx, {:flow_stuck, type, opts}) do
    pipeline_read_command(ctx, {:stuck, type, opts})
  end

  defp pipeline_read_command(_ctx, _op),
    do: {:error, "ERR unsupported flow pipeline read command"}

  defp pipeline_read_result(read_fun), do: {:other, read_fun}

  defp pipeline_read_get_results([], _ctx), do: %{}

  defp pipeline_read_get_results(get_ops, ctx) do
    decoded =
      get_ops
      |> Enum.group_by(fn {_idx, _id, partition_key, _payload_return} -> partition_key end)
      |> Enum.flat_map(fn {partition_key, group} ->
        ids = Enum.map(group, fn {_idx, id, _partition_key, _payload_return} -> id end)
        values = Router.flow_batch_get(ctx, ids, partition_key)

        group
        |> Enum.zip(values)
        |> Enum.map(fn {{idx, _id, _partition_key, payload_return}, value} ->
          {idx, pipeline_read_decode_get(value), payload_return}
        end)
      end)

    decoded
    |> hydrate_pipeline_get_results(ctx)
    |> Map.new()
  end

  defp hydrate_pipeline_get_results(decoded, ctx) do
    {records, pass_through} =
      Enum.reduce(decoded, {[], []}, fn
        {idx, {:ok, record}, payload_return}, {records_acc, pass_acc} when is_map(record) ->
          {[{idx, record, payload_return} | records_acc], pass_acc}

        {idx, result, _payload_return}, {records_acc, pass_acc} ->
          {records_acc, [{idx, result} | pass_acc]}
      end)

    hydrated =
      records
      |> Enum.reverse()
      |> Enum.group_by(fn {_idx, _record, payload_return} ->
        {Map.fetch!(payload_return, :enabled?), Map.fetch!(payload_return, :max_bytes)}
      end)
      |> Enum.flat_map(fn
        {{false, _max_bytes}, entries} ->
          Enum.map(entries, fn {idx, record, _payload_return} -> {idx, {:ok, record}} end)

        {{true, max_bytes}, entries} ->
          hydrated_records =
            hydrate_payload_records(
              ctx,
              Enum.map(entries, fn {_idx, record, _payload_return} -> record end),
              %{enabled?: true, max_bytes: max_bytes}
            )

          entries
          |> Enum.map(fn {idx, _record, _payload_return} -> idx end)
          |> Enum.zip(hydrated_records)
          |> Enum.map(fn {idx, record} -> {idx, {:ok, record}} end)
      end)

    pass_through ++ hydrated
  end

  defp pipeline_read_decode_get(nil), do: {:ok, nil}
  defp pipeline_read_decode_get(value) when is_binary(value), do: safe_decode_record(value)
  defp pipeline_read_decode_get({:error, _reason} = error), do: error
  defp pipeline_read_decode_get(_other), do: {:ok, nil}

  defp hydrate_payload_result(_ctx, {:ok, nil}, _payload_return), do: {:ok, nil}

  defp hydrate_payload_result(ctx, {:ok, record}, payload_return) when is_map(record) do
    {:ok, hd(hydrate_payload_records(ctx, [record], payload_return))}
  end

  defp hydrate_payload_result(_ctx, other, _payload_return), do: other

  defp hydrate_payload_records(_ctx, records, %{enabled?: false}), do: records
  defp hydrate_payload_records(_ctx, [], _payload_return), do: []

  defp hydrate_payload_records(ctx, records, %{enabled?: true, max_bytes: max_bytes}) do
    ref_entries =
      records
      |> Enum.with_index()
      |> Enum.flat_map(fn {record, idx} ->
        [
          {idx, :payload, Map.get(record, :payload_ref)},
          {idx, :result, Map.get(record, :result_ref)},
          {idx, :error, Map.get(record, :error_ref)}
        ]
      end)

    fetchable_refs =
      ref_entries
      |> Enum.map(fn {_idx, _kind, ref} -> ref end)
      |> Enum.uniq()
      |> Enum.filter(fn ref ->
        is_binary(ref) and ref != "" and byte_size(ref) <= Router.max_key_size()
      end)

    values =
      ctx
      |> Ferricstore.Flow.ValueStore.raw_mget_with_file_refs(fetchable_refs, file_ref_payload_threshold(max_bytes))
      |> Enum.zip(fetchable_refs)
      |> Map.new(fn {value, ref} -> {ref, value} end)

    Enum.map(records, fn record ->
      Enum.reduce([:payload, :result, :error], record, fn kind, acc ->
        ref = Map.get(acc, flow_value_ref_field(kind))
        apply_flow_value_result(acc, kind, ref, Map.get(values, ref), max_bytes)
      end)
    end)
  end

  defp apply_flow_value_result(record, _kind, nil, _value, _max_bytes), do: record
  defp apply_flow_value_result(record, _kind, "", _value, _max_bytes), do: record

  defp apply_flow_value_result(record, kind, ref, value, max_bytes) when is_binary(ref) do
    if byte_size(ref) > Router.max_key_size() do
      Map.put(record, flow_value_error_field(kind), "ERR #{kind}_ref key too large")
    else
      apply_flow_value_result_for_valid_ref(record, kind, ref, value, max_bytes)
    end
  end

  defp apply_flow_value_result(record, _kind, _ref, _other, _max_bytes), do: record

  defp apply_flow_value_result_for_valid_ref(record, kind, _ref, nil, _max_bytes) do
    record
    |> Map.put(kind, nil)
    |> Map.put(flow_value_missing_field(kind), true)
  end

  defp apply_flow_value_result_for_valid_ref(
         record,
         kind,
         _ref,
         {:file_ref, _path, _offset, size},
         _max_bytes
       ) do
    record
    |> Map.put(flow_value_omitted_field(kind), true)
    |> Map.put(flow_value_size_field(kind), flow_value_user_size_from_file_size(size))
  end

  defp apply_flow_value_result_for_valid_ref(record, kind, _ref, encoded_value, max_bytes)
       when is_binary(encoded_value) do
    {decoded, size} = Codec.decode_value_with_user_size(encoded_value)

    if size <= max_bytes do
      record
      |> Map.put(kind, decoded)
      |> Map.put(flow_value_size_field(kind), size)
    else
      record
      |> Map.put(flow_value_omitted_field(kind), true)
      |> Map.put(flow_value_size_field(kind), size)
    end
  end

  defp apply_flow_value_result_for_valid_ref(record, _kind, _ref, _other, _max_bytes), do: record

  defp hydrate_named_value_result({:ok, nil}, _ctx, _names), do: {:ok, nil}

  defp hydrate_named_value_result({:ok, record}, ctx, names) when is_map(record) do
    {:ok, hd(hydrate_named_value_records(ctx, [record], names))}
  end

  defp hydrate_named_value_result(other, _ctx, _names), do: other

  defp hydrate_named_value_records(_ctx, records, nil), do: records
  defp hydrate_named_value_records(_ctx, [], _names), do: []

  defp hydrate_named_value_records(ctx, records, :all) do
    names =
      records
      |> Enum.flat_map(fn record -> Map.keys(Codec.flow_record_value_refs(record)) end)
      |> Enum.uniq()

    hydrate_named_value_records(ctx, records, names)
  end

  defp hydrate_named_value_records(_ctx, records, []), do: records

  defp hydrate_named_value_records(ctx, records, names) when is_list(names) do
    ref_entries =
      records
      |> Enum.with_index()
      |> Enum.flat_map(fn {record, idx} ->
        refs = Codec.flow_record_value_refs(record)

        Enum.flat_map(names, fn name ->
          case Map.get(refs, name) do
            %{ref: ref} when is_binary(ref) and ref != "" -> [{idx, name, ref}]
            %{"ref" => ref} when is_binary(ref) and ref != "" -> [{idx, name, ref}]
            ref when is_binary(ref) and ref != "" -> [{idx, name, ref}]
            _other -> []
          end
        end)
      end)

    fetchable_refs =
      ref_entries
      |> Enum.map(fn {_idx, _name, ref} -> ref end)
      |> Enum.uniq()
      |> Enum.filter(fn ref -> byte_size(ref) <= Router.max_key_size() end)

    values =
      ctx
      |> Ferricstore.Flow.ValueStore.raw_mget(fetchable_refs)
      |> Enum.zip(fetchable_refs)
      |> Map.new(fn {value, ref} -> {ref, value} end)

    values_by_record =
      Enum.reduce(ref_entries, %{}, fn {idx, name, ref}, acc ->
        case Map.get(values, ref) do
          value when is_binary(value) ->
            Map.update(acc, idx, %{name => Codec.decode_value(value)}, fn existing ->
              Map.put(existing, name, Codec.decode_value(value))
            end)

          _other ->
            acc
        end
      end)

    records
    |> Enum.with_index()
    |> Enum.map(fn {record, idx} ->
      case Map.get(values_by_record, idx) do
        values when is_map(values) and map_size(values) > 0 -> Map.put(record, :values, values)
        _other -> record
      end
    end)
  end

  defp flow_value_ref_field(:payload), do: :payload_ref
  defp flow_value_ref_field(:result), do: :result_ref
  defp flow_value_ref_field(:error), do: :error_ref

  defp flow_value_error_field(:payload), do: :payload_error
  defp flow_value_error_field(:result), do: :result_error
  defp flow_value_error_field(:error), do: :error_error

  defp flow_value_missing_field(:payload), do: :payload_missing
  defp flow_value_missing_field(:result), do: :result_missing
  defp flow_value_missing_field(:error), do: :error_missing

  defp flow_value_omitted_field(:payload), do: :payload_omitted
  defp flow_value_omitted_field(:result), do: :result_omitted
  defp flow_value_omitted_field(:error), do: :error_omitted

  defp flow_value_size_field(:payload), do: :payload_size
  defp flow_value_size_field(:result), do: :result_size
  defp flow_value_size_field(:error), do: :error_size

  defp file_ref_payload_threshold(max_bytes) when max_bytes < 1, do: 1

  defp file_ref_payload_threshold(max_bytes) do
    max_bytes + flow_value_codec_overhead_bytes() + 1
  end

  defp flow_value_codec_overhead_bytes, do: byte_size(@value_bin_magic) + 1

  defp flow_value_user_size_from_file_size(size) when is_integer(size) and size >= 0 do
    max(0, size - flow_value_codec_overhead_bytes())
  end

  defp flow_value_user_size_from_file_size(size), do: size

  defp pipeline_read_history_results([], _ctx), do: %{}

  defp pipeline_read_history_results(history_ops, ctx) do
    hot_ops =
      Enum.filter(history_ops, fn
        {_idx, _id, _partition_key, _history_key, _query, false, false, %{enabled?: false}} ->
          true

        _cold_or_consistent ->
          false
      end)

    cold_ops = history_ops -- hot_ops

    cold_results =
      Map.new(cold_ops, fn {idx, id, partition_key, history_key, query, include_cold?,
                            consistent?, value_return} ->
        {idx,
         flow_history_read(
           ctx,
           id,
           partition_key,
           history_key,
           query,
           include_cold?,
           consistent?,
           value_return
         )}
      end)

    hot_results =
      if hot_ops == [] do
        %{}
      else
        pipeline_read_hot_history_results(hot_ops, ctx)
      end

    Map.merge(hot_results, cold_results)
  end

  defp pipeline_read_hot_history_results(history_ops, ctx) do
    requests =
      Enum.map(history_ops, fn {idx, id, partition_key, history_key, query, false, false,
                                value_return} ->
        fetch_count = flow_history_query_fetch_count(query)

        {start_idx, stop_idx} =
          flow_history_hot_range(ctx, id, partition_key, history_key, fetch_count)

        {idx, id, partition_key, history_key, query, start_idx, stop_idx, false, value_return}
      end)

    router_requests =
      Enum.map(requests, fn {_idx, _id, _partition_key, history_key, _query, start_idx, stop_idx,
                             reverse?, _value_return} ->
        {history_key, start_idx, stop_idx, reverse?}
      end)

    case Router.flow_index_rank_range_many(ctx, router_requests) do
      {:ok, rank_results} ->
        history_ops
        |> Enum.zip(rank_results)
        |> Map.new(fn {{idx, id, partition_key, history_key, query, _include_cold?, _consistent?,
                        value_return}, rank_result} ->
          {idx,
           history_result_from_rank(
             ctx,
             id,
             partition_key,
             history_key,
             query,
             rank_result,
             value_return
           )}
        end)

      :unavailable ->
        Map.new(history_ops, fn {idx, _id, _partition_key, history_key, query, _include_cold?,
                                 _consistent?, value_return} ->
          {idx, flow_history_hot_fallback_scan(ctx, history_key, query, value_return)}
        end)
    end
  end

  defp history_result_from_rank(ctx, _id, _partition_key, history_key, query, [], value_return),
    do: flow_history_hot_fallback_scan(ctx, history_key, query, value_return)

  defp history_result_from_rank(
         ctx,
         id,
         partition_key,
         history_key,
         query,
         event_refs,
         value_return
       ) do
    event_ids = Enum.map(event_refs, fn {event_id, _score} -> event_id end)

    with {:ok, events} <-
           flow_history_from_event_ids(
             ctx,
             id,
             partition_key,
             history_key,
             event_ids,
             value_return
           ) do
      {:ok, flow_history_apply_query(events, query)}
    end
  end

  defp history_query_attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, partition_key} <- optional_partition_key(opts),
         history_key = __MODULE__.Keys.history_key(id, partition_key),
         :ok <- validate_key_size(history_key),
         {:ok, count} <- flow_count(opts),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, true),
         {:ok, consistent?} <- optional_boolean(opts, :consistent_projection, true),
         {:ok, value_return} <- history_value_return_opts(opts),
         {:ok, query} <- flow_history_query_opts(opts, count) do
      {:ok, {partition_key, history_key, query, include_cold?, consistent?, value_return}}
    end
  end

  defp fail_many_attrs(items, opts, partition_key) do
    base_opts = Keyword.delete(opts, :partition_key)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, lease_token, item_opts} <- fail_many_item_opts(item),
           {:ok, item_partition_key} <- many_item_partition_key(id, partition_key, item_opts),
           {:ok, attrs} <-
             fail_attrs(
               id,
               lease_token,
               merge_many_item_opts(base_opts, item_opts, item_partition_key)
             ) do
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

  defp cancel_many_attrs(items, opts, partition_key) do
    base_opts = Keyword.delete(opts, :partition_key)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, item_opts} <- cancel_many_item_opts(item),
           {:ok, item_partition_key} <- many_item_partition_key(id, partition_key, item_opts),
           {:ok, attrs} <-
             cancel_attrs(id, merge_many_item_opts(base_opts, item_opts, item_partition_key)) do
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

  defp retry_many_attrs(items, opts, partition_key) do
    base_opts = Keyword.delete(opts, :partition_key)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, lease_token, item_opts} <- retry_many_item_opts(item),
           {:ok, item_partition_key} <- many_item_partition_key(id, partition_key, item_opts),
           {:ok, attrs} <-
             retry_attrs(
               id,
               lease_token,
               merge_many_item_opts(base_opts, item_opts, item_partition_key)
             ) do
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

  defp complete_many_item_opts(
         %{id: id, lease_token: lease_token, fencing_token: fencing_token} = item
       )
       when is_binary(id) do
    {:ok, id, lease_token,
     [fencing_token: fencing_token] ++
       complete_many_item_result_ref(item) ++
       complete_many_item_result(item) ++
       complete_many_item_payload(item) ++ complete_many_item_partition_key(item)}
  end

  defp complete_many_item_opts(
         %{"id" => id, "lease_token" => lease_token, "fencing_token" => fencing_token} = item
       )
       when is_binary(id) do
    {:ok, id, lease_token,
     [fencing_token: fencing_token] ++
       complete_many_item_result_ref(item) ++
       complete_many_item_result(item) ++
       complete_many_item_payload(item) ++ complete_many_item_partition_key(item)}
  end

  defp complete_many_item_opts({id, lease_token, item_opts})
       when is_binary(id) and is_binary(lease_token) and is_list(item_opts) do
    if Keyword.keyword?(item_opts) do
      {:ok, id, lease_token, item_opts}
    else
      {:error, "ERR flow opts must be a keyword list"}
    end
  end

  defp complete_many_item_opts({id, item_opts}) when is_binary(id) and is_list(item_opts) do
    cond do
      not Keyword.keyword?(item_opts) ->
        {:error, "ERR flow opts must be a keyword list"}

      not is_binary(Keyword.get(item_opts, :lease_token)) ->
        {:error, "ERR flow lease_token must be a non-empty string"}

      true ->
        {:ok, id, Keyword.fetch!(item_opts, :lease_token), item_opts}
    end
  end

  defp complete_many_item_opts(
         {:id, id, :lease_token, lease_token, :fencing_token, fencing_token}
       )
       when is_binary(id) do
    {:ok, id, lease_token, [fencing_token: fencing_token]}
  end

  defp complete_many_item_opts(
         {:id, id, :partition_key, partition_key, :lease_token, lease_token, :fencing_token,
          fencing_token}
       )
       when is_binary(id) do
    {:ok, id, lease_token, [partition_key: partition_key, fencing_token: fencing_token]}
  end

  defp complete_many_item_opts(_item), do: {:error, "ERR flow id must be a non-empty string"}

  defp retry_many_item_opts(
         %{id: id, lease_token: lease_token, fencing_token: fencing_token} = item
       )
       when is_binary(id) do
    {:ok, id, lease_token,
     [fencing_token: fencing_token] ++
       retry_many_item_error_ref(item) ++
       retry_many_item_error(item) ++
       retry_many_item_payload(item) ++
       retry_many_item_retry_policy(item) ++ retry_many_item_partition_key(item)}
  end

  defp retry_many_item_opts(
         %{"id" => id, "lease_token" => lease_token, "fencing_token" => fencing_token} = item
       )
       when is_binary(id) do
    {:ok, id, lease_token,
     [fencing_token: fencing_token] ++
       retry_many_item_error_ref(item) ++
       retry_many_item_error(item) ++
       retry_many_item_payload(item) ++
       retry_many_item_retry_policy(item) ++ retry_many_item_partition_key(item)}
  end

  defp retry_many_item_opts({id, lease_token, item_opts})
       when is_binary(id) and is_binary(lease_token) and is_list(item_opts) do
    if Keyword.keyword?(item_opts) do
      {:ok, id, lease_token, item_opts}
    else
      {:error, "ERR flow opts must be a keyword list"}
    end
  end

  defp retry_many_item_opts({id, item_opts}) when is_binary(id) and is_list(item_opts) do
    cond do
      not Keyword.keyword?(item_opts) ->
        {:error, "ERR flow opts must be a keyword list"}

      not is_binary(Keyword.get(item_opts, :lease_token)) ->
        {:error, "ERR flow lease_token must be a non-empty string"}

      true ->
        {:ok, id, Keyword.fetch!(item_opts, :lease_token), item_opts}
    end
  end

  defp retry_many_item_opts({:id, id, :lease_token, lease_token, :fencing_token, fencing_token})
       when is_binary(id) do
    {:ok, id, lease_token, [fencing_token: fencing_token]}
  end

  defp retry_many_item_opts(
         {:id, id, :partition_key, partition_key, :lease_token, lease_token, :fencing_token,
          fencing_token}
       )
       when is_binary(id) do
    {:ok, id, lease_token, [partition_key: partition_key, fencing_token: fencing_token]}
  end

  defp retry_many_item_opts(_item), do: {:error, "ERR flow id must be a non-empty string"}

  defp fail_many_item_opts(
         %{id: id, lease_token: lease_token, fencing_token: fencing_token} = item
       )
       when is_binary(id) do
    {:ok, id, lease_token,
     [fencing_token: fencing_token] ++
       fail_many_item_error_ref(item) ++
       fail_many_item_error(item) ++
       fail_many_item_payload(item) ++ fail_many_item_partition_key(item)}
  end

  defp fail_many_item_opts(
         %{"id" => id, "lease_token" => lease_token, "fencing_token" => fencing_token} = item
       )
       when is_binary(id) do
    {:ok, id, lease_token,
     [fencing_token: fencing_token] ++
       fail_many_item_error_ref(item) ++
       fail_many_item_error(item) ++
       fail_many_item_payload(item) ++ fail_many_item_partition_key(item)}
  end

  defp fail_many_item_opts({id, lease_token, item_opts})
       when is_binary(id) and is_binary(lease_token) and is_list(item_opts) do
    if Keyword.keyword?(item_opts) do
      {:ok, id, lease_token, item_opts}
    else
      {:error, "ERR flow opts must be a keyword list"}
    end
  end

  defp fail_many_item_opts({id, item_opts}) when is_binary(id) and is_list(item_opts) do
    cond do
      not Keyword.keyword?(item_opts) ->
        {:error, "ERR flow opts must be a keyword list"}

      not is_binary(Keyword.get(item_opts, :lease_token)) ->
        {:error, "ERR flow lease_token must be a non-empty string"}

      true ->
        {:ok, id, Keyword.fetch!(item_opts, :lease_token), item_opts}
    end
  end

  defp fail_many_item_opts({:id, id, :lease_token, lease_token, :fencing_token, fencing_token})
       when is_binary(id) do
    {:ok, id, lease_token, [fencing_token: fencing_token]}
  end

  defp fail_many_item_opts(
         {:id, id, :partition_key, partition_key, :lease_token, lease_token, :fencing_token,
          fencing_token}
       )
       when is_binary(id) do
    {:ok, id, lease_token, [partition_key: partition_key, fencing_token: fencing_token]}
  end

  defp fail_many_item_opts(_item), do: {:error, "ERR flow id must be a non-empty string"}

  defp cancel_many_item_opts(%{id: id, fencing_token: fencing_token} = item)
       when is_binary(id) do
    {:ok, id,
     [fencing_token: fencing_token] ++
       cancel_many_item_lease_token(item) ++
       cancel_many_item_reason_ref(item) ++
       cancel_many_item_reason(item) ++ cancel_many_item_partition_key(item)}
  end

  defp cancel_many_item_opts(%{"id" => id, "fencing_token" => fencing_token} = item)
       when is_binary(id) do
    {:ok, id,
     [fencing_token: fencing_token] ++
       cancel_many_item_lease_token(item) ++
       cancel_many_item_reason_ref(item) ++
       cancel_many_item_reason(item) ++ cancel_many_item_partition_key(item)}
  end

  defp cancel_many_item_opts({id, item_opts}) when is_binary(id) and is_list(item_opts) do
    if Keyword.keyword?(item_opts) do
      {:ok, id, item_opts}
    else
      {:error, "ERR flow opts must be a keyword list"}
    end
  end

  defp cancel_many_item_opts({:id, id, :fencing_token, fencing_token}) when is_binary(id) do
    {:ok, id, [fencing_token: fencing_token]}
  end

  defp cancel_many_item_opts(
         {:id, id, :partition_key, partition_key, :fencing_token, fencing_token}
       )
       when is_binary(id) do
    {:ok, id, [partition_key: partition_key, fencing_token: fencing_token]}
  end

  defp cancel_many_item_opts(_item), do: {:error, "ERR flow id must be a non-empty string"}

  defp transition_many_attrs(items, opts, partition_key, from_state, to_state) do
    base_opts = Keyword.delete(opts, :partition_key)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, item_opts} <- transition_many_item_opts(item),
           {:ok, item_partition_key} <- many_item_partition_key(id, partition_key, item_opts),
           {:ok, attrs} <-
             transition_attrs(
               id,
               from_state,
               to_state,
               merge_many_item_opts(base_opts, item_opts, item_partition_key)
             ) do
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

  defp transition_many_item_opts(%{id: id, fencing_token: fencing_token} = item)
       when is_binary(id) do
    {:ok, id,
     [fencing_token: fencing_token] ++
       transition_many_item_payload(item) ++
       transition_many_item_lease_token(item) ++ transition_many_item_partition_key(item)}
  end

  defp transition_many_item_opts(%{"id" => id, "fencing_token" => fencing_token} = item)
       when is_binary(id) do
    {:ok, id,
     [fencing_token: fencing_token] ++
       transition_many_item_payload(item) ++
       transition_many_item_lease_token(item) ++ transition_many_item_partition_key(item)}
  end

  defp transition_many_item_opts({id, item_opts}) when is_binary(id) and is_list(item_opts) do
    if Keyword.keyword?(item_opts) do
      {:ok, id, item_opts}
    else
      {:error, "ERR flow opts must be a keyword list"}
    end
  end

  defp transition_many_item_opts(
         {:id, id, :fencing_token, fencing_token, :lease_token, lease_token}
       )
       when is_binary(id) do
    opts =
      if is_nil(lease_token),
        do: [fencing_token: fencing_token],
        else: [fencing_token: fencing_token, lease_token: lease_token]

    {:ok, id, opts}
  end

  defp transition_many_item_opts(
         {:id, id, :partition_key, partition_key, :fencing_token, fencing_token, :lease_token,
          lease_token}
       )
       when is_binary(id) do
    opts =
      if is_nil(lease_token),
        do: [partition_key: partition_key, fencing_token: fencing_token],
        else: [
          partition_key: partition_key,
          fencing_token: fencing_token,
          lease_token: lease_token
        ]

    {:ok, id, opts}
  end

  defp transition_many_item_opts(_item), do: {:error, "ERR flow id must be a non-empty string"}

  defp merge_many_item_opts(base_opts, [], partition_key) do
    Keyword.put(base_opts, :partition_key, partition_key)
  end

  defp merge_many_item_opts(base_opts, item_opts, partition_key) do
    base_opts
    |> Keyword.merge(Keyword.delete(item_opts, :partition_key))
    |> Keyword.put(:partition_key, partition_key)
  end

  defp transition_many_item_lease_token(item) do
    cond do
      Map.has_key?(item, :lease_token) -> [lease_token: Map.get(item, :lease_token)]
      Map.has_key?(item, "lease_token") -> [lease_token: Map.get(item, "lease_token")]
      true -> []
    end
  end

  defp transition_many_item_partition_key(item) do
    []
    |> maybe_put_item_opt(:partition_key, item, :partition_key, "partition_key")
  end

  defp transition_many_item_payload(item) do
    []
    |> maybe_put_item_opt(:payload, item, :payload, "payload")
    |> maybe_put_item_opt(:payload_ref, item, :payload_ref, "payload_ref")
    |> maybe_put_item_opt(:values, item, :values, "values")
    |> maybe_put_item_opt(:value_refs, item, :value_refs, "value_refs")
    |> maybe_put_item_opt(:drop_values, item, :drop_values, "drop_values")
    |> maybe_put_item_opt(:override_values, item, :override_values, "override_values")
  end

  defp complete_many_item_result_ref(item) do
    []
    |> maybe_put_item_opt(:result_ref, item, :result_ref, "result_ref")
  end

  defp complete_many_item_result(item) do
    []
    |> maybe_put_item_opt(:result, item, :result, "result")
  end

  defp complete_many_item_payload(item) do
    []
    |> maybe_put_item_opt(:payload, item, :payload, "payload")
    |> maybe_put_item_opt(:values, item, :values, "values")
    |> maybe_put_item_opt(:value_refs, item, :value_refs, "value_refs")
    |> maybe_put_item_opt(:drop_values, item, :drop_values, "drop_values")
    |> maybe_put_item_opt(:override_values, item, :override_values, "override_values")
  end

  defp complete_many_item_partition_key(item) do
    []
    |> maybe_put_item_opt(:partition_key, item, :partition_key, "partition_key")
  end

  defp retry_many_item_error_ref(item) do
    []
    |> maybe_put_item_opt(:error_ref, item, :error_ref, "error_ref")
  end

  defp retry_many_item_error(item) do
    []
    |> maybe_put_item_opt(:error, item, :error, "error")
  end

  defp retry_many_item_payload(item) do
    []
    |> maybe_put_item_opt(:payload, item, :payload, "payload")
    |> maybe_put_item_opt(:values, item, :values, "values")
    |> maybe_put_item_opt(:value_refs, item, :value_refs, "value_refs")
    |> maybe_put_item_opt(:drop_values, item, :drop_values, "drop_values")
    |> maybe_put_item_opt(:override_values, item, :override_values, "override_values")
  end

  defp retry_many_item_retry_policy(item) do
    []
    |> maybe_put_item_opt(:retry, item, :retry, "retry")
  end

  defp retry_many_item_partition_key(item) do
    []
    |> maybe_put_item_opt(:partition_key, item, :partition_key, "partition_key")
  end

  defp fail_many_item_error_ref(item) do
    []
    |> maybe_put_item_opt(:error_ref, item, :error_ref, "error_ref")
  end

  defp fail_many_item_error(item) do
    []
    |> maybe_put_item_opt(:error, item, :error, "error")
  end

  defp fail_many_item_payload(item) do
    []
    |> maybe_put_item_opt(:payload, item, :payload, "payload")
    |> maybe_put_item_opt(:values, item, :values, "values")
    |> maybe_put_item_opt(:value_refs, item, :value_refs, "value_refs")
    |> maybe_put_item_opt(:drop_values, item, :drop_values, "drop_values")
    |> maybe_put_item_opt(:override_values, item, :override_values, "override_values")
  end

  defp fail_many_item_partition_key(item) do
    []
    |> maybe_put_item_opt(:partition_key, item, :partition_key, "partition_key")
  end

  defp cancel_many_item_lease_token(item) do
    cond do
      Map.has_key?(item, :lease_token) -> [lease_token: Map.get(item, :lease_token)]
      Map.has_key?(item, "lease_token") -> [lease_token: Map.get(item, "lease_token")]
      true -> []
    end
  end

  defp cancel_many_item_reason_ref(item) do
    []
    |> maybe_put_item_opt(:reason_ref, item, :reason_ref, "reason_ref")
  end

  defp cancel_many_item_reason(item) do
    []
    |> maybe_put_item_opt(:reason, item, :reason, "reason")
    |> maybe_put_item_opt(:values, item, :values, "values")
    |> maybe_put_item_opt(:value_refs, item, :value_refs, "value_refs")
    |> maybe_put_item_opt(:drop_values, item, :drop_values, "drop_values")
    |> maybe_put_item_opt(:override_values, item, :override_values, "override_values")
  end

  defp cancel_many_item_partition_key(item) do
    []
    |> maybe_put_item_opt(:partition_key, item, :partition_key, "partition_key")
  end

  defp maybe_put_item_opt(opts, opt_key, item, atom_key, string_key) do
    cond do
      Map.has_key?(item, atom_key) -> Keyword.put(opts, opt_key, Map.get(item, atom_key))
      Map.has_key?(item, string_key) -> Keyword.put(opts, opt_key, Map.get(item, string_key))
      true -> opts
    end
  end

  defp many_item_partition_key(
         id,
         partition_key,
         item_opts,
         mismatch_error \\ "ERR flow partition_key mismatch in batch"
       )

  defp many_item_partition_key(id, nil, item_opts, _mismatch_error) do
    case optional_partition_key(partition_key: Keyword.get(item_opts, :partition_key)) do
      {:ok, nil} when is_binary(id) -> {:ok, __MODULE__.Keys.auto_partition_key(id)}
      other -> other
    end
  end

  defp many_item_partition_key(_id, partition_key, item_opts, :allow_override)
       when is_binary(partition_key) do
    item_opts
    |> Keyword.get(:partition_key, partition_key)
    |> required_partition_key()
  end

  defp many_item_partition_key(_id, partition_key, item_opts, mismatch_error)
       when is_binary(partition_key) do
    case Keyword.fetch(item_opts, :partition_key) do
      {:ok, item_partition_key} ->
        case required_partition_key(item_partition_key) do
          {:ok, ^partition_key} -> {:ok, partition_key}
          {:ok, _other} -> {:error, mismatch_error}
          {:error, _reason} = error -> error
        end

      :error ->
        {:ok, partition_key}
    end
  end

  defp validate_create_many_items([_ | _] = items), do: validate_many_item_count(items)
  defp validate_create_many_items(_items), do: {:error, "ERR flow items must be a non-empty list"}

  defp validate_transition_many_items([_ | _] = items), do: validate_many_item_count(items)

  defp validate_transition_many_items(_items),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp validate_complete_many_items([_ | _] = items), do: validate_many_item_count(items)

  defp validate_complete_many_items(_items),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp validate_retry_many_items([_ | _] = items), do: validate_many_item_count(items)

  defp validate_retry_many_items(_items),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp validate_fail_many_items([_ | _] = items), do: validate_many_item_count(items)

  defp validate_fail_many_items(_items),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp validate_cancel_many_items([_ | _] = items), do: validate_many_item_count(items)

  defp validate_cancel_many_items(_items),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp validate_many_item_count(items) do
    max = flow_max_batch_items()

    if length(items) <= max do
      :ok
    else
      {:error, "ERR flow batch item count exceeds maximum #{max}"}
    end
  end

  defp flow_max_batch_items do
    case Application.get_env(:ferricstore, :flow_max_batch_items, @default_max_batch_items) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_max_batch_items
    end
  end

  defp validate_unique_create_ids(attrs_list) do
    {_seen, result} =
      Enum.reduce_while(attrs_list, {MapSet.new(), :ok}, fn %{id: id}, {seen, :ok} ->
        if MapSet.member?(seen, id) do
          {:halt, {seen, {:error, "ERR flow duplicate id in batch"}}}
        else
          {:cont, {MapSet.put(seen, id), :ok}}
        end
      end)

    result
  end

  defp validate_unique_transition_ids(attrs_list) do
    {_seen, result} =
      Enum.reduce_while(attrs_list, {MapSet.new(), :ok}, fn %{id: id}, {seen, :ok} ->
        if MapSet.member?(seen, id) do
          {:halt, {seen, {:error, "ERR flow duplicate id in batch"}}}
        else
          {:cont, {MapSet.put(seen, id), :ok}}
        end
      end)

    result
  end

  defp required_binary(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      {:ok, _} -> {:error, "ERR flow #{key} must be a non-empty string"}
      :error -> {:error, "ERR flow #{key} is required"}
    end
  end

  defp optional_binary(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a non-empty string"}
    end
  end

  defp optional_binary_or_nil(opts, key, default) do
    case Keyword.get(opts, key, default) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a string"}
    end
  end

  defp validate_ref_size(_key, nil), do: :ok

  defp validate_ref_size(key, value) when is_binary(value) do
    if byte_size(value) <= @max_ref_size do
      :ok
    else
      {:error, "ERR flow #{key} too large (max #{@max_ref_size} bytes)"}
    end
  end

  defp optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a boolean"}
    end
  end

  defp required_non_neg_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      {:ok, _} -> {:error, "ERR flow #{key} must be a non-negative integer"}
      :error -> {:error, "ERR flow #{key} is required"}
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
    optional_non_neg_integer(opts, :block_ms, 0)
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

  defp history_value_return_opts(opts) do
    with {:ok, enabled?} <- optional_boolean(opts, :values, false),
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

  defp optional_non_neg_integer_or_nil(opts, key) do
    case Keyword.get(opts, key, nil) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a non-negative integer"}
    end
  end

  defp optional_pos_integer_or_nil(opts, key) do
    case Keyword.get(opts, key, nil) do
      nil -> {:ok, nil}
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a positive integer"}
    end
  end

  defp optional_retry_policy(opts) do
    if Keyword.has_key?(opts, :retry) do
      opts
      |> Keyword.get(:retry)
      |> RetryPolicy.normalize_override()
    else
      {:ok, nil}
    end
  end

  defp optional_retention_ttl_ms(opts) do
    cond do
      Keyword.has_key?(opts, :ttl_ms) ->
        {:error, "ERR flow ttl_ms was renamed to retention_ttl_ms"}

      Keyword.has_key?(opts, :retention_ttl_ms) ->
        case Keyword.get(opts, :retention_ttl_ms) do
          value when is_integer(value) and value > 0 ->
            {:ok, value}

          _ ->
            {:error, "ERR flow retention_ttl_ms must be a positive integer"}
        end

      true ->
        {:ok, nil}
    end
  end

  defp optional_history_hot_max_events(opts) do
    if Keyword.has_key?(opts, :history_hot_max_events) do
      case Keyword.get(opts, :history_hot_max_events) do
        value when is_integer(value) and value >= 0 ->
          max = flow_max_history_hot_max_events()

          if value <= max do
            {:ok, value}
          else
            {:error, "ERR flow history_hot_max_events exceeds maximum #{max}"}
          end

        _ ->
          {:error, "ERR flow history_hot_max_events must be a non-negative integer"}
      end
    else
      {:ok, nil}
    end
  end

  defp optional_history_max_events(opts) do
    if Keyword.has_key?(opts, :history_max_events) do
      case Keyword.get(opts, :history_max_events) do
        value when is_integer(value) and value > 0 ->
          max = flow_max_history_max_events()

          if value <= max do
            {:ok, value}
          else
            {:error, "ERR flow history_max_events exceeds maximum #{max}"}
          end

        _ ->
          {:error, "ERR flow history_max_events must be a positive integer"}
      end
    else
      {:ok, nil}
    end
  end

  defp validate_history_event_caps(nil, _history_max_events), do: :ok
  defp validate_history_event_caps(_history_hot_max_events, nil), do: :ok

  defp validate_history_event_caps(history_hot_max_events, history_max_events)
       when is_integer(history_hot_max_events) and is_integer(history_max_events) do
    if history_max_events >= history_hot_max_events do
      :ok
    else
      {:error,
       "ERR flow history_max_events must be greater than or equal to history_hot_max_events"}
    end
  end

  defp flow_max_history_hot_max_events do
    case Application.get_env(
           :ferricstore,
           :flow_max_history_hot_max_events,
           @max_history_hot_max_events
         ) do
      value when is_integer(value) and value > 0 -> value
      _ -> @max_history_hot_max_events
    end
  end

  defp flow_max_history_max_events do
    case Application.get_env(
           :ferricstore,
           :flow_max_history_max_events,
           @max_history_max_events
         ) do
      value when is_integer(value) and value > 0 -> min(value, @max_history_max_events)
      _ -> @max_history_max_events
    end
  end

  defp optional_priority(opts, default) do
    case Keyword.get(opts, :priority, default) do
      value when is_integer(value) and value >= 0 and value <= @max_priority -> {:ok, value}
      _ -> {:error, "ERR flow priority must be between 0 and #{@max_priority}"}
    end
  end

  defp optional_priority_or_nil(opts) do
    case Keyword.get(opts, :priority, nil) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 and value <= @max_priority -> {:ok, value}
      _ -> {:error, "ERR flow priority must be between 0 and #{@max_priority}"}
    end
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
    state_size = max_binary_size(states)
    tag_size = max_partition_tag_size(partition_keys)
    priority_size = priority_key_size(priority)

    validate_generated_key_size(
      due_key_size(type, state_size, priority_size, tag_size),
      max_key_size
    )
  end

  defp validate_claim_due_key_lengths(type, state, priority, partition_keys, max_key_size)
       when is_binary(state) and is_list(partition_keys) do
    tag_size = max_partition_tag_size(partition_keys)
    priority_size = priority_key_size(priority)

    validate_generated_key_size(
      due_key_size(type, byte_size(state), priority_size, tag_size),
      max_key_size
    )
  end

  defp validate_claim_due_key_lengths(type, states, priority, partition_key, max_key_size)
       when is_list(states) do
    state_size = max_binary_size(states)
    tag_size = partition_tag_size(partition_key)
    priority_size = priority_key_size(priority)

    validate_generated_key_size(
      due_key_size(type, state_size, priority_size, tag_size),
      max_key_size
    )
  end

  defp validate_claim_due_key_lengths(type, state, priority, partition_key, max_key_size) do
    tag_size = partition_tag_size(partition_key)
    priority_size = priority_key_size(priority)

    validate_generated_key_size(
      due_key_size(type, byte_size(state), priority_size, tag_size),
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

  defp integer_decimal_size(value),
    do: value |> Integer.to_string() |> byte_size()

  defp max_binary_size([head | tail]) do
    Enum.reduce(tail, byte_size(head), fn value, max_size ->
      max(max_size, byte_size(value))
    end)
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
    do: partition_key |> __MODULE__.Keys.tag() |> byte_size()

  defp validate_generated_key_size(size, max_key_size) when size <= max_key_size, do: :ok

  defp validate_generated_key_size(_size, max_key_size),
    do: {:error, "ERR key too large (max #{max_key_size} bytes)"}

  defp optional_pos_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a positive integer"}
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

  defp optional_auto_partition_key(opts) do
    case Keyword.fetch(opts, :partition_key) do
      :error -> {:ok, :auto}
      {:ok, :auto} -> {:ok, :auto}
      {:ok, "AUTO"} -> {:ok, :auto}
      {:ok, "auto"} -> {:ok, :auto}
      {:ok, _value} -> optional_partition_key(opts)
    end
  end

  defp optional_claim_partition_key(opts) do
    case Keyword.get(opts, :partition_key, nil) do
      nil ->
        {:ok, :auto}

      :any ->
        {:ok, :any}

      :auto ->
        {:ok, :auto}

      :global ->
        {:ok, nil}

      value when is_binary(value) and value != "" ->
        case String.upcase(value) do
          "ANY" -> {:ok, :any}
          "AUTO" -> {:ok, :auto}
          "GLOBAL" -> {:ok, nil}
          _ -> {:ok, value}
        end

      _ ->
        optional_partition_key(opts)
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

      value
      when value in [
             :jobs_compact_state,
             :job_compact_state,
             :jobs_compact_with_state,
             :job_compact_with_state
           ] ->
        {:ok, :jobs_compact_state}

      value when is_binary(value) ->
        case String.upcase(value) do
          "RECORDS" -> {:ok, :records}
          "RECORD" -> {:ok, :records}
          "FULL" -> {:ok, :records}
          "JOBS" -> {:ok, :jobs}
          "JOB" -> {:ok, :jobs}
          "JOBS_COMPACT" -> {:ok, :jobs_compact}
          "JOB_COMPACT" -> {:ok, :jobs_compact}
          "JOBS_COMPACT_STATE" -> {:ok, :jobs_compact_state}
          "JOB_COMPACT_STATE" -> {:ok, :jobs_compact_state}
          "JOBS_COMPACT_WITH_STATE" -> {:ok, :jobs_compact_state}
          "JOB_COMPACT_WITH_STATE" -> {:ok, :jobs_compact_state}
          _ -> {:error, "ERR flow claim return must be records, jobs, or jobs_compact"}
        end

      _ ->
        {:error,
         "ERR flow claim return must be records, jobs, jobs_compact, or jobs_compact_state"}
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
    cond do
      values == [] ->
        {:error, "ERR flow states must be a non-empty list"}

      true ->
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
        claim_state_any?(value) ->
          {:cont, {:ok, true, acc}}

        is_binary(value) and value != "" ->
          {:cont, {:ok, any?, [value | acc]}}

        true ->
          {:halt, {:error, "ERR flow state must be a non-empty string"}}
      end
    end)
    |> case do
      {:ok, true, []} ->
        {:ok, :any}

      {:ok, true, _states} ->
        {:error, "ERR flow STATE ANY cannot be mixed with explicit states"}

      {:ok, false, states} ->
        case dedupe_claim_states_keep_last(states) do
          [single] -> {:ok, single}
          deduped -> {:ok, deduped}
        end

      {:error, _reason} = error ->
        error
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

  defp required_partition_key(partition_key) do
    case optional_partition_key(partition_key: partition_key) do
      {:ok, nil} -> {:error, "ERR flow partition_key is required"}
      {:ok, value} -> {:ok, value}
      {:error, _reason} = error -> error
    end
  end

  defp maybe_put_attr(attrs, _key, nil), do: attrs
  defp maybe_put_attr(attrs, key, value), do: Map.put(attrs, key, value)

  defp maybe_put_default_attr(attrs, _key, value, value), do: attrs
  defp maybe_put_default_attr(attrs, key, value, _default), do: Map.put(attrs, key, value)

  defp maybe_put_flow_value(attrs, opts, key) do
    if Keyword.has_key?(opts, key) do
      Map.put(attrs, key, Keyword.fetch!(opts, key))
    else
      attrs
    end
  end

  defp maybe_put_flow_value_ref(attrs, opts, key) do
    if Keyword.has_key?(opts, key) do
      Map.put(attrs, key, Keyword.fetch!(opts, key))
    else
      attrs
    end
  end

  defp maybe_put_named_value_opts(attrs, opts) do
    attrs
    |> maybe_put_flow_value_ref(opts, :values)
    |> maybe_put_flow_value_ref(opts, :value_refs)
    |> maybe_put_flow_value_ref(opts, :drop_values)
    |> maybe_put_flow_value_ref(opts, :override_values)
  end

  defp flow_policy_read(ctx, type) do
    case Stats.with_cache_tracking_disabled(fn ->
           Router.get(ctx, __MODULE__.Keys.policy_key(type))
         end) do
      nil ->
        {:ok, nil}

      value when is_binary(value) ->
        case RetryPolicy.decode_flow_policy(value) do
          {:ok, policy} -> {:ok, policy}
          :error -> {:error, "ERR flow policy is corrupt"}
        end

      _other ->
        {:error, "ERR flow policy is corrupt"}
    end
  end

  defp policy_response(type, policy, nil) do
    states = Map.get(policy || %{}, :states, %{})

    %{
      type: type,
      retry: RetryPolicy.resolve(policy, nil, nil),
      retention: policy_response_retention(policy, nil),
      states:
        Map.new(states, fn {state, _state_policy} ->
          {state,
           %{
             retry: RetryPolicy.resolve(policy, state, nil),
             retention: policy_response_retention(policy, state)
           }}
        end)
    }
  end

  defp policy_response(type, policy, state) when is_binary(state) do
    %{
      type: type,
      state: state,
      retry: RetryPolicy.resolve(policy, state, nil),
      retention: policy_response_retention(policy, state)
    }
  end

  defp policy_response_retention(policy, state) do
    policy
    |> RetryPolicy.resolve_retention(state, nil)
    |> Map.delete(:history_hot_max_events)
  end

  defp now_ms, do: CommandTime.now_ms()

end
