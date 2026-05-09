defmodule Ferricstore.Flow do
  @moduledoc false

  import Bitwise

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.RetryPolicy
  alias Ferricstore.Store.Router

  @default_state "queued"
  @default_priority 0
  @max_priority 2
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
  @terminal_states ["completed", "failed", "cancelled"]
  @history_tag :flow_history_v1
  @value_bin_magic "FSV1"

  # Flow records and history are durable bytes. Before Flow is public, keep one
  # current compact schema and change it directly. Once users can have persisted
  # Flow data, incompatible field-order/type changes need explicit migration.
  @record_bin_magic "FSF4"
  @history_bin_magic "FSH1"

  def create(ctx, id, opts) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- create_attrs(id, opts) do
        Router.flow_create(ctx, attrs)
      end

    observe_flow(:create, started, result, %{flow_id: id})
  end

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
    valid_results = Router.flow_create_batch(ctx, Enum.map(valid, fn {_idx, attrs} -> attrs end))

    indexed_results =
      valid
      |> Enum.map(fn {idx, _attrs} -> idx end)
      |> Enum.zip(valid_results)
      |> Enum.reduce(indexed_results, fn {idx, result}, acc -> Map.put(acc, idx, result) end)

    results = for idx <- 0..(length(creates) - 1), do: Map.fetch!(indexed_results, idx)
    observe_flow_batch(:create, started, results)
    results
  end

  def create_batch_independent(_ctx, _creates),
    do: [{:error, "ERR flow opts must be a keyword list"}]

  def create_many(ctx, partition_key, items, opts)
      when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_create_many_items(items),
           {:ok, attrs_list} <- create_many_attrs(items, opts, partition_key),
           :ok <- validate_unique_create_ids(attrs_list) do
        Router.flow_create_many(ctx, partition_key, attrs_list)
      end

    observe_flow(:create, started, result, %{flow_id: nil})
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

    observe_flow(:spawn_children, started, result, %{flow_id: parent_id})
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
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)) do
      case Router.flow_get(ctx, id, partition_key) do
        nil ->
          {:ok, nil}

        value when is_binary(value) ->
          hydrate_payload_result(ctx, safe_decode_record(value), payload_return)

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
      case Router.put(
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
    result = claim_due_result(ctx, type, opts)

    observe_flow(:claim_due, started, result, %{flow_type: type})
  end

  def reclaim(ctx, type, opts) when is_binary(type) and is_list(opts) do
    started = flow_start_time()

    result =
      opts
      |> Keyword.put(:state, "running")
      |> Keyword.put(:reclaim_expired, false)
      |> then(&claim_due_result(ctx, type, &1))

    observe_flow(:reclaim, started, result, %{flow_type: type})
  end

  defp claim_due_result(ctx, type, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, state} <- optional_claim_states(opts),
         {:ok, worker} <- required_binary(opts, :worker),
         {:ok, lease_ms} <- optional_pos_integer(opts, :lease_ms, @default_lease_ms),
         {:ok, limit} <- optional_claim_limit(opts),
         {:ok, priority} <- optional_priority_or_nil(opts),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, payload_return} <- payload_return_opts(opts, true),
         {:ok, reclaim_expired?} <- optional_boolean(opts, :reclaim_expired, true),
         {:ok, reclaim_ratio} <- optional_reclaim_ratio(opts),
         {:ok, partition_key} <- optional_claim_partition_key(opts),
         :ok <- validate_claim_due_keys(type, state, priority, partition_key) do
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
        |> maybe_put_attr(:now_ms, now)

      case claim_due_router_result(ctx, attrs, reclaim_expired?, reclaim_ratio) do
        {:ok, records} when is_list(records) ->
          {:ok, hydrate_payload_records(ctx, records, payload_return)}

        other ->
          other
      end
    end
  end

  defp claim_due_router_result(ctx, %{state: "running"} = attrs, _reclaim_expired?, _ratio) do
    Router.flow_claim_due(ctx, attrs)
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

    observe_flow(:extend_lease, started, result, %{flow_id: id})
  end

  def complete(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- complete_attrs(id, lease_token, opts) do
        Router.flow_complete(ctx, attrs)
      end

    observe_flow(:complete, started, result, %{flow_id: id})
  end

  def complete_many(ctx, partition_key, items, opts)
      when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_complete_many_items(items),
           {:ok, attrs_list} <- complete_many_attrs(items, opts, partition_key),
           :ok <- validate_unique_transition_ids(attrs_list) do
        Router.flow_complete_many(ctx, partition_key, attrs_list)
      end

    observe_flow(:complete, started, result, %{flow_id: nil})
  end

  def complete_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def transition(ctx, id, from_state, to_state, opts \\ [])
      when is_binary(id) and is_binary(from_state) and is_binary(to_state) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- transition_attrs(id, from_state, to_state, opts) do
        Router.flow_transition(ctx, attrs)
      end

    observe_flow(:transition, started, result, %{
      flow_id: id,
      from_state: from_state,
      to_state: to_state
    })
  end

  def transition_many(ctx, partition_key, from_state, to_state, items, opts)
      when is_binary(from_state) and is_binary(to_state) and is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_transition_many_items(items),
           {:ok, attrs_list} <-
             transition_many_attrs(items, opts, partition_key, from_state, to_state),
           :ok <- validate_unique_transition_ids(attrs_list) do
        Router.flow_transition_many(ctx, partition_key, attrs_list)
      end

    observe_flow(:transition, started, result, %{
      flow_id: nil,
      from_state: from_state,
      to_state: to_state
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
    observe_flow_batch(:transition, started, results)
    results
  end

  def transition_batch_independent(_ctx, _transitions),
    do: [{:error, "ERR flow opts must be a keyword list"}]

  @doc false
  def pipeline_write_batch_independent(_ctx, []), do: []

  def pipeline_write_batch_independent(ctx, ops) when is_list(ops) do
    started = flow_start_time()

    {valid, indexed_results} =
      ops
      |> Enum.with_index()
      |> Enum.reduce({[], %{}}, fn {op, idx}, {valid_acc, result_acc} ->
        case pipeline_write_command(op) do
          {:ok, keyed_command} -> {[{idx, keyed_command} | valid_acc], result_acc}
          {:error, _reason} = error -> {valid_acc, Map.put(result_acc, idx, error)}
        end
      end)

    valid = Enum.reverse(valid)

    valid_results =
      valid
      |> Enum.map(fn {_idx, keyed_command} -> keyed_command end)
      |> then(&Router.flow_command_batch(ctx, &1))

    indexed_results =
      valid
      |> Enum.map(fn {idx, _keyed_command} -> idx end)
      |> Enum.zip(valid_results)
      |> Enum.reduce(indexed_results, fn {idx, result}, acc -> Map.put(acc, idx, result) end)

    results = for idx <- 0..(length(ops) - 1), do: Map.fetch!(indexed_results, idx)
    observe_flow_batch(:pipeline_write, started, results)
    results
  end

  def pipeline_write_batch_independent(_ctx, _ops),
    do: [{:error, "ERR flow opts must be a keyword list"}]

  @doc false
  def pipeline_claim_due_batch(_ctx, []), do: []

  def pipeline_claim_due_batch(ctx, ops) when is_list(ops) do
    started = flow_start_time()

    {results, stats} =
      ops
      |> Enum.map(&pipeline_claim_due_command/1)
      |> pipeline_claim_due_results(ctx, [], %{groups: 0, coalesced_calls: 0})

    :telemetry.execute(
      [:ferricstore, :flow, :pipeline_claim_due_batch],
      Map.merge(stats, %{commands: length(ops), duration_us: elapsed_us(started)}),
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

          {:history, id, partition_key, history_key, count, include_cold?, consistent?,
           value_return} ->
            {get_acc,
             [
               {idx, id, partition_key, history_key, count, include_cold?, consistent?,
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

    observe_pipeline_read_batch(started, ops)
    results
  end

  def pipeline_read_batch(_ctx, _ops),
    do: [{:error, "ERR flow opts must be a keyword list"}]

  def retry(ctx, id, lease_token, opts)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- retry_attrs(id, lease_token, opts) do
        Router.flow_retry(ctx, attrs)
      end

    observe_flow(:retry, started, result, %{flow_id: id})
  end

  def retry_many(ctx, partition_key, items, opts) when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_retry_many_items(items),
           {:ok, attrs_list} <- retry_many_attrs(items, opts, partition_key),
           :ok <- validate_unique_transition_ids(attrs_list) do
        Router.flow_retry_many(ctx, partition_key, attrs_list)
      end

    observe_flow(:retry, started, result, %{flow_id: nil})
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

    observe_flow(:fail, started, result, %{flow_id: id})
  end

  def fail_many(ctx, partition_key, items, opts) when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_fail_many_items(items),
           {:ok, attrs_list} <- fail_many_attrs(items, opts, partition_key),
           :ok <- validate_unique_transition_ids(attrs_list) do
        Router.flow_fail_many(ctx, partition_key, attrs_list)
      end

    observe_flow(:fail, started, result, %{flow_id: nil})
  end

  def fail_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def cancel(ctx, id, opts \\ []) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- cancel_attrs(id, opts) do
        Router.flow_cancel(ctx, attrs)
      end

    observe_flow(:cancel, started, result, %{flow_id: id})
  end

  def cancel_many(ctx, partition_key, items, opts) when is_list(items) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, partition_key} <- optional_partition_key(partition_key: partition_key),
           :ok <- validate_cancel_many_items(items),
           {:ok, attrs_list} <- cancel_many_attrs(items, opts, partition_key),
           :ok <- validate_unique_transition_ids(attrs_list) do
        Router.flow_cancel_many(ctx, partition_key, attrs_list)
      end

    observe_flow(:cancel, started, result, %{flow_id: nil})
  end

  def cancel_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def retention_cleanup(ctx, opts \\ [])

  def retention_cleanup(ctx, opts) when is_list(opts) do
    started = flow_start_time()

    result =
      with :ok <- validate_opts(opts),
           {:ok, limit} <- optional_pos_integer(opts, :limit, 100),
           {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()) do
        Router.flow_retention_cleanup(ctx, %{limit: limit, now_ms: now})
      end

    observe_flow(:retention_cleanup, started, result, %{flow_id: nil})
  end

  def retention_cleanup(_ctx, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def rewind(ctx, id, opts) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- rewind_attrs(id, opts) do
        Router.flow_rewind(ctx, attrs)
      end

    observe_flow(:rewind, started, result, %{flow_id: id})
  end

  def list(ctx, type, opts \\ [])

  def list(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, state} <- flow_state(opts),
         {:ok, partition_key} <- optional_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         index_key = __MODULE__.Keys.state_index_key(type, state, partition_key),
         :ok <- validate_key_size(index_key),
         {:ok, ids} <-
           flow_index_ids(
             ctx,
             index_key,
             state,
             partition_key,
             count,
             include_cold? or consistent_projection?,
             consistent_projection?
           ) do
      flow_records_for_ids(ctx, ids, partition_key)
    end
  end

  def list(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def list(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def by_parent(ctx, parent_flow_id, opts \\ [])

  def by_parent(ctx, parent_flow_id, opts)
      when is_binary(parent_flow_id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(parent_flow_id),
         {:ok, partition_key} <- optional_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         index_key = __MODULE__.Keys.parent_index_key(parent_flow_id, partition_key),
         :ok <- validate_key_size(index_key),
         {:ok, records} <-
           flow_records_for_index(
             ctx,
             index_key,
             partition_key,
             count,
             include_cold? or consistent_projection?,
             consistent_projection?
           ) do
      {:ok, filter_flow_records(records, :parent_flow_id, parent_flow_id, count)}
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
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         index_key = __MODULE__.Keys.root_index_key(root_flow_id, partition_key),
         :ok <- validate_key_size(index_key),
         {:ok, indexed_records} <-
           flow_records_for_index(
             ctx,
             index_key,
             partition_key,
             count,
             include_cold? or consistent_projection?,
             consistent_projection?
           ),
         {:ok, root_record} <- flow_root_record(ctx, root_flow_id, partition_key) do
      indexed_records = filter_flow_records(indexed_records, :root_flow_id, root_flow_id, count)

      records =
        [root_record | indexed_records]
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq_by(&Map.get(&1, :id))
        |> Enum.take(count)

      {:ok, records}
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
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         index_key = __MODULE__.Keys.correlation_index_key(correlation_id, partition_key),
         :ok <- validate_key_size(index_key),
         {:ok, records} <-
           flow_records_for_index(
             ctx,
             index_key,
             partition_key,
             count,
             include_cold? or consistent_projection?,
             consistent_projection?
           ) do
      {:ok, filter_flow_records(records, :correlation_id, correlation_id, count)}
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
         {:ok, partition_key} <- optional_partition_key(opts),
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
       |> Map.put(:partition_key, partition_key)
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
         {:ok, partition_key} <- optional_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, older_than_ms} <- optional_non_neg_integer(opts, :older_than_ms, 0),
         {:ok, now_ms} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
         index_key = __MODULE__.Keys.inflight_index_key(type, partition_key),
         :ok <- validate_key_size(index_key),
         cutoff = now_ms - older_than_ms,
         {:ok, ids} <- flow_zrangebyscore(ctx, index_key, "-inf", Integer.to_string(cutoff)) do
      flow_records_for_ids(ctx, Enum.take(ids, count), partition_key)
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
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, value_return} <- history_value_return_opts(opts) do
      flow_history_read(
        ctx,
        id,
        partition_key,
        history_key,
        count,
        include_cold? or consistent_projection?,
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
         count,
         false,
         _consistent?,
         value_return
       ) do
    if flow_history_state_exists?(ctx, id, partition_key) do
      case flow_history_hot_refs(ctx, id, partition_key, history_key, count) do
        {:ok, []} ->
          flow_history_fallback_scan(ctx, history_key, count, value_return)

        {:ok, event_refs} ->
          event_ids = Enum.map(event_refs, fn {event_id, _score} -> event_id end)

          flow_history_from_event_ids(
            ctx,
            id,
            partition_key,
            history_key,
            event_ids,
            value_return
          )
      end
    else
      {:ok, []}
    end
  end

  defp flow_history_read(
         ctx,
         id,
         partition_key,
         history_key,
         count,
         true,
         consistent?,
         value_return
       ) do
    if flow_history_state_exists?(ctx, id, partition_key) do
      with {:ok, hot_refs} <- flow_history_hot_refs(ctx, id, partition_key, history_key, count),
           {:ok, cold_refs} <- flow_history_lmdb_refs(ctx, history_key, count, consistent?) do
        scan_count = flow_lmdb_query_scan_count(count)

        event_ids =
          (hot_refs ++ cold_refs)
          |> Enum.sort_by(fn {event_id, score} -> {score, event_id} end)
          |> Enum.uniq_by(fn {event_id, _score} -> event_id end)
          |> Enum.take(-scan_count)
          |> Enum.map(fn {event_id, _score} -> event_id end)

        case event_ids do
          [] ->
            flow_history_fallback_scan(ctx, history_key, count, value_return)

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
              {:ok, Enum.take(events, -count)}
            end
        end
      end
    else
      {:ok, []}
    end
  end

  defp flow_history_state_exists?(ctx, id, partition_key) do
    case Router.flow_get(ctx, id, partition_key) do
      value when is_binary(value) -> true
      _ -> false
    end
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
      start_idx = max(total - max, 0)
      {start_idx, start_idx + count - 1}
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

  defp flow_history_lmdb_refs(_ctx, _history_key, count, _consistent?) when count <= 0,
    do: {:ok, []}

  defp flow_history_lmdb_refs(ctx, history_key, count, consistent?) do
    if Ferricstore.Flow.LMDB.mirror?() do
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

        with {:ok, _swept} <-
               Ferricstore.Flow.LMDB.sweep_expired_history(path, now_ms, sweep_limit),
             {:ok, entries} <-
               Ferricstore.Flow.LMDB.prefix_entries(
                 path,
                 prefix,
                 flow_history_lmdb_query_scan_count(count)
               ) do
          {:ok, flow_decode_history_index_entries(entries, path, now_ms)}
        end
      end
    else
      {:ok, []}
    end
  end

  defp flow_maybe_flush_lmdb_shard(_ctx, _shard_index, false), do: :ok

  defp flow_maybe_flush_lmdb_shard(ctx, shard_index, true),
    do: Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index)

  defp flow_require_lmdb_mirror_healthy_shard(ctx, index_key, shard_index) do
    if Ferricstore.Flow.LMDB.mirror?() and flow_lmdb_mirror_degraded_shard?(ctx, shard_index) do
      {:error, "ERR flow LMDB projection unavailable for #{index_key}"}
    else
      :ok
    end
  end

  defp flow_history_from_event_ids(ctx, id, partition_key, history_key, event_ids, value_return) do
    compound_keys =
      Enum.map(event_ids, &__MODULE__.Keys.stream_entry_key(id, &1, partition_key))

    values = Router.compound_batch_get(ctx, history_key, compound_keys)

    entries =
      event_ids
      |> Enum.zip(values)
      |> Enum.flat_map(fn
        {event_id, value} when is_binary(value) ->
          [{event_id, decode_history_fields(value)}]

        _missing ->
          []
      end)

    {:ok,
     entries
     |> Enum.map(&flow_history_entry_to_tuple/1)
     |> hydrate_history_values(ctx, value_return)}
  end

  defp flow_history_fallback_scan(ctx, history_key, count, value_return) do
    prefix = "X:" <> history_key <> <<0>>
    prefix_size = byte_size(prefix)

    entries =
      ctx
      |> Router.compound_scan(history_key, prefix)
      |> Enum.flat_map(fn
        {<<^prefix::binary-size(prefix_size), event_id::binary>>, value}
        when is_binary(value) ->
          [{event_id, decode_history_fields(value)}]

        {event_id, value} when is_binary(event_id) and is_binary(value) ->
          [{event_id, decode_history_fields(value)}]

        _other ->
          []
      end)
      |> Enum.sort_by(fn {event_id, _fields} -> {flow_history_event_ms(event_id), event_id} end)
      |> Enum.take(-count)

    {:ok,
     entries
     |> Enum.map(&flow_history_entry_to_tuple/1)
     |> hydrate_history_values(ctx, value_return)}
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
  # here; only payload_ref/result_ref/error_ref metadata is stored. Flow records
  # are not public-persisted yet, so this intentionally supports one current
  # format.
  def encode_record(record) when is_map(record) do
    [
      @record_bin_magic,
      encode_bin(Map.get(record, :id)),
      encode_bin(Map.get(record, :type)),
      encode_bin(Map.get(record, :state)),
      encode_int(Map.get(record, :version)),
      encode_int(Map.get(record, :attempts)),
      encode_int(Map.get(record, :fencing_token)),
      encode_int(Map.get(record, :created_at_ms)),
      encode_int(Map.get(record, :updated_at_ms)),
      encode_int(Map.get(record, :next_run_at_ms)),
      encode_int(Map.get(record, :priority)),
      encode_int(Map.get(record, :ttl_ms)),
      encode_int(Map.get(record, :history_hot_max_events)),
      encode_int(Map.get(record, :history_max_events)),
      encode_int(Map.get(record, :retention_ttl_ms)),
      encode_int(Map.get(record, :terminal_retention_until_ms)),
      encode_bin(Map.get(record, :partition_key)),
      encode_bin(Map.get(record, :payload_ref)),
      encode_bin(Map.get(record, :parent_flow_id)),
      encode_bin(Map.get(record, :parent_partition_key)),
      encode_bin(Map.get(record, :root_flow_id)),
      encode_bin(Map.get(record, :correlation_id)),
      encode_bin(Map.get(record, :result_ref)),
      encode_bin(Map.get(record, :error_ref)),
      encode_bin(Map.get(record, :lease_owner)),
      encode_bin(Map.get(record, :lease_token)),
      encode_int(Map.get(record, :lease_deadline_ms)),
      encode_bin(Map.get(record, :run_state)),
      encode_bin(Map.get(record, :rewound_to_event_id)),
      encode_term(Map.get(record, :child_groups, %{}))
    ]
    |> IO.iodata_to_binary()
  end

  @doc false
  def encode_value(value), do: @value_bin_magic <> :erlang.term_to_binary(value)

  @doc false
  def decode_value(@value_bin_magic <> encoded) do
    :erlang.binary_to_term(encoded)
  rescue
    _ -> @value_bin_magic <> encoded
  end

  def decode_value(value), do: value

  @doc false
  # Flow has not shipped as a public durable format yet, so recovery accepts
  # only the current compact record encoding.
  def decode_record(@record_bin_magic <> rest), do: decode_record_bin(rest)
  def decode_record(_value), do: raise(ArgumentError, "invalid flow record")

  @doc false
  # History entries have their own durable schema because they are retained for
  # audit/debug and rewind.
  def encode_history_fields(record, event, now_ms, meta \\ %{})
      when is_map(record) and is_binary(event) and is_integer(now_ms) do
    meta_fields = encode_history_meta(normalize_history_meta(meta))

    [
      @history_bin_magic,
      encode_bin(event),
      encode_int(Map.get(record, :version)),
      encode_int(now_ms),
      encode_bin(Map.get(record, :id)),
      encode_bin(Map.get(record, :type)),
      encode_bin(Map.get(record, :state)),
      encode_int(Map.get(record, :priority, 0)),
      encode_int(Map.get(record, :attempts, 0)),
      encode_int(Map.get(record, :fencing_token, 0)),
      encode_int(Map.get(record, :created_at_ms, now_ms)),
      encode_int(Map.get(record, :updated_at_ms, now_ms)),
      encode_int(Map.get(record, :next_run_at_ms)),
      encode_int(Map.get(record, :lease_deadline_ms)),
      encode_bin(Map.get(record, :lease_owner)),
      encode_bin(Map.get(record, :payload_ref)),
      encode_bin(Map.get(record, :parent_flow_id)),
      encode_bin(Map.get(record, :root_flow_id)),
      encode_bin(Map.get(record, :correlation_id)),
      encode_bin(Map.get(record, :result_ref)),
      encode_bin(Map.get(record, :error_ref)),
      encode_bin(Map.get(record, :rewound_to_event_id)),
      meta_fields
    ]
    |> IO.iodata_to_binary()
  end

  @doc false
  # Decode history into the current RESP-facing field list.
  def decode_history_fields(@history_bin_magic <> rest), do: decode_history_fields_bin(rest)
  def decode_history_fields(_value), do: []

  defp decode_history_fields_term({
         @history_tag,
         event,
         version,
         at,
         id,
         type,
         state,
         priority,
         attempts,
         fencing_token,
         created_at_ms,
         updated_at_ms,
         next_run_at_ms,
         lease_deadline_ms,
         lease_owner,
         payload_ref,
         parent_flow_id,
         root_flow_id,
         correlation_id,
         result_ref,
         error_ref,
         rewound_to_event_id,
         meta_fields
       }) do
    base_fields = [
      "event",
      event,
      "version",
      history_integer(version),
      "at",
      history_integer(at),
      "id",
      history_string(id),
      "type",
      history_string(type),
      "state",
      history_string(state),
      "priority",
      history_integer(priority),
      "attempts",
      history_integer(attempts),
      "fencing_token",
      history_integer(fencing_token),
      "created_at_ms",
      history_integer(created_at_ms),
      "updated_at_ms",
      history_integer(updated_at_ms),
      "next_run_at_ms",
      history_optional_integer(next_run_at_ms),
      "lease_deadline_ms",
      history_optional_integer(lease_deadline_ms),
      "lease_owner",
      history_string(lease_owner),
      "payload_ref",
      history_string(payload_ref),
      "parent_flow_id",
      history_string(parent_flow_id),
      "root_flow_id",
      history_string(root_flow_id),
      "correlation_id",
      history_string(correlation_id),
      "result_ref",
      history_string(result_ref),
      "error_ref",
      history_string(error_ref),
      "rewound_to_event_id",
      history_string(rewound_to_event_id)
    ]

    base_fields ++ normalize_history_decoded_meta(meta_fields)
  end

  defp decode_history_fields_term(_value), do: []

  defp decode_record_bin(rest) do
    with {:ok, id, rest} <- decode_bin(rest),
         {:ok, type, rest} <- decode_bin(rest),
         {:ok, state, rest} <- decode_bin(rest),
         {:ok, version, rest} <- decode_int(rest),
         {:ok, attempts, rest} <- decode_int(rest),
         {:ok, fencing_token, rest} <- decode_int(rest),
         {:ok, created_at_ms, rest} <- decode_int(rest),
         {:ok, updated_at_ms, rest} <- decode_int(rest),
         {:ok, next_run_at_ms, rest} <- decode_int(rest),
         {:ok, priority, rest} <- decode_int(rest),
         {:ok, ttl_ms, rest} <- decode_int(rest),
         {:ok, history_hot_max_events, rest} <- decode_int(rest),
         {:ok, history_max_events, rest} <- decode_int(rest),
         {:ok, retention_ttl_ms, rest} <- decode_int(rest),
         {:ok, terminal_retention_until_ms, rest} <- decode_int(rest),
         {:ok, partition_key, rest} <- decode_bin(rest),
         {:ok, payload_ref, rest} <- decode_bin(rest),
         {:ok, parent_flow_id, rest} <- decode_bin(rest),
         {:ok, parent_partition_key, rest} <- decode_bin(rest),
         {:ok, root_flow_id, rest} <- decode_bin(rest),
         {:ok, correlation_id, rest} <- decode_bin(rest),
         {:ok, result_ref, rest} <- decode_bin(rest),
         {:ok, error_ref, rest} <- decode_bin(rest),
         {:ok, lease_owner, rest} <- decode_bin(rest),
         {:ok, lease_token, rest} <- decode_bin(rest),
         {:ok, lease_deadline_ms, rest} <- decode_int(rest),
         {:ok, run_state, rest} <- decode_bin(rest),
         {:ok, rewound_to_event_id, rest} <- decode_bin(rest),
         {:ok, child_groups, ""} <- decode_term(rest) do
      record = %{
        id: id,
        type: type,
        state: state,
        version: version,
        attempts: attempts,
        fencing_token: fencing_token,
        created_at_ms: created_at_ms,
        updated_at_ms: updated_at_ms,
        next_run_at_ms: next_run_at_ms,
        priority: priority,
        ttl_ms: ttl_ms,
        history_hot_max_events: history_hot_max_events,
        history_max_events: history_max_events,
        retention_ttl_ms: retention_ttl_ms,
        terminal_retention_until_ms: terminal_retention_until_ms,
        partition_key: partition_key,
        payload_ref: payload_ref,
        parent_flow_id: parent_flow_id,
        parent_partition_key: parent_partition_key,
        root_flow_id: root_flow_id,
        correlation_id: correlation_id,
        result_ref: result_ref,
        error_ref: error_ref,
        lease_owner: lease_owner,
        lease_token: lease_token,
        lease_deadline_ms: lease_deadline_ms,
        run_state: run_state,
        child_groups: normalize_child_groups(child_groups)
      }

      if is_nil(rewound_to_event_id) do
        record
      else
        Map.put(record, :rewound_to_event_id, rewound_to_event_id)
      end
    else
      _ -> raise ArgumentError, "invalid flow record"
    end
  end

  defp decode_history_fields_bin(rest) do
    with {:ok, event, rest} <- decode_bin(rest),
         {:ok, version, rest} <- decode_int(rest),
         {:ok, at, rest} <- decode_int(rest),
         {:ok, id, rest} <- decode_bin(rest),
         {:ok, type, rest} <- decode_bin(rest),
         {:ok, state, rest} <- decode_bin(rest),
         {:ok, priority, rest} <- decode_int(rest),
         {:ok, attempts, rest} <- decode_int(rest),
         {:ok, fencing_token, rest} <- decode_int(rest),
         {:ok, created_at_ms, rest} <- decode_int(rest),
         {:ok, updated_at_ms, rest} <- decode_int(rest),
         {:ok, next_run_at_ms, rest} <- decode_int(rest),
         {:ok, lease_deadline_ms, rest} <- decode_int(rest),
         {:ok, lease_owner, rest} <- decode_bin(rest),
         {:ok, payload_ref, rest} <- decode_bin(rest),
         {:ok, parent_flow_id, rest} <- decode_bin(rest),
         {:ok, root_flow_id, rest} <- decode_bin(rest),
         {:ok, correlation_id, rest} <- decode_bin(rest),
         {:ok, result_ref, rest} <- decode_bin(rest),
         {:ok, error_ref, rest} <- decode_bin(rest),
         {:ok, rewound_to_event_id, rest} <- decode_bin(rest),
         {:ok, meta_fields, ""} <- decode_history_meta(rest) do
      decode_history_fields_term({
        @history_tag,
        event,
        version,
        at,
        id,
        type,
        state,
        priority,
        attempts,
        fencing_token,
        created_at_ms,
        updated_at_ms,
        next_run_at_ms,
        lease_deadline_ms,
        lease_owner,
        payload_ref,
        parent_flow_id,
        root_flow_id,
        correlation_id,
        result_ref,
        error_ref,
        rewound_to_event_id,
        meta_fields
      })
    else
      _ -> []
    end
  end

  defp normalize_history_meta(meta) when is_map(meta) do
    meta
    |> Enum.flat_map(fn
      {key, value} when is_atom(key) -> history_meta_pair(Atom.to_string(key), value)
      {key, value} when is_binary(key) -> history_meta_pair(key, value)
      _other -> []
    end)
  end

  defp normalize_history_meta(_meta), do: []

  defp history_meta_pair(key, nil), do: [{key, ""}]
  defp history_meta_pair(key, value) when is_binary(value), do: [{key, value}]
  defp history_meta_pair(key, value) when is_atom(value), do: [{key, Atom.to_string(value)}]
  defp history_meta_pair(key, value) when is_integer(value), do: [{key, Integer.to_string(value)}]
  defp history_meta_pair(key, value), do: [{key, to_string(value)}]

  defp encode_history_meta(fields) do
    [encode_int(length(fields)), Enum.map(fields, &encode_history_meta_pair/1)]
  end

  defp encode_history_meta_pair({key, value}), do: [encode_bin(key), encode_bin(value)]

  defp decode_history_meta(rest) do
    with {:ok, count, rest} <- decode_int(rest) do
      decode_history_meta_pairs(count, rest, [])
    end
  end

  defp decode_history_meta_pairs(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_history_meta_pairs(count, rest, acc) when is_integer(count) and count > 0 do
    with {:ok, key, rest} <- decode_bin(rest),
         {:ok, value, rest} <- decode_bin(rest) do
      decode_history_meta_pairs(count - 1, rest, [{key, value} | acc])
    end
  end

  defp decode_history_meta_pairs(_count, _rest, _acc), do: :error

  defp normalize_history_decoded_meta(fields) when is_list(fields) do
    Enum.flat_map(fields, fn
      {key, value} when is_binary(key) and is_binary(value) -> [key, value]
      _other -> []
    end)
  end

  defp normalize_history_decoded_meta(_fields), do: []

  defp encode_int(value) when is_integer(value) and value >= 0, do: encode_varint(value + 1)
  defp encode_int(_value), do: <<0>>

  defp decode_int(binary) do
    with {:ok, encoded, rest} <- decode_varint(binary) do
      case encoded do
        0 -> {:ok, nil, rest}
        value -> {:ok, value - 1, rest}
      end
    end
  end

  defp encode_bin(value) when is_binary(value),
    do: [encode_varint(byte_size(value) + 1), value]

  defp encode_bin(_value), do: <<0>>

  defp decode_bin(binary) do
    with {:ok, encoded, rest} <- decode_varint(binary) do
      case encoded do
        0 ->
          {:ok, nil, rest}

        size when size > 0 ->
          len = size - 1

          case rest do
            <<value::binary-size(len), tail::binary>> -> {:ok, value, tail}
            _ -> :error
          end
      end
    end
  end

  defp encode_term(value) do
    value
    |> :erlang.term_to_binary()
    |> encode_bin()
  end

  defp decode_term(binary) do
    with {:ok, encoded, rest} <- decode_bin(binary) do
      {:ok, :erlang.binary_to_term(encoded, [:safe]), rest}
    end
  rescue
    _ -> :error
  end

  defp normalize_child_groups(groups) when is_map(groups), do: groups
  defp normalize_child_groups(_groups), do: %{}

  defp encode_varint(value) when value < 128, do: <<value>>

  defp encode_varint(value) when value >= 128 do
    <<(value &&& 0x7F) ||| 0x80>> <> encode_varint(value >>> 7)
  end

  defp decode_varint(binary), do: decode_varint(binary, 0, 0)

  defp decode_varint(<<byte, rest::binary>>, acc, shift) when shift < 70 do
    value = acc ||| (byte &&& 0x7F) <<< shift

    if (byte &&& 0x80) == 0 do
      {:ok, value, rest}
    else
      decode_varint(rest, value, shift + 7)
    end
  end

  defp decode_varint(_binary, _acc, _shift), do: :error

  defp history_integer(value) when is_integer(value), do: Integer.to_string(value)
  defp history_integer(_value), do: "0"

  defp history_optional_integer(value) when is_integer(value), do: Integer.to_string(value)
  defp history_optional_integer(_value), do: ""

  defp history_string(value) when is_binary(value), do: value
  defp history_string(_value), do: ""

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

  defp safe_decode_record(value) when is_binary(value) do
    {:ok, decode_record(value)}
  rescue
    _ -> {:ok, nil}
  end

  defp prepend_decoded_record(value, acc) when is_binary(value) do
    case safe_decode_record(value) do
      {:ok, nil} -> acc
      {:ok, record} -> [record | acc]
    end
  end

  defp flow_records_for_index(ctx, index_key, partition_key, count, include_cold?, consistent?) do
    with {:ok, ram_entries} <- flow_ram_index_entries(ctx, index_key, count) do
      if include_cold? do
        with {:ok, lmdb_entries} <-
               flow_lmdb_query_index_entries(ctx, index_key, partition_key, count, consistent?) do
          ids =
            (Enum.map(ram_entries, fn {id, score} -> {id, score} end) ++
               Enum.map(lmdb_entries, fn {id, updated_at_ms, _state_key} ->
                 {id, updated_at_ms}
               end))
            |> Enum.sort_by(fn {id, score} -> {score, id} end)
            |> Enum.uniq_by(fn {id, _score} -> id end)
            |> Enum.map(fn {id, _score} -> id end)
            |> Enum.take(count)

          flow_records_for_ids(ctx, ids, partition_key)
        end
      else
        ids = Enum.map(ram_entries, fn {id, _score} -> id end)
        flow_records_for_ids(ctx, ids, partition_key)
      end
    end
  end

  defp filter_flow_records(records, field, value, count) do
    records
    |> Enum.filter(&(Map.get(&1, field) == value))
    |> Enum.take(count)
  end

  defp flow_root_record(ctx, root_flow_id, partition_key) do
    case get(ctx, root_flow_id, partition_key: partition_key) do
      {:ok, %{root_flow_id: ^root_flow_id} = record} -> {:ok, record}
      {:ok, nil} -> {:ok, nil}
      {:ok, _record} -> {:ok, nil}
      {:error, _reason} = error -> error
    end
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

  defp flow_index_ids(ctx, index_key, state, partition_key, count, include_cold?, consistent?)
       when state in @terminal_states do
    with {:ok, ram_entries} <- flow_ram_index_entries(ctx, index_key, count),
         {:ok, lmdb_entries} <-
           flow_terminal_lmdb_entries(
             ctx,
             index_key,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?
           ) do
      ids =
        (ram_entries ++ lmdb_entries)
        |> Enum.sort_by(fn {id, score} -> {score, id} end)
        |> Enum.uniq_by(fn {id, _score} -> id end)
        |> Enum.map(fn {id, _score} -> id end)
        |> Enum.take(count)

      {:ok, ids}
    end
  end

  defp flow_index_ids(ctx, index_key, state, partition_key, count, include_cold?, consistent?) do
    with {:ok, ram_ids} <- flow_zrange(ctx, index_key, 0, count - 1),
         {:ok, lmdb_ids} <-
           flow_terminal_lmdb_ids(
             ctx,
             index_key,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?
           ) do
      {:ok, (ram_ids ++ lmdb_ids) |> Enum.uniq() |> Enum.take(count)}
    end
  end

  defp flow_ram_index_entries(_ctx, _index_key, count) when count <= 0, do: {:ok, []}

  defp flow_ram_index_entries(ctx, index_key, count) do
    case Router.flow_index_rank_range(ctx, index_key, 0, count - 1, false) do
      {:ok, entries} -> {:ok, entries}
      :unavailable -> {:ok, []}
    end
  end

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
           flow_terminal_lmdb_ids(ctx, index_key, state, partition_key, lmdb_count, true, false) do
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
         _consistent?
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
         _consistent?
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
         consistent?
       ) do
    if include_cold? and Ferricstore.Flow.LMDB.mirror?() do
      with {:ok, entries} <-
             flow_lmdb_index_entries(ctx, index_key, partition_key, count, consistent?) do
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
         _consistent?
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
         _consistent?
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
         consistent?
       ) do
    if include_cold? and Ferricstore.Flow.LMDB.mirror?() do
      flow_lmdb_index_entries(ctx, index_key, partition_key, count, consistent?)
    else
      {:ok, []}
    end
  end

  defp flow_lmdb_index_entries(_ctx, _index_key, _partition_key, count, _consistent?)
       when count <= 0,
       do: {:ok, []}

  defp flow_lmdb_index_entries(ctx, index_key, partition_key, count, consistent?) do
    with :ok <- flow_maybe_flush_lmdb_for_index(ctx, index_key, partition_key, consistent?),
         :ok <- flow_require_lmdb_mirror_healthy(ctx, index_key, partition_key) do
      prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(index_key)
      now_ms = now_ms()
      sweep_limit = flow_terminal_lmdb_sweep_limit()

      ctx
      |> flow_lmdb_paths_for_index(index_key, partition_key)
      |> Enum.reduce_while({:ok, []}, fn path, {:ok, acc} ->
        with {:ok, _swept} <-
               Ferricstore.Flow.LMDB.sweep_expired_terminal(path, now_ms, sweep_limit),
             {:ok, entries} <- Ferricstore.Flow.LMDB.prefix_entries(path, prefix, count) do
          {:cont, {:ok, flow_decode_terminal_index_entries(entries, path, now_ms) ++ acc}}
        else
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, entries} ->
          entries =
            entries
            |> Enum.sort_by(fn {id, updated_at_ms} -> {updated_at_ms, id} end)
            |> Enum.take(count)

          {:ok, entries}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp flow_lmdb_query_index_entries(_ctx, _index_key, _partition_key, count, _consistent?)
       when count <= 0,
       do: {:ok, []}

  defp flow_lmdb_query_index_entries(ctx, index_key, partition_key, count, consistent?) do
    with :ok <- flow_maybe_flush_lmdb_for_index(ctx, index_key, partition_key, consistent?),
         :ok <- flow_require_lmdb_mirror_healthy(ctx, index_key, partition_key) do
      prefix = Ferricstore.Flow.LMDB.query_index_prefix(index_key)
      now_ms = now_ms()
      scan_count = flow_lmdb_query_scan_count(count)

      ctx
      |> flow_lmdb_paths_for_index(index_key, partition_key)
      |> Enum.reduce_while({:ok, []}, fn path, {:ok, acc} ->
        with {:ok, entries} <- Ferricstore.Flow.LMDB.prefix_entries(path, prefix, scan_count) do
          {:cont, {:ok, flow_decode_query_index_entries(entries, path, now_ms) ++ acc}}
        else
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, entries} ->
          entries =
            entries
            |> Enum.sort_by(fn {id, updated_at_ms, _state_key} -> {updated_at_ms, id} end)

          {:ok, entries}

        {:error, _reason} = error ->
          error
      end
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

  defp flow_history_lmdb_query_scan_count(count) when is_integer(count) and count > 0 do
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

    max(count, max_scan)
  end

  defp flow_require_lmdb_mirror_healthy(ctx, index_key, partition_key) do
    if Ferricstore.Flow.LMDB.mirror?() and
         flow_lmdb_mirror_degraded?(ctx, index_key, partition_key) do
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
      |> Router.batch_get_with_file_refs(refs, file_ref_payload_threshold(max_bytes))
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
    |> Map.put(kind <> "_size", size)
  end

  defp apply_history_value_result_for_valid_ref(fields, kind, encoded_value, max_bytes)
       when is_binary(encoded_value) do
    size = byte_size(encoded_value)

    if size <= max_bytes do
      fields
      |> Map.put(kind, decode_value(encoded_value))
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

  defp observe_flow(command, started, result, fallback_metadata) do
    measurements = flow_measurements(started, command, result)
    metadata = flow_metadata(result, fallback_metadata)

    :telemetry.execute([:ferricstore, :flow, command, :stop], measurements, metadata)

    result
  end

  defp observe_flow_batch(command, started, results) do
    records =
      results
      |> Enum.flat_map(fn
        {:ok, record} when is_map(record) -> [record]
        _ -> []
      end)

    measurements = flow_measurements(started, command, {:ok, records})
    metadata = flow_metadata({:ok, records}, %{flow_id: nil})

    :telemetry.execute([:ferricstore, :flow, command, :stop], measurements, metadata)
    :ok
  end

  defp observe_pipeline_read_batch(started, ops) do
    :telemetry.execute(
      [:ferricstore, :flow, :pipeline_read_batch],
      %{
        count: length(ops),
        gets: Enum.count(ops, &match?({:get, _id, _opts}, &1)),
        histories: Enum.count(ops, &match?({:history, _id, _opts}, &1)),
        duration: System.monotonic_time() - started
      },
      %{source: :pipeline}
    )
  end

  defp flow_measurements(started, command, result) do
    count = result_count(result)

    %{
      duration_ms:
        System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond),
      count: count,
      claimed: if(command == :claim_due, do: count, else: 0)
    }
  end

  defp elapsed_us(started) do
    System.convert_time_unit(System.monotonic_time() - started, :native, :microsecond)
  end

  defp result_count({:ok, records}) when is_list(records), do: length(records)
  defp result_count({:ok, nil}), do: 0
  defp result_count({:ok, _record}), do: 1
  defp result_count(_result), do: 0

  defp flow_metadata({:ok, records}, fallback) when is_list(records) do
    records
    |> List.first(%{})
    |> flow_record_metadata()
    |> Map.merge(fallback, fn _key, record_value, fallback_value ->
      record_value || fallback_value
    end)
    |> Map.merge(%{result: :ok, reason: nil})
  end

  defp flow_metadata({:ok, record}, fallback) when is_map(record) do
    record
    |> flow_record_metadata()
    |> Map.merge(fallback, fn _key, record_value, fallback_value ->
      record_value || fallback_value
    end)
    |> Map.merge(%{result: :ok, reason: nil})
  end

  defp flow_metadata({:ok, _value}, fallback),
    do: Map.merge(fallback, %{result: :ok, reason: nil})

  defp flow_metadata({:error, reason}, fallback) when is_binary(reason) do
    Map.merge(fallback, %{result: :error, reason: flow_error_reason(reason)})
  end

  defp flow_metadata(_result, fallback),
    do: Map.merge(fallback, %{result: :error, reason: :error})

  defp flow_record_metadata(record) when is_map(record) do
    %{
      flow_id: Map.get(record, :id),
      flow_type: Map.get(record, :type),
      to_state: Map.get(record, :state),
      worker_id: Map.get(record, :lease_owner),
      fencing_token: Map.get(record, :fencing_token)
    }
  end

  defp flow_record_metadata(_record), do: %{}

  defp flow_error_reason(reason) do
    cond do
      String.contains?(reason, "wrong state") -> :wrong_state
      String.contains?(reason, "stale flow lease") -> :stale_token
      String.contains?(reason, "not found") -> :missing
      String.contains?(reason, "already exists") -> :exists
      true -> :error
    end
  end

  defp validate_opts(opts) do
    if Keyword.keyword?(opts) do
      :ok
    else
      {:error, "ERR flow opts must be a keyword list"}
    end
  end

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

  defp validate_flow_keys(id, type, state, priority, partition_key) do
    with :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         :ok <- validate_key_size(__MODULE__.Keys.history_key(id, partition_key)),
         :ok <- validate_key_size(__MODULE__.Keys.due_key(type, state, priority, partition_key)) do
      validate_key_size(
        __MODULE__.Keys.stream_entry_key(
          id,
          "18446744073709551615-18446744073709551615",
          partition_key
        )
      )
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
         :ok <- reject_public_value_ref_input(opts, :payload_ref, :payload),
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
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_flow_keys(id, type, state, priority, partition_key) do
      attrs =
        %{
          id: id,
          type: type,
          state: state,
          payload_ref: nil,
          parent_flow_id: parent_flow_id,
          root_flow_id: root_flow_id,
          correlation_id: correlation_id,
          idempotent: idempotent,
          retention_ttl_ms: retention_ttl_ms,
          history_hot_max_events: history_hot_max_events,
          history_max_events: history_max_events,
          priority: priority,
          partition_key: partition_key
        }
        |> maybe_put_flow_value(opts, :payload)
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
             many_item_partition_key(partition_key, item_opts, mismatch_error),
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

  defp create_many_item_opts({:id, id, :payload_ref, _payload_ref}) when is_binary(id) do
    {:error, "ERR flow payload_ref input is not supported; use payload"}
  end

  defp create_many_item_opts({:id, id, :payload, payload}) when is_binary(id) do
    {:ok, id, [payload: payload]}
  end

  defp create_many_item_opts(
         {:id, id, :partition_key, _partition_key, :payload_ref, _payload_ref}
       )
       when is_binary(id) do
    {:error, "ERR flow payload_ref input is not supported; use payload"}
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
          lease_token: lease_token,
          fencing_token: fencing_token,
          priority: priority,
          partition_key: partition_key
        }
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_attr(:now_ms, now)
        |> maybe_put_attr(:run_at_ms, run_at_ms)

      {:ok, attrs}
    end
  end

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
          error_ref: nil,
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
          ttl_ms: ttl_ms,
          result_ref: nil,
          partition_key: partition_key
        }
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_flow_value(opts, :result)
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
           {:ok, item_partition_key} <- many_item_partition_key(partition_key, item_opts),
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
          ttl_ms: ttl_ms,
          error_ref: nil,
          partition_key: partition_key
        }
        |> maybe_put_flow_value(opts, :payload)
        |> maybe_put_flow_value(opts, :error)
        |> maybe_put_attr(:now_ms, now)

      {:ok, attrs}
    end
  end

  defp cancel_attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, lease_token} <- optional_lease_token(opts),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, ttl_ms} <- optional_pos_integer_or_nil(opts, :ttl_ms),
         {:ok, reason_ref} <- optional_binary_or_nil(opts, :reason_ref, nil),
         :ok <- validate_ref_size(:reason_ref, reason_ref),
         :ok <- validate_cancel_reason_source(opts) do
      attrs =
        %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          ttl_ms: ttl_ms,
          reason_ref: reason_ref,
          partition_key: partition_key
        }
        |> maybe_put_cancel_reason(opts)
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

  defp maybe_put_cancel_reason(attrs, opts) do
    if Keyword.has_key?(opts, :reason) do
      Map.put(attrs, :error, Keyword.fetch!(opts, :reason))
    else
      attrs
    end
  end

  defp rewind_attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, to_event} <- required_binary(opts, :to_event),
         {:ok, expect_state} <- optional_binary_or_nil(opts, :expect_state, nil),
         {:ok, run_at_ms} <- optional_non_neg_integer_or_nil(opts, :run_at_ms),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, reason_ref} <- optional_binary_or_nil(opts, :reason_ref, nil),
         :ok <- validate_ref_size(:reason_ref, reason_ref),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         :ok <- validate_key_size(__MODULE__.Keys.history_key(id, partition_key)) do
      attrs =
        %{
          id: id,
          to_event: to_event,
          expect_state: expect_state,
          run_at_ms: run_at_ms,
          reason_ref: reason_ref,
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

  defp pipeline_write_command({:transition, id, from_state, to_state, opts}) do
    with {:ok, attrs} <- transition_attrs(id, from_state, to_state, opts) do
      pipeline_state_command(:flow_transition, attrs)
    end
  end

  defp pipeline_write_command({:complete, _id, _lease_token, _opts}) do
    {:error, "ERR unsupported flow pipeline command"}
  end

  defp pipeline_write_command({:retry, _id, _lease_token, _opts}) do
    {:error, "ERR unsupported flow pipeline command"}
  end

  defp pipeline_write_command({:fail, _id, _lease_token, _opts}) do
    {:error, "ERR unsupported flow pipeline command"}
  end

  defp pipeline_write_command({:cancel, _id, _opts}) do
    {:error, "ERR unsupported flow pipeline command"}
  end

  defp pipeline_write_command({:rewind, id, opts}) do
    with {:ok, attrs} <- rewind_attrs(id, opts) do
      pipeline_state_command(:flow_rewind, attrs)
    end
  end

  defp pipeline_write_command(_op), do: {:error, "ERR unsupported flow pipeline command"}

  defp pipeline_state_command(command, %{id: id, partition_key: partition_key} = attrs) do
    key = __MODULE__.Keys.state_key(id, partition_key)
    {:ok, {key, {command, key, attrs}}}
  end

  defp pipeline_claim_due_command({:claim_due, type, opts})
       when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, state} <- optional_claim_states(opts),
         {:ok, worker} <- required_binary(opts, :worker),
         {:ok, lease_ms} <- optional_pos_integer(opts, :lease_ms, @default_lease_ms),
         {:ok, limit} <- optional_claim_limit(opts),
         {:ok, priority} <- optional_priority_or_nil(opts),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, payload_return} <- payload_return_opts(opts, true),
         {:ok, reclaim_expired?} <- optional_boolean(opts, :reclaim_expired, true),
         {:ok, reclaim_ratio} <- optional_reclaim_ratio(opts),
         {:ok, partition_key} <- optional_claim_partition_key(opts),
         :ok <- validate_claim_due_keys(type, state, priority, partition_key) do
      normalized_opts =
        claim_due_normalized_opts(
          state,
          worker,
          lease_ms,
          limit,
          priority,
          now,
          payload_return,
          reclaim_expired?,
          reclaim_ratio,
          partition_key
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
        |> maybe_put_attr(:now_ms, now)

      queue_key = {type, state, priority, now, partition_key}

      key =
        {type, state, worker, lease_ms, priority, now, payload_return, reclaim_expired?,
         reclaim_ratio, partition_key}

      {:ok,
       %{
         type: type,
         attrs: attrs,
         opts: normalized_opts,
         limit: limit,
         key: key,
         queue_key: queue_key,
         payload_return: payload_return,
         reclaim_expired?: reclaim_expired?,
         reclaim_ratio: reclaim_ratio,
         groupable?: partition_key != :any
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
    pipeline_claim_due_adjacent_results(rest, ctx, Enum.reverse(results) ++ acc, stats)
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

    {indexed_results, stats} =
      Enum.reduce(groups, {indexed_results, stats}, fn {_key, indexed_claims},
                                                       {result_acc, stats_acc} ->
        indexed_claims = Enum.reverse(indexed_claims)
        claims = Enum.map(indexed_claims, fn {_idx, claim} -> claim end)
        {results, stats_acc} = execute_claim_due_run(ctx, claims, stats_acc)

        indexed_claims
        |> Enum.map(fn {idx, _claim} -> idx end)
        |> Enum.zip(results)
        |> Enum.reduce({result_acc, stats_acc}, fn {idx, result}, {acc, stats} ->
          {Map.put(acc, idx, result), stats}
        end)
      end)

    results = for idx <- 0..(length(commands) - 1), do: Map.fetch!(indexed_results, idx)
    {results, stats}
  end

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
      |> hydrate_claim_due_pipeline_results(ctx, hd(claims).payload_return)
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

  defp hydrate_claim_due_pipeline_results(results, ctx, payload_return) do
    Enum.map(results, fn
      {:ok, records} -> {:ok, hydrate_payload_records(ctx, records, payload_return)}
      other -> other
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
         payload_return,
         reclaim_expired?,
         reclaim_ratio,
         partition_key
       ) do
    [
      state: state,
      worker: worker,
      lease_ms: lease_ms,
      limit: limit,
      payload: payload_return.enabled?,
      payload_max_bytes: payload_return.max_bytes,
      reclaim_expired: reclaim_expired?,
      reclaim_ratio: reclaim_ratio
    ]
    |> maybe_put_keyword(:priority, priority)
    |> maybe_put_keyword(:now_ms, now)
    |> maybe_put_keyword(:partition_key, partition_key)
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

  defp pipeline_read_command(_ctx, {:history, id, opts}) when is_binary(id) and is_list(opts) do
    with {:ok, {partition_key, history_key, count, include_cold?, consistent?, value_return}} <-
           history_query_attrs(id, opts) do
      {:history, id, partition_key, history_key, count, include_cold?, consistent?, value_return}
    end
  end

  defp pipeline_read_command(ctx, {:list, type, opts}) when is_binary(type) and is_list(opts),
    do: pipeline_read_result(fn -> list(ctx, type, opts) end)

  defp pipeline_read_command(ctx, {:by_parent, parent_flow_id, opts})
       when is_binary(parent_flow_id) and is_list(opts),
       do: pipeline_read_result(fn -> by_parent(ctx, parent_flow_id, opts) end)

  defp pipeline_read_command(ctx, {:by_root, root_flow_id, opts})
       when is_binary(root_flow_id) and is_list(opts),
       do: pipeline_read_result(fn -> by_root(ctx, root_flow_id, opts) end)

  defp pipeline_read_command(ctx, {:by_correlation, correlation_id, opts})
       when is_binary(correlation_id) and is_list(opts),
       do: pipeline_read_result(fn -> by_correlation(ctx, correlation_id, opts) end)

  defp pipeline_read_command(ctx, {:info, type, opts}) when is_binary(type) and is_list(opts),
    do: pipeline_read_result(fn -> info(ctx, type, opts) end)

  defp pipeline_read_command(ctx, {:stuck, type, opts}) when is_binary(type) and is_list(opts),
    do: pipeline_read_result(fn -> stuck(ctx, type, opts) end)

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
      |> Router.batch_get_with_file_refs(fetchable_refs, file_ref_payload_threshold(max_bytes))
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
    |> Map.put(flow_value_size_field(kind), size)
  end

  defp apply_flow_value_result_for_valid_ref(record, kind, _ref, encoded_value, max_bytes)
       when is_binary(encoded_value) do
    size = byte_size(encoded_value)

    if size <= max_bytes do
      record
      |> Map.put(kind, decode_value(encoded_value))
      |> Map.put(flow_value_size_field(kind), size)
    else
      record
      |> Map.put(flow_value_omitted_field(kind), true)
      |> Map.put(flow_value_size_field(kind), size)
    end
  end

  defp apply_flow_value_result_for_valid_ref(record, _kind, _ref, _other, _max_bytes), do: record

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
  defp file_ref_payload_threshold(max_bytes), do: max_bytes + 1

  defp pipeline_read_history_results([], _ctx), do: %{}

  defp pipeline_read_history_results(history_ops, ctx) do
    hot_ops =
      Enum.filter(history_ops, fn
        {_idx, _id, _partition_key, _history_key, _count, false, false, %{enabled?: false}} ->
          true

        _cold_or_consistent ->
          false
      end)

    cold_ops = history_ops -- hot_ops

    cold_results =
      Map.new(cold_ops, fn {idx, id, partition_key, history_key, count, include_cold?,
                            consistent?, value_return} ->
        {idx,
         flow_history_read(
           ctx,
           id,
           partition_key,
           history_key,
           count,
           include_cold? or consistent?,
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
      Enum.map(history_ops, fn {idx, id, partition_key, history_key, count, false, false,
                                value_return} ->
        {start_idx, stop_idx} = flow_history_hot_range(ctx, id, partition_key, history_key, count)
        {idx, id, partition_key, history_key, start_idx, stop_idx, false, value_return}
      end)

    router_requests =
      Enum.map(requests, fn {_idx, _id, _partition_key, history_key, start_idx, stop_idx,
                             reverse?, _value_return} ->
        {history_key, start_idx, stop_idx, reverse?}
      end)

    case Router.flow_index_rank_range_many(ctx, router_requests) do
      {:ok, rank_results} ->
        history_ops
        |> Enum.zip(rank_results)
        |> Map.new(fn {{idx, id, partition_key, history_key, count, _include_cold?, _consistent?,
                        value_return}, rank_result} ->
          {idx,
           history_result_from_rank(
             ctx,
             id,
             partition_key,
             history_key,
             count,
             rank_result,
             value_return
           )}
        end)

      :unavailable ->
        Map.new(history_ops, fn {idx, _id, _partition_key, history_key, count, _include_cold?,
                                 _consistent?, value_return} ->
          {idx, flow_history_fallback_scan(ctx, history_key, count, value_return)}
        end)
    end
  end

  defp history_result_from_rank(ctx, _id, _partition_key, history_key, count, [], value_return),
    do: flow_history_fallback_scan(ctx, history_key, count, value_return)

  defp history_result_from_rank(
         ctx,
         id,
         partition_key,
         history_key,
         _count,
         event_refs,
         value_return
       ) do
    event_ids = Enum.map(event_refs, fn {event_id, _score} -> event_id end)
    flow_history_from_event_ids(ctx, id, partition_key, history_key, event_ids, value_return)
  end

  defp history_query_attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, partition_key} <- optional_partition_key(opts),
         history_key = __MODULE__.Keys.history_key(id, partition_key),
         :ok <- validate_key_size(history_key),
         {:ok, count} <- flow_count(opts),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, value_return} <- history_value_return_opts(opts) do
      {:ok, {partition_key, history_key, count, include_cold?, consistent?, value_return}}
    end
  end

  defp fail_many_attrs(items, opts, partition_key) do
    base_opts = Keyword.delete(opts, :partition_key)

    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, lease_token, item_opts} <- fail_many_item_opts(item),
           {:ok, item_partition_key} <- many_item_partition_key(partition_key, item_opts),
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
           {:ok, item_partition_key} <- many_item_partition_key(partition_key, item_opts),
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
           {:ok, item_partition_key} <- many_item_partition_key(partition_key, item_opts),
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
           {:ok, item_partition_key} <- many_item_partition_key(partition_key, item_opts),
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
         partition_key,
         item_opts,
         mismatch_error \\ "ERR flow partition_key mismatch in batch"
       )

  defp many_item_partition_key(nil, item_opts, _mismatch_error) do
    item_opts
    |> Keyword.get(:partition_key)
    |> required_partition_key()
  end

  defp many_item_partition_key(partition_key, item_opts, :allow_override)
       when is_binary(partition_key) do
    item_opts
    |> Keyword.get(:partition_key, partition_key)
    |> required_partition_key()
  end

  defp many_item_partition_key(partition_key, item_opts, mismatch_error)
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
        value when is_integer(value) and value > 0 ->
          max = flow_max_history_hot_max_events()

          if value <= max do
            {:ok, value}
          else
            {:error, "ERR flow history_hot_max_events exceeds maximum #{max}"}
          end

        _ ->
          {:error, "ERR flow history_hot_max_events must be a positive integer"}
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
    Enum.reduce_while(@max_priority..0//-1, :ok, fn priority, :ok ->
      case validate_claim_due_keys(type, state, priority, partition_key) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_claim_due_keys(type, :any, _priority, _partition_key) do
    validate_key_size(__MODULE__.Keys.due_key(type, @default_state, 0, nil))
  end

  defp validate_claim_due_keys(type, states, priority, partition_key) when is_list(states) do
    Enum.reduce_while(states, :ok, fn state, :ok ->
      case validate_claim_due_keys(type, state, priority, partition_key) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_claim_due_keys(type, state, priority, :any) when is_binary(state) do
    validate_key_size(__MODULE__.Keys.due_key(type, state, priority, nil))
  end

  defp validate_claim_due_keys(type, state, priority, partition_key) do
    validate_key_size(__MODULE__.Keys.due_key(type, state, priority, partition_key))
  end

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

  defp optional_claim_partition_key(opts) do
    case Keyword.get(opts, :partition_key, nil) do
      :any ->
        {:ok, :any}

      value when is_binary(value) and value != "" ->
        case String.upcase(value) do
          "ANY" -> {:ok, :any}
          "GLOBAL" -> {:ok, nil}
          _ -> {:ok, value}
        end

      _ ->
        optional_partition_key(opts)
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
        {:ok, @default_state}
    end
  end

  defp normalize_claim_state_values(:any), do: {:ok, :any}

  defp normalize_claim_state_values(value) when is_binary(value) do
    if String.upcase(value) == "ANY" do
      {:ok, :any}
    else
      normalize_claim_state_values([value])
    end
  end

  defp normalize_claim_state_values(values) when is_list(values) do
    cond do
      values == [] ->
        {:error, "ERR flow states must be a non-empty list"}

      Enum.any?(values, &claim_state_any?/1) ->
        if length(values) == 1 do
          {:ok, :any}
        else
          {:error, "ERR flow STATE ANY cannot be mixed with explicit states"}
        end

      true ->
        values
        |> Enum.reduce_while({:ok, []}, fn
          value, {:ok, acc} when is_binary(value) and value != "" -> {:cont, {:ok, [value | acc]}}
          _bad, {:ok, _acc} -> {:halt, {:error, "ERR flow state must be a non-empty string"}}
        end)
        |> case do
          {:ok, [single]} -> {:ok, single}
          {:ok, states} -> {:ok, Enum.reverse(Enum.uniq(states))}
          {:error, _reason} = error -> error
        end
    end
  end

  defp normalize_claim_state_values(_value),
    do: {:error, "ERR flow state must be a non-empty string"}

  defp claim_state_any?(:any), do: true
  defp claim_state_any?(value) when is_binary(value), do: String.upcase(value) == "ANY"
  defp claim_state_any?(_value), do: false

  defp required_partition_key(partition_key) do
    case optional_partition_key(partition_key: partition_key) do
      {:ok, nil} -> {:error, "ERR flow partition_key is required"}
      {:ok, value} -> {:ok, value}
      {:error, _reason} = error -> error
    end
  end

  defp maybe_put_attr(attrs, _key, nil), do: attrs
  defp maybe_put_attr(attrs, key, value), do: Map.put(attrs, key, value)

  defp maybe_put_flow_value(attrs, opts, key) do
    if Keyword.has_key?(opts, key) do
      Map.put(attrs, key, Keyword.fetch!(opts, key))
    else
      attrs
    end
  end

  defp flow_policy_read(ctx, type) do
    case Router.get(ctx, __MODULE__.Keys.policy_key(type)) do
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
      retention: RetryPolicy.resolve_retention(policy, nil, nil),
      states:
        Map.new(states, fn {state, _state_policy} ->
          {state,
           %{
             retry: RetryPolicy.resolve(policy, state, nil),
             retention: RetryPolicy.resolve_retention(policy, state, nil)
           }}
        end)
    }
  end

  defp policy_response(type, policy, state) when is_binary(state) do
    %{
      type: type,
      state: state,
      retry: RetryPolicy.resolve(policy, state, nil),
      retention: RetryPolicy.resolve_retention(policy, state, nil)
    }
  end

  defp now_ms, do: CommandTime.now_ms()

  defmodule Keys do
    @moduledoc false

    @global_tag "{f}"
    @partition_tag_prefix "{f:"

    def state_key(id, partition_key \\ nil) do
      "f:" <> tag(partition_key) <> ":s:" <> id
    end

    def state_key_from_due_key(due_key, id) when is_binary(due_key) and is_binary(id) do
      case :binary.match(due_key, "}:d:") do
        {pos, _len} when pos >= 2 ->
          tag = binary_part(due_key, 2, pos + 1 - 2)
          {:ok, "f:" <> tag <> ":s:" <> id}

        :nomatch ->
          :error
      end
    end

    def history_key(id, partition_key \\ nil) do
      "f:" <> tag(partition_key) <> ":h:" <> id
    end

    def value_key(id, kind, version, partition_key \\ nil)
        when kind in [:payload, :result, :error] and is_integer(version) do
      "f:" <>
        tag(partition_key) <>
        ":v:" <> flow_value_kind(kind) <> ":" <> id <> ":" <> Integer.to_string(version)
    end

    def policy_key(type) do
      "f:" <> @global_tag <> ":policy:" <> type
    end

    def policy_key?(key) when is_binary(key),
      do: String.starts_with?(key, "f:" <> @global_tag <> ":policy:")

    def policy_key?(_key), do: false

    def due_key(type, state, priority, partition_key \\ nil) do
      "f:" <>
        tag(partition_key) <>
        ":d:" <> type <> ":" <> state <> ":p" <> Integer.to_string(priority)
    end

    def state_index_key(type, state, partition_key \\ nil) do
      "f:" <> tag(partition_key) <> ":i:s:" <> type <> ":" <> state
    end

    def inflight_index_key(type, partition_key \\ nil) do
      "f:" <> tag(partition_key) <> ":i:r:" <> type
    end

    def worker_index_key(worker, partition_key \\ nil) do
      "f:" <> tag(partition_key) <> ":i:w:" <> worker
    end

    def parent_index_key(parent_flow_id, partition_key \\ nil) do
      "f:" <> tag(partition_key) <> ":i:p:" <> parent_flow_id
    end

    def root_index_key(root_flow_id, partition_key \\ nil) do
      "f:" <> tag(partition_key) <> ":i:o:" <> root_flow_id
    end

    def correlation_index_key(correlation_id, partition_key \\ nil) do
      "f:" <> tag(partition_key) <> ":i:c:" <> correlation_id
    end

    def stream_entry_key(id, event_id, partition_key \\ nil) do
      "X:" <> history_key(id, partition_key) <> <<0>> <> event_id
    end

    def state_key?(key) when is_binary(key) do
      String.starts_with?(key, "f:{f") and String.contains?(key, "}:s:")
    end

    def state_key?(_key), do: false

    def tag(nil), do: @global_tag
    def tag(:global), do: @global_tag

    def tag(partition_key) when is_binary(partition_key) do
      @partition_tag_prefix <>
        Base.url_encode64(:crypto.hash(:sha256, partition_key), padding: false) <> "}"
    end

    defp flow_value_kind(:payload), do: "p"
    defp flow_value_kind(:result), do: "r"
    defp flow_value_kind(:error), do: "e"
  end
end
