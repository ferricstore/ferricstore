defmodule Ferricstore.Flow do
  @moduledoc false

  import Bitwise

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.ClaimWaiters
  alias Ferricstore.Flow.RetryPolicy
  alias Ferricstore.Stats
  alias Ferricstore.Store.{BlobValue, ColdRead, Router}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @default_state "queued"
  @default_priority 0
  @max_priority 2
  @claim_waiter_max_wake_per_ready_bucket 8
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
  @value_bin_magic "FSV2"

  # Flow records and history are durable bytes. Before Flow is public, keep one
  # current compact schema and change it directly. Once users can have persisted
  # Flow data, incompatible field-order/type changes need explicit migration.
  @record_bin_magic "FSF5"
  @history_bin_magic "FSH2"
  @record_value_refs_key "__value_refs__"
  @history_value_refs_key "value_refs"

  # FSF5 stores only the required mutable state fields inline. Optional fields
  # are controlled by the flag word below so nil/default values do not repeat on
  # every state record. Keep this layout in lockstep with the Rust NIF codec.
  @record_flag_attempts 1 <<< 0
  @record_flag_fencing_token 1 <<< 1
  @record_flag_next_run_at_ms 1 <<< 2
  @record_flag_priority 1 <<< 3
  @record_flag_ttl_ms 1 <<< 4
  @record_flag_history_hot_max_events 1 <<< 5
  @record_flag_history_max_events 1 <<< 6
  @record_flag_retention_ttl_ms 1 <<< 7
  @record_flag_terminal_retention_until_ms 1 <<< 8
  @record_flag_partition_key 1 <<< 9
  @record_flag_payload_ref 1 <<< 10
  @record_flag_parent_flow_id 1 <<< 11
  @record_flag_parent_partition_key 1 <<< 12
  @record_flag_root_flow_id 1 <<< 13
  @record_flag_root_flow_id_self 1 <<< 14
  @record_flag_correlation_id 1 <<< 15
  @record_flag_result_ref 1 <<< 16
  @record_flag_error_ref 1 <<< 17
  @record_flag_lease_owner 1 <<< 18
  @record_flag_lease_token 1 <<< 19
  @record_flag_lease_deadline_ms 1 <<< 20
  @record_flag_run_state 1 <<< 21
  @record_flag_rewound_to_event_id 1 <<< 22
  @record_flag_sidecar 1 <<< 23

  # FSH2 stores per-event history only. Immutable workflow metadata such as id,
  # type, parent/root, and correlation id is restored from the current/snapshot
  # record when user-facing history is decoded.
  @history_flag_priority 1 <<< 0
  @history_flag_attempts 1 <<< 1
  @history_flag_fencing_token 1 <<< 2
  @history_flag_created_at_ms 1 <<< 3
  @history_flag_updated_at_ms 1 <<< 4
  @history_flag_next_run_at_ms 1 <<< 5
  @history_flag_lease_deadline_ms 1 <<< 6
  @history_flag_lease_owner 1 <<< 7
  @history_flag_payload_ref 1 <<< 8
  @history_flag_result_ref 1 <<< 9
  @history_flag_error_ref 1 <<< 10
  @history_flag_rewound_to_event_id 1 <<< 11
  @history_flag_meta 1 <<< 12

  def create(ctx, id, opts) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- create_attrs(id, opts) do
        ctx
        |> Router.flow_create(attrs)
        |> maybe_notify_claim_waiters(attrs, :state)
      end

    observe_flow(:create, started, result, %{
      flow_id: id,
      flow_type: Keyword.get(opts, :type),
      _count: 1
    })
  end

  def value_put(ctx, value, opts \\ [])

  def value_put(ctx, value, opts) when is_list(opts) do
    started = flow_start_time()

    result =
      with :ok <- validate_opts(opts),
           {:ok, partition_key} <- optional_partition_key(opts),
           {:ok, owner_flow_id} <- optional_binary_or_nil(opts, :owner_flow_id, nil),
           :ok <- validate_ref_size(:owner_flow_id, owner_flow_id),
           {:ok, name} <- optional_binary_or_nil(opts, :name, nil),
           :ok <- validate_ref_size(:name, name),
           {:ok, override?} <- optional_boolean(opts, :override, false),
           {:ok, now} <- optional_now_ms(opts),
           {:ok, ttl_ms} <- optional_pos_integer_or_nil(opts, :ttl_ms) do
        if is_binary(owner_flow_id) and is_binary(name) do
          attrs = %{
            id: owner_flow_id,
            name: name,
            value: value,
            partition_key: partition_key,
            override: override?
          }

          attrs = maybe_put_attr(attrs, :now_ms, now)
          Router.flow_named_value_put(ctx, attrs)
        else
          ref_id = shared_value_ref_id()
          ref = __MODULE__.Keys.value_key(ref_id, :shared, 1, partition_key)

          with :ok <- validate_key_size(ref),
               expire_at = flow_value_expire_at(now, ttl_ms),
               :ok <- Router.put(ctx, ref, encode_value(value), expire_at) do
            response = %{ref: ref, partition_key: partition_key}
            {:ok, maybe_put_attr(response, :owner_flow_id, owner_flow_id)}
          end
        end
      end

    observe_flow(:value_put, started, result, %{flow_id: Keyword.get(opts, :owner_flow_id)})
  end

  def value_put(_ctx, _value, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def value_mget(ctx, refs) when is_list(refs) do
    case flow_value_raw_mget(ctx, refs) do
      values when is_list(values) -> {:ok, Enum.map(values, &decode_value/1)}
      {:error, _reason} = error -> error
      other -> {:error, "ERR flow value mget failed: #{inspect(other)}"}
    end
  end

  def value_mget(_ctx, _refs), do: {:error, "ERR flow refs must be a list"}

  defp flow_value_raw_mget(_ctx, []), do: []

  defp flow_value_raw_mget(ctx, refs) do
    values =
      Stats.with_cache_tracking_disabled(fn ->
        Router.batch_get(ctx, refs)
      end)

    flow_value_fill_lmdb_missing(values, ctx, refs)
  end

  defp flow_value_raw_mget_with_file_refs(_ctx, [], _min_file_ref_size), do: []

  defp flow_value_raw_mget_with_file_refs(ctx, refs, min_file_ref_size) do
    values =
      Stats.with_cache_tracking_disabled(fn ->
        Router.batch_get_with_file_refs(ctx, refs, min_file_ref_size)
      end)

    flow_value_fill_lmdb_missing(values, ctx, refs)
  end

  defp flow_value_fill_lmdb_missing(values, ctx, refs)
       when is_list(values) and is_list(refs) and length(values) == length(refs) do
    missing =
      refs
      |> Enum.zip(values)
      |> Enum.with_index()
      |> Enum.flat_map(fn
        {{ref, nil}, idx} when is_binary(ref) ->
          if flow_generated_payload_value_ref?(ref), do: [{idx, ref}], else: []

        _entry ->
          []
      end)

    if missing == [] do
      values
    else
      lmdb_values =
        ctx
        |> flow_value_lmdb_mget(Enum.map(missing, fn {_idx, ref} -> ref end))
        |> List.to_tuple()

      replacements =
        missing
        |> Enum.with_index()
        |> Map.new(fn {{idx, _ref}, lmdb_idx} -> {idx, elem(lmdb_values, lmdb_idx)} end)

      values
      |> Enum.with_index()
      |> Enum.map(fn
        {nil, idx} -> Map.get(replacements, idx)
        {value, _idx} -> value
      end)
    end
  end

  defp flow_value_fill_lmdb_missing(values, _ctx, _refs), do: values

  defp flow_value_lmdb_mget(_ctx, []), do: []

  defp flow_value_lmdb_mget(ctx, refs) do
    now = now_ms()

    results =
      refs
      |> Enum.with_index()
      |> Enum.group_by(fn {ref, _idx} -> flow_value_lmdb_path(ctx, ref) end)
      |> Enum.reduce(%{}, fn {path, group}, acc ->
        group_refs = Enum.map(group, fn {ref, _idx} -> ref end)

        lmdb_values =
          case Ferricstore.Flow.LMDB.get_many(path, group_refs) do
            {:ok, values} -> values
            {:error, _reason} -> Enum.map(group_refs, fn _ref -> :not_found end)
          end

        group
        |> Enum.zip(lmdb_values)
        |> Enum.reduce(acc, fn {{ref, idx}, lmdb_value}, inner_acc ->
          Map.put(inner_acc, idx, flow_value_lmdb_decode(ctx, ref, lmdb_value, now))
        end)
      end)

    for idx <- 0..(length(refs) - 1)//1, do: Map.get(results, idx)
  end

  defp flow_value_lmdb_path(ctx, ref) do
    shard_index = Router.shard_for(ctx, ref)

    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> Ferricstore.Flow.LMDB.path()
  end

  defp flow_value_lmdb_decode(ctx, ref, {:ok, blob}, now) when is_binary(blob) do
    case Ferricstore.Flow.LMDB.decode_value_locator(blob, now) do
      {:ok, locator} ->
        flow_value_read_lmdb_locator(ctx, ref, locator)

      :not_locator ->
        case Ferricstore.Flow.LMDB.decode_value(blob, now) do
          {:ok, value} -> flow_value_maybe_materialize_lmdb_value(ctx, ref, value)
          _other -> nil
        end

      _other ->
        nil
    end
  end

  defp flow_value_lmdb_decode(_ctx, _ref, _result, _now), do: nil

  defp flow_value_maybe_materialize_lmdb_value(ctx, ref, value) when is_binary(value) do
    shard_index = Router.shard_for(ctx, ref)

    case BlobValue.maybe_materialize(
           ctx.data_dir,
           shard_index,
           BlobValue.threshold(ctx),
           value
         ) do
      {:ok, materialized} -> materialized
      {:error, _reason} -> nil
    end
  end

  defp flow_value_maybe_materialize_lmdb_value(_ctx, _ref, value), do: value

  defp flow_value_read_lmdb_locator(ctx, key, {file_id, offset, _value_size}) do
    shard_index = Router.shard_for(ctx, key)

    with {:ok, value} <- flow_value_read_locator_bytes(ctx, shard_index, key, file_id, offset),
         {:ok, materialized} <-
           BlobValue.maybe_materialize(
             ctx.data_dir,
             shard_index,
             BlobValue.threshold(ctx),
             value
           ) do
      materialized
    else
      _error -> nil
    end
  end

  defp flow_value_read_locator_bytes(ctx, shard_index, key, file_id, offset)
       when is_integer(file_id) and file_id >= 0 do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> ShardETS.file_path(file_id)
    |> ColdRead.pread_at(offset, key, 10_000)
  end

  defp flow_value_read_locator_bytes(
         ctx,
         shard_index,
         _key,
         {:flow_history, _file_id} = file_id,
         offset
       ) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> Ferricstore.Flow.HistoryProjector.read_value(file_id, offset)
  end

  defp flow_value_read_locator_bytes(ctx, shard_index, key, file_id, _offset)
       when is_tuple(file_id) do
    Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(ctx, shard_index, file_id, key)
  end

  defp flow_value_read_locator_bytes(_ctx, _shard_index, _key, _file_id, _offset),
    do: {:error, :bad_flow_value_locator}

  defp flow_generated_payload_value_ref?("f:" <> _rest = ref) do
    case :binary.split(ref, ":v:") do
      ["f:" <> tag, <<kind, ?:, rest::binary>>]
      when byte_size(tag) > 0 and kind in [?p, ?r, ?e] and byte_size(rest) > 0 ->
        true

      _other ->
        false
    end
  end

  defp flow_generated_payload_value_ref?(_ref), do: false

  def signal(ctx, id, opts) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- signal_attrs(id, opts) do
        Router.flow_signal(ctx, attrs)
      end

    observe_flow(:signal, started, result, %{flow_id: id, _count: 1})
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

    observe_flow(:create, started, result, %{
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

    observe_flow(:spawn_children, started, result, %{flow_id: parent_id, _count: 1})
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

    result =
      with {:ok, block_ms} <- optional_claim_block_ms(opts) do
        claim_opts = Keyword.delete(opts, :block_ms)

        if block_ms > 0 do
          claim_due_blocking_result(ctx, type, claim_opts, block_ms)
        else
          claim_due_result(ctx, type, claim_opts)
        end
      end

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

      case claim_due_router_result(ctx, attrs, reclaim_expired?, reclaim_ratio) do
        {:ok, records} when is_list(records) ->
          {:ok, claim_due_return_records(ctx, records, payload_return, return_mode, named_values)}

        other ->
          other
      end
    end
  end

  defp claim_due_blocking_result(ctx, type, opts, block_ms) do
    case claim_due_result(ctx, type, opts) do
      {:ok, [_ | _]} = claimed ->
        claimed

      {:ok, []} ->
        with {:ok, keys, limit} <- claim_due_wait_registration(type, opts) do
          deadline = System.monotonic_time(:millisecond) + block_ms
          ClaimWaiters.register(keys, self(), deadline, limit: limit)

          try do
            case claim_due_result(ctx, type, opts) do
              {:ok, [_ | _]} = claimed -> claimed
              {:ok, []} -> claim_due_wait_loop(ctx, type, opts, deadline)
              other -> other
            end
          after
            ClaimWaiters.unregister(keys, self())
          end
        end

      other ->
        other
    end
  end

  defp claim_due_wait_loop(ctx, type, opts, deadline) do
    remaining = max(0, deadline - System.monotonic_time(:millisecond))
    waiter_message = ClaimWaiters.message()

    receive do
      {^waiter_message, _key} ->
        case claim_due_result(ctx, type, opts) do
          {:ok, []} ->
            if System.monotonic_time(:millisecond) >= deadline do
              {:ok, []}
            else
              claim_due_wait_loop(ctx, type, opts, deadline)
            end

          other ->
            other
        end
    after
      remaining ->
        {:ok, []}
    end
  end

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
      |> ClaimWaiters.notify_ready_many(@claim_waiter_max_wake_per_ready_bucket)
    end

    result
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

  defp claim_due_return_records(ctx, records, payload_return, return_mode),
    do: claim_due_return_records(ctx, records, payload_return, return_mode, nil)

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

    observe_flow(:extend_lease, started, result, %{flow_id: id, _count: 1})
  end

  def complete(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- complete_attrs(id, lease_token, opts) do
        Router.flow_complete(ctx, attrs)
      end

    observe_flow(:complete, started, result, %{flow_id: id, _count: 1})
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

    observe_flow(:complete, started, result, %{flow_id: nil, _count: length(items)})
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

    observe_flow(:transition, started, result, %{
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

    observe_flow(:transition, started, result, %{
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
    observe_flow_batch(:transition, started, results)
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

    observe_flow_batch(:pipeline_write, started, results)
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
        ctx
        |> Router.flow_retry(attrs)
        |> maybe_notify_claim_waiters(attrs, :any)
      end

    observe_flow(:retry, started, result, %{flow_id: id, _count: 1})
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
          |> maybe_notify_claim_waiters(attrs_list, :any)
        else
          ctx
          |> Router.flow_retry_many(partition_key, attrs_list)
          |> maybe_notify_claim_waiters(attrs_list, :any)
        end
      end

    observe_flow(:retry, started, result, %{flow_id: nil, _count: length(items)})
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

    observe_flow(:fail, started, result, %{flow_id: id, _count: 1})
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

    observe_flow(:fail, started, result, %{flow_id: nil, _count: length(items)})
  end

  def fail_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def cancel(ctx, id, opts \\ []) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- cancel_attrs(id, opts) do
        Router.flow_cancel(ctx, attrs)
      end

    observe_flow(:cancel, started, result, %{flow_id: id, _count: 1})
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

    observe_flow(:cancel, started, result, %{flow_id: nil, _count: length(items)})
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
        ctx
        |> Router.flow_rewind(attrs)
        |> maybe_notify_claim_waiters(attrs, :any)
      end

    observe_flow(:rewind, started, result, %{flow_id: id, _count: 1})
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
         {:ok, records} <-
           flow_terminal_records(
             ctx,
             type,
             state,
             partition_key,
             fetch_count,
             include_cold? or consistent_projection?,
             consistent_projection?
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
                 flow_history_lmdb_reverse_scan?(query)
               ) do
          scan_count = flow_lmdb_query_scan_count(fetch_count)

          event_ids =
            (hot_refs ++ cold_refs)
            |> Enum.sort_by(fn {event_id, score} -> {score, event_id} end)
            |> Enum.uniq_by(fn {event_id, _score} -> event_id end)
            |> Enum.take(-flow_history_merge_count(query, scan_count))
            |> Enum.map(fn {event_id, _score} -> event_id end)

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

  defp flow_history_merge_count(query, scan_count) do
    if flow_history_query_filtering?(query), do: scan_count, else: query.count
  end

  defp flow_history_lmdb_reverse_scan?(query), do: not flow_history_query_filtering?(query)

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

  defp flow_history_lmdb_refs(_ctx, _history_key, count, _consistent?, _reverse?) when count <= 0,
    do: {:ok, []}

  defp flow_history_lmdb_refs(ctx, history_key, count, consistent?, reverse?) do
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
               flow_history_lmdb_query_scan_count(count, reverse?),
               reverse?
             ) do
        {:ok, flow_decode_history_index_entries(entries, path, now_ms)}
      end
    end
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
          [{event_id, decode_history_fields(value, decode_context)}]
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
          [{event_id, decode_history_fields(value, decode_context)}]

        {event_id, value} when is_binary(event_id) and is_binary(value) ->
          [{event_id, decode_history_fields(value, decode_context)}]

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
      flow_history_lmdb_query_scan_count(count)
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
    events
    |> Enum.filter(&flow_history_event_matches?(&1, query))
    |> maybe_reverse_history(query)
    |> Enum.take(query.count)
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

  defp maybe_reverse_history(events, %{rev?: true}), do: Enum.reverse(events)
  defp maybe_reverse_history(events, _query), do: events

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
         consistent?
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
             consistent?
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
         consistent?
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
             consistent?
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

  defp flow_terminal_records(ctx, type, state, partition_key, count, include_cold?, consistent?) do
    with {:ok, ids} <-
           flow_terminal_ids(ctx, type, state, partition_key, count, include_cold?, consistent?),
         {:ok, records} <- flow_records_for_ids(ctx, ids, partition_key) do
      {:ok, Enum.filter(records, &(Map.get(&1, :state) == state))}
    end
  end

  defp flow_terminal_ids(ctx, type, state, partition_key, count, include_cold?, consistent?) do
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
             consistent?
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
  # here; only payload_ref/result_ref/error_ref metadata is stored. Flow records
  # are not public-persisted yet, so this intentionally supports one current
  # format.
  def encode_record(record) when is_map(record) do
    NIF.flow_record_encode(
      Map.get(record, :id),
      Map.get(record, :type),
      Map.get(record, :state),
      Map.get(record, :version),
      Map.get(record, :attempts),
      Map.get(record, :fencing_token),
      Map.get(record, :created_at_ms),
      Map.get(record, :updated_at_ms),
      Map.get(record, :next_run_at_ms),
      Map.get(record, :priority),
      Map.get(record, :ttl_ms),
      Map.get(record, :history_hot_max_events),
      Map.get(record, :history_max_events),
      Map.get(record, :retention_ttl_ms),
      Map.get(record, :terminal_retention_until_ms),
      Map.get(record, :partition_key),
      Map.get(record, :payload_ref),
      Map.get(record, :parent_flow_id),
      Map.get(record, :parent_partition_key),
      Map.get(record, :root_flow_id),
      Map.get(record, :correlation_id),
      Map.get(record, :result_ref),
      Map.get(record, :error_ref),
      Map.get(record, :lease_owner),
      Map.get(record, :lease_token),
      Map.get(record, :lease_deadline_ms),
      Map.get(record, :run_state),
      Map.get(record, :rewound_to_event_id),
      record |> encode_record_sidecar() |> IO.iodata_to_binary()
    )
  end

  @doc false
  def encode_record_elixir(record) when is_map(record) do
    sidecar =
      record
      |> encode_record_sidecar()
      |> IO.iodata_to_binary()

    flags = encode_record_flags(record, sidecar)

    # Wire order is part of the durable schema. Add new fields as flagged
    # trailing data or bump @record_bin_magic and update the Rust NIF/test
    # parity checks.
    [
      @record_bin_magic,
      encode_int(flags),
      encode_bin(Map.get(record, :id)),
      encode_bin(Map.get(record, :type)),
      encode_bin(Map.get(record, :state)),
      encode_int(Map.get(record, :version)),
      encode_int(Map.get(record, :created_at_ms)),
      encode_int(Map.get(record, :updated_at_ms)),
      encode_flagged_int(flags, @record_flag_attempts, Map.get(record, :attempts)),
      encode_flagged_int(flags, @record_flag_fencing_token, Map.get(record, :fencing_token)),
      encode_flagged_int(flags, @record_flag_next_run_at_ms, Map.get(record, :next_run_at_ms)),
      encode_flagged_int(flags, @record_flag_priority, Map.get(record, :priority)),
      encode_flagged_int(flags, @record_flag_ttl_ms, Map.get(record, :ttl_ms)),
      encode_flagged_int(
        flags,
        @record_flag_history_hot_max_events,
        Map.get(record, :history_hot_max_events)
      ),
      encode_flagged_int(
        flags,
        @record_flag_history_max_events,
        Map.get(record, :history_max_events)
      ),
      encode_flagged_int(
        flags,
        @record_flag_retention_ttl_ms,
        Map.get(record, :retention_ttl_ms)
      ),
      encode_flagged_int(
        flags,
        @record_flag_terminal_retention_until_ms,
        Map.get(record, :terminal_retention_until_ms)
      ),
      encode_flagged_bin(flags, @record_flag_partition_key, Map.get(record, :partition_key)),
      encode_flagged_bin(flags, @record_flag_payload_ref, Map.get(record, :payload_ref)),
      encode_flagged_bin(flags, @record_flag_parent_flow_id, Map.get(record, :parent_flow_id)),
      encode_flagged_bin(
        flags,
        @record_flag_parent_partition_key,
        Map.get(record, :parent_partition_key)
      ),
      encode_flagged_bin(flags, @record_flag_root_flow_id, Map.get(record, :root_flow_id)),
      encode_flagged_bin(flags, @record_flag_correlation_id, Map.get(record, :correlation_id)),
      encode_flagged_bin(flags, @record_flag_result_ref, Map.get(record, :result_ref)),
      encode_flagged_bin(flags, @record_flag_error_ref, Map.get(record, :error_ref)),
      encode_flagged_bin(flags, @record_flag_lease_owner, Map.get(record, :lease_owner)),
      encode_flagged_bin(flags, @record_flag_lease_token, Map.get(record, :lease_token)),
      encode_flagged_int(
        flags,
        @record_flag_lease_deadline_ms,
        Map.get(record, :lease_deadline_ms)
      ),
      encode_flagged_bin(flags, @record_flag_run_state, Map.get(record, :run_state)),
      encode_flagged_bin(
        flags,
        @record_flag_rewound_to_event_id,
        Map.get(record, :rewound_to_event_id)
      ),
      if((flags &&& @record_flag_sidecar) != 0, do: sidecar, else: [])
    ]
    |> IO.iodata_to_binary()
  end

  defp encode_record_flags(record, sidecar) do
    0
    |> record_flag_int(record, :attempts, @record_flag_attempts, 0)
    |> record_flag_int(record, :fencing_token, @record_flag_fencing_token, 0)
    |> record_flag_int(record, :next_run_at_ms, @record_flag_next_run_at_ms, nil)
    |> record_flag_int(record, :priority, @record_flag_priority, 0)
    |> record_flag_int(record, :ttl_ms, @record_flag_ttl_ms, nil)
    |> record_flag_int(record, :history_hot_max_events, @record_flag_history_hot_max_events, nil)
    |> record_flag_int(record, :history_max_events, @record_flag_history_max_events, nil)
    |> record_flag_int(record, :retention_ttl_ms, @record_flag_retention_ttl_ms, nil)
    |> record_flag_int(
      record,
      :terminal_retention_until_ms,
      @record_flag_terminal_retention_until_ms,
      nil
    )
    |> record_flag_bin(record, :partition_key, @record_flag_partition_key)
    |> record_flag_bin(record, :payload_ref, @record_flag_payload_ref)
    |> record_flag_bin(record, :parent_flow_id, @record_flag_parent_flow_id)
    |> record_flag_bin(record, :parent_partition_key, @record_flag_parent_partition_key)
    |> record_flag_root(record)
    |> record_flag_bin(record, :correlation_id, @record_flag_correlation_id)
    |> record_flag_bin(record, :result_ref, @record_flag_result_ref)
    |> record_flag_bin(record, :error_ref, @record_flag_error_ref)
    |> record_flag_bin(record, :lease_owner, @record_flag_lease_owner)
    |> record_flag_bin(record, :lease_token, @record_flag_lease_token)
    |> record_flag_int(record, :lease_deadline_ms, @record_flag_lease_deadline_ms, 0)
    |> record_flag_bin(record, :run_state, @record_flag_run_state)
    |> record_flag_bin(record, :rewound_to_event_id, @record_flag_rewound_to_event_id)
    |> maybe_put_flag(@record_flag_sidecar, not record_empty_sidecar?(sidecar))
  end

  defp record_flag_int(flags, record, key, flag, omitted_default) do
    value = Map.get(record, key)
    maybe_put_flag(flags, flag, not is_nil(value) and value != omitted_default)
  end

  defp record_flag_bin(flags, record, key, flag) do
    maybe_put_flag(flags, flag, is_binary(Map.get(record, key)))
  end

  defp record_flag_root(flags, record) do
    root_flow_id = Map.get(record, :root_flow_id)
    id = Map.get(record, :id)

    # Most root flows point to themselves. Store that common case as a flag
    # instead of repeating the id bytes in every state record.
    cond do
      is_binary(root_flow_id) and root_flow_id == id ->
        flags ||| @record_flag_root_flow_id_self

      is_binary(root_flow_id) ->
        flags ||| @record_flag_root_flow_id

      true ->
        flags
    end
  end

  defp maybe_put_flag(flags, flag, true), do: flags ||| flag
  defp maybe_put_flag(flags, _flag, _false), do: flags

  defp nonempty_binary?(value), do: is_binary(value) and value != ""

  defp encode_flagged_int(flags, flag, value) do
    if (flags &&& flag) != 0, do: encode_int(value), else: []
  end

  defp encode_flagged_bin(flags, flag, value) do
    if (flags &&& flag) != 0, do: encode_bin(value), else: []
  end

  @doc false
  def encode_value(@value_bin_magic <> _rest = value), do: @value_bin_magic <> <<1>> <> value

  def encode_value(value) when is_binary(value), do: value
  def encode_value(value), do: @value_bin_magic <> <<2>> <> :erlang.term_to_binary(value)

  @doc false
  def decode_value(@value_bin_magic <> <<1, encoded::binary>>), do: encoded

  def decode_value(@value_bin_magic <> <<2, encoded::binary>>) do
    :erlang.binary_to_term(encoded, [:safe])
  rescue
    _ -> encoded
  end

  def decode_value(value), do: value

  defp decode_value_with_user_size(@value_bin_magic <> <<1, encoded::binary>>) do
    {encoded, byte_size(encoded)}
  end

  defp decode_value_with_user_size(@value_bin_magic <> <<2, encoded::binary>>) do
    {:erlang.binary_to_term(encoded, [:safe]), byte_size(encoded)}
  rescue
    _ -> {encoded, byte_size(encoded)}
  end

  defp decode_value_with_user_size(value) when is_binary(value), do: {value, byte_size(value)}
  defp decode_value_with_user_size(value), do: {value, 0}

  @doc false
  # Flow has not shipped as a public durable format yet, so recovery accepts
  # only the current compact record encoding.
  def record_blob?(@record_bin_magic <> _rest), do: true
  def record_blob?(_value), do: false

  def decode_record(@record_bin_magic <> _rest = value) do
    case NIF.flow_record_decode(value) do
      {:ok, fields} -> decode_record_fields(fields)
      _ -> raise(ArgumentError, "invalid flow record")
    end
  end

  def decode_record(_value), do: raise(ArgumentError, "invalid flow record")

  @doc false
  def decode_record_elixir(@record_bin_magic <> rest), do: decode_record_bin(rest)

  def decode_record_elixir(_value), do: raise(ArgumentError, "invalid flow record")

  @doc false
  # History entries have their own durable schema because they are retained for
  # audit/debug and rewind.
  def encode_history_fields(record, event, now_ms, meta \\ %{})
      when is_map(record) and is_binary(event) and is_integer(now_ms) do
    encode_history_parts(
      event,
      Map.get(record, :version),
      now_ms,
      Map.get(record, :id),
      Map.get(record, :type),
      Map.get(record, :state),
      Map.get(record, :priority, 0),
      Map.get(record, :attempts, 0),
      Map.get(record, :fencing_token, 0),
      Map.get(record, :created_at_ms, now_ms),
      Map.get(record, :updated_at_ms, now_ms),
      Map.get(record, :next_run_at_ms),
      Map.get(record, :lease_deadline_ms),
      Map.get(record, :lease_owner),
      Map.get(record, :payload_ref),
      Map.get(record, :parent_flow_id),
      Map.get(record, :root_flow_id),
      Map.get(record, :correlation_id),
      Map.get(record, :result_ref),
      Map.get(record, :error_ref),
      Map.get(record, :rewound_to_event_id),
      normalize_history_meta(record_history_meta(record, meta))
    )
  end

  @doc false
  def encode_history_fields_elixir(record, event, now_ms, meta \\ %{})
      when is_map(record) and is_binary(event) and is_integer(now_ms) do
    encode_history_parts_elixir(
      event,
      Map.get(record, :version),
      now_ms,
      Map.get(record, :id),
      Map.get(record, :type),
      Map.get(record, :state),
      Map.get(record, :priority, 0),
      Map.get(record, :attempts, 0),
      Map.get(record, :fencing_token, 0),
      Map.get(record, :created_at_ms, now_ms),
      Map.get(record, :updated_at_ms, now_ms),
      Map.get(record, :next_run_at_ms),
      Map.get(record, :lease_deadline_ms),
      Map.get(record, :lease_owner),
      Map.get(record, :payload_ref),
      Map.get(record, :parent_flow_id),
      Map.get(record, :root_flow_id),
      Map.get(record, :correlation_id),
      Map.get(record, :result_ref),
      Map.get(record, :error_ref),
      Map.get(record, :rewound_to_event_id),
      normalize_history_meta(record_history_meta(record, meta))
    )
  end

  @doc false
  def history_snapshot(record, event, now_ms, meta \\ %{})
      when is_map(record) and is_binary(event) and is_integer(now_ms) do
    {
      event,
      Map.get(record, :version),
      now_ms,
      Map.get(record, :id),
      Map.get(record, :type),
      Map.get(record, :state),
      Map.get(record, :priority, 0),
      Map.get(record, :attempts, 0),
      Map.get(record, :fencing_token, 0),
      Map.get(record, :created_at_ms, now_ms),
      Map.get(record, :updated_at_ms, now_ms),
      Map.get(record, :next_run_at_ms),
      Map.get(record, :lease_deadline_ms),
      Map.get(record, :lease_owner),
      Map.get(record, :payload_ref),
      Map.get(record, :parent_flow_id),
      Map.get(record, :root_flow_id),
      Map.get(record, :correlation_id),
      Map.get(record, :result_ref),
      Map.get(record, :error_ref),
      Map.get(record, :rewound_to_event_id),
      normalize_history_meta(record_history_meta(record, meta))
    }
  end

  @doc false
  def encode_history_snapshot({
        event,
        version,
        now_ms,
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
    encode_history_parts(
      event,
      version,
      now_ms,
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
    )
  end

  defp encode_history_parts(
         event,
         version,
         now_ms,
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
       ) do
    NIF.flow_history_encode(
      event,
      version,
      now_ms,
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
      meta_fields |> encode_history_meta() |> IO.iodata_to_binary()
    )
  end

  defp encode_history_parts_elixir(
         event,
         version,
         now_ms,
         _id,
         _type,
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
         _parent_flow_id,
         _root_flow_id,
         _correlation_id,
         result_ref,
         error_ref,
         rewound_to_event_id,
         meta_fields
       ) do
    flags =
      encode_history_flags(
        priority,
        attempts,
        fencing_token,
        created_at_ms,
        updated_at_ms,
        now_ms,
        next_run_at_ms,
        lease_deadline_ms,
        lease_owner,
        payload_ref,
        result_ref,
        error_ref,
        rewound_to_event_id,
        meta_fields
      )

    # History entries intentionally omit immutable workflow identity fields.
    # decode_history_fields/2 must get record context when callers need the full
    # RESP-facing history shape.
    [
      @history_bin_magic,
      encode_int(flags),
      encode_bin(event),
      encode_int(version),
      encode_int(now_ms),
      encode_bin(state),
      encode_flagged_int(flags, @history_flag_priority, priority),
      encode_flagged_int(flags, @history_flag_attempts, attempts),
      encode_flagged_int(flags, @history_flag_fencing_token, fencing_token),
      encode_flagged_int(flags, @history_flag_created_at_ms, created_at_ms),
      encode_flagged_int(flags, @history_flag_updated_at_ms, updated_at_ms),
      encode_flagged_int(flags, @history_flag_next_run_at_ms, next_run_at_ms),
      encode_flagged_int(flags, @history_flag_lease_deadline_ms, lease_deadline_ms),
      encode_flagged_bin(flags, @history_flag_lease_owner, lease_owner),
      encode_flagged_bin(flags, @history_flag_payload_ref, payload_ref),
      encode_flagged_bin(flags, @history_flag_result_ref, result_ref),
      encode_flagged_bin(flags, @history_flag_error_ref, error_ref),
      encode_flagged_bin(flags, @history_flag_rewound_to_event_id, rewound_to_event_id),
      if((flags &&& @history_flag_meta) != 0, do: encode_history_meta(meta_fields), else: [])
    ]
    |> IO.iodata_to_binary()
  end

  defp encode_history_flags(
         priority,
         attempts,
         fencing_token,
         created_at_ms,
         updated_at_ms,
         now_ms,
         next_run_at_ms,
         lease_deadline_ms,
         lease_owner,
         payload_ref,
         result_ref,
         error_ref,
         rewound_to_event_id,
         meta_fields
       ) do
    0
    |> maybe_put_flag(@history_flag_priority, is_integer(priority) and priority != 0)
    |> maybe_put_flag(@history_flag_attempts, is_integer(attempts) and attempts != 0)
    |> maybe_put_flag(
      @history_flag_fencing_token,
      is_integer(fencing_token) and fencing_token != 0
    )
    |> maybe_put_flag(
      @history_flag_created_at_ms,
      is_integer(created_at_ms) and created_at_ms != now_ms
    )
    |> maybe_put_flag(
      @history_flag_updated_at_ms,
      is_integer(updated_at_ms) and updated_at_ms != now_ms
    )
    |> maybe_put_flag(@history_flag_next_run_at_ms, is_integer(next_run_at_ms))
    |> maybe_put_flag(
      @history_flag_lease_deadline_ms,
      is_integer(lease_deadline_ms) and lease_deadline_ms != 0
    )
    |> maybe_put_flag(@history_flag_lease_owner, nonempty_binary?(lease_owner))
    |> maybe_put_flag(@history_flag_payload_ref, nonempty_binary?(payload_ref))
    |> maybe_put_flag(@history_flag_result_ref, nonempty_binary?(result_ref))
    |> maybe_put_flag(@history_flag_error_ref, nonempty_binary?(error_ref))
    |> maybe_put_flag(@history_flag_rewound_to_event_id, nonempty_binary?(rewound_to_event_id))
    |> maybe_put_flag(@history_flag_meta, is_list(meta_fields) and meta_fields != [])
  end

  @doc false
  # Decode history into the current RESP-facing field list. FSH2 callers should
  # pass the state record/context so omitted immutable fields can be restored.
  def decode_history_fields(value, context \\ %{})

  def decode_history_fields(@history_bin_magic <> rest, context),
    do: decode_history_fields_bin(rest, context)

  def decode_history_fields(_value, _context), do: []

  @doc false
  def decode_history_fields_elixir(value, context \\ %{})

  def decode_history_fields_elixir(@history_bin_magic <> rest, context),
    do: decode_history_fields_bin(rest, context)

  def decode_history_fields_elixir(_value, _context), do: []

  defp decode_history_fields_term(
         {
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
         },
         context
       ) do
    id = history_context_string(context, :id, id)
    type = history_context_string(context, :type, type)
    parent_flow_id = history_context_string(context, :parent_flow_id, parent_flow_id)
    root_flow_id = history_context_string(context, :root_flow_id, root_flow_id)
    correlation_id = history_context_string(context, :correlation_id, correlation_id)

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

  defp decode_history_fields_term(_value, _context), do: []

  defp decode_record_bin(rest) do
    with {:ok, flags, rest} <- decode_int(rest),
         flags when is_integer(flags) <- flags,
         {:ok, id, rest} <- decode_bin(rest),
         {:ok, type, rest} <- decode_bin(rest),
         {:ok, state, rest} <- decode_bin(rest),
         {:ok, version, rest} <- decode_int(rest),
         {:ok, created_at_ms, rest} <- decode_int(rest),
         {:ok, updated_at_ms, rest} <- decode_int(rest),
         {:ok, attempts, rest} <- decode_flagged_int(flags, @record_flag_attempts, rest, 0),
         {:ok, fencing_token, rest} <-
           decode_flagged_int(flags, @record_flag_fencing_token, rest, 0),
         {:ok, next_run_at_ms, rest} <-
           decode_flagged_int(flags, @record_flag_next_run_at_ms, rest, nil),
         {:ok, priority, rest} <- decode_flagged_int(flags, @record_flag_priority, rest, 0),
         {:ok, ttl_ms, rest} <- decode_flagged_int(flags, @record_flag_ttl_ms, rest, nil),
         {:ok, history_hot_max_events, rest} <-
           decode_flagged_int(flags, @record_flag_history_hot_max_events, rest, nil),
         {:ok, history_max_events, rest} <-
           decode_flagged_int(flags, @record_flag_history_max_events, rest, nil),
         {:ok, retention_ttl_ms, rest} <-
           decode_flagged_int(flags, @record_flag_retention_ttl_ms, rest, nil),
         {:ok, terminal_retention_until_ms, rest} <-
           decode_flagged_int(flags, @record_flag_terminal_retention_until_ms, rest, nil),
         {:ok, partition_key, rest} <-
           decode_flagged_bin(flags, @record_flag_partition_key, rest, nil),
         {:ok, payload_ref, rest} <-
           decode_flagged_bin(flags, @record_flag_payload_ref, rest, nil),
         {:ok, parent_flow_id, rest} <-
           decode_flagged_bin(flags, @record_flag_parent_flow_id, rest, nil),
         {:ok, parent_partition_key, rest} <-
           decode_flagged_bin(flags, @record_flag_parent_partition_key, rest, nil),
         {:ok, root_flow_id, rest} <- decode_record_root(flags, id, rest),
         {:ok, correlation_id, rest} <-
           decode_flagged_bin(flags, @record_flag_correlation_id, rest, nil),
         {:ok, result_ref, rest} <- decode_flagged_bin(flags, @record_flag_result_ref, rest, nil),
         {:ok, error_ref, rest} <- decode_flagged_bin(flags, @record_flag_error_ref, rest, nil),
         {:ok, lease_owner, rest} <-
           decode_flagged_bin(flags, @record_flag_lease_owner, rest, nil),
         {:ok, lease_token, rest} <-
           decode_flagged_bin(flags, @record_flag_lease_token, rest, nil),
         {:ok, lease_deadline_ms, rest} <-
           decode_flagged_int(flags, @record_flag_lease_deadline_ms, rest, 0),
         {:ok, run_state, rest} <- decode_flagged_bin(flags, @record_flag_run_state, rest, nil),
         {:ok, rewound_to_event_id, rest} <-
           decode_flagged_bin(flags, @record_flag_rewound_to_event_id, rest, nil),
         {:ok, child_groups, ""} <- decode_record_sidecar(flags, rest) do
      {child_groups, value_refs} = split_record_sidecar(child_groups)

      record =
        %{
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
        |> maybe_put_decoded_value_refs(value_refs)

      if is_nil(rewound_to_event_id) do
        record
      else
        Map.put(record, :rewound_to_event_id, rewound_to_event_id)
      end
    else
      _ -> raise ArgumentError, "invalid flow record"
    end
  end

  defp decode_flagged_int(flags, flag, rest, default) do
    if (flags &&& flag) != 0 do
      decode_int(rest)
    else
      {:ok, default, rest}
    end
  end

  defp decode_flagged_bin(flags, flag, rest, default) do
    if (flags &&& flag) != 0 do
      decode_bin(rest)
    else
      {:ok, default, rest}
    end
  end

  defp decode_record_root(flags, id, rest) do
    cond do
      (flags &&& @record_flag_root_flow_id_self) != 0 ->
        {:ok, id, rest}

      (flags &&& @record_flag_root_flow_id) != 0 ->
        decode_bin(rest)

      true ->
        {:ok, nil, rest}
    end
  end

  defp decode_record_sidecar(flags, rest) do
    if (flags &&& @record_flag_sidecar) != 0 do
      decode_child_groups(rest)
    else
      empty = record_empty_sidecar()
      {:ok, child_groups, ""} = decode_child_groups(empty)
      {:ok, child_groups, rest}
    end
  end

  defp decode_record_fields([
         id,
         type,
         state,
         version,
         attempts,
         fencing_token,
         created_at_ms,
         updated_at_ms,
         next_run_at_ms,
         priority,
         ttl_ms,
         history_hot_max_events,
         history_max_events,
         retention_ttl_ms,
         terminal_retention_until_ms,
         partition_key,
         payload_ref,
         parent_flow_id,
         parent_partition_key,
         root_flow_id,
         correlation_id,
         result_ref,
         error_ref,
         lease_owner,
         lease_token,
         lease_deadline_ms,
         run_state,
         rewound_to_event_id,
         child_groups_encoded
       ])
       when is_binary(child_groups_encoded) do
    with {:ok, child_groups, ""} <- decode_child_groups(child_groups_encoded) do
      {child_groups, value_refs} = split_record_sidecar(child_groups)

      record =
        %{
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
        |> maybe_put_decoded_value_refs(value_refs)

      if is_nil(rewound_to_event_id) do
        record
      else
        Map.put(record, :rewound_to_event_id, rewound_to_event_id)
      end
    else
      _ -> raise ArgumentError, "invalid flow record"
    end
  end

  defp decode_record_fields(_fields), do: raise(ArgumentError, "invalid flow record")

  defp maybe_put_decoded_value_refs(record, refs) when is_map(refs) and map_size(refs) > 0,
    do: Map.put(record, :value_refs, refs)

  defp maybe_put_decoded_value_refs(record, _refs), do: record

  defp decode_history_fields_bin(rest, context) do
    with {:ok, flags, rest} <- decode_int(rest),
         flags when is_integer(flags) <- flags,
         {:ok, event, rest} <- decode_bin(rest),
         {:ok, version, rest} <- decode_int(rest),
         {:ok, at, rest} <- decode_int(rest),
         {:ok, state, rest} <- decode_bin(rest),
         {:ok, priority, rest} <- decode_flagged_int(flags, @history_flag_priority, rest, 0),
         {:ok, attempts, rest} <- decode_flagged_int(flags, @history_flag_attempts, rest, 0),
         {:ok, fencing_token, rest} <-
           decode_flagged_int(flags, @history_flag_fencing_token, rest, 0),
         {:ok, created_at_ms, rest} <-
           decode_flagged_int(flags, @history_flag_created_at_ms, rest, at),
         {:ok, updated_at_ms, rest} <-
           decode_flagged_int(flags, @history_flag_updated_at_ms, rest, at),
         {:ok, next_run_at_ms, rest} <-
           decode_flagged_int(flags, @history_flag_next_run_at_ms, rest, nil),
         {:ok, lease_deadline_ms, rest} <-
           decode_flagged_int(flags, @history_flag_lease_deadline_ms, rest, nil),
         {:ok, lease_owner, rest} <-
           decode_flagged_bin(flags, @history_flag_lease_owner, rest, nil),
         {:ok, payload_ref, rest} <-
           decode_flagged_bin(flags, @history_flag_payload_ref, rest, nil),
         {:ok, result_ref, rest} <- decode_flagged_bin(flags, @history_flag_result_ref, rest, nil),
         {:ok, error_ref, rest} <- decode_flagged_bin(flags, @history_flag_error_ref, rest, nil),
         {:ok, rewound_to_event_id, rest} <-
           decode_flagged_bin(flags, @history_flag_rewound_to_event_id, rest, nil),
         {:ok, meta_fields, ""} <- decode_history_meta_for_flags(flags, rest) do
      decode_history_fields_term(
        {
          @history_tag,
          event,
          version,
          at,
          nil,
          nil,
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
          nil,
          nil,
          nil,
          result_ref,
          error_ref,
          rewound_to_event_id,
          meta_fields
        },
        context
      )
    else
      _ -> []
    end
  end

  defp decode_history_meta_for_flags(flags, rest) do
    if (flags &&& @history_flag_meta) != 0 do
      decode_history_meta(rest)
    else
      {:ok, [], rest}
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

  defp record_history_meta(record, meta) when is_map(record) and is_map(meta) do
    refs = flow_record_value_refs(record)

    if map_size(refs) == 0 do
      meta
    else
      Map.put_new(meta, @history_value_refs_key, Jason.encode!(encode_value_refs(refs)))
    end
  end

  defp record_history_meta(_record, meta), do: meta

  defp history_meta_pair(key, nil), do: [{key, ""}]
  defp history_meta_pair(key, value) when is_binary(value), do: [{key, value}]
  defp history_meta_pair(key, value) when is_atom(value), do: [{key, Atom.to_string(value)}]
  defp history_meta_pair(key, value) when is_integer(value), do: [{key, Integer.to_string(value)}]
  defp history_meta_pair(key, value), do: [{key, to_string(value)}]

  defp encode_history_meta([]), do: <<1>>

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

  defp encode_int(value) when is_integer(value) and value >= 0 and value < 127,
    do: <<value + 1>>

  defp encode_int(value) when is_integer(value) and value >= 0, do: encode_varint(value + 1)

  defp encode_int(_value), do: <<0>>

  defp decode_int(<<0, rest::binary>>), do: {:ok, nil, rest}

  defp decode_int(<<encoded, rest::binary>>) when encoded < 128,
    do: {:ok, encoded - 1, rest}

  defp decode_int(binary) do
    with {:ok, encoded, rest} <- decode_varint(binary) do
      case encoded do
        0 -> {:ok, nil, rest}
        value -> {:ok, value - 1, rest}
      end
    end
  end

  defp encode_bin(value) when is_binary(value) and byte_size(value) < 127,
    do: [<<byte_size(value) + 1>>, value]

  defp encode_bin(value) when is_binary(value),
    do: [encode_varint(byte_size(value) + 1), value]

  defp encode_bin(_value), do: <<0>>

  defp decode_bin(<<0, rest::binary>>), do: {:ok, nil, rest}

  defp decode_bin(<<encoded, rest::binary>>) when encoded < 128 do
    len = encoded - 1

    case rest do
      <<value::binary-size(len), tail::binary>> -> {:ok, value, tail}
      _ -> :error
    end
  end

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

  defp encode_child_groups(groups) when is_map(groups) and map_size(groups) == 0,
    do: encode_bin("J{}")

  defp encode_child_groups(groups) when is_map(groups) do
    ["J", Jason.encode!(groups)]
    |> IO.iodata_to_binary()
    |> encode_bin()
  end

  defp encode_child_groups(_groups), do: encode_bin("J{}")

  defp encode_record_sidecar(record) when is_map(record) do
    child_groups =
      record
      |> Map.get(:child_groups, %{})
      |> normalize_child_groups()

    refs = flow_record_value_refs(record)

    if map_size(refs) == 0 do
      encode_child_groups(child_groups)
    else
      child_groups
      |> Map.put(@record_value_refs_key, encode_value_refs(refs))
      |> encode_child_groups()
    end
  end

  defp record_empty_sidecar do
    %{}
    |> encode_child_groups()
    |> IO.iodata_to_binary()
  end

  defp record_empty_sidecar?(sidecar), do: sidecar == record_empty_sidecar()

  defp split_record_sidecar(groups) when is_map(groups) do
    {encoded_refs, child_groups} = Map.pop(groups, @record_value_refs_key, %{})
    {child_groups, decode_value_refs(encoded_refs)}
  end

  defp split_record_sidecar(_groups), do: {%{}, %{}}

  defp flow_record_value_refs(record) when is_map(record) do
    record
    |> Map.get(:value_refs, %{})
    |> decode_value_refs()
  end

  defp flow_record_value_refs(_record), do: %{}

  defp encode_value_refs(refs) when is_map(refs) do
    refs
    |> decode_value_refs()
    |> Map.new(fn {name, entry} ->
      {name,
       %{
         "ref" => Map.get(entry, :ref),
         "version" => Map.get(entry, :version),
         "digest" => Map.get(entry, :digest)
       }}
    end)
  end

  defp encode_value_refs(_refs), do: %{}

  defp decode_value_refs(refs) when is_map(refs) do
    refs
    |> Enum.reduce(%{}, fn
      {name, %{} = entry}, acc when is_binary(name) and name != "" ->
        ref = Map.get(entry, :ref) || Map.get(entry, "ref")

        if is_binary(ref) and ref != "" do
          Map.put(acc, name, %{
            ref: ref,
            version: value_ref_integer(Map.get(entry, :version) || Map.get(entry, "version")),
            digest: value_ref_binary(Map.get(entry, :digest) || Map.get(entry, "digest"))
          })
        else
          acc
        end

      {name, ref}, acc when is_binary(name) and name != "" and is_binary(ref) and ref != "" ->
        Map.put(acc, name, %{ref: ref, version: nil, digest: nil})

      _entry, acc ->
        acc
    end)
  end

  defp decode_value_refs(value) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} -> decode_value_refs(decoded)
      _ -> %{}
    end
  end

  defp decode_value_refs(_refs), do: %{}

  defp value_ref_integer(value) when is_integer(value), do: value

  defp value_ref_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} -> parsed
      _ -> nil
    end
  end

  defp value_ref_integer(_value), do: nil

  defp value_ref_binary(value) when is_binary(value) and value != "", do: value
  defp value_ref_binary(_value), do: nil

  defp decode_child_groups(binary) do
    with {:ok, encoded, rest} <- decode_bin(binary),
         {:ok, decoded} <- decode_child_groups_payload(encoded) do
      {:ok, decoded, rest}
    end
  end

  defp decode_child_groups_payload("J{}"), do: {:ok, %{}}
  defp decode_child_groups_payload("J" <> json), do: Jason.decode(json)

  defp decode_child_groups_payload(encoded) do
    {:ok, :erlang.binary_to_term(encoded, [:safe])}
  rescue
    _ -> :error
  end

  defp normalize_child_groups(groups) when is_map(groups), do: groups
  defp normalize_child_groups(_groups), do: %{}

  defp encode_varint(value) when value < 128, do: <<value>>

  defp encode_varint(value) when value < 16_384 do
    <<(value &&& 0x7F) ||| 0x80, value >>> 7>>
  end

  defp encode_varint(value) when value < 2_097_152 do
    <<(value &&& 0x7F) ||| 0x80, (value >>> 7 &&& 0x7F) ||| 0x80, value >>> 14>>
  end

  defp encode_varint(value) when value < 268_435_456 do
    <<(value &&& 0x7F) ||| 0x80, (value >>> 7 &&& 0x7F) ||| 0x80,
      (value >>> 14 &&& 0x7F) ||| 0x80, value >>> 21>>
  end

  defp encode_varint(value) when value < 34_359_738_368 do
    <<(value &&& 0x7F) ||| 0x80, (value >>> 7 &&& 0x7F) ||| 0x80,
      (value >>> 14 &&& 0x7F) ||| 0x80, (value >>> 21 &&& 0x7F) ||| 0x80, value >>> 28>>
  end

  defp encode_varint(value) when value < 4_398_046_511_104 do
    <<(value &&& 0x7F) ||| 0x80, (value >>> 7 &&& 0x7F) ||| 0x80,
      (value >>> 14 &&& 0x7F) ||| 0x80, (value >>> 21 &&& 0x7F) ||| 0x80,
      (value >>> 28 &&& 0x7F) ||| 0x80, value >>> 35>>
  end

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

  defp history_context_string(context, key, fallback) when is_map(context) do
    case Map.get(context, key) || Map.get(context, Atom.to_string(key)) do
      value when is_binary(value) -> value
      _ -> fallback
    end
  end

  defp history_context_string(_context, _key, fallback), do: fallback

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
                 query.rev?
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
    if include_cold? do
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
    if include_cold? do
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
            |> Enum.take(count)

          {:ok, entries}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp flow_lmdb_query_index_entries(
         _ctx,
         _index_key,
         _partition_key,
         count,
         _consistent?,
         _reverse?
       )
       when count <= 0,
       do: {:ok, []}

  defp flow_lmdb_query_index_entries(ctx, index_key, partition_key, count, consistent?, reverse?) do
    with :ok <- flow_maybe_flush_lmdb_for_index(ctx, index_key, partition_key, consistent?),
         :ok <- flow_require_lmdb_mirror_healthy(ctx, index_key, partition_key) do
      prefix = Ferricstore.Flow.LMDB.query_index_prefix(index_key)
      now_ms = now_ms()
      scan_count = flow_lmdb_query_scan_count(count)

      ctx
      |> flow_lmdb_paths_for_index(index_key, partition_key)
      |> Enum.reduce_while({:ok, []}, fn path, {:ok, acc} ->
        with {:ok, entries} <-
               Ferricstore.Flow.LMDB.prefix_entries(path, prefix, scan_count, reverse?) do
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
    flow_history_lmdb_query_scan_count(count, false)
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

    max(count, max_scan)
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
      |> flow_value_raw_mget_with_file_refs(refs, file_ref_payload_threshold(max_bytes))
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
    {decoded, size} = decode_value_with_user_size(encoded_value)

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

  defp observe_flow(command, started, result, fallback_metadata) do
    {success_count, fallback_metadata} = Map.pop(fallback_metadata, :_count)
    measurements = flow_measurements(started, command, result, success_count)
    metadata = flow_metadata(result, fallback_metadata)

    :telemetry.execute([:ferricstore, :flow, command, :stop], measurements, metadata)

    result
  end

  defp observe_flow_batch(command, started, results) do
    {success_count, first_record} = flow_batch_success_count_and_first_record(results)

    measurements =
      flow_measurements(started, command, {:ok, first_record}, success_count)

    metadata = flow_metadata({:ok, first_record}, %{flow_id: nil})

    :telemetry.execute([:ferricstore, :flow, command, :stop], measurements, metadata)
    :ok
  end

  defp flow_batch_success_count_and_first_record(results) do
    Enum.reduce(results, {0, nil}, fn
      :ok, {count, first_record} ->
        {count + 1, first_record}

      {:ok, record}, {count, nil} when is_map(record) ->
        {count + 1, record}

      {:ok, _record}, {count, first_record} ->
        {count + 1, first_record}

      _other, acc ->
        acc
    end)
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

  defp flow_measurements(started, command, result, success_count) do
    count = result_count(result, success_count)

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

  defp result_count(:ok, count) when is_integer(count) and count >= 0, do: count
  defp result_count({:ok, _value}, count) when is_integer(count) and count >= 0, do: count
  defp result_count(result, _count), do: result_count(result)

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

  defp flow_metadata(:ok, fallback),
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
          return_mode,
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
        {type, state, worker, lease_ms, priority, now, return_mode, payload_return,
         reclaim_expired?, reclaim_ratio, partition_key}

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
    {:ok, claim_due_return_records(ctx, records, claim.payload_return, claim.return_mode)}
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
        hd(claims).return_mode
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

  defp hydrate_claim_due_pipeline_results(results, ctx, payload_return, return_mode) do
    Enum.map(results, fn
      {:ok, records} -> {:ok, claim_due_return_records(ctx, records, payload_return, return_mode)}
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
         return_mode,
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
      return: return_mode,
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
      |> flow_value_raw_mget_with_file_refs(fetchable_refs, file_ref_payload_threshold(max_bytes))
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
    {decoded, size} = decode_value_with_user_size(encoded_value)

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
      |> Enum.flat_map(fn record -> Map.keys(flow_record_value_refs(record)) end)
      |> Enum.uniq()

    hydrate_named_value_records(ctx, records, names)
  end

  defp hydrate_named_value_records(_ctx, records, []), do: records

  defp hydrate_named_value_records(ctx, records, names) when is_list(names) do
    ref_entries =
      records
      |> Enum.with_index()
      |> Enum.flat_map(fn {record, idx} ->
        refs = flow_record_value_refs(record)

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
      |> flow_value_raw_mget(fetchable_refs)
      |> Enum.zip(fetchable_refs)
      |> Map.new(fn {value, ref} -> {ref, value} end)

    values_by_record =
      Enum.reduce(ref_entries, %{}, fn {idx, name, ref}, acc ->
        case Map.get(values, ref) do
          value when is_binary(value) ->
            Map.update(acc, idx, %{name => decode_value(value)}, fn existing ->
              Map.put(existing, name, decode_value(value))
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
         partition_key,
         item_opts,
         mismatch_error \\ "ERR flow partition_key mismatch in batch"
       )

  defp many_item_partition_key(nil, item_opts, _mismatch_error) do
    optional_partition_key(partition_key: Keyword.get(item_opts, :partition_key))
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

  defp shared_value_ref_id do
    :crypto.strong_rand_bytes(18)
    |> Base.url_encode64(padding: false)
  end

  defp flow_value_expire_at(_now, nil), do: 0
  defp flow_value_expire_at(now, ttl_ms), do: now + ttl_ms

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

  defmodule Keys do
    @moduledoc false

    @global_tag "{f}"
    @partition_tag_prefix "{f:"
    @auto_partition_prefix "__flow_auto__:"
    @auto_partition_buckets 256
    @auto_partition_tags 0..(@auto_partition_buckets - 1)
                         |> Enum.map(&("{fa:" <> Integer.to_string(&1) <> "}"))
                         |> List.to_tuple()

    def state_key(id, partition_key \\ nil)

    def state_key(id, nil) when is_binary(id) do
      "f:" <> tag(auto_partition_key(id)) <> ":s:" <> id
    end

    def state_key(id, partition_key) do
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

    def history_key(id, partition_key \\ nil)

    def history_key(id, nil) when is_binary(id) do
      "f:" <> tag(auto_partition_key(id)) <> ":h:" <> id
    end

    def history_key(id, partition_key) do
      "f:" <> tag(partition_key) <> ":h:" <> id
    end

    def value_key(id, kind, version, partition_key \\ nil)

    def value_key(id, kind, version, nil)
        when kind in [:payload, :result, :error, :shared] and is_integer(version) and
               is_binary(id) do
      value_key(id, kind, version, auto_partition_key(id))
    end

    def value_key(id, kind, version, partition_key)
        when kind in [:payload, :result, :error, :shared] and is_integer(version) do
      "f:" <>
        tag(partition_key) <>
        ":v:" <> flow_value_kind(kind) <> ":" <> id <> ":" <> Integer.to_string(version)
    end

    def shared_value_link_prefix(owner_flow_id, partition_key \\ nil)
        when is_binary(owner_flow_id) do
      "f:" <> tag(partition_key) <> ":svl:" <> owner_flow_id <> ":"
    end

    def signal_idempotency_key(id, idempotency_key, partition_key \\ nil)
        when is_binary(id) and is_binary(idempotency_key) do
      "f:" <> tag(partition_key) <> ":sig:" <> id <> ":" <> idempotency_key
    end

    def policy_key(type) do
      "f:" <> @global_tag <> ":policy:" <> type
    end

    def policy_key?(key) when is_binary(key),
      do: String.starts_with?(key, "f:" <> @global_tag <> ":policy:")

    def policy_key?(_key), do: false

    def value_key?(<<"f:{", rest::binary>>), do: :binary.match(rest, "}:v:") != :nomatch
    def value_key?(_key), do: false

    def history_key?(<<"f:{", rest::binary>>), do: :binary.match(rest, "}:h:") != :nomatch
    def history_key?(_key), do: false

    def due_key(type, state, priority, partition_key \\ nil) do
      "f:" <>
        tag(partition_key) <>
        ":d:" <> type <> ":" <> state <> ":p" <> Integer.to_string(priority)
    end

    def due_any_key(type, priority, partition_key \\ nil) do
      "f:" <>
        tag(partition_key) <>
        ":da:" <> type <> ":p" <> Integer.to_string(priority)
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
      stream_entry_key_from_history_key(history_key(id, partition_key), event_id)
    end

    def stream_entry_key_from_history_key(history_key, event_id)
        when is_binary(history_key) and is_binary(event_id) do
      "X:" <> history_key <> <<0>> <> event_id
    end

    def state_key?(key) when is_binary(key) do
      String.starts_with?(key, "f:{f") and String.contains?(key, "}:s:")
    end

    def state_key?(_key), do: false

    def tag(nil), do: @global_tag
    def tag(:global), do: @global_tag

    def tag(<<@auto_partition_prefix, bucket::binary>> = partition_key) do
      case auto_partition_bucket(bucket) do
        {:ok, bucket_index} ->
          elem(@auto_partition_tags, bucket_index)

        :error ->
          hashed_partition_tag(partition_key)
      end
    end

    def tag(partition_key) when is_binary(partition_key), do: hashed_partition_tag(partition_key)

    defp hashed_partition_tag(partition_key) do
      @partition_tag_prefix <>
        Base.url_encode64(:crypto.hash(:sha256, partition_key), padding: false) <> "}"
    end

    def auto_partition_key(id) when is_binary(id) do
      bucket =
        id
        |> :erlang.crc32()
        |> rem(@auto_partition_buckets)

      @auto_partition_prefix <> Integer.to_string(bucket)
    end

    def auto_partition_keys do
      Enum.map(0..(@auto_partition_buckets - 1), fn bucket ->
        @auto_partition_prefix <> Integer.to_string(bucket)
      end)
    end

    def auto_partition_key?(<<@auto_partition_prefix, bucket::binary>>) do
      match?({:ok, _bucket_index}, auto_partition_bucket(bucket))
    end

    def auto_partition_key?(_partition_key), do: false

    defp auto_partition_bucket(bucket), do: auto_partition_bucket(bucket, 0, false)

    defp auto_partition_bucket(<<>>, value, true) when value < @auto_partition_buckets,
      do: {:ok, value}

    defp auto_partition_bucket(<<digit, rest::binary>>, value, _seen?)
         when digit >= ?0 and digit <= ?9 do
      next = value * 10 + (digit - ?0)

      if next < @auto_partition_buckets do
        auto_partition_bucket(rest, next, true)
      else
        :error
      end
    end

    defp auto_partition_bucket(_bucket, _value, _seen?), do: :error

    defp flow_value_kind(:payload), do: "p"
    defp flow_value_kind(:result), do: "r"
    defp flow_value_kind(:error), do: "e"
    defp flow_value_kind(:shared), do: "s"
  end
end
