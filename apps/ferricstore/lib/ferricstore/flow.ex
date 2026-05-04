defmodule Ferricstore.Flow do
  @moduledoc false

  alias Ferricstore.Store.Router

  @default_state "queued"
  @default_priority 0
  @max_priority 2
  @default_lease_ms 30_000
  @default_limit 1
  @max_ref_size 4_096

  def create(ctx, id, opts) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with {:ok, attrs} <- create_attrs(id, opts, now_ms()) do
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
          case create_attrs(id, opts, now_ms()) do
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
    now = now_ms()

    result =
      with {:ok, partition_key} <- required_partition_key(partition_key),
           :ok <- validate_create_many_items(items),
           {:ok, attrs_list} <- create_many_attrs(items, opts, partition_key, now),
           :ok <- validate_unique_create_ids(attrs_list) do
        Router.flow_create_many(ctx, partition_key, attrs_list)
      end

    observe_flow(:create, started, result, %{flow_id: nil})
  end

  def create_many(_ctx, _partition_key, _items, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def get(ctx, id, opts \\ []) when is_binary(id) and is_list(opts) do
    with :ok <- validate_id(id),
         :ok <- validate_opts(opts),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)) do
      case Router.flow_get(ctx, id, partition_key) do
        nil -> {:ok, nil}
        value when is_binary(value) -> {:ok, decode_record(value)}
      end
    end
  end

  def claim_due(ctx, type, opts) when is_binary(type) and is_list(opts) do
    started = flow_start_time()

    result =
      with :ok <- validate_opts(opts),
           :ok <- validate_type(type),
           {:ok, state} <- optional_binary(opts, :state, @default_state),
           {:ok, worker} <- required_binary(opts, :worker),
           {:ok, lease_ms} <- optional_pos_integer(opts, :lease_ms, @default_lease_ms),
           {:ok, limit} <- optional_pos_integer(opts, :limit, @default_limit),
           {:ok, priority} <- optional_priority_or_nil(opts),
           {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
           {:ok, partition_key} <- optional_partition_key(opts),
           :ok <- validate_claim_due_keys(type, state, priority, partition_key) do
        attrs = %{
          type: type,
          state: state,
          worker: worker,
          lease_ms: lease_ms,
          limit: limit,
          priority: priority,
          now_ms: now,
          partition_key: partition_key
        }

        Router.flow_claim_due(ctx, attrs)
      end

    observe_flow(:claim_due, started, result, %{flow_type: type})
  end

  def complete(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with :ok <- validate_opts(opts),
           :ok <- validate_id(id),
           :ok <- validate_lease_token(lease_token),
           {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
           {:ok, partition_key} <- optional_partition_key(opts),
           :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
           {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
           {:ok, ttl_ms} <- optional_non_neg_integer_or_nil(opts, :ttl_ms),
           {:ok, result_ref} <- optional_binary_or_nil(opts, :result_ref, nil),
           :ok <- validate_ref_size(:result_ref, result_ref) do
        Router.flow_complete(ctx, %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          ttl_ms: ttl_ms,
          result_ref: result_ref,
          now_ms: now,
          partition_key: partition_key
        })
      end

    observe_flow(:complete, started, result, %{flow_id: id})
  end

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
      with {:ok, partition_key} <- required_partition_key(partition_key),
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

  def retry(ctx, id, lease_token, opts)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with :ok <- validate_opts(opts),
           :ok <- validate_id(id),
           :ok <- validate_lease_token(lease_token),
           {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
           {:ok, partition_key} <- optional_partition_key(opts),
           :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
           {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
           {:ok, run_at_ms} <- optional_non_neg_integer(opts, :run_at_ms, now),
           {:ok, error_ref} <- optional_binary_or_nil(opts, :error_ref, nil),
           :ok <- validate_ref_size(:error_ref, error_ref) do
        Router.flow_retry(ctx, %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          run_at_ms: run_at_ms,
          error_ref: error_ref,
          now_ms: now,
          partition_key: partition_key
        })
      end

    observe_flow(:retry, started, result, %{flow_id: id})
  end

  def fail(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    started = flow_start_time()

    result =
      with :ok <- validate_opts(opts),
           :ok <- validate_id(id),
           :ok <- validate_lease_token(lease_token),
           {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
           {:ok, partition_key} <- optional_partition_key(opts),
           :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
           {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
           {:ok, ttl_ms} <- optional_non_neg_integer_or_nil(opts, :ttl_ms),
           {:ok, error_ref} <- optional_binary_or_nil(opts, :error_ref, nil),
           :ok <- validate_ref_size(:error_ref, error_ref) do
        Router.flow_fail(ctx, %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          ttl_ms: ttl_ms,
          error_ref: error_ref,
          now_ms: now,
          partition_key: partition_key
        })
      end

    observe_flow(:fail, started, result, %{flow_id: id})
  end

  def cancel(ctx, id, opts \\ []) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with :ok <- validate_opts(opts),
           :ok <- validate_id(id),
           {:ok, lease_token} <- optional_lease_token(opts),
           {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
           {:ok, partition_key} <- optional_partition_key(opts),
           :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
           {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
           {:ok, ttl_ms} <- optional_non_neg_integer_or_nil(opts, :ttl_ms),
           {:ok, reason_ref} <- optional_binary_or_nil(opts, :reason_ref, nil),
           :ok <- validate_ref_size(:reason_ref, reason_ref) do
        Router.flow_cancel(ctx, %{
          id: id,
          lease_token: lease_token,
          fencing_token: fencing_token,
          ttl_ms: ttl_ms,
          reason_ref: reason_ref,
          now_ms: now,
          partition_key: partition_key
        })
      end

    observe_flow(:cancel, started, result, %{flow_id: id})
  end

  def rewind(ctx, id, opts) when is_binary(id) and is_list(opts) do
    started = flow_start_time()

    result =
      with :ok <- validate_opts(opts),
           :ok <- validate_id(id),
           {:ok, to_event} <- required_binary(opts, :to_event),
           {:ok, expect_state} <- optional_binary_or_nil(opts, :expect_state, nil),
           {:ok, run_at_ms} <- optional_non_neg_integer_or_nil(opts, :run_at_ms),
           {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
           {:ok, reason_ref} <- optional_binary_or_nil(opts, :reason_ref, nil),
           :ok <- validate_ref_size(:reason_ref, reason_ref),
           {:ok, partition_key} <- optional_partition_key(opts),
           :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
           :ok <- validate_key_size(__MODULE__.Keys.history_key(id, partition_key)) do
        Router.flow_rewind(ctx, %{
          id: id,
          to_event: to_event,
          expect_state: expect_state,
          run_at_ms: run_at_ms,
          reason_ref: reason_ref,
          now_ms: now,
          partition_key: partition_key
        })
      end

    observe_flow(:rewind, started, result, %{flow_id: id})
  end

  def decode_record(value) when is_binary(value), do: :erlang.binary_to_term(value)

  defp flow_start_time, do: System.monotonic_time()

  defp observe_flow(command, started, result, fallback_metadata) do
    measurements = flow_measurements(started, command, result)
    metadata = flow_metadata(result, fallback_metadata)

    :telemetry.execute([:ferricstore, :flow, command, :stop], measurements, metadata)
    publish_flow_notifications(command, result)

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
    publish_flow_notifications(command, {:ok, records})
    :ok
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

  defp publish_flow_notifications(command, {:ok, records}) when is_list(records) do
    Enum.each(records, &publish_flow_record(command, &1))
  end

  defp publish_flow_notifications(command, {:ok, record}) when is_map(record) do
    publish_flow_record(command, record)
  end

  defp publish_flow_notifications(_command, _result), do: :ok

  defp publish_flow_record(command, %{id: id, type: type} = record)
       when is_binary(id) and is_binary(type) do
    message = flow_pubsub_message(command, record)

    safe_publish("flow_changed:" <> id, message)
    safe_publish("flow_type_changed:" <> type, message)

    if publish_due_wakeup?(command, record) do
      safe_publish("flow_due:" <> type, message)
    end

    :ok
  end

  defp publish_flow_record(_command, _record), do: :ok

  defp publish_due_wakeup?(command, record)
       when command in [:create, :transition, :retry] do
    is_integer(Map.get(record, :next_run_at_ms)) and Map.get(record, :state) != "running"
  end

  defp publish_due_wakeup?(_command, _record), do: false

  defp flow_pubsub_message(command, record) do
    [
      "event=",
      flow_event_name(command),
      ";id=",
      Map.get(record, :id, ""),
      ";type=",
      Map.get(record, :type, ""),
      ";state=",
      Map.get(record, :state, ""),
      ";version=",
      record |> Map.get(:version, 0) |> Integer.to_string()
    ]
    |> IO.iodata_to_binary()
  end

  defp flow_event_name(:create), do: "created"
  defp flow_event_name(:claim_due), do: "claimed"
  defp flow_event_name(:transition), do: "transitioned"
  defp flow_event_name(:retry), do: "retry"
  defp flow_event_name(:fail), do: "failed"
  defp flow_event_name(:cancel), do: "cancelled"
  defp flow_event_name(:complete), do: "completed"
  defp flow_event_name(:rewind), do: "rewound"
  defp flow_event_name(command), do: Atom.to_string(command)

  defp safe_publish(channel, message) do
    Ferricstore.PubSub.publish(channel, message)
  rescue
    _ -> 0
  end

  defp validate_opts(opts) do
    if Keyword.keyword?(opts) do
      :ok
    else
      {:error, "ERR flow opts must be a keyword list"}
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

  defp create_attrs(id, opts, default_now) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, type} <- required_binary(opts, :type),
         {:ok, state} <- optional_binary(opts, :state, @default_state),
         {:ok, payload_ref} <- optional_binary_or_nil(opts, :payload_ref, nil),
         :ok <- validate_ref_size(:payload_ref, payload_ref),
         {:ok, now} <- optional_non_neg_integer(opts, :now_ms, default_now),
         {:ok, run_at_ms} <- optional_non_neg_integer(opts, :run_at_ms, now),
         {:ok, ttl_ms} <- optional_non_neg_integer_or_nil(opts, :ttl_ms),
         {:ok, history_max_events} <- optional_pos_integer_or_nil(opts, :history_max_events),
         {:ok, priority} <- optional_priority(opts, @default_priority),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_flow_keys(id, type, state, priority, partition_key) do
      {:ok,
       %{
         id: id,
         type: type,
         state: state,
         payload_ref: payload_ref,
         run_at_ms: run_at_ms,
         ttl_ms: ttl_ms,
         history_max_events: history_max_events,
         priority: priority,
         now_ms: now,
         partition_key: partition_key
       }}
    end
  end

  defp create_many_attrs(items, opts, partition_key, default_now) do
    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, item_opts} <- create_many_item_opts(item),
           {:ok, attrs} <-
             create_attrs(
               id,
               opts |> Keyword.merge(item_opts) |> Keyword.put(:partition_key, partition_key),
               default_now
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

  defp create_many_item_opts(id) when is_binary(id), do: {:ok, id, []}

  defp create_many_item_opts(%{id: id} = item) when is_binary(id) do
    {:ok, id, create_many_item_payload_ref(item)}
  end

  defp create_many_item_opts(%{"id" => id} = item) when is_binary(id) do
    {:ok, id, create_many_item_payload_ref(item)}
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

  defp create_many_item_opts(_item), do: {:error, "ERR flow id must be a non-empty string"}

  defp create_many_item_payload_ref(item) do
    cond do
      Map.has_key?(item, :payload_ref) -> [payload_ref: Map.get(item, :payload_ref)]
      Map.has_key?(item, "payload_ref") -> [payload_ref: Map.get(item, "payload_ref")]
      true -> []
    end
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
         {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
         {:ok, run_at_ms} <- optional_non_neg_integer(opts, :run_at_ms, now),
         {:ok, priority} <- optional_priority_or_nil(opts) do
      {:ok,
       %{
         id: id,
         from_state: from_state,
         to_state: to_state,
         lease_token: lease_token,
         fencing_token: fencing_token,
         run_at_ms: run_at_ms,
         priority: priority,
         now_ms: now,
         partition_key: partition_key
       }}
    end
  end

  defp transition_many_attrs(items, opts, partition_key, from_state, to_state) do
    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      with {:ok, id, item_opts} <- transition_many_item_opts(item),
           {:ok, attrs} <-
             transition_attrs(
               id,
               from_state,
               to_state,
               opts |> Keyword.merge(item_opts) |> Keyword.put(:partition_key, partition_key)
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
    {:ok, id, [fencing_token: fencing_token] ++ transition_many_item_lease_token(item)}
  end

  defp transition_many_item_opts(%{"id" => id, "fencing_token" => fencing_token} = item)
       when is_binary(id) do
    {:ok, id, [fencing_token: fencing_token] ++ transition_many_item_lease_token(item)}
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

  defp transition_many_item_opts(_item), do: {:error, "ERR flow id must be a non-empty string"}

  defp transition_many_item_lease_token(item) do
    cond do
      Map.has_key?(item, :lease_token) -> [lease_token: Map.get(item, :lease_token)]
      Map.has_key?(item, "lease_token") -> [lease_token: Map.get(item, "lease_token")]
      true -> []
    end
  end

  defp validate_create_many_items([_ | _]), do: :ok
  defp validate_create_many_items(_items), do: {:error, "ERR flow items must be a non-empty list"}

  defp validate_transition_many_items([_ | _]), do: :ok

  defp validate_transition_many_items(_items),
    do: {:error, "ERR flow items must be a non-empty list"}

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

  defp required_non_neg_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      {:ok, _} -> {:error, "ERR flow #{key} must be a non-negative integer"}
      :error -> {:error, "ERR flow #{key} is required"}
    end
  end

  defp optional_non_neg_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a non-negative integer"}
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
      case validate_key_size(__MODULE__.Keys.due_key(type, state, priority, partition_key)) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
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

  defp required_partition_key(partition_key) do
    case optional_partition_key(partition_key: partition_key) do
      {:ok, nil} -> {:error, "ERR flow partition_key is required"}
      {:ok, value} -> {:ok, value}
      {:error, _reason} = error -> error
    end
  end

  defp now_ms, do: System.os_time(:millisecond)

  defmodule Keys do
    @moduledoc false

    @global_tag "{flow}"
    @partition_tag_prefix "{flow:"

    def state_key(id, partition_key \\ nil) do
      "flow:" <> tag(partition_key) <> ":state:" <> id
    end

    def history_key(id, partition_key \\ nil) do
      "flow:" <> tag(partition_key) <> ":history:" <> id
    end

    def due_key(type, state, priority, partition_key \\ nil) do
      "flow:" <>
        tag(partition_key) <>
        ":due:" <> type <> ":" <> state <> ":p" <> Integer.to_string(priority)
    end

    def state_index_key(type, state, partition_key \\ nil) do
      "flow:" <> tag(partition_key) <> ":idx:state:" <> type <> ":" <> state
    end

    def inflight_index_key(type, partition_key \\ nil) do
      "flow:" <> tag(partition_key) <> ":idx:inflight:" <> type
    end

    def worker_index_key(worker, partition_key \\ nil) do
      "flow:" <> tag(partition_key) <> ":idx:worker:" <> worker
    end

    def stream_entry_key(id, event_id, partition_key \\ nil) do
      "X:" <> history_key(id, partition_key) <> <<0>> <> event_id
    end

    def tag(nil), do: @global_tag
    def tag(:global), do: @global_tag

    def tag(partition_key) when is_binary(partition_key) do
      @partition_tag_prefix <>
        Base.encode16(:crypto.hash(:sha256, partition_key), case: :lower) <> "}"
    end
  end
end
