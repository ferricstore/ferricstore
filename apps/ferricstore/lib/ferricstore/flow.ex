defmodule Ferricstore.Flow do
  @moduledoc false

  alias Ferricstore.Store.Router

  @default_state "queued"
  @default_priority 0
  @max_priority 2
  @default_lease_ms 30_000
  @default_limit 1

  def create(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, type} <- required_binary(opts, :type),
         {:ok, state} <- optional_binary(opts, :state, @default_state),
         {:ok, payload_ref} <- optional_binary_or_nil(opts, :payload_ref, nil),
         {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
         {:ok, run_at_ms} <- optional_non_neg_integer(opts, :run_at_ms, now),
         {:ok, ttl_ms} <- optional_non_neg_integer_or_nil(opts, :ttl_ms),
         {:ok, history_max_events} <- optional_pos_integer_or_nil(opts, :history_max_events),
         {:ok, priority} <- optional_priority(opts, @default_priority),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_flow_keys(id, type, state, priority, partition_key) do
      attrs = %{
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
      }

      Router.flow_create(ctx, attrs)
    end
  end

  def get(ctx, id, opts \\ []) when is_binary(id) and is_list(opts) do
    with :ok <- validate_id(id),
         :ok <- validate_opts(opts),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)) do
      case Router.get(ctx, __MODULE__.Keys.state_key(id, partition_key)) do
        nil -> {:ok, nil}
        value when is_binary(value) -> {:ok, decode_record(value)}
      end
    end
  end

  def claim_due(ctx, type, opts) when is_binary(type) and is_list(opts) do
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
  end

  def complete(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
         {:ok, ttl_ms} <- optional_non_neg_integer_or_nil(opts, :ttl_ms),
         {:ok, result_ref} <- optional_binary_or_nil(opts, :result_ref, nil) do
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
  end

  def transition(ctx, id, from_state, to_state, opts \\ [])
      when is_binary(id) and is_binary(from_state) and is_binary(to_state) and is_list(opts) do
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
      Router.flow_transition(ctx, %{
        id: id,
        from_state: from_state,
        to_state: to_state,
        lease_token: lease_token,
        fencing_token: fencing_token,
        run_at_ms: run_at_ms,
        priority: priority,
        now_ms: now,
        partition_key: partition_key
      })
    end
  end

  def retry(ctx, id, lease_token, opts)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
         {:ok, run_at_ms} <- optional_non_neg_integer(opts, :run_at_ms, now),
         {:ok, error_ref} <- optional_binary_or_nil(opts, :error_ref, nil) do
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
  end

  def fail(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
         {:ok, ttl_ms} <- optional_non_neg_integer_or_nil(opts, :ttl_ms),
         {:ok, error_ref} <- optional_binary_or_nil(opts, :error_ref, nil) do
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
  end

  def cancel(ctx, id, opts \\ []) when is_binary(id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, lease_token} <- optional_lease_token(opts),
         {:ok, fencing_token} <- required_non_neg_integer(opts, :fencing_token),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id, partition_key)),
         {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
         {:ok, ttl_ms} <- optional_non_neg_integer_or_nil(opts, :ttl_ms),
         {:ok, reason_ref} <- optional_binary_or_nil(opts, :reason_ref, nil) do
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
  end

  def decode_record(value) when is_binary(value), do: :erlang.binary_to_term(value)

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
