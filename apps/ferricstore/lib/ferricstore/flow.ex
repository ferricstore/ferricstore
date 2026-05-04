defmodule Ferricstore.Flow do
  @moduledoc false

  alias Ferricstore.Store.Router

  @default_state "queued"
  @default_priority 0
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
         {:ok, priority} <- optional_non_neg_integer(opts, :priority, @default_priority),
         :ok <- validate_flow_keys(id, type, state, priority) do
      attrs = %{
        id: id,
        type: type,
        state: state,
        payload_ref: payload_ref,
        run_at_ms: run_at_ms,
        priority: priority,
        now_ms: now
      }

      Router.flow_create(ctx, attrs)
    end
  end

  def get(ctx, id) when is_binary(id) do
    with :ok <- validate_id(id),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id)) do
      case Router.get(ctx, __MODULE__.Keys.state_key(id)) do
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
         {:ok, priority} <- optional_non_neg_integer(opts, :priority, @default_priority),
         {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
         :ok <- validate_key_size(__MODULE__.Keys.due_key(type, state, priority)) do
      attrs = %{
        type: type,
        state: state,
        worker: worker,
        lease_ms: lease_ms,
        limit: limit,
        priority: priority,
        now_ms: now
      }

      Router.flow_claim_due(ctx, attrs)
    end
  end

  def complete(ctx, id, lease_token, opts \\ [])
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id)),
         {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
         {:ok, result_ref} <- optional_binary_or_nil(opts, :result_ref, nil) do
      Router.flow_complete(ctx, %{
        id: id,
        lease_token: lease_token,
        result_ref: result_ref,
        now_ms: now
      })
    end
  end

  def retry(ctx, id, lease_token, opts)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         :ok <- validate_lease_token(lease_token),
         :ok <- validate_key_size(__MODULE__.Keys.state_key(id)),
         {:ok, now} <- optional_non_neg_integer(opts, :now_ms, now_ms()),
         {:ok, run_at_ms} <- optional_non_neg_integer(opts, :run_at_ms, now),
         {:ok, error_ref} <- optional_binary_or_nil(opts, :error_ref, nil) do
      Router.flow_retry(ctx, %{
        id: id,
        lease_token: lease_token,
        run_at_ms: run_at_ms,
        error_ref: error_ref,
        now_ms: now
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

  defp validate_lease_token(token) when is_binary(token) and token != "", do: :ok

  defp validate_lease_token(_token),
    do: {:error, "ERR flow lease_token must be a non-empty string"}

  defp validate_flow_keys(id, type, state, priority) do
    with :ok <- validate_key_size(__MODULE__.Keys.state_key(id)),
         :ok <- validate_key_size(__MODULE__.Keys.history_key(id)),
         :ok <- validate_key_size(__MODULE__.Keys.due_key(type, state, priority)) do
      validate_key_size(
        __MODULE__.Keys.stream_entry_key(id, "18446744073709551615-18446744073709551615")
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

  defp optional_non_neg_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a non-negative integer"}
    end
  end

  defp optional_pos_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a positive integer"}
    end
  end

  defp now_ms, do: System.os_time(:millisecond)

  defmodule Keys do
    @moduledoc false

    @tag "{flow}"

    def state_key(id), do: "flow:" <> @tag <> ":state:" <> id
    def history_key(id), do: "flow:" <> @tag <> ":history:" <> id

    def due_key(type, state, priority) do
      "flow:" <> @tag <> ":due:" <> type <> ":" <> state <> ":p" <> Integer.to_string(priority)
    end

    def stream_entry_key(id, event_id), do: "X:" <> history_key(id) <> <<0>> <> event_id
  end
end
