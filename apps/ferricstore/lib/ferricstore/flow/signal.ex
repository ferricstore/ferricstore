defmodule Ferricstore.Flow.Signal do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, ScopeBinding}
  alias Ferricstore.Flow.Telemetry, as: FlowTelemetry
  alias Ferricstore.Store.Router

  @max_ref_size 4_096

  @doc false
  def run(ctx, id, opts) when is_binary(id) and is_list(opts) do
    started = FlowTelemetry.start_time()

    result =
      with {:ok, attrs} <- attrs(id, opts),
           {:ok, attrs} <- ScopeBinding.bind_mutation(ctx, :signal, attrs) do
        Router.flow_signal(ctx, attrs)
      end

    FlowTelemetry.observe(:signal, started, result, %{flow_id: id, _count: 1})
  end

  def run(_ctx, id, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def run(_ctx, _id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc false
  def attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, signal} <- required_binary(opts, :signal),
         {:ok, if_state} <- optional_signal_states(opts),
         {:ok, transition_to} <- optional_binary_or_nil(opts, :transition_to, nil),
         :ok <- reject_running_state_transition(transition_to),
         {:ok, idempotency_key} <- optional_binary_or_nil(opts, :idempotency_key, nil),
         :ok <- validate_ref_size(:idempotency_key, idempotency_key),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(Keys.state_key(id, partition_key)),
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

  defp validate_opts(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, "ERR flow opts must be a keyword list"}
  end

  defp validate_id(id) when is_binary(id) and id != "", do: :ok
  defp validate_id(_id), do: {:error, "ERR flow id must be a non-empty string"}

  defp required_binary(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      {:ok, _} -> {:error, "ERR flow #{key} must be a non-empty string"}
      :error -> {:error, "ERR flow #{key} is required"}
    end
  end

  defp optional_binary_or_nil(opts, key, default) do
    case Keyword.get(opts, key, default) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a string"}
    end
  end

  defp reject_running_state_transition("running"),
    do: {:error, "ERR flow running state is only entered by FLOW.CLAIM_DUE"}

  defp reject_running_state_transition(_state), do: :ok

  defp validate_ref_size(_key, nil), do: :ok

  defp validate_ref_size(key, value) when is_binary(value) do
    if byte_size(value) <= @max_ref_size do
      :ok
    else
      {:error, "ERR flow #{key} too large (max #{@max_ref_size} bytes)"}
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

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
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

  defp maybe_put_attr(attrs, _key, nil), do: attrs
  defp maybe_put_attr(attrs, key, value), do: Map.put(attrs, key, value)

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
end
