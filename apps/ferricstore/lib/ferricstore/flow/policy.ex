defmodule Ferricstore.Flow.Policy do
  @moduledoc false

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.RetryPolicy
  alias Ferricstore.Stats
  alias Ferricstore.Store.Router

  @doc false
  def set(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         :ok <- validate_key_size(Keys.policy_key(type)),
         {:ok, policy} <- RetryPolicy.normalize_flow_policy(type, opts) do
      case Router.flow_policy_put_all(
             ctx,
             Keys.policy_key(type),
             RetryPolicy.encode_flow_policy(policy),
             0
           ) do
        :ok -> {:ok, response(type, policy, Keyword.get(opts, :state))}
        {:error, _reason} = error -> error
      end
    end
  end

  def set(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc false
  def get(ctx, type, opts \\ [])

  def get(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, state} <- optional_binary_or_nil(opts, :state, nil),
         :ok <- validate_key_size(Keys.policy_key(type)),
         {:ok, policy} <- read(ctx, type) do
      {:ok, response(type, policy, state)}
    end
  end

  def get(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def raw(ctx, type) when is_binary(type), do: read(ctx, type)
  def raw(_ctx, _type), do: {:error, "ERR flow type must be a non-empty string"}

  @doc false
  def raw_entry(ctx, type) when is_binary(type), do: read_entry(ctx, type)

  def raw_entry(_ctx, _type),
    do: {:error, "ERR flow type must be a non-empty string"}

  defp read(ctx, type) do
    with {:ok, {_generation, policy}} <- read_entry(ctx, type) do
      {:ok, policy}
    end
  end

  defp read_entry(ctx, type) do
    case Stats.with_cache_tracking_disabled(fn ->
           Router.get(ctx, Keys.policy_key(type))
         end) do
      nil ->
        {:ok, {0, nil}}

      value when is_binary(value) ->
        case RetryPolicy.decode_flow_policy_entry(value) do
          {:ok, entry} -> {:ok, entry}
          :error -> {:error, "ERR flow policy is corrupt"}
        end

      _other ->
        {:error, "ERR flow policy is corrupt"}
    end
  end

  defp response(type, policy, nil) do
    states = Map.get(policy || %{}, :states, %{})

    %{
      type: type,
      version: Map.get(policy || %{}, :version),
      max_active_ms: RetryPolicy.resolve_max_active_ms(policy),
      retry: RetryPolicy.resolve(policy, nil, nil),
      retention: response_retention(policy, nil),
      indexed_attributes: RetryPolicy.indexed_attributes(policy),
      indexed_state_meta: RetryPolicy.indexed_state_meta(policy),
      governance: RetryPolicy.governance(policy),
      states:
        Map.new(states, fn {state, _state_policy} ->
          {state,
           %{
             mode: RetryPolicy.state_mode(policy, state),
             max_active_ms: RetryPolicy.resolve_max_active_ms(policy),
             retry: RetryPolicy.resolve(policy, state, nil),
             retention: response_retention(policy, state),
             governance: state_governance(policy, state)
           }}
        end)
    }
  end

  defp response(type, policy, state) when is_binary(state) do
    %{
      type: type,
      state: state,
      version: Map.get(policy || %{}, :version),
      mode: RetryPolicy.state_mode(policy, state),
      max_active_ms: RetryPolicy.resolve_max_active_ms(policy),
      retry: RetryPolicy.resolve(policy, state, nil),
      retention: response_retention(policy, state),
      indexed_attributes: RetryPolicy.indexed_attributes(policy),
      indexed_state_meta: RetryPolicy.indexed_state_meta(policy),
      governance: state_governance(policy, state) || RetryPolicy.governance(policy)
    }
  end

  defp state_governance(%{states: states}, state) when is_map(states) and is_binary(state) do
    case Map.get(states, state) do
      %{governance: governance} -> governance
      _other -> nil
    end
  end

  defp state_governance(_policy, _state), do: nil

  defp response_retention(policy, state) do
    policy
    |> RetryPolicy.resolve_retention(state, nil)
    |> Map.delete(:history_hot_max_events)
  end

  defp validate_opts(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, "ERR flow opts must be a keyword list"}
  end

  defp validate_type(type) when is_binary(type) and type != "", do: :ok
  defp validate_type(_type), do: {:error, "ERR flow type must be a non-empty string"}

  defp optional_binary_or_nil(opts, key, default) do
    case Keyword.get(opts, key, default) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a string"}
    end
  end

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end
end
