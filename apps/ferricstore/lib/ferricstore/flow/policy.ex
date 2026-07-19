defmodule Ferricstore.Flow.Policy do
  @moduledoc false

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.PolicyPatch
  alias Ferricstore.Flow.RetryPolicy
  alias Ferricstore.Stats
  alias Ferricstore.Store.Router

  @doc false
  def set(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         :ok <- validate_key_size(Keys.policy_key(type)),
         {:ok, response_state} <- optional_binary_or_nil(opts, :state, nil),
         {:ok, expected_generation} <- optional_expected_generation(opts),
         {:ok, replace?} <- optional_boolean(opts, :replace, false),
         patch <- PolicyPatch.from_opts(opts) do
      do_set(ctx, type, patch, response_state, expected_generation, replace?)
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
         {:ok, {generation, policy}} <- read_entry(ctx, type) do
      {:ok, response(type, generation, policy, state)}
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
    result =
      Stats.with_cache_tracking_disabled(fn ->
        Router.read_shard_value(ctx, 0, Keys.policy_key(type))
      end)

    case result do
      {:ok, value} -> decode_entry(value, type)
      :unavailable -> {:error, "ERR flow policy shard not available"}
      {:error, _reason} -> {:error, "ERR flow policy read failed"}
      _invalid -> {:error, "ERR flow policy read failed"}
    end
  end

  defp decode_entry(value, type) do
    case value do
      nil ->
        {:ok, {0, nil}}

      value when is_binary(value) ->
        case RetryPolicy.decode_flow_policy_entry(value) do
          {:ok, {generation, %{type: ^type} = policy}} -> {:ok, {generation, policy}}
          :error -> {:error, "ERR flow policy is corrupt"}
          _mismatched -> {:error, "ERR flow policy is corrupt"}
        end

      _other ->
        {:error, "ERR flow policy is corrupt"}
    end
  end

  defp do_set(ctx, type, patch, response_state, expected_generation, replace?) do
    case Router.flow_policy_patch_all(
           ctx,
           Keys.policy_key(type),
           patch,
           replace?,
           expected_generation
         ) do
      {:ok, value} when is_binary(value) ->
        case RetryPolicy.decode_flow_policy_entry(value) do
          {:ok, {generation, installed_policy}} ->
            {:ok, response(type, generation, installed_policy, response_state)}

          :error ->
            {:error, "ERR flow policy allocation failed"}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp response(type, generation, policy, nil) do
    states = Map.get(policy || %{}, :states, %{})

    %{
      type: type,
      generation: generation,
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

  defp response(type, generation, policy, state) when is_binary(state) do
    %{
      type: type,
      state: state,
      generation: generation,
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

  defp optional_expected_generation(opts) do
    case Keyword.get(opts, :expected_generation) do
      nil ->
        {:ok, nil}

      generation when is_integer(generation) and generation >= 0 ->
        if generation <= RetryPolicy.max_policy_generation() do
          {:ok, generation}
        else
          {:error, "ERR flow expected_generation must be a non-negative integer"}
        end

      _invalid ->
        {:error, "ERR flow expected_generation must be a non-negative integer"}
    end
  end

  defp optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _invalid -> {:error, "ERR flow #{key} must be boolean"}
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
