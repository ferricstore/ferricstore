defmodule Ferricstore.Flow.RetryPolicy do
  @moduledoc false

  @max_retries 1_000
  @max_delay_ms 2_592_000_000
  @max_retention_ttl_ms 31_536_000_000
  @max_active_ms 31_536_000_000
  @max_history_hot_max_events 10_000
  @max_history_max_events 1_000_000

  @default %{
    max_retries: 3,
    backoff: %{kind: :exponential, base_ms: 1_000, max_ms: 30_000, jitter_pct: 20},
    exhausted_to: "failed"
  }

  @default_retention %{
    ttl_ms: 604_800_000,
    history_hot_max_events: 0,
    history_max_events: 100_000
  }

  @type t :: %{
          required(:max_retries) => non_neg_integer(),
          required(:backoff) => %{
            required(:kind) => :none | :fixed | :linear | :exponential,
            required(:base_ms) => non_neg_integer(),
            required(:max_ms) => non_neg_integer(),
            required(:jitter_pct) => non_neg_integer()
          },
          required(:exhausted_to) => binary()
        }

  @spec default() :: t()
  def default, do: @default

  def default_retention do
    %{
      ttl_ms: default_retention_ttl_ms(),
      history_hot_max_events: default_history_hot_max_events(),
      history_max_events: default_history_max_events()
    }
  end

  @spec normalize_flow_policy(binary(), term()) :: {:ok, map()} | {:error, binary()}
  def normalize_flow_policy(type, opts) when is_binary(type) and is_list(opts) do
    if Keyword.keyword?(opts) do
      normalize_flow_policy(type, Map.new(opts))
    else
      {:error, "ERR flow opts must be a keyword list"}
    end
  end

  def normalize_flow_policy(type, attrs) when is_binary(type) and is_map(attrs) do
    with :ok <- reject_top_level_state_mode(attrs),
         {:ok, version} <- optional_policy_version(attrs),
         {:ok, max_active_ms} <- optional_max_active_ms(attrs),
         {:ok, retry} <- optional_retry_override(attrs),
         {:ok, retention} <- optional_retention_override(attrs),
         {:ok, indexed_attributes} <- optional_indexed_attributes(attrs),
         {:ok, indexed_state_meta} <- optional_indexed_state_meta(attrs),
         {:ok, governance} <- optional_governance(attrs),
         {:ok, states} <- normalize_state_policies(fetch_policy(attrs, :states, "states", %{})) do
      policy =
        %{
          type: type,
          version: version,
          max_active_ms: max_active_ms,
          retry: retry,
          retention: retention,
          states: states,
          indexed_attributes: indexed_attributes,
          indexed_state_meta: indexed_state_meta,
          governance: governance
        }
        |> drop_nil_policy_fields()

      {:ok, policy}
    end
  end

  def normalize_flow_policy(_type, _attrs),
    do: {:error, "ERR flow policy must be a map or keyword list"}

  @spec normalize_override(term()) :: {:ok, map() | nil} | {:error, binary()}
  def normalize_override(nil), do: {:ok, nil}

  def normalize_override(policy) when is_list(policy) do
    if Keyword.keyword?(policy) do
      policy |> Map.new() |> normalize_override()
    else
      {:error, "ERR flow retry policy must be a map or keyword list"}
    end
  end

  def normalize_override(policy) when is_map(policy) do
    with :ok <- reject_old_max_attempts(policy),
         {:ok, max_retries} <- optional_max_retries(policy),
         {:ok, backoff} <- optional_backoff(policy),
         {:ok, exhausted_to} <- optional_exhausted_to(policy) do
      override =
        %{}
        |> maybe_put(:max_retries, max_retries)
        |> maybe_put(:backoff, backoff)
        |> maybe_put(:exhausted_to, exhausted_to)

      {:ok, override}
    end
  end

  def normalize_override(_policy),
    do: {:error, "ERR flow retry policy must be a map or keyword list"}

  @spec resolve(map() | nil, binary(), map() | nil) :: t()
  def resolve(flow_policy, state, command_override) do
    default()
    |> merge_retry(policy_retry(flow_policy))
    |> merge_retry(state_retry(flow_policy, state))
    |> merge_retry(command_override)
  end

  def state_mode(%{states: states}, state) when is_map(states) and is_binary(state) do
    case Map.get(states, state) do
      %{mode: mode} when mode in [:parallel, :fifo] -> mode
      _other -> :parallel
    end
  end

  def state_mode(_flow_policy, _state), do: :parallel

  def state_fifo?(flow_policy, state), do: state_mode(flow_policy, state) == :fifo

  def fifo_states(%{states: states}) when is_map(states) do
    states
    |> Enum.flat_map(fn
      {state, %{mode: :fifo}} -> [state]
      _other -> []
    end)
    |> MapSet.new()
  end

  def fifo_states(_flow_policy), do: MapSet.new()

  def resolve_max_active_ms(flow_policy, command_override \\ nil)

  def resolve_max_active_ms(_flow_policy, command_override) when is_integer(command_override),
    do: command_override

  def resolve_max_active_ms(_flow_policy, command_override)
      when command_override in [:infinity, "infinity", "INFINITY"],
      do: nil

  def resolve_max_active_ms(%{max_active_ms: max_active_ms}, _command_override)
      when is_integer(max_active_ms),
      do: max_active_ms

  def resolve_max_active_ms(_flow_policy, _command_override), do: nil

  def resolve_retention(flow_policy, state, command_override) do
    default_retention()
    |> merge_retention(policy_retention(flow_policy))
    |> merge_retention(state_retention(flow_policy, state))
    |> merge_retention(command_override)
    |> normalize_resolved_retention_caps()
  end

  def indexed_attributes(%{indexed_attributes: names}) when is_list(names) do
    case Ferricstore.Flow.Attributes.normalize_indexed_names(names) do
      {:ok, names} -> names
      {:error, _reason} -> []
    end
  end

  def indexed_attributes(_policy), do: []

  def indexed_state_meta(%{indexed_state_meta: key}) do
    case Ferricstore.Flow.StateMeta.normalize_indexed_key(key) do
      {:ok, key} -> key
      {:error, _reason} -> nil
    end
  end

  def indexed_state_meta(_policy), do: nil

  defp optional_policy_version(attrs) do
    case fetch_policy(attrs, :version, "version", nil) do
      nil ->
        {:ok, nil}

      value when is_binary(value) and value != "" ->
        {:ok, value}

      value when is_integer(value) and value >= 0 ->
        {:ok, value}

      _other ->
        {:error, "ERR flow policy version must be a non-empty string or non-negative integer"}
    end
  end

  defp optional_max_active_ms(attrs) do
    if has_policy_key?(attrs, :max_active_ms, "max_active_ms") do
      attrs
      |> fetch_policy(:max_active_ms, "max_active_ms", nil)
      |> validate_max_active_ms()
    else
      {:ok, nil}
    end
  end

  def governance(%{governance: governance}) when is_map(governance), do: governance
  def governance(_policy), do: nil

  def normalize_retention_override(nil), do: {:ok, nil}

  def normalize_retention_override(retention) when is_list(retention) do
    if Keyword.keyword?(retention) do
      retention |> Map.new() |> normalize_retention_override()
    else
      {:error, "ERR flow retention policy must be a map or keyword list"}
    end
  end

  def normalize_retention_override(retention) when is_map(retention) do
    with {:ok, ttl_ms} <- optional_retention_ttl_ms(retention),
         {:ok, nil} <- optional_history_hot_max_events(retention),
         {:ok, history_max_events} <- optional_history_max_events(retention) do
      override =
        %{}
        |> maybe_put(:ttl_ms, ttl_ms)
        |> maybe_put(:history_max_events, history_max_events)

      {:ok, override}
    end
  end

  def normalize_retention_override(_retention),
    do: {:error, "ERR flow retention policy must be a map or keyword list"}

  @spec encode_flow_policy(map()) :: binary()
  def encode_flow_policy(policy) when is_map(policy) do
    :erlang.term_to_binary({:flow_policy_v1, policy})
  end

  @spec decode_flow_policy(binary()) :: {:ok, map()} | :error
  def decode_flow_policy(value) when is_binary(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {:flow_policy_v1, policy} when is_map(policy) ->
        if old_max_attempts_policy?(policy), do: :error, else: {:ok, policy}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_flow_policy(_value), do: :error

  @spec normalize(term()) :: {:ok, t()} | {:error, binary()}
  def normalize(nil), do: {:ok, default()}

  def normalize(policy) when is_list(policy) do
    if Keyword.keyword?(policy) do
      policy |> Map.new() |> normalize()
    else
      {:error, "ERR flow retry policy must be a map or keyword list"}
    end
  end

  def normalize(policy) when is_map(policy) do
    with :ok <- reject_old_max_attempts(policy),
         {:ok, max_retries} <- normalize_max_retries(policy),
         {:ok, backoff} <- normalize_backoff(policy),
         {:ok, exhausted_to} <- normalize_exhausted_to(policy) do
      {:ok, %{max_retries: max_retries, backoff: backoff, exhausted_to: exhausted_to}}
    end
  end

  def normalize(_policy), do: {:error, "ERR flow retry policy must be a map or keyword list"}

  @spec attempt_allowed?(t(), non_neg_integer()) :: boolean()
  def attempt_allowed?(%{max_retries: max_retries}, attempts) when is_integer(attempts),
    do: attempts <= max_retries

  @spec next_run_at_ms(t(), binary(), non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def next_run_at_ms(%{backoff: backoff}, id, attempt, now_ms)
      when is_binary(id) and is_integer(attempt) and is_integer(now_ms) do
    delay =
      backoff
      |> delay_ms(attempt)
      |> apply_jitter(backoff, id, attempt, now_ms)
      |> min(backoff.max_ms)

    now_ms + delay
  end

  defp normalize_max_retries(policy) do
    value = fetch_policy(policy, :max_retries, "max_retries", @default.max_retries)

    validate_max_retries(value)
  end

  defp optional_max_retries(policy) do
    if has_policy_key?(policy, :max_retries, "max_retries") do
      policy
      |> fetch_policy(:max_retries, "max_retries", nil)
      |> validate_max_retries()
    else
      {:ok, nil}
    end
  end

  defp validate_max_retries(value) do
    if is_integer(value) and value >= 0 and value <= @max_retries do
      {:ok, value}
    else
      {:error, "ERR flow retry max_retries must be between 0 and #{@max_retries}"}
    end
  end

  defp optional_backoff(policy) do
    if has_policy_key?(policy, :backoff, "backoff") do
      normalize_backoff(policy)
    else
      {:ok, nil}
    end
  end

  defp reject_old_max_attempts(policy) do
    if has_policy_key?(policy, :max_attempts, "max_attempts") do
      {:error, "ERR flow retry max_attempts was renamed to max_retries"}
    else
      :ok
    end
  end

  defp normalize_backoff(policy) do
    backoff = fetch_policy(policy, :backoff, "backoff", %{})

    backoff =
      cond do
        is_nil(backoff) -> %{}
        is_map(backoff) -> backoff
        is_list(backoff) and Keyword.keyword?(backoff) -> Map.new(backoff)
        true -> :invalid
      end

    if backoff == :invalid do
      {:error, "ERR flow retry backoff must be a map or keyword list"}
    else
      with {:ok, kind} <- normalize_backoff_kind(backoff),
           {:ok, base_ms} <-
             normalize_delay(backoff, :base_ms, "base_ms", @default.backoff.base_ms),
           {:ok, max_ms} <- normalize_delay(backoff, :max_ms, "max_ms", @default.backoff.max_ms),
           {:ok, jitter_pct} <- normalize_jitter(backoff) do
        {:ok, %{kind: kind, base_ms: base_ms, max_ms: max_ms, jitter_pct: jitter_pct}}
      end
    end
  end

  defp normalize_backoff_kind(backoff) do
    case fetch_policy(backoff, :kind, "kind", @default.backoff.kind) do
      kind when kind in [:none, :fixed, :linear, :exponential] ->
        {:ok, kind}

      kind when kind in ["none", "fixed", "linear", "exponential"] ->
        {:ok, String.to_existing_atom(kind)}

      _ ->
        {:error, "ERR flow retry backoff kind must be none, fixed, linear, or exponential"}
    end
  end

  defp normalize_delay(backoff, atom_key, string_key, default) do
    value = fetch_policy(backoff, atom_key, string_key, default)

    if is_integer(value) and value >= 0 and value <= @max_delay_ms do
      {:ok, value}
    else
      {:error, "ERR flow retry #{string_key} must be between 0 and #{@max_delay_ms}"}
    end
  end

  defp normalize_jitter(backoff) do
    value = fetch_policy(backoff, :jitter_pct, "jitter_pct", @default.backoff.jitter_pct)

    if is_integer(value) and value >= 0 and value <= 100 do
      {:ok, value}
    else
      {:error, "ERR flow retry jitter_pct must be between 0 and 100"}
    end
  end

  defp normalize_exhausted_to(policy) do
    case fetch_policy(policy, :exhausted_to, "exhausted_to", @default.exhausted_to) do
      "running" ->
        {:error, "ERR flow retry exhausted_to cannot be running"}

      value when is_binary(value) and value != "" ->
        {:ok, value}

      _ ->
        {:error, "ERR flow retry exhausted_to must be a non-empty string"}
    end
  end

  defp optional_exhausted_to(policy) do
    if has_policy_key?(policy, :exhausted_to, "exhausted_to") do
      normalize_exhausted_to(policy)
    else
      {:ok, nil}
    end
  end

  defp optional_retry_override(attrs) do
    if has_policy_key?(attrs, :retry, "retry") do
      attrs
      |> fetch_policy(:retry, "retry", nil)
      |> normalize_override()
    else
      {:ok, nil}
    end
  end

  defp optional_retention_override(attrs) do
    if has_policy_key?(attrs, :retention, "retention") do
      attrs
      |> fetch_policy(:retention, "retention", nil)
      |> normalize_retention_override()
    else
      {:ok, nil}
    end
  end

  defp optional_indexed_attributes(attrs) do
    if has_policy_key?(attrs, :indexed_attributes, "indexed_attributes") do
      attrs
      |> fetch_policy(:indexed_attributes, "indexed_attributes", [])
      |> Ferricstore.Flow.Attributes.normalize_indexed_names()
    else
      {:ok, nil}
    end
  end

  defp optional_indexed_state_meta(attrs) do
    if has_policy_key?(attrs, :indexed_state_meta, "indexed_state_meta") do
      attrs
      |> fetch_policy(:indexed_state_meta, "indexed_state_meta", nil)
      |> Ferricstore.Flow.StateMeta.normalize_indexed_key()
    else
      {:ok, nil}
    end
  end

  defp optional_governance(attrs) do
    if has_policy_key?(attrs, :governance, "governance") do
      attrs
      |> fetch_policy(:governance, "governance", nil)
      |> Ferricstore.Flow.Governance.Policy.normalize()
    else
      {:ok, nil}
    end
  end

  defp reject_top_level_state_mode(attrs) do
    if has_policy_key?(attrs, :mode, "mode") do
      {:error, "ERR flow state mode is state-level only"}
    else
      :ok
    end
  end

  defp normalize_state_policies(states) when is_map(states) do
    Enum.reduce_while(states, {:ok, %{}}, fn {state, policy}, {:ok, acc} ->
      with {:ok, state} <- normalize_state_name(state),
           policy = policy_map(policy),
           :ok <- reject_state_level_max_active_ms(policy),
           {:ok, mode} <- optional_state_mode(policy),
           {:ok, retry} <- optional_retry_override(policy),
           {:ok, retention} <- optional_retention_override(policy),
           {:ok, governance} <- optional_governance(policy) do
        {:cont,
         {:ok,
          Map.put(
            acc,
            state,
            drop_nil_policy_fields(%{
              mode: mode,
              retry: retry,
              retention: retention,
              governance: governance
            })
          )}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp normalize_state_policies(states) when is_list(states) do
    cond do
      Keyword.keyword?(states) ->
        states |> Map.new() |> normalize_state_policies()

      Enum.all?(states, &state_policy_pair?/1) ->
        states |> Map.new() |> normalize_state_policies()

      true ->
        {:error, "ERR flow policy states must be a map, keyword list, or state-policy pair list"}
    end
  end

  defp normalize_state_policies(_states),
    do: {:error, "ERR flow policy states must be a map or keyword list"}

  defp normalize_state_name(state) when is_binary(state) and state != "running" and state != "",
    do: {:ok, state}

  defp normalize_state_name("running"), do: {:error, "ERR flow policy state cannot be running"}

  defp normalize_state_name(_state),
    do: {:error, "ERR flow policy state must be a non-empty string"}

  defp state_policy_pair?({state, policy})
       when is_binary(state) and (is_map(policy) or is_list(policy)),
       do: true

  defp state_policy_pair?(_entry), do: false

  defp optional_state_mode(policy) do
    if has_policy_key?(policy, :mode, "mode") do
      policy
      |> fetch_policy(:mode, "mode", nil)
      |> normalize_state_mode()
    else
      {:ok, nil}
    end
  end

  defp normalize_state_mode(mode) when mode in [:parallel, :fifo], do: {:ok, mode}

  defp normalize_state_mode(mode) when is_binary(mode) do
    case String.downcase(mode) do
      "parallel" -> {:ok, :parallel}
      "fifo" -> {:ok, :fifo}
      _other -> {:error, "ERR flow state mode must be parallel or fifo"}
    end
  end

  defp normalize_state_mode(_mode),
    do: {:error, "ERR flow state mode must be parallel or fifo"}

  defp reject_state_level_max_active_ms(policy) do
    if has_policy_key?(policy, :max_active_ms, "max_active_ms") do
      {:error, "ERR flow max_active_ms is type-level only"}
    else
      :ok
    end
  end

  defp policy_map(policy) when is_map(policy), do: policy

  defp policy_map(policy) when is_list(policy) do
    if Keyword.keyword?(policy), do: Map.new(policy), else: %{}
  end

  defp policy_map(_policy), do: %{}

  defp merge_retry(policy, nil), do: policy
  defp merge_retry(policy, override) when override == %{}, do: policy

  defp merge_retry(policy, override) when is_map(override) do
    Enum.reduce(override, policy, fn
      {:backoff, backoff}, acc when is_map(backoff) ->
        Map.update!(acc, :backoff, &Map.merge(&1, backoff))

      {key, value}, acc when key in [:max_retries, :exhausted_to] ->
        Map.put(acc, key, value)

      _entry, acc ->
        acc
    end)
  end

  defp policy_retry(%{retry: retry}) when is_map(retry), do: retry
  defp policy_retry(_policy), do: nil

  defp state_retry(%{states: states}, state) when is_map(states) and is_binary(state) do
    case Map.get(states, state) do
      %{retry: retry} when is_map(retry) -> retry
      _ -> nil
    end
  end

  defp state_retry(_policy, _state), do: nil

  defp policy_retention(%{retention: retention}) when is_map(retention), do: retention
  defp policy_retention(_policy), do: nil

  defp state_retention(%{states: states}, state) when is_map(states) and is_binary(state) do
    case Map.get(states, state) do
      %{retention: retention} when is_map(retention) -> retention
      _ -> nil
    end
  end

  defp state_retention(_policy, _state), do: nil

  defp merge_retention(policy, nil), do: policy
  defp merge_retention(policy, override) when override == %{}, do: policy

  defp merge_retention(policy, override) when is_map(override) do
    Enum.reduce(override, policy, fn
      {key, value}, acc when key in [:ttl_ms, :history_hot_max_events, :history_max_events] ->
        Map.put(acc, key, value)

      _entry, acc ->
        acc
    end)
  end

  defp optional_retention_ttl_ms(policy) do
    if has_policy_key?(policy, :ttl_ms, "ttl_ms") do
      policy
      |> fetch_policy(:ttl_ms, "ttl_ms", nil)
      |> validate_retention_ttl_ms()
    else
      {:ok, nil}
    end
  end

  defp validate_retention_ttl_ms(value)
       when is_integer(value) and value > 0 and value <= @max_retention_ttl_ms,
       do: {:ok, value}

  defp validate_retention_ttl_ms(_value),
    do: {:error, "ERR flow retention ttl_ms must be between 1 and #{@max_retention_ttl_ms}"}

  defp validate_max_active_ms(value)
       when is_integer(value) and value > 0 and value <= @max_active_ms,
       do: {:ok, value}

  defp validate_max_active_ms(value) when value in [nil, :infinity, "infinity", "INFINITY"],
    do: {:ok, nil}

  defp validate_max_active_ms(_value),
    do: {:error, "ERR flow max_active_ms must be between 1 and #{@max_active_ms} or infinity"}

  defp optional_history_hot_max_events(policy) do
    if has_policy_key?(policy, :history_hot_max_events, "history_hot_max_events") do
      {:error, "ERR flow retention history_hot_max_events is internal"}
    else
      {:ok, nil}
    end
  end

  defp optional_history_max_events(policy) do
    if has_policy_key?(policy, :history_max_events, "history_max_events") do
      policy
      |> fetch_policy(:history_max_events, "history_max_events", nil)
      |> validate_history_max_events()
    else
      {:ok, nil}
    end
  end

  defp validate_history_max_events(value) when is_integer(value) and value > 0 do
    max = max_history_max_events()

    if value <= max do
      {:ok, value}
    else
      {:error, "ERR flow retention history_max_events must be between 1 and #{max}"}
    end
  end

  defp validate_history_max_events(_value) do
    max = max_history_max_events()
    {:error, "ERR flow retention history_max_events must be between 1 and #{max}"}
  end

  defp normalize_resolved_retention_caps(retention) do
    history_hot_max_events = Map.fetch!(retention, :history_hot_max_events)
    history_max_events = Map.fetch!(retention, :history_max_events)

    retention
    |> Map.put(:history_hot_max_events, min(history_hot_max_events, history_max_events))
    |> Map.put(:history_max_events, history_max_events)
  end

  defp default_retention_ttl_ms do
    case Application.get_env(
           :ferricstore,
           :flow_default_retention_ttl_ms,
           @default_retention.ttl_ms
         ) do
      value when is_integer(value) and value > 0 and value <= @max_retention_ttl_ms -> value
      _ -> @default_retention.ttl_ms
    end
  end

  defp default_history_hot_max_events do
    max = max_history_hot_max_events()

    case Application.get_env(
           :ferricstore,
           :flow_default_history_hot_max_events,
           @default_retention.history_hot_max_events
         ) do
      value when is_integer(value) and value >= 0 -> min(value, max)
      _ -> min(@default_retention.history_hot_max_events, max)
    end
  end

  defp max_history_hot_max_events do
    case Application.get_env(
           :ferricstore,
           :flow_max_history_hot_max_events,
           @max_history_hot_max_events
         ) do
      value when is_integer(value) and value > 0 -> value
      _ -> @max_history_hot_max_events
    end
  end

  defp default_history_max_events do
    max = max_history_max_events()
    hot_max = default_history_hot_max_events()

    default =
      case Application.get_env(
             :ferricstore,
             :flow_default_history_max_events,
             @default_retention.history_max_events
           ) do
        value when is_integer(value) and value > 0 -> min(value, max)
        _ -> min(@default_retention.history_max_events, max)
      end

    min(max(default, hot_max), max)
  end

  defp max_history_max_events do
    case Application.get_env(
           :ferricstore,
           :flow_max_history_max_events,
           @max_history_max_events
         ) do
      value when is_integer(value) and value > 0 -> min(value, @max_history_max_events)
      _ -> @max_history_max_events
    end
  end

  defp drop_nil_policy_fields(policy) do
    policy
    |> drop_nil_field(:retry)
    |> drop_nil_field(:retention)
    |> drop_nil_field(:indexed_attributes)
    |> drop_nil_field(:indexed_state_meta)
    |> drop_nil_field(:governance)
  end

  defp drop_nil_field(policy, key) do
    if Map.get(policy, key) == nil, do: Map.delete(policy, key), else: policy
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp delay_ms(%{kind: :none}, _attempt), do: 0
  defp delay_ms(%{kind: :fixed, base_ms: base_ms}, _attempt), do: base_ms

  defp delay_ms(%{kind: :linear, base_ms: base_ms, max_ms: max_ms}, attempt) do
    min(base_ms * max(attempt, 0), max_ms)
  end

  defp delay_ms(%{kind: :exponential, base_ms: base_ms, max_ms: max_ms}, attempt) do
    steps = max(attempt - 1, 0)

    if steps == 0 do
      min(base_ms, max_ms)
    else
      Enum.reduce_while(1..steps, base_ms, fn _, acc ->
        if acc >= max_ms do
          {:halt, max_ms}
        else
          {:cont, min(acc * 2, max_ms)}
        end
      end)
    end
  end

  defp apply_jitter(delay, %{jitter_pct: 0}, _id, _attempt, _now_ms), do: delay
  defp apply_jitter(0, _backoff, _id, _attempt, _now_ms), do: 0

  defp apply_jitter(delay, %{jitter_pct: jitter_pct}, id, attempt, now_ms) do
    spread = div(delay * jitter_pct, 100)

    if spread <= 0 do
      delay
    else
      offset = rem(:erlang.phash2({id, attempt, now_ms}), spread * 2 + 1) - spread
      max(delay + offset, 0)
    end
  end

  defp fetch_policy(policy, atom_key, string_key, default) do
    cond do
      is_map(policy) and Map.has_key?(policy, atom_key) -> Map.fetch!(policy, atom_key)
      is_map(policy) and Map.has_key?(policy, string_key) -> Map.fetch!(policy, string_key)
      true -> default
    end
  end

  defp has_policy_key?(policy, atom_key, string_key) do
    is_map(policy) and (Map.has_key?(policy, atom_key) or Map.has_key?(policy, string_key))
  end

  defp old_max_attempts_policy?(policy) when is_map(policy) do
    old_max_attempts_retry?(fetch_policy(policy, :retry, "retry", nil)) or
      old_max_attempts_states?(fetch_policy(policy, :states, "states", nil))
  end

  defp old_max_attempts_policy?(_policy), do: false

  defp old_max_attempts_states?(states) when is_map(states) do
    Enum.any?(states, fn
      {_state, %{retry: retry}} when is_map(retry) -> old_max_attempts_retry?(retry)
      {_state, %{"retry" => retry}} when is_map(retry) -> old_max_attempts_retry?(retry)
      _entry -> false
    end)
  end

  defp old_max_attempts_states?(_states), do: false

  defp old_max_attempts_retry?(retry) when is_map(retry),
    do: has_policy_key?(retry, :max_attempts, "max_attempts")

  defp old_max_attempts_retry?(_retry), do: false
end
