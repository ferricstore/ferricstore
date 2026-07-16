defmodule Ferricstore.Flow.Governance.Policy do
  @moduledoc false

  @default %{
    mode: :minimal,
    effects: %{allowed: [], denied: [], require_idempotency_key: true},
    audit: %{effect_events: true, denials: :sampled},
    limits: %{},
    budgets: %{},
    circuits: %{},
    approvals: %{effects: [], states: [], assignees: []}
  }

  @modes [:minimal, :ledger, :full]
  @denial_audit_modes [:off, :sampled, :all]
  @max_rules 1_000
  @max_string_list_values 1_000
  @max_string_bytes 65_535
  @max_error_class_bytes 256
  @max_policy_bytes 1_048_576
  @max_exact_integer 9_007_199_254_740_991
  @enforcement_modes [
    :strict_global,
    :approximate_global
  ]

  def default, do: @default

  def normalize(nil), do: {:ok, nil}

  def normalize(policy) when is_list(policy) do
    if Keyword.keyword?(policy) do
      policy |> Map.new() |> normalize()
    else
      {:error, "ERR flow governance policy must be a map or keyword list"}
    end
  end

  def normalize(policy) when is_map(policy) do
    with {:ok, mode} <- optional_mode(policy),
         {:ok, effects} <- optional_effects(policy),
         {:ok, audit} <- optional_audit(policy),
         {:ok, limits} <- optional_limits(policy),
         {:ok, budgets} <- optional_budgets(policy),
         {:ok, circuits} <- optional_circuits(policy),
         {:ok, approvals} <- optional_approvals(policy) do
      %{}
      |> maybe_put(:mode, mode)
      |> maybe_put(:effects, effects)
      |> maybe_put(:audit, audit)
      |> maybe_put(:limits, limits)
      |> maybe_put(:budgets, budgets)
      |> maybe_put(:circuits, circuits)
      |> maybe_put(:approvals, approvals)
      |> validate_policy_size()
    end
  end

  def normalize(_policy), do: {:error, "ERR flow governance policy must be a map or keyword list"}

  def resolve(policy, state \\ nil, _effect_type \\ nil) do
    base =
      default()
      |> merge_policy(flow_governance(policy))
      |> merge_policy(state_policy(policy, state))

    base
    |> Map.put(:policy_hash, policy_hash(base))
    |> maybe_put(:policy_version, policy_version(policy))
  end

  def policy_hash(policy) when is_map(policy) do
    policy
    |> Map.delete(:policy_hash)
    |> Map.delete(:policy_version)
    |> Map.delete(:version)
    |> canonical_term()
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  def effect_decision(policy, effect_type, opts) do
    effects = Map.get(policy, :effects, %{})
    denied = Map.get(effects, :denied, [])
    allowed = Map.get(effects, :allowed, [])
    idempotency_key = Keyword.get(opts, :idempotency_key)

    cond do
      effect_type in denied ->
        {:deny, :effect_denied, "Effect #{effect_type} is denied by policy"}

      allowed != [] and effect_type not in allowed ->
        {:deny, :effect_not_allowed, "Effect #{effect_type} is not allowed by policy"}

      Map.get(effects, :require_idempotency_key, true) and not present_binary?(idempotency_key) ->
        {:deny, :idempotency_key_required, "Effect #{effect_type} requires an idempotency_key"}

      true ->
        :allow
    end
  end

  def approval_required?(policy, state, effect_type) do
    approvals = Map.get(policy, :approvals, %{})
    effects = Map.get(approvals, :effects, [])
    states = Map.get(approvals, :states, [])

    (is_binary(effect_type) and effect_type in effects) or
      (is_binary(state) and state in states)
  end

  defp optional_mode(policy) do
    case fetch_policy(policy, :mode, "mode", nil) do
      nil ->
        {:ok, nil}

      mode when is_atom(mode) and mode in @modes ->
        {:ok, mode}

      mode when is_binary(mode) ->
        case String.downcase(mode) do
          "minimal" -> {:ok, :minimal}
          "ledger" -> {:ok, :ledger}
          "full" -> {:ok, :full}
          _other -> {:error, "ERR invalid flow governance mode"}
        end

      _other ->
        {:error, "ERR invalid flow governance mode"}
    end
  end

  defp optional_effects(policy) do
    case fetch_policy(policy, :effects, "effects", nil) do
      nil ->
        {:ok, nil}

      effects when is_list(effects) ->
        if Keyword.keyword?(effects) do
          optional_effects(%{effects: Map.new(effects)})
        else
          {:error, "ERR flow governance effects policy must be a map or keyword list"}
        end

      effects when is_map(effects) ->
        with {:ok, allowed} <- optional_string_list(effects, :allowed, "allowed"),
             {:ok, denied} <- optional_string_list(effects, :denied, "denied"),
             {:ok, require_idempotency_key} <-
               optional_boolean(effects, :require_idempotency_key, "require_idempotency_key") do
          {:ok,
           %{}
           |> maybe_put(:allowed, allowed)
           |> maybe_put(:denied, denied)
           |> maybe_put(:require_idempotency_key, require_idempotency_key)}
        end

      _other ->
        {:error, "ERR flow governance effects policy must be a map or keyword list"}
    end
  end

  defp optional_audit(policy) do
    case fetch_policy(policy, :audit, "audit", nil) do
      nil ->
        {:ok, nil}

      audit when is_list(audit) ->
        if Keyword.keyword?(audit) do
          optional_audit(%{audit: Map.new(audit)})
        else
          {:error, "ERR flow governance audit policy must be a map or keyword list"}
        end

      audit when is_map(audit) ->
        with {:ok, effect_events} <- optional_boolean(audit, :effect_events, "effect_events"),
             {:ok, denials} <- optional_denials(audit) do
          {:ok,
           %{}
           |> maybe_put(:effect_events, effect_events)
           |> maybe_put(:denials, denials)}
        end

      _other ->
        {:error, "ERR flow governance audit policy must be a map or keyword list"}
    end
  end

  defp optional_denials(audit) do
    case fetch_policy(audit, :denials, "denials", nil) do
      nil ->
        {:ok, nil}

      mode when is_atom(mode) and mode in @denial_audit_modes ->
        {:ok, mode}

      mode when is_binary(mode) ->
        case String.downcase(mode) do
          "off" -> {:ok, :off}
          "sampled" -> {:ok, :sampled}
          "all" -> {:ok, :all}
          _other -> {:error, "ERR invalid flow governance denials audit mode"}
        end

      _other ->
        {:error, "ERR invalid flow governance denials audit mode"}
    end
  end

  defp optional_limits(policy) do
    optional_rule_map(policy, :limits, "limits", &normalize_limit_rule/1)
  end

  defp optional_budgets(policy) do
    optional_rule_map(policy, :budgets, "budgets", &normalize_budget_rule/1)
  end

  defp optional_circuits(policy) do
    optional_rule_map(policy, :circuits, "circuits", &normalize_circuit_rule/1)
  end

  defp optional_approvals(policy) do
    case fetch_policy(policy, :approvals, "approvals", nil) do
      nil ->
        {:ok, nil}

      approvals when is_list(approvals) ->
        if Keyword.keyword?(approvals) do
          optional_approvals(%{approvals: Map.new(approvals)})
        else
          {:error, "ERR flow governance approvals policy must be a map or keyword list"}
        end

      approvals when is_map(approvals) ->
        with {:ok, effects} <- optional_string_list(approvals, :effects, "effects"),
             {:ok, states} <- optional_string_list(approvals, :states, "states"),
             {:ok, assignees} <- optional_string_list(approvals, :assignees, "assignees"),
             {:ok, timeout_ms} <- optional_positive_integer(approvals, :timeout_ms, "timeout_ms") do
          {:ok,
           %{}
           |> maybe_put(:effects, effects)
           |> maybe_put(:states, states)
           |> maybe_put(:assignees, assignees)
           |> maybe_put(:timeout_ms, timeout_ms)}
        end

      _other ->
        {:error, "ERR flow governance approvals policy must be a map or keyword list"}
    end
  end

  defp optional_rule_map(policy, atom_key, binary_key, normalizer) do
    case fetch_policy(policy, atom_key, binary_key, nil) do
      nil ->
        {:ok, nil}

      rules when is_list(rules) ->
        if Keyword.keyword?(rules) do
          optional_rule_map(%{atom_key => Map.new(rules)}, atom_key, binary_key, normalizer)
        else
          {:error, "ERR flow governance #{binary_key} policy must be a map or keyword list"}
        end

      rules when is_map(rules) ->
        if map_size(rules) <= @max_rules do
          rules
          |> Enum.reduce_while({:ok, %{}}, fn
            {name, rule}, {:ok, acc}
            when is_binary(name) and name != "" and byte_size(name) <= @max_string_bytes ->
              case normalizer.(rule) do
                {:ok, normalized} -> {:cont, {:ok, Map.put(acc, name, normalized)}}
                {:error, _reason} = error -> {:halt, error}
              end

            {name, _rule}, _acc when is_binary(name) and name != "" ->
              {:halt,
               {:error,
                "ERR flow governance #{binary_key} names must be at most #{@max_string_bytes} bytes"}}

            _entry, _acc ->
              {:halt,
               {:error, "ERR flow governance #{binary_key} names must be non-empty strings"}}
          end)
        else
          {:error,
           "ERR flow governance #{binary_key} policy must contain at most #{@max_rules} rules"}
        end

      _other ->
        {:error, "ERR flow governance #{binary_key} policy must be a map or keyword list"}
    end
  end

  defp normalize_limit_rule(rule) do
    with {:ok, rule} <- normalize_rule_map(rule, "limit"),
         {:ok, limit} <- required_non_negative_integer(rule, :limit, "limit"),
         {:ok, enforcement} <- optional_enforcement(rule),
         {:ok, lease_size} <- optional_positive_integer(rule, :lease_size, "lease_size") do
      {:ok,
       %{limit: limit}
       |> maybe_put(:enforcement, enforcement)
       |> maybe_put(:lease_size, lease_size)}
    end
  end

  defp normalize_budget_rule(rule) do
    with {:ok, rule} <- normalize_rule_map(rule, "budget"),
         {:ok, limit} <- required_non_negative_integer(rule, :limit, "limit"),
         {:ok, window_ms} <- required_positive_integer(rule, :window_ms, "window_ms"),
         {:ok, enforcement} <- optional_enforcement(rule),
         {:ok, lease_size} <- optional_positive_integer(rule, :lease_size, "lease_size"),
         {:ok, unit} <- optional_string(rule, :unit, "unit") do
      {:ok,
       %{limit: limit, window_ms: window_ms}
       |> maybe_put(:enforcement, enforcement)
       |> maybe_put(:lease_size, lease_size)
       |> maybe_put(:unit, unit)}
    end
  end

  defp normalize_circuit_rule(rule) do
    with {:ok, rule} <- normalize_rule_map(rule, "circuit"),
         {:ok, failure_threshold} <-
           required_positive_integer(rule, :failure_threshold, "failure_threshold"),
         {:ok, open_ms} <- required_positive_integer(rule, :open_ms, "open_ms"),
         {:ok, window_ms} <- optional_positive_integer(rule, :window_ms, "window_ms"),
         {:ok, min_calls} <- optional_positive_integer(rule, :min_calls, "min_calls"),
         {:ok, failure_rate_pct} <- optional_percent(rule, :failure_rate_pct, "failure_rate_pct"),
         {:ok, latency_threshold_ms} <-
           optional_positive_integer(rule, :latency_threshold_ms, "latency_threshold_ms"),
         {:ok, error_classes} <-
           optional_string_list(
             rule,
             :error_classes,
             "error_classes",
             @max_error_class_bytes
           ),
         {:ok, half_open_max_probes} <-
           optional_positive_integer(rule, :half_open_max_probes, "half_open_max_probes"),
         {:ok, half_open_success_threshold} <-
           optional_positive_integer(
             rule,
             :half_open_success_threshold,
             "half_open_success_threshold"
           ),
         {:ok, :ok} <-
           validate_circuit_tracking_bounds(failure_threshold, min_calls, failure_rate_pct) do
      {:ok,
       %{failure_threshold: failure_threshold, open_ms: open_ms}
       |> maybe_put(:window_ms, window_ms)
       |> maybe_put(:min_calls, min_calls)
       |> maybe_put(:failure_rate_pct, failure_rate_pct)
       |> maybe_put(:latency_threshold_ms, latency_threshold_ms)
       |> maybe_put(:error_classes, error_classes)
       |> maybe_put(:half_open_max_probes, half_open_max_probes)
       |> maybe_put(:half_open_success_threshold, half_open_success_threshold)}
    end
  end

  defp validate_circuit_tracking_bounds(failure_threshold, min_calls, failure_rate_pct) do
    cond do
      is_integer(min_calls) and min_calls > 64 ->
        {:error, "ERR flow governance circuit min_calls must be <= 64"}

      is_nil(failure_rate_pct) and failure_threshold > 64 ->
        {:error,
         "ERR flow governance circuit failure_threshold must be <= 64 without failure_rate_pct"}

      not is_nil(failure_rate_pct) and is_nil(min_calls) and failure_threshold > 64 ->
        {:error,
         "ERR flow governance circuit min_calls is required when failure_threshold exceeds 64"}

      true ->
        {:ok, :ok}
    end
  end

  defp optional_string_list(policy, atom_key, binary_key) do
    optional_string_list(policy, atom_key, binary_key, @max_string_bytes)
  end

  defp optional_string_list(policy, atom_key, binary_key, max_bytes) do
    case fetch_policy(policy, atom_key, binary_key, nil) do
      nil -> {:ok, nil}
      values when is_list(values) -> normalize_string_list(values, binary_key, max_bytes)
      _other -> {:error, "ERR flow governance #{binary_key} must be a list of strings"}
    end
  end

  defp normalize_string_list(values, name, max_bytes) do
    normalize_string_list(values, name, max_bytes, MapSet.new(), [], 0)
  end

  defp normalize_string_list([], _name, _max_bytes, _seen, values, _count),
    do: {:ok, Enum.reverse(values)}

  defp normalize_string_list(
         [_value | _rest],
         _name,
         _max_bytes,
         _seen,
         _values,
         @max_string_list_values
       ),
       do: {:error, "ERR flow governance list must contain at most 1000 values"}

  defp normalize_string_list([value | rest], name, max_bytes, seen, values, count)
       when is_binary(value) and value != "" and byte_size(value) <= max_bytes do
    if MapSet.member?(seen, value) do
      normalize_string_list(rest, name, max_bytes, seen, values, count + 1)
    else
      normalize_string_list(
        rest,
        name,
        max_bytes,
        MapSet.put(seen, value),
        [value | values],
        count + 1
      )
    end
  end

  defp normalize_string_list([value | _rest], name, max_bytes, _seen, _values, _count)
       when is_binary(value) and value != "",
       do: {:error, "ERR flow governance #{name} values must be at most #{max_bytes} bytes"}

  defp normalize_string_list(_values, _name, _max_bytes, _seen, _normalized, _count),
    do: {:error, "ERR flow governance list values must be non-empty strings"}

  defp optional_boolean(policy, atom_key, binary_key) do
    case fetch_policy(policy, atom_key, binary_key, nil) do
      nil -> {:ok, nil}
      value when is_boolean(value) -> {:ok, value}
      _other -> {:error, "ERR flow governance #{binary_key} must be boolean"}
    end
  end

  defp optional_string(policy, atom_key, binary_key) do
    case fetch_policy(policy, atom_key, binary_key, nil) do
      nil ->
        {:ok, nil}

      value when is_binary(value) and value != "" and byte_size(value) <= @max_string_bytes ->
        {:ok, value}

      _other ->
        {:error,
         "ERR flow governance #{binary_key} must be a non-empty string of at most #{@max_string_bytes} bytes"}
    end
  end

  defp optional_enforcement(policy) do
    case fetch_policy(policy, :enforcement, "enforcement", nil) do
      nil ->
        {:ok, nil}

      mode when is_atom(mode) and mode in @enforcement_modes ->
        {:ok, mode}

      mode when is_binary(mode) ->
        case String.downcase(mode) do
          "strict_global" -> {:ok, :strict_global}
          "approximate_global" -> {:ok, :approximate_global}
          _other -> {:error, "ERR invalid flow governance enforcement mode"}
        end

      _other ->
        {:error, "ERR invalid flow governance enforcement mode"}
    end
  end

  defp optional_positive_integer(policy, atom_key, binary_key) do
    case fetch_policy(policy, atom_key, binary_key, nil) do
      nil -> {:ok, nil}
      value when is_integer(value) and value > 0 and value <= @max_exact_integer -> {:ok, value}
      _other -> {:error, "ERR flow governance #{binary_key} must be a positive integer"}
    end
  end

  defp optional_percent(policy, atom_key, binary_key) do
    case fetch_policy(policy, atom_key, binary_key, nil) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 1 and value <= 100 -> {:ok, value}
      _other -> {:error, "ERR flow governance #{binary_key} must be an integer from 1 to 100"}
    end
  end

  defp required_positive_integer(policy, atom_key, binary_key) do
    case fetch_policy(policy, atom_key, binary_key, nil) do
      value when is_integer(value) and value > 0 and value <= @max_exact_integer -> {:ok, value}
      _other -> {:error, "ERR flow governance #{binary_key} must be a positive integer"}
    end
  end

  defp required_non_negative_integer(policy, atom_key, binary_key) do
    case fetch_policy(policy, atom_key, binary_key, nil) do
      value when is_integer(value) and value >= 0 and value <= @max_exact_integer -> {:ok, value}
      _other -> {:error, "ERR flow governance #{binary_key} must be a non-negative integer"}
    end
  end

  defp normalize_rule_map(rule, name) when is_list(rule) do
    if Keyword.keyword?(rule) do
      {:ok, Map.new(rule)}
    else
      {:error, "ERR flow governance #{name} rule must be a map or keyword list"}
    end
  end

  defp normalize_rule_map(rule, _name) when is_map(rule), do: {:ok, rule}

  defp normalize_rule_map(_rule, name) do
    {:error, "ERR flow governance #{name} rule must be a map or keyword list"}
  end

  defp flow_governance(%{governance: governance}) when is_map(governance), do: governance
  defp flow_governance(policy) when is_map(policy), do: policy
  defp flow_governance(_policy), do: nil

  defp state_policy(%{states: states}, state) when is_map(states) and is_binary(state) do
    Map.get(states, state, %{}) |> Map.get(:governance)
  end

  defp state_policy(_policy, _state), do: nil

  defp policy_version(%{version: version}) when is_binary(version) and version != "", do: version

  defp policy_version(%{version: version})
       when is_integer(version) and version >= 0 and version <= @max_exact_integer,
       do: version

  defp policy_version(%{governance: %{version: version}})
       when is_binary(version) and version != "",
       do: version

  defp policy_version(%{governance: %{version: version}})
       when is_integer(version) and version >= 0 and version <= @max_exact_integer,
       do: version

  defp policy_version(_policy), do: nil

  defp merge_policy(base, nil), do: base
  defp merge_policy(base, policy) when is_map(policy), do: deep_merge(base, policy)
  defp merge_policy(base, _policy), do: base

  defp deep_merge(left, right) do
    Map.merge(left, right, fn
      _key, left_value, right_value when is_map(left_value) and is_map(right_value) ->
        deep_merge(left_value, right_value)

      _key, _left_value, right_value ->
        right_value
    end)
  end

  defp canonical_term(value) when is_map(value) do
    value
    |> Enum.map(fn {key, value} -> {key, canonical_term(value)} end)
    |> Enum.sort_by(fn {key, _value} -> inspect(key) end)
  end

  defp canonical_term(values) when is_list(values), do: Enum.map(values, &canonical_term/1)
  defp canonical_term(value), do: value

  defp present_binary?(value), do: is_binary(value) and value != ""

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp validate_policy_size(policy) do
    if :erlang.external_size(policy) <= @max_policy_bytes do
      {:ok, policy}
    else
      {:error, "ERR flow governance policy exceeds #{@max_policy_bytes}-byte durable limit"}
    end
  end

  defp fetch_policy(policy, atom_key, binary_key, default) do
    cond do
      Map.has_key?(policy, atom_key) -> Map.fetch!(policy, atom_key)
      Map.has_key?(policy, binary_key) -> Map.fetch!(policy, binary_key)
      true -> default
    end
  end
end
