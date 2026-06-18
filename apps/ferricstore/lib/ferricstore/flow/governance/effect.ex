defmodule Ferricstore.Flow.Governance.Effect do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.Governance.AtomicRecord
  alias Ferricstore.Flow.Governance.CircuitStore
  alias Ferricstore.Flow.Governance.Decision
  alias Ferricstore.Flow.Governance.Ledger
  alias Ferricstore.Flow.Governance.Policy
  alias Ferricstore.Flow.Governance.Telemetry
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  @terminal_statuses [:confirmed, :failed, :compensated]

  def reserve(ctx, id, effect_key, effect_type, opts \\ [])

  def reserve(ctx, id, effect_key, effect_type, opts)
      when is_binary(id) and is_binary(effect_key) and is_binary(effect_type) and is_list(opts) do
    result =
      with :ok <- validate_non_empty(:id, id),
           :ok <- validate_non_empty(:effect_key, effect_key),
           :ok <- validate_non_empty(:effect_type, effect_type),
           {:ok, record, partition_key} <- load_flow_record(ctx, id, opts),
           :ok <- validate_flow_lease(record, opts),
           {:ok, operation_digest} <- required_binary(opts, :operation_digest),
           {:ok, idempotency_key} <- optional_binary(opts, :idempotency_key, nil),
           {:ok, now_ms} <- optional_now_ms(opts),
           {:ok, policy} <- effective_policy(ctx, record, effect_type),
           :ok <- allowed_by_policy?(ctx, policy, record, effect_key, effect_type, opts, now_ms),
           :ok <- allowed_by_circuit?(ctx, policy, record, effect_key, effect_type, now_ms),
           effect =
             effect_record(
               record,
               effect_key,
               effect_type,
               operation_digest,
               idempotency_key,
               policy,
               now_ms
             ),
           effect_key_bin = Keys.governance_effect_key(id, effect_key, partition_key),
           :ok <- validate_key_size(effect_key_bin),
           encoded = encode_effect(effect),
           set_result <-
             Router.set(ctx, effect_key_bin, encoded, %{
               expire_at_ms: 0,
               nx: true,
               xx: false,
               get: false,
               keepttl: false
             }) do
        case set_result do
          :ok ->
            write_ledger(ctx, record, :effect_reserved, effect, now_ms)
            {:ok, Map.put(effect, :decision, :reserved)}

          nil ->
            existing_effect_decision(ctx, effect_key_bin, operation_digest)

          {:error, _reason} = error ->
            error
        end
      end

    Telemetry.emit(:effect_reserve, result, %{
      flow_id: id,
      effect_key: effect_key,
      effect_type: effect_type
    })
  end

  def reserve(_ctx, _id, _effect_key, _effect_type, _opts),
    do: {:error, "ERR flow effect opts must be a keyword list"}

  def confirm(ctx, id, effect_key, opts \\ []) do
    update_status(ctx, id, effect_key, :confirmed, opts)
  end

  def fail(ctx, id, effect_key, opts \\ []) do
    update_status(ctx, id, effect_key, :failed, opts)
  end

  def compensate(ctx, id, effect_key, opts \\ []) do
    update_status(ctx, id, effect_key, :compensated, opts)
  end

  def get(ctx, id, effect_key, opts \\ [])

  def get(ctx, id, effect_key, opts)
      when is_binary(id) and is_binary(effect_key) and is_list(opts) do
    with {:ok, partition_key} <- optional_partition_key(opts),
         key = Keys.governance_effect_key(id, effect_key, partition_key),
         :ok <- validate_key_size(key) do
      case Router.get(ctx, key) do
        nil -> {:ok, nil}
        value when is_binary(value) -> decode_effect_result(value)
        _other -> {:error, "ERR flow governance effect record is corrupt"}
      end
    end
  end

  def get(_ctx, _id, _effect_key, _opts),
    do: {:error, "ERR flow effect opts must be a keyword list"}

  defp update_status(ctx, id, effect_key, status, opts)
       when is_binary(id) and is_binary(effect_key) and status in @terminal_statuses and
              is_list(opts) do
    result =
      with {:ok, record, partition_key} <- load_flow_record(ctx, id, opts),
           :ok <- validate_flow_lease(record, opts),
           {:ok, now_ms} <- optional_now_ms(opts),
           key = Keys.governance_effect_key(id, effect_key, partition_key),
           :ok <- validate_key_size(key),
           {:ok, updated} <-
             AtomicRecord.mutate(
               ctx,
               key,
               &decode_effect_result/1,
               &encode_effect/1,
               fn -> {:error, "ERR flow effect not found"} end,
               fn effect ->
                 case transition_effect(effect, status, opts, now_ms) do
                   {:ok, updated} -> {:ok, updated}
                   {:error, _reason} = error -> error
                 end
               end
             ) do
        write_ledger(ctx, record, :"effect_#{status}", updated, now_ms)
        maybe_record_circuit(ctx, record, updated, status, now_ms)
        {:ok, updated}
      end

    Telemetry.emit(:"effect_#{status}", result, %{flow_id: id, effect_key: effect_key})
  end

  defp update_status(_ctx, _id, _effect_key, _status, _opts),
    do: {:error, "ERR flow effect opts must be a keyword list"}

  defp existing_effect_decision(ctx, key, operation_digest) do
    with {:ok, existing} <- get_existing_effect(ctx, key) do
      if Map.get(existing, :operation_digest) == operation_digest do
        {:ok, Map.put(existing, :decision, :already_reserved)}
      else
        {:error,
         Decision.conflict(%{
           message: "Effect key already exists with a different operation_digest",
           policy: "effect_idempotency",
           effect_key: Map.get(existing, :effect_key),
           status: Map.get(existing, :status),
           decision_id: decision_id("effect_conflict", key)
         })}
      end
    end
  end

  defp transition_effect(effect, status, opts, now_ms) do
    current_status = Map.get(effect, :status)

    cond do
      current_status == status ->
        {:ok, Map.put(effect, :decision, :already_applied)}

      current_status in @terminal_statuses and current_status != status ->
        {:error,
         Decision.conflict(%{
           message: "Effect is already terminal",
           policy: "effect_lifecycle",
           effect_key: Map.get(effect, :effect_key),
           current_status: current_status,
           requested_status: status,
           decision_id: decision_id("effect_terminal", Map.get(effect, :effect_key, ""))
         })}

      true ->
        {:ok,
         effect
         |> Map.put(:status, status)
         |> Map.put(:updated_at_ms, now_ms)
         |> maybe_put_binary(:external_id, Keyword.get(opts, :external_id))
         |> maybe_put_binary(:error, Keyword.get(opts, :error))
         |> maybe_put_binary(:reason, Keyword.get(opts, :reason))
         |> maybe_put_non_negative_integer(:latency_ms, Keyword.get(opts, :latency_ms))
         |> Map.put(:decision, status)}
    end
  end

  defp maybe_record_circuit(ctx, record, effect, status, now_ms) do
    if Map.get(effect, :decision) == status and status in [:confirmed, :failed] do
      do_record_circuit(ctx, record, effect, status, now_ms)
    else
      :ok
    end
  end

  defp do_record_circuit(ctx, _record, effect, status, now_ms) do
    with {:ok, scope, rule} <- effect_circuit_rule(effect) do
      opts =
        rule
        |> Map.to_list()
        |> Keyword.new()
        |> Keyword.merge(
          now_ms: now_ms,
          latency_ms: Map.get(effect, :latency_ms),
          error_class: Map.get(effect, :reason) || Map.get(effect, :error)
        )

      case status do
        :failed -> CircuitStore.record_failure(ctx, scope, opts)
        :confirmed -> CircuitStore.record_success(ctx, scope, opts)
      end
    else
      :skip -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp circuit_rule(policy, effect_type) do
    scope = "effect:" <> effect_type

    case Map.fetch(Map.get(policy, :circuits, %{}), scope) do
      {:ok, rule} -> {:ok, scope, rule}
      :error -> :skip
    end
  end

  defp effect_circuit_rule(effect) do
    case {Map.get(effect, :circuit_scope), Map.get(effect, :circuit_rule)} do
      {scope, rule} when is_binary(scope) and scope != "" and is_map(rule) ->
        {:ok, scope, rule}

      _other ->
        old_effect_circuit_rule(effect)
    end
  end

  defp old_effect_circuit_rule(effect) do
    case {
      Map.get(effect, :circuit_scope),
      Map.get(effect, :circuit_failure_threshold),
      Map.get(effect, :circuit_open_ms)
    } do
      {scope, failure_threshold, open_ms}
      when is_binary(scope) and scope != "" and is_integer(failure_threshold) and
             failure_threshold > 0 and is_integer(open_ms) and open_ms > 0 ->
        {:ok, scope, %{failure_threshold: failure_threshold, open_ms: open_ms}}

      _other ->
        :skip
    end
  end

  defp effect_record(
         record,
         effect_key,
         effect_type,
         operation_digest,
         idempotency_key,
         policy,
         now_ms
       ) do
    %{
      flow_id: Map.fetch!(record, :id),
      partition_key: Map.get(record, :partition_key),
      type: Map.get(record, :type),
      state: Map.get(record, :state),
      effect_key: effect_key,
      effect_type: effect_type,
      status: :reserved,
      operation_digest: operation_digest,
      idempotency_key: idempotency_key,
      policy_hash: Map.get(policy, :policy_hash),
      policy_version: Map.get(policy, :policy_version),
      created_at_ms: now_ms,
      updated_at_ms: now_ms
    }
    |> put_circuit_metadata(policy, effect_type)
  end

  defp put_circuit_metadata(effect, policy, effect_type) do
    case circuit_rule(policy, effect_type) do
      {:ok, scope, rule} ->
        effect
        |> Map.put(:circuit_scope, scope)
        |> Map.put(:circuit_failure_threshold, Map.fetch!(rule, :failure_threshold))
        |> Map.put(:circuit_open_ms, Map.fetch!(rule, :open_ms))
        |> Map.put(:circuit_rule, rule)

      :skip ->
        effect
    end
  end

  defp allowed_by_policy?(ctx, policy, record, effect_key, effect_type, opts, now_ms) do
    if Policy.approval_required?(policy, Map.get(record, :state), effect_type) do
      denial =
        Decision.approval_required(%{
          policy: "approval_required",
          scope: governance_scope(record, opts),
          type: Map.get(record, :type),
          state: Map.get(record, :state),
          effect_type: effect_type,
          policy_hash: Map.get(policy, :policy_hash),
          policy_version: Map.get(policy, :policy_version),
          enforcement: "strict_local",
          decision_id:
            decision_id("approval_required", Map.get(record, :id) <> ":" <> effect_type)
        })

      write_denial_ledger(
        ctx,
        record,
        :approval_required,
        effect_key,
        effect_type,
        denial,
        now_ms
      )

      {:error, denial}
    else
      effect_allowed_by_policy?(ctx, policy, record, effect_key, effect_type, opts, now_ms)
    end
  end

  defp allowed_by_circuit?(ctx, policy, record, effect_key, effect_type, now_ms) do
    circuits = Map.get(policy, :circuits, %{})
    scope = "effect:" <> effect_type

    if Map.has_key?(circuits, scope) do
      case CircuitStore.allow(ctx, scope, now_ms) do
        :ok ->
          :ok

        {:error, denial} ->
          denial =
            denial
            |> Map.put(:policy_hash, Map.get(policy, :policy_hash))
            |> Map.put(:policy_version, Map.get(policy, :policy_version))

          write_denial_ledger(ctx, record, :circuit_open, effect_key, effect_type, denial, now_ms)
          {:error, denial}
      end
    else
      :ok
    end
  end

  defp effect_allowed_by_policy?(ctx, policy, record, effect_key, effect_type, opts, now_ms) do
    case Policy.effect_decision(policy, effect_type, opts) do
      :allow ->
        :ok

      {:deny, reason, message} ->
        denial =
          Decision.effect_denied(%{
            message: message,
            policy: Atom.to_string(reason),
            scope: governance_scope(record, opts),
            type: Map.get(record, :type),
            state: Map.get(record, :state),
            effect_type: effect_type,
            policy_hash: Map.get(policy, :policy_hash),
            policy_version: Map.get(policy, :policy_version),
            enforcement: "strict_local",
            decision_id: decision_id(reason, Map.get(record, :id) <> ":" <> effect_type)
          })

        write_denial_ledger(ctx, record, :effect_denied, effect_key, effect_type, denial, now_ms)
        {:error, denial}
    end
  end

  defp write_denial_ledger(ctx, record, kind, effect_key, effect_type, denial, now_ms) do
    Ledger.append(
      ctx,
      record,
      kind,
      %{
        effect_key: effect_key,
        effect_type: effect_type,
        status: :denied,
        code: Map.get(denial, :code),
        message: Map.get(denial, :message),
        policy: Map.get(denial, :policy),
        policy_hash: Map.get(denial, :policy_hash),
        policy_version: Map.get(denial, :policy_version)
      },
      now_ms
    )
  end

  defp effective_policy(ctx, record, effect_type) do
    case Ferricstore.Flow.Policy.raw(ctx, Map.get(record, :type)) do
      {:ok, flow_policy} ->
        {:ok, Policy.resolve(flow_policy, Map.get(record, :state), effect_type)}

      {:error, _reason} = error ->
        error
    end
  end

  defp load_flow_record(ctx, id, opts) do
    with {:ok, partition_key} <- optional_partition_key(opts) do
      case Router.flow_get(ctx, id, partition_key) do
        value when is_binary(value) ->
          record = Codec.decode_record(value)

          partition_key =
            Map.get(record, :partition_key) || partition_key || Keys.auto_partition_key(id)

          {:ok, record, partition_key}

        nil ->
          {:error, "ERR flow not found"}

        _other ->
          {:error, "ERR flow record is corrupt"}
      end
    end
  rescue
    _ -> {:error, "ERR flow record is corrupt"}
  end

  defp validate_flow_lease(record, opts) do
    with {:ok, lease_token} <- required_binary(opts, :lease_token),
         {:ok, fencing_token} <- required_integer(opts, :fencing_token) do
      cond do
        Map.get(record, :lease_token) != lease_token ->
          {:error, "ERR flow lease token mismatch"}

        Map.get(record, :fencing_token) != fencing_token ->
          {:error, "ERR flow fencing token mismatch"}

        true ->
          :ok
      end
    end
  end

  defp get_existing_effect(ctx, key) do
    case Router.get(ctx, key) do
      value when is_binary(value) -> decode_effect_result(value)
      nil -> {:error, "ERR flow governance effect not found"}
      _other -> {:error, "ERR flow governance effect record is corrupt"}
    end
  end

  defp write_ledger(ctx, record, event, effect, now_ms) do
    Ledger.append(ctx, record, event, effect, now_ms)
  end

  defp encode_effect(effect), do: :erlang.term_to_binary({:flow_governance_effect_v1, effect})

  defp decode_effect_result(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {:flow_governance_effect_v1, effect} when is_map(effect) -> {:ok, effect}
      _other -> {:error, "ERR flow governance effect record is corrupt"}
    end
  rescue
    _ -> {:error, "ERR flow governance effect record is corrupt"}
  end

  defp governance_scope(record, opts) do
    Keyword.get(opts, :governance_scope) ||
      Map.get(record, :partition_key) ||
      Keys.auto_partition_key(Map.fetch!(record, :id))
  end

  defp optional_partition_key(opts) do
    case Keyword.get(opts, :partition_key) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      _other -> {:error, "ERR flow partition_key must be a string"}
    end
  end

  defp optional_now_ms(opts) do
    case Keyword.get(opts, :now_ms, CommandTime.now_ms()) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _other -> {:error, "ERR flow now_ms must be a non-negative integer"}
    end
  end

  defp required_binary(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, "ERR flow #{key} must be a non-empty string"}
    end
  end

  defp optional_binary(opts, key, default) do
    case Keyword.get(opts, key, default) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      _other -> {:error, "ERR flow #{key} must be a string"}
    end
  end

  defp required_integer(opts, key) do
    case Keyword.get(opts, key) do
      value when is_integer(value) -> {:ok, value}
      _other -> {:error, "ERR flow #{key} must be an integer"}
    end
  end

  defp validate_non_empty(field, value) do
    if value != "", do: :ok, else: {:error, "ERR flow #{field} must be a non-empty string"}
  end

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  defp maybe_put_binary(map, _key, nil), do: map
  defp maybe_put_binary(map, key, value) when is_binary(value), do: Map.put(map, key, value)
  defp maybe_put_binary(map, _key, _value), do: map

  defp maybe_put_non_negative_integer(map, _key, nil), do: map

  defp maybe_put_non_negative_integer(map, key, value) when is_integer(value) and value >= 0,
    do: Map.put(map, key, value)

  defp maybe_put_non_negative_integer(map, _key, _value), do: map

  defp decision_id(reason, subject) do
    :crypto.hash(:sha256, "#{reason}:#{subject}")
    |> Base.url_encode64(padding: false)
  end
end
