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
  alias Ferricstore.Flow.RetentionGuard
  alias Ferricstore.Store.Router
  alias Ferricstore.TermCodec

  @terminal_statuses [:confirmed, :failed, :compensated]
  @effect_statuses [:reserved | @terminal_statuses]
  @max_exact_integer 9_007_199_254_740_991
  @max_effect_field_bytes 262_144
  @max_record_bytes 900_000

  def reserve(ctx, id, effect_key, effect_type, opts \\ [])

  def reserve(ctx, id, effect_key, effect_type, opts)
      when is_binary(id) and is_binary(effect_key) and is_binary(effect_type) and is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           :ok <- validate_bounded_binary(:id, id, Router.max_key_size()),
           :ok <- validate_bounded_binary(:effect_key, effect_key, Router.max_key_size()),
           :ok <- validate_bounded_binary(:effect_type, effect_type, Router.max_key_size()),
           {:ok, record, partition_key} <- load_flow_record(ctx, id, opts),
           :ok <- validate_flow_lease(record, opts),
           {:ok, operation_digest} <-
             required_bounded_binary(opts, :operation_digest, @max_effect_field_bytes),
           {:ok, idempotency_key} <-
             optional_bounded_binary(opts, :idempotency_key, nil, @max_effect_field_bytes),
           {:ok, now_ms} <- optional_now_ms(opts),
           effect_key_bin = Keys.governance_effect_key(id, effect_key, partition_key),
           :ok <- validate_key_size(effect_key_bin) do
        reserve_or_replay_effect(ctx, %{
          record: record,
          effect_key: effect_key,
          effect_type: effect_type,
          operation_digest: operation_digest,
          idempotency_key: idempotency_key,
          now_ms: now_ms,
          opts: opts,
          storage_key: effect_key_bin
        })
      else
        false -> {:error, "ERR flow effect opts must be a keyword list"}
        {:error, _reason} = error -> error
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
    with true <- Keyword.keyword?(opts),
         :ok <- validate_bounded_binary(:id, id, Router.max_key_size()),
         :ok <- validate_bounded_binary(:effect_key, effect_key, Router.max_key_size()),
         {:ok, partition_key} <- optional_partition_key(opts),
         partition_key = partition_key || Keys.auto_partition_key(id),
         key = Keys.governance_effect_key(id, effect_key, partition_key),
         :ok <- validate_key_size(key) do
      case Router.get(ctx, key) do
        nil ->
          {:ok, nil}

        value when is_binary(value) ->
          with {:ok, effect} <- decode_effect_result(value), do: {:ok, public_effect(effect)}

        _other ->
          {:error, "ERR flow governance effect record is corrupt"}
      end
    else
      false -> {:error, "ERR flow effect opts must be a keyword list"}
      {:error, _reason} = error -> error
    end
  end

  def get(_ctx, _id, _effect_key, _opts),
    do: {:error, "ERR flow effect opts must be a keyword list"}

  defp update_status(ctx, id, effect_key, status, opts)
       when is_binary(id) and is_binary(effect_key) and status in @terminal_statuses and
              is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           :ok <- validate_bounded_binary(:id, id, Router.max_key_size()),
           :ok <- validate_bounded_binary(:effect_key, effect_key, Router.max_key_size()),
           :ok <- validate_terminal_options(opts),
           {:ok, record, partition_key} <- load_flow_record(ctx, id, opts),
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
             ),
           {:ok, finalized} <-
             finalize_effect_side_effects(ctx, key, record, updated, status) do
        {:ok, finalized}
      else
        false -> {:error, "ERR flow effect opts must be a keyword list"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(:"effect_#{status}", result, %{flow_id: id, effect_key: effect_key})
  end

  defp update_status(_ctx, _id, _effect_key, _status, _opts),
    do: {:error, "ERR flow effect opts must be a keyword list"}

  defp existing_effect_decision(ctx, key, operation_digest) do
    with {:ok, existing} <- get_existing_effect(ctx, key) do
      existing_effect_result(existing, key, operation_digest)
    end
  end

  defp existing_effect_result(existing, key, operation_digest) do
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

  defp reserve_or_replay_effect(ctx, reservation) do
    case get_optional_effect(ctx, reservation.storage_key) do
      {:ok, nil} ->
        reserve_new_effect(ctx, reservation)

      {:ok, existing} ->
        with {:ok, replayed} <-
               existing_effect_result(
                 existing,
                 reservation.storage_key,
                 reservation.operation_digest
               ) do
          finalize_effect_side_effects(
            ctx,
            reservation.storage_key,
            reservation.record,
            replayed,
            :reserved
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp reserve_new_effect(ctx, reservation) do
    with {:ok, policy} <-
           effective_policy(ctx, reservation.record, reservation.effect_type),
         :ok <-
           allowed_by_policy?(
             ctx,
             policy,
             reservation.record,
             reservation.effect_key,
             reservation.effect_type,
             reservation.opts,
             reservation.now_ms
           ),
         {:ok, circuit_permit} <-
           allowed_by_circuit?(
             ctx,
             policy,
             reservation.record,
             reservation.effect_key,
             reservation.effect_type,
             reservation.now_ms
           ),
         effect =
           effect_record(
             reservation.record,
             reservation.effect_key,
             reservation.effect_type,
             reservation.operation_digest,
             reservation.idempotency_key,
             policy,
             reservation.now_ms
           ),
         encoded = encode_effect(effect),
         :ok <- run_before_effect_set_hook(effect),
         set_result <-
           Router.set(ctx, reservation.storage_key, encoded, %{
             expire_at_ms: 0,
             nx: true,
             xx: false,
             get: false,
             keepttl: false,
             flow_retention_owner: retention_owner(reservation.record)
           }) do
      case set_result do
        :ok ->
          finalize_effect_side_effects(
            ctx,
            reservation.storage_key,
            reservation.record,
            Map.put(effect, :decision, :reserved),
            :reserved
          )

        nil ->
          with :ok <-
                 CircuitStore.release_permit(ctx, circuit_permit, reservation.now_ms),
               {:ok, replayed} <-
                 existing_effect_decision(
                   ctx,
                   reservation.storage_key,
                   reservation.operation_digest
                 ) do
            finalize_effect_side_effects(
              ctx,
              reservation.storage_key,
              reservation.record,
              replayed,
              :reserved
            )
          end

        {:error, _reason} = error ->
          case CircuitStore.release_permit(ctx, circuit_permit, reservation.now_ms) do
            :ok -> error
            {:error, _reason} = release_error -> release_error
          end
      end
    end
  end

  defp get_optional_effect(ctx, key) do
    case Router.get(ctx, key) do
      nil -> {:ok, nil}
      value when is_binary(value) -> decode_effect_result(value)
      {:error, _reason} = error -> error
      _other -> {:error, "ERR flow governance effect record is corrupt"}
    end
  end

  defp run_before_effect_set_hook(effect) do
    case Process.get(:ferricstore_governance_effect_before_set_hook) do
      hook when is_function(hook, 1) -> hook.(effect)
      _missing -> :ok
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

      now_ms < Map.get(effect, :updated_at_ms, Map.get(effect, :created_at_ms, 0)) ->
        {:error, "ERR flow effect now_ms cannot precede updated_at_ms"}

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

  defp finalize_effect_side_effects(ctx, key, record, effect, :reserved) do
    decision = Map.get(effect, :decision)

    with {:ok, finalized} <-
           ensure_effect_side_effects(ctx, key, record, effect, :reserved) do
      {:ok, public_effect(finalized, decision)}
    end
  end

  defp finalize_effect_side_effects(ctx, key, record, effect, status)
       when status in @terminal_statuses do
    decision = Map.get(effect, :decision)

    with {:ok, effect} <-
           ensure_effect_side_effects(ctx, key, record, effect, :reserved),
         {:ok, finalized} <-
           ensure_effect_side_effects(ctx, key, record, effect, status) do
      {:ok, public_effect(finalized, decision)}
    end
  end

  defp ensure_effect_side_effects(ctx, key, record, effect, status) do
    if side_effects_applied?(effect, status) do
      {:ok, effect}
    else
      at_ms = side_effect_at_ms(effect, status)

      with :ok <- write_effect_ledger(ctx, record, effect, status, at_ms),
           :ok <- maybe_record_circuit(ctx, record, effect, status, at_ms),
           {:ok, finalized} <- mark_side_effects_applied(ctx, key, status) do
        {:ok, finalized}
      end
    end
  end

  defp write_effect_ledger(ctx, record, effect, status, at_ms) do
    fields =
      effect
      |> Map.put(:status, status)
      |> Map.put(:event_id, effect_ledger_event_id(effect, status))

    write_ledger(ctx, record, :"effect_#{status}", fields, at_ms)
  end

  defp maybe_record_circuit(ctx, record, effect, status, now_ms)
       when status in [:confirmed, :failed] do
    case do_record_circuit(ctx, record, effect, status, now_ms) do
      {:ok, _circuit} -> :ok
      :ok -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp maybe_record_circuit(_ctx, _record, _effect, _status, _now_ms), do: :ok

  defp mark_side_effects_applied(ctx, key, status) do
    AtomicRecord.mutate(
      ctx,
      key,
      &decode_effect_result/1,
      &encode_effect/1,
      fn -> {:error, "ERR flow effect not found"} end,
      fn effect ->
        if side_effect_status_matches?(effect, status) do
          {:ok, put_applied_side_effect(effect, status)}
        else
          {:error, "ERR flow effect changed while finalizing governance side effects"}
        end
      end
    )
  end

  defp side_effect_status_matches?(_effect, :reserved), do: true
  defp side_effect_status_matches?(effect, status), do: Map.get(effect, :status) == status

  defp put_applied_side_effect(effect, status) do
    applied =
      effect
      |> Map.get(:applied_side_effects, [])
      |> Kernel.++([status])
      |> Enum.uniq()
      |> Enum.sort_by(&side_effect_status_rank/1)

    Map.put(effect, :applied_side_effects, applied)
  end

  defp side_effects_applied?(effect, status),
    do: status in Map.get(effect, :applied_side_effects, [])

  defp side_effect_status_rank(:reserved), do: 0
  defp side_effect_status_rank(:confirmed), do: 1
  defp side_effect_status_rank(:failed), do: 1
  defp side_effect_status_rank(:compensated), do: 1

  defp side_effect_at_ms(effect, :reserved), do: Map.fetch!(effect, :created_at_ms)
  defp side_effect_at_ms(effect, _terminal_status), do: Map.fetch!(effect, :updated_at_ms)

  defp effect_ledger_event_id(effect, status) do
    decision_id(
      "effect_ledger_#{status}",
      Map.fetch!(effect, :flow_id) <> ":" <> Map.fetch!(effect, :effect_key)
    )
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
      updated_at_ms: now_ms,
      applied_side_effects: []
    }
    |> put_circuit_metadata(policy, effect_type)
  end

  defp put_circuit_metadata(effect, policy, effect_type) do
    case circuit_rule(policy, effect_type) do
      {:ok, scope, rule} ->
        effect
        |> Map.put(:circuit_scope, scope)
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
      case CircuitStore.acquire(ctx, scope, now_ms) do
        {:ok, permit} ->
          {:ok, permit}

        {:error, denial} when is_map(denial) ->
          denial =
            denial
            |> Map.put(:policy_hash, Map.get(policy, :policy_hash))
            |> Map.put(:policy_version, Map.get(policy, :policy_version))

          write_denial_ledger(ctx, record, :circuit_open, effect_key, effect_type, denial, now_ms)
          {:error, denial}

        {:error, _reason} = error ->
          error
      end
    else
      {:ok, nil}
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
    case Ledger.append(ctx, record, event, effect, now_ms) do
      {:ok, :ok} -> :ok
      :ok -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp retention_owner(record) do
    id = Map.fetch!(record, :id)
    partition_key = Map.get(record, :partition_key)

    %{
      id: id,
      partition_key: partition_key,
      state_key: Keys.state_key(id, partition_key),
      expected_guard: RetentionGuard.encode(record)
    }
  end

  defp encode_effect(effect) do
    encoded_effect = Map.delete(effect, :decision)
    TermCodec.encode({:flow_governance_effect_v1, encoded_effect})
  end

  defp decode_effect_result(value) do
    case TermCodec.decode(value) do
      {:ok, {:flow_governance_effect_v1, effect}} when is_map(effect) ->
        if valid_effect?(effect) do
          {:ok, effect}
        else
          {:error, "ERR flow governance effect record is corrupt"}
        end

      _other ->
        {:error, "ERR flow governance effect record is corrupt"}
    end
  end

  defp valid_effect?(effect) do
    valid_required_binary(effect, :flow_id, Router.max_key_size()) and
      valid_required_binary(effect, :type, Router.max_key_size()) and
      valid_required_binary(effect, :state, Router.max_key_size()) and
      valid_required_binary(effect, :effect_key, Router.max_key_size()) and
      valid_required_binary(effect, :effect_type, Router.max_key_size()) and
      valid_required_binary(effect, :operation_digest, @max_effect_field_bytes) and
      valid_optional_binary(effect, :partition_key, Router.max_key_size()) and
      valid_optional_binary(effect, :idempotency_key, @max_effect_field_bytes) and
      valid_optional_binary(effect, :policy_hash, @max_effect_field_bytes) and
      valid_policy_version?(Map.get(effect, :policy_version)) and
      Map.get(effect, :status) in @effect_statuses and
      valid_timestamp?(Map.get(effect, :created_at_ms)) and
      valid_timestamp?(Map.get(effect, :updated_at_ms)) and
      Map.get(effect, :updated_at_ms) >= Map.get(effect, :created_at_ms) and
      valid_optional_binary(effect, :external_id, @max_effect_field_bytes) and
      valid_optional_binary(effect, :error, @max_effect_field_bytes) and
      valid_optional_binary(effect, :reason, @max_effect_field_bytes) and
      valid_optional_non_negative_integer(effect, :latency_ms) and
      valid_applied_side_effects?(effect) and valid_circuit_metadata?(effect) and
      :erlang.external_size(effect) <= @max_record_bytes
  end

  defp valid_applied_side_effects?(effect) do
    case {Map.get(effect, :status), Map.get(effect, :applied_side_effects)} do
      {:reserved, []} -> true
      {:reserved, [:reserved]} -> true
      {status, []} when status in @terminal_statuses -> true
      {status, [:reserved]} when status in @terminal_statuses -> true
      {status, [:reserved, status]} when status in @terminal_statuses -> true
      _invalid -> false
    end
  end

  defp valid_circuit_metadata?(effect) do
    scope_present? = Map.has_key?(effect, :circuit_scope)
    rule_present? = Map.has_key?(effect, :circuit_rule)

    cond do
      not scope_present? and not rule_present? ->
        true

      scope_present? and rule_present? ->
        scope = Map.get(effect, :circuit_scope)
        rule = Map.get(effect, :circuit_rule)

        if is_binary(scope) and scope != "" and byte_size(scope) <= Router.max_key_size() and
             is_map(rule) do
          case Policy.normalize(%{circuits: %{scope => rule}}) do
            {:ok, %{circuits: %{^scope => canonical}}} -> canonical == rule
            _invalid -> false
          end
        else
          false
        end

      true ->
        false
    end
  end

  defp valid_required_binary(map, key, max_bytes) do
    case Map.fetch(map, key) do
      {:ok, value} when is_binary(value) ->
        byte_size(value) > 0 and byte_size(value) <= max_bytes

      _missing_or_invalid ->
        false
    end
  end

  defp valid_optional_binary(map, key, max_bytes) do
    case Map.fetch(map, key) do
      :error -> true
      {:ok, nil} -> true
      {:ok, value} when is_binary(value) -> byte_size(value) <= max_bytes
      _invalid -> false
    end
  end

  defp valid_policy_version?(nil), do: true

  defp valid_policy_version?(version) when is_binary(version),
    do: byte_size(version) > 0 and byte_size(version) <= @max_effect_field_bytes

  defp valid_policy_version?(version), do: valid_timestamp?(version)

  defp valid_timestamp?(value),
    do: is_integer(value) and value >= 0 and value <= @max_exact_integer

  defp valid_optional_non_negative_integer(map, key) do
    case Map.fetch(map, key) do
      :error -> true
      {:ok, nil} -> true
      {:ok, value} -> valid_timestamp?(value)
    end
  end

  defp governance_scope(record, opts) do
    Keyword.get(opts, :governance_scope) ||
      Map.get(record, :partition_key) ||
      Keys.auto_partition_key(Map.fetch!(record, :id))
  end

  defp optional_partition_key(opts) do
    case Keyword.get(opts, :partition_key) do
      nil ->
        {:ok, nil}

      value when is_binary(value) ->
        if byte_size(value) <= Router.max_key_size(),
          do: {:ok, value},
          else: {:error, "ERR flow partition_key must be a string"}

      _other ->
        {:error, "ERR flow partition_key must be a string"}
    end
  end

  defp optional_now_ms(opts) do
    case Keyword.get(opts, :now_ms, CommandTime.now_ms()) do
      value when is_integer(value) and value >= 0 and value <= @max_exact_integer -> {:ok, value}
      _other -> {:error, "ERR flow now_ms must be a non-negative integer"}
    end
  end

  defp required_bounded_binary(opts, key, max_bytes) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" and byte_size(value) <= max_bytes ->
        {:ok, value}

      _other ->
        {:error, "ERR flow #{key} must be a non-empty string of at most #{max_bytes} bytes"}
    end
  end

  defp optional_bounded_binary(opts, key, default, max_bytes) do
    case Keyword.get(opts, key, default) do
      nil -> {:ok, nil}
      value when is_binary(value) and byte_size(value) <= max_bytes -> {:ok, value}
      _other -> {:error, "ERR flow #{key} must be a string of at most #{max_bytes} bytes"}
    end
  end

  defp required_binary(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, "ERR flow #{key} must be a non-empty string"}
    end
  end

  defp required_integer(opts, key) do
    case Keyword.get(opts, key) do
      value when is_integer(value) -> {:ok, value}
      _other -> {:error, "ERR flow #{key} must be an integer"}
    end
  end

  defp validate_bounded_binary(field, value, max_bytes) do
    if value != "" and byte_size(value) <= max_bytes do
      :ok
    else
      {:error, "ERR flow #{field} must be a non-empty string of at most #{max_bytes} bytes"}
    end
  end

  defp validate_terminal_options(opts) do
    with :ok <- validate_optional_binary_option(opts, :external_id),
         :ok <- validate_optional_binary_option(opts, :error),
         :ok <- validate_optional_binary_option(opts, :reason),
         :ok <- validate_optional_latency(opts) do
      :ok
    end
  end

  defp validate_optional_binary_option(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> :ok
      {:ok, nil} -> :ok
      {:ok, value} when is_binary(value) and byte_size(value) <= @max_effect_field_bytes -> :ok
      _invalid -> {:error, "ERR flow #{key} must be a string"}
    end
  end

  defp validate_optional_latency(opts) do
    case Keyword.fetch(opts, :latency_ms) do
      :error -> :ok
      {:ok, nil} -> :ok
      {:ok, value} when is_integer(value) and value >= 0 and value <= @max_exact_integer -> :ok
      _invalid -> {:error, "ERR flow latency_ms must be a non-negative integer"}
    end
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

  defp public_effect(effect, decision \\ nil) do
    effect =
      Map.drop(effect, [
        :applied_side_effects,
        :decision
      ])

    if is_nil(decision), do: effect, else: Map.put(effect, :decision, decision)
  end

  defp decision_id(reason, subject) do
    :crypto.hash(:sha256, "#{reason}:#{subject}")
    |> Base.url_encode64(padding: false)
  end
end
