defmodule Ferricstore.Flow.Governance.LimitStore do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Governance.Catalog
  alias Ferricstore.Flow.Governance.CreditLease
  alias Ferricstore.Flow.Governance.LimitRecord
  alias Ferricstore.Flow.Governance.Telemetry
  alias Ferricstore.Flow.Governance.View
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  @max_mutation_amount 1_000
  @max_exact_version 9_007_199_254_740_991
  @max_policy_version_bytes 262_144

  def lease(ctx, scope, opts \\ [])

  def lease(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           {:ok, key} <- limit_key(scope),
           {:ok, shard_id} <- required_shard_id(ctx, opts),
           {:ok, amount} <- required_mutation_amount(opts, :amount),
           {:ok, ttl_ms} <- required_positive_integer(opts, :ttl_ms),
           {:ok, now_ms} <- optional_now_ms(opts),
           :ok <- validate_deadline(now_ms, ttl_ms),
           {:ok, configuration} <- limit_configuration(opts),
           owner_exists? = not is_nil(Router.get(ctx, key)),
           :ok <- require_limit_for_new_owner(owner_exists?, configuration),
           :ok <- register_existing_limit(ctx, key, owner_exists?) do
        result =
          Router.flow_governance_limit_mutate(ctx, key, %{
            op: :lease,
            scope: scope,
            shard_id: shard_id,
            shard_count: ctx.shard_count,
            amount: amount,
            ttl_ms: ttl_ms,
            now_ms: now_ms,
            configuration: configuration
          })

        register_committed_limit(ctx, key, result, opts)
      else
        false -> {:error, "ERR flow limit opts must be a keyword list"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(:limit_lease, result, limit_metadata(scope, opts))
  end

  def lease(_ctx, _scope, _opts), do: {:error, "ERR flow limit opts must be a keyword list"}

  def spend(ctx, scope, opts \\ [])

  def spend(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           {:ok, key} <- limit_key(scope),
           {:ok, shard_id} <- required_shard_id(ctx, opts),
           {:ok, amount} <- required_mutation_amount(opts, :amount),
           {:ok, now_ms} <- optional_now_ms(opts),
           {:ok, ttl_ms} <- optional_positive_integer(opts, :ttl_ms),
           :ok <- validate_deadline(now_ms, ttl_ms),
           {:ok, lease_epoch} <- current_lease_epoch(ctx, key, shard_id),
           :ok <- ensure_limit_registered(ctx, key),
           {:ok, reservation_ids} <- spend_reservation_ids(opts, amount, lease_epoch),
           {:ok, configuration} <- limit_configuration(opts) do
        ctx
        |> Router.flow_governance_limit_mutate(key, %{
          op: :spend,
          scope: scope,
          shard_id: shard_id,
          shard_count: ctx.shard_count,
          lease_epoch: lease_epoch,
          amount: amount,
          now_ms: now_ms,
          ttl_ms: ttl_ms,
          reservation_ids: reservation_ids,
          configuration: configuration
        })
        |> normalize_spend_result(scope, shard_id, reservation_ids)
      else
        false -> {:error, "ERR flow limit opts must be a keyword list"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(:limit_spend, result, limit_metadata(scope, opts))
  end

  def spend(_ctx, _scope, _opts), do: {:error, "ERR flow limit opts must be a keyword list"}

  @doc false
  def spend_reserved(ctx, scope, opts, reservation_ids)
      when is_binary(scope) and scope != "" and is_list(opts) and is_list(reservation_ids) do
    result =
      with true <- Keyword.keyword?(opts),
           {:ok, key} <- limit_key(scope),
           {:ok, shard_id} <- required_shard_id(ctx, opts),
           {:ok, amount} <- required_mutation_amount(opts, :amount),
           {:ok, reservation_ids} <- validate_preallocated_ids(reservation_ids, amount),
           {:ok, now_ms} <- optional_now_ms(opts),
           {:ok, ttl_ms} <- optional_positive_integer(opts, :ttl_ms),
           :ok <- validate_deadline(now_ms, ttl_ms),
           {:ok, configuration} <- limit_configuration(opts),
           {:ok, _current_epoch} <- current_lease_epoch(ctx, key, shard_id),
           :ok <- ensure_limit_registered(ctx, key),
           {:ok, lease_epoch} <- reservation_ids_epoch(reservation_ids) do
        Router.flow_governance_limit_mutate(ctx, key, %{
          op: :spend,
          scope: scope,
          shard_id: shard_id,
          shard_count: ctx.shard_count,
          lease_epoch: lease_epoch,
          amount: amount,
          now_ms: now_ms,
          ttl_ms: ttl_ms,
          reservation_ids: reservation_ids,
          configuration: configuration
        })
      else
        false -> {:error, "ERR flow limit reserved spend opts are invalid"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(:limit_spend_reserved, result, limit_metadata(scope, opts))
  end

  def spend_reserved(_ctx, _scope, _opts, _reservation_ids),
    do: {:error, "ERR flow limit reserved spend opts are invalid"}

  @doc false
  def generate_reservation_ids(ctx, scope, shard_id, amount)
      when is_binary(scope) and scope != "" and is_integer(amount) and amount > 0 and
             amount <= @max_mutation_amount do
    with {:ok, shard_id} <- required_shard_id(ctx, shard_id: shard_id),
         {:ok, key} <- limit_key(scope),
         {:ok, lease_epoch} <-
           current_lease_epoch(ctx, key, shard_id) do
      {:ok, new_reservation_ids(amount, lease_epoch)}
    end
  end

  def generate_reservation_ids(_ctx, _scope, _shard_id, _amount),
    do: {:error, "ERR flow limit reservation preallocation options are invalid"}

  @doc false
  def normalize_spend_result(
        {:error, {:timeout, :unknown_outcome}},
        scope,
        shard_id,
        reservation_ids
      )
      when is_binary(scope) and is_integer(shard_id) and is_list(reservation_ids) do
    {:error,
     %{
       code: "FLOW_LIMIT_SPEND_UNKNOWN_OUTCOME",
       scope: scope,
       shard_id: shard_id,
       reservation_ids: reservation_ids,
       message:
         "Flow limit spend outcome is unknown; release these reservation_ids before retrying"
     }}
  end

  def normalize_spend_result(result, _scope, _shard_id, _reservation_ids), do: result

  def renew(ctx, scope, opts \\ [])

  def renew(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           {:ok, key} <- limit_key(scope),
           {:ok, shard_id} <- required_shard_id(ctx, opts),
           {:ok, ttl_ms} <- required_positive_integer(opts, :ttl_ms),
           {:ok, now_ms} <- optional_now_ms(opts),
           :ok <- validate_deadline(now_ms, ttl_ms),
           :ok <- ensure_existing_limit_registered(ctx, key) do
        Router.flow_governance_limit_mutate(ctx, key, %{
          op: :renew,
          scope: scope,
          shard_id: shard_id,
          shard_count: ctx.shard_count,
          ttl_ms: ttl_ms,
          now_ms: now_ms
        })
      else
        false -> {:error, "ERR flow limit opts must be a keyword list"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(:limit_renew, result, limit_metadata(scope, opts))
  end

  def renew(_ctx, _scope, _opts), do: {:error, "ERR flow limit opts must be a keyword list"}

  def release(ctx, scope, opts \\ [])

  def release(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           {:ok, key} <- limit_key(scope),
           {:ok, shard_id} <- required_shard_id(ctx, opts),
           {:ok, reservation_ids} <- release_reservation_ids(opts),
           {:ok, amount} <- release_amount(opts, reservation_ids),
           {:ok, now_ms} <- optional_now_ms(opts),
           :ok <- ensure_existing_limit_registered(ctx, key) do
        Router.flow_governance_limit_mutate(ctx, key, %{
          op: :release,
          scope: scope,
          shard_id: shard_id,
          shard_count: ctx.shard_count,
          amount: amount,
          reservation_ids: reservation_ids,
          now_ms: now_ms
        })
      else
        false -> {:error, "ERR flow limit opts must be a keyword list"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(:limit_release, result, limit_metadata(scope, opts))
  end

  def release(_ctx, _scope, _opts), do: {:error, "ERR flow limit opts must be a keyword list"}

  def get(ctx, scope, opts \\ [])

  def get(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           {:ok, key} <- limit_key(scope),
           {:ok, now_ms} <- optional_now_ms(opts) do
        case Router.get(ctx, key) do
          nil ->
            {:ok, nil}

          _existing ->
            with :ok <- ensure_limit_registered(ctx, key) do
              Router.flow_governance_limit_mutate(ctx, key, %{
                op: :get,
                scope: scope,
                now_ms: now_ms
              })
            end
        end
      else
        false -> {:error, "ERR flow limit opts must be a keyword list"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(:limit_reclaim, result, %{scope: scope})
  end

  def get(_ctx, _scope, _opts), do: {:error, "ERR flow limit opts must be a keyword list"}

  @doc false
  def cleanup(ctx, scope, opts \\ [])

  def cleanup(ctx, scope, opts)
      when is_binary(scope) and scope != "" and is_list(opts) do
    with true <- Keyword.keyword?(opts),
         {:ok, key} <- limit_key(scope),
         {:ok, now_ms} <- optional_now_ms(opts) do
      case Router.get(ctx, key) do
        nil ->
          {:ok, %{deleted: 0, pending?: false}}

        _existing ->
          Router.flow_governance_limit_mutate(ctx, key, %{
            op: :cleanup,
            scope: scope,
            now_ms: now_ms
          })
      end
    else
      false -> {:error, "ERR flow limit cleanup opts must be a keyword list"}
      {:error, _reason} = error -> error
    end
  end

  def cleanup(_ctx, _scope, _opts),
    do: {:error, "ERR flow limit cleanup opts must be a keyword list"}

  def list(ctx, opts \\ [])

  def list(ctx, opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         {:ok, limit} <- optional_list_limit(opts),
         {:ok, scopes} <- optional_scope_filters(opts),
         {:ok, now_ms} <- optional_now_ms(opts),
         {:ok, limits} <-
           collect_list_limits(ctx, scopes, limit, now_ms) do
      {:ok, limits}
    else
      false -> {:error, "ERR flow limit opts must be a keyword list"}
      {:error, _reason} = error -> error
    end
  end

  def list(_ctx, _opts), do: {:error, "ERR flow limit opts must be a keyword list"}

  defp collect_list_limits(ctx, nil, limit, now_ms) do
    Catalog.collect(
      ctx,
      :limit,
      limit,
      &load_list_limit(ctx, &1, nil, now_ms),
      &Map.get(&1, :scope)
    )
  end

  defp collect_list_limits(ctx, scopes, limit, now_ms) when is_list(scopes) do
    keys = Enum.map(scopes, &Keys.governance_limit_key/1)

    Catalog.collect_keys(
      keys,
      limit,
      &load_list_limit(ctx, &1, scopes, now_ms),
      &Map.get(&1, :scope)
    )
  end

  defp load_list_limit(ctx, key, scopes, now_ms) do
    case Router.get(ctx, key) do
      nil ->
        :skip

      value when is_binary(value) ->
        with {:ok, owner} <- LimitRecord.decode_owner(value) do
          owner = owner |> CreditLease.reclaim_expired(now_ms) |> View.public()
          if matches_scope?(owner, scopes), do: {:ok, owner}, else: :skip
        end

      {:error, _reason} = error ->
        error

      _other ->
        {:error, "ERR flow limit record is corrupt"}
    end
  end

  defp optional_now_ms(opts) do
    case Keyword.get(opts, :now_ms, CommandTime.now_ms()) do
      value when is_integer(value) and value >= 0 and value <= @max_exact_version ->
        {:ok, value}

      _other ->
        {:error, "ERR flow limit now_ms must be a non-negative integer"}
    end
  end

  defp required_positive_integer(opts, key) do
    case Keyword.get(opts, key) do
      value when is_integer(value) and value > 0 and value <= @max_exact_version ->
        {:ok, value}

      _other ->
        {:error, "ERR flow limit #{key} must be a positive integer"}
    end
  end

  defp required_mutation_amount(opts, key) do
    case Keyword.get(opts, key) do
      value when is_integer(value) and value > 0 and value <= @max_mutation_amount ->
        {:ok, value}

      _other ->
        {:error,
         "ERR flow limit #{key} must be a positive integer at most #{@max_mutation_amount}"}
    end
  end

  defp required_shard_id(%{shard_count: shard_count}, opts)
       when is_integer(shard_count) and shard_count > 0 do
    case Keyword.get(opts, :shard_id) do
      value when is_integer(value) and value >= 0 and value < shard_count ->
        {:ok, value}

      _other ->
        {:error, "ERR flow limit shard_id must be between 0 and #{shard_count - 1}"}
    end
  end

  defp optional_positive_integer(opts, key) do
    case Keyword.get(opts, key) do
      nil ->
        {:ok, nil}

      value when is_integer(value) and value > 0 and value <= @max_exact_version ->
        {:ok, value}

      _other ->
        {:error, "ERR flow limit #{key} must be a positive integer"}
    end
  end

  defp spend_reservation_ids(opts, amount, lease_epoch) do
    case Keyword.fetch(opts, :reservation_ids) do
      :error ->
        {:ok, new_reservation_ids(amount, lease_epoch)}

      {:ok, _reservation_ids} ->
        {:error, "ERR flow limit reservation_ids cannot be supplied for spend"}
    end
  end

  defp release_reservation_ids(opts) do
    case Keyword.fetch(opts, :reservation_ids) do
      :error -> {:error, "ERR flow limit reservation_ids must contain one unique id per credit"}
      {:ok, reservation_ids} -> validate_release_reservation_ids(reservation_ids)
    end
  end

  defp validate_release_reservation_ids(reservation_ids)
       when is_list(reservation_ids) and reservation_ids != [] and
              length(reservation_ids) <= @max_mutation_amount do
    if length(Enum.uniq(reservation_ids)) == length(reservation_ids) and
         Enum.all?(reservation_ids, &LimitRecord.valid_reservation_id?/1) do
      {:ok, reservation_ids}
    else
      {:error, "ERR flow limit reservation_ids must contain one unique id per credit"}
    end
  end

  defp validate_release_reservation_ids(_reservation_ids),
    do: {:error, "ERR flow limit reservation_ids must contain one unique id per credit"}

  defp validate_preallocated_ids(reservation_ids, amount)
       when is_list(reservation_ids) and length(reservation_ids) == amount and
              amount <= @max_mutation_amount do
    if length(Enum.uniq(reservation_ids)) == amount and
         Enum.all?(reservation_ids, &LimitRecord.valid_reservation_id?/1) do
      {:ok, reservation_ids}
    else
      {:error, "ERR flow limit reserved spend ids must contain one unique id per credit"}
    end
  end

  defp validate_preallocated_ids(_reservation_ids, _amount),
    do: {:error, "ERR flow limit reserved spend ids must contain one unique id per credit"}

  defp release_amount(opts, reservation_ids) do
    case Keyword.fetch(opts, :amount) do
      :error ->
        {:ok, length(reservation_ids)}

      {:ok, amount}
      when is_integer(amount) and amount > 0 and
             amount == length(reservation_ids) ->
        {:ok, amount}

      _invalid ->
        {:error, "ERR flow limit reservation_ids must contain one unique id per credit"}
    end
  end

  defp limit_configuration(opts) do
    with {:ok, limit} <- optional_non_negative_integer(opts, :limit),
         {:ok, policy_version} <- optional_policy_version(opts),
         {:ok, config_version} <- optional_config_version(opts, policy_version) do
      if is_nil(limit) and (not is_nil(config_version) or not is_nil(policy_version)) do
        {:error, "ERR flow limit limit is required with configuration version"}
      else
        {:ok,
         %{
           limit: limit,
           config_version: config_version,
           policy_version: CreditLease.policy_version_fingerprint(policy_version)
         }}
      end
    end
  end

  defp optional_non_negative_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      :error ->
        {:ok, nil}

      {:ok, value}
      when is_integer(value) and value >= 0 and value <= @max_exact_version ->
        {:ok, value}

      {:ok, _invalid} ->
        {:error, "ERR flow limit #{key} must be a non-negative integer"}
    end
  end

  defp optional_policy_version(opts) do
    case Keyword.fetch(opts, :policy_version) do
      :error ->
        {:ok, nil}

      {:ok, value}
      when is_integer(value) and value >= 0 and value <= @max_exact_version ->
        {:ok, value}

      {:ok, value}
      when is_binary(value) and value != "" and byte_size(value) <= @max_policy_version_bytes ->
        {:ok, value}

      {:ok, _invalid} ->
        {:error, "ERR flow limit policy_version is invalid"}
    end
  end

  defp optional_config_version(opts, policy_version) do
    case Keyword.fetch(opts, :config_version) do
      :error when is_integer(policy_version) ->
        {:ok, policy_version}

      :error when is_binary(policy_version) ->
        {:error, "ERR flow limit config_version is required with binary policy_version"}

      :error ->
        {:ok, nil}

      {:ok, value}
      when is_integer(value) and value >= 0 and value <= @max_exact_version ->
        {:ok, value}

      {:ok, _invalid} ->
        {:error, "ERR flow limit config_version must be a non-negative integer"}
    end
  end

  defp new_reservation_ids(amount, lease_epoch) do
    batch_id =
      16
      |> :crypto.strong_rand_bytes()
      |> Base.url_encode64(padding: false)

    Enum.map(
      1..amount,
      &("flr1:" <>
          Integer.to_string(lease_epoch) <>
          ":" <> batch_id <> ":" <> Integer.to_string(&1))
    )
  end

  defp reservation_ids_epoch([first | remaining]) do
    with {:ok, epoch} <- reservation_id_epoch(first),
         true <- Enum.all?(remaining, &(reservation_id_epoch(&1) == {:ok, epoch})) do
      {:ok, epoch}
    else
      _invalid -> {:error, "ERR flow limit reserved spend ids have an invalid lease generation"}
    end
  end

  defp reservation_ids_epoch(_ids),
    do: {:error, "ERR flow limit reserved spend ids have an invalid lease generation"}

  defp reservation_id_epoch("flr1:" <> encoded) do
    case :binary.split(encoded, ":") do
      [epoch, _rest] ->
        case Integer.parse(epoch) do
          {epoch, ""} when epoch >= 0 and epoch <= @max_exact_version -> {:ok, epoch}
          _invalid -> :error
        end

      _invalid ->
        :error
    end
  end

  defp reservation_id_epoch(_id), do: :error

  defp optional_list_limit(opts) do
    case Keyword.get(opts, :limit, 100) do
      value when is_integer(value) and value > 0 -> {:ok, min(value, 1_000)}
      _other -> {:error, "ERR flow limit limit must be a positive integer"}
    end
  end

  defp optional_scope_filters(opts) do
    case {Keyword.get(opts, :scope), Keyword.get(opts, :partition_key)} do
      {scope, _partition_key} when is_binary(scope) and scope != "" ->
        with {:ok, _key} <- limit_key(scope), do: {:ok, [scope]}

      {nil, partition_key} when is_binary(partition_key) and partition_key != "" ->
        scopes = [partition_key, "partition:" <> partition_key]

        with true <- Enum.all?(scopes, &match?({:ok, _key}, limit_key(&1))) do
          {:ok, scopes}
        else
          false -> {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
        end

      {nil, nil} ->
        {:ok, nil}

      _other ->
        {:error, "ERR flow limit scope must be a non-empty string"}
    end
  end

  defp matches_scope?(_record, nil), do: true
  defp matches_scope?(record, scopes), do: Map.get(record, :scope) in scopes

  defp limit_key(scope) do
    key = Keys.governance_limit_key(scope)

    if byte_size(key) <= Router.max_key_size() do
      {:ok, key}
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  defp validate_deadline(_now_ms, nil), do: :ok

  defp validate_deadline(now_ms, ttl_ms)
       when is_integer(now_ms) and is_integer(ttl_ms) and
              now_ms <= @max_exact_version - ttl_ms,
       do: :ok

  defp validate_deadline(_now_ms, _ttl_ms),
    do: {:error, "ERR flow limit lease deadline exceeds the supported integer range"}

  defp require_limit_for_new_owner(false, %{limit: nil}) do
    {:error, "ERR flow limit limit must be a non-negative integer"}
  end

  defp require_limit_for_new_owner(_owner_exists?, _configuration), do: :ok

  defp register_existing_limit(ctx, key, true), do: ensure_limit_registered(ctx, key)
  defp register_existing_limit(_ctx, _key, false), do: :ok

  defp ensure_existing_limit_registered(ctx, key) do
    register_existing_limit(ctx, key, not is_nil(Router.get(ctx, key)))
  end

  defp register_committed_limit(ctx, key, result, opts) do
    case Router.get(ctx, key) do
      value when is_binary(value) ->
        case ensure_limit_registered(ctx, key) do
          :ok ->
            run_after_catalog_registration_hook(opts, key)
            result

          {:error, _reason} = error ->
            if match?({:ok, _reply}, result), do: error, else: result
        end

      _missing_or_invalid ->
        result
    end
  end

  defp run_after_catalog_registration_hook(opts, key) do
    case Keyword.get(opts, :after_catalog_registration_fun) do
      fun when is_function(fun, 1) -> fun.(key)
      _missing -> :ok
    end
  end

  defp ensure_limit_registered(ctx, key) do
    case Catalog.member?(ctx, Keys.governance_catalog_key(:limit), key) do
      {:ok, true} -> :ok
      {:ok, false} -> Catalog.register(ctx, :limit, key)
      {:error, _reason} = error -> error
    end
  end

  defp current_lease_epoch(ctx, key, shard_id) do
    case Router.get(ctx, key) do
      nil ->
        {:error, "ERR flow limit not found"}

      value when is_binary(value) ->
        with {:ok, owner} <- LimitRecord.decode_owner(value) do
          case Map.fetch(owner.leases, shard_id) do
            {:ok, lease} -> {:ok, lease.epoch}
            :error -> {:ok, 0}
          end
        end

      _invalid ->
        {:error, "ERR flow limit record is corrupt"}
    end
  end

  defp limit_metadata(scope, opts) do
    if Keyword.keyword?(opts) do
      %{
        scope: scope,
        shard_id: Keyword.get(opts, :shard_id),
        amount: Keyword.get(opts, :amount)
      }
    else
      %{scope: scope, shard_id: nil, amount: nil}
    end
  end
end
