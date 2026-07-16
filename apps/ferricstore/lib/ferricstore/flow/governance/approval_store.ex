defmodule Ferricstore.Flow.Governance.ApprovalStore do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Governance.AtomicRecord
  alias Ferricstore.Flow.Governance.Approval
  alias Ferricstore.Flow.Governance.ApprovalCatalogRepair
  alias Ferricstore.Flow.Governance.Catalog
  alias Ferricstore.Flow.Governance.Decision
  alias Ferricstore.Flow.Governance.Telemetry
  alias Ferricstore.Flow.Governance.View
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router
  alias Ferricstore.TermCodec

  @max_assignees 1_000
  @max_exact_integer 9_007_199_254_740_991
  @max_dimension_bytes 65_535
  @max_field_bytes 262_144
  @max_record_bytes 900_000

  def request(ctx, id, opts \\ [])

  def request(ctx, id, opts) when is_binary(id) and id != "" and is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           {:ok, key} <- approval_key(id),
           {:ok, flow_id} <- required_binary(opts, :flow_id),
           {:ok, scope} <- required_binary(opts, :scope),
           :ok <- validate_catalog_dimensions(scope, flow_id),
           {:ok, now_ms} <- optional_now_ms(opts),
           {:ok, assignees} <- optional_string_list(opts, :assignees),
           {:ok, reason} <- optional_binary(opts, :reason),
           {:ok, requested_by} <- optional_binary(opts, :requested_by),
           {:ok, policy_hash} <- optional_binary(opts, :policy_hash),
           {:ok, policy_version} <- optional_policy_version(opts),
           {:ok, expires_at_ms} <- optional_expires_at_ms(opts, now_ms),
           approval =
             Approval.request(id,
               flow_id: flow_id,
               scope: scope,
               reason: reason,
               requested_by: requested_by,
               assignees: assignees,
               policy_hash: policy_hash,
               policy_version: policy_version,
               expires_at_ms: expires_at_ms,
               now_ms: now_ms
             ),
           {:ok, encoded} <- encode_for_write(approval),
           :ok <- Catalog.register(ctx, :approval, key),
           set_result <-
             Router.set(ctx, key, encoded, %{
               expire_at_ms: 0,
               nx: true,
               xx: false,
               get: false,
               keepttl: false
             }) do
        case set_result do
          :ok ->
            :ok = register_committed_approval(ctx, key, scope, flow_id)
            {:ok, View.public(approval)}

          nil ->
            {:error, Decision.conflict(%{approval_id: id, message: "Approval already exists"})}

          {:error, _reason} = error ->
            error
        end
      else
        false -> {:error, "ERR flow approval opts must be a keyword list"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(:approval_request, result, %{
      approval_id: id,
      flow_id: safe_option(opts, :flow_id),
      scope: safe_option(opts, :scope)
    })
  end

  def request(_ctx, _id, _opts),
    do: {:error, "ERR flow approval opts must be a keyword list"}

  defp register_committed_approval(ctx, key, scope, flow_id) do
    [
      Keys.governance_approval_scope_catalog_key(scope),
      Keys.governance_approval_flow_catalog_key(flow_id)
    ]
    |> Enum.each(fn catalog_key ->
      case Catalog.register_key(ctx, catalog_key, key) do
        :ok -> :ok
        {:error, _reason} -> ApprovalCatalogRepair.mark_dirty(ctx)
      end
    end)

    :ok
  end

  def approve(ctx, id, opts \\ []), do: decide(ctx, id, :approve, opts)
  def reject(ctx, id, opts \\ []), do: decide(ctx, id, :reject, opts)

  def get(ctx, id, opts \\ [])

  def get(ctx, id, opts) when is_binary(id) and id != "" and is_list(opts) do
    with true <- Keyword.keyword?(opts),
         {:ok, key} <- approval_key(id) do
      case Router.get(ctx, key) do
        nil ->
          {:ok, nil}

        value when is_binary(value) ->
          with {:ok, approval} <- decode(value), do: {:ok, View.public(approval)}

        _other ->
          {:error, "ERR flow approval record is corrupt"}
      end
    else
      false -> {:error, "ERR flow approval opts must be a keyword list"}
      {:error, _reason} = error -> error
    end
  end

  def get(_ctx, _id, _opts),
    do: {:error, "ERR flow approval opts must be a keyword list"}

  def list(ctx, opts \\ [])

  def list(ctx, opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         {:ok, limit} <- optional_limit(opts),
         {:ok, status} <- optional_status(opts),
         {:ok, scopes} <- optional_scope_filters(opts),
         {:ok, flow_id} <- optional_filter_binary(opts, :flow_id),
         {:ok, approvals} <-
           collect_list_approvals(ctx, status, scopes, flow_id, limit) do
      {:ok, approvals}
    else
      false -> {:error, "ERR flow approval opts must be a keyword list"}
      {:error, _reason} = error -> error
    end
  end

  def list(_ctx, _opts), do: {:error, "ERR flow approval opts must be a keyword list"}

  defp collect_list_approvals(ctx, status, scopes, flow_id, limit)
       when is_binary(flow_id) do
    catalog_key = Keys.governance_approval_flow_catalog_key(flow_id)

    with :ok <- validate_dimension_size(flow_id),
         :ok <- validate_key_size(catalog_key) do
      collect_approval_catalog(
        ctx,
        catalog_key,
        {:flow, flow_id},
        status,
        scopes,
        flow_id,
        limit
      )
    end
  end

  defp collect_list_approvals(ctx, status, scopes, nil, limit) when is_list(scopes) do
    scopes
    |> Enum.reduce_while({:ok, []}, fn scope, {:ok, approvals} ->
      catalog_key = Keys.governance_approval_scope_catalog_key(scope)

      result =
        with :ok <- validate_dimension_size(scope),
             :ok <- validate_key_size(catalog_key) do
          collect_approval_catalog(
            ctx,
            catalog_key,
            {:scope, scope},
            status,
            scopes,
            nil,
            limit
          )
        end

      case result do
        {:ok, page} -> {:cont, {:ok, merge_approval_results(approvals, page, limit)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp collect_list_approvals(ctx, status, nil, nil, limit) do
    Catalog.collect(
      ctx,
      :approval,
      limit,
      &load_list_approval(ctx, &1, status, nil, nil),
      &Map.get(&1, :requested_at_ms, 0),
      :desc
    )
  end

  defp collect_approval_catalog(
         ctx,
         catalog_key,
         exact_filter,
         status,
         scopes,
         flow_id,
         limit
       ) do
    with :ok <-
           ApprovalCatalogRepair.step(
             ctx,
             catalog_key,
             &approval_catalog_member?(ctx, &1, exact_filter),
             &approval_catalog_targets(ctx, &1)
           ) do
      Catalog.collect_key(
        ctx,
        catalog_key,
        limit,
        &load_list_approval(ctx, &1, status, scopes, flow_id),
        &Map.get(&1, :requested_at_ms, 0),
        :desc
      )
    end
  end

  defp merge_approval_results(left, right, limit) do
    (left ++ right)
    |> Enum.uniq_by(&Map.get(&1, :id))
    |> Enum.sort_by(&Map.get(&1, :requested_at_ms, 0), :desc)
    |> Enum.take(limit)
  end

  defp load_list_approval(ctx, key, status, scopes, flow_id) do
    case Router.get(ctx, key) do
      nil ->
        :skip

      value when is_binary(value) ->
        with {:ok, approval} <- decode(value) do
          approval = View.public(approval)

          if matches_status?(approval, status) and matches_scope?(approval, scopes) and
               matches_binary?(approval, :flow_id, flow_id),
             do: {:ok, approval},
             else: :skip
        end

      {:error, _reason} = error ->
        error

      _other ->
        {:error, "ERR flow approval record is corrupt"}
    end
  end

  defp approval_catalog_member?(ctx, key, {dimension, expected}) do
    case Router.get(ctx, key) do
      nil ->
        false

      value when is_binary(value) ->
        with {:ok, approval} <- decode(value) do
          Map.get(approval, dimension_to_field(dimension)) == expected
        end

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, "ERR flow approval record is corrupt"}
    end
  end

  defp approval_catalog_targets(ctx, key) do
    case Router.get(ctx, key) do
      nil ->
        :missing

      value when is_binary(value) ->
        case decode(value) do
          {:ok, approval} ->
            approval_catalog_keys(approval.scope, approval.flow_id)

          {:error, _reason} ->
            :skip
        end

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, "ERR flow approval record is corrupt"}
    end
  end

  defp dimension_to_field(:scope), do: :scope
  defp dimension_to_field(:flow), do: :flow_id

  defp decide(ctx, id, action, opts) when is_binary(id) and id != "" and is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           {:ok, key} <- approval_key(id),
           {:ok, now_ms} <- optional_now_ms(opts),
           {:ok, approver} <- required_binary(opts, :approver),
           {:ok, reason} <- optional_binary(opts, :reason),
           update_opts = [
             approver: approver,
             reason: reason,
             now_ms: now_ms
           ] do
        AtomicRecord.mutate(
          ctx,
          key,
          &decode/1,
          &encode/1,
          fn -> {:error, "ERR flow approval not found"} end,
          fn approval ->
            case apply_decision(approval, action, update_opts) do
              {:ok, updated} -> {:ok, updated, View.public(updated)}
              {:error, reason, updated} -> {:error, reason, updated}
              {:error, _reason} = error -> error
            end
          end
        )
      else
        false -> {:error, "ERR flow approval opts must be a keyword list"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(approval_action_name(action), result, approval_metadata(id, result))
  end

  defp decide(_ctx, _id, _action, _opts),
    do: {:error, "ERR flow approval opts must be a keyword list"}

  defp apply_decision(approval, :approve, opts), do: Approval.approve(approval, opts)
  defp apply_decision(approval, :reject, opts), do: Approval.reject(approval, opts)

  defp safe_option(opts, key) do
    if Keyword.keyword?(opts), do: Keyword.get(opts, key), else: nil
  end

  defp encode(approval), do: TermCodec.encode({:flow_governance_approval_v1, approval})

  defp encode_for_write(approval) do
    encoded = encode(approval)

    if byte_size(encoded) <= @max_record_bytes do
      {:ok, encoded}
    else
      {:error, "ERR flow approval record exceeds #{@max_record_bytes}-byte durable limit"}
    end
  end

  defp decode(value) do
    case TermCodec.decode(value) do
      {:ok, {:flow_governance_approval_v1, %Approval{} = approval}} ->
        if Approval.valid?(approval),
          do: {:ok, approval},
          else: {:error, "ERR flow approval record is corrupt"}

      _other ->
        {:error, "ERR flow approval record is corrupt"}
    end
  end

  defp required_binary(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" and byte_size(value) <= @max_field_bytes ->
        {:ok, value}

      value when is_binary(value) and value != "" ->
        {:error, "ERR flow approval #{key} must be at most #{@max_field_bytes} bytes"}

      _other ->
        {:error, "ERR flow approval #{key} must be a non-empty string"}
    end
  end

  defp optional_now_ms(opts) do
    case Keyword.get(opts, :now_ms, CommandTime.now_ms()) do
      value when is_integer(value) and value >= 0 and value <= @max_exact_integer -> {:ok, value}
      _other -> {:error, "ERR flow approval now_ms must be a non-negative integer"}
    end
  end

  defp optional_expires_at_ms(opts, now_ms) do
    case Keyword.fetch(opts, :timeout_ms) do
      {:ok, value}
      when is_integer(value) and value > 0 and value <= @max_exact_integer - now_ms ->
        {:ok, now_ms + value}

      {:ok, _other} ->
        {:error, "ERR flow approval timeout_ms must be a positive integer"}

      :error ->
        optional_absolute_expiry(opts, now_ms)
    end
  end

  defp optional_absolute_expiry(opts, now_ms) do
    case Keyword.get(opts, :expires_at_ms) do
      nil ->
        {:ok, nil}

      value when is_integer(value) and value > now_ms and value <= @max_exact_integer ->
        {:ok, value}

      value when is_integer(value) and value >= 0 ->
        {:error, "ERR flow approval expires_at_ms must be greater than now_ms"}

      _other ->
        {:error, "ERR flow approval expires_at_ms must be a non-negative integer"}
    end
  end

  defp optional_limit(opts) do
    case Keyword.get(opts, :limit, 100) do
      value when is_integer(value) and value > 0 -> {:ok, min(value, 1_000)}
      _other -> {:error, "ERR flow approval limit must be a positive integer"}
    end
  end

  defp optional_status(opts) do
    case Keyword.get(opts, :status) do
      nil ->
        {:ok, nil}

      value when value in [:pending, :approved, :rejected, :expired] ->
        {:ok, value}

      "pending" ->
        {:ok, :pending}

      "approved" ->
        {:ok, :approved}

      "rejected" ->
        {:ok, :rejected}

      "expired" ->
        {:ok, :expired}

      _other ->
        {:error, "ERR flow approval status must be pending, approved, rejected, or expired"}
    end
  end

  defp optional_filter_binary(opts, key) do
    case Keyword.get(opts, key) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, "ERR flow approval #{key} must be a non-empty string"}
    end
  end

  defp optional_binary(opts, key) do
    case Keyword.get(opts, key) do
      nil ->
        {:ok, nil}

      value when is_binary(value) and value != "" and byte_size(value) <= @max_field_bytes ->
        {:ok, value}

      value when is_binary(value) and value != "" ->
        {:error, "ERR flow approval #{key} must be at most #{@max_field_bytes} bytes"}

      _other ->
        {:error, "ERR flow approval #{key} must be a non-empty string"}
    end
  end

  defp optional_policy_version(opts) do
    case Keyword.get(opts, :policy_version) do
      nil ->
        {:ok, nil}

      value when is_binary(value) and value != "" and byte_size(value) <= @max_field_bytes ->
        {:ok, value}

      value when is_binary(value) and value != "" ->
        {:error, "ERR flow approval policy_version must be at most #{@max_field_bytes} bytes"}

      value when is_integer(value) and value >= 0 and value <= @max_exact_integer ->
        {:ok, value}

      _other ->
        {:error,
         "ERR flow approval policy_version must be a non-empty string or non-negative integer"}
    end
  end

  defp optional_string_list(opts, key) do
    case Keyword.get(opts, key, []) do
      values when is_list(values) ->
        values
        |> Enum.reduce_while({:ok, [], MapSet.new(), 0}, fn
          value, {:ok, acc, seen, count}
          when is_binary(value) and value != "" and byte_size(value) <= @max_dimension_bytes and
                 count < @max_assignees ->
            if MapSet.member?(seen, value) do
              {:cont, {:ok, acc, seen, count + 1}}
            else
              {:cont, {:ok, [value | acc], MapSet.put(seen, value), count + 1}}
            end

          _value, _acc ->
            {:halt, {:error, "ERR flow approval #{key} must be a list of non-empty strings"}}
        end)
        |> case do
          {:ok, values, _seen, _count} -> {:ok, Enum.reverse(values)}
          {:error, _reason} = error -> error
        end

      _other ->
        {:error, "ERR flow approval #{key} must be a list of non-empty strings"}
    end
  end

  defp optional_scope_filters(opts) do
    case {Keyword.get(opts, :scope), Keyword.get(opts, :partition_key)} do
      {scope, _partition_key} when is_binary(scope) and scope != "" ->
        {:ok, [scope]}

      {nil, partition_key} when is_binary(partition_key) and partition_key != "" ->
        {:ok, [partition_key, "partition:" <> partition_key]}

      {nil, nil} ->
        {:ok, nil}

      _other ->
        {:error, "ERR flow approval scope must be a non-empty string"}
    end
  end

  defp matches_status?(_approval, nil), do: true
  defp matches_status?(approval, status), do: Map.get(approval, :status) == status
  defp matches_scope?(_approval, nil), do: true
  defp matches_scope?(approval, scopes), do: Map.get(approval, :scope) in scopes
  defp matches_binary?(_approval, _key, nil), do: true
  defp matches_binary?(approval, key, value), do: Map.get(approval, key) == value

  defp approval_action_name(:approve), do: :approval_approve
  defp approval_action_name(:reject), do: :approval_reject

  defp approval_metadata(id, {:ok, approval}) when is_map(approval) do
    metadata = %{
      approval_id: id,
      flow_id: Map.get(approval, :flow_id),
      scope: Map.get(approval, :scope)
    }

    case {Map.get(approval, :requested_at_ms), Map.get(approval, :decided_at_ms)} do
      {requested_at_ms, decided_at_ms}
      when is_integer(requested_at_ms) and is_integer(decided_at_ms) ->
        Map.put(metadata, :wait_ms, max(decided_at_ms - requested_at_ms, 0))

      _other ->
        metadata
    end
  end

  defp approval_metadata(id, _result), do: %{approval_id: id}

  defp approval_key(id) do
    key = Keys.governance_approval_key(id)

    with :ok <- validate_key_size(key), do: {:ok, key}
  end

  defp validate_catalog_dimensions(scope, flow_id) do
    with :ok <- validate_dimension_size(scope),
         :ok <- validate_dimension_size(flow_id),
         {:ok, _catalog_keys} <- approval_catalog_keys(scope, flow_id),
         do: :ok
  end

  defp approval_catalog_keys(scope, flow_id) do
    keys = [
      Keys.governance_approval_scope_catalog_key(scope),
      Keys.governance_approval_flow_catalog_key(flow_id)
    ]

    with :ok <- validate_dimension_size(scope),
         :ok <- validate_dimension_size(flow_id),
         true <- Enum.all?(keys, &(validate_key_size(&1) == :ok)) do
      {:ok, keys}
    else
      false -> {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
      {:error, _reason} = error -> error
    end
  end

  defp validate_dimension_size(value) do
    if byte_size(value) <= Router.max_key_size(),
      do: :ok,
      else: {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
  end

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size(),
      do: :ok,
      else: {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
  end
end
