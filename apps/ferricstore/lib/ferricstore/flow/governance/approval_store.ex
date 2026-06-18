defmodule Ferricstore.Flow.Governance.ApprovalStore do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Governance.AtomicRecord
  alias Ferricstore.Flow.Governance.Approval
  alias Ferricstore.Flow.Governance.Decision
  alias Ferricstore.Flow.Governance.Telemetry
  alias Ferricstore.Flow.Governance.View
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  def request(ctx, id, opts \\ [])

  def request(ctx, id, opts) when is_binary(id) and id != "" and is_list(opts) do
    result =
      with {:ok, flow_id} <- required_binary(opts, :flow_id),
           {:ok, scope} <- required_binary(opts, :scope),
           {:ok, now_ms} <- optional_now_ms(opts),
           {:ok, assignees} <- optional_string_list(opts, :assignees),
           {:ok, policy_hash} <- optional_binary(opts, :policy_hash),
           {:ok, policy_version} <- optional_policy_version(opts),
           {:ok, expires_at_ms} <- optional_expires_at_ms(opts, now_ms),
           approval =
             Approval.request(id,
               flow_id: flow_id,
               scope: scope,
               reason: Keyword.get(opts, :reason),
               requested_by: Keyword.get(opts, :requested_by),
               assignees: assignees,
               policy_hash: policy_hash,
               policy_version: policy_version,
               expires_at_ms: expires_at_ms,
               now_ms: now_ms
             ),
           key = Keys.governance_approval_key(id),
           set_result <-
             Router.set(ctx, key, encode(approval), %{
               expire_at_ms: 0,
               nx: true,
               xx: false,
               get: false,
               keepttl: false
             }) do
        case set_result do
          :ok ->
            {:ok, View.public(approval)}

          nil ->
            {:error, Decision.conflict(%{approval_id: id, message: "Approval already exists"})}

          {:error, _reason} = error ->
            error
        end
      end

    Telemetry.emit(:approval_request, result, %{
      approval_id: id,
      flow_id: Keyword.get(opts, :flow_id),
      scope: Keyword.get(opts, :scope)
    })
  end

  def request(_ctx, _id, _opts),
    do: {:error, "ERR flow approval opts must be a keyword list"}

  def approve(ctx, id, opts \\ []), do: decide(ctx, id, :approve, opts)
  def reject(ctx, id, opts \\ []), do: decide(ctx, id, :reject, opts)

  def get(ctx, id, opts \\ [])

  def get(ctx, id, opts) when is_binary(id) and id != "" and is_list(opts) do
    case Router.get(ctx, Keys.governance_approval_key(id)) do
      nil ->
        {:ok, nil}

      value when is_binary(value) ->
        with {:ok, approval} <- decode(value), do: {:ok, View.public(approval)}

      _other ->
        {:error, "ERR flow approval record is corrupt"}
    end
  end

  def get(_ctx, _id, _opts),
    do: {:error, "ERR flow approval opts must be a keyword list"}

  def list(ctx, opts \\ [])

  def list(ctx, opts) when is_list(opts) do
    with {:ok, limit} <- optional_limit(opts),
         {:ok, status} <- optional_status(opts),
         {:ok, scopes} <- optional_scope_filters(opts),
         {:ok, flow_id} <- optional_filter_binary(opts, :flow_id) do
      approvals =
        ctx
        |> Router.keys()
        |> Enum.filter(&Keys.governance_approval_key?/1)
        |> Enum.reduce([], fn key, acc ->
          case Router.get(ctx, key) do
            value when is_binary(value) ->
              case decode(value) do
                {:ok, approval} -> [View.public(approval) | acc]
                {:error, _reason} -> acc
              end

            _other ->
              acc
          end
        end)
        |> Enum.filter(&matches_status?(&1, status))
        |> Enum.filter(&matches_scope?(&1, scopes))
        |> Enum.filter(&matches_binary?(&1, :flow_id, flow_id))
        |> Enum.sort_by(&Map.get(&1, :requested_at_ms, 0), :desc)
        |> Enum.take(limit)

      {:ok, approvals}
    end
  end

  def list(_ctx, _opts), do: {:error, "ERR flow approval opts must be a keyword list"}

  defp decide(ctx, id, action, opts) when is_binary(id) and id != "" and is_list(opts) do
    result =
      with {:ok, now_ms} <- optional_now_ms(opts),
           {:ok, approver} <- required_binary(opts, :approver),
           update_opts = [
             approver: approver,
             reason: Keyword.get(opts, :reason),
             now_ms: now_ms
           ] do
        AtomicRecord.mutate(
          ctx,
          Keys.governance_approval_key(id),
          &decode/1,
          &encode/1,
          fn -> {:error, "ERR flow approval not found"} end,
          fn approval ->
            case apply_decision(approval, action, update_opts) do
              {:ok, updated} -> {:ok, updated, View.public(updated)}
              {:error, _reason} = error -> error
            end
          end
        )
      end

    Telemetry.emit(approval_action_name(action), result, approval_metadata(id, result))
  end

  defp decide(_ctx, _id, _action, _opts),
    do: {:error, "ERR flow approval opts must be a keyword list"}

  defp apply_decision(approval, :approve, opts), do: Approval.approve(approval, opts)
  defp apply_decision(approval, :reject, opts), do: Approval.reject(approval, opts)

  defp encode(approval), do: :erlang.term_to_binary({:flow_governance_approval_v1, approval})

  defp decode(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {:flow_governance_approval_v1, %Approval{} = approval} -> {:ok, approval}
      _other -> {:error, "ERR flow approval record is corrupt"}
    end
  rescue
    _ -> {:error, "ERR flow approval record is corrupt"}
  end

  defp required_binary(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, "ERR flow approval #{key} must be a non-empty string"}
    end
  end

  defp optional_now_ms(opts) do
    case Keyword.get(opts, :now_ms, CommandTime.now_ms()) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _other -> {:error, "ERR flow approval now_ms must be a non-negative integer"}
    end
  end

  defp optional_expires_at_ms(opts, now_ms) do
    case Keyword.fetch(opts, :timeout_ms) do
      {:ok, value} when is_integer(value) and value > 0 -> {:ok, now_ms + value}
      {:ok, _other} -> {:error, "ERR flow approval timeout_ms must be a positive integer"}
      :error -> optional_non_negative_integer(opts, :expires_at_ms)
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
      nil -> {:ok, nil}
      value when value in [:pending, :approved, :rejected] -> {:ok, value}
      "pending" -> {:ok, :pending}
      "approved" -> {:ok, :approved}
      "rejected" -> {:ok, :rejected}
      _other -> {:error, "ERR flow approval status must be pending, approved, or rejected"}
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
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, "ERR flow approval #{key} must be a non-empty string"}
    end
  end

  defp optional_policy_version(opts) do
    case Keyword.get(opts, :policy_version) do
      nil ->
        {:ok, nil}

      value when is_binary(value) and value != "" ->
        {:ok, value}

      value when is_integer(value) and value >= 0 ->
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
        |> Enum.reduce_while({:ok, []}, fn
          value, {:ok, acc} when is_binary(value) and value != "" ->
            {:cont, {:ok, [value | acc]}}

          _value, _acc ->
            {:halt, {:error, "ERR flow approval #{key} must be a list of non-empty strings"}}
        end)
        |> case do
          {:ok, values} -> {:ok, values |> Enum.reverse() |> Enum.uniq()}
          {:error, _reason} = error -> error
        end

      _other ->
        {:error, "ERR flow approval #{key} must be a list of non-empty strings"}
    end
  end

  defp optional_non_negative_integer(opts, key) do
    case Keyword.get(opts, key) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _other -> {:error, "ERR flow approval #{key} must be a non-negative integer"}
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
end
