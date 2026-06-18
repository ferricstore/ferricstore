defmodule Ferricstore.Flow.Governance.LimitStore do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Governance.AtomicRecord
  alias Ferricstore.Flow.Governance.CreditLease
  alias Ferricstore.Flow.Governance.Telemetry
  alias Ferricstore.Flow.Governance.View
  alias Ferricstore.Flow.Keys

  def lease(ctx, scope, opts \\ [])

  def lease(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with {:ok, shard_id} <- required_non_negative_integer(opts, :shard_id),
           {:ok, amount} <- required_positive_integer(opts, :amount),
           {:ok, ttl_ms} <- required_positive_integer(opts, :ttl_ms),
           {:ok, now_ms} <- optional_now_ms(opts) do
        AtomicRecord.mutate(
          ctx,
          Keys.governance_limit_key(scope),
          &decode/1,
          &encode/1,
          fn -> new_owner(scope, opts) end,
          fn owner ->
            case CreditLease.grant(owner, shard_id, amount, now_ms: now_ms, ttl_ms: ttl_ms) do
              {:ok, updated_owner, lease} ->
                {:ok, updated_owner,
                 %{owner: View.public(updated_owner), lease: View.public(lease)}}

              {:error, denial, updated_owner} ->
                {:error, denial, updated_owner}
            end
          end
        )
      end

    Telemetry.emit(:limit_lease, result, limit_metadata(scope, opts))
  end

  def lease(_ctx, _scope, _opts), do: {:error, "ERR flow limit opts must be a keyword list"}

  def spend(ctx, scope, opts \\ [])

  def spend(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with {:ok, shard_id} <- required_non_negative_integer(opts, :shard_id),
           {:ok, amount} <- required_positive_integer(opts, :amount),
           {:ok, now_ms} <- optional_now_ms(opts) do
        AtomicRecord.mutate(
          ctx,
          Keys.governance_limit_key(scope),
          &decode/1,
          &encode/1,
          fn -> {:error, "ERR flow limit not found"} end,
          fn owner ->
            case CreditLease.spend(owner, shard_id, amount, now_ms: now_ms) do
              {:ok, updated_owner, lease} ->
                {:ok, updated_owner,
                 %{owner: View.public(updated_owner), lease: View.public(lease)}}

              {:error, denial, updated_owner} ->
                {:error, denial, updated_owner}
            end
          end
        )
      end

    Telemetry.emit(:limit_spend, result, limit_metadata(scope, opts))
  end

  def spend(_ctx, _scope, _opts), do: {:error, "ERR flow limit opts must be a keyword list"}

  def release(ctx, scope, opts \\ [])

  def release(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with {:ok, shard_id} <- required_non_negative_integer(opts, :shard_id),
           {:ok, amount} <- required_positive_integer(opts, :amount) do
        AtomicRecord.mutate(
          ctx,
          Keys.governance_limit_key(scope),
          &decode/1,
          &encode/1,
          fn -> {:error, "ERR flow limit not found"} end,
          fn owner ->
            updated_owner = CreditLease.release(owner, shard_id, amount)
            {:ok, updated_owner, View.public(updated_owner)}
          end
        )
      end

    Telemetry.emit(:limit_release, result, limit_metadata(scope, opts))
  end

  def release(_ctx, _scope, _opts), do: {:error, "ERR flow limit opts must be a keyword list"}

  def get(ctx, scope, opts \\ [])

  def get(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with {:ok, now_ms} <- optional_now_ms(opts) do
        AtomicRecord.mutate(
          ctx,
          Keys.governance_limit_key(scope),
          &decode/1,
          &encode/1,
          fn -> {:return, {:ok, nil}} end,
          fn owner ->
            updated_owner = CreditLease.reclaim_expired(owner, now_ms)
            {:ok, updated_owner, View.public(updated_owner)}
          end
        )
      end

    Telemetry.emit(:limit_reclaim, result, %{scope: scope})
  end

  def get(_ctx, _scope, _opts), do: {:error, "ERR flow limit opts must be a keyword list"}

  def list(ctx, opts \\ [])

  def list(ctx, opts) when is_list(opts) do
    with {:ok, limit} <- optional_list_limit(opts),
         {:ok, scopes} <- optional_scope_filters(opts),
         {:ok, now_ms} <- optional_now_ms(opts) do
      limits =
        ctx
        |> Ferricstore.Store.Router.keys()
        |> Enum.filter(&Keys.governance_limit_key?/1)
        |> Enum.reduce([], fn key, acc ->
          case Ferricstore.Store.Router.get(ctx, key) do
            value when is_binary(value) ->
              case decode(value) do
                {:ok, owner} ->
                  owner
                  |> CreditLease.reclaim_expired(now_ms)
                  |> View.public()
                  |> then(&[&1 | acc])

                {:error, _reason} ->
                  acc
              end

            _other ->
              acc
          end
        end)
        |> Enum.filter(&matches_scope?(&1, scopes))
        |> Enum.sort_by(&Map.get(&1, :scope))
        |> Enum.take(limit)

      {:ok, limits}
    end
  end

  def list(_ctx, _opts), do: {:error, "ERR flow limit opts must be a keyword list"}

  defp new_owner(scope, opts) do
    with {:ok, limit} <- required_non_negative_integer(opts, :limit) do
      {:ok, CreditLease.owner(scope, limit)}
    end
  end

  defp encode(owner), do: :erlang.term_to_binary({:flow_governance_limit_v1, owner})

  defp decode(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {:flow_governance_limit_v1, %CreditLease.Owner{} = owner} -> {:ok, owner}
      _other -> {:error, "ERR flow limit record is corrupt"}
    end
  rescue
    _ -> {:error, "ERR flow limit record is corrupt"}
  end

  defp optional_now_ms(opts) do
    case Keyword.get(opts, :now_ms, CommandTime.now_ms()) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _other -> {:error, "ERR flow limit now_ms must be a non-negative integer"}
    end
  end

  defp required_positive_integer(opts, key) do
    case Keyword.get(opts, key) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _other -> {:error, "ERR flow limit #{key} must be a positive integer"}
    end
  end

  defp required_non_negative_integer(opts, key) do
    case Keyword.get(opts, key) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _other -> {:error, "ERR flow limit #{key} must be a non-negative integer"}
    end
  end

  defp optional_list_limit(opts) do
    case Keyword.get(opts, :limit, 100) do
      value when is_integer(value) and value > 0 -> {:ok, min(value, 1_000)}
      _other -> {:error, "ERR flow limit limit must be a positive integer"}
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
        {:error, "ERR flow limit scope must be a non-empty string"}
    end
  end

  defp matches_scope?(_record, nil), do: true
  defp matches_scope?(record, scopes), do: Map.get(record, :scope) in scopes

  defp limit_metadata(scope, opts) do
    %{
      scope: scope,
      shard_id: Keyword.get(opts, :shard_id),
      amount: Keyword.get(opts, :amount)
    }
  end
end
