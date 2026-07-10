defmodule Ferricstore.Flow.Governance.BudgetStore do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Governance.AtomicRecord
  alias Ferricstore.Flow.Governance.Budget
  alias Ferricstore.Flow.Governance.Catalog
  alias Ferricstore.Flow.Governance.Telemetry
  alias Ferricstore.Flow.Governance.View
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  def reserve(ctx, scope, amount, opts \\ [])

  def reserve(ctx, scope, amount, opts)
      when is_binary(scope) and scope != "" and is_integer(amount) and amount > 0 and
             is_list(opts) do
    result =
      with {:ok, now_ms} <- optional_now_ms(opts),
           {:ok, requested_reservation_id} <- optional_reservation_id(opts) do
        AtomicRecord.mutate(
          ctx,
          Keys.governance_budget_key(scope),
          &decode/1,
          &encode/1,
          fn -> new_budget(scope, opts, now_ms) end,
          fn budget ->
            reservation_id = requested_reservation_id || new_reservation_id()

            case Budget.reserve(budget, amount, now_ms: now_ms, reservation_id: reservation_id) do
              {:ok, updated, reservation} ->
                {:ok, updated, budget_reply(updated, reservation)}

              {:error, denial, updated} ->
                {:error, denial, updated}
            end
          end,
          catalog_kind: :budget
        )
      end

    Telemetry.emit(:budget_reserve, result, %{scope: scope, amount: amount})
  end

  def reserve(_ctx, _scope, _amount, _opts),
    do: {:error, "ERR flow budget opts must be a keyword list"}

  def commit(ctx, scope, reservation_id, actual_amount, opts \\ [])

  def commit(ctx, scope, reservation_id, actual_amount, opts)
      when is_binary(scope) and scope != "" and is_binary(reservation_id) and
             reservation_id != "" and is_integer(actual_amount) and actual_amount >= 0 and
             is_list(opts) do
    result =
      with {:ok, now_ms} <- optional_now_ms(opts) do
        AtomicRecord.mutate(
          ctx,
          Keys.governance_budget_key(scope),
          &decode/1,
          &encode/1,
          fn -> {:return, {:error, "ERR flow budget not found"}} end,
          fn budget ->
            case Budget.commit(budget, reservation_id, actual_amount,
                   now_ms: now_ms,
                   usage: Keyword.get(opts, :usage)
                 ) do
              {:ok, updated, reservation} -> {:ok, updated, budget_reply(updated, reservation)}
              {:error, reason, updated} -> {:error, reason, updated}
            end
          end
        )
      end

    Telemetry.emit(:budget_commit, result, %{
      scope: scope,
      reservation_id: reservation_id,
      actual_amount: actual_amount
    })
  end

  def commit(_ctx, _scope, _reservation_id, _actual_amount, _opts),
    do: {:error, "ERR flow budget commit opts must be a keyword list"}

  def release(ctx, scope, reservation_id, opts \\ [])

  def release(ctx, scope, reservation_id, opts)
      when is_binary(scope) and scope != "" and is_binary(reservation_id) and
             reservation_id != "" and is_list(opts) do
    result =
      with {:ok, now_ms} <- optional_now_ms(opts) do
        AtomicRecord.mutate(
          ctx,
          Keys.governance_budget_key(scope),
          &decode/1,
          &encode/1,
          fn -> {:return, {:error, "ERR flow budget not found"}} end,
          fn budget ->
            case Budget.release(budget, reservation_id, now_ms: now_ms) do
              {:ok, updated, reservation} -> {:ok, updated, budget_reply(updated, reservation)}
              {:error, reason, updated} -> {:error, reason, updated}
            end
          end
        )
      end

    Telemetry.emit(:budget_release, result, %{scope: scope, reservation_id: reservation_id})
  end

  def release(_ctx, _scope, _reservation_id, _opts),
    do: {:error, "ERR flow budget release opts must be a keyword list"}

  def get(ctx, scope, opts \\ [])

  def get(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    case Router.get(ctx, Keys.governance_budget_key(scope)) do
      nil ->
        {:ok, nil}

      value when is_binary(value) ->
        with {:ok, budget} <- decode(value), do: {:ok, View.public(budget)}

      _other ->
        {:error, "ERR flow budget record is corrupt"}
    end
  end

  def get(_ctx, _scope, _opts), do: {:error, "ERR flow budget opts must be a keyword list"}

  def list(ctx, opts \\ [])

  def list(ctx, opts) when is_list(opts) do
    with {:ok, limit} <- optional_limit(opts),
         {:ok, scopes} <- optional_scope_filters(opts),
         {:ok, budgets} <-
           collect_list_budgets(ctx, scopes, limit) do
      {:ok, budgets}
    end
  end

  def list(_ctx, _opts), do: {:error, "ERR flow budget opts must be a keyword list"}

  defp collect_list_budgets(ctx, nil, limit) do
    Catalog.collect(
      ctx,
      :budget,
      limit,
      &load_list_budget(ctx, &1, nil),
      &Map.get(&1, :scope)
    )
  end

  defp collect_list_budgets(ctx, scopes, limit) when is_list(scopes) do
    keys = Enum.map(scopes, &Keys.governance_budget_key/1)

    Catalog.collect_keys(
      keys,
      limit,
      &load_list_budget(ctx, &1, scopes),
      &Map.get(&1, :scope)
    )
  end

  defp load_list_budget(ctx, key, scopes) do
    with value when is_binary(value) <- Router.get(ctx, key),
         {:ok, budget} <- decode(value),
         budget = View.public(budget),
         true <- matches_scope?(budget, scopes) do
      {:ok, budget}
    else
      _missing_corrupt_or_filtered -> :skip
    end
  end

  defp new_budget(scope, opts, now_ms) do
    with {:ok, limit} <- required_positive_integer(opts, :limit),
         {:ok, window_ms} <- required_positive_integer(opts, :window_ms) do
      {:ok, Budget.fixed_window(scope, limit, window_ms, now_ms: now_ms)}
    end
  end

  defp encode(budget), do: :erlang.term_to_binary({:flow_governance_budget_v1, budget})

  defp decode(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {:flow_governance_budget_v1, %Budget{} = budget} -> {:ok, Budget.normalize(budget)}
      _other -> {:error, "ERR flow budget record is corrupt"}
    end
  rescue
    _ -> {:error, "ERR flow budget record is corrupt"}
  end

  defp optional_now_ms(opts) do
    case Keyword.get(opts, :now_ms, CommandTime.now_ms()) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _other -> {:error, "ERR flow budget now_ms must be a non-negative integer"}
    end
  end

  defp optional_reservation_id(opts) do
    case Keyword.get(opts, :reservation_id) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, "ERR flow budget reservation_id must be a non-empty string"}
    end
  end

  defp required_positive_integer(opts, key) do
    case Keyword.get(opts, key) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _other -> {:error, "ERR flow budget #{key} must be a positive integer"}
    end
  end

  defp optional_limit(opts) do
    case Keyword.get(opts, :limit, 100) do
      value when is_integer(value) and value > 0 -> {:ok, min(value, 1_000)}
      _other -> {:error, "ERR flow budget limit must be a positive integer"}
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
        {:error, "ERR flow budget scope must be a non-empty string"}
    end
  end

  defp matches_scope?(_record, nil), do: true
  defp matches_scope?(record, scopes), do: Map.get(record, :scope) in scopes

  defp budget_reply(budget, reservation) do
    budget
    |> View.public()
    |> Map.merge(Budget.public_reservation(reservation))
  end

  defp new_reservation_id do
    16
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end
end
