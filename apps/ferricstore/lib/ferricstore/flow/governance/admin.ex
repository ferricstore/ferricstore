defmodule Ferricstore.Flow.Governance.Admin do
  @moduledoc false

  alias Ferricstore.Flow.Governance.ApprovalStore
  alias Ferricstore.Flow.Governance.BudgetStore
  alias Ferricstore.Flow.Governance.CircuitStore
  alias Ferricstore.Flow.Governance.LimitStore

  def overview(ctx, opts \\ [])

  def overview(ctx, opts) when is_list(opts) do
    with {:ok, approvals} <- ApprovalStore.list(ctx, opts),
         {:ok, budgets} <- BudgetStore.list(ctx, opts),
         {:ok, limits} <- LimitStore.list(ctx, opts),
         {:ok, circuits} <- CircuitStore.list(ctx, opts) do
      {:ok,
       %{
         approvals: approvals,
         budgets: budgets,
         limits: limits,
         circuits: circuits,
         counts: %{
           approvals: length(approvals),
           pending_approvals: Enum.count(approvals, &(Map.get(&1, :status) == :pending)),
           budgets: length(budgets),
           limits: length(limits),
           circuits: length(circuits),
           open_circuits: Enum.count(circuits, &(Map.get(&1, :status) == :open)),
           half_open_circuits: Enum.count(circuits, &(Map.get(&1, :status) == :half_open))
         }
       }}
    end
  end

  def overview(_ctx, _opts), do: {:error, "ERR flow governance opts must be a keyword list"}
end
