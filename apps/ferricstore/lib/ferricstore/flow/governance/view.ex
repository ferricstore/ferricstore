defmodule Ferricstore.Flow.Governance.View do
  @moduledoc false

  alias Ferricstore.Flow.Governance.{Budget, CreditLease}

  def public(%Budget{} = budget), do: Budget.public(budget)

  def public(%CreditLease.Owner{} = owner) do
    owner
    |> Map.from_struct()
    |> Map.drop([:policy_version, :cleanup_head, :cleanup_tail])
    |> Map.put(:policy_version_hash, CreditLease.policy_version_hash(owner.policy_version))
    |> Map.update!(:leases, fn leases ->
      Map.new(leases, fn {shard_id, lease} -> {shard_id, public(lease)} end)
    end)
  end

  def public(%CreditLease.Lease{} = lease) do
    lease
    |> Map.from_struct()
    |> Map.drop([:reservations, :reservation_page, :reservation_page_fill])
  end

  def public(%{__struct__: _struct} = value), do: Map.from_struct(value)
  def public(value), do: value
end
