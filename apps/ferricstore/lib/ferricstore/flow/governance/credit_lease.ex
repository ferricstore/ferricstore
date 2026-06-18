defmodule Ferricstore.Flow.Governance.CreditLease do
  @moduledoc false

  alias Ferricstore.Flow.Governance.Decision

  defmodule Owner do
    @moduledoc false
    defstruct [:scope, :limit, :free, epoch: 0, leases: %{}]
  end

  defmodule Lease do
    @moduledoc false
    defstruct [
      :shard_id,
      :epoch,
      :expires_at_ms,
      available: 0,
      in_use: 0,
      pending_reclaim: 0,
      drain_rate: 0.0,
      last_spend_at_ms: nil
    ]
  end

  def owner(scope, limit) when is_binary(scope) and is_integer(limit) and limit >= 0 do
    %Owner{scope: scope, limit: limit, free: limit}
  end

  def grant(%Owner{} = owner, shard_id, requested, opts)
      when is_integer(requested) and requested > 0 do
    now_ms = Keyword.fetch!(opts, :now_ms)
    ttl_ms = Keyword.fetch!(opts, :ttl_ms)
    owner = reclaim_expired(owner, now_ms)
    grant = min(requested, owner.free)

    if grant > 0 do
      %Lease{} = lease = Map.get(owner.leases, shard_id, new_lease(shard_id, owner.epoch + 1))

      lease = %{
        lease
        | available: lease.available + grant,
          expires_at_ms: now_ms + ttl_ms,
          pending_reclaim: max(lease.pending_reclaim - grant, 0)
      }

      owner = %Owner{
        owner
        | free: owner.free - grant,
          epoch: max(owner.epoch, lease.epoch),
          leases: Map.put(owner.leases, shard_id, lease)
      }

      {:ok, owner, lease}
    else
      owner = mark_reclaim(owner, shard_id, requested)
      {:error, exhausted(owner, requested), owner}
    end
  end

  def spend(%Owner{} = owner, shard_id, amount, opts) when is_integer(amount) and amount > 0 do
    now_ms = Keyword.fetch!(opts, :now_ms)

    case Map.fetch(owner.leases, shard_id) do
      {:ok, %Lease{available: available} = lease} when available >= amount ->
        lease = %Lease{
          lease
          | available: available - amount,
            in_use: lease.in_use + amount,
            last_spend_at_ms: now_ms
        }

        owner = %{owner | leases: Map.put(owner.leases, shard_id, lease)}
        {:ok, owner, lease}

      _other ->
        {:error, exhausted(owner, amount), owner}
    end
  end

  def release(%Owner{} = owner, shard_id, amount) when is_integer(amount) and amount > 0 do
    case Map.fetch(owner.leases, shard_id) do
      {:ok, lease} ->
        released = min(amount, lease.in_use)
        lease = %{lease | in_use: lease.in_use - released, available: lease.available + released}
        %{owner | leases: Map.put(owner.leases, shard_id, lease)}

      :error ->
        owner
    end
  end

  def reclaim_expired(%Owner{} = owner, now_ms) do
    {leases, reclaimed} =
      Enum.reduce(owner.leases, {%{}, 0}, fn {shard_id, lease}, {leases, reclaimed} ->
        if lease.expires_at_ms <= now_ms do
          {leases, reclaimed + lease.available + lease.in_use}
        else
          {Map.put(leases, shard_id, lease), reclaimed}
        end
      end)

    %{owner | free: min(owner.limit, owner.free + reclaimed), leases: leases}
  end

  defp new_lease(shard_id, epoch) do
    %Lease{shard_id: shard_id, epoch: epoch}
  end

  defp mark_reclaim(%Owner{} = owner, requester_shard_id, requested) do
    {leases, _remaining} =
      Enum.reduce(owner.leases, {owner.leases, requested}, fn
        {_shard_id, _lease}, {leases, 0} ->
          {leases, 0}

        {^requester_shard_id, _lease}, acc ->
          acc

        {shard_id, lease}, {leases, remaining} ->
          reclaimable = max(lease.available - lease.pending_reclaim, 0)
          reclaim = min(reclaimable, remaining)
          lease = %{lease | pending_reclaim: lease.pending_reclaim + reclaim}
          {Map.put(leases, shard_id, lease), remaining - reclaim}
      end)

    %{owner | leases: leases}
  end

  defp exhausted(%Owner{} = owner, requested) do
    Decision.limit_exceeded(%{
      scope: owner.scope,
      requested: requested,
      free: owner.free,
      limit: owner.limit,
      policy: "limit",
      message: "Governance credits exhausted for #{owner.scope}"
    })
  end
end
