defmodule Ferricstore.Flow.Governance.CreditLease do
  @moduledoc false

  alias Ferricstore.Flow.Governance.Decision

  @max_exact_version 9_007_199_254_740_991

  defmodule Owner do
    @moduledoc false
    defstruct [
      :scope,
      :limit,
      :free,
      :policy_version,
      config_version: 0,
      epoch: 0,
      cleanup_head: 1,
      cleanup_tail: 0,
      leases: %{}
    ]
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
      last_spend_at_ms: nil,
      reservation_page: 0,
      reservation_page_fill: 0,
      reservations: %{}
    ]
  end

  def owner(scope, limit, opts \\ [])

  def owner(scope, limit, opts)
      when is_binary(scope) and is_integer(limit) and limit >= 0 and is_list(opts) do
    %Owner{
      scope: scope,
      limit: limit,
      free: limit,
      config_version: Keyword.get(opts, :config_version, 0),
      policy_version: policy_version_fingerprint(Keyword.get(opts, :policy_version))
    }
  end

  def reconfigure(%Owner{} = owner, limit, config_version, policy_version \\ nil)
      when is_integer(limit) and limit >= 0 and is_integer(config_version) and
             config_version >= 0 do
    %Owner{} = owner = normalize_owner(owner)
    current_version = Map.get(owner, :config_version, 0)
    policy_version = policy_version_fingerprint(policy_version)

    cond do
      config_version < current_version ->
        {:ok, owner}

      config_version == current_version and
          (limit != owner.limit or policy_version_conflict?(owner, policy_version)) ->
        {:error, "ERR flow limit config_version conflict", owner}

      config_version == current_version ->
        {:ok, owner}

      true ->
        owner = %{
          owner
          | limit: limit,
            config_version: config_version,
            policy_version: policy_version
        }

        {:ok, rebalance_capacity(owner)}
    end
  end

  def grant(%Owner{} = owner, shard_id, requested, opts)
      when is_integer(requested) and requested > 0 do
    now_ms = Keyword.fetch!(opts, :now_ms)
    ttl_ms = Keyword.fetch!(opts, :ttl_ms)
    owner = reclaim_expired(owner, now_ms)
    grant = min(requested, owner.free)

    cond do
      grant > 0 and not Map.has_key?(owner.leases, shard_id) and
          owner.epoch >= @max_exact_version ->
        {:error, "ERR flow limit lease generation exhausted", owner}

      grant > 0 ->
        lease =
          owner.leases
          |> Map.get(shard_id, new_lease(shard_id, owner.epoch + 1))
          |> normalize_lease()

        lease = %{
          lease
          | available: lease.available + grant,
            expires_at_ms: extend_expiry(lease.expires_at_ms, now_ms + ttl_ms),
            pending_reclaim: max(lease.pending_reclaim - grant, 0)
        }

        owner = %Owner{
          owner
          | free: owner.free - grant,
            epoch: max(owner.epoch, lease.epoch),
            leases: Map.put(owner.leases, shard_id, lease)
        }

        {:ok, owner, lease}

      true ->
        owner = mark_reclaim(owner, shard_id, requested)
        {:error, exhausted(owner, requested), owner}
    end
  end

  def spend(%Owner{} = owner, shard_id, amount, opts) when is_integer(amount) and amount > 0 do
    now_ms = Keyword.fetch!(opts, :now_ms)
    owner = reclaim_expired(owner, now_ms)
    reservation_ids = Keyword.get(opts, :reservation_ids, [])

    case Map.fetch(owner.leases, shard_id) do
      {:ok, %Lease{} = raw_lease} ->
        %Lease{} = lease = normalize_lease(raw_lease)

        cond do
          not valid_reservation_ids?(reservation_ids, amount) ->
            {:error, "ERR flow limit reservation_ids must match amount", owner}

          reservation_ids != [] and
              Enum.all?(reservation_ids, &Map.has_key?(lease.reservations, &1)) ->
            {:ok, owner, lease}

          reservation_ids != [] and
              Enum.any?(reservation_ids, &Map.has_key?(lease.reservations, &1)) ->
            {:error, "ERR flow limit reservation_id conflict", owner}

          is_integer(lease.last_spend_at_ms) and now_ms < lease.last_spend_at_ms ->
            {:error, "ERR flow limit now_ms cannot precede last_spend_at_ms", owner}

          lease.available >= amount ->
            lease = %Lease{
              lease
              | available: lease.available - amount,
                in_use: lease.in_use + amount,
                last_spend_at_ms: now_ms,
                expires_at_ms: spend_expires_at_ms(lease, now_ms, opts),
                reservations: put_reservations(lease.reservations, reservation_ids)
            }

            owner = %{owner | leases: Map.put(owner.leases, shard_id, lease)}
            {:ok, owner, lease}

          true ->
            {:error, exhausted(owner, amount), owner}
        end

      _other ->
        {:error, exhausted(owner, amount), owner}
    end
  end

  def renew(%Owner{} = owner, shard_id, opts) when is_integer(shard_id) and shard_id >= 0 do
    now_ms = Keyword.fetch!(opts, :now_ms)
    ttl_ms = Keyword.fetch!(opts, :ttl_ms)
    owner = reclaim_expired(owner, now_ms)

    case Map.fetch(owner.leases, shard_id) do
      {:ok, raw_lease} ->
        lease = normalize_lease(raw_lease)
        lease = %{lease | expires_at_ms: max(lease.expires_at_ms, now_ms + ttl_ms)}
        {:ok, %{owner | leases: Map.put(owner.leases, shard_id, lease)}, lease}

      :error ->
        {:error, "ERR flow limit lease expired", owner}
    end
  end

  def release(owner, shard_id, amount, opts \\ [])

  def release(%Owner{} = owner, shard_id, amount, opts)
      when is_integer(amount) and amount > 0 and is_list(opts) do
    reservation_ids = Keyword.get(opts, :reservation_ids)

    if valid_release_reservation_ids?(reservation_ids, amount) do
      release_reservations(owner, shard_id, reservation_ids)
    else
      owner
    end
  end

  @doc false
  def release_identified_amount(%Owner{} = owner, shard_id, amount)
      when is_integer(amount) and amount >= 0 do
    case Map.fetch(owner.leases, shard_id) do
      {:ok, raw_lease} ->
        lease = normalize_lease(raw_lease)
        released = min(amount, lease.in_use)
        lease = %{lease | in_use: lease.in_use - released, available: lease.available + released}

        owner
        |> Map.put(:leases, Map.put(owner.leases, shard_id, lease))
        |> rebalance_capacity()

      :error ->
        owner
    end
  end

  @doc false
  def expired_lease_refs(%Owner{} = owner, now_ms)
      when is_integer(now_ms) and now_ms >= 0 do
    owner.leases
    |> Enum.flat_map(fn {shard_id, raw_lease} ->
      lease = normalize_lease(raw_lease)

      if lease.expires_at_ms <= now_ms and lease.reservation_page > 0 do
        [{shard_id, lease.epoch, lease.reservation_page}]
      else
        []
      end
    end)
    |> Enum.sort()
  end

  @doc false
  def normalize_owner(%Owner{} = owner) do
    owner = struct(Owner, Map.from_struct(owner))

    leases =
      Map.new(owner.leases, fn {shard_id, lease} ->
        {shard_id, normalize_lease(lease)}
      end)

    %{owner | leases: leases}
  end

  @doc false
  def policy_version_fingerprint(nil), do: nil

  def policy_version_fingerprint({:sha256, digest} = fingerprint)
      when is_binary(digest) and byte_size(digest) == 32,
      do: fingerprint

  def policy_version_fingerprint(policy_version)
      when is_binary(policy_version) or is_integer(policy_version) do
    {:sha256, :crypto.hash(:sha256, :erlang.term_to_binary(policy_version))}
  end

  @doc false
  def policy_version_hash(nil), do: nil

  def policy_version_hash({:sha256, digest}) when is_binary(digest) and byte_size(digest) == 32,
    do: Base.url_encode64(digest, padding: false)

  def reclaim_expired(%Owner{} = owner, now_ms) do
    leases =
      Enum.reduce(owner.leases, %{}, fn {shard_id, raw_lease}, leases ->
        lease = normalize_lease(raw_lease)

        if lease.expires_at_ms <= now_ms do
          leases
        else
          Map.put(leases, shard_id, lease)
        end
      end)

    owner
    |> Map.put(:leases, leases)
    |> rebalance_capacity()
  end

  defp new_lease(shard_id, epoch) do
    %Lease{shard_id: shard_id, epoch: epoch}
  end

  defp normalize_lease(%Lease{} = lease) do
    lease
    |> Map.from_struct()
    |> Map.update(:reservations, %{}, fn
      reservations when is_map(reservations) -> reservations
      _invalid -> %{}
    end)
    |> then(&struct(Lease, &1))
  end

  defp valid_reservation_ids?([], _amount), do: true

  defp valid_reservation_ids?(reservation_ids, amount) when is_list(reservation_ids) do
    length(reservation_ids) == amount and
      length(Enum.uniq(reservation_ids)) == amount and
      Enum.all?(reservation_ids, &(is_binary(&1) and &1 != ""))
  end

  defp valid_reservation_ids?(_reservation_ids, _amount), do: false

  defp valid_release_reservation_ids?(reservation_ids, amount)
       when is_list(reservation_ids) and reservation_ids != [] do
    valid_reservation_ids?(reservation_ids, amount)
  end

  defp valid_release_reservation_ids?(_reservation_ids, _amount), do: false

  defp put_reservations(reservations, reservation_ids) do
    Enum.reduce(reservation_ids, reservations, fn reservation_id, acc ->
      Map.put(acc, reservation_id, 1)
    end)
  end

  defp pop_reservations(reservations, []), do: {reservations, 0}

  defp pop_reservations(reservations, reservation_ids) do
    reservation_ids
    |> Enum.uniq()
    |> Enum.reduce({reservations, 0}, fn reservation_id, {reservations, released} ->
      case Map.fetch(reservations, reservation_id) do
        {:ok, amount} when is_integer(amount) and amount > 0 ->
          {Map.delete(reservations, reservation_id), released + amount}

        _missing_or_invalid ->
          {reservations, released}
      end
    end)
  end

  defp release_reservations(owner, shard_id, reservation_ids) do
    case Map.fetch(owner.leases, shard_id) do
      {:ok, raw_lease} ->
        lease = normalize_lease(raw_lease)
        {reservations, identified_release} = pop_reservations(lease.reservations, reservation_ids)
        released = min(identified_release, lease.in_use)

        lease = %{
          lease
          | in_use: lease.in_use - released,
            available: lease.available + released,
            reservations: reservations
        }

        owner
        |> Map.put(:leases, Map.put(owner.leases, shard_id, lease))
        |> rebalance_capacity()

      :error ->
        owner
    end
  end

  defp spend_expires_at_ms(lease, now_ms, opts) do
    case Keyword.get(opts, :ttl_ms) do
      ttl_ms when is_integer(ttl_ms) and ttl_ms > 0 -> max(lease.expires_at_ms, now_ms + ttl_ms)
      _none -> lease.expires_at_ms
    end
  end

  defp extend_expiry(nil, expires_at_ms), do: expires_at_ms

  defp extend_expiry(current_expires_at_ms, expires_at_ms),
    do: max(current_expires_at_ms, expires_at_ms)

  defp rebalance_capacity(%Owner{} = owner) do
    allocated = allocated_credits(owner.leases)

    if allocated <= owner.limit do
      %{owner | free: owner.limit - allocated}
    else
      {leases, _remaining_excess} =
        owner.leases
        |> Enum.sort_by(fn {shard_id, _lease} -> shard_id end)
        |> Enum.reduce({%{}, allocated - owner.limit}, fn
          {shard_id, raw_lease}, {leases, remaining_excess} ->
            lease = normalize_lease(raw_lease)
            drained = min(lease.available, remaining_excess)
            available = lease.available - drained

            lease = %{
              lease
              | available: available,
                pending_reclaim: min(lease.pending_reclaim, available)
            }

            {Map.put(leases, shard_id, lease), remaining_excess - drained}
        end)

      allocated = allocated_credits(leases)
      %{owner | leases: leases, free: max(owner.limit - allocated, 0)}
    end
  end

  defp allocated_credits(leases) do
    Enum.reduce(leases, 0, fn {_shard_id, raw_lease}, total ->
      lease = normalize_lease(raw_lease)
      total + lease.available + lease.in_use
    end)
  end

  defp policy_version_conflict?(_owner, nil), do: false

  defp policy_version_conflict?(owner, policy_version),
    do: Map.get(owner, :policy_version) != policy_version

  defp mark_reclaim(%Owner{} = owner, requester_shard_id, requested) do
    {leases, _remaining} =
      owner.leases
      |> Enum.sort_by(fn {shard_id, _lease} -> shard_id end)
      |> Enum.reduce({owner.leases, requested}, fn
        {_shard_id, _lease}, {leases, 0} ->
          {leases, 0}

        {^requester_shard_id, _lease}, acc ->
          acc

        {shard_id, raw_lease}, {leases, remaining} ->
          lease = normalize_lease(raw_lease)
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
