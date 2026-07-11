defmodule Ferricstore.Flow.Governance.LimitRecord do
  @moduledoc false

  alias Ferricstore.Flow.Governance.CreditLease

  @owner_tag :flow_governance_limit_v1
  @reservation_tag :flow_governance_limit_reservation
  @page_tag :flow_governance_limit_reservation_page
  @cleanup_tag :flow_governance_limit_cleanup
  @page_size 256
  @max_reservation_pages 256
  @max_reservation_id_bytes 256
  @max_exact_version 9_007_199_254_740_991

  @doc false
  def page_size, do: @page_size

  @doc false
  def max_reservation_pages, do: @max_reservation_pages

  @doc false
  def valid_reservation_id?(id),
    do: is_binary(id) and id != "" and byte_size(id) <= @max_reservation_id_bytes

  @doc false
  def encode_owner(%CreditLease.Owner{} = owner) do
    owner = owner |> CreditLease.normalize_owner() |> detach_reservations()

    with :ok <- validate_owner(owner) do
      {:ok, :erlang.term_to_binary({@owner_tag, owner})}
    end
  end

  @doc false
  def decode_owner(value) when is_binary(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {@owner_tag, %CreditLease.Owner{} = owner} -> decode_current_owner(owner)
      _invalid -> {:error, "ERR flow limit record is corrupt"}
    end
  rescue
    _error -> {:error, "ERR flow limit record is corrupt"}
  end

  def decode_owner(_value), do: {:error, "ERR flow limit record is corrupt"}

  @doc false
  def encode_reservation(reservation_id, status \\ :active)

  def encode_reservation(reservation_id, status)
      when is_binary(reservation_id) and status in [:active, :released] do
    :erlang.term_to_binary({@reservation_tag, reservation_id, status})
  end

  @doc false
  def decode_reservation(value, expected_id)
      when is_binary(value) and is_binary(expected_id) do
    case :erlang.binary_to_term(value, [:safe]) do
      {@reservation_tag, ^expected_id, status} when status in [:active, :released] ->
        {:ok, status}

      {@reservation_tag, _other_id, status} when status in [:active, :released] ->
        {:error, "ERR flow limit reservation key collision"}

      _invalid ->
        {:error, "ERR flow limit reservation record is corrupt"}
    end
  rescue
    _error -> {:error, "ERR flow limit reservation record is corrupt"}
  end

  @doc false
  def encode_page(reservation_ids) when is_list(reservation_ids) do
    if reservation_ids != [] and length(reservation_ids) <= @page_size and
         length(Enum.uniq(reservation_ids)) == length(reservation_ids) and
         Enum.all?(reservation_ids, &valid_reservation_id?/1) do
      {:ok, :erlang.term_to_binary({@page_tag, reservation_ids})}
    else
      {:error, "ERR flow limit reservation page is invalid"}
    end
  end

  @doc false
  def decode_page(value) when is_binary(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {@page_tag, reservation_ids}
      when is_list(reservation_ids) and reservation_ids != [] ->
        if length(reservation_ids) <= @page_size and
             length(Enum.uniq(reservation_ids)) == length(reservation_ids) and
             Enum.all?(reservation_ids, &valid_reservation_id?/1) do
          {:ok, reservation_ids}
        else
          {:error, "ERR flow limit reservation page is corrupt"}
        end

      _invalid ->
        {:error, "ERR flow limit reservation page is corrupt"}
    end
  rescue
    _error -> {:error, "ERR flow limit reservation page is corrupt"}
  end

  @doc false
  def encode_cleanup(shard_id, epoch, next_page, last_page)
      when is_integer(shard_id) and shard_id >= 0 and is_integer(epoch) and epoch > 0 and
             is_integer(next_page) and next_page > 0 and is_integer(last_page) and
             last_page >= next_page and epoch <= @max_exact_version and
             last_page <= @max_exact_version do
    :erlang.term_to_binary({@cleanup_tag, shard_id, epoch, next_page, last_page})
  end

  @doc false
  def decode_cleanup(value) when is_binary(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {@cleanup_tag, shard_id, epoch, next_page, last_page}
      when is_integer(shard_id) and shard_id >= 0 and is_integer(epoch) and epoch > 0 and
             is_integer(next_page) and next_page > 0 and is_integer(last_page) and
             last_page >= next_page and epoch <= @max_exact_version and
             last_page <= @max_exact_version ->
        {:ok, %{shard_id: shard_id, epoch: epoch, next_page: next_page, last_page: last_page}}

      _invalid ->
        {:error, "ERR flow limit cleanup record is corrupt"}
    end
  rescue
    _error -> {:error, "ERR flow limit cleanup record is corrupt"}
  end

  @doc false
  def detach_reservations(%CreditLease.Owner{} = owner) do
    leases =
      Map.new(owner.leases, fn {shard_id, lease} ->
        {shard_id, %{lease | reservations: %{}}}
      end)

    %{owner | leases: leases}
  end

  defp decode_current_owner(owner) do
    owner = CreditLease.normalize_owner(owner)

    with :ok <- validate_detached_reservations(owner),
         :ok <- validate_owner(owner) do
      {:ok, owner}
    end
  end

  defp validate_detached_reservations(owner) do
    if Enum.all?(owner.leases, fn {_shard_id, lease} -> lease.reservations == %{} end) do
      :ok
    else
      {:error, "ERR flow limit record is corrupt"}
    end
  end

  defp validate_owner(%CreditLease.Owner{} = owner) do
    valid? =
      is_binary(owner.scope) and owner.scope != "" and is_integer(owner.limit) and
        owner.limit >= 0 and owner.limit <= @max_exact_version and is_integer(owner.free) and
        owner.free >= 0 and owner.free <= @max_exact_version and is_integer(owner.epoch) and
        owner.epoch >= 0 and owner.epoch <= @max_exact_version and
        is_integer(owner.config_version) and owner.config_version >= 0 and
        owner.config_version <= @max_exact_version and valid_policy_version?(owner.policy_version) and
        is_integer(owner.cleanup_head) and owner.cleanup_head > 0 and
        owner.cleanup_head <= @max_exact_version and is_integer(owner.cleanup_tail) and
        owner.cleanup_tail >= 0 and owner.cleanup_tail <= @max_exact_version and
        owner.cleanup_head <= owner.cleanup_tail + 1 and is_map(owner.leases) and
        Enum.all?(owner.leases, &valid_lease_entry?/1)

    if valid?, do: :ok, else: {:error, "ERR flow limit record is corrupt"}
  end

  defp valid_lease_entry?({shard_id, %CreditLease.Lease{} = lease}) do
    is_integer(shard_id) and shard_id >= 0 and lease.shard_id == shard_id and
      is_integer(lease.epoch) and lease.epoch > 0 and lease.epoch <= @max_exact_version and
      is_integer(lease.expires_at_ms) and lease.expires_at_ms >= 0 and
      lease.expires_at_ms <= @max_exact_version and is_integer(lease.available) and
      lease.available >= 0 and lease.available <= @max_exact_version and
      is_integer(lease.in_use) and lease.in_use >= 0 and lease.in_use <= @max_exact_version and
      is_integer(lease.pending_reclaim) and lease.pending_reclaim >= 0 and
      lease.pending_reclaim <= @max_exact_version and is_integer(lease.reservation_page) and
      lease.reservation_page >= 0 and lease.reservation_page <= @max_reservation_pages and
      is_integer(lease.reservation_page_fill) and
      valid_page_fill?(lease.reservation_page, lease.reservation_page_fill) and
      (is_nil(lease.last_spend_at_ms) or
         (is_integer(lease.last_spend_at_ms) and lease.last_spend_at_ms >= 0 and
            lease.last_spend_at_ms <= @max_exact_version)) and
      lease.reservations == %{}
  end

  defp valid_lease_entry?(_entry), do: false

  defp valid_policy_version?(nil), do: true

  defp valid_policy_version?({:sha256, digest})
       when is_binary(digest) and byte_size(digest) == 32,
       do: true

  defp valid_policy_version?(_version), do: false

  defp valid_page_fill?(0, 0), do: true

  defp valid_page_fill?(page, fill),
    do: page > 0 and is_integer(fill) and fill > 0 and fill <= @page_size
end
