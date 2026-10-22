defmodule Ferricstore.Raft.LocalLeadership do
  @moduledoc false

  # Work that apply hints on every replica but that ends in a Raft proposal
  # runs on the shard's leader only: every other node would repeat the same
  # proposal. A node that cannot tell its role does the work anyway, since
  # such proposals are idempotent.

  alias Ferricstore.Raft.Backend
  alias Ferricstore.Store.Router

  @spec leads?(map(), non_neg_integer()) :: boolean()
  def leads?(%{name: :default} = ctx, shard_index) when is_integer(shard_index) do
    not Router.durable_raft?(ctx) or Backend.local_role(shard_index) in [:leader, :unknown]
  end

  def leads?(_ctx, _shard_index), do: true
end
