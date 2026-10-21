defmodule Ferricstore.Transaction.ReplicaCompat do
  @moduledoc """
  Command forms MULTI queues only at cluster feature level 4.

  A MULTI transaction's commands run in the state machine on every replica.
  Releases before level 4 do not handle some forms this release accepts:
  EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT with several flags raise there,
  which aborts the whole transaction on that replica, and BITCOUNT ranges and
  BITPOS answer differently. During a rolling upgrade a transaction holding
  one would leave old and new replicas in different states, so MULTI refuses
  these forms while queueing until every member runs a release that applies
  them (`Ferricstore.Cluster.FeatureLevel`, level 4).
  """

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Store.Router

  @expiry_tags [:expire, :pexpire, :expireat, :pexpireat]

  @doc "Whether a parsed command needs cluster feature level 4 inside MULTI."
  @spec level_4_form?(term()) :: boolean()
  def level_4_form?({tag, _key, _time, flags}) when tag in @expiry_tags and is_list(flags),
    do: true

  def level_4_form?({tag, args}) when tag in [:bitcount, :bitpos] and is_list(args), do: true
  def level_4_form?(_ast), do: false

  @doc """
  The error MULTI answers for `command` with `ast` on `key`, or nil when the
  form is one every release applies or level 4 is active on the key's shard.
  """
  @spec queue_error(binary(), term(), [binary()], FerricStore.Instance.t()) :: binary() | nil
  def queue_error(command, ast, keys, ctx) do
    if level_4_form?(ast) and not level_4_active?(ctx, keys) do
      "ERR this form of #{String.downcase(command)} inside MULTI needs cluster feature level 4 " <>
        "(CLUSTER.FEATURES.RAISE 4 once every node runs this release)"
    end
  end

  defp level_4_active?(ctx, [key | _rest]) when is_binary(key) do
    Router.read_shard_value(ctx, Router.shard_for(ctx, key), FeatureLevel.key(4)) ==
      {:ok, FeatureLevel.marker()}
  rescue
    _error -> false
  end

  defp level_4_active?(_ctx, _keys), do: false
end
