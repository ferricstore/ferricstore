defmodule Ferricstore.Transaction.ReplicaCompat do
  @moduledoc """
  Command forms MULTI queues only at the cluster feature level that introduced
  them (4 or 5).

  A MULTI transaction's commands run in the state machine on every replica.
  Releases before level 4 do not handle some forms this release accepts:
  EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT with several flags raise there,
  which aborts the whole transaction on that replica, and BITCOUNT ranges and
  BITPOS answer differently. During a rolling upgrade a transaction holding
  one would leave old and new replicas in different states, so MULTI refuses
  these forms while queueing until every member runs a release that applies
  them (`Ferricstore.Cluster.FeatureLevel`, level 4). Level 5 adds forms that
  releases before it do not parse at all, such as ZRANK ... WITHSCORE.
  """

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Store.Router

  @expiry_tags [:expire, :pexpire, :expireat, :pexpireat]

  @doc "Whether a parsed command needs cluster feature level 4 inside MULTI."
  @spec level_4_form?(term()) :: boolean()
  def level_4_form?(ast), do: required_level(ast) == 4

  @doc """
  The cluster feature level a parsed command needs inside MULTI, or nil for a
  form every release applies.
  """
  @spec required_level(term()) :: 4 | 5 | nil
  def required_level({tag, _key, _time, flags}) when tag in @expiry_tags and is_list(flags),
    do: 4

  def required_level({tag, args}) when tag in [:bitcount, :bitpos] and is_list(args), do: 4

  # Level 5: forms releases before it do not parse.
  def required_level({tag, _key, _member, :withscore}) when tag in [:zrank, :zrevrank], do: 5

  def required_level(_ast), do: nil

  @doc """
  The error MULTI answers for `command` with `ast` on `key`, or nil when the
  form is one every release applies or its level is active on the key's shard.
  """
  @spec queue_error(binary(), term(), [binary()], FerricStore.Instance.t()) :: binary() | nil
  def queue_error(command, ast, keys, ctx) do
    case required_level(ast) do
      nil ->
        nil

      level ->
        unless level_active?(ctx, keys, level) do
          "ERR this form of #{String.downcase(command)} inside MULTI needs cluster feature " <>
            "level #{level} (CLUSTER.FEATURES.RAISE #{level} once every node runs this release)"
        end
    end
  end

  defp level_active?(ctx, [key | _rest], level) when is_binary(key) do
    Router.read_shard_value(ctx, Router.shard_for(ctx, key), FeatureLevel.key(level)) ==
      {:ok, FeatureLevel.marker()}
  rescue
    _error -> false
  end

  defp level_active?(_ctx, _keys, _level), do: false
end
