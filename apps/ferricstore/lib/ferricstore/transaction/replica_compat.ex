defmodule Ferricstore.Transaction.ReplicaCompat do
  @moduledoc """
  Command forms MULTI queues only at the cluster feature level that introduced
  them (4, 5, 6 or 10).

  A MULTI transaction's commands run in the state machine on every replica.
  Releases before level 4 do not handle some forms this release accepts:
  EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT with several flags raise there,
  which aborts the whole transaction on that replica, and BITCOUNT ranges and
  BITPOS answer differently. During a rolling upgrade a transaction holding
  one would leave old and new replicas in different states, so MULTI refuses
  these forms while queueing until every member runs a release that applies
  them (`Ferricstore.Cluster.FeatureLevel`, level 4). Level 5 adds forms that
  releases before it do not parse at all, such as ZRANK ... WITHSCORE and
  HEXPIRE ... NX, or refuse, such as PFMERGE with no source and HEXPIRE with
  a TTL of 0. Level 10 adds ZUNION, ZINTER, ZDIFF, their STORE forms,
  ZINTERCARD, ZRANGESTORE, ZRANGEBYLEX, ZREVRANGEBYLEX, ZLEXCOUNT, the
  ZREMRANGEBY* commands and ZMPOP, which releases before it do not have, and
  level 12 the commands added with it (HMSET, TOUCH, ...).
  """

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Store.Router

  @expiry_tags [:expire, :pexpire, :expireat, :pexpireat]
  @zset_algebra_tags [
    :zunion,
    :zinter,
    :zdiff,
    :zunionstore,
    :zinterstore,
    :zdiffstore,
    :zintercard,
    :zrangestore,
    :zrangebylex,
    :zrevrangebylex,
    :zlexcount,
    :zremrangebyrank,
    :zremrangebyscore,
    :zremrangebylex,
    :zmpop
  ]
  @field_expiry_tags [:hexpire, :hpexpire]
  # Level 12: commands older releases do not have.
  @level_12_tags [
    :hmset,
    :touch,
    :hexpireat,
    :hpexpireat,
    :hpexpiretime,
    :lmpop,
    :lcs,
    :sort,
    :sort_ro
  ]

  @doc "Whether a parsed command needs cluster feature level 4 inside MULTI."
  @spec level_4_form?(term()) :: boolean()
  def level_4_form?(ast), do: required_level(ast) == 4

  @doc """
  The cluster feature level a parsed command needs inside MULTI, or nil for a
  form every release applies.
  """
  @spec required_level(term()) :: 4 | 5 | 6 | 10 | 12 | nil
  def required_level(ast)
      when is_tuple(ast) and tuple_size(ast) > 1 and elem(ast, 0) in @level_12_tags,
      do: 12

  def required_level({tag, _key, _time, flags}) when tag in @expiry_tags and is_list(flags),
    do: 4

  def required_level({tag, args}) when tag in [:bitcount, :bitpos] and is_list(args), do: 4

  # Level 5: forms releases before it do not parse or refuse.
  def required_level({tag, _key, _member, :withscore}) when tag in [:zrank, :zrevrank], do: 5
  def required_level({:pfmerge, [_destination]}), do: 5

  def required_level({tag, _key, 0, fields}) when tag in @field_expiry_tags and is_list(fields),
    do: 5

  def required_level({tag, _key, _ttl, _condition, fields})
      when tag in @field_expiry_tags and is_list(fields),
      do: 5

  # HSETEX's Redis 8 form, and HGETEX with a TTL of 0 (older releases parse
  # neither: they refuse the TTL, and answer the form an unsupported command).
  def required_level({:hsetex, _key, {_condition, _expiry}, pairs}) when is_list(pairs), do: 5

  def required_level({:hgetex, _key, {unit, 0}, fields})
      when unit in [:ex, :px] and is_list(fields),
      do: 5

  # ZRANGE ... BYLEX (older releases have no clause for its form).
  def required_level({:zrange, _key, {:lex, _start, _stop}, opts}) when is_map(opts), do: 5

  # SET ... IFEQ / IFNE (older releases refuse the options).
  def required_level({:set, _key, _value, opts}) when is_list(opts) do
    if Enum.any?(opts, &match?({tag, _expected} when tag in [:ifeq, :ifne], &1)), do: 5
  end

  # ZADD INCR (older releases refuse the option).
  def required_level({:zadd, _key, opts, pairs}) when is_list(opts) and is_list(pairs),
    do: if(:incr in opts, do: 5)

  # Level 10: sorted-set commands older releases do not have.
  def required_level({tag, args}) when tag in @zset_algebra_tags and is_list(args), do: 10

  # TDIGEST (in a transaction from level 6: older releases refuse it there; its
  # digest is a plain value the transaction reads and writes like any other).
  def required_level(ast) when is_tuple(ast) and tuple_size(ast) > 0 and is_atom(elem(ast, 0)) do
    if tdigest_tag?(elem(ast, 0)), do: 6
  end

  def required_level(_ast), do: nil

  defp tdigest_tag?(tag), do: String.starts_with?(Atom.to_string(tag), "tdigest_")

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

  @doc false
  @spec level_active?(FerricStore.Instance.t(), [binary()], pos_integer()) :: boolean()
  def level_active?(ctx, [key | _rest], level) when is_binary(key) do
    Router.read_shard_value(ctx, Router.shard_for(ctx, key), FeatureLevel.key(level)) ==
      {:ok, FeatureLevel.marker()}
  rescue
    _error -> false
  end

  def level_active?(_ctx, _keys, _level), do: false
end
