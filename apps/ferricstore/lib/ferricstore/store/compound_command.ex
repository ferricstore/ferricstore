defmodule Ferricstore.Store.CompoundCommand do
  @moduledoc """
  Builds the default Raft command contract for compound-key writes.

  Router and the remaining Shard Raft-proxy helpers must use this module
  instead of hand-building equivalent tuples. Custom/direct shard calls still
  carry the Redis key because the local shard handler needs it for promotion and
  index bookkeeping; replicated default-instance commands keep the lean shape
  applied by `Ferricstore.Raft.StateMachine`.
  """

  @type batch_put_entry :: {binary(), binary(), non_neg_integer()}

  @spec put(binary(), binary(), non_neg_integer()) ::
          {:compound_put, binary(), binary(), non_neg_integer()}
  def put(compound_key, value, expire_at_ms),
    do: {:compound_put, compound_key, value, expire_at_ms}

  @spec batch_put(binary(), [batch_put_entry()]) ::
          {:compound_batch_put, binary(), [batch_put_entry()]}
  def batch_put(redis_key, entries), do: {:compound_batch_put, redis_key, entries}

  @spec delete(binary()) :: {:compound_delete, binary()}
  def delete(compound_key), do: {:compound_delete, compound_key}

  @spec batch_delete(binary(), [binary()]) :: {:compound_batch_delete, binary(), [binary()]}
  def batch_delete(redis_key, compound_keys),
    do: {:compound_batch_delete, redis_key, compound_keys}

  @spec delete_prefix(binary()) :: {:compound_delete_prefix, binary()}
  def delete_prefix(prefix), do: {:compound_delete_prefix, prefix}

  @spec normalize_batch_reply(term()) :: :ok | {:error, term()}
  def normalize_batch_reply({:ok, results}) when is_list(results) do
    case Enum.find(results, &match?({:error, _}, &1)) do
      nil -> :ok
      {:error, _} = error -> error
    end
  end

  def normalize_batch_reply(:ok), do: :ok
  def normalize_batch_reply({:error, _} = error), do: error
  def normalize_batch_reply(other), do: {:error, other}
end
