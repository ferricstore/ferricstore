defmodule Ferricstore.Store.CompoundCommand do
  @moduledoc """
  Builds the default Raft command contract for compound-key writes.

  Router and the remaining Shard Raft-proxy helpers must use this module
  instead of hand-building equivalent tuples. Custom/direct shard calls still
  carry the Redis key because the local shard handler needs it for promotion and
  index bookkeeping; replicated default-instance commands keep the lean shape
  applied by `Ferricstore.Raft.StateMachine`.
  """

  alias Ferricstore.ErrorReasons

  @type batch_put_entry :: {binary(), binary(), non_neg_integer()}

  @spec type_claim(binary(), atom()) :: {:compound_type_claim, binary(), atom()}
  def type_claim(redis_key, type), do: {:compound_type_claim, redis_key, type}

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

  @spec normalize_batch_reply(term(), non_neg_integer()) :: :ok | {:error, term()}
  def normalize_batch_reply({:ok, results}, expected_count)
      when is_list(results) and is_integer(expected_count) and expected_count >= 0 do
    case validate_batch_results(results, expected_count, nil) do
      {:ok, nil} -> :ok
      {:ok, {:error, _reason} = error} -> error
      :invalid -> ErrorReasons.write_timeout_unknown()
    end
  end

  def normalize_batch_reply({:ok, _invalid}, expected_count)
      when is_integer(expected_count) and expected_count >= 0,
      do: ErrorReasons.write_timeout_unknown()

  def normalize_batch_reply(:ok, expected_count)
      when is_integer(expected_count) and expected_count >= 0,
      do: :ok

  def normalize_batch_reply({:error, _} = error, _expected_count), do: error
  def normalize_batch_reply(other, _expected_count), do: {:error, other}

  defp validate_batch_results([], 0, first_error), do: {:ok, first_error}
  defp validate_batch_results([], _remaining, _first_error), do: :invalid
  defp validate_batch_results([_result | _results], 0, _first_error), do: :invalid

  defp validate_batch_results([:ok | results], remaining, first_error),
    do: validate_batch_results(results, remaining - 1, first_error)

  defp validate_batch_results([{:error, _reason} = error | results], remaining, nil),
    do: validate_batch_results(results, remaining - 1, error)

  defp validate_batch_results([{:error, _reason} | results], remaining, first_error),
    do: validate_batch_results(results, remaining - 1, first_error)

  defp validate_batch_results([_invalid | _results], _remaining, _first_error), do: :invalid
end
