defmodule Ferricstore.Store.WriteVersion do
  @moduledoc """
  Per-shard atomic write version counters for WATCH/EXEC support.

  When writes bypass the Shard GenServer (quorum durability path), the Shard's
  internal `write_version` field is not updated. This module provides a shared
  atomic counter per shard that can be incremented by any process -- the Router
  (quorum bypass), the Shard GenServer (async path), or the StateMachine.

  The counters are stored in a single `:counters` reference held in
  `:persistent_term`, giving ~5ns read/increment with zero contention on
  different shard indices.

  ## Usage

      # During application boot (called once):
      WriteVersion.init(4)

      # Increment after a successful write:
      WriteVersion.increment(shard_index)

      # Read the current version for WATCH:
      WriteVersion.get(shard_index)
  """

  @pt_key :ferricstore_write_versions

  @doc """
  Initializes the per-shard write version counters.

  Must be called once during application startup before any writes occur.
  `shard_count` determines the number of independent counters.
  """
  @spec init(pos_integer()) :: :ok
  def init(shard_count) do
    ref = :counters.new(shard_count, [:write_concurrency])
    :persistent_term.put(@pt_key, ref)
    :ok
  end

  @doc """
  Atomically increments the write version for `shard_index`.

  Returns `:ok`. The shard_index is 0-based but `:counters` uses 1-based
  indices, so we add 1 internally.
  """
  @spec increment(non_neg_integer()) :: :ok
  def increment(shard_index) do
    ref = :persistent_term.get(@pt_key)
    size = :counters.info(ref).size
    if shard_index < size, do: :counters.add(ref, shard_index + 1, 1)
    :ok
  end

  @doc "Atomically increments the write version using instance ctx."
  @spec increment(FerricStore.Instance.t(), non_neg_integer()) :: :ok
  def increment(ctx, shard_index) do
    ref = ctx.write_version
    size = :counters.info(ref).size
    if shard_index < size, do: :counters.add(ref, shard_index + 1, 1)
    :ok
  end

  @doc """
  Returns the current write version for `shard_index`.
  """
  @spec get(non_neg_integer()) :: non_neg_integer()
  def get(shard_index) do
    ref = :persistent_term.get(@pt_key)
    size = :counters.info(ref).size

    if shard_index < size do
      :counters.get(ref, shard_index + 1)
    else
      0
    end
  end

  @doc "Returns the current write version using instance ctx."
  @spec get(FerricStore.Instance.t(), non_neg_integer()) :: non_neg_integer()
  def get(ctx, shard_index) do
    ref = ctx.write_version
    size = :counters.info(ref).size

    if shard_index < size do
      :counters.get(ref, shard_index + 1)
    else
      0
    end
  end
end
