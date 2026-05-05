defmodule Ferricstore.Store.DiskPressure do
  @moduledoc """
  Per-shard atomic disk pressure flags.

  When a Bitcask flush fails (e.g., ENOSPC), the shard sets its disk pressure
  flag. The Router write path checks this flag before accepting writes,
  returning an error instead of silently queuing data that can't be persisted.

  The flag is cleared when a flush succeeds, allowing writes to resume
  automatically once disk space is available (e.g., after compaction reclaims
  space or an operator adds storage).

  Uses `:atomics` for lock-free ~5ns reads from any process.
  """

  @pt_key :ferricstore_disk_pressure

  @spec init(pos_integer()) :: :ok
  def init(shard_count) do
    ref = :atomics.new(shard_count, signed: false)
    :persistent_term.put(@pt_key, ref)
    :ok
  end

  @spec set(non_neg_integer()) :: :ok
  def set(shard_index) do
    ref = :persistent_term.get(@pt_key)
    size = :atomics.info(ref).size
    if shard_index < size, do: :atomics.put(ref, shard_index + 1, 1)
    :ok
  end

  @doc "Sets disk pressure flag for a shard using instance ctx."
  @spec set(FerricStore.Instance.t(), non_neg_integer()) :: :ok
  def set(nil, _shard_index),
    do: raise(ArgumentError, "instance_ctx is required — shard must be started with instance_ctx")

  def set(ctx, shard_index) do
    ref = ctx.disk_pressure
    size = :atomics.info(ref).size
    if shard_index < size, do: :atomics.put(ref, shard_index + 1, 1)
    :ok
  end

  @spec clear(non_neg_integer()) :: :ok
  def clear(shard_index) do
    ref = :persistent_term.get(@pt_key)
    size = :atomics.info(ref).size
    if shard_index < size, do: :atomics.put(ref, shard_index + 1, 0)
    :ok
  end

  @doc "Clears disk pressure flag for a shard using instance ctx."
  @spec clear(FerricStore.Instance.t(), non_neg_integer()) :: :ok
  def clear(nil, _shard_index),
    do: raise(ArgumentError, "instance_ctx is required — shard must be started with instance_ctx")

  def clear(ctx, shard_index) do
    ref = ctx.disk_pressure
    size = :atomics.info(ref).size
    if shard_index < size, do: :atomics.put(ref, shard_index + 1, 0)
    :ok
  end

  @spec under_pressure?(non_neg_integer()) :: boolean()
  def under_pressure?(shard_index) do
    ref = :persistent_term.get(@pt_key)
    size = :atomics.info(ref).size

    if shard_index < size do
      :atomics.get(ref, shard_index + 1) == 1
    else
      false
    end
  end

  @doc "Checks disk pressure for a shard using instance ctx."
  @spec under_pressure?(FerricStore.Instance.t(), non_neg_integer()) :: boolean()
  def under_pressure?(nil, _shard_index),
    do: raise(ArgumentError, "instance_ctx is required — shard must be started with instance_ctx")

  def under_pressure?(ctx, shard_index) do
    ref = ctx.disk_pressure
    size = :atomics.info(ref).size

    if shard_index < size do
      :atomics.get(ref, shard_index + 1) == 1
    else
      false
    end
  end
end
