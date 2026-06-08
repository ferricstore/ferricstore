defmodule Ferricstore.Store.Shard.ETS.Accounting do
  @moduledoc false

  alias Ferricstore.Store.ExpiryTracker

  @doc false
  def offheap_size(value) when is_binary(value) and byte_size(value) > 64, do: byte_size(value)
  def offheap_size(_value), do: 0

  @doc false
  def track_binary_delete(%{instance_ctx: %{keydir_binary_bytes: ref}, index: idx} = state, key)
      when ref != nil do
    previous = :ets.lookup(state.keydir, key)
    ExpiryTracker.adjust_for_state(state, ExpiryTracker.entry_expire_at(previous), 0)

    bytes =
      case previous do
        [{^key, val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(val)
        _ -> 0
      end

    if bytes > 0, do: :atomics.sub(ref, idx + 1, bytes)
  end

  def track_binary_delete(_, _), do: :ok

  @doc false
  def track_binary_delete(
        %{instance_ctx: %{keydir_binary_bytes: ref}, index: idx} = state,
        key,
        value
      )
      when ref != nil do
    previous = :ets.lookup(state.keydir, key)
    ExpiryTracker.adjust_for_state(state, ExpiryTracker.entry_expire_at(previous), 0)

    bytes = offheap_size(key) + offheap_size(value)
    if bytes > 0, do: :atomics.sub(ref, idx + 1, bytes)
  end

  def track_binary_delete(_, _, _), do: :ok

  @doc false
  def track_binary_add(shard_index, key, value, %{keydir_binary_bytes: ref}) when ref != nil do
    bytes = offheap_size(key) + offheap_size(value)
    if bytes > 0, do: :atomics.add(ref, shard_index + 1, bytes)
  end

  def track_binary_add(_, _, _, _), do: :ok
end
