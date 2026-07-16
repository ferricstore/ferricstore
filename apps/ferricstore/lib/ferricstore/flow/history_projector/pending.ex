defmodule Ferricstore.Flow.HistoryProjector.Pending do
  @moduledoc false

  alias Ferricstore.Flow.HistoryProjector.PendingRegistry

  def reserve_pending(%{pending_counter: counter, max_pending_entries: max_pending}, count),
    do: reserve_pending_counter(counter, count, max_pending)

  def reserve_pending(projector, count) when is_atom(projector) do
    case PendingRegistry.lookup(projector) do
      {:ok, counter, max_pending} -> reserve_pending_counter(counter, count, max_pending)
      :error -> {:error, :not_registered}
    end
  end

  def reserve_pending_counter(_counter, count, _max_pending) when count <= 0, do: :ok

  def reserve_pending_counter(counter, count, :infinity) do
    :atomics.add(counter, 1, count)
    :ok
  end

  def reserve_pending_counter(counter, count, max_pending)
      when is_integer(max_pending) and max_pending >= 0 do
    pending_entries = :atomics.add_get(counter, 1, count)

    if pending_entries <= max_pending do
      :ok
    else
      _ = :atomics.add_get(counter, 1, -count)
      {:error, :queue_full, max(pending_entries - count, 0), max_pending}
    end
  end

  def release_pending(_state, count) when count <= 0, do: :ok

  def release_pending(%{pending_counter: counter}, count) do
    release_pending_counter(counter, count)
  end

  def release_pending(projector, count) when is_atom(projector) do
    case PendingRegistry.lookup(projector) do
      {:ok, counter, _max_pending} -> release_pending_counter(counter, count)
      :error -> {:error, :not_registered}
    end
  end

  defp release_pending_counter(counter, count) do
    pending_entries = :atomics.add_get(counter, 1, -count)

    if pending_entries < 0 do
      :atomics.put(counter, 1, 0)
    end

    :ok
  rescue
    _ -> :ok
  end

  def register_pending_counter(projector, counter, max_pending) when is_atom(projector) do
    PendingRegistry.register(projector, counter, max_pending)
  end

  def unregister_pending_counter(projector), do: PendingRegistry.unregister(projector)

  defdelegate reserve_replay_range(projector, entries), to: PendingRegistry
  defdelegate mark_replay_range_flushed(projector, index), to: PendingRegistry
  defdelegate trim_replay_reservation(projector, index), to: PendingRegistry
  defdelegate replay_reservation_flushed_index(projector), to: PendingRegistry

  def append_overflow(projector, entries),
    do: PendingRegistry.append_overflow(projector, entries)

  def commit_overflow(projector, sequence),
    do: PendingRegistry.commit_overflow(projector, sequence)

  def delete_overflow(projector, sequence),
    do: PendingRegistry.delete_overflow(projector, sequence)

  def take_overflow(projector, max_entries),
    do: PendingRegistry.take_overflow(projector, max_entries)

  def discard(projector), do: PendingRegistry.discard(projector)
end
