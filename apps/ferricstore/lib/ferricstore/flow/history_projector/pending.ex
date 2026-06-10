defmodule Ferricstore.Flow.HistoryProjector.Pending do
  @moduledoc false

  alias Ferricstore.Flow.HistoryProjector.PendingRegistry

  def reserve_pending(%{pending_counter: counter, max_pending_entries: max_pending}, count),
    do: reserve_pending_counter(counter, count, max_pending)

  def reserve_pending(projector, count) when is_atom(projector) do
    case PendingRegistry.lookup(projector) do
      {:ok, counter, max_pending} -> reserve_pending_counter(counter, count, max_pending)
      :error -> :ok
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

  def reserve_replay_range(projector, entries) do
    case entry_index_range(entries) do
      nil ->
        :ok

      {min_index, max_index} ->
        table = PendingRegistry.replay_table()

        case :ets.lookup(table, projector) do
          [{^projector, old_min, old_max, flushed_index}] ->
            :ets.insert(
              table,
              {projector, min(old_min, min_index), max(old_max, max_index), flushed_index}
            )

          _missing ->
            :ets.insert(table, {projector, min_index, max_index, 0})
        end

        :ok
    end
  rescue
    _ -> :ok
  end

  def mark_replay_range_flushed(_projector, nil), do: :ok

  def mark_replay_range_flushed(projector, index) when is_integer(index) and index >= 0 do
    table = PendingRegistry.replay_table()

    case :ets.lookup(table, projector) do
      [{^projector, min_index, max_index, flushed_index}] ->
        :ets.insert(table, {projector, min_index, max_index, max(flushed_index, index)})

      _missing ->
        :ok
    end

    :ok
  rescue
    _ -> :ok
  end

  def trim_replay_reservation(_projector, nil), do: :ok

  def trim_replay_reservation(projector, index) when is_integer(index) and index >= 0 do
    table = PendingRegistry.replay_table()

    case :ets.lookup(table, projector) do
      [{^projector, _min_index, max_index, _flushed_index}] when index >= max_index ->
        :ets.delete(table, projector)

      [{^projector, min_index, max_index, flushed_index}] when index >= min_index ->
        :ets.insert(table, {projector, index + 1, max_index, flushed_index})

      _other ->
        :ok
    end

    :ok
  rescue
    _ -> :ok
  end

  def replay_reservation_flushed_index(projector) do
    table = PendingRegistry.replay_table()

    case :ets.lookup(table, projector) do
      [{^projector, _min_index, _max_index, flushed_index}] when is_integer(flushed_index) ->
        flushed_index

      _missing ->
        0
    end
  rescue
    _ -> 0
  end

  def append_overflow(projector, entries),
    do: PendingRegistry.append_overflow(projector, entries)

  def take_overflow(projector), do: PendingRegistry.take_overflow(projector)

  def entry_index_range(entries) do
    Enum.reduce(entries, nil, fn entry, acc ->
      case Map.get(entry, :ra_index) do
        index when is_integer(index) and index >= 0 ->
          case acc do
            nil -> {index, index}
            {min_index, max_index} -> {min(min_index, index), max(max_index, index)}
          end

        _other ->
          acc
      end
    end)
  end
end
