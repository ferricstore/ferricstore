defmodule Ferricstore.Flow.HistoryProjector.PendingRegistry do
  @moduledoc false

  @pending_registry :ferricstore_flow_history_projector_pending_registry
  @replay_reservation_registry :ferricstore_flow_history_projector_replay_reservations
  @overflow_registry :ferricstore_flow_history_projector_overflow

  def reserve(%{pending_counter: counter, max_pending_entries: max_pending}, count),
    do: reserve_counter(counter, count, max_pending)

  def reserve(projector, count) when is_atom(projector) do
    case lookup(projector) do
      {:ok, counter, max_pending} -> reserve_counter(counter, count, max_pending)
      :error -> :ok
    end
  end

  def reserve_counter(_counter, count, _max_pending) when count <= 0, do: :ok

  def reserve_counter(counter, count, :infinity) do
    :atomics.add(counter, 1, count)
    :ok
  end

  def reserve_counter(counter, count, max_pending)
      when is_integer(max_pending) and max_pending >= 0 do
    pending_entries = :atomics.add_get(counter, 1, count)

    if pending_entries <= max_pending do
      :ok
    else
      _ = :atomics.add_get(counter, 1, -count)
      {:error, :queue_full, max(pending_entries - count, 0), max_pending}
    end
  end

  def release(_state, count) when count <= 0, do: :ok

  def release(%{pending_counter: counter}, count) do
    pending_entries = :atomics.add_get(counter, 1, -count)

    if pending_entries < 0 do
      :atomics.put(counter, 1, 0)
    end

    :ok
  rescue
    _ -> :ok
  end

  def register(projector, counter, max_pending) when is_atom(projector) do
    @pending_registry
    |> ensure_pending_registry()
    |> :ets.insert({projector, counter, max_pending})

    :ok
  end

  def unregister(nil), do: :ok

  def unregister(projector) when is_atom(projector) do
    case :ets.whereis(@pending_registry) do
      :undefined -> :ok
      table -> :ets.delete(table, projector)
    end

    :ok
  rescue
    _ -> :ok
  end

  def reserve_replay_range(projector, entries) do
    case entry_index_range(entries) do
      nil ->
        :ok

      {min_index, max_index} ->
        table = ensure_replay_reservation_registry()

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
    table = ensure_replay_reservation_registry()

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
    table = ensure_replay_reservation_registry()

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
    table = ensure_replay_reservation_registry()

    case :ets.lookup(table, projector) do
      [{^projector, _min_index, _max_index, flushed_index}] when is_integer(flushed_index) ->
        flushed_index

      _missing ->
        0
    end
  rescue
    _ -> 0
  end

  def lookup(projector) when is_atom(projector) do
    table = ensure_pending_registry(@pending_registry)

    case :ets.lookup(table, projector) do
      [{^projector, counter, max_pending}] -> {:ok, counter, max_pending}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  def replay_table, do: ensure_replay_reservation_registry()

  def append_overflow(projector, entries) when is_atom(projector) and is_list(entries) do
    table = ensure_overflow_registry()
    sequence = :erlang.unique_integer([:monotonic, :positive])
    :ets.insert(table, {{projector, sequence}, entries})
    :ok
  rescue
    error -> {:error, {:overflow_append_failed, error}}
  end

  def take_overflow(projector) when is_atom(projector) do
    table = ensure_overflow_registry()

    rows =
      :ets.select(table, [
        {{{projector, :"$1"}, :"$2"}, [], [{{:"$1", :"$2"}}]}
      ])

    rows
    |> Enum.sort_by(fn {sequence, _entries} -> sequence end)
    |> Enum.flat_map(fn {sequence, entries} ->
      :ets.delete(table, {projector, sequence})
      entries
    end)
  rescue
    _ -> []
  end

  defp ensure_pending_registry(name) do
    ensure_registry(
      name,
      [:named_table, :public, :set, read_concurrency: true],
      :pending_registry_unavailable
    )
  end

  defp ensure_replay_reservation_registry do
    ensure_registry(
      @replay_reservation_registry,
      [
        :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ],
      :replay_reservation_registry_unavailable
    )
  end

  defp ensure_overflow_registry do
    ensure_registry(
      @overflow_registry,
      [
        :named_table,
        :public,
        :ordered_set,
        read_concurrency: true,
        write_concurrency: true
      ],
      :overflow_registry_unavailable
    )
  end

  defp ensure_registry(name, opts, unavailable_reason) do
    case :ets.whereis(name) do
      :undefined -> ensure_registry_slow(name, opts, unavailable_reason)
      table -> table
    end
  end

  defp ensure_registry_slow(name, opts, unavailable_reason) do
    Ferricstore.Flow.HistoryProjector.TableOwner.ensure_tables()

    case :ets.whereis(name) do
      :undefined ->
        try do
          :ets.new(name, opts)
        rescue
          ArgumentError ->
            case :ets.whereis(name) do
              :undefined -> :erlang.error(unavailable_reason)
              table -> table
            end
        end

      table ->
        table
    end
  end

  defp entry_index_range(entries) do
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
