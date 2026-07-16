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
      :error -> {:error, :not_registered}
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
        reserve_replay_range_cas(table, projector, min_index, max_index)
    end
  rescue
    error -> {:error, {:replay_reservation_failed, error}}
  end

  def mark_replay_range_flushed(_projector, nil), do: :ok

  def mark_replay_range_flushed(projector, index) when is_integer(index) and index >= 0 do
    table = ensure_replay_reservation_registry()
    mark_replay_range_flushed_cas(table, projector, index)
  rescue
    error -> {:error, {:replay_flush_mark_failed, error}}
  end

  def trim_replay_reservation(_projector, nil), do: :ok

  def trim_replay_reservation(projector, index) when is_integer(index) and index >= 0 do
    table = ensure_replay_reservation_registry()
    trim_replay_reservation_cas(table, projector, index)
  rescue
    error -> {:error, {:replay_reservation_trim_failed, error}}
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
    :ets.insert(table, {{projector, sequence}, {:pending, entries}})
    {:ok, sequence}
  rescue
    error -> {:error, {:overflow_append_failed, error}}
  end

  def commit_overflow(projector, sequence)
      when is_atom(projector) and is_integer(sequence) and sequence > 0 do
    table = ensure_overflow_registry()
    commit_overflow_cas(table, {projector, sequence})
  rescue
    error -> {:error, {:overflow_commit_failed, error}}
  end

  def delete_overflow(projector, sequence)
      when is_atom(projector) and is_integer(sequence) and sequence > 0 do
    table = ensure_overflow_registry()
    :ets.delete(table, {projector, sequence})
    :ok
  rescue
    error -> {:error, {:overflow_delete_failed, error}}
  end

  def take_overflow(projector, max_entries)
      when is_atom(projector) and is_integer(max_entries) and max_entries > 0 do
    table = ensure_overflow_registry()
    take_overflow_entries(table, projector, {projector, 0}, max_entries, [])
  rescue
    error -> {:error, {:overflow_take_failed, error}}
  end

  def take_overflow(_projector, max_entries),
    do: {:error, {:invalid_overflow_take_limit, max_entries}}

  def discard(projector) when is_atom(projector) do
    overflow_table = ensure_overflow_registry()
    replay_table = ensure_replay_reservation_registry()

    :ets.select_delete(overflow_table, [
      {{{projector, :"$1"}, :"$2"}, [], [true]}
    ])

    :ets.delete(replay_table, projector)
    :ok
  rescue
    error -> {:error, {:pending_discard_failed, error}}
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

  defp take_overflow_entries(_table, _projector, _key, 0, acc),
    do: {:ok, Enum.reverse(acc)}

  defp take_overflow_entries(table, projector, key, remaining, acc) do
    case :ets.next(table, key) do
      :"$end_of_table" ->
        {:ok, Enum.reverse(acc)}

      {^projector, _sequence} = next_key ->
        case :ets.lookup(table, next_key) do
          [{^next_key, {:ready, entries}}] when is_list(entries) ->
            {taken, rest} = Enum.split(entries, remaining)
            next_acc = Enum.reverse(taken, acc)

            case rest do
              [] ->
                :ets.delete(table, next_key)

                take_overflow_entries(
                  table,
                  projector,
                  next_key,
                  remaining - length(taken),
                  next_acc
                )

              [_ | _] ->
                :ets.insert(table, {next_key, {:ready, rest}})
                {:ok, Enum.reverse(next_acc)}
            end

          [{^next_key, {:pending, entries}}] when is_list(entries) ->
            {:ok, Enum.reverse(acc)}

          invalid ->
            {:error, {:invalid_overflow_row, next_key, invalid}}
        end

      _other_projector ->
        {:ok, Enum.reverse(acc)}
    end
  end

  defp reserve_replay_range_cas(table, projector, min_index, max_index) do
    case :ets.lookup(table, projector) do
      [] ->
        if :ets.insert_new(table, {projector, min_index, max_index, 0}) do
          :ok
        else
          reserve_replay_range_cas(table, projector, min_index, max_index)
        end

      [{^projector, old_min, old_max, flushed_index} = current]
      when is_integer(old_min) and old_min >= 0 and is_integer(old_max) and old_max >= old_min and
             is_integer(flushed_index) and flushed_index >= 0 ->
        replacement =
          {projector, min(old_min, min_index), max(old_max, max_index), flushed_index}

        replace_replay_row(
          table,
          current,
          replacement,
          fn -> reserve_replay_range_cas(table, projector, min_index, max_index) end
        )

      invalid ->
        {:error, {:invalid_replay_reservation, projector, invalid}}
    end
  end

  defp commit_overflow_cas(table, key) do
    case :ets.lookup(table, key) do
      [{^key, {:pending, entries}}] when is_list(entries) ->
        case :ets.update_element(table, key, {2, {:ready, entries}}) do
          true -> :ok
          false -> commit_overflow_cas(table, key)
        end

      [{^key, {:ready, entries}}] when is_list(entries) ->
        :ok

      [] ->
        {:error, {:overflow_row_missing, key}}

      invalid ->
        {:error, {:invalid_overflow_row, key, invalid}}
    end
  end

  defp mark_replay_range_flushed_cas(table, projector, index) do
    case :ets.lookup(table, projector) do
      [] ->
        :ok

      [{^projector, min_index, max_index, flushed_index} = current]
      when is_integer(min_index) and min_index >= 0 and is_integer(max_index) and
             max_index >= min_index and is_integer(flushed_index) and flushed_index >= 0 ->
        replacement = {projector, min_index, max_index, max(flushed_index, index)}

        replace_replay_row(
          table,
          current,
          replacement,
          fn -> mark_replay_range_flushed_cas(table, projector, index) end
        )

      invalid ->
        {:error, {:invalid_replay_reservation, projector, invalid}}
    end
  end

  defp trim_replay_reservation_cas(table, projector, index) do
    case :ets.lookup(table, projector) do
      [] ->
        :ok

      [{^projector, min_index, max_index, flushed_index} = current]
      when is_integer(min_index) and min_index >= 0 and is_integer(max_index) and
             max_index >= min_index and is_integer(flushed_index) and flushed_index >= 0 ->
        cond do
          index >= max_index ->
            case :ets.select_delete(table, [{current, [], [true]}]) do
              1 -> :ok
              0 -> trim_replay_reservation_cas(table, projector, index)
            end

          index >= min_index ->
            replacement = {projector, index + 1, max_index, flushed_index}

            replace_replay_row(
              table,
              current,
              replacement,
              fn -> trim_replay_reservation_cas(table, projector, index) end
            )

          true ->
            :ok
        end

      invalid ->
        {:error, {:invalid_replay_reservation, projector, invalid}}
    end
  end

  defp replace_replay_row(_table, current, current, _retry), do: :ok

  defp replace_replay_row(table, current, replacement, retry) do
    case :ets.select_replace(table, [{current, [], [{replacement}]}]) do
      1 -> :ok
      0 -> retry.()
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
