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

  # How far the history log holds a projector's history: everything up to
  # the index is appended and synced, though maybe not published yet. Only a
  # successor that recovered, which publishes what the log holds, may count
  # it as flushed. It names the shard data directory, so a projector of the
  # same name started for another directory does not take it.
  def mark_replay_range_logged(projector, shard_data_path, index)
      when is_atom(projector) and is_binary(shard_data_path) and is_integer(index) and
             index >= 0 do
    table = ensure_replay_reservation_registry()
    key = {:logged, projector}

    case :ets.lookup(table, key) do
      [{^key, ^shard_data_path, logged}] when is_integer(logged) and logged >= index -> :ok
      _older_other_directory_or_none -> :ets.insert(table, {key, shard_data_path, index})
    end

    :ok
  rescue
    error -> {:error, {:replay_log_mark_failed, error}}
  end

  def replay_logged_index(projector, shard_data_path)
      when is_atom(projector) and is_binary(shard_data_path) do
    table = ensure_replay_reservation_registry()

    case :ets.lookup(table, {:logged, projector}) do
      [{_key, ^shard_data_path, index}] when is_integer(index) and index >= 0 -> index
      _none_or_other_directory -> 0
    end
  rescue
    _ -> 0
  end

  def replay_recovery_required?(projector) when is_atom(projector) do
    table = ensure_replay_reservation_registry()

    case :ets.lookup(table, projector) do
      [] ->
        false

      [{^projector, min_index, max_index, flushed_index}]
      when is_integer(min_index) and min_index >= 0 and is_integer(max_index) and
             max_index >= min_index and is_integer(flushed_index) and flushed_index >= 0 ->
        flushed_index < max_index

      _invalid ->
        true
    end
  rescue
    _ -> true
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
    # The writer is kept so a row it never committed (it died mid-apply, and
    # the apply will run again) can be told from one still being staged.
    :ets.insert(table, {{projector, sequence}, {:pending, entries, self()}})
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

  # Overflow drains oldest first, and the queue may take a newer entry while
  # older ones still wait here; a projected index past the oldest waiting
  # entry would claim history that is not written. :infinity when none waits.
  def overflow_publish_limit(projector) when is_atom(projector) do
    case :ets.whereis(@overflow_registry) do
      # Without the table nothing can wait in overflow.
      :undefined -> :infinity
      table -> overflow_publish_limit(table, projector, {projector, 0})
    end
  rescue
    ArgumentError -> :infinity
  end

  defp overflow_publish_limit(table, projector, key) do
    case :ets.next(table, key) do
      {^projector, _sequence} = next_key ->
        case :ets.lookup(table, next_key) do
          # Drained meanwhile: the next row is now the oldest.
          [] ->
            overflow_publish_limit(table, projector, next_key)

          [{_key, {:pending, _entries, writer}}] = row ->
            if abandoned_writer?(writer),
              do: overflow_publish_limit(table, projector, next_key),
              else: oldest_overflow_limit(row)

          row ->
            oldest_overflow_limit(row)
        end

      _end_or_other_projector ->
        :infinity
    end
  end

  # Entries without a Raft index come from a projector outside WARaft, which
  # has no replay cursor to hold back.
  defp oldest_overflow_limit([{_key, {:pending, entries, _writer}}]) when is_list(entries),
    do: oldest_entries_limit(entries)

  defp oldest_overflow_limit([{_key, {:ready, entries}}]) when is_list(entries),
    do: oldest_entries_limit(entries)

  defp oldest_overflow_limit(_invalid), do: 0

  defp oldest_entries_limit(entries) do
    entries
    |> Enum.flat_map(fn
      %{ra_index: index} when is_integer(index) -> [index]
      _entry -> []
    end)
    |> case do
      [] -> :infinity
      indexes -> max(Enum.min(indexes) - 1, 0)
    end
  end

  # Whether history waits in the overflow queue: an entry that is not the
  # tombstone of one already removed, in a row the drain will take. Rows a
  # writer died before committing are dropped by the drain, so they do not
  # count.
  def overflow_history_pending?(projector) when is_atom(projector) do
    case :ets.whereis(@overflow_registry) do
      :undefined -> {:ok, false}
      table -> {:ok, overflow_history_pending?(table, projector, {projector, 0})}
    end
  rescue
    error -> {:error, {:overflow_read_failed, error}}
  end

  defp overflow_history_pending?(table, projector, key) do
    case :ets.next(table, key) do
      {^projector, _sequence} = next_key ->
        case :ets.lookup(table, next_key) do
          [{_key, {:pending, entries, writer}}] ->
            (not abandoned_writer?(writer) and history_entries?(entries)) or
              overflow_history_pending?(table, projector, next_key)

          [{_key, {:ready, entries}}] ->
            history_entries?(entries) or overflow_history_pending?(table, projector, next_key)

          # Drained meanwhile.
          [] ->
            overflow_history_pending?(table, projector, next_key)

          _invalid ->
            true
        end

      _end_or_other_projector ->
        false
    end
  end

  defp history_entries?(entries) when is_list(entries),
    do: Enum.any?(entries, &(not match?(%{tombstone: true}, &1)))

  defp history_entries?(_invalid), do: true

  # Rows are staged and committed by the applying process on this node.
  defp abandoned_writer?(writer) when is_pid(writer), do: not Process.alive?(writer)
  defp abandoned_writer?(_writer), do: false

  def delete_overflow(projector, sequence)
      when is_atom(projector) and is_integer(sequence) and sequence > 0 do
    table = ensure_overflow_registry()
    :ets.delete(table, {projector, sequence})
    :ok
  rescue
    error -> {:error, {:overflow_delete_failed, error}}
  end

  # A stopping projector fills the overflow row it took with
  # `append_overflow(projector, [])` with the history it never flushed. The
  # row keeps its place in drain order. The hand-over names the shard data
  # directory: a projector of the same name started for another directory (a
  # restarted instance) drops it instead of writing it there.
  def hand_over(projector, _shard_data_path, sequence, []),
    do: delete_overflow(projector, sequence)

  def hand_over(projector, shard_data_path, sequence, entries)
      when is_atom(projector) and is_binary(shard_data_path) and is_integer(sequence) and
             sequence > 0 and is_list(entries) do
    registry = ensure_pending_registry(@pending_registry)
    overflow = ensure_overflow_registry()

    sequences =
      case :ets.lookup(registry, {:hand_over, projector}) do
        [{_key, ^shard_data_path, sequences}] -> sequences
        _none_or_other_directory -> []
      end

    # The marker goes first: a row it names but never filled stays a
    # placeholder of a dead writer, which the drain drops.
    :ets.insert(registry, {{:hand_over, projector}, shard_data_path, [sequence | sequences]})
    :ets.insert(overflow, {{projector, sequence}, {:ready, entries}})
    :ok
  rescue
    error -> {:error, {:history_hand_over_failed, error}}
  end

  def take_hand_over(projector, shard_data_path)
      when is_atom(projector) and is_binary(shard_data_path) do
    registry = ensure_pending_registry(@pending_registry)

    case :ets.take(registry, {:hand_over, projector}) do
      [{_key, ^shard_data_path, _sequences}] ->
        :ok

      [{_key, _other_directory, sequences}] ->
        Enum.each(sequences, &delete_overflow(projector, &1))

      [] ->
        :ok
    end
  rescue
    error -> {:error, {:history_hand_over_take_failed, error}}
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
    :ets.delete(replay_table, {:logged, projector})
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

          [{^next_key, {:pending, entries, writer}} = row] when is_list(entries) ->
            if abandoned_writer?(writer) do
              :ets.delete_object(table, row)
              take_overflow_entries(table, projector, next_key, remaining, acc)
            else
              {:ok, Enum.reverse(acc)}
            end

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
      [{^key, {:pending, entries, _writer}}] when is_list(entries) ->
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
