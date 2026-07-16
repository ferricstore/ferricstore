defmodule Ferricstore.Commands.Stream.Waiters do
  @moduledoc false

  alias Ferricstore.Commands.Stream.CacheKey

  @stream_waiters_table :ferricstore_stream_waiters

  @spec register(binary(), pid(), binary()) :: :ok
  def register(stream_key, pid, last_seen_id), do: register(stream_key, pid, last_seen_id, nil)

  @spec register(binary(), pid(), binary(), term()) :: :ok
  def register(stream_key, pid, last_seen_id, store) do
    ensure_table()
    Ferricstore.Waiters.Monitor.track(pid)
    registered_at = System.monotonic_time(:microsecond)

    :ets.insert(
      @stream_waiters_table,
      {CacheKey.build(store, stream_key), pid, last_seen_id, registered_at}
    )

    :ok
  end

  @spec unregister(binary(), pid()) :: :ok
  def unregister(stream_key, pid), do: unregister(stream_key, pid, nil)

  @spec unregister(binary(), pid(), term()) :: :ok
  def unregister(stream_key, pid, store) do
    if :ets.whereis(@stream_waiters_table) != :undefined do
      :ets.match_delete(
        @stream_waiters_table,
        {CacheKey.build(store, stream_key), pid, :_, :_}
      )
    end

    :ok
  end

  @spec cleanup(pid()) :: :ok
  def cleanup(pid) do
    Ferricstore.Commands.Stream.WaiterCleanup.cleanup(pid)
  end

  @spec count(binary()) :: non_neg_integer()
  def count(stream_key), do: count(stream_key, nil)

  @spec count(binary(), term()) :: non_neg_integer()
  def count(stream_key, store) do
    ensure_table()

    :ets.select_count(@stream_waiters_table, [
      {{CacheKey.build(store, stream_key), :_, :_, :_}, [], [true]}
    ])
  end

  @spec snapshot(non_neg_integer()) :: [map()]
  def snapshot(limit \\ 100) do
    ensure_table()
    limit = max(limit, 0)

    if limit == 0 do
      []
    else
      :ets.safe_fixtable(@stream_waiters_table, true)

      try do
        ranked =
          @stream_waiters_table
          |> :ets.first()
          |> snapshot_waiter_keys(
            System.monotonic_time(:microsecond),
            limit,
            :gb_sets.empty()
          )

        ranked
        |> :gb_sets.to_list()
        |> Enum.map(&elem(&1, 1))
      after
        :ets.safe_fixtable(@stream_waiters_table, false)
      end
    end
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp snapshot_waiter_keys(:"$end_of_table", _now_us, _limit, acc), do: acc

  defp snapshot_waiter_keys(stream_key, now_us, limit, acc) do
    next_key = :ets.next(@stream_waiters_table, stream_key)

    acc =
      case waiter_snapshot_row(stream_key, now_us) do
        nil -> acc
        row -> insert_waiter_snapshot(row, stream_key, acc, limit)
      end

    snapshot_waiter_keys(next_key, now_us, limit, acc)
  end

  defp waiter_snapshot_row(stream_key, now_us) do
    case CacheKey.raw(stream_key) do
      raw_stream_key when is_binary(raw_stream_key) ->
        {count, oldest_registered_at, last_seen_id} =
          @stream_waiters_table
          |> :ets.lookup(stream_key)
          |> Enum.reduce({0, nil, nil}, fn
            {^stream_key, _pid, last_seen_id, registered_at}, {count, oldest, first_seen}
            when is_integer(registered_at) ->
              oldest = if is_nil(oldest), do: registered_at, else: min(oldest, registered_at)
              first_seen = if is_nil(first_seen), do: last_seen_id, else: first_seen
              {count + 1, oldest, first_seen}

            _invalid, acc ->
              acc
          end)

        if count == 0 do
          nil
        else
          %{
            key: raw_stream_key,
            waiters: count,
            oldest_wait_us: max(now_us - oldest_registered_at, 0),
            last_seen_id: last_seen_id
          }
        end

      nil ->
        nil
    end
  end

  defp insert_waiter_snapshot(row, cache_key, acc, limit) do
    rank = {-row.waiters, row.key, cache_key}
    ranked = :gb_sets.add({rank, row}, acc)

    if :gb_sets.size(ranked) > limit do
      :gb_sets.del_element(:gb_sets.largest(ranked), ranked)
    else
      ranked
    end
  end

  @spec notify(binary()) :: :ok
  def notify(stream_key), do: notify(stream_key, nil)

  @spec notify(binary(), term()) :: :ok
  def notify(stream_key, store) do
    case :ets.whereis(@stream_waiters_table) do
      :undefined ->
        :ok

      _ref ->
        cache_key = CacheKey.build(store, stream_key)
        entries = :ets.lookup(@stream_waiters_table, cache_key)

        Enum.each(entries, fn {_cache_key, pid, _last_id, _reg_at} ->
          send(pid, {:stream_waiter_notify, stream_key})
        end)

        :ets.match_delete(@stream_waiters_table, {cache_key, :_, :_, :_})
        :ok
    end
  end

  @spec clear(binary(), term()) :: :ok
  def clear(stream_key, store) do
    if :ets.whereis(@stream_waiters_table) != :undefined do
      :ets.match_delete(
        @stream_waiters_table,
        {CacheKey.build(store, stream_key), :_, :_, :_}
      )
    end

    :ok
  end

  defp ensure_table do
    case :ets.whereis(@stream_waiters_table) do
      :undefined ->
        try do
          :ets.new(@stream_waiters_table, [:duplicate_bag, :public, :named_table])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end
  end
end
