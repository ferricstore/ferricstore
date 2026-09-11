defmodule FerricstoreServer.Health.Dashboard.Data.Clients do
  @moduledoc false
  import FerricstoreServer.Health.Dashboard.QueryParams, only: [dashboard_param: 2]

  @limit 500
  @scan_budget 10_000
  @registry :ferricstore_server_connections

  def snapshot(opts \\ [], table \\ @registry, observer \\ nil) do
    filters = %{q: dashboard_param(opts, "q"), cursor: dashboard_param(opts, "cursor")}

    if String.length(filters.q) > 256 or byte_size(filters.cursor) > 128 do
      unavailable(filters, :invalid_filters)
    else
      collect(filters, table, observer)
    end
  end

  defp collect(filters, table, observer) do
    try do
      # Fix only for this bounded traversal. Writers remain free to disconnect
      # clients, but a deleted key stays usable by :ets.next until release.
      :ets.safe_fixtable(table, true)

      try do
        total = :ets.info(table, :size)

        with {:ok, key} <- first_key(table, filters.cursor) do
          now = System.monotonic_time(:millisecond)
          query = String.downcase(filters.q)

          {rows, scanned, last_key, next_key} =
            scan(table, key, query, now, [], 0, 0, nil, observer)

          %{
            status: :ok,
            clients: Enum.reverse(rows),
            scanned_count: scanned,
            total_registered: total,
            complete?: next_key == :"$end_of_table",
            next_cursor:
              if(next_key == :"$end_of_table", do: nil, else: Integer.to_string(last_key)),
            filters: filters
          }
        else
          {:error, status} -> unavailable(filters, status)
        end
      after
        :ets.safe_fixtable(table, false)
      end
    rescue
      ArgumentError -> unavailable(filters)
    catch
      :exit, _ -> unavailable(filters)
    end
  end

  defp first_key(table, ""), do: {:ok, :ets.first(table)}

  defp first_key(table, cursor) do
    case Integer.parse(cursor) do
      {id, ""} when id >= 0 ->
        if :ets.member(table, id),
          do: {:ok, :ets.next(table, id)},
          else: {:error, :expired_cursor}

      _ ->
        {:error, :expired_cursor}
    end
  end

  defp scan(_table, :"$end_of_table" = key, _query, _now, rows, _count, scanned, last, _observer),
    do: {rows, scanned, last, key}

  defp scan(_table, key, _query, _now, rows, count, scanned, last, _observer)
       when count >= @limit or scanned >= @scan_budget,
       do: {rows, scanned, last, key}

  defp scan(table, key, query, now, rows, count, scanned, _last, observer) do
    row =
      case :ets.lookup(table, key) do
        [{id, pid, summary}] when is_integer(id) and is_pid(pid) and is_map(summary) ->
          if matches?(id, summary, query), do: row(id, pid, summary, now), else: nil

        [{id, pid}] when is_integer(id) and is_pid(pid) ->
          if matches?(id, %{}, query), do: row(id, pid, %{}, now), else: nil

        _ ->
          nil
      end

    {rows, count} = if row, do: {[row | rows], count + 1}, else: {rows, count}
    if is_function(observer, 1), do: observer.({:before_next, key})
    scan(table, :ets.next(table, key), query, now, rows, count, scanned + 1, key, observer)
  end

  defp matches?(_id, _summary, ""), do: true

  defp matches?(id, summary, query) do
    Enum.any?(
      [
        Integer.to_string(id)
        | Enum.map([:client_name, :username, :peer, :flags], &Map.get(summary, &1))
      ],
      fn value -> is_binary(value) and String.contains?(String.downcase(value), query) end
    )
  end

  defp row(id, pid, summary, now) do
    created = Map.get(summary, :created_at_ms)

    %{
      pid: pid,
      client_id: id,
      client_name: Map.get(summary, :client_name),
      username: Map.get(summary, :username),
      peer: Map.get(summary, :peer, "unknown"),
      flags: Map.get(summary, :flags, ""),
      age_seconds: if(is_integer(created), do: max(0, div(now - created, 1_000)), else: 0)
    }
  end

  defp unavailable(filters, status \\ :unavailable) do
    %{
      status: status,
      clients: [],
      scanned_count: 0,
      total_registered: nil,
      complete?: false,
      next_cursor: nil,
      filters: filters
    }
  end
end
