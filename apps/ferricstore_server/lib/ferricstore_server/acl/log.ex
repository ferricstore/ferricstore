defmodule FerricstoreServer.Acl.Log do
  @moduledoc """
  The ACL LOG: this node's latest ACL denials and failed authentications,
  newest first, as Redis keeps them. A denial like one logged in the last
  60 seconds (same reason, object and user) counts on that entry instead of
  adding one, and the log keeps the latest #{128} entries.

  It is always on, unlike `Ferricstore.AuditLog`, and node-local, as Redis's
  is per server.
  """

  @table :ferricstore_server_acl_log
  @max_entries 128
  @merge_window_ms 60_000
  @merge_scan 16

  @type reason :: binary()

  @doc false
  @spec init_table() :: :ok
  def init_table do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [:ordered_set, :public, :named_table, {:write_concurrency, true}])
        rescue
          ArgumentError -> :ok
        end

      _tid ->
        :ok
    end

    :ok
  end

  @doc """
  Records a denial: `reason` is `"command"`, `"key"`, `"channel"` or
  `"auth"`, `object` what was denied (a command name, or `AUTH`).
  """
  @spec record(reason(), binary(), binary() | nil, binary()) :: :ok
  def record(reason, object, username, client_info) do
    init_table()
    now = System.system_time(:millisecond)
    username = username || "default"

    case recent_match(reason, object, username, now) do
      {id, entry} ->
        :ets.insert(@table, {id, %{entry | count: entry.count + 1, updated_ms: now}})

      nil ->
        id = System.unique_integer([:positive, :monotonic])

        :ets.insert(
          @table,
          {id,
           %{
             count: 1,
             reason: reason,
             context: "toplevel",
             object: object,
             username: username,
             client_info: client_info,
             entry_id: id,
             created_ms: now,
             updated_ms: now
           }}
        )

        trim()
    end

    :ok
  rescue
    _error -> :ok
  end

  @doc "The latest `count` entries (or all), newest first, in Redis's form."
  @spec entries(non_neg_integer() | :all) :: [map()]
  def entries(count) do
    init_table()
    now = System.system_time(:millisecond)

    @table
    |> :ets.tab2list()
    |> Enum.sort_by(fn {_id, entry} -> {entry.updated_ms, entry.entry_id} end, :desc)
    |> then(fn entries -> if count == :all, do: entries, else: Enum.take(entries, count) end)
    |> Enum.map(fn {_id, entry} -> format(entry, now) end)
  end

  @doc "Clears the log."
  @spec reset() :: :ok
  def reset do
    init_table()
    :ets.delete_all_objects(@table)
    :ok
  end

  defp recent_match(reason, object, username, now) do
    @table
    |> newest(@merge_scan)
    |> Enum.find(fn {_id, entry} ->
      entry.reason == reason and entry.object == object and entry.username == username and
        now - entry.updated_ms <= @merge_window_ms
    end)
  end

  defp newest(table, count), do: newest(table, :ets.last(table), count, [])

  defp newest(_table, :"$end_of_table", _count, acc), do: Enum.reverse(acc)
  defp newest(_table, _key, 0, acc), do: Enum.reverse(acc)

  defp newest(table, key, count, acc) do
    case :ets.lookup(table, key) do
      [row] -> newest(table, :ets.prev(table, key), count - 1, [row | acc])
      [] -> newest(table, :ets.prev(table, key), count, acc)
    end
  end

  defp trim do
    if :ets.info(@table, :size) > @max_entries do
      case :ets.first(@table) do
        :"$end_of_table" -> :ok
        oldest -> :ets.delete(@table, oldest) && trim()
      end
    end
  end

  defp format(entry, now) do
    %{
      "count" => entry.count,
      "reason" => entry.reason,
      "context" => entry.context,
      "object" => entry.object,
      "username" => entry.username,
      "age-seconds" => (now - entry.updated_ms) / 1_000,
      "client-info" => entry.client_info,
      "entry-id" => entry.entry_id,
      "timestamp-created" => entry.created_ms,
      "timestamp-last-updated" => entry.updated_ms
    }
  end
end
