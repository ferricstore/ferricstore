defmodule FerricstoreServer.Connection.Registry do
  @moduledoc """
  Tracks live connection processes by native client ID.

  The table is intentionally tiny and write-light: one insert on connection
  open, small summary updates when connection metadata changes, one delete on
  close, and point lookups for commands like `CLIENT KILL`.
  """

  @table :ferricstore_server_connections

  @spec init_table() :: :ok
  def init_table do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [
            :set,
            :public,
            :named_table,
            {:read_concurrency, true},
            {:write_concurrency, :auto}
          ])
        rescue
          ArgumentError -> :ok
        end

      _tid ->
        :ok
    end

    :ok
  end

  @type summary :: %{
          optional(:client_id) => pos_integer(),
          optional(:pid) => pid(),
          optional(:client_name) => binary() | nil,
          optional(:username) => binary() | nil,
          optional(:peer) => binary(),
          optional(:created_at_ms) => integer(),
          optional(:flags) => binary()
        }

  @spec register(pos_integer(), pid(), summary()) :: :ok
  def register(client_id, pid \\ self(), summary \\ %{})
      when is_integer(client_id) and is_pid(pid) and is_map(summary) do
    init_table()
    :ets.insert(@table, {client_id, pid, Map.put(summary, :client_id, client_id)})
    :ok
  end

  @spec update(pos_integer(), pid(), summary()) :: :ok
  def update(client_id, pid \\ self(), summary)
      when is_integer(client_id) and is_pid(pid) and is_map(summary) do
    init_table()

    case :ets.lookup(@table, client_id) do
      [{^client_id, ^pid, _old_summary}] ->
        :ets.insert(@table, {client_id, pid, Map.put(summary, :client_id, client_id)})

      [{^client_id, ^pid}] ->
        :ets.insert(@table, {client_id, pid, Map.put(summary, :client_id, client_id)})

      _ ->
        :ok
    end

    :ok
  end

  @spec unregister(pos_integer(), pid()) :: :ok
  def unregister(client_id, pid \\ self()) when is_integer(client_id) and is_pid(pid) do
    init_table()

    case :ets.lookup(@table, client_id) do
      [{^client_id, ^pid, _summary}] -> :ets.delete(@table, client_id)
      [{^client_id, ^pid}] -> :ets.delete(@table, client_id)
      _ -> :ok
    end

    :ok
  end

  @type snapshot :: %{
          clients: [summary()],
          registered_count: non_neg_integer(),
          pubsub_count: non_neg_integer(),
          transaction_count: non_neg_integer(),
          oldest_created_at_ms: integer() | nil
        }

  @spec snapshot(non_neg_integer()) :: snapshot()
  def snapshot(limit \\ 500) do
    init_table()
    limit = max(limit, 0)
    now_ms = System.monotonic_time(:millisecond)

    {clients, registered_count, pubsub_count, transaction_count, oldest_created_at_ms} =
      :ets.foldl(
        fn entry, acc -> snapshot_entry(entry, acc, limit, now_ms) end,
        {:gb_trees.empty(), 0, 0, 0, nil},
        @table
      )

    %{
      clients: clients |> :gb_trees.to_list() |> Enum.map(&elem(&1, 1)),
      registered_count: registered_count,
      pubsub_count: pubsub_count,
      transaction_count: transaction_count,
      oldest_created_at_ms: oldest_created_at_ms
    }
  end

  defp snapshot_entry(
         {client_id, pid, summary},
         acc,
         limit,
         now_ms
       )
       when is_integer(client_id) and is_pid(pid) and is_map(summary) do
    if Process.alive?(pid) do
      add_live_summary(client_id, pid, summary, acc, limit, now_ms)
    else
      :ets.delete(@table, client_id)
      acc
    end
  end

  defp snapshot_entry({client_id, pid}, acc, limit, now_ms)
       when is_integer(client_id) and is_pid(pid) do
    if Process.alive?(pid) do
      add_live_summary(client_id, pid, %{}, acc, limit, now_ms)
    else
      :ets.delete(@table, client_id)
      acc
    end
  end

  defp snapshot_entry(_invalid, acc, _limit, _now_ms), do: acc

  defp add_live_summary(
         client_id,
         pid,
         summary,
         {clients, registered_count, pubsub_count, transaction_count, oldest_created_at_ms},
         limit,
         now_ms
       ) do
    summary = summary |> Map.put(:client_id, client_id) |> Map.put(:pid, pid)
    created_at_ms = valid_created_at(summary, now_ms)
    flags = Map.get(summary, :flags, "")

    clients = bounded_client_insert(clients, {created_at_ms, client_id}, summary, limit)

    {
      clients,
      registered_count + 1,
      pubsub_count + flag_count(flags, "S"),
      transaction_count + flag_count(flags, "M"),
      oldest_created_at(oldest_created_at_ms, created_at_ms)
    }
  end

  defp bounded_client_insert(clients, _rank, _summary, 0), do: clients

  defp bounded_client_insert(clients, rank, summary, limit) do
    clients = :gb_trees.enter(rank, summary, clients)

    if :gb_trees.size(clients) > limit do
      {largest_rank, _summary} = :gb_trees.largest(clients)
      :gb_trees.delete(largest_rank, clients)
    else
      clients
    end
  end

  defp valid_created_at(%{created_at_ms: created_at_ms}, _now_ms) when is_integer(created_at_ms),
    do: created_at_ms

  defp valid_created_at(_summary, now_ms), do: now_ms

  defp flag_count(flags, flag) when is_binary(flags) do
    if String.contains?(flags, flag), do: 1, else: 0
  end

  defp flag_count(_flags, _flag), do: 0

  defp oldest_created_at(nil, created_at_ms), do: created_at_ms
  defp oldest_created_at(oldest, created_at_ms), do: min(oldest, created_at_ms)

  @spec lookup(pos_integer()) :: {:ok, pid()} | {:error, :not_found}
  def lookup(client_id) when is_integer(client_id) do
    init_table()

    case :ets.lookup(@table, client_id) do
      [{^client_id, pid, _summary}] when is_pid(pid) ->
        if Process.alive?(pid) do
          {:ok, pid}
        else
          :ets.delete(@table, client_id)
          {:error, :not_found}
        end

      [{^client_id, pid}] when is_pid(pid) ->
        if Process.alive?(pid) do
          {:ok, pid}
        else
          :ets.delete(@table, client_id)
          {:error, :not_found}
        end

      _ ->
        {:error, :not_found}
    end
  end

  @spec kill(pos_integer(), pid()) :: :ok | {:error, :not_found | :self}
  def kill(client_id, caller_pid \\ self()) when is_integer(client_id) and is_pid(caller_pid) do
    case lookup(client_id) do
      {:ok, ^caller_pid} ->
        {:error, :self}

      {:ok, pid} ->
        send(pid, :client_kill)
        :ok

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end
end
