defmodule FerricstoreServer.Connection.Registry do
  @moduledoc """
  Tracks live connection processes by Redis client ID.

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

  @spec summaries() :: [summary()]
  def summaries do
    init_table()

    @table
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {client_id, pid, summary} when is_integer(client_id) and is_pid(pid) and is_map(summary) ->
        if Process.alive?(pid) do
          [summary |> Map.put(:client_id, client_id) |> Map.put(:pid, pid)]
        else
          :ets.delete(@table, client_id)
          []
        end

      {client_id, pid} when is_integer(client_id) and is_pid(pid) ->
        if Process.alive?(pid) do
          [%{client_id: client_id, pid: pid}]
        else
          :ets.delete(@table, client_id)
          []
        end

      _ ->
        []
    end)
  end

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
