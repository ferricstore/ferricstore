defmodule FerricstoreServer.Connection.Registry do
  @moduledoc """
  Tracks live connection processes by native client ID.

  The table is intentionally tiny and write-light: one insert on connection
  open, small summary updates when connection metadata changes, one delete on
  close, and point lookups for commands like `CLIENT KILL`.
  """

  @table :ferricstore_server_connections
  @acl_table :ferricstore_server_connection_acl_memberships

  @spec init_table() :: :ok
  def init_table do
    ensure_table(@table, :set)
    ensure_table(@acl_table, :bag)
    :ok
  end

  defp ensure_table(table, type) do
    case :ets.whereis(table) do
      :undefined ->
        try do
          :ets.new(table, [
            type,
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
    delete_acl_memberships(client_id)
    :ets.insert(@table, {client_id, pid, Map.put(summary, :client_id, client_id)})
    maybe_add_summary_acl_user(client_id, pid, summary)
    :ok
  end

  @spec update(pos_integer(), pid(), summary()) :: :ok
  def update(client_id, pid \\ self(), summary)
      when is_integer(client_id) and is_pid(pid) and is_map(summary) do
    init_table()

    case :ets.lookup(@table, client_id) do
      [{^client_id, ^pid, old_summary}] ->
        maybe_replace_summary_acl_user(client_id, pid, old_summary, summary)
        :ets.insert(@table, {client_id, pid, Map.put(summary, :client_id, client_id)})

      [{^client_id, ^pid}] ->
        maybe_add_summary_acl_user(client_id, pid, summary)
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
      [{^client_id, ^pid, _summary}] ->
        :ets.delete(@table, client_id)
        delete_acl_memberships(client_id)

      [{^client_id, ^pid}] ->
        :ets.delete(@table, client_id)
        delete_acl_memberships(client_id)

      _ ->
        :ok
    end

    :ok
  end

  @doc false
  @spec add_acl_user(pos_integer(), pid(), binary()) :: :ok
  def add_acl_user(client_id, pid, username)
      when is_integer(client_id) and is_pid(pid) and is_binary(username) do
    init_table()

    if registered_connection?(client_id, pid) do
      :ets.insert(@acl_table, [
        {{:user, username}, client_id, pid},
        {{:client, client_id}, username, pid}
      ])
    end

    :ok
  end

  @doc false
  @spec remove_acl_user(pos_integer(), pid(), binary()) :: :ok
  def remove_acl_user(client_id, pid, username)
      when is_integer(client_id) and is_pid(pid) and is_binary(username) do
    init_table()
    delete_acl_membership(client_id, pid, username)
    :ok
  end

  @doc false
  @spec replace_acl_user(pos_integer(), pid(), binary(), binary()) :: :ok
  def replace_acl_user(client_id, pid, previous_username, username)
      when is_integer(client_id) and is_pid(pid) and is_binary(previous_username) and
             is_binary(username) do
    if previous_username == username do
      add_acl_user(client_id, pid, username)
    else
      :ok = add_acl_user(client_id, pid, username)
      :ok = remove_acl_user(client_id, pid, previous_username)
    end
  end

  @doc false
  @spec acl_user_pids(binary()) :: [pid()]
  def acl_user_pids(username) when is_binary(username) do
    init_table()

    @acl_table
    |> :ets.lookup({:user, username})
    |> Enum.reduce([], fn
      {{:user, ^username}, client_id, pid}, pids when is_integer(client_id) and is_pid(pid) ->
        if registered_connection?(client_id, pid) and Process.alive?(pid) do
          [pid | pids]
        else
          delete_acl_membership(client_id, pid, username)
          pids
        end

      _invalid, pids ->
        pids
    end)
  end

  @doc false
  @spec all_pids() :: [pid()]
  def all_pids do
    init_table()

    :ets.foldl(
      fn
        {client_id, pid, _summary}, pids when is_integer(client_id) and is_pid(pid) ->
          collect_live_pid(client_id, pid, pids)

        {client_id, pid}, pids when is_integer(client_id) and is_pid(pid) ->
          collect_live_pid(client_id, pid, pids)

        _invalid, pids ->
          pids
      end,
      [],
      @table
    )
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
      unregister(client_id, pid)
      acc
    end
  end

  defp snapshot_entry({client_id, pid}, acc, limit, now_ms)
       when is_integer(client_id) and is_pid(pid) do
    if Process.alive?(pid) do
      add_live_summary(client_id, pid, %{}, acc, limit, now_ms)
    else
      unregister(client_id, pid)
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
          unregister(client_id, pid)
          {:error, :not_found}
        end

      [{^client_id, pid}] when is_pid(pid) ->
        if Process.alive?(pid) do
          {:ok, pid}
        else
          unregister(client_id, pid)
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

  defp maybe_add_summary_acl_user(client_id, pid, %{username: username})
       when is_binary(username),
       do: add_acl_user(client_id, pid, username)

  defp maybe_add_summary_acl_user(_client_id, _pid, _summary), do: :ok

  defp maybe_replace_summary_acl_user(client_id, pid, old_summary, new_summary) do
    previous_username = Map.get(old_summary, :username)
    username = Map.get(new_summary, :username)

    cond do
      is_binary(previous_username) and is_binary(username) ->
        replace_acl_user(client_id, pid, previous_username, username)

      is_binary(username) ->
        add_acl_user(client_id, pid, username)

      is_binary(previous_username) ->
        remove_acl_user(client_id, pid, previous_username)

      true ->
        :ok
    end
  end

  defp registered_connection?(client_id, pid) do
    case :ets.lookup(@table, client_id) do
      [{^client_id, ^pid, _summary}] -> true
      [{^client_id, ^pid}] -> true
      _other -> false
    end
  end

  defp delete_acl_memberships(client_id) do
    @acl_table
    |> :ets.lookup({:client, client_id})
    |> Enum.each(fn
      {{:client, ^client_id}, username, pid} ->
        delete_acl_membership(client_id, pid, username)

      _invalid ->
        :ok
    end)

    :ok
  end

  defp delete_acl_membership(client_id, pid, username) do
    :ets.delete_object(@acl_table, {{:user, username}, client_id, pid})
    :ets.delete_object(@acl_table, {{:client, client_id}, username, pid})
    :ok
  end

  defp collect_live_pid(client_id, pid, pids) do
    if Process.alive?(pid) do
      [pid | pids]
    else
      unregister(client_id, pid)
      pids
    end
  end
end
