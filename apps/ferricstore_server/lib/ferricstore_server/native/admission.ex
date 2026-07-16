defmodule FerricstoreServer.Native.Admission do
  @moduledoc false

  use GenServer

  alias Ferricstore.Stats

  @table :ferricstore_server_native_admission_leases

  @doc false
  @spec init_table(atom()) :: :ok
  def init_table(table \\ @table) when is_atom(table) do
    case :ets.whereis(table) do
      :undefined ->
        try do
          :ets.new(table, [
            :set,
            :public,
            :named_table,
            {:read_concurrency, true},
            {:write_concurrency, :auto}
          ])

          :ok
        rescue
          ArgumentError -> :ok
        end

      _tid ->
        :ok
    end
  end

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec acquire(GenServer.server(), pid()) :: {:ok, reference()} | {:error, atom()}
  def acquire(server \\ __MODULE__, owner \\ self()) when is_pid(owner) do
    GenServer.call(server, {:acquire, owner})
  catch
    :exit, _reason -> {:error, :admission_unavailable}
  end

  @spec release(GenServer.server(), reference()) :: :ok
  def release(server \\ __MODULE__, token) when is_reference(token) do
    GenServer.call(server, {:release, token})
  catch
    :exit, _reason -> :ok
  end

  @doc false
  def count(server \\ __MODULE__), do: GenServer.call(server, :count)

  @impl true
  def init(opts) do
    table = Keyword.get(opts, :table, @table)
    :ok = init_table(table)

    max_connections =
      Keyword.get_lazy(opts, :max_connections, fn ->
        Application.get_env(:ferricstore, :maxclients, 10_000)
      end)

    if is_integer(max_connections) and max_connections > 0 do
      {leases, tokens, monitors} = restore_leases(table)

      {:ok,
       %{
         table: table,
         max_connections: max_connections,
         leases: leases,
         tokens: tokens,
         monitors: monitors
       }}
    else
      {:stop, {:invalid_max_connections, max_connections}}
    end
  end

  @impl true
  def handle_call({:acquire, owner}, _from, state) do
    case Map.get(state.leases, owner) do
      %{token: token} ->
        {:reply, {:ok, token}, state}

      nil when map_size(state.leases) >= state.max_connections ->
        {:reply, {:error, :max_connections}, state}

      nil ->
        token = make_ref()
        monitor_ref = Process.monitor(owner)
        true = :ets.insert(state.table, {owner, token})
        Stats.incr_connections()

        state = %{
          state
          | leases: Map.put(state.leases, owner, %{token: token, monitor_ref: monitor_ref}),
            tokens: Map.put(state.tokens, token, owner),
            monitors: Map.put(state.monitors, monitor_ref, owner)
        }

        {:reply, {:ok, token}, state}
    end
  end

  def handle_call({:release, token}, _from, state) do
    {:reply, :ok, release_token(state, token, true)}
  end

  def handle_call(:count, _from, state), do: {:reply, map_size(state.leases), state}

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, owner, _reason}, state) do
    case Map.get(state.monitors, monitor_ref) do
      ^owner ->
        token = state.leases |> Map.fetch!(owner) |> Map.fetch!(:token)
        {:noreply, release_token(state, token, false)}

      _stale ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.leases, fn {_owner, %{monitor_ref: monitor_ref}} ->
      Process.demonitor(monitor_ref, [:flush])
    end)

    :ok
  end

  defp release_token(state, token, demonitor?) do
    case Map.pop(state.tokens, token) do
      {nil, _tokens} ->
        state

      {owner, tokens} ->
        {%{monitor_ref: monitor_ref}, leases} = Map.pop(state.leases, owner)
        if demonitor?, do: Process.demonitor(monitor_ref, [:flush])
        true = :ets.delete(state.table, owner)
        Stats.decr_connections()

        %{
          state
          | leases: leases,
            tokens: tokens,
            monitors: Map.delete(state.monitors, monitor_ref)
        }
    end
  end

  defp restore_leases(table) do
    table
    |> :ets.tab2list()
    |> Enum.reduce({%{}, %{}, %{}}, fn
      {owner, token}, {leases, tokens, monitors} when is_pid(owner) and is_reference(token) ->
        if Process.alive?(owner) do
          monitor_ref = Process.monitor(owner)

          {
            Map.put(leases, owner, %{token: token, monitor_ref: monitor_ref}),
            Map.put(tokens, token, owner),
            Map.put(monitors, monitor_ref, owner)
          }
        else
          true = :ets.delete(table, owner)
          Stats.decr_connections()
          {leases, tokens, monitors}
        end

      invalid, acc ->
        true = :ets.delete_object(table, invalid)
        acc
    end)
  end
end
