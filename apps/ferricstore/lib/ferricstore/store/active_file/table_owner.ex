defmodule Ferricstore.Store.ActiveFile.TableOwner do
  @moduledoc false

  use GenServer

  alias Ferricstore.Store.ETSTableHeir

  @table :ferricstore_active_files
  @table_heir Ferricstore.Store.ActiveFile.TableHeir
  @heir_retry_ms 10

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec ensure_table() :: :ok | {:error, :table_owner_unavailable}
  def ensure_table do
    case Process.whereis(__MODULE__) do
      nil -> {:error, :table_owner_unavailable}
      pid -> GenServer.call(pid, :ensure_table)
    end
  catch
    :exit, _reason -> {:error, :table_owner_unavailable}
  end

  @impl true
  def init(_opts) do
    heir = Process.whereis(@table_heir) || stable_heir()

    if Process.whereis(@table_heir) == heir do
      :ok = ETSTableHeir.claim_tables(@table_heir, [@table])
    end

    :ok = ensure_table(heir)
    {:ok, monitor_heir(%{heir: heir, heir_monitor: nil}, heir)}
  end

  @impl true
  def handle_call(:ensure_table, _from, %{heir: heir} = state) do
    {:reply, ensure_table(heir), state}
  end

  @impl true
  def handle_info({:"ETS-TRANSFER", @table, _from, _gift}, state), do: {:noreply, state}

  def handle_info(
        {:DOWN, monitor, :process, heir, _reason},
        %{heir_monitor: monitor, heir: heir} = state
      ) do
    schedule_heir_rearm()
    {:noreply, %{state | heir: nil, heir_monitor: nil}}
  end

  def handle_info(:rearm_table_heir, state) do
    case Process.whereis(@table_heir) do
      heir when is_pid(heir) ->
        if Process.alive?(heir) do
          case rearm_table(heir) do
            :ok -> {:noreply, monitor_heir(state, heir)}
            :retry -> schedule_rearm_and_continue(state)
          end
        else
          schedule_rearm_and_continue(state)
        end

      _missing ->
        schedule_rearm_and_continue(state)
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp ensure_table(heir) do
    case :ets.whereis(@table) do
      :undefined ->
        options = [
          :set,
          :public,
          :named_table,
          read_concurrency: true,
          write_concurrency: true
        ]

        options = if is_pid(heir), do: [{:heir, heir, __MODULE__} | options], else: options

        try do
          :ets.new(@table, options)
          :ok
        rescue
          ArgumentError -> :ok
        end

      table ->
        set_heir_if_owner(table, heir)
        :ok
    end
  end

  defp set_heir_if_owner(table, heir) when is_pid(heir) do
    if :ets.info(table, :owner) == self() do
      :ets.setopts(table, {:heir, heir, __MODULE__})
    end
  end

  defp set_heir_if_owner(_table, _heir), do: :ok

  defp rearm_table(heir) do
    ensure_table(heir)
  rescue
    ArgumentError -> :retry
  end

  defp monitor_heir(%{heir_monitor: monitor} = state, heir) when is_pid(heir) do
    if is_reference(monitor), do: Process.demonitor(monitor, [:flush])
    %{state | heir: heir, heir_monitor: Process.monitor(heir)}
  end

  defp monitor_heir(state, _heir), do: state

  defp schedule_rearm_and_continue(state) do
    schedule_heir_rearm()
    {:noreply, state}
  end

  defp schedule_heir_rearm do
    Process.send_after(self(), :rearm_table_heir, @heir_retry_ms)
  end

  defp stable_heir do
    Process.get(:"$ancestors", [])
    |> Enum.find_value(fn
      pid when is_pid(pid) -> pid
      name when is_atom(name) -> Process.whereis(name)
      _other -> nil
    end)
  end
end
