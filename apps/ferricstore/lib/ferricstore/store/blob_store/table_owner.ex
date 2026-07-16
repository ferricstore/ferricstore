defmodule Ferricstore.Store.BlobStore.TableOwner do
  @moduledoc false

  use GenServer

  alias Ferricstore.Store.ETSTableHeir

  @table_heir Ferricstore.Store.BlobStore.TableHeir
  @heir_retry_ms 10

  @tables [
    {:ferricstore_blob_store_recovery,
     [
       :named_table,
       :public,
       :set,
       {:read_concurrency, true},
       {:write_concurrency, true}
     ]},
    {:ferricstore_blob_store_dirs,
     [
       :named_table,
       :public,
       :set,
       {:read_concurrency, true},
       {:write_concurrency, :auto}
     ]},
    {:ferricstore_blob_store_segments,
     [
       :named_table,
       :public,
       :set,
       {:read_concurrency, true},
       {:write_concurrency, true}
     ]},
    {:ferricstore_blob_store_locks,
     [
       :named_table,
       :public,
       :set,
       {:read_concurrency, true},
       {:write_concurrency, :auto}
     ]},
    {:ferricstore_blob_store_protected_refs,
     [
       :named_table,
       :public,
       :set,
       {:read_concurrency, true},
       {:write_concurrency, :auto}
     ]},
    {:ferricstore_blob_store_hardened_protections,
     [
       :named_table,
       :public,
       :set,
       {:read_concurrency, true},
       {:write_concurrency, :auto}
     ]}
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec ensure_tables() :: :ok | {:error, :table_owner_unavailable}
  def ensure_tables do
    case Process.whereis(__MODULE__) do
      nil ->
        {:error, :table_owner_unavailable}

      pid ->
        GenServer.call(pid, :ensure_tables)
    end
  catch
    :exit, _reason -> {:error, :table_owner_unavailable}
  end

  @impl true
  def init(_opts) do
    heir = Process.whereis(@table_heir) || stable_heir()

    if Process.whereis(@table_heir) == heir do
      :ok = ETSTableHeir.claim_tables(@table_heir, table_names())
    end

    :ok = ensure_all_tables(heir)
    {:ok, monitor_heir(%{heir: heir, heir_monitor: nil}, heir)}
  end

  @impl true
  def handle_call(:ensure_tables, _from, %{heir: heir} = state) do
    :ok = ensure_all_tables(heir)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:"ETS-TRANSFER", _table, _from, _gift}, state), do: {:noreply, state}

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
          case rearm_tables(heir) do
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

  defp ensure_all_tables(heir) do
    Enum.each(@tables, &ensure_table(&1, heir))
    :ok
  end

  defp ensure_table({name, opts}, heir) do
    case :ets.whereis(name) do
      :undefined ->
        try do
          opts = if is_pid(heir), do: [{:heir, heir, __MODULE__} | opts], else: opts
          :ets.new(name, opts)
        rescue
          ArgumentError -> :ok
        end

      tid ->
        set_heir_if_owner(tid, heir)
        :ok
    end
  end

  defp set_heir_if_owner(table, heir) when is_pid(heir) do
    if :ets.info(table, :owner) == self() do
      :ets.setopts(table, {:heir, heir, __MODULE__})
    end
  end

  defp set_heir_if_owner(_table, _heir), do: :ok

  defp table_names, do: Enum.map(@tables, &elem(&1, 0))

  defp rearm_tables(heir) do
    ensure_all_tables(heir)
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
