defmodule Ferricstore.Flow.HistoryProjector.TableOwner do
  @moduledoc false

  use GenServer

  @tables [
    {:ferricstore_flow_history_projector_pending_registry,
     [:named_table, :public, :set, {:read_concurrency, true}, {:write_concurrency, true}]},
    {:ferricstore_flow_history_projector_replay_reservations,
     [:named_table, :public, :set, {:read_concurrency, true}, {:write_concurrency, true}]},
    {:ferricstore_flow_history_projector_overflow,
     [:named_table, :public, :ordered_set, {:read_concurrency, true}, {:write_concurrency, true}]}
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @spec ensure_tables() :: :ok
  def ensure_tables do
    case Process.whereis(__MODULE__) do
      nil -> start_unlinked()
      _pid -> GenServer.call(__MODULE__, :ensure_tables)
    end
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init(_opts) do
    ensure_all_tables()
    {:ok, %{}}
  end

  @impl true
  def handle_call(:ensure_tables, _from, state) do
    ensure_all_tables()
    {:reply, :ok, state}
  end

  defp start_unlinked do
    case GenServer.start(__MODULE__, [], name: __MODULE__) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> GenServer.call(__MODULE__, :ensure_tables)
      {:error, _reason} -> :ok
    end
  end

  defp ensure_all_tables do
    Enum.each(@tables, fn {name, opts} ->
      case :ets.whereis(name) do
        :undefined ->
          try do
            :ets.new(name, opts)
          rescue
            ArgumentError -> :ok
          end

        _tid ->
          :ok
      end
    end)

    :ok
  end
end
