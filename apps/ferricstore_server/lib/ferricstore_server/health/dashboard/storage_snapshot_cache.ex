defmodule FerricstoreServer.Health.Dashboard.StorageSnapshotCache do
  @moduledoc false

  use GenServer

  @table __MODULE__
  @entry :snapshot

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def lookup do
    case :ets.lookup(@table, @entry) do
      [{@entry, identity, cached_at_ms, snapshot}] ->
        {:ok, identity, cached_at_ms, snapshot}

      [] ->
        :miss
    end
  rescue
    ArgumentError -> :miss
  end

  def put(identity, cached_at_ms, snapshot) do
    GenServer.call(__MODULE__, {:put, identity, cached_at_ms, snapshot})
  catch
    :exit, _reason -> :unavailable
  end

  @impl true
  def init(:ok) do
    table =
      :ets.new(@table, [
        :named_table,
        :set,
        :protected,
        read_concurrency: true
      ])

    {:ok, table}
  end

  @impl true
  def handle_call({:put, identity, cached_at_ms, snapshot}, _from, table) do
    true = :ets.insert(table, {@entry, identity, cached_at_ms, snapshot})
    {:reply, :ok, table}
  end
end
