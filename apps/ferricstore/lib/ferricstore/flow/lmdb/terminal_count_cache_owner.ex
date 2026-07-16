defmodule Ferricstore.Flow.LMDB.TerminalCountCacheOwner do
  @moduledoc false

  use GenServer

  @table :ferricstore_flow_lmdb_terminal_count_cache
  @table_options [
    :named_table,
    :public,
    :set,
    read_concurrency: true,
    write_concurrency: true
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @spec ensure_table() :: :ok | {:error, term()}
  def ensure_table do
    case Process.whereis(__MODULE__) do
      owner when is_pid(owner) -> GenServer.call(owner, :ensure_table)
      nil -> {:error, :not_started}
    end
  catch
    :exit, _reason -> {:error, :not_started}
  end

  @doc false
  def table_name, do: @table

  @impl true
  def init(_opts) do
    case ensure_owned_table() do
      :ok -> {:ok, %{}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:ensure_table, _from, state) do
    {:reply, ensure_owned_table(), state}
  end

  defp ensure_owned_table do
    case :ets.whereis(@table) do
      :undefined ->
        _table = :ets.new(@table, @table_options)
        :ok

      table ->
        if :ets.info(table, :owner) == self() do
          :ok
        else
          {:error, {:terminal_count_cache_owned_by, :ets.info(table, :owner)}}
        end
    end
  end
end
