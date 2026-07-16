defmodule Ferricstore.Store.ETSTableHeir do
  @moduledoc false

  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, name: Keyword.fetch!(opts, :name))
  end

  @spec claim_tables(GenServer.server(), [atom()]) :: :ok
  def claim_tables(heir, table_names) when is_list(table_names) do
    GenServer.call(heir, {:claim_tables, self(), table_names})
  end

  @impl true
  def init(:ok), do: {:ok, %{}}

  @impl true
  def handle_call({:claim_tables, owner, table_names}, {owner, _tag}, state)
      when is_pid(owner) do
    Enum.each(table_names, fn table_name ->
      case :ets.whereis(table_name) do
        :undefined ->
          :ok

        table ->
          if :ets.info(table, :owner) == self() do
            :ets.give_away(table, owner, {__MODULE__, table_name})
          end
      end
    end)

    {:reply, :ok, state}
  end

  def handle_call({:claim_tables, _owner, _table_names}, _from, state) do
    {:reply, {:error, :owner_mismatch}, state}
  end

  @impl true
  def handle_info({:"ETS-TRANSFER", _table, _from, _gift}, state), do: {:noreply, state}
end
