defmodule Ferricstore.SharedTables do
  @moduledoc false

  # Creates node-wide named ETS tables on behalf of the processes that use
  # them. A table belongs to the process that creates it and goes when that
  # process exits, so a table first used by a short-lived client or worker
  # would take every other user's rows with it. Created here, it lasts as
  # long as the application. Without the application running, the caller
  # creates the table itself.

  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Makes sure the named table `name` exists, creating it with `options` if not."
  @spec ensure(atom(), list()) :: :ok
  def ensure(name, options) when is_atom(name) and is_list(options) do
    case :ets.whereis(name) do
      :undefined -> create(name, options)
      _table -> :ok
    end
  end

  defp create(name, options) do
    case Process.whereis(__MODULE__) do
      owner when is_pid(owner) and owner != self() ->
        GenServer.call(owner, {:ensure, name, options})

      _here_or_missing ->
        create_here(name, options)
    end
  catch
    :exit, _reason -> create_here(name, options)
  end

  defp create_here(name, options) do
    case :ets.whereis(name) do
      :undefined ->
        :ets.new(name, options)
        :ok

      _table ->
        :ok
    end
  rescue
    # Another process created it first.
    ArgumentError -> :ok
  end

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:ensure, name, options}, _from, state) do
    {:reply, create_here(name, options), state}
  end
end
