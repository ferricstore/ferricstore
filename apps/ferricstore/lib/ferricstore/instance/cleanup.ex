defmodule FerricStore.Instance.Cleanup do
  @moduledoc false

  use GenServer

  @spec start_link(atom()) :: GenServer.on_start()
  def start_link(name) do
    GenServer.start_link(__MODULE__, name)
  end

  @impl true
  def init(name) do
    Process.flag(:trap_exit, true)
    {:ok, name}
  end

  @impl true
  def terminate(_reason, name) do
    FerricStore.Instance.cleanup(name)
    :ok
  end
end
