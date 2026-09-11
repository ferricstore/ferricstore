defmodule FerricstoreHttp.RecordingFerricstoreBackend do
  @moduledoc false

  use Agent

  @behaviour FerricstoreHttp.Backend

  alias FerricstoreHttp.Backends.Ferricstore

  def start_link(_opts), do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)
  def reset, do: Agent.update(__MODULE__, fn _commands -> MapSet.new() end)
  def commands, do: Agent.get(__MODULE__, & &1)

  @impl FerricstoreHttp.Backend
  def authenticate(username, password, opts),
    do: Ferricstore.authenticate(username, password, opts)

  @impl FerricstoreHttp.Backend
  def execute_batch(session, commands, opts) do
    result = Ferricstore.execute_batch(session, commands, opts)
    if match?({:ok, _results}, result), do: record(commands)
    result
  end

  @impl FerricstoreHttp.Backend
  def prepare_batch(commands, opts), do: Ferricstore.prepare_batch(commands, opts)

  @impl FerricstoreHttp.Backend
  def execute_prepared_batches(session, batches, opts) do
    Ferricstore.execute_prepared_batches(session, batches, opts)
  end

  @impl FerricstoreHttp.Backend
  def prepared_batching_supported?, do: true

  @impl FerricstoreHttp.Backend
  def ready?, do: Ferricstore.ready?()

  defp record(commands) do
    names = MapSet.new(commands, &command_name/1)
    Agent.update(__MODULE__, &MapSet.union(&1, names))
  end

  defp command_name([name | _args]) when is_binary(name), do: String.upcase(name)
  defp command_name(%{"command" => name}) when is_binary(name), do: String.upcase(name)
  defp command_name(_invalid), do: ""
end
