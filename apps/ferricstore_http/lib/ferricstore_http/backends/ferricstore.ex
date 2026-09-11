defmodule FerricstoreHttp.Backends.Ferricstore do
  @moduledoc """
  Production backend that calls FerricStore's public transport gateways.
  """

  @behaviour FerricstoreHttp.Backend

  alias FerricstoreServer.Acl.CatalogProjector
  alias FerricstoreServer.{AuthenticationGateway, CommandGateway}

  @impl FerricstoreHttp.Backend
  def authenticate(username, password, opts) do
    AuthenticationGateway.authenticate(username, password, opts)
  end

  @impl FerricstoreHttp.Backend
  def execute_batch(session, commands, opts) do
    with {:ok, commands} <- normalize_commands(commands) do
      CommandGateway.execute_batch(session, commands, execution_opts(opts))
    end
  end

  @impl FerricstoreHttp.Backend
  def prepare_batch(commands, opts) do
    with {:ok, commands} <- normalize_commands(commands) do
      CommandGateway.prepare_batch(commands, opts)
    end
  end

  @impl FerricstoreHttp.Backend
  def execute_prepared_batches(session, batches, opts) do
    CommandGateway.execute_prepared_batches(session, batches, opts)
  end

  @impl FerricstoreHttp.Backend
  def prepared_batching_supported?, do: true

  @impl FerricstoreHttp.Backend
  def ready?, do: CatalogProjector.ready?()

  defp normalize_commands(commands) when is_list(commands) do
    commands
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {command, index}, {:ok, normalized} ->
      case normalize_command(command) do
        {:ok, command} -> {:cont, {:ok, [command | normalized]}}
        :error -> {:halt, {:error, {:malformed_command, index}}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_commands(_commands), do: {:error, {:invalid_batch, "commands must be a list"}}

  defp normalize_command(command) when is_list(command), do: {:ok, command}

  defp normalize_command(
         %{"command" => command, "opcode" => opcode, "payload" => payload} = descriptor
       )
       when map_size(descriptor) == 3 do
    case CommandGateway.native_command(command, opcode, payload) do
      {:ok, native_command} -> {:ok, native_command}
      {:error, :invalid_native_command} -> :error
    end
  end

  defp normalize_command(_command), do: :error

  defp execution_opts(opts) do
    case Keyword.pop(opts, :request_context) do
      {%{} = context, opts} when map_size(context) > 0 ->
        store = Keyword.get_lazy(opts, :store, fn -> FerricStore.Instance.get(:default) end)
        Keyword.put(opts, :store, Map.put(store, :request_context, context))

      {_empty_or_missing, opts} ->
        opts
    end
  end
end
