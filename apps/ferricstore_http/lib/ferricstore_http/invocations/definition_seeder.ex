defmodule FerricstoreHttp.Invocations.DefinitionSeeder do
  @moduledoc false

  use GenServer

  alias FerricstoreHttp.Config
  alias FerricstoreHttp.Invocations.{Definition, DefinitionStore, SystemSession}

  @spec start_link(Config.t()) :: GenServer.on_start()
  def start_link(%Config{} = config), do: GenServer.start_link(__MODULE__, config)

  @impl GenServer
  def init(%Config{invocation_definitions_file: file} = config) do
    with {:ok, definitions} <- read_definitions(file),
         {:ok, definitions} <- validate_definitions(definitions),
         {:ok, context} <- SystemSession.context(),
         :ok <- put_definitions(definitions, context, config) do
      {:ok, %{count: length(definitions)}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  defp read_definitions(file) do
    with {:ok, json} <- File.read(file),
         {:ok, decoded} <- Jason.decode(json) do
      definitions_from(decoded)
    end
  end

  defp definitions_from(%{"definitions" => definitions}) when is_list(definitions),
    do: {:ok, definitions}

  defp definitions_from(definitions) when is_list(definitions), do: {:ok, definitions}

  defp definitions_from(%{} = definitions_by_name) do
    Enum.reduce_while(definitions_by_name, {:ok, []}, fn
      {name, %{} = definition}, {:ok, definitions} ->
        {:cont, {:ok, [Map.put_new(definition, "name", name) | definitions]}}

      {_name, _invalid}, _definitions ->
        {:halt, {:error, :invalid_invocation_definitions_file}}
    end)
    |> case do
      {:ok, definitions} -> {:ok, Enum.reverse(definitions)}
      {:error, _reason} = error -> error
    end
  end

  defp definitions_from(_invalid), do: {:error, :invalid_invocation_definitions_file}

  defp validate_definitions(definitions) do
    Enum.reduce_while(definitions, {:ok, []}, fn attrs, {:ok, validated} ->
      case Definition.new(attrs) do
        {:ok, definition} -> {:cont, {:ok, [definition | validated]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, validated} -> {:ok, Enum.reverse(validated)}
      {:error, _reason} = error -> error
    end
  end

  defp put_definitions(definitions, context, config) do
    Enum.reduce_while(definitions, :ok, fn definition, :ok ->
      case DefinitionStore.put(context, definition, config) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end
end
