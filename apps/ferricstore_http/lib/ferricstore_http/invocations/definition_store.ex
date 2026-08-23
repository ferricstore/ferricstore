defmodule FerricstoreHttp.Invocations.DefinitionStore do
  @moduledoc "Durable invocation definition access through the in-process backend."

  alias FerricstoreHttp.{Auth, Config}
  alias FerricstoreHttp.Invocations.{Backend, Definition}

  @spec get(Auth.Context.t(), binary(), Config.t(), keyword()) ::
          {:ok, Definition.t()} | {:error, term()}
  def get(context, name, config, opts \\ [])

  def get(_context, name, _config, _opts) when not is_binary(name),
    do: {:error, :invalid_invocation_name}

  def get(context, name, config, opts) do
    case Backend.definition_get(context, name, config, opts) do
      {:ok, nil} -> {:error, :definition_not_found}
      {:ok, %{} = attrs} -> Definition.new(attrs)
      {:error, _reason} = error -> error
      _invalid -> {:error, :malformed_definition}
    end
  end

  @spec list(Auth.Context.t(), Config.t(), keyword()) ::
          {:ok, [Definition.t()]} | {:error, term()}
  def list(context, config, opts \\ []) do
    with {:ok, definitions} <- Backend.definition_list(context, config, opts) do
      parse_definitions(definitions)
    end
  end

  @spec put(Auth.Context.t(), Definition.t() | map(), Config.t(), keyword()) ::
          :ok | {:error, term()}
  def put(context, definition, config, opts \\ [])

  def put(context, %Definition{} = definition, config, opts) do
    Backend.definition_put(context, Definition.to_map(definition), config, opts)
  end

  def put(context, %{} = attrs, config, opts) do
    with {:ok, definition} <- Definition.new(attrs),
         do: put(context, definition, config, opts)
  end

  defp parse_definitions(definitions) when is_list(definitions) do
    Enum.reduce_while(definitions, {:ok, []}, fn attrs, {:ok, acc} ->
      case Definition.new(attrs) do
        {:ok, definition} -> {:cont, {:ok, [definition | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, parsed} -> {:ok, Enum.reverse(parsed)}
      {:error, _reason} = error -> error
    end
  end

  defp parse_definitions(_definitions), do: {:error, :malformed_definition_list}
end
