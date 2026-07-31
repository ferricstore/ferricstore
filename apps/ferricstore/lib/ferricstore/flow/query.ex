defmodule Ferricstore.Flow.Query do
  @moduledoc """
  Versioned Flow query entry point for embedded/reference execution.

  Native server traffic uses the Rust parser and then calls `execute/2` with
  the same canonical request. The Elixir parser remains an independent oracle.
  """

  alias Ferricstore.Flow.Query.{Binder, Error, ReferenceParser, Request, Surface}

  @spec execute_reference(FerricStore.Instance.t() | map(), binary(), binary(), map()) ::
          {:ok, term()} | {:error, term()}
  def execute_reference(ctx, version, query, params) do
    with {:ok, bound} <- prepare_reference(version, query, params) do
      execute(ctx, bound)
    end
  end

  @doc false
  @spec prepare_reference(binary(), binary(), map()) :: {:ok, Request.t()} | {:error, atom()}
  def prepare_reference(version, query, params) do
    with :ok <- validate_version(version),
         {:ok, request} <- ReferenceParser.parse(query),
         {:ok, bound} <- Binder.bind(request, params) do
      {:ok, bound}
    end
  end

  @doc false
  @spec prepare_text(binary(), binary(), map(), module()) ::
          {:ok, Request.t()} | {:error, atom()}
  def prepare_text(version, query, params, parser \\ ReferenceParser) when is_atom(parser) do
    with :ok <- validate_version(version),
         {:ok, request} <- parser.parse(query),
         {:ok, bound} <- Binder.bind_text(request, params) do
      {:ok, bound}
    end
  end

  @doc false
  @spec prepare_text_diagnostic(binary(), binary(), map(), module()) ::
          {:ok, Request.t()} | {:error, Error.t()}
  def prepare_text_diagnostic(version, query, params, parser \\ ReferenceParser)
      when is_atom(parser) do
    with :ok <- validate_version(version),
         {:ok, request} <- parse_diagnostic(parser, query),
         {:ok, bound} <- bind_text_diagnostic(request, params) do
      {:ok, bound}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} when is_atom(reason) -> {:error, Error.new(reason)}
    end
  end

  @doc false
  @spec partition_key(Request.t()) :: {:ok, binary()} | {:error, :unsupported_query_shape}
  def partition_key(%Request{predicate: {:and, predicates}}) when is_list(predicates) do
    case Enum.find(predicates, &match?({:eq, :partition_key, _value}, &1)) do
      {:eq, :partition_key, {:literal, :keyword, value}}
      when is_binary(value) and value != "" ->
        {:ok, value}

      nil ->
        auto_partition_key(predicates)

      _invalid ->
        {:error, :unsupported_query_shape}
    end
  end

  def partition_key(%Request{}), do: {:error, :unsupported_query_shape}

  defp auto_partition_key([{:eq, :run_id, {:literal, :keyword, id}}])
       when is_binary(id) and id != "",
       do: {:ok, Ferricstore.Flow.Keys.auto_partition_key(id)}

  defp auto_partition_key(_predicates), do: {:error, :unsupported_query_shape}

  @spec execute(FerricStore.Instance.t() | map(), Request.t()) ::
          {:ok, term()} | {:error, term()}
  def execute(ctx, %Request{} = request) do
    with :ok <- Request.validate_bound(request) do
      FerricStore.Flow.QueryEngine.execute(ctx, request)
    end
  end

  @spec validate_version(term()) :: :ok | {:error, :unsupported_query_version}
  def validate_version(version) do
    if Surface.supported_version?(version),
      do: :ok,
      else: {:error, :unsupported_query_version}
  end

  defp parse_diagnostic(parser, query) do
    if Code.ensure_loaded?(parser) and function_exported?(parser, :parse_diagnostic, 1) do
      parser.parse_diagnostic(query)
    else
      case parser.parse(query) do
        {:error, reason} when is_atom(reason) -> {:error, Error.diagnose(reason, query)}
        result -> result
      end
    end
  end

  defp bind_text_diagnostic(%Request{} = request, params) do
    case Binder.bind_text(request, params) do
      {:error, :missing_parameter} ->
        missing_names = Binder.missing_parameter_names(request, params)
        {:error, missing_parameter_diagnostic(missing_names)}

      result ->
        result
    end
  end

  defp missing_parameter_diagnostic([name]) do
    Error.new(:missing_parameter,
      detail: "Missing named parameter: @#{name}.",
      hint: "Add #{name} to the parameters object.",
      context: %{"missing_parameters" => [name]}
    )
  end

  defp missing_parameter_diagnostic([first | _rest] = names) do
    Error.new(:missing_parameter,
      detail: "Missing #{length(names)} named parameters; first: @#{first}.",
      hint: "Add the missing names to the parameters object.",
      context: %{
        "missing_parameter_count" => length(names),
        "missing_parameters" => Enum.take(names, 16)
      }
    )
  end

  defp missing_parameter_diagnostic([]), do: Error.new(:missing_parameter)

  @spec error_message(atom()) :: binary()
  defdelegate error_message(reason), to: Error, as: :message
end
