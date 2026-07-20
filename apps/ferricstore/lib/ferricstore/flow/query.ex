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

  @spec error_message(atom()) :: binary()
  defdelegate error_message(reason), to: Error, as: :message
end
