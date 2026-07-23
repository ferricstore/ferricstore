defmodule FerricstoreServer.Native.FlowQuery do
  @moduledoc false

  alias Ferricstore.Flow.Query
  alias Ferricstore.Flow.Query.{Binder, Error, Request}
  alias FerricstoreServer.Native.FQLParser

  @spec execute(FerricStore.Instance.t() | map(), binary(), binary(), map()) ::
          {:ok, term()} | {:error, term()}
  def execute(ctx, version, query, params) do
    with {:ok, bound} <- prepare(version, query, params) do
      Query.execute(ctx, bound)
    end
  end

  @spec prepare(binary(), binary(), map()) :: {:ok, Request.t()} | {:error, Error.t()}
  def prepare(version, query, params) do
    with :ok <- Query.validate_version(version),
         {:ok, request} <- FQLParser.parse_diagnostic(query),
         {:ok, bound} <- Binder.bind(request, params) do
      {:ok, bound}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} when is_atom(reason) -> {:error, Error.new(reason)}
    end
  end

  @spec execute_prepared(FerricStore.Instance.t() | map(), Request.t()) ::
          {:ok, term()} | {:error, term()}
  def execute_prepared(ctx, %Request{} = request), do: Query.execute(ctx, request)
end
