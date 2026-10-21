defmodule FerricstoreHttp.Backend do
  @moduledoc """
  Boundary between the HTTP protocol and FerricStore execution.

  Implementations return opaque sessions. HTTP modules must never inspect or
  cache credential material inside those sessions.
  """

  @type session :: term()
  @type command :: [term()] | map()
  @type result :: %{required(:status) => atom(), required(:value) => term()}

  @callback authenticate(binary(), binary(), keyword()) ::
              {:ok, session()} | {:error, term()}
  @callback execute_batch(session(), [command()], keyword()) ::
              {:ok, [result()]} | {:error, term()}
  @callback prepare_batch([command()], keyword()) :: {:ok, term()} | {:error, term()}
  @callback execute_prepared_batches(session(), [term()], keyword()) ::
              {:ok, [[result()]]} | {:error, term()}
  @callback prepared_batching_supported?() :: boolean()
  @callback ready?() :: boolean()

  @doc """
  Returns `session` attributed to another connection from the same address.

  Backends that implement it let connections presenting the same credentials
  from one address share one credential check; each still gets a session
  carrying its own peer.
  """
  @callback bind_peer(session(), term()) :: session()

  @optional_callbacks prepare_batch: 2,
                      execute_prepared_batches: 3,
                      prepared_batching_supported?: 0,
                      bind_peer: 2
end
