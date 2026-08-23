defmodule FerricstoreHttp.Auth.Provider do
  @moduledoc """
  Authentication-provider contract for HTTP requests.

  Providers receive normalized request metadata rather than Cowboy request
  objects. This keeps authentication independent of the HTTP implementation.
  """

  @type request :: %{
          required(:authorization) => binary() | nil,
          required(:peer) => term(),
          required(:backend) => module(),
          required(:headers) => %{optional(binary()) => binary() | nil}
        }

  @callback authenticate(request(), keyword()) ::
              {:ok, term() | FerricstoreHttp.Auth.Identity.t()} | {:error, term()}

  @doc """
  Returns credential material suitable for a process-secret cache fingerprint.

  Providers must opt in explicitly. The material is HMACed immediately and is
  never retained. Providers whose sessions cannot safely be revalidated should
  return `:bypass` or omit this callback.
  """
  @callback cache_key(request(), keyword()) :: {:ok, term()} | :bypass

  @optional_callbacks cache_key: 2
end
