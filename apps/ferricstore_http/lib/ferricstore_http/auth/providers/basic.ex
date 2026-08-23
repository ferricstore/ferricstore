defmodule FerricstoreHttp.Auth.Providers.Basic do
  @moduledoc """
  FerricStore ACL authentication through standard HTTP Basic credentials.

  Deployments must use TLS whenever credentials cross a network.
  """

  @behaviour FerricstoreHttp.Auth.Provider

  @impl FerricstoreHttp.Auth.Provider
  def authenticate(%{authorization: authorization, peer: peer, backend: backend}, _opts) do
    with {:ok, username, password} <- credentials(authorization) do
      case backend.authenticate(username, password, peer: peer) do
        {:ok, session} ->
          {:ok, %FerricstoreHttp.Auth.Identity{session: session, subject: username}}

        {:error, _reason} = error ->
          error
      end
    end
  end

  @impl FerricstoreHttp.Auth.Provider
  def cache_key(%{authorization: authorization}, _opts) do
    case split_authorization(authorization) do
      {"basic", encoded} when encoded != "" -> {:ok, encoded}
      _missing_or_unsupported -> :bypass
    end
  end

  defp credentials(authorization) when is_binary(authorization) do
    case split_authorization(authorization) do
      {scheme, encoded} -> decode_credentials(scheme, encoded)
      _missing_token -> {:error, :unauthenticated}
    end
  end

  defp credentials(_missing_or_unsupported), do: {:error, :unauthenticated}

  defp split_authorization(authorization) when is_binary(authorization) do
    case String.split(authorization, " ", parts: 2, trim: true) do
      [scheme, encoded] -> {String.downcase(scheme), encoded}
      _missing_token -> :error
    end
  end

  defp split_authorization(_missing), do: :error

  defp decode_credentials("basic", encoded) do
    with {:ok, decoded} <- Base.decode64(encoded),
         [username, password] <- String.split(decoded, ":", parts: 2),
         true <- username != "" do
      {:ok, username, password}
    else
      _invalid -> {:error, :unauthenticated}
    end
  end

  defp decode_credentials(_unsupported, _encoded), do: {:error, :unauthenticated}
end
