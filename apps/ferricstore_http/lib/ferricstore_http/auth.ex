defmodule FerricstoreHttp.Auth do
  @moduledoc "Authentication and request-scoped identity for the HTTP boundary."

  alias FerricstoreHttp.Auth.Cache
  alias FerricstoreHttp.{Config, Metrics}

  defmodule Context do
    @moduledoc "Authenticated context passed through the HTTP backend boundary."

    @enforce_keys [:session]
    defstruct [:session, :cache_key, :subject, scopes: []]

    @type t :: %__MODULE__{
            session: term(),
            cache_key: binary() | nil,
            subject: binary() | nil,
            scopes: [binary()]
          }
  end

  defmodule Identity do
    @moduledoc "Authenticated provider result before request-scoped context is applied."

    @enforce_keys [:session]
    defstruct [:session, :subject, scopes: []]

    @type t :: %__MODULE__{
            session: term(),
            subject: binary() | nil,
            scopes: [binary()]
          }
  end

  @context_headers ~w(x-ferricstore-subject x-ferricstore-scopes)
  @max_subject_bytes 4_096
  @max_scopes 64
  @max_scope_bytes 1_024

  @spec authenticate(binary() | nil, term(), Config.t()) ::
          {:ok, Identity.t()} | {:error, term()}
  def authenticate(authorization, peer, %Config{} = config) do
    authenticate(authorization, peer, %{}, config)
  end

  @spec authenticate(binary() | nil, term(), map(), Config.t()) ::
          {:ok, Identity.t()} | {:error, term()}
  def authenticate(authorization, peer, headers, %Config{} = config) when is_map(headers) do
    request = request(authorization, peer, headers, config)

    with {:ok, authenticated} <- config.auth_provider.authenticate(request, config.auth_options),
         {:ok, identity} <- normalize_identity(authenticated) do
      {:ok, apply_trusted_headers(identity, headers, config)}
    end
  end

  @spec resolve(binary() | nil, term(), Config.t()) ::
          {:ok, Context.t()} | {:error, term()}
  def resolve(authorization, peer, %Config{} = config) do
    resolve(authorization, peer, %{}, config)
  end

  @spec resolve(binary() | nil, term(), map(), Config.t()) ::
          {:ok, Context.t()} | {:error, term()}
  def resolve(authorization, peer, headers, %Config{} = config) when is_map(headers) do
    request = request(authorization, peer, headers, config)

    if cacheable?(config) do
      resolve_cached(request, peer, headers, config)
    else
      authenticate_direct(request, headers, config, :bypass)
    end
  end

  @spec invalidate(Context.t()) :: :ok
  def invalidate(%Context{cache_key: nil}), do: :ok

  def invalidate(%Context{cache_key: cache_key, session: session}) do
    Cache.invalidate(cache_key, session)
  end

  defp resolve_cached(request, peer, headers, config) do
    case config.auth_provider.cache_key(request, config.auth_options) do
      {:ok, material} ->
        cache_key = Cache.reference(config.auth_provider, config.backend, {material, peer})

        authenticate = fn -> authenticate_provider(request, config) end

        case Cache.fetch(cache_key, peer, authenticate, config.request_timeout_ms) do
          {:ok, identity, source} ->
            observe_cache(source, config)
            {:ok, context(identity, cache_key, headers, config)}

          {:error, reason, source} ->
            observe_cache(source, config)
            {:error, reason}
        end

      :bypass ->
        authenticate_direct(request, headers, config, :bypass)

      _invalid ->
        authenticate_direct(request, headers, config, :bypass)
    end
  rescue
    _cache_unavailable -> {:error, :authentication_unavailable}
  end

  defp authenticate_direct(request, headers, config, source) do
    observe_cache(source, config)

    with {:ok, authenticated} <- config.auth_provider.authenticate(request, config.auth_options),
         {:ok, identity} <- normalize_identity(authenticated) do
      {:ok, context(identity, nil, headers, config)}
    end
  end

  defp authenticate_provider(request, config) do
    with {:ok, authenticated} <-
           config.auth_provider.authenticate(request, config.auth_options) do
      normalize_identity(authenticated)
    end
  end

  defp request(authorization, peer, headers, config) do
    %{
      authorization: authorization,
      peer: peer,
      backend: config.backend,
      headers: Map.take(headers, @context_headers)
    }
  end

  defp normalize_identity(%Identity{} = identity), do: validate_identity(identity)

  defp normalize_identity(session), do: {:ok, %Identity{session: session}}

  defp validate_identity(%Identity{} = identity) do
    with :ok <- optional_bounded(identity.subject, @max_subject_bytes),
         :ok <- scopes(identity.scopes) do
      {:ok, identity}
    else
      _invalid -> {:error, :unauthenticated}
    end
  end

  defp context(%Identity{} = identity, cache_key, headers, config) do
    identity = apply_trusted_headers(identity, headers, config)

    %Context{
      session: identity.session,
      cache_key: cache_key,
      subject: identity.subject,
      scopes: identity.scopes
    }
  end

  defp apply_trusted_headers(identity, _headers, %Config{trust_context_headers: false}),
    do: identity

  defp apply_trusted_headers(%Identity{} = identity, headers, %Config{
         trust_context_headers: true
       }) do
    %Identity{
      identity
      | subject:
          trusted_identity(headers["x-ferricstore-subject"], identity.subject, @max_subject_bytes),
        scopes: trusted_scopes(headers["x-ferricstore-scopes"], identity.scopes)
    }
  end

  defp trusted_identity(value, _fallback, max_bytes)
       when is_binary(value) and value != "" and byte_size(value) <= max_bytes,
       do: value

  defp trusted_identity(_value, fallback, _max_bytes), do: fallback

  defp trusted_scopes(value, fallback) when is_binary(value) do
    parsed = value |> String.split([",", " "], trim: true) |> Enum.uniq()
    if scopes(parsed) == :ok, do: parsed, else: fallback
  end

  defp trusted_scopes(_value, fallback), do: fallback

  defp optional_bounded(nil, _max_bytes), do: :ok

  defp optional_bounded(value, max_bytes)
       when is_binary(value) and value != "" and byte_size(value) <= max_bytes,
       do: :ok

  defp optional_bounded(_value, _max_bytes), do: :error

  defp scopes(values) when is_list(values) and length(values) <= @max_scopes do
    if Enum.all?(values, &(is_binary(&1) and &1 != "" and byte_size(&1) <= @max_scope_bytes)),
      do: :ok,
      else: :error
  end

  defp scopes(_values), do: :error

  defp cacheable?(%Config{auth_cache_enabled: true, auth_provider: provider}) do
    function_exported?(provider, :cache_key, 2)
  end

  defp cacheable?(%Config{}), do: false

  defp observe_cache(source, %Config{metrics_enabled: true}),
    do: Metrics.observe_auth_cache(source)

  defp observe_cache(_source, %Config{metrics_enabled: false}), do: :ok
end
