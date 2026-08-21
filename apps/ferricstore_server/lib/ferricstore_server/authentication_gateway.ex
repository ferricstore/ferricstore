defmodule FerricstoreServer.AuthenticationGateway do
  @moduledoc """
  Transport-neutral authentication boundary for trusted protocol applications.

  External protocol applications can authenticate FerricStore ACL credentials
  without opening a native TCP connection. Successful authentication returns an
  opaque session that contains no plaintext credential. Consumers must pass that
  session to `FerricstoreServer.CommandGateway`; the command gateway revalidates
  its credential generation before executing each batch.

  This module does not parse HTTP headers or own an authentication cache. Those
  concerns belong to the protocol application.
  """

  alias Ferricstore.{AuditLog, Config}
  alias FerricstoreServer.{Acl, AuthRateLimiter}
  alias FerricstoreServer.Acl.CatalogProjector
  alias FerricstoreServer.Connection.Auth, as: ConnectionAuth

  defmodule Session do
    @moduledoc """
    Opaque authenticated identity accepted by `FerricstoreServer.CommandGateway`.

    Sessions contain an ACL authorization snapshot and credential generation,
    but never the submitted password.
    """

    @derive {Inspect, only: [:username, :auth_epoch]}
    @enforce_keys [
      :username,
      :auth_epoch,
      :acl_cache,
      :credential_source,
      :peer,
      :authenticated_at_ms
    ]
    defstruct @enforce_keys

    @opaque t :: %__MODULE__{
              username: binary(),
              auth_epoch: non_neg_integer(),
              acl_cache: map() | :full_access,
              credential_source: :acl | {:requirepass, binary()},
              peer: term(),
              authenticated_at_ms: integer()
            }
  end

  @type error ::
          :unauthenticated
          | :authentication_unavailable
          | {:invalid_credentials, binary()}
          | {:rate_limited, pos_integer()}

  @doc """
  Authenticates an OSS ACL or legacy `requirepass` credential.

  `:peer` should identify the original caller and is used by the shared
  authentication rate limiter and audit log.
  """
  @spec authenticate(binary(), binary(), keyword()) :: {:ok, Session.t()} | {:error, error()}
  def authenticate(username, password, opts \\ [])

  def authenticate(username, password, opts)
      when is_binary(username) and is_binary(password) and is_list(opts) do
    peer = Keyword.get(opts, :peer)

    with :ok <- projection_ready(),
         {:ok, reservation} <- reserve_authentication(peer, username, password) do
      case verify_and_open_session(username, password, peer) do
        {:ok, %Session{}} = authenticated ->
          :ok = AuthRateLimiter.release_success(reservation)
          audit(:auth_success, username, peer, false)
          authenticated

        {:error, _reason} = error ->
          audit(:auth_failure, username, peer, false)
          error
      end
    else
      {:error, {:rate_limited, retry_after_ms}} ->
        audit(:auth_failure, username, peer, true)
        {:error, {:rate_limited, retry_after_ms}}

      {:error, reason} when is_binary(reason) ->
        {:error, {:invalid_credentials, reason}}

      {:error, _reason} = error ->
        error
    end
  end

  def authenticate(_username, _password, _opts),
    do: {:error, {:invalid_credentials, "authentication credentials must be binaries"}}

  @doc """
  Revalidates a session against the current ACL credential generation.

  Authorization caches are rebuilt after validation so an accepted batch uses
  the current command, key, and channel rules.
  """
  @spec validate(Session.t()) :: {:ok, Session.t()} | {:error, :reauthentication_required}
  def validate(%Session{} = session) do
    with true <- CatalogProjector.ready?(),
         %{enabled: true, auth_epoch: auth_epoch} = user <- Acl.get_user(session.username),
         true <- auth_epoch == session.auth_epoch,
         true <- Acl.credential_active?(user),
         true <- credential_source_current?(session.credential_source),
         acl_cache when acl_cache != :denied <- ConnectionAuth.build_acl_cache(session.username) do
      {:ok, %{session | acl_cache: acl_cache}}
    else
      _stale_disabled_or_unavailable -> {:error, :reauthentication_required}
    end
  end

  def validate(_invalid), do: {:error, :reauthentication_required}

  defp verify_and_open_session(username, password, peer) do
    user = Acl.get_user(username)
    auth_epoch = Map.get(user || %{}, :auth_epoch)

    if acl_password_configured?(user) do
      authenticate_acl(username, password, auth_epoch, peer)
    else
      verify_legacy_or_passwordless(username, password, user, auth_epoch, peer)
    end
  end

  defp verify_legacy_or_passwordless(username, password, user, auth_epoch, peer) do
    requirepass = requirepass()

    cond do
      username == "default" and secure_equal?(password, requirepass) ->
        open_session(
          username,
          auth_epoch,
          {:requirepass, credential_fingerprint(requirepass)},
          peer
        )

      username == "default" and configured_secret?(requirepass) ->
        {:error, :unauthenticated}

      match?(%{enabled: true, password: nil}, user) and not configured_secret?(requirepass) ->
        hide_passwordless_account(username, password)

      true ->
        authenticate_acl(username, password, auth_epoch, peer)
    end
  end

  defp acl_password_configured?(%{password: stored}), do: is_binary(stored)
  defp acl_password_configured?(_user), do: false

  defp authenticate_acl(username, password, auth_epoch, peer) do
    case Acl.authenticate(username, password) do
      {:ok, ^username} -> open_session(username, auth_epoch, :acl, peer)
      {:error, _reason} -> {:error, :unauthenticated}
    end
  end

  defp hide_passwordless_account(username, password) do
    _result = Acl.authenticate(username, password)
    {:error, :unauthenticated}
  end

  defp open_session(username, expected_auth_epoch, credential_source, peer)
       when is_integer(expected_auth_epoch) and expected_auth_epoch >= 0 do
    case {CatalogProjector.ready?(), Acl.get_user(username)} do
      {true, %{enabled: true, auth_epoch: ^expected_auth_epoch} = user} ->
        acl_cache = ConnectionAuth.build_acl_cache(username)

        if acl_cache == :denied or not Acl.credential_active?(user) or
             not credential_source_current?(credential_source) do
          {:error, :unauthenticated}
        else
          {:ok,
           %Session{
             username: username,
             auth_epoch: expected_auth_epoch,
             acl_cache: acl_cache,
             credential_source: credential_source,
             peer: peer,
             authenticated_at_ms: System.monotonic_time(:millisecond)
           }}
        end

      _changed_during_authentication ->
        {:error, :unauthenticated}
    end
  end

  defp open_session(_username, _auth_epoch, _credential_source, _peer),
    do: {:error, :unauthenticated}

  defp reserve_authentication(peer, username, password) do
    case AuthRateLimiter.permit(peer, username, password) do
      {:ok, _reservation} = permitted -> permitted
      {:error, {:rate_limited, _retry_after_ms}} = limited -> limited
      {:error, reason} when is_binary(reason) -> {:error, reason}
    end
  end

  defp projection_ready do
    if CatalogProjector.ready?(), do: :ok, else: {:error, :authentication_unavailable}
  end

  defp credential_source_current?(:acl), do: true

  defp credential_source_current?({:requirepass, expected_fingerprint}) do
    requirepass = requirepass()

    configured_secret?(requirepass) and
      secure_equal?(expected_fingerprint, credential_fingerprint(requirepass))
  end

  defp credential_source_current?(_unknown), do: false

  defp configured_secret?(secret), do: is_binary(secret) and secret != ""

  defp credential_fingerprint(secret) when is_binary(secret),
    do: :crypto.hash(:sha256, ["ferricstore-requirepass", 0, secret])

  defp secure_equal?(left, right) when is_binary(left) and is_binary(right),
    do: byte_size(left) == byte_size(right) and :crypto.hash_equals(left, right)

  defp secure_equal?(_left, _right), do: false

  defp requirepass do
    Config.get_value("requirepass") || Application.get_env(:ferricstore, :requirepass)
  rescue
    _error -> Application.get_env(:ferricstore, :requirepass)
  catch
    :exit, _reason -> Application.get_env(:ferricstore, :requirepass)
  end

  defp audit(event, username, peer, rate_limited) do
    AuditLog.log(event, %{
      username: username,
      client_ip: format_peer(peer),
      surface: :protocol_gateway,
      rate_limited: rate_limited
    })
  end

  defp format_peer({ip, _port}) when is_tuple(ip), do: format_peer(ip)

  defp format_peer(peer) when is_tuple(peer) do
    case :inet.ntoa(peer) do
      address when is_list(address) -> List.to_string(address)
      _other -> inspect(peer)
    end
  rescue
    _error -> inspect(peer)
  end

  defp format_peer(peer) when is_binary(peer), do: peer
  defp format_peer(_peer), do: "unknown"
end
