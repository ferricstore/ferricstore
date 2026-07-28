defmodule FerricstoreServer.Health.Endpoint.Login do
  @moduledoc false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.Password
  alias FerricstoreServer.Health.Endpoint.AccessPage

  @authentication_error "WRONGPASS invalid username-password pair or user is disabled."

  @spec authenticate(binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def authenticate(username, password) do
    case authenticate_session(username, password) do
      {:ok, username, _auth_epoch} -> {:ok, username}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec authenticate_session(binary(), binary()) ::
          {:ok, binary(), non_neg_integer()} | {:error, binary()}
  def authenticate_session(username, password)
      when is_binary(username) and is_binary(password) do
    authenticate_session_with(username, password, fn -> Acl.authenticate(username, password) end)
  end

  def authenticate_session(_username, _password), do: authentication_error()

  @doc false
  @spec authenticate_session(binary(), binary(), (binary(), binary() -> boolean())) ::
          {:ok, binary(), non_neg_integer()} | {:error, binary()}
  def authenticate_session(username, password, verifier)
      when is_binary(username) and is_binary(password) and is_function(verifier, 2) do
    authenticate_session_with(username, password, fn ->
      Acl.authenticate(username, password, verifier)
    end)
  end

  def authenticate_session(_username, _password, _verifier), do: authentication_error()

  @doc false
  @spec peer_string(term()) :: binary()
  def peer_string(peer) when is_tuple(peer) do
    case :inet.ntoa(peer) do
      address when is_list(address) -> List.to_string(address)
      _other -> inspect(peer)
    end
  rescue
    _error -> inspect(peer)
  end

  def peer_string(_peer), do: "unknown"

  def location(path) do
    "/dashboard/login?" <> URI.encode_query(%{"next" => sanitize_next(path)})
  end

  def sanitize_next(path) when is_binary(path) do
    cond do
      path == "" -> "/dashboard"
      has_control_byte?(path) -> "/dashboard"
      String.starts_with?(path, "//") -> "/dashboard"
      String.starts_with?(path, "/dashboard/login") -> "/dashboard"
      String.starts_with?(path, "/dashboard") -> path
      true -> "/dashboard"
    end
  end

  def sanitize_next(_path), do: "/dashboard"

  def render_page(next, error) do
    safe_next = sanitize_next(next)

    form_html =
      """
      <form method="post" action="/dashboard/login">
        <input type="hidden" name="next" value="#{AccessPage.escape(safe_next)}">
        <label for="username">Username</label>
        <input id="username" name="username" maxlength="1024" autocomplete="username" required autofocus autocapitalize="none" spellcheck="false">
        <label for="password">Password</label>
        <input id="password" name="password" type="password" maxlength="4096" autocomplete="current-password" required>
        <button type="submit">Sign in</button>
      </form>
      """

    AccessPage.render(%{
      title: "FerricStore Dashboard Login",
      kicker: "Protected mode",
      heading: "Sign in to the control plane",
      copy:
        "Use a FerricStore ACL account. Every dashboard page keeps that account's command and key boundaries.",
      error: error,
      form_html: form_html,
      context_heading: "Access follows the live ACL policy.",
      context_items: [
        {"Identity", "FerricStore ACL"},
        {"Transport", "Local or HTTPS"},
        {"Session", "Signed and revocable"}
      ],
      footer_items: ["ACL-scoped access", "Revoked when credentials or rules change"]
    })
  end

  defp has_control_byte?(path) do
    :binary.match(path, [<<"\r">>, <<"\n">>]) != :nomatch or
      :binary.match(path, for(byte <- 0..31, byte not in [?\r, ?\n], do: <<byte>>)) != :nomatch or
      :binary.match(path, <<127>>) != :nomatch
  end

  defp authenticate_session_with(username, password, authenticate) do
    case Acl.get_user(username) do
      %{enabled: true, password: stored_hash, auth_epoch: initial_epoch}
      when is_binary(stored_hash) and is_integer(initial_epoch) and initial_epoch >= 0 ->
        case authenticate.() do
          {:ok, ^username} ->
            case Acl.get_user(username) do
              %{enabled: true, auth_epoch: ^initial_epoch} ->
                {:ok, username, initial_epoch}

              _changed_during_authentication ->
                authentication_error()
            end

          {:error, _reason} ->
            authentication_error()
        end

      _missing_disabled_or_passwordless ->
        _verified = Password.verify(password, Password.dummy_hash())
        authentication_error()
    end
  end

  defp authentication_error, do: {:error, @authentication_error}
end
