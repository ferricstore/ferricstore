defmodule FerricstoreServer.Health.Endpoint.Login do
  @moduledoc false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.Password

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
    case Acl.get_user(username) do
      %{enabled: true, password: stored_hash, auth_epoch: auth_epoch}
      when is_binary(stored_hash) ->
        if Password.verify(password, stored_hash) do
          {:ok, username, auth_epoch}
        else
          authentication_error()
        end

      _missing_disabled_or_passwordless ->
        _verified = Password.verify(password, Password.dummy_hash())
        authentication_error()
    end
  end

  def authenticate_session(_username, _password), do: authentication_error()

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
    error_html = if is_binary(error), do: "<p class=\"error\">#{html_escape(error)}</p>", else: ""

    """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <title>FerricStore Dashboard Login</title>
      <style>
        body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0d1117;color:#e6edf3;font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
        main{width:min(420px,calc(100vw - 32px));border:1px solid #30363d;background:#161b22;padding:24px}
        h1{font-size:22px;margin:0 0 8px}
        p{color:#8b949e;margin:0 0 18px}
        label{display:block;margin:14px 0 6px;color:#c9d1d9;font-size:13px}
        input{box-sizing:border-box;width:100%;border:1px solid #30363d;background:#0d1117;color:#e6edf3;padding:10px 12px;font:inherit}
        button{margin-top:18px;width:100%;border:0;background:#238636;color:white;padding:11px 12px;font:inherit;cursor:pointer}
        .error{border:1px solid #f85149;color:#ffb4ae;background:#2d1215;padding:10px 12px}
      </style>
    </head>
    <body>
      <main>
        <h1>FerricStore Dashboard</h1>
        <p>Sign in with a FerricStore ACL user. Page access follows that user's command and key permissions.</p>
        #{error_html}
        <form method="post" action="/dashboard/login">
          <input type="hidden" name="next" value="#{html_escape(safe_next)}">
          <label for="username">Username</label>
          <input id="username" name="username" autocomplete="username" autofocus>
          <label for="password">Password</label>
          <input id="password" name="password" type="password" autocomplete="current-password">
          <button type="submit">Sign in</button>
        </form>
      </main>
    </body>
    </html>
    """
  end

  defp has_control_byte?(path) do
    :binary.match(path, [<<"\r">>, <<"\n">>]) != :nomatch or
      :binary.match(path, for(byte <- 0..31, byte not in [?\r, ?\n], do: <<byte>>)) != :nomatch or
      :binary.match(path, <<127>>) != :nomatch
  end

  defp html_escape(value) when is_binary(value) do
    value
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
  end

  defp html_escape(value), do: value |> to_string() |> html_escape()

  defp authentication_error, do: {:error, @authentication_error}
end
