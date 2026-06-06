defmodule FerricstoreServer.Health.Endpoint.Session do
  @moduledoc false

  alias FerricstoreServer.Acl

  @dashboard_session_cookie "ferricstore_dashboard"
  @dashboard_session_ttl_ms 8 * 60 * 60 * 1_000
  @dashboard_session_secret_key {__MODULE__, :dashboard_session_secret}

  def session_cookie(username) do
    token = session_token(username)
    max_age = div(@dashboard_session_ttl_ms, 1_000)

    "#{@dashboard_session_cookie}=#{token}; Path=/dashboard; Max-Age=#{max_age}; HttpOnly; SameSite=Lax"
  end

  def clear_session_cookie do
    "#{@dashboard_session_cookie}=; Path=/dashboard; Max-Age=0; HttpOnly; SameSite=Lax"
  end

  def session_user(headers) do
    with token when is_binary(token) <- cookie_value(headers, @dashboard_session_cookie),
         [encoded_payload, signature] <- String.split(token, ".", parts: 2),
         true <- constant_time_equal?(signature, session_signature(encoded_payload)),
         {:ok, payload} <- Base.url_decode64(encoded_payload, padding: false),
         {username, expires_at} when is_binary(username) and is_integer(expires_at) <-
           safe_binary_to_term(payload),
         true <- expires_at > System.system_time(:millisecond),
         %{enabled: true} <- Acl.get_user(username) do
      username
    else
      _ -> nil
    end
  end

  def cookie_value(headers, name) do
    headers
    |> Map.get("cookie", "")
    |> String.split(";")
    |> Enum.find_value(fn part ->
      case String.split(String.trim(part), "=", parts: 2) do
        [^name, value] -> value
        _ -> nil
      end
    end)
  end

  defp session_token(username) do
    expires_at = System.system_time(:millisecond) + @dashboard_session_ttl_ms
    payload = :erlang.term_to_binary({username, expires_at})
    encoded_payload = Base.url_encode64(payload, padding: false)
    signature = session_signature(encoded_payload)
    encoded_payload <> "." <> signature
  end

  defp session_signature(encoded_payload) do
    :crypto.mac(:hmac, :sha256, session_secret(), encoded_payload)
    |> Base.url_encode64(padding: false)
  end

  defp session_secret do
    case Application.get_env(:ferricstore, :dashboard_session_secret) do
      secret when is_binary(secret) and byte_size(secret) >= 32 ->
        secret

      _ ->
        case :persistent_term.get(@dashboard_session_secret_key, nil) do
          nil ->
            secret = :crypto.strong_rand_bytes(32)
            :persistent_term.put(@dashboard_session_secret_key, secret)
            secret

          secret ->
            secret
        end
    end
  end

  defp safe_binary_to_term(payload) do
    :erlang.binary_to_term(payload, [:safe])
  rescue
    _ -> :error
  end

  defp constant_time_equal?(left, right) when is_binary(left) and is_binary(right) do
    :crypto.hash(:sha256, left) == :crypto.hash(:sha256, right)
  end

  defp constant_time_equal?(_left, _right), do: false
end
