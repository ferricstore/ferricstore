defmodule FerricstoreServer.Health.Endpoint.Session do
  @moduledoc false

  import Bitwise

  alias FerricstoreServer.Acl

  @dashboard_session_cookie "ferricstore_dashboard"
  @dashboard_csrf_cookie "ferricstore_dashboard_csrf"
  @dashboard_session_ttl_ms 8 * 60 * 60 * 1_000
  @dashboard_session_secret_key {__MODULE__, :dashboard_session_secret}
  @max_forwarded_for_hops 32
  @csrf_process_key {__MODULE__, :csrf_token}
  @request_headers_process_key {__MODULE__, :request_headers}
  @request_peer_process_key {__MODULE__, :request_peer}

  def session_cookie(username, headers \\ %{}) do
    session_cookie(username, :unknown, headers)
  end

  @doc false
  def session_cookie(username, peer, headers) do
    user = Acl.get_user(username)
    auth_epoch = Map.get(user || %{}, :auth_epoch, -1)
    session_cookie(username, auth_epoch, peer, headers)
  end

  @doc false
  def session_cookie(username, auth_epoch, peer, headers) when is_integer(auth_epoch) do
    token = session_token(username, auth_epoch)
    max_age = div(@dashboard_session_ttl_ms, 1_000)

    "#{@dashboard_session_cookie}=#{token}; Path=/dashboard; Max-Age=#{max_age}; HttpOnly; SameSite=Lax#{secure_suffix(peer, headers)}"
  end

  def clear_session_cookie(headers \\ %{}) do
    clear_session_cookie(:unknown, headers)
  end

  @doc false
  def clear_session_cookie(peer, headers) do
    "#{@dashboard_session_cookie}=; Path=/dashboard; Max-Age=0; HttpOnly; SameSite=Lax#{secure_suffix(peer, headers)}"
  end

  @doc false
  @spec secure_request?(map()) :: boolean()
  def secure_request?(headers) when is_map(headers), do: secure_request?(:unknown, headers)
  def secure_request?(_headers), do: false

  @doc false
  @spec secure_request?(term(), map()) :: boolean()
  def secure_request?(peer, headers) when is_map(headers),
    do: request_scheme(peer, headers) == "https"

  def secure_request?(_peer, _headers), do: false

  @doc false
  @spec current_request_secure?() :: boolean()
  def current_request_secure? do
    secure_request?(
      Process.get(@request_peer_process_key, :unknown),
      Process.get(@request_headers_process_key, %{})
    )
  end

  @doc false
  @spec client_peer(term(), map()) :: term()
  def client_peer(peer, headers) when is_map(headers) do
    if forwarded_headers_trusted?(peer) do
      case forwarded_for_chain(headers) do
        {:ok, forwarded_chain} -> resolve_client_peer(forwarded_chain, peer)
        :error -> peer
      end
    else
      peer
    end
  end

  def client_peer(peer, _headers), do: peer

  @doc false
  @spec prepare_request(map()) :: :ok
  def prepare_request(headers) when is_map(headers) do
    prepare_request(:unknown, headers)
  end

  @doc false
  @spec prepare_request(term(), map()) :: :ok
  def prepare_request(peer, headers) when is_map(headers) do
    token =
      case cookie_value(headers, @dashboard_csrf_cookie) do
        token when is_binary(token) -> if valid_csrf_token?(token), do: token, else: csrf_token()
        _other -> csrf_token()
      end

    Process.put(@csrf_process_key, token)
    Process.put(@request_headers_process_key, headers)
    Process.put(@request_peer_process_key, peer)
    :ok
  end

  @doc false
  @spec csrf_pair(map()) :: {binary(), binary()}
  def csrf_pair(headers \\ %{}) when is_map(headers) do
    csrf_pair(:unknown, headers)
  end

  @doc false
  @spec csrf_pair(term(), map()) :: {binary(), binary()}
  def csrf_pair(peer, headers) when is_map(headers) do
    token = csrf_token()
    {token, csrf_cookie(token, peer, headers)}
  end

  @doc false
  @spec protect_html(binary()) :: {binary(), {binary(), binary()}}
  def protect_html(body) when is_binary(body) do
    token = request_csrf_token()
    headers = Process.get(@request_headers_process_key, %{})
    peer = Process.get(@request_peer_process_key, :unknown)

    {protect_forms(body, token), {"Set-Cookie", csrf_cookie(token, peer, headers)}}
  end

  @doc false
  @spec protect_live_payload(map()) :: {map(), {binary(), binary()}}
  def protect_live_payload(payload) when is_map(payload) do
    token = request_csrf_token()
    headers = Process.get(@request_headers_process_key, %{})
    peer = Process.get(@request_peer_process_key, :unknown)

    protected_payload =
      cond do
        is_map(Map.get(payload, :components)) ->
          Map.update!(payload, :components, &protect_components(&1, token))

        is_map(Map.get(payload, "components")) ->
          Map.update!(payload, "components", &protect_components(&1, token))

        true ->
          payload
      end

    {protected_payload, {"Set-Cookie", csrf_cookie(token, peer, headers)}}
  end

  @doc false
  @spec validate_state_change(binary(), binary(), map(), binary()) :: :ok | {:error, binary()}
  def validate_state_change("POST", "/dashboard" <> _rest, headers, body) do
    validate_state_change("POST", "/dashboard", :unknown, headers, body)
  end

  def validate_state_change(_method, _path, _headers, _body), do: :ok

  @doc false
  @spec validate_state_change(binary(), binary(), term(), map(), binary()) ::
          :ok | {:error, binary()}
  def validate_state_change("POST", "/dashboard" <> _rest, peer, headers, body) do
    with :ok <- validate_origin(peer, headers),
         token when is_binary(token) <- cookie_value(headers, @dashboard_csrf_cookie),
         true <- valid_csrf_token?(token),
         submitted when is_binary(submitted) <- csrf_body_token(body),
         true <- constant_time_equal?(token, submitted) do
      :ok
    else
      {:error, _reason} = error -> error
      _other -> {:error, "CSRF token is missing or invalid"}
    end
  end

  def validate_state_change(_method, _path, _peer, _headers, _body), do: :ok

  def session_user(headers) do
    with token when is_binary(token) <- cookie_value(headers, @dashboard_session_cookie),
         [encoded_payload, signature] <- String.split(token, ".", parts: 2),
         true <- constant_time_equal?(signature, session_signature(encoded_payload)),
         {:ok, payload} <- Base.url_decode64(encoded_payload, padding: false),
         {username, expires_at, auth_epoch, acl_fingerprint}
         when is_binary(username) and is_integer(expires_at) and is_integer(auth_epoch) and
                is_binary(acl_fingerprint) <-
           safe_binary_to_term(payload),
         true <- expires_at > System.system_time(:millisecond),
         %{enabled: true} = user <- Acl.get_user(username),
         true <- Map.get(user, :auth_epoch) == auth_epoch,
         true <- constant_time_equal?(acl_fingerprint, acl_fingerprint(user)) do
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

  defp session_token(username, auth_epoch) do
    expires_at = System.system_time(:millisecond) + @dashboard_session_ttl_ms
    user = Acl.get_user(username)
    fingerprint = acl_fingerprint(user)
    payload = :erlang.term_to_binary({username, expires_at, auth_epoch, fingerprint})
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
    byte_size(left) == byte_size(right) and :crypto.hash_equals(left, right)
  end

  defp constant_time_equal?(_left, _right), do: false

  defp csrf_token do
    nonce = :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)
    nonce <> "." <> csrf_signature(nonce)
  end

  defp request_csrf_token do
    Process.get(@csrf_process_key) || csrf_token()
  end

  defp protect_components(components, token) do
    Map.new(components, fn
      {name, html} when is_binary(html) -> {name, protect_forms(html, token)}
      entry -> entry
    end)
  end

  defp protect_forms(body, token) do
    hidden = ~s(<input type="hidden" name="_csrf_token" value="#{token}">)

    Regex.replace(
      ~r/<form\b(?=[^>]*\bmethod=["']post["'])[^>]*>/i,
      body,
      fn opening_tag -> opening_tag <> hidden end
    )
  end

  defp valid_csrf_token?(token) when is_binary(token) do
    case String.split(token, ".", parts: 2) do
      [nonce, signature] when nonce != "" ->
        constant_time_equal?(signature, csrf_signature(nonce))

      _other ->
        false
    end
  end

  defp valid_csrf_token?(_token), do: false

  defp csrf_signature(nonce) do
    :crypto.mac(:hmac, :sha256, session_secret(), "csrf:" <> nonce)
    |> Base.url_encode64(padding: false)
  end

  defp csrf_cookie(token, peer, headers) do
    max_age = div(@dashboard_session_ttl_ms, 1_000)

    "#{@dashboard_csrf_cookie}=#{token}; Path=/dashboard; Max-Age=#{max_age}; HttpOnly; SameSite=Strict#{secure_suffix(peer, headers)}"
  end

  defp csrf_body_token(body) when is_binary(body) do
    body
    |> URI.decode_query()
    |> Map.get("_csrf_token")
  rescue
    _error -> nil
  end

  defp validate_origin(peer, headers) do
    case Map.get(headers, "origin") do
      nil ->
        :ok

      "" ->
        :ok

      "null" ->
        {:error, "request Origin is not allowed"}

      origin ->
        if allowed_origin?(origin, peer, headers),
          do: :ok,
          else: {:error, "request Origin is not allowed"}
    end
  end

  defp allowed_origin?(origin, peer, headers) do
    configured_origins = Application.get_env(:ferricstore, :dashboard_allowed_origins, [])

    origin in configured_origins or same_request_origin?(origin, peer, headers)
  end

  defp same_request_origin?(origin, peer, headers) do
    scheme = if secure_request?(peer, headers), do: "https", else: "http"
    host = request_host(peer, headers)

    with %URI{scheme: ^scheme, host: origin_host} = origin_uri <- URI.parse(origin),
         true <- is_binary(origin_host),
         %URI{host: request_host} = request_uri <- URI.parse("#{scheme}://#{host}"),
         true <- is_binary(request_host) do
      String.downcase(origin_host) == String.downcase(request_host) and
        origin_uri.port == request_uri.port and origin_uri.userinfo == nil and
        origin_uri.query == nil and origin_uri.fragment == nil and
        origin_uri.path in [nil, "", "/"]
    else
      _other -> false
    end
  end

  defp request_host(peer, headers) do
    if forwarded_headers_trusted?(peer) do
      headers
      |> Map.get("x-forwarded-host", Map.get(headers, "host", ""))
      |> String.split(",", parts: 2)
      |> hd()
      |> String.trim()
    else
      Map.get(headers, "host", "")
    end
  end

  defp secure_suffix(peer, headers) do
    if secure_cookie?(peer, headers), do: "; Secure", else: ""
  end

  defp secure_cookie?(peer, headers) do
    case Application.get_env(:ferricstore, :dashboard_cookie_secure, :auto) do
      true -> true
      false -> false
      :auto -> secure_request?(peer, headers)
      "true" -> true
      "false" -> false
      _other -> secure_request?(peer, headers)
    end
  end

  defp request_scheme(peer, headers) do
    if forwarded_headers_trusted?(peer) do
      headers
      |> Map.get("x-forwarded-proto", "")
      |> String.split(",", parts: 2)
      |> hd()
      |> String.trim()
      |> String.downcase()
      |> case do
        scheme when scheme in ["http", "https"] -> scheme
        _other -> "http"
      end
    else
      "http"
    end
  end

  defp forwarded_headers_trusted?(peer) do
    Application.get_env(:ferricstore, :dashboard_trust_proxy_headers, false) == true and
      trusted_proxy_peer?(peer)
  end

  defp forwarded_for_chain(headers) do
    with value when is_binary(value) and value != "" <- Map.get(headers, "x-forwarded-for"),
         parts <- String.split(value, ",", trim: false),
         true <- length(parts) in 1..@max_forwarded_for_hops do
      Enum.reduce_while(parts, {:ok, []}, fn part, {:ok, addresses} ->
        case parse_address(part) do
          {:ok, address} -> {:cont, {:ok, [normalize_mapped_ipv4(address) | addresses]}}
          _error -> {:halt, :error}
        end
      end)
      |> case do
        {:ok, addresses} -> {:ok, Enum.reverse(addresses)}
        :error -> :error
      end
    else
      _other -> :error
    end
  end

  defp resolve_client_peer(forwarded_chain, peer) do
    socket_peer = peer_address(peer)

    case Enum.drop_while(Enum.reverse(forwarded_chain ++ [socket_peer]), &trusted_proxy_peer?/1) do
      [client_peer | _rest] -> client_peer
      [] -> hd(forwarded_chain)
    end
  end

  defp peer_address({ip, _port}) when is_tuple(ip), do: normalize_mapped_ipv4(ip)
  defp peer_address(ip) when is_tuple(ip), do: normalize_mapped_ipv4(ip)
  defp peer_address(peer), do: peer

  @doc false
  @spec trusted_proxy_peer?(term()) :: boolean()
  def trusted_proxy_peer?({ip, _port}) when is_tuple(ip), do: trusted_proxy_peer?(ip)

  def trusted_proxy_peer?(peer) when is_tuple(peer) do
    peer = normalize_mapped_ipv4(peer)

    :ferricstore
    |> Application.get_env(:dashboard_trusted_proxies, [])
    |> List.wrap()
    |> Enum.any?(&peer_matches_proxy?(peer, &1))
  end

  def trusted_proxy_peer?(_peer), do: false

  defp peer_matches_proxy?(peer, {network, prefix})
       when is_tuple(network) and is_integer(prefix) do
    cidr_match?(peer, normalize_mapped_ipv4(network), prefix)
  end

  defp peer_matches_proxy?(peer, entry) when is_binary(entry) do
    case String.split(entry, "/", parts: 2) do
      [address] ->
        with {:ok, network} <- parse_address(address) do
          peer == normalize_mapped_ipv4(network)
        else
          _other -> false
        end

      [address, prefix] ->
        with {:ok, network} <- parse_address(address),
             {prefix, ""} <- Integer.parse(prefix) do
          cidr_match?(peer, normalize_mapped_ipv4(network), prefix)
        else
          _other -> false
        end
    end
  end

  defp peer_matches_proxy?(peer, network) when is_tuple(network) do
    peer == normalize_mapped_ipv4(network)
  end

  defp peer_matches_proxy?(_peer, _entry), do: false

  defp cidr_match?(peer, network, prefix) when tuple_size(peer) == tuple_size(network) do
    bits = if tuple_size(peer) == 4, do: 32, else: 128

    if prefix in 0..bits do
      shift = bits - prefix
      address_integer(peer) >>> shift == address_integer(network) >>> shift
    else
      false
    end
  end

  defp cidr_match?(_peer, _network, _prefix), do: false

  defp address_integer(address) when tuple_size(address) == 4 do
    address
    |> Tuple.to_list()
    |> Enum.reduce(0, fn part, acc -> (acc <<< 8) + part end)
  end

  defp address_integer(address) do
    address
    |> Tuple.to_list()
    |> Enum.reduce(0, fn part, acc -> (acc <<< 16) + part end)
  end

  defp normalize_mapped_ipv4({0, 0, 0, 0, 0, 65_535, high, low}) do
    {high >>> 8, high &&& 0xFF, low >>> 8, low &&& 0xFF}
  end

  defp normalize_mapped_ipv4(address), do: address

  defp parse_address(address) do
    address
    |> String.trim()
    |> String.to_charlist()
    |> :inet.parse_address()
  rescue
    _error -> :error
  end

  defp acl_fingerprint(user) when is_map(user) do
    security_state = %{
      enabled: Map.get(user, :enabled, false),
      password: Map.get(user, :password),
      commands: normalize_acl_set(Map.get(user, :commands)),
      denied_commands: normalize_acl_set(Map.get(user, :denied_commands)),
      keys: normalize_acl_patterns(Map.get(user, :keys)),
      channels: normalize_acl_patterns(Map.get(user, :channels))
    }

    security_state
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp acl_fingerprint(_user), do: :crypto.hash(:sha256, "missing-acl-user")

  defp normalize_acl_set(:all), do: :all
  defp normalize_acl_set(%MapSet{} = values), do: values |> MapSet.to_list() |> Enum.sort()
  defp normalize_acl_set(values) when is_list(values), do: Enum.sort(values)
  defp normalize_acl_set(_values), do: []

  defp normalize_acl_patterns(:all), do: :all

  defp normalize_acl_patterns(patterns) when is_list(patterns) do
    Enum.map(patterns, fn
      {pattern, access, _compiled} -> {pattern, access}
      {pattern, _compiled} -> pattern
      pattern -> pattern
    end)
  end

  defp normalize_acl_patterns(_patterns), do: []
end
