defmodule FerricstoreServer.Health.Endpoint.SecurityTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.AuthRateLimiter
  alias Ferricstore.AuditLog
  alias FerricstoreServer.Acl

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    old_protected_mode = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    AuthRateLimiter.reset()

    on_exit(fn ->
      restore_env(:protected_mode, old_protected_mode)
      AuthRateLimiter.reset()
    end)

    :ok
  end

  test "dashboard POST rejects a missing CSRF token" do
    response = http_request("POST", "/dashboard/logout", "", [])

    assert status_code(response) == 403
    assert response =~ "CSRF"
  end

  test "dashboard forms receive a token and accept a same-origin POST" do
    get_response = http_request("GET", "/dashboard/login", "", [])
    csrf_token = csrf_token(get_response)
    csrf_cookie = response_cookie(get_response, "ferricstore_dashboard_csrf")

    assert is_binary(csrf_token)
    assert is_binary(csrf_cookie)

    body = URI.encode_query(%{"_csrf_token" => csrf_token})

    post_response =
      http_request("POST", "/dashboard/logout", body, [
        {"Cookie", csrf_cookie},
        {"Origin", "http://localhost"}
      ])

    assert status_code(post_response) == 302
  end

  test "live dashboard component forms retain the request CSRF token" do
    id = "csrf-live-flow-#{System.unique_integer([:positive])}"

    assert :ok =
             FerricStore.flow_create(id,
               type: "csrf-live",
               state: "queued",
               run_at_ms: System.system_time(:millisecond)
             )

    page_response = http_request("GET", "/dashboard/flow/#{id}", "", [])
    token = csrf_token(page_response)
    cookie = response_cookie(page_response, "ferricstore_dashboard_csrf")

    api_response =
      http_request("GET", "/dashboard/api/flow/#{id}", "", [{"Cookie", cookie}])

    assert status_code(api_response) == 200
    assert {:ok, payload} = api_response |> response_body() |> Jason.decode()
    component = payload["components"]["flow_detail"]

    assert component =~ ~s(method="post")
    assert component =~ ~s(name="_csrf_token" value="#{token}")
  end

  test "dashboard POST rejects a mismatched Origin even with a valid token" do
    get_response = http_request("GET", "/dashboard/login", "", [])
    csrf_token = csrf_token(get_response)
    csrf_cookie = response_cookie(get_response, "ferricstore_dashboard_csrf")
    body = URI.encode_query(%{"_csrf_token" => csrf_token})

    response =
      http_request("POST", "/dashboard/logout", body, [
        {"Cookie", csrf_cookie},
        {"Origin", "https://attacker.example"}
      ])

    assert status_code(response) == 403
    assert response =~ "Origin"
  end

  test "HTTP responses include security headers and dashboard HTML has a restrictive CSP" do
    health_response = http_request("GET", "/health/live", "", [])

    assert response_header(health_response, "x-content-type-options") == "nosniff"
    assert response_header(health_response, "x-frame-options") == "DENY"
    assert response_header(health_response, "referrer-policy") == "no-referrer"
    assert response_header(health_response, "cache-control") == "no-store"
    assert response_header(health_response, "permissions-policy") =~ "camera=()"

    dashboard_response = http_request("GET", "/dashboard/login", "", [])
    content_security_policy = response_header(dashboard_response, "content-security-policy")

    assert content_security_policy =~ "default-src 'none'"
    assert content_security_policy =~ "frame-ancestors 'none'"
    assert content_security_policy =~ "form-action 'self'"
  end

  test "HSTS is emitted only when the dashboard request is considered secure" do
    old_trust_proxy = Application.get_env(:ferricstore, :dashboard_trust_proxy_headers)
    old_trusted_proxies = Application.get_env(:ferricstore, :dashboard_trusted_proxies)
    old_remote_access = Application.get_env(:ferricstore, :dashboard_remote_access)
    Application.put_env(:ferricstore, :dashboard_trust_proxy_headers, false)
    Application.put_env(:ferricstore, :dashboard_trusted_proxies, [])

    on_exit(fn ->
      restore_env(:dashboard_trust_proxy_headers, old_trust_proxy)
      restore_env(:dashboard_trusted_proxies, old_trusted_proxies)
      restore_env(:dashboard_remote_access, old_remote_access)
    end)

    plain_response = http_request("GET", "/dashboard/login", "", [])
    assert response_header(plain_response, "strict-transport-security") == nil

    Application.put_env(:ferricstore, :dashboard_trust_proxy_headers, true)
    Application.put_env(:ferricstore, :dashboard_trusted_proxies, ["127.0.0.1/32"])
    Application.put_env(:ferricstore, :dashboard_remote_access, true)

    secure_response =
      http_request("GET", "/dashboard/login", "", [{"X-Forwarded-Proto", "https"}])

    assert status_code(secure_response) == 200

    assert response_header(secure_response, "strict-transport-security") ==
             "max-age=31536000; includeSubDomains"
  end

  test "dashboard login rate limits and audits failures" do
    old_audit_enabled = Application.get_env(:ferricstore, :audit_log_enabled)
    old_max_attempts = Application.get_env(:ferricstore, :auth_rate_limit_max_attempts)

    Application.put_env(:ferricstore, :protected_mode, true)
    Application.put_env(:ferricstore, :audit_log_enabled, true)
    Application.put_env(:ferricstore, :auth_rate_limit_max_attempts, 1)
    assert :ok = Acl.set_user("dashboard-user", ["on", ">secret", "~*", "+@all"])
    :ok = AuditLog.reset()
    :ok = AuthRateLimiter.reset()

    on_exit(fn ->
      restore_env(:audit_log_enabled, old_audit_enabled)
      restore_env(:auth_rate_limit_max_attempts, old_max_attempts)
      AuthRateLimiter.reset()
    end)

    get_response = http_request("GET", "/dashboard/login", "", [])
    token = csrf_token(get_response)
    cookie = response_cookie(get_response, "ferricstore_dashboard_csrf")

    body =
      URI.encode_query(%{
        "_csrf_token" => token,
        "username" => "dashboard-user",
        "password" => "wrong"
      })

    headers = [{"Cookie", cookie}, {"Origin", "http://localhost"}]
    assert 401 == http_request("POST", "/dashboard/login", body, headers) |> status_code()
    assert 429 == http_request("POST", "/dashboard/login", body, headers) |> status_code()

    assert eventually(fn ->
             events = AuditLog.get()

             Enum.any?(events, fn {_id, _at, event, details} ->
               event == :auth_failure and details[:surface] == :dashboard and
                 details[:rate_limited] == true
             end)
           end)
  end

  test "successful dashboard logins do not consume the failure budget" do
    old_max_attempts = Application.get_env(:ferricstore, :auth_rate_limit_max_attempts)
    username = "dashboard-success-rate-limit"

    Application.put_env(:ferricstore, :protected_mode, true)
    Application.put_env(:ferricstore, :auth_rate_limit_max_attempts, 1)
    assert :ok = Acl.set_user(username, ["on", ">secret", "~*", "+@all"])
    :ok = AuthRateLimiter.reset()

    on_exit(fn ->
      Acl.del_user(username)
      restore_env(:auth_rate_limit_max_attempts, old_max_attempts)
      AuthRateLimiter.reset()
    end)

    get_response = http_request("GET", "/dashboard/login", "", [])
    token = csrf_token(get_response)
    cookie = response_cookie(get_response, "ferricstore_dashboard_csrf")

    body =
      URI.encode_query(%{
        "_csrf_token" => token,
        "username" => username,
        "password" => "secret"
      })

    headers = [{"Cookie", cookie}, {"Origin", "http://localhost"}]
    assert 302 == http_request("POST", "/dashboard/login", body, headers) |> status_code()
    assert 302 == http_request("POST", "/dashboard/login", body, headers) |> status_code()
  end

  test "trusted proxy login rate limits distinct forwarded clients independently" do
    old_max_attempts = Application.get_env(:ferricstore, :auth_rate_limit_max_attempts)
    old_remote_access = Application.get_env(:ferricstore, :dashboard_remote_access)
    old_trust_proxy = Application.get_env(:ferricstore, :dashboard_trust_proxy_headers)
    old_trusted_proxies = Application.get_env(:ferricstore, :dashboard_trusted_proxies)

    Application.put_env(:ferricstore, :auth_rate_limit_max_attempts, 1)
    Application.put_env(:ferricstore, :dashboard_remote_access, true)
    Application.put_env(:ferricstore, :dashboard_trust_proxy_headers, true)
    Application.put_env(:ferricstore, :dashboard_trusted_proxies, ["127.0.0.0/8"])
    AuthRateLimiter.reset()

    on_exit(fn ->
      restore_env(:auth_rate_limit_max_attempts, old_max_attempts)
      restore_env(:dashboard_remote_access, old_remote_access)
      restore_env(:dashboard_trust_proxy_headers, old_trust_proxy)
      restore_env(:dashboard_trusted_proxies, old_trusted_proxies)
      AuthRateLimiter.reset()
    end)

    proxy_headers = [
      {"X-Forwarded-Proto", "https"},
      {"X-Forwarded-Host", "localhost"}
    ]

    get_response = http_request("GET", "/dashboard/login", "", proxy_headers)
    token = csrf_token(get_response)
    cookie = response_cookie(get_response, "ferricstore_dashboard_csrf")

    login_headers = fn client_ip ->
      [
        {"Cookie", cookie},
        {"Origin", "https://localhost"},
        {"X-Forwarded-For", "#{client_ip}, 127.0.0.2"}
        | proxy_headers
      ]
    end

    assert 401 ==
             login_request(token, "proxy-user-a", login_headers.("198.51.100.10"))
             |> status_code()

    assert 429 ==
             login_request(token, "proxy-user-b", login_headers.("198.51.100.10"))
             |> status_code()

    assert 401 ==
             login_request(token, "proxy-user-c", login_headers.("198.51.100.11"))
             |> status_code()
  end

  test "dashboard login rejects passwordless ACL users" do
    Application.put_env(:ferricstore, :protected_mode, true)
    assert :ok = Acl.set_user("dashboard-nopass", ["on", "nopass", "~*", "+@all"])

    get_response = http_request("GET", "/dashboard/login", "", [])
    token = csrf_token(get_response)
    cookie = response_cookie(get_response, "ferricstore_dashboard_csrf")

    body =
      URI.encode_query(%{
        "_csrf_token" => token,
        "username" => "dashboard-nopass",
        "password" => "anything"
      })

    response =
      http_request("POST", "/dashboard/login", body, [
        {"Cookie", cookie},
        {"Origin", "http://localhost"}
      ])

    assert status_code(response) == 401
    refute response_header(response, "set-cookie") =~ "ferricstore_dashboard="
  end

  defp http_request(method, path, body, headers) do
    {:ok, conn} =
      :gen_tcp.connect({127, 0, 0, 1}, Endpoint.port(), [:binary, active: false, packet: :raw])

    header_lines = Enum.map_join(headers, "", fn {name, value} -> "#{name}: #{value}\r\n" end)

    request =
      "#{method} #{path} HTTP/1.1\r\n" <>
        "Host: localhost\r\n" <>
        header_lines <>
        "Content-Type: application/x-www-form-urlencoded\r\n" <>
        "Content-Length: #{byte_size(body)}\r\n" <>
        "Connection: close\r\n\r\n" <>
        body

    :ok = :gen_tcp.send(conn, request)
    response = recv_all(conn, "")
    :gen_tcp.close(conn)
    response
  end

  defp login_request(csrf_token, username, headers) do
    body =
      URI.encode_query(%{
        "_csrf_token" => csrf_token,
        "username" => username,
        "password" => "wrong"
      })

    http_request("POST", "/dashboard/login", body, headers)
  end

  defp recv_all(conn, acc) do
    case :gen_tcp.recv(conn, 0, 5_000) do
      {:ok, data} -> recv_all(conn, acc <> data)
      {:error, :closed} -> acc
    end
  end

  defp status_code(response) do
    [status_line | _rest] = String.split(response, "\r\n")
    [_, code | _rest] = String.split(status_line, " ")
    String.to_integer(code)
  end

  defp response_body(response) do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [_headers, body] -> body
      _other -> ""
    end
  end

  defp csrf_token(response) do
    case Regex.run(~r/name="_csrf_token" value="([^"]+)"/, response) do
      [_, token] -> token
      _other -> nil
    end
  end

  defp response_cookie(response, name) do
    response
    |> String.split("\r\n")
    |> Enum.find_value(fn line ->
      prefix = "Set-Cookie: #{name}="

      if String.starts_with?(line, prefix) do
        line
        |> String.replace_prefix("Set-Cookie: ", "")
        |> String.split(";", parts: 2)
        |> hd()
      end
    end)
  end

  defp response_header(response, name) do
    prefix = String.downcase(name) <> ":"

    response
    |> String.split("\r\n")
    |> Enum.find_value(fn line ->
      if String.starts_with?(String.downcase(line), prefix) do
        line
        |> String.split(":", parts: 2)
        |> List.last()
        |> String.trim()
      end
    end)
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, attempts) when attempts > 0 do
    fun.() or
      (
        Process.sleep(10)
        eventually(fun, attempts - 1)
      )
  end

  defp eventually(_fun, 0), do: false

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
