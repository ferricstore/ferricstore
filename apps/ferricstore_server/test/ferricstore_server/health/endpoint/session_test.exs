defmodule FerricstoreServer.Health.Endpoint.SessionTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Health.Endpoint.Session

  @tag :dashboard_session_secret_config
  test "remote dashboards require an explicit strong shared session secret" do
    old_remote_access = Application.get_env(:ferricstore, :dashboard_remote_access)
    old_session_secret = Application.get_env(:ferricstore, :dashboard_session_secret)

    on_exit(fn ->
      restore_env(:dashboard_remote_access, old_remote_access)
      restore_env(:dashboard_session_secret, old_session_secret)
    end)

    Application.put_env(:ferricstore, :dashboard_remote_access, true)
    Application.delete_env(:ferricstore, :dashboard_session_secret)

    assert_raise ArgumentError, ~r/dashboard_session_secret/, fn ->
      Session.initialize_secret!()
    end

    Application.put_env(:ferricstore, :dashboard_session_secret, "too-short")

    assert_raise ArgumentError, ~r/at least 32 bytes/, fn ->
      Session.initialize_secret!()
    end

    Application.put_env(:ferricstore, :dashboard_session_secret, String.duplicate("s", 32))
    assert :ok = Session.initialize_secret!()
  end

  test "session_cookie emits scoped http-only dashboard cookie" do
    cookie = Session.session_cookie("admin")

    assert cookie =~ "ferricstore_dashboard="
    assert cookie =~ "Path=/dashboard"
    assert cookie =~ "Max-Age=28800"
    assert cookie =~ "HttpOnly"
    assert cookie =~ "SameSite=Lax"
  end

  test "clear_session_cookie expires the dashboard cookie" do
    assert Session.clear_session_cookie() ==
             "ferricstore_dashboard=; Path=/dashboard; Max-Age=0; HttpOnly; SameSite=Lax"
  end

  test "session cookies become Secure for trusted TLS proxy requests" do
    old_trust_proxy = Application.get_env(:ferricstore, :dashboard_trust_proxy_headers)
    old_trusted_proxies = Application.get_env(:ferricstore, :dashboard_trusted_proxies)
    old_cookie_secure = Application.get_env(:ferricstore, :dashboard_cookie_secure)

    Application.put_env(:ferricstore, :dashboard_trust_proxy_headers, true)
    Application.put_env(:ferricstore, :dashboard_trusted_proxies, ["10.0.0.0/8"])
    Application.put_env(:ferricstore, :dashboard_cookie_secure, :auto)

    on_exit(fn ->
      restore_env(:dashboard_trust_proxy_headers, old_trust_proxy)
      restore_env(:dashboard_trusted_proxies, old_trusted_proxies)
      restore_env(:dashboard_cookie_secure, old_cookie_secure)
    end)

    headers = %{"x-forwarded-proto" => "https"}
    cookie = Session.session_cookie("admin", {10, 2, 3, 4}, headers)

    assert cookie =~ "; Secure"
    refute Session.session_cookie("admin", {192, 0, 2, 1}, headers) =~ "; Secure"
  end

  test "client_peer resolves a sanitized client through only a trusted proxy chain" do
    old_trust_proxy = Application.get_env(:ferricstore, :dashboard_trust_proxy_headers)
    old_trusted_proxies = Application.get_env(:ferricstore, :dashboard_trusted_proxies)

    Application.put_env(:ferricstore, :dashboard_trust_proxy_headers, true)
    Application.put_env(:ferricstore, :dashboard_trusted_proxies, ["10.0.0.0/8"])

    on_exit(fn ->
      restore_env(:dashboard_trust_proxy_headers, old_trust_proxy)
      restore_env(:dashboard_trusted_proxies, old_trusted_proxies)
    end)

    trusted_peer = {10, 0, 0, 4}

    assert Session.client_peer(trusted_peer, %{
             "x-forwarded-for" => "198.51.100.27, 10.1.2.3",
             "x-forwarded-proto" => "https"
           }) == {198, 51, 100, 27}

    assert Session.client_peer({192, 0, 2, 4}, %{
             "x-forwarded-for" => "198.51.100.28"
           }) == {192, 0, 2, 4}

    assert Session.client_peer(trusted_peer, %{
             "x-forwarded-for" => "198.51.100.29, not-an-address"
           }) == trusted_peer

    assert Session.client_peer(trusted_peer, %{
             "x-forwarded-for" => <<255>>
           }) == trusted_peer
  end

  test "cookie_value extracts named cookies" do
    headers = %{"cookie" => "one=1; ferricstore_dashboard=token; two=2"}

    assert Session.cookie_value(headers, "ferricstore_dashboard") == "token"
    assert Session.cookie_value(headers, "missing") == nil
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
