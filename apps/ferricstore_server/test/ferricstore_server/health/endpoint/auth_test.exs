defmodule FerricstoreServer.Health.Endpoint.AuthTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Health.Endpoint.Auth
  alias FerricstoreServer.Health.Endpoint.Session

  setup do
    old_protected_mode = Application.get_env(:ferricstore, :protected_mode)
    old_session_secret = Application.get_env(:ferricstore, :dashboard_session_secret)
    old_remote_access = Application.get_env(:ferricstore, :dashboard_remote_access)
    old_insecure_http = Application.get_env(:ferricstore, :dashboard_allow_insecure_http)
    old_trust_proxy = Application.get_env(:ferricstore, :dashboard_trust_proxy_headers)
    old_trusted_proxies = Application.get_env(:ferricstore, :dashboard_trusted_proxies)
    old_public_scheme = Application.get_env(:ferricstore, :dashboard_public_scheme)

    Application.put_env(:ferricstore, :dashboard_session_secret, String.duplicate("s", 32))
    Application.delete_env(:ferricstore, :dashboard_remote_access)
    Application.delete_env(:ferricstore, :dashboard_allow_insecure_http)
    Application.delete_env(:ferricstore, :dashboard_trust_proxy_headers)
    Application.delete_env(:ferricstore, :dashboard_trusted_proxies)
    Application.delete_env(:ferricstore, :dashboard_public_scheme)
    Acl.reset!()

    on_exit(fn ->
      restore_env(:protected_mode, old_protected_mode)
      restore_env(:dashboard_session_secret, old_session_secret)
      restore_env(:dashboard_remote_access, old_remote_access)
      restore_env(:dashboard_allow_insecure_http, old_insecure_http)
      restore_env(:dashboard_trust_proxy_headers, old_trust_proxy)
      restore_env(:dashboard_trusted_proxies, old_trusted_proxies)
      restore_env(:dashboard_public_scheme, old_public_scheme)
      Acl.reset!()
    end)

    :ok
  end

  test "authorize_request allows dashboard and metrics in open mode" do
    Application.put_env(:ferricstore, :protected_mode, false)

    assert Auth.authorize_request("GET", "/dashboard/config", {127, 0, 0, 1}, %{}) == :ok
    assert Auth.authorize_request("GET", "/metrics", {10, 0, 0, 1}, %{}) == :ok
    assert Auth.observability_authorized?({10, 0, 0, 1}, %{})
  end

  test "dashboard remote access is denied by default without affecting health routes" do
    Application.put_env(:ferricstore, :protected_mode, false)

    assert Auth.authorize_request("GET", "/health/ready", {10, 0, 0, 1}, %{}) == :ok

    assert Auth.authorize_request("GET", "/dashboard/login", {10, 0, 0, 1}, %{}) ==
             {:forbidden, {"DASHBOARD", []}, "remote dashboard access is disabled"}
  end

  test "remote dashboard access requires a secure request unless explicitly overridden" do
    Application.put_env(:ferricstore, :protected_mode, false)
    Application.put_env(:ferricstore, :dashboard_remote_access, true)

    assert Auth.authorize_request("GET", "/dashboard", {10, 0, 0, 1}, %{}) ==
             {:forbidden, {"DASHBOARD", []}, "secure dashboard transport is required"}

    Application.put_env(:ferricstore, :dashboard_allow_insecure_http, true)
    assert Auth.authorize_request("GET", "/dashboard", {10, 0, 0, 1}, %{}) == :ok
  end

  test "remote dashboard access accepts HTTPS from an explicitly trusted proxy" do
    Application.put_env(:ferricstore, :protected_mode, false)
    Application.put_env(:ferricstore, :dashboard_remote_access, true)
    Application.put_env(:ferricstore, :dashboard_trust_proxy_headers, true)
    Application.put_env(:ferricstore, :dashboard_trusted_proxies, ["10.0.0.0/8"])

    headers = %{"x-forwarded-proto" => "https"}
    assert Auth.authorize_request("GET", "/dashboard", {10, 0, 0, 1}, headers) == :ok

    assert Auth.authorize_request("GET", "/dashboard", {192, 0, 2, 1}, headers) ==
             {:forbidden, {"DASHBOARD", []}, "secure dashboard transport is required"}
  end

  test "public scheme and forwarded headers cannot make an untrusted peer secure" do
    Application.put_env(:ferricstore, :protected_mode, false)
    Application.put_env(:ferricstore, :dashboard_remote_access, true)
    Application.put_env(:ferricstore, :dashboard_public_scheme, "https")
    Application.put_env(:ferricstore, :dashboard_trust_proxy_headers, true)
    Application.put_env(:ferricstore, :dashboard_trusted_proxies, ["10.0.0.0/8"])

    headers = %{"x-forwarded-proto" => "https"}

    assert Auth.authorize_request("GET", "/dashboard", {192, 0, 2, 1}, headers) ==
             {:forbidden, {"DASHBOARD", []}, "secure dashboard transport is required"}

    assert {:forbidden, {"DASHBOARD", []}, _reason} =
             Auth.authorize_request("GET", "/dashboard", {127, 0, 0, 1}, headers)
  end

  test "a configured loopback proxy cannot fall back to direct local access" do
    Application.put_env(:ferricstore, :protected_mode, false)
    Application.put_env(:ferricstore, :dashboard_remote_access, true)
    Application.put_env(:ferricstore, :dashboard_trust_proxy_headers, true)
    Application.put_env(:ferricstore, :dashboard_trusted_proxies, ["127.0.0.1/32"])

    assert Auth.authorize_request("GET", "/dashboard", {127, 0, 0, 1}, %{}) ==
             {:forbidden, {"DASHBOARD", []}, "secure dashboard transport is required"}

    assert Auth.authorize_request(
             "GET",
             "/dashboard",
             {127, 0, 0, 1},
             %{"x-forwarded-proto" => "http"}
           ) == {:forbidden, {"DASHBOARD", []}, "secure dashboard transport is required"}

    assert Auth.authorize_request(
             "GET",
             "/dashboard",
             {127, 0, 0, 1},
             %{"x-forwarded-proto" => "https"}
           ) == :ok
  end

  test "authorize_request requires login in protected mode" do
    Application.put_env(:ferricstore, :protected_mode, true)

    assert Auth.authorize_request("GET", "/dashboard/api/overview", {127, 0, 0, 1}, %{}) ==
             {:unauthorized, "login required"}

    assert {:redirect_login, location} =
             Auth.authorize_request("GET", "/dashboard/config", {127, 0, 0, 1}, %{})

    assert location =~ "/dashboard/login?next="
    refute Auth.observability_authorized?({127, 0, 0, 1}, %{})
  end

  test "authorize_request enforces dashboard route command ACLs" do
    Application.put_env(:ferricstore, :protected_mode, true)
    assert :ok = Acl.set_user("viewer", ["on", "nopass", "~*", "-@all", "+info"])
    headers = session_headers("viewer")

    assert Auth.authorize_request("GET", "/dashboard", {127, 0, 0, 1}, headers) == :ok

    assert {:forbidden, {"CONFIG", []}, reason} =
             Auth.authorize_request("GET", "/dashboard/config", {127, 0, 0, 1}, headers)

    assert is_binary(reason)
  end

  test "authorize_request enforces dashboard key ACLs" do
    Application.put_env(:ferricstore, :protected_mode, true)
    assert :ok = Acl.set_user("tenant", ["on", "nopass", "~tenant:*", "-@all", "+get"])
    headers = session_headers("tenant")

    assert Auth.authorize_request(
             "GET",
             "/dashboard/keyspace?key=tenant%3A1",
             {127, 0, 0, 1},
             headers
           ) == :ok

    assert {:forbidden, {"GET", [key: {"other:1", :read}]}, reason} =
             Auth.authorize_request(
               "GET",
               "/dashboard/keyspace?key=other%3A1",
               {127, 0, 0, 1},
               headers
             )

    assert reason =~ "GET key"
  end

  test "global Flow schedules and governance pages require wildcard read access" do
    Application.put_env(:ferricstore, :protected_mode, true)

    assert :ok =
             Acl.set_user("tenant-flow-admin", [
               "on",
               "nopass",
               "%R~tenant-a:*",
               "-@all",
               "+FLOW.SCHEDULE.LIST",
               "+FLOW.GOVERNANCE.OVERVIEW"
             ])

    tenant_headers = session_headers("tenant-flow-admin")

    for {path, command} <- [
          {"/dashboard/flow/schedules", "FLOW.SCHEDULE.LIST"},
          {"/dashboard/flow/governance", "FLOW.GOVERNANCE.OVERVIEW"}
        ] do
      assert {:forbidden, {^command, [key: {"*", :read}]}, reason} =
               Auth.authorize_request("GET", path, {127, 0, 0, 1}, tenant_headers)

      assert reason =~ "key"
    end

    assert :ok =
             Acl.set_user("global-flow-admin", [
               "on",
               "nopass",
               "~*",
               "-@all",
               "+FLOW.SCHEDULE.LIST",
               "+FLOW.GOVERNANCE.OVERVIEW"
             ])

    global_headers = session_headers("global-flow-admin")

    assert Auth.authorize_request(
             "GET",
             "/dashboard/flow/schedules",
             {127, 0, 0, 1},
             global_headers
           ) == :ok

    assert Auth.authorize_request(
             "GET",
             "/dashboard/flow/governance",
             {127, 0, 0, 1},
             global_headers
           ) == :ok
  end

  test "dashboard collect opts include acl username when session is valid" do
    Application.put_env(:ferricstore, :protected_mode, true)
    assert :ok = Acl.set_user("alice", ["on", "nopass", "~*", "+@all"])
    headers = session_headers("alice")

    assert Auth.dashboard_collect_opts({10, 0, 0, 1}, headers) == %{"acl_username" => "alice"}

    assert Auth.dashboard_flow_collect_opts([limit: 10], {10, 0, 0, 1}, headers) ==
             [acl_username: "alice", limit: 10]
  end

  test "dashboard sessions are revoked when ACL credentials or rules change" do
    Application.put_env(:ferricstore, :protected_mode, true)
    assert :ok = Acl.set_user("mutable", ["on", ">old-secret", "~*", "+info"])

    credentials_cookie = Session.session_cookie("mutable")
    assert Session.session_user(%{"cookie" => credentials_cookie}) == "mutable"

    assert :ok = Acl.set_user("mutable", [">new-secret"])
    assert Session.session_user(%{"cookie" => credentials_cookie}) == nil

    rules_cookie = Session.session_cookie("mutable")
    assert Session.session_user(%{"cookie" => rules_cookie}) == "mutable"

    assert :ok = Acl.set_user("mutable", ["+config"])
    assert Session.session_user(%{"cookie" => rules_cookie}) == nil
  end

  test "dashboard sessions stay revoked after ACL rules are restored" do
    Application.put_env(:ferricstore, :protected_mode, true)
    assert :ok = Acl.set_user("reversible", ["on", "nopass", "~*", "+info"])

    cookie = Session.session_cookie("reversible")
    assert Session.session_user(%{"cookie" => cookie}) == "reversible"

    assert :ok = Acl.set_user("reversible", ["+config"])
    assert Session.session_user(%{"cookie" => cookie}) == nil

    assert :ok = Acl.set_user("reversible", ["-config"])
    assert Session.session_user(%{"cookie" => cookie}) == nil
  end

  test "dashboard sessions stay revoked after an ACL user is deleted and recreated" do
    Application.put_env(:ferricstore, :protected_mode, true)
    rules = ["on", "nopass", "~*", "+info"]
    assert :ok = Acl.set_user("recreated", rules)

    cookie = Session.session_cookie("recreated")
    assert Session.session_user(%{"cookie" => cookie}) == "recreated"

    assert :ok = Acl.del_user("recreated")
    assert :ok = Acl.set_user("recreated", rules)
    assert Session.session_user(%{"cookie" => cookie}) == nil
  end

  test "dashboard sessions reject non-canonical external-term payloads" do
    Application.put_env(:ferricstore, :protected_mode, true)
    username = "canonical-" <> String.duplicate("a", 512)
    assert :ok = Acl.set_user(username, ["on", "nopass", "~*", "+info"])

    cookie = Session.session_cookie(username)
    token = Session.cookie_value(%{"cookie" => cookie}, "ferricstore_dashboard")
    [encoded_payload, _signature] = String.split(token, ".", parts: 2)
    {:ok, payload} = Base.url_decode64(encoded_payload, padding: false)
    term = :erlang.binary_to_term(payload, [:safe])

    compressed = :erlang.term_to_binary(term, compressed: 9)
    assert <<131, 80, _rest::binary>> = compressed

    for non_canonical <- [compressed, payload <> <<0>>] do
      headers = %{"cookie" => signed_session_cookie(non_canonical)}
      assert Session.session_user(headers) == nil
    end
  end

  defp session_headers(username) do
    %{"cookie" => Session.session_cookie(username)}
  end

  defp signed_session_cookie(payload) do
    encoded_payload = Base.url_encode64(payload, padding: false)

    signature =
      :crypto.mac(:hmac, :sha256, String.duplicate("s", 32), encoded_payload)
      |> Base.url_encode64(padding: false)

    "ferricstore_dashboard=#{encoded_payload}.#{signature}"
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
