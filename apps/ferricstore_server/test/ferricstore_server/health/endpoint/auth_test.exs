defmodule FerricstoreServer.Health.Endpoint.AuthTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Health.Endpoint.Auth
  alias FerricstoreServer.Health.Endpoint.Session

  setup do
    old_protected_mode = Application.get_env(:ferricstore, :protected_mode)
    old_session_secret = Application.get_env(:ferricstore, :dashboard_session_secret)

    Application.put_env(:ferricstore, :dashboard_session_secret, String.duplicate("s", 32))
    Acl.reset!()

    on_exit(fn ->
      restore_env(:protected_mode, old_protected_mode)
      restore_env(:dashboard_session_secret, old_session_secret)
      Acl.reset!()
    end)

    :ok
  end

  test "authorize_request allows dashboard and metrics in open mode" do
    Application.put_env(:ferricstore, :protected_mode, false)

    assert Auth.authorize_request("GET", "/dashboard/config", {10, 0, 0, 1}, %{}) == :ok
    assert Auth.authorize_request("GET", "/metrics", {10, 0, 0, 1}, %{}) == :ok
    assert Auth.observability_authorized?({10, 0, 0, 1}, %{})
  end

  test "authorize_request requires login in protected mode" do
    Application.put_env(:ferricstore, :protected_mode, true)

    assert Auth.authorize_request("GET", "/dashboard/api/overview", {10, 0, 0, 1}, %{}) ==
             {:unauthorized, "login required"}

    assert {:redirect_login, location} =
             Auth.authorize_request("GET", "/dashboard/config", {10, 0, 0, 1}, %{})

    assert location =~ "/dashboard/login?next="
    refute Auth.observability_authorized?({127, 0, 0, 1}, %{})
  end

  test "authorize_request enforces dashboard route command ACLs" do
    Application.put_env(:ferricstore, :protected_mode, true)
    assert :ok = Acl.set_user("viewer", ["on", "nopass", "~*", "-@all", "+info"])
    headers = session_headers("viewer")

    assert Auth.authorize_request("GET", "/dashboard", {10, 0, 0, 1}, headers) == :ok

    assert {:forbidden, {"CONFIG", []}, reason} =
             Auth.authorize_request("GET", "/dashboard/config", {10, 0, 0, 1}, headers)

    assert is_binary(reason)
  end

  test "authorize_request enforces dashboard key ACLs" do
    Application.put_env(:ferricstore, :protected_mode, true)
    assert :ok = Acl.set_user("tenant", ["on", "nopass", "~tenant:*", "-@all", "+get"])
    headers = session_headers("tenant")

    assert Auth.authorize_request(
             "GET",
             "/dashboard/keyspace?key=tenant%3A1",
             {10, 0, 0, 1},
             headers
           ) == :ok

    assert {:forbidden, {"GET", [key: {"other:1", :read}]}, reason} =
             Auth.authorize_request(
               "GET",
               "/dashboard/keyspace?key=other%3A1",
               {10, 0, 0, 1},
               headers
             )

    assert reason =~ "GET key"
  end

  test "dashboard collect opts include acl username when session is valid" do
    Application.put_env(:ferricstore, :protected_mode, true)
    assert :ok = Acl.set_user("alice", ["on", "nopass", "~*", "+@all"])
    headers = session_headers("alice")

    assert Auth.dashboard_collect_opts({10, 0, 0, 1}, headers) == %{"acl_username" => "alice"}

    assert Auth.dashboard_flow_collect_opts([limit: 10], {10, 0, 0, 1}, headers) ==
             [acl_username: "alice", limit: 10]
  end

  defp session_headers(username) do
    %{"cookie" => Session.session_cookie(username)}
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
