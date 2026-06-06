defmodule FerricstoreServer.Health.Endpoint.SessionTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Endpoint.Session

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

  test "cookie_value extracts named cookies" do
    headers = %{"cookie" => "one=1; ferricstore_dashboard=token; two=2"}

    assert Session.cookie_value(headers, "ferricstore_dashboard") == "token"
    assert Session.cookie_value(headers, "missing") == nil
  end
end
