defmodule FerricstoreServer.Health.Endpoint.ForbiddenTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Endpoint.Forbidden

  test "requirement_details includes command rule" do
    assert Forbidden.requirement_details("FLOW.QUERY") == %{
             required_command: "FLOW.QUERY",
             required_acl_rule: "+FLOW.QUERY"
           }
  end

  test "requirement_details includes key rule when present" do
    assert Forbidden.requirement_details({"FLOW.GET", key: {"tenant-a", :read}}) == %{
             required_command: "FLOW.GET",
             required_acl_rule: "+FLOW.GET",
             required_key: "tenant-a",
             required_key_access: "read",
             required_key_rule: "%R~tenant-a"
           }
  end

  test "dashboard denial reuses the responsive protected-access surface" do
    details =
      Forbidden.requirement_details({"FLOW.QUERY", key: {"type:sms", :read}})

    html =
      Forbidden.render_page(
        details,
        "NOPERM <blocked>",
        "/dashboard/flow/query?kind=list"
      )

    assert html =~ ~s(<meta name="viewport" content="width=device-width, initial-scale=1">)
    assert html =~ ~s(<body class="auth-body">)
    assert html =~ "Access denied"
    assert html =~ "NOPERM &lt;blocked&gt;"
    assert html =~ "+FLOW.QUERY"
    assert html =~ "read on type:sms"
    assert html =~ "%R~type:sms"
    assert html =~ "Sign in as another user"
    assert html =~ ~s(<form method="post" action="/dashboard/logout">)
    assert html =~ ~s(name="next" value="/dashboard/flow/query?kind=list")
    refute html =~ "<body><h1>Forbidden</h1>"
  end
end
