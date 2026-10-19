defmodule FerricstoreServer.Health.Dashboard.AdminRedesignTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.Admin

  defp slow(id, duration_us, command),
    do: %{id: id, timestamp_us: 1_000_000, duration_us: duration_us, command: command}

  test "Slow log opens with the worst command, or says nothing was slow" do
    html =
      Admin.render_slowlog_summary([
        slow(1, 12_500, ["GET", "a"]),
        slow(2, 250_000, ["KEYS", "*"])
      ])

    assert html =~ "slowlog-status is-idle"
    assert html =~ "<strong>2 slow commands recorded</strong>"
    assert html =~ "worst 250.0 ms: KEYS"
    assert html =~ ~s(<h2 class="section-title sr-only">Slow Log Summary</h2>)

    assert Admin.render_slowlog_summary([]) =~ "<strong>No slow commands recorded</strong>"

    unavailable = Admin.render_slowlog_summary(%{status: :unavailable})
    assert unavailable =~ "slowlog-status is-warning"
  end

  test "an empty slow log is a prompt without filter controls" do
    html = Admin.render_slowlog_table([])

    assert html =~ ~s(<p class="section-empty">No slow commands recorded.</p>)
    refute html =~ "<table"
    refute html =~ "data-dashboard-table-filter"
  end

  test "Clients opens with connections and blocked clients" do
    conns = %{active: 3, blocked: 1, tracking: 0}
    blocked = Admin.render_clients_summary(conns, [])
    assert blocked =~ "clients-status is-warning"
    assert blocked =~ "<strong>1 blocked of 3 connections</strong>"

    assert Admin.render_clients_summary(%{conns | blocked: 0}, []) =~
             "<strong>3 native connections</strong>"

    assert Admin.render_clients_summary(%{active: 0, blocked: 0, tracking: 0}, []) =~
             "<strong>No native connections</strong>"
  end

  test "an empty connection page is a prompt; the flags legend comes with rows" do
    html = Admin.render_clients_table([], "No matching connections in this bounded page")

    assert html =~ ~s(<p class="section-empty">No matching connections in this bounded page</p>)
    refute html =~ "<table"
    refute html =~ "Flags: M=in MULTI"
  end

  alias FerricstoreServer.Health.Dashboard.Render.Security

  test "Security opens with whether access is protected" do
    protected =
      Security.render_acl_security_summary(%{
        protected_mode: true,
        configured_users: true,
        acl_user_count: 3,
        current_user: "default",
        can_manage_users: true
      })

    assert protected =~ "security-status is-healthy"
    assert protected =~ "<strong>Protected: 3 ACL users</strong>"
    assert protected =~ "signed in as default"
    assert protected =~ "account changes enabled"

    open = Security.render_acl_security_summary(%{protected_mode: false, configured_users: false})
    assert open =~ "security-status is-warning"
    assert open =~ "<strong>Protected mode is off</strong>"
  end

  test "the accounts list comes first and account creation folds below it" do
    html =
      Security.render_acl_security(%{
        current_user: "default",
        can_manage_users: true,
        acl_users: [%{username: "default", state: "on"}]
      })

    assert html =~
             ~s(<details class="dashboard-disclosure acl-create"><summary>Create an account</summary>)

    assert position(html, "ACL.LIST") < position(html, "Create an account")

    failed =
      Security.render_acl_security(%{
        current_user: "default",
        can_manage_users: true,
        acl_users: [],
        account_draft: %{"username" => "ops", "role" => "observer"}
      })

    assert failed =~ ~s(<details class="dashboard-disclosure acl-create" open>)
  end

  defp position(html, needle) do
    {index, _length} = :binary.match(html, needle)
    index
  end
end
