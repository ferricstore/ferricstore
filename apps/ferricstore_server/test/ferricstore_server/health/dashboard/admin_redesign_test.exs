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
end
