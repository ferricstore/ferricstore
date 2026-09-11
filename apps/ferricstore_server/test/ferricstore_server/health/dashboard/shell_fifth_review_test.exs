defmodule FerricstoreServer.Health.Dashboard.ShellFifthReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Layout
  alias FerricstoreServer.Health.Dashboard.Layout.Styles

  test "KV local and global navigation share labels, order, and active destination" do
    sidebar = Layout.render_sidebar_static("commands")
    [_, kv] = Regex.run(~r/data-dashboard-nav-group="KV \/ Data"[^>]*>(.*?)<\/details>/s, sidebar)
    local = Layout.render_kv_subnav("commands")
    assert links(kv) == links(local)

    assert local =~
             ~s(aria-current="page" title="Traffic, slowlog, and command groups">Command Catalog)

    assert length(Regex.scan(~r/aria-current="page"/, kv)) == 1
  end

  test "sidebar preferences are bounded session state and always reveal the active route" do
    script = Layout.dashboard_live_script()
    assert script =~ "ferricstore.sidebar.groups.v1"
    assert script =~ "sessionStorage.getItem"
    assert script =~ "sessionStorage.setItem"
    assert script =~ "group.querySelector('[aria-current=\"page\"]')"
    assert script =~ "setupSidebarPreferences();"
  end

  test "refresh supports draft cancellation before navigation" do
    script = Layout.dashboard_live_script()
    assert script =~ "new CustomEvent('dashboard:before-refresh', { cancelable: true })"
    assert script =~ "if (document.dispatchEvent(refreshEvent)) { window.location.reload(); }"
  end

  test "keyboard help names the actual context-independent search behavior" do
    html = Layout.render_keyboard_shortcuts_modal()
    assert html =~ "Focus search (when available)"
    refute html =~ "Search Flow / Partition"
  end

  test "operational labels and caveats have a twelve pixel minimum" do
    sizes = Regex.scan(~r/font-size:\s*(0\.\d+)rem/, Styles.stylesheet())
    assert sizes != []
    assert Enum.all?(sizes, fn [_, value] -> String.to_float(value) >= 0.75 end)
  end

  defp links(html) do
    Regex.scan(~r/<a[^>]*href="([^"]+)"[^>]*>(.*?)<\/a>/s, html)
    |> Enum.map(fn [_, href, body] ->
      {href, body |> String.replace(~r/<[^>]+>/, "") |> String.trim()}
    end)
  end
end
