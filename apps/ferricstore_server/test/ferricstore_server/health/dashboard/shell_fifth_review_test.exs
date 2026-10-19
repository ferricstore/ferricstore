defmodule FerricstoreServer.Health.Dashboard.ShellFifthReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Layout
  alias FerricstoreServer.Health.Dashboard.Layout.Styles

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
end
