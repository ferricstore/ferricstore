defmodule FerricstoreServer.Health.Dashboard.ShellHardeningTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.{Format, Layout}
  alias FerricstoreServer.Health.Dashboard.Layout.Styles

  test "millisecond and microsecond timestamps retain precision and label UTC" do
    assert Format.format_timestamp_ms(1_788_949_000_001) == "2026-09-09 10:16:40.001 UTC"
    assert Format.format_timestamp_ms(1_788_949_000_999) == "2026-09-09 10:16:40.999 UTC"
    assert Format.format_timestamp_us(1_788_949_000_001_001) == "2026-09-09 10:16:40.001001 UTC"
    assert Format.format_timeline_timestamp_ms(1_788_949_000_001) == "10:16:40.001 UTC"
    assert Format.format_timestamp_ms_or_dash(nil) == "-"
    assert Format.format_timestamp_ms_or_dash(0) == "-"
  end

  test "metric help exposes a separate description instead of a paragraph image name" do
    html = Format.info_icon("A <bounded> definition")
    assert html =~ ~s(data-tooltip="A &lt;bounded&gt; definition")
    assert html =~ ~s(aria-label="Metric help")
    refute html =~ ~s(role="img")
    refute html =~ ~s(aria-label="A &lt;bounded&gt; definition")
  end

  test "shell has no-JS skip navigation and configurable character shortcuts" do
    assert Layout.render_skip_link() =~ ~s(href="#dashboard-main")
    assert Layout.render_keyboard_shortcuts_modal() =~ "data-dashboard-character-shortcuts"
    assert Styles.stylesheet() =~ ".dashboard-skip-link:focus"
  end

  test "shared script installs bounded timeouts, retained state, and public clipboard contract" do
    script = Layout.dashboard_live_script()
    assert script =~ "window.dashboardCopyText"
    assert script =~ "requestController.abort()"
    assert script =~ "data-dashboard-live-toggle"
    assert script =~ "data-dashboard-scroll-key"
    assert script =~ "window.location.pathname + window.location.search + window.location.hash"
    assert script =~ "event.metaKey"
    assert script =~ "action_snapshot"
  end
end
