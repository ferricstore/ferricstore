defmodule FerricstoreServer.Health.Dashboard.LayoutLiveTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Layout

  test "live polling exposes freshness, retry, backoff, and session expiry handling" do
    script = Layout.dashboard_live_script()

    assert script =~ ~s(data-dashboard-live-status)
    assert script =~ ~s(data-dashboard-live-retry)
    assert script =~ "Updated just now"
    assert script =~ "Stale"
    assert script =~ "response.status === 401"
    assert script =~ ~s(/dashboard/login?next=)
    assert script =~ "Math.pow(2, failureCount - 1)"
    assert script =~ "window.setTimeout(tick"
    assert script =~ ~r/patchComponents\(payload\.components\);\s+ensureLiveStatusMounted\(\);/
    assert script =~ ~s(root.dataset.dashboardLiveError = "")
    refute script =~ "window.setInterval(tick, intervalMs)"
  end
end
