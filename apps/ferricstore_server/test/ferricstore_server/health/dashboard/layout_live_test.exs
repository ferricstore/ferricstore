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
    assert script =~ "if (dashboardInteractionPaused())"
    assert script =~ "Updates paused while editing"
    assert script =~ ~s(input, textarea, select, [data-dashboard-live-pause])
    refute script =~ ~s(input, textarea, select, button, [data-dashboard-live-pause])
    assert script =~ "componentsPatched = patchComponents(payload.components)"
    assert script =~ "if (componentsPatched)"
    assert script =~ ~s(root.dataset.dashboardLiveError = "")
    refute script =~ "window.setInterval(tick, intervalMs)"
  end

  test "malformed URL fragments cannot abort dashboard interaction setup" do
    script = Layout.dashboard_live_script()

    assert script =~ "function decodeDashboardHash"
    assert script =~ "try { return decodeURIComponent(value); }"
    assert script =~ "catch (_error) { return \"\"; }"
    assert script =~ "decodeDashboardHash(hash.slice(1))"
    refute script =~ "var anchor = decodeURIComponent(hash.slice(1));"
  end

  test "journal hash selection survives live component replacement" do
    script = Layout.dashboard_live_script()

    assert script =~ "function selectJournalStepFromHash"
    assert script =~ "anchor.indexOf(\"journal-flow-event-\") !== 0"
    assert script =~ "activeSelectedStepId = step.getAttribute(\"data-flow-event-id\")"
    assert script =~ "window.addEventListener(\"hashchange\", selectJournalStepFromHash)"
    assert script =~ ~r/applyJournalState\(document\);\s+selectJournalStepFromHash\(\);/
  end
end
