defmodule FerricstoreServer.Health.Dashboard.SummaryCardsTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.Overview

  test "measured values keep the large figure and words read as words" do
    html =
      Overview.render_ops_summary("Summary", [
        %{label: "Keys", value: "2.6K"},
        %{label: "Size", value: "7.09 MB"},
        %{label: "Hot reads", value: "Unavailable"},
        %{label: "Mode", value: "on", class: "c-green"},
        %{label: "Largest", value: "Shard 11"}
      ])

    assert html =~ ~s(<div class="ops-summary-value">2.6K</div>)
    assert html =~ ~s(<div class="ops-summary-value">7.09 MB</div>)
    assert html =~ ~s(<div class="ops-summary-value is-text">Unavailable</div>)
    assert html =~ ~s(<div class="ops-summary-value c-green is-text">on</div>)
    assert html =~ ~s(<div class="ops-summary-value is-text">Shard 11</div>)
  end

  test "table scroll boxes contain absolutely positioned labels" do
    css = FerricstoreServer.Health.Dashboard.Layout.Styles.stylesheet()
    assert css =~ ~r/\.table-scroll \{ position: relative;/
  end
end
