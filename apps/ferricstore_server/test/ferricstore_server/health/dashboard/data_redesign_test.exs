defmodule FerricstoreServer.Health.Dashboard.DataRedesignTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Layout.Styles
  alias FerricstoreServer.Health.Dashboard.Render.Overview

  test "summary counts are one connected strip" do
    css = Styles.stylesheet()

    assert css =~
             ".ops-summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 0; margin-bottom: 20px; border: 1px solid var(--line); border-radius: 12px; background: var(--surface-raised); overflow: hidden; }"

    # Dividers between cells in both directions, clipped at the outer edge when the strip wraps.
    assert css =~ ".ops-summary-card { box-shadow: 1px 1px 0 var(--line-subtle);"
  end

  test "a page with a status line can keep the summary heading for screen readers only" do
    html =
      Overview.render_ops_summary("Storage Summary", [%{label: "Disk", value: "1 MB"}],
        heading: :hidden
      )

    assert html =~ ~s(<h2 class="section-title sr-only">Storage Summary</h2>)

    assert Overview.render_ops_summary("Storage Summary", [%{label: "Disk", value: "1 MB"}]) =~
             ~s(<h2 class="section-title">Storage Summary</h2>)
  end
end
