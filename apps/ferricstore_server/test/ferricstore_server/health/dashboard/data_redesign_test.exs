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

  alias FerricstoreServer.Health.Dashboard.Render.KVPages

  @shards [
    %{index: 0, disk_bytes: 1_000_000, data_file_count: 1, hint_file_count: 0},
    %{index: 1, disk_bytes: 3_000_000, data_file_count: 2, hint_file_count: 1}
  ]

  test "Storage opens with where the bytes are" do
    html =
      KVPages.render_storage_summary(%{
        shards: @shards,
        total_disk_bytes: 5_000_000,
        total_files: 4
      })

    assert html =~ "storage-status is-idle"
    assert html =~ "<strong>4.77 MB on disk across 2 shards</strong>"
    assert html =~ "largest: shard 1 at 2.86 MB, 75% of shard data"
    assert html =~ "976.56 KB outside shard directories"
    assert html =~ ~s(<h2 class="section-title sr-only">Storage Summary</h2>)
    refute html =~ "Largest Shard"
  end

  test "each shard's size is drawn against the largest shard" do
    html = KVPages.render_storage_table(@shards)

    assert html =~ ~s(<span class="size-bar" style="width: 33%"></span>)
    assert html =~ ~s(<span class="size-bar" style="width: 100%"></span>)
    assert html =~ "<th>Data files</th><th>Hint files</th>"
  end
end
