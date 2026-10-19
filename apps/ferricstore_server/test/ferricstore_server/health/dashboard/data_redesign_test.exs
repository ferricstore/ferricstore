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

  alias FerricstoreServer.Health.Dashboard.Render.Admin

  defp merge(index, overrides \\ []),
    do:
      Map.merge(
        %{
          shard_index: index,
          mode: :hot,
          merging: false,
          last_merge_at: nil,
          merge_count: 0,
          total_bytes_reclaimed: 0
        },
        Map.new(overrides)
      )

  test "Merges says whether anything has merged or is merging" do
    never = Admin.render_merge_summary([merge(0), merge(1)])
    assert never =~ "merges-status is-idle"
    assert never =~ "<strong>No merges since start</strong>"
    assert never =~ "all 2 shards idle"

    now = System.system_time(:millisecond)

    done =
      Admin.render_merge_summary([
        merge(0, last_merge_at: now - 90_000, merge_count: 3, total_bytes_reclaimed: 2_048),
        merge(1)
      ])

    assert done =~ "merges-status is-healthy"
    assert done =~ "<strong>No merge running</strong>"
    assert done =~ ~r/last merge 1\.\dm ago/
    assert done =~ "3 merges reclaimed 2.0 KB"

    running = Admin.render_merge_summary([merge(0, merging: true), merge(1)])
    assert running =~ "<strong>1 of 2 shards merging now</strong>"
  end

  test "when no shard has merged, the per-shard table folds away" do
    html = Admin.render_merge_table([merge(0), merge(1)])

    assert html =~
             ~s(<details class="dashboard-disclosure"><summary>Per-shard merge status</summary>)

    merged = Admin.render_merge_table([merge(0, merge_count: 1), merge(1)])
    refute merged =~ "<details"
    assert merged =~ "<table"
  end
end
