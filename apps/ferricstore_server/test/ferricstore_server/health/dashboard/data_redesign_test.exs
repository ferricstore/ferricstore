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

  alias FerricstoreServer.Health.Dashboard.Render.Prefixes

  @prefix_data %{
    total_sampled: 2_601,
    prefixes: [
      %{prefix: "cache", keys: 900, pct: 34.6, hot_reads: nil, cold_reads: nil},
      %{prefix: "user", keys: 800, pct: 30.8, hot_reads: 18, cold_reads: 0}
    ]
  }

  test "Prefixes opens with the largest prefix's share" do
    html = Prefixes.render_prefixes_summary(@prefix_data)

    assert html =~ "prefixes-status is-idle"
    assert html =~ "<strong>2 prefixes in 2.6K sampled keys</strong>"
    assert html =~ "largest: cache, 34.6% of the sample"
    assert html =~ ~s(<h2 class="section-title sr-only">Prefix Summary</h2>)
  end

  test "each prefix's share of the sample is a bar, and missing counters read quietly" do
    html = Prefixes.render_prefixes_table(@prefix_data)

    assert html =~ ~s(<span class="size-bar" style="width: 35%"></span>)
    assert html =~ ~s(<span class="c-muted">Unavailable</span>)
  end

  test "Read path opens with the hit rate, or says there are no samples" do
    html =
      KVPages.render_reads_summary(%{
        hotcold: %{hit_ratio: 90.0, hot_reads: 1_800, cold_reads: 0, total_misses: 200}
      })

    assert html =~ "reads-status is-idle"
    assert html =~ "<strong>90.0% hit rate</strong>"
    assert html =~ "1.8K hot reads"
    assert html =~ "0 cold reads from disk"
    assert html =~ "200 misses"

    assert KVPages.render_reads_summary(%{
             hotcold: %{hit_ratio: 0.0, hot_reads: 0, cold_reads: 0}
           }) =~
             "<strong>No read samples yet</strong>"
  end

  test "each prefix's cold share is a bar" do
    html =
      KVPages.render_read_prefix_table(%{
        prefixes: [%{prefix: "user", hot_reads: 15, cold_reads: 5, cold_pct: 25.0}]
      })

    assert html =~ ~s(<span class="size-bar is-cold" style="width: 25%"></span>)
  end

  test "Commands opens with traffic and slow log, never a measured zero for missing samples" do
    busy =
      KVPages.render_commands_summary(%{
        summary: %{
          total_commands: 12_000,
          ops_per_sec: 4.2,
          slowlog_entries: 3,
          slowest_us: 12_500
        }
      })

    assert busy =~ "commands-status is-idle"
    assert busy =~ "<strong>12.0K commands since start</strong>"
    assert busy =~ "4.2/s on average"
    assert busy =~ "3 slow log entries, slowest 12.5 ms"

    assert KVPages.render_commands_summary(%{summary: %{total_commands: 0}}) =~
             "<strong>No commands since start</strong>"

    unavailable = KVPages.render_commands_summary(%{summary: %{slowlog_status: :unavailable}})
    assert unavailable =~ "commands-status is-warning"
    assert unavailable =~ "Slow log unavailable"
  end

  test "an empty slow log is a prompt that links to the Slow log page" do
    html = KVPages.render_command_slowlog_table(%{slow_by_command: []})

    refute html =~ "<table"

    assert html =~
             ~s(<p class="section-empty">No slow commands recorded. <a href="/dashboard/slowlog">Open Slow log</a></p>)
  end

  test "the command groups are reference material and fold away" do
    html = KVPages.render_kv_command_reference(%{})

    assert html =~
             ~s(<details class="dashboard-disclosure" id="command-reference"><summary>Command reference</summary>)
  end
end
