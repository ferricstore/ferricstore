defmodule FerricstoreServer.Health.Dashboard.StatusLayoutTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Layout.Styles
  alias FerricstoreServer.Health.Dashboard.Render.{Admin, Prefixes}

  @long String.duplicate("tenant-segment-", 20)

  test "long names in status lines are shortened" do
    prefixes =
      Prefixes.render_prefixes_summary(%{
        prefixes: [%{prefix: @long, keys: 10, pct: 100.0, hot_reads: nil, cold_reads: nil}],
        total_sampled: 10
      })

    assert prefixes =~ "largest: " <> String.slice(@long, 0, 40) <> "…"
    refute prefixes =~ @long

    slow =
      Admin.render_slowlog_summary([
        %{id: 1, timestamp_us: 1, duration_us: 20_000, command: [@long, "k"]}
      ])

    assert slow =~ "worst 20.0 ms: " <> String.slice(@long, 0, 40) <> "…"
  end

  test "status text wraps instead of widening the page" do
    css = Styles.stylesheet()
    assert css =~ ".page-status span { overflow-wrap: anywhere; min-width: 0; }"
  end

  test "the governance count strip keeps its dividers when it wraps" do
    css = Styles.stylesheet()

    assert css =~
             ".stat-strip > div { padding: 12px 16px; box-shadow: 1px 1px 0 var(--line-subtle); }"

    refute css =~ ".stat-strip > div:last-child { border-right: 0; }"
  end
end
