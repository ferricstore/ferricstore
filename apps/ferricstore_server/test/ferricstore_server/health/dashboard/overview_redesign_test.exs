defmodule FerricstoreServer.Health.Dashboard.OverviewRedesignTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Render.Overview

  test "the overview uses the shared page header instead of a top bar" do
    html = Dashboard.render(Dashboard.collect())

    refute html =~ "top-bar"
    assert html =~ ~s(<h1 class="page-title">Overview</h1>)
    assert html =~ ~s(data-live-component="content")
    assert html =~ ~r{<main.*<footer class="overview-footer">.*</main>}s

    refute Map.has_key?(
             Dashboard.live_overview_payload(Dashboard.collect()).components,
             "top_bar"
           )
  end

  test "a clean snapshot says so in one line" do
    html = Overview.render_operator_attention(quiet_data())

    assert html =~ "Healthy"
    assert html =~ "No warnings in this snapshot"
    refute html =~ "Needs attention"
  end

  test "attention lists each problem with a way to act on it" do
    data =
      quiet_data()
      |> put_in([:overview, :status], :starting)
      |> put_in([:lifecycle, :keydir_full], true)
      |> Map.put(:flow_summary, %{failed: 0, expired_leases_sampled: 31})

    html = Overview.render_operator_attention(data)

    assert html =~ "Needs attention"
    assert html =~ ~s(<span class="overview-attention-count">3</span>)
    assert html =~ "31 sampled expired leases"
    assert html =~ ~s(href="/dashboard/flow/failures")
    assert html =~ "The node is still starting"
    assert html =~ "The keydir is full"
    refute html =~ "Healthy"
  end

  test "headline metrics keep exact counters for the browser-measured rates" do
    html = Overview.render_overview_metrics(quiet_data())

    assert html =~ "data-dashboard-recent-rates"
    assert html =~ ~s(data-commands="9007199254740993")
    assert html =~ ~s(data-recent-rate="commands")
    assert html =~ ~s(data-recent-spark="commands")
    assert html =~ "Waiting for a second sample"
    assert html =~ "since start"
    assert html =~ ~s(data-dashboard-disclosure-key="recent-rate-observations")
  end

  test "hit rate stays neutral until reads are sampled" do
    html = Overview.render_overview_metrics(quiet_data())
    assert html =~ "No read samples"

    sampled =
      quiet_data()
      |> Map.put(:hotcold, %{
        total_lookups: 10,
        total_hits: 9,
        total_misses: 1,
        total_cold: 0,
        hit_ratio: 90.0,
        ram_ratio: 100.0,
        disk_ratio: 0.0,
        sample_rate: 100
      })

    html = Overview.render_overview_metrics(sampled)
    assert html =~ ">90.0%<"
    assert html =~ "sampled 1:100"
  end

  test "keys show the total with what is on disk and what left" do
    html = Overview.render_overview_metrics(quiet_data())

    assert html =~ ">4,000<"
    assert html =~ "7.95 MB on disk"
    assert html =~ "3 expired"
    assert html =~ "0 evicted"
  end

  test "workflow card splits running runs whose lease expired" do
    html =
      Overview.render_overview_workflows(%{
        types: 8,
        active: 102,
        queued: 70,
        running: 31,
        failed: 1,
        expired_leases_sampled: 31,
        sampled_records: 102
      })

    assert html =~ ~s(href="/dashboard/flow")
    assert html =~ ">102<"
    assert html =~ "active runs across 8 types"
    assert html =~ ~s(style="flex: 70")
    assert html =~ ~s(style="flex: 31")
    refute html =~ ~s(style="flex: 0")
    assert html =~ "70 queued, 0 running, 31 with expired leases, 1 failed"
  end

  test "workflow card lists the busiest types with links to their runs" do
    html =
      Overview.render_overview_workflows(%{
        types: 2,
        active: 5,
        queued: 3,
        running: 2,
        failed: 1,
        sampled_records: 6,
        top_types: [
          %{type: "webhook.deliver", queued: 2, running: 2, failed: 1},
          %{type: "a<b", queued: 1, running: 0, failed: 0}
        ]
      })

    assert html =~ ~s(href="/dashboard/flow/states?type=webhook.deliver">webhook.deliver</a>)
    assert html =~ ~s(href="/dashboard/flow/states?type=a%3Cb">a&lt;b</a>)
    assert html =~ ~s(<th scope="col">Failed</th>)
  end

  test "the overview flow summary carries its busiest types" do
    summary = FerricstoreServer.Health.Dashboard.Flow.Sample.collect_summary()
    assert is_list(summary.top_types)
    assert length(summary.top_types) <= 5
  end

  test "workflow card has a plain empty state" do
    html = Overview.render_overview_workflows(%{active: 0, sampled_records: 0})
    assert html =~ "No active workflow runs in the sample."
    refute html =~ "overview-state-bar"
  end

  test "shards render as a grid that marks the ones that are down" do
    shards =
      for index <- 0..15 do
        %{
          index: index,
          status: if(index == 3, do: "down", else: "ok"),
          keys: 100 + index,
          ets_memory_bytes: 1_024,
          disk_bytes: 2_048
        }
      end

    html =
      Overview.render_overview_shards(shards, %{
        cluster_mode: :standalone,
        node_name: :test@localhost
      })

    assert length(Regex.scan(~r/class="overview-shard( is-down)?"/, html)) == 16
    assert html =~ ~s(<ol class="overview-shard-grid" style="--shard-columns: 8">)
    assert length(Regex.scan(~r/class="overview-shard is-down"/, html)) == 1
    assert html =~ "15 of 16 healthy"
    assert html =~ "test@localhost"
    assert html =~ "Shard 3: down"
    assert html =~ ~s(href="/dashboard/raft")
  end

  test "small cards summarise storage, keydir, clients and slow commands" do
    html = Overview.render_overview_cards(quiet_data())

    assert html =~ "7.95 MB"
    assert html =~ "<dt>Merges</dt><dd>idle</dd>"
    assert html =~ "1.0 KB of 256.0 KB"
    assert html =~ ~s(href="/dashboard/clients")
    assert html =~ "None logged since start"
  end

  defp quiet_data do
    %{
      generated_at_ms: 1_000,
      overview: %{
        status: :ok,
        total_keys: 4_000,
        total_commands: 9_007_199_254_740_993,
        uptime_seconds: 37,
        run_id: "run-1",
        version: "0.12.6"
      },
      hotcold: %{
        total_lookups: 0,
        total_hits: 0,
        total_misses: 0,
        total_cold: 0,
        hit_ratio: 0.0,
        ops_per_sec: 12.5,
        sample_rate: 100
      },
      memory: %{pressure_level: :ok, total_bytes: 100, max_bytes: 1_000, ratio: 0.1},
      connections: %{active: 2, blocked: 0, tracking: 0},
      cluster: %{cluster_mode: :standalone, node_name: :test@localhost},
      shards: [%{index: 0, status: "ok", keys: 4_000, ets_memory_bytes: 1, disk_bytes: 1}],
      lifecycle: %{
        expired_total: 3,
        evicted_total: 0,
        keydir_bytes: 1_024,
        keydir_max_ram: 262_144,
        keydir_ratio: 0.004,
        keydir_full: false
      },
      storage_summary: %{total_disk_bytes: 8_336_179},
      merge: [%{merging: false}],
      slowlog: [],
      flow_summary: %{failed: 0, expired_leases_sampled: 0},
      subsystem_health: %{policy_migration: %{status: :healthy, issues: []}}
    }
  end
end
