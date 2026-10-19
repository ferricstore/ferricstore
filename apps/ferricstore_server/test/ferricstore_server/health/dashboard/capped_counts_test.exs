defmodule FerricstoreServer.Health.Dashboard.CappedCountsTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Data.Operational
  alias FerricstoreServer.Health.Dashboard.Flow.Browse
  alias FerricstoreServer.Health.Dashboard.Render.{Admin, Prefixes}
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records

  test "the Due page counts every scheduled run, though it lists the earliest 80" do
    type = "capped-due-#{System.unique_integer([:positive])}"
    later = System.system_time(:millisecond) + 3_600_000

    for n <- 1..81 do
      :ok =
        FerricStore.flow_create("#{type}-#{n}",
          type: type,
          state: "queued",
          partition_key: "p",
          run_at_ms: later + n
        )
    end

    data = Browse.collect_due_page(type: type)
    assert length(data.scheduled) == 80
    assert data.scheduled_sampled == 81

    summary =
      Records.render_flow_due_summary(data.due_now, data.scheduled, data.scheduled_sampled)

    assert summary =~ "81 scheduled, next in"

    section =
      Records.render_flow_due_records(
        "Scheduled Future",
        data.scheduled,
        data.total_sampled,
        data.sample_limit,
        data.scheduled_sampled
      )

    assert section =~ ~s(<span class="badge badge-idle">80 of 81</span>)
    assert section =~ "Earliest 80 shown"
  end

  test "Prefixes counts every prefix in the sample, though it lists the largest 50" do
    rows =
      for n <- 1..50,
          do: %{prefix: "p#{n}", keys: 100 - n, pct: 1.0, hot_reads: nil, cold_reads: nil}

    html =
      Prefixes.render_prefixes_summary(%{prefixes: rows, prefix_count: 70, total_sampled: 4_000})

    assert html =~ "<strong>70 prefixes in 4.0K sampled keys</strong>"
    assert html =~ "largest 50 shown"
  end

  test "a full slow log window reads as the most recent entries, not a total" do
    limit = Operational.slowlog_limit()

    entries =
      for n <- 1..limit,
          do: %{id: n, timestamp_us: 1_000_000, duration_us: 20_000, command: ["GET", "k"]}

    assert Admin.render_slowlog_summary(entries) =~
             "<strong>#{limit} most recent slow commands</strong>"

    assert Admin.render_slowlog_summary(Enum.take(entries, 3)) =~
             "<strong>3 slow commands recorded</strong>"
  end
end
