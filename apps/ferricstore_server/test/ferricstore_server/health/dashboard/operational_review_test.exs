defmodule FerricstoreServer.Health.Dashboard.OperationalReviewTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Data.Operational
  alias FerricstoreServer.Health.Dashboard.Flow.Projection, as: ProjectionData
  alias FerricstoreServer.Health.Dashboard.Render.{Admin, DoctorPages, MessagingPages, Overview}
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Projection

  test "projection status distinguishes recovered failures and absent telemetry" do
    metrics = [metric("lag", 0), metric("pending_ops", 0), metric("persist_failures", 7)]
    assert %{health: "healthy", failures: 7} = Projection.flow_projection_rollup(metrics)

    assert Projection.flow_projection_health_class(Projection.flow_projection_rollup(metrics)) ==
             "c-green"

    assert %{health: "unavailable"} = Projection.flow_projection_rollup([])

    assert %{health: "unavailable"} =
             Projection.flow_projection_rollup([metric("persist_failures", 7)])

    assert Projection.flow_projection_health_class(Projection.flow_projection_rollup([])) ==
             "c-muted"

    assert ProjectionData.default_health().lmdb_projection == :asynchronous
    html = Projection.render_flow_projection_health(%{metrics: metrics})
    assert html =~ "Historical failures"
    assert html =~ "since start"
    refute html =~ ~r{</dd>\s*<span>}
  end

  test "missing projection measurements do not appear as measured zero lag or failures" do
    html = Projection.render_flow_projection_health(%{metrics: []})
    assert html =~ ~r{<dt>Lag</dt>\s*<dd[^>]*>Unavailable}
    assert html =~ ~r{<dt>Pending</dt>\s*<dd[^>]*>Unavailable}
    assert html =~ ~r{<dt>Historical failures</dt>\s*<dd[^>]*>Unavailable}
  end

  test "memory snapshot retains RSS pressure and uses the governing budget in the top bar" do
    stats = %{
      total_bytes: 100,
      max_bytes: 1_000,
      ratio: 0.1,
      pressure_level: :pressure,
      eviction_policy: :volatile_lru,
      shards: %{},
      rss_bytes: 1_800,
      rss_ratio: 0.9,
      rss_pressure_level: :pressure,
      memory_limit: 2_000,
      keydir_bytes: 80,
      keydir_max_ram: 500
    }

    memory = Operational.memory_snapshot(stats)
    assert memory.rss_bytes == 1_800
    assert memory.memory_limit == 2_000
    html = Overview.render_top_bar(top_data(memory))
    assert html =~ "Process RSS"
    assert html =~ "1.76 KB / 1.95 KB"
    assert html =~ "width:90.0%"
    assert html =~ "Tracked allocations"
    assert html =~ "100 B / 1000 B"
    assert html =~ "Avg ops/sec"
    assert html =~ "since start"
  end

  test "missing RSS telemetry is explicitly unavailable instead of zero process memory" do
    html =
      Overview.render_top_bar(
        top_data(%{total_bytes: 100, max_bytes: 1_000, ratio: 0.1, pressure_level: :ok})
      )

    assert html =~ "Process RSS unavailable"
    assert html =~ "Tracked allocations"
  end

  test "consensus positions keep exact integers for one-entry lag" do
    html =
      Admin.render_raft_table([
        %{
          shard: 0,
          status: :ok,
          leader: nil,
          current_term: 1,
          commit_index: 2_701,
          last_applied: 2_700,
          members: []
        }
      ])

    assert html =~ ">2701</td>"
    assert html =~ ">2700</td>"
    refute html =~ "2.7K"
  end

  test "runtime parameter table shows effective values and their source without inventing defaults" do
    html =
      Admin.render_config_parameters([
        %{
          parameter: "slowlog-max-len",
          scope: "runtime",
          mutability: "read-write",
          notes: "entry cap",
          value: "256",
          source: "CONFIG GET"
        },
        %{parameter: "not-available", scope: "runtime", mutability: "read-only", notes: "missing"}
      ])

    assert html =~ "Effective value"
    assert html =~ "CONFIG GET"
    assert html =~ ">256</td>"
    assert html =~ "Unavailable"
    assert html =~ ~s(aria-label="Runtime parameters")
  end

  test "Config leads with effective parameters instead of the command reference" do
    html =
      FerricstoreServer.Health.Dashboard.render_config_page(%{
        namespace_config: [],
        config_parameters: [],
        config_commands: []
      })

    {values_offset, _} = :binary.match(html, "Runtime Parameters")
    {commands_offset, _} = :binary.match(html, "Configuration Commands")
    assert values_offset < commands_offset
  end

  test "Doctor uses shared labeled controls and actionable button classes" do
    html = DoctorPages.render_doctor_actions()
    assert html =~ ~s(class="flow-search-input" id="doctor-scope")
    assert html =~ ~s(class="flow-search-button")
    refute html =~ "flow-action-button"
    refute html =~ "flow-form-label"
  end

  test "Doctor action notices describe submission-time status rather than current job state" do
    alias FerricstoreServer.Health.Dashboard.DoctorSupport

    assert {:ok, message} =
             DoctorSupport.normalize_doctor_form_result(%{
               "job_id" => "job-1",
               "status" => "running"
             })

    assert message =~ "status at submission: running"
    refute message =~ "is running"

    assert {:error, "denied"} =
             DoctorSupport.normalize_doctor_form_result(%{
               "status" => "error",
               "error" => "denied"
             })
  end

  test "bounded operational tables advertise local filter targets and accessible status" do
    htmls = [
      Admin.render_clients_table([]),
      Admin.render_slowlog_table([]),
      MessagingPages.render_stream_top_streams(%{}),
      MessagingPages.render_stream_consumers(%{}),
      MessagingPages.render_stream_waiters(%{}),
      MessagingPages.render_stream_activity_log(%{}),
      MessagingPages.render_pubsub_channels(%{}),
      MessagingPages.render_pubsub_patterns(%{}),
      MessagingPages.render_pubsub_activity(%{})
    ]

    for html <- htmls do
      assert html =~ "data-dashboard-table-filter"
      assert html =~ "data-dashboard-filter-target"
      assert html =~ "data-dashboard-filter-status"
      assert html =~ "loaded"
      assert html =~ "role=\"status\""
    end
  end

  defp metric(field, value),
    do: %{name: ~s(ferricstore_flow_lmdb_#{field}{shard_index="0"}), value: to_string(value)}

  defp top_data(memory) do
    %{
      overview: %{status: :ok, total_keys: 0},
      hotcold: %{total_lookups: 0, hit_ratio: 0.0, ops_per_sec: 0.0, sample_rate: 100},
      memory: memory,
      connections: %{active: 1},
      cluster: %{cluster_mode: :standalone, node_name: :test@localhost}
    }
  end
end
