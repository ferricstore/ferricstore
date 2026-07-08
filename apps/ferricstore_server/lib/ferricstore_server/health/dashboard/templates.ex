defmodule FerricstoreServer.Health.Dashboard.Templates do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard.Flow.{Browse, Projection, Query}

  require EEx

  import FerricstoreServer.Health.Dashboard.Layout
  import FerricstoreServer.Health.Dashboard.Render.Admin
  import FerricstoreServer.Health.Dashboard.Render.Capabilities
  import FerricstoreServer.Health.Dashboard.Render.DoctorPages
  import FerricstoreServer.Health.Dashboard.Render.FlowCharts
  import FerricstoreServer.Health.Dashboard.Render.FlowComponents
  import FerricstoreServer.Health.Dashboard.Render.FlowDetail
  import FerricstoreServer.Health.Dashboard.Render.FlowFilters
  import FerricstoreServer.Health.Dashboard.Render.FlowGovernance
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory, except: [flow_signal_rows: 2]
  import FerricstoreServer.Health.Dashboard.Render.FlowOverview
  import FerricstoreServer.Health.Dashboard.Render.FlowSchedules

  import FerricstoreServer.Health.Dashboard.Render.FlowQueryPolicy,
    except: [flow_policy_editor_data: 1]

  import FerricstoreServer.Health.Dashboard.Render.FlowTables,
    except: [default_flow_projection_health: 0]

  import FerricstoreServer.Health.Dashboard.Render.KVPages, except: [kv_command_groups: 0]
  import FerricstoreServer.Health.Dashboard.Render.MessagingPages
  import FerricstoreServer.Health.Dashboard.Render.Overview
  import FerricstoreServer.Health.Dashboard.Render.Prefixes
  import FerricstoreServer.Health.Dashboard.Render.Security

  @templates_dir Path.expand("templates", __DIR__)

  EEx.function_from_file(:def, :overview, Path.join(@templates_dir, "overview.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :slowlog, Path.join(@templates_dir, "slowlog.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :merge, Path.join(@templates_dir, "merge.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :config, Path.join(@templates_dir, "config.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(
    :def,
    :capabilities,
    Path.join(@templates_dir, "capabilities.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(:def, :security, Path.join(@templates_dir, "security.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :raft, Path.join(@templates_dir, "raft.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :clients, Path.join(@templates_dir, "clients.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :storage, Path.join(@templates_dir, "storage.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :doctor, Path.join(@templates_dir, "doctor.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :prefixes, Path.join(@templates_dir, "prefixes.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :keyspace, Path.join(@templates_dir, "keyspace.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :commands, Path.join(@templates_dir, "commands.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :reads, Path.join(@templates_dir, "reads.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :streams, Path.join(@templates_dir, "streams.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :pubsub, Path.join(@templates_dir, "pubsub.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :flow, Path.join(@templates_dir, "flow.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:def, :flow_states, Path.join(@templates_dir, "flow_states.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(
    :def,
    :flow_workers,
    Path.join(@templates_dir, "flow_workers.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(:def, :flow_due, Path.join(@templates_dir, "flow_due.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(
    :def,
    :flow_schedules,
    Path.join(@templates_dir, "flow_schedules.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(
    :def,
    :flow_governance,
    Path.join(@templates_dir, "flow_governance.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(
    :def,
    :flow_failures,
    Path.join(@templates_dir, "flow_failures.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(
    :def,
    :flow_lineage,
    Path.join(@templates_dir, "flow_lineage.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(:def, :flow_query, Path.join(@templates_dir, "flow_query.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(
    :def,
    :flow_signals,
    Path.join(@templates_dir, "flow_signals.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(
    :def,
    :flow_policies,
    Path.join(@templates_dir, "flow_policies.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(
    :def,
    :flow_retention,
    Path.join(@templates_dir, "flow_retention.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(:def, :flow_detail, Path.join(@templates_dir, "flow_detail.html.eex"), [
    :assigns
  ])

  defp render_overview_content(data) do
    """
    #{render_cache_performance(data.hotcold)}
    #{render_lifecycle(data.lifecycle)}
    #{render_shards(data.shards)}
    #{render_memory_alert(data.memory)}
    #{render_connections(data.connections)}
    """
  end

  defp default_flow_projection_health, do: Projection.default_health()

  defp flow_page_filters(data), do: Browse.overview_page_filters(data)

  defp flow_states_page_filters(data), do: Browse.states_page_filters(data)

  defp flow_states_page_limit(data), do: Browse.states_page_limit(data)

  defp flow_signals_page_filters(data), do: Query.signals_page_filters(data)
end
