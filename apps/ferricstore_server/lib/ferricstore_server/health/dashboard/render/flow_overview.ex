defmodule FerricstoreServer.Health.Dashboard.Render.FlowOverview do

import FerricstoreServer.Health.Dashboard.Format
import FerricstoreServer.Health.Dashboard.QueryParams


  def render_flow_overview(summary, total_sampled, sample_limit) do
    """
    <div class="section-title">Flow Overview <span class="badge badge-idle">sampled #{format_number(total_sampled)} / #{format_number(sample_limit)}</span></div>
    <div class="flow-card-grid">
      #{render_flow_stat_card("Types", Map.get(summary, :types, 0), "discovered workflow types")}
      #{render_flow_stat_card("Active", Map.get(summary, :active, 0), "queued + running")}
      #{render_flow_stat_card("Queued", Map.get(summary, :queued, 0), "ready or scheduled")}
      #{render_flow_stat_card("Running", Map.get(summary, :running, 0), "leased by workers")}
      #{render_flow_stat_card("Failed", Map.get(summary, :failed, 0), "terminal failures")}
      #{render_flow_stat_card("Inflight", Map.get(summary, :inflight, 0), "index-backed lease count")}
    </div>
    """
  end

  def render_flow_stat_card(label, value, detail) do
    rendered_value =
      case value do
        value when is_binary(value) -> escape(value)
        value when is_integer(value) -> format_number(value)
        value -> escape(to_string(value))
      end

    """
    <div class="flow-card">
      <div class="flow-card-label">#{escape(label)}</div>
      <div class="flow-card-value">#{rendered_value}</div>
      <div class="flow-card-detail">#{escape(detail)}</div>
    </div>
    """
  end

  def render_flow_subnav(active) do
    groups = [
      {"Monitor",
       [
         {"overview", "/dashboard/flow", "Overview",
          "Flow summary, projection health, and recent records"},
         {"states", "/dashboard/flow/states", "States",
          "Filter Flow records by type, state, time, and ID"},
         {"workers", "/dashboard/flow/workers", "Workers", "Worker leases and running work"},
         {"due", "/dashboard/flow/due", "Due", "Claimable and expired Flow work"}
       ]},
      {"Debug",
       [
         {"failures", "/dashboard/flow/failures", "Failures",
          "Failed, stuck, and expired-lease recovery"},
         {"lineage", "/dashboard/flow/lineage", "Lineage",
          "Parent, root, and correlation queries"},
         {"query", "/dashboard/flow/query", "Query", "Bounded Flow query explorer"},
         {"signals", "/dashboard/flow/signals", "Signals", "Recent FLOW.SIGNAL events"}
       ]},
      {"Operate",
       [
         {"policies", "/dashboard/flow/policies", "Policies",
          "Retry and retention policy editor"},
         {"retention", "/dashboard/flow/retention", "Retention",
          "Terminal cleanup and disk-pressure maintenance"}
       ]}
    ]

    links =
      Enum.map_join(groups, "\n", fn {group_label, items} ->
        rendered_items =
          Enum.map_join(items, "\n", fn {key, href, label, title} ->
            active_class = if key == active, do: " active", else: ""
            current = if key == active, do: ~s( aria-current="page"), else: ""

            ~s(<a class="flow-tab#{active_class}" href="#{href}"#{current} title="#{escape_attr(title)}">#{escape(label)}</a>)
          end)

        """
        <div class="flow-tab-group">
          <span class="flow-tab-group-label">#{escape(group_label)}</span>
          <div class="flow-tab-group-links">#{rendered_items}</div>
        </div>
        """
      end)

    """
    <div class="flow-nav-row">
      <div class="flow-tabs">
        #{links}
      </div>
      <form class="flow-search" action="/dashboard/flow/lookup" method="get" aria-label="Flow lookup">
        <input class="flow-search-input mono" type="search" name="id" placeholder="Search flow ID" autocomplete="off" aria-label="Flow ID" title="Open a flow by ID.">
        <input class="flow-search-input mono" type="search" name="partition_key" placeholder="Partition key" autocomplete="off" aria-label="Partition key" title="With a Flow ID, scopes the detail lookup. Without a Flow ID, filters the overview to this partition.">
        <button class="flow-search-button" type="submit" title="Open a flow by ID or filter overview by partition">Search</button>
      </form>
    </div>
    """
  end

  def render_flow_failures_flash(%{flash: %{kind: :ok, message: message}}),
    do: ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)

  def render_flow_failures_flash(%{flash: %{kind: :error, message: message}}),
    do: ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)

  def render_flow_failures_flash(_data), do: ""

  def render_flow_exact_scan_status(data) do
    filters = flow_failures_page_filters(data)

    if Map.get(filters, :scan_exact, false) do
      status = Map.get(data, :exact_scan_status, %{failures: :skipped, stuck: :skipped})

      status
      |> Enum.flat_map(fn {source, source_status} ->
        case source_status do
          {:error, reason} ->
            [
              """
              <div class="flow-alert flow-alert-error">
                Exact scan issue: #{flow_recovery_source_command(source)} failed with #{escape(inspect(reason, limit: 8))}. Sampled rows are still shown; zero candidates is not authoritative.
              </div>
              """
            ]

          _ ->
            []
        end
      end)
      |> Enum.join("")
    else
      ""
    end
  end

  def flow_recovery_source_command(:failures), do: "FLOW.FAILURES"
  def flow_recovery_source_command(:stuck), do: "FLOW.STUCK"
  def flow_recovery_source_command(source), do: source |> to_string() |> String.upcase()

end
