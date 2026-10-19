defmodule FerricstoreServer.Health.Dashboard.Render.FlowComponents do
  require EEx

  alias FerricstoreServer.Health.Dashboard.Flow.Recovery

  import FerricstoreServer.Health.Dashboard.Format

  import FerricstoreServer.Health.Dashboard.FlowRecord,
    only: [
      flow_expired_lease?: 1,
      flow_record_attempts: 1,
      flow_record_id: 1,
      flow_record_lease_expires_at_ms: 1,
      flow_record_max_attempts: 1,
      flow_record_partition_key: 1,
      flow_record_type: 1,
      flow_record_updated_at_ms: 1,
      flow_record_worker: 1,
      flow_recovery_reason: 1
    ]

  import FerricstoreServer.Health.Dashboard.Render.FlowHistory,
    only: [render_flow_id_link: 2, render_flow_value_ref_badges: 2]

  import FerricstoreServer.Health.Dashboard.Render.FlowFilters,
    only: [render_flow_type_datalist: 2]

  import FerricstoreServer.Health.Dashboard.Render.FlowQueryPolicy
  import FerricstoreServer.Health.Dashboard.Render.FlowTables

  @templates_dir Path.expand("../templates", __DIR__)

  EEx.function_from_file(
    :defp,
    :component_flow_failures_controls,
    Path.join(@templates_dir, "flow_failures_controls.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_recovery_actions,
    Path.join(@templates_dir, "flow_recovery_actions.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_failures_summary,
    Path.join(@templates_dir, "flow_failures_summary.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_failures_table,
    Path.join(@templates_dir, "flow_failures_table.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_lineage_controls,
    Path.join(@templates_dir, "flow_lineage_controls.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_lineage_summary,
    Path.join(@templates_dir, "flow_lineage_summary.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_lineage_graph,
    Path.join(@templates_dir, "flow_lineage_graph.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_lineage_table,
    Path.join(@templates_dir, "flow_lineage_table.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_query_controls,
    Path.join(@templates_dir, "flow_query_controls.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_query_result,
    Path.join(@templates_dir, "flow_query_result.html.eex"),
    [:assigns]
  )

  def render_flow_failures_controls(data), do: component_flow_failures_controls(%{data: data})
  def render_flow_recovery_actions(data), do: component_flow_recovery_actions(%{data: data})
  def render_flow_failures_summary(data), do: component_flow_failures_summary(%{data: data})
  def render_flow_failures_table(data), do: component_flow_failures_table(%{data: data})
  def render_flow_lineage_controls(data), do: component_flow_lineage_controls(%{data: data})
  def render_flow_lineage_summary(data), do: component_flow_lineage_summary(%{data: data})
  def render_flow_lineage_graph(data), do: component_flow_lineage_graph(%{data: data})
  def render_flow_lineage_table(data), do: component_flow_lineage_table(%{data: data})
  def render_flow_query_controls(data), do: component_flow_query_controls(%{data: data})
  def render_flow_query_result(data), do: component_flow_query_result(%{data: data})

  defp flow_failure_groups(data) do
    data
    |> Map.get(:candidates, [])
    |> Enum.split_with(&flow_expired_lease?/1)
  end

  # Expired leases by type and partition, the largest group first. Each group
  # opens to its runs and a reclaim limited to its own type and partition.
  defp render_expired_lease_groups([], _data),
    do: ~s(<p class="failure-empty">No expired leases in this sample.</p>)

  defp render_expired_lease_groups(records, data) do
    scopes = Map.new(Recovery.reclaim_groups(data), &{{&1.type, &1.partition_key}, &1})
    filters = Recovery.page_filters(data)

    groups =
      records
      |> Enum.group_by(&{flow_record_type(&1), flow_record_partition_key(&1)})
      |> Enum.sort_by(fn {{type, partition}, rows} -> {-length(rows), type, partition || ""} end)
      |> Enum.map_join("", fn {{type, partition} = key, rows} ->
        render_expired_lease_group(type, partition, rows, Map.get(scopes, key), filters)
      end)

    ~s(<div class="failure-groups">#{groups}</div>)
  end

  defp render_expired_lease_group(type, partition, rows, scope, filters) do
    ended =
      rows
      |> Enum.map(&flow_record_lease_expires_at_ms/1)
      |> Enum.filter(&is_integer/1)
      |> Enum.min(fn -> nil end)

    workers =
      rows
      |> Enum.map(&flow_record_worker/1)
      |> Enum.reject(&(&1 in [nil, ""]))
      |> Enum.uniq()

    worker =
      case workers do
        [] -> "no worker recorded"
        [one] -> one
        [one | rest] -> "#{one} +#{length(rest)}"
      end

    run_rows =
      Enum.map_join(rows, "", fn record ->
        """
        <tr>
          <td class="mono">#{render_flow_id_link(flow_record_id(record), partition)}</td>
          <td>#{render_attempts(record)}</td>
          <td class="mono">#{escape(flow_record_worker(record) || "-")}</td>
          <td>#{format_clock_ms(flow_record_lease_expires_at_ms(record))}</td>
          <td>#{format_clock_ms(flow_record_updated_at_ms(record))}</td>
        </tr>
        """
      end)

    """
    <details class="failure-group">
      <summary>
        <span class="failure-group-scope"><span class="mono">#{escape(type)}</span><span class="failure-group-partition mono">#{escape(partition || "no partition")}</span></span>
        <span class="failure-group-count">#{length(rows)}</span>
        <span class="failure-group-since">#{if ended, do: "lease ended " <> format_clock_ms(ended), else: "lease end unknown"}</span>
        <span class="failure-group-worker mono">#{escape(worker)}</span>
      </summary>
      <div class="failure-group-body">
        <div class="table-scroll" role="region" aria-label="Expired leases in #{escape_attr(type)} #{escape_attr(partition || "")}" tabindex="0">
          <table class="failure-runs">
            <thead><tr><th scope="col">Run</th><th scope="col">Attempts</th><th scope="col">Worker</th><th scope="col">Lease ended</th><th scope="col">Last update</th></tr></thead>
            <tbody>#{run_rows}</tbody>
          </table>
        </div>
        #{render_group_reclaim(type, partition, scope, filters)}
      </div>
    </details>
    """
  end

  defp render_group_reclaim(type, partition, %{can_prepare?: true}, filters) do
    """
    <form class="failure-reclaim" action="/dashboard/flow/failures" method="post" data-dashboard-single-submit>
      <input type="hidden" name="action" value="reclaim">
      <input type="hidden" name="type" value="#{escape_attr(type)}">
      <input type="hidden" name="partition_key" value="#{escape_attr(partition)}">
      <input type="hidden" name="return_q" value="#{escape_attr(Map.get(filters, :q) || "")}">
      <input type="hidden" name="return_limit" value="#{Map.get(filters, :limit)}">
      <input type="hidden" name="return_exact" value="#{if Map.get(filters, :scan_exact, false), do: "true", else: "false"}">
      <p class="failure-reclaim-scope">Reclaim up to 25 expired leases in #{escape(type)} · #{escape(partition)}, including ones this sample does not show. Their current workers can no longer complete them.</p>
      <details class="failure-reclaim-options">
        <summary>Options</summary>
        <div class="flow-policy-grid">
          <label class="flow-policy-field"><span>Worker</span><input class="flow-search-input mono" name="worker" value="dashboard-recovery" title="Lease owner recorded for reclaimed work"></label>
          <label class="flow-policy-field"><span>Limit</span><input class="flow-search-input mono" type="number" min="1" max="200" name="limit" value="25" title="Maximum expired leases to reclaim"></label>
          <label class="flow-policy-field"><span>Lease ms</span><input class="flow-search-input mono" type="number" min="1" name="lease_ms" value="30000" title="New lease duration for reclaimed work"></label>
        </div>
      </details>
      <div class="failure-reclaim-actions">
        <label class="flow-check-label"><input type="checkbox" name="confirm_reclaim" value="true" required> I understand this moves these leases to a new owner.</label>
        <button class="flow-search-button flow-danger-button" type="submit">Reclaim expired leases</button>
      </div>
    </form>
    """
  end

  defp render_group_reclaim(type, partition, _scope, _filters) do
    reason =
      if type in [nil, ""] or partition in [nil, ""],
        do:
          "Reclaim needs an exact partition. Use Reclaim by type and partition below to reclaim across automatic partitions.",
        else: "Your account cannot reclaim leases in this partition."

    ~s(<p class="failure-reclaim-scope c-muted">#{escape(reason)}</p>)
  end

  defp render_attempts(record) do
    attempts = flow_record_attempts(record)

    case flow_record_max_attempts(record) do
      max when is_integer(max) and max > 0 ->
        "#{format_number(attempts)} of #{format_number(max)}"

      _ ->
        format_number(attempts)
    end
  end

  defp render_terminal_failure_rows([]),
    do: ~s(<p class="failure-empty">No failed or out-of-retry runs in this sample.</p>)

  defp render_terminal_failure_rows(records) do
    rows =
      Enum.map_join(records, "", fn record ->
        partition = flow_record_partition_key(record)

        """
        <tr>
          <td class="mono">#{render_flow_id_link(flow_record_id(record), partition)}<span class="flow-run-secondary mono">#{escape(partition || "auto/global")}</span></td>
          <td class="mono">#{escape(flow_record_type(record))}</td>
          <td>#{escape(flow_recovery_reason(record))}</td>
          <td>#{render_attempts(record)}</td>
          <td>#{format_clock_ms(flow_record_updated_at_ms(record))}</td>
          <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
        </tr>
        """
      end)

    """
    <div class="table-scroll" role="region" aria-label="Failed and retry-exhausted workflows" tabindex="0">
      <table class="failure-runs failure-terminal">
        <thead><tr><th scope="col">Run</th><th scope="col">Type</th><th scope="col">Reason</th><th scope="col">Attempts</th><th scope="col">Last update</th><th scope="col">Values</th></tr></thead>
        <tbody>#{rows}</tbody>
      </table>
    </div>
    """
  end

  defp flow_failures_page_filters(data), do: Recovery.page_filters(data)
end
