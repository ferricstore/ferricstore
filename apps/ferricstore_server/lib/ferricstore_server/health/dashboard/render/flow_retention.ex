defmodule FerricstoreServer.Health.Dashboard.Render.FlowRetention do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Render.Admin, only: [render_config_command_table: 2]
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory, only: [flow_detail_path: 2]
  import FerricstoreServer.Health.Dashboard.Render.FlowOverview, only: [render_flow_stat_card: 3]

  import FerricstoreServer.Health.Dashboard.Render.FlowTables,
    only: [default_flow_projection_health: 0]

  @flow_dashboard_sample_limit 400
  @flow_dashboard_retention_default_limit 100
  @flow_dashboard_retention_max_limit 10_000

  def render_flow_retention_summary(data) do
    storage = Map.get(data, :storage, %{})
    projection = Map.get(data, :projection, default_flow_projection_health())

    metrics =
      case Map.get(projection, :metrics, %{}) do
        metric_map when is_map(metric_map) -> metric_map
        _ -> %{}
      end

    pending =
      case Map.get(metrics, :lmdb_pending, Map.get(metrics, "lmdb_pending", 0)) do
        value when is_integer(value) and value >= 0 -> value
        _ -> 0
      end

    global_metrics =
      if Map.get(storage, :restricted, false) or Map.get(projection, :restricted, false) do
        ""
      else
        """
        #{render_flow_stat_card("Disk", format_bytes(Map.get(storage, :total_disk_bytes, 0)), "current data directory footprint")}
        #{render_flow_stat_card("Query Index Lag", pending, "cold query-index work pending before cleanup")}
        """
      end

    """
    <div class="section-title">Sample Preview <span class="badge badge-idle">sampled #{format_number(Map.get(data, :total_sampled, 0))} / #{format_number(Map.get(data, :sample_limit, @flow_dashboard_sample_limit))}</span></div>
    <div class="flow-card-grid">
      #{render_flow_stat_card("Active Timeouts", Map.get(data, :active_timeout_eligible_sampled, 0), "overdue active Flow records that cleanup will fail")}
      #{render_flow_stat_card("Terminal Deletes", Map.get(data, :terminal_eligible_sampled, 0), "expired terminal Flow records that cleanup will remove")}
      #{render_flow_stat_card("Active Sample", Map.get(data, :active_sampled, 0), "all non-terminal Flow records in the sample")}
      #{global_metrics}
    </div>
    """
  end

  def render_flow_retention_controls(data) do
    limit = Map.get(data, :limit, @flow_dashboard_retention_default_limit)
    flash = render_flow_retention_flash(Map.get(data, :flash))

    """
    <div id="flow-retention-maintenance" class="flow-policy-panel">
      <div class="section-title">Retention Cleanup #{info_icon("Fails overdue active Flow records whose max active runtime expired, then deletes terminal Flow data whose retention TTL expired.")}</div>
      #{flash}
      <div class="pressure-alert level-warning">
        <div class="pressure-details">
          Dry Run only previews sampled eligible records. Run Cleanup fails overdue active Flow records before deleting expired terminal Flow records, and executes globally with the supplied per-shard limit. A sampled preview of zero is not proof that global cleanup will change zero records.
        </div>
      </div>
      <form class="flow-policy-form" action="/dashboard/flow/retention" method="post">
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Limit</span>
            <input class="flow-search-input mono" type="number" name="limit" min="1" max="#{@flow_dashboard_retention_max_limit}" value="#{limit}" required title="Maximum active timeouts and terminal deletions to process per shard">
          </label>
        </div>
        <label class="flow-check-label" title="Required before the destructive cleanup command is accepted.">
          <input type="checkbox" name="confirm_cleanup" value="true">
          I understand cleanup is global and may fail active Flow records whose max active runtime expired.
        </label>
        <div class="flow-filter-note">Requires +FLOW.RETENTION_CLEANUP. Use Dry Run first; cleanup is intentionally separate from the sampled preview.</div>
        <div class="flow-policy-actions">
          <button class="flow-search-button" type="submit" name="action" value="dry_run" title="Preview active timeouts and terminal deletions without changing data">Dry Run</button>
          <button class="flow-search-button flow-danger-button" type="submit" name="action" value="cleanup" title="Fail overdue active records and run durable terminal cleanup now">Run Cleanup</button>
        </div>
      </form>
    </div>
    """
  end

  def render_flow_retention_flash(%{kind: :dry_run, limit: limit}) do
    ~s(<div class="flow-alert flow-alert-ok">Dry run ready for limit #{format_number(limit)}. No data was removed.</div>)
  end

  def render_flow_retention_flash(%{kind: :ok, counts: counts, limit: limit}) do
    message =
      "Cleanup completed: #{format_number(Map.get(counts, :active_timeouts, 0))} active flows timed out, " <>
        "#{format_number(Map.get(counts, :flows, 0))} terminal flows, " <>
        "#{format_number(Map.get(counts, :history, 0))} history rows, " <>
        "#{format_number(Map.get(counts, :values, 0))} values removed (limit #{format_number(limit)})."

    ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)
  end

  def render_flow_retention_flash(%{kind: :error, message: message}) do
    ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)
  end

  def render_flow_retention_flash(_flash), do: ""

  def render_flow_retention_commands do
    render_config_command_table("Flow Retention Commands", flow_retention_command_reference())
  end

  def flow_retention_command_reference do
    [
      %{
        command: "FLOW.RETENTION_CLEANUP [LIMIT <n>]",
        scope: "Flow data",
        mutability: "read-write",
        notes:
          "Fails active Flow records past max_active_ms, then deletes expired terminal records, durable history, generated values, and shared value links under one limit."
      },
      %{
        command: "FLOW.POLICY.SET <type> RETENTION_TTL_MS <ms>",
        scope: "Flow type",
        mutability: "read-write",
        notes:
          "Sets how long terminal Flow data is retained before cleanup is allowed to remove it."
      }
    ]
  end

  def render_flow_retention_candidates(data) do
    candidates = Map.get(data, :candidates, [])
    active_timeout_candidates = Map.get(data, :active_timeout_candidates, [])
    now_ms = Map.get(data, :now_ms, System.system_time(:millisecond))

    render_flow_active_timeout_candidates(active_timeout_candidates, now_ms) <>
      render_flow_terminal_retention_candidates(candidates, now_ms)
  end

  defp render_flow_active_timeout_candidates(candidates, now_ms) do
    rows =
      case candidates do
        [] ->
          """
          <tr>
            <td colspan="8" class="c-muted">No overdue active Flow records found in the dashboard sample.</td>
          </tr>
          """

        _ ->
          Enum.map_join(candidates, "\n", &render_flow_active_timeout_candidate_row(&1, now_ms))
      end

    """
    <div class="section-title">Sampled Active Timeouts <span class="badge badge-idle">#{format_number(length(candidates))}</span></div>
    <table>
      <thead>
        <tr>
          <th>Flow</th>
          <th>Type</th>
          <th>State</th>
          <th>Partition</th>
          <th>Max Active</th>
          <th>Timeout At</th>
          <th>Overdue For</th>
          <th>Created</th>
        </tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  defp render_flow_terminal_retention_candidates(candidates, now_ms) do
    rows =
      case candidates do
        [] ->
          """
          <tr>
            <td colspan="8" class="c-muted">No expired terminal Flow records found in the dashboard sample.</td>
          </tr>
          """

        _ ->
          Enum.map_join(candidates, "\n", &render_flow_retention_candidate_row(&1, now_ms))
      end

    """
    <div class="section-title">Sampled Terminal Deletions <span class="badge badge-idle">#{format_number(length(candidates))}</span></div>
    <table>
      <thead>
        <tr>
          <th>Flow</th>
          <th>Type</th>
          <th>State</th>
          <th>Partition</th>
          <th>Attempts</th>
          <th>Retention Until</th>
          <th>Expired For</th>
          <th>Updated</th>
        </tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  defp render_flow_active_timeout_candidate_row(record, now_ms) do
    id = flow_record_id(record)
    partition_key = flow_record_partition_key(record)
    created_at_ms = flow_record_created_at_ms(record)
    max_active_ms = flow_first_integer(record, [:max_active_ms]) || 0
    timeout_at_ms = created_at_ms + max_active_ms
    overdue_for_ms = max(now_ms - timeout_at_ms, 0)
    href = flow_detail_path(id, flow_detail_url_partition_key(partition_key))

    """
    <tr>
      <td><a class="mono" href="#{href}">#{escape(id)}</a></td>
      <td class="mono">#{escape(flow_record_type(record))}</td>
      <td><span class="flow-pill">#{escape(flow_record_state(record))}</span></td>
      <td class="mono">#{escape(partition_key || "-")}</td>
      <td>#{format_duration_ms(max_active_ms)}</td>
      <td>#{format_timestamp_ms_or_dash(timeout_at_ms)}</td>
      <td>#{format_duration_ms(overdue_for_ms)}</td>
      <td>#{format_timestamp_ms_or_dash(created_at_ms)}</td>
    </tr>
    """
  end

  def render_flow_retention_candidate_row(record, now_ms) do
    id = flow_record_id(record)
    partition_key = flow_record_partition_key(record)
    retention_until = flow_retention_until_ms(record)
    expired_for = if is_integer(retention_until), do: max(now_ms - retention_until, 0), else: 0
    href = flow_detail_path(id, flow_detail_url_partition_key(partition_key))

    """
    <tr>
      <td><a class="mono" href="#{href}">#{escape(id)}</a></td>
      <td class="mono">#{escape(flow_record_type(record))}</td>
      <td><span class="flow-pill flow-pill-terminal">#{escape(flow_record_state(record))}</span></td>
      <td class="mono">#{escape(partition_key || "-")}</td>
      <td>#{format_number(flow_record_attempts(record))}</td>
      <td>#{format_timestamp_ms_or_dash(retention_until)}</td>
      <td>#{format_duration_ms(expired_for)}</td>
      <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
    </tr>
    """
  end
end
