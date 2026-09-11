defmodule FerricstoreServer.Health.Dashboard.Render.FlowRetention do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Render.Admin, only: [render_config_command_table: 2]
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory, only: [flow_detail_path: 2]

  import FerricstoreServer.Health.Dashboard.Render.FlowOverview,
    only: [render_flow_summary_metric: 3]

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
        #{render_flow_summary_metric("Disk", format_bytes(Map.get(storage, :total_disk_bytes, 0)), "current data directory footprint")}
        #{render_flow_summary_metric("Pending index operations", pending, "operations queued for the cold query-index writer")}
        """
      end

    """
    <h2 class="section-title">Sample Preview <span class="badge badge-idle">#{sampled_scan_label(Map.get(data, :total_sampled, 0), Map.get(data, :sample_limit, @flow_dashboard_sample_limit))}</span></h2>
    <dl class="flow-overview-ribbon" aria-label="Retention sample metrics">
      #{render_flow_summary_metric("Active Timeouts", Map.get(data, :active_timeout_eligible_sampled, 0), "sampled candidates for runtime enforcement")}
      #{render_flow_summary_metric("Terminal Deletes", Map.get(data, :terminal_eligible_sampled, 0), "sampled candidates for terminal deletion")}
      #{render_flow_summary_metric("Active Sample", Map.get(data, :active_sampled, 0), "non-terminal records in this sample")}
      #{global_metrics}
    </dl>
    """
  end

  def render_flow_retention_controls(data) do
    limit = Map.get(data, :limit, @flow_dashboard_retention_default_limit)
    result = Map.get(data, :flash)
    flash = render_flow_retention_flash(result)
    raw = Map.get(data, :limit_input, limit)
    raw = if raw == "" and not match?(%{kind: :error}, result), do: limit, else: raw
    raw = to_string(raw)
    input_type = if Regex.match?(~r/^\d+$/, raw), do: "number", else: "text"
    cleanup? = get_in(data, [:action_capabilities, :cleanup]) != false

    """
    <div id="flow-retention-maintenance" class="flow-retention-controls">
      <h2 class="section-title">Retention Cleanup #{info_icon("Fails overdue active Flow records whose max active runtime expired, then deletes terminal Flow data whose retention TTL expired, under one shared global record limit.", "About retention cleanup")}</h2>
      #{flash}
      <p class="flow-section-note">The bounded sample does not simulate global cleanup. Active timeouts consume the shared limit before terminal deletions. A sampled count of zero is not proof that global cleanup will change zero records.</p>
      <form class="flow-policy-form" action="/dashboard/flow/retention" method="post" data-dashboard-single-submit>
          <label class="flow-policy-field flow-retention-limit">
            <span>Global record limit</span>
            <input class="flow-search-input mono" type="#{input_type}" inputmode="numeric" name="limit" min="1" max="#{@flow_dashboard_retention_max_limit}" value="#{escape_attr(raw)}" required title="Maximum active timeouts and terminal deletions combined across all shards">
          </label>
        <div class="flow-policy-actions">
          <button class="flow-search-button" type="submit" name="action" value="dry_run" title="Refresh bounded sampled candidates without simulating cleanup or changing data">Refresh sampled preview</button>
          #{if cleanup?, do: ~s(<button class="flow-search-button" type="submit" name="action" value="review_cleanup">Review global cleanup</button>), else: ~s(<span class="flow-section-note">Cleanup unavailable: requires +FLOW.RETENTION_CLEANUP and global write access.</span>)}
        </div>
      </form>
      #{if cleanup?, do: render_global_cleanup_review(result), else: ""}
    </div>
    """
  end

  defp render_global_cleanup_review(%{kind: :review, limit: limit} = review) do
    """
    <section class="flow-management-group" aria-label="Global cleanup review">
      <h3>Global cleanup review</h3>
      <dl class="flow-definition-list">
        <dt>Command</dt><dd><code>FLOW.RETENTION_CLEANUP LIMIT #{limit}</code></dd>
        <dt>Scope</dt><dd>All shards, all workflow types and partitions in this instance.</dd>
        <dt>Record budget</dt><dd>At most #{limit} active timeouts and terminal deletions combined. Active timeouts consume the shared limit before terminal deletions.</dd>
        <dt>Impact unknown</dt><dd>No command-equivalent dry run is available. Sampled candidates are separate evidence, not an execution plan. Durable history and value removals depend on the selected records; their count is unknown.</dd>
        <dt>Execution</dt><dd>Eligibility is evaluated at execution time. Backend key and byte budgets may stop this run earlier. No continuation is submitted automatically.</dd>
      </dl>
      <form action="/dashboard/flow/retention" method="post" data-dashboard-single-submit>
        <input type="hidden" name="action" value="cleanup">
        <input type="hidden" name="limit" value="#{limit}">
        <input type="hidden" name="reviewed_limit" value="#{escape_attr(to_string(Map.get(review, :reviewed_limit, limit)))}">
        <input type="hidden" name="reviewed_at_ms" value="#{escape_attr(to_string(Map.get(review, :reviewed_at_ms, "")))}">
        <label class="flow-check-label"><input type="checkbox" name="confirm_cleanup" value="true" required aria-describedby="retention-global-confirmation">Confirm global cleanup with a shared limit of #{limit} records.</label>
        <p class="flow-section-note" id="retention-global-confirmation">Overdue active records may be failed; expired terminal records and their retained data may be permanently deleted. This review expires after five minutes.</p>
        <button class="flow-search-button flow-danger-button" type="submit">Run Cleanup</button>
      </form>
    </section>
    """
  end

  defp render_global_cleanup_review(_result), do: ""

  def render_flow_retention_flash(%{kind: :dry_run}) do
    ~s(<div class="flow-alert" role="status">Sampled preview refreshed. It does not apply the global cleanup limit or simulate global cleanup. No data was changed.</div>)
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
    ~s(<div class="flow-alert flow-alert-error" role="alert">#{escape(message)}. The submitted limit is retained; review and confirm the global operation again before retrying.</div>)
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

    render_flow_active_timeout_candidates(
      active_timeout_candidates,
      now_ms,
      Map.get(data, :active_timeout_eligible_sampled, length(active_timeout_candidates))
    ) <>
      render_flow_terminal_retention_candidates(
        candidates,
        now_ms,
        Map.get(data, :terminal_eligible_sampled, length(candidates))
      )
  end

  defp render_flow_active_timeout_candidates(candidates, now_ms, total) do
    rows =
      case candidates do
        [] ->
          """
          <tr>
            <td colspan="8" class="c-muted">#{if total > 0, do: "Sampled active candidates were omitted by the display limit.", else: "No overdue active Flow records found in the dashboard sample."}</td>
          </tr>
          """

        _ ->
          Enum.map_join(candidates, "\n", &render_flow_active_timeout_candidate_row(&1, now_ms))
      end

    """
    <h2 class="section-title">Sampled Active Timeouts <span class="badge badge-idle">#{format_number(length(candidates))}</span></h2>
    #{candidate_coverage(candidates, total)}
    <div class="table-scroll" role="region" aria-label="Sampled active workflow timeouts" tabindex="0"><table>
      <thead>
        <tr>
          <th>Flow</th>
          <th>Type</th>
          <th>Runtime status</th>
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
    </table></div>
    """
  end

  defp render_flow_terminal_retention_candidates(candidates, now_ms, total) do
    rows =
      case candidates do
        [] ->
          """
          <tr>
            <td colspan="8" class="c-muted">#{if total > 0, do: "Sampled terminal candidates were omitted by the display limit.", else: "No expired terminal Flow records found in the dashboard sample."}</td>
          </tr>
          """

        _ ->
          Enum.map_join(candidates, "\n", &render_flow_retention_candidate_row(&1, now_ms))
      end

    """
    <h2 class="section-title">Sampled Terminal Deletions <span class="badge badge-idle">#{format_number(length(candidates))}</span></h2>
    #{candidate_coverage(candidates, total)}
    <div class="table-scroll" role="region" aria-label="Sampled terminal workflow deletions" tabindex="0"><table>
      <thead>
        <tr>
          <th>Flow</th>
          <th>Type</th>
          <th>Runtime status</th>
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
    </table></div>
    """
  end

  defp candidate_coverage(candidates, total) do
    omitted = max(total - length(candidates), 0)

    ~s(<p class="flow-section-note">#{length(candidates)} of #{total} sampled candidates shown#{if omitted > 0, do: "; #{omitted} omitted by this category's display limit", else: ""}. Each category has its own bounded display allowance.</p>)
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
