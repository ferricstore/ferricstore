defmodule FerricstoreServer.Health.Dashboard.Render.FlowTables.Records do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory
  import FerricstoreServer.Health.Dashboard.Render.FlowFilters

  @flow_terminal_states ~w(completed failed cancelled)

  def render_flow_states_table(
        states,
        total_sampled,
        filtered_sampled,
        sample_limit,
        filters,
        source_status \\ :ok
      ) do
    rows =
      case states do
        [] ->
          message =
            if source_status == :ok,
              do: "No Flow states discovered for this type filter",
              else: "State results could not be established from the available sources."

          ~s(<tr><td colspan="12" class="c-muted">#{message}</td></tr>)

        _ ->
          Enum.map_join(states, "\n", fn state ->
            expired_class = if state.expired_leases > 0, do: "c-red", else: ""
            retry_class = if Map.get(state, :retrying, 0) > 0, do: "c-yellow", else: ""
            failed_class = if Map.get(state, :failed, 0) > 0, do: "c-red", else: ""
            maxed_class = if Map.get(state, :max_attempts_reached, 0) > 0, do: "c-red", else: ""
            href = state_records_path(filters, state.type, state.state)
            type_href = state_records_path(filters, state.type, nil)

            """
            <tr>
              <td class="mono"><a class="flow-link" href="#{escape_attr(type_href)}">#{escape(state.type)}</a></td>
              <td class="#{flow_state_class(state.state)}"><a class="flow-link" href="#{escape_attr(href)}">#{escape(state.state)}</a></td>
              <td class="#{failed_class}">#{format_number(Map.get(state, :failed, 0))}</td>
              <td class="#{expired_class}">#{format_number(state.expired_leases)}</td>
              <td class="#{maxed_class}">#{format_number(Map.get(state, :max_attempts_reached, 0))}</td>
              <td>#{flow_state_operational_hint(state)}</td>
              <td>#{format_duration_ms(state.oldest_due_ms)}</td>
              <td class="#{retry_class}">#{format_number(Map.get(state, :retrying, 0))}</td>
              <td><a class="flow-link" href="#{escape_attr(href)}" aria-label="Inspect #{escape_attr(state.type)} #{escape_attr(state.state)} records">#{format_number(state.count)}</a></td>
              <td>#{format_number(state.due_now)}</td>
              <td>#{format_number(state.running)}</td>
              <td>#{render_flow_state_mode_badge(Map.get(state, :mode, :parallel))}</td>
            </tr>
            """
          end)
      end

    filter_label = flow_filter_summary(filters)

    sample_label =
      case source_status do
        :unavailable ->
          "Results unavailable"

        :partial ->
          "Partial results: " <>
            bounded_sample_label(filtered_sampled, total_sampled, sample_limit)

        _ ->
          bounded_sample_label(filtered_sampled, total_sampled, sample_limit)
      end

    table = """
    <table class="flow-states-table">
      <thead>
        <tr>
          <th>Type</th>
          <th>Stored state</th>
          <th>Failed #{info_icon("Terminal failed flows. They are not claimable unless user logic rewinds or creates new work.")}</th>
          <th>Expired #{info_icon("Running flows whose lease deadline passed. Recovery eligibility depends on state policy and claim limits; expiry is not a terminal failure.")}</th>
          <th>Maxed #{info_icon("Flows whose attempts reached max_attempts/max_retries in the sampled records.")}</th>
          <th>Hint</th>
          <th>Oldest Due</th>
          <th>Retrying #{info_icon("Non-terminal flows with attempts > 0. They were retried and may be waiting for their next run time.")}</th>
          <th>Sample Count</th>
          <th>Due Now #{info_icon("Non-terminal flows whose scheduled time has passed. Due time alone does not establish claimability; FIFO ordering, leases, and policy limits may still block a claim.")}</th>
          <th>Running #{info_icon("Flows currently leased to workers through FLOW.CLAIM_DUE.")}</th>
          <th>Mode #{info_icon("FIFO states preserve per-partition order and let at most one active Flow block each partition lane. Parallel is the default.")}</th>
        </tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """

    """
    <h2 class="section-title">Flow States <span class="badge badge-idle">#{escape(filter_label)}</span> <span class="badge badge-idle">#{sample_label}</span></h2>
    <p class="flow-section-note">Exception columns first. Scroll for distribution columns.</p>
    #{accessible_table("Flow states", table)}
    """
  end

  def render_flow_states_sources(data) do
    if Map.get(data, :terminal_source_status) in [:error, :timeout] do
      heading =
        if Map.get(data, :source_status) == :partial,
          do: "Partial results",
          else: "Terminal records unavailable"

      reason =
        if Map.get(data, :terminal_source_status) == :timeout, do: "timed out", else: "failed"

      """
      <div class="pressure-alert level-warning" role="status">
        <div class="pressure-details"><strong>#{heading}</strong><p>The bounded terminal-record lookup #{reason}. State counts and recent records may be incomplete; available hot records are still shown.</p></div>
        <button type="button" class="flow-search-button" data-dashboard-refresh>Retry current scope</button>
      </div>
      """
    else
      ""
    end
  end

  def render_flow_fifo_lanes(lanes, total_sampled, sample_limit, coverage \\ %{}) do
    FerricstoreServer.Health.Dashboard.Render.FlowFifo.render(
      lanes,
      total_sampled,
      sample_limit,
      coverage
    )
  end

  defp render_flow_state_mode_badge(:fifo), do: ~s(<span class="badge badge-ok">FIFO</span>)
  defp render_flow_state_mode_badge("fifo"), do: render_flow_state_mode_badge(:fifo)

  defp render_flow_state_mode_badge(:mixed),
    do:
      ~s(<span class="badge badge-idle" title="This stored-state group contains logical states with different execution modes">mixed</span>)

  defp render_flow_state_mode_badge(:unknown),
    do:
      ~s(<span class="badge badge-idle" title="State policy could not be read">Unavailable</span>)

  defp render_flow_state_mode_badge(_mode), do: ~s(<span class="badge badge-idle">parallel</span>)

  def flow_state_operational_hint(state) do
    cond do
      state.expired_leases > 0 ->
        ~s(<span class="c-red" title="Check state policy and claim limits for recovery eligibility">Lease expired</span>)

      Map.get(state, :failed, 0) > 0 ->
        ~s(<span class="c-red">terminal failed</span>)

      Map.get(state, :max_attempts_reached, 0) > 0 ->
        ~s(<span class="c-red">retry attempts maxed</span>)

      state.due_now > 0 ->
        ~s(<span class="c-muted">Due time reached</span>)

      Map.get(state, :retrying, 0) > 0 ->
        ~s(<span class="c-yellow">retry backoff/attempts</span>)

      state.state in @flow_terminal_states ->
        ~s(<span class="c-muted">terminal</span>)

      true ->
        ~s(<span class="c-muted">healthy</span>)
    end
  end

  def render_flow_state_breakdown(types) do
    rows =
      case types do
        [] ->
          ~s(<tr><td colspan="10" class="c-muted">No Flow state records discovered</td></tr>)

        _ ->
          Enum.map_join(types, "\n", fn type ->
            exact_badge =
              if Map.get(type, :exact, false) do
                ~s(<span class="badge badge-ok">exact</span>)
              else
                ~s(<span class="badge badge-idle">sample</span>)
              end

            """
            <tr>
              <td class="mono">#{escape(type.type)}</td>
              <td>#{exact_badge}</td>
              <td>#{format_number(type.total)}</td>
              <td>#{format_number(type.active)}</td>
              <td>#{format_number(type.queued)}</td>
              <td>#{format_number(type.running)}</td>
              <td>#{format_number(type.completed)}</td>
              <td class="#{if type.failed > 0, do: "c-red", else: ""}">#{format_number(type.failed)}</td>
              <td>#{format_number(type.cancelled)}</td>
              <td>#{render_flow_custom_states(type.states)}</td>
            </tr>
            """
          end)
      end

    table = """
    <table>
      <thead>
        <tr><th>Type</th><th>Count Source</th><th>Total</th><th>Active</th><th>Queued</th><th>Running</th><th>Completed</th><th>Failed</th><th>Cancelled</th><th>Observed States</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """

    """
    <h2 class="section-title">State Breakdown</h2>
    #{accessible_table("Workflow state breakdown", table)}
    """
  end

  def render_flow_custom_states(states) when is_map(states) do
    states
    |> Enum.sort_by(fn {state, _count} -> state end)
    |> Enum.map_join(" ", fn {state, count} ->
      ~s(<span class="flow-pill">#{escape(state)} #{format_number(count)}</span>)
    end)
  end

  def render_flow_custom_states(_states), do: ""

  def render_flow_workers(workers, filters \\ %{}) do
    rows =
      case workers do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No running Flow leases discovered in sample</td></tr>)

        _ ->
          Enum.map_join(workers, "\n", fn worker ->
            expired_class = if worker.expired > 0, do: "c-red", else: ""

            query =
              filters
              |> Map.take([:type, :partition_key, :sort])
              |> Map.put(:worker, worker.worker)
              |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
              |> URI.encode_query()

            href = "/dashboard/flow/workers?" <> query <> "#flow-running-records"

            """
            <tr>
              <td class="mono"><a class="flow-link" href="#{escape_attr(href)}" title="Inspect this worker's sampled running records">#{escape(worker.worker)}</a></td>
              <td><a class="flow-link" href="#{escape_attr(href)}">#{format_number(worker.running)}</a></td>
              <td class="#{expired_class}">#{format_number(worker.expired)}</td>
              <td>#{format_duration_ms(worker.oldest_lease_ms)}</td>
            </tr>
            """
          end)
      end

    table = """
    <table>
      <thead>
        <tr><th>Worker</th><th>Running</th><th>Expired</th><th>Oldest Expired By</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """

    """
    <h2 class="section-title">Workers / Leases</h2>
    #{render_worker_sort(filters)}
    #{accessible_table("Workflow workers and leases", table)}
    """
  end

  defp render_worker_sort(filters) do
    scope =
      filters
      |> Map.take([:type, :partition_key, :worker])
      |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
      |> Enum.map_join(fn {key, value} ->
        ~s(<input type="hidden" name="#{key}" value="#{escape_attr(value)}">)
      end)

    ~s(<form class="flow-filter-form" action="/dashboard/flow/workers" method="get">#{scope}#{render_flow_summary_sort(Map.get(filters, :sort, "attention"), "flow-worker-sort")}<button class="flow-search-button" type="submit">Apply order</button></form>)
  end

  def render_flow_running_records(records, total_sampled, sample_limit) do
    rows =
      case records do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No running Flow records discovered in sample</td></tr>)

        _ ->
          Enum.map_join(records, "\n", fn record ->
            expired_class = if flow_expired_lease?(record), do: "c-red", else: ""
            id = flow_record_id(record)
            partition = flow_record_partition_key(record)

            """
            <tr>
              <td class="flow-worker-identity">
                <div class="mono">#{render_flow_id_link(id, partition)}</div>
                <span class="flow-run-secondary">#{escape(flow_record_type(record))}</span>
                <span class="flow-run-secondary mono" title="Partition">#{escape(partition || "auto/global")}</span>
              </td>
              <td class="mono">#{escape(flow_record_worker(record) || "-")}</td>
              <td class="#{expired_class}">#{escape(flow_waiting_reason(record))}</td>
              <td>
                #{format_timestamp_ms_or_dash(flow_record_lease_expires_at_ms(record))}
                #{render_worker_lease_details(record, id, partition)}
              </td>
            </tr>
            """
          end)
      end

    table = """
    <table class="flow-worker-records-table">
      <thead>
        <tr><th>Workflow</th><th>Worker</th><th>Status</th><th>Lease Expires (UTC)</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """

    """
    <h2 class="section-title" id="flow-running-records">Running Records <span class="badge badge-idle">#{sampled_scan_label(total_sampled, sample_limit)}</span></h2>
    #{accessible_table("Running workflow records", table)}
    """
  end

  defp render_worker_lease_details(record, id, partition) do
    key =
      {id, partition}
      |> :erlang.term_to_binary()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    """
    <details class="flow-worker-lease-details" data-dashboard-disclosure-key="lease-#{key}" data-dashboard-live-pause>
      <summary>Lease details<span class="sr-only"> for #{escape(id)} in #{escape(partition || "auto/global")}</span></summary>
      <dl>
        <dt>Lease token</dt><dd>#{escape(to_string(flow_field(record, :lease_token, nil) || "-"))}</dd>
        <dt>Fencing token</dt><dd>#{escape(to_string(flow_field(record, :fencing_token, nil) || "-"))}</dd>
      </dl>
    </details>
    """
  end

  def render_flow_due_records(title, records, total_sampled, sample_limit) do
    ordering =
      if title == "Due Now",
        do: "Oldest due first in current sample",
        else: "Earliest scheduled first in current sample"

    rows =
      case records do
        [] ->
          ~s(<tr><td colspan="7" class="c-muted">No #{escape(String.downcase(title))} records discovered in sample</td></tr>)

        _ ->
          Enum.map_join(records, "\n", fn record ->
            """
            <tr>
              <td class="flow-run-identity mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}<span class="flow-run-secondary mono" title="Partition">#{escape(flow_record_partition_key(record) || "auto/global")}</span></td>
              <td class="mono">#{escape(flow_record_type(record))}</td>
              <td class="#{flow_state_class(flow_record_state(record))}">#{escape(flow_record_state(record))}</td>
              <td>#{render_due_waiting_reason(record)}</td>
              <td>#{format_timestamp_ms_or_dash(flow_record_run_at_ms(record))}#{render_due_relative_time(record)}</td>
              <td>#{escape(to_string(flow_field(record, :priority, 0)))}</td>
              <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
            </tr>
            """
          end)
      end

    table = """
    <table>
      <thead>
        <tr><th>Workflow / partition</th><th>Type</th><th>Stored state</th><th>Why Waiting</th><th>Run At (UTC)</th><th>Priority</th><th>Values</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """

    """
    <h2 class="section-title">#{escape(title)} <span class="badge badge-idle">#{sampled_scan_label(total_sampled, sample_limit)}</span></h2>
    <p class="flow-section-note">#{ordering}</p>
    #{accessible_table(title <> " workflow records", table)}
    """
  end

  defp render_due_waiting_reason(record) do
    case Map.get(record, :dashboard_fifo_blocker) do
      blocker when is_binary(blocker) and blocker != "" ->
        if blocker != flow_record_id(record) do
          "Observed FIFO blocker: " <>
            render_flow_id_link(blocker, flow_record_partition_key(record))
        else
          escape(flow_waiting_reason(record))
        end

      _ ->
        escape(flow_waiting_reason(record))
    end
  end

  defp render_due_relative_time(record) do
    case {flow_record_run_at_ms(record), Map.get(record, :dashboard_snapshot_ms)} do
      {run_at, snapshot} when is_integer(run_at) and is_integer(snapshot) ->
        delta = snapshot - run_at

        label =
          if delta >= 0,
            do: "#{format_duration_ms(delta)} overdue",
            else: "in #{format_duration_ms(-delta)}"

        ~s(<span class="flow-run-secondary" title="Relative to captured snapshot at #{format_timestamp_ms_or_dash(snapshot)}">#{label}</span>)

      _ ->
        ""
    end
  end

  def render_flow_failures_rows([]) do
    ~s(<tr><td colspan="9" class="c-muted">No failed, exhausted, or expired-lease records found in the current bounded view.</td></tr>)
  end

  def render_flow_failures_rows(records) do
    Enum.map_join(records, "\n", fn record ->
      state = flow_record_state(record)

      """
      <tr>
        <td class="flow-run-identity mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}<span class="flow-run-secondary mono" title="Partition">#{escape(flow_record_partition_key(record) || "auto/global")}</span></td>
        <td class="mono">#{escape(flow_record_type(record))}</td>
        <td class="#{flow_state_class(state)}">#{escape(state)}</td>
        <td>#{escape(flow_recovery_reason(record))}</td>
        <td>#{format_number(flow_record_attempts(record))}</td>
        <td class="mono">#{escape(flow_record_worker(record) || "-")}</td>
        <td>#{format_timestamp_ms_or_dash(flow_record_lease_expires_at_ms(record))}</td>
        <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
        <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
      </tr>
      """
    end)
  end

  def render_flow_recent_records(records, limit \\ nil, source_status \\ :ok) do
    rows =
      case records do
        [] ->
          message =
            if source_status == :ok,
              do: "No Flow records discovered in the current scope",
              else: "Recent records could not be established from the available sources."

          ~s(<tr><td colspan="6" class="c-muted">#{message}</td></tr>)

        _ ->
          Enum.map_join(records, "\n", fn record ->
            id = flow_record_id(record)
            state = flow_record_state(record)
            status = flow_record_status_label(record)
            partition = flow_record_partition_key(record)
            detail_path = flow_detail_path(id, partition)
            logical_state = flow_record_logical_state(record)
            wrapped_state = logical_state |> escape() |> String.replace("_", "_<wbr>")

            status_badge =
              cond do
                status == "expired lease" ->
                  ~s(<span class="badge badge-failed">expired lease</span>)

                state == "running" ->
                  ~s(<span class="badge badge-running">leased</span>)

                status == "due" ->
                  ~s(<span class="badge badge-due">due</span>)

                status == "retrying" ->
                  ~s(<span class="badge badge-retrying">retrying</span>)

                state == "failed" ->
                  ~s(<span class="badge badge-failed">failed</span>)

                state == "completed" ->
                  ~s(<span class="badge badge-completed">completed</span>)

                true ->
                  ~s(<span class="badge badge-idle">#{escape(if status == "terminal", do: state, else: status)}</span>)
              end

            """
            <tr>
              <td class="flow-run-identity">
                <div class="mono">#{render_flow_id_link(id, partition)}</div>
                <span class="flow-run-secondary" title="#{escape_attr(flow_record_type(record))}">#{escape(flow_record_type(record))}</span>
                <span class="flow-run-secondary mono" title="Partition">#{escape(partition || "auto/global")}</span>
              </td>
              <td><span class="flow-run-step mono" title="Workflow state: #{escape_attr(logical_state)}">#{wrapped_state}</span><span class="flow-run-secondary" title="Stored state: #{escape_attr(state)}">Stored: #{escape(state)}</span>#{status_badge}</td>
              <td><span class="flow-run-reason">#{escape(flow_waiting_reason(record))}</span><span class="flow-run-secondary">#{format_number(flow_record_attempts(record))} attempts</span></td>
              <td class="flow-run-timing"><span><span class="c-muted">Run</span> #{format_timestamp_ms_or_dash(flow_record_run_at_ms(record))}</span><span><span class="c-muted">Updated</span> #{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</span></td>
              <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
              <td class="flow-row-actions">
                <a class="flow-link" href="#{detail_path}" title="Inspect workflow">Inspect</a>
                <button type="button" class="copy-btn-inline" data-copy-text="#{escape_attr(id)}" title="Copy Flow ID">Copy</button>
              </td>
            </tr>
            """
          end)
      end

    limit_badge =
      case limit do
        limit when is_integer(limit) ->
          ~s( <span class="badge badge-idle">limit #{format_number(limit)}</span>)

        _ ->
          ""
      end

    """
    <h2 class="section-title" id="flow-recent-records">Recent Flow Records#{limit_badge}</h2>
    <div class="table-scroll" role="region" aria-label="Recent workflow records" tabindex="0"><table class="flow-runs-table">
      <thead>
        <tr><th>Workflow</th><th>Workflow / stored state</th><th>Activity</th><th>Timing (UTC)</th><th>Values</th><th>Actions</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table></div>
    """
  end

  defp state_records_path(filters, type, state) do
    query =
      filters
      |> Map.take([:partition_key, :range, :time_mode, :from_ms, :to_ms, :q, :limit, :sort])
      |> Map.merge(%{type: type, state: state})
      |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
      |> URI.encode_query()

    "/dashboard/flow/states?" <> query <> "#flow-recent-records"
  end
end
