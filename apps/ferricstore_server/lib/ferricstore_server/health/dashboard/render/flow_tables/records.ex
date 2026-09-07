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
        filters
      ) do
    rows =
      case states do
        [] ->
          ~s(<tr><td colspan="12" class="c-muted">No Flow states discovered for this type filter</td></tr>)

        _ ->
          Enum.map_join(states, "\n", fn state ->
            due_class = if state.due_now > 0, do: "c-yellow", else: ""
            expired_class = if state.expired_leases > 0, do: "c-red", else: ""
            retry_class = if Map.get(state, :retrying, 0) > 0, do: "c-yellow", else: ""
            failed_class = if Map.get(state, :failed, 0) > 0, do: "c-red", else: ""
            maxed_class = if Map.get(state, :max_attempts_reached, 0) > 0, do: "c-red", else: ""

            """
            <tr>
              <td class="mono">#{escape(state.type)}</td>
              <td class="#{flow_state_class(state.state)}">#{escape(state.state)}</td>
              <td>#{render_flow_state_mode_badge(Map.get(state, :mode, :parallel))}</td>
              <td>#{format_number(state.count)}</td>
              <td class="#{due_class}">#{format_number(state.due_now)}</td>
              <td>#{format_number(state.running)}</td>
              <td class="#{retry_class}">#{format_number(Map.get(state, :retrying, 0))}</td>
              <td class="#{failed_class}">#{format_number(Map.get(state, :failed, 0))}</td>
              <td class="#{expired_class}">#{format_number(state.expired_leases)}</td>
              <td class="#{maxed_class}">#{format_number(Map.get(state, :max_attempts_reached, 0))}</td>
              <td>#{format_duration_ms(state.oldest_due_ms)}</td>
              <td>#{flow_state_operational_hint(state)}</td>
            </tr>
            """
          end)
      end

    filter_label = flow_filter_summary(filters)

    table = """
    <table>
      <thead>
        <tr>
          <th>Type</th>
          <th>State</th>
          <th>Mode #{info_icon("FIFO states preserve per-partition order and let at most one active Flow block each partition lane. Parallel is the default.")}</th>
          <th>Sample Count</th>
          <th>Due Now #{info_icon("Non-terminal flows with run_at/next_run_at at or before now. Workers should be able to claim them.")}</th>
          <th>Running #{info_icon("Flows currently leased to workers through FLOW.CLAIM_DUE.")}</th>
          <th>Retrying #{info_icon("Non-terminal flows with attempts > 0. They were retried and may be waiting for their next run time.")}</th>
          <th>Failed #{info_icon("Terminal failed flows. They are not claimable unless user logic rewinds or creates new work.")}</th>
          <th>Expired #{info_icon("Running flows whose lease deadline passed. This is reclaimable work, not a terminal failure.")}</th>
          <th>Maxed #{info_icon("Flows whose attempts reached max_attempts/max_retries in the sampled records.")}</th>
          <th>Oldest Due</th>
          <th>Hint</th>
        </tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """

    """
    <div class="section-title">Flow States <span class="badge badge-idle">#{escape(filter_label)}</span> <span class="badge badge-idle">#{bounded_sample_label(filtered_sampled, total_sampled, sample_limit)}</span></div>
    #{accessible_table("Flow states", table)}
    """
  end

  def render_flow_fifo_lanes(lanes, total_sampled, sample_limit) do
    FerricstoreServer.Health.Dashboard.Render.FlowFifo.render(lanes, total_sampled, sample_limit)
  end

  defp render_flow_state_mode_badge(:fifo), do: ~s(<span class="badge badge-ok">FIFO</span>)
  defp render_flow_state_mode_badge("fifo"), do: render_flow_state_mode_badge(:fifo)
  defp render_flow_state_mode_badge(_mode), do: ~s(<span class="badge badge-idle">parallel</span>)

  def flow_state_operational_hint(state) do
    cond do
      state.expired_leases > 0 ->
        ~s(<span class="c-red">leases need reclaim</span>)

      Map.get(state, :failed, 0) > 0 ->
        ~s(<span class="c-red">terminal failed</span>)

      Map.get(state, :max_attempts_reached, 0) > 0 ->
        ~s(<span class="c-red">retry attempts maxed</span>)

      state.due_now > 0 and state.running == 0 ->
        ~s(<span class="c-yellow">due work, no running sample</span>)

      state.due_now > 0 ->
        ~s(<span class="c-yellow">workers should drain</span>)

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
    <div class="section-title">State Breakdown</div>
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

  def render_flow_workers(workers) do
    rows =
      case workers do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No running Flow leases discovered in sample</td></tr>)

        _ ->
          Enum.map_join(workers, "\n", fn worker ->
            expired_class = if worker.expired > 0, do: "c-red", else: ""

            """
            <tr>
              <td class="mono">#{escape(worker.worker)}</td>
              <td>#{format_number(worker.running)}</td>
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
    <div class="section-title">Workers / Leases</div>
    #{accessible_table("Workflow workers and leases", table)}
    """
  end

  def render_flow_running_records(records, total_sampled, sample_limit) do
    rows =
      case records do
        [] ->
          ~s(<tr><td colspan="7" class="c-muted">No running Flow records discovered in sample</td></tr>)

        _ ->
          Enum.map_join(records, "\n", fn record ->
            expired_class = if flow_expired_lease?(record), do: "c-red", else: ""

            """
            <tr>
              <td class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</td>
              <td class="mono">#{escape(flow_record_type(record))}</td>
              <td class="mono">#{escape(flow_record_worker(record) || "-")}</td>
              <td class="#{expired_class}">#{escape(flow_waiting_reason(record))}</td>
              <td>#{format_timestamp_ms_or_dash(flow_record_lease_expires_at_ms(record))}</td>
              <td>#{escape(to_string(flow_field(record, :lease_token, "-")))}</td>
              <td>#{escape(to_string(flow_field(record, :fencing_token, "-")))}</td>
            </tr>
            """
          end)
      end

    table = """
    <table>
      <thead>
        <tr><th>ID</th><th>Type</th><th>Worker</th><th>Status</th><th>Lease Expires</th><th>Lease Token</th><th>Fencing</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """

    """
    <div class="section-title">Running Records <span class="badge badge-idle">#{sampled_scan_label(total_sampled, sample_limit)}</span></div>
    #{accessible_table("Running workflow records", table)}
    """
  end

  def render_flow_due_records(title, records, total_sampled, sample_limit) do
    rows =
      case records do
        [] ->
          ~s(<tr><td colspan="7" class="c-muted">No #{escape(String.downcase(title))} records discovered in sample</td></tr>)

        _ ->
          Enum.map_join(records, "\n", fn record ->
            """
            <tr>
              <td class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</td>
              <td class="mono">#{escape(flow_record_type(record))}</td>
              <td class="#{flow_state_class(flow_record_state(record))}">#{escape(flow_record_state(record))}</td>
              <td>#{escape(flow_waiting_reason(record))}</td>
              <td>#{format_timestamp_ms_or_dash(flow_record_run_at_ms(record))}</td>
              <td>#{escape(to_string(flow_field(record, :priority, 0)))}</td>
              <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
            </tr>
            """
          end)
      end

    table = """
    <table>
      <thead>
        <tr><th>ID</th><th>Type</th><th>State</th><th>Why Waiting</th><th>Run At</th><th>Priority</th><th>Values</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """

    """
    <div class="section-title">#{escape(title)} <span class="badge badge-idle">#{sampled_scan_label(total_sampled, sample_limit)}</span></div>
    #{accessible_table(title <> " workflow records", table)}
    """
  end

  def render_flow_failures_rows([]) do
    ~s(<tr><td colspan="9" class="c-muted">No failed, exhausted, or expired-lease records found in the current bounded view.</td></tr>)
  end

  def render_flow_failures_rows(records) do
    Enum.map_join(records, "\n", fn record ->
      state = flow_record_state(record)

      """
      <tr>
        <td class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</td>
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

  def render_flow_recent_records(records, limit \\ nil) do
    rows =
      case records do
        [] ->
          ~s(<tr><td colspan="6" class="c-muted">No Flow records discovered in the current scope</td></tr>)

        _ ->
          Enum.map_join(records, "\n", fn record ->
            id = flow_record_id(record)
            state = flow_record_state(record)
            status = flow_record_status_label(record)
            partition = flow_record_partition_key(record)
            detail_path = flow_detail_path(id, partition)

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
              <td><span class="flow-run-step mono">#{escape(flow_record_logical_state(record))}</span>#{status_badge}</td>
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
    <div class="section-title">Recent Flow Records#{limit_badge}</div>
    <div class="table-scroll" role="region" aria-label="Recent workflow records" tabindex="0"><table class="flow-runs-table">
      <thead>
        <tr><th>Workflow</th><th>State</th><th>Activity</th><th>Timing (UTC)</th><th>Values</th><th>Actions</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table></div>
    """
  end
end
