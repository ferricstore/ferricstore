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
          ~s(<tr><td colspan="11" class="c-muted">No Flow states discovered for this type filter</td></tr>)

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

    """
    <div class="section-title">Flow States <span class="badge badge-idle">#{escape(filter_label)}</span> <span class="badge badge-idle">sampled #{format_number(filtered_sampled)} / #{format_number(total_sampled)} / #{format_number(sample_limit)}</span></div>
    <table>
      <thead>
        <tr>
          <th>Type</th>
          <th>State</th>
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
  end

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

    """
    <div class="section-title">State Breakdown</div>
    <table>
      <thead>
        <tr><th>Type</th><th>Count Source</th><th>Total</th><th>Active</th><th>Queued</th><th>Running</th><th>Completed</th><th>Failed</th><th>Cancelled</th><th>Observed States</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
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

    """
    <div class="section-title">Workers / Leases</div>
    <table>
      <thead>
        <tr><th>Worker</th><th>Running</th><th>Expired</th><th>Oldest Expired By</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
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

    """
    <div class="section-title">Running Records <span class="badge badge-idle">sampled #{format_number(total_sampled)} / #{format_number(sample_limit)}</span></div>
    <table>
      <thead>
        <tr><th>ID</th><th>Type</th><th>Worker</th><th>Status</th><th>Lease Expires</th><th>Lease Token</th><th>Fencing</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
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

    """
    <div class="section-title">#{escape(title)} <span class="badge badge-idle">sampled #{format_number(total_sampled)} / #{format_number(sample_limit)}</span></div>
    <table>
      <thead>
        <tr><th>ID</th><th>Type</th><th>State</th><th>Why Waiting</th><th>Run At</th><th>Priority</th><th>Values</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
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
          ~s(<tr><td colspan="10" class="c-muted">No Flow records discovered</td></tr>)

        _ ->
          Enum.map_join(records, "\n", fn record ->
            id = flow_record_id(record)
            state = flow_record_state(record)
            state_class = flow_state_class(state)
            status = flow_record_status_label(record)

            """
            <tr>
              <td class="mono">#{render_flow_id_link(id, flow_record_partition_key(record))}</td>
              <td class="mono">#{escape(flow_record_type(record))}</td>
              <td class="#{state_class}">#{escape(state)}</td>
              <td>#{escape(status)}</td>
              <td>#{format_number(flow_record_attempts(record))}</td>
              <td class="mono">#{escape(flow_record_worker(record) || "-")}</td>
              <td>#{escape(flow_waiting_reason(record))}</td>
              <td>#{format_timestamp_ms_or_dash(flow_record_run_at_ms(record))}</td>
              <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
              <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
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
    <table>
      <thead>
        <tr><th>ID</th><th>Type</th><th>State</th><th>Status</th><th>Attempts</th><th>Worker</th><th>Why Waiting</th><th>Run At</th><th>Updated</th><th>Values</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end
end
