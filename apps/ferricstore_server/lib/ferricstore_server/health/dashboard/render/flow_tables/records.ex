defmodule FerricstoreServer.Health.Dashboard.Render.FlowTables.Records do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory
  import FerricstoreServer.Health.Dashboard.Render.FlowFilters

  alias FerricstoreServer.Health.Dashboard.Render.{FlowOverview, PageStatus}

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

          ~s(<tr><td colspan="6" class="c-muted">#{message}</td></tr>)

        _ ->
          Enum.map_join(states, "\n", fn state ->
            href = state_records_path(filters, state.type, state.state)
            type_href = state_records_path(filters, state.type, nil)

            """
            <tr>
              <td class="mono"><a class="flow-link" href="#{escape_attr(type_href)}">#{escape(state.type)}</a></td>
              <td class="#{flow_state_class(state.state)}"><a class="flow-link" href="#{escape_attr(href)}">#{escape(state.state)}</a></td>
              <td class="flow-state-attention">#{render_flow_state_attention(state)}</td>
              <td>#{format_duration_ms(state.oldest_due_ms)}</td>
              <td class="flow-state-runs"><a class="flow-link" href="#{escape_attr(href)}" aria-label="Inspect #{escape_attr(state.type)} #{escape_attr(state.state)} records">#{format_number(state.count)}</a>#{render_flow_state_due(state)}</td>
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
          <th>State</th>
          <th>Attention</th>
          <th>Oldest due</th>
          <th>Runs</th>
          <th>Mode</th>
        </tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """

    """
    <h2 class="section-title">Runs by state <span class="badge badge-idle">#{escape(filter_label)}</span> <span class="badge badge-idle">#{sample_label}</span></h2>
    #{accessible_table("Flow states", table)}
    <p class="flow-section-note">Expired leases are not failures: whether a run can be reclaimed depends on its state's policy and claim limits. Due time alone does not establish claimability; FIFO order, leases and policy limits can still hold a run back. FIFO states keep one active run per partition.</p>
    """
  end

  # Each kind of problem gets its own badge. The kinds overlap (a failed run can also
  # be out of retries), so they are never added up.
  defp render_flow_state_attention(state) do
    [
      {state.expired_leases, "badge-warning", "expired"},
      {Map.get(state, :failed, 0), "badge-failed", "failed"},
      {Map.get(state, :max_attempts_reached, 0), "badge-failed", "out of retries"},
      {Map.get(state, :retrying, 0), "badge-retrying", "retrying"}
    ]
    |> Enum.filter(fn {count, _class, _label} -> count > 0 end)
    |> case do
      [] ->
        flow_state_operational_hint(state)

      badges ->
        Enum.map_join(badges, " ", fn {count, class, label} ->
          ~s(<span class="badge #{class}">#{format_grouped(count)} #{label}</span>)
        end)
    end
  end

  defp render_flow_state_due(%{due_now: due_now}) when is_integer(due_now) and due_now > 0,
    do: ~s(<span class="flow-state-due">#{format_grouped(due_now)} due now</span>)

  defp render_flow_state_due(_state), do: ""

  def render_flow_states_overview(_states, _sampled, :unavailable, _filters), do: ""
  def render_flow_states_overview([], _sampled, _source_status, _filters), do: ""

  def render_flow_states_overview(states, sampled, _source_status, filters) do
    attention =
      [
        {sum_states(states, :expired_leases), "with expired leases"},
        {sum_states(states, :failed), "failed"},
        {sum_states(states, :max_attempts_reached), "out of retries"}
      ]
      |> Enum.filter(fn {count, _label} -> count > 0 end)

    status =
      case attention do
        [] ->
          PageStatus.render(
            "runs",
            :healthy,
            "No runs need attention in this sample of #{format_grouped(sampled)}"
          )

        items ->
          failures = FlowOverview.flow_failure_investigation_path(%{filters: filters})

          PageStatus.render(
            "runs",
            :warning,
            "Needs attention",
            Enum.map(items, fn {count, label} -> "#{format_grouped(count)} #{label}" end),
            ~s(<a href="#{escape_attr(failures)}">Open Failures</a>)
          )
      end

    status <> render_flow_state_split(states)
  end

  defp sum_states(states, key), do: states |> Enum.map(&(Map.get(&1, key) || 0)) |> Enum.sum()

  defp render_flow_state_split(states) do
    totals =
      states
      |> Enum.group_by(& &1.state, & &1.count)
      |> Enum.map(fn {state, counts} -> {state, Enum.sum(counts)} end)
      |> Enum.sort_by(fn {state, count} -> {-count, state} end)

    label =
      Enum.map_join(totals, ", ", fn {state, count} -> "#{format_grouped(count)} #{state}" end)

    bar =
      Enum.map_join(totals, "", fn {state, count} ->
        ~s(<span class="overview-state is-#{state_split_class(state)}" style="flex: #{count}"></span>)
      end)

    legend =
      Enum.map_join(totals, "", fn {state, count} ->
        ~s(<li><span class="overview-swatch is-#{state_split_class(state)}" aria-hidden="true"></span>#{escape(state)} <strong>#{format_grouped(count)}</strong></li>)
      end)

    """
    <div class="runs-split">
      <div class="overview-state-bar" role="img" aria-label="#{escape_attr(label)}">#{bar}</div>
      <ul class="overview-legend">#{legend}</ul>
    </div>
    """
  end

  defp state_split_class(state) when state in ~w(queued running failed completed cancelled),
    do: state

  defp state_split_class(_state), do: "other"

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

            worker = flow_record_worker(record)
            reason = flow_waiting_reason(record)

            # The worker column already says who holds the lease.
            status =
              if is_binary(worker) and reason == "leased by " <> worker,
                do: "Leased",
                else: reason

            """
            <tr>
              <td class="flow-worker-identity">
                <div class="mono">#{render_flow_id_link(id, partition)}</div>
                #{render_run_scope(record)}
              </td>
              <td class="mono">#{escape(worker || "-")}</td>
              <td#{if expired_class == "", do: "", else: ~s( class="#{expired_class}")}>#{escape(status)}</td>
              <td>
                #{render_clock(flow_record_lease_expires_at_ms(record))}
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

  # A run's type and partition on one line under its ID.
  defp render_run_scope(record) do
    ~s(<span class="flow-run-secondary">#{escape(flow_record_type(record))} · <span class="mono">#{escape(flow_record_partition_key(record) || "auto/global")}</span></span>)
  end

  # Time to the second, with the full timestamp in the tooltip.
  defp render_clock(nil), do: "-"

  defp render_clock(ms) when is_integer(ms),
    do:
      ~s(<span title="#{escape_attr(format_timestamp_ms_or_dash(ms))}">#{format_clock_ms(ms)}</span>)

  defp render_clock(_ms), do: "-"

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
          ~s(<tr><td colspan="6" class="c-muted">No #{escape(String.downcase(title))} records discovered in sample</td></tr>)

        _ ->
          Enum.map_join(records, "\n", fn record ->
            """
            <tr>
              <td class="flow-run-identity"><div class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</div>#{render_run_scope(record)}</td>
              <td class="#{flow_state_class(flow_record_state(record))}">#{escape(flow_record_state(record))}</td>
              <td>#{render_due_waiting_reason(record)}</td>
              <td>#{render_clock(flow_record_run_at_ms(record))}#{render_due_relative_time(record)}</td>
              <td>#{escape(to_string(flow_field(record, :priority, 0)))}</td>
              <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
            </tr>
            """
          end)
      end

    table = """
    <table>
      <thead>
        <tr><th>Workflow</th><th>Stored state</th><th>Why waiting</th><th>Run at (UTC)</th><th>Priority</th><th>Values</th></tr>
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
                  ~s(<span class="badge badge-warning">expired lease</span>)

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

            stored =
              if state == logical_state,
                do: "",
                else: ~s(<span class="flow-run-stored">stored #{escape(state)}</span>)

            timing =
              "Run #{format_timestamp_ms_or_dash(flow_record_run_at_ms(record))} · Updated #{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}"

            """
            <tr>
              <td class="flow-run-identity">
                <div class="mono">#{render_flow_id_link(id, partition)}</div>
                <span class="flow-run-secondary">#{escape(flow_record_type(record))} · <span class="mono">#{escape(partition || "auto/global")}</span></span>
              </td>
              <td><span class="flow-run-state" title="Workflow state: #{escape_attr(logical_state)} · Stored state: #{escape_attr(state)}"><span class="flow-run-step mono">#{wrapped_state}</span>#{status_badge}</span>#{stored}</td>
              <td><span class="flow-run-reason">#{escape(flow_waiting_reason(record))}</span><span class="flow-run-attempts">#{format_number(flow_record_attempts(record))} attempts</span></td>
              <td class="flow-run-updated" title="#{escape_attr(timing)}">#{format_clock_ms(flow_record_updated_at_ms(record))}</td>
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
        <tr><th>Workflow</th><th>State</th><th>Activity</th><th>Updated (UTC)</th><th>Values</th><th>Actions</th></tr>
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
