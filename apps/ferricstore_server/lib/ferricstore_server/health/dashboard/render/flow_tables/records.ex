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
              <td class="flow-state-oldest">#{if state.due_now > 0, do: format_duration_ms(state.oldest_due_ms), else: "-"}</td>
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
        cond do
          state.due_now > 0 -> flow_state_operational_hint(state)
          state.state in @flow_terminal_states -> ~s(<span class="c-muted">Terminal</span>)
          true -> ~s(<span class="c-muted">None</span>)
        end

      badges ->
        Enum.map_join(badges, " ", fn {count, class, label} ->
          ~s(<span class="badge #{class}">#{format_grouped(count)} #{label}</span>)
        end)
    end
  end

  defp render_flow_state_due(%{due_now: due_now, count: due_now}) when due_now > 0,
    do: ~s(<span class="flow-state-due">all due now</span>)

  defp render_flow_state_due(%{due_now: due_now}) when is_integer(due_now) and due_now > 0,
    do: ~s(<span class="flow-state-due">#{format_grouped(due_now)} due now</span>)

  defp render_flow_state_due(_state), do: ""

  def render_flow_states_overview(_states, _sampled, :unavailable, _filters), do: ""
  def render_flow_states_overview([], _sampled, _source_status, _filters), do: ""

  def render_flow_states_overview(states, sampled, source_status, filters) do
    attention =
      [
        {sum_states(states, :expired_leases), "with expired leases"},
        {sum_states(states, :failed), "failed"},
        {sum_states(states, :max_attempts_reached), "out of retries"}
      ]
      |> Enum.filter(fn {count, _label} -> count > 0 end)

    status =
      case {attention, source_status} do
        {[], :partial} ->
          # Terminal records did not load, and failed runs live there.
          PageStatus.render("runs", :idle, "No problems in the records that loaded", [
            "terminal records are unavailable, so failed runs may be missing"
          ])

        {[], _ok} ->
          PageStatus.render(
            "runs",
            :healthy,
            "No runs need attention in this sample of #{format_grouped(sampled)}"
          )

        {items, _status} ->
          failures = FlowOverview.flow_failure_investigation_path(%{filters: filters})
          partial = if source_status == :partial, do: ["partial results"], else: []

          PageStatus.render(
            "runs",
            :warning,
            "Needs attention",
            Enum.map(items, fn {count, label} -> "#{format_grouped(count)} #{label}" end) ++
              partial,
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

  def render_flow_workers_status([], _filters),
    do: PageStatus.render("workers", :idle, "No leases held in this sample")

  def render_flow_workers_status(workers, filters) do
    leases = sum_states(workers, :running)
    expired = sum_states(workers, :expired)
    with_expired = Enum.count(workers, &((&1.expired || 0) > 0))

    if expired > 0 do
      failures = FlowOverview.flow_failure_investigation_path(%{filters: filters})

      PageStatus.render(
        "workers",
        :warning,
        "#{format_grouped(expired)} expired #{plural(expired, "lease", "leases")}",
        [
          "on #{format_grouped(with_expired)} of #{format_grouped(length(workers))} workers",
          "#{format_grouped(leases)} leases held"
        ],
        ~s(<a href="#{escape_attr(failures)}">Open Failures</a>)
      )
    else
      PageStatus.render(
        "workers",
        :healthy,
        "#{format_grouped(length(workers))} #{plural(length(workers), "worker", "workers")} holding #{format_grouped(leases)} #{plural(leases, "lease", "leases")}",
        ["No expired leases"]
      )
    end
  end

  defp plural(1, one, _many), do: one
  defp plural(_count, _one, many), do: many

  def render_flow_workers(workers, filters \\ %{})

  def render_flow_workers([], _filters),
    do: """
    <h2 class="section-title">Workers</h2>
    <p class="section-empty">No worker holds a lease in this sample.</p>
    """

  def render_flow_workers(workers, filters) do
    widest = workers |> Enum.map(&(&1.running || 0)) |> Enum.max(fn -> 0 end) |> max(1)

    rows =
      Enum.map_join(workers, "\n", fn worker ->
        running = worker.running || 0
        expired = worker.expired || 0

        query =
          filters
          |> Map.take([:type, :partition_key, :sort])
          |> Map.put(:worker, worker.worker)
          |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
          |> URI.encode_query()

        href = "/dashboard/flow/workers?" <> query <> "#flow-running-records"

        status =
          if expired > 0,
            do: ~s(<span class="badge badge-warning">#{format_grouped(expired)} expired</span>),
            else: ~s(<span class="badge badge-ok">Current</span>)

        oldest = if expired > 0, do: format_duration_ms(worker.oldest_lease_ms), else: "-"

        """
        <tr>
          <td class="mono"><a class="flow-link" href="#{escape_attr(href)}" title="Inspect this worker's sampled running records">#{escape(worker.worker)}</a></td>
          <td class="worker-leases"><a class="flow-link" href="#{escape_attr(href)}">#{format_number(running)}</a><span class="worker-lease-bar" style="width: #{percent(running, widest)}%"><span class="is-expired" style="width: #{percent(expired, max(running, 1))}%"></span></span></td>
          <td>#{status}</td>
          <td>#{oldest}</td>
        </tr>
        """
      end)

    table = """
    <table class="flow-workers-table">
      <thead>
        <tr><th>Worker</th><th>Leases</th><th>Status</th><th>Oldest expired by</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """

    """
    <div class="section-head"><h2 class="section-title">Workers <span class="badge badge-idle">#{format_grouped(length(workers))}</span></h2>#{render_worker_sort(filters)}</div>
    #{accessible_table("Workflow workers and leases", table)}
    """
  end

  defp percent(part, whole) when is_integer(part) and is_integer(whole) and whole > 0,
    do: min(100, round(part * 100 / whole))

  defp percent(_part, _whole), do: 0

  defp render_worker_sort(filters) do
    scope =
      filters
      |> Map.take([:type, :partition_key, :worker])
      |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
      |> Enum.map_join(fn {key, value} ->
        ~s(<input type="hidden" name="#{key}" value="#{escape_attr(value)}">)
      end)

    ~s(<form class="section-head-form" action="/dashboard/flow/workers" method="get">#{scope}#{render_flow_summary_sort(Map.get(filters, :sort, "attention"), "flow-worker-sort")}<button class="flow-search-button" type="submit">Apply order</button></form>)
  end

  def render_flow_running_records([], _total_sampled, _sample_limit),
    do: """
    <h2 class="section-title" id="flow-running-records">Leases</h2>
    <p class="section-empty">No run is leased in this sample.</p>
    """

  def render_flow_running_records(records, total_sampled, sample_limit) do
    rows =
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
          <td class="flow-due-cell">
            #{render_lease_relative(flow_record_lease_expires_at_ms(record))}#{render_clock(flow_record_lease_expires_at_ms(record))}
            #{render_worker_lease_details(record, id, partition)}
          </td>
        </tr>
        """
      end)

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
    <h2 class="section-title" id="flow-running-records">Leases <span class="badge badge-idle">#{format_grouped(length(records))}</span></h2>
    <p class="flow-section-note">#{sampled_scan_label(total_sampled, sample_limit)}</p>
    #{accessible_table("Running workflow records", table)}
    """
  end

  # A run's type and partition on one line under its ID.
  defp render_run_scope(record) do
    ~s(<span class="flow-run-secondary">#{escape(flow_record_type(record))} · <span class="mono">#{escape(flow_record_partition_key(record) || "auto/global")}</span></span>)
  end

  defp render_lease_relative(expires_at) when is_integer(expires_at) do
    case expires_at - System.system_time(:millisecond) do
      left when left >= 0 -> ~s(<span class="flow-due-when">in #{format_duration_ms(left)}</span>)
      left -> ~s(<span class="flow-due-when">expired #{format_duration_ms(-left)} ago</span>)
    end
  end

  defp render_lease_relative(_expires_at), do: ""

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

  def render_flow_due_summary(due_now, scheduled, scheduled_total \\ nil) do
    scheduled_count = scheduled_total || length(scheduled)

    {state, title} =
      case due_now do
        [] -> {:healthy, "Nothing is due now"}
        [_] -> {:idle, "1 run due now"}
        runs -> {:idle, "#{format_grouped(length(runs))} runs due now"}
      end

    oldest =
      due_now
      |> Enum.map(&due_delta_ms/1)
      |> Enum.filter(&(is_integer(&1) and &1 >= 0))
      |> Enum.max(fn -> nil end)

    next =
      scheduled
      |> Enum.map(&due_delta_ms/1)
      |> Enum.filter(&(is_integer(&1) and &1 < 0))
      |> Enum.max(fn -> nil end)

    details =
      [
        oldest && "oldest #{format_duration_ms(oldest)} overdue",
        case {scheduled_count, next} do
          {0, _} ->
            "Nothing scheduled in this sample"

          {count, nil} ->
            "#{format_grouped(count)} scheduled"

          {count, next} ->
            "#{format_grouped(count)} scheduled, next in #{format_duration_ms(-next)}"
        end
      ]
      |> Enum.reject(&is_nil/1)

    PageStatus.render("due", state, title, details) <>
      ~s(<p class="flow-section-note">Due time alone does not establish claimability; FIFO order, leases and policy limits can still hold a run back.</p>)
  end

  # Positive when the run is overdue, negative when it is still in the future.
  defp due_delta_ms(record) do
    case {flow_record_run_at_ms(record), Map.get(record, :dashboard_snapshot_ms)} do
      {run_at, snapshot} when is_integer(run_at) and is_integer(snapshot) -> snapshot - run_at
      {run_at, _} when is_integer(run_at) -> System.system_time(:millisecond) - run_at
      _ -> nil
    end
  end

  def render_flow_due_records(title, records, total_sampled, sample_limit, listed_total \\ nil) do
    shown = length(records)
    capped? = is_integer(listed_total) and listed_total > shown

    {heading, ordering, shown_label, empty} =
      if title == "Due Now",
        do:
          {"Due now", "Oldest due first", "Oldest #{format_grouped(shown)} shown",
           "Nothing is due now in this sample."},
        else:
          {"Scheduled", "Earliest scheduled first", "Earliest #{format_grouped(shown)} shown",
           "No runs are scheduled for later in this sample."}

    # A capped list says how much of the sample it shows.
    {count_label, ordering} =
      if capped?,
        do: {"#{format_grouped(shown)} of #{format_grouped(listed_total)}", shown_label},
        else: {format_grouped(shown), ordering}

    note =
      ~s(<p class="flow-section-note">#{ordering} · #{sampled_scan_label(total_sampled, sample_limit)}</p>)

    title_html =
      ~s(<h2 class="section-title">#{heading} <span class="badge badge-idle">#{count_label}</span></h2>)

    if records == [] do
      title_html <> ~s(\n<p class="section-empty">#{empty}</p>\n)
    else
      title_html <> "\n" <> note <> "\n" <> render_flow_due_table(title, records)
    end
  end

  defp render_flow_due_table(title, records) do
    rows =
      Enum.map_join(records, "\n", fn record ->
        """
        <tr>
          <td class="flow-run-identity"><div class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</div>#{render_run_scope(record)}</td>
          <td class="#{flow_state_class(flow_record_state(record))}">#{escape(flow_record_state(record))}</td>
          <td>#{render_due_waiting_reason(record)}</td>
          <td class="flow-due-cell">#{render_due_relative_time(record)}#{render_due_clock(flow_record_run_at_ms(record))}</td>
          <td>#{escape(to_string(flow_field(record, :priority, 0)))}</td>
          <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
        </tr>
        """
      end)

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

    accessible_table(title <> " workflow records", table)
  end

  defp render_due_clock(ms) when is_integer(ms),
    do:
      ~s(<span class="flow-run-secondary" title="#{escape_attr(format_timestamp_ms_or_dash(ms))}">#{format_clock_ms(ms)}</span>)

  defp render_due_clock(_ms), do: ~s(<span class="flow-run-secondary">-</span>)

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
    case due_delta_ms(record) do
      delta when is_integer(delta) and delta >= 0 ->
        ~s(<span class="flow-due-when">#{format_duration_ms(delta)} overdue</span>)

      delta when is_integer(delta) ->
        ~s(<span class="flow-due-when">in #{format_duration_ms(-delta)}</span>)

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
