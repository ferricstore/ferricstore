defmodule FerricstoreServer.Health.Dashboard.Render.FlowGovernance do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.FlowRecord

  import FerricstoreServer.Health.Dashboard.Render.FlowHistory,
    only: [flow_state_class: 1, render_flow_id_link: 2]

  import FerricstoreServer.Health.Dashboard.Render.FlowOverview, only: [render_flow_stat_card: 3]

  def render_flow_governance_summary(data) do
    counts = Map.get(data, :counts, %{})

    """
    <div class="flow-card-grid">
      #{render_flow_stat_card("Approvals", Map.get(counts, :approvals, 0), "bounded approval records")}
      #{render_flow_stat_card("Pending", Map.get(counts, :pending_approvals, 0), "approval requests waiting")}
      #{render_flow_stat_card("Budgets", Map.get(counts, :budgets, 0), "durable budget counters")}
      #{render_flow_stat_card("Limits", Map.get(counts, :limits, 0), "distributed credit owners")}
      #{render_flow_stat_card("Circuits", Map.get(counts, :circuits, 0), "durable effect circuit scopes")}
      #{render_flow_stat_card("Open", Map.get(counts, :open_circuits, 0), "currently rejecting effects")}
    </div>
    #{render_flow_governance_flash(Map.get(data, :flash))}
    #{render_flow_governance_error(Map.get(data, :error))}
    """
  end

  def render_flow_governance_filters(data) do
    filters = Map.get(data, :filters, %{})

    """
    <form class="flow-search" action="/dashboard/flow/governance" method="get" aria-label="Governance filters">
      <input class="flow-search-input mono" type="search" name="scope" value="#{escape(Map.get(filters, :scope, "") || "")}" placeholder="scope / tenant" title="Governance scope filter">
      <input class="flow-search-input mono" type="search" name="flow_id" value="#{escape(Map.get(filters, :flow_id, "") || "")}" placeholder="flow id" title="Approval flow id filter">
      <select class="flow-search-input mono" name="status" title="Approval status filter">
        #{status_options(Map.get(filters, :status))}
      </select>
      <select class="flow-search-input mono" name="circuit_status" title="Circuit status filter">
        #{circuit_status_options(Map.get(filters, :circuit_status))}
      </select>
      <input class="flow-search-input mono flow-filter-limit" type="number" min="1" max="500" name="limit" value="#{Map.get(filters, :limit, 100)}" title="Maximum records per governance section">
      <button class="flow-search-button" type="submit">Refresh</button>
    </form>
    """
  end

  def render_flow_governance_state_meta_filters(data) do
    filters = Map.get(data, :filters, %{})

    """
    <div class="section-title">State Metadata</div>
    <form class="flow-search" action="/dashboard/flow/governance" method="get" aria-label="State metadata filters">
      <input class="flow-search-input mono" type="search" name="meta_type" value="#{escape_attr(Map.get(filters, :meta_type, "") || "")}" placeholder="workflow type" title="Workflow type with an indexed state metadata policy">
      <input class="flow-search-input mono" type="search" name="meta_state" value="#{escape_attr(Map.get(filters, :meta_state, "") || "")}" placeholder="metadata state" title="State whose metadata should be matched">
      <input class="flow-search-input mono" type="search" name="meta_key" value="#{escape_attr(Map.get(filters, :meta_key, "") || "")}" placeholder="ai.model / risk_tier" title="Indexed state metadata key">
      <input class="flow-search-input mono" type="search" name="meta_value" value="#{escape_attr(Map.get(filters, :meta_value, "") || "")}" placeholder="gpt-5 / high" title="State metadata value">
      <select class="flow-search-input mono" name="meta_value_type" title="State metadata value type">
        #{state_meta_value_type_options(Map.get(filters, :meta_value_type))}
      </select>
      <input class="flow-search-input mono" type="search" name="meta_partition_key" value="#{escape_attr(Map.get(filters, :meta_partition_key, "") || "")}" placeholder="partition optional" title="Optional partition key">
      <input class="flow-search-input mono flow-filter-limit" type="number" min="1" max="500" name="limit" value="#{Map.get(filters, :limit, 100)}" title="Maximum records returned">
      <button class="flow-search-button" type="submit">Search</button>
    </form>
    """
  end

  def render_flow_governance_state_meta(data) when is_map(data) do
    result =
      Map.get(data, :state_meta_result, %{
        status: :idle,
        command: "FLOW.SEARCH",
        rows: [],
        message: "Enter workflow type, metadata state, key, and value"
      })

    filters = Map.get(data, :filters, %{})
    rows = Map.get(result, :rows, [])

    rendered_rows =
      if rows == [] do
        ~s(<tr><td colspan="7" class="c-muted">No state metadata records loaded.</td></tr>)
      else
        Enum.map_join(rows, "\n", &state_meta_row(&1, filters))
      end

    """
    <div class="section-title">State Metadata Results <span class="badge badge-idle">#{escape(Map.get(result, :command, "FLOW.SEARCH"))}</span></div>
    #{state_meta_status(result)}
    <table>
      <thead><tr><th>ID</th><th>Type</th><th>Current State</th><th>Metadata State</th><th>Indexed Key</th><th>Metadata</th><th>Updated</th></tr></thead>
      <tbody>#{rendered_rows}</tbody>
    </table>
    """
  end

  def render_flow_governance_circuit_actions do
    """
    <div class="section-title">Circuit Actions</div>
    <form class="flow-search" action="/dashboard/flow/governance" method="post" aria-label="Circuit breaker actions">
      <input class="flow-search-input mono" type="search" name="scope" placeholder="effect scope, e.g. effect:payment.charge" required>
      <input class="flow-search-input mono flow-filter-limit" type="number" min="1" name="failure_threshold" value="3" title="Failures before automatic open">
      <input class="flow-search-input mono flow-filter-limit" type="number" min="1" name="open_ms" value="30000" title="Open duration in milliseconds">
      <button class="flow-search-button" type="submit" name="action" value="open_circuit">Open</button>
      <button class="flow-search-button" type="submit" name="action" value="close_circuit">Close</button>
    </form>
    """
  end

  def render_flow_governance_circuit_graph(circuits) when is_list(circuits) do
    total = max(length(circuits), 1)
    open = Enum.count(circuits, &(Map.get(&1, :status) == :open))
    half_open = Enum.count(circuits, &(Map.get(&1, :status) == :half_open))
    closed = Enum.count(circuits, &(Map.get(&1, :status) == :closed))

    """
    <div class="section-title">Circuit Status Mix</div>
    <div class="flow-bars" role="img" aria-label="Circuit status distribution">
      #{circuit_bar("open", open, total, "status-bad")}
      #{circuit_bar("half-open", half_open, total, "status-warn")}
      #{circuit_bar("closed", closed, total, "status-good")}
    </div>
    """
  end

  def render_flow_governance_circuits(circuits) when is_list(circuits) do
    rows =
      if circuits == [] do
        ~s(<tr><td colspan="9" class="c-muted">No governance circuits found.</td></tr>)
      else
        Enum.map_join(circuits, "\n", &circuit_row/1)
      end

    """
    <div class="section-title">Circuits</div>
    <table>
      <thead><tr><th>Scope</th><th>Status</th><th>Failures</th><th>Threshold</th><th>Retry After</th><th>Last Failure</th><th>Last Success</th><th>Updated</th><th>Actions</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table>
    """
  end

  def render_flow_governance_approvals(approvals) when is_list(approvals) do
    rows =
      if approvals == [] do
        ~s(<tr><td colspan="8" class="c-muted">No approval requests found.</td></tr>)
      else
        Enum.map_join(approvals, "\n", &approval_row/1)
      end

    """
    <div class="section-title">Approvals</div>
    <table>
      <thead><tr><th>ID</th><th>Status</th><th>Flow</th><th>Scope</th><th>Requested</th><th>Expires</th><th>Policy</th><th>Reason</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table>
    """
  end

  def render_flow_governance_budgets(budgets) when is_list(budgets) do
    rows =
      if budgets == [] do
        ~s(<tr><td colspan="8" class="c-muted">No governance budgets found.</td></tr>)
      else
        Enum.map_join(budgets, "\n", &budget_row/1)
      end

    """
    <div class="section-title">Budgets</div>
    <table>
      <thead><tr><th>Scope</th><th>Used</th><th>Remaining</th><th>Limit</th><th>Over</th><th>Reservations</th><th>Window</th><th>Window Start</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table>
    """
  end

  def render_flow_governance_limits(limits) when is_list(limits) do
    rows =
      if limits == [] do
        ~s(<tr><td colspan="5" class="c-muted">No governance limits found.</td></tr>)
      else
        Enum.map_join(limits, "\n", &limit_row/1)
      end

    """
    <div class="section-title">Limits</div>
    <table>
      <thead><tr><th>Scope</th><th>Free</th><th>Limit</th><th>Epoch</th><th>Leases</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table>
    """
  end

  defp render_flow_governance_error(nil), do: ""

  defp render_flow_governance_error(reason),
    do: ~s(<div class="flow-alert flow-alert-error">#{escape(reason)}</div>)

  defp render_flow_governance_flash(%{kind: :ok, message: message}),
    do: ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)

  defp render_flow_governance_flash(%{kind: :error, message: message}),
    do: ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)

  defp render_flow_governance_flash(_flash), do: ""

  defp status_options(selected) do
    [
      {"", "all statuses"},
      {"pending", "pending"},
      {"approved", "approved"},
      {"rejected", "rejected"}
    ]
    |> Enum.map_join(fn {value, label} ->
      selected_attr = if value == to_string(selected || ""), do: " selected", else: ""
      ~s(<option value="#{escape(value)}"#{selected_attr}>#{escape(label)}</option>)
    end)
  end

  defp circuit_status_options(selected) do
    [
      {"", "all circuits"},
      {"open", "open"},
      {"half_open", "half-open"},
      {"closed", "closed"}
    ]
    |> Enum.map_join(fn {value, label} ->
      selected_attr = if value == to_string(selected || ""), do: " selected", else: ""
      ~s(<option value="#{escape(value)}"#{selected_attr}>#{escape(label)}</option>)
    end)
  end

  defp state_meta_value_type_options(selected) do
    [
      {"string", "string"},
      {"integer", "integer"},
      {"float", "float"},
      {"boolean", "boolean"}
    ]
    |> Enum.map_join(fn {value, label} ->
      selected_attr = if value == to_string(selected || "string"), do: " selected", else: ""
      ~s(<option value="#{escape(value)}"#{selected_attr}>#{escape(label)}</option>)
    end)
  end

  defp state_meta_status(%{status: :ok, message: message}),
    do: ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)

  defp state_meta_status(%{status: :idle, message: message}),
    do: ~s(<div class="flow-section-note">#{escape(message)}</div>)

  defp state_meta_status(%{message: message}),
    do: ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)

  defp state_meta_status(_result), do: ""

  defp state_meta_row(record, filters) when is_map(record) do
    state = flow_record_state(record)
    meta_state = Map.get(filters, :meta_state)
    meta_key = Map.get(filters, :meta_key)

    """
    <tr>
      <td class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</td>
      <td class="mono">#{escape(flow_record_type(record))}</td>
      <td class="#{flow_state_class(state)}">#{escape(state)}</td>
      <td class="mono">#{escape(meta_state || "-")}</td>
      <td class="mono">#{escape(flow_record_indexed_state_meta(record) || "-")}</td>
      <td>#{state_meta_badges(record, meta_state, meta_key)}</td>
      <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
    </tr>
    """
  end

  defp state_meta_row(record, _filters) do
    """
    <tr>
      <td class="mono">#{escape(inspect(record, limit: 5))}</td>
      <td colspan="6" class="c-muted">non-record result</td>
    </tr>
    """
  end

  defp state_meta_badges(record, selected_state, selected_key) do
    entries =
      record
      |> flow_record_state_meta()
      |> Enum.flat_map(fn {state, meta} ->
        Enum.map(meta, fn {key, value} -> {state, key, value} end)
      end)
      |> Enum.sort_by(fn {state, key, _value} -> {state, key} end)

    case entries do
      [] ->
        ~s(<span class="c-muted">none</span>)

      _ ->
        entries
        |> Enum.take(32)
        |> Enum.map_join(" ", fn {state, key, value} ->
          class =
            if state == selected_state and key == selected_key do
              "badge badge-ok"
            else
              "badge badge-idle"
            end

          label = "#{state}.#{key}=#{state_meta_value(value)}"
          ~s(<span class="#{class}">#{escape(label)}</span>)
        end)
        |> maybe_append_state_meta_overflow(length(entries))
    end
  end

  defp maybe_append_state_meta_overflow(html, count) when count > 32,
    do: html <> ~s( <span class="badge badge-idle">+#{count - 32} more</span>)

  defp maybe_append_state_meta_overflow(html, _count), do: html

  defp state_meta_value(value) when is_binary(value), do: value
  defp state_meta_value(value) when is_integer(value), do: Integer.to_string(value)
  defp state_meta_value(value) when is_float(value), do: Float.to_string(value)
  defp state_meta_value(value) when is_boolean(value), do: to_string(value)
  defp state_meta_value(value), do: inspect(value, limit: 10)

  defp circuit_bar(label, value, total, class) do
    percent = value * 100 / total

    """
    <div class="flow-bar-row">
      <span class="mono">#{escape(label)}</span>
      <div class="flow-bar-track"><span class="#{class}" style="width: #{Float.round(percent, 1)}%"></span></div>
      <span class="mono">#{format_number(value)}</span>
    </div>
    """
  end

  defp circuit_row(circuit) do
    scope = Map.get(circuit, :scope, "")

    """
    <tr>
      <td class="mono">#{escape(scope)}</td>
      <td>#{circuit_badge(Map.get(circuit, :status))}</td>
      <td>#{format_number(Map.get(circuit, :failure_count, 0))}</td>
      <td>#{format_number(Map.get(circuit, :failure_threshold, 0))}</td>
      <td>#{format_retry_after(Map.get(circuit, :retry_after_ms))}</td>
      <td>#{format_timestamp_ms_or_dash(Map.get(circuit, :last_failure_ms))}</td>
      <td>#{format_timestamp_ms_or_dash(Map.get(circuit, :last_success_ms))}</td>
      <td>#{format_timestamp_ms_or_dash(Map.get(circuit, :updated_at_ms))}</td>
      <td>#{circuit_actions(scope, Map.get(circuit, :status))}</td>
    </tr>
    """
  end

  defp circuit_badge(:open), do: ~s(<span class="status-badge status-bad">open</span>)
  defp circuit_badge(:half_open), do: ~s(<span class="status-badge status-warn">half-open</span>)
  defp circuit_badge(:closed), do: ~s(<span class="status-badge status-good">closed</span>)
  defp circuit_badge(status), do: ~s(<span class="status-badge">#{escape(status || "-")}</span>)

  defp format_retry_after(nil), do: "-"
  defp format_retry_after(value), do: "#{format_number(value)} ms"

  defp circuit_actions("", _status), do: "-"

  defp circuit_actions(scope, :closed) do
    circuit_action_button(scope, "open_circuit", "Open")
  end

  defp circuit_actions(scope, _status) do
    circuit_action_button(scope, "close_circuit", "Close")
  end

  defp circuit_action_button(scope, action, label) do
    """
    <form style="display:inline" action="/dashboard/flow/governance" method="post">
      <input type="hidden" name="scope" value="#{escape_attr(scope)}">
      <button class="flow-search-button" type="submit" name="action" value="#{escape_attr(action)}">#{escape(label)}</button>
    </form>
    """
  end

  def render_flow_governance_circuit_timeline(circuits) when is_list(circuits) do
    events =
      circuits
      |> Enum.flat_map(fn circuit ->
        circuit
        |> Map.get(:events, [])
        |> Enum.map(&Map.put(&1, :scope, Map.get(circuit, :scope, "")))
      end)
      |> Enum.sort_by(&Map.get(&1, :at_ms, 0), :desc)
      |> Enum.take(100)

    rows =
      if events == [] do
        ~s(<tr><td colspan="7" class="c-muted">No circuit events found.</td></tr>)
      else
        Enum.map_join(events, "\n", &circuit_event_row/1)
      end

    """
    <div class="section-title">Circuit Timeline</div>
    <table>
      <thead><tr><th>Time</th><th>Scope</th><th>Event</th><th>Status</th><th>Failures</th><th>Latency</th><th>Error Class</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table>
    """
  end

  defp circuit_event_row(event) do
    """
    <tr>
      <td>#{format_timestamp_ms_or_dash(Map.get(event, :at_ms))}</td>
      <td class="mono">#{escape(event_text(Map.get(event, :scope)))}</td>
      <td class="mono">#{escape(event_text(Map.get(event, :kind)))}</td>
      <td>#{circuit_badge(Map.get(event, :status))}</td>
      <td>#{format_number(Map.get(event, :failures, 0))}</td>
      <td>#{format_retry_after(Map.get(event, :latency_ms))}</td>
      <td class="mono">#{escape(event_text(Map.get(event, :error_class)))}</td>
    </tr>
    """
  end

  defp event_text(nil), do: "-"
  defp event_text(value) when is_binary(value), do: value
  defp event_text(value), do: to_string(value)

  defp approval_row(approval) do
    """
    <tr>
      <td class="mono">#{escape(Map.get(approval, :id, "-"))}</td>
      <td>#{escape(Map.get(approval, :status, "-"))}</td>
      <td class="mono">#{escape(Map.get(approval, :flow_id, "-"))}</td>
      <td class="mono">#{escape(Map.get(approval, :scope, "-"))}</td>
      <td>#{format_timestamp_ms_or_dash(Map.get(approval, :requested_at_ms))}</td>
      <td>#{format_timestamp_ms_or_dash(Map.get(approval, :expires_at_ms))}</td>
      <td class="mono">#{escape(Map.get(approval, :policy_version, Map.get(approval, :policy_hash, "-")) || "-")}</td>
      <td>#{escape(Map.get(approval, :reason, "-") || "-")}</td>
    </tr>
    """
  end

  defp budget_row(budget) do
    """
    <tr>
      <td class="mono">#{escape(Map.get(budget, :scope, "-"))}</td>
      <td>#{format_number(Map.get(budget, :used, 0))}</td>
      <td>#{format_number(Map.get(budget, :remaining, 0))}</td>
      <td>#{format_number(Map.get(budget, :limit, 0))}</td>
      <td>#{format_budget_over(Map.get(budget, :over_budget, false))}</td>
      <td>#{format_number(Map.get(budget, :reservations_count, 0))}</td>
      <td>#{format_number(Map.get(budget, :window_ms, 0))} ms</td>
      <td>#{format_timestamp_ms_or_dash(Map.get(budget, :window_start_ms))}</td>
    </tr>
    """
  end

  defp format_budget_over(true), do: ~s(<span class="status-badge status-bad">yes</span>)
  defp format_budget_over(_), do: ~s(<span class="status-badge status-good">no</span>)

  defp limit_row(limit) do
    leases = Map.get(limit, :leases, %{})

    """
    <tr>
      <td class="mono">#{escape(Map.get(limit, :scope, "-"))}</td>
      <td>#{format_number(Map.get(limit, :free, 0))}</td>
      <td>#{format_number(Map.get(limit, :limit, 0))}</td>
      <td>#{format_number(Map.get(limit, :epoch, 0))}</td>
      <td class="mono">#{format_number(map_size(leases))}</td>
    </tr>
    """
  end
end
