defmodule FerricstoreServer.Health.Dashboard.Render.FlowGovernance do
  alias FerricstoreServer.Health.Dashboard.Flow.Governance
  alias FerricstoreServer.Health.Dashboard.Render.FlowQueryResults
  alias FerricstoreServer.Health.Dashboard.Render.{FlowNavigation, StateMetadata}
  alias Ferricstore.Flow.Governance.CircuitStore

  @metadata_fields ~w(meta_type meta_state meta_key meta_value meta_value_type meta_partition_key meta_cursor)
  @overview_fields ~w(scope approval_status flow_id circuit_status limit)
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.FlowRecord

  import FerricstoreServer.Health.Dashboard.Render.FlowHistory,
    only: [flow_state_class: 1, render_flow_id_link: 2]

  import FerricstoreServer.Health.Dashboard.Render.FlowOverview, only: [render_flow_stat_card: 3]

  def render_flow_governance_form_recovery(data) do
    draft = Map.get(data, :action_draft, %{})
    action = Map.get(draft, "action", "")
    filters = Map.put(Map.get(data, :filters, %{}), :action_draft, draft)

    content =
      case Map.get(data, :review) do
        %{approval: approval, action_capabilities: capabilities} ->
          approval_confirmation(
            Map.put(approval, :action_capabilities, capabilities),
            action,
            action_label(action),
            filters,
            action == "reject_approval"
          )

        %{scope: scope, circuit: circuit, fingerprint: fingerprint} = review ->
          current = circuit || %{scope: scope, status: nil}
          filters = Map.put(filters, :action_capabilities, review.action_capabilities)
          circuit_action_button(current, action, action_label(action), filters, fingerprint)

        _ ->
          recovery_review_form(draft, filters)
      end

    """
    <section aria-labelledby="governance-recovery-title">
      <h2 class="section-title" id="governance-recovery-title">#{escape(action_label(action))} review</h2>
      #{render_flow_governance_error(Map.get(data, :error))}
      #{content}
      <p><a class="flow-link" href="#{escape_attr(governance_return_path(filters))}">Back to governance</a></p>
    </section>
    """
  end

  defp recovery_review_form(draft, filters) do
    action = Map.get(draft, "action", "")
    circuit? = action in ["open_circuit", "close_circuit"]
    target_fields = if circuit?, do: ~w(scope), else: ~w(approval_id approval_scope)

    target =
      Enum.map_join(target_fields, "", fn field ->
        value = Map.get(draft, field, "")
        label = if field == "approval_id", do: "Approval ID", else: "Effect scope"

        ~s(<div><dt>#{label}</dt><dd class="mono">#{escape(value)}</dd></div>)
      end)

    target_inputs =
      Enum.map_join(target_fields, "", fn field ->
        ~s(<input type="hidden" name="#{field}" value="#{escape_attr(Map.get(draft, field, ""))}">)
      end)

    """
    <p class="flow-section-note">Draft retained. Refresh the exact target, review its current state, and confirm again before submitting.</p>
    <form class="flow-policy-form" action="/dashboard/flow/governance" method="post" data-dashboard-single-submit data-dashboard-returned-draft="true">
      <input type="hidden" name="action" value="#{escape_attr(action)}">
      <input type="hidden" name="review_only" value="true">
      #{target_inputs}
      #{if circuit?, do: circuit_filter_inputs(filters), else: governance_filter_inputs(filters)}
      <dl class="flow-action-target">#{target}</dl>
      #{cond do
      action == "open_circuit" -> circuit_open_settings(%{}, draft)
      circuit? -> ""
      true -> approval_reason_field(draft)
    end}
      <button class="flow-search-button" type="submit">Refresh and review</button>
    </form>
    """
  end

  defp action_label("open_circuit"), do: "Open"
  defp action_label("close_circuit"), do: "Close"
  defp action_label("approve_approval"), do: "Approve"
  defp action_label("reject_approval"), do: "Reject"
  defp action_label(_), do: "Governance action"

  defp governance_return_path(filters) do
    "/dashboard/flow/governance?" <> URI.encode_query(Governance.filter_params(filters))
  end

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
      #{governance_filter_inputs(filters, @overview_fields ++ ["meta_cursor"])}
      <label class="flow-policy-field"><span>Governance scope</span><input class="flow-search-input mono" type="search" name="scope" value="#{escape(Map.get(filters, :scope, "") || "")}" placeholder="all visible scopes" title="Governance scope filter"></label>
      <label class="flow-policy-field"><span>Workflow ID</span><input class="flow-search-input mono" type="search" name="flow_id" value="#{escape(Map.get(filters, :flow_id, "") || "")}" placeholder="all visible workflows" title="Approval flow id filter"></label>
      <label class="flow-policy-field"><span>Approval status</span><select class="flow-search-input mono" name="approval_status" title="Approval status filter">
        #{status_options(Map.get(filters, :status))}
      </select></label>
      <label class="flow-policy-field"><span>Circuit status</span><select class="flow-search-input mono" name="circuit_status" title="Circuit status filter">
        #{circuit_status_options(Map.get(filters, :circuit_status))}
      </select></label>
      <label class="flow-policy-field"><span>Max records per section</span><input class="flow-search-input mono flow-filter-limit" type="number" min="1" max="100" name="limit" value="#{Map.get(filters, :limit, 100)}" title="Maximum records per governance section"></label>
      <button class="flow-search-button" type="submit">Refresh</button>
    </form>
    """
  end

  def state_meta_open?(data) do
    result = Map.get(data, :state_meta_result, %{})
    filters = Map.get(data, :filters, %{})

    Map.get(result, :status, :idle) != :idle or
      Enum.any?([:meta_type, :meta_state, :meta_key, :meta_value, :meta_partition_key], fn key ->
        Map.get(filters, key) not in [nil, ""]
      end)
  end

  def render_flow_governance_state_meta_filters(data) do
    filters = Map.get(data, :filters, %{})

    """
    <h2 class="section-title">State Metadata</h2>
    <form class="flow-governance-meta-form" action="/dashboard/flow/governance" method="get" aria-label="State metadata filters">
      #{governance_filter_inputs(filters, @metadata_fields ++ ["limit"])}
      <label class="flow-policy-field"><span>Workflow type</span><input class="flow-search-input mono" type="search" name="meta_type" value="#{escape_attr(Map.get(filters, :meta_type, "") || "")}" title="Required workflow type with an indexed state metadata policy" required></label>
      <label class="flow-policy-field"><span>Metadata state</span><input class="flow-search-input mono" type="search" name="meta_state" value="#{escape_attr(Map.get(filters, :meta_state, "") || "")}" title="Required state whose metadata should be matched" required></label>
      <label class="flow-policy-field"><span>Indexed key</span><input class="flow-search-input mono" type="search" name="meta_key" value="#{escape_attr(Map.get(filters, :meta_key, "") || "")}" title="Required indexed state metadata key" required></label>
      <label class="flow-policy-field"><span>Exact value</span><input class="flow-search-input mono" type="search" name="meta_value" value="#{escape_attr(Map.get(filters, :meta_value, "") || "")}" title="Exact state metadata value; blank matches an empty string"></label>
      <label class="flow-policy-field"><span>Value type</span><select class="flow-search-input mono" name="meta_value_type" title="State metadata value type">
        #{state_meta_value_type_options(Map.get(filters, :meta_value_type))}
      </select></label>
      <label class="flow-policy-field"><span>Partition key</span><input class="flow-search-input mono" type="search" name="meta_partition_key" value="#{escape_attr(Map.get(filters, :meta_partition_key, "") || "")}" title="Required partition key" required></label>
      <label class="flow-policy-field"><span>Max records</span><input class="flow-search-input mono" type="number" min="1" max="100" name="limit" value="#{Map.get(filters, :limit, 100)}" title="Maximum records returned"></label>
      <button class="flow-search-button" type="submit">Search</button>
    </form>
    """
  end

  def render_flow_governance_state_meta(data) when is_map(data) do
    result =
      Map.get(data, :state_meta_result, %{
        status: :idle,
        command: "FLOW.QUERY",
        rows: [],
        message: "Enter partition, workflow type, metadata state, key, and value"
      })

    filters = Map.get(data, :filters, %{})
    rows = Map.get(result, :rows, [])

    rendered_rows =
      if rows == [] do
        message =
          if get_in(result, [:page, :has_more]) == true,
            do: "No visible records on this page. Continue to the next page.",
            else: "No state metadata records loaded."

        ~s(<tr><td colspan="7" class="c-muted">#{message}</td></tr>)
      else
        Enum.map_join(rows, "\n", &state_meta_row(&1, filters))
      end

    """
    <h2 class="section-title">State Metadata Results <span class="badge badge-idle">#{escape(Map.get(result, :command, "FLOW.QUERY"))}</span></h2>
    #{state_meta_status(result)}
    #{FlowQueryResults.render_flow_query_metadata(Map.take(result, [:quality]))}
    <div class="table-scroll" role="region" aria-label="Workflow state metadata results" tabindex="0"><table>
      <thead><tr><th>ID</th><th>Type</th><th>Current State</th><th>Metadata State</th><th>Indexed Key</th><th>Metadata</th><th>Updated</th></tr></thead>
      <tbody>#{rendered_rows}</tbody>
    </table></div>
    #{metadata_pagination(result, filters)}
    """
  end

  def render_flow_governance_circuit_actions(filters \\ %{})

  def render_flow_governance_circuit_actions(%{
        action_capabilities: %{open_circuit: false, close_circuit: false}
      }),
      do:
        ~s(<p class="flow-section-note">Circuit actions require the corresponding FLOW.CIRCUIT command and write access to the selected effect scope.</p>)

  def render_flow_governance_circuit_actions(filters) do
    review = Map.get(filters, :circuit_review)
    scope = if is_map(review), do: Map.get(review, :scope, ""), else: ""

    """
    <h2 class="section-title">Circuit Actions</h2>
    <form class="flow-search" action="/dashboard/flow/governance" method="get" aria-label="Review circuit scope">
      #{governance_filter_inputs(filters)}
      <label class="flow-policy-field"><span>Effect scope</span><input class="flow-search-input mono" type="search" name="circuit_review_scope" value="#{escape_attr(scope)}" placeholder="effect:payment.charge" required></label>
      <button class="flow-search-button" type="submit">Review circuit</button>
    </form>
    #{render_circuit_review(review, filters)}
    """
  end

  defp render_circuit_review(nil, _filters), do: ""

  defp render_circuit_review(%{status: :error, message: message}, _filters),
    do: ~s(<p class="flow-alert flow-alert-error" role="status">#{escape(message)}</p>)

  defp render_circuit_review(
         %{status: :ok, scope: scope, circuit: circuit, fingerprint: fingerprint} = review,
         filters
       ) do
    filters = Map.put(filters, :action_capabilities, Map.get(review, :action_capabilities))
    current = circuit || %{scope: scope, status: nil}

    """
    <section class="flow-circuit-review" aria-label="Reviewed circuit">
      <h3 class="section-title">Reviewed circuit</h3>
      <p class="mono">#{escape(scope)}</p>
      <p>Current status: #{escape(if circuit, do: to_string(circuit.status), else: "not configured")}</p>
      #{circuit_action_button(current, "open_circuit", "Open", filters, fingerprint)}
      #{if circuit, do: circuit_action_button(current, "close_circuit", "Close", filters, fingerprint), else: ""}
    </section>
    """
  end

  def render_flow_governance_circuit_graph(circuits) when is_list(circuits) do
    total = max(length(circuits), 1)
    open = Enum.count(circuits, &(Map.get(&1, :status) == :open))
    half_open = Enum.count(circuits, &(Map.get(&1, :status) == :half_open))
    closed = Enum.count(circuits, &(Map.get(&1, :status) == :closed))

    """
    <h2 class="section-title">Circuit Status Mix</h2>
    <div class="flow-bars" role="img" aria-label="Circuit status distribution">
      #{circuit_bar("open", open, total, "status-bad")}
      #{circuit_bar("half-open", half_open, total, "status-warn")}
      #{circuit_bar("closed", closed, total, "status-good")}
    </div>
    """
  end

  def render_flow_governance_circuits(circuits, filters \\ %{}) when is_list(circuits) do
    rows =
      if circuits == [] do
        ~s(<tr><td colspan="9" class="c-muted">No governance circuits found.</td></tr>)
      else
        Enum.map_join(circuits, "\n", &circuit_row(&1, filters))
      end

    """
    <h2 class="section-title">Circuits</h2>
    <div class="table-scroll" role="region" aria-label="Governance circuits" tabindex="0"><table>
      <thead><tr><th>Scope</th><th>Status</th><th>Failures</th><th>Threshold</th><th>Retry After</th><th>Last Failure</th><th>Last Success</th><th>Updated</th><th>Actions</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table></div>
    """
  end

  def render_flow_governance_approvals(approvals) when is_list(approvals),
    do: render_flow_governance_approvals(approvals, %{})

  def render_flow_governance_approvals(approvals, filters)
      when is_list(approvals) and is_map(filters) do
    rows =
      if approvals == [] do
        ~s(<tr><td colspan="9" class="c-muted">No approval requests found.</td></tr>)
      else
        Enum.map_join(approvals, "\n", &approval_row(&1, filters))
      end

    """
    <h2 class="section-title">Approvals</h2>
    <div class="table-scroll" role="region" aria-label="Governance approvals" tabindex="0"><table>
      <thead><tr><th>ID</th><th>Status</th><th>Flow</th><th>Scope</th><th>Requested</th><th>Expires</th><th>Policy</th><th>Reason</th><th>Decision</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table></div>
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
    <h2 class="section-title">Budgets</h2>
    <div class="table-scroll" role="region" aria-label="Governance budgets" tabindex="0"><table>
      <thead><tr><th>Scope</th><th>Used</th><th>Remaining</th><th>Limit</th><th>Over</th><th>Reservations</th><th>Window</th><th>Window Start</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table></div>
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
    <h2 class="section-title">Limits</h2>
    <div class="table-scroll" role="region" aria-label="Governance concurrency limits" tabindex="0"><table>
      <thead><tr><th>Scope</th><th>Free</th><th>Limit</th><th>Epoch</th><th>Leases</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table></div>
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
      {"rejected", "rejected"},
      {"expired", "expired"}
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
    StateMetadata.render(record, selected: {selected_state, selected_key}, compact: true)
  end

  defp circuit_bar(label, value, total, class) do
    percent = value * 100 / total

    fill =
      if value > 0,
        do: ~s(<span class="#{class}" style="width: #{Float.round(percent, 1)}%"></span>),
        else: ""

    """
    <div class="flow-bar-row">
      <span class="mono">#{escape(label)}</span>
      <div class="flow-bar-track">#{fill}</div>
      <span class="mono">#{format_number(value)}</span>
    </div>
    """
  end

  defp circuit_row(circuit, filters) do
    scope = Map.get(circuit, :scope, "")

    filters =
      Map.put(
        filters,
        :action_capabilities,
        Map.get(circuit, :action_capabilities, Map.get(filters, :action_capabilities))
      )

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
      <td>#{circuit_actions(circuit, filters)}</td>
    </tr>
    """
  end

  defp circuit_badge(:open), do: ~s(<span class="status-badge status-bad">open</span>)
  defp circuit_badge(:half_open), do: ~s(<span class="status-badge status-warn">half-open</span>)
  defp circuit_badge(:closed), do: ~s(<span class="status-badge status-good">closed</span>)
  defp circuit_badge(status), do: ~s(<span class="status-badge">#{escape(status || "-")}</span>)

  defp format_retry_after(nil), do: "-"
  defp format_retry_after(value), do: "#{format_number(value)} ms"

  defp circuit_actions(%{scope: ""}, _filters), do: "-"

  defp circuit_actions(%{status: :closed} = circuit, filters) do
    circuit_action_button(circuit, "open_circuit", "Open", filters)
  end

  defp circuit_actions(circuit, filters) do
    circuit_action_button(circuit, "close_circuit", "Close", filters)
  end

  defp circuit_action_button(circuit, action, label, filters, fingerprint \\ nil) do
    if action_allowed?(filters, String.to_existing_atom(action)) do
      scope = Map.get(circuit, :scope, "")
      fingerprint = fingerprint || CircuitStore.review_fingerprint(circuit)
      draft = Map.get(filters, :action_draft, %{})
      retained? = Map.get(draft, "action") == action and Map.get(draft, "scope") == scope

      impact =
        if action == "open_circuit",
          do:
            "New effects in this scope are rejected until the open duration passes; half-open probe rules then apply. In-flight effects are not cancelled.",
          else:
            "Effects in this scope may proceed, subject to other governance checks. Failure and half-open counters are reset."

      """
      <details class="flow-action-confirm"#{if retained?, do: " open", else: ""}>
        <summary class="flow-search-button">Review #{escape(label)}</summary>
        <div class="flow-action-confirm-panel">
          <strong>#{escape(label)} circuit</strong>
          <dl class="flow-action-target">
            <div><dt>Effect scope</dt><dd class="mono">#{escape(scope)}</dd></div>
            <div><dt>Reviewed status</dt><dd>#{escape(to_string(Map.get(circuit, :status) || "not configured"))}</dd></div>
            <div><dt>Updated</dt><dd>#{format_timestamp_ms_or_dash(Map.get(circuit, :updated_at_ms))}</dd></div>
          </dl>
          <p>#{impact}</p>
          <form action="/dashboard/flow/governance" method="post" data-dashboard-single-submit#{if retained?, do: ~s( data-dashboard-returned-draft="true"), else: ""}>
            <input type="hidden" name="scope" value="#{escape_attr(scope)}">
            <input type="hidden" name="action" value="#{escape_attr(action)}">
            <input type="hidden" name="expected_review" value="#{escape_attr(fingerprint)}">
            #{circuit_filter_inputs(filters)}
            #{if action == "open_circuit", do: circuit_open_settings(circuit, if(retained?, do: draft, else: %{})), else: ""}
            <label class="flow-check-label"><input type="checkbox" name="confirm_action" value="true" required>I reviewed this effect scope and the impact of #{String.downcase(label)}.</label>
            <button class="flow-search-button" type="submit">Confirm #{escape(label)}</button>
          </form>
        </div>
      </details>
      """
    else
      ~s(<span class="c-muted">Read only</span>)
    end
  end

  defp circuit_open_settings(circuit, draft) do
    """
    <label class="flow-policy-field"><span>Failure threshold</span><input class="flow-search-input" type="text" inputmode="numeric" name="failure_threshold" value="#{escape_attr(to_string(Map.get(draft, "failure_threshold", Map.get(circuit, :failure_threshold, 3))))}" required></label>
    <label class="flow-policy-field"><span>Open duration (ms)</span><input class="flow-search-input" type="text" inputmode="numeric" name="open_ms" value="#{escape_attr(to_string(Map.get(draft, "open_ms", Map.get(circuit, :open_ms, 30_000))))}" required></label>
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
    <h2 class="section-title">Circuit Timeline</h2>
    <div class="table-scroll" role="region" aria-label="Governance circuit timeline" tabindex="0"><table>
      <thead><tr><th>Time</th><th>Scope</th><th>Event</th><th>Status</th><th>Failures</th><th>Latency</th><th>Error Class</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table></div>
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

  defp approval_row(approval, filters) do
    """
    <tr>
      <td class="mono">#{escape(Map.get(approval, :id, "-"))}</td>
      <td>#{approval |> Map.get(:status, "-") |> event_text() |> escape()}</td>
      <td class="mono">#{FlowNavigation.workflow_reference(Map.get(approval, :flow_id), Map.get(approval, :partition_key))}</td>
      <td class="mono">#{escape(Map.get(approval, :scope, "-"))}</td>
      <td>#{format_timestamp_ms_or_dash(Map.get(approval, :requested_at_ms))}</td>
      <td>#{format_timestamp_ms_or_dash(Map.get(approval, :expires_at_ms))}</td>
      <td class="mono">#{escape(Map.get(approval, :policy_version, Map.get(approval, :policy_hash, "-")) || "-")}</td>
      <td>#{escape(Map.get(approval, :reason, "-") || "-")}</td>
      <td>#{approval_actions(approval, filters)}</td>
    </tr>
    """
  end

  defp approval_actions(%{status: :pending} = approval, filters) do
    [
      if(action_allowed?(approval, :approve_approval),
        do: approval_confirmation(approval, "approve_approval", "Approve", filters)
      ),
      if(action_allowed?(approval, :reject_approval),
        do: approval_confirmation(approval, "reject_approval", "Reject", filters, true)
      )
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
  end

  defp approval_actions(_approval, _filters), do: ~s(<span class="c-muted">decided</span>)

  defp approval_confirmation(approval, action, label, filters, danger? \\ false) do
    id = Map.get(approval, :id, "")
    scope = Map.get(approval, :scope, "")
    requested_at_ms = Map.get(approval, :requested_at_ms, "")
    class = if danger?, do: "flow-search-button flow-danger-button", else: "flow-search-button"
    draft = Map.get(filters, :action_draft, %{})
    retained? = Map.get(draft, "action") == action and Map.get(draft, "approval_id") == id

    """
    <details class="flow-action-confirm"#{if retained?, do: " open", else: ""}>
      <summary class="#{class}">#{escape(label)}</summary>
      <div class="flow-action-confirm-panel">
        <strong>Confirm #{escape(label)}</strong>
        <span class="mono">#{escape(id)}</span>
        <dl class="flow-action-target"><div><dt>Effect scope</dt><dd class="mono">#{escape(scope)}</dd></div><div><dt>Requested</dt><dd>#{format_timestamp_ms_or_dash(requested_at_ms)}</dd></div></dl>
        <form action="/dashboard/flow/governance" method="post" data-dashboard-single-submit#{if retained?, do: ~s( data-dashboard-returned-draft="true"), else: ""}>
          <input type="hidden" name="action" value="#{escape_attr(action)}">
          <input type="hidden" name="approval_id" value="#{escape_attr(id)}">
          <input type="hidden" name="approval_scope" value="#{escape_attr(scope)}">
          <input type="hidden" name="expected_status" value="pending">
          <input type="hidden" name="expected_requested_at_ms" value="#{requested_at_ms |> to_string() |> escape_attr()}">
          #{governance_filter_inputs(filters)}
          #{approval_reason_field(if retained?, do: draft, else: %{})}
          <label class="flow-check-label"><input type="checkbox" name="confirm_action" value="true" required>I reviewed this approval and its current request.</label>
          <button class="#{class}" type="submit">Confirm #{escape(label)}</button>
        </form>
      </div>
    </details>
    """
  end

  defp approval_reason_field(draft) do
    ~s(<label>Decision reason <input class="flow-search-input" type="text" name="decision_reason" maxlength="262144" placeholder="optional" value="#{escape_attr(Map.get(draft, "decision_reason", ""))}"></label>)
  end

  defp governance_filter_inputs(filters, except \\ []) do
    filters
    |> Governance.filter_params()
    |> Map.drop(except)
    |> Enum.sort()
    |> Enum.map_join("", fn {name, value} ->
      ~s(<input type="hidden" name="#{name}" value="#{value |> to_string() |> escape_attr()}">)
    end)
  end

  defp action_allowed?(data, action) do
    case Map.get(data, :action_capabilities) do
      nil -> true
      capabilities -> Map.get(capabilities, action, false)
    end
  end

  defp circuit_filter_inputs(filters) do
    governance_filter_inputs(filters, ["scope"]) <>
      ~s(<input type="hidden" name="return_scope" value="#{escape_attr(Map.get(filters, :scope, Map.get(filters, "scope")) || "")}">)
  end

  defp metadata_pagination(result, filters) do
    link = fn label, cursor ->
      ~s(<a class="flow-history-page-link" href="#{escape_attr(Governance.metadata_path(filters, cursor))}">#{label}</a>)
    end

    first =
      if Map.get(filters, :meta_cursor) not in [nil, ""], do: link.("First page", nil), else: ""

    next =
      case {Map.get(result, :status), Map.get(result, :page)} do
        {:ok, %{has_more: true, cursor: cursor}} when is_binary(cursor) and cursor != "" ->
          link.("Next page", cursor)

        {:ok, %{has_more: true}} ->
          ~s(<span class="flow-section-note">More records exist; narrow the query or retry from the first page.</span>)

        _ ->
          ""
      end

    retry =
      if Map.get(result, :status) in [:error, :timeout],
        do: link.("Retry query", Map.get(filters, :meta_cursor)),
        else: ""

    ~s(<nav class="flow-history-pages" aria-label="State metadata pages">#{first}#{next}#{retry}</nav>)
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
