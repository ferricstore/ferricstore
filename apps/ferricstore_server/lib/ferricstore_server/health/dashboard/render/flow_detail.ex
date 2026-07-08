defmodule FerricstoreServer.Health.Dashboard.Render.FlowDetail do
  import FerricstoreServer.Health.Dashboard.Format

  import FerricstoreServer.Health.Dashboard.FlowRecord,
    except: [
      flow_named_value_refs: 1,
      flow_value_ref_entries: 2,
      normalize_flow_named_value_refs: 1,
      normalize_flow_value_ref: 1
    ]

  import FerricstoreServer.Health.Dashboard.Render.FlowHistory
  import FerricstoreServer.Health.Dashboard.Render.FlowTables

  @flow_dashboard_value_ref_limit 40
  @flow_terminal_states ~w(completed failed cancelled)

  def flow_record_status_label(record) do
    state = flow_record_state(record)

    cond do
      state in @flow_terminal_states -> "terminal"
      flow_expired_lease?(record) -> "expired lease"
      state == "running" -> "running"
      flow_retrying?(record) -> "retrying"
      flow_scheduled_future?(record) -> "scheduled"
      flow_due_now?(record) -> "due"
      true -> "active"
    end
  end

  def render_flow_detail_flash(%{flash: %{kind: :ok, message: message}})
      when is_binary(message) do
    ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)
  end

  def render_flow_detail_flash(%{flash: %{kind: :error, message: message}})
      when is_binary(message) do
    ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)
  end

  def render_flow_detail_flash(_data), do: ""

  def render_flow_detail(%{record: nil} = data) do
    reason =
      case Map.get(data, :record_status) do
        :timeout ->
          "Flow lookup timed out. The Flow record may still exist, but the dashboard did not wait for a slow FLOW.GET path."

        {:error, error} ->
          dashboard_internal_error("Flow lookup failed", error)

        {:exit, error} ->
          dashboard_internal_error("Flow lookup exited", :exit, error)

        _ ->
          "Flow #{data.id} was not found in the hot state sample or default Flow lookup."
      end

    """
    <div class="section-title">Flow Detail</div>
    <div class="pressure-alert level-warning">
      <div class="pressure-details">#{escape(reason)}</div>
    </div>
    """
  end

  def render_flow_detail(data) do
    record = data.record
    state = flow_record_state(record)
    logical_state = flow_record_logical_state(record)
    state_mode = Map.get(data, :state_mode, :parallel)

    """
    <div class="section-title">Flow Detail <span class="badge #{flow_state_badge_class(state)}">#{escape(state)}</span></div>
    <div class="flow-detail-grid">
      <div class="flow-card flow-card-wide">
        <div class="flow-card-label">ID</div>
        <div class="flow-card-value mono" style="font-size:1rem;">#{escape(flow_record_id(record))}</div>
        <div class="flow-card-detail">type #{escape(flow_record_type(record))}</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Why waiting</div>
        <div class="flow-card-value" style="font-size:1rem;">#{escape(data.waiting_reason)}</div>
        <div class="flow-card-detail">computed from current durable state</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">State mode</div>
        <div class="flow-card-value #{flow_detail_mode_class(state_mode)}" style="font-size:1rem;">#{escape(flow_detail_mode_label(state_mode))}</div>
        <div class="flow-card-detail">logical state #{escape(logical_state)}</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Fencing</div>
        <div class="flow-card-value">#{escape(to_string(flow_field(record, :fencing_token, "-")))}</div>
        <div class="flow-card-detail">lease safety token</div>
      </div>
    </div>
    #{render_flow_detail_table(record)}
    #{render_flow_detail_fifo_lane(data)}
    #{render_flow_detail_signals(data)}
    #{render_flow_rewind_action(data)}
    #{render_flow_signal_action(data)}
    """
  end

  def render_flow_detail_fifo_lane(%{fifo_lane: %{} = lane}) do
    render_flow_fifo_lanes([lane], Map.get(lane, :count, 1), Map.get(lane, :count, 1))
  end

  def render_flow_detail_fifo_lane(%{state_mode: :fifo}) do
    """
    <div class="flow-help">This Flow is in a FIFO state, but no lane peers were visible in the bounded dashboard sample.</div>
    """
  end

  def render_flow_detail_fifo_lane(_data), do: ""

  def render_flow_detail_signals(%{record: %{} = record, history: history})
      when is_list(history) do
    rows = flow_signal_rows(record, history)

    render_flow_signals_table(rows, nil, nil, nil, %{}, :detail)
  end

  def render_flow_detail_signals(_data), do: ""

  def render_flow_detail_table(record) do
    fields = [
      {"Type", flow_record_type(record)},
      {"State", flow_record_state(record)},
      {"Logical State", flow_record_logical_state(record)},
      {"Partition", flow_record_partition_key(record) || "auto/global"},
      {"Worker", flow_record_worker(record) || "-"},
      {"Priority", flow_field(record, :priority, 0)},
      {"Attempts", flow_field(record, :attempts, flow_field(record, :attempt, 0))},
      {"Run At", format_timestamp_ms_or_dash(flow_record_run_at_ms(record))},
      {"Lease Expires", format_timestamp_ms_or_dash(flow_record_lease_expires_at_ms(record))},
      {"Updated", format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))},
      {"Parent", flow_field(record, :parent_flow_id, "-")},
      {"Root", flow_field(record, :root_flow_id, "-")},
      {"Correlation", flow_field(record, :correlation_id, "-")},
      {"Attributes", {:safe, render_flow_attribute_badges(record)}},
      {"State Meta", {:safe, render_flow_state_meta_badges(record)}},
      {"Value Refs", {:safe, render_flow_value_ref_badges(record)}}
    ]

    rows =
      Enum.map_join(fields, "\n", fn {label, value} ->
        rendered =
          case value do
            {:safe, html} when is_binary(html) -> html
            value when is_binary(value) -> escape(value)
            value when is_integer(value) -> Integer.to_string(value)
            value -> to_string(value)
          end

        """
        <tr>
          <td class="c-muted">#{escape(label)}</td>
          <td class="mono">#{rendered}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Current State</div>
    <table>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  defp flow_detail_mode_label(:fifo), do: "FIFO"
  defp flow_detail_mode_label("fifo"), do: "FIFO"
  defp flow_detail_mode_label(_mode), do: "parallel"

  defp flow_detail_mode_class(:fifo), do: "c-green"
  defp flow_detail_mode_class("fifo"), do: "c-green"
  defp flow_detail_mode_class(_mode), do: ""

  def render_flow_attribute_badges(record) do
    attrs = flow_record_attributes(record)

    if map_size(attrs) == 0 do
      ~s(<span class="badge badge-idle">none</span>)
    else
      attrs
      |> Enum.sort_by(fn {name, _value} -> name end)
      |> Enum.map_join(" ", fn {name, value} ->
        label = "#{name}=#{flow_attribute_display_value(value)}"
        ~s(<span class="badge badge-idle">#{escape(label)}</span>)
      end)
    end
  end

  defp flow_attribute_display_value(value) when is_binary(value), do: value
  defp flow_attribute_display_value(value) when is_integer(value), do: Integer.to_string(value)
  defp flow_attribute_display_value(value) when is_boolean(value), do: to_string(value)
  defp flow_attribute_display_value(value), do: inspect(value)

  def render_flow_state_meta_badges(record) do
    entries =
      record
      |> flow_record_state_meta()
      |> Enum.flat_map(fn {state, meta} ->
        Enum.map(meta, fn {name, value} -> {state, name, value} end)
      end)
      |> Enum.sort_by(fn {state, name, _value} -> {state, name} end)

    case entries do
      [] ->
        ~s(<span class="badge badge-idle">none</span>)

      _ ->
        entries
        |> Enum.take(32)
        |> Enum.map_join(" ", fn {state, name, value} ->
          label = "#{state}.#{name}=#{flow_attribute_display_value(value)}"
          ~s(<span class="badge badge-idle">#{escape(label)}</span>)
        end)
        |> append_state_meta_overflow(length(entries))
    end
  end

  defp append_state_meta_overflow(html, count) when count > 32,
    do: html <> ~s( <span class="badge badge-idle">+#{count - 32} more</span>)

  defp append_state_meta_overflow(html, _count), do: html

  def render_flow_rewind_action(%{record: %{} = record} = data) do
    targets = flow_rewind_targets(Map.get(data, :history, []))
    id = flow_record_id(record)

    partition_key =
      flow_detail_url_partition_key(
        Map.get(data, :partition_key) || flow_record_partition_key(record)
      )

    action = "/dashboard/flow/" <> URI.encode(id, &URI.char_unreserved?/1) <> "/rewind"
    partition_input = render_flow_rewind_partition_input(partition_key)

    {select, button_attrs} =
      case targets do
        [] ->
          {~s(<select class="flow-search-input mono" name="to_event" title="Loaded history has no state event to rewind to" disabled><option>No rewind target in loaded history</option></select>),
           ~s( disabled title="No rewind target in loaded history")}

        _ ->
          {~s(<select class="flow-search-input mono" name="to_event" title="Choose one of this flow&#39;s loaded history events">#{render_flow_rewind_options(targets)}</select>),
           ~s( title="Create a durable rewind to the selected event")}
      end

    """
    <div class="flow-policy-panel">
      <div class="section-title">Rewind #{info_icon("Rewind creates a durable FLOW.REWIND command to move this flow back to a selected state from its own loaded history.")}</div>
      <form class="flow-policy-form" action="#{escape_attr(action)}" method="post">
        <input type="hidden" name="id" value="#{escape_attr(id)}">
        #{partition_input}
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Target event</span>
            #{select}
          </label>
          <label class="flow-policy-field">
            <span>Run at ms</span>
            <input class="flow-search-input mono" type="number" name="run_at_ms" min="0" placeholder="current" title="Optional run_at override in milliseconds; blank keeps the current scheduler choice">
          </label>
        </div>
        <div class="flow-policy-actions">
          <label class="flow-check-label" title="Required before the dashboard sends FLOW.REWIND.">
            <input type="checkbox" name="confirm_rewind" value="true">
            I reviewed the target event and understand this creates a new rewind event.
          </label>
          <button class="flow-search-button flow-danger-button" type="submit"#{button_attrs}>Rewind</button>
        </div>
      </form>
    </div>
    """
  end

  def render_flow_rewind_action(_data), do: ""

  def render_flow_signal_action(%{record: %{} = record} = data) do
    id = flow_record_id(record)

    partition_key =
      flow_detail_url_partition_key(
        Map.get(data, :partition_key) || flow_record_partition_key(record)
      )

    action = "/dashboard/flow/" <> URI.encode(id, &URI.char_unreserved?/1) <> "/signal"
    partition_input = render_flow_rewind_partition_input(partition_key)

    """
    <div class="flow-policy-panel">
      <div class="section-title">Send Signal #{info_icon("External signal records a signal payload event and can optionally transition the flow state.")}</div>
      <form class="flow-policy-form" action="#{escape_attr(action)}" method="post">
        <input type="hidden" name="id" value="#{escape_attr(id)}">
        #{partition_input}
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Signal Name</span>
            <input class="flow-search-input mono" type="text" name="signal" required placeholder="e.g. payment_received" title="Required signal name">
          </label>
          <label class="flow-policy-field">
            <span>Transition To (optional)</span>
            <input class="flow-search-input mono" type="text" name="transition_to" placeholder="e.g. processing" title="Optional state to transition to upon receiving the signal">
          </label>
        </div>
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Idempotency Key (optional)</span>
            <input class="flow-search-input mono" type="text" name="idempotency_key" placeholder="unique token" title="Optional unique signal deduplication key">
          </label>
          <label class="flow-policy-field">
            <span>Gated If State (optional)</span>
            <input class="flow-search-input mono" type="text" name="if_state" placeholder="e.g. payment_pending" title="Optional state constraint: signal only applies if the flow is currently in this state">
          </label>
        </div>
        <div class="flow-policy-actions">
          <button class="flow-search-button" type="submit">Send Signal</button>
        </div>
      </form>
    </div>
    """
  end

  def render_flow_signal_action(_data), do: ""

  def render_flow_rewind_partition_input(partition_key)
      when is_binary(partition_key) and partition_key != "" do
    ~s(<input type="hidden" name="partition_key" value="#{escape_attr(partition_key)}">)
  end

  def render_flow_rewind_partition_input(_partition_key), do: ""

  def flow_rewind_targets(history) do
    history
    |> flow_history_timeline_rows()
    |> Enum.filter(fn row ->
      event_id = to_string(row.event_id)
      is_binary(row.to_state) and row.to_state != "" and event_id != "" and event_id != "-"
    end)
    |> Enum.uniq_by(fn row -> to_string(row.event_id) end)
  end

  def render_flow_rewind_options(targets) do
    Enum.map_join(targets, "\n", fn row ->
      event_id = to_string(row.event_id)
      state = row.to_state
      label = "#{state} / #{flow_history_event_label(row.fields)} / #{event_id}"

      ~s(<option value="#{escape_attr(event_id)}">#{escape(label)}</option>)
    end)
  end

  def render_flow_value_store(%{record: nil}),
    do: ~s(<div id="flow-value-store" hidden aria-hidden="true"></div>)

  def render_flow_value_store(data) do
    refs = Map.get(data, :value_refs, [])
    values_by_ref = Map.get(data, :values_by_ref, %{})
    status = Map.get(data, :values_status, :ok)

    rows =
      Enum.map_join(refs, "\n", fn entry ->
        anchor = flow_value_ref_anchor(entry.ref)
        preview = flow_value_store_preview(status, values_by_ref, entry.ref)
        label = escape_attr(entry.label)
        ref = escape_attr(entry.ref)

        """
        <div id="#{anchor}" class="flow-value-row" data-flow-value-ref="#{ref}" data-flow-value-label="#{label}">
          <pre class="flow-value-preview" data-flow-value-preview>#{escape(preview)}</pre>
        </div>
        """
      end)

    limit_note =
      if length(refs) >= @flow_dashboard_value_ref_limit do
        ~s( data-flow-value-limit-note="Showing first #{format_number(@flow_dashboard_value_ref_limit)} refs.")
      else
        ""
      end

    """
    <div id="flow-value-store" hidden aria-hidden="true"#{limit_note}>
      #{rows}
    </div>
    """
  end

  def flow_value_store_preview(:ok, values_by_ref, ref) do
    flow_value_preview(Map.get(values_by_ref, ref, :not_loaded))
  end

  def flow_value_store_preview(:skipped, _values_by_ref, _ref),
    do: "Value is not loaded on this page."

  def flow_value_store_preview(:timeout, _values_by_ref, _ref), do: "Value lookup timed out."

  def flow_value_store_preview({:error, reason}, _values_by_ref, _ref) do
    dashboard_internal_error("Value lookup failed", reason)
  end

  def flow_value_store_preview({:exit, reason}, _values_by_ref, _ref) do
    dashboard_internal_error("Value lookup exited", :exit, reason)
  end

  def flow_value_store_preview(_status, _values_by_ref, _ref),
    do: "Value is not loaded on this page."

  def render_flow_value_modal do
    """
    <div id="flow-value-modal" class="flow-value-modal" hidden role="dialog" aria-modal="true" aria-labelledby="flow-value-modal-title">
      <div class="flow-value-modal-backdrop" data-flow-value-modal-close></div>
      <div class="flow-value-modal-panel">
        <div class="flow-value-modal-header">
          <div>
            <div id="flow-value-modal-title" class="section-title">Value Inspector</div>
            <div id="flow-value-modal-ref" class="flow-value-modal-ref mono"></div>
          </div>
          <button class="flow-value-modal-close" type="button" data-flow-value-modal-close title="Close value inspector">Close</button>
        </div>
        <pre id="flow-value-modal-body" class="flow-value-modal-body"></pre>
        <div class="flow-value-modal-actions">
          <button id="flow-value-modal-copy" class="flow-search-button" type="button" title="Copy the displayed value">Copy</button>
          <span id="flow-value-modal-copy-status" class="c-muted"></span>
        </div>
      </div>
    </div>
    """
  end

  def render_flow_debug(%{record: nil}) do
    """
    <div class="section-title">Debug Inspector</div>
    <div class="pressure-alert level-warning">
      <div class="pressure-details">No current Flow record is available to inspect.</div>
    </div>
    """
  end

  def render_flow_debug(data) do
    record = data.record
    history = Map.get(data, :history, [])

    cards = [
      {"Execution", flow_execution_debug_summary(record), Map.get(data, :waiting_reason, "-")},
      {"Lease", flow_lease_debug_summary(record), flow_lease_debug_detail(record)},
      {"Values", flow_values_debug_summary(record),
       "payload/result/error/named value references"},
      {"History", flow_history_debug_summary(history),
       "latest events loaded for this detail view"}
    ]

    card_html =
      Enum.map_join(cards, "\n", fn {label, value, detail} ->
        """
        <div class="flow-card">
          <div class="flow-card-label">#{escape(label)}</div>
          <div class="flow-card-value" style="font-size:1rem;">#{escape(value)}</div>
          <div class="flow-card-detail">#{escape(detail)}</div>
        </div>
        """
      end)

    rows =
      flow_debug_rows(record, history)
      |> Enum.map_join("\n", fn {label, value} ->
        """
        <tr>
          <td class="c-muted">#{escape(label)}</td>
          <td class="mono">#{escape(value)}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Debug Inspector</div>
    <div class="flow-card-grid">
      #{card_html}
    </div>
    <table>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  def flow_debug_rows(record, history) do
    [
      {"Identity", flow_debug_identity(record)},
      {"Storage", flow_debug_storage(record)},
      {"Run Timing", flow_run_debug_summary(record)},
      {"Lease", flow_lease_debug_summary(record)},
      {"Retry", flow_retry_debug_summary(record)},
      {"Values", flow_values_debug_detail(record)},
      {"Last Event", flow_last_event_debug_summary(history)}
    ] ++ flow_value_ref_debug_rows(record)
  end

  def flow_debug_identity(record) do
    "type=#{flow_record_type(record)} id=#{flow_record_id(record)} state=#{flow_record_state(record)}"
  end

  def flow_debug_storage(record) do
    partition = flow_record_partition_key(record) || "auto/global"
    "partition=#{partition} projection=lagged"
  end

  def flow_execution_debug_summary(record) do
    state = flow_record_state(record)

    cond do
      state in @flow_terminal_states -> "terminal #{state}"
      state == "running" -> "running"
      flow_due_now?(record) -> "claimable now"
      true -> "waiting"
    end
  end

  def flow_run_debug_summary(record) do
    now = System.system_time(:millisecond)

    case flow_record_run_at_ms(record) do
      run_at when is_integer(run_at) and run_at > now ->
        "scheduled in #{format_duration_ms(run_at - now)} at #{format_timestamp_ms_or_dash(run_at)}"

      run_at when is_integer(run_at) and run_at > 0 ->
        "due since #{format_duration_ms(now - run_at)} at #{format_timestamp_ms_or_dash(run_at)}"

      _ ->
        "no run_at metadata"
    end
  end

  def flow_lease_debug_summary(record) do
    now = System.system_time(:millisecond)

    case {flow_record_state(record), flow_record_lease_expires_at_ms(record)} do
      {"running", expires_at} when is_integer(expires_at) and expires_at > now ->
        "running until #{format_timestamp_ms_or_dash(expires_at)}"

      {"running", expires_at} when is_integer(expires_at) and expires_at > 0 ->
        "expired #{format_duration_ms(now - expires_at)} ago"

      {"running", _} ->
        "running without lease expiry"

      {_state, _expires_at} ->
        "not leased"
    end
  end

  def flow_lease_debug_detail(record) do
    worker = flow_record_worker(record) || "-"
    token = flow_debug_value_or_dash(flow_field(record, :lease_token, nil))
    "worker=#{worker} token=#{token}"
  end

  def flow_debug_value_or_dash(nil), do: "-"
  def flow_debug_value_or_dash(""), do: "-"
  def flow_debug_value_or_dash(value) when is_binary(value), do: value
  def flow_debug_value_or_dash(value) when is_atom(value), do: Atom.to_string(value)
  def flow_debug_value_or_dash(value) when is_integer(value), do: Integer.to_string(value)
  def flow_debug_value_or_dash(value), do: inspect(value, limit: 5)

  def flow_retry_debug_summary(record) do
    attempts = flow_field(record, :attempts, flow_field(record, :attempt, 0))
    max_attempts = flow_field(record, :max_attempts, "-")
    exhausted_to = flow_field(record, :exhausted_to, "-")
    "attempts=#{attempts} max=#{max_attempts} exhausted_to=#{exhausted_to}"
  end

  def flow_values_debug_summary(record) do
    "#{length(flow_value_ref_debug_rows(record))} refs"
  end

  def flow_values_debug_detail(record) do
    flow_value_ref_debug_rows(record)
    |> Enum.map_join(", ", fn {label, _value} -> label end)
    |> case do
      "" -> "none"
      labels -> labels
    end
  end

  def flow_detail_value_refs(record, history) do
    history_refs =
      Enum.flat_map(history, fn entry ->
        {event_id, fields} = normalize_flow_history_entry(entry)
        flow_value_ref_entries(fields, "event #{event_id}")
      end)

    (flow_value_ref_entries(record, "current state") ++ history_refs)
    |> dedupe_flow_value_refs()
  end

  def dedupe_flow_value_refs(entries) do
    entries
    |> Enum.reduce({MapSet.new(), []}, fn entry, {seen, acc} ->
      if MapSet.member?(seen, entry.ref) do
        {seen, acc}
      else
        {MapSet.put(seen, entry.ref), [entry | acc]}
      end
    end)
    |> elem(1)
    |> Enum.reverse()
  end

  def flow_value_ref_debug_rows(record) do
    base_refs =
      [
        {"payload_ref", flow_field(record, :payload_ref, nil)},
        {"result_ref", flow_field(record, :result_ref, nil)},
        {"error_ref", flow_field(record, :error_ref, nil)}
      ]
      |> Enum.filter(fn {_label, ref} -> is_binary(ref) and ref != "" end)

    named_refs =
      record
      |> flow_named_value_refs()
      |> Enum.map(fn {name, ref} -> {"value:#{to_string(name)}", ref} end)
      |> Enum.sort_by(fn {name, _ref} -> name end)

    base_refs ++ named_refs
  end

  def flow_value_ref_entries(record, source) do
    base_refs =
      [
        {"payload", flow_field(record, :payload_ref, nil)},
        {"result", flow_field(record, :result_ref, nil)},
        {"error", flow_field(record, :error_ref, nil)}
      ]
      |> Enum.flat_map(fn {label, ref} ->
        case ref do
          ref when is_binary(ref) and ref != "" ->
            [%{label: label, ref: ref, source: source}]

          _ ->
            []
        end
      end)

    named_refs =
      record
      |> flow_named_value_refs()
      |> Enum.map(fn {name, ref} -> %{label: to_string(name), ref: ref, source: source} end)
      |> Enum.sort_by(& &1.label)

    base_refs ++ named_refs
  end

  def flow_named_value_refs(record) do
    record
    |> flow_field(:value_refs, flow_field(record, :values_refs, %{}))
    |> normalize_flow_named_value_refs()
  end

  def normalize_flow_named_value_refs(refs) when is_map(refs) do
    Enum.flat_map(refs, fn {name, ref} ->
      case normalize_flow_value_ref(ref) do
        ref when is_binary(ref) and ref != "" -> [{name, ref}]
        _ -> []
      end
    end)
  end

  def normalize_flow_named_value_refs(refs) when is_binary(refs) do
    case Jason.decode(refs) do
      {:ok, decoded} -> normalize_flow_named_value_refs(decoded)
      _ -> []
    end
  end

  def normalize_flow_named_value_refs(_refs), do: []

  def normalize_flow_value_ref(ref) when is_binary(ref), do: ref

  def normalize_flow_value_ref(ref) when is_map(ref) do
    flow_field(ref, :ref, nil)
  end

  def normalize_flow_value_ref(_ref), do: nil

  def flow_history_debug_summary(history), do: "#{length(history)} events"

  def flow_last_event_debug_summary([]), do: "none"

  def flow_last_event_debug_summary(history) do
    {event_id, fields} =
      history
      |> List.last()
      |> normalize_flow_history_entry()

    "#{event_id}: #{flow_history_event_label(fields)} #{flow_history_state_move(fields)}"
  end
end
