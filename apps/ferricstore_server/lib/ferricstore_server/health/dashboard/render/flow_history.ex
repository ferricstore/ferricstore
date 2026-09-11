defmodule FerricstoreServer.Health.Dashboard.Render.FlowHistory do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.FlowRecord

  @flow_dashboard_history_default_count 50
  @inline_detail_page_bytes 64 * 1024
  @inline_detail_field_bytes 8 * 1024
  @flow_terminal_states ~w(completed failed cancelled)

  def flow_signal_rows(record, history) when is_map(record) and is_list(history) do
    history
    |> flow_history_timeline_rows()
    |> Enum.filter(&flow_signal_event?(&1.fields))
    |> Enum.map(fn row ->
      %{
        id: flow_record_id(record),
        partition_key: flow_detail_url_partition_key(flow_record_partition_key(record)),
        type: flow_record_type(record),
        event_id: to_string(row.event_id),
        time_ms: row.time_ms,
        signal: flow_field_string(row.fields, :signal, "-"),
        from_state: row.from_state,
        to_state: row.to_state,
        fields: row.fields,
        record: record
      }
    end)
  end

  def flow_signal_rows(_record, _history), do: []

  defp flow_signal_event?(fields) do
    fields
    |> flow_field_string(:event, flow_field_string(fields, :action, ""))
    |> String.downcase() == "signaled"
  end

  defp journal_signal_name(fields) do
    name = flow_field_string(fields, :signal, "")

    if flow_signal_event?(fields) and name != "" do
      preview = bounded_inline_detail(name, 256)
      suffix = if preview.truncated, do: "... (truncated)", else: ""
      ~s(<span class="journal-signal-name mono">#{escape(preview.value)}#{suffix}</span>)
    else
      ""
    end
  end

  def render_flow_history_timeline(_history, :forbidden, _page) do
    ~s(<div class="flow-section-note" role="status">History restricted. Requires FLOW.HISTORY access for this workflow.</div>)
  end

  def render_flow_history_timeline(history, status, page) do
    timeline_rows = history |> flow_history_timeline_rows() |> with_inline_detail_budget()
    event_count = length(timeline_rows)
    event_label = if event_count == 1, do: "event", else: "events"

    journal_html =
      cond do
        status == :timeout ->
          ~s(<div class="flow-lineage-empty">History temporarily unavailable: FLOW.HISTORY timed out.</div>)

        match?({:error, _}, status) ->
          {_tag, reason} = status

          ~s(<div class="flow-lineage-empty">History temporarily unavailable: #{escape(dashboard_internal_error("FLOW.HISTORY failed", reason))}</div>)

        match?({:exit, _}, status) ->
          {_tag, reason} = status

          ~s(<div class="flow-lineage-empty">History temporarily unavailable: #{escape(dashboard_internal_error("FLOW.HISTORY exited", :exit, reason))}</div>)

        timeline_rows == [] ->
          ~s(<div class="flow-lineage-empty">No history events found yet</div>)

        true ->
          render_flow_journal_steps(timeline_rows)
      end

    rows =
      cond do
        status == :timeout ->
          ~s(<tr><td colspan="8" class="c-muted">History temporarily unavailable: FLOW.HISTORY timed out.</td></tr>)

        match?({:error, _}, status) ->
          {_tag, reason} = status

          ~s(<tr><td colspan="8" class="c-muted">History temporarily unavailable: #{escape(dashboard_internal_error("FLOW.HISTORY failed", reason))}</td></tr>)

        match?({:exit, _}, status) ->
          {_tag, reason} = status

          ~s(<tr><td colspan="8" class="c-muted">History temporarily unavailable: #{escape(dashboard_internal_error("FLOW.HISTORY exited", :exit, reason))}</td></tr>)

        timeline_rows == [] ->
          ~s(<tr><td colspan="8" class="c-muted">No history events found yet</td></tr>)

        true ->
          Enum.map_join(timeline_rows, "\n", fn row ->
            fields = row.fields
            anchor = flow_history_event_anchor(row.event_id)

            """
            <tr id="#{anchor}" class="timeline-event-row">
              <td class="mono"><a class="flow-event-link" href="##{anchor}">#{escape(to_string(row.event_id))}</a></td>
              <td>#{format_timestamp_ms_or_dash(row.time_ms)}</td>
              <td>#{flow_history_action_html(fields)}#{render_flow_raw_event_details(row)}</td>
              <td>#{escape(flow_history_state_move(row))}</td>
              <td>#{escape(flow_history_version_summary(fields))}</td>
              <td>#{escape(flow_history_attempt_summary(fields))}</td>
              <td class="mono">#{escape(flow_history_worker_summary(fields))}</td>
              <td class="mono">#{flow_history_refs_summary_html(fields, row)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="flow-journal-card" data-flow-history-detail-byte-budget="#{@inline_detail_page_bytes}">
      <div class="flow-card-header">
        <div class="flow-card-header-title">
          <span>Execution Journal</span>
          <span class="badge badge-idle">#{format_number(event_count)} #{event_label} on this page</span>
        </div>
        <div class="view-toggle" role="tablist" aria-label="Journal View Mode">
          <button type="button" role="tab" class="active" id="journal-tab-tree" aria-controls="journal-panel-tree" data-journal-view-toggle="tree" aria-selected="true" tabindex="0">Journal</button>
          <button type="button" role="tab" id="journal-tab-table" aria-controls="journal-panel-table" data-journal-view-toggle="table" aria-selected="false" tabindex="-1">Raw Events</button>
        </div>
      </div>
      #{render_flow_history_controls(page, status)}
      #{if Enum.any?(timeline_rows, &event_has_inline_error?/1), do: ~s(<p class="flow-section-note">Expanded inline diagnostics: 64 KiB page budget; up to 8 KiB per field.</p>), else: ""}
      <div id="journal-panel-tree" role="tabpanel" aria-labelledby="journal-tab-tree" data-journal-view="tree">
        #{journal_html}
      </div>
      <div id="journal-panel-table" role="tabpanel" aria-labelledby="journal-tab-table" data-journal-view="table" hidden>
        <h2 class="section-title sr-only">Timeline</h2>
        <div class="table-scroll" role="region" aria-label="Workflow history events" tabindex="0"><table>
          <thead>
            <tr><th>Event</th><th>Time</th><th>Action</th><th>State Change</th><th>Version</th><th>Attempts</th><th>Worker</th><th>Values</th></tr>
          </thead>
          <tbody>
            #{rows}
          </tbody>
        </table></div>
      </div>
    </div>
    """
  end

  def render_flow_journal_steps([]) do
    ~s(<div class="flow-lineage-empty">No history events found yet</div>)
  end

  def render_flow_journal_steps(rows) do
    {step_items, inspectors} =
      rows
      |> with_inline_detail_budget()
      |> Enum.map(fn row ->
        fields = row.fields
        anchor = flow_history_event_anchor(row.event_id)
        action_label = flow_history_event_label(fields)
        state_move = flow_history_state_move(row)
        node_class = flow_journal_node_class(row)
        worker = flow_history_worker_summary(fields)

        worker_badge =
          if worker != "-",
            do: ~s(<span class="flow-pill mono">worker: #{escape(worker)}</span>),
            else: ""

        attempts = flow_history_attempt_summary(fields)

        attempts_badge =
          if attempts != "-", do: ~s(<span class="flow-pill">#{escape(attempts)}</span>), else: ""

        values_html = flow_history_refs_summary_html(fields, row)

        values_section =
          if values_html != "-",
            do:
              ~s(<div class="journal-step-values" style="margin-top: 6px;">#{values_html}</div>),
            else: ""

        inspector_id = "journal-inspector-#{anchor}"
        inspector = render_flow_journal_event_inspector(row, inspector_id)

        step = """
        <div class="journal-step" id="journal-#{anchor}" data-flow-event-id="#{escape_attr(to_string(row.event_id))}">
          <div class="journal-step-node #{node_class}"></div>
          <div class="journal-step-trigger" tabindex="0" role="button" aria-expanded="false" aria-controls="#{escape_attr(inspector_id)}">
            <div class="journal-step-body">
              <div class="journal-step-top">
                <span class="journal-step-title">#{escape(action_label)} <span class="c-muted" style="font-size:0.75rem; font-weight:normal;">##{escape(to_string(row.event_id))}</span></span>
                <span class="journal-step-duration">#{format_timestamp_ms_or_dash(row.time_ms)}</span>
              </div>
              <div class="journal-step-meta">
                <span class="badge #{flow_state_badge_class(row.to_state)}">#{escape(state_move)}</span>
                #{journal_signal_name(row.fields)}
                #{worker_badge}
                #{attempts_badge}
              </div>
            </div>
          </div>
          #{values_section}
        </div>
        """

        {step, inspector}
      end)
      |> Enum.unzip()

    """
    <div class="flow-journal-workspace">
      <div class="flow-journal-tree">
        #{Enum.join(step_items, "\n")}
      </div>
      <aside class="flow-journal-inspector" hidden aria-label="Selected event">
        #{Enum.join(inspectors, "\n")}
      </aside>
    </div>
    """
  end

  defp render_flow_journal_event_inspector(row, inspector_id) do
    fields = row.fields

    details =
      [
        {"Event", to_string(row.event_id)},
        {"Occurred", format_timestamp_ms_or_dash(row.time_ms)},
        {"Action", flow_history_event_label(fields)},
        {"State change", flow_history_state_move(row)},
        {"Worker", flow_history_worker_summary(fields)},
        {"Attempts", flow_history_attempt_summary(fields)}
      ] ++ flow_event_specific_details(fields)

    rows =
      details
      |> Enum.reject(fn {_label, value} -> value in [nil, "", "-"] end)
      |> Enum.map_join("\n", fn {label, value} ->
        """
        <div class="journal-event-inspector-item">
          <dt>#{escape(label)}</dt>
          <dd class="mono">#{escape(value)}</dd>
        </div>
        """
      end)

    """
    <div class="journal-event-inspector" hidden id="#{escape_attr(inspector_id)}" role="region" aria-label="Event details for #{escape_attr(to_string(row.event_id))}">
      <div class="journal-event-inspector-title">Event details</div>
      <dl class="journal-event-inspector-grid">
        #{rows}
      </dl>
      #{render_flow_event_errors(row)}
    </div>
    """
  end

  defp render_flow_raw_event_details(row) do
    details =
      row.fields
      |> flow_event_specific_details()
      |> Enum.reject(fn {_label, value} -> value in [nil, "", "-"] end)
      |> Enum.map_join("", fn {label, value} ->
        ~s(<div><dt>#{escape(label)}</dt><dd class="mono">#{escape(value)}</dd></div>)
      end)

    """
    <details class="flow-raw-event-details">
      <summary>Event fields</summary>
      <dl>#{details}</dl>
      #{if event_has_inline_error?(row), do: ~s(<a class="flow-event-link" href="#journal-#{flow_history_event_anchor(row.event_id)}">Inspect error and reason</a>), else: ""}
    </details>
    """
  end

  defp flow_event_specific_details(fields) do
    [
      {"Signal name", :signal},
      {"Idempotency key", :idempotency_key},
      {"Rewind target event", :to_event},
      {"Retry decision", :retry_decision},
      {"Retry reason", :retry_reason},
      {"Fencing token", :fencing_token}
    ]
    |> Enum.map(fn {label, key} ->
      value = bounded_flow_history_detail(flow_field(fields, key, nil))

      value =
        if key == :idempotency_key and value == "-" and flow_signal_event?(fields),
          do: "Not recorded in event",
          else: value

      {label, value}
    end)
  end

  defp with_inline_detail_budget(rows) do
    fields =
      Enum.reduce(rows, 0, fn row, count ->
        count + Enum.count([:error, :reason], &(flow_field(row.fields, &1, nil) not in [nil, ""]))
      end)

    limit = min(@inline_detail_field_bytes, div(@inline_detail_page_bytes, max(fields, 1)))
    Enum.map(rows, &Map.put(&1, :inline_detail_limit, limit))
  end

  defp event_has_inline_error?(row),
    do: Enum.any?([:error, :reason], &(flow_field(row.fields, &1, nil) not in [nil, ""]))

  defp render_flow_event_errors(row) do
    fields = row.fields
    limit = Map.get(row, :inline_detail_limit, @inline_detail_field_bytes)

    [{"Error", :error}, {"Reason", :reason}]
    |> Enum.map_join("", fn {label, key} ->
      case flow_field(fields, key, nil) do
        value when value in [nil, ""] ->
          ""

        value ->
          full = bounded_inline_detail(value, limit)
          preview = bounded_flow_history_detail(full.value)

          note =
            cond do
              full.truncated and limit < @inline_detail_field_bytes ->
                "#{label} preview limited by 64 KiB page budget (#{limit} bytes for this field)"

              full.truncated ->
                "#{label} preview limited to 8 KiB"

              is_binary(value) ->
                "Complete #{String.downcase(label)}"

              true ->
                "#{label} detail (bounded inspection)"
            end

          """
          <div class="journal-event-inspector-item"><strong>#{label}</strong><p class="mono">#{escape(preview)}</p></div>
          <details class="flow-history-full-detail">
            <summary>#{note}</summary>
            <pre>#{escape(full.value)}</pre>
          </details>
          """
      end
    end)
  end

  defp bounded_inline_detail(value, limit) do
    preview = FerricstoreServer.Health.Dashboard.ValuePreview.render(value)

    if byte_size(preview.value) > limit do
      prefix = binary_part(preview.value, 0, limit)

      valid =
        case :unicode.characters_to_binary(prefix, :utf8, :utf8) do
          value when is_binary(value) -> value
          {:incomplete, value, _} -> value
        end

      %{value: valid, truncated: true}
    else
      preview
    end
  end

  defp bounded_flow_history_detail(nil), do: "-"
  defp bounded_flow_history_detail(""), do: "-"

  defp bounded_flow_history_detail(value) when is_binary(value) do
    if String.length(value) > 512, do: String.slice(value, 0, 512) <> "...", else: value
  end

  defp bounded_flow_history_detail(value) when is_atom(value) or is_number(value),
    do: to_string(value)

  defp bounded_flow_history_detail(value),
    do: inspect(value, limit: 10, printable_limit: 512)

  defp flow_journal_node_class(row) do
    fields = row.fields
    label = flow_history_event_label(fields)

    cond do
      label in ["Failed", "Fail"] or row.to_state == "failed" -> "node-error"
      label in ["Retry", "Retried"] -> "node-warn"
      flow_signal_event?(fields) -> "node-warn"
      flow_history_terminal_event?(fields) -> "node-ok"
      true -> "node-active"
    end
  end

  defp render_flow_history_controls(%{has_older: false, has_newer: false} = page, :ok) do
    scoped? =
      Enum.any?(
        [:older_url, :newer_url, :before, :after_cursor],
        &(Map.get(page, &1) not in [nil, ""])
      )

    if scoped? or
         Map.get(page, :count, @flow_dashboard_history_default_count) !=
           @flow_dashboard_history_default_count,
       do: render_flow_history_pagination(page),
       else: ""
  end

  defp render_flow_history_controls(page, _status), do: render_flow_history_pagination(page)

  def render_flow_history_pagination(nil), do: ""

  def render_flow_history_pagination(page) when is_map(page) do
    newer = render_flow_history_page_link("Newer", Map.get(page, :newer_url))
    older = render_flow_history_page_link("Older", Map.get(page, :older_url))
    count = Map.get(page, :count, @flow_dashboard_history_default_count)

    count_links =
      [50, 100, 250]
      |> Enum.map_join(" ", fn option ->
        class =
          if option == count do
            "flow-history-count flow-history-count-active"
          else
            "flow-history-count"
          end

        ~s(<a class="#{class}" href="#{flow_detail_history_count_url(page, option)}">#{option}</a>)
      end)

    """
    <div class="flow-history-controls">
      <div class="flow-history-pages">
        #{newer}
        #{older}
      </div>
      <div class="flow-history-counts">
        <span class="c-muted">History page</span>
        #{count_links}
      </div>
    </div>
    """
  end

  def render_flow_history_page_link(label, url) when is_binary(url) and url != "" do
    ~s(<a class="flow-history-page-link" href="#{escape(url)}">#{label}</a>)
  end

  def render_flow_history_page_link(label, _url) do
    ~s(<span class="flow-history-page-link flow-history-page-disabled">#{label}</span>)
  end

  def flow_detail_history_count_url(%{id: id, partition_key: partition_key}, count),
    do: flow_detail_path(id, partition_key, %{"history_count" => count})

  def flow_detail_history_count_url(_page, count), do: "?history_count=#{count}"

  def render_flow_id_link(id, partition_key) do
    href = flow_detail_path(id, flow_detail_url_partition_key(partition_key))
    ~s(<a class="flow-link" href="#{href}">#{escape(id)}</a>)
  end

  def flow_detail_path(id, partition_key), do: flow_detail_path(id, partition_key, %{})

  def flow_detail_path(id, partition_key, params) when is_map(params) do
    path = "/dashboard/flow/" <> URI.encode(id, &URI.char_unreserved?/1)
    params = flow_detail_query_params(partition_key, params)

    if map_size(params) == 0, do: path, else: path <> "?" <> URI.encode_query(params)
  end

  def flow_detail_live_url(id, partition_key, history_page) do
    path = "/dashboard/api/flow/" <> URI.encode(id, &URI.char_unreserved?/1)

    history_params =
      if is_map(history_page), do: Map.get(history_page, :current_live_params, %{}), else: %{}

    params = flow_detail_query_params(partition_key, history_params)

    if map_size(params) == 0, do: path, else: path <> "?" <> URI.encode_query(params)
  end

  def flow_detail_query_params(partition_key, params) do
    params =
      params
      |> Enum.reduce(%{}, fn
        {_key, nil}, acc ->
          acc

        {key, value}, acc when is_atom(key) ->
          Map.put(acc, Atom.to_string(key), value)

        {key, value}, acc ->
          Map.put(acc, to_string(key), value)
      end)

    case partition_key do
      key when is_binary(key) and key != "" -> Map.put(params, "partition_key", key)
      _ -> params
    end
  end

  def render_flow_value_ref_badges(record, mode \\ :local) do
    badges =
      record
      |> flow_value_ref_entries("current state")
      |> Enum.map(&render_flow_value_ref_badge(record, mode, &1))

    case badges do
      [] -> ~s(<span class="c-muted">none</span>)
      _ -> Enum.join(badges, " ")
    end
  end

  def render_flow_value_ref_badge(record, mode, %{label: label, ref: ref} = entry) do
    source =
      if Map.get(entry, :source) in ["historical", "history event"],
        do: "historical",
        else: "current"

    event_id = Map.get(entry, :event_id)

    suffix =
      if source == "historical" and event_id not in [nil, ""],
        do: ":event:" <> Base.url_encode64(to_string(event_id), padding: false),
        else: ""

    anchor = flow_value_ref_anchor(ref) <> suffix
    href = flow_value_ref_href(record, mode, anchor)
    title = "Open #{label} value"

    provenance =
      [
        {"source", source},
        {"workflow", if(is_map(record), do: flow_record_id(record), else: nil)},
        {"partition",
         if(is_map(record), do: flow_record_partition_key(record) || "auto/global", else: nil)},
        {"event", Map.get(entry, :event_id)},
        {"action", Map.get(entry, :action)},
        {"time", Map.get(entry, :time)}
      ]
      |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
      |> Enum.map_join("", fn {key, value} ->
        ~s( data-flow-value-#{key}="#{escape_attr(to_string(value))}")
      end)

    ~s(<a class="flow-pill flow-value-ref-link" href="#{escape_attr(href)}" title="#{escape_attr(title)}" aria-label="#{escape_attr(title)}" data-flow-value-ref="#{escape_attr(ref)}" data-flow-value-label="#{escape_attr(label)}"#{provenance}>#{escape(label)}</a>)
  end

  def flow_value_ref_href(record, :detail_link, anchor) when is_map(record) do
    id = flow_record_id(record)
    partition_key = flow_detail_url_partition_key(flow_record_partition_key(record))
    flow_detail_path(id, partition_key) <> "##{anchor}"
  end

  def flow_value_ref_href(_record, _mode, anchor), do: "##{anchor}"

  def normalize_flow_history_entry({event_id, fields}) when is_map(fields),
    do: {event_id, fields}

  def normalize_flow_history_entry({event_id, fields}) when is_list(fields),
    do: {event_id, Map.new(fields)}

  def normalize_flow_history_entry(entry), do: {"-", %{raw: inspect(entry, limit: 5)}}

  def flow_history_timeline_rows(history) do
    history
    |> Enum.map(&normalize_flow_history_entry/1)
    |> Enum.sort_by(fn {event_id, fields} ->
      {flow_history_event_time_ms(event_id, fields), to_string(event_id)}
    end)
    |> Enum.map_reduce(nil, fn {event_id, fields}, previous_state ->
      current_state = flow_history_current_state(fields)
      from_state = flow_history_previous_state(fields, previous_state)

      row = %{
        event_id: event_id,
        fields: fields,
        time_ms: flow_history_event_time_ms(event_id, fields),
        from_state: from_state,
        to_state: current_state
      }

      next_state =
        case current_state do
          "" -> previous_state
          state -> state
        end

      {row, next_state}
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  def flow_history_event_anchor(event_id) do
    encoded =
      event_id
      |> to_string()
      |> Base.url_encode64(padding: false)

    "flow-event-" <> encoded
  end

  def flow_value_ref_anchor(ref) do
    encoded = Base.url_encode64(ref, padding: false)
    "flow-value-" <> encoded
  end

  def flow_value_preview(:not_loaded), do: "not loaded"
  def flow_value_preview(nil), do: "missing"

  def flow_value_preview(value) do
    FerricstoreServer.Health.Dashboard.ValuePreview.render(value).value
  end

  def flow_truncate_preview(value) when is_binary(value) do
    flow_value_preview(value)
  end

  def flow_history_event_time_ms(event_id, fields) do
    flow_first_integer(fields, [:at, :updated_at_ms, :created_at_ms, :run_at_ms]) ||
      flow_history_event_id_time_ms(event_id)
  end

  def flow_history_event_id_time_ms(event_id) do
    event_id
    |> to_string()
    |> String.split("-", parts: 2)
    |> List.first()
    |> case do
      part when is_binary(part) ->
        case Integer.parse(part) do
          {parsed, _rest} -> parsed
          _ -> nil
        end

      _ ->
        nil
    end
  end

  def flow_history_current_state(fields) do
    flow_field_string(fields, :to_state, flow_field_string(fields, :state, ""))
  end

  def flow_history_previous_state(fields, previous_state) do
    flow_field_string(fields, :from_state, previous_state || "")
  end

  def flow_history_event_label(fields) do
    raw = flow_field_string(fields, :event, flow_field_string(fields, :action, "event"))

    case String.downcase(raw) do
      "create" -> "Created"
      "created" -> "Created"
      "transition" -> "Transitioned"
      "transitioned" -> "Transitioned"
      "retry" -> "Retry"
      "retried" -> "Retry"
      "complete" -> "Completed"
      "completed" -> "Completed"
      "fail" -> "Failed"
      "failed" -> "Failed"
      "cancel" -> "Cancelled"
      "canceled" -> "Cancelled"
      "cancelled" -> "Cancelled"
      "claim" -> "Claimed"
      "claimed" -> "Claimed"
      other -> other |> String.replace("_", " ") |> String.capitalize()
    end
  end

  def flow_history_action_html(fields) do
    label = flow_history_event_label(fields)

    terminal_badge =
      if flow_history_terminal_event?(fields) do
        ~s( <span class="flow-pill">terminal</span>)
      else
        ""
      end

    escape(label) <> terminal_badge
  end

  def flow_history_terminal_event?(fields) do
    event =
      fields
      |> flow_field_string(:event, flow_field_string(fields, :action, ""))
      |> String.downcase()

    state =
      fields
      |> flow_history_current_state()
      |> String.downcase()

    event in ["completed", "complete", "failed", "fail", "cancelled", "canceled", "cancel"] or
      state in @flow_terminal_states
  end

  def flow_history_state_move(%{from_state: from_state, to_state: to_state}) do
    cond do
      is_binary(from_state) and from_state != "" and is_binary(to_state) and to_state != "" and
          from_state != to_state ->
        from_state <> " -> " <> to_state

      is_binary(to_state) and to_state != "" ->
        to_state

      true ->
        "-"
    end
  end

  def flow_history_state_move(fields) do
    from_state = flow_field_string(fields, :from_state, "")
    to_state = flow_field_string(fields, :to_state, flow_field_string(fields, :state, ""))

    cond do
      from_state != "" and to_state != "" -> from_state <> " -> " <> to_state
      to_state != "" -> to_state
      true -> "-"
    end
  end

  def flow_history_version_summary(fields) do
    ["version", "fencing_token"]
    |> flow_history_key_value_summary(fields)
  end

  def flow_history_attempt_summary(fields) do
    ["attempts", "max_attempts"]
    |> flow_history_key_value_summary(fields)
  end

  def flow_history_worker_summary(fields) do
    flow_first_non_empty_binary(fields, [:worker, :lease_owner]) || "-"
  end

  def flow_history_refs_summary_html(fields, row \\ nil) do
    provenance =
      if is_map(row),
        do: %{
          source: "historical",
          event_id: to_string(row.event_id),
          action: flow_history_event_label(fields),
          time: format_timestamp_ms_or_dash(row.time_ms)
        },
        else: %{source: "historical"}

    badges =
      fields
      |> flow_value_ref_entries("history event")
      |> Enum.map(&render_flow_value_ref_badge(nil, :local, Map.merge(&1, provenance)))

    case badges do
      [] -> "-"
      _ -> Enum.join(badges, " ")
    end
  end

  def flow_history_key_value_summary(keys, fields) do
    keys
    |> Enum.flat_map(fn key ->
      atom_key = String.to_existing_atom(key)

      case flow_field(fields, atom_key, nil) do
        nil -> []
        "" -> []
        value -> ["#{key}=#{value}"]
      end
    end)
    |> case do
      [] -> "-"
      parts -> Enum.join(parts, ", ")
    end
  end

  def flow_state_class("failed"), do: "c-red"
  def flow_state_class("cancelled"), do: "c-yellow"
  def flow_state_class(_state), do: ""

  def flow_state_badge_class("failed"), do: "badge-pressure"
  def flow_state_badge_class("cancelled"), do: "badge-warning"
  def flow_state_badge_class(state) when state in @flow_terminal_states, do: "badge-ok"
  def flow_state_badge_class(_state), do: "badge-idle"
end
