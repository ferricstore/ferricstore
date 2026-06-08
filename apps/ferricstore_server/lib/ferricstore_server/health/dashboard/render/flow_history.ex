defmodule FerricstoreServer.Health.Dashboard.Render.FlowHistory do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.FlowRecord

  @flow_dashboard_history_default_count 50
  @flow_dashboard_value_preview_bytes 8 * 1024
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

  def render_flow_history_timeline(history, status, page) do
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

        history == [] ->
          ~s(<tr><td colspan="8" class="c-muted">No history events found yet</td></tr>)

        true ->
          history
          |> flow_history_timeline_rows()
          |> Enum.map_join("\n", fn row ->
            fields = row.fields
            anchor = flow_history_event_anchor(row.event_id)

            """
            <tr id="#{anchor}" class="timeline-event-row">
              <td class="mono"><a class="flow-event-link" href="##{anchor}">#{escape(to_string(row.event_id))}</a></td>
              <td>#{format_timestamp_ms_or_dash(row.time_ms)}</td>
              <td>#{flow_history_action_html(fields)}</td>
              <td>#{escape(flow_history_state_move(row))}</td>
              <td>#{escape(flow_history_version_summary(fields))}</td>
              <td>#{escape(flow_history_attempt_summary(fields))}</td>
              <td class="mono">#{escape(flow_history_worker_summary(fields))}</td>
              <td class="mono">#{flow_history_refs_summary_html(fields)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Timeline</div>
    #{render_flow_history_pagination(page)}
    <table>
      <thead>
        <tr><th>Event</th><th>Time</th><th>Action</th><th>State Change</th><th>Version</th><th>Attempts</th><th>Worker</th><th>Values</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

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

  def render_flow_value_ref_badge(record, mode, %{label: label, ref: ref}) do
    anchor = flow_value_ref_anchor(ref)
    href = flow_value_ref_href(record, mode, anchor)
    title = "Open #{label} value"

    ~s(<a class="flow-pill flow-value-ref-link" href="#{escape_attr(href)}" title="#{escape_attr(title)}" aria-label="#{escape_attr(title)}" data-flow-value-ref="#{escape_attr(ref)}" data-flow-value-label="#{escape_attr(label)}">#{escape(label)}</a>)
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

  def flow_value_preview(value) when is_binary(value) do
    if String.valid?(value) do
      flow_truncate_preview(value)
    else
      value
      |> inspect(limit: :infinity, printable_limit: @flow_dashboard_value_preview_bytes)
      |> flow_truncate_preview()
    end
  end

  def flow_value_preview(value) do
    value
    |> inspect(pretty: true, limit: 50, printable_limit: @flow_dashboard_value_preview_bytes)
    |> flow_truncate_preview()
  end

  def flow_truncate_preview(value) when is_binary(value) do
    if String.length(value) > @flow_dashboard_value_preview_bytes do
      String.slice(value, 0, @flow_dashboard_value_preview_bytes) <> "\n... truncated ..."
    else
      value
    end
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

  def flow_history_refs_summary_html(fields) do
    badges =
      fields
      |> flow_value_ref_entries("history event")
      |> Enum.map(&render_flow_value_ref_badge(nil, :local, &1))

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
  def flow_state_class("running"), do: "c-green"
  def flow_state_class(_state), do: ""

  def flow_state_badge_class("failed"), do: "badge-pressure"
  def flow_state_badge_class("cancelled"), do: "badge-warning"
  def flow_state_badge_class(state) when state in @flow_terminal_states, do: "badge-ok"
  def flow_state_badge_class("running"), do: "badge-merging"
  def flow_state_badge_class(_state), do: "badge-idle"
end
