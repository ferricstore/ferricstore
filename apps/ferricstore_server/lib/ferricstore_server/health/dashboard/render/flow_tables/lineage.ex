defmodule FerricstoreServer.Health.Dashboard.Render.FlowTables.Lineage do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory

  def render_flow_lineage_hints([]), do: ""

  def render_flow_lineage_hints(hints) do
    links =
      Enum.map_join(hints, " ", fn hint ->
        params = %{"mode" => hint.mode, "id" => hint.id}
        href = "/dashboard/flow/lineage?" <> URI.encode_query(params)

        ~s(<a class="flow-pill flow-link" href="#{href}">#{escape(hint.label)} #{escape(hint.id)}</a>)
      end)

    ~s(<div class="flow-section-note">Recent lineage hints: #{links}</div>)
  end

  def flow_lineage_result_label(%{status: :idle, message: message}), do: message
  def flow_lineage_result_label(%{status: :ok, command: command}), do: "#{command} result"
  def flow_lineage_result_label(%{status: status, message: message}), do: "#{status}: #{message}"
  def flow_lineage_result_label(_result), do: "lineage result"

  def render_flow_lineage_nodes([], filters) do
    target = Map.get(filters, :target)

    if is_binary(target) and target != "" do
      ~s(<div class="flow-lineage-empty">No lineage records matched this query.</div>)
    else
      ~s(<div class="flow-lineage-empty">Choose parent, root, or correlation and enter an id.</div>)
    end
  end

  def render_flow_lineage_nodes(records, _filters) do
    records
    |> Enum.take(40)
    |> Enum.map_join("\n", fn record ->
      state = flow_record_state(record)

      """
      <a class="flow-lineage-node #{flow_state_class(state)}" href="#{flow_detail_path(flow_record_id(record), flow_record_partition_key(record))}">
        <span class="flow-lineage-node-id">#{escape(flow_record_id(record))}</span>
        <span class="flow-lineage-node-meta">#{escape(flow_record_type(record))} / #{escape(state)}</span>
      </a>
      """
    end)
  end

  def render_flow_lineage_rows([]) do
    ~s(<tr><td colspan="8" class="c-muted">No lineage records loaded.</td></tr>)
  end

  def render_flow_lineage_rows(records) do
    Enum.map_join(records, "\n", fn record ->
      state = flow_record_state(record)

      """
      <tr>
        <td class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</td>
        <td class="mono">#{escape(flow_record_type(record))}</td>
        <td class="#{flow_state_class(state)}">#{escape(state)}</td>
        <td class="mono">#{escape(flow_record_parent_id(record) || "-")}</td>
        <td class="mono">#{escape(flow_record_root_id(record) || "-")}</td>
        <td class="mono">#{escape(flow_record_correlation_id(record) || "-")}</td>
        <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
        <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
      </tr>
      """
    end)
  end

  def flow_query_result_command(%{command: command}) when is_binary(command), do: command
  def flow_query_result_command(_result), do: "FLOW.QUERY"

  def render_flow_query_status(%{status: :ok, message: message}),
    do: ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)

  def render_flow_query_status(%{status: :idle, message: message}),
    do: ~s(<div class="flow-section-note">#{escape(message)}</div>)

  def render_flow_query_status(%{status: _status, message: message}),
    do: ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)

  def render_flow_query_status(_result), do: ""

  def render_flow_query_rows(%{rows: []}) do
    ~s(<tr><td colspan="6" class="c-muted">No rows.</td></tr>)
  end

  def render_flow_query_rows(%{command: "FLOW.HISTORY", rows: rows}) do
    Enum.map_join(rows, "\n", fn entry ->
      {event_id, fields} = normalize_flow_history_entry(entry)

      """
      <tr>
        <td class="mono">#{escape(to_string(event_id))}</td>
        <td class="mono">history</td>
        <td>#{flow_history_action_html(fields)}</td>
        <td>#{format_timestamp_ms_or_dash(flow_history_event_time_ms(event_id, fields))}</td>
        <td class="mono">#{escape(flow_history_worker_summary(fields))}</td>
        <td>#{flow_history_refs_summary_html(fields)}</td>
      </tr>
      """
    end)
  end

  def render_flow_query_rows(%{command: "FLOW.STATS", rows: rows}) do
    rows
    |> List.wrap()
    |> Enum.flat_map(fn
      row when is_map(row) -> Map.to_list(row)
      other -> [{"value", other}]
    end)
    |> Enum.map_join("\n", fn {key, value} ->
      """
      <tr>
        <td class="mono">#{escape(to_string(key))}</td>
        <td colspan="5" class="mono">#{escape(inspect(value, limit: 20))}</td>
      </tr>
      """
    end)
  end

  def render_flow_query_rows(%{rows: rows}) do
    Enum.map_join(rows, "\n", fn
      record when is_map(record) ->
        state = flow_record_state(record)

        """
        <tr>
          <td class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</td>
          <td class="mono">#{escape(flow_record_type(record))}</td>
          <td class="#{flow_state_class(state)}">#{escape(state)}</td>
          <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
          <td class="mono">#{escape(flow_record_worker(record) || "-")}</td>
          <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
        </tr>
        """

      other ->
        """
        <tr>
          <td class="mono">#{escape(inspect(other, limit: 5))}</td>
          <td colspan="5" class="c-muted">non-record result</td>
        </tr>
        """
    end)
  end
end
