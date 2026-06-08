defmodule FerricstoreServer.Health.Dashboard.Render.FlowTables.Signals do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory
  import FerricstoreServer.Health.Dashboard.Render.FlowFilters

  def render_flow_signals_table(signals, total_sampled, filtered_sampled, sample_limit, filters) do
    render_flow_signals_table(
      signals,
      total_sampled,
      filtered_sampled,
      sample_limit,
      filters,
      :page
    )
  end

  def render_flow_signals_table(
        signals,
        total_sampled,
        filtered_sampled,
        sample_limit,
        filters,
        mode
      ) do
    rows =
      case signals do
        [] ->
          colspan = if mode == :detail, do: 6, else: 8

          message =
            if mode == :page and not Map.get(filters, :scan_history, false) do
              "Signal history scan is off. Enable Scan histories to search sampled recent Flow history."
            else
              "No signal events found in loaded history"
            end

          ~s(<tr><td colspan="#{colspan}" class="c-muted">#{escape(message)}</td></tr>)

        _ ->
          Enum.map_join(signals, "\n", &render_flow_signal_row(&1, mode))
      end

    title = flow_signals_table_title(total_sampled, filtered_sampled, sample_limit, filters, mode)

    """
    <div class="section-title">#{title}</div>
    <table>
      <thead>
        #{render_flow_signals_table_head(mode)}
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  def render_flow_signal_row(row, :detail) do
    """
    <tr>
      <td class="mono"><a class="flow-event-link" href="#{flow_signal_event_href(row, :detail)}">#{escape(Map.get(row, :event_id, "-"))}</a></td>
      <td>#{format_timestamp_ms_or_dash(Map.get(row, :time_ms))}</td>
      <td class="mono">#{escape(Map.get(row, :signal, "-"))}</td>
      <td>#{flow_signal_state_move_html(row)}</td>
      <td class="mono">#{flow_signal_refs_summary_html(row, :detail)}</td>
      <td><a class="flow-link" href="#{flow_signal_event_href(row, :detail)}">event</a></td>
    </tr>
    """
  end

  def render_flow_signal_row(row, :page) do
    id = Map.get(row, :id, "")
    partition_key = Map.get(row, :partition_key)

    """
    <tr>
      <td class="mono">#{render_flow_id_link(id, partition_key)}</td>
      <td class="mono">#{escape(Map.get(row, :type, "-"))}</td>
      <td class="mono"><a class="flow-event-link" href="#{flow_signal_event_href(row, :page)}">#{escape(Map.get(row, :event_id, "-"))}</a></td>
      <td>#{format_timestamp_ms_or_dash(Map.get(row, :time_ms))}</td>
      <td class="mono">#{escape(Map.get(row, :signal, "-"))}</td>
      <td>#{flow_signal_state_move_html(row)}</td>
      <td class="mono">#{flow_signal_refs_summary_html(row, :page)}</td>
      <td><a class="flow-link" href="#{flow_signal_event_href(row, :page)}">detail</a></td>
    </tr>
    """
  end

  def render_flow_signals_table_head(:detail) do
    """
    <tr><th>Event</th><th>Time</th><th>Signal</th><th>State Change</th><th>Values</th><th>Jump</th></tr>
    """
  end

  def render_flow_signals_table_head(:page) do
    """
    <tr><th>Flow</th><th>Type</th><th>Event</th><th>Time</th><th>Signal</th><th>State Change</th><th>Values</th><th>Open</th></tr>
    """
  end

  def flow_signals_table_title(nil, _filtered_sampled, _sample_limit, _filters, :detail),
    do: "Signals"

  def flow_signals_table_title(total_sampled, filtered_sampled, sample_limit, filters, :page)
      when is_integer(total_sampled) and is_integer(filtered_sampled) and
             is_integer(sample_limit) do
    "Flow Signals <span class=\"badge badge-idle\">#{escape(flow_signals_filter_summary(filters))}</span> <span class=\"badge badge-idle\">sampled #{format_number(filtered_sampled)} / #{format_number(total_sampled)} / #{format_number(sample_limit)}</span>"
  end

  def flow_signals_table_title(
        _total_sampled,
        _filtered_sampled,
        _sample_limit,
        _filters,
        _mode
      ),
      do: "Signals"

  def flow_signal_state_move_html(row) do
    from_state = Map.get(row, :from_state, "")
    to_state = Map.get(row, :to_state, "")

    cond do
      is_binary(from_state) and from_state != "" and is_binary(to_state) and to_state != "" and
          from_state != to_state ->
        escape(from_state) <> " -> " <> escape(to_state)

      is_binary(to_state) and to_state != "" ->
        escape(to_state)

      true ->
        "-"
    end
  end

  def flow_signal_event_href(row, :detail) do
    "#" <> flow_history_event_anchor(Map.get(row, :event_id, "-"))
  end

  def flow_signal_event_href(row, :page) do
    anchor = flow_history_event_anchor(Map.get(row, :event_id, "-"))
    id = Map.get(row, :id, "")

    case id do
      "" -> "#" <> anchor
      id -> flow_detail_path(id, Map.get(row, :partition_key)) <> "#" <> anchor
    end
  end

  def flow_signal_refs_summary_html(row, mode) do
    record = Map.get(row, :record)
    badge_mode = if mode == :page, do: :detail_link, else: :local

    badges =
      row
      |> Map.get(:fields, %{})
      |> flow_value_ref_entries("signal event")
      |> Enum.map(&render_flow_value_ref_badge(record, badge_mode, &1))

    case badges do
      [] -> "-"
      _ -> Enum.join(badges, " ")
    end
  end
end
