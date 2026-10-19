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
        [] when mode == :page ->
          ""

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
    event_count = length(signals)

    count_label =
      if mode == :page and not Map.get(filters, :scan_history, false),
        do: "Not scanned",
        else:
          "#{format_number(event_count)} signal #{if event_count == 1, do: "event", else: "events"}"

    title = title <> ~s( <span class="badge badge-idle">#{count_label}</span>)

    table = """
    <table>
      <thead>
        #{render_flow_signals_table_head(mode)}
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """

    label = if mode == :detail, do: "Workflow signal history", else: "Workflow signals"

    body =
      if mode == :page and signals == [],
        do: render_flow_signals_empty(filters, filtered_sampled),
        else: accessible_table(label, table)

    """
    <h2 class="section-title">#{title}</h2>
    #{body}
    """
  end

  # Reading signals means reading each sampled run's history, so the scan stays
  # opt-in; the prompt makes it one click and keeps the current filters.
  defp render_flow_signals_empty(filters, candidates) do
    if Map.get(filters, :scan_history, false) do
      ~s(<p class="section-empty">No signal events in the scanned histories.</p>)
    else
      query =
        filters
        |> Map.put(:scan_history, true)
        |> FerricstoreServer.Health.Dashboard.Render.FlowQueryControls.flow_signals_filter_query()

      count = if is_integer(candidates), do: format_grouped(candidates) <> " ", else: ""

      """
      <div class="section-empty">
        <p>Signals are read from run histories, one read per run, so scans are opt-in.</p>
        <a class="flow-search-button" href="/dashboard/flow/signals?#{escape_attr(query)}">Scan #{count}run histories</a>
      </div>
      """
    end
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
      <td class="flow-run-identity mono">#{render_flow_id_link(id, partition_key)}<span class="flow-run-secondary mono" title="Partition">#{escape(partition_key || "auto/global")}</span></td>
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
    "Flow Signals <span class=\"badge badge-idle\">#{escape(flow_signals_filter_summary(filters))}</span>"
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
    "#journal-" <> flow_history_event_anchor(Map.get(row, :event_id, "-"))
  end

  def flow_signal_event_href(row, :page) do
    anchor = flow_history_event_anchor(Map.get(row, :event_id, "-"))
    id = Map.get(row, :id, "")

    case id do
      "" -> "#journal-" <> anchor
      id -> flow_detail_path(id, Map.get(row, :partition_key)) <> "#journal-" <> anchor
    end
  end

  def flow_signal_refs_summary_html(row, mode) do
    record = Map.get(row, :record)
    badge_mode = if mode == :page, do: :detail_link, else: :local

    badges =
      row
      |> Map.get(:fields, %{})
      |> flow_value_ref_entries("signal event")
      |> Enum.map(
        &Map.merge(&1, %{
          source: "historical",
          event_id: Map.get(row, :event_id),
          action: "Signaled",
          time: format_timestamp_ms_or_dash(Map.get(row, :time_ms))
        })
      )
      |> Enum.map(&render_flow_value_ref_badge(record, badge_mode, &1))

    case badges do
      [] -> "-"
      _ -> Enum.join(badges, " ")
    end
  end
end
