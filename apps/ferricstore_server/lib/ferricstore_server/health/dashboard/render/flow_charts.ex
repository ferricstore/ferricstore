defmodule FerricstoreServer.Health.Dashboard.Render.FlowCharts do

import FerricstoreServer.Health.Dashboard.Format
import FerricstoreServer.Health.Dashboard.Render.FlowHistory

@flow_dashboard_timeline_chart_max_events 80

  def render_flow_issue_cards(summary) do
    due_now = Map.get(summary, :due_now_sampled, 0)
    expired = Map.get(summary, :expired_leases_sampled, 0)
    failed = Map.get(summary, :failed, 0)

    if due_now == 0 and expired == 0 and failed == 0 do
      ""
    else
      render_flow_issue_cards(due_now, expired, failed)
    end
  end

  def render_flow_issue_cards(due_now, expired, failed) do
    due_class = if due_now > 0, do: "badge-warning", else: "badge-ok"
    expired_class = if expired > 0, do: "badge-pressure", else: "badge-ok"
    failed_class = if failed > 0, do: "badge-pressure", else: "badge-ok"

    """
    <div class="section-title">Task Issues</div>
    <div class="flow-issue-row">
      <div class="flow-issue"><span class="badge #{due_class}">#{format_number(due_now)}</span><span>due now in sample</span></div>
      <div class="flow-issue"><span class="badge #{expired_class}">#{format_number(expired)}</span><span>expired leases in sample</span></div>
      <div class="flow-issue"><span class="badge #{failed_class}">#{format_number(failed)}</span><span>failed terminal flows</span></div>
    </div>
    """
  end

  def render_flow_states_chart(states) do
    rows =
      states
      |> Enum.take(16)
      |> Enum.map(fn state ->
        %{
          label: "#{state.type}:#{state.state}",
          values: [
            {"Due", state.due_now, "bar-yellow"},
            {"Running", state.running, "bar-green"},
            {"Retry", Map.get(state, :retrying, 0), "bar-blue"},
            {"Failed", Map.get(state, :failed, 0), "bar-red"},
            {"Expired", state.expired_leases, "bar-red"}
          ]
        }
      end)

    """
    <div class="section-title">State Charts</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">State pressure</div>
        #{render_bar_chart(rows)}
      </div>
    </div>
    """
  end

  def render_flow_workers_chart(workers) do
    rows =
      workers
      |> Enum.take(20)
      |> Enum.map(fn worker ->
        %{
          label: worker.worker,
          values: [
            {"Running", worker.running, "bar-green"},
            {"Expired", worker.expired, "bar-red"}
          ]
        }
      end)

    """
    <div class="section-title">Worker Charts</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">Lease health by worker</div>
        #{render_bar_chart(rows)}
      </div>
    </div>
    """
  end

  def render_flow_due_chart(due_now, scheduled) do
    rows = [
      %{
        label: "Claim readiness",
        values: [
          {"Due now", length(due_now), "bar-yellow"},
          {"Scheduled", length(scheduled), "bar-blue"}
        ]
      }
    ]

    """
    <div class="section-title">Due Charts</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">Due vs scheduled</div>
        #{render_bar_chart(rows)}
      </div>
    </div>
    """
  end

  def render_flow_timeline_chart(history) do
    timeline =
      history
      |> flow_history_timeline_rows()
      |> Enum.take(@flow_dashboard_timeline_chart_max_events)
      |> Enum.reverse()

    """
    <div class="section-title">Timeline Chart</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">State graph</div>
        #{render_timeline_sequence(timeline)}
      </div>
    </div>
    """
  end

  def render_bar_chart([]), do: ~s(<div class="chart-empty">No chart data</div>)

  def render_bar_chart(rows) do
    max_value =
      rows
      |> Enum.flat_map(& &1.values)
      |> Enum.map(fn {_label, value, _class} -> numeric_metric_value(value) end)
      |> Enum.max(fn -> 0 end)
      |> max(1)

    row_html =
      Enum.map_join(rows, "\n", fn row ->
        bars =
          Enum.map_join(row.values, "\n", fn {label, value, class} ->
            value = numeric_metric_value(value)
            width = max(2, round(value / max_value * 100))

            """
            <div class="chart-bar-line">
              <span class="chart-bar-label">#{escape(label)}</span>
              <span class="chart-bar-track"><span class="chart-bar-fill #{class}" style="width: #{width}%"></span></span>
              <span class="chart-bar-value">#{format_number(value)}</span>
            </div>
            """
          end)

        """
        <div class="chart-row">
          <div class="chart-row-label">#{escape(row.label)}</div>
          <div class="chart-row-bars">#{bars}</div>
        </div>
        """
      end)

    ~s(<div class="chart-bars">#{row_html}</div>)
  end

  def render_timeline_sequence([]), do: ~s(<div class="chart-empty">No timeline events</div>)

  def render_timeline_sequence(timeline) do
    rows = flow_timeline_duration_rows(timeline)
    states = flow_timeline_states(rows)
    layout = flow_timeline_graph_layout(rows, states)
    points = flow_timeline_graph_points(rows, states, layout)

    lane_html = render_flow_timeline_lanes(states, layout)
    axis_html = render_flow_timeline_axis(layout)
    path_html = render_flow_timeline_path(points)
    duration_html = render_flow_timeline_duration_segments(points)
    transition_html = render_flow_timeline_transition_segments(points)
    node_html = render_flow_timeline_nodes(points)
    caption = "#{length(rows)} events on this page · click a node to jump to the event row"

    """
    <div class="flow-timeline-graph">
      <div class="flow-timeline-scroll">
        <svg class="flow-timeline-svg" viewBox="0 0 #{layout.width} #{layout.height}" width="#{layout.width}" height="#{layout.height}" role="img" aria-label="Flow state timeline graph">
          <rect class="flow-timeline-bg" x="0" y="0" width="#{layout.width}" height="#{layout.height}" rx="8"></rect>
          #{lane_html}
          #{axis_html}
          #{duration_html}
          #{transition_html}
          #{path_html}
          #{node_html}
        </svg>
      </div>
      <div class="flow-timeline-caption">#{escape(caption)}</div>
    </div>
    """
  end

  def flow_timeline_states(rows) do
    states =
      rows
      |> Enum.map(&flow_timeline_state_label/1)
      |> Enum.reject(&(&1 in ["", "-"]))
      |> Enum.uniq()

    case states do
      [] -> ["event"]
      _ -> states
    end
  end

  def flow_timeline_graph_layout(rows, states) do
    count = length(rows)
    lane_count = max(length(states), 1)
    left = 132
    right = 52
    top = 42
    bottom = 52
    lane_gap = 66
    step = flow_timeline_graph_step(count)
    plot_width = max(640, max(count - 1, 1) * step)

    times =
      rows
      |> Enum.map(& &1.time_ms)
      |> Enum.filter(&is_integer/1)

    min_time = Enum.min(times, fn -> nil end)
    max_time = Enum.max(times, fn -> nil end)

    %{
      width: left + plot_width + right,
      height: top + bottom + max(lane_count - 1, 0) * lane_gap,
      left: left,
      right: right,
      top: top,
      bottom: bottom,
      lane_gap: lane_gap,
      plot_width: plot_width,
      count: count,
      min_time: min_time,
      max_time: max_time
    }
  end

  def flow_timeline_graph_step(count) when count > 60, do: 38
  def flow_timeline_graph_step(count) when count > 40, do: 46
  def flow_timeline_graph_step(count) when count > 20, do: 58
  def flow_timeline_graph_step(_count), do: 88

  def flow_timeline_graph_points(rows, states, layout) do
    state_index = states |> Enum.with_index() |> Map.new()
    count = length(rows)

    rows
    |> Enum.with_index()
    |> Enum.map(fn {row, index} ->
      state = flow_timeline_state_label(row)
      lane = Map.get(state_index, state, 0)

      row
      |> Map.put(:state, state)
      |> Map.put(:x, flow_timeline_x(row, index, count, layout))
      |> Map.put(:y, flow_timeline_y(lane, layout))
    end)
  end

  def flow_timeline_x(row, index, count, %{min_time: min_time, max_time: max_time} = layout)
       when is_integer(min_time) and is_integer(max_time) and max_time > min_time do
    case row.time_ms do
      time when is_integer(time) ->
        layout.left + round((time - min_time) / max(max_time - min_time, 1) * layout.plot_width)

      _ ->
        flow_timeline_index_x(index, count, layout)
    end
  end

  def flow_timeline_x(_row, index, count, layout),
    do: flow_timeline_index_x(index, count, layout)

  def flow_timeline_index_x(index, count, layout) do
    layout.left + round(index / max(count - 1, 1) * layout.plot_width)
  end

  def flow_timeline_y(lane, layout), do: layout.top + lane * layout.lane_gap

  def render_flow_timeline_lanes(states, layout) do
    states
    |> Enum.with_index()
    |> Enum.map_join("\n", fn {state, index} ->
      y = flow_timeline_y(index, layout)
      x2 = layout.width - layout.right + 12

      """
      <g class="flow-timeline-lane">
        <line x1="#{layout.left}" y1="#{y}" x2="#{x2}" y2="#{y}"></line>
        <text class="flow-timeline-lane-label" x="#{layout.left - 14}" y="#{y + 4}" text-anchor="end">#{escape(state)}</text>
      </g>
      """
    end)
  end

  def render_flow_timeline_axis(%{min_time: nil}), do: ""

  def render_flow_timeline_axis(%{min_time: min_time, max_time: max_time} = layout) do
    baseline_y = layout.height - layout.bottom + 20
    max_time = max_time || min_time

    ticks =
      if max_time > min_time do
        [min_time, min_time + div(max_time - min_time, 2), max_time]
      else
        [min_time]
      end

    ticks
    |> Enum.uniq()
    |> Enum.map_join("\n", fn tick ->
      x =
        layout.left +
          if max_time > min_time do
            round((tick - min_time) / max(max_time - min_time, 1) * layout.plot_width)
          else
            0
          end

      """
      <g class="flow-timeline-axis">
        <line x1="#{x}" y1="#{layout.top - 16}" x2="#{x}" y2="#{baseline_y - 8}"></line>
        <text class="flow-timeline-axis-label" x="#{x}" y="#{baseline_y}" text-anchor="middle">#{escape(format_timeline_timestamp_ms(tick))}</text>
      </g>
      """
    end)
  end

  def render_flow_timeline_path([]), do: ""

  def render_flow_timeline_path(points) do
    d =
      points
      |> Enum.with_index()
      |> Enum.map_join(" ", fn {point, index} ->
        prefix = if index == 0, do: "M", else: "L"
        "#{prefix} #{point.x} #{point.y}"
      end)

    ~s(<path class="flow-timeline-path" d="#{d}"></path>)
  end

  def render_flow_timeline_duration_segments(points) do
    points
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map_join("\n", fn [point, next_point] ->
      class = flow_timeline_bar_class(point)
      duration = format_duration_ms(Map.get(point, :duration_ms, 0))
      title = flow_timeline_event_title(point)

      """
      <line class="flow-timeline-duration-segment #{class}" x1="#{point.x}" y1="#{point.y}" x2="#{next_point.x}" y2="#{point.y}">
        <title>#{escape(title <> " · held " <> duration)}</title>
      </line>
      """
    end)
  end

  def render_flow_timeline_transition_segments(points) do
    points
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map_join("\n", fn [point, next_point] ->
      mid_x = round((point.x + next_point.x) / 2)

      d =
        "M #{point.x} #{point.y} C #{mid_x} #{point.y} #{mid_x} #{next_point.y} #{next_point.x} #{next_point.y}"

      ~s(<path class="flow-timeline-transition" d="#{d}"></path>)
    end)
  end

  def render_flow_timeline_nodes(points) do
    dense? = length(points) > 28

    points
    |> Enum.map_join("\n", fn point ->
      anchor = flow_history_event_anchor(point.event_id)
      title = flow_timeline_event_title(point)
      label = flow_timeline_node_label_text(point)
      node_class = flow_timeline_node_class(point)
      label_html = if dense?, do: "", else: render_flow_timeline_node_label(point, label)

      """
      <a href="##{anchor}" class="flow-timeline-node-link">
        <circle class="flow-timeline-node #{node_class}" cx="#{point.x}" cy="#{point.y}" r="7">
          <title>#{escape(title)}</title>
        </circle>
      </a>
      #{label_html}
      """
    end)
  end

  def render_flow_timeline_node_label(point, label) do
    y = point.y - 13

    ~s(<text class="flow-timeline-node-label" x="#{point.x + 10}" y="#{y}">#{escape(label)}</text>)
  end

  def flow_timeline_node_label_text(row) do
    label = flow_history_event_label(row.fields)

    case label do
      "Retry" -> "Retry"
      "Failed" -> "Failed"
      "Completed" -> "Completed"
      "Cancelled" -> "Cancelled"
      _ -> flow_timeline_state_label(row)
    end
  end

  def flow_timeline_node_class(row) do
    fields = row.fields

    cond do
      flow_history_terminal_event?(fields) -> "flow-timeline-node-terminal"
      flow_history_event_label(fields) == "Retry" -> "flow-timeline-node-retry"
      flow_history_event_label(fields) == "Failed" -> "flow-timeline-node-failed"
      true -> "flow-timeline-node-normal"
    end
  end

  def flow_timeline_duration_rows(timeline) do
    timeline
    |> Enum.with_index()
    |> Enum.map(fn {row, index} ->
      next_row = Enum.at(timeline, index + 1)
      Map.put(row, :duration_ms, flow_timeline_duration_ms(row, next_row))
    end)
  end

  def flow_timeline_duration_ms(%{time_ms: start_ms}, %{time_ms: end_ms})
       when is_integer(start_ms) and is_integer(end_ms) and end_ms >= start_ms do
    end_ms - start_ms
  end

  def flow_timeline_duration_ms(_row, _next_row), do: 0

  def flow_timeline_bar_class(row) do
    fields = row.fields

    cond do
      flow_history_terminal_event?(fields) -> "bar-green"
      flow_history_event_label(fields) == "Retry" -> "bar-red"
      flow_history_event_label(fields) == "Failed" -> "bar-red"
      true -> "bar-blue"
    end
  end

  def flow_timeline_state_label(row) do
    case row.to_state do
      state when is_binary(state) and state != "" -> state
      _ -> flow_timeline_previous_state_label(row)
    end
  end

  def flow_timeline_previous_state_label(row) do
    case Map.get(row, :from_state) do
      state when is_binary(state) and state != "" -> state
      _ -> flow_history_event_label(row.fields)
    end
  end

  def flow_timeline_event_title(row) do
    [
      to_string(row.event_id),
      format_timestamp_ms_or_dash(row.time_ms),
      flow_history_event_label(row.fields),
      flow_history_state_move(row),
      "duration #{format_duration_ms(Map.get(row, :duration_ms, 0))}"
    ]
    |> Enum.reject(&(&1 in ["", "-"]))
    |> Enum.join(" · ")
  end

  def numeric_metric_value(value) when is_integer(value), do: value
  def numeric_metric_value(value) when is_float(value), do: round(value)

  def numeric_metric_value(value) when is_binary(value) do
    case Float.parse(value) do
      {parsed, _rest} -> round(parsed)
      :error -> 0
    end
  end

  def numeric_metric_value(_value), do: 0
end
