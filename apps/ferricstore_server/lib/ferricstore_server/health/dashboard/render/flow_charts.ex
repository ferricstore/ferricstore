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
    <div class="section-title">Step Waterfall</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">Step durations</div>
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
    range = flow_step_waterfall_range(rows)
    axis_html = render_flow_step_waterfall_axis(range)
    row_html = Enum.map_join(rows, "\n", &render_flow_step_waterfall_row(&1, range))
    caption = "#{length(rows)} events on this page · click a row to jump to the event row"

    """
    <div class="flow-step-waterfall">
      <div class="flow-step-waterfall-scroll">
        <div class="flow-step-waterfall-header">
          <span>Step</span>
          <span class="flow-step-waterfall-axis">#{axis_html}</span>
          <span>Elapsed</span>
        </div>
        <div class="flow-step-waterfall-rows">
          #{row_html}
        </div>
      </div>
      <div class="flow-timeline-caption">#{escape(caption)}</div>
    </div>
    """
  end

  def flow_step_waterfall_range(rows) do
    times =
      rows
      |> Enum.map(& &1.time_ms)
      |> Enum.filter(&is_integer/1)

    min_time = Enum.min(times, fn -> nil end)

    max_time =
      rows
      |> Enum.map(fn row ->
        case row.time_ms do
          time when is_integer(time) -> time + max(Map.get(row, :duration_ms, 0), 0)
          _ -> nil
        end
      end)
      |> Enum.reject(&is_nil/1)
      |> Enum.max(fn -> min_time end)

    total_ms =
      case {min_time, max_time} do
        {start_ms, end_ms} when is_integer(start_ms) and is_integer(end_ms) ->
          max(end_ms - start_ms, 1)

        _ ->
          1
      end

    %{min_time: min_time, max_time: max_time, total_ms: total_ms}
  end

  def render_flow_step_waterfall_axis(%{min_time: nil}) do
    ~s(<span class="flow-step-waterfall-axis-label" style="left: 0%">event order</span>)
  end

  def render_flow_step_waterfall_axis(%{total_ms: total_ms}) do
    [0, div(total_ms, 4), div(total_ms, 2), div(total_ms * 3, 4), total_ms]
    |> Enum.uniq()
    |> Enum.map_join("\n", fn offset_ms ->
      left = flow_step_waterfall_percent(offset_ms, total_ms)
      label = "+" <> format_duration_ms(offset_ms)

      ~s(<span class="flow-step-waterfall-axis-label" style="left: #{left}%">#{escape(label)}</span>)
    end)
  end

  def render_flow_step_waterfall_row(row, range) do
    anchor = flow_history_event_anchor(row.event_id)
    title = flow_timeline_event_title(row)
    label = flow_timeline_node_label_text(row)
    state_move = flow_history_state_move(row)
    action = flow_history_event_label(row.fields)
    duration_ms = max(Map.get(row, :duration_ms, 0), 0)
    offset_ms = flow_step_waterfall_offset_ms(row, range)
    left = flow_step_waterfall_percent(offset_ms, range.total_ms)
    width = flow_step_waterfall_width_percent(duration_ms, range.total_ms, left)
    class = flow_timeline_bar_class(row)
    duration = format_duration_ms(duration_ms)
    offset = "+" <> format_duration_ms(offset_ms)

    """
    <a class="flow-step-waterfall-row" href="##{anchor}" title="#{escape_attr(title)}">
      <span class="flow-step-waterfall-label">
        <span class="flow-step-waterfall-step">#{escape(label)}</span>
        <span class="flow-step-waterfall-state">#{escape(action)} · #{escape(state_move)}</span>
      </span>
      <span class="flow-step-waterfall-track">
        <span class="flow-step-waterfall-marker" style="left: #{left}%"></span>
        <span class="flow-step-waterfall-bar #{class}" style="left: #{left}%; width: #{width}%"></span>
      </span>
      <span class="flow-step-waterfall-duration">
        <span>#{escape(duration)}</span>
        <span>#{escape(offset)}</span>
      </span>
    </a>
    """
  end

  def flow_step_waterfall_offset_ms(%{time_ms: time_ms}, %{min_time: min_time})
      when is_integer(time_ms) and is_integer(min_time) do
    max(time_ms - min_time, 0)
  end

  def flow_step_waterfall_offset_ms(_row, _range), do: 0

  def flow_step_waterfall_width_percent(duration_ms, total_ms, left_percent) do
    width = flow_step_waterfall_percent(duration_ms, total_ms)

    width
    |> max(0.6)
    |> min(max(100.0 - left_percent, 0.6))
  end

  def flow_step_waterfall_percent(value, total_ms) do
    percent = max(value, 0) / max(total_ms, 1) * 100.0

    percent
    |> max(0.0)
    |> min(100.0)
    |> Float.round(2)
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
