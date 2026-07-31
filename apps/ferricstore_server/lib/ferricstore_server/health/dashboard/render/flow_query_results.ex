defmodule FerricstoreServer.Health.Dashboard.Render.FlowQueryResults do
  @moduledoc false

  alias Ferricstore.Flow.Query.Field

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory

  @quality_fields [
    {:exactness, "Exactness"},
    {:freshness, "Freshness"},
    {:coverage, "Coverage"},
    {:pagination, "Pagination"}
  ]

  @usage_fields [
    {:range_seeks, "Range seeks", :number},
    {:range_pages, "Range pages", :number},
    {:scanned_entries, "Scanned entries", :number},
    {:scanned_bytes, "Scanned bytes", :bytes},
    {:hydrated_records, "Hydrated records", :number},
    {:residual_checks, "Residual checks", :number},
    {:duplicate_entries, "Duplicates removed", :number},
    {:result_records, "Result records", :number},
    {:response_bytes, "Response bytes", :bytes},
    {:memory_high_water_bytes, "Memory high water", :bytes},
    {:wall_time_us, "Wall time", :duration_us}
  ]

  @chart_color_count 10
  @time_chart_width 480
  @time_chart_height 160
  @time_plot_left 36
  @time_plot_right 12
  @time_plot_top 18
  @time_plot_bottom 130

  def flow_query_result_command(%{command: command}) when is_binary(command), do: command
  def flow_query_result_command(_result), do: "FLOW.QUERY"

  def render_flow_query_status(%{status: :ok, message: message}),
    do: ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)

  def render_flow_query_status(%{status: :idle, message: message}),
    do: ~s(<div class="flow-section-note">#{escape(message)}</div>)

  def render_flow_query_status(%{status: _status, message: message}),
    do: ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)

  def render_flow_query_status(_result), do: ""

  def render_flow_query_metadata(result) when is_map(result) do
    quality = render_metadata_group("Query Quality", Map.get(result, :quality), @quality_fields)
    usage = render_metadata_group("Query Usage", Map.get(result, :usage), @usage_fields)
    page = render_page_status(Map.get(result, :page))

    if quality == "" and usage == "" and page == "" do
      ""
    else
      ~s(<div class="flow-query-metadata">#{quality}#{usage}#{page}</div>)
    end
  end

  def render_flow_query_metadata(_result), do: ""

  def render_flow_query_visualization(%{
        visualization: %{scope: :current_page, row_count: row_count, charts: charts}
      })
      when is_integer(row_count) and is_list(charts) and charts != [] do
    rendered = Enum.map_join(charts, "", &render_page_chart/1)

    """
    <details class="flow-query-visualization" open>
      <summary>
        <span>Visualize current page</span>
        <span class="badge badge-idle">Current page &middot; #{format_number(row_count)} rows</span>
      </summary>
      <div class="flow-query-chart-grid">#{rendered}</div>
    </details>
    """
  end

  def render_flow_query_visualization(_result), do: ""

  def render_flow_query_table(%{explain: explain}) when is_map(explain) do
    render_explain(explain)
  end

  def render_flow_query_table(%{presentation: :workbench, scalar: scalar})
      when is_map(scalar) do
    kind = Map.get(scalar, :kind) || "result"
    value = Map.get(scalar, :value)

    """
    <div class="flow-query-table-wrap">
      <table class="flow-query-table">
        <caption class="sr-only">Query scalar result</caption>
        <thead><tr><th scope="col">Result</th><th scope="col">Value</th></tr></thead>
        <tbody><tr><td class="mono">#{escape(to_string(kind))}</td><td class="mono">#{escape(display_cell_value(value))}</td></tr></tbody>
      </table>
    </div>
    """
  end

  def render_flow_query_table(%{
        presentation: :workbench,
        columns: columns,
        column_selectors: selectors,
        rows: rows,
        source: source
      })
      when is_list(columns) and is_list(selectors) and is_list(rows) do
    header = Enum.map_join(columns, "", &"<th scope=\"col\">#{escape(&1)}</th>")
    body = render_projected_rows(rows, source, selectors)

    """
    <div class="flow-query-table-wrap">
      <table class="flow-query-table flow-query-projection-table">
        <caption class="sr-only">Query result records</caption>
        <thead><tr>#{header}</tr></thead>
        <tbody>#{body}</tbody>
      </table>
    </div>
    """
  end

  def render_flow_query_table(result) do
    """
    <div class="flow-query-table-wrap">
      <table class="flow-query-table">
        <caption class="sr-only">Query result records</caption>
        <thead>
          <tr><th scope="col">ID / Event</th><th scope="col">Type</th><th scope="col">State / Action</th><th scope="col">Time</th><th scope="col">Worker</th><th scope="col">Values</th></tr>
        </thead>
        <tbody>#{render_flow_query_rows(result)}</tbody>
      </table>
    </div>
    """
  end

  def render_flow_query_continuation(%{continuation: continuation})
      when is_map(continuation) do
    fql = Map.get(continuation, :fql, "")
    params_json = Map.get(continuation, :params_json, "{}")
    cursor = Map.get(continuation, :cursor, "")
    surface = render_continuation_surface(continuation)

    """
    <form class="flow-query-pagination" action="/dashboard/flow/query" method="post">
      #{surface}
      <input type="hidden" name="fql" value="#{escape_attr(fql)}">
      <input type="hidden" name="params_json" value="#{escape_attr(params_json)}">
      <input type="hidden" name="cursor" value="#{escape_attr(cursor)}">
      <button class="flow-search-button secondary" type="submit" name="action" value="run">Next page</button>
    </form>
    """
  end

  def render_flow_query_continuation(_result), do: ""

  defp render_continuation_surface(%{mode: :guided, guided_query: guided_query})
       when is_binary(guided_query) and guided_query != "" do
    """
    <input type="hidden" name="surface" value="guided">
    <input type="hidden" name="guided_query" value="#{escape_attr(guided_query)}">
    """
  end

  defp render_continuation_surface(_continuation), do: ""

  defp render_page_chart(%{kind: :category, field: field, values: values}) do
    render_donut_chart(chart_title(field), values)
  end

  defp render_page_chart(%{kind: :time, field: field, values: values}) do
    render_time_chart(chart_title(field), values)
  end

  defp render_page_chart(_chart), do: ""

  defp render_donut_chart(title, values) do
    total = Enum.reduce(values, 0, &(&1.count + &2))

    {segments, _offset} =
      values
      |> Enum.with_index()
      |> Enum.map_reduce(0.0, fn {value, index}, offset ->
        percentage = if total > 0, do: value.count * 100.0 / total, else: 0.0
        color = rem(index, @chart_color_count)

        segment = """
        <circle class="flow-query-chart-segment flow-query-chart-color-#{color}" cx="60" cy="60" r="44" pathLength="100" stroke-dasharray="#{svg_number(percentage)} #{svg_number(100.0 - percentage)}" stroke-dashoffset="-#{svg_number(offset)}">
          <title>#{escape(value.label)}: #{format_number(value.count)} (#{percentage_label(percentage)})</title>
        </circle>
        """

        {segment, offset + percentage}
      end)

    legend =
      values
      |> Enum.with_index()
      |> Enum.map_join("", fn {value, index} ->
        percentage = if total > 0, do: value.count * 100.0 / total, else: 0.0
        color = rem(index, @chart_color_count)

        """
        <li>
          <span class="flow-query-chart-swatch flow-query-chart-color-#{color}" aria-hidden="true"></span>
          <span class="flow-query-chart-label mono" title="#{escape_attr(value.label)}">#{escape(value.label)}</span>
          <strong class="mono">#{format_number(value.count)}</strong>
          <span class="flow-query-chart-percent mono">#{percentage_label(percentage)}</span>
        </li>
        """
      end)

    """
    <section class="flow-query-chart flow-query-category-chart">
      <div class="flow-query-chart-title">#{escape(title)}</div>
      <div class="flow-query-donut-layout">
        <svg class="flow-query-donut" role="img" aria-label="#{escape_attr(title)} distribution across #{format_number(total)} rows" viewBox="0 0 120 120">
          <title>#{escape(title)} distribution</title>
          <desc>Current-page counts grouped by #{escape(title)}.</desc>
          <circle class="flow-query-donut-track" cx="60" cy="60" r="44"></circle>
          #{Enum.join(segments)}
          <text class="flow-query-donut-total" x="60" y="58" text-anchor="middle">#{format_number(total)}</text>
          <text class="flow-query-donut-caption" x="60" y="73" text-anchor="middle">rows</text>
        </svg>
        <ol class="flow-query-chart-legend">#{legend}</ol>
      </div>
    </section>
    """
  end

  defp render_time_chart(title, values) do
    maximum = values |> Enum.map(& &1.count) |> Enum.max(fn -> 1 end) |> max(1)
    plot_width = @time_chart_width - @time_plot_left - @time_plot_right
    plot_height = @time_plot_bottom - @time_plot_top
    slot_width = plot_width / max(length(values), 1)
    bar_width = max(slot_width - 10, 8)

    bars =
      values
      |> Enum.with_index()
      |> Enum.map_join("", fn {value, index} ->
        height = value.count * plot_height / maximum
        x = @time_plot_left + index * slot_width + (slot_width - bar_width) / 2
        y = @time_plot_bottom - height
        color = rem(index, @chart_color_count)

        """
        <rect class="flow-query-time-bar flow-query-chart-color-#{color}" x="#{svg_number(x)}" y="#{svg_number(y)}" width="#{svg_number(bar_width)}" height="#{svg_number(height)}" rx="3">
          <title>#{escape(time_bucket_label(value))}: #{format_number(value.count)}</title>
        </rect>
        """
      end)

    first = List.first(values)
    last = List.last(values)

    """
    <section class="flow-query-chart flow-query-chart-time">
      <div class="flow-query-chart-title">#{escape(title)} over time</div>
      <svg class="flow-query-time-chart" role="img" aria-label="#{escape_attr(title)} distribution across #{format_number(length(values))} time buckets" viewBox="0 0 #{@time_chart_width} #{@time_chart_height}">
        <title>#{escape(title)} over time</title>
        <desc>Current-page records grouped into chronological time buckets. Empty intervals remain visible.</desc>
        #{render_time_grid(maximum)}
        #{bars}
      </svg>
      <div class="flow-query-time-range mono">
        <span>#{escape(time_endpoint_label(first, :from_ms))}</span>
        <span>#{escape(time_endpoint_label(last, :to_ms))}</span>
      </div>
    </section>
    """
  end

  defp render_time_grid(maximum) do
    midpoint = div(maximum + 1, 2)

    [
      {@time_plot_top, maximum},
      {div(@time_plot_top + @time_plot_bottom, 2), midpoint},
      {@time_plot_bottom, 0}
    ]
    |> Enum.map_join("", fn {y, label} ->
      """
      <line class="flow-query-time-grid" x1="#{@time_plot_left}" y1="#{y}" x2="#{@time_chart_width - @time_plot_right}" y2="#{y}"></line>
      <text class="flow-query-time-tick" x="#{@time_plot_left - 7}" y="#{y + 3}" text-anchor="end">#{format_number(label)}</text>
      """
    end)
  end

  defp time_endpoint_label(nil, _key), do: "-"

  defp time_endpoint_label(value, key) when is_map(value),
    do: value |> Map.fetch!(key) |> format_timestamp_ms_or_dash()

  defp percentage_label(value) do
    rounded = Float.round(value, 1)

    if rounded == trunc(rounded),
      do: "#{trunc(rounded)}%",
      else: :erlang.float_to_binary(rounded, decimals: 1) <> "%"
  end

  defp svg_number(value) when is_integer(value), do: Integer.to_string(value)
  defp svg_number(value) when is_float(value), do: :erlang.float_to_binary(value, decimals: 2)

  defp time_bucket_label(%{from_ms: from_ms, to_ms: to_ms}) when from_ms == to_ms,
    do: format_timestamp_ms_or_dash(from_ms)

  defp time_bucket_label(%{from_ms: from_ms, to_ms: to_ms}),
    do: "#{format_timestamp_ms_or_dash(from_ms)} - #{format_timestamp_ms_or_dash(to_ms)}"

  defp chart_title(field) when is_binary(field) do
    field
    |> String.replace("_ms", "")
    |> String.replace("_", " ")
    |> String.capitalize()
  end

  def render_flow_query_rows(%{scalar: %{kind: kind, value: value}}) do
    """
    <tr>
      <td class="mono">#{escape(to_string(kind || "result"))}</td>
      <td colspan="5" class="mono">#{escape(inspect(value, limit: 20))}</td>
    </tr>
    """
  end

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

  defp render_projected_rows([], _source, selectors) do
    colspan = max(length(selectors), 1)
    ~s(<tr><td colspan="#{colspan}" class="c-muted">No rows.</td></tr>)
  end

  defp render_projected_rows(rows, source, selectors) do
    Enum.map_join(rows, "\n", fn
      row when is_map(row) ->
        cells = Enum.map_join(selectors, "", &render_projected_cell(row, source, &1))
        "<tr>#{cells}</tr>"

      other ->
        colspan = max(length(selectors), 1)

        ~s(<tr><td colspan="#{colspan}" class="mono">#{escape(display_cell_value(other))}</td></tr>)
    end)
  end

  defp render_projected_cell(record, :runs, :run_id) do
    value = projected_value(record, :runs, :run_id)
    partition = projected_value(record, :runs, :partition_key)

    case value do
      id when is_binary(id) and id != "" ->
        ~s(<td class="mono">#{render_flow_id_link(id, partition)}</td>)

      _missing ->
        ~s(<td class="c-muted">-</td>)
    end
  end

  defp render_projected_cell(record, source, selector) do
    value = projected_value(record, source, selector)
    class = projected_cell_class(selector, value)
    ~s(<td class="#{class}">#{escape(display_projected_value(selector, value))}</td>)
  end

  defp projected_value(record, :runs, selector)
       when selector in [:attributes, :state_meta] do
    fetch_value(record, selector)
  end

  defp projected_value(record, :runs, selector) do
    case Field.fetch(record, selector) do
      {:ok, value} -> value
      :missing -> nil
    end
  end

  defp projected_value(record, :events, {:event_field, name}) do
    case fetch_value(record, :fields) do
      fields when is_map(fields) -> fetch_value(fields, name)
      _missing -> nil
    end
  end

  defp projected_value(record, :events, selector) when selector in [:event_id, :fields],
    do: fetch_value(record, selector)

  defp projected_value(_record, _source, _selector), do: nil

  defp fetch_value(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp fetch_value(map, key) when is_map(map) and is_binary(key), do: Map.get(map, key)
  defp fetch_value(_map, _key), do: nil

  defp projected_cell_class(:state, value) when is_binary(value), do: flow_state_class(value)
  defp projected_cell_class(_selector, value) when is_integer(value), do: "mono num"
  defp projected_cell_class(_selector, _value), do: "mono"

  defp display_projected_value(selector, value)
       when selector in [
              :created_at_ms,
              :updated_at_ms,
              :next_run_at_ms,
              :lease_deadline_ms
            ] and is_integer(value),
       do: format_timestamp_ms_or_dash(value)

  defp display_projected_value(_selector, value), do: display_cell_value(value)

  defp display_cell_value(nil), do: "null"
  defp display_cell_value(value) when is_binary(value), do: value
  defp display_cell_value(value) when is_integer(value) or is_float(value), do: to_string(value)
  defp display_cell_value(value) when is_boolean(value), do: to_string(value)

  defp display_cell_value(value) when is_map(value) or is_list(value) do
    inspect(value, limit: 20, printable_limit: 512, charlists: :as_lists)
  end

  defp display_cell_value(value), do: inspect(value, limit: 10, printable_limit: 256)

  defp render_explain(explain) do
    plan = Map.get(explain, :plan, %{}) || %{}
    estimate = Map.get(explain, :estimate, %{}) || %{}
    actual = Map.get(explain, :actual, %{}) || %{}
    bounds = Map.get(explain, :bounds, %{}) || %{}
    stats = Map.get(explain, :stats, %{}) || %{}
    decision = Map.get(explain, :decision, %{}) || %{}

    """
    <div class="flow-query-explain">
      #{render_chosen_plan(explain, plan)}
      #{render_explain_metrics(estimate, actual, bounds)}
      #{render_explain_decision(decision, stats)}
      #{render_explain_diagnostic(Map.get(explain, :diagnostic))}
      #{render_explain_alternatives(Map.get(explain, :alternatives, []))}
    </div>
    """
  end

  defp render_chosen_plan(explain, plan) do
    index = Map.get(plan, :index)
    projection = Map.get(plan, :projection, %{}) || %{}

    """
    <section class="flow-query-plan-section">
      <div class="flow-query-plan-title">Chosen Plan</div>
      <dl class="flow-query-plan-grid">
        #{plan_item("Status", Map.get(explain, :status))}
        #{plan_item("Path", Map.get(plan, :path))}
        #{plan_item("Record source", Map.get(plan, :record_source))}
        #{plan_item("Order", Map.get(plan, :order))}
        #{plan_identifier_item("Index", explain_index_name(index))}
        #{plan_item("Generation", explain_index_generation(index), "Generation ")}
        #{plan_item("Projection", projection_fields(projection))}
        #{plan_item("Hydration", hydration_label(projection))}
      </dl>
    </section>
    """
  end

  defp plan_item(label, value, prefix \\ "")

  defp plan_item(_label, nil, _prefix), do: ""
  defp plan_item(_label, "", _prefix), do: ""

  defp plan_item(label, value, prefix) do
    "<div><dt>#{escape(label)}</dt><dd class=\"mono\">#{escape(prefix <> human_value(value))}</dd></div>"
  end

  defp plan_identifier_item(_label, nil), do: ""
  defp plan_identifier_item(_label, ""), do: ""

  defp plan_identifier_item(label, value) do
    "<div><dt>#{escape(label)}</dt><dd class=\"mono\">#{escape(to_string(value))}</dd></div>"
  end

  defp explain_index_name(index) when is_map(index), do: Map.get(index, :logical_id)
  defp explain_index_name(index) when is_binary(index), do: index
  defp explain_index_name(_index), do: nil

  defp explain_index_generation(index) when is_map(index), do: Map.get(index, :generation)
  defp explain_index_generation(_index), do: nil

  defp projection_fields(%{fields: fields}) when is_list(fields), do: Enum.join(fields, ", ")
  defp projection_fields(%{fields: fields}) when is_binary(fields), do: fields
  defp projection_fields(_projection), do: nil

  defp hydration_label(%{requires_hydration: true}), do: "required"
  defp hydration_label(%{requires_hydration: false}), do: "not required"
  defp hydration_label(_projection), do: nil

  @explain_metrics [
    {:range_seeks, "Range seeks", :number},
    {:scanned_entries, "Scanned entries", :number},
    {:scanned_bytes, "Scanned bytes", :bytes},
    {:hydrated_records, "Hydrated records", :number},
    {:result_records, "Result records", :number},
    {:response_bytes, "Response bytes", :bytes},
    {:executor_memory_bytes, "Executor memory", :bytes},
    {:wall_time_ms, "Wall time", :duration_ms},
    {:wall_time_us, "Wall time", :duration_us},
    {:cost, "Planner cost", :number},
    {:scan_records, "Scan records", :number}
  ]

  defp render_explain_metrics(estimate, actual, bounds) do
    rows =
      Enum.flat_map(@explain_metrics, fn {key, label, format} ->
        values = [Map.get(estimate, key), Map.get(actual, key), Map.get(bounds, key)]

        if Enum.all?(values, &is_nil/1) do
          []
        else
          [
            "<tr><th scope=\"row\">#{escape(label)}</th>",
            metric_cell(Enum.at(values, 0), format),
            metric_cell(Enum.at(values, 1), format),
            metric_cell(Enum.at(values, 2), format),
            "</tr>"
          ]
        end
      end)
      |> Enum.join()

    if rows == "" do
      ""
    else
      """
      <section class="flow-query-plan-section">
        <div class="flow-query-plan-title">Estimates and Bounds</div>
        <div class="flow-query-table-wrap">
          <table class="flow-query-plan-metrics">
            <caption class="sr-only">Query plan estimates and bounds</caption>
            <thead><tr><th scope="col">Resource</th><th scope="col">Estimate</th><th scope="col">Actual</th><th scope="col">Hard bound</th></tr></thead>
            <tbody>#{rows}</tbody>
          </table>
        </div>
      </section>
      """
    end
  end

  defp metric_cell(nil, _format), do: ~s(<td class="c-muted">-</td>)

  defp metric_cell(value, format),
    do: ~s(<td class="mono">#{escape(format_explain_metric(value, format))}</td>)

  defp format_explain_metric(value, :bytes) when is_integer(value) and value >= 0,
    do: format_bytes(value)

  defp format_explain_metric(value, :duration_us) when is_integer(value) and value >= 0,
    do: format_duration_us(value)

  defp format_explain_metric(value, :duration_ms) when is_integer(value) and value >= 0,
    do: format_duration_ms(value)

  defp format_explain_metric(value, :number) when is_integer(value) and value >= 0,
    do: format_number(value)

  defp format_explain_metric(value, _format), do: human_value(value)

  defp render_explain_decision(decision, stats) do
    if map_size(decision) == 0 and map_size(stats) == 0 do
      ""
    else
      """
      <section class="flow-query-plan-section">
        <div class="flow-query-plan-title">Planner Decision</div>
        <dl class="flow-query-plan-grid">
          #{plan_item("Reason", Map.get(decision, :reason))}
          #{plan_item("Bounded candidates", Map.get(decision, :bounded_candidate_count))}
          #{plan_item("Cost model", Map.get(decision, :cost_model))}
          #{plan_item("Statistics", Map.get(stats, :source))}
          #{plan_item("Confidence", Map.get(stats, :confidence))}
          #{plan_item("Statistics age", stats_age(Map.get(stats, :age_ms)))}
        </dl>
      </section>
      """
    end
  end

  defp stats_age(value) when is_integer(value) and value >= 0,
    do: format_duration_us(value * 1_000)

  defp stats_age(_value), do: nil

  defp render_explain_diagnostic(diagnostic) when is_map(diagnostic) do
    message = Map.get(diagnostic, :message) || Map.get(diagnostic, :detail)
    hint = Map.get(diagnostic, :hint)

    """
    <section class="flow-query-plan-section">
      <div class="flow-query-plan-title">Diagnostic</div>
      <div class="flow-alert flow-alert-error">#{escape(human_value(message))}</div>
      #{if is_binary(hint), do: ~s(<div class="flow-section-note">#{escape(hint)}</div>), else: ""}
    </section>
    """
  end

  defp render_explain_diagnostic(_diagnostic), do: ""

  defp render_explain_alternatives(alternatives)
       when is_list(alternatives) and alternatives != [] do
    rows =
      Enum.map_join(alternatives, "", fn alternative ->
        comparison = Map.get(alternative, :comparison, %{}) || %{}

        """
        <tr>
          <td class="mono">#{escape(human_value(Map.get(alternative, :path)))}</td>
          <td class="mono">#{escape(human_value(Map.get(alternative, :record_source)))}</td>
          <td class="mono">#{escape(human_value(Map.get(comparison, :reason_not_selected)))}</td>
        </tr>
        """
      end)

    """
    <section class="flow-query-plan-section">
      <div class="flow-query-plan-title">Alternatives</div>
      <div class="flow-query-table-wrap"><table><caption class="sr-only">Alternative query plans</caption><thead><tr><th scope="col">Path</th><th scope="col">Source</th><th scope="col">Why not selected</th></tr></thead><tbody>#{rows}</tbody></table></div>
    </section>
    """
  end

  defp render_explain_alternatives(_alternatives), do: ""

  defp human_value(nil), do: ""
  defp human_value(value) when is_binary(value), do: String.replace(value, "_", " ")
  defp human_value(value) when is_atom(value) or is_number(value), do: to_string(value)
  defp human_value(value), do: display_cell_value(value)

  defp render_metadata_group(_title, value, _fields) when not is_map(value), do: ""

  defp render_metadata_group(title, value, fields) do
    rows =
      fields
      |> Enum.flat_map(fn field -> metadata_row(value, field) end)
      |> Enum.join()

    if rows == "" do
      ""
    else
      """
      <section class="flow-query-metadata-group">
        <div class="flow-query-metadata-title">#{escape(title)}</div>
        <dl class="flow-query-metadata-list">#{rows}</dl>
      </section>
      """
    end
  end

  defp metadata_row(values, {key, label}) do
    metadata_row(values, {key, label, :token})
  end

  defp metadata_row(values, {key, label, format}) do
    case Map.fetch(values, key) do
      {:ok, value} ->
        [
          "<div><dt>",
          escape(label),
          "</dt><dd>",
          escape(format_metadata_value(value, format)),
          "</dd></div>"
        ]

      :error ->
        []
    end
  end

  defp render_page_status(%{has_more: true}) do
    ~s(<div class="flow-section-note flow-query-page-note">More rows are available through the authenticated query cursor.</div>)
  end

  defp render_page_status(_page), do: ""

  defp format_metadata_value(value, :token) when is_binary(value),
    do: String.replace(value, "_", " ")

  defp format_metadata_value(value, :bytes) when is_integer(value) and value >= 0,
    do: format_bytes(value)

  defp format_metadata_value(value, :duration_us) when is_integer(value) and value >= 0,
    do: format_duration_us(value)

  defp format_metadata_value(value, :number) when is_integer(value) and value >= 0,
    do: format_number(value)

  defp format_metadata_value(value, _format) when is_binary(value), do: value
  defp format_metadata_value(value, _format), do: inspect(value, limit: 10)
end
