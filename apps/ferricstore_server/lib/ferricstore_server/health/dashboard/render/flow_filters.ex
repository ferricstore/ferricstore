defmodule FerricstoreServer.Health.Dashboard.Render.FlowFilters do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.QueryParams

  @flow_dashboard_recent_limit 40
  @flow_dashboard_max_recent_limit 200
  @flow_dashboard_time_range_options [
    {nil, "All time"},
    {"5m", "Last 5 minutes"},
    {"15m", "Last 15 minutes"},
    {"1h", "Last 1 hour"},
    {"6h", "Last 6 hours"},
    {"24h", "Last 24 hours"}
  ]

  def render_flow_type_filter(data) do
    filters =
      Map.get(data, :filters, %{
        type: Map.get(data, :type_filter),
        state: nil,
        partition_key: nil,
        q: nil,
        range: nil,
        from_ms: nil,
        to_ms: nil,
        limit: @flow_dashboard_recent_limit
      })

    type_filter = Map.get(filters, :type)
    state_filter = Map.get(filters, :state)
    name_filter = Map.get(filters, :q)
    partition_key = Map.get(filters, :partition_key)
    range_filter = Map.get(filters, :range)
    available_types = Map.get(data, :available_types, [])
    available_states = Map.get(data, :available_states, [])

    type_options =
      [nil | available_types]
      |> Enum.map_join("\n", fn
        nil ->
          selected = if is_nil(type_filter), do: " selected", else: ""
          ~s(<option value=""#{selected}>All types</option>)

        type ->
          selected = if type == type_filter, do: " selected", else: ""
          ~s(<option value="#{escape_attr(type)}"#{selected}>#{escape(type)}</option>)
      end)

    state_options =
      [nil | available_states]
      |> Enum.map_join("\n", fn
        nil ->
          selected = if is_nil(state_filter), do: " selected", else: ""
          ~s(<option value=""#{selected}>All states</option>)

        state ->
          selected = if state == state_filter, do: " selected", else: ""
          ~s(<option value="#{escape_attr(state)}"#{selected}>#{escape(state)}</option>)
      end)

    range_options = render_flow_range_options(range_filter)

    custom_from_value =
      if range_filter, do: "", else: flow_filter_time_value(Map.get(filters, :from_ms))

    custom_to_value =
      if range_filter, do: "", else: flow_filter_time_value(Map.get(filters, :to_ms))

    clear =
      if flow_filter_active?(filters) do
        ~s(<a class="flow-filter-clear" href="/dashboard/flow/states" title="Clear Flow state filters">Clear</a>)
      else
        ""
      end

    filtered_sampled = Map.get(data, :filtered_sampled, Map.get(data, :total_sampled, 0))
    total_sampled = Map.get(data, :total_sampled, filtered_sampled)
    limit = Map.get(filters, :limit, @flow_dashboard_recent_limit)

    """
    <div class="flow-filter-panel">
      <form class="flow-filter-form" action="/dashboard/flow/states" method="get">
        <label for="flow-state-type-filter">Type</label>
        <select id="flow-state-type-filter" class="flow-search-input" name="type" title="Filter by workflow type">
          #{type_options}
        </select>
        <label for="flow-state-state-filter">State</label>
        <select id="flow-state-state-filter" class="flow-search-input" name="state" title="Filter by current workflow state">
          #{state_options}
        </select>
        <label for="flow-state-partition-filter">Partition</label>
        <input id="flow-state-partition-filter" class="flow-search-input mono" type="search" name="partition_key" value="#{escape_attr(partition_key || "")}" placeholder="required for cold rows" title="Filter by partition and enable cold terminal queries">
        <label for="flow-state-name-filter">ID</label>
        <input id="flow-state-name-filter" class="flow-search-input mono" type="search" name="q" value="#{escape_attr(name_filter || "")}" placeholder="contains" title="Filter by Flow ID substring">
        <label for="flow-state-range-filter">Updated</label>
        <select id="flow-state-range-filter" class="flow-search-input flow-filter-range" name="range" title="Use a quick sliding window or Custom for From/To">
          #{range_options}
        </select>
        <label for="flow-state-from-filter">From UTC</label>
        <input id="flow-state-from-filter" class="flow-search-input mono flow-filter-time" type="datetime-local" name="from" step="60" value="#{escape_attr(custom_from_value)}" title="Custom UTC start time, used when Updated is All time">
        <label for="flow-state-to-filter">To UTC</label>
        <input id="flow-state-to-filter" class="flow-search-input mono flow-filter-time" type="datetime-local" name="to" step="60" value="#{escape_attr(custom_to_value)}" title="Custom UTC end time, used when Updated is All time">
        <label for="flow-state-limit-filter">Recent Limit</label>
        <input id="flow-state-limit-filter" class="flow-search-input mono flow-filter-limit" type="number" name="limit" min="1" max="#{@flow_dashboard_max_recent_limit}" value="#{limit}" title="Maximum recent records shown below">
        <button class="flow-search-button" type="submit" title="Apply Flow state filters">Apply</button>
        #{clear}
      </form>
      <div class="flow-filter-note">
        Showing #{escape(flow_filter_summary(filters))} · #{format_number(filtered_sampled)} / #{format_number(total_sampled)} sampled records
        #{info_icon("Updated quick ranges are sliding windows and override custom From/To. Custom times are interpreted as UTC. Limit applies to Recent Flow Records only.")}
      </div>
    </div>
    """
  end

  def render_flow_signals_filter(data) do
    filters = flow_signals_page_filters(data)
    type_filter = Map.get(filters, :type)
    signal_filter = Map.get(filters, :signal)
    name_filter = Map.get(filters, :q)
    available_types = Map.get(data, :available_types, [])

    type_options =
      [nil | available_types]
      |> Enum.map_join("\n", fn
        nil ->
          selected = if is_nil(type_filter), do: " selected", else: ""
          ~s(<option value=""#{selected}>All types</option>)

        type ->
          selected = if type == type_filter, do: " selected", else: ""
          ~s(<option value="#{escape_attr(type)}"#{selected}>#{escape(type)}</option>)
      end)

    clear =
      if flow_signal_filter_active?(filters) do
        ~s(<a class="flow-filter-clear" href="/dashboard/flow/signals" title="Clear Flow signal filters">Clear</a>)
      else
        ""
      end

    filtered_sampled = Map.get(data, :filtered_sampled, Map.get(data, :total_sampled, 0))
    total_sampled = Map.get(data, :total_sampled, filtered_sampled)
    limit = Map.get(filters, :limit, @flow_dashboard_recent_limit)
    scan_checked = if Map.get(filters, :scan_history, false), do: " checked", else: ""

    """
    <div class="flow-filter-panel">
      <form class="flow-filter-form" action="/dashboard/flow/signals" method="get">
        <label for="flow-signal-type-filter">Type</label>
        <select id="flow-signal-type-filter" class="flow-search-input" name="type" title="Filter signals by workflow type">
          #{type_options}
        </select>
        <label for="flow-signal-name-filter">Signal</label>
        <input id="flow-signal-name-filter" class="flow-search-input mono" type="search" name="signal" value="#{escape_attr(signal_filter || "")}" placeholder="contains" title="Filter by signal name substring">
        <label for="flow-signal-id-filter">Flow ID</label>
        <input id="flow-signal-id-filter" class="flow-search-input mono" type="search" name="q" value="#{escape_attr(name_filter || "")}" placeholder="contains" title="Filter by Flow ID substring">
        <label for="flow-signal-limit-filter">Limit</label>
        <input id="flow-signal-limit-filter" class="flow-search-input mono flow-filter-limit" type="number" name="limit" min="1" max="#{@flow_dashboard_max_recent_limit}" value="#{limit}" title="Maximum signal rows shown below">
        <label class="flow-check-label" title="Read recent Flow histories for the sampled flows. This is intentionally opt-in because it can be expensive under load.">
          <input type="checkbox" name="scan" value="true"#{scan_checked}> Scan histories
        </label>
        <button class="flow-search-button" type="submit" title="Apply Flow signal filters">Apply</button>
        #{clear}
      </form>
      <div class="flow-filter-note">
        Showing #{escape(flow_signals_filter_summary(filters))} · #{format_number(filtered_sampled)} / #{format_number(total_sampled)} sampled records
        #{info_icon("Default view avoids history scans so the dashboard stays cheap during soak. Enable Scan histories to inspect recent sampled history, or use Flow detail for full paginated history.")}
      </div>
    </div>
    """
  end

  def render_flow_range_options(selected_range) do
    Enum.map_join(@flow_dashboard_time_range_options, "\n", fn {range, label} ->
      value = range || ""
      selected = if range == selected_range, do: " selected", else: ""
      ~s(<option value="#{escape_attr(value)}"#{selected}>#{escape(label)}</option>)
    end)
  end

  def flow_filter_active?(filters) do
    Enum.any?([:type, :state, :partition_key, :q, :range, :from_ms, :to_ms], fn key ->
      case Map.get(filters, key) do
        nil -> false
        "" -> false
        _ -> true
      end
    end) or Map.get(filters, :limit, @flow_dashboard_recent_limit) != @flow_dashboard_recent_limit
  end

  def flow_filter_time_value(nil), do: ""

  def flow_filter_time_value(value) when is_integer(value) do
    case DateTime.from_unix(value, :millisecond) do
      {:ok, datetime} ->
        datetime
        |> DateTime.to_iso8601()
        |> binary_part(0, 16)

      _ ->
        Integer.to_string(value)
    end
  end

  def flow_filter_summary(filters) do
    [
      Map.get(filters, :type) || "all types",
      Map.get(filters, :state) || "all states",
      flow_filter_partition_label(Map.get(filters, :partition_key)),
      flow_filter_name_label(Map.get(filters, :q)),
      flow_filter_time_label(filters),
      flow_filter_limit_label(Map.get(filters, :limit))
    ]
    |> Enum.reject(&(&1 == ""))
    |> Enum.join(" / ")
  end

  def flow_filter_name_label(nil), do: ""
  def flow_filter_name_label(query), do: "id contains #{query}"

  def flow_filter_partition_label(nil), do: ""
  def flow_filter_partition_label(partition_key), do: "partition #{partition_key}"

  def flow_signal_filter_active?(filters) do
    Enum.any?([:type, :signal, :q], fn key ->
      case Map.get(filters, key) do
        nil -> false
        "" -> false
        _ -> true
      end
    end) or Map.get(filters, :limit, @flow_dashboard_recent_limit) != @flow_dashboard_recent_limit or
      Map.get(filters, :scan_history, false)
  end

  def flow_signals_filter_summary(filters) do
    [
      Map.get(filters, :type) || "all types",
      flow_signal_name_label(Map.get(filters, :signal)),
      flow_filter_name_label(Map.get(filters, :q)),
      flow_filter_limit_label(Map.get(filters, :limit)),
      flow_signal_scan_label(Map.get(filters, :scan_history, false))
    ]
    |> Enum.reject(&(&1 == ""))
    |> Enum.join(" / ")
  end

  def flow_signal_name_label(nil), do: ""
  def flow_signal_name_label(signal), do: "signal contains #{signal}"

  def flow_signal_scan_label(true), do: "history scan enabled"
  def flow_signal_scan_label(_), do: "history scan off"

  def flow_filter_time_range_label(nil, nil), do: ""

  def flow_filter_time_range_label(from_ms, nil),
    do: "updated from #{flow_filter_time_display(from_ms)}"

  def flow_filter_time_range_label(nil, to_ms),
    do: "updated to #{flow_filter_time_display(to_ms)}"

  def flow_filter_time_range_label(from_ms, to_ms),
    do: "updated #{flow_filter_time_display(from_ms)}..#{flow_filter_time_display(to_ms)}"

  def flow_filter_time_label(%{range: range}) when is_binary(range) do
    case flow_time_range_label(range) do
      "" -> ""
      label -> "updated #{label}"
    end
  end

  def flow_filter_time_label(filters) when is_map(filters) do
    flow_filter_time_range_label(Map.get(filters, :from_ms), Map.get(filters, :to_ms))
  end

  def flow_time_range_label("5m"), do: "last 5 minutes"
  def flow_time_range_label("15m"), do: "last 15 minutes"
  def flow_time_range_label("1h"), do: "last 1 hour"
  def flow_time_range_label("6h"), do: "last 6 hours"
  def flow_time_range_label("24h"), do: "last 24 hours"
  def flow_time_range_label(_range), do: ""

  def flow_filter_time_display(value) when is_integer(value), do: flow_filter_time_value(value)

  def flow_filter_limit_label(@flow_dashboard_recent_limit), do: ""
  def flow_filter_limit_label(limit) when is_integer(limit), do: "recent limit #{limit}"
  def flow_filter_limit_label(_limit), do: ""
end
