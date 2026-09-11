defmodule FerricstoreServer.Health.Dashboard.Render.FlowFilters do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.QueryParams

  @flow_dashboard_recent_limit 40
  @flow_dashboard_max_recent_limit 200
  @flow_terminal_states ~w(cancelled completed failed)
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

    default_time_mode =
      cond do
        range_filter -> "relative"
        Map.get(filters, :from_ms) || Map.get(filters, :to_ms) -> "custom"
        true -> "all"
      end

    time_mode = Map.get(filters, :time_mode, default_time_mode)
    available_types = Map.get(data, :available_types, [])

    available_states =
      data
      |> Map.get(:available_states, [])
      |> Kernel.++([state_filter])
      |> Kernel.++(@flow_terminal_states)
      |> Enum.filter(&(is_binary(&1) and &1 != ""))
      |> Enum.uniq()
      |> Enum.sort()

    type_datalist =
      render_flow_type_datalist("flow-state-type-options", [type_filter | available_types])

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

    range_options =
      @flow_dashboard_time_range_options
      |> Enum.reject(fn {range, _label} -> is_nil(range) end)
      |> Enum.map_join("", fn {range, label} ->
        ~s(<option value="#{range}"#{if range == (range_filter || "15m"), do: " selected", else: ""}>#{label}</option>)
      end)

    time_mode_options =
      Enum.map_join(
        [{"all", "All time"}, {"relative", "Relative"}, {"custom", "Custom"}],
        "",
        fn {mode, label} ->
          ~s(<option value="#{mode}"#{if mode == time_mode, do: " selected", else: ""}>#{label}</option>)
        end
      )

    errors = Map.get(filters, :errors, %{})
    draft = Map.get(filters, :draft, %{})
    invalid_from? = Map.has_key?(errors, :from) and is_nil(Map.get(filters, :from_ms))
    invalid_to? = Map.has_key?(errors, :to) and is_nil(Map.get(filters, :to_ms))

    custom_from_value =
      if range_filter, do: "", else: state_filter_time_value(Map.get(filters, :from_ms))

    custom_to_value =
      if range_filter, do: "", else: state_filter_time_value(Map.get(filters, :to_ms))

    custom_from_value = if invalid_from?, do: Map.get(draft, :from, ""), else: custom_from_value
    custom_to_value = if invalid_to?, do: Map.get(draft, :to, ""), else: custom_to_value

    clear =
      if flow_filter_active?(filters) or map_size(errors) > 0 do
        ~s(<a class="flow-filter-clear" href="/dashboard/flow/states" title="Clear Flow state filters">Clear</a>)
      else
        ""
      end

    filtered_sampled = Map.get(data, :filtered_sampled, Map.get(data, :total_sampled, 0))
    total_sampled = Map.get(data, :total_sampled, filtered_sampled)
    limit = Map.get(filters, :limit, @flow_dashboard_recent_limit)

    """
    <div class="flow-filter-panel">
      <form class="flow-filter-form flow-state-filter-form" action="/dashboard/flow/states" method="get">
        <label class="flow-filter-field" for="flow-state-type-filter">
        <span>Type</span>
        <input id="flow-state-type-filter" class="flow-search-input mono" type="search" name="type" value="#{escape_attr(type_filter || "")}" list="flow-state-type-options" placeholder="all types" autocomplete="off" title="Filter by workflow type; enter any known type for bounded cold lookup">
        #{type_datalist}
        </label>
        <label class="flow-filter-field" for="flow-state-state-filter">
        <span>Stored state</span>
        <select id="flow-state-state-filter" class="flow-search-input" name="state" title="Filter by current workflow state">
          #{state_options}
        </select>
        </label>
        <label class="flow-filter-field" for="flow-state-partition-filter">
        <span>Partition</span>
        <input id="flow-state-partition-filter" class="flow-search-input mono" type="search" name="partition_key" value="#{escape_attr(partition_key || "")}" placeholder="type + partition for cold" title="Filter by partition; one explicit type and partition enable cold terminal queries">
        </label>
        <label class="flow-filter-field" for="flow-state-name-filter">
        <span>ID</span>
        <input id="flow-state-name-filter" class="flow-search-input mono" type="search" name="q" value="#{escape_attr(name_filter || "")}" placeholder="contains" title="Filter by Flow ID substring">
        </label>
        <fieldset class="flow-filter-time-group">
        <legend>Updated time (UTC)</legend>
        <label class="flow-filter-field" for="flow-state-time-mode">
        <span>Time mode</span>
        <select id="flow-state-time-mode" class="flow-search-input" name="time_mode">#{time_mode_options}</select>
        </label>
        <label class="flow-filter-field" for="flow-state-range-filter" data-flow-time-mode="relative">
        <span>Range</span>
        <select id="flow-state-range-filter" class="flow-search-input flow-filter-range" name="range" title="Sliding updated-time window">
          #{range_options}
        </select>
        </label>
        <label class="flow-filter-field" for="flow-state-from-filter" data-flow-time-mode="custom">
        <span>From UTC</span>
        <input id="flow-state-from-filter" class="flow-search-input mono flow-filter-time" type="#{if invalid_from?, do: "text", else: "datetime-local"}" name="from" step="0.001" value="#{escape_attr(custom_from_value)}" title="Custom UTC start time"#{state_time_error_attrs(errors, :from)}>
        #{render_state_time_error(errors, :from)}
        </label>
        <label class="flow-filter-field" for="flow-state-to-filter" data-flow-time-mode="custom">
        <span>To UTC</span>
        <input id="flow-state-to-filter" class="flow-search-input mono flow-filter-time" type="#{if invalid_to?, do: "text", else: "datetime-local"}" name="to" step="0.001" value="#{escape_attr(custom_to_value)}" title="Custom UTC end time"#{state_time_error_attrs(errors, :to)}>
        #{render_state_time_error(errors, :to)}
        </label>
        </fieldset>
        <noscript><p class="flow-filter-note">Time mode controls which bounds are applied. Relative uses Range; Custom uses From UTC and To UTC.</p></noscript>
        <div class="flow-filter-actions">
        #{render_flow_summary_sort(Map.get(filters, :sort, "attention"), "flow-state-sort")}
        <label class="flow-filter-field" for="flow-state-limit-filter">
        <span>Recent Limit</span>
        <input id="flow-state-limit-filter" class="flow-search-input mono flow-filter-limit" type="number" name="limit" min="1" max="#{@flow_dashboard_max_recent_limit}" value="#{limit}" title="Maximum recent records shown below">
        </label>
        <button class="flow-search-button" type="submit" title="Apply Flow state filters">Apply</button>
        #{clear}
        </div>
      </form>
      <div class="flow-filter-note">
        #{if map_size(errors) > 0, do: ~s(<span role="alert">Query not run. Correct the time filters.</span>), else: "Showing #{escape(flow_filter_summary(filters))} · #{format_number(filtered_sampled)} matching of #{format_number(total_sampled)} sampled records"}
        #{info_icon("Relative mode uses a sliding updated-time window. Custom mode uses UTC bounds. One explicit type and partition enable bounded cold terminal lookup. All-type views remain sampled. Limit applies to Recent Flow Records only.", "About state filters")}
      </div>
    </div>
    """ <> render_state_time_validation_script()
  end

  defp state_time_error_attrs(errors, key) do
    if Map.has_key?(errors, key),
      do: ~s( aria-invalid="true" aria-describedby="flow-state-#{key}-error"),
      else: ""
  end

  defp state_filter_time_value(value),
    do: FerricstoreServer.Health.Dashboard.Flow.TimeFilter.input_value(value)

  defp render_state_time_error(errors, key) do
    case Map.get(errors, key) do
      nil ->
        ""

      message ->
        ~s(<span id="flow-state-#{key}-error" class="flow-field-error">#{escape(message)}</span>)
    end
  end

  defp render_state_time_validation_script do
    """
    <script>
    document.addEventListener("DOMContentLoaded", function () {
      var form = document.querySelector(".flow-state-filter-form");
      if (!form) { return; }
      var from = form.elements.from;
      var to = form.elements.to;
      var range = form.elements.range;
      var mode = form.elements.time_mode;
      function validate() {
        form.querySelectorAll("[data-flow-time-mode]").forEach(function (label) {
          label.hidden = label.dataset.flowTimeMode !== mode.value;
        });
        range.disabled = mode.value !== "relative";
        [from, to].forEach(function (input) { input.disabled = mode.value !== "custom"; });
        var reversed = mode.value === "custom" && from.value && to.value &&
          Date.parse(from.value + "Z") > Date.parse(to.value + "Z");
        to.setCustomValidity(reversed ? "From UTC must not be later than To UTC" : "");
      }
      form.addEventListener("input", validate);
      form.addEventListener("change", validate);
      validate();
    });
    </script>
    """
  end

  def render_flow_signals_filter(data) do
    filters = flow_signals_page_filters(data)
    type_filter = Map.get(filters, :type)
    partition_key = Map.get(filters, :partition_key)
    signal_filter = Map.get(filters, :signal)
    name_filter = Map.get(filters, :q)
    available_types = Map.get(data, :available_types, [])

    type_options = render_sampled_type_options(available_types, type_filter)

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
    scan_status = render_flow_signal_scan_status(Map.get(data, :signal_scan))

    """
    <div class="flow-filter-panel">
      <form class="flow-filter-form" action="/dashboard/flow/signals" method="get">
        <label class="flow-filter-field" for="flow-signal-type-filter">
          <span>Type</span>
          <select id="flow-signal-type-filter" class="flow-search-input" name="type" title="Filter signals by workflow type">
            #{type_options}
          </select>
        </label>
        <label class="flow-filter-field" for="flow-signal-partition-filter">
          <span>Partition</span>
          <input id="flow-signal-partition-filter" class="flow-search-input mono" type="search" name="partition_key" value="#{escape_attr(partition_key || "")}" placeholder="optional" title="Filter sampled workflows before reading any histories">
        </label>
        <label class="flow-filter-field" for="flow-signal-name-filter">
          <span>Signal</span>
          <input id="flow-signal-name-filter" class="flow-search-input mono" type="search" name="signal" value="#{escape_attr(signal_filter || "")}" placeholder="contains" title="Filter by signal name substring">
        </label>
        <label class="flow-filter-field" for="flow-signal-id-filter">
          <span>Flow ID</span>
          <input id="flow-signal-id-filter" class="flow-search-input mono" type="search" name="q" value="#{escape_attr(name_filter || "")}" placeholder="contains" title="Filter by Flow ID substring">
        </label>
        <label class="flow-filter-field" for="flow-signal-limit-filter">
          <span>Limit</span>
          <input id="flow-signal-limit-filter" class="flow-search-input mono flow-filter-limit" type="number" name="limit" min="1" max="#{@flow_dashboard_max_recent_limit}" value="#{limit}" title="Maximum signal rows shown below">
        </label>
        <label class="flow-check-label" title="Read recent Flow histories for the sampled flows. This is intentionally opt-in because it can be expensive under load.">
          <input type="checkbox" name="scan" value="true"#{scan_checked}> Scan histories
        </label>
        <button class="flow-search-button" type="submit" title="Apply Flow signal filters">Apply</button>
        #{clear}
      </form>
      <div class="flow-filter-note">
        #{escape(flow_signals_filter_summary(filters))} · #{format_number(filtered_sampled)} candidate workflows of #{format_number(total_sampled)} sampled
        #{scan_status}
        #{info_icon("Default view avoids history scans so the dashboard stays cheap during soak. Enable Scan histories to inspect recent sampled history, or use Flow detail for full paginated history.", "About signal history scans")}
      </div>
    </div>
    """
  end

  defp render_flow_signal_scan_status(%{requested: true} = scan) do
    inspected = Map.get(scan, :inspected_flows, 0)
    completed = Map.get(scan, :completed_flows, inspected)
    failed = Map.get(scan, :failed_flows, 0)
    limited = Map.get(scan, :history_limited_flows, 0)
    history_limit = Map.get(scan, :history_limit, 25)

    coverage =
      if Map.get(scan, :truncated, false) or failed > 0 or limited > 0 or
           Map.get(scan, :result_truncated, false) do
        "Partial coverage. Narrow Type, Partition, or Flow ID; open a workflow for older paginated history."
      else
        "Recent histories covered for these candidates; the workflow sample is not exhaustive."
      end

    failures =
      case failed do
        0 -> ""
        1 -> " · 1 history read failed"
        count -> " · #{format_number(count)} history reads failed"
      end

    history_bound =
      if limited > 0,
        do: " · #{limited} histories reached the #{history_limit}-event bound",
        else: ""

    result_bound =
      if Map.get(scan, :result_truncated, false),
        do:
          " · #{format_number(Map.get(scan, :matched_events, 0))} matching events in loaded histories; result limit reached",
        else: ""

    " · Auto-refresh paused · #{format_number(completed)} histories read (#{format_number(inspected)} attempted)#{failures}#{history_bound}#{result_bound}. #{coverage}"
  end

  defp render_flow_signal_scan_status(_scan), do: ""

  def render_flow_summary_sort(sort, id) do
    options =
      Enum.map_join(
        [{"attention", "Attention first"}, {"distribution", "Distribution"}],
        fn {value, label} ->
          ~s(<option value="#{value}"#{if sort == value, do: " selected", else: ""}>#{label}</option>)
        end
      )

    ~s(<label class="flow-filter-field" for="#{id}"><span>Order</span><select class="flow-search-input" id="#{id}" name="sort">#{options}</select></label>)
  end

  def render_flow_type_datalist(id, types) when is_binary(id) and is_list(types) do
    options =
      types
      |> Enum.filter(&(is_binary(&1) and &1 != ""))
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.map_join("", &~s(<option value="#{escape_attr(&1)}"></option>))

    ~s(<datalist id="#{escape_attr(id)}">#{options}</datalist>)
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
    Enum.any?([:type, :partition_key, :signal, :q], fn key ->
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
      flow_filter_partition_label(Map.get(filters, :partition_key)),
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

  defp render_sampled_type_options(available_types, selected_type) do
    types =
      [selected_type | available_types]
      |> Enum.reject(&(&1 in [nil, ""]))
      |> Enum.uniq()
      |> Enum.sort()

    all_selected = if is_nil(selected_type), do: " selected", else: ""
    all = ~s(<option value=""#{all_selected}>All types</option>)

    options =
      Enum.map_join(types, "\n", fn type ->
        selected = if type == selected_type, do: " selected", else: ""
        ~s(<option value="#{escape_attr(type)}"#{selected}>#{escape(type)}</option>)
      end)

    all <> "\n" <> options
  end
end
