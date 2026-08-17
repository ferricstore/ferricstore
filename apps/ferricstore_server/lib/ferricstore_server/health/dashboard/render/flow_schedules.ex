defmodule FerricstoreServer.Health.Dashboard.Render.FlowSchedules do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.FlowOverview, only: [render_flow_stat_card: 3]

  def render_flow_schedules_summary(data) do
    summary = Map.get(data, :summary, %{})

    """
    <div class="flow-card-grid">
      #{render_flow_stat_card("Schedules", Map.get(summary, :total, 0), "bounded durable catalog result")}
      #{render_flow_stat_card("Active", Map.get(summary, "active", 0), "eligible for scheduler firing")}
      #{render_flow_stat_card("Paused", Map.get(summary, "paused", 0), "disabled until resumed")}
      #{render_flow_stat_card("Failed", Map.get(summary, "failed", 0), "failed schedule definitions")}
    </div>
    """
  end

  def render_flow_schedules_flash(%{kind: :ok, message: message}),
    do: ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)

  def render_flow_schedules_flash(%{kind: :error, message: message}),
    do: ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)

  def render_flow_schedules_flash(_flash), do: ""

  def render_flow_schedules_filters(data) do
    filters = Map.get(data, :filters, %{})

    """
    <div class="flow-filter-panel">
      <form class="flow-search" action="/dashboard/flow/schedules" method="get" aria-label="Schedule filters">
        <input class="flow-search-input mono" type="search" name="q" aria-label="Schedule ID contains" value="#{escape_attr(Map.get(filters, :q) || "")}" placeholder="schedule id contains..." title="Filter the bounded catalog result by schedule id substring">
        #{schedule_select("state", Map.get(filters, :state, :all), ["all", "active", "paused", "running", "failed", "completed", "cancelled"])}
        #{schedule_select("kind", Map.get(filters, :kind), ["", "one_shot", "delay", "interval", "cron"])}
        <input class="flow-search-input mono" type="number" min="1" max="500" name="limit" aria-label="Schedule limit" value="#{Map.get(filters, :limit, 100)}" title="Maximum schedules to show">
        <button class="flow-search-button" type="submit">Filter</button>
      </form>
      <div class="flow-filter-note">State and kind are applied during the durable catalog scan. ID contains filters the bounded rows retained by Limit.</div>
    </div>
    """
  end

  def render_flow_schedules_table(schedules) when is_list(schedules),
    do: render_flow_schedules_table(schedules, %{})

  def render_flow_schedules_table(schedules, filters)
      when is_list(schedules) and is_map(filters) do
    rows =
      if schedules == [] do
        ~s(<tr><td colspan="12" class="c-muted">No schedules matched the current filters.</td></tr>)
      else
        Enum.map_join(schedules, "\n", &render_flow_schedule_row(&1, filters))
      end

    """
    <div class="section-title">Schedules</div>
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>State</th>
          <th>Kind</th>
          <th>Next Due</th>
          <th>Last Fire</th>
          <th>Fires</th>
          <th>Target</th>
          <th>Overlap</th>
          <th>Catch-up</th>
          <th>End</th>
          <th>Last Target</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  def render_flow_failed_schedules([]), do: ""

  def render_flow_failed_schedules(failed_schedules) do
    rows =
      Enum.map_join(failed_schedules, "\n", fn schedule ->
        """
        <tr>
          <td class="mono">#{escape(Map.get(schedule, :id, "-"))}</td>
          <td>#{escape(Map.get(schedule, :last_overlap_reason, Map.get(schedule, :end_reason, "-")) || "-")}</td>
          <td>#{format_timestamp_ms_or_dash(Map.get(schedule, :last_overlap_at_ms))}</td>
          <td class="mono">#{escape(Map.get(schedule, :last_overlap_target_id, "-") || "-")}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Failed Schedules</div>
    <table>
      <thead><tr><th>ID</th><th>Reason</th><th>At</th><th>Previous Target</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table>
    """
  end

  defp render_flow_schedule_row(%{error: reason}, _filters) do
    ~s(<tr><td colspan="12" class="flow-alert-error">#{escape(reason)}</td></tr>)
  end

  defp render_flow_schedule_row(schedule, filters) do
    id = Map.get(schedule, :id, "")
    target = Map.get(schedule, :target, %{})

    """
    <tr>
      <td class="mono">#{escape(id)}</td>
      <td><span class="#{schedule_state_class(Map.get(schedule, :state))}">#{escape(Map.get(schedule, :state, "-"))}</span></td>
      <td class="mono">#{escape(schedule_kind(schedule))}</td>
      <td>#{format_timestamp_ms_or_dash(Map.get(schedule, :next_run_at_ms))}</td>
      <td>#{format_timestamp_ms_or_dash(Map.get(schedule, :last_fire_at_ms))}</td>
      <td class="mono">#{format_number(Map.get(schedule, :fire_count, 0))}</td>
      <td class="mono">#{escape(Map.get(target, :type, "-"))}</td>
      <td>#{schedule_overlap_summary(schedule)}</td>
      <td>#{schedule_catchup_summary(schedule)}</td>
      <td>#{schedule_end_summary(schedule)}</td>
      <td class="mono">#{escape(Map.get(schedule, :last_target_id, "-") || "-")}</td>
      <td>#{render_flow_schedule_actions(schedule, filters)}</td>
    </tr>
    """
  end

  defp render_flow_schedule_actions(schedule, filters) do
    id = Map.get(schedule, :id, "")
    state = Map.get(schedule, :state)

    [
      if(state == "active", do: schedule_confirmation(schedule, "fire", "Fire", filters)),
      if(state == "active", do: schedule_action_button(id, "pause", "Pause", filters)),
      if(state == "paused", do: schedule_action_button(id, "resume", "Resume", filters)),
      if(state in ["active", "paused", "failed"],
        do: schedule_confirmation(schedule, "delete", "Delete", filters, true)
      )
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
  end

  defp schedule_action_button(id, action, label, filters, danger? \\ false) do
    class = if danger?, do: "flow-search-button flow-danger-button", else: "flow-search-button"

    """
    <form style="display:inline" action="/dashboard/flow/schedules" method="post">
      <input type="hidden" name="id" value="#{escape_attr(id)}">
      #{schedule_filter_inputs(filters)}
      <button class="#{class}" type="submit" name="action" value="#{escape_attr(action)}">#{escape(label)}</button>
    </form>
    """
  end

  defp schedule_confirmation(schedule, action, label, filters, danger? \\ false) do
    id = Map.get(schedule, :id, "")
    state = Map.get(schedule, :state, "")
    version = Map.get(schedule, :version, "")
    class = if danger?, do: "flow-search-button flow-danger-button", else: "flow-search-button"

    """
    <details class="flow-action-confirm">
      <summary class="#{class}">#{escape(label)}</summary>
      <div class="flow-action-confirm-panel">
        <strong>Confirm #{escape(label)}</strong>
        <span class="mono">#{escape(id)}</span>
        <form action="/dashboard/flow/schedules" method="post" data-dashboard-single-submit>
          <input type="hidden" name="id" value="#{escape_attr(id)}">
          <input type="hidden" name="action" value="#{escape_attr(action)}">
          <input type="hidden" name="confirm_action" value="true">
          <input type="hidden" name="expected_state" value="#{escape_attr(state)}">
          <input type="hidden" name="expected_version" value="#{version |> to_string() |> escape_attr()}">
          #{schedule_filter_inputs(filters)}
          <button class="#{class}" type="submit">Confirm #{escape(label)}</button>
        </form>
      </div>
    </details>
    """
  end

  defp schedule_filter_inputs(filters) do
    [
      {"state", Map.get(filters, :state)},
      {"kind", Map.get(filters, :kind)},
      {"q", Map.get(filters, :q)},
      {"limit", Map.get(filters, :limit)}
    ]
    |> Enum.reject(fn {_name, value} -> value in [nil, ""] end)
    |> Enum.map_join("", fn {name, value} ->
      ~s(<input type="hidden" name="#{name}" value="#{value |> to_string() |> escape_attr()}">)
    end)
  end

  defp schedule_select(name, selected, values) do
    aria_label = if name == "state", do: "Schedule state", else: "Schedule kind"

    options =
      Enum.map_join(values, "\n", fn value ->
        option_label = if value == "", do: "any kind", else: value
        selected_attr = if to_string(selected || "") == value, do: " selected", else: ""

        ~s(<option value="#{escape_attr(value)}"#{selected_attr}>#{escape(option_label)}</option>)
      end)

    ~s(<select class="flow-search-input mono" name="#{escape_attr(name)}" aria-label="#{aria_label}">#{options}</select>)
  end

  defp schedule_kind(schedule), do: schedule |> Map.get(:kind, "-") |> to_string()

  defp schedule_overlap_summary(schedule) do
    policy = schedule |> Map.get(:overlap_policy, :allow) |> to_string()
    reason = Map.get(schedule, :last_overlap_reason)

    if is_binary(reason) and reason != "" do
      "#{escape(policy)}<br><span class=\"c-muted\">#{escape(reason)}</span>"
    else
      escape(policy)
    end
  end

  defp schedule_catchup_summary(%{catchup_policy: policy} = schedule)
       when not is_nil(policy) do
    count = Map.get(schedule, :coalesced_count, 0)
    last_count = Map.get(schedule, :last_coalesced_count, 0)
    last_at_ms = Map.get(schedule, :last_catchup_at_ms)

    details =
      if count > 0 do
        "#{format_number(count)} coalesced<br>last #{format_number(last_count)} at #{format_timestamp_ms_or_dash(last_at_ms)}"
      end

    if details,
      do: "#{policy |> to_string() |> escape()}<br><span class=\"c-muted\">#{details}</span>",
      else: policy |> to_string() |> escape()
  end

  defp schedule_catchup_summary(_schedule), do: "-"

  defp schedule_end_summary(schedule) do
    reason = Map.get(schedule, :end_reason)
    max_fires = Map.get(schedule, :max_fires)
    end_at_ms = Map.get(schedule, :end_at_ms)

    parts =
      []
      |> maybe_part("reason", reason)
      |> maybe_part("max", max_fires)
      |> maybe_part("until", end_at_ms)

    if parts == [], do: "-", else: Enum.join(parts, "<br>")
  end

  defp maybe_part(parts, _label, nil), do: parts

  defp maybe_part(parts, label, value),
    do: ["#{escape(label)} #{value |> to_string() |> escape()}" | parts]

  defp schedule_state_class("failed"), do: "flow-pill flow-pill-failed"
  defp schedule_state_class("completed"), do: "flow-pill flow-pill-terminal"
  defp schedule_state_class("cancelled"), do: "flow-pill flow-pill-terminal"
  defp schedule_state_class("paused"), do: "flow-pill flow-pill-scheduled"
  defp schedule_state_class(_state), do: "flow-pill flow-pill-active"
end
