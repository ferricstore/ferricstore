defmodule FerricstoreServer.Health.Dashboard.Render.FlowSchedules do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.FlowOverview, only: [render_flow_stat_card: 3]

  def render_flow_schedules_summary(data) do
    summary = Map.get(data, :summary, %{})

    """
    <div class="flow-card-grid">
      #{render_flow_stat_card("Schedules", Map.get(summary, :total, 0), "sampled durable schedules")}
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
    <form class="flow-search" action="/dashboard/flow/schedules" method="get" aria-label="Schedule filters">
      <input class="flow-search-input mono" type="search" name="q" value="#{escape_attr(Map.get(filters, :q) || "")}" placeholder="schedule id contains..." title="Filter schedules by id substring">
      #{schedule_select("state", Map.get(filters, :state, :all), ["all", "active", "paused", "failed", "completed", "cancelled"])}
      #{schedule_select("kind", Map.get(filters, :kind), ["", "one_shot", "delay", "interval", "cron"])}
      <input class="flow-search-input mono" type="number" min="1" max="500" name="limit" value="#{Map.get(filters, :limit, 100)}" title="Maximum schedules to show">
      <button class="flow-search-button" type="submit">Filter</button>
    </form>
    """
  end

  def render_flow_schedules_table(schedules) when is_list(schedules) do
    rows =
      if schedules == [] do
        ~s(<tr><td colspan="11" class="c-muted">No schedules matched the current filters.</td></tr>)
      else
        Enum.map_join(schedules, "\n", &render_flow_schedule_row/1)
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

  defp render_flow_schedule_row(%{error: reason}) do
    ~s(<tr><td colspan="11" class="flow-alert-error">#{escape(reason)}</td></tr>)
  end

  defp render_flow_schedule_row(schedule) do
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
      <td>#{schedule_end_summary(schedule)}</td>
      <td class="mono">#{escape(Map.get(schedule, :last_target_id, "-") || "-")}</td>
      <td>#{render_flow_schedule_actions(schedule)}</td>
    </tr>
    """
  end

  defp render_flow_schedule_actions(schedule) do
    id = Map.get(schedule, :id, "")
    state = Map.get(schedule, :state)

    [
      if(state == "active", do: schedule_action_button(id, "fire", "Fire")),
      if(state == "active", do: schedule_action_button(id, "pause", "Pause")),
      if(state == "paused", do: schedule_action_button(id, "resume", "Resume")),
      if(state in ["active", "paused", "failed"],
        do: schedule_action_button(id, "delete", "Delete", true)
      )
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
  end

  defp schedule_action_button(id, action, label, danger? \\ false) do
    class = if danger?, do: "flow-search-button flow-danger-button", else: "flow-search-button"

    """
    <form style="display:inline" action="/dashboard/flow/schedules" method="post">
      <input type="hidden" name="id" value="#{escape_attr(id)}">
      <button class="#{class}" type="submit" name="action" value="#{escape_attr(action)}">#{escape(label)}</button>
    </form>
    """
  end

  defp schedule_select(name, selected, values) do
    options =
      Enum.map_join(values, "\n", fn value ->
        label = if value == "", do: "any kind", else: value
        selected_attr = if to_string(selected || "") == value, do: " selected", else: ""

        ~s(<option value="#{escape_attr(value)}"#{selected_attr}>#{escape(label)}</option>)
      end)

    ~s(<select class="flow-search-input mono" name="#{escape_attr(name)}">#{options}</select>)
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
