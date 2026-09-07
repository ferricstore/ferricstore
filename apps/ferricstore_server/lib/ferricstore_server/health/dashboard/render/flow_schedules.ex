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

  def render_flow_schedule_create_form(data \\ %{}) do
    filters = Map.get(data, :filters, %{})
    draft = Map.get(data, :draft, %{})
    kind = Map.get(draft, "schedule_kind", "cron")
    overlap = Map.get(draft, "overlap_policy", "skip")
    open = if Map.has_key?(data, :draft), do: " open", else: ""

    """
    <details class="flow-policy-panel" style="margin-bottom: 24px;" id="flow-schedule-create-panel"#{open}>
      <summary style="cursor: pointer; font-weight: 600; color: #818cf8; list-style: none; display: flex; align-items: center; justify-content: space-between; user-select: none;">
        <span style="display: inline-flex; align-items: center; gap: 8px; font-size: 0.95rem;">
          <span style="font-size: 1.1rem;">⏱️</span> Create Durable Schedule
        </span>
        <span style="font-size: 0.8rem; color: #94a3b8; font-weight: 400;">(click to expand)</span>
      </summary>

      <form class="flow-policy-form" action="/dashboard/flow/schedules" method="post" style="margin-top: 16px;" data-dashboard-single-submit>
        <input type="hidden" name="action" value="create">
        #{schedule_filter_inputs(filters)}

        <div class="flow-policy-grid" style="grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));">
          <label class="flow-policy-field">
            <span>Schedule ID *</span>
            <input class="flow-search-input mono" type="text" name="id" value="#{draft_attr(draft, "id")}" placeholder="e.g. daily_stripe_sync" required autocomplete="off">
          </label>

          <label class="flow-policy-field">
            <span>Schedule Kind *</span>
            <select class="flow-search-input mono" name="schedule_kind" id="schedule-create-kind">
              #{create_options(kind, [{"cron", "Cron Expression"}, {"interval", "Interval (every_ms)"}, {"delay", "One-Shot Delay (delay_ms)"}])}
            </select>
          </label>

          <label class="flow-policy-field" id="schedule-field-cron"#{timing_visibility(kind, "cron")}>
            <span>Cron Expression *</span>
            <input class="flow-search-input mono" type="text" name="cron" value="#{draft_attr(draft, "cron")}"#{timing_attributes(kind, "cron")} placeholder="0 9 * * * (standard 5-field cron)" autocomplete="off">
          </label>

          <label class="flow-policy-field" id="schedule-field-interval"#{timing_visibility(kind, "interval")}>
            <span>Interval (every_ms) *</span>
            <input class="flow-search-input mono" type="number" name="every_ms" value="#{draft_attr(draft, "every_ms")}"#{timing_attributes(kind, "interval")} min="1" placeholder="e.g. 60000 (1 minute)" autocomplete="off">
          </label>

          <label class="flow-policy-field" id="schedule-field-delay"#{timing_visibility(kind, "delay")}>
            <span>Delay (delay_ms) *</span>
            <input class="flow-search-input mono" type="number" name="delay_ms" value="#{draft_attr(draft, "delay_ms")}"#{timing_attributes(kind, "delay")} min="0" placeholder="e.g. 300000 (5 minutes)" autocomplete="off">
          </label>

          <label class="flow-policy-field">
            <span>Target Workflow Type *</span>
            <input class="flow-search-input mono" type="text" name="target_type" value="#{draft_attr(draft, "target_type")}" placeholder="e.g. stripe_sync" required autocomplete="off">
          </label>

          <label class="flow-policy-field">
            <span>Target Partition Key</span>
            <input class="flow-search-input mono" type="text" name="target_partition" value="#{draft_attr(draft, "target_partition")}" placeholder="e.g. tenant-stripe (optional)" autocomplete="off">
          </label>

          <label class="flow-policy-field">
            <span>Overlap Policy</span>
            <select class="flow-search-input mono" name="overlap_policy"#{disabled_unless(kind in ["cron", "interval"])}>
              #{create_options(overlap, [{"skip", "skip (skip tick if prior fire is running)"}, {"allow", "allow (fire even if a prior flow is running)"}, {"queue_after_previous", "queue_after_previous (wait for the prior flow)"}, {"fail_schedule", "fail_schedule (stop after an overlap)"}])}
            </select>
          </label>

          <label class="flow-policy-field">
            <span>Timezone</span>
            <input class="flow-search-input mono" type="text" name="timezone" value="#{draft_attr(draft, "timezone", "Etc/UTC")}"#{disabled_unless(kind == "cron")} placeholder="Etc/UTC" autocomplete="off">
          </label>

          <label class="flow-policy-field">
            <span>Max Fires</span>
            <input class="flow-search-input mono" type="number" name="max_fires" value="#{draft_attr(draft, "max_fires")}"#{disabled_unless(kind in ["cron", "interval"])} min="1" placeholder="unlimited" autocomplete="off">
          </label>
        </div>

        <div style="display: grid; gap: 6px; margin-top: 10px;">
          <label class="flow-policy-field">
            <span>Target Payload (JSON, optional)</span>
            <textarea class="flow-search-input mono" name="target_payload" rows="2" placeholder='{"source": "scheduler", "region": "us-east"}'>#{escape(Map.get(draft, "target_payload", ""))}</textarea>
          </label>
        </div>

        <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 14px; flex-wrap: wrap; gap: 12px;">
          <label class="flow-check-label" style="font-size: 0.82rem;">
            <input type="checkbox" name="overwrite" value="true"#{if Map.get(draft, "overwrite") == "true", do: " checked", else: ""}> Overwrite existing schedule if ID exists
          </label>
          <button class="flow-search-button" type="submit">➕ Create Schedule</button>
        </div>
      </form>

      #{FerricstoreServer.Health.Dashboard.Render.FlowFormScripts.schedule_script()}
    </details>
    """
  end

  defp draft_attr(draft, field, default \\ ""),
    do: draft |> Map.get(field, default) |> escape_attr()

  defp timing_attributes(kind, kind), do: " required"
  defp timing_attributes(_kind, _field), do: " disabled"
  defp timing_visibility(kind, kind), do: ""
  defp timing_visibility(_kind, _field), do: ~s( style="display: none;")
  defp disabled_unless(true), do: ""
  defp disabled_unless(false), do: " disabled"

  defp create_options(selected, options) do
    options =
      if List.keymember?(options, selected, 0),
        do: options,
        else: [{selected, "Invalid selection: #{selected}"} | options]

    Enum.map_join(options, "\n", fn {value, label} ->
      selected_attr = if value == selected, do: " selected", else: ""
      ~s(<option value="#{escape_attr(value)}"#{selected_attr}>#{escape(label)}</option>)
    end)
  end

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
    <div class="table-scroll" role="region" aria-label="Workflow schedules" tabindex="0"><table>
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
    </table></div>
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
    <div class="table-scroll" role="region" aria-label="Failed workflow schedules" tabindex="0"><table>
      <thead><tr><th>ID</th><th>Reason</th><th>At</th><th>Previous Target</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table></div>
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
    state = Map.get(schedule, :state)

    [
      if(state == "active", do: schedule_confirmation(schedule, "fire", "Fire", filters)),
      if(state == "active", do: schedule_action_button(schedule, "pause", "Pause", filters)),
      if(state == "paused", do: schedule_action_button(schedule, "resume", "Resume", filters)),
      if(state in ["active", "paused", "failed"],
        do: schedule_confirmation(schedule, "delete", "Delete", filters, true)
      )
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
  end

  defp schedule_action_button(schedule, action, label, filters, danger? \\ false) do
    id = Map.get(schedule, :id, "")
    state = Map.get(schedule, :state, "")
    version = Map.get(schedule, :version, "")
    class = if danger?, do: "flow-search-button flow-danger-button", else: "flow-search-button"

    """
    <form style="display:inline" action="/dashboard/flow/schedules" method="post" data-dashboard-single-submit>
      <input type="hidden" name="id" value="#{escape_attr(id)}">
      <input type="hidden" name="action" value="#{escape_attr(action)}">
      <input type="hidden" name="expected_state" value="#{escape_attr(state)}">
      <input type="hidden" name="expected_version" value="#{version |> to_string() |> escape_attr()}">
      #{schedule_filter_inputs(filters)}
      <button class="#{class}" type="submit">#{escape(label)}</button>
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
