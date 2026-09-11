defmodule FerricstoreServer.Health.Dashboard.Render.FlowSchedules do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.FlowOverview, only: [render_flow_stat_card: 3]
  alias FerricstoreServer.Health.Dashboard.Flow.ScheduleEditor
  alias FerricstoreServer.Health.Dashboard.Render.DurationInput

  @overlap_options [
    {"skip", "Skip", "Skip this tick while the previous target is active."},
    {"allow", "Allow", "Fire even while the previous target is active."},
    {"queue_after_previous", "Queue after previous",
     "Wait until the previous target is no longer active."},
    {"fail_schedule", "Fail schedule",
     "Stop the schedule if its previous target is still active."}
  ]

  @overlap_not_applicable "Overlap policy applies only to cron and interval schedules."

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
    do:
      ~s(<div class="flow-alert flow-alert-error" id="flow-schedule-create-error" role="alert" tabindex="-1">#{escape(message)}</div>)

  def render_flow_schedules_flash(_flash), do: ""

  def render_flow_schedule_create_form(data \\ %{})

  def render_flow_schedule_create_form(%{action_capabilities: %{create: false}}),
    do:
      ~s(<p class="flow-section-note">Schedule creation requires +FLOW.SCHEDULE.LIST, +FLOW.SCHEDULE.CREATE and global write access.</p>)

  def render_flow_schedule_create_form(data) do
    filters = Map.get(data, :filters, %{})
    draft = Map.get(data, :draft, %{})
    kind = Map.get(draft, "schedule_kind", "cron")
    overlap = Map.get(draft, "overlap_policy", "skip")
    open = if Map.has_key?(data, :draft), do: " open", else: ""
    review = Map.get(data, :review)
    errors = Map.get(data, :field_errors, %{})
    editing? = ScheduleEditor.editing?(draft)
    dirty? = Map.has_key?(data, :draft) and not Map.get(data, :hydrated_edit, false)

    """
    <details class="dashboard-disclosure" id="flow-schedule-create-panel"#{open}>
      <summary>#{if editing?, do: "Edit schedule: #{escape(Map.get(draft, "id", ""))}", else: "Create durable schedule"}</summary>

      <form class="flow-policy-form" action="/dashboard/flow/schedules" method="post" style="margin-top: 16px;" data-dashboard-single-submit data-schedule-draft="#{dirty?}">
        <input type="hidden" name="action" value="create">
        #{schedule_filter_inputs(filters)}
        #{if editing?, do: Enum.map_join(~w(editing original_state original_version), fn field -> ~s(<input type="hidden" name="#{field}" value="#{draft_attr(draft, field)}">) end), else: ""}

        <fieldset class="flow-management-group"><legend>Identity</legend>
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Schedule ID *</span>
            <input class="flow-search-input mono" type="text" name="id" value="#{draft_attr(draft, "id")}" placeholder="e.g. daily_stripe_sync" required autocomplete="off"#{if editing?, do: " readonly", else: ""}#{field_attributes(errors, "id")}>
            #{field_error(errors, "id")}
          </label>
        </div></fieldset>
        <fieldset class="flow-management-group"><legend>Timing</legend>
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Schedule Kind *</span>
            <select class="flow-search-input mono" name="schedule_kind" id="schedule-create-kind"#{field_attributes(errors, "schedule_kind")}>
              #{create_options(kind, [{"cron", "Cron expression"}, {"interval", "Interval"}, {"delay", "One-shot delay"}, {"one_shot", "One-shot at UTC time"}])}
            </select>
            #{field_error(errors, "schedule_kind")}
          </label>

          <label class="flow-policy-field" id="schedule-field-cron"#{timing_visibility(kind, "cron")}>
            <span>Cron Expression *</span>
            <input class="flow-search-input mono" type="text" name="cron" value="#{draft_attr(draft, "cron")}"#{timing_attributes(kind, "cron")} placeholder="0 9 * * * (standard 5-field cron)" autocomplete="off"#{field_attributes(errors, "cron")}>
            #{field_error(errors, "cron")}
          </label>

          <div class="flow-policy-field" id="schedule-field-interval"#{timing_visibility(kind, "interval")}>
            <span id="schedule-interval-label">Interval *</span>
            <div class="flow-duration-control">
            <input class="flow-search-input mono" type="text" inputmode="decimal" name="every_ms" value="#{draft_attr(draft, "every_ms")}"#{timing_attributes(kind, "interval")} data-duration-min="1" autocomplete="off" aria-labelledby="schedule-interval-label"#{field_attributes(errors, "every_ms")}>
            #{DurationInput.units("every_ms", Map.get(draft, "every_ms_unit", "milliseconds"), kind != "interval")}
            </div>
            #{field_error(errors, "every_ms")}
          </div>

          <div class="flow-policy-field" id="schedule-field-delay"#{timing_visibility(kind, "delay")}>
            <span id="schedule-delay-label">Delay *</span>
            <div class="flow-duration-control">
            <input class="flow-search-input mono" type="text" inputmode="decimal" name="delay_ms" value="#{draft_attr(draft, "delay_ms")}"#{timing_attributes(kind, "delay")} data-duration-min="0" autocomplete="off" aria-labelledby="schedule-delay-label"#{field_attributes(errors, "delay_ms")}>
            #{DurationInput.units("delay_ms", Map.get(draft, "delay_ms_unit", "milliseconds"), kind != "delay")}
            </div>
            #{field_error(errors, "delay_ms")}
          </div>

          <label class="flow-policy-field" id="schedule-field-one_shot"#{timing_visibility(kind, "one_shot")}>
            <span>Run at (UTC) *</span>
            <input class="flow-search-input mono" type="datetime-local" step="0.001" name="at_utc" value="#{draft_attr(draft, "at_utc")}"#{timing_attributes(kind, "one_shot")} min="1970-01-01T00:00"#{field_attributes(errors, "at_utc")}>
            #{field_error(errors, "at_utc")}
          </label>

          <label class="flow-policy-field" data-schedule-bound#{if kind in ["cron", "interval"], do: "", else: " hidden"}>
            <span>Start at (UTC, optional)</span>
            <input class="flow-search-input mono" type="datetime-local" step="0.001" name="start_at_utc" value="#{draft_attr(draft, "start_at_utc")}"#{disabled_unless(kind in ["cron", "interval"])} min="1970-01-01T00:00"#{field_attributes(errors, "start_at_utc")}>
            #{field_error(errors, "start_at_utc")}
          </label>

          <label class="flow-policy-field" data-schedule-bound#{if kind in ["cron", "interval"], do: "", else: " hidden"}>
            <span>End at (UTC, optional)</span>
            <input class="flow-search-input mono" type="datetime-local" step="0.001" name="end_at_utc" value="#{draft_attr(draft, "end_at_utc")}"#{disabled_unless(kind in ["cron", "interval"])} min="1970-01-01T00:00"#{field_attributes(errors, "end_at_utc")}>
            #{field_error(errors, "end_at_utc")}
          </label>
        </div></fieldset>
        <fieldset class="flow-management-group"><legend>Target</legend>
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Target Workflow Type *</span>
            <input class="flow-search-input mono" type="text" name="target_type" value="#{draft_attr(draft, "target_type")}" placeholder="e.g. stripe_sync" required autocomplete="off"#{field_attributes(errors, "target_type")}>
            #{field_error(errors, "target_type")}
          </label>

          <label class="flow-policy-field">
            <span>Target Partition Key</span>
            <input class="flow-search-input mono" type="text" name="target_partition" value="#{draft_attr(draft, "target_partition")}" placeholder="e.g. tenant-stripe (optional)" autocomplete="off">
          </label>
        </div>
        <label class="flow-policy-field">
          <span>Target Payload (JSON, optional)</span>
          <textarea class="flow-search-input mono flow-schedule-payload" name="target_payload" rows="5" placeholder='{"source": "scheduler", "region": "us-east"}'#{field_attributes(errors, "target_payload")}>#{escape(Map.get(draft, "target_payload", ""))}</textarea>
          #{field_error(errors, "target_payload")}
        </label>
        </fieldset>
        <fieldset class="flow-management-group" data-schedule-recurrence#{if kind in ["cron", "interval"], do: "", else: " hidden"}><legend>Recurrence</legend>
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Overlap Policy</span>
            <select class="flow-search-input mono" name="overlap_policy"#{disabled_unless(kind in ["cron", "interval"])}#{field_attributes(errors, "overlap_policy", "schedule-overlap-description")}>
              #{overlap_options(overlap)}
            </select>
            <small class="flow-field-help" id="schedule-overlap-description" role="status">#{overlap_description(overlap, kind)}</small>
            #{field_error(errors, "overlap_policy")}
          </label>

          <label class="flow-policy-field" data-schedule-timezone#{if kind == "cron", do: "", else: " hidden"}>
            <span>Timezone</span>
            <input class="flow-search-input mono" type="text" name="timezone" value="#{draft_attr(draft, "timezone", "Etc/UTC")}"#{disabled_unless(kind == "cron")} placeholder="Etc/UTC" autocomplete="off">
          </label>

          <label class="flow-policy-field">
            <span>Max Fires</span>
            <input class="flow-search-input mono" type="number" name="max_fires" value="#{draft_attr(draft, "max_fires")}"#{disabled_unless(kind in ["cron", "interval"])} min="1" placeholder="unlimited" autocomplete="off"#{field_attributes(errors, "max_fires")}>
            #{field_error(errors, "max_fires")}
          </label>
        </div></fieldset>

        <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 14px; flex-wrap: wrap; gap: 12px;">
          #{if editing?, do: ~s|<input type="hidden" name="overwrite" value="true"><span class="flow-section-note">Editing version #{draft_attr(draft, "original_version")}. Replacement reactivates the schedule and resets its fire count.</span>|, else: ~s|<label class="flow-check-label"><input type="checkbox" name="overwrite" value="true"#{if Map.get(draft, "overwrite") in ["true", "on", "1"], do: " checked", else: ""}> Replace existing schedule (review required)</label>|}
          <button class="flow-search-button secondary" type="submit" name="preview" value="true" formnovalidate>Review schedule</button>
          <a class="flow-link" data-discard-draft href="#{escape_attr(schedule_discard_path(draft, filters))}">Discard changes</a>
        </div>
        <p class="flow-section-note" data-schedule-dirty-status role="status" hidden>Unsaved schedule changes</p>
        #{render_schedule_review(review)}
      </form>

      #{FerricstoreServer.Health.Dashboard.Render.FlowFormScripts.duration_script()}
      #{FerricstoreServer.Health.Dashboard.Render.FlowFormScripts.schedule_script()}
    </details>
    """
  end

  defp draft_attr(draft, field, default \\ ""),
    do: draft |> Map.get(field, default) |> escape_attr()

  defp schedule_discard_path(draft, filters) do
    params = Map.new(filters, fn {key, value} -> {to_string(key), value} end)

    params =
      if ScheduleEditor.editing?(draft),
        do: Map.merge(params, %{"id" => draft["id"], "edit" => "true"}),
        else: params

    "/dashboard/flow/schedules?" <>
      URI.encode_query(Enum.reject(params, fn {_key, value} -> value in [nil, ""] end)) <>
      "#flow-schedule-create-panel"
  end

  defp overlap_options(selected) do
    options =
      if List.keymember?(@overlap_options, selected, 0),
        do: @overlap_options,
        else: [{selected, "Invalid selection", "Select an overlap policy."} | @overlap_options]

    Enum.map_join(options, "\n", fn {value, label, description} ->
      ~s(<option value="#{escape_attr(value)}" data-overlap-description="#{escape_attr(description)}"#{if selected == value, do: " selected", else: ""}>#{label}</option>)
    end)
  end

  defp overlap_description(selected, kind) when kind in ["cron", "interval"] do
    case List.keyfind(@overlap_options, selected, 0) do
      {_value, _label, description} -> description
      nil -> "Select an overlap policy."
    end
  end

  defp overlap_description(_selected, _kind), do: @overlap_not_applicable

  defp field_attributes(errors, name, help_id \\ nil) do
    invalid = if Map.has_key?(errors, name), do: ~s( aria-invalid="true"), else: ""

    describedby =
      if help_id,
        do: "#{help_id} schedule-create-#{name}-error",
        else: "schedule-create-#{name}-error"

    ~s( aria-describedby="#{describedby}"#{invalid})
  end

  defp field_error(errors, name) do
    message = Map.get(errors, name, "")
    hidden = if message == "", do: " hidden", else: ""

    ~s(<small class="flow-field-error" id="schedule-create-#{name}-error" data-schedule-field-error#{hidden}>#{escape(message)}</small>)
  end

  defp render_schedule_review(nil), do: ""

  defp render_schedule_review(%{current: current, planned: planned, fields: fields}) do
    hidden =
      Enum.map_join(fields, fn {name, value} ->
        ~s(<input type="hidden" name="#{name}" value="#{escape_attr(value)}" data-schedule-review-field>)
      end)

    impact =
      cond do
        is_nil(current) ->
          "Creates a new active schedule."

        current.state == "paused" ->
          "Paused schedule will become active. Fire count resets to 0."

        true ->
          "Replaces the current definition and sets the schedule active. Fire count resets to 0."
      end

    confirmation =
      if current do
        ~s(<label class="flow-check-label"><input type="checkbox" name="confirm_replace" value="true" required> Confirm replacement of #{escape(planned.id)} at version #{current.version}, including reactivation and fire-count reset.</label>)
      else
        ""
      end

    """
    <section class="flow-schedule-review" data-schedule-review aria-label="Schedule review">
      <h3>Schedule review: #{escape(planned.id)}</h3>
      <p>#{impact}</p>
      <div class="flow-policy-grid">
        #{if current, do: schedule_review_definition("Current definition", current), else: ""}
        #{schedule_review_definition(if(current, do: "Replacement definition", else: "New definition"), planned)}
      </div>
      #{hidden}
      #{confirmation}
      <button class="flow-search-button" type="submit" data-schedule-confirm>#{if current, do: "Confirm replacement", else: "Create schedule"}</button>
      <p class="flow-field-help">Review valid for 5 minutes. Any definition edit requires another review.</p>
    </section>
    <p class="flow-field-help" data-schedule-review-stale hidden>Schedule draft changed. Review the schedule again.</p>
    """
  end

  defp schedule_review_definition(title, schedule) do
    target = Map.get(schedule, :target, %{})

    timing =
      case Map.get(schedule, :kind) do
        :cron ->
          Map.get(schedule, :cron)

        :interval ->
          "Every #{format_duration_ms(Map.get(schedule, :every_ms, 0))} (#{Map.get(schedule, :every_ms)} ms)"

        :delay ->
          "After #{format_duration_ms(Map.get(schedule, :delay_ms, 0))}"

        _ ->
          "One shot"
      end

    fields = [
      {"State / version",
       "#{Map.get(schedule, :state)} / #{Map.get(schedule, :version) || "new"}"},
      {"Timing", timing},
      {"Timezone", Map.get(schedule, :timezone) || "UTC"},
      {"Next fire (UTC)", format_timestamp_ms_or_dash(Map.get(schedule, :next_run_at_ms))},
      {"Next fire (selected timezone)", schedule_local_time(schedule)},
      {"Start bound (UTC)", format_timestamp_ms_or_dash(Map.get(schedule, :start_at_ms))},
      {"End bound (UTC)", format_timestamp_ms_or_dash(Map.get(schedule, :end_at_ms))},
      {"Target type", Map.get(target, :type, "-")},
      {"Target partition", Map.get(target, :partition_key) || "automatic"},
      {"Overlap policy", to_string(Map.get(schedule, :overlap_policy) || "not applicable")},
      {"Max fires", to_string(Map.get(schedule, :max_fires) || "unlimited")},
      {"Fire count", to_string(Map.get(schedule, :fire_count, 0))}
    ]

    entries =
      Enum.map_join(fields, fn {label, value} ->
        ~s(<dt>#{label}</dt><dd>#{escape(value || "-")}</dd>)
      end)

    definition =
      Map.take(schedule, [
        :id,
        :kind,
        :cron,
        :timezone,
        :every_ms,
        :delay_ms,
        :start_at_ms,
        :end_at_ms,
        :max_fires,
        :overlap_policy,
        :overlap_retry_ms,
        :catchup_policy,
        :target
      ])

    metadata =
      case Jason.encode(definition, pretty: true) do
        {:ok, json} -> json
        {:error, _} -> inspect(definition, limit: :infinity, printable_limit: :infinity)
      end

    ~s(<div><h4>#{title}</h4><dl>#{entries}</dl><details><summary>Definition metadata and target values</summary><pre>#{escape(metadata)}</pre></details></div>)
  end

  defp schedule_local_time(schedule) do
    timezone = Map.get(schedule, :timezone) || "Etc/UTC"

    with ms when is_integer(ms) <- Map.get(schedule, :next_run_at_ms),
         {:ok, date} <- DateTime.from_unix(ms, :millisecond),
         {:ok, shifted} <- DateTime.shift_zone(date, timezone, Tz.TimeZoneDatabase) do
      DateTime.to_iso8601(shifted) <> " " <> timezone
    else
      _ -> "-"
    end
  end

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
      <form class="flow-search" action="/dashboard/flow/schedules" method="get" aria-label="Exact schedule lookup">
        <label class="flow-policy-field"><span>Exact schedule ID</span><input class="flow-search-input mono" type="text" name="id" value="#{escape_attr(Map.get(filters, :id) || "")}" required></label>
        <button class="flow-search-button" type="submit">Open schedule</button>
        <a class="flow-link" href="/dashboard/flow/schedules">Browse schedules</a>
      </form>
      <form class="flow-search" action="/dashboard/flow/schedules" method="get" aria-label="Schedule filters">
        <label class="flow-policy-field"><span>ID contains</span><input class="flow-search-input mono" type="search" name="q" aria-label="Schedule ID contains" value="#{escape_attr(Map.get(filters, :q) || "")}" title="Filter the bounded catalog result by schedule id substring"></label>
        <label class="flow-policy-field"><span>State</span>#{schedule_select("state", Map.get(filters, :state, :all), ["all", "active", "paused", "running", "failed", "completed", "cancelled"])}</label>
        <label class="flow-policy-field"><span>Kind</span>#{schedule_select("kind", Map.get(filters, :kind), ["", "one_shot", "delay", "interval", "cron"])}</label>
        <label class="flow-policy-field"><span>Limit</span><input class="flow-search-input mono" type="number" min="1" max="500" name="limit" aria-label="Schedule limit" value="#{Map.get(filters, :limit, 100)}" title="Maximum schedules to show"></label>
        <button class="flow-search-button" type="submit">Filter</button>
      </form>
      <div class="flow-filter-note">State and kind are applied during the durable catalog scan. ID contains searches up to 500 retained schedules; Limit controls matching rows displayed. Exact ID lookup is independent of this sample.</div>
      #{if Map.get(data, :scan_limited?, false), do: ~s(<p class="flow-alert flow-alert-warning">Catalog sample limit reached. Use exact ID lookup for schedules outside this sample.</p>), else: ""}
    </div>
    """
  end

  def render_flow_schedules_table(schedules) when is_list(schedules),
    do: render_flow_schedules_table(schedules, %{})

  def render_flow_schedules_table(schedules, filters)
      when is_list(schedules) and is_map(filters) do
    rows =
      if schedules == [] do
        ~s(<tr><td colspan="12" class="c-muted">No schedules matched in this retained catalog sample. Use exact ID lookup to check a known schedule.</td></tr>)
      else
        Enum.map_join(schedules, "\n", &render_flow_schedule_row(&1, filters))
      end

    """
    <h2 class="section-title">Schedules</h2>
    <div class="table-scroll" role="region" aria-label="Workflow schedules" tabindex="0"><table class="flow-schedules-table">
      <colgroup><col style="width: 180px"><col style="width: 100px"><col style="width: 100px"><col style="width: 140px"><col style="width: 140px"><col style="width: 60px"><col style="width: 140px"><col style="width: 120px"><col style="width: 100px"><col style="width: 120px"><col style="width: 180px"><col style="width: 170px"></colgroup>
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
    <h2 class="section-title">Failed Schedules</h2>
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
      <td class="mono">#{last_target_link(schedule)}</td>
      <td>#{render_flow_schedule_actions(schedule, filters)}</td>
    </tr>
    <tr class="flow-schedule-definition-row">
      <td colspan="12">#{schedule_definition(schedule)}</td>
    </tr>
    """
  end

  defp schedule_definition(schedule) do
    target = Map.get(schedule, :target) || %{}

    timing = [
      {"Kind", schedule_kind(schedule)},
      {"Cron expression", Map.get(schedule, :cron)},
      {"Interval (ms)", Map.get(schedule, :every_ms)},
      {"Delay (ms)", Map.get(schedule, :delay_ms)},
      {"Timezone", Map.get(schedule, :timezone)},
      {"Initial run (UTC)", format_timestamp_ms_or_dash(Map.get(schedule, :initial_run_at_ms))},
      {"Start bound (UTC)", format_timestamp_ms_or_dash(Map.get(schedule, :start_at_ms))},
      {"End bound (UTC)", format_timestamp_ms_or_dash(Map.get(schedule, :end_at_ms))},
      {"Target type", Map.get(target, :type)},
      {"Target partition", Map.get(target, :partition_key) || "Unpartitioned"}
    ]

    rows =
      timing
      |> Enum.reject(fn {_label, value} -> is_nil(value) end)
      |> Enum.map_join("", fn {label, value} ->
        "<div><dt>#{escape(label)}</dt><dd>#{escape(to_string(value))}</dd></div>"
      end)

    ~s(<details class="dashboard-disclosure"><summary aria-label="Schedule definition for #{escape_attr(Map.get(schedule, :id, ""))}">Schedule definition</summary><dl>#{rows}</dl><p class="flow-section-note">Target payloads are not displayed. This view does not fetch target workflow payloads. Missing original timing is shown as a dash.</p></details>)
  end

  defp last_target_link(schedule) do
    case Map.get(schedule, :last_target_id) do
      id when is_binary(id) and id != "" ->
        target = Map.get(schedule, :target) || %{}

        path =
          FerricstoreServer.Health.Endpoint.FlowPaths.flow_detail_location(
            id,
            Map.get(target, :partition_key)
          )

        ~s(<a class="flow-link" href="#{escape_attr(path)}">#{escape(id)}</a>)

      _ ->
        "-"
    end
  end

  defp render_flow_schedule_actions(schedule, filters) do
    state = Map.get(schedule, :state)

    caps =
      Map.get(schedule, :action_capabilities, %{
        fire: true,
        pause: true,
        resume: true,
        delete: true
      })

    [
      if(Map.get(caps, :edit, Map.get(caps, :create, false)),
        do:
          ~s(<a class="flow-search-button" href="#{escape_attr(schedule_edit_path(schedule, filters))}">Edit</a>)
      ),
      if(state == "active" and caps.fire,
        do: schedule_confirmation(schedule, "fire", "Fire", filters)
      ),
      if(state == "active" and caps.pause,
        do: schedule_action_button(schedule, "pause", "Pause", filters)
      ),
      if(state == "paused" and caps.resume,
        do: schedule_action_button(schedule, "resume", "Resume", filters)
      ),
      if(state in ["active", "paused", "failed"] and caps.delete,
        do: schedule_confirmation(schedule, "delete", "Delete", filters, true)
      )
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
  end

  defp schedule_edit_path(schedule, filters) do
    params =
      filters
      |> Map.take([:state, :kind, :q, :limit])
      |> Map.put(:id, schedule.id)
      |> Map.put(:edit, "true")

    "/dashboard/flow/schedules?" <>
      URI.encode_query(Enum.reject(params, fn {_key, value} -> value in [nil, ""] end)) <>
      "#flow-schedule-create-panel"
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
      {"return_id", Map.get(filters, :id)},
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
      |> maybe_part(
        "until",
        if(is_integer(end_at_ms), do: format_timestamp_ms_or_dash(end_at_ms))
      )

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
