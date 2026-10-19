defmodule FerricstoreServer.Health.Dashboard.Render.DoctorPages do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.DoctorSupport

  def render_doctor_flash(%{status: status, message: message})
      when status in ["ok", "error"] and is_binary(message) and message != "" do
    klass = if status == "ok", do: "c-green", else: "c-red"

    """
    <div class="flow-card flow-card-wide" role="status">
      <div class="flow-card-label">Doctor action</div>
      <div class="flow-card-value #{klass}" style="font-size:1rem;">#{escape(message)}</div>
    </div>
    """
  end

  def render_doctor_flash(_flash), do: ""

  def render_doctor_summary(%{"error" => _} = check),
    do: render_doctor_collection_error("Doctor CHECK unavailable", check)

  def render_doctor_summary(check) do
    checks = Map.get(check, "checks", [])
    total = length(checks)
    failed = Enum.count(checks, &(Map.get(&1, "status") == "error"))
    warnings = Enum.count(checks, &(Map.get(&1, "status") == "warning"))
    took = "Checked in #{format_duration_ms(Map.get(check, "duration_ms", 0))}."

    {state, title, detail} =
      cond do
        total == 0 ->
          {"warning", "No checks returned", "The doctor answered without any check results."}

        failed > 0 ->
          {"degraded", "#{failed} of #{total} checks failed", took}

        warnings > 0 or Map.get(check, "status") != "ok" ->
          {"warning",
           "#{max(warnings, 1)} of #{total} checks #{if max(warnings, 1) == 1, do: "needs", else: "need"} attention",
           took}

        true ->
          {"healthy", "All #{total} checks pass", took}
      end

    """
    <section class="doctor-status is-#{state}" aria-label="Doctor result">
      <strong>#{escape(title)}</strong>
      <span>#{escape(detail)}</span>
    </section>
    """
  end

  @check_names %{
    "bitcask" => "Bitcask storage",
    "blob_refs" => "Blob references",
    "flow_lmdb" => "Flow LMDB projection"
  }

  def render_doctor_checks(check) do
    rows =
      case Map.get(check, "checks", []) do
        [] ->
          message =
            if Map.has_key?(check, "error"),
              do: "Checks unavailable; retry diagnostics.",
              else: "No doctor checks returned"

          ~s(<tr><td colspan="4" class="c-muted">#{message}</td></tr>)

        checks ->
          Enum.map_join(checks, "\n", fn item ->
            scope = Map.get(item, "scope", "")
            status = Map.get(item, "status", "")

            """
            <tr>
              <td><strong>#{escape(Map.get(@check_names, scope, scope))}</strong><span class="mono">#{escape(scope)}</span></td>
              <td><span class="badge #{doctor_badge_class(status)}">#{escape(status)}</span></td>
              <td>#{escape(Map.get(item, "message", ""))}</td>
              <td class="mono">#{escape(doctor_metric_summary(Map.get(item, "scope"), Map.get(item, "metrics", %{})))}</td>
            </tr>
            """
          end)
      end

    """
    <h2 class="section-title">Checks</h2>
    <div class="table-scroll" role="region" aria-label="Doctor checks" tabindex="0"><table class="doctor-checks">
      <thead>
        <tr><th>Check</th><th>Status</th><th>Meaning</th><th>Key metrics</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table></div>
    """
  end

  defp doctor_badge_class("ok"), do: "badge-ok"
  defp doctor_badge_class("warning"), do: "badge-warning"
  defp doctor_badge_class("error"), do: "badge-failed"
  defp doctor_badge_class(_status), do: "badge-idle"

  def render_doctor_actions do
    """
    <h2 class="section-title">Actions</h2>
    <div class="flow-card-grid">
      <form class="flow-card flow-card-wide" action="/dashboard/doctor" method="post" data-dashboard-single-submit>
        <input type="hidden" name="action" value="start_check">
        <div class="flow-card-label">Start background check</div>
        <div class="flow-card-detail">Runs the selected doctor scope as a background job and keeps the result queryable by job id.</div>
        <label class="flow-field" for="doctor-scope"><span>Scope</span>
        <select class="flow-search-input" id="doctor-scope" name="scope">
          <option value="ALL">All</option>
          <option value="BITCASK">Bitcask / keydir</option>
          <option value="BLOB_REFS">Blob refs</option>
          <option value="FLOW_LMDB">Flow LMDB</option>
        </select>
        </label>
        <button type="submit" class="flow-search-button">Start check</button>
      </form>
      <div class="flow-card flow-card-wide">
        <div class="flow-card-label">Repair Flow projection</div>
        <div class="flow-card-detail">Starts FERRICSTORE.DOCTOR START REPAIR PROJECTIONS for the LMDB cold/query projection. Flow hot indexes stay on the normal apply path.</div>
        <details class="flow-action-confirm">
          <summary class="flow-search-button">Review repair</summary>
          <div class="flow-action-confirm-panel">
            <strong>Confirm projection reconciliation</strong>
            <span>This starts one bounded background repair job. An existing repair for this instance will not be duplicated.</span>
            <form action="/dashboard/doctor" method="post" data-dashboard-single-submit>
              <input type="hidden" name="action" value="repair_flow_lmdb">
              <input type="hidden" name="confirm_action" value="true">
              <input type="hidden" name="expected_action" value="repair_flow_lmdb">
              <button type="submit" class="flow-search-button">Confirm Flow LMDB repair</button>
            </form>
          </div>
        </details>
      </div>
    </div>
    """
  end

  def render_doctor_jobs(%{"error" => _} = result) do
    render_doctor_collection_error("Doctor LIST unavailable", result) <>
      case Map.get(result, "jobs", []) do
        [] -> ""
        jobs -> render_doctor_jobs(jobs)
      end
  end

  def render_doctor_jobs(%{} = result), do: render_doctor_jobs(Map.get(result, "jobs", []))

  def render_doctor_jobs(jobs) do
    rows =
      case jobs do
        [] ->
          ~s(<tr><td colspan="6" class="c-muted">No doctor jobs yet</td></tr>)

        _ ->
          Enum.map_join(jobs, "\n", fn job ->
            cancel =
              if Map.get(job, "status") == "running" do
                """
                <details class="flow-action-confirm">
                  <summary class="flow-link-button">Cancel</summary>
                  <div class="flow-action-confirm-panel">
                    <strong>Cancel running job</strong>
                    <span class="mono">#{escape(Map.get(job, "job_id", ""))}</span>
                    <form action="/dashboard/doctor" method="post" data-dashboard-single-submit>
                      <input type="hidden" name="action" value="cancel">
                      <input type="hidden" name="job_id" value="#{escape_attr(Map.get(job, "job_id", ""))}">
                      <input type="hidden" name="expected_status" value="running">
                      <button type="submit" class="flow-danger-button">Confirm cancel</button>
                    </form>
                  </div>
                </details>
                """
              else
                ""
              end

            """
            <tr>
              <td class="mono">#{escape(Map.get(job, "job_id", ""))}</td>
              <td>#{escape(Map.get(job, "kind", ""))}</td>
              <td class="#{doctor_status_class(Map.get(job, "status", ""))}">#{escape(Map.get(job, "status", ""))}</td>
              <td>#{escape(Enum.join(Map.get(job, "scopes", []), ", "))}</td>
              <td>#{escape(doctor_job_result_summary(job))}</td>
              <td>#{cancel}</td>
            </tr>
            """
          end)
      end

    """
    <h2 class="section-title">Background jobs</h2>
    <div class="table-scroll" role="region" aria-label="Doctor jobs" tabindex="0">
    <table>
      <thead>
        <tr><th>Job</th><th>Kind</th><th>Status</th><th>Scopes</th><th>Result</th><th>Action</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    </div>
    """
  end

  defp render_doctor_collection_error(title, result) do
    """
    <div class="flow-alert flow-alert-error" role="status">
      <strong>#{escape(title)}</strong>
      <p>#{escape(Map.get(result, "error", "Diagnostics could not be collected."))}</p>
      <a class="flow-link" href="/dashboard/doctor">Retry diagnostics</a>
    </div>
    """
  end

  def render_doctor_command_reference(commands) do
    rows =
      Enum.map_join(commands, "\n", fn entry ->
        """
        <tr>
          <td class="mono">#{escape(entry.command)}</td>
          <td>#{escape(entry.purpose)}</td>
          <td>#{escape(entry.permission)}</td>
        </tr>
        """
      end)

    """
    <details class="dashboard-disclosure">
      <summary>Run from the CLI</summary>
      <div class="dashboard-disclosure-body">
        <p class="flow-section-note">The same checks and repairs over the native protocol.</p>
        <div class="table-scroll" role="region" aria-label="Doctor command reference" tabindex="0">
        <table>
          <thead><tr><th>Command</th><th>Purpose</th><th>Permission</th></tr></thead>
          <tbody>#{rows}</tbody>
        </table>
        </div>
      </div>
    </details>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Prefixes Sub-page
  # ---------------------------------------------------------------------------
end
