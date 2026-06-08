defmodule FerricstoreServer.Health.Dashboard.Render.DoctorPages do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Overview
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

  def render_doctor_summary(check) do
    status = Map.get(check, "status", "error")
    checks = Map.get(check, "checks", [])
    failed = Enum.count(checks, &(Map.get(&1, "status") == "error"))
    warnings = Enum.count(checks, &(Map.get(&1, "status") == "warning"))

    render_ops_summary("Doctor Summary", [
      %{
        label: "Status",
        value: status,
        class: doctor_status_class(status),
        detail: "aggregated check result"
      },
      %{label: "Checks", value: format_number(length(checks)), detail: "bounded metadata probes"},
      %{
        label: "Warnings",
        value: format_number(warnings),
        class: if(warnings > 0, do: "c-yellow", else: "c-green")
      },
      %{
        label: "Errors",
        value: format_number(failed),
        class: if(failed > 0, do: "c-red", else: "c-green")
      },
      %{
        label: "Duration",
        value: format_duration_ms(Map.get(check, "duration_ms", 0)),
        detail: "inline CHECK time"
      }
    ])
  end

  def render_doctor_checks(check) do
    rows =
      case Map.get(check, "checks", []) do
        [] ->
          ~s(<tr><td colspan="5" class="c-muted">No doctor checks returned</td></tr>)

        checks ->
          Enum.map_join(checks, "\n", fn item ->
            metrics = Map.get(item, "metrics", %{})

            """
            <tr>
              <td class="mono">#{escape(Map.get(item, "scope", ""))}</td>
              <td class="#{doctor_status_class(Map.get(item, "status", ""))}">#{escape(Map.get(item, "status", ""))}</td>
              <td>#{escape(Map.get(item, "message", ""))}</td>
              <td>#{escape(doctor_metric_summary(Map.get(item, "scope"), metrics))}</td>
              <td class="mono">FERRICSTORE.DOCTOR CHECK SCOPE #{escape(String.upcase(Map.get(item, "scope", "")))}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Checks</div>
    <table>
      <thead>
        <tr><th>Scope</th><th>Status</th><th>Meaning</th><th>Key metrics</th><th>Command</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  def render_doctor_actions do
    """
    <div class="section-title">Actions</div>
    <div class="flow-card-grid">
      <form class="flow-card flow-card-wide" action="/dashboard/doctor" method="post">
        <input type="hidden" name="action" value="start_check">
        <div class="flow-card-label">Start background check</div>
        <div class="flow-card-detail">Runs the selected doctor scope as a background job and keeps the result queryable by job id.</div>
        <label class="flow-form-label" for="doctor-scope">Scope</label>
        <select id="doctor-scope" name="scope">
          <option value="ALL">All</option>
          <option value="BITCASK">Bitcask / keydir</option>
          <option value="BLOB_REFS">Blob refs</option>
          <option value="FLOW_LMDB">Flow LMDB</option>
        </select>
        <button type="submit" class="flow-action-button">Start</button>
      </form>
      <form class="flow-card flow-card-wide" action="/dashboard/doctor" method="post">
        <input type="hidden" name="action" value="repair_flow_lmdb">
        <div class="flow-card-label">Repair Flow projection</div>
        <div class="flow-card-detail">Starts FERRICSTORE.DOCTOR START REPAIR PROJECTIONS for the LMDB cold/query projection. Flow hot indexes stay on the normal apply path.</div>
        <button type="submit" class="flow-action-button">Repair Flow LMDB</button>
      </form>
    </div>
    """
  end

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
                <form action="/dashboard/doctor" method="post" style="display:inline">
                  <input type="hidden" name="action" value="cancel">
                  <input type="hidden" name="job_id" value="#{escape_attr(Map.get(job, "job_id", ""))}">
                  <button type="submit" class="flow-link-button">Cancel</button>
                </form>
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
    <div class="section-title">Background Jobs</div>
    <table>
      <thead>
        <tr><th>Job</th><th>Kind</th><th>Status</th><th>Scopes</th><th>Result</th><th>Action</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
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
    <div class="section-title">Command Reference</div>
    <table>
      <thead><tr><th>Command</th><th>Purpose</th><th>Permission</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Prefixes Sub-page
  # ---------------------------------------------------------------------------
end
