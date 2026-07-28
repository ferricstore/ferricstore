defmodule FerricstoreServer.Health.Dashboard.Render.Security do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Overview, only: [render_ops_summary: 2]

  def render_acl_security(data) when is_map(data) do
    """
    #{render_acl_security_summary(data)}
    #{render_security_flash(Map.get(data, :flash))}
    #{render_account_management(data)}
    #{render_acl_users(data)}
    #{render_acl_tester(data)}
    #{render_dashboard_route_requirements(data)}
    """
  end

  def render_acl_security_summary(data) do
    protected_mode = Map.get(data, :protected_mode, false)
    configured_users = Map.get(data, :configured_users, false)
    current_user = Map.get(data, :current_user) || "open"

    render_ops_summary("ACL Security", [
      %{
        label: "Protected Mode",
        value: if(protected_mode, do: "on", else: "off"),
        class: if(protected_mode, do: "c-green", else: "c-yellow")
      },
      %{
        label: "Configured Users",
        value: if(configured_users, do: "yes", else: "no"),
        class: if(configured_users, do: "c-green", else: "c-yellow")
      },
      %{
        label: "ACL Users",
        value: format_number(Map.get(data, :acl_user_count, 0))
      },
      %{
        label: "Principal",
        value: current_user,
        detail: "dashboard request identity"
      },
      %{
        label: "Mutation Surface",
        value: if(Map.get(data, :can_manage_users, false), do: "enabled", else: "read only"),
        class: if(Map.get(data, :can_manage_users, false), do: "c-green", else: "c-muted"),
        detail: "derived from ACL.SETUSER"
      }
    ])
  end

  def render_security_flash(%{status: status, message: message})
      when status in ["ok", "error"] and is_binary(message) do
    class = if status == "ok", do: "acl-flash-ok", else: "acl-flash-error"
    role = if status == "error", do: "alert", else: "status"

    ~s(<div class="acl-flash #{class}" role="#{role}">#{escape(message)}</div>)
  end

  def render_security_flash(_flash), do: ""

  def render_account_management(data) do
    if Map.get(data, :can_manage_users, false) do
      """
      <section class="acl-management" aria-labelledby="acl-management-title">
        <div class="acl-management-heading">
          <div>
            <div class="section-title" id="acl-management-title">Account management</div>
            <p>Create a passworded ACL identity for dashboard or native access.</p>
          </div>
          <span class="badge badge-ok">ACL.SETUSER</span>
        </div>
        <form class="acl-create-form" action="/dashboard/security/users" method="post">
          <div class="acl-form-grid">
            <label>Username
              <input class="flow-search-input mono" type="text" name="username" maxlength="1024" autocomplete="username" required placeholder="operations-reader">
            </label>
            <label>Password
              <input class="flow-search-input" type="password" name="password" minlength="12" maxlength="4096" autocomplete="new-password" required>
            </label>
            <label>Confirm password
              <input class="flow-search-input" type="password" name="password_confirmation" minlength="12" maxlength="4096" autocomplete="new-password" required>
            </label>
          </div>
          <fieldset class="acl-role-selector">
            <legend>Access profile</legend>
            <label><input type="radio" name="role" value="admin" checked><span><strong>Administrator</strong><small>All commands, keys, and channels</small></span></label>
            <label><input type="radio" name="role" value="observer"><span><strong>Observer</strong><small>Read commands with scoped keys</small></span></label>
            <label><input type="radio" name="role" value="custom"><span><strong>Custom</strong><small>Explicit ACL modifiers</small></span></label>
          </fieldset>
          <div class="acl-form-grid acl-scope-grid">
            <label>Observer key pattern
              <input class="flow-search-input mono" type="text" name="key_pattern" maxlength="4096" value="*">
            </label>
            <label>Observer channel pattern
              <input class="flow-search-input mono" type="text" name="channel_pattern" maxlength="4096" value="*">
            </label>
          </div>
          <label class="acl-modifier-field">Custom ACL modifiers <span>one modifier per line</span>
            <textarea class="mono" name="modifiers" maxlength="6000" rows="4" placeholder="+GET&#10;%R~tenant-a:*&#10;&amp;tenant-a:*"></textarea>
          </label>
          <div class="acl-form-actions">
            <button class="flow-search-button acl-primary-button" type="submit">Create account</button>
          </div>
        </form>
      </section>
      """
    else
      """
      <section class="acl-readonly-panel" aria-label="Account management access">
        <div><strong>Read-only access</strong><span>Account mutations require <code>+ACL.SETUSER</code>.</span></div>
        <span class="badge badge-idle">ACL.LIST</span>
      </section>
      """
    end
  end

  def render_acl_tester(data) do
    tester = Map.get(data, :tester, %{})
    input = Map.get(tester, :input, %{})

    """
    <div class="section-title">ACL Tester</div>
    <div class="flow-filter-panel">
      <form class="flow-filter-form" action="/dashboard/security" method="get" aria-label="ACL tester">
        <label>User <input class="flow-search-input mono" type="search" name="user" value="#{escape_attr(Map.get(input, :user, ""))}" autocomplete="off" placeholder="default"></label>
        <label>Command <input class="flow-search-input mono" type="search" name="command" value="#{escape_attr(Map.get(input, :command, ""))}" autocomplete="off" placeholder="GET"></label>
        <label>Key <input class="flow-search-input mono" type="search" name="key" value="#{escape_attr(Map.get(input, :key, ""))}" autocomplete="off" placeholder="tenant:key"></label>
        <label>Key Access #{render_key_access_select(Map.get(input, :key_access, :read))}</label>
        <label>Channel <input class="flow-search-input mono" type="search" name="channel" value="#{escape_attr(Map.get(input, :channel, ""))}" autocomplete="off" placeholder="tenant:events"></label>
        <label>Route <input class="flow-search-input mono" type="search" name="route_path" value="#{escape_attr(Map.get(input, :route_path, ""))}" autocomplete="off" placeholder="/dashboard/flow"></label>
        <button class="flow-search-button" type="submit">Check</button>
      </form>
    </div>
    #{render_acl_test_results(tester)}
    """
  end

  defp render_key_access_select(selected) do
    read_selected = if selected == :read, do: " selected", else: ""
    write_selected = if selected == :write, do: " selected", else: ""

    """
    <select class="flow-search-input mono" name="key_access" title="Key access mode">
      <option value="read"#{read_selected}>read</option>
      <option value="write"#{write_selected}>write</option>
    </select>
    """
  end

  defp render_acl_test_results(tester) do
    rows =
      [:command, :key, :channel, :route]
      |> Enum.map_join("\n", fn kind ->
        result = Map.get(tester, kind, %{status: :idle, label: "Not checked", detail: ""})

        """
        <tr>
          <td>#{kind |> Atom.to_string() |> String.capitalize()}</td>
          <td>#{render_acl_status(result)}</td>
          <td class="mono">#{escape(Map.get(result, :detail, ""))}</td>
        </tr>
        """
      end)

    """
    #{table_scroll("ACL test results", """
    <table>
      <thead><tr><th>Check</th><th>Result</th><th>Detail</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table>
    """)}
    """
  end

  defp render_acl_status(%{status: :allowed, label: label}),
    do: ~s(<span class="badge badge-ok">#{escape(label)}</span>)

  defp render_acl_status(%{status: :denied, label: label}),
    do: ~s(<span class="badge badge-reject">#{escape(label)}</span>)

  defp render_acl_status(%{label: label}),
    do: ~s(<span class="badge badge-idle">#{escape(label)}</span>)

  def render_acl_users(data) do
    users = Map.get(data, :acl_users, [])
    current_user = Map.get(data, :current_user)
    can_manage_users = Map.get(data, :can_manage_users, false)
    can_delete_users = Map.get(data, :can_delete_users, false)

    rows =
      case users do
        [] ->
          ~s(<tr><td colspan="6" class="c-muted">No ACL users visible</td></tr>)

        _ ->
          Enum.map_join(users, "\n", fn user ->
            state = Map.get(user, :state, "unknown")
            state_class = if state == "on", do: "badge-ok", else: "badge-idle"
            password_configured = Map.get(user, :password_configured, false)
            auth_class = if password_configured, do: "badge-ok", else: "badge-warning"
            auth_label = if password_configured, do: "passworded", else: "no password"
            username = Map.get(user, :username, "")

            """
            <tr>
              <td><span class="mono">#{escape(username)}</span>#{current_badge(username, current_user)}</td>
              <td><span class="badge #{state_class}">#{escape(state)}</span></td>
              <td><span class="badge #{auth_class}">#{auth_label}</span></td>
              <td>#{escape(Map.get(user, :access, "Restricted"))}</td>
              <td><div class="mono acl-rule-summary">#{escape(Map.get(user, :rule, ""))}</div></td>
              <td>#{render_user_actions(user, current_user, can_manage_users, can_delete_users)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Accounts <span class="badge badge-idle">ACL.LIST</span></div>
    #{table_scroll("ACL account list", """
    <table>
      <thead><tr><th>User</th><th>State</th><th>Authentication</th><th>Access</th><th>Rule summary</th><th>Actions</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table>
    """)}
    """
  end

  defp current_badge(username, username),
    do: ~s( <span class="badge badge-merging">current</span>)

  defp current_badge(_username, _current_user), do: ""

  defp render_user_actions(_user, _current_user, false, _can_delete_users),
    do: ~s(<span class="c-muted">View only</span>)

  defp render_user_actions(user, current_user, true, can_delete_users) do
    username = Map.get(user, :username, "")
    state = Map.get(user, :state, "off")

    """
    <div class="acl-row-actions">
      #{render_state_action(username, state, current_user)}
      #{render_password_action(username)}
      #{render_modifier_action(username)}
      #{render_delete_action(username, current_user, can_delete_users)}
    </div>
    """
  end

  defp render_state_action("default", _state, _current_user),
    do: ~s(<span class="acl-action-note">recovery</span>)

  defp render_state_action(username, "on", username),
    do: ~s(<span class="acl-action-note">current</span>)

  defp render_state_action(username, state, _current_user) do
    enabled = state != "on"
    label = if enabled, do: "Enable", else: "Disable"

    """
    <form action="/dashboard/security/users/state" method="post">
      <input type="hidden" name="username" value="#{escape_attr(username)}">
      <input type="hidden" name="enabled" value="#{enabled}">
      <button class="acl-text-button" type="submit">#{label}</button>
    </form>
    """
  end

  defp render_password_action(username) do
    """
    <details class="acl-inline-editor">
      <summary>Reset password</summary>
      <form action="/dashboard/security/users/password" method="post">
        <input type="text" name="username" value="#{escape_attr(username)}" autocomplete="username" hidden>
        <label>New password<input type="password" name="password" minlength="12" maxlength="4096" autocomplete="new-password" required></label>
        <label>Confirm<input type="password" name="password_confirmation" minlength="12" maxlength="4096" autocomplete="new-password" required></label>
        <button class="flow-search-button" type="submit">Update</button>
      </form>
    </details>
    """
  end

  defp render_modifier_action("default"), do: ""

  defp render_modifier_action(username) do
    """
    <details class="acl-inline-editor">
      <summary>ACL modifiers</summary>
      <form action="/dashboard/security/users/rules" method="post">
        <input type="hidden" name="username" value="#{escape_attr(username)}">
        <label>One per line<textarea class="mono" name="modifiers" maxlength="6000" rows="4" required></textarea></label>
        <button class="flow-search-button" type="submit">Apply</button>
      </form>
    </details>
    """
  end

  defp render_delete_action("default", _current_user, _can_delete_users), do: ""
  defp render_delete_action(username, username, _can_delete_users), do: ""
  defp render_delete_action(_username, _current_user, false), do: ""

  defp render_delete_action(username, _current_user, true) do
    """
    <details class="acl-inline-editor">
      <summary class="acl-delete-button">Delete</summary>
      <form action="/dashboard/security/users/delete" method="post">
        <input type="hidden" name="username" value="#{escape_attr(username)}">
        <p>Delete <strong class="mono">#{escape(username)}</strong>? Existing sessions will stop immediately.</p>
        <button class="flow-search-button flow-danger-button" type="submit">Confirm delete</button>
      </form>
    </details>
    """
  end

  def render_dashboard_route_requirements(data) do
    rows =
      data
      |> Map.get(:route_requirements, [])
      |> Enum.map_join("\n", fn route ->
        """
        <tr>
          <td>#{escape(Map.get(route, :section, ""))}</td>
          <td class="mono">#{escape(Map.get(route, :method, ""))}</td>
          <td class="mono">#{escape(Map.get(route, :path, ""))}</td>
          <td class="mono">#{escape(Map.get(route, :command, ""))}</td>
          <td class="mono">#{escape(Map.get(route, :key, ""))}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Dashboard Route Requirements</div>
    #{table_scroll("Dashboard route requirements", """
    <table>
      <thead><tr><th>Page</th><th>Method</th><th>Path</th><th>Required ACL Command</th><th>Key Scope</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table>
    """)}
    """
  end

  defp table_scroll(label, table) do
    ~s(<div class="table-scroll" role="region" aria-label="#{escape_attr(label)}" tabindex="0">#{table}</div>)
  end
end
