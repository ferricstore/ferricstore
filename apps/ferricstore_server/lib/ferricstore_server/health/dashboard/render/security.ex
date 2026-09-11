defmodule FerricstoreServer.Health.Dashboard.Render.Security do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Overview, only: [render_ops_summary: 2]

  def render_acl_security(%{modifier_form_only?: true} = data) do
    draft = Map.get(data, :account_draft, %{})
    username = Map.get(draft, "username", "")

    """
    <section aria-labelledby="acl-modifier-recovery-title">
      <h2 class="section-title" id="acl-modifier-recovery-title">ACL modifiers for <span class="mono">#{escape(username)}</span></h2>
      #{render_modifier_action(username, draft, get_in(data, [:flash, :message]))}
      <p class="flow-section-note">Nonsecret modifiers retained. Password modifiers were removed.</p>
      <a class="flow-link" href="/dashboard/security">Back to security</a>
    </section>
    """
  end

  def render_acl_security(%{account_form_only?: true} = data) do
    render_security_flash(data.flash) <>
      render_account_management(data) <>
      ~s(<a class="flow-link" href="/dashboard/security">Back to security</a>)
  end

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

    can_mutate =
      Map.get(data, :can_manage_users, false) or Map.get(data, :can_delete_users, false)

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
        value: if(can_mutate, do: "enabled", else: "read only"),
        class: if(can_mutate, do: "c-green", else: "c-muted"),
        detail: "command-specific account permissions"
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
      draft = Map.get(data, :account_draft, %{})
      role = Map.get(draft, "role", "observer")

      """
      <section class="acl-management" aria-labelledby="acl-management-title">
        <div class="acl-management-heading">
          <div>
            <h2 class="section-title" id="acl-management-title">Account management</h2>
            <p>Create a passworded ACL identity for dashboard or native access.</p>
          </div>
          <span class="badge badge-ok">ACL.SETUSER</span>
        </div>
        <form class="acl-create-form" action="/dashboard/security/users" method="post" data-dashboard-single-submit data-acl-profile-form>
          <div class="acl-form-grid">
            <label>Username
              <input class="flow-search-input mono" type="text" name="username" maxlength="1024" autocomplete="username" required placeholder="operations-reader" value="#{escape_attr(Map.get(draft, "username", ""))}">
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
            <label><input type="radio" name="role" value="admin"#{role_checked(role, "admin")}><span><strong>Administrator</strong><small>All commands, keys, and channels</small></span></label>
            <label><input type="radio" name="role" value="observer"#{role_checked(role, "observer")}><span><strong>Observer</strong><small>Read commands with scoped keys</small></span></label>
            <label><input type="radio" name="role" value="custom"#{role_checked(role, "custom")}><span><strong>Custom</strong><small>Explicit ACL modifiers</small></span></label>
          </fieldset>
          <fieldset class="acl-form-grid acl-scope-grid" data-acl-profile="observer"#{if role != "observer", do: " disabled hidden", else: ""}>
            <legend>Observer scope</legend>
            <label>Observer key pattern
              <input class="flow-search-input mono" type="text" name="key_pattern" maxlength="4096" value="#{escape_attr(Map.get(draft, "key_pattern", "*"))}">
            </label>
            <label>Observer channel pattern
              <input class="flow-search-input mono" type="text" name="channel_pattern" maxlength="4096" value="#{escape_attr(Map.get(draft, "channel_pattern", "*"))}">
            </label>
          </fieldset>
          <fieldset data-acl-profile="custom"#{if role != "custom", do: " disabled hidden", else: ""}>
            <legend>Custom access</legend>
            <label class="acl-modifier-field">Custom ACL modifiers <span>one modifier per line</span>
              <textarea class="mono" name="modifiers" maxlength="6000" rows="4" required placeholder="+GET&#10;%R~tenant-a:*&#10;&amp;tenant-a:*">#{escape(Map.get(draft, "modifiers", ""))}</textarea>
            </label>
          </fieldset>
          <p class="flow-section-note" data-acl-profile-preview role="status">#{escape(profile_summary(role))}</p>
          <div class="acl-form-actions">
            <button class="flow-search-button acl-primary-button" type="submit">Create account</button>
          </div>
        </form>
      </section>
      """
    else
      if Map.get(data, :can_delete_users, false) do
        """
        <section class="acl-readonly-panel" aria-label="Account deletion access">
          <div><strong>Account deletion</strong><span>Create, state, password, and rule changes require <code>+ACL.SETUSER</code>.</span></div>
          <span class="badge badge-idle">ACL.DELUSER</span>
        </section>
        """
      else
        prerequisite =
          if Map.get(data, :current_user) == nil do
            ~s(Use protected mode and an authenticated dashboard session to manage accounts. <a href="https://github.com/ferricstore/ferricstore/blob/main/guides/security.md#dashboard-bootstrap-and-login">Secure setup and login</a>)
          else
            ~s(Account mutations require <code>+ACL.SETUSER</code> or <code>+ACL.DELUSER</code>.)
          end

        """
        <section class="acl-readonly-panel" aria-label="Account management access">
          <div><strong>Read-only access</strong><span>#{prerequisite}</span></div>
          <span class="badge badge-idle">ACL.LIST</span>
        </section>
        """
      end
    end
  end

  defp role_checked(role, role), do: " checked"
  defp role_checked(_role, _option), do: ""

  defp profile_summary("admin"),
    do: "Administrator: all commands, keys, and channels. No scope restrictions."

  defp profile_summary("custom"),
    do: "Custom: only the explicit ACL modifiers below; no implicit read access."

  defp profile_summary(_),
    do: "Observer: read access within the selected key and channel patterns."

  def render_acl_tester(data) do
    tester = Map.get(data, :tester, %{})
    input = Map.get(tester, :input, %{})
    errors = Map.get(tester, :errors, %{})

    target_description =
      if Map.has_key?(errors, :targets),
        do: "acl-target-help acl-target-error",
        else: "acl-target-help"

    """
    <h2 class="section-title">ACL Tester</h2>
    <div class="flow-filter-panel acl-tester-panel">
      <p class="flow-filter-note" id="acl-target-help">Check an enabled account against one or more command, key, channel, or route targets.</p>
      #{tester_field_error(errors, :targets, "acl-target-error")}
      <form class="flow-filter-form acl-tester-form" action="/dashboard/security" method="get" aria-label="ACL tester" aria-describedby="#{target_description}">
        <label>User <input class="flow-search-input mono" type="search" name="user" value="#{escape_attr(Map.get(input, :user, ""))}" autocomplete="off" placeholder="default"#{tester_error_attributes(errors, :user, "acl-user-error")}>#{tester_field_error(errors, :user, "acl-user-error")}</label>
        <label>Command <input class="flow-search-input mono" type="search" name="command" value="#{escape_attr(Map.get(input, :command, ""))}" autocomplete="off" placeholder="GET"#{tester_error_attributes(errors, :command, "acl-command-error")}>#{tester_field_error(errors, :command, "acl-command-error")}</label>
        <label>Key <input class="flow-search-input mono" type="search" name="key" value="#{escape_attr(Map.get(input, :key, ""))}" autocomplete="off" placeholder="tenant:key"></label>
        <label>Key Access #{render_key_access_select(Map.get(input, :key_access, :read))}</label>
        <label>Channel <input class="flow-search-input mono" type="search" name="channel" value="#{escape_attr(Map.get(input, :channel, ""))}" autocomplete="off" placeholder="tenant:events"></label>
        <label>HTTP method <select class="flow-search-input mono" name="route_method">#{Enum.map_join(["GET", "POST"], "", fn method -> ~s(<option value="#{method}"#{if Map.get(input, :route_method, "GET") == method, do: " selected", else: ""}>#{method}</option>) end)}</select></label>
        <label>Route <input class="flow-search-input mono" type="search" name="route_path" value="#{escape_attr(Map.get(input, :route_path, ""))}" autocomplete="off" placeholder="/dashboard/flow"></label>
        <button class="flow-search-button" type="submit">Check</button>
      </form>
    </div>
    #{render_acl_test_results(tester)}
    """
  end

  defp tester_error_attributes(errors, field, id) do
    if Map.has_key?(errors, field),
      do: ~s( aria-invalid="true" aria-describedby="#{id}"),
      else: ""
  end

  defp tester_field_error(errors, field, id) do
    case Map.get(errors, field) do
      nil ->
        ""

      message ->
        ~s(<span class="flow-field-error" id="#{id}" role="alert">#{escape(message)}</span>)
    end
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
    <h2 class="section-title">Accounts <span class="badge badge-idle">ACL.LIST</span></h2>
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

  defp render_user_actions(_user, _current_user, false, false),
    do: ~s(<span class="c-muted">View only</span>)

  defp render_user_actions(user, current_user, can_manage_users, can_delete_users) do
    username = Map.get(user, :username, "")
    state = Map.get(user, :state, "off")

    """
    <div class="acl-row-actions">
      #{if can_manage_users, do: render_state_action(username, state, current_user), else: ""}
      #{if can_manage_users, do: render_password_action(username), else: ""}
      #{if can_manage_users, do: render_modifier_action(username), else: ""}
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
    <form action="/dashboard/security/users/state" method="post" data-dashboard-single-submit>
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
      <form action="/dashboard/security/users/password" method="post" data-dashboard-single-submit>
        <input type="text" name="username" value="#{escape_attr(username)}" autocomplete="username" hidden>
        <label>New password<input type="password" name="password" minlength="12" maxlength="4096" autocomplete="new-password" required></label>
        <label>Confirm<input type="password" name="password_confirmation" minlength="12" maxlength="4096" autocomplete="new-password" required></label>
        <button class="flow-search-button" type="submit">Update</button>
      </form>
    </details>
    """
  end

  defp render_modifier_action(username, draft \\ %{}, error \\ nil)

  defp render_modifier_action("default", _draft, nil), do: ""

  defp render_modifier_action(username, draft, error) do
    """
    <details class="acl-inline-editor"#{if error, do: " open", else: ""}>
      <summary>ACL modifiers</summary>
      <form action="/dashboard/security/users/rules" method="post" data-dashboard-single-submit#{if error, do: ~s( data-dashboard-returned-draft="true"), else: ""}>
        <input type="hidden" name="username" value="#{escape_attr(username)}">
        <label>One per line<textarea class="mono" name="modifiers" maxlength="6000" rows="4" required#{if error, do: ~s( aria-invalid="true" aria-describedby="acl-modifiers-error"), else: ""}>#{escape(Map.get(draft, "modifiers", ""))}</textarea></label>
        #{if error, do: ~s(<p class="flow-field-error" id="acl-modifiers-error" role="alert">#{escape(error)}</p>), else: ""}
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
      <form action="/dashboard/security/users/delete" method="post" data-dashboard-single-submit>
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
    <h2 class="section-title">Dashboard Route Requirements</h2>
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
