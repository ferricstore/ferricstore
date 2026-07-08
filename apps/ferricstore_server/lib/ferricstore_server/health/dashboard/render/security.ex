defmodule FerricstoreServer.Health.Dashboard.Render.Security do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Overview, only: [render_ops_summary: 2]

  def render_acl_security(data) when is_map(data) do
    """
    #{render_acl_security_summary(data)}
    #{render_acl_tester(data)}
    #{render_acl_users(data)}
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
        value: "hidden",
        detail: "read-only OSS diagnostics"
      }
    ])
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

    rows =
      case users do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No ACL users visible</td></tr>)

        _ ->
          Enum.map_join(users, "\n", fn user ->
            state = Map.get(user, :state, "unknown")
            state_class = if state == "on", do: "badge-ok", else: "badge-idle"

            """
            <tr>
              <td class="mono">#{escape(Map.get(user, :username, ""))}</td>
              <td><span class="badge #{state_class}">#{escape(state)}</span></td>
              <td class="mono">#{escape(Map.get(user, :summary, ""))}</td>
              <td class="mono">#{escape(Map.get(user, :rule, ""))}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">ACL Users <span class="badge badge-idle">ACL.LIST</span></div>
    #{table_scroll("ACL account list", """
    <table>
      <thead><tr><th>User</th><th>State</th><th>Rules</th><th>Full Rule Summary</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table>
    """)}
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
