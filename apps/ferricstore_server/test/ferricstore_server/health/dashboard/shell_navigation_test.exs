defmodule FerricstoreServer.Health.Dashboard.ShellNavigationTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Layout

  # Every dashboard page, by the key its template passes to the shell.
  @pages %{
    "overview" => "/dashboard",
    "flow" => "/dashboard/flow",
    "flow_states" => "/dashboard/flow/states",
    "flow_due" => "/dashboard/flow/due",
    "flow_failures" => "/dashboard/flow/failures",
    "flow_schedules" => "/dashboard/flow/schedules",
    "flow_workers" => "/dashboard/flow/workers",
    "flow_query" => "/dashboard/flow/query",
    "flow_lineage" => "/dashboard/flow/lineage",
    "flow_signals" => "/dashboard/flow/signals",
    "flow_policies" => "/dashboard/flow/policies",
    "flow_governance" => "/dashboard/flow/governance",
    "flow_retention" => "/dashboard/flow/retention",
    "keyspace" => "/dashboard/keyspace",
    "prefixes" => "/dashboard/prefixes",
    "reads" => "/dashboard/reads",
    "storage" => "/dashboard/storage",
    "merge" => "/dashboard/merge",
    "commands" => "/dashboard/commands",
    "streams" => "/dashboard/streams",
    "pubsub" => "/dashboard/pubsub",
    "raft" => "/dashboard/raft",
    "clients" => "/dashboard/clients",
    "slowlog" => "/dashboard/slowlog",
    "doctor" => "/dashboard/doctor",
    "security" => "/dashboard/security",
    "config" => "/dashboard/config",
    "capabilities" => "/dashboard/capabilities"
  }

  test "the sidebar has one link per destination in always-open sections" do
    nav = sidebar_nav(Layout.render_sidebar_static("overview"))

    refute nav =~ "<details"
    assert section_labels(nav) == ["Workflows", "Data", "Cluster", "Admin"]

    assert links(nav) == [
             {"/dashboard", "Overview"},
             {"/dashboard/flow", "Overview"},
             {"/dashboard/flow/states", "Runs"},
             {"/dashboard/flow/failures", "Failures"},
             {"/dashboard/flow/schedules", "Schedules"},
             {"/dashboard/flow/workers", "Workers"},
             {"/dashboard/flow/query", "Query"},
             {"/dashboard/flow/policies", "Settings"},
             {"/dashboard/keyspace", "Keys"},
             {"/dashboard/storage", "Storage"},
             {"/dashboard/commands", "Commands"},
             {"/dashboard/streams", "Streams & Pub/Sub"},
             {"/dashboard/raft", "Consensus"},
             {"/dashboard/clients", "Clients"},
             {"/dashboard/slowlog", "Slow log"},
             {"/dashboard/doctor", "Doctor"},
             {"/dashboard/security", "Security"},
             {"/dashboard/config", "Config"}
           ]
  end

  test "a page without its own sidebar link marks its parent destination" do
    for {active, parent_href} <- [
          {"prefixes", "/dashboard/keyspace"},
          {"reads", "/dashboard/keyspace"},
          {"merge", "/dashboard/storage"},
          {"pubsub", "/dashboard/streams"},
          {"flow_due", "/dashboard/flow/states"},
          {"flow_lineage", "/dashboard/flow/query"},
          {"flow_signals", "/dashboard/flow/query"},
          {"flow_governance", "/dashboard/flow/policies"},
          {"flow_retention", "/dashboard/flow/policies"},
          {"capabilities", "/dashboard/config"},
          {"flow_detail", "/dashboard/flow"}
        ] do
      nav = sidebar_nav(Layout.render_sidebar_static(active))
      assert current_links(nav) == [parent_href], "#{active} should mark #{parent_href}"
    end
  end

  test "every dashboard page is reachable from the sidebar or its destination's tabs" do
    sidebar =
      MapSet.new(links(sidebar_nav(Layout.render_sidebar_static("overview"))), &elem(&1, 0))

    for {active, href} <- @pages do
      tabs = MapSet.new(links(Layout.render_page_header(active)), &elem(&1, 0))

      assert MapSet.member?(sidebar, href) or MapSet.member?(tabs, href),
             "#{href} is not reachable from the sidebar or the #{active} header"
    end
  end

  test "the header names the section, the destination and the open view" do
    html = Layout.render_page_header("prefixes", intro: "Sample <prefixes>.")

    assert html =~ ~s(<p class="page-eyebrow">Data</p>)
    assert html =~ ~s(<h1 class="page-title">Keys</h1>)
    assert html =~ ~s(<p class="page-intro">Sample &lt;prefixes&gt;.</p>)
    assert html =~ ~s(<nav class="page-tabs" aria-label="Keys views">)

    assert current_links(html) == ["/dashboard/prefixes"]

    assert links(html) == [
             {"/dashboard/keyspace", "Browse"},
             {"/dashboard/prefixes", "Prefixes"},
             {"/dashboard/reads", "Read path"}
           ]

    assert html =~ "data-dashboard-status-host"
    assert html =~ "data-dashboard-instance"
    assert html =~ "data-dashboard-captured-at"
  end

  test "a destination with one view has no tabs, and a page outside the navigation keeps its title" do
    consensus = Layout.render_page_header("raft")
    assert consensus =~ ~s(<p class="page-eyebrow">Cluster</p>)
    assert consensus =~ ~s(<h1 class="page-title">Consensus</h1>)
    refute consensus =~ "page-tabs"

    action = Layout.render_page_header("flow_detail", title: "Workflow <action>")
    assert action =~ ~s(<p class="page-eyebrow">Workflows</p>)
    assert action =~ ~s(<h1 class="page-title">Workflow &lt;action&gt;</h1>)
  end

  test "Failures carries a badge only when sampled runs need triage" do
    busy = %{flow_summary: %{expired_leases_sampled: 3, failed: 2}, slowlog: []}
    quiet = %{flow_summary: %{expired_leases_sampled: 0, failed: 0}, slowlog: []}

    assert Layout.render_sidebar(busy, "overview") =~
             ~r{href="/dashboard/flow/failures"[^>]*><span class="nav-label">Failures</span><span class="nav-badge nav-badge-warn">5</span>}

    refute Layout.render_sidebar(quiet, "overview") =~ "nav-badge"
  end

  test "the live status mounts in the header's status host" do
    script = Layout.dashboard_live_script()
    assert script =~ ~s{document.querySelector("[data-dashboard-status-host]")}
    refute script =~ "setupSidebarPreferences"
  end

  defp sidebar_nav(html) do
    [_, nav] = Regex.run(~r{<nav class="sidebar"[^>]*>(.*?)</nav>}s, html)
    nav
  end

  defp section_labels(html) do
    Regex.scan(
      ~r{role="group" aria-labelledby="([^"]+)"><span class="nav-section-label" id="\1">([^<]+)</span>},
      html,
      capture: :all_but_first
    )
    |> Enum.map(fn [_id, label] -> label end)
    |> List.flatten()
  end

  defp links(html) do
    Regex.scan(~r{<a[^>]*href="(/dashboard[^"]*)"[^>]*>(.*?)</a>}s, html)
    |> Enum.map(fn [_, href, body] ->
      label = body |> String.replace(~r/<span class="nav-badge[^"]*">[^<]*<\/span>/, "")

      {href,
       label |> String.replace(~r/<[^>]+>/, "") |> String.replace("&amp;", "&") |> String.trim()}
    end)
  end

  defp current_links(html) do
    Regex.scan(~r{<a[^>]*href="([^"]+)"[^>]*aria-current="page"}, html, capture: :all_but_first)
    |> List.flatten()
  end
end
