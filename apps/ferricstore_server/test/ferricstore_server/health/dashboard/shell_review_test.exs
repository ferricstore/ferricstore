defmodule FerricstoreServer.Health.Dashboard.ShellReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Layout
  alias FerricstoreServer.Health.Dashboard.Layout.Styles
  alias FerricstoreServer.Health.Dashboard.Assets
  alias FerricstoreServer.Health.Endpoint.Session

  test "every dashboard shell includes one native shortcut dialog" do
    html = Layout.render_sidebar_static("flow_states")
    assert html =~ ~s(<dialog id="keyboard-shortcuts-modal")
    refute html =~ ~s(aria-hidden="true")
    assert html =~ "data-dashboard-shortcuts-open"
    assert Layout.dashboard_live_script() =~ "modal.showModal()"
  end

  test "shared clipboard and search handlers validate their real outcome and target" do
    script = Layout.dashboard_live_script()
    assert script =~ "await navigator.clipboard.writeText(text)"
    assert script =~ "Copy failed"
    assert script =~ "input.flow-search-input"
    assert script =~ "getClientRects().length"
  end

  test "confirmation panels stay in layout and long tables own vertical scrolling" do
    css = Styles.stylesheet()
    assert css =~ ".flow-action-confirm-panel { position: static;"
    assert css =~ "max-height: min(65vh, 720px)"
    assert css =~ ".table-scroll table { min-width: 100%; overflow: visible; }"
    assert css =~ ".flow-link { color: #38bdf8; text-decoration: underline;"
  end

  test "snapshot freshness, bounded filtering and scope retention are shared enhancements" do
    script = Layout.dashboard_live_script()
    assert script =~ "data-dashboard-snapshot"
    assert script =~ "data-dashboard-table-filter"
    assert script =~ "preserveWorkflowScope"
    assert script =~ "new URL(window.location.href)"
    assert Layout.render_subpage_header("Doctor") =~ "data-dashboard-captured-at"
  end

  test "Overview scope summary identifies type-only and combined predicates" do
    alias FerricstoreServer.Health.Dashboard.Render.FlowQueryControls

    for filters <- [%{type: "review<&"}, %{type: "review<&", partition_key: "scope<&"}] do
      html = FlowQueryControls.render_flow_overview_filter(%{filters: filters})
      assert html =~ "review&lt;&amp;"
      assert html =~ "Clear all scope filters"
      if filters[:partition_key], do: assert(html =~ "scope&lt;&amp;")
    end
  end

  test "protected session labels use the verified current request identity" do
    old = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, true)
    on_exit(fn -> restore_env(:protected_mode, old) end)
    username = "shell-reader<&"
    on_exit(fn -> FerricstoreServer.Acl.del_user(username) end)
    :ok = FerricstoreServer.Acl.set_user(username, ["on", ">test-password", "+GET", "~*"])
    cookie = Session.session_cookie(username)
    :ok = Session.prepare_request(%{"cookie" => cookie})
    assert Layout.render_sidebar_static("flow_detail") =~ "shell-reader&lt;&amp;"
    :ok = Session.prepare_request(%{})
    refute Layout.render_sidebar_static("flow_detail") =~ "shell-reader&lt;&amp;"
  end

  test "fingerprinted assets contain no page data and replace inline shell bytes" do
    html = Layout.page_head("Test", 2)
    assert html =~ Assets.path(:css)
    assert html =~ Assets.path(:js)
    refute html =~ "<style>"
    refute html =~ "function patchComponents"
    assert {:ok, "text/css; charset=utf-8", css} = Assets.fetch(Assets.path(:css))
    assert css == Styles.stylesheet()
    assert {:ok, "text/javascript; charset=utf-8", js} = Assets.fetch(Assets.path(:js))
    assert js =~ "function patchComponents"
    refute js =~ "<script"
    assert :error = Assets.fetch("/dashboard/assets/../../secret")
    assert :error = Assets.fetch(Assets.path(:css) <> "x")
  end

  test "asset HTTP delivery is immutable while protected documents remain private" do
    port = FerricstoreServer.Health.Endpoint.port()
    css = http_get(port, Assets.path(:css))
    assert extract_status_code(css) == 200
    assert extract_header(css, "cache-control") == "public, max-age=31536000, immutable"
    assert extract_header(css, "x-content-type-options") == "nosniff"
    assert length(Regex.scan(~r/Cache-Control:/i, extract_headers(css))) == 1
    refute extract_header(css, "set-cookie")
    assert extract_body(css) == Styles.stylesheet()
    assert extract_status_code(http_get(port, "/dashboard/assets/unknown.js")) == 404
    assert extract_header(http_get(port, "/dashboard/login"), "cache-control") == "no-store"
  end

  test "Workers and Due preserve both investigation predicates in HTML and live data" do
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    suffix = Integer.to_string(System.unique_integer([:positive]))
    type = "shell-scope-" <> suffix

    for {id, t, partition} <- [
          {"included", type, "one"},
          {"other-partition", type, "two"},
          {"other-type", type <> "-other", "one"}
        ] do
      :ok = FerricStore.flow_create(id <> suffix, type: t, partition_key: partition)

      assert {:ok, [_claim]} =
               FerricStore.flow_claim_due(t,
                 partition_key: partition,
                 worker: "scope-worker",
                 lease_ms: 60_000,
                 limit: 1
               )
    end

    for route <- ["workers", "due"] do
      path =
        "/dashboard/flow/" <>
          route <> "?" <> URI.encode_query(%{type: type, partition_key: "one"})

      html = http_get(FerricstoreServer.Health.Endpoint.port(), path) |> extract_body()
      assert html =~ "partition_key=one"
      assert html =~ "type=#{type}"

      if route == "workers" do
        assert html =~ "included#{suffix}"
        refute html =~ "other-partition#{suffix}"
        refute html =~ "other-type#{suffix}"

        live =
          http_get(
            FerricstoreServer.Health.Endpoint.port(),
            String.replace(path, "/dashboard/flow/", "/dashboard/api/flow/")
          )
          |> extract_body()
          |> Jason.decode!()

        records = live["components"]["flow_running_records"]
        assert records =~ "included#{suffix}"
        refute records =~ "other-partition#{suffix}"
        refute records =~ "other-type#{suffix}"
      end
    end
  end

  test "missing live detail stops stale execution and mutation presentation" do
    {:ok, payload} =
      FerricstoreServer.Health.Dashboard.LivePayload.live_payload(
        "flow/shell-never-existed?partition_key=none",
        []
      )

    assert payload.detail_unavailable == true
    assert payload.components["flow_history"] == ""
    assert payload.components["flow_debug"] == ""
  end
end
