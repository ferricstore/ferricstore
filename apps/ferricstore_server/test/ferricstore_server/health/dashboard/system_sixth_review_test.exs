defmodule FerricstoreServer.Health.Dashboard.SystemSixthReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Layout.Styles

  alias FerricstoreServer.Health.Dashboard.Render.{
    Admin,
    FlowIndexCatalog,
    FlowSchedules,
    MessagingPages
  }

  alias FerricstoreServer.Health.Endpoint.{Auth, Login}

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.fetch_env(:ferricstore, :protected_mode)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:ferricstore, :protected_mode, value)
        :error -> Application.delete_env(:ferricstore, :protected_mode)
      end
    end)

    :ok
  end

  test "17 login accurately labels protected mode without changing open-mode authorization" do
    Application.put_env(:ferricstore, :protected_mode, false)
    html = Login.render_page("/dashboard/flow?type=invoice", nil)
    assert html =~ "Protected mode off"
    assert html =~ "Open access"
    assert html =~ "signing in does not enable ACL enforcement"
    refute html =~ ">Protected access<"
    refute html =~ "Every dashboard page keeps that account"
    assert html =~ ~s(action="/dashboard/login")
    assert html =~ ~s(name="next" value="/dashboard/flow?type=invoice")
    refute html =~ ~s(action="/dashboard/setup")
    assert Auth.authorize_request("GET", "/dashboard", {127, 0, 0, 1}, %{}) == :ok
  end

  test "17 protected login retains its credential form and actual protection status" do
    Application.put_env(:ferricstore, :protected_mode, true)
    html = Login.render_page("//elsewhere.test", "<bad credentials>")
    assert html =~ "Protected mode on"
    assert html =~ ">Protected access<"
    assert html =~ "Every dashboard page keeps that account"
    assert html =~ "&lt;bad credentials&gt;"
    assert html =~ ~s(name="next" value="/dashboard")
    assert html =~ ~s(type="password")
    refute html =~ ">Open access<"
  end

  test "18 Latest stream time has deliberate date and compact time lines with full precision preserved" do
    timestamp = DateTime.to_unix(~U[2026-09-10 15:17:31.123456Z], :microsecond)
    html = MessagingPages.render_stream_activity_summary(%{summary: %{latest_at_us: timestamp}})
    assert html =~ "ops-summary-timestamp"
    assert html =~ ">2026-09-10</div>"
    assert html =~ ~s(datetime="2026-09-10T15:17:31.123456Z")
    assert html =~ ~s(class="stream-latest-time")
    assert html =~ ">15:17:31.123 UTC</time>"
    assert MessagingPages.render_stream_activity_summary(%{}) =~ ">idle</div>"
  end

  test "19 absent slowlog observations do not claim measured average or worst latency" do
    html = Admin.render_slowlog_summary([])
    assert length(Regex.scan(~r/>No samples</, html)) == 2
    assert html =~ ">0</div>"
    observed = Admin.render_slowlog_summary([%{duration_us: 0}])
    refute observed =~ "No samples"
    assert observed =~ ">0.0 ms</div>"
  end

  test "20 overlap policy uses compact labels and associated selected behavior while preserving review" do
    descriptions = [
      {"skip", "Skip", "Skip this tick while the previous target is active."},
      {"allow", "Allow", "Fire even while the previous target is active."},
      {"queue_after_previous", "Queue after previous",
       "Wait until the previous target is no longer active."},
      {"fail_schedule", "Fail schedule",
       "Stop the schedule if its previous target is still active."}
    ]

    for {policy, label, description} <- descriptions do
      html =
        FlowSchedules.render_flow_schedule_create_form(%{
          draft: %{"schedule_kind" => "cron", "overlap_policy" => policy}
        })

      assert html =~ ~r/<option[^>]*value="#{policy}"[^>]*selected[^>]*>#{label}<\/option>/
      assert html =~ ~s(id="schedule-overlap-description" role="status">#{description}</small>)

      assert html =~
               ~s(aria-describedby="schedule-overlap-description schedule-create-overlap_policy-error")

      assert html =~ "Review schedule"
      refute html =~ "skip (skip tick"
    end

    one_shot =
      FlowSchedules.render_flow_schedule_create_form(%{draft: %{"schedule_kind" => "delay"}})

    assert one_shot =~ "Overlap policy applies only to cron and interval schedules."
  end

  test "21 service statuses are grouped in an explicit definition grid with escaped values" do
    html =
      FlowIndexCatalog.render(%{
        status: :ok,
        snapshot: %{"services" => %{"registry" => "<running>"}}
      })

    assert html =~ ~s(<dl class="flow-index-services" aria-label="Query index services">)
    assert html =~ "<dt>Registry</dt><dd>&lt;running&gt;</dd>"
    assert html =~ "<dt>Statistics worker</dt><dd>unavailable</dd>"
  end

  test "22 shared button styling centers native buttons and button-styled anchors" do
    [_, rules] = Regex.run(~r/\.flow-search-button\s*\{([^}]+)\}/, Styles.stylesheet())
    assert rules =~ "display: inline-flex"
    assert rules =~ "align-items: center"
    assert rules =~ "justify-content: center"
    assert rules =~ "text-decoration: none"
  end
end
