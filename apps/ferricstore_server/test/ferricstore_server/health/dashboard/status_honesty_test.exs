defmodule FerricstoreServer.Health.Dashboard.StatusHonestyTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.{
    Capabilities,
    FlowGovernance,
    MessagingPages,
    Security
  }

  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Records

  describe "a failed or partial load never reads as healthy" do
    test "Governance says its records are unavailable, not that governance is unused" do
      html =
        FlowGovernance.render_flow_governance_summary(%{
          counts: %{approvals: 0, pending_approvals: 0, circuits: 0, open_circuits: 0},
          error: "ERR governance overview timed out"
        })

      assert html =~ "governance-status is-warning"
      assert html =~ "<strong>Governance records unavailable</strong>"
      refute html =~ "not in use"
      refute html =~ "stat-strip"
    end

    test "partial Runs results never show a healthy line" do
      state = %{
        type: "t",
        state: "queued",
        count: 5,
        due_now: 0,
        running: 0,
        expired_leases: 0,
        failed: 0,
        max_attempts_reached: 0,
        retrying: 0
      }

      html = Records.render_flow_states_overview([state], 5, :partial, %{})

      assert html =~ "runs-status is-idle"
      assert html =~ "<strong>No problems in the records that loaded</strong>"
      assert html =~ "failed runs may be missing"
      refute html =~ "is-healthy"

      flagged = Records.render_flow_states_overview([%{state | failed: 2}], 5, :partial, %{})
      assert flagged =~ "runs-status is-warning"
      assert flagged =~ "partial results"
    end

    test "Capabilities that could not be read say so" do
      html = Capabilities.render_management_capability_summary(%{})

      assert html =~ "capabilities-status is-warning"
      assert html =~ "<strong>Capabilities unavailable</strong>"
      refute html =~ "0 of 0"
    end
  end

  describe "counts the viewer's access may limit say visible" do
    test "Streams and Pub/Sub" do
      assert MessagingPages.render_stream_activity_summary(%{}) =~
               "<strong>No visible stream activity in the retained window</strong>"

      assert MessagingPages.render_pubsub_summary(%{}) =~
               "<strong>No visible subscriptions</strong>"
    end

    test "Governance" do
      html = FlowGovernance.render_flow_governance_summary(%{counts: %{}})

      assert html =~ "<strong>No governance records visible</strong>"
      refute html =~ "not in use"
    end
  end

  describe "Security" do
    test "protected mode off says the dashboard needs no sign-in" do
      html =
        Security.render_acl_security_summary(%{protected_mode: false, configured_users: true})

      assert html =~ "the dashboard needs no sign-in"
    end

    test "no session reads as not signed in" do
      html =
        Security.render_acl_security_summary(%{
          protected_mode: true,
          configured_users: true,
          acl_user_count: 2
        })

      assert html =~ "not signed in"
      refute html =~ "signed in as open"
    end
  end
end
