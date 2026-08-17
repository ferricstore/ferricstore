defmodule FerricstoreServer.Health.Dashboard.FlowGovernanceActionsTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Flow.Governance
  alias FerricstoreServer.Health.Dashboard.Render.FlowGovernance
  alias FerricstoreServer.Health.Endpoint.RouteRequirements

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    :ok
  end

  test "pending approvals render confirmed decisions with preserved filters" do
    approval = %{
      id: "approval-1",
      status: :pending,
      flow_id: "flow-1",
      scope: "tenant-a:payments",
      requested_at_ms: 1_000
    }

    html =
      FlowGovernance.render_flow_governance_approvals([approval], %{
        scope: "tenant-a:payments",
        status: "pending",
        limit: 25
      })

    assert html =~ "Confirm Approve"
    assert html =~ "Confirm Reject"
    assert html =~ ~s(name="confirm_action" value="true")
    assert html =~ ~s(name="expected_status" value="pending")
    assert html =~ ~s(name="expected_requested_at_ms" value="1000")
    assert html =~ ~s(name="decision_reason")
    assert html =~ ~s(name="approval_status" value="pending")
    assert html =~ ~s(data-dashboard-single-submit)
  end

  test "approval actions have command-specific scope requirements" do
    params = %{"action" => "approve_approval", "approval_scope" => "tenant-a:payments"}

    assert RouteRequirements.flow_governance_form_requirement(params) ==
             {"FLOW.APPROVAL.APPROVE", key: {"tenant-a:payments", :write}}

    assert RouteRequirements.flow_governance_form_requirement(%{
             params
             | "action" => "reject_approval"
           }) == {"FLOW.APPROVAL.REJECT", key: {"tenant-a:payments", :write}}
  end

  test "approval decisions require confirmation and reject a stale replay" do
    now_ms = System.system_time(:millisecond)
    id = "dashboard-approval-#{System.unique_integer([:positive])}"

    assert {:ok, approval} =
             FerricStore.flow_approval_request(id,
               flow_id: "flow-1",
               scope: "tenant-a:payments",
               requested_by: "worker",
               now_ms: now_ms
             )

    params = %{
      "action" => "approve_approval",
      "approval_id" => id,
      "approval_scope" => approval.scope,
      "expected_status" => "pending",
      "expected_requested_at_ms" => Integer.to_string(approval.requested_at_ms),
      "decision_reason" => "verified"
    }

    assert {:error, message} = Governance.apply_form(params, approver: "operator")
    assert message =~ "confirmation"
    assert {:ok, %{status: :pending}} = FerricStore.flow_approval_get(id)

    confirmed = Map.put(params, "confirm_action", "true")
    assert {:ok, message} = Governance.apply_form(confirmed, approver: "operator")
    assert message =~ "approved"

    assert {:ok, decided} = FerricStore.flow_approval_get(id)
    assert decided.status == :approved
    assert decided.decided_by == "operator"
    assert decided.decision_reason == "verified"

    assert {:error, stale_message} = Governance.apply_form(confirmed, approver: "operator")
    assert stale_message =~ "changed"
  end

  test "protected HTTP decisions enforce the action command and record the session actor" do
    previous_protected_mode = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, true)
    FerricstoreServer.Acl.reset!()

    on_exit(fn ->
      restore_env(:protected_mode, previous_protected_mode)
      FerricstoreServer.Acl.reset!()
    end)

    now_ms = System.system_time(:millisecond)
    id = "dashboard-http-approval-#{System.unique_integer([:positive])}"
    scope = "tenant-a:payments"

    assert {:ok, approval} =
             FerricStore.flow_approval_request(id,
               flow_id: "flow-http-1",
               scope: scope,
               now_ms: now_ms
             )

    :ok =
      FerricstoreServer.Acl.set_user("approval-rejector", [
        "on",
        ">secret",
        "%R~*",
        "%W~tenant-a:*",
        "-@all",
        "+FLOW.GOVERNANCE.OVERVIEW",
        "+FLOW.APPROVAL.REJECT"
      ])

    denied_login = dashboard_login("approval-rejector", "secret")

    assert extract_status_code(denied_login) == 302
    assert dashboard_session_cookie(denied_login) =~ "ferricstore_dashboard="

    params = %{
      "action" => "approve_approval",
      "approval_id" => id,
      "approval_scope" => scope,
      "confirm_action" => "true",
      "expected_status" => "pending",
      "expected_requested_at_ms" => Integer.to_string(approval.requested_at_ms)
    }

    {denied_csrf_token, denied_cookie} =
      csrf_credentials(dashboard_session_cookie(denied_login))

    denied =
      http_post_form(
        FerricstoreServer.Health.Endpoint.port(),
        "/dashboard/flow/governance",
        Map.put(params, "_csrf_token", denied_csrf_token),
        [{"Cookie", denied_cookie}]
      )

    assert extract_status_code(denied) == 403
    refute extract_body(denied) =~ "CSRF"
    assert extract_body(denied) =~ "FLOW.APPROVAL.APPROVE"
    assert {:ok, %{status: :pending}} = FerricStore.flow_approval_get(id)

    :ok =
      FerricstoreServer.Acl.set_user("approval-operator", [
        "on",
        ">secret",
        "%R~*",
        "%W~tenant-a:*",
        "-@all",
        "+FLOW.GOVERNANCE.OVERVIEW",
        "+FLOW.APPROVAL.APPROVE"
      ])

    allowed_login = dashboard_login("approval-operator", "secret")

    assert extract_status_code(allowed_login) == 302
    assert dashboard_session_cookie(allowed_login) =~ "ferricstore_dashboard="

    {allowed_csrf_token, allowed_cookie} =
      csrf_credentials(dashboard_session_cookie(allowed_login))

    allowed =
      http_post_form(
        FerricstoreServer.Health.Endpoint.port(),
        "/dashboard/flow/governance",
        Map.put(params, "_csrf_token", allowed_csrf_token),
        [{"Cookie", allowed_cookie}]
      )

    assert extract_status_code(allowed) == 302
    assert {:ok, decided} = FerricStore.flow_approval_get(id)
    assert decided.status == :approved
    assert decided.decided_by == "approval-operator"
  end

  defp csrf_credentials(session_cookie) do
    response =
      http_get(FerricstoreServer.Health.Endpoint.port(), "/dashboard/flow/governance", [
        {"Cookie", session_cookie}
      ])

    assert extract_status_code(response) == 200
    [_, token] = Regex.run(~r/name="_csrf_token" value="([^"]+)"/, extract_body(response))
    csrf_cookie = response |> extract_header("set-cookie") |> String.split(";", parts: 2) |> hd()
    {token, session_cookie <> "; " <> csrf_cookie}
  end

  defp dashboard_login(username, password) do
    page = http_get(FerricstoreServer.Health.Endpoint.port(), "/dashboard/login")
    [_, token] = Regex.run(~r/name="_csrf_token" value="([^"]+)"/, extract_body(page))
    cookie = page |> extract_header("set-cookie") |> String.split(";", parts: 2) |> hd()

    http_post_form(
      FerricstoreServer.Health.Endpoint.port(),
      "/dashboard/login",
      %{"username" => username, "password" => password, "_csrf_token" => token},
      [{"Cookie", cookie}]
    )
  end
end
