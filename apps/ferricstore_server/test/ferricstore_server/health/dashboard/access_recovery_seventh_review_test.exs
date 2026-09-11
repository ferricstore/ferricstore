defmodule FerricstoreServer.Health.Dashboard.AccessRecoverySeventhReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias Ferricstore.Flow.Governance.CircuitStore
  alias FerricstoreServer.Acl
  alias FerricstoreServer.Health.Dashboard.Accounts
  alias FerricstoreServer.Health.Dashboard.Flow.Recovery
  alias FerricstoreServer.Health.Dashboard.Render.{FlowComponents, Security}
  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.Session

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, true)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    :ok
  end

  test "rejected ACL modifiers reopen the exact editor without credentials or ACL list reads" do
    actor = user(["-@all", "+ACL.SETUSER"])
    target = " " <> uid("<account>") <> " "

    params = %{
      "username" => target,
      "modifiers" =>
        "+GET\n%R~tenant:*\noff\n >private-password\n#private-hash\n!removed-hash\n<removed-password",
      "password" => "secret-form-password"
    }

    response = post(actor, "/dashboard/security/users/rules", params)
    assert extract_status_code(response) == 422
    assert extract_header(response, "location") == nil
    html = extract_body(response)
    assert html =~ ~s(<details class="acl-inline-editor" open)
    assert html =~ ~s(name="username" value="#{escape(target)}")
    assert html =~ "+GET\n%R~tenant:*\noff"
    assert html =~ ~s(aria-invalid="true")
    assert html =~ "Password and account state modifiers use separate controls."
    refute html =~ "private-password"
    refute html =~ "private-hash"
    refute html =~ "removed-hash"
    refute html =~ "removed-password"
    refute html =~ "secret-form-password"
    refute html =~ "ACL account list"
    refute html =~ "Create account"
  end

  test "delete-only administrators get Delete without SETUSER controls or misleading view-only labels" do
    html =
      Security.render_acl_security(%{
        current_user: "deleter",
        can_manage_users: false,
        can_delete_users: true,
        acl_users: [
          %{username: "default", state: "on"},
          %{username: "deleter", state: "on"},
          %{username: "target", state: "on"}
        ]
      })

    assert html =~ "Confirm delete"
    assert length(Regex.scan(~r/action="\/dashboard\/security\/users\/delete"/, html)) == 1
    refute html =~ "Account mutations require"
    refute html =~ "Read-only access"
    refute html =~ "Reset password"
    refute html =~ "ACL modifiers</summary>"
    refute html =~ ~s(action="/dashboard/security/users/state")
  end

  test "credential modifier validation never echoes credential text" do
    existing = user(["-@all"])
    before = Acl.get_user(existing)

    for target <- [uid("missing"), existing], prefix <- [">", "<", "#", "!"] do
      assert {:error, message} =
               Accounts.apply_modifiers("actor", %{
                 "username" => target,
                 "modifiers" => "+GET\n " <> prefix <> "private-credential"
               })

      assert message == "Password and account state modifiers use separate controls."
      refute message =~ "private-credential"
      assert Acl.get_user(existing) == before
    end
  end

  test "stale circuit failure retains settings and exact target but never silently refreshes review" do
    scope = " " <> uid("circuit<&") <> " "

    actor =
      user([
        "-@all",
        "+FLOW.GOVERNANCE.OVERVIEW",
        "+FLOW.CIRCUIT.OPEN",
        "+FLOW.CIRCUIT.GET",
        "%R~*",
        "%W~*"
      ])

    {:ok, original} = FerricStore.flow_circuit_open(scope)
    {:ok, current} = FerricStore.flow_circuit_close(scope)

    params = %{
      "action" => "open_circuit",
      "scope" => scope,
      "return_scope" => "unrelated-filter",
      "circuit_review_scope" => scope,
      "expected_review" => CircuitStore.review_fingerprint(original),
      "confirm_action" => "true",
      "failure_threshold" => "17",
      "open_ms" => "98765"
    }

    response = post(actor, "/dashboard/flow/governance", params)
    assert extract_status_code(response) == 422
    html = extract_body(response)
    assert html =~ ~s(name="failure_threshold" value="17")
    assert html =~ ~s(name="open_ms" value="98765")
    assert html =~ ~s(name="scope" value="#{escape(scope)}")
    assert html =~ "Refresh and review"
    refute html =~ ~s(name="expected_review")
    refute html =~ ~s(name="confirm_action")
    refute html =~ "Confirm Open"
    assert {:ok, ^current} = FerricStore.flow_circuit_get(scope)

    review_response =
      post(actor, "/dashboard/flow/governance", Map.put(params, "review_only", "true"))

    assert extract_status_code(review_response) == 200
    review_html = extract_body(review_response)

    assert review_html =~
             ~s(name="expected_review" value="#{CircuitStore.review_fingerprint(current)}")

    assert review_html =~ ~s(name="failure_threshold" value="17")
    assert review_html =~ ~s(name="open_ms" value="98765")
    assert review_html =~ ~s(type="checkbox" name="confirm_action" value="true" required)
    refute review_html =~ ~s(value="true" required checked)
    assert {:ok, ^current} = FerricStore.flow_circuit_get(scope)

    fresh = Map.put(params, "expected_review", CircuitStore.review_fingerprint(current))
    assert extract_status_code(post(actor, "/dashboard/flow/governance", fresh)) == 302
    assert {:ok, %{failure_threshold: 17, open_ms: 98765}} = FerricStore.flow_circuit_get(scope)
    assert extract_status_code(post(actor, "/dashboard/flow/governance", fresh)) == 422
  end

  test "approval rejection retains reason without granting a mutation-only actor an automatic read" do
    scope = " " <> uid("approval-scope") <> " "
    id = " " <> uid("approval") <> " "
    actor = user(["-@all", "+FLOW.GOVERNANCE.OVERVIEW", "+FLOW.APPROVAL.REJECT", "%W~*"])
    {:ok, approval} = FerricStore.flow_approval_request(id, flow_id: "workflow", scope: scope)

    params = %{
      "action" => "reject_approval",
      "approval_id" => id,
      "approval_scope" => scope,
      "expected_status" => "pending",
      "expected_requested_at_ms" => "0",
      "decision_reason" => "Retain <review> reason",
      "confirm_action" => "true"
    }

    response = post(actor, "/dashboard/flow/governance", params)
    assert extract_status_code(response) == 422
    html = extract_body(response)
    assert html =~ "Retain &lt;review&gt; reason"
    assert html =~ ~s(name="approval_id" value="#{id}")
    assert html =~ ~s(name="approval_scope" value="#{scope}")
    refute html =~ ~s(name="expected_requested_at_ms")
    refute html =~ "workflow</a>"

    denied = post(actor, "/dashboard/flow/governance", Map.put(params, "review_only", "true"))
    assert extract_status_code(denied) == 403
    assert {:ok, ^approval} = FerricStore.flow_approval_get(id)

    :ok = Acl.set_user(actor, ["+FLOW.APPROVAL.GET", "%R~*"])
    reviewed = post(actor, "/dashboard/flow/governance", Map.put(params, "review_only", "true"))
    assert extract_status_code(reviewed) == 200

    assert extract_body(reviewed) =~
             ~s(name="expected_requested_at_ms" value="#{approval.requested_at_ms}")

    assert extract_body(reviewed) =~ "Retain &lt;review&gt; reason"

    assert extract_body(reviewed) =~
             ~s(type="checkbox" name="confirm_action" value="true" required)

    assert {:ok, ^approval} = FerricStore.flow_approval_get(id)

    fresh =
      Map.put(params, "expected_requested_at_ms", Integer.to_string(approval.requested_at_ms))

    assert extract_status_code(post(actor, "/dashboard/flow/governance", fresh)) == 302

    assert {:ok,
            %{status: :rejected, decided_by: ^actor, decision_reason: "Retain <review> reason"}} =
             FerricStore.flow_approval_get(id)

    assert extract_status_code(post(actor, "/dashboard/flow/governance", fresh)) == 422
  end

  test "fresh approval review cannot substitute another scope to read a target" do
    scope = uid("private-scope")
    id = uid("private-approval")

    {:ok, _} =
      FerricStore.flow_approval_request(id, flow_id: "private-flow-evidence", scope: scope)

    actor =
      user([
        "-@all",
        "+FLOW.GOVERNANCE.OVERVIEW",
        "+FLOW.APPROVAL.GET",
        "+FLOW.APPROVAL.REJECT",
        "%R~public",
        "%W~public"
      ])

    response =
      post(actor, "/dashboard/flow/governance", %{
        "action" => "reject_approval",
        "approval_id" => id,
        "approval_scope" => "public",
        "decision_reason" => "keep",
        "review_only" => "true"
      })

    assert extract_status_code(response) == 403
    refute extract_body(response) =~ "private-flow-evidence"
    refute extract_body(response) =~ scope
    refute extract_body(response) =~ ~s(name="expected_requested_at_ms")
  end

  test "expired evidence groups offer scoped prepare links preserving literal type and partition" do
    type = " all <type> "
    partition = " ALL &partition "
    filters = %{type: nil, partition_key: nil, q: nil, limit: 40, scan_exact: false}

    expired = %{
      id: "one",
      type: type,
      partition_key: partition,
      state: "running",
      lease_expires_at_ms: 1
    }

    failed = %{id: "terminal", type: "not-reclaimable", partition_key: "other", state: "failed"}

    html =
      FlowComponents.render_flow_recovery_actions(%{
        filters: filters,
        candidates: [expired, Map.put(expired, :id, "two"), failed]
      })

    assert html =~ "Prepare reclaim"
    assert html =~ "2 expired leases"
    assert html =~ URI.encode_www_form(type)
    assert html =~ URI.encode_www_form(partition)
    refute html =~ "not-reclaimable"
    assert length(Regex.scan(~r/>Prepare reclaim</, html)) == 1

    opts =
      Recovery.opts_from_query(URI.encode_query(%{"type" => type, "partition_key" => partition}))

    assert opts[:type] == type
    assert opts[:partition_key] == partition

    prepared =
      Recovery.prepare_reclaim_path(%{type: type, partition_key: partition}, %{
        filters
        | scan_exact: true,
          q: "invoice",
          limit: 80
      })

    assert URI.decode_query(URI.parse(prepared).query) == %{
             "type" => type,
             "partition_key" => partition,
             "exact" => "true",
             "q" => "invoice",
             "limit" => "80"
           }
  end

  test "prepared reclaim submits literal identifiers including native partition selector words" do
    previous = Application.get_env(:ferricstore, :flow_dashboard_flow_reclaim_fun)
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_reclaim_fun, fn type, opts ->
      send(parent, {:reclaim, type, opts})
      {:ok, []}
    end)

    on_exit(fn -> restore_env(:flow_dashboard_flow_reclaim_fun, previous) end)

    for partition <- ["any", "AUTO", "global", " literal "] do
      assert {:ok, _} =
               Recovery.apply_form(%{
                 "action" => "reclaim",
                 "type" => " all ",
                 "partition_key" => partition,
                 "confirm_reclaim" => "true"
               })

      assert_received {:reclaim, " all ", opts}
      assert opts[:partition_keys] == [partition]
      refute Keyword.has_key?(opts, :partition_key)
    end
  end

  defp user(rules) do
    name = uid("actor")
    :ok = Acl.set_user(name, ["on", "nopass" | rules])
    on_exit(fn -> Acl.del_user(name) end)
    name
  end

  defp post(actor, path, params) do
    session = Session.session_cookie(actor) |> String.split(";", parts: 2) |> hd()
    {token, csrf_cookie} = Session.csrf_pair()
    csrf = csrf_cookie |> String.split(";", parts: 2) |> hd()

    http_post_form(Endpoint.port(), path, Map.put(params, "_csrf_token", token), [
      {"Cookie", session <> "; " <> csrf}
    ])
  end

  defp uid(prefix), do: "seventh-#{prefix}-#{System.unique_integer([:positive])}"
  defp escape(value), do: FerricstoreServer.Health.Dashboard.Format.escape_attr(value)
end
