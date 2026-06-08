defmodule FerricstoreServer.Health.DashboardTest.Sections.AclFlowActions do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Health.Dashboard
      alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint
      alias Ferricstore.NamespaceConfig
      alias Ferricstore.Test.ShardHelpers

  describe "dashboard ACL login and Flow write permissions" do
    setup do
      FerricstoreServer.Acl.reset!()
      :ok
    end

    test "protected mode off leaves dashboard open without a session" do
      Application.put_env(:ferricstore, :protected_mode, false)

      response = http_get(HealthEndpoint.port(), "/dashboard/flow")

      assert extract_status_code(response) == 200
      assert extract_body(response) =~ "FerricFlow"
    end

    test "protected mode redirects dashboard pages to ACL login" do
      Application.put_env(:ferricstore, :protected_mode, true)

      response = http_get(HealthEndpoint.port(), "/dashboard/flow")

      assert extract_status_code(response) == 302

      assert extract_header(response, "location") ==
               "/dashboard/login?next=%2Fdashboard%2Fflow"
    end

    test "protected dashboard API replies with JSON login error" do
      Application.put_env(:ferricstore, :protected_mode, true)

      response = http_get(HealthEndpoint.port(), "/dashboard/api/overview")

      assert extract_status_code(response) == 401
      assert response =~ "application/json"
      assert Jason.decode!(extract_body(response)) == %{"error" => "login required"}
    end

    test "invalid dashboard login keeps the user on a readable error page" do
      Application.put_env(:ferricstore, :protected_mode, true)

      response =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "missing",
          "password" => "wrong",
          "next" => "/dashboard/flow"
        })

      assert extract_status_code(response) == 401
      body = extract_body(response)
      assert body =~ "FerricStore Dashboard"
      assert body =~ "WRONGPASS invalid username-password pair or user is disabled."
      assert body =~ ~s(name="next" value="/dashboard/flow")
    end

    test "dashboard login rejects control bytes in next before writing Location" do
      Application.put_env(:ferricstore, :protected_mode, true)
      :ok = FerricstoreServer.Acl.set_user("dash", ["on", ">secret"])

      response =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "dash",
          "password" => "secret",
          "next" => "/dashboard\r\nX-Injected: yes"
        })

      assert extract_status_code(response) == 302
      assert extract_header(response, "location") == "/dashboard"
      refute extract_header(response, "x-injected")
    end

    test "ACL login creates a dashboard session for permitted commands" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("dash", [
          "on",
          ">secret",
          "~*",
          "-@all",
          "+FLOW.LIST"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "dash",
          "password" => "secret",
          "next" => "/dashboard/flow"
        })

      assert extract_status_code(login) == 302
      assert extract_header(login, "location") == "/dashboard/flow"

      response =
        http_get(HealthEndpoint.port(), "/dashboard/flow", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 200
      assert extract_body(response) =~ "FerricFlow"
    end

    test "dashboard actions enforce Redis command permissions" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-read", [
          "on",
          ">secret",
          "~*",
          "-@all",
          "+FLOW.LIST",
          "+FLOW.POLICY.GET"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-read",
          "password" => "secret"
        })

      response =
        http_post_form(
          HealthEndpoint.port(),
          "/dashboard/flow/policies",
          %{"type" => "email", "max_attempts" => "3"},
          [{"Cookie", dashboard_session_cookie(login)}]
        )

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.POLICY.SET"
      assert extract_body(response) =~ "+FLOW.POLICY.SET"
      assert extract_body(response) =~ "Required ACL command"

      retention_response =
        http_post_form(
          HealthEndpoint.port(),
          "/dashboard/flow/retention",
          %{"action" => "cleanup", "limit" => "1"},
          [{"Cookie", dashboard_session_cookie(login)}]
        )

      assert extract_status_code(retention_response) == 403
      assert extract_body(retention_response) =~ "FLOW.RETENTION_CLEANUP"
      assert extract_body(retention_response) =~ "+FLOW.RETENTION_CLEANUP"
      assert extract_body(retention_response) =~ "Required ACL command"
    end

    test "metrics endpoint requires the metrics ACL command instead of INFO" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("metrics-only", [
          "on",
          ">secret",
          "~*",
          "-@all",
          "+FERRICSTORE.METRICS"
        ])

      :ok =
        FerricstoreServer.Acl.set_user("info-only", [
          "on",
          ">secret",
          "~*",
          "-@all",
          "+INFO"
        ])

      metrics_login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "metrics-only",
          "password" => "secret",
          "next" => "/metrics"
        })

      metrics_response =
        http_get(HealthEndpoint.port(), "/metrics", [
          {"Cookie", dashboard_session_cookie(metrics_login)}
        ])

      assert extract_status_code(metrics_response) == 200
      assert metrics_response =~ "text/plain"

      info_login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "info-only",
          "password" => "secret",
          "next" => "/metrics"
        })

      info_response =
        http_get(HealthEndpoint.port(), "/metrics", [
          {"Cookie", dashboard_session_cookie(info_login)}
        ])

      assert extract_status_code(info_response) == 403
      assert extract_body(info_response) =~ "FERRICSTORE.METRICS"
      assert extract_body(info_response) =~ "+FERRICSTORE.METRICS"
    end

    test "retention dry-run only needs Flow read permission" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-retention-dry-run", [
          "on",
          ">secret",
          "~*",
          "-@all",
          "+FLOW.LIST"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-retention-dry-run",
          "password" => "secret"
        })

      response =
        http_post_form(
          HealthEndpoint.port(),
          "/dashboard/flow/retention",
          %{"action" => "dry_run", "limit" => "1"},
          [{"Cookie", dashboard_session_cookie(login)}]
        )

      assert extract_status_code(response) == 302
      assert extract_header(response, "location") =~ "status=dry_run"
    end

    test "dashboard retention cleanup requires global Flow write key access" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-retention-tenant", [
          "on",
          ">secret",
          "%W~tenant-a:*",
          "-@all",
          "+FLOW.LIST",
          "+FLOW.RETENTION_CLEANUP"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-retention-tenant",
          "password" => "secret"
        })

      response =
        http_post_form(
          HealthEndpoint.port(),
          "/dashboard/flow/retention",
          %{"action" => "cleanup", "limit" => "1", "confirm_cleanup" => "true"},
          [{"Cookie", dashboard_session_cookie(login)}]
        )

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.RETENTION_CLEANUP"
      assert extract_body(response) =~ "key"
    end

    test "dashboard policy POST enforces ACL key patterns from form type" do
      Application.put_env(:ferricstore, :protected_mode, true)
      denied_type = "denied:email:#{System.unique_integer([:positive])}"

      :ok =
        FerricstoreServer.Acl.set_user("flow-policy-writer", [
          "on",
          ">secret",
          "%W~allowed:*",
          "-@all",
          "+FLOW.POLICY.SET"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-policy-writer",
          "password" => "secret"
        })

      response =
        http_post_form(
          HealthEndpoint.port(),
          "/dashboard/flow/policies",
          %{"type" => denied_type, "max_attempts" => "3"},
          [{"Cookie", dashboard_session_cookie(login)}]
        )

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.POLICY.SET"
      assert extract_body(response) =~ "%W~#{denied_type}"

      policy_key = Ferricstore.Flow.Keys.policy_key(denied_type)
      assert nil == Ferricstore.Store.Router.get(FerricStore.Instance.get(:default), policy_key)
    end

    test "dashboard failure reclaim POST enforces ACL key patterns from form type" do
      Application.put_env(:ferricstore, :protected_mode, true)
      denied_type = "denied:reclaim:#{System.unique_integer([:positive])}"

      :ok =
        FerricstoreServer.Acl.set_user("flow-reclaimer", [
          "on",
          ">secret",
          "%W~allowed:*",
          "-@all",
          "+FLOW.RECLAIM"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-reclaimer",
          "password" => "secret"
        })

      response =
        http_post_form(
          HealthEndpoint.port(),
          "/dashboard/flow/failures",
          %{
            "action" => "reclaim",
            "type" => denied_type,
            "limit" => "1",
            "lease_ms" => "30000",
            "confirm_reclaim" => "true"
          },
          [{"Cookie", dashboard_session_cookie(login)}]
        )

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.RECLAIM"
      assert extract_body(response) =~ "%W~#{denied_type}"
      assert extract_body(response) =~ "write"
    end

    test "dashboard failure reclaim POST enforces ACL key patterns from form partition" do
      Application.put_env(:ferricstore, :protected_mode, true)
      type = "email"
      denied_partition = "tenant-denied:#{System.unique_integer([:positive])}"

      :ok =
        FerricstoreServer.Acl.set_user("flow-reclaimer-partition", [
          "on",
          ">secret",
          "%W~#{type}",
          "-@all",
          "+FLOW.RECLAIM"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-reclaimer-partition",
          "password" => "secret"
        })

      response =
        http_post_form(
          HealthEndpoint.port(),
          "/dashboard/flow/failures",
          %{
            "action" => "reclaim",
            "type" => type,
            "partition_key" => denied_partition,
            "limit" => "1",
            "lease_ms" => "30000",
            "confirm_reclaim" => "true"
          },
          [{"Cookie", dashboard_session_cookie(login)}]
        )

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.RECLAIM"
      assert extract_body(response) =~ "%W~#{denied_partition}"
      assert extract_body(response) =~ "write"
    end

    test "dashboard rewind action reports missing command and write key permission" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-reader-only", [
          "on",
          ">secret",
          "~tenant-a:*",
          "-@all",
          "+FLOW.GET",
          "+FLOW.REWIND"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-reader-only",
          "password" => "secret"
        })

      response =
        http_post_form(
          HealthEndpoint.port(),
          "/dashboard/flow/tenant-b%3Aflow-1/rewind",
          %{"to_event" => "1"},
          [{"Cookie", dashboard_session_cookie(login)}]
        )

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.REWIND"
      assert extract_body(response) =~ "+FLOW.REWIND"
      assert extract_body(response) =~ "%W~tenant-b:flow-1"
      assert extract_body(response) =~ "write"
    end

    test "flow detail pages enforce ACL key patterns" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("tenant-reader", [
          "on",
          ">secret",
          "~tenant-a:*",
          "-@all",
          "+FLOW.GET"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "tenant-reader",
          "password" => "secret"
        })

      response =
        http_get(HealthEndpoint.port(), "/dashboard/flow/tenant-b%3Aflow-1", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.GET"
      assert extract_body(response) =~ "+FLOW.GET"
      assert extract_body(response) =~ "~tenant-b:flow-1"
      assert extract_body(response) =~ "read"
      assert extract_body(response) =~ "key"
    end

    test "flow detail pages enforce ACL partition key from query" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-id-only-reader", [
          "on",
          ">secret",
          "~flow-1",
          "-@all",
          "+FLOW.GET"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-id-only-reader",
          "password" => "secret"
        })

      query = URI.encode_query(%{"partition_key" => "tenant-b"})

      response =
        http_get(HealthEndpoint.port(), "/dashboard/flow/flow-1?#{query}", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.GET"
      assert extract_body(response) =~ "%R~tenant-b"
      assert extract_body(response) =~ "read"
    end

    test "flow query history enforces ACL key patterns from query id" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-history-tenant-a", [
          "on",
          ">secret",
          "~tenant-a:*",
          "-@all",
          "+FLOW.HISTORY"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-history-tenant-a",
          "password" => "secret"
        })

      query =
        URI.encode_query(%{
          "kind" => "history",
          "id" => "tenant-b:flow-1"
        })

      response =
        http_get(HealthEndpoint.port(), "/dashboard/flow/query?#{query}", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.HISTORY"
      assert extract_body(response) =~ "%R~tenant-b:flow-1"
      assert extract_body(response) =~ "read"
    end

    test "flow query pages enforce ACL key patterns from partition filters" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-list-tenant-a", [
          "on",
          ">secret",
          "~tenant-a:*",
          "-@all",
          "+FLOW.LIST"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-list-tenant-a",
          "password" => "secret"
        })

      query =
        URI.encode_query(%{
          "kind" => "list",
          "type" => "email",
          "partition_key" => "tenant-b:queue"
        })

      response =
        http_get(HealthEndpoint.port(), "/dashboard/flow/query?#{query}", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.LIST"
      assert extract_body(response) =~ "%R~tenant-b:queue"
      assert extract_body(response) =~ "read"
    end

    test "flow query pages enforce ACL type keys when no partition filter exists" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-failures-billing-only", [
          "on",
          ">secret",
          "~billing",
          "-@all",
          "+FLOW.FAILURES"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-failures-billing-only",
          "password" => "secret"
        })

      query =
        URI.encode_query(%{
          "kind" => "failures",
          "type" => "checkout"
        })

      response =
        http_get(HealthEndpoint.port(), "/dashboard/flow/query?#{query}", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.FAILURES"
      assert extract_body(response) =~ "%R~checkout"
      assert extract_body(response) =~ "read"
    end

    test "flow failures page enforces ACL key patterns from partition filters" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-failures-tenant-a", [
          "on",
          ">secret",
          "~tenant-a:*",
          "-@all",
          "+FLOW.FAILURES"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-failures-tenant-a",
          "password" => "secret"
        })

      query =
        URI.encode_query(%{
          "type" => "email",
          "partition_key" => "tenant-b:queue"
        })

      response =
        http_get(HealthEndpoint.port(), "/dashboard/flow/failures?#{query}", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.FAILURES"
      assert extract_body(response) =~ "%R~tenant-b:queue"
      assert extract_body(response) =~ "read"
    end

    test "dashboard rewind action enforces ACL partition key from form" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-id-only-rewinder", [
          "on",
          ">secret",
          "~flow-1",
          "-@all",
          "+FLOW.REWIND"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-id-only-rewinder",
          "password" => "secret"
        })

      response =
        http_post_form(
          HealthEndpoint.port(),
          "/dashboard/flow/flow-1/rewind",
          %{"partition_key" => "tenant-b", "to_event" => "1", "confirm_rewind" => "true"},
          [{"Cookie", dashboard_session_cookie(login)}]
        )

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.REWIND"
      assert extract_body(response) =~ "%W~tenant-b"
      assert extract_body(response) =~ "write"
    end

    test "dashboard API forbidden replies include required ACL command and key details" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("tenant-api-reader", [
          "on",
          ">secret",
          "~tenant-a:*",
          "-@all",
          "+FLOW.GET"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "tenant-api-reader",
          "password" => "secret"
        })

      response =
        http_get(HealthEndpoint.port(), "/dashboard/api/flow/tenant-b%3Aflow-1", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 403

      assert Jason.decode!(extract_body(response)) == %{
               "error" => "forbidden",
               "reason" =>
                 "NOPERM this user has no permissions to access one of the keys mentioned in the command (FLOW.GET key)",
               "required_acl_rule" => "+FLOW.GET",
               "required_command" => "FLOW.GET",
               "required_key" => "tenant-b:flow-1",
               "required_key_access" => "read",
               "required_key_rule" => "%R~tenant-b:flow-1"
             }
    end

  end
    end
  end
end
