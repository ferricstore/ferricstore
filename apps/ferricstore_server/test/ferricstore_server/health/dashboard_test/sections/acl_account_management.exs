defmodule FerricstoreServer.Health.DashboardTest.Sections.AclAccountManagement do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint

      describe "dashboard HTTP-only ACL bootstrap and account management" do
        @describetag :dashboard_accounts

        setup do
          FerricstoreServer.Acl.reset!()
          Application.put_env(:ferricstore, :protected_mode, true)
          :ok
        end

        test "an initial protected dashboard redirects to one-time setup" do
          response = http_get(HealthEndpoint.port(), "/dashboard")

          assert extract_status_code(response) == 302

          assert extract_header(response, "location") ==
                   "/dashboard/setup?next=%2Fdashboard"

          setup_page = http_get(HealthEndpoint.port(), "/dashboard/setup?next=%2Fdashboard")
          assert extract_status_code(setup_page) == 200
          assert extract_body(setup_page) =~ "Create the recovery administrator"
          assert extract_body(setup_page) =~ ~s(name="password_confirmation")

          direct_login =
            http_get(
              HealthEndpoint.port(),
              "/dashboard/login?next=%2Fdashboard%2Fsecurity"
            )

          assert extract_status_code(direct_login) == 302

          assert extract_header(direct_login, "location") ==
                   "/dashboard/setup?next=%2Fdashboard%2Fsecurity"
        end

        test "local HTTP setup passwords default and starts an authenticated session" do
          response =
            http_post_form(HealthEndpoint.port(), "/dashboard/setup", %{
              "password" => "bootstrap-password-123",
              "password_confirmation" => "bootstrap-password-123",
              "next" => "/dashboard/security"
            })

          assert extract_status_code(response) == 302
          assert extract_header(response, "location") =~ "/dashboard/security?"

          assert {:ok, "default"} =
                   FerricstoreServer.Acl.authenticate("default", "bootstrap-password-123")

          cookie = dashboard_session_cookie(response)

          security =
            http_get(HealthEndpoint.port(), "/dashboard/security", [{"Cookie", cookie}])

          assert extract_status_code(security) == 200
          assert extract_body(security) =~ "Account management"

          setup_again = http_get(HealthEndpoint.port(), "/dashboard/setup")
          assert extract_status_code(setup_again) == 302
          assert extract_header(setup_again, "location") == "/dashboard/login"
        end

        test "an ACL administrator creates a second dashboard user over HTTP" do
          previous_audit_enabled = Application.get_env(:ferricstore, :audit_log_enabled)
          Application.put_env(:ferricstore, :audit_log_enabled, true)
          :ok = Ferricstore.AuditLog.reset()

          on_exit(fn -> restore_env(:audit_log_enabled, previous_audit_enabled) end)

          :ok =
            FerricstoreServer.Acl.set_user("default", [
              "on",
              ">default-password-123",
              "~*",
              "&*",
              "+@all"
            ])

          login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "default",
              "password" => "default-password-123"
            })

          cookie = dashboard_session_cookie(login)

          create =
            http_post_form(
              HealthEndpoint.port(),
              "/dashboard/security/users",
              %{
                "username" => "observer",
                "password" => "observer-password-123",
                "password_confirmation" => "observer-password-123",
                "role" => "observer",
                "key_pattern" => "tenant-a:*",
                "channel_pattern" => "tenant-a:*"
              },
              [{"Cookie", cookie}]
            )

          assert extract_status_code(create) == 302
          assert extract_header(create, "location") =~ "status=ok"

          assert create
                 |> extract_header("location")
                 |> URI.parse()
                 |> Map.fetch!(:query)
                 |> URI.decode_query()
                 |> Map.fetch!("message") == "Account 'observer' created."

          assert {:ok, "observer"} =
                   FerricstoreServer.Acl.authenticate("observer", "observer-password-123")

          observer_login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "observer",
              "password" => "observer-password-123"
            })

          assert extract_status_code(observer_login) == 302

          Ferricstore.Test.ShardHelpers.eventually(
            fn ->
              Enum.any?(Ferricstore.AuditLog.get(), fn {_id, _at, event, details} ->
                event == :acl_user_change and details[:target] == "observer"
              end)
            end,
            "expected dashboard account mutation audit event",
            20,
            10
          )

          {_id, _at, :acl_user_change, details} =
            Enum.find(Ferricstore.AuditLog.get(), fn {_id, _at, event, details} ->
              event == :acl_user_change and details[:target] == "observer"
            end)

          assert details[:actor] == "default"
          assert details[:action] == :create
          assert details[:outcome] == :ok
          refute inspect(details) =~ "observer-password-123"
        end

        test "an observer cannot create dashboard users" do
          :ok =
            FerricstoreServer.Acl.set_user("observer", [
              "on",
              ">observer-password-123",
              "~*",
              "-@all",
              "+ACL.LIST",
              "+INFO"
            ])

          login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "observer",
              "password" => "observer-password-123"
            })

          response =
            http_post_form(
              HealthEndpoint.port(),
              "/dashboard/security/users",
              %{
                "username" => "forbidden-user",
                "password" => "forbidden-password-123",
                "password_confirmation" => "forbidden-password-123",
                "role" => "admin"
              },
              [{"Cookie", dashboard_session_cookie(login)}]
            )

          assert extract_status_code(response) == 403
          assert extract_body(response) =~ "ACL.SETUSER"
          assert FerricstoreServer.Acl.get_user("forbidden-user") == nil
        end

        test "password reset revokes the target session and deletion removes the account" do
          :ok = FerricstoreServer.Acl.set_user("default", ["on", ">default-password-123"])
          :ok = FerricstoreServer.Acl.set_user("member", ["on", ">member-password-123"])

          admin_login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "default",
              "password" => "default-password-123"
            })

          member_login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "member",
              "password" => "member-password-123"
            })

          reset =
            http_post_form(
              HealthEndpoint.port(),
              "/dashboard/security/users/password",
              %{
                "username" => "member",
                "password" => "replacement-password-123",
                "password_confirmation" => "replacement-password-123"
              },
              [{"Cookie", dashboard_session_cookie(admin_login)}]
            )

          assert extract_status_code(reset) == 302

          assert reset
                 |> extract_header("location")
                 |> URI.parse()
                 |> Map.fetch!(:query)
                 |> URI.decode_query()
                 |> Map.fetch!("message") == "Password for 'member' reset."

          revoked =
            http_get(HealthEndpoint.port(), "/dashboard", [
              {"Cookie", dashboard_session_cookie(member_login)}
            ])

          assert extract_status_code(revoked) == 302
          assert extract_header(revoked, "location") =~ "/dashboard/login"

          accounts =
            http_get(HealthEndpoint.port(), "/dashboard/security", [
              {"Cookie", dashboard_session_cookie(admin_login)}
            ])

          assert extract_body(accounts) =~ "Confirm delete"

          delete =
            http_post_form(
              HealthEndpoint.port(),
              "/dashboard/security/users/delete",
              %{"username" => "member"},
              [{"Cookie", dashboard_session_cookie(admin_login)}]
            )

          assert extract_status_code(delete) == 302

          assert delete
                 |> extract_header("location")
                 |> URI.parse()
                 |> Map.fetch!(:query)
                 |> URI.decode_query()
                 |> Map.fetch!("message") == "Account 'member' deleted."

          assert FerricstoreServer.Acl.get_user("member") == nil
        end

        test "rule and state forms mutate exactly the requested account" do
          :ok = FerricstoreServer.Acl.set_user("default", ["on", ">default-password-123"])
          :ok = FerricstoreServer.Acl.set_user("member", ["on", ">member-password-123"])

          admin_login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "default",
              "password" => "default-password-123"
            })

          admin_cookie = dashboard_session_cookie(admin_login)

          rules =
            http_post_form(
              HealthEndpoint.port(),
              "/dashboard/security/users/rules",
              %{"username" => "member", "modifiers" => "+GET\n%R~tenant-a:*"},
              [{"Cookie", admin_cookie}]
            )

          assert extract_status_code(rules) == 302

          assert rules
                 |> extract_header("location")
                 |> URI.parse()
                 |> Map.fetch!(:query)
                 |> URI.decode_query()
                 |> Map.fetch!("message") ==
                   "ACL modifiers for 'member' updated."

          assert :ok = FerricstoreServer.Acl.check_command("member", "GET")
          assert :ok = FerricstoreServer.Acl.check_key_access("member", "tenant-a:key", :read)

          malformed_state =
            http_post_form(
              HealthEndpoint.port(),
              "/dashboard/security/users/state",
              %{"username" => "member", "enabled" => "unexpected"},
              [{"Cookie", admin_cookie}]
            )

          assert extract_header(malformed_state, "location") =~ "status=error"
          assert FerricstoreServer.Acl.get_user("member").enabled

          member_login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "member",
              "password" => "member-password-123"
            })

          disabled =
            http_post_form(
              HealthEndpoint.port(),
              "/dashboard/security/users/state",
              %{"username" => "member", "enabled" => "false"},
              [{"Cookie", admin_cookie}]
            )

          assert extract_status_code(disabled) == 302

          assert disabled
                 |> extract_header("location")
                 |> URI.parse()
                 |> Map.fetch!(:query)
                 |> URI.decode_query()
                 |> Map.fetch!("message") == "Account 'member' disabled."

          refute FerricstoreServer.Acl.get_user("member").enabled

          revoked =
            http_get(HealthEndpoint.port(), "/dashboard", [
              {"Cookie", dashboard_session_cookie(member_login)}
            ])

          assert extract_header(revoked, "location") =~ "/dashboard/login"
        end

        test "resetting the current password clears the current dashboard session" do
          :ok = FerricstoreServer.Acl.set_user("default", ["on", ">default-password-123"])

          login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "default",
              "password" => "default-password-123"
            })

          reset =
            http_post_form(
              HealthEndpoint.port(),
              "/dashboard/security/users/password",
              %{
                "username" => "default",
                "password" => "replacement-password-123",
                "password_confirmation" => "replacement-password-123"
              },
              [{"Cookie", dashboard_session_cookie(login)}]
            )

          assert extract_status_code(reset) == 302
          assert extract_header(reset, "location") == "/dashboard/login"
          assert extract_header(reset, "set-cookie") =~ "ferricstore_dashboard=;"

          assert {:ok, "default"} =
                   FerricstoreServer.Acl.authenticate("default", "replacement-password-123")
        end
      end
    end
  end
end
