defmodule FerricstoreServer.Health.Dashboard.ManagementThirdReviewTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Flow.PolicyEditor

  alias FerricstoreServer.Health.Dashboard.Render.{
    Admin,
    Capabilities,
    FlowPolicy,
    FlowIndexCatalog
  }

  alias FerricstoreServer.Health.Dashboard.Render.Security

  test "static snapshot belongs to the header without JavaScript layout repair" do
    layout = FerricstoreServer.Health.Dashboard.Layout
    refute layout.sidebar_html("flow_schedules", %{}) =~ ~s(data-dashboard-snapshot)
    header = layout.render_subpage_header("Schedules")
    assert header =~ ~s(data-dashboard-snapshot)
    assert header =~ ~s(<a href="" class="flow-search-button" data-dashboard-refresh)
  end

  test "help outside metric headers has an explicit escaped topic" do
    html =
      apply(FerricstoreServer.Health.Dashboard.Format, :info_icon, [
        "Description",
        "About <filters>\" scope"
      ])

    assert html =~ ~s(aria-label="About &lt;filters&gt;&quot; scope")
    refute html =~ ~s(aria-label="Metric help")
  end

  test "policy only enables fields applicable to the loaded scope" do
    editor = %{PolicyEditor.empty() | type: "review"}
    defaults = FlowPolicy.render_flow_policy_editor(%{editor: editor})
    assert defaults =~ ~r/<select[^>]*name="mode"[^>]*disabled/
    refute defaults =~ ~r/<input[^>]*name="indexed_attributes"[^>]*disabled/

    state = FlowPolicy.render_flow_policy_editor(%{editor: %{editor | state: "queued"}})
    refute state =~ ~r/<select[^>]*name="mode"[^>]*disabled/
    assert state =~ ~r/<input[^>]*name="indexed_attributes"[^>]*disabled/
    assert state =~ ~r/<input[^>]*name="indexed_state_meta"[^>]*disabled/
    assert state =~ "Type-wide settings are unchanged"
  end

  test "policy exposes unsaved draft status" do
    html =
      FlowPolicy.render_flow_policy_editor(%{editor: %{PolicyEditor.empty() | type: "review"}})

    assert html =~ ~s(data-policy-dirty-status)
    assert html =~ "Unsaved changes"
  end

  test "account profiles disable irrelevant scope controls and preview effective access" do
    for role <- ["admin", "observer", "custom"] do
      html =
        Security.render_account_management(%{
          can_manage_users: true,
          account_draft: %{"role" => role}
        })

      assert html =~ ~s(data-acl-profile-preview)

      for section <- ["observer", "custom"] do
        [fieldset] = Regex.run(~r/<fieldset[^>]*data-acl-profile="#{section}"[^>]*>/, html)
        assert String.contains?(fieldset, "disabled") == (section != role)
        assert String.contains?(fieldset, "hidden") == (section != role)
      end
    end
  end

  test "every account mutation opts into duplicate-submit protection" do
    data = %{
      can_manage_users: true,
      can_delete_users: true,
      current_user: "admin",
      acl_users: [%{username: "operator", state: "on", password_configured: true}]
    }

    html = Security.render_account_management(data) <> Security.render_acl_users(data)
    forms = Regex.scan(~r/<form[^>]*method="post"[^>]*>/, html) |> List.flatten()
    assert length(forms) == 5
    assert Enum.all?(forms, &String.contains?(&1, "data-dashboard-single-submit"))
  end

  test "validation disclosure accurately identifies command reference" do
    html = FlowIndexCatalog.render(%{status: :ok, snapshot: %{"indexes" => [%{"id" => "idx"}]}})
    assert html =~ "Validation command"
    refute html =~ "Inspect validation"
    assert html =~ "FLOW.QUERY.INDEXES idx"
  end

  test "runtime parameter columns have a scoped layout contract" do
    html = Admin.render_config_parameters([])
    assert html =~ ~s(class="config-parameters-table")
    assert html =~ ~s(<colgroup>)
  end

  test "capability command uses code-scale presentation" do
    html = Capabilities.render_management_capability_summary(%{})
    assert html =~ ~s(ops-summary-code)
    assert html =~ "FERRICSTORE.CAPABILITIES"
  end
end
