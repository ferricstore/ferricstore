defmodule FerricstoreServer.Health.Dashboard.ManagementFormsTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Render.{FlowSchedules, FlowGovernance}
  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.Session
  alias FerricstoreServer.Health.Dashboard.Flow.Schedules
  alias FerricstoreServer.Acl

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    :ok
  end

  test "invalid schedule JSON returns an escaped editable draft, not a redirect" do
    id = "invalid-draft-#{System.unique_integer([:positive])}"
    payload = ~s|{"unfinished":"</textarea><script>bad()</script>|
    {token, cookie} = FerricstoreServer.Health.Endpoint.Session.csrf_pair()

    response =
      http_post_form(
        Endpoint.port(),
        "/dashboard/flow/schedules",
        %{
          "_csrf_token" => token,
          "action" => "create",
          "id" => id,
          "schedule_kind" => "interval",
          "every_ms" => "60000",
          "target_type" => "draft-email",
          "target_partition" => "scope<&",
          "target_payload" => payload,
          "overlap_policy" => "skip",
          "max_fires" => "9",
          "q" => "keep-filter"
        },
        [{"Cookie", cookie |> String.split(";", parts: 2) |> hd()}]
      )

    assert extract_status_code(response) == 422
    refute extract_header(response, "location")
    body = extract_body(response)
    assert body =~ ~s(name="id" value="#{id}")
    assert body =~ ~s(name="every_ms")
    assert body =~ ~s(value="60000")
    assert body =~ "scope&lt;&amp;"
    assert body =~ "&lt;/textarea&gt;&lt;script&gt;bad()&lt;/script&gt;"
    refute body =~ "<script>bad()</script>"
    assert body =~ "target payload must be valid JSON"
    assert body =~ ~r/id="flow-schedule-create-panel"[^>]*open/
    assert body =~ ~s(name="_csrf_token")
    assert body =~ "keep-filter"
  end

  test "schedule drafts select the active mode and disable irrelevant fields without JavaScript" do
    for {kind, active} <- [{"cron", "cron"}, {"interval", "every_ms"}, {"delay", "delay_ms"}] do
      html = FlowSchedules.render_flow_schedule_create_form(%{draft: %{"schedule_kind" => kind}})
      assert html =~ ~s(<option value="#{kind}" selected)

      for name <- ["cron", "every_ms", "delay_ms"] do
        [input] = Regex.run(~r/<input[^>]*name="#{name}"[^>]*>/, html)

        if name == active do
          assert input =~ "required"
          refute input =~ "disabled"
        else
          assert input =~ "disabled"
        end
      end
    end
  end

  test "a principal without catalog read access gets only its submitted draft back" do
    username = "schedule-draft-#{System.unique_integer([:positive])}"
    Application.put_env(:ferricstore, :protected_mode, true)

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "%W~*",
               "-@all",
               "+FLOW.SCHEDULE.LIST",
               "+FLOW.SCHEDULE.CREATE"
             ])

    on_exit(fn -> Acl.del_user(username) end)
    cookie = Session.session_cookie(username) |> String.split(";", parts: 2) |> hd()

    assert extract_status_code(
             http_get(Endpoint.port(), "/dashboard/flow/schedules", [{"Cookie", cookie}])
           ) == 403

    response =
      post_schedule(
        %{
          "action" => "create",
          "id" => "not-created",
          "target_type" => "draft-type",
          "schedule_kind" => "interval",
          "every_ms" => "0"
        },
        cookie
      )

    assert extract_status_code(response) == 422
    body = extract_body(response)
    assert body =~ ~s(name="id" value="not-created")
    refute body =~ ~s(aria-label="Workflow schedules")
    refute body =~ "No schedules matched"

    assert :ok = Acl.set_user(username, ["-FLOW.SCHEDULE.CREATE"])
    cookie = Session.session_cookie(username) |> String.split(";", parts: 2) |> hd()

    denied =
      post_schedule(
        %{"action" => "create", "id" => "denied-draft", "target_type" => "draft-type"},
        cookie
      )

    assert extract_status_code(denied) == 403
    refute extract_body(denied) =~ ~s(id="flow-schedule-create-panel")
  end

  test "successful schedule submission redirects once and preserves list filters" do
    id = "valid-draft-#{System.unique_integer([:positive])}"

    response =
      post_schedule(%{
        "action" => "create",
        "id" => id,
        "target_type" => "draft-test",
        "schedule_kind" => "interval",
        "every_ms" => "3600000",
        "max_fires" => "2",
        "target_payload" => "{\"ok\":true}",
        "q" => "keep-filter"
      })

    assert extract_status_code(response) == 302
    location = extract_header(response, "location")
    assert location =~ "status=ok"
    assert location =~ "q=keep-filter"
    refute location =~ "target_payload"
    assert {:ok, schedule} = FerricStore.flow_schedule_get(id)
    assert schedule.max_fires == 2
    assert schedule.fire_count == 0
    on_exit(fn -> FerricStore.flow_schedule_delete(id) end)
  end

  test "error page data only retains allowlisted draft fields" do
    data =
      Schedules.create_error_page(
        %{"id" => "draft", "_csrf_token" => "secret", "unexpected" => "private", "q" => "filter"},
        "bad input"
      )

    assert data.draft == %{"id" => "draft"}
    assert data.filters.q == "filter"
    assert data.catalog_loaded == false
  end

  test "draft reflection is allowlisted and escapes text attributes and textarea content" do
    html =
      FlowSchedules.render_flow_schedule_create_form(%{
        draft: %{
          "id" => ~s|" autofocus onfocus="bad()|,
          "target_payload" => "</textarea><script>bad()</script>",
          "_csrf_token" => "submitted-secret",
          "unexpected" => "unwanted"
        }
      })

    assert html =~ "&quot; autofocus onfocus=&quot;bad()"
    assert html =~ "&lt;/textarea&gt;&lt;script&gt;bad()&lt;/script&gt;"
    refute html =~ "submitted-secret"
    refute html =~ "unwanted"
  end

  test "governance metadata stays open for non-idle results and partial submitted searches" do
    for status <- [:ok, :error] do
      assert FlowGovernance.state_meta_open?(%{
               state_meta_result: %{status: status},
               filters: %{}
             })
    end

    assert FlowGovernance.state_meta_open?(%{
             state_meta_result: %{status: :idle},
             filters: %{meta_type: "email"}
           })

    refute FlowGovernance.state_meta_open?(%{
             state_meta_result: %{status: :idle},
             filters: %{meta_type: ""}
           })

    refute FlowGovernance.state_meta_open?(%{})
    refute FlowGovernance.state_meta_open?(%{filters: %{meta_value_type: "string"}})
  end

  defp post_schedule(params, session_cookie \\ "") do
    {token, cookie} = Session.csrf_pair()
    cookie = cookie |> String.split(";", parts: 2) |> hd()

    http_post_form(
      Endpoint.port(),
      "/dashboard/flow/schedules",
      Map.put(params, "_csrf_token", token),
      [{"Cookie", Enum.join([session_cookie, cookie], "; ")}]
    )
  end
end
