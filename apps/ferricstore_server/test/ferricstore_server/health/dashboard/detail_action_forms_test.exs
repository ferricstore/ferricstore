defmodule FerricstoreServer.Health.Dashboard.DetailActionFormsTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.{Acl, Health.Dashboard}
  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.Session

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    :ok
  end

  test "rewind forms carry the displayed state and version" do
    {id, _event, record} = create_ready()
    data = Dashboard.collect_flow_detail_page(id, values: false, history_count: 100)
    html = Dashboard.render_flow_detail_page(data)
    assert html =~ ~s(name="expect_state" value="ready")
    assert html =~ ~s(name="expected_version" value="#{record.version}")
    assert html =~ ~s(name="history_count" value="100")
    assert html =~ ~r/name="confirm_rewind"[^>]*required/
  end

  test "a stale rewind is rejected even if the state changes back to the displayed state" do
    {id, event, record} = create_ready()
    params = rewind_params(id, event, record)
    :ok = FerricStore.flow_transition(id, "ready", "approved", fencing_token: 0)
    :ok = FerricStore.flow_transition(id, "approved", "ready", fencing_token: 0)
    {:ok, before} = FerricStore.flow_get(id)
    assert {:error, message} = Dashboard.apply_flow_rewind_form(params)
    assert message =~ "changed"
    assert {:ok, ^before} = FerricStore.flow_get(id)
  end

  test "unchanged rewind succeeds and duplicate submission is rejected" do
    {id, event, record} = create_ready()
    params = rewind_params(id, event, record)
    assert {:ok, ^id, nil} = Dashboard.apply_flow_rewind_form(params)
    {:ok, rewound} = FerricStore.flow_get(id)
    assert rewound.state == "queued"
    assert {:error, _} = Dashboard.apply_flow_rewind_form(params)
    assert {:ok, ^rewound} = FerricStore.flow_get(id)
  end

  test "explicit rewind scheduling restores event schedule or applies a UTC override" do
    for mode <- ["keep", "now", "at"] do
      {id, event, record} = create_ready()

      {:ok, [{^event, fields}]} =
        FerricStore.flow_history(id, from_event: event, to_event: event, count: 1)

      before = System.system_time(:millisecond)

      params =
        Map.merge(rewind_params(id, event, record), %{
          "schedule_mode" => mode,
          "run_at_utc" => "2030-01-02T03:04:05.006"
        })

      assert {:ok, ^id, nil} = Dashboard.apply_flow_rewind_form(params)
      {:ok, rewound} = FerricStore.flow_get(id)

      case mode do
        "keep" ->
          assert to_string(rewound.next_run_at_ms) == fields["next_run_at_ms"]

        "now" ->
          assert rewound.next_run_at_ms >= before and
                   rewound.next_run_at_ms <= System.system_time(:millisecond)

        "at" ->
          assert rewound.next_run_at_ms ==
                   DateTime.to_unix(~U[2030-01-02 03:04:05.006Z], :millisecond)
      end
    end
  end

  test "rejected UTC scheduling keeps the escaped draft and reviewed snapshot" do
    {id, event, record} = create_ready()

    params =
      Map.merge(rewind_params(id, event, record), %{
        "schedule_mode" => "at",
        "run_at_utc" => ~s|bad"<&|,
        "reviewed_type" => "type<script>"
      })

    response = post(id, "rewind", params)
    assert extract_status_code(response) == 422
    html = extract_body(response)
    assert html =~ ~s(type="text" name="run_at_utc" value="bad&quot;&lt;&amp;")
    assert html =~ "type&lt;script&gt;"
    assert html =~ ~s(<option value="at" selected>)
    assert {:ok, ^record} = FerricStore.flow_get(id)
  end

  test "rewind rejects missing or invalid confirmation guards" do
    {id, event, record} = create_ready()
    params = rewind_params(id, event, record)

    for field <- ["expect_state", "expected_version"] do
      assert {:error, _} = Dashboard.apply_flow_rewind_form(Map.delete(params, field))
    end

    for value <- ["-1", "oops", ""] do
      assert {:error, _} =
               Dashboard.apply_flow_rewind_form(Map.put(params, "expected_version", value))
    end

    assert {:ok, ^record} = FerricStore.flow_get(id)
  end

  test "the replicated rewind path validates expected version before mutation" do
    {id, event, record} = create_ready()

    assert {:error, "ERR flow changed concurrently"} =
             FerricStore.flow_rewind(id, to_event: event, expected_version: record.version - 1)

    assert {:ok, ^record} = FerricStore.flow_get(id)
    assert {:error, _} = FerricStore.flow_rewind(id, to_event: event, expected_version: -1)
    assert :ok = FerricStore.flow_rewind(id, to_event: event, expected_version: record.version)
  end

  test "signal rejection preserves draft, history scope and CSRF then correction succeeds" do
    {id, _event, _record} = create_ready()

    params = %{
      "signal" => "payment",
      "transition_to" => "approved",
      "idempotency_key" => "event-1",
      "history_count" => "100",
      "history_before" => "123-4",
      "unexpected" => "do-not-reflect"
    }

    response = post(id, "signal", params)
    assert extract_status_code(response) == 422
    html = extract_body(response)
    assert html =~ ~s(name="signal" value="payment")
    assert html =~ ~s(name="transition_to" value="approved")
    assert html =~ ~s(name="idempotency_key" value="event-1")
    assert html =~ ~s(name="history_before" value="123-4")
    assert html =~ "history_count=100"
    assert html =~ ~s(role="alert")
    assert html =~ ~s(name="_csrf_token")
    assert html =~ ~s(aria-invalid="true")
    refute html =~ "do-not-reflect"
    refute html =~ ~s(data-dashboard-live-url="/)
    corrected = post(id, "signal", Map.put(params, "if_state", "ready"))
    assert extract_status_code(corrected) == 302
    assert extract_header(corrected, "location") =~ "history_before=123-4"
    {:ok, current} = FerricStore.flow_get(id)
    assert current.state == "approved"
  end

  test "rewind rejection preserves escaped target and invalid numeric draft without rereading history" do
    {id, event, record} = create_ready()
    params = rewind_params(id, event, record) |> Map.put("run_at_ms", ~s|bad"<&|)
    response = post(id, "rewind", params)
    assert extract_status_code(response) == 422
    html = extract_body(response)
    assert html =~ ~s(name="to_event" value="#{event}")
    assert html =~ ~s(name="run_at_ms" value="bad&quot;&lt;&amp;")
    assert html =~ ~s(type="text" name="run_at_ms")
    assert html =~ ~s(name="expected_version" value="#{record.version}")
    refute html =~ "Execution Journal"
    refute html =~ "Event intervals"
  end

  test "action-only users receive just their escaped draft on errors" do
    {id, _event, _record} = create_ready()
    Application.put_env(:ferricstore, :protected_mode, true)
    username = "action-writer-#{System.unique_integer([:positive])}"
    :ok = Acl.set_user(username, ["on", "nopass", "-@all", "+FLOW.SIGNAL", "%W~#{id}"])
    on_exit(fn -> Acl.del_user(username) end)
    cookie = Session.session_cookie(username) |> String.split(";", parts: 2) |> hd()

    assert extract_status_code(
             http_get(Endpoint.port(), "/dashboard/flow/#{id}", [{"Cookie", cookie}])
           ) == 403

    response =
      post(
        id,
        "signal",
        %{"signal" => "<script>bad()</script>", "transition_to" => "approved"},
        cookie
      )

    assert extract_status_code(response) == 422
    html = extract_body(response)
    assert html =~ "&lt;script&gt;bad()&lt;/script&gt;"
    refute html =~ "<script>bad()</script>"
    refute html =~ "Execution Journal"
    refute html =~ "Attempts"
  end

  defp create_ready do
    id = "action-form-#{System.unique_integer([:positive])}"
    :ok = FerricStore.flow_create(id, type: "action-forms", state: "queued")
    {:ok, [{event, _} | _]} = FerricStore.flow_history(id)
    :ok = FerricStore.flow_transition(id, "queued", "ready", fencing_token: 0)
    {:ok, record} = FerricStore.flow_get(id)
    {id, event, record}
  end

  defp rewind_params(id, event, record),
    do: %{
      "id" => id,
      "to_event" => event,
      "expect_state" => record.state,
      "expected_version" => to_string(record.version),
      "confirm_rewind" => "true"
    }

  defp post(id, action, params, session_cookie \\ "") do
    {token, cookie} = Session.csrf_pair()
    cookie = cookie |> String.split(";", parts: 2) |> hd()
    path = "/dashboard/flow/" <> URI.encode(id, &URI.char_unreserved?/1) <> "/" <> action

    http_post_form(Endpoint.port(), path, Map.put(params, "_csrf_token", token), [
      {"Cookie", session_cookie <> "; " <> cookie}
    ])
  end
end
