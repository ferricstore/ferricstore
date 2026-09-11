defmodule FerricstoreServer.Health.Dashboard.DetailSecurityRegressionsTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.{Acl, Health.Dashboard}
  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.Session

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :protected_mode)
    history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
    Application.put_env(:ferricstore, :protected_mode, true)

    on_exit(fn ->
      restore_env(:protected_mode, previous)
      restore_env(:flow_dashboard_flow_history_fun, history)
    end)

    :ok
  end

  for action <- ["signal", "rewind"] do
    @action action
    test "#{action} authorizes and executes the route ID, never the supplied body ID" do
      allowed = uid("allowed")
      victim = uid("victim")
      create_ready(allowed)
      victim_event = create_ready(victim)
      command = if @action == "signal", do: "FLOW.SIGNAL", else: "FLOW.REWIND"
      {_, cookie} = user(["%W~#{allowed}", "-@all", "+#{command}"])
      {:ok, original} = FerricStore.flow_get(victim)
      {:ok, original_history} = FerricStore.flow_history(victim)
      params = params(@action, victim, victim_event)
      assert extract_status_code(post(victim, @action, params, cookie)) == 403
      response = post(allowed, @action, params, cookie)
      assert extract_status_code(response) in [302, 422]
      assert {:ok, ^original} = FerricStore.flow_get(victim)
      assert {:ok, ^original_history} = FerricStore.flow_history(victim)
    end

    test "#{action} preserves significant spaces in encoded route IDs" do
      victim = uid("exact")
      allowed = " " <> victim <> " "
      event = create_ready(allowed)
      create_ready(victim)
      command = if @action == "signal", do: "FLOW.SIGNAL", else: "FLOW.REWIND"
      {_, cookie} = user(["%W~#{allowed}", "-@all", "+#{command}"])
      {:ok, original} = FerricStore.flow_get(victim)
      response = post(allowed, @action, params(@action, allowed, event), cookie)
      assert extract_status_code(response) == 302
      assert {:ok, ^original} = FerricStore.flow_get(victim)
      {:ok, changed} = FerricStore.flow_get(allowed)
      assert changed.state == if(@action == "signal", do: "changed", else: "queued")
    end
  end

  test "GET-only users cannot read history through HTML or live JSON" do
    id = uid("history")
    create_ready(id)
    marker = uid("historical-marker")
    :ok = FerricStore.flow_signal(id, signal: marker)
    :ok = FerricStore.flow_signal(id, signal: "current")
    {:ok, record} = FerricStore.flow_get(id)
    refute Jason.encode!(record) =~ marker
    {username, cookie} = user(["%R~*", "-@all", "+FLOW.GET"])

    for prefix <- ["/dashboard/flow/", "/dashboard/api/flow/"] do
      response = http_get(Endpoint.port(), prefix <> id, [{"Cookie", cookie}])
      assert extract_status_code(response) == 200
      refute response =~ marker
      assert response =~ "History restricted"
    end

    :ok = Acl.set_user(username, ["+FLOW.HISTORY"])
    cookie = session_cookie(username)

    for prefix <- ["/dashboard/flow/", "/dashboard/api/flow/"] do
      assert http_get(Endpoint.port(), prefix <> id, [{"Cookie", cookie}]) =~ marker
    end
  end

  test "denied history does not invoke the history reader" do
    id = uid("no-history-read")
    create_ready(id)
    {username, _} = user(["%R~*", "-@all", "+FLOW.GET"])
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn _, _ ->
      send(parent, :history_read)
      {:ok, []}
    end)

    data = Dashboard.collect_flow_detail_page(id, acl_username: username, values: false)
    assert data.history_status == :forbidden
    assert data.history == []
    refute_receive :history_read
  end

  test "historical value references require history permission but current values still work" do
    id = uid("values")
    :ok = FerricStore.flow_create(id, type: "detail-security", payload: "historical-secret")
    {:ok, old} = FerricStore.flow_get(id)

    :ok =
      FerricStore.flow_transition(id, "queued", "ready",
        fencing_token: 0,
        payload: "current-value"
      )

    {:ok, current} = FerricStore.flow_get(id)
    {username, cookie} = user(["%R~*", "-@all", "+FLOW.GET"])
    path = fn ref -> "/dashboard/api/flow/value?" <> URI.encode_query(%{flow: id, ref: ref}) end
    response = http_get(Endpoint.port(), path.(old.payload_ref), [{"Cookie", cookie}])
    refute response =~ "historical-secret"
    assert response =~ "not visible"

    assert http_get(Endpoint.port(), path.(current.payload_ref), [{"Cookie", cookie}]) =~
             "current-value"

    :ok = Acl.set_user(username, ["+FLOW.HISTORY"])

    assert http_get(Endpoint.port(), path.(old.payload_ref), [
             {"Cookie", session_cookie(username)}
           ]) =~ "historical-secret"
  end

  defp params("signal", id, _),
    do: %{"id" => id, "signal" => "review", "if_state" => "ready", "transition_to" => "changed"}

  defp params("rewind", id, event) do
    {:ok, record} = FerricStore.flow_get(id)

    %{
      "id" => id,
      "to_event" => event,
      "confirm_rewind" => "true",
      "expect_state" => "ready",
      "expected_version" => record.version
    }
  end

  defp create_ready(id) do
    :ok = FerricStore.flow_create(id, type: "detail-security", state: "queued")
    {:ok, [{event, _} | _]} = FerricStore.flow_history(id)
    :ok = FerricStore.flow_transition(id, "queued", "ready", fencing_token: 0)
    event
  end

  defp user(rules) do
    name = uid("user")
    :ok = Acl.set_user(name, ["on", "nopass" | rules])
    on_exit(fn -> Acl.del_user(name) end)
    {name, session_cookie(name)}
  end

  defp session_cookie(name),
    do: Session.session_cookie(name) |> String.split(";", parts: 2) |> hd()

  defp uid(prefix), do: "detail-#{prefix}-#{System.unique_integer([:positive])}"

  defp post(id, action, params, session_cookie) do
    {token, cookie} = Session.csrf_pair()
    cookie = cookie |> String.split(";", parts: 2) |> hd()
    path = "/dashboard/flow/" <> URI.encode(id, &URI.char_unreserved?/1) <> "/" <> action

    http_post_form(Endpoint.port(), path, Map.put(params, "_csrf_token", token), [
      {"Cookie", session_cookie <> "; " <> cookie}
    ])
  end
end
